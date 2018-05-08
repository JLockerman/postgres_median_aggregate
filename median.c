#include <postgres.h>
#include <fmgr.h>
#include <lib/rbtree.h>
#include <catalog/pg_type.h>
#include <utils/typcache.h>
#include <utils/lsyscache.h>
#include <fmgr.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(median_transfn);


/* RedBlack Tree specialization for use as a histogram */

typedef struct HTree {
	RBTree	*tree;
	uint64	num_elements;
	//TODO is it safe to store the node contianing the meidan
	//HistNode	*median;
} HTree;

typedef struct HistNode {
	RBNode	node;
	Datum	data;
	uint64	count;
} HistNode;

typedef union MedianResult {
	char	nothing;
	Datum	data;
} MedianResult;

/* HNodes are compared based on data */
static inline int
hnode_compare(const RBNode *existing, const RBNode *newdata, void *arg)
{
	HistNode *e = (HistNode *)existing;
	HistNode *n = (HistNode *)newdata;
	TypeCacheEntry *tentry = (TypeCacheEntry *)arg;
	PGFunction cmp_fn = tentry->cmp_proc_finfo.fn_addr;
	//TODO correct collation?
	//TODO cache?
	Oid	collation = get_typcollation(tentry->type_id); //tentry->rng_collation;
	Datum cmp = DirectFunctionCall2Coll(cmp_fn, collation, e->data, n->data);
	return DatumGetInt32(cmp);
}

/* combining HNodes sums their counts*/
static inline void
hnode_combine(RBNode *existing, const RBNode *newdata, void *arg)
{
	HistNode *e = (HistNode *)existing;
	HistNode *n = (HistNode *)newdata;
	//FIXME overflow check?
	e->count = e->count + n->count;
	return;
}

static inline RBNode *
hnode_alloc(void *arg)
{
	return palloc(sizeof(HistNode));
}

static inline HTree *
htree_create(TypeCacheEntry *tentry)
{
	HTree *h_tree = palloc(sizeof(HTree));
	h_tree->num_elements = 0;
	RBTree *rb_tree = rb_create(sizeof(HistNode), hnode_compare, hnode_combine, hnode_alloc, NULL, tentry);
	h_tree->tree = rb_tree;
	return h_tree;
}

static inline HistNode *
htree_insert(HTree *hist, Datum data)
{
	bool is_new = false;
	HistNode	new;
	new.node.color = 0;
	new.node.left = NULL;
	new.node.right = NULL;
	new.node.parent = NULL;
	new.data = data;
	new.count = 1;
	hist->num_elements += 1;
	return (HistNode *)rb_insert(hist->tree, (RBNode *)&new, &is_new);
}

static inline bool
htree_median(HTree *hist, Datum *median0, MedianResult *median1)
{
	bool has_two = (hist->num_elements % 2) == 0;
	uint64 mid;
	uint64 seen = 0;
	RBTreeIterator iter;
	if (has_two)
	{
		mid = (hist->num_elements / 2);
	}

	else
	{
		mid = (hist->num_elements / 2) + 1;
	}

	rb_begin_iterate(hist->tree, LeftRightWalk, &iter);
	while(seen < mid)
	{
		HistNode *node = (HistNode *)rb_iterate(&iter);

		if (seen + node->count >= mid)
		{
			//TODO copy datum?
			*median0 = node->data;
		}
		seen += node->count;
	}

	if (has_two)
	{
		if (seen >= mid + 1)
		{
			median1->data = *median0;
		}

		else {
			HistNode *node = (HistNode *)rb_iterate(&iter);
			Assert(seen + node->count >= mid + 1);
			median1->data = node->data;
		}
	}

	return has_two;
}


/*********/
/*********/
/*********/


/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 *
 * median(state, val) => state
 *
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext	agg_context;
	MemoryContext	oldcontext;
	Pointer	state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	HTree	*hist;
	TypeCacheEntry	*tentry;
	Oid				element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	oldcontext = MemoryContextSwitchTo(agg_context);

	if (!OidIsValid(element_type))
        elog(ERROR, "could not determine data type of input");

	//TODO check entry/cmp_fn is valid
	tentry = lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO);

	if (state == NULL)
		state = (Pointer)htree_create(tentry);

	if (!PG_ARGISNULL(1))
	{
		Datum	val_datum = PG_GETARG_DATUM(1);
		hist = (HTree *) state;
		htree_insert(hist, val_datum);
	}

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 *
 *
 *
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
	MemoryContext	oldcontext;
	Oid				element_type = get_fn_expr_rettype(fcinfo->flinfo);
	HTree	*hist;
	Datum	median;
	bool	has_two;
	Datum	median0_datum = CharGetDatum(0);
	MedianResult	median1_res;
	median1_res.nothing = 0;


	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	oldcontext = MemoryContextSwitchTo(agg_context);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	hist = (HTree *)PG_GETARG_POINTER(0);

	if (hist == NULL)
		PG_RETURN_NULL();

	has_two = htree_median(hist, &median0_datum, &median1_res);
	if(has_two)
	{
		// median = (DatumGetInt32(median0_datum) + DatumGetInt32(median1_res.data)) / 2;
		median = median0_datum;
	}

	else
	{
		median = median0_datum;
	}

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_DATUM(median);
}
