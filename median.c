/*-------------------------------------------------------------------------
 *
 * median.c
 *
 * Provides median aggregate function and moving-window variant.
 *
 * This works by keeping a sparse histogram as a red-black tree of
 * (datum, count) in transition and inverse and iterating thorough
 * this until the middle element ins found in median.
 *
 *-------------------------------------------------------------------------
 */

#include <postgres.h>
#include <fmgr.h>
#include <lib/rbtree.h>
#include <catalog/pg_type.h>
#include <utils/datum.h>
#include <utils/typcache.h>
#include <utils/lsyscache.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* RedBlack Tree specialization for use as a histogram */

typedef struct HTree
{
	RBTree	   *tree;
	uint64		num_elements;
	int			typ_len;
	bool		typ_by_val;
} HTree;

typedef struct HistNode
{
	RBNode		node;
	Datum		data;
	uint64		count;
} HistNode;

typedef struct HistMeta
{
	PGFunction	cmp_fn;
	Oid			collation;
} HistMeta;

/* HNodes are compared based on data */
static inline int
hnode_compare(const RBNode *existing, const RBNode *newdata, void *arg)
{
	HistNode   *e = (HistNode *) existing;
	HistNode   *n = (HistNode *) newdata;
	HistMeta   *meta = (HistMeta *) arg;
	Datum		cmp = DirectFunctionCall2Coll(meta->cmp_fn, meta->collation, e->data, n->data);

	return DatumGetInt32(cmp);
}

/* combining HNodes sums their counts */
static inline void
hnode_combine(RBNode *existing, const RBNode *newdata, void *arg)
{
	HistNode   *e = (HistNode *) existing;
	HistNode   *n = (HistNode *) newdata;

	/* FIXME overflow check? */
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
	HTree	   *h_tree = palloc(sizeof(HTree));
	HistMeta   *h_meta = palloc(sizeof(HistMeta));

	h_tree->num_elements = 0;
	h_tree->typ_len = tentry->typlen;
	h_tree->typ_by_val = tentry->typbyval;

	h_meta->cmp_fn = tentry->cmp_proc_finfo.fn_addr;
	h_meta->collation = get_typcollation(tentry->type_id);

	RBTree	   *rb_tree = rb_create(sizeof(HistNode), hnode_compare, hnode_combine, hnode_alloc, NULL, h_meta);

	h_tree->tree = rb_tree;
	return h_tree;
}

/*
 * inserting an element into the histogram increments the
 * number of elements stored in the histogram and that element's count
 */
static inline HistNode *
htree_insert(HTree *hist, Datum data)
{
	bool		is_new = false;
	HistNode	new;
	HistNode   *ret;

	new.node.color = 0;
	new.node.left = NULL;
	new.node.right = NULL;
	new.node.parent = NULL;
	new.data = data;
	new.count = 1;
	hist->num_elements += 1;
	ret = (HistNode *) rb_insert(hist->tree, (RBNode *) &new, &is_new);
	if (is_new)
	{
		/*
		 * to ensure the tree elements live long enough, we transfer new
		 * elements into our context here.
		 */
		ret->data = datumTransfer(ret->data, hist->typ_by_val, hist->typ_len);
	}
	return ret;
}

/*
 * removing an element from the histogram simply decrements that count
 * and the number of elements in the histogram
 */
static inline bool
htree_remove(HTree *hist, Datum data)
{
	HistNode	to_find;
	HistNode   *found;

	to_find.data = data;

	found = (HistNode *) rb_find(hist->tree, (RBNode *) &to_find);

	if (found != NULL && found->count > 0)
	{
		found->count -= 1;
		hist->num_elements -= 1;
		return true;
	}
	return false;
}

static inline uint64
htree_num_elements(HTree *hist)
{
	return hist->num_elements;
}

/*
 * to find the median we perform an in-order transversal of the rb-tree
 * stopping when we reach the middle element
 * (num_elements / 2 for even num_elements,
 *  (num_elements / 2) + 1 for odd)
 */
static inline Datum
htree_median(HTree *hist)
{
	Datum		median;
	uint64		mid;
	uint64		seen = 0;
	RBTreeIterator iter;

	if ((hist->num_elements % 2) == 0)
	{
		mid = (hist->num_elements / 2);
	}

	else
	{
		mid = (hist->num_elements / 2) + 1;
	}

	rb_begin_iterate(hist->tree, LeftRightWalk, &iter);
	while (seen < mid)
	{
		HistNode   *node = (HistNode *) rb_iterate(&iter);

		if (node == NULL)
			elog(ERROR, "Internal Error, invalid histogram");

		median = node->data;
		seen += node->count;
	}

	/*
	 * post condition: (seen - node->count < mid) and (seen + node->count >=
	 * mid) so node->data is the middle element
	 */

	return median;
}


/*********/
/*********/
/*********/

PG_FUNCTION_INFO_V1(median_transfn);

/*
 * Median state transfer function.
 *
 * This function adds elements to the sparse histogram,
 * initializing the histogram on the first call.
 *
 * median(HTree*, val) => HTree*
 *
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
	MemoryContext oldcontext;
	Pointer		state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	HTree	   *hist;
	TypeCacheEntry *tentry;
	Oid			element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	/*
	 * The RBTree root is allocated using palloc, so we switch contexts here
	 * to ensure its lifetime is correct. The other functions do not allocate,
	 * so they do not switch contexts.
	 */
	oldcontext = MemoryContextSwitchTo(agg_context);

	if (!OidIsValid(element_type))
		elog(ERROR, "could not determine data type of input");

	/* TODO check entry/cmp_fn is valid */
	tentry = lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO);

	if (state == NULL)
		state = (Pointer) htree_create(tentry);

	if (!PG_ARGISNULL(1))
	{
		Datum		val_datum = PG_GETARG_DATUM(1);

		hist = (HTree *) state;
		htree_insert(hist, val_datum);
	}

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_POINTER(state);
}

/*********/
/*********/
/*********/

PG_FUNCTION_INFO_V1(median_invfn);

/*
 * Median inverse function.
 *
 * This function removes an element from the sparse histogram.
 *
 * median(HTree*, val) => HTree*
 *
 */
Datum
median_invfn(PG_FUNCTION_ARGS)
{
	Pointer		state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	Datum		val_datum = PG_GETARG_DATUM(1);

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "median_invfn called in non-aggregate context");

	if (state == NULL)
		elog(ERROR, "median_invfn called before median_transfn");

	htree_remove((HTree *) state, val_datum);

	PG_RETURN_POINTER(state);
}

/*********/
/*********/
/*********/

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * Find the median of the histogram
 * by iterating through it until the middle element.
 *
 * median(HTree*, ...) => val
 *
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
	HTree	   *hist;
	Datum		median;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	hist = (HTree *) PG_GETARG_POINTER(0);

	if (hist == NULL)
		PG_RETURN_NULL();

	if (htree_num_elements(hist) == 0)
		PG_RETURN_NULL();

	median = htree_median(hist);
	median = datumTransfer(median, hist->typ_by_val, hist->typ_len);

	PG_RETURN_DATUM(median);
}
