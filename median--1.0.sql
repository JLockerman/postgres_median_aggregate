CREATE OR REPLACE FUNCTION _median_transfn(state internal, val anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_transfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_finalfn(state internal, val anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'median_finalfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_invfn(state internal, val anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_invfn'
LANGUAGE C IMMUTABLE;

DROP AGGREGATE IF EXISTS median (ANYELEMENT);
CREATE AGGREGATE median (ANYELEMENT)
(
    sfunc = _median_transfn,
    stype = internal,
    finalfunc = _median_finalfn,
    finalfunc_extra,
    mstype = internal,
    msfunc = _median_transfn,
    minvfunc = _median_invfn,
    mfinalfunc = _median_finalfn,
    mfinalfunc_extra
);
