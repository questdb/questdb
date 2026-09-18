# Scalar row expiry and latest-by: scope and performance tradeoff

## Decision

Limit `SqlOptimiser.pushLatestByToTableModel()` to physical table reads that
`SqlParser.expandExpiringTable()` introduces for a scalar `EXPIRE ROWS WHEN`
policy. This is a compatibility rewrite for expiry expansion, not a general
latest-by performance optimisation or a cost-based plan choice.

The parser marks that physical read with `isScalarExpiryRead`. It does not mark
ordinary tables, ordinary view expansion, materialized views without a policy,
or the relative/window policy expansions. An ordinary view or CTE that refers
to a scalar-expiry view can still contain a marked read; the existing shape and
semantic guards must also accept the path to it. Merely mentioning an expiring
view elsewhere in a join or UNION does not opt other reads into the rewrite.

The marker records parser provenance, rather than looking up the current policy
again during optimisation. `QueryModel.clear()` resets it when the model pool
reuses a model. It is separate from `isExpiryKeepFilter`: the latter protects
nullable predicates from Boolean inversion and deliberately excludes timestamp
predicates that the optimiser can turn into intervals. Those predicates still
need the latest-by rewrite.

## Why keep the rewrite for scalar expiry?

Scalar expiry turns a reference to `mv` into
`SELECT * FROM mv WHERE <keep-filter>`. A latest-by query now sees a subquery
instead of a physical table. The random-access `LatestByLightRecordCursorFactory`
returns its winning rows in key-map insertion order, not timestamp order, and
correctly advertises no designated timestamp.

The direct-table latest-by factories return rows in timestamp order. Hoisting
the synthetic wrapper recovers that contract, so adding a scalar expiry policy
does not by itself break `SAMPLE BY` or timestamp joins above a previously valid
direct latest-by read. This must work for INT and other keys, non-indexed SYMBOL
keys, and indexed SYMBOL keys alike. Advertising a timestamp on unordered light
output would instead permit incorrect sampling, joins or ORDER BY elision.

The existing guards still reject transformations that change timestamp choice,
projection, alias scope, filters, joins, limits or ordering. Live views remain
excluded: direct latest-by currently reads their disk tier without the published
in-memory lead. Relative/window policies retain their existing restrictions on
timestamp-dependent operators above another latest-by; this change does not
extend those semantics.

## What improves, and what we give up

Ordinary nested table queries, ordinary views over tables, and nested reads of
materialized views without scalar expiry retain their pre-hoist plans. In
particular, a selective predicate can continue to use async/JIT filtering before
`LatestBy light`, rather than moving into a scalar latest-by scan. The restriction
also deliberately withdraws the broad indexed-subquery speedup and any extra
timestamp-dependent query shapes that only the general-purpose hoist enabled.
Direct-table latest-by remains unchanged.

Scalar-expiry reads can still benefit from index access, timestamp interval
pruning, or SYMBOL-key early termination, depending on the selected factory and
predicate. An index's mere presence does not guarantee an index-seek plan.

## Accepted residual cost

For a hoisted scalar-expiry read, `generateLatestByTableQuery()` consumes the
residual predicate. The later generic filtering stage cannot install an
async/JIT filter below it. For example, `LatestByAllFilteredRecordCursor` scans
backward and invokes `filter.getBool(record)` row by row.

A selective full-scan query over an expiring view can therefore remain slower
than the equivalent filtered subquery with the hoist disabled. This includes a
query that does not itself need timestamp-ordered output. The restriction
contains that plan change to scalar-expiry expansion; it does not eliminate this
cost or promise performance parity for every expiry query. The cost is a chosen
implementation limitation, not a claim that expiry semantics inherently require
scalar filtering.

The PR discussion at revision `5204d37e82` reported approximately 6 ms without the
hoist versus 15-18 ms with it for a selective filtered query, and approximately
96 ms versus 1-2 ms in the opposite direction for an unfiltered indexed query.
Those are historical measurements of the broad hoist, not new measurements of
this scoped change or predictions for every expiry workload. See the
[performance review](https://github.com/questdb/questdb/pull/7263#issuecomment-5379388414)
and the earlier
[ordering correction](https://github.com/questdb/questdb/pull/7263#issuecomment-5354023131).

We accept the residual expiry-only cost in this change to retain the timestamp
contract without extending the performance regression to unrelated workloads.
Disabling hoisting for all predicates would undo that contract. Restricting it
to indexed keys would make query validity depend on an index again. Neither is
a complete replacement for this scope decision.

## Follow-up to close the remaining gap

Add an execution path that combines async/JIT residual filtering with latest-by
reduction and genuinely timestamp-ordered output. Preserve interval/index
restriction and SYMBOL early exits instead of replacing every query with a full
scan. A candidate can sort only the winning rows before replay, but that adds
work proportional to the number of keys and is not automatically faster. A
frame-oriented implementation that consumes filtered matches in timestamp order
is another option.

Filtering must precede winner selection. If A's newest row fails the predicate
and an older row passes, the answer must contain the older row. NULL policy
semantics, NULL keys, equal timestamps, timestamp precision, cancellation and
native-memory ownership must also survive the new execution path.

Benchmark candidates against both existing paths across:

- Indexed/non-indexed SYMBOL, INT and composite keys; low and high cardinality.
- Selective, broad, absent and non-JIT-compatible residual predicates.
- Timestamp intervals and indexed key restrictions, with and without residuals.
- Early termination near the newest rows versus missing keys requiring a full scan.
- JIT on/off, supported platforms, one/multiple workers, and empty inputs.
- Plain latest-by, explicit ORDER BY, and downstream SAMPLE BY / ASOF / LT / SPLICE.

Report regressions alongside improvements before widening the scope. Do not
restore designated-timestamp metadata without verifying actual output order.

## Validation of this scope change

`LatestByHoistEquivalenceTest` compares enabled/disabled plans, rows, projections
and timestamp metadata. Ordinary shapes must keep identical plans; accepted
scalar-expiry shapes must actually change plans. Its selective-filter regression
covers INT, indexed SYMBOL and composite keys, ordinary subqueries and SQL views,
JIT enabled/disabled, NULL keys and filtering before latest-by. The scope tests
fail when the provenance gate is removed.

The tests also cover model-pool reuse, a mixed ordinary/expiry UNION, materialized
views without expiry, and installing/dropping a flippable timestamp policy.
`MatViewExpireRowsTest` covers downstream SAMPLE BY and ASOF without an index.
`LatestByTest`, `ExplainPlanTest` and the live-view read tests pin the ordinary
plans, ordering and freshness contracts. These are plan/correctness assertions,
not wall-clock performance guarantees.
