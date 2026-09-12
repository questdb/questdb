# Fused hash join scalar aggregation

[RFC 130](https://github.com/questdb/rfc/discussions/130), task 8.

## Execution and ownership

`AsyncHashJoinGroupByRecordCursorFactory` now handles both keyed and unkeyed
aggregation. The same immutable build, logical frame getters, acquired execution
slots, input/post-join filters, duplicate enumeration and outer null extension
serve both forms. No grouping sink, grouping map, map fragment or sharding context
is created for unkeyed execution.

Each owner/worker slot allocates a `SimpleMapValue` during execution initialization,
after the build is frozen. The new tracker-aware constructor charges its native
storage to the current query; the original constructor keeps existing callers'
allocation behavior. Each intermediate value column occupies 32 bytes. This fixed
state lives alongside the build until cursor close and is released before the
query tracker, including when a later slot allocation or function initialization
fails. Compilation allocates no scalar native state. The V1 aggregate allowlist
requires no separate aggregate allocator.

The existing updater initializes empty aggregate values and writes the first
surviving joined pair with `updateNew`; later pairs use `updateExisting`. Slots
retain a separate empty/nonempty marker. After probe tasks drain, the owner copies
the first nonempty partial if necessary and merges subsequent partials through
`GroupByFunctionsUpdater`. SUM and AVG retain their existing intermediate state;
AVG merges sums/counts, including unequal partial counts. Empty slots are skipped.
Merge checks cancellation between slots and before exposing the final value.

The cursor produces exactly one row. `size()` is one before and after computation,
`calculateSize()` counts only remaining output, and `toTop()` rereads the computed
value without another build or probe. Unkeyed execution does not advertise random
access or input ordering. Close cancels/drains the shared frame sequence before
releasing scalar state, frame resources and build backing. A new execution starts
with fresh values, counters, functions and build dictionaries.

## Selection and SQL semantics

Both `cairo.sql.parallel.groupby.enabled` and
`cairo.sql.parallel.hash.join.groupby.enabled` must permit selection, with positive
configured query-worker slots. The experimental flag remains false by default.
Owner-only work stealing remains supported when slots are configured but consumer
threads are absent. EXPLAIN uses the existing `Async Hash Join Group By` operator
and adds `aggregation: scalar`; it retains orientation, filters and actual children
without building data. `mergeCardinality` reports one for a finalized scalar row,
even when no joined pairs survive filtering.

The declared allowlist is unchanged:

| Aggregate | Argument type | Result type |
| --- | --- | --- |
| `sum`, `avg` | DOUBLE | DOUBLE |
| `count(*)`, `count()` | No argument | LONG |
| `count(expr)` | INT, LONG, DOUBLE, SYMBOL | LONG |

Both probe and build arguments use the same compiled implementations as keyed
execution. Unsupported overloads, DISTINCT, order-sensitive aggregates and custom
implementations outside the exact class allowlist remain excluded. Final scalar
expressions, aliases, ordering and limits stay in their ordinary parent operators.

| Cause | INNER | LEFT / normalized RIGHT |
| --- | --- | --- |
| Empty build or no equality matches | Empty aggregate input | Aggregate surviving preserved rows with null build values |
| Empty probe, including both inputs empty | Empty aggregate input | Empty aggregate input |
| Input/post-join filters reject every candidate | Empty aggregate input | Empty aggregate input |
| All ON matches fail WHERE | No input from those probe rows | No replacement null-extended row |

Empty aggregate input returns zero counts and null SUM/AVG. Null extension counts
once in `count(*)`, contributes zero to counts of null build arguments, and still
runs expressions such as `coalesce`. Duplicate matches contribute once per pair.
Null INT equality keys retain QuestDB's existing matching behavior.

## Validation

**651 tests passed across 26 suites, with zero failures, errors or skips.**
This includes ten new test methods plus expanded planner/differential coverage.
The benchmark package build passed. The 100,000-row/four-worker keyed smoke
comparison passed all 43 explicit result checks, including all 40 measured
executions. This smoke check validates integration, not the primary performance gate.

`HashJoinGroupByPlannerTest` compares ordinary and fused plans, result values and
result types. Its aggregate/type matrix contains 96 keyed/scalar cases: eight
input scenarios, INNER/LEFT/normalized RIGHT, and one/four configured workers.
Scenarios include mixed matches/misses and duplicate fanout, all-null arguments,
empty build, no matches, empty probe, post-join rejection, probe-input rejection,
and both inputs empty. Every case executes the same factory twice. Binary-fraction
values keep these result comparisons exact. Separate concurrent cases exercise
partial merges with nonuniform values and counts.

The global/experimental flag matrix now covers keyed, scalar aggregate projections
and count-only SQL at zero/one/four workers. Additional tests cover filtered input
aliases and reordered projections, normalized RIGHT filters and interval scans,
null-accepting outer predicates, final ratio/order/limit, scalar EXPLAIN/metrics,
compile-and-close, pre-dispatch close, partial-output close, cursor reread and
remaining-size contracts, dictionary changes and factory reuse.

`AsyncHashJoinGroupByTest` extends the acquired-slot concurrency and fault fixtures
to scalar updates. Tests demonstrate concurrent probes and merging, cancellation
inside a 100,000-entry duplicate chain, function initialization/probe/merge failures,
merge cancellation, released slots, and successful reuse. A memory-limit test lets
the build fit but fails a later scalar allocation while earlier slots are live;
all query allocations return to zero, and the factory succeeds after lifting the
limit. Shared keyed execution, owner/sharded merging, storage and scheduler tests
remain in the regression set.

These checks complete task 8. The broader randomized/storage/concurrent-query and
resource qualification is task 9. Task 7's keyed gate is historical; completed-V1
benchmarks and any default-enablement decision remain task 10.


Reproduce with JDK 25 and Maven 3:

```bash
mvn -pl core test \
  -Dtest=HashJoinGroupByPlannerTest,AsyncHashJoinGroupByTest,HashJoinGroupByFunctionsTest,HashJoinGroupByCandidateTest,IntHashJoinBuildTest,SqlCodeGeneratorWorkerFunctionExtractionTest,GroupByUtilsTest,GroupByRewriteTest,GroupByTest,HashJoinTest,JoinRecordMetadataTest,GroupByMapFragmentTest,AsyncFilterContextTest,AsyncFilteredRecordCursorFactoryTest,QueryRegistryMemoryTrackerTest,ParallelGroupByMemoryTrackerTest,ParallelHorizonJoinMemoryTrackerTest,PostAggregationCircuitBreakerTest,QueryParallelFiberDispatcherTest,HorizonJoinTest,GroupByFunctionsUpdaterFactoryTest,SortedRunsMergeTest,AvgDoubleGroupByFunctionFactoryTest,CountSymbolGroupByFunctionFactoryTest,CountTest,CountColumnTest
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinGroupByBenchmark \
  --rows=100000 --plants=1000 --workers=4 --warmups=1 --runs=10 --repetitions=2 \
  --revision=task8-working-tree \
  '--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler'
```
