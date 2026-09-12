# Fused hash join planner and diagnostics

[RFC 130](https://github.com/questdb/rfc/discussions/130), tasks 6 and 6a.

## Selection and controls

`cairo.sql.parallel.hash.join.groupby.enabled` is experimental and defaults to
`false`. Its environment equivalent is
`QDB_CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_ENABLED`. Both this property and
`cairo.sql.parallel.groupby.enabled` must be true, and the SQL execution context
must have a positive shared query-worker count. The context's global parallel
GROUP BY switch continues to gate selection after runtime setter changes.

| Global GROUP BY | Experimental fused flag | Query workers | Eligible keyed plan |
| --- | --- | --- | --- |
| false | either | any | Ordinary join and aggregation |
| true | false | any | Ordinary join and aggregation |
| true | true | zero | Ordinary join and aggregation |
| true | true | positive | Async Hash Join Group By |

The flag is wired through property parsing, default and wrapped Cairo configuration,
SQL execution contexts and the generic test property overrides. Positive slot counts
support owner work stealing even when consumer threads are not running. Worker
availability does not promise that every execution actually uses all workers.

The planner calls the candidate API before generating the ordinary join/group-by
pipeline. It verifies the compiled probe's frame/filter capabilities and the exact
aggregate/type allowlist, and selects only keyed aggregation. Shared cursor models retain their existing
ownership and plans; they are rejected before speculative child compilation. Unkeyed execution is
task 8. See the [capability table](parallel-hash-join-group-by.md). There is no build-size
cutoff, runtime fallback, or consumed-input replay.

## Filters, projections and ownership

Both children compile under the enclosing query registration and memory tracker.
Candidate base-column indexes are mapped through actual compiled projections;
pure projections above an asynchronous filter are peeled and their column maps
composed before filter takeover. Joined functions use the eventual frame coordinates
and copied build-payload order, including aliases and reordered RIGHT inputs.

Single-input WHERE conjuncts referencing the preserved physical probe are mapped
to its base table before child compilation. This retains timestamp interval scans
and ordinary row filters after RIGHT-to-LEFT normalization. Build-only residual ON
predicates filter the physical build cursor. Null-accepting build-side WHERE
predicates stay after matching/null extension. Rejected real matches never produce
replacement null-right rows. Final projection, ordering and limiting remain ordinary
operators above the grouped result.

Speculative construction keeps independent snapshots of WHERE and backup-WHERE
fields because child generation can overwrite the standard backups. Rejected
candidates restore those fields and close children/functions. Unsupported shape
and non-stealable probe-filter tests compare the complete ordinary plans and results.
With parallel filtering disabled, a remaining ordinary serial probe filter is not
stealable and keeps the ordinary join/aggregation path; build filters can still run
through their ordinary cursor.

Worker filter clones compile while the original wrapper still owns the owner
filter and base. Only a successful `halfClose()` transfers those handles. Fused
probing evaluates interpreted filters through logical row getters; unused JIT and
bind-memory handles are released during transfer. The new filter context and fused
factory consume their resources on constructor entry, including failure.

## EXPLAIN

EXPLAIN reports configured workers, logical and physical join types, the input-swap
flag, qualified equality keys, `buildStrategy: shared`, grouping keys/functions,
aggregate functions, post-join and taken-over probe filters, and labelled Probe/Build
children. Child plans retain interval scans and build filters. The factory copies
only the joined metadata needed to render functions; it retains no candidate models.
EXPLAIN does not acquire the build cursor or populate execution metrics.

## Execution metrics and benchmark adapter

`AsyncHashJoinGroupByRecordCursorFactory.getMetrics()` exposes the last execution's
`HashJoinGroupByMetrics`. Read it after result computation. It resets at the next
cursor acquisition and remains available after cursor close; rereading the already
computed result does not recount input rows.

| Metric | Meaning |
| --- | --- |
| buildRows / buildKeys / buildBytes | Filtered build rows, distinct INT keys (including a matching null key), and retained frozen build bytes |
| scannedRows | Rows visited in dispatched probe frames, before the taken-over row filter; rows outside interval scans are excluded |
| matchedPairs | Equality-ON matches before post-join filtering, including every duplicate |
| nullExtendedRows | One candidate for each surviving probe-input row without an ON match in physical LEFT execution |
| survivingRows | Matched or null-extended candidates passing the post-join filter and reaching aggregate updates |
| mergeCardinality | Final grouping-map row count |
| buildNanos | Build-child cursor acquisition, filtering, copying/freezing and child cursor close |
| initNanos | Probe frame-cursor acquisition and execution function/slot initialization |
| probeNanos | Frame preparation, scheduling, decoding, probing and aggregate updates through worker drain |
| mergeNanos | Owner or sharded merge through merged-cursor preparation |

Workers increment counters only in acquired slots. The owner combines them after
probe completion. Phase timings are wall-clock durations, not summed worker CPU
time. Failed or incomplete executions may have partial metrics. Total latency still
comes from the runner's acquisition-through-final-ordered-consumption measurement,
which includes final projection/sort and consumer work.

`HashJoinGroupByBenchmark.PlannerCandidateCompiler` enables selection only while
compiling the candidate SQL, then restores the execution-context setting. The
existing runner alternates the ordinary/fused arms over the same data and checks
every ordered result. Its CSV now includes the metrics above alongside total time
and memory. Ordinary-arm phase/counter fields are empty.

Peak memory retains the runner's existing 1 ms **process native allocation delta**
sampler, starting before cursor acquisition and ending after final output. It includes
live build, probe and merge state plus temporary allocations that overlap a sample,
excludes mappings and Java heap, and can miss brief peaks or include shared-pool
allocations. It is not an exact per-query high-water mark. `buildBytes` is the frozen
build footprint, not peak query memory. Memory-limit fault tests independently
exercise the query tracker.

## Validation

The planner suite uses native-memory leak checks and ordinary SQL compilation.
It covers the full global/experimental flag matrix with zero, one and four configured
workers and INNER/LEFT/normalized RIGHT; the motivating ordered query; LIMIT;
interval/probe/build/post-join filters with JIT enabled and disabled; aliases and
filtered projections; unsupported shapes; 100,008 build rows; metrics, empty builds
and reuse; column tops, Parquet/mixed storage and logical conversion; bind/SYMBOL
rebinding; stale child plans; and registered-query memory failure/reuse.

Partition-format guards remain active. A filtered native child can request normal
recompilation after conversion to Parquet. Tests check cleanup, successful selection
on recompilation, and reuse of the original factory after restoring native storage.

Across 28 suites, **876 distinct cases: 853 passed, 23 existing conditional cases
skipped, zero remaining failures/errors**. The broad run first encountered an
existing QuestDB process on ports 9000/9003/9009/8812 in
`DynamicPropServerConfigurationTest`. Its 50 tests passed on a separate run with
temporary test-only bind overrides (HTTP 9001, PostgreSQL 18812, ephemeral min-HTTP
and line-TCP ports, plus the matching test PostgreSQL URL). The source was restored;
no server was stopped and no port change is included in the commit. The final
planner/candidate/lateral subset passed 69 cases with 11 conditional skips.

Regression scope and exact commands:

```bash
mvn -pl core test \
  -Dtest=HashJoinGroupByPlannerTest,AsyncHashJoinGroupByTest,HashJoinGroupByFunctionsTest,HashJoinGroupByCandidateTest,IntHashJoinBuildTest,SqlCodeGeneratorWorkerFunctionExtractionTest,GroupByUtilsTest,GroupByRewriteTest,HashJoinTest,JoinRecordMetadataTest,GroupByMapFragmentTest,AsyncFilterContextTest,AsyncFilteredRecordCursorFactoryTest,QueryRegistryMemoryTrackerTest,MapTest,OrderedMapTest,ShardedMapCursorTest,ParallelGroupByMemoryTrackerTest,ParallelHorizonJoinMemoryTrackerTest,PostAggregationCircuitBreakerTest,QueryParallelFiberDispatcherTest,HorizonJoinTest,PropServerConfigurationTest,DynamicPropServerConfigurationTest,WalApplySqlExecutionContextTest,MatViewRefreshSqlExecutionContextTest,LiveViewRefreshSqlExecutionContextTest
mvn -pl core test -Dtest=DynamicPropServerConfigurationTest
mvn -pl core test -Dtest=HashJoinGroupByPlannerTest,HashJoinGroupByCandidateTest,LateralJoinSharedCursorTest
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinGroupByBenchmark \
  --rows=100000 --plants=1000 --workers=4 --warmups=1 --runs=10 --repetitions=2 \
  --revision=task6-working-tree \
  '--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler'
```

The benchmark package passed. The [smoke report](parallel-hash-join-group-by-planner-smoke.txt)
retains both plans, generator/environment, ordered results and all 40 measured
samples across two alternating repetitions. Every result check passed. All 20
fused samples reported 100 build rows/keys, 8,320 retained build bytes, 100,000
scanned rows, 9,946 matched/surviving pairs, zero null extensions and 120 final
groups, with nonzero build/init/probe/merge timings.

The small smoke workload validates integration only. Task 7's primary 100-million-row,
repeatable 2× end-to-end performance gate remains pending; default enablement is
still a separate rollout decision.
