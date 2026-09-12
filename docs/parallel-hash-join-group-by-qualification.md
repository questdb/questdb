# Parallel hash join / group by: V1 qualification

> Completed-V1 measurements and the current rollout decision are in the
> [task 10 report](parallel-hash-join-group-by-v1.md). This report retains its historical task scope.

[RFC 130](https://github.com/questdb/rfc/discussions/130), task 9.

Task 9 adds storage, concurrency and resource qualification for experimental
keyed and scalar execution. It changes tests and documentation; the qualified
engine implementation is task 8's `0c940672cd`. Both configuration gates and the
positive query-worker requirement remain in place. The experimental flag remains
false by default. Completed-V1 benchmarking and any separate default-enablement
change are task 10.

## Storage and capability coverage

`HashJoinGroupByQualificationTest` compares type-annotated results with SQL
compiled with fusion disabled. Positive cases assert fused selection, and
negative cases assert exact ordinary-plan equality with the experimental flag
enabled. Each successful candidate execution is repeated on the same factory.

| Published capability | Positive and negative evidence |
| --- | --- |
| INT equality and join orientation | Seeded INNER, LEFT and normalized RIGHT queries, with either table as the physical probe; zero, negative, null and duplicate keys. Ordinary-plan assertions for LONG/SYMBOL/expression keys, cross/temporal joins and cross-input residuals. Existing candidate/planner tests cover composite keys and additional joins. |
| Native and Parquet storage | Both inputs cycle through native, one converted probe partition, mixed partitions on both sides, all Parquet and back to native, reusing ten keyed/scalar factories across five join orientations. Two-row Parquet row groups exercise multiple decoded groups. |
| Typed logical access | All published payload types, including nanosecond TIMESTAMP precision, are read on both inputs. Keyed output groups by the typed columns; scalar predicates exercise the remaining getters. Column tops precede inserted values; an additional matrix adds DOUBLE/SYMBOL columns after conversion, so the old Parquet files have no corresponding columns. SHORT-to-INT keys, FLOAT-to-DOUBLE values and INT-to-LONG columns are tested with mixed partitions and logical decoding. |
| Filters and cached format guards | Keyed/scalar INNER/LEFT/normalized RIGHT queries run with JIT enabled and disabled, probe predicates and timestamp interval scans. Conversion invalidates a native-only child filter; the test asserts the ordinary stale-plan exception, released slots and query registration, successful recompilation for mixed/all-Parquet data, and reuse of the original factory after conversion back to native. Fusion uses interpreted logical probe getters after filter takeover. |
| Symbols, binds and source validity | Both keyed/scalar paths rebind numeric and string variables, truncate/repopulate symbol dictionaries, then execute with empty builds. Schema invalidation on either source requests recompilation and cleans up. Existing planner tests also combine storage conversion, symbol replacement and rebinding. |
| Excluded access paths and values | Ordinary-plan equality for indexed probe access, latest-by, unions, table functions, LIMIT and DISTINCT barriers, unsafe preserved-side ON and cross-input WHERE. Referenced STRING, VARCHAR, BINARY, UUID, LONG256, DECIMAL, ARRAY and geohash values retain ordinary plans; unreferenced columns preserve fused eligibility. Internal LONG128/RECORD and all geohash widths are checked at the capability boundary. |
| Aggregates and expressions | Existing planner/function tests cover every exact allowed SUM/AVG DOUBLE and COUNT INT/LONG/DOUBLE/SYMBOL implementation, result types, expressions, aliases, final projection/order/limit, empty-input causes, null extension and rejection of unsupported compiled functions. |
| Selection controls | Existing configuration/planner matrices cover both flags at zero/one/four configured workers, keyed/scalar INNER/LEFT/RIGHT, EXPLAIN without execution, and larger eligible builds without a cutoff. |

The seeded differential test uses `Rnd(130, 9)`: **24 data sets × 3 worker
counts × 5 orientations × 2 aggregation modes = 720 SQL cases**, each with two
candidate executions. Input sizes, key domains, skew, duplicates, nullable
arguments, group cardinality, frame sizes, filter placement and selectivity vary.
The fixed matrix also includes empty probe/build, predicates rejecting all pairs,
null-accepting outer WHERE, and safe build-only ON filtering. Binary-fraction
values keep these comparisons exact. The existing aggregate/type matrix covers
all-null inputs, both inputs empty, no matches and the other distinct outer
empty-input causes. Every positive SQL case asserts selection to prevent an
unintended ordinary plan from hiding a failure.

## Concurrency, failure and memory coverage

`HashJoinGroupByConcurrentTest` runs three independently compiled queries against
a shared engine and queues, with separate contexts and cancellation tokens.
A barrier requires all three builds to be live before probing. The matrix covers
owner-only work stealing, real legacy workers, real fiber workers, native/mixed
Parquet probes and owner/sharded aggregation. It performs **144 executions**, of
which **12 are deliberately cancelled**. Peer queries must complete correctly,
and the same cancelled factory must succeed on the next execution. Output is
reread and partially consumed before close. Every iteration checks released
slots and query registration; the worker-pool helper checks shutdown and idle
readers/writers, and the enclosing helper checks native-memory leaks.

The existing acquired-slot gates separately prove simultaneous probe and shard
merge activity, so concurrency coverage does not depend solely on scheduler
luck in the SQL workload. Shared dispatcher regressions cover fiber scheduling,
cancellation and work stealing.

| Phase or lifecycle | Qualification evidence |
| --- | --- |
| Build consumption | New keyed/scalar tests inject source failures before, during and near the end of consumption. They assert exactly one cursor acquisition/close, the exact read count, no rewind/replay, no probing, zero tracked allocations and successful reuse after every failure. |
| Build cancellation | New keyed/scalar factory tests cancel at six build checkpoints each and verify zero tracked allocations, no probing, released slots and successful reuse after each cancellation. `IntHashJoinBuildTest` additionally interrupts every build check, including growth, copying, dictionary work and freeze. |
| Build memory | Existing byte-limit sweep and explicit hash-rehash/duplicate-payload overlap tests cover hash slots, duplicate storage, symbol indexes and text allocations, with cleanup/reopen. Integrated factory tests fail build allocation and then successfully reuse the factory, including keyed/scalar INNER/LEFT/normalized RIGHT plans whose swapped build exceeds the registered query limit. |
| Initialization | Existing keyed/scalar function-initialization faults verify execution-state release and reuse; function-boundary tests cover owner state donation, slot-local functions and SYMBOL initialization. |
| Decoding | The acquired-slot Parquet allocation-failure fixture now covers scalar execution as well as keyed execution. Failure releases the slot and every query allocation; the same factory then succeeds against Parquet and native partitions. |
| Probe and slot state | Existing gates inject worker failures and cancellation inside a 100,000-entry duplicate chain. Keyed map growth and scalar-slot allocations fail while the shared build remains live, proving the combined state is charged. New keyed/scalar cases also cancel an empty-build all-miss scan and inject a timeout at pair 32 of a 100,000-entry duplicate chain, then reexecute successfully. Each fixture drains tasks, checks released slots/allocations and reuses the factory. |
| Merge | Existing owner and sharded tests cover injected exceptions, cancellation, destination allocations overlapping live sources, sharded redistribution and recovery. Scalar tests validate intermediate AVG sum/count merging, merge exceptions and cancellation. |
| Output/close | Existing keyed/scalar fixtures cover compile-and-close, close before dispatch, partial output, size/remaining size, toTop and reuse. Keyed owner/sharded output tests additionally exercise every row through random access; scalar output advertises no random access. |

Memory breaches preserve normal query memory errors. There is no build-size
fallback, consumed-input replay, or default-enablement change in this task.

## Validation and reproduction

**1,790 tests passed across 41 suites, with three existing conditional skips
and zero failures/errors** (1,793 total). This combines the 40-suite regression
run, the final rerun of changed qualification/fault/planner suites, and all 50
dynamic configuration tests; repeated cases are counted once. Fourteen new test
methods and the expanded planner memory-limit matrix passed. The benchmark
package build also passed. The three skips
were the Windows-only import-path case, the explicitly unstable-order codegen
case, and the DECIMAL filter case when the existing randomized suite chose
Parquet.

The regression command covers planner/code generation, candidate/function
contracts, joins, grouping/merging, query accounting, configuration, storage and
worker scheduling. Run with JDK 25 and Maven 3:

```bash
mvn -pl core test \
  -Dtest=HashJoinGroupByQualificationTest,HashJoinGroupByConcurrentTest,HashJoinGroupByPlannerTest,AsyncHashJoinGroupByTest,HashJoinGroupByFunctionsTest,HashJoinGroupByCandidateTest,IntHashJoinBuildTest,SqlCodeGeneratorWorkerFunctionExtractionTest,SqlCodeGeneratorTest,SqlOptimiserTest,GroupByUtilsTest,GroupByRewriteTest,GroupByTest,HashJoinTest,JoinRecordMetadataTest,GroupByMapFragmentTest,AsyncFilterContextTest,AsyncFilteredRecordCursorFactoryTest,QueryRegistryMemoryTrackerTest,ParallelGroupByMemoryTrackerTest,ParallelHorizonJoinMemoryTrackerTest,PostAggregationCircuitBreakerTest,QueryParallelFiberDispatcherTest,HorizonJoinTest,GroupByFunctionsUpdaterFactoryTest,SortedRunsMergeTest,AvgDoubleGroupByFunctionFactoryTest,CountSymbolGroupByFunctionFactoryTest,CountTest,CountColumnTest,OrderedMapTest,Unordered4MapTest,Unordered8MapTest,PropServerConfigurationTest,ParquetColumnTypeConversionTest,ParquetRowGroupPruningTest,ParquetMemoryTrackerTest,ParallelParquetMemoryTrackerTest,ReadParquetCancellationTest,ParallelFilterTest
mvn -pl core test -Dtest=DynamicPropServerConfigurationTest
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
```

The dynamic configuration suite needs free server ports. A local server occupied
the defaults during this run, so its test source temporarily used these
`getBootstrapConfig()` environment overrides: HTTP `127.0.0.1:9001`, HTTP-min
`127.0.0.1:19003`, PG `127.0.0.1:18812` and ILP/TCP `127.0.0.1:19009`, with the
PG test connection helper also using 18812. The original source was restored
byte-for-byte after the run. No server or production configuration was changed.

Task 7's keyed performance report remains historical. These correctness checks
make no new speedup claim and do not establish outer/scalar/Parquet performance.
Task 10 must rerun the fixed primary gate and complete the broader benchmark
matrix before proposing a separate rollout change.
