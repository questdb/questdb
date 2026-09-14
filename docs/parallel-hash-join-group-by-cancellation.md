# Fused hash join circuit-breaker audit (RFC task 9d)

Task 9d adds checks to delegated filtered build scans, frame preparation,
Parquet row-group traversal, sparse map cursors and final output. The keyed and
scalar reducer checks rejected/missed probe rows at frame boundaries and retains
throttled duplicate-pair checks. The later boundary simplification removes polling
inside hash-collision walks, map redistribution and map merge loops.

The experimental flag remains false. The [shared-throttle follow-up](parallel-hash-join-group-by-throttling.md)
replaces task 9f's operator-specific counters with the standard
`statefulThrowExceptionIfTripped()` API. The [boundary-check follow-up](parallel-hash-join-group-by-breaker-boundaries.md)
aligns probe and merge polling with the existing parallel GROUP BY factories.
The loop and ownership audit below reflects that implementation; the validation
section retains the original task 9d results.
Task 10 must repeat the full rollout matrix on the completed implementation.

## Loop audit

`row` below means `statefulThrowExceptionIfTripped()`: a cheap counter with a
real check on its first call and at the configured throttle. `time` means
`statefulThrowExceptionIfTrippedTimeThrottled()`: cancellation/timeout on each
call, with elapsed-time throttling of network connection checks. Neither API
allocates on successful checks. Work bounds describe loop iterations, not a
wall-clock deadline for an operating-system call or native codec invocation.

| Execution loop | Breaker and work bound |
| --- | --- |
| `AsyncHashJoinGroupByRecordCursorFactory.getCursor`, build acquisition | Owner query breaker bound before opening children; existing time check before build. |
| `IntHashJoinBuild.build` / `append` | Each consumed build row calls the active owner's standard throttled API; standalone append retains its row check. Known-size row reservation checks before allocation and retains tracked allocation/copy bounds. Null INT keys are counted; checks remain in rehash, SYMBOL hashing/comparison/copy and chunked native allocation/copy. Collision walks have no checks. |
| `FilteredRecordCursor.hasNext` | Newly bound owner row check before each predicate evaluation, including rejected rows hidden inside one `hasNext`. Binding is refreshed by every `of`. |
| `AsyncFilteredRecordCursorFactory.filter` | Acquired job/continuation wrapper; new row checks in both row-ID and count-only loops, before predicate rejection. The slot is released in `finally`. |
| `AsyncJitFilteredRecordCursorFactory.filter` | Same new row checks in the interpreted column-top/type-conversion branches. Native JIT evaluation has a time check before the call and processes one native frame or selected Parquet row-group range; it does not run a Java cursor loop. |
| `AsyncFilterAtom.preTouchColumns` | New row check per selected row, using the calling filter job's wrapper. Inner column loops are bounded by the compiled projection. |
| `FwdTableReaderPageFrameCursor.next` / `BwdTableReaderPageFrameCursor.next` | Owner breaker bound in `of`; new time checks per frame/partition attempt, including skipped frames. This also bounds the qualified children consumed by ordered/unordered sequence address-cache preparation. |
| Full/interval partition `next` and `calculateSize` | The query breaker binds in the partition factory/`of`; new time checks per partition/interval attempt, including empty partitions and nonintersecting ranges consumed inside one call. Interval frame-count estimation also checks per range. Direct non-query reader users keep a NOOP overload. |
| Forward/backward `computeParquetFrame` | New time checks during row-group seeking and pruning, including groups rejected before any frame is returned. |
| Frame navigation / Parquet decoding | Existing frame checks remain; decoding processes the requested frame/window through the existing native decoder. The audit does not promise interruption inside a single native codec call. |
| Fused reducer's outer row loop | The shared `UnorderedPageFrameReduceJob.reduce` checks the active query before each frame, as for parallel GROUP BY and Top-K. Rejected rows, all-miss INNER and LEFT null extension finish at frame granularity. There is no per-probe-row breaker or sequence-flag poll; the owner checks again before merging, including after the last frame. |
| Frozen lookup / duplicate traversal | Standalone `find` checks at lookup entry; the fused caller uses `findUnchecked` under the scheduler's frame check. Neither path checks collision steps. The existing `next()` checks each duplicate advance with the standard throttled API; no probe-owned counters remain. The sequence-active test remains inside every duplicate iteration, before post-join rejection or aggregate updates. |
| Grouping sink and aggregate updater | Called under frame/pair checks; per-record function and key loops are bounded by compiled query shape. Empty scalar initialization is bounded by configured slots/functions. |
| `GroupByMapFragment.shard` | Uses the ordinary unchecked redistribution loop and cursor. The scheduler checks before a reducer frame; final `shardAll` checks before each fragment. No per-entry or empty-slot check during redistribution. |
| `Unordered4MapCursor` / `Unordered8MapCursor` | Optional breaker binds before `init` calls `toTop`. New row checks per slot while skipping empty runs, including initial seek, between returned groups, final tail and reread. Ordinary cursor acquisition clears the binding. Cursor/map close also clears it, including failed initialization. |
| Owner map merge | Uses ordinary map merge loops with no per-slot/entry checks. The fused owner checks before and after result merging. Destination/source accounting and source-close ordering are unchanged. |
| Sharded map merge | Existing owner checks while publishing/waiting; shared `PostAggregationCircuitBreaker` propagates cancellation/errors to detached merge workers. Workers check the flag before each shard task, as in ordinary parallel GROUP BY. No breaker is passed into map merge loops. Every published task drains before the owner tears down shared execution storage. |
| Scalar merge | Existing row check per slot and the common unthrottled owner check after merging; no grouping cursor or data-dependent output traversal. |
| Fused output `hasNext` / `calculateSize` | New owner row checks after result construction. Keyed output binds the breaker to owner/sharded map cursors before any sparse initialization. Scalar output is at most one row; keyed size calculation uses map counts rather than walking rows. |
| Final projection, sorting and LIMIT | Existing parents consume the checked fused cursor. Random record lookup is constant work; `toTop` reuses the same execution binding and checked sparse seek. |
| Drain and cleanup | Intentionally finish even after cancellation: sequence `await`, shard-merge drain and slot/native-resource release must complete before frozen build, symbol views or maps are freed. Bounded schema/slot teardown loops do not throw fresh cancellation exceptions. |

## Ownership and throttling

The owner uses the execution context's active query breaker. Build work finishes
before the atom initializes each independently acquired slot's frozen lookup
view. Network breakers continue to be copied into wrappers, with the existing
fd, timeout, timer and generation-aware cancellation binding conventions.
The scheduler checks the query start time and propagates interruption reasons;
its mandatory drain protocol is unchanged.

A thread-safe atomic cancellation delegate has a mutable **single-threaded** row
counter. Sharing that counter among wrappers was unsafe. Wrappers now maintain
independent row counters for shared delegates and call their unthrottled check
only at the configured frequency. Atomic unthrottled checks no longer mutate
the owner's row counter. Network delegates retain their own private counters.
Initialization/reset starts a fresh local window, wrapper-to-wrapper binding
unwraps the delegate, and cleanup clears the active binding. Successful checks
add no objects, thread locals, locks or heap collections.

The fused slot and probe no longer maintain their own throttle counters.
Duplicate advances call the independently bound slot breaker's standard API;
probe input scans use the shared scheduler's frame checks. The breaker initializes
a fresh counter when the query binds. Build rows use the owner's standard API.
Collision walks and map redistribution/merge entries have no breaker polling.

Each probe still caches two frozen native addresses and the table mask, refreshed
on rebinding. The global network breaker and wrapper implementations are unchanged.
The shared `PostAggregationCircuitBreaker` uses its existing non-mutating flag
check before each shard task. The temporary stateful overrides and map-entry
breaker plumbing have been removed.
See the [follow-up audit](parallel-hash-join-group-by-throttling.md) for the parallel
factory survey and updated cancellation, allocation and performance evidence.

## Named regression coverage

| Requirement | Tests and assertions |
| --- | --- |
| Large builds, growth and copy | `AsyncHashJoinGroupByTest.testBuildCancellationAndReuseKeyedAndScalar`; `IntHashJoinBuildTest.testCancellationAtEveryBuildCheckAndReuse`, `testCancellationDuringPayloadCopy`, `testCancellationInsideDuplicateIteration`: interruption, native balance and reuse. |
| Rejected build rows hidden inside delegated cursors | `AsyncHashJoinGroupByTest.testRejectedSerialBuildCancellationAndReuse`, `testRejectedAsyncBuildCancellationAndReuse`, `testRejectedJitBuildCancellationAndReuse`: 100,000-row builds; native/mixed/Parquet; keyed/scalar; cancellation/timeout at predicate call 32; stop within the owning slot's configured row throttle; close and ordinary-result reuse. The JIT fixture asserts it reaches the interpreted column-top branch. |
| Count-only delegated filtering | `AsyncHashJoinGroupByTest.testRejectedCountOnlyFilterCancellationAndReuse`: interpreted and JIT column-top count-only tasks, bounded rejected-row checks, query cleanup and a successful 100,000-row count on reuse. |
| All-miss and all-rejected probe scans | `AsyncHashJoinGroupByTest.testRejectedProbeFrameCancellationAcrossStorageAndReuse` and `testNetworkProbeChecksAtFrameBoundaries`: native/mixed/Parquet and keyed/scalar, cancellation/timeout triggered at predicate call 32 and observed at the next frame/merge boundary, released worker slots and query tracker, ordinary-result reuse. |
| Duplicate chains, null extension and timeout | `testCancellationInsideDuplicateChainAndReuse`, `testScalarCancellationInsideDuplicateChainAndReuse`, `testOuterMissFrameCancellationAndDuplicateTimeoutAndReuse` in `AsyncHashJoinGroupByTest`: interruption within the configured throttle bound after cancellation at joined pair 32, even within a 100,000-row duplicate chain, both aggregation modes, accepted/rejected post-join predicates, native/Parquet probes and reuse. Outer misses use frame boundaries, including cancellation within the last frame. |
| Frame preparation | `PageFrameScanCancellationTest.testDirectFrameCursorsObserveCancellationAndRebind`: direct forward/backward native/Parquet cursors, cancellation between frame requests at production row-throttle settings, successful reacquisition. Existing `testMultiFrameScanObservesMidScanCancellation` covers the record-cursor parent. |
| Delegated partition traversal | `PageFrameScanCancellationTest.testPartitionSizeAndRejectedIntervalTraversalCancellation`: forward/backward full size calculation and all-rejected interval scans, cancellation at 16 checks and successful same-factory reuse. Full/interval cursor and interval-filter suites cover ordinary data, empty partitions, Parquet and timestamp variants. |
| Sparse initialization and output holes | `MapCursorCancellationTest.testSparseInitializationAndTraversalCancelAndRebind`: 4/8-byte maps with long initial/final empty runs, interruption at exactly 16 slot checks, checked and ordinary acquisition, reread and sharded output reuse. |
| Large keyed output | `AsyncHashJoinGroupByTest.testOutputCancellationAndReuseAllMapTypes`: single INT, timestamp and composite keys; owner/sharded merge; cancellation between groups, slot/tracker release and ordinary-result reuse. |
| Merging and drain | Existing `AsyncHashJoinGroupByTest.testMergeCancellationDrainsAndReusesBothPaths`, `testScalarMergeCancellationAndReuse`; `PostAggregationCircuitBreakerTest` and dispatcher drain tests preserve normal error classification and complete cleanup. |
| Independent throttles and deadlines | `SqlExecutionCircuitBreakerWrapperTest.testSharedDelegateHasIndependentThrottlesAndRebinds`: two independent eight-call windows, no shared stateful delegate call, first check on rebinding, unaffected peer. `testNetworkSlotsCopyTimeoutAndCancellationAndRebind`: distinct network delegates, deterministic clock expiry within eight calls, cancellation propagation and independent rebinding. |
| Concurrent queries and storage | `HashJoinGroupByConcurrentTest.testConcurrentQueriesWithOwnerWorkStealing`, `testConcurrentQueriesWithLegacyWorkers`, `testConcurrentQueriesWithFiberWorkers`: keyed/scalar, native/mixed storage, owner/sharded merges, cancel one live query while peers complete, then reuse the same factories. |

The first broad run completed 55 suites with no test failures before a synthetic
`PageFrameReduceDispatcherTest` sequence read its unopened native frame cache.
That fixture had relied on the old Java row-count list's zero-filled backing.
Test-only sequence subclasses now give synthetic tasks an explicit zero row
budget; real scans and explicit row-budget overrides retain their existing
behavior. The repaired dispatcher suite passes all 53 tests. The cleanup-steal test in
`QueryParallelFiberDispatcherTest` now cancels after foreign task publication,
so earlier frame-preparation checks do not bypass the drain phase being tested;
all 40 tests in that suite pass.

The output and serial-filter tests were run before the production fix. Output
returned another group after cancellation; the serial filter evaluated all
100,005 source rows instead of stopping at call 32. Both pass with the fix.

## Validation and reproduction

**2,384 Java tests pass across 70 suites**, with 26 existing conditional skips
and zero failures/errors (2,410 total). The [per-suite results](parallel-hash-join-group-by-cancellation/regressions.csv)
count rerun suites once, using their final result.
The benchmark package and all **43 ordered smoke-result checks** pass; the
[smoke output](parallel-hash-join-group-by-cancellation/smoke.log.gz) is retained.

The allocation rerun initially found a 160-byte source SYMBOL-view allocation
when two owners interchanged readers after the scheduling-dependent warmup. The
[original failure census](parallel-hash-join-group-by-cancellation/symbol-view-setup-failure.log.gz)
is retained. The harness now deterministically seeds the existing bounded view
pools during reported setup: 21 empty views per dictionary for one owner, or 42
for each of two simultaneously acquired source readers. The bound comes from
four source SYMBOL references per owner/worker slot and one build-copy view per
owner; it is independent of rows, groups and dictionary cardinality. No symbol
text is accessed. The three measured builds still copy a disjoint, previously
unseen symbol range into freshly allocated native dictionaries/maps, and byte
and site assertions have no new exemptions. Production pooling is unchanged.

The final controlled C1 matrix passes **24 cases, 234 measured candidate
executions and 405 owner/worker byte windows**, with zero unexplained bytes and
all four workers participating in each census case. It includes native/mixed/
Parquet inputs, owner/sharded/scalar modes, INNER/LEFT/normalized RIGHT, logical
conversion, one/two owners, 8,192/65,536 new symbols, forced native growth,
output, close, cancellation cleanup and same-factory reuse. Every case matches
the ordinary plan. See the [per-thread summary](parallel-hash-join-group-by-cancellation/allocation/summary.csv),
[case matrix](parallel-hash-join-group-by-cancellation/allocation/cases.csv),
[commands](parallel-hash-join-group-by-cancellation/allocation/commands.txt),
[environment and source hashes](parallel-hash-join-group-by-cancellation/allocation/environment.txt)
and paired byte/site logs in that directory. Compiler, bounded setup, deliberate
failures and shared-framework allocations retain the task 9c measurement
boundaries; this does not assert zero allocation under the default JVM.

The native library is built with the existing Rust profiles; this task changes
Java only. The small smoke and allocation checks are integration evidence, not
the pending task 10 latency gate.

```bash
cancellation_test_suites=$(python3 - <<'SUITES'
import csv
with open('docs/parallel-hash-join-group-by-cancellation/regressions.csv') as source:
    print(','.join(row['suite'].rsplit('.', 1)[-1] for row in csv.DictReader(source)))
SUITES
)
mvn -pl core test -P build-rust-library,qdbr-release -Dtest="$cancellation_test_suites"
mvn -pl benchmarks -am package -P build-rust-library,qdbr-release -DskipTests -Dmaven.test.skip=true
bash benchmarks/parallel-hash-join-group-by-allocation.sh docs/parallel-hash-join-group-by-cancellation/allocation
```
