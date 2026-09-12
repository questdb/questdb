# Keyed fused hash join execution

Implements [RFC 130](https://github.com/questdb/rfc/discussions/130) tasks 4–5.
`AsyncHashJoinGroupByRecordCursorFactory` now executes forced keyed INNER and
LEFT OUTER joins, including RIGHT joins whose inputs and expressions have already
been normalized. Planner selection and configuration remain task 6. No SQL plan
changes automatically, and the end-to-end performance gate remains pending.

## Construction and ownership

The factory consumes both compiled child factories, `HashJoinGroupByFunctions`,
and an interpreted probe `AsyncFilterContext` on constructor entry, including
failure. It only borrows `HashJoinGroupByMetadata` during construction: the atom
copies build layout/key indexes and creates joined records before that metadata
can close. The functions and filter context must contain the same positive
number of worker slots. The owner has an additional independent slot.

The children belong to one enclosing query registration, as ordinary planner
children do. They must not each carry a top-level `QueryProgress` wrapper. The
forced tests compile children directly with `SqlCodeGenerator.generate()` and
wrap the fused factory once with `QueryProgress`, so one tracker and cancellation
scope cover build, probe, merge, and output.

The probe child must supply page frames. Its interval scan and direct projection
mapping are preserved. Safely extracted probe predicates are passed separately
as interpreted owner/worker functions in the filter context; the build retains
its ordinary filtered cursor. This boundary does not detach filter wrappers or
extract ON predicates. Task 6 must perform those planner ownership transfers.
Compiled/JIT probe filters and unkeyed functions are rejected at construction.

The frame sequence owns the atom. The atom owns copied build storage, slot
records and breakers, and `GroupByMapFragment` state through a
`GroupByShardingContext`. Functions and the filter context remain factory-owned.
No aggregate map or decoder backing is allocated before execution binds its
memory tracker. The unused filter row-id lists are closed because this reducer
filters logical rows directly.

## Execution and publication

1. Cursor acquisition consumes the filtered build cursor once and freezes the
   copied lookup. The source cursor can close because payloads and SYMBOL
   dictionaries are owned by the build.
2. The frame sequence acquires the probe cursor. Each slot binds its own frozen
   lookup, logical `PageFrameMemoryRecord`, joined record, symbol views and
   breaker. Functions initialize once; the owner offers state to worker clones.
   Output symbol functions are ready before the cursor returns to its parent.
3. First output access prepares frames and decoder pools, then publishes tasks
   through `UnorderedPageFrameSequence`. Its queue publication makes the frozen
   build and initialized slot state visible before worker probes run.
4. Each reducer acquires a logical slot through `PerWorkerLocks` (or uses the
   dedicated owner slot). Decoder navigation, logical record initialization and
   map allocation all occur inside the slot-release cleanup scope. No slot is
   permanently assigned to a thread.
5. For each probe row, evaluate the probe filter, enumerate every INT-key match,
   evaluate the post-join filter, then update the slot's grouping map. LEFT misses
   instead evaluate one typed null-right candidate. Rejected real matches never
   create a replacement miss. Throttled circuit-breaker checks and sequence
   cancellation checks occur in both row and duplicate loops.
6. A slot crossing the configured grouping threshold enables sharding. At the
   next frame boundary each acquired slot redistributes its existing map, then
   routes updates by the grouping key hash. The first shard supplies reusable
   key-writing scratch. The previous execution's cardinality hint can enable
   sharding before the first frame on reuse.
7. After all reducers finish, small maps merge on the owner through the optimized
   native map merge and `GroupByFunctionsUpdater`. Otherwise the owner finishes
   any outstanding redistribution and publishes `GroupByMergeShardJob` tasks
   through `GroupByShardingContext`. Each task acquires its own updater slot and
   merges the corresponding source shards into a separate tracked destination.
   Source maps remain live while destination allocations are charged. AVG keeps
   its intermediate sum/count state through the merge.
8. The cursor exposes virtual aggregate records, symbols and random access over
   either the owner map or `ShardedMapCursor`. `toTop()` rereads the computed
   result without another build or probe scan. The factory reports no input ordering.

Both merge paths preserve SYMBOL backing for parent projections and sorting.
The ordinary `VirtualRecordCursorFactory` and `SortedLightRecordCursorFactory`
can consume the output, including the motivating ratio's capacity contribution
once per matching reading/payload pair.

An empty INNER build skips probe dispatch. LEFT execution still scans preserved
rows, including when no copied SYMBOL dictionary entries exist. Both cases
retain normal keyed empty-input semantics.

## Cleanup and reuse

Close or a reduce/merge failure cancels the sequence and drains published work
before releasing execution backing. Parallel merge publication always drains its
queued tasks in `finally`, before the cursor can clear maps or build storage.
The owner propagates captured worker errors with their memory-limit/interruption
flags intact. Native merge loops check cancellation while scanning source slots,
including copies of new groups that never invoke an aggregate merge function.
Redistribution checks both shard allocation and entry-copy loops. The shared
post-aggregation breaker reads its cancellation flag without modifying a shared
throttle counter.

The shared scheduler converts the countdown latch's negative value into a
positive finished-task count for work-stealing decisions. Passing the negative
value made completed tasks look outstanding and could stall owner-only sharded
execution after the first reclaimed merge task.

After output finishes, cleanup calls `functions.cursorClosed()`, clears slot
views, releases decoder/map resources,
and closes the build; the probe cursor closes last. Initialization rollback
releases functions while their symbol sources are still alive, before the frame
sequence closes a failed probe acquisition. Cleanup preserves the original error
and attempts the remaining resources if a function cleanup throws.

Every new acquisition builds fresh lookup/dictionary state and initializes
functions again. Reuse after failure retains no acquired slot, map contents or
build handles. Build, duplicate/payload/dictionary, decoder and aggregate map
allocations use the enclosing query tracker. There is no replay or serial fallback.

## Validation

`AsyncHashJoinGroupByTest` forces this factory and compares it with ordinary SQL.
The task-4 tests use native-memory leak checks and cover:

- INNER/LEFT/normalized RIGHT results, duplicates, null keys/payloads, empty inputs,
  all misses, count/coalesce and the original year/month/SUM/AVG expressions.
- Probe/build filters, interval scans, null-accepting and null-rejecting WHERE,
  reordered/pruned normalized projections, probe SYMBOL column tops, bind
  rebinding and replacement build dictionaries.
- Compile-and-close, close before dispatch, partial output close, result reread,
  random access, output SYMBOL availability before dispatch, and factory reuse.
- Simultaneous acquired worker probes, once-per-execution function initialization,
  injected worker/init failures, and cancellation after 32 evaluations in a
  100,000-match duplicate chain.
- Build and reduce memory-limit errors, worker slot release after allocation and
  Parquet decoder failure, zero remaining tracked bytes, and successful reuse.
  Latches prove worker acquisition before checking released slots.
- Native-to-Parquet and back on a cached factory, mixed partitions, and logical
  DOUBLE-to-FLOAT storage conversion with an explicit DOUBLE aggregate argument.
- Constructor rejection/cleanup for unkeyed inputs.

```bash
mvn -pl core test \
  -Dtest=AsyncHashJoinGroupByTest,HashJoinGroupByFunctionsTest,HashJoinGroupByCandidateTest,IntHashJoinBuildTest,SqlCodeGeneratorWorkerFunctionExtractionTest,GroupByUtilsTest,GroupByRewriteTest,HashJoinTest,JoinRecordMetadataTest,GroupByMapFragmentTest,AsyncFilterContextTest,QueryRegistryMemoryTrackerTest,MapTest,OrderedMapTest,ShardedMapCursorTest,ParallelGroupByMemoryTrackerTest,ParallelHorizonJoinMemoryTrackerTest,PostAggregationCircuitBreakerTest,QueryParallelFiberDispatcherTest,HorizonJoinTest
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
```

Task 5's regression command ran **586 tests: 575 passed, 11 existing map-variant
cases skipped, zero failures/errors**. The 23 fused execution tests include eight
new tests for forced owner/sharded merging, concurrent merge/failure gates, the
ordered solar query and outer/right variants, unequal AVG partial counts,
10,003 final groups, empty-build/dictionary reuse, and merge memory-limit and
cancellation cleanup. Cursor checks reread results, visit every row ID in reverse
across shards and calculate remaining size. Four map variants and the ordered
variable-size path independently cancel long copy/merge loops; the fragment
test cancels after 31 redistributed entries and verifies cleanup/reuse. These
are 14 additional passing test cases in total. The benchmark package command
also passed.

The shared regressions cover ordinary group-by and horizon memory limits,
172 horizon-join cases, and 40 parallel fiber dispatcher cases. Task 4's earlier
159-test result and the earlier component measurements remain historical records.

These checks qualify the forced keyed pipeline. Task 6's planner selection and
diagnostics, task 7's benchmark gate, and task 9's wider storage/concurrency
qualification remain pending. Earlier component storage measurements remain
unchanged.
