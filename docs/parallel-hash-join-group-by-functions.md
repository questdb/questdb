# Joined metadata and function boundary

Implements [RFC 130](https://github.com/questdb/rfc/discussions/130) task 3. This
compiles and evaluates joined expressions over constructed pairs. Task 4 now
connects this boundary to the [keyed execution lifecycle](parallel-hash-join-group-by-execution.md).
Planner selection remains task 6; default SQL plans and configuration are unchanged.

## Compilation and indexes

`HashJoinGroupByCandidate` retains resolved grouping/aggregate columns and
post-join predicates alongside its base-table column provenance. These are
compiler inputs, valid before model mutation or compiler reuse. Input filters and
the safely extractable build-only ON expression keep their existing ownership.

`HashJoinGroupByMetadata` takes the candidate and both compiled inputs' metadata
and mappings. Each input mapping is **compiled record index → base-table index**;
it is neither a writer-index list nor an identity mapping assumed from SQL order.
It validates mapping sizes, required columns and types, then produces:

- Probe and build join-key indexes addressing the actual compiled input records.
- `getBuildColumns()`, addressing the compiled build record in pruned payload order,
  and matching payload metadata for `IntHashJoinBuild`.
- Combined `JoinRecordMetadata`: the compiled probe columns followed by copied
  payload fields, qualified with the logical input aliases. Resolved expressions
  are remapped into that layout; their output names and types survive input swaps.
- Post-join filters containing dependencies from projection wrappers and joined
  predicates. Filter-only payload columns are retained; build-input-filter-only
  columns are not copied.

For example, a build projection `(installed_kwp cap, plant_id id, country c)` has
base indexes `[2,0,1]`. When the query needs country then capacity, the build key
index is `1`, copied source indexes are `[2,0]`, and payload getters use `0,1`.
This remains true for a normalized `p RIGHT JOIN r`.

Copied SYMBOL metadata advertises `SymbolTable`, matching the owned dictionary's
actual interface. Its immutable backing does not imply `StaticSymbolTable`'s
reverse-lookup contract. Inheriting that flag from a table input would incorrectly
compile static-symbol functions against the copied dictionary.

## Slot records and functions

`HashJoinGroupByRecord` reuses `OuterJoinRecord` getters and a slot-local typed
`NullRecordFactory` record. `of(probeRecord, probeSymbols, buildProbe)` binds a
logical input record (later a `PageFrameMemoryRecord`) and resets the payload to
null. After advancing a match, `setHasMatch(true)` exposes the probe's copied
record; `setHasMatch(false)` exposes nulls on an ON miss. Match detection remains
outside WHERE filtering: rejected matches must never become replacement misses.

Each slot needs its own frozen probe, joined record and logical probe symbol
source. Both record getters and symbol-table lookups use the same physical
layout. A/B symbol flyweights are independent across copied-payload views, and
empty-build null keys resolve without a real dictionary entry.

`SqlCodeGenerator.compileHashJoinGroupByFunctions()` assembles owner functions
with `GroupByUtils`, rechecks compiled capabilities, and uses the existing worker
projection/filter compiler helpers. `HashJoinGroupByFunctions` owns those
functions, output metadata, grouping sinks and aggregate updaters. Slot `-1` is
the owner; nonnegative indexes are logical worker slots. Mutable functions are
cloned, while thread-safe functions are borrowed through `PerWorkerFunctionList`.
Aggregate value indexes stay aligned with the owner, including AVG's sum/count.

For each execution:

1. Build and freeze copied storage, then bind every slot's joined record and symbol
   source. No input cursor is consumed by metadata or function compilation.
2. Call `functions.init(ownerAndSlotSources, context)` once. Initialize owner keys,
   aggregate arguments and filters, offer that state to worker clones, then
   initialize those clones with independent symbol tables. Restore the context's
   clone flag even if initialization fails. Never initialize these functions per
   frame or independently reevaluate owner query-constant state in each worker.
3. Initialize parent projections/sorts after `init` returns. Output key functions
   are already initialized and `getSymbolTable`/`newSymbolTable` are usable before
   the first pair or output row. Aggregate owners are not initialized again here.
4. After draining all slots and finishing output, call `cursorClosed()` to release
   execution-local function state, including after failed initialization. It skips
   borrowed worker references and attempts every cleanup callback. Clear joined
   records and release the build only after symbol consumers finish. Close the
   compiled functions and metadata when their owning factory closes.

The caller owns the children, build, frame resources, maps and publication/drain
barriers. This boundary borrows their records and symbols. The next task must
connect query memory tracking before map/frame allocations and implement the
actual cursor lifecycle; these constructed-pair tests do not qualify frame paths
or prove fused parallel execution.

## Validation

```bash
mvn -pl core test \
  -Dtest=HashJoinGroupByFunctionsTest,HashJoinGroupByCandidateTest,IntHashJoinBuildTest,SqlCodeGeneratorWorkerFunctionExtractionTest,GroupByUtilsTest,GroupByRewriteTest,HashJoinTest,JoinRecordMetadataTest
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
```

124 tests pass, including 14 new tests under the native memory-leak harness.
Constructed pairs use the real grouping sinks, maps and aggregate updaters and
compare each owner's/worker's output with ordinary SQL. Coverage includes the
original year/month/country grouping, SUM/AVG and final capacity ratio projection,
duplicates, null keys/payloads, empty builds, null extension, post-join filters,
counts/coalesce, filter-only payloads, aliases above joins, normalized right joins
with reordered/pruned inputs, every supported typed getter (including timestamp
nanoseconds), probe symbols/column tops, independent A/B payload symbols, owner-only
execution, bind rebinding and dictionary replacement. Injected functions verify
owner state donation, initialization counts, partial compile cleanup, context
restoration and successful initialization after failure.

The benchmark package builds; no new performance claim is made. The committed
[component measurements](parallel-hash-join-group-by-build.md) and
[ordinary-plan baseline](parallel-hash-join-group-by-baseline.md) remain the available
measurements. RFC task 7's four-worker end-to-end 2× gate remains pending.
