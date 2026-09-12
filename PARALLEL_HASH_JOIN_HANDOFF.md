# Parallel hash join / fused aggregation handoff

Updated 2026-09-12. Branch: `puzpuzpuz_parallel_fused_hash_join`.
Draft PR: [questdb/questdb#7618](https://github.com/questdb/questdb/pull/7618).
Design and dependency order: [RFC 130](https://github.com/questdb/rfc/discussions/130).

## Completed

1. **Define eligibility and build the comparison harness** — commit `2a0fe6f6b3`.
   The pre-construction candidate API, function/type allowlist, predicate and
   mapping contracts, seeded end-to-end runner, and primary ordinary-plan baseline
   are in the branch. See [capabilities and reproduction commands](docs/parallel-hash-join-group-by.md)
   and [the 100-million-row baseline](docs/parallel-hash-join-group-by-baseline.md).
2. **Implement and validate the immutable build boundary** — commit `19c72c58a0`.
   `IntHashJoinBuild` owns tracked native INT hash slots, typed copied payloads,
   duplicate links, and a UTF-16 SYMBOL dictionary. `FrozenHashJoinBuild` supplies
   independent lookup/record/symbol views, execution-local handles, and a boundary
   usable by a later sharded implementation. Build/growth/probe cancellation and
   cleanup/reopen behavior are tested. See [layout, ownership and measurements](docs/parallel-hash-join-group-by-build.md)
   and [all storage samples](docs/parallel-hash-join-group-by-build-results.csv).

3. **Implement joined metadata, record access, and function initialization** — commit `e7e5a2e510`.
   `HashJoinGroupByMetadata` composes candidate base indexes with actual compiled
   input projections and copied payload order. `HashJoinGroupByRecord` supplies
   slot-local logical/null getters and symbol routing. `HashJoinGroupByFunctions`
   assembles grouping sinks, aggregate updaters and independent mutable functions,
   initializes owner state once and donates it to workers, prepares output symbols
   before parent initialization, and releases execution state through `cursorClosed()`.
   See [API, ownership and validation](docs/parallel-hash-join-group-by-functions.md).

4. **Implement the keyed factory, atom, cursor lifecycle, and frame reducer** — commit `ce38d0e7d3`.
   `AsyncHashJoinGroupByRecordCursorFactory` now executes forced keyed INNER/LEFT
   and normalized RIGHT plans using immutable build storage, independently
   acquired slot state, logical frame access and an interruptible owner merge.
   It drains tasks before cleanup and retains SYMBOL backing through output.
   See [execution, ownership and validation](docs/parallel-hash-join-group-by-execution.md).

5. **Connect keyed merging and the output cursor** — this update.
   The fused reducer switches from slot maps to sharded updates and uses the
   existing parallel shard merge and `ShardedMapCursor`. Small maps use the
   optimized owner merge. Native map merge loops and map redistribution accept
   cancellation checks; destination allocations remain charged alongside live
   sources. The shared scheduler now converts the countdown latch to a positive
   completed-task count, fixing owner-only merge work stealing. Final projection,
   SYMBOL sorting, ratio evaluation, cursor reread and failure/reuse are tested.
   See [execution, merging and validation](docs/parallel-hash-join-group-by-execution.md).

The branch executes the fused keyed operator through its explicit construction
boundary. Default plans, configuration and automatic EXPLAIN selection remain
unchanged. The RFC's 2× end-to-end gate is still pending.

## Next pending task: 6

**Integrate planner selection, configuration, and diagnostics.**

- Connect the candidate API to fused factory construction before the ordinary
  join/group-by pipeline. Compile children and joined functions, preserve
  projections, aliases, intervals and final ordering/limiting, then transfer
  child/filter ownership only after all capability checks pass.
- Add `cairo.sql.parallel.hash.join.groupby.enabled`, default false, and wire
  configuration wrappers, SQL execution context and test overrides. Define
  behavior with no query workers and existing parallel-aggregation controls.
- Implement safe probe-filter takeover and post-join filtering with correct
  predicate placement for INNER/LEFT and normalized RIGHT joins. Unsupported
  shapes must retain their existing plans without leaked speculative resources.
- Expand EXPLAIN with grouping/aggregate expressions, filters, join condition,
  logical/physical orientation and input swapping, plus `buildStrategy: shared`.
  EXPLAIN must not consume the build input.
- Add benchmark instrumentation: build rows/keys/bytes, scanned rows, matched
  pairs, null extensions, surviving candidates, merge cardinality, phase timings
  and peak memory. Keep worker counters local until completion.

Completion requires enabled/disabled result and plan assertions for supported
and rejected shapes, including aliases, filters, normalized RIGHT and larger
builds. Do not introduce a build-size cutoff, runtime fallback or input replay.
The keyed performance gate remains task 7; no speedup is claimed yet.

Integration notes: compile ordinary child factories under **one enclosing
query registration**; do not nest independently registered `QueryProgress` roots.
The factory consumes children/functions/interpreted filter context on constructor
entry, including failure, and borrows joined metadata only during construction.
Functions/filter context must match its positive worker-slot count. Owner-only
execution is supported by work stealing when no consumer threads are running;
zero configured slots, unkeyed and compiled/JIT probe filters are not accepted by
this construction boundary. Task 6 defines worker/control policy and filter takeover.

## Contracts to preserve

`open(tracker, breaker)` binds tracking before any native build allocation.
`build(cursor, keyColumn)` consumes a borrowed source cursor once and freezes;
the caller closes that cursor. Freezing ends mutation but the caller must publish
through a task synchronization boundary. Give every acquired logical slot its own
probe and circuit breaker. Drain all probes and finish symbol output before
closing the build. Closing invalidates all handles and views; reopen creates a
fresh execution and dictionary. No build-size fallback or consumed-input replay.

The copied representation costs more build time and retained memory than the
light join's row IDs. Final component measurements cover 10k/100k/1m unique keys
and 10k keys with fanout 10, with both rounds and all regressions retained.
In particular, the 100k unique case probes more slowly. These measurements select
a safe initial boundary; they do not pass the later keyed pipeline's performance
gate or justify default enablement.

## Validation for task 5

The [execution report](docs/parallel-hash-join-group-by-execution.md) contains the
exact regression command. **575 tests passed, 11 existing conditional map cases
skipped, zero failures/errors** (586 total). This includes 23 fused execution
tests, 10,003 final groups on both merge paths, the ordered motivating query,
concurrent merge/failure gates, cancellation, destination memory-limit breaches,
all-row random access, remaining size, partial close and successful factory reuse.
There are 14 additional passing cases across fused execution, map merge and
redistribution tests. Shared regressions include ordinary group-by/horizon memory
tracking and the parallel fiber dispatcher.
`mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true` also passed.

## Validation for task 4

See the [execution report](docs/parallel-hash-join-group-by-execution.md) for exact
commands and scope. **159 tests passed**, including 15 new tests, and
`mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true` passed.
The new tests force the factory under one query tracker,
compare ordinary SQL results, and use memory-leak checks. They cover semantics,
concurrent acquired slots, bounded duplicate cancellation, worker/init failure,
allocation/decoder cleanup, output reread and reuse. Mixed native/Parquet and
logical conversion cases exercise the actual typed frame getter path.

Task 3's 124 tests (14 new) and successful benchmark package remain recorded in its
[function-boundary guide](docs/parallel-hash-join-group-by-functions.md). Task 2's
98 tests and four storage comparisons remain in its
[report](docs/parallel-hash-join-group-by-build.md). All 160 measured storage samples
are retained. Task 4 does not rerun or supersede those component measurements.
