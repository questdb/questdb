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

5. **Connect keyed merging and the output cursor** — commit `a92b71c813`.
   The fused reducer switches from slot maps to sharded updates and uses the
   existing parallel shard merge and `ShardedMapCursor`. Small maps use the
   optimized owner merge. Native map merge loops and map redistribution accept
   cancellation checks; destination allocations remain charged alongside live
   sources. The shared scheduler now converts the countdown latch to a positive
   completed-task count, fixing owner-only merge work stealing. Final projection,
   SYMBOL sorting, ratio evaluation, cursor reread and failure/reuse are tested.
   See [execution, merging and validation](docs/parallel-hash-join-group-by-execution.md).

6. **Integrate planner selection, configuration, and diagnostics (including task 6a)** — this update.
   Ordinary SQL compilation selects the keyed shared-build operator when both
   `cairo.sql.parallel.hash.join.groupby.enabled` (default false) and the existing
   `cairo.sql.parallel.groupby.enabled` switch permit it, with positive configured
   query-worker slots. Planner construction preserves projected column mappings,
   filters, aliases, interval scans, final projections/sorting/limits and normalized
   RIGHT orientation. Probe-only WHERE predicates can become interval/input filters;
   build-only ON residuals filter the build; build-side WHERE stays post-join.
   Speculative filters are restored independently and resource transfer follows
   successful capability checks. EXPLAIN describes orientation, filters, functions
   and actual children without executing the build. Execution metrics and the
   benchmark's planner adapter now expose both plans, phase timings and counters.
   See [planner, ownership, controls and diagnostics](docs/parallel-hash-join-group-by-planner.md).

The branch now supports experimental automatic selection for eligible keyed queries.
The experimental default remains false. Tasks 1–6 and 6a are complete; the RFC's
2× end-to-end performance gate is still pending.

## Next pending task: 7

**Publish the keyed prototype benchmark and enforce the early gate.**

- Run the fixed primary workload: 100 million fact rows, 100,000 unique dimension
  keys, fanout 1, 10% selected keys, seed 130, five years of warm native monthly
  data, and four query workers. Preserve the original installed-capacity denominator.
- Use the existing runner and
  `'--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler'`
  to compile ordinary and enabled plans in the same JVM. Warm both, alternate at
  least ten measured runs per arm, repeat the comparison, and consume final ordered
  results. Build afresh on every execution; include all phases in total latency.
- Publish source revision, generator and commands, hardware/JDK/configuration,
  plans, ordered result checks, medians/spread, phase timings, and sampled peak
  native memory. Measure one/two/four workers plus build-size/selectivity variants.
- Require reproducible ≥2× median four-worker end-to-end speedup on the fixed
  primary workload. If it misses, retain experimental status, profile and revise
  Phase 1, then rerun before task 8. Report regressions and copied-payload memory
  costs. Do not introduce a build-size cutoff or runtime fallback.

Task 6's small benchmark smoke check establishes integration only and does not
satisfy this gate. The [comparison harness guide](docs/parallel-hash-join-group-by.md)
and [planner metrics guide](docs/parallel-hash-join-group-by-planner.md) explain
commands and measurements. Unkeyed execution, including the same flag matrix,
remains task 8; broader qualification and rollout remain tasks 9–10.

Integration notes: children compile under **one enclosing query registration**.
The factory consumes children/functions/interpreted filter context on constructor
entry, including failure, and borrows joined metadata only during construction.
Functions/filter context must match its positive slot count. Owner-only work
stealing works without running consumer threads; zero configured query workers
keep ordinary execution. Probe filter takeover uses interpreted logical getters,
releases unused JIT handles and composes peeled projection mappings. Ordinary
serial probe filters that cannot be stolen keep the existing plan. Child partition-
format guards continue to request normal recompilation; they are not bypassed.

## Validation for task 6

See the [planner guide](docs/parallel-hash-join-group-by-planner.md) for the exact
regression command and smoke invocation. **853 tests passed, 23 existing conditional
cases skipped, zero remaining failures/errors** across 28 suites. The dynamic
configuration suite passed with temporary isolated test ports after encountering
an existing local server; those test-source changes were restored. The benchmark
package passed, and all 40 measured smoke executions matched ordered results with
consistent counters. These are integration checks, not the primary performance gate.
Planner tests assert both
results and selected plans with native-memory leak checks, including all flag
combinations for INNER/LEFT/RIGHT at zero/one/four workers, filtered aliases,
normalized interval extraction, unsupported fallback plan equality, larger builds,
metrics/empty-build reuse, bind/SYMBOL and storage changes, and registered-query
memory failure/cleanup/reuse. Existing forced execution, sharded-merge and
concurrency/failure tests remain in the regression set.

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
