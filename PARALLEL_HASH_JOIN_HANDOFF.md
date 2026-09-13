# Parallel hash join / fused aggregation handoff

Updated 2026-09-13. Branch: `puzpuzpuz_parallel_fused_hash_join`.
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

6. **Integrate planner selection, configuration, and diagnostics (including task 6a)** — commit `755ee06eac`.
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

7. **Publish the keyed prototype benchmark and enforce the early gate** — commit `7c606730e9`.
   The fixed 100-million-row/four-worker primary comparison passes in both rounds:
   ordinary/fused medians 2,323.627/335.099 ms (**6.934×**) and
   2,321.217/333.964 ms (**6.951×**). No engine tuning or workload change was needed.
   `HashJoinGroupByBenchmark --require-primary-gate=true` rejects altered primary
   parameters and fails unless every repetition reaches the unrounded 2× target.
   A sequential script reproduces the primary, one/two-worker, build-size,
   selectivity and small-input cases, retaining commands, environment, plans,
   ordered results and every sample. See the [benchmark report and raw data](docs/parallel-hash-join-group-by-benchmark.md).

8. **Add unkeyed aggregation through the same build/probe pipeline** — commit `0c940672cd`.
   The existing factory/atom/reducer now supports scalar partial states, without
   grouping maps or sharding contexts. Each slot's `SimpleMapValue` is allocated
   under the execution tracker alongside the live build, initialized with the
   aggregates' empty values, and merged after probe drain through the existing
   intermediate-state updaters. The cursor returns exactly one row, including
   empty input, supports reread and reports no random access. EXPLAIN identifies
   `aggregation: scalar`; the same global/experimental/positive-worker gate applies.
   All declared SUM/AVG DOUBLE and COUNT INT/LONG/DOUBLE/SYMBOL variants are covered
   for keyed and scalar INNER/LEFT/normalized RIGHT execution. See
   [scalar execution and validation](docs/parallel-hash-join-group-by-unkeyed.md).

9. **Complete storage, concurrency, and resource qualification** — commit `39c741ac73`.
   The published V1 capability table is backed by plan-checked native/Parquet,
   mixed-partition, column-top and logical-conversion comparisons for all payload
   types on both inputs, keyed/scalar filtered storage guards, symbol/bind reuse
   and source invalidation. A fixed-seed matrix adds 720 differential SQL cases.
   Concurrent-query tests cover owner work stealing and real legacy/fiber workers,
   both merge paths, cancellation isolation and reuse. New build-source failure,
   build cancellation and scalar decoder failure tests complete the existing
   lifecycle/resource matrix. No engine change was required. See
   [coverage and reproduction](docs/parallel-hash-join-group-by-qualification.md).

10. **Original V1 benchmark and rollout decision** — commit `58b1dc04cc`.
    The fixed 100-million-row/four-worker primary gate passes again at
    **7.179× and 7.144×** median speedup. A 44-case matrix covers keyed/scalar
    INNER/LEFT/normalized RIGHT and both physical orientations, build footprints
    and scan costs, match rates, post-filters, fanout/skew, group counts, worker
    scaling, concurrent load, native/mixed/Parquet and verified cold storage,
    plus an 88 MiB query limit. All 2,040 measured executions and 2,608 explicit
    result comparisons pass. Per-query memory sampling supplements process-native
    deltas. The rollout decision is **retain experimental default false**:
    swapped RIGHT is 96–97% slower with a much larger copied build, and tiny
    effective scans regress. The ten-million-row Parquet RIGHT pilot was stopped
    during warmups; a repeated one-million-row native/Parquet pair is published.
    See [completed-V1 results, limitations and rollout](docs/parallel-hash-join-group-by-v1.md).

9a. **Complete allocation-time native memory tracking** — commit `c09746fd94`.
    The allocation-site audit covers build/SYMBOL/duplicate storage, keyed/scalar
    state, both merges, frame caches, filtered-source row buffers and Java/Rust
    decoder scratch. Frame-cache and filter/decoder helper allocations now bind
    the query tracker before growth. Rust dictionary/data-page scratch uses the
    output buffers' tracked allocator. Shared filter tasks release query-owned
    backing when collected, before their tracker can be recycled. New growth,
    high-cardinality, combined-state, unlimited-limit and reuse tests supplement
    the existing failure/concurrency coverage. See the [allocation audit and validation](docs/parallel-hash-join-group-by-memory.md).

9b. **Keep large and data-dependent execution structures off heap** — commit `d2eceb5116`.
    Frame descriptors and sparse decoder indices now use tracked native vectors;
    duplicate frame-count lists and per-frame decoder references are removed from
    qualified scans. Source SYMBOL views and fused predicate implementations avoid
    dictionary-sized Java caches. Decoder shell counts are bounded even for sparse
    declarations; covering-index inputs explicitly keep ordinary plans. Increasing
    cardinality measurements cover native/mixed/Parquet, owner/sharded/scalar,
    fresh execution and reuse, with result and native-balance checks. See the
    [retained-heap audit, measurements and validation](docs/parallel-hash-join-group-by-heap.md).

9c. **Make successful fused execution zero-GC after bounded setup** — this update.
    Reuse native-growth scratch, frozen/probe/SYMBOL views, scalar shells and
    native-closed decoder buffers. Preserve independent slot generations, expired
    handle checks, query tracking and close/reuse semantics. Paired exact owner/
    worker byte counters and allocation-site stacks cover fresh builds, unseen
    symbols, forced growth, all merges, output, close, concurrency and reuse.
    The controlled C1 allocation gate and default-JVM regression/retained-heap
    checks have separate documented boundaries. See the [execution allocation
    audit and reproducible artifacts](docs/parallel-hash-join-group-by-allocation.md).

The branch supports experimental automatic selection for eligible keyed and
unkeyed queries. Tasks 1–9, 6a and 9a–9c are complete. **V1 is not complete:** the
current RFC requires 9a–9e after the original task 10 benchmark. Tasks 9d–9e
remain pending, then task 10 must be rerun on that implementation. Keep the
experimental default false; default enablement remains a separate reviewable
configuration/planner change.

## Next pending RFC task: 9d (V1)

**Complete circuit-breaker integration for every potentially large execution loop.**

- Audit build-source consumption, filtered scans, keyed/scalar probing and
  aggregation, cursor-based merging and output, including delegated `hasNext()`
  implementations that consume many rows internally.
- Use existing throttled breaker calls on rejected rows, misses, null extensions,
  and successful matches. Check inside long duplicate/nested loops, preserving
  current growth, rehash, copy/decode, redistribution and native merge checks.
- Bind the active query breaker before work and propagate it to independently
  owned worker slots. Preserve throttling, zero-GC successful checks and correct
  rebinding; concurrent workers must not share mutable throttle state unsafely.
- Add deterministic cancellation/timeout tests for large builds, all-miss/all-
  rejected scans, duplicate chains and merge/output traversal, for keyed/scalar
  and enabled storage paths. Bound loop work between checks. Verify drain,
  cleanup, query-memory release, same-factory reuse and unaffected peer queries.
- Publish the loop audit and named tests; rerun affected regressions and include
  breaker overhead in task 10's end-to-end measurements.

Task **9e** remains the expanded semantic/storage/negative matrix and coverage
mapping. Explicitly cover **SQL LHS and RHS SYMBOL columns as grouping keys and
aggregate arguments**, separately and together, including the same column in both
roles, repeated references, multiple SYMBOL columns, COUNT(SYMBOL), and supported
SYMBOL-consuming expressions. Test symbol-table initialization/routing for owner,
workers, merges and output; independent cloned views; RIGHT input swapping;
empty/null dictionaries and outer null extension; differing text-to-ID mappings;
native/Parquet/mixed storage; first execution/reuse, dictionary changes, bind
rebinding and concurrent factories. Compare resolved text and metadata with the
ordinary path, and map these cases to named tests in the 9e coverage table.

After 9d–9e, repeat task 10's performance/rollout gate. Parallel radix build (11)
and broader extensions (12) remain post-V1. Keep
`cairo.sql.parallel.hash.join.groupby.enabled=false` and the global parallel GROUP
BY gate. Do not add a build-size threshold, runtime fallback or input replay.

The [original task 10 report](docs/parallel-hash-join-group-by-v1.md) and its
[raw data](docs/parallel-hash-join-group-by-v1/summary.csv) describe the earlier
implementation; their timings and sampled memory peaks do not validate 9a–9e.
Task 9's qualification results are historical; affected-suite reruns are recorded
in the task 9a–9c guides. The ten-million-row Parquet RIGHT pilot remains incomplete
as documented in the original report. Repeated decoding under the bounded sparse
cache and uncached SYMBOL predicate CPU costs need task 10 measurements.

Integration notes: children compile under **one enclosing query registration**.
The factory consumes children/functions/interpreted filter context on constructor
entry, including failure, and borrows joined metadata only during construction.
Functions/filter context must match its positive slot count. Owner-only work
stealing works without running consumer threads; zero configured query workers
keep ordinary execution. Probe filter takeover uses interpreted logical getters,
releases unused JIT handles and composes peeled projection mappings. Ordinary
serial probe filters that cannot be stolen keep the existing plan. Child partition-
format guards continue to request normal recompilation; they are not bypassed.

## Validation for task 9c

**1,985 Java tests passed across 55 suites**, with two existing conditional skips
and zero failures/errors (1,987 total). The benchmark package and 43 ordered smoke
checks passed. The retained-heap rerun passed all 864 snapshots, 288 candidate
executions and 144 ordinary references; every closed query-native balance was
zero. The largest fixed-case heap increase was 1,256 bytes; the largest array
was 16,400 bytes.

The [allocation guide](docs/parallel-hash-join-group-by-allocation.md) records the
controlled JVM flags, compiler/setup/shared-framework/failure boundaries, exact
per-thread counters, allocation counts/stacks, workload and source hashes. Its
24-case matrix checks 234 measured candidate executions and 405 owner/worker
windows with zero bytes attributable to the fused pipeline, including unseen
symbols, native growth, 131,072 groups, both owners and all four workers. Each
case also checks cancellation cleanup, same-factory reuse and ordinary results.
These measurements preserve task 9b's retention bound; they do not replace the
pending task 10 latency gate. The experimental default remains false.

## Validation for task 9b

1,933 Java tests passed across 52 suites, with two existing conditional skips
and zero failures/errors (1,935 total). The benchmark package passed. All 864
retained-heap snapshots, 288 candidate executions against 144 ordinary references,
and 43 ordered smoke-result checks passed. Every candidate cursor close left
zero query-native bytes; the largest fixed-case heap increase was 2,936 bytes,
and the largest individual array was 16,400 bytes.

The [heap guide](docs/parallel-hash-join-group-by-heap.md) records storage/control
bounds, measurement exclusions, exact commands and retained per-class/sample
artifacts. The current change modifies Java only; task 9a's native library still
requires the `build-rust-library,qdbr-release` profiles. The smoke benchmark is
integration evidence; task 10 remains pending after 9c–9e.

## Validation for task 9a

**1,645 Java tests passed across 41 suites, with two existing conditional skips
and zero failures/errors** (1,647 total). **603 Rust tests passed**, with two
existing ignored decoder cases. The benchmark package passed and the
100,000-row/four-worker smoke comparison passed all **43 ordered result checks**,
including all 40 measured executions. This is integration evidence, not a repeat
of the task 10 performance gate. The [allocation audit](docs/parallel-hash-join-group-by-memory.md)
records every covered allocation site, lifecycle, new regression and exact
Java/native reproduction command. Compile the changed Rust library with the
`build-rust-library,qdbr-release` Maven profiles.

## Validation for task 10

The benchmark package build, shell syntax checks, 16 CLI guards and twelve
concurrent keyed/scalar orientation smoke workloads passed. The final 44-case
matrix contains **2,040 measured executions and 2,608 result comparisons**, with
zero mismatches; all 80 cold-cache preparations verified residency. The
artifact validator rejects incomplete samples, missing checks and failed gates.
The primary ordered reference exactly matches task 7. Source/jar hashes, both
measurement environments, commands, plans, counters, memory estimates and every
sample are retained in the [report](docs/parallel-hash-join-group-by-v1.md).
No production engine code changed and the task 9 engine suites were not rerun.

## Validation for task 9

**1,790 tests passed across 41 suites, three existing conditional skips, and zero
failures/errors** (1,793 total, counting rerun cases once). All 50 dynamic
configuration tests passed with temporary isolated ports; the original test
source was restored. The benchmark package build passed.

The [qualification guide](docs/parallel-hash-join-group-by-qualification.md) maps
published capabilities and each failure phase to tests and records reproduction
commands. New coverage comprises 720 fixed-seed differential SQL cases (each
candidate executed twice), 144 concurrent executions across three worker modes
(including 12 expected cancellations), all supported payload getters on both
storage paths, invalidation/rebinding, and per-failure cleanup/reuse assertions.
The task adds fourteen test methods across three suites and expands the planner
memory-limit matrix. Existing resource and lifecycle tests remain part of the
broader regression set.

## Validation for task 8

**651 tests passed across 26 suites, with zero failures, errors or skips.**
This includes ten new test methods plus expanded planner/differential coverage.
The benchmark package build passed. The 100,000-row/four-worker keyed smoke
comparison passed all 43 explicit result checks, including all 40 measured
executions. This smoke check validates integration, not the primary performance gate.

The [scalar guide](docs/parallel-hash-join-group-by-unkeyed.md) records exact commands
and coverage. The differential matrix checks all declared aggregate/type pairs
on both inputs for keyed/unkeyed INNER/LEFT/RIGHT, including duplicate fanout,
all-null arguments, empty build/probe/both, no matches, and filters rejecting all
candidates. It compares result types as well as values. The flag matrix now covers
scalar aggregate projections and count-only queries at zero/one/four workers.
Scalar tests exercise concurrent slot use, intermediate-state merging, long-chain
cancellation, initialization/probe/merge failure and cancellation, scalar allocation
breaches with the build still live, early close, remaining size, reread and reuse.
Task 7's measurements remain historical; completed-V1 benchmarking is task 10.

## Validation for task 7

The benchmark package build and script syntax check passed. Eleven CLI checks
reject invalid gate settings before data generation. Across nine matrix cases,
**360 measured executions** match the ordered reference, with 459 explicit
comparisons including warmups. The additional initial primary comparison retains
40 measured executions, also matching results. The [report](docs/parallel-hash-join-group-by-benchmark.md)
contains all timings, memory costs, commands, result checks and limitations.
The small-probe/large-build diagnostic regressed by 6.8–12.9%; primary retained
native memory was 1.97× ordinary. These costs remain explicit rollout inputs.
No production engine code changed; task 6's regression results below were not
rerun or replaced by this benchmark-only task.

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
