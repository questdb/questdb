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

9c. **Make successful fused execution zero-GC after bounded setup** — commit `0c5d8d78af`.
    Reuse native-growth scratch, frozen/probe/SYMBOL views, scalar shells and
    native-closed decoder buffers. Preserve independent slot generations, expired
    handle checks, query tracking and close/reuse semantics. Paired exact owner/
    worker byte counters and allocation-site stacks cover fresh builds, unseen
    symbols, forced growth, all merges, output, close, concurrency and reuse.
    The controlled C1 allocation gate and default-JVM regression/retained-heap
    checks have separate documented boundaries. See the [execution allocation
    audit and reproducible artifacts](docs/parallel-hash-join-group-by-allocation.md).

9d. **Complete circuit-breaker integration** — commit `560fe4b59a`.
    Delegated serial/async build filters check rejected rows, including the JIT
    factory's interpreted fallback and column pre-touch. Forward/backward frame
    preparation checks skipped frames, empty partitions, rejected intervals and
    Parquet row groups. Sparse grouping cursors bind the query breaker before initialization and check empty-slot
    scans; fused output checks every cursor advance. Shared atomic delegates
    now use independently owned wrapper throttle counters. Existing row/pair,
    build-growth, merge and mandatory drain checks remain. Deterministic tests
    cover native/mixed/Parquet interruption, bounded loop work, timeout,
    independent rebinding, cleanup and ordinary-result reuse. See the
    [loop audit, ownership and validation](docs/parallel-hash-join-group-by-cancellation.md).

9e. **Expand the semantic, storage, SYMBOL and negative matrix** — commit `a7500b88c7`.
    Eight new semantic tests cross independent SQL LHS/RHS grouping and argument
    roles with INNER/LEFT/normalized RIGHT, every allowlisted aggregate/type,
    reordered projections, all nine native/mixed/Parquet input pairs, join-key and
    payload tops, nulls, duplicates and extreme keys. SYMBOL cases cover multiple
    dictionaries/columns, repeated key/argument use, cloned views, expressions,
    rebinding, growth, empty/null dictionaries and factory reuse. A new fixed-seed
    matrix adds 648 differential SQL cases. Concurrent tests now use both inputs'
    SYMBOLs with native/mixed/all-Parquet storage. Shared differential assertions
    compare exact names/types even for empty results. Excluded plans and invalid
    SQL retain ordinary behavior. See the [coverage map and validation](docs/parallel-hash-join-group-by-semantics.md).

10. **Rebenchmark V1 after tasks 9a–9e and record rollout** — this update.
    The fixed 100-million-row/four-worker gate passes at **3.312× median
    speedup in both rounds** with active owner/worker circuit breakers. The 51-case matrix
    repeats all 44 historical workloads and adds uncached build/post-join SYMBOL
    predicates and constrained Parquet-cache cases. All **2,320 measured executions
    and 2,965 result comparisons** pass; all 80 cold preparations verify residency.
    The runner now installs the normal network breaker with the server's default
    throttle and no client socket, and complete artifact validation rejects a
    missing matrix case. Production engine code is unchanged. Keep the experimental
    default false: swapped RIGHT and other small/build-dominated cases regress.
    See the [rerun report, measurements and limits](docs/parallel-hash-join-group-by-v1-rerun.md).

The branch supports experimental automatic selection for eligible keyed and
unkeyed queries. **Experimental V1 is complete:** tasks 1–9, 6a, 9a–9e and the task
10 rerun are complete. There are no pending V1 implementation tasks. The rollout
decision retains `cairo.sql.parallel.hash.join.groupby.enabled=false`; accepted
default enablement remains a separate reviewable configuration/planner change.
Keep the global parallel GROUP BY gate and positive-worker requirement. Do not
add a build-size threshold, runtime fallback or consumed-input replay.

## Next RFC task: 11 (after V1)

**Implement parallel radix build as a separate strategy.**

- Define hash-to-shard routing behind the frozen lookup interface, scan eligible
  right frames into slot-owned radix buffers and build independent partitions.
- Preserve all duplicate payloads and common SYMBOL encoding. Publish only after
  build tasks finish; stream left probes through the existing aggregation pipeline.
- Test skew, empty shards, cancellation, cleanup and temporary-memory peaks under
  the existing tracked-native, bounded-heap and execution-allocation contracts.
- Measure end-to-end crossover against owner build before defining strategy
  selection. Do not copy aggregation shard counts/thresholds without evidence.

Task 12's native right/full outer and broader execution extensions remain separate
post-V1 work. The [original task 10 report](docs/parallel-hash-join-group-by-v1.md)
is historical; the [rerun](docs/parallel-hash-join-group-by-v1-rerun.md) qualifies the
implementation after 9a–9e. Its active-breaker configuration means historical
absolute timings are not an isolated before/after measure of breaker overhead.
The earlier ten-million-row Parquet RIGHT pilot remains incomplete; the rerun
repeats the declared one-million-row native/Parquet pair and makes no large-pilot
scaling claim. Allocation/retained-heap reports keep their documented boundaries.

Integration notes: children compile under **one enclosing query registration**.
The factory consumes children/functions/interpreted filter context on constructor
entry, including failure, and borrows joined metadata only during construction.
Functions/filter context must match its positive slot count. Owner-only work
stealing works without running consumer threads; zero configured query workers
keep ordinary execution. Probe filter takeover uses interpreted logical getters,
releases unused JIT handles and composes peeled projection mappings. Ordinary
serial probe filters that cannot be stolen keep the existing plan. Child partition-
format guards continue to request normal recompilation; they are not bypassed.

## Validation for the task 10 rerun

The benchmark package passed with `build-rust-library,qdbr-release`. Eight targeted
smoke workloads passed **388 ordered result checks**, including two concurrent
owners. Shell syntax, five pre-generation CLI guards and artifact-validator
positive/negative checks passed. The final validator requires all **51 cases**, two
rounds and ten runs per arm/owner: **2,320 measured executions and 2,965 result
comparisons**, with zero mismatches. All **80 cold preparations** passed residency
verification. All 44 historical workload references are unchanged; the added LIKE
build and constrained-cache references also match their controls.

The [report](docs/parallel-hash-join-group-by-v1-rerun.md) retains commands, source/jar
hashes, plans, every sample, phase metrics, sampled query-memory peaks, smoke logs,
validation output and the interrupted preliminary no-op run. The final matrix
uses active network breakers, the server-default throttle, unlimited timeout and
no socket; timer reset is timed and worker wrappers bind independently. It includes
9d's successful breaker checks, but does not measure client disconnect syscalls or
replace earlier failure/allocation tests. The production implementation is unchanged;
task 9e's 2,392 passing Java tests at the measured revision are prerequisite evidence,
not a fresh suite run in this benchmark-only change.

## Validation for task 9e

**2,392 Java tests passed across 71 suites**, with 26 existing conditional skips
and zero failures/errors (2,418 total, counting final suite reruns once).
The [semantic coverage guide](docs/parallel-hash-join-group-by-semantics.md) maps
new and rerun tests to SQL column roles, aggregate types, SYMBOL routing/lifetime,
all nine storage pairs, column tops, predicate semantics and exclusions. The
[per-suite results](docs/parallel-hash-join-group-by-semantics/regressions.csv)
include the affected planner, join, aggregate, storage, memory and cancellation
regressions after tasks 9a–9d.

The new seeded matrix adds **648 differential SQL cases**, each candidate run
twice, alongside the existing 720-case matrix. Expanded concurrency coverage
runs **216 executions**, including **18 expected cancellations**, through owner,
legacy and fiber workers, with both inputs' SYMBOL columns, native/mixed/Parquet,
owner/sharded/scalar merges, peer isolation, cleanup and successful reuse.
The benchmark package and all **43 ordered smoke-result checks** passed; the
[smoke output](docs/parallel-hash-join-group-by-semantics/smoke.log.gz) is retained.
This is integration evidence, not the repeated task 10 performance gate.
No production change was required. Earlier task 9c/9d allocation measurements
retain their original boundaries; the completed task 10 latency/rollout rerun is
recorded above.

## Validation for task 9d

**2,384 Java tests passed across 70 suites**, with 26 existing conditional skips
and zero failures/errors (2,410 total, counting final suite reruns once).
The benchmark package and all **43 ordered smoke-result checks** passed.
The [cancellation guide](docs/parallel-hash-join-group-by-cancellation.md) records
loop coverage, normal interruption reasons, query/worker ownership, throttling
bounds and named tests. Its [per-suite results](docs/parallel-hash-join-group-by-cancellation/regressions.csv)
include planner/storage/resource regressions and the repaired scheduler fixtures.

An expanded dispatcher run exposed synthetic tasks reading unopened native
frame caches; test-only sequence subclasses now supply their zero row budgets.
A cleanup-steal fixture now cancels after publication so frame-preparation checks
do not bypass its intended drain phase. The repaired suites pass all 53 and 40
tests, respectively. These are test-fixture changes, with normal scan and explicit
row-budget behavior retained.

The final allocation rerun passes **24 cases, 234 measured candidate executions
and 405 owner/worker windows**, with zero unexplained successful-execution bytes
and all four workers participating in each census case. Native/mixed/Parquet,
owner/sharded/scalar, join normalization, logical conversion, unseen symbols,
forced native growth, close, cancellation and reuse all pass. The [per-thread
summary](docs/parallel-hash-join-group-by-cancellation/allocation/summary.csv) and
paired logs retain the controlled C1 measurement boundary from task 9c.

One initial run exposed a 160-byte SYMBOL-view allocation when two owners
interchanged readers after scheduling-dependent warmup. The harness now seeds
21/42 empty views per source dictionary during reported bounded setup, without
reading symbol text; the count depends only on query expressions and owner/worker
slots. Production pooling and measured-execution assertions are unchanged. The
[cancellation guide](docs/parallel-hash-join-group-by-cancellation.md) retains the
original failure and explains the deterministic setup. The task 10 rerun above
measures end-to-end latency with active breaker checks after task 9e.

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
These measurements preserve task 9b's retention bound. The completed task 10
latency gate is recorded above. The experimental default remains false.

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
integration evidence; the task 10 rerun above follows completed tasks 9c–9e.

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

## Validation for the original task 10 benchmark (historical)

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
