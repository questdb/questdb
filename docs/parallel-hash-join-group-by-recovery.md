# Fused hash join performance recovery (RFC task 9f)

Task **9f passed at commit `4ae9efb0f0`** under [RFC 130](https://github.com/questdb/rfc/discussions/130).
The experimental configuration remains false. The later
[shared-throttle follow-up](parallel-hash-join-group-by-throttling.md) superseded
the counters described below and failed 3 of 54 recovery bounds at `d2e59fc832`.
The current [boundary-check simplification](https://github.com/questdb/questdb/blob/4a05a7eb242e1225e57b3f1bd7d3798059a2820c/core/src/main/java/io/questdb/cairo/sql/async/UnorderedPageFrameReduceJob.java)
passes all 54 recovery bounds in the [task 9g remeasurement](parallel-hash-join-group-by-remeasurement.md)
at `4a05a7eb24`. Its 24-case C1 allocation matrix also passes. An unresolved
swapped-RIGHT warmup JVM crash reopens the execution-reliability prerequisite and
blocks task 10 despite a successful retry. V1 is not yet complete. This report
retains the original investigation and measurements.

## Fixed reference and measurement boundary

The reference is commit `58b1dc04cc`, the original completed task 10 engine and
harness, whose published primary fused medians were 319.808 and 321.527 ms. Its
owner installed `NOOP_CIRCUIT_BREAKER`; worker wrappers delegated to that no-op.
The prechange active-breaker engine/harness is commit `12ff320ae7`. All measured
revisions use this host's AMD Ryzen 9 7900 (12 cores/24 threads), OpenJDK 25.0.4,
Linux 7.0.0-30-generic and `-Xmx8g`. Exact settings and JVM arguments are retained
with each matrix.

The primary dataset, SQL and settings remain fixed: seed 130, 100 million fact
rows, 100,000 dimension keys, 10% country selection, fanout one, five years of
monthly native partitions and four query workers. Both arms freshly filter and
build, scan/probe/aggregate, merge, project, sort and consume the complete ordered
result. Two repetitions each alternate ten measured executions per arm after
three warmups. Candidate timers reset inside measured execution. The normal
network breaker uses the server-default 2,000,000-call throttle, unlimited timeout
and no client socket. This measures active cancellation checks, not disconnect
syscalls or a network protocol.

Controlled comparisons use the same benchmark sources compiled against the fixed
reference jar, then place those classes before each engine jar on the classpath.
The original unmodified reference harness is also reproduced separately. The
`--breaker=noop` option is explicitly diagnostic; the candidate primary gate rejects
it before data generation. It cannot qualify a recovery candidate.

The 27 cases in `benchmarks/parallel-hash-join-group-by-recovery-cases.txt` were
selected before measuring the recovery matrix. They repeat the affected row/pair
pipeline across keyed/scalar, INNER/LEFT/normalized RIGHT, swapped build/probe
orientation, missed/rejected/matched rows, duplicate fanout, compressed and large
group sets, one/two/four workers, concurrent queries and native/mixed/Parquet
storage. Workload parameters come from the existing task 10 script. The broader
51-case rollout matrix, including cold data, tiny scans, build-footprint limits
and additional SYMBOL/cache controls, remains task 10.

Each case and repetition must independently satisfy candidate median latency at
most 1.10 times the lower of its reproduced reference median and the original
task 10 published median. A slower reference reproduction cannot relax the
pinned target. The primary also requires at least
2x ordinary/fused speedup. The validator checks sample completeness, active versus
no-op configuration, logical work counters and identical ordered reference
results. It retains failures and never averages regressions across cases.

## Investigation and implementation

The initial unchanged active-breaker run reproduced 710.474/711.148 ms fused
latency. Build/init/merge occupy little of the primary; the regression is in the
probe phase. JFR Java execution samples mostly land in the inlined probe/lookup
path, not explicitly in the clock method. This sampling result alone does not
quantify clock cost; native perf counters are unavailable on this host
(`perf_event_paranoid=4`). The controlled breaker comparisons and deterministic
clock-call census supply the additional causal evidence.

The time-throttled API checks time and cancellation on every invocation and only
throttles socket polling. The reducer invoked it for every scanned row and joined
pair, in addition to the lookup's existing row-throttled checks. A deterministic
100,000-row rejected scan made 100,034 clock reads before the change.

A first candidate replaced both calls with row-throttled calls. Its primary
medians were 350.442/347.833 ms. A repeated original reference varied from
319.022/318.545 ms to 306.324/306.911 ms, leaving insufficient recovery margin.
These exploratory measurements are retained, including the faster reference.

Matched controls using the common harness separate the main breaker effect from
engine changes: the old reference with active breakers measures 712.829/704.743 ms;
the prechange engine with active breakers measures 705.377/704.611 ms; the same
prechange engine with the diagnostic no-op measures 322.075/319.058 ms. The large
primary regression therefore follows the per-row timing API, rather than a
comparable engine regression between those revisions. Smaller cases still show
JVM/code-generation and run variation: some old-reference common-harness LEFT
runs are slower than their originally published results. The validator uses the
lower reference so that variation cannot weaken acceptance.

The first complete checked-lookup candidate recovers the primary to
331.882/332.127 ms but **fails 11 of 54 individual bounds**: INNER, one-worker
primary, swapped LEFT, 1% matches, zero matches, and one Parquet repetition.
Its [comparison](parallel-hash-join-group-by-recovery/first-comparison.csv) and
[complete raw run](parallel-hash-join-group-by-recovery/first-matrix.tar.gz) are
retained. Further pilots tested a simpler network countdown, a cached direct
network delegate, local row budgets, bounded row chunks, and local statistics.
Direct delegate caching did not resolve the sparse cases; local statistics made
them slower. Those changes were removed. An empty-build shortcut was also
removed before measurement: the all-miss fixture retains one build key.

Row chunks restored primary latency to 321.893/317.541 ms, but the zero-match
case still exceeded its bound. Keeping the collision budget in the acquired
probe removes wrapper calls on each collision; the final pilot passes primary,
1% matches and zero matches, with the latter at 7.832/7.510 ms. All pilot samples,
including failed variants, are in the [tuning archive](parallel-hash-join-group-by-recovery/tuning-pilots.tar.gz)
and [per-case summary](parallel-hash-join-group-by-recovery/tuning-summary.csv).
The expanded collision-budget run then exposed the remaining one-worker and
keyed LEFT regressions (813 ms and 110 ms respectively). It stopped after nine
complete cases with three failing bounds; its [samples and prior validation](parallel-hash-join-group-by-recovery/collision-budget-matrix.tar.gz)
are retained. Separating the tight row/pair loop from frame acquisition, decoding
cleanup and chunk bookkeeping recovers the next pilot to 760 ms for one worker
and 97–98 ms for LEFT, while its two sparse cases pass. The JVM-sensitive layout
of this loop matters in addition to choosing the appropriate breaker API.
The complete helper matrix subsequently **fails nine of 54 bounds**: both
one-worker rounds, both swapped INNER rounds, one 1% match round, both hot-chain
rounds and both concurrent scalar LEFT rounds. Swapped INNER spends most of its
extra time in build; concurrent scalar LEFT regresses in probe/aggregation. This
contradicts treating the faster four-case helper pilot as sufficient evidence.
The full samples and comparison are retained in
[the helper matrix archive](parallel-hash-join-group-by-recovery/chunk-helper-matrix.tar.gz).

A shared local row/pair counter then improves primary latency to 295/293 ms
and the sparse cases, but worsens the hot chain to 987/967 ms; its swapped INNER
and concurrent scalar cases also fail. Restoring the existing checked `next()`
operation removes that local pair counter and recovers the hot chain to 688 ms,
with primary at 307/306 ms. That seven-case pilot still fails four bounds in
swapped INNER and concurrent scalar LEFT. These failed trials are retained in
[the later pilot archive](parallel-hash-join-group-by-recovery/late-pilots.tar.gz).

The next common-reducer pilot passed swapped INNER (33/33 ms), hot chain
(about 630 ms) and concurrent scalar LEFT (177/164 ms). Its full 27-case matrix
nevertheless **fails seven of 54 bounds**: both RIGHT rounds, both swapped INNER
rounds, both swapped LEFT rounds and one concurrent scalar LEFT round. Primary
latency passes at 298.286/300.808 ms (7.762×/7.705×). The pilot and full-run jars
have identical uncompressed entries, including classes and resources; different
compiled source does not explain the contradictory timings. The
[complete failed matrix and its passing regression/allocation checks](parallel-hash-join-group-by-recovery/probe-pairs-matrix.tar.gz)
are retained. Faster pilot measurements cannot replace failed full-matrix cases.

Additional frame-layout and scalar-checked pilots still fail individual bounds.
Separating the scalar reducer and removing keyed branches passes the primary,
one-worker and swapped cases, but fails both hot-chain rounds (761/767 ms) and
both concurrent scalar rounds (254/232 ms). The scalar split and frame-layout
changes are removed. All four additional pilots, including their source patches,
commands and samples, are retained in the
[final tuning archive](parallel-hash-join-group-by-recovery/final-tuning.tar.gz).

Keyed and scalar execution retain the shared reducer. A local row budget is saved
once per frame, with independent probe-owned duplicate and collision budgets.
Rejected rows, misses and null extensions consume row work. Every real check uses
the active slot wrapper's unthrottled API at its configured interval. The sequence
is checked on every row/pair, and slot release and decoder cleanup remain in
nested `finally` blocks. Query binding, clear and failed frames reset row budgets.

The internal `findUnchecked` entry point owns an independent collision countdown
carried across lookups and reset on probe rebinding. It retains the already-read
chain head and avoids clearing the payload address on every lookup: this caller
only reads payload after advancing a match, and outer misses read the joined
record's null side. Each probe caches the frozen table address, mask and payload base on rebinding,
then captures lookup inputs before collision traversal. A growth/rebinding test
exercises the new table capacity and backing through both the existing and periodic
probe APIs. The periodic advance positions the payload directly from its private
row offset and owns its own countdown. Real checks call the independently
bound active wrapper's unthrottled API. Original checked probe APIs are unchanged.

The build consumer keeps its row countdown local, using its owner's actual
configuration (one check per row when no configuration is exposed). Its shared
row-copy helper retains checks in collisions, growth and SYMBOL/native copying;
public `append()` retains its checked entry and failure cleanup. Caching the row
offset and prior chain head avoids reloading them after payload getters. The
frozen row count is derived once from used bytes and fixed row size.

The fresh build cursor also supplies its known remaining row count, when
available, to reserve tracked native row storage before copying. Unknown-size
cursors retain normal growth; this changes capacity allocation, not algorithm
selection. The public two-argument build API remains suitable for partially
consumed or unknown-size cursors. Reservation checks cancellation, validates
multiplication/size limits and participates in the same close-on-failure path.
This removes doubling copies from the swapped builds: the reservation pilot
improves swapped INNER/LEFT to approximately 20–21 ms and RIGHT to 103–104 ms,
but still fails one concurrent scalar round (272.162 ms). Those failures remain
part of the evidence; the reservation pilot does not qualify task 9f.

Restoring the shared reducer with reservation alone passes concurrent scalar
LEFT (224/197 ms), but still fails the hot chain (720/718 ms). Caching frozen
metadata in each probe and reusing one probe-row ID throughout duplicate
traversal improves those cases to 199/155 ms and 709/707 ms respectively; the hot
chain remains just outside the limit. Keeping only the matched-pair metric in a
frame-local variable, flushed in `finally`, then gives 626/629 ms for the hot
chain and 213/194 ms for concurrent scalar LEFT. Failure still accounts for
completed pairs before releasing the slot. The
[metadata/metric pilot archive](parallel-hash-join-group-by-recovery/metadata-metric-pilots.tar.gz)
retains all three variants. These optimizations remove repeated metadata loads,
row-ID construction and metric writes; only the full matrix can qualify them.

The resulting full matrix passes 53 of 54 bounds. Concurrent keyed execution
with four owners misses round zero by 0.037384 ms (99.264214 ms versus a
99.226829 ms limit), a ratio of 1.100414. The strict gate still fails; no tolerance
or reference is changed. Its [complete matrix and source snapshot](parallel-hash-join-group-by-recovery/local-matches-matrix.tar.gz)
are retained. The next candidate avoids writing the same matched/null record
reference repeatedly: it changes the joined side only when the match state
changes. This is local to the fused joined-record view and retains the existing
initialization, typed-null selection and rebinding behavior.

The record-state full matrix passes 52 of 54 bounds. Four-query concurrency
improves to 90.157/90.415 ms and passes, but the 1% match case fails at
35.566/34.842 ms. Its [complete matrix, source and focused validation](parallel-hash-join-group-by-recovery/record-state-matrix.tar.gz)
are retained. Collision traversal is then tightened without changing polling
cadence: its countdown stays local through one collision chain and is stored
back on completion; a throwing check leaves the stored countdown zero. The
native slot address advances directly and wraps at the table boundary, avoiding
index masking and address reconstruction at each collision. The existing dense
collision test verifies wraparound and exact polling across lookups and retries.
The [three-case pilot](parallel-hash-join-group-by-recovery/collision-walk-pilot.tar.gz)
passes 1% matches at 30.835/30.792 ms, zero matches at 8.027/6.968 ms and four-query
concurrency at 80.225/83.543 ms. The full matrix is repeated on that frozen source.

The collision-walk full matrix passes 52 of 54 bounds, including 1% matches
at 31.390/30.942 ms, but the hot chain regresses to 762.137/759.882 ms. Its
[complete matrix and bytecode snapshots](parallel-hash-join-group-by-recovery/collision-walk-matrix.tar.gz)
are retained. The lookup grew from 185 to 222 bytecode bytes; this observation
and the timing changes suggest sensitivity to compilation layout, without proving
a particular inlining threshold. Moving collision traversal into a separate
helper keeps the fast lookup small while retaining local countdown and pointer
advancement. Its [pilot](parallel-hash-join-group-by-recovery/collision-helper-pilot.tar.gz)
passes 1% matches at 31.874/31.409 ms, hot chain at 631.875/629.087 ms and four-query
concurrency at 97.379/85.262 ms.

A subsequent scanned-row metric experiment reused the loop index and flushed it
in `finally`, removing one heap write per row. Its [pilot](parallel-hash-join-group-by-recovery/local-scans-pilot.tar.gz)
passes all six bounds, but its [full matrix](parallel-hash-join-group-by-recovery/local-scans-matrix.tar.gz)
fails both LEFT rounds (109.697/108.410 ms) and both hot-chain rounds
(819.635/808.435 ms). That experiment is removed. The final candidate restores
the exact collision-helper source and packaged jar that passed its focused tests
and pilot; source equality is checked before freezing the full-run artifact.
Scanned-row accounting remains in the slot, while only matched-pair accounting
is accumulated locally and flushed during frame cleanup.

Earlier profiling also exposed code-layout sensitivity. A scalar helper using
the new lookup failed at 461/429 ms, while matched JFR diagnostics measured
240/243 ms for that variant and 187/176 ms for the earlier checked lookup.
Profiling changes observed latency, so these are attribution evidence. Of 4,322
sampled stacks under the fused factory, 2,238 end in the new collision lookup,
versus 1,114 of 3,034 in the earlier static lookup. The
[filtered CPU stacks](parallel-hash-join-group-by-recovery/scalar-cpu-summary.txt)
and full recordings in [the later pilot archive](parallel-hash-join-group-by-recovery/late-pilots.tar.gz)
retain this contradictory evidence and the inlining/native-attribution limits.

The global network breaker and slot wrapper implementations are unchanged by
this task. A new test verifies the existing exact first/every-N cadence, explicit
checks, timer resets, invalid throttle values and cancellation retries. All slots
bind independent wrappers before probing. Frame/job boundary checks, filtered
source work, decoding, merges, output and mandatory drain retain their work bounds.
The budgets and cached lookup metadata add five fixed integers and two native
address fields across each slot/probe; no growing heap structure is added. Reserving known row storage remains charged to the
query and can reduce transient doubling capacity. Memory-limit, cleanup and reuse
checks cover that path.

## Final recovery results

All **54 case/repetition comparisons pass** across the fixed 27 cases. Every
candidate median is at most 1.10 times its pinned limit; the largest ratio is
**1.0744**. The primary fused medians are **304.162 and 304.092 ms**, with
**7.755× and 7.739×** ordinary/fused speedup. Both primary requirements pass in both rounds.

| Round | Original published reference, ms | Reproduced reference, ms | Active candidate, ms | Candidate / lower reference | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 319.808 | 313.995 | 304.162 | 0.9687 | 7.755× |
| 2 | 321.527 | 312.123 | 304.092 | 0.9743 | 7.739× |

The [complete comparison](parallel-hash-join-group-by-recovery/comparison.csv)
contains every individual bound. Each arm has **1,360 measured executions and
1,741 result comparisons** (2,720 and 3,482 across reference/candidate matrices).
All ordered references and logical work counters agree. Per-case plans, every
sample, medians/spreads, phase timings and memory observations are retained in
[the reference summary](parallel-hash-join-group-by-recovery/reference/summary.csv),
[the final candidate summary](parallel-hash-join-group-by-recovery/candidate/summary.csv)
and their adjacent logs. The [raw final matrix archive](parallel-hash-join-group-by-recovery/final-matrix.tar.gz)
preserves exact output before whitespace normalization.

[Primary phase profiles](parallel-hash-join-group-by-recovery/primary-phases.csv)
compare build, sequence initialization, probe/aggregation, merge and remaining
ordered-consumption/cleanup time for each control and the final run. The existing
`init_ns` metric covers sequence binding; dispatch preparation, frame navigation
and decoding are included in `probe_ns`. JFR stack sampling supplements these
wall-clock timers and has inlining/native-attribution limits. The primary's
large regression and recovery occur in probe/aggregation; fresh build, binding,
merge and final consumption remain inside the end-to-end boundary.

The measured engine is commit `4ae9efb0f0`, identified before commit by
parent `12ff320ae7`, the
[complete core/benchmark patch](parallel-hash-join-group-by-recovery/final-source.patch.gz)
and [jar/source hashes](parallel-hash-join-group-by-recovery/final-artifacts.sha256).
Its allocation and latency runs use the same frozen jar. No compilation, tests,
allocation profiling or competing benchmark JVMs overlap final latency collection.

The first control runner was paused between controls for validation/refinement.
Its root environment records the earlier 350-ms candidate; the first failed
matrix's own environment and archived `final-candidate.sha256` identify the
actually measured `refined.jar`. The reference matrix was frozen before later
pilots and never replaced. The earliest exploratory reference had a brief common-
harness compilation overlap and is retained as exploratory evidence; the complete
control/reference/final matrices run sequentially. These distinctions prevent
an exploratory jar or profiled timing from being presented as acceptance evidence.

## Validation

All **2,400 Java tests across 71 suites pass**, with 26 conditional skips and
zero failures/errors (2,426 total). The [per-suite results](parallel-hash-join-group-by-recovery/regressions.csv)
and [full final log](parallel-hash-join-group-by-recovery/final-regressions.txt.gz)
cover the affected planner, joins, aggregates, SYMBOL/storage, memory, scheduler,
breaker ownership, peer isolation, mandatory drain and reuse regressions.

The preliminary run's duplicate-test predicate was moved into the build by the
optimizer. The corrected fixture uses a null-accepting outer predicate and asserts
that all 100,000 pairs are actually visited. Later direct-build allocation tests
exposed 1,704–2,280 bytes after the original 20 small warmups. With TLABs disabled and JFR recording allocations outside TLABs, the trace
attributed exactly 2,232 bytes to 31
Strings and 31 byte arrays at the test's probe-loop backedge. The
[filtered stacks](parallel-hash-join-group-by-recovery/build-allocation-summary.txt)
and [failed/passing logs](parallel-hash-join-group-by-recovery/tuning-test-logs.tar.gz)
are retained. Increasing the fixed 512-row warmup to 200 executions removes this
VM setup effect; the two measured fresh builds still introduce 65,536 symbols,
force native growth, and require zero bytes without new exemptions. This test
adjustment does not change benchmark warmups. The independent C1 byte/site matrix
continues to enforce the production allocation boundary.

The final benchmark package passes. The [allocation rerun](parallel-hash-join-group-by-recovery/allocation/summary.csv)
passes **24 cases, 234 measured executions and 405 owner/worker windows**, with zero
unexplained bytes and all four workers participating in every census case. It uses
the established controlled-C1, exact-byte/site boundary, now with the shared
benchmark context's active network breaker (the allocation configuration retains
its test throttle of five). Native/mixed/Parquet, owner/sharded/scalar, concurrent
owners, normalized RIGHT, logical conversion, forced growth, output, cancellation
cleanup and reuse all pass. This preserves the prior allocation scope/exemptions;
it is not a claim about application result-list allocation or all JVM configurations.

`AsyncHashJoinGroupByTest` adds deterministic network-breaker
clock-call and bounded cancellation/timeout tests for rejected rows, all misses,
outer null extension and long duplicate chains, keyed/scalar and enabled storage.
The clock census fixes frames at 4,096 rows and uses a test-only stack observer
to count reads under the reducer, independently of scheduler polling and frame
preparation outside it. A prior total-clock census was sensitive to those calls;
its failure and passing isolated/suite reruns are retained. The counter still
rejects a clock read on every probe row or duplicate. The same scoped census
[fails on the prechange engine and passes on the final engine](parallel-hash-join-group-by-recovery/clock-regression-summary.txt);
[exact commands and both logs](parallel-hash-join-group-by-recovery/clock-regression.tar.gz)
retain that regression control. The tests verify that the
fixture visits all 100,000 rows/pairs, then check native
cleanup, slot release and same-factory ordinary-result reuse after interruption.
Existing cancellation tests now assert the configured throttle bound rather than
requiring the former per-call timing API to throw on precisely the next row.

`IntHashJoinBuildTest` additionally checks bulk-build polling frequency, cancellation
and timeout within 64 rows, released native memory and same-builder reuse. It
also verifies exact tracked row reservation, overflow cleanup and reuse. It
covers exact collision- and duplicate-check cadence across
lookups, interruption inside a long collision chain, active network and shared
atomic rebinding, expired probe generations, peer independence and reuse.
`NetworkSqlExecutionCircuitBreakerTest` checks the unchanged first/every-N cadence,
explicit checks, reset/rearm/clear, invalid throttle values and cancellation retry.

The recovery validator has passed deliberate missing-case/sample, disabled-breaker,
changed-reference-result and greater-than-10% regression guards, plus its positive
control. Three CLI guards also reject disabled primary breakers and invalid modes
before data generation. Shell syntax checks pass. The final recovery results above satisfy both performance gates.

## Reproduction

Build `58b1dc04cc` and `12ff320ae7` in separate checkouts with
`mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true` (use
`-P build-rust-library,qdbr-release` for the prechange/candidate tracked decoder).
Build this branch with the native profiles, then run:

```bash
bash benchmarks/parallel-hash-join-group-by-recovery.sh \
    /path/to/reference/benchmarks.jar /path/to/prechange/benchmarks.jar \
    /tmp/new-recovery-results
```

The script compiles the common harness once before measuring, retains exact
commands/source/jar hashes and runs all JVMs sequentially. Do not overlap tests,
compilation, allocation profiling or other benchmarks with latency collection.
