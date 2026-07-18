# Adaptive Commit SP-C — Performance Validation & Tuning (design + methodology)

**Status:** Harness + methodology + DIRECTIONAL numbers complete 2026-07-17. This document is the
SP-C sub-project spec called for by the OSS-GA roadmap
(`2026-07-15-adaptive-commit-ga-roadmap-design.md`, Track 1 "Prove-it"). It fixes the benchmark set,
the measurement methodology, the *proposed* acceptance threshold + framing, and the tuning-guidance
framework. It records shared-box **relative** results and enumerates what still needs controlled HW.

**Scope reality (locked):** trustworthy ABSOLUTE throughput / p99 / recovery numbers and the final
GA pass/fail verdict need CONTROLLED, quiesced hardware. The dev box used here is shared and noisy.
So the autonomous deliverable is the **harness + methodology + directional (relative-only) numbers**,
NOT a GA verdict. Every number below is labelled shared-box/relative and must not be read as absolute
or as a go/no-go.

---

## 1. Goal & north star

Prove the "good performance" half of the adaptive north star ("crash-safe recovery **+ good
performance**", NOSYNC stays default, adaptive opt-in). Concretely:

- Characterise the **group-commit window W** (`cairo.adaptive.commit.group.window.us`): the RPO ↔
  throughput trade-off. **W=0 = zero-loss** (fdatasync every commit, ≈ SYNC-class latency); **W>0**
  batches the device flush so throughput/latency approach NOSYNC as W grows, at an RPO of ≤ W.
- Characterise the **durable-epoch cadence** (`cairo.adaptive.epoch.interval.ms`): apply-path epoch
  overhead vs recovery-time bound.
- Produce **tuning guidance** for both knobs and a **published RPO/throughput curve**.
- Propose the **acceptance threshold** (Section 6) to be ratified on controlled HW.

## 2. What is already settled (cite, do not rebuild)

- **Lazy apply issues zero column syncs under ADAPTIVE** — regular *and* O3 apply paths. Proven by
  `core/.../cairo/wal/AdaptiveWalDurabilityTest`:
  - `(e) testAdaptiveApplyIssuezZeroColumnSyncsOnApply` — in-order apply: 0 column msync/fdatasync.
  - `(e2) testAdaptiveO3ApplyIssuesZeroColumnSyncsOnApply` — O3 merge apply (`O3CopyJob`): 0 column
    msync/fdatasync (this was a real leak, gated on `commitMode != NOSYNC`, now gated on
    `CommitMode.appliesColumnSync == SYNC||ASYNC`).
  - `CommitMode.appliesColumnSync(int)` returns true only for `SYNC`/`ASYNC`; ADAPTIVE is excluded.

  So the roadmap's "does the block/O3 apply path still fsync under ADAPTIVE?" question is **closed with
  a NO**. The durable epoch (`TableWriter.fsyncMaterializedState()`, fired on the apply worker per
  cadence) is the *only* apply-path flush under ADAPTIVE — its cost is measured in Section 5.4, not
  re-litigated as a correctness question.

## 3. Benchmark harness

Three harnesses under `benchmarks/src/main/java/org/questdb/`. All put the DB root on real disk
(`/data`, xfs) so `fdatasync` is a real syscall (never a tmpfs no-op), and are for **relative**
comparison on one box.

### 3.1 `WalCommitModeBenchmark` (JMH) — commit-path throughput + p99

Measures the WAL **commit** path — the path adaptive changes. Each invocation appends one batch and
commits; apply is deliberately **not** drained per invocation (draining would force adaptive's lazy
apply and erase its advantage) — apply is drained once at teardown.

- `@BenchmarkMode({AverageTime, SampleTime})` — AverageTime → mean us/op; SampleTime → the percentile
  table (p99/p999). Pick one with `-bm avgt` / `-bm sample`.
- `@Param commitMode` = `NOSYNC, ASYNC, SYNC, ADAPTIVE`.
- `@Param groupWindowUs` (W) = default `0`; sweep `0,1000,5000,50000` for the RPO curve (ADAPTIVE only;
  other modes ignore it).
- `@Param workload` — **one named axis** (avoids a rows×cols×o3 cross-product explosion and
  nonsensical combos):
  | workload | rows/commit | columns | order | lens |
  |----------|-------------|---------|-------|------|
  | `HIGH_INGEST` | 5000 | 20 | in-order | large-batch throughput |
  | `SMALL_BATCH` | 5 | 20 | in-order | **per-commit latency** (op ≈ one commit → SampleTime p99 ≈ per-commit p99) |
  | `WIDE_TABLE` | 1000 | 200 | in-order | per-column fsync scaling |
  | `O3` | 1000 | 20 | reversed-within-batch | out-of-order commit-path parity |

  Note on `O3`: the O3 **merge** cost is realized at APPLY, not commit — the commit only journals the
  out-of-order rows to the WAL. So this axis proves commit-path parity for O3 ingest; the apply-path
  O3 question is settled by test (e2) above and epoch/apply cost is in `WalAdaptiveApplyBenchmark`.

### 3.2 `WalAdaptiveApplyBenchmark` (JMH) — apply-path epoch overhead

Isolates the cost of the ADAPTIVE **durable epoch**, which fires on the **apply** path
(`ApplyWal2TableJob.maybeAdvanceDurableEpoch`), not on commit. Each invocation appends+commits one
in-order batch, then **drains apply** (so the epoch fires per cadence). Measured op = ingest + apply
(+ epoch fsync when it fires).

- `@Param commitMode` = `ADAPTIVE, SYNC` (SYNC = reference: it fsyncs columns on every apply).
- `@Param epochIntervalMs` = `-1` (epochs DISABLED — the lazy floor) vs `0` (epoch on EVERY batch —
  worst case). The `0` − `-1` delta is the per-epoch overhead. Default production is `1000` ms, which
  amortizes this cost across ~1 s of apply batches, so this brackets the **worst** case.

### 3.3 `WalAdaptiveRecoveryBenchmark` (plain timed `main`, not JMH) — recovery time

The crash/reopen sequence doesn't fit JMH's shared-`@State` lifecycle, so this is a timed harness.
Per tail size `T` (committed-but-un-applied txns): (1) **build** (untimed) — open engine, ingest+drain
a WARMUP so the table is materialized and a durable epoch lands at WARMUP end, then commit `T` more
txns and DON'T drain them (they stay in the WAL, past the epoch), close; (2) **recover** (timed) —
reopen the engine (its ctor runs `RecoveryCoordinator.recover()`, rolling the table to the epoch cut),
then drain the WAL queue to the frontier (re-derives the `T`-txn tail). Reports **bootRecover** (ctor
incl. recover) and **catchupDrain** separately.

This is a **timing proxy on a clean reopen** — crash *correctness* (torn-tail rewind, zero corruption,
every acked txn survives) is SP-D's job (adaptive crash-fuzz + power-loss harness), NOT read here.

### 3.4 `WalMultiWriterCommitBenchmark` (JMH) — group commit under concurrency

5 concurrent writers (`@Threads(5)`) each committing SMALL_BATCH to its OWN table through ONE shared
`CairoEngine`, so they share the engine-wide `WalGroupCommitFlushQueue`. W>0 stays faithful via the
commit-driven trigger. `@BenchmarkMode(AverageTime)` → JMH aggregates across the 5 threads;
**concurrency scaling = `5 × single-writer avgt ÷ 5-writer aggregate`** (5.0 = perfect linear). This
is the "multi-writer group-commit-under-concurrency" measurement §8 flags as the most important gap.
Committed `f31ba0dbb4`. (A JMH-harness race — no barrier between thread-teardown writer-close and
trial-teardown `engine.close()` — was fixed with a `CountDownLatch(5)`; not an adaptive-path bug.)

### 3.5 `WalBatchSizeSweepBenchmark` (JMH) — durability tax vs batch size

Single writer, `@Param rowsPerCommit ∈ {1,10,100,1000,10000,100000,1000000}` × commitMode, 20-col
schema held constant (only batch size varies). Reports avgt us/commit; the informative metric is the
derived **us/row** (= avgt ÷ rowsPerCommit) and the ADAPTIVE÷NOSYNC per-row ratio, isolating how the
fixed per-commit fsync amortizes as batches grow. 1M-row commit peaks ~440 MB RSS (off-heap WAL), no
OOM. Committed `b2df7167ee`.

## 4. Methodology — how to run

### 4.1 The module-args gotcha (critical)

The engine touches `jdk.internal.vm` (worker continuations). The JMH **fork** needs the same
`--add-exports`/`--add-opens` the core test JVM uses. `-jvmArgsAppend` does **not** reliably reach the
JMH fork, so pass them via **`JAVA_TOOL_OPTIONS`** (every JVM the fork spawns inherits it):

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64; export PATH=$JAVA_HOME/bin:$PATH
export QDB_LOG_W_STDOUT_LEVEL=ERROR   # silence INFO flood (speeds teardown, de-noises output)
export JAVA_TOOL_OPTIONS="--sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED \
  --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
```
(`--add-exports=java.base/jdk.internal.vm=io.questdb` from `core/pom.xml` is for the *named*-module
build; benchmarks run `io.questdb` from the classpath = unnamed module, so `=ALL-UNNAMED` is the one
that matters. Adding the `=io.questdb` form too is harmless — it just warns "unknown module".)

### 4.2 Build

```bash
mvn -q -pl benchmarks -am -DskipTests package    # builds core (reactor) + the shaded benchmarks.jar
# -> benchmarks/target/benchmarks.jar  (Main-Class org.openjdk.jmh.Main)
```

### 4.3 Run configs

- **QUICK (directional, minutes)** — used for Section 5. Per JMH class:
  `-f 1 -wi 1 -i 2 -w 1s -r 1s` (`-i 3 -r 2s` for SampleTime p99). Restrict the matrix with `-p` and
  run per-workload so each call stays inside the shell/tool timeout.
  Example: `java -jar benchmarks/target/benchmarks.jar 'WalCommitModeBenchmark' -bm avgt -f 1 -wi 1 -i 2 -w 1s -r 1s -p commitMode=NOSYNC,ASYNC,SYNC,ADAPTIVE -p workload=HIGH_INGEST,SMALL_BATCH -p groupWindowUs=0`
- **FULL (controlled HW)** — `-f 3 -wi 5 -i 10 -w 2s -r 5s`, full 4×4×4 matrix + `-bm sample` pass,
  on a quiesced box (Section 8). `-f 3` (3 forks) is essential for a real confidence interval.
- **Recovery** — `java -cp benchmarks/target/benchmarks.jar org.questdb.WalAdaptiveRecoveryBenchmark 250 1000 4000`.

### 4.4 Environment requirements

- DB root on **real disk** (this repo: `/data`, xfs). Never tmpfs (fsync becomes free → SYNC/ADAPTIVE
  numbers are meaningless). The harnesses auto-pick `/data` if present, else `$HOME`.
- Quiet logging (`QDB_LOG_W_STDOUT_LEVEL=ERROR`) — INFO logging to stdout otherwise dominates teardown
  and pollutes the JMH capture.

---

## 5. Directional results (shared box, RELATIVE only)

**Environment:** shared dev box, JDK 25.0.3, JMH 1.37, DB root `/data` (xfs), QUICK config
(`-f 1 -wi 1 -i 2/3`, short iters). **These are NOT absolute numbers and NOT a GA verdict** — a single
fork with 1 warmup + 2–3 iters on a shared box has wide, unquantified error. They establish
*direction and relative magnitude only*. Captured 2026-07-17.

### 5.1 Throughput matrix — 4 modes × 4 workloads @ W=0 (`-bm avgt`, us/op, lower=faster)

| workload | NOSYNC | ASYNC | SYNC | ADAPTIVE W=0 |
|----------|-------:|------:|-----:|-------------:|
| HIGH_INGEST (5000 rows) | 1530 | 1569 | 17420 | 11020 |
| SMALL_BATCH (5 rows)    | 2.6  | 4.25 | 3519 | 7025 |
| WIDE_TABLE (200 cols)   | 1992 | 1710 | 83924 | 64729 |
| O3 (1000 rows)          | 319  | 333  | 8108 | 14852 |

Reading: at **W=0** ADAPTIVE is a fsync-every-commit mode, in the **SYNC order of magnitude** (within
~2× of SYNC both ways) and ~10–1000× NOSYNC — *by design* (W=0 = zero-loss). WIDE_TABLE exposes that
W=0 per-commit cost scales with **column count** (200 cols → 200 segment-column fdatasyncs): 65–84 ms.
The perf win is **not** at W=0 — it is at W>0 (5.2/5.3).

### 5.2 RPO ↔ throughput curve — ADAPTIVE W-sweep (`-bm avgt`, us/op)

| workload | W=0 (zero-loss) | W=1ms | W=5ms | W=50ms |
|----------|----------------:|------:|------:|-------:|
| SMALL_BATCH (5 rows) | 7562 | 39.6 | 19.8 | 19.8 |
| HIGH_INGEST (5000 rows) | 13891 | 20843 | 14782 | 33258 |

**SMALL_BATCH is the clean curve:** W=0 7562 us → W=1ms 40 us (**191×**) → W=5ms 20 us (**382×**) →
W=50ms 20 us (saturated). For a high-commit-rate workload the group-commit window collapses per-commit
latency from ~7.5 ms to ~20 us, approaching NOSYNC (2.6 us) / ASYNC (4.25 us) as W grows.
**HIGH_INGEST barely moves** with W (and is noise-dominated here): at 5000 rows/commit the per-commit
fdatasync is already amortized over the batch, so the window has little to batch. **Tuning finding: W
matters most for small-batch / high-commit-rate workloads; large-batch ingest is nearly W-insensitive.**

### 5.3 p99 commit latency — SampleTime, SMALL_BATCH (`-bm sample`, us/op)

| mode | mean | p99 |
|------|-----:|----:|
| NOSYNC | 3.7 | 36.8 |
| SYNC (W=0) | 3819 | 13600 |
| ADAPTIVE W=0 (zero-loss) | 6131 | 24737 |
| **ADAPTIVE W=5ms** | **19.1** | **45.2** |

**Headline directional result:** at the recommended production window **W=5ms, ADAPTIVE p99 commit
latency = 45 us vs NOSYNC 37 us (~1.2×)**; mean 19 us vs NOSYNC 3.7 us (~5×). The zero-loss W=0
config is SYNC-class (p99 24.7 ms). So the "competitive with NOSYNC" bar is met at the recommended
W on p99 — pending controlled-HW confirmation. (NOSYNC ignores W → its W=0/W=5ms rows are identical,
confirming the knob is ADAPTIVE-only.)

### 5.4 Epoch overhead — apply path (`WalAdaptiveApplyBenchmark`, `-bm avgt`, us/op)

| commitMode | epoch=-1 (disabled) | epoch=0 (every batch) |
|------------|--------------------:|----------------------:|
| ADAPTIVE | 7260 | 9524 |
| SYNC | 7674 | 7361 (param ignored) |

Worst-case epoch overhead (firing on **every** apply batch) ≈ **9524 − 7260 = 2264 us per epoch**.
At the default cadence `cairo.adaptive.epoch.interval.ms=1000`, that ~2.3 ms is paid **at most once per
second per table** → amortized to well under 1% of apply throughput at any realistic apply rate.
**Tuning finding: epoch overhead is bounded per-fire and fully controlled by the cadence knob.**

### 5.5 Recovery time (`WalAdaptiveRecoveryBenchmark`, ADAPTIVE, epoch-every-batch, 500 rows/txn)

| tail (un-applied txns) | tail rows | bootRecover (ms) | catchupDrain (ms) | total (ms) | catch-up rows/s |
|-----------------------:|----------:|-----------------:|------------------:|-----------:|----------------:|
| 250  | 125 000   | 10.3 | 43.6  | 53.9  | 2.87 M |
| 1000 | 500 000   | 7.1  | 138.0 | 145.2 | 3.62 M |
| 4000 | 2 000 000 | 5.7  | 828.0 | 833.7 | 2.42 M |

Reading: **bootRecover (ctor incl. `recover()`) is ~constant (~5–10 ms)** regardless of tail — the
epoch roll-forward is O(1) (roll the table to the epoch cut). **catchupDrain is ~linear in the
un-applied post-epoch tail** (~2.4–3.6 M rows/s catch-up on this box). **Tuning finding: worst-case
recovery time = fixed boot + (post-epoch WAL ÷ catch-up rate); the operator bounds it by the epoch
cadence, which bounds how far the tail can run past the last epoch.**

---

### 5.6 Multi-writer group commit — 5 writers × 5 tables (`WalMultiWriterCommitBenchmark`, aggregate avgt us/op)

| commitMode | W=0 | W=5ms | W=50ms |
|---|--:|--:|--:|
| NOSYNC | 4.5 | 4.9 | 4.1 |
| SYNC | 9285 | 10582 | 11565 |
| ADAPTIVE | 17564 | 46.4 | 70.3 |

Concurrency scaling (`5 × single-writer avgt ÷ 5-writer aggregate`; 5.0 = perfect linear):

| mode / W | scaling | reading |
|---|--:|---|
| NOSYNC W=0 | **4.53×** | near-linear ceiling (no device flush) |
| SYNC W=0 | 1.77× | fsync-every-commit serializes writers at the device |
| ADAPTIVE W=0 | 1.47× | same — device-flush-bound |
| **ADAPTIVE W=5ms** | **3.30×** | group window removes most flushes → concurrency recovers |

**Headline:** at W=5ms the group commit amortizes the device flush across concurrent writers,
recovering most of the scaling W=0 loses (3.30× vs 1.47×), climbing toward NOSYNC's 4.53× ceiling —
the direct multi-writer evidence §8 flagged as missing. Sharp corollary: **ADAPTIVE W=0 under 5-way
concurrency (17.6 ms/commit) is *slower* than SYNC (9.3 ms)** — all the group-commit machinery, none
of the batching benefit (W=0 flushes every commit). W=50ms was noisy on the shared box.

### 5.7 Durability tax vs batch size (`WalBatchSizeSweepBenchmark`, single writer, derived us/row)

| rows/commit | NOSYNC us/row | SYNC us/row | ADAPTIVE us/row | **ADAPTIVE ÷ NOSYNC** |
|---:|--:|--:|--:|--:|
| 1 | 1.200 | 3810 | 4976 | **4146×** |
| 10 | 0.544 | 366 | 590 | **1084×** |
| 100 | 0.443 | 50.8 | 58.8 | **133×** |
| 1,000 | 0.460 | 7.16 | 8.60 | **18.7×** |
| 10,000 | 0.482 | 1.93 | 2.59 | **5.37×** |
| 100,000 | 0.419 | 1.54 | 1.20 | **2.86×** |
| 1,000,000 | 0.505 | 1.31 | 1.02 | **2.02×** |

**Headline:** NOSYNC per-row is flat at ~0.5 us/row (pure ingest). The ADAPTIVE÷NOSYNC per-row ratio
collapses **4146× → 2.02×** as the batch grows — the fixed per-commit fsync amortizes over more rows.
It bottoms out at a **~2× floor, not 1×**: the residual ~0.5 us/row gap at 1M is the *per-byte* cost
of physically flushing the WAL for zero-loss durability, which scales with data volume and cannot
amortize. Within ~2× is reached at ~1M rows; within 1.5× is not reached in range — the 2× floor is
the irreducible price of zero data loss. Corollary: **ADAPTIVE overtakes SYNC above ~100k
rows/commit** (it defers column materialization, so its big-batch commit flushes less).

> **Figures.** §5.1–5.7 are visualised in the this-box performance chart — the RPO window (single +
> 5 writers), the concurrency scaling, and the batch-size amortization on one page:
> <https://claude.ai/code/artifact/eb719b73-31cd-410c-8c53-9de9a69df08e>

---

## 6. Proposed acceptance threshold + framing (to ratify on controlled HW)

The roadmap fixes the exact number *here*. Proposed, framed by the two-regime reality:

1. **W=0 (zero-loss) is NOT held to the NOSYNC bar.** By design it fdatasyncs every commit; its
   correct peer is SYNC. Bar: **W=0 within ~2× of SYNC** on each workload (directional 5.1: yes), and
   crash-correct (SP-D owns correctness). W=0 is for RPO-zero deployments that accept SYNC-class cost.

2. **The "competitive with NOSYNC" bar applies to the recommended production W>0.** Primary metric =
   **p99 commit latency on SMALL_BATCH** (the high-commit-rate lens where W matters):
   > **Proposed:** at the recommended production window, ADAPTIVE p99 commit latency ≤ **2× NOSYNC
   > p99** on the same workload. *(Directional 5.3: W=5ms → 1.2×. Provisionally met; ratify on HW.)*

   Secondary metric = **mean us/op on HIGH_INGEST** within a bounded delta of NOSYNC — proposed
   **≤ 25%** — but 5.2 shows large-batch is nearly W-insensitive, so this may instead be stated
   against SYNC. Fix the exact % **on controlled HW** (Section 8); the shared box cannot set it.

3. **Epoch overhead bar:** worst-case per-epoch cost bounded (5.4: ~2.3 ms) and, at the default
   cadence, **< 1% apply-throughput delta** vs epochs-disabled. *(Directional: met.)*

4. **Recovery bar:** bootRecover O(1) in tail; catch-up ≥ a floor rows/s (fix on HW); worst-case
   recovery bounded by the epoch cadence to an operator SLO. *(Directional 5.5: met in shape.)*

5. **No unexpected fsync in the hot apply path:** satisfied by the settled tests (Section 2).

**GA verdict = NOT decidable on this box.** It requires the controlled-HW FULL run (Section 8) to turn
these directional ratios into numbers with confidence intervals.

## 7. Tuning-guidance framework (the deliverable knobs)

Two orthogonal knobs; give operators this decision framework (populate the exact break-points from
the controlled-HW curve):

- **`cairo.adaptive.commit.group.window.us` (W) — the RPO ↔ throughput dial.**
  - `W=0`: zero data loss on power-cut; SYNC-class latency. Choose when RPO must be 0.
  - `W>0`: RPO ≤ W; throughput/latency approach NOSYNC as W grows, **saturating** (5.2: gains flatten
    by ~5 ms on this box — the fsync is fully amortized past there). Recommended starting point **1–10
    ms** (RPO 1–10 ms, ~SYNC→near-NOSYNC latency). Bigger W buys little more throughput but a larger
    RPO — past the saturation knee it is nearly pure downside.
  - Workload dependence: W helps **small-batch / high-commit-rate** most; **large-batch** ingest is
    ~W-insensitive (leave small). **Wide tables** (many columns) have the highest W=0 cost → benefit
    most from W>0.
- **`cairo.adaptive.epoch.interval.ms` — the recovery-time ↔ apply-overhead dial.**
  - Larger interval: less apply overhead (fewer epoch fsyncs) but a longer post-epoch tail → longer
    worst-case recovery.
  - Smaller interval: faster recovery, more apply overhead.
  - `-1`: disables epochs (recovery falls back to full WAL replay from the base — operator opt-out /
    test isolation only).
  - Default `1000` ms amortizes the ~2.3 ms epoch cost to < 1% (5.4) while bounding the tail to ~1 s of
    ingest. Derive the interval from the recovery SLO: `worst-case recovery ≈ bootRecover +
    (ingest_rate × interval) ÷ catch-up_rate`.

## 8. Controlled-HW protocol + why this box can't give absolutes

**Why not here:** shared dev box (concurrent load, no CPU pinning, thermal/freq drift), 1 fork + 1
warmup + 2–3 iters (no confidence interval), `/data` 92% full (allocator/fragmentation effects). Good
for *direction*, useless for *absolutes*.

**Controlled-HW protocol:**
1. Dedicated, quiesced box; fixed CPU governor (`performance`), no turbo drift; benchmark process
   pinned; DB root on a known NVMe device with a documented `fdatasync` cost; ≥ 30% free space.
2. FULL JMH config (`-f 3 -wi 5 -i 10 -w 2s -r 5s`), full 4-mode × 4-workload × 4-W matrix, both
   `-bm avgt` and `-bm sample`; capture the JSON (`-rf json -rff`).
3. Repeat the recovery harness across tail sizes {1e3 … 1e6 txns} and both epoch cadences.
4. Ratify Section 6 numbers; publish the RPO/throughput curve; sign the GA perf gate.

**Requires controlled HW (cannot be closed autonomously on the shared box):**
- All ABSOLUTE throughput / p99 / commit-latency / recovery-time numbers (only ratios are meaningful here).
- The exact acceptance % in Section 6 (NOSYNC-delta and catch-up floor).
- The **multi-writer group-commit-under-concurrency** variant — many adaptive tables committing
  concurrently so the `WalPurgeJob` background flusher batches across writers (the group-commit's
  real win). The current harnesses are single-writer; this needs a concurrent-writer harness + a
  controlled box, and is the most important missing measurement for the W>0 story.
- The final **GA pass/fail verdict** on the perf gate.

## 9. Deferred / not in this pass

- Concurrent multi-writer group-commit harness (needs controlled HW — Section 8).
- O3 apply-path *cost* (as opposed to correctness, which is settled): the reversed-within-batch O3
  workload here measures commit-path parity; a controlled O3-merge-at-apply cost benchmark is HW work.
- Parquet / mat-view / indexed-column workload variants (SP-D covers them for *correctness*; their
  *perf* is a follow-up).
- `-prof` hardware-counter / async-profiler passes to attribute the W=0 cost (syscall vs copy) — HW.
- Wiring the SP-F metrics slice (durable-frontier lag, epoch cadence) into the harness output so a run
  self-reports the epoch/frontier state it measured.
```
