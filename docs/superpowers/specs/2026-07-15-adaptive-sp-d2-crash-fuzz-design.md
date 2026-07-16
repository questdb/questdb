# SP-D D2 — Randomized adaptive crash-fuzz — Design

**Parent:** `2026-07-15-adaptive-sp-d-crash-validation-design.md` (SP-D, Track 1 "Prove-it"); the roadmap
`2026-07-15-adaptive-commit-ga-roadmap-design.md`. **Status:** design approved (full-op fingerprint
oracle; CI-fast default + nightly override). OSS core, branch `nw_adaptive_commit`, JDK25.

**Goal:** Prove "crash-safe, no corruption" for adaptive commit mode against *randomized* workload
**shapes** — arbitrary interleavings of inserts, O3, schema changes, truncate, drop-partition,
replace-range dedup, TTL, and parquet conversion — beyond D1's hand-picked deterministic workloads. D1
swept the *crash-point* dimension exhaustively on fixed shapes; D2 adds the *shape* dimension by wiring
the existing seeded WAL fuzzer to the existing crash+recover harness (a clean, currently-unwired
intersection). This is go/no-go durability evidence that gates GA.

## Grounding — reuse surface (all confirmed against the code)

- **Sweep driver (reuse unchanged):** `AbstractAdaptiveCrashSweepTest.forEachAdaptiveCrashPoint(
  AdaptiveCrashWorkload workload, int cap)` — count-pass to learn `N = durabilityOpCount()` for this
  workload, then sweep `k = 1..min(N, cap)`: `setup(k)` → arm `crashFf.armCrashAt(base + k)` → `commit()`
  (expects a `CrashSimulationError` **or** a swallowed suspend) → `recoverAfterCrash(tokens)` →
  `oracle(k, n)`. The driver asserts the crash actually `fired` each k (loud on any non-determinism) and
  already performs the 5 faithful fresh-restart resets + the recovery triple (`releaseAllReaders/Writers/
  WalWriters` → `crash(dbRoot)` → clear transient suspend → evict pooled `TxnScoreboard` →
  `new RecoveryCoordinator(engine).recover()` → `notifyWalTxnRepublisher` → `drainWalQueue()`).
  `AdaptiveCrashWorkload` = `{ TableToken[] setup(int iteration); void commit(); int oracle(int k,int n);
  default void teardown(); }`.
- **Crash facade:** `CrashFaultFilesFacade` — `armCrashAt(int)`, `durabilityOpCount()`, `crash(dbRoot)`
  (rolls back **every** regular file under dbRoot — no per-table filter), `markDurableBaseline`, `reset`.
  Durability ops that count = `fdatasync`/`syncfs`/`fsync`/`fsyncAndClose`/`msync` (both async modes);
  `write`/`copy`/`mmap`/`open` do **not**.
- **Fuzzer (reuse for generation + apply only):** `FuzzRunner` (plain public composition object, package
  `io.questdb.test.cairo.fuzz`) — `setFuzzProbabilities(...)`, `setFuzzCounts(...)`,
  `generateTransactions(tableName, rnd)` → `ObjList<FuzzTransaction>`, `applyToWal(txns, tableName,
  walWriterCount, applyRnd)` (write+commit only, **no** drain, **no** suspend-assert),
  `applyNonWal(txns, tableName, rnd)` (non-WAL twin via `TableWriter`), `generateRandom(Log)` /
  `generateRandom(Log, s0, s1)` (logs + reprints the seed pair for repro). `FuzzTransaction` applies
  against `TableWriterAPI`, common to `WalWriter` and `TableWriter` — the same op-list drives both tables.
  Op library (`FuzzTransactionGenerator.generateSet`): insert / O3 insert / replace-range,
  add/drop/rename column, column-type-change, truncate, drop-partition, partition→parquet/native,
  add-covering-index, drop+recreate table, set-TTL, set-parquet-encoding, set-table-format.

### Two composition rules the wiring analysis mandates

1. **Never call `fuzzer.applyWal(...)`** — it ends with `Assert.assertFalse("Table is suspended", ...)`,
   so a crash swallowed into a suspend throws an `AssertionError` (wrong type, aborts the sweep). Drive
   commits via `fuzzer.applyToWal(...)` **+ the inherited `drainWalQueue()`**, checking
   `anyTableSuspended(tokens)` between/after — the exact shape of
   `AdaptiveCrashSweepSelfCheckTest.IdentityWorkload.commit()`.
2. **Install the facade only via the inherited `runWithCrashFacade(...)`** (which swaps
   `AbstractCairoTest.ff`). Do **not** call `assertMemoryLeak(fuzzer.getFileFacade(), ...)` — that would
   clobber `ff` with a stale `FailureFileFacade` wrapping a pre-crash delegate. `fuzzer.withDb(...)` in
   setup captures its own `ff` only for `applyNonWal`'s IO-failure knobs, which D2 does not use.

## Architecture

`RandomizedAdaptiveCrashFuzzTest extends AbstractAdaptiveCrashSweepTest`, holding
`private final FuzzRunner fuzzer = new FuzzRunner();` as a field (single inheritance forces composition
over extending `AbstractFuzzTest`). It replicates `AbstractFuzzTest`'s glue: `@Before` →
`fuzzer.withDb(engine, sqlExecutionContext); fuzzer.clearSeeds();`; `@After` → `fuzzer.after();`.

For each seed `s` in the budget, a `FuzzCrashWorkload implements AdaptiveCrashWorkload` runs one
`forEachAdaptiveCrashPoint(workload)` sweep:

- **Determinism (required):** the driver's count-pass must reproduce the sweep-pass, so the workload is
  deterministic — a **fixed seed**, **`walWriterCount = 1`**, and a **fixed `applyRnd`** derived from the
  seed. Multi-writer/parallel apply and wall-clock are excluded (matches D1's "single-threaded
  deterministic drain" scope). Any residual non-determinism trips the driver's `fired` assertion loudly —
  a fail, never a silent pass.
- **`setup(iteration)`:** (re)create the WAL table with the D2 schema; regenerate the **same** fuzz
  transaction list from the fixed seed (independent of `iteration`). On first call only, build the twin +
  fingerprints (below) and cache them on the workload.
- **`commit()`:** `fuzzer.applyToWal(txns, walTable, 1, applyRnd)`; `drainWalQueue()`; stop promptly if
  `anyTableSuspended`.

## The oracle — fingerprint membership + RPO bars

`assertSqlCursors(twin, walTable)` is **unusable**: it is a strict lock-step compare that hard-fails on
the first missing row (illegal under W>0 tail loss) and is outright wrong once the shape contains a
truncate/drop (recovered can then have *more* rows than the final twin). `recovered ⊆ twin-final` is
therefore not a sound bar. Instead:

**Build a fingerprint history.** Apply the fuzz txns to a **reference twin** one transaction at a time;
after each, record a state fingerprint into `fp[0..M]` (`M` = committed-txn count). The twin is a **WAL
table applied without any crash** (drain after each txn) — *not* a BYPASS-WAL/non-WAL table, because a
non-WAL writer cannot perform replace-range dedup commits (`FuzzRunner.applyNonWal` excludes those rows),
so only a WAL twin faithfully mirrors the full op library. A fingerprint is the twin's full committed
state captured schema-aware: for D2's small tables, the **ordered dump `printSql("select * from <twin>
order by ts", sink)`** rendered to a String — chosen over a hash so a mismatch shows the actual diverging
rows; a `long` xxh3 over that dump is a scale optimization deferred past v1. The twin is built **once per
seed, before any crash is armed**, its `fp[0..M]` cached in memory, and its table then **dropped** —
because `crash(dbRoot)` rolls back every file under dbRoot indiscriminately.

`P` throughout := the twin **fingerprint index** the recovered state matches. When two txns coincide (e.g.
insert-then-truncate-back) `P` := the **largest** matching index (conservative).

**Per crash point `k`, assert:**

1. **No silent corruption (membership) — all W.** The recovered table's fingerprint **equals some
   `fp[P]`**, i.e. it is *exactly* a real committed snapshot. This single check subsumes no-torn-txn,
   no-wrong-value, and no-phantom, and holds for **every** op type (truncate/drop/replace included). A
   recovered state that matches no `fp[P]` is corruption → fail. This is the **primary safety bar**.
2. **Clean reopen — all W.** The table is not left permanently `suspended`; a follow-up write + read
   succeeds.
3. **RPO at W=0 (exact).** Under W=0 every returned commit is synchronously durable (adaptive == SYNC), so
   two exact bars hold: **full at the end** — at `k = N` the recovered `P == M` (the durable WAL holds
   every committed txn; recovery + replay reconstructs the full state); and a **monotone staircase** — `P`
   is non-decreasing as `k` increases (the D1 "monotonic floor"). Together these *are* the "no acked txn
   lost" guarantee at W=0.
4. **W>0 (corruption-free under batching).** Under W>0 the durable frontier legitimately lags by ≤ W, so
   full-at-N and the strict staircase (bar 3) are relaxed — but bars 1–2 still hold at **every** crash
   point: each recovered state is a **member** of `fp[]` (a valid committed snapshot — the deferred-flush
   batching path, `flushPendingDurable`, introduces no corruption) and the table reopens clean. This is
   W>0's v1 deliverable: proving adaptive is corruption-free under group-commit batching across randomized
   shapes. The precise RPO **quantity** ("loses ≤ W" and "loses < NOSYNC") is **deferred**: the crash index
   is a durability-op count, and adaptive vs NOSYNC issue *different* fsync sequences, so the same `k` is
   not the same logical moment across modes — a sound NOSYNC/W comparison needs a transaction-boundary
   crash harness or acked-frontier-at-crash instrumentation, out of v1 scope.
5. **Necessity (negative control, once per seed).** With
   `CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED = false`, at least one crash point must leave the table
   at a `P` strictly short of what recovery achieves (recovery is load-bearing), mirroring D1.

**W is a swept config knob, not a second oracle.** Bars 1–2 + 5 are W-independent; W=0 additionally gets
the exact RPO bars (3); W>0 exercises the batching path for corruption (4). The primary safety property —
*no silent corruption across randomized shapes* — is bar 1, enforced at every crash point for every W.

## Configuration

- **Budget (approved: CI-fast default + nightly override).** Default: a handful of **fixed** seeds (≈5–8)
  plus **one logged random seed** per run (QuestDB's fuzz idiom — fixed seeds give reproducible CI, the
  random seed broadens coverage over time and reprints via `fuzzer.after()` for repro), with small
  `setFuzzCounts` (modest `fuzzRowCount`/`transactionCount`/`partitionCount`) so each seed's `N ≈ 30–60`
  and its full sweep runs in ~30–60 s (total suite cost ~5–10 min). A system property
  (e.g. `-Dfuzz.adaptive.crash.seeds=<n>`) unlocks hundreds of seeds and larger counts for nightly; the
  test reads it with the small default. Truncation past the sweep `cap` (200) is logged, never silent.
- **Op profile (approved: full destructive library).** Enable insert / O3 / add-drop-rename column /
  column-type-change / **truncate / drop-partition / replace-range dedup / set-TTL /
  partition→parquet-native** / add-covering-index, on a **rich multi-type schema** (designated-timestamp
  + symbol + varchar + numeric/long/double columns) so the run covers the parent spec's named
  "parquet/TTL/dedup/wide/varchar" gaps. **Excluded in v1:** `probabilityOfDropTable` (drop+recreate
  churns the table token/identity, a distinct recovery concern) — set to 0 and note it. Rollback may stay
  enabled (the membership oracle is robust to it: a rolled-back txn simply leaves `fp` unchanged).
- **W sweep.** Per seed, run W ∈ {0, one small window, one larger window} via
  `CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US`; `CAIRO_COMMIT_MODE = adaptive`,
  `CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS` per the D1 lazy-gap convention.

## Testing / acceptance

Every seed × W sweeps green — the fingerprint-membership + RPO bars hold at every crash point
`k ≤ min(N, cap)` — **or** a seed surfaces a real adaptive durability bug (a recovered state matching no
`fp[P]`, a durable-acked txn lost, or a corruption), which is a **GA-blocker** to file and fix (the point
of Prove-it). A failing seed is fully reproducible from its logged `s0,s1` pair. The suite carries its own
self-check (mirroring `AdaptiveCrashSweepSelfCheckTest`), run at **W=0**: a fuzz workload with a known
committed sequence must match `fp[M]` at k=N and produce a strictly-monotone staircase (the W=0 bar-3
exact-RPO tighteners); and the negative control (bar 5) must go RED with roll-forward disabled. Fluent `assertQuery`/`AbstractCairoTest` house style, JDK25. Heavier
seeds carry a slow marker if runtime warrants (decide in the plan).

## Scope / non-goals

- **In:** single-node, single WAL writer, deterministic seeded apply; the full destructive op library on a
  rich schema; W ∈ {0, small, large}; CI-fast + nightly-override budget.
- **Out (v1):** drop-**table** shape churn; multi-writer/parallel-apply non-determinism (D3 soak territory);
  the W>0 RPO **quantity** (both "loses ≤ W" and any NOSYNC comparison — the crash index is a durability-op
  count that does not align across commit modes, so it needs a txn-boundary crash harness or
  acked-frontier-at-crash instrumentation; W>0 in v1 asserts corruption-freedom only); exact-duplicate-
  timestamp rows (`equalTsRowsProb = 0` so the `order by ts` fingerprint stays canonical — replace-range
  dedup over distinct timestamps is still exercised); real hardware power-loss (D4); the
  `CommitModeBenchmark` adaptive arm (SP-C). Mat-view fuzz reuses D1's W4 finding — a fuzz mat-view shape
  is a candidate follow-up, not v1.

## Open decisions — resolved here

- **Oracle:** fingerprint-membership (not `⊆ twin-final`), against a **WAL** reference twin (non-WAL can't
  replace-range) — sound for destructive ops. Resolved.
- **W>0 RPO:** W=0 gets exact RPO (full-at-N + staircase); W>0 asserts corruption-freedom (membership +
  clean-reopen) only — the RPO quantity + NOSYNC comparison are deferred (crash index doesn't align across
  commit modes). W is a swept knob. Resolved.
- **Budget:** CI-fast fixed+random seeds default, `-D` nightly override. Resolved.
- **Op/schema scope:** full destructive library minus drop-table, `equalTsRowsProb = 0`, rich multi-type
  schema (the `FuzzRunner` fixed schema: symbol/varchar/binary/long128/ipv4/indexed-symbol). Resolved.
