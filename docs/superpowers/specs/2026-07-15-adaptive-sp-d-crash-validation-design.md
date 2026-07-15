# SP-D — Durability / Crash Validation — Design

**Parent roadmap:** `2026-07-15-adaptive-commit-ga-roadmap-design.md` (SP-D, Track 1 "Prove-it").
**Status:** Design draft 2026-07-15.

**Goal:** Prove the "crash-safe, no corruption" half of the north star for adaptive commit mode
*beyond* the current set of hand-picked crash tests — by building the missing **exhaustive
crash-point sweep** driver and running the schema variants that the adaptive epoch/recovery cycle has
**never** been crash-tested against. This is the go/no-go durability evidence that gates GA.

## Grounding — what already exists (do NOT rebuild)

A reconnaissance of `core/src/test/java/io/questdb/test/cairo/crash/` establishes the reusable base:

- **`CrashFaultFilesFacade`** (737 lines, extends `TestFilesFacadeImpl`) — a device-cache /
  metadata-journaling durability model. Key API: `armCrashAt(int n)` (throw `CrashSimulationError`
  on the n-th *durability op* — only `fdatasync`/`syncfs`/`fsync`/`fsyncAndClose`/`msync` count;
  `write`/`copy` do not); `durabilityOpCount()` (read the running count, to compute offsets);
  `tornTail(path, offset, len)` (per-file deterministic byte-range zeroing); `crash(dbRoot)`
  (roll every tracked file back to its durable content + apply torn tails); `markDurableBaseline`;
  `reset()`. `modelSharedJournal` toggles ext4/xfs shared-journal vs `fast_commit` per-inode
  strictness. `CrashSimulationError extends Error` (so production `catch(CairoException/Throwable)`
  cannot swallow it).
- **The crash oracle** — `AbstractCrashConsistencyTest` provides two bars:
  `assertNoSilentCorruption(...)` (Bar 1 / containment: any row read back is correct, OR a loud
  Cairo error; *fewer* rows is acceptable, a silently WRONG value never is) and
  `assertSyncDurable(...)` (Bar 2: every durably-committed row present and correct). `crashAndReopen()`
  does `releaseAllReaders()` + `releaseAllWriters()` + `crash(dbRoot)`.
- **Recovery trigger (production-faithful)** — tests replay `CairoEngine.completeInit()`:
  `new RecoveryCoordinator(engine).recover()` → `engine.notifyWalTxnRepublisher(tt)` →
  `drainWalQueue()`, on the same live engine. `TableWriter.fsyncMaterializedState()` drives a durable
  epoch at a known seqTxn; `SnapshotMarker` writes the `_snapshot` anchor.
- **Adaptive config keys** (`PropertyKey`): `CAIRO_COMMIT_MODE` (`adaptive`),
  `CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS` (0 = every batch, <0 = disabled),
  `CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US` (the RPO window **W**; 0 = synchronous fsync-before-return),
  `CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED` (kill-switch, default true).
- **Existing adaptive crash oracles** (each with a paired negative control proving the mechanism is
  *necessary*): `AdaptiveEpochCrashTest`, `AdaptiveEpochFsWideFlushCrashTest`,
  `AdaptiveGroupCommitCrashTest`, `AdaptiveRecoveryRollForwardCrashTest` (headline),
  `AdaptiveRecoveryTornEpochCopyCrashTest`, `PerTableAdaptiveIsolationCrashTest`,
  `SnapshotMarkerCrashTest`. **All use a bare `(ts timestamp, v long)` in-order table.**
- **Seeded WAL fuzzer** (for later increments) — `WalWriterFuzzTest` / `AbstractFuzzTest` /
  `FuzzRunner` (1560 lines) / `FuzzTransactionGenerator` + a ~15-op library; seeds via
  `TestUtils.generateRandom(LOG)` (logged for repro); differential oracle
  `assertSqlCursors(nonWalTable, walTable)`. **Zero references to `CommitMode.ADAPTIVE`** — never
  wired to the crash facade.
- **Real hardware power-cut harness** (git-tracked, at repo root) — `power-cut-manual.md`,
  `power-cut-dmflakey.sh`, `crash-consistency-pkill.sh`, `syncfs-microtest.sh` + `CrashIngestWriter`
  / `CrashVerifier` (verdicts `CONSISTENT`/`LOUD_FAILURE`/`SILENT_CORRUPTION`). **`parseCommitMode`
  accepts only `SYNC`/`NOSYNC`; never invokes `RecoveryCoordinator`** — it is the pre-adaptive
  SYNC-path harness (it has already caught a real `SYNC+batched` ext4 silent-corruption bug).

## Gaps (the SP-D surface)

Confirmed-missing (grep-verified): the `forEachCrashPoint` exhaustive sweep (specced in
`2026-06-22-crash-consistency-design.md:79`, cap 200, **never implemented** — 3 `armCrashAt` sites
total); mat-view crash test; parquet crash test; O3 through the adaptive cycle; indexed-symbol /
wide / array / varchar through the adaptive cycle; TTL / drop-partition / dedup under adaptive crash;
multi-table simultaneous crash; randomized crash-fuzz; soak; a WAL/adaptive power-loss harness;
`CommitModeBenchmark` ADAPTIVE arm (→ SP-C, not here).

## Decomposition — SP-D increments

Each increment is independently testable software; they are ordered by certainty/leverage.

| ID | Increment | Risk | This spec |
|----|-----------|------|-----------|
| **D1** | **Exhaustive crash-point sweep + schema-gap closure** | Low (reuses `armCrashAt` + oracle) | **detailed below** |
| D2 | Randomized adaptive crash-fuzz (`FuzzTransactionGenerator` × crash × recover) | Med (marries two subsystems) | outline only |
| D3 | Soak (long-running ingest + periodic crash+recover, nightly-tagged) | Low | outline only |
| D4 | Adaptive power-loss protocol (extend `CrashIngestWriter`/`CrashVerifier` + dm-flakey to WAL/epochs) | High (partly external hardware) | outline only |

D1 builds the deterministic backbone; D2's fuzz reuses D1's sweep primitive and oracle; D3 reuses the
workloads; D4 is the real-hardware cross-check of the JVM model.

---

## D1 design — Exhaustive crash-point sweep + schema-gap closure

### D1.a The sweep driver — `forEachCrashPoint`

A reusable helper on `AbstractCrashConsistencyTest` (so all crash tests gain it):

```
forEachAdaptiveCrashPoint(WorkloadFactory workload, int cap):
  // 1. Count pass: run the workload to completion with NO fault; record N = durabilityOpCount().
  // 2. Sweep: for k in 1..min(N, cap):
  //      crashFf.reset(); set up a fresh db-root/table state;
  //      run the workload, but crashFf.armCrashAt(k) BEFORE the commit phase;
  //      expect a CrashSimulationError to propagate out of the apply/commit;
  //      crashFf.crash(dbRoot);
  //      recover: releaseAllReaders()/releaseAllWriters();
  //               new RecoveryCoordinator(engine).recover();
  //               notifyWalTxnRepublisher(tt); drainWalQueue();
  //      assert the crash-aware oracle (below) for THIS k;
  //   log the set of k actually exercised; if N > cap, LOG the truncation (no silent cap).
```

- `WorkloadFactory` = a closure that (i) creates the table(s) with a fixed schema, (ii) applies a
  fixed, deterministic sequence of commits, and (iii) exposes the expected committed sequence for the
  oracle. Determinism is required so the same k reproduces the same crash across the count and sweep
  passes.
- Cap default **200** (inherited from the original crash-consistency spec). The count pass makes the
  sweep exact when N ≤ cap.
- The driver gets its **own self-check** (mirroring `CrashModelSelfCheckTest`): a workload with a
  known number of durability ops must be swept at exactly that many distinct points, and a k beyond N
  must be a no-op — proving the driver injects where it claims.

### D1.b The crash-aware oracle (deterministic workloads, W = 0)

D1 fixes **W = 0** (`CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US = 0`): every commit fsyncs its WAL before
returning, so *adaptive committed state == SYNC committed state*. That makes the oracle exact and
removes RPO ambiguity (the W>0 RPO-window sweep is deferred to D2). After recovery at any crash point
k, assert **all** of:

1. **No silent corruption** — `assertNoSilentCorruption`: every row that reads back is bit-correct,
   or a loud `CairoException`/`CairoError` is thrown. A silently wrong value is always a failure.
2. **Durable-commit survival** — every commit that *returned success before* op k is present and
   correct after recovery (`assertSyncDurable` semantics against the workload's known committed
   sequence).
3. **Atomicity / prefix-consistency** — the recovered row-set is exactly the set of fully-committed
   txns; never a partial or torn txn (no half-applied commit, no row from an interrupted txn).
4. **Clean reopen** — the table is not left `suspended`; a follow-up write + read succeeds.
5. **Necessity (negative control, once per workload, not per k)** — with
   `CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED=false`, at least one crash point must leave the
   table WRONG/short, proving recovery is what fixes it (mirrors the existing headline tests).

### D1.c Curated schema workloads (close the named gaps)

Each is swept by D1.a and checked by D1.b. Ordered easy→hard so a late blocker doesn't sink earlier
coverage:

- **W0 — baseline (in-order, 2-col).** Validates the sweep driver itself against the known-good
  `(ts, v)` pattern the existing tests already trust. Anchors the driver's self-check.
- **W1 — O3 (out-of-order).** Adaptive table; commits with deliberately out-of-order timestamps to
  force the O3 merge path; sweep. Closes "O3 never run through the adaptive cycle."
- **W2 — indexed symbol.** Adaptive table with an indexed `symbol` column; sweep. The recovered
  table's symbol index must be consistent with its data (an index/data skew after recovery is a
  corruption).
- **W3 — multi-table simultaneous.** 2–3 *all-adaptive* tables, interleaved commits, ONE crash, one
  `recover()` pass; sweep. Exercises `RecoveryCoordinator.recoverTable()`'s per-table loop — every
  table must recover consistently, none left behind. (Risk: prior single-table limitation was a torn
  **NOSYNC** sequencer on teardown; all-adaptive tables should avoid it — verify early; if teardown
  is unstable, cap at 2 tables and document.)
- **W4 — mat-view (highest complexity, last).** Base adaptive table + a materialized view; commits
  trigger refresh; sweep. Base and view must recover to a mutually consistent cut (the view's
  refreshed state must not reference base rows that recovery rewound away). This is the explicit open
  risk from `adaptive-commit-mode-design.md:244`. If it proves too intricate for a clean sweep,
  split it into its own increment D1.5 rather than weakening the oracle.

### D1.d Reuse map

`assertNoSilentCorruption` / `assertSyncDurable` (oracle bars) and `crashAndReopen` (crash+release)
from `AbstractCrashConsistencyTest`; `armCrashAt` / `durabilityOpCount` / `crash` / `reset` from
`CrashFaultFilesFacade`; the recover-trigger triple from the existing adaptive tests. **New code is
only:** the `forEachAdaptiveCrashPoint` driver, its self-check, and the five workloads W0–W4.

## Open decisions — resolved here

- **Sweep cap K = 200** (original spec); truncation past N is logged, never silent.
- **W (group-commit window) = 0 for D1** — synchronous, adaptive==SYNC, exact oracle. W>0 RPO-window
  sweeps move to **D2** (the fuzz, where the RPO bound `loses ≤ W and < NOSYNC` is the natural check).
- **Fuzz seed count / iteration budget → D2. Soak duration → D3. Power-loss hardware protocol → D4.**
  These require their own environment/data and are not fixed here.

## Testing / acceptance (D1)

Every workload W0–W4 sweeps green — the oracle holds at *every* crash point k ≤ min(N, 200) — **or**
the sweep surfaces a real adaptive durability bug, which is a **GA-blocker** to file and fix (that is
the point of Prove-it). The driver self-check passes. Each workload ships its D1.b-#5 negative
control. Build/run per house style (`AbstractCairoTest`, JDK25); these are heavier tests, so they may
carry a `@Test`-level slow marker if runtime warrants (decide in the plan).

## Non-goals (D1)

Randomized fuzz (D2); soak (D3); real hardware (D4); parquet, TTL, dedup, wide/array/varchar schemas
(D2's fuzz op-library covers these more cheaply than hand-written sweeps); the `CommitModeBenchmark`
adaptive arm (SP-C).
