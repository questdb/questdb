# SP-D / D4 — Adaptive Power-Loss Protocol (real-hardware cross-check)

**Status:** Design draft 2026-07-17. Sub-project of the adaptive-commit GA roadmap
(`2026-07-15-adaptive-commit-ga-roadmap-design.md`), SP-D Track-1 "Prove-it". Fills the
`D4 = outline only` gap in `2026-07-15-adaptive-sp-d-crash-validation-design.md`.

**Goal:** Cross-check on **real hardware** the crash-safety that D1/D2 proved in the JVM
device-cache model (`CrashFaultFilesFacade`). A real `dm-flakey drop_writes` power cut against the
**adaptive / WAL** path must uphold the same oracle the simulated sweeps do — most importantly the
durable-ack contract: *every acked txn survives; nothing acked is ever lost; no silent corruption*.

**Scope (user-chosen):** the protocol **document** (workload, fault schedule, recovery, oracle,
pass/fail bar, matrix, run procedure) **+ extend the harness code** so it is runnable. Actually
*running* it needs root on a real disk (external — this dev box has none; the dm-flakey script is
already marked UNTESTED for that reason). Not in scope: executing the run; a soak harness (that is
D3, cross-referenced below).

---

## Background — what already exists (do not rebuild)

- **`benchmarks/.../org/questdb/CrashIngestWriter.java`** — a `main()` CLI that creates ONE table
  `t (id long, v long, s symbol index, ts timestamp) timestamp(ts) partition by DAY **bypass wal**`,
  drives a **direct `TableWriter`**, commits every `K=1000` rows (deterministic values so the
  verifier can recompute them: `id=i`, `v=i*2654435761`, `s=SYMBOLS[i%4]`, `ts=BASE+i·1s`), and after
  each commit writes an fsync'd `_progress` watermark (atomic tmp→rename→dir-fsync). Runs until killed.
- **`benchmarks/.../org/questdb/CrashVerifier.java`** — reopens the root, `select ... order by ts`,
  bit-checks every row against the formulas, and prints `CONSISTENT count=.. watermark=..` /
  `LOUD_FAILURE:` / `SILENT_CORRUPTION`. Extra bars: `count % K == 0` (no torn commit) and
  `count >= watermark` (acked survived).
- **`power-cut-dmflakey.sh`** (+ `power-cut-manual.md`) — the real power cut: loop device over a
  4 GB image on a **real disk** (`/data`), `dm-flakey` pass-through, mkfs+mount, run the writer to a
  min-committed threshold, capture `COMMITTED` from `_progress`, `kill -9`, then **the cut** =
  `dmsetup` swap to a `drop_writes` table + `umount` (writeback silently discarded). A preflight
  `prove_cut_drops_unsynced()` proves the cut drops un-fsync'd data before any DB result is trusted.
- **`crash-consistency-pkill.sh`** — `kill -9` only (page cache survives ⇒ *not* power loss).

**Why they are SYNC/NOSYNC-only today (the exact gaps D4 closes):**
1. `CrashIngestWriter.parseCommitMode` accepts only `SYNC`/`NOSYNC` (else throws); `CrashVerifier`
   hardcodes `getCommitMode()==SYNC`.
2. The table is `bypass wal` / non-WAL — it never touches the WAL sequencer, the adaptive
   **group-commit window**, the **durable epoch**, or `localDurableSeqTxn`.
3. `CrashVerifier` never calls `RecoveryCoordinator.recover()` / `drainWalQueue()` — the
   production-faithful adaptive recovery path.

---

## The protocol

### Workload (extended `CrashIngestWriter`)
- Same deterministic `t (id, v, s symbol index, ts)` schema, but **WAL** (drop `bypass wal`) so
  commits flow sequencer → apply job → durable epoch. Keep `id=i / v=Knuth(i) / s=SYMBOLS[i%4] /
  ts=BASE+i·1s` so the verifier recomputes them; indexed symbol + DAY rollover exercise the same
  index/partition paths.
- Commit every `K=1000` rows. After each commit capture the **acked watermark** =
  `SeqTxnTracker.getLocalDurableSeqTxn()` (equivalently `SELECT localDurableSeqTxn FROM wal_tables()`
  filtered to `t`) into `_progress` — the adaptive-faithful replacement for the old row-count
  watermark. Record BOTH the committed seqTxn and the local-durable seqTxn (the gap between them is
  the at-risk window under W>0).
- New params: `-DcommitMode=adaptive` (accepted alongside SYNC/NOSYNC), `-Dgroup.window.us=<W>`
  (`cairo.adaptive.commit.group.window.us`; 0 or 50000), `-Depoch.interval.ms=<ms>`
  (`cairo.adaptive.epoch.interval.ms`). Drive the apply job so the durable frontier actually advances
  (run `ApplyWal2TableJob` + `CheckWalTransactionsJob`, or ingest via the normal server path).

### Fault schedule
- Unchanged mechanism: `dm-flakey drop_writes` + `umount` after `MIN_COMMITTED` rows (the existing
  `run_one`), with the `prove_cut_drops_unsynced` preflight. One cut per run for the matrix; the
  soak variant (D3) loops periodic cuts.

### Recovery (the new reopen path)
- On reopen the verifier must run the **production adaptive recovery triple**:
  `RecoveryCoordinator.recover()` → `notifyWalTxnRepublisher(t)` → `drainWalQueue()` before reading,
  so the durable epoch is rolled forward exactly as a real reboot does. (Today's verifier skips this.)

### Oracle / pass-fail bar (mirrors D1/D2 + `AdaptiveGroupCommitCrashTest`)
Let `Wm` = the `localDurableSeqTxn` captured pre-cut; `C` = committed seqTxn pre-cut.

1. **No silent corruption — HARD BAR, every mode/W.** Every recovered row bit-matches its formula;
   no wrong value, no gap below the recovered high-water. A `SILENT_CORRUPTION` verdict is a
   **GA-blocker**. (== D1/D2 fingerprint-membership.)
2. **Clean reopen (no-suspend).** The table is not left `suspended`; recovery either rolls forward
   or reads a clean committed prefix. A `LOUD_FAILURE` (CairoException/loud throw) that leaves a
   *clean readable prefix* is acceptable-but-noted; a suspend that never clears is a fail.
3. **Adaptive W=0 == SYNC (zero loss).** Recovered `count`/frontier `>= C` (the full committed
   history survives; `adaptive==SYNC`). Verdict `DURABLE`.
4. **Adaptive W>0 (the RPO contract).** (a) Every **acked** txn survives: recovered frontier `>= Wm`
   — *never* below the durable-ack. (b) Un-flushed loss is bounded: any lost txn is in `(Wm, C]`
   (RPO ≤ W). (c) The ack never claimed a lost txn (`Wm` itself always survives). Verdict `RPO_OK`
   if (a)+(b)+(c) hold with no corruption; `DURABILITY_FAILURE` if any acked txn is missing or any
   corruption.
5. **Necessity negative control.** One matrix cell runs with
   `cairo.adaptive.recovery.roll.forward.enabled=false`; at least one cut must then lose/short data,
   proving the roll-forward earns its keep.

### Matrix
`{ adaptive W=0, adaptive W=50ms }` (the subjects) `+ { SYNC, NOSYNC }` (controls) `×
{ ext4, xfs } × { batched sync on, off }`. SYNC must be DURABLE (regression guard on the existing
path); NOSYNC loss is expected/tolerated (baseline). Adaptive W=0 must match SYNC; adaptive W=50ms
must satisfy the RPO contract.

### Run procedure (hand-to-hardware)
Root + a real disk. Per cell: `prove_cut_drops_unsynced` preflight → ingest to `MIN_COMMITTED`,
capture `(C, Wm)` → cut → remount → run the recovery-triple verifier → interpret verdict vs the bar.
Repeat over N seeds/cut-points. Document expected verdict per cell.

---

## Harness-extension requirements (the code deliverable)
1. **`CrashIngestWriter`**: `parseCommitMode` also accepts `"adaptive"` → `CommitMode.ADAPTIVE`; add
   `group.window.us` + `epoch.interval.ms` system props wired into the `CairoConfiguration`
   overrides; switch the table to WAL; ingest through the WAL path + run the apply/check jobs so the
   durable frontier advances; capture `localDurableSeqTxn` into `_progress` alongside the committed
   seqTxn.
2. **`CrashVerifier`**: accept the commit mode (don't hardcode SYNC); on reopen run
   `RecoveryCoordinator.recover()` + `notifyWalTxnRepublisher` + `drainWalQueue()`; assert the bar
   above against the captured `(C, Wm)` — emit `DURABLE` / `RPO_OK` / `DURABILITY_FAILURE` /
   `SILENT_CORRUPTION` / `LOUD_FAILURE` verdicts.
3. **`power-cut-dmflakey.sh`**: parameterize `COMMIT_MODE ∈ {adaptive,SYNC,NOSYNC}` and `W`; default
   run-set adds `adaptive W=0` and `adaptive W=50ms`; interpret verdicts per the bar (adaptive W=0 ⇒
   expect DURABLE; W=50ms ⇒ expect RPO_OK). Keep the `prove_cut_drops_unsynced` gate.
4. **Testability in a no-root dev env:** the Java tools must **compile** (benchmarks module) and pass
   a **dry-run smoke** (ingest a few K rows, no cut, verifier reads back CONSISTENT + the recovery
   triple runs clean). The shell script must pass `bash -n`. The real cut is external; clearly label
   what is dev-verifiable vs hardware-only.

---

## Open decisions (proposals — confirm on review)
- **W>0 pass/fail bar** → *proposed:* the **durable-ack frontier** (`localDurableSeqTxn`) is primary
  (acked ⇒ survives, W-independent, the true contract); wall-clock RPO ≤ W is a secondary
  observation, not the gate (crash-index-to-time doesn't align cleanly, same reason D2 deferred the
  exact W>0 quantity bar).
- **Seeds / cut-points per cell** → *proposed:* small fixed set (e.g. 3 cut-points × 2 seeds) for a
  routine hardware run; more under a nightly/soak budget.
- **Soak (D3)** → out of this spec; *proposed default when D3 lands:* 8 h continuous ingest + a cut
  every ~10 min, nightly-tagged. Listed here only to note the boundary.

## Acceptance
- The protocol document exists and is hand-to-hardware complete (workload, fault, recovery, oracle,
  bar, matrix, procedure).
- `CrashIngestWriter`/`CrashVerifier`/`power-cut-dmflakey.sh` extended to the adaptive/WAL path;
  Java compiles + dry-run smoke green; `bash -n` clean; hardware-only steps labelled.
- No claim of a *passing hardware run* (that is external); the deliverable is *ready to run*.
