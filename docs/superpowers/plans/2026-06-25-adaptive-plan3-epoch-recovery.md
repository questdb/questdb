# Adaptive Plan 3 — Epoch + Recovery

> Execute via superpowers:subagent-driven-development. Grounded by an architect scout pass (file:line below).

**Goal:** Close adaptive's recovery gap. Under lazy apply, `_txn`/`_cv` are msync-durable but partition columns are not, so a power cut can leave `_txn.appliedSeqTxn=N` while column data for txns ≤ N is non-durable/torn. Plan 3 adds a **durable epoch** (periodic fully-durable cut + `_snapshot` marker) and **recovery** that opens an adaptive table at the last durable epoch and re-applies the durable WAL forward to the frontier.

## Key architectural insight (from scout)
WAL apply is **idempotent through `_txn`**: the apply loop hard-asserts `seqTxn == writer.getAppliedSeqTxn()+1` (`ApplyWal2TableJob.java:445`) and `getAppliedSeqTxn()` derives from the persisted `_txn` (`TableWriter.java:2197`). So you cannot reset the apply cursor against a live writer — recovery must reset `_txn`/`_cv`/columns to the epoch, then the existing apply pipeline (`CheckWalTransactionsJob:99-109 → ApplyWal2TableJob`) rolls forward unchanged. **No new apply mode; no change to the contiguity assert.**

## Design decisions (this plan)
- **Epoch cut via Option 2 (durable copies):** the epoch job fsyncs the live materialized columns AND writes durable copies `_txn.epoch` + `_cv.epoch` (like checkpoint's `dumpTo`, `DatabaseCheckpointAgent.java:439/444`). Recovery copies them over `_txn`/`_cv` before first open. This is obviously-correct and avoids the untested writer-reseat (Option 1, deferred).
- **Recovery ALWAYS opens at the last epoch + re-applies** `(epoch.seqTxn, frontier]` (matches spec §5.4; avoids the "is the live state torn?" detection problem; bounded by epoch cadence). Absent `_snapshot` ⇒ epoch 0 ⇒ today's full replay (back-compat).
- **Epoch advance runs from inside the apply worker** right after an apply batch commits while it holds the writer (single-threaded per table, consistent A/B cut, no quiesce/lock). Hook near `ApplyWal2TableJob.applyOutstandingWalTransactions` (~`:573`).
- **Frontier = highest CRC-/length-intact seqTxn** — already enforced: a torn `_event`/txnlog record or short segment suspends apply (Plans 1/1b/1c). Recovery just lets apply stop at the first torn record.

## Tasks (independently testable; order A → D → B → C)

### Task A — `_snapshot` marker
- New `core/src/main/java/io/questdb/cairo/SnapshotMarker.java` (small-file R/W, mirroring the `_cv` A/B + full-body CRC pattern). `TableUtils`: add `SNAPSHOT_FILE_NAME="_snapshot"` + `SNAPSHOT_CHECKSUM_MAGIC`; reuse `calculateCvAreaChecksum` (`TableUtils.java:417`).
- Body `{epochSeqTxn:i64, epochTxn:i64, ts:i64, formatVersion:i32}` + `{MAGIC:i64, checksum:i64}` trailer; two slots + fenced version word (mirror `TxWriter.java:201`); torn-slot fallback (mirror `TxReader.java:730`); both-torn ⇒ absent ⇒ epoch 0. `write()` **fsyncs** the marker fd (INV-5).
- **Test:** unit — write, corrupt slot A → falls back to B; corrupt both → absent. `CrashFaultFilesFacade` + `armCrashAt` mid-flip → never reads a torn flip as a newer epoch.

### Task D — WAL-purge floor
- `SeqTxnTracker` (`cairo/wal/seq/SeqTxnTracker.java`): add `volatile long durableEpochSeqTxn` (init `0` for ADAPTIVE tables, `Long.MAX_VALUE` otherwise so the min is a no-op) + setter.
- `WalPurgeJob.getSafeToPurgeUpToTxn` (`:494`): add `safeToPurgeTxn = Math.min(safeToPurgeTxn, tracker.getDurableEpochSeqTxn())`. Result already flows to `getCursor`/`getCurrentSeqPart` (retains both segments + seq-parts).
- **Test:** extend `WalPurgeJobTest` — epoch below applied ⇒ `(epoch,applied]` retained; advance epoch ⇒ purgeable. (INV-2.)

### Task B — `SnapshotEpochJob` (durable flush + copies + marker + pin)
- New `TableWriter.fsyncMaterializedState()` — run the proven `syncColumnsBatchedSync` 3-pass + `syncfs` (`:13696-13733`) + symbol-writer sync + `columnVersionWriter` fsync + `txWriter` fsync, **unconditionally** (independent of commit mode). Then write durable `_txn.epoch`/`_cv.epoch` copies of the just-committed cut.
- New `SnapshotEpochJob` (or an in-apply-worker hook): trigger on interval/Δtxns/Δbytes (config `cairo.adaptive.epoch.interval.ms`/`.txns`/`.bytes`). `advance(token)`: `fsyncMaterializedState()` → `SnapshotMarker.write(seqTxn, txn, ts)` (fsync) → scoreboard `acquireTxn(EPOCH_ID, epochTxn)` then `releaseTxn(EPOCH_ID, priorEpochTxn)` (new pin before old release, INV-5) → `tracker.setDurableEpochSeqTxn(seqTxn)`.
- **`TxnScoreboardV2` `EPOCH_ID`** (CAREFUL, concurrency-critical): `toInternalId(id)=id+VIRTUAL_ID_COUNT` (`:314`); `CHECKPOINT_ID=-1`→0. Adding `EPOCH_ID` requires deliberately extending the id→slot mapping (bump `VIRTUAL_ID_COUNT`/map carefully) with its own `TxnScoreboardTest` coverage. Get the off-by-one right or the CHECKPOINT slot corrupts.
- Register alongside `WalPurgeJob` in `ServerMain.java:553-563` via the `WalJobFactory`.
- **Test:** `CrashFaultFilesFacade` — apply txns, `advance()`, apply more, `crash()`, reopen → epoch'd rows durable (`assertSyncDurable`); **negative control:** without `advance()`, lazily-applied rows are lost on `crash()` (proves columns were non-durable). `armCrashAt` mid-epoch-fsync → no silent corruption.

### Task C — Recovery roll-forward (boot)
- New `RecoveryCoordinator.recover(tableToken)` at engine startup (near `DatabaseCheckpointAgent.recover()` `:786`, after checkpoint restore): read `_snapshot` (Task A) → `epoch`; if present, copy `_txn.epoch`/`_cv.epoch` over `_txn`/`_cv` (**fsync the restored state before clearing/relying on the marker** — audit #5), so the table opens exactly at the epoch cut. Then normal boot (`CheckWalTransactionsJob → ApplyWal2TableJob`) re-applies `(epoch.seqTxn, frontier]` — idempotent re-derive.
- Recovery must itself be idempotent (a crash mid-recovery restarts cleanly from the same epoch — the durable copies are immutable until the next epoch).
- **Headline test (`CrashFaultFilesFacade`, ADAPTIVE):** ① apply through `epoch.seqTxn`, `advance()`; ② lazily apply `(epoch,N]` (msync `_txn`, columns NOT fsync'd); ③ record rows a reader sees; ④ `crash()` + reopen; ⑤ assert every WAL-committed row through the durable frontier present+correct (`assertSyncDurable`), table NOT suspended, no reader saw a now-absent row (INV-3 spot-check). **Negative control:** epoch/recovery disabled ⇒ data loss or suspend.

## Crash-test harness (the oracle)
`io.questdb.test.cairo.crash`: `AbstractCrashConsistencyTest` (`runWithCrashFacade`/`markDurableBaseline`/`crashAndReopen`, oracles `assertNoSilentCorruption`/`assertSyncDurable`) + `CrashFaultFilesFacade` — models the Linux contract: `msync(MS_SYNC)`=durable, mmap stores=non-durable-until-journaled, `fsync`/`fdatasync`/`syncfs`=durable. Reproduces the exact "`_txn` ahead of durable columns" gap and proves the fix.

## Risks / scope
- **C2 (writer-reseat, Option 1) is NOT used** — Option 2 durable copies replace it. If Option 2's per-epoch two-file cost is later a concern, revisit Option 1 behind a flag.
- **`EPOCH_ID` scoreboard mapping** — the one concurrency-critical micro-change; dedicated tests.
- v1 epoch consistency via the in-apply-worker hook. Group-commit `W>0` is Plan 2b (not here). Read-gating + `DurableAckRegistry` tier + obs is Plan 4.
