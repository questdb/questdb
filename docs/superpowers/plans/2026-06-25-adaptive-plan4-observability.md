# Adaptive Plan 4 — Observability (read-gating subsumed)

> Execute via superpowers:subagent-driven-development. Final plan.

## Read-gating: subsumed by construction (design note)
Spec §7 proposed clamping reader visibility to the durable frontier to prevent phantom/non-monotonic reads. The implemented design makes this **unnecessary in OSS v1**: WAL apply reads the **durable** WAL (the sequencer txnlog record is fdatasync'd, in segment→events→seq order, before apply materializes it — Plan 2A), so a row is visible (in `_txn`) only after it is WAL-durable ⇒ **visibility never exceeds the WAL-durable frontier ⇒ no phantom**. The only residual is transient staleness on a crash (the lazily-msync'd `_txn` may roll back to the epoch), but recovery (Plan 3C) re-applies to the durable frontier **before the table serves reads**, so no live reader observes a rolled-back row. The `read_durability` tier gate (`local`/`replicated`/`latest`) + the `DurableAckRegistry` local-fsync tier remain **Enterprise v2** (spec §17 S1/S2).

## Goal (OSS v1): expose the adaptive durability state in `wal_tables()`.
Add columns so operators can see the durable epoch, WAL retention floor, and recovery activity:
- `commitMode` — the effective commit mode for the table (string: `nosync`/`async`/`sync`/`adaptive`).
- `durableEpochSeqTxn` — `SeqTxnTracker.getDurableEpochSeqTxn()` (the last durable epoch; `0` if none yet).
- `walRetentionTxn` — the WAL-purge floor for the table (what `WalPurgeJob.getSafeToPurgeUpToTxn` computes; under adaptive = min over consumers incl. the epoch).
- `recoveryIncarnation` — a counter bumped each time `RecoveryCoordinator` rolls the table back to an epoch + re-applies (for detecting recovery activity / the §7 client-detection hook).

## Tasks
### Task A — `recoveryIncarnation` counter
- Add `volatile long recoveryIncarnation` to `SeqTxnTracker` (in-memory; or persist a small counter if cheap). `RecoveryCoordinator.recoverTable` increments it when it actually restores an epoch cut (not on no-op/skip).
- Test: a recovery-with-restore bumps the counter; a no-op boot (no marker) does not.

### Task B — `wal_tables()` columns
- Find the `wal_tables()` function factory (catalogue; near `WalTransactionsFunctionFactory` / the `wal_tables` cursor). Add the four columns above, sourcing `durableEpochSeqTxn`/`recoveryIncarnation` from `engine.getTableSequencerAPI().getTxnTracker(token)`, `commitMode` from config (global for v1), and `walRetentionTxn` from the purge-floor computation (or `durableEpochSeqTxn` under adaptive as the dominant term).
- Test: `assertSql` on `select name, commitMode, durableEpochSeqTxn, walRetentionTxn, recoveryIncarnation from wal_tables()` for an adaptive table after some commits + an epoch → expected values; for a nosync table → `commitMode='nosync'`, `durableEpochSeqTxn`=MAX/`n/a`.
- Keep additive: don't break existing `wal_tables()` consumers (append columns at the end).

## Deferred (documented, not v1)
- **Per-table `commit_mode` override** (`CREATE TABLE ... WITH commit_mode='adaptive'` / `ALTER TABLE ... SET PARAM`): needs `_meta` param storage + DDL + per-table mode reads at the sync sites. The global `cairo.commit.mode=adaptive` covers v1. Follow-on.
- **`read_durability` tiers + `DurableAckRegistry` local-fsync tier**: Enterprise v2 (spec §17).
- **Group-commit window `W>0`** (`WalCommitDurability` batching): Plan 2b (perf).

## Regression: WAL + catalogue suites green; existing `wal_tables()` tests unbroken.
