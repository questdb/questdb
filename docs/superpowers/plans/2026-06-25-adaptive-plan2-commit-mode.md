# Adaptive Plan 2 — `CommitMode.ADAPTIVE` + durable WAL commit / lazy apply

> Execute via superpowers:subagent-driven-development.

**Goal:** Introduce the `adaptive` commit mode (spec §4): the **WAL commit** is made durable (fdatasync of segment data → events → sequencer, in that order, before the commit returns/acks), while the **table apply** stays lazy (msync only, like NOSYNC) because the materialized table is a rebuildable cache of the durable WAL. v1 RPO = per-commit fsync (window `W=0`, zero-loss). Group-commit batching (`W>0`) is a deferred follow-on (Plan 2b).

**Why it works:** the engine already gates all syncs on `commitMode != CommitMode.NOSYNC`, and the WAL write-side ORDER (segment → events → sequencer) is already correct (CORRUPTION_AUDIT.md §3). Adaptive just makes the WAL-commit-path syncs *durable* (add fdatasync) and the *apply*-path syncs *lazy* (skip under adaptive). The integrity to detect a torn-but-replayable WAL frontier is already in place (Plans 1/1b/1c).

## Key sites (verified)
- `CommitMode.java`: `ASYNC=0, SYNC=1, NOSYNC=2` → add `ADAPTIVE=3`. Parse `"adaptive"` in `PropServerConfiguration.getCommitMode` (~line 2568).
- **WAL commit path = durable under adaptive** (`WalWriter.java`): segment column flush `ff.msync(...)` (~1881-1899), `events.sync()` (~1429/1676/1954), segment file `ff.fsyncAndClose` on roll (~872-883), and the sequencer `TableTransactionLogV2.sync0()` (msync of part→header). `WalEventWriter.sync()` and `sync0()` currently msync when `!= NOSYNC`.
- **Apply path = lazy under adaptive** (`TableWriter.java`): `syncColumns()` (~13569) + the apply-side `commitMode != NOSYNC` sync sites (~1059/1070, 8790, 10962, 12378/12391, 12468). For WAL tables `TableWriter` is the apply target driven by `ApplyWal2TableJob`.

## Tasks

### Task A — `CommitMode.ADAPTIVE` + config + durable WAL commit
- Add `ADAPTIVE=3`; parse `"adaptive"`.
- On the **WAL commit path**, under `ADAPTIVE`, make the segment-data + events + sequencer flush **durable**: after the existing msync, `ff.fdatasync(fd)` each (reuse `Files.fdatasync`, already exposed on this branch), preserving the data→events→seq order so the durable pointer never precedes its data. Concretely: durable-sync the segment column files on commit, then `events` (`WalEventWriter`: add an fdatasync under adaptive), then the sequencer (`TableTransactionLogV2.sync0`: fdatasync part then header under adaptive). SYNC/ASYNC/NOSYNC behavior unchanged.
- **Test (fsync accounting):** use a counting `FilesFacade` (see `AppendOnlySyncNarrowingTest` on this branch for the pattern) to assert that an `adaptive` WAL commit issues `fdatasync` on the segment data, events, and sequencer files, in data-before-pointer order; and that NOSYNC issues none. Plus an end-to-end round-trip on an `adaptive` table (`CREATE TABLE ... WITH commit_mode... ` or the global config) producing correct query results.

### Task B — lazy apply under adaptive + tests
- On the **apply path** (`TableWriter.syncColumns` and the apply-side sync sites), treat `ADAPTIVE` as `NOSYNC` (skip the apply fsync/msync). Recommended: a small helper `isApplySyncEnabled(commitMode)` = `commitMode == SYNC || commitMode == ASYNC` (both NOSYNC and ADAPTIVE skip apply sync), and use it at the apply-side sites — do NOT change the WAL-write-path checks. Audit each `commitMode != NOSYNC` site in `TableWriter` and classify it as apply (skip under adaptive) vs WAL-segment-write (durable) before changing it.
- **Test (fsync accounting):** under `adaptive`, `ApplyWal2TableJob` materializing a txn does NOT fsync/msync the table partition columns (apply is lazy), whereas SYNC does. Plus: an `adaptive` table's data is correct after apply (the lazy apply still produces the right materialized state).

### Task C — per-table override + observability stub (small)
- `CREATE TABLE ... WITH commit_mode='adaptive'` / `ALTER TABLE ... SET PARAM commit_mode='adaptive'` if the per-table commit-mode plumbing exists (else note global-only for v1 and defer per-table to Plan 4). Add `commit_mode` to `wal_tables()` if cheap (else defer to Plan 4).

## Oracle / invariants
- INV-1 (spec): under adaptive, the WAL (segment→events→seq) is fdatasync-durable before the commit returns — every acked txn is replayable. Verified by fsync-accounting (order + presence) and, if feasible, the `nw_sync_batch` power-cut harness (an adaptive-committed txn survives a power cut; the materialized table, even if behind, is rebuilt from the durable WAL).
- Apply is lazy: no table-column fsync on apply under adaptive (fsync-accounting).
- adaptive data round-trips correctly (functional).

## Scope notes
- v1 is `W=0` per-commit fsync. The group-commit coordinator (`WalCommitDurability` batching across concurrent writers, `W>0`) is **Plan 2b** (perf; not correctness).
- Recovery to the durable WAL frontier + epochs is **Plan 3**; read-visibility gating + `DurableAckRegistry` local tier is **Plan 4**. Plan 2 only makes the WAL durable and the apply lazy.
- Do not change SYNC's behavior (the audit's Phase-B fsync-for-SYNC is a separate effort).
