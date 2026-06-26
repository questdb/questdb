# Adaptive Deferred 1 — per-table `commit_mode` override

> Execute via superpowers:subagent-driven-development. Spec §10.

**Goal:** Let a table choose its commit mode independently of the global `cairo.commit.mode`, so some tables can be `adaptive` while others are `nosync`. `CREATE TABLE ... WITH commit_mode='adaptive'` and `ALTER TABLE ... SET PARAM commit_mode='adaptive'`. Stored in `_meta`; an UNSET sentinel ⇒ fall back to the global mode (back-compat: existing tables are UNSET).

**Mapped facts:**
- `_meta` additive field: offsets in `TableUtils` (`META_OFFSET_O3_MAX_LAG=24`, `META_OFFSET_TABLE_FORMAT` at the tail, `META_OFFSET_META_FORMAT_MINOR_VERSION`). Add `META_OFFSET_COMMIT_MODE` (INT) at the tail + bump the minor format version; old `_meta` lacking it ⇒ read as `UNSET`.
- `CREATE ... WITH`: `SqlParser.java:~1761-1797` parses `maxUncommittedRows`/`o3MaxLag` → builder. Add `commit_mode` (string→`CommitMode` int).
- `ALTER ... SET PARAM`: `AlterOperation` `SET_PARAM_MAX_UNCOMMITTED_ROWS`/`SET_PARAM_COMMIT_LAG` (+ the `svc.setMetaMaxUncommittedRows` apply path + `SqlParser`). Add `SET_PARAM_COMMIT_MODE` + `setMetaCommitMode`.
- Metadata accessor: mirror `TableUtils.getO3MaxLag`/`TableWriterMetadata.getO3MaxLag` → `getCommitMode()`.

**The consistency requirement (critical):** the table's *effective* mode = `meta.getCommitMode() != UNSET ? meta.getCommitMode() : configuration.getCommitMode()`. EVERY adaptive decision point must use the SAME effective per-table mode, not the global config:
- WAL-commit durability (`WalWriter` fdatasync sites), the apply lazy gate (`appliesColumnSync` callers in `TableWriter`/`O3CopyJob`/`MemoryPMARImpl.setApplyLazy`), the epoch trigger (`ApplyWal2TableJob.maybeAdvanceDurableEpoch`), the WAL-purge floor (`WalPurgeJob.getSafeToPurgeUpToTxn`'s `commitMode==ADAPTIVE` gate), and `RecoveryCoordinator` (which tables to recover). Audit each `configuration.getCommitMode()` / `== CommitMode.ADAPTIVE` site and switch to the table's effective mode where the decision is per-table.

**Tasks (TDD):**
1. `_meta` field + minor-version bump + `getCommitMode()` accessor + UNSET back-compat (old table reads UNSET). Test: create table, default UNSET; round-trip a stored mode.
2. `CREATE TABLE ... WITH commit_mode='adaptive'` parse + persist. Test: created table's `wal_tables().commitMode` (Plan 4) shows per-table mode; SQL error on bad value.
3. `ALTER TABLE ... SET PARAM commit_mode='sync'` + `setMetaCommitMode`. Test: alter changes the effective mode.
4. Thread the effective per-table mode through ALL adaptive decision points (above). Test: with global `nosync`, a `WITH commit_mode='adaptive'` table gets durable WAL + lazy apply + epochs + recovery (reuse the adaptive durability/epoch/recovery test patterns but set the mode per-table, global=nosync) — and a sibling nosync table on the same instance does NOT. This is the headline correctness test (per-table isolation).

**Observability:** update `wal_tables().commitMode` (Plan 4) to report the per-table effective mode instead of the global.

**Back-compat / scope:** additive `_meta` (minor bump, old tables UNSET→global); `WITH`/`SET PARAM` reject unknown mode names. Don't change the global-mode behavior for tables that don't set it.
