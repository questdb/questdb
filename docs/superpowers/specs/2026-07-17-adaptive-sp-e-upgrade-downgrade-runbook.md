# Adaptive commit mode — upgrade / downgrade / mixed-version runbook

**Status:** 2026-07-17. Operator-facing companion to
[SP-E design](2026-07-17-adaptive-sp-e-upgrade-compat-design.md). OSS core, branch `nw_adaptive_commit`.

Adaptive (`CommitMode.ADAPTIVE`) is **opt-in**; `nosync` remains the default. Turning it on/off is a
runtime commit-mode change — there is **no on-disk migration and no file rewrite**. All the new adaptive
artifacts are additive and gated (see "Inertness guarantees" below), so upgrade and downgrade are safe
and reversible. This runbook gives the operator steps and what to expect.

---

## A. Upgrade an existing db to adaptive

You can enable adaptive **globally** or **per table**. Prefer per-table first (smaller blast radius).

**Per table (recommended):**
```sql
ALTER TABLE trades SET PARAM commit_mode='adaptive';
```
- Takes effect on the next commit. `wal_tables()` shows `commitMode = 'adaptive'` for that table.
- From then on the table's WAL commits are made durable and it begins taking **durable epochs**; the
  epoch artifacts (`_snapshot`, `_txn.epoch`, `_cv.epoch`) appear in the table dir. WAL segments are now
  retained back to the last durable epoch (recovery floor).
- Tune the epoch cadence with `cairo.adaptive.epoch.interval.ms` and the group-commit RPO window
  (`cairo.adaptive.commit.group.window.us`) — see SP-C's tuning guidance.

**Globally:**
```
cairo.commit.mode=adaptive
```
- Affects every table whose `_meta` commit-mode override is UNSET (i.e. every table that never set an
  explicit per-table mode). Per-table overrides win over the global (`effectiveCommitMode`).

**No downtime / no rewrite.** Existing tables are untouched on disk until their next commit. There is no
`_meta` rewrite required to *read* an old table under adaptive — old formats are fully supported.

**Rollout sequencing (multi-node / with a load balancer):** upgrade binaries first (see C), confirm the
cluster is healthy on the new binary, *then* flip commit mode. Do not flip commit mode while any node is
still on an old binary that cannot itself run adaptive — see C for why this is safe but pointless.

---

## B. Downgrade back (turn adaptive off)

**Per table:**
```sql
ALTER TABLE trades SET PARAM commit_mode='nosync';   -- or 'sync' / 'async'
-- or revert to the global default:
ALTER TABLE trades SET PARAM commit_mode='unset';
```
**Globally:** set `cairo.commit.mode` back to `nosync` (affects UNSET tables).

**What happens:**
- The table immediately applies WAL under the new mode. No suspend, no corruption. `wal_tables()` reports
  the new mode. *(Proven: `AdaptiveUpgradeCompatTest.testDowngradeFromAdaptiveDrainsCleanlyAndArtifactsAreInert`.)*
- The leftover `_snapshot`/`_txn.epoch`/`_cv.epoch` are **not** deleted — they stay on disk but become
  **inert**. The live read/apply path never opens them.
- The WAL purge floor drops from the durable epoch to the applied seqTxn (the epoch floor applies only
  under adaptive), so WAL segments above the frozen epoch become purgeable again — normal `nosync`
  retention resumes.
- **On the next restart**, `RecoveryCoordinator.recover()` sees the leftover `_snapshot` marker but
  **skips roll-forward** because the table's effective mode is no longer adaptive. The live `_txn` (ahead
  of the frozen epoch) is left as-is; **no rollback, no data loss, no suspend**.
  *(Proven: `AdaptiveUpgradeCompatTest.testDowngradeThenRebootPreservesDataAndIgnoresStaleEpoch`, which
  additionally force-purges the WAL below live before rebooting to show a rollback would have been lossy —
  yet all rows survive.)*

**Durability expectation after downgrade:** the table now has the crash semantics of its new mode
(`nosync` = the pre-adaptive default: recent un-fsynced commits may be lost on power loss; `sync` =
per-commit column flush). This is exactly what you asked for by switching. The adaptive epoch is a
*recovery* mechanism that only applies while adaptive is on.

**Optional cleanup (not required):** the leftover `_snapshot`/`.epoch` files are small and inert. If you
want them gone, drop and recreate the table, or ignore them — a future engine build may garbage-collect
them, but leaving them is safe.

---

## C. Mixed-version rolling upgrade (what to expect)

During a rolling upgrade, an **old** binary and a **new** binary may both open the same db (sequentially,
per node). The contract:

- **New binary reading anything an old binary wrote:** always fine — the new binary supports every older
  on-disk format (meta v1/v2, no body checksums, no epoch artifacts).
- **Old binary reading a db a *new* binary wrote, adaptive OFF:** byte-identical to what the old binary
  itself writes for non-adaptive tables (the new artifacts only appear on *adaptive* tables). Fine.
- **Old binary reading a db a *new* binary wrote, adaptive ON:** the old binary **ignores** every adaptive
  artifact:
  - `_snapshot`, `_txn.epoch`, `_cv.epoch` are **separate files** the old binary never opens (it has no
    recovery step and opens `_meta`/`_txn`/`_cv`/columns by name).
  - the `_meta` `commit_mode` field (meta v3) sits in reserved header space the old binary never reads;
    it reads the table as its own latest known format and defers commit mode to its global setting.
  - the `_txn`/`_cv`/`_event` body/record checksums are magic/zero-gated trailers the old binary never
    looks at.

  The old binary therefore boots, reads all tables, and serves correct data. What it does **not** do:
  honor the per-table `commit_mode` override (it has no such concept — it uses its global
  `cairo.commit.mode`), and it does **not** run adaptive recovery. So a table you intended to be adaptive
  is, on the old node, just running the old node's global mode. This is why you should **finish the binary
  upgrade before flipping commit mode**: flipping earlier is safe but the old nodes won't act adaptively.

**Recommended order:** (1) roll the new binary across all nodes with commit mode unchanged; (2) verify
health; (3) enable adaptive (per-table or global). **Rollback:** (1) turn adaptive off (B); (2) roll back
binaries. Turning adaptive off before rolling back binaries guarantees the epoch artifacts are already
inert for the old binary (they would be inert anyway, but this keeps the mental model simple).

---

## D. Inertness guarantees + their gate basis (the evidence)

Every new adaptive artifact is inert to an unaware reader by an explicit gate. In-process tests drive each
gate; an old binary is, by construction, "a reader that does not know the artifact", so the gate is the
evidence the mixed-version matrix holds.

| Artifact | Gate | Evidence (test) |
|----------|------|-----------------|
| `_meta` `commit_mode` field (meta v2→v3) | version-gated: pre-v3 meta → `getCommitMode` returns UNSET → global mode; field lives in reserved header space | `AdaptiveUpgradeCompatTest.testMetaCommitModeFieldIsInertOnPreV3Meta` |
| `_snapshot` marker + `.epoch` copies | separate files off the read path; recovery double-gated (marker exists AND effective mode == ADAPTIVE) | `AdaptiveUpgradeCompatTest.testStraySnapshotAndEpochArtifactsAreInertOnNormalOpen` |
| `_event` per-record CRC trailer | magic-gated (`WALE_CHECKSUM_MAGIC`); no magic → read unverified; trailer inside the record length so navigation skips it | `WalEventChecksumTest.testLegacyRecordWithoutTrailerStillReads` (inert) + `testTornEventRecordSuspendsTable` (non-vacuity) |
| `_txn` body checksum | zero-sentinel (`stored == 0` → skip); reserved gap bytes | `TxnTest.testOpenOldFormatTxn_noBodyChecksum` (inert) + `testTornTxnBody*` (non-vacuity) |
| `_cv` body checksum trailer | magic-gated (`CV_CHECKSUM_MAGIC`); size fields still data-only | `ColumnVersionWriterTest.testCvChecksumAbsent{OldFormat,RealLegacyShape,PageRoundedNoTrailer}` |
| downgrade (stale epoch on reboot) | `recover()` skips roll-forward when effective mode != ADAPTIVE (`RecoveryCoordinator.java:118`) | `AdaptiveUpgradeCompatTest.testDowngradeThenRebootPreservesDataAndIgnoresStaleEpoch` |

---

## E. External verification protocol (real old binary — cannot run in the JUnit suite)

The in-process tests prove the *gates*. The final `{old-binary} × {adaptive on/off}` open matrix and the
rolling-upgrade *cluster* test need a real prior binary and must be run by an operator. Proposed set
(adaptive branched from master at `9.4.4-SNAPSHOT`, fork `cdb0ea073b`, meta `LATEST=2`; the commit-mode v3
field has **never shipped in a release**):

| Old binary | Meta minor version | Why it's in the set |
|------------|--------------------|---------------------|
| `9.4.4-SNAPSHOT` @ `cdb0ea073b` (pre-adaptive master fork) | **v2** (table_format) | The immediate predecessor; exercises exactly the v2→v3 `_meta` gap with the same code base minus adaptive. **The most important row.** |
| `9.4.0` (latest GA) | v1 | Real released binary an operator might roll back to. |
| `9.3.5` | v1 | Prior major, latest patch. |
| `9.2.3` | v1 | Two majors back. |

**Protocol:**
1. With the **new** binary, create a db with (a) one table `WITH commit_mode='adaptive'` that has ingested
   enough to take at least one durable epoch (confirm `_snapshot`/`_txn.epoch`/`_cv.epoch` exist), and
   (b) one `nosync` sibling table with data. Clean-stop the new binary.
2. For each old binary in the set: start it on that db-root. Assert it (i) boots without error, (ii) lists
   both tables, (iii) returns correct data from both (row counts + a checksum/aggregate), (iv) logs **no**
   error mentioning `_snapshot`, `.epoch`, `commit_mode`, or a meta/format-version rejection. Ingest a few
   rows into each table and confirm they apply. Clean-stop.
3. Re-open with the **new** binary; confirm both tables still read correctly and the adaptive table
   resumes adaptive behavior (new epochs advance).
4. **Rolling-upgrade cluster test** (if running replicated Enterprise, out of OSS scope but noted): with a
   2-node cluster, upgrade one node's binary at a time with commit mode unchanged, confirm replication
   stays healthy throughout, then flip commit mode and confirm again.

**Expected result:** all green. The in-process gate tests are the standing evidence that it will be; this
protocol is the confirmation on real binaries.
</content>
