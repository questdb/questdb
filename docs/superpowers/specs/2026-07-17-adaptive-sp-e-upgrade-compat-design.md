# SP-E — Upgrade & mixed-version compatibility (design)

**Status:** design approved 2026-07-17. OSS core, branch `nw_adaptive_commit`. Sub-project of the
[Adaptive-Commit OSS-GA Roadmap](2026-07-15-adaptive-commit-ga-roadmap-design.md) (Track 2 — Harden-it).

**Goal.** Make adaptive commit mode safe to *roll out* and *roll back* across versions. Adaptive
(`CommitMode.ADAPTIVE = 3`) adds NEW on-disk artifacts to a table dir. The bar:

1. An **older binary** opening a db written by adaptive must not choke — every new artifact is either a
   separate file the old reader never opens, or an additive/magic-gated field the old reader ignores.
2. **Meta-format v2→3** (the per-table `commit_mode` field): an old reader reads UNSET → the global
   `cairo.commit.mode`.
3. **Rolling upgrade** (mixed old/new binaries against one db) behaves.
4. **Downgrade** (turning adaptive off) drains cleanly and leaves the epoch artifacts inert.

**Acceptance bar (from the roadmap).** The `{old,new} binary × {adaptive on,off}` db-open matrix is
clean; a documented upgrade + downgrade runbook exists.

## What is testable here vs. what is external

The `{old-binary}` half of the matrix and the rolling-upgrade *cluster* test need a **real old
binary** and are out of process — they cannot run in this repo's JUnit suite. They are specified as an
**external protocol** (see the runbook + the external-matrix note below).

What IS autonomously testable in-process — and is the core of this sub-project — is the **inertness of
each new artifact**: for every artifact, the *gate* that makes an unaware reader ignore it is a code
path we can drive directly. If the gate holds in-process, the external old-binary result follows from
the format contract (an old binary is, by construction, "a reader that does not know the artifact").
We prove each gate with a poke-the-bytes test in the established style (`TxnTest`,
`ColumnVersionWriterTest`, `SnapshotMarkerTest`) and, where an existing test already covers the
inert direction, we **cite it** rather than duplicate.

## Artifact inventory + how each is gated

Adaptive adds these on-disk artifacts. For each: the gate, and where it is proven inert.

| # | Artifact | New in adaptive | Gate that makes an unaware reader ignore it | Inertness proof |
|---|----------|-----------------|----------------------------------------------|-----------------|
| 1 | `_meta` per-table `commit_mode` field (`META_OFFSET_COMMIT_MODE`), meta minor version **v2→v3** (`META_FORMAT_MINOR_VERSION_COMMIT_MODE = 3`) | field + version bump | `TableUtils.getCommitMode(MemoryR)` is gated on `isMetaFormatAtLeast(mem, 3)`; a pre-v3 meta short-circuits to `CommitMode.UNSET` → `effectiveCommitMode(UNSET, global)` = global. The field lives in the reserved header region `[53,57)` before column data at 128, so an old binary (which reads only fields it knows, through table-format at v2) never reads it. | **NEW** `AdaptiveUpgradeCompatTest.testMetaCommitModeFieldIsInertOnPreV3Meta` |
| 2 | `_snapshot` epoch marker (`SnapshotMarker`, table-root A/B + CRC) | separate file | It is a **separate file**. The normal read path (`TableReader`/`TxReader`/`ColumnVersionReader`) opens `_meta`/`_txn`/`_cv`/columns **by name** and never lists or opens `_snapshot`. Only `RecoveryCoordinator.recover()` (adaptive boot) and `ApplyWal2TableJob` (adaptive apply) open it, and `recover()` is **double-gated**: (a) `_snapshot` must exist, and (b) `resolveEffectiveCommitMode(token) == ADAPTIVE` (`RecoveryCoordinator.java:118`). | **NEW** `AdaptiveUpgradeCompatTest.testStraySnapshotAndEpochArtifactsAreInertOnNormalOpen` |
| 3 | `.epoch` copies `_txn.epoch` / `_cv.epoch` (`TableUtils.EPOCH_COPY_SUFFIX`) | separate files | Same as #2 — separate files never opened by the normal read path; only `RecoveryCoordinator` reads them, behind the same double gate. | **NEW** (same test as #2) |
| 4 | `_event` per-record CRC trailer `[MAGIC \| xxh3]` (`WalUtils.WALE_CHECKSUM_TRAILER_SIZE`, appended by `WalEventWriter.finishRecord`) | trailer appended to each record | **Magic-gated**: `WalEventCursor.verifyRecordChecksum` returns without verifying when the 8 bytes at the trailer offset are not `WALE_CHECKSUM_MAGIC`. The trailer is included in the record's length prefix, so a length-based reader advances past it to the next record; the body is read by internal record structure, unchanged. | **EXISTING** `WalEventChecksumTest.testLegacyRecordWithoutTrailerStillReads` (inert) + `testTornEventRecordSuspendsTable` (non-vacuity) |
| 5 | `_txn` body checksum (`TX_OFFSET_BODY_CHECKSUM_64`, `TableUtils.calculateTxnBodyChecksum`) | 8 bytes in the reserved gap `[116,124)` | **Zero-sentinel**: `TxReader.unsafeVerifyBodyChecksum` treats a stored `0` as "absent" and skips the check. An old binary never reads `[116,124)` (reserved gap); a new binary reading an old `_txn` sees `0` → skip. No meta version bump. | **EXISTING** `TxnTest.testOpenOldFormatTxn_noBodyChecksum` (inert) + `testTornTxnBody*` (non-vacuity) |
| 6 | `_cv` body checksum trailer `[MAGIC \| xxh3]` (`TableUtils.CV_CHECKSUM_MAGIC`) | 16-byte trailer after each area | **Magic-gated**: `ColumnVersionReader` present-detection requires `getLong(offset+size) == CV_CHECKSUM_MAGIC`; a page-rounded legacy `_cv` (non-zero, non-magic bytes there) reads as "absent" → skip. `OFFSET_SIZE_{A,B}` still records data-only size, so old readers never look past it. | **EXISTING** `ColumnVersionWriterTest.testCvChecksumAbsent{OldFormat,RealLegacyShape,PageRoundedNoTrailer}` |

**Common design pattern.** Every new field is *additive in previously-reserved space* and gated by a
version bump (meta), a zero sentinel (`_txn`), or a 64-bit magic (`_cv`, `_event`, `_snapshot`). This
mirrors how `TTL` (v1) and `TABLE_FORMAT` (v2) were added before it — the same forward/back-compat
contract QuestDB already ships. Artifacts #2/#3 do not even need a gate for the *read* path because
they are separate files; they need one only for the *recovery* path, which the effective-mode gate
provides.

## Downgrade (turning adaptive off)

Two supported downgrade actions:

- **Per-table:** `ALTER TABLE t SET PARAM commit_mode='nosync'` (or `'sync'`/`'async'`/`'unset'`).
- **Global:** lower `cairo.commit.mode` from `adaptive` (affects every table whose `_meta` is UNSET).

Neither purges the leftover `_snapshot`/`.epoch` — they stay on disk. They are safe because:

- **Live operation** never reads them (artifacts #2/#3 are separate files off the read path). The table
  keeps applying WAL under its new (non-adaptive) mode; no suspend, no corruption.
- **On reboot**, `RecoveryCoordinator.recover()` iterates every WAL table but **skips roll-forward when
  `resolveEffectiveCommitMode(token) != ADAPTIVE`** (`RecoveryCoordinator.java:118`). A downgraded
  table's effective mode is now non-adaptive (from `_meta` for the per-table switch, or from the global
  config for the UNSET case — `resolveEffectiveCommitMode` reads both), so its stale `_snapshot` marker
  is never even loaded. The live `_txn` (ahead of the frozen epoch) is left untouched.

**Why the gate is load-bearing — the downgrade hazard it closes.** The WAL purge floor pins segments
back to the durable epoch **only under ADAPTIVE** (`WalPurgeJob.java:543–548`). After a downgrade the
floor drops to the applied seqTxn, so segments above the frozen epoch become purgeable. *If* recovery
still rolled the live `_txn`/`_cv` back to the stale epoch (as it does for a genuinely-adaptive table),
the subsequent WAL replay of `(epoch, live]` could find those segments **purged** → data loss or a
suspended table. The effective-mode gate is exactly what prevents that rollback for a downgraded table.
This is the crux of downgrade safety and is proven by
`testDowngradeThenRebootPreservesDataAndIgnoresStaleEpoch`, whose non-vacuity asserts the stale marker
is still present and the below-live WAL was force-purged, yet all rows survive — only possible if
`recover()` skipped the roll-forward.

## Compatibility matrix

`{old,new} binary × {adaptive on, off}` at db-open. "old" = any released binary or the pre-adaptive
master fork point (meta ≤ v2, no `commit_mode` field, no recovery, no epoch artifacts). "new" = this
branch.

| binary → / db written by ↓ | new binary | old binary |
|---|---|---|
| **new, adaptive off** (nosync/sync/…) | normal | normal — db is byte-identical to what old writes (no new artifacts unless a table is adaptive) |
| **new, adaptive on** | normal + recovery | **the SP-E case**: old binary ignores `_snapshot`/`.epoch` (separate files), reads `_meta` as UNSET→global (pre-v3 gate on *its* side is trivially "field absent"), reads `_txn`/`_cv`/`_event` bodies (magic/zero-gated trailers) |
| **old, adaptive n/a** | new binary reads it fine (old formats are all supported) | baseline |

The in-process tests prove the **new-binary** column (including new-binary-reading-downgraded-db) and
the *gates* that the old-binary column relies on. The **old-binary column itself** is the external
protocol.

## Test plan (TDD, house style: fluent `assertQuery` / `AbstractCairoTest`)

New tests in `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveUpgradeCompatTest.java`:

1. `testMetaCommitModeFieldIsInertOnPreV3Meta` — create `WITH commit_mode='adaptive'` (real v3 meta,
   field = ADAPTIVE). Poke the meta minor-version high short 3→2 (keeping the checksum low short valid)
   → a pre-v3 meta. Reopen via a fresh `TableReaderMetadata.loadMetadata()` → `getCommitMode()` ==
   `UNSET`; assert `effectiveCommitMode(UNSET, NOSYNC)` == NOSYNC. **Non-vacuity A/B:** the same file
   at v3 (control) → `getCommitMode()` == ADAPTIVE; `effectiveCommitMode(ADAPTIVE, NOSYNC)` == ADAPTIVE.
   Only the one version-gate byte differs, so the gate is provably what makes the field inert.
2. `testStraySnapshotAndEpochArtifactsAreInertOnNormalOpen` — create a plain nosync table, insert,
   drain. Fabricate the epoch trio in the table dir (`SnapshotMarker.write` + copy live `_txn`/`_cv` to
   `.epoch`). Assert they exist. A normal query + a fresh `TableReader` read the correct data (no
   choke), and the artifacts remain (untouched → ignored). Reboot on a fresh `CairoEngine` → clean open,
   data intact, no suspend (recover() skips: effective mode != ADAPTIVE). Non-vacuity: artifacts present
   throughout.
3. `testDowngradeFromAdaptiveDrainsCleanlyAndArtifactsAreInert` (downgrade, live) — global adaptive +
   epoch-every-batch; create table, insert, drain → epoch trio present. `ALTER … SET PARAM
   commit_mode='nosync'`, drain. Insert more, drain. Assert: not suspended, all rows present,
   `wal_tables().commitMode` == nosync. Artifacts remain (inert).
4. `testDowngradeThenRebootPreservesDataAndIgnoresStaleEpoch` (downgrade, reboot) — as #3, then force a
   WAL purge (drops segments below live under the nosync floor), reboot on a fresh `CairoEngine`, drain.
   Assert: all rows survive, not suspended, and (non-vacuity) the `_snapshot` marker still exists with
   `epochSeqTxn < live seqTxn` and the below-live WAL dirs were purged — so a roll-forward would have
   been lossy, proving `recover()` skipped it.

Cited (no duplication): #4 `_event` = `WalEventChecksumTest.testLegacyRecordWithoutTrailerStillReads`;
#5 `_txn` = `TxnTest.testOpenOldFormatTxn_noBodyChecksum`; #6 `_cv` =
`ColumnVersionWriterTest.testCvChecksumAbsent{OldFormat,RealLegacyShape,PageRoundedNoTrailer}`.

**No behavior is weakened.** Every new assertion is a read-compat / inertness assertion. The
non-vacuity controls (torn-trailer suspends, v3 field is read, purged-WAL-yet-data-survives) show the
gates are load-bearing.

## External-matrix note (cannot be verified in-process — needs a real old binary)

The `{old-binary} × {adaptive on/off}` open matrix and the rolling-upgrade *cluster* test require
running a real prior binary against a db an adaptive binary wrote. Proposed old-binary set (adaptive
branched from master at `9.4.4-SNAPSHOT`, fork commit `cdb0ea073b`, meta `LATEST=2`; the commit-mode
v3 field has **never shipped in a release**):

- **`9.4.4-SNAPSHOT` @ `cdb0ea073b`** — the immediate pre-adaptive master fork point, meta **v2**
  (table_format). The most important old binary: it exercises exactly the v2→v3 gap for the same code
  base minus adaptive.
- **`9.4.0`** — the latest GA release, meta **v1**.
- **`9.3.5`** and **`9.2.3`** — the prior two majors' latest patches, meta **v1**.

Protocol (detailed in the runbook): write a db with the new binary (one adaptive table that has taken a
durable epoch, one nosync sibling), cleanly stop, open with each old binary, assert: boots, both tables
readable, the adaptive table's data correct, no error referencing `_snapshot`/`.epoch`/`commit_mode`.
The in-process gate tests are the evidence that this will pass; the external run is the confirmation.

## Scope

OSS core only. New: one test class + this spec + the runbook. **No production-code change** — SP-E is a
*validation + documentation* sub-project; the gates it verifies already exist (they were built with the
artifacts). Enterprise inherits the gates via the submodule with no ent-side change. Branch kept as-is
per the standing integration/merge exclusion.

## Out of scope (SP-E backlog / other sub-projects)

- The external old-binary + rolling-upgrade *execution* (operator-run; this spec hands over the
  protocol + version set).
- Any migration tooling (none is needed — the formats are forward/back-compatible, no rewrite).
- Enterprise replication/backup coexistence under the epoch → **SP-A**.
- Proactively purging `_snapshot`/`.epoch` on downgrade: **not required** (the effective-mode recovery
  gate makes them inert), and deliberately avoided to keep the recovery path the single source of
  downgrade safety. Noted as a possible future hygiene optimization only.
</content>
</invoke>
