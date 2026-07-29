/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;

/**
 * Adaptive durable-EPOCH recovery roll-forward (Plan 3 Task C).
 * <p>
 * Runs ONCE at engine startup, AFTER the table-name registry is loaded but BEFORE normal WAL apply
 * ({@code CheckWalTransactionsJob} -> {@code ApplyWal2TableJob}) runs for any table. For each
 * ADAPTIVE WAL table that recorded a durable epoch ({@code _snapshot} marker + immutable
 * {@code _meta.epoch}/{@code _txn.epoch}/{@code _cv.epoch} copies written by
 * {@link TableWriter#fsyncMaterializedState()}), it RESTORES the durable cut: publishes the
 * matching metadata and copies {@code _txn.epoch}->{@code _txn} / {@code _cv.epoch}->{@code _cv},
 * fsyncing them (and the table dir) BEFORE proceeding, so the table
 * opens at exactly {@code epoch.{seqTxn,txn}}. The existing boot path then idempotently re-applies
 * {@code (epoch.seqTxn, frontier]} from the durable WAL.
 * <p>
 * This is the keystone that makes ADAPTIVE crash-safe end to end: under ADAPTIVE the table apply is
 * LAZY (partition columns are non-durable between epochs — see {@link CommitMode#appliesColumnSync}),
 * so after a power cut the materialized state can be torn ahead of the last durable epoch. Rewinding
 * {@code _txn}/{@code _cv} to the durable cut and re-deriving the rest from the durable WAL rebuilds
 * exactly the lost rows.
 *
 * <h2>Why this is safe and idempotent</h2>
 * <ul>
 *   <li><b>Restore-before-rely (audit #5):</b> the copied {@code _txn}/{@code _cv} are fsync'd, and
 *       the table dir is fsync'd, BEFORE recovery returns — so a crash immediately after recovery
 *       sees the restored cut, not a half-copied file.</li>
 *   <li><b>Idempotent:</b> the {@code .epoch} copies are IMMUTABLE until the next epoch overwrites
 *       them ({@link TableWriter#fsyncMaterializedState()} rewrites them only when a new, later cut
 *       is taken). Re-running recovery (e.g. a crash mid-recovery) re-copies the same bytes and lands
 *       on the same cut. Apply itself is idempotent through {@code _txn} (the contiguity assert
 *       {@code seqTxn == appliedSeqTxn + 1} is satisfied because {@code _txn} now says
 *       {@code epoch.seqTxn}).</li>
 *   <li><b>Conservative fallback:</b> a table with NO {@code _snapshot} marker (or whose
 *       {@code .epoch} copies are absent) is left UNTOUCHED -> today's behaviour (full WAL replay /
 *       normal open). Non-adaptive tables and non-WAL tables are never touched.</li>
 * </ul>
 */
public class RecoveryCoordinator {
    private static final Log LOG = LogFactory.getLog(RecoveryCoordinator.class);
    // Left in a table dir by a restore that cleared the table's epoch artifacts, to say the cleared state
    // is a trustworthy restored cut rather than a lost anchor. Consumed once the baseline is republished.
    public static final String RESTORE_ENROL_FILE_NAME = "_epoch.enrol";
    private final boolean checkpointRestored;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;

    public RecoveryCoordinator(CairoEngine engine) {
        this(engine, false);
    }

    public RecoveryCoordinator(CairoEngine engine, boolean checkpointRestored) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
        this.checkpointRestored = checkpointRestored;
    }

    /**
     * Restore the durable epoch cut for every adaptive WAL table that has one. Must be called once,
     * after the table registry is loaded and before any WAL apply.
     */
    public void recover() {
        // Durable epochs are a per-table property. Do not short-circuit on the global mode: a table-level
        // ADAPTIVE override on a NOSYNC instance still has a creation baseline and must be recovered.
        // Every adaptive WAL table must have a trustworthy marker/generation; absence fails startup closed.
        final ObjHashSet<TableToken> tokens = new ObjHashSet<>();
        final ObjList<TableToken> checkpointEnrollments = new ObjList<>();
        engine.getTableTokens(tokens, false);

        try (Path src = new Path(); Path dst = new Path(); Path dir = new Path()) {
            for (int i = 0, n = tokens.size(); i < n; i++) {
                final TableToken token = tokens.get(i);
                // Skip regular views: a view token is isWal()==true but has no _meta/_txn/_cv/data and no
                // durable epoch, and its ViewState is not yet hydrated at this point in completeInit()
                // (views compile lazily, after recover()) — so resolveEffectiveCommitMode()'s metadata read
                // would throw `view does not exist` and fail boot. Mat-views (isView()==false) keep their
                // on-disk _meta and are still recovered.
                if (!token.isWal() || token.isView()) {
                    continue;
                }
                try {
                    // On cold boot this may read _meta. Any inability to determine or restore an adaptive
                    // table's replay floor aborts initialization; sequencer suspension does not fence readers.
                    final boolean metadataBoundEpoch = hasMetadataBoundEpoch(token, dir);
                    final boolean enrolledAdaptive;
                    try {
                        final int effectiveMode = metadataBoundEpoch
                                ? resolveEffectiveCommitModeNoRetry(token, dir)
                                : engine.getTableSequencerAPI().resolveEffectiveCommitMode(token);
                        // The effective mode says how this table will be written NEXT; the enrolled record
                        // says how its materialized state was LEFT. A table applied lazily under adaptive is
                        // torn ahead of its durable epoch no matter what the config file says on the way back
                        // up, so an operator turning adaptive off must not turn the repair off with it.
                        // Skipping requires BOTH: not adaptive now, and no lazy state to reconcile.
                        enrolledAdaptive = readEnrolledCommitMode(token, dir) == CommitMode.ADAPTIVE;
                        if (effectiveMode != CommitMode.ADAPTIVE && !enrolledAdaptive) {
                            continue;
                        }
                        failIfRecoveryDisabled(token);
                    } catch (CairoException | CairoError metadataFailure) {
                        // A crash during structural metadata swap can leave live _meta absent/torn while the
                        // previous durable epoch still has a bound, checksummed _meta payload. Recover from
                        // that generation before a retrying metadata reader can spin forever on a pinned clock.
                        if (metadataBoundEpoch) {
                            failIfRecoveryDisabled(token);
                            recoverTable(token, src, dst, dir);
                            continue;
                        }
                        throw metadataFailure;
                    }
                    tablePath(dir, token).concat(TableUtils.SNAPSHOT_FILE_NAME);
                    if (!ff.exists(dir.$()) && (checkpointRestored || isMarkedForRestoreEnrolment(token, dir))) {
                        // A restore is a trustworthy, internally consistent materialized cut, but restore
                        // metadata intentionally excludes adaptive epoch anchors -- both the checkpoint path
                        // (TableSnapshotRestore) and an out-of-process restore that clears them via
                        // removeAdaptiveEpochArtifacts, because a SOURCE anchor left behind would roll the
                        // freshly restored _txn/_cv back to a pre-restore cut. Record the table for
                        // synchronous baseline publication below. A one-startup in-memory exemption is not
                        // sufficient: a second restart before WAL apply must find a durable marker.
                        //
                        // checkpointRestored is an in-process flag, so it cannot speak for a restore that ran
                        // before this engine existed; such a restore leaves the per-table marker instead (see
                        // markRestoredForEnrolment). Without one of the two signals an absent anchor is
                        // indistinguishable from a table whose anchor was lost, which recoverTable must keep
                        // refusing.
                        checkpointEnrollments.add(token);
                        continue;
                    }
                    if (!ff.exists(dir.$()) && !enrolledAdaptive) {
                        // A table that was never enrolled cannot have an anchor, and cannot have been applied
                        // lazily either -- enrollment is what permits lazy apply, and it publishes the anchor
                        // first. Its live state is exactly as durable as the mode it was written under
                        // promised, so there is nothing here to roll forward. The TableWriter constructor
                        // performs the crash-safe enrollment (baseline at the live cut, then the record)
                        // before this table can be applied lazily for the first time.
                        //
                        // This is the ONLY case in which an absent anchor is not a hard failure. An enrolled
                        // table with no anchor falls through to recoverTable and is refused: its state may be
                        // ahead of a cut nothing can name any more.
                        continue;
                    }
                    recoverTable(token, src, dst, dir);
                } catch (CairoException | CairoError e) {
                    if (CairoException.isDataSyncFailure(e)) {
                        // A writeback failure poisons the process-wide page-cache durability state. Do not
                        // serve siblings or retry recovery in the same process.
                        engine.handleDataSyncFailure(e);
                    }
                    // An adaptive table may not be exposed from its possibly non-durable live state.
                    // Abort engine initialization instead of merely suspending WAL apply (readers are not
                    // fenced by sequencer suspension).
                    throw e;
                }
            }
        }

        // Do this only after the validation/restoration pass has completed for every table. Publish directly
        // from the restored files rather than opening TableWriter: constructor maintenance (index/purge repair)
        // must not mutate checkpoint state before the caller's configured recovery jobs run.
        for (int i = 0, n = checkpointEnrollments.size(); i < n; i++) {
            final TableToken token = checkpointEnrollments.getQuick(i);
            try (TableMetadata metadata = engine.getTableMetadata(token);
                 Path markerPath = new Path();
                 SnapshotMarker marker = new SnapshotMarker(configuration)) {
                DurableEpochManifest.publishCheckpointRestored(
                        configuration,
                        token,
                        metadata.getTimestampType(),
                        metadata.getPartitionBy(),
                        configuration.getMicrosecondClock().getTicks() / 1000L
                );
                markerPath.of(configuration.getDbRoot()).concat(token).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(markerPath.$());
                if (!marker.tryLoad()) {
                    throw CairoException.critical(0)
                            .put("checkpoint-restored adaptive baseline marker is invalid [table=")
                            .put(token.getTableName()).put(']');
                }
                pinRecoveredEpoch(token, marker.getEpochTxn(), marker.getEpochSeqTxn());
                // Consume the restore marker only now: the baseline above is durable and validated, so a
                // crash before this point simply re-enrols on the next start rather than losing the signal
                // and refusing to boot.
                markerPath.of(configuration.getDbRoot()).concat(token).concat(RESTORE_ENROL_FILE_NAME);
                ff.removeQuiet(markerPath.$());
            } catch (CairoException | CairoError e) {
                if (CairoException.isDataSyncFailure(e)) {
                    engine.handleDataSyncFailure(e);
                }
                throw e;
            }
        }
    }

    private void failIfRecoveryDisabled(TableToken token) {
        if (!configuration.isAdaptiveRecoveryRollForwardEnabled()) {
            // Skipping recovery can expose a lazily materialized, torn table. Keep the configuration seam for
            // negative-control tests, but never let a production startup turn it into a fail-open read path.
            throw CairoException.critical(0)
                    .put("adaptive recovery is disabled; refusing unsafe startup [table=")
                    .put(token.getTableName()).put(']');
        }
    }

    private boolean hasMetadataBoundEpoch(TableToken token, Path markerPath) {
        tablePath(markerPath, token).concat(TableUtils.SNAPSHOT_FILE_NAME);
        if (!ff.exists(markerPath.$())) {
            return false;
        }
        try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
            marker.of(markerPath.$());
            final SnapshotMarker.Candidate[] candidates = marker.loadCandidates();
            for (int i = 0; i < candidates.length; i++) {
                final SnapshotMarker.Candidate candidate = candidates[i];
                if (candidate.formatVersion != SnapshotMarker.LEGACY_FORMAT_VERSION
                        && DurableEpochManifest.isMetadataBound(configuration, token, candidate.generation)) {
                    return true;
                }
            }
        } catch (Throwable ignored) {
            return false;
        }
        return false;
    }

    private boolean isMarkedForRestoreEnrolment(TableToken token, Path dir) {
        tablePath(dir, token).concat(RESTORE_ENROL_FILE_NAME);
        return ff.exists(dir.$());
    }

    private int resolveEffectiveCommitModeNoRetry(TableToken token, Path metaPath) {
        tablePath(metaPath, token).concat(TableUtils.META_FILE_NAME);
        try (TableReaderMetadata metadata = new TableReaderMetadata(configuration)) {
            metadata.loadMetadata(metaPath.$());
            return CommitMode.effectiveCommitMode(metadata.getCommitMode(), configuration.getCommitMode());
        }
    }

    /**
     * The commit mode this table's materialized state is enrolled under, read straight from {@code _meta}
     * (see {@link TableUtils#META_OFFSET_ENROLLED_COMMIT_MODE}). {@link CommitMode#ADAPTIVE} means the state
     * may be lazily ahead of the durable epoch; anything else means it may not.
     * <p>
     * Fails closed on an unreadable {@code _meta} rather than defaulting to "not enrolled": the caller uses
     * a non-ADAPTIVE answer to permit an absent anchor, and inferring that from a file it could not read
     * would turn a corrupt table into a silent live-state fallback. The callers reach here only after the
     * effective-mode resolution above has already read the same file, so this throws only if it became
     * unreadable in between.
     */
    private int readEnrolledCommitMode(TableToken token, Path metaPath) {
        tablePath(metaPath, token).concat(TableUtils.META_FILE_NAME);
        final long size = ff.length(metaPath.$());
        if (size <= 0) {
            throw CairoException.critical(ff.errno())
                    .put("could not read table metadata to resolve adaptive enrollment [table=")
                    .put(token.getTableName()).put(", path=").put(metaPath).put(']');
        }
        try (MemoryCMR metaMem = Vm.getCMRInstance(ff, metaPath.$(), size, MemoryTag.MMAP_TABLE_READER)) {
            return TableUtils.getEnrolledCommitMode(metaMem);
        }
    }

    private void recoverTable(TableToken token, Path src, Path dst, Path dir) {
        tablePath(dir, token).concat(TableUtils.SNAPSHOT_FILE_NAME);
        if (!ff.exists(dir.$())) {
            throw CairoException.critical(0)
                    .put("adaptive epoch marker is absent; refusing unsafe live-state fallback [table=")
                    .put(token.getTableName()).put(']');
        }

        SnapshotMarker.Candidate selected = null;
        boolean repairMarkerSelector = false;
        try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
            tablePath(dir, token).concat(TableUtils.SNAPSHOT_FILE_NAME);
            marker.of(dir.$());
            final boolean selectorLoads = marker.tryLoad();
            final boolean loadedFromSelector = marker.wasLoadedFromSelector();
            final long selectorSeqTxn = selectorLoads ? marker.getEpochSeqTxn() : -1;
            final long selectorTxn = selectorLoads ? marker.getEpochTxn() : -1;
            final int selectorGeneration = selectorLoads ? marker.getGeneration() : SnapshotMarker.LEGACY_GENERATION;
            final SnapshotMarker.Candidate[] candidates = marker.loadCandidates();
            for (int i = 0; i < candidates.length; i++) {
                final SnapshotMarker.Candidate candidate = candidates[i];
                // V2 additionally verifies the binding manifest. Legacy V1 remains readable when both
                // internally checksummed payloads load and their complete available txn tuple matches.
                if (epochCopiesValid(token, src, candidate)) {
                    selected = candidate;
                    repairMarkerSelector = !loadedFromSelector
                            || selectorSeqTxn != candidate.epochSeqTxn
                            || selectorTxn != candidate.epochTxn
                            || selectorGeneration != candidate.generation;
                    break;
                }
            }
        }
        if (selected == null) {
            throw CairoException.critical(0)
                    .put("no trustworthy adaptive epoch generation; refusing unsafe live-state fallback [table=")
                    .put(token.getTableName()).put(']');
        }
        final long epochSeqTxn = selected.epochSeqTxn;
        final int epochGeneration = selected.generation;

        // C2 (restore / checkpoint / PITR coexistence): a durable epoch is a PAST cut, so in one lineage the
        // live _txn is always at or ahead of it (lazy apply only advances _txn after an epoch is taken). If
        // the live _txn loads cleanly at a seqTxn BELOW this epoch, the table was rewound BENEATH a stale,
        // higher-lineage epoch by a backup / checkpoint-recover / PITR restore that left _snapshot/.epoch
        // behind (nothing on the restore path clears them). Rolling _txn forward to that epoch would
        // resurrect the discarded lineage and leave _txn ahead of the restored sequencer. SKIP; the restored
        // live state + normal WAL replay is correct. A TORN/unreadable live _txn (the genuine post-crash
        // state this mechanism exists to repair) does NOT trip the guard — see epochIsAheadOfLiveTxn.
        if (epochIsAheadOfLiveTxn(token, src, selected)) {
            throw CairoException.critical(0)
                    .put("adaptive epoch post-dates live state; refusing wrong-lineage recovery [table=")
                    .put(token.getTableName()).put(", epochSeqTxn=").put(epochSeqTxn).put(']');
        }

        // SYMMETRIC C2 (review Finding C2, second half). The guard above rejects an epoch AHEAD of the live
        // _txn. The reverse — a stale epoch BELOW a divergently-restored live _txn — is NOT (and cannot cheaply
        // be) rejected here: seqTxn carries no lineage tag, so a below-_txn epoch is indistinguishable from the
        // NORMAL same-lineage past cut that recover() is meant to adopt (rewind _txn/_cv to the epoch, then
        // WAL-replay forward). Its safety rests entirely on the INVARIANT that every path which rewinds or
        // diverges the live _txn (a backup / checkpoint / PITR restore; a primary->replica demote) clears
        // _snapshot/.epoch via TableSnapshotRestore -> RecoveryCoordinator.removeAdaptiveEpochArtifacts. So any
        // epoch that SURVIVES to this adoption point is necessarily the SAME lineage as the live _txn (a
        // legitimate fast-boot anchor), never a stale wrong-lineage one. A full runtime symmetric guard is
        // infeasible (no cheap lineage signal — see the AHEAD-case reasoning in epochIsAheadOfLiveTxn), so this
        // is a documented invariant. Defensive, assertion-only re-check (read-only, zero prod cost): a refactor
        // that ever let an AHEAD epoch reach adoption — the wrong-lineage resurrection this guard prevents —
        // trips here loudly instead of silently corrupting the restored table.
        assert !epochIsAheadOfLiveTxn(token, src, selected)
                : "adaptive recovery would ADOPT an epoch that post-dates the live _txn (wrong-lineage "
                + "resurrection); the freshness guard must SKIP it [table=" + token.getTableName()
                + ", epochSeqTxn=" + epochSeqTxn + ']';

        // Restore the durable cut. V2 manifests first restore _meta from the same generation,
        // so the epoch _txn can always be interpreted even if structural WAL transactions advanced live
        // metadata after the epoch. The metadata swap is fsync'd and renamed before pointer restoration.
        final boolean metadataBound = selected.formatVersion != SnapshotMarker.LEGACY_FORMAT_VERSION
                && DurableEpochManifest.isMetadataBound(configuration, token, epochGeneration);
        if (metadataBound) {
            restoreMetadataFile(token, src, dst, epochGeneration);
            fsyncDir(token, dir);
        }

        // Restore _txn.epoch -> _txn, _cv.epoch -> _cv. TableUtils.replaceFileContent (truncate + transfer)
        // fully replaces the live, lazily-advanced files with the epoch's canonical A/B record. The
        // .epoch copies are immutable until the next epoch, so re-running this (a crash mid-recovery)
        // re-copies identical bytes and lands on the same cut -> idempotent.
        //
        // Restore _txn (the pointer) BEFORE _cv (the data) here. We are overwriting live files that, post-
        // crash, sit at the FRONTIER. A crash BETWEEN these two restores leaves _txn at the (older) epoch
        // while _cv is still at the (newer) frontier -> _txn behind _cv, the SAFE skew (the older _txn
        // never references column versions beyond it). The reverse would briefly leave _txn at the frontier
        // over an epoch _cv (a dangling reference). Recovery re-runs on the next boot and completes the pair.
        //
        // NOTE: the restore truncates its destination before writing, so a restore that fails
        // mid-transfer leaves the live file torn. Recovery propagates that failure and startup aborts before
        // readers can observe it. The next startup retries from the immutable validated generation.
        restoreFile(token, src, dst, TableUtils.TXN_FILE_NAME, epochGeneration);
        restoreFile(token, src, dst, TableUtils.COLUMN_VERSION_FILE_NAME, epochGeneration);

        // Restore-BEFORE-rely (audit #5): the copied _txn/_cv must be durable, and their directory
        // entries (sizes/names) journaled, BEFORE the boot path opens the table and re-applies the WAL
        // on top of this cut. fsync each restored file's fd, then the table dir.
        fsyncFile(token, dst, TableUtils.TXN_FILE_NAME);
        fsyncFile(token, dst, TableUtils.COLUMN_VERSION_FILE_NAME);
        fsyncDir(token, dir);
        if (repairMarkerSelector) {
            // The selector word is unchecksummed and may have reverted, or its selected generation may have
            // failed payload validation. Re-publish the generation we actually restored before any later
            // epoch can overwrite that last trustworthy payload as the nominally "inactive" generation.
            tablePath(dir, token).concat(TableUtils.SNAPSHOT_FILE_NAME);
            try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                marker.of(dir.$());
                if (selected.generation == SnapshotMarker.LEGACY_GENERATION) {
                    marker.write(selected.epochSeqTxn, selected.epochTxn, selected.epochTs);
                } else {
                    marker.write(selected.epochSeqTxn, selected.epochTxn, selected.epochTs, selected.generation);
                }
            }
        }
        if (metadataBound) {
            // epochIsAheadOfLiveTxn and runtime users may have populated pooled/catalogue metadata from the
            // pre-recovery schema. A real startup has no such tenant; evict it before WAL replay.
            engine.resetTableMetadataForRecovery(token);
        }

        LOG.info().$("adaptive epoch roll-forward restored durable cut [table=").$(token)
                .$(", epochSeqTxn=").$(epochSeqTxn).I$();

        // Bump the in-memory recovery incarnation counter ONLY on a successful validated restore
        // (not on no-op/skip/absent-marker/torn-copy paths). The tracker is initialised lazily on
        // first WAL apply, but the object itself always exists (getSeqTxnTracker creates it).
        final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        pinRecoveredEpoch(token, selected.epochTxn, epochSeqTxn);
        tracker.bumpRecoveryIncarnation();
        engine.getMetrics().walMetrics().incrementRecoveryEvents();
    }

    private void pinRecoveredEpoch(TableToken token, long epochTxn, long epochSeqTxn) {
        final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        final long priorEpochTxn = tracker.getPinnedEpochTxn();
        if (priorEpochTxn == epochTxn) {
            tracker.setDurableEpochSeqTxn(epochSeqTxn);
            return;
        }
        final boolean priorSlotA = tracker.isPinnedEpochSlotA();
        // Runtime checkpoint recovery can replace a live engine's existing epoch pin. Reserve the other
        // ping-pong slot first, then release the old slot only after the new cut is protected.
        boolean slotA = priorEpochTxn < 0 || !priorSlotA;
        try (TxnScoreboard scoreboard = engine.getTxnScoreboard(token)) {
            int newSlotId = slotA ? TxnScoreboard.EPOCH_ID_A : TxnScoreboard.EPOCH_ID_B;
            if (!scoreboard.incrementTxn(newSlotId, epochTxn)) {
                if (priorEpochTxn >= 0) {
                    throw CairoException.critical(0)
                            .put("could not pin recovered adaptive epoch [table=").put(token.getTableName())
                            .put(", epochTxn=").put(epochTxn).put(']');
                }
                slotA = false;
                newSlotId = TxnScoreboard.EPOCH_ID_B;
                if (!scoreboard.incrementTxn(newSlotId, epochTxn)) {
                    throw CairoException.critical(0)
                            .put("could not pin recovered adaptive epoch [table=").put(token.getTableName())
                            .put(", epochTxn=").put(epochTxn).put(']');
                }
            }
            if (priorEpochTxn >= 0) {
                scoreboard.releaseTxn(
                        priorSlotA ? TxnScoreboard.EPOCH_ID_A : TxnScoreboard.EPOCH_ID_B,
                        priorEpochTxn
                );
            }
        }
        tracker.setPinnedEpoch(epochTxn, slotA);
        tracker.setDurableEpochSeqTxn(epochSeqTxn);
    }

    /**
     * C1 guard: validate the immutable {@code _txn.epoch}/{@code _cv.epoch} copies BEFORE they are
     * allowed to overwrite the live files. Returns {@code true} only if BOTH copies fully load via the
     * same A/B-checksummed readers the engine uses ({@link TxReader#unsafeLoadAll()} /
     * {@link ColumnVersionReader#readSafe()}) AND the loaded {@code _txn.epoch} {@code seqTxn} matches
     * the {@code _snapshot} marker's {@code epochSeqTxn} (the three anchor files agree on one cut).
     * <p>
     * Any failure mode of a TORN copy is treated as invalid and returns {@code false}:
     * <ul>
     *   <li>a 0-byte / short {@code _txn.epoch} -> {@code TxReader.ofRO} throws {@code CairoException}
     *       ({@code fileNotFound}: length below the base header);</li>
     *   <li>a full-size but corrupt-body copy -> the reader returns {@code false} (one slot torn) or
     *       throws {@code CairoException} (both A/B slots fail their checksum);</li>
     *   <li>a SIGBUS reading past a truncated mmap -> {@code InternalError} / {@code CairoError};</li>
     *   <li>a stale copy whose {@code seqTxn} != the marker -> a clean load but a mismatch.</li>
     * </ul>
     * An invalid candidate is never copied over a live file. Recovery tries the previous marker generation;
     * if no bound candidate validates, startup fails closed rather than trusting live materialized state.
     */
    private boolean epochCopiesValid(TableToken token, Path scratch, SnapshotMarker.Candidate candidate) {
        final long markerEpochSeqTxn = candidate.epochSeqTxn;
        final int epochGeneration = candidate.generation;
        final boolean metadataBound = candidate.formatVersion != SnapshotMarker.LEGACY_FORMAT_VERSION
                && DurableEpochManifest.isMetadataBound(configuration, token, epochGeneration);
        int partitionBy;
        int timestampType;
        long metadataVersion;
        TableReaderMetadata epochMetadata = null;
        try {
            if (metadataBound) {
                epochCopyPath(scratch, token, TableUtils.META_FILE_NAME, epochGeneration);
                epochMetadata = new TableReaderMetadata(configuration);
                epochMetadata.loadMetadata(scratch.$());
                if (epochMetadata.getTableId() != token.getTableId()) {
                    return false;
                }
                partitionBy = epochMetadata.getPartitionBy();
                timestampType = epochMetadata.getTimestampType();
                metadataVersion = epochMetadata.getMetadataVersion();
            } else {
                try (TableMetadata meta = engine.getTableMetadata(token)) {
                    partitionBy = meta.getPartitionBy();
                    timestampType = meta.getTimestampType();
                    metadataVersion = meta.getMetadataVersion();
                }
            }
        } catch (CairoException | CairoError e) {
            // Cannot interpret the candidate without its matching metadata; mark it invalid. If no other
            // candidate validates, recoverTable() aborts startup instead of spinning on a permanent mismatch.
            LOG.error().$("adaptive epoch validation could not read epoch metadata [table=")
                    .$(token).$(", error=").$safe(e.getFlyweightMessage()).I$();
            Misc.free(epochMetadata);
            return false;
        }

        // Validate _txn.epoch: a clean A/B-checksummed load whose seqTxn equals the marker's epoch cut.
        TxReader txReader = null;
        ColumnVersionReader cvReader = null;
        try {
            epochCopyPath(scratch, token, TableUtils.TXN_FILE_NAME, epochGeneration);
            txReader = new TxReader(ff);
            txReader.ofRO(scratch.$(), timestampType, partitionBy);
            if (!txReader.unsafeLoadAll()) {
                return false; // torn _txn.epoch (one slot bad; the other absent/old)
            }
            final long copySeqTxn = txReader.getSeqTxn();
            if (copySeqTxn != markerEpochSeqTxn
                    || txReader.getTxn() != candidate.epochTxn
                    || txReader.getMetadataVersion() != metadataVersion) {
                LOG.error().$("adaptive epoch _txn identity does not match _snapshot marker [table=").$(token)
                        .$(", copySeqTxn=").$(copySeqTxn).$(", markerSeqTxn=").$(markerEpochSeqTxn)
                        .$(", copyTxn=").$(txReader.getTxn()).$(", markerTxn=").$(candidate.epochTxn).I$();
                return false;
            }

            // Validate _cv.epoch: a clean A/B-checksummed load (readSafe verifies the live area's checksum
            // and only adopts a self-consistent record).
            epochCopyPath(scratch, token, TableUtils.COLUMN_VERSION_FILE_NAME, epochGeneration);
            cvReader = new ColumnVersionReader();
            cvReader.ofRO(ff, scratch.$());
            if (!cvReader.readSafe() || txReader.getColumnVersion() != cvReader.getVersion()) {
                return false;
            }
            if (candidate.formatVersion == SnapshotMarker.LEGACY_FORMAT_VERSION) {
                return true;
            }
            tablePath(scratch, token);
            final int rootLen = scratch.size();
            return DurableEpochManifest.validate(
                    configuration,
                    token,
                    scratch,
                    rootLen,
                    epochGeneration,
                    markerEpochSeqTxn,
                    candidate.epochTxn,
                    txReader.getColumnVersion(),
                    metadataVersion
            );
        } catch (Throwable e) {
            // FAIL SAFE: ANY failure while decoding a torn copy means "invalid -> skip restore", never
            // "fall through and blindly copy it over the live file". Concrete failure modes seen on a torn
            // copy: CairoException (fileNotFound on a 0-byte _txn.epoch; both-A/B checksum mismatch), an
            // mmap bounds AssertionError or SIGBUS-as-InternalError from a garbage base header steering an
            // out-of-bounds read, CairoError. We deliberately catch broadly here because the cost of a
            // false "invalid" is only a fall-back to the (always-safe) live files + full WAL replay,
            // whereas the cost of letting it through would be the brick this guard exists to prevent. The
            // exception is confined to one table's .epoch decode and never escapes to abort recovery for
            // the rest of the tables.
            LOG.error().$("adaptive epoch durable copy failed validation, skipping roll-forward [table=").$(token)
                    .$(", error=").$safe(String.valueOf(e.getMessage())).I$();
            return false;
        } finally {
            Misc.free(cvReader);
            Misc.free(txReader);
            Misc.free(epochMetadata);
        }
    }

    /**
     * Restore/PITR freshness guard. Returns {@code true} iff the LIVE {@code _txn} loads cleanly AND its
     * {@code seqTxn} is strictly BELOW {@code epochSeqTxn} — i.e. the durable epoch is AHEAD of the current
     * materialized state. In a single lineage that cannot happen (an epoch is a past cut; lazy apply only
     * advances {@code _txn} after it), so it signals the table was rewound beneath a stale, higher-lineage
     * epoch by a backup / checkpoint-recover / PITR restore that failed to clear {@code _snapshot}/{@code
     * .epoch}.
     * <p>
     * FAIL-OPEN on an unreadable live {@code _txn}: a torn / short / garbage {@code _txn} is exactly the
     * genuine post-crash state this whole mechanism exists to repair, so it returns {@code false} (NOT
     * ahead -> allow the roll-forward). We only ever SKIP recovery on a CLEAN load that is provably behind
     * the epoch — never on the crash case.
     */
    private boolean epochIsAheadOfLiveTxn(TableToken token, Path scratch, SnapshotMarker.Candidate candidate) {
        final long epochSeqTxn = candidate.epochSeqTxn;
        final int partitionBy;
        final int timestampType;
        try {
            if (candidate.formatVersion != SnapshotMarker.LEGACY_FORMAT_VERSION
                    && DurableEpochManifest.isMetadataBound(configuration, token, candidate.generation)) {
                epochCopyPath(scratch, token, TableUtils.META_FILE_NAME, candidate.generation);
                try (TableReaderMetadata meta = new TableReaderMetadata(configuration)) {
                    meta.loadMetadata(scratch.$());
                    partitionBy = meta.getPartitionBy();
                    timestampType = meta.getTimestampType();
                }
            } else {
                try (TableMetadata meta = engine.getTableMetadata(token)) {
                    partitionBy = meta.getPartitionBy();
                    timestampType = meta.getTimestampType();
                }
            }
        } catch (CairoException | CairoError e) {
            // Cannot interpret _txn without metadata; do NOT block recovery (the normal open path will
            // surface any real metadata problem properly).
            return false;
        }
        TxReader liveTxn = null;
        try {
            tablePath(scratch, token).concat(TableUtils.TXN_FILE_NAME);
            liveTxn = new TxReader(ff);
            liveTxn.ofRO(scratch.$(), timestampType, partitionBy);
            if (!liveTxn.unsafeLoadAll()) {
                // Torn / short / garbage live _txn = the genuine post-crash case this mechanism exists to
                // repair -> NOT "ahead" -> allow the roll-forward.
                return false;
            }

            // INVARIANT PIN (review Finding C2). The return-true SKIP below is only SOUND because, on a
            // SINGLE lineage, a CLEAN unsafeLoadAll() can never report a seqTxn BELOW the durable epoch.
            // That rests on a SLOT-SELECTION property of TxReader which we pin here so a future A/B refactor
            // cannot silently turn a genuine post-crash cut into a wrongful skip:
            //
            //   unsafeLoadAll() returns the record from the VERSION-WORD-selected (latest) A/B slot, and
            //   ONLY when that latest slot is torn does it fall back to its IMMEDIATE predecessor
            //   (version - 1). It never returns an older slot. So the loaded record's version (getVersion(),
            //   == its stored txn) is either the on-disk version word (clean latest) or exactly one below it
            //   (torn-latest fallback): versionWord - getVersion() in {0, 1}.
            //
            // Why that yields loadedSeqTxn >= epochSeqTxn on ONE lineage:
            //   - The version word is MONOTONE and durably floored at V_E: fsyncMaterializedState() fsync'd
            //     the live _txn at version=V_E / seqTxn=epochSeqTxn BEFORE copying it to _txn.epoch, and lazy
            //     apply only advances the word afterwards, so the post-crash word is >= V_E.
            //   - The predecessor (version - 1) is reached only when the latest is torn, and the latest can
            //     be torn only when the word > V_E (at word == V_E the latest slot IS the durable, un-torn
            //     epoch record — a torn latest implies a strictly-later write overwrote/advanced it), so
            //     version - 1 >= V_E there too.
            //   - seqTxn is monotone with version within a lineage, hence loadedSeqTxn >= epochSeqTxn.
            // A clean load BELOW the epoch is therefore NEVER a slot-selection artifact — it is the genuine
            // multi-lineage / stale-epoch case (a restore/PITR rewound the live _txn beneath a leftover,
            // higher-lineage epoch), which is exactly what the return-true SKIP handles.
            //
            // Why NOT a blanket `assert loadedSeqTxn >= epochSeqTxn`: that WRONG form would fire on the
            // legitimate multi-lineage skip this method exists to detect (there loadedSeqTxn < epochSeqTxn by
            // design). The invariant is single-lineage-scoped; we can soundly pin only the lineage-INDEPENDENT
            // slot-selection property (latest, or its immediate predecessor), which holds equally in the
            // multi-lineage case (a restored _txn is self-consistent and loads its own latest slot). At
            // recovery there is no concurrent writer, so the on-disk version word is stable and this is
            // race-free.
            final long loadedVersion = liveTxn.getVersion();
            final long versionWord = liveTxn.unsafeReadVersion();
            assert versionWord - loadedVersion >= 0 && versionWord - loadedVersion <= 1
                    : "TxReader A/B slot-selection regression under adaptive recovery: unsafeLoadAll must "
                    + "return the version-selected (latest) _txn slot, or its immediate predecessor when the "
                    + "latest is torn — never an older slot [table=" + token.getTableName()
                    + ", loadedVersion=" + loadedVersion + ", versionWord=" + versionWord + ']';

            return liveTxn.getSeqTxn() < epochSeqTxn;
        } catch (AssertionError ae) {
            // The invariant pin above must stay LOUD — never fail-open. A slot-selection regression is a real
            // bug, not the torn-_txn condition the broad catch below deliberately tolerates, so re-throw it
            // (otherwise the catch (Throwable) would swallow it into a silent "not ahead").
            throw ae;
        } catch (Throwable e) {
            // Torn / unreadable live _txn = the genuine post-crash case -> NOT "ahead" -> allow recovery.
            return false;
        } finally {
            Misc.free(liveTxn);
        }
    }

    /**
     * Copy {@code <fileName>.epoch} over {@code <fileName>} in the table dir (O_TRUNC replace).
     */
    private void restoreFile(TableToken token, Path src, Path dst, CharSequence fileName, int epochGeneration) {
        epochCopyPath(src, token, fileName, epochGeneration);
        tablePath(dst, token).concat(fileName);
        try {
            TableUtils.replaceFileContent(ff, src.$(), dst.$(), configuration.getWriterFileOpenOpts());
        } catch (CairoException e) {
            throw CairoException.critical(e.getErrno())
                    .put("adaptive epoch roll-forward failed to restore [table=").put(token.getTableName())
                    .put(", src=").put(src).put(", dst=").put(dst)
                    .put(", reason=").put(e.getFlyweightMessage()).put(']');
        }
    }

    private void restoreMetadataFile(TableToken token, Path src, Path dst, int epochGeneration) {
        epochCopyPath(src, token, TableUtils.META_FILE_NAME, epochGeneration);
        tablePath(dst, token).concat(TableUtils.META_FILE_NAME);
        // Replace the existing inode in place so any pooled mmap observes the restored bytes. A crash can
        // tear this copy, but the bound immutable epoch metadata remains authoritative and the next startup's
        // no-retry metadata probe re-enters recovery and copies it again.
        try {
            TableUtils.replaceFileContent(ff, src.$(), dst.$(), configuration.getWriterFileOpenOpts());
        } catch (CairoException e) {
            throw CairoException.critical(e.getErrno())
                    .put("adaptive epoch roll-forward failed to restore metadata [table=").put(token.getTableName())
                    .put(", src=").put(src).put(", dst=").put(dst)
                    .put(", reason=").put(e.getFlyweightMessage()).put(']');
        }
        fsyncFile(token, dst, TableUtils.META_FILE_NAME);
        try (TableReaderMetadata metadata = new TableReaderMetadata(configuration)) {
            metadata.loadMetadata(dst.$());
            LOG.info().$("adaptive epoch metadata restored [table=").$(token)
                    .$(", metadataVersion=").$(metadata.getMetadataVersion()).I$();
        }
    }

    /**
     * fsync a single table-dir file by path (open RW, fsync, close).
     */
    private void fsyncFile(TableToken token, Path dst, CharSequence fileName) {
        tablePath(dst, token).concat(fileName);
        final long fd = TableUtils.openRW(ff, dst.$(), LOG, configuration.getWriterFileOpenOpts());
        if (fd == -1) {
            throw CairoException.critical(ff.errno())
                    .put("adaptive epoch roll-forward could not open restored file to fsync [table=")
                    .put(token.getTableName()).put(", file=").put(dst).put(']');
        }
        try {
            ff.fsync(fd);
        } finally {
            ff.close(fd);
        }
    }

    /**
     * fsync the table directory entry so the restored file sizes/names are journaled.
     */
    private void fsyncDir(TableToken token, Path dir) {
        if (Os.isWindows()) {
            return; // no directory fsync on Windows (mirrors TableWriter's dir-sync guards)
        }
        tablePath(dir, token).slash$();
        final long dirFd = TableUtils.openRONoCache(ff, dir.$(), LOG);
        if (dirFd == -1) {
            throw CairoException.critical(ff.errno())
                    .put("adaptive recovery could not open table directory to fsync [table=")
                    .put(token.getTableName()).put(", path=").put(dir).put(']');
        }
        ff.fsyncAndClose(dirFd);
    }

    private Path tablePath(Path p, TableToken token) {
        return p.of(configuration.getDbRoot()).concat(token);
    }

    private Path epochCopyPath(Path p, TableToken token, CharSequence baseFileName, int epochGeneration) {
        tablePath(p, token).concat(baseFileName).put(TableUtils.EPOCH_COPY_SUFFIX);
        if (epochGeneration != SnapshotMarker.LEGACY_GENERATION) {
            p.put('.').put(epochGeneration);
        }
        return p;
    }

    /**
     * Fail-closed removal of the adaptive durable-epoch anchor for ONE table — the {@code _snapshot}
     * marker plus the immutable {@code _meta.epoch}/{@code _txn.epoch}/{@code _cv.epoch} copies — given {@code path} positioned
     * at the table root and {@code tableRootLen} = the length of that table-root prefix.
     * <p>
     * Call this when the local materialized state has been SUPERSEDED by an external event and the on-disk
     * epoch would otherwise be a stale, wrong-lineage anchor for {@link #recover()}:
     * <ul>
     *   <li>a backup / checkpoint / PITR <b>restore</b> (the restore rewinds {@code _txn}/{@code _cv} but
     *       does not re-copy the epoch, so a leftover epoch could roll the restored state forward again);</li>
     *   <li>a primary-&gt;replica <b>demote</b> (a replica never advances the epoch and recovers by
     *       re-download, so any local epoch is a stale primary-tenure artifact).</li>
     * </ul>
     * Removing the anchor prevents recovery from selecting a stale lineage. Absent files are accepted so
     * this remains idempotent for non-adaptive / never-epoch'd tables, but any artifact that still exists
     * after deletion is a hard restore failure. Only the {@code .epoch} copies + marker are removed; the LIVE
     * {@code _txn}/{@code _cv} are never touched. Leaves {@code path} trimmed back to the table root.
     */
    /**
     * Records that this table's epoch artifacts were cleared by a wholesale restore of its files, so the
     * next startup republishes a baseline at the RESTORED cut instead of refusing to start.
     *
     * <p>Separate from {@link #removeAdaptiveEpochArtifacts} on purpose. Clearing the artifacts is also how
     * a test manufactures a genuinely anchorless table, and how a torn or superseded anchor is discarded;
     * neither of those may be silently re-enrolled. Only a caller that KNOWS the files it just laid down are
     * a trustworthy, internally consistent cut may say so, and it says so with this call.
     *
     * <p>Marks the table rather than the database because a restore may cover a subset of tables. The
     * marker is consumed by {@code recover()} once the baseline is published.
     */
    public static void markRestoredForEnrolment(FilesFacade ff, Path path, int tableRootLen) {
        path.trimTo(tableRootLen).concat(RESTORE_ENROL_FILE_NAME);
        if (!ff.touch(path.$())) {
            throw CairoException.critical(ff.errno())
                    .put("could not mark restored table for adaptive epoch enrolment [path=").put(path).put(']');
        }
        path.trimTo(tableRootLen);
    }

    public static void removeAdaptiveEpochArtifacts(FilesFacade ff, Path path, int tableRootLen) {
        removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(TableUtils.SNAPSHOT_FILE_NAME));
        removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(TableUtils.META_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX));
        removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(TableUtils.TXN_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX));
        removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX));
        for (int generation = 0; generation < 2; generation++) {
            removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(TableUtils.META_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation));
            removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(TableUtils.TXN_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation));
            removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation));
            removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(DurableEpochManifest.FILE_NAME).put('.').put(generation));
        }
        path.trimTo(tableRootLen);
    }

    private static void removeAdaptiveEpochArtifactOrFail(FilesFacade ff, Path path) {
        if (ff.exists(path.$()) && !ff.removeQuiet(path.$()) && ff.exists(path.$())) {
            throw CairoException.critical(ff.errno())
                    .put("could not remove stale adaptive epoch artifact [path=").put(path).put(']');
        }
    }
}
