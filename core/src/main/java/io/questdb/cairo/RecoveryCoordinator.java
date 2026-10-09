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

import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexChainWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Adaptive durable-EPOCH recovery roll-forward (Plan 3 Task C).
 * <p>
 * Runs ONCE at engine startup, AFTER the table-name registry is loaded but BEFORE normal WAL apply
 * ({@code CheckWalTransactionsJob} -> {@code ApplyWal2TableJob}) runs for any table. For each
 * ADAPTIVE WAL table that recorded a durable epoch (a save point file {@code _epoch.manifest.N} holding
 * copies of {@code _meta}, {@code _txn} and {@code _cv}, the valid one with the higher cut selected), it
 * RESTORES the durable cut: writes each saved copy over its live file where they differ,
 * fsyncing them (and the table dir) BEFORE proceeding, so the table
 * opens at exactly {@code epoch.{seqTxn,txn}}. The existing boot path then idempotently re-applies
 * {@code (epoch.seqTxn, frontier]} from the durable WAL. A table whose live files are already byte-for-byte
 * the epoch copies, which is what a clean shutdown leaves, has nothing to restore: recovery only pins its
 * epoch, and does not count it as a recovery.
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
 *   <li><b>A save point decides:</b> a table is enrolled exactly when it has a save point file. Its writer
 *       publishes one before it applies anything lazily, and every path that resets its lineage removes it.
 *       Any other table, including every table a non-adaptive instance or an older build wrote, costs two
 *       existence checks and is left UNTOUCHED (normal open / WAL apply). Non-WAL tables are never touched.</li>
 *   <li><b>The exit:</b> a start outside adaptive makes every enrolled table durable, removes its save point
 *       (the older generation first) and releases its pin, so no save point outlives adaptive.</li>
 * </ul>
 */
public class RecoveryCoordinator {
    private static final Log LOG = LogFactory.getLog(RecoveryCoordinator.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    // Whether the last false answer of epochCopiesValid was a definite content mismatch, not a read failure.
    private boolean contentMismatch;

    public RecoveryCoordinator(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
    }

    /**
     * Restore the durable epoch cut for every WAL table that has a save point and, on a start outside adaptive,
     * make each such table durable and remove its save point. Must be called once, after the table registry is
     * loaded and before any WAL apply.
     */
    public void recover() {
        // The save point decides whether a table is rolled forward, never the instance mode: a table left
        // enrolled is still recovered when the instance now runs nosync, and then leaves adaptive here.
        final boolean adaptiveInstance = configuration.getCommitMode() == CommitMode.ADAPTIVE;
        final ObjHashSet<TableToken> tokens = new ObjHashSet<>();
        final ObjList<TableToken> exits = new ObjList<>();
        engine.getTableTokens(tokens, false);

        try (Path src = new Path(); Path dst = new Path(); Path dir = new Path()) {
            for (int i = 0, n = tokens.size(); i < n; i++) {
                final TableToken token = tokens.get(i);
                // Skip regular views: a view token is isWal()==true but has no _meta/_txn/_cv/data and no
                // durable epoch, and its ViewState is not yet hydrated at this point in completeInit()
                // (views compile lazily, after recover()). Mat-views (isView()==false) keep their on-disk
                // _meta and are still recovered.
                if (!token.isWal() || token.isView() || !hasAnchor(token, dir)) {
                    continue;
                }
                try {
                    failIfRecoveryDisabled(token);
                    // Before anything below reads _meta, finish the _todo_ repair of a metadata swap that a
                    // crash interrupted. TableWriter performs it on open, which comes after this pass.
                    repairInterruptedMetaSwap(token, src, dst);
                    // A table converting to non-WAL keeps its save point: the conversion replays its WAL
                    // from it first.
                    if (recoverTable(token, src, dst, dir) && !adaptiveInstance && !isConversionToNonWalPending(token, dir)) {
                        exits.add(token);
                    }
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

            // The exit, after every table is restored. The live files of a table enrolled under adaptive are
            // only as durable as its save point says; make them durable before the save point goes, since
            // nothing restores them afterwards. Where syncfs is filesystem-wide one flush covers every table;
            // elsewhere each table is flushed file by file.
            boolean isFileSystemSynced = false;
            for (int i = 0, n = exits.size(); i < n; i++) {
                final TableToken token = exits.getQuick(i);
                try {
                    if (!ff.isSyncfsFileSystemWide()) {
                        try (TableMetadata metadata = engine.getTableMetadata(token)) {
                            DurableEpochManifest.fsyncTableFiles(configuration, token, metadata.getTimestampType(), metadata.getPartitionBy());
                        }
                    } else if (!isFileSystemSynced) {
                        DatabaseCheckpointAgent.syncRestoredState(ff, dir.of(configuration.getDbRoot()), true);
                        isFileSystemSynced = true;
                    }
                    tablePath(dir, token);
                    removeAdaptiveEpochArtifacts(ff, dir, dir.size());
                    unpinEpoch(engine, token);
                    LOG.info().$("table left adaptive commit mode, its save point is removed [table=").$(token).I$();
                } catch (CairoException | CairoError e) {
                    if (CairoException.isDataSyncFailure(e)) {
                        engine.handleDataSyncFailure(e);
                    }
                    throw e;
                }
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

    private boolean hasAnchor(TableToken token, Path dir) {
        return DurableEpochManifest.exists(ff, tablePath(dir, token), dir.size());
    }

    // Whether a conversion to non-WAL waits for this table; the conversion runs after this pass.
    private boolean isConversionToNonWalPending(TableToken token, Path dir) {
        tablePath(dir, token).concat(WalUtils.CONVERT_FILE_NAME);
        try {
            return ff.exists(dir.$()) && !TableConverter.readWalEnabled(dir.$(), ff);
        } catch (CairoException e) {
            return false;
        }
    }

    private boolean isLiveTxnSavedByOtherGeneration(
            TableToken token,
            Path src,
            int rootLen,
            DurableEpochManifest.Candidate[] candidates,
            DurableEpochManifest.Candidate ahead
    ) {
        for (DurableEpochManifest.Candidate other : candidates) {
            if (other.generation != ahead.generation
                    && DurableEpochManifest.validateSections(configuration, token, src, rootLen, other.generation, other.epochSeqTxn, other.epochTxn)
                    && DurableEpochManifest.liveTxnEqualsSection(configuration, src, rootLen, other.generation)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The txn of the table's live {@code _txn}, or {@code -1} when it does not load cleanly. The txn does not
     * depend on the partitioning or the timestamp type, so this needs no metadata: it runs while {@code _meta}
     * may be missing.
     */
    private long readLiveTxn(TableToken token, Path txnPath) {
        tablePath(txnPath, token).concat(TableUtils.TXN_FILE_NAME);
        try (TxReader txReader = new TxReader(ff)) {
            txReader.ofRO(txnPath.$(), ColumnType.TIMESTAMP, PartitionBy.NONE);
            return txReader.unsafeLoadAll() ? txReader.getTxn() : -1;
        } catch (CairoException | CairoError e) {
            return -1;
        }
    }

    /**
     * Removes a generation that failed a content check of {@link #epochCopiesValid} on a definite mismatch, once
     * the older generation is restored and has passed them. The same bytes fail the same way at every start, so
     * the file can never be selected; left in place, it stays listed ahead of the live cut until the next save
     * point overwrites it, and the lineage guard refuses a start that a kill leaves inside the replay. A failed
     * removal leaves the table as it was before it.
     */
    private void removeRejectedGeneration(TableToken token, Path dir, DurableEpochManifest.Candidate rejected) {
        LOG.critical().$("removing adaptive save point that failed its content checks [table=").$(token)
                .$(", generation=").$(rejected.generation)
                .$(", epochSeqTxn=").$(rejected.epochSeqTxn)
                .$(", epochTxn=").$(rejected.epochTxn).I$();
        tablePath(dir, token);
        final int rootLen = dir.size();
        dir.concat(DurableEpochManifest.FILE_NAME).put('.').put(rejected.generation);
        if (ff.removeQuiet(dir.$())) {
            DurableEpochManifest.fsyncDirectory(configuration, dir, rootLen);
        } else {
            final int errno = ff.errno();
            LOG.error().$("could not remove rejected adaptive save point [path=").$(dir).$(", errno=").$(errno).I$();
        }
    }

    /**
     * Removes, from one partition, the value and cover files of one POSTING column version whose generation the
     * chain does not name at or below the save point's txn. Leaves everything when the chain is absent, empty or
     * does not walk cleanly. Returns the number of files removed.
     */
    private int removeUnnamedPostingGenerations(
            Path path,
            int partitionLen,
            CharSequence columnName,
            long columnNameTxn,
            long epochTxn,
            PostingIndexChainWriter chain,
            PostingIndexChainEntry.Snapshot entry,
            LongList namedSealTxns,
            LongList orphanSealTxns
    ) {
        final LPSZ keyFile = PostingIndexUtils.keyFileName(path.trimTo(partitionLen), columnName, columnNameTxn);
        final long keyFileSize = ff.length(keyFile);
        if (keyFileSize < PostingIndexUtils.KEY_FILE_RESERVED) {
            return 0;
        }
        namedSealTxns.clear();
        boolean trimNeeded = false;
        try (MemoryCMR keyMem = Vm.getCMRInstance(ff, keyFile, keyFileSize, MemoryTag.MMAP_INDEX_READER)) {
            chain.resetState();
            chain.openExisting(keyMem);
            if (!chain.hasHead() || chain.getRegionLimit() > keyFileSize) {
                return 0;
            }
            long offset = chain.getHeadEntryOffset();
            for (long visited = 0; offset != PostingIndexUtils.V2_NO_HEAD; visited++) {
                if (visited >= chain.getEntryCount() || offset < chain.getRegionBase()
                        || offset + PostingIndexUtils.V2_ENTRY_HEADER_SIZE > chain.getRegionLimit()) {
                    return 0;
                }
                PostingIndexChainEntry.read(keyMem, offset, 0, entry);
                if (entry.txnAtSeal > epochTxn) {
                    trimNeeded = true;
                } else {
                    if (namedSealTxns.size() == 0 && entry.genCount > 1) {
                        // The restored head: gens appended to it after the save point are in flight too.
                        final long lastGenTxnAtSeal = PostingIndexChainEntry.resolveGenDirOffset(offset, entry.genCount - 1, entry.coveringFormat, entry.coverCount)
                                + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL;
                        if (lastGenTxnAtSeal + Long.BYTES > chain.getRegionLimit()) {
                            return 0;
                        }
                        trimNeeded |= keyMem.getLong(lastGenTxnAtSeal) > epochTxn;
                    }
                    namedSealTxns.add(entry.sealTxn);
                }
                offset = entry.prevEntryOffset;
            }
            if (trimNeeded) {
                // The chain stops naming the entries above the save point before their files go. Otherwise its head
                // names a removed file: the O3 rollback reads it, and the purge replay's live-head check lets the
                // save point's file go. This is the walk the writer runs at open, made durable before the unlinks.
                try (MemoryCMARW keyRwMem = Vm.getCMARWInstance(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), keyFileSize, MemoryTag.MMAP_INDEX_WRITER, CairoConfiguration.O_NONE)) {
                    chain.resetState();
                    chain.openExisting(keyRwMem);
                    orphanSealTxns.clear();
                    chain.recoveryDropAbandoned(keyRwMem, epochTxn, orphanSealTxns);
                    keyRwMem.setSize(Math.max(Math.max(chain.getRegionLimit(), PostingIndexUtils.KEY_FILE_RESERVED), keyFileSize));
                    keyRwMem.sync(false);
                }
            }
        } catch (CairoException e) {
            LOG.error().$("could not check posting index chain, leaving its files [path=").$(keyFile)
                    .$(", error=").$safe(e.getFlyweightMessage()).I$();
            return 0;
        }
        final int[] removed = {0};
        PostingIndexUtils.scanSealedFiles(ff, path, partitionLen, columnName, new PostingIndexUtils.SealedFileVisitor() {
            @Override
            public void onCoverDataFile(int includeIdx, long filePostingColumnNameTxn, long coveredColumnNameTxn, long sealTxn) {
                if (filePostingColumnNameTxn == columnNameTxn && namedSealTxns.indexOf(sealTxn) < 0
                        && ff.removeQuiet(PostingIndexUtils.coverDataFileName(path.trimTo(partitionLen), columnName, includeIdx, filePostingColumnNameTxn, coveredColumnNameTxn, sealTxn))) {
                    removed[0]++;
                }
                path.trimTo(partitionLen);
            }

            @Override
            public void onValueFile(long fileColumnNameTxn, long sealTxn) {
                if (fileColumnNameTxn == columnNameTxn && namedSealTxns.indexOf(sealTxn) < 0
                        && ff.removeQuiet(PostingIndexUtils.valueFileName(path.trimTo(partitionLen), columnName, fileColumnNameTxn, sealTxn))) {
                    removed[0]++;
                }
                path.trimTo(partitionLen);
            }
        });
        path.trimTo(partitionLen);
        return removed[0];
    }

    /**
     * In every partition of the save point, removes the value and cover files of each POSTING column whose
     * generation the column's chain does not name at or below the save point's txn. A reseal after the save point
     * in a partition that no writer reopens with its chain walk (an older partition taking rows in place) leaves
     * such files, and a power loss can revert their chain record while the file stays. No reader or writer exists
     * yet, and no reader at or above the save point's txn picks an entry sealed above it. The partitions, column
     * versions and txn come from the live files, which equal the save point's sections when this runs.
     */
    private void removeUnnamedPostingGenerations(TableToken token, Path path) {
        TableReaderMetadata metadata = null;
        TxReader txReader = null;
        ColumnVersionReader cvReader = null;
        try {
            metadata = new TableReaderMetadata(configuration);
            metadata.loadMetadata(tablePath(path, token).concat(TableUtils.META_FILE_NAME).$());
            boolean hasPostingColumn = false;
            for (int c = 0, n = metadata.getColumnCount(); c < n; c++) {
                hasPostingColumn |= IndexType.isPosting(metadata.getColumnMetadata(c).getIndexType());
            }
            if (!hasPostingColumn) {
                return;
            }
            final int timestampType = metadata.getTimestampType();
            final int partitionBy = metadata.getPartitionBy();
            txReader = new TxReader(ff);
            txReader.ofRO(tablePath(path, token).concat(TableUtils.TXN_FILE_NAME).$(), timestampType, partitionBy);
            cvReader = new ColumnVersionReader();
            cvReader.ofRO(ff, tablePath(path, token).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
            if (!txReader.unsafeLoadAll() || !cvReader.readSafe()) {
                return;
            }
            final long epochTxn = txReader.getTxn();
            final PostingIndexChainWriter chain = new PostingIndexChainWriter();
            final PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();
            final LongList namedSealTxns = new LongList();
            final LongList orphanSealTxns = new LongList();
            tablePath(path, token);
            final int rootLen = path.size();
            int removed = 0;
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                final long partitionTimestamp = txReader.getPartitionTimestampByIndex(i);
                TableUtils.setPathForNativePartition(path.trimTo(rootLen), timestampType, partitionBy, partitionTimestamp, txReader.getPartitionNameTxn(i));
                final int partitionLen = path.size();
                int removedInPartition = 0;
                for (int c = 0, m = metadata.getColumnCount(); c < m; c++) {
                    if (IndexType.isPosting(metadata.getColumnMetadata(c).getIndexType())) {
                        removedInPartition += removeUnnamedPostingGenerations(
                                path,
                                partitionLen,
                                metadata.getColumnName(c),
                                cvReader.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(c)),
                                epochTxn,
                                chain,
                                entry,
                                namedSealTxns,
                                orphanSealTxns
                        );
                    }
                }
                if (removedInPartition > 0) {
                    removed += removedInPartition;
                    // Unlinks that a power loss reverts come back unnamed: a leak, never a removed live file.
                    if (!Os.isWindows()) {
                        final long dirFd = TableUtils.openRONoCache(ff, path.trimTo(partitionLen).$(), LOG);
                        if (dirFd > -1) {
                            ff.fsyncAndClose(dirFd);
                        }
                    }
                }
            }
            if (removed > 0) {
                LOG.info().$("adaptive recovery removed posting index files the save point does not name [table=").$(token)
                        .$(", epochTxn=").$(epochTxn)
                        .$(", files=").$(removed).I$();
            }
        } finally {
            tablePath(path, token);
            Misc.free(cvReader);
            Misc.free(txReader);
            Misc.free(metadata);
        }
    }

    /**
     * Finishes the {@code _todo_} repair of a {@code _meta} swap that a crash interrupted, so that this pass
     * reads the metadata the table will actually open with.
     * <p>
     * {@link TableWriter} swaps metadata by renaming {@code _meta} to {@code _meta.prev}, arming {@code _todo_}
     * with {@link TableUtils#TODO_RESTORE_META}, then renaming {@code _meta.swp} to {@code _meta}. A crash
     * between the two renames leaves no {@code _meta} at all; a crash after the second, before {@code _txn}
     * commits, leaves the uncommitted new {@code _meta} in place. The writer repairs both on open by renaming
     * {@code _meta.prev} back over {@code _meta}, but it opens only after this pass, which would otherwise refuse
     * to start on the missing file in every commit mode.
     * <p>
     * Applies the writer's own rule: the {@code _todo_} txn pair must agree and match the live {@code _txn}.
     * Anything else is a stale entry, which the writer ignores, and which must never roll {@code _meta} back.
     * {@code _todo_} stays armed: the writer then finds no {@code _meta.prev} and only clears it.
     */
    private void repairInterruptedMetaSwap(TableToken token, Path prevPath, Path metaPath) {
        tablePath(prevPath, token).concat(TableUtils.TODO_FILE_NAME);
        final long fd = ff.openRO(prevPath.$());
        if (fd == -1) {
            return;
        }
        final long todoTxn;
        final long metaPrevIndex;
        try {
            // Layout of TableWriter.writeRestoreMetaTodo(): txn at 0 and again at 24, entry count at 32, entry
            // code at 40, _meta.prev index at TODO_META_INDEX_OFFSET. A cleared _todo_ stops at the count.
            if (ff.length(fd) < TableUtils.TODO_META_INDEX_OFFSET + Long.BYTES
                    || ff.readNonNegativeLong(fd, 32) <= 0
                    || ff.readNonNegativeLong(fd, 40) != TableUtils.TODO_RESTORE_META) {
                return;
            }
            todoTxn = ff.readNonNegativeLong(fd, 0);
            metaPrevIndex = ff.readNonNegativeLong(fd, TableUtils.TODO_META_INDEX_OFFSET);
            if (todoTxn < 0 || ff.readNonNegativeLong(fd, 24) != todoTxn || metaPrevIndex < 0) {
                return;
            }
        } finally {
            ff.close(fd);
        }
        if (todoTxn != readLiveTxn(token, metaPath)) {
            return;
        }

        tablePath(prevPath, token).concat(TableUtils.META_PREV_FILE_NAME);
        if (metaPrevIndex > 0) {
            prevPath.put('.').put(metaPrevIndex);
        }
        if (!ff.exists(prevPath.$())) {
            return;
        }
        tablePath(metaPath, token).concat(TableUtils.META_FILE_NAME);
        LOG.info().$("repairing interrupted metadata swap [table=").$(token).$(", from=").$(prevPath).I$();
        ff.removeQuiet(metaPath.$());
        if (TableUtils.renamePublish(ff, prevPath.$(), metaPath.$(), configuration.getCommitMode()) != Files.FILES_RENAME_OK) {
            throw CairoException.critical(ff.errno())
                    .put("could not repair interrupted metadata swap [table=").put(token.getTableName())
                    .put(", from=").put(prevPath).put(", to=").put(metaPath).put(']');
        }
    }

    /**
     * Last durable sequencer transaction for {@code token}, or {@code -1} when it cannot be established.
     * Read on the refusal path (see the lineage classification in {@link #recoverTable}) and when the live state
     * already matches the save point, to tell whether WAL passes ran after it.
     */
    private long sequencerFrontierOrUnknown(TableToken token) {
        try {
            return engine.getTableSequencerAPI().lastTxn(token);
        } catch (CairoException | CairoError e) {
            // Unknown frontier. Do NOT guess: the caller falls through to the fail-closed refusal.
            LOG.error().$("could not read sequencer frontier while classifying an adaptive epoch [table=").$(token)
                    .$(", error=").$((Throwable) e).I$();
            return -1;
        }
    }

    /**
     * Restores the table's save point. Returns {@code false} when the table turns out to have none: only a save
     * point a crash interrupted before it was published, or one of a superseded lineage, which are removed.
     */
    private boolean recoverTable(TableToken token, Path src, Path dst, Path dir) {
        // A generation whose header was never written is a save point a crash interrupted before it was published.
        tablePath(dir, token);
        DurableEpochManifest.removeUnpublished(configuration, dir, dir.size(), token.getTableId());
        if (!hasAnchor(token, dir)) {
            return false;
        }

        DurableEpochManifest.Candidate selected = null;
        DurableEpochManifest.Candidate ahead = null;
        DurableEpochManifest.Candidate rejected = null;
        boolean restored = false;
        tablePath(dir, token);
        final DurableEpochManifest.Candidate[] candidates = DurableEpochManifest.loadCandidates(ff, dir, dir.size(), token.getTableId());
        for (int i = 0; i < candidates.length; i++) {
            final DurableEpochManifest.Candidate candidate = candidates[i];
            tablePath(src, token);
            final int rootLen = src.size();
            // The header and the section checksums first: a generation that fails them never touches the
            // live files.
            if (!DurableEpochManifest.validateSections(configuration, token, src, rootLen, candidate.generation, candidate.epochSeqTxn, candidate.epochTxn)) {
                continue;
            }
            // The lineage guard reads the live _txn, so it runs before any section is written over it.
            if (epochIsAheadOfLiveTxn(token, src, candidate)) {
                // A fallback below leaves this generation listed first and the live _txn rewound to an older
                // one. A live _txn equal to another valid generation's is that rewind: go on to it. A generation
                // of this lineage is within its sequencer; one beyond it is discarded after the loop.
                if (sequencerFrontierOrUnknown(token) >= candidate.epochSeqTxn
                        && isLiveTxnSavedByOtherGeneration(token, src, rootLen, candidates, candidate)) {
                    LOG.info().$("adaptive epoch ahead of live state is a rejected generation, skipping it [table=").$(token)
                            .$(", epochSeqTxn=").$(candidate.epochSeqTxn).I$();
                    continue;
                }
                ahead = candidate;
                break;
            }
            restored |= DurableEpochManifest.restoreLiveFiles(configuration, token, src, rootLen, candidate.generation);
            // The content checks read files, so they run on the live files, which now equal the sections.
            // A generation that fails them is a write-side bug; the other generation is restored over it.
            if (epochCopiesValid(token, src, candidate)) {
                selected = candidate;
                break;
            }
            if (contentMismatch) {
                rejected = candidate;
            }
        }

        // C2 (restore / checkpoint / PITR coexistence): a durable epoch is a PAST cut, so in one lineage the
        // live _txn is always at or ahead of it (lazy apply only advances _txn after an epoch is taken). If
        // the live _txn loads cleanly at a seqTxn BELOW this epoch, the table was rewound BENEATH a stale,
        // higher-lineage epoch by a backup / checkpoint-recover / PITR restore that left the save point
        // behind (nothing on the restore path clears them). Rolling _txn forward to that epoch would
        // resurrect the discarded lineage and leave _txn ahead of the restored sequencer. REFUSE: the epoch
        // cannot be adopted, and the live state below it may be the result of writes the storage lost, so
        // this fails the startup closed rather than serving a table nobody can vouch for. A TORN/unreadable
        // live _txn (the genuine post-crash state this mechanism exists to repair) does NOT trip the guard —
        // see epochIsAheadOfLiveTxn.
        if (ahead != null) {
            final long epochSeqTxn = ahead.epochSeqTxn;
            // Classify before refusing. An epoch is always taken at a cut that is ALREADY in the durable
            // sequencer log, so epochSeqTxn <= sequencer lastTxn holds for every SAME-lineage epoch. An epoch
            // BEYOND the sequencer frontier therefore cannot belong to this lineage at all: the sequencer was
            // re-seeded wholesale (ALTER TABLE ... SET TYPE WAL re-registers it from txn 0, REBASE WAL builds
            // a new one, an out-of-process restore may drop a fresh txn_seq in place), which makes the anchor
            // unreachable rather than evidence of lost writes — no replay can ever produce the cut it names.
            // Discard it instead of refusing to start; the table's next writer enrols it at the live cut. This is
            // not a relaxation of the refusal above: the live _txn here loaded CLEAN (epochIsAheadOfLiveTxn returns
            // true only then), so the table has a self-consistent cut, and WAL replay from the sequencer frontier
            // is exactly what the new lineage needs.
            final long frontier = sequencerFrontierOrUnknown(token);
            if (frontier >= 0 && epochSeqTxn > frontier) {
                LOG.critical().$("adaptive epoch belongs to a superseded WAL lineage, discarding anchor [table=").$(token)
                        .$(", epochSeqTxn=").$(epochSeqTxn)
                        .$(", seqLastTxn=").$(frontier).I$();
                tablePath(dir, token);
                removeAdaptiveEpochArtifacts(ff, dir, dir.size());
                return false;
            }
            throw CairoException.critical(0)
                    .put("adaptive epoch post-dates live state; refusing wrong-lineage recovery [table=")
                    .put(token.getTableName()).put(", dir=").put(token.getDirName()).put(", epochSeqTxn=").put(epochSeqTxn)
                    .put("]; move ").put(DurableEpochManifest.FILE_NAME).put('.').put(ahead.generation)
                    .put(" out of the table directory and start again");
        }
        if (selected == null) {
            final CairoException e = CairoException.critical(0)
                    .put("no trustworthy adaptive epoch generation; refusing unsafe live-state fallback [table=")
                    .put(token.getTableName()).put(", dir=").put(token.getDirName()).put(']');
            // A generation restored before its content checks failed has replaced the live files, so moving the
            // save point out would start the table from that generation.
            if (restored) {
                throw e.put("; recovery has rewritten the live table files, restore the table from a backup or a checkpoint");
            }
            throw e.put("; after a clean stop or a process kill, move ").put(DurableEpochManifest.FILE_NAME).put(".0 and ")
                    .put(DurableEpochManifest.FILE_NAME).put(".1 out of the table directory and start again;")
                    .put(" after a power loss, restore the table from a backup or a checkpoint");
        }
        if (rejected != null) {
            removeRejectedGeneration(token, dir, rejected);
        }
        final long epochSeqTxn = selected.epochSeqTxn;

        // SYMMETRIC C2 (review Finding C2, second half). The guard above rejects an epoch AHEAD of the live
        // _txn. The reverse — a stale epoch BELOW a divergently-restored live _txn — is NOT (and cannot cheaply
        // be) rejected here: seqTxn carries no lineage tag, so a below-_txn epoch is indistinguishable from the
        // NORMAL same-lineage past cut that recover() is meant to adopt (rewind _txn/_cv to the epoch, then
        // WAL-replay forward). Its safety rests entirely on the INVARIANT that every path which rewinds or
        // diverges the live _txn (a backup / checkpoint / PITR restore; a primary->replica demote) clears
        // the save point via TableSnapshotRestore -> RecoveryCoordinator.removeAdaptiveEpochArtifacts. So any
        // epoch that SURVIVES to this adoption point is necessarily the SAME lineage as the live _txn (a
        // legitimate fast-boot anchor), never a stale wrong-lineage one.

        // A clean shutdown leaves nothing to restore: the writer's close epoch saves the live files as they
        // stand, and nothing writes them after it. The comparison is exact, so any difference, a torn live file
        // included, took the restore above, and only that counts as a recovery.
        if (!restored) {
            // A power loss usually reverts _txn and _cv to the save point, so nothing is restored, but the passes
            // applied after it can have left index generations behind. Those passes are in the WAL, after the save
            // point; after a clean stop with everything applied the WAL holds nothing past it.
            if (sequencerFrontierOrUnknown(token) > epochSeqTxn) {
                removeUnnamedPostingGenerations(token, src);
            }
            pinRecoveredEpoch(token, selected.epochTxn, epochSeqTxn);
            LOG.info().$("adaptive epoch matches live state, nothing to restore [table=").$(token)
                    .$(", epochSeqTxn=").$(epochSeqTxn).I$();
            return true;
        }

        // Reads the live files, which now equal the save point. A crash before it is covered by the branch above
        // on the next start: index generations left behind come from passes after the save point.
        removeUnnamedPostingGenerations(token, src);
        // The pooled/catalogue metadata may hold the pre-recovery schema. A real startup has no such tenant;
        // evict it before WAL replay.
        engine.resetTableMetadataForRecovery(token);

        LOG.info().$("adaptive epoch roll-forward restored durable cut [table=").$(token)
                .$(", epochSeqTxn=").$(epochSeqTxn).I$();

        // Bump the in-memory recovery incarnation counter ONLY on a successful validated restore
        // (not on no-op/skip/absent-marker/torn-copy paths). The tracker is initialised lazily on
        // first WAL apply, but the object itself always exists (getSeqTxnTracker creates it).
        final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        pinRecoveredEpoch(token, selected.epochTxn, epochSeqTxn);
        tracker.bumpRecoveryIncarnation();
        engine.getMetrics().walMetrics().incrementRecoveryEvents();
        return true;
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
            // Record the pin while this handle still keeps the scoreboard pooled. Once it closes, nothing may
            // reference the board, idle eviction can free it, and the pool seeds the replacement from this
            // record (SeqTxnTracker.restoreEpochPin). Recording it after the close would let a replacement
            // come up with the prior pin, or none, and leave the recovered cut unprotected.
            tracker.setPinnedEpoch(epochTxn, slotA);
        }
        tracker.setDurableEpochSeqTxn(epochSeqTxn);
    }

    /**
     * C1 guard: the content checks of a restored save point, run on the live files once they equal the
     * generation's sections ({@link TxReader} and {@link ColumnVersionReader} read only from a file). Returns
     * {@code true} only if {@code _meta} loads with this table's id, {@code _txn} and {@code _cv} fully load
     * via the same A/B-checksummed readers the engine uses ({@link TxReader#unsafeLoadAll()} /
     * {@link ColumnVersionReader#readSafe()}), {@code _txn} names the marker's cut, its symbol count matches
     * {@code _meta}, {@code _cv} is at the column version {@code _txn} names, and the save point still
     * validates against those versions.
     * <p>
     * Any failure to decode is treated as invalid and returns {@code false}: a short file, a corrupt body, a
     * SIGBUS reading past a truncated mmap, or a cut other than the marker's. Recovery then restores the
     * previous marker generation; if no generation validates, startup fails closed. A definite mismatch (table id,
     * cut, symbol count, versions) also sets {@link #contentMismatch}, and recovery removes this generation once
     * the previous one is restored.
     */
    private boolean epochCopiesValid(TableToken token, Path scratch, DurableEpochManifest.Candidate candidate) {
        contentMismatch = false;
        final long markerEpochSeqTxn = candidate.epochSeqTxn;
        final int epochGeneration = candidate.generation;
        int partitionBy;
        int timestampType;
        long metadataVersion;
        TableReaderMetadata epochMetadata = null;
        try {
            tablePath(scratch, token).concat(TableUtils.META_FILE_NAME);
            epochMetadata = new TableReaderMetadata(configuration);
            epochMetadata.loadMetadata(scratch.$());
            if (epochMetadata.getTableId() != token.getTableId()) {
                Misc.free(epochMetadata);
                contentMismatch = true;
                return false;
            }
            partitionBy = epochMetadata.getPartitionBy();
            timestampType = epochMetadata.getTimestampType();
            metadataVersion = epochMetadata.getMetadataVersion();
        } catch (CairoException | CairoError e) {
            // Cannot interpret the candidate without its matching metadata; mark it invalid. If no other
            // candidate validates, recoverTable() aborts startup instead of spinning on a permanent mismatch.
            LOG.error().$("adaptive epoch validation could not read epoch metadata [table=")
                    .$(token).$(", error=").$safe(e.getFlyweightMessage()).I$();
            Misc.free(epochMetadata);
            return false;
        }

        // Validate _txn: a clean A/B-checksummed load whose seqTxn equals the marker's epoch cut.
        TxReader txReader = null;
        ColumnVersionReader cvReader = null;
        try {
            tablePath(scratch, token).concat(TableUtils.TXN_FILE_NAME);
            txReader = new TxReader(ff);
            txReader.ofRO(scratch.$(), timestampType, partitionBy);
            if (!txReader.unsafeLoadAll()) {
                return false; // torn _txn (one slot bad; the other absent/old)
            }
            final long copySeqTxn = txReader.getSeqTxn();
            if (copySeqTxn != markerEpochSeqTxn
                    || txReader.getTxn() != candidate.epochTxn
                    || txReader.getMetadataVersion() != metadataVersion) {
                LOG.error().$("adaptive epoch _txn identity does not match its save point header [table=").$(token)
                        .$(", copySeqTxn=").$(copySeqTxn).$(", markerSeqTxn=").$(markerEpochSeqTxn)
                        .$(", copyTxn=").$(txReader.getTxn()).$(", markerTxn=").$(candidate.epochTxn).I$();
                contentMismatch = true;
                return false;
            }

            // Cross-check the two copies against EACH OTHER. Integrity is already covered -- the A/B
            // checksums prove each payload is intact, and DurableEpochManifest.validate() proves each is the
            // byte-for-byte file this generation published. Neither says the payloads describe the same table
            // SHAPE, because a checksum cannot tell a faithfully-recorded skew from a faithfully-recorded
            // consistent cut. Only a semantic comparison can.
            //
            // _txn's symbol area is written from denseSymbolMapWriters and _meta carries the live symbol
            // columns, so these two counts agreeing is exactly that comparison. Equal metadataVersions do not
            // imply it: TableWriter.changeColumnType moves the symbol count on a commit that does not bump the
            // version, so a cut taken inside its structural window records both copies at the SAME version
            // while they disagree by one. (That write-side window is closed in advanceDurableEpoch; this is
            // the read-side backstop, and it is what stands between any future variant and an unopenable
            // table.) Restoring a mismatched pair rewinds the table into a state nothing can open -- symbol
            // rollback indexes past the writer list, then WAL apply rejects the table outright and it stays
            // suspended while the sequencer runs ahead. Reject, exactly as for a torn payload: the marker's
            // other generation is restored over it, and failing that recovery aborts startup.
            if (epochMetadata != null) {
                int metaSymbolColumns = 0;
                for (int c = 0, n = epochMetadata.getColumnCount(); c < n; c++) {
                    final int type = epochMetadata.getColumnType(c);
                    if (type > -1 && ColumnType.isSymbol(type)) {
                        metaSymbolColumns++;
                    }
                }
                if (txReader.getSymbolColumnCount() != metaSymbolColumns) {
                    LOG.error().$("adaptive epoch _txn and _meta disagree on symbol columns [table=").$(token)
                            .$(", generation=").$(epochGeneration)
                            .$(", txnSymbolColumns=").$(txReader.getSymbolColumnCount())
                            .$(", metaSymbolColumns=").$(metaSymbolColumns).I$();
                    contentMismatch = true;
                    return false;
                }
            }

            // Validate _cv: a clean A/B-checksummed load (readSafe verifies the live area's checksum and only
            // adopts a self-consistent record).
            tablePath(scratch, token).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            cvReader = new ColumnVersionReader();
            cvReader.ofRO(ff, scratch.$());
            if (!cvReader.readSafe()) {
                return false;
            }
            if (txReader.getColumnVersion() != cvReader.getVersion()) {
                contentMismatch = true;
                return false;
            }
            tablePath(scratch, token);
            final int rootLen = scratch.size();
            if (DurableEpochManifest.validate(
                    configuration,
                    token,
                    scratch,
                    rootLen,
                    epochGeneration,
                    markerEpochSeqTxn,
                    candidate.epochTxn,
                    txReader.getColumnVersion(),
                    metadataVersion
            )) {
                return true;
            }
            // The sections passed before the restore, so this is the header's versions, or a read failure,
            // which the read below throws.
            contentMismatch = !DurableEpochManifest.recordsVersions(configuration, scratch, rootLen, epochGeneration, txReader.getColumnVersion(), metadataVersion);
            return false;
        } catch (Throwable e) {
            // FAIL SAFE: ANY failure while decoding a torn copy means "invalid -> skip restore", never
            // "fall through and blindly copy it over the live file". Concrete failure modes seen on a torn
            // copy: CairoException (fileNotFound on a 0-byte _txn.epoch; both-A/B checksum mismatch), an
            // mmap bounds AssertionError or SIGBUS-as-InternalError from a garbage base header steering an
            // out-of-bounds read, CairoError. We deliberately catch broadly here because the cost of a
            // false "invalid" is a fall-back to the older generation, restored over the live files, and a
            // longer WAL replay, whereas the cost of letting it through would be the brick this guard exists
            // to prevent. Such a failure is never a content mismatch, so the generation's file stays. The
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
     * Restore/PITR freshness guard. Returns {@code true} when the durable epoch is ahead of the live
     * {@code _txn}: the live {@code _txn} loads cleanly from its latest A/B slot, and its {@code seqTxn} is
     * below {@code epochSeqTxn}. On a single lineage that cannot happen, because an epoch is a past cut and
     * lazy apply only advances {@code _txn} after it. It means a backup, checkpoint-recover or PITR restore
     * rewound the table beneath a stale, higher-lineage epoch and did not clear its save point.
     * <p>
     * The guard fails open. It returns {@code false} for any live {@code _txn} that does not load cleanly
     * from its latest slot, which lets recovery roll forward to the epoch. A torn, short or garbage
     * {@code _txn} is the post-crash state recovery exists to repair. That includes a {@code _txn} whose
     * latest slot is torn and which {@code unsafeLoadAll()} serves from the predecessor slot. On a single
     * lineage the predecessor slot can predate the epoch, so its {@code seqTxn} is no evidence of a restore.
     * <p>
     * The caller decides what a {@code true} answer means, by comparing the epoch with the sequencer
     * frontier: an epoch beyond it belongs to a re-seeded lineage and is discarded, and anything else fails
     * the startup closed. This method itself makes no such distinction.
     */
    private boolean epochIsAheadOfLiveTxn(TableToken token, Path scratch, DurableEpochManifest.Candidate candidate) {
        final long epochSeqTxn = candidate.epochSeqTxn;
        TxReader liveTxn = null;
        try {
            tablePath(scratch, token).concat(TableUtils.TXN_FILE_NAME);
            liveTxn = new TxReader(ff);
            // Neither the seqTxn nor the area geometry depends on the partitioning or the timestamp type, so
            // this needs no metadata, as in readLiveTxn.
            liveTxn.ofRO(scratch.$(), ColumnType.TIMESTAMP, PartitionBy.NONE);
            if (!liveTxn.unsafeLoadAll()) {
                // Torn / short / garbage live _txn = the genuine post-crash case this mechanism exists to
                // repair -> NOT "ahead" -> allow the roll-forward.
                return false;
            }

            // Invariant pin (review Finding C2). unsafeLoadAll() returns the record in the latest A/B slot, which
            // the version word selects. Only when that slot is torn does it fall back to the predecessor slot
            // (version - 1), and it never returns an older slot. So versionWord - getVersion() is 0 for the
            // latest slot and 1 for the predecessor slot. The assert pins this, so an A/B refactor cannot
            // silently change which record the guard judges. The property holds on any lineage. Recovery runs
            // with no concurrent writer, so the version word on disk cannot change during this check.
            final long loadedVersion = liveTxn.getVersion();
            final long versionWord = liveTxn.unsafeReadVersion();
            assert versionWord - loadedVersion >= 0 && versionWord - loadedVersion <= 1
                    : "TxReader A/B slot-selection regression under adaptive recovery: unsafeLoadAll must "
                    + "return the version-selected (latest) _txn slot, or its immediate predecessor when the "
                    + "latest is torn — never an older slot [table=" + token.getTableName()
                    + ", loadedVersion=" + loadedVersion + ", versionWord=" + versionWord + ']';

            // A load from the predecessor slot means a crash, not a restore, even when it is below the epoch.
            // ADAPTIVE never msyncs _txn between epochs, and the kernel writes dirty mmap pages back in no fixed
            // order. A power loss can therefore persist a later page of commit V_E+2, which reuses the A/B slot
            // of the epoch record V_E, while page 0 still holds version word V_E. The latest slot is then torn,
            // and the predecessor slot holds V_E - 1, below the epoch. A restore leaves a self-consistent _txn
            // that loads from its latest slot, so returning false here does not hide a restore from the guard.
            if (versionWord != loadedVersion) {
                return false;
            }

            // On a single lineage, a crash cannot leave the latest slot below the epoch:
            // - fsyncMaterializedState() fsyncs the live _txn at version V_E and seqTxn epochSeqTxn before
            //   the save point copies it, and the version word only grows after that;
            // - a checksum covers the record in the latest slot;
            // - seqTxn grows with version within a lineage.
            // A latest slot below the epoch therefore means a restore or PITR rewound the live _txn beneath a
            // leftover, higher-lineage epoch. The caller decides what to do about it.
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

    private Path tablePath(Path p, TableToken token) {
        return p.of(configuration.getDbRoot()).concat(token);
    }

    /**
     * Fail-closed removal of the adaptive durable-epoch anchor for ONE table — the save point files
     * {@code _epoch.manifest.0} and {@code _epoch.manifest.1}, the older one first — given
     * {@code path} positioned at the table root and {@code tableRootLen} = the length of that table-root prefix.
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
     * after deletion is a hard restore failure. Only the save point files are removed; the LIVE
     * {@code _txn}/{@code _cv} are never touched. Leaves {@code path} trimmed back to the table root.
     */
    public static void removeAdaptiveEpochArtifacts(FilesFacade ff, Path path, int tableRootLen) {
        final DurableEpochManifest.Candidate[] saved = DurableEpochManifest.loadCandidates(ff, path, tableRootLen, -1);
        for (int generation = 0; generation < 2; generation++) {
            if (saved.length < 1 || (saved[0].generation != generation && (saved.length < 2 || saved[1].generation != generation))) {
                // Never published, or damaged: recovery selects neither.
                removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(DurableEpochManifest.FILE_NAME).put('.').put(generation));
            }
        }
        // The older save point goes first, and its removal is durable before the newer one goes: a crash in between
        // must leave the newer one, never the older alone, whose WAL the purge floor may have released.
        for (int i = saved.length - 1; i > -1; i--) {
            removeAdaptiveEpochArtifactOrFail(ff, path.trimTo(tableRootLen).concat(DurableEpochManifest.FILE_NAME).put('.').put(saved[i].generation));
            DurableEpochManifest.fsyncDirectory(ff, path, tableRootLen);
        }
        path.trimTo(tableRootLen);
    }

    // Releases the pin of the save point recovery restored, once the save point is removed.
    static void unpinEpoch(CairoEngine engine, TableToken token) {
        final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        final long pinnedTxn = tracker.getPinnedEpochTxn();
        if (pinnedTxn < 0) {
            return;
        }
        try (TxnScoreboard scoreboard = engine.getTxnScoreboard(token)) {
            scoreboard.releaseTxn(tracker.isPinnedEpochSlotA() ? TxnScoreboard.EPOCH_ID_A : TxnScoreboard.EPOCH_ID_B, pinnedTxn);
            // While this handle keeps the scoreboard pooled, as pinRecoveredEpoch records the pin.
            tracker.clearPinnedEpoch();
        }
    }

    private static void removeAdaptiveEpochArtifactOrFail(FilesFacade ff, Path path) {
        if (ff.exists(path.$()) && !ff.removeQuiet(path.$()) && ff.exists(path.$())) {
            throw CairoException.critical(ff.errno())
                    .put("could not remove stale adaptive epoch artifact [path=").put(path).put(']');
        }
    }
}
