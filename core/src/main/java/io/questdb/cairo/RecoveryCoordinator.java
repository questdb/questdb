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
 * Restores each WAL table that has a save point to it, once at engine startup, after the table registry loads
 * and before any WAL apply ({@code CheckWalTransactionsJob} -> {@code ApplyWal2TableJob}). A save point is a file
 * {@code _epoch.manifest.N} holding copies of {@code _meta}, {@code _txn} and {@code _cv}. Of the valid
 * generations, recovery selects the one with the higher cut and writes each copy over its live file where the
 * two differ, syncing the files and the table directory, so the table opens at the save point's {@code seqTxn}
 * and {@code txn}. WAL apply then replays {@code (seqTxn, frontier]} from the durable WAL. A table whose live
 * files equal the copies, which a clean shutdown leaves, has nothing to restore: recovery only pins its save
 * point and does not count a recovery.
 * <p>
 * Adaptive applies lazily: it does not sync partition columns between save points (see
 * {@link CommitMode#appliesColumnSync}), so after a power loss the live state can be torn past the last save
 * point. Rewinding {@code _txn} and {@code _cv} to the save point and replaying the WAL rebuilds the lost rows.
 *
 * <h2>Why this is safe and idempotent</h2>
 * <ul>
 *   <li><b>Restore before rely:</b> the restored files and the table directory are synced before recovery
 *       returns, so a crash right after it sees the restored cut, not a half-copied file.</li>
 *   <li><b>Idempotent:</b> the writer writes a new save point into the other generation, so the selected one
 *       does not change before the next save point. Recovery run again after a crash copies the same bytes and
 *       lands on the same cut, and WAL apply continues from the {@code seqTxn} the restored {@code _txn}
 *       names.</li>
 *   <li><b>A save point decides:</b> a table is enrolled exactly when it has a save point file. Its writer
 *       publishes one before it applies anything lazily, and every path that resets its lineage removes it.
 *       Any other table, including every table a non-adaptive instance or an older build wrote, costs two
 *       existence checks and is left to the normal open and WAL apply. Non-WAL tables are never touched.</li>
 *   <li><b>The exit:</b> a start outside adaptive makes every enrolled table durable, removes its save point
 *       (the older generation first) and releases its pin, so no save point outlives adaptive.</li>
 * </ul>
 */
public class RecoveryCoordinator {
    private static final Log LOG = LogFactory.getLog(RecoveryCoordinator.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    // Whether the last false answer of areEpochCopiesValid was a definite content mismatch, not a read failure.
    private boolean contentMismatch;
    // Whether a selected save point header was rewritten without its own sync; recover() flushes them all at its end.
    private boolean isHeaderFlushPending;

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
        // Boot runs before a role is installed; recovery needs the configured grade.
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
                    if (recoverTable(token, src, dir) && !adaptiveInstance && !isConversionToNonWalPending(token, dir)) {
                        exits.add(token);
                    }
                } catch (CairoException | CairoError e) {
                    // A writeback failure poisons the process-wide page-cache durability state. Do not
                    // serve siblings or retry recovery in the same process.
                    engine.handleDataSyncFailure(e);
                    // An adaptive table may not be exposed from its possibly non-durable live state.
                    // Abort engine initialization instead of merely suspending WAL apply (readers are not
                    // fenced by sequencer suspension).
                    throw e;
                }
            }

            // One flush for every header rewritten above, before recover() returns: nothing trusts them before then.
            if (isHeaderFlushPending) {
                try {
                    DatabaseCheckpointAgent.syncRestoredState(ff, dir.of(configuration.getDbRoot()), true);
                } catch (CairoException | CairoError e) {
                    engine.handleDataSyncFailure(e);
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
                    engine.handleDataSyncFailure(e);
                    throw e;
                }
            }
        }
    }

    private void failIfRecoveryDisabled(TableToken token) {
        if (!configuration.isAdaptiveRecoveryRollForwardEnabled()) {
            // Skipping recovery can expose a lazily materialized, torn table, so turning recovery off refuses
            // the start instead of reading the live files.
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
     * Removes a generation that failed a content check of {@link #areEpochCopiesValid} on a definite mismatch, once
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
     * such files, and a power loss can lose their chain record while the file stays. No reader or writer exists
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
                    // An unlink that a power loss undoes brings back an unnamed file: a leak, never a removed
                    // live file.
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
     * Finishes the {@code _todo_} repair of a {@code _meta} swap that a crash interrupted, so that this pass reads
     * the metadata the table opens with. {@link TableWriter} renames {@code _meta} to {@code _meta.prev}, arms
     * {@code _todo_} with {@link TableUtils#TODO_RESTORE_META}, then renames {@code _meta.swp} to {@code _meta}.
     * A crash between the renames leaves no {@code _meta}; a crash after the second, before {@code _txn}
     * commits, leaves an uncommitted {@code _meta}. The writer repairs both on open by renaming {@code _meta.prev}
     * back, but it opens after this pass, which would otherwise refuse to start on the missing file.
     * <p>
     * Applies the writer's rule: the {@code _todo_} txn pair must agree and match the live {@code _txn}; any other
     * entry is stale and never rolls {@code _meta} back. {@code _todo_} stays armed: the writer then finds no
     * {@code _meta.prev} and only clears it.
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
        // Boot runs before a role is installed; recovery needs the configured grade.
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
            // Unknown frontier: rather than guess, the caller falls through to the fail-closed refusal.
            LOG.error().$("could not read sequencer frontier while classifying an adaptive epoch [table=").$(token)
                    .$(", error=").$((Throwable) e).I$();
            return -1;
        }
    }

    /**
     * Restores the table's save point. Returns {@code false} when the table turns out to have none: only a save
     * point a crash interrupted before it was published, or one of a superseded lineage, which are removed.
     */
    private boolean recoverTable(TableToken token, Path src, Path dir) {
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
                LOG.critical().$("adaptive save point failed its checksums, trying the other generation [table=").$(token)
                        .$(", generation=").$(candidate.generation).$(", epochSeqTxn=").$(candidate.epochSeqTxn).I$();
                continue;
            }
            // The lineage guard reads the live _txn, so it runs before any section is written over it.
            if (isEpochAheadOfLiveTxn(token, src, candidate)) {
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
            // The content checks read files, so they run on the live files once they equal the sections.
            // A generation that fails them is a write-side bug; the other generation is restored over it.
            if (areEpochCopiesValid(token, src, candidate)) {
                selected = candidate;
                break;
            }
            if (contentMismatch) {
                rejected = candidate;
            }
        }

        // A save point is a past cut, so within one lineage the live _txn is at or ahead of it. A live _txn
        // that loads cleanly below it means a backup, checkpoint or PITR restore rewound the table under a save
        // point it left behind. Rolling forward to it would resurrect the discarded lineage, and the live state
        // below it may come from writes the storage lost, so the start fails closed. A torn live _txn, the
        // post-crash state recovery repairs, does not trip the guard (see isEpochAheadOfLiveTxn).
        if (ahead != null) {
            final long epochSeqTxn = ahead.epochSeqTxn;
            // Classify before refusing. A save point is taken at a cut already in the durable sequencer log, so
            // one of this lineage is at or below the sequencer frontier. One beyond it belongs to a re-seeded
            // sequencer (SET TYPE WAL re-registers it from txn 0, REBASE WAL builds a new one, an out-of-process
            // restore may put a fresh txn_seq in place): no replay reaches its cut, and it is no evidence of lost
            // writes. Discard it; the table's next writer enrols it at the live cut. The live _txn loaded cleanly
            // here, so the table has a self-consistent cut to replay the new lineage from.
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
        // The selected save point may be only in the page cache; the purges and the next save point trust it once
        // recover() returns. Where syncfs is filesystem-wide, recover() flushes every rewritten header at once.
        // Boot runs before a role is installed; recovery needs the configured grade.
        if (configuration.getCommitMode() == CommitMode.ADAPTIVE) {
            tablePath(dir, token);
            final boolean isFlushedByRecover = ff.isSyncfsFileSystemWide();
            DurableEpochManifest.rewriteHeader(ff, dir, dir.size(), selected.generation, !isFlushedByRecover);
            isHeaderFlushPending |= isFlushedByRecover;
        }
        if (rejected != null) {
            removeRejectedGeneration(token, dir, rejected);
        }
        final long epochSeqTxn = selected.epochSeqTxn;
        // The other generation stays pinned too: a later recovery falls back to it if the selected one is damaged.
        long otherEpochTxn = -1;
        for (int i = 0; i < candidates.length; i++) {
            if (candidates[i] != selected && candidates[i] != rejected) {
                otherEpochTxn = candidates[i].epochTxn;
            }
        }

        // The lineage guard rejects a save point ahead of the live _txn, not one below a live _txn that a
        // restore made diverge: seqTxn carries no lineage tag, so that save point looks like the past cut of
        // the same lineage that recovery adopts. This relies on every path that rewinds or diverges the live
        // _txn (a backup, checkpoint or PITR restore, a primary-to-replica demote) removing the save point
        // through removeAdaptiveEpochArtifacts, so a save point that reaches this point is of the live lineage.

        // A clean shutdown leaves nothing to restore: the writer's close epoch saves the live files as they
        // stand, and nothing writes them after it. The comparison is exact, so any difference, a torn live file
        // included, took the restore above, and only that counts as a recovery.
        if (!restored) {
            // A power loss usually rolls _txn and _cv back to the save point, so nothing is restored, but the passes
            // applied after it can have left index generations behind. Those passes are in the WAL, after the save
            // point; after a clean stop with everything applied the WAL holds nothing past it.
            if (sequencerFrontierOrUnknown(token) > epochSeqTxn) {
                removeUnnamedPostingGenerations(token, src);
            }
            pinRecoveredEpoch(token, selected.epochTxn, epochSeqTxn, otherEpochTxn, selected.generation);
            LOG.info().$("adaptive epoch matches live state, nothing to restore [table=").$(token)
                    .$(", epochSeqTxn=").$(epochSeqTxn).I$();
            return true;
        }

        // Reads the live files, which equal the save point from here on. A crash before it is covered by the
        // branch above on the next start: index generations left behind come from passes after the save point.
        removeUnnamedPostingGenerations(token, src);
        // When recovery runs in a live engine (a checkpoint recovery), pooled metadata may hold the schema from
        // before the restore; evict it before WAL replay.
        engine.resetTableMetadataForRecovery(token);

        LOG.info().$("adaptive epoch roll-forward restored durable cut [table=").$(token)
                .$(", epochSeqTxn=").$(epochSeqTxn).I$();

        // Only a validated restore bumps the in-memory recovery incarnation counter; every other path returns
        // above. The tracker initialises lazily on the first WAL apply, but getTxnTracker always returns one.
        final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        pinRecoveredEpoch(token, selected.epochTxn, epochSeqTxn, otherEpochTxn, selected.generation);
        tracker.bumpRecoveryIncarnation();
        engine.getMetrics().walMetrics().incrementRecoveryEvents();
        return true;
    }

    // Pins the save point recovery selected, or a baseline published outside a writer, and records `generation`,
    // the one the table now stands on, so that the writer's next save point goes into the other one.
    private void pinRecoveredEpoch(TableToken token, long epochTxn, long epochSeqTxn, long otherEpochTxn, int generation) {
        final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        final long priorEpochTxn = tracker.getPinnedEpochTxn();
        final long priorOtherEpochTxn = tracker.getOtherPinnedEpochTxn();
        if (priorEpochTxn == epochTxn && priorOtherEpochTxn == otherEpochTxn) {
            // The pins stand; the generation may not: a recovery in a live engine can select the other generation
            // of a tie (both at one txn), and the next save point must spare the one it selected.
            if (tracker.getPinnedEpochGeneration() != generation) {
                tracker.setPinnedEpoch(epochTxn, tracker.isPinnedEpochSlotA(), otherEpochTxn, generation);
            }
            tracker.setDurableEpochSeqTxn(epochSeqTxn);
            return;
        }
        final boolean priorSlotA = tracker.isPinnedEpochSlotA();
        // Runtime checkpoint recovery can replace a live engine's existing epoch pin. Reserve the other
        // ping-pong slot first, then release the old slot only after the new cut is protected.
        boolean slotA = priorEpochTxn < 0 || !priorSlotA;
        try (TxnScoreboard scoreboard = engine.getTxnScoreboard(token)) {
            // A prior other pin holds the slot the recovered cut takes.
            if (priorOtherEpochTxn >= 0) {
                scoreboard.releaseTxn(priorSlotA ? TxnScoreboard.EPOCH_ID_B : TxnScoreboard.EPOCH_ID_A, priorOtherEpochTxn);
            }
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
            // The other generation, which a later recovery falls back to if the selected one is damaged.
            long pinnedOtherEpochTxn = otherEpochTxn;
            if (otherEpochTxn >= 0 && !scoreboard.incrementTxn(slotA ? TxnScoreboard.EPOCH_ID_B : TxnScoreboard.EPOCH_ID_A, otherEpochTxn)) {
                LOG.critical().$("could not pin the other adaptive save point, a fallback to it may miss partition versions [table=")
                        .$(token).$(", epochTxn=").$(otherEpochTxn).I$();
                pinnedOtherEpochTxn = -1;
            }
            // Record the pin while this handle still keeps the scoreboard pooled. Once it closes, nothing may
            // reference the board, idle eviction can free it, and the pool seeds the replacement from this
            // record (SeqTxnTracker.restoreEpochPin). Recording it after the close would let a replacement
            // come up with the prior pin, or none, and leave the recovered cut unprotected.
            tracker.setPinnedEpoch(epochTxn, slotA, pinnedOtherEpochTxn, generation);
        }
        tracker.setDurableEpochSeqTxn(epochSeqTxn);
    }

    /**
     * The content checks of a restored save point, run on the live files once they equal the generation's
     * sections ({@link TxReader} and {@link ColumnVersionReader} read only from a file). Returns {@code true} only
     * if {@code _meta} loads with this table's id, {@code _txn} and {@code _cv} fully load through the engine's
     * A/B-checksummed readers ({@link TxReader#unsafeLoadAll()}, {@link ColumnVersionReader#readSafe()}),
     * {@code _txn} names the header's cut, its symbol count matches {@code _meta}, {@code _cv} is at the column
     * version {@code _txn} names, and the save point still validates against those versions.
     * <p>
     * Any failure to decode the live files returns {@code false}: a short file, a corrupt body, a read past a
     * truncated mapping. Recovery then restores the other generation; if none validates, the start fails closed. A
     * definite mismatch (table id, cut, symbol count, versions) also sets {@link #contentMismatch}, and recovery
     * removes this generation once the other one is restored. A read error of the save point file itself throws:
     * its sections passed their checksums before the restore, so it is a fault, never a verdict.
     */
    private boolean areEpochCopiesValid(TableToken token, Path scratch, DurableEpochManifest.Candidate candidate) {
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

        // Validate _txn: a clean A/B-checksummed load at the cut the header names.
        TxReader txReader = null;
        ColumnVersionReader cvReader = null;
        final long columnVersion;
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

            // Cross-check _txn against _meta. The checksums prove each section intact and the file this
            // generation saved, not that the two describe the same table shape. _txn's symbol area is written
            // from denseSymbolMapWriters and _meta carries the live symbol columns, so the two counts must agree.
            // Equal metadata versions do not imply it: TableWriter.changeColumnType moves the symbol count on a
            // commit that keeps the version. The writer takes no save point in that window (advanceDurableEpoch);
            // this is the read-side backstop. A mismatched pair leaves a table neither the writer nor WAL apply
            // can open, so it is rejected like a torn one: the other generation is restored over it, and failing
            // that recovery stops the start.
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
            columnVersion = txReader.getColumnVersion();
        } catch (Throwable e) {
            // Fail safe: any failure to decode the restored files means invalid. A torn file can throw a
            // CairoException (a short file, both A/B checksums wrong), a CairoError, a mapping bounds
            // AssertionError or a SIGBUS surfaced as an InternalError. The catch is broad because a false invalid
            // costs a fallback to the other generation and a longer WAL replay, while a false valid opens a table
            // nothing can read. Such a failure is never a content mismatch, so the generation's file stays.
            LOG.error().$("adaptive epoch durable copy failed validation, skipping roll-forward [table=").$(token)
                    .$(", error=").$safe(String.valueOf(e.getMessage())).I$();
            return false;
        } finally {
            Misc.free(cvReader);
            Misc.free(txReader);
            Misc.free(epochMetadata);
        }
        // The save point file, outside the catch above: it cannot SIGBUS under recovery and its sections passed
        // their checksums, so a failure to read it is a fault that fails the start, not a generation that failed
        // its content checks.
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
                columnVersion,
                metadataVersion
        )) {
            return true;
        }
        // The sections passed before the restore, so this is the header's versions.
        contentMismatch = !DurableEpochManifest.recordsVersions(configuration, scratch, rootLen, epochGeneration, columnVersion, metadataVersion);
        return false;
    }

    /**
     * The lineage guard. Returns {@code true} when the save point is ahead of the live {@code _txn}: the live
     * {@code _txn} loads cleanly from its latest A/B slot and its {@code seqTxn} is below the save point's. Within
     * one lineage that cannot happen, because a save point is a past cut and lazy apply only advances {@code _txn}
     * after it; it means a restore rewound the table under a save point it left behind.
     * <p>
     * The guard fails open: a live {@code _txn} that does not load cleanly from its latest slot returns
     * {@code false} and lets recovery roll forward. A torn, short or garbage {@code _txn} is the post-crash state
     * recovery repairs, and a predecessor slot can predate the save point within one lineage, so its
     * {@code seqTxn} is no evidence of a restore. The caller compares the save point with the sequencer frontier:
     * one beyond it belongs to a re-seeded lineage and is discarded, anything else fails the start closed.
     */
    private boolean isEpochAheadOfLiveTxn(TableToken token, Path scratch, DurableEpochManifest.Candidate candidate) {
        final long epochSeqTxn = candidate.epochSeqTxn;
        TxReader liveTxn = null;
        try {
            tablePath(scratch, token).concat(TableUtils.TXN_FILE_NAME);
            liveTxn = new TxReader(ff);
            // Neither the seqTxn nor the area geometry depends on the partitioning or the timestamp type, so
            // this needs no metadata, as in readLiveTxn.
            liveTxn.ofRO(scratch.$(), ColumnType.TIMESTAMP, PartitionBy.NONE);
            if (!liveTxn.unsafeLoadAll()) {
                // A torn, short or garbage live _txn is the post-crash state recovery repairs: not ahead.
                return false;
            }

            // unsafeLoadAll() returns the record in the latest A/B slot, which the version word selects. Only
            // when that slot is torn does it fall back to the predecessor slot (version - 1), and it never
            // returns an older slot. So versionWord - getVersion() is 0 for the latest slot and 1 for the
            // predecessor slot. The assert pins this, so an A/B refactor cannot silently change which record the
            // guard judges. The property holds on any lineage. Recovery runs with no concurrent writer, so the
            // version word on disk cannot change during this check.
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
            // A slot-selection regression is a bug, not the torn _txn the broad catch below tolerates: rethrow
            // it rather than let the catch turn it into "not ahead".
            throw ae;
        } catch (Throwable e) {
            // A torn or unreadable live _txn is the post-crash state recovery repairs: not ahead.
            return false;
        } finally {
            Misc.free(liveTxn);
        }
    }

    private Path tablePath(Path p, TableToken token) {
        return p.of(configuration.getDbRoot()).concat(token);
    }

    /**
     * Removes the table's save point files, {@code _epoch.manifest.0} and {@code _epoch.manifest.1}, the older
     * first, and fails if one stays. {@code path} is at the table root, {@code tableRootLen} long, and is left
     * there. Absent files are accepted; the live {@code _txn} and {@code _cv} are not touched.
     * <p>
     * Call it when an external event supersedes the table's live state, so that a leftover save point would be a
     * wrong-lineage cut for {@link #recover()}: a backup, checkpoint or PITR restore, which rewinds {@code _txn}
     * and {@code _cv} without a new save point, and a primary-to-replica demote, since a replica never takes a
     * save point and recovers by download.
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

    // Pins the generation-zero save point a conversion to WAL or a REBASE WAL clone publishes outside a writer: it
    // names the table's partitions, and a writer pins only the save points it publishes itself.
    static void pinPublishedBaseline(CairoEngine engine, TableToken token, long baselineTxn) {
        new RecoveryCoordinator(engine).pinRecoveredEpoch(token, baselineTxn, 0, -1, 0);
    }

    /**
     * Releases the pins of the save point recovery restored, or a writer published, once the save point is removed:
     * the exit from adaptive at start, a conversion to non-WAL, and the Enterprise demote clear of a leftover save
     * point outside adaptive, which cannot go through the writer's reconcile.
     */
    public static void unpinEpoch(CairoEngine engine, TableToken token) {
        final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        final long pinnedTxn = tracker.getPinnedEpochTxn();
        if (pinnedTxn < 0) {
            return;
        }
        try (TxnScoreboard scoreboard = engine.getTxnScoreboard(token)) {
            scoreboard.releaseTxn(tracker.isPinnedEpochSlotA() ? TxnScoreboard.EPOCH_ID_A : TxnScoreboard.EPOCH_ID_B, pinnedTxn);
            final long otherTxn = tracker.getOtherPinnedEpochTxn();
            if (otherTxn >= 0) {
                scoreboard.releaseTxn(tracker.isPinnedEpochSlotA() ? TxnScoreboard.EPOCH_ID_B : TxnScoreboard.EPOCH_ID_A, otherTxn);
            }
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
