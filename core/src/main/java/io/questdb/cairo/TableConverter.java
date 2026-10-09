/*+*****************************************************************************
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

import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.CONVERT_FILE_NAME;

public class TableConverter {
    private static final Log LOG = LogFactory.getLog(TableConverter.class);
    private static final String REPLAY_LOCK_REASON = "convertReplay";

    public static ObjList<TableToken> convertTables(
            CairoEngine engine,
            TableSequencerAPI tableSequencerAPI,
            TableFlagResolver tableFlagResolver,
            TableNameRegistry tableNameRegistry
    ) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final ObjList<TableToken> convertedTables = new ObjList<>();
        if (!configuration.isTableTypeConversionEnabled()) {
            LOG.info().$("table type conversion is disabled").$();
            return null;
        }
        if (configuration.isReadOnlyInstance()) {
            LOG.info().$("read only instance is not allowed to perform table type conversion").$();
            return null;
        }

        final Path path = Path.getThreadLocal(configuration.getDbRoot());
        final Path metaPath = Path.getThreadLocal2(configuration.getDbRoot());
        final int rootLen = path.size();
        final Utf8StringSink dirNameSink = Misc.getThreadLocalUtf8Sink();
        final FilesFacade ff = configuration.getFilesFacade();
        final long findPtr = ff.findFirst(path.$());
        TxWriter txWriter = null;
        try {
            do {
                if (ff.isDirOrSoftLinkDirNoDots(path, rootLen, ff.findName(findPtr), ff.findType(findPtr), dirNameSink)) {
                    if (!ff.exists(path.concat(WalUtils.CONVERT_FILE_NAME).$())) {
                        continue;
                    }
                    try {
                        final boolean walEnabled = readWalEnabled(path.$(), ff);
                        LOG.info().$("converting table [dirName=").$(dirNameSink)
                                .$(", walEnabled=").$(walEnabled)
                                .I$();

                        final String dirName = dirNameSink.toString();
                        TableToken existingToken = tableNameRegistry.getTableTokenByDirName(dirName);
                        if (!walEnabled
                                && existingToken != null
                                && existingToken.isWal()
                                && !tableNameRegistry.isWalTableDropped(dirName)) {
                            // The conversion below deletes the WAL, so apply it first. A replay that cannot
                            // finish throws, and the table stays a WAL table.
                            //
                            // The WAL apply code uses the same thread-local Path and sink as this method,
                            // so reset both afterwards, even when the replay throws: the directory scan
                            // continues with them.
                            try {
                                replayWalBeforeConversion(engine, existingToken);
                            } finally {
                                path.of(configuration.getDbRoot());
                                metaPath.of(configuration.getDbRoot());
                                dirNameSink.clear();
                                dirNameSink.put(dirName);
                            }
                        }

                        path.trimTo(rootLen).concat(dirNameSink);
                        metaPath.trimTo(rootLen).concat(dirNameSink);
                        try (final MemoryMARW metaMem = Vm.getCMARWInstance()) {
                            openSmallFile(ff, metaPath, rootLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER_METADATA);

                            if (metaMem.getBool(TableUtils.META_OFFSET_WAL_ENABLED) == walEnabled && existingToken != null && existingToken.isWal() == walEnabled) {
                                LOG.info().$("skipping conversion, table already has the expected type [dirName=").$(dirNameSink)
                                        .$(", walEnabled=").$(walEnabled)
                                        .I$();
                            } else {
                                final String tableName;
                                try (final MemoryCMR mem = Vm.getCMRInstance()) {
                                    final String name = TableUtils.readTableName(path.of(configuration.getDbRoot()).concat(dirNameSink), rootLen, mem, ff);
                                    tableName = name != null ? name : dirName;
                                }

                                final int tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
                                boolean isProtected = tableFlagResolver.isProtected(tableName);
                                boolean isSystem = tableFlagResolver.isSystem(tableName);
                                boolean isPublic = tableFlagResolver.isPublic(tableName);
                                TableToken.Type type = isLiveViewDefinitionFileExists(configuration, path, dirName) ? TableToken.Type.LIVE_VIEW
                                        : isMatViewDefinitionFileExists(configuration, path, dirName) ? TableToken.Type.MAT_VIEW
                                          : isViewDefinitionFileExists(configuration, path, dirName) ? TableToken.Type.VIEW
                                            : TableToken.Type.TABLE;
                                boolean isWal = walEnabled || type.isImplicitlyWal();
                                final TableToken token = new TableToken(tableName, dirName, engine.getConfiguration().getDbLogName(), tableId, type, isWal, isSystem, isProtected, isPublic);

                                if (txWriter == null) {
                                    txWriter = new TxWriter(ff, configuration);
                                }
                                txWriter.ofRW(path.trimTo(rootLen).concat(dirNameSink).concat(TXN_FILE_NAME).$());
                                // Structural one-shot conversion, outside any table writer and outside the
                                // adaptive epoch's coverage: take the SYNC grade under ADAPTIVE.
                                txWriter.setCommitMode(CommitMode.structuralCommitMode(configuration.getCommitMode()));

                                // A type conversion RESETS this table's WAL lineage: resetLagValuesUnsafe()
                                // below zeroes _txn.seqTxn, and the WAL branch re-seeds txn_seq from txn 0.
                                // Every durable-epoch artifact in this directory anchors the lineage that ends
                                // here, so it must go -- the rule every other rewinding path already obeys
                                // (TableSnapshotRestore on checkpoint restore, and the REBASE WAL staging
                                // clone, which excludes the source anchor and publishes its own). Left behind,
                                // the stale anchor post-dates the reset live _txn and startup recovery refuses
                                // to bring the instance up. A crash after the removal leaves a table without a
                                // save point, which is not enrolled; _convert survives and the conversion runs
                                // again on the next boot.
                                path.trimTo(rootLen).concat(dirNameSink);
                                RecoveryCoordinator.removeAdaptiveEpochArtifacts(ff, path, path.size());

                                txWriter.resetLagValuesUnsafe();

                                int convertedTimestampType = ColumnType.TIMESTAMP;
                                int convertedPartitionBy = PartitionBy.NONE;
                                if (walEnabled) {
                                    try (TableWriterMetadata metadata = new TableWriterMetadata(token)) {
                                        metadata.reload(metaPath, metaMem);
                                        convertedTimestampType = metadata.getTimestampIndex() < 0
                                                ? ColumnType.TIMESTAMP
                                                : metadata.getColumnType(metadata.getTimestampIndex());
                                        convertedPartitionBy = metadata.getPartitionBy();
                                        tableSequencerAPI.registerTable(tableId, metadata, token);
                                    }

                                    // Reset structure version in _meta and _txn files. Through
                                    // resetMetadataVersion, never a raw putLong: the metadataVersion is
                                    // checksummed into the meta-format minor-version field, so rewriting it
                                    // in place otherwise switches off every version-gated tail field and the
                                    // converted table silently loses its TTL and table format.
                                    TableUtils.resetMetadataVersion(metaMem, 0);
                                    path.trimTo(rootLen).concat(dirNameSink);
                                    txWriter.resetStructureVersionUnsafe();
                                } else {
                                    if (!tableNameRegistry.isWalTableDropped(dirName) && tableSequencerAPI.prepareToConvertToNonWal(token)) {
                                        removeWalPersistence(path, rootLen, ff, dirNameSink);
                                    } else {
                                        LOG.info().$("WAL table will not be converted to non-WAL, table is dropped [dirName=").$(dirNameSink).I$();
                                        continue;
                                    }
                                }
                                metaMem.putBool(TableUtils.META_OFFSET_WAL_ENABLED, walEnabled);

                                // The converted table is a fresh WAL lineage sitting at seqTxn 0, so give it
                                // its own generation-zero anchor here, the way the REBASE WAL clone does. A
                                // non-adaptive instance publishes nothing.
                                path.trimTo(rootLen).concat(dirNameSink);
                                if (walEnabled && configuration.getCommitMode() == CommitMode.ADAPTIVE) {
                                    DurableEpochManifest.publishInitialAt(
                                            configuration,
                                            token,
                                            path,
                                            convertedTimestampType,
                                            convertedPartitionBy,
                                            configuration.getMicrosecondClock().getTicks() / 1000L
                                    );
                                }

                                convertedTables.add(token);

                                try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                                    metadataRW.hydrateTable(token);
                                }
                            }

                            path.trimTo(rootLen).concat(dirNameSink).concat(CONVERT_FILE_NAME);
                            if (!ff.removeQuiet(path.$())) {
                                LOG.critical().$("could not remove _convert file [path=").$(path).I$();
                            }
                        }
                    } catch (Exception e) {
                        // _convert stays, so the next start tries again.
                        LOG.critical().$("table conversion failed, the table keeps its type [path=").$(path).$(", e=").$(e).I$();
                    } finally {
                        if (txWriter != null) {
                            txWriter.close();
                        }
                    }
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
        }
        return convertedTables;
    }

    private static void applyOutstandingWal(CairoEngine engine, TableToken token, long lastSeqTxn) {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        try (ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 0)) {
            long previousSeqTxn = -1;
            while (true) {
                // applyWalDirect() never throws. When a transaction fails to apply, it suspends the table and
                // returns. It can also return before the end of the WAL, when it runs out of its time quota or
                // backs off under memory pressure. So call it again while it makes progress, and stop at the
                // end of the WAL, when the table is suspended, or after a call that applied nothing.
                job.applyWalDirect(token, Job.RUNNING_STATUS);
                try (TableWriter writer = engine.getWriterUnsafe(token, REPLAY_LOCK_REASON)) {
                    final long seqTxn = writer.getSeqTxn();
                    if (seqTxn < lastSeqTxn) {
                        if (seqTxn != previousSeqTxn && !tracker.isSuspended()) {
                            previousSeqTxn = seqTxn;
                            continue;
                        }
                        throw CairoException.critical(0)
                                .put("could not apply the WAL before the conversion to non-WAL, the table stays a WAL table;")
                                .put(" resume it, let it catch up and restart [table=").put(token.getTableName())
                                .put(", seqTxn=").put(seqTxn)
                                .put(", lastSeqTxn=").put(lastSeqTxn)
                                .put(", suspended=").put(tracker.isSuspended())
                                .put(']');
                    }
                    // After the conversion the WAL is gone, so the replayed rows must be on disk first.
                    try {
                        writer.fsyncMaterializedState();
                    } catch (CairoException | CairoError e) {
                        // No save point on close; the caller stops the engine.
                        if (CairoException.isDataSyncFailure(e)) {
                            writer.markDistressed();
                        }
                        throw e;
                    }
                    LOG.info().$("replayed WAL before conversion [table=").$(token)
                            .$(", seqTxn=").$(seqTxn)
                            .I$();
                    break;
                }
            }
        }
        // The conversion rewrites _txn and _meta directly. Close the writer and readers the replay left in
        // the pools, or they would keep serving the table's state from before the conversion.
        final String lockedReason = engine.lockAll(token, REPLAY_LOCK_REASON, true);
        if (lockedReason != null) {
            throw CairoException.critical(0)
                    .put("could not release table after WAL replay [table=").put(token.getTableName())
                    .put(", reason=").put(lockedReason)
                    .put(']');
        }
        engine.unlock(AllowAllSecurityContext.INSTANCE, token, null, false);
    }

    // The seqTxn in the table's _txn file. Returns Long.MAX_VALUE when the file does not load, so that the
    // caller skips the replay and the table converts as it did before the replay existed.
    private static long readSeqTxn(FilesFacade ff, Path path, int tableRootLen) {
        path.trimTo(tableRootLen).concat(TXN_FILE_NAME);
        try (TxReader txReader = new TxReader(ff)) {
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.NONE);
            return txReader.unsafeLoadAll() ? txReader.getSeqTxn() : Long.MAX_VALUE;
        } catch (CairoException e) {
            return Long.MAX_VALUE;
        } finally {
            path.trimTo(tableRootLen);
        }
    }

    static boolean readWalEnabled(LPSZ path, FilesFacade ff) {
        long fd = -1;
        try {
            fd = ff.openRO(path);
            if (fd < 1) {
                throw CairoException.critical(ff.errno()).put("could not open file [path=").put(path).put(']');
            }

            final byte walType = ff.readNonNegativeByte(fd, 0);
            return switch (walType) {
                case TABLE_TYPE_WAL, TABLE_TYPE_VIEW, TABLE_TYPE_MAT, TABLE_TYPE_LIVE_VIEW -> true;
                case TABLE_TYPE_NON_WAL -> false;
                default ->
                        throw CairoException.critical(ff.errno()).put("could not read walType from file [path=").put(path).put(']');
            };
        } finally {
            ff.close(fd);
        }
    }

    /**
     * Applies the table's outstanding WAL before the conversion to non-WAL deletes it, in every mode.
     * <p>
     * A table with a save point leaves adaptive here, as recovery's exit does on a start outside adaptive, which
     * skips such a table: after the replay the table is flushed, the save point removed and its pin released. A
     * crash before the removal leaves the save point, so the next start rewinds the table to it and replays again.
     * <p>
     * A replay that stops early, at a transaction that fails to apply or after a pass that applied nothing,
     * throws: the caller keeps {@code _convert}, the WAL and the save point, the table stays a WAL table and the
     * next start retries. A failed flush stops the engine.
     */
    private static void replayWalBeforeConversion(CairoEngine engine, TableToken token) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        final long lastSeqTxn;
        try {
            lastSeqTxn = engine.getTableSequencerAPI().lastTxn(token);
        } catch (CairoException e) {
            // No readable sequencer, so no WAL to replay. The conversion already handles a missing sequencer.
            LOG.info().$("cannot read sequencer, converting without WAL replay [table=").$(token)
                    .$(", error=").$((Throwable) e)
                    .I$();
            return;
        }
        try (Path tablePath = new Path()) {
            tablePath.of(configuration.getDbRoot()).concat(token);
            final int tableRootLen = tablePath.size();
            if (DurableEpochManifest.loadCandidates(ff, tablePath, tableRootLen, -1).length == 0
                    && readSeqTxn(ff, tablePath, tableRootLen) >= lastSeqTxn) {
                return;
            }
            try {
                applyOutstandingWal(engine, token, lastSeqTxn);
                // On an adaptive instance the replay's writer may also have enrolled a table that had no save point.
                RecoveryCoordinator.removeAdaptiveEpochArtifacts(ff, tablePath.trimTo(tableRootLen), tableRootLen);
                RecoveryCoordinator.unpinEpoch(engine, token);
            } catch (Throwable th) {
                if (CairoException.isDataSyncFailure(th)) {
                    engine.handleDataSyncFailure(th);
                }
                throw th;
            }
        }
    }

    private static void removeWalPersistence(Path path, int rootLen, FilesFacade ff, Utf8Sequence dirName) {
        path.trimTo(rootLen).concat(dirName).concat(WalUtils.SEQ_DIR).$();
        if (!ff.rmdir(path)) {
            LOG.error()
                    .$("could not remove sequencer dir [errno=").$(ff.errno())
                    .$(", path=").$(path)
                    .I$();
        }

        path.trimTo(rootLen).concat(dirName);
        int plen = path.size();
        final long pFind = ff.findFirst(path.$());
        if (pFind > 0) {
            try {
                do {
                    long name = ff.findName(pFind);
                    int type = ff.findType(pFind);
                    if (CairoKeywords.isWal(name)) {
                        path.trimTo(plen).concat(name);
                        if (type == Files.DT_FILE && CairoKeywords.isLock(name)) {
                            if (!ff.removeQuiet(path.$())) {
                                LOG.error()
                                        .$("could not remove wal lock file [errno=").$(ff.errno())
                                        .$(", path=").$(path)
                                        .I$();
                            }
                        } else {
                            if (!ff.rmdir(path)) {
                                LOG.error()
                                        .$("could not remove wal dir [errno=").$(ff.errno())
                                        .$(", path=").$(path)
                                        .I$();
                            }
                        }
                    }
                } while (ff.findNext(pFind) > 0);
            } finally {
                ff.findClose(pFind);
            }
        }
    }
}
