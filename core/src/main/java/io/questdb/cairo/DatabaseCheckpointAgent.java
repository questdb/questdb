/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.MessageBus;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewGraph;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriterMetadata;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

public class DatabaseCheckpointAgent implements DatabaseCheckpointStatus, QuietCloseable {

    private final static Log LOG = LogFactory.getLog(DatabaseCheckpointAgent.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final boolean lightweightCheckpointSupported;
    private final ReentrantLock lock = new ReentrantLock();
    private final MessageBus messageBus;
    private final WalWriterMetadata metadata; // protected with #lock
    private final MicrosecondClock microClock;
    private final StringSink nameSink = new StringSink(); // protected with #lock
    private final Path path = new Path(); // protected with #lock
    private final LongList scoreboardTxns = new LongList();
    private final ObjList<TxnScoreboard> scoreboards = new ObjList<>();
    private final AtomicLong startedAtTimestamp = new AtomicLong(Numbers.LONG_NULL); // Numbers.LONG_NULL means no ongoing checkpoint
    private final SymbolMapUtil symbolMapUtil = new SymbolMapUtil();
    private final GrowOnlyTableNameRegistryStore tableNameRegistryStore; // protected with #lock
    private final Utf8StringSink utf8Sink = new Utf8StringSink();
    private ColumnVersionReader columnVersionReader = null;
    private Path partitionCleanPath; // To be used exclusively as parameter for `removePartitionDirsNotAttached`.
    private DateFormat partitionDirFmt;
    private int pathTableLen;
    private TableReaderMetadata tableMetadata = null;
    private TxWriter txWriter = null;
    private final FindVisitor removePartitionDirsNotAttached = this::removePartitionDirsNotAttached;
    private SimpleWaitingLock walPurgeJobRunLock = null; // used as a suspend/resume handler for the WalPurgeJob

    DatabaseCheckpointAgent(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.messageBus = engine.getMessageBus();
        this.microClock = configuration.getMicrosecondClock();
        this.ff = configuration.getFilesFacade();
        this.metadata = new WalWriterMetadata(ff);
        this.tableNameRegistryStore = new GrowOnlyTableNameRegistryStore(ff);
        this.lightweightCheckpointSupported = configuration.getScoreboardFormat() > 1;
    }

    @TestOnly
    public void clear() {
        lock.lock();
        try {
            metadata.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            Misc.free(path);
            Misc.free(metadata);
            Misc.free(tableNameRegistryStore);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean partitionsLocked() {
        // With new version of scoreboard there is no need to pause partition clean jobs
        return !lightweightCheckpointSupported && isInProgress();
    }

    public void setWalPurgeJobRunLock(@Nullable SimpleWaitingLock walPurgeJobRunLock) {
        this.walPurgeJobRunLock = walPurgeJobRunLock;
    }

    @Override
    public long startedAtTimestamp() {
        return startedAtTimestamp.get();
    }

    private static void copyOrError(Path srcPath, Path dstPath, FilesFacade ff, AtomicInteger counter, String fileName) {
        srcPath.concat(fileName);
        dstPath.concat(fileName);
        if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
            throw CairoException.critical(ff.errno())
                    .put("Checkpoint recovery failed. Aborting QuestDB startup. Cause: Error could not copy ")
                    .put(fileName)
                    .put(" file [src=")
                    .put(srcPath)
                    .put(", dst=")
                    .put(dstPath)
                    .put(']');
        } else {
            counter.incrementAndGet();
            LOG.info()
                    .$("recovered ").$(fileName).$(" file [src=").$(srcPath)
                    .$(", dst=").$(dstPath)
                    .I$();
        }
    }

    private void checkpointCreate(SqlExecutionContext executionContext, CharSequence checkpointRoot) throws SqlException {
        try {
            final long startedAt = microClock.getTicks();
            if (!startedAtTimestamp.compareAndSet(Numbers.LONG_NULL, startedAt)) {
                throw SqlException.position(0).put("Waiting for CHECKPOINT RELEASE to be called");
            }

            try {
                path.of(checkpointRoot).concat(configuration.getDbDirectory());
                final int checkpointDbLen = path.size();
                // delete contents of the checkpoint's "db" dir.
                if (ff.exists(path.slash$())) {
                    path.trimTo(checkpointDbLen).$();
                    if (!ff.rmdir(path)) {
                        throw CairoException.critical(ff.errno()).put("Could not remove checkpoint dir [dir=").put(path).put(']');
                    }
                }
                // recreate the checkpoint's "db" dir.
                path.trimTo(checkpointDbLen).slash$();
                if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
                    throw CairoException.critical(ff.errno()).put("Could not create [dir=").put(path).put(']');
                }

                // Suspend the WalPurgeJob
                if (walPurgeJobRunLock != null) {
                    final long timeout = configuration.getCircuitBreakerConfiguration().getQueryTimeout();
                    while (!walPurgeJobRunLock.tryLock(timeout, TimeUnit.MICROSECONDS)) {
                        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    }
                }

                try {
                    // Prepare table name registry for copying.
                    path.trimTo(checkpointDbLen).$();
                    tableNameRegistryStore.of(path, 0);
                    path.trimTo(checkpointDbLen).$();

                    final ObjHashSet<TableToken> tables = new ObjHashSet<>();
                    final ObjList<TableToken> ordered = new ObjList<>();
                    engine.getTableTokens(tables, false);
                    engine.getMatViewGraph().orderByDependentViews(tables, ordered);

                    try (
                            MemoryCMARW mem = Vm.getCMARWInstance();
                            BlockFileReader matViewFileReader = new BlockFileReader(configuration);
                            BlockFileWriter matViewFileWriter = new BlockFileWriter(ff, configuration.getCommitMode())
                    ) {
                        MatViewStateReader matViewStateReader = null;

                        // Copy metadata files for all tables.
                        for (int t = 0, n = ordered.size(); t < n; t++) {
                            TableToken tableToken = ordered.get(t);
                            if (engine.isTableDropped(tableToken)) {
                                LOG.info().$("skipping, table is dropped [table=").$(tableToken).I$();
                                continue;
                            }

                            boolean isWalTable = engine.isWalTable(tableToken);
                            path.of(checkpointRoot).concat(configuration.getDbDirectory());
                            LOG.info().$("creating table checkpoint [table=").$(tableToken).I$();

                            path.trimTo(checkpointDbLen).concat(tableToken);
                            final int rootLen = path.size();
                            if (isWalTable) {
                                path.concat(WalUtils.SEQ_DIR);
                            }
                            if (ff.mkdirs(path.slash(), configuration.getMkDirMode()) != 0) {
                                throw CairoException.critical(ff.errno()).put("could not create [dir=").put(path).put(']');
                            }

                            for (; ; ) {
                                if (engine.isTableDropped(tableToken)) {
                                    LOG.info().$("skipping, table is concurrently dropped [table=").$(tableToken).I$();
                                    break;
                                }

                                // For mat views, copy view definition and state before copying the underlying table.
                                // This way, the state copy will never hold a txn number that is newer than what's
                                // in the table copy (otherwise, such a situation may lead to lost view refresh data).
                                if (tableToken.isMatView()) {
                                    final MatViewGraph matViewGraph = engine.getMatViewGraph();
                                    final MatViewDefinition matViewDefinition = matViewGraph.getViewDefinition(tableToken);
                                    if (matViewDefinition != null) {
                                        matViewFileWriter.of(path.trimTo(rootLen).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());
                                        MatViewDefinition.append(matViewDefinition, matViewFileWriter);
                                        LOG.info().$("materialized view definition included in the checkpoint [view=").$(tableToken).I$();
                                        // the following call overwrites the path
                                        final boolean isMatViewStateExists = TableUtils.isMatViewStateFileExists(configuration, path, tableToken.getDirName());
                                        if (isMatViewStateExists) {
                                            matViewFileReader.of(path.of(configuration.getDbRoot()).concat(tableToken.getDirName()).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                                            if (matViewStateReader == null) {
                                                matViewStateReader = new MatViewStateReader();
                                            }
                                            matViewStateReader.of(matViewFileReader, tableToken);
                                            // restore the path
                                            path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(tableToken);

                                            matViewFileWriter.of(path.concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                                            MatViewState.append(matViewStateReader, matViewFileWriter);

                                            LOG.info().$("materialized view state included in the checkpoint [view=").$(tableToken).I$();
                                        } else {
                                            LOG.info().$("materialized view state not found [view=").$(tableToken).I$();
                                        }
                                    } else {
                                        LOG.info().$("skipping, materialized view is concurrently dropped [view=").$(tableToken).I$();
                                        break;
                                    }
                                }

                                TableReader reader = null;
                                try {
                                    try {
                                        reader = engine.getReaderWithRepair(tableToken);
                                    } catch (EntryLockedException e) {
                                        LOG.info().$("waiting for locked table [table=").$(tableToken).I$();
                                        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                                        continue;
                                    } catch (CairoException e) {
                                        if (engine.isTableDropped(tableToken)) {
                                            LOG.info().$("skipping, table is concurrently dropped [table=").$(tableToken).I$();
                                            break;
                                        }
                                        throw e;
                                    } catch (TableReferenceOutOfDateException e) {
                                        LOG.info().$("retrying, table reference is out of date [table=").$(tableToken).I$();
                                        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                                        continue;
                                    }

                                    // restore the path
                                    path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(tableToken);

                                    // Copy _meta file.
                                    path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME);
                                    mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                                    reader.getMetadata().dumpTo(mem);
                                    mem.close(false);
                                    // Copy _txn file.
                                    path.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME);
                                    mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                                    reader.getTxFile().dumpTo(mem);
                                    mem.close(false);
                                    // Copy _cv file.
                                    path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                                    mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                                    reader.getColumnVersionReader().dumpTo(mem);
                                    mem.close(false);

                                    if (lightweightCheckpointSupported) {
                                        long txn = reader.getTxn();
                                        TxnScoreboard scoreboard = engine.getTxnScoreboard(tableToken);
                                        if (!scoreboard.incrementTxn(TxnScoreboard.CHECKPOINT_ID, txn)) {
                                            throw CairoException.nonCritical().put("cannot lock table for checkpoint [table=").put(tableToken).put(']');
                                        }
                                        scoreboardTxns.add(txn);
                                        scoreboardTxns.add(
                                                Numbers.encodeLowHighInts(
                                                        reader.getMetadata().getPartitionBy(),
                                                        reader.getMetadata().getTimestampType()
                                                )
                                        );
                                        scoreboards.add(scoreboard);
                                    }

                                    if (isWalTable) {
                                        // Add entry to table name registry copy.
                                        tableNameRegistryStore.logAddTable(tableToken);

                                        metadata.clear();
                                        long lastTxn = engine.getTableSequencerAPI().getTableMetadata(tableToken, metadata);
                                        path.trimTo(rootLen).concat(WalUtils.SEQ_DIR);
                                        metadata.switchTo(path, path.size(), true); // dump sequencer metadata to checkpoint's  "db/tableName/txn_seq/_meta"
                                        metadata.close(true, Vm.TRUNCATE_TO_POINTER);

                                        mem.smallFile(ff, path.concat(TableUtils.TXN_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                                        mem.putLong(lastTxn); // write lastTxn to checkpoint's "db/tableName/txn_seq/_txn"
                                        mem.close(true, Vm.TRUNCATE_TO_POINTER);
                                    }

                                    LOG.info().$("table included in the checkpoint [table=").$(tableToken).I$();
                                    break;
                                } finally {
                                    Misc.free(reader);
                                }
                            }
                        }

                        path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(TableUtils.CHECKPOINT_META_FILE_NAME);
                        mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                        mem.putStr(configuration.getSnapshotInstanceId());
                        mem.close();

                        // Flush dirty pages and filesystem metadata to disk
                        if (ff.sync() != 0) {
                            throw CairoException.critical(ff.errno()).put("Could not sync");
                        }

                        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                        LOG.info().$("checkpoint created").$();
                    }
                } catch (Throwable e) {
                    // Resume the WalPurgeJob
                    if (walPurgeJobRunLock != null) {
                        walPurgeJobRunLock.unlock();
                    }
                    LOG.error().$("checkpoint error [e=").$(e).I$();
                    throw e;
                } finally {
                    tableNameRegistryStore.close();
                }
            } catch (Throwable e) {
                startedAtTimestamp.set(Numbers.LONG_NULL);
                releaseScoreboardTxns(false);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    private void rebuildSymbolFiles(Path tablePath, AtomicInteger recoveredSymbolFiles, int pathTableLen) {
        tablePath.trimTo(pathTableLen);
        for (int i = 0; i < tableMetadata.getColumnCount(); i++) {
            final int columnType = tableMetadata.getColumnType(i);
            if (ColumnType.isSymbol(columnType)) {
                final int cleanSymbolCount = txWriter.getSymbolValueCount(tableMetadata.getDenseSymbolIndex(i));
                final String columnName = tableMetadata.getColumnName(i);
                LOG.info().$("rebuilding symbol files [table=").$(tablePath)
                        .$(", column=").$safe(columnName)
                        .$(", count=").$(cleanSymbolCount)
                        .I$();

                final int writerIndex = tableMetadata.getWriterIndex(i);
                symbolMapUtil.rebuildSymbolFiles(
                        configuration,
                        tablePath,
                        columnName,
                        columnVersionReader.getDefaultColumnNameTxn(writerIndex),
                        cleanSymbolCount,
                        -1
                );
                recoveredSymbolFiles.incrementAndGet();
            }
        }
    }

    private void rebuildTableFiles(Path tablePath, AtomicInteger recoveredSymbolFiles) {
        pathTableLen = tablePath.size();
        try {
            if (tableMetadata == null) {
                tableMetadata = new TableReaderMetadata(configuration);
            }
            tableMetadata.loadMetadata(tablePath.concat(TableUtils.META_FILE_NAME).$());

            if (txWriter == null) {
                txWriter = new TxWriter(configuration.getFilesFacade(), configuration);
            }
            txWriter.ofRW(tablePath.trimTo(pathTableLen).concat(TXN_FILE_NAME).$(), tableMetadata.getTimestampType(), tableMetadata.getPartitionBy());
            txWriter.unsafeLoadAll();

            if (columnVersionReader == null) {
                columnVersionReader = new ColumnVersionReader();
            }
            tablePath.trimTo(pathTableLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            columnVersionReader.ofRO(configuration.getFilesFacade(), tablePath.$());
            columnVersionReader.readUnsafe();

            // Symbols are not append-only data structures, they can be corrupt
            // when symbol files are copied while written to. We need to rebuild them.
            rebuildSymbolFiles(tablePath, recoveredSymbolFiles, pathTableLen);

            if (tableMetadata.isWalEnabled() && txWriter.getLagRowCount() > 0) {
                LOG.info().$("resetting WAL lag [table=").$(tablePath)
                        .$(", walLagRowCount=").$(txWriter.getLagRowCount())
                        .I$();
                // WAL Lag values is not strictly append-only data structures, it can be overwritten
                // while the snapshot was copied. Resetting it will re-apply data from copied WAL files
                txWriter.resetLagAppliedRows();
            }

            if (PartitionBy.isPartitioned(tableMetadata.getPartitionBy())) {
                // Remove non-attached partitions
                LOG.debug().$("purging non attached partitions [path=").$(tablePath.$()).I$();
                partitionCleanPath = tablePath; // parameter for `removePartitionDirsNotAttached`
                this.partitionDirFmt = PartitionBy.getPartitionDirFormatMethod(
                        tableMetadata.getTimestampType(),
                        tableMetadata.getPartitionBy()
                );
                ff.iterateDir(tablePath.$(), removePartitionDirsNotAttached);
            }
        } finally {
            tablePath.trimTo(pathTableLen);
        }
    }

    private void releaseScoreboardTxns(boolean schedulePartitionPurge) {
        for (int i = 0, n = scoreboards.size(); i < n; i++) {
            long txn = scoreboardTxns.get(2 * i);
            TxnScoreboard scoreboard = scoreboards.getQuick(i);
            scoreboard.releaseTxn(TxnScoreboard.CHECKPOINT_ID, txn);

            if (schedulePartitionPurge && scoreboard.isOutdated(txn)) {
                long raw = scoreboardTxns.getQuick(2 * i + 1);
                int partitionBy = Numbers.decodeLowInt(raw);
                int timestampType = Numbers.decodeHighInt(raw);
                TableUtils.schedulePurgeO3Partitions(messageBus, scoreboard.getTableToken(), timestampType, partitionBy);
            }
        }
        scoreboardTxns.clear();
        Misc.freeObjList(scoreboards);
        scoreboards.clear();
    }

    private void removePartitionDirsNotAttached(long pUtf8NameZ, int type) {
        // Do not remove detached partitions, they are probably about to be attached
        // Do not remove wal and sequencer directories either
        int checkedType = ff.typeDirOrSoftLinkDirNoDots(partitionCleanPath, pathTableLen, pUtf8NameZ, type, utf8Sink);
        if (checkedType != Files.DT_UNKNOWN &&
                !CairoKeywords.isDetachedDirMarker(pUtf8NameZ) &&
                !CairoKeywords.isWal(pUtf8NameZ) &&
                !CairoKeywords.isTxnSeq(pUtf8NameZ) &&
                !CairoKeywords.isSeq(pUtf8NameZ) &&
                !Utf8s.endsWithAscii(utf8Sink, configuration.getAttachPartitionSuffix())
        ) {
            try {
                long txn;
                int txnSep = Utf8s.indexOfAscii(utf8Sink, '.');
                if (txnSep < 0) {
                    txnSep = utf8Sink.size();
                    txn = -1;
                } else {
                    txn = Numbers.parseLong(utf8Sink, txnSep + 1, utf8Sink.size());
                }
                long dirTimestamp = partitionDirFmt.parse(utf8Sink.asAsciiCharSequence(), 0, txnSep, EN_LOCALE);
                if (txWriter.getPartitionNameTxnByPartitionTimestamp(dirTimestamp) == txn) {
                    return;
                }
                if (!ff.unlinkOrRemove(partitionCleanPath, LOG)) {
                    LOG.info()
                            .$("failed to purge unused partition version [path=").$(partitionCleanPath)
                            .$(", errno=").$(ff.errno())
                            .I$();
                } else {
                    LOG.info().$("purged unused partition version [path=").$(partitionCleanPath).I$();
                }
                partitionCleanPath.trimTo(pathTableLen).$();
            } catch (NumericException ignore) {
                // not a date?
                // ignore exception and leave the directory
                partitionCleanPath.trimTo(pathTableLen);
                partitionCleanPath.concat(pUtf8NameZ).$();
                LOG.error().$("invalid partition directory inside table folder: ").$(partitionCleanPath).$();
            } finally {
                partitionCleanPath.trimTo(pathTableLen);
            }
        }
    }

    void checkpointCreate(SqlExecutionContext executionContext, boolean isLegacy) throws SqlException {
        // Windows doesn't support sync() system call.
        if (Os.isWindows()) {
            if (isLegacy) {
                throw SqlException.position(0).put("Snapshot is not supported on Windows");
            }
            throw SqlException.position(0).put("Checkpoint is not supported on Windows");
        }

        if (!lock.tryLock()) {
            if (isLegacy) {
                throw SqlException.position(0).put("Another snapshot command is in progress");
            }
            throw SqlException.position(0).put("Another checkpoint command is in progress");
        }
        CharSequence checkpointRoot = isLegacy ? configuration.getLegacyCheckpointRoot() : configuration.getCheckpointRoot();

        if (isLegacy) {
            path.of(configuration.getCheckpointRoot());
            if (ff.exists(path.$())) {
                LOG.info().$("removing checkpoint directory to create legacy snapshot [path=").$(path).I$();
                ff.rmdir(path);
            }
        }
        checkpointCreate(executionContext, checkpointRoot);
    }

    void checkpointRelease() throws SqlException {
        if (!lock.tryLock()) {
            throw SqlException.position(0).put("Another checkpoint command is in progress");
        }
        try {
            releaseScoreboardTxns(true);

            // Delete checkpoint's "db" directory.
            path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).$();
            ff.rmdir(path); // it's fine to ignore errors here

            // Delete snapshot's "db" directory.
            path.of(configuration.getLegacyCheckpointRoot()).concat(configuration.getDbDirectory()).$();
            ff.rmdir(path); // it's fine to ignore errors here

            // Resume the WalPurgeJob
            if (walPurgeJobRunLock != null) {
                try {
                    walPurgeJobRunLock.unlock();
                } catch (IllegalStateException ignore) {
                    // not an error here
                    // checkpointRelease() can be called several time in a row.
                }
            }

            // reset checkpoint-in-flight flag.
            startedAtTimestamp.set(Numbers.LONG_NULL);
        } finally {
            lock.unlock();
        }
    }

    void recover() {
        if (!configuration.isCheckpointRecoveryEnabled()) {
            return;
        }

        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence installRoot = configuration.getInstallRoot();
        final CharSequence dbRoot = configuration.getDbRoot();
        final CharSequence checkpointRoot = configuration.getCheckpointRoot();
        final CharSequence legacyCheckpointRoot = configuration.getLegacyCheckpointRoot();

        try (
                Path srcPath = new Path();
                Path dstPath = new Path();
                MemoryCMARW memFile = Vm.getCMARWInstance()
        ) {
            // use current checkpoint root if it exists and don't
            // bother checking that legacy path is there or not

            srcPath.of(checkpointRoot);
            if (!ff.exists(srcPath.$())) {
                srcPath.of(legacyCheckpointRoot);

                // check if a legacy path exists, in case it doesn't,
                // we should report errors against the current checkpoint root
                if (!ff.exists(srcPath.$())) {
                    srcPath.of(checkpointRoot);
                }
            }

            srcPath.concat(configuration.getDbDirectory());
            final int checkpointRootLen = srcPath.size();

            dstPath.of(installRoot).concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME);
            boolean triggerExists = ff.exists(dstPath.$());

            // Check if the checkpoint dir exists.
            if (!ff.exists(srcPath.slash$())) {
                if (triggerExists) {
                    throw CairoException.nonCritical().put("checkpoint trigger file found, but the checkpoint directory does not exist [dir=").put(srcPath).put(", trigger=").put(dstPath).put(']');
                }
                return;
            }

            // Check if the checkpoint metadata file exists.
            // legacy file first:
            srcPath.trimTo(checkpointRootLen).concat(TableUtils.CHECKPOINT_LEGACY_META_FILE_NAME);

            if (!ff.exists(srcPath.$())) {
                // now the current metadata file
                srcPath.trimTo(checkpointRootLen).concat(TableUtils.CHECKPOINT_META_FILE_NAME);
            }

            if (!ff.exists(srcPath.$())) {
                if (triggerExists) {
                    throw CairoException.nonCritical().put("checkpoint trigger file found, but the checkpoint metadata file does not exist [file=").put(srcPath).put(", trigger=").put(dstPath).put(']');
                }
                return;
            }

            // Check if the snapshot instance id is different from what's in the snapshot.
            memFile.smallFile(ff, srcPath.$(), MemoryTag.MMAP_DEFAULT);

            final CharSequence currentInstanceId = configuration.getSnapshotInstanceId();
            CharSequence snapshotInstanceId = memFile.getStrA(0);
            if (Chars.empty(snapshotInstanceId)) {
                // Check _snapshot.txt file too reading it as a text file.
                srcPath.trimTo(checkpointRootLen).concat(TableUtils.CHECKPOINT_LEGACY_META_FILE_NAME_TXT);
                String snapshotInstanceIdRaw = TableUtils.readText(ff, srcPath.$());
                if (snapshotInstanceIdRaw != null) {
                    snapshotInstanceId = snapshotInstanceIdRaw.trim();
                }
            }

            if (!triggerExists
                    && (
                    Chars.empty(currentInstanceId)
                            || Chars.empty(snapshotInstanceId)
                            || Chars.equals(currentInstanceId, snapshotInstanceId)
            )
            ) {
                LOG.info()
                        .$("skipping recovery from checkpoint [currentId=").$(currentInstanceId)
                        .$(", previousId=").$(snapshotInstanceId)
                        .I$();
                return;
            }

            // OK, we need to recover from the snapshot.
            if (triggerExists) {
                LOG.info().$("starting checkpoint recovery [trigger=file]").$();
            } else {
                LOG.info()
                        .$("starting checkpoint recovery [trigger=snapshot id")
                        .$(", currentId=").$(currentInstanceId)
                        .$(", previousId=").$(snapshotInstanceId)
                        .I$();
            }

            dstPath.of(dbRoot);
            final int rootLen = dstPath.size();

            // First delete all table name registry files in dst.
            srcPath.trimTo(checkpointRootLen).$();
            final int snapshotDbLen = srcPath.size();
            for (; ; ) {
                dstPath.trimTo(rootLen).$();
                int version = TableNameRegistryStore.findLastTablesFileVersion(ff, dstPath, nameSink);
                dstPath.trimTo(rootLen).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii('.').put(version);
                LOG.info().$("backup removing table name registry file [dst=").$(dstPath).I$();
                if (!ff.removeQuiet(dstPath.$())) {
                    throw CairoException.critical(ff.errno())
                            .put("Checkpoint recovery failed. Aborting QuestDB startup. Cause: Error could not remove registry file [file=").put(dstPath).put(']');
                }
                if (version == 0) {
                    break;
                }
            }
            // Now copy the file name registry.
            srcPath.trimTo(snapshotDbLen).concat(TABLE_REGISTRY_NAME_FILE).putAscii(".0");
            dstPath.trimTo(rootLen).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii(".0");
            if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
                throw CairoException.critical(ff.errno())
                        .put("Checkpoint recovery failed. Aborting QuestDB startup. Cause: Could not copy registry file [src=").put(srcPath).put(", dst=").put(dstPath).put(']');
            }

            AtomicInteger recoveredMetaFiles = new AtomicInteger();
            AtomicInteger recoveredTxnFiles = new AtomicInteger();
            AtomicInteger recoveredCVFiles = new AtomicInteger();
            AtomicInteger recoveredWalFiles = new AtomicInteger();
            AtomicInteger symbolFilesCount = new AtomicInteger();
            srcPath.trimTo(checkpointRootLen);
            ff.iterateDir(
                    srcPath.$(), (pUtf8NameZ, type) -> {
                        if (ff.isDirOrSoftLinkDirNoDots(srcPath, snapshotDbLen, pUtf8NameZ, type)) {
                            dstPath.trimTo(rootLen).concat(pUtf8NameZ);
                            int srcPathLen = srcPath.size();
                            int dstPathLen = dstPath.size();

                            copyOrError(srcPath, dstPath, ff, recoveredMetaFiles, TableUtils.META_FILE_NAME);
                            copyOrError(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), ff, recoveredTxnFiles, TableUtils.TXN_FILE_NAME);
                            copyOrError(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), ff, recoveredCVFiles, TableUtils.COLUMN_VERSION_FILE_NAME);
                            // Reset _todo_ file otherwise TableWriter will start restoring metadata on open.
                            TableUtils.resetTodoLog(ff, dstPath, dstPathLen, memFile);
                            rebuildTableFiles(dstPath.trimTo(dstPathLen), symbolFilesCount);

                            // Go inside SEQ_DIR
                            srcPath.trimTo(srcPathLen).concat(WalUtils.SEQ_DIR);
                            srcPathLen = srcPath.size();
                            srcPath.concat(TableUtils.META_FILE_NAME);

                            dstPath.trimTo(dstPathLen).concat(WalUtils.SEQ_DIR);
                            dstPathLen = dstPath.size();
                            dstPath.concat(TableUtils.META_FILE_NAME);

                            if (ff.exists(srcPath.$())) {
                                if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
                                    throw CairoException.critical(ff.errno())
                                            .put("Checkpoint recovery failed. Aborting QuestDB startup. Cause: Error could not copy meta file [src=").put(srcPath).put(", dst=").put(dstPath).put(']');
                                } else {
                                    srcPath.trimTo(srcPathLen);
                                    openSmallFile(ff, srcPath, srcPathLen, memFile, TableUtils.TXN_FILE_NAME, MemoryTag.MMAP_TX_LOG);
                                    long newMaxTxn = memFile.getLong(0L); // snapshot/db/tableName/txn_seq/_txn

                                    memFile.smallFile(ff, dstPath.$(), MemoryTag.MMAP_SEQUENCER_METADATA);
                                    dstPath.trimTo(dstPathLen);
                                    openSmallFile(ff, dstPath, dstPathLen, memFile, TXNLOG_FILE_NAME_META_INX, MemoryTag.MMAP_TX_LOG);

                                    if (newMaxTxn >= 0) {
                                        dstPath.trimTo(dstPathLen);
                                        openSmallFile(ff, dstPath, dstPathLen, memFile, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
                                        // get oldMaxTxn from dbRoot/tableName/txn_seq/_txnlog
                                        long oldMaxTxn = memFile.getLong(TableTransactionLogFile.MAX_TXN_OFFSET_64);
                                        if (newMaxTxn < oldMaxTxn) {
                                            // update header of dbRoot/tableName/txn_seq/_txnlog with new values
                                            memFile.putLong(TableTransactionLogFile.MAX_TXN_OFFSET_64, newMaxTxn);
                                            LOG.info()
                                                    .$("updated ").$(TXNLOG_FILE_NAME).$(" file [path=").$(dstPath)
                                                    .$(", oldMaxTxn=").$(oldMaxTxn)
                                                    .$(", newMaxTxn=").$(newMaxTxn)
                                                    .I$();
                                        }
                                    }

                                    recoveredWalFiles.incrementAndGet();
                                    LOG.info()
                                            .$("recovered ").$(TableUtils.META_FILE_NAME).$(" file [src=").$(srcPath)
                                            .$(", dst=").$(dstPath)
                                            .I$();
                                }
                            }
                        }
                    }
            );
            LOG.info()
                    .$("checkpoint recovered [metaFilesCount=").$(recoveredMetaFiles.get())
                    .$(", txnFilesCount=").$(recoveredTxnFiles.get())
                    .$(", cvFilesCount=").$(recoveredCVFiles.get())
                    .$(", walFilesCount=").$(recoveredWalFiles.get())
                    .$(", symbolFilesCount=").$(symbolFilesCount.get())
                    .I$();

            // Delete checkpoint directory to avoid recovery on next restart.
            srcPath.trimTo(checkpointRootLen).$();
            memFile.close();
            if (!ff.rmdir(srcPath)) {
                throw CairoException.critical(ff.errno())
                        .put("could not remove checkpoint dir [dir=").put(srcPath)
                        .put(", errno=").put(ff.errno())
                        .put(']');
            }
            dstPath.of(installRoot).concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME);
            if (triggerExists && !ff.removeQuiet(dstPath.$())) {
                throw CairoException.critical(ff.errno())
                        .put("could not remove restore trigger file. file permission issues? [file=").put(dstPath).put(']');
            }
        } finally {
            tableMetadata = Misc.free(tableMetadata);
            columnVersionReader = Misc.free(columnVersionReader);
            txWriter = Misc.free(txWriter);
        }
    }
}
