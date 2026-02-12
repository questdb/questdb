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

import io.questdb.MessageBus;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewGraph;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.mv.WalTxnRangeLoader;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewGraph;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriterMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.preferences.SettingsStore;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class DatabaseCheckpointAgent implements DatabaseCheckpointStatus, QuietCloseable {

    private final static Log LOG = LogFactory.getLog(DatabaseCheckpointAgent.class);
    private final ObjList<String> backupLockedDirNames = new ObjList<>();
    private final LongList backupLockedSeqTxns = new LongList();
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final ReentrantLock lock = new ReentrantLock();
    private final MessageBus messageBus;
    private final WalWriterMetadata metadata; // protected with #lock
    private final MicrosecondClock microClock;
    private final StringSink nameSink = new StringSink(); // protected with #lock
    private final Path path = new Path(); // protected with #lock
    private final LongList scoreboardTxns = new LongList();
    private final ObjList<TxnScoreboard> scoreboards = new ObjList<>();
    private final AtomicLong startedAtTimestamp = new AtomicLong(Numbers.LONG_NULL); // Numbers.LONG_NULL means no ongoing checkpoint
    private final GrowOnlyTableNameRegistryStore tableNameRegistryStore; // protected with #lock
    private final TxReader txReader;
    private volatile boolean ownsWalPurgeJobRunLock = false;
    private SimpleWaitingLock walPurgeJobRunLock = null;

    DatabaseCheckpointAgent(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.messageBus = engine.getMessageBus();
        this.microClock = configuration.getMicrosecondClock();
        this.ff = configuration.getFilesFacade();
        this.metadata = new WalWriterMetadata(ff);
        this.tableNameRegistryStore = new GrowOnlyTableNameRegistryStore(ff);
        this.txReader = new TxReader(configuration.getFilesFacade());
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
            Misc.freeObjList(scoreboards);
        } finally {
            lock.unlock();
        }
    }

    public void setWalPurgeJobRunLock(@Nullable SimpleWaitingLock walPurgeJobRunLock) {
        this.walPurgeJobRunLock = walPurgeJobRunLock;
    }

    @Override
    public long startedAtTimestamp() {
        return startedAtTimestamp.get();
    }

    /*
     * Remove all the table directories that do not exist in the checkpoint.
     * This will remove any tables that were created after the checkpoint was taken
     */
    private static void removeNonCheckpointedTableDirs(FilesFacade ff, Path dstPath, int rootLen, Path srcPath, int snapshotDbLen) {
        dstPath.trimTo(rootLen).$();
        LOG.info().$("searching ").$(dstPath).$(" for orphaned table dirs").$();
        ff.iterateDir(
                dstPath.$(), (pUtf8NameZ, type) -> {
                    if (ff.isDirOrSoftLinkDirNoDots(dstPath, rootLen, pUtf8NameZ, type)) {
                        srcPath.trimTo(snapshotDbLen);

                        // Check if the checkpoint dir is configured to be inside the db dir,
                        // so we do not delete checkpoint directory itself
                        if (!Utf8s.startsWith(srcPath, dstPath)) {
                            srcPath.concat(pUtf8NameZ);

                            // Check that the table exists in the checkpoint
                            // so that we do not restore tables that were created after the checkpoint was taken
                            // that may be in an inconsistent state.
                            if (!ff.exists(srcPath.$())) {
                                LOG.advisory().$("removing orphan table directory [dir=").$(dstPath).I$();

                                if (!ff.rmdir(dstPath)) {
                                    LOG.critical().$("failed to remove orphan table directory [dir=").$(dstPath)
                                            .$(", errno=").$(ff.errno()).I$();
                                    throw CairoException.critical(ff.errno())
                                            .put("Checkpoint recovery failed. Aborting QuestDB startup; " +
                                                    "could not remove orphan table directory, " +
                                                    "remove it manually to continue [dir=").put(dstPath).put(']');
                                }
                            }
                        }
                    }
                }
        );
        dstPath.trimTo(rootLen);
        srcPath.trimTo(snapshotDbLen);
    }

    /**
     * Restore _preferences~store from checkpoint to db_root if it exists in the checkpoint.
     * This will overwrite any existing file at the destination.
     */
    private static void restorePreferencesStore(FilesFacade ff, Path dstPath, int rootLen, Path srcPath, int snapshotDbLen) {
        srcPath.trimTo(snapshotDbLen).concat(SettingsStore.PREFERENCES_FILE_NAME).$();
        if (ff.exists(srcPath.$())) {
            dstPath.trimTo(rootLen).concat(SettingsStore.PREFERENCES_FILE_NAME).$();
            if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
                LOG.error().$("could not restore _preferences~store [errno=").$(ff.errno()).I$();
            } else {
                LOG.info().$("_preferences~store restored from checkpoint").$();
            }
        }
    }

    private void checkpointCreate(SqlExecutionCircuitBreaker circuitBreaker, CharSequence checkpointRoot, boolean isIncrementalBackup) throws SqlException {
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

                // Suspend the WalPurgeJob to prevent WAL _event files from being deleted.
                // This is needed for both incremental and non-incremental backups because
                // mat view refresh intervals are loaded from _event files during checkpoint creation.
                if (walPurgeJobRunLock != null) {
                    final long timeout = configuration.getCircuitBreakerConfiguration().getQueryTimeout();
                    while (!walPurgeJobRunLock.tryLock(timeout, TimeUnit.MICROSECONDS)) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                    }
                    ownsWalPurgeJobRunLock = true;
                }

                TableToken tableToken = null;

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
                            BlockFileWriter matViewFileWriter = new BlockFileWriter(ff, configuration.getCommitMode());
                            WalTxnRangeLoader walTxnRangeLoader = new WalTxnRangeLoader(configuration)
                    ) {
                        MatViewStateReader matViewStateReader = null;
                        // Map to track base table seqTxns for mat view interval updates
                        // Key: base table name, Value: seqTxn included in checkpoint
                        final CharSequenceObjHashMap<Long> baseTableSeqTxns = new CharSequenceObjHashMap<>();
                        // List of mat views that need their refresh intervals updated
                        final ObjList<TableToken> matViewsToUpdate = new ObjList<>();

                        // Phase 1: Copy metadata files for all tables and record base table seqTxns
                        for (int t = 0, n = ordered.size(); t < n; t++) {
                            tableToken = ordered.get(t);
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
                                long mvBaseTableTxn = -1;
                                if (tableToken.isMatView()) {
                                    final MatViewGraph matViewGraph = engine.getMatViewGraph();
                                    final MatViewDefinition matViewDefinition = matViewGraph.getViewDefinition(tableToken);
                                    if (matViewDefinition != null) {
                                        matViewFileWriter.of(path.trimTo(rootLen).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());
                                        MatViewDefinition.append(matViewDefinition, matViewFileWriter);
                                        LOG.info().$("materialized view definition included in the checkpoint [view=").$(tableToken).I$();
                                        // the following call overwrites the path
                                        final boolean matViewStateExists = TableUtils.isMatViewStateFileExists(configuration, path, tableToken.getDirName());
                                        if (matViewStateExists) {
                                            boolean matViewStateValid = false;
                                            try {
                                                matViewFileReader.of(path.of(configuration.getDbRoot()).concat(tableToken.getDirName()).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                                                matViewStateValid = true;
                                            } catch (CairoException e) {
                                                // ApplyWal2TableJob might be creating the mat view state file concurrently, to avoid any
                                                // concurrency issues we skip copying the mat view state file in this case.
                                                LOG.info().$("skipping, materialized view state file is not accessible [view=").$(tableToken).I$();
                                            }

                                            if (matViewStateValid) {
                                                if (matViewStateReader == null) {
                                                    matViewStateReader = new MatViewStateReader();
                                                }
                                                matViewStateReader.of(matViewFileReader, tableToken);
                                                mvBaseTableTxn = matViewStateReader.getLastRefreshBaseTxn();
                                                // restore the path
                                                path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(tableToken);

                                                matViewFileWriter.of(path.concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                                                MatViewState.append(matViewStateReader, matViewFileWriter);

                                                // Mark this mat view for potential interval update in phase 2
                                                matViewsToUpdate.add(tableToken);

                                                LOG.info().$("materialized view state included in the checkpoint [view=").$(tableToken).I$();
                                            }
                                        } else {
                                            LOG.info().$("materialized view state not found [view=").$(tableToken).I$();
                                        }
                                    } else {
                                        LOG.info().$("skipping, materialized view is concurrently dropped [view=").$(tableToken).I$();
                                        break;
                                    }
                                }

                                if (tableToken.isView()) {
                                    final ViewGraph viewGraph = engine.getViewGraph();
                                    ViewDefinition viewDefinition;
                                    long lastTxn = -1;

                                    viewDefinition = viewGraph.getViewDefinition(tableToken);
                                    if (viewDefinition != null) {

                                        matViewFileWriter.of(path.trimTo(rootLen).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME).$());
                                        ViewDefinition.append(viewDefinition, matViewFileWriter);

                                        // Write table _txn file to checkpoint. Read it safely, it can be changing on view alters.
                                        try (var trdr = txReader.ofRO(
                                                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(),
                                                ColumnType.TIMESTAMP_MICRO,
                                                PartitionBy.NOT_APPLICABLE
                                        )) {
                                            TableUtils.safeReadTxn(
                                                    trdr,
                                                    configuration.getMillisecondClock(),
                                                    configuration.getSpinLockTimeout()
                                            );
                                            if (trdr.seqTxn == viewDefinition.getSeqTxn()) {
                                                lastTxn = trdr.seqTxn;
                                                // Dump _txn file to checkpoint
                                                path.of(checkpointRoot)
                                                        .concat(configuration.getDbDirectory()).concat(tableToken)
                                                        .concat(TXN_FILE_NAME)
                                                        .$();
                                                mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                                                trdr.dumpTo(mem);
                                                mem.close(false);
                                            }
                                        }

                                        tableNameRegistryStore.logAddTable(tableToken);
                                        path.trimTo(rootLen).concat(WalUtils.SEQ_DIR);
                                        if (ff.mkdirs(path.slash(), configuration.getMkDirMode()) != 0) {
                                            throw CairoException.critical(ff.errno()).put("could not create [dir=").put(path).put(']');
                                        }

                                        // Generate _name file from table token (not copied from db_root).
                                        path.trimTo(rootLen).concat(TableUtils.TABLE_NAME_FILE);
                                        mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                                        TableUtils.createTableNameFile(mem, tableToken.getTableName());

                                        // Copy view _meta file to checkpoint
                                        final Path auxPath = Path.PATH2.get();
                                        auxPath.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();
                                        path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();
                                        if (ff.copy(auxPath.$(), path.$()) < 0) {
                                            throw CairoException.critical(ff.errno()).put("could not copy view metadata file [table=").put(tableToken).put(']');
                                        }

                                        // Copy txn_seq/_meta file to checkpoint
                                        auxPath.of(configuration.getDbRoot()).concat(tableToken).concat(WalUtils.SEQ_DIR).concat(TableUtils.META_FILE_NAME).$();
                                        path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(tableToken).concat(WalUtils.SEQ_DIR).concat(TableUtils.META_FILE_NAME).$();
                                        if (ff.copy(auxPath.$(), path.$()) < 0) {
                                            throw CairoException.critical(ff.errno()).put("could not copy view sequencer metadata file [table=").put(tableToken).put(']');
                                        }

                                        // Write txn_seq/_txn to checkpoint
                                        path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(tableToken).concat(WalUtils.SEQ_DIR);
                                        mem.smallFile(ff, path.concat(TableUtils.CHECKPOINT_SEQ_TXN_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                                        mem.putLong(lastTxn);
                                        mem.close(true, Vm.TRUNCATE_TO_POINTER);

                                        LOG.info().$("view included in the checkpoint [view=").$(tableToken).I$();
                                    } else {
                                        LOG.info().$("skipping, view is concurrently dropped [view=").$(tableToken).I$();
                                    }
                                    break;
                                }

                                TableReader reader = null;
                                try {
                                    try {
                                        reader = engine.getReaderWithRepair(tableToken);
                                    } catch (EntryLockedException e) {
                                        LOG.info().$("waiting for locked table [table=").$(tableToken).I$();
                                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                                        continue;
                                    } catch (CairoException e) {
                                        if (engine.isTableDropped(tableToken)) {
                                            LOG.info().$("skipping, table is concurrently dropped [table=").$(tableToken).I$();
                                            break;
                                        }
                                        throw e;
                                    } catch (TableReferenceOutOfDateException e) {
                                        LOG.info().$("retrying, table reference is out of date [table=").$(tableToken).I$();
                                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
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
                                    path.trimTo(rootLen).concat(TXN_FILE_NAME);
                                    mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                                    reader.getTxFile().dumpTo(mem);
                                    mem.close(false);
                                    // Copy _cv file.
                                    path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                                    mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                                    reader.getColumnVersionReader().dumpTo(mem);
                                    mem.close(false);

                                    // Generate _name file from table token (not copied from db_root).
                                    path.trimTo(rootLen).concat(TableUtils.TABLE_NAME_FILE);
                                    mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                                    TableUtils.createTableNameFile(mem, tableToken.getTableName());

                                    long txn = reader.getTxn();
                                    long seqTxn = reader.getSeqTxn();
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

                                    if (isWalTable) {
                                        // Add entry to table name registry copy.
                                        tableNameRegistryStore.logAddTable(tableToken);

                                        // Record the seqTxn for this table (may be a base table for mat views)
                                        baseTableSeqTxns.put(tableToken.getTableName(), seqTxn);

                                        if (isIncrementalBackup) {
                                            BackupSeqPartLock seqPartLock = engine.getBackupSeqPartLock();
                                            seqPartLock.lock(tableToken, seqTxn);
                                        }

                                        // Fetch sequencer metadata and the last committed sequencer txn.
                                        // The metadata will be dumped to the checkpoint, and lastTxn is stored
                                        // separately to know which sequencer txn the metadata corresponds to.
                                        metadata.clear();
                                        long lastTxn = engine.getTableSequencerAPI().getTableMetadata(tableToken, metadata);
                                        path.trimTo(rootLen).concat(WalUtils.SEQ_DIR);
                                        metadata.switchTo(path, path.size(), true); // dump sequencer metadata to checkpoint's  "db/tableName/txn_seq/_meta"
                                        metadata.close(true, Vm.TRUNCATE_TO_POINTER);

                                        mem.smallFile(ff, path.concat(TableUtils.CHECKPOINT_SEQ_TXN_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                                        // write lastTxn to the end of checkpoint's "db/tableName/txn_seq/_txn"
                                        // This is not main table _txn file but an additional one 8 bytes file that stores
                                        // the sequencer txn of the snapshot sequencer metadata
                                        mem.putLong(lastTxn);
                                        mem.close(true, Vm.TRUNCATE_TO_POINTER);
                                    }

                                    LogRecord logRecord = LOG.info().$("table included in the checkpoint [table=").$(tableToken)
                                            .$(", txn=").$(txn)
                                            .$(", seqTxn=").$(seqTxn);
                                    if (tableToken.isMatView()) {
                                        logRecord.$(", mvBaseTableTxn=").$(mvBaseTableTxn);
                                    }
                                    logRecord.I$();
                                    break;
                                } finally {
                                    Misc.free(reader);
                                }
                            }
                        }

                        // Update mat view states with missing refresh intervals
                        // This is only needed for incremental backups when WAL base table _event files are
                        // not copied to with the checkpoint.
                        if (isIncrementalBackup) {
                            updateMatViewRefreshIntervals(
                                    checkpointRoot,
                                    matViewsToUpdate,
                                    baseTableSeqTxns,
                                    matViewFileReader,
                                    matViewFileWriter,
                                    walTxnRangeLoader
                            );
                        }

                        path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(TableUtils.CHECKPOINT_META_FILE_NAME);
                        mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                        mem.putStr(configuration.getSnapshotInstanceId());
                        mem.close();

                        // Copy _preferences~store if it exists (contains WebConsole instance name, etc.)
                        SettingsStore settingsStore = engine.getSettingsStore();
                        if (settingsStore.getVersion() > 0) {
                            path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(SettingsStore.PREFERENCES_FILE_NAME).$();
                            settingsStore.persistTo(path.$(), ff, configuration.getCommitMode());
                            LOG.info().$("_preferences~store included in the checkpoint").$();
                        }

                        // Flush dirty pages and filesystem metadata to disk
                        if (!isIncrementalBackup && ff.sync() != 0) {
                            throw CairoException.critical(ff.errno()).put("Could not sync");
                        }

                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                        LOG.info().$("checkpoint created").$();
                    }
                } catch (Throwable e) {
                    // Resume the WalPurgeJob
                    if (walPurgeJobRunLock != null && ownsWalPurgeJobRunLock && walPurgeJobRunLock.isLocked()) {
                        walPurgeJobRunLock.unlock();
                        ownsWalPurgeJobRunLock = false;
                    }
                    engine.getBackupSeqPartLock().clear();
                    var log = LOG.error().$("cannot create checkpoint [e=").$(e);
                    if (tableToken != null) {
                        log.$(", table=").$(tableToken);
                    }
                    log.I$();
                    if (e instanceof CairoException ex && tableToken != null) {
                        // Copy exception message in case the exception instance is re-used
                        StringSink ss = Misc.getThreadLocalSink();
                        ss.put(ex.getFlyweightMessage());

                        throw CairoException.critical(ex.errno).put("error creating checkpoint [table=")
                                .put(tableToken).put(", error=").put(ss)
                                .put(']');
                    }
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
            if (isIncrementalBackup && ownsWalPurgeJobRunLock) {
                walPurgeJobRunLock.unlock();
                ownsWalPurgeJobRunLock = false;
            }
            lock.unlock();
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

    /**
     * Updates mat view states in the checkpoint with missing refresh intervals.
     * This ensures the checkpoint mat view state includes intervals up to the checkpoint base table txn,
     * so that after restore the mat view can do an incremental refresh instead of a full refresh.
     */
    private void updateMatViewRefreshIntervals(
            CharSequence checkpointRoot,
            ObjList<TableToken> matViewsToUpdate,
            CharSequenceObjHashMap<Long> baseTableSeqTxns,
            BlockFileReader matViewFileReader,
            BlockFileWriter matViewFileWriter,
            WalTxnRangeLoader walTxnRangeLoader
    ) {
        final LongList intervals = new LongList();
        MatViewStateReader matViewStateReader = null;

        for (int m = 0, n = matViewsToUpdate.size(); m < n; m++) {
            TableToken matViewToken = matViewsToUpdate.get(m);
            final MatViewGraph matViewGraph = engine.getMatViewGraph();
            final MatViewDefinition matViewDefinition = matViewGraph.getViewDefinition(matViewToken);
            if (matViewDefinition == null) {
                continue;
            }

            final String baseTableName = matViewDefinition.getBaseTableName();
            final Long checkpointBaseSeqTxn = baseTableSeqTxns.get(baseTableName);
            if (checkpointBaseSeqTxn == null) {
                // Base table not in checkpoint (might be non-WAL or dropped)
                continue;
            }

            // Re-read the mat view state from checkpoint
            path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$();
            if (!ff.exists(path.$())) {
                continue;
            }

            matViewFileReader.of(path.$());
            if (matViewStateReader == null) {
                matViewStateReader = new MatViewStateReader();
            }
            matViewStateReader.of(matViewFileReader, matViewToken);

            // Skip invalid mat views - they will do a full refresh on restore anyway
            if (matViewStateReader.isInvalid()) {
                continue;
            }

            // Check if we need to update intervals
            final long mvLastRefreshTxn = Math.max(
                    matViewStateReader.getLastRefreshBaseTxn(),
                    matViewStateReader.getRefreshIntervalsBaseTxn()
            );

            if (mvLastRefreshTxn >= checkpointBaseSeqTxn || mvLastRefreshTxn < 0) {
                // Mat view is up to date with checkpoint base table txn, or never refreshed
                continue;
            }
            // Load missing intervals from WAL transactions
            try {
                TableToken baseTableToken = engine.verifyTableName(baseTableName);
                intervals.clear();
                walTxnRangeLoader.load(engine, Path.PATH.get(), baseTableToken, intervals, mvLastRefreshTxn, checkpointBaseSeqTxn);

                if (intervals.size() > 0) {
                    // Merge with existing intervals
                    final int dividerIndex = intervals.size();
                    intervals.addAll(matViewStateReader.getRefreshIntervals());
                    IntervalUtils.unionInPlace(intervals, dividerIndex);

                    // Cap the number of intervals
                    final int cacheCapacity = configuration.getMatViewMaxRefreshIntervals() << 1;
                    if (intervals.size() > cacheCapacity) {
                        intervals.setQuick(cacheCapacity - 1, intervals.getQuick(intervals.size() - 1));
                        intervals.setPos(cacheCapacity);
                    }
                }

                // Rewrite the mat view state with updated intervals
                path.of(checkpointRoot).concat(configuration.getDbDirectory()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$();
                matViewFileWriter.of(path.$());
                MatViewState.append(
                        matViewStateReader.getLastRefreshTimestampUs(),
                        matViewStateReader.getLastRefreshBaseTxn(),
                        matViewStateReader.isInvalid(),
                        matViewStateReader.getInvalidationReason(),
                        matViewStateReader.getLastPeriodHi(),
                        intervals.size() > 0 ? intervals : matViewStateReader.getRefreshIntervals(),
                        checkpointBaseSeqTxn,
                        matViewFileWriter
                );

                LOG.info().$("updated materialized view state with missing refresh intervals [view=").$(matViewToken)
                        .$(", fromTxn=").$(mvLastRefreshTxn)
                        .$(", toTxn=").$(checkpointBaseSeqTxn)
                        .$(", intervalCount=").$(intervals.size() / 2)
                        .I$();
            } catch (CairoException e) {
                // Log warning but don't fail the checkpoint - the mat view will do a full refresh on restore
                LOG.error().$("could not load missing refresh intervals for mat view [view=").$(matViewToken)
                        .$(", baseTable=").$(baseTableName)
                        .$(", error=").$(e.getFlyweightMessage())
                        .I$();
            }
        }
    }

    void checkpointCreate(SqlExecutionCircuitBreaker circuitBreaker, boolean isLegacy, boolean incrementalBackup) throws SqlException {
        // Windows doesn't support sync() system call.
        if (!incrementalBackup && Os.isWindows()) {
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
        checkpointCreate(circuitBreaker, checkpointRoot, incrementalBackup);
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
            if (walPurgeJobRunLock != null && ownsWalPurgeJobRunLock && walPurgeJobRunLock.isLocked()) {
                try {
                    walPurgeJobRunLock.unlock();
                    ownsWalPurgeJobRunLock = false;
                } catch (IllegalStateException ignore) {
                    // not an error here
                    // checkpointRelease() can be called several time in a row.
                }
            }

            // Clear backup seq part locks
            engine.getBackupSeqPartLock().clear();
            backupLockedDirNames.clear();
            backupLockedSeqTxns.clear();

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
                MemoryCMARW memFile = Vm.getCMARWInstance();
                TableSnapshotRestore recoveryAgent = new TableSnapshotRestore(configuration)
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

            recoveryAgent.restoreTableRegistry(srcPath, dstPath, snapshotDbLen, rootLen, nameSink);

            AtomicInteger recoveredMetaFiles = new AtomicInteger();
            AtomicInteger recoveredWalFiles = new AtomicInteger();
            AtomicInteger symbolFilesCount = new AtomicInteger();
            srcPath.trimTo(checkpointRootLen);

            try {
                ff.iterateDir(
                        srcPath.$(), (pUtf8NameZ, type) -> {
                            if (ff.isDirOrSoftLinkDirNoDots(srcPath, snapshotDbLen, pUtf8NameZ, type)) {
                                dstPath.trimTo(rootLen).concat(pUtf8NameZ);

                                recoveryAgent.restoreTableFiles(
                                        srcPath,
                                        dstPath,
                                        recoveredMetaFiles,
                                        recoveredWalFiles,
                                        symbolFilesCount,
                                        configuration.getCheckpointRecoveryRebuildColumnIndexes()
                                );
                            }
                        }
                );

                restorePreferencesStore(ff, dstPath, rootLen, srcPath, snapshotDbLen);
                removeNonCheckpointedTableDirs(ff, dstPath, rootLen, srcPath, snapshotDbLen);
            } catch (Throwable e) {
                LOG.error().$("error during checkpoint recovery, aborting async tasks [error=").$(e).I$();
                recoveryAgent.abortParallelTasks();
                recoveryAgent.finalizeParallelTasks();
                throw e;
            }

            recoveryAgent.finalizeParallelTasks();
            LOG.info()
                    .$("checkpoint recovered [metaFilesCount=").$(recoveredMetaFiles.get())
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
        }
    }
}
