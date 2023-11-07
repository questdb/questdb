/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriterMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.TableListRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.cairo.wal.seq.TableTransactionLog.MAX_TXN_OFFSET;

public class DatabaseSnapshotAgentImpl implements DatabaseSnapshotAgent {

    private final static Log LOG = LogFactory.getLog(DatabaseSnapshotAgentImpl.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final AtomicBoolean inProgress = new AtomicBoolean();
    private final ReentrantLock lock = new ReentrantLock();
    private final WalWriterMetadata metadata; // protected with #lock
    private final StringSink nameSink = new StringSink(); // protected with #lock
    private final Path path = new Path(); // protected with #lock
    private final GrowOnlyTableNameRegistryStore tableNameRegistryStore; // protected with #lock
    private SimpleWaitingLock walPurgeJobRunLock = null; // used as a suspend/resume handler for the WalPurgeJob

    DatabaseSnapshotAgentImpl(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
        this.metadata = new WalWriterMetadata(ff);
        this.tableNameRegistryStore = new GrowOnlyTableNameRegistryStore(ff);
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
    public boolean isInProgress() {
        return inProgress.get();
    }

    public void setWalPurgeJobRunLock(@Nullable SimpleWaitingLock walPurgeJobRunLock) {
        this.walPurgeJobRunLock = walPurgeJobRunLock;
    }

    void completeSnapshot() throws SqlException {
        if (!lock.tryLock()) {
            throw SqlException.position(0).put("Another snapshot command in progress");
        }
        try {
            // Delete snapshot/db directory.
            path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory()).$();
            ff.rmdir(path); // it's fine to ignore errors here

            // Resume the WalPurgeJob
            if (walPurgeJobRunLock != null) {
                try {
                    walPurgeJobRunLock.unlock();
                } catch (IllegalStateException ignore) {
                    // not an error here
                    // completeSnapshot can be called several time in a row.
                }
            }

            // Reset snapshot in-flight flag.
            inProgress.set(false);
        } finally {
            lock.unlock();
        }
    }

    void prepareSnapshot(SqlExecutionContext executionContext) throws SqlException {
        // Windows doesn't support sync() system call.
        if (Os.isWindows()) {
            throw SqlException.position(0).put("Snapshots are not supported on Windows");
        }

        if (!lock.tryLock()) {
            throw SqlException.position(0).put("Another snapshot command in progress");
        }
        try {
            if (!inProgress.compareAndSet(false, true)) {
                throw SqlException.position(0).put("Waiting for SNAPSHOT COMPLETE to be called");
            }

            try {
                path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory());
                int snapshotDbLen = path.size();
                // Delete all contents of the snapshot/db dir.
                if (ff.exists(path.slash$())) {
                    path.trimTo(snapshotDbLen).$();
                    if (!ff.rmdir(path)) {
                        throw CairoException.critical(ff.errno()).put("Could not remove snapshot dir [dir=").put(path).put(']');
                    }
                }
                // Recreate the snapshot/db dir.
                path.trimTo(snapshotDbLen).slash$();
                if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
                    throw CairoException.critical(ff.errno()).put("Could not create [dir=").put(path).put(']');
                }

                // Suspend the WalPurgeJob
                if (walPurgeJobRunLock != null) {
                    final long timeout = configuration.getCircuitBreakerConfiguration().getTimeout();
                    while (!walPurgeJobRunLock.tryLock(timeout, TimeUnit.MICROSECONDS)) {
                        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    }
                }

                try {
                    // Prepare table name registry for copying.
                    path.trimTo(snapshotDbLen).$();
                    tableNameRegistryStore.of(path, 0);

                    path.trimTo(snapshotDbLen).$();
                    try (
                            TableListRecordCursorFactory factory = new TableListRecordCursorFactory();
                            RecordCursor cursor = factory.getCursor(executionContext);
                            MemoryCMARW mem = Vm.getCMARWInstance()
                    ) {
                        final int tableNameIndex = factory.getMetadata().getColumnIndex(TableListRecordCursorFactory.TABLE_NAME_COLUMN);
                        final Record record = cursor.getRecord();

                        // Copy metadata files for all tables.
                        while (cursor.hasNext()) {
                            CharSequence tableName = record.getStr(tableNameIndex);
                            path.of(configuration.getRoot());
                            TableToken tableToken = engine.verifyTableName(tableName);
                            if (
                                    TableUtils.isValidTableName(tableName, tableName.length())
                                            && ff.exists(path.concat(tableToken).concat(TableUtils.META_FILE_NAME).$())
                            ) {
                                boolean isWalTable = engine.isWalTable(tableToken);
                                path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory());
                                LOG.info().$("preparing for snapshot [table=").$(tableName).I$();

                                path.trimTo(snapshotDbLen).concat(tableToken);
                                int rootLen = path.size();
                                if (isWalTable) {
                                    path.concat(WalUtils.SEQ_DIR);
                                }
                                if (ff.mkdirs(path.slash$(), configuration.getMkDirMode()) != 0) {
                                    throw CairoException.critical(ff.errno()).put("could not create [dir=").put(path).put(']');
                                }

                                for (; ; ) {
                                    TableReader reader = null;
                                    try {
                                        reader = engine.getReaderWithRepair(tableToken);
                                        // Copy _meta file.
                                        path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$();
                                        mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                                        reader.getMetadata().dumpTo(mem);
                                        mem.close(false);
                                        // Copy _txn file.
                                        path.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME).$();
                                        mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                                        reader.getTxFile().dumpTo(mem);
                                        mem.close(false);
                                        // Copy _cv file.
                                        path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                                        mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                                        reader.getColumnVersionReader().dumpTo(mem);
                                        mem.close(false);
                                        break;
                                    } catch (EntryLockedException e) {
                                        LOG.info().$("waiting for locked table [table=").$(tableName).I$();
                                        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                                    } finally {
                                        Misc.free(reader);
                                    }
                                }

                                if (isWalTable) {
                                    // Add entry to table name registry copy.
                                    tableNameRegistryStore.logAddTable(tableToken);

                                    metadata.clear();
                                    long lastTxn = engine.getTableSequencerAPI().getTableMetadata(tableToken, metadata);
                                    path.trimTo(rootLen).concat(WalUtils.SEQ_DIR);
                                    metadata.switchTo(path, path.size(), true); // dump sequencer metadata to snapshot/db/tableName/txn_seq/_meta
                                    metadata.close(true, Vm.TRUNCATE_TO_POINTER);

                                    mem.smallFile(ff, path.concat(TableUtils.TXN_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                                    mem.putLong(lastTxn); // write lastTxn to snapshot/db/tableName/txn_seq/_txn
                                    mem.close(true, Vm.TRUNCATE_TO_POINTER);
                                }
                            } else {
                                LOG.error().$("skipping, invalid table name or missing metadata [table=").$(tableName).I$();
                            }
                        }

                        path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory()).concat(TableUtils.SNAPSHOT_META_FILE_NAME).$();
                        mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                        mem.putStr(configuration.getSnapshotInstanceId());
                        mem.close();

                        // Flush dirty pages and filesystem metadata to disk
                        if (ff.sync() != 0) {
                            throw CairoException.critical(ff.errno()).put("Could not sync");
                        }

                        LOG.info().$("snapshot copying finished").$();
                    }
                } catch (Throwable e) {
                    // Resume the WalPurgeJob
                    if (walPurgeJobRunLock != null) {
                        walPurgeJobRunLock.unlock();
                    }
                    LOG.error().$("snapshot error [e=").$(e).I$();
                    throw e;
                } finally {
                    tableNameRegistryStore.close();
                }
            } catch (Throwable e) {
                inProgress.set(false);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    void recoverSnapshot() {
        if (!configuration.isSnapshotRecoveryEnabled()) {
            return;
        }

        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();
        final CharSequence snapshotRoot = configuration.getSnapshotRoot();

        try (
                Path srcPath = new Path();
                Path dstPath = new Path();
                MemoryCMARW memFile = Vm.getCMARWInstance()
        ) {
            srcPath.of(snapshotRoot).concat(configuration.getDbDirectory());
            final int snapshotRootLen = srcPath.size();
            dstPath.of(root);
            final int rootLen = dstPath.size();

            // Check if the snapshot dir exists.
            if (!ff.exists(srcPath.slash$())) {
                return;
            }

            // Check if the snapshot metadata file exists.
            srcPath.trimTo(snapshotRootLen).concat(TableUtils.SNAPSHOT_META_FILE_NAME).$();
            if (!ff.exists(srcPath)) {
                return;
            }

            // Check if the snapshot instance id is different from what's in the snapshot.
            memFile.smallFile(ff, srcPath, MemoryTag.MMAP_DEFAULT);

            final CharSequence currentInstanceId = configuration.getSnapshotInstanceId();
            final CharSequence snapshotInstanceId = memFile.getStr(0);
            if (Chars.empty(currentInstanceId) || Chars.empty(snapshotInstanceId) || Chars.equals(currentInstanceId, snapshotInstanceId)) {
                return;
            }

            LOG.info()
                    .$("starting snapshot recovery [currentId=`").$(currentInstanceId)
                    .$("`, previousId=`").$(snapshotInstanceId)
                    .I$();

            // OK, we need to recover from the snapshot.

            // First delete all table name registry files in dst.
            srcPath.trimTo(snapshotRootLen).$();
            final int snapshotDbLen = srcPath.size();
            for (; ; ) {
                dstPath.trimTo(rootLen).$();
                int version = TableNameRegistryStore.findLastTablesFileVersion(ff, dstPath, nameSink);
                dstPath.trimTo(rootLen).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii('.').put(version).$();
                LOG.info().$("backup removing table name registry file [dst=").$(dstPath).I$();
                if (!ff.remove(dstPath)) {
                    LOG.error()
                            .$("could not remove tables.d file [dst=").$(dstPath)
                            .$(", errno=").$(ff.errno())
                            .I$();
                }
                if (version == 0) {
                    break;
                }
            }
            // Now copy the file name registry.
            srcPath.trimTo(snapshotDbLen).concat(TABLE_REGISTRY_NAME_FILE).putAscii(".0").$();
            dstPath.trimTo(rootLen).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii(".0").$();
            if (ff.copy(srcPath, dstPath) < 0) {
                LOG.error()
                        .$("could not copy tables.d file [src=").$(srcPath)
                        .$(", dst=").$(dstPath)
                        .$(", errno=").$(ff.errno())
                        .I$();
            }

            AtomicInteger recoveredMetaFiles = new AtomicInteger();
            AtomicInteger recoveredTxnFiles = new AtomicInteger();
            AtomicInteger recoveredCVFiles = new AtomicInteger();
            AtomicInteger recoveredWalFiles = new AtomicInteger();
            srcPath.trimTo(snapshotRootLen).$();
            ff.iterateDir(srcPath, (pUtf8NameZ, type) -> {
                if (ff.isDirOrSoftLinkDirNoDots(srcPath, snapshotDbLen, pUtf8NameZ, type)) {
                    dstPath.trimTo(rootLen).concat(pUtf8NameZ);
                    int srcPathLen = srcPath.size();
                    int dstPathLen = dstPath.size();

                    srcPath.concat(TableUtils.META_FILE_NAME).$();
                    dstPath.concat(TableUtils.META_FILE_NAME).$();
                    if (ff.exists(srcPath) && ff.exists(dstPath)) {
                        if (ff.copy(srcPath, dstPath) < 0) {
                            LOG.error()
                                    .$("could not copy _meta file [src=").$(srcPath)
                                    .$(", dst=").$(dstPath)
                                    .$(", errno=").$(ff.errno())
                                    .I$();
                        } else {
                            recoveredMetaFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered _meta file [src=").$(srcPath)
                                    .$(", dst=").$(dstPath)
                                    .I$();
                        }
                    }

                    srcPath.trimTo(srcPathLen).concat(TableUtils.TXN_FILE_NAME).$();
                    dstPath.trimTo(dstPathLen).concat(TableUtils.TXN_FILE_NAME).$();
                    if (ff.exists(srcPath) && ff.exists(dstPath)) {
                        if (ff.copy(srcPath, dstPath) < 0) {
                            LOG.error()
                                    .$("could not copy _txn file [src=").$(srcPath)
                                    .$(", dst=").$(dstPath)
                                    .$(", errno=").$(ff.errno())
                                    .I$();
                        } else {
                            recoveredTxnFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered _txn file [src=").$(srcPath)
                                    .$(", dst=").$(dstPath)
                                    .I$();
                        }
                    }

                    srcPath.trimTo(srcPathLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                    dstPath.trimTo(dstPathLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                    if (ff.exists(srcPath) && ff.exists(dstPath)) {
                        if (ff.copy(srcPath, dstPath) < 0) {
                            LOG.error()
                                    .$("could not copy _cv file [src=").$(srcPath)
                                    .$(", dst=").$(dstPath)
                                    .$(", errno=").$(ff.errno())
                                    .I$();
                        } else {
                            recoveredCVFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered _cv file [src=").$(srcPath)
                                    .$(", dst=").$(dstPath)
                                    .I$();
                        }
                    }

                    // Go inside SEQ_DIR
                    srcPath.trimTo(srcPathLen).concat(WalUtils.SEQ_DIR);
                    srcPathLen = srcPath.size();
                    srcPath.concat(TableUtils.META_FILE_NAME).$();

                    dstPath.trimTo(dstPathLen).concat(WalUtils.SEQ_DIR);
                    dstPathLen = dstPath.size();
                    dstPath.concat(TableUtils.META_FILE_NAME).$();

                    if (ff.exists(srcPath) && ff.exists(dstPath)) {
                        if (ff.copy(srcPath, dstPath) < 0) {
                            LOG.critical()
                                    .$("could not copy ").$(TableUtils.META_FILE_NAME).$(" file [src=").$(srcPath)
                                    .utf8(", dst=").$(dstPath)
                                    .$(", errno=").$(ff.errno())
                                    .I$();
                        } else {
                            try {
                                srcPath.trimTo(srcPathLen);
                                openSmallFile(ff, srcPath, srcPathLen, memFile, TableUtils.TXN_FILE_NAME, MemoryTag.MMAP_TX_LOG);
                                long newMaxTxn = memFile.getLong(0L); // snapshot/db/tableName/txn_seq/_txn

                                memFile.smallFile(ff, dstPath, MemoryTag.MMAP_SEQUENCER_METADATA);
                                dstPath.trimTo(dstPathLen);
                                openSmallFile(ff, dstPath, dstPathLen, memFile, TXNLOG_FILE_NAME_META_INX, MemoryTag.MMAP_TX_LOG);

                                if (newMaxTxn >= 0) {
                                    dstPath.trimTo(dstPathLen);
                                    openSmallFile(ff, dstPath, dstPathLen, memFile, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
                                    // get oldMaxTxn from dbRoot/tableName/txn_seq/_txnlog
                                    long oldMaxTxn = memFile.getLong(MAX_TXN_OFFSET);
                                    if (newMaxTxn < oldMaxTxn) {
                                        // update header of dbRoot/tableName/txn_seq/_txnlog with new values
                                        memFile.putLong(MAX_TXN_OFFSET, newMaxTxn);
                                        LOG.info()
                                                .$("updated ").$(TXNLOG_FILE_NAME).$(" file [path=").$(dstPath)
                                                .$(", oldMaxTxn=").$(oldMaxTxn)
                                                .$(", newMaxTxn=").$(newMaxTxn)
                                                .I$();
                                    }
                                }
                            } catch (CairoException ex) {
                                LOG.critical()
                                        .$("could not update file [src=").$(dstPath)
                                        .$("`, ex=").$(ex.getFlyweightMessage())
                                        .$(", errno=").$(ff.errno())
                                        .I$();
                            }

                            recoveredWalFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered ").$(TableUtils.META_FILE_NAME).$(" file [src=").$(srcPath)
                                    .$(", dst=").$(dstPath)
                                    .I$();
                        }
                    }
                }
            });
            LOG.info()
                    .$("snapshot recovery finished [metaFilesCount=").$(recoveredMetaFiles.get())
                    .$(", txnFilesCount=").$(recoveredTxnFiles.get())
                    .$(", cvFilesCount=").$(recoveredCVFiles.get())
                    .$(", walFilesCount=").$(recoveredWalFiles.get())
                    .I$();

            // Delete snapshot directory to avoid recovery on next restart.
            srcPath.trimTo(snapshotRootLen).$();
            memFile.close();
            if (!ff.rmdir(srcPath)) {
                throw CairoException.critical(ff.errno())
                        .put("could not remove snapshot dir [dir=").put(srcPath)
                        .put(", errno=").put(ff.errno())
                        .put(']');
            }
        }
    }
}
