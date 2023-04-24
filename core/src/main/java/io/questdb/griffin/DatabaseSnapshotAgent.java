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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriterMetadata;
import io.questdb.griffin.engine.table.TableListRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME_META_INX;
import static io.questdb.cairo.wal.seq.TableTransactionLog.MAX_TXN_OFFSET;

public class DatabaseSnapshotAgent implements Closeable {

    private final static Log LOG = LogFactory.getLog(DatabaseSnapshotAgent.class);
    private final AtomicBoolean activePrepareFlag = new AtomicBoolean();
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final ReentrantLock lock = new ReentrantLock(); // protects below fields
    private final WalWriterMetadata metadata;
    private final Path path = new Path();
    // List of readers kept around to lock partitions while a database snapshot is being made.
    private final ObjList<TableReader> snapshotReaders = new ObjList<>();
    private SimpleWaitingLock walPurgeJobRunLock = null; // used as a suspend/resume handler for the WalPurgeJob

    public DatabaseSnapshotAgent(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
        this.metadata = new WalWriterMetadata(ff);
    }

    public static void recoverSnapshot(CairoEngine engine) {
        final CairoConfiguration configuration = engine.getConfiguration();
        if (!configuration.isSnapshotRecoveryEnabled()) {
            return;
        }

        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();
        final CharSequence snapshotRoot = configuration.getSnapshotRoot();

        try (Path srcPath = new Path(); Path dstPath = new Path(); MemoryCMARW memFile = Vm.getCMARWInstance()) {
            srcPath.of(snapshotRoot).concat(configuration.getDbDirectory());
            final int snapshotRootLen = srcPath.length();
            dstPath.of(root);
            final int rootLen = dstPath.length();

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
                    .$("`]").$();

            // OK, we need to recover from the snapshot.
            AtomicInteger recoveredMetaFiles = new AtomicInteger();
            AtomicInteger recoveredTxnFiles = new AtomicInteger();
            AtomicInteger recoveredCVFiles = new AtomicInteger();
            AtomicInteger recoveredWalFiles = new AtomicInteger();
            srcPath.trimTo(snapshotRootLen).$();
            final int snapshotDbLen = srcPath.length();
            ff.iterateDir(srcPath, (pUtf8NameZ, type) -> {
                if (ff.isDirOrSoftLinkDirNoDots(srcPath, snapshotDbLen, pUtf8NameZ, type)) {
                    dstPath.trimTo(rootLen).concat(pUtf8NameZ);
                    int srcPathLen = srcPath.length();
                    int dstPathLen = dstPath.length();

                    srcPath.concat(TableUtils.META_FILE_NAME).$();
                    dstPath.concat(TableUtils.META_FILE_NAME).$();
                    if (ff.exists(srcPath) && ff.exists(dstPath)) {
                        if (ff.copy(srcPath, dstPath) < 0) {
                            LOG.error()
                                    .$("could not copy _meta file [src=").utf8(srcPath)
                                    .$(", dst=").utf8(dstPath)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        } else {
                            recoveredMetaFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered _meta file [src=").utf8(srcPath)
                                    .$(", dst=").utf8(dstPath)
                                    .$(']').$();
                        }
                    }

                    srcPath.trimTo(srcPathLen).concat(TableUtils.TXN_FILE_NAME).$();
                    dstPath.trimTo(dstPathLen).concat(TableUtils.TXN_FILE_NAME).$();
                    if (ff.exists(srcPath) && ff.exists(dstPath)) {
                        if (ff.copy(srcPath, dstPath) < 0) {
                            LOG.error()
                                    .$("could not copy _txn file [src=").utf8(srcPath)
                                    .$(", dst=").utf8(dstPath)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        } else {
                            recoveredTxnFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered _txn file [src=").utf8(srcPath)
                                    .$(", dst=").utf8(dstPath)
                                    .$(']').$();
                        }
                    }

                    srcPath.trimTo(srcPathLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                    dstPath.trimTo(dstPathLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                    if (ff.exists(srcPath) && ff.exists(dstPath)) {
                        if (ff.copy(srcPath, dstPath) < 0) {
                            LOG.error()
                                    .$("could not copy _cv file [src=").utf8(srcPath)
                                    .$(", dst=").utf8(dstPath)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        } else {
                            recoveredCVFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered _cv file [src=").utf8(srcPath)
                                    .$(", dst=").utf8(dstPath)
                                    .$(']').$();
                        }
                    }

                    // Go inside SEQ_DIR
                    srcPath.trimTo(srcPathLen).concat(WalUtils.SEQ_DIR);
                    srcPathLen = srcPath.length();
                    srcPath.concat(TableUtils.META_FILE_NAME).$();

                    dstPath.trimTo(dstPathLen).concat(WalUtils.SEQ_DIR);
                    dstPathLen = dstPath.length();
                    dstPath.concat(TableUtils.META_FILE_NAME).$();

                    if (ff.exists(srcPath) && ff.exists(dstPath)) {
                        if (ff.copy(srcPath, dstPath) < 0) {
                            LOG.critical()
                                    .$("could not copy ").$(TableUtils.META_FILE_NAME).$(" file [src=").utf8(srcPath)
                                    .utf8(", dst=").utf8(dstPath)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
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
                                                .$("updated ").$(TXNLOG_FILE_NAME).$(" file [path=").utf8(dstPath)
                                                .$(", oldMaxTxn=").$(oldMaxTxn)
                                                .$(", newMaxTxn=").$(newMaxTxn)
                                                .$(']').$();
                                    }
                                }
                            } catch (CairoException ex) {
                                LOG.critical()
                                        .$("could not update file [src=").utf8(dstPath)
                                        .$("`, ex=").$(ex.getFlyweightMessage())
                                        .$(", errno=").$(ff.errno())
                                        .$(']').$();
                            }

                            recoveredWalFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered ").$(TableUtils.META_FILE_NAME).$(" file [src=").utf8(srcPath)
                                    .$(", dst=").utf8(dstPath)
                                    .$(']').$();
                        }
                    }
                }
            });
            LOG.info()
                    .$("snapshot recovery finished [metaFilesCount=").$(recoveredMetaFiles.get())
                    .$(", txnFilesCount=").$(recoveredTxnFiles.get())
                    .$(", cvFilesCount=").$(recoveredCVFiles.get())
                    .$(", walFilesCount=").$(recoveredWalFiles.get())
                    .$(']').$();

            // Delete snapshot directory to avoid recovery on next restart.
            srcPath.trimTo(snapshotRootLen).$();
            memFile.close();
            if (ff.rmdir(srcPath) != 0) {
                throw CairoException.critical(ff.errno())
                        .put("could not remove snapshot dir [dir=").put(srcPath)
                        .put(", errno=").put(ff.errno())
                        .put(']');
            }
        }
    }

    @TestOnly
    public void clear() {
        lock.lock();
        try {
            unsafeReleaseReaders();
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
            unsafeReleaseReaders();
            metadata.close();
        } finally {
            lock.unlock();
        }
    }

    public void completeSnapshot() throws SqlException {
        if (!lock.tryLock()) {
            throw SqlException.position(0).put("Another snapshot command in progress");
        }
        try {
            activePrepareFlag.set(false); // reset snapshot prepare flag
            if (snapshotReaders.size() == 0) {
                LOG.info().$("Snapshot has no tables, SNAPSHOT COMPLETE is ignored.").$();
            }

            // Delete snapshot/db directory.
            path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory()).$();
            ff.rmdir(path); // it's fine to ignore errors here

            // Release locked readers if any.
            unsafeReleaseReaders();
            // Resume the WalPurgeJob
            if (walPurgeJobRunLock != null) {
                try {
                    walPurgeJobRunLock.unlock();
                } catch (IllegalStateException ignore) {
                    // not an error here
                    // completeSnapshot can be called several time in a row.
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void prepareSnapshot(SqlExecutionContext executionContext) throws SqlException {
        // Windows doesn't support sync() system call.
        if (Os.isWindows()) {
            throw SqlException.position(0).put("Snapshots are not supported on Windows");
        }

        if (!lock.tryLock()) {
            throw SqlException.position(0).put("Another snapshot command in progress");
        }
        try {
            // activePrepareFlag is used to detect active snapshot prepare called on an empty DB
            if (activePrepareFlag.get()) {
                throw SqlException.position(0).put("Waiting for SNAPSHOT COMPLETE to be called");
            }

            path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory());
            int snapshotLen = path.length();
            // Delete all contents of the snapshot/db dir.
            if (ff.exists(path.slash$())) {
                path.trimTo(snapshotLen).$();
                if (ff.rmdir(path) != 0) {
                    throw CairoException.critical(ff.errno()).put("Could not remove snapshot dir [dir=").put(path).put(']');
                }
            }
            // Recreate the snapshot/db dir.
            path.trimTo(snapshotLen).slash$();
            if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
                throw CairoException.critical(ff.errno()).put("Could not create [dir=").put(path).put(']');
            }

            try (TableListRecordCursorFactory factory = new TableListRecordCursorFactory()) {
                final int tableNameIndex = factory.getMetadata().getColumnIndex(TableListRecordCursorFactory.TABLE_NAME_COLUMN);
                try (RecordCursor cursor = factory.getCursor(executionContext)) {
                    final Record record = cursor.getRecord();

                    // Suspend the WalPurgeJob
                    if (walPurgeJobRunLock != null) {
                        final long timeout = configuration.getCircuitBreakerConfiguration().getTimeout();
                        while (!walPurgeJobRunLock.tryLock(timeout, TimeUnit.MICROSECONDS)) {
                            executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                        }
                    }

                    try (MemoryCMARW mem = Vm.getCMARWInstance()) {
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

                                TableReader reader = engine.getReaderWithRepair(tableToken);
                                snapshotReaders.add(reader);

                                path.trimTo(snapshotLen).concat(tableToken);
                                int rootLen = path.length();

                                if (isWalTable) {
                                    path.concat(WalUtils.SEQ_DIR);
                                }
                                if (ff.mkdirs(path.slash$(), configuration.getMkDirMode()) != 0) {
                                    throw CairoException.critical(ff.errno()).put("Could not create [dir=").put(path).put(']');
                                }

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

                                if (isWalTable) {
                                    metadata.clear();
                                    long lastTxn = engine.getTableSequencerAPI().getTableMetadata(tableToken, metadata);
                                    path.trimTo(rootLen).concat(WalUtils.SEQ_DIR);
                                    metadata.switchTo(path, path.length()); // dump sequencer metadata to snapshot/db/tableName/txn_seq/_meta
                                    metadata.close(Vm.TRUNCATE_TO_POINTER);

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

                        activePrepareFlag.set(true);
                        LOG.info().$("snapshot copying finished").$();
                    } catch (Throwable e) {
                        // Resume the WalPurgeJob
                        if (walPurgeJobRunLock != null) {
                            walPurgeJobRunLock.unlock();
                        }
                        unsafeReleaseReaders();
                        LOG.error()
                                .$("snapshot error [e=").$(e)
                                .I$();
                        throw e;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void setWalPurgeJobRunLock(@Nullable SimpleWaitingLock walPurgeJobRunLock) {
        this.walPurgeJobRunLock = walPurgeJobRunLock;
    }

    private void unsafeReleaseReaders() {
        Misc.freeObjListAndClear(snapshotReaders);
    }
}
