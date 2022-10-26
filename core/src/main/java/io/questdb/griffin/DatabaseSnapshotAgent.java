/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.engine.table.TableListRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DatabaseSnapshotAgent implements Closeable {

    private final static Log LOG = LogFactory.getLog(DatabaseSnapshotAgent.class);

    private final CairoEngine engine;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final ReentrantLock lock = new ReentrantLock(); // protects below fields
    private final Path path = new Path();
    // List of readers kept around to lock partitions while a database snapshot is being made.
    private final ObjList<TableReader> snapshotReaders = new ObjList<>();

    public DatabaseSnapshotAgent(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
    }

    @Override
    public void close() {
        lock.lock();
        try {
            Misc.free(path);
            unsafeReleaseReaders();
        } finally {
            lock.unlock();
        }
    }

    @TestOnly
    public void clear() {
        lock.lock();
        try {
            unsafeReleaseReaders();
        } finally {
            lock.unlock();
        }
    }

    private void unsafeReleaseReaders() {
        Misc.freeObjList(snapshotReaders);
        snapshotReaders.clear();
    }

    public static void recoverSnapshot(CairoEngine engine) {
        final CairoConfiguration configuration = engine.getConfiguration();
        if (!configuration.isSnapshotRecoveryEnabled()) {
            return;
        }

        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();
        final CharSequence snapshotRoot = configuration.getSnapshotRoot();

        try (Path path = new Path(); Path copyPath = new Path()) {
            path.of(snapshotRoot).concat(configuration.getDbDirectory());
            final int snapshotRootLen = path.length();
            copyPath.of(root);
            final int rootLen = copyPath.length();

            // Check if the snapshot dir exists.
            if (!ff.exists(path.slash$())) {
                return;
            }

            // Check if the snapshot metadata file exists.
            path.trimTo(snapshotRootLen).concat(TableUtils.SNAPSHOT_META_FILE_NAME).$();
            if (!ff.exists(path)) {
                return;
            }

            // Check if the snapshot instance id is different from what's in the snapshot.
            try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);

                final CharSequence currentInstanceId = configuration.getSnapshotInstanceId();
                final CharSequence snapshotInstanceId = mem.getStr(0);
                if (Chars.empty(currentInstanceId) || Chars.empty(snapshotInstanceId) || Chars.equals(currentInstanceId, snapshotInstanceId)) {
                    return;
                }

                LOG.info()
                        .$("starting snapshot recovery [currentId=`").$(currentInstanceId)
                        .$("`, previousId=`").$(snapshotInstanceId)
                        .$("`]").$();
            }

            // OK, we need to recover from the snapshot.
            AtomicInteger recoveredMetaFiles = new AtomicInteger();
            AtomicInteger recoveredTxnFiles = new AtomicInteger();
            AtomicInteger recoveredCVFiles = new AtomicInteger();
            path.trimTo(snapshotRootLen).$();
            final int snapshotDbLen = path.length();
            ff.iterateDir(path, (pUtf8NameZ, type) -> {
                if (Files.isDir(pUtf8NameZ, type)) {
                    path.trimTo(snapshotDbLen).concat(pUtf8NameZ);
                    copyPath.trimTo(rootLen).concat(pUtf8NameZ);
                    final int plen = path.length();
                    final int cplen = copyPath.length();

                    path.concat(TableUtils.META_FILE_NAME).$();
                    copyPath.concat(TableUtils.META_FILE_NAME).$();
                    if (ff.exists(path) && ff.exists(copyPath)) {
                        if (ff.copy(path, copyPath) < 0) {
                            LOG.error()
                                    .$("could not copy _meta file [src=").$(path)
                                    .$(", dst=").$(copyPath)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        } else {
                            recoveredMetaFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered _meta file [src=").$(path)
                                    .$(", dst=").$(copyPath)
                                    .$(']').$();
                        }
                    }

                    path.trimTo(plen).concat(TableUtils.TXN_FILE_NAME).$();
                    copyPath.trimTo(cplen).concat(TableUtils.TXN_FILE_NAME).$();
                    if (ff.exists(path) && ff.exists(copyPath)) {
                        if (ff.copy(path, copyPath) < 0) {
                            LOG.error()
                                    .$("could not copy _txn file [src=").$(path)
                                    .$(", dst=").$(copyPath)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        } else {
                            recoveredTxnFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered _txn file [src=").$(path)
                                    .$(", dst=").$(copyPath)
                                    .$(']').$();
                        }
                    }

                    path.trimTo(plen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                    copyPath.trimTo(cplen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                    if (ff.exists(path) && ff.exists(copyPath)) {
                        if (ff.copy(path, copyPath) < 0) {
                            LOG.error()
                                    .$("could not copy _cv file [src=").$(path)
                                    .$(", dst=").$(copyPath)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        } else {
                            recoveredCVFiles.incrementAndGet();
                            LOG.info()
                                    .$("recovered _cv file [src=").$(path)
                                    .$(", dst=").$(copyPath)
                                    .$(']').$();
                        }
                    }
                }
            });
            LOG.info()
                    .$("snapshot recovery finished [metaFilesCount=").$(recoveredMetaFiles.get())
                    .$(", txnFilesCount=").$(recoveredTxnFiles.get())
                    .$(", cvFilesCount=").$(recoveredCVFiles.get())
                    .$(']').$();

            // Delete snapshot directory to avoid recovery on next restart.
            path.trimTo(snapshotRootLen).$();
            if (ff.rmdir(path) != 0) {
                throw CairoException.critical(ff.errno())
                        .put("could not remove snapshot dir [dir=").put(path)
                        .put(", errno=").put(ff.errno())
                        .put(']');
            }
        }
    }

    public void completeSnapshot() throws SqlException {
        if (!lock.tryLock()) {
            throw SqlException.position(0).put("Another snapshot command in progress");
        }
        try {
            if (snapshotReaders.size() == 0) {
                LOG.info().$("Snapshot has no tables, SNAPSHOT COMPLETE is ignored.").$();
                return;
            }

            // Delete snapshot/db directory.
            path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory()).$();
            ff.rmdir(path); // it's fine to ignore errors here

            // Release locked readers if any.
            unsafeReleaseReaders();
        } finally {
            lock.unlock();
        }
    }

    public void prepareSnapshot(SqlExecutionContext executionContext) throws SqlException {
        // Windows doesn't support sync() system call.
        if (Os.type == Os.WINDOWS) {
            throw SqlException.position(0).put("Snapshots are not supported on Windows");
        }

        if (!lock.tryLock()) {
            throw SqlException.position(0).put("Another snapshot command in progress");
        }
        try {
            if (snapshotReaders.size() > 0) {
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

            try (
                    TableListRecordCursorFactory factory = new TableListRecordCursorFactory(configuration.getFilesFacade(), configuration.getRoot())
            ) {
                final int tableNameIndex = factory.getMetadata().getColumnIndex(TableListRecordCursorFactory.TABLE_NAME_COLUMN);
                try (RecordCursor cursor = factory.getCursor(executionContext)) {
                    final Record record = cursor.getRecord();
                    try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                        // Copy metadata files for all tables.

                        while (cursor.hasNext()) {
                            CharSequence tableName = record.getStr(tableNameIndex);
                            if (
                                    TableUtils.isValidTableName(tableName, tableName.length())
                                            && ff.exists(path.of(configuration.getRoot()).concat(tableName).concat(TableUtils.META_FILE_NAME).$())
                            ) {
                                path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory());
                                LOG.info().$("preparing for snapshot [table=").$(tableName).I$();

                                TableReader reader = engine.getReaderForStatement(executionContext, tableName, "snapshot");
                                snapshotReaders.add(reader);

                                path.trimTo(snapshotLen).concat(tableName).slash$();
                                if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
                                    throw CairoException.critical(ff.errno()).put("Could not create [dir=").put(path).put(']');
                                }

                                int rootLen = path.length();
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
                    } catch (Throwable e) {
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
}
