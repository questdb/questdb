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

public class DatabaseSnapshotAgent implements Closeable {

    private final static Log LOG = LogFactory.getLog(DatabaseSnapshotAgent.class);

    private final CairoEngine engine;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final Path path = new Path();
    private boolean snapshotInProgress;
    // List of readers kept around to lock partitions while a database snapshot is being made.
    private final ObjList<TableReader> snapshotReaders = new ObjList<>();

    public DatabaseSnapshotAgent(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
    }

    @Override
    public synchronized void close() {
        Misc.free(path);
        releaseReaders();
    }

    @TestOnly
    public synchronized void clear() {
        snapshotInProgress = false;
        releaseReaders();
    }

    @TestOnly
    public synchronized void releaseReaders() {
        Misc.freeObjList(snapshotReaders);
        snapshotReaders.clear();
    }

    public synchronized void prepareSnapshot(SqlExecutionContext executionContext) throws SqlException {
        // Windows doesn't support sync() system call.
        if (Os.type == Os.WINDOWS) {
            throw SqlException.position(0).put("Snapshots are not supported on Windows");
        }

        if (snapshotInProgress) {
            throw SqlException.position(0).put("Another snapshot command in progress");
        }
        snapshotInProgress = true;

        path.of(configuration.getSnapshotRoot());
        int snapshotLen = path.length();
        // Delete all contents of the snapshot dir.
        if (ff.exists(path.slash$())) {
            path.trimTo(snapshotLen).$();
            if (ff.rmdir(path) != 0) {
                throw CairoException.instance(ff.errno()).put("Could not remove snapshot dir [dir=").put(path).put(']');
            }
        }
        // Recreate the snapshot dir.
        path.trimTo(snapshotLen).slash$();
        if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
            throw CairoException.instance(ff.errno()).put("Could not create [dir=").put(path).put(']');
        }

        try (
                TableListRecordCursorFactory factory = new TableListRecordCursorFactory(configuration.getFilesFacade(), configuration.getRoot())
        ) {
            final int tableNameIndex = factory.getMetadata().getColumnIndex(TableListRecordCursorFactory.TABLE_NAME_COLUMN);
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                final Record record = cursor.getRecord();
                // We need directory's fd to fsync it later.
                path.trimTo(snapshotLen).$();
                final long snapshotDirFd = !ff.isRestrictedFileSystem() ? TableUtils.openRO(ff, path, LOG) : 0;
                try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                    while (cursor.hasNext()) {
                        CharSequence tableName = record.getStr(tableNameIndex);
                        TableReader reader = engine.getReaderForStatement(executionContext, tableName, "snapshot");
                        snapshotReaders.add(reader);

                        path.trimTo(snapshotLen).concat(configuration.getDbDirectory()).concat(tableName).slash$();
                        if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
                            throw CairoException.instance(ff.errno()).put("Could not create [dir=").put(path).put(']');
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
                        LOG.info().$("snapshot copied [table=").$(tableName).$(']').$();
                    }

                    // Flush dirty pages and filesystem metadata to disk
                    if (ff.sync() != 0) {
                        throw CairoException.instance(ff.errno()).put("Could not sync");
                    }

                    // Write instance id to the snapshot metadata file.
                    path.trimTo(snapshotLen).concat(TableUtils.SNAPSHOT_META_FILE_NAME).$();
                    mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                    mem.putStr(0, configuration.getSnapshotInstanceId());
                    mem.sync(false);
                    mem.close(false);

                    // It is important to fsync parent directory's metadata to make sure that
                    // the snapshot metadata file is included into the snapshot.
                    if (snapshotDirFd > 0) {
                        if (ff.fsync(snapshotDirFd) != 0) {
                            LOG.error()
                                    .$("could not fsync [fd=").$(snapshotDirFd)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        }
                    }
                } catch (Throwable e) {
                    Misc.freeObjList(snapshotReaders);
                    snapshotReaders.clear();
                    LOG.error()
                            .$("snapshot prepare error [e=").$(e)
                            .I$();
                    throw e;
                } finally {
                    if (snapshotDirFd > 0) {
                        ff.close(snapshotDirFd);
                    }
                }
            }
        }
    }

    public synchronized void completeSnapshot() throws SqlException {
        if (!snapshotInProgress) {
            throw SqlException.position(0).put("SNAPSHOT PREPARE must be called before SNAPSHOT COMPLETE");
        }

        // Delete snapshot directory.
        path.of(configuration.getSnapshotRoot()).$();
        ff.rmdir(path); // it's fine to ignore errors here

        // Release locked readers if any.
        Misc.freeObjList(snapshotReaders);
        snapshotReaders.clear();

        snapshotInProgress = false;
    }

    public static void recoverSnapshot(CairoEngine engine) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();
        final CharSequence snapshotRoot = configuration.getSnapshotRoot();

        try (Path path = new Path(); Path copyPath = new Path()) {
            path.of(snapshotRoot);
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
                if (Chars.equals(currentInstanceId, snapshotInstanceId)) {
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
            path.trimTo(snapshotRootLen).concat(configuration.getDbDirectory()).$();
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
                                    .$("could not copy snapshot _meta file [src=").$(path)
                                    .$(", dst=").$(copyPath)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        } else {
                            recoveredMetaFiles.incrementAndGet();
                        }
                    }

                    path.trimTo(plen).concat(TableUtils.TXN_FILE_NAME).$();
                    copyPath.trimTo(cplen).concat(TableUtils.TXN_FILE_NAME).$();
                    if (ff.exists(path) && ff.exists(copyPath)) {
                        if (ff.copy(path, copyPath) < 0) {
                            LOG.error()
                                    .$("could not copy snapshot _txn file [src=").$(path)
                                    .$(", dst=").$(copyPath)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        } else {
                            recoveredTxnFiles.incrementAndGet();
                        }
                    }
                }
            });
            LOG.info()
                    .$("snapshot recovery finished [metaFilesCount=").$(recoveredMetaFiles.get())
                    .$(", txnFilesCount=").$(recoveredTxnFiles.get())
                    .$(']').$();

            // Delete snapshot directory to avoid recovery on next restart.
            path.trimTo(snapshotRootLen).$();
            if (ff.rmdir(path) != 0) {
                LOG.error()
                        .$("could not remove snapshot dir [dir=").$(path)
                        .$(", errno=").$(ff.errno())
                        .$(']').$();
            }
        }
    }
}
