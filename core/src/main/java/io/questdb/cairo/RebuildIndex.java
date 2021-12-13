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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.lockName;

public class RebuildIndex implements Closeable, Mutable {
    private final Path path = new Path();
    private final MemoryMAR ddlMem = Vm.getMARInstance();

    private int rootLen;
    private CairoConfiguration configuration;
    private long lockFd;
    private final MemoryMR indexMem = Vm.getMRInstance();
    private static final Log LOG = LogFactory.getLog(RebuildIndex.class);
    private FilesFacade unlockFilesFacade;

    public RebuildIndex of(String tablePath, CairoConfiguration configuration) {
        this.path.concat(tablePath);
        this.rootLen = tablePath.length();
        this.configuration = configuration;
        return this;
    }

    @Override
    public void clear() {
        path.trimTo(0);
    }

    public void rebuildAllPartitions() {
        FilesFacade ff = configuration.getFilesFacade();
        lock(ff);
        path.trimTo(rootLen);

        SymbolColumnIndexer indexer = new SymbolColumnIndexer();

        try (TableReaderMetadata metadata = new TableReaderMetadata(ff)) {
            path.concat(TableUtils.META_FILE_NAME);
            metadata.of(path, ColumnType.VERSION);
            path.trimTo(rootLen);

            int partitionBy = metadata.getPartitionBy();
            DateFormat partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);
            StringSink sink = new StringSink();

            try (TxReader txReader = new TxReader(ff, path, partitionBy)) {
                txReader.unsafeLoadAll();
                path.trimTo(rootLen);

                if (PartitionBy.isPartitioned(partitionBy)) {
                    for (int partitionIndex = txReader.getPartitionCount() - 1; partitionIndex > -1; partitionIndex--) {
                        long partitionTimestamp = txReader.getPartitionTimestamp(partitionIndex);
                        long partitionSize = txReader.getPartitionSize(partitionIndex);
                        sink.clear();
                        partitionDirFormatMethod.format(partitionTimestamp, null, null, sink);

                        for (int columnIndex = metadata.getColumnCount() - 1; columnIndex > -1; columnIndex--) {
                            if (metadata.isColumnIndexed(columnIndex)) {
                                CharSequence columnName = metadata.getColumnName(columnIndex);
                                rebuildIndex(indexer, columnName, sink.toString(), metadata.getIndexValueBlockCapacity(columnIndex), partitionSize, ff);
                            }
                        }
                    }
                }
            }
        }
    }

    private void rebuildIndex(SymbolColumnIndexer indexer, CharSequence columnName, String partitionName, int indexValueBlockCapacity, long partitionSize, FilesFacade ff) {
        path.trimTo(rootLen).concat(partitionName);
        final int plen = path.length();

        if (ff.exists(path.$())) {

            try (final MemoryMR roMem = indexMem) {
                LOG.info().$("indexing [path=").$(path).I$();

                removeIndexFiles(columnName, ff);

                TableUtils.dFile(path.trimTo(plen), columnName);
                createIndexFiles(columnName, indexValueBlockCapacity, plen, ff);
                final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), columnName, plen, false);

                if (partitionSize > columnTop) {
                    TableUtils.dFile(path.trimTo(plen), columnName);
                    final long columnSize = (partitionSize - columnTop) << ColumnType.pow2SizeOf(ColumnType.INT);
                    roMem.of(ff, path, columnSize, columnSize, MemoryTag.MMAP_TABLE_WRITER);
                    indexer.configureWriter(configuration, path.trimTo(plen), columnName, columnTop);
                    indexer.index(roMem, columnTop, partitionSize);
                }
            }
        }

    }

    private void createIndexFiles(CharSequence columnName, int indexValueBlockCapacity, int plen, FilesFacade ff) {
        try {
            BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);

            // reuse memory column object to create index and close it at the end
            try {
                ddlMem.smallFile(ff, path, MemoryTag.MMAP_TABLE_WRITER);
                BitmapIndexWriter.initKeyMemory(ddlMem, indexValueBlockCapacity);
            } catch (CairoException e) {
                // looks like we could not create key file properly
                // lets not leave half-baked file sitting around
                LOG.error()
                        .$("could not create index [name=").utf8(path)
                        .$(", errno=").$(e.getErrno())
                        .$(']').$();
                if (!ff.remove(path)) {
                    LOG.error()
                            .$("could not remove '").utf8(path).$("'. Please remove MANUALLY.")
                            .$("[errno=").$(ff.errno())
                            .$(']').$();
                }
                throw e;
            } finally {
                ddlMem.close();
            }
            if (!ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName))) {
                LOG.error().$("could not create index [name=").$(path).$(']').$();
                throw CairoException.instance(ff.errno()).put("could not create index [name=").put(path).put(']');
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private void removeIndexFiles(CharSequence columnName, FilesFacade ff) {
        final int plen = path.length();
        BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
        removeFile(path, ff);

        BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
        removeFile(path, ff);
    }

    private void removeFile(Path path, FilesFacade ff) {
        LOG.info().$("deleting ").$(path).$();
        ff.remove(this.path);
    }

    private void lock(FilesFacade ff) {
        try {
            this.unlockFilesFacade = ff;
            path.trimTo(rootLen);
            lockName(path);
            this.lockFd = TableUtils.lock(ff, path);
        } finally {
            path.trimTo(rootLen);
        }

        if (this.lockFd == -1L) {
            throw CairoException.instance(ff.errno()).put("Cannot lock table: ").put(path.$());
        }
    }

    @Override
    public void close() {
        path.trimTo(rootLen);
        lockName(path);
        releaseLock(unlockFilesFacade);
        this.path.close();
    }

    private void releaseLock(FilesFacade ff) {
        if (lockFd != -1L) {
            ff.close(lockFd);
            try {
                path.trimTo(rootLen);
                lockName(path);
                if (ff.exists(path) && !ff.remove(path)) {
                    throw CairoException.instance(ff.errno()).put("Cannot remove ").put(path);
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }
}
