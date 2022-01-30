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
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.lockName;

/**
 * Rebuild index independently of TableWriter
 * Main purpose is for support cases when table data is corrupt and TableWriter cannot be opened
 */
public class RebuildIndex implements Closeable, Mutable {
    private static final int ALL = -1;
    private final Path path = new Path();
    private final MemoryMAR ddlMem = Vm.getMARInstance();

    private int rootLen;
    private CairoConfiguration configuration;
    private long lockFd;
    private final MemoryMR indexMem = Vm.getMRInstance();
    private static final Log LOG = LogFactory.getLog(RebuildIndex.class);
    private TableReaderMetadata metadata;
    private final SymbolColumnIndexer indexer = new SymbolColumnIndexer();
    private final StringSink tempStringSink = new StringSink();

    public RebuildIndex of(CharSequence tablePath, CairoConfiguration configuration) {
        this.path.concat(tablePath);
        this.rootLen = tablePath.length();
        this.configuration = configuration;
        return this;
    }

    @Override
    public void clear() {
        path.trimTo(0);
        tempStringSink.clear();
    }

    public void rebuildAll() {
        rebuildPartitionColumn(null, null);
    }

    public void rebuildColumn(CharSequence columnName) {
        rebuildPartitionColumn(null, columnName);
    }

    public void rebuildPartition(CharSequence rebuildPartitionName) {
        rebuildPartitionColumn(rebuildPartitionName, null);
    }

    public void rebuildPartitionColumn(CharSequence rebuildPartitionName, CharSequence rebuildColumn) {
        FilesFacade ff = configuration.getFilesFacade();
        path.trimTo(rootLen);
        path.concat(TableUtils.META_FILE_NAME);
        if (metadata == null) {
            metadata = new TableReaderMetadata(ff);
        }
        metadata.of(path.$(), ColumnType.VERSION);
        try {
            lock(ff);

            // Resolve column id if the column name specified
            int rebuildColumnIndex = ALL;
            if (rebuildColumn != null) {
                rebuildColumnIndex = metadata.getColumnIndexQuiet(rebuildColumn, 0, rebuildColumn.length());
                if (rebuildColumnIndex < 0) {
                    throw CairoException.instance(0).put("Column does not exist");
                }
            }

            path.trimTo(rootLen);
            int partitionBy = metadata.getPartitionBy();
            DateFormat partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);

            try (TxReader txReader = new TxReader(ff).ofRO(path, partitionBy)) {
                TableUtils.unsafeReadTxFile(txReader);
                path.trimTo(rootLen);

                if (PartitionBy.isPartitioned(partitionBy)) {
                    // Resolve partition timestamp if partition name specified
                    long rebuildPartitionTs = ALL;
                    if (rebuildPartitionName != null) {
                        rebuildPartitionTs = PartitionBy.parsePartitionDirName(rebuildPartitionName, partitionBy);
                    }

                    for (int partitionIndex = txReader.getPartitionCount() - 1; partitionIndex > -1; partitionIndex--) {
                        long partitionTimestamp = txReader.getPartitionTimestamp(partitionIndex);
                        if (rebuildPartitionTs == ALL || partitionTimestamp == rebuildPartitionTs) {
                            long partitionSize = txReader.getPartitionSize(partitionIndex);
                            rebuildIndex(
                                    rebuildColumnIndex,
                                    ff,
                                    indexer,
                                    metadata,
                                    partitionDirFormatMethod,
                                    tempStringSink,
                                    partitionTimestamp,
                                    partitionSize);
                        }
                    }
                } else {
                    long partitionSize = txReader.getTransientRowCount();
                    rebuildIndex(rebuildColumnIndex, ff, indexer, metadata, partitionDirFormatMethod, tempStringSink, 0, partitionSize);
                }
            }
        } finally {
            metadata.close();
            indexer.clear();
            path.trimTo(rootLen);
            lockName(path);
            releaseLock(ff);
        }
    }

    private void rebuildIndex(
            int rebuildColumnIndex,
            FilesFacade ff,
            SymbolColumnIndexer indexer,
            TableReaderMetadata metadata,
            DateFormat partitionDirFormatMethod,
            StringSink sink,
            long partitionTimestamp,
            long partitionSize
    ) {
        sink.clear();
        partitionDirFormatMethod.format(partitionTimestamp, null, null, sink);

        if (rebuildColumnIndex == ALL) {
            for (int columnIndex = metadata.getColumnCount() - 1; columnIndex > -1; columnIndex--) {
                if (metadata.isColumnIndexed(columnIndex)) {
                    CharSequence columnName = metadata.getColumnName(columnIndex);
                    int indexValueBlockCapacity = metadata.getIndexValueBlockCapacity(columnIndex);
                    rebuildIndex(indexer, columnName, sink, indexValueBlockCapacity, partitionSize, ff);
                }
            }
        } else {
            if (metadata.isColumnIndexed(rebuildColumnIndex)) {
                CharSequence columnName = metadata.getColumnName(rebuildColumnIndex);
                int indexValueBlockCapacity = metadata.getIndexValueBlockCapacity(rebuildColumnIndex);
                rebuildIndex(indexer, columnName, sink, indexValueBlockCapacity, partitionSize, ff);
            } else {
                throw CairoException.instance(0).put("Column is not indexed");
            }
        }
    }

    private void rebuildIndex(
            SymbolColumnIndexer indexer,
            CharSequence columnName,
            CharSequence partitionName,
            int indexValueBlockCapacity,
            long partitionSize,
            FilesFacade ff
    ) {
        path.trimTo(rootLen).concat(partitionName);
        final int plen = path.length();

        if (ff.exists(path.$())) {
            try (final MemoryMR roMem = indexMem) {
                removeIndexFiles(columnName, ff);
                TableUtils.dFile(path.trimTo(plen), columnName);

                if (ff.exists(path.$())) {
                    LOG.info().$("indexing [path=").utf8(path).I$();
                    final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), columnName, plen, false);
                    createIndexFiles(columnName, indexValueBlockCapacity, plen, ff);

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
    }

    private void createIndexFiles(CharSequence columnName, int indexValueBlockCapacity, int plen, FilesFacade ff) {
        try {
            BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
            try {
                LOG.info().$("writing ").utf8(path).$();
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
                LOG.error().$("could not create index [name=").utf8(path).$(']').$();
                throw CairoException.instance(ff.errno()).put("could not create index [name=").put(path).put(']');
            }
            LOG.info().$("writing ").utf8(path).$();
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
        LOG.info().$("deleting ").utf8(path).$();
        if (!ff.remove(this.path)) {
            if (!ff.exists(this.path)) {
                // This is fine, index can be corrupt, rewriting is what we try to do here
                LOG.info().$("index file did not exist, file will be re-written [path=").utf8(path).I$();
            } else {
                throw CairoException.instance(ff.errno()).put("cannot remove index file");
            }
        }
    }

    private void lock(FilesFacade ff) {
        try {
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
        this.path.close();
        Misc.free(metadata);
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
