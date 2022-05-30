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

import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.NullMemoryMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import java.io.Closeable;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class WalReader implements Closeable, SymbolTableSource {
    private static final Log LOG = LogFactory.getLog(WalReader.class);

    private final FilesFacade ff;
    private final Path path;
    private final int rootLen;
    private final WalReaderMetadata metadata;
    private final WalReaderRecordCursor recordCursor = new WalReaderRecordCursor();
    private final String tableName;
    private final String walName;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final CairoConfiguration configuration;
    private final MemoryMR todoMem = Vm.getMRInstance();
    private final long segmentId;
    private final long walRowCount;
    private int partitionCount;
    private LongList columnTops;
    private ObjList<MemoryMR> columns;
    private ObjList<BitmapIndexReader> bitmapIndexes;
    private int columnCount;
    private int columnCountShl;
    private long tempMem8b = Unsafe.malloc(8, MemoryTag.NATIVE_TABLE_READER);

    public WalReader(CairoConfiguration configuration, CharSequence tableName, CharSequence walName, long segmentId, IntList walSymbolCounts, long walRowCount) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.tableName = Chars.toString(tableName);
        this.walName = Chars.toString(walName);
        this.segmentId = segmentId;
        this.walRowCount = walRowCount;
        this.path = new Path();
        this.path.of(configuration.getRoot()).concat(this.tableName).concat(walName);
        this.rootLen = path.length();
        try {
            this.metadata = openMetaFile();
            this.columnCount = this.metadata.getColumnCount();
            this.columnCountShl = getColumnBits(columnCount);
            LOG.debug().$("open [table=").$(this.tableName).I$();
            openSymbolMaps(walSymbolCounts);
            partitionCount = 1;

            int capacity = getColumnBase(partitionCount);
            this.columns = new ObjList<>(capacity);
            this.columns.setPos(capacity + 2);
            this.columns.setQuick(0, NullMemoryMR.INSTANCE);
            this.columns.setQuick(1, NullMemoryMR.INSTANCE);
            this.bitmapIndexes = new ObjList<>(capacity);
            this.bitmapIndexes.setPos(capacity + 2);
            this.columnTops = new LongList(capacity / 2);
            this.columnTops.setPos(capacity / 2);
            this.recordCursor.of(this);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public static int getPrimaryColumnIndex(int base, int index) {
        return 2 + base + index * 2;
    }

    @Override
    public void close() {
        if (isOpen()) {
            freeSymbolMapReaders();
            freeBitmapIndexCache();
            Misc.free(metadata);
            Misc.free(todoMem);
            freeColumns();
            freeTempMem();
            Misc.free(path);
            LOG.debug().$("closed '").utf8(tableName).$('\'').$();
        }
    }

    public MemoryR getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
    }

    public int getColumnBase(int partitionIndex) {
        return partitionIndex << columnCountShl;
    }

    public long getColumnTop(int base, int columnIndex) {
        return this.columnTops.getQuick(base / 2 + columnIndex);
    }

    public String getWalName() {
        return walName;
    }

    public WalReaderRecordCursor getCursor() {
        recordCursor.toTop();
        return recordCursor;
    }

    long getPartitionRowCount(int partitionIndex) {
        return -1;
    }

    public long openPartition(int partitionIndex) {
        final long size = getPartitionRowCount(partitionIndex);
        if (size != -1) {
            return size;
        }
        return openPartition0(partitionIndex);
    }

    private long openPartition0(int partitionIndex) {
        path.slash().put(segmentId);
        try {
            if (ff.exists(path.$())) {
                path.chop$();
                openPartitionColumns(path, getColumnBase(partitionIndex), walRowCount);
                return walRowCount;
            }
            LOG.error().$("open partition failed, partition does not exist on the disk. [path=").utf8(path.$()).I$();
            throw CairoException.instance(0).put("Table '").put(tableName)
                    .put("' data directory does not exist on the disk at ")
                    .put(path)
                    .put(". Restore data on disk or drop the table.");
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartitionColumns(Path path, int columnBase, long partitionRowCount) {
        for (int i = 0; i < columnCount; i++) {
            reloadColumnAt(
                    path,
                    this.columns,
                    this.columnTops,
                    this.bitmapIndexes,
                    columnBase,
                    i,
                    partitionRowCount
            );
        }
    }

    private void reloadColumnAt(
            Path path,
            ObjList<MemoryMR> columns,
            LongList columnTops,
            ObjList<BitmapIndexReader> indexReaders,
            int columnBase,
            int columnIndex,
            long partitionRowCount
    ) {
        final int plen = path.length();
        try {
            final CharSequence name = metadata.getColumnName(columnIndex);
            final int primaryIndex = getPrimaryColumnIndex(columnBase, columnIndex);
            final int secondaryIndex = primaryIndex + 1;

            MemoryMR mem1 = columns.getQuick(primaryIndex);
            MemoryMR mem2 = columns.getQuick(secondaryIndex);

            final long columnTop = 0L;
            long columnTxn = COLUMN_NAME_TXN_NONE;
            final long columnRowCount = partitionRowCount - columnTop;
            assert partitionRowCount < 0 || columnRowCount >= 0;

            // When column is added mid-table existence the top record is only
            // created in the current partition. Older partitions would simply have no
            // column file. This makes it necessary to check the partition timestamp in Column Version file
            // of when the column was added.
            if (partitionRowCount > 0) {
                final int columnType = metadata.getColumnType(columnIndex);

                if (ColumnType.isVariableLength(columnType)) {
                    long columnSize = columnRowCount * 8L + 8L;
                    TableUtils.iFile(path.trimTo(plen), name, columnTxn);
                    mem2 = openOrCreateMemory(path, columns, secondaryIndex, mem2, columnSize);
                    columnSize = mem2.getLong(columnRowCount * 8L);
                    TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                    openOrCreateMemory(path, columns, primaryIndex, mem1, columnSize);
                } else {
                    long columnSize = columnRowCount << ColumnType.pow2SizeOf(columnType);
                    TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                    openOrCreateMemory(path, columns, primaryIndex, mem1, columnSize);
                    Misc.free(columns.getAndSetQuick(secondaryIndex, null));
                }

                columnTops.setQuick(columnBase / 2 + columnIndex, columnTop);

                if (metadata.isColumnIndexed(columnIndex)) {
                    BitmapIndexReader indexReader = indexReaders.getQuick(primaryIndex);
                    if (indexReader instanceof BitmapIndexBwdReader) {
                        // name txn is -1 because the parent call sets up partition name for us
                        ((BitmapIndexBwdReader) indexReader).of(configuration, path.trimTo(plen), name, columnTxn, columnTop, -1);
                    }

                    indexReader = indexReaders.getQuick(secondaryIndex);
                    if (indexReader instanceof BitmapIndexFwdReader) {
                        ((BitmapIndexFwdReader) indexReader).of(configuration, path.trimTo(plen), name, columnTxn, columnTop, -1);
                    }

                } else {
                    Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                    Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));
                }
            } else {
                Misc.free(columns.getAndSetQuick(primaryIndex, NullMemoryMR.INSTANCE));
                Misc.free(columns.getAndSetQuick(secondaryIndex, NullMemoryMR.INSTANCE));
                // the appropriate index for NUllColumn will be created lazily when requested
                // these indexes have state and may not be always required
                Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));
            }
        } finally {
            path.trimTo(plen);
        }
    }

    public long size() {
        return -1;
    }

    public long getMaxTimestamp() {
        return -1;
    }

    public long getMinTimestamp() {
        return -1;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex);
    }

    public SymbolMapDiff getSymbolMapDiff(int columnIndex) {
        return metadata.getSymbolMapDiff(columnIndex);
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex);
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex).newSymbolTableView();
    }

    public String getTableName() {
        return tableName;
    }

    public long getTransientRowCount() {
        return walRowCount;
    }

    public long getVersion() {
        return -1;
    }

    public boolean isOpen() {
        return tempMem8b != 0;
    }

    private static int getColumnBits(int columnCount) {
        return Numbers.msb(Numbers.ceilPow2(columnCount) * 2);
    }

    private void freeBitmapIndexCache() {
        Misc.freeObjList(bitmapIndexes);
    }

    private void freeColumns() {
        Misc.freeObjList(columns);
    }

    private void freeSymbolMapReaders() {
        for (int i = 0, n = symbolMapReaders.size(); i < n; i++) {
            Misc.free(symbolMapReaders.getQuick(i));
        }
        symbolMapReaders.clear();
    }

    private void freeTempMem() {
        if (tempMem8b != 0) {
            Unsafe.free(tempMem8b, 8, MemoryTag.NATIVE_TABLE_READER);
            tempMem8b = 0;
        }
    }

    int getColumnCount() {
        return columnCount;
    }

    int getPartitionIndex(int columnBase) {
        return columnBase >>> columnCountShl;
    }

    private void handleMetadataLoadException(long deadline, CairoException ex) {
        // This is temporary solution until we can get multiple version of metadata not overwriting each other
        if (isMetaFileMissingFileSystemError(ex)) {
            if (configuration.getMicrosecondClock().getTicks() < deadline) {
                LOG.info().$("error reloading metadata [table=").$(tableName)
                        .$(", errno=").$(ex.getErrno())
                        .$(", error=").$(ex.getFlyweightMessage()).I$();
                Os.pause();
            } else {
                LOG.error().$("metadata read timeout [timeout=").$(configuration.getSpinLockTimeoutUs()).utf8("μs]").$();
                throw CairoException.instance(ex.getErrno()).put("Metadata read timeout. Last error: ").put(ex.getFlyweightMessage());
            }
        } else {
            throw ex;
        }
    }

    private boolean isMetaFileMissingFileSystemError(CairoException ex) {
        int errno = ex.getErrno();
        return errno == CairoException.ERRNO_FILE_DOES_NOT_EXIST || errno == CairoException.METADATA_VALIDATION;
    }

    private WalReaderMetadata openMetaFile() {
        final long deadline = this.configuration.getMicrosecondClock().getTicks() + this.configuration.getSpinLockTimeoutUs();
        final WalReaderMetadata metadata = new WalReaderMetadata(ff, segmentId);
        try {
            while (true) {
                try {
                    return metadata.of(path, 0);
                } catch (CairoException ex) {
                    handleMetadataLoadException(deadline, ex);
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    @NotNull
    private MemoryMR openOrCreateMemory(
            Path path,
            ObjList<MemoryMR> columns,
            int primaryIndex,
            MemoryMR mem,
            long columnSize
    ) {
        if (mem != null && mem != NullMemoryMR.INSTANCE) {
            mem.of(ff, path, columnSize, columnSize, MemoryTag.MMAP_TABLE_WAL_READER);
        } else {
            mem = Vm.getMRInstance(ff, path, columnSize, MemoryTag.MMAP_TABLE_WAL_READER);
            columns.setQuick(primaryIndex, mem);
        }
        return mem;
    }

    private void openSymbolMaps(IntList walSymbolCounts) {
        final int columnCount = metadata.getColumnCount();
        int symbolMapIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                // symbol map index array is sparse
                symbolMapReaders.extendAndSet(i, new SymbolMapReaderImpl(
                        configuration,
                        path,
                        metadata.getColumnName(i),
                        COLUMN_NAME_TXN_NONE,
                        walSymbolCounts.get(symbolMapIndex++)
                ));
            }
        }
    }
}
