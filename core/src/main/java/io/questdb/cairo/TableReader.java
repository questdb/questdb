/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

public class TableReader implements Closeable, SymbolTableSource {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private final ColumnCopyStruct tempCopyStruct = new ColumnCopyStruct();
    private final FilesFacade ff;
    private final Path path;
    private final int rootLen;
    private final TableReaderMetadata metadata;
    private final DateFormat partitionFormat;
    private final LongList openPartitionTimestamp;
    private final LongList openPartitionSize;
    private final TableReaderRecordCursor recordCursor = new TableReaderRecordCursor();
    private final Timestamps.TimestampFloorMethod timestampFloorMethod;
    private final String tableName;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final CairoConfiguration configuration;
    private final IntList symbolCountSnapshot = new IntList();
    private final TxReader txFile;
    private int partitionCount;
    private LongList columnTops;
    private ObjList<ReadOnlyColumn> columns;
    private ObjList<BitmapIndexReader> bitmapIndexes;
    private int columnCount;
    private int columnCountBits;
    private long rowCount;
    private long txn = TableUtils.INITIAL_TXN;
    private long tempMem8b = Unsafe.malloc(8);

    public TableReader(CairoConfiguration configuration, CharSequence tableName) {
        LOG.info().$("open '").utf8(tableName).$('\'').$();
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.tableName = Chars.toString(tableName);
        this.path = new Path().of(configuration.getRoot()).concat(tableName);
        this.rootLen = path.length();
        try {
            failOnPendingTodo();
            this.txFile = new TxReader(ff, path);
            this.txFile.open();
            this.metadata = openMetaFile();
            this.columnCount = this.metadata.getColumnCount();
            this.columnCountBits = getColumnBits(columnCount);
            int partitionBy = this.metadata.getPartitionBy();
            txFile.initPartitionBy(partitionBy);
            readTxnSlow();
            openSymbolMaps();

            partitionCount = txFile.getPartitionsCount();
            partitionFormat = TableUtils.getPartitionDateFmt(partitionBy);
            timestampFloorMethod = partitionBy == PartitionBy.NONE ? null : TableUtils.getPartitionFloor(partitionBy);

            int capacity = getColumnBase(partitionCount);
            this.columns = new ObjList<>(capacity);
            this.columns.setPos(capacity + 2);
            this.columns.setQuick(0, NullColumn.INSTANCE);
            this.columns.setQuick(1, NullColumn.INSTANCE);
            this.bitmapIndexes = new ObjList<>(capacity);
            this.bitmapIndexes.setPos(capacity + 2);

            this.openPartitionTimestamp = new LongList(partitionCount);
            for (int i = 0; i < partitionCount; i++) {
                this.openPartitionTimestamp.add(txFile.getPartitionTimestamp(i));
            }
            this.openPartitionSize = new LongList(partitionCount);
            this.openPartitionSize.seed(partitionCount, -1);

            this.columnTops = new LongList(capacity / 2);
            this.columnTops.setPos(capacity / 2);
            this.recordCursor.of(this);
        } catch (CairoException e) {
            close();
            throw e;
        }
    }

    public static int getPrimaryColumnIndex(int base, int index) {
        return 2 + base + index * 2;
    }

    public double avgDouble(int columnIndex) {
        double result = 0;
        long countTotal = 0;
        for (int i = 0; i < partitionCount; i++) {
            openPartition(i);
            final int base = getColumnBase(i);
            final int index = getPrimaryColumnIndex(base, columnIndex);
            final ReadOnlyColumn column = columns.getQuick(index);
            if (column != null) {
                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
                    final long a = column.getPageAddress(pageIndex);
                    final long count = column.getPageSize(pageIndex) / Double.BYTES;
                    result += Vect.avgDouble(a, count);
                    countTotal++;
                }
            }
        }

        if (countTotal == 0) {
            return 0;
        }

        return result / countTotal;
    }

    @Override
    public void close() {
        if (isOpen()) {
            freeSymbolMapReaders();
            freeBitmapIndexCache();
            Misc.free(path);
            Misc.free(metadata);
            Misc.free(txFile);
            freeColumns();
            freeTempMem();
            LOG.info().$("closed '").utf8(tableName).$('\'').$();
        }
    }

    /**
     * Closed column files. Similarly to {@link #closeColumnForRemove(CharSequence)} closed reader column files before
     * column can be removed. This method takes column index usually resolved from column name by #TableReaderMetadata.
     * Bounds checking is performed via assertion.
     *
     * @param columnIndex column index
     */
    public void closeColumnForRemove(int columnIndex) {
        assert columnIndex > -1 && columnIndex < columnCount;
        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            // replace columns we force closed with special marker object
            // when we come to reloading table reader we would be able to
            // tell that column has to be attempted to be read from disk
            closeColumn(getColumnBase(partitionIndex), columnIndex);
        }

        if (metadata.getColumnType(columnIndex) == ColumnType.SYMBOL) {
            // same goes for symbol map reader - replace object with maker instance
            Misc.free(symbolMapReaders.getAndSetQuick(columnIndex, EmptySymbolMapReader.INSTANCE));
        }
    }

    /**
     * Closes column files. This method should be used before call to TableWriter.removeColumn() on
     * Windows OS.
     *
     * @param columnName name of column to be closed.
     */
    public void closeColumnForRemove(CharSequence columnName) {
        closeColumnForRemove(metadata.getColumnIndex(columnName));
    }

    public long floorToPartitionTimestamp(long timestamp) {
        return timestampFloorMethod.floor(timestamp);
    }

    public BitmapIndexReader getBitmapIndexReader(int columnBase, int columnIndex, int direction) {
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        BitmapIndexReader reader = bitmapIndexes.getQuick(direction == BitmapIndexReader.DIR_BACKWARD ? index : index + 1);
        return reader == null ? createBitmapIndexReaderAt(index, columnBase, columnIndex, direction) : reader;
    }

    public ReadOnlyColumn getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
    }

    public int getColumnBase(int partitionIndex) {
        return partitionIndex << columnCountBits;
    }

    public long getColumnTop(int base, int columnIndex) {
        return this.columnTops.getQuick(base / 2 + columnIndex);
    }

    public TableReaderRecordCursor getCursor() {
        recordCursor.toTop();
        return recordCursor;
    }

    public long getDataVersion() {
        return this.txFile.getDataVersion();
    }

    public long getMaxTimestamp() {
        return txFile.getMaxTimestamp();
    }

    public TableReaderMetadata getMetadata() {
        return metadata;
    }

    public long getMinTimestamp() {
        return txFile.getMinTimestamp();
    }

    public long getPageValueCount(int partitionIndex, long rowLo, long rowHi, int columnIndex) {
        return rowHi - rowLo - getColumnTop(getColumnBase(partitionIndex), columnIndex);
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getPartitionIndexByTimestamp(long timestamp) {
        int end = openPartitionTimestamp.binarySearch(timestamp);
        if (end < 0) {
            // This will return -1 if searched timestamp is before the first partition
            // The caller should handle negative return values
            end = -end - 2;
        }
        return end;
    }

    public int getPartitionedBy() {
        return metadata.getPartitionBy();
    }

    public SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex);
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex);
    }

    public String getTableName() {
        return tableName;
    }

    public long getVersion() {
        return this.txFile.getStructureVersion();
    }

    public boolean isOpen() {
        return tempMem8b != 0;
    }

    public double maxDouble(int columnIndex) {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < partitionCount; i++) {
            openPartition(i);
            final int base = getColumnBase(i);
            final int index = getPrimaryColumnIndex(base, columnIndex);
            final ReadOnlyColumn column = columns.getQuick(index);
            if (column != null) {
                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
                    long a = column.getPageAddress(pageIndex);
                    long count = column.getPageSize(pageIndex) / Double.BYTES;
                    double x = Vect.maxDouble(a, count);
                    if (x > max) {
                        max = x;
                    }
                }
            }
        }
        return max;
    }

    public double minDouble(int columnIndex) {
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < partitionCount; i++) {
            openPartition(i);
            final int base = getColumnBase(i);
            final int index = getPrimaryColumnIndex(base, columnIndex);
            final ReadOnlyColumn column = columns.getQuick(index);
            if (column != null) {
                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
                    long a = column.getPageAddress(pageIndex);
                    long count = column.getPageSize(pageIndex) / Double.BYTES;
                    double x = Vect.minDouble(a, count);
                    if (x < min) {
                        min = x;
                    }
                }
            }
        }
        return min;
    }

    public long openPartition(int partitionIndex) {
        final long size = getPartitionRowCount(partitionIndex);
        if (size != -1) {
            return size;
        }
        return openPartition0(partitionIndex);
    }

    public void reconcileOpenPartitionsFrom(int partitionIndex) {
        int txSize = txFile.getPartitionsCount();

        // Example of reconciliation between open partitions
        // and updated tx file
        // old      1    2   10   11   13
        // new      3    4    5   11   12    13    14
        // --------------------------------------------------------------
        // old      1    2    -    -     -    10    11     -     13     -
        // new      -    -    3    4     5     -    11    12     13    14
        // op       d    d    i    i     i     d     r     d      r     i
        // d=delete, i=insert, r=refresh
        int txPartitionIndex = partitionIndex;
        boolean changed = false;

        while (partitionIndex < partitionCount && txPartitionIndex < txSize) {
            long openPartTs = openPartitionTimestamp.getQuick(partitionIndex);
            long openPartSize = openPartitionSize.getQuick(partitionIndex);
            long txPartTs = txFile.getPartitionTimestamp(txPartitionIndex);

            if (openPartTs < txPartTs) {
                // Deleted partitions
                // This will decrement partitionCount
                deletePartition(partitionIndex);
            } else if (openPartTs > txPartTs) {
                // Insert partition
                insertPartition(partitionIndex, txPartTs);
                txPartitionIndex++;
                partitionIndex++;
            } else {
                // Refresh partition
                long newPartitionSize = txFile.getPartitionSize(txPartitionIndex);
                if (openPartSize != newPartitionSize) {
                    if (openPartSize > -1L) {
                        // Resize partition
                        reloadPartition(partitionIndex, newPartitionSize);
                        openPartitionSize.setQuick(partitionIndex, newPartitionSize);
                        LOG.info().$("updated partition size [partition=").$(openPartTs).I$();
                    }
                    changed = true;
                }
                txPartitionIndex++;
                partitionIndex++;
            }
        }

        // if while finished on txPartitionIndex == txSize condition
        // remove deleted opened partitions
        while (partitionIndex < partitionCount) {
            deletePartition(partitionIndex);
            changed = true;
        }

        // if while finished on partitionIndex == partitionCount condition
        // insert new partitions at the end
        for (; partitionIndex < txSize; partitionIndex++) {
            insertPartition(partitionIndex, txFile.getPartitionTimestamp(partitionIndex));
            changed = true;
        }

        if (changed) {
            reloadSymbolMapCounts();
        }

        assert openPartitionSize.size() == openPartitionTimestamp.size() && openPartitionSize.size() == partitionCount;
    }

    public void reshuffleSymbolMapReaders(long pTransitionIndex) {
        final int columnCount = Unsafe.getUnsafe().getInt(pTransitionIndex + 4);
        final long index = pTransitionIndex + 8;
        final long stateAddress = index + columnCount * 8L;

        if (columnCount > this.columnCount) {
            symbolMapReaders.setPos(columnCount);
        }

        Unsafe.getUnsafe().setMemory(stateAddress, columnCount, (byte) 0);

        // this is a silly exercise in walking the index
        for (int i = 0; i < columnCount; i++) {

            // prevent writing same entry once
            if (Unsafe.getUnsafe().getByte(stateAddress + i) == -1) {
                continue;
            }

            Unsafe.getUnsafe().putByte(stateAddress + i, (byte) -1);

            int copyFrom = Unsafe.getUnsafe().getInt(index + i * 8L);

            // don't copy entries to themselves, unless symbol map was deleted
            if (copyFrom == i + 1 && copyFrom < columnCount) {
                SymbolMapReader reader = symbolMapReaders.getQuick(copyFrom);
                if (reader != null && reader.isDeleted()) {
                    symbolMapReaders.setQuick(copyFrom, reloadSymbolMapReader(copyFrom, reader));
                }
                continue;
            }

            // check where we source entry:
            // 1. from another entry
            // 2. create new instance
            SymbolMapReader tmp;
            if (copyFrom > 0) {
                tmp = copyOrRenewSymbolMapReader(symbolMapReaders.getAndSetQuick(copyFrom - 1, null), i);

                int copyTo = Unsafe.getUnsafe().getInt(index + i * 8L + 4);

                // now we copied entry, what do we do with value that was already there?
                // do we copy it somewhere else?
                while (copyTo > 0) {
                    // Yeah, we do. This can get recursive!
                    // prevent writing same entry twice
                    if (Unsafe.getUnsafe().getByte(stateAddress + copyTo - 1) == -1) {
                        break;
                    }
                    Unsafe.getUnsafe().putByte(stateAddress + copyTo - 1, (byte) -1);

                    tmp = copyOrRenewSymbolMapReader(tmp, copyTo - 1);
                    copyTo = Unsafe.getUnsafe().getInt(index + (copyTo - 1) * 8L + 4);
                }
                Misc.free(tmp);
            } else {
                // new instance
                Misc.free(symbolMapReaders.getAndSetQuick(i, reloadSymbolMapReader(i, null)));
            }
        }

        // ended up with fewer columns than before?
        // free resources for the "extra" symbol map readers and contract the list
        if (columnCount < this.columnCount) {
            for (int i = columnCount; i < this.columnCount; i++) {
                Misc.free(symbolMapReaders.getQuick(i));
            }
            symbolMapReaders.setPos(columnCount);
        }
    }

    public long size() {
        return rowCount;
    }

    public double sumDouble(int columnIndex) {
        double result = 0;
        for (int i = 0; i < partitionCount; i++) {
            openPartition(i);
            final int base = getColumnBase(i);
            final int index = getPrimaryColumnIndex(base, columnIndex);
            final ReadOnlyColumn column = columns.getQuick(index);
            if (column != null) {
                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
                    long a = column.getPageAddress(pageIndex);
                    long count = column.getPageSize(pageIndex) / Double.BYTES;
                    result += Vect.sumDouble(a, count);
                }
            }
        }
        return result;
    }

    private static int getColumnBits(int columnCount) {
        return Numbers.msb(Numbers.ceilPow2(columnCount) * 2);
    }

    private static boolean isEntryToBeProcessed(long address, int index) {
        if (Unsafe.getUnsafe().getByte(address + index) == -1) {
            return false;
        }
        Unsafe.getUnsafe().putByte(address + index, (byte) -1);
        return true;
    }

    private static void growColumn(ReadOnlyColumn mem1, ReadOnlyColumn mem2, int type, long rowCount) {
        if (rowCount > 0) {
            // subtract column top
            switch (type) {
                default:
                    mem1.grow(rowCount << ColumnType.pow2SizeOf(type));
                    break;
                case ColumnType.BINARY:
                    growBin(mem1, mem2, rowCount);
                    break;
                case ColumnType.STRING:
                    growStr(mem1, mem2, rowCount);
                    break;
            }
        }
    }

    private static void growStr(ReadOnlyColumn mem1, ReadOnlyColumn mem2, long rowCount) {
        assert mem2 != null;
        mem2.grow(rowCount * 8);
        final long offset = mem2.getLong((rowCount - 1) * 8);
        mem1.grow(offset + 4);
        final long len = mem1.getInt(offset);
        if (len > 0) {
            mem1.grow(offset + len * 2 + 4);
        }
    }

    private static void growBin(ReadOnlyColumn mem1, ReadOnlyColumn mem2, long rowCount) {
        assert mem2 != null;
        mem2.grow(rowCount * 8);
        final long offset = mem2.getLong((rowCount - 1) * 8);
        // grow data column to value offset + length, so that we can read length
        mem1.grow(offset + 8);
        final long len = mem1.getLong(offset);
        if (len > 0) {
            mem1.grow(offset + len + 8);
        }
    }

    private void closeColumn(int columnBase, int columnIndex) {
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        Misc.free(columns.getAndSetQuick(index, NullColumn.INSTANCE));
        Misc.free(columns.getAndSetQuick(index + 1, NullColumn.INSTANCE));
        Misc.free(bitmapIndexes.getAndSetQuick(index, null));
        Misc.free(bitmapIndexes.getAndSetQuick(index + 1, null));
    }

    private void copyColumnsTo(
            ObjList<ReadOnlyColumn> columns,
            LongList columnTops,
            ObjList<BitmapIndexReader> indexReaders,
            int columnBase,
            int columnIndex,
            long partitionRowCount,
            boolean lastPartition) {
        ReadOnlyColumn mem1 = tempCopyStruct.mem1;
        final boolean reload = (mem1 instanceof ExtendableOnePageMemory || mem1 instanceof NullColumn) && mem1.isDeleted();
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        tempCopyStruct.mem1 = columns.getAndSetQuick(index, mem1);
        tempCopyStruct.mem2 = columns.getAndSetQuick(index + 1, tempCopyStruct.mem2);
        tempCopyStruct.top = columnTops.getAndSetQuick(columnBase / 2 + columnIndex, tempCopyStruct.top);
        tempCopyStruct.backwardReader = indexReaders.getAndSetQuick(index, tempCopyStruct.backwardReader);
        tempCopyStruct.forwardReader = indexReaders.getAndSetQuick(index + 1, tempCopyStruct.forwardReader);
        if (reload) {
            reloadColumnAt(path, columns, columnTops, indexReaders, columnBase, columnIndex, partitionRowCount, lastPartition);
        }
    }

    private SymbolMapReader copyOrRenewSymbolMapReader(SymbolMapReader reader, int columnIndex) {
        if (reader != null && reader.isDeleted()) {
            reader = reloadSymbolMapReader(columnIndex, reader);
        }
        return symbolMapReaders.getAndSetQuick(columnIndex, reader);
    }

    private BitmapIndexReader createBitmapIndexReaderAt(int globalIndex, int columnBase, int columnIndex, int direction) {
        BitmapIndexReader reader;
        if (!metadata.isColumnIndexed(columnIndex)) {
            throw CairoException.instance(0).put("Not indexed: ").put(metadata.getColumnName(columnIndex));
        }

        ReadOnlyColumn col = columns.getQuick(globalIndex);
        if (col instanceof NullColumn) {
            if (direction == BitmapIndexReader.DIR_BACKWARD) {
                reader = new BitmapIndexBwdNullReader();
                bitmapIndexes.setQuick(globalIndex, reader);
            } else {
                reader = new BitmapIndexFwdNullReader();
                bitmapIndexes.setQuick(globalIndex + 1, reader);
            }
        } else {
            Path path = pathGenPartitioned(getPartitionIndex(columnBase));
            try {
                if (direction == BitmapIndexReader.DIR_BACKWARD) {
                    reader = new BitmapIndexBwdReader(configuration, path.chopZ(), metadata.getColumnName(columnIndex), getColumnTop(columnBase, columnIndex));
                    bitmapIndexes.setQuick(globalIndex, reader);
                } else {
                    reader = new BitmapIndexFwdReader(configuration, path.chopZ(), metadata.getColumnName(columnIndex), getColumnTop(columnBase, columnIndex));
                    bitmapIndexes.setQuick(globalIndex + 1, reader);
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
        return reader;
    }

    private void createNewColumnList(int columnCount, long pTransitionIndex, int columnBits) {
        int capacity = partitionCount << columnBits;
        final ObjList<ReadOnlyColumn> columns = new ObjList<>(capacity);
        final LongList columnTops = new LongList(capacity / 2);
        final ObjList<BitmapIndexReader> indexReaders = new ObjList<>(capacity);
        columns.setPos(capacity + 2);
        columns.setQuick(0, NullColumn.INSTANCE);
        columns.setQuick(1, NullColumn.INSTANCE);
        columnTops.setPos(capacity / 2);
        indexReaders.setPos(capacity + 2);
        final long pIndexBase = pTransitionIndex + 8;

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final int base = partitionIndex << columnBits;
            final int oldBase = partitionIndex << columnCountBits;
            try {
                final Path path = pathGenPartitioned(partitionIndex);
                long partitionRowCount = openPartitionSize.getQuick(partitionIndex);
                final boolean lastPartition = partitionIndex == partitionCount - 1;
                for (int i = 0; i < columnCount; i++) {
                    final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L) - 1;
                    if (copyFrom > -1) {
                        fetchColumnsFrom(this.columns, this.columnTops, this.bitmapIndexes, oldBase, copyFrom);
                        copyColumnsTo(columns, columnTops, indexReaders, base, i, partitionRowCount, lastPartition);
                    } else {
                        // new instance
                        reloadColumnAt(path, columns, columnTops, indexReaders, base, i, partitionRowCount, lastPartition);
                    }
                }

                // free remaining columns
                for (int i = 0; i < this.columnCount; i++) {
                    final int index = getPrimaryColumnIndex(oldBase, i);
                    Misc.free(this.columns.getQuick(index));
                    Misc.free(this.columns.getQuick(index + 1));
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
        this.columns = columns;
        this.columnTops = columnTops;
        this.columnCountBits = columnBits;
        this.bitmapIndexes = indexReaders;
    }

    private void deletePartition(int partitionIndex) {
        long partitionSize = openPartitionSize.getQuick(partitionIndex);
        long partitionTimestamp = openPartitionTimestamp.getQuick(partitionIndex);
        int base = getColumnBase(partitionIndex);
        if (partitionSize > -1L) {
            for (int k = 0; k < columnCount; k++) {
                closeColumn(base, k);
            }
        }
        int baseIndex = getPrimaryColumnIndex(getColumnBase(partitionIndex), 0);
        int newBaseIndex = getPrimaryColumnIndex(getColumnBase(partitionIndex + 1), 0);
        columns.remove(baseIndex, newBaseIndex - 1);

        openPartitionTimestamp.removeIndex(partitionIndex);
        openPartitionSize.removeIndex(partitionIndex);

        LOG.info().$("deleted partition [path=").$(path).$(",timestamp=").$ts(partitionTimestamp).I$();
        partitionCount--;
    }

    private void failOnPendingTodo() {
        try {
            if (ff.exists(path.concat(TableUtils.TODO_FILE_NAME).$())) {
                throw CairoException.instance(0).put("Table ").put(path.$()).put(" is pending recovery.");
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void fetchColumnsFrom(ObjList<ReadOnlyColumn> columns, LongList columnTops, ObjList<BitmapIndexReader> indexReaders, int columnBase, int columnIndex) {
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        tempCopyStruct.mem1 = columns.getAndSetQuick(index, null);
        tempCopyStruct.mem2 = columns.getAndSetQuick(index + 1, null);
        tempCopyStruct.top = columnTops.getQuick(columnBase / 2 + columnIndex);
        tempCopyStruct.backwardReader = indexReaders.getAndSetQuick(index, null);
        tempCopyStruct.forwardReader = indexReaders.getAndSetQuick(index + 1, null);
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
            Unsafe.free(tempMem8b, 8);
            tempMem8b = 0;
        }
    }

    int getColumnCount() {
        return columnCount;
    }

    int getPartitionIndex(int columnBase) {
        return columnBase >>> columnCountBits;
    }

    long getPartitionRowCount(int partitionIndex) {
        return openPartitionSize.getQuick(partitionIndex);
    }

    long getTransientRowCount() {
        return txFile.getTransientRowCount();
    }

    long getTxn() {
        return txn;
    }

    boolean hasNull(int columnIndex) {
        for (int i = 0; i < partitionCount; i++) {
            openPartition(i);
            final int base = getColumnBase(i);
            final int index = getPrimaryColumnIndex(base, columnIndex);
            final ReadOnlyColumn column = columns.getQuick(index);
            if (column != null) {
                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
                    long a = column.getPageAddress(pageIndex);
                    long count = column.getPageSize(pageIndex) / Integer.BYTES;
                    if (Vect.hasNull(a, count)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void insertPartition(int partitionIndex, long timestamp) {
        int columnBase = getColumnBase(partitionIndex);
        int baseIndex = getPrimaryColumnIndex(columnBase, 0);
        int newBaseIndex = getPrimaryColumnIndex(getColumnBase(partitionIndex + 1), 0);

        int columnResize = newBaseIndex - baseIndex;
        columns.insert(baseIndex, columnResize);
        columns.set(baseIndex, baseIndex + columnCount * 2 - 1, null);

        bitmapIndexes.insert(columnBase, columnResize);
        columnTops.insert(columnBase, columnResize / 2);

        openPartitionTimestamp.add(partitionIndex, timestamp);
        openPartitionSize.add(partitionIndex, -1L);

        partitionCount++;
        LOG.info().$("inserted partition [path=").$(path).$(",timestamp=").$ts(timestamp).I$();
    }

    boolean isColumnCached(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex).isCached();
    }

    private TableReaderMetadata openMetaFile() {
        try {
            return new TableReaderMetadata(ff, path.concat(TableUtils.META_FILE_NAME).$());
        } finally {
            path.trimTo(rootLen);
        }
    }

    @NotNull
    private ReadOnlyColumn openOrCreateMemory(Path path, ObjList<ReadOnlyColumn> columns, boolean lastPartition, int primaryIndex, ReadOnlyColumn mem) {
        if (mem != null && mem != NullColumn.INSTANCE) {
            mem.of(ff, path, ff.getMapPageSize(), ff.length(path));
        } else {
            if (lastPartition) {
                mem = new ExtendableOnePageMemory(ff, path, ff.getMapPageSize());
            } else {
                mem = new OnePageMemory(ff, path, ff.length(path));
            }
            columns.setQuick(primaryIndex, mem);
        }
        return mem;
    }

    private long openPartition0(int partitionIndex) {
        if (txFile.getPartitionsCount() < 2 && txFile.getTransientRowCount() == 0) {
            return -1;
        }

        try {
            Path path = pathGenPartitioned(partitionIndex);
            if (ff.exists(path)) {
                path.chopZ();

                final boolean lastPartition = partitionIndex == partitionCount - 1;
                final long partitionSize = txFile.getPartitionSize(partitionIndex);

                LOG.info()
                        .$("open partition ").utf8(path.$())
                        .$(" [rowCount=").$(partitionSize)
                        .$(", transientRowCount=").$(txFile.getTransientRowCount())
                        .$(", partitionIndex=").$(partitionIndex)
                        .$(", partitionCount=").$(partitionCount)
                        .$(']').$();

                if (partitionSize > 0) {
                    openPartitionColumns(path, getColumnBase(partitionIndex), partitionSize, lastPartition);
                    this.openPartitionSize.setQuick(partitionIndex, partitionSize);
                }

                return partitionSize;
            }
            LOG.error().$("open partition failed, partition does not exist on the disk. [path=").utf8(path.$()).I$();

            if (getPartitionedBy() != PartitionBy.NONE) {
                var exception = CairoException.instance(0).put("Partition '");
                formatPartitionDirName(partitionIndex, exception.message);
                exception.put("' does not exist in table '")
                        .put(tableName)
                        .put("' directory. Run [ALTER TABLE ").put(tableName).put(" DROP PARTITION LIST '");
                formatPartitionDirName(partitionIndex, exception.message);
                exception.put("'] to repair the table or restore the partition directory.");
                throw exception;
            } else {
                throw CairoException.instance(0).put("Table '").put(tableName)
                        .put("' data directory does not exist on the disk at ")
                        .put(path)
                        .put(". Restore data on disk or drop the table.");
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartitionColumns(Path path, int columnBase, long partitionRowCount, boolean lastPartition) {
        for (int i = 0; i < columnCount; i++) {
            reloadColumnAt(path, this.columns, this.columnTops, this.bitmapIndexes, columnBase, i, partitionRowCount, lastPartition);
        }
    }

    private void openSymbolMaps() {
        int symbolColumnIndex = 0;
        final int columnCount = metadata.getColumnCount();
        symbolMapReaders.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (metadata.getColumnType(i) == ColumnType.SYMBOL) {
                SymbolMapReaderImpl symbolMapReader = new SymbolMapReaderImpl(configuration, path, metadata.getColumnName(i), symbolCountSnapshot.getQuick(symbolColumnIndex++));
                symbolMapReaders.extendAndSet(i, symbolMapReader);
            }
        }
    }

    private Path pathGenPartitioned(int partitionIndex) {
        formatPartitionDirName(partitionIndex, path.put(Files.SEPARATOR));
        return path.$();
    }

    private void formatPartitionDirName(int partitionIndex, CharSink sink) {
        partitionFormat.format(
                openPartitionTimestamp.getQuick(partitionIndex),
                null, // this format does not need locale access
                null,
                sink
        );
    }

    private boolean readTxnSlow() {
        int count = 0;
        final long deadline = configuration.getMicrosecondClock().getTicks() + configuration.getSpinLockTimeoutUs();
        while (true) {
            long txn = txFile.readTxn();

            // exit if this is the same as we already have
            if (txn == this.txn) {
                return false;
            }

            // make sure this isn't re-ordered
            Unsafe.getUnsafe().loadFence();

            // do start and end sequences match? if so we have a chance at stable read
            if (txn == txFile.readTxnCheck()) {
                // great, we seem to have got stable read, lets do some reading
                // and check later if it was worth it

                Unsafe.getUnsafe().loadFence();
                txFile.readUnchecked();

                this.symbolCountSnapshot.clear();
                this.txFile.readSymbolCounts(this.symbolCountSnapshot);


                Unsafe.getUnsafe().loadFence();
                // ok, we have snapshot, check if our snapshot is stable
                if (txn == txFile.getTxn()) {
                    // good, very stable, congrats
                    this.txn = txn;
                    this.rowCount = txFile.getFixedRowCount() + txFile.getTransientRowCount();
                    LOG.info()
                            .$("new transaction [txn=").$(txn)
                            .$(", transientRowCount=").$(txFile.getTransientRowCount())
                            .$(", fixedRowCount=").$(txFile.getFixedRowCount())
                            .$(", maxTimestamp=").$ts(txFile.getMaxTimestamp())
                            .$(", attempts=").$(count)
                            .$(", thread=").$(Thread.currentThread().getName())
                            .$(']').$();
                    return true;
                }
                // This is unlucky, sequences have changed while we were reading transaction data
                // We must discard and try again
            }
            count++;
            if (configuration.getMicrosecondClock().getTicks() > deadline) {
                LOG.error().$("tx read timeout [timeout=").$(configuration.getSpinLockTimeoutUs()).utf8("μs]").$();
                throw CairoException.instance(0).put("Transaction read timeout");
            }
            LockSupport.parkNanos(1);
        }
    }

    private void reconcileOpenPartitions(long prevPartitionVersion) {
        // Reconcile partition full or partial
        // Partial will only update row count of last partition and append new partitions
        if (this.txFile.getPartitionTableVersion() == prevPartitionVersion) {
            reconcileOpenPartitionsFrom(Math.max(0, partitionCount - 1));
            return;
        }
        reconcileOpenPartitionsFrom(0);
    }

    private void reloadColumnAt(
            Path path,
            ObjList<ReadOnlyColumn> columns,
            LongList columnTops,
            ObjList<BitmapIndexReader> indexReaders,
            int columnBase,
            int columnIndex,
            long partitionRowCount,
            boolean lastPartition
    ) {
        int plen = path.length();
        try {
            final CharSequence name = metadata.getColumnName(columnIndex);
            final int primaryIndex = getPrimaryColumnIndex(columnBase, columnIndex);
            final int secondaryIndex = primaryIndex + 1;

            ReadOnlyColumn mem1 = columns.getQuick(primaryIndex);
            ReadOnlyColumn mem2 = columns.getQuick(secondaryIndex);

            if (ff.exists(TableUtils.dFile(path.trimTo(plen), name))) {

                mem1 = openOrCreateMemory(path, columns, lastPartition, primaryIndex, mem1);

                final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), name, plen, tempMem8b);
                final int type = metadata.getColumnType(columnIndex);

                switch (type) {
                    case ColumnType.BINARY:
                    case ColumnType.STRING:
                        TableUtils.iFile(path.trimTo(plen), name);
                        mem2 = openOrCreateMemory(path, columns, lastPartition, secondaryIndex, mem2);
                        growColumn(mem1, mem2, type, partitionRowCount - columnTop);
                        break;
                    default:
                        Misc.free(columns.getAndSetQuick(secondaryIndex, null));
                        growColumn(mem1, null, type, partitionRowCount - columnTop);
                        break;
                }

                columnTops.setQuick(columnBase / 2 + columnIndex, columnTop);

                if (metadata.isColumnIndexed(columnIndex)) {
                    BitmapIndexReader indexReader = indexReaders.getQuick(primaryIndex);
                    if (indexReader instanceof BitmapIndexBwdReader) {
                        ((BitmapIndexBwdReader) indexReader).of(configuration, path.trimTo(plen), name, columnTop);
                    }

                    indexReader = indexReaders.getQuick(secondaryIndex);
                    if (indexReader instanceof BitmapIndexFwdReader) {
                        ((BitmapIndexFwdReader) indexReader).of(configuration, path.trimTo(plen), name, columnTop);
                    }

                } else {
                    Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                    Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));
                }
            } else {
                Misc.free(columns.getAndSetQuick(primaryIndex, NullColumn.INSTANCE));
                Misc.free(columns.getAndSetQuick(secondaryIndex, NullColumn.INSTANCE));
                // the appropriate index for NUllColumn will be created lazily when requested
                // these indexes have state and may not be always required
                Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private void reloadColumnChanges() {
        // create transition index, which will help us reuse already open resources
        long pTransitionIndex = metadata.createTransitionIndex();
        try {
            metadata.applyTransitionIndex(pTransitionIndex);
            final int columnCount = Unsafe.getUnsafe().getInt(pTransitionIndex + 4);

            int columnCountBits = getColumnBits(columnCount);
            // when a column is added we cannot easily reshuffle columns in-place
            // the reason is that we'd have to create gaps in columns list between
            // partitions. It is possible in theory, but this could be an algo for
            // another day.
            if (columnCountBits > this.columnCountBits) {
                createNewColumnList(columnCount, pTransitionIndex, columnCountBits);
            } else {
                reshuffleColumns(columnCount, pTransitionIndex);
            }
            // rearrange symbol map reader list
            reshuffleSymbolMapReaders(pTransitionIndex);
            this.columnCount = columnCount;
        } finally {
            TableReaderMetadata.freeTransitionIndex(pTransitionIndex);
        }
    }

    public boolean reload() {
        if (this.txn == txFile.readTxn()) {
            return false;
        }
        return reloadSlow();
    }

    private boolean reloadSlow() {
        // Save tx file versions on stack
        final long prevStructVersion = this.txFile.getStructureVersion();
        final long prevPartitionVersion = this.txFile.getPartitionTableVersion();

        // reload tx file, this will update the versions
        if (this.readTxnSlow()) {
            reloadStruct(prevStructVersion);
            // partition reload will apply truncate if necessary
            // applyTruncate for non-partitioned tables only
            reconcileOpenPartitions(prevPartitionVersion);
            return true;
        }

        return false;
    }

    /**
     * Updates boundaries of all columns in partition.
     *
     * @param partitionIndex index of partition
     * @param rowCount       number of rows in partition
     */
    private void reloadPartition(int partitionIndex, long rowCount) {
        int symbolMapIndex = 0;
        int columnBase = getColumnBase(partitionIndex);
        for (int i = 0; i < columnCount; i++) {
            final int index = getPrimaryColumnIndex(columnBase, i);
            growColumn(
                    columns.getQuick(index),
                    columns.getQuick(index + 1),
                    metadata.getColumnType(i),
                    rowCount - getColumnTop(columnBase, i)
            );

            // reload symbol map
            SymbolMapReader reader = symbolMapReaders.getQuick(i);
            if (reader == null) {
                continue;
            }
            reader.updateSymbolCount(symbolCountSnapshot.getQuick(symbolMapIndex++));
        }
    }

    private void reloadStruct(long prevStructVersion) {
        if (prevStructVersion == txFile.getStructureVersion()) {
            return;
        }
        reloadStructSlow();
    }

    private void reloadStructSlow() {
        reloadColumnChanges();
        reloadSymbolMapCounts();
    }

    private void reloadSymbolMapCounts() {
        int symbolMapIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            if (metadata.getColumnType(i) != ColumnType.SYMBOL) {
                continue;
            }
            symbolMapReaders.getQuick(i).updateSymbolCount(symbolCountSnapshot.getQuick(symbolMapIndex++));
        }
    }

    private SymbolMapReader reloadSymbolMapReader(int columnIndex, SymbolMapReader reader) {
        if (metadata.getColumnType(columnIndex) == ColumnType.SYMBOL) {
            if (reader instanceof SymbolMapReaderImpl) {
                ((SymbolMapReaderImpl) reader).of(configuration, path, metadata.getColumnName(columnIndex), 0);
                return reader;
            }
            return new SymbolMapReaderImpl(configuration, path, metadata.getColumnName(columnIndex), 0);
        } else {
            return reader;
        }
    }

    private void reshuffleColumns(int columnCount, long pTransitionIndex) {

        final long pIndexBase = pTransitionIndex + 8;
        final long pState = pIndexBase + columnCount * 8L;

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            int base = getColumnBase(partitionIndex);
            try {
                final Path path = pathGenPartitioned(partitionIndex);
                final long partitionRowCount = openPartitionSize.getQuick(partitionIndex);
                final boolean lastPartition = partitionIndex == partitionCount - 1;

                Unsafe.getUnsafe().setMemory(pState, columnCount, (byte) 0);

                for (int i = 0; i < columnCount; i++) {

                    if (isEntryToBeProcessed(pState, i)) {
                        final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L) - 1;

                        if (copyFrom == i) {
                            // It appears that column hasn't changed its position. There are three possibilities here:
                            // 1. Column has been deleted and re-added by the same name. We must check if file
                            //    descriptor is still valid. If it isn't, reload the column from disk
                            // 2. Column has been forced out of the reader via closeColumnForRemove(). This is required
                            //    on Windows before column can be deleted. In this case we must check for marker
                            //    instance and the column from disk
                            // 3. Column hasn't been altered and we can skip to next column.
                            ReadOnlyColumn col = columns.getQuick(getPrimaryColumnIndex(base, i));
                            if ((col instanceof ExtendableOnePageMemory && col.isDeleted()) || col instanceof NullColumn) {
                                reloadColumnAt(path, columns, columnTops, bitmapIndexes, base, i, partitionRowCount, lastPartition);
                            }
                            continue;
                        }

                        if (copyFrom > -1) {
                            fetchColumnsFrom(this.columns, this.columnTops, this.bitmapIndexes, base, copyFrom);
                            copyColumnsTo(this.columns, this.columnTops, this.bitmapIndexes, base, i, partitionRowCount, lastPartition);
                            int copyTo = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L + 4) - 1;
                            while (copyTo > -1 && isEntryToBeProcessed(pState, copyTo)) {
                                copyColumnsTo(this.columns, this.columnTops, this.bitmapIndexes, base, copyTo, partitionRowCount, lastPartition);
                                copyTo = Unsafe.getUnsafe().getInt(pIndexBase + (copyTo - 1) * 8L + 4);
                            }
                            Misc.free(tempCopyStruct.mem1);
                            Misc.free(tempCopyStruct.mem2);
                            Misc.free(tempCopyStruct.backwardReader);
                            Misc.free(tempCopyStruct.forwardReader);
                        } else {
                            // new instance
                            reloadColumnAt(path, columns, columnTops, bitmapIndexes, base, i, partitionRowCount, lastPartition);
                        }
                    }
                }
                for (int i = columnCount; i < this.columnCount; i++) {
                    int index = getPrimaryColumnIndex(base, i);
                    Misc.free(columns.getQuick(index));
                    Misc.free(columns.getQuick(index + 1));
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private static class ColumnCopyStruct {
        ReadOnlyColumn mem1;
        ReadOnlyColumn mem2;
        BitmapIndexReader backwardReader;
        BitmapIndexReader forwardReader;
        long top;
    }

}
