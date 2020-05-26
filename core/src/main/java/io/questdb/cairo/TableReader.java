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

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.microtime.Timestamps;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

import static io.questdb.cairo.TableUtils.TX_OFFSET_MIN_TIMESTAMP;

public class TableReader implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private static final PartitionPathGenerator YEAR_GEN = TableReader::pathGenYear;
    private static final PartitionPathGenerator MONTH_GEN = TableReader::pathGenMonth;
    private static final PartitionPathGenerator DAY_GEN = TableReader::pathGenDay;
    private static final PartitionPathGenerator DEFAULT_GEN = (reader, partitionIndex) -> reader.pathGenDefault();
    private static final ReloadMethod FIRST_TIME_PARTITIONED_RELOAD_METHOD = TableReader::reloadInitialPartitioned;
    private static final ReloadMethod FIRST_TIME_NON_PARTITIONED_RELOAD_METHOD = TableReader::reloadInitialNonPartitioned;
    private static final ReloadMethod PARTITIONED_RELOAD_METHOD = TableReader::reloadPartitioned;
    private static final ReloadMethod NON_PARTITIONED_RELOAD_METHOD = TableReader::reloadNonPartitioned;
    private static final Timestamps.TimestampFloorMethod ENTITY_FLOOR_METHOD = timestamp -> timestamp;
    private final ColumnCopyStruct tempCopyStruct = new ColumnCopyStruct();
    private final FilesFacade ff;
    private final Path path;
    private final int rootLen;
    private final ReadOnlyColumn txMem;
    private final TableReaderMetadata metadata;
    private final LongList partitionRowCounts;
    private final PartitionPathGenerator partitionPathGenerator;
    private final TableReaderRecordCursor recordCursor = new TableReaderRecordCursor();
    private final Timestamps.TimestampFloorMethod timestampFloorMethod;
    private final IntervalLengthMethod intervalLengthMethod;
    private final Timestamps.TimestampAddMethod timestampAddMethod;
    private final String tableName;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final CairoConfiguration configuration;
    private final IntList symbolCountSnapshot = new IntList();
    private final LongHashSet removedPartitions = new LongHashSet();
    private LongList columnTops;
    private ObjList<ReadOnlyColumn> columns;
    private ObjList<BitmapIndexReader> bitmapIndexes;
    private int columnCount;
    private int columnCountBits;
    private long transientRowCount;
    private long structVersion;
    private long dataVersion;
    private long prevStructVersion;
    private long partitionTableVersion;
    private long prevPartitionTableVersion;
    private long rowCount;
    private long txn = TableUtils.INITIAL_TXN;
    private long maxTimestamp = Numbers.LONG_NaN;
    private int partitionCount;
    private long minTimestamp = Long.MAX_VALUE;
    private long prevMinTimestamp = Long.MAX_VALUE;
    private ReloadMethod reloadMethod;
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
            this.txMem = openTxnFile();
            this.metadata = openMetaFile();
            this.columnCount = this.metadata.getColumnCount();
            this.columnCountBits = getColumnBits(columnCount);
            switch (this.metadata.getPartitionBy()) {
                case PartitionBy.DAY:
                    partitionPathGenerator = DAY_GEN;
                    reloadMethod = FIRST_TIME_PARTITIONED_RELOAD_METHOD;
                    timestampFloorMethod = Timestamps::floorDD;
                    intervalLengthMethod = Timestamps::getDaysBetween;
                    timestampAddMethod = Timestamps::addDays;
                    break;
                case PartitionBy.MONTH:
                    partitionPathGenerator = MONTH_GEN;
                    reloadMethod = FIRST_TIME_PARTITIONED_RELOAD_METHOD;
                    timestampFloorMethod = Timestamps::floorMM;
                    intervalLengthMethod = Timestamps::getMonthsBetween;
                    timestampAddMethod = Timestamps::addMonths;
                    break;
                case PartitionBy.YEAR:
                    partitionPathGenerator = YEAR_GEN;
                    reloadMethod = FIRST_TIME_PARTITIONED_RELOAD_METHOD;
                    timestampFloorMethod = Timestamps::floorYYYY;
                    intervalLengthMethod = Timestamps::getYearsBetween;
                    timestampAddMethod = Timestamps::addYear;
                    break;
                default:
                    partitionPathGenerator = DEFAULT_GEN;
                    reloadMethod = FIRST_TIME_NON_PARTITIONED_RELOAD_METHOD;
                    timestampFloorMethod = ENTITY_FLOOR_METHOD;
                    intervalLengthMethod = null;
                    timestampAddMethod = null;
                    break;
            }
            readTxn();
            openSymbolMaps();
            this.prevStructVersion = structVersion;
            this.prevPartitionTableVersion = partitionTableVersion;
            if (metadata.getPartitionBy() == PartitionBy.NONE) {
                checkDefaultPartitionExistsAndUpdatePartitionCount();
            } else {
                partitionCount = calculatePartitionCount();
            }
            int capacity = getColumnBase(partitionCount);
            this.columns = new ObjList<>(capacity);
            this.columns.setPos(capacity + 2);
            this.columns.setQuick(0, NullColumn.INSTANCE);
            this.columns.setQuick(1, NullColumn.INSTANCE);
            this.bitmapIndexes = new ObjList<>(capacity);
            this.bitmapIndexes.setPos(capacity + 2);
            this.partitionRowCounts = new LongList(partitionCount);
            this.partitionRowCounts.seed(partitionCount, -1);
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
            Misc.free(txMem);
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

    public int getColumnBase(int partitionIndex) {
        return partitionIndex << columnCountBits;
    }

    public TableReaderRecordCursor getCursor() {
        recordCursor.toTop();
        return recordCursor;
    }

    public long getDataVersion() {
        return dataVersion;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getPageAddressAt(int partitionIndex, long row, int columnIndex) {
        final int base = getColumnBase(partitionIndex);
        final int column = getPrimaryColumnIndex(base, columnIndex);
        final long r = row - getColumnTop(base, columnIndex);
        final int columnType = metadata.getColumnType(columnIndex);
        switch (columnType) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                return getColumn(column + 1).getLong(r * Long.BYTES);
            default:
                return r * ColumnType.sizeOf(columnType);
        }
    }

    public long getPageValueCount(int partitionIndex, long rowLo, long rowHi, int columnIndex) {
        return rowHi - rowLo - getColumnTop(getColumnBase(partitionIndex), columnIndex);
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getPartitionCountBetweenTimestamps(long partitionTimestamp1, long partitionTimestamp2) {
        return (int) intervalLengthMethod.calculate(partitionTimestamp1, partitionTimestamp2);
    }

    public int getPartitionedBy() {
        return metadata.getPartitionBy();
    }

    public SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex);
    }

    public String getTableName() {
        return tableName;
    }

    public long getVersion() {
        return this.structVersion;
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

    public boolean reload() {
        return reloadMethod.reload(this);
    }

    public void reshuffleSymbolMapReaders(long pTransitionIndex) {
        final int columnCount = Unsafe.getUnsafe().getInt(pTransitionIndex + 4);
        final long index = pTransitionIndex + 8;
        final long stateAddress = index + columnCount * 8;

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

            int copyFrom = Unsafe.getUnsafe().getInt(index + i * 8);

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

                int copyTo = Unsafe.getUnsafe().getInt(index + i * 8 + 4);

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
                    copyTo = Unsafe.getUnsafe().getInt(index + (copyTo - 1) * 8 + 4);
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

    private void applyTruncate() {
        LOG.info().$("truncate detected").$();
        for (int i = 0, n = partitionCount; i < n; i++) {
            long size = openPartition0(i);
            if (size == -1) {
                int base = getColumnBase(i);
                for (int k = 0; k < columnCount; k++) {
                    final int index = getPrimaryColumnIndex(base, k);
                    Misc.free(columns.getAndSetQuick(index, null));
                    Misc.free(columns.getAndSetQuick(index + 1, null));
                    Misc.free(bitmapIndexes.getAndSetQuick(index, null));
                    Misc.free(bitmapIndexes.getAndSetQuick(index + 1, null));
                }
                partitionRowCounts.setQuick(i, -1);
            }
        }
        reloadSymbolMapCounts();
        partitionCount = calculatePartitionCount();
        if (partitionCount > 0) {
            updateCapacities();
        }
    }

    private int calculatePartitionCount() {
        if (minTimestamp == Long.MAX_VALUE) {
            return 0;
        } else {
            return maxTimestamp == Long.MIN_VALUE ? 1 : getPartitionCountBetweenTimestamps(
                    minTimestamp,
                    floorToPartitionTimestamp(maxTimestamp)
            ) + 1;
        }
    }

    private void checkDefaultPartitionExistsAndUpdatePartitionCount() {
        if (maxTimestamp == Numbers.LONG_NaN) {
            partitionCount = 0;
        } else {
            Path path = pathGenDefault();
            partitionCount = ff.exists(path) ? 1 : 0;
            path.trimTo(rootLen);
        }
    }

    private void closeColumn(int columnBase, int columnIndex) {
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        Misc.free(columns.getAndSetQuick(index, NullColumn.INSTANCE));
        Misc.free(columns.getAndSetQuick(index + 1, NullColumn.INSTANCE));
        Misc.free(bitmapIndexes.getAndSetQuick(index, null));
        Misc.free(bitmapIndexes.getAndSetQuick(index + 1, null));
    }

    private void closeRemovedPartitions() {
        for (int i = 0, n = removedPartitions.size(); i < n; i++) {
            final long timestamp = removedPartitions.get(i);

            int partitionIndex = getPartitionCountBetweenTimestamps(prevMinTimestamp, timestamp);
            if (partitionIndex > -1) {
                if (partitionIndex < partitionCount) {
                    if (getPartitionRowCount(partitionIndex) != -1) {
                        // this is an open partition
                        int base = getColumnBase(partitionIndex);
                        for (int k = 0; k < columnCount; k++) {
                            closeColumn(base, k);
                        }
                        partitionRowCounts.setQuick(partitionIndex, -1);
                    }
                    // partition has not yet been opened
                } else {
                    LOG.error()
                            .$("partition index is out of range [partitionIndex=").$(partitionIndex)
                            .$(", partitionCount=").$(partitionCount)
                            .$(", timestamp=").$ts(timestamp)
                            .$(']').$();
                }
            }
        }
        LOG.info()
                .$("purging memory [prevMinTimestamp=").$ts(prevMinTimestamp)
                .$(", minTimestamp=").$ts(minTimestamp)
                .$(", partitionCount=").$(partitionCount)
                .$(']').$();

        // adjust columns list when leading partitions have been removed
        if (prevMinTimestamp != minTimestamp) {
            assert prevMinTimestamp < minTimestamp;
            int delta = getPartitionCountBetweenTimestamps(prevMinTimestamp, minTimestamp);
            columns.remove(2, getColumnBase(delta) + 1);
            prevMinTimestamp = minTimestamp;
            partitionCount -= delta;
        }
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
        final boolean reload = (mem1 instanceof ReadOnlyMemory || mem1 instanceof NullColumn) && mem1.isDeleted();
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
            Path path = partitionPathGenerator.generate(this, getPartitionIndex(columnBase));
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
                final Path path = partitionPathGenerator.generate(this, partitionIndex);
                final long partitionRowCount = partitionRowCounts.getQuick(partitionIndex);
                final boolean lastPartition = partitionIndex == partitionCount - 1;
                for (int i = 0; i < columnCount; i++) {
                    final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8) - 1;
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

    ReadOnlyColumn getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
    }

    int getColumnCount() {
        return columnCount;
    }

    long getColumnTop(int base, int columnIndex) {
        return this.columnTops.getQuick(base / 2 + columnIndex);
    }

    int getPartitionIndex(int columnBase) {
        return columnBase >>> columnCountBits;
    }

    long getPartitionRowCount(int partitionIndex) {
        assert partitionRowCounts.size() > 0;
        return partitionRowCounts.getQuick(partitionIndex);
    }

    long getTransientRowCount() {
        return transientRowCount;
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

    private void incrementPartitionCountBy(int delta) {
        partitionRowCounts.seed(partitionCount, delta, -1);
        partitionCount += delta;
        updateCapacities();
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

    long openPartition(int partitionIndex) {
        final long size = getPartitionRowCount(partitionIndex);
        if (size != -1) {
            return size;
        }
        return openPartition0(partitionIndex);
    }

    private long openPartition0(int partitionIndex) {
        // is this table is partitioned?
        if (timestampAddMethod != null
                && removedPartitions.contains(timestampAddMethod.calculate(
                minTimestamp, partitionIndex
        ))) {
            return -1;
        }

        // todo: this may not be the best place to check if partition is out of range
        if (maxTimestamp == Long.MIN_VALUE) {
            return -1;
        }

        try {
            Path path = partitionPathGenerator.generate(this, partitionIndex);
            if (ff.exists(path)) {

                path.chopZ();

                final boolean lastPartition = partitionIndex == partitionCount - 1;
                final long partitionSize = lastPartition ? transientRowCount : TableUtils.readPartitionSize(ff, path, tempMem8b);

                LOG.info()
                        .$("open partition ").utf8(path.$())
                        .$(" [rowCount=").$(partitionSize)
                        .$(", transientRowCount=").$(transientRowCount)
                        .$(", partitionIndex=").$(partitionIndex)
                        .$(", partitionCount=").$(partitionCount)
                        .$(']').$();

                if (partitionSize > 0) {
                    openPartitionColumns(path, getColumnBase(partitionIndex), partitionSize, lastPartition);
                    partitionRowCounts.setQuick(partitionIndex, partitionSize);
                    if (maxTimestamp != Numbers.LONG_NaN) {
                        if (reloadMethod == FIRST_TIME_PARTITIONED_RELOAD_METHOD) {
                            reloadMethod = PARTITIONED_RELOAD_METHOD;
                        } else if (reloadMethod == FIRST_TIME_NON_PARTITIONED_RELOAD_METHOD) {
                            reloadMethod = NON_PARTITIONED_RELOAD_METHOD;
                        }
                    }
                }

                return partitionSize;
            }
            return -1;
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

    private ReadOnlyColumn openTxnFile() {
        try {
            return new ReadOnlyMemory(ff, path.concat(TableUtils.TXN_FILE_NAME).$(), ff.getPageSize(), TableUtils.getSymbolWriterIndexOffset(0));
        } finally {
            path.trimTo(rootLen);
        }
    }

    private Path pathGenDay(int partitionIndex) {
        TableUtils.fmtDay.format(
                Timestamps.addDays(minTimestamp, partitionIndex),
                null, // this format does not need locale access
                null,
                path.put(Files.SEPARATOR)
        );
        return path.$();
    }

    private Path pathGenDefault() {
        return path.concat(TableUtils.DEFAULT_PARTITION_NAME).$();
    }

    private Path pathGenMonth(int partitionIndex) {
        TableUtils.fmtMonth.format(
                Timestamps.addMonths(minTimestamp, partitionIndex),
                null, // this format does not need locale access
                null,
                path.put(Files.SEPARATOR)
        );
        return path.$();
    }

    private Path pathGenYear(int partitionIndex) {
        TableUtils.fmtYear.format(
                Timestamps.addYear(minTimestamp, partitionIndex),
                null, // this format does not need locale access
                null,
                path.put(Files.SEPARATOR)
        );
        return path.$();
    }

    private boolean readTxn() {
        // fast path
        return this.txn != txMem.getLong(TableUtils.TX_OFFSET_TXN) && readTxnSlow();
    }

    private boolean readTxnSlow() {
        int count = 0;
        final long deadline = configuration.getMicrosecondClock().getTicks() + configuration.getSpinLockTimeoutUs();
        while (true) {
            long txn = txMem.getLong(TableUtils.TX_OFFSET_TXN);

            // exit if this is the same as we already have
            if (txn == this.txn) {
                return false;
            }

            // make sure this isn't re-ordered
            Unsafe.getUnsafe().loadFence();

            // do start and end sequences match? if so we have a chance at stable read
            if (txn == txMem.getLong(TableUtils.TX_OFFSET_TXN_CHECK)) {
                // great, we seem to have got stable read, lets do some reading
                // and check later if it was worth it

                Unsafe.getUnsafe().loadFence();
                final long transientRowCount = txMem.getLong(TableUtils.TX_OFFSET_TRANSIENT_ROW_COUNT);
                final long fixedRowCount = txMem.getLong(TableUtils.TX_OFFSET_FIXED_ROW_COUNT);
                final long minTimestamp = txMem.getLong(TX_OFFSET_MIN_TIMESTAMP);
                final long maxTimestamp = txMem.getLong(TableUtils.TX_OFFSET_MAX_TIMESTAMP);
                final long structVersion = txMem.getLong(TableUtils.TX_OFFSET_STRUCT_VERSION);
                final long dataVersion = txMem.getLong(TableUtils.TX_OFFSET_DATA_VERSION);
                final long partitionTableVersion = txMem.getLong(TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION);

                this.symbolCountSnapshot.clear();
                int symbolMapCount = txMem.getInt(TableUtils.TX_OFFSET_MAP_WRITER_COUNT);
                if (symbolMapCount > 0) {
                    txMem.grow(TableUtils.getSymbolWriterIndexOffset(symbolMapCount));
                    for (int i = 0; i < symbolMapCount; i++) {
                        symbolCountSnapshot.add(txMem.getInt(TableUtils.getSymbolWriterIndexOffset(i)));
                    }
                }

                txMem.grow(TableUtils.getPartitionTableIndexOffset(symbolMapCount, 0));

                this.removedPartitions.clear();
                int partitionTableSize = txMem.getInt(TableUtils.getPartitionTableSizeOffset(symbolMapCount));
                if (partitionTableSize > 0) {
                    txMem.grow(TableUtils.getPartitionTableIndexOffset(symbolMapCount, partitionTableSize));
                    for (int i = 0; i < partitionTableSize; i++) {
                        this.removedPartitions.add(txMem.getLong(TableUtils.getPartitionTableIndexOffset(symbolMapCount, i)));
                    }
                }

                Unsafe.getUnsafe().loadFence();
                // ok, we have snapshot, check if our snapshot is stable
                if (txn == txMem.getLong(TableUtils.TX_OFFSET_TXN)) {
                    // good, very stable, congrats
                    this.txn = txn;
                    this.transientRowCount = transientRowCount;
                    this.rowCount = fixedRowCount + transientRowCount;
                    this.prevMinTimestamp = this.minTimestamp;
                    if (minTimestamp == Long.MAX_VALUE) {
                        this.minTimestamp = Long.MAX_VALUE;
                    } else {
                        this.minTimestamp = timestampFloorMethod.floor(minTimestamp);
                    }
                    this.maxTimestamp = maxTimestamp;
                    this.structVersion = structVersion;
                    this.dataVersion = dataVersion;
                    this.partitionTableVersion = partitionTableVersion;
                    LOG.info()
                            .$("new transaction [txn=").$(txn)
                            .$(", transientRowCount=").$(transientRowCount)
                            .$(", fixedRowCount=").$(fixedRowCount)
                            .$(", minTimestamp=").$ts(this.minTimestamp)
                            .$(", maxTimestamp=").$ts(this.maxTimestamp)
                            .$(", attempts=").$(count)
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

                if (mem1 != null && mem1 != NullColumn.INSTANCE) {
                    mem1.of(ff, path, ff.getMapPageSize(), ff.length(path));
                } else {
                    if (lastPartition) {
                        mem1 = new ReadOnlyMemory(ff, path, ff.getMapPageSize(), 0);
                    } else {
                        mem1 = new OnePageMemory(ff, path, ff.length(path));
                    }
                    columns.setQuick(primaryIndex, mem1);
                }

                final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), name, plen, tempMem8b);
                final int type = metadata.getColumnType(columnIndex);

                switch (type) {
                    case ColumnType.BINARY:
                    case ColumnType.STRING:
                        TableUtils.iFile(path.trimTo(plen), name);
                        if (mem2 != null && mem2 != NullColumn.INSTANCE) {
                            mem2.of(ff, path, ff.getMapPageSize(), ff.length(path));
                        } else {
                            if (lastPartition) {
                                mem2 = new ReadOnlyMemory(ff, path, ff.getMapPageSize(), 0);
                            } else {
                                mem2 = new OnePageMemory(ff, path, ff.length(path));
                            }
                            columns.setQuick(secondaryIndex, mem2);
                        }
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

    private boolean reloadInitialNonPartitioned() {
        final long dataVersion = this.dataVersion;
        final long structVersion = this.structVersion;
        final long partitionTableVersion = this.partitionTableVersion;

        if (readTxn()) {
            reloadStruct();
            reloadSymbolMapCounts();
            checkDefaultPartitionExistsAndUpdatePartitionCount();
            if (partitionCount > 0) {
                updateCapacities();
                reloadMethod = NON_PARTITIONED_RELOAD_METHOD;
                return true;
            }
        }
        return dataVersion != this.dataVersion
                || structVersion != this.structVersion
                || partitionTableVersion != this.partitionTableVersion;
    }

    private boolean reloadInitialPartitioned() {
        if (readTxn()) {
            reloadStruct();
            return reloadInitialPartitioned0();
        }
        return false;
    }

    private boolean reloadInitialPartitioned0() {
        reloadSymbolMapCounts();
        partitionCount = calculatePartitionCount();
        if (partitionCount > 0) {
            updateCapacities();
            if (maxTimestamp != Long.MIN_VALUE) {
                reloadMethod = PARTITIONED_RELOAD_METHOD;
            }
        }
        return true;
    }

    private boolean reloadNonPartitioned() {
        // calling readTxn will set "rowCount" member variable
        if (readTxn()) {
            reloadStruct();
            if (getPartitionRowCount(0) > -1) {
                reloadPartition(0, rowCount);
            } else {
                openPartition0(0);
                reloadSymbolMapCounts();
            }
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
        partitionRowCounts.setQuick(partitionIndex, rowCount);
    }

    private boolean reloadPartitioned() {
        assert timestampFloorMethod != null;
        final long currentPartitionTimestamp = maxTimestamp == Long.MIN_VALUE ? maxTimestamp : floorToPartitionTimestamp(maxTimestamp);
        final long dataVersion = this.dataVersion;
        if (readTxn()) {
            reloadStruct();
            if (this.dataVersion != dataVersion) {
                applyTruncate();
                return true;
            }

            if (partitionCount == 0) {
                // old partition count was 0
                reloadSymbolMapCounts();
                incrementPartitionCountBy(calculatePartitionCount());
                return true;
            }

            assert intervalLengthMethod != null;

            //  calculate timestamp delta between before and after reload.
            int delta = getPartitionCountBetweenTimestamps(currentPartitionTimestamp, floorToPartitionTimestamp(maxTimestamp));
            int partitionIndex = partitionCount - 1;
            // do we have something to reload?
            if (getPartitionRowCount(partitionIndex) > -1) {
                if (delta > 0) {
                    incrementPartitionCountBy(delta);
                    Path path = partitionPathGenerator.generate(this, partitionIndex);
                    try {
                        reloadPartition(partitionIndex, TableUtils.readPartitionSize(ff, path.chopZ(), tempMem8b));
                    } finally {
                        path.trimTo(rootLen);
                    }
                } else {
                    reloadPartition(partitionIndex, transientRowCount);
                }
                return true;
            }

            if (delta > 0) {
                // although we have nothing to reload we still have to bump partition count
                incrementPartitionCountBy(delta);
            }

            reloadSymbolMapCounts();
            return true;
        }
        return false;
    }

    private void reloadStruct() {
        // fast path
        if (this.prevStructVersion == this.structVersion && this.prevPartitionTableVersion == this.partitionTableVersion) {
            return;
        }
        reloadStructSlow();
    }

    private void reloadStructSlow() {
        if (this.prevStructVersion != this.structVersion) {
            reloadColumnChanges();
            this.prevStructVersion = this.structVersion;
        }

        if (this.prevPartitionTableVersion != this.partitionTableVersion) {
            closeRemovedPartitions();
            this.prevPartitionTableVersion = partitionTableVersion;
        }
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
                final Path path = partitionPathGenerator.generate(this, partitionIndex);
                final long partitionRowCount = partitionRowCounts.getQuick(partitionIndex);
                final boolean lastPartition = partitionIndex == partitionCount - 1;

                Unsafe.getUnsafe().setMemory(pState, columnCount, (byte) 0);

                for (int i = 0; i < columnCount; i++) {

                    if (isEntryToBeProcessed(pState, i)) {
                        final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8) - 1;

                        if (copyFrom == i) {
                            // It appears that column hasn't changed its position. There are three possibilities here:
                            // 1. Column has been deleted and re-added by the same name. We must check if file
                            //    descriptor is still valid. If it isn't, reload the column from disk
                            // 2. Column has been forced out of the reader via closeColumnForRemove(). This is required
                            //    on Windows before column can be deleted. In this case we must check for marker
                            //    instance and the column from disk
                            // 3. Column hasn't been altered and we can skip to next column.
                            ReadOnlyColumn col = columns.getQuick(getPrimaryColumnIndex(base, i));
                            if ((col instanceof ReadOnlyMemory && col.isDeleted()) || col instanceof NullColumn) {
                                reloadColumnAt(path, columns, columnTops, bitmapIndexes, base, i, partitionRowCount, lastPartition);
                            }
                            continue;
                        }

                        if (copyFrom > -1) {
                            fetchColumnsFrom(this.columns, this.columnTops, this.bitmapIndexes, base, copyFrom);
                            copyColumnsTo(this.columns, this.columnTops, this.bitmapIndexes, base, i, partitionRowCount, lastPartition);
                            int copyTo = Unsafe.getUnsafe().getInt(pIndexBase + i * 8 + 4) - 1;
                            while (copyTo > -1 && isEntryToBeProcessed(pState, copyTo)) {
                                copyColumnsTo(this.columns, this.columnTops, this.bitmapIndexes, base, copyTo, partitionRowCount, lastPartition);
                                copyTo = Unsafe.getUnsafe().getInt(pIndexBase + (copyTo - 1) * 8 + 4);
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

    private void updateCapacities() {
        int capacity = getColumnBase(partitionCount);
        columns.setPos(capacity + 2);
        bitmapIndexes.setPos(capacity + 2);
        this.partitionRowCounts.seed(partitionCount, -1);
        this.columnTops.setPos(capacity / 2);
    }

    @FunctionalInterface
    private interface IntervalLengthMethod {
        long calculate(long minTimestamp, long maxTimestamp);
    }

    @FunctionalInterface
    private interface ReloadMethod {
        boolean reload(TableReader reader);
    }

    @FunctionalInterface
    private interface PartitionPathGenerator {
        Path generate(TableReader reader, int partitionIndex);
    }

    private static class ColumnCopyStruct {
        ReadOnlyColumn mem1;
        ReadOnlyColumn mem2;
        BitmapIndexReader backwardReader;
        BitmapIndexReader forwardReader;
        long top;
    }

}
