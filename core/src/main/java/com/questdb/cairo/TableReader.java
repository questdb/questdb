/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.microtime.DateFormat;
import com.questdb.std.microtime.DateLocaleFactory;
import com.questdb.std.microtime.Dates;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

public class TableReader implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private static final PartitionPathGenerator YEAR_GEN = TableReader::pathGenYear;
    private static final PartitionPathGenerator MONTH_GEN = TableReader::pathGenMonth;
    private static final PartitionPathGenerator DAY_GEN = TableReader::pathGenDay;
    private static final PartitionPathGenerator DEFAULT_GEN = (reader, partitionIndex) -> reader.pathGenDefault();
    private static final ReloadMethod PARTITIONED_RELOAD_METHOD = TableReader::reloadPartitioned;
    private static final ReloadMethod NON_PARTITIONED_RELOAD_METHOD = TableReader::reloadNonPartitioned;
    private static final ReloadMethod FIRST_TIME_PARTITIONED_RELOAD_METHOD = TableReader::reloadInitialPartitioned;
    private static final ReloadMethod FIRST_TIME_NON_PARTITIONED_RELOAD_METHOD = TableReader::reloadInitialNonPartitioned;

    private final ColumnCopyStruct tempCopyStruct = new ColumnCopyStruct();
    private final FilesFacade ff;
    private final Path path;
    private final int rootLen;
    private final ReadOnlyMemory txMem;
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final TableReaderMetadata metadata;
    private final LongList partitionRowCounts;
    private final PartitionPathGenerator partitionPathGenerator;
    private final TableReaderRecordCursor recordCursor = new TableReaderRecordCursor();
    private final DateFormat dateFormat;
    private final TimestampFloorMethod timestampFloorMethod;
    private final IntervalLengthMethod intervalLengthMethod;
    private final String tableName;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final CairoConfiguration configuration;
    private final IntList symbolCountSnapshot = new IntList();
    private LongList columnTops;
    private ObjList<ReadOnlyColumn> columns;
    private ObjList<BitmapIndexReader> bitmapIndexes;
    private int columnCount;
    private int columnCountBits;
    private long transientRowCount;
    private long structVersion;
    private long prevStructVersion;
    private long rowCount;
    private long txn = TableUtils.INITIAL_TXN;
    private long maxTimestamp = Numbers.LONG_NaN;
    private int partitionCount;
    private long partitionMin = Long.MAX_VALUE;
    private ReloadMethod reloadMethod;
    private long tempMem8b = Unsafe.malloc(8);

    public TableReader(CairoConfiguration configuration, CharSequence tableName) {
        LOG.info().$("open '").utf8(tableName).$('\'').$();
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.tableName = Chars.stringOf(tableName);
        this.path = new Path().of(configuration.getRoot()).concat(tableName);
        this.rootLen = path.length();
        try {
            failOnPendingTodo();
            this.txMem = openTxnFile();
            this.metadata = openMetaFile();
            this.columnCount = this.metadata.getColumnCount();
            this.columnCountBits = getColumnBits(columnCount);
            readTxn();
            openSymbolMaps();
            this.prevStructVersion = structVersion;
            switch (this.metadata.getPartitionBy()) {
                case PartitionBy.DAY:
                    partitionPathGenerator = DAY_GEN;
                    reloadMethod = FIRST_TIME_PARTITIONED_RELOAD_METHOD;
                    timestampFloorMethod = Dates::floorDD;
                    intervalLengthMethod = Dates::getDaysBetween;
                    dateFormat = TableUtils.fmtDay;
                    partitionMin = findPartitionMinimum(dateFormat);
                    partitionCount = calculatePartitionCount();
                    break;
                case PartitionBy.MONTH:
                    partitionPathGenerator = MONTH_GEN;
                    reloadMethod = FIRST_TIME_PARTITIONED_RELOAD_METHOD;
                    timestampFloorMethod = Dates::floorMM;
                    intervalLengthMethod = Dates::getMonthsBetween;
                    dateFormat = TableUtils.fmtMonth;
                    partitionMin = findPartitionMinimum(dateFormat);
                    partitionCount = calculatePartitionCount();
                    break;
                case PartitionBy.YEAR:
                    partitionPathGenerator = YEAR_GEN;
                    reloadMethod = FIRST_TIME_PARTITIONED_RELOAD_METHOD;
                    timestampFloorMethod = Dates::floorYYYY;
                    intervalLengthMethod = Dates::getYearsBetween;
                    dateFormat = TableUtils.fmtYear;
                    partitionMin = findPartitionMinimum(dateFormat);
                    partitionCount = calculatePartitionCount();
                    break;
                default:
                    partitionPathGenerator = DEFAULT_GEN;
                    reloadMethod = FIRST_TIME_NON_PARTITIONED_RELOAD_METHOD;
                    timestampFloorMethod = null;
                    intervalLengthMethod = null;
                    dateFormat = null;
                    countDefaultPartitions();
                    break;
            }

            int capacity = getColumnBase(partitionCount);
            this.columns = new ObjList<>(capacity);
            this.columns.setPos(capacity);
            this.bitmapIndexes = new ObjList<>(capacity / 2);
            this.bitmapIndexes.setPos(capacity / 2);
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
            final int base = getColumnBase(partitionIndex);
            // replace columns we force closed with special marker object
            // when we come to reloading table reader we would be able to
            // tell that column has to be attempted to be read from disk
            Misc.free(columns.getAndSetQuick(getPrimaryColumnIndex(base, columnIndex), ForceNullColumn.INSTANCE));
            Misc.free(columns.getAndSetQuick(getSecondaryColumnIndex(base, columnIndex), ForceNullColumn.INSTANCE));
            Misc.free(bitmapIndexes.getAndSetQuick(base / 2 + columnIndex, null));
        }

        if (metadata.getColumnType(columnIndex) == ColumnType.SYMBOL) {
            // same goes for symbol map reader - replace object with maker instance
            Misc.free(symbolMapReaders.getAndSetQuick(columnIndex, ForceEmptySymbolMapReader.INSTANCE));
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

    public BitmapIndexReader getBitmapIndexReader(int columnBase, int columnIndex) {
        BitmapIndexReader reader = bitmapIndexes.getQuick(columnBase / 2 + columnIndex);
        return reader == null ? createBitmapIndexReaderAt(columnBase, columnIndex) : reader;
    }

    public RecordCursor getCursor() {
        recordCursor.toTop();
        return recordCursor;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getPartitionCountBetweenTimestamps(long partitionTimestamp1, long partitionTimestamp2) {
        return (int) intervalLengthMethod.calculate(partitionTimestamp1, partitionTimestamp2);
    }

    public long getPartitionMin() {
        return partitionMin;
    }

    public int getPartitionedBy() {
        return metadata.getPartitionBy();
    }

    public SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex);
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public boolean isOpen() {
        return tempMem8b != 0;
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
            if (copyFrom == i + 1) {
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

    private static int getColumnBits(int columnCount) {
        return Numbers.msb(Numbers.ceilPow2(columnCount) * 2);
    }

    static int getPrimaryColumnIndex(int base, int index) {
        return base + index * 2;
    }

    static int getSecondaryColumnIndex(int base, int index) {
        return getPrimaryColumnIndex(base, index) + 1;
    }

    private static long readPartitionSize(FilesFacade ff, Path path, long tempMem) {
        int plen = path.length();
        try {
            if (ff.exists(path.concat(TableUtils.ARCHIVE_FILE_NAME).$())) {
                long fd = ff.openRO(path);
                if (fd == -1) {
                    throw CairoException.instance(Os.errno()).put("Cannot open: ").put(path);
                }

                try {
                    if (ff.read(fd, tempMem, 8, 0) != 8) {
                        throw CairoException.instance(Os.errno()).put("Cannot read: ").put(path);
                    }
                    return Unsafe.getUnsafe().getLong(tempMem);
                } finally {
                    ff.close(fd);
                }
            } else {
                throw CairoException.instance(0).put("Doesn't exist: ").put(path);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private static boolean isEntryToBeProcessed(long address, int index) {
        if (Unsafe.getUnsafe().getByte(address + index) == -1) {
            return false;
        }
        Unsafe.getUnsafe().putByte(address + index, (byte) -1);
        return true;
    }

    private static void growColumn(ReadOnlyColumn mem1, ReadOnlyColumn mem2, int type, long rowCount) {
        long offset;
        long len;
        if (rowCount > 0) {
            // subtract column top
            switch (type) {
                case ColumnType.BINARY:
                    assert mem2 != null;
                    mem2.grow(rowCount * 8);
                    offset = mem2.getLong((rowCount - 1) * 8);
                    // grow data column to value offset + length, so that we can read length
                    mem1.grow(offset + 8);
                    len = mem1.getLong(offset);
                    if (len > 0) {
                        mem1.grow(offset + len + 8);
                    }
                    break;
                case ColumnType.STRING:
                    assert mem2 != null;
                    mem2.grow(rowCount * 8);
                    offset = mem2.getLong((rowCount - 1) * 8);
                    mem1.grow(offset + 4);
                    len = mem1.getInt(offset);
                    if (len > 0) {
                        mem1.grow(offset + len * 2 + 4);
                    }
                    break;
                default:
                    mem1.grow(rowCount << ColumnType.pow2SizeOf(type));
                    break;
            }
        }
    }

    private int calculatePartitionCount() {
        if (partitionMin == Long.MAX_VALUE) {
            return 0;
        } else {
            return maxTimestamp == Numbers.LONG_NaN ? 1 : getPartitionCountBetweenTimestamps(partitionMin, floorToPartitionTimestamp(maxTimestamp)) + 1;
        }
    }

    private void copyColumnsTo(ObjList<ReadOnlyColumn> columns, LongList columnTops, ObjList<BitmapIndexReader> indexReaders, int columnBase, int columnIndex, long partitionRowCount) {
        ReadOnlyColumn mem1 = tempCopyStruct.mem1;
        boolean reload = (mem1 instanceof ReadOnlyMemory || mem1 instanceof ForceNullColumn) && mem1.isDeleted();
        tempCopyStruct.mem1 = columns.getAndSetQuick(getPrimaryColumnIndex(columnBase, columnIndex), mem1);
        tempCopyStruct.mem2 = columns.getAndSetQuick(getSecondaryColumnIndex(columnBase, columnIndex), tempCopyStruct.mem2);
        tempCopyStruct.top = columnTops.getAndSetQuick(columnBase / 2 + columnIndex, tempCopyStruct.top);
        tempCopyStruct.bitmapIndexReader = indexReaders.getAndSetQuick(columnBase / 2 + columnIndex, tempCopyStruct.bitmapIndexReader);
        if (reload) {
            reloadColumnAt(path, columns, columnTops, indexReaders, columnBase, columnIndex, partitionRowCount);
        }
    }

    private SymbolMapReader copyOrRenewSymbolMapReader(SymbolMapReader reader, int columnIndex) {
        if (reader != null && reader.isDeleted()) {
            reader = reloadSymbolMapReader(columnIndex, reader);
        }
        return symbolMapReaders.getAndSetQuick(columnIndex, reader);
    }

    private void countDefaultPartitions() {
        if (maxTimestamp == Numbers.LONG_NaN) {
            partitionCount = 0;
        } else {
            Path path = pathGenDefault();
            partitionCount = ff.exists(path) ? 1 : 0;
            path.trimTo(rootLen);
        }
    }

    @NotNull
    private BitmapIndexReader createBitmapIndexReaderAt(int columnBase, int columnIndex) {
        BitmapIndexReader reader;
        if (!metadata.isColumnIndexed(columnIndex)) {
            throw CairoException.instance(0).put("Not indexed: ").put(metadata.getColumnName(columnIndex));
        }

        ReadOnlyColumn col = columns.getQuick(getPrimaryColumnIndex(columnBase, columnIndex));
        if (col instanceof NullColumn) {
            reader = new BitmapIndexNullReader();
        } else {
            Path path = partitionPathGenerator.generate(this, getPartitionIndex(columnBase));
            try {
                reader = new BitmapIndexBackwardReader(configuration, path.chopZ(), metadata.getColumnName(columnIndex), getColumnTop(columnBase, columnIndex));
            } finally {
                path.trimTo(rootLen);
            }
        }
        bitmapIndexes.setQuick(columnBase / 2 + columnIndex, reader);
        return reader;
    }

    private void createNewColumnList(int columnCount, long pTransitionIndex, int columnBits) {
        int capacity = partitionCount << columnBits;
        final ObjList<ReadOnlyColumn> columns = new ObjList<>(capacity);
        final LongList columnTops = new LongList(capacity / 2);
        final ObjList<BitmapIndexReader> indexReaders = new ObjList<>(capacity / 2);
        columns.setPos(capacity);
        columnTops.setPos(capacity / 2);
        indexReaders.setPos(capacity / 2);
        final long pIndexBase = pTransitionIndex + 8;

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final int base = partitionIndex << columnBits;
            final int oldBase = partitionIndex << columnCountBits;
            try {
                Path path = partitionPathGenerator.generate(this, partitionIndex);
                final long partitionRowCount = partitionRowCounts.getQuick(partitionIndex);
                for (int i = 0; i < columnCount; i++) {
                    final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8) - 1;
                    if (copyFrom > -1) {
                        fetchColumnsFrom(this.columns, this.columnTops, this.bitmapIndexes, oldBase, copyFrom);
                        copyColumnsTo(columns, columnTops, indexReaders, base, i, partitionRowCount);
                    } else {
                        // new instance
                        reloadColumnAt(path, columns, columnTops, indexReaders, base, i, partitionRowCount);
                    }
                }

                // free remaining columns
                for (int i = 0; i < this.columnCount; i++) {
                    Misc.free(this.columns.getQuick(getPrimaryColumnIndex(oldBase, i)));
                    Misc.free(this.columns.getQuick(getSecondaryColumnIndex(oldBase, i)));
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

    private void doReloadStruct() {
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
        tempCopyStruct.mem1 = columns.getAndSetQuick(getPrimaryColumnIndex(columnBase, columnIndex), null);
        tempCopyStruct.mem2 = columns.getAndSetQuick(getSecondaryColumnIndex(columnBase, columnIndex), null);
        tempCopyStruct.top = columnTops.getQuick(columnBase / 2 + columnIndex);
        tempCopyStruct.bitmapIndexReader = indexReaders.getAndSetQuick(columnBase / 2 + columnIndex, null);
    }

    private long findPartitionMinimum(DateFormat partitionDirFmt) {
        long partitionMin = Long.MAX_VALUE;
        try {
            long p = ff.findFirst(path.$());
            if (p > 0) {
                try {
                    do {
                        int type = ff.findType(p);
                        if (type == Files.DT_DIR || type == Files.DT_LNK) {
                            try {
                                long time = partitionDirFmt.parse(nativeLPSZ.of(ff.findName(p)), DateLocaleFactory.INSTANCE.getDefaultDateLocale());
                                if (time < partitionMin && time <= maxTimestamp) {
                                    partitionMin = time;
                                }
                            } catch (NumericException ignore) {
                            }
                        }
                    } while (ff.findNext(p) > 0);
                } finally {
                    ff.findClose(p);
                }
            }
        } finally {
            path.trimTo(rootLen);
        }

        return partitionMin;
    }

    private void freeBitmapIndexCache() {
        if (bitmapIndexes != null) {
            for (int i = 0, n = bitmapIndexes.size(); i < n; i++) {
                Misc.free(bitmapIndexes.getQuick(i));
            }
        }
    }

    private void freeColumns() {
        if (columns != null) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                Misc.free(columns.getQuick(i));
            }
        }
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

    int getColumnBase(int partitionIndex) {
        return partitionIndex << columnCountBits;
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
        try {
            Path path = partitionPathGenerator.generate(this, partitionIndex);
            if (ff.exists(path)) {

                path.chopZ();

                final long partitionSize = partitionIndex == partitionCount - 1 ? transientRowCount : readPartitionSize(ff, path, tempMem8b);

                LOG.info().$("open partition ").utf8(path.$()).$(" [rowCount=").$(partitionSize).$(", transientRowCount=").$(transientRowCount).$(", partitionIndex=").$(partitionIndex).$(", partitionCount=").$(partitionCount).$(']').$();

                if (partitionSize > 0) {
                    openPartitionColumns(path, getColumnBase(partitionIndex), partitionSize);
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

    private void openPartitionColumns(Path path, int columnBase, long partitionRowCount) {
        for (int i = 0; i < columnCount; i++) {
            if (columns.getQuick(getPrimaryColumnIndex(columnBase, i)) == null) {
                reloadColumnAt(path, this.columns, this.columnTops, this.bitmapIndexes, columnBase, i, partitionRowCount);
            }
        }
    }

    private void openSymbolMaps() {
        int symbolColumnIndex = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getColumnType(i) == ColumnType.SYMBOL) {
                SymbolMapReaderImpl symbolMapReader = new SymbolMapReaderImpl(configuration, path, metadata.getColumnName(i), symbolCountSnapshot.getQuick(symbolColumnIndex++));
                symbolMapReaders.extendAndSet(i, symbolMapReader);
            }
        }
    }

    private ReadOnlyMemory openTxnFile() {
        try {
            return new ReadOnlyMemory(ff, path.concat(TableUtils.TXN_FILE_NAME).$(), ff.getPageSize(), TableUtils.TX_OFFSET_MAP_WRITER_COUNT + 4);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private Path pathGenDay(int partitionIndex) {
        TableUtils.fmtDay.format(
                Dates.addDays(partitionMin, partitionIndex),
                DateLocaleFactory.INSTANCE.getDefaultDateLocale(),
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
                Dates.addMonths(partitionMin, partitionIndex),
                DateLocaleFactory.INSTANCE.getDefaultDateLocale(),
                null,
                path.put(Files.SEPARATOR)
        );
        return path.$();
    }

    private Path pathGenYear(int partitionIndex) {
        TableUtils.fmtYear.format(
                Dates.addYear(partitionMin, partitionIndex),
                DateLocaleFactory.INSTANCE.getDefaultDateLocale(),
                null,
                path.put(Files.SEPARATOR)
        );
        return path.$();
    }

    private boolean readTxn() {
        int count = 0;
        final long deadline = configuration.getMicrosecondClock().getTicks() + configuration.getSpinLockTimeoutUs();
        while (true) {
            long txn = txMem.getLong(TableUtils.TX_OFFSET_TXN);

            // exit if this is the same as we alrwady have
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
                final long maxTimestamp = txMem.getLong(TableUtils.TX_OFFSET_MAX_TIMESTAMP);
                final long structVersion = txMem.getLong(TableUtils.TX_OFFSET_STRUCT_VERSION);

                this.symbolCountSnapshot.clear();
                int symbolMapCount = txMem.getInt(TableUtils.TX_OFFSET_MAP_WRITER_COUNT);
                if (symbolMapCount > 0) {
                    txMem.grow(TableUtils.TX_OFFSET_MAP_WRITER_COUNT + 4 + symbolMapCount * 4);
                    for (int i = 0; i < symbolMapCount; i++) {
                        symbolCountSnapshot.add(txMem.getInt(TableUtils.TX_OFFSET_MAP_WRITER_COUNT + 4 + i * 4));
                    }
                }

                Unsafe.getUnsafe().loadFence();
                // ok, we have snapshot, check if our snapshot is stable
                if (txn == txMem.getLong(TableUtils.TX_OFFSET_TXN)) {
                    // good, very stable, congrats
                    this.txn = txn;
                    this.transientRowCount = transientRowCount;
                    this.rowCount = fixedRowCount + transientRowCount;
                    this.maxTimestamp = maxTimestamp;
                    this.structVersion = structVersion;
                    LOG.info().$("new transaction [txn=").$(txn).$(", transientRowCount=").$(transientRowCount).$(", fixedRowCount=").$(fixedRowCount).$(", maxTimestamp=").$(maxTimestamp).$(", attempts=").$(count).$(']').$();
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

    private void reloadColumnAt(Path path, ObjList<ReadOnlyColumn> columns, LongList columnTops, ObjList<BitmapIndexReader> indexReaders, int columnBase, int columnIndex, long partitionRowCount) {
        int plen = path.length();
        try {
            final CharSequence name = metadata.getColumnName(columnIndex);
            final int primaryIndex = getPrimaryColumnIndex(columnBase, columnIndex);
            final int secondaryIndex = getSecondaryColumnIndex(columnBase, columnIndex);
            final int topIndex = columnBase / 2 + columnIndex;

            ReadOnlyColumn mem1 = columns.getQuick(primaryIndex);
            ReadOnlyColumn mem2 = columns.getQuick(secondaryIndex);

            BitmapIndexReader indexReader = indexReaders.getQuick(topIndex);
            if (ff.exists(TableUtils.dFile(path.trimTo(plen), name))) {


                if (mem1 instanceof ReadOnlyMemory) {
                    ((ReadOnlyMemory) mem1).of(ff, path, ff.getMapPageSize(), 0);
                } else {
                    mem1 = new ReadOnlyMemory(ff, path, ff.getMapPageSize(), 0);
                    columns.setQuick(primaryIndex, mem1);
                }

                final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), name, plen, tempMem8b);
                final int type = metadata.getColumnType(columnIndex);

                switch (type) {
                    case ColumnType.BINARY:
                    case ColumnType.STRING:
                        TableUtils.iFile(path.trimTo(plen), name);
                        if (mem2 instanceof ReadOnlyMemory) {
                            ((ReadOnlyMemory) mem2).of(ff, path, ff.getMapPageSize(), 0);
                        } else {
                            mem2 = new ReadOnlyMemory(ff, path, ff.getMapPageSize(), 0);
                            columns.setQuick(secondaryIndex, mem2);
                        }
                        growColumn(mem1, mem2, type, partitionRowCount - columnTop);
                        break;
                    default:
                        Misc.free(columns.getAndSetQuick(secondaryIndex, null));
                        growColumn(mem1, null, type, partitionRowCount - columnTop);
                        break;
                }

                columnTops.setQuick(topIndex, columnTop);

                if (metadata.isColumnIndexed(columnIndex)) {
                    if (indexReader instanceof BitmapIndexBackwardReader) {
                        ((BitmapIndexBackwardReader) indexReader).of(configuration, path.trimTo(plen), name, columnTop);
                    }
                } else {
                    Misc.free(indexReaders.getAndSetQuick(topIndex, null));
                }
            } else {
                Misc.free(columns.getAndSetQuick(primaryIndex, NullColumn.INSTANCE));
                Misc.free(columns.getAndSetQuick(secondaryIndex, NullColumn.INSTANCE));
                // the appropriate index for NUllColumn will be created lazily when requested
                // these indexes have state and may not be always required
                Misc.free(indexReaders.getAndSetQuick(topIndex, null));
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private boolean reloadInitialNonPartitioned() {
        if (readTxn()) {
            reloadStruct();
            reloadSymbolMapCounts();
            countDefaultPartitions();
            if (partitionCount > 0) {
                updateCapacities();
                reloadMethod = NON_PARTITIONED_RELOAD_METHOD;
                return true;
            }
        }
        return false;
    }

    private boolean reloadInitialPartitioned() {
        if (readTxn()) {
            reloadStruct();
            reloadSymbolMapCounts();
            partitionMin = findPartitionMinimum(dateFormat);
            partitionCount = calculatePartitionCount();
            if (partitionCount > 0) {
                updateCapacities();
                if (maxTimestamp != Numbers.LONG_NaN) {
                    reloadMethod = PARTITIONED_RELOAD_METHOD;
                }
            }
            return true;
        }
        return false;
    }

    private boolean reloadNonPartitioned() {
        // calling readTxn will set "rowCount" member variable
        if (readTxn()) {
            reloadStruct();
            reloadPartition(0, rowCount);
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
            growColumn(
                    columns.getQuick(getPrimaryColumnIndex(columnBase, i)),
                    columns.getQuick(getSecondaryColumnIndex(columnBase, i)),
                    metadata.getColumnType(i),
                    rowCount - getColumnTop(columnBase, i)
            );

            // reload symbol map
            SymbolMapReader reader = symbolMapReaders.getQuick(i);
            if (reader != null) {
                reader.updateSymbolCount(symbolCountSnapshot.getQuick(symbolMapIndex++));
            }
        }
        partitionRowCounts.setQuick(partitionIndex, rowCount);
    }

    private boolean reloadPartitioned() {
        assert timestampFloorMethod != null;
        long currentPartitionTimestamp = floorToPartitionTimestamp(maxTimestamp);
        boolean b = readTxn();
        if (b) {
            reloadStruct();
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
                        reloadPartition(partitionIndex, readPartitionSize(ff, path.chopZ(), tempMem8b));
                    } finally {
                        path.trimTo(rootLen);
                    }
                } else {
                    reloadPartition(partitionIndex, transientRowCount);
                }
            } else if (delta > 0) {
                // although we have nothing to reload we still have to bump partition count
                incrementPartitionCountBy(delta);
            }
            return true;
        }
        return false;
    }

    private void reloadStruct() {
        if (this.prevStructVersion != this.structVersion) {
            doReloadStruct();
            this.prevStructVersion = this.structVersion;
        }
    }

    private void reloadSymbolMapCounts() {
        int symbolMapIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            if (metadata.getColumnType(i) == ColumnType.SYMBOL) {
                symbolMapReaders.getQuick(i).updateSymbolCount(symbolCountSnapshot.getQuick(symbolMapIndex++));
            }
        }
    }

    private SymbolMapReader reloadSymbolMapReader(int columnIndex, SymbolMapReader reader) {
//        RecordColumnMetadata m = metadata.getColumnQuick(columnIndex);
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
        final long pState = pIndexBase + columnCount * 8;

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            int base = getColumnBase(partitionIndex);
            try {
                Path path = partitionPathGenerator.generate(this, partitionIndex);
                final long partitionRowCount = partitionRowCounts.getQuick(partitionIndex);

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
                            if ((col instanceof ReadOnlyMemory && col.isDeleted()) || col instanceof ForceNullColumn) {
                                reloadColumnAt(path, columns, columnTops, bitmapIndexes, base, i, partitionRowCount);
                            }
                            continue;
                        }

                        if (copyFrom > -1) {
                            fetchColumnsFrom(this.columns, this.columnTops, this.bitmapIndexes, base, copyFrom);
                            copyColumnsTo(this.columns, this.columnTops, this.bitmapIndexes, base, i, partitionRowCount);
                            int copyTo = Unsafe.getUnsafe().getInt(pIndexBase + i * 8 + 4) - 1;
                            while (copyTo > -1 && isEntryToBeProcessed(pState, copyTo)) {
                                copyColumnsTo(this.columns, this.columnTops, this.bitmapIndexes, base, copyTo, partitionRowCount);
                                copyTo = Unsafe.getUnsafe().getInt(pIndexBase + (copyTo - 1) * 8 + 4);
                            }
                            Misc.free(tempCopyStruct.mem1);
                            Misc.free(tempCopyStruct.mem2);
                            Misc.free(tempCopyStruct.bitmapIndexReader);
                        } else {
                            // new instance
                            reloadColumnAt(path, columns, columnTops, bitmapIndexes, base, i, partitionRowCount);
                        }
                    }
                }
                for (int i = columnCount; i < this.columnCount; i++) {
                    Misc.free(columns.getQuick(getPrimaryColumnIndex(base, i)));
                    Misc.free(columns.getQuick(getSecondaryColumnIndex(base, i)));
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private void updateCapacities() {
        int capacity = getColumnBase(partitionCount);
        columns.setPos(capacity);
        bitmapIndexes.setPos(capacity / 2);
        this.partitionRowCounts.seed(partitionCount, -1);
        this.columnTops.setPos(capacity / 2);
    }

    @FunctionalInterface
    private interface IntervalLengthMethod {
        long calculate(long minTimestamp, long maxTimestamp);
    }

    @FunctionalInterface
    private interface TimestampFloorMethod {
        long floor(long timestamp);
    }

    @FunctionalInterface
    private interface ReloadMethod {
        boolean reload(TableReader reader);
    }

    @FunctionalInterface
    private interface PartitionPathGenerator {
        Path generate(TableReader reader, int partitionIndex);
    }

    private static class ForceEmptySymbolMapReader extends EmptySymbolMapReader {
        private static final ForceEmptySymbolMapReader INSTANCE = new ForceEmptySymbolMapReader();
    }

    private static class ForceNullColumn extends NullColumn {
        private static final ForceNullColumn INSTANCE = new ForceNullColumn();
    }

    private static class ColumnCopyStruct {
        ReadOnlyColumn mem1;
        ReadOnlyColumn mem2;
        BitmapIndexReader bitmapIndexReader;
        long top;
    }

}
