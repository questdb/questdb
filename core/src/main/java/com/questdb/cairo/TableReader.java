/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.common.*;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.microtime.DateFormat;
import com.questdb.std.microtime.DateLocaleFactory;
import com.questdb.std.microtime.Dates;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

public class TableReader implements Closeable, RecordCursor {
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
    private final TableRecord record = new TableRecord();
    private final PartitionPathGenerator partitionPathGenerator;
    private final DateFormat dateFormat;
    private final TimestampFloorMethod timestampFloorMethod;
    private final IntervalLengthMethod intervalLengthMethod;
    private final CharSequence name;
    private final ObjList<SymbolMapReaderImpl> symbolMapReaders = new ObjList<>();
    private final CairoConfiguration configuration;
    private final IntList symbolCountSnapshot = new IntList();
    private LongList columnTops;
    private ObjList<ReadOnlyColumn> columns;
    private int columnCount;
    private int columnCountBits;
    private long transientRowCount;
    private long structVersion;
    private long prevStructVersion;
    private long rowCount;
    private long txn = -1;
    private long maxTimestamp;
    private int partitionCount;
    private long partitionMin;
    private ReloadMethod reloadMethod;
    private long tempMem8b = Unsafe.malloc(8);
    private int partitionIndex = 0;

    public TableReader(CairoConfiguration configuration, CharSequence name) {
        LOG.info().$("open '").utf8(name).$('\'').$();
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.name = Chars.stringOf(name);
        this.path = new Path().of(configuration.getRoot()).concat(name);
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
            columns.setPos(capacity);
            this.partitionRowCounts = new LongList(partitionCount);
            this.partitionRowCounts.seed(partitionCount, -1);
            this.columnTops = new LongList(capacity / 2);
            this.columnTops.setPos(capacity / 2);
        } catch (AssertionError e) {
            close();
            throw e;
        }
    }

    public void applyTransitionIndexToSymbolMapReaders(long pTransitionIndex) {
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

            // don't copy entries to themselves
            if (copyFrom == i + 1) {
                continue;
            }

            // check where we source entry:
            // 1. from another entry
            // 2. create new instance
            SymbolMapReaderImpl tmp;
            if (copyFrom > 0) {
                tmp = symbolMapReaders.getAndSetQuick(copyFrom - 1, null);
                tmp = symbolMapReaders.getAndSetQuick(i, tmp);

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

                    tmp = symbolMapReaders.getAndSetQuick(copyTo - 1, tmp);
                    copyTo = Unsafe.getUnsafe().getInt(index + (copyTo - 1) * 8 + 4);
                }
                Misc.free(tmp);
            } else {
                // new instance
                RecordColumnMetadata m = metadata.getColumnQuick(i);
                if (m.getType() == ColumnType.SYMBOL) {
                    SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, m.getName(), 0);
                    tmp = symbolMapReaders.getAndSetQuick(i, reader);
                    Misc.free(tmp);
                } else {
                    Misc.free(symbolMapReaders.getAndSetQuick(i, null));
                }
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

    @Override
    public void close() {
        if (isOpen()) {
            for (int i = 0, n = symbolMapReaders.size(); i < n; i++) {
                Misc.free(symbolMapReaders.getQuick(i));
            }
            symbolMapReaders.clear();

            Misc.free(path);
            Misc.free(metadata);
            Misc.free(txMem);
            if (columns != null) {
                for (int i = 0, n = columns.size(); i < n; i++) {
                    ReadOnlyColumn mem = columns.getQuick(i);
                    if (mem != null) {
                        mem.close();
                    }
                }
            }
            if (tempMem8b != 0) {
                Unsafe.free(tempMem8b, 8);
                tempMem8b = 0;
            }

            LOG.info().$("closed '").utf8(name).$('\'').$();
        }
    }

    /**
     * Closes column files. This method should be used before call to TableWriter.removeColumn() on
     * Windows OS.
     *
     * @param columnName name of column to be closed.
     * @throws NoSuchColumnException when column is not found.
     */
    public void closeColumnForRemove(CharSequence columnName) {
        closeColumnForRemove(metadata.getColumnIndex(columnName));
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
            Misc.free(columns.getAndSetQuick(getPrimaryColumnIndex(base, columnIndex), NullColumn.INSTANCE));
            Misc.free(columns.getAndSetQuick(getSecondaryColumnIndex(base, columnIndex), NullColumn.INSTANCE));
        }
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public CharSequence getName() {
        return name;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new TableRecord();
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public Record recordAt(long rowId) {
        record.columnBase = getColumnBase(Rows.toPartitionIndex(rowId));
        record.recordIndex = Rows.toLocalRowID(rowId);
        return record;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        TableRecord rec = (TableRecord) record;
        rec.columnBase = getColumnBase(Rows.toPartitionIndex(rowId));
        rec.recordIndex = Rows.toLocalRowID(rowId);
    }

    @Override
    public void releaseCursor() {
        // nothing to do
    }

    @Override
    public void toTop() {
        partitionIndex = 0;
        record.recordIndex = record.maxRecordIndex = -1;
    }

    @Override
    public boolean hasNext() {
        return record.recordIndex < record.maxRecordIndex || switchPartition();
    }

    @Override
    public Record next() {
        record.recordIndex++;
        return record;
    }

    public boolean isOpen() {
        return tempMem8b != 0;
    }

    public boolean reload() {
        return reloadMethod.reload(this);
    }

    public long size() {
        return rowCount;
    }

    private static int getColumnBits(int columnCount) {
        return Numbers.msb(Numbers.ceilPow2(columnCount) * 2);
    }

    private static int getPrimaryColumnIndex(int base, int index) {
        return base + index * 2;
    }

    private static int getSecondaryColumnIndex(int base, int index) {
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

    private int calculatePartitionCount() {
        if (partitionMin == Long.MAX_VALUE) {
            return 0;
        } else {
            return maxTimestamp == Numbers.LONG_NaN ? 1 : (int) (intervalLengthMethod.calculate(partitionMin, timestampFloorMethod.floor(maxTimestamp)) + 1);
        }
    }

    private void copyColumnsTo(ObjList<ReadOnlyColumn> columns, LongList columnTops, int base, int index) {
        if (tempCopyStruct.mem1 != null && !ff.exists(tempCopyStruct.mem1.getFd())) {
            Misc.free(tempCopyStruct.mem1);
            Misc.free(tempCopyStruct.mem2);
            fetchColumnsFrom(columns, columnTops, base, index);
            createColumnInstanceAt(path, columns, columnTops, index, base);
        } else {
            tempCopyStruct.mem1 = columns.getAndSetQuick(getPrimaryColumnIndex(base, index), tempCopyStruct.mem1);
            tempCopyStruct.mem2 = columns.getAndSetQuick(getSecondaryColumnIndex(base, index), tempCopyStruct.mem2);
            tempCopyStruct.top = columnTops.getAndSetQuick(base / 2 + index, tempCopyStruct.top);
        }
    }

    private void countDefaultPartitions() {
        Path path = pathGenDefault();
        partitionCount = ff.exists(path) ? 1 : 0;
        path.trimTo(rootLen);
    }

    private void createColumnInstanceAt(Path path, ObjList<ReadOnlyColumn> columns, LongList columnTops, int columnIndex, int columnBase) {
        int plen = path.length();
        try {
            String name = metadata.getColumnName(columnIndex);
            if (ff.exists(TableUtils.dFile(path.trimTo(plen), name))) {
                // we defer setting rowCount

                columns.setQuick(getPrimaryColumnIndex(columnBase, columnIndex),
                        new ReadOnlyMemory(ff, path, ff.getMapPageSize()));

                switch (metadata.getColumnQuick(columnIndex).getType()) {
                    case ColumnType.BINARY:
                    case ColumnType.STRING:
                        columns.setQuick(getSecondaryColumnIndex(columnBase, columnIndex),
                                new ReadOnlyMemory(ff, TableUtils.iFile(path.trimTo(plen), name), ff.getMapPageSize()));
                        break;
                    default:
                        break;
                }
                columnTops.setQuick(columnBase / 2 + columnIndex, TableUtils.readColumnTop(ff, path.trimTo(plen), name, plen, tempMem8b));
            } else {
                columns.setQuick(getPrimaryColumnIndex(columnBase, columnIndex), NullColumn.INSTANCE);
                columns.setQuick(getSecondaryColumnIndex(columnBase, columnIndex), NullColumn.INSTANCE);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private void createNewColumnList(int columnCount, long pTransitionIndex, int columnBits) {
        int capacity = partitionCount << columnBits;
        ObjList<ReadOnlyColumn> columns = new ObjList<>(capacity);
        LongList columnTops = new LongList();
        columns.setPos(capacity);
        columnTops.setPos(capacity / 2);
        final long pIndexBase = pTransitionIndex + 8;

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final int base = partitionIndex << columnBits;
            final int oldBase = partitionIndex << columnCountBits;

            try {
                Path path = partitionPathGenerator.generate(this, partitionIndex);
                for (int i = 0; i < columnCount; i++) {
                    final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8) - 1;
                    if (copyFrom > -1) {
                        fetchColumnsFrom(this.columns, this.columnTops, oldBase, copyFrom);
                        copyColumnsTo(columns, columnTops, base, i);
                    } else {
                        // new instance
                        createColumnInstanceAt(path, columns, columnTops, i, base);
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
                reshuffleExistingColumnList(columnCount, pTransitionIndex);
            }
            // rearrange symbol map reader list
            applyTransitionIndexToSymbolMapReaders(pTransitionIndex);
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

    private void fetchColumnsFrom(ObjList<ReadOnlyColumn> columns, LongList columnTops, int base, int index) {
        tempCopyStruct.mem1 = columns.getAndSetQuick(getPrimaryColumnIndex(base, index), null);
        tempCopyStruct.mem2 = columns.getAndSetQuick(getSecondaryColumnIndex(base, index), null);
        tempCopyStruct.top = columnTops.getQuick(base / 2 + index);
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
                                if (time < partitionMin) {
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

    private int getColumnBase(int partitionIndex) {
        return partitionIndex << columnCountBits;
    }

    private void incrementPartitionCountBy(int delta) {
        partitionRowCounts.seed(partitionCount, delta, -1);
        partitionCount += delta;
        int capacity = getColumnBase(partitionCount);
        columns.setPos(capacity);
        // we calculate capacity based on two entries per column
        // for tops we only need one entry
        columnTops.setPos(capacity / 2);
    }

    private TableReaderMetadata openMetaFile() {
        try {
            return new TableReaderMetadata(ff, path.concat(TableUtils.META_FILE_NAME).$());
        } finally {
            path.trimTo(rootLen);
        }
    }

    private long openPartition(int partitionIndex, int columnBase, boolean last) {
        try {
            Path path = partitionPathGenerator.generate(this, partitionIndex);
            final long partitionSize;
            if (ff.exists(path)) {
                path.chopZ();

                if (last) {
                    partitionSize = transientRowCount;
                } else {
                    partitionSize = readPartitionSize(ff, path, tempMem8b);
                }

                LOG.info().$("open partition ").$(path.$()).$(" [rowCount=").$(partitionSize).$(']').$();

                if (partitionSize > -1) {
                    openPartitionColumns(path, columnBase);
                    partitionRowCounts.setQuick(partitionIndex, partitionSize);
                }
            } else {
                partitionSize = -1;
            }
            return partitionSize;
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartitionColumns(Path path, int columnBase) {
        for (int i = 0; i < columnCount; i++) {
            if (columns.getQuick(getPrimaryColumnIndex(columnBase, i)) == null) {
                createColumnInstanceAt(path, this.columns, this.columnTops, i, columnBase);
            }
        }
    }

    private void openSymbolMaps() {
        int symbolColumnIndex = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            RecordColumnMetadata m = metadata.getColumnQuick(i);
            if (m.getType() == ColumnType.SYMBOL) {
                SymbolMapReaderImpl symbolMapReader = new SymbolMapReaderImpl(configuration, path, m.getName(), symbolCountSnapshot.getQuick(symbolColumnIndex++));
                symbolMapReaders.extendAndSet(i, symbolMapReader);
            }
        }
    }

    private ReadOnlyMemory openTxnFile() {
        try {
            return new ReadOnlyMemory(ff, path.concat(TableUtils.TXN_FILE_NAME).$(), ff.getPageSize());
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
        while (true) {
            long txn = txMem.getLong(TableUtils.TX_OFFSET_TXN);

            if (txn == this.txn) {
                return false;
            }

            Unsafe.getUnsafe().loadFence();
            long transientRowCount = txMem.getLong(TableUtils.TX_OFFSET_TRANSIENT_ROW_COUNT);
            long fixedRowCount = txMem.getLong(TableUtils.TX_OFFSET_FIXED_ROW_COUNT);
            long maxTimestamp = txMem.getLong(TableUtils.TX_OFFSET_MAX_TIMESTAMP);
            long structVersion = txMem.getLong(TableUtils.TX_OFFSET_STRUCT_VERSION);

            this.symbolCountSnapshot.clear();
            int symbolMapCount = txMem.getInt(TableUtils.TX_OFFSET_MAP_WRITER_COUNT);
            for (int i = 0; i < symbolMapCount; i++) {
                symbolCountSnapshot.add(txMem.getInt(TableUtils.TX_OFFSET_MAP_WRITER_COUNT + 4 + i * 4));
            }

            Unsafe.getUnsafe().loadFence();
            if (txn == txMem.getLong(TableUtils.TX_OFFSET_TXN_CHECK)) {
                this.txn = txn;
                this.transientRowCount = transientRowCount;
                this.rowCount = fixedRowCount + transientRowCount;
                this.maxTimestamp = maxTimestamp;
                this.structVersion = structVersion;
                break;
            }
            LockSupport.parkNanos(1);
        }
        return true;
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
        if (partitionRowCounts.getQuick(partitionIndex) > -1) {
            int columnBase = getColumnBase(partitionIndex);
            for (int i = 0; i < columnCount; i++) {
                ReadOnlyColumn col = columns.getQuick(getPrimaryColumnIndex(columnBase, i));
                assert col != null : "oops, base:" + columnBase + ", i:" + i + ", partition:" + partitionIndex + ", rowCount:" + rowCount + ", partitionSize:" + partitionRowCounts.getQuick(partitionIndex);
                col.trackFileSize();
                ReadOnlyColumn mem2 = columns.getQuick(getSecondaryColumnIndex(columnBase, i));
                if (mem2 != null) {
                    mem2.trackFileSize();
                }

                // reload symbol map
                SymbolMapReaderImpl reader = symbolMapReaders.getQuick(i);
                if (reader != null) {
                    reader.updateSymbolCount(symbolCountSnapshot.getQuick(symbolMapIndex++));
                }
            }
            partitionRowCounts.setQuick(partitionIndex, rowCount);
        } else {
            reloadSymbolMapCounts();
        }
    }

    private boolean reloadPartitioned() {
        assert timestampFloorMethod != null;
        long currentPartitionTimestamp = timestampFloorMethod.floor(maxTimestamp);
        boolean b = readTxn();
        if (b) {
            reloadStruct();
            assert intervalLengthMethod != null;
            int delta = (int) intervalLengthMethod.calculate(currentPartitionTimestamp, timestampFloorMethod.floor(maxTimestamp));
            int partitionIndex = partitionCount - 1;
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
            if (metadata.getColumnQuick(i).getType() == ColumnType.SYMBOL) {
                symbolMapReaders.getQuick(i).updateSymbolCount(symbolCountSnapshot.getQuick(symbolMapIndex++));
            }
        }
    }

    private void reshuffleExistingColumnList(int columnCount, long pTransitionIndex) {

        final long pIndexBase = pTransitionIndex + 8;
        final long pState = pIndexBase + columnCount * 8;

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            int base = getColumnBase(partitionIndex);
            try {
                Path path = partitionPathGenerator.generate(this, partitionIndex);

                Unsafe.getUnsafe().setMemory(pState, columnCount, (byte) 0);

                for (int i = 0; i < columnCount; i++) {

                    if (isEntryToBeProcessed(pState, i)) {
                        final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8) - 1;

                        if (copyFrom == i) {
                            continue;
                        }

                        if (copyFrom > -1) {
                            fetchColumnsFrom(this.columns, this.columnTops, base, copyFrom);
                            copyColumnsTo(this.columns, this.columnTops, base, i);
                            int copyTo = Unsafe.getUnsafe().getInt(pIndexBase + i * 8 + 4) - 1;
                            while (copyTo > -1 && isEntryToBeProcessed(pState, copyTo)) {
                                copyColumnsTo(this.columns, this.columnTops, base, copyTo);
                                copyTo = Unsafe.getUnsafe().getInt(pIndexBase + (copyTo - 1) * 8 + 4);
                            }
                            Misc.free(tempCopyStruct.mem1);
                            Misc.free(tempCopyStruct.mem2);
                        } else {
                            // new instance
                            createColumnInstanceAt(path, this.columns, this.columnTops, i, base);
                        }
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private boolean switchPartition() {
        while (partitionIndex < partitionCount) {
            final int columnBase = getColumnBase(partitionIndex);

            assert partitionRowCounts.size() > 0 : "index: " + partitionIndex + ", count: " + partitionCount;
            long partitionSize = partitionRowCounts.getQuick(partitionIndex);
            if (partitionSize == -1) {
                partitionSize = openPartition(partitionIndex++, columnBase, partitionIndex == partitionCount);
            } else {
                partitionIndex++;
            }

            if (partitionSize > 0) {
                record.maxRecordIndex = partitionSize - 1;
                record.recordIndex = -1;
                record.columnBase = columnBase;
                return true;
            }
        }
        return false;
    }

    private void updateCapacities() {
        int capacity = getColumnBase(partitionCount);
        columns.setPos(capacity);
        this.partitionRowCounts.seed(partitionCount, -1);
        this.columnTops = new LongList(capacity / 2);
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

    private static class ColumnCopyStruct {
        ReadOnlyColumn mem1;
        ReadOnlyColumn mem2;
        long top;
    }

    private class TableRecord implements Record {

        private int columnBase;
        private long recordIndex = 0;
        private long maxRecordIndex = -1;

        @Override
        public byte get(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return 0;
            }
            return colA(col).getByte(index);
        }

        @Override
        public BinarySequence getBin2(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return null;
            }
            return colA(col).getBin(colB(col).getLong(index * 8));
        }

        @Override
        public long getBinLen(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return -1;
            }
            return colA(col).getBinLen(colB(col).getLong(index * 8));
        }

        @Override
        public boolean getBool(int col) {
            long index = getIndex(col);
            return index >= 0 && colA(col).getBool(index);
        }

        @Override
        public long getDate(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return Numbers.LONG_NaN;
            }
            return colA(col).getLong(index * 8);
        }

        @Override
        public double getDouble(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return Double.NaN;
            }
            return colA(col).getDouble(index * 8);
        }

        @Override
        public float getFloat(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return Float.NaN;
            }
            return colA(col).getFloat(index * 4);
        }

        @Override
        public CharSequence getFlyweightStr(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return null;
            }
            return colA(col).getStr(colB(col).getLong(index * 8));
        }

        @Override
        public CharSequence getFlyweightStrB(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return null;
            }
            return colA(col).getStr2(colB(col).getLong(index * 8));
        }

        @Override
        public int getInt(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return Numbers.INT_NaN;
            }
            return colA(col).getInt(index * 4);
        }

        @Override
        public long getLong(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return Numbers.LONG_NaN;
            }
            return colA(col).getLong(index * 8);
        }

        @Override
        public long getRowId() {
            return Rows.toRowID(columnBase >>> columnCountBits, recordIndex);
        }

        @Override
        public short getShort(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return 0;
            }
            return colA(col).getShort(index * 2);
        }

        @Override
        public int getStrLen(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return -1;
            }
            return colA(col).getStrLen(colB(col).getLong(index * 8));
        }

        @Override
        public CharSequence getSym(int col) {
            long index = getIndex(col);
            if (index < 0) {
                return null;
            }
            return symbolMapReaders.getQuick(col).value(colA(col).getInt(index * 4));
        }

        private ReadOnlyColumn colA(int col) {
            return columns.getQuick(columnBase + col * 2);
        }

        private ReadOnlyColumn colB(int col) {
            return columns.getQuick(columnBase + col * 2 + 1);
        }

        private long getIndex(int col) {
            assert col > -1 && col < columnCount : "Column index out of bounds: " + col + " >= " + columnCount;
            long top = columnTops.getQuick(columnBase / 2 + col);
            return top > 0L ? recordIndex - top : recordIndex;
        }
    }
}
