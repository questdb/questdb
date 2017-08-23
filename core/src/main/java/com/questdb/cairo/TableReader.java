package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.ex.NumericException;
import com.questdb.factory.configuration.AbstractRecordMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.*;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.std.BinarySequence;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.std.time.Dates;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

public class TableReader implements Closeable, RecordCursor {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private final static NextRecordFunction nextPartitionFunction = new NextPartitionFunction();
    private final static NextRecordFunction nextPartitionRecordFunction = new NextPartitionRecordFunction();
    private static final PartitionPathGenerator YEAR_GEN = (reader, partitionIndex) -> {
        TableUtils.fmtYear.format(
                Dates.addYear(reader.partitionMin, partitionIndex),
                DateLocaleFactory.INSTANCE.getDefaultDateLocale(),
                null,
                reader.path.put(Path.SEPARATOR)
        );
        return reader.path.$();

    };

    private static final PartitionPathGenerator MONTH_GEN = (reader, partitionIndex) -> {
        TableUtils.fmtMonth.format(
                Dates.addMonths(reader.partitionMin, partitionIndex),
                DateLocaleFactory.INSTANCE.getDefaultDateLocale(),
                null,
                reader.path.put(Path.SEPARATOR)
        );
        return reader.path.$();
    };

    private static final PartitionPathGenerator DAY_GEN = (reader, partitionIndex) -> {
        TableUtils.fmtDay.format(
                Dates.addDays(reader.partitionMin, partitionIndex),
                DateLocaleFactory.INSTANCE.getDefaultDateLocale(),
                null,
                reader.path.put(Path.SEPARATOR)
        );
        return reader.path.$();
    };

    private static final PartitionPathGenerator DEFAULT_GEN = (reader, partitionIndex) -> reader.path.concat(TableUtils.DEFAULT_PARTITION_NAME).$();

    private static final ReloadMethod PARTITIONED_RELOAD_METHOD = reader -> {
        long currentPartitionTimestamp = reader.timestampFloorMethod.floor(reader.maxTimestamp);
        boolean b = reader.readTxn();
        if (b) {
            int delta = getIntervalLength(reader.partitionBy, currentPartitionTimestamp, reader.timestampFloorMethod.floor(reader.maxTimestamp));
            int partitionIndex = reader.partitionCount - 1;
            if (delta > 0) {
                reader.partitionCount += delta;
                reader.partitionSizes.seed(partitionIndex + 1, delta, -1);
                reader.columns.setPos(reader.getColumnCapacity(reader.partitionCount, reader.columnCount));

                CompositePath path = reader.partitionPathGenerator.generate(reader, partitionIndex);
                try {
                    path.trimTo(path.length());
                    reader.reloadPartition(partitionIndex, readPartitionSize(reader.ff, path, reader.tempMem8b));
                } finally {
                    path.trimTo(reader.rootLen);
                }
            } else {
                reader.reloadPartition(partitionIndex, reader.transientRowCount);
            }
            return true;
        }
        return false;
    };

    private static final ReloadMethod NON_PARTITIONED_RELOAD_METHOD = reader -> {
        // calling readTxn will set "size" member variable
        if (reader.readTxn()) {
            reader.reloadPartition(0, reader.size);
            return true;
        }
        return false;
    };

    private static final TimestampFloorMethod INAPROPRIATE_FLOOR_METHOD = timestamp -> {
        throw CairoException.instance(0).put("Cannot get partition floor for non-partitioned table");
    };

    private final ObjList<ReadOnlyMemory> columns;
    private final FilesFacade ff;
    private final CompositePath path;
    private final int rootLen;
    private final ReadOnlyMemory txMem;
    private final ReadOnlyMemory metaMem;
    private final int partitionBy;
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final RecordMetadata metadata;
    private final LongList partitionSizes;
    private final TableRecord record = new TableRecord();
    private final PartitionPathGenerator partitionPathGenerator;
    private final ReloadMethod reloadMethod;
    private final TimestampFloorMethod timestampFloorMethod;
    private final IntervalLengthMethod intervalLengthMethod;
    private int columnCount;
    private int columnCountBits;
    private long transientRowCount;
    private long size;
    private long txn = -1;
    private int timestampIndex;
    private long maxTimestamp;
    private int partitionCount;
    private long partitionMin;
    private long columnTops[];
    private long tempMem8b = Unsafe.malloc(8);
    private int partitionIndex = 0;
    private NextRecordFunction currentRecordFunction = nextPartitionFunction;

    public TableReader(FilesFacade ff, CharSequence root, CharSequence name) {
        this.ff = ff;
        this.path = new CompositePath().of(root).concat(name);
        this.rootLen = path.length();
        failOnPendingTodo();
        this.txMem = openTxnFile();
        this.metaMem = openMetaFile();
        this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        this.columnCountBits = Numbers.msb(Numbers.ceilPow2(this.columnCount) * 2);
        this.partitionBy = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
        this.timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.metadata = new Meta(columnCount);
        readTxn();

        switch (partitionBy) {
            case PartitionBy.DAY:
                partitionPathGenerator = DAY_GEN;
                reloadMethod = PARTITIONED_RELOAD_METHOD;
                timestampFloorMethod = Dates::floorDD;
                intervalLengthMethod = Dates::getDaysBetween;
                partitionMin = findPartitionMinimum(TableUtils.fmtDay);
                break;
            case PartitionBy.MONTH:
                partitionPathGenerator = MONTH_GEN;
                reloadMethod = PARTITIONED_RELOAD_METHOD;
                timestampFloorMethod = Dates::floorMM;
                intervalLengthMethod = Dates::getMonthsBetween;
                partitionMin = findPartitionMinimum(TableUtils.fmtMonth);
                break;
            case PartitionBy.YEAR:
                partitionPathGenerator = YEAR_GEN;
                reloadMethod = PARTITIONED_RELOAD_METHOD;
                timestampFloorMethod = Dates::floorYYYY;
                intervalLengthMethod = Dates::getYearsBetween;
                partitionMin = findPartitionMinimum(TableUtils.fmtYear);
                break;
            default:
                partitionPathGenerator = DEFAULT_GEN;
                reloadMethod = NON_PARTITIONED_RELOAD_METHOD;
                timestampFloorMethod = INAPROPRIATE_FLOOR_METHOD;
                intervalLengthMethod = (min, max) -> 0;
                partitionMin = Long.MAX_VALUE;
                break;
        }

        partitionCount = getPartitionCount();
        int columnListCapacity = getColumnCapacity(partitionCount, columnCount);
        this.columns = new ObjList<>(columnListCapacity);
        columns.setPos(columnListCapacity);
        this.partitionSizes = new LongList(partitionCount);
        this.partitionSizes.seed(partitionCount, -1);
        this.columnTops = new long[columnCount];
    }

    @Override
    public void close() {
        Misc.free(path);
        Misc.free(metaMem);
        Misc.free(txMem);
        for (int i = 0, n = columns.size(); i < n; i++) {
            VirtualMemory mem = columns.getQuick(i);
            if (mem != null) {
                mem.close();
            }
        }
        if (tempMem8b != 0) {
            Unsafe.free(tempMem8b, 8);
            tempMem8b = 0;
        }
    }

    public RecordMetadata getMetadata() {
        return metadata;
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
        currentRecordFunction = nextPartitionFunction;
    }

    @Override
    public boolean hasNext() {
        return partitionIndex < partitionCount;
    }

    @Override
    public Record next() {
        return currentRecordFunction.next(this);
    }

    public boolean reload() {
        return reloadMethod.reload(this);
    }

    public long size() {
        return size;
    }

    private static int getPrimaryColumnIndex(int base, int index) {
        return base + index * 2;
    }

    private static int getSecondaryColumnIndex(int base, int index) {
        return getPrimaryColumnIndex(base, index) + 1;
    }

    private static long readPartitionSize(FilesFacade ff, CompositePath path, long tempMem) {
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

    private static int getIntervalLength(int partitionBy, long min, long max) {
        switch (partitionBy) {
            case PartitionBy.YEAR:
                return (int) Dates.getYearsBetween(min, max);
            case PartitionBy.MONTH:
                return (int) Dates.getMonthsBetween(min, max);
            case PartitionBy.DAY:
                return (int) Dates.getDaysBetween(min, max);
            default:
                return 0;
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
                    } while (ff.findNext(p));
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

    private int getColumnCapacity(int partitionCount, int columnCount) {
        return partitionCount == 1 ? columnCount * 2 : getColumnBase(partitionCount);
    }

    private int getPartitionCount() {
        if (partitionMin == Long.MAX_VALUE) {
            return 1;
        } else {
            return (int) (intervalLengthMethod.calculate(partitionMin, timestampFloorMethod.floor(maxTimestamp)) + 1);
        }
    }

    private ReadOnlyMemory openMetaFile() {
        try {
            return new ReadOnlyMemory(ff, path.concat(TableUtils.META_FILE_NAME).$(), ff.getPageSize());
        } finally {
            path.trimTo(rootLen);
        }
    }

    private long openPartition(int partitionIndex, int columnBase, long columnTops[], boolean last) {
        try {
            CompositePath path = partitionPathGenerator.generate(this, partitionIndex);
            final long partitionSize;
            if (ff.exists(path)) {
                path.trimTo(path.length());

                if (last) {
                    partitionSize = transientRowCount;
                } else {
                    partitionSize = readPartitionSize(ff, path, tempMem8b);
                }

                LOG.info().$("Open partition: ").$(path.$()).$(" [size=").$(partitionSize).$(']').$();

                if (partitionSize > 0) {
                    openPartitionColumns(columnBase, columnTops);
                }
            } else {
                partitionSize = 0;
            }
            partitionSizes.setQuick(partitionIndex, partitionSize);
            return partitionSize;
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartitionColumns(int columnBase, long[] columnTops) {
        int plen = path.length();
        try {
            for (int i = 0; i < columnCount; i++) {
                if (columns.getQuick(getPrimaryColumnIndex(columnBase, i)) == null) {
                    String name = metadata.getColumnName(i);
                    if (ff.exists(TableUtils.dFile(path.trimTo(plen), name))) {
                        // we defer setting size
                        final ReadOnlyMemory mem1 = new ReadOnlyMemory(ff, path, TableUtils.getMapPageSize(ff));

                        columns.setQuick(getPrimaryColumnIndex(columnBase, i), mem1);

                        switch (metadata.getColumnQuick(i).getType()) {
                            case ColumnType.BINARY:
                            case ColumnType.STRING:
                            case ColumnType.SYMBOL:
                                columns.setQuick(getSecondaryColumnIndex(columnBase, i),
                                        new ReadOnlyMemory(ff, TableUtils.iFile(path.trimTo(plen), name), TableUtils.getMapPageSize(ff)));
                                break;
                            default:
                                break;
                        }
                        columnTops[i] = TableUtils.readColumnTop(ff, path, name, plen);
                    }
                }
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private ReadOnlyMemory openTxnFile() {
        try {
            if (ff.exists(path.concat(TableUtils.TXN_FILE_NAME).$())) {
                return new ReadOnlyMemory(ff, path, ff.getPageSize());
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);

        } finally {
            path.trimTo(rootLen);
        }
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
            this.maxTimestamp = txMem.getLong(TableUtils.TX_OFFSET_MAX_TIMESTAMP);
            Unsafe.getUnsafe().loadFence();
            if (txn == txMem.getLong(TableUtils.TX_OFFSET_TXN)) {
                this.txn = txn;
                this.transientRowCount = transientRowCount;
                this.size = fixedRowCount + transientRowCount;
                break;
            }
            LockSupport.parkNanos(1);
        }
        return true;
    }

    private void reloadPartition(int partitionIndex, long size) {
        if (partitionSizes.getQuick(partitionIndex) > -1) {
            int columnBase = getColumnBase(partitionIndex);
            for (int i = 0; i < columnCount; i++) {
                columns.getQuick(getPrimaryColumnIndex(columnBase, i)).trackFileSize();
                ReadOnlyMemory mem2 = columns.getQuick(getSecondaryColumnIndex(columnBase, i));
                if (mem2 != null) {
                    mem2.trackFileSize();
                }
            }
            partitionSizes.setQuick(partitionIndex, size);
        }
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
        CompositePath generate(TableReader reader, int partitionIndex);
    }

    @FunctionalInterface
    private interface NextRecordFunction {
        Record next(TableReader reader);
    }

    private static class ColumnMeta implements RecordColumnMetadata {
        private final int type;
        private String name;

        public ColumnMeta(String name, int type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public int getBucketCount() {
            return 0;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public SymbolTable getSymbolTable() {
            return null;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public boolean isIndexed() {
            return false;
        }
    }

    private static class NextPartitionFunction implements NextRecordFunction {
        @Override
        public Record next(TableReader reader) {
            long partitionSize = reader.partitionSizes.getQuick(reader.partitionIndex);
            int columnBase = reader.getColumnBase(reader.partitionIndex);

            TableReader.TableRecord rec = reader.record;
            if (partitionSize == -1) {
                rec.maxRecordIndex = reader.openPartition(reader.partitionIndex, columnBase, reader.columnTops, reader.partitionIndex == reader.partitionCount - 1) - 1;
            } else {
                rec.maxRecordIndex = partitionSize - 1;
            }

            rec.recordIndex = -1;
            rec.columnBase = columnBase;
            reader.currentRecordFunction = nextPartitionRecordFunction;
            return reader.currentRecordFunction.next(reader);
        }
    }

    private static class NextPartitionRecordFunction implements NextRecordFunction {
        @Override
        public Record next(TableReader reader) {
            TableReader.TableRecord rec = reader.record;
            if (++rec.recordIndex < rec.maxRecordIndex) {
                return rec;
            } else {
                reader.currentRecordFunction = nextPartitionFunction;
                reader.partitionIndex++;
                return rec;
            }
        }
    }

    private class TableRecord implements Record {
        private int columnBase;
        private long recordIndex = 0;
        private long maxRecordIndex = -1;

        @Override
        public byte get(int col) {
            return colA(col).getByte(recordIndex);
        }

        @Override
        public BinarySequence getBin2(int col) {
            return colA(col).getBin(colB(col).getLong(recordIndex * 8));
        }

        @Override
        public long getBinLen(int col) {
            return colA(col).getLong(colB(col).getLong(recordIndex * 8));
        }

        @Override
        public boolean getBool(int col) {
            return colA(col).getBool(recordIndex);
        }

        @Override
        public long getDate(int col) {
            return colA(col).getLong(recordIndex * 8);
        }

        @Override
        public double getDouble(int col) {
            return colA(col).getDouble(recordIndex * 8);
        }

        @Override
        public float getFloat(int col) {
            return colA(col).getFloat(recordIndex * 4);
        }

        @Override
        public CharSequence getFlyweightStr(int col) {
            return colA(col).getStr(colB(col).getLong(recordIndex * 8));
        }

        @Override
        public CharSequence getFlyweightStrB(int col) {
            return colA(col).getStr2(colB(col).getLong(recordIndex * 8));
        }

        @Override
        public int getInt(int col) {
            return colA(col).getInt(recordIndex * 4);
        }

        @Override
        public long getLong(int col) {
            return colA(col).getLong(recordIndex * 8);
        }

        @Override
        public long getRowId() {
            return Rows.toRowID(columnBase >>> columnCountBits, recordIndex);
        }

        @Override
        public short getShort(int col) {
            return colA(col).getShort(recordIndex * 2);
        }

        @Override
        public int getStrLen(int col) {
            return colA(col).getInt(colB(col).getLong(recordIndex * 8));
        }

        @Override
        public CharSequence getSym(int col) {
            return null;
        }

        private ReadOnlyMemory colA(int col) {
            return columns.getQuick(columnBase + col * 2);
        }

        private ReadOnlyMemory colB(int col) {
            return columns.getQuick(columnBase + col * 2 + 1);
        }
    }

    private class Meta extends AbstractRecordMetadata {
        private final ObjList<ColumnMeta> columnMetadata;
        private final CharSequenceIntHashMap columnNameHashTable;

        public Meta(int columnCount) {
            this.columnMetadata = new ObjList<>(columnCount);
            this.columnNameHashTable = new CharSequenceIntHashMap(columnCount);

            long offset = TableUtils.getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                int type = metaMem.getInt(TableUtils.META_OFFSET_COLUMN_TYPES + i * 4);
                CharSequence name = metaMem.getStr(offset);
                assert name != null;
                String s = name.toString();
                columnMetadata.add(new ColumnMeta(s, type));
                columnNameHashTable.put(s, i);
                offset += ReadOnlyMemory.getStorageLength(name);
            }
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public int getColumnIndexQuiet(CharSequence name) {
            return columnNameHashTable.get(name);
        }

        @Override
        public RecordColumnMetadata getColumnQuick(int index) {
            return columnMetadata.getQuick(index);
        }

        @Override
        public int getTimestampIndex() {
            return timestampIndex;
        }
    }
}
