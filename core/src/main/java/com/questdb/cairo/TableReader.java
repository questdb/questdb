package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.ex.NumericException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.*;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.std.BinarySequence;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.std.time.Dates;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

public class TableReader implements Closeable, RecordCursor {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
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
            int delta = (int) reader.intervalLengthMethod.calculate(currentPartitionTimestamp, reader.timestampFloorMethod.floor(reader.maxTimestamp));
            int partitionIndex = reader.partitionCount - 1;
            if (delta > 0) {
                reader.incrementPartitionCountBy(delta);
                CompositePath path = reader.partitionPathGenerator.generate(reader, partitionIndex);
                try {
                    reader.reloadPartition(partitionIndex, readPartitionSize(reader.ff, path.chopZ(), reader.tempMem8b));
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
    private static final TimestampFloorMethod INAPPROPRIATE_FLOOR_METHOD = timestamp -> {
        throw CairoException.instance(0).put("Cannot get partition floor for non-partitioned table");
    };
    private final ObjList<ReadOnlyColumn> columns;
    private final FilesFacade ff;
    private final CompositePath path;
    private final int rootLen;
    private final ReadOnlyMemory txMem;
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final TableMetadata metadata;
    private final LongList partitionSizes;
    private final TableRecord record = new TableRecord();
    private final PartitionPathGenerator partitionPathGenerator;
    private final ReloadMethod reloadMethod;
    private final TimestampFloorMethod timestampFloorMethod;
    private final IntervalLengthMethod intervalLengthMethod;
    private final LongList columnTops;
    private int columnCount;
    private int columnCountBits;
    private long transientRowCount;
    private long size;
    private long txn = -1;
    private long maxTimestamp;
    private int partitionCount;
    private long partitionMin;
    private long tempMem8b = Unsafe.malloc(8);
    private int partitionIndex = 0;

    public TableReader(FilesFacade ff, CharSequence root, CharSequence name) {
        this.ff = ff;
        this.path = new CompositePath().of(root).concat(name);
        this.rootLen = path.length();
        failOnPendingTodo();
        this.txMem = openTxnFile();
        this.metadata = openMetaFile();
        this.columnCount = this.metadata.getColumnCount();
        this.columnCountBits = Numbers.msb(Numbers.ceilPow2(this.columnCount) * 2);
        readTxn();

        switch (this.metadata.getPartitionBy()) {
            case PartitionBy.DAY:
                partitionPathGenerator = DAY_GEN;
                reloadMethod = PARTITIONED_RELOAD_METHOD;
                timestampFloorMethod = Dates::floorDD;
                intervalLengthMethod = Dates::getDaysBetween;
                partitionMin = findPartitionMinimum(TableUtils.fmtDay);
                partitionCount = getPartitionCount();
                break;
            case PartitionBy.MONTH:
                partitionPathGenerator = MONTH_GEN;
                reloadMethod = PARTITIONED_RELOAD_METHOD;
                timestampFloorMethod = Dates::floorMM;
                intervalLengthMethod = Dates::getMonthsBetween;
                partitionMin = findPartitionMinimum(TableUtils.fmtMonth);
                partitionCount = getPartitionCount();
                break;
            case PartitionBy.YEAR:
                partitionPathGenerator = YEAR_GEN;
                reloadMethod = PARTITIONED_RELOAD_METHOD;
                timestampFloorMethod = Dates::floorYYYY;
                intervalLengthMethod = Dates::getYearsBetween;
                partitionMin = findPartitionMinimum(TableUtils.fmtYear);
                partitionCount = getPartitionCount();
                break;
            default:
                partitionPathGenerator = DEFAULT_GEN;
                reloadMethod = NON_PARTITIONED_RELOAD_METHOD;
                timestampFloorMethod = INAPPROPRIATE_FLOOR_METHOD;
                intervalLengthMethod = (min, max) -> 0;
                partitionCount = 1;
                break;
        }

        int capacity = getColumnBase(partitionCount);
        this.columns = new ObjList<>(capacity);
        columns.setPos(capacity);
        this.partitionSizes = new LongList(partitionCount);
        this.partitionSizes.seed(partitionCount, -1);
        this.columnTops = new LongList(capacity / 2);
    }

    @Override
    public void close() {
        Misc.free(path);
        Misc.free(metadata);
        Misc.free(txMem);
        for (int i = 0, n = columns.size(); i < n; i++) {
            ReadOnlyColumn mem = columns.getQuick(i);
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

    public boolean reload() {
        return reloadMethod.reload(this);
    }

    public void reloadStruct() {
        long address = metadata.createTransitionIndex();
        try {
            metadata.applyTransitionIndex(address);
            final int columnCount = Unsafe.getUnsafe().getInt(address + 4);
            final long index = address + 8;
            final long stateAddress = index + columnCount * 8;


            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {

                int base = getColumnBase(partitionIndex);

                try {
                    CompositePath path = partitionPathGenerator.generate(this, partitionIndex);

                    Unsafe.getUnsafe().setMemory(stateAddress, columnCount, (byte) 0);

                    for (int i = 0; i < columnCount; i++) {

                        if (Unsafe.getUnsafe().getByte(stateAddress + i) == -1) {
                            continue;
                        }

                        Unsafe.getUnsafe().putByte(stateAddress + i, (byte) -1);

                        final int copyFrom = Unsafe.getUnsafe().getInt(index + i * 8);

                        if (copyFrom == i + 1) {
                            continue;
                        }

                        if (copyFrom > 0) {
                            ReadOnlyColumn mem1;
                            ReadOnlyColumn mem2;

                            mem1 = columns.getAndSetQuick(getPrimaryColumnIndex(base, copyFrom - 1), null);
                            mem2 = columns.getAndSetQuick(getSecondaryColumnIndex(base, copyFrom - 1), null);

                            mem1 = columns.getAndSetQuick(getPrimaryColumnIndex(base, i), mem1);
                            mem2 = columns.getAndSetQuick(getSecondaryColumnIndex(base, i), mem2);

                            int copyTo = Unsafe.getUnsafe().getInt(index + i * 8 + 4);
                            while (copyTo > 0) {
                                if (Unsafe.getUnsafe().getByte(stateAddress + copyTo - 1) == -1) {
                                    break;
                                }
                                Unsafe.getUnsafe().putByte(stateAddress + copyTo - 1, (byte) -1);

                                mem1 = columns.getAndSetQuick(getPrimaryColumnIndex(base, copyTo - 1), mem1);
                                mem2 = columns.getAndSetQuick(getSecondaryColumnIndex(base, copyTo - 1), mem2);
                                copyTo = Unsafe.getUnsafe().getInt(index + (copyTo - 1) * 8 + 4);
                            }
                            Misc.free(mem1);
                            Misc.free(mem2);
                        } else {
                            // new instance
                            createColumnInstanceAt(path, i, base);
                        }
                    }
                } finally {
                    path.trimTo(rootLen);
                }
            }
            this.columnCount = columnCount;
        } finally {
            TableMetadata.freeTransitionIndex(address);
        }
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

    private void createColumnInstanceAt(CompositePath path, int columnIndex, int columnBase) {
        int plen = path.length();
        try {
            String name = metadata.getColumnName(columnIndex);
            if (ff.exists(TableUtils.dFile(path.trimTo(plen), name))) {
                // we defer setting size

                columns.setQuick(getPrimaryColumnIndex(columnBase, columnIndex),
                        new ReadOnlyMemory(ff, path, TableUtils.getMapPageSize(ff)));

                switch (metadata.getColumnQuick(columnIndex).getType()) {
                    case ColumnType.BINARY:
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                        columns.setQuick(getSecondaryColumnIndex(columnBase, columnIndex),
                                new ReadOnlyMemory(ff, TableUtils.iFile(path.trimTo(plen), name), TableUtils.getMapPageSize(ff)));
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

    private int getPartitionCount() {
        if (partitionMin == Long.MAX_VALUE) {
            return 0;
        } else {
            return (int) (intervalLengthMethod.calculate(partitionMin, timestampFloorMethod.floor(maxTimestamp)) + 1);
        }
    }

    private void incrementPartitionCountBy(int delta) {
        partitionSizes.seed(partitionCount, delta, -1);
        partitionCount += delta;
        int capacity = getColumnBase(partitionCount);
        columns.setPos(capacity);
        // we calculate capacity based on two entries per column
        // for tops we only need one entry
        columnTops.setPos(capacity / 2);
    }

    private TableMetadata openMetaFile() {
        try {
            return new TableMetadata(ff, path.concat(TableUtils.META_FILE_NAME).$());
        } finally {
            path.trimTo(rootLen);
        }
    }

    private long openPartition(int partitionIndex, int columnBase, boolean last) {
        try {
            CompositePath path = partitionPathGenerator.generate(this, partitionIndex);
            final long partitionSize;
            if (ff.exists(path)) {
                path.chopZ();

                if (last) {
                    partitionSize = transientRowCount;
                } else {
                    partitionSize = readPartitionSize(ff, path, tempMem8b);
                }

                LOG.info().$("Open partition: ").$(path.$()).$(" [size=").$(partitionSize).$(']').$();

                if (partitionSize > 0) {
                    openPartitionColumns(path, columnBase);
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

    private void openPartitionColumns(CompositePath path, int columnBase) {
        for (int i = 0; i < columnCount; i++) {
            if (columns.getQuick(getPrimaryColumnIndex(columnBase, i)) == null) {
                createColumnInstanceAt(path, i, columnBase);
            }
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
                ReadOnlyColumn mem2 = columns.getQuick(getSecondaryColumnIndex(columnBase, i));
                if (mem2 != null) {
                    mem2.trackFileSize();
                }
            }
            partitionSizes.setQuick(partitionIndex, size);
        }
    }

    private boolean switchPartition() {
        while (partitionIndex < partitionCount) {
            final int columnBase = getColumnBase(partitionIndex);

            long partitionSize = partitionSizes.getQuick(partitionIndex);
            if (partitionSize == -1) {
                partitionSize = openPartition(partitionIndex++, columnBase, partitionIndex == partitionCount);
            } else {
                partitionIndex++;
            }

            if (partitionSize != 0) {
                record.maxRecordIndex = partitionSize - 1;
                record.recordIndex = -1;
                record.columnBase = columnBase;
                return true;
            }
        }
        return false;
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

        private ReadOnlyColumn colA(int col) {
            return columns.getQuick(columnBase + col * 2);
        }

        private ReadOnlyColumn colB(int col) {
            return columns.getQuick(columnBase + col * 2 + 1);
        }
    }
}
