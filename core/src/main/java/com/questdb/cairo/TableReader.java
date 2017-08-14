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

import static com.questdb.cairo.TableWriter.*;

public class TableReader implements Closeable, RecordCursor {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private final static NextRecordFunction nextPartitionFunction = new NextPartitionFunction();
    private final static NextRecordFunction nextPartitionRecordFunction = new NextPartitionRecordFunction();
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
    private final DateFormat partitionDirFmt;
    private final TableRecord record = new TableRecord();
    private int columnCount;
    private int columnCountBits;
    private long transientRowCount;
    private long size;
    private long txn = -1;
    private int timestampIndex;
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
        this.metaMem = new ReadOnlyMemory(ff);
        openMetaFile();
        this.columnCount = metaMem.getInt(META_OFFSET_COUNT);
        this.columnCountBits = Numbers.msb(Numbers.ceilPow2(this.columnCount) * 2);
        this.partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
        this.timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
        this.metadata = new Meta(columnCount);
        this.partitionDirFmt = TableWriter.selectPartitionDirFmt(partitionBy);
        countPartitions();
        int columnListCapacity = (this.partitionCount == 1 ? columnCount * 2 : partitionCount << columnCountBits);
        this.columns = new ObjList<>(columnListCapacity);
        columns.extendAndSet(columnListCapacity - 1, null);
        this.partitionSizes = new LongList(partitionCount);
        this.partitionSizes.seed(partitionCount, -1);
        readTxn();
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
        record.columnBase = Rows.toPartitionIndex(rowId) << columnCountBits;
        record.recordIndex = Rows.toLocalRowID(rowId);
        return record;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        TableRecord rec = (TableRecord) record;
        rec.columnBase = Rows.toPartitionIndex(rowId) << columnCountBits;
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

    public boolean refresh() {
        return readTxn();
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

    private static long readPartitionSize(FilesFacade ff, CompositePath path) {
        int plen = path.length();
        try {
            if (ff.exists(path.concat(TableWriter.ARCHIVE_FILE_NAME).$())) {
                long fd = ff.openRO(path);
                if (fd == -1) {
                    throw CairoException.instance(Os.errno()).put("Cannot open: ").put(path);
                }

                try {
                    long mem = Unsafe.malloc(8);
                    try {
                        if (ff.read(fd, mem, 8, 0) != 8) {
                            throw CairoException.instance(Os.errno()).put("Cannot read: ").put(path);
                        }
                        return Unsafe.getUnsafe().getLong(mem);
                    } finally {
                        Unsafe.free(mem, 8);
                    }
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

    private void countPartitions() {
        if (partitionDirFmt == null) {
            partitionCount = 1;
            return;
        }

        partitionMin = Long.MAX_VALUE;
        long partitionMax = Long.MIN_VALUE;

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

                                if (time > partitionMax) {
                                    partitionMax = time;
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

        if (partitionMin == Long.MAX_VALUE) {
            partitionCount = 1;
            return;
        }

        switch (partitionBy) {
            case PartitionBy.YEAR:
                partitionCount = (int) Dates.getYearsBetween(partitionMin, partitionMax) + 1;
                break;
            case PartitionBy.MONTH:
                partitionCount = (int) Dates.getMonthsBetween(partitionMin, partitionMax) + 1;
                break;
            case PartitionBy.DAY:
                partitionCount = (int) Dates.getDaysBetween(partitionMin, partitionMax) + 1;
                break;
            default:
                break;
        }
    }

    private void failOnPendingTodo() {
        try {
            if (ff.exists(path.concat(TODO_FILE_NAME).$())) {
                throw CairoException.instance(0).put("Table ").put(path.$()).put(" is pending recovery.");
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private long loadDefaultPartition(int columnIndex, long[] columnOffsets) {
        try {
            path.concat(TableWriter.DEFAULT_PARTITION_NAME).$();
            if (ff.exists(path)) {
                openPartitionColumns(columnIndex, transientRowCount, columnOffsets);
            }
        } finally {
            path.trimTo(rootLen);
        }
        return transientRowCount;
    }

    private void openMetaFile() {
        try {
            metaMem.of(path.concat(META_FILE_NAME).$(), ff.getPageSize(), ff.length(path));
        } finally {
            path.trimTo(rootLen);
        }
    }

    private long openPartition(int partitionIndex, int columnBase, long columnTops[], boolean last) {
        long size;

        switch (partitionBy) {
            case PartitionBy.YEAR:
                size = openPartition(columnBase, Dates.addYear(partitionMin, partitionIndex), last, columnTops);
                break;
            case PartitionBy.MONTH:
                size = openPartition(columnBase, Dates.addMonths(partitionMin, partitionIndex), last, columnTops);
                break;
            case PartitionBy.DAY:
                size = openPartition(columnBase, Dates.addDays(partitionMin, partitionIndex), last, columnTops);
                break;
            default:
                size = loadDefaultPartition(columnBase, columnTops);
                break;
        }
        partitionSizes.setQuick(partitionIndex, size);
        return size;
    }

    private long openPartition(int columnBase, long partitionTimestamp, boolean last, long[] columnTops) {
        try {
            partitionDirFmt.format(partitionTimestamp, DateLocaleFactory.INSTANCE.getDefaultDateLocale(), null, path.put(Path.SEPARATOR));
            final long partitionSize;
            if (ff.exists(path.$())) {

                path.trimTo(path.length());

                if (last) {
                    partitionSize = transientRowCount;
                } else {
                    partitionSize = readPartitionSize(ff, path);
                }

                LOG.info().$("Open partition: ").$(path.$()).$(" [size=").$(partitionSize).$(']').$();

                if (partitionSize > 0) {
                    openPartitionColumns(columnBase, partitionSize, columnTops);
                }
            } else {
                partitionSize = 0;
            }
            return partitionSize;
        } finally {
            path.trimTo(rootLen);
        }

    }

    private void openPartitionColumns(int columnBase, long partitionSize, long[] columnTops) {
        int plen = path.length();
        try {
            for (int i = 0; i < columnCount; i++) {
                if (columns.getQuick(getPrimaryColumnIndex(columnBase, i)) == null) {
                    String name = metadata.getColumnName(i);
                    if (ff.exists(TableWriter.dFile(path.trimTo(plen), name))) {

                        // we defer setting size
                        ReadOnlyMemory mem1 = new ReadOnlyMemory(ff);
                        ReadOnlyMemory mem2;

                        long offset;
                        long len;
                        mem1.of(path, TableWriter.getMapPageSize(ff));
                        columns.setQuick(getPrimaryColumnIndex(columnBase, i), mem1);

                        switch (metadata.getColumnQuick(i).getType()) {
                            case ColumnType.STRING:
                            case ColumnType.SYMBOL:
                                mem2 = new ReadOnlyMemory(ff, TableWriter.iFile(path.trimTo(plen), name), TableWriter.getMapPageSize(ff), partitionSize * 8);
                                offset = mem2.getLong((partitionSize - 1) * 8);
                                if (ff.read(mem1.getFd(), tempMem8b, 4, offset) != 4) {
                                    throw CairoException.instance(ff.errno()).put("Cannot read string column length, fd=").put(mem2.getFd()).put(", offset=").put(offset);
                                }
                                len = Unsafe.getUnsafe().getInt(tempMem8b);
                                if (len == -1) {
                                    mem1.setSize(offset + 4);
                                } else {
                                    mem1.setSize(offset + len + 4);
                                }
                                columns.setQuick(getSecondaryColumnIndex(columnBase, i), mem2);
                                break;
                            case ColumnType.BINARY:
                                mem2 = new ReadOnlyMemory(ff, TableWriter.iFile(path.trimTo(plen), name), TableWriter.getMapPageSize(ff), partitionSize * 8);
                                offset = mem2.getLong((partitionSize - 1) * 8);
                                if (ff.read(mem1.getFd(), tempMem8b, 8, offset) != 8) {
                                    throw CairoException.instance(ff.errno()).put("Cannot read bin column length, fd=").put(mem2.getFd()).put(", offset=").put(offset);
                                }
                                len = Unsafe.getUnsafe().getLong(tempMem8b);
                                if (len == -1) {
                                    mem1.setSize(offset + 8);
                                } else {
                                    mem1.setSize(offset + len + 8);
                                }
                                columns.setQuick(getSecondaryColumnIndex(columnBase, i), mem2);
                                break;
                            case ColumnType.LONG:
                            case ColumnType.DOUBLE:
                            case ColumnType.DATE:
                                mem1.setSize(partitionSize * 8);
                                break;
                            case ColumnType.INT:
                            case ColumnType.FLOAT:
                                mem1.setSize(partitionSize * 4);
                                break;
                            case ColumnType.SHORT:
                                mem1.setSize(partitionSize * 2);
                                break;
                            case ColumnType.BOOLEAN:
                            case ColumnType.BYTE:
                                mem1.setSize(partitionSize);
                                break;
                            default:
                                throw CairoException.instance(0).put("Unsupported type: ").put(ColumnType.nameOf(metadata.getColumnQuick(i).getType()));
                        }
                        columnTops[i] = readColumnTop(ff, path, name, plen);
                    }
                }
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private ReadOnlyMemory openTxnFile() {
        try {
            if (ff.exists(path.concat(TXN_FILE_NAME).$())) {
                return new ReadOnlyMemory(ff, path, ff.getPageSize(), ff.length(path));
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);

        } finally {
            path.trimTo(rootLen);
        }
    }

    private boolean readTxn() {
        while (true) {
            long txn = txMem.getLong(TableWriter.TX_OFFSET_TXN);

            if (txn == this.txn) {
                return false;
            }

            Unsafe.getUnsafe().loadFence();
            long transientRowCount = txMem.getLong(TableWriter.TX_OFFSET_TRANSIENT_ROW_COUNT);
            long fixedRowCount = txMem.getLong(TableWriter.TX_OFFSET_FIXED_ROW_COUNT);
            long maxTimestamp = txMem.getLong(TableWriter.TX_OFFSET_MAX_TIMESTAMP);
            Unsafe.getUnsafe().loadFence();
            if (txn == txMem.getLong(TableWriter.TX_OFFSET_TXN)) {
                this.txn = txn;
                this.transientRowCount = transientRowCount;
                this.size = fixedRowCount + transientRowCount;
                break;
            }
            LockSupport.parkNanos(1);
        }
        return true;
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
            int columnBase = reader.partitionIndex << reader.columnCountBits;

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

            long offset = TableWriter.getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                int type = metaMem.getInt(TableWriter.META_OFFSET_COLUMN_TYPES + i * 4);
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
