package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.ex.NumericException;
import com.questdb.factory.configuration.AbstractRecordMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.*;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.RecordCursor;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.std.time.Dates;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import static com.questdb.cairo.TableWriter.*;

public class TableReader implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
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
    private int columnCount;
    private int columnBlockSize;
    private long transientRowCount;
    private long size;
    private long maxTimestamp;
    private long txn = -1;
    private int timestampIndex;
    private int partitionCount;
    private long partitionMin;
    private long columnTops[];

    public TableReader(FilesFacade ff, CharSequence root, CharSequence name) {
        this.ff = ff;
        this.path = new CompositePath().of(root).concat(name);
        this.rootLen = path.length();
        this.txMem = openTxnFile();

        if (readTodoTaskCode() != -1) {
            throw CairoException.instance(0).put("Table ").put(path.$()).put(" is pending recovery.");
        }

        this.metaMem = new ReadOnlyMemory(ff);
        openMetaFile();
        this.columnCount = metaMem.getInt(META_OFFSET_COUNT);
        this.columnBlockSize = Numbers.ceilPow2(this.columnCount);
        this.partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
        this.timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
        this.metadata = new Meta(columnCount);
        this.partitionDirFmt = TableWriter.selectPartitionDirFmt(partitionBy);
        countPartitions();
        int columnListCapacity = (this.partitionCount == 1 ? columnCount : columnBlockSize * partitionCount) * 2;
        this.columns = new ObjList<>(columnListCapacity);
        columns.extendAndSet(columnListCapacity - 1, null);
        this.partitionSizes = new LongList(partitionCount);
        this.partitionSizes.seed(partitionCount, -1);
        readTx();
        this.columnTops = new long[columnCount];
    }

    @Override
    public void close() throws IOException {
        Misc.free(path);
        Misc.free(metaMem);
        Misc.free(txMem);
        for (int i = 0, n = columns.size(); i < n; i++) {
            VirtualMemory mem = columns.getQuick(i);
            if (mem != null) {
                mem.close();
            }
        }
    }

    public RecordCursor prepareCursor(CancellationHandler cancellationHandler) {
        return null;
    }

    // this is prototype to help visualize iteration over table data
    public void readAll() {
        int last = partitionCount - 1;
        for (int i = 0; i < partitionCount; i++) {
            int columnIndex = i * columnBlockSize * 2;
            long partitionSize = partitionSizes.getQuick(i);

            if (partitionSize == -1) {
                partitionSize = openPartition(i, columnIndex, columnTops, i == last);
            }

            while (partitionSize-- > 0) {

            }
        }
    }

    public long size() {
        return size;
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
                            } catch (NumericException e) {
                                e.printStackTrace();
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

    /**
     * This method is used to configure variable-length columns, such as STRING or BINARY.
     * It will create instance of index memory and use it to find pointer to last entry in data column.
     * This pointer is used to set size of data column. This isnt entirely accurate as it doesn't take
     * into account the length data entry this pointer is identifying. To simplify instantiation we will
     * extend data column by at least length of size entry, which is 8 bytes for BINARY and 4 bytes for STRING.
     * This is what 'suffixLen' refers to.
     */
    private ReadOnlyMemory openIndexColumn(LPSZ p, ReadOnlyMemory dataMem, long partitionSize, int suffixLen) {
        ReadOnlyMemory mem = new ReadOnlyMemory(ff, p, TableWriter.getMapPageSize(ff), partitionSize * 8);
        dataMem.setSize(TableWriter.getMapPageSize(ff), mem.getLong((partitionSize - 1) * 8) + suffixLen);
        return mem;
    }

    private void openMetaFile() {
        try {
            metaMem.of(path.concat(META_FILE_NAME).$(), ff.getPageSize(), ff.length(path));
        } finally {
            path.trimTo(rootLen);
        }
    }

    private long openPartition(int partitionIndex, int columnIndex, long columnTops[], boolean last) {
        long size;

        switch (partitionBy) {
            case PartitionBy.YEAR:
                size = openPartition(columnIndex, Dates.addYear(partitionMin, partitionIndex), last, columnTops);
                break;
            case PartitionBy.MONTH:
                size = openPartition(columnIndex, Dates.addMonths(partitionMin, partitionIndex), last, columnTops);
                break;
            case PartitionBy.DAY:
                size = openPartition(columnIndex, Dates.addDays(partitionMin, partitionIndex), last, columnTops);
                break;
            default:
                size = loadDefaultPartition(columnIndex, columnTops);
                break;
        }
        partitionSizes.setQuick(partitionIndex, size);
        return size;
    }

    private long openPartition(int columnIndex, long partitionTimestamp, boolean last, long[] columnTops) {
        try {
            partitionDirFmt.format(partitionTimestamp, DateLocaleFactory.INSTANCE.getDefaultDateLocale(), null, path.put(Path.SEPARATOR));
            final long partitionSize;
            if (ff.exists(path.$())) {
                path.trimTo(path.length());
                if (last) {
                    partitionSize = transientRowCount;
                } else {
                    partitionSize = readPartitionSize();
                }

                if (partitionSize > 0) {
                    openPartitionColumns(columnIndex, partitionSize, columnTops);
                }
            } else {
                partitionSize = 0;
            }
            return partitionSize;
        } finally {
            path.trimTo(rootLen);
        }

    }

    private void openPartitionColumns(int columnIndex, long partitionSize, long[] columnTops) {
        int plen = path.length();
        try {
            for (int i = 0; i < columnCount; i++) {
                if (columns.getQuick(columnIndex + i * 2) == null) {
                    String name = metadata.getColumnName(i);
                    if (ff.exists(TableWriter.dFile(path.trimTo(plen), name))) {

                        // we defer setting size
                        ReadOnlyMemory mem1 = new ReadOnlyMemory(ff);
                        mem1.of(path);
                        columns.setQuick(columnIndex + i * 2, mem1);

                        switch (metadata.getColumnQuick(i).getType()) {
                            case ColumnType.STRING:
                            case ColumnType.SYMBOL:
                                columns.setQuick(columnIndex + i * 2 + 1, openIndexColumn(TableWriter.iFile(path.trimTo(plen), name), mem1, partitionSize, 4));
                                break;
                            case ColumnType.BINARY:
                                columns.setQuick(columnIndex + i * 2 + 1, openIndexColumn(TableWriter.iFile(path.trimTo(plen), name), mem1, partitionSize, 8));
                                break;
                            case ColumnType.LONG:
                            case ColumnType.DOUBLE:
                            case ColumnType.DATE:
                                mem1.setSize(TableWriter.getMapPageSize(ff), partitionSize * 8);
                                break;
                            case ColumnType.INT:
                            case ColumnType.FLOAT:
                                mem1.setSize(TableWriter.getMapPageSize(ff), partitionSize * 4);
                                break;
                            case ColumnType.SHORT:
                                mem1.setSize(TableWriter.getMapPageSize(ff), partitionSize * 2);
                                break;
                            case ColumnType.BOOLEAN:
                            case ColumnType.BYTE:
                                mem1.setSize(TableWriter.getMapPageSize(ff), partitionSize);
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

    private long readPartitionSize() {
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

    private byte readTodoTaskCode() {
        try {
            if (ff.exists(path.concat(TODO_FILE_NAME).$())) {
                long todoFd = ff.openRO(path);
                if (todoFd == -1) {
                    throw CairoException.instance(Os.errno()).put("Cannot open *todo*: ").put(path);
                }
                try {
                    long buf = Unsafe.malloc(1);
                    try {
                        if (ff.read(todoFd, buf, 1, 0) != 1) {
                            LOG.info().$("Cannot read *todo* code. File seems to be truncated. Ignoring").$();
                            return -1;
                        }
                        return Unsafe.getUnsafe().getByte(buf);
                    } finally {
                        Unsafe.free(buf, 1);
                    }
                } finally {
                    ff.close(todoFd);
                }
            }
            return -1;
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void readTx() {
        while (true) {
            long txn = txMem.getLong(TableWriter.TX_OFFSET_TXN);

            if (txn == this.txn) {
                break;
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
                this.maxTimestamp = maxTimestamp;
                break;
            }

            LockSupport.parkNanos(1);
        }
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
