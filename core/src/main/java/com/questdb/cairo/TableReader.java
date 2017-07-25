package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.ex.NumericException;
import com.questdb.factory.configuration.AbstractRecordMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.*;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.std.time.Dates;
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
    private long fixedRowCount;
    private long maxTimestamp;
    private long txn = -1;
    private int timestampIndex;
    private int partitionCount;
    private long partitionMin;
    private long partitionMax;

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
        this.columns = new ObjList<>(this.partitionCount == 1 ? columnCount : columnBlockSize * partitionCount);
        this.partitionSizes = new LongList(partitionCount);
        this.partitionSizes.seed(partitionCount, -1);
        readTx();
    }

    @Override
    public void close() throws IOException {

    }

    // this is prototype to help visualize iteration over table data
    public void readAll() {
        for (int i = 0; i < partitionCount; i++) {
            int columnIndex = i * columnBlockSize;
            long partitionSize = partitionSizes.getQuick(i);

            if (partitionSize == -1) {
                partitionSize = loadPartition(i, columnIndex);
            }


        }
    }

    private void countPartitions() {
        if (partitionDirFmt == null) {
            partitionCount = 1;
            return;
        }

        partitionMin = Long.MAX_VALUE;
        partitionMax = Long.MIN_VALUE;
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

    private long loadPartition(int index, int columnIndex) {
        long partitionTimestamp;

        switch (partitionBy) {
            case PartitionBy.YEAR:
                partitionTimestamp = Dates.addYear(partitionMin, index);
                break;
            case PartitionBy.MONTH:
                partitionTimestamp = Dates.addMonths(partitionMin, index);
                break;
            case PartitionBy.DAY:
                partitionTimestamp = Dates.addDays(partitionMin, index);
                break;
            default:
                return -1;
        }

        partitionDirFmt.format(partitionTimestamp, DateLocaleFactory.INSTANCE.getDefaultDateLocale(), null, path);
        int plen = path.length();

        long partitionSize = 0;

        if (ff.exists(path.$())) {
            path.trimTo(plen);
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
                        partitionSize = Unsafe.getUnsafe().getLong(mem);
                    } finally {
                        Unsafe.free(mem, 8);
                    }
                } finally {
                    ff.close(fd);
                }

                if (partitionSize > 0) {

                }

            } else {
                throw CairoException.instance(0).put("Doesn't exist: ").put(path);
            }
        } else {
            partitionSize = 0;
        }
        partitionSizes.setQuick(index, partitionSize);
        return partitionSize;
    }

    private void openMetaFile() {
        try {
            metaMem.of(path.concat(META_FILE_NAME).$(), ff.getPageSize(), ff.length(path));
        } finally {
            path.trimTo(rootLen);
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
                this.fixedRowCount = fixedRowCount;
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
