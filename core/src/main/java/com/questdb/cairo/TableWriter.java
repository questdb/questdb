package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.misc.Files;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.VirtualMemory;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

import java.util.function.LongSupplier;

public class TableWriter {
    private final JournalMetadata metadata;
    private final ObjList<AppendMemory> columns = new ObjList<>();
    private final CompositePath path;
    private final LongList refs = new LongList();
    private final Row row = new Row();
    private final int rootLen;
    private long masterRef = 0;
    private int columnCount;
    private Runnable[] nullers;
    private int mode = 509;
    private VirtualMemory indexMem;
    private LongSupplier sizer;

    public TableWriter(CharSequence location, JournalMetadata metadata) {
        this.metadata = metadata;
        this.columnCount = metadata.getColumnCount();
        this.refs.extendAndSet(columnCount, 0);
        this.nullers = new Runnable[columnCount];
        this.path = new CompositePath().of(location).concat(metadata.getName());
        this.rootLen = path.length();

        if (metadata.getPartitionBy() == PartitionBy.NONE || metadata.getPartitionBy() == PartitionBy.DEFAULT) {
            configurePartition("default");
        }
    }

    public void commit() {
        indexMem.jumpTo(8);
        for (int i = 0; i < columnCount; i++) {
            VirtualMemory m = columns.getQuick(i * 2);
            indexMem.putLong(m.size());

            m = columns.getQuick(i * 2 + 1);
            if (m == null) {
                indexMem.skip(8);
            } else {
                indexMem.putLong(m.size());
            }
        }
    }

    public Row newRow() {
        masterRef++;
        return row;
    }

    public void truncate() {
        for (int i = 0; i < columnCount; i++) {
            columns.getQuick(i * 2).truncate();
            AppendMemory mem = columns.getQuick(i * 2 + 1);
            if (mem != null) {
                mem.truncate();
            }
        }
    }

    private void configureNuller(int index, int type, AppendMemory mem1, AppendMemory mem2) {
        switch (type) {
            case ColumnType.STRING:
                nullers[index] = () -> mem2.putLong(mem1.putNullStr());
                break;
            case ColumnType.SYMBOL:
                nullers[index] = () -> mem1.putInt(SymbolTable.VALUE_IS_NULL);
                break;
            case ColumnType.INT:
                nullers[index] = () -> mem1.putInt(Numbers.INT_NaN);
                break;
            case ColumnType.FLOAT:
                nullers[index] = () -> mem1.putFloat(Float.NaN);
                break;
            case ColumnType.DOUBLE:
                nullers[index] = () -> mem1.putDouble(Double.NaN);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
                nullers[index] = () -> mem1.putLong(Numbers.LONG_NaN);
                break;
            case ColumnType.BINARY:
                nullers[index] = () -> mem2.putLong(mem1.putNullBin());
                break;
            default:
                break;
        }
    }

    private void configurePartition(CharSequence name) {
        path.concat(name);
        int partitionPathLen = path.length();

        if (Files.mkdirs(path.append(Path.SEPARATOR).$(), mode) != 0) {
            throw new RuntimeException("cannot create def partition");
        }

        path.trimTo(partitionPathLen);

        path.concat("_index.qdb").$();
        if (Files.exists(path)) {
            indexMem = readIndexData(path);
        } else {
            indexMem = createIndexData(path);
        }
        path.trimTo(partitionPathLen);

        assert columnCount > 0;

        for (int i = 0; i < columnCount; i++) {
            ColumnMetadata m = metadata.getColumnQuick(i);
            columns.add(new AppendMemory(path.concat(m.name).append(".d").$(), 4096 * 4096, indexMem.getLong(i * 16 + 8)));
            path.trimTo(partitionPathLen);

            if (m.type == ColumnType.BINARY || m.type == ColumnType.STRING || m.type == ColumnType.SYMBOL) {
                columns.add(new AppendMemory(path.concat(m.name).append(".i").$(), 4096 * 4096, indexMem.getLong(i * 16 + 16)));
                path.trimTo(partitionPathLen);
            } else {
                columns.add(null);
            }
            // set nullers
            configureNuller(i, m.type, columns.getQuick(i), columns.getQuick(i + 1));
        }
        path.trimBy(partitionPathLen - rootLen);
    }

    private VirtualMemory createIndexData(LPSZ path) {
        ReadWriteMemory mem = new ReadWriteMemory(path, (int) Files.PAGE_SIZE, 0L, (int) Files.PAGE_SIZE);
        mem.putInt(0xdeadbeef);
        mem.putInt(columnCount);
        // reserve space for column sizes and their initial row counts
        // note: there are two files for string and binary columns, so we keep two 64bit values for each logical column in the table.
        // note: to save disk space initial row count may be non-zero if column added later on

        // struct {
        //      int tag;
        //      int columnCount;
        //      COLUMN_SIZE sizes[columnCount];
        //      long firstRows[columnCount];
        // }
        // typedef struct {
        //      long file1Size;
        //      long file2Size;
        // } COLUMN_SIZE

        mem.skip(columnCount * 8 * 3);
        return mem;
    }

    private VirtualMemory readIndexData(LPSZ path) {
        ReadWriteMemory indexMem = new ReadWriteMemory(path);
        // validate header mark and column count
        if (indexMem.getInt(0) != 0xdeadbeef) {
            indexMem.close();
            throw new RuntimeException("bad header");
        }

        if (indexMem.getInt(4) != columnCount) {
            indexMem.close();
            throw new RuntimeException("Column count mismatch");
        }

        return indexMem;
    }


    public class Row {
        public void append() {
            for (int i = 0; i < columnCount; i++) {
                if (refs.getQuick(i) < masterRef) {
                    Unsafe.arrayGet(nullers, i).run();
                }
            }
        }

        public void putDate(int index, long value) {
            putLong(index, value);
        }

        public void putDouble(int index, double value) {
            columns.getQuick(index * 2).putDouble(value);
            refs.setQuick(index, masterRef);
        }

        public void putInt(int index, int value) {
            columns.getQuick(index * 2).putInt(value);
            refs.setQuick(index, masterRef);
        }

        public void putLong(int index, long value) {
            columns.getQuick(index * 2).putLong(value);
            refs.setQuick(index, masterRef);
        }

        public void putStr(int index, CharSequence value) {
            long r = columns.getQuick(index * 2).putStr(value);
            columns.getQuick(index * 2 + 1).putLong(r);
            refs.setQuick(index, masterRef);
        }
    }

}
