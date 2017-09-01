package com.questdb.cairo;

import com.questdb.factory.configuration.AbstractRecordMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.FilesFacade;
import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.ObjList;
import com.questdb.std.str.CompositePath;
import com.questdb.store.ColumnType;

import java.io.Closeable;

class TableMetadata extends AbstractRecordMetadata implements Closeable {
    private final ObjList<TableColumnMetadata> columnMetadata;
    private final CharSequenceIntHashMap columnNameHashTable;
    private final int timestampIndex;
    private final ReadOnlyMemory metaMem;
    private final int columnCount;

    public TableMetadata(FilesFacade ff, CompositePath path) {
        this.metaMem = new ReadOnlyMemory(ff, path, ff.getPageSize());
        this.timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        this.columnMetadata = new ObjList<>(columnCount);
        this.columnNameHashTable = new CharSequenceIntHashMap(columnCount);

        long offset = TableUtils.getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            CharSequence name = metaMem.getStr(offset);
            assert name != null;
            String s = name.toString();
            columnMetadata.add(new TableColumnMetadata(s, TableUtils.getColumnType(metaMem, i)));
            columnNameHashTable.put(s, i);
            offset += ReadOnlyMemory.getStorageLength(name);
        }
    }

    public static void freeTransitionIndex(long address) {
        if (address == 0) {
            return;
        }

        Unsafe.free(address, Unsafe.getUnsafe().getInt(address) * 16);
    }

    public static <T> void shuffle(long address, Shuffler<T> shuffler, Factory<T> factory) {
        final int n = Unsafe.getUnsafe().getInt(address);
        final int columnCount = Unsafe.getUnsafe().getInt(address + 4);
        final long index = address + 8;
        final long stateAddress = index + columnCount * 8;

        Unsafe.getUnsafe().setMemory(stateAddress, columnCount, (byte) 0);

        // this is a silly exercise in walking the index
        for (int i = 0; i < n; i++) {

            if (Unsafe.getUnsafe().getByte(stateAddress + i) == -1) {
                continue;
            }

            Unsafe.getUnsafe().putByte(stateAddress + i, (byte) -1);

            final int copyFrom = Unsafe.getUnsafe().getInt(index + i * 8);

            if (copyFrom == i + 1) {
                continue;
            }

            if (copyFrom > 0) {
                T tmp = shuffler.move(copyFrom - 1, null);
                shuffler.move(i, tmp);

                int copyTo = Unsafe.getUnsafe().getInt(index + i * 8 + 4);
                while (copyTo > 0) {
                    if (Unsafe.getUnsafe().getByte(stateAddress + copyTo - 1) == -1) {
                        break;
                    }
                    Unsafe.getUnsafe().putByte(stateAddress + copyTo - 1, (byte) -1);
                    tmp = shuffler.move(copyTo - 1, tmp);
                    copyTo = Unsafe.getUnsafe().getInt(index + (copyTo - 1) * 8 + 4);
                }
                Misc.free(tmp);
            } else {
                // new instance
                shuffler.move(i, factory.newInstance(i));
            }
        }
    }

    public void applyTransitionIndex(long index) {
        shuffle(index, this::moveMetadata, this::newInstance);
    }

    @Override
    public void close() {
        metaMem.close();
    }

    public long createTransitionIndex() {
        int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        int n = Math.max(this.columnCount, columnCount);
        final long address;
        long index = address = Unsafe.malloc(n * 16);
        Unsafe.getUnsafe().putInt(index, n);
        Unsafe.getUnsafe().putInt(index + 4, columnCount);
        index += 8;

        // index structure is
        // [copy from, copy to] int tuples, each of which is index into original column metadata
        // the number of these tuples is DOUBLE of maximum of old and new column count.
        // Tuples are separated into two areas, one is immutable, which drives how metadata should be moved,
        // the other is the state of moving algo. Moving algo will start with copy of immutable area and will
        // continue to zero out tuple values in mutable area when metadata is moved. Mutable area is

        // "copy from" == 0 indicates that column is newly added, similarly
        // "copy to" == 0 indicates that old column has been deleted
        //

        long offset = TableUtils.getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            CharSequence name = metaMem.getStr(offset);
            offset += ReadOnlyMemory.getStorageLength(name);
            int oldPosition = columnNameHashTable.get(name);
            // write primary (immutable) index
            if (oldPosition > -1) {
                Unsafe.getUnsafe().putInt(index + i * 8, oldPosition + 1);
                Unsafe.getUnsafe().putInt(index + oldPosition * 8 + 4, i + 1);
            }
        }

        /*
        Unsafe.getUnsafe().setMemory(index + columnCount * 8, columnCount, (byte) 0);

        // this is a silly exercise in walking the index
        for (int i = 0; i < n; i++) {

            if (Unsafe.getUnsafe().getByte(index + columnCount * 8 + i) == -1) {
                continue;
            }

            Unsafe.getUnsafe().putByte(index + columnCount * 8 + i, (byte) -1);

            int copyFrom = Unsafe.getUnsafe().getInt(index + i * 8);
            int copyTo = Unsafe.getUnsafe().getInt(index + i * 8 + 4);

            if (copyFrom == i + 1) {
                continue;
            }

            TableColumnMetadata tmp = columnMetadata.getQuick(i);
            if (copyFrom > 0) {
                columnMetadata.setQuick(i, columnMetadata.getAndSetQuick(copyFrom - 1, null));
            } else {
                System.out.println("new metadata");
            }

            while (copyTo > 0) {
                if (Unsafe.getUnsafe().getByte(index + columnCount * 8 + copyTo - 1) == -1) {
                    break;
                }
                Unsafe.getUnsafe().putByte(index + columnCount * 8 + copyTo - 1, (byte) -1);
                tmp = columnMetadata.getAndSetQuick(copyTo - 1, tmp);
                copyTo = Unsafe.getUnsafe().getInt(index + (copyTo - 1) * 8 + 4);
            }

            if (tmp != null) {
                System.out.println("Release: " + tmp);
            }
            // 01932373421
        }
        */
        return address;
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

    public int getPartitionBy() {
        return metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
    }

    private TableColumnMetadata moveMetadata(int index, TableColumnMetadata metadata) {
        return columnMetadata.getAndSetQuick(index, metadata);
    }

    private TableColumnMetadata newInstance(int index) {
        int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        if (index < columnCount) {
            long offset = TableUtils.getColumnNameOffset(columnCount);
            CharSequence name = null;
            for (int i = 0; i <= index; i++) {
                name = metaMem.getStr(offset);
                offset += ReadOnlyMemory.getStorageLength(name);
            }

            if (name == null) {
                throw CairoException.instance(0).put("Got NULL column name while looking for column index ").put(index).put(" in file [").put(metaMem.getFd()).put(']');
            }

            int type = TableUtils.getColumnType(metaMem, index);

            if (ColumnType.sizeOf(type) == -1) {
                throw CairoException.instance(0).put("Unrecognized column type for index ").put(index).put(" in file [").put(metaMem.getFd()).put(']');
            }
            return new TableColumnMetadata(name.toString(), type);
        }

        throw CairoException.instance(0).put("Column index is beyond column count. [").put(index).put(">=").put(columnCount).put("] in file [").put(metaMem.getFd()).put(']');
    }

    @FunctionalInterface
    public interface Shuffler<T> {
        T move(int index, T item);
    }

    @FunctionalInterface
    public interface Factory<T> {
        T newInstance(int index);
    }
}
