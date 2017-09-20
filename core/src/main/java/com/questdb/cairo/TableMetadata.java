package com.questdb.cairo;

import com.questdb.factory.configuration.AbstractRecordMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.FilesFacade;
import com.questdb.misc.Unsafe;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.ObjList;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CompositePath;
import com.questdb.store.ColumnType;

import java.io.Closeable;

class TableMetadata extends AbstractRecordMetadata implements Closeable {
    private final static ThreadLocal<CharSequenceIntHashMap> tlColumnNameIndexMap = new ThreadLocal<>(CharSequenceIntHashMap::new);
    private final ObjList<TableColumnMetadata> columnMetadata;
    private final CharSequenceIntHashMap columnNameIndexMap = new CharSequenceIntHashMap();
    private final ReadOnlyMemory metaMem;
    private final CompositePath path;
    private final FilesFacade ff;
    private int timestampIndex;
    private int columnCount;
    private ReadOnlyMemory transitionMeta;

    public TableMetadata(FilesFacade ff, CompositePath path) {
        this.ff = ff;
        this.path = new CompositePath().of(path).$();
        this.metaMem = new ReadOnlyMemory(ff, path, ff.getPageSize());

        try {
            validate(ff, metaMem, this.columnNameIndexMap);
            this.timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            this.columnMetadata = new ObjList<>(columnCount);
            long offset = TableUtils.getColumnNameOffset(columnCount);

            // don't create strings in this loop, we already have them in columnNameIndexMap
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStr(offset);
                int index = columnNameIndexMap.getEntry(name);
                columnMetadata.add(new TableColumnMetadata(columnNameIndexMap.entryKey(index).toString(), TableUtils.getColumnType(metaMem, i)));
                offset += ReadOnlyMemory.getStorageLength(name);
            }
        } catch (CairoException e) {
            close();
            throw e;
        }
    }

    public static void freeTransitionIndex(long address) {
        if (address == 0) {
            return;
        }
        Unsafe.free(address, Unsafe.getUnsafe().getInt(address));
    }

    public static void validate(FilesFacade ff, ReadOnlyMemory metaMem, CharSequenceIntHashMap nameIndex) {
        try {
            final int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            final int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            long offset = TableUtils.getColumnNameOffset(columnCount);

            if (offset < columnCount || (
                    columnCount > 0 && (offset < 0 || offset >= ff.length(metaMem.getFd())))) {
                throw ex(metaMem).put("Incorrect columnCount: ").put(columnCount);
            }

            if (timestampIndex < -1 || timestampIndex >= columnCount) {
                throw ex(metaMem).put("Timestamp index is outside of columnCount");
            }

            if (timestampIndex != -1) {
                int timestampType = TableUtils.getColumnType(metaMem, timestampIndex);
                if (timestampType != ColumnType.DATE) {
                    throw ex(metaMem).put("Timestamp column must by DATE but found ").put(ColumnType.nameOf(timestampType));
                }
            }

            // validate column types
            for (int i = 0; i < columnCount; i++) {
                int type = TableUtils.getColumnType(metaMem, i);
                if (ColumnType.sizeOf(type) == -1) {
                    throw ex(metaMem).put("Invalid column type ").put(type).put(" at [").put(i).put(']');
                }
            }

            // validate column names
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStr(offset);
                if (name == null || name.length() < 1) {
                    throw ex(metaMem).put("NULL column name at [").put(i).put(']');
                }

                String s = name.toString();
                if (!nameIndex.put(s, i)) {
                    throw ex(metaMem).put("Duplicate column: ").put(s).put(" at [").put(i).put(']');
                }
                offset += ReadOnlyMemory.getStorageLength(name);
            }
        } catch (CairoException e) {
            nameIndex.clear();
            throw e;
        }
    }

    public void applyTransitionIndex(long address) {
        // re-open _meta file
        this.metaMem.of(ff, path, ff.getPageSize());

        this.columnNameIndexMap.clear();

        final int columnCount = Unsafe.getUnsafe().getInt(address + 4);
        final long index = address + 8;
        final long stateAddress = index + columnCount * 8;

        // this also copies metadata entries exactly once
        // the flag is there to monitor if we ever copy timestamp somewhere else
        // after timestamp moves once - we no longer interested in tracking it
        boolean timestampRequired = true;

        if (columnCount > this.columnCount) {
            columnMetadata.setPos(columnCount);
            this.columnCount = columnCount;
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
            if (copyFrom > 0) {
                TableColumnMetadata tmp = moveMetadata(copyFrom - 1, null);
                tmp = moveMetadata(i, tmp);

                if (copyFrom - 1 == timestampIndex && timestampRequired) {
                    timestampIndex = i;
                    timestampRequired = false;
                }

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

                    tmp = moveMetadata(copyTo - 1, tmp);
                    copyFrom = copyTo;
                    copyTo = Unsafe.getUnsafe().getInt(index + (copyTo - 1) * 8 + 4);

                    if ((copyFrom - 1) == timestampIndex && timestampRequired) {
                        timestampIndex = copyTo - 1;
                        timestampRequired = false;
                    }
                }

                // we have something left over?
                // this means we are losing entry, better clear timestamp index
                if (tmp != null && i == timestampIndex && timestampRequired) {
                    timestampIndex = -1;
                    timestampRequired = false;
                }
            } else {
                // new instance
                moveMetadata(i, newInstance(i, columnCount));
            }
        }

        // ended up with fewer columns than before?
        // good idea to resize array and may be drop timestamp index
        if (columnCount < this.columnCount) {
            if (timestampIndex >= columnCount && timestampRequired) {
                timestampIndex = -1;
            }
            columnMetadata.setPos(columnCount);
            this.columnCount = columnCount;
        }
    }

    @Override
    public void close() {
        metaMem.close();
        path.close();
    }

    public long createTransitionIndex() {
        if (transitionMeta == null) {
            transitionMeta = new ReadOnlyMemory();
        }

        transitionMeta.of(ff, path, ff.getPageSize());
        try (ReadOnlyMemory metaMem = transitionMeta) {

            validate(ff, metaMem);

            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            int n = Math.max(this.columnCount, columnCount);
            final long address;
            final int size = n * 16;

            long index = address = Unsafe.malloc(size);
            Unsafe.getUnsafe().setMemory(address, size, (byte) 0);
            Unsafe.getUnsafe().putInt(index, size);
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
                int oldPosition = columnNameIndexMap.get(name);
                // write primary (immutable) index
                if (oldPosition > -1 && TableUtils.getColumnType(metaMem, i) == TableUtils.getColumnType(this.metaMem, oldPosition)) {
                    Unsafe.getUnsafe().putInt(index + i * 8, oldPosition + 1);
                    Unsafe.getUnsafe().putInt(index + oldPosition * 8 + 4, i + 1);
                } else {
                    Unsafe.getUnsafe().putLong(index + i * 8, 0);
                }
            }
            return address;
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        return columnNameIndexMap.get(name);
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

    private static void validate(FilesFacade ff, ReadOnlyMemory metaMem) {
        CharSequenceIntHashMap map = tlColumnNameIndexMap.get();
        map.clear();
        validate(ff, metaMem, map);
    }

    private static CairoException ex(ReadOnlyMemory mem) {
        return CairoException.instance(0).put("Invalid metadata at fd=").put(mem.getFd()).put(". ");
    }

    private TableColumnMetadata moveMetadata(int index, TableColumnMetadata metadata) {
        return columnMetadata.getAndSetQuick(index, metadata);
    }

    private TableColumnMetadata newInstance(int index, int columnCount) {
        long offset = TableUtils.getColumnNameOffset(columnCount);
        CharSequence name = null;
        for (int i = 0; i <= index; i++) {
            name = metaMem.getStr(offset);
            offset += ReadOnlyMemory.getStorageLength(name);
        }
        assert name != null;
        return new TableColumnMetadata(name.toString(), TableUtils.getColumnType(metaMem, index));
    }
}
