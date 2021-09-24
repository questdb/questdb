/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class TableReaderMetadata extends BaseRecordMetadata implements Closeable {
    private final MemoryMR metaMem;
    private final Path path;
    private final FilesFacade ff;
    private final LowerCaseCharSequenceIntHashMap tmpValidationMap = new LowerCaseCharSequenceIntHashMap();
    private int id;
    private MemoryMR transitionMeta;

    public TableReaderMetadata(FilesFacade ff) {
        this.path = new Path();
        this.ff = ff;
        this.metaMem = Vm.getMRInstance();
        this.columnMetadata = new ObjList<>(columnCount);
        this.columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    }

    public TableReaderMetadata(FilesFacade ff, Path path) {
        this(ff);
        of(path, ColumnType.VERSION);
    }

    public TableReaderMetadata of(Path path, int expectedVersion) {
        this.path.of(path).$();
        try {
            this.metaMem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
            this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            this.columnNameIndexMap.clear();
            TableUtils.validate(ff, metaMem, this.columnNameIndexMap, expectedVersion);
            this.timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            this.id = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
            this.columnMetadata.clear();
            long offset = TableUtils.getColumnNameOffset(columnCount);

            // don't create strings in this loop, we already have them in columnNameIndexMap
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStr(offset);
                assert name != null;
                columnMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(name),
                                TableUtils.getColumnType(metaMem, i),
                                TableUtils.isColumnIndexed(metaMem, i),
                                TableUtils.getIndexBlockCapacity(metaMem, i),
                                true,
                                null
                        )
                );
                offset += Vm.getStorageLength(name);
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
        return this;
    }

    public static void freeTransitionIndex(long address) {
        if (address == 0) {
            return;
        }
        Unsafe.free(address, Unsafe.getUnsafe().getInt(address), MemoryTag.NATIVE_DEFAULT);
    }

    public void applyTransitionIndex(long pTransitionIndex) {
        // re-open _meta file
        this.metaMem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
        this.columnNameIndexMap.clear();

        final int columnCount = Unsafe.getUnsafe().getInt(pTransitionIndex + 4);
        final long index = pTransitionIndex + 8;
        final long stateAddress = index + columnCount * 8L;


        if (columnCount > this.columnCount) {
            columnMetadata.setPos(columnCount);
            this.columnCount = columnCount;
        }

        Vect.memset(stateAddress, columnCount, 0);

        // this is a silly exercise in walking the index
        for (int i = 0; i < columnCount; i++) {

            // prevent writing same entry once
            if (Unsafe.getUnsafe().getByte(stateAddress + i) == -1) {
                continue;
            }

            Unsafe.getUnsafe().putByte(stateAddress + i, (byte) -1);

            int copyFrom = Unsafe.getUnsafe().getInt(index + i * 8L);

            // don't copy entries to themselves
            if (copyFrom == i + 1) {
                columnNameIndexMap.put(columnMetadata.getQuick(i).getName(), i);
                continue;
            }

            // check where we source entry:
            // 1. from another entry
            // 2. create new instance
            TableColumnMetadata tmp;
            if (copyFrom > 0) {
                tmp = moveMetadata(copyFrom - 1, null);
                columnNameIndexMap.put(tmp.getName(), i);
                tmp = moveMetadata(i, tmp);

                int copyTo = Unsafe.getUnsafe().getInt(index + i * 8L + 4);

                // now we copied entry, what do we do with value that was already there?
                // do we copy it somewhere else?
                while (copyTo > 0) {

                    // Yeah, we do. This can get recursive!

                    // prevent writing same entry twice
                    if (Unsafe.getUnsafe().getByte(stateAddress + copyTo - 1) == -1) {
                        break;
                    }
                    Unsafe.getUnsafe().putByte(stateAddress + copyTo - 1, (byte) -1);

                    columnNameIndexMap.put(tmp.getName(), copyTo - 1);
                    tmp = moveMetadata(copyTo - 1, tmp);
                    copyTo = Unsafe.getUnsafe().getInt(index + (copyTo - 1) * 8L + 4);
                }
            } else {
                // new instance
                TableColumnMetadata m = newInstance(i, columnCount);
                moveMetadata(i, m);
                columnNameIndexMap.put(m.getName(), i);
            }
        }

        // ended up with fewer columns than before?
        // good idea to resize array and may be drop timestamp index
        if (columnCount < this.columnCount) {
            // there is assertion further in the code that
            // new metadata does not suddenly displace non-null object
            // to make sure all fine and dandy we need to remove
            // all metadata objects from tail of the list
            columnMetadata.set(columnCount, this.columnCount, null);
            // and set metadata list to correct length
            columnMetadata.setPos(columnCount);
            // we are done
            this.columnCount = columnCount;
        }
        this.timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
    }

    public void cloneTo(MemoryMA mem) {
        long len = ff.length(metaMem.getFd());
        for (long p = 0; p < len; p++) {
            mem.putByte(metaMem.getByte(p));
        }
    }

    @Override
    public void close() {
        Misc.free(metaMem);
        Misc.free(path);
    }

    public long createTransitionIndex() {
        if (transitionMeta == null) {
            transitionMeta = Vm.getMRInstance();
        }

        transitionMeta.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
        try (MemoryMR metaMem = transitionMeta) {

            tmpValidationMap.clear();
            TableUtils.validate(ff, metaMem, tmpValidationMap, ColumnType.VERSION);

            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            int n = Math.max(this.columnCount, columnCount);
            final long pTransitionIndex;
            final int size = n * 16;

            long index = pTransitionIndex = Unsafe.calloc(size, MemoryTag.NATIVE_DEFAULT);
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
                offset += Vm.getStorageLength(name);
                int oldPosition = columnNameIndexMap.get(name);
                // write primary (immutable) index
                if (oldPosition > -1
                        && TableUtils.getColumnType(metaMem, i) == TableUtils.getColumnType(this.metaMem, oldPosition)
                        && TableUtils.isColumnIndexed(metaMem, i) == TableUtils.isColumnIndexed(this.metaMem, oldPosition)) {
                    Unsafe.getUnsafe().putInt(index + i * 8L, oldPosition + 1);
                    Unsafe.getUnsafe().putInt(index + oldPosition * 8L + 4, i + 1);
                } else {
                    Unsafe.getUnsafe().putLong(index + i * 8L, 0);
                }
            }
            return pTransitionIndex;
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    public int getId() {
        return id;
    }

    public int getPartitionBy() {
        return metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
    }

    public int getVersion() {
        return metaMem.getInt(TableUtils.META_OFFSET_VERSION);
    }

    public int getMaxUncommittedRows() {
        return metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
    }

    public long getCommitLag() {
        return metaMem.getLong(TableUtils.META_OFFSET_COMMIT_LAG);
    }

    private TableColumnMetadata moveMetadata(int index, TableColumnMetadata metadata) {
        return columnMetadata.getAndSetQuick(index, metadata);
    }

    private TableColumnMetadata newInstance(int index, int columnCount) {
        long offset = TableUtils.getColumnNameOffset(columnCount);
        CharSequence name = null;
        for (int i = 0; i <= index; i++) {
            name = metaMem.getStr(offset);
            offset += Vm.getStorageLength(name);
        }
        assert name != null;
        return new TableColumnMetadata(
                Chars.toString(name),
                TableUtils.getColumnType(metaMem, index),
                TableUtils.isColumnIndexed(metaMem, index),
                TableUtils.getIndexBlockCapacity(metaMem, index),
                true,
                null
        );
    }
}
