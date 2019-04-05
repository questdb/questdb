/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

import com.questdb.std.*;
import com.questdb.std.str.Path;

import java.io.Closeable;

class TableReaderMetadata extends BaseRecordMetadata implements Closeable {
    private final ReadOnlyMemory metaMem;
    private final Path path;
    private final FilesFacade ff;
    private final CharSequenceIntHashMap tmpValidationMap = new CharSequenceIntHashMap();
    private ReadOnlyMemory transitionMeta;

    public TableReaderMetadata(FilesFacade ff, Path path) {
        this.ff = ff;
        this.path = new Path().of(path).$();
        try {
            this.metaMem = new ReadOnlyMemory(ff, path, ff.getPageSize(), ff.length(path));
            this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            this.columnNameIndexMap = new CharSequenceIntHashMap(columnCount);
            TableUtils.validate(ff, metaMem, this.columnNameIndexMap);
            this.timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            this.columnMetadata = new ObjList<>(columnCount);
            long offset = TableUtils.getColumnNameOffset(columnCount);

            // don't create strings in this loop, we already have them in columnNameIndexMap
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStr(offset);
                assert name != null;
                columnMetadata.add(
                        new TableColumnMetadata(
                                name.toString(),
                                TableUtils.getColumnType(metaMem, i),
                                TableUtils.isColumnIndexed(metaMem, i),
                                TableUtils.getIndexBlockCapacity(metaMem, i)
                        )
                );
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

    public void applyTransitionIndex(long pTransitionIndex) {
        // re-open _meta file
        this.metaMem.of(ff, path, ff.getPageSize(), ff.length(path));

        this.columnNameIndexMap.clear();

        final int columnCount = Unsafe.getUnsafe().getInt(pTransitionIndex + 4);
        final long index = pTransitionIndex + 8;
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

                    columnNameIndexMap.put(tmp.getName(), copyTo - 1);
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
                TableColumnMetadata m = newInstance(i, columnCount);
                moveMetadata(i, m);
                columnNameIndexMap.put(m.getName(), i);
            }
        }

        // ended up with fewer columns than before?
        // good idea to resize array and may be drop timestamp index
        if (columnCount < this.columnCount) {
            if (timestampIndex >= columnCount && timestampRequired) {
                timestampIndex = -1;
            }
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
    }

    @Override
    public void close() {
        Misc.free(metaMem);
        Misc.free(path);
    }

    public long createTransitionIndex() {
        if (transitionMeta == null) {
            transitionMeta = new ReadOnlyMemory();
        }

        transitionMeta.of(ff, path, ff.getPageSize(), ff.length(path));
        try (ReadOnlyMemory metaMem = transitionMeta) {

            tmpValidationMap.clear();
            TableUtils.validate(ff, metaMem, tmpValidationMap);

            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            int n = Math.max(this.columnCount, columnCount);
            final long pTransitionIndex;
            final int size = n * 16;

            long index = pTransitionIndex = Unsafe.calloc(size);
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
                if (oldPosition > -1
                        && TableUtils.getColumnType(metaMem, i) == TableUtils.getColumnType(this.metaMem, oldPosition)
                        && TableUtils.isColumnIndexed(metaMem, i) == TableUtils.isColumnIndexed(this.metaMem, oldPosition)) {
                    Unsafe.getUnsafe().putInt(index + i * 8, oldPosition + 1);
                    Unsafe.getUnsafe().putInt(index + oldPosition * 8 + 4, i + 1);
                } else {
                    Unsafe.getUnsafe().putLong(index + i * 8, 0);
                }
            }
            return pTransitionIndex;
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    public int getPartitionBy() {
        return metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
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
        return new TableColumnMetadata(
                name.toString(),
                TableUtils.getColumnType(metaMem, index),
                TableUtils.isColumnIndexed(metaMem, index),
                TableUtils.getIndexBlockCapacity(metaMem, index)
        );
    }
}
