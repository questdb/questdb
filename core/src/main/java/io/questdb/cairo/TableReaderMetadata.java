/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
    private final Path path;
    private final FilesFacade ff;
    private final LowerCaseCharSequenceIntHashMap tmpValidationMap = new LowerCaseCharSequenceIntHashMap();
    private MemoryMR metaMem;
    private int partitionBy;
    private int version;
    private int tableId;
    private int maxUncommittedRows;
    private long commitLag;
    private long structureVersion;

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

    public void applyTransitionIndex(long pTransitionIndex) {
        //  swap meta and transitionMeta
        MemoryMR temp = this.metaMem;
        this.metaMem = this.transitionMeta;
        transitionMeta = temp;
        transitionMeta.close(); // Memory is safe to double close, do not assign null to transitionMeta

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
        this.timestampIndex     = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.structureVersion   = metaMem.getLong(TableUtils.META_OFFSET_STRUCTURE_VERSION);
        this.maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
        this.commitLag          = metaMem.getLong(TableUtils.META_OFFSET_COMMIT_LAG);
    }

    public void cloneTo(MemoryMA mem) {
        long len = ff.length(metaMem.getFd());
        for (long p = 0; p < len; p++) {
            mem.putByte(metaMem.getByte(p));
        }
    }

    public void dumpTo(MemoryMA mem) {
        mem.jumpTo(0);
        mem.putInt(columnCount);
        mem.putInt(partitionBy);
        mem.putInt(timestampIndex);
        mem.putInt(version);
        mem.putInt(tableId);
        mem.putInt(maxUncommittedRows);
        mem.putLong(commitLag);
        mem.putLong(structureVersion);

        mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);
        for (int i = 0; i < columnCount; i++) {
            TableColumnMetadata col = columnMetadata.getQuick(i);
            mem.putInt(col.getType());
            long flags = 0;
            if (col.isIndexed()) {
                flags |= TableUtils.META_FLAG_BIT_INDEXED;
            }
            // ignore META_FLAG_BIT_SEQUENTIAL flag for now

            mem.putLong(flags);
            mem.putInt(col.getIndexValueBlockCapacity());
            mem.putLong(col.getHash());
            mem.skip(8);
        }

        for (int i = 0; i < columnCount; i++) {
            TableColumnMetadata col = columnMetadata.getQuick(i);
            mem.putStr(col.getName());
        }
    }

    @Override
    public void close() {
        // TableReaderMetadata is re-usable after close, don't assign nulls
        Misc.free(metaMem);
        Misc.free(path);
        Misc.free(transitionMeta);
    }

    public long createTransitionIndex(long txnStructureVersion) {
        if (transitionMeta == null) {
            transitionMeta = Vm.getMRInstance();
        }

        transitionMeta.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
        if (transitionMeta.size() >= TableUtils.META_OFFSET_STRUCTURE_VERSION + 8
                && txnStructureVersion != transitionMeta.getLong(TableUtils.META_OFFSET_STRUCTURE_VERSION)) {
            // No match
            return -1;
        }

        tmpValidationMap.clear();
        TableUtils.validate(transitionMeta, tmpValidationMap, ColumnType.VERSION);
        return TableUtils.createTransitionIndex(transitionMeta, this.metaMem, this.columnCount, this.columnNameIndexMap);
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    public long getCommitLag() {
        return commitLag;
    }

    public int getId() {
        return tableId;
    }

    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public long getStructureVersion() {
        return structureVersion;
    }

    public int getVersion() {
        return version;
    }

    public TableReaderMetadata of(Path path, int expectedVersion) {
        this.path.of(path).$();
        try {
            this.metaMem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
            this.columnNameIndexMap.clear();
            TableUtils.validate(metaMem, this.columnNameIndexMap, expectedVersion);

            this.columnCount        = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            this.partitionBy        = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
            this.timestampIndex     = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            this.version            = metaMem.getInt(TableUtils.META_OFFSET_VERSION);
            this.tableId            = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
            this.maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
            this.commitLag          = metaMem.getLong(TableUtils.META_OFFSET_COMMIT_LAG);
            this.structureVersion   = metaMem.getLong(TableUtils.META_OFFSET_STRUCTURE_VERSION);

            this.columnMetadata.clear();
            long offset = TableUtils.getColumnNameOffset(columnCount);

            // don't create strings in this loop, we already have them in columnNameIndexMap
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStr(offset);
                assert name != null;
                columnMetadata.add(
                        new TableColumnMetadata(
                                Chars.toString(name),
                                TableUtils.getColumnHash(metaMem, i),
                                TableUtils.getColumnType(metaMem, i),
                                TableUtils.isColumnIndexed(metaMem, i),
                                TableUtils.getIndexBlockCapacity(metaMem, i),
                                true,
                                null)
                );
                offset += Vm.getStorageLength(name);
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
        return this;
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
                TableUtils.getColumnHash(metaMem, index),
                TableUtils.getColumnType(metaMem, index),
                TableUtils.isColumnIndexed(metaMem, index),
                TableUtils.getIndexBlockCapacity(metaMem, index),
                true,
                null);
    }
}
