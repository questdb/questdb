/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

public class TableReaderMetadata extends AbstractRecordMetadata implements TableMetadata, Mutable {
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final LowerCaseCharSequenceIntHashMap tmpValidationMap = new LowerCaseCharSequenceIntHashMap();
    private boolean isSoftLink;
    private int maxUncommittedRows;
    private MemoryMR metaMem;
    private int metadataVersion;
    private long o3MaxLag;
    private int partitionBy;
    private Path path;
    private int plen;
    private int tableId;
    private TableToken tableToken;
    private TransitionIndex transitionIndex;
    private MemoryMR transitionMeta;
    private boolean walEnabled;

    public TableReaderMetadata(CairoConfiguration configuration, TableToken tableToken) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.tableToken = tableToken;
        this.path = new Path().of(configuration.getRoot()).concat(tableToken.getDirName()).$();
        this.plen = path.size();
        this.isSoftLink = Files.isSoftLink(path);
        this.metaMem = Vm.getMRInstance();
    }

    // constructor used to read random metadata files
    public TableReaderMetadata(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.tableToken = null;
        this.metaMem = Vm.getMRInstance();
    }

    public void applyTransitionIndex() {
        // swap meta and transitionMeta
        MemoryMR temp = this.metaMem;
        this.metaMem = this.transitionMeta;
        transitionMeta = temp;
        transitionMeta.close(); // Memory is safe to double close, do not assign null to transitionMeta
        this.columnNameIndexMap.clear();
        int existingColumnCount = this.columnCount;

        int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        assert columnCount >= existingColumnCount;
        columnMetadata.setPos(columnCount);
        int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
        this.metadataVersion = metaMem.getInt(TableUtils.META_OFFSET_METADATA_VERSION);
        this.maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
        this.o3MaxLag = metaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG);
        this.walEnabled = metaMem.getBool(TableUtils.META_OFFSET_WAL_ENABLED);
        long offset = TableUtils.getColumnNameOffset(columnCount);

        int shiftLeft = 0, existingIndex = 0;
        for (int metaIndex = 0; metaIndex < columnCount; metaIndex++) {
            CharSequence name = metaMem.getStrA(offset);
            offset += Vm.getStorageLength(name);
            assert name != null;
            int columnType = TableUtils.getColumnType(metaMem, metaIndex);
            boolean isIndexed = TableUtils.isColumnIndexed(metaMem, metaIndex);
            boolean isDedupKey = TableUtils.isColumnDedupKey(metaMem, metaIndex);
            int indexBlockCapacity = TableUtils.getIndexBlockCapacity(metaMem, metaIndex);
            TableColumnMetadata existing = null;
            String newName;

            if (existingIndex < existingColumnCount) {
                existing = columnMetadata.getQuick(existingIndex);

                if (existing.getWriterIndex() > metaIndex) {
                    // This column must be deleted so existing dense columns do not contain it
                    assert columnType < 0;
                    continue;
                }
            }
            assert existing == null || existing.getWriterIndex() == metaIndex; // Same column

            if (columnType < 0) {
                // column dropped
                shiftLeft++;
            } else {
                // existing column
                boolean rename = existing != null && !Chars.equals(existing.getName(), name);
                newName = rename || existing == null ? Chars.toString(name) : existing.getName();
                if (rename
                        || existing == null
                        || existing.isIndexed() != isIndexed
                        || existing.getIndexValueBlockCapacity() != indexBlockCapacity
                        || existing.isDedupKey() != isDedupKey
                ) {
                    columnMetadata.setQuick(existingIndex - shiftLeft,
                            new TableColumnMetadata(
                                    newName,
                                    columnType,
                                    isIndexed,
                                    indexBlockCapacity,
                                    true,
                                    null,
                                    metaIndex,
                                    isDedupKey

                            )
                    );
                } else if (shiftLeft > 0) {
                    columnMetadata.setQuick(existingIndex - shiftLeft, existing);
                }
                this.columnNameIndexMap.put(newName, existingIndex - shiftLeft);
                if (timestampIndex == metaIndex) {
                    this.timestampIndex = existingIndex - shiftLeft;
                }
            }
            existingIndex++;
        }
        columnMetadata.setPos(existingIndex - shiftLeft);
        this.columnCount = columnMetadata.size();
        if (timestampIndex < 0) {
            this.timestampIndex = timestampIndex;
        }
    }

    @Override
    public void clear() {
        super.clear();
        Misc.free(metaMem);
        Misc.free(transitionMeta);
    }

    @Override
    public void close() {
        metaMem = Misc.free(metaMem);
        path = Misc.free(path);
        transitionMeta = Misc.free(transitionMeta);
    }

    public TransitionIndex createTransitionIndex(long txnMetadataVersion) {
        if (transitionMeta == null) {
            transitionMeta = Vm.getMRInstance();
        }

        transitionMeta.smallFile(ff, path, MemoryTag.NATIVE_TABLE_READER);
        if (transitionMeta.size() >= TableUtils.META_OFFSET_METADATA_VERSION + 8
                && txnMetadataVersion != transitionMeta.getLong(TableUtils.META_OFFSET_METADATA_VERSION)) {
            // No match
            return null;
        }

        tmpValidationMap.clear();
        TableUtils.validateMeta(transitionMeta, tmpValidationMap, ColumnType.VERSION);
        return createTransitionIndex(transitionMeta);
    }

    public void dumpTo(MemoryMA mem) {
        // Since _meta files are immutable and get updated with a single atomic rename
        // operation replacing the old file with the new one, it's ok to clone the metadata
        // by copying metaMem's contents. Even if _meta file was already replaced, the file
        // should be still kept on disk until inode's ref counter is above zero.
        long len = metaMem.size();
        for (long p = 0; p < len; p++) {
            mem.putByte(metaMem.getByte(p));
        }
    }

    @Override
    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    @Override
    public long getMetadataVersion() {
        return metadataVersion;
    }

    @Override
    public long getO3MaxLag() {
        return o3MaxLag;
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    public boolean isSoftLink() {
        return isSoftLink;
    }

    @Override
    public boolean isWalEnabled() {
        return walEnabled;
    }

    public void load(Path path) {
        try {
            this.metaMem.smallFile(ff, path, MemoryTag.NATIVE_TABLE_READER);
            this.columnNameIndexMap.clear();
            TableUtils.validateMeta(metaMem, this.columnNameIndexMap, ColumnType.VERSION);
            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            this.partitionBy = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
            this.tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
            this.maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
            this.o3MaxLag = metaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG);
            this.metadataVersion = metaMem.getInt(TableUtils.META_OFFSET_METADATA_VERSION);
            this.walEnabled = metaMem.getBool(TableUtils.META_OFFSET_WAL_ENABLED);
            this.columnMetadata.clear();
            long offset = TableUtils.getColumnNameOffset(columnCount);
            this.timestampIndex = -1;

            // don't create strings in this loop, we already have them in columnNameIndexMap
            int denseColumnCount = 0;
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStrA(offset);
                assert name != null;
                int columnType = TableUtils.getColumnType(metaMem, i);
                if (columnType > -1) {
                    TableColumnMetadata columnMeta = new TableColumnMetadata(
                            Chars.toString(name),
                            columnType,
                            TableUtils.isColumnIndexed(metaMem, i),
                            TableUtils.getIndexBlockCapacity(metaMem, i),
                            true,
                            null,
                            i,
                            TableUtils.isColumnDedupKey(metaMem, i)
                    );
                    int columnPlaceIndex = TableUtils.getReplacingColumnIndex(metaMem, i);
                    if (columnPlaceIndex > -1 && columnPlaceIndex < columnMetadata.size()) {
                        assert columnMetadata.get(columnPlaceIndex) == null;
                        columnMetadata.set(columnPlaceIndex, columnMeta);
                    } else {
                        columnMetadata.add(columnMeta);
                    }
                    if (i == timestampIndex) {
                        this.timestampIndex = denseColumnCount;
                    }
                    denseColumnCount++;
                } else {
                    // This column can be replaced by another column, leave a placeholder
                    columnMetadata.add(null);
                }
                offset += Vm.getStorageLength(name);
            }

            if (denseColumnCount != columnMetadata.size()) {
                int denseIndex = 0;
                for (int i = 0; i < columnMetadata.size(); i++) {
                    TableColumnMetadata current = columnMetadata.get(i);
                    if (current != null) {
                        columnMetadata.set(denseIndex++, current);
                    }
                }
                columnMetadata.setPos(denseColumnCount);
            }

            this.columnCount = columnMetadata.size();
        } catch (Throwable e) {
            clear();
            throw e;
        }
    }

    public void load() {
        final long timeout = configuration.getSpinLockTimeout();
        final MillisecondClock millisecondClock = configuration.getMillisecondClock();
        long deadline = configuration.getMillisecondClock().getTicks() + timeout;
        this.path.trimTo(plen).concat(TableUtils.META_FILE_NAME).$();
        boolean existenceChecked = false;
        while (true) {
            try {
                load(path);
                return;
            } catch (CairoException ex) {
                if (!existenceChecked) {
                    path.trimTo(plen).slash$();
                    if (!ff.exists(path)) {
                        throw CairoException.tableDoesNotExist(tableToken.getTableName());
                    }
                    path.trimTo(plen).concat(TableUtils.META_FILE_NAME).$();
                }
                existenceChecked = true;
                TableUtils.handleMetadataLoadException(tableToken.getTableName(), deadline, ex, millisecondClock, timeout);
            }
        }
    }

//    public static long createTransitionIndex(
//            MemoryR masterMeta,
//            AbstractRecordMetadata slaveMeta
//    ) {
//        int slaveColumnCount = slaveMeta.columnCount;
//        int masterColumnCount = masterMeta.getInt(META_OFFSET_COUNT);
//        final long pTransitionIndex;
//        final int size = 8 + masterColumnCount * 8;
//
//        long index = pTransitionIndex = Unsafe.calloc(size, MemoryTag.NATIVE_TABLE_READER);
//        Unsafe.getUnsafe().putInt(index, size);
//        index += 8;
//
//        // index structure is
//        // [action: int, copy from:int]
//
//        // action: if -1 then current column in slave is deleted or renamed, else it's reused
//        // "copy from" >= 0 indicates that column is to be copied from slave position
//        // "copy from" < 0  indicates that column is new and should be taken from updated metadata position
//        // "copy from" == Integer.MIN_VALUE  indicates that column is deleted for good and should not be re-added from any source
//
//        long offset = getColumnNameOffset(masterColumnCount);
//        int slaveIndex = 0;
//        int shiftLeft = 0;
//        for (int masterIndex = 0; masterIndex < masterColumnCount; masterIndex++) {
//            CharSequence name = masterMeta.getStrA(offset);
//            offset += Vm.getStorageLength(name);
//            int masterColumnType = getColumnType(masterMeta, masterIndex);
//
//            if (slaveIndex < slaveColumnCount) {
//                int existingWriterIndex = slaveMeta.getWriterIndex(slaveIndex);
//                if (existingWriterIndex > masterIndex) {
//                    // This column must be deleted so existing dense columns do not contain it
//                    assert masterColumnType < 0;
//                    continue;
//                }
//                assert existingWriterIndex == masterIndex;
//            }
//
//            int outIndex = slaveIndex - shiftLeft;
//            if (masterColumnType < 0) {
//                shiftLeft++; // Deleted in master
//                if (slaveIndex < slaveColumnCount) {
//                    Unsafe.getUnsafe().putInt(index + slaveIndex * 8L, -1);
//                    Unsafe.getUnsafe().putInt(index + slaveIndex * 8L + 4, Integer.MIN_VALUE);
//                }
//            } else {
//                if (
//                        slaveIndex < slaveColumnCount
//                                && isColumnIndexed(masterMeta, masterIndex) == slaveMeta.isColumnIndexed(slaveIndex)
//                                && Chars.equals(name, slaveMeta.getColumnName(slaveIndex))
//                ) {
//                    // reuse
//                    Unsafe.getUnsafe().putInt(index + outIndex * 8L + 4, slaveIndex);
//                    if (slaveIndex > outIndex) {
//                        // mark to do nothing with existing column, this may be overwritten later
//                        Unsafe.getUnsafe().putInt(index + slaveIndex * 8L + 4, Integer.MIN_VALUE);
//                    }
//                } else {
//                    // new
//                    if (slaveIndex < slaveColumnCount) {
//                        // column deleted at slaveIndex
//                        Unsafe.getUnsafe().putInt(index + slaveIndex * 8L, -1);
//                        Unsafe.getUnsafe().putInt(index + slaveIndex * 8L + 4, Integer.MIN_VALUE);
//                    }
//                    Unsafe.getUnsafe().putInt(index + outIndex * 8L + 4, -masterIndex - 1);
//                }
//            }
//            slaveIndex++;
//        }
//        Unsafe.getUnsafe().putInt(pTransitionIndex + 4, slaveIndex - shiftLeft);
//        return pTransitionIndex;
//    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    private TransitionIndex createTransitionIndex(
            MemoryR newMeta
    ) {
        if (transitionIndex == null) {
            transitionIndex = new TransitionIndex();
        } else {
            transitionIndex.clear();
        }

        int oldColumnCount = columnCount;
        int newColumnCount = newMeta.getInt(TableUtils.META_OFFSET_COUNT);

        // index structure is
        // [action: int, copy from:int]

        // action: if -1 then current column in old is deleted or renamed, else it's reused
        // "copy from" >= 0 indicates that column is to be copied from old position
        // "copy from" < 0  indicates that column is new and should be taken from updated metadata position
        // "copy from" == Integer.MIN_VALUE  indicates that column is deleted for good and should not be re-added from any source

        long offset = TableUtils.getColumnNameOffset(newColumnCount);
        int oldIndex = 0;
        int shiftLeft = 0;
        for (int newIndex = 0; newIndex < newColumnCount; newIndex++) {
            CharSequence name = newMeta.getStrA(offset);
            offset += Vm.getStorageLength(name);
            int newColumnType = TableUtils.getColumnType(newMeta, newIndex);
            int newOrderIndex = TableUtils.getReplacingColumnIndex(newMeta, newIndex);

            if (oldIndex < oldColumnCount) {
                int oldWriterIndex = this.getWriterIndex(oldIndex);
                if (oldWriterIndex > newIndex) {
                    // This column must be deleted so existing dense columns do not contain it
                    assert newColumnType < 0;
                    continue;
                }
                assert oldWriterIndex == newIndex;
            }

            int outIndex = oldIndex - shiftLeft;
            if (newColumnType < 0) {
                shiftLeft++; // Deleted in new
                if (oldIndex < oldColumnCount) {
                    transitionIndex.markDeleted(oldIndex);
//                    Unsafe.getUnsafe().putInt(index + oldIndex * 8L, -1);
//                    Unsafe.getUnsafe().putInt(index + oldIndex * 8L + 4, Integer.MIN_VALUE);
                }
            } else {
                if (
                        oldIndex < oldColumnCount
                                && TableUtils.isColumnIndexed(newMeta, newIndex) == this.isColumnIndexed(oldIndex)
                                && Chars.equals(name, this.getColumnName(oldIndex))
                ) {
                    // reuse
                    transitionIndex.markReusedAction(outIndex, oldIndex);
//                    Unsafe.getUnsafe().putInt(index + outIndex * 8L + 4, oldIndex);
                    if (oldIndex > outIndex) {
                        // mark to do nothing with existing column, this may be overwritten later
                        transitionIndex.markReplaced(oldIndex);
//                        Unsafe.getUnsafe().putInt(index + oldIndex * 8L + 4, Integer.MIN_VALUE);
                    }
                } else {
                    // new
                    if (oldIndex < oldColumnCount) {
                        // column deleted at oldIndex
                        transitionIndex.markDeleted(oldIndex);
//                        Unsafe.getUnsafe().putInt(index + oldIndex * 8L, -1);
//                        Unsafe.getUnsafe().putInt(index + oldIndex * 8L + 4, Integer.MIN_VALUE);
                    }
                    transitionIndex.markCopyFrom(outIndex, newIndex);
//                    Unsafe.getUnsafe().putInt(index + outIndex * 8L + 4, -newIndex - 1);
                }
            }
            oldIndex++;
        }
        return transitionIndex;
    }

    public static class TransitionIndex {
        private final IntList actions = new IntList();

        public void clear() {
            actions.setAll(actions.capacity(), 0);
            actions.clear();
        }

        public int getAction(int index) {
            return actions.get(index * 2);
        }

        public int getCopyFromIndex(int index) {
            return actions.get(index * 2 + 1);
        }

        public void markCopyFrom(int index, int newIndex) {
            actions.extendAndSet(index * 2 + 1, -newIndex - 1);
        }

        public void markDeleted(int index) {
            actions.extendAndSet(index * 2, -1);
            actions.extendAndSet(index * 2 + 1, Integer.MIN_VALUE);
            //                    Unsafe.getUnsafe().putInt(index + oldIndex * 8L, -1);
//                    Unsafe.getUnsafe().putInt(index + oldIndex * 8L + 4, Integer.MIN_VALUE);
        }

        public void markReplaced(int index) {
            actions.extendAndSet(index * 2 + 1, Integer.MIN_VALUE);
        }

        public void markReusedAction(int index, int oldIndex) {
            actions.extendAndSet(index * 2 + 1, oldIndex);
        }
    }
}
