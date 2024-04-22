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
    private final IntList columnOrderMap = new IntList();
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

        int shiftLeft = 0, existingIndex = 0;
        buildWriterOrderMap(metaMem, columnCount);

        for (int i = 0, n = columnOrderMap.size(); i < n; i += 3) {
            int metaIndex = columnOrderMap.get(i);
            if (metaIndex < 0) {
                continue;
            }

            CharSequence name = metaMem.getStrA(columnOrderMap.get(i + 1));
            int denseSymbolIndex = columnOrderMap.get(i + 2);
            assert name != null;
            int columnType = TableUtils.getColumnType(metaMem, metaIndex);
            boolean isIndexed = TableUtils.isColumnIndexed(metaMem, metaIndex);
            boolean isDedupKey = TableUtils.isColumnDedupKey(metaMem, metaIndex);
            int indexBlockCapacity = TableUtils.getIndexBlockCapacity(metaMem, metaIndex);
            TableColumnMetadata existing = null;
            String newName;

            if (existingIndex < existingColumnCount) {
                existing = columnMetadata.getQuick(existingIndex);
                if (existing.getWriterIndex() != metaIndex && columnType < 0) {
                    // This column must be deleted so existing dense columns do not contain it
                    continue;
                }
            }

            if (columnType < 0) {
                // column dropped
                shiftLeft++;
            } else {
                // existing column
                boolean rename = existing != null && !Chars.equals(existing.getName(), name);
                newName = rename || existing == null ? Chars.toString(name) : existing.getName();
                if (rename
                        || existing == null
                        || existing.getWriterIndex() != metaIndex
                        || existing.isIndexed() != isIndexed
                        || existing.getIndexValueBlockCapacity() != indexBlockCapacity
                        || existing.isDedupKey() != isDedupKey
                        || existing.getDenseSymbolIndex() != denseSymbolIndex
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
                                    isDedupKey,
                                    denseSymbolIndex
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

    public int getDenseSymbolIndex(int columnIndex) {
        return columnMetadata.getQuick(columnIndex).getDenseSymbolIndex();
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
            this.timestampIndex = -1;

            // don't create strings in this loop, we already have them in columnNameIndexMap
            buildWriterOrderMap(metaMem, columnCount);
            for (int i = 0, n = columnOrderMap.size(); i < n; i += 3) {
                int writerIndex = columnOrderMap.get(i);
                if (writerIndex < 0) {
                    continue;
                }
                CharSequence name = metaMem.getStrA(columnOrderMap.get(i + 1));
                int denseSymbolIndex = columnOrderMap.get(i + 2);

                assert name != null;
                int columnType = TableUtils.getColumnType(metaMem, writerIndex);

                if (columnType > -1) {
                    TableColumnMetadata columnMeta = new TableColumnMetadata(
                            Chars.toString(name),
                            columnType,
                            TableUtils.isColumnIndexed(metaMem, writerIndex),
                            TableUtils.getIndexBlockCapacity(metaMem, writerIndex),
                            true,
                            null,
                            writerIndex,
                            TableUtils.isColumnDedupKey(metaMem, writerIndex),
                            denseSymbolIndex
                    );
                    int columnPlaceIndex = TableUtils.getReplacingColumnIndex(metaMem, writerIndex);
                    if (columnPlaceIndex > -1 && columnPlaceIndex < columnMetadata.size()) {
                        assert columnMetadata.get(columnPlaceIndex) == null;
                        columnMetadata.set(columnPlaceIndex, columnMeta);
                    } else {
                        columnMetadata.add(columnMeta);
                    }
                    if (writerIndex == timestampIndex) {
                        this.timestampIndex = columnMetadata.size() - 1;
                    }
                }
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

    private void buildWriterOrderMap(MemoryR newMeta, int newColumnCount) {
        int nameOffset = (int) TableUtils.getColumnNameOffset(newColumnCount);
        columnOrderMap.clear();

        int denseSymbolIndex = 0;
        for (int i = 0; i < newColumnCount; i++) {
            int nameLen = (int) Vm.getStorageLength(newMeta.getInt(nameOffset));
            int newOrderIndex = TableUtils.getReplacingColumnIndex(newMeta, i);
            boolean isSymbol = ColumnType.isSymbol(TableUtils.getColumnType(newMeta, i));

            if (newOrderIndex > -1 && newOrderIndex < newColumnCount - 1) {
                // Replace the column index
                columnOrderMap.set(3 * newOrderIndex, i);
                columnOrderMap.set(3 * newOrderIndex + 1, nameOffset);
                columnOrderMap.set(3 * newOrderIndex + 2, isSymbol ? denseSymbolIndex : -1);

                columnOrderMap.add(-newOrderIndex - 1);
                columnOrderMap.add(0);
                columnOrderMap.add(0);

            } else {
                columnOrderMap.add(i);
                columnOrderMap.add(nameOffset);
                columnOrderMap.add(isSymbol ? denseSymbolIndex : -1);
            }
            nameOffset += nameLen;
            if (isSymbol) {
                denseSymbolIndex++;
            }
        }
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

        int oldIndex = 0;
        int shiftLeft = 0;
        buildWriterOrderMap(newMeta, newColumnCount);

        for (int i = 0, n = columnOrderMap.size(); i < n; i += 3) {
            int writerIndex = columnOrderMap.get(i);
            if (writerIndex < 0) {
                continue;
            }
            CharSequence name = newMeta.getStrA(columnOrderMap.get(i + 1));
            int newColumnType = TableUtils.getColumnType(newMeta, writerIndex);

            int oldWriterIndex = -1;
            if (oldIndex < oldColumnCount) {
                oldWriterIndex = this.getWriterIndex(oldIndex);
                if (oldWriterIndex != writerIndex && newColumnType < 0) {
                    // This column must be deleted so existing dense columns do not contain it
                    continue;
                }
            }

            int outIndex = oldIndex - shiftLeft;
            if (newColumnType < 0) {
                shiftLeft++; // Deleted in new
                if (oldIndex < oldColumnCount) {
                    transitionIndex.markDeleted(oldIndex);
                }
            } else {
                if (
                        oldIndex < oldColumnCount
                                && oldWriterIndex == writerIndex
                                && TableUtils.isColumnIndexed(newMeta, writerIndex) == this.isColumnIndexed(oldIndex)
                                && Chars.equals(name, this.getColumnName(oldIndex))
                ) {
                    // reuse
                    transitionIndex.markReusedAction(outIndex, oldIndex);
                    if (oldIndex > outIndex) {
                        // mark to do nothing with existing column, this may be overwritten later
                        transitionIndex.markReplaced(oldIndex);
                    }
                } else {
                    // new
                    if (oldIndex < oldColumnCount) {
                        // column deleted at oldIndex
                        transitionIndex.markDeleted(oldIndex);
                    }
                    transitionIndex.markCopyFrom(outIndex, writerIndex);
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

        public boolean closeColumn(int index) {
            return actions.get(index * 2) == -1;
        }

        public int getCopyFromIndex(int index) {
            return actions.get(index * 2 + 1);
        }

        public boolean replaceWithNew(int index) {
            return actions.get(index * 2 + 1) != Integer.MIN_VALUE && actions.get(index * 2 + 1) < 0;
        }

        private void markCopyFrom(int index, int newIndex) {
            actions.extendAndSet(index * 2 + 1, -newIndex - 1);
        }

        private void markDeleted(int index) {
            actions.extendAndSet(index * 2, -1);
            actions.extendAndSet(index * 2 + 1, Integer.MIN_VALUE);
        }

        private void markReplaced(int index) {
            actions.extendAndSet(index * 2 + 1, Integer.MIN_VALUE);
        }

        private void markReusedAction(int index, int oldIndex) {
            actions.extendAndSet(index * 2 + 1, oldIndex);
        }
    }
}
