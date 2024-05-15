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
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.validationException;

public class TableReaderMetadata extends AbstractRecordMetadata implements TableMetadata, Mutable {
    private final IntList columnOrderMap = new IntList();
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
    private TableReaderMetadataTransitionIndex transitionIndex;
    private MemoryMR transitionMeta;
    private boolean walEnabled;

    public TableReaderMetadata(CairoConfiguration configuration, TableToken tableToken) {
        try {
            this.configuration = configuration;
            this.ff = configuration.getFilesFacade();
            this.tableToken = tableToken;
            this.path = new Path();
            this.path.of(configuration.getRoot()).concat(tableToken.getDirName()).$();
            this.plen = path.size();
            this.isSoftLink = Files.isSoftLink(path);
            this.metaMem = Vm.getMRInstance();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    // constructor used to read random metadata files
    public TableReaderMetadata(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.tableToken = null;
        this.metaMem = Vm.getMRInstance();
    }

    public TableReaderMetadataTransitionIndex applyTransition() {
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
        int newColumnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);

        if (transitionIndex == null) {
            transitionIndex = new TableReaderMetadataTransitionIndex();
        } else {
            transitionIndex.clear();
        }

        buildWriterOrderMap(metaMem, newColumnCount);
        for (int i = 0, n = columnOrderMap.size(); i < n; i += 3) {
            int stableIndex = i / 3;
            int writerIndex = columnOrderMap.get(i);
            if (writerIndex < 0) {
                continue;
            }
            CharSequence name = metaMem.getStrA(columnOrderMap.get(i + 1));
            assert name != null;
            int denseSymbolIndex = columnOrderMap.get(i + 2);
            int newColumnType = TableUtils.getColumnType(metaMem, writerIndex);
            int columnType = TableUtils.getColumnType(metaMem, writerIndex);
            boolean isIndexed = TableUtils.isColumnIndexed(metaMem, writerIndex);
            boolean isDedupKey = TableUtils.isColumnDedupKey(metaMem, writerIndex);
            int indexBlockCapacity = TableUtils.getIndexBlockCapacity(metaMem, writerIndex);
            TableReaderMetadataColumn existing = null;
            String newName;

            if (existingIndex < existingColumnCount) {
                existing = (TableReaderMetadataColumn) columnMetadata.getQuick(existingIndex);
                int existingStableIndex = existing.getStableIndex();
                if (existingStableIndex > stableIndex && columnType < 0) {
                    // This column must be deleted so existing dense columns do not contain it
                    continue;
                }
            }

            // index structure is
            // [action: deleted | reused, copy from:int index]
            // "copy from" >= 0 indicates that column is to be copied from old position
            // "copy from" < 0  indicates that column is new and should be taken from updated metadata position
            // "copy from" == Integer.MIN_VALUE  indicates that column is deleted for good and should not be re-added from any source

            int outIndex = existingIndex - shiftLeft;
            if (newColumnType < 0) {
                shiftLeft++; // Deleted in new
                if (existing != null) {
                    transitionIndex.markDeleted(existingIndex);
                }
            } else {
                // existing column
                boolean rename = existing != null && !Chars.equals(existing.getName(), name);
                newName = rename || existing == null ? Chars.toString(name) : existing.getName();

                if (rename
                        || existing == null
                        || existing.getWriterIndex() != writerIndex
                        || existing.isIndexed() != isIndexed
                        || existing.getIndexValueBlockCapacity() != indexBlockCapacity
                        || existing.isDedupKey() != isDedupKey
                        || existing.getDenseSymbolIndex() != denseSymbolIndex
                        || existing.getStableIndex() != stableIndex
                ) {
                    // new
                    columnMetadata.setQuick(
                            outIndex,
                            new TableReaderMetadataColumn(
                                    newName,
                                    columnType,
                                    isIndexed,
                                    indexBlockCapacity,
                                    true,
                                    null,
                                    writerIndex,
                                    isDedupKey,
                                    denseSymbolIndex,
                                    stableIndex
                            )
                    );
                    if (existing != null) {
                        // column deleted at existingIndex
                        transitionIndex.markDeleted(existingIndex);
                    }
                    transitionIndex.markCopyFrom(outIndex, writerIndex);
                } else {
                    // reuse
                    columnMetadata.setQuick(outIndex, existing);
                    transitionIndex.markReusedAction(outIndex, existingIndex);
                    if (existingIndex > outIndex) {
                        // mark to do nothing with existing column, this may be overwritten later
                        transitionIndex.markReplaced(existingIndex);
                    }
                }
                this.columnNameIndexMap.put(newName, outIndex);
                if (timestampIndex == writerIndex) {
                    this.timestampIndex = outIndex;
                }
            }
            existingIndex++;
        }


        columnMetadata.setPos(existingIndex - shiftLeft);
        this.columnCount = columnMetadata.size();
        if (timestampIndex < 0) {
            this.timestampIndex = timestampIndex;
        }

        return transitionIndex;
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
        return ((TableReaderMetadataColumn) columnMetadata.getQuick(columnIndex)).getDenseSymbolIndex();
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
            TableUtils.validateMeta(metaMem, null, ColumnType.VERSION);
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

            buildWriterOrderMap(metaMem, columnCount);
            this.columnNameIndexMap.clear();

            for (int i = 0, n = columnOrderMap.size(); i < n; i += 3) {
                int writerIndex = columnOrderMap.get(i);
                if (writerIndex < 0) {
                    continue;
                }
                int stableIndex = i / 3;
                CharSequence name = metaMem.getStrA(columnOrderMap.get(i + 1));
                int denseSymbolIndex = columnOrderMap.get(i + 2);

                assert name != null;
                int columnType = TableUtils.getColumnType(metaMem, writerIndex);

                if (columnType > -1) {
                    String colName = Chars.toString(name);
                    columnMetadata.add(
                            new TableReaderMetadataColumn(
                                    colName,
                                    columnType,
                                    TableUtils.isColumnIndexed(metaMem, writerIndex),
                                    TableUtils.getIndexBlockCapacity(metaMem, writerIndex),
                                    true,
                                    null,
                                    writerIndex,
                                    TableUtils.isColumnDedupKey(metaMem, writerIndex),
                                    denseSymbolIndex,
                                    stableIndex
                            )
                    );
                    int denseIndex = columnMetadata.size() - 1;
                    if (!columnNameIndexMap.put(colName, denseIndex)) {
                        throw validationException(metaMem).put("Duplicate column [name=").put(name).put("] at ").put(i);
                    }
                    if (writerIndex == timestampIndex) {
                        this.timestampIndex = denseIndex;
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

    public boolean prepareTransition(long txnMetadataVersion) {
        if (transitionMeta == null) {
            transitionMeta = Vm.getMRInstance();
        }

        transitionMeta.smallFile(ff, path, MemoryTag.NATIVE_TABLE_READER);
        if (transitionMeta.size() >= TableUtils.META_OFFSET_METADATA_VERSION + 8
                && txnMetadataVersion != transitionMeta.getLong(TableUtils.META_OFFSET_METADATA_VERSION)) {
            // No match
            return false;
        }

        tmpValidationMap.clear();
        TableUtils.validateMeta(transitionMeta, tmpValidationMap, ColumnType.VERSION);
        return true;
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    private void buildWriterOrderMap(MemoryMR newMeta, int newColumnCount) {
        int nameOffset = (int) TableUtils.getColumnNameOffset(newColumnCount);
        columnOrderMap.clear();

        int denseSymbolIndex = 0;
        for (int i = 0; i < newColumnCount; i++) {
            int strLen = TableUtils.getInt(newMeta, newMeta.size(), nameOffset);
            if (strLen == TableUtils.NULL_LEN) {
                throw validationException(metaMem).put("NULL column name at [").put(i).put(']');
            }
            if (strLen < 1 || strLen > 255) {
                // EXT4 and many others do not allow file name length > 255 bytes
                throw validationException(metaMem).put("String length of ").put(strLen).put(" is invalid at offset ").put(nameOffset);
            }
            int nameLen = (int) Vm.getStorageLength(strLen);
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
}
