/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

public class TableReaderMetadata extends AbstractRecordMetadata implements TableMetadata, Mutable {
    protected final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final TableMetadataFileBlock.MetadataHolder holder = new TableMetadataFileBlock.MetadataHolder();
    private final TableMetadataFileBlock.MetadataHolder transitionHolder = new TableMetadataFileBlock.MetadataHolder();
    private BlockFileReader blockFileReader;
    private MemoryCMR dumpMem; // reusable memory for dumpTo
    private boolean isSoftLink;
    private int maxUncommittedRows;
    private long metadataVersion;
    private long o3MaxLag;
    private int partitionBy;
    private Path path;
    private int plen;
    private int tableId;
    private TableToken tableToken;
    private TableReaderMetadataTransitionIndex transitionIndex;
    private int ttlHoursOrMonths;
    private boolean walEnabled;
    private int writerColumnCount;

    public TableReaderMetadata(CairoConfiguration configuration, TableToken tableToken) {
        try {
            this.configuration = configuration;
            this.ff = configuration.getFilesFacade();
            this.tableToken = tableToken;
            this.path = new Path();
            this.path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            this.plen = path.size();
            this.isSoftLink = Files.isSoftLink(path.$());
            this.blockFileReader = new BlockFileReader(configuration);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    // This constructor used to read random metadata files.
    public TableReaderMetadata(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.tableToken = null;
        this.blockFileReader = new BlockFileReader(configuration);
    }

    public TableReaderMetadataTransitionIndex applyTransition() {
        // Apply the transition from transitionHolder to current metadata
        return applyTransition0(transitionHolder, columnCount);
    }

    public TableReaderMetadataTransitionIndex applyTransitionFrom(TableReaderMetadata srcMeta) {
        // Copy metadata from srcMeta's holder
        copyHolderFrom(srcMeta.holder);
        return applyTransition0(holder, columnCount);
    }

    @Override
    public void clear() {
        super.clear();
        holder.clear();
        transitionHolder.clear();
        partitionBy = 0;
        walEnabled = false;
        metadataVersion = 0;
        tableId = 0;
        maxUncommittedRows = 0;
        o3MaxLag = 0;
        ttlHoursOrMonths = 0;
        writerColumnCount = 0;
    }

    @Override
    public void close() {
        blockFileReader = Misc.free(blockFileReader);
        dumpMem = Misc.free(dumpMem);
        path = Misc.free(path);
    }

    /**
     * Copies column metadata to a list for external use.
     *
     * @return list of column metadata
     */
    public ObjList<TableColumnMetadata> copyColumns() {
        ObjList<TableColumnMetadata> result = new ObjList<>(holder.columns.size());
        result.addAll(holder.columns);
        return result;
    }

    /**
     * Dumps the metadata file content to the given memory for checkpoint/backup purposes.
     * Reconstructs the metadata file path and copies raw bytes via memory mapping.
     *
     * @param mem the memory to dump to
     */
    public void dumpTo(MemoryMA mem) {
        // Reconstruct metadata file path
        int savedLen = path.size();
        try {
            path.trimTo(plen).concat(TableUtils.META_FILE_NAME).$();

            // Use memory mapping for efficient copying (like original implementation)
            if (dumpMem == null) {
                dumpMem = Vm.getCMRInstance();
            }
            dumpMem.smallFile(ff, path.$(), MemoryTag.NATIVE_METADATA_READER);
            long len = dumpMem.size();
            for (long p = 0; p < len; p++) {
                mem.putByte(dumpMem.getByte(p));
            }
        } finally {
            path.trimTo(savedLen);
            Misc.free(dumpMem);
            dumpMem = null;
        }
    }

    public int getDenseSymbolIndex(int columnIndex) {
        return ((TableReaderMetadataColumn) columnMetadata.getQuick(columnIndex)).getDenseSymbolIndex();
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return getColumnMetadata(columnIndex).getIndexValueBlockCapacity();
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
    public boolean getSymbolCacheFlag(int columnIndex) {
        return getColumnMetadata(columnIndex).isSymbolIndexFlag();
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return getColumnMetadata(columnIndex).getSymbolCapacity();
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public CharSequence getTableName() {
        return tableToken.getTableName();
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public int getTtlHoursOrMonths() {
        return ttlHoursOrMonths;
    }

    public int getWriterColumnCount() {
        return writerColumnCount;
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return getColumnMetadata(columnIndex).isSymbolIndexFlag();
    }

    public boolean isSoftLink() {
        return isSoftLink;
    }

    @Override
    public boolean isWalEnabled() {
        return walEnabled;
    }

    public void loadMetadata(LPSZ path) {
        try {
            blockFileReader.of(path);
            Path pathWrapper = Path.getThreadLocal(path);
            TableMetadataFileBlock.read(blockFileReader, holder, pathWrapper);
            readFromHolder(holder);
        } catch (Throwable e) {
            clear();
            throw e;
        }
    }

    public void loadMetadata() {
        final long spinLockTimeout = configuration.getSpinLockTimeout();
        final MillisecondClock millisecondClock = configuration.getMillisecondClock();
        long deadline = configuration.getMillisecondClock().getTicks() + spinLockTimeout;
        path.trimTo(plen).concat(TableUtils.META_FILE_NAME);
        boolean existenceChecked = false;
        while (true) {
            try {
                loadMetadata(path.$());
                return;
            } catch (CairoException ex) {
                if (!existenceChecked) {
                    path.trimTo(plen).slash();
                    if (!ff.exists(path.$())) {
                        throw CairoException.tableDoesNotExist(tableToken.getTableName());
                    }
                    path.trimTo(plen).concat(TableUtils.META_FILE_NAME).$();
                }
                existenceChecked = true;
                TableUtils.handleMetadataLoadException(tableToken, deadline, ex, millisecondClock, spinLockTimeout);
            }
        }
    }

    public void loadFrom(TableReaderMetadata srcMeta) {
        assert tableToken.equals(srcMeta.tableToken);
        // Copy src meta holder.
        copyHolderFrom(srcMeta.holder);
        // Now, read it.
        try {
            readFromHolder(holder);
        } catch (Throwable e) {
            clear();
            throw e;
        }
    }

    public boolean prepareTransition(long txnMetadataVersion) {
        path.trimTo(plen).concat(TableUtils.META_FILE_NAME);
        blockFileReader.of(path.$());
        TableMetadataFileBlock.read(blockFileReader, transitionHolder, path);

        // Check if version matches
        if (txnMetadataVersion != transitionHolder.metadataVersion) {
            // No match
            return false;
        }
        return true;
    }

    /**
     * Reads metadata fields from a MetadataHolder into this instance.
     */
    private void readFromHolder(TableMetadataFileBlock.MetadataHolder h) {
        this.writerColumnCount = h.columnCount;
        int timestampWriterIndex = h.timestampIndex;
        this.partitionBy = h.partitionBy;
        this.tableId = h.tableId;
        this.maxUncommittedRows = h.maxUncommittedRows;
        this.o3MaxLag = h.o3MaxLag;
        this.metadataVersion = h.metadataVersion;
        this.walEnabled = h.walEnabled;
        this.ttlHoursOrMonths = h.ttlHoursOrMonths;
        this.columnMetadata.clear();
        this.timestampIndex = -1;
        this.columnNameIndexMap.clear();

        int denseSymbolIndex = 0;
        for (int i = 0, n = h.columns.size(); i < n; i++) {
            TableColumnMetadata col = h.columns.getQuick(i);
            int columnType = col.getColumnType();
            int writerIndex = col.getWriterIndex();

            if (columnType > -1) {
                String colName = col.getColumnName();
                int symbolIndex = ColumnType.isSymbol(columnType) ? denseSymbolIndex++ : -1;
                columnMetadata.add(
                        new TableReaderMetadataColumn(
                                colName,
                                columnType,
                                col.isSymbolIndexFlag(),
                                col.getIndexValueBlockCapacity(),
                                true,
                                null,
                                writerIndex,
                                col.isDedupKeyFlag(),
                                symbolIndex,
                                i, // stableIndex
                                col.isSymbolCacheFlag(),
                                col.getSymbolCapacity()
                        )
                );
                int denseIndex = columnMetadata.size() - 1;
                if (!columnNameIndexMap.put(colName, denseIndex)) {
                    throw CairoException.critical(CairoException.METADATA_VALIDATION)
                            .put("Duplicate column [name=").put(colName).put("] at ").put(i);
                }
                if (writerIndex == timestampWriterIndex) {
                    this.timestampIndex = denseIndex;
                }
            }
        }
        this.columnCount = columnMetadata.size();
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    private TableReaderMetadataTransitionIndex applyTransition0(TableMetadataFileBlock.MetadataHolder newHolder, int existingColumnCount) {
        columnNameIndexMap.clear();

        int newColumnCount = newHolder.columnCount;
        this.writerColumnCount = newColumnCount;
        assert newColumnCount >= existingColumnCount;
        columnMetadata.setPos(newColumnCount);
        int timestampWriterIndex = newHolder.timestampIndex;
        this.tableId = newHolder.tableId;
        this.metadataVersion = newHolder.metadataVersion;
        this.maxUncommittedRows = newHolder.maxUncommittedRows;
        this.o3MaxLag = newHolder.o3MaxLag;
        this.walEnabled = newHolder.walEnabled;
        this.ttlHoursOrMonths = newHolder.ttlHoursOrMonths;

        if (transitionIndex == null) {
            transitionIndex = new TableReaderMetadataTransitionIndex();
        } else {
            transitionIndex.clear();
        }

        int shiftLeft = 0, existingIndex = 0;
        int denseSymbolIndex = 0;
        ObjList<TableColumnMetadata> newColumns = newHolder.columns;

        for (int i = 0, n = newColumns.size(); i < n; i++) {
            TableColumnMetadata newCol = newColumns.getQuick(i);
            int writerIndex = newCol.getWriterIndex();
            int columnType = newCol.getColumnType();
            String name = newCol.getColumnName();
            boolean isIndexed = newCol.isSymbolIndexFlag();
            boolean isDedupKey = newCol.isDedupKeyFlag();
            int indexBlockCapacity = newCol.getIndexValueBlockCapacity();
            boolean symbolIsCached = newCol.isSymbolCacheFlag();
            int symbolCapacity = newCol.getSymbolCapacity();
            int symbolIndex = ColumnType.isSymbol(columnType) ? denseSymbolIndex++ : -1;

            TableReaderMetadataColumn existing = null;

            if (existingIndex < existingColumnCount) {
                existing = (TableReaderMetadataColumn) columnMetadata.getQuick(existingIndex);
                int existingStableIndex = existing.getStableIndex();
                if (existingStableIndex > i && columnType < 0) {
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
            if (columnType < 0) {
                shiftLeft++; // Deleted in new
                if (existing != null) {
                    transitionIndex.markDeleted(existingIndex);
                }
            } else {
                // existing column
                boolean rename = existing != null && !Chars.equals(existing.getColumnName(), name);
                String newName = rename || existing == null ? name : existing.getColumnName();

                if (rename
                        || existing == null
                        || existing.getWriterIndex() != writerIndex
                        || existing.isSymbolIndexFlag() != isIndexed
                        || existing.getIndexValueBlockCapacity() != indexBlockCapacity
                        || existing.isDedupKeyFlag() != isDedupKey
                        || existing.getDenseSymbolIndex() != symbolIndex
                        || existing.getStableIndex() != i
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
                                    symbolIndex,
                                    i, // stableIndex
                                    symbolIsCached,
                                    symbolCapacity
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
                columnNameIndexMap.put(newName, outIndex);
                if (timestampWriterIndex == writerIndex) {
                    this.timestampIndex = outIndex;
                }
            }
            existingIndex++;
        }

        columnMetadata.setPos(existingIndex - shiftLeft);
        this.columnCount = columnMetadata.size();
        if (timestampWriterIndex < 0) {
            this.timestampIndex = timestampWriterIndex;
        }

        return transitionIndex;
    }

    private void copyHolderFrom(TableMetadataFileBlock.MetadataHolder src) {
        holder.tableId = src.tableId;
        holder.partitionBy = src.partitionBy;
        holder.timestampIndex = src.timestampIndex;
        holder.columnCount = src.columnCount;
        holder.metadataVersion = src.metadataVersion;
        holder.walEnabled = src.walEnabled;
        holder.maxUncommittedRows = src.maxUncommittedRows;
        holder.o3MaxLag = src.o3MaxLag;
        holder.ttlHoursOrMonths = src.ttlHoursOrMonths;
        holder.columns.clear();
        holder.columns.addAll(src.columns);
        holder.columnNameIndexMap.clear();
        for (int i = 0, n = src.columns.size(); i < n; i++) {
            holder.columnNameIndexMap.put(src.columns.getQuick(i).getColumnName(), i);
        }
    }
}
