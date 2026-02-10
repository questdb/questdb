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
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
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
        TableReaderMetadataTransitionIndex result = applyTransition0(transitionHolder, columnCount);
        // Update holder to reflect current state (for applyTransitionFrom to work correctly)
        copyHolderFrom(transitionHolder);
        return result;
    }

    public TableReaderMetadataTransitionIndex applyTransitionFrom(TableReaderMetadata srcMeta) {
        // Copy metadata from srcMeta's holder (which should be up-to-date after applyTransition)
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
        if (path == null) {
            throw CairoException.critical(CairoException.METADATA_VALIDATION)
                    .put("cannot dump metadata: path not initialized");
        }
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
            // Free memory FIRST to avoid leak if path.trimTo throws
            dumpMem = Misc.free(dumpMem);
            path.trimTo(savedLen);
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

    public void loadMetadata(LPSZ path) {
        try {
            blockFileReader.of(path);
            TableMetadataFileBlock.read(blockFileReader, holder, path);
            // Close reader after reading - we have all data in holder
            blockFileReader.close();
            readFromHolder(holder);
        } catch (Throwable e) {
            clear();
            throw e;
        }
    }

    public void loadMetadata() {
        path.trimTo(plen).concat(TableUtils.META_FILE_NAME);
        try {
            loadMetadata(path.$());
        } catch (CairoException ex) {
            path.trimTo(plen).slash();
            if (!ff.exists(path.$())) {
                throw CairoException.tableDoesNotExist(tableToken.getTableName());
            }
            throw ex;
        }
    }

    public TransitionResult prepareTransition(long txnMetadataVersion) {
        path.trimTo(plen).concat(TableUtils.META_FILE_NAME);
        blockFileReader.of(path.$());

        // Convert logical metadata version to BlockFile version (add 1)
        TransitionResult result = TableMetadataFileBlock.read(blockFileReader, transitionHolder, path.$(), txnMetadataVersion + 1);
        blockFileReader.close();

        return result;
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    private TableReaderMetadataTransitionIndex applyTransition0(TableMetadataFileBlock.MetadataHolder newHolder, int existingColumnCount) {
        int newColumnCount = newHolder.columnCount;
        this.writerColumnCount = newColumnCount;
        int timestampWriterIndex = newHolder.timestampIndex;
        this.tableId = newHolder.tableId;
        // Convert BlockFile version to logical metadata version (subtract 1)
        this.metadataVersion = newHolder.metadataVersion - 1;
        this.maxUncommittedRows = newHolder.maxUncommittedRows;
        this.o3MaxLag = newHolder.o3MaxLag;
        this.walEnabled = newHolder.walEnabled;
        this.ttlHoursOrMonths = newHolder.ttlHoursOrMonths;

        if (transitionIndex == null) {
            transitionIndex = new TableReaderMetadataTransitionIndex();
        } else {
            transitionIndex.clear();
        }

        ObjList<TableColumnMetadata> newColumns = newHolder.columns;
        int n = newColumns.size();

        // Save existing column info for transition tracking
        ObjList<TableColumnMetadata> existingColumns = new ObjList<>(existingColumnCount);
        for (int i = 0; i < existingColumnCount; i++) {
            existingColumns.add(columnMetadata.getQuick(i));
        }

        // Phase 1: Build temporary arrays mapping logical positions to columns
        // using replacingIndex for proper positioning (same as readFromHolder)
        ObjList<TableColumnMetadata> tempColumns = new ObjList<>(n);
        IntList symbolIndices = new IntList(n);
        IntList stableIndices = new IntList(n);

        for (int i = 0; i < n; i++) {
            tempColumns.add(null);
            symbolIndices.add(-1);
            stableIndices.add(-1);
        }

        int denseSymbolIndex = 0;
        for (int i = 0; i < n; i++) {
            TableColumnMetadata newCol = newColumns.getQuick(i);
            int columnType = newCol.getColumnType();

            if (columnType > -1) {
                int replacingIndex = newCol.getReplacingIndex();
                int targetPos;

                if (replacingIndex >= 0 && replacingIndex < n) {
                    targetPos = replacingIndex;
                } else {
                    targetPos = i;
                }

                int symbolIndex = ColumnType.isSymbol(columnType) ? denseSymbolIndex++ : -1;
                tempColumns.setQuick(targetPos, newCol);
                symbolIndices.setQuick(targetPos, symbolIndex);
                stableIndices.setQuick(targetPos, i);
            }
        }

        // Phase 2: Build final column list and transition index
        // Mark all existing columns as potentially deleted first
        for (int i = 0; i < existingColumnCount; i++) {
            transitionIndex.markDeleted(i);
        }

        columnMetadata.clear();
        columnNameIndexMap.clear();
        this.timestampIndex = -1;

        for (int i = 0; i < n; i++) {
            TableColumnMetadata newCol = tempColumns.getQuick(i);
            if (newCol != null) {
                int columnType = newCol.getColumnType();
                int writerIndex = newCol.getWriterIndex();
                String name = newCol.getColumnName();
                boolean isIndexed = newCol.isSymbolIndexFlag();
                boolean isDedupKey = newCol.isDedupKeyFlag();
                int indexBlockCapacity = newCol.getIndexValueBlockCapacity();
                boolean symbolIsCached = newCol.isSymbolCacheFlag();
                int symbolCapacity = newCol.getSymbolCapacity();
                int symbolIndex = symbolIndices.getQuick(i);
                int stableIndex = stableIndices.getQuick(i);

                int outIndex = columnMetadata.size();

                // Check if this column existed before (by name) for transition tracking
                int existingIndex = -1;
                for (int j = 0; j < existingColumnCount; j++) {
                    TableColumnMetadata candidate = existingColumns.getQuick(j);
                    if (candidate != null && Chars.equals(candidate.getColumnName(), name)) {
                        existingIndex = j;
                        break;
                    }
                }

                columnMetadata.add(
                        new TableReaderMetadataColumn(
                                name,
                                columnType,
                                isIndexed,
                                indexBlockCapacity,
                                true,
                                null,
                                writerIndex,
                                isDedupKey,
                                symbolIndex,
                                stableIndex,
                                symbolIsCached,
                                symbolCapacity
                        )
                );

                // Update transition index
                if (existingIndex >= 0) {
                    // Column existed - mark as reused (un-delete it)
                    transitionIndex.markReusedAction(outIndex, existingIndex);
                }
                transitionIndex.markCopyFrom(outIndex, writerIndex);

                columnNameIndexMap.put(name, outIndex);
                if (timestampWriterIndex == writerIndex) {
                    this.timestampIndex = outIndex;
                }
            }
        }

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
        // Convert BlockFile version to logical metadata version (subtract 1)
        this.metadataVersion = h.metadataVersion - 1;
        this.walEnabled = h.walEnabled;
        this.ttlHoursOrMonths = h.ttlHoursOrMonths;
        this.columnMetadata.clear();
        this.timestampIndex = -1;
        this.columnNameIndexMap.clear();

        // Build column list using replacingIndex for positioning, similar to legacy format.
        // Columns with replacingIndex >= 0 "take over" the position of the column they replace.
        // This maintains logical column order after column type changes.
        int n = h.columns.size();

        // Pre-size the list and use a temporary array to track positions
        // Entry format: [col, symbolIndex, stableIndex] for each position
        ObjList<TableColumnMetadata> tempColumns = new ObjList<>(n);
        IntList symbolIndices = new IntList(n);
        IntList stableIndices = new IntList(n);

        // Initialize with nulls
        for (int i = 0; i < n; i++) {
            tempColumns.add(null);
            symbolIndices.add(-1);
            stableIndices.add(-1);
        }

        int denseSymbolIndex = 0;
        for (int i = 0; i < n; i++) {
            TableColumnMetadata col = h.columns.getQuick(i);
            int columnType = col.getColumnType();

            if (columnType > -1) {
                int replacingIndex = col.getReplacingIndex();
                int targetPos;

                if (replacingIndex >= 0 && replacingIndex < n) {
                    // This column replaces another - use the replaced column's position
                    targetPos = replacingIndex;
                } else {
                    // No replacement - use current slot position
                    targetPos = i;
                }

                int symbolIndex = ColumnType.isSymbol(columnType) ? denseSymbolIndex++ : -1;
                tempColumns.setQuick(targetPos, col);
                symbolIndices.setQuick(targetPos, symbolIndex);
                stableIndices.setQuick(targetPos, i);
            }
        }

        // Now build the final column list, skipping null entries (tombstone positions)
        for (int i = 0; i < n; i++) {
            TableColumnMetadata col = tempColumns.getQuick(i);
            if (col != null) {
                int columnType = col.getColumnType();
                int writerIndex = col.getWriterIndex();
                String colName = col.getColumnName();
                int symbolIndex = symbolIndices.getQuick(i);
                int stableIndex = stableIndices.getQuick(i);

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
                                stableIndex,
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

    public enum TransitionResult {
        SUCCESS,          // version matched
        READ_ERROR,       // error reading, retry with same version
        VERSION_MISMATCH, // the version cannot be read
    }
}
