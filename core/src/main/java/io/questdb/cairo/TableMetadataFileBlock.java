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

import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.NotNull;

/**
 * Handles reading and writing table metadata using the BlockFile format.
 * <p>
 * Block Types:
 * <ul>
 *   <li>Type 0 (CORE): Table-level metadata (ID, partition mode, timestamp index, etc.)</li>
 *   <li>Type 1 (COLUMNS): Column definitions (types, flags, names)</li>
 *   <li>Type 2 (SETTINGS): Additional settings (TTL, o3MaxLag, maxUncommittedRows)</li>
 * </ul>
 * <p>
 * The BlockFile format provides atomic updates through dual-region versioning,
 * eliminating race conditions during metadata modifications.
 */
public class TableMetadataFileBlock {
    public static final int BLOCK_TYPE_CORE = 0;
    public static final int BLOCK_TYPE_COLUMNS = 1;
    public static final int BLOCK_TYPE_SETTINGS = 2;

    // Column flags (same as TableUtils)
    public static final int FLAG_INDEXED = 1;
    public static final int FLAG_SYMBOL_CACHE = 1 << 2;
    public static final int FLAG_DEDUP_KEY = 1 << 3;

    private static final Log LOG = LogFactory.getLog(TableMetadataFileBlock.class);

    /**
     * Writes table metadata to a BlockFileWriter.
     *
     * @param writer          the BlockFileWriter to write to (must already be opened)
     * @param formatVersion   metadata format version (ColumnType.VERSION)
     * @param tableId         unique table identifier
     * @param partitionBy     partition mode (e.g., PartitionBy.DAY)
     * @param timestampIndex  index of the designated timestamp column (-1 if none)
     * @param metadataVersion metadata modification counter
     * @param walEnabled      whether WAL is enabled for this table
     * @param maxUncommittedRows maximum rows before auto-commit
     * @param o3MaxLag        out-of-order maximum lag in microseconds
     * @param ttlHoursOrMonths TTL setting (positive=hours, negative=months)
     * @param columns         list of column metadata
     */
    public static void write(
            @NotNull BlockFileWriter writer,
            int formatVersion,
            int tableId,
            int partitionBy,
            int timestampIndex,
            long metadataVersion,
            boolean walEnabled,
            int maxUncommittedRows,
            long o3MaxLag,
            int ttlHoursOrMonths,
            @NotNull ObjList<TableColumnMetadata> columns
    ) {
        // Block 0: CORE metadata
        AppendableBlock block = writer.append();
        writeCoreBlock(block, formatVersion, tableId, partitionBy, timestampIndex, columns.size(), metadataVersion, walEnabled);
        block.commit(BLOCK_TYPE_CORE);

        // Block 1: COLUMNS
        block = writer.append();
        writeColumnsBlock(block, columns);
        block.commit(BLOCK_TYPE_COLUMNS);

        // Block 2: SETTINGS
        block = writer.append();
        writeSettingsBlock(block, maxUncommittedRows, o3MaxLag, ttlHoursOrMonths);
        block.commit(BLOCK_TYPE_SETTINGS);

        writer.commit();
    }

    /**
     * Reads table metadata from a BlockFileReader into the provided holder.
     *
     * @param reader the BlockFileReader to read from (must already be opened with of())
     * @param holder the holder to populate with metadata
     * @param path   path for error messages
     */
    public static void read(
            @NotNull BlockFileReader reader,
            @NotNull MetadataHolder holder,
            @NotNull LPSZ path
    ) {
        holder.clear();

        boolean coreFound = false;
        boolean columnsFound = false;

        BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            ReadableBlock block = cursor.next();
            switch (block.type()) {
                case BLOCK_TYPE_CORE:
                    readCoreBlock(block, holder);
                    coreFound = true;
                    break;
                case BLOCK_TYPE_COLUMNS:
                    readColumnsBlock(block, holder);
                    columnsFound = true;
                    break;
                case BLOCK_TYPE_SETTINGS:
                    readSettingsBlock(block, holder);
                    break;
                default:
                    // Unknown block type - skip for forward compatibility
                    LOG.info().$("skipping unknown metadata block type [type=").$(block.type())
                            .$(", path=").$(path).I$();
            }
        }

        if (!coreFound) {
            throw CairoException.critical(CairoException.METADATA_VALIDATION)
                    .put("metadata core block not found [path=").put(path).put(']');
        }
        if (!columnsFound) {
            throw CairoException.critical(CairoException.METADATA_VALIDATION)
                    .put("metadata columns block not found [path=").put(path).put(']');
        }
    }

    private static void writeCoreBlock(
            AppendableBlock block,
            int formatVersion,
            int tableId,
            int partitionBy,
            int timestampIndex,
            int columnCount,
            long metadataVersion,
            boolean walEnabled
    ) {
        block.putInt(formatVersion);
        block.putInt(tableId);
        block.putInt(partitionBy);
        block.putInt(timestampIndex);
        block.putInt(columnCount);
        block.putLong(metadataVersion);
        block.putBool(walEnabled);
    }

    private static void readCoreBlock(ReadableBlock block, MetadataHolder holder) {
        long offset = 0;
        holder.formatVersion = block.getInt(offset);
        offset += Integer.BYTES;
        holder.tableId = block.getInt(offset);
        offset += Integer.BYTES;
        holder.partitionBy = block.getInt(offset);
        offset += Integer.BYTES;
        holder.timestampIndex = block.getInt(offset);
        offset += Integer.BYTES;
        holder.columnCount = block.getInt(offset);
        offset += Integer.BYTES;
        holder.metadataVersion = block.getLong(offset);
        offset += Long.BYTES;
        holder.walEnabled = block.getBool(offset);
    }

    private static void writeColumnsBlock(AppendableBlock block, ObjList<TableColumnMetadata> columns) {
        int columnCount = columns.size();
        block.putInt(columnCount);

        for (int i = 0; i < columnCount; i++) {
            TableColumnMetadata col = columns.getQuick(i);

            block.putInt(col.getColumnType());

            int flags = 0;
            if (col.isSymbolIndexFlag()) {
                flags |= FLAG_INDEXED;
            }
            if (col.isSymbolCacheFlag()) {
                flags |= FLAG_SYMBOL_CACHE;
            }
            if (col.isDedupKeyFlag()) {
                flags |= FLAG_DEDUP_KEY;
            }
            block.putInt(flags);

            block.putInt(col.getIndexValueBlockCapacity());
            block.putInt(col.getSymbolCapacity());
            block.putInt(col.getWriterIndex());
            block.putInt(col.getReplacingIndex());
            block.putStr(col.getColumnName());
        }
    }

    private static void readColumnsBlock(ReadableBlock block, MetadataHolder holder) {
        long offset = 0;
        int columnCount = block.getInt(offset);
        offset += Integer.BYTES;

        holder.columns.clear();
        holder.columnNameIndexMap.clear();

        for (int i = 0; i < columnCount; i++) {
            int type = block.getInt(offset);
            offset += Integer.BYTES;

            int flags = block.getInt(offset);
            offset += Integer.BYTES;

            int indexValueBlockCapacity = block.getInt(offset);
            offset += Integer.BYTES;

            int symbolCapacity = block.getInt(offset);
            offset += Integer.BYTES;

            int writerIndex = block.getInt(offset);
            offset += Integer.BYTES;

            int replacingIndex = block.getInt(offset);
            offset += Integer.BYTES;

            CharSequence name = block.getStr(offset);
            offset += Vm.getStorageLength(name);

            boolean indexed = (flags & FLAG_INDEXED) != 0;
            boolean symbolCached = (flags & FLAG_SYMBOL_CACHE) != 0;
            boolean dedupKey = (flags & FLAG_DEDUP_KEY) != 0;

            String columnName = Chars.toString(name);

            TableColumnMetadata column = new TableColumnMetadata(
                    columnName,
                    type,
                    indexed,
                    indexValueBlockCapacity,
                    true, // symbolTableStatic (always true when reading from file)
                    null, // RecordMetadata (not stored in file)
                    writerIndex,
                    dedupKey,
                    replacingIndex,
                    symbolCached,
                    symbolCapacity
            );

            holder.columns.add(column);
            holder.columnNameIndexMap.put(columnName, i);
        }
    }

    private static void writeSettingsBlock(
            AppendableBlock block,
            int maxUncommittedRows,
            long o3MaxLag,
            int ttlHoursOrMonths
    ) {
        block.putInt(maxUncommittedRows);
        block.putLong(o3MaxLag);
        block.putInt(ttlHoursOrMonths);
    }

    private static void readSettingsBlock(ReadableBlock block, MetadataHolder holder) {
        long offset = 0;
        holder.maxUncommittedRows = block.getInt(offset);
        offset += Integer.BYTES;
        holder.o3MaxLag = block.getLong(offset);
        offset += Long.BYTES;
        holder.ttlHoursOrMonths = block.getInt(offset);
    }

    /**
     * Holder class for metadata read from a BlockFile.
     * This is populated by the read() method.
     */
    public static class MetadataHolder {
        // Core block fields
        public int formatVersion;
        public int tableId;
        public int partitionBy;
        public int timestampIndex;
        public int columnCount;
        public long metadataVersion;
        public boolean walEnabled;

        // Settings block fields
        public int maxUncommittedRows;
        public long o3MaxLag;
        public int ttlHoursOrMonths;

        // Columns block fields
        public final ObjList<TableColumnMetadata> columns = new ObjList<>();
        public final LowerCaseCharSequenceIntHashMap columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();

        public void clear() {
            formatVersion = 0;
            tableId = 0;
            partitionBy = 0;
            timestampIndex = -1;
            columnCount = 0;
            metadataVersion = 0;
            walEnabled = false;
            maxUncommittedRows = 0;
            o3MaxLag = 0;
            ttlHoursOrMonths = 0;
            columns.clear();
            columnNameIndexMap.clear();
        }
    }
}
