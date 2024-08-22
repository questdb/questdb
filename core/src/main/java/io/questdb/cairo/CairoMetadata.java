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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;


public class CairoMetadata implements Sinkable {
    public static final CairoMetadata INSTANCE = new CairoMetadata();
    public static final Log LOG = LogFactory.getLog(CairoMetadata.class);
    private final ConcurrentHashMap<CairoTable> tables = new ConcurrentHashMap<>();
    ThreadLocal<ColumnVersionReader> tlColumnVersionReader = ThreadLocal.withInitial(ColumnVersionReader::new);
    ThreadLocal<Path> tlPath = ThreadLocal.withInitial(Path::new);

    public CairoMetadata() {
    }

    public void clear() {
        tables.clear();
    }

    public @NotNull CairoTable getTableQuick(@NotNull TableToken tableToken) {
        final CairoTable table = getTableQuiet(tableToken);
        if (table == null) {
            throw CairoException.tableDoesNotExist(tableToken.getTableName());
        }
        return table;
    }

    public CairoTable getTableQuiet(@NotNull TableToken tableToken) {
        return tables.get(tableToken.getDirName());
    }

    public int getTablesCount() {
        return tables.size();
    }

    /**
     * Hydrates table metadata, bypassing TableWriter/Reader.
     *
     * @param token
     * @param configuration
     * @param logger
     * @param path
     * @param columnVersionReader
     * @param blindUpsert         Specifies whether upsert is blind or only if non-null. This is important as TableWriter should take priority over other processes calling this function (async hydration job, queries)
     */
    public void hydrateTable(@NotNull TableToken token, @NotNull CairoConfiguration configuration, @NotNull Log logger, @NotNull Path path, @NotNull ColumnVersionReader columnVersionReader, boolean blindUpsert) {
        logger.debugW().$("hydrating metadata [table=").$(token.getTableName()).I$();

        // set up table path
        path.of(configuration.getRoot())
                .concat(token.getDirName())
                .concat(TableUtils.META_FILE_NAME)
                .trimTo(path.size());

        MemoryCMR metaMem = Vm.getCMRInstance();

        try {
            // open metadata
            metaMem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.NATIVE_METADATA_READER);
            TableUtils.validateMeta(metaMem, null, ColumnType.VERSION);

            // create table to work with
            CairoTable table = new CairoTable(token);
            table.setMetadataVersion(Long.MIN_VALUE);

            int metadataVersion = metaMem.getInt(TableUtils.META_OFFSET_METADATA_VERSION);

            // make sure we aren't duplicating work
            CairoTable potentiallyExistingTable = tables.get(token.getDirName());
            if (potentiallyExistingTable != null && potentiallyExistingTable.getMetadataVersion() > metadataVersion) {
                logger.debugW().$("table in cache with newer version [table=")
                        .$(token.getTableName()).$(", version=").$(potentiallyExistingTable.getMetadataVersion()).I$();
                return;
            }

            // get basic metadata
            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);

            logger.debugW().$("reading columns [table=").$(token.getTableName())
                    .$(", count=").$(columnCount)
                    .I$();

            table.setMetadataVersion(metadataVersion);

            logger.debugW().$("set metadata version [table=").$(token.getTableName())
                    .$(", version=").$(metadataVersion)
                    .I$();

            table.setPartitionBy(metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY));
            table.setMaxUncommittedRows(metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS));
            table.setO3MaxLag(metaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG));
            table.setTimestampIndex(metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX));

            TableUtils.buildWriterOrderMap(metaMem, table.columnOrderMap, metaMem, columnCount);

            // populate columns
            for (int i = 0, n = table.columnOrderMap.size(); i < n; i += 3) {

                int writerIndex = table.columnOrderMap.get(i);
                if (writerIndex < 0) {
                    continue;
                }
                int stableIndex = i / 3;
                CharSequence name = metaMem.getStrA(table.columnOrderMap.get(i + 1));

                assert name != null;
                int columnType = TableUtils.getColumnType(metaMem, writerIndex);

                if (columnType > -1) {
                    String columnName = Chars.toString(name);
                    CairoColumn column = new CairoColumn();

                    logger.debugW().$("hydrating column [table=").$(token.getTableName()).$(", column=").$(columnName).I$();

                    column.setName(columnName);
                    table.upsertColumn(column);

                    column.setPosition((int) (table.getColumnCount() - 1 < 0 ? 0 : table.getColumnCount() - 1));
                    column.setType(columnType);

                    if (column.getType() < 0) {
                        // deleted
                        continue;
                    }

                    column.setIsIndexed(TableUtils.isColumnIndexed(metaMem, writerIndex));
                    column.setIndexBlockCapacity(TableUtils.getIndexBlockCapacity(metaMem, writerIndex));
                    column.setIsSymbolTableStatic(true);
                    column.setIsDedupKey(TableUtils.isColumnDedupKey(metaMem, writerIndex));
                    column.setWriterIndex(writerIndex);
                    column.setStableIndex(stableIndex);
                    column.setIsDesignated(writerIndex == table.getTimestampIndex());
                    column.setIsSequential(TableUtils.isSequential(metaMem, writerIndex));

                    if (column.getIsDedupKey()) {
                        table.setIsDedup(true);
                    }

                    if (ColumnType.isSymbol(columnType)) {
                        logger.debugW().$("hydrating symbol metadata [table=").$(token.getTableName()).$(", column=").$(columnName).I$();

                        // get column version
                        path.trimTo(configuration.getRoot().length())
                                .concat(table.getDirectoryName())
                                .concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                        columnVersionReader.ofRO(configuration.getFilesFacade(),
                                path.$());

                        final long columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(writerIndex);
                        columnVersionReader.close();

                        // use txn to find correct symbol entry
                        final LPSZ offsetFileName = TableUtils.offsetFileName(
                                path.trimTo(configuration.getRoot().length()).concat(table.getDirectoryName())
                                , columnName, columnNameTxn);

                        // initialise symbol map memory
                        MemoryCMR offsetMem = Vm.getCMRInstance();
                        final long offsetMemSize = SymbolMapWriter.keyToOffset(0) + Long.BYTES;
                        offsetMem.of(configuration.getFilesFacade(), offsetFileName, offsetMemSize, offsetMemSize, MemoryTag.NATIVE_METADATA_READER);

                        // get symbol properties
                        column.setSymbolCapacity(offsetMem.getInt(SymbolMapWriter.HEADER_CAPACITY));
                        assert column.getSymbolCapacity() > 0;

                        column.setSymbolCached(offsetMem.getBool(SymbolMapWriter.HEADER_CACHE_ENABLED));

                        offsetMem.close();
                    }
                }
            }

            if (blindUpsert) {
                tables.put(token.getDirName(), table);
            } else {
                tables.putIfAbsent(token.getDirName(), table);
            }

        } finally {
            metaMem.close();
            path.close();
        }
    }

    /**
     * @param token
     * @param configuration
     * @param logger
     * @param blindUpsert
     * @see CairoMetadata#hydrateTable(TableToken, CairoConfiguration, Log, Path, ColumnVersionReader, boolean)
     */
    public void hydrateTable(@NotNull TableToken token, @NotNull CairoConfiguration configuration, @NotNull Log logger, boolean blindUpsert) {
        logger.debugW().$("hydrating table using thread-local path and column version reader [table=").$(token.getTableName()).I$();
        try {
            hydrateTable(token, configuration, logger, tlPath.get(), tlColumnVersionReader.get(), blindUpsert);
        } finally {
            tlPath.get().close();
            tlColumnVersionReader.get().close();
        }
    }

    public void hydrateTable(@NotNull TableWriterMetadata metadata, @NotNull Log logger, boolean blindUpsert) {
        CairoTable table = new CairoTable();

        logger.debugW().$("hydrating metadata [table=").$(metadata.getTableName()).I$();

        // create table to work with
        table.setTableToken(metadata.getTableToken());
        table.setMetadataVersion(metadata.getMetadataVersion());

        int columnCount = metadata.getColumnCount();

        logger.debugW().$("set metadata version [table=").$(table.getName()).$(", version=").$(table.getMetadataVersion()).I$();

        table.setPartitionBy(metadata.getPartitionBy());
        table.setMaxUncommittedRows(metadata.getMaxUncommittedRows());
        table.setO3MaxLag(metadata.getO3MaxLag());
        table.setTimestampIndex(metadata.getTimestampIndex());

        logger.debugW().$("reading columns [table=").$(table.getName()).$(", count=").$(columnCount).I$();

        for (int i = 0; i < columnCount; i++) {
            TableColumnMetadata columnMetadata = metadata.columnMetadata.getQuick(i);
            CairoColumn column = new CairoColumn();

            column.setName(columnMetadata.getName());
            column.setType(columnMetadata.getType());

            if (column.getType() < 0) {
                // deleted
                continue;
            }

            column.setPosition(i);
            column.setIsIndexed(columnMetadata.isIndexed());
            column.setIndexBlockCapacity(columnMetadata.getIndexValueBlockCapacity());
            column.setIsSymbolTableStatic(columnMetadata.isSymbolTableStatic());
            column.setIsDedupKey(columnMetadata.isDedupKey());
            column.setWriterIndex(columnMetadata.getWriterIndex());
            column.setStableIndex(i);
            column.setIsDesignated(table.getTimestampIndex() == column.getWriterIndex());
            column.setIsSequential(metadata.isSequential(i));

            if (column.getIsDedupKey()) {
                table.setIsDedup(true);
            }

            if (ColumnType.isSymbol(column.getType())) {
                column.setSymbolCapacity(metadata.getSymbolCapacity(i));
                column.setSymbolCached(metadata.getSymbolCacheFlag(i));
            }
            table.upsertColumn(column);
        }

        if (blindUpsert) {
            tables.put(table.getDirectoryName(), table);
        } else {
            tables.putIfAbsent(table.getDirectoryName(), table);
        }
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put("CairoMetadata [");
        sink.put("tableCount=").put(getTablesCount()).put("]");
        sink.put('\n');

        for (CairoTable table : tables.values()) {
            sink.put('\t');
            table.toSink(sink);
            sink.put('\n');
        }
    }

    public String toString0() {
        StringSink sink = Misc.getThreadLocalSink();
        this.toSink(sink);
        return sink.toString();
    }
}



