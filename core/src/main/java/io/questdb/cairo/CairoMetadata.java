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
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;


public class CairoMetadata implements Sinkable {
    public static final CairoMetadata INSTANCE = new CairoMetadata();
    public static final Log LOG = LogFactory.getLog(CairoMetadata.class);
    private final SimpleReadWriteLock lock; // consider StampedLock
    private final CharSequenceObjHashMap<CairoTable> tables;

    public CairoMetadata() {
        this.tables = new CharSequenceObjHashMap<>();
        this.lock = new SimpleReadWriteLock();
    }

    public static void hydrateTable(@NotNull TableToken token, @NotNull CairoConfiguration configuration, @NotNull Path path, @NotNull Log logger, @NotNull ColumnVersionReader columnVersionReader) {
        logger.debugW().$("hydrating metadata [table=").$(token.getTableName()).I$();

        // set up table path
        path.of(configuration.getRoot())
                .concat(token.getDirName())
                .concat(TableUtils.META_FILE_NAME)
                .trimTo(path.size());

        // open metadata
        MemoryCMR metaMem = Vm.getCMRInstance();
        metaMem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.NATIVE_METADATA_READER);
        TableUtils.validateMeta(metaMem, null, ColumnType.VERSION);

        // create table to work with
        CairoTable table = new CairoTable(token);
        table.lock.writeLock().lock();

        table.setLastMetadataVersionUnsafe(Long.MIN_VALUE);

        int metadataVersion = metaMem.getInt(TableUtils.META_OFFSET_METADATA_VERSION);

        // make sure we aren't duplicating work
        try {
            logger.debugW().$("trying to add table [table=").$(token.getTableName()).I$();
            CairoMetadata.INSTANCE.addTable(table);
            logger.debugW().$("added table [table=").$(token.getTableName()).I$();
        } catch (CairoException e) {
            logger.debugW().$("table already present [table=").$(token.getTableName()).I$();
            final CairoTable alreadyHydrated = CairoMetadata.INSTANCE.getTableQuietUnsafe(token.getTableName());
            if (alreadyHydrated != null) {
                long version = alreadyHydrated.getLastMetadataVersionUnsafe();

                if (version == metadataVersion) {
                    logger.debugW().$("already up to date [table=").$(token.getTableName())
                            .$(", version=").$(version).I$();
                    metaMem.close();
                    return;
                }

                logger.debugW().$("updating metadata for existing table [table=").$(token.getTableName()).I$();

                table.lock.writeLock().unlock();
                table = alreadyHydrated;
                table.lock.writeLock().lock();

            } else {
                throw e;
            }
        }

        // get basic metadata
        int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);

        logger.debugW().$("reading columns [table=").$(token.getTableName())
                .$(", count=").$(columnCount)
                .I$();

        table.setLastMetadataVersionUnsafe(metadataVersion);

        logger.debugW().$("set metadata version [table=").$(token.getTableName())
                .$(", version=").$(metadataVersion)
                .I$();

        table.setPartitionByUnsafe(PartitionBy.toString(metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY)));
        table.setMaxUncommittedRowsUnsafe(metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS));
        table.setO3MaxLagUnsafe(metaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG));
        table.setTimestampIndexUnsafe(metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX));

        TableUtils.buildWriterOrderMap(metaMem, table.columnOrderMap, metaMem, columnCount);

        // populate columns
        for (int i = 0, n = table.columnOrderMap.size(); i < n; i += 3) {

            int writerIndex = table.columnOrderMap.get(i);
            if (writerIndex < 0) {
                continue;
            }
            int stableIndex = i / 3;
            CharSequence name = metaMem.getStrA(table.columnOrderMap.get(i + 1));
            int denseSymbolIndex = table.columnOrderMap.get(i + 2);

            assert name != null;
            int columnType = TableUtils.getColumnType(metaMem, writerIndex);

            if (columnType > -1) {
                String columnName = Chars.toString(name);
                CairoColumn column = new CairoColumn();

                logger.debugW().$("hydrating column [table=").$(token.getTableName()).$(", column=").$(columnName).I$();

                // set basic values
                column.setNameUnsafe(columnName);
                column.setPositionUnsafe((int) (table.getColumnCountUnsafe() - 1 < 0 ? 0 : table.getColumnCountUnsafe() - 1));
                column.setTypeUnsafe(columnType);
                column.setIsIndexedUnsafe(TableUtils.isColumnIndexed(metaMem, writerIndex));
                column.setIndexBlockCapacityUnsafe(TableUtils.getIndexBlockCapacity(metaMem, writerIndex));
                column.setIsSymbolTableStaticUnsafe(true);
                column.setIsDedupKeyUnsafe(TableUtils.isColumnDedupKey(metaMem, writerIndex));
                column.setWriterIndexUnsafe(writerIndex);
                column.setDenseSymbolIndexUnsafe(denseSymbolIndex);
                column.setStableIndex(stableIndex);
                column.setIsDesignatedUnsafe(writerIndex == table.getTimestampIndexUnsafe());
                column.setIsSequentialUnsafe(TableUtils.isSequential(metaMem, writerIndex));

                if (ColumnType.isSymbol(columnType)) {

                    logger.debugW().$("hydrating symbol metadata [table=").$(token.getTableName()).$(", column=").$(columnName).I$();

                    // get column version
                    path.trimTo(configuration.getRoot().length())
                            .concat(table.getDirectoryNameUnsafe())
                            .concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                    columnVersionReader.ofRO(configuration.getFilesFacade(),
                            path.$());

                    final long columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(writerIndex);
                    columnVersionReader.close();

                    // use txn to find correct symbol entry
                    final LPSZ offsetFileName = TableUtils.offsetFileName(
                            path.trimTo(configuration.getRoot().length()).concat(table.getDirectoryNameUnsafe())
                            , columnName, columnNameTxn);

                    // initialise symbol map memory
                    MemoryCMR offsetMem = Vm.getCMRInstance();
                    final long offsetMemSize = SymbolMapWriter.keyToOffset(0) + Long.BYTES;
                    offsetMem.of(configuration.getFilesFacade(), offsetFileName, offsetMemSize, offsetMemSize, MemoryTag.NATIVE_METADATA_READER);

                    // get symbol properties
                    column.setSymbolCapacityUnsafe(offsetMem.getInt(SymbolMapWriter.HEADER_CAPACITY));
                    assert column.getSymbolCapacityUnsafe() > 0;

                    column.setSymbolCachedUnsafe(offsetMem.getBool(SymbolMapWriter.HEADER_CACHE_ENABLED));

                    offsetMem.close();
                }

                logger.debugW().$("hydrating column [table=").$(token.getTableName()).$(", column=").$(columnName).I$();

                table.addColumnUnsafe(column);
            }
        }

        table.lock.writeLock().unlock();
        metaMem.close();
    }

    public void addColumn(@NotNull TableToken token,
                          CharSequence columnName,
                          int columnType,
                          int position,
                          int symbolCapacity,
                          boolean symbolCacheFlag,
                          boolean isIndexed,
                          int indexValueBlockCapacity,
                          boolean isSequential,
                          boolean isDedupKey,
                          long metadataVersion
    ) {
        final CairoTable table = getTableQuick(token.getTableName());
        try {
            table.lock.writeLock().lock();

            if (metadataVersion < table.getLastMetadataVersionUnsafe()) {
                throw CairoException.staleTableMetadata(table.getName(),
                        table.getLastMetadataVersionUnsafe(), metadataVersion);
            }

            LOG.debugW().$("adding column [table=").$(table.getNameUnsafe()).$(", column=").$(columnName).I$();

            // ensure column is not present
            CairoColumn existingColumn = table.getColumnQuietUnsafe(columnName);
            CairoColumn newColumn;

            if (existingColumn != null) {
                assert Chars.equals(existingColumn.getNameUnsafe(), columnName);
                LOG.debugW().$("column already present, updating instead [table=").$(table.getNameUnsafe()).$(", column=").$(columnName).I$();
                newColumn = existingColumn;
            } else {
                newColumn = new CairoColumn();
            }

            newColumn.setNameUnsafe(columnName.toString());
            newColumn.setTypeUnsafe(columnType);
            newColumn.setPositionUnsafe(position);
            newColumn.setSymbolCapacityUnsafe(symbolCapacity);
            newColumn.setSymbolCachedUnsafe(symbolCacheFlag);
            newColumn.setIsIndexedUnsafe(isIndexed);
            newColumn.setIndexBlockCapacityUnsafe(indexValueBlockCapacity);
            newColumn.setIsSequentialUnsafe(isSequential);
            newColumn.setIsDedupKeyUnsafe(isDedupKey);

            if (newColumn != existingColumn) {
                table.addColumnUnsafe(newColumn);
            }

            table.token = token;
        } finally {
            table.setLastMetadataVersionUnsafe(metadataVersion);
            table.lock.writeLock().unlock();
        }
    }

    // fails if table already exists
    // assume newtable is locked already
    public void addTable(@NotNull CairoTable newTable) {
        final String tableName = newTable.getNameUnsafe();
        final CairoTable existingTable = getTableQuiet(tableName);
        if (existingTable != null) {
            throw CairoException.nonCritical().put("already exists in metadata [table=").put(tableName).put("]");
        }
        lock.writeLock().lock();
        tables.put(tableName, newTable);
        lock.writeLock().unlock();
    }

    public void clear() {
        lock.writeLock().lock();
        for (int i = 0, n = tables.size(); i < n; i++) {
            final CairoTable table = tables.get(tables.keys().getQuick(i));
            table.clear();
        }
        tables.clear();
        lock.writeLock().unlock();
    }

    public CairoTable getTableQuick(@NotNull CharSequence tableName) {
        final CairoTable table = getTableQuiet(tableName);
        if (table == null) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        return table;
    }

    public CairoTable getTableQuiet(@NotNull CharSequence tableName) {
        lock.readLock().lock();
        final CairoTable tbl = tables.get(tableName);
        lock.readLock().unlock();
        return tbl;
    }

    public CairoTable getTableQuietUnsafe(@NotNull CharSequence tableName) {
        return tables.get(tableName);
    }

    public int getTablesCountUnsafe() {
        return tables.size();
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put("CairoMetadata [");
        sink.put("tableCount=").put(getTablesCountUnsafe()).put("]");
        sink.put('\n');
        for (int i = 0, n = tables.size(); i < n; i++) {
            sink.put('\t');
            tables.get(tables.keys().getQuick(i)).toSink(sink);
            sink.put('\n');
        }
    }

    public String toString0() {
        StringSink sink = Misc.getThreadLocalSink();
        this.toSink(sink);
        return sink.toString();
    }
}



