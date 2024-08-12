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
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;


public class CairoMetadata {
    public static final CairoMetadata INSTANCE = new CairoMetadata();
    public static final Log LOG = LogFactory.getLog(CairoMetadata.class);
    private final SimpleReadWriteLock lock; // consider StampedLock
    private final CharSequenceObjHashMap<CairoTable> tables;

    public CairoMetadata() {
        this.tables = new CharSequenceObjHashMap<>();
        this.lock = new SimpleReadWriteLock();
    }

    public static void hydrateTable(@NotNull TableToken token, @NotNull CairoConfiguration configuration, @NotNull Path path, @NotNull Log logger, @NotNull ColumnVersionReader columnVersionReader) {
        logger.debugW().$("Hydrating metadata for [table=").$(token.getTableName()).I$();

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
            CairoMetadata.INSTANCE.addTable(table);
            logger.debugW().$("Added table [table=").$(token.getTableName()).I$();
        } catch (CairoException e) {
            final CairoTable alreadyHydrated = CairoMetadata.INSTANCE.getTableQuick(token.getTableName());
            logger.debugW().$("Table already present [table=").$(token.getTableName()).I$();
            if (alreadyHydrated != null) {
                alreadyHydrated.lock.writeLock().lock();
                long version = alreadyHydrated.getLastMetadataVersionUnsafe();

                if (version == metadataVersion) {
                    alreadyHydrated.lock.writeLock().unlock();
                    return;
                }

                logger.debugW().$("Updating metadata for existing table [table=").$(token.getTableName()).I$();

                table.lock.writeLock().unlock();
                table = alreadyHydrated;

            } else {
                throw e;
            }
        }

        // get basic metadata
        int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);

        table.setLastMetadataVersionUnsafe(metadataVersion);
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

                // set basic values
                column.setNameUnsafe(columnName);
                column.setTypeUnsafe(columnType);
                column.setIsIndexedUnsafe(TableUtils.isColumnIndexed(metaMem, writerIndex));
                column.setIndexBlockCapacityUnsafe(TableUtils.getIndexBlockCapacity(metaMem, writerIndex));
                column.setIsSymbolTableStaticUnsafe(true);
                column.setIsDedupKeyUnsafe(TableUtils.isColumnDedupKey(metaMem, writerIndex));
                column.setWriterIndexUnsafe(writerIndex);
                column.setDenseSymbolIndexUnsafe(denseSymbolIndex);
                column.setStableIndex(stableIndex);
                column.setDesignated(writerIndex == table.getTimestampIndexUnsafe());

                if (ColumnType.isSymbol(columnType)) {
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

                logger.debugW().$("Hydrating column [table=").$(token.getTableName()).$(", column=").$(columnName).I$();

                table.addColumnUnsafe(column);
            }
        }

        table.lock.writeLock().unlock();

        metaMem.close();
    }

    // fails if table already exists
    public void addTable(@NotNull CairoTable newTable) {
        lock.writeLock().lock();
        final String tableName = newTable.getNameUnsafe();
        final CairoTable existingTable = tables.get(tableName);
        if (existingTable != null) {
            throw CairoException.nonCritical().put("already exists in CairoMetadata [table=").put(tableName).put("]");
        }
        tables.put(tableName, newTable);
        lock.writeLock().unlock();
    }

    public CairoTable getTableQuick(@NotNull CharSequence tableName) {
        final CairoTable tbl = getTableQuiet(tableName);
        if (tbl == null) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        return tbl;
    }

    public CairoTable getTableQuiet(@NotNull CharSequence tableName) {
        lock.readLock().lock();
        final CairoTable tbl = tables.get(tableName);
        lock.readLock().unlock();
        return tbl;
    }

    public int getTablesCountUnsafe() {
        return tables.size();
    }
}



