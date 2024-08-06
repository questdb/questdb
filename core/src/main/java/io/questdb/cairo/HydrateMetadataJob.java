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
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.CairoColumn;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import java.io.Closeable;


public class HydrateMetadataJob extends SynchronizedJob implements Closeable {
    public static final Log LOG = LogFactory.getLog(HydrateMetadataJob.class);
    public static boolean completed = false;
    ColumnVersionReader columnVersionReader = new ColumnVersionReader();
    CairoConfiguration configuration;
    MemoryCMR metaMem;
    MemoryCMR offsetMem;
    Path path = new Path();
    int position;
    ObjHashSet<TableToken> tokens = new ObjHashSet<>();

    public HydrateMetadataJob(CairoEngine engine) {
        engine.getTableTokens(tokens, false);
        position = 0;
        this.configuration = engine.getConfiguration();
    }

    @Override
    public void close() {
        if (metaMem != null) {
            metaMem.close();
            metaMem = null;
        }
        if (path != null) {
            path.close();
            path = null;
        }
        if (offsetMem != null) {
            offsetMem.close();
            offsetMem = null;
        }
    }

    @Override
    protected boolean runSerially() {

        if (completed) {
            close();
            return true;
        }

        if (position == -1) {
            return true;
        }

        if (position >= tokens.size()) {
            completed = true;
            metaMem.close();
            path.close();
            LOG.infoW().$("Metadata hydration completed [num_tables=").$(CairoMetadata.INSTANCE.getTablesCountUnsafe()).I$();
            tokens.clear();
            position = -1;
            return true;
        }

        final TableToken token = tokens.get(position);

        LOG.debugW().$("Hydrating metadata for [table=").$(token.getTableName()).I$();

        if (token.isSystem()) {
            position++;
            return false;
        }

        // set up table path
        path.of(configuration.getRoot())
                .concat(token.getDirName())
                .concat(TableUtils.META_FILE_NAME)
                .trimTo(path.size());

        // open metadata
        metaMem = Vm.getCMRInstance();
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
            LOG.debugW().$("Added table [table=").$(token.getTableName()).I$();
        } catch (CairoException e) {
            final CairoTable alreadyHydrated = CairoMetadata.INSTANCE.getTableQuick(token.getTableName());
            LOG.debugW().$("Table already present [table=").$(token.getTableName()).I$();
            if (alreadyHydrated != null) {
                alreadyHydrated.lock.writeLock().lock();
                long version = alreadyHydrated.getLastMetadataVersionUnsafe();
                alreadyHydrated.lock.writeLock().unlock();

                if (version == metadataVersion) {
                    return true;
                }

                LOG.debugW().$("Updating metadata [table=").$(token.getTableName()).I$();

                table = alreadyHydrated;

            } else {
                throw e;
            }
        }

        // [NW] - continue hydrating table and columns etc.
        int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        int partitionBy = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
        int maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
        long o3MaxLag = metaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG);

        table.setPartitionByUnsafe(PartitionBy.toString(partitionBy));
        table.setMaxUncommittedRowsUnsafe(maxUncommittedRows);
        table.setO3MaxLagUnsafe(o3MaxLag);
        table.setLastMetadataVersionUnsafe(metadataVersion);
        table.setDesignatedTimestampIndexUnsafe(timestampIndex);

        TableUtils.buildWriterOrderMap(metaMem, table.columnOrderMap, metaMem, columnCount);

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

                column.setNameUnsafe(columnName);
                column.setTypeUnsafe(columnType);
                column.setIsIndexedUnsafe(TableUtils.isColumnIndexed(metaMem, writerIndex));
                column.setIndexBlockCapacityUnsafe(TableUtils.getIndexBlockCapacity(metaMem, writerIndex));
                column.setIsSymbolTableStaticUnsafe(true);
                column.setIsDedupKeyUnsafe(TableUtils.isColumnDedupKey(metaMem, writerIndex));
                column.setWriterIndexUnsafe(writerIndex);
                column.setDenseSymbolIndexUnsafe(denseSymbolIndex);
                column.setStableIndex(stableIndex);
                column.setDesignated(writerIndex == timestampIndex);

                path.trimTo(configuration.getRoot().length())
                        .concat(table.getDirectoryNameUnsafe())
                        .concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                columnVersionReader.ofRO(configuration.getFilesFacade(),
                        path.$());

                final long columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(writerIndex);

                if (ColumnType.isSymbol(columnType)) {
                    final LPSZ offsetFileName = TableUtils.offsetFileName(
                            path.trimTo(configuration.getRoot().length()).concat(table.getDirectoryNameUnsafe())
                            , columnName, columnNameTxn);

                    offsetMem = Vm.getCMRInstance();
                    final long offsetMemSize = SymbolMapWriter.keyToOffset(0) + Long.BYTES;
                    offsetMem.of(configuration.getFilesFacade(), offsetFileName, offsetMemSize, offsetMemSize, MemoryTag.NATIVE_METADATA_READER);
                    column.setSymbolCapacityUnsafe(offsetMem.getInt(SymbolMapWriter.HEADER_CAPACITY));
                    assert column.getSymbolCapacityUnsafe() > 0;
                    column.setSymbolCachedUnsafe(offsetMem.getBool(SymbolMapWriter.HEADER_CACHE_ENABLED));

                    offsetMem.close();
                    columnVersionReader.close();
                }

                LOG.debugW().$("Hydrating column [table=").$(token.getTableName()).$(", column=").$(columnName).I$();

                table.addColumnUnsafe(column);

                if (writerIndex == timestampIndex) {
                    table.setDesignatedTimestampIndexUnsafe(table.columns.size() - 1);
                }
            }
        }

        table.lock.writeLock().unlock();

        metaMem.close();
        position++;

        return false;
    }
}
