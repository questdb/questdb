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

import io.questdb.TelemetryConfigLogger;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

/**
 * A metadata cache for serving SQL requests for basic table info.
 */
public class MetadataCache implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MetadataCache.class);
    private static final Comparator<CairoColumn> comparator = Comparator.comparingInt(CairoColumn::getPosition);
    private final MetadataCacheReaderImpl cacheReader = new MetadataCacheReaderImpl();
    private final MetadataCacheWriterImpl cacheWriter = new MetadataCacheWriterImpl();
    private final CairoEngine engine;
    private final SimpleReadWriteLock rwLock = new SimpleReadWriteLock();
    private final CharSequenceObjHashMap<CairoTable> tableMap = new CharSequenceObjHashMap<>();
    private BlockFileReader blockFileReader;
    private long version;

    public MetadataCache(CairoEngine engine) {
        this.engine = engine;
    }

    @Override
    public void close() {
        blockFileReader = Misc.free(blockFileReader);
        tableMap.clear();
    }

    /**
     * Used on a background thread at startup to populate the cache.
     * Cache is also populated on-demand by SQL metadata functions.
     * Takes a lock per table to not prevent the ongoing progress of the database.
     * Generally completes quickly.
     */
    public void onStartupAsyncHydrator() {
        try {
            final ObjHashSet<TableToken> tableTokensSet = new ObjHashSet<>();
            engine.getTableTokens(tableTokensSet, false);
            final ObjList<TableToken> tableTokens = tableTokensSet.getList();

            LOG.info().$("metadata hydration started [tables=").$(tableTokens.size()).I$();
            for (int i = 0, n = tableTokens.size(); i < n; i++) {
                // acquire a write lock for each table
                try (MetadataCacheWriter ignore = writeLock()) {
                    hydrateTableStartup(tableTokens.getQuick(i), false);
                }
            }

            try (MetadataCacheReader metadataRO = readLock()) {
                LOG.info().$("metadata hydration completed [tables=").$(metadataRO.getTableCount()).I$();
            }
        } catch (CairoException e) {
            LogRecord l = e.isCritical() ? LOG.critical() : LOG.error();
            l.$safe(e.getFlyweightMessage()).$();
        } finally {
            Path.clearThreadLocals();
        }
    }

    /**
     * Begins the read-path by taking a read lock and acquiring a thread-local
     * {@link MetadataCacheReaderImpl}, an implementation of {@link MetadataCacheReader}.
     *
     * @return {@link MetadataCacheReader}
     */
    public MetadataCacheReader readLock() {
        rwLock.readLock().lock();
        return cacheReader;
    }

    /**
     * Begins the read-path by taking a write lock and acquiring a thread-local
     * {@link MetadataCacheWriterImpl}, an implementation of {@link MetadataCacheWriter}.
     * Also increments a version counter, used to invalidate the cache.
     * The version counter does not guarantee a version change for any particular table,
     * so each {@link CairoTable} has its own metadata version.
     * Returns a singleton writer, not a thread-local, since there should be one
     * writer at a time.
     *
     * @return {@link MetadataCacheWriter}
     */
    public MetadataCacheWriter writeLock() {
        rwLock.writeLock().lock();
        version++;
        return cacheWriter;
    }

    private void hydrateTableStartup(@NotNull TableToken token, boolean throwError) {
        if (engine.isTableDropped(token)) {
            // Table writer can still process some transactions when DROP table has already
            // been executed, essentially updating dropped table. We should ignore such updates.
            return;
        }

        if (token.isView()) {
            // views do not have _meta file
            // view metadata will be added to the cache when the view is compiled
            return;
        }

        Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot());

        // set up the dir path
        path.concat(token.getDirName());

        boolean isSoftLink = Files.isSoftLink(path.$());

        // set up the table path
        path.concat(TableUtils.META_FILE_NAME).trimTo(path.size());

        // create table to work with
        CairoTable table = new CairoTable(token);

        try {
            // open metadata using BlockFile format
            if (blockFileReader == null) {
                blockFileReader = new BlockFileReader(engine.getConfiguration());
            }
            blockFileReader.of(path.$());
            TableMetadataFileBlock.MetadataHolder holder = new TableMetadataFileBlock.MetadataHolder();
            TableMetadataFileBlock.read(blockFileReader, holder, path.$());
            // Close reader after reading - we have all data in holder
            blockFileReader.close();

            table.setMetadataVersion(Long.MIN_VALUE);

            long metadataVersion = holder.metadataVersion;

            // make sure we aren't duplicating work
            CairoTable potentiallyExistingTable = tableMap.get(token.getTableName());
            if (potentiallyExistingTable != null && potentiallyExistingTable.getMetadataVersion() > metadataVersion) {
                LOG.debug().$("table in cache with newer version [table=").$(token)
                        .$(", version=").$(potentiallyExistingTable.getMetadataVersion())
                        .I$();
                return;
            }

            // get basic metadata
            int columnCount = holder.columnCount;

            LOG.debug().$("reading columns [table=").$(token)
                    .$(", count=").$(columnCount)
                    .I$();

            table.setMetadataVersion(metadataVersion);

            LOG.debug().$("set metadata version [table=").$(token)
                    .$(", version=").$(metadataVersion)
                    .I$();

            table.setPartitionBy(holder.partitionBy);
            table.setMaxUncommittedRows(holder.maxUncommittedRows);
            table.setO3MaxLag(holder.o3MaxLag);
            int timestampWriterIndex = holder.timestampIndex;
            table.setTimestampIndex(-1);
            table.setTtlHoursOrMonths(holder.ttlHoursOrMonths);
            table.setSoftLinkFlag(isSoftLink);

            // Populate columns from BlockFile metadata
            // Columns with replacingIndex take the logical position of the column they replace
            // Sort by effective slot (replacingIndex if set, otherwise writerIndex) to get logical order
            ObjList<TableColumnMetadata> sortedColumns = new ObjList<>(holder.columns.size());
            for (int i = 0, n = holder.columns.size(); i < n; i++) {
                TableColumnMetadata colMeta = holder.columns.getQuick(i);
                // Skip deleted columns (tombstones) - they're placeholders for replaced columns
                if (colMeta.getColumnType() < 0) {
                    continue;
                }
                sortedColumns.add(colMeta);
            }
            // Sort by effective slot: replacingIndex if >= 0, otherwise writerIndex
            sortedColumns.sort((a, b) -> {
                int slotA = a.getReplacingIndex() >= 0 ? a.getReplacingIndex() : a.getWriterIndex();
                int slotB = b.getReplacingIndex() >= 0 ? b.getReplacingIndex() : b.getWriterIndex();
                return Integer.compare(slotA, slotB);
            });

            for (int i = 0, n = sortedColumns.size(); i < n; i++) {
                TableColumnMetadata colMeta = sortedColumns.getQuick(i);
                int columnType = colMeta.getColumnType();
                String columnName = colMeta.getColumnName();
                int writerIndex = colMeta.getWriterIndex();

                CairoColumn column = new CairoColumn();

                LOG.debug().$("hydrating column [table=").$(token).$(", column=").$safe(columnName).I$();

                column.setName(columnName);

                // Position is determined by sorted order (effective slot order)
                column.setPosition(table.getColumnCount());
                column.setType(columnType);
                column.setIndexedFlag(colMeta.isSymbolIndexFlag());
                column.setIndexBlockCapacity(colMeta.getIndexValueBlockCapacity());
                column.setSymbolTableStaticFlag(true);
                column.setDedupKeyFlag(colMeta.isDedupKeyFlag());
                column.setWriterIndex(writerIndex);

                boolean isDesignated = writerIndex == timestampWriterIndex;
                column.setDesignatedFlag(isDesignated);
                if (isDesignated) {
                    // Timestamp index is the logical index of the column in the column list. Rather than
                    // physical index of the column in the metadata file (writer index).
                    table.setTimestampType(columnType);
                    table.setTimestampIndex(table.getColumnCount());
                }

                if (column.isDedupKey()) {
                    table.setDedupFlag(true);
                }

                if (columnType == ColumnType.SYMBOL) {
                    // BlockFile format always stores symbol capacity
                    column.setSymbolCapacity(colMeta.getSymbolCapacity());
                    column.setSymbolCached(colMeta.isSymbolCacheFlag());
                }

                table.upsertColumn(column);
            }

            tableMap.put(table.getTableName(), table);
            LOG.debug().$("hydrated metadata [table=").$(token).I$();
        } catch (Throwable e) {
            // get rid of stale metadata
            tableMap.remove(token.getTableName());
            // if we can't hydrate and the table is not dropped, it's a critical error
            LogRecord log = engine.isTableDropped(token) ? LOG.info() : LOG.critical();
            try {
                log
                        .$("could not hydrate metadata [table=").$(token)
                        .$(", msg=");
                if (e instanceof FlyweightMessageContainer) {
                    log.$safe(((FlyweightMessageContainer) e).getFlyweightMessage());
                } else {
                    log.$safe(e.getMessage());
                }
                log.$(", errno=").$(e instanceof CairoException ? ((CairoException) e).errno : 0);
            } finally {
                log.I$();
            }
            if (throwError) {
                throw e;
            }
        }
    }

    /**
     * An implementation of {@link MetadataCacheReader }. Provides a read-path into the metadata cache.
     */
    private class MetadataCacheReaderImpl implements MetadataCacheReader, QuietCloseable {

        @Override
        public void close() {
            rwLock.readLock().unlock();
        }

        /**
         * Returns a table ONLY if it is already present in the cache.
         *
         * @param tableToken the token for the table
         * @return CairoTable the table, if present in the cache.
         */
        @Override
        public @Nullable CairoTable getTable(@NotNull TableToken tableToken) {
            return tableMap.get(tableToken.getTableName());
        }

        /**
         * Returns a count of the tables in the cache.
         */
        @Override
        public int getTableCount() {
            return tableMap.size();
        }

        /**
         * Returns the current cache version.
         */
        @Override
        public long getVersion() {
            return version;
        }

        @Override
        public boolean isVisibleTable(@NotNull CharSequence tableName) {
            CairoConfiguration configuration = engine.getConfiguration();

            // sys table
            if (Chars.startsWith(tableName, configuration.getSystemTableNamePrefix())
                    && !Chars.startsWith(tableName, configuration.getParquetExportTableNamePrefix())) {
                return false;
            }

            // telemetry table
            if (configuration.getTelemetryConfiguration().hideTables()
                    && (Chars.equals(tableName, TelemetryTask.TABLE_NAME)
                    || Chars.equals(tableName, TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME))
            ) {
                return false;
            }

            // query tracing table
            if (Chars.equals(tableName, QueryTracingJob.TABLE_NAME)) {
                return false;
            }

            return TableUtils.isFinalTableName((String) tableName, configuration.getTempRenamePendingTablePrefix());
        }

        /**
         * Refreshes a snapshot of the cache by checking versions of the cache,
         * and inner tables, and copying references to any tables that have changed.
         *
         * @param localCache   the snapshot to be refreshed
         * @param priorVersion the version of the snapshot
         * @return the current version of the snapshot
         */
        @Override
        public long snapshot(CharSequenceObjHashMap<CairoTable> localCache, long priorVersion) {
            if (priorVersion >= getVersion()) {
                return priorVersion;
            }

            // pull from cairoTables into localCache
            for (int i = tableMap.size() - 1; i >= 0; i--) {
                CairoTable latestTable = tableMap.getAt(i);
                CairoTable cachedTable = localCache.get(latestTable.getTableName());

                if (
                        (cachedTable == null
                                || cachedTable.getMetadataVersion() < latestTable.getMetadataVersion()
                                || cachedTable.getTableToken().getTableId() != latestTable.getTableToken().getTableId())
                                && isVisibleTable(latestTable.getTableName())
                ) {
                    localCache.put(latestTable.getTableName(), latestTable);
                }
            }

            for (int i = localCache.size() - 1; i >= 0; i--) {
                CairoTable cachedTable = localCache.getAt(i);
                CairoTable latestTable = tableMap.get(cachedTable.getTableName());

                if (latestTable == null) {
                    localCache.remove(cachedTable.getTableName());
                }
            }

            return version;
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sink.put("MetadataCache [");
            sink.put("tableCount=").put(tableMap.size()).put(']');
            sink.put('\n');

            for (int i = 0, n = tableMap.size(); i < n; i++) {
                sink.put('\t');
                tableMap.getAt(i).toSink(sink);
                sink.put('\n');
            }
        }
    }

    /**
     * An implementation of {@link MetadataCacheWriter }. Provides a read-path into the metadata cache.
     */
    private class MetadataCacheWriterImpl extends MetadataCacheReaderImpl implements MetadataCacheWriter {

        /**
         * Clears the table cache.
         */
        @Override
        public void clearCache() {
            tableMap.clear();
        }

        /**
         * Closes the writer, releasing the global metadata cache write-lock.
         */
        @Override
        public void close() {
            rwLock.writeLock().unlock();
        }

        /**
         * Removes a table from the cache
         *
         * @param tableToken the table token.
         */
        @Override
        public void dropTable(@NotNull TableToken tableToken) {
            String tableName = tableToken.getTableName();
            CairoTable entry = tableMap.get(tableName);
            if (entry != null && tableToken.equals(entry.getTableToken())) {
                tableMap.remove(tableName);
                LOG.info().$("dropped [table=").$(tableToken).I$();
            }
        }

        /**
         * @see MetadataCacheWriter#hydrateTable(TableToken)
         */
        @Override
        public void hydrateTable(@NotNull TableMetadata tableMetadata) {
            if (engine.isTableDropped(tableMetadata.getTableToken())) {
                // Table writer can still process some transactions when DROP table has already
                // been executed, essentially updating dropped table. We should ignore such updates.
                return;
            }

            final TableToken tableToken = tableMetadata.getTableToken();
            final CairoTable table = new CairoTable(tableToken);
            final long metadataVersion = tableMetadata.getMetadataVersion();
            table.setMetadataVersion(metadataVersion);

            LOG.debug().$("set metadata version [table=").$(tableToken)
                    .$(", version=").$(metadataVersion)
                    .I$();

            CairoTable potentiallyExistingTable = tableMap.get(tableToken.getTableName());
            if (potentiallyExistingTable != null && potentiallyExistingTable.getMetadataVersion() > metadataVersion) {
                LOG.info()
                        .$("table in cache with newer version [table=").$(tableToken)
                        .$(", version=").$(potentiallyExistingTable.getMetadataVersion())
                        .I$();
                return;
            }

            int columnCount = tableMetadata.getColumnCount();

            LOG.debug().$("reading columns [table=").$(tableToken)
                    .$(", count=").$(columnCount)
                    .I$();

            table.setPartitionBy(tableMetadata.getPartitionBy());
            table.setMaxUncommittedRows(tableMetadata.getMaxUncommittedRows());
            table.setO3MaxLag(tableMetadata.getO3MaxLag());
            int timestampWriterIndex = tableMetadata.getTimestampIndex();
            table.setTimestampIndex(-1);
            table.setTtlHoursOrMonths(tableMetadata.getTtlHoursOrMonths());
            Path tempPath = Path.getThreadLocal(engine.getConfiguration().getDbRoot());
            table.setSoftLinkFlag(Files.isSoftLink(tempPath.concat(tableToken.getDirNameUtf8()).$()));

            for (int i = 0; i < columnCount; i++) {
                final TableColumnMetadata columnMetadata = tableMetadata.getColumnMetadata(i);

                int columnType = columnMetadata.getColumnType();
                if (columnType < 0) {
                    continue; // marked for deletion
                }

                String columnName = columnMetadata.getColumnName();
                LOG.debug().$("hydrating column [table=").$(tableToken).$(", column=").$safe(columnName).I$();

                CairoColumn column = new CairoColumn();

                column.setName(columnName);
                column.setType(columnType);
                int replacingIndex = columnMetadata.getReplacingIndex();
                column.setPosition(replacingIndex > -1 ? replacingIndex : i);
                column.setIndexedFlag(columnMetadata.isSymbolIndexFlag());
                column.setIndexBlockCapacity(columnMetadata.getIndexValueBlockCapacity());
                column.setSymbolTableStaticFlag(columnMetadata.isSymbolTableStatic());
                column.setDedupKeyFlag(columnMetadata.isDedupKeyFlag());

                int writerIndex = columnMetadata.getWriterIndex();
                column.setWriterIndex(writerIndex);

                boolean isDesignated = writerIndex == timestampWriterIndex;
                column.setDesignatedFlag(isDesignated);
                if (isDesignated) {
                    table.setTimestampIndex(table.getColumnCount());
                }

                if (column.isDedupKey()) {
                    table.setDedupFlag(true);
                }

                if (ColumnType.isSymbol(column.getType())) {
                    LOG.debug().$("hydrating symbol metadata [table=").$(tableToken).$(", column=").$safe(columnName).I$();
                    column.setSymbolCapacity(tableMetadata.getSymbolCapacity(i));
                    column.setSymbolCached(tableMetadata.getSymbolCacheFlag(i));
                }

                table.upsertColumn(column);
            }

            table.columns.sort(comparator);

            for (int i = 0, n = table.columns.size(); i < n; i++) {
                // Update column positions after sort
                final CairoColumn column = table.columns.getQuick(i);
                column.setPosition(i);
                // Update designated timestamp index
                if (column.isDesignated()) {
                    table.setTimestampIndex(i);
                }
                // Update column name index map
                table.columnNameIndexMap.put(table.columns.getQuick(i).getName(), i);
            }

            tableMap.put(table.getTableName(), table);
            LOG.info().$("hydrated [table=").$(table.getTableToken()).I$();
        }

        @Override
        public void hydrateTable(@NotNull TableToken token) {
            hydrateTableStartup(token, true);
        }

        @Override
        public void renameTable(@NotNull TableToken fromTableToken, @NotNull TableToken toTableToken) {
            String tableName = fromTableToken.getTableName();
            final int index = tableMap.keyIndex(tableName);

            // Metadata may not be hydrated at this point, handle this case
            if (index < 0) {
                CairoTable fromTab = tableMap.valueAt(index);
                tableMap.removeAt(index);
                tableMap.put(toTableToken.getTableName(), new CairoTable(toTableToken, fromTab));
            }
        }
    }
}
