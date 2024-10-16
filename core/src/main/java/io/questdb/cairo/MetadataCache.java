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

import io.questdb.TelemetryConfigLogger;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
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
    private final SimpleReadWriteLock rwlock = new SimpleReadWriteLock();
    private final CharSequenceObjHashMap<CairoTable> tableMap = new CharSequenceObjHashMap<>();
    private MemoryCMR metaMem = Vm.getCMRInstance();
    private long version;

    public MetadataCache(CairoEngine engine) {
        this.engine = engine;
    }

    @Override
    public void close() {
        this.metaMem = Misc.free(metaMem);
        this.tableMap.clear();
    }

    /**
     * Used on a background thread at startup to populate the cache.
     * Cache is also populated on-demand by SQL metadata functions.
     * Takes a lock per table to not prevent ongoing probress of the database.
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
                    hydrateTable0(tableTokens.getQuick(i));
                }
            }

            try (MetadataCacheReader metadataRO = readLock()) {
                LOG.info().$("metadata hydration completed [tables=").$(metadataRO.getTableCount()).I$();
            }
        } catch (CairoException e) {
            LogRecord l = e.isCritical() ? LOG.critical() : LOG.error();
            l.$(e.getFlyweightMessage()).$();
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
        rwlock.readLock().lock();
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
        rwlock.writeLock().lock();
        version++;
        return cacheWriter;
    }

    private void hydrateTable0(@NotNull TableToken token) throws CairoException {

        Path path = Path.getThreadLocal(engine.getConfiguration().getRoot());

        // set up dir path
        path.concat(token.getDirName());

        boolean isSoftLink = Files.isSoftLink(path.$());

        // set up table path
        path.concat(TableUtils.META_FILE_NAME)
                .trimTo(path.size());

        // create table to work with
        CairoTable table = new CairoTable(token);

        try {
            // open metadata
            metaMem.smallFile(engine.getConfiguration().getFilesFacade(), path.$(), MemoryTag.NATIVE_METADATA_READER);
            TableUtils.validateMeta(metaMem, null, ColumnType.VERSION);

            table.setMetadataVersion(Long.MIN_VALUE);

            int metadataVersion = metaMem.getInt(TableUtils.META_OFFSET_METADATA_VERSION);

            // make sure we aren't duplicating work
            CairoTable potentiallyExistingTable = tableMap.get(token.getTableName());
            if (potentiallyExistingTable != null && potentiallyExistingTable.getMetadataVersion() > metadataVersion) {
                LOG.debug().$("table in cache with newer version [table=")
                        .$(token).$(", version=").$(potentiallyExistingTable.getMetadataVersion()).I$();
                return;
            }

            // get basic metadata
            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);

            LOG.debug().$("reading columns [table=").$(token)
                    .$(", count=").$(columnCount)
                    .I$();

            table.setMetadataVersion(metadataVersion);

            LOG.debug().$("set metadata version [table=").$(token)
                    .$(", version=").$(metadataVersion)
                    .I$();

            table.setPartitionBy(metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY));
            table.setMaxUncommittedRows(metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS));
            table.setO3MaxLag(metaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG));
            table.setTimestampIndex(metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX));
            table.setIsSoftLink(isSoftLink);

            TableUtils.buildWriterOrderMap(metaMem, table.columnOrderMap, metaMem, columnCount);

            // populate columns
            for (int i = 0, n = table.columnOrderMap.size(); i < n; i += 3) {

                int writerIndex = table.columnOrderMap.get(i);
                if (writerIndex < 0) {
                    continue;
                }

                CharSequence name = metaMem.getStrA(table.columnOrderMap.get(i + 1));

                assert name != null;
                int columnType = TableUtils.getColumnType(metaMem, writerIndex);

                if (columnType > -1) {
                    String columnName = Chars.toString(name);
                    CairoColumn column = new CairoColumn();

                    LOG.debug().$("hydrating column [table=").$(token).$(", column=").$(columnName).I$();

                    column.setName(columnName);
                    table.upsertColumn(column);

                    // this corresponds to either the latest entry
                    // or to the slot of a tombstoned entry (a column altered)
                    int existingIndex = TableUtils.getReplacingColumnIndex(metaMem, writerIndex);
                    int position = existingIndex > -1 ? existingIndex : (int) (table.getColumnCount() - 1);

                    column.setPosition(position);
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
                    column.setIsDesignated(writerIndex == table.getTimestampIndex());
                    if (columnType == ColumnType.SYMBOL) {
                        column.setSymbolCapacity(TableUtils.getSymbolCapacity(metaMem, writerIndex));
                        column.setSymbolCached(TableUtils.isSymbolCached(metaMem, writerIndex));
                    }
                    if (column.getIsDedupKey()) {
                        table.setIsDedup(true);
                    }
                }
            }

            tableMap.put(table.getTableName(), table);
        } catch (Throwable e) {
            // get rid of stale metadata
            tableMap.remove(token.getTableName());
            // if can't hydrate and table is not dropped, it's a critical error
            LogRecord root = engine.isTableDropped(token) ? LOG.info() : LOG.critical();
            root.$("could not hydrate metadata [table=").$(table.getTableToken()).I$();
        } finally {
            Misc.free(metaMem);
        }
    }

    /**
     * An implementation of {@link MetadataCacheReader }. Provides a read-path into the metadata cache.
     */
    private class MetadataCacheReaderImpl implements MetadataCacheReader, QuietCloseable {

        @Override
        public void close() {
            rwlock.readLock().unlock();
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
            if (Chars.startsWith(tableName, configuration.getSystemTableNamePrefix())) {
                return false;
            }

            // telemetry table
            if (configuration.getTelemetryConfiguration().hideTables()
                    && (Chars.equals(tableName, TelemetryTask.TABLE_NAME)
                    || Chars.equals(tableName, TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME))
            ) {
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
                                || cachedTable.getMetadataVersion() < latestTable.getMetadataVersion())
                                && isVisibleTable(latestTable.getTableName())) {
                    localCache.put(latestTable.getTableName(), latestTable);
                }
            }

            for (int i = localCache.size() - 1; i >= 0; i--) {
                CairoTable cachedTable = localCache.getAt(i);
                CairoTable latestTable = tableMap.get(cachedTable.getTableName());

                if (latestTable == null) {
                    localCache.remove(cachedTable.getTableName());
                    continue;
                }

                if (cachedTable.getMetadataVersion() < latestTable.getMetadataVersion()
                        && isVisibleTable(cachedTable.getTableName())) {
                    localCache.put(cachedTable.getTableName(), latestTable);
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
            rwlock.writeLock().unlock();
        }

        /**
         * Removes a table from the cache
         *
         * @param tableToken the table token.
         */
        @Override
        public void dropTable(@NotNull TableToken tableToken) {
            String tableName = tableToken.getTableName();
            tableMap.remove(tableName);
            LOG.info().$("dropped metadata [table=").$(tableName).I$();
        }

        /**
         * Rehydrates all tables in the database.
         */
        @Override
        public void hydrateAllTables() {
            clearCache();
            ObjHashSet<TableToken> tableTokensSet = new ObjHashSet<>();
            engine.getTableTokens(tableTokensSet, false);
            ObjList<TableToken> tableTokens = tableTokensSet.getList();

            if (tableTokens.size() == 0) {
                LOG.error().$("could not hydrate metadata, there are no table tokens").$();
                return;
            }

            for (int i = 0, n = tableTokens.size(); i < n; i++) {
                final TableToken tableToken = tableTokens.getQuick(i);
                try {
                    hydrateTable(tableToken);
                } catch (CairoException ex) {
                    LOG.error().$("could not hydrate metadata, exception:  ").$(ex.getFlyweightMessage()).$(" [table=").$(tableToken.getTableName()).I$();
                    throw ex;
                }
            }
        }

        /**
         * @see MetadataCacheWriter#hydrateTable(CharSequence)
         */
        @Override
        public void hydrateTable(@NotNull CharSequence tableName) throws TableReferenceOutOfDateException {
            final TableToken token = engine.getTableTokenIfExists(tableName);
            if (token == null) {
                throw TableReferenceOutOfDateException.of(tableName);
            }
            hydrateTable(token);
        }

        /**
         * @see MetadataCacheWriter#hydrateTable(TableToken)
         */
        @Override
        public void hydrateTable(@NotNull TableWriterMetadata tableMetadata) {
            final TableToken tableToken = tableMetadata.getTableToken();
            LOG.info().$("hydrating metadata [table=").$(tableToken).I$();

            CairoTable table = new CairoTable(tableToken);
            final long metadataVersion = tableMetadata.getMetadataVersion();
            table.setMetadataVersion(metadataVersion);

            LOG.debug().$("set metadata version [table=").$(tableToken)
                    .$(", version=").$(metadataVersion)
                    .I$();

            CairoTable potentiallyExistingTable = tableMap.get(tableToken.getTableName());
            if (potentiallyExistingTable != null && potentiallyExistingTable.getMetadataVersion() > metadataVersion) {
                LOG.info()
                        .$("table in cache with newer version [table=").$(tableToken)
                        .$(", version=").$(potentiallyExistingTable.getMetadataVersion()).I$();
                return;
            }

            int columnCount = tableMetadata.getColumnCount();

            LOG.debug().$("reading columns [table=").$(tableToken.getTableName())
                    .$(", count=").$(columnCount)
                    .I$();


            table.setPartitionBy(tableMetadata.getPartitionBy());
            table.setMaxUncommittedRows(tableMetadata.getMaxUncommittedRows());
            table.setO3MaxLag(tableMetadata.getO3MaxLag());

            int timestampIndex = tableMetadata.getTimestampIndex();
            table.setTimestampIndex(timestampIndex);
            table.setIsSoftLink(tableMetadata.isSoftLink());

            for (int i = 0; i < columnCount; i++) {
                final TableColumnMetadata columnMetadata = tableMetadata.getColumnMetadata(i);
                CharSequence columnName = columnMetadata.getName();

                int columnType = columnMetadata.getType();

                if (columnType < 0) {
                    continue; // marked for deletion
                }

                LOG.debug().$("hydrating column [table=").$(tableToken).$(", column=").$(columnName).I$();

                CairoColumn column = new CairoColumn();

                column.setName(columnName); // check this, not sure the char sequence is preserved
                column.setType(columnType);
                column.setPosition(columnMetadata.getReplacingIndex() > 0 ? columnMetadata.getReplacingIndex() - 1 : i);
                column.setIsIndexed(columnMetadata.isIndexed());
                column.setIndexBlockCapacity(columnMetadata.getIndexValueBlockCapacity());
                column.setIsSymbolTableStatic(columnMetadata.isSymbolTableStatic());
                column.setIsDedupKey(columnMetadata.isDedupKey());
                column.setWriterIndex(columnMetadata.getWriterIndex());
                column.setIsDesignated(column.getWriterIndex() == timestampIndex);

                if (column.getIsDedupKey()) {
                    table.setIsDedup(true);
                }

                if (ColumnType.isSymbol(column.getType())) {
                    LOG.debug().$("hydrating symbol metadata [table=").$(tableToken).$(", column=").$(columnName).I$();
                    column.setSymbolCapacity(tableMetadata.getSymbolCapacity(i));
                    column.setSymbolCached(tableMetadata.getSymbolCacheFlag(i));
                }

                table.upsertColumn(column);
            }

            table.columns.sort(comparator);

            for (int i = 0, n = table.columns.size(); i < n; i++) {
                table.columnNameIndexMap.put(table.columns.getQuick(i).getName(), i);
            }

            tableMap.put(table.getTableName(), table);
            LOG.info().$("hydrated metadata [table=").$(table.getTableToken()).I$();
        }

        @Override
        public void hydrateTable(@NotNull TableToken token) throws CairoException {
            LOG.info().$("hydrating metadata [table=").$(token).I$();
            hydrateTable0(token);
            LOG.info().$("hydrated metadata [table=").$(token).I$();
        }

        @Override
        public void renameTable(@NotNull TableToken fromTableToken, @NotNull TableToken toTableToken) {
            dropTable(fromTableToken);
            hydrateTable(toTableToken);
        }
    }
}
