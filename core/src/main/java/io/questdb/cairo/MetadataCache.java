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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
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
    private ColumnVersionReader columnVersionReader;
    private MemoryCMR metaMem = Vm.getCMRInstance();
    private long version;

    public MetadataCache(CairoEngine engine) {
        this.engine = engine;
    }

    @Override
    public void close() {
        metaMem = Misc.free(metaMem);
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

    private @NotNull ColumnVersionReader getColumnVersionReader() {
        if (columnVersionReader != null) {
            return columnVersionReader;
        }
        return columnVersionReader = new ColumnVersionReader();
    }

    private void hydrateTableStartup(@NotNull TableToken token, boolean throwError) {
        if (engine.isTableDropped(token)) {
            // Table writer can still process some transactions when DROP table has already
            // been executed, essentially updating dropped table. We should ignore such updates.
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
            // open metadata
            metaMem.smallFile(engine.getConfiguration().getFilesFacade(), path.$(), MemoryTag.NATIVE_METADATA_READER);
            TableUtils.validateMeta(metaMem, null, ColumnType.VERSION);

            table.setMetadataVersion(Long.MIN_VALUE);

            long metadataVersion = metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION);

            // make sure we aren't duplicating work
            CairoTable potentiallyExistingTable = tableMap.get(token.getTableName());
            if (potentiallyExistingTable != null && potentiallyExistingTable.getMetadataVersion() > metadataVersion) {
                LOG.debug().$("table in cache with newer version [table=").$(token)
                        .$(", version=").$(potentiallyExistingTable.getMetadataVersion())
                        .I$();
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
            int timestampWriterIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            table.setTimestampIndex(-1);
            table.setTtlHoursOrMonths(TableUtils.getTtlHoursOrMonths(metaMem));
            table.setSoftLinkFlag(isSoftLink);

            TableUtils.buildColumnListFromMetadataFile(metaMem, columnCount, table.columnOrderList);
            boolean isMetaFormatUpToDate = TableUtils.isMetaFormatUpToDate(metaMem);
            // populate columns
            for (int i = 0, n = table.columnOrderList.size(); i < n; i += 3) {
                int writerIndex = table.columnOrderList.get(i);
                // negative writer index columns were "replaced", as in renamed, or had their type changed
                if (writerIndex < 0) {
                    continue;
                }

                CharSequence name = metaMem.getStrA(table.columnOrderList.get(i + 1));

                assert name != null;
                int columnType = TableUtils.getColumnType(metaMem, writerIndex);

                // negative type means column was deleted
                if (columnType < 0) {
                    continue;
                }
                String columnName = Chars.toString(name);
                CairoColumn column = new CairoColumn();

                LOG.debug().$("hydrating column [table=").$(token).$(", column=").$safe(columnName).I$();

                column.setName(columnName);

                // Column positions already determined
                column.setPosition(table.getColumnCount());
                column.setType(columnType);
                column.setIndexedFlag(TableUtils.isColumnIndexed(metaMem, writerIndex));
                column.setIndexBlockCapacity(TableUtils.getIndexBlockCapacity(metaMem, writerIndex));
                column.setSymbolTableStaticFlag(true);
                column.setDedupKeyFlag(TableUtils.isColumnDedupKey(metaMem, writerIndex));
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
                    if (isMetaFormatUpToDate) {
                        column.setSymbolCapacity(TableUtils.getSymbolCapacity(metaMem, writerIndex));
                        column.setSymbolCached(TableUtils.isSymbolCached(metaMem, writerIndex));
                    } else {
                        LOG.debug().$("updating symbol capacity [table=").$(token).$(", column=").$safe(columnName).I$();
                        loadCapacities(column, token, path, engine.getConfiguration(), getColumnVersionReader());
                    }
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
        } finally {
            Misc.free(metaMem);
        }
    }

    private void loadCapacities(
            CairoColumn column,
            TableToken token,
            Path path,
            CairoConfiguration configuration,
            ColumnVersionReader columnVersionReader
    ) {
        final CharSequence columnName = column.getName();
        final int writerIndex = column.getWriterIndex();

        try (columnVersionReader) {
            LOG.debug().$("hydrating symbol metadata [table=").$(token).$(", column=").$safe(columnName).I$();

            // get column version
            path.trimTo(configuration.getDbRoot().length()).concat(token);
            final int rootLen = path.size();
            path.concat(TableUtils.COLUMN_VERSION_FILE_NAME);

            final long columnNameTxn;
            final FilesFacade ff = configuration.getFilesFacade();
            try (columnVersionReader) {
                columnVersionReader.ofRO(ff, path.$());
                columnVersionReader.readUnsafe();
                columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(writerIndex);
            }

            // initialize symbol map memory
            final long capacityOffset = SymbolMapWriter.HEADER_CAPACITY;
            final int capacity;
            final byte isCached;
            final LPSZ offsetFileName = TableUtils.offsetFileName(path.trimTo(rootLen), columnName, columnNameTxn);
            long fd = TableUtils.openRO(ff, offsetFileName, LOG);
            try {
                // use txn to find the correct symbol entry
                capacity = ff.readNonNegativeInt(fd, capacityOffset);
                isCached = ff.readNonNegativeByte(fd, SymbolMapWriter.HEADER_CACHE_ENABLED);
            } finally {
                ff.close(fd);
            }

            // get symbol properties
            if (capacity > 0 && isCached >= 0) {
                column.setSymbolCapacity(capacity);
                column.setSymbolCached(isCached != 0);
            } else {
                if (capacity <= 0) {
                    throw CairoException.nonCritical().put("invalid capacity [table=").put(token.getTableName()).put(", column=").put(columnName)
                            .put(", capacityOffset=").put(capacityOffset).put(", capacity=").put(capacity).put(']');
                }
                throw CairoException.nonCritical().put("invalid cache flag [table=").put(token.getTableName()).put(", column=").put(columnName)
                        .put(", cacheFlagOffset=").put(SymbolMapWriter.HEADER_CACHE_ENABLED).put(", cacheFlag=").put(isCached).put(']');
            }
        } catch (CairoException ex) {
            // Don't stall startup.
            LOG.error().$("could not load symbol metadata [table=").$(token).$(", column=").$safe(columnName)
                    .$(", errno=").$(ex.getErrno())
                    .$(", message=").$safe(ex.getMessage())
                    .I$();
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
        public void hydrateTable(@NotNull TableWriterMetadata tableMetadata) {
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
                table.columns.getQuick(i).setPosition(i);
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
