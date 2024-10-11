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
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CairoMetadata {
    private static final Log LOG = LogFactory.getLog(CairoMetadata.class);
    private final CairoEngine engine;
    private final SimpleReadWriteLock lock = new SimpleReadWriteLock();
    private final ThreadLocal<CairoMetadataReader> reader = ThreadLocal.withInitial(CairoMetadataReader::new);
    private final HashMap<CharSequence, CairoTable> tables = new HashMap<>();
    private final ThreadLocal<ColumnVersionReader> tlColumnVersionReader = ThreadLocal.withInitial(ColumnVersionReader::new);
    private final ThreadLocal<Path> tlPath = ThreadLocal.withInitial(Path::new);
    private final CairoMetadataWriter writer = new CairoMetadataWriter();
    ThreadLocal<StringSink> tlSink = ThreadLocal.withInitial(StringSink::new);
    private long version;

    public CairoMetadata(CairoEngine engine) {
        this.engine = engine;
    }

    public void asyncHydrator() {
        try {
            final ObjHashSet<TableToken> tableTokensSet = new ObjHashSet<>();
            engine.getTableTokens(tableTokensSet, false);
            final ObjList<TableToken> tableTokens = tableTokensSet.getList();

            LOG.info().$("metadata hydration started [tables=").$(tableTokens.size()).I$();

            try (CairoMetadataRW metadataRW = write()) {
                for (int i = 0, n = tableTokens.size(); i < n; i++) {
                    metadataRW.hydrateTable(tableTokens.getQuick(i), false);
                }
                LOG.info().$("metadata hydration completed [tables=").$(metadataRW.getTableCount()).I$();
            }
        } catch (CairoException e) {
            LogRecord l = e.isCritical() ? LOG.critical() : LOG.error();
            l.$(e.getMessage()).$();
        }
    }

    public CairoMetadataRO read() {
        lock.readLock().lock();
        return reader.get();
    }

    /**
     * Thread unsafe function for debug printing the metadata object, doesn't require manual closing.
     */
    @SuppressWarnings("unused")
    @TestOnly
    public String toString0Unsafe() {
        StringSink sink = tlSink.get();
        sink.put("CairoMetadata [");
        sink.put("tableCount=").put(tables.size()).put(']');
        sink.put('\n');

        for (CairoTable table : tables.values()) {
            sink.put('\t');
            table.toSink(sink);
            sink.put('\n');
        }
        String s = sink.toString();
        sink.clear();
        tlSink.remove();
        return s;
    }

    public CairoMetadataRW write() {
        lock.writeLock().lock();
        version++;
        return writer;
    }

    private class CairoMetadataReader implements CairoMetadataRO, Closeable, AutoCloseable {
        @Override
        public void close() {
            lock.readLock().unlock();
        }

        public void filterVisibleTables(HashMap<CharSequence, CairoTable> localCache) {
            Iterator<Map.Entry<CharSequence, CairoTable>> iterator = localCache.entrySet().iterator();

            boolean isSys = false;
            boolean isTel = false;
            boolean isNotFinal = false;

            while (iterator.hasNext()) {
                CairoTable table = iterator.next().getValue();

                if (Chars.startsWith(table.getTableName(), engine.getConfiguration().getSystemTableNamePrefix())) {
                    isSys = true;
                }

                // telemetry table
                if (engine.getConfiguration().getTelemetryConfiguration().hideTables()
                        && (Chars.equals(table.getTableName(), TelemetryTask.TABLE_NAME)
                        || Chars.equals(table.getTableName(), TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME))
                ) {
                    isTel = true;
                }

                if (!TableUtils.isFinalTableName(table.getTableName(), engine.getConfiguration().getTempRenamePendingTablePrefix())) {
                    isNotFinal = true;
                }

                // if shouldn't be visble, remove it
                if (isSys || isTel || isNotFinal) {
                    iterator.remove();
                }

                // reset
                isSys = false;
                isTel = false;
                isNotFinal = false;
            }

        }

        public @Nullable CairoTable getTable(@NotNull TableToken tableToken) {
            return tables.get(tableToken.getTableName());
        }

        @Override
        public int getTableCount() {
            return tables.size();
        }

        public long getVersion() {
            return version;
        }

        public @Nullable CairoTable getVisibleTable(@NotNull TableToken tableToken) {
            CairoConfiguration configuration = engine.getConfiguration();
            if (Chars.startsWith(tableToken.getTableName(), configuration.getSystemTableNamePrefix())) {
                return null;
            }
            // telemetry table
            if (configuration.getTelemetryConfiguration().hideTables()
                    && (Chars.equals(tableToken.getTableName(), TelemetryTask.TABLE_NAME)
                    || Chars.equals(tableToken.getTableName(), TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME))
            ) {
                return null;
            }

            if (TableUtils.isFinalTableName(tableToken.getTableName(), configuration.getTempRenamePendingTablePrefix())) {
                return getTable(tableToken);
            }

            return null;
        }

        @Override
        public void snapshotCreate(HashMap<CharSequence, CairoTable> localCache) {
            localCache.putAll(tables);
        }

        @Override
        public long snapshotRefresh(HashMap<CharSequence, CairoTable> localCache, long priorVersion) {
            if (priorVersion >= getVersion()) {
                return priorVersion;
            }

            Iterator<Map.Entry<CharSequence, CairoTable>> iterator = tables.entrySet().iterator();

            // pull from cairoTables into localCache
            while (iterator.hasNext()) {
                CairoTable latestTable = iterator.next().getValue();
                CairoTable cachedTable = localCache.get(latestTable.getTableName());
                if (cachedTable == null) {
                    localCache.put(latestTable.getTableName(), latestTable);
                } else if (cachedTable.getMetadataVersion() < latestTable.getMetadataVersion()) {
                    localCache.put(cachedTable.getTableName(), latestTable);
                } else if (cachedTable.getMetadataVersion() > latestTable.getMetadataVersion()) {
                    throw new RuntimeException("disordered metadata versions");
                } else {
                    assert cachedTable.getMetadataVersion() == latestTable.getMetadataVersion();
                    // otherwise its up to date, so we loop
                }
            }

            iterator = localCache.entrySet().iterator();
            while (iterator.hasNext()) {
                CairoTable cachedTable = iterator.next().getValue();
                CairoTable latestTable = tables.get(cachedTable.getTableName());
                if (latestTable == null) {
                    // if its not in the main cache, removed it from local cache
                    iterator.remove();
                } else if (cachedTable.getMetadataVersion() < latestTable.getMetadataVersion()) {
                    localCache.put(cachedTable.getTableName(), latestTable);
                } else if (cachedTable.getMetadataVersion() > latestTable.getMetadataVersion()) {
                    throw new RuntimeException("disordered metadata versions");
                } else {
                    assert cachedTable.getMetadataVersion() == latestTable.getMetadataVersion();
                    // otherwise its up to date, so we loop
                }
            }

            return version;
        }

        /**
         * For debug printing the metadata object, doesn't require manual closing.
         */
        @TestOnly
        public String toString0() {
            StringSink sink = tlSink.get();
            sink.put("CairoMetadata [");
            sink.put("tableCount=").put(tables.size()).put(']');
            sink.put('\n');

            for (CairoTable table : tables.values()) {
                sink.put('\t');
                table.toSink(sink);
                sink.put('\n');
            }
            close();
            String s = sink.toString();
            sink.clear();
            tlSink.remove();
            return s;
        }
    }

    private class CairoMetadataWriter extends CairoMetadataReader implements Closeable, CairoMetadataRW, AutoCloseable {

        public void clear() {
            tables.clear();
        }

        @Override
        public void close() {
            lock.writeLock().unlock();
        }

        public void dropTable(@NotNull CharSequence tableName) {
            tables.remove(tableName);
            LOG.info().$("dropped metadata [table=").$(tableName).I$();
        }

        public void dropTable(@NotNull TableToken tableToken) {
            dropTable(tableToken.getTableName());
        }

        @Override
        public CairoTable getTable(@NotNull TableToken tableToken) {
            CairoTable table = super.getTable(tableToken);
            if (table == null) {
                // hydrate cache
                hydrateTable(tableToken, true);
            }
            table = super.getTable(tableToken);
            return table;
        }

        /**
         * This is dangerous and may clobber any concurrent metadata changes. This should only be used in last-resort cases,
         * for example, for testing purposes.
         */
        @TestOnly
        public void hydrateAllTables() {
            ObjHashSet<TableToken> tableTokensSet = new ObjHashSet<>();
            engine.getTableTokens(tableTokensSet, false);
            ObjList<TableToken> tableTokens = tableTokensSet.getList();

            if (tableTokens.size() == 0) {
                LOG.error().$("could not hydrate metadata, there are no table tokens").$();
                return;
            }

            TableToken tableToken = tableTokens.getQuick(0);

            try {
                for (int i = 0, n = tableTokens.size(); i < n; i++) {
                    tableToken = tableTokens.getQuick(i);
                    hydrateTable(tableToken, true);
                }
            } catch (CairoException ex) {
                LOG.error().$("could not hydrate metadata, exception:  ").$(ex.getMessage()).$(" [table=").$(tableToken.getTableName()).I$();
            }
        }


        /**
         * @see CairoMetadataRW#hydrateTable(TableToken, boolean)
         */
        public void hydrateTable(@NotNull TableToken token, boolean infoLog) {
            LOG.debug().$("hydrating table using thread-local path and column version reader [table=")
                    .$(token).I$();
            try {
                hydrateTable(token, tlPath.get(), tlColumnVersionReader.get(), infoLog);
            } finally {
                tlPath.get().close();
                tlColumnVersionReader.get().close();
                tlColumnVersionReader.remove();
                tlPath.remove();
            }
        }

        /**
         * @see CairoMetadataRW#hydrateTable(CharSequence, boolean)
         */
        public void hydrateTable(@NotNull CharSequence tableName, boolean infoLog) throws TableReferenceOutOfDateException {
            final TableToken token = engine.getTableTokenIfExists(tableName);
            if (token == null) {
                throw TableReferenceOutOfDateException.of(tableName);
            }
            hydrateTable(token, infoLog);
        }

        /**
         * @see CairoMetadataRW#hydrateTable(TableToken, boolean)
         */
        public void hydrateTable(@NotNull TableWriterMetadata tableMetadata, boolean infoLog) {
            final TableToken tableToken = tableMetadata.getTableToken();

            if (infoLog) {
                LOG.info().$("hydrating metadata [table=").$(tableToken).I$();
            }

            CairoTable table = new CairoTable(tableToken);
            final long metadataVersion = tableMetadata.getMetadataVersion();
            table.setMetadataVersion(metadataVersion);

            LOG.debug().$("set metadata version [table=").$(tableToken)
                    .$(", version=").$(metadataVersion)
                    .I$();

            CairoTable potentiallyExistingTable = tables.get(tableToken.getTableName());
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

            table.columns.sort(Comparator.comparingInt(CairoColumn::getPosition));

            for (int i = 0, n = table.columns.size(); i < n; i++) {
                table.columnNameIndexMap.put(table.columns.getQuick(i).getName(), i);
            }

            tables.put(table.getTableName(), table);

            if (infoLog) {
                LOG.info().$("hydrated metadata [table=").$(table.getTableToken()).I$();
            }

        }

        public void hydrateTable(
                @NotNull TableToken token,
                @NotNull Path path,
                @NotNull ColumnVersionReader columnVersionReader,
                boolean infoLog
        ) throws CairoException {
            if (infoLog) {
                LOG.info().$("hydrating metadata [table=").$(token).I$();
            }

            // set up dir path
            path.of(engine.getConfiguration().getRoot())
                    .concat(token.getDirName());

            boolean isSoftLink = Files.isSoftLink(path.$());

            // set up table path
            path.concat(TableUtils.META_FILE_NAME)
                    .trimTo(path.size());

            // create table to work with
            CairoTable table = new CairoTable(token);
            MemoryCMR metaMem = Vm.getCMRInstance();

            try {
                // open metadata
                metaMem.smallFile(engine.getConfiguration().getFilesFacade(), path.$(), MemoryTag.NATIVE_METADATA_READER);
                TableUtils.validateMeta(metaMem, null, ColumnType.VERSION);

                table.setMetadataVersion(Long.MIN_VALUE);

                int metadataVersion = metaMem.getInt(TableUtils.META_OFFSET_METADATA_VERSION);

                // make sure we aren't duplicating work
                CairoTable potentiallyExistingTable = tables.get(token.getTableName());
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

                        if (column.getIsDedupKey()) {
                            table.setIsDedup(true);
                        }

                        if (ColumnType.isSymbol(columnType)) {
                            LOG.debug().$("hydrating symbol metadata [table=").$(token).$(", column=").$(columnName).I$();


                            // get column version
                            path.trimTo(engine.getConfiguration().getRoot().length())
                                    .concat(table.getDirectoryName())
                                    .concat(TableUtils.COLUMN_VERSION_FILE_NAME);

                            columnVersionReader.ofRO(engine.getConfiguration().getFilesFacade(),
                                    path.$());

                            columnVersionReader.readUnsafe();
                            final long columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(writerIndex);

                            // use txn to find correct symbol entry
                            final LPSZ offsetFileName = TableUtils.offsetFileName(
                                    path.trimTo(engine.getConfiguration().getRoot().length()).concat(table.getDirectoryName())
                                    , columnName, columnNameTxn);

                            // initialise symbol map memory
                            try (MemoryCMR offsetMem = Vm.getCMRInstance()) {
                                final long offsetMemSize = SymbolMapWriter.keyToOffset(0) + Long.BYTES;
                                offsetMem.of(engine.getConfiguration().getFilesFacade(), offsetFileName, offsetMemSize, offsetMemSize, MemoryTag.NATIVE_METADATA_READER);

                                // get symbol properties
                                column.setSymbolCapacity(offsetMem.getInt(SymbolMapWriter.HEADER_CAPACITY));
                                assert column.getSymbolCapacity() > 0;

                                column.setSymbolCached(offsetMem.getBool(SymbolMapWriter.HEADER_CACHE_ENABLED));
                            }
                        }
                    }
                }

                tables.put(table.getTableName(), table);

                if (infoLog) {
                    LOG.info().$("hydrated metadata [table=").$(table.getTableToken()).I$();
                }
            } catch (CairoException e) {
                dropTable(token); // get rid of stale metadata
                // if can't hydrate and table is not dropped, it's a critical error
                LogRecord root = engine.isTableDropped(token) ? LOG.info() : LOG.critical();
                root.$("could not hydrate metadata [table=").$(table.getTableToken()).I$();
            } finally {
                columnVersionReader.close();
                path.close();
                metaMem.close();
                Misc.free(metaMem);
            }
        }
    }
}
