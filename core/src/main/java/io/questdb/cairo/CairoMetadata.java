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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CairoMetadata {
    private static final Log LOG = LogFactory.getLog(CairoMetadata.class);
    private final CairoEngine engine;
    private final SimpleReadWriteLock lock = new SimpleReadWriteLock();
    private final ThreadLocal<CairoMetadataReader> reader = ThreadLocal.withInitial(CairoMetadataReader::new);
    private final HashMap<CharSequence, CairoTable> tables = new HashMap<>();
    private final CairoMetadataWriter writer = new CairoMetadataWriter();
    private long version;

    public CairoMetadata(CairoEngine engine) {
        this.engine = engine;
    }

    public long getVersion() {
        return version;
    }

    public CairoMetadataRO read() {
        lock.readLock().lock();
        return reader.get();
    }

    public CairoMetadataRW write() {
        lock.writeLock().lock();
        version++;
        return writer;
    }

    interface CairoMetadataRO {
        @Nullable
        CairoTable getTable(@NotNull TableToken tableToken);

        int getTableCount();

        @Nullable
        CairoTable getVisibleTable(@NotNull TableToken tableToken);

        void snapshotCreate(HashMap<CharSequence, CairoTable> localCache);

        void snapshotRefresh(HashMap<CharSequence, CairoTable> localCache);

        String toString0();
    }

    interface CairoMetadataRW extends CairoMetadataRO {
        void clear();

        void dropTable(@NotNull TableToken tableToken);

        void dropTable(@NotNull CharSequence tableName);
    }

    private class CairoMetadataReader implements CairoMetadataRO, Closeable {
        @Override
        public void close() {
            lock.readLock().unlock();
        }

        public @Nullable CairoTable getTable(@NotNull TableToken tableToken) {
            return tables.get(tableToken.getTableName());
        }

        @Override
        public int getTableCount() {
            return tables.size();
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
        public void snapshotRefresh(HashMap<CharSequence, CairoTable> localCache) {
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
        }

        /**
         * For debug printing the metadata object.
         */
        @TestOnly
        public String toString0() {
            StringSink sink = Misc.getThreadLocalSink();
            sink.put("MetadataCache [");
            sink.put("tableCount=").put(tables.size()).put(']');
            sink.put('\n');

            for (CairoTable table : tables.values()) {
                sink.put('\t');
                table.toSink(sink);
                sink.put('\n');
            }
            close();
            return sink.toString();
        }
    }

    private class CairoMetadataWriter extends CairoMetadataReader implements Closeable, CairoMetadataRW {

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
            }
            table = super.getTable(tableToken);
            return table;
        }
    }
}
