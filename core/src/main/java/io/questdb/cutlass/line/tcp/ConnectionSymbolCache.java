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

package io.questdb.cutlass.line.tcp;

import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;

/**
 * Manages symbol ID caches for all (table, column) pairs in a connection.
 * <p>
 * This class provides a hierarchical cache structure:
 * <pre>
 *   Connection
 *     └── Table (by tableToken)
 *           └── Column (by columnIndex)
 *                 └── ClientSymbolCache (clientId → tableSymbolId)
 * </pre>
 * <p>
 * The cache is designed to:
 * <ul>
 *   <li>Create caches lazily on first access</li>
 *   <li>Support invalidation at table level (e.g., on segment rollover)</li>
 *   <li>Clear all caches on connection close</li>
 * </ul>
 * <p>
 * Thread safety: This class is NOT thread-safe. It should only be accessed
 * from the connection's processing thread.
 */
public class ConnectionSymbolCache {

    private static final int DEFAULT_TABLE_CAPACITY = 8;
    private static final int DEFAULT_COLUMN_CAPACITY = 8;

    // Map: tableToken → list of caches (indexed by columnIndex)
    private final LongObjHashMap<ObjList<ClientSymbolCache>> tableColumnCaches;

    // Statistics
    private long cacheHits;
    private long cacheMisses;

    /**
     * Creates a new connection symbol cache.
     */
    public ConnectionSymbolCache() {
        this.tableColumnCaches = new LongObjHashMap<>(DEFAULT_TABLE_CAPACITY);
        this.cacheHits = 0;
        this.cacheMisses = 0;
    }

    /**
     * Gets or creates a cache for the specified (table, column) pair.
     *
     * @param tableToken  the table's unique token
     * @param columnIndex the column's index within the table
     * @return the cache for this (table, column) pair
     */
    public ClientSymbolCache getCache(long tableToken, int columnIndex) {
        ObjList<ClientSymbolCache> columnCaches = tableColumnCaches.get(tableToken);

        if (columnCaches == null) {
            // First access for this table - create column cache list
            columnCaches = new ObjList<>(DEFAULT_COLUMN_CAPACITY);
            tableColumnCaches.put(tableToken, columnCaches);
        }

        // Ensure capacity for this column index
        while (columnCaches.size() <= columnIndex) {
            columnCaches.add(null);
        }

        ClientSymbolCache cache = columnCaches.getQuick(columnIndex);
        if (cache == null) {
            // First access for this column - create cache
            cache = new ClientSymbolCache();
            columnCaches.setQuick(columnIndex, cache);
        }

        return cache;
    }

    /**
     * Invalidates (clears) the cache for a specific table.
     * <p>
     * This should be called when the table's symbol watermark changes,
     * e.g., after segment rollover.
     *
     * @param tableToken the table's unique token
     */
    public void invalidateTable(long tableToken) {
        ObjList<ClientSymbolCache> columnCaches = tableColumnCaches.get(tableToken);
        if (columnCaches != null) {
            for (int i = 0, n = columnCaches.size(); i < n; i++) {
                ClientSymbolCache cache = columnCaches.getQuick(i);
                if (cache != null) {
                    cache.clear();
                }
            }
        }
    }

    /**
     * Clears all caches for all tables.
     * <p>
     * This should be called when the connection is closed.
     */
    public void clear() {
        tableColumnCaches.forEach((tableToken, columnCaches) -> {
            if (columnCaches != null) {
                for (int i = 0, n = columnCaches.size(); i < n; i++) {
                    ClientSymbolCache cache = columnCaches.getQuick(i);
                    if (cache != null) {
                        cache.clear();
                    }
                }
                columnCaches.clear();
            }
        });
        tableColumnCaches.clear();
        cacheHits = 0;
        cacheMisses = 0;
    }

    /**
     * Records a cache hit for statistics.
     */
    public void recordHit() {
        cacheHits++;
    }

    /**
     * Records a cache miss for statistics.
     */
    public void recordMiss() {
        cacheMisses++;
    }

    /**
     * Returns the total number of cache hits.
     */
    public long getCacheHits() {
        return cacheHits;
    }

    /**
     * Returns the total number of cache misses.
     */
    public long getCacheMisses() {
        return cacheMisses;
    }

    /**
     * Returns the cache hit rate as a percentage (0.0 to 100.0).
     * Returns 0.0 if no lookups have been recorded.
     */
    public double getHitRatePercent() {
        long total = cacheHits + cacheMisses;
        if (total == 0) {
            return 0.0;
        }
        return (cacheHits * 100.0) / total;
    }

    /**
     * Returns the number of tables currently cached.
     */
    public int getTableCount() {
        return tableColumnCaches.size();
    }

    /**
     * Returns the number of column caches for a specific table.
     *
     * @param tableToken the table's unique token
     * @return the number of column caches, or 0 if table not cached
     */
    public int getColumnCacheCount(long tableToken) {
        ObjList<ClientSymbolCache> columnCaches = tableColumnCaches.get(tableToken);
        if (columnCaches == null) {
            return 0;
        }
        int count = 0;
        for (int i = 0, n = columnCaches.size(); i < n; i++) {
            if (columnCaches.getQuick(i) != null) {
                count++;
            }
        }
        return count;
    }
}
