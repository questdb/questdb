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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cutlass.line.tcp.ClientSymbolCache;
import io.questdb.cutlass.line.tcp.ConnectionSymbolCache;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ConnectionSymbolCache}.
 */
public class ConnectionSymbolCacheTest {

    @Test
    public void testGetCache_createsCacheOnFirstAccess() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        // Initially no tables
        assertEquals(0, connCache.getTableCount());

        // Get cache for table 1, column 0
        ClientSymbolCache cache = connCache.getCache(1L, 0);

        assertNotNull(cache);
        assertEquals(1, connCache.getTableCount());
        assertEquals(1, connCache.getColumnCacheCount(1L));
    }

    @Test
    public void testGetCache_returnsSameCacheOnSubsequentAccess() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        ClientSymbolCache cache1 = connCache.getCache(1L, 0);
        ClientSymbolCache cache2 = connCache.getCache(1L, 0);

        // Should return the same cache instance
        assertSame(cache1, cache2);
    }

    @Test
    public void testGetCache_createsIndependentCachesPerColumn() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        ClientSymbolCache cache0 = connCache.getCache(1L, 0);
        ClientSymbolCache cache1 = connCache.getCache(1L, 1);
        ClientSymbolCache cache2 = connCache.getCache(1L, 2);

        // Each column should have its own cache
        assertNotSame(cache0, cache1);
        assertNotSame(cache1, cache2);
        assertNotSame(cache0, cache2);

        // All caches should be independent
        cache0.put(0, 10);
        cache1.put(0, 20);
        cache2.put(0, 30);

        assertEquals(10, cache0.get(0));
        assertEquals(20, cache1.get(0));
        assertEquals(30, cache2.get(0));

        assertEquals(3, connCache.getColumnCacheCount(1L));
    }

    @Test
    public void testGetCache_createsIndependentCachesPerTable() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        ClientSymbolCache table1Cache = connCache.getCache(1L, 0);
        ClientSymbolCache table2Cache = connCache.getCache(2L, 0);
        ClientSymbolCache table3Cache = connCache.getCache(3L, 0);

        // Each table should have its own cache
        assertNotSame(table1Cache, table2Cache);
        assertNotSame(table2Cache, table3Cache);

        assertEquals(3, connCache.getTableCount());
    }

    @Test
    public void testInvalidateTable_clearsOnlyThatTable() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        // Set up caches for two tables
        ClientSymbolCache table1Col0 = connCache.getCache(1L, 0);
        ClientSymbolCache table1Col1 = connCache.getCache(1L, 1);
        ClientSymbolCache table2Col0 = connCache.getCache(2L, 0);

        // Add entries to all caches
        table1Col0.put(0, 10);
        table1Col1.put(0, 20);
        table2Col0.put(0, 30);

        // Invalidate table 1 only
        connCache.invalidateTable(1L);

        // Table 1 caches should be cleared
        assertEquals(ClientSymbolCache.NO_ENTRY, table1Col0.get(0));
        assertEquals(ClientSymbolCache.NO_ENTRY, table1Col1.get(0));

        // Table 2 cache should be intact
        assertEquals(30, table2Col0.get(0));
    }

    @Test
    public void testInvalidateTable_nonExistentTable_noOp() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        ClientSymbolCache cache = connCache.getCache(1L, 0);
        cache.put(0, 10);

        // Invalidate a table that doesn't exist
        connCache.invalidateTable(999L);

        // Original cache should be unaffected
        assertEquals(10, cache.get(0));
    }

    @Test
    public void testClear_removesAllTables() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        // Set up multiple tables
        ClientSymbolCache table1Cache = connCache.getCache(1L, 0);
        ClientSymbolCache table2Cache = connCache.getCache(2L, 0);
        ClientSymbolCache table3Cache = connCache.getCache(3L, 0);

        table1Cache.put(0, 10);
        table2Cache.put(0, 20);
        table3Cache.put(0, 30);

        assertEquals(3, connCache.getTableCount());

        // Clear everything
        connCache.clear();

        assertEquals(0, connCache.getTableCount());

        // Getting cache again should create new instances
        ClientSymbolCache newCache1 = connCache.getCache(1L, 0);
        assertNotSame(table1Cache, newCache1);
        assertEquals(ClientSymbolCache.NO_ENTRY, newCache1.get(0));
    }

    @Test
    public void testStatistics_recordHitAndMiss() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        assertEquals(0, connCache.getCacheHits());
        assertEquals(0, connCache.getCacheMisses());

        connCache.recordHit();
        connCache.recordHit();
        connCache.recordMiss();

        assertEquals(2, connCache.getCacheHits());
        assertEquals(1, connCache.getCacheMisses());
    }

    @Test
    public void testStatistics_hitRatePercent() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        // No lookups - rate should be 0
        assertEquals(0.0, connCache.getHitRatePercent(), 0.001);

        // 3 hits, 1 miss = 75%
        connCache.recordHit();
        connCache.recordHit();
        connCache.recordHit();
        connCache.recordMiss();

        assertEquals(75.0, connCache.getHitRatePercent(), 0.001);
    }

    @Test
    public void testStatistics_clearedOnClear() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        connCache.recordHit();
        connCache.recordMiss();

        connCache.clear();

        assertEquals(0, connCache.getCacheHits());
        assertEquals(0, connCache.getCacheMisses());
    }

    @Test
    public void testGetColumnCacheCount_sparseColumns() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        // Create caches for non-contiguous column indices
        connCache.getCache(1L, 0);
        connCache.getCache(1L, 5);
        connCache.getCache(1L, 10);

        // Should count only the actually created caches
        assertEquals(3, connCache.getColumnCacheCount(1L));
    }

    @Test
    public void testGetColumnCacheCount_nonExistentTable() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        assertEquals(0, connCache.getColumnCacheCount(999L));
    }

    @Test
    public void testLargeColumnIndex() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        // Should handle large column indices
        ClientSymbolCache cache = connCache.getCache(1L, 1000);
        assertNotNull(cache);

        cache.put(0, 42);
        assertEquals(42, cache.get(0));
    }

    @Test
    public void testMultipleTablesMultipleColumns() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        // Create a grid of tables and columns
        for (int table = 1; table <= 5; table++) {
            for (int col = 0; col < 3; col++) {
                ClientSymbolCache cache = connCache.getCache(table, col);
                // Use table*100 + col as a unique value
                cache.put(0, table * 100 + col);
            }
        }

        assertEquals(5, connCache.getTableCount());

        // Verify all values are independent
        for (int table = 1; table <= 5; table++) {
            assertEquals(3, connCache.getColumnCacheCount(table));
            for (int col = 0; col < 3; col++) {
                ClientSymbolCache cache = connCache.getCache(table, col);
                assertEquals(table * 100 + col, cache.get(0));
            }
        }
    }

    @Test
    public void testClearPreservesStructureButClearsData() {
        ConnectionSymbolCache connCache = new ConnectionSymbolCache();

        // Get references before clear
        ClientSymbolCache cache1 = connCache.getCache(1L, 0);
        cache1.put(0, 10);

        // Clear
        connCache.clear();

        // Get a new cache - should be different instance
        ClientSymbolCache cache2 = connCache.getCache(1L, 0);
        assertNotSame(cache1, cache2);

        // New cache should be empty
        assertEquals(ClientSymbolCache.NO_ENTRY, cache2.get(0));
    }
}
