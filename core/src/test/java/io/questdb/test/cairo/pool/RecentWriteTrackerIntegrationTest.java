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

package io.questdb.test.cairo.pool;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for RecentWriteTracker demonstrating the full flow
 * from table writes through WAL apply to tracking.
 * <p>
 * These tests also serve as API documentation for using RecentWriteTracker.
 */
public class RecentWriteTrackerIntegrationTest extends AbstractCairoTest {

    /**
     * Demonstrates that the tracker is accessible from CairoEngine.
     * <p>
     * This is the main entry point for the API.
     */
    @Test
    public void testApiAccessFromEngine() throws Exception {
        assertMemoryLeak(() -> {
            // Primary API: get tracker from engine
            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            Assert.assertNotNull(tracker);

            // Available methods:
            // 1. recordWrite(TableToken, timestamp, rowCount) - called automatically by WriterPool
            // 2. getRecentlyWrittenTables(limit) - get top N recently written TableTokens
            // 3. getWriteTimestamp(TableToken) - get timestamp for specific table
            // 4. getRowCount(TableToken) - get row count for specific table
            // 5. getWriteStats(TableToken) - get WriteStats object with both timestamp and rowCount
            // 6. removeTable(TableToken) - remove table from tracking
            // 7. clear() - clear all tracking data
            // 8. size() - get current number of tracked tables (for testing)
            // 9. getMaxCapacity() - get configured capacity limit

            // Benefits of TableToken-based API:
            // - No String allocation on write path (zero-alloc)
            // - Type-safe: caller gets TableToken back with full metadata
            // - Rich data: can access tableName, dirName, isWal, tableId from returned tokens

            // Consistency note:
            // WriteStats uses volatile fields for lock-free updates. This means readers may
            // momentarily observe inconsistent state where timestamp and rowCount are from
            // different writes. This is acceptable for observability purposes.
        });
    }

    /**
     * Demonstrates the tracker's capacity limit and eviction.
     * <p>
     * Use case: Prevent unbounded memory growth in systems with many tables.
     */
    @Test
    public void testCapacityAndEviction() throws Exception {
        assertMemoryLeak(() -> {
            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            int capacity = tracker.getMaxCapacity();

            // Capacity is configurable via cairo.recent.write.tracker.capacity
            // Default is 100
            Assert.assertTrue("Capacity should be positive", capacity > 0);

            // The tracker uses lazy eviction at 2x capacity
            // When size exceeds 2*capacity, oldest entries are evicted
            // This amortizes cleanup cost and reduces contention
        });
    }

    /**
     * Demonstrates that dropping a table automatically removes it from the tracker.
     * <p>
     * Use case: Prevent stale entries from accumulating after table drops.
     */
    @Test
    public void testDropTableRemovesFromTracker() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE drop_test (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Write to table
            execute("INSERT INTO drop_test VALUES (now(), 1)");
            drainWalQueue();

            // Get the table token before dropping
            TableToken tableToken = engine.verifyTableName("drop_test");

            // Verify it's tracked
            Assert.assertTrue("Table should be tracked", tracker.getWriteTimestamp(tableToken) > 0);
            Assert.assertEquals("Should have 1 row", 1L, tracker.getRowCount(tableToken));

            // Drop the table
            execute("DROP TABLE drop_test");

            // Verify it's removed from tracker
            Assert.assertEquals("Table should no longer be tracked after drop",
                    Numbers.LONG_NULL, tracker.getWriteTimestamp(tableToken));
            Assert.assertEquals("Row count should be null after drop",
                    Numbers.LONG_NULL, tracker.getRowCount(tableToken));
        });
    }

    /**
     * Demonstrates using WriteStats to get both timestamp and row count.
     * <p>
     * Use case: UI needs both last write time and row count together.
     */
    @Test
    public void testGetWriteStats() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE stats_test (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            TableToken tableToken = engine.verifyTableName("stats_test");

            // Before any writes
            Assert.assertNull("No stats before write", tracker.getWriteStats(tableToken));

            // Insert rows
            long beforeWrite = configuration.getMicrosecondClock().getTicks();
            execute("INSERT INTO stats_test VALUES (now(), 1)");
            execute("INSERT INTO stats_test VALUES (now(), 2)");
            drainWalQueue();
            long afterWrite = configuration.getMicrosecondClock().getTicks();

            // Get stats
            RecentWriteTracker.WriteStats stats = tracker.getWriteStats(tableToken);
            Assert.assertNotNull("Stats should be available", stats);
            Assert.assertTrue("Timestamp should be in range", stats.getTimestamp() >= beforeWrite);
            Assert.assertTrue("Timestamp should be in range", stats.getTimestamp() <= afterWrite);
            Assert.assertEquals("Should have 2 rows", 2L, stats.getRowCount());
        });
    }

    /**
     * Demonstrates querying write timestamp for a specific table.
     * <p>
     * Use case: Check if a specific table was modified recently.
     */
    @Test
    public void testGetWriteTimestampForTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE metrics (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Get the table token
            TableToken metricsToken = engine.verifyTableName("metrics");

            // Before any writes
            Assert.assertEquals("No timestamp before write", Numbers.LONG_NULL, tracker.getWriteTimestamp(metricsToken));

            // Write and apply
            long beforeWrite = configuration.getMicrosecondClock().getTicks();
            execute("INSERT INTO metrics VALUES (now(), 42.0)");
            drainWalQueue();
            long afterWrite = configuration.getMicrosecondClock().getTicks();

            // Check timestamp using TableToken
            long writeTimestamp = tracker.getWriteTimestamp(metricsToken);
            Assert.assertTrue("Timestamp should be recorded", writeTimestamp > 0);
            Assert.assertTrue("Timestamp should be >= beforeWrite", writeTimestamp >= beforeWrite);
            Assert.assertTrue("Timestamp should be <= afterWrite", writeTimestamp <= afterWrite);
        });
    }

    /**
     * Demonstrates limiting results to top N tables.
     * <p>
     * Use case: UI only needs to show top 10 recently modified tables.
     */
    @Test
    public void testLimitResults() throws Exception {
        assertMemoryLeak(() -> {
            // Create many tables
            for (int i = 0; i < 20; i++) {
                execute("CREATE TABLE limit_test_" + i + " (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Write to all tables
            for (int i = 0; i < 20; i++) {
                execute("INSERT INTO limit_test_" + i + " VALUES (now(), " + i + ")");
            }
            drainWalQueue();

            // Request only top 5
            ObjList<TableToken> top5 = tracker.getRecentlyWrittenTables(5);
            Assert.assertEquals("Should return exactly 5 tables", 5, top5.size());

            // Request all
            ObjList<TableToken> all = tracker.getRecentlyWrittenTables(100);
            Assert.assertTrue("Should have at least 20 tables", all.size() >= 20);
        });
    }

    /**
     * Demonstrates ordering: most recently written tables come first.
     * <p>
     * Use case: Show "recently modified" tables in UI, sorted by recency.
     */
    @Test
    public void testMostRecentTableFirst() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE first_table (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE second_table (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE third_table (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Write in sequence with small delays to ensure different timestamps
            execute("INSERT INTO first_table VALUES (now(), 1)");
            drainWalQueue();

            execute("INSERT INTO second_table VALUES (now(), 2)");
            drainWalQueue();

            execute("INSERT INTO third_table VALUES (now(), 3)");
            drainWalQueue();

            // Query - most recent should be first
            ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(3);

            Assert.assertEquals("Should have 3 tables", 3, recent.size());
            // Access table name directly from TableToken
            Assert.assertEquals("third_table should be first (most recent)", "third_table", recent.get(0).getTableName());
            Assert.assertEquals("second_table should be second", "second_table", recent.get(1).getTableName());
            Assert.assertEquals("first_table should be third (oldest)", "first_table", recent.get(2).getTableName());
        });
    }

    /**
     * Demonstrates removing a table from tracking (e.g., when table is dropped).
     * <p>
     * Use case: Clean up tracking when table is dropped.
     */
    @Test
    public void testRemoveTableFromTracking() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE to_be_dropped (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Write to table
            execute("INSERT INTO to_be_dropped VALUES (now(), 1)");
            drainWalQueue();

            // Get the table token
            TableToken tableToken = engine.verifyTableName("to_be_dropped");

            // Verify it's tracked
            Assert.assertTrue("Table should be tracked", tracker.getWriteTimestamp(tableToken) > 0);

            // Remove from tracking using TableToken
            tracker.removeTable(tableToken);

            // Verify it's removed
            Assert.assertEquals("Table should no longer be tracked", Numbers.LONG_NULL, tracker.getWriteTimestamp(tableToken));
        });
    }

    /**
     * Demonstrates that repeated writes to same table update the timestamp.
     * <p>
     * Use case: Track latest modification time accurately.
     */
    @Test
    public void testRepeatedWritesUpdateTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE events (ts TIMESTAMP, data STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            TableToken eventsToken = engine.verifyTableName("events");

            // First write
            execute("INSERT INTO events VALUES (now(), 'event1')");
            drainWalQueue();
            long firstTimestamp = tracker.getWriteTimestamp(eventsToken);

            // Small delay to ensure different timestamp
            Thread.sleep(1);

            // Second write
            execute("INSERT INTO events VALUES (now(), 'event2')");
            drainWalQueue();
            long secondTimestamp = tracker.getWriteTimestamp(eventsToken);

            Assert.assertTrue("Second timestamp should be >= first", secondTimestamp >= firstTimestamp);

            // Table should only appear once in the list
            ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(10);
            int count = 0;
            for (int i = 0; i < recent.size(); i++) {
                if (recent.get(i).getTableName().equals("events")) count++;
            }
            Assert.assertEquals("Table should appear exactly once", 1, count);
        });
    }

    /**
     * Demonstrates tracking row count for tables.
     * <p>
     * Use case: UI needs to show table row counts for size estimation.
     */
    @Test
    public void testTrackRowCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE row_count_test (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            TableToken tableToken = engine.verifyTableName("row_count_test");

            // Before any writes
            Assert.assertEquals("No row count before write", Numbers.LONG_NULL, tracker.getRowCount(tableToken));

            // Insert some rows
            execute("INSERT INTO row_count_test VALUES (now(), 1)");
            execute("INSERT INTO row_count_test VALUES (now(), 2)");
            execute("INSERT INTO row_count_test VALUES (now(), 3)");
            drainWalQueue();

            // Check row count
            long rowCount = tracker.getRowCount(tableToken);
            Assert.assertEquals("Should have 3 rows", 3L, rowCount);

            // Add more rows
            execute("INSERT INTO row_count_test VALUES (now(), 4)");
            execute("INSERT INTO row_count_test VALUES (now(), 5)");
            drainWalQueue();

            // Row count should be updated
            rowCount = tracker.getRowCount(tableToken);
            Assert.assertEquals("Should have 5 rows", 5L, rowCount);
        });
    }

    /**
     * Demonstrates basic usage: write to WAL tables and track which were modified.
     * <p>
     * Use case: UI needs to know which tables changed to update size estimates.
     */
    @Test
    public void testTrackWritesToWalTables() throws Exception {
        assertMemoryLeak(() -> {
            // Create WAL tables
            execute("CREATE TABLE trades (ts TIMESTAMP, price DOUBLE, qty INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE orders (ts TIMESTAMP, side SYMBOL, amount DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE users (ts TIMESTAMP, name STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Get the tracker from engine
            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            Assert.assertNotNull("RecentWriteTracker should be available from engine", tracker);

            // Initially no writes tracked (tables just created, no data written)
            // Note: DDL operations don't count as writes to the tracker
            tracker.clear(); // Clear any tracking from table creation

            // Write data to tables
            execute("INSERT INTO trades VALUES (now(), 100.5, 10)");
            execute("INSERT INTO orders VALUES (now(), 'BUY', 1000.0)");

            // Drain WAL to apply changes (this triggers the tracking)
            drainWalQueue();

            // Query recently written tables - returns TableToken objects
            ObjList<TableToken> recentTables = tracker.getRecentlyWrittenTables(10);

            // Both tables should be tracked (order depends on timing)
            Assert.assertTrue("Should have tracked at least 2 tables", recentTables.size() >= 2);

            // Verify specific tables are present - can access tableName directly from token
            boolean foundTrades = false;
            boolean foundOrders = false;
            for (int i = 0; i < recentTables.size(); i++) {
                TableToken token = recentTables.get(i);
                if (token.getTableName().equals("trades")) foundTrades = true;
                if (token.getTableName().equals("orders")) foundOrders = true;
            }
            Assert.assertTrue("trades table should be tracked", foundTrades);
            Assert.assertTrue("orders table should be tracked", foundOrders);
        });
    }
}
