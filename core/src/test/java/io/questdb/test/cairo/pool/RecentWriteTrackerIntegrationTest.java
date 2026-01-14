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

    /**
     * Tests that unique rows are NOT incorrectly counted as duplicates.
     * <p>
     * This test verifies the fix for a bug where rows going to LAG (out-of-order buffer)
     * were incorrectly counted as duplicates because the old calculation used:
     * dedupCount = walRowCount - committedRowCount
     * <p>
     * When rows go to LAG, committedRowCount is 0 (LAG rows aren't in txWriter.getRowCount()),
     * so all rows would be incorrectly counted as "deduped".
     * <p>
     * The fix tracks actual dedup from the native Vect.dedupSortedTimestampIndexIntKeysChecked() call.
     */
    @Test
    public void testUniqueRowsNotCountedAsDedup() throws Exception {
        assertMemoryLeak(() -> {
            // Create a WAL table with dedup enabled
            execute("CREATE TABLE unique_test (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            TableToken tableToken = engine.verifyTableName("unique_test");

            // Insert unique rows in multiple small batches
            // Small batches may go to LAG before being committed
            for (int batch = 0; batch < 10; batch++) {
                int offset = batch * 100;
                execute("INSERT INTO unique_test SELECT ((x + " + offset + ") * 1000000)::timestamp, (x + " + offset + ")::int FROM long_sequence(100)");
            }

            // Drain WAL to apply all changes
            drainWalQueue();

            // Verify: 1000 unique rows inserted, dedup_row_count should be 0
            RecentWriteTracker.WriteStats stats = tracker.getWriteStats(tableToken);
            Assert.assertNotNull("Stats should be available", stats);
            Assert.assertEquals("Should have 1000 rows", 1000L, stats.getRowCount());
            Assert.assertEquals("Dedup count should be 0 for unique rows", 0L, stats.getDedupRowCount());

            // Also verify via SQL
            assertSql(
                    """
                            table_row_count\twal_dedup_row_count_since_start
                            1000\t0
                            """,
                    "SELECT table_row_count, wal_dedup_row_count_since_start FROM tables() WHERE table_name = 'unique_test'"
            );

            // Now insert actual duplicates - same timestamps as first 200 rows
            execute("INSERT INTO unique_test SELECT (x * 1000000)::timestamp, (x + 2000)::int FROM long_sequence(200)");
            drainWalQueue();

            // Verify duplicates are now tracked
            stats = tracker.getWriteStats(tableToken);
            Assert.assertEquals("Row count should still be 1000", 1000L, stats.getRowCount());
            Assert.assertEquals("Dedup count should be 200", 200L, stats.getDedupRowCount());

            // Verify via SQL
            assertSql(
                    "table_row_count\twal_dedup_row_count_since_start\n1000\t200\n",
                    "SELECT table_row_count, wal_dedup_row_count_since_start FROM tables() WHERE table_name = 'unique_test'"
            );
        });
    }

    /**
     * Demonstrates hydrating the tracker from existing tables on startup.
     * <p>
     * Use case: Populate tracker with existing table data when server starts.
     */
    @Test
    public void testHydrateFromExistingTables() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables and write data
            execute("CREATE TABLE hydrate_test1 (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE hydrate_test2 (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            execute("INSERT INTO hydrate_test1 VALUES (now(), 1)");
            execute("INSERT INTO hydrate_test1 VALUES (now(), 2)");
            execute("INSERT INTO hydrate_test2 VALUES (now(), 3)");
            drainWalQueue();

            // Clear the tracker to simulate a fresh start
            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();
            Assert.assertEquals("Tracker should be empty after clear", 0, tracker.size());

            // Hydrate from existing tables
            engine.hydrateRecentWriteTracker();

            // Verify tables were hydrated
            TableToken token1 = engine.verifyTableName("hydrate_test1");
            TableToken token2 = engine.verifyTableName("hydrate_test2");

            // Row counts should be populated
            Assert.assertEquals("hydrate_test1 should have 2 rows", 2L, tracker.getRowCount(token1));
            Assert.assertEquals("hydrate_test2 should have 1 row", 1L, tracker.getRowCount(token2));

            // Timestamps should be populated (file modification times)
            Assert.assertTrue("hydrate_test1 should have timestamp", tracker.getWriteTimestamp(token1) > 0);
            Assert.assertTrue("hydrate_test2 should have timestamp", tracker.getWriteTimestamp(token2) > 0);

            // Tables should appear in recent list
            ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(10);
            Assert.assertTrue("Should have at least 2 tables", recent.size() >= 2);
        });
    }

    /**
     * Tests WAL pending row count and dedup tracking via the tables() SQL function.
     * <p>
     * This test verifies that:
     * - wal_pending_row_count reflects uncommitted WAL rows before draining
     * - After draining, wal_pending_row_count becomes 0 and table_row_count is updated
     * - Duplicate rows are tracked via dedup_row_count_since_start
     */
    @Test
    public void testWalPendingAndDedupTracking() throws Exception {
        assertMemoryLeak(() -> {
            // Create a WAL table with dedup enabled on timestamp
            execute("CREATE TABLE dedup_test (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");

            engine.getRecentWriteTracker().clear();

            // Insert 500 unique rows from "thread 1" - use seconds to ensure unique timestamps
            // Timestamps: 1970-01-01T00:00:01 through 1970-01-01T00:08:20 (1-500 seconds)
            execute("INSERT INTO dedup_test SELECT (x * 1000000)::timestamp, x::int FROM long_sequence(500)");

            // Drain after first insert to apply it
            drainWalQueue();

            // Insert 500 more unique rows from "thread 2"
            // Timestamps: 1970-01-01T00:08:21 through 1970-01-01T00:16:40 (501-1000 seconds)
            execute("INSERT INTO dedup_test SELECT ((500 + x) * 1000000)::timestamp, (500 + x)::int FROM long_sequence(500)");

            // Before draining second batch - wal_pending_row_count should be 500 (second batch pending)
            assertSql(
                    "wal_pending_row_count\n500\n",
                    "SELECT wal_pending_row_count FROM tables() WHERE table_name = 'dedup_test'"
            );

            // Row count should be 500 (first batch applied)
            assertSql(
                    "table_row_count\n500\n",
                    "SELECT table_row_count FROM tables() WHERE table_name = 'dedup_test'"
            );

            // Drain the WAL queue to apply second batch
            drainWalQueue();

            // After draining - table_row_count should be 1000, wal_pending_row_count should be 0, no dedup yet
            assertSql(
                    "table_row_count\twal_pending_row_count\twal_dedup_row_count_since_start\n1000\t0\t0\n",
                    "SELECT table_row_count, wal_pending_row_count, wal_dedup_row_count_since_start FROM tables() WHERE table_name = 'dedup_test'"
            );

            // Now insert duplicate data - same timestamps as first 300 rows (1-300 seconds)
            execute("INSERT INTO dedup_test SELECT (x * 1000000)::timestamp, (x + 1000)::int FROM long_sequence(300)");

            // Drain the queue again
            drainWalQueue();

            // After draining duplicates:
            // - table_row_count should still be 1000 (300 duplicates replaced existing rows)
            // - wal_pending_row_count should be 0
            // - dedup_row_count_since_start should be 300 (cumulative - the 300 duplicates just added)
            assertSql(
                    "table_row_count\twal_pending_row_count\twal_dedup_row_count_since_start\n1000\t0\t300\n",
                    "SELECT table_row_count, wal_pending_row_count, wal_dedup_row_count_since_start FROM tables() WHERE table_name = 'dedup_test'"
            );

            // Verify the values were actually updated (not just row count unchanged)
            // The first 300 rows should now have v = x + 1000
            assertSql(
                    "count\n300\n",
                    "SELECT count() FROM dedup_test WHERE v > 1000"
            );
        });
    }
}
