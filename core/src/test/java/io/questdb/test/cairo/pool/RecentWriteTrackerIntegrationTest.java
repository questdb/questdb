/*+*****************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
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
            // 1. recordWrite(TableToken, timestamp, rowCount, writerTxn) - called automatically by WriterPool
            // 2. getWriteTimestamp(TableToken) - get timestamp for specific table
            // 3. getRowCount(TableToken) - get row count for specific table
            // 4. getWriteStats(TableToken) - get WriteStats object with both timestamp and rowCount
            // 5. removeTable(TableToken) - remove table from tracking
            // 6. clear() - clear all tracking data
            // 7. size() - get current number of tracked tables (for testing)
            // 8. getMaxCapacity() - get configured capacity limit

            // Benefits of TableToken-based API:
            // - No String allocation on write path (zero-alloc)
            // - Type-safe: callers use TableToken keys with full metadata
            // - Rich data: tableName, dirName, isWal, tableId are available from the key

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
            Assert.assertTrue("Capacity should be positive", capacity > 0);

            // The tracker uses lazy eviction at 2x capacity
            // When size exceeds 2*capacity, oldest entries are evicted
            // This amortizes cleanup cost and reduces contention
        });
    }

    /**
     * Sister bug to the physical-row inflation: dedupRowsRemovedSinceLastCommit
     * follows the same reset pattern as physicallyWrittenRowsSinceLastCommit
     * (only reset by processWalBlock at the start of a real data write). Skipped
     * data iterations do not reset it, so recordWalProcessed reads the prior
     * iteration's dedup count and adds it to stats.dedupRowCount again. This
     * inflates the wal_dedup_row_count_since_start column in tables(). The
     * test deliberately drives one drain that contains:
     * iter 1: INSERT 100 unique rows in day-1 partition (no dedup, baseline)
     * iter 2: INSERT 200 duplicates (dedupRowsRemovedSinceLastCommit -> 200)
     * iter 3: INSERT 1 row in day-2 partition that the next REPLACE_RANGE covers
     * -> trySkipWalTransactions, counter still 200, recordWalProcessed
     * adds 200 a second time
     * iter 4: REPLACE_RANGE covering day-2
     * Expected dedup count: 200. With bug: 400.
     */
    @Test
    public void testDedupCountNotInflatedBySkippedReplaceRange() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, 1);
            execute("CREATE TABLE wa_dedup (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            engine.getRecentWriteTracker().clear();

            TableToken tt = engine.verifyTableName("wa_dedup");
            long day1 = 86_400_000_000L;
            long day2 = 172_800_000_000L;

            // iter 1: 100 unique rows in day 1 (no dedup yet).
            try (WalWriter ww = engine.getWalWriter(tt)) {
                for (long i = 0; i < 100; i++) {
                    TableWriter.Row row = ww.newRow(day1 + i * 1_000_000L);
                    row.putInt(1, (int) i);
                    row.append();
                }
                ww.commit();
            }
            drainWalQueue();
            engine.getRecentWriteTracker().clear();

            // iter 2: 200 duplicates of day 1 timestamps (dedup counter -> 200).
            try (WalWriter ww = engine.getWalWriter(tt)) {
                for (long i = 0; i < 200; i++) {
                    TableWriter.Row row = ww.newRow(day1 + (i % 100) * 1_000_000L);
                    row.putInt(1, 999);
                    row.append();
                }
                ww.commit();
            }

            // iter 3: 1 row in day 2 that will be covered by the upcoming REPLACE_RANGE.
            try (WalWriter ww = engine.getWalWriter(tt)) {
                TableWriter.Row row = ww.newRow(day2);
                row.putInt(1, 0);
                row.append();
                ww.commit();
            }

            // iter 4: REPLACE_RANGE covering day 2.
            try (WalWriter ww = engine.getWalWriter(tt)) {
                ww.commitWithParams(day2, day2 + 200_000_000L, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
            }

            drainWalQueue();

            assertQuery("SELECT wal_dedup_row_count_since_start FROM tables() WHERE table_name = 'wa_dedup'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("wal_dedup_row_count_since_start\n200\n");
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

            // Second write
            execute("INSERT INTO events VALUES (now(), 'event2')");
            drainWalQueue();
            long secondTimestamp = tracker.getWriteTimestamp(eventsToken);

            Assert.assertTrue("Second timestamp should be >= first", secondTimestamp >= firstTimestamp);

            RecentWriteTracker.WriteStats stats = tracker.getWriteStats(eventsToken);
            Assert.assertNotNull("Table should remain tracked", stats);
            Assert.assertEquals("Timestamp should match latest write", secondTimestamp, stats.getTimestamp());
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
     * Demonstrates basic usage: write to WAL tables and track table statistics.
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

            TableToken tradesToken = engine.verifyTableName("trades");
            TableToken ordersToken = engine.verifyTableName("orders");
            TableToken usersToken = engine.verifyTableName("users");
            Assert.assertNotNull("trades table should be tracked", tracker.getWriteStats(tradesToken));
            Assert.assertNotNull("orders table should be tracked", tracker.getWriteStats(ordersToken));
            Assert.assertNull("users table should not be tracked without writes", tracker.getWriteStats(usersToken));
            Assert.assertEquals("trades row count should be tracked", 1L, tracker.getRowCount(tradesToken));
            Assert.assertEquals("orders row count should be tracked", 1L, tracker.getRowCount(ordersToken));
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
            assertQuery("SELECT table_row_count, wal_dedup_row_count_since_start FROM tables() WHERE table_name = 'unique_test'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            table_row_count\twal_dedup_row_count_since_start
                            1000\t0
                            """);

            // Now insert actual duplicates - same timestamps as first 200 rows
            execute("INSERT INTO unique_test SELECT (x * 1000000)::timestamp, (x + 2000)::int FROM long_sequence(200)");
            drainWalQueue();

            // Verify duplicates are now tracked
            stats = tracker.getWriteStats(tableToken);
            Assert.assertEquals("Row count should still be 1000", 1000L, stats.getRowCount());
            Assert.assertEquals("Dedup count should be 200", 200L, stats.getDedupRowCount());

            // Verify via SQL
            assertQuery("SELECT table_row_count, wal_dedup_row_count_since_start FROM tables() WHERE table_name = 'unique_test'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("table_row_count\twal_dedup_row_count_since_start\n1000\t200\n");
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

            Assert.assertNotNull("hydrate_test1 should be tracked", tracker.getWriteStats(token1));
            Assert.assertNotNull("hydrate_test2 should be tracked", tracker.getWriteStats(token2));
        });
    }

    @Test
    public void testHydrateTimestampNsDoesNotUseNativeTimestampAsEvictionActivity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE hydrate_ns_activity (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO hydrate_ns_activity VALUES ('2024-01-01T00:00:00.000000123Z'::TIMESTAMP_NS, 1)");
            drainWalQueue();

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();
            engine.hydrateRecentWriteTracker();

            TableToken hydratedToken = engine.verifyTableName("hydrate_ns_activity");
            RecentWriteTracker.WriteStats hydratedStats = tracker.getWriteStats(hydratedToken);
            Assert.assertNotNull(hydratedStats);
            Assert.assertTrue(hydratedStats.getLastWalTimestamp() > 1_000_000_000_000_000_000L);

            int capacity = tracker.getMaxCapacity();
            long newerActivityTimestamp = configuration.getMicrosecondClock().getTicks() + 1_000_000_000L;
            TableToken liveToken = new TableToken("live_activity", "live_activity", null, 100_000, false, false, false);
            tracker.recordWrite(liveToken, newerActivityTimestamp, 1L, 1L);
            for (int i = 0; i < capacity - 1; i++) {
                int tableId = 101_000 + i;
                String tableName = "protected_activity_" + i;
                tracker.recordWrite(
                        new TableToken(tableName, tableName, null, tableId, false, false, false),
                        newerActivityTimestamp + i + 1L,
                        1L,
                        1L
                );
            }

            for (int i = 0; i < capacity; i++) {
                int tableId = 202_000 + i;
                String tableName = "old_activity_" + i;
                tracker.recordWrite(
                        new TableToken(tableName, tableName, null, tableId, false, false, false),
                        i + 1L,
                        1L,
                        1L
                );
            }

            Assert.assertEquals(capacity, tracker.size());
            Assert.assertNotNull("live wall-clock activity should survive eviction", tracker.getWriteStats(liveToken));
            Assert.assertNull("hydrated native nanosecond data timestamp must not pin eviction activity", tracker.getWriteStats(hydratedToken));
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
            assertQuery("SELECT wal_pending_row_count FROM tables() WHERE table_name = 'dedup_test'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("wal_pending_row_count\n500\n");

            // Row count should be 500 (first batch applied)
            assertQuery("SELECT table_row_count FROM tables() WHERE table_name = 'dedup_test'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("table_row_count\n500\n");

            // Drain the WAL queue to apply second batch
            drainWalQueue();

            // After draining - table_row_count should be 1000, wal_pending_row_count should be 0, no dedup yet
            assertQuery("SELECT table_row_count, wal_pending_row_count, wal_dedup_row_count_since_start FROM tables() WHERE table_name = 'dedup_test'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("table_row_count\twal_pending_row_count\twal_dedup_row_count_since_start\n1000\t0\t0\n");

            // Now insert duplicate data - same timestamps as first 300 rows (1-300 seconds)
            execute("INSERT INTO dedup_test SELECT (x * 1000000)::timestamp, (x + 1000)::int FROM long_sequence(300)");

            // Drain the queue again
            drainWalQueue();

            // After draining duplicates:
            // - table_row_count should still be 1000 (300 duplicates replaced existing rows)
            // - wal_pending_row_count should be 0
            // - dedup_row_count_since_start should be 300 (cumulative - the 300 duplicates just added)
            assertQuery("SELECT table_row_count, wal_pending_row_count, wal_dedup_row_count_since_start FROM tables() WHERE table_name = 'dedup_test'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("table_row_count\twal_pending_row_count\twal_dedup_row_count_since_start\n1000\t0\t300\n");

            // Verify the values were actually updated (not just row count unchanged)
            // The first 300 rows should now have v = x + 1000
            assertQuery("SELECT count() FROM dedup_test WHERE v > 1000")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n300\n");
        });
    }

    /**
     * INSERT followed by an UPDATE that matches no rows, in the same WAL apply
     * job. The INSERT is append-only (per-commit amp 1.0). The UPDATE goes
     * through the SQL branch and matches zero rows, so it does not write any
     * physical rows and never triggers an internal commit on the writer. With
     * the bug, the SQL iteration re-reads the INSERT's counter and inflates
     * physicalRowsAdded; the recorded per-job amplification ends up ~2.0
     * instead of ~1.0. With the fix, both iterations stay at ~1.0.
     */
    @Test
    public void testWriteAmpNotInflatedByInterleavedUpdate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wa_update (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            engine.getRecentWriteTracker().clear();

            // Seed a partition with many rows so the UPDATE rewrites a large partition.
            execute("INSERT INTO wa_update SELECT (86400000000 + x * 1000000)::timestamp, x::int FROM long_sequence(10000)");
            drainWalQueue();
            engine.getRecentWriteTracker().clear();

            // Now interleave small INSERTs with UPDATEs in the same drain.
            // Each pair becomes a single ApplyWal2TableJob run that covers two
            // WAL transactions (one DATA, one SQL/UPDATE).
            for (int i = 0; i < 20; i++) {
                long base = (i + 100) * 86_400_000_000L;
                execute("INSERT INTO wa_update SELECT (" + base + " + x * 1000000)::timestamp, x::int FROM long_sequence(10)");
                execute("UPDATE wa_update SET v = v + 1 WHERE v = -1");
                drainWalQueue();
            }

            StringSink sink = new StringSink();
            try (RecordCursorFactory factory = select(
                    "SELECT table_write_amp_count, table_write_amp_p50, table_write_amp_p90, table_write_amp_p99, table_write_amp_max " +
                            "FROM tables() WHERE table_name = 'wa_update'"
            )) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Record r = cursor.getRecord();
                    long count = r.getLong(0);
                    double p50 = r.getDouble(1);
                    double p90 = r.getDouble(2);
                    double p99 = r.getDouble(3);
                    double max = r.getDouble(4);
                    sink.put("count=").put(count).put(" p50=").put(p50)
                            .put(" p90=").put(p90).put(" p99=").put(p99).put(" max=").put(max);
                    // Each INSERT was append-only (amp=1.0). The UPDATE matches no rows
                    // (WHERE v = -1), so it should write zero physical rows. The
                    // recorded amplification per job must remain ~1.0.
                    Assert.assertEquals("p50 should be ~1.0, got " + sink, 1.0, p50, 0.05);
                    Assert.assertEquals("p90 should be ~1.0, got " + sink, 1.0, p90, 0.05);
                    Assert.assertEquals("p99 should be ~1.0, got " + sink, 1.0, p99, 0.05);
                    Assert.assertEquals("max should be ~1.0, got " + sink, 1.0, max, 0.05);
                }
            }
        });
    }

    /**
     * Demonstrates the bug for the MAT_VIEW_INVALIDATE path. The case branch
     * in processWalCommit only calls updateMatViewRefreshState and
     * markSeqTxnCommitted; it does not reset
     * physicallyWrittenRowsSinceLastCommit. A MAT_VIEW_INVALIDATE
     * transaction that follows a data write in the same WAL apply job
     * causes the data write's physical row count to be re-counted on the
     * invalidate iteration. lastCommittedRows = 0 for invalidate, so
     * physicalRowsAdded grows but rowsAdded does not.
     * <p>
     * The test invokes WalWriter.resetMatViewState directly on a regular WAL
     * table to emit a synthetic MAT_VIEW_INVALIDATE event. WalWriter does not
     * validate that the table is a mat view, so this is an artificial but
     * deterministic way to drive that case branch from a unit test.
     */
    @Test
    public void testWriteAmpNotInflatedByMatViewInvalidate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wa_mvi (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            engine.getRecentWriteTracker().clear();

            TableToken tt = engine.verifyTableName("wa_mvi");

            for (int i = 0; i < 10; i++) {
                long base = (i + 1) * 86_400_000_000L;
                execute("INSERT INTO wa_mvi SELECT (" + base + " + x * 1000000)::timestamp, x::int FROM long_sequence(100)");
                // Emit a MAT_VIEW_INVALIDATE transaction immediately after the data write.
                try (WalWriter ww = engine.getWalWriter(tt)) {
                    ww.resetMatViewState(i, i, true, "test invalidation " + i, Numbers.LONG_NULL, null, -1);
                }
                drainWalQueue();
            }

            StringSink sink = new StringSink();
            try (RecordCursorFactory factory = select(
                    "SELECT table_write_amp_count, table_write_amp_p50, table_write_amp_p90, table_write_amp_p99, table_write_amp_max " +
                            "FROM tables() WHERE table_name = 'wa_mvi'"
            )) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Record r = cursor.getRecord();
                    long count = r.getLong(0);
                    double p50 = r.getDouble(1);
                    double p90 = r.getDouble(2);
                    double p99 = r.getDouble(3);
                    double max = r.getDouble(4);
                    sink.put("count=").put(count).put(" p50=").put(p50)
                            .put(" p90=").put(p90).put(" p99=").put(p99).put(" max=").put(max);
                    // Each insert is append-only (amp 1.0), MAT_VIEW_INVALIDATE writes nothing.
                    // True per-job amp = 1.0. Bug inflates it.
                    Assert.assertEquals("p50 should be ~1.0, got " + sink, 1.0, p50, 0.05);
                    Assert.assertEquals("p90 should be ~1.0, got " + sink, 1.0, p90, 0.05);
                    Assert.assertEquals("p99 should be ~1.0, got " + sink, 1.0, p99, 0.05);
                    Assert.assertEquals("max should be ~1.0, got " + sink, 1.0, max, 0.05);
                }
            }
        });
    }

    /**
     * Demonstrates the bug for the trySkipWalTransactions path. When a future
     * WAL transaction has WAL_DEDUP_MODE_REPLACE_RANGE that fully covers an
     * earlier transaction's timestamp range, ApplyWal2TableJob calls
     * trySkipWalTransactions to skip the earlier transaction without writing
     * data. trySkipWalTransactions does not reset
     * physicallyWrittenRowsSinceLastCommit, so the previous data commit's
     * physical row count is re-read on the skipped iteration.
     * <p>
     * Layout the test sets up in a single drain:
     * iter 1: INSERT into range R1 (not covered by future REPLACE_RANGE) -- normal write, counter = N
     * iter 2: INSERT into range R2 (covered by future REPLACE_RANGE) -- SKIPPED, counter still N
     * the physicalRowsAdded accumulator re-reads N and inflates
     * iter 3: REPLACE_RANGE write -- normal write, counter resets
     */
    @Test
    public void testWriteAmpNotInflatedBySkippedReplaceRange() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, 1);
            execute("CREATE TABLE wa_skip (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            engine.getRecentWriteTracker().clear();

            TableToken tt = engine.verifyTableName("wa_skip");

            long day1 = 86_400_000_000L;
            long day2 = 172_800_000_000L;

            // INSERT 1: large append-only into day 1 partition. Range NOT covered by
            // the future REPLACE_RANGE, so this iteration writes normally and leaves
            // physicallyWrittenRowsSinceLastCommit at ~1000.
            try (WalWriter ww = engine.getWalWriter(tt)) {
                for (long i = 0; i < 1000; i++) {
                    TableWriter.Row row = ww.newRow(day1 + i * 1_000_000L);
                    row.putInt(1, (int) i);
                    row.append();
                }
                ww.commit();
            }

            // INSERT 2: single row in day 2. Range will be covered by the next
            // REPLACE_RANGE, so this iteration is skippable via trySkipWalTransactions.
            try (WalWriter ww = engine.getWalWriter(tt)) {
                TableWriter.Row row = ww.newRow(day2);
                row.putInt(1, 0);
                row.append();
                ww.commit();
            }

            // REPLACE_RANGE covering only day 2's range (not day 1).
            try (WalWriter ww = engine.getWalWriter(tt)) {
                ww.commitWithParams(day2, day2 + 200_000_000L, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
            }

            drainWalQueue();

            StringSink sink = new StringSink();
            try (RecordCursorFactory factory = select(
                    "SELECT table_write_amp_count, table_write_amp_p50, table_write_amp_p90, table_write_amp_p99, table_write_amp_max " +
                            "FROM tables() WHERE table_name = 'wa_skip'"
            )) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Record r = cursor.getRecord();
                    long count = r.getLong(0);
                    double p50 = r.getDouble(1);
                    double p90 = r.getDouble(2);
                    double p99 = r.getDouble(3);
                    double max = r.getDouble(4);
                    sink.put("count=").put(count).put(" p50=").put(p50)
                            .put(" p90=").put(p90).put(" p99=").put(p99).put(" max=").put(max);
                    // True per-job amp: phys=1000, log=1001 -> 0.999 -> recorded as 1.0.
                    // With bug: phys = 1000 (insert1) + 1000 (stale on skipped iter) = 2000;
                    // log = 1000 + 1 (skipped seg row count) = 1001 -> amp ~ 2.0.
                    Assert.assertEquals("p50 should be ~1.0, got " + sink, 1.0, p50, 0.05);
                    Assert.assertEquals("p90 should be ~1.0, got " + sink, 1.0, p90, 0.05);
                    Assert.assertEquals("p99 should be ~1.0, got " + sink, 1.0, p99, 0.05);
                    Assert.assertEquals("max should be ~1.0, got " + sink, 1.0, max, 0.05);
                }
            }
        });
    }

    /**
     * The other skip trigger besides REPLACE_RANGE: a future TRUNCATE
     * supersedes prior data writes, so calculateSkipTransactionCount returns
     * a non-zero count for any data transaction whose timestamp range is
     * before the TRUNCATE. trySkipWalTransactions then runs without
     * resetting the counter. The bug pattern is identical to the
     * REPLACE_RANGE case; this test exercises the TRUNCATE skip route via
     * cross-drain leakage from the pooled TableWriter.
     */
    @Test
    public void testWriteAmpNotInflatedBySkippedTruncate() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, 1);
            execute("CREATE TABLE wa_truncate_skip (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            TableToken tt = engine.verifyTableName("wa_truncate_skip");
            long day1 = 86_400_000_000L;
            long day2 = 172_800_000_000L;

            // Drain 1: a normal DATA write that succeeds and leaves the
            // pooled TableWriter with a populated counter.
            try (WalWriter ww = engine.getWalWriter(tt)) {
                for (long i = 0; i < 1000; i++) {
                    TableWriter.Row row = ww.newRow(day1 + i * 1_000_000L);
                    row.putInt(1, (int) i);
                    row.append();
                }
                ww.commit();
            }
            drainWalQueue();
            engine.getRecentWriteTracker().clear();

            // Drain 2: a skippable DATA write followed by a TRUNCATE. When the
            // pooled writer is reused, its counter still holds the value left by
            // drain 1. The skip path does not reset, so the per-job accumulator
            // re-reads that stale value into physicalRowsAdded for this drain.
            try (WalWriter ww = engine.getWalWriter(tt)) {
                TableWriter.Row row = ww.newRow(day2);
                row.putInt(1, 0);
                row.append();
                ww.commit();
            }
            execute("TRUNCATE TABLE wa_truncate_skip");
            drainWalQueue();

            // Drain 2 wrote no physical rows (the data txn was skipped and the
            // TRUNCATE doesn't write rows), so the per-job amplification is 0
            // and gets filtered out by the amplification > 0 check in
            // recordMergeStats. The tracker was cleared between drains, so
            // count must stay at 0. Without the fix, the stale counter from
            // drain 1 would inflate amp to ~2000 and bump count to 1.
            assertQuery("SELECT table_write_amp_count FROM tables() WHERE table_name = 'wa_truncate_skip'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("table_write_amp_count\n0\n");
        });
    }

    /**
     * Each commit's per-job amplification is supposed to be aggregated into the
     * write_amp histogram, with one sample per ApplyWal2TableJob run. If every
     * commit produces amplification 1.0 (append-only inserts, no rewrites),
     * the percentiles in tables() must also be 1.0.
     * <p>
     * This test reproduces a customer scenario where the per-commit log line
     * shows ampl=1.01 but tables() reports P90/P99 of thousands.
     */
    @Test
    public void testWriteAmpPercentilesMatchPerCommitValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wa_test (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            engine.getRecentWriteTracker().clear();

            // 50 separate, append-only commits, each strictly increasing in time.
            // Drain after every insert so each becomes its own ApplyWal2TableJob run
            // and contributes one sample to the write_amp histogram.
            for (int i = 0; i < 50; i++) {
                long base = (i + 1) * 86_400_000_000L;
                execute("INSERT INTO wa_test SELECT (" + base + " + x * 1000000)::timestamp, x::int FROM long_sequence(100)");
                drainWalQueue();
            }

            assertQuery("SELECT table_write_amp_count FROM tables() WHERE table_name = 'wa_test'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("table_write_amp_count\n50\n");

            // Every commit was append-only, so per-commit amp = 1.0 for all 50 samples.
            // Therefore every percentile and the max must be 1.0 (within histogram
            // precision of 2 significant digits, exact 1.0 is exactly 1.0).
            StringSink sink = new StringSink();
            try (RecordCursorFactory factory = select(
                    "SELECT table_write_amp_p50, table_write_amp_p90, table_write_amp_p99, table_write_amp_max " +
                            "FROM tables() WHERE table_name = 'wa_test'"
            )) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Record r = cursor.getRecord();
                    double p50 = r.getDouble(0);
                    double p90 = r.getDouble(1);
                    double p99 = r.getDouble(2);
                    double max = r.getDouble(3);
                    sink.put("p50=").put(p50).put(" p90=").put(p90)
                            .put(" p99=").put(p99).put(" max=").put(max);
                    Assert.assertEquals("p50 should be ~1.0, got " + sink, 1.0, p50, 0.05);
                    Assert.assertEquals("p90 should be ~1.0, got " + sink, 1.0, p90, 0.05);
                    Assert.assertEquals("p99 should be ~1.0, got " + sink, 1.0, p99, 0.05);
                    Assert.assertEquals("max should be ~1.0, got " + sink, 1.0, max, 0.05);
                }
            }
        });
    }
}
