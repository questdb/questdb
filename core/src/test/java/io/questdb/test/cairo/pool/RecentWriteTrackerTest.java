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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class RecentWriteTrackerTest {

    @Test
    public void testBasicRecordAndRetrieve() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        TableToken table2 = createTableToken("table2", 2);
        TableToken table3 = createTableToken("table3", 3);

        tracker.recordWrite(table1, 1000L, 100L, 1L);
        tracker.recordWrite(table2, 2000L, 200L, 2L);
        tracker.recordWrite(table3, 3000L, 300L, 3L);

        Assert.assertEquals(3, tracker.size());

        Assert.assertEquals(1000L, tracker.getWriteTimestamp(table1));
        Assert.assertEquals(100L, tracker.getRowCount(table1));
        Assert.assertEquals(2000L, tracker.getWriteTimestamp(table2));
        Assert.assertEquals(200L, tracker.getRowCount(table2));
        Assert.assertEquals(3000L, tracker.getWriteTimestamp(table3));
        Assert.assertEquals(300L, tracker.getRowCount(table3));
    }

    @Test
    public void testClear() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        TableToken table2 = createTableToken("table2", 2);

        tracker.recordWrite(table1, 1000L, 100L, 1L);
        tracker.recordWrite(table2, 2000L, 200L, 2L);

        Assert.assertEquals(2, tracker.size());

        tracker.clear();

        Assert.assertEquals(0, tracker.size());
        Assert.assertNull(tracker.getWriteStats(table1));
        Assert.assertNull(tracker.getWriteStats(table2));
    }

    @Test
    public void testConcurrentReadsAndWrites() throws Exception {
        final int capacity = 100;
        final int numWriterThreads = 2;
        final int numReaderThreads = 2;
        final int operationsPerThread = 500;

        RecentWriteTracker tracker = new RecentWriteTracker(capacity);

        // Pre-populate with some data
        for (int i = 0; i < 50; i++) {
            TableToken table = createTableToken("initial_table" + i, i);
            tracker.recordWrite(table, i * 1000L, i * 10L, i);
        }

        CyclicBarrier barrier = new CyclicBarrier(numWriterThreads + numReaderThreads);
        AtomicInteger errors = new AtomicInteger(0);

        Thread[] threads = new Thread[numWriterThreads + numReaderThreads];

        // Writer threads
        for (int t = 0; t < numWriterThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        int tableIndex = i % 30;
                        String tableName = "writer" + threadId + "_table" + tableIndex;
                        int tableId = 1000 + threadId * 100 + tableIndex;
                        TableToken table = createTableToken(tableName, tableId);
                        tracker.recordWrite(table, System.nanoTime(), i * 10L, i);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                }
            });
            threads[t].start();
        }

        // Reader threads
        for (int t = 0; t < numReaderThreads; t++) {
            threads[numWriterThreads + t] = new Thread(() -> {
                try {
                    barrier.await();
                    TableToken lookupTable = createTableToken("initial_table0", 0);
                    for (int i = 0; i < operationsPerThread; i++) {
                        RecentWriteTracker.WriteStats stats = tracker.getWriteStats(lookupTable);
                        if (stats != null) {
                            Assert.assertTrue(stats.getTimestamp() >= 0);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                }
            });
            threads[numWriterThreads + t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Assert.assertEquals(0, errors.get());
    }

    @Test
    public void testConcurrentWrites() throws Exception {
        final int capacity = 100;
        final int numThreads = 4;
        final int writesPerThread = 1000;

        RecentWriteTracker tracker = new RecentWriteTracker(capacity);
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger errors = new AtomicInteger(0);

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < writesPerThread; i++) {
                        // Each thread writes to its own set of tables
                        int tableIndex = i % 50;
                        String tableName = "thread" + threadId + "_table" + tableIndex;
                        int tableId = threadId * 1000 + tableIndex;
                        TableToken table = createTableToken(tableName, tableId);
                        tracker.recordWrite(table, System.nanoTime(), i * 10L, i);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                }
            });
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Assert.assertEquals(0, errors.get());
        // Each thread writes to 50 unique tables, so total unique tables = numThreads * 50 = 200
        // But capacity is 100, and eviction happens at 2x capacity (200)
        // So we might have anywhere from 100 to 200 entries
        Assert.assertTrue("Size should be reasonable", tracker.size() > 0 && tracker.size() <= 200);

        // Verify we can still retrieve a known entry directly.
        Assert.assertNotNull(tracker.getWriteStats(createTableToken("thread0_table0", 0)));
    }

    @Test
    public void testEmptyTracker() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        Assert.assertEquals(0, tracker.size());
        Assert.assertNull(tracker.getWriteStats(createTableToken("missing", 1)));
    }

    @Test
    public void testEviction() {
        // Small capacity to trigger eviction
        RecentWriteTracker tracker = new RecentWriteTracker(5);

        // Add more than 2x capacity to trigger eviction
        for (int i = 0; i < 15; i++) {
            TableToken table = createTableToken("table" + i, i);
            tracker.recordWrite(table, i * 1000L, i * 10L, i);
        }

        // After eviction, size should be around capacity (5)
        // The eviction keeps the most recent entries
        Assert.assertTrue("Size should be <= capacity after eviction", tracker.size() <= 10);

        // The most recent tables should still be present
        Assert.assertNotNull("table14 should survive as the most recent write", tracker.getWriteStats(createTableToken("table14", 14)));
    }

    @Test
    public void testRecordMergeStatsEvictsWhenCreatingEntries() {
        RecentWriteTracker tracker = new RecentWriteTracker(2);

        for (int i = 0; i < 5; i++) {
            tracker.recordMergeStats(createTableToken("merge" + i, i), 1.0, 10L, i, i + 100L);
        }

        Assert.assertEquals("recordMergeStats should apply lazy eviction", 2, tracker.size());
    }

    @Test
    public void testRecordWalProcessedEvictsWhenCreatingEntries() {
        RecentWriteTracker tracker = new RecentWriteTracker(2);

        for (int i = 0; i < 5; i++) {
            tracker.recordWalProcessed(createTableToken("wal_processed" + i, i), i, 1L, 0L);
        }

        Assert.assertEquals("recordWalProcessed should apply lazy eviction", 2, tracker.size());
    }

    @Test
    public void testSetFloorSeqTxnEvictsWhenCreatingEntries() {
        RecentWriteTracker tracker = new RecentWriteTracker(2);

        for (int i = 0; i < 5; i++) {
            tracker.setFloorSeqTxn(createTableToken("floor" + i, i), i);
        }

        Assert.assertEquals("setFloorSeqTxn should apply lazy eviction", 2, tracker.size());
    }

    @Test
    public void testGetMaxCapacity() {
        RecentWriteTracker tracker = new RecentWriteTracker(42);
        Assert.assertEquals(42, tracker.getMaxCapacity());
    }

    @Test
    public void testGetRowCount() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        tracker.recordWrite(table1, 12345L, 500L, 10L);

        Assert.assertEquals(500L, tracker.getRowCount(table1));

        TableToken nonexistent = createTableToken("nonexistent", 999);
        Assert.assertEquals(Numbers.LONG_NULL, tracker.getRowCount(nonexistent));
    }

    @Test
    public void testGetWriteStats() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        tracker.recordWrite(table1, 12345L, 500L, 10L);

        RecentWriteTracker.WriteStats stats = tracker.getWriteStats(table1);
        Assert.assertNotNull(stats);
        Assert.assertEquals(12345L, stats.getTimestamp());
        Assert.assertEquals(500L, stats.getRowCount());
        Assert.assertEquals(10L, stats.getWriterTxn());

        // Update and verify same object is reused (zero-allocation for updates)
        tracker.recordWrite(table1, 23456L, 750L, 11L);
        Assert.assertEquals(23456L, stats.getTimestamp());
        Assert.assertEquals(750L, stats.getRowCount());
        Assert.assertEquals(11L, stats.getWriterTxn());

        // Nonexistent table returns null
        TableToken nonexistent = createTableToken("nonexistent", 999);
        Assert.assertNull(tracker.getWriteStats(nonexistent));
    }

    @Test
    public void testGetWriteTimestamp() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        tracker.recordWrite(table1, 12345L, 500L, 10L);

        Assert.assertEquals(12345L, tracker.getWriteTimestamp(table1));

        TableToken nonexistent = createTableToken("nonexistent", 999);
        Assert.assertEquals(Numbers.LONG_NULL, tracker.getWriteTimestamp(nonexistent));
    }

    @Test
    public void testGetWriterTxn() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        tracker.recordWrite(table1, 12345L, 500L, 42L);

        Assert.assertEquals(42L, tracker.getWriterTxn(table1));

        TableToken nonexistent = createTableToken("nonexistent", 999);
        Assert.assertEquals(Numbers.LONG_NULL, tracker.getWriterTxn(nonexistent));
    }

    @Test
    public void testEvictionUsesActivityTimestamp() {
        // Capacity 2, eviction at 4 (2x)
        RecentWriteTracker tracker = new RecentWriteTracker(2);

        TableToken table1 = createTableToken("table1", 1);
        TableToken table2 = createTableToken("table2", 2);
        TableToken table3 = createTableToken("table3", 3);
        TableToken table4 = createTableToken("table4", 4);
        TableToken table5 = createTableToken("table5", 5);

        // table1: writer timestamp 1000 (oldest writer)
        tracker.recordWrite(table1, 1000L, 100L, 1L);
        // table2: writer timestamp 2000
        tracker.recordWrite(table2, 2000L, 200L, 2L);
        // table3: writer timestamp 3000
        tracker.recordWrite(table3, 3000L, 300L, 3L);
        // table4: writer timestamp 4000
        tracker.recordWrite(table4, 4000L, 400L, 4L);

        // Now give table1 recent WAL activity. Eviction uses activity time, not WAL data time.
        tracker.recordWalWrite(table1, 10L, 10000L, 0L);

        // Add table5 to trigger eviction (size becomes 5, exceeds 2*2=4)
        tracker.recordWrite(table5, 5000L, 500L, 5L);

        // After eviction, should have 2 entries
        // table2 should be evicted because it has the oldest activity timestamp
        // table1 should survive because recordWalWrite refreshed tracker activity
        Assert.assertEquals(2, tracker.size());
        Assert.assertNotNull("table1 should survive (recent WAL activity)", tracker.getWriteStats(table1));
        Assert.assertNull("table2 should be evicted (oldest)", tracker.getWriteStats(table2));
    }

    @Test
    public void testRecordWalProcessedCreatesEntryThatSurvivesEviction() {
        RecentWriteTracker tracker = new RecentWriteTracker(2);

        for (int i = 0; i < 4; i++) {
            tracker.recordWrite(createTableToken("old" + i, i), i + 1L, 10L, 1L);
        }

        TableToken fresh = createTableToken("fresh", 100);
        tracker.recordWalProcessed(fresh, 10L, 5L, 1L);
        tracker.recordWrite(createTableToken("trigger", 101), 1_000L, 42L, 1L);

        RecentWriteTracker.WriteStats stats = tracker.getWriteStats(fresh);
        Assert.assertNotNull("WAL processed entry should survive the eviction that follows insertion", stats);
        Assert.assertEquals(1L, stats.getDedupRowCount());
        Assert.assertEquals(0L, stats.getWalRowCount());
    }

    @Test
    public void testWalWriteWithOldDataTimestampSurvivesEviction() {
        RecentWriteTracker tracker = new RecentWriteTracker(2);

        for (int i = 0; i < 4; i++) {
            tracker.recordWrite(createTableToken("old" + i, i), i + 1L, 10L, 1L);
        }

        TableToken fresh = createTableToken("fresh", 100);
        tracker.recordWalWrite(fresh, 10L, 0L, 5L);

        RecentWriteTracker.WriteStats stats = tracker.getWriteStats(fresh);
        Assert.assertNotNull("fresh WAL activity must not be evicted because its data timestamp is old", stats);
        Assert.assertEquals(0L, stats.getLastWalTimestamp());
        Assert.assertEquals(5L, stats.getWalRowCount());
    }

    @Test
    public void testRemoveTable() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        TableToken table2 = createTableToken("table2", 2);

        tracker.recordWrite(table1, 1000L, 100L, 1L);
        tracker.recordWrite(table2, 2000L, 200L, 2L);

        Assert.assertEquals(2, tracker.size());

        tracker.removeTable(table1);

        Assert.assertEquals(1, tracker.size());
        Assert.assertEquals(Numbers.LONG_NULL, tracker.getWriteTimestamp(table1));
        Assert.assertEquals(2000L, tracker.getWriteTimestamp(table2));
    }

    @Test
    public void testRowCountUpdatedOnWrite() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);

        // First write
        tracker.recordWrite(table1, 1000L, 100L, 1L);
        Assert.assertEquals(100L, tracker.getRowCount(table1));

        // Second write with more rows
        tracker.recordWrite(table1, 2000L, 250L, 2L);
        Assert.assertEquals(250L, tracker.getRowCount(table1));

        // Third write with even more rows
        tracker.recordWrite(table1, 3000L, 500L, 3L);
        Assert.assertEquals(500L, tracker.getRowCount(table1));

        // Verify timestamp also updated
        Assert.assertEquals(3000L, tracker.getWriteTimestamp(table1));
        Assert.assertEquals(3L, tracker.getWriterTxn(table1));
    }

    @Test
    public void testUpdateExistingTable() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        TableToken table2 = createTableToken("table2", 2);

        tracker.recordWrite(table1, 1000L, 100L, 1L);
        tracker.recordWrite(table2, 2000L, 200L, 2L);
        // Update table1 with newer timestamp and row count
        tracker.recordWrite(table1, 3000L, 150L, 3L);

        Assert.assertEquals(2, tracker.size());

        Assert.assertEquals(3000L, tracker.getWriteTimestamp(table1));
        Assert.assertEquals(150L, tracker.getRowCount(table1));
        Assert.assertEquals(3L, tracker.getWriterTxn(table1));
        Assert.assertEquals(2000L, tracker.getWriteTimestamp(table2));
        Assert.assertEquals(200L, tracker.getRowCount(table2));
    }

    @Test
    public void testGetWalTimestampAccessor() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        TableToken table2 = createTableToken("table2", 2);

        // No entry - should return LONG_NULL
        Assert.assertEquals(Numbers.LONG_NULL, tracker.getLastWalTimestamp(table1));

        // Writer-only entry - walTimestamp should be LONG_NULL
        tracker.recordWrite(table1, 1000L, 100L, 1L);
        Assert.assertEquals(Numbers.LONG_NULL, tracker.getLastWalTimestamp(table1));

        // WAL write - walTimestamp should be set
        tracker.recordWalWrite(table2, 5L, 3000L, 0L);
        Assert.assertEquals(3000L, tracker.getLastWalTimestamp(table2));
    }

    @Test
    public void testRecordWriteIfAbsentDoesNotOverwrite() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);

        // First insert via recordWrite (simulates writer)
        tracker.recordWrite(table1, 5000L, 500L, 50L);

        // Attempt to insert via recordWriteIfAbsent (simulates hydration with stale data)
        boolean inserted = tracker.recordWriteIfAbsent(table1, 1000L, 100L, 10L, 5L, 1000L, 500L, 1500L);

        // Should not have overwritten
        Assert.assertFalse("recordWriteIfAbsent should return false when entry exists", inserted);
        Assert.assertEquals("Writer timestamp should be preserved", 5000L, tracker.getWriteTimestamp(table1));
        Assert.assertEquals("Writer rowCount should be preserved", 500L, tracker.getRowCount(table1));
        Assert.assertEquals("Writer txn should be preserved", 50L, tracker.getWriterTxn(table1));
        Assert.assertEquals("Size should still be 1", 1, tracker.size());
    }

    @Test
    public void testRecordWriteIfAbsentInsertsWhenEmpty() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);

        // Insert via recordWriteIfAbsent when no entry exists (with sequencerTxn)
        boolean inserted = tracker.recordWriteIfAbsent(table1, 1000L, 100L, 10L, 5L, 1000L, 500L, 1500L);

        Assert.assertTrue("recordWriteIfAbsent should return true when inserting new entry", inserted);
        Assert.assertEquals(1000L, tracker.getWriteTimestamp(table1));
        Assert.assertEquals(100L, tracker.getRowCount(table1));
        Assert.assertEquals(10L, tracker.getWriterTxn(table1));
        Assert.assertEquals(5L, tracker.getSequencerTxn(table1));
        Assert.assertEquals(500L, tracker.getWriteStats(table1).getTableMinTimestamp());
        Assert.assertEquals(1500L, tracker.getWriteStats(table1).getTableMaxTimestamp());
        Assert.assertEquals(1, tracker.size());
    }

    @Test
    public void testRecordWriteOverwritesHydratedData() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);

        // First insert via recordWriteIfAbsent (simulates hydration with sequencerTxn)
        boolean inserted = tracker.recordWriteIfAbsent(table1, 1000L, 100L, 10L, 5L, 1000L, 500L, 1500L);
        Assert.assertTrue(inserted);
        Assert.assertEquals(5L, tracker.getSequencerTxn(table1));
        Assert.assertEquals(1000L, tracker.getWriteStats(table1).getLastWalTimestamp());

        // Writer updates with fresh data (preserves sequencerTxn)
        tracker.recordWrite(table1, 5000L, 500L, 50L);

        // Writer data should win, but sequencerTxn preserved
        Assert.assertEquals("Writer timestamp should overwrite", 5000L, tracker.getWriteTimestamp(table1));
        Assert.assertEquals("Writer rowCount should overwrite", 500L, tracker.getRowCount(table1));
        Assert.assertEquals("Writer txn should overwrite", 50L, tracker.getWriterTxn(table1));
        Assert.assertEquals("SequencerTxn should be preserved", 5L, tracker.getSequencerTxn(table1));
    }

    @Test
    public void testUpdateWalHigherTxnWins() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);

        // First WAL write with txn=5, timestamp=1000
        tracker.recordWalWrite(table1, 5L, 1000L, 0L);
        Assert.assertEquals(5L, tracker.getSequencerTxn(table1));
        Assert.assertEquals(1000L, tracker.getLastWalTimestamp(table1));

        // Higher txn and higher timestamp should both update
        tracker.recordWalWrite(table1, 10L, 2000L, 0L);
        Assert.assertEquals(10L, tracker.getSequencerTxn(table1));
        Assert.assertEquals(2000L, tracker.getLastWalTimestamp(table1));
    }

    @Test
    public void testUpdateWalLowerTxnBacksOff() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);

        // First WAL write with higher txn=10, timestamp=2000
        tracker.recordWalWrite(table1, 10L, 2000L, 0L);
        Assert.assertEquals(10L, tracker.getSequencerTxn(table1));
        Assert.assertEquals(2000L, tracker.getLastWalTimestamp(table1));

        // Lower txn should NOT update sequencerTxn, lower timestamp should NOT update walTimestamp
        tracker.recordWalWrite(table1, 5L, 1000L, 0L);
        Assert.assertEquals("SequencerTxn should remain 10", 10L, tracker.getSequencerTxn(table1));
        Assert.assertEquals("WalTimestamp should remain 2000", 2000L, tracker.getLastWalTimestamp(table1));
    }

    @Test
    public void testUpdateWalMixedUpdates() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);

        // First WAL write: txn=10, timestamp=1000
        tracker.recordWalWrite(table1, 10L, 1000L, 0L);
        Assert.assertEquals(10L, tracker.getSequencerTxn(table1));
        Assert.assertEquals(1000L, tracker.getLastWalTimestamp(table1));

        // Lower txn but higher timestamp: txn should stay, timestamp should update
        tracker.recordWalWrite(table1, 5L, 2000L, 0L);
        Assert.assertEquals("SequencerTxn should remain 10 (higher wins)", 10L, tracker.getSequencerTxn(table1));
        Assert.assertEquals("WalTimestamp should update to 2000 (higher wins)", 2000L, tracker.getLastWalTimestamp(table1));

        // Higher txn but lower timestamp: txn should update, timestamp should stay
        tracker.recordWalWrite(table1, 15L, 1500L, 0L);
        Assert.assertEquals("SequencerTxn should update to 15", 15L, tracker.getSequencerTxn(table1));
        Assert.assertEquals("WalTimestamp should remain 2000 (higher wins)", 2000L, tracker.getLastWalTimestamp(table1));
    }

    private static TableToken createTableToken(String tableName, int tableId) {
        return new TableToken(tableName, tableName, null, tableId, false, false, false);
    }
}
