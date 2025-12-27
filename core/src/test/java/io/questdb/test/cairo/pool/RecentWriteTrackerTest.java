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

        tracker.recordWrite(table1, 1000L, 100L);
        tracker.recordWrite(table2, 2000L, 200L);
        tracker.recordWrite(table3, 3000L, 300L);

        Assert.assertEquals(3, tracker.size());

        ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(10);
        Assert.assertEquals(3, recent.size());
        // Most recent first
        Assert.assertEquals("table3", recent.get(0).getTableName());
        Assert.assertEquals("table2", recent.get(1).getTableName());
        Assert.assertEquals("table1", recent.get(2).getTableName());
    }

    @Test
    public void testClear() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        TableToken table2 = createTableToken("table2", 2);

        tracker.recordWrite(table1, 1000L, 100L);
        tracker.recordWrite(table2, 2000L, 200L);

        Assert.assertEquals(2, tracker.size());

        tracker.clear();

        Assert.assertEquals(0, tracker.size());
        ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(10);
        Assert.assertEquals(0, recent.size());
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
            tracker.recordWrite(table, i * 1000L, i * 10L);
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
                        tracker.recordWrite(table, System.nanoTime(), i * 10L);
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
                    for (int i = 0; i < operationsPerThread; i++) {
                        ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(10);
                        // Just verify it doesn't throw and returns valid data
                        Assert.assertNotNull(recent);
                        Assert.assertTrue(recent.size() <= 10);

                        // Also test single table lookup
                        TableToken lookupTable = createTableToken("initial_table0", 0);
                        tracker.getWriteTimestamp(lookupTable);
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
                        tracker.recordWrite(table, System.nanoTime(), i * 10L);
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

        // Verify we can still retrieve results
        ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(10);
        Assert.assertTrue(recent.size() > 0 && recent.size() <= 10);
    }

    @Test
    public void testEmptyTracker() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        Assert.assertEquals(0, tracker.size());
        ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(10);
        Assert.assertEquals(0, recent.size());
    }

    @Test
    public void testEviction() {
        // Small capacity to trigger eviction
        RecentWriteTracker tracker = new RecentWriteTracker(5);

        // Add more than 2x capacity to trigger eviction
        for (int i = 0; i < 15; i++) {
            TableToken table = createTableToken("table" + i, i);
            tracker.recordWrite(table, i * 1000L, i * 10L);
        }

        // After eviction, size should be around capacity (5)
        // The eviction keeps the most recent entries
        Assert.assertTrue("Size should be <= capacity after eviction", tracker.size() <= 10);

        // The most recent tables should still be present
        ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(5);
        Assert.assertTrue(recent.size() > 0);
        // table14 should be there as it's the most recent
        Assert.assertEquals("table14", recent.get(0).getTableName());
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
        tracker.recordWrite(table1, 12345L, 500L);

        Assert.assertEquals(500L, tracker.getRowCount(table1));

        TableToken nonexistent = createTableToken("nonexistent", 999);
        Assert.assertEquals(Numbers.LONG_NULL, tracker.getRowCount(nonexistent));
    }

    @Test
    public void testGetWriteStats() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        tracker.recordWrite(table1, 12345L, 500L);

        RecentWriteTracker.WriteStats stats = tracker.getWriteStats(table1);
        Assert.assertNotNull(stats);
        Assert.assertEquals(12345L, stats.getTimestamp());
        Assert.assertEquals(500L, stats.getRowCount());

        // Update and verify same object is reused (zero-allocation for updates)
        tracker.recordWrite(table1, 23456L, 750L);
        Assert.assertEquals(23456L, stats.getTimestamp());
        Assert.assertEquals(750L, stats.getRowCount());

        // Nonexistent table returns null
        TableToken nonexistent = createTableToken("nonexistent", 999);
        Assert.assertNull(tracker.getWriteStats(nonexistent));
    }

    @Test
    public void testGetWriteTimestamp() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        tracker.recordWrite(table1, 12345L, 500L);

        Assert.assertEquals(12345L, tracker.getWriteTimestamp(table1));

        TableToken nonexistent = createTableToken("nonexistent", 999);
        Assert.assertEquals(Numbers.LONG_NULL, tracker.getWriteTimestamp(nonexistent));
    }

    @Test
    public void testLimitResults() {
        RecentWriteTracker tracker = new RecentWriteTracker(100);

        for (int i = 0; i < 50; i++) {
            TableToken table = createTableToken("table" + i, i);
            tracker.recordWrite(table, i * 1000L, i * 10L);
        }

        Assert.assertEquals(50, tracker.size());

        // Request only top 5
        ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(5);
        Assert.assertEquals(5, recent.size());
        // Most recent first (highest timestamps)
        Assert.assertEquals("table49", recent.get(0).getTableName());
        Assert.assertEquals("table48", recent.get(1).getTableName());
        Assert.assertEquals("table47", recent.get(2).getTableName());
        Assert.assertEquals("table46", recent.get(3).getTableName());
        Assert.assertEquals("table45", recent.get(4).getTableName());
    }

    @Test
    public void testRemoveTable() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        TableToken table2 = createTableToken("table2", 2);

        tracker.recordWrite(table1, 1000L, 100L);
        tracker.recordWrite(table2, 2000L, 200L);

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
        tracker.recordWrite(table1, 1000L, 100L);
        Assert.assertEquals(100L, tracker.getRowCount(table1));

        // Second write with more rows
        tracker.recordWrite(table1, 2000L, 250L);
        Assert.assertEquals(250L, tracker.getRowCount(table1));

        // Third write with even more rows
        tracker.recordWrite(table1, 3000L, 500L);
        Assert.assertEquals(500L, tracker.getRowCount(table1));

        // Verify timestamp also updated
        Assert.assertEquals(3000L, tracker.getWriteTimestamp(table1));
    }

    @Test
    public void testUpdateExistingTable() {
        RecentWriteTracker tracker = new RecentWriteTracker(10);

        TableToken table1 = createTableToken("table1", 1);
        TableToken table2 = createTableToken("table2", 2);

        tracker.recordWrite(table1, 1000L, 100L);
        tracker.recordWrite(table2, 2000L, 200L);
        // Update table1 with newer timestamp and row count
        tracker.recordWrite(table1, 3000L, 150L);

        Assert.assertEquals(2, tracker.size());

        ObjList<TableToken> recent = tracker.getRecentlyWrittenTables(10);
        Assert.assertEquals(2, recent.size());
        // table1 should now be first (most recent)
        Assert.assertEquals("table1", recent.get(0).getTableName());
        Assert.assertEquals("table2", recent.get(1).getTableName());
    }

    private static TableToken createTableToken(String tableName, int tableId) {
        return new TableToken(tableName, tableName, null, tableId, false, false, false);
    }
}
