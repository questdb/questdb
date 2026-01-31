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

package io.questdb.test.cairo;

import io.questdb.cairo.idx.RoaringBitmapIndexBwdReader;
import io.questdb.cairo.idx.RoaringBitmapIndexFwdReader;
import io.questdb.cairo.idx.RoaringBitmapIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class RoaringBitmapIndexTest extends AbstractCairoTest {

    @Test
    public void testSimpleAddAndRead() {
        final int keyCount = 30;
        final int valuesPerKey = 100;

        try (Path path = new Path().of(root)) {
            final int plen = path.size();

            // Write index
            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration)) {
                writer.of(path, "test", COLUMN_NAME_TXN_NONE, true);

                for (int key = 0; key < keyCount; key++) {
                    for (int i = 0; i < valuesPerKey; i++) {
                        long value = key * 10000L + i;
                        writer.add(key, value);
                    }
                }
                writer.commit();
            }

            // Read forward
            try (RoaringBitmapIndexFwdReader reader = new RoaringBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                Assert.assertEquals(keyCount, reader.getKeyCount());

                for (int key = 0; key < keyCount; key++) {
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    int count = 0;
                    long lastValue = -1;
                    while (cursor.hasNext()) {
                        long value = cursor.next();
                        Assert.assertTrue("Values should be ascending", value > lastValue);
                        lastValue = value;
                        count++;
                    }
                    Assert.assertEquals(valuesPerKey, count);
                }
            }

            // Read backward
            try (RoaringBitmapIndexBwdReader reader = new RoaringBitmapIndexBwdReader(
                    configuration, path.trimTo(plen), "test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                for (int key = 0; key < keyCount; key++) {
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    int count = 0;
                    long lastValue = Long.MAX_VALUE;
                    while (cursor.hasNext()) {
                        long value = cursor.next();
                        Assert.assertTrue("Values should be descending", value < lastValue);
                        lastValue = value;
                        count++;
                    }
                    Assert.assertEquals(valuesPerKey, count);
                }
            }
        }
    }

    @Test
    public void testRangeQuery() {
        try (Path path = new Path().of(root)) {
            final int plen = path.size();

            // Write index with values 0, 10, 20, 30, ..., 990
            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration)) {
                writer.of(path, "range_test", COLUMN_NAME_TXN_NONE, true);

                for (int i = 0; i < 100; i++) {
                    writer.add(0, i * 10L);
                }
                writer.commit();
            }

            // Query range [250, 750]
            try (RoaringBitmapIndexFwdReader reader = new RoaringBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "range_test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                RowCursor cursor = reader.getCursor(true, 0, 250, 750);
                LongList values = new LongList();
                while (cursor.hasNext()) {
                    values.add(cursor.next());
                }

                // Should get values 250, 260, 270, ..., 750 = 51 values
                Assert.assertEquals(51, values.size());
                Assert.assertEquals(250, values.get(0));
                Assert.assertEquals(750, values.get(values.size() - 1));
            }
        }
    }

    @Test
    public void testLargeIndex() {
        // Test with enough values to trigger array -> bitmap promotion
        final int valueCount = 10000;  // More than 4096 to trigger bitmap

        try (Path path = new Path().of(root)) {
            final int plen = path.size();

            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration)) {
                writer.of(path, "large_test", COLUMN_NAME_TXN_NONE, true);

                for (int i = 0; i < valueCount; i++) {
                    writer.add(0, i);
                }
                writer.commit();
            }

            // Read forward and verify
            try (RoaringBitmapIndexFwdReader reader = new RoaringBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "large_test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                long expected = 0;
                while (cursor.hasNext()) {
                    long value = cursor.next();
                    Assert.assertEquals(expected, value);
                    expected++;
                    count++;
                }
                Assert.assertEquals(valueCount, count);
            }

            // Read backward and verify
            try (RoaringBitmapIndexBwdReader reader = new RoaringBitmapIndexBwdReader(
                    configuration, path.trimTo(plen), "large_test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                long expected = valueCount - 1;
                while (cursor.hasNext()) {
                    long value = cursor.next();
                    Assert.assertEquals(expected, value);
                    expected--;
                    count++;
                }
                Assert.assertEquals(valueCount, count);
            }
        }
    }

    @Test
    public void testMultipleChunks() {
        // Test values that span multiple chunks (each chunk is 65536 values)
        try (Path path = new Path().of(root)) {
            final int plen = path.size();

            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration)) {
                writer.of(path, "multi_chunk_test", COLUMN_NAME_TXN_NONE, true);

                // Add values in chunks 0, 1, and 2
                writer.add(0, 100);        // chunk 0
                writer.add(0, 1000);       // chunk 0
                writer.add(0, 65536);      // chunk 1 (first value in second chunk)
                writer.add(0, 65600);      // chunk 1
                writer.add(0, 131072);     // chunk 2 (first value in third chunk)
                writer.add(0, 131100);     // chunk 2
                writer.commit();
            }

            // Read forward
            try (RoaringBitmapIndexFwdReader reader = new RoaringBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "multi_chunk_test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                LongList values = new LongList();
                while (cursor.hasNext()) {
                    values.add(cursor.next());
                }

                Assert.assertEquals(6, values.size());
                Assert.assertEquals(100, values.get(0));
                Assert.assertEquals(1000, values.get(1));
                Assert.assertEquals(65536, values.get(2));
                Assert.assertEquals(65600, values.get(3));
                Assert.assertEquals(131072, values.get(4));
                Assert.assertEquals(131100, values.get(5));
            }

            // Read backward
            try (RoaringBitmapIndexBwdReader reader = new RoaringBitmapIndexBwdReader(
                    configuration, path.trimTo(plen), "multi_chunk_test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                LongList values = new LongList();
                while (cursor.hasNext()) {
                    values.add(cursor.next());
                }

                Assert.assertEquals(6, values.size());
                Assert.assertEquals(131100, values.get(0));
                Assert.assertEquals(131072, values.get(1));
                Assert.assertEquals(65600, values.get(2));
                Assert.assertEquals(65536, values.get(3));
                Assert.assertEquals(1000, values.get(4));
                Assert.assertEquals(100, values.get(5));
            }
        }
    }

    @Test
    public void testSparseKeys() {
        // Test with non-consecutive keys
        try (Path path = new Path().of(root)) {
            final int plen = path.size();

            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration)) {
                writer.of(path, "sparse_keys_test", COLUMN_NAME_TXN_NONE, true);

                // Add values for keys 0, 5, 10
                writer.add(0, 100);
                writer.add(0, 200);
                writer.add(5, 1000);
                writer.add(5, 2000);
                writer.add(10, 5000);
                writer.commit();
            }

            try (RoaringBitmapIndexFwdReader reader = new RoaringBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "sparse_keys_test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                // Key 0
                RowCursor cursor0 = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                Assert.assertTrue(cursor0.hasNext());
                Assert.assertEquals(100, cursor0.next());
                Assert.assertTrue(cursor0.hasNext());
                Assert.assertEquals(200, cursor0.next());
                Assert.assertFalse(cursor0.hasNext());

                // Key 5
                RowCursor cursor5 = reader.getCursor(true, 5, 0, Long.MAX_VALUE);
                Assert.assertTrue(cursor5.hasNext());
                Assert.assertEquals(1000, cursor5.next());
                Assert.assertTrue(cursor5.hasNext());
                Assert.assertEquals(2000, cursor5.next());
                Assert.assertFalse(cursor5.hasNext());

                // Key 10
                RowCursor cursor10 = reader.getCursor(true, 10, 0, Long.MAX_VALUE);
                Assert.assertTrue(cursor10.hasNext());
                Assert.assertEquals(5000, cursor10.next());
                Assert.assertFalse(cursor10.hasNext());

                // Non-existent key should return empty cursor
                RowCursor cursorEmpty = reader.getCursor(true, 3, 0, Long.MAX_VALUE);
                Assert.assertFalse(cursorEmpty.hasNext());
            }
        }
    }

    @Test
    public void testEmptyIndex() {
        try (Path path = new Path().of(root)) {
            final int plen = path.size();

            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration)) {
                writer.of(path, "empty_test", COLUMN_NAME_TXN_NONE, true);
                writer.commit();
            }

            try (RoaringBitmapIndexFwdReader reader = new RoaringBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "empty_test", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(0, reader.getKeyCount());

                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    @Test
    public void testBackwardReaderSimple() {
        // Simplest backward read test
        try (Path path = new Path().of(root)) {
            final int plen = path.size();

            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration)) {
                writer.of(path, "bwd_simple_test", COLUMN_NAME_TXN_NONE, true);
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(0, 30);
                writer.commit();
            }

            // First verify forward reader works
            try (RoaringBitmapIndexFwdReader fwdReader = new RoaringBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "bwd_simple_test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                Assert.assertEquals("Forward reader should have 1 key", 1, fwdReader.getKeyCount());
                RowCursor fwdCursor = fwdReader.getCursor(true, 0, 0, Long.MAX_VALUE);
                LongList fwdValues = new LongList();
                while (fwdCursor.hasNext()) {
                    fwdValues.add(fwdCursor.next());
                }
                Assert.assertEquals("Forward reader should have 3 values", 3, fwdValues.size());
            }

            // Read backward
            try (RoaringBitmapIndexBwdReader reader = new RoaringBitmapIndexBwdReader(
                    configuration, path.trimTo(plen), "bwd_simple_test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                Assert.assertEquals("Backward reader should have 1 key", 1, reader.getKeyCount());

                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                LongList values = new LongList();
                while (cursor.hasNext()) {
                    values.add(cursor.next());
                }

                Assert.assertEquals(3, values.size());
                Assert.assertEquals(30, values.get(0));
                Assert.assertEquals(20, values.get(1));
                Assert.assertEquals(10, values.get(2));
            }
        }
    }

    @Test
    public void testConcurrentReadWhileAppending() throws Exception {
        // Test that reader can read consistent data while writer is appending concurrently
        final int totalWrites = 10000;
        final int batchSize = 100;
        final java.util.concurrent.atomic.AtomicReference<Throwable> writerError = new java.util.concurrent.atomic.AtomicReference<>();
        final java.util.concurrent.atomic.AtomicReference<Throwable> readerError = new java.util.concurrent.atomic.AtomicReference<>();
        final java.util.concurrent.atomic.AtomicBoolean writerDone = new java.util.concurrent.atomic.AtomicBoolean(false);
        final java.util.concurrent.CountDownLatch readerReady = new java.util.concurrent.CountDownLatch(1);

        try (Path writerPath = new Path().of(root);
             Path readerPath = new Path().of(root)) {

            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration)) {
                writer.of(writerPath, "concurrent_test", COLUMN_NAME_TXN_NONE, true);

                // Initial write so reader can open
                writer.add(0, 0);
                writer.commit();

                // Writer thread: continuously append and commit
                Thread writerThread = new Thread(() -> {
                    try {
                        // Wait for reader to be ready
                        readerReady.await();

                        for (int i = 1; i < totalWrites; i++) {
                            writer.add(0, i);
                            if (i % batchSize == 0) {
                                writer.commit();
                            }
                        }
                        writer.commit();
                    } catch (Throwable t) {
                        writerError.set(t);
                    } finally {
                        writerDone.set(true);
                    }
                });

                // Reader thread: continuously read and verify consistency
                Thread readerThread = new Thread(() -> {
                    try (RoaringBitmapIndexFwdReader reader = new RoaringBitmapIndexFwdReader(
                            configuration, readerPath, "concurrent_test", COLUMN_NAME_TXN_NONE, -1, 0)) {

                        readerReady.countDown();
                        int readIterations = 0;

                        while (!writerDone.get() || readIterations < 10) {
                            reader.reloadConditionally();
                            RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);

                            int count = 0;
                            long lastValue = -1;
                            while (cursor.hasNext()) {
                                long value = cursor.next();
                                // Verify ascending order (consistency check)
                                if (value <= lastValue) {
                                    throw new AssertionError("Values not ascending: " + lastValue + " -> " + value);
                                }
                                // Verify value equals index (no gaps or corruption)
                                if (value != count) {
                                    throw new AssertionError("Expected value " + count + " but got " + value);
                                }
                                lastValue = value;
                                count++;
                            }

                            readIterations++;
                            Thread.yield();  // Allow writer to make progress
                        }

                        // Final read should see all values
                        reader.reloadConditionally();
                        RowCursor finalCursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                        int finalCount = 0;
                        while (finalCursor.hasNext()) {
                            finalCursor.next();
                            finalCount++;
                        }
                        Assert.assertEquals("Final read should see all values", totalWrites, finalCount);

                    } catch (Throwable t) {
                        readerError.set(t);
                    }
                });

                writerThread.start();
                readerThread.start();

                writerThread.join(30000);
                readerThread.join(30000);

                if (writerError.get() != null) {
                    throw new AssertionError("Writer failed", writerError.get());
                }
                if (readerError.get() != null) {
                    throw new AssertionError("Reader failed", readerError.get());
                }
            }
        }
    }
}
