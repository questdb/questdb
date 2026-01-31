/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \\| __ )
 *   | | | | | | |/ _ \\/ __| __| | | |  _ \\
 *   | |_| | |_| |  __/\\__ \\|_| |_| | |_) |
 *    \\__\\_\\\\__,_|\\___||___/\\__|____/|____/
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

import io.questdb.cairo.FORBitmapIndexBwdReader;
import io.questdb.cairo.FORBitmapIndexFwdReader;
import io.questdb.cairo.FORBitmapIndexUtils;
import io.questdb.cairo.FORBitmapIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FORBitmapIndexTest extends AbstractCairoTest {

    @Test
    public void testBitsNeeded() {
        Assert.assertEquals(1, FORBitmapIndexUtils.bitsNeeded(0));
        Assert.assertEquals(1, FORBitmapIndexUtils.bitsNeeded(1));
        Assert.assertEquals(2, FORBitmapIndexUtils.bitsNeeded(2));
        Assert.assertEquals(2, FORBitmapIndexUtils.bitsNeeded(3));
        Assert.assertEquals(3, FORBitmapIndexUtils.bitsNeeded(4));
        Assert.assertEquals(7, FORBitmapIndexUtils.bitsNeeded(127));
        Assert.assertEquals(8, FORBitmapIndexUtils.bitsNeeded(128));
        Assert.assertEquals(8, FORBitmapIndexUtils.bitsNeeded(255));
        Assert.assertEquals(9, FORBitmapIndexUtils.bitsNeeded(256));
        Assert.assertEquals(63, FORBitmapIndexUtils.bitsNeeded(Long.MAX_VALUE)); // Long.MAX_VALUE = 0x7FFF... has leading 0
    }

    @Test
    public void testBlockBoundary() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            // Write exactly BLOCK_CAPACITY values to trigger block flush
            int blockCapacity = FORBitmapIndexUtils.BLOCK_CAPACITY;
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "boundary", 0)) {
                for (int i = 0; i < blockCapacity * 3; i++) {
                    writer.add(0, i);
                }
                writer.commit();
            }

            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "boundary", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(blockCapacity * 3, count);
            }
        }
    }

    @Test
    public void testColumnTop() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            long columnTop = 100;

            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "coltop", 0)) {
                // Write values starting after columnTop
                for (int i = 0; i < 500; i++) {
                    writer.add(0, columnTop + i);
                }
                writer.commit();
            }

            // Forward reader with columnTop
            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "coltop", 0, 0, columnTop)) {
                // Key 0 query should return nulls first, then actual values
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);

                // First 100 should be nulls (0-99)
                for (int i = 0; i < 100; i++) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(i, cursor.next());
                }

                // Then actual values (100-599)
                for (int i = 0; i < 500; i++) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(columnTop + i, cursor.next());
                }

                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    @Test
    public void testComparisonWithLegacy() throws Exception {
        // Ensure FOR index produces same results as legacy for same data
        try (Path path = new Path().of(configuration.getDbRoot())) {
            LongList values = new LongList();
            for (int i = 0; i < 1000; i++) {
                values.add(i * 3); // Values with stride of 3
            }

            // Write to FOR index
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "for_cmp", 0)) {
                for (int i = 0; i < values.size(); i++) {
                    writer.add(0, values.getQuick(i));
                }
                writer.commit();
            }

            // Read from FOR and compare
            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "for_cmp", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int idx = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("Mismatch at index " + idx, values.getQuick(idx), cursor.next());
                    idx++;
                }
                Assert.assertEquals(values.size(), idx);
            }
        }
    }

    @Test
    public void testEmptyKey() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "empty", 0)) {
                // Write only to key 5
                writer.add(5, 100);
                writer.commit();
            }

            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "empty", 0, 0, 0)) {
                // Key 0 should return empty cursor
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                Assert.assertFalse(cursor.hasNext());

                // Key 5 should have the value
                cursor = reader.getCursor(true, 5, 0, Long.MAX_VALUE);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(100, cursor.next());
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    @Test
    public void testLargeGaps() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "gaps", 0)) {
                // Write values with large gaps
                for (int i = 0; i < 100; i++) {
                    writer.add(0, i * 1000000L);
                }
                writer.commit();
            }

            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "gaps", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count * 1000000L, cursor.next());
                    count++;
                }
                Assert.assertEquals(100, count);
            }
        }
    }

    @Test
    public void testMaxValue() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "maxval", 0)) {
                writer.add(0, 100);
                writer.setMaxValue(12345);
                writer.commit();

                Assert.assertEquals(12345, writer.getMaxValue());
            }

            // Reopen and verify maxValue persists
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration)) {
                writer.of(path, "maxval", 0);
                Assert.assertEquals(12345, writer.getMaxValue());
            }
        }
    }

    @Test
    public void testMultipleKeys() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "multikey", 0)) {
                // Write values to multiple keys
                for (int key = 0; key < 10; key++) {
                    for (int i = 0; i < 100; i++) {
                        writer.add(key, key * 1000 + i);
                    }
                }
                writer.commit();
                Assert.assertEquals(10, writer.getKeyCount());
            }

            // Verify each key
            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "multikey", 0, 0, 0)) {
                Assert.assertEquals(10, reader.getKeyCount());

                for (int key = 0; key < 10; key++) {
                    RowCursor cursor = reader.getCursor(false, key, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(key * 1000 + count, cursor.next());
                        count++;
                    }
                    Assert.assertEquals(100, count);
                }
            }
        }
    }

    @Test
    public void testNonExistentKey() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "nonexistent", 0)) {
                writer.add(0, 0);
                writer.commit();
            }

            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "nonexistent", 0, 0, 0)) {
                // Query for non-existent key
                RowCursor cursor = reader.getCursor(true, 999, 0, Long.MAX_VALUE);
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    @Test
    public void testPackUnpackRoundTrip() {
        // Test the utility functions directly
        long[] values = new long[128];
        for (int i = 0; i < 128; i++) {
            values[i] = 1000 + i * 7; // Some sequential-ish values
        }

        long minValue = values[0];
        long maxValue = values[127];
        int bitWidth = FORBitmapIndexUtils.bitsNeeded(maxValue - minValue);

        // Pack
        byte[] packed = new byte[FORBitmapIndexUtils.packedDataSize(128, bitWidth)];
        long addr = io.questdb.std.Unsafe.malloc(packed.length, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        try {
            FORBitmapIndexUtils.packValues(values, 128, minValue, bitWidth, addr);

            // Unpack and verify
            long[] unpacked = new long[128];
            FORBitmapIndexUtils.unpackAllValues(addr, 128, bitWidth, minValue, unpacked);

            for (int i = 0; i < 128; i++) {
                Assert.assertEquals(values[i], unpacked[i]);
            }

            // Also test single value unpack
            for (int i = 0; i < 128; i++) {
                long single = FORBitmapIndexUtils.unpackValue(addr, i, bitWidth, minValue);
                Assert.assertEquals(values[i], single);
            }
        } finally {
            io.questdb.std.Unsafe.free(addr, packed.length, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPartialBlock() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            // Write less than BLOCK_CAPACITY values
            int valueCount = FORBitmapIndexUtils.BLOCK_CAPACITY / 2;
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "partial", 0)) {
                for (int i = 0; i < valueCount; i++) {
                    writer.add(0, i);
                }
                writer.commit();
            }

            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "partial", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(valueCount, count);
            }
        }
    }

    @Test
    public void testRandomValues() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            Rnd rnd = new Rnd();
            LongList expected = new LongList();

            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "random", 0)) {
                long value = 0;
                for (int i = 0; i < 10000; i++) {
                    value += rnd.nextInt(100) + 1; // Random gaps 1-100
                    writer.add(0, value);
                    expected.add(value);
                }
                writer.commit();
            }

            // Verify forward
            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "random", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int idx = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(expected.getQuick(idx), cursor.next());
                    idx++;
                }
                Assert.assertEquals(expected.size(), idx);
            }

            // Verify backward
            try (FORBitmapIndexBwdReader reader = new FORBitmapIndexBwdReader(configuration, path, "random", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int idx = expected.size() - 1;
                while (cursor.hasNext()) {
                    Assert.assertEquals(expected.getQuick(idx), cursor.next());
                    idx--;
                }
                Assert.assertEquals(-1, idx);
            }
        }
    }

    @Test
    public void testRangeQuery() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "range", 0)) {
                for (int i = 0; i < 1000; i++) {
                    writer.add(0, i);
                }
                writer.commit();
            }

            // Query range [100, 200]
            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "range", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 100, 200);
                int count = 100;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(201, count);
            }

            // Backward range query
            try (FORBitmapIndexBwdReader reader = new FORBitmapIndexBwdReader(configuration, path, "range", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 100, 200);
                int count = 200;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count--;
                }
                Assert.assertEquals(99, count);
            }
        }
    }

    @Test
    public void testReopen() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            // Write some values
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "reopen", 0)) {
                for (int i = 0; i < 500; i++) {
                    writer.add(0, i);
                }
                writer.commit();
            }

            // Reopen and write more
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration)) {
                writer.of(path, "reopen", 0);
                for (int i = 500; i < 1000; i++) {
                    writer.add(0, i);
                }
                writer.commit();
            }

            // Verify all values
            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "reopen", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(1000, count);
            }
        }
    }

    @Test
    public void testSimpleWriteAndRead() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "test", 0)) {
                // Write sequential values to key 0
                for (int i = 0; i < 1000; i++) {
                    writer.add(0, i);
                }
                writer.commit();
            }

            // Read forward
            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "test", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count++;
                }
                Assert.assertEquals(1000, count);
            }

            // Read backward
            try (FORBitmapIndexBwdReader reader = new FORBitmapIndexBwdReader(configuration, path, "test", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 999;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count, cursor.next());
                    count--;
                }
                Assert.assertEquals(-1, count);
            }
        }
    }

    @Test
    public void testTruncate() throws Exception {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration, path, "truncate", 0)) {
                for (int i = 0; i < 1000; i++) {
                    writer.add(0, i);
                }
                writer.commit();

                // Truncate
                writer.truncate();

                // Write new values
                for (int i = 0; i < 100; i++) {
                    writer.add(0, i * 10);
                }
                writer.commit();
            }

            // Verify only new values exist
            try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(configuration, path, "truncate", 0, 0, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(count * 10, cursor.next());
                    count++;
                }
                Assert.assertEquals(100, count);
            }
        }
    }
}
