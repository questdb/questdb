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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DeltaBitmapIndexBwdReader;
import io.questdb.cairo.DeltaBitmapIndexFwdReader;
import io.questdb.cairo.DeltaBitmapIndexUtils;
import io.questdb.cairo.DeltaBitmapIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class DeltaBitmapIndexTest extends AbstractCairoTest {
    private Path path;
    private int plen;

    public static void create(CairoConfiguration configuration, Path path, CharSequence name) {
        int plen = path.size();
        try {
            final FilesFacade ff = configuration.getFilesFacade();
            try (
                    MemoryMA mem = Vm.getSmallCMARWInstance(
                            ff,
                            DeltaBitmapIndexUtils.keyFileName(path, name, COLUMN_NAME_TXN_NONE),
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                DeltaBitmapIndexWriter.initKeyMemory(mem);
            }
            ff.touch(DeltaBitmapIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    @Before
    public void setUp() {
        path = new Path().of(configuration.getDbRoot());
        plen = path.size();
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        path = Misc.free(path);
        super.tearDown();
    }

    @Test
    public void testAdd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 1000);
                writer.add(256, 1234);
                writer.add(64, 10);
                writer.add(64, 987);
                writer.add(256, 5567);
                writer.add(64, 991);
                writer.add(64, 992);
                writer.add(64, 993);

                assertThat("[5567,1234]", writer.getCursor(256), list);
                assertThat("[993,992,991,987,10]", writer.getCursor(64), list);
                assertThat("[1000]", writer.getCursor(0), list);
            }

            // Verify with readers
            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[5567,1234]", reader.getCursor(true, 256, 0, Long.MAX_VALUE), list);
                assertThat("[993,992,991,987,10]", reader.getCursor(true, 64, 0, Long.MAX_VALUE), list);
                assertThat("[1000]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[1234,5567]", reader.getCursor(true, 256, 0, Long.MAX_VALUE), list);
                assertThat("[10,987,991,992,993]", reader.getCursor(true, 64, 0, Long.MAX_VALUE), list);
                assertThat("[1000]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testAdd1MValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int maxKeys = 1024;
            int N = 1000000;

            IntList keys = new IntList();
            IntObjHashMap<LongList> lists = new IntObjHashMap<>();

            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Track last value per key to ensure ascending order
                LongList lastValuePerKey = new LongList();
                for (int i = 0; i < maxKeys; i++) {
                    lastValuePerKey.add(0);
                }

                for (int i = 0; i < N; i++) {
                    int key = i % maxKeys;
                    long lastVal = lastValuePerKey.getQuick(key);
                    long value = lastVal + 1 + rnd.nextPositiveInt() % 100; // Ensure ascending
                    lastValuePerKey.setQuick(key, value);

                    writer.add(key, value);

                    LongList list = lists.get(key);
                    if (list == null) {
                        lists.put(key, list = new LongList());
                        keys.add(key);
                    }
                    list.add(value);
                }
            }

            // Read values backward and compare
            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    LongList list = lists.get(keys.getQuick(i));
                    Assert.assertNotNull(list);
                    RowCursor cursor = reader.getCursor(true, keys.getQuick(i), 0, Long.MAX_VALUE);
                    int z = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertTrue(z > -1);
                        Assert.assertEquals(list.getQuick(z - 1), cursor.next());
                        z--;
                    }
                    Assert.assertEquals(0, z);
                }
            }

            // Read values forward and compare
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    LongList list = lists.get(keys.getQuick(i));
                    Assert.assertNotNull(list);
                    RowCursor cursor = reader.getCursor(true, keys.getQuick(i), 0, Long.MAX_VALUE);
                    int z = 0;
                    int sz = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertTrue(z < sz);
                        Assert.assertEquals(list.getQuick(z), cursor.next());
                        z++;
                    }
                    Assert.assertEquals(sz, z);
                }
            }
        });
    }

    @Test
    public void testBackwardCursorTimeout() throws Exception {
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public long getSpinLockTimeout() {
                return 1;
            }
        };
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter w = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                w.add(0, 10);
            }

            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                    try (Path path = new Path()) {
                        path.of(configuration.getDbRoot()).concat("x").put(".dk");
                        FilesFacade ff = configuration.getFilesFacade();
                        mem.of(ff, path.$(), ff.getMapPageSize(), ff.length(path.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                    }
                    mem.putLong(DeltaBitmapIndexUtils.getKeyEntryOffset(0) + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, 10);

                    try {
                        reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                        Assert.fail();
                    } catch (CairoException e) {
                        Assert.assertTrue(Chars.contains(e.getMessage(), "cursor could not consistently read index header"));
                    }
                }
            }
        });
    }

    @Test
    public void testBackwardReaderConstructorBadSig() throws Exception {
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public long getSpinLockTimeout() {
                return 1;
            }
        };
        TestUtils.assertMemoryLeak(() -> {
            try (MemoryMA mem = openKey()) {
                mem.skip(DeltaBitmapIndexUtils.KEY_FILE_RESERVED);
            }
            assertBackwardReaderConstructorFail(configuration, "Unknown format");
            assertForwardReaderConstructorFail(configuration, "Unknown format");
        });
    }

    @Test
    public void testCompressionRatioSequentialData() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 100000;

            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Add sequential row IDs (common in time-series)
                for (int i = 0; i < N; i++) {
                    writer.add(0, i);
                }

                long valueMemSize = writer.getValueMemSize();
                long uncompressedSize = (long) N * 8; // 8 bytes per value

                // First value is 8 bytes, remaining 99999 values are 1 byte each (delta=1)
                // Expected: 8 + 99999 = 100007 bytes
                // Compression ratio should be approximately 8x
                double ratio = (double) uncompressedSize / valueMemSize;
                Assert.assertTrue("Expected compression ratio > 2, got " + ratio, ratio > 2);
            }
        });
    }

    @Test
    public void testConcurrentBackwardReadWhileAppending() throws Exception {
        // Test that backward reader can read consistent data while writer is appending
        final int totalWrites = 10000;
        final AtomicReference<Throwable> writerError = new AtomicReference<>();
        final AtomicReference<Throwable> readerError = new AtomicReference<>();
        final AtomicBoolean writerDone = new AtomicBoolean(false);
        final CountDownLatch readerReady = new CountDownLatch(1);

        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // Initial write so reader can open
                writer.add(0, 0);

                // Writer thread: continuously append
                Thread writerThread = new Thread(() -> {
                    try {
                        readerReady.await();
                        for (int i = 1; i < totalWrites; i++) {
                            writer.add(0, i);
                        }
                    } catch (Throwable t) {
                        writerError.set(t);
                    } finally {
                        writerDone.set(true);
                    }
                });

                // Reader thread: continuously read backward and verify consistency
                Thread readerThread = new Thread(() -> {
                    try (Path readerPath = new Path().of(configuration.getDbRoot())) {
                        try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(
                                configuration, readerPath, "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                            readerReady.countDown();
                            int readIterations = 0;

                            while (!writerDone.get() || readIterations < 10) {
                                reader.reloadConditionally();
                                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);

                                LongList values = new LongList();
                                while (cursor.hasNext()) {
                                    values.add(cursor.next());
                                }

                                // Verify descending order (backward cursor)
                                for (int i = 0; i < values.size() - 1; i++) {
                                    if (values.getQuick(i) <= values.getQuick(i + 1)) {
                                        throw new AssertionError("Values not descending at index " + i);
                                    }
                                }

                                // Verify values are consistent (no gaps when reversed)
                                int count = values.size();
                                for (int i = 0; i < count; i++) {
                                    long expected = count - 1 - i;
                                    long actual = values.getQuick(i);
                                    if (actual != expected) {
                                        throw new AssertionError("Expected " + expected + " but got " + actual + " at position " + i);
                                    }
                                }

                                readIterations++;
                                Thread.yield();
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
                        }
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
        });
    }

    @Test
    public void testConcurrentForwardReadWhileAppending() throws Exception {
        // Test that forward reader can read consistent data while writer is appending
        final int totalWrites = 10000;
        final AtomicReference<Throwable> writerError = new AtomicReference<>();
        final AtomicReference<Throwable> readerError = new AtomicReference<>();
        final AtomicBoolean writerDone = new AtomicBoolean(false);
        final CountDownLatch readerReady = new CountDownLatch(1);

        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // Initial write so reader can open
                writer.add(0, 0);

                // Writer thread: continuously append
                Thread writerThread = new Thread(() -> {
                    try {
                        readerReady.await();
                        for (int i = 1; i < totalWrites; i++) {
                            writer.add(0, i);
                        }
                    } catch (Throwable t) {
                        writerError.set(t);
                    } finally {
                        writerDone.set(true);
                    }
                });

                // Reader thread: continuously read and verify consistency
                Thread readerThread = new Thread(() -> {
                    try (Path readerPath = new Path().of(configuration.getDbRoot())) {
                        try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                                configuration, readerPath, "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

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
                                Thread.yield();
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
                        }
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
        });
    }

    @Test
    public void testConcurrentMultiKeyReadWhileAppending() throws Exception {
        // Test concurrent read/write with multiple keys
        final int totalWrites = 5000;
        final int maxKeys = 10;
        final AtomicReference<Throwable> writerError = new AtomicReference<>();
        final AtomicReference<Throwable> readerError = new AtomicReference<>();
        final AtomicBoolean writerDone = new AtomicBoolean(false);
        final CountDownLatch readerReady = new CountDownLatch(2);  // 2 reader threads

        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            IntList keys = new IntList();
            IntObjHashMap<LongList> expectedValues = new IntObjHashMap<>();

            // Pre-compute values to write
            for (int i = 0; i < totalWrites; i++) {
                int key = rnd.nextPositiveInt() % maxKeys;
                LongList list = expectedValues.get(key);
                if (list == null) {
                    list = new LongList();
                    expectedValues.put(key, list);
                    keys.add(key);
                }
                list.add(i);
            }

            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // Writer thread
                Thread writerThread = new Thread(() -> {
                    try {
                        readerReady.await();
                        Rnd writerRnd = new Rnd();
                        for (int i = 0; i < totalWrites; i++) {
                            int key = writerRnd.nextPositiveInt() % maxKeys;
                            writer.add(key, i);
                        }
                    } catch (Throwable t) {
                        writerError.set(t);
                    } finally {
                        writerDone.set(true);
                    }
                });

                // Forward reader thread
                Thread fwdReaderThread = new Thread(() -> {
                    try (Path readerPath = new Path().of(configuration.getDbRoot())) {
                        try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                                configuration, readerPath, "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                            readerReady.countDown();
                            while (!writerDone.get()) {
                                reader.reloadConditionally();
                                for (int i = 0; i < keys.size(); i++) {
                                    int key = keys.getQuick(i);
                                    LongList expected = expectedValues.get(key);
                                    RowCursor cursor = reader.getCursor(false, key, 0, Long.MAX_VALUE);

                                    int count = 0;
                                    long lastValue = -1;
                                    while (cursor.hasNext()) {
                                        long value = cursor.next();
                                        // Verify ascending order
                                        if (value <= lastValue) {
                                            throw new AssertionError("Key " + key + ": values not ascending");
                                        }
                                        // Verify value matches expected
                                        if (count < expected.size() && value != expected.getQuick(count)) {
                                            throw new AssertionError("Key " + key + ": expected " + expected.getQuick(count) + " but got " + value);
                                        }
                                        lastValue = value;
                                        count++;
                                    }
                                }
                                Thread.yield();
                            }
                        }
                    } catch (Throwable t) {
                        readerError.set(t);
                    }
                });

                // Backward reader thread
                Thread bwdReaderThread = new Thread(() -> {
                    try (Path readerPath = new Path().of(configuration.getDbRoot())) {
                        try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(
                                configuration, readerPath, "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                            readerReady.countDown();
                            while (!writerDone.get()) {
                                reader.reloadConditionally();
                                for (int i = 0; i < keys.size(); i++) {
                                    int key = keys.getQuick(i);
                                    RowCursor cursor = reader.getCursor(false, key, 0, Long.MAX_VALUE);

                                    long lastValue = Long.MAX_VALUE;
                                    while (cursor.hasNext()) {
                                        long value = cursor.next();
                                        // Verify descending order
                                        if (value >= lastValue) {
                                            throw new AssertionError("Key " + key + ": values not descending");
                                        }
                                        lastValue = value;
                                    }
                                }
                                Thread.yield();
                            }
                        }
                    } catch (Throwable t) {
                        if (readerError.get() == null) {
                            readerError.set(t);
                        }
                    }
                });

                writerThread.start();
                fwdReaderThread.start();
                bwdReaderThread.start();

                writerThread.join(30000);
                fwdReaderThread.join(30000);
                bwdReaderThread.join(30000);

                if (writerError.get() != null) {
                    throw new AssertionError("Writer failed", writerError.get());
                }
                if (readerError.get() != null) {
                    throw new AssertionError("Reader failed", readerError.get());
                }
            }
        });
    }

    @Test
    public void testDeltaEncodingDecoding() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Test various delta sizes
                long base = 0;

                // 1-byte deltas (0-127)
                writer.add(0, base);
                writer.add(0, base + 1);      // delta = 1
                writer.add(0, base + 128);    // delta = 127

                // 2-byte deltas (128-16383)
                writer.add(0, base + 256);    // delta = 128
                writer.add(0, base + 16639);  // delta = 16383

                // 4-byte deltas (16384-536870911)
                writer.add(0, base + 33022);  // delta = 16383
                writer.add(0, base + 536903933); // larger delta

                // Verify values can be read back correctly
                LongList list = new LongList();
                RowCursor cursor = writer.getCursor(0);
                while (cursor.hasNext()) {
                    list.add(cursor.next());
                }

                Assert.assertEquals(7, list.size());
                Assert.assertEquals(536903933, list.getQuick(0)); // backward cursor, last value first
                Assert.assertEquals(33022, list.getQuick(1));
                Assert.assertEquals(16639, list.getQuick(2));
                Assert.assertEquals(256, list.getQuick(3));
                Assert.assertEquals(128, list.getQuick(4));
                Assert.assertEquals(1, list.getQuick(5));
                Assert.assertEquals(0, list.getQuick(6));
            }
        });
    }

    @Test
    public void testEmptyBackwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertEmptyCursor(reader);
            }
        });
    }

    @Test
    public void testEmptyForwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertEmptyCursor(reader);
            }
        });
    }

    @Test
    public void testForwardCursorTimeout() throws Exception {
        CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public long getSpinLockTimeout() {
                return 1;
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter w = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                w.add(0, 10);
            }

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                    try (Path path = new Path()) {
                        path.of(configuration.getDbRoot()).concat("x").put(".dk");
                        FilesFacade ff = configuration.getFilesFacade();
                        mem.of(ff, path.$(), ff.getMapPageSize(), ff.length(path.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                    }

                    long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(0);
                    mem.putLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, 10);

                    try {
                        reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                        Assert.fail();
                    } catch (CairoException e) {
                        Assert.assertTrue(Chars.contains(e.getMessage(), "cursor could not consistently read index header"));
                    }
                }
            }
        });
    }

    @Test
    public void testMinMaxBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(0, 30);
                writer.add(0, 40);
                writer.add(0, 50);
            }

            // Note: cursor.next() returns (value - minValue), so values are relative to minValue
            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                // All values (minValue=0, so no offset)
                assertThat("[50,40,30,20,10]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                // Bounded (minValue=20, so values 20,30,40 become 0,10,20)
                assertThat("[20,10,0]", reader.getCursor(true, 0, 20, 40), list);
                // Min bound only (minValue=30, so values 30,40,50 become 0,10,20)
                assertThat("[20,10,0]", reader.getCursor(true, 0, 30, Long.MAX_VALUE), list);
                // Max bound only (minValue=0, so no offset)
                assertThat("[30,20,10]", reader.getCursor(true, 0, 0, 30), list);
                // Single value (minValue=30, so value 30 becomes 0)
                assertThat("[0]", reader.getCursor(true, 0, 30, 30), list);
                // No match
                assertThat("[]", reader.getCursor(true, 0, 100, 200), list);
            }

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                // All values (minValue=0, so no offset)
                assertThat("[10,20,30,40,50]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                // Bounded (minValue=20, so values 20,30,40 become 0,10,20)
                assertThat("[0,10,20]", reader.getCursor(true, 0, 20, 40), list);
                // Min bound only (minValue=30, so values 30,40,50 become 0,10,20)
                assertThat("[0,10,20]", reader.getCursor(true, 0, 30, Long.MAX_VALUE), list);
                // Max bound only (minValue=0, so no offset)
                assertThat("[10,20,30]", reader.getCursor(true, 0, 0, 30), list);
                // Single value (minValue=30, so value 30 becomes 0)
                assertThat("[0]", reader.getCursor(true, 0, 30, 30), list);
                // No match
                assertThat("[]", reader.getCursor(true, 0, 100, 200), list);
            }
        });
    }

    @Test
    public void testReopenExistingIndex() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            // Write initial values
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
                writer.add(1, 150);
            }

            // Reopen and add more values
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 300);
                writer.add(1, 250);

                assertThat("[300,200,100]", writer.getCursor(0), list);
                assertThat("[250,150]", writer.getCursor(1), list);
            }

            // Verify with readers
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[100,200,300]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[150,250]", reader.getCursor(true, 1, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testRollbackValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(0, 30);
                writer.add(1, 15);
                writer.add(1, 25);
                writer.add(1, 35);

                // Rollback to max value 25
                writer.rollbackValues(25);

                assertThat("[20,10]", writer.getCursor(0), list);
                assertThat("[25,15]", writer.getCursor(1), list);
            }

            // Verify with readers
            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[20,10]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[25,15]", reader.getCursor(true, 1, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testSparseKeys() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Add to sparse keys (key 0, 100, 500)
                writer.add(0, 10);
                writer.add(100, 20);
                writer.add(500, 30);

                assertThat("[10]", writer.getCursor(0), list);
                assertThat("[20]", writer.getCursor(100), list);
                assertThat("[30]", writer.getCursor(500), list);
                // Keys in between should be empty
                assertThat("[]", writer.getCursor(50), list);
                assertThat("[]", writer.getCursor(200), list);
            }

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[10]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[20]", reader.getCursor(true, 100, 0, Long.MAX_VALUE), list);
                assertThat("[30]", reader.getCursor(true, 500, 0, Long.MAX_VALUE), list);
                assertThat("[]", reader.getCursor(true, 50, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
                writer.add(1, 150);

                assertThat("[200,100]", writer.getCursor(0), list);

                writer.truncate();

                Assert.assertEquals(0, writer.getKeyCount());
                assertThat("[]", writer.getCursor(0), list);
                assertThat("[]", writer.getCursor(1), list);

                // Add values after truncate
                writer.add(0, 300);
                assertThat("[300]", writer.getCursor(0), list);
            }
        });
    }

    private void assertBackwardReaderConstructorFail(CairoConfiguration configuration, CharSequence contains) {
        try {
            new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
            Assert.fail();
        } catch (CairoException e) {
            Assert.assertTrue(Chars.contains(e.getMessage(), contains));
        }
    }

    private void assertEmptyCursor(io.questdb.cairo.BitmapIndexReader reader) {
        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
        Assert.assertFalse(cursor.hasNext());
    }

    private void assertForwardReaderConstructorFail(CairoConfiguration configuration, CharSequence contains) {
        try {
            new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
            Assert.fail();
        } catch (CairoException e) {
            Assert.assertTrue(Chars.contains(e.getMessage(), contains));
        }
    }

    private void assertThat(String expected, RowCursor cursor, LongList temp) {
        temp.clear();
        while (cursor.hasNext()) {
            temp.add(cursor.next());
        }
        Assert.assertEquals(expected, temp.toString());
    }

    private MemoryMA openKey() {
        try (Path path = new Path()) {
            MemoryMA mem = Vm.getSmallCMARWInstance(
                    configuration.getFilesFacade(),
                    path.of(configuration.getDbRoot()).concat("x").put(".dk").$(),
                    MemoryTag.MMAP_DEFAULT,
                    configuration.getWriterFileOpenOpts()
            );
            mem.toTop();
            return mem;
        }
    }
}
