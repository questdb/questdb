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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.idx.BitmapIndexReader;
import io.questdb.cairo.idx.DeltaBitmapIndexBwdReader;
import io.questdb.cairo.idx.DeltaBitmapIndexFwdReader;
import io.questdb.cairo.idx.DeltaBitmapIndexUtils;
import io.questdb.cairo.idx.DeltaBitmapIndexWriter;
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
            assertBackwardReaderConstructorFail(configuration);
            assertForwardReaderConstructorFail(configuration);
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

    // ==================== NEW EDGE CASE TESTS ====================

    @Test
    public void testDeltaEncodingBoundaries() throws Exception {
        // Test exact boundary values for delta encoding:
        // 1-byte: 0-127
        // 2-byte: 128-16383
        // 4-byte: 16384-536870911
        // 9-byte: > 536870911
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Key 0: 1-byte boundary tests (delta 0-127)
                writer.add(0, 0);       // first value
                writer.add(0, 127);     // delta = 127 (max 1-byte)
                writer.add(0, 128);     // delta = 1 (min 1-byte)
                writer.add(0, 128);     // delta = 0 (zero delta)

                // Key 1: 2-byte boundary tests (delta 128-16383)
                writer.add(1, 0);
                writer.add(1, 128);     // delta = 128 (min 2-byte)
                writer.add(1, 16511);   // delta = 16383 (max 2-byte): 16511 - 128 = 16383
                writer.add(1, 32894);   // delta = 16383 (max 2-byte): 32894 - 16511 = 16383

                // Key 2: 4-byte boundary tests (delta 16384-536870911)
                writer.add(2, 0);
                writer.add(2, 16384);           // delta = 16384 (min 4-byte)
                writer.add(2, 536887295);       // delta = 536870911 (max 4-byte): 536887295 - 16384 = 536870911

                // Key 3: 9-byte encoding (delta > 536870911)
                writer.add(3, 0);
                writer.add(3, 536870912L);              // delta = 536870912 (min 9-byte)
                writer.add(3, 1000536870912L);          // delta = 1 trillion

                // Verify all values can be read back correctly (backward cursor)
                assertThat("[128,128,127,0]", writer.getCursor(0), list);
                assertThat("[32894,16511,128,0]", writer.getCursor(1), list);
                assertThat("[536887295,16384,0]", writer.getCursor(2), list);
                assertThat("[1000536870912,536870912,0]", writer.getCursor(3), list);
            }

            // Verify with forward reader
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[0,127,128,128]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[0,128,16511,32894]", reader.getCursor(true, 1, 0, Long.MAX_VALUE), list);
                assertThat("[0,16384,536887295]", reader.getCursor(true, 2, 0, Long.MAX_VALUE), list);
                assertThat("[0,536870912,1000536870912]", reader.getCursor(true, 3, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testEncodeDeltaNegativeThrowsException() throws Exception {
        // Test that encodeDelta() throws for negative delta (direct call to utility method)
        // This exercises L189 in DeltaBitmapIndexUtils
        TestUtils.assertMemoryLeak(() -> {
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    configuration.getFilesFacade(),
                    path.trimTo(plen).concat("test.bin").$(),
                    MemoryTag.MMAP_DEFAULT,
                    configuration.getWriterFileOpenOpts())) {
                try {
                    DeltaBitmapIndexUtils.encodeDelta(mem, -1);
                    Assert.fail("Expected CairoException for negative delta");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "delta index value cannot be negative"));
                }
            }
        });
    }

    @Test
    public void testEncodeDeltaOverflowThrowsException() throws Exception {
        // Test that encodeDelta() throws for delta > MAX_DELTA (direct call to utility method)
        // This exercises L193 in DeltaBitmapIndexUtils
        TestUtils.assertMemoryLeak(() -> {
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    configuration.getFilesFacade(),
                    path.trimTo(plen).concat("test.bin").$(),
                    MemoryTag.MMAP_DEFAULT,
                    configuration.getWriterFileOpenOpts())) {
                try {
                    DeltaBitmapIndexUtils.encodeDelta(mem, DeltaBitmapIndexUtils.MAX_DELTA + 1);
                    Assert.fail("Expected CairoException for delta overflow");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "delta index value exceeds maximum"));
                }
            }
        });
    }

    @Test
    public void testDataRelocationSlowPath() throws Exception {
        // Test the slow path in appendDeltaEncodedValue when data needs relocation
        // This happens when interleaving writes to different keys
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Write to key 0
                writer.add(0, 100);
                writer.add(0, 200);

                // Write to key 1 (now key 1's data is at the end)
                writer.add(1, 150);
                writer.add(1, 250);

                // Write to key 0 again - this triggers slow path (data relocation)
                writer.add(0, 300);
                writer.add(0, 400);

                // Write to key 1 again - also triggers slow path
                writer.add(1, 350);

                // Verify values
                assertThat("[400,300,200,100]", writer.getCursor(0), list);
                assertThat("[350,250,150]", writer.getCursor(1), list);
            }

            // Reopen and verify data is correctly persisted after relocation
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                assertThat("[400,300,200,100]", writer.getCursor(0), list);
                assertThat("[350,250,150]", writer.getCursor(1), list);

                // Add more values after reopen
                writer.add(0, 500);
                writer.add(1, 450);

                assertThat("[500,400,300,200,100]", writer.getCursor(0), list);
                assertThat("[450,350,250,150]", writer.getCursor(1), list);
            }

            // Verify with forward reader
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[100,200,300,400,500]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[150,250,350,450]", reader.getCursor(true, 1, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testRollbackClearsAllValuesForKey() throws Exception {
        // Test rollback that removes ALL values for some keys (keepCount == 0)
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(1, 100);  // All values for key 1 are > 50
                writer.add(1, 200);
                writer.add(2, 5);    // Some values for key 2 survive
                writer.add(2, 60);

                // Rollback to max value 50 - clears all values for key 1
                writer.rollbackValues(50);

                assertThat("[20,10]", writer.getCursor(0), list);
                assertThat("[]", writer.getCursor(1), list);  // Key 1 completely cleared
                assertThat("[5]", writer.getCursor(2), list);

                Assert.assertEquals(50, writer.getMaxValue());
            }

            // Verify with readers after reopen
            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[20,10]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[]", reader.getCursor(true, 1, 0, Long.MAX_VALUE), list);
                assertThat("[5]", reader.getCursor(true, 2, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testRollbackConditionallyWithZero() throws Exception {
        // Test rollbackConditionally(0) which should call truncate()
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(1, 15);
                writer.setMaxValue(20);  // maxValue must be set explicitly

                Assert.assertEquals(2, writer.getKeyCount());
                Assert.assertEquals(20, writer.getMaxValue());

                // rollbackConditionally(0) should truncate everything
                writer.rollbackConditionally(0);

                Assert.assertEquals(0, writer.getKeyCount());
                assertThat("[]", writer.getCursor(0), list);
                assertThat("[]", writer.getCursor(1), list);
            }
        });
    }

    @Test
    public void testRollbackConditionallyOnEmptyIndex() throws Exception {
        // Test rollbackConditionally on an empty index (getMaxValue() returns -1)
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                Assert.assertEquals(-1, writer.getMaxValue());
                Assert.assertEquals(0, writer.getKeyCount());

                // Should not throw and should be a no-op
                writer.rollbackConditionally(100);
                writer.rollbackConditionally(0);
                writer.rollbackConditionally(-1);

                Assert.assertEquals(0, writer.getKeyCount());
            }
        });
    }

    @Test
    public void testRollbackConditionallyNoOp() throws Exception {
        // Test rollbackConditionally when row > currentMaxRow (nothing to rollback)
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.setMaxValue(20);

                // Rollback with row > maxValue should be a no-op
                writer.rollbackConditionally(100);

                assertThat("[20,10]", writer.getCursor(0), list);
                Assert.assertEquals(20, writer.getMaxValue());
            }
        });
    }

    @Test
    public void testRollbackWithOutOfOrderKeyData() throws Exception {
        // Test rollbackValues when key data is out of order in the value file
        // This exercises the L371 condition being false (key data ends before newValueMemSize)
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Write to key 1 FIRST - its data will be at offset 0 in value file
                writer.add(1, 100);
                writer.add(1, 200);

                // Write to key 0 SECOND - its data will be at a higher offset
                writer.add(0, 300);
                writer.add(0, 400);

                // Now in value file: key 1 data at offset 0, key 0 data at higher offset
                // During rollbackValues iteration (k=0, then k=1):
                // - k=0: key 0's data at high offset, newValueMemSize tracks its end
                // - k=1: key 1's data at offset 0, ends before newValueMemSize (L371 is false)

                // Rollback with high maxValue - no values removed, just tracks data extents
                writer.rollbackValues(500);

                // Verify data is preserved
                assertThat("[400,300]", writer.getCursor(0), list);
                assertThat("[200,100]", writer.getCursor(1), list);
            }

            // Verify after reopen
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[300,400]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[100,200]", reader.getCursor(true, 1, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testSparseKeyIntermediateSlots() throws Exception {
        // Test adding values to intermediate keys that were created as empty slots
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Create key 100, which creates empty slots for keys 0-99
                writer.add(100, 1000);

                Assert.assertEquals(101, writer.getKeyCount());

                // Now add values to intermediate keys (empty slots)
                writer.add(50, 500);
                writer.add(50, 600);
                writer.add(0, 100);

                assertThat("[100]", writer.getCursor(0), list);
                assertThat("[600,500]", writer.getCursor(50), list);
                assertThat("[1000]", writer.getCursor(100), list);
                assertThat("[]", writer.getCursor(25), list);
                assertThat("[]", writer.getCursor(75), list);
            }

            // Verify after reopen
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[100]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[500,600]", reader.getCursor(true, 50, 0, Long.MAX_VALUE), list);
                assertThat("[1000]", reader.getCursor(true, 100, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testColumnTopNullCursorBackward() throws Exception {
        // Test NullCursor behavior in backward reader when columnTop > 0
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Add values for key 0 starting at row 100 (simulating columnTop = 100)
                writer.add(0, 100);
                writer.add(0, 150);
                writer.add(0, 200);
            }

            // Open reader with columnTop = 100
            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 100)) {

                // Query for key 0 from row 0 - should return actual values first, then nulls
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);

                // Backward cursor returns: real values in reverse (200, 150, 100), then nulls (99, 98, ..., 0)
                list.clear();
                int count = 0;
                while (cursor.hasNext() && count < 110) {
                    list.add(cursor.next());
                    count++;
                }

                // First 3 values should be actual data (200-0=200, 150-0=150, 100-0=100)
                Assert.assertEquals(103, list.size());  // 3 real + 100 nulls
                Assert.assertEquals(200, list.getQuick(0));
                Assert.assertEquals(150, list.getQuick(1));
                Assert.assertEquals(100, list.getQuick(2));
                // Then nulls (99, 98, ..., 0)
                Assert.assertEquals(99, list.getQuick(3));
                Assert.assertEquals(98, list.getQuick(4));
                Assert.assertEquals(0, list.getQuick(102));
            }
        });
    }

    @Test
    public void testColumnTopNullCursorForward() throws Exception {
        // Test NullCursor behavior in forward reader when columnTop > 0
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Add values for key 0 starting at row 100
                writer.add(0, 100);
                writer.add(0, 150);
                writer.add(0, 200);
            }

            // Open reader with columnTop = 100
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 100)) {

                // Query for key 0 from row 0 - should return nulls first, then actual values
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);

                list.clear();
                int count = 0;
                while (cursor.hasNext() && count < 110) {
                    list.add(cursor.next());
                    count++;
                }

                // Forward cursor returns: nulls first (0, 1, ..., 99), then real values (100, 150, 200)
                Assert.assertEquals(103, list.size());  // 100 nulls + 3 real
                // First 100 are nulls (0-99)
                Assert.assertEquals(0, list.getQuick(0));
                Assert.assertEquals(1, list.getQuick(1));
                Assert.assertEquals(99, list.getQuick(99));
                // Then real values (100, 150, 200)
                Assert.assertEquals(100, list.getQuick(100));
                Assert.assertEquals(150, list.getQuick(101));
                Assert.assertEquals(200, list.getQuick(102));

                // Query with minValue >= columnTop - should skip null region (L98 3rd cond false)
                // Note: next() returns (value - minValue), so values 100,150,200 with minValue=100 return 0,50,100
                cursor = reader.getCursor(false, 0, 100, Long.MAX_VALUE);
                list.clear();
                while (cursor.hasNext()) {
                    list.add(cursor.next());
                }
                // Should only return real values, no nulls (relative to minValue)
                Assert.assertEquals(3, list.size());
                Assert.assertEquals(0, list.getQuick(0));   // 100 - 100
                Assert.assertEquals(50, list.getQuick(1));  // 150 - 100
                Assert.assertEquals(100, list.getQuick(2)); // 200 - 100

                // Query with minValue > columnTop
                // Values 150,200 with minValue=120 return 30,80
                cursor = reader.getCursor(false, 0, 120, Long.MAX_VALUE);
                list.clear();
                while (cursor.hasNext()) {
                    list.add(cursor.next());
                }
                // Should return values >= 120 (relative to minValue)
                Assert.assertEquals(2, list.size());
                Assert.assertEquals(30, list.getQuick(0));  // 150 - 120
                Assert.assertEquals(80, list.getQuick(1));  // 200 - 120
            }
        });
    }

    @Test
    public void testZeroDelta() throws Exception {
        // Test adding the same value twice (delta = 0)
        // Note: This is actually an assertion violation in the writer
        // But we should verify the 1-byte encoding works for delta=0
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 100);  // Same value - delta = 0
                writer.add(0, 100);  // Same value again
                writer.add(0, 101);  // delta = 1

                assertThat("[101,100,100,100]", writer.getCursor(0), list);
            }

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[100,100,100,101]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testColumnNameTxnInFileName() throws Exception {
        // Test that columnNameTxn is appended to file names when > COLUMN_NAME_TXN_NONE
        // This exercises L265 in DeltaBitmapIndexUtils.keyFileName() and L276 in valueFileName()
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            long columnNameTxn = 12345L;

            // Create index files with txn suffix
            int plenSaved = path.size();
            try (
                    MemoryMA mem = Vm.getSmallCMARWInstance(
                            configuration.getFilesFacade(),
                            DeltaBitmapIndexUtils.keyFileName(path.trimTo(plenSaved), "x", columnNameTxn),
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                DeltaBitmapIndexWriter.initKeyMemory(mem);
            }
            configuration.getFilesFacade().touch(
                    DeltaBitmapIndexUtils.valueFileName(path.trimTo(plenSaved), "x", columnNameTxn)
            );

            // Open writer with txn suffix
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration)) {
                writer.of(path.trimTo(plenSaved), "x", columnNameTxn, false);
                writer.add(0, 100);
                writer.add(0, 200);
            }

            // Verify files were created with txn suffix
            try (Path checkPath = new Path()) {
                checkPath.of(configuration.getDbRoot()).concat("x").put(".dk.").put(columnNameTxn).$();
                Assert.assertTrue(configuration.getFilesFacade().exists(checkPath.$()));

                checkPath.of(configuration.getDbRoot()).concat("x").put(".dv.").put(columnNameTxn).$();
                Assert.assertTrue(configuration.getFilesFacade().exists(checkPath.$()));
            }

            // Verify data can be read back
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plenSaved), "x", columnNameTxn, -1, 0)) {
                assertThat("[100,200]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testWriterCursorVsReaderCursorConsistency() throws Exception {
        // Test that writer.getCursor() and reader cursors return same data
        TestUtils.assertMemoryLeak(() -> {
            LongList writerList = new LongList();
            LongList readerFwdList = new LongList();
            LongList readerBwdList = new LongList();

            create(configuration, path.trimTo(plen), "x");
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                Rnd rnd = new Rnd();
                long value = 0;
                for (int i = 0; i < 100; i++) {
                    value += 1 + rnd.nextPositiveInt() % 100;
                    writer.add(0, value);
                }

                // Collect from writer cursor (backward)
                RowCursor wCursor = writer.getCursor(0);
                while (wCursor.hasNext()) {
                    writerList.add(wCursor.next());
                }
            }

            // Collect from readers
            try (DeltaBitmapIndexFwdReader fwdReader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
                 DeltaBitmapIndexBwdReader bwdReader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                RowCursor fwdCursor = fwdReader.getCursor(true, 0, 0, Long.MAX_VALUE);
                while (fwdCursor.hasNext()) {
                    readerFwdList.add(fwdCursor.next());
                }

                RowCursor bwdCursor = bwdReader.getCursor(true, 0, 0, Long.MAX_VALUE);
                while (bwdCursor.hasNext()) {
                    readerBwdList.add(bwdCursor.next());
                }
            }

            // Verify: writer cursor is backward, so should match bwdReader
            Assert.assertEquals(writerList.size(), readerBwdList.size());
            Assert.assertEquals(writerList.size(), readerFwdList.size());

            for (int i = 0; i < writerList.size(); i++) {
                Assert.assertEquals("Mismatch at index " + i, writerList.getQuick(i), readerBwdList.getQuick(i));
            }

            // Forward and backward should be reverse of each other
            for (int i = 0; i < readerFwdList.size(); i++) {
                Assert.assertEquals(readerFwdList.getQuick(i), readerBwdList.getQuick(readerBwdList.size() - 1 - i));
            }
        });
    }

    @Test
    public void testLargeNumberOfKeys() throws Exception {
        // Test with 10000+ keys
        TestUtils.assertMemoryLeak(() -> {
            int numKeys = 10000;
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                for (int key = 0; key < numKeys; key++) {
                    writer.add(key, key * 10);
                    writer.add(key, key * 10 + 5);
                }

                Assert.assertEquals(numKeys, writer.getKeyCount());

                // Verify some random keys
                LongList list = new LongList();
                assertThat("[5,0]", writer.getCursor(0), list);
                assertThat("[50005,50000]", writer.getCursor(5000), list);
                assertThat("[99995,99990]", writer.getCursor(9999), list);
            }

            // Verify after reopen
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                LongList list = new LongList();
                assertThat("[0,5]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[50000,50005]", reader.getCursor(true, 5000, 0, Long.MAX_VALUE), list);
                assertThat("[99990,99995]", reader.getCursor(true, 9999, 0, Long.MAX_VALUE), list);

                Assert.assertEquals(numKeys, reader.getKeyCount());
            }
        });
    }

    @Test
    public void testGetKeyCountAndMaxValueTracking() throws Exception {
        // Test that getKeyCount and getMaxValue are correctly tracked
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                Assert.assertEquals(0, writer.getKeyCount());
                Assert.assertEquals(-1, writer.getMaxValue());

                writer.add(0, 100);
                Assert.assertEquals(1, writer.getKeyCount());

                writer.setMaxValue(100);
                Assert.assertEquals(100, writer.getMaxValue());

                writer.add(5, 200);
                Assert.assertEquals(6, writer.getKeyCount());  // keys 0-5

                writer.setMaxValue(200);
                Assert.assertEquals(200, writer.getMaxValue());

                // After rollback
                writer.rollbackValues(150);
                Assert.assertEquals(150, writer.getMaxValue());
                Assert.assertEquals(6, writer.getKeyCount());  // Key count doesn't change

                // After truncate
                writer.truncate();
                Assert.assertEquals(0, writer.getKeyCount());
                Assert.assertEquals(-1, writer.getMaxValue());
            }
        });
    }

    @Test
    public void testCloseNoTruncate() throws Exception {
        // Test closeNoTruncate behavior
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
                writer.closeNoTruncate();

                // Writer is now closed, can't use it
                Assert.assertFalse(writer.isOpen());
            }

            // Data should still be readable
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[100,200]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testNonCachedCursorInstances() throws Exception {
        // Test that non-cached cursor instances work correctly
        TestUtils.assertMemoryLeak(() -> {
            LongList list1 = new LongList();
            LongList list2 = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(1, 15);
                writer.add(1, 25);
            }

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                // Get two non-cached cursors simultaneously
                RowCursor cursor1 = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                RowCursor cursor2 = reader.getCursor(false, 1, 0, Long.MAX_VALUE);

                // Interleave reading
                Assert.assertTrue(cursor1.hasNext());
                list1.add(cursor1.next());

                Assert.assertTrue(cursor2.hasNext());
                list2.add(cursor2.next());

                Assert.assertTrue(cursor1.hasNext());
                list1.add(cursor1.next());

                Assert.assertTrue(cursor2.hasNext());
                list2.add(cursor2.next());

                Assert.assertFalse(cursor1.hasNext());
                Assert.assertFalse(cursor2.hasNext());

                Assert.assertEquals("[10,20]", list1.toString());
                Assert.assertEquals("[15,25]", list2.toString());
            }

            // Same test for backward reader
            list1.clear();
            list2.clear();

            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor1 = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                RowCursor cursor2 = reader.getCursor(false, 1, 0, Long.MAX_VALUE);

                while (cursor1.hasNext()) {
                    list1.add(cursor1.next());
                }
                while (cursor2.hasNext()) {
                    list2.add(cursor2.next());
                }

                Assert.assertEquals("[20,10]", list1.toString());
                Assert.assertEquals("[25,15]", list2.toString());
            }
        });
    }

    @Test
    public void testSingleValuePerKey() throws Exception {
        // Test with exactly one value per key
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            int numKeys = 100;
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                for (int key = 0; key < numKeys; key++) {
                    writer.add(key, key * 100);
                }

                for (int key = 0; key < numKeys; key++) {
                    assertThat("[" + (key * 100) + "]", writer.getCursor(key), list);
                }
            }

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int key = 0; key < numKeys; key++) {
                    assertThat("[" + (key * 100) + "]", reader.getCursor(true, key, 0, Long.MAX_VALUE), list);
                }
            }
        });
    }

    @Test
    public void testReaderWithNonExistentKey() throws Exception {
        // Test reader behavior when requesting a key that was never created
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(5, 500);
                // Keys 1-4 exist but are empty, keys 6+ don't exist
            }

            try (DeltaBitmapIndexFwdReader fwdReader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
                 DeltaBitmapIndexBwdReader bwdReader = new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                // Empty intermediate keys
                Assert.assertFalse(fwdReader.getCursor(true, 3, 0, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(bwdReader.getCursor(true, 3, 0, Long.MAX_VALUE).hasNext());

                // Keys beyond keyCount should return empty cursor
                Assert.assertFalse(fwdReader.getCursor(true, 100, 0, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(bwdReader.getCursor(true, 100, 0, Long.MAX_VALUE).hasNext());

                // After updateKeyCount, the same key should still return empty
                fwdReader.updateKeyCount();
                bwdReader.updateKeyCount();
                Assert.assertFalse(fwdReader.getCursor(true, 100, 0, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(bwdReader.getCursor(true, 100, 0, Long.MAX_VALUE).hasNext());
            }
        });
    }

    @Test
    public void testRollbackWithValueInBetween() throws Exception {
        // Test rollback to a value that falls between existing values
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(0, 30);
                writer.add(0, 40);

                // Rollback to 25 (between 20 and 30)
                writer.rollbackValues(25);

                assertThat("[20,10]", writer.getCursor(0), list);
                Assert.assertEquals(25, writer.getMaxValue());
            }
        });
    }

    @Test
    public void testMultipleRollbacks() throws Exception {
        // Test multiple consecutive rollbacks
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(0, 30);
                writer.add(0, 40);
                writer.add(0, 50);

                writer.rollbackValues(45);
                assertThat("[40,30,20,10]", writer.getCursor(0), list);

                writer.rollbackValues(35);
                assertThat("[30,20,10]", writer.getCursor(0), list);

                writer.rollbackValues(15);
                assertThat("[10]", writer.getCursor(0), list);

                writer.rollbackValues(5);
                assertThat("[]", writer.getCursor(0), list);
            }
        });
    }

    @Test
    public void testWriterFileNotFound() throws Exception {
        // Test opening writer on non-existent index
        TestUtils.assertMemoryLeak(() -> {
            // Don't call create() - index doesn't exist

            try {
                new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "nonexistent", COLUMN_NAME_TXN_NONE);
                Assert.fail("Expected CairoException");
            } catch (CairoException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "index does not exist"));
            }
        });
    }

    @Test
    public void testReaderReloadConditionally() throws Exception {
        // Test that reloadConditionally properly updates reader state
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
            }

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[100]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);

                // Add more data with a new writer
                try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                    writer.add(0, 200);
                    writer.add(0, 300);
                }

                // Before reload, reader may still see old data
                // After reload, should see new data
                reader.reloadConditionally();
                assertThat("[100,200,300]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testVeryLargeDelta() throws Exception {
        // Test with large delta values that fit within the 56-bit delta limit
        // (decodeDeltaUnsafe packs bytes consumed in upper 8 bits, limiting delta to 2^56-1)
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            // Max delta that fits in 56 bits: 0x00FFFFFFFFFFFFFFL = 72057594037927935
            long maxSafeDelta = DeltaBitmapIndexUtils.MAX_DELTA;

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 0);
                writer.add(0, 1000000000000L);           // 1 trillion (9-byte encoding)
                writer.add(0, 10000000000000L);          // 10 trillion
                writer.add(0, maxSafeDelta);             // Max safe delta value

                assertThat("[" + maxSafeDelta + ",10000000000000,1000000000000,0]",
                        writer.getCursor(0), list);
            }

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[0,1000000000000,10000000000000," + maxSafeDelta + "]",
                        reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testDeltaOverflowThrowsException() throws Exception {
        // Test that adding values with delta > MAX_DELTA throws CairoException
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 0);

                // Try to add a value that would create a delta > MAX_DELTA
                long overflowDelta = DeltaBitmapIndexUtils.MAX_DELTA + 1;
                try {
                    writer.add(0, overflowDelta);
                    Assert.fail("Expected CairoException for delta overflow");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "delta index value exceeds maximum"));
                }
            }
        });
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    public void testNegativeKeyThrowsException() throws Exception {
        // Test that adding with negative key throws CairoException
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                try {
                    writer.add(-1, 100);
                    Assert.fail("Expected CairoException for negative key");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "index key cannot be negative"));
                }
            }
        });
    }

    @Test
    public void testDescendingValuesThrowsException() throws Exception {
        // Test that adding values in descending order throws CairoException
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);

                try {
                    writer.add(0, 150);  // Descending - should fail
                    Assert.fail("Expected CairoException for descending values");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "index values must be added in ascending order"));
                }
            }
        });
    }

    @Test
    public void testValueCountOverflowBehavior() throws Exception {
        // Test behavior when value count approaches Integer.MAX_VALUE
        // The countCheck is stored as int, so it wraps around at Integer.MAX_VALUE
        // This test verifies the consistency check mechanism
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Add some values
                writer.add(0, 0);
                writer.add(0, 1);
                writer.add(0, 2);

                LongList list = new LongList();
                assertThat("[2,1,0]", writer.getCursor(0), list);
            }

            // Manually corrupt the countCheck to simulate overflow scenario
            try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                try (Path keyPath = new Path()) {
                    keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                    FilesFacade ff = configuration.getFilesFacade();
                    mem.of(ff, keyPath.$(), ff.getMapPageSize(), ff.length(keyPath.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                }

                long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(0);
                // Set valueCount to a large value but countCheck to a different value
                mem.putLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, Integer.MAX_VALUE + 1L);
                mem.putInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, 1);  // Mismatched
            }

            // Reader should detect the mismatch and timeout
            final CairoConfiguration shortTimeoutConfig = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getSpinLockTimeout() {
                    return 1;
                }
            };

            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    shortTimeoutConfig, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                try {
                    reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                    Assert.fail("Expected CairoException for inconsistent header");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "cursor could not consistently read index header"));
                }
            }
        });
    }

    @Test
    public void testLargeKeyValue() throws Exception {
        // Test with large key values (but not near Integer.MAX_VALUE due to memory constraints)
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            int largeKey = 100000;  // Large but reasonable for testing

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(largeKey, 100);
                writer.add(largeKey, 200);
                writer.add(0, 50);  // Also add to key 0

                Assert.assertEquals(largeKey + 1, writer.getKeyCount());
                assertThat("[200,100]", writer.getCursor(largeKey), list);
                assertThat("[50]", writer.getCursor(0), list);
            }

            // Verify after reopen
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[100,200]", reader.getCursor(true, largeKey, 0, Long.MAX_VALUE), list);
                assertThat("[50]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                Assert.assertEquals(largeKey + 1, reader.getKeyCount());
            }
        });
    }

    @Test
    public void testPartialWriteRecovery() throws Exception {
        // Test recovery after simulated partial write (inconsistent state)
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            // Write some valid data
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(0, 30);
            }

            // Simulate partial write by corrupting sequence numbers
            try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                try (Path keyPath = new Path()) {
                    keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                    FilesFacade ff = configuration.getFilesFacade();
                    mem.of(ff, keyPath.$(), ff.getMapPageSize(), ff.length(keyPath.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                }

                // Corrupt sequence check (simulate crash during header update)
                long originalSeq = mem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                mem.putLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK, originalSeq + 999);
            }

            // Writer should fail to open due to sequence mismatch
            try {
                new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE);
                Assert.fail("Expected CairoException for sequence mismatch");
            } catch (CairoException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "Sequence mismatch"));
            }
        });
    }

    @Test
    public void testCorruptedKeyEntry() throws Exception {
        // Test behavior when a key entry is corrupted (countCheck mismatch)
        final CairoConfiguration shortTimeoutConfig = new DefaultTestCairoConfiguration(root) {
            @Override
            public long getSpinLockTimeout() {
                return 1;
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            // Write valid data
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
            }

            // Corrupt the key entry's countCheck
            try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                try (Path keyPath = new Path()) {
                    keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                    FilesFacade ff = configuration.getFilesFacade();
                    mem.of(ff, keyPath.$(), ff.getMapPageSize(), ff.length(keyPath.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                }

                long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(0);
                // valueCount is 2, set countCheck to something else
                mem.putInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, 999);
            }

            // Reader should detect the corruption and timeout
            try (DeltaBitmapIndexBwdReader reader = new DeltaBitmapIndexBwdReader(
                    shortTimeoutConfig, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                try {
                    reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                    Assert.fail("Expected CairoException for corrupted key entry");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "cursor could not consistently read index header"));
                }
            }
        });
    }

    @Test
    public void testCorruptedDataLenTooLarge() throws Exception {
        // Test decodeAllValues when dataLen is corrupted to be larger than needed
        // This exercises the second condition in the decode loop (result.size() < valueCount)
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            // Write valid data
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(0, 30);
            }

            // Corrupt the dataLen to be larger than actual data
            try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                try (Path keyPath = new Path()) {
                    keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                    FilesFacade ff = configuration.getFilesFacade();
                    mem.of(ff, keyPath.$(), ff.getMapPageSize(), ff.length(keyPath.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                }

                long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(0);
                int originalDataLen = mem.getInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN);
                // Set dataLen to be much larger than actual (add 100 bytes)
                mem.putInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN, originalDataLen + 100);
            }

            // Open writer and trigger rollbackValues which uses decodeAllValues
            // The decode loop should terminate via valueCount check, not offset check
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // Rollback to value 25 - removes value 30, keeps 10 and 20
                writer.rollbackValues(25);

                // Verify correct values are kept despite corrupted dataLen
                assertThat("[20,10]", writer.getCursor(0), list);
            }
        });
    }

    @Test
    public void testCorruptedDataLenTooSmall() throws Exception {
        // Test reader when dataLen is corrupted to be smaller than needed
        // This exercises L316 in DeltaBitmapIndexFwdReader (readOffset >= dataEndOffset early exit)
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            // Write valid data - 3 values means 8 bytes for first + 2 delta bytes = ~10 bytes
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(0, 20);
                writer.add(0, 30);
            }

            // Corrupt the dataLen to be smaller than actual data (just 8 bytes = only first value)
            try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                try (Path keyPath = new Path()) {
                    keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                    FilesFacade ff = configuration.getFilesFacade();
                    mem.of(ff, keyPath.$(), ff.getMapPageSize(), ff.length(keyPath.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                }

                long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(0);
                // Set dataLen to 8 (only enough for first value, not the deltas)
                // valueCount is still 3, but data is truncated
                mem.putInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN, 8);
            }

            // Open forward reader and try to read - should hit early exit at L316
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                list.clear();
                while (cursor.hasNext()) {
                    list.add(cursor.next());
                }
                // Should only get first value due to truncated dataLen
                Assert.assertEquals(1, list.size());
                Assert.assertEquals(10, list.getQuick(0));
            }
        });
    }

    @Test
    public void testTruncatedKeyFile() throws Exception {
        // Test opening an index with a truncated key file (smaller than KEY_FILE_RESERVED)
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = configuration.getFilesFacade();

            // Create a truncated key file directly (smaller than 64 bytes)
            try (Path keyPath = new Path()) {
                keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");

                // Create file and truncate to exactly 32 bytes (less than KEY_FILE_RESERVED = 64)
                long fd = ff.openRW(keyPath.$(), configuration.getWriterFileOpenOpts());
                Assert.assertTrue(fd > 0);
                ff.truncate(fd, 32);
                ff.close(fd);

                // Also create an empty value file (otherwise the writer might fail for different reason)
                try (Path valuePath = new Path()) {
                    valuePath.of(configuration.getDbRoot()).concat("x").put(".dv");
                    ff.touch(valuePath.$());
                }
            }

            // Try to open the writer - should fail with "Index file too short"
            try {
                new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE);
                Assert.fail("Expected CairoException for truncated key file");
            } catch (CairoException e) {
                Assert.assertTrue("Expected 'Index file too short' but got: " + e.getMessage(),
                        Chars.contains(e.getMessage(), "Index file too short"));
            }
        });
    }

    @Test
    public void testKeyCountMismatchFileLength() throws Exception {
        // Test opening an index where keyCount in header claims more keys than file has space for
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            // Corrupt the keyCount to claim more keys than file actually has
            try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                try (Path keyPath = new Path()) {
                    keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                    FilesFacade ff = configuration.getFilesFacade();
                    mem.of(ff, keyPath.$(), ff.getMapPageSize(), ff.length(keyPath.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                }

                // Set keyCount to a large value (e.g., 1000) but file only has header (64 bytes)
                // Expected file size for 1000 keys = 64 + 1000 * 32 = 32064 bytes
                mem.putInt(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT, 1000);
            }

            // Try to open the writer - should fail with "Key count does not match file length"
            try {
                new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE);
                Assert.fail("Expected CairoException for key count mismatch");
            } catch (CairoException e) {
                Assert.assertTrue("Expected 'Key count does not match file length' but got: " + e.getMessage(),
                        Chars.contains(e.getMessage(), "Key count does not match file length"));
            }
        });
    }

    @Test
    public void testRebuildCachesWithEmptyKeys() throws Exception {
        // Test rebuildCaches when there are empty key slots (valueCount == 0)
        // This exercises L602 condition 1 being false
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            // Create index with sparse keys
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(5, 100);  // Creates empty slots for keys 0-4
                writer.add(5, 200);
            }

            // Reopen writer - rebuildCaches() will process keys 0-4 with valueCount == 0
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                Assert.assertEquals(6, writer.getKeyCount());

                // Empty keys should return empty cursors
                assertThat("[]", writer.getCursor(0), list);
                assertThat("[]", writer.getCursor(1), list);
                assertThat("[]", writer.getCursor(4), list);

                // Key 5 should have values
                assertThat("[200,100]", writer.getCursor(5), list);

                // Can still add to empty keys
                writer.add(2, 50);
                assertThat("[50]", writer.getCursor(2), list);
            }
        });
    }

    @Test
    public void testRebuildCachesWithCorruptedCountCheck() throws Exception {
        // Test rebuildCaches when countCheck doesn't match valueCount
        // This exercises L602 condition 2 being false - entry is treated as invalid
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
            }

            // Corrupt the countCheck for key 0
            try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                try (Path keyPath = new Path()) {
                    keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                    FilesFacade ff = configuration.getFilesFacade();
                    mem.of(ff, keyPath.$(), ff.getMapPageSize(), ff.length(keyPath.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                }

                long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(0);
                // Set countCheck to wrong value (valueCount is 2, set countCheck to 999)
                mem.putInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, 999);
            }

            // Reopen writer - rebuildCaches() should treat key 0 as invalid (empty)
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // Key 0 should appear empty due to countCheck mismatch
                assertThat("[]", writer.getCursor(0), list);
            }
        });
    }

    @Test
    public void testRebuildCachesWithNegativeDataOffset() throws Exception {
        // Test rebuildCaches when dataOffset is negative (corrupted)
        // This exercises L602 condition 3 being false - entry is treated as invalid
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
            }

            // Corrupt the dataOffset for key 0 to be negative
            try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                try (Path keyPath = new Path()) {
                    keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                    FilesFacade ff = configuration.getFilesFacade();
                    mem.of(ff, keyPath.$(), ff.getMapPageSize(), ff.length(keyPath.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                }

                long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(0);
                // Set dataOffset to negative value
                mem.putLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_OFFSET, -100);
            }

            // Reopen writer - rebuildCaches() should treat key 0 as invalid (empty)
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // Key 0 should appear empty due to negative dataOffset
                assertThat("[]", writer.getCursor(0), list);
            }
        });
    }

    @Test
    public void testWriterCreateMode() throws Exception {
        // Test opening writer with create=true to create a new index from scratch
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();

            // Use writer with create=true (no pre-existing index)
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration)) {
                writer.of(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, true);

                // Add some data
                writer.add(0, 100);
                writer.add(0, 200);
                writer.add(1, 150);
            }

            // Verify data can be read back
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[100,200]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[150]", reader.getCursor(true, 1, 0, Long.MAX_VALUE), list);
            }

            // Reopen with create=true should reset the index
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration)) {
                writer.of(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, true);

                // Add different data
                writer.add(0, 500);
            }

            // Verify only new data exists
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[500]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                // Key 1 should no longer exist (empty cursor)
                assertThat("[]", reader.getCursor(true, 1, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testReaderAfterWriterTruncate() throws Exception {
        // Test that reader handles writer truncation correctly
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            // Write initial data
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
                writer.add(1, 150);
            }

            // Open reader
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                assertThat("[100,200]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);

                // Truncate with new writer
                try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                    writer.truncate();
                    writer.add(0, 500);
                }

                // Reader should see old data until reload
                reader.reloadConditionally();

                // After reload, should see new data
                assertThat("[500]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testInvalidDataOffset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            // Write valid data
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
            }

            // Corrupt dataOffset to point beyond file
            try (io.questdb.cairo.vm.api.MemoryMARW mem = Vm.getCMARWInstance()) {
                try (Path keyPath = new Path()) {
                    keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                    FilesFacade ff = configuration.getFilesFacade();
                    mem.of(ff, keyPath.$(), ff.getMapPageSize(), ff.length(keyPath.$()), MemoryTag.MMAP_DEFAULT, io.questdb.cairo.CairoConfiguration.O_NONE, -1);
                }

                long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(0);
                // Set dataOffset to an invalid large value
                mem.putLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_OFFSET, 999999999L);
            }

            // Writer should detect invalid state on reopen (cache rebuild fails validation)
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // The cache rebuild should skip this key due to invalid dataOffset >= valueMemSize
                LongList list = new LongList();
                assertThat("[]", writer.getCursor(0), list);  // No values due to invalid offset
            }
        });
    }

    @Test
    public void testKeyZeroSpecialHandling() throws Exception {
        // Test that key 0 (often used for NULL in symbol tables) works correctly
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Add many values to key 0
                for (int i = 0; i < 1000; i++) {
                    writer.add(0, i);
                }

                // Also add to other keys
                writer.add(1, 500);
                writer.add(2, 600);

                Assert.assertEquals(3, writer.getKeyCount());

                // Verify key 0 has all values
                RowCursor cursor = writer.getCursor(0);
                int count = 0;
                while (cursor.hasNext()) {
                    cursor.next();
                    count++;
                }
                Assert.assertEquals(1000, count);
            }

            // Verify with reader
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                int count = 0;
                long lastValue = -1;
                while (cursor.hasNext()) {
                    long value = cursor.next();
                    Assert.assertTrue("Values should be ascending", value > lastValue);
                    lastValue = value;
                    count++;
                }
                Assert.assertEquals(1000, count);
            }
        });
    }

    @Test
    public void testConcurrentWriterReaderWithRollback() throws Exception {
        // Test concurrent read while writer does rollback
        final int totalWrites = 5000;
        final AtomicReference<Throwable> writerError = new AtomicReference<>();
        final AtomicReference<Throwable> readerError = new AtomicReference<>();
        final AtomicBoolean writerDone = new AtomicBoolean(false);
        final CountDownLatch readerReady = new CountDownLatch(1);

        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // Initial writes
                for (int i = 0; i < 100; i++) {
                    writer.add(0, i);
                }
                writer.setMaxValue(99);

                Thread writerThread = new Thread(() -> {
                    try {
                        readerReady.await();
                        for (int i = 100; i < totalWrites; i++) {
                            writer.add(0, i);
                            writer.setMaxValue(i);

                            // Occasionally rollback
                            if (i % 500 == 0) {
                                writer.rollbackValues(i - 100);
                                // Re-add values after rollback
                                for (int j = i - 99; j <= i; j++) {
                                    writer.add(0, j);
                                }
                                writer.setMaxValue(i);
                            }
                        }
                    } catch (Throwable t) {
                        writerError.set(t);
                    } finally {
                        writerDone.set(true);
                    }
                });

                Thread readerThread = new Thread(() -> {
                    try (Path readerPath = new Path().of(configuration.getDbRoot())) {
                        try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                                configuration, readerPath, "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                            readerReady.countDown();
                            while (!writerDone.get()) {
                                reader.reloadConditionally();
                                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);

                                long lastValue = -1;
                                while (cursor.hasNext()) {
                                    long value = cursor.next();
                                    // Values should be ascending
                                    if (value <= lastValue) {
                                        throw new AssertionError("Values not ascending: " + lastValue + " -> " + value);
                                    }
                                    lastValue = value;
                                }
                                Thread.yield();
                            }
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
    public void testEmptyKeyAfterRollback() throws Exception {
        // Test that keys with no values after rollback are handled correctly
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                // Add values where all of key 1's values are above the rollback point
                writer.add(0, 10);
                writer.add(1, 100);
                writer.add(1, 200);
                writer.add(2, 20);

                // Rollback to 50 - key 1 should become empty
                writer.rollbackValues(50);

                assertThat("[10]", writer.getCursor(0), list);
                assertThat("[]", writer.getCursor(1), list);
                assertThat("[20]", writer.getCursor(2), list);

                // Now add new values to the emptied key
                writer.add(1, 60);
                writer.add(1, 70);

                assertThat("[70,60]", writer.getCursor(1), list);
            }

            // Verify after reopen
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[10]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
                assertThat("[60,70]", reader.getCursor(true, 1, 0, Long.MAX_VALUE), list);
                assertThat("[20]", reader.getCursor(true, 2, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testCloseWithPartialInitialization() throws Exception {
        // Test that close() handles partial initialization correctly
        // This exercises the keyCount > -1 and valueMemSize > -1 checks in close()
        TestUtils.assertMemoryLeak(() -> {
            // Create a key file with valid size but invalid signature
            // This will cause of() to fail after opening keyMem but before setting keyCount
            try (Path keyPath = new Path()) {
                keyPath.of(configuration.getDbRoot()).concat("x").put(".dk");
                FilesFacade ff = configuration.getFilesFacade();

                // Create file with enough size but wrong signature
                try (MemoryMA mem = Vm.getSmallCMARWInstance(
                        ff,
                        keyPath.$(),
                        MemoryTag.MMAP_DEFAULT,
                        configuration.getWriterFileOpenOpts()
                )) {
                    // Write invalid signature but valid file size
                    mem.putByte((byte) 0x00);  // Wrong signature (should be 0xfc)
                    mem.skip(DeltaBitmapIndexUtils.KEY_FILE_RESERVED - 1);  // Pad to valid size
                }

                // Also create empty value file
                ff.touch(DeltaBitmapIndexUtils.valueFileName(
                        keyPath.of(configuration.getDbRoot()), "x", COLUMN_NAME_TXN_NONE));
            }

            // Now try to open - this should fail at signature check
            // but keyMem will be opened before the failure
            DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration);
            try {
                writer.of(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE);
                Assert.fail("Expected CairoException for invalid signature");
            } catch (CairoException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "Unknown format"));
            }

            // Writer should be in clean state after failed of()
            Assert.assertFalse(writer.isOpen());

            // Close should be safe to call (no-op since already closed by failed of())
            writer.close();
        });
    }

    @Test
    public void testCommitWithSyncMode() throws Exception {
        // Test commit() with SYNC mode
        final CairoConfiguration syncConfig = new DefaultTestCairoConfiguration(root) {
            @Override
            public int getCommitMode() {
                return CommitMode.SYNC;
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            create(syncConfig, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(syncConfig, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
                writer.commit();  // Should sync synchronously

                writer.add(0, 300);
                writer.commit();

                LongList list = new LongList();
                assertThat("[300,200,100]", writer.getCursor(0), list);
            }

            // Verify data persisted
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    syncConfig, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                LongList list = new LongList();
                assertThat("[100,200,300]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testCommitWithAsyncMode() throws Exception {
        // Test commit() with ASYNC mode
        final CairoConfiguration asyncConfig = new DefaultTestCairoConfiguration(root) {
            @Override
            public int getCommitMode() {
                return CommitMode.ASYNC;
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            create(asyncConfig, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(asyncConfig, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
                writer.commit();  // Should sync asynchronously

                writer.add(0, 300);
                writer.commit();

                LongList list = new LongList();
                assertThat("[300,200,100]", writer.getCursor(0), list);
            }

            // Verify data persisted
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    asyncConfig, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                LongList list = new LongList();
                assertThat("[100,200,300]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testCommitWithNoSyncMode() throws Exception {
        // Test commit() with NOSYNC mode (no-op)
        final CairoConfiguration noSyncConfig = new DefaultTestCairoConfiguration(root);

        TestUtils.assertMemoryLeak(() -> {
            create(noSyncConfig, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(noSyncConfig, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
                writer.commit();  // Should be a no-op

                writer.add(0, 300);
                writer.commit();

                LongList list = new LongList();
                assertThat("[300,200,100]", writer.getCursor(0), list);
            }

            // Verify data persisted (still written to mmap, just not synced)
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    noSyncConfig, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                LongList list = new LongList();
                assertThat("[100,200,300]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testCommitAfterRollback() throws Exception {
        // Test that commit works correctly after rollback
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 100);
                writer.add(0, 200);
                writer.add(0, 300);
                writer.commit();

                // Rollback
                writer.rollbackValues(150);
                writer.commit();

                LongList list = new LongList();
                assertThat("[100]", writer.getCursor(0), list);
            }

            // Verify after reopen
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                LongList list = new LongList();
                assertThat("[100]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testCommitWithMultipleKeys() throws Exception {
        // Test commit with data spread across multiple keys
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x");

            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // Add data to multiple keys
                for (int key = 0; key < 10; key++) {
                    for (int i = 0; i < 100; i++) {
                        writer.add(key, key * 1000 + i);
                    }
                }
                writer.commit();

                Assert.assertEquals(10, writer.getKeyCount());
            }

            // Verify all data persisted
            try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(
                    configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(10, reader.getKeyCount());

                for (int key = 0; key < 10; key++) {
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    int count = 0;
                    while (cursor.hasNext()) {
                        cursor.next();
                        count++;
                    }
                    Assert.assertEquals(100, count);
                }
            }
        });
    }

    private void assertBackwardReaderConstructorFail(CairoConfiguration configuration) {
        try {
            new DeltaBitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
            Assert.fail();
        } catch (CairoException e) {
            Assert.assertTrue(Chars.contains(e.getMessage(), "Unknown format"));
        }
    }

    private void assertEmptyCursor(BitmapIndexReader reader) {
        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
        Assert.assertFalse(cursor.hasNext());
    }

    private void assertForwardReaderConstructorFail(CairoConfiguration configuration) {
        try {
            new DeltaBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
            Assert.fail();
        } catch (CairoException e) {
            Assert.assertTrue(Chars.contains(e.getMessage(), "Unknown format"));
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
