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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.AbstractBitmapIndexReader;
import io.questdb.cairo.idx.BitmapIndexBwdReader;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.idx.ConcurrentBitmapIndexFwdReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.griffin.engine.table.LatestByArguments;
import io.questdb.std.BitmapIndexUtilsNative;
import io.questdb.std.Chars;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static org.junit.Assert.assertTrue;

public class BitmapIndexTest extends AbstractCairoTest {
    private Path path;
    private int plen;

    public static void create(CairoConfiguration configuration, Path path, CharSequence name, int valueBlockCapacity) {
        int plen = path.size();
        try {
            final FilesFacade ff = configuration.getFilesFacade();
            try (
                    MemoryMA mem = Vm.getSmallCMARWInstance(
                            ff,
                            BitmapIndexUtils.keyFileName(path, name, COLUMN_NAME_TXN_NONE),
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                BitmapIndexWriter.initKeyMemory(mem, Numbers.ceilPow2(valueBlockCapacity));
            }
            ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
        } finally {
            path.trimTo(plen);
        }
    }

    /**
     * Reads the header of an index key file through {@code keyFd}, as a process that opens the file after a kill
     * finds it.
     */
    public static String readKeyFileHeader(FilesFacade ff, long keyFd) {
        final long buf = Unsafe.malloc(BitmapIndexUtils.KEY_FILE_RESERVED, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(BitmapIndexUtils.KEY_FILE_RESERVED, ff.read(keyFd, buf, BitmapIndexUtils.KEY_FILE_RESERVED, 0));
            return "signature=" + (Unsafe.getByte(buf + BitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) & 0xff)
                    + ", sequence=" + Unsafe.getLong(buf + BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)
                    + ", sequenceCheck=" + Unsafe.getLong(buf + BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK)
                    + ", blockValueCount=" + Unsafe.getInt(buf + BitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT)
                    + ", keyCount=" + Unsafe.getInt(buf + BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT)
                    + ", valueMemSize=" + Unsafe.getLong(buf + BitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE)
                    + ", maxValue=" + Unsafe.getLong(buf + BitmapIndexUtils.KEY_RESERVED_OFFSET_MAX_VALUE);
        } finally {
            Unsafe.free(buf, BitmapIndexUtils.KEY_FILE_RESERVED, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertAllZero(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            Assert.assertEquals("byte " + i, 0, bytes[i]);
        }
    }

    // A key whose entry lies past the end of a key file of keyFileLength bytes
    private static int farKey(long keyFileLength) {
        return (int) ((keyFileLength - BitmapIndexUtils.KEY_FILE_RESERVED) / BitmapIndexUtils.KEY_ENTRY_SIZE) + 100;
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
            create(configuration, path.trimTo(plen), "x", 4);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 1000);
                writer.add(256, 1234);
                writer.add(64, 10);
                writer.add(64, 987);
                writer.add(256, 5567);
                writer.add(64, 91);
                writer.add(64, 92);
                writer.add(64, 93);

                assertThat("[5567,1234]", writer.getCursor(256), list);
                assertThat("[93,92,91,987,10]", writer.getCursor(64), list);
                assertThat("[1000]", writer.getCursor(0), list);
                assertThat("[]", writer.getCursor(1000), list);
            }

            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[5567,1234]", reader.getCursor(256, 0, Long.MAX_VALUE), list);
                assertThat("[93,92,91,987,10]", reader.getCursor(64, 0, Long.MAX_VALUE), list);
                assertThat("[1000]", reader.getCursor(0, 0, Long.MAX_VALUE), list);
            }

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[1234,5567]", reader.getCursor(256, 0, Long.MAX_VALUE), list);
                assertThat("[10,987,91,92,93]", reader.getCursor(64, 0, Long.MAX_VALUE), list);
                assertThat("[1000]", reader.getCursor(0, 0, Long.MAX_VALUE), list);
            }
        });
    }

    @Test
    public void testCursorNotRepooledWhenClosedOffOperatingThread() throws Exception {
        // Genuine regression test for the M1 operating-thread re-pool gate on the
        // default (bitmap) index reader: pooling reuses the same cursor instance, so
        // whether an off-thread close re-pools is observable through object identity.
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 4);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(256, 1234);
                writer.add(256, 5567);
            }
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                // Positive control: an on-thread close re-pools the cursor, so the
                // next getCursor() on the operating thread hands back the same one.
                final RowCursor c1 = reader.getCursor(256, 0, Long.MAX_VALUE);
                c1.close();
                final RowCursor c2 = reader.getCursor(256, 0, Long.MAX_VALUE);
                Assert.assertSame("on-thread close should re-pool the cursor", c1, c2);

                // Close c2 off the operating thread: the gate must skip re-pooling
                // (getCursor() stamped the reader to this thread), so the next
                // getCursor() allocates a fresh instance instead of handing back a
                // cursor still being torn down elsewhere. Without the gate the
                // off-thread close would re-pool c2 and getCursor() would return it.
                closeOffThread(c2);
                final RowCursor c3 = reader.getCursor(256, 0, Long.MAX_VALUE);
                Assert.assertNotSame("off-thread close must not re-pool the cursor", c2, c3);
                c3.close();
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

            // we need to have a conventional structure, which will unfortunately be memory-inefficient, to
            // assert that the index is populated correctly
            create(configuration, path.trimTo(plen), "x", 128);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < N; i++) {
                    int key = i % maxKeys;
                    long value = rnd.nextPositiveLong();
                    writer.add(key, value);

                    LongList list = lists.get(key);
                    if (list == null) {
                        lists.put(key, list = new LongList());
                        keys.add(key);
                    }
                    list.add(value);
                }
            }

            // read values and compare to the structure index is expected to be holding
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    LongList list = lists.get(keys.getQuick(i));
                    Assert.assertNotNull(list);
                    RowCursor cursor = reader.getCursor(keys.getQuick(i), 0, Long.MAX_VALUE);
                    int z = list.size();
                    while (cursor.hasNext()) {
                        assertTrue(z > -1);
                        Assert.assertEquals(list.getQuick(z - 1), cursor.next());
                        z--;
                    }

                    // makes sure the entire list is processed
                    Assert.assertEquals(0, z);
                    Misc.free(cursor);
                }
            }

            // read values and compare to the structure index is expected to be holding
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    LongList list = lists.get(keys.getQuick(i));
                    Assert.assertNotNull(list);
                    RowCursor cursor = reader.getCursor(keys.getQuick(i), 0, Long.MAX_VALUE);
                    int z = 0;
                    int sz = list.size();
                    while (cursor.hasNext()) {
                        assertTrue(z < sz);
                        Assert.assertEquals(list.getQuick(z), cursor.next());
                        z++;
                    }

                    // makes sure the entire list is processed
                    Assert.assertEquals(sz, z);
                    Misc.free(cursor);
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
            create(configuration, path.trimTo(plen), "x", 1024);

            try (BitmapIndexWriter w = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                w.add(0, 10);
            }

            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    try (Path path = new Path()) {
                        path.of(configuration.getDbRoot()).concat("x").put(".k");
                        LPSZ name = path.$();
                        FilesFacade ff = configuration.getFilesFacade();
                        mem.of(ff, name, ff.getMapPageSize(), ff.length(name), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                    }
                    mem.putLong(BitmapIndexUtils.getKeyEntryOffset(0) + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, 10);

                    try {
                        reader.getCursor(0, 0, Long.MAX_VALUE);
                        Assert.fail();
                    } catch (CairoException e) {
                        assertTrue(Chars.contains(e.getMessage(), "cursor could not consistently read index header"));
                    }
                }
            }
        });
    }

    @Test
    public void testBackwardReaderConstructorBadSequence() throws Exception {
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public long getSpinLockTimeout() {
                return 1;
            }
        };
        TestUtils.assertMemoryLeak(() -> {
            setupIndexHeader();
            assertBackwardReaderConstructorFail(configuration, "could not consistently");
            assertForwardReaderConstructorFail(configuration, "could not consistently");
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
            try (MemoryA mem = openKey()) {
                mem.skip(BitmapIndexUtils.KEY_FILE_RESERVED);
            }
            assertBackwardReaderConstructorFail(configuration, "Unknown format");
            assertForwardReaderConstructorFail(configuration, "Unknown format");
        });
    }

    @Test
    public void testBackwardReaderDoesNotUnmapPages() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            create(configuration, path.trimTo(plen), "x", 1024);

            class CountingFacade extends FilesFacadeImpl {
                private int count = 0;

                @Override
                public long getMapPageSize() {
                    return 1024 * 1024;
                }

                @Override
                public void munmap(long address, long size, int memoryTag) {
                    super.munmap(address, size, memoryTag);
                    count++;
                }
            }

            final CountingFacade facade = new CountingFacade();

            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return facade;
                }
            };


            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    for (int i = 0; i < 100000; i++) {
                        int key = rnd.nextPositiveInt() % 1024;
                        long value = rnd.nextPositiveLong();
                        writer.add(key, value);

                        RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                        boolean found = false;
                        while (cursor.hasNext()) {
                            if (value == cursor.next()) {
                                found = true;
                                break;
                            }
                        }
                        assertTrue(found);
                        Misc.free(cursor);
                    }
                    Assert.assertEquals(0, facade.count);
                }
            }
        });
    }

    @Test
    public void testBackwardReaderKeyUpdateFail() {
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public long getSpinLockTimeout() {
                return 4;
            }
        };
        create(configuration, path.trimTo(plen), "x", 1024);

        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
            writer.add(0, 1000);
        }

        try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

            // should have single value in cursor
            RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
            assertTrue(cursor.hasNext());
            Assert.assertEquals(1000, cursor.next());
            Assert.assertFalse(cursor.hasNext());

            try (
                    Path path = new Path();
                    MemoryCMARW mem = Vm.getSmallCMARWInstance(
                            configuration.getFilesFacade(),
                            path.of(root).concat("x").put(".k").$(),
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                // change sequence but not sequence check
                long seq = mem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                mem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, 22);

                try {
                    reader.getCursor(10, 0, Long.MAX_VALUE);
                } catch (CairoException e) {
                    assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                // make sure the index fails until the sequence is not up to date

                try {
                    reader.getCursor(0, 0, Long.MAX_VALUE);
                } catch (CairoException e) {
                    assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                mem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);

                // test that index recovers
                Misc.free(cursor);
                cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
                assertTrue(cursor.hasNext());
                Assert.assertEquals(1000, cursor.next());
                Assert.assertFalse(cursor.hasNext());
            }
            Misc.free(cursor);
        }
    }

    @Test
    public void testBeyondPageSize() throws Exception {

        // value count for a single pass, there are two passes
        final int N = 2_000_000;
        final int K = 1000;

        TestUtils.assertMemoryLeak(() -> {
            // values array holds
            IntList values = new IntList(N + N);
            values.setPos(N + N);

            final Rnd rnd = new Rnd();
            create(configuration, path.trimTo(plen), "x", 1024);

            long expectedSum = 0;

            // current random value distribution ensures we have all K keys
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < N; i++) {
                        final int k = rnd.nextInt(K);
                        writer.add(k, i);
                        expectedSum += i;
                    }
                }

                // reopen the indexer and add more values
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < N; i++) {
                        final int k = rnd.nextInt(K);
                        writer.add(k, i + N);
                        expectedSum += i + N;
                    }
                }

                // current random value distribution ensures we have all K keys
                reader.updateKeyCount();
                Assert.assertEquals(K, reader.getKeyCount());

                // compute the sum of all values
                long sum = 0;
                for (int i = 0; i < K; i++) {
                    try (RowCursor cursor = reader.getCursor(i, Long.MIN_VALUE, Long.MAX_VALUE)) {
                        while (cursor.hasNext()) {
                            sum += cursor.next();
                        }
                    }
                }

                Assert.assertEquals(expectedSum, sum);
            }

            // test branch new reader
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                long sum = 0;
                for (int i = 0; i < K; i++) {
                    try (RowCursor cursor = reader.getCursor(i, Long.MIN_VALUE, Long.MAX_VALUE)) {
                        while (cursor.hasNext()) {
                            sum += cursor.next();
                        }
                    }
                }
                Assert.assertEquals(expectedSum, sum);
            }
        });
    }

    @Test
    public void testConcurrentForwardCursorReadBreadth() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testConcurrentForwardCursor(rnd.nextInt(1000000), 1024);
    }

    @Test
    public void testConcurrentForwardCursorReadHeight() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testConcurrentForwardCursor(rnd.nextInt(1000000), 10000);
    }

    @Test
    public void testConcurrentForwardReaderRepoolsCursorOfMissingKey() throws Exception {
        // getCursor() pops a pooled cursor before it knows whether the key has rows. For a key past
        // the key count it returns the shared empty cursor, whose close() pools nothing, so
        // getCursor() itself must return the popped cursor to the pool. Pooling reuses the same
        // instance, so a lost cursor shows as a fresh instance on the next lookup.
        TestUtils.assertMemoryLeak(() -> {
            final LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x", 4);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 10);
                writer.add(1, 20);
            }
            try (ConcurrentBitmapIndexFwdReader reader = new ConcurrentBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                final RowCursor pooled = reader.getCursor(1, 0, Long.MAX_VALUE);
                assertThat("[20]", pooled, list);
                for (int i = 0; i < 3; i++) {
                    final RowCursor empty = reader.getCursor(1_000, 0, Long.MAX_VALUE);
                    Assert.assertSame(EmptyRowCursor.INSTANCE, empty);
                    empty.close();
                }
                final RowCursor reused = reader.getCursor(0, 0, Long.MAX_VALUE);
                Assert.assertSame("a lookup of a missing key must return the pooled cursor to the pool", pooled, reused);
                assertThat("[10]", reused, list);
            }
        });
    }

    @Test
    public void testConcurrentWriterAndBackwardReadBreadth() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testConcurrentBackwardRW(rnd.nextInt(10000000), 1024);
    }

    @Test
    public void testConcurrentWriterAndBackwardReadHeight() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        // randomize row count with a floor for meaningful coverage; smaller range on slow CI runners (Mac, Windows)
        int rowCount = Os.isLinux() ? 100_000 + rnd.nextInt(900_000) : 10_000 + rnd.nextInt(90_000);
        testConcurrentBackwardRW(rowCount, 100000);
    }

    @Test
    public void testConcurrentWriterAndForwardReadBreadth() throws Exception {
        testConcurrentForwardRW(1000000, 1024);
    }

    @Test
    public void testConcurrentWriterAndForwardReadHeight() throws Exception {
        testConcurrentForwardRW(100000, 10000);
    }

    @Test
    public void testConsistentReadsMakeNoClockCalls() throws Exception {
        // Each seqlock read loop arms its spin deadline only after a failed attempt, so opening
        // the readers and reading consistent entries must not touch the clock. A torn entry must
        // still time out: the clock advances 1ms per call, so the read gives up after a few calls.
        final AtomicLong clockTicks = new AtomicLong();
        final CairoConfiguration countingConfiguration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull MillisecondClock getMillisecondClock() {
                return clockTicks::incrementAndGet;
            }

            @Override
            public long getSpinLockTimeout() {
                return 1;
            }
        };
        TestUtils.assertMemoryLeak(() -> {
            final LongList tmp = new LongList();
            create(configuration, path.trimTo(plen), "x", 4);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(0, 1);
                writer.add(1, 2);
                writer.add(0, 3);
            }

            try (
                    BitmapIndexFwdReader fwdReader = new BitmapIndexFwdReader(countingConfiguration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
                    BitmapIndexBwdReader bwdReader = new BitmapIndexBwdReader(countingConfiguration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
                    ConcurrentBitmapIndexFwdReader concurrentReader = new ConcurrentBitmapIndexFwdReader(countingConfiguration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
                    MemoryMARW keyMem = Vm.getCMARWInstance();
                    Path keyPath = new Path()
            ) {
                assertThat("[1,3]", fwdReader.getCursor(0, 0, Long.MAX_VALUE), tmp);
                assertThat("[3,1]", bwdReader.getCursor(0, 0, Long.MAX_VALUE), tmp);
                assertThat("[1,3]", concurrentReader.getCursor(0, 0, Long.MAX_VALUE), tmp);
                assertThat("[2]", concurrentReader.initCursor(null, 1, 0, Long.MAX_VALUE), tmp);
                Assert.assertEquals(2, fwdReader.countMatchesInRange(0, 0, Long.MAX_VALUE, BitmapIndexUtils.UNBOUNDED_BLOCK_HOPS));
                // a key past the key count makes the forward reader re-read the key count
                assertThat("[]", fwdReader.getCursor(2, 0, Long.MAX_VALUE), tmp);
                Assert.assertEquals(0, clockTicks.get());

                keyPath.of(configuration.getDbRoot()).concat("x").put(".k");
                keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);

                // tear key 0's entry: its value count no longer matches its count check
                final long keyEntryOffset = BitmapIndexUtils.getKeyEntryOffset(0);
                final long valueCount = keyMem.getLong(keyEntryOffset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);
                keyMem.putLong(keyEntryOffset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, 10);
                assertSpinTimeout(() -> fwdReader.getCursor(0, 0, Long.MAX_VALUE));
                assertSpinTimeout(() -> fwdReader.countMatchesInRange(0, 0, Long.MAX_VALUE, BitmapIndexUtils.UNBOUNDED_BLOCK_HOPS));
                assertSpinTimeout(() -> bwdReader.getCursor(0, 0, Long.MAX_VALUE));
                assertSpinTimeout(() -> concurrentReader.getCursor(0, 0, Long.MAX_VALUE));
                keyMem.putLong(keyEntryOffset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, valueCount);

                // tear the header: its sequence no longer matches its sequence check
                final long seq = keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                keyMem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq + 1);
                assertSpinTimeout(() -> fwdReader.getCursor(2, 0, Long.MAX_VALUE));
                keyMem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);

                // the readers recover once the entries are consistent again
                final long ticks = clockTicks.get();
                assertThat("[1,3]", fwdReader.getCursor(0, 0, Long.MAX_VALUE), tmp);
                assertThat("[3,1]", bwdReader.getCursor(0, 0, Long.MAX_VALUE), tmp);
                assertThat("[1,3]", concurrentReader.getCursor(0, 0, Long.MAX_VALUE), tmp);
                Assert.assertEquals(ticks, clockTicks.get());
            }
        });
    }

    @Test
    public void testCppLatestByIndexReader() {
        final int valueBlockCapacity = 256;
        final long keyCount = 5;
        create(configuration, path.trimTo(plen), "x", valueBlockCapacity);

        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
            for (int i = 0; i < keyCount; i++) {
                for (int j = 0; j <= i; j++) {
                    writer.add(i, j);
                }
            }
        }

        try (DirectLongList rows = new DirectLongList(keyCount, MemoryTag.NATIVE_LONG_LIST)) {
            rows.setCapacity(keyCount);
            rows.setPos(rows.getCapacity());
            GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

            long argsAddress = LatestByArguments.allocateMemory();
            LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
            LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());

            LatestByArguments.setKeyLo(argsAddress, 0);
            LatestByArguments.setKeyHi(argsAddress, keyCount);
            LatestByArguments.setRowsSize(argsAddress, 0);

            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                BitmapIndexUtilsNative.latestScanBackward(
                        reader.getKeyBaseAddress(),
                        reader.getKeyMemorySize(),
                        reader.getValueBaseAddress(),
                        reader.getValueMemorySize(),
                        argsAddress,
                        reader.getColumnTop(),
                        Long.MAX_VALUE,
                        0,
                        0,
                        valueBlockCapacity - 1
                );
            }

            long rowCount = LatestByArguments.getRowsSize(argsAddress);
            Assert.assertEquals(keyCount, rowCount);
            long keyLo = LatestByArguments.getKeyLo(argsAddress);
            Assert.assertEquals(0, keyLo); // we do not update key range anymore
            long keyHi = LatestByArguments.getKeyHi(argsAddress);
            Assert.assertEquals(keyCount, keyHi);

            LatestByArguments.releaseMemory(argsAddress);

            for (int i = 0; i < rows.getCapacity(); ++i) {
                Assert.assertEquals(i, Rows.toLocalRowID(rows.get(i) - 1));
            }
        }
    }

    @Test
    public void testCppLatestByIndexReaderIgnoreSparseKeysUpdates() {
        final int valueBlockCapacity = 32;
        final int keyCount = 42;

        create(configuration, path.trimTo(plen), "x", valueBlockCapacity);

        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
            for (int i = 0; i < keyCount; i++) {
                // add even keys
                if (i % 2 == 0) {
                    writer.add(i, i);
                }
            }
        }

        try (DirectLongList rows = new DirectLongList(keyCount, MemoryTag.NATIVE_LONG_LIST)) {

            rows.setCapacity(keyCount);
            rows.setPos(rows.getCapacity());
            GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

            long argsAddress = LatestByArguments.allocateMemory();
            LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
            LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());

            LatestByArguments.setKeyLo(argsAddress, 0);
            LatestByArguments.setKeyHi(argsAddress, keyCount);
            LatestByArguments.setRowsSize(argsAddress, 0);

            //fixing memory mapping here
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                // we should ignore this update
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < keyCount; i++) {
                        // add odd keys
                        if (i % 2 != 0) {
                            writer.add(i, i);
                        }
                    }
                }

                BitmapIndexUtilsNative.latestScanBackward(
                        reader.getKeyBaseAddress(),
                        reader.getKeyMemorySize(),
                        reader.getValueBaseAddress(),
                        reader.getValueMemorySize(),
                        argsAddress,
                        reader.getColumnTop(),
                        Long.MAX_VALUE,
                        0,
                        0,
                        valueBlockCapacity - 1
                );
            }

            long rowCount = LatestByArguments.getRowsSize(argsAddress);
            Assert.assertEquals(keyCount / 2, rowCount);
            long keyLo = LatestByArguments.getKeyLo(argsAddress);
            Assert.assertEquals(0, keyLo);
            long keyHi = LatestByArguments.getKeyHi(argsAddress);
            Assert.assertEquals(keyCount, keyHi);

            LatestByArguments.releaseMemory(argsAddress);
            // sort and check found keys 0, 2, 4, ....
            Vect.sortULongAscInPlace(rows.getAddress(), rowCount);
            for (long i = 0; i < rowCount; ++i) {
                Assert.assertEquals(2 * i, Rows.toLocalRowID(rows.get(i) - 1));
            }
            // sort and check not found keys 1, 3, 5, ...
            Vect.sortULongAscInPlace(rows.getAddress() + rowCount * Long.BYTES, keyCount - rowCount);
            for (long i = 0; i < keyCount - rowCount; ++i) {
                Assert.assertEquals(2 * i + 1, rows.get(i));
            }
        }
    }

    @Test
    public void testCppLatestByIndexReaderIgnoreUpdates() {
        final int valueBlockCapacity = 32;
        final long keyCount = 1024;

        create(configuration, path.trimTo(plen), "x", valueBlockCapacity);

        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
            for (int i = 0; i < keyCount; i++) {
                for (int j = 0; j <= i; j++) {
                    writer.add(i, j);
                }
            }
        }

        try (DirectLongList rows = new DirectLongList(keyCount, MemoryTag.NATIVE_LONG_LIST)) {

            rows.setCapacity(keyCount);
            rows.setPos(rows.getCapacity());
            GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

            long argsAddress = LatestByArguments.allocateMemory();
            LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
            LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());

            LatestByArguments.setKeyLo(argsAddress, 0);
            LatestByArguments.setKeyHi(argsAddress, keyCount);
            LatestByArguments.setRowsSize(argsAddress, 0);

            //fixing memory mapping here
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                // we should ignore this update
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                    for (int i = (int) keyCount; i < keyCount * 2; i++) {
                        writer.add(i, 2L * i);
                    }
                }
                // and this one
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < keyCount; i++) {
                        writer.add((int) keyCount - 1, 10L * i);
                    }
                }

                reader.updateKeyCount();

                BitmapIndexUtilsNative.latestScanBackward(
                        reader.getKeyBaseAddress(),
                        reader.getKeyMemorySize(),
                        reader.getValueBaseAddress(),
                        reader.getValueMemorySize(),
                        argsAddress,
                        reader.getColumnTop(),
                        Long.MAX_VALUE,
                        0,
                        0,
                        valueBlockCapacity - 1
                );
            }

            long rowCount = LatestByArguments.getRowsSize(argsAddress);
            Assert.assertEquals(keyCount, rowCount);
            long keyLo = LatestByArguments.getKeyLo(argsAddress);
            Assert.assertEquals(0, keyLo);
            long keyHi = LatestByArguments.getKeyHi(argsAddress);
            Assert.assertEquals(keyCount, keyHi);

            LatestByArguments.releaseMemory(argsAddress);

            for (int i = 0; i < rows.getCapacity(); ++i) {
                Assert.assertEquals(i, Rows.toLocalRowID(rows.get(i) - 1));
            }
        }
    }

    @Test
    public void testCppLatestByIndexReaderIndexedWithTruncate() throws Exception {
        assertMemoryLeak(() -> {
            final int timestampIncrement = 1000000 * 60 * 5;
            final int M = 1000;
            final int N = 100;
            final int indexBlockCapacity = 32;
            // Separate two symbol columns with primitive. It will make problems visible if the index does not shift correctly
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, indexBlockCapacity).
                    col("i", ColumnType.INT).
                    timestamp();
            AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();
            final String[] symbols = new String[N];

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += timestampIncrement);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.append();
                }
                writer.commit();
                rnd.reset();

                writer.addColumn(
                        "c",
                        ColumnType.SYMBOL,
                        Numbers.ceilPow2(N),
                        true,
                        IndexType.BITMAP,
                        indexBlockCapacity,
                        false,
                        AllowAllSecurityContext.INSTANCE);
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += timestampIncrement);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.putSym(4, symbols[rnd.nextPositiveInt() % N]);
                    row.append();
                }
                writer.commit();
                try (DirectLongList rows = new DirectLongList(N, MemoryTag.NATIVE_LONG_LIST)) {
                    rows.setPos(rows.getCapacity());
                    GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

                    try (TableReader tableReader = newOffPoolReader(configuration, "x")) {
                        tableReader.openPartition(0);
                        final int columnBase = tableReader.getColumnBase(0);
                        final int columnIndex = tableReader.getMetadata().getColumnIndex("c");
                        IndexReader reader = tableReader.getIndexReader(
                                0,
                                columnIndex,
                                IndexReader.DIR_BACKWARD
                        );

                        long columnTop = tableReader.getColumnTop(columnBase, columnIndex);

                        long argsAddress = LatestByArguments.allocateMemory();

                        LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
                        LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());

                        LatestByArguments.setKeyLo(argsAddress, 0);
                        LatestByArguments.setKeyHi(argsAddress, N);
                        LatestByArguments.setRowsSize(argsAddress, 0);
                        BitmapIndexUtilsNative.latestScanBackward(
                                reader.getKeyBaseAddress(),
                                reader.getKeyMemorySize(),
                                reader.getValueBaseAddress(),
                                reader.getValueMemorySize(),
                                argsAddress,
                                columnTop,
                                Long.MAX_VALUE,
                                0,
                                0,
                                indexBlockCapacity - 1
                        );

                        long rowCount = LatestByArguments.getRowsSize(argsAddress);
                        Assert.assertEquals(N, rowCount);
                        long keyLo = LatestByArguments.getKeyLo(argsAddress);
                        Assert.assertEquals(0, keyLo);
                        long keyHi = LatestByArguments.getKeyHi(argsAddress);
                        Assert.assertEquals(N, keyHi);

                        LatestByArguments.releaseMemory(argsAddress);
                        Assert.assertEquals(columnTop - 1, Rows.toLocalRowID(rows.get(0) - 1));
                    }
                }
            }
        });
    }

    @Test
    public void testEmptyBackwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertEmptyCursor(reader);
            }
        });
    }

    @Test
    public void testEmptyConcurrentForwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            try (ConcurrentBitmapIndexFwdReader reader = new ConcurrentBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertEmptyCursor(reader);
            }
        });
    }

    @Test
    public void testEmptyCursor() {
        RowCursor cursor = new EmptyRowCursor();
        Assert.assertFalse(cursor.hasNext());
        Assert.assertEquals(0, cursor.next());
    }

    @Test
    public void testEmptyForwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertEmptyCursor(reader);
            }
        });
    }

    @Test
    public void testExtendGranularityWithFdBasedOf() throws Exception {
        // Simulates what O3CopyJob.updateIndex does when appending to a non-last
        // partition: opens BitmapIndexWriter with init=false using fd-based API.
        // The fd-based BitmapIndexWriter.of() uses the 5-arg MemoryCMARWImpl.of()
        // for keyMem. This test verifies that extend0() grows the key file in
        // page-sized chunks (not 1 byte at a time).
        int initialKeys = 1000;
        int newKeys = 10_000;
        int indexBlockCapacity = 4;

        AtomicInteger allocateCount = new AtomicInteger();
        LongList allocateSizes = new LongList();
        long[] trackedKeyFd = {-1};
        AtomicBoolean tracking = new AtomicBoolean();

        FilesFacade testFf = new TestFilesFacadeImpl() {
            @Override
            public boolean allocate(long fd, long size) {
                if (tracking.get() && fd == trackedKeyFd[0]) {
                    allocateCount.incrementAndGet();
                    allocateSizes.add(size);
                }
                return super.allocate(fd, size);
            }
        };

        assertMemoryLeak(testFf, () -> {
            create(configuration, path.trimTo(plen), "x", indexBlockCapacity);

            try (BitmapIndexWriter w = new BitmapIndexWriter(configuration)) {
                // Phase 1: create the index with init=true (simulates first partition creation).
                w.of(path, "x", COLUMN_NAME_TXN_NONE, indexBlockCapacity);
                for (int key = 0; key < initialKeys; key++) {
                    w.add(key, key);
                }
                w.commit();
                w.close();

                // Phase 2: reopen with init=false using fd-based API,
                // exactly as O3CopyJob.updateIndex does for OPEN_MID_PARTITION_FOR_APPEND.
                long keyFd = TableUtils.openRW(
                        testFf,
                        BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE),
                        LOG,
                        configuration.getWriterFileOpenOpts()
                );
                long valueFd = TableUtils.openRW(
                        testFf,
                        BitmapIndexUtils.valueFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE),
                        LOG,
                        configuration.getWriterFileOpenOpts()
                );
                trackedKeyFd[0] = keyFd;
                tracking.set(true);

                // init=false: O3CopyJob passes row > 0 → init = (row == 0) = false
                w.of(configuration, keyFd, valueFd, false, indexBlockCapacity);

                // Add new keys, simulating new unique symbols in O3 append.
                for (int key = initialKeys; key < initialKeys + newKeys; key++) {
                    w.add(key, key);
                }
                w.commit();

                tracking.set(false);

                LOG.info()
                        .$("bitmap index .k allocate calls [initialKeys=").$(initialKeys)
                        .$(", newKeys=").$(newKeys)
                        .$(", allocateCount=").$(allocateCount.get())
                        .I$();
                for (int i = 0, n = allocateSizes.size(); i < n; i++) {
                    LOG.info()
                            .$("  allocate[").$(i)
                            .$("] size=").$(allocateSizes.get(i))
                            .I$();
                }

                // With the fix, extend0() rounds up to page-sized chunks, so
                // 10K new keys (320KB of key data) should need very few allocate
                // calls — not tens of thousands.
                assertTrue(
                        "expected fewer than 100 allocate() calls but got " + allocateCount.get(),
                        allocateCount.get() < 100
                );
            }
        });
    }

    @Test
    public void testFarNewKeyKeyFileExtensionFails() throws Exception {
        // A new key past the key file grows the file when it writes its entry. When the file cannot grow, add() fails
        // before it writes the entry or the key count, and the same add succeeds once the file can grow.
        final KeyFileDiskFullFacade ff = new KeyFileDiskFullFacade();
        assertMemoryLeak(ff, () -> {
            writeCleanIndex(100);
            final long length = keyFileLength();
            final int farKey = farKey(length);
            final LongList values = new LongList();
            BitmapIndexWriter.testZeroedKeyBytes.set(0);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                ff.isDiskFull = true;
                try {
                    writer.add(farKey, 1_000);
                    Assert.fail("the add must fail when the key file cannot grow to hold the entry of the new key");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                }
                Assert.assertEquals(100, writer.getKeyCount());
                Assert.assertEquals(length, keyFileLength());

                ff.isDiskFull = false;
                writer.add(farKey, 1_000);
                writer.setMaxValue(1_000);
                Assert.assertEquals(farKey + 1, writer.getKeyCount());
                Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
            }
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(farKey + 1, reader.getKeyCount());
                for (int key = 0; key < 100; key++) {
                    assertThat("[" + key + "]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                }
                for (int key = 100; key < farKey; key++) {
                    assertThat("[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                }
                assertThat("[1000]", reader.getCursor(farKey, 0, Long.MAX_VALUE), values);
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
            create(configuration, path.trimTo(plen), "x", 1024);

            try (BitmapIndexWriter w = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                w.add(0, 10);
            }

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    try (Path path = new Path()) {
                        path.of(configuration.getDbRoot()).concat("x").put(".k");
                        mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                    }

                    long offset = BitmapIndexUtils.getKeyEntryOffset(0);
                    mem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, 10);

                    try {
                        reader.getCursor(0, 0, Long.MAX_VALUE);
                        Assert.fail();
                    } catch (CairoException e) {
                        assertTrue(Chars.contains(e.getMessage(), "cursor could not consistently read index header"));
                    }
                }
            }
        });
    }

    @Test
    public void testForwardReaderDoesNotUnmapPages() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            create(configuration, path.trimTo(plen), "x", 1024);

            class CountingFacade extends FilesFacadeImpl {
                private int count = 0;

                @Override
                public long getMapPageSize() {
                    return 1024 * 1024;
                }

                @Override
                public void munmap(long address, long size, int memoryTag) {
                    super.munmap(address, size, memoryTag);
                    count++;
                }
            }

            final CountingFacade facade = new CountingFacade();

            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return facade;
                }
            };


            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    for (int i = 0; i < 100000; i++) {
                        int key = rnd.nextPositiveInt() % 1024;
                        long value = rnd.nextPositiveLong();
                        writer.add(key, value);

                        RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                        boolean found = false;
                        while (cursor.hasNext()) {
                            if (value == cursor.next()) {
                                found = true;
                                break;
                            }
                        }
                        assertTrue(found);
                        Misc.free(cursor);
                    }
                    Assert.assertEquals(0, facade.count);
                }
            }
        });
    }

    @Test
    public void testForwardReaderKeyUpdateFail() {

        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public long getSpinLockTimeout() {
                return 3;
            }
        };

        create(configuration, path.trimTo(plen), "x", 1024);

        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
            writer.add(0, 1000);
        }

        try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {

            // should have single value in cursor
            RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
            assertTrue(cursor.hasNext());
            Assert.assertEquals(1000, cursor.next());
            Assert.assertFalse(cursor.hasNext());

            try (
                    Path path = new Path();
                    MemoryCMARW mem = Vm.getSmallCMARWInstance(
                            configuration.getFilesFacade(),
                            path.of(root).concat("x").put(".k").$(),
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                // change sequence but not sequence check
                long seq = mem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                mem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, 22);

                try {
                    reader.getCursor(10, 0, Long.MAX_VALUE);
                    Assert.fail();
                } catch (CairoException e) {
                    assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                // make sure the index fails until the sequence is not up to date

                try {
                    reader.getCursor(0, 0, Long.MAX_VALUE);
                    Assert.fail();
                } catch (CairoException e) {
                    assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                mem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);
                Misc.free(cursor);
                // test that index recovers
                cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
                assertTrue(cursor.hasNext());
                Assert.assertEquals(1000, cursor.next());
                Assert.assertFalse(cursor.hasNext());
            }
            Misc.free(cursor);
        }
    }

    @Test
    public void testIndexDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE);
                Assert.fail();
            } catch (CairoException e) {
                assertTrue(Chars.contains(e.getMessage(), "does not exist"));
            }
        });
    }

    @Test
    public void testInit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 1024);
            Rnd r = TestUtils.generateRandom(LOG);
            final int count = 100 + r.nextInt(1_000_000);
            final int maxKeys = 10 + r.nextInt(1000);

            BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE);
            LongList values = new LongList();
            for (int i = 0; i < count; i++) {
                int key = r.nextInt(maxKeys);
                writer.add(key, i);
                values.add((long) key * count + i);
            }
            assertValuesMatchBitmapWriter(count, writer, values);

            writer.close();
            int plen = path.size();
            FilesFacade ff = configuration.getFilesFacade();

            // Make x.k dirty, write random data
            long fd = ff.openRW(path.concat("x.k").$(), configuration.getWriterFileOpenOpts());
            long len = ff.length(fd);
            assertTrue(len > 0);
            long address = TableUtils.mapRW(ff, fd, len, MemoryTag.MMAP_DEFAULT);
            Vect.memset(address, len, 1);
            ff.close(fd);
            ff.munmap(address, len, MemoryTag.MMAP_DEFAULT);
            path.trimTo(plen);

            // Force initialise index, e.g. truncate
            writer.of(path, "x", COLUMN_NAME_TXN_NONE, 4096);
            values.clear();
            for (int i = 0; i < count; i++) {
                int key = r.nextInt(maxKeys);
                writer.add(key, i);
                values.add((long) key * count + i);
            }
            assertValuesMatchBitmapWriter(count, writer, values);

            writer.close();
        });
    }

    @Test
    public void testLimitBackwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            writeTripleValues(265);

            LongList tmp = new LongList();
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertBackwardCursorLimit(reader, 0, 260, tmp, 0);
                assertBackwardCursorLimit(reader, 0, 260, tmp, 0);
                assertBackwardCursorLimit(reader, 0, 16, tmp, 0);
                assertBackwardCursorLimit(reader, 0, 9, tmp, 0);
                Assert.assertFalse(reader.getCursor(0, -1L, -1L).hasNext());
            }
        });
    }

    @Test
    public void testLimitBackwardCursorWithNulls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            int N = 265;
            int nullsN = 3;
            writeTripleValues(N);

            LongList tmp = new LongList();
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, nullsN)) {
                assertBackwardCursorLimit(reader, 1, 260, tmp, nullsN - 1);
                assertBackwardCursorLimit(reader, 1, 260, tmp, nullsN - 1);
            }
        });
    }

    @Test
    public void testLimitConcurrentForwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            int N = 265;
            long initialMemSize;
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < N; i++) {
                    if (i % 3 == 0) {
                        continue;
                    }
                    writer.add(0, i);
                    writer.add(0, i);
                    writer.add(0, i);
                }
                initialMemSize = writer.getValueMemSize();
            }

            LongList tmp = new LongList();
            try (ConcurrentBitmapIndexFwdReader reader = new ConcurrentBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertForwardCursorLimit(reader, 260, N, tmp, 9, 0);
                assertForwardCursorLimit(reader, 260, N, tmp, 9, 0);
                assertForwardCursorLimit(reader, 260, N - 2, tmp, 6, 0);
                assertForwardCursorLimit(reader, 16, N, tmp, 498, 0);
                assertForwardCursorLimit(reader, 9, N, tmp, 510, 0);
                Assert.assertFalse(reader.getCursor(0, 266, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(reader.getCursor(0, Long.MAX_VALUE, Long.MAX_VALUE).hasNext());

                // Write a lot more values, so that we extend the value file.
                long newMemSize;
                int newKeysN = 5 * N;
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                    for (int i = N; i < newKeysN; i++) {
                        if (i % 3 == 0) {
                            continue;
                        }
                        writer.add(0, i);
                        writer.add(0, i);
                        writer.add(0, i);
                    }
                    newMemSize = writer.getValueMemSize();
                }
                assertTrue("Value file should grow in size", newMemSize > initialMemSize);

                // The reader should stop at the last known valueMem size boundary now.
                assertForwardCursorLimit(reader, 0, N, tmp, 528, 0);
                assertForwardCursorLimit(reader, 0, N + newKeysN, tmp, 639, 0);
                Assert.assertFalse(reader.getCursor(0, N + newKeysN, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(reader.getCursor(0, Long.MAX_VALUE, Long.MAX_VALUE).hasNext());
            }
        });
    }

    @Test
    public void testLimitConcurrentForwardCursorWithNulls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            int N = 265;
            int nullsN = 3;
            writeTripleValues(N);

            LongList tmp = new LongList();
            try (ConcurrentBitmapIndexFwdReader reader = new ConcurrentBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, nullsN)) {
                assertForwardCursorLimit(reader, 1, N, tmp, 530, nullsN - 1);
                assertForwardCursorLimit(reader, 1, N, tmp, 530, nullsN - 1);
            }
        });
    }

    @Test
    public void testLimitForwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            int N = 265;
            writeTripleValues(N);

            LongList tmp = new LongList();
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertForwardCursorLimit(reader, 260, N, tmp, 9, 0);
                assertForwardCursorLimit(reader, 260, N, tmp, 9, 0);
                assertForwardCursorLimit(reader, 260, N - 2, tmp, 6, 0);
                assertForwardCursorLimit(reader, 16, N, tmp, 498, 0);
                assertForwardCursorLimit(reader, 9, N, tmp, 510, 0);
                Assert.assertFalse(reader.getCursor(0, 266, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(reader.getCursor(0, Long.MAX_VALUE, Long.MAX_VALUE).hasNext());
            }
        });
    }

    @Test
    public void testLimitForwardCursorWithNulls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            int N = 265;
            int nullsN = 3;
            writeTripleValues(N);

            LongList tmp = new LongList();
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, nullsN)) {
                assertForwardCursorLimit(reader, 1, N, tmp, 530, nullsN - 1);
                assertForwardCursorLimit(reader, 1, N, tmp, 530, nullsN - 1);
            }
        });
    }

    @Test
    public void testNegativeKeyCountKeepsHeader() throws Exception {
        // No writer stores a negative key count, but both ways of opening a writer accept one, as a corrupt or tampered
        // file can hold. The open writes nothing, and a new key zeroes the entries it skips from the end of the header:
        // the header keeps its signature, block value count and sequence pair, and the file opens again.
        assertMemoryLeak(() -> {
            final LongList values = new LongList();
            // the entries below a count of -1 would end inside the header, of -2 at its start, of -3 before the file
            for (int keyCount : new int[]{-1, -2, -3, Integer.MIN_VALUE + 1, Integer.MIN_VALUE}) {
                for (boolean isOpenByFd : new boolean[]{false, true}) {
                    removeIndexFiles();
                    create(configuration, path.trimTo(plen), "x", 64);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                        for (int key = 0; key < 100; key++) {
                            writer.add(key, 1_000 + key);
                        }
                        writer.setMaxValue(1_099);
                        writer.closeNoTruncate();
                    }
                    writeKeyFileInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
                    final byte[] header = readKeyFile(0, BitmapIndexUtils.KEY_FILE_RESERVED);

                    BitmapIndexWriter.testZeroedKeyBytes.set(0);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                        openIndexWriter(writer, isOpenByFd);
                        Assert.assertEquals(keyCount, writer.getKeyCount());
                        Assert.assertArrayEquals(header, readKeyFile(0, BitmapIndexUtils.KEY_FILE_RESERVED));
                        Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());

                        // every key is new, and key 0 skips none
                        writer.add(0, 5);
                        Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                        // key 40 skips the old entries of keys 1 to 39: [96, 1_344)
                        writer.add(40, 6);
                        Assert.assertEquals(1_248, BitmapIndexWriter.testZeroedKeyBytes.get());
                        Assert.assertEquals(41, writer.getKeyCount());

                        final ByteBuffer newHeader = ByteBuffer.wrap(readKeyFile(0, BitmapIndexUtils.KEY_FILE_RESERVED)).order(ByteOrder.nativeOrder());
                        Assert.assertEquals(BitmapIndexUtils.SIGNATURE, newHeader.get(BitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE));
                        Assert.assertEquals(64, newHeader.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT));
                        Assert.assertEquals(41, newHeader.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT));
                        Assert.assertEquals(
                                newHeader.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE),
                                newHeader.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK)
                        );
                        assertNewKeysOverNegativeKeyCount(writer::getCursor, values);
                    }
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                        Assert.assertEquals(41, writer.getKeyCount());
                    }
                    // a reader opens the file once add() has published a key count that covers the entries
                    try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                        Assert.assertEquals(41, reader.getKeyCount());
                        assertNewKeysOverNegativeKeyCount(key -> reader.getCursor(key, 0, Long.MAX_VALUE), values);
                    }
                }
            }
        });
    }

    @Test
    public void testNegativeKeyCountSparseFirstKeyZeroesOldEntries() throws Exception {
        // A negative key count covers no entry, so a first new key above 0 skips the entries from the end of the header
        // on. A corrupt or tampered file can hold old entries there: add() zeroes them before the key count makes them
        // keys, or keys 0 to 39 would carry their old values. The header keeps its signature, block value count, max
        // value and sequence pair.
        assertMemoryLeak(() -> {
            final LongList values = new LongList();
            for (int keyCount : new int[]{-1, -2, Integer.MIN_VALUE}) {
                for (boolean isOpenByFd : new boolean[]{false, true}) {
                    removeIndexFiles();
                    // 100 old entries, key k holding the value 1_000 + k, under a negative key count
                    writeStaleKeysAboveZeroKeyCount(64);
                    writeKeyFileInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);

                    BitmapIndexWriter.testZeroedKeyBytes.set(0);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                        openIndexWriter(writer, isOpenByFd);
                        Assert.assertEquals(keyCount, writer.getKeyCount());
                        // the first add: key 40 skips the old entries of keys 0 to 39, [64, 1_344)
                        writer.add(40, 6);
                        Assert.assertEquals(1_280, BitmapIndexWriter.testZeroedKeyBytes.get());
                        Assert.assertEquals(41, writer.getKeyCount());
                        assertAllZero(readKeyFile(BitmapIndexUtils.KEY_FILE_RESERVED, 1_344));

                        final ByteBuffer header = ByteBuffer.wrap(readKeyFile(0, BitmapIndexUtils.KEY_FILE_RESERVED)).order(ByteOrder.nativeOrder());
                        Assert.assertEquals(BitmapIndexUtils.SIGNATURE, header.get(BitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE));
                        Assert.assertEquals(64, header.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT));
                        Assert.assertEquals(41, header.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT));
                        Assert.assertEquals(1_099, header.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_MAX_VALUE));
                        Assert.assertEquals(
                                header.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE),
                                header.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK)
                        );
                        for (int key = 0; key < 40; key++) {
                            assertThat("[]", writer.getCursor(key), values);
                        }
                        assertThat("[6]", writer.getCursor(40), values);
                    }
                    // a reader opens the file once add() has published a key count that covers the entries
                    try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                        Assert.assertEquals(41, reader.getKeyCount());
                        for (int key = 0; key < 40; key++) {
                            assertThat("[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                        }
                        assertThat("[6]", reader.getCursor(40, 0, Long.MAX_VALUE), values);
                    }
                }
            }
        });
    }

    @Test
    public void testNewKeyAfterCleanReopenWritesNoSkippedKeys() throws Exception {
        // A writer that closes cleanly cuts the key file after the page that holds the last entry, and zeroes the rest
        // of that page. The next writer open writes nothing, and a new key finds zeros in the entries it skips, dense,
        // sparse or past the end of the file, so it writes nothing either.
        assertMemoryLeak(() -> {
            final LongList values = new LongList();
            // 0 keys, one, a partial page, a key file that ends on the page and has no tail, and many pages
            for (int keyCount : new int[]{0, 1, 100, (int) ((Files.PAGE_SIZE - BitmapIndexUtils.KEY_FILE_RESERVED) / BitmapIndexUtils.KEY_ENTRY_SIZE), 20_001}) {
                for (boolean isOpenByFd : new boolean[]{false, true}) {
                    removeIndexFiles();
                    writeCleanIndex(keyCount);
                    final long keyMemSize = BitmapIndexUtils.KEY_FILE_RESERVED + keyCount * BitmapIndexUtils.KEY_ENTRY_SIZE;
                    final long length = keyFileLength();
                    Assert.assertTrue(length >= keyMemSize);
                    assertAllZero(readKeyFile(keyMemSize, length));

                    BitmapIndexWriter.testZeroedKeyBytes.set(0);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                        openIndexWriter(writer, isOpenByFd);
                        Assert.assertEquals(keyCount, writer.getKeyCount());
                        Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                        assertNewKeysWriteNoSkippedKeys(writer, keyCount, length, values);
                    }
                    assertNewKeysReadBack(keyCount, length, values);
                }
            }

            // a new index has no entries: a writer that creates it maps the key file to the end of its header
            for (boolean isOpenByFd : new boolean[]{false, true}) {
                removeIndexFiles();
                create(configuration, path.trimTo(plen), "x", 64);
                BitmapIndexWriter.testZeroedKeyBytes.set(0);
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                    if (isOpenByFd) {
                        final FilesFacade ff = configuration.getFilesFacade();
                        final long keyFd = TableUtils.openRW(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
                        final long valueFd = TableUtils.openRW(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
                        path.trimTo(plen);
                        writer.of(configuration, keyFd, valueFd, true, 64);
                    } else {
                        writer.of(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, 64);
                    }
                    Assert.assertEquals(0, writer.getKeyCount());
                    assertNewKeysWriteNoSkippedKeys(writer, 0, keyFileLength(), values);
                }
            }
        });
    }

    @Test
    public void testNewKeyAfterInSessionTruncateReadsNoSkippedKeys() throws Exception {
        // truncate() zeroes every entry and moves the append offset of the key memory to the end of the header. A new
        // key in the same session reads none of the entries it skips, though the file held old entries past the page
        // truncate() keeps mapped.
        final int r = keyAppendPageRatio();
        assertMemoryLeak(() -> {
            final LongList values = new LongList();
            BitmapIndexWriter.isScannedKeyBytesCounterEnabled = true;
            try {
                for (boolean isOpenByFd : new boolean[]{false, true}) {
                    removeIndexFiles();
                    create(configuration, path.trimTo(plen), "x", 64);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                        for (int key = 0; key < 1_000 * r; key++) {
                            writer.add(key, key);
                        }
                        writer.setMaxValue(1_000 * r - 1);
                        writer.closeNoTruncate();
                    }
                    // the state a kill inside truncate() leaves, over more than the page truncate() keeps mapped
                    writeKeyFileInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT, 0);
                    Assert.assertTrue(keyFileLength() > configuration.getDataIndexKeyAppendPageSize());

                    BitmapIndexWriter.testZeroedKeyBytes.set(0);
                    BitmapIndexWriter.testScannedKeyBytes.set(0);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                        openIndexWriter(writer, isOpenByFd);
                        writer.truncate();
                        Assert.assertEquals(0, writer.getKeyCount());
                        writer.add(900 * r, 5);
                        writer.setMaxValue(5);
                        Assert.assertEquals(0, BitmapIndexWriter.testScannedKeyBytes.get());
                        Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                        Assert.assertEquals(900 * r + 1, writer.getKeyCount());
                        for (int key = 0; key < 900 * r; key++) {
                            assertThat("[]", writer.getCursor(key), values);
                        }
                        assertThat("[5]", writer.getCursor(900 * r), values);
                    }
                    try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                        Assert.assertEquals(900 * r + 1, reader.getKeyCount());
                        for (int key = 0; key < 900 * r; key++) {
                            assertThat("[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                        }
                        assertThat("[5]", reader.getCursor(900 * r, 0, Long.MAX_VALUE), values);
                    }
                }
            } finally {
                BitmapIndexWriter.isScannedKeyBytesCounterEnabled = false;
            }
        });
    }

    @Test
    public void testNewKeyAfterNonTruncatingReopenWritesNoSkippedKeys() throws Exception {
        // The old entries a kill inside truncate() leaves above the key count stay on disk through a writer session that
        // adds no new key and closes without truncating, as a rollback does, and the first new key past them zeroes
        // them. A truncating close zeroes the rest of the page that holds the last entry, which holds them here.
        assertMemoryLeak(() -> {
            final LongList values = new LongList();
            for (boolean isTruncatingClose : new boolean[]{false, true}) {
                removeIndexFiles();
                writeStaleKeysAboveZeroKeyCount(64);
                BitmapIndexWriter.testZeroedKeyBytes.set(0);
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                    Assert.assertEquals(0, writer.getKeyCount());
                    if (!isTruncatingClose) {
                        writer.closeNoTruncate();
                    }
                }
                Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                    writer.add(50, 5);
                    writer.setMaxValue(5);
                    // [64, 1_664)
                    Assert.assertEquals(isTruncatingClose ? 0 : 1_600, BitmapIndexWriter.testZeroedKeyBytes.get());
                    Assert.assertEquals(51, writer.getKeyCount());
                }
                try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    Assert.assertEquals(51, reader.getKeyCount());
                    for (int key = 0; key < 50; key++) {
                        assertThat("[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                    }
                    assertThat("[5]", reader.getCursor(50, 0, Long.MAX_VALUE), values);
                }
            }
        });
    }

    @Test
    public void testNewKeyReadsOnlyTheKeysItSkipsBelowMappedLength() throws Exception {
        // Opening a writer reads no entry. A new key reads the entries it skips, and only those below the length of the
        // key file the writer mapped at open: past it the file grew with zeros in this session. A close that does not
        // truncate, as a rollback makes, leaves a whole key append page past the last entry.
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_INDEX_KEY_APPEND_PAGE_SIZE, 512 * 1024);
        assertMemoryLeak(() -> {
            final LongList values = new LongList();
            BitmapIndexWriter.isScannedKeyBytesCounterEnabled = true;
            try {
                for (boolean isOpenByFd : new boolean[]{false, true}) {
                    removeIndexFiles();
                    create(configuration, path.trimTo(plen), "x", 64);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                        for (int key = 0; key < 1_000; key++) {
                            writer.add(key, key);
                        }
                        writer.setMaxValue(999);
                        writer.closeNoTruncate();
                    }
                    final long length = keyFileLength();
                    Assert.assertEquals(512 * 1024, length);
                    // its entry lies past the length the writer maps
                    final int farKey = (int) (length / BitmapIndexUtils.KEY_ENTRY_SIZE);

                    BitmapIndexWriter.testZeroedKeyBytes.set(0);
                    BitmapIndexWriter.testScannedKeyBytes.set(0);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                        openIndexWriter(writer, isOpenByFd);
                        Assert.assertEquals(0, BitmapIndexWriter.testScannedKeyBytes.get());
                        writer.add(1_000, 1_000);
                        Assert.assertEquals(0, BitmapIndexWriter.testScannedKeyBytes.get());
                        // keys 1_001 to 1_006
                        writer.add(1_007, 1_007);
                        Assert.assertEquals(192, BitmapIndexWriter.testScannedKeyBytes.get());
                        // [32_320, length)
                        writer.add(farKey, farKey);
                        writer.setMaxValue(farKey);
                        Assert.assertEquals(192 + length - 32_320, BitmapIndexWriter.testScannedKeyBytes.get());
                        Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                    }

                    BitmapIndexWriter.testScannedKeyBytes.set(0);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                        openIndexWriter(writer, isOpenByFd);
                        writer.add(farKey + 1, farKey + 1);
                        writer.setMaxValue(farKey + 1);
                        Assert.assertEquals(0, BitmapIndexWriter.testScannedKeyBytes.get());
                        Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                    }
                    try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                        Assert.assertEquals(farKey + 2, reader.getKeyCount());
                        for (int key = 0; key <= farKey + 1; key++) {
                            final boolean hasValue = key <= 1_000 || key == 1_007 || key >= farKey;
                            assertThat(hasValue ? "[" + key + "]" : "[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                        }
                    }
                }
            } finally {
                BitmapIndexWriter.isScannedKeyBytesCounterEnabled = false;
            }
        });
    }

    @Test
    public void testNewKeyValueAllocationFailsAfterZeroingSkippedKeys() throws Exception {
        // add() zeroes the old entries a new key skips before it allocates the value block of the key. When the value
        // file cannot grow for that block, add() fails with the key count as it was and the zeroed entries above it,
        // and the retry finds them zero.
        final long pageSize = Files.PAGE_SIZE;
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE, pageSize);
        final ValueFileDiskFullFacade ff = new ValueFileDiskFullFacade();
        assertMemoryLeak(ff, () -> {
            // a block of a quarter page of values ends past the value file of 100 single-value keys
            writeStaleKeysAboveZeroKeyCount((int) (pageSize / 4));
            final LongList values = new LongList();
            BitmapIndexWriter.testZeroedKeyBytes.set(0);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                ff.isDiskFull = true;
                try {
                    writer.add(50, 5);
                    Assert.fail("the add must fail when the value file cannot grow to cover the new block");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                }
                Assert.assertEquals(0, writer.getKeyCount());
                // [64, 1_664)
                Assert.assertEquals(1_600, BitmapIndexWriter.testZeroedKeyBytes.get());
                assertAllZero(readKeyFile(BitmapIndexUtils.KEY_FILE_RESERVED, 1_664));

                ff.isDiskFull = false;
                writer.add(50, 5);
                writer.setMaxValue(5);
                Assert.assertEquals(1_600, BitmapIndexWriter.testZeroedKeyBytes.get());
                Assert.assertEquals(51, writer.getKeyCount());
                for (int key = 0; key < 50; key++) {
                    assertThat("[]", writer.getCursor(key), values);
                }
                assertThat("[5]", writer.getCursor(50), values);
            }
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(51, reader.getKeyCount());
                for (int key = 0; key < 50; key++) {
                    assertThat("[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                }
                assertThat("[5]", reader.getCursor(50, 0, Long.MAX_VALUE), values);
            }
        });
    }

    @Test
    public void testNewKeyZeroesOrphanEntryAboveKeyCount() throws Exception {
        // A process killed inside add() after it wrote the entry of a new key and before it raised the key count leaves
        // the entry above the key count. A later new key past it zeroes it, and the same key added again overwrites all
        // of it: the old value never comes back.
        assertMemoryLeak(() -> {
            final LongList values = new LongList();
            for (boolean isOrphanKeyAddedAgain : new boolean[]{false, true}) {
                for (boolean isOpenByFd : new boolean[]{false, true}) {
                    removeIndexFiles();
                    create(configuration, path.trimTo(plen), "x", 64);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                        for (int key = 0; key < 10; key++) {
                            writer.add(key, key);
                        }
                        writer.add(20, 777);
                        writer.setMaxValue(777);
                        writer.closeNoTruncate();
                    }
                    // the key count from before the add of key 20
                    writeKeyFileInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT, 10);

                    BitmapIndexWriter.testZeroedKeyBytes.set(0);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                        openIndexWriter(writer, isOpenByFd);
                        Assert.assertEquals(10, writer.getKeyCount());
                        Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                        if (isOrphanKeyAddedAgain) {
                            // [384, 704) holds zeros
                            writer.add(20, 3);
                            Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                        } else {
                            // [384, 544) holds zeros
                            writer.add(15, 1);
                            Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                            // [576, 1_024) holds the entry of key 20, at 704
                            writer.add(30, 2);
                            Assert.assertEquals(448, BitmapIndexWriter.testZeroedKeyBytes.get());
                        }
                        writer.setMaxValue(9);
                        assertNewKeysOverOrphanEntry(writer::getCursor, isOrphanKeyAddedAgain, values);
                    }
                    try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                        Assert.assertEquals(isOrphanKeyAddedAgain ? 21 : 31, reader.getKeyCount());
                        assertNewKeysOverOrphanEntry(key -> reader.getCursor(key, 0, Long.MAX_VALUE), isOrphanKeyAddedAgain, values);
                    }
                }
            }
        });
    }

    @Test
    public void testNewKeyZeroesStaleByteAtGapBoundaries() throws Exception {
        // One byte that is not zero anywhere in the entries a new key skips makes the key zero them all: at the first
        // byte above the key count, inside the gap, and at the last byte of the key file. 0x80 and 0xFF are negative
        // bytes.
        assertMemoryLeak(() -> {
            final LongList values = new LongList();
            for (byte stale : new byte[]{0x01, (byte) 0x80, (byte) 0xFF}) {
                for (int position = 0; position < 3; position++) {
                    removeIndexFiles();
                    writeCleanIndex(100);
                    final long length = keyFileLength();
                    final long staleOffset = switch (position) {
                        case 0 -> 3_264;
                        case 1 -> (3_264 + length) / 2;
                        default -> length - 1;
                    };
                    writeKeyFile(staleOffset, new byte[]{stale});
                    final int farKey = farKey(length);

                    BitmapIndexWriter.testZeroedKeyBytes.set(0);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                        Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                        Assert.assertArrayEquals(new byte[]{stale}, readKeyFile(staleOffset, staleOffset + 1));
                        writer.add(farKey, 100_000);
                        writer.setMaxValue(100_000);
                        // [3_264, length)
                        Assert.assertEquals(length - 3_264, BitmapIndexWriter.testZeroedKeyBytes.get());
                        Assert.assertArrayEquals(new byte[]{0}, readKeyFile(staleOffset, staleOffset + 1));
                        for (int key = 100; key < farKey; key++) {
                            assertThat("[]", writer.getCursor(key), values);
                        }
                        assertThat("[100000]", writer.getCursor(farKey), values);
                    }
                }
            }
        });
    }

    @Test
    public void testNewKeyZeroesStaleByteInOddLengthKeyFile() throws Exception {
        // A key file that ends inside an entry, as a torn or corrupt file can: a new key past the file reads the bytes
        // after the last whole long one at a time, and zeroes them when one is not zero.
        assertMemoryLeak(() -> {
            final LongList values = new LongList();
            for (boolean isOpenByFd : new boolean[]{false, true}) {
                removeIndexFiles();
                writeCleanIndex(100);
                // 13 bytes past the last entry, the last one not zero
                final FilesFacade ff = configuration.getFilesFacade();
                final long fd = TableUtils.openRW(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
                path.trimTo(plen);
                try {
                    Assert.assertTrue(ff.truncate(fd, 3_277));
                } finally {
                    ff.close(fd);
                }
                writeKeyFile(3_276, new byte[]{0x5A});

                BitmapIndexWriter.testZeroedKeyBytes.set(0);
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                    openIndexWriter(writer, isOpenByFd);
                    Assert.assertEquals(100, writer.getKeyCount());
                    Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                    writer.add(200, 5);
                    writer.setMaxValue(5);
                    // [3_264, 3_277)
                    Assert.assertEquals(13, BitmapIndexWriter.testZeroedKeyBytes.get());
                    Assert.assertArrayEquals(new byte[]{0}, readKeyFile(3_276, 3_277));
                    for (int key = 100; key < 200; key++) {
                        assertThat("[]", writer.getCursor(key), values);
                    }
                    assertThat("[5]", writer.getCursor(200), values);
                }
                try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    Assert.assertEquals(201, reader.getKeyCount());
                    for (int key = 0; key < 100; key++) {
                        assertThat("[" + key + "]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                    }
                    for (int key = 100; key < 200; key++) {
                        assertThat("[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                    }
                    assertThat("[5]", reader.getCursor(200, 0, Long.MAX_VALUE), values);
                }
            }
        });
    }

    @Test
    public void testNewKeyZeroesStaleKeysAfterTruncateKill() throws Exception {
        assertNewKeyZeroesStaleKeysAfterTruncateKill(false);
    }

    @Test
    public void testNewKeyZeroesStaleKeysAfterTruncateKillOpenByFd() throws Exception {
        assertNewKeyZeroesStaleKeysAfterTruncateKill(true);
    }

    @Test
    public void testNewKeyZeroesStaleKeysLeftByKillAtKeyFileCut() throws Exception {
        // truncate() zeroes the entries in the page it keeps mapped, then cuts the key file after that page. A process
        // killed at the cut leaves the old entries of the pages past it above a zero key count. The next writer open
        // writes nothing, and each new key zeroes the old entries it skips.
        final int r = keyAppendPageRatio();
        final long keyAppendPageSize = configuration.getDataIndexKeyAppendPageSize();
        final int keyCount = 1_000 * r;
        final KeyFileCutFacade ff = new KeyFileCutFacade();
        assertMemoryLeak(ff, () -> {
            create(configuration, path.trimTo(plen), "x", 64);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < 20 * keyCount; i++) {
                    writer.add(i % keyCount, i);
                }
                writer.setMaxValue(20L * keyCount - 1);
                ff.isRecording = true;
                writer.truncate();
                ff.isRecording = false;
            }
            final byte[] keyFileAtCut = ff.keyFileAtCut;
            Assert.assertNotNull(keyFileAtCut);
            final ByteBuffer header = ByteBuffer.wrap(keyFileAtCut).order(ByteOrder.nativeOrder());
            Assert.assertEquals(0, header.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT));
            final long entriesEnd = BitmapIndexUtils.KEY_FILE_RESERVED + keyCount * BitmapIndexUtils.KEY_ENTRY_SIZE;
            Assert.assertTrue(keyFileAtCut.length >= entriesEnd);
            for (int i = BitmapIndexUtils.KEY_FILE_RESERVED; i < keyFileAtCut.length; i++) {
                if (i < keyAppendPageSize || i >= entriesEnd) {
                    Assert.assertEquals("byte " + i, 0, keyFileAtCut[i]);
                }
            }
            // every old entry past the page counts 20 values
            for (long offset = keyAppendPageSize; offset < entriesEnd; offset += BitmapIndexUtils.KEY_ENTRY_SIZE) {
                Assert.assertEquals("entry at " + offset, 20, header.getLong((int) (offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT)));
            }

            // the restart finds the key file as the kill left it
            writeKeyFile(0, keyFileAtCut);
            final LongList values = new LongList();
            final int firstKey = 700 * r;
            final int lastKey = keyCount - 1;
            BitmapIndexWriter.testZeroedKeyBytes.set(0);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                Assert.assertEquals(0, writer.getKeyCount());
                Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                writer.add(firstKey, 5);
                // [64, 64 + 32 * firstKey)
                Assert.assertEquals(22_400L * r, BitmapIndexWriter.testZeroedKeyBytes.get());
                writer.add(lastKey, 6);
                writer.setMaxValue(6);
                // and [64 + 32 * (firstKey + 1), 64 + 32 * lastKey)
                Assert.assertEquals(32_000L * r - 64, BitmapIndexWriter.testZeroedKeyBytes.get());
            }
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(keyCount, reader.getKeyCount());
                for (int key = 0; key < keyCount; key++) {
                    assertThat(key == firstKey ? "[5]" : key == lastKey ? "[6]" : "[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                }
            }
        });
    }

    @Test
    public void testNullMemDoesNotCauseInfiniteLoops() {
        try {
            BitmapIndexUtils.searchValueBlock(new NullMemoryCMR(), 0L, 63L, 1L);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "index is corrupt, rowid not found [offset=0, cellCount=63, value=1]");
        }
    }

    @Test
    public void testReaderSeesSkippedKeysEmptyAfterRepair() throws Exception {
        // A reader open on the index a kill inside truncate() left reads no key. A writer zeroes the old entries a new
        // key skips before it publishes the key count that covers them, so the reader that takes the new count reads
        // those keys empty.
        assertMemoryLeak(() -> {
            writeStaleKeysAboveZeroKeyCount(64);
            final LongList values = new LongList();
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(0, reader.getKeyCount());
                assertThat("[]", reader.getCursor(10, 0, Long.MAX_VALUE), values);

                BitmapIndexWriter.testZeroedKeyBytes.set(0);
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                    Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                    writer.add(50, 5);
                    writer.setMaxValue(5);
                    // [64, 1_664)
                    Assert.assertEquals(1_600, BitmapIndexWriter.testZeroedKeyBytes.get());

                    // the reader takes the key count of the writer at the next cursor
                    for (int key = 0; key < 50; key++) {
                        assertThat("[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                    }
                    assertThat("[5]", reader.getCursor(50, 0, Long.MAX_VALUE), values);
                    Assert.assertEquals(51, reader.getKeyCount());
                }
            }
        });
    }

    @Test
    public void testRollbackOnEmptyIndexDiskFullKeepsHeaderCovered() throws Exception {
        // At the smallest value page the OS allows (4 KiB on Linux x86, 16 KiB on macOS arm64, 64 KiB on Windows) a
        // block of a quarter page of values (two pages and 16 bytes) is larger than the value file a new writer maps,
        // so the column-top rollback of an empty index has to grow the file before it publishes one block. When that
        // growth fails, the rollback must fail and leave the published size where the file still covers it.
        final long pageSize = Files.PAGE_SIZE;
        final int blockValueCount = (int) (pageSize / 4);
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE, pageSize);
        final ValueFileDiskFullFacade ff = new ValueFileDiskFullFacade();
        assertMemoryLeak(ff, () -> {
            Assert.assertEquals(pageSize, configuration.getDataIndexValueAppendPageSize());
            create(configuration, path.trimTo(plen), "x", blockValueCount);
            final long blockCapacity = blockValueCount * 8L + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                Assert.assertTrue(valueFileLength() < blockCapacity);
                ff.isDiskFull = true;
                try {
                    writer.rollbackConditionally(10);
                    Assert.fail("the rollback must fail when the value file cannot grow to the size it publishes");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                }
                Assert.assertEquals(0, writer.getValueMemSize());
                Assert.assertEquals(0, readValueMemSizeHeader());
                try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    Assert.assertEquals(0, reader.getKeyCount());
                }
                // what a kill at this point leaves on disk
                writer.closeNoTruncate();
            }
            ff.isDiskFull = false;
            final LongList values = new LongList();
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                writer.rollbackConditionally(10);
                Assert.assertEquals(blockCapacity, writer.getValueMemSize());
                Assert.assertTrue(valueFileLength() >= blockCapacity);
                writer.add(0, 10);
                writer.setMaxValue(10);
            }
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[10]", reader.getCursor(0, 0, Long.MAX_VALUE), values);
            }
        });
    }

    @Test
    public void testRollbackOnEmptyIndexKeepsValueFileCoveringHeader() throws Exception {
        // The first commit after ADD COLUMN ... INDEX into a partition that has rows rolls the empty index back to
        // the column top, then adds keys. Every value size the writer publishes after that must be covered by the
        // value file: a reader opens the file after each add, and a writer reopens it after a close that does not
        // truncate, which leaves the files as a kill would. At the smallest value page the OS allows, blocks of a
        // sixteenth of a page of values (half a page and 16 bytes) put a page boundary inside the second block.
        final long pageSize = Files.PAGE_SIZE;
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE, pageSize);
        assertMemoryLeak(() -> {
            Assert.assertEquals(pageSize, configuration.getDataIndexValueAppendPageSize());
            create(configuration, path.trimTo(plen), "x", (int) (pageSize / 16));
            final LongList values = new LongList();
            final int keyCount = 40;
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                writer.rollbackConditionally(10);
                for (int key = 0; key < keyCount; key++) {
                    writer.add(key, 10 + key);
                    writer.setMaxValue(10 + key);
                    try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                        assertThat("[" + (10 + key) + "]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                    }
                    Assert.assertTrue("value file shorter than the published size at key " + key, valueFileLength() >= writer.getValueMemSize());
                }
                writer.closeNoTruncate();
            }
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                Assert.assertEquals(keyCount, writer.getKeyCount());
                for (int key = 0; key < keyCount; key++) {
                    assertThat("[" + (10 + key) + "]", writer.getCursor(key), values);
                }
            }
        });
    }

    @Test
    public void testSimpleRollback() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd modelRnd = new Rnd();
            final int maxKeys = 1024;
            final int N = 1000000;
            final int CUTOFF = 60000;

            // this is an assertion in case somebody changes the test
            //noinspection ConstantConditions
            assert CUTOFF < N;

            IntList keys = new IntList();
            IntObjHashMap<LongList> lists = new IntObjHashMap<>();

            // populate model for both reader and writer
            for (int i = 0; i < N; i++) {
                int key = modelRnd.nextPositiveInt() % maxKeys;
                LongList list = lists.get(key);
                if (list == null) {
                    lists.put(key, list = new LongList());
                    keys.add(key);
                }

                if (i > CUTOFF) {
                    continue;
                }

                list.add(i);
            }

            Rnd rnd = new Rnd();
            create(configuration, path.trimTo(plen), "x", 1024);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < N; i++) {
                    writer.add(rnd.nextPositiveInt() % maxKeys, i);
                }
                writer.rollbackValues(CUTOFF);
            }

            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(--v), cursor.next());
                    }
                    Assert.assertEquals(0, v);
                    Misc.free(cursor);
                }
            }

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(v++), cursor.next());
                    }
                    Assert.assertEquals(list.size(), v);
                    Misc.free(cursor);
                }
            }

            try (ConcurrentBitmapIndexFwdReader reader = new ConcurrentBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(v++), cursor.next());
                    }
                    Assert.assertEquals(list.size(), v);
                    Misc.free(cursor);
                }
            }

            // add more data to the model
            for (int i = 0; i < N; i++) {
                int key = modelRnd.nextPositiveInt() % maxKeys;
                LongList list = lists.get(key);
                if (list == null) {
                    lists.put(key, list = new LongList());
                    keys.add(key);
                }
                list.add(i + N);
            }

            // add more date to index
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < N; i++) {
                    writer.add(rnd.nextPositiveInt() % maxKeys, i + N);
                }
            }

            // assert against the model again
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(--v), cursor.next());
                    }
                    Assert.assertEquals(0, v);
                    Misc.free(cursor);
                }
            }

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(v++), cursor.next());
                    }
                    Assert.assertEquals(list.size(), v);
                    Misc.free(cursor);
                }
            }

            try (ConcurrentBitmapIndexFwdReader reader = new ConcurrentBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(v++), cursor.next());
                    }
                    Assert.assertEquals(list.size(), v);
                    Misc.free(cursor);
                }
            }
        });
    }

    @Test
    public void testTruncate() {
        create(configuration, path.trimTo(plen), "x", 1024);

        int count = 2_500;
        // add multiple keys, 1,2,3,4,5,6,7... etc
        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
            for (int i = 0; i < count; i++) {
                writer.add(i, 1000);
            }

            // check that these keys exist in the index
            Assert.assertEquals(count, writer.getKeyCount());

            for (int i = 0; i < count; i++) {
                RowCursor cursor = writer.getCursor(i);
                assertTrue(cursor.hasNext());
                Assert.assertEquals(1000, cursor.next());
                Assert.assertFalse(cursor.hasNext());
                Misc.free(cursor);
            }

            writer.truncate();
            Assert.assertEquals(0, writer.getKeyCount());

            // now add the middle key first
            writer.add(900, 8000);
            Assert.assertEquals(901, writer.getKeyCount());

            try (IndexReader reader = new BitmapIndexBwdReader(configuration, path, "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(901, reader.getKeyCount());
                RowCursor cursor = reader.getCursor(900, 0, 1_000_000);
                assertTrue(cursor.hasNext());
                Assert.assertEquals(8000, cursor.next());
                Assert.assertFalse(cursor.hasNext());

                // assert that key below 900 do no have values
                for (int i = 0; i < 900; i++) {
                    Assert.assertFalse(reader.getCursor(i, 0, 1_000_000).hasNext());
                }
                Misc.free(cursor);
            }

        }

    }

    @Test
    public void testTruncateKeepsKeyFileHeader() throws Exception {
        // truncate() empties the index in place. A process killed when it cuts the key file or the value file finds
        // the header with its signature, block value count and sequence pair as they were, and the key count, value
        // memory size and max value already empty, so a reader and the next writer open take the key file.
        final KeyFileCutFacade ff = new KeyFileCutFacade();
        assertMemoryLeak(ff, () -> {
            create(configuration, path.trimTo(plen), "x", 64);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // keys past the first key page, values over many blocks
                for (int i = 0; i < 20_000; i++) {
                    writer.add(i % 1_000, i);
                }
                writer.setMaxValue(19_999);
                final long sequence = ff.readNonNegativeLong(ff.keyFd, BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                Assert.assertTrue(sequence > 0);
                Assert.assertEquals(
                        "signature=250, sequence=" + sequence + ", sequenceCheck=" + sequence + ", blockValueCount=64"
                                + ", keyCount=1000, valueMemSize=" + writer.getValueMemSize() + ", maxValue=19999",
                        readKeyFileHeader(ff, ff.keyFd)
                );
                final String emptyHeader = "signature=250, sequence=" + sequence + ", sequenceCheck=" + sequence
                        + ", blockValueCount=64, keyCount=0, valueMemSize=0, maxValue=-1";

                ff.isRecording = true;
                writer.truncate();
                ff.isRecording = false;

                TestUtils.assertEquals(
                        "cut x.k: " + emptyHeader + "\n"
                                + "cut x.v: " + emptyHeader + "\n",
                        ff.cuts
                );
                Assert.assertEquals(emptyHeader, readKeyFileHeader(ff, ff.keyFd));
                Assert.assertEquals(0, writer.getKeyCount());
                Assert.assertFalse(writer.getCursor(5).hasNext());
            }
            final LongList values = new LongList();
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(0, reader.getKeyCount());
            }
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                writer.add(5, 7);
                writer.add(5, 8);
                writer.setMaxValue(8);
            }
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(6, reader.getKeyCount());
                assertThat("[8,7]", reader.getCursor(5, 0, Long.MAX_VALUE), values);
                for (int key = 0; key < 5; key++) {
                    assertThat("[]", reader.getCursor(key, 0, Long.MAX_VALUE), values);
                }
            }
        });
    }

    @Test
    public void testValueBlockAllocationDiskFullKeepsHeaderCovered() throws Exception {
        // At the smallest value page the OS allows, with blocks of a sixteenth of a page of values (half a page and
        // 16 bytes), the next block after the column-top rollback of an empty index ends past the one page the writer
        // has mapped, so adding the first key has to grow the value file. When that growth fails, the add must fail
        // before it publishes the block.
        final long pageSize = Files.PAGE_SIZE;
        final int blockValueCount = (int) (pageSize / 16);
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE, pageSize);
        final ValueFileDiskFullFacade ff = new ValueFileDiskFullFacade();
        assertMemoryLeak(ff, () -> {
            Assert.assertEquals(pageSize, configuration.getDataIndexValueAppendPageSize());
            create(configuration, path.trimTo(plen), "x", blockValueCount);
            final long blockCapacity = blockValueCount * 8L + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
            final LongList values = new LongList();
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                writer.rollbackConditionally(10);
                Assert.assertEquals(blockCapacity, writer.getValueMemSize());
                ff.isDiskFull = true;
                try {
                    writer.add(0, 10);
                    Assert.fail("the add must fail when the value file cannot grow to cover the new block");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                }
                Assert.assertEquals(blockCapacity, writer.getValueMemSize());
                Assert.assertEquals(blockCapacity, readValueMemSizeHeader());
                Assert.assertTrue(valueFileLength() >= blockCapacity);
                try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    Assert.assertEquals(0, reader.getKeyCount());
                }
                // what a kill at this point leaves on disk
                writer.closeNoTruncate();
            }
            ff.isDiskFull = false;
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                // the failed add raised the max value; the retry rolls back to the same column top first
                writer.rollbackConditionally(10);
                writer.add(0, 10);
                writer.add(1, 11);
                writer.setMaxValue(11);
                Assert.assertTrue(valueFileLength() >= writer.getValueMemSize());
            }
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[10]", reader.getCursor(0, 0, Long.MAX_VALUE), values);
                assertThat("[11]", reader.getCursor(1, 0, Long.MAX_VALUE), values);
            }
        });
    }

    @Test
    public void testValueFileShortByMoreThanLastBlockStillFails() throws Exception {
        // A value file that does not reach the first cell of the last block lost a value a writer had written, so
        // it is a torn file, and every way of opening the index refuses it.
        TestUtils.assertMemoryLeak(() -> {
            final long valueMemSize = writeThreeKeysEndingWithSingleValueBlock();
            final long blockCapacity = 256 * 8 + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
            final FilesFacade ff = configuration.getFilesFacade();
            for (long length : new long[]{valueMemSize - blockCapacity + Long.BYTES - 1, valueMemSize - blockCapacity}) {
                truncateValueFile(length);
                assertBackwardReaderConstructorFail(configuration, "bitmap index value file too short");
                assertWriterConstructorFail("bitmap index value file too short");
                final long keyFd = TableUtils.openRW(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
                final long valueFd = TableUtils.openRW(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
                path.trimTo(plen);
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                    writer.of(configuration, keyFd, valueFd, false, 256);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "bitmap index value file too short");
                }
                Assert.assertEquals(length, valueFileLength());
            }
        });
    }

    @Test
    public void testValueFileShortByPartOfLastBlockOpens() throws Exception {
        // An earlier build could publish a block before the value file covered all of it: the file then ends past
        // the block's first cell, which the writer wrote first, but before the block's end. A writer stopped there
        // (a kill, or a close that does not truncate) leaves the file that way. It holds every written value, so
        // readers open it, and a writer extends it, through both ways of opening a writer. Windows cannot map a
        // read-only file past its end, so there a reader fails cleanly until a writer has extended the file, as it
        // did before the length checks.
        TestUtils.assertMemoryLeak(() -> {
            final long valueMemSize = writeThreeKeysEndingWithSingleValueBlock();
            final long blockCapacity = 256 * 8 + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
            final long shortLength = valueMemSize - blockCapacity + Long.BYTES;
            final FilesFacade ff = configuration.getFilesFacade();
            final LongList values = new LongList();

            truncateValueFile(shortLength);
            if (Os.isWindows()) {
                try (BitmapIndexBwdReader ignored = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    Assert.fail("Windows cannot map the value file past its end");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not mmap");
                }
            } else {
                try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                    assertThat("[2,0]", reader.getCursor(0, 0, Long.MAX_VALUE), values);
                    assertThat("[1]", reader.getCursor(1, 0, Long.MAX_VALUE), values);
                    assertThat("[3]", reader.getCursor(2, 0, Long.MAX_VALUE), values);
                }
            }

            final long keyFd = TableUtils.openRW(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
            final long valueFd = TableUtils.openRW(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
            path.trimTo(plen);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                writer.of(configuration, keyFd, valueFd, false, 256);
                Assert.assertEquals(valueMemSize, writer.getValueMemSize());
                Assert.assertTrue(valueFileLength() >= valueMemSize);
            }

            truncateValueFile(shortLength);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
                Assert.assertTrue(valueFileLength() >= valueMemSize);
                writer.add(2, 4);
                writer.add(3, 5);
                writer.setMaxValue(5);
            }
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[2,0]", reader.getCursor(0, 0, Long.MAX_VALUE), values);
                assertThat("[1]", reader.getCursor(1, 0, Long.MAX_VALUE), values);
                assertThat("[4,3]", reader.getCursor(2, 0, Long.MAX_VALUE), values);
                assertThat("[5]", reader.getCursor(3, 0, Long.MAX_VALUE), values);
            }
        });
    }

    @Test
    public void testWriterConstructorBadSequence() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            setupIndexHeader();
            assertWriterConstructorFail("Sequence mismatch");
        });
    }

    @Test
    public void testWriterConstructorBadSig() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MemoryA mem = openKey()) {
                mem.skip(BitmapIndexUtils.KEY_FILE_RESERVED);
            }
            assertWriterConstructorFail("Unknown format");
        });
    }

    @Test
    public void testWriterConstructorFileTooSmall() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            openKey().close();
            assertWriterConstructorFail("Index file too short");
        });
    }

    @Test
    public void testWriterConstructorIncorrectValueCount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MemoryA mem = openKey()) {
                mem.putByte(BitmapIndexUtils.SIGNATURE);
                mem.skip(9);
                mem.putLong(1000);
                mem.skip(4);
                mem.putLong(0);
                mem.skip(BitmapIndexUtils.KEY_FILE_RESERVED - mem.getAppendOffset());

            }
            // The map-size guard fires first (value file missing -> too short).
            assertWriterConstructorFail("bitmap index value file too short");
        });
    }

    @Test
    public void testWriterConstructorKeyMismatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (MemoryA mem = openKey()) {
                mem.putByte(BitmapIndexUtils.SIGNATURE);
                mem.skip(20);
                mem.putLong(300);
                mem.skip(BitmapIndexUtils.KEY_FILE_RESERVED - mem.getAppendOffset());
            }

            // Memory impl will round truncate size to the OS page size.
            // Therefore, truncate file manually to below the expected file size

            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat("x").put(".k");
                long fd = TableUtils.openFileRWOrFail(ff, path.$(), configuration.getWriterFileOpenOpts());
                try {
                    ff.truncate(fd, 64);
                } finally {
                    ff.close(fd);
                }
            }
            assertWriterConstructorFail("Key count");
        });
    }

    private void assertBackwardCursorLimit(BitmapIndexBwdReader reader, int min, int max, LongList tmp, int nExpectedNulls) {
        tmp.clear();
        try (RowCursor cursor = reader.getCursor(0, min, max)) {
            while (cursor.hasNext()) {
                tmp.add(cursor.next());
            }
        }

        int len = tmp.size() - nExpectedNulls;
        for (int i = 0, n = max - min; i < n; i++) {
            if ((i + min) % 3 == 0) {
                continue;
            }

            Assert.assertEquals(i, tmp.getQuick(--len));
            Assert.assertEquals(i, tmp.getQuick(--len));
            Assert.assertEquals(i, tmp.getQuick(--len));
        }

        for (int i = 0; i < nExpectedNulls; i++) {
            Assert.assertEquals(i, tmp.getQuick(tmp.size() - i - 1));
        }
    }

    private void assertBackwardReaderConstructorFail(CairoConfiguration configuration, CharSequence contains) {
        try {
            new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
            Assert.fail();
        } catch (CairoException e) {
            assertTrue(Chars.contains(e.getMessage(), contains));
        }
    }

    private void assertEmptyCursor(AbstractBitmapIndexReader reader) {
        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
        Assert.assertFalse(cursor.hasNext());
        Assert.assertEquals(0, cursor.next());
        Misc.free(cursor);
    }

    private void assertForwardCursorLimit(AbstractBitmapIndexReader reader, int min, int N, LongList tmp, int nExpectedResults, int nExpectedNulls) {
        assertTrue(reader instanceof BitmapIndexFwdReader || reader instanceof ConcurrentBitmapIndexFwdReader);
        tmp.clear();
        try (RowCursor cursor = reader.getCursor(0, min, N - 1)) {
            while (cursor.hasNext()) {
                tmp.add(cursor.next());
            }
        }

        Assert.assertEquals(nExpectedResults, tmp.size());

        for (int i = 0; i < nExpectedNulls; i++) {
            Assert.assertEquals(i, tmp.getQuick(i));
        }

        int len = nExpectedNulls;
        for (int i = 0, n = Math.min(N - min, nExpectedNulls); i < n; i++) {
            if ((i + min) % 3 == 0) {
                continue;
            }

            Assert.assertEquals(i, tmp.getQuick(len++));
            Assert.assertEquals(i, tmp.getQuick(len++));
            Assert.assertEquals(i, tmp.getQuick(len++));
        }
    }

    private void assertForwardReaderConstructorFail(CairoConfiguration configuration, CharSequence contains) {
        try {
            new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0);
            Assert.fail();
        } catch (CairoException e) {
            assertTrue(Chars.contains(e.getMessage(), contains));
        }
    }

    // A process killed inside truncate() after the key count drops and before the entries are zeroed leaves the old
    // entries above a zero key count, with the value file as it was. The writer open leaves them, and add() zeroes the
    // old entries a new key skips before the key count makes them keys, or those keys would carry the old values.
    private void assertNewKeyZeroesStaleKeysAfterTruncateKill(boolean isOpenByFd) throws Exception {
        assertMemoryLeak(() -> {
            writeStaleKeysAboveZeroKeyCount(64);
            // the entries of keys 0 to 99, one value each
            final byte[] oldEntries = readKeyFile(BitmapIndexUtils.KEY_FILE_RESERVED, 3_264);
            final ByteBuffer oldEntryBuffer = ByteBuffer.wrap(oldEntries).order(ByteOrder.nativeOrder());
            for (int key = 0; key < 100; key++) {
                Assert.assertEquals(1, oldEntryBuffer.getLong((int) (key * BitmapIndexUtils.KEY_ENTRY_SIZE + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT)));
            }

            final LongList values = new LongList();
            BitmapIndexWriter.testZeroedKeyBytes.set(0);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                openIndexWriter(writer, isOpenByFd);
                Assert.assertEquals(0, writer.getKeyCount());
                Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                Assert.assertArrayEquals(oldEntries, readKeyFile(BitmapIndexUtils.KEY_FILE_RESERVED, 3_264));

                // a new key past the old entries: every key below it becomes a key that holds nothing
                writer.add(50, 5);
                // [64, 1_664)
                Assert.assertEquals(1_600, BitmapIndexWriter.testZeroedKeyBytes.get());
                writer.add(10, 6);
                Assert.assertEquals(1_600, BitmapIndexWriter.testZeroedKeyBytes.get());
                Assert.assertEquals(51, writer.getKeyCount());
                writer.add(99, 7);
                // and [1_696, 3_232)
                Assert.assertEquals(3_136, BitmapIndexWriter.testZeroedKeyBytes.get());
                // [3_264, 4_864) lies above the old entries and holds zeros
                writer.add(150, 8);
                Assert.assertEquals(3_136, BitmapIndexWriter.testZeroedKeyBytes.get());
                writer.setMaxValue(8);
                Assert.assertEquals(151, writer.getKeyCount());
                assertNewKeysOverStaleKeys(writer::getCursor, values);
            }
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(151, reader.getKeyCount());
                assertNewKeysOverStaleKeys(key -> reader.getCursor(key, 0, Long.MAX_VALUE), values);
            }
        });
    }

    // The keys of an index of keyCount keys, each holding its key, after assertNewKeysWriteNoSkippedKeys()
    private void assertNewKeysOverCleanKeys(IntFunction<RowCursor> cursors, int keyCount, long keyFileLength, LongList values) {
        final int sparseKey = keyCount + 7;
        final int farKey = farKey(keyFileLength);
        for (int key = 0; key <= farKey; key++) {
            final String expected;
            if (key == 0) {
                expected = keyCount > 0 ? "[1000003,0]" : "[1000003,1000000]";
            } else if (key < keyCount) {
                expected = "[" + key + "]";
            } else if (key == keyCount) {
                expected = "[1000000]";
            } else if (key == sparseKey) {
                expected = "[1000001]";
            } else if (key == farKey) {
                expected = "[1000002]";
            } else {
                expected = "[]";
            }
            assertThat(expected, cursors.apply(key), values);
        }
    }

    // The keys testNegativeKeyCountKeepsHeader() adds over 100 old entries
    private void assertNewKeysOverNegativeKeyCount(IntFunction<RowCursor> cursors, LongList values) {
        assertThat("[5]", cursors.apply(0), values);
        for (int key = 1; key < 40; key++) {
            assertThat("[]", cursors.apply(key), values);
        }
        assertThat("[6]", cursors.apply(40), values);
    }

    // The keys testNewKeyZeroesOrphanEntryAboveKeyCount() adds over keys 0 to 9 and the orphan entry of key 20
    private void assertNewKeysOverOrphanEntry(IntFunction<RowCursor> cursors, boolean isOrphanKeyAddedAgain, LongList values) {
        for (int key = 0; key < 10; key++) {
            assertThat("[" + key + "]", cursors.apply(key), values);
        }
        for (int key = 10, keyCount = isOrphanKeyAddedAgain ? 21 : 31; key < keyCount; key++) {
            final String expected;
            if (isOrphanKeyAddedAgain) {
                expected = key == 20 ? "[3]" : "[]";
            } else {
                expected = key == 15 ? "[1]" : key == 30 ? "[2]" : "[]";
            }
            assertThat(expected, cursors.apply(key), values);
        }
    }

    // The keys assertNewKeyZeroesStaleKeysAfterTruncateKill() adds over 100 old entries
    private void assertNewKeysOverStaleKeys(IntFunction<RowCursor> cursors, LongList values) {
        for (int key = 0; key < 151; key++) {
            final String expected = switch (key) {
                case 10 -> "[6]";
                case 50 -> "[5]";
                case 99 -> "[7]";
                case 150 -> "[8]";
                default -> "[]";
            };
            assertThat(expected, cursors.apply(key), values);
        }
    }

    private void assertNewKeysReadBack(int keyCount, long keyFileLength, LongList values) {
        try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
            Assert.assertEquals(farKey(keyFileLength) + 1, reader.getKeyCount());
            assertNewKeysOverCleanKeys(key -> reader.getCursor(key, 0, Long.MAX_VALUE), keyCount, keyFileLength, values);
        }
    }

    // Adds a dense new key, a sparse one, one whose entry lies past a key file of keyFileLength bytes and a value of
    // key 0 to the writer of an index of keyCount keys, each holding its key, and checks that none of them zeroes
    // anything
    private void assertNewKeysWriteNoSkippedKeys(BitmapIndexWriter writer, int keyCount, long keyFileLength, LongList values) {
        final int farKey = farKey(keyFileLength);
        writer.add(keyCount, 1_000_000);
        writer.add(keyCount + 7, 1_000_001);
        writer.add(farKey, 1_000_002);
        writer.add(0, 1_000_003);
        writer.setMaxValue(1_000_003);
        Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
        Assert.assertEquals(farKey + 1, writer.getKeyCount());
        assertNewKeysOverCleanKeys(writer::getCursor, keyCount, keyFileLength, values);
    }

    private void assertSpinTimeout(Runnable read) {
        try {
            read.run();
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), AbstractBitmapIndexReader.INDEX_CORRUPT);
        }
    }

    private void assertThat(LongList expected, RowCursor cursor) {
        int i = 0;
        while (cursor.hasNext()) {
            long val = cursor.next();
            if (i < expected.size()) {
                Assert.assertEquals(expected.getQuick(i++), val);
            } else {
                Assert.fail("element should not be in index: " + val);
            }
        }
        Assert.assertEquals("missing values in index", i, expected.size());
    }

    private void assertThat(String expected, RowCursor cursor, LongList temp) {
        temp.clear();
        while (cursor.hasNext()) {
            temp.add(cursor.next());
        }
        Assert.assertEquals(expected, temp.toString());
        Misc.free(cursor);
    }

    private void assertValuesMatchBitmapWriter(int count, BitmapIndexWriter writer, LongList values) {
        values.sort();
        LongList keyValues = new LongList();
        int lastKey = 0;
        for (int i = 0; i < values.size(); i++) {
            long keyVal = values.get(i);
            int key = (int) (keyVal / count);
            if (lastKey != key) {
                keyValues.reverse();
                assertThat(keyValues, writer.getCursor(lastKey));
                keyValues.clear();
                lastKey = key;
            }
            keyValues.add(keyVal % count);

        }
        keyValues.reverse();
        assertThat(keyValues, writer.getCursor(lastKey));
    }

    private void assertWriterConstructorFail(CharSequence contains) {
        try {
            new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), contains);
        }
    }

    private void closeOffThread(RowCursor cursor) throws InterruptedException {
        final Thread t = new Thread(cursor::close);
        t.start();
        t.join();
    }

    // The key append page in units of the 16 KiB the tests set: larger where the OS page is
    private int keyAppendPageRatio() {
        final long keyAppendPageSize = configuration.getDataIndexKeyAppendPageSize();
        Assert.assertEquals(0, keyAppendPageSize % 16_384);
        return (int) (keyAppendPageSize / 16_384);
    }

    private long keyFileLength() {
        try {
            return configuration.getFilesFacade().length(BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE));
        } finally {
            path.trimTo(plen);
        }
    }

    // Opens the writer on the index files that exist: by path, or by the file descriptors the O3 jobs pass
    private void openIndexWriter(BitmapIndexWriter writer, boolean isOpenByFd) {
        if (isOpenByFd) {
            final FilesFacade ff = configuration.getFilesFacade();
            final long keyFd = TableUtils.openRW(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
            final long valueFd = TableUtils.openRW(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
            path.trimTo(plen);
            writer.of(configuration, keyFd, valueFd, false, 64);
        } else {
            writer.of(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE);
        }
    }

    private MemoryA openKey() {
        try (Path path = new Path()) {
            MemoryMA mem = Vm.getSmallCMARWInstance(
                    configuration.getFilesFacade(),
                    path.of(configuration.getDbRoot()).concat("x").put(".k").$(),
                    MemoryTag.MMAP_DEFAULT,
                    configuration.getWriterFileOpenOpts()
            );
            mem.toTop();
            return mem;
        }
    }

    // The bytes of the key file in [lo, hi), as a process that opens the file finds them
    private byte[] readKeyFile(long lo, long hi) {
        final int size = (int) (hi - lo);
        final byte[] bytes = new byte[size];
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = TableUtils.openRO(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG);
        path.trimTo(plen);
        final long buf = Unsafe.malloc(Math.max(size, 1), MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(size, ff.read(fd, buf, size, lo));
            for (int i = 0; i < size; i++) {
                bytes[i] = Unsafe.getByte(buf + i);
            }
            return bytes;
        } finally {
            Unsafe.free(buf, Math.max(size, 1), MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private long readValueMemSizeHeader() {
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = TableUtils.openRO(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG);
        try {
            return ff.readNonNegativeLong(fd, BitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
        } finally {
            ff.close(fd);
            path.trimTo(plen);
        }
    }

    private void removeIndexFiles() {
        final FilesFacade ff = configuration.getFilesFacade();
        ff.removeQuiet(BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE));
        ff.removeQuiet(BitmapIndexUtils.valueFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE));
        path.trimTo(plen);
    }

    private void setupIndexHeader() {
        try (MemoryA mem = openKey()) {
            mem.putByte(BitmapIndexUtils.SIGNATURE);
            mem.putLong(10); // sequence
            mem.putLong(0); // value mem size
            mem.putInt(64); // block length
            mem.putLong(0); // key count
            mem.putLong(9); // sequence check
            mem.skip(BitmapIndexUtils.KEY_FILE_RESERVED - mem.getAppendOffset());
        }
    }

    private void testConcurrentBackwardRW(int N, int maxKeys) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            IntList keys = new IntList();
            IntObjHashMap<LongList> lists = new IntObjHashMap<>();

            // populate model for both reader and writer
            for (int i = 0; i < N; i++) {
                int key = rnd.nextPositiveInt() % maxKeys;

                LongList list = lists.get(key);
                if (list == null) {
                    lists.put(key, list = new LongList());
                    keys.add(key);
                }
                list.add(i);
            }

            final int threadCount = 3;
            CountDownLatch stopLatch = new CountDownLatch(threadCount);
            CyclicBarrier startBarrier = new CyclicBarrier(threadCount);
            AtomicInteger errors = new AtomicInteger();

            // create empty index
            create(configuration, path.trimTo(plen), "x", 1024);

            new Thread(() -> {
                try {
                    startBarrier.await();
                    try (Path path = new Path().of(configuration.getDbRoot())) {
                        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                            int pass = 0;
                            while (true) {
                                boolean added = false;
                                for (int i = 0, n = keys.size(); i < n; i++) {
                                    int key = keys.getQuick(i);
                                    LongList values = lists.get(key);
                                    if (pass < values.size()) {
                                        writer.add(key, values.getQuick(pass));
                                        added = true;
                                    }
                                }
                                pass++;
                                if (!added) {
                                    break;
                                }
                            }
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    errors.incrementAndGet();
                } finally {
                    stopLatch.countDown();
                }
            }).start();

            class MyReader implements Runnable {
                @Override
                public void run() {
                    try {
                        startBarrier.await();
                        try (Path path = new Path().of(configuration.getDbRoot())) {
                            try (BitmapIndexBwdReader reader1 = new BitmapIndexBwdReader(configuration, path, "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                                LongList tmp = new LongList();
                                while (true) {
                                    boolean keepGoing = false;
                                    for (int i = keys.size() - 1; i > -1; i--) {
                                        int key = keys.getQuick(i);
                                        LongList values = lists.get(key);
                                        RowCursor cursor = reader1.getCursor(key, 0, Long.MAX_VALUE);

                                        tmp.clear();
                                        while (cursor.hasNext()) {
                                            tmp.add(cursor.next());
                                        }

                                        int sz = tmp.size();
                                        for (int k = 0; k < sz; k++) {
                                            Assert.assertEquals(values.getQuick(sz - k - 1), tmp.getQuick(k));
                                        }

                                        if (sz < values.size()) {
                                            keepGoing = true;
                                        }
                                        Misc.free(cursor);
                                    }

                                    if (!keepGoing) {
                                        break;
                                    }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        errors.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            }

            new Thread(new MyReader()).start();
            new Thread(new MyReader()).start();

            assertTrue(stopLatch.await(20000, TimeUnit.SECONDS));
            Assert.assertEquals(0, errors.get());
        });
    }

    private void testConcurrentForwardCursor(int N, int maxKeys) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            IntList keys = new IntList();
            IntObjHashMap<LongList> lists = new IntObjHashMap<>();

            // populate model for both reader and writer to be used for concurrent writers
            for (int i = 0; i < N; i++) {
                int key = rnd.nextPositiveInt() % maxKeys;

                LongList list = lists.get(key);
                if (list == null) {
                    lists.put(key, list = new LongList());
                    keys.add(key);
                }
                list.add(i);
            }

            final int threadCount = 3;
            CountDownLatch stopLatch = new CountDownLatch(threadCount);
            AtomicBoolean writerDone = new AtomicBoolean();
            CyclicBarrier startBarrier = new CyclicBarrier(threadCount);
            AtomicInteger errors = new AtomicInteger();

            // create empty index
            create(configuration, path.trimTo(plen), "x", 1024);

            // write a zero value per each key; note that it won't be in the model
            try (Path path = new Path().of(configuration.getDbRoot())) {
                try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0, n = keys.size(); i < n; i++) {
                        int key = keys.getQuick(i);
                        writer.add(key, 0);
                    }
                }
            }

            // init the reader
            final ConcurrentBitmapIndexFwdReader reader = new ConcurrentBitmapIndexFwdReader(
                    configuration,
                    path.of(configuration.getDbRoot()),
                    "x",
                    COLUMN_NAME_TXN_NONE,
                    -1,
                    0
            );

            new Thread(() -> {
                try {
                    startBarrier.await();
                    try (Path path = new Path().of(configuration.getDbRoot())) {
                        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                            int pass = 0;
                            while (true) {
                                boolean added = false;
                                for (int i = 0, n = keys.size(); i < n; i++) {
                                    int key = keys.getQuick(i);
                                    LongList values = lists.get(key);
                                    if (pass < values.size()) {
                                        writer.add(key, values.getQuick(pass));
                                        added = true;
                                    }
                                }
                                pass++;
                                if (!added) {
                                    break;
                                }
                            }
                        }
                    }
                    writerDone.set(true);
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    errors.incrementAndGet();
                } finally {
                    stopLatch.countDown();
                }
            }).start();

            class MyCursorReader implements Runnable {
                final ConcurrentBitmapIndexFwdReader reader;

                public MyCursorReader(ConcurrentBitmapIndexFwdReader reader) {
                    this.reader = reader;
                }

                @Override
                public void run() {
                    try {
                        startBarrier.await();
                        LongList tmp = new LongList();
                        RowCursor cursor = null;
                        while (!writerDone.get()) {
                            for (int j = keys.size() - 1; j > -1; j--) {
                                int key = keys.getQuick(j);
                                LongList values = lists.get(key);
                                cursor = reader.initCursor(cursor, key, 0, Long.MAX_VALUE);

                                tmp.clear();
                                while (cursor.hasNext()) {
                                    tmp.add(cursor.next());
                                }

                                int sz = tmp.size();
                                // We expect at least a single value per key.
                                assertTrue(sz > 0);
                                // The very first value must be zero.
                                Assert.assertEquals(0, tmp.getQuick(0));
                                for (int k = 1; k < sz; k++) {
                                    Assert.assertEquals(values.getQuick(k - 1), tmp.getQuick(k));
                                }
                            }
                        }
                    } catch (Throwable e) {
                        errors.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            }

            new Thread(new MyCursorReader(reader)).start();
            new Thread(new MyCursorReader(reader)).start();

            try {
                assertTrue(stopLatch.await(20000, TimeUnit.SECONDS));
                Assert.assertEquals(0, errors.get());
            } finally {
                Misc.free(reader);
            }
        });
    }

    private void testConcurrentForwardRW(int N, int maxKeys) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            IntList keys = new IntList();
            IntObjHashMap<LongList> lists = new IntObjHashMap<>();

            // populate model for both reader and writer
            for (int i = 0; i < N; i++) {
                int key = rnd.nextPositiveInt() % maxKeys;

                LongList list = lists.get(key);
                if (list == null) {
                    lists.put(key, list = new LongList());
                    keys.add(key);
                }
                list.add(i);
            }

            final int threadCount = 3;
            CountDownLatch stopLatch = new CountDownLatch(threadCount);
            CyclicBarrier startBarrier = new CyclicBarrier(threadCount);
            AtomicInteger errors = new AtomicInteger();

            // create empty index
            create(configuration, path.trimTo(plen), "x", 1024);

            new Thread(() -> {
                try {
                    startBarrier.await();
                    try (Path path = new Path().of(configuration.getDbRoot())) {
                        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE)) {
                            int pass = 0;
                            while (true) {
                                boolean added = false;
                                for (int i = 0, n = keys.size(); i < n; i++) {
                                    int key = keys.getQuick(i);
                                    LongList values = lists.get(key);
                                    if (pass < values.size()) {
                                        writer.add(key, values.getQuick(pass));
                                        added = true;
                                    }
                                }
                                pass++;
                                if (!added) {
                                    break;
                                }
                            }
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    errors.incrementAndGet();
                } finally {
                    stopLatch.countDown();
                }
            }).start();

            class MyReader implements Runnable {
                @Override
                public void run() {
                    try {
                        startBarrier.await();
                        try (Path path = new Path().of(configuration.getDbRoot())) {
                            try (BitmapIndexFwdReader reader1 = new BitmapIndexFwdReader(configuration, path, "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                                LongList tmp = new LongList();
                                while (true) {
                                    boolean keepGoing = false;
                                    for (int i = keys.size() - 1; i > -1; i--) {
                                        int key = keys.getQuick(i);
                                        LongList values = lists.get(key);
                                        RowCursor cursor = reader1.getCursor(key, 0, Long.MAX_VALUE);

                                        tmp.clear();
                                        while (cursor.hasNext()) {
                                            tmp.add(cursor.next());
                                        }

                                        int sz = tmp.size();
                                        for (int k = 0; k < sz; k++) {
                                            Assert.assertEquals(values.getQuick(k), tmp.getQuick(k));
                                        }

                                        if (sz < values.size()) {
                                            keepGoing = true;
                                        }
                                        Misc.free(cursor);
                                    }

                                    if (!keepGoing) {
                                        break;
                                    }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        errors.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            }

            new Thread(new MyReader()).start();
            new Thread(new MyReader()).start();

            assertTrue(stopLatch.await(20000, TimeUnit.SECONDS));
            Assert.assertEquals(0, errors.get());
        });
    }

    private void truncateValueFile(long length) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = TableUtils.openRW(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
        try {
            Assert.assertTrue(ff.truncate(fd, length));
        } finally {
            ff.close(fd);
            path.trimTo(plen);
        }
    }

    private long valueFileLength() {
        try {
            return configuration.getFilesFacade().length(BitmapIndexUtils.valueFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE));
        } finally {
            path.trimTo(plen);
        }
    }

    // An index of keyCount keys, each holding its key as its value, as a writer that closes cleanly leaves it
    private void writeCleanIndex(int keyCount) {
        create(configuration, path.trimTo(plen), "x", 64);
        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
            for (int key = 0; key < keyCount; key++) {
                writer.add(key, key);
            }
            writer.setMaxValue(keyCount - 1);
        }
    }

    private void writeKeyFile(long offset, byte[] bytes) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = TableUtils.openRW(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE), LOG, configuration.getWriterFileOpenOpts());
        path.trimTo(plen);
        final long buf = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(buf + i, bytes[i]);
            }
            Assert.assertEquals(bytes.length, ff.write(fd, buf, bytes.length, offset));
        } finally {
            Unsafe.free(buf, bytes.length, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private void writeKeyFileInt(long offset, int value) {
        final byte[] bytes = new byte[Integer.BYTES];
        ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()).putInt(0, value);
        writeKeyFile(offset, bytes);
    }

    // The index a process killed inside truncate() leaves after the key count drops and before the entries are
    // zeroed: 100 keys, each with the value 1_000 + key, the key file as long as the writer made it, and a zero key
    // count
    private void writeStaleKeysAboveZeroKeyCount(int blockValueCount) {
        create(configuration, path.trimTo(plen), "x", blockValueCount);
        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
            for (int key = 0; key < 100; key++) {
                writer.add(key, 1_000 + key);
            }
            writer.setMaxValue(1_099);
            // no close cuts the key file to the key count
            writer.closeNoTruncate();
        }
        writeKeyFileInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT, 0);
        final ByteBuffer header = ByteBuffer.wrap(readKeyFile(0, BitmapIndexUtils.KEY_FILE_RESERVED)).order(ByteOrder.nativeOrder());
        Assert.assertEquals(0, header.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT));
    }

    // Writes keys 0 and 1 into one block each, a second value of key 0 into its block, and key 2's single value
    // into the last block, the state a new key's block allocation leaves, and returns the value size.
    private long writeThreeKeysEndingWithSingleValueBlock() {
        create(configuration, path.trimTo(plen), "x", 256);
        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
            writer.add(0, 0);
            writer.add(1, 1);
            writer.add(0, 2);
            writer.add(2, 3);
            writer.setMaxValue(3);
            Assert.assertEquals(3 * (256 * 8 + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED), writer.getValueMemSize());
            return writer.getValueMemSize();
        }
    }

    private void writeTripleValues(int N) {
        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE)) {
            for (int i = 0; i < N; i++) {
                if (i % 3 == 0) {
                    continue;
                }
                writer.add(0, i);
                writer.add(0, i);
                writer.add(0, i);
            }
        }
    }

    // Records the key file header at each cut of x.k or x.v while recording, where a process kill can land, and the
    // whole key file at the cut of x.k.
    private static class KeyFileCutFacade extends TestFilesFacadeImpl {
        private final StringSink cuts = new StringSink();
        private boolean isRecording;
        private long keyFd = -1;
        private byte[] keyFileAtCut;
        private long valueFd = -1;

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.endsWithAscii(name, "x.k")) {
                keyFd = fd;
            } else if (Utf8s.endsWithAscii(name, "x.v")) {
                valueFd = fd;
            }
            return fd;
        }

        @Override
        public boolean truncate(long fd, long size) {
            if (isRecording && (fd == keyFd || fd == valueFd)) {
                cuts.put("cut ").put(fd == keyFd ? "x.k" : "x.v").put(": ").put(readKeyFileHeader(this, keyFd)).put('\n');
                if (fd == keyFd) {
                    keyFileAtCut = readWholeFile(fd);
                }
            }
            return super.truncate(fd, size);
        }

        private byte[] readWholeFile(long fd) {
            final int size = (int) length(fd);
            final byte[] bytes = new byte[size];
            final long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(size, read(fd, buf, size, 0));
                for (int i = 0; i < size; i++) {
                    bytes[i] = Unsafe.getByte(buf + i);
                }
                return bytes;
            } finally {
                Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static class KeyFileDiskFullFacade extends TestFilesFacadeImpl {
        private boolean isDiskFull;
        private long keyFd = -1;

        @Override
        public boolean allocate(long fd, long size) {
            return !(isDiskFull && fd == keyFd) && super.allocate(fd, size);
        }

        @Override
        public boolean allocate(long fd, long allocatedSize, long size) {
            return !(isDiskFull && fd == keyFd) && super.allocate(fd, allocatedSize, size);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.endsWithAscii(name, ".k")) {
                keyFd = fd;
            }
            return fd;
        }
    }

    private static class ValueFileDiskFullFacade extends TestFilesFacadeImpl {
        private boolean isDiskFull;
        private long valueFd = -1;

        @Override
        public boolean allocate(long fd, long size) {
            return !(isDiskFull && fd == valueFd) && super.allocate(fd, size);
        }

        @Override
        public boolean allocate(long fd, long allocatedSize, long size) {
            return !(isDiskFull && fd == valueFd) && super.allocate(fd, allocatedSize, size);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.endsWithAscii(name, ".v")) {
                valueFd = fd;
            }
            return fd;
        }
    }
}
