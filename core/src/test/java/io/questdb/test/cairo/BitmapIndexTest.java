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

import io.questdb.cairo.idx.AbstractIndexReader;
import io.questdb.cairo.idx.BitmapIndexBwdReader;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexReader;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.ConcurrentBitmapIndexFwdReader;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
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
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

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
                assertThat("[5567,1234]", reader.getCursor(true, 256, 0, Long.MAX_VALUE), list);
                assertThat("[93,92,91,987,10]", reader.getCursor(true, 64, 0, Long.MAX_VALUE), list);
                assertThat("[1000]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                assertThat("[1234,5567]", reader.getCursor(true, 256, 0, Long.MAX_VALUE), list);
                assertThat("[10,987,91,92,93]", reader.getCursor(true, 64, 0, Long.MAX_VALUE), list);
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
                    RowCursor cursor = reader.getCursor(true, keys.getQuick(i), 0, Long.MAX_VALUE);
                    int z = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertTrue(z > -1);
                        Assert.assertEquals(list.getQuick(z - 1), cursor.next());
                        z--;
                    }

                    // makes sure the entire list is processed
                    Assert.assertEquals(0, z);
                }
            }

            // read values and compare to the structure index is expected to be holding
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
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

                    // makes sure the entire list is processed
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

                        RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                        boolean found = false;
                        while (cursor.hasNext()) {
                            if (value == cursor.next()) {
                                found = true;
                                break;
                            }
                        }
                        Assert.assertTrue(found);
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
            RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
            Assert.assertTrue(cursor.hasNext());
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
                    reader.getCursor(true, 10, 0, Long.MAX_VALUE);
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                // make sure the index fails until the sequence is not up to date

                try {
                    reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                mem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);

                // test that index recovers
                cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1000, cursor.next());
                Assert.assertFalse(cursor.hasNext());
            }
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
                    RowCursor cursor = reader.getCursor(true, i, Long.MIN_VALUE, Long.MAX_VALUE);
                    while (cursor.hasNext()) {
                        sum += cursor.next();
                    }
                }

                Assert.assertEquals(expectedSum, sum);
            }

            // test branch new reader
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                long sum = 0;
                for (int i = 0; i < K; i++) {
                    RowCursor cursor = reader.getCursor(true, i, Long.MIN_VALUE, Long.MAX_VALUE);
                    while (cursor.hasNext()) {
                        sum += cursor.next();
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
    public void testConcurrentWriterAndBackwardReadBreadth() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testConcurrentBackwardRW(rnd.nextInt(10000000), 1024);
    }

    @Test
    public void testConcurrentWriterAndBackwardReadHeight() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testConcurrentBackwardRW(rnd.nextInt(1000000), 100000);
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
                        true,
                        indexBlockCapacity,
                        false
                );
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
                        BitmapIndexReader reader = tableReader.getBitmapIndexReader(
                                0,
                                columnIndex,
                                BitmapIndexReader.DIR_BACKWARD
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

                        RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                        boolean found = false;
                        while (cursor.hasNext()) {
                            if (value == cursor.next()) {
                                found = true;
                                break;
                            }
                        }
                        Assert.assertTrue(found);
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
            RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
            Assert.assertTrue(cursor.hasNext());
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
                    reader.getCursor(true, 10, 0, Long.MAX_VALUE);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                // make sure the index fails until the sequence is not up to date

                try {
                    reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                mem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);

                // test that index recovers
                cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1000, cursor.next());
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    @Test
    public void testIndexDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new BitmapIndexWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE);
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "does not exist"));
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
            Assert.assertTrue(len > 0);
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
                assertBackwardCursorLimit(reader, 0, 260, tmp, 0, false);
                assertBackwardCursorLimit(reader, 0, 260, tmp, 0, true);
                assertBackwardCursorLimit(reader, 0, 16, tmp, 0, true);
                assertBackwardCursorLimit(reader, 0, 9, tmp, 0, true);
                Assert.assertFalse(reader.getCursor(true, 0, -1L, -1L).hasNext());
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
                assertBackwardCursorLimit(reader, 1, 260, tmp, nullsN - 1, true);
                assertBackwardCursorLimit(reader, 1, 260, tmp, nullsN - 1, false);
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
                assertForwardCursorLimit(reader, 260, N, tmp, 9, 0, false);
                assertForwardCursorLimit(reader, 260, N, tmp, 9, 0, true);
                assertForwardCursorLimit(reader, 260, N - 2, tmp, 6, 0, true);
                assertForwardCursorLimit(reader, 16, N, tmp, 498, 0, true);
                assertForwardCursorLimit(reader, 9, N, tmp, 510, 0, true);
                Assert.assertFalse(reader.getCursor(true, 0, 266, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(reader.getCursor(true, 0, Long.MAX_VALUE, Long.MAX_VALUE).hasNext());

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
                Assert.assertTrue("Value file should grow in size", newMemSize > initialMemSize);

                // The reader should stop at the last known valueMem size boundary now.
                assertForwardCursorLimit(reader, 0, N, tmp, 528, 0, true);
                assertForwardCursorLimit(reader, 0, N + newKeysN, tmp, 639, 0, true);
                Assert.assertFalse(reader.getCursor(true, 0, N + newKeysN, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(reader.getCursor(true, 0, Long.MAX_VALUE, Long.MAX_VALUE).hasNext());
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
                assertForwardCursorLimit(reader, 1, N, tmp, 530, nullsN - 1, true);
                assertForwardCursorLimit(reader, 1, N, tmp, 530, nullsN - 1, false);
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
                assertForwardCursorLimit(reader, 260, N, tmp, 9, 0, false);
                assertForwardCursorLimit(reader, 260, N, tmp, 9, 0, true);
                assertForwardCursorLimit(reader, 260, N - 2, tmp, 6, 0, true);
                assertForwardCursorLimit(reader, 16, N, tmp, 498, 0, true);
                assertForwardCursorLimit(reader, 9, N, tmp, 510, 0, true);
                Assert.assertFalse(reader.getCursor(true, 0, 266, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(reader.getCursor(true, 0, Long.MAX_VALUE, Long.MAX_VALUE).hasNext());
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
                assertForwardCursorLimit(reader, 1, N, tmp, 530, nullsN - 1, true);
                assertForwardCursorLimit(reader, 1, N, tmp, 530, nullsN - 1, false);
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
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(--v), cursor.next());
                    }
                    Assert.assertEquals(0, v);
                }
            }

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(v++), cursor.next());
                    }
                    Assert.assertEquals(list.size(), v);
                }
            }

            try (ConcurrentBitmapIndexFwdReader reader = new ConcurrentBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(v++), cursor.next());
                    }
                    Assert.assertEquals(list.size(), v);
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
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(--v), cursor.next());
                    }
                    Assert.assertEquals(0, v);
                }
            }

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(v++), cursor.next());
                    }
                    Assert.assertEquals(list.size(), v);
                }
            }

            try (ConcurrentBitmapIndexFwdReader reader = new ConcurrentBitmapIndexFwdReader(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    int key = keys.getQuick(i);
                    // do not limit reader, we have to read everything index has
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    LongList list = lists.get(key);

                    int v = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(list.getQuick(v++), cursor.next());
                    }
                    Assert.assertEquals(list.size(), v);
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
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1000, cursor.next());
                Assert.assertFalse(cursor.hasNext());
            }

            writer.truncate();
            Assert.assertEquals(0, writer.getKeyCount());

            // now add the middle key first
            writer.add(900, 8000);
            Assert.assertEquals(901, writer.getKeyCount());

            try (BitmapIndexReader reader = new BitmapIndexBwdReader(configuration, path, "x", COLUMN_NAME_TXN_NONE, -1, 0)) {
                Assert.assertEquals(901, reader.getKeyCount());
                RowCursor cursor = reader.getCursor(true, 900, 0, 1_000_000);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(8000, cursor.next());
                Assert.assertFalse(cursor.hasNext());

                // assert that key below 900 do no have values
                for (int i = 0; i < 900; i++) {
                    Assert.assertFalse(reader.getCursor(true, i, 0, 1_000_000).hasNext());
                }
            }

        }

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
            assertWriterConstructorFail("corrupt file");
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

    private void assertBackwardCursorLimit(BitmapIndexBwdReader reader, int min, int max, LongList tmp, int nExpectedNulls, boolean cached) {
        tmp.clear();
        RowCursor cursor = reader.getCursor(cached, 0, min, max);
        while (cursor.hasNext()) {
            tmp.add(cursor.next());
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
            Assert.assertTrue(Chars.contains(e.getMessage(), contains));
        }
    }

    private void assertEmptyCursor(AbstractIndexReader reader) {
        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
        Assert.assertFalse(cursor.hasNext());
        Assert.assertEquals(0, cursor.next());
    }

    private void assertForwardCursorLimit(AbstractIndexReader reader, int min, int N, LongList tmp, int nExpectedResults, int nExpectedNulls, boolean cached) {
        Assert.assertTrue(reader instanceof BitmapIndexFwdReader || reader instanceof ConcurrentBitmapIndexFwdReader);
        tmp.clear();
        RowCursor cursor = reader.getCursor(cached, 0, min, N - 1);
        while (cursor.hasNext()) {
            tmp.add(cursor.next());
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
            Assert.assertTrue(Chars.contains(e.getMessage(), contains));
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
                                        RowCursor cursor = reader1.getCursor(true, key, 0, Long.MAX_VALUE);

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

            Assert.assertTrue(stopLatch.await(20000, TimeUnit.SECONDS));
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
                                Assert.assertTrue(sz > 0);
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
                Assert.assertTrue(stopLatch.await(20000, TimeUnit.SECONDS));
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
                                        RowCursor cursor = reader1.getCursor(true, key, 0, Long.MAX_VALUE);

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

            Assert.assertTrue(stopLatch.await(20000, TimeUnit.SECONDS));
            Assert.assertEquals(0, errors.get());
        });
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
}
