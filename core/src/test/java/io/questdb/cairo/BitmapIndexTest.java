/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.AppendOnlyVirtualMemory;
import io.questdb.cairo.vm.PagedMappedReadWriteMemory;
import io.questdb.cairo.vm.PagedSlidingReadOnlyMemory;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.griffin.engine.table.LatestByArguments;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BitmapIndexTest extends AbstractCairoTest {

    private Path path;
    private int plen;

    public static void create(CairoConfiguration configuration, Path path, CharSequence name, int valueBlockCapacity) {
        int plen = path.length();
        try {
            FilesFacade ff = configuration.getFilesFacade();
            try (AppendOnlyVirtualMemory mem = new AppendOnlyVirtualMemory(ff, BitmapIndexUtils.keyFileName(path, name), ff.getPageSize())) {
                BitmapIndexWriter.initKeyMemory(mem, Numbers.ceilPow2(valueBlockCapacity));
            }
            ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), name));
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    @Before
    public void setUp() {
        path = new Path().of(configuration.getRoot());
        plen = path.length();
        super.setUp();
    }

    @Override
    @After
    public void tearDown() {
        Misc.free(path);
        super.tearDown();
    }

    @Test
    public void testAdd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            LongList list = new LongList();
            create(configuration, path.trimTo(plen), "x", 4);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
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

            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0)) {
                assertThat("[5567,1234]", reader.getCursor(true, 256, 0, Long.MAX_VALUE), list);
                assertThat("[93,92,91,987,10]", reader.getCursor(true, 64, 0, Long.MAX_VALUE), list);
                assertThat("[1000]", reader.getCursor(true, 0, 0, Long.MAX_VALUE), list);
            }

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", 0)) {
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
            // assert that index is populated correctly
            create(configuration, path.trimTo(plen), "x", 128);
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
                for (int i = 0; i < N; i++) {
                    int key = i % maxKeys;
                    long value = rnd.nextLong();
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
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    LongList list = lists.get(keys.getQuick(i));
                    Assert.assertNotNull(list);
                    RowCursor cursor = reader.getCursor(true, keys.getQuick(i), Long.MIN_VALUE, Long.MAX_VALUE);
                    int z = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertTrue(z > -1);
                        Assert.assertEquals(list.getQuick(z - 1), cursor.next());
                        z--;
                    }

                    // makes sure entire list is processed
                    Assert.assertEquals(0, z);
                }
            }

            // read values and compare to the structure index is expected to be holding
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", 0)) {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    LongList list = lists.get(keys.getQuick(i));
                    Assert.assertNotNull(list);
                    RowCursor cursor = reader.getCursor(true, keys.getQuick(i), Long.MIN_VALUE, Long.MAX_VALUE);
                    int z = 0;
                    int sz = list.size();
                    while (cursor.hasNext()) {
                        Assert.assertTrue(z < sz);
                        Assert.assertEquals(list.getQuick(z), cursor.next());
                        z++;
                    }

                    // makes sure entire list is processed
                    Assert.assertEquals(sz, z);
                }
            }
        });
    }

    @Test
    public void testBackwardCursorTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 1024);

            try (BitmapIndexWriter w = new BitmapIndexWriter(configuration, path.trimTo(plen), "x")) {
                w.add(0, 10);
            }

            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0)) {

                try (PagedMappedReadWriteMemory mem = new PagedMappedReadWriteMemory()) {
                    try (Path path = new Path()) {
                        path.of(configuration.getRoot()).concat("x").put(".k").$();
                        mem.of(configuration.getFilesFacade(), path, configuration.getFilesFacade().getPageSize());
                    }

                    long offset = BitmapIndexUtils.getKeyEntryOffset(0);
                    mem.jumpTo(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);
                    mem.putLong(10);

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
        TestUtils.assertMemoryLeak(() -> {
            setupIndexHeader();
            assertBackwardReaderConstructorFail("could not consistently");
            assertForwardReaderConstructorFail("could not consistently");
        });
    }

    @Test
    public void testBackwardReaderConstructorBadSig() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AppendOnlyVirtualMemory mem = openKey()) {
                mem.skip(BitmapIndexUtils.KEY_FILE_RESERVED);
            }
            assertBackwardReaderConstructorFail("Unknown format");
            assertForwardReaderConstructorFail("Unknown format");
        });
    }

    @Test
    public void testBackwardReaderConstructorFileTooSmall() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            openKey().close();
            assertBackwardReaderConstructorFail("Index file too short");
            assertForwardReaderConstructorFail("Index file too short");
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
                public void munmap(long address, long size) {
                    super.munmap(address, size);
                    count++;
                }
            }

            final CountingFacade facade = new CountingFacade();

            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return facade;
                }
            };


            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x")) {
                try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0)) {
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
        create(configuration, path.trimTo(plen), "x", 1024);

        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
            writer.add(0, 1000);
        }

        try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0)) {

            // should have single value in cursor
            RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(1000, cursor.next());
            Assert.assertFalse(cursor.hasNext());

            try (Path path = new Path();
                 PagedMappedReadWriteMemory mem = new PagedMappedReadWriteMemory(
                         configuration.getFilesFacade(),
                         path.of(root).concat("x").put(".k").$(),
                         configuration.getFilesFacade().getPageSize())
            ) {
                // change sequence but not sequence check
                long seq = mem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                mem.jumpTo(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                mem.putLong(22);

                try {
                    reader.getCursor(true, 10, 0, Long.MAX_VALUE);
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                // make sure index fails until sequence is not up to date

                try {
                    reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                mem.jumpTo(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                mem.putLong(seq);

                // test that index recovers
                cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1000, cursor.next());
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    @Test
    public void testConcurrentWriterAndBackwardReadBreadth() throws Exception {
        testConcurrentBackwardRW(10000000, 1024);
    }

    @Test
    public void testConcurrentWriterAndBackwardReadHeight() throws Exception {
        testConcurrentBackwardRW(1000000, 100000);
    }

    @Test
    public void testConcurrentWriterAndForwardReadBreadth() throws Exception {
        testConcurrentForwardRW(10000000, 1024);
    }

    @Test
    public void testConcurrentWriterAndForwardReadHeight() throws Exception {
        testConcurrentForwardRW(1000000, 100000);
    }

    @Test
    public void testEmptyCursor() {
        RowCursor cursor = new EmptyRowCursor();
        Assert.assertFalse(cursor.hasNext());
        Assert.assertEquals(0, cursor.next());
    }

    @Test
    public void testForwardCursorTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 1024);

            try (BitmapIndexWriter w = new BitmapIndexWriter(configuration, path.trimTo(plen), "x")) {
                w.add(0, 10);
            }

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", 0)) {

                try (PagedMappedReadWriteMemory mem = new PagedMappedReadWriteMemory()) {
                    try (Path path = new Path()) {
                        path.of(configuration.getRoot()).concat("x").put(".k").$();
                        mem.of(configuration.getFilesFacade(), path, configuration.getFilesFacade().getPageSize());
                    }

                    long offset = BitmapIndexUtils.getKeyEntryOffset(0);
                    mem.jumpTo(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);
                    mem.putLong(10);

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
                public void munmap(long address, long size) {
                    super.munmap(address, size);
                    count++;
                }
            }

            final CountingFacade facade = new CountingFacade();

            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return facade;
                }
            };


            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x")) {
                try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", 0)) {
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
        create(configuration, path.trimTo(plen), "x", 1024);

        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
            writer.add(0, 1000);
        }

        try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", 0)) {

            // should have single value in cursor
            RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(1000, cursor.next());
            Assert.assertFalse(cursor.hasNext());

            try (Path path = new Path();
                 PagedMappedReadWriteMemory mem = new PagedMappedReadWriteMemory(
                         configuration.getFilesFacade(),
                         path.of(root).concat("x").put(".k").$(),
                         configuration.getFilesFacade().getPageSize())
            ) {
                // change sequence but not sequence check
                long seq = mem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                mem.jumpTo(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                mem.putLong(22);

                try {
                    reader.getCursor(true, 10, 0, Long.MAX_VALUE);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                // make sure index fails until sequence is not up to date

                try {
                    reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "could not consistently"));
                }

                mem.jumpTo(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                mem.putLong(seq);

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
                new BitmapIndexWriter(configuration, path, "x");
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "does not exist"));
            }
        });
    }

    @Test
    public void testIntIndex() throws Exception {
        final int plen = path.length();
        final Rnd rnd = new Rnd();
        int N = 100000000;
        final int MOD = 1024;
        TestUtils.assertMemoryLeak(() -> {
            try (AppendOnlyVirtualMemory mem = new AppendOnlyVirtualMemory()) {

                mem.of(configuration.getFilesFacade(), path.concat("x.dat").$(), configuration.getFilesFacade().getMapPageSize());

                for (int i = 0; i < N; i++) {
                    mem.putInt(rnd.nextPositiveInt() & (MOD - 1));
                }

                try (PagedSlidingReadOnlyMemory rwin = new PagedSlidingReadOnlyMemory()) {
                    rwin.of(mem);

                    create(configuration, path.trimTo(plen), "x", N / MOD / 128);
                    try (BitmapIndexWriter writer = new BitmapIndexWriter()) {
                        writer.of(configuration, path.trimTo(plen), "x");
                        indexInts(rwin, writer, N);
                    }
                }
            }
        });
    }

    @Test
    public void testLimitBackwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x")) {
                for (int i = 0; i < 265; i++) {
                    if (i % 3 == 0) {
                        continue;
                    }
                    writer.add(0, i);
                    writer.add(0, i);
                    writer.add(0, i);
                }
            }

            LongList tmp = new LongList();
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0)) {
                assertBackwardCursorLimit(reader, 260L, tmp);
                assertBackwardCursorLimit(reader, 16L, tmp);
                assertBackwardCursorLimit(reader, 9L, tmp);
                Assert.assertFalse(reader.getCursor(true, 0, -1L, -1L).hasNext());
            }
        });
    }

    @Test
    public void testLimitForwardCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(configuration, path.trimTo(plen), "x", 128);

            int N = 265;
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x")) {
                for (int i = 0; i < N; i++) {
                    if (i % 3 == 0) {
                        continue;
                    }
                    writer.add(0, i);
                    writer.add(0, i);
                    writer.add(0, i);
                }
            }

            LongList tmp = new LongList();
            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", 0)) {
                assertForwardCursorLimit(reader, 260, N, tmp, 9);
                assertForwardCursorLimit(reader, 260, N - 2, tmp, 6);
                assertForwardCursorLimit(reader, 16, N, tmp, 498);
                assertForwardCursorLimit(reader, 9, N, tmp, 510);
                Assert.assertFalse(reader.getCursor(true, 0, 266, Long.MAX_VALUE).hasNext());
                Assert.assertFalse(reader.getCursor(true, 0, Long.MAX_VALUE, Long.MAX_VALUE).hasNext());
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

            // this is an assertion in case somebody change the test
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
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x")) {
                for (int i = 0; i < N; i++) {
                    writer.add(rnd.nextPositiveInt() % maxKeys, i);
                }
                writer.rollbackValues(CUTOFF);
            }

            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0)) {
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

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", 0)) {
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

            // add more data to model
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
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path.trimTo(plen), "x")) {
                for (int i = 0; i < N; i++) {
                    writer.add(rnd.nextPositiveInt() % maxKeys, i + N);
                }
            }

            // assert against model again
            try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0)) {
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

            try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", 0)) {
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
        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
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

            // now add middle key first
            writer.add(900, 8000);
            Assert.assertEquals(901, writer.getKeyCount());

            try (BitmapIndexReader reader = new BitmapIndexBwdReader(configuration, path, "x", 0)) {
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
            try (AppendOnlyVirtualMemory mem = openKey()) {
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
            try (AppendOnlyVirtualMemory mem = openKey()) {
                mem.putByte(BitmapIndexUtils.SIGNATURE);
                mem.skip(9);
                mem.putLong(1000);
                mem.skip(4);
                mem.putLong(0);
                mem.skip(BitmapIndexUtils.KEY_FILE_RESERVED - mem.getAppendOffset());

            }
            assertWriterConstructorFail("Incorrect file size");
        });
    }

    @Test
    public void testWriterConstructorKeyMismatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AppendOnlyVirtualMemory mem = openKey()) {
                mem.putByte(BitmapIndexUtils.SIGNATURE);
                mem.skip(20);
                mem.putLong(300);
                mem.skip(BitmapIndexUtils.KEY_FILE_RESERVED - mem.getAppendOffset());
            }
            assertWriterConstructorFail("Key count");
        });
    }

    @Test
    public void testCppLatestByIndexReader() {
        final int valueBlockCapacity = 256;
        final long keyCount = 5;
        create(configuration, path.trimTo(plen), "x", valueBlockCapacity);

        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
            for (int i = 0; i < keyCount; i++) {
                for (int j = 0; j <= i; j++) {
                    writer.add(i, j);
                }
            }
        }

        DirectLongList rows = new DirectLongList(keyCount);

        rows.extend(keyCount);
        rows.setPos(rows.getCapacity());
        GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

        try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0)) {
            long argsAddress = LatestByArguments.allocateMemory();
            LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
            LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());

            LatestByArguments.setKeyLo(argsAddress, 0);
            LatestByArguments.setKeyHi(argsAddress, keyCount);
            LatestByArguments.setRowsSize(argsAddress, 0);

            BitmapIndexUtilsNative.latestScanBackward(
                    reader.getKeyBaseAddress(),
                    reader.getKeyMemorySize(),
                    reader.getValueBaseAddress(),
                    reader.getValueMemorySize(),
                    argsAddress,
                    reader.getUnIndexedNullCount(),
                    Long.MAX_VALUE, 0,
                    0, valueBlockCapacity - 1
            );

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
        rows.close();
    }

    @Test
    public void testCppLatestByIndexReaderIgnoreUpdates() {
        final int valueBlockCapacity = 32;
        final long keyCount = 1024;

        create(configuration, path.trimTo(plen), "x", valueBlockCapacity);

        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
            for (int i = 0; i < keyCount; i++) {
                for (int j = 0; j <= i; j++) {
                    writer.add(i, j);
                }
            }
        }

        DirectLongList rows = new DirectLongList(keyCount);

        rows.extend(keyCount);
        rows.setPos(rows.getCapacity());
        GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

        //fixing memory mapping here
        BitmapIndexBwdReader reader = new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0);

        // we should ignore this update
        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
            for (int i = (int)keyCount; i < keyCount*2; i++) {
                writer.add(i, 2L*i);
            }
        }
        // and this one
        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
            for (int i = 0; i < keyCount; i++) {
                writer.add((int)keyCount - 1, 10L*i);
            }
        }


        long argsAddress = LatestByArguments.allocateMemory();
        LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
        LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());

        LatestByArguments.setKeyLo(argsAddress, 0);
        LatestByArguments.setKeyHi(argsAddress, keyCount);
        LatestByArguments.setRowsSize(argsAddress, 0);

        BitmapIndexUtilsNative.latestScanBackward(
                reader.getKeyBaseAddress(),
                reader.getKeyMemorySize(),
                reader.getValueBaseAddress(),
                reader.getValueMemorySize(),
                argsAddress,
                reader.getUnIndexedNullCount(),
                Long.MAX_VALUE, 0,
                0, valueBlockCapacity - 1
        );

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
        rows.close();
        reader.close();
    }

    @Test
    public void testCppLatestByIndexReaderIndexedWithTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int timestampIncrement = 1000000 * 60 * 5;
            final int M = 1000;
            final int N = 100;
            final int indexBlockCapacity = 32;
            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, indexBlockCapacity).
                    col("i", ColumnType.INT).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final String[] symbols = new String[N];

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = new TableWriter(configuration, "x")) {
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
                DirectLongList rows = new DirectLongList(N);

                rows.setPos(rows.getCapacity());
                GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

                try (TableReader tableReader = new TableReader(configuration, "x")) {
                    tableReader.openPartition(0);
                    final int columnBase = tableReader.getColumnBase(0);
                    final int columnIndex = tableReader.getMetadata().getColumnIndex("c");
                    BitmapIndexReader reader = tableReader.getBitmapIndexReader(0,
                            columnBase, columnIndex, BitmapIndexReader.DIR_BACKWARD);

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
                            Long.MAX_VALUE, 0,
                            0, indexBlockCapacity - 1
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
                rows.close();
            }
        });
    }

    private static void indexInts(PagedSlidingReadOnlyMemory srcMem, BitmapIndexWriter writer, long hi) {
        srcMem.updateSize();
        for (long r = 0L; r < hi; r++) {
            final long offset = r * 4;
            writer.add(srcMem.getInt(offset), offset);
        }
    }

    private void assertBackwardCursorLimit(BitmapIndexBwdReader reader, long max, LongList tmp) {
        tmp.clear();
        RowCursor cursor = reader.getCursor(true, 0, 0, max);
        while (cursor.hasNext()) {
            tmp.add(cursor.next());
        }

        int len = tmp.size();
        for (int i = 0; i < max; i++) {
            if (i % 3 == 0) {
                continue;
            }

            Assert.assertEquals(i, tmp.getQuick(--len));
            Assert.assertEquals(i, tmp.getQuick(--len));
            Assert.assertEquals(i, tmp.getQuick(--len));
        }
    }

    private void assertBackwardReaderConstructorFail(CharSequence contains) {
        try {
            new BitmapIndexBwdReader(configuration, path.trimTo(plen), "x", 0);
            Assert.fail();
        } catch (CairoException e) {
            Assert.assertTrue(Chars.contains(e.getMessage(), contains));
        }
    }

    private void assertForwardCursorLimit(BitmapIndexFwdReader reader, int min, int N, LongList tmp, int nExpectedResults) {
        tmp.clear();
        RowCursor cursor = reader.getCursor(true, 0, min, N - 1);
        while (cursor.hasNext()) {
            tmp.add(cursor.next());
        }

        Assert.assertEquals(nExpectedResults, tmp.size());
        int len = 0;
        for (int i = min; i < N; i++) {
            if (i % 3 == 0) {
                continue;
            }

            Assert.assertEquals(i, tmp.getQuick(len++));
            Assert.assertEquals(i, tmp.getQuick(len++));
            Assert.assertEquals(i, tmp.getQuick(len++));
        }
    }

    private void assertForwardReaderConstructorFail(CharSequence contains) {
        try {
            new BitmapIndexFwdReader(configuration, path.trimTo(plen), "x", 0);
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

    private void assertWriterConstructorFail(CharSequence contains) {
        try {
            new BitmapIndexWriter(configuration, path.trimTo(plen), "x");
            Assert.fail();
        } catch (CairoException e) {
            Assert.assertTrue(Chars.contains(e.getMessage(), contains));
        }
    }

    private AppendOnlyVirtualMemory openKey() {
        try (Path path = new Path()) {
            return new AppendOnlyVirtualMemory(configuration.getFilesFacade(), path.of(configuration.getRoot()).concat("x").put(".k").$(), configuration.getFilesFacade().getPageSize());
        }
    }

    private void setupIndexHeader() {
        try (AppendOnlyVirtualMemory mem = openKey()) {
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
                    try (Path path = new Path().of(configuration.getRoot())) {
                        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
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
                    e.printStackTrace();
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
                        try (Path path = new Path().of(configuration.getRoot())) {
                            try (BitmapIndexBwdReader reader1 = new BitmapIndexBwdReader(configuration, path, "x", 0)) {
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
                        e.printStackTrace();
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
                    try (Path path = new Path().of(configuration.getRoot())) {
                        try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration, path, "x")) {
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
                    e.printStackTrace();
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
                        try (Path path = new Path().of(configuration.getRoot())) {
                            try (BitmapIndexFwdReader reader1 = new BitmapIndexFwdReader(configuration, path, "x", 0)) {
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
                        e.printStackTrace();
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

}