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

import com.sun.management.ThreadMXBean;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapCachePool;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.SymbolMapSegments;
import io.questdb.cairo.SymbolMapUtil;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.SymbolValueCountCollector;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewSymbolCache;
import io.questdb.cairo.lv.LiveViewSymbolTable;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.functions.bool.StaticSymbolKeys;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NativeMemoryBudget;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.SymbolMapWriter.keyToOffset;
import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.TableUtils.offsetFileName;

public class SymbolMapTest extends AbstractCairoTest {
    private final static SymbolValueCountCollector NOOP_COLLECTOR = (_, _) -> {
    };

    public static void create(Path path, CharSequence name, int symbolCapacity, boolean useCache) {
        int plen = path.size();
        try {
            try (
                    MemoryCMARW mem = Vm.getSmallCMARWInstance(
                            configuration.getFilesFacade(),
                            path.concat(name).put(".o").$(),
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                mem.putInt(symbolCapacity);
                mem.putBool(useCache);
                mem.jumpTo(SymbolMapWriter.HEADER_SIZE);
            }

            configuration.getFilesFacade().touch(path.trimTo(plen).concat(name).put(".c").$());
            BitmapIndexTest.create(configuration, path.trimTo(plen), name, 4);
        } finally {
            path.trimTo(plen);
        }
    }

    @Test
    public void testSharedCacheConcurrentFirstReadTakesOneReference() throws Exception {
        assertMemoryLeak(() -> {
            final int threadCount = 4;
            final int[] keys = {0, 1, 4097};
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 8192, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
                    for (int key = 0; key < 4098; key++) {
                        writer.put("s" + key);
                    }
                }
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
                try (SymbolMapCachePool pool = new SymbolMapCachePool(budget)) {
                    for (int round = 0; round < 200; round++) {
                        try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 4098, pool, 0)) {
                            ObjList<SymbolTable> views = new ObjList<>();
                            for (int t = 0; t < threadCount; t++) {
                                views.add(reader.newSymbolTableView());
                            }
                            TestUtils.runConcurrently(threadCount, t -> {
                                SymbolTable view = views.getQuick(t);
                                for (int key : keys) {
                                    TestUtils.assertEquals("s" + key, view.valueOf(key));
                                }
                            });
                            Assert.assertEquals(1, pool.getEntryCount());
                        }
                        Assert.assertEquals(0, pool.getEntryCount());
                        Assert.assertEquals(0, budget.getUsedBytes());
                    }
                }
            }
        });
    }

    @Test
    public void testSharedCacheBudgetStopKeepsLoadedValuesAndLogsOnce() throws Exception {
        assertMemoryLeak(() -> {
            final int keys = SymbolMapSegments.SEGMENT_KEYS;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "v", 3 * keys, -1, 0);
                createMap(path, "y", "w", keys, -1, 0);
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
                LogCapture logs = new LogCapture();
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(budget);
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 3 * keys, pool, 0)
                ) {
                    logs.start();
                    DirectString mapped = new DirectString();
                    TestUtils.assertEquals("v0", reader.valueOf(0));
                    final long usedBytes = budget.getUsedBytes();
                    budget.setLimit(usedBytes);
                    for (int i = 0; i < 3; i++) {
                        CharSequence value = reader.valueOf(keys);
                        TestUtils.assertEquals("v" + keys, value);
                        Assert.assertEquals(address(reader.valueOf(keys, mapped)), address(value));
                    }
                    CharSequence cached = reader.valueOf(1);
                    TestUtils.assertEquals("v1", cached);
                    Assert.assertNotEquals(address(reader.valueOf(1, mapped)), address(cached));
                    Assert.assertEquals(keys, reader.getCacheSize());
                    Assert.assertEquals(usedBytes, budget.getUsedBytes());
                    // Room for the refused segment admits again.
                    budget.setLimit(1024 * 1024);
                    CharSequence resumed = reader.valueOf(2 * keys);
                    TestUtils.assertEquals("v" + (2 * keys), resumed);
                    Assert.assertNotEquals(address(reader.valueOf(2 * keys, mapped)), address(resumed));
                    Assert.assertEquals(2 * keys, reader.getCacheSize());
                    Assert.assertEquals(reader.getCacheAllocatedBytes(), budget.getUsedBytes());

                    budget.setLimit(budget.getUsedBytes());
                    TestUtils.assertEquals("v" + keys, reader.valueOf(keys));
                    Assert.assertEquals(2 * keys, reader.getCacheSize());
                    logs.drain();
                    logs.assertOnlyOnce("symbol cache stopped admitting new values.*column=x, reason=budget");

                    reader.of(configuration, path, "y", COLUMN_NAME_TXN_NONE, keys, pool, 0);
                    Assert.assertEquals(0, budget.getUsedBytes());
                    TestUtils.assertEquals("w0", reader.valueOf(0));
                    Assert.assertEquals(keys, reader.getCacheSize());
                } finally {
                    logs.stop();
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testSharedCacheConcurrentLoads() throws Exception {
        assertMemoryLeak(() -> {
            final int count = 5000;
            final long seed = System.nanoTime();
            LOG.info().$("testSharedCacheConcurrentLoads [seed=").$(seed).I$();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "s", count, -1, 0);
                NativeMemoryBudget budget = new NativeMemoryBudget(16 * 1024 * 1024);
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(budget);
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, count, pool, 0)
                ) {
                    ObjList<SymbolTable> views = new ObjList<>();
                    for (int t = 0; t < 8; t++) {
                        views.add(reader.newSymbolTableView());
                    }
                    TestUtils.runConcurrently(8, t -> {
                        Rnd rnd = new Rnd(seed, t);
                        SymbolTable view = views.getQuick(t);
                        for (int i = 0; i < 20_000; i++) {
                            int key = rnd.nextInt(count);
                            TestUtils.assertEquals("s" + key, view.valueOf(key));
                        }
                    });
                    for (int key = 0; key < count; key++) {
                        TestUtils.assertEquals("s" + key, reader.valueOf(key));
                    }
                    Assert.assertEquals(count, reader.getCacheSize());
                    Assert.assertEquals(reader.getCacheAllocatedBytes(), budget.getUsedBytes());
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testSharedCacheGrowingDictionaryChargesAboutItsSize() throws Exception {
        assertMemoryLeak(() -> {
            final int count = 10 * SymbolMapSegments.SEGMENT_KEYS;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "value", count, -1, 0);
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
                try (
                        SymbolMapReaderImpl source = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, count);
                        SymbolMapSegments loadedAtOnce = new SymbolMapSegments(budget);
                        SymbolMapSegments loadedAsAdded = new SymbolMapSegments(budget)
                ) {
                    MemoryR offsets = source.getSymbolOffsetsColumn();
                    MemoryR chars = source.getSymbolValuesColumn();
                    for (int key = 0; key < count; key += SymbolMapSegments.SEGMENT_KEYS) {
                        Assert.assertEquals(SymbolMapSegments.LOADED, loadedAtOnce.load(key, count, offsets, chars));
                    }
                    // A reader of the newest row loads each key as the writer adds it.
                    for (int key = 0; key < count; key++) {
                        Assert.assertEquals(SymbolMapSegments.LOADED, loadedAsAdded.load(key, key + 1, offsets, chars));
                    }
                    DirectString view = new DirectString();
                    for (int key = 0; key < count; key++) {
                        TestUtils.assertEquals(source.valueOf(key), loadedAsAdded.get(key, view));
                    }
                    // Allowed extra: the eighth of slack per segment, the first segment's first block and the
                    // directories it outgrew.
                    final long atOnce = loadedAtOnce.allocatedBytes();
                    final long asAdded = loadedAsAdded.allocatedBytes();
                    Assert.assertTrue(asAdded + " bytes loaded as added, " + atOnce + " at once", asAdded < atOnce * 1.2);
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testSharedCacheLoadsWholeSegmentOnFirstRead() throws Exception {
        assertMemoryLeak(() -> {
            final int keys = SymbolMapSegments.SEGMENT_KEYS;
            final int count = 2 * keys + 10;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "v", count, -1, 0);
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(budget);
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, count, pool, 0);
                        SymbolMapReaderImpl other = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, count, pool, 0)
                ) {
                    DirectString mapped = new DirectString();
                    TestUtils.assertEquals("v" + (keys + 1), reader.valueOf(keys + 1));
                    Assert.assertEquals(keys, reader.getCacheSize());
                    Assert.assertEquals(reader.getCacheAllocatedBytes(), budget.getUsedBytes());
                    final long allocatedBytes = reader.getCacheAllocatedBytes();
                    long previous = 0;
                    for (int key = keys; key < 2 * keys; key++) {
                        CharSequence value = reader.valueOf(key);
                        TestUtils.assertEquals("v" + key, value);
                        Assert.assertNotEquals(address(reader.valueOf(key, mapped)), address(value));
                        if (previous != 0) {
                            Assert.assertEquals(previous + 4 + 2L * ("v" + (key - 1)).length(), address(value));
                        }
                        previous = address(value);
                    }
                    Assert.assertEquals(keys, reader.getCacheSize());
                    Assert.assertEquals(allocatedBytes, reader.getCacheAllocatedBytes());
                    Assert.assertEquals(address(reader.valueOf(keys)), address(other.valueOf(keys)));
                    Assert.assertEquals(keys, other.getCacheSize());

                    TestUtils.assertEquals("v" + (count - 1), other.valueOf(count - 1));
                    Assert.assertEquals(keys + 10, reader.getCacheSize());
                    TestUtils.assertEquals("v0", reader.valueOf(0));
                    Assert.assertEquals(count, reader.getCacheSize());
                    Assert.assertEquals(reader.getCacheAllocatedBytes(), budget.getUsedBytes());
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testSharedCacheOomStopsAdmissionAndLogsOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "s", 8, -1, 0);
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
                LogCapture logs = new LogCapture();
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(budget);
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 8, pool, 0)
                ) {
                    long limit = Unsafe.getRssMemLimit();
                    logs.start();
                    try {
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                        for (int key = 0; key < 8; key++) {
                            TestUtils.assertEquals("s" + key, reader.valueOf(key));
                        }
                    } finally {
                        Unsafe.setRssMemLimit(limit);
                    }
                    // the stop outlives the memory shortage, even once the budget gains room
                    Assert.assertTrue(budget.tryReserve(1));
                    budget.release(1);
                    for (int key = 0; key < 8; key++) {
                        TestUtils.assertEquals("s" + key, reader.valueBOf(key));
                    }
                    Assert.assertEquals(0, reader.getCacheSize());
                    Assert.assertEquals(0, reader.getCacheAllocatedBytes());
                    Assert.assertEquals(0, budget.getUsedBytes());
                    logs.drain();
                    logs.assertOnlyOnce("symbol cache stopped admitting new values.*column=x, reason=oom");
                } finally {
                    logs.stop();
                }
            }
        });
    }

    @Test
    public void testSharedCachePartialSegmentGrowth() throws Exception {
        assertMemoryLeak(() -> {
            final int keys = SymbolMapSegments.SEGMENT_KEYS;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "v", keys + 1, 100, 4096);
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(budget);
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 3, pool, 0);
                        SymbolMapReaderImpl older = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 3, pool, 0)
                ) {
                    StaticSymbolTable view = reader.newSymbolTableView();
                    CharSequence retained = view.valueOf(0);
                    TestUtils.assertEquals("v0", retained);
                    Assert.assertEquals(3, reader.getCacheSize());
                    final long inPlaceBytes = reader.getCacheAllocatedBytes();
                    final long inPlaceAddress = address(retained);

                    reader.updateSymbolCount(6);
                    TestUtils.assertEquals("v5", reader.valueOf(5));
                    Assert.assertEquals(6, reader.getCacheSize());
                    Assert.assertEquals(inPlaceBytes, reader.getCacheAllocatedBytes());
                    Assert.assertEquals(inPlaceAddress, address(reader.valueOf(0)));
                    Assert.assertNull(older.valueOf(5));
                    Assert.assertEquals(inPlaceAddress, address(older.valueOf(0)));

                    reader.updateSymbolCount(keys);
                    String longValue = "v100" + "x".repeat(4096);
                    TestUtils.assertEquals(longValue, reader.valueOf(100));
                    Assert.assertEquals(keys, reader.getCacheSize());
                    Assert.assertTrue(reader.getCacheAllocatedBytes() > inPlaceBytes);
                    Assert.assertNotEquals(inPlaceAddress, address(reader.valueOf(0)));
                    TestUtils.assertEquals("v0", retained);
                    TestUtils.assertEquals("v0", reader.valueOf(0));
                    TestUtils.assertEquals("v5", reader.valueOf(5));
                    Assert.assertEquals(reader.getCacheAllocatedBytes(), budget.getUsedBytes());

                    reader.updateSymbolCount(keys + 1);
                    TestUtils.assertEquals("v" + keys, reader.valueOf(keys));
                    Assert.assertEquals(keys + 1, reader.getCacheSize());
                    TestUtils.assertEquals("v0", reader.valueOf(0));
                    Assert.assertEquals(reader.getCacheAllocatedBytes(), budget.getUsedBytes());
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testSharedCachePublishesAfterCopy() throws Exception {
        assertMemoryLeak(() -> {
            final int keys = SymbolMapSegments.SEGMENT_KEYS;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "v", keys, 100, 4096);
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
                try (SymbolMapReaderImpl source = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, keys)) {
                    MemoryR offsets = source.getSymbolOffsetsColumn();
                    MemoryR chars = source.getSymbolValuesColumn();
                    DirectString view = new DirectString();
                    int[] loadingKey = {0};
                    int[] symbolCount = {3};
                    long[] publishedAddress = {0};
                    int[] copies = {0};
                    try (SymbolMapSegments segments = new SymbolMapSegments(budget) {
                        @Override
                        protected void publish(long slotAddress, long slot) {
                            copies[0]++;
                            Assert.assertNull(get(loadingKey[0], view));
                            if (publishedAddress[0] != 0) {
                                Assert.assertEquals(publishedAddress[0], address(get(0, view)));
                            }
                            Assert.assertEquals(NOT_LOADED, load(loadingKey[0], keys, offsets, chars));
                            super.publish(slotAddress, slot);
                            for (int k = 0; k < symbolCount[0]; k++) {
                                TestUtils.assertEquals(source.valueOf(k), get(k, view));
                            }
                        }
                    }) {
                        Assert.assertEquals(SymbolMapSegments.LOADED, segments.load(0, symbolCount[0], offsets, chars));
                        Assert.assertEquals(1, copies[0]);
                        publishedAddress[0] = address(segments.get(0, view));
                        loadingKey[0] = 5;
                        symbolCount[0] = 6;
                        Assert.assertEquals(SymbolMapSegments.LOADED, segments.load(5, symbolCount[0], offsets, chars));
                        Assert.assertEquals(2, copies[0]);
                        Assert.assertEquals(publishedAddress[0], address(segments.get(0, view)));
                        TestUtils.assertEquals("v5", segments.get(5, view));
                        loadingKey[0] = 200;
                        symbolCount[0] = keys;
                        Assert.assertEquals(SymbolMapSegments.LOADED, segments.load(200, symbolCount[0], offsets, chars));
                        Assert.assertEquals(3, copies[0]);
                        Assert.assertNotEquals(publishedAddress[0], address(segments.get(0, view)));
                        TestUtils.assertEquals("v200", segments.get(200, view));
                        Assert.assertEquals(SymbolMapSegments.LOADED, segments.load(200, keys, offsets, chars));
                        Assert.assertEquals(3, copies[0]);
                    }
                    Assert.assertEquals(0, budget.getUsedBytes());
                }
            }
        });
    }

    @Test
    public void testSharedCacheResumesWhenBudgetHasRoom() throws Exception {
        assertMemoryLeak(() -> {
            final int keys = SymbolMapSegments.SEGMENT_KEYS;
            final int count = 3 * keys;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "v", count, -1, 0);
                int[] reservations = {0};
                boolean[] isRoomTakenAfterCheck = {false};
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024) {
                    @Override
                    public boolean hasRoomFor(long bytes) {
                        if (isRoomTakenAfterCheck[0]) {
                            isRoomTakenAfterCheck[0] = false;
                            return true;
                        }
                        return super.hasRoomFor(bytes);
                    }

                    @Override
                    public boolean tryReserve(long bytes) {
                        reservations[0]++;
                        return super.tryReserve(bytes);
                    }
                };
                try (SymbolMapReaderImpl source = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, count)) {
                    MemoryR offsets = source.getSymbolOffsetsColumn();
                    MemoryR chars = source.getSymbolValuesColumn();
                    DirectString view = new DirectString();
                    SymbolMapSegments segments = new SymbolMapSegments(budget);
                    try {
                        Assert.assertEquals(SymbolMapSegments.LOADED, segments.load(0, count, offsets, chars));
                        budget.setLimit(budget.getUsedBytes());
                        Assert.assertEquals(SymbolMapSegments.STOPPED, segments.load(keys, count, offsets, chars));
                        Assert.assertEquals(SymbolMapSegments.STOP_BUDGET, segments.getStopReason());

                        // One byte of room is less than the refused segment, so the cache does not try again.
                        budget.setLimit(budget.getUsedBytes() + 1);
                        final int attempts = reservations[0];
                        Assert.assertEquals(SymbolMapSegments.NOT_LOADED, segments.load(keys, count, offsets, chars));
                        Assert.assertEquals(attempts, reservations[0]);

                        isRoomTakenAfterCheck[0] = true;
                        Assert.assertEquals(SymbolMapSegments.NOT_LOADED, segments.load(keys, count, offsets, chars));
                        Assert.assertEquals(attempts, reservations[0]);
                        Assert.assertNull(segments.get(keys, view));

                        budget.setLimit(1024 * 1024);
                        Assert.assertEquals(SymbolMapSegments.LOADED, segments.load(keys, count, offsets, chars));
                        TestUtils.assertEquals(source.valueOf(keys), segments.get(keys, view));
                        Assert.assertEquals(2 * keys, segments.size());

                        // Only the first stop reports STOPPED.
                        budget.setLimit(budget.getUsedBytes());
                        Assert.assertEquals(SymbolMapSegments.NOT_LOADED, segments.load(2 * keys, count, offsets, chars));
                        Assert.assertNull(segments.get(2 * keys, view));
                        Assert.assertEquals(segments.allocatedBytes(), budget.getUsedBytes());
                    } finally {
                        segments.close();
                    }
                    budget.setLimit(1024 * 1024);
                    Assert.assertEquals(SymbolMapSegments.NOT_LOADED, segments.load(0, count, offsets, chars));
                    Assert.assertEquals(0, segments.allocatedBytes());
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testSharedCacheSegmentBeyondLimitStops() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "v", 2, 1, 4096);
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
                try (
                        SymbolMapReaderImpl source = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 2);
                        SymbolMapSegments segments = new SymbolMapSegments(budget, 1024)
                ) {
                    MemoryR offsets = source.getSymbolOffsetsColumn();
                    MemoryR chars = source.getSymbolValuesColumn();
                    DirectString view = new DirectString();
                    Assert.assertEquals(SymbolMapSegments.STOPPED, segments.load(0, 2, offsets, chars));
                    Assert.assertEquals(SymbolMapSegments.STOP_BUDGET, segments.getStopReason());
                    budget.setLimit(2 * 1024 * 1024);
                    Assert.assertEquals(SymbolMapSegments.NOT_LOADED, segments.load(0, 2, offsets, chars));
                    Assert.assertNull(segments.get(0, view));
                    Assert.assertEquals(0, segments.size());
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testSharedCacheValueFormats() throws Exception {
        assertMemoryLeak(() -> {
            final int keys = SymbolMapSegments.SEGMENT_KEYS;
            ObjList<String> values = new ObjList<>("", "界", "😀", "a".repeat(30), "b".repeat(31));
            for (int key = values.size(); key < keys + 2; key++) {
                values.add(key == keys - 1 || key == keys ? "c".repeat(65_536) + key : "k" + key);
            }
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 2 * keys, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, writer.put(null));
                    for (int key = 0; key < values.size(); key++) {
                        Assert.assertEquals(key, writer.put(values.getQuick(key)));
                    }
                }
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, values.size(), pool, 0)
                ) {
                    ObjList<StaticSymbolTable> views = new ObjList<>(reader, reader.newSymbolTableView());
                    for (int v = 0; v < views.size(); v++) {
                        StaticSymbolTable view = views.getQuick(v);
                        Assert.assertNull(view.valueOf(SymbolTable.VALUE_IS_NULL));
                        Assert.assertNull(view.valueOf(values.size()));
                        for (int key = 0; key < values.size(); key++) {
                            int next = (key + 1) % values.size();
                            CharSequence a = view.valueOf(key);
                            CharSequence b = view.valueBOf(next);
                            TestUtils.assertEquals(values.getQuick(key), a);
                            TestUtils.assertEquals(values.getQuick(next), b);
                            Assert.assertEquals(values.getQuick(key).hashCode(), Chars.hashCode(a));
                            Assert.assertEquals(key, view.keyOf(a));
                            TestUtils.assertEquals(values.getQuick(key), a);
                            TestUtils.assertEquals(values.getQuick(next), b);
                        }
                    }
                    Assert.assertEquals(values.size(), reader.getCacheSize());
                }
            }
        });
    }

    @Test
    public void testSharedCacheRetainedOnlyByReadersThatRead() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 8, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
                    writer.put("aaaa");
                    writer.put("bbbb");
                }
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                        SymbolMapReaderImpl unread = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 2, pool, 0)
                ) {
                    Assert.assertEquals(0, pool.getEntryCount());
                    Assert.assertEquals(1, unread.keyOf("bbbb"));
                    TestUtils.assertEquals("aaaa", unread.valueOf(0, new DirectString()));
                    Assert.assertNull(unread.valueOf(SymbolTable.VALUE_IS_NULL));
                    Assert.assertNull(unread.valueOf(2));
                    Assert.assertEquals(0, pool.getEntryCount());

                    try (SymbolMapReaderImpl read = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 2, pool, 0)) {
                        TestUtils.assertEquals("aaaa", read.valueOf(0));
                        Assert.assertEquals(1, pool.getEntryCount());
                    }
                    Assert.assertEquals(0, pool.getEntryCount());

                    TestUtils.assertEquals("bbbb", unread.valueBOf(1));
                    Assert.assertEquals(1, pool.getEntryCount());
                    Assert.assertEquals(2, unread.getCacheSize());

                    unread.of(configuration, path, "x", COLUMN_NAME_TXN_NONE, 2, pool, 0);
                    Assert.assertEquals(0, pool.getEntryCount());
                    TestUtils.assertEquals("aaaa", unread.newSymbolTableView().valueOf(0));
                    Assert.assertEquals(1, pool.getEntryCount());
                }
            }
        });
    }

    @Test
    public void testSymbolTableViewsKeepValueHashes() throws Exception {
        assertMemoryLeak(() -> {
            // an empty value hashes to 0
            ObjList<String> values = new ObjList<>("a".repeat(30), "b".repeat(31), "");
            ObjList<String> others = new ObjList<>("x", "y", "z");
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 8, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
                    for (int i = 0; i < values.size(); i++) {
                        writer.put(values.getQuick(i));
                    }
                }
                create(path, "y", 8, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "y", COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
                    for (int i = 0; i < others.size(); i++) {
                        writer.put(others.getQuick(i));
                    }
                }
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, values.size(), pool, 0)
                ) {
                    for (int key = 0; key < values.size(); key++) {
                        final int expected = values.getQuick(key).hashCode();
                        Assert.assertFalse(reader.hasValueHash(key));
                        for (int read = 0; read < 2; read++) {
                            CharSequence value = reader.valueOf(key);
                            Assert.assertTrue(DirectString.isRawView(value));
                            Assert.assertEquals(expected, Chars.hashCode(value));
                            Assert.assertTrue(reader.hasValueHash(key));
                        }
                        Assert.assertEquals(expected, Chars.hashCode(reader.valueBOf(key)));
                    }
                    Assert.assertEquals(values.size(), reader.getCacheSize());

                    // Both flyweights read the stored hash instead of hashing the chars.
                    reader.putValueHash(1, 42);
                    CharSequence a = reader.valueOf(1);
                    Assert.assertEquals(42, Chars.hashCode(a));
                    Assert.assertEquals(42, Chars.hashCode(reader.valueBOf(1)));
                    // A lookup that finds no value leaves the earlier binding and its key alone.
                    Assert.assertNull(reader.valueOf(values.size()));
                    Assert.assertEquals(42, Chars.hashCode(a));
                    // Rebinding moves the key with the value.
                    Assert.assertEquals(values.getQuick(0).hashCode(), Chars.hashCode(reader.valueOf(0)));
                    Assert.assertEquals(values.getQuick(0).hashCode(), Chars.hashCode(a));
                    // The translator's lookups bind no key.
                    StaticSymbolTable view = reader.newSymbolTableView();
                    Assert.assertEquals(values.getQuick(0).hashCode(), Chars.hashCode(view.valueOf(0)));
                    Assert.assertEquals(values.getQuick(1).hashCode(), Chars.hashCode(view.valueOfOnce(1)));

                    // Reopening the reader on another dictionary drops the stored hashes.
                    reader.of(configuration, path, "y", COLUMN_NAME_TXN_NONE, others.size(), pool, 0);
                    for (int key = 0; key < others.size(); key++) {
                        Assert.assertFalse(reader.hasValueHash(key));
                        Assert.assertEquals(others.getQuick(key).hashCode(), Chars.hashCode(reader.valueOf(key)));
                    }
                }
            }
        });
    }

    @Test
    public void testSharedCacheConcurrentStopAndResume() throws Exception {
        assertMemoryLeak(() -> {
            final int count = 16 * SymbolMapSegments.SEGMENT_KEYS;
            final long roomyLimit = 1024 * 1024;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "v", count, -1, 0);
                // The first allocation is refused, so at least one stop happens whatever the threads do.
                NativeMemoryBudget budget = new NativeMemoryBudget(1);
                LogCapture logs = new LogCapture();
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(budget);
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, count, pool, 0)
                ) {
                    logs.start();
                    TestUtils.runConcurrently(8, worker -> {
                        SymbolTable view = reader.newSymbolTableView();
                        for (int pass = 0; pass < 10; pass++) {
                            for (int key = 0; key < count; key++) {
                                TestUtils.assertEquals("v" + key, view.valueOf(key));
                            }
                            if (worker == 0) {
                                budget.setLimit(pass % 2 == 0 ? roomyLimit : budget.getUsedBytes());
                            }
                        }
                    });
                    budget.setLimit(roomyLimit);
                    for (int key = 0; key < count; key++) {
                        TestUtils.assertEquals("v" + key, reader.valueOf(key));
                    }
                    Assert.assertEquals(count, reader.getCacheSize());
                    Assert.assertEquals(reader.getCacheAllocatedBytes(), budget.getUsedBytes());
                    logs.drain();
                    logs.assertOnlyOnce("symbol cache stopped admitting new values.*column=x, reason=budget");
                } finally {
                    logs.stop();
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testSharedCacheConcurrentStopLogsOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 8192, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
                    for (int key = 0; key < 5000; key++) {
                        writer.put("s" + key);
                    }
                }
                LogCapture logs = new LogCapture();
                try {
                    for (boolean isOom : new boolean[]{false, true}) {
                        try (
                                SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(isOom ? 1024 * 1024 : 1));
                                SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 5000, pool, 0)
                        ) {
                            logs.start();
                            long limit = Unsafe.getRssMemLimit();
                            try {
                                if (isOom) {
                                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                                }
                                TestUtils.runConcurrently(8, worker -> {
                                    SymbolTable view = reader.newSymbolTableView();
                                    for (int key = 4096; key < 5000; key++) {
                                        TestUtils.assertEquals("s" + key, view.valueOf(key));
                                    }
                                });
                            } finally {
                                Unsafe.setRssMemLimit(limit);
                            }
                            TestUtils.assertEquals("s4999", reader.valueOf(4999));
                            TestUtils.assertEquals("s0", reader.valueOf(0));
                            Assert.assertEquals(0, reader.getCacheSize());
                            logs.drain();
                            logs.assertOnlyOnce("symbol cache stopped admitting new values.*column=x, reason=" + (isOom ? "oom" : "budget"));
                        }
                    }
                } finally {
                    logs.stop();
                }
            }
        });
    }

    @Test
    public void testSharedCachePoolAcquireRacesShutdown() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
            try (Path path = new Path().of("table"); SymbolMapCachePool pool = new SymbolMapCachePool(budget)) {
                CountDownLatch acquiring = new CountDownLatch(1);
                CountDownLatch closed = new CountDownLatch(1);
                CharSequence column = new AbstractCharSequence() {
                    @Override
                    public char charAt(int index) {
                        acquiring.countDown();
                        try {
                            Assert.assertTrue(closed.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                        return 's';
                    }

                    @Override
                    public int length() {
                        return 1;
                    }
                };
                TestUtils.runConcurrently(2, worker -> {
                    if (worker == 0) {
                        try (SymbolMapCachePool.Entry entry = pool.acquire(path, column, -1, 0)) {
                            Assert.assertNull(entry);
                        }
                    } else {
                        try {
                            Assert.assertTrue(acquiring.await(10, TimeUnit.SECONDS));
                            pool.close();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        } finally {
                            closed.countDown();
                        }
                    }
                });
                Assert.assertEquals(0, pool.getEntryCount());
                Assert.assertEquals(0, budget.getUsedBytes());
                Assert.assertNull(pool.acquire(path, "s", -1, 0));
            }
        });
    }

    @Test
    public void testSharedCachePoolAcquireRacesLastRelease() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
            try (
                    Path dbPath = new Path().of(configuration.getDbRoot());
                    SymbolMapCachePool pool = new SymbolMapCachePool(budget);
                    Path path = new Path().of("table")
            ) {
                createMap(dbPath, "x", "shared", 1, -1, 0);
                final SymbolMapReaderImpl source = new SymbolMapReaderImpl(configuration, dbPath, "x", COLUMN_NAME_TXN_NONE, 1);
                TestUtils.runConcurrently(4, worker -> {
                    DirectString view = new DirectString();
                    for (int i = 0; i < 5000; i++) {
                        try (SymbolMapCachePool.Entry entry = pool.acquire(path, "sym", -1, 0)) {
                            Assert.assertNotNull(entry);
                            entry.getSegments().load(0, 1, source.getSymbolOffsetsColumn(), source.getSymbolValuesColumn());
                            CharSequence value = entry.getSegments().get(0, view);
                            if (value != null) {
                                TestUtils.assertEquals("shared0", value);
                            }
                            try (SymbolMapCachePool.Entry same = pool.acquire(path, "sym", -1, 0)) {
                                Assert.assertSame(entry, same);
                            }
                        }
                    }
                });
                source.close();
                Assert.assertEquals(0, pool.getEntryCount());
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testSharedCachePoolCloseKeepsReferencedValues() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024);
            try (
                    Path dbPath = new Path().of(configuration.getDbRoot());
                    SymbolMapCachePool pool = new SymbolMapCachePool(budget);
                    Path path = new Path().of("table");
                    SymbolMapCachePool.Entry first = pool.acquire(path, "sym", -1, 0);
                    SymbolMapCachePool.Entry second = pool.acquire(path, "sym", -1, 0)
            ) {
                Assert.assertSame(first, second);
                createMap(dbPath, "x", "retained", 1, -1, 0);
                try (SymbolMapReaderImpl source = new SymbolMapReaderImpl(configuration, dbPath, "x", COLUMN_NAME_TXN_NONE, 1)) {
                    Assert.assertEquals(
                            SymbolMapSegments.LOADED,
                            first.getSegments().load(0, 1, source.getSymbolOffsetsColumn(), source.getSymbolValuesColumn())
                    );
                }
                DirectString view = new DirectString();
                first.getSegments().get(0, view);
                pool.close();
                pool.clear();
                Assert.assertEquals(0, pool.getEntryCount());
                Assert.assertNull(pool.acquire(path, "sym", -1, 0));
                Assert.assertTrue(budget.getUsedBytes() > 0);
                TestUtils.assertEquals("retained0", view);
                TestUtils.assertEquals("retained0", second.getSegments().get(0, view));
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testSharedCachePoolVersionKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                    Path path = new Path().of("table");
                    Path otherPath = new Path().of("table:other");
                    SymbolMapCachePool.Entry first = pool.acquire(path, "other:sym", -1, 0);
                    SymbolMapCachePool.Entry other = pool.acquire(otherPath, "sym", -1, 0);
                    SymbolMapCachePool.Entry renamed = pool.acquire(path, "other:sym", 1, 0);
                    SymbolMapCachePool.Entry truncated = pool.acquire(path, "other:sym", -1, 1)
            ) {
                Assert.assertEquals(4, pool.getEntryCount());
                Assert.assertNotSame(first, other);
                Assert.assertNotSame(first, renamed);
                Assert.assertNotSame(first, truncated);
            }
        });
    }

    @Test
    public void testSharedCacheDictionaryScan() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 8192, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
                    for (int i = 0; i < 5000; i++) {
                        Assert.assertEquals(i, writer.put("symbol" + i));
                    }
                }
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 5000, pool, 0);
                        SymbolMapReaderImpl older = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 4999, pool, 0)
                ) {
                    Assert.assertEquals(0, reader.getCacheSize());
                    ObjList<StaticSymbolTable> views = new ObjList<>(reader, reader.newSymbolTableView(), older, older.newSymbolTableView());
                    for (int i = 0; i < views.size(); i++) {
                        StaticSymbolTable view = views.getQuick(i);
                        for (int key = 0; key < view.getSymbolCount(); key++) {
                            TestUtils.assertEquals("symbol" + key, view.valueOf(key));
                        }
                        Assert.assertNull(view.valueOf(-1));
                        Assert.assertNull(view.valueOf(view.getSymbolCount()));
                        // the first scan loads every segment, the later ones only read them
                        Assert.assertEquals(5000, reader.getCacheSize());
                        Assert.assertEquals(
                                ((DirectString) reader.valueOf(4096)).lo(),
                                ((DirectString) view.valueOf(4096)).lo()
                        );
                    }
                    Assert.assertTrue(reader.getCacheAllocatedBytes() > 0);
                    long cachedAddress = ((DirectString) reader.valueBOf(4999)).lo();
                    Assert.assertEquals(cachedAddress, ((DirectString) reader.valueOf(4999)).lo());
                    Assert.assertNotEquals(cachedAddress, ((DirectString) reader.valueOf(4999, new DirectString())).lo());
                    StaticSymbolTable worker = reader.newSymbolTableView();
                    CharSequence held = worker.valueOf(4096);
                    reader.valueOf(4097);
                    TestUtils.assertEquals("symbol4096", held);
                    Assert.assertEquals(5000, reader.getCacheSize());
                }
            }
        });
    }

    @Test
    public void testSharedCacheReaderViewsVisibilityAndSparseKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 128, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
                    for (int i = 0; i < 10_000; i++) {
                        Assert.assertEquals(i, writer.put("symbol" + i));
                    }
                }
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 10_000, pool, 0);
                        SymbolMapReaderImpl older = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 9999, pool, 0)
                ) {
                    TestUtils.assertEquals("symbol9999", reader.valueOf(9999));
                    final int tailKeys = 10_000 % SymbolMapSegments.SEGMENT_KEYS;
                    Assert.assertEquals(tailKeys, reader.getCacheSize());
                    Assert.assertEquals(0, older.getCacheSize());
                    TestUtils.assertEquals("symbol0", older.valueOf(0));
                    Assert.assertEquals(tailKeys + SymbolMapSegments.SEGMENT_KEYS, older.getCacheSize());
                    Assert.assertEquals(1, pool.getEntryCount());
                    Assert.assertTrue(reader.getCacheAllocatedBytes() < 16 * 1024);
                    Assert.assertNull(older.valueOf(9999));
                    Assert.assertNull(older.newSymbolTableView().valueOf(9999));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, older.keyOf("symbol9999"));
                    ObjList<SymbolTable> views = new ObjList<>(reader, reader.newSymbolTableView(), older, older.newSymbolTableView());
                    int key = 4096;
                    for (int v = 0; v < views.size(); v++) {
                        SymbolTable view = views.getQuick(v);
                        // Miss/miss, hit/miss, miss/hit, and hit/hit must all preserve A and B.
                        for (int mask = 0; mask < 4; mask++, key += 2) {
                            if ((mask & 1) != 0) {
                                reader.valueOf(key);
                            }
                            if ((mask & 2) != 0) {
                                reader.valueOf(key + 1);
                            }
                            CharSequence a = view.valueOf(key);
                            CharSequence b = view.valueBOf(key + 1);
                            TestUtils.assertEquals("symbol" + key, a);
                            TestUtils.assertEquals("symbol" + (key + 1), b);
                            Assert.assertFalse(((DirectString) a).isStable());
                        }
                    }
                }
                try (SymbolMapReaderImpl standalone = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 10_000)) {
                    TestUtils.assertEquals("symbol9999", standalone.valueOf(9999));
                    Assert.assertEquals(0, standalone.getCacheSize());
                }
            }
        });
    }

    @Test
    public void testSharedCacheReaderWithZeroAndExhaustedBudget() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 8192, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
                    for (int key = 0; key < 4097; key++) {
                        writer.put("s" + key);
                    }
                }
                for (long limit : new long[]{0, 1, 1024}) {
                    final boolean isDisabled = limit == 0;
                    NativeMemoryBudget budget = new NativeMemoryBudget(limit);
                    Assert.assertTrue(budget.tryReserve(limit));
                    final long tagBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE);
                    try (
                            SymbolMapCachePool pool = new SymbolMapCachePool(budget);
                            SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 4097, pool, 0)
                    ) {
                        SymbolTable view = reader.newSymbolTableView();
                        TestUtils.assertEquals("s0", reader.valueOf(0));
                        TestUtils.assertEquals("s1", reader.valueBOf(1));
                        TestUtils.assertEquals("s4096", reader.valueOf(4096));
                        for (int i = 0; i < 10; i++) {
                            TestUtils.assertEquals("s0", reader.valueOf(0));
                            TestUtils.assertEquals("s4096", reader.valueBOf(4096));
                            TestUtils.assertEquals("s0", view.valueOf(0));
                            TestUtils.assertEquals("s4096", view.valueBOf(4096));
                        }
                        Assert.assertEquals(0, reader.getCacheSize());
                        Assert.assertEquals(0, reader.getCacheAllocatedBytes());
                        Assert.assertEquals(isDisabled ? 0 : 1, pool.getEntryCount());
                        Assert.assertEquals(0, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE) - tagBytes);
                        Assert.assertEquals(limit, budget.getUsedBytes());
                    }
                    budget.release(limit);
                    Assert.assertEquals(0, budget.getUsedBytes());
                }
            }
        });
    }

    @Test
    public void testAppend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                Rnd rnd = new Rnd();

                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                N,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = N - 1;
                    // append second batch and check that symbol keys start with N
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }

                    // try to append first batch - this should return symbol keys starting with 0
                    rnd.reset();
                    prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, writer.put(null));
                }
            }
        });
    }

    @Test
    public void testCacheExhaustionFallsBackToTheOnDiskIndex() throws Exception {
        // The cache addresses its keys by 32-bit word offsets, so a column that interns
        // enough distinct symbols exhausts it and the writer drops it and looks every later
        // symbol up on disk instead. Reaching that in production takes gigabytes of
        // symbols; the limit is lowered here so the transition itself can be walked over.
        final long previousLimit = SymbolMapWriter.setCacheKeyBufferLimit(256);
        try {
            TestUtils.assertMemoryLeak(() -> {
                try (Path path = new Path().of(configuration.getDbRoot())) {
                    create(path, "x", 128, true);
                    final int symbolCount;
                    final int uncachedFrom;
                    try (
                            SymbolMapWriter writer = new SymbolMapWriter(
                                    configuration,
                                    path,
                                    "x",
                                    COLUMN_NAME_TXN_NONE,
                                    0,
                                    -1,
                                    NOOP_COLLECTOR,
                                    -1
                            )
                    ) {
                        Assert.assertTrue("the column asked for a cache", writer.isCached());
                        Assert.assertTrue("...and got one", writer.isCacheAllocated());

                        // Intern until the key buffer runs out. Every symbol is distinct, so
                        // each one takes buffer the previous ones did not.
                        final ObjList<String> cached = new ObjList<>();
                        int key = -1;
                        while (writer.isCacheAllocated() && cached.size() < 64) {
                            final String symbol = "cached-symbol-" + cached.size();
                            Assert.assertEquals(++key, writer.put(symbol));
                            cached.add(symbol);
                        }
                        Assert.assertFalse(
                                "the cache must be dropped once its key buffer is exhausted,"
                                        + " rather than grown past what it can address",
                                writer.isCacheAllocated()
                        );
                        // A concrete floor rather than "more than one". The limit above admits
                        // six of these symbols and the seventh is the one that trips the drop -
                        // the list holds all seven, since the tripping symbol is interned too,
                        // just through the fallback. A change to the cache's sizing that
                        // collapsed the cached run to a single insert would otherwise leave this
                        // case asserting the fallback over almost nothing.
                        Assert.assertTrue(
                                "the cached run was " + cached.size() + " symbols, too short to"
                                        + " exercise the cache before the drop",
                                cached.size() >= 5
                        );
                        // The column's own CACHE flag is untouched: dropping the accelerator
                        // is the writer's business, not a change to what the column declared.
                        Assert.assertTrue(writer.isCached());

                        // Existing symbols: every key interned while the cache was alive must
                        // still resolve to the same key, now off the on-disk index.
                        for (int i = 0, n = cached.size(); i < n; i++) {
                            Assert.assertEquals(
                                    "symbol " + i + " changed key after the cache was dropped",
                                    i,
                                    writer.put(cached.getQuick(i))
                            );
                        }

                        // New symbols: still appended, still sequential, still uncached.
                        final int firstUncached = cached.size();
                        for (int i = 0; i < 32; i++) {
                            Assert.assertEquals(firstUncached + i, writer.put("uncached-symbol-" + i));
                            Assert.assertFalse(writer.isCacheAllocated());
                        }

                        // Duplicates of both eras resolve to their own key rather than
                        // appending a second copy.
                        Assert.assertEquals(0, writer.put(cached.getQuick(0)));
                        Assert.assertEquals(firstUncached, writer.put("uncached-symbol-0"));
                        Assert.assertEquals(firstUncached + 31, writer.put("uncached-symbol-31"));
                        Assert.assertEquals(firstUncached + 32, writer.getSymbolCount());
                        Assert.assertEquals(SymbolTable.VALUE_IS_NULL, writer.put(null));
                        symbolCount = writer.getSymbolCount();
                        uncachedFrom = firstUncached;
                    }

                    // ...and the whole map reads back holding the values the writer was given,
                    // in the keys it handed out. Reading each value back by name is what says
                    // the fallback wrote the same thing the cache would: an identity
                    // round-trip alone cannot tell a correct map from a consistently wrong one.
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(
                            configuration,
                            path,
                            "x",
                            COLUMN_NAME_TXN_NONE,
                            symbolCount
                    )) {
                        Assert.assertEquals(symbolCount, reader.getSymbolCount());
                        for (int i = 0; i < symbolCount; i++) {
                            TestUtils.assertEquals(
                                    i < uncachedFrom
                                            ? "cached-symbol-" + i
                                            : "uncached-symbol-" + (i - uncachedFrom),
                                    reader.valueOf(i)
                            );
                            Assert.assertEquals(i, reader.keyOf(reader.valueOf(i)));
                        }
                    }
                }
            });
        } finally {
            SymbolMapWriter.setCacheKeyBufferLimit(previousLimit);
        }
    }

    @Test
    public void testCloseReleasesCacheWhenCharFileReleaseFails() throws Exception {
        assertCloseReleasesCacheWhenReleaseFails(".c");
    }

    @Test
    public void testCloseReleasesCacheWhenIndexValueFileReleaseFails() throws Exception {
        assertCloseReleasesCacheWhenReleaseFails(".v");
    }

    @Test
    public void testConcurrentSymbolTableAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int keys = 1000;
            final int iterations = 10000;
            final int readerCount = 3;

            CountDownLatch stopLatch = new CountDownLatch(readerCount);
            CyclicBarrier startBarrier = new CyclicBarrier(readerCount);
            AtomicInteger errors = new AtomicInteger();

            IntObjHashMap<String> symbols = new IntObjHashMap<>();

            SymbolMapReaderImpl reader;
            try (final Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", keys, false);

                // Obtain the reader when there are no symbols yet.
                reader = new SymbolMapReaderImpl(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0
                );

                // Write the symbols.
                try (final SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1,
                        false
                )) {
                    int prev = -1;
                    for (int i = 0; i < keys; i++) {
                        String symbol = "sym" + i;
                        int key = writer.put(symbol);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                        symbols.put(key, symbol);
                    }
                }
            }

            // Reload the reader.
            reader.updateSymbolCount(keys);

            class ReaderThread extends Thread {
                final StaticSymbolTable symbolTable;

                ReaderThread(StaticSymbolTable symbolTable) {
                    this.symbolTable = symbolTable;
                }

                @Override
                public void run() {
                    Rnd rnd = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());
                    try {
                        startBarrier.await();
                        for (int i = 0; i < iterations; i++) {
                            int key = rnd.nextPositiveInt() % symbols.size();
                            int actualKey = symbolTable.keyOf(symbols.get(key));
                            Assert.assertEquals(key, actualKey);
                        }
                    } catch (Throwable e) {
                        errors.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        stopLatch.countDown();
                    }
                }
            }

            new ReaderThread(reader).start();
            for (int i = 0; i < readerCount - 1; i++) {
                new ReaderThread(reader.newSymbolTableView()).start();
            }

            try {
                Assert.assertTrue(stopLatch.await(20000, TimeUnit.SECONDS));
                Assert.assertEquals(0, errors.get());
            } finally {
                Misc.free(reader);
            }
        });
    }

    @Test
    public void testCorruptOffsetFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1024;
            try (Path path = new Path().of(configuration.getDbRoot()); Path path2 = new Path()) {
                create(path, "x", N, true);
                int pathSize = path.size();
                int symbolCount = 6;

                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    Assert.assertEquals(0, writer.put("A1"));
                    Assert.assertEquals(1, writer.put("A2"));
                    Assert.assertEquals(2, writer.put("A3"));
                    Assert.assertEquals(3, writer.put("A4"));
                    Assert.assertEquals(4, writer.put("A5"));
                    Assert.assertEquals(5, writer.put("A6"));
                }

                // Corrupt offset file, backup it first
                var oFile = offsetFileName(path.trimTo(pathSize), "x", -1);
                path2.of(path).put(".bak");
                FilesFacade ff = configuration.getFilesFacade();
                ff.copy(oFile, path2.$());

                try (MemoryCMARW mem = Vm.getSmallCMARWInstance(
                        configuration.getFilesFacade(),
                        offsetFileName(path.trimTo(pathSize), "x", -1),
                        MemoryTag.MMAP_DEFAULT,
                        configuration.getWriterFileOpenOpts()
                )) {
                    for (long l = SymbolMapWriter.HEADER_SIZE; l < mem.size(); l += 8) {
                        Unsafe.putLong(mem.addressOf(l), 0);
                    }
                    mem.jumpTo(mem.size());
                }

                try (
                        SymbolMapWriter ignore = new SymbolMapWriter(
                                configuration,
                                path.trimTo(pathSize),
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                symbolCount,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    Assert.fail("expected corrupt exception");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "symbol column map is corrupt"));
                }

                // restore .o file
                oFile = offsetFileName(path.trimTo(pathSize), "x", -1);
                path2.of(path).put(".bak");
                ff.remove(oFile);
                ff.copy(path2.$(), oFile);

                // Check that .c file still has the values
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path.trimTo(pathSize),
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                symbolCount,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    Assert.assertEquals(5, writer.put("A6"));
                    Assert.assertEquals(4, writer.put("A5"));
                    Assert.assertEquals(3, writer.put("A4"));
                    Assert.assertEquals(2, writer.put("A3"));
                    Assert.assertEquals(1, writer.put("A2"));
                    Assert.assertEquals(0, writer.put("A1"));
                }
            }
        });
    }

    @Test
    public void testKeyOfCachedKeyLeavesReaderCacheEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int n = 5000;
                create(path, "x", n, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1
                )) {
                    for (int i = 0; i < n; i++) {
                        writer.put("key" + i);
                    }
                }

                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, n, pool, 0)
                ) {
                    Assert.assertTrue(reader.isCached());
                    Assert.assertEquals(n - 1, reader.keyOf("key" + (n - 1), n - 1));
                    Assert.assertEquals(4500, reader.keyOf("key4500", n - 1));
                    Assert.assertEquals(4097, reader.keyOf("key4097", SymbolTable.VALUE_NOT_FOUND));
                    Assert.assertEquals(4099, reader.keyOf("key4099", n));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("missing", 4096));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, reader.keyOf(null, 4096));
                    Assert.assertEquals(0, reader.getCacheSize());
                    final CharSequence value = reader.valueOf(4500);
                    Assert.assertEquals(SymbolMapSegments.SEGMENT_KEYS, reader.getCacheSize());
                    // a wrong hint must not rebind the valueOf() view passed in as the value
                    Assert.assertEquals(4500, reader.keyOf(value, 4501));
                    TestUtils.assertEquals("key4500", value);
                    Assert.assertEquals(SymbolMapSegments.SEGMENT_KEYS, reader.getCacheSize());
                }
            }
        });
    }

    @Test
    public void testKeyOfCachedKeyThroughLiveViewOverlayLeavesReaderCacheEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int n = 5000;
                create(path, "x", n, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1
                )) {
                    for (int i = 0; i < n; i++) {
                        writer.put("key" + i);
                    }
                }

                final IntList columnTypes = new IntList();
                columnTypes.add(ColumnType.SYMBOL);
                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, n, pool, 0);
                        LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes);
                        LiveViewSymbolTable overlay = new LiveViewSymbolTable().of(reader, cache, 0, 0, false, false)
                ) {
                    Assert.assertTrue(reader.isCached());
                    Assert.assertEquals(n - 1, overlay.keyOf("key" + (n - 1), n - 1));
                    Assert.assertEquals(4500, overlay.keyOf("key4500", n - 1));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, overlay.keyOf("missing", n));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, overlay.keyOf(null, 3));
                    Assert.assertEquals(0, reader.getCacheSize());
                }
            }
        });
    }

    @Test
    public void testKeyOfPastIndexKeyCountAllocatesNothing() throws Exception {
        // A value whose bounded hash falls at or past the index's key count gets the shared empty
        // row cursor. Neither the reader's pooled cursor nor a view's reused one may be lost to it,
        // or the next in-range lookup allocates a replacement cursor.
        TestUtils.assertMemoryLeak(() -> {
            final int symbolCapacity = 1024;
            // SymbolMapReaderImpl bounds value hashes with this mask
            final int maxHash = Numbers.ceilPow2(symbolCapacity / 2) - 1;
            // The only stored value hashes to bucket 0, so the index's key count is 1 and every
            // other bucket lies past it.
            String stored = null;
            for (int i = 0; stored == null; i++) {
                final String candidate = "stored" + i;
                if (Hash.boundedHash(candidate, maxHash) == 0) {
                    stored = candidate;
                }
            }
            String missing = null;
            for (int i = 0; missing == null; i++) {
                final String candidate = "missing" + i;
                if (Hash.boundedHash(candidate, maxHash) > 0) {
                    missing = candidate;
                }
            }

            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", symbolCapacity, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.assertEquals(0, writer.put(stored));
                }
                try (
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 1);
                        TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
                ) {
                    final StaticSymbolTable view = reader.newSymbolTableView();
                    // the first round fills the reader's cursor pool and the view's cursor
                    assertAlternatingLookups(reader, view, stored, missing, 100);

                    final ThreadMXBean threadMXBean = scope.getBean();
                    final long threadId = Thread.currentThread().threadId();
                    final long before = threadMXBean.getThreadAllocatedBytes(threadId);
                    assertAlternatingLookups(reader, view, stored, missing, 10_000);
                    final long allocated = threadMXBean.getThreadAllocatedBytes(threadId) - before;
                    Assert.assertTrue(
                            "10,000 rounds of lookups allocated " + allocated + " bytes",
                            allocated < 64 * 1024
                    );
                }
            }
        });
    }

    @Test
    public void testLookupPerformance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000000;
            int symbolCount = 1024;
            ObjList<String> symbols = new ObjList<>();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", symbolCount, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    Rnd rnd = new Rnd();
                    int prev = -1;
                    for (int i = 0; i < symbolCount; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        symbols.add(cs.toString());
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }

                    long t = System.nanoTime();
                    for (int i = 0; i < N; i++) {
                        int key = rnd.nextPositiveInt() % symbolCount;
                        Assert.assertEquals(key, writer.put(symbols.getQuick(key)));
                    }
                    System.out.println("SymbolMapWriter lookup performance [10M <500ms]: " + (System.nanoTime() - t) / 1000000);
                }
            }
        });
    }

    @Test
    public void testMapDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                try {
                    new SymbolMapWriter(
                            configuration,
                            path,
                            "x",
                            COLUMN_NAME_TXN_NONE,
                            0,
                            -1,
                            NOOP_COLLECTOR,
                            -1,
                            false
                    );
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "does not exist"));
                }
            }
        });
    }

    @Test
    public void testMergeAppend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                create(path, "y", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "y",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = -1;
                    for (int i = N; i < 2 * N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                create(path, "z", 2 * N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "z",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                        boolean remapped = SymbolMapWriter.mergeSymbols(writer, reader);
                        Assert.assertFalse(remapped);
                    }

                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "y", COLUMN_NAME_TXN_NONE, N)) {
                        boolean remapped = SymbolMapWriter.mergeSymbols(writer, reader);
                        Assert.assertTrue(remapped);
                    }
                }

                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "z", COLUMN_NAME_TXN_NONE, 2 * N)) {

                    for (int i = 0; i < 2 * N; i++) {
                        CharSequence cs = String.valueOf(i);
                        TestUtils.assertEquals(cs, reader.valueOf(i));
                        Assert.assertEquals(i, reader.keyOf(cs));
                    }

                    Assert.assertEquals(2 * N, reader.getSymbolCount());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertNull(reader.valueOf(2 * N));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                }
            }
        });
    }

    @Test
    public void testMergeIntoEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }

                }

                create(path, "y", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "y",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                        boolean remapped = SymbolMapWriter.mergeSymbols(writer, reader);
                        Assert.assertFalse(remapped);

                        for (int i = 0; i < N; i++) {
                            CharSequence cs = String.valueOf(i);
                            TestUtils.assertEquals(cs, reader.valueOf(i));
                            Assert.assertEquals(i, reader.keyOf(cs));
                        }

                        Assert.assertEquals(N, reader.getSymbolCount());
                        Assert.assertNull(reader.valueOf(-1));
                        Assert.assertNull(reader.valueOf(N));
                        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                    }
                }
            }
        });
    }

    @Test
    public void testMergeOverlapped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int plen = path.size();
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                create(path, "y", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "y",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i + N / 2);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                int T = N + N / 2;
                create(path, "z", T, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "z",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                        try (
                                MemoryCMARW mem = Vm.getSmallCMARWInstance(
                                        configuration.getFilesFacade(),
                                        path.concat("x").put(TableUtils.SYMBOL_KEY_REMAP_FILE_SUFFIX).$(),
                                        MemoryTag.MMAP_DEFAULT,
                                        configuration.getWriterFileOpenOpts()
                                )
                        ) {
                            SymbolMapWriter.mergeSymbols(writer, reader, mem);
                            for (int i = 0; i < N; i++) {
                                long newId = mem.getInt(i * Integer.BYTES);
                                Assert.assertEquals(i, newId);
                            }
                        }
                    }

                    path.trimTo(plen);
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "y", COLUMN_NAME_TXN_NONE, N)) {
                        try (
                                MemoryCMARW mem = Vm.getSmallCMARWInstance(
                                        configuration.getFilesFacade(),
                                        path.concat("y").put(TableUtils.SYMBOL_KEY_REMAP_FILE_SUFFIX).$(),
                                        MemoryTag.MMAP_DEFAULT,
                                        configuration.getWriterFileOpenOpts()
                                )
                        ) {
                            SymbolMapWriter.mergeSymbols(writer, reader, mem);
                            for (int i = 0; i < N; i++) {
                                long newId = mem.getInt(i * Integer.BYTES);
                                Assert.assertEquals(i + N / 2, newId);
                            }
                        }
                    }
                }

                path.trimTo(plen);
                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "z", COLUMN_NAME_TXN_NONE, T)) {

                    for (int i = 0; i < T; i++) {
                        CharSequence cs = String.valueOf(i);
                        TestUtils.assertEquals(cs, reader.valueOf(i));
                        Assert.assertEquals(i, reader.keyOf(cs));
                    }

                    Assert.assertEquals(T, reader.getSymbolCount());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertNull(reader.valueOf(T));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                }
            }
        });
    }

    @Test
    public void testMergeWithEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }
                    create(path, "y", N, true);
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "y", COLUMN_NAME_TXN_NONE, 0)) {
                        boolean remapped = SymbolMapWriter.mergeSymbols(writer, reader);
                        Assert.assertFalse(remapped);
                    }
                }

                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        TestUtils.assertEquals(cs, reader.valueOf(i));
                        Assert.assertEquals(i, reader.keyOf(cs));
                    }

                    Assert.assertEquals(N, reader.getSymbolCount());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertNull(reader.valueOf(N));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                }
            }
        });
    }

    @Test
    public void testReadEmptySymbolMap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0)) {
                    Assert.assertEquals(N, reader.getSymbolCapacity());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, reader.keyOf(null));
                }
            }
        });
    }

    @Test
    public void testReaderCache() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 16, true);

                int[] keys = new int[16];
                try (SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1,
                        false
                )) {
                    for (int i = 0; i < keys.length; i++) {
                        keys[i] = writer.put("key" + i);
                    }
                }

                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, keys.length, pool, 0)
                ) {
                    Assert.assertTrue(reader.isCached());
                    Assert.assertEquals(0, reader.getCacheSize());
                    for (int i = 0; i < keys.length; i++) {
                        TestUtils.assertEquals("key" + i, reader.valueOf(keys[i]));
                        Assert.assertEquals(keys.length, reader.getCacheSize());
                    }
                    for (int i = 0; i < keys.length; i++) {
                        final int j = keys.length - 1 - i;
                        final CharSequence a = reader.valueOf(keys[i]);
                        final CharSequence b = reader.valueBOf(keys[j]);
                        TestUtils.assertEquals("key" + i, a);
                        TestUtils.assertEquals("key" + j, b);
                    }
                    Assert.assertEquals(keys.length, reader.getCacheSize());
                }
            }
        });
    }

    @Test
    public void testReaderWhenMapDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                try {
                    new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "does not exist"));
                }
            }
        });
    }

    @Test
    public void testReaderWithShortHeader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int plen = path.size();
                Assert.assertTrue(configuration.getFilesFacade().touch(path.concat("x").put(".o").$()));
                try {
                    new SymbolMapReaderImpl(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, 0);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "too short"));
                }
            }
        });
    }

    @Test
    public void testRollback() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1024;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    Rnd rnd = new Rnd();
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }

                    writer.rollback(N / 2);

                    prev = N / 2 - 1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }
            }
        });
    }

    @Test
    public void testRollbackAndRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1024;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    Assert.assertEquals(0, writer.put("A1"));
                    Assert.assertEquals(1, writer.put("A2"));
                    Assert.assertEquals(2, writer.put("A3"));
                    Assert.assertEquals(3, writer.put("A4"));
                    Assert.assertEquals(4, writer.put("A5"));

                    Assert.assertEquals(5, writer.put("A6"));
                    Assert.assertEquals(6, writer.put("A7"));
                    Assert.assertEquals(7, writer.put("A8"));
                    Assert.assertEquals(8, writer.put("A9"));
                    Assert.assertEquals(9, writer.put("A10"));

                    writer.rollback(5);

                    Assert.assertEquals(5, writer.put("A6"));
                    Assert.assertEquals(6, writer.put("A7"));
                    Assert.assertEquals(7, writer.put("A8"));
                    Assert.assertEquals(8, writer.put("A9"));
                    Assert.assertEquals(9, writer.put("A10"));

                }
            }
        });
    }

    @Test
    public void testRollbackFuzz() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        int symbols = 64 + rnd.nextInt(1024);

        int resets = 10 + rnd.nextInt(10);

        // Create longer symbols to hit various mapping page sizes
        int symbolPrefixSize = (rnd.nextInt(200) + 5) / 3;
        StringBuilder symbolPrefix = new StringBuilder("abc");
        symbolPrefix.repeat("abc", Math.max(0, symbolPrefixSize));
        String prefix = symbolPrefix.toString();

        TestUtils.assertMemoryLeak(() -> {
            int N = 128;
            ObjList<CharSequence> symbolList = new ObjList<>();
            IntList indexList = new IntList();

            try (Path path = new Path().of(configuration.getDbRoot())) {

                SymbolMapUtil smu = new SymbolMapUtil();
                create(path, "x", N, true);

                SymbolMapWriter w = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1,
                        false
                );
                int hi = rnd.nextInt(symbols);
                hi = addRange(w, 0, hi, rnd, symbolList, indexList, prefix);

                for (int i = 0; i < resets; i++) {
                    int resetTo = Math.max(0, rnd.nextInt(Math.max(1, hi - 100)));
                    w.close();

                    destroySymbolFilesOffsets(path, "x", resetTo, rnd);
                    smu.rebuildSymbolFiles(configuration, path, "x", -1, resetTo, -1);

                    w = new SymbolMapWriter(
                            configuration,
                            path,
                            "x",
                            COLUMN_NAME_TXN_NONE,
                            resetTo,
                            -1,
                            NOOP_COLLECTOR,
                            -1,
                            false
                    );

                    hi = resetTo + rnd.nextInt(symbols - resetTo);
                    hi = addRange(w, resetTo, Math.max(resetTo, hi), rnd, symbolList, indexList, prefix);
                }
                w.close();
            }
        });
    }

    @Test
    public void testShortHeader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int plen = path.size();
                Assert.assertTrue(configuration.getFilesFacade().touch(path.concat("x").put(".o").$()));
                try {
                    new SymbolMapWriter(
                            configuration,
                            path.trimTo(plen),
                            "x",
                            COLUMN_NAME_TXN_NONE,
                            0,
                            -1,
                            NOOP_COLLECTOR,
                            -1,
                            false
                    );
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "too short"));
                }
            }
        });
    }

    @Test
    public void testSimpleAdd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1000000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1,
                        false
                )
                ) {
                    Rnd rnd = new Rnd();
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }
            }
        });
    }

    @Test
    public void testSimpleRead() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1000000;
            Rnd rnd = new Rnd();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }
                }
                rnd.reset();
                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        TestUtils.assertEquals(cs, reader.valueOf(i));
                        Assert.assertEquals(i, reader.keyOf(cs));
                    }

                    Assert.assertEquals(N, reader.getSymbolCount());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertNull(reader.valueOf(N));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                }
            }
        });
    }

    @Test
    public void testStaticSymbolKeysFollowSymbolCount() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createMap(path, "x", "v", 5, -1, 0);
                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 2)) {
                    CharSequenceHashSet values = new CharSequenceHashSet();
                    values.add("v1");
                    values.add("v2");
                    values.add("v3");
                    StaticSymbolKeys keys = new StaticSymbolKeys();
                    Assert.assertTrue(keys.of(reader, values));
                    assertKeys(keys, 1);
                    Assert.assertFalse(keys.of(reader, values));
                    assertKeys(keys, 1);

                    // Fewer new keys than values: of() looks up the new keys.
                    reader.updateSymbolCount(4);
                    Assert.assertFalse(keys.of(reader, values));
                    assertKeys(keys, 1, 2, 3);

                    reader.updateSymbolCount(2);
                    Assert.assertTrue(keys.of(reader, values));
                    assertKeys(keys, 1);

                    reader.updateSymbolCount(5);
                    Assert.assertTrue(keys.of(reader, values));
                    assertKeys(keys, 1, 2, 3);
                }
            }
        });
    }

    @Test
    public void testSymbolCapacityRebuildRestoresTheDroppedCache() throws Exception {
        // ALTER TABLE ... ALTER COLUMN ... SYMBOL CAPACITY re-opens the map's files and
        // re-indexes every value it holds, so it is the one non-destructive lever an
        // operator has over a writer that dropped its cache - and the only lever at all
        // on a WAL table, whose TRUNCATE keeps symbol maps. The rebuild used to skip
        // setupCache whenever the requested flag matched the one the writer already
        // carried, and the drop leaves that flag on, so the request could never differ.
        final long previousLimit = SymbolMapWriter.setCacheKeyBufferLimit(256);
        try {
            assertMemoryLeak(() -> {
                try (Path path = new Path().of(configuration.getDbRoot())) {
                    create(path, "x", 128, true);
                    try (
                            SymbolMapWriter writer = new SymbolMapWriter(
                                    configuration,
                                    path,
                                    "x",
                                    COLUMN_NAME_TXN_NONE,
                                    0,
                                    -1,
                                    NOOP_COLLECTOR,
                                    -1
                            )
                    ) {
                        final ObjList<String> exhausted = exhaustCache(writer);

                        writer.rebuildCapacity(configuration, path, "x", COLUMN_NAME_TXN_NONE, 1024, true);

                        Assert.assertTrue(
                                "a capacity rebuild that is still asked for a cache must hand one"
                                        + " back rather than leave the writer on the on-disk index",
                                writer.isCacheAllocated()
                        );
                        Assert.assertTrue(writer.isCached());
                        Assert.assertEquals(1024, writer.getSymbolCapacity());

                        // The rebuild re-indexed what the column already held, so the values
                        // survive at their original keys, and the fresh cache - which starts
                        // empty over a non-empty column - fills from the on-disk index rather
                        // than handing out keys of its own.
                        Assert.assertEquals(exhausted.size(), writer.getSymbolCount());
                        for (int i = 0, n = exhausted.size(); i < n; i++) {
                            Assert.assertEquals(i, writer.put(exhausted.getQuick(i)));
                            Assert.assertEquals(i, writer.put(exhausted.getQuick(i)));
                        }
                        Assert.assertEquals(exhausted.size(), writer.getSymbolCount());
                    }
                }
            });
        } finally {
            SymbolMapWriter.setCacheKeyBufferLimit(previousLimit);
        }
    }

    @Test
    public void testSymbolCountShrinkBoundsLookupAndCache() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 8, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.assertEquals(0, writer.put("first"));
                    Assert.assertEquals(1, writer.put("second"));
                }

                try (
                        SymbolMapCachePool pool = new SymbolMapCachePool(new NativeMemoryBudget(1024 * 1024));
                        SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 2, pool, 0)
                ) {
                    TestUtils.assertEquals("first", reader.valueOf(0));
                    TestUtils.assertEquals("second", reader.valueOf(1));
                    Assert.assertEquals(2, reader.getCacheSize());

                    // the values stay in the shared entry, the reader's bound hides them
                    reader.updateSymbolCount(1);
                    Assert.assertEquals(1, reader.getSymbolCount());
                    Assert.assertEquals(2, reader.getCacheSize());
                    TestUtils.assertEquals("first", reader.valueOf(0));
                    Assert.assertNull(reader.valueOf(1));
                    Assert.assertNull(reader.valueBOf(1));
                    Assert.assertEquals(0, reader.keyOf("first"));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("second"));

                    reader.updateSymbolCount(0);
                    Assert.assertEquals(0, reader.getSymbolCount());
                    Assert.assertNull(reader.valueOf(0));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("first"));
                }
            }
        });
    }

    @Test
    public void testTransactionalRead() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1000000;
            Rnd rnd = new Rnd();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }

                    rnd.reset();
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                        for (int i = 0; i < N; i++) {
                            CharSequence cs = rnd.nextChars(10);
                            TestUtils.assertEquals(cs, reader.valueOf(i));
                            Assert.assertEquals(i, reader.keyOf(cs));
                        }

                        Assert.assertNull(reader.valueOf(N));
                        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                        Assert.assertEquals(N, writer.put("XYZ"));

                        // must not be able to read new symbol
                        Assert.assertNull(reader.valueOf(N));
                        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("XYZ"));

                        reader.updateSymbolCount(N + 1);
                        TestUtils.assertEquals("XYZ", reader.valueOf(N));
                        Assert.assertEquals(N, reader.keyOf("XYZ"));
                    }
                }
            }
        });
    }

    @Test
    public void testTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1024;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1,
                                false
                        )
                ) {
                    Rnd rnd = new Rnd();
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }

                    Assert.assertEquals(N, writer.getSymbolCount());

                    writer.truncate();

                    Assert.assertEquals(0, writer.getSymbolCount());

                    // reset RND to exercise symbol cache
                    rnd.reset();
                    prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                    Assert.assertEquals(N, writer.getSymbolCount());
                }
            }
        });
    }

    @Test
    public void testTruncateRestoresTheDroppedCache() throws Exception {
        // A column that runs its cache's key buffer out keeps working off the on-disk
        // index, at roughly twice the cost per lookup when the column's declared
        // capacity matches its population and far more when it does not. TRUNCATE
        // empties the column, which retires the exhaustion outright, so a writer the
        // column still tells to cache has to come back cached rather than stay degraded
        // for as long as its table writer stays pooled.
        final long previousLimit = SymbolMapWriter.setCacheKeyBufferLimit(256);
        try {
            assertMemoryLeak(() -> {
                try (Path path = new Path().of(configuration.getDbRoot())) {
                    create(path, "x", 128, true);
                    try (
                            SymbolMapWriter writer = new SymbolMapWriter(
                                    configuration,
                                    path,
                                    "x",
                                    COLUMN_NAME_TXN_NONE,
                                    0,
                                    -1,
                                    NOOP_COLLECTOR,
                                    -1
                            )
                    ) {
                        final ObjList<String> exhausted = exhaustCache(writer);

                        writer.truncate();

                        Assert.assertEquals(0, writer.getSymbolCount());
                        Assert.assertTrue(
                                "TRUNCATE empties the key buffer the cache ran out of, so a column"
                                        + " that is still told to cache must get its cache back",
                                writer.isCacheAllocated()
                        );
                        Assert.assertTrue(writer.isCached());

                        // A working cache rather than an empty shell, and a bounded one:
                        // the same run of values interns from zero again, hands out the same
                        // keys, and runs the key buffer out a second time at the same point.
                        // A restored cache that could not exhaust again would mean TRUNCATE
                        // had handed out an unbounded one.
                        final ObjList<String> second = exhaustCache(writer);
                        Assert.assertEquals(exhausted.size(), second.size());
                        for (int i = 0, n = exhausted.size(); i < n; i++) {
                            Assert.assertEquals(exhausted.getQuick(i), second.getQuick(i));
                        }
                    }
                }
            });
        } finally {
            SymbolMapWriter.setCacheKeyBufferLimit(previousLimit);
        }
    }

    @Test
    public void testUpdateCacheFlagOffReleasesTheCache() throws Exception {
        // ALTER TABLE ... ALTER COLUMN ... NOCACHE lands here, on the writer already
        // serving the column - nothing re-opens it. put() dispatches on the cache rather
        // than on the flag, because a cache dropped on key-buffer exhaustion leaves the
        // flag on, so a NOCACHE that wrote only the flag would leave the writer caching
        // and leave the cache's native buffers charged to NATIVE_TABLE_WRITER until the
        // table writer leaves the pool - for a column the user just asked not to cache.
        assertMemoryLeak(() -> {
            final int values = 10_000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 16, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    for (int i = 0; i < values; i++) {
                        Assert.assertEquals(i, writer.put("key" + i));
                    }
                    Assert.assertTrue("the column asked for a cache and must have got one", writer.isCacheAllocated());

                    final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
                    writer.updateCacheFlag(false);
                    final long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);

                    // The cache is the writer's only NATIVE_TABLE_WRITER allocation - its
                    // mapped files are charged to MMAP_INDEX_WRITER - so the whole of this
                    // difference is the cache going back to the allocator.
                    Assert.assertTrue(
                            "NOCACHE must hand the cache's native buffers back [before=" + before
                                    + ", after=" + after + ']',
                            after < before
                    );
                    Assert.assertFalse(writer.isCacheAllocated());
                    Assert.assertFalse(writer.isCached());

                    // Still a working map: every key interned while the cache was alive
                    // resolves to the same key off the on-disk index, new values still
                    // append sequentially, and neither re-establishes a cache.
                    for (int i = 0; i < values; i++) {
                        Assert.assertEquals(i, writer.put("key" + i));
                    }
                    for (int i = 0; i < 32; i++) {
                        Assert.assertEquals(values + i, writer.put("later" + i));
                    }
                    Assert.assertFalse(writer.isCacheAllocated());
                    Assert.assertEquals(values + 32, writer.getSymbolCount());

                    // A capacity rebuild re-runs the cache decision, and its gate admits a
                    // writer whose flag is on but whose cache is gone. A column turned
                    // NOCACHE carries the flag off, so the rebuild must leave it uncached
                    // rather than hand the cache back through that arm.
                    writer.rebuildCapacity(configuration, path, "x", COLUMN_NAME_TXN_NONE, 1024, false);
                    Assert.assertFalse(
                            "a capacity rebuild of a NOCACHE column must not re-establish its cache",
                            writer.isCacheAllocated()
                    );
                    Assert.assertFalse(writer.isCached());
                }

                // ...and the header carries the answer, so the next writer over the same
                // files opens uncached too.
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path.of(configuration.getDbRoot()),
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                values + 32,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.assertFalse(writer.isCached());
                    Assert.assertFalse(writer.isCacheAllocated());
                    Assert.assertEquals(0, writer.put("key0"));
                }
            }
        });
    }

    @Test
    public void testUpdateCacheFlagOnEstablishesTheCache() throws Exception {
        // The reverse direction, ALTER TABLE ... ALTER COLUMN ... CACHE. The writer
        // serving a NOCACHE column holds no cache, so raising the flag alone would leave
        // put() on the on-disk index and the column no faster than it was.
        assertMemoryLeak(() -> {
            final int values = 10_000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 16, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    for (int i = 0; i < values; i++) {
                        Assert.assertEquals(i, writer.put("key" + i));
                    }
                    Assert.assertFalse("the column asked for no cache", writer.isCacheAllocated());

                    final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
                    writer.updateCacheFlag(true);
                    final long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);

                    Assert.assertTrue(
                            "CACHE must build the cache the column now asks for [before=" + before
                                    + ", after=" + after + ']',
                            after > before
                    );
                    Assert.assertTrue(writer.isCacheAllocated());
                    Assert.assertTrue(writer.isCached());

                    // The new cache starts empty over a non-empty column, so the first
                    // lookup of each value comes off the on-disk index and the second off
                    // the cache. Both must give the key the value already had.
                    for (int i = 0; i < values; i++) {
                        Assert.assertEquals(i, writer.put("key" + i));
                        Assert.assertEquals(i, writer.put("key" + i));
                    }
                    Assert.assertEquals(values, writer.put("later"));
                    Assert.assertEquals(values, writer.put("later"));
                    Assert.assertEquals(values + 1, writer.getSymbolCount());
                }

                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path.of(configuration.getDbRoot()),
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                values + 1,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.assertTrue(writer.isCached());
                    Assert.assertTrue(writer.isCacheAllocated());
                    Assert.assertEquals(0, writer.put("key0"));
                }
            }
        });
    }

    private int addRange(SymbolMapWriter w, int lo, int hi, Rnd rnd, ObjList<CharSequence> symbolList, IntList indexList, String prefix) {
        LOG.info().$("Resetting range [").$(lo).$(", ").$(hi).$("]").$();

        symbolList.setPos(hi);
        indexList.setPos(hi);
        for (int i = lo; i < hi; i++) {
            int id = i + rnd.nextInt(hi * 2);
            String symbol = id % 3 == 0 ? "" : prefix + id;
            int symi = w.put(symbol);
            symbolList.setQuick(i, symbol);
            indexList.setQuick(i, symi);
        }

        // Read back all and check
        int symMax = 0;
        for (int i = 0; i < hi; i++) {
            int symi = w.put(symbolList.getQuick(i));
            Assert.assertEquals(indexList.get(i), symi);
            symMax = Math.max(symMax, symi);
        }
        return symMax;
    }

    private void assertAlternatingLookups(SymbolMapReaderImpl reader, StaticSymbolTable view, String stored, String missing, int rounds) {
        for (int i = 0; i < rounds; i++) {
            Assert.assertEquals(0, reader.keyOf(stored));
            Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf(missing));
            Assert.assertEquals(0, view.keyOf(stored));
            Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, view.keyOf(missing));
        }
    }

    /**
     * Closes a cached symbol map writer while the release of one of its mapped files
     * fails, and asserts the off-heap value-to-key cache still went back to the
     * allocator. {@code failingFileSuffix} picks which file's release raises: ".c"
     * fails inside the char memory's own close, ".v" fails one level down, inside
     * {@link io.questdb.cairo.idx.BitmapIndexWriter#close()}.
     * <p>
     * The failure lands on the file's truncate, which
     * {@link io.questdb.cairo.vm.Vm#bestEffortClose} reaches after the mapping is gone
     * and before the descriptor's own close in the enclosing finally. So the file the
     * facade fails releases everything it owns and still raises, and every resource
     * this test can leak belongs to the writer rather than to the injection.
     */
    private void assertCloseReleasesCacheWhenReleaseFails(String failingFileSuffix) throws Exception {
        final AtomicLong failingFd = new AtomicLong(-1);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade failingFf = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (Utf8s.endsWithAscii(name, failingFileSuffix)) {
                    failingFd.set(fd);
                }
                return fd;
            }

            @Override
            public boolean truncate(long fd, long size) {
                if (isArmed.get() && fd == failingFd.get()) {
                    throw CairoException.critical(0).put("injected truncate failure");
                }
                return super.truncate(fd, size);
            }
        };

        assertMemoryLeak(failingFf, () -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 64, true);
                final SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1
                );
                for (int i = 0; i < 32; i++) {
                    Assert.assertEquals(i, writer.put("symbol" + i));
                }
                Assert.assertTrue("the column asked for a cache and must have got one", writer.isCacheAllocated());

                isArmed.set(true);
                try {
                    writer.close();
                    Assert.fail("close() must surface the release failure the facade injected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "injected truncate failure");
                } finally {
                    isArmed.set(false);
                }

                // The cache is two native buffers. A release step that raises before the
                // writer gets to them strands them for the life of the process, since
                // nothing else references them once the writer is gone.
                Assert.assertFalse(
                        "close() must release the off-heap cache even when another release fails",
                        writer.isCacheAllocated()
                );
            }
        });
    }

    private void destroySymbolFilesOffsets(Path path, String name, int cleanCount, Rnd rnd) {
        int plen = path.size();
        try {
            FilesFacade ff = configuration.getFilesFacade();
            offsetFileName(path.trimTo(plen), name, -1);
            long size = ff.length(path.$());

            long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
            long address = TableUtils.mapRW(ff, fd, size, MemoryTag.MMAP_DEFAULT);
            for (long i = keyToOffset(cleanCount); i + 4 < size; i += 4) {
                Unsafe.putInt(address + i, rnd.nextInt());
            }

            ff.munmap(address, size, MemoryTag.MMAP_DEFAULT);
            ff.close(fd);

        } finally {
            path.trimTo(plen);
        }
    }

    @Test
    public void testWriterCacheIsOffHeapAndReleased() throws Exception {
        // The writer's value-to-key cache holds its keys in unmanaged memory, so it
        // shows up under NATIVE_TABLE_WRITER rather than on the Java heap, and it has
        // to be released on close - the enclosing assertMemoryLeak is what proves the
        // second half. Measured as the difference between a cached and an uncached
        // writer over the same values, so the writer's other native structures, which
        // are identical between the two, cancel out.
        TestUtils.assertMemoryLeak(() -> {
            final int values = 10_000;
            Assert.assertTrue(taggedBytesForWriter("cached", true, values) > taggedBytesForWriter("plain", false, values));
        });
    }

    private static void assertKeys(StaticSymbolKeys keys, int... expected) {
        for (int key = 0, e = 0; key < 5; key++) {
            final boolean isExpected = e < expected.length && expected[e] == key;
            Assert.assertEquals("key " + key, isExpected, keys.contains(key));
            if (isExpected) {
                e++;
            }
        }
    }

    private static long address(CharSequence value) {
        Assert.assertNotNull(value);
        return ((DirectString) value).lo();
    }

    private static void createMap(Path path, String name, String prefix, int count, int longKey, int suffixLength) {
        create(path, name, Numbers.ceilPow2(count), true);
        try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, name, COLUMN_NAME_TXN_NONE, 0, -1, NOOP_COLLECTOR, -1)) {
            for (int key = 0; key < count; key++) {
                String value = prefix + key;
                if (key == longKey) {
                    value += "x".repeat(suffixLength);
                }
                Assert.assertEquals(key, writer.put(value));
            }
        }
    }

    /**
     * Interns distinct values into {@code writer} until its cache runs the key buffer out
     * and the writer drops it, and returns every value interned along the way - the last of
     * them is the one whose insert tripped the drop, so that one went in through the on-disk
     * fallback. The caller has to have lowered
     * {@link SymbolMapWriter#setCacheKeyBufferLimit(long)} first; at the production ceiling
     * getting here takes eight gigabytes of distinct keys.
     */
    private static ObjList<String> exhaustCache(SymbolMapWriter writer) {
        Assert.assertTrue("the column asked for a cache", writer.isCached());
        Assert.assertTrue("...and got one", writer.isCacheAllocated());
        final ObjList<String> symbols = new ObjList<>();
        while (writer.isCacheAllocated() && symbols.size() < 64) {
            final String symbol = "cached-symbol-" + symbols.size();
            Assert.assertEquals(symbols.size(), writer.put(symbol));
            symbols.add(symbol);
        }
        Assert.assertFalse(
                "the cache must be dropped once its key buffer is exhausted, rather than"
                        + " grown past what it can address",
                writer.isCacheAllocated()
        );
        // The same floor the exhaustion case asserts: a sizing change that collapsed the
        // cached run to a single insert would leave every caller of this exercising the
        // fallback over almost nothing.
        Assert.assertTrue(
                "the cached run was " + symbols.size() + " symbols, too short to exercise"
                        + " the cache before the drop",
                symbols.size() >= 5
        );
        return symbols;
    }

    /**
     * Peak NATIVE_TABLE_WRITER bytes a symbol map writer holds after interning
     * {@code values} distinct values, over its own baseline. The writer is closed
     * before returning, so the caller's leak check also covers the release path.
     */
    private static long taggedBytesForWriter(CharSequence name, boolean useCache, int values) {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            create(path, name, 16, useCache);
            final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
            try (SymbolMapWriter writer = new SymbolMapWriter(
                    configuration,
                    path,
                    name,
                    COLUMN_NAME_TXN_NONE,
                    0,
                    -1,
                    NOOP_COLLECTOR,
                    -1
            )) {
                Assert.assertEquals(useCache, writer.isCached());
                for (int i = 0; i < values; i++) {
                    writer.put("key" + i);
                }
                return Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER) - before;
            }
        }
    }


}
