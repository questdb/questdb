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

package io.questdb.test.cairo.map;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectLongLongAscList;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Unordered8MapTest extends AbstractCairoTest {
    Decimal128 decimal128 = new Decimal128();
    Decimal256 decimal256 = new Decimal256();

    @Test
    public void testAllValueTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.BYTE);
            valueTypes.add(ColumnType.SHORT);
            valueTypes.add(ColumnType.CHAR);
            valueTypes.add(ColumnType.INT);
            valueTypes.add(ColumnType.LONG);
            valueTypes.add(ColumnType.FLOAT);
            valueTypes.add(ColumnType.DOUBLE);
            valueTypes.add(ColumnType.BOOLEAN);
            valueTypes.add(ColumnType.DATE);
            valueTypes.add(ColumnType.TIMESTAMP);
            valueTypes.add(ColumnType.getGeoHashTypeWithBits(20));
            valueTypes.add(ColumnType.LONG256);
            valueTypes.add(ColumnType.UUID);
            valueTypes.add(ColumnType.getDecimalType(2, 0)); // DECIMAL8
            valueTypes.add(ColumnType.getDecimalType(4, 0)); // DECIMAL16
            valueTypes.add(ColumnType.getDecimalType(8, 0)); // DECIMAL32
            valueTypes.add(ColumnType.getDecimalType(16, 0)); // DECIMAL64
            valueTypes.add(ColumnType.getDecimalType(32, 0)); // DECIMAL128
            valueTypes.add(ColumnType.getDecimalType(64, 0)); // DECIMAL256

            try (Unordered8Map map = new Unordered8Map(ColumnType.DATE, valueTypes, 64, 0.8, 24)) {
                final int N = 100;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putDate(rnd.nextLong());

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());

                    value.putByte(0, rnd.nextByte());
                    value.putShort(1, rnd.nextShort());
                    value.putChar(2, rnd.nextChar());
                    value.putInt(3, rnd.nextInt());
                    value.putLong(4, rnd.nextLong());
                    value.putFloat(5, rnd.nextFloat());
                    value.putDouble(6, rnd.nextDouble());
                    value.putBool(7, rnd.nextBoolean());
                    value.putDate(8, rnd.nextLong());
                    value.putTimestamp(9, rnd.nextLong());
                    value.putInt(10, rnd.nextInt());
                    Long256Impl long256 = new Long256Impl();
                    long256.fromRnd(rnd);
                    value.putLong256(11, long256);
                    value.putLong128(12, rnd.nextLong(), rnd.nextLong());
                    value.putByte(13, rnd.nextByte());
                    value.putShort(14, rnd.nextShort());
                    value.putInt(15, rnd.nextInt());
                    value.putLong(16, rnd.nextLong());
                    decimal128.ofRaw(
                            rnd.nextLong(),
                            rnd.nextLong()
                    );
                    value.putDecimal128(17, decimal128);
                    decimal256.ofRaw(
                            rnd.nextLong(),
                            rnd.nextLong(),
                            rnd.nextLong(),
                            rnd.nextLong()
                    );
                    value.putDecimal256(18, decimal256);
                }

                rnd.reset();

                // assert that all values are good
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putDate(rnd.nextLong());

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());

                    Assert.assertEquals(rnd.nextByte(), value.getByte(0));
                    Assert.assertEquals(rnd.nextShort(), value.getShort(1));
                    Assert.assertEquals(rnd.nextChar(), value.getChar(2));
                    Assert.assertEquals(rnd.nextInt(), value.getInt(3));
                    Assert.assertEquals(rnd.nextLong(), value.getLong(4));
                    Assert.assertEquals(rnd.nextFloat(), value.getFloat(5), 0.000000001f);
                    Assert.assertEquals(rnd.nextDouble(), value.getDouble(6), 0.000000001d);
                    Assert.assertEquals(rnd.nextBoolean(), value.getBool(7));
                    Assert.assertEquals(rnd.nextLong(), value.getDate(8));
                    Assert.assertEquals(rnd.nextLong(), value.getTimestamp(9));
                    Assert.assertEquals(rnd.nextInt(), value.getInt(10));
                    Long256Impl long256 = new Long256Impl();
                    long256.fromRnd(rnd);
                    Assert.assertEquals(long256, value.getLong256A(11));
                    Assert.assertEquals(rnd.nextLong(), value.getLong128Lo(12));
                    Assert.assertEquals(rnd.nextLong(), value.getLong128Hi(12));
                    Assert.assertEquals(rnd.nextByte(), value.getDecimal8(13));
                    Assert.assertEquals(rnd.nextShort(), value.getDecimal16(14));
                    Assert.assertEquals(rnd.nextInt(), value.getDecimal32(15));
                    Assert.assertEquals(rnd.nextLong(), value.getDecimal64(16));
                    value.getDecimal128(17, decimal128);
                    Assert.assertEquals(rnd.nextLong(), decimal128.getHigh());
                    Assert.assertEquals(rnd.nextLong(), decimal128.getLow());
                    value.getDecimal256(18, decimal256);
                    Assert.assertEquals(rnd.nextLong(), decimal256.getHh());
                    Assert.assertEquals(rnd.nextLong(), decimal256.getHl());
                    Assert.assertEquals(rnd.nextLong(), decimal256.getLh());
                    Assert.assertEquals(rnd.nextLong(), decimal256.getLl());
                }

                try (RecordCursor cursor = map.getCursor()) {
                    HashMap<Long, Long> keyToRowIds = new HashMap<>();
                    LongList rowIds = new LongList();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        // key part, comes after value part in records
                        long key = record.getDate(19);
                        keyToRowIds.put(key, record.getRowId());
                        rowIds.add(record.getRowId());
                    }

                    // Validate that we get the same sequence after toTop.
                    cursor.toTop();
                    int i = 0;
                    while (cursor.hasNext()) {
                        long key = record.getDate(19);
                        Assert.assertEquals((long) keyToRowIds.get(key), record.getRowId());
                        Assert.assertEquals(rowIds.getQuick(i++), record.getRowId());
                    }

                    // Validate that recordAt jumps to what we previously inserted.
                    rnd.reset();
                    for (i = 0; i < N; i++) {
                        long key = rnd.nextLong();
                        long rowId = keyToRowIds.get(key);
                        cursor.recordAt(record, rowId);

                        // value part, it comes first in record
                        int col = 0;
                        Assert.assertEquals(rnd.nextByte(), record.getByte(col++));
                        Assert.assertEquals(rnd.nextShort(), record.getShort(col++));
                        Assert.assertEquals(rnd.nextChar(), record.getChar(col++));
                        Assert.assertEquals(rnd.nextInt(), record.getInt(col++));
                        Assert.assertEquals(rnd.nextLong(), record.getLong(col++));
                        Assert.assertEquals(rnd.nextFloat(), record.getFloat(col++), 0.000000001f);
                        Assert.assertEquals(rnd.nextDouble(), record.getDouble(col++), 0.000000001d);
                        Assert.assertEquals(rnd.nextBoolean(), record.getBool(col++));
                        Assert.assertEquals(rnd.nextLong(), record.getDate(col++));
                        Assert.assertEquals(rnd.nextLong(), record.getTimestamp(col++));
                        Assert.assertEquals(rnd.nextInt(), record.getInt(col++));
                        Long256Impl long256 = new Long256Impl();
                        long256.fromRnd(rnd);
                        Assert.assertEquals(long256, record.getLong256A(col++));
                        Assert.assertEquals(rnd.nextLong(), record.getLong128Lo(col));
                        Assert.assertEquals(rnd.nextLong(), record.getLong128Hi(col++));
                        Assert.assertEquals(rnd.nextByte(), record.getDecimal8(col++));
                        Assert.assertEquals(rnd.nextShort(), record.getDecimal16(col++));
                        Assert.assertEquals(rnd.nextInt(), record.getDecimal32(col++));
                        Assert.assertEquals(rnd.nextLong(), record.getDecimal64(col++));
                        record.getDecimal128(col++, decimal128);
                        Assert.assertEquals(rnd.nextLong(), decimal128.getHigh());
                        Assert.assertEquals(rnd.nextLong(), decimal128.getLow());
                        record.getDecimal256(col, decimal256);
                        Assert.assertEquals(rnd.nextLong(), decimal256.getHh());
                        Assert.assertEquals(rnd.nextLong(), decimal256.getHl());
                        Assert.assertEquals(rnd.nextLong(), decimal256.getLh());
                        Assert.assertEquals(rnd.nextLong(), decimal256.getLl());
                    }
                }
            }
        });
    }

    @Test
    public void testClearOnLazyMapDoesNotCrash() throws Exception {
        // A lazily constructed map (openOnInit=false) keeps memStart == 0 until reopen(),
        // so clear() must skip the backing memset instead of writing to address 0.
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);
            try (Unordered8Map map = new Unordered8Map(ColumnType.LONG, valueTypes, 64, 0.8, 24, false)) {
                Assert.assertFalse(map.isOpen());
                map.clear();
                Assert.assertFalse(map.isOpen());
                Assert.assertEquals(0, map.size());

                // The map remains usable after a real reopen().
                map.reopen();
                Assert.assertTrue(map.isOpen());
                MapKey key = map.withKey();
                key.putLong(42);
                MapValue value = key.createValue();
                Assert.assertTrue(value.isNew());
                value.putLong(0, 1);
                Assert.assertEquals(1, map.size());

                map.clear();
                Assert.assertEquals(0, map.size());
            }
        });
    }

    @Test
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            HashMap<Long, Long> oracle = new HashMap<>();
            try (Unordered8Map map = new Unordered8Map(ColumnType.LONG, valueTypes, 64, 0.8, Integer.MAX_VALUE)) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    long l = rnd.nextLong();
                    key.putLong(l);

                    MapValue value = key.createValue();
                    value.putLong(0, l);

                    oracle.put(l, l);
                }

                Assert.assertEquals(oracle.size(), map.size());

                // assert map contents
                for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                    MapKey key = map.withKey();
                    key.putLong(e.getKey());

                    MapValue value = key.findValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals((long) e.getKey(), value.getLong(0));
                    Assert.assertEquals((long) e.getValue(), value.getLong(0));
                }
            }
        });
    }

    @Test
    public void testProbeViewConcurrentProbes() throws Exception {
        // The point of the view: many threads read one frozen map at once. The map's own MapKey
        // cannot serve this, since it holds one staged key and hands every caller the same value
        // flyweight.
        TestUtils.assertMemoryLeak(() -> {
            final int keyCount = 10_000;
            final int workerCount = 4;
            try (Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1024)) {
                fillLongKeys(map, 0, keyCount);

                final ExecutorService executor = Executors.newFixedThreadPool(workerCount);
                final CountDownLatch start = new CountDownLatch(1);
                final List<Future<?>> futures = new ArrayList<>();
                try {
                    for (int worker = 0; worker < workerCount; worker++) {
                        final int shift = worker;
                        // The owner allocates every view before publication.
                        final Unordered8Map.ProbeView a = new Unordered8Map.ProbeView().of(map);
                        final Unordered8Map.ProbeView b = new Unordered8Map.ProbeView().of(map);
                        futures.add(executor.submit(() -> {
                            try {
                                // Task submission and the latch publish the filled map.
                                start.await();
                                for (int i = 0; i < keyCount; i++) {
                                    final int key = (i + shift * 997) % keyCount;
                                    final int otherKey = (key + 1) % keyCount;
                                    a.withKey().putLong(key);
                                    b.withKey().putLong(otherKey);
                                    MapValue hit = a.findValue();
                                    Assert.assertNotNull(hit);
                                    Assert.assertEquals(otherKey, b.findValue().getLong(0));
                                    // Another view's flyweight does not overwrite this one.
                                    Assert.assertEquals(key, hit.getLong(0));
                                    b.withKey().putLong(keyCount + key);
                                    Assert.assertNull(b.findValue());
                                    Assert.assertEquals(key, hit.getLong(0));
                                }
                            } finally {
                                a.close();
                                b.close();
                            }
                            return null;
                        }));
                    }
                    start.countDown();
                    for (Future<?> future : futures) {
                        future.get(60, TimeUnit.SECONDS);
                    }
                } finally {
                    start.countDown();
                    executor.shutdownNow();
                    // Do not release native backing until readers have stopped even on failure.
                    Assert.assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));
                }
            }
        });
    }

    @Test
    public void testProbeViewHitsAndMisses() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 10_000;
            try (
                    Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1024);
                    Unordered8Map.ProbeView view = new Unordered8Map.ProbeView()
            ) {
                fillLongKeys(map, 0, N);
                view.of(map);
                for (int i = 0; i < N; i++) {
                    view.withKey().putLong(i);
                    MapValue value = view.findValue();
                    Assert.assertNotNull(value);
                    Assert.assertEquals(i, value.getLong(0));
                }
                for (int i = N; i < N + 100; i++) {
                    view.withKey().putLong(i);
                    Assert.assertNull(view.findValue());
                }
                // A negative key is an ordinary entry, and so is Long.MIN_VALUE, the NULL LONG.
                view.withKey().putLong(Numbers.LONG_NULL);
                Assert.assertNull(view.findValue());
                MapKey key = map.withKey();
                key.putLong(Numbers.LONG_NULL);
                key.createValue().putLong(0, -7);
                view.of(map).withKey().putLong(Numbers.LONG_NULL);
                Assert.assertEquals(-7, view.findValue().getLong(0));
            }
        });
    }

    @Test
    public void testProbeViewRejectsAClosedMap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1024, false);
                    Unordered8Map.ProbeView view = new Unordered8Map.ProbeView()
            ) {
                try {
                    view.of(map);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "map probe view needs an open map");
                }
                map.reopen();
                fillLongKeys(map, 0, 8);
                view.of(map).withKey().putLong(3);
                Assert.assertEquals(3, view.findValue().getLong(0));
            }
        });
    }

    @Test
    public void testProbeViewRejectsAnIncompatibleLayout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Unordered8Map narrow = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1024);
                    Unordered8Map wide = new Unordered8Map(ColumnType.LONG, new ArrayColumnTypes().add(ColumnType.LONG).add(ColumnType.INT), 64, 0.5, 1024);
                    Unordered8Map.ProbeView view = new Unordered8Map.ProbeView().of(narrow)
            ) {
                try {
                    view.of(wide);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "map probe view is bound to a different value layout");
                }
            }
        });
    }

    @Test
    public void testProbeViewRejectsUnsupportedPuts() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1024);
                    Unordered8Map.ProbeView view = new Unordered8Map.ProbeView().of(map)
            ) {
                final ObjList<Consumer<? super Unordered8Map.ProbeView>> puts = new ObjList<>();
                puts.add(v -> v.putArray(null));
                puts.add(v -> v.putBin(null));
                puts.add(v -> v.putBool(false));
                puts.add(v -> v.putByte((byte) 0));
                puts.add(v -> v.putChar('a'));
                puts.add(v -> v.putDecimal128(null));
                puts.add(v -> v.putDecimal256(null));
                puts.add(v -> v.putDouble(0));
                puts.add(v -> v.putFloat(0));
                puts.add(v -> v.putIPv4(0));
                puts.add(v -> v.putInt(0));
                puts.add(v -> v.putInterval(null));
                puts.add(v -> v.putLong128(0, 0));
                puts.add(v -> v.putLong256(null));
                puts.add(v -> v.putLong256(0, 0, 0, 0));
                puts.add(v -> v.putShort((short) 0));
                puts.add(v -> v.putStr(null));
                puts.add(v -> v.putStr(null, 0, 0));
                puts.add(v -> v.putVarchar((Utf8Sequence) null));
                puts.add(v -> v.skip(1));
                for (int i = 0, n = puts.size(); i < n; i++) {
                    final int index = i;
                    Assert.assertThrows(
                            "put " + index + " must be rejected on an eight-byte key",
                            UnsupportedOperationException.class,
                            () -> puts.getQuick(index).accept(view)
                    );
                }
            }
        });
    }

    @Test
    public void testProbeViewRetargetsToAnotherMap() throws Exception {
        // Stage once, then probe every map with the same layout. A partitioned build needs
        // exactly this: one key, a lookup per partition.
        TestUtils.assertMemoryLeak(() -> {
            final int N = 1000;
            try (
                    Unordered8Map a = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1024);
                    Unordered8Map b = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1024);
                    Unordered8Map.ProbeView view = new Unordered8Map.ProbeView()
            ) {
                fillLongKeys(a, 0, N);
                for (int i = 0; i < N; i++) {
                    MapKey key = b.withKey();
                    key.putLong(i);
                    key.createValue().putLong(0, i + 1_000_000);
                }
                view.of(a);
                for (int i = 0; i < N; i++) {
                    view.withKey().putLong(i);
                    Assert.assertEquals(i, view.of(a).findValue().getLong(0));
                    // Rebinding leaves the staged key in place.
                    Assert.assertEquals(i + 1_000_000, view.of(b).findValue().getLong(0));
                }
            }
        });
    }

    @Test
    public void testProbeViewReuseAcrossReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1024, false);
                    Unordered8Map.ProbeView view = new Unordered8Map.ProbeView()
            ) {
                map.reopen();
                fillLongKeys(map, 0, 100);
                view.of(map).withKey().putLong(99);
                Assert.assertEquals(99, view.findValue().getLong(0));

                map.close();
                map.reopen();
                fillLongKeys(map, 0, 50);
                view.of(map).withKey().putLong(49);
                Assert.assertEquals(49, view.findValue().getLong(0));
                view.withKey().putLong(99);
                Assert.assertNull("the view reads the reopened map, not the closed one", view.findValue());
            }
        });
    }

    @Test
    public void testProbeViewStagesAndSplitsKeysOverSeveralMaps() throws Exception {
        // A partitioned hash join build stages each key once through a view bound to the layout
        // alone, inserts the raw key into the map that the top bits of its hash select, and probes
        // with a view that picks the map the same way. The zero key takes its own entry in one map.
        TestUtils.assertMemoryLeak(() -> {
            final int n = 1_000;
            final int mapCount = 4;
            final ObjList<Unordered8Map> maps = new ObjList<>();
            final long raw = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try (Unordered8Map.ProbeView stager = new Unordered8Map.ProbeView(); Unordered8Map.ProbeView view = new Unordered8Map.ProbeView()) {
                for (int m = 0; m < mapCount; m++) {
                    maps.add(new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 16, 0.5, Integer.MAX_VALUE, false));
                }
                // The layout binds a map that has not opened.
                stager.ofLayout(maps.getQuick(0));
                for (int m = 0; m < mapCount; m++) {
                    maps.getQuick(m).reopen();
                }
                for (int i = 0; i < n; i++) {
                    stager.withKey().putLong(splitKey(i));
                    Assert.assertEquals(Long.BYTES, stager.copyStagedKey(raw));
                    Assert.assertEquals(Long.BYTES, stager.getStagedKeySize());
                    final Unordered8Map target = maps.getQuick((int) (stager.hash() >>> 62));
                    final MapKey key = target.withRawKey(raw, Long.BYTES);
                    // The map hashes the raw key as the view hashed the staged one.
                    Assert.assertEquals(stager.hash(), key.hash());
                    final MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putLong(0, i);
                }
                long keyCount = 0;
                for (int m = 0; m < mapCount; m++) {
                    final Unordered8Map map = maps.getQuick(m);
                    keyCount += map.size();
                    view.of(map);
                }
                Assert.assertEquals(n, keyCount);
                for (int i = 0; i < n + 100; i++) {
                    view.withKey().putLong(splitKey(i));
                    final int selected = (int) (view.hash() >>> 62);
                    for (int m = 0; m < mapCount; m++) {
                        final MapValue value = view.findValueIn(maps.getQuick(m));
                        if (m == selected && i < n) {
                            Assert.assertNotNull("key " + i, value);
                            Assert.assertEquals(i, value.getLong(0));
                        } else {
                            Assert.assertNull("key " + i + " in map " + m, value);
                        }
                    }
                }
            } finally {
                Unsafe.free(raw, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Misc.freeObjList(maps);
            }
        });
    }

    @Test
    public void testProbeViewZeroKey() throws Exception {
        // Zero marks an empty slot, so the zero key lives in its own entry past the hash table.
        // The view has to snapshot both that entry and the flag that says whether it is live.
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1024);
                    Unordered8Map.ProbeView view = new Unordered8Map.ProbeView()
            ) {
                fillLongKeys(map, 1, 10);
                view.of(map);
                view.withKey().putLong(0);
                Assert.assertNull("the map holds no zero key yet", view.findValue());
                view.withKey().putLong(5);
                Assert.assertEquals(5, view.findValue().getLong(0));

                MapKey key = map.withKey();
                key.putLong(0);
                MapValue value = key.createValue();
                Assert.assertTrue(value.isNew());
                value.putLong(0, 42);

                view.of(map).withKey().putLong(5);
                Assert.assertEquals(5, view.findValue().getLong(0));
                // withKey() has to clear the staged key, otherwise the next probe reuses it.
                view.withKey();
                Assert.assertEquals("withKey() resets the staged key to zero", 42, view.findValue().getLong(0));
            }
        });
    }

    @Test
    public void testPutBinUnsupported() throws Exception {
        assertUnsupported(key -> key.putBin(null));
    }

    @Test
    public void testPutDecimal128Unsupported() throws Exception {
        assertUnsupported(key -> key.putDecimal128(null));
    }

    @Test
    public void testPutDecimal256Unsupported() throws Exception {
        assertUnsupported(key -> key.putDecimal256(null));
    }

    @Test
    public void testPutDoubleUnsupported() throws Exception {
        assertUnsupported(key -> key.putDouble(0.0));
    }

    @Test
    public void testPutLong128Unsupported() throws Exception {
        assertUnsupported(key -> key.putLong128(0, 0));
    }

    @Test
    public void testPutLong256ObjectUnsupported() throws Exception {
        assertUnsupported(key -> key.putLong256(null));
    }

    @Test
    public void testPutLong256ValuesUnsupported() throws Exception {
        assertUnsupported(key -> key.putLong256(0, 0, 0, 0));
    }

    @Test
    public void testPutStrRangeUnsupported() throws Exception {
        assertUnsupported(key -> key.putStr(null, 0, 0));
    }

    @Test
    public void testPutStrUnsupported() throws Exception {
        assertUnsupported(key -> key.putStr(null));
    }

    @Test
    public void testPutVarcharUnsupported() throws Exception {
        assertUnsupported(key -> key.putVarchar((Utf8Sequence) null));
    }

    @Test
    public void testSingleZeroKey() {
        try (Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 16, 0.8, 24)) {
            MapKey key = map.withKey();
            key.putLong(0);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 42);

            try (RecordCursor cursor = map.getCursor()) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong(1));
                Assert.assertEquals(42, record.getLong(0));

                // Validate that we get the same sequence after toTop.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong(1));
                Assert.assertEquals(42, record.getLong(0));
            }
        }
    }

    @Test
    public void testTopK() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int heapCapacity = 5;
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered8Map map = new Unordered8Map(ColumnType.TIMESTAMP, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    DirectLongLongSortedList list = new DirectLongLongAscList(heapCapacity, MemoryTag.NATIVE_DEFAULT)
            ) {
                for (int i = 0; i < 100; i++) {
                    MapKey key = map.withKey();
                    key.putTimestamp(i);

                    MapValue value = key.createValue();
                    value.putLong(0, i);
                }

                MapRecordCursor mapCursor = map.getCursor();
                mapCursor.longTopK(list, LongColumn.newInstance(0));

                Assert.assertEquals(heapCapacity, list.size());

                MapRecord mapRecord = mapCursor.getRecord();
                DirectLongLongSortedList.Cursor heapCursor = list.getCursor();
                for (int i = 0; i < heapCapacity; i++) {
                    Assert.assertTrue(heapCursor.hasNext());
                    mapCursor.recordAt(mapRecord, heapCursor.index());
                    Assert.assertEquals(heapCursor.value(), mapRecord.getLong(0));
                }
            }
        });
    }

    @Test
    public void testTwoKeysIncludingZero() {
        try (Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 16, 0.8, 24)) {
            MapKey key = map.withKey();
            key.putLong(0);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 0);

            key = map.withKey();
            key.putLong(1);
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 1);

            try (RecordCursor cursor = map.getCursor()) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getLong(1));
                Assert.assertEquals(1, record.getLong(0));
                // Zero is always last when iterating.
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong(1));
                Assert.assertEquals(0, record.getLong(0));

                // Validate that we get the same sequence after toTop.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getLong(1));
                Assert.assertEquals(1, record.getLong(0));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong(1));
                Assert.assertEquals(0, record.getLong(0));
            }
        }
    }

    @Test
    public void testUnsupportedKeyTypes() throws Exception {
        short[] columnTypes = new short[]{
                ColumnType.BINARY,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.LONG128,
                ColumnType.DOUBLE,
                ColumnType.UUID,
                ColumnType.LONG256,
                ColumnType.DECIMAL128,
                ColumnType.DECIMAL256,
        };
        for (short columnType : columnTypes) {
            TestUtils.assertMemoryLeak(() -> {
                try (Unordered8Map ignore = new Unordered8Map(columnType, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "unexpected key type"));
                }
            });
        }
    }

    private static void assertUnsupported(Consumer<? super MapKey> putKeyFn) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Unordered8Map map = new Unordered8Map(ColumnType.TIMESTAMP, new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
                MapKey key = map.withKey();
                try {
                    putKeyFn.accept(key);
                    Assert.fail();
                } catch (UnsupportedOperationException e) {
                    Assert.assertTrue(true);
                }
            }
        });
    }

    // The zero key first, NULL next, then keys either side of zero.
    private static long splitKey(int i) {
        return i == 0 ? 0 : i == 1 ? Numbers.LONG_NULL : (i % 2 == 0 ? i : -i) * 1_000_003L;
    }

    private static void fillLongKeys(Unordered8Map map, int from, int to) {
        for (int i = from; i < to; i++) {
            MapKey key = map.withKey();
            key.putLong(i);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, i);
        }
    }
}
