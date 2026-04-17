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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.MapValueMergeFunction;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.std.Chars;
import io.questdb.std.DirectLongList;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestDirectUtf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

@RunWith(Parameterized.class)
public class MapTest extends AbstractCairoTest {
    private static DirectUtf8Sink STABLE_SINK;
    private final MapType mapType;
    private Rnd rnd;

    public MapTest(MapType mapType) {
        this.mapType = mapType;
    }

    @AfterClass
    public static void closeSink() {
        STABLE_SINK.close();
    }

    @BeforeClass
    public static void createSink() {
        STABLE_SINK = new DirectUtf8Sink(100 * 1024 * 1024);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {MapType.ORDERED_MAP},
                {MapType.UNORDERED_4_MAP},
                {MapType.UNORDERED_8_MAP},
                {MapType.UNORDERED_VARCHAR_MAP},
        });
    }

    @After
    public void clearSink() {
        STABLE_SINK.clear();
    }

    @Before
    public void setupRnd() {
        rnd = TestUtils.generateRandom(null);
    }

    @Test
    public void testAllValueTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            ColumnTypes keyTypes = keyColumnType(ColumnType.INT);

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

            try (Map map = createMap(keyTypes, valueTypes, 64, 0.8, 24)) {
                final int N = 10000;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, rnd.nextInt());

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
                }

                rnd.reset();

                // assert that all values are good
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, rnd.nextInt());

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
                }

                try (RecordCursor cursor = map.getCursor()) {
                    HashMap<String, Long> keyToRowIds = new HashMap<>();
                    LongList rowIds = new LongList();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        // key part, comes after value part in records
                        int in = readKey(record, 13);
                        String key = String.valueOf(in);
                        keyToRowIds.put(key, record.getRowId());
                        rowIds.add(record.getRowId());
                    }

                    // Validate that we get the same sequence after toTop.
                    cursor.toTop();
                    int i = 0;
                    while (cursor.hasNext()) {
                        int in = readKey(record, 13);
                        String key = String.valueOf(in);
                        Assert.assertEquals((long) keyToRowIds.get(key), record.getRowId());
                        Assert.assertEquals(rowIds.getQuick(i++), record.getRowId());
                    }

                    // Validate that recordAt jumps to what we previously inserted.
                    rnd.reset();
                    for (i = 0; i < N; i++) {
                        int in = rnd.nextInt();
                        String key = String.valueOf(in);
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
                        Assert.assertEquals(rnd.nextLong(), record.getLong128Hi(col));
                    }
                }
            }
        });
    }

    @Test
    public void testBatchAddressableRejectsOversizedHeap() throws Exception {
        // OrderedMap's MAX_HEAP_SIZE already sits below BATCH_OFFSET_MASK, so it
        // does not carry a construction-time guard.
        Assume.assumeTrue(mapType != MapType.ORDERED_MAP);
        TestUtils.assertMemoryLeak(() -> {
            // 64 x LONG256 = 2048 bytes of value per entry. At the map's
            // post-ceilPow2 cap of 2^30 keys, entrySize * keyCapacity exceeds
            // the 39-bit BATCH_OFFSET_MASK, so the guard must fail construction
            // before any malloc.
            final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            for (int i = 0; i < 64; i++) {
                valueTypes.add(ColumnType.LONG256);
            }
            try {
                // keyCapacity = 2^29 with loadFactor 0.5 drives post-ceilPow2 keyCapacity to 2^30,
                // the map's hard cap.
                createMap(keyColumnType(ColumnType.INT), valueTypes, 1 << 29, 0.5, Integer.MAX_VALUE);
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "exceeds batched probe addressable range");
            }
        });
    }

    @Test
    public void testClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), N / 2, 0.5f, 100)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());

                map.clear();

                Assert.assertEquals(0, map.size());

                // Fill the map once again and verify contents.
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(N + i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());
            }
        });
    }

    @Test
    public void testCollisionPerformance() throws Exception {
        Assume.assumeFalse(mapType == MapType.UNORDERED_VARCHAR_MAP);
        TestUtils.assertMemoryLeak(() -> {
            // These used to be default FastMap configuration for a join
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG), 2097152, 0.5, 10000)) {
                for (int i = 0; i < 40_000_000; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, (i + 15) % 151269);

                    MapValue value = key.createValue();
                    value.putLong(0, i);
                }
            }
        });
    }

    @Test
    public void testCopyValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = keyColumnType(ColumnType.INT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Map mapA = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    Map mapB = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                final int N = 10000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    populateKey(keyA, i);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putLong(0, i);
                }

                RecordCursor cursorA = mapA.getCursor();
                MapRecord recordA = mapA.getRecord();
                while (cursorA.hasNext()) {
                    MapKey keyB = mapB.withKey();
                    recordA.copyToKey(keyB);
                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    recordA.copyValue(valueB);
                }

                Assert.assertEquals(mapA.size(), mapB.size());

                // assert that all map A keys and values are in map B
                cursorA.toTop();
                while (cursorA.hasNext()) {
                    MapValue valueA = recordA.getValue();

                    MapKey keyB = mapB.withKey();
                    recordA.copyToKey(keyB);
                    MapValue valueB = keyB.findValue();

                    Assert.assertFalse(valueB.isNew());
                    Assert.assertEquals(valueA.getLong(0), valueB.getLong(0));
                }
            }
        });
    }

    @Test
    public void testKeyCopyFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = keyColumnType(ColumnType.IPv4);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.INT);

            try (
                    Map mapA = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    Map mapB = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                final int N = 10000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    populateKey(keyA, i);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putInt(0, i + 1);

                    MapKey keyB = mapB.withKey();
                    keyB.copyFrom(keyA);

                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    valueB.putInt(0, i + 1);
                }

                Assert.assertEquals(mapA.size(), mapB.size());

                // assert that all map A keys can be found in map B
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    populateKey(keyA, i);

                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i);

                    MapValue valueA = keyA.findValue();
                    Assert.assertFalse(valueA.isNew());

                    MapValue valueB = keyB.findValue();
                    Assert.assertFalse(valueB.isNew());

                    Assert.assertEquals(i + 1, valueA.getInt(0));
                    Assert.assertEquals(valueA.getInt(0), valueB.getInt(0));
                }
            }
        });
    }

    @Test
    public void testKeyHashCode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), 64, 0.8, 24)) {
                final int N = 100000;
                final LongList keyHashCodes = new LongList(N);
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);
                    key.commit();
                    long hashCode = key.hash();
                    keyHashCodes.add(hashCode);

                    MapValue value = key.createValue(hashCode);
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i + 2);
                }

                final LongList recordHashCodes = new LongList(N);
                RecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                while (cursor.hasNext()) {
                    recordHashCodes.add(record.keyHashCode());
                }

                keyHashCodes.sort();
                recordHashCodes.sort();
                TestUtils.assertEquals(keyHashCodes, recordHashCodes);
            }
        });
    }

    @Test
    public void testKeyOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ColumnTypes types = keyColumnType(ColumnType.INT);

            final int N = 10000;
            try (Map map = createMap(types, null, 64, 0.5, Integer.MAX_VALUE)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);
                    MapValue values = key.createValue();
                    Assert.assertTrue(values.isNew());
                }

                int[] cnts = new int[N];
                try (RecordCursor cursor = map.getCursor()) {
                    final MapRecord record = (MapRecord) cursor.getRecord();
                    while (cursor.hasNext()) {
                        int index;
                        if (mapType == MapType.UNORDERED_VARCHAR_MAP) {
                            index = Numbers.parseInt(record.getVarcharA(0));
                        } else {
                            index = record.getInt(0);
                        }
                        cnts[index]++;
                    }
                }

                for (int i = 0; i < cnts.length; i++) {
                    Assert.assertEquals("count on index " + i + " is " + cnts[i], 1, cnts[i]);
                }
            }
        });
    }

    @Test(expected = LimitOverflowException.class)
    public void testMaxResizes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG256), 64, 0.8, 1)) {
                for (int i = 0; i < 100000; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);
                    key.createValue();
                }
            }
        });
    }

    @Test
    public void testMergeIntersectingMaps() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = keyColumnType(ColumnType.INT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.INT);

            try (
                    Map mapA = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    Map mapB = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    populateKey(keyA, i);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putInt(0, i);
                }

                for (int i = 0; i < 2 * N; i++) {
                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i);

                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    valueB.putInt(0, i);
                }

                Assert.assertEquals(2 * mapA.size(), mapB.size());

                mapA.merge(mapB, new TestMapValueMergeFunction());

                Assert.assertEquals(mapA.size(), mapB.size());

                // assert that all map B keys can be found in map A
                RecordCursor cursorA = mapA.getCursor();
                MapRecord recordA = mapA.getRecord();
                while (cursorA.hasNext()) {
                    int i = readKey(recordA, 1);
                    MapValue valueA = recordA.getValue();

                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i);
                    MapValue valueB = keyB.findValue();

                    Assert.assertFalse(valueB.isNew());
                    if (i < N) {
                        Assert.assertEquals(valueA.getInt(0), 2 * valueB.getInt(0));
                    } else {
                        Assert.assertEquals(valueA.getInt(0), valueB.getInt(0));
                    }
                }
            }
        });
    }

    @Test
    public void testMergeNonIntersectingMaps() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = keyColumnType(ColumnType.INT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.INT);

            try (
                    Map mapA = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    Map mapB = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    populateKey(keyA, i);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putInt(0, i);
                }

                for (int i = N; i < 2 * N; i++) {
                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i);

                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    valueB.putInt(0, i);
                }

                Assert.assertEquals(mapA.size(), mapB.size());

                mapA.merge(mapB, new TestMapValueMergeFunction());

                Assert.assertEquals(mapA.size(), 2 * mapB.size());

                // assert that map A contains all map B keys and its initial keys
                RecordCursor cursorA = mapA.getCursor();
                MapRecord recordA = mapA.getRecord();
                while (cursorA.hasNext()) {
                    int i = readKey(recordA, 1);
                    MapValue valueA = recordA.getValue();

                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i);
                    MapValue valueB = keyB.findValue();

                    if (i < N) {
                        Assert.assertNull(valueB);
                    } else {
                        Assert.assertFalse(valueB.isNew());
                        Assert.assertEquals(valueA.getInt(0), valueB.getInt(0));
                    }
                }
            }
        });
    }

    @Test
    public void testMergeStressTest() throws Exception {
        // Here we aim to resize map A as many times as possible to catch
        // possible bugs with append address initialization.
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = keyColumnType(ColumnType.INT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.INT);

            try (
                    Map mapA = createMap(keyTypes, valueTypes, 16, 0.9, Integer.MAX_VALUE);
                    Map mapB = createMap(keyTypes, valueTypes, 16, 0.9, Integer.MAX_VALUE)
            ) {
                final int N = 100;
                final int M = 1000;
                for (int i = 0; i < N; i++) {
                    mapB.clear();
                    for (int j = 0; j < M; j++) {
                        MapKey keyB = mapB.withKey();
                        populateKey(keyB, M * i + j);

                        MapValue valueB = keyB.createValue();
                        Assert.assertTrue(valueB.isNew());
                        valueB.putInt(0, M * i + j);
                    }

                    mapA.merge(mapB, new TestMapValueMergeFunction());
                    Assert.assertEquals((i + 1) * M, mapA.size());
                }
            }
        });
    }

    @Test
    public void testProbeBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 6 keys, 4 distinct: first occurrence is new, duplicates are not.
            final int[] logicalKeys = {10, 20, 30, 10, 20, 40};
            final boolean[] expectedIsNew = {true, true, true, false, false, true};
            final int batchRows = logicalKeys.length;

            try (
                    Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG), 64, 0.8, 24);
                    DirectLongList batch = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    TestPageFrameRecord testRecord = new TestPageFrameRecord(mapType, logicalKeys)
            ) {
                batch.setPos(batchRows);
                final RecordSink sink = new TestRecordSink(columnTypeForMapType(), -1);

                map.reserveCapacity(batchRows);
                final long entryBase = map.probeBatch(testRecord, sink, 0, batchRows, batch.getAddress());
                Assert.assertEquals(4, map.size());

                for (int i = 0; i < batchRows; i++) {
                    final long encoded = Unsafe.getUnsafe().getLong(batch.getAddress() + ((long) i << 3));
                    Assert.assertEquals(i, Map.decodeBatchRowIndex(encoded));
                    Assert.assertEquals(expectedIsNew[i], Map.isNewBatchEntry(encoded));

                    // Decoded offset must land inside the map's entry storage.
                    final long offset = Map.decodeBatchOffset(encoded);
                    final MapValue valueAt = map.valueAt(entryBase + offset);
                    Assert.assertNotNull(valueAt);
                }

                assertMapContainsKeys(map, new int[]{10, 20, 30, 40});
            }
        });
    }

    @Test
    public void testProbeBatchFiltered() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 7 frame rows, filter selects 4 of them by frame-relative row id.
            final int[] logicalKeys = {10, 20, 30, 40, 50, 10, 20};
            final long[] selectedRowIds = {1, 3, 5, 6};
            // Post-filter: 20 (new), 40 (new), 10 (new), 20 (dup)
            final boolean[] expectedIsNew = {true, true, true, false};
            final int batchRows = selectedRowIds.length;

            try (
                    Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG), 64, 0.8, 24);
                    DirectLongList rowIds = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    DirectLongList batch = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    TestPageFrameRecord testRecord = new TestPageFrameRecord(mapType, logicalKeys)
            ) {
                for (long rowId : selectedRowIds) {
                    rowIds.add(rowId);
                }
                batch.setPos(batchRows);
                final RecordSink sink = new TestRecordSink(columnTypeForMapType(), -1);

                map.reserveCapacity(batchRows);
                final long entryBase = map.probeBatchFiltered(
                        testRecord, sink, rowIds.getAddress(), 0, batchRows, batch.getAddress()
                );
                Assert.assertEquals(3, map.size());

                for (int i = 0; i < batchRows; i++) {
                    final long encoded = Unsafe.getUnsafe().getLong(batch.getAddress() + ((long) i << 3));
                    // Encoded rowIndex is the frame-relative row id, not the position in the filter list.
                    Assert.assertEquals(selectedRowIds[i], Map.decodeBatchRowIndex(encoded));
                    Assert.assertEquals(expectedIsNew[i], Map.isNewBatchEntry(encoded));

                    final long offset = Map.decodeBatchOffset(encoded);
                    Assert.assertNotNull(map.valueAt(entryBase + offset));
                }

                assertMapContainsKeys(map, new int[]{10, 20, 40});
            }
        });
    }

    @Test
    public void testProbeBatchFilteredOrderedMapVarSizeKey() throws Exception {
        // Exercises OrderedMap.probeBatchFilteredVarSize (keySize == -1).
        Assume.assumeTrue(mapType == MapType.ORDERED_MAP);
        TestUtils.assertMemoryLeak(() -> {
            final int[] logicalKeys = {10, 20, 30, 40, 50, 10, 20};
            final long[] selectedRowIds = {1, 3, 5, 6};
            final boolean[] expectedIsNew = {true, true, true, false};
            final int batchRows = selectedRowIds.length;

            try (
                    Map map = new OrderedMap(
                            32 * 1024,
                            new SingleColumnType(ColumnType.VARCHAR),
                            new SingleColumnType(ColumnType.LONG),
                            64,
                            0.8,
                            24
                    );
                    DirectLongList rowIds = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    DirectLongList batch = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    TestPageFrameRecord testRecord = new TestPageFrameRecord(MapType.UNORDERED_VARCHAR_MAP, logicalKeys)
            ) {
                for (long rowId : selectedRowIds) {
                    rowIds.add(rowId);
                }
                batch.setPos(batchRows);
                final RecordSink sink = new TestRecordSink(ColumnType.VARCHAR, -1);

                map.reserveCapacity(batchRows);
                final long entryBase = map.probeBatchFiltered(
                        testRecord, sink, rowIds.getAddress(), 0, batchRows, batch.getAddress()
                );
                Assert.assertEquals(3, map.size());

                for (int i = 0; i < batchRows; i++) {
                    final long encoded = Unsafe.getUnsafe().getLong(batch.getAddress() + ((long) i << 3));
                    Assert.assertEquals(selectedRowIds[i], Map.decodeBatchRowIndex(encoded));
                    Assert.assertEquals(expectedIsNew[i], Map.isNewBatchEntry(encoded));
                    final long offset = Map.decodeBatchOffset(encoded);
                    Assert.assertNotNull(map.valueAt(entryBase + offset));
                }

                final int[] found = new int[(int) map.size()];
                int n = 0;
                try (RecordCursor cursor = map.getCursor()) {
                    final MapRecord record = (MapRecord) cursor.getRecord();
                    while (cursor.hasNext()) {
                        found[n++] = Numbers.parseInt(record.getVarcharA(1));
                    }
                }
                Arrays.sort(found);
                Assert.assertArrayEquals(new int[]{10, 20, 40}, found);
            }
        });
    }

    @Test
    public void testProbeBatchFilteredWithDirectColumnIndex() throws Exception {
        // Exercises the direct-column fast path in Unordered4Map/Unordered8Map/UnorderedVarcharMap.
        Assume.assumeTrue(mapType != MapType.ORDERED_MAP);
        TestUtils.assertMemoryLeak(() -> {
            final int[] logicalKeys = {7, 14, 21, 28, 7, 14, 35};
            final long[] selectedRowIds = {0, 2, 4, 5};
            // Post-filter: 7 (new), 21 (new), 7 (dup of row 0), 14 (new)
            final boolean[] expectedIsNew = {true, true, false, true};
            final int batchRows = selectedRowIds.length;

            try (
                    Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG), 64, 0.8, 24);
                    DirectLongList rowIds = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    DirectLongList batch = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    TestPageFrameRecord testRecord = new TestPageFrameRecord(mapType, logicalKeys)
            ) {
                for (long rowId : selectedRowIds) {
                    rowIds.add(rowId);
                }
                batch.setPos(batchRows);
                final RecordSink sink = new TestRecordSink(columnTypeForMapType(), 0);

                map.reserveCapacity(batchRows);
                final long entryBase = map.probeBatchFiltered(
                        testRecord, sink, rowIds.getAddress(), 0, batchRows, batch.getAddress()
                );
                Assert.assertEquals(3, map.size());

                for (int i = 0; i < batchRows; i++) {
                    final long encoded = Unsafe.getUnsafe().getLong(batch.getAddress() + ((long) i << 3));
                    Assert.assertEquals(selectedRowIds[i], Map.decodeBatchRowIndex(encoded));
                    Assert.assertEquals(expectedIsNew[i], Map.isNewBatchEntry(encoded));
                    final long offset = Map.decodeBatchOffset(encoded);
                    Assert.assertNotNull(map.valueAt(entryBase + offset));
                }

                assertMapContainsKeys(map, new int[]{7, 14, 21});
            }
        });
    }

    @Test
    public void testProbeBatchOrderedMapVarSizeKey() throws Exception {
        // Exercises OrderedMap.probeBatchVarSize (keySize == -1). The parameterized
        // testProbeBatch covers the fixed-size path with an INT key.
        Assume.assumeTrue(mapType == MapType.ORDERED_MAP);
        TestUtils.assertMemoryLeak(() -> {
            final int[] logicalKeys = {10, 20, 30, 10, 20, 40};
            final boolean[] expectedIsNew = {true, true, true, false, false, true};
            final int batchRows = logicalKeys.length;

            try (
                    Map map = new OrderedMap(
                            32 * 1024,
                            new SingleColumnType(ColumnType.VARCHAR),
                            new SingleColumnType(ColumnType.LONG),
                            64,
                            0.8,
                            24
                    );
                    DirectLongList batch = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    TestPageFrameRecord testRecord = new TestPageFrameRecord(MapType.UNORDERED_VARCHAR_MAP, logicalKeys)
            ) {
                batch.setPos(batchRows);
                final RecordSink sink = new TestRecordSink(ColumnType.VARCHAR, -1);

                map.reserveCapacity(batchRows);
                final long entryBase = map.probeBatch(testRecord, sink, 0, batchRows, batch.getAddress());
                Assert.assertEquals(4, map.size());

                for (int i = 0; i < batchRows; i++) {
                    final long encoded = Unsafe.getUnsafe().getLong(batch.getAddress() + ((long) i << 3));
                    Assert.assertEquals(i, Map.decodeBatchRowIndex(encoded));
                    Assert.assertEquals(expectedIsNew[i], Map.isNewBatchEntry(encoded));
                    final long offset = Map.decodeBatchOffset(encoded);
                    Assert.assertNotNull(map.valueAt(entryBase + offset));
                }

                final int[] found = new int[(int) map.size()];
                int n = 0;
                try (RecordCursor cursor = map.getCursor()) {
                    final MapRecord record = (MapRecord) cursor.getRecord();
                    while (cursor.hasNext()) {
                        // Value column is at index 0 (LONG), key column is at index 1 (VARCHAR).
                        found[n++] = Numbers.parseInt(record.getVarcharA(1));
                    }
                }
                Arrays.sort(found);
                Assert.assertArrayEquals(new int[]{10, 20, 30, 40}, found);
            }
        });
    }

    @Test
    public void testProbeBatchWithDirectColumnIndex() throws Exception {
        // OrderedMap never uses the direct-column fast path (it ignores getDirectColumnIndex
        // and always dispatches to the keySize-based paths), so there's nothing extra to verify here.
        Assume.assumeTrue(mapType != MapType.ORDERED_MAP);
        TestUtils.assertMemoryLeak(() -> {
            final int[] logicalKeys = {7, 14, 21, 7, 14};
            final boolean[] expectedIsNew = {true, true, true, false, false};
            final int batchRows = logicalKeys.length;

            try (
                    Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG), 64, 0.8, 24);
                    DirectLongList batch = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    TestPageFrameRecord testRecord = new TestPageFrameRecord(mapType, logicalKeys)
            ) {
                batch.setPos(batchRows);
                final RecordSink sink = new TestRecordSink(columnTypeForMapType(), 0);

                map.reserveCapacity(batchRows);
                final long entryBase = map.probeBatch(testRecord, sink, 0, batchRows, batch.getAddress());
                Assert.assertEquals(3, map.size());

                for (int i = 0; i < batchRows; i++) {
                    final long encoded = Unsafe.getUnsafe().getLong(batch.getAddress() + ((long) i << 3));
                    Assert.assertEquals(i, Map.decodeBatchRowIndex(encoded));
                    Assert.assertEquals(expectedIsNew[i], Map.isNewBatchEntry(encoded));
                    final long offset = Map.decodeBatchOffset(encoded);
                    Assert.assertNotNull(map.valueAt(entryBase + offset));
                }

                assertMapContainsKeys(map, new int[]{7, 14, 21});
            }
        });
    }

    @Test
    public void testReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), N / 2, 0.5f, 100)) {
                Assert.assertTrue(map.isOpen());

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());

                map.close();
                Assert.assertFalse(map.isOpen());
                map.close(); // Close must be idempotent
                map.reopen();
                Assert.assertTrue(map.isOpen());

                Assert.assertEquals(0, map.size());

                // Fill the map once again and verify contents.
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(N + i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());
            }
        });
    }

    @Test
    public void testReopenWithKeyCapacityAndPageSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), N / 2, 0.5f, 100)) {
                final int initialKeyCapacity = map.getKeyCapacity();
                final long initialHeapSize = map.getHeapSize();

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());

                map.close();
                map.reopen(N, N * 128);

                Assert.assertEquals(0, map.size());
                Assert.assertNotEquals(initialKeyCapacity, map.getKeyCapacity());
                if (initialHeapSize != -1) {
                    Assert.assertNotEquals(initialHeapSize, map.getHeapSize());
                }

                // Fill the map once again and verify contents.
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(N + i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());
            }
        });
    }

    @Test
    public void testReserveCapacityStrictContract() throws Exception {
        // OrderedMap inherits the no-op default — only the unordered maps need the
        // strict free > additionalKeys contract. UnorderedVarcharMap.probeBatch
        // routes inserts through asNew, which rehashes when --free == 0; without
        // the strict contract, the last insertion in a batch would reallocate
        // memStart and invalidate offsets already packed into batchAddr.
        Assume.assumeTrue(mapType != MapType.ORDERED_MAP);
        TestUtils.assertMemoryLeak(() -> {
            // keyCapacity=16, loadFactor=0.5: constructor rounds actual capacity up
            // to 32, yielding an initial free of 16.
            final int initialKeyCapacity = 32;
            final int initialFree = 16;
            final int batchRows = 5;
            final int prefill = initialFree - batchRows;

            final int[] batchKeys = new int[batchRows];
            for (int i = 0; i < batchRows; i++) {
                batchKeys[i] = 1_000 + i;
            }

            try (
                    Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG), 16, 0.5, 24);
                    DirectLongList batch = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    TestPageFrameRecord testRecord = new TestPageFrameRecord(mapType, batchKeys)
            ) {
                Assert.assertEquals(initialKeyCapacity, map.getKeyCapacity());

                // Pre-fill so that free == batchRows exactly, hitting the boundary
                // case where the old contract left reserveCapacity as a no-op.
                // Starts at 1 so the zero-key slot in Unordered4Map/Unordered8Map
                // (which doesn't consume a free hash-table slot) stays untouched.
                for (int i = 1; i <= prefill; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);
                    Assert.assertTrue(key.createValue().isNew());
                }
                Assert.assertEquals(prefill, map.size());
                Assert.assertEquals(initialKeyCapacity, map.getKeyCapacity());

                // At free == batchRows, reserveCapacity must rehash to guarantee
                // free > batchRows on return.
                map.reserveCapacity(batchRows);
                final int capAfterReserve = map.getKeyCapacity();
                Assert.assertTrue(capAfterReserve > initialKeyCapacity);

                batch.setPos(batchRows);
                final RecordSink sink = new TestRecordSink(columnTypeForMapType(), -1);
                final long entryBase = map.probeBatch(testRecord, sink, 0, batchRows, batch.getAddress());

                // probeBatch must not trigger a rehash — reserveCapacity already
                // left enough headroom for all batchRows insertions.
                Assert.assertEquals(capAfterReserve, map.getKeyCapacity());
                Assert.assertEquals(prefill + batchRows, map.size());

                for (int i = 0; i < batchRows; i++) {
                    final long encoded = Unsafe.getUnsafe().getLong(batch.getAddress() + ((long) i << 3));
                    Assert.assertTrue(Map.isNewBatchEntry(encoded));
                    final long offset = Map.decodeBatchOffset(encoded);
                    Assert.assertNotNull(map.valueAt(entryBase + offset));
                }
            }
        });
    }

    @Test
    public void testRestoreInitialCapacity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), N / 2, 0.5f, 100)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());

                map.restoreInitialCapacity();

                Assert.assertEquals(0, map.size());

                // Fill the map once again and verify contents.
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(N + i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());
            }
        });
    }

    @Test
    public void testRowIdAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 10;
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), 64, 0.5, 100)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i);
                    MapValue values = key.createValue();
                    Assert.assertTrue(values.isNew());
                    values.putInt(0, i);
                }

                // Iterate map to double the value.
                LongList rowIds = new LongList();
                try (RecordCursor cursor = map.getCursor()) {
                    final MapRecord recordA = (MapRecord) cursor.getRecord();
                    while (cursor.hasNext()) {
                        rowIds.add(recordA.getRowId());
                        MapValue value = recordA.getValue();
                        value.putInt(0, value.getInt(0) * 2);
                    }

                    final MapRecord recordB = (MapRecord) cursor.getRecordB();
                    Assert.assertNotSame(recordB, recordA);

                    for (int i = 0, n = rowIds.size(); i < n; i++) {
                        cursor.recordAt(recordB, rowIds.getQuick(i));
                        int expected = readKey(recordB, 1) * 2;
                        Assert.assertEquals(expected, recordB.getInt(0));
                    }
                }
            }
        });
    }

    @Test
    public void testSetBatchEmptyValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long sentinel = 0xDEAD_BEEFL;
            final int[] logicalKeys = {5, 15, 5, 25};
            final boolean[] expectedIsNew = {true, true, false, true};
            final int batchRows = logicalKeys.length;

            try (
                    Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG), 64, 0.8, 24);
                    DirectLongList batch = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    TestPageFrameRecord testRecord = new TestPageFrameRecord(mapType, logicalKeys)
            ) {
                batch.setPos(batchRows);
                final RecordSink sink = new TestRecordSink(columnTypeForMapType(), -1);

                map.setBatchEmptyValue(new TestFunctionsUpdater(sentinel));
                map.reserveCapacity(batchRows);
                final long entryBase = map.probeBatch(testRecord, sink, 0, batchRows, batch.getAddress());

                Assert.assertEquals(3, map.size());
                for (int i = 0; i < batchRows; i++) {
                    final long encoded = Unsafe.getUnsafe().getLong(batch.getAddress() + ((long) i << 3));
                    Assert.assertEquals(expectedIsNew[i], Map.isNewBatchEntry(encoded));
                    final long offset = Map.decodeBatchOffset(encoded);
                    // Newly inserted entries were prefilled with the sentinel via setBatchEmptyValue.
                    // Duplicates reference existing entries that were also created via the same path.
                    // batchAddr offsets now point at the start of the value region.
                    Assert.assertEquals(sentinel, Unsafe.getUnsafe().getLong(entryBase + offset));
                }

                // Clear the empty value pattern so subsequent tests don't leak the scratch buffer.
                map.setBatchEmptyValue(null);
            }
        });
    }

    @Test
    public void testSetBatchEmptyValueAllZero() throws Exception {
        // Maps detect an all-zero empty value pattern and skip the per-entry memcpy.
        // New entries must still read as zero because fresh slots are zeroed by clear().
        TestUtils.assertMemoryLeak(() -> {
            final int[] logicalKeys = {1, 2, 3};
            final int batchRows = logicalKeys.length;

            try (
                    Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG), 64, 0.8, 24);
                    DirectLongList batch = new DirectLongList(batchRows, MemoryTag.NATIVE_DEFAULT);
                    TestPageFrameRecord testRecord = new TestPageFrameRecord(mapType, logicalKeys)
            ) {
                batch.setPos(batchRows);
                final RecordSink sink = new TestRecordSink(columnTypeForMapType(), -1);

                map.setBatchEmptyValue(new TestFunctionsUpdater(0L));
                map.reserveCapacity(batchRows);
                final long entryBase = map.probeBatch(testRecord, sink, 0, batchRows, batch.getAddress());

                Assert.assertEquals(3, map.size());
                for (int i = 0; i < batchRows; i++) {
                    final long encoded = Unsafe.getUnsafe().getLong(batch.getAddress() + ((long) i << 3));
                    Assert.assertTrue(Map.isNewBatchEntry(encoded));
                    final long offset = Map.decodeBatchOffset(encoded);
                    Assert.assertEquals(0L, Unsafe.getUnsafe().getLong(entryBase + offset));
                }

                // Clearing a null updater must be a no-op and idempotent.
                map.setBatchEmptyValue(null);
                map.setBatchEmptyValue(null);
            }
        });
    }

    @Test
    public void testSetKeyCapacity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), 64, 0.5, 2147483647)) {
                Assert.assertEquals(128, map.getKeyCapacity());

                map.setKeyCapacity(130);
                Assert.assertEquals(512, map.getKeyCapacity());

                map.setKeyCapacity(1000);
                Assert.assertEquals(2048, map.getKeyCapacity());

                // this call should be ignored
                map.setKeyCapacity(10);
                Assert.assertEquals(2048, map.getKeyCapacity());
            }
        });
    }

    @Test
    public void testSetKeyCapacityOverflow() throws Exception {
        // Run this test for every map type.
        TestUtils.assertMemoryLeak(() -> {
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), 16, 0.75, Integer.MAX_VALUE)) {
                try {
                    // should fail with 0.75 load factor
                    map.setKeyCapacity(Integer.MAX_VALUE / 4 * 3 + 1);
                    Assert.fail();
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "map capacity overflow");
                }
                // Should be fine, but it's expensive to run on every CI run:
                // map.setKeyCapacity(Integer.MAX_VALUE / 8 * 3 );
            }
        });
    }

    @Test
    public void testUnsupportedValueBinary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Map ignore = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.BINARY), 64, 0.5, 1)) {
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "value type is not supported"));
            }
        });
    }

    private void assertMapContainsKeys(Map map, int[] expected) throws NumericException {
        final int[] sorted = Arrays.copyOf(expected, expected.length);
        Arrays.sort(sorted);
        final int[] actual = new int[(int) map.size()];
        int n = 0;
        try (RecordCursor cursor = map.getCursor()) {
            final MapRecord record = (MapRecord) cursor.getRecord();
            while (cursor.hasNext()) {
                // Value column is at index 0 (LONG), key column is at index 1.
                actual[n++] = readKey(record, 1);
            }
        }
        Assert.assertEquals(sorted.length, n);
        Arrays.sort(actual);
        Assert.assertArrayEquals(sorted, actual);
    }

    private int columnTypeForMapType() {
        switch (mapType) {
            case UNORDERED_8_MAP:
                return ColumnType.LONG;
            case UNORDERED_VARCHAR_MAP:
                return ColumnType.VARCHAR;
            default:
                return ColumnType.INT;
        }
    }

    private Map createMap(ColumnTypes keyTypes, ColumnTypes valueTypes, int keyCapacity, double loadFactor, int maxResizes) {
        switch (mapType) {
            case ORDERED_MAP:
                return new OrderedMap(32 * 1024, keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes);
            case UNORDERED_4_MAP:
                Assert.assertEquals(1, keyTypes.getColumnCount());
                return new Unordered4Map(ColumnType.INT, valueTypes, keyCapacity, loadFactor, maxResizes);
            case UNORDERED_8_MAP:
                Assert.assertEquals(1, keyTypes.getColumnCount());
                return new Unordered8Map(ColumnType.LONG, valueTypes, keyCapacity, loadFactor, maxResizes);
            case UNORDERED_VARCHAR_MAP:
                Assert.assertEquals(1, keyTypes.getColumnCount());
                Assert.assertEquals(ColumnType.VARCHAR, keyTypes.getColumnType(0));
                return new UnorderedVarcharMap(valueTypes, keyCapacity, loadFactor, maxResizes, 128 * 1024, 4 * Numbers.SIZE_1GB);
            default:
                throw new IllegalArgumentException("Unknown map type: " + mapType);
        }
    }

    private SingleColumnType keyColumnType(int preferredKeyColumnType) {
        return new SingleColumnType((mapType == MapType.UNORDERED_VARCHAR_MAP ? ColumnType.VARCHAR : preferredKeyColumnType));
    }

    private void populateKey(MapKey key, int index) {
        if (mapType == MapType.UNORDERED_VARCHAR_MAP) {
            int mode = rnd.nextInt(10);
            if (mode == 0) {
                // 10% chances of using on-heap utf8 sequence
                key.putVarchar(new Utf8String(String.valueOf(index)));
            } else if (mode == 1) {
                // 10% of using a non-stable off-heap sequence
                try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
                    sink.put(index);
                    key.putVarchar(sink);
                }
            } else {
                // 80% of using a stable off-heap sequence
                long lo = STABLE_SINK.hi();
                STABLE_SINK.put(index);
                TestDirectUtf8String directStr = new TestDirectUtf8String(true);
                directStr.of(lo, STABLE_SINK.hi(), true);
                key.putVarchar(directStr);
            }
        } else if (mapType == MapType.UNORDERED_8_MAP) {
            key.putLong(index);
        } else {
            key.putInt(index);
        }
    }

    private int readKey(Record record, int index) throws NumericException {
        if (mapType == MapType.UNORDERED_VARCHAR_MAP) {
            return Numbers.parseInt(record.getVarcharA(index));
        } else if (mapType == MapType.UNORDERED_8_MAP) {
            return (int) record.getLong(index);
        } else {
            return record.getInt(index);
        }
    }

    public enum MapType {
        ORDERED_MAP, UNORDERED_4_MAP, UNORDERED_8_MAP, UNORDERED_VARCHAR_MAP
    }

    private static class TestFunctionsUpdater implements GroupByFunctionsUpdater {
        private final long sentinelValue;

        TestFunctionsUpdater(long sentinelValue) {
            this.sentinelValue = sentinelValue;
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setFunctions(ObjList<GroupByFunction> groupByFunctions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateEmpty(MapValue value) {
            value.putLong(0, sentinelValue);
        }

        @Override
        public void updateExisting(MapValue value, Record record, long rowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateNew(MapValue value, Record record, long rowId) {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestMapValueMergeFunction implements MapValueMergeFunction {

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            destValue.addInt(0, srcValue.getInt(0));
        }
    }

    // Minimal PageFrameMemoryRecord for exercising probeBatch paths: stores the
    // logical keys once and services both the direct-column fast path (via
    // getPageAddress) and the sink-based slow path (via getInt/getLong/getVarcharA).
    private static class TestPageFrameRecord extends PageFrameMemoryRecord {
        private final int[] logicalKeys;
        private final MapType mapType;
        private final ObjList<Utf8String> varcharKeys;
        private long bufferAddr;
        private long bufferSize;

        TestPageFrameRecord(MapType mapType, int[] logicalKeys) {
            this.mapType = mapType;
            this.logicalKeys = logicalKeys;
            if (mapType == MapType.UNORDERED_VARCHAR_MAP) {
                this.varcharKeys = new ObjList<>(logicalKeys.length);
                for (int i = 0; i < logicalKeys.length; i++) {
                    this.varcharKeys.add(new Utf8String(String.valueOf(logicalKeys[i])));
                }
                this.bufferAddr = 0;
                this.bufferSize = 0;
            } else {
                this.varcharKeys = null;
                final long elemSize = mapType == MapType.UNORDERED_8_MAP ? Long.BYTES : Integer.BYTES;
                this.bufferSize = (long) logicalKeys.length * elemSize;
                this.bufferAddr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
                for (int i = 0; i < logicalKeys.length; i++) {
                    if (mapType == MapType.UNORDERED_8_MAP) {
                        Unsafe.getUnsafe().putLong(bufferAddr + (long) i * Long.BYTES, logicalKeys[i]);
                    } else {
                        Unsafe.getUnsafe().putInt(bufferAddr + (long) i * Integer.BYTES, logicalKeys[i]);
                    }
                }
            }
        }

        @Override
        public void close() {
            if (bufferAddr != 0) {
                bufferAddr = Unsafe.free(bufferAddr, bufferSize, MemoryTag.NATIVE_DEFAULT);
                bufferSize = 0;
            }
        }

        @Override
        public int getInt(int columnIndex) {
            return Unsafe.getUnsafe().getInt(bufferAddr + rowIndex * Integer.BYTES);
        }

        @Override
        public long getLong(int columnIndex) {
            return Unsafe.getUnsafe().getLong(bufferAddr + rowIndex * Long.BYTES);
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return bufferAddr;
        }

        @Override
        public Utf8Sequence getVarcharA(int columnIndex) {
            return varcharKeys.getQuick((int) rowIndex);
        }
    }

    private static class TestRecordSink implements RecordSink {
        private final int columnType;
        private final int directColumnIndex;

        TestRecordSink(int columnType, int directColumnIndex) {
            this.columnType = columnType;
            this.directColumnIndex = directColumnIndex;
        }

        @Override
        public void copy(Record r, RecordSinkSPI w) {
            switch (columnType) {
                case ColumnType.INT:
                    w.putInt(r.getInt(0));
                    break;
                case ColumnType.LONG:
                    w.putLong(r.getLong(0));
                    break;
                case ColumnType.VARCHAR:
                    w.putVarchar(r.getVarcharA(0));
                    break;
                default:
                    throw new UnsupportedOperationException("column type: " + ColumnType.nameOf(columnType));
            }
        }

        @Override
        public int getDirectColumnIndex() {
            return directColumnIndex;
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
            // no-op
        }
    }
}
