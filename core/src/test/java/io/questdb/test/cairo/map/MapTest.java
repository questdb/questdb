/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.*;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
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
                {MapType.UNORDERED_16_MAP},
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
                    populateKey(key, rnd.nextInt(), ColumnType.INT);

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
                    populateKey(key, rnd.nextInt(), ColumnType.INT);

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
                        int in = readKey(record, 13, ColumnType.INT);
                        String key = String.valueOf(in);
                        keyToRowIds.put(key, record.getRowId());
                        rowIds.add(record.getRowId());
                    }

                    // Validate that we get the same sequence after toTop.
                    cursor.toTop();
                    int i = 0;
                    while (cursor.hasNext()) {
                        int in = readKey(record, 13, ColumnType.INT);
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
    public void testClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), N / 2, 0.5f, 100)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i, ColumnType.INT);

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
                    populateKey(key, N + i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i, ColumnType.INT);

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
                    populateKey(key, (i + 15) % 151269, ColumnType.INT);

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
                    populateKey(keyA, i, ColumnType.INT);

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
            SingleColumnType keyTypes = keyColumnType(ColumnType.SHORT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.INT);

            try (
                    Map mapA = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    Map mapB = createMap(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                final int N = 10000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    populateKey(keyA, i, ColumnType.SHORT);

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
                    populateKey(keyA, i, ColumnType.SHORT);

                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i, ColumnType.SHORT);

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
                    populateKey(key, i, ColumnType.INT);
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
                    populateKey(key, i, ColumnType.INT);
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
                    populateKey(key, i, ColumnType.INT);
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
                    populateKey(keyA, i, ColumnType.INT);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putInt(0, i);
                }

                for (int i = 0; i < 2 * N; i++) {
                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i, ColumnType.INT);

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
                    int i = readKey(recordA, 1, ColumnType.INT);
                    MapValue valueA = recordA.getValue();

                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i, ColumnType.INT);
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
                    populateKey(keyA, i, ColumnType.INT);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putInt(0, i);
                }

                for (int i = N; i < 2 * N; i++) {
                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i, ColumnType.INT);

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
                    int i = readKey(recordA, 1, ColumnType.INT);
                    MapValue valueA = recordA.getValue();

                    MapKey keyB = mapB.withKey();
                    populateKey(keyB, i, ColumnType.INT);
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
                        populateKey(keyB, M * i + j, ColumnType.INT);

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
    public void testReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), N / 2, 0.5f, 100)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());

                map.close();
                map.close(); // Close must be idempotent
                map.reopen();

                Assert.assertEquals(0, map.size());

                // Fill the map once again and verify contents.
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i, ColumnType.INT);

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
                    populateKey(key, i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i, ColumnType.INT);

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
                    populateKey(key, N + i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(N + i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());
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
                    populateKey(key, i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, i, ColumnType.INT);

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
                    populateKey(key, N + i, ColumnType.INT);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    populateKey(key, N + i, ColumnType.INT);

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
                    populateKey(key, i, ColumnType.INT);
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
                        int expected = readKey(recordB, 1, ColumnType.INT) * 2;
                        Assert.assertEquals(expected, recordB.getInt(0));
                    }
                }
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
        TestUtils.assertMemoryLeak(() -> {
            try (Map map = createMap(keyColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), 16, 0.75, Integer.MAX_VALUE)) {
                try {
                    map.setKeyCapacity(Integer.MAX_VALUE);
                    Assert.fail();
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "map capacity overflow");
                }
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

    private Map createMap(ColumnTypes keyTypes, ColumnTypes valueTypes, int keyCapacity, double loadFactor, int maxResizes) {
        switch (mapType) {
            case ORDERED_MAP:
                return new OrderedMap(32 * 1024, keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes);
            case UNORDERED_4_MAP:
                return new Unordered4Map(keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes);
            case UNORDERED_8_MAP:
                return new Unordered8Map(keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes);
            case UNORDERED_16_MAP:
                return new Unordered16Map(keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes);
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

    private void populateKey(MapKey key, int index, int preferredKeyType) {
        if (mapType == MapType.UNORDERED_VARCHAR_MAP) {
            int mode = rnd.nextInt(10);
            if (mode == 0) {
                // 10% chances of using on-heap utf8 sequence
                key.putVarchar(new Utf8String(String.valueOf(index)));
            } else if (mode == 1) {
                // 10% of using a non-stable offheap sequence
                try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
                    sink.put(index);
                    key.putVarchar(sink);
                }
            } else {
                // 80% of using a stable offheap sequence
                long hi = STABLE_SINK.hi();
                STABLE_SINK.put(index);
                DirectUtf8String directStr = new DirectUtf8String(true);
                directStr.of(hi, STABLE_SINK.hi(), true, true);
                key.putVarchar(directStr);
            }
        } else {
            switch (preferredKeyType) {
                case ColumnType.INT:
                    key.putInt(index);
                    break;
                case ColumnType.SHORT:
                    key.putShort((short) index);
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported key type:" + ColumnType.nameOf(preferredKeyType));
            }
        }
    }

    private int readKey(Record record, int index, int preferredType) throws NumericException {
        if (mapType == MapType.UNORDERED_VARCHAR_MAP) {
            return Numbers.parseInt(record.getVarcharA(index));
        }
        if (preferredType == ColumnType.INT) {
            return record.getInt(index);
        } else if (preferredType == ColumnType.SHORT) {
            return record.getShort(index);
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + ColumnType.nameOf(preferredType));
        }
    }

    public enum MapType {
        ORDERED_MAP, UNORDERED_4_MAP, UNORDERED_8_MAP, UNORDERED_16_MAP, UNORDERED_VARCHAR_MAP
    }

    private static class TestMapValueMergeFunction implements MapValueMergeFunction {

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            destValue.addInt(0, srcValue.getInt(0));
        }
    }
}
