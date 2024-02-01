/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.Chars;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class Unordered2MapTest extends AbstractCairoTest {

    @Test
    public void testAllValueTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.BYTE);
            keyTypes.add(ColumnType.BYTE);

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

            try (Unordered2Map map = new Unordered2Map(keyTypes, valueTypes)) {
                final int N = 100;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte(rnd.nextByte());
                    key.putByte(rnd.nextByte());

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
                    long256.setAll(
                            rnd.nextLong(),
                            rnd.nextLong(),
                            rnd.nextLong(),
                            rnd.nextLong()
                    );
                    value.putLong256(11, long256);
                    value.putLong128(12, rnd.nextLong(), rnd.nextLong());
                }

                rnd.reset();

                // assert that all values are good
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte(rnd.nextByte());
                    key.putByte(rnd.nextByte());

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
                    long256.setAll(
                            rnd.nextLong(),
                            rnd.nextLong(),
                            rnd.nextLong(),
                            rnd.nextLong()
                    );
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
                        int col = 13;
                        byte b1 = record.getByte(col++);
                        byte b2 = record.getByte(col);
                        String key = b1 + "," + b2;
                        keyToRowIds.put(key, record.getRowId());
                        rowIds.add(record.getRowId());
                    }

                    // Validate that we get the same sequence after toTop.
                    cursor.toTop();
                    int i = 0;
                    while (cursor.hasNext()) {
                        int col = 13;
                        byte b1 = record.getByte(col++);
                        byte b2 = record.getByte(col);
                        String key = b1 + "," + b2;
                        Assert.assertEquals((long) keyToRowIds.get(key), record.getRowId());
                        Assert.assertEquals(rowIds.getQuick(i++), record.getRowId());
                    }

                    // Validate that recordAt jumps to what we previously inserted.
                    rnd.reset();
                    for (i = 0; i < N; i++) {
                        byte b1 = rnd.nextByte();
                        byte b2 = rnd.nextByte();
                        String key = b1 + "," + b2;
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
                        long256.setAll(
                                rnd.nextLong(),
                                rnd.nextLong(),
                                rnd.nextLong(),
                                rnd.nextLong()
                        );
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
            int N = 100;
            try (Unordered2Map map = new Unordered2Map(new SingleColumnType(ColumnType.SHORT), new SingleColumnType(ColumnType.INT))) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putShort((short) i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putShort((short) i);

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
                    key.putShort((short) (N + i));

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putShort((short) (N + i));

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(N + i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());
            }
        });
    }

    @Test
    public void testCopyValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.SHORT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered2Map mapA = new Unordered2Map(keyTypes, valueTypes);
                    Unordered2Map mapB = new Unordered2Map(keyTypes, valueTypes)
            ) {
                final int N = 30000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    keyA.putShort((short) i);

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
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.SHORT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.INT);

            HashMap<Short, Integer> oracle = new HashMap<>();
            try (Unordered2Map map = new Unordered2Map(keyTypes, valueTypes)) {
                final int N = 10000;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    short sh = rnd.nextShort();
                    key.putShort(sh);

                    MapValue value = key.createValue();
                    value.putInt(0, sh);

                    oracle.put(sh, (int) sh);
                }

                Assert.assertEquals(oracle.size(), map.size());

                // assert map contents
                for (Map.Entry<Short, Integer> e : oracle.entrySet()) {
                    MapKey key = map.withKey();
                    key.putShort(e.getKey());

                    MapValue value = key.findValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals((int) e.getKey(), value.getInt(0));
                    Assert.assertEquals((int) e.getValue(), value.getInt(0));
                }
            }
        });
    }

    @Test
    public void testKeyCopyFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.SHORT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.INT);

            try (
                    Unordered2Map mapA = new Unordered2Map(keyTypes, valueTypes);
                    Unordered2Map mapB = new Unordered2Map(keyTypes, valueTypes)
            ) {
                final int N = 10000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    keyA.putShort((short) i);

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
                    keyA.putShort((short) i);

                    MapKey keyB = mapB.withKey();
                    keyB.putShort((short) i);

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
    public void testKeyOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType types = new SingleColumnType(ColumnType.SHORT);

            final int N = 10000;
            try (Unordered2Map map = new Unordered2Map(types, null)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putShort((short) i);
                    MapValue values = key.createValue();
                    Assert.assertTrue(values.isNew());
                }

                int[] cnts = new int[N];
                try (RecordCursor cursor = map.getCursor()) {
                    final MapRecord record = (MapRecord) cursor.getRecord();
                    while (cursor.hasNext()) {
                        cnts[record.getShort(0)]++;
                    }
                }

                for (int i = 0; i < cnts.length; i++) {
                    Assert.assertEquals("count on index " + i + " is " + cnts[i], 1, cnts[i]);
                }
            }
        });
    }

    @Test
    public void testMergeIntersectingMaps() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.SHORT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered2Map mapA = new Unordered2Map(keyTypes, valueTypes);
                    Unordered2Map mapB = new Unordered2Map(keyTypes, valueTypes)
            ) {
                final int N = Short.MAX_VALUE / 2;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    keyA.putShort((short) i);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putLong(0, i);
                }

                for (int i = 0; i < 2 * N; i++) {
                    MapKey keyB = mapB.withKey();
                    keyB.putShort((short) i);

                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    valueB.putLong(0, i);
                }

                Assert.assertEquals(2 * mapA.size(), mapB.size());

                mapA.merge(mapB, new TestMapValueMergeFunction());

                Assert.assertEquals(mapA.size(), mapB.size());

                // assert that all map B keys can be found in map A
                RecordCursor cursorA = mapA.getCursor();
                MapRecord recordA = mapA.getRecord();
                while (cursorA.hasNext()) {
                    short i = recordA.getShort(1);
                    MapValue valueA = recordA.getValue();

                    MapKey keyB = mapB.withKey();
                    keyB.putShort(i);
                    MapValue valueB = keyB.findValue();

                    Assert.assertFalse(valueB.isNew());
                    if (i < N) {
                        Assert.assertEquals(valueA.getLong(0), 2 * valueB.getLong(0));
                    } else {
                        Assert.assertEquals(valueA.getLong(0), valueB.getLong(0));
                    }
                }
            }
        });
    }

    @Test
    public void testMergeNonIntersectingMaps() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.SHORT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered2Map mapA = new Unordered2Map(keyTypes, valueTypes);
                    Unordered2Map mapB = new Unordered2Map(keyTypes, valueTypes)
            ) {
                final int N = Short.MAX_VALUE / 2;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    keyA.putShort((short) i);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putLong(0, i);
                }

                for (int i = N; i < 2 * N; i++) {
                    MapKey keyB = mapB.withKey();
                    keyB.putShort((short) i);

                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    valueB.putLong(0, i);
                }

                Assert.assertEquals(mapA.size(), mapB.size());

                mapA.merge(mapB, new TestMapValueMergeFunction());

                Assert.assertEquals(mapA.size(), 2 * mapB.size());

                // assert that map A contains all map B keys and its initial keys
                RecordCursor cursorA = mapA.getCursor();
                MapRecord recordA = mapA.getRecord();
                while (cursorA.hasNext()) {
                    short i = recordA.getShort(1);
                    MapValue valueA = recordA.getValue();

                    MapKey keyB = mapB.withKey();
                    keyB.putShort(i);
                    MapValue valueB = keyB.findValue();

                    if (i < N) {
                        Assert.assertNull(valueB);
                    } else {
                        Assert.assertFalse(valueB.isNew());
                        Assert.assertEquals(valueA.getLong(0), valueB.getLong(0));
                    }
                }
            }
        });
    }

    @Test
    public void testMergeStressTest() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.SHORT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered2Map mapA = new Unordered2Map(keyTypes, valueTypes);
                    Unordered2Map mapB = new Unordered2Map(keyTypes, valueTypes)
            ) {
                final int N = 100;
                final int M = 10;
                for (int i = 0; i < N; i++) {
                    mapB.clear();
                    for (int j = 0; j < M; j++) {
                        MapKey keyB = mapB.withKey();
                        keyB.putShort((short) (M * i + j));

                        MapValue valueB = keyB.createValue();
                        Assert.assertTrue(valueB.isNew());
                        valueB.putLong(0, M * i + j);
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
            try (Unordered2Map map = new Unordered2Map(new SingleColumnType(ColumnType.BYTE), new SingleColumnType(ColumnType.INT))) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte((byte) i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte((byte) i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());

                map.close();
                map.reopen();

                Assert.assertEquals(0, map.size());

                // Fill the map once again and verify contents.
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte((byte) (N + i));

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte((byte) (N + i));

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
            try (Unordered2Map map = new Unordered2Map(new SingleColumnType(ColumnType.SHORT), new SingleColumnType(ColumnType.INT))) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putShort((short) i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putShort((short) i);

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
                    key.putShort((short) (N + i));

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putShort((short) (N + i));

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
            final int N = 10000;
            try (Unordered2Map map = new Unordered2Map(new SingleColumnType(ColumnType.SHORT), new SingleColumnType(ColumnType.INT))) {
                for (int i = -N; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putShort((short) i);
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
                        Assert.assertEquals(recordB.getShort(1) * 2, recordB.getInt(0));
                    }
                }
            }
        });
    }

    @Test
    public void testSingleZeroKey() {
        try (Unordered2Map map = new Unordered2Map(new SingleColumnType(ColumnType.SHORT), new SingleColumnType(ColumnType.LONG))) {
            MapKey key = map.withKey();
            key.putShort((short) 0);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 42);

            try (RecordCursor cursor = map.getCursor()) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getShort(1));
                Assert.assertEquals(42, record.getLong(0));

                // Validate that we get the same sequence after toTop.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getShort(1));
                Assert.assertEquals(42, record.getLong(0));
            }
        }
    }

    @Test
    public void testUnsupportedKeyTypes() throws Exception {
        short[] columnTypes = new short[]{
                ColumnType.BINARY,
                ColumnType.STRING,
                ColumnType.LONG128,
                ColumnType.UUID,
                ColumnType.LONG256,
                ColumnType.LONG,
                ColumnType.DOUBLE,
                ColumnType.INT,
                ColumnType.FLOAT,
                ColumnType.TIMESTAMP,
                ColumnType.DATE,
                ColumnType.GEOINT,
                ColumnType.GEOLONG,
        };
        for (short columnType : columnTypes) {
            TestUtils.assertMemoryLeak(() -> {
                try (Unordered2Map ignore = new Unordered2Map(new SingleColumnType(columnType), new SingleColumnType(ColumnType.LONG))) {
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "unexpected key size"));
                }
            });
        }
    }

    private static class TestMapValueMergeFunction implements MapValueMergeFunction {

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            destValue.addLong(0, srcValue.getLong(0));
        }
    }
}
