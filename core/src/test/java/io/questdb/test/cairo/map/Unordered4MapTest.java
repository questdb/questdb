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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.Unordered4Map;
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
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class Unordered4MapTest extends AbstractCairoTest {
    Decimal128 decimal128 = new Decimal128();
    Decimal256 decimal256 = new Decimal256();

    @Test
    public void testAllValueTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.BYTE);
            keyTypes.add(ColumnType.SHORT);

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

            try (Unordered4Map map = new Unordered4Map(keyTypes, valueTypes, 64, 0.8, 24)) {
                final int N = 100;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte(rnd.nextByte());
                    key.putShort(rnd.nextShort());

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
                    key.putByte(rnd.nextByte());
                    key.putShort(rnd.nextShort());

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
                    HashMap<String, Long> keyToRowIds = new HashMap<>();
                    LongList rowIds = new LongList();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        // key part, comes after value part in records
                        int col = 19;
                        byte b = record.getByte(col++);
                        short sh = record.getShort(col);
                        String key = b + "," + sh;
                        keyToRowIds.put(key, record.getRowId());
                        rowIds.add(record.getRowId());
                    }

                    // Validate that we get the same sequence after toTop.
                    cursor.toTop();
                    int i = 0;
                    while (cursor.hasNext()) {
                        int col = 19;
                        byte b = record.getByte(col++);
                        short sh = record.getShort(col);
                        String key = b + "," + sh;
                        Assert.assertEquals((long) keyToRowIds.get(key), record.getRowId());
                        Assert.assertEquals(rowIds.getQuick(i++), record.getRowId());
                    }

                    // Validate that recordAt jumps to what we previously inserted.
                    rnd.reset();
                    for (i = 0; i < N; i++) {
                        byte b = rnd.nextByte();
                        short sh = rnd.nextShort();
                        String key = b + "," + sh;
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
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.INT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            HashMap<Integer, Long> oracle = new HashMap<>();
            try (Unordered4Map map = new Unordered4Map(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    int i0 = rnd.nextInt();
                    key.putInt(i0);

                    MapValue value = key.createValue();
                    value.putLong(0, i0);

                    oracle.put(i0, (long) i0);
                }

                Assert.assertEquals(oracle.size(), map.size());

                // assert map contents
                for (Map.Entry<Integer, Long> e : oracle.entrySet()) {
                    MapKey key = map.withKey();
                    key.putInt(e.getKey());

                    MapValue value = key.findValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals((long) e.getKey(), value.getLong(0));
                    Assert.assertEquals((long) e.getValue(), value.getLong(0));
                }
            }
        });
    }

    @Test
    public void testPutBinUnsupported() throws Exception {
        assertUnsupported(key -> key.putBin(null));
    }

    @Test
    public void testPutDateUnsupported() throws Exception {
        assertUnsupported(key -> key.putDate(0));
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
    public void testPutLongUnsupported() throws Exception {
        assertUnsupported(key -> key.putLong(0));
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
    public void testPutTimestampUnsupported() throws Exception {
        assertUnsupported(key -> key.putTimestamp(0));
    }

    @Test
    public void testPutVarcharUnsupported() throws Exception {
        assertUnsupported(key -> key.putVarchar((Utf8Sequence) null));
    }

    @Test
    public void testSingleZeroKey() {
        try (Unordered4Map map = new Unordered4Map(new SingleColumnType(ColumnType.INT), new SingleColumnType(ColumnType.LONG), 16, 0.8, 24)) {
            MapKey key = map.withKey();
            key.putInt(0);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 42);

            try (RecordCursor cursor = map.getCursor()) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getInt(1));
                Assert.assertEquals(42, record.getLong(0));

                // Validate that we get the same sequence after toTop.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getInt(1));
                Assert.assertEquals(42, record.getLong(0));
            }
        }
    }

    @Test
    public void testTopK() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int heapCapacity = 3;
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.INT);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered4Map map = new Unordered4Map(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    DirectLongLongSortedList list = new DirectLongLongAscList(heapCapacity, MemoryTag.NATIVE_DEFAULT)
            ) {
                for (int i = 0; i < 100; i++) {
                    MapKey key = map.withKey();
                    key.putInt(i);

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
    public void testUnsupportedKeyTypes() throws Exception {
        short[] columnTypes = new short[]{
                ColumnType.BINARY,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.LONG128,
                ColumnType.UUID,
                ColumnType.LONG256,
                ColumnType.LONG,
                ColumnType.DOUBLE,
                ColumnType.TIMESTAMP,
                ColumnType.DATE,
                ColumnType.GEOLONG,
                ColumnType.DECIMAL64,
                ColumnType.DECIMAL128,
                ColumnType.DECIMAL256,
        };
        for (short columnType : columnTypes) {
            TestUtils.assertMemoryLeak(() -> {
                try (Unordered4Map ignore = new Unordered4Map(new SingleColumnType(columnType), new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "unexpected key size"));
                }
            });
        }
    }

    private static void assertUnsupported(Consumer<? super MapKey> putKeyFn) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Unordered4Map map = new Unordered4Map(new SingleColumnType(ColumnType.BOOLEAN), new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
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
}
