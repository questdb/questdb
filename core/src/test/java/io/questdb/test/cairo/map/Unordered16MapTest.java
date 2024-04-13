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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.Unordered16Map;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.*;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class Unordered16MapTest extends AbstractCairoTest {

    @Test
    public void testAllValueTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.BYTE);
            keyTypes.add(ColumnType.SHORT);
            keyTypes.add(ColumnType.INT);
            keyTypes.add(ColumnType.LONG);

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

            try (Unordered16Map map = new Unordered16Map(keyTypes, valueTypes, 64, 0.8, 24)) {
                final int N = 10000;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte(rnd.nextByte());
                    key.putShort(rnd.nextShort());
                    key.putInt(rnd.nextInt());
                    key.putLong(rnd.nextLong());

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
                    key.putByte(rnd.nextByte());
                    key.putShort(rnd.nextShort());
                    key.putInt(rnd.nextInt());
                    key.putLong(rnd.nextLong());

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
                        int col = 13;
                        byte b = record.getByte(col++);
                        short sh = record.getShort(col++);
                        int in = record.getInt(col++);
                        long l = record.getLong(col);
                        String key = b + "," + sh + "," + in + "," + l;
                        keyToRowIds.put(key, record.getRowId());
                        rowIds.add(record.getRowId());
                    }

                    // Validate that we get the same sequence after toTop.
                    cursor.toTop();
                    int i = 0;
                    while (cursor.hasNext()) {
                        int col = 13;
                        byte b = record.getByte(col++);
                        short sh = record.getShort(col++);
                        int in = record.getInt(col++);
                        long l = record.getLong(col);
                        String key = b + "," + sh + "," + in + "," + l;
                        Assert.assertEquals((long) keyToRowIds.get(key), record.getRowId());
                        Assert.assertEquals(rowIds.getQuick(i++), record.getRowId());
                    }

                    // Validate that recordAt jumps to what we previously inserted.
                    rnd.reset();
                    for (i = 0; i < N; i++) {
                        byte b = rnd.nextByte();
                        short sh = rnd.nextShort();
                        int in = rnd.nextInt();
                        long l = rnd.nextLong();
                        String key = b + "," + sh + "," + in + "," + l;
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
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.LONG128);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            HashMap<Uuid, Long> oracle = new HashMap<>();
            try (Unordered16Map map = new Unordered16Map(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    long l0 = rnd.nextLong();
                    long l1 = rnd.nextLong();
                    key.putLong128(l0, l1);

                    MapValue value = key.createValue();
                    value.putLong(0, l1);

                    oracle.put(new Uuid(l0, l1), l1);
                }

                Assert.assertEquals(oracle.size(), map.size());

                // assert map contents
                for (Map.Entry<Uuid, Long> e : oracle.entrySet()) {
                    MapKey key = map.withKey();
                    key.putLong128(e.getKey().getLo(), e.getKey().getHi());

                    MapValue value = key.findValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(e.getKey().getHi(), value.getLong(0));
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
        try (Unordered16Map map = new Unordered16Map(new SingleColumnType(ColumnType.LONG128), new SingleColumnType(ColumnType.LONG), 16, 0.8, 24)) {
            MapKey key = map.withKey();
            key.putLong128(0, 0);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 42);

            try (RecordCursor cursor = map.getCursor()) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong128Lo(1));
                Assert.assertEquals(0, record.getLong128Hi(1));
                Assert.assertEquals(42, record.getLong(0));

                // Validate that we get the same sequence after toTop.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong128Lo(1));
                Assert.assertEquals(0, record.getLong128Hi(1));
                Assert.assertEquals(42, record.getLong(0));
            }
        }
    }

    @Test
    public void testTwoKeysIncludingZero() {
        try (Unordered16Map map = new Unordered16Map(new SingleColumnType(ColumnType.UUID), new SingleColumnType(ColumnType.LONG), 16, 0.8, 24)) {
            MapKey key = map.withKey();
            key.putLong128(0, 0);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 0);

            key = map.withKey();
            key.putLong128(1, 1);
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 1);

            try (RecordCursor cursor = map.getCursor()) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getLong128Lo(1));
                Assert.assertEquals(1, record.getLong128Hi(1));
                Assert.assertEquals(1, record.getLong(0));
                // Zero is always last when iterating.
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong128Lo(1));
                Assert.assertEquals(0, record.getLong128Hi(1));
                Assert.assertEquals(0, record.getLong(0));

                // Validate that we get the same sequence after toTop.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getLong128Lo(1));
                Assert.assertEquals(1, record.getLong128Hi(1));
                Assert.assertEquals(1, record.getLong(0));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong128Lo(1));
                Assert.assertEquals(0, record.getLong128Hi(1));
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
                ColumnType.LONG256,
        };
        for (short columnType : columnTypes) {
            TestUtils.assertMemoryLeak(() -> {
                try (Unordered16Map ignore = new Unordered16Map(new SingleColumnType(columnType), new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "unexpected key size"));
                }
            });
        }
    }

    private static void assertUnsupported(Consumer<? super MapKey> putKeyFn) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Unordered16Map map = new Unordered16Map(new SingleColumnType(ColumnType.BOOLEAN), new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
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
