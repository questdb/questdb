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
import io.questdb.std.*;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class FixedSizeMapTest extends AbstractCairoTest {

    @Test
    public void testAllKeyTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.BYTE);
            keyTypes.add(ColumnType.SHORT);
            keyTypes.add(ColumnType.CHAR);
            keyTypes.add(ColumnType.INT);
            keyTypes.add(ColumnType.LONG);
            keyTypes.add(ColumnType.FLOAT);
            keyTypes.add(ColumnType.DOUBLE);
            keyTypes.add(ColumnType.BOOLEAN);
            keyTypes.add(ColumnType.DATE);
            keyTypes.add(ColumnType.TIMESTAMP);
            keyTypes.add(ColumnType.getGeoHashTypeWithBits(13));
            keyTypes.add(ColumnType.LONG256);

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

            try (FixedSizeMap map = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24)) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte(rnd.nextByte());
                    key.putShort(rnd.nextShort());
                    key.putChar(rnd.nextChar());
                    key.putInt(rnd.nextInt());
                    key.putLong(rnd.nextLong());
                    key.putFloat(rnd.nextFloat());
                    key.putDouble(rnd.nextDouble());
                    key.putBool(rnd.nextBoolean());
                    key.putDate(rnd.nextLong());
                    key.putTimestamp(rnd.nextLong());
                    key.putShort(rnd.nextShort());
                    Long256Impl long256 = new Long256Impl();
                    long256.fromRnd(rnd);
                    key.putLong256(long256);

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
                    value.putLong256(11, long256);
                }

                rnd.reset();

                // assert that all values are good
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putByte(rnd.nextByte());
                    key.putShort(rnd.nextShort());
                    key.putChar(rnd.nextChar());
                    key.putInt(rnd.nextInt());
                    key.putLong(rnd.nextLong());
                    key.putFloat(rnd.nextFloat());
                    key.putDouble(rnd.nextDouble());
                    key.putBool(rnd.nextBoolean());
                    key.putDate(rnd.nextLong());
                    key.putTimestamp(rnd.nextLong());
                    key.putShort(rnd.nextShort());
                    Long256Impl long256 = new Long256Impl();
                    long256.fromRnd(rnd);
                    key.putLong256(long256);

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
                    Assert.assertEquals(long256, value.getLong256A(11));
                }

                // RecordCursor is covered in testAllValueTypes
            }
        });
    }

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

            try (FixedSizeMap map = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24)) {
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
    public void testCollisionPerformance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            ArrayColumnTypes valueTypes = new ArrayColumnTypes();

            keyTypes.add(ColumnType.LONG);
            keyTypes.add(ColumnType.LONG);

            valueTypes.add(ColumnType.LONG);

            try (FixedSizeMap map = new FixedSizeMap(Numbers.SIZE_1MB, keyTypes, valueTypes, 1024, 0.5, Integer.MAX_VALUE)) {
                for (int i = 0; i < 40_000_000; i++) {
                    MapKey key = map.withKey();
                    key.putLong(i / 151);
                    key.putLong((i + 3) / 151);

                    MapValue value = key.createValue();
                    value.putLong(0, i);
                }

                final long keyCapacityBefore = map.getKeyCapacity();
                final long memUsedBefore = Unsafe.getMemUsed();

                map.restoreInitialCapacity();

                Assert.assertTrue(keyCapacityBefore > map.getKeyCapacity());
                Assert.assertTrue(memUsedBefore > Unsafe.getMemUsed());
            }
        });
    }

    @Test
    public void testCopyToKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.INT);
            keyTypes.add(ColumnType.LONG);

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);

            try (
                    FixedSizeMap mapA = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                    FixedSizeMap mapB = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
            ) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    keyA.putInt(i);
                    keyA.putLong(i + 1);

                    MapKey keyB = mapB.withKey();
                    keyB.putInt(i);
                    keyB.putLong(i + 1);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putLong(0, i + 2);

                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    valueB.putLong(0, i + 2);
                }

                Assert.assertEquals(mapA.size(), mapB.size());

                // assert that all map A keys can be found in map B
                RecordCursor cursorA = mapA.getCursor();
                MapRecord recordA = mapA.getRecord();
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
    public void testCopyValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.INT);
            keyTypes.add(ColumnType.LONG);

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);

            try (
                    FixedSizeMap mapA = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                    FixedSizeMap mapB = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
            ) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    keyA.putInt(i);
                    keyA.putLong(i + 1);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putLong(0, i + 2);
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
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.LONG128);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            HashMap<Uuid, Long> oracle = new HashMap<>();
            try (FixedSizeMap map = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)) {
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
    public void testGeoHashRecordAsKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 5000;
            final Rnd rnd = new Rnd();
            int precisionBits = 10;
            int geohashType = ColumnType.getGeoHashTypeWithBits(precisionBits);

            BytecodeAssembler asm = new BytecodeAssembler();
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE);
            model.col("a", ColumnType.LONG).col("b", geohashType);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, "x", metrics)) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = writer.newRow();
                    long rndGeohash = GeoHashes.fromCoordinatesDeg(rnd.nextDouble() * 180 - 90, rnd.nextDouble() * 360 - 180, precisionBits);
                    row.putLong(0, i);
                    row.putGeoHash(1, rndGeohash);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (
                        FixedSizeMap map = new FixedSizeMap(
                                Numbers.SIZE_1MB,
                                new SymbolAsStrTypes(reader.getMetadata()),
                                new ArrayColumnTypes()
                                        .add(ColumnType.LONG)
                                        .add(ColumnType.INT)
                                        .add(ColumnType.SHORT)
                                        .add(ColumnType.BYTE)
                                        .add(ColumnType.FLOAT)
                                        .add(ColumnType.DOUBLE)
                                        .add(ColumnType.DATE)
                                        .add(ColumnType.TIMESTAMP)
                                        .add(ColumnType.BOOLEAN)
                                        .add(ColumnType.UUID),
                                N,
                                0.9f,
                                1
                        )
                ) {
                    RecordSink sink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter, true);
                    // this random will be populating values
                    Rnd rnd2 = new Rnd();

                    RecordCursor cursor = reader.getCursor();
                    populateMap(map, rnd2, cursor, sink);

                    try (RecordCursor mapCursor = map.getCursor()) {
                        long c = 0;
                        rnd.reset();
                        rnd2.reset();
                        final Record record = mapCursor.getRecord();
                        while (mapCursor.hasNext()) {
                            // value
                            Assert.assertEquals(++c, record.getLong(0));
                            Assert.assertEquals(rnd2.nextInt(), record.getInt(1));
                            Assert.assertEquals(rnd2.nextShort(), record.getShort(2));
                            Assert.assertEquals(rnd2.nextByte(), record.getByte(3));
                            Assert.assertEquals(rnd2.nextFloat(), record.getFloat(4), 0.000001f);
                            Assert.assertEquals(rnd2.nextDouble(), record.getDouble(5), 0.000000001);
                            Assert.assertEquals(rnd2.nextLong(), record.getDate(6));
                            Assert.assertEquals(rnd2.nextLong(), record.getTimestamp(7));
                            Assert.assertEquals(rnd2.nextBoolean(), record.getBool(8));
                            Assert.assertEquals(rnd2.nextLong(), record.getLong128Lo(9));
                            Assert.assertEquals(rnd2.nextLong(), record.getLong128Hi(9));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testHeapBoundaries() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Here, the entry size is 16 bytes, so that we fill the heap up to the boundary exactly before growing it.
            Rnd rnd = new Rnd();
            int expectedEntrySize = 16;

            try (
                    FixedSizeMap map = new FixedSizeMap(
                            32,
                            new SingleColumnType(ColumnType.LONG),
                            new SingleColumnType(ColumnType.LONG),
                            16,
                            0.8,
                            1024
                    )
            ) {
                final int N = 100;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putLong(rnd.nextLong());

                    long usedHeap = map.getUsedHeapSize();
                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    Assert.assertEquals(expectedEntrySize, (int) (map.getUsedHeapSize() - usedHeap));

                    value.putLong(0, rnd.nextLong());
                }

                rnd.reset();

                // assert that all values are good
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putLong(rnd.nextLong());

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());

                    Assert.assertEquals(rnd.nextLong(), value.getLong(0));
                }

                Assert.assertEquals(N, map.size());
            }
        });
    }

    @Test
    public void testKeyCopyFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.INT);
            keyTypes.add(ColumnType.LONG);

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);

            try (
                    FixedSizeMap mapA = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                    FixedSizeMap mapB = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
            ) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    keyA.putInt(i);
                    keyA.putLong(i + 1);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putLong(0, i + 2);

                    MapKey keyB = mapB.withKey();
                    keyB.copyFrom(keyA);

                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    valueB.putLong(0, i + 2);
                }

                Assert.assertEquals(mapA.size(), mapB.size());

                // assert that all map A keys can be found in map B
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    keyA.putInt(i);
                    keyA.putLong(i + 1);

                    MapKey keyB = mapB.withKey();
                    keyB.putInt(i);
                    keyB.putLong(i + 1);

                    MapValue valueA = keyA.findValue();
                    Assert.assertFalse(valueA.isNew());

                    MapValue valueB = keyB.findValue();
                    Assert.assertFalse(valueB.isNew());

                    Assert.assertEquals(i + 2, valueA.getLong(0));
                    Assert.assertEquals(valueA.getLong(0), valueB.getLong(0));
                }
            }
        });
    }

    @Test
    public void testKeyHashCode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.INT);
            keyTypes.add(ColumnType.LONG);

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);

            try (FixedSizeMap map = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24)) {
                final int N = 100000;
                final LongList keyHashCodes = new LongList(N);
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(i);
                    key.putLong(i + 1);
                    long hashCode = key.hash();
                    keyHashCodes.add(hashCode);

                    MapValue value = key.createValue(hashCode);
                    Assert.assertTrue(value.isNew());
                    value.putLong(0, i + 2);
                }

                final LongList recordHashCodes = new LongList(N);
                RecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                while (cursor.hasNext()) {
                    recordHashCodes.add(record.keyHashCode());
                }

                TestUtils.assertEquals(keyHashCodes, recordHashCodes);
            }
        });
    }

    @Test
    public void testKeyOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ColumnTypes types = new SingleColumnType(ColumnType.INT);

            final int N = 10000;
            try (FixedSizeMap map = new FixedSizeMap(Numbers.SIZE_1MB, types, null, 64, 0.5, Integer.MAX_VALUE)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(i);
                    MapValue values = key.createValue();
                    Assert.assertTrue(values.isNew());
                }

                try (RecordCursor cursor = map.getCursor()) {
                    final MapRecord record = (MapRecord) cursor.getRecord();
                    int i = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(i, record.getInt(0));
                        i++;
                    }
                }
            }
        });
    }

    @Test
    public void testLong256AndCharAsKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.LONG256);
            keyTypes.add(ColumnType.CHAR);

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.DOUBLE);

            Long256Impl long256 = new Long256Impl();

            try (FixedSizeMap map = new FixedSizeMap(64, keyTypes, valueTypes, 64, 0.8, 24)) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    long256.fromRnd(rnd);
                    key.putLong256(long256);
                    key.putChar(rnd.nextChar());

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putDouble(0, rnd.nextDouble());
                }

                rnd.reset();

                // assert that all values are good
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    long256.fromRnd(rnd);
                    key.putLong256(long256);
                    key.putChar(rnd.nextChar());

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(rnd.nextDouble(), value.getDouble(0), 0.000000001d);
                }

                try (RecordCursor cursor = map.getCursor()) {
                    rnd.reset();
                    assertCursorLong256(rnd, cursor, long256);

                    rnd.reset();
                    cursor.toTop();
                    assertCursorLong256(rnd, cursor, long256);
                }
            }
        });
    }

    @Test
    public void testMerge() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.INT);
            keyTypes.add(ColumnType.LONG);

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);

            try (
                    FixedSizeMap mapA = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                    FixedSizeMap mapB = new FixedSizeMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
            ) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = mapA.withKey();
                    keyA.putInt(i);
                    keyA.putLong(i + 1);

                    MapValue valueA = keyA.createValue();
                    Assert.assertTrue(valueA.isNew());
                    valueA.putLong(0, i + 2);
                }

                for (int i = 0; i < 2 * N; i++) {
                    MapKey keyB = mapB.withKey();
                    keyB.putInt(i);
                    keyB.putLong(i + 1);

                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    valueB.putLong(0, i + 2);
                }

                Assert.assertEquals(2 * mapA.size(), mapB.size());

                mapA.merge(mapB, new TestMapValueMergeFunction());

                Assert.assertEquals(mapA.size(), mapB.size());

                // assert that all map B keys can be found in map A
                RecordCursor cursorA = mapA.getCursor();
                MapRecord recordA = mapA.getRecord();
                while (cursorA.hasNext()) {
                    int i = recordA.getInt(1);
                    MapValue valueA = recordA.getValue();

                    MapKey keyB = mapB.withKey();
                    keyB.putInt(i);
                    keyB.putLong(i + 1);
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
    public void testNoValueColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final SingleColumnType keyTypes = new SingleColumnType();
            final Rnd rnd = new Rnd();
            final int N = 100;
            try (FixedSizeMap map = new FixedSizeMap(2 * Numbers.SIZE_1MB, keyTypes.of(ColumnType.INT), 128, 0.7f, 1)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(rnd.nextInt());
                    Assert.assertTrue(key.create());
                }

                Assert.assertEquals(N, map.size());

                rnd.reset();

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(rnd.nextInt());
                    Assert.assertFalse(key.notFound());
                }
                Assert.assertEquals(N, map.size());
            }
        });
    }

    @Test
    public void testPutBinUnsupported() throws Exception {
        assertUnsupported(key -> key.putBin(null));
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
        try (FixedSizeMap map = new FixedSizeMap(128, new SingleColumnType(ColumnType.LONG128), new SingleColumnType(ColumnType.LONG), 16, 0.8, 24)) {
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
        try (FixedSizeMap map = new FixedSizeMap(128, new SingleColumnType(ColumnType.UUID), new SingleColumnType(ColumnType.LONG), 16, 0.8, 24)) {
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
                Assert.assertEquals(0, record.getLong128Lo(1));
                Assert.assertEquals(0, record.getLong128Hi(1));
                Assert.assertEquals(0, record.getLong(0));
                // Zero is always last when iterating.
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getLong128Lo(1));
                Assert.assertEquals(1, record.getLong128Hi(1));
                Assert.assertEquals(1, record.getLong(0));

                // Validate that we get the same sequence after toTop.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong128Lo(1));
                Assert.assertEquals(0, record.getLong128Hi(1));
                Assert.assertEquals(0, record.getLong(0));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getLong128Lo(1));
                Assert.assertEquals(1, record.getLong128Hi(1));
                Assert.assertEquals(1, record.getLong(0));
            }
        }
    }

    @Test
    public void testUnsupportedKeyTypes() throws Exception {
        short[] columnTypes = new short[]{
                ColumnType.BINARY,
                ColumnType.STRING,
                ColumnType.VARCHAR,
        };
        for (short columnType : columnTypes) {
            TestUtils.assertMemoryLeak(() -> {
                try (FixedSizeMap ignore = new FixedSizeMap(128, new SingleColumnType(columnType), new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "var-size keys are not supported"));
                }
            });
        }
    }

    private static void assertUnsupported(Consumer<? super MapKey> putKeyFn) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (FixedSizeMap map = new FixedSizeMap(1024, new SingleColumnType(ColumnType.BOOLEAN), new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
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

    private void assertCursorLong256(Rnd rnd, RecordCursor cursor, Long256Impl long256) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            long256.fromRnd(rnd);
            Long256 long256a = record.getLong256A(1);
            Long256 long256b = record.getLong256B(1);

            Assert.assertEquals(long256a.getLong0(), long256.getLong0());
            Assert.assertEquals(long256a.getLong1(), long256.getLong1());
            Assert.assertEquals(long256a.getLong2(), long256.getLong2());
            Assert.assertEquals(long256a.getLong3(), long256.getLong3());

            Assert.assertEquals(long256b.getLong0(), long256.getLong0());
            Assert.assertEquals(long256b.getLong1(), long256.getLong1());
            Assert.assertEquals(long256b.getLong2(), long256.getLong2());
            Assert.assertEquals(long256b.getLong3(), long256.getLong3());

            Assert.assertEquals(rnd.nextChar(), record.getChar(2));

            // value part, it comes first in record

            Assert.assertEquals(rnd.nextDouble(), record.getDouble(0), 0.000000001d);
        }
    }

    private void populateMap(FixedSizeMap map, Rnd rnd2, RecordCursor cursor, RecordSink sink) {
        long counter = 0;
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            MapKey key = map.withKey();
            key.put(record, sink);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, ++counter);
            value.putInt(1, rnd2.nextInt());
            value.putShort(2, rnd2.nextShort());
            value.putByte(3, rnd2.nextByte());
            value.putFloat(4, rnd2.nextFloat());
            value.putDouble(5, rnd2.nextDouble());
            value.putDate(6, rnd2.nextLong());
            value.putTimestamp(7, rnd2.nextLong());
            value.putBool(8, rnd2.nextBoolean());
            value.putLong128(9, rnd2.nextLong(), rnd2.nextLong());
        }
    }

    private static class TestMapValueMergeFunction implements MapValueMergeFunction {

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            destValue.addLong(0, srcValue.getLong(0));
        }
    }
}
