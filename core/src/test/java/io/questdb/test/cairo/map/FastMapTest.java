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

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.*;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestRecord;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FastMapTest extends AbstractCairoTest {

    @Test
    public void testAllTypesFixedSizeKey() {
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

        try (FastMap map = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)) {
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
                long256.setAll(
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong()
                );
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
                long256.setAll(
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong()
                );
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

            // RecordCursor is covered in testAllTypesVarSizeKey
        }
    }

    @Test
    public void testAllTypesReverseColumnAccess() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.BYTE);
        keyTypes.add(ColumnType.SHORT);
        keyTypes.add(ColumnType.CHAR);
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.LONG);
        keyTypes.add(ColumnType.FLOAT);
        keyTypes.add(ColumnType.DOUBLE);
        keyTypes.add(ColumnType.STRING);
        keyTypes.add(ColumnType.STRING);
        keyTypes.add(ColumnType.BINARY);
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

        final TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();
        final Long256Impl long256 = new Long256Impl();

        try (FastMap map = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)) {
            MapKey key = map.withKey();
            key.putByte((byte) 1);
            key.putShort((short) 2);
            key.putChar('3');
            key.putInt(4);
            key.putLong(5);
            key.putFloat(6.0f);
            key.putDouble(7.0);
            key.putStr("888", 0, 1);
            key.putStrLowerCase("99", 0, 1);
            key.putBin(binarySequence.of(new byte[]{10}));
            key.putBool(true);
            key.putDate(12);
            key.putTimestamp(13);
            key.putShort((short) 14);
            long256.setAll(15, 15, 15, 15);
            key.putLong256(long256);

            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());

            // use addXYZ() method to initialize values where possible
            value.putByte(0, (byte) 0);
            value.addByte(0, (byte) 1);
            value.putShort(1, (short) 0);
            value.addShort(1, (short) 2);
            value.putChar(2, '3');
            value.putInt(3, 0);
            value.addInt(3, 4);
            value.putLong(4, 0);
            value.addLong(4, 5);
            value.putFloat(5, 0);
            value.addFloat(5, 6);
            value.putDouble(6, 0);
            value.addDouble(6, 7);
            value.putBool(7, true);
            value.putDate(8, 9);
            value.putTimestamp(9, 10);
            value.putInt(10, 11);
            value.putLong256(11, Long256Impl.ZERO_LONG256);
            long256.setAll(12, 12, 12, 12);
            value.addLong256(11, long256);

            // assert that all values are good

            key = map.withKey();
            key.putByte((byte) 1);
            key.putShort((short) 2);
            key.putChar('3');
            key.putInt(4);
            key.putLong(5);
            key.putFloat(6.0f);
            key.putDouble(7.0);
            key.putStrLowerCase("8");
            key.putStr("9");
            key.putBin(binarySequence.of(new byte[]{10}));
            key.putBool(true);
            key.putDate(12);
            key.putTimestamp(13);
            key.putShort((short) 14);
            long256.setAll(15, 15, 15, 15);
            key.putLong256(long256);

            value = key.createValue();
            Assert.assertFalse(value.isNew());

            // access the value columns in reverse order
            long256.setAll(12, 12, 12, 12);
            Assert.assertEquals(long256, value.getLong256A(11));
            Assert.assertEquals(11, value.getInt(10));
            Assert.assertEquals(10, value.getTimestamp(9));
            Assert.assertEquals(9, value.getDate(8));
            Assert.assertTrue(value.getBool(7));
            Assert.assertEquals(7, value.getDouble(6), 0.000000001d);
            Assert.assertEquals(6, value.getFloat(5), 0.000000001f);
            Assert.assertEquals(5, value.getLong(4));
            Assert.assertEquals(4, value.getInt(3));
            Assert.assertEquals('3', value.getChar(2));
            Assert.assertEquals(2, value.getShort(1));
            Assert.assertEquals(1, value.getByte(0));

            try (RecordCursor cursor = map.getCursor()) {
                assertCursorAllTypesReverseOrder(cursor);
            }
        }
    }

    @Test
    public void testAllTypesVarSizeKey() {
        Rnd rnd = new Rnd();

        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.BYTE);
        keyTypes.add(ColumnType.SHORT);
        keyTypes.add(ColumnType.CHAR);
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.LONG);
        keyTypes.add(ColumnType.FLOAT);
        keyTypes.add(ColumnType.DOUBLE);
        keyTypes.add(ColumnType.STRING);
        keyTypes.add(ColumnType.BOOLEAN);
        keyTypes.add(ColumnType.DATE);
        keyTypes.add(ColumnType.TIMESTAMP);
        keyTypes.add(ColumnType.getGeoHashTypeWithBits(13));
        keyTypes.add(ColumnType.LONG256);
        keyTypes.add(ColumnType.UUID);

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

        try (FastMap map = new FastMap(128, keyTypes, valueTypes, 64, 0.8, 24)) {
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
                if ((rnd.nextPositiveInt() % 4) == 0) {
                    key.putStr(null);
                } else {
                    key.putStr(rnd.nextChars(rnd.nextPositiveInt() % 16));
                }
                key.putBool(rnd.nextBoolean());
                key.putDate(rnd.nextLong());
                key.putTimestamp(rnd.nextLong());
                key.putShort(rnd.nextShort());
                Long256Impl long256 = new Long256Impl();
                long256.setAll(
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong()
                );
                key.putLong256(long256);
                key.putLong128(rnd.nextLong(), rnd.nextLong()); // UUID

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
                value.putLong128(12, rnd.nextLong(), rnd.nextLong());
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
                if ((rnd.nextPositiveInt() % 4) == 0) {
                    key.putStr(null);
                } else {
                    key.putStr(rnd.nextChars(rnd.nextPositiveInt() % 16));
                }
                key.putBool(rnd.nextBoolean());
                key.putDate(rnd.nextLong());
                key.putTimestamp(rnd.nextLong());
                key.putShort(rnd.nextShort());
                Long256Impl long256 = new Long256Impl();
                long256.setAll(
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong()
                );
                key.putLong256(long256);
                key.putLong128(rnd.nextLong(), rnd.nextLong()); // UUID

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
                Assert.assertEquals(rnd.nextLong(), value.getLong128Lo(12));
                Assert.assertEquals(rnd.nextLong(), value.getLong128Hi(12));
            }

            try (RecordCursor cursor = map.getCursor()) {
                rnd.reset();
                assertCursorAllTypes(rnd, cursor);

                rnd.reset();
                cursor.toTop();
                assertCursorAllTypes(rnd, cursor);
            }
        }
    }

    @Test
    public void testAppendExisting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int N = 10;
            try (
                    FastMap map = new FastMap(
                            Numbers.SIZE_1MB,
                            new SingleColumnType(ColumnType.STRING),
                            new SingleColumnType(ColumnType.LONG),
                            N / 2,
                            0.5f,
                            1
                    )
            ) {
                ObjList<String> keys = new ObjList<>();
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(11);
                    keys.add(s.toString());
                    MapKey key = map.withKey();
                    key.putStr(s);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putLong(0, i + 1);
                }
                Assert.assertEquals(N, map.size());

                for (int i = 0, n = keys.size(); i < n; i++) {
                    MapKey key = map.withKey();
                    CharSequence s = keys.getQuick(i);
                    key.putStr(s);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(i + 1, value.getLong(0));
                }
            }
        });
    }

    @Test
    public void testAppendUnique() throws Exception {
        testAppendUnique(3);
    }

    @Test
    public void testClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (
                    FastMap map = new FastMap(
                            Numbers.SIZE_1MB,
                            new SingleColumnType(ColumnType.INT),
                            new SingleColumnType(ColumnType.INT),
                            N / 2, 0.5f, 1
                    )
            ) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(i);

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
                    key.putInt(N + i);

                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, N + i);
                }

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(N + i);

                    MapValue value = key.createValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(N + i, value.getInt(0));
                }

                Assert.assertEquals(N, map.size());
            }
        });
    }

    @Test
    public void testCollisionPerformance() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        ArrayColumnTypes valueTypes = new ArrayColumnTypes();

        keyTypes.add(ColumnType.STRING);
        keyTypes.add(ColumnType.STRING);

        valueTypes.add(ColumnType.LONG);

        // These are default FastMap configuration for a join
        try (FastMap map = new FastMap(4194304, keyTypes, valueTypes, 2097152 / 4, 0.5, 2147483647)) {
            for (int i = 0; i < 40_000_000; i++) {
                MapKey key = map.withKey();
                key.putStr(Integer.toString(i / 151));
                key.putStr(Integer.toString((i + 3) / 151));

                MapValue value = key.createValue();
                value.putLong(0, i);
            }

            final long keyCapacityBefore = map.getKeyCapacity();
            final long memUsedBefore = Unsafe.getMemUsed();
            final long areaSizeBefore = map.getHeapSize();

            map.restoreInitialCapacity();

            Assert.assertTrue(keyCapacityBefore > map.getKeyCapacity());
            Assert.assertTrue(memUsedBefore > Unsafe.getMemUsed());
            Assert.assertTrue(areaSizeBefore > map.getHeapSize());
        }
    }

    @Test
    public void testCollisionPerformanceLongKeys() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.LONG);
        keyTypes.add(ColumnType.INT);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        // These are default FastMap configuration for a join
        try (FastMap map = new FastMap(4194304, keyTypes, valueTypes, 2097152, 0.5, 10000)) {
            for (int i = 0; i < 40_000_000; i++) {
                MapKey key = map.withKey();
                key.putLong(i / 151);
                key.putInt((i + 15) % 269);

                MapValue value = key.createValue();
                value.putLong(0, i);
            }
        }
    }

    @Test
    public void testConstructorRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();
            createTestTable(10, new Rnd(), binarySequence);

            try (TableReader reader = newTableReader(configuration, "x")) {
                try {
                    new CompactMap(1024, reader.getMetadata(), new SingleColumnType(ColumnType.LONG), 16, 0.75, 1, Integer.MAX_VALUE);
                    Assert.fail();
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "Unsupported column type");
                }
            }
        });
    }

    @Test
    public void testCopyToKeyFixedSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.LONG);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (
                FastMap mapA = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                FastMap mapB = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
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
    }

    @Test
    public void testCopyToKeyVarSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.STRING);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (
                FastMap mapA = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                FastMap mapB = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
        ) {
            final int N = 100000;
            for (int i = 0; i < N; i++) {
                MapKey keyA = mapA.withKey();
                keyA.putInt(i);
                keyA.putStr(Chars.repeat("a", i % 32));

                MapKey keyB = mapB.withKey();
                keyB.putInt(i);
                keyB.putStr(Chars.repeat("a", i % 32));

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
    }

    @Test
    public void testCopyValueFixedSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.LONG);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (
                FastMap mapA = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                FastMap mapB = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
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
    }

    @Test
    public void testCopyValueVarSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.STRING);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (
                FastMap mapA = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                FastMap mapB = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
        ) {
            final int N = 100000;
            for (int i = 0; i < N; i++) {
                MapKey keyA = mapA.withKey();
                keyA.putInt(i);
                keyA.putStr(Chars.repeat("a", i % 32));

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
    }

    @Test
    public void testFixedSizeKeyOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ColumnTypes types = new SingleColumnType(ColumnType.INT);

            final int N = 10000;
            try (FastMap map = new FastMap(Numbers.SIZE_1MB, types, null, 64, 0.5, 1)) {
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
    public void testGeoHashRecordAsKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 5000;
            final Rnd rnd = new Rnd();
            int precisionBits = 10;
            int geohashType = ColumnType.getGeoHashTypeWithBits(precisionBits);

            BytecodeAssembler asm = new BytecodeAssembler();
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
                model.col("a", ColumnType.LONG).col("b", geohashType);
                CreateTableTestUtils.create(model);
            }

            try (TableWriter writer = newTableWriter(configuration, "x", metrics)) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = writer.newRow();
                    long rndGeohash = GeoHashes.fromCoordinatesDeg(rnd.nextDouble() * 180 - 90, rnd.nextDouble() * 360 - 180, precisionBits);
                    row.putLong(0, i);
                    row.putGeoHash(1, rndGeohash);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newTableReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (
                        FastMap map = new FastMap(
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
                                N, 0.9f, 1
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
    public void testHeapBoundariesFixedSizeKey() {
        // Here, the entry size is 16 bytes, so that we fill the heap up to the boundary exactly before growing it.
        Rnd rnd = new Rnd();
        int expectedEntrySize = 16;

        try (
                FastMap map = new FastMap(
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
    }

    @Test
    public void testHeapBoundariesVarSizeKey() {
        // Here, the entry size is 32 bytes, so that we fill the heap up to the boundary exactly before growing it.
        Rnd rnd = new Rnd();
        int expectedEntrySize = 32;

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);
        valueTypes.add(ColumnType.LONG);

        try (
                FastMap map = new FastMap(
                        32,
                        new SingleColumnType(ColumnType.STRING),
                        valueTypes,
                        16,
                        0.8,
                        1024
                )
        ) {
            final int N = 100;
            for (int i = 0; i < N; i++) {
                MapKey key = map.withKey();
                key.putStr(rnd.nextString(4));

                long usedHeap = map.getUsedHeapSize();
                MapValue value = key.createValue();
                Assert.assertTrue(value.isNew());
                Assert.assertEquals(expectedEntrySize, (int) (map.getUsedHeapSize() - usedHeap));

                value.putLong(0, rnd.nextLong());
                value.putLong(1, rnd.nextLong());
            }

            rnd.reset();

            // assert that all values are good
            for (int i = 0; i < N; i++) {
                MapKey key = map.withKey();
                key.putStr(rnd.nextString(4));

                MapValue value = key.createValue();
                Assert.assertFalse(value.isNew());

                Assert.assertEquals(rnd.nextLong(), value.getLong(0));
                Assert.assertEquals(rnd.nextLong(), value.getLong(1));
            }

            Assert.assertEquals(N, map.size());
        }
    }

    @Test
    public void testKeyCopyFromFixedSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.LONG);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (
                FastMap mapA = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                FastMap mapB = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
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
    }

    @Test
    public void testKeyCopyFromVarSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.STRING);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (
                FastMap mapA = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                FastMap mapB = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
        ) {
            final int N = 100000;
            for (int i = 0; i < N; i++) {
                MapKey keyA = mapA.withKey();
                keyA.putInt(i);
                keyA.putStr(Chars.repeat("a", i % 32));

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
                keyA.putStr(Chars.repeat("a", i % 32));

                MapKey keyB = mapB.withKey();
                keyB.putInt(i);
                keyB.putStr(Chars.repeat("a", i % 32));

                MapValue valueA = keyA.findValue();
                Assert.assertFalse(valueA.isNew());

                MapValue valueB = keyB.findValue();
                Assert.assertFalse(valueB.isNew());

                Assert.assertEquals(i + 2, valueA.getLong(0));
                Assert.assertEquals(valueA.getLong(0), valueB.getLong(0));
            }
        }
    }

    @Test
    public void testKeyHashCodeFixedSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.LONG);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (FastMap map = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)) {
            final int N = 100000;
            final IntList keyHashCodes = new IntList(N);
            for (int i = 0; i < N; i++) {
                MapKey key = map.withKey();
                key.putInt(i);
                key.putLong(i + 1);
                int hashCode = key.hash();
                keyHashCodes.add(hashCode);

                MapValue value = key.createValue(hashCode);
                Assert.assertTrue(value.isNew());
                value.putLong(0, i + 2);
            }

            final IntList recordHashCodes = new IntList(N);
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                recordHashCodes.add(record.keyHashCode());
            }

            TestUtils.assertEquals(keyHashCodes, recordHashCodes);
        }
    }

    @Test
    public void testKeyHashCodeVarSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.STRING);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (FastMap map = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)) {
            final int N = 100000;
            final IntList keyHashCodes = new IntList(N);
            for (int i = 0; i < N; i++) {
                MapKey key = map.withKey();
                key.putInt(i);
                key.putStr(Chars.repeat("a", i % 32));
                key.commit();
                int hashCode = key.hash();
                keyHashCodes.add(hashCode);

                MapValue value = key.createValue(hashCode);
                Assert.assertTrue(value.isNew());
                value.putLong(0, i + 2);
            }

            final IntList recordHashCodes = new IntList(N);
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                recordHashCodes.add(record.keyHashCode());
            }

            TestUtils.assertEquals(keyHashCodes, recordHashCodes);
        }
    }

    @Test
    public void testLargeBinSequence() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ColumnTypes keyTypes = new SingleColumnType(ColumnType.BINARY);
            ColumnTypes valueTypes = new SingleColumnType(ColumnType.INT);
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();
            try (FastMap map = new FastMap(Numbers.SIZE_1MB, keyTypes, valueTypes, 64, 0.5, 1)) {
                final Rnd rnd = new Rnd();
                MapKey key = map.withKey();
                key.putBin(binarySequence.of(rnd.nextBytes(10)));
                MapValue value = key.createValue();
                value.putInt(0, rnd.nextInt());

                BinarySequence bad = new BinarySequence() {
                    @Override
                    public byte byteAt(long index) {
                        return 0;
                    }

                    @Override
                    public long length() {
                        return Integer.MAX_VALUE + 1L;
                    }
                };

                try {
                    map.withKey().putBin(bad);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "binary column is too large");
                }

                key = map.withKey();
                key.putBin(binarySequence.of(rnd.nextBytes(20)));
                value = key.createValue();
                value.putInt(0, rnd.nextInt());

                Assert.assertEquals(2, map.size());

                // and read
                rnd.reset();
                key = map.withKey();
                key.putBin(binarySequence.of(rnd.nextBytes(10)));
                Assert.assertEquals(rnd.nextInt(), key.findValue().getInt(0));

                key = map.withKey();
                key.putBin(binarySequence.of(rnd.nextBytes(20)));
                Assert.assertEquals(rnd.nextInt(), key.findValue().getInt(0));
            }
        });
    }

    @Test
    public void testLong256AndCharAsKey() {
        Rnd rnd = new Rnd();

        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.LONG256);
        keyTypes.add(ColumnType.CHAR);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.DOUBLE);

        Long256Impl long256 = new Long256Impl();

        try (FastMap map = new FastMap(64, keyTypes, valueTypes, 64, 0.8, 24)) {
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
    }

    @Test(expected = LimitOverflowException.class)
    public void testMaxResizes() throws Exception {
        testAppendUnique(1);
    }

    // This test crashes CircleCI, probably due to amount of memory it needs to run
    // I'm going to find out how to deal with that
    @Test
    public void testMemoryStretch() throws Exception {
        if (System.getProperty("questdb.enable_heavy_tests") != null) {
            TestUtils.assertMemoryLeak(() -> {
                ArrayColumnTypes keyTypes = new ArrayColumnTypes();
                ColumnTypes valueTypes = new SingleColumnType(ColumnType.LONG);
                int N = 1500000;
                for (int i = 0; i < N; i++) {
                    keyTypes.add(ColumnType.STRING);
                }

                final Rnd rnd = new Rnd();
                try (FastMap map = new FastMap(Numbers.SIZE_1MB, keyTypes, valueTypes, 1024, 0.5f, 1)) {
                    try {
                        MapKey key = map.withKey();
                        for (int i = 0; i < N; i++) {
                            key.putStr(rnd.nextChars(1024));
                        }
                        key.createValue();
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "row data is too large");
                    }
                }
            });
        }
    }

    @Test
    public void testMergeFixedSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.LONG);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (
                FastMap mapA = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                FastMap mapB = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
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
    }

    @Test
    public void testMergeVarSizeKey() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        keyTypes.add(ColumnType.STRING);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        try (
                FastMap mapA = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24);
                FastMap mapB = new FastMap(1024, keyTypes, valueTypes, 64, 0.8, 24)
        ) {
            final int N = 100000;
            for (int i = 0; i < N; i++) {
                MapKey keyA = mapA.withKey();
                keyA.putInt(i);
                keyA.putStr(Chars.repeat("a", i % 32));

                MapValue valueA = keyA.createValue();
                Assert.assertTrue(valueA.isNew());
                valueA.putLong(0, i + 2);
            }

            for (int i = 0; i < 2 * N; i++) {
                MapKey keyB = mapB.withKey();
                keyB.putInt(i);
                keyB.putStr(Chars.repeat("a", i % 32));

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
                keyB.putStr(Chars.repeat("a", i % 32));
                MapValue valueB = keyB.findValue();

                Assert.assertFalse(valueB.isNew());
                if (i < N) {
                    Assert.assertEquals(valueA.getLong(0), 2 * valueB.getLong(0));
                } else {
                    Assert.assertEquals(valueA.getLong(0), valueB.getLong(0));
                }
            }
        }
    }

    @Test
    public void testNoValueColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final SingleColumnType keyTypes = new SingleColumnType();
            final Rnd rnd = new Rnd();
            final int N = 100;
            try (FastMap map = new FastMap(2 * Numbers.SIZE_1MB, keyTypes.of(ColumnType.INT), 128, 0.7f, 1)) {
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
    public void testRecordAsKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 5000;
            final Rnd rnd = new Rnd();
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();

            createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = newTableReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (
                        FastMap map = new FastMap(
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

                    final int keyColumnOffset = map.getValueColumnCount();

                    // this random will be populating values
                    Rnd rnd2 = new Rnd();

                    RecordCursor cursor = reader.getCursor();
                    populateMap(map, rnd2, cursor, sink);

                    try (RecordCursor mapCursor = map.getCursor()) {
                        assertCursor2(rnd, binarySequence, keyColumnOffset, rnd2, mapCursor);
                        mapCursor.toTop();
                        assertCursor2(rnd, binarySequence, keyColumnOffset, rnd2, mapCursor);
                    }
                }
            }
        });
    }

    @Test
    public void testRowIdAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ColumnTypes types = new SingleColumnType(ColumnType.INT);

            final int N = 10000;
            final Rnd rnd = new Rnd();
            try (FastMap map = new FastMap(Numbers.SIZE_1MB, types, types, 64, 0.5, 1)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(rnd.nextInt());
                    MapValue values = key.createValue();
                    Assert.assertTrue(values.isNew());
                    values.putInt(0, i + 1);
                }

                // reset random generator and iterate map to double the value
                rnd.reset();
                LongList list = new LongList();
                try (RecordCursor cursor = map.getCursor()) {
                    final MapRecord recordA = (MapRecord) cursor.getRecord();
                    while (cursor.hasNext()) {
                        list.add(recordA.getRowId());
                        Assert.assertEquals(rnd.nextInt(), recordA.getInt(1));
                        MapValue value = recordA.getValue();
                        value.putInt(0, value.getInt(0) * 2);
                    }

                    final MapRecord recordB = (MapRecord) cursor.getRecordB();
                    Assert.assertNotSame(recordB, recordA);

                    rnd.reset();
                    for (int i = 0, n = list.size(); i < n; i++) {
                        cursor.recordAt(recordB, list.getQuick(i));
                        Assert.assertEquals((i + 1) * 2, recordB.getInt(0));
                        Assert.assertEquals(rnd.nextInt(), recordB.getInt(1));
                    }
                }
            }
        });
    }

    @Test
    public void testSetKeyCapacity() {
        try (FastMap map = new FastMap(4096, new SingleColumnType(ColumnType.INT), new SingleColumnType(ColumnType.INT), 64, 0.5, 2147483647)) {
            Assert.assertEquals(128, map.getKeyCapacity());

            map.setKeyCapacity(130);
            Assert.assertEquals(512, map.getKeyCapacity());

            map.setKeyCapacity(1000);
            Assert.assertEquals(2048, map.getKeyCapacity());

            // this call should be ignored
            map.setKeyCapacity(10);
            Assert.assertEquals(2048, map.getKeyCapacity());
        }
    }

    @Test
    public void testUnsupportedKeyValueBinary() throws Exception {
        testUnsupportedValueType();
    }

    @Test
    public void testValueAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 1000;
            final Rnd rnd = new Rnd();
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();

            createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = newTableReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (
                        FastMap map = new FastMap(
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
                                        .add(ColumnType.getGeoHashTypeWithBits(5))
                                        .add(ColumnType.getGeoHashTypeWithBits(10))
                                        .add(ColumnType.getGeoHashTypeWithBits(20))
                                        .add(ColumnType.getGeoHashTypeWithBits(40)),
                                N,
                                0.9f,
                                1
                        )
                ) {
                    RecordSink sink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter, true);

                    // this random will be populating values
                    Rnd rnd2 = new Rnd();

                    RecordCursor cursor = reader.getCursor();
                    Record record = cursor.getRecord();
                    populateMapGeo(map, rnd2, cursor, sink);

                    cursor.toTop();
                    rnd2.reset();
                    long c = 0;
                    while (cursor.hasNext()) {
                        MapKey key = map.withKey();
                        key.put(record, sink);
                        MapValue value = key.findValue();
                        Assert.assertNotNull(value);
                        Assert.assertEquals(++c, value.getLong(0));
                        Assert.assertEquals(rnd2.nextInt(), value.getInt(1));
                        Assert.assertEquals(rnd2.nextShort(), value.getShort(2));
                        Assert.assertEquals(rnd2.nextByte(), value.getByte(3));
                        Assert.assertEquals(rnd2.nextFloat(), value.getFloat(4), 0.000001f);
                        Assert.assertEquals(rnd2.nextDouble(), value.getDouble(5), 0.000000001);
                        Assert.assertEquals(rnd2.nextLong(), value.getDate(6));
                        Assert.assertEquals(rnd2.nextLong(), value.getTimestamp(7));
                        Assert.assertEquals(rnd2.nextBoolean(), value.getBool(8));
                        Assert.assertEquals((byte) Math.abs(rnd2.nextByte()), value.getGeoByte(9));
                        Assert.assertEquals((short) Math.abs(rnd2.nextShort()), value.getGeoShort(10));
                        Assert.assertEquals(Math.abs(rnd2.nextInt()), value.getGeoInt(11));
                        Assert.assertEquals(Math.abs(rnd2.nextLong()), value.getGeoLong(12));
                    }
                }
            }
        });
    }

    @Test
    public void testValueRandomWrite() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 10000;
            final Rnd rnd = new Rnd();
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();

            createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = newTableReader(configuration, "x")) {
                ListColumnFilter listColumnFilter = new ListColumnFilter();
                for (int i = 0, n = reader.getMetadata().getColumnCount(); i < n; i++) {
                    listColumnFilter.add(i + 1);
                }

                try (
                        FastMap map = new FastMap(
                                Numbers.SIZE_1MB,
                                new SymbolAsIntTypes().of(reader.getMetadata()),
                                new ArrayColumnTypes()
                                        .add(ColumnType.LONG)
                                        .add(ColumnType.INT)
                                        .add(ColumnType.SHORT)
                                        .add(ColumnType.BYTE)
                                        .add(ColumnType.FLOAT)
                                        .add(ColumnType.DOUBLE)
                                        .add(ColumnType.DATE)
                                        .add(ColumnType.TIMESTAMP)
                                        .add(ColumnType.BOOLEAN),
                                N, 0.9f, 1
                        )
                ) {
                    RecordSink sink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), listColumnFilter, false);

                    // this random will be populating values
                    Rnd rnd2 = new Rnd();

                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    long counter = 0;
                    while (cursor.hasNext()) {
                        MapKey key = map.withKey();
                        key.put(record, sink);
                        MapValue value = key.createValue();
                        Assert.assertTrue(value.isNew());
                        value.putFloat(4, rnd2.nextFloat());
                        value.putDouble(5, rnd2.nextDouble());
                        value.putDate(6, rnd2.nextLong());
                        value.putTimestamp(7, rnd2.nextLong());
                        value.putBool(8, rnd2.nextBoolean());

                        value.putLong(0, ++counter);
                        value.putInt(1, rnd2.nextInt());
                        value.putShort(2, rnd2.nextShort());
                        value.putByte(3, rnd2.nextByte());
                    }

                    cursor.toTop();
                    rnd2.reset();
                    long c = 0;
                    while (cursor.hasNext()) {
                        MapKey key = map.withKey();
                        key.put(record, sink);
                        MapValue value = key.findValue();
                        Assert.assertNotNull(value);

                        Assert.assertEquals(rnd2.nextFloat(), value.getFloat(4), 0.000001f);
                        Assert.assertEquals(rnd2.nextDouble(), value.getDouble(5), 0.000000001);
                        Assert.assertEquals(rnd2.nextLong(), value.getDate(6));
                        Assert.assertEquals(rnd2.nextLong(), value.getTimestamp(7));
                        Assert.assertEquals(rnd2.nextBoolean(), value.getBool(8));

                        Assert.assertEquals(++c, value.getLong(0));
                        Assert.assertEquals(rnd2.nextInt(), value.getInt(1));
                        Assert.assertEquals(rnd2.nextShort(), value.getShort(2));
                        Assert.assertEquals(rnd2.nextByte(), value.getByte(3));
                    }
                }
            }
        });
    }

    @Test
    public void testVarSizeKeyOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ColumnTypes types = new SingleColumnType(ColumnType.STRING);

            final int N = 10000;
            try (FastMap map = new FastMap(Numbers.SIZE_1MB, types, null, 64, 0.5, 1)) {
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putStr(Chars.repeat("a", i % 32));
                    key.createValue();
                }

                try (RecordCursor cursor = map.getCursor()) {
                    final MapRecord record = (MapRecord) cursor.getRecord();
                    int i = 0;
                    while (cursor.hasNext()) {
                        TestUtils.assertEquals(Chars.repeat("a", i % 32), record.getStr(0));
                        i++;
                    }
                }
            }
        });
    }

    private void assertCursor2(Rnd rnd, TestRecord.ArrayBinarySequence binarySequence, int keyColumnOffset, Rnd rnd2, RecordCursor mapCursor) {
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
            // key fields
            Assert.assertEquals(rnd.nextByte(), record.getByte(keyColumnOffset));
            Assert.assertEquals(rnd.nextShort(), record.getShort(keyColumnOffset + 1));
            if (rnd.nextInt() % 4 == 0) {
                Assert.assertEquals(Numbers.INT_NaN, record.getInt(keyColumnOffset + 2));
            } else {
                Assert.assertEquals(rnd.nextInt(), record.getInt(keyColumnOffset + 2));
            }

            if (rnd.nextInt() % 4 == 0) {
                Assert.assertEquals(Numbers.LONG_NaN, record.getLong(keyColumnOffset + 3));
            } else {
                Assert.assertEquals(rnd.nextLong(), record.getLong(keyColumnOffset + 3));
            }

            if (rnd.nextInt() % 4 == 0) {
                Assert.assertEquals(Numbers.LONG_NaN, record.getDate(keyColumnOffset + 4));
            } else {
                Assert.assertEquals(rnd.nextLong(), record.getDate(keyColumnOffset + 4));
            }

            if (rnd.nextInt() % 4 == 0) {
                Assert.assertEquals(Numbers.LONG_NaN, record.getTimestamp(keyColumnOffset + 5));
            } else {
                Assert.assertEquals(rnd.nextLong(), record.getTimestamp(keyColumnOffset + 5));
            }

            if (rnd.nextInt() % 4 == 0) {
                Assert.assertTrue(Float.isNaN(record.getFloat(keyColumnOffset + 6)));
            } else {
                Assert.assertEquals(rnd.nextFloat(), record.getFloat(keyColumnOffset + 6), 0.00000001f);
            }

            if (rnd.nextInt() % 4 == 0) {
                Assert.assertTrue(Double.isNaN(record.getDouble(keyColumnOffset + 7)));
            } else {
                Assert.assertEquals(rnd.nextDouble(), record.getDouble(keyColumnOffset + 7), 0.0000000001d);
            }

            if (rnd.nextInt() % 4 == 0) {
                Assert.assertNull(record.getStr(keyColumnOffset + 8));
                Assert.assertNull(record.getStrB(keyColumnOffset + 8));
                Assert.assertEquals(-1, record.getStrLen(keyColumnOffset + 8));
                AbstractCairoTest.sink.clear();
                record.getStr(keyColumnOffset + 8, AbstractCairoTest.sink);
                Assert.assertEquals(0, AbstractCairoTest.sink.length());
            } else {
                CharSequence tmp = rnd.nextChars(5);
                TestUtils.assertEquals(tmp, record.getStr(keyColumnOffset + 8));
                TestUtils.assertEquals(tmp, record.getStrB(keyColumnOffset + 8));
                Assert.assertEquals(tmp.length(), record.getStrLen(keyColumnOffset + 8));
                AbstractCairoTest.sink.clear();
                record.getStr(keyColumnOffset + 8, AbstractCairoTest.sink);
                TestUtils.assertEquals(tmp, AbstractCairoTest.sink);
            }

            // we are storing symbol as string, assert as such

            if (rnd.nextInt() % 4 == 0) {
                Assert.assertNull(record.getStr(keyColumnOffset + 9));
            } else {
                TestUtils.assertEquals(rnd.nextChars(3), record.getStr(keyColumnOffset + 9));
            }

            Assert.assertEquals(rnd.nextBoolean(), record.getBool(keyColumnOffset + 10));

            if (rnd.nextInt() % 4 == 0) {
                TestUtils.assertEquals(null, record.getBin(keyColumnOffset + 11), record.getBinLen(keyColumnOffset + 11));
            } else {
                binarySequence.of(rnd.nextBytes(25));
                TestUtils.assertEquals(binarySequence, record.getBin(keyColumnOffset + 11), record.getBinLen(keyColumnOffset + 11));
            }

            if (rnd.nextInt() % 4 == 0) {
                Assert.assertEquals(Numbers.LONG_NaN, record.getLong128Hi(keyColumnOffset + 12));
                Assert.assertEquals(Numbers.LONG_NaN, record.getLong128Lo(keyColumnOffset + 12));
            } else {
                Assert.assertEquals(rnd.nextLong(), record.getLong128Lo(keyColumnOffset + 12));
                Assert.assertEquals(rnd.nextLong(), record.getLong128Hi(keyColumnOffset + 12));
            }
        }
        Assert.assertEquals(5000, c);
    }

    private void assertCursorAllTypes(Rnd rnd, RecordCursor cursor) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            // key part, comes after value part in records
            int col = 13;
            Assert.assertEquals(rnd.nextByte(), record.getByte(col++));
            Assert.assertEquals(rnd.nextShort(), record.getShort(col++));
            Assert.assertEquals(rnd.nextChar(), record.getChar(col++));
            Assert.assertEquals(rnd.nextInt(), record.getInt(col++));
            Assert.assertEquals(rnd.nextLong(), record.getLong(col++));
            Assert.assertEquals(rnd.nextFloat(), record.getFloat(col++), 0.000000001f);
            Assert.assertEquals(rnd.nextDouble(), record.getDouble(col++), 0.000000001d);

            if ((rnd.nextPositiveInt() % 4) == 0) {
                Assert.assertNull(record.getStr(col));
                Assert.assertEquals(-1, record.getStrLen(col++));
            } else {
                CharSequence expected = rnd.nextChars(rnd.nextPositiveInt() % 16);
                TestUtils.assertEquals(expected, record.getStr(col++));
            }

            Assert.assertEquals(rnd.nextBoolean(), record.getBool(col++));
            Assert.assertEquals(rnd.nextLong(), record.getDate(col++));
            Assert.assertEquals(rnd.nextLong(), record.getTimestamp(col++));
            Assert.assertEquals(rnd.nextShort(), record.getShort(col++));
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

            // value part, it comes first in record
            col = 0;
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
            Assert.assertEquals(long256, record.getLong256A(col++));
            Assert.assertEquals(rnd.nextLong(), record.getLong128Lo(col));
            Assert.assertEquals(rnd.nextLong(), record.getLong128Hi(col));
        }
    }

    private void assertCursorAllTypesReverseOrder(RecordCursor cursor) {
        final Record record = cursor.getRecord();
        Assert.assertTrue(cursor.hasNext());

        final Long256Impl long256 = new Long256Impl();

        final int keys = 15;
        final int values = 11;
        int col = keys + values;
        // key
        long256.setAll(15, 15, 15, 15);
        Assert.assertEquals(long256, record.getLong256A(col--));
        Assert.assertEquals(14, record.getShort(col--));
        Assert.assertEquals(13, record.getTimestamp(col--));
        Assert.assertEquals(12, record.getDate(col--));
        Assert.assertTrue(record.getBool(col--));
        BinarySequence binarySequence = record.getBin(col--);
        Assert.assertEquals(1, binarySequence.length());
        Assert.assertEquals(10, binarySequence.byteAt(0));
        TestUtils.assertEquals("9", record.getStr(col--));
        TestUtils.assertEquals("8", record.getStr(col--));
        Assert.assertEquals(7, record.getDouble(col--), 0.000000001d);
        Assert.assertEquals(6, record.getFloat(col--), 0.000000001f);
        Assert.assertEquals(5, record.getLong(col--));
        Assert.assertEquals(4, record.getInt(col--));
        Assert.assertEquals('3', record.getChar(col--));
        Assert.assertEquals(2, record.getShort(col--));
        Assert.assertEquals(1, record.getByte(col--));

        // value
        long256.setAll(12, 12, 12, 12);
        Assert.assertEquals(long256, record.getLong256A(col--));
        Assert.assertEquals(11, record.getInt(col--));
        Assert.assertEquals(10, record.getTimestamp(col--));
        Assert.assertEquals(9, record.getDate(col--));
        Assert.assertTrue(record.getBool(col--));
        Assert.assertEquals(7, record.getDouble(col--), 0.000000001d);
        Assert.assertEquals(6, record.getFloat(col--), 0.000000001f);
        Assert.assertEquals(5, record.getLong(col--));
        Assert.assertEquals(4, record.getInt(col--));
        Assert.assertEquals('3', record.getChar(col--));
        Assert.assertEquals(2, record.getShort(col--));
        Assert.assertEquals(1, record.getByte(col));

        Assert.assertFalse(cursor.hasNext());
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

    private void createTestTable(int n, Rnd rnd, TestRecord.ArrayBinarySequence binarySequence) {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
            model
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SHORT)
                    .col("c", ColumnType.INT)
                    .col("d", ColumnType.LONG)
                    .col("e", ColumnType.DATE)
                    .col("f", ColumnType.TIMESTAMP)
                    .col("g", ColumnType.FLOAT)
                    .col("h", ColumnType.DOUBLE)
                    .col("i", ColumnType.STRING)
                    .col("j", ColumnType.SYMBOL)
                    .col("k", ColumnType.BOOLEAN)
                    .col("l", ColumnType.BINARY)
                    .col("m", ColumnType.UUID);
            CreateTableTestUtils.create(model);
        }

        try (TableWriter writer = newTableWriter(configuration, "x", metrics)) {
            for (int i = 0; i < n; i++) {
                TableWriter.Row row = writer.newRow();
                row.putByte(0, rnd.nextByte());
                row.putShort(1, rnd.nextShort());

                if (rnd.nextInt() % 4 == 0) {
                    row.putInt(2, Numbers.INT_NaN);
                } else {
                    row.putInt(2, rnd.nextInt());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(3, Numbers.LONG_NaN);
                } else {
                    row.putLong(3, rnd.nextLong());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(4, Numbers.LONG_NaN);
                } else {
                    row.putDate(4, rnd.nextLong());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(5, Numbers.LONG_NaN);
                } else {
                    row.putTimestamp(5, rnd.nextLong());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putFloat(6, Float.NaN);
                } else {
                    row.putFloat(6, rnd.nextFloat());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putDouble(7, Double.NaN);
                } else {
                    row.putDouble(7, rnd.nextDouble());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putStr(8, null);
                } else {
                    row.putStr(8, rnd.nextChars(5));
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putSym(9, null);
                } else {
                    row.putSym(9, rnd.nextChars(3));
                }

                row.putBool(10, rnd.nextBoolean());

                if (rnd.nextInt() % 4 == 0) {
                    row.putBin(11, null);
                } else {
                    binarySequence.of(rnd.nextBytes(25));
                    row.putBin(11, binarySequence);
                }

                // UUID
                if (rnd.nextInt() % 4 == 0) {
                    row.putLong128(12, Numbers.LONG_NaN, Numbers.LONG_NaN);
                } else {
                    row.putLong128(12, rnd.nextLong(), rnd.nextLong());
                }
                row.append();
            }
            writer.commit();
        }
    }

    private void populateMap(FastMap map, Rnd rnd2, RecordCursor cursor, RecordSink sink) {
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

    private void populateMapGeo(Map map, Rnd rnd2, RecordCursor cursor, RecordSink sink) {
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
            value.putByte(9, (byte) Math.abs(rnd2.nextByte()));
            value.putShort(10, (short) Math.abs(rnd2.nextShort()));
            value.putInt(11, Math.abs(rnd2.nextInt()));
            value.putLong(12, Math.abs(rnd2.nextLong()));
        }
    }

    private void testAppendUnique(int maxResizes) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int N = 100000;
            int M = 25;
            try (
                    FastMap map = new FastMap(
                            Numbers.SIZE_1MB,
                            new SingleColumnType(ColumnType.STRING),
                            new SingleColumnType(ColumnType.LONG),
                            N / 4, 0.5f, maxResizes
                    )
            ) {
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(M);
                    MapKey key = map.withKey();
                    key.putStr(s);
                    MapValue value = key.createValue();
                    value.putLong(0, i + 1);
                }
                Assert.assertEquals(N, map.size());

                long expectedAppendOffset = map.getAppendOffset();

                rnd.reset();
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(M);
                    MapKey key = map.withKey();
                    key.putStr(s);
                    MapValue value = key.findValue();
                    Assert.assertNotNull(value);
                    Assert.assertEquals(i + 1, value.getLong(0));
                }
                Assert.assertEquals(N, map.size());
                Assert.assertEquals(expectedAppendOffset, map.getAppendOffset());
            }
        });
    }

    private void testUnsupportedValueType() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (FastMap ignore = new FastMap(Numbers.SIZE_1MB, new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.BINARY), 64, 0.5, 1)) {
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "value type is not supported"));
            }
        });
    }

    private static class TestMapValueMergeFunction implements MapValueMergeFunction {

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            destValue.addLong(0, srcValue.getLong(0));
        }
    }
}
