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

package io.questdb.cairo.map;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CompactMapTest extends AbstractCairoTest {

    @Test
    public void testAppendExisting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int N = 10;
            try (CompactMap map = new CompactMap(
                    1024 * 1024,
                    new SingleColumnType(ColumnType.STRING),
                    new SingleColumnType(ColumnType.LONG),
                    N / 2,
                    0.9)) {
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
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int N = 100000;
            int M = 25;
            try (CompactMap map = new CompactMap(
                    1024 * 1024,
                    new SingleColumnType(ColumnType.STRING),
                    new SingleColumnType(ColumnType.LONG),
                    2 * N, 0.7)) {
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

    @Test
    public void testConstructorRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();
            CairoTestUtils.createTestTable(10, new Rnd(), binarySequence);
            SingleColumnType columnTypes = new SingleColumnType();

            try (TableReader reader = new TableReader(configuration, "x")) {
                try {
                    new CompactMap(1024, reader.getMetadata(), columnTypes.of(ColumnType.LONG), 16, 0.75);
                    Assert.fail();
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "Unsupported column type");
                }
            }
        });
    }

    @Test
    public void testKeyLookup() {
        // tweak capacity in such a way that we only have one spare slot before resize is needed
        // this way algo that shuffles "foreign" slots away should face problems
        double loadFactor = 0.9;
        try (CompactMap map = new CompactMap(
                1024 * 1024,
                new SingleColumnType(ColumnType.STRING),
                new SingleColumnType(ColumnType.LONG),
                12,
                loadFactor,
                new MockHash())) {
            MapKey key;
            MapValue value;

            key = map.withKey();
            key.putStr("000-ABCDE");
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putDouble(0, 12.5);

            // difference in last character
            key = map.withKey();
            key.putStr("000-ABCDG");
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putDouble(0, 11.5);

            // different hash code
            key = map.withKey();
            key.putStr("100-ABCDE");
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putDouble(0, 10.5);

            // check that we cannot get value with straight up non-existing hash code
            key = map.withKey();
            key.putStr("200-ABCDE");
            Assert.assertNull(key.findValue());

            // check that we don't get value when we go down the linked list
            // 004 will produce the same hashcode as 100
            key = map.withKey();
            key.putStr("004-ABCDE");
            Assert.assertNull(key.findValue());

            // check that we don't get value when we go down the linked list
            // this will produce 001 hashcode, which should find that slot 1 is occupied by indirect hit
            key = map.withKey();
            key.putStr("033-ABCDE");
            Assert.assertNull(key.findValue());
        }
    }

    @Test
    public void testRecordAsKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 5000;
            final Rnd rnd = new Rnd();
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();

            CairoTestUtils.createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = new TableReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (CompactMap map = new CompactMap(
                        1024 * 1024,
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
                        ,
                        N,
                        0.9)) {

                    RecordSink sink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter, true);

                    final int keyColumnOffset = map.getValueColumnCount();

                    // this random will be populating values
                    Rnd rnd2 = new Rnd();

                    RecordCursor cursor = reader.getCursor();
                    populateMap(map, rnd2, cursor, sink);

                    try (RecordCursor mapCursor = map.getCursor()) {
                        assertMap2(rnd, binarySequence, keyColumnOffset, rnd2, mapCursor);
                        mapCursor.toTop();
                        assertMap2(rnd, binarySequence, keyColumnOffset, rnd2, mapCursor);
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
            try (CompactMap map = new CompactMap(Numbers.SIZE_1MB, types, types, 64, 0.5)) {

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
                try (RecordCursor mapCursor = map.getCursor()) {
                    final MapRecord record = (MapRecord) mapCursor.getRecord();
                    while (mapCursor.hasNext()) {
                        list.add(record.getRowId());
                        Assert.assertEquals(rnd.nextInt(), record.getInt(1));
                        MapValue value = record.getValue();
                        value.putInt(0, value.getInt(0) * 2);
                    }

                    // access map by rowid now
                    rnd.reset();
                    Record rec = mapCursor.getRecordB();
                    for (int i = 0, n = list.size(); i < n; i++) {
                        mapCursor.recordAt(rec, list.getQuick(i));
                        Assert.assertEquals((i + 1) * 2, rec.getInt(0));
                        Assert.assertEquals(rnd.nextInt(), rec.getInt(1));
                    }

                    rnd.reset();
                    Assert.assertNotSame(rec, mapCursor.getRecord());

                    for (int i = 0, n = list.size(); i < n; i++) {
                        mapCursor.recordAt(rec, list.getQuick(i));
                        Assert.assertEquals((i + 1) * 2, rec.getInt(0));
                        Assert.assertEquals(rnd.nextInt(), rec.getInt(1));
                    }
                }
            }
        });
    }

    @Test
    public void testUnableToFindFreeSlot() {

        // test what happens when map runs out of free slots while trying to
        // reshuffle "foreign" entries out of the way

        Rnd rnd = new Rnd();

        // these have to be power of two for on the limit testing
        // QMap will round capacity to next highest power of two!

        int N = 256;
        int M = 32;


        // This function must know about entry structure.
        // To make hash consistent we will assume that first character of
        // string is always a number and this number will be hash code of string.
        class MockHash implements CompactMap.HashFunction {
            @Override
            public long hash(VirtualMemory mem, long offset, long size) {
                // we have single key field, which is string
                // the offset of string is 8 bytes for key cell + 4 bytes for string length, total is 12
                char c = mem.getChar(offset + 12);
                return c - '0';
            }
        }

        StringSink sink = new StringSink();
        // tweak capacity in such a way that we only have one spare slot before resize is needed
        // this way algo that shuffles "foreign" slots away should face problems
        double loadFactor = 0.9999999;
        try (CompactMap map = new CompactMap(
                1024 * 1024,
                new SingleColumnType(ColumnType.STRING),
                new SingleColumnType(ColumnType.LONG),
                (long) (N * loadFactor), loadFactor, new MockHash())) {

            // assert that key capacity is what we expect, otherwise this test would be useless
            Assert.assertEquals(N, map.getActualCapacity());
            testUnableToFindFreeSlot0(rnd, N, M, sink, map);
            map.clear();
            rnd.reset();
            testUnableToFindFreeSlot0(rnd, N, M, sink, map);
        }
    }

    @Test
    public void testUnableToFindFreeSlot2() {

        // test that code that deals with key collection is always
        // protected by map capacity check.

        Rnd rnd = new Rnd();

        // these have to be power of two for on the limit testing
        // QMap will round capacity to next highest power of two!

        int N = 256;
        int M = 32;

        StringSink sink = new StringSink();
        // tweak capacity in such a way that we only have one spare slot before resize is needed
        // this way algo that shuffles "foreign" slots away should face problems
        double loadFactor = 0.9999999;
        try (CompactMap map = new CompactMap(
                1024 * 1024,
                new SingleColumnType(ColumnType.STRING),
                new SingleColumnType(ColumnType.LONG),
                (long) (N * loadFactor), loadFactor, new MockHash())) {

            // assert that key capacity is what we expect, otherwise this test would be useless
            Assert.assertEquals(N, map.getActualCapacity());

            long target = map.getKeyCapacity();

            sink.clear();
            sink.put("000");
            for (long i = 0; i < target - 2; i++) {
                // keep the first character
                sink.clear(3);
                rnd.nextChars(sink, 5);
                MapKey key = map.withKey();
                key.putStr(sink);
                MapValue value = key.createValue();
                Assert.assertTrue(value.isNew());
                value.putLong(0, i + 1);
            }

            sink.clear();
            sink.put(target - 2);

            populate(rnd, sink, map, target - 1, M + N, 3);

            // assert result

            rnd.reset();

            sink.clear();
            sink.put("000");
            assertMap(rnd, sink, map, 0, target - 2, 3);

            sink.clear();
            sink.put(target - 2);
            assertMap(rnd, sink, map, target - 1, M + N, 3);
        }
    }

    @Test
    public void testValueAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
                        .col("l", ColumnType.BINARY);
                CairoTestUtils.create(model);
            }

            final int N = 1000;
            final Rnd rnd = new Rnd();
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();

            CairoTestUtils.createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = new TableReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (CompactMap map = new CompactMap(
                        1024 * 1024,
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
                        ,
                        N,
                        0.9)) {

                    RecordSink sink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter, true);

                    // this random will be populating values
                    Rnd rnd2 = new Rnd();

                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    populateMap(map, rnd2, cursor, sink);

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
                    }
                }
            }
        });
    }

    @Test
    public void testValueRandomWrite() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            final int N = 1000;
            final Rnd rnd = new Rnd();
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();

            CairoTestUtils.createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = new TableReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (CompactMap map = new CompactMap(
                        1024 * 1024,
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
                                .add(ColumnType.BOOLEAN)
                        ,
                        N,
                        0.9)) {

                    RecordSink sink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter, false);

                    // this random will be populating values
                    Rnd rnd2 = new Rnd();

                    RecordCursor cursor = reader.getCursor();
                    Record record = cursor.getRecord();
                    long counter = 0;
                    while (cursor.hasNext()) {
                        MapKey key = map.withKey();
                        key.put(record, sink);
                        MapValue value1 = key.createValue();
                        Assert.assertTrue(value1.isNew());
                        value1.putFloat(4, rnd2.nextFloat());
                        value1.putDouble(5, rnd2.nextDouble());
                        value1.putDate(6, rnd2.nextLong());
                        value1.putTimestamp(7, rnd2.nextLong());
                        value1.putBool(8, rnd2.nextBoolean());

                        value1.putLong(0, ++counter);
                        value1.putInt(1, rnd2.nextInt());
                        value1.putShort(2, rnd2.nextShort());
                        value1.putByte(3, rnd2.nextByte());
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

    private void assertMap(Rnd rnd, StringSink sink, CompactMap map, long lo, long hi, int prefixLen) {
        for (long i = lo; i < hi; i++) {
            // keep the first character
            sink.clear(prefixLen);
            rnd.nextChars(sink, 5);
            MapKey key = map.withKey();
            key.putStr(sink);
            MapValue value = key.createValue();
            Assert.assertFalse(value.isNew());
            Assert.assertEquals(i + 1, value.getLong(0));
        }
    }

    private void assertMap2(Rnd rnd, TestRecord.ArrayBinarySequence binarySequence, int keyColumnOffset, Rnd rnd2, RecordCursor mapCursor) {
        long c = 0;
        rnd.reset();
        rnd2.reset();
        Record record = mapCursor.getRecord();
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
            } else {
                CharSequence tmp = rnd.nextChars(5);
                TestUtils.assertEquals(tmp, record.getStr(keyColumnOffset + 8));
                TestUtils.assertEquals(tmp, record.getStrB(keyColumnOffset + 8));
                Assert.assertEquals(tmp.length(), record.getStrLen(keyColumnOffset + 8));
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

        }
        Assert.assertEquals(5000, c);
    }

    private void populate(Rnd rnd, StringSink sink, CompactMap map, long lo, long hi, int prefixLen) {
        for (long i = lo; i < hi; i++) {
            // keep the first few characters
            sink.clear(prefixLen);
            rnd.nextChars(sink, 5);
            MapKey key = map.withKey();
            key.putStr(sink);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, i + 1);
        }
    }

    private void populateMap(CompactMap map, Rnd rnd2, RecordCursor cursor, RecordSink sink) {
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
        }
    }

    private void testUnableToFindFreeSlot0(Rnd rnd, int n, int m, StringSink sink, CompactMap map) {
        long target = map.getKeyCapacity();

        sink.clear();
        sink.put('0');
        populate(rnd, sink, map, 0, target - 1, 1);

        sink.clear();
        sink.put('1');

        populate(rnd, sink, map, target - 1, m + n, 1);

        // assert result

        rnd.reset();

        sink.clear();
        sink.put('0');
        assertMap(rnd, sink, map, 0, target - 1, 1);

        sink.clear();
        sink.put('1');

        assertMap(rnd, sink, map, target - 1, m + n, 1);
    }

    // This hash function will use first three characters as hash code
    // we need decent spread of hash codes making single character not enough
    private static class MockHash implements CompactMap.HashFunction {
        @Override
        public long hash(VirtualMemory mem, long offset, long size) {
            // string begins after 8-byte cell for key value
            CharSequence cs = mem.getStr(offset + 8);
            try {
                return Numbers.parseLong(cs, 0, 3);
            } catch (NumericException e) {
                // this must not happen unless test doesn't comply
                // with key format
                throw new RuntimeException();
            }
        }
    }
}
