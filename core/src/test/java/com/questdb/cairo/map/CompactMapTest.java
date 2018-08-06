/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.map;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.std.*;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
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
            createTestTable(10, new Rnd(), binarySequence);
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

            createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = new TableReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (CompactMap map = new CompactMap(
                        1024 * 1024,
                        new SymbolAsStrTypes(reader.getMetadata()),
                        new ArrayColumnTypes().reset()
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

                    long c = 0;
                    rnd.reset();
                    rnd2.reset();
                    for (Record record : map) {
                        // value
                        Assert.assertEquals(++c, record.getLong(0));
                        Assert.assertEquals(rnd2.nextInt(), record.getInt(1));
                        Assert.assertEquals(rnd2.nextShort(), record.getShort(2));
                        Assert.assertEquals(rnd2.nextByte(), record.getByte(3));
                        Assert.assertEquals(rnd2.nextFloat2(), record.getFloat(4), 0.000001f);
                        Assert.assertEquals(rnd2.nextDouble2(), record.getDouble(5), 0.000000001);
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
                            Assert.assertEquals(rnd.nextFloat2(), record.getFloat(keyColumnOffset + 6), 0.00000001f);
                        }

                        if (rnd.nextInt() % 4 == 0) {
                            Assert.assertTrue(Double.isNaN(record.getDouble(keyColumnOffset + 7)));
                        } else {
                            Assert.assertEquals(rnd.nextDouble2(), record.getDouble(keyColumnOffset + 7), 0.0000000001d);
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
                    Assert.assertEquals(N, c);
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
                for (MapRecord record : map) {
                    list.add(record.getRowId());
                    Assert.assertEquals(rnd.nextInt(), record.getInt(1));
                    MapValue value = record.getValue();
                    value.putInt(0, value.getInt(0) * 2);
                }

                // access map by rowid now
                rnd.reset();
                for (int i = 0, n = list.size(); i < n; i++) {
                    MapRecord record = map.recordAt(list.getQuick(i));
                    Assert.assertEquals((i + 1) * 2, record.getInt(0));
                    Assert.assertEquals(rnd.nextInt(), record.getInt(1));
                }
            }
        });
    }

    @Test
    public void testRowIdStore() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 10000;
            final Rnd rnd = new Rnd();
            final TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();
            createTestTable(N, rnd, binarySequence);

            ColumnTypes types = new SingleColumnType(ColumnType.LONG);

            try (CompactMap map = new CompactMap(1024, types, types, N / 4, 0.5f)) {

                try (TableReader reader = new TableReader(configuration, "x")) {
                    RecordCursor cursor = reader.getCursor();

                    long counter = 0;
                    while (cursor.hasNext()) {
                        Record record = cursor.next();
                        MapKey key = map.withKeyAsLong(record.getRowId());
                        MapValue values = key.createValue();
                        Assert.assertTrue(values.isNew());
                        values.putLong(0, ++counter);
                    }

                    cursor.toTop();
                    counter = 0;
                    while (cursor.hasNext()) {
                        Record record = cursor.next();
                        MapKey key = map.withKeyAsLong(record.getRowId());
                        MapValue values = key.findValue();
                        Assert.assertNotNull(values);
                        Assert.assertEquals(++counter, values.getLong(0));
                    }
                }

                Assert.assertEquals(N, map.size());
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
                // we have singe key field, which is string
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

            createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = new TableReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (CompactMap map = new CompactMap(
                        1024 * 1024,
                        new SymbolAsStrTypes(reader.getMetadata()),
                        new ArrayColumnTypes().reset()
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
                    populateMap(map, rnd2, cursor, sink);

                    cursor.toTop();
                    rnd2.reset();
                    long c = 0;
                    while (cursor.hasNext()) {
                        MapKey key = map.withKey();
                        key.put(cursor.next(), sink);
                        MapValue value = key.findValue();
                        Assert.assertNotNull(value);
                        Assert.assertEquals(++c, value.getLong(0));
                        Assert.assertEquals(rnd2.nextInt(), value.getInt(1));
                        Assert.assertEquals(rnd2.nextShort(), value.getShort(2));
                        Assert.assertEquals(rnd2.nextByte(), value.getByte(3));
                        Assert.assertEquals(rnd2.nextFloat2(), value.getFloat(4), 0.000001f);
                        Assert.assertEquals(rnd2.nextDouble2(), value.getDouble(5), 0.000000001);
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

            createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = new TableReader(configuration, "x")) {
                EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
                entityColumnFilter.of(reader.getMetadata().getColumnCount());

                try (CompactMap map = new CompactMap(
                        1024 * 1024,
                        new SymbolAsIntTypes(reader.getMetadata()),
                        new ArrayColumnTypes().reset()
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
                    long counter = 0;
                    while (cursor.hasNext()) {
                        MapKey key = map.withKey();
                        key.put(cursor.next(), sink);
                        MapValue value1 = key.createValue();
                        Assert.assertTrue(value1.isNew());
                        value1.putFloat(4, rnd2.nextFloat2());
                        value1.putDouble(5, rnd2.nextDouble2());
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
                        key.put(cursor.next(), sink);
                        MapValue value = key.findValue();
                        Assert.assertNotNull(value);

                        Assert.assertEquals(rnd2.nextFloat2(), value.getFloat(4), 0.000001f);
                        Assert.assertEquals(rnd2.nextDouble2(), value.getDouble(5), 0.000000001);
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
                    .col("l", ColumnType.BINARY);
            CairoTestUtils.create(model);
        }

        try (TableWriter writer = new TableWriter(configuration, "x")) {
            for (int i = 0; i < n; i++) {
                TableWriter.Row row = writer.newRow(0);
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
                    row.putFloat(6, rnd.nextFloat2());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putDouble(7, Double.NaN);
                } else {
                    row.putDouble(7, rnd.nextDouble2());
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
                row.append();
            }
            writer.commit();
        }
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
        while (cursor.hasNext()) {
            MapKey key = map.withKey();
            key.put(cursor.next(), sink);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, ++counter);
            value.putInt(1, rnd2.nextInt());
            value.putShort(2, rnd2.nextShort());
            value.putByte(3, rnd2.nextByte());
            value.putFloat(4, rnd2.nextFloat2());
            value.putDouble(5, rnd2.nextDouble2());
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
    private class MockHash implements CompactMap.HashFunction {
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