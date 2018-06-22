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

package com.questdb.cairo.map2;

import com.questdb.cairo.*;
import com.questdb.cairo.map.*;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.std.*;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DirectMapTest extends AbstractCairoTest {

    @Test
    public void testAppendExisting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int N = 10;
            try (DirectMap map = new DirectMap(
                    Numbers.SIZE_1MB,
                    new SingleColumnType(ColumnType.STRING),
                    new SingleColumnType(ColumnType.LONG),
                    N / 2,
                    0.5f)) {
                ObjList<String> keys = new ObjList<>();
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(11);
                    keys.add(s.toString());
                    DirectMap.Key key = map.withKey();
                    key.putStr(s);

                    DirectMap.Value value = map.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putLong(0, i + 1);
                }
                Assert.assertEquals(N, map.size());

                for (int i = 0, n = keys.size(); i < n; i++) {
                    DirectMap.Key key = map.withKey();
                    CharSequence s = keys.getQuick(i);
                    key.putStr(s);
                    DirectMap.Value value = map.createValue();
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
            try (DirectMap map = new DirectMap(
                    Numbers.SIZE_1MB,
                    new SingleColumnType(ColumnType.STRING),
                    new SingleColumnType(ColumnType.LONG),
                    N / 4, 0.5f)) {
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(M);
                    DirectMap.Key key = map.withKey();
                    key.putStr(s);
                    DirectMap.Value value = map.createValue();
                    value.putLong(0, i + 1);
                }
                Assert.assertEquals(N, map.size());

                long expectedAppendOffset = map.getAppendOffset();

                rnd.reset();
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(M);
                    DirectMap.Key key = map.withKey();
                    key.putStr(s);
                    DirectMap.Value value = map.findValue();
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

            try (TableReader reader = new TableReader(configuration, "x")) {
                try {
                    new QMap(1024, reader.getMetadata(), new SingleColumnType(ColumnType.LONG), 16, 0.75);
                    Assert.fail();
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "Unsupported column type");
                }
            }
        });
    }

    @Test
    public void testDuplicateValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int N = 100;
            ColumnTypes types = new SingleColumnType(ColumnType.INT);

            // hash everything into the same slot simulating collisions
            DirectMap.HashFunction hash = (address, len) -> 0;


            try (DirectMap map = new DirectMap(1024, types, types, N / 4, 0.5f, hash)) {
                // lookup key that doesn't exist
                map.withKey().putInt(10);
                Assert.assertTrue(map.excludesKey());
                assertDupes(map, rnd, N);
                map.clear();
                assertDupes(map, rnd, N);
            }
        });
    }

    @Test
    public void testLargeBinSequence() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            ColumnTypes keyTypes = new SingleColumnType(ColumnType.BINARY);
            ColumnTypes valueTypes = new SingleColumnType(ColumnType.INT);
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();
            try (DirectMap map = new DirectMap(Numbers.SIZE_1MB, keyTypes, valueTypes, 64, 0.5)) {
                final Rnd rnd = new Rnd();
                map.withKey().putBin(binarySequence.of(rnd.nextBytes(10)));
                DirectMap.Value value = map.createValue();
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
                    TestUtils.assertContains(e.getMessage(), "binary column is too large");
                }

                map.withKey().putBin(binarySequence.of(rnd.nextBytes(20)));
                value = map.createValue();
                value.putInt(0, rnd.nextInt());

                Assert.assertEquals(2, map.size());

                // and read
                rnd.reset();
                map.withKey().putBin(binarySequence.of(rnd.nextBytes(10)));
                Assert.assertEquals(rnd.nextInt(), map.findValue().getInt(0));

                map.withKey().putBin(binarySequence.of(rnd.nextBytes(20)));
                Assert.assertEquals(rnd.nextInt(), map.findValue().getInt(0));
            }
        });
    }

    @Test
    // This test crashes CircleCI, probably due to amount of memory it need to run
    // I'm going to find out how to deal with that
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
                try (DirectMap map = new DirectMap(Numbers.SIZE_1MB, keyTypes, valueTypes, 1024, 0.5f)) {
                    try {
                        DirectMap.Key key = map.withKey();
                        for (int i = 0; i < N; i++) {
                            key.putStr(rnd.nextChars(1024));
                        }
                        map.createValue();
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getMessage(), "row data is too large");
                    }
                }
            });
        }
    }

    @Test
    public void testNoValueColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ColumnTypes keyTypes = new SingleColumnType(ColumnType.INT);
            final Rnd rnd = new Rnd();
            final int N = 100;
            try (DirectMap map = new DirectMap(2 * Numbers.SIZE_1MB, keyTypes, 128, 0.7f)) {
                for (int i = 0; i < N; i++) {
                    map.withKey().putInt(rnd.nextInt());
                    Assert.assertTrue(map.createKey());
                }

                Assert.assertEquals(N, map.size());

                rnd.reset();

                for (int i = 0; i < N; i++) {
                    map.withKey().putInt(rnd.nextInt());
                    Assert.assertFalse(map.excludesKey());
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

            try (TableReader reader = new TableReader(configuration, "x")) {
                IntList columns = new IntList();
                columns.add(0);
                columns.add(1);
                columns.add(2);
                columns.add(3);
                columns.add(4);
                columns.add(5);
                columns.add(6);
                columns.add(7);
                columns.add(8);
                columns.add(9);
                columns.add(10);
                columns.add(11);

                try (DirectMap map = new DirectMap(
                        Numbers.SIZE_1MB,
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
                        0.9f)) {

                    RecordSink sink = RecordSinkFactory.newInstance(asm, reader.getMetadata(), columns, true);

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
            try (DirectMap map = new DirectMap(Numbers.SIZE_1MB, types, types, 64, 0.5)) {


                for (int i = 0; i < N; i++) {
                    map.withKey().putInt(rnd.nextInt());
                    DirectMap.Value values = map.createValue();
                    Assert.assertTrue(values.isNew());
                    values.putInt(0, i + 1);
                }

                // reset random generator and iterate map to double the value
                rnd.reset();
                LongList list = new LongList();
                for (DirectMapRecord record : map) {
                    list.add(record.getRowId());
                    Assert.assertEquals(rnd.nextInt(), record.getInt(1));
                    DirectMap.Value values = record.values();
                    values.putInt(0, values.getInt(0) * 2);
                }

                // access map by rowid now
                rnd.reset();
                for (int i = 0, n = list.size(); i < n; i++) {
                    DirectMapRecord record = map.recordAt(list.getQuick(i));
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

            try (DirectMap map = new DirectMap(1024, types, types, N / 4, 0.5f)) {

                try (TableReader reader = new TableReader(configuration, "x")) {
                    RecordCursor cursor = reader.getCursor();

                    long counter = 0;
                    while (cursor.hasNext()) {
                        Record record = cursor.next();
                        map.withKeyAsLong(record.getRowId());
                        DirectMap.Value values = map.createValue();
                        Assert.assertTrue(values.isNew());
                        values.putLong(0, ++counter);
                    }

                    cursor.toTop();
                    counter = 0;
                    while (cursor.hasNext()) {
                        Record record = cursor.next();
                        map.withKeyAsLong(record.getRowId());
                        DirectMap.Value values = map.findValue();
                        Assert.assertNotNull(values);
                        Assert.assertEquals(++counter, values.getLong(0));
                    }
                }

                Assert.assertEquals(N, map.size());
            }
        });
    }

    @Test
    public void testUnsupportedKeyValueBinary() throws Exception {
        testUnsupportedValueType(ColumnType.BINARY);
    }

    @Test
    public void testUnsupportedKeyValueSymbol() throws Exception {
        testUnsupportedValueType(ColumnType.SYMBOL);
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
                IntList columns = new IntList();
                columns.add(0);
                columns.add(1);
                columns.add(2);
                columns.add(3);
                columns.add(4);
                columns.add(5);
                columns.add(6);
                columns.add(7);
                columns.add(8);
                columns.add(9);
                columns.add(10);
                columns.add(11);

                try (DirectMap map = new DirectMap(
                        Numbers.SIZE_1MB,
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
                        0.9f)) {

                    RecordSink sink = RecordSinkFactory.newInstance(asm, reader.getMetadata(), columns, true);

                    // this random will be populating values
                    Rnd rnd2 = new Rnd();

                    RecordCursor cursor = reader.getCursor();
                    populateMap(map, rnd2, cursor, sink);

                    cursor.toTop();
                    rnd2.reset();
                    long c = 0;
                    while (cursor.hasNext()) {
                        map.withKey().putRecord(cursor.next(), sink);
                        DirectMap.Value value = map.findValue();
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

            final int N = 10000;
            final Rnd rnd = new Rnd();
            TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();

            createTestTable(N, rnd, binarySequence);

            BytecodeAssembler asm = new BytecodeAssembler();

            try (TableReader reader = new TableReader(configuration, "x")) {
                IntList columns = new IntList();
                columns.add(0);
                columns.add(1);
                columns.add(2);
                columns.add(3);
                columns.add(4);
                columns.add(5);
                columns.add(6);
                columns.add(7);
                columns.add(8);
                columns.add(9);
                columns.add(10);
                columns.add(11);

                try (DirectMap map = new DirectMap(
                        Numbers.SIZE_1MB,
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
                        0.9f)) {

                    RecordSink sink = RecordSinkFactory.newInstance(asm, reader.getMetadata(), columns, false);

                    // this random will be populating values
                    Rnd rnd2 = new Rnd();

                    RecordCursor cursor = reader.getCursor();
                    long counter = 0;
                    while (cursor.hasNext()) {
                        map.withKey().putRecord(cursor.next(), sink);
                        DirectMap.Value value = map.createValue();
                        Assert.assertTrue(value.isNew());
                        value.putFloat(4, rnd2.nextFloat2());
                        value.putDouble(5, rnd2.nextDouble2());
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
                        map.withKey().putRecord(cursor.next(), sink);
                        DirectMap.Value value = map.findValue();
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

    private void assertDupes(DirectMap map, Rnd rnd, int n) {
        for (int i = 0; i < n; i++) {
            int key = rnd.nextInt() & (16 - 1);
            map.withKey().putInt(key);
            DirectMap.Value values = map.createValue();
            if (values.isNew()) {
                values.putInt(0, 0);
            } else {
                values.putInt(0, values.getInt(0) + 1);
            }
        }
        Assert.assertEquals(map.size(), 16);

        // attempt to read keys higher than bucket value
        // this must yield null values
        for (int i = 0; i < n * 2; i++) {
            int key = (rnd.nextInt() & (16 - 1)) + 16;
            map.withKey().putInt(key);
            Assert.assertTrue(map.excludesKey());
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

    private void populateMap(DirectMap map, Rnd rnd2, RecordCursor cursor, RecordSink sink) {
        long counter = 0;
        while (cursor.hasNext()) {
            map.withKey().putRecord(cursor.next(), sink);
            DirectMap.Value value = map.createValue();
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

    private void testUnsupportedValueType(int columnType) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new DirectMap(Numbers.SIZE_1MB, new SingleColumnType(ColumnType.LONG), new SingleColumnType(columnType), 64, 0.5);
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "value type is not supported"));
            }
        });
    }
}