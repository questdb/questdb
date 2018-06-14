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

public class QMapTest extends AbstractCairoTest {

    @Test
    public void testAppendExisting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            int N = 10;
            try (QMap map = new QMap(
                    1024 * 1024,
                    new SingleColumnType(ColumnType.STRING),
                    new SingleColumnType(ColumnType.LONG),
                    N / 2,
                    0.9)) {
                ObjList<String> keys = new ObjList<>();
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(11);
                    keys.add(s.toString());
                    QMap.Key key = map.withKey();
                    key.putStr(s);

                    QMap.Value value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putLong(i + 1);
                }
                Assert.assertEquals(N, map.size());

                for (int i = 0, n = keys.size(); i < n; i++) {
                    QMap.Key key = map.withKey();
                    CharSequence s = keys.getQuick(i);
                    key.putStr(s);
                    QMap.Value value = key.createValue();
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
            int N = 10000000;
            try (QMap map = new QMap(
                    1024 * 1024,
                    new SingleColumnType(ColumnType.STRING),
                    new SingleColumnType(ColumnType.LONG),
                    N / 2, 0.9)) {
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(11);
                    QMap.Key key = map.withKey();
                    key.putStr(s);
                    QMap.Value value = key.createValue();
                    value.putLong(i + 1);
                }
                Assert.assertEquals(N, map.size());

                long expectedAppendOffset = map.getAppendOffset();

                rnd.reset();
                for (int i = 0; i < N; i++) {
                    CharSequence s = rnd.nextChars(11);
                    QMap.Key key = map.withKey();
                    key.putStr(s);
                    QMap.Value value = key.findValue();
                    Assert.assertNotNull(value);
                    Assert.assertEquals(i + 1, value.getLong(0));
                }
                Assert.assertEquals(N, map.size());
                Assert.assertEquals(expectedAppendOffset, map.getAppendOffset());
            }
        });
    }

    @Test
    public void testKeyLookup() {
        // tweak capacity in such a way that we only have one spare slot before resize is needed
        // this way algo that shuffles "foreign" slots away should face problems
        double loadFactor = 0.9;
        try (QMap map = new QMap(
                1024 * 1024,
                new SingleColumnType(ColumnType.STRING),
                new SingleColumnType(ColumnType.LONG),
                12,
                loadFactor,
                new MockHash())) {
            QMap.Key key;
            QMap.Value value;

            key = map.withKey();
            key.putStr("000-ABCDE");
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putDouble(12.5);

            // difference in last character
            key = map.withKey();
            key.putStr("000-ABCDG");
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putDouble(11.5);

            // different hash code
            key = map.withKey();
            key.putStr("100-ABCDE");
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putDouble(10.5);

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
            key.putStr("017-ABCDE");
            Assert.assertNull(key.findValue());
        }
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
        class MockHash implements QMap.HashFunction {
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
        try (QMap map = new QMap(
                1024 * 1024,
                new SingleColumnType(ColumnType.STRING),
                new SingleColumnType(ColumnType.LONG),
                (long) (N * loadFactor), loadFactor, new MockHash())) {

            // assert that key capacity is what we expect, otherwise this test would be useless
            Assert.assertEquals(N, map.getActualCapacity());

            long target = map.getKeyCapacity();

            sink.clear();
            sink.put('0');
            populate(rnd, sink, map, 0, target - 1, 1);

            sink.clear();
            sink.put('1');

            populate(rnd, sink, map, target - 1, M + N, 1);

            // assert result

            rnd.reset();

            sink.clear();
            sink.put('0');
            assertMap(rnd, sink, map, 0, target - 1, 1);

            sink.clear();
            sink.put('1');

            assertMap(rnd, sink, map, target - 1, M + N, 1);
        }
    }

    private void assertMap(Rnd rnd, StringSink sink, QMap map, long lo, long hi, int prefixLen) {
        for (long i = lo; i < hi; i++) {
            // keep the first character
            sink.clear(prefixLen);
            rnd.nextChars(sink, 5);
            QMap.Key key = map.withKey();
            key.putStr(sink);
            QMap.Value value = key.createValue();
            Assert.assertFalse(value.isNew());
            Assert.assertEquals(i + 1, value.getLong(0));
        }
    }

    private void populate(Rnd rnd, StringSink sink, QMap map, long lo, long hi, int prefixLen) {
        for (long i = lo; i < hi; i++) {
            // keep the first few characters
            sink.clear(prefixLen);
            rnd.nextChars(sink, 5);
            QMap.Key key = map.withKey();
            key.putStr(sink);
            QMap.Value value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(i + 1);
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
        try (QMap map = new QMap(
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
                QMap.Key key = map.withKey();
                key.putStr(sink);
                QMap.Value value = key.createValue();
                Assert.assertTrue(value.isNew());
                value.putLong(i + 1);
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
    public void testRecordAsKey() throws Exception {
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

            try (TableWriter writer = new TableWriter(configuration, "x")) {
                for (int i = 0; i < N; i++) {
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

                try (QMap map = new QMap(
                        1024 * 1024,
                        new SymbolAsStrTypes(reader.getMetadata()),
                        new SingleColumnType(ColumnType.LONG),
                        N,
                        0.9)) {
                    map.configureKeyAdaptor(asm, reader.getMetadata(), columns, true);

                    long counter = 0;
                    RecordCursor cursor = reader.getCursor();
                    while (cursor.hasNext()) {
                        QMap.Key key = map.withKey();
                        key.putRecord(cursor.next());
                        QMap.Value value = key.createValue();
                        value.putLong(++counter);
                    }

                    long c = 0;
                    rnd.reset();
                    for (Record record : map.getCursor()) {
                        System.out.println(c);
                        // value part
                        Assert.assertEquals(++c, record.getLong(0));
                        Assert.assertEquals(rnd.nextByte(), record.getByte(1));
                        Assert.assertEquals(rnd.nextShort(), record.getShort(2));
                        if (rnd.nextInt() % 4 == 0) {
                            Assert.assertEquals(Numbers.INT_NaN, record.getInt(3));
                        } else {
                            Assert.assertEquals(rnd.nextInt(), record.getInt(3));
                        }

                        if (rnd.nextInt() % 4 == 0) {
                            Assert.assertEquals(Numbers.LONG_NaN, record.getLong(4));
                        } else {
                            Assert.assertEquals(rnd.nextLong(), record.getLong(4));
                        }

                        if (rnd.nextInt() % 4 == 0) {
                            Assert.assertEquals(Numbers.LONG_NaN, record.getDate(5));
                        } else {
                            Assert.assertEquals(rnd.nextLong(), record.getDate(5));
                        }

                        if (rnd.nextInt() % 4 == 0) {
                            Assert.assertEquals(Numbers.LONG_NaN, record.getTimestamp(6));
                        } else {
                            Assert.assertEquals(rnd.nextLong(), record.getTimestamp(6));
                        }

                        if (rnd.nextInt() % 4 == 0) {
                            Assert.assertTrue(Float.isNaN(record.getFloat(7)));
                        } else {
                            Assert.assertEquals(rnd.nextFloat2(), record.getFloat(7), 0.00000001f);
                        }

                        if (rnd.nextInt() % 4 == 0) {
                            Assert.assertTrue(Double.isNaN(record.getDouble(8)));
                        } else {
                            Assert.assertEquals(rnd.nextDouble2(), record.getDouble(8), 0.0000000001d);
                        }

                        if (rnd.nextInt() % 4 == 0) {
                            Assert.assertNull(record.getStr(9));
                            Assert.assertNull(record.getStrB(9));
                            Assert.assertEquals(-1, record.getStrLen(9));
                        } else {
                            CharSequence tmp = rnd.nextChars(5);
                            TestUtils.assertEquals(tmp, record.getStr(9));
                            TestUtils.assertEquals(tmp, record.getStrB(9));
                            Assert.assertEquals(tmp.length(), record.getStrLen(9));
                        }
                        // we are storing symbol as string, assert as such

                        if (rnd.nextInt() % 4 == 0) {
                            Assert.assertNull(record.getStr(10));
                        } else {
                            TestUtils.assertEquals(rnd.nextChars(3), record.getStr(10));
                        }

                        Assert.assertEquals(rnd.nextBoolean(), record.getBool(11));

                        if (rnd.nextInt() % 4 == 0) {
                            TestUtils.assertEquals(null, record.getBin(12), record.getBinLen(12));
                        } else {
                            binarySequence.of(rnd.nextBytes(25));
                            TestUtils.assertEquals(binarySequence, record.getBin(12), record.getBinLen(12));
                        }

                    }
                    Assert.assertEquals(counter, c);
                }
            }
        });
    }

    // This hash function will use first three characters as hash code
    // we need decent spread of hash codes making single character not enough
    private class MockHash implements QMap.HashFunction {
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