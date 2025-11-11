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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RecordArrayTest extends AbstractCairoTest {
    public static final long SIZE_4M = 2 * 1024 * 1024L;
    private static final BytecodeAssembler asm = new BytecodeAssembler();
    private static final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();

    @Test
    public void testPseudoRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            int N = 10000;
            CreateTableTestUtils.createTestTable(N, new Rnd(), new TestRecord.ArrayBinarySequence());
            try (
                    TableReader reader = newOffPoolReader(configuration, "x");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                entityColumnFilter.of(reader.getMetadata().getColumnCount());
                RecordSink recordSink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter);
                try (RecordArray chain = new RecordArray(reader.getMetadata(), recordSink, SIZE_4M, Integer.MAX_VALUE)) {
                    LongList rows = new LongList();
                    Record cursorRecord = cursor.getRecord();
                    chain.setSymbolTableResolver(cursor);
                    while (cursor.hasNext()) {
                        rows.add(chain.put(cursorRecord));
                    }

                    Assert.assertEquals(N, rows.size());
                    cursor.toTop();

                    final Record rec = chain.getRecordB();
                    cursor.toTop();

                    for (int i = 0, n = rows.size(); i < n; i++) {
                        long row = rows.getQuick(i);
                        Assert.assertTrue(cursor.hasNext());
                        chain.recordAt(rec, row);
                        Assert.assertEquals(row, rec.getRowId());
                        assertSame(cursorRecord, rec, reader.getMetadata());
                    }
                }
            }
        });
    }

    @Test
    public void testReuseWithClear() throws Exception {
        testChainReuseWithClearFunction(RecordArray::clear);
    }

    @Test
    public void testReuseWithClose() throws Exception {
        testChainReuseWithClearFunction(RecordArray::close);
    }

    @Test
    public void testReuseWithReleaseCursor() throws Exception {
        testChainReuseWithClearFunction(RecordArray::close);
    }

    @Test
    public void testSkipAndRefill() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("y", ColumnType.INT));
            metadata.add(new TableColumnMetadata("z", ColumnType.INT));

            ListColumnFilter filter = new ListColumnFilter();
            filter.add(1);
            filter.add(-2);
            filter.add(3);

            RecordSink sink = RecordSinkFactory.getInstance(asm, metadata, filter);

            long[] cols = new long[metadata.getColumnCount()];

            final ObjList<Function> funcs = new ObjList<>();
            funcs.add(new LongFunction() {
                @Override
                public long getLong(Record rec) {
                    return cols[0];
                }

                @Override
                public boolean isThreadSafe() {
                    return true;
                }
            });

            funcs.add(null);
            funcs.add(new IntFunction() {
                @Override
                public int getInt(Record rec) {
                    return (int) cols[2];
                }

                @Override
                public boolean isThreadSafe() {
                    return true;
                }
            });

            final VirtualRecord rec = new VirtualRecord(funcs);
            try (RecordArray chain = new RecordArray(metadata, sink, SIZE_4M, Integer.MAX_VALUE)) {
                cols[0] = 100;
                cols[2] = 200;
                long o = chain.put(rec);

                // out of band update of column
                Unsafe.getUnsafe().putInt(chain.addressOf(chain.getOffsetOfColumn(o, 1)), 55);

                cols[0] = 110;
                cols[2] = 210;
                o = chain.put(rec);
                Unsafe.getUnsafe().putInt(chain.getAddress(o, 1), 66);

                AbstractCairoTest.sink.clear();
                chain.toTop();
                final Record r = chain.getRecord();
                while (chain.hasNext()) {
                    TestUtils.println(r, metadata, AbstractCairoTest.sink);
                }

                String expected = "100\t55\t200\n" +
                        "110\t66\t210\n";

                TestUtils.assertEquals(expected, AbstractCairoTest.sink);

                AbstractCairoTest.sink.clear();
                chain.toBottom();
                while (chain.hasPrev()) {
                    TestUtils.println(r, metadata, AbstractCairoTest.sink);
                }

                String expectedBack = "110\t66\t210\n" +
                        "100\t55\t200\n";

                TestUtils.assertEquals(expectedBack, AbstractCairoTest.sink);
            }
        });
    }

    @Test
    public void testWriteAndRead() throws Exception {
        assertMemoryLeak(
                () -> {
                    final int N = 10000 * 2;
                    CreateTableTestUtils.createTestTable(N, new Rnd(), new TestRecord.ArrayBinarySequence());
                    try (TableReader reader = newOffPoolReader(configuration, "x")) {
                        entityColumnFilter.of(reader.getMetadata().getColumnCount());
                        RecordSink recordSink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter);

                        try (RecordArray chain = new RecordArray(reader.getMetadata(), recordSink, 4 * 1024 * 1024L, Integer.MAX_VALUE)) {
                            populateChain(chain, reader);
                            assertChain(chain, N, reader);
                            assertChain(chain, N, reader);
                        }
                    }
                }
        );
    }

    private static void populateChain(RecordArray chain, TableReader reader) {
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            final Record record = cursor.getRecord();
            chain.setSymbolTableResolver(cursor);
            while (cursor.hasNext()) {
                chain.put(record);
            }
        }
    }

    private void assertChain(RecordArray chain, long expectedCount, TableReader reader) {
        long count = 0L;
        chain.toTop();
        Record chainRecord = chain.getRecord();
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            Record readerRecord = cursor.getRecord();
            chain.setSymbolTableResolver(cursor);
            LongList rows = new LongList();

            while (chain.hasNext()) {
                Assert.assertTrue(cursor.hasNext());
                rows.add(readerRecord.getRowId());
                assertSame(readerRecord, chainRecord, reader.getMetadata());
                count++;
            }
            Assert.assertEquals(expectedCount, count);

            chain.toBottom();
            Record recordB = cursor.getRecordB();
            int size = rows.size() - 1;
            while (chain.hasPrev()) {
                assert size >= 0;
                cursor.recordAt(recordB, rows.getQuick(size));
                assertSame(recordB, chainRecord, reader.getMetadata());
                size--;
            }
            assert size == -1;
        }
    }

    private void assertSame(Record expected, Record actual, RecordMetadata metadata) {
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                case ColumnType.INT:
                    Assert.assertEquals(expected.getInt(i), actual.getInt(i));
                    break;
                case ColumnType.IPv4:
                    Assert.assertEquals(expected.getIPv4(i), actual.getIPv4(i));
                    break;
                case ColumnType.DOUBLE:
                    Assert.assertEquals(expected.getDouble(i), actual.getDouble(i), 0.000000001D);
                    break;
                case ColumnType.LONG:
                    Assert.assertEquals(expected.getLong(i), actual.getLong(i));
                    break;
                case ColumnType.DATE:
                    Assert.assertEquals(expected.getDate(i), actual.getDate(i));
                    break;
                case ColumnType.TIMESTAMP:
                    Assert.assertEquals(expected.getTimestamp(i), actual.getTimestamp(i));
                    break;
                case ColumnType.BOOLEAN:
                    Assert.assertEquals(expected.getBool(i), actual.getBool(i));
                    break;
                case ColumnType.BYTE:
                    Assert.assertEquals(expected.getByte(i), actual.getByte(i));
                    break;
                case ColumnType.SHORT:
                    Assert.assertEquals(expected.getShort(i), actual.getShort(i));
                    break;
                case ColumnType.SYMBOL:
                    TestUtils.assertEquals(expected.getSymA(i), actual.getSymA(i));
                    break;
                case ColumnType.FLOAT:
                    Assert.assertEquals(expected.getFloat(i), actual.getFloat(i), 0.00000001f);
                    break;
                case ColumnType.STRING:
                    CharSequence e = expected.getStrA(i);
                    CharSequence cs1 = actual.getStrA(i);
                    CharSequence cs2 = actual.getStrB(i);
                    TestUtils.assertEquals(e, cs1);
                    Assert.assertFalse(cs1 != null && cs1 == cs2);
                    TestUtils.assertEquals(e, cs2);
                    if (cs1 == null) {
                        Assert.assertEquals(TableUtils.NULL_LEN, actual.getStrLen(i));
                    } else {
                        Assert.assertEquals(cs1.length(), actual.getStrLen(i));
                    }
                    break;
                case ColumnType.VARCHAR:
                    Utf8Sequence us = expected.getVarcharA(i);
                    Utf8Sequence us1 = actual.getVarcharA(i);
                    Utf8Sequence us2 = actual.getVarcharB(i);
                    TestUtils.assertEquals(us, us1);
                    Assert.assertFalse(us1 != null && us1 == us2);
                    TestUtils.assertEquals(us, us2);
                    if (us1 == null) {
                        Assert.assertEquals(TableUtils.NULL_LEN, actual.getVarcharSize(i));
                    } else {
                        Assert.assertEquals(us1.size(), actual.getVarcharSize(i));
                    }
                    if (us != null && us1 != null && us2 != null) {
                        Assert.assertEquals(us.isAscii(), us1.isAscii());
                        Assert.assertEquals(us.isAscii(), us2.isAscii());
                    }
                    break;
                case ColumnType.BINARY:
                    TestUtils.assertEquals(expected.getBin(i), actual.getBin(i), actual.getBinLen(i));
                    break;
                case ColumnType.UUID:
                    Assert.assertEquals(expected.getLong128Hi(i), actual.getLong128Hi(i));
                    Assert.assertEquals(expected.getLong128Lo(i), actual.getLong128Lo(i));
                    break;
                case ColumnType.DECIMAL8:
                    Assert.assertEquals(expected.getDecimal8(i), actual.getDecimal8(i));
                    break;
                case ColumnType.DECIMAL16:
                    Assert.assertEquals(expected.getDecimal16(i), actual.getDecimal16(i));
                    break;
                case ColumnType.DECIMAL32:
                    Assert.assertEquals(expected.getDecimal32(i), actual.getDecimal32(i));
                    break;
                case ColumnType.DECIMAL64:
                    Assert.assertEquals(expected.getDecimal64(i), actual.getDecimal64(i));
                    break;
                case ColumnType.DECIMAL128: {
                    Decimal128 expectedDecimal = new Decimal128();
                    expected.getDecimal128(i, expectedDecimal);
                    Decimal128 actualDecimal = new Decimal128();
                    actual.getDecimal128(i, actualDecimal);
                    Assert.assertEquals(expectedDecimal, actualDecimal);
                    break;
                }
                case ColumnType.DECIMAL256: {
                    Decimal256 expectedDecimal = new Decimal256();
                    expected.getDecimal256(i, expectedDecimal);
                    Decimal256 actualDecimal = new Decimal256();
                    actual.getDecimal256(i, actualDecimal);
                    Assert.assertEquals(expectedDecimal, actualDecimal);
                    break;
                }
                default:
                    throw CairoException.critical(0).put("Record chain does not support: ").put(ColumnType.nameOf(metadata.getColumnType(i)));
            }
        }
    }

    private void testChainReuseWithClearFunction(ClearFunc clear) throws Exception {
        assertMemoryLeak(() -> {
            final int N = 10000;
            Rnd rnd = new Rnd();

            // in the spirit of using only what's available in this package
            // we create temporary table the hard way

            CreateTableTestUtils.createTestTable(N, rnd, new TestRecord.ArrayBinarySequence());
            try (TableReader reader = newOffPoolReader(configuration, "x")) {

                entityColumnFilter.of(reader.getMetadata().getColumnCount());
                RecordSink recordSink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter);
                try (RecordArray chain = new RecordArray(reader.getMetadata(), recordSink, 4 * 1024 * 1024L, Integer.MAX_VALUE)) {
                    populateChain(chain, reader);
                    assertChain(chain, N, reader);

                    clear.clear(chain);

                    populateChain(chain, reader);
                    assertChain(chain, N, reader);
                }
            }
        });
    }

    @FunctionalInterface
    private interface ClearFunc {
        void clear(RecordArray chain);
    }
}
