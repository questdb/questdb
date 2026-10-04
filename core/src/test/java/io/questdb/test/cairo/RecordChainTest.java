/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.ByteFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RecordChainTest extends AbstractCairoTest {
    public static final long SIZE_4M = 4 * 1024 * 1024L;
    private static final BytecodeAssembler asm = new BytecodeAssembler();
    private static final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();

    @Test
    public void testAppendFixedRecords() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("i", ColumnType.INT));
            metadata.add(new TableColumnMetadata("b", ColumnType.BYTE));
            metadata.add(new TableColumnMetadata("d", ColumnType.DOUBLE));
            entityColumnFilter.of(metadata.getColumnCount());
            RecordSink sink = RecordSinkFactory.getInstance(configuration, asm, metadata, entityColumnFilter);
            final long[] x = new long[1];
            final ObjList<Function> funcs = new ObjList<>();
            funcs.add(new LongFunction() {
                @Override
                public long getLong(Record rec) {
                    return x[0];
                }
            });
            funcs.add(new IntFunction() {
                @Override
                public int getInt(Record rec) {
                    return (int) (x[0] * 3);
                }
            });
            funcs.add(new ByteFunction() {
                @Override
                public byte getByte(Record rec) {
                    return (byte) x[0];
                }
            });
            funcs.add(new DoubleFunction() {
                @Override
                public double getDouble(Record rec) {
                    return x[0] / 7.0;
                }
            });
            final VirtualRecord rec = new VirtualRecord(funcs);
            final Rnd rnd = TestUtils.generateRandom(LOG);
            // 4 KB pages: the batches below cross many of them, and the chain grows under them
            try (RecordChain chain = new RecordChain(metadata, sink, 4096, Integer.MAX_VALUE)) {
                final long stride = chain.getFixedRecordStride();
                // a link, then 8 + 4 + 1 + 8 bytes of columns
                Assert.assertEquals(8 + 21, stride);
                for (int fill = 0; fill < 3; fill++) {
                    // the first fill grows the chain, the others reuse its memory
                    if (fill == 2) {
                        chain.clearKeepingMemory(Long.MAX_VALUE);
                    } else {
                        chain.rewind(fill * 500);
                    }
                    long prev = -1;
                    long value = 0;
                    final int total = 1000 + fill;
                    while (value < total) {
                        if (rnd.nextInt(4) == 0) {
                            // a record put the usual way, between batches
                            x[0] = value++;
                            prev = chain.put(rec, prev);
                            continue;
                        }
                        final int n = (int) Math.min(total - value, 1 + rnd.nextInt(70));
                        final long first = chain.appendFixedRecords(prev, n);
                        final long address = chain.addressOf(first);
                        for (int r = 0; r < n; r++) {
                            final long v = value++;
                            final long a = address + r * stride;
                            Unsafe.putLong(a + chain.getOffsetOfColumn(0, 0), v);
                            Unsafe.putInt(a + chain.getOffsetOfColumn(0, 1), (int) (v * 3));
                            Unsafe.putByte(a + chain.getOffsetOfColumn(0, 2), (byte) v);
                            Unsafe.putDouble(a + chain.getOffsetOfColumn(0, 3), v / 7.0);
                        }
                        prev = first + (n - 1) * stride;
                        // the record a later put links to is the last one appended
                        Assert.assertEquals(-1, chain.getNextRecordOffset(prev));
                    }
                    // read back by the links, by record offset and as one sequential block
                    chain.toTop();
                    final Record r = chain.getRecord();
                    long i = 0;
                    while (chain.hasNext()) {
                        Assert.assertEquals(i, r.getLong(0));
                        Assert.assertEquals((int) (i * 3), r.getInt(1));
                        Assert.assertEquals((byte) i, r.getByte(2));
                        Assert.assertEquals(i / 7.0, r.getDouble(3), 0.0);
                        i++;
                    }
                    Assert.assertEquals(total, i);
                    chain.toTop();
                    final RecordBlock block = chain.peekSequentialRecordBlock(Integer.MAX_VALUE);
                    Assert.assertNotNull(block);
                    Assert.assertEquals(total, block.getRowCount());
                    for (int row = 0; row < total; row++) {
                        Assert.assertEquals(row, Unsafe.getLong(block.getColumnAddress(0) + row * block.getColumnStride(0)));
                        Assert.assertEquals(row / 7.0, Unsafe.getDouble(block.getColumnAddress(3) + row * block.getColumnStride(3)), 0.0);
                    }
                }
            }
        });
    }

    @Test
    public void testClearKeepingMemory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
            entityColumnFilter.of(metadata.getColumnCount());
            RecordSink sink = RecordSinkFactory.getInstance(configuration, asm, metadata, entityColumnFilter);
            final long[] x = new long[1];
            final ObjList<Function> funcs = new ObjList<>();
            funcs.add(new LongFunction() {
                @Override
                public long getLong(Record rec) {
                    return x[0];
                }
            });
            final VirtualRecord rec = new VirtualRecord(funcs);
            try (RecordChain chain = new RecordChain(metadata, sink, 4096, Integer.MAX_VALUE)) {
                final long memBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                for (int fill = 0; fill < 4; fill++) {
                    long prev = -1;
                    for (int i = 0; i < 1000; i++) {
                        x[0] = fill * 10_000L + i;
                        prev = chain.put(rec, prev);
                    }
                    chain.toTop();
                    long i = 0;
                    while (chain.hasNext()) {
                        Assert.assertEquals(fill * 10_000L + i++, chain.getRecord().getLong(0));
                    }
                    Assert.assertEquals(1000, i);
                    final long memFilled = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                    Assert.assertTrue(memFilled > memBefore);
                    // cleared with its records still to be read: they are gone at once, as with clear()
                    chain.toTop();
                    if (fill < 2) {
                        // within the limit: the records go, the memory stays
                        chain.clearKeepingMemory(1000 * chain.getFixedRecordStride() * 2);
                        Assert.assertEquals(memFilled, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                    } else {
                        // above it: given back, as clear() does
                        chain.clearKeepingMemory(1000);
                        Assert.assertEquals(memBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                    }
                    Assert.assertFalse(chain.hasNext());
                    chain.toTop();
                    Assert.assertFalse(chain.hasNext());
                    Assert.assertNull(chain.peekSequentialRecordBlock(10));
                }
            }
        });
    }

    @Test
    public void testClear() throws Exception {
        assertMemoryLeak(() -> {
            CreateTableTestUtils.createTestTable(10000, new Rnd(), new TestRecord.ArrayBinarySequence());
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                entityColumnFilter.of(reader.getColumnCount());
                RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, reader.getMetadata(), entityColumnFilter);
                try (RecordChain chain = new RecordChain(reader.getMetadata(), recordSink, SIZE_4M, Integer.MAX_VALUE)) {
                    Assert.assertFalse(chain.hasNext());
                    populateChain(chain, reader);
                    chain.toTop();
                    Assert.assertTrue(chain.hasNext());
                    chain.clear();
                    chain.toTop();
                    Assert.assertFalse(chain.hasNext());
                }
            }
        });
    }

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
                RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, reader.getMetadata(), entityColumnFilter);
                try (RecordChain chain = new RecordChain(reader.getMetadata(), recordSink, SIZE_4M, Integer.MAX_VALUE)) {
                    LongList rows = new LongList();
                    Record cursorRecord = cursor.getRecord();

                    chain.setSymbolTableResolver(cursor);

                    long o = -1L;
                    while (cursor.hasNext()) {
                        o = chain.put(cursorRecord, o);
                        rows.add(o);
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
        testChainReuseWithClearFunction(RecordChain::clear);
    }

    @Test
    public void testReuseWithClose() throws Exception {
        testChainReuseWithClearFunction(RecordChain::close);
    }

    @Test
    public void testReuseWithReleaseCursor() throws Exception {
        testChainReuseWithClearFunction(RecordChain::close);
    }

    @Test
    public void testRewindKeepsMemoryAndReservesUpFront() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("s", ColumnType.STRING));
            entityColumnFilter.of(metadata.getColumnCount());
            RecordSink sink = RecordSinkFactory.getInstance(configuration, asm, metadata, entityColumnFilter);
            final long[] x = new long[1];
            final ObjList<Function> funcs = new ObjList<>();
            funcs.add(new LongFunction() {
                @Override
                public long getLong(Record rec) {
                    return x[0];
                }
            });
            funcs.add(new StrFunction() {
                @Override
                public CharSequence getStrA(Record rec) {
                    return x[0] % 3 == 0 ? null : "v" + x[0];
                }

                @Override
                public CharSequence getStrB(Record rec) {
                    return getStrA(rec);
                }
            });
            final VirtualRecord rec = new VirtualRecord(funcs);
            // 64 KB pages: 1000 records of 8 + 8 + 8 bytes plus strings span several
            try (RecordChain chain = new RecordChain(metadata, sink, 64 * 1024, Integer.MAX_VALUE)) {
                long memAfterFirstFill = 0;
                for (int fill = 0; fill < 3; fill++) {
                    final int n = 1000 + fill;
                    chain.rewind(n);
                    chain.toTop();
                    Assert.assertFalse(chain.hasNext());
                    if (fill > 0) {
                        // the memory of the earlier fill was kept, not freed and allocated again
                        Assert.assertEquals(memAfterFirstFill, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                    }
                    long o = -1;
                    for (int i = 0; i < n; i++) {
                        x[0] = fill * 10_000L + i;
                        o = chain.put(rec, o);
                    }
                    if (fill == 0) {
                        memAfterFirstFill = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                    }
                    chain.toTop();
                    final Record r = chain.getRecord();
                    int i = 0;
                    while (chain.hasNext()) {
                        final long expected = fill * 10_000L + i;
                        Assert.assertEquals(expected, r.getLong(0));
                        TestUtils.assertEquals(expected % 3 == 0 ? null : "v" + expected, r.getStrA(1));
                        i++;
                    }
                    Assert.assertEquals(n, i);
                }
                // after clear() the next rewind allocates again and the chain still works
                chain.clear();
                chain.rewind(10);
                x[0] = 7;
                chain.put(rec, -1);
                chain.toTop();
                Assert.assertTrue(chain.hasNext());
                Assert.assertEquals(7, chain.getRecord().getLong(0));
                Assert.assertFalse(chain.hasNext());
            }
        });
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

            RecordSink sink = RecordSinkFactory.getInstance(configuration, asm, metadata, filter);

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
            try (RecordChain chain = new RecordChain(metadata, sink, SIZE_4M, Integer.MAX_VALUE)) {
                long o = -1;
                cols[0] = 100;
                cols[2] = 200;
                o = chain.put(rec, o);

                // out of band update of column
                Unsafe.putInt(chain.addressOf(chain.getOffsetOfColumn(o, 1)), 55);

                cols[0] = 110;
                cols[2] = 210;
                o = chain.put(rec, o);
                Unsafe.putInt(chain.getAddress(o, 1), 66);

                AbstractCairoTest.sink.clear();
                chain.toTop();
                final Record r = chain.getRecord();
                while (chain.hasNext()) {
                    TestUtils.println(r, metadata, AbstractCairoTest.sink);
                }

                String expected = """
                        100\t55\t200
                        110\t66\t210
                        """;

                TestUtils.assertEquals(expected, AbstractCairoTest.sink);
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
                        RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, reader.getMetadata(), entityColumnFilter);

                        try (RecordChain chain = new RecordChain(reader.getMetadata(), recordSink, 4 * 1024 * 1024L, Integer.MAX_VALUE)) {
                            populateChain(chain, reader);
                            assertChain(chain, N, reader);
                            assertChain(chain, N, reader);
                        }
                    }
                }
        );
    }

    private static void populateChain(RecordChain chain, TableReader reader) {
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            final Record record = cursor.getRecord();
            chain.setSymbolTableResolver(cursor);
            long o = -1L;
            while (cursor.hasNext()) {
                o = chain.put(record, o);
            }
        }
    }

    private void assertChain(RecordChain chain, long expectedCount, TableReader reader) {
        long count = 0L;
        chain.toTop();
        Record chainRecord = chain.getRecord();
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            Record readerRecord = cursor.getRecord();
            chain.setSymbolTableResolver(cursor);

            while (chain.hasNext()) {
                Assert.assertTrue(cursor.hasNext());
                assertSame(readerRecord, chainRecord, reader.getMetadata());
                count++;
            }
            Assert.assertEquals(expectedCount, count);
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
                RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, reader.getMetadata(), entityColumnFilter);
                try (RecordChain chain = new RecordChain(reader.getMetadata(), recordSink, 4 * 1024 * 1024L, Integer.MAX_VALUE)) {
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
        void clear(RecordChain chain);
    }
}
