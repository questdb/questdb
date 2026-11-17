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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
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
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.griffin.engine.TestBinarySequence;
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
                RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, reader.getMetadata(), entityColumnFilter);
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
    public void testFixedWidthSchemaSkipsAuxMem() throws Exception {
        // Verifies the auxMem-skip path: when every column is fixed-width (varOffset == 0),
        // RecordArray computes row offsets as N * fixOffset instead of allocating the
        // per-row index. beginRecord/recordAtRowIndex/hasNext must round-trip correctly.
        TestUtils.assertMemoryLeak(() -> {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("a", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("b", ColumnType.LONG));

            // sink is not invoked because we only beginRecord + putLong directly
            try (RecordArray chain = new RecordArray(metadata, null, SIZE_4M, Integer.MAX_VALUE)) {
                final int N = 1_000;
                for (int i = 0; i < N; i++) {
                    chain.beginRecord();
                    chain.putLong(i);
                    chain.putLong(i * 100L);
                }

                final Record rec = chain.getRecord();
                for (int i = 0; i < N; i++) {
                    chain.recordAtRowIndex(rec, i);
                    Assert.assertEquals(i, rec.getLong(0));
                    Assert.assertEquals(i * 100L, rec.getLong(1));
                }

                // Sequential walk via hasNext should yield the same values in order.
                chain.toTop();
                long expected = 0;
                while (chain.hasNext()) {
                    Assert.assertEquals(expected, rec.getLong(0));
                    Assert.assertEquals(expected * 100L, rec.getLong(1));
                    expected++;
                }
                Assert.assertEquals(N, expected);
            }
        });
    }

    @Test
    public void testFixedWidthSchemaReuseAfterClear() throws Exception {
        // Round-trip beginRecord + clear() + beginRecord on an all-fixed-width schema
        // so the auxMem == null path is exercised through the reuse cycle (the LIGHT
        // window cursor reopens the narrow chain after close + clear).
        TestUtils.assertMemoryLeak(() -> {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("a", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("b", ColumnType.LONG));

            try (RecordArray chain = new RecordArray(metadata, null, SIZE_4M, Integer.MAX_VALUE)) {
                final int N = 1_000;
                for (int round = 0; round < 2; round++) {
                    chain.clear();
                    for (int i = 0; i < N; i++) {
                        chain.beginRecord();
                        chain.putLong(i + round);
                        chain.putLong((i + round) * 100L);
                    }

                    final Record rec = chain.getRecord();
                    for (int i = 0; i < N; i++) {
                        chain.recordAtRowIndex(rec, i);
                        Assert.assertEquals(i + round, rec.getLong(0));
                        Assert.assertEquals((i + round) * 100L, rec.getLong(1));
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
            try (RecordArray chain = new RecordArray(metadata, sink, SIZE_4M, Integer.MAX_VALUE)) {
                cols[0] = 100;
                cols[2] = 200;
                long o = chain.put(rec);

                // out of band update of column
                Unsafe.putInt(chain.addressOf(chain.getOffsetOfColumn(o, 1)), 55);

                cols[0] = 110;
                cols[2] = 210;
                o = chain.put(rec);
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

                AbstractCairoTest.sink.clear();
                chain.toBottom();
                while (chain.hasPrev()) {
                    TestUtils.println(r, metadata, AbstractCairoTest.sink);
                }

                String expectedBack = """
                        110\t66\t210
                        100\t55\t200
                        """;

                TestUtils.assertEquals(expectedBack, AbstractCairoTest.sink);
            }
        });
    }

    @Test
    public void testSkipAndRefillVarSize() throws Exception {
        // The var-size counterpart of testSkipAndRefill. A skipped var-size column must take
        // its own slot in the record header, or each var-size column after it reads the slot
        // of the one before. A skipped slot reads NULL until it is refilled, which is what an
        // array window function's pass 2 does through putArray().
        TestUtils.assertMemoryLeak(() -> {
            final int arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("a", ColumnType.VARCHAR));
            metadata.add(new TableColumnMetadata("w", arrayType));
            metadata.add(new TableColumnMetadata("b", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("v", ColumnType.VARCHAR));
            metadata.add(new TableColumnMetadata("c", ColumnType.VARCHAR));
            metadata.add(new TableColumnMetadata("d", ColumnType.BINARY));

            ListColumnFilter filter = new ListColumnFilter();
            filter.add(1);
            filter.add(-2);
            filter.add(3);
            filter.add(-4);
            filter.add(5);
            filter.add(6);

            // values of columns a, b and c, at record indexes 0, 2 and 4, and of column d
            final String[] values = new String[3];
            final TestBinarySequence[] bin = new TestBinarySequence[1];
            final Record rec = new Record() {
                @Override
                public BinarySequence getBin(int col) {
                    return bin[0];
                }

                @Override
                public CharSequence getStrA(int col) {
                    return values[col / 2];
                }

                @Override
                public Utf8Sequence getVarcharA(int col) {
                    final String value = values[col / 2];
                    return value != null ? new Utf8String(value) : null;
                }
            };

            for (int copierType : new int[]{RecordSinkFactory.SINK_TYPE_SINGLE_METHOD, RecordSinkFactory.SINK_TYPE_LOOPING}) {
                RecordSink sink = RecordSinkFactory.getInstance(copierConfiguration(copierType), asm, metadata, filter);
                try (
                        RecordArray chain = new RecordArray(metadata, sink, SIZE_4M, Integer.MAX_VALUE);
                        DirectArray array = new DirectArray(configuration)
                ) {
                    values[0] = "a0";
                    values[1] = "b0";
                    values[2] = "c0";
                    bin[0] = null;
                    final long row0 = chain.put(rec);
                    values[0] = "a1";
                    values[1] = null;
                    values[2] = "c1";
                    bin[0] = new TestBinarySequence().of(new byte[]{1, 2, 3});
                    final long row1 = chain.put(rec);

                    array.setType(arrayType);
                    array.setDimLen(0, 1);
                    array.applyShape();
                    array.putDouble(0, 1.5);
                    chain.putArray(row0, 1, array);
                    array.putDouble(0, 2.5);
                    chain.putArray(row1, 1, array);

                    AbstractCairoTest.sink.clear();
                    chain.toTop();
                    final Record r = chain.getRecord();
                    while (chain.hasNext()) {
                        TestUtils.println(r, metadata, AbstractCairoTest.sink);
                    }
                    TestUtils.assertEquals(
                            """
                                    a0\t[1.5]\tb0\t\tc0\t
                                    a1\t[2.5]\t\t\tc1\t00000000 01 02 03
                                    """,
                            AbstractCairoTest.sink
                    );

                    AbstractCairoTest.sink.clear();
                    chain.toBottom();
                    while (chain.hasPrev()) {
                        TestUtils.println(r, metadata, AbstractCairoTest.sink);
                    }
                    TestUtils.assertEquals(
                            """
                                    a1\t[2.5]\t\t\tc1\t00000000 01 02 03
                                    a0\t[1.5]\tb0\t\tc0\t
                                    """,
                            AbstractCairoTest.sink
                    );
                }
            }
        });
    }

    @Test
    public void testSkipVarSizeAcrossChunkBoundaries() throws Exception {
        // The chunked sink splits the copy across methods, while the chain's var-size slot
        // cursor runs on through all of them. Sliding a skipped array column across the first
        // chunk boundary makes it the last column of one method and then the first of the
        // next; the var-size columns around it keep their own slots either way.
        TestUtils.assertMemoryLeak(() -> {
            final int arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            final CairoConfiguration chunked = copierConfiguration(RecordSinkFactory.SINK_TYPE_CHUNKED);
            final Record rec = new Record() {
                @Override
                public int getInt(int col) {
                    return col;
                }

                @Override
                public Utf8Sequence getVarcharA(int col) {
                    return new Utf8String("v" + col);
                }
            };
            for (int paddingCount = 400; paddingCount <= 450; paddingCount++) {
                // a VARCHAR, the INT padding, the skipped array, a VARCHAR, 100 INTs, a VARCHAR
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                final ListColumnFilter filter = new ListColumnFilter();
                addColumn(metadata, filter, ColumnType.VARCHAR, false);
                for (int i = 0; i < paddingCount; i++) {
                    addColumn(metadata, filter, ColumnType.INT, false);
                }
                final int skipped = addColumn(metadata, filter, arrayType, true);
                addColumn(metadata, filter, ColumnType.VARCHAR, false);
                for (int i = 0; i < 100; i++) {
                    addColumn(metadata, filter, ColumnType.INT, false);
                }
                addColumn(metadata, filter, ColumnType.VARCHAR, false);

                final RecordSink sink = RecordSinkFactory.getInstance(chunked, asm, metadata, filter);
                final String sinkClass = sink.getClass().getName();
                Assert.assertTrue(sinkClass, sinkClass.startsWith("io.questdb.cairo.chunkedsink/"));
                try (
                        RecordArray chain = new RecordArray(metadata, sink, SIZE_4M, Integer.MAX_VALUE);
                        DirectArray array = new DirectArray(configuration)
                ) {
                    final long row = chain.put(rec);
                    array.setType(arrayType);
                    array.setDimLen(0, 1);
                    array.applyShape();
                    array.putDouble(0, paddingCount);
                    chain.putArray(row, skipped, array);

                    final Record r = chain.getRecord();
                    chain.toTop();
                    Assert.assertTrue(chain.hasNext());
                    for (int col = 0, n = metadata.getColumnCount(); col < n; col++) {
                        if (col == skipped) {
                            Assert.assertEquals(paddingCount, r.getArray(col, arrayType).getDouble(0), 0.0);
                        } else if (metadata.getColumnType(col) == ColumnType.VARCHAR) {
                            TestUtils.assertEquals("v" + col, r.getVarcharA(col));
                        } else {
                            Assert.assertEquals(col, r.getInt(col));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSkipVarSizeBesideFixedSizeSkips() throws Exception {
        // A fixed-size skip steps over bytes of the fixed-size area and a var-size skip over
        // a slot of the var-size header; neither may disturb the other. Skips of every fixed
        // width sit between var-size skips and copied columns, and once refilled the way the
        // window passes refill them, every column reads back its own value.
        TestUtils.assertMemoryLeak(() -> {
            final int arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            final ListColumnFilter filter = new ListColumnFilter();
            addColumn(metadata, filter, ColumnType.VARCHAR, false);
            addColumn(metadata, filter, ColumnType.BOOLEAN, true);
            addColumn(metadata, filter, arrayType, true);
            addColumn(metadata, filter, ColumnType.SHORT, true);
            addColumn(metadata, filter, ColumnType.INT, false);
            addColumn(metadata, filter, ColumnType.VARCHAR, true);
            addColumn(metadata, filter, ColumnType.LONG, true);
            addColumn(metadata, filter, ColumnType.STRING, false);
            addColumn(metadata, filter, ColumnType.UUID, true);
            addColumn(metadata, filter, arrayType, true);
            addColumn(metadata, filter, ColumnType.LONG256, true);
            addColumn(metadata, filter, ColumnType.DOUBLE, false);
            addColumn(metadata, filter, ColumnType.VARCHAR, false);

            final int[] row = new int[1];
            final Record rec = new Record() {
                @Override
                public double getDouble(int col) {
                    return row[0] + 0.5;
                }

                @Override
                public int getInt(int col) {
                    return row[0] * 10;
                }

                @Override
                public CharSequence getStrA(int col) {
                    return "s" + row[0];
                }

                @Override
                public Utf8Sequence getVarcharA(int col) {
                    return new Utf8String("v" + col + '_' + row[0]);
                }
            };

            for (int copierType : new int[]{RecordSinkFactory.SINK_TYPE_SINGLE_METHOD, RecordSinkFactory.SINK_TYPE_LOOPING}) {
                final RecordSink sink = RecordSinkFactory.getInstance(copierConfiguration(copierType), asm, metadata, filter);
                try (
                        RecordArray chain = new RecordArray(metadata, sink, SIZE_4M, Integer.MAX_VALUE);
                        DirectArray array = new DirectArray(configuration)
                ) {
                    final long[] offsets = new long[3];
                    for (int i = 0; i < offsets.length; i++) {
                        row[0] = i;
                        offsets[i] = chain.put(rec);
                    }

                    array.setType(arrayType);
                    for (int i = 0; i < offsets.length; i++) {
                        final long offset = offsets[i];
                        Unsafe.putByte(chain.getAddress(offset, 1), (byte) (i % 2));
                        Unsafe.putShort(chain.getAddress(offset, 3), (short) (i + 3));
                        Unsafe.putLong(chain.getAddress(offset, 6), i + 6L);
                        final long uuid = chain.getAddress(offset, 8);
                        Unsafe.putLong(uuid, i + 80L);
                        Unsafe.putLong(uuid + Long.BYTES, i + 81L);
                        final long long256 = chain.getAddress(offset, 10);
                        for (int k = 0; k < 4; k++) {
                            Unsafe.putLong(long256 + (long) k * Long.BYTES, i * 100L + k);
                        }
                        array.setDimLen(0, i + 1);
                        array.applyShape();
                        for (int k = 0; k <= i; k++) {
                            array.putDouble(k, i + k * 0.25);
                        }
                        chain.putArray(offset, 2, array);
                        array.putDouble(0, -i - 1.0);
                        chain.putArray(offset, 9, array);
                    }

                    final Record r = chain.getRecord();
                    chain.toTop();
                    for (int i = 0; i < offsets.length; i++) {
                        Assert.assertTrue(chain.hasNext());
                        TestUtils.assertEquals("v0_" + i, r.getVarcharA(0));
                        Assert.assertEquals(i % 2 == 1, r.getBool(1));
                        final ArrayView first = r.getArray(2, arrayType);
                        Assert.assertEquals(i + 1, first.getDimLen(0));
                        for (int k = 0; k <= i; k++) {
                            Assert.assertEquals(i + k * 0.25, first.getDouble(k), 0.0);
                        }
                        Assert.assertEquals(i + 3, r.getShort(3));
                        Assert.assertEquals(i * 10, r.getInt(4));
                        Assert.assertNull(r.getVarcharA(5));
                        Assert.assertEquals(i + 6L, r.getLong(6));
                        TestUtils.assertEquals("s" + i, r.getStrA(7));
                        Assert.assertEquals(i + 80L, r.getLong128Lo(8));
                        Assert.assertEquals(i + 81L, r.getLong128Hi(8));
                        final ArrayView second = r.getArray(9, arrayType);
                        Assert.assertEquals(i + 1, second.getDimLen(0));
                        Assert.assertEquals(-i - 1.0, second.getDouble(0), 0.0);
                        for (int k = 1; k <= i; k++) {
                            Assert.assertEquals(i + k * 0.25, second.getDouble(k), 0.0);
                        }
                        final Long256 long256 = r.getLong256A(10);
                        Assert.assertEquals(i * 100L, long256.getLong0());
                        Assert.assertEquals(i * 100L + 1, long256.getLong1());
                        Assert.assertEquals(i * 100L + 2, long256.getLong2());
                        Assert.assertEquals(i * 100L + 3, long256.getLong3());
                        Assert.assertEquals(i + 0.5, r.getDouble(11), 0.0);
                        TestUtils.assertEquals("v12_" + i, r.getVarcharA(12));
                    }
                    Assert.assertFalse(chain.hasNext());
                }
            }
        });
    }

    @Test
    public void testSkipVarSizeRefillAfterGrowthAndClear() throws Exception {
        // Pass 2 writes the array outputs once every row is in place, so the chain's memory
        // grows, and moves, between a row's copy and its refill. A 4 KiB page makes it grow
        // repeatedly over 10,000 rows, and clear() then empties the chain for a second round.
        TestUtils.assertMemoryLeak(() -> {
            final int arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            final ListColumnFilter filter = new ListColumnFilter();
            addColumn(metadata, filter, ColumnType.VARCHAR, false);
            addColumn(metadata, filter, arrayType, true);
            addColumn(metadata, filter, ColumnType.STRING, false);
            addColumn(metadata, filter, ColumnType.VARCHAR, true);
            addColumn(metadata, filter, ColumnType.VARCHAR, false);

            final int rowCount = 10_000;
            final int[] value = new int[1];
            final Record rec = new Record() {
                @Override
                public CharSequence getStrA(int col) {
                    return value[0] % 7 == 0 ? null : "b" + value[0];
                }

                @Override
                public Utf8Sequence getVarcharA(int col) {
                    return new Utf8String((col == 0 ? "a" : "c") + value[0]);
                }
            };

            for (int copierType : new int[]{RecordSinkFactory.SINK_TYPE_SINGLE_METHOD, RecordSinkFactory.SINK_TYPE_LOOPING}) {
                final RecordSink sink = RecordSinkFactory.getInstance(copierConfiguration(copierType), asm, metadata, filter);
                try (
                        RecordArray chain = new RecordArray(metadata, sink, 4096, Integer.MAX_VALUE);
                        DirectArray array = new DirectArray(configuration)
                ) {
                    final LongList offsets = new LongList(rowCount);
                    for (int round = 0; round < 2; round++) {
                        chain.clear();
                        offsets.clear();
                        for (int i = 0; i < rowCount; i++) {
                            value[0] = round * rowCount + i;
                            offsets.add(chain.put(rec));
                        }
                        array.setType(arrayType);
                        for (int i = 0; i < rowCount; i++) {
                            final int length = i % 5 + 1;
                            array.setDimLen(0, length);
                            array.applyShape();
                            for (int k = 0; k < length; k++) {
                                array.putDouble(k, round * rowCount + i + k);
                            }
                            chain.putArray(offsets.getQuick(i), 1, array);
                        }

                        final Record r = chain.getRecord();
                        chain.toTop();
                        for (int i = 0; i < rowCount; i++) {
                            Assert.assertTrue(chain.hasNext());
                            final int expected = round * rowCount + i;
                            TestUtils.assertEquals("a" + expected, r.getVarcharA(0));
                            final ArrayView refilled = r.getArray(1, arrayType);
                            Assert.assertEquals(i % 5 + 1, refilled.getDimLen(0));
                            for (int k = 0; k < i % 5 + 1; k++) {
                                Assert.assertEquals(expected + k, refilled.getDouble(k), 0.0);
                            }
                            if (expected % 7 == 0) {
                                Assert.assertNull(r.getStrA(2));
                            } else {
                                TestUtils.assertEquals("b" + expected, r.getStrA(2));
                            }
                            Assert.assertNull(r.getVarcharA(3));
                            TestUtils.assertEquals("c" + expected, r.getVarcharA(4));
                        }
                        Assert.assertFalse(chain.hasNext());
                    }
                }
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

                        try (RecordArray chain = new RecordArray(reader.getMetadata(), recordSink, 4 * 1024 * 1024L, Integer.MAX_VALUE)) {
                            populateChain(chain, reader);
                            assertChain(chain, N, reader);
                            assertChain(chain, N, reader);
                        }
                    }
                }
        );
    }

    private static int addColumn(GenericRecordMetadata metadata, ListColumnFilter filter, int type, boolean isSkipped) {
        final int index = metadata.getColumnCount();
        metadata.add(new TableColumnMetadata("c" + index, type));
        filter.add(isSkipped ? -index - 1 : index + 1);
        return index;
    }

    private static CairoConfiguration copierConfiguration(int copierType) {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public int getCopierType() {
                return copierType;
            }
        };
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
                RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, reader.getMetadata(), entityColumnFilter);
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
