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

package com.questdb.cairo;

import com.questdb.cairo.sql.Record;
import com.questdb.std.BytecodeAssembler;
import com.questdb.std.LongList;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RecordChainTest extends AbstractCairoTest {
    public static final long SIZE_4M = 4 * 1024 * 1024L;
    private static final GenericRecordMetadata metadata;
    private static final BytecodeAssembler asm = new BytecodeAssembler();
    private static final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();

    @Test
    public void testClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Record record = new TestRecord();
            entityColumnFilter.of(metadata.getColumnCount());
            RecordSink recordSink = RecordSinkFactory.getInstance(asm, metadata, entityColumnFilter, true);
            try (RecordChain chain = new RecordChain(metadata, recordSink, SIZE_4M)) {
                Assert.assertFalse(chain.hasNext());
                populateChain(chain, record);
                chain.toTop();
                Assert.assertTrue(chain.hasNext());
                chain.clear();
                chain.toTop();
                Assert.assertFalse(chain.hasNext());
            }
        });
    }

    @Test
    public void testPseudoRandomAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            entityColumnFilter.of(metadata.getColumnCount());
            RecordSink recordSink = RecordSinkFactory.getInstance(asm, metadata, entityColumnFilter, true);
            try (RecordChain chain = new RecordChain(metadata, recordSink, SIZE_4M)) {
                Record record = new TestRecord();
                LongList rows = new LongList();
                long o = -1L;
                for (int i = 0; i < N; i++) {
                    o = chain.put(record, o);
                    rows.add(o);
                }

                Assert.assertEquals(N, rows.size());

                Record expected = new TestRecord();

                for (int i = 0, n = rows.size(); i < n; i++) {
                    long row = rows.getQuick(i);
                    Record actual = chain.recordAt(row);
                    Assert.assertEquals(row, actual.getRowId());
                    assertSame(expected, actual);
                }

                Record expected2 = new TestRecord();
                Record rec2 = chain.newRecord();

                for (int i = 0, n = rows.size(); i < n; i++) {
                    long row = rows.getQuick(i);
                    chain.recordAt(rec2, row);
                    Assert.assertEquals(row, rec2.getRowId());
                    assertSame(expected2, rec2);
                }

                Record expected3 = new TestRecord();
                Record rec3 = chain.getRecord();

                for (int i = 0, n = rows.size(); i < n; i++) {
                    long row = rows.getQuick(i);
                    chain.recordAt(rec3, row);
                    Assert.assertEquals(row, rec3.getRowId());
                    assertSame(expected3, rec3);
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
    public void testWriteAndRead() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    final int N = 10000;
                    Record record = new TestRecord();
                    //3686831118
                    //2768496651

//                    CollectionRecordMetadata mm = new CollectionRecordMetadata();
//                    mm.add(new RecordColumnMetadataImpl("int", ColumnType.INT));
//                    mm.add(new RecordColumnMetadataImpl("long", ColumnType.LONG));
//                    mm.add(new RecordColumnMetadataImpl("short", ColumnType.SHORT));
//                    mm.add(new RecordColumnMetadataImpl("double", ColumnType.DOUBLE));
//                    mm.add(new RecordColumnMetadataImpl("float", ColumnType.FLOAT));
//                    mm.add(new RecordColumnMetadataImpl("str", ColumnType.STRING));
//                    mm.add(new RecordColumnMetadataImpl("byte", ColumnType.BYTE));
//                    mm.add(new RecordColumnMetadataImpl("date", ColumnType.DATE));
//                    mm.add(new RecordColumnMetadataImpl("bool", ColumnType.BOOLEAN));
//                    mm.add(new RecordColumnMetadataImpl("str2", ColumnType.STRING));
//                    mm.add(new RecordColumnMetadataImpl("sym", ColumnType.SYMBOL));
//
//
//                    TestRecord2 rec2 = new TestRecord2();
//                    try (RecordList records = new RecordList(mm, 4 * 1024 * 1024)) {
//                        long o = -1L;
//                        long t = 0;
//                        for (int i = -N; i < N; i++) {
//                            if (i == 0) {
//                                t = System.nanoTime();
//                            }
//                            o = records.append(rec2, o);
//                        }
//                        System.out.println(System.nanoTime() - t);
//                    }
//

                    entityColumnFilter.of(metadata.getColumnCount());
                    RecordSink recordSink = RecordSinkFactory.getInstance(asm, metadata, entityColumnFilter, true);

                    try (RecordChain chain = new RecordChain(metadata, recordSink, 4 * 1024 * 1024L)) {
                        long o = -1L;
//                        long t = 0;
                        for (int i = -N; i < N; i++) {
//                            if (i == 0) {
//                                t = System.nanoTime();
//                            }
                            o = chain.put(record, o);
                        }
//                        System.out.println("RecordChain append time: " + (System.nanoTime() - t));
                        assertChain(chain, new TestRecord(), N * 2);
                        assertChain(chain, new TestRecord(), N * 2);
                    }
                }
        );
    }

    private static void populateChain(RecordChain chain, Record record) {
        long o = -1L;
        for (int i = 0; i < 10000; i++) {
            o = chain.put(record, o);
        }
    }

    private void assertChain(RecordChain chain, Record r2, long expectedCount) {
        long count = 0L;
        chain.toTop();
        while (chain.hasNext()) {
            assertSame(r2, chain.next());
            count++;
        }
        Assert.assertEquals(expectedCount, count);
    }

    private void assertSame(Record expected, Record actual) {
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            switch (metadata.getColumnType(i)) {
                case ColumnType.INT:
                    Assert.assertEquals(expected.getInt(i), actual.getInt(i));
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
                    TestUtils.assertEquals(expected.getSym(i), actual.getSym(i));
                    break;
                case ColumnType.FLOAT:
                    Assert.assertEquals(expected.getFloat(i), actual.getFloat(i), 0.00000001f);
                    break;
                case ColumnType.STRING:
                    CharSequence e = expected.getStr(i);
                    CharSequence cs1 = actual.getStr(i);
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
                case ColumnType.BINARY:
                    TestUtils.assertEquals(expected.getBin(i), actual.getBin(i), actual.getBinLen(i));
                    break;
                default:
                    throw CairoException.instance(0).put("Record chain does not support: ").put(ColumnType.nameOf(metadata.getColumnType(i)));

            }
        }
    }

    private void testChainReuseWithClearFunction(ClearFunc clear) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Record record = new TestRecord();
            Record expected = new TestRecord();
            final int N = 10000;
            entityColumnFilter.of(metadata.getColumnCount());
            RecordSink recordSink = RecordSinkFactory.getInstance(asm, metadata, entityColumnFilter, true);
            try (RecordChain chain = new RecordChain(metadata, recordSink, 4 * 1024 * 1024L)) {

                populateChain(chain, record);
                assertChain(chain, expected, N);

                clear.clear(chain);

                populateChain(chain, record);
                assertChain(chain, expected, N);
            }
        });
    }

    @FunctionalInterface
    private interface ClearFunc {
        void clear(RecordChain chain);
    }
/*

    public class TestRecord2 implements com.questdb.store.Record {
        final Rnd rnd = new Rnd();


        @Override
        public boolean getBool(int col) {
            return rnd.nextBoolean();
        }

        @Override
        public byte getByte(int col) {
            return rnd.nextByte();
        }

        @Override
        public long getDate(int col) {
            return rnd.nextPositiveLong();
        }

        @Override
        public double getDouble(int col) {
            return rnd.nextDouble2();
        }

        @Override
        public float getFloat(int col) {
            return rnd.nextFloat2();
        }

        @Override
        public CharSequence getFlyweightStr(int col) {
            return rnd.nextInt() % 16 == 0 ? null : rnd.nextChars(15);
        }

        @Override
        public CharSequence getFlyweightStrB(int col) {
            return rnd.nextInt() % 16 == 0 ? null : rnd.nextChars(15);
        }

        @Override
        public int getInt(int col) {
            return rnd.nextInt();
        }

        @Override
        public long getLong(int col) {
            return rnd.nextLong();
        }

        @Override
        public long getRowId() {
            return -1;
        }

        @Override
        public short getShort(int col) {
            return rnd.nextShort();
        }

        @Override
        public int getStrLen(int col) {
            return 15;
        }

        @Override
        public CharSequence getSym(int col) {
            return rnd.nextChars(10);
        }
    }
*/

    static {
        metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("int", ColumnType.INT));
        metadata.add(new TableColumnMetadata("long", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("short", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("double", ColumnType.DOUBLE));
        metadata.add(new TableColumnMetadata("float", ColumnType.FLOAT));
        metadata.add(new TableColumnMetadata("str", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("byte", ColumnType.BYTE));
        metadata.add(new TableColumnMetadata("date", ColumnType.DATE));
        metadata.add(new TableColumnMetadata("bool", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("str2", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("sym", ColumnType.SYMBOL));
        metadata.add(new TableColumnMetadata("bin", ColumnType.BINARY));
        metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
    }
}