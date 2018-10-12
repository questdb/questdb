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
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.std.BytecodeAssembler;
import com.questdb.std.LongList;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RecordChainTest extends AbstractCairoTest {
    public static final long SIZE_4M = 4 * 1024 * 1024L;
    private static final BytecodeAssembler asm = new BytecodeAssembler();
    private static final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();

    @Test
    public void testClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoTestUtils.createTestTable(10000, new Rnd(), new TestRecord.ArrayBinarySequence());
            try (TableReader reader = new TableReader(configuration, "x")) {
                entityColumnFilter.of(reader.getColumnCount());
                RecordSink recordSink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter, false);
                try (RecordChain chain = new RecordChain(reader.getMetadata(), recordSink, SIZE_4M)) {
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
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            CairoTestUtils.createTestTable(N, new Rnd(), new TestRecord.ArrayBinarySequence());
            try (TableReader reader = new TableReader(configuration, "x")) {
                entityColumnFilter.of(reader.getMetadata().getColumnCount());
                RecordSink recordSink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter, false);
                try (RecordChain chain = new RecordChain(reader.getMetadata(), recordSink, SIZE_4M)) {
                    LongList rows = new LongList();
                    Record chainRecord = chain.getRecord();
                    RecordCursor cursor = reader.getCursor();
                    Record cursorRecord = cursor.getRecord();

                    chain.setSymbolTableResolver(cursor);

                    long o = -1L;
                    while (cursor.hasNext()) {
                        o = chain.put(cursorRecord, o);
                        rows.add(o);
                    }

                    Assert.assertEquals(N, rows.size());
                    cursor.toTop();

                    for (int i = 0, n = rows.size(); i < n; i++) {
                        long row = rows.getQuick(i);
                        Assert.assertTrue(cursor.hasNext());
                        chain.recordAt(row);
                        Assert.assertEquals(row, chainRecord.getRowId());
                        assertSame(cursorRecord, chainRecord, reader.getMetadata());
                    }

                    Record rec2 = chain.newRecord();
                    cursor.toTop();

                    for (int i = 0, n = rows.size(); i < n; i++) {
                        long row = rows.getQuick(i);
                        Assert.assertTrue(cursor.hasNext());
                        chain.recordAt(rec2, row);
                        Assert.assertEquals(row, rec2.getRowId());
                        assertSame(cursorRecord, rec2, reader.getMetadata());
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
    public void testWriteAndRead() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    final int N = 10000 * 2;
                    CairoTestUtils.createTestTable(N, new Rnd(), new TestRecord.ArrayBinarySequence());
                    try (TableReader reader = new TableReader(configuration, "x")) {
                        entityColumnFilter.of(reader.getMetadata().getColumnCount());
                        RecordSink recordSink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter, false);

                        try (RecordChain chain = new RecordChain(reader.getMetadata(), recordSink, 4 * 1024 * 1024L)) {
                            populateChain(chain, reader);
                            assertChain(chain, N, reader);
                            assertChain(chain, N, reader);
                        }
                    }
                }
        );
    }

    private static void populateChain(RecordChain chain, TableReader reader) {
        RecordCursor cursor = reader.getCursor();
        final Record record = cursor.getRecord();
        chain.setSymbolTableResolver(cursor);
        long o = -1L;
        while (cursor.hasNext()) {
            o = chain.put(record, o);
        }
    }

    private void assertChain(RecordChain chain, long expectedCount, TableReader reader) {
        long count = 0L;
        chain.toTop();
        Record chainRecord = chain.getRecord();
        RecordCursor cursor = reader.getCursor();
        Record readerRecord = cursor.getRecord();
        chain.setSymbolTableResolver(cursor);

        while (chain.hasNext()) {
            Assert.assertTrue(cursor.hasNext());
            assertSame(readerRecord, chainRecord, reader.getMetadata());
            count++;
        }
        Assert.assertEquals(expectedCount, count);
    }

    private void assertSame(Record expected, Record actual, RecordMetadata metadata) {
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
            final int N = 10000;
            Rnd rnd = new Rnd();

            // in a spirit of using only whats available in this package
            // we create temporary table the hard way

            CairoTestUtils.createTestTable(N, rnd, new TestRecord.ArrayBinarySequence());
            try (TableReader reader = new TableReader(configuration, "x")) {

                entityColumnFilter.of(reader.getMetadata().getColumnCount());
                RecordSink recordSink = RecordSinkFactory.getInstance(asm, reader.getMetadata(), entityColumnFilter, false);
                try (RecordChain chain = new RecordChain(reader.getMetadata(), recordSink, 4 * 1024 * 1024L)) {

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