/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin;

import com.questdb.cairo.AbstractCairoTest;
import com.questdb.cairo.CairoEngine;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.BinarySequence;
import com.questdb.std.IntList;
import com.questdb.std.LongList;
import com.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;

public class AbstractGriffinTest extends AbstractCairoTest {
    protected static final BindVariableService bindVariableService = new BindVariableService();
    protected static final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl().with(AllowAllCairoSecurityContext.INSTANCE, bindVariableService);
    private static final LongList rows = new LongList();
    protected static CairoEngine engine;
    protected static SqlCompiler compiler;

    public static void assertVariableColumns(RecordCursorFactory factory) {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            RecordMetadata metadata = factory.getMetadata();
            final int columnCount = metadata.getColumnCount();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    switch (metadata.getColumnType(i)) {
                        case ColumnType.STRING:
                            CharSequence a = record.getStr(i);
                            CharSequence b = record.getStrB(i);
                            if (a == null) {
                                Assert.assertNull(b);
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                            } else {
                                Assert.assertNotSame(a, b);
                                TestUtils.assertEquals(a, b);
                                Assert.assertEquals(a.length(), record.getStrLen(i));
                            }
                            break;
                        case ColumnType.BINARY:
                            BinarySequence s = record.getBin(i);
                            if (s == null) {
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getBinLen(i));
                            } else {
                                Assert.assertEquals(s.length(), record.getBinLen(i));
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    @BeforeClass
    public static void setUp2() {
        engine = new CairoEngine(configuration);
        compiler = new SqlCompiler(engine);
        bindVariableService.clear();
    }

    @AfterClass
    public static void tearDown() {
        engine.close();
        compiler.close();
    }

    protected static void assertCursor(CharSequence expected, RecordCursorFactory factory, boolean supportsRandomAccess) throws IOException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            if (expected == null) {
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());
                return;
            }

            sink.clear();
            rows.clear();
            printer.print(cursor, factory.getMetadata(), true);

            TestUtils.assertEquals(expected, sink);

            final RecordMetadata metadata = factory.getMetadata();

            testSymbolAPI(metadata, cursor);

            // test API where same record is being updated by cursor
            cursor.toTop();
            Record record = cursor.getRecord();
            Assert.assertNotNull(record);
            sink.clear();
            printer.printHeader(metadata);
            while (cursor.hasNext()) {
                printer.print(record, metadata);
            }
            TestUtils.assertEquals(expected, sink);

            if (supportsRandomAccess) {

                Assert.assertTrue(factory.isRandomAccessCursor());

                cursor.toTop();

                sink.clear();
                while (cursor.hasNext()) {
                    rows.add(record.getRowId());
                }

                printer.printHeader(metadata);
                for (int i = 0, n = rows.size(); i < n; i++) {
                    cursor.recordAt(record, rows.getQuick(i));
                    printer.print(record, metadata);
                }

                TestUtils.assertEquals(expected, sink);

                // test internal record
                sink.clear();
                printer.printHeader(metadata);
                for (int i = 0, n = rows.size(); i < n; i++) {
                    cursor.recordAt(rows.getQuick(i));
                    printer.print(record, metadata);
                }

                TestUtils.assertEquals(expected, sink);

                // test _new_ record
                sink.clear();
                record = cursor.newRecord();
                printer.printHeader(metadata);
                for (int i = 0, n = rows.size(); i < n; i++) {
                    cursor.recordAt(record, rows.getQuick(i));
                    printer.print(record, metadata);
                }

                TestUtils.assertEquals(expected, sink);
            } else {
                Assert.assertFalse(factory.isRandomAccessCursor());
                try {
                    record.getRowId();
                    Assert.fail();
                } catch (UnsupportedOperationException ignore) {
                }

                try {
                    cursor.newRecord();
                    Assert.fail();
                } catch (UnsupportedOperationException ignore) {
                }

                try {
                    cursor.recordAt(0);
                    Assert.fail();
                } catch (UnsupportedOperationException ignore) {
                }

                try {
                    cursor.recordAt(record, 0);
                    Assert.fail();
                } catch (UnsupportedOperationException ignore) {
                }


            }
        }
    }

    protected static void testSymbolAPI(RecordMetadata metadata, RecordCursor cursor) {
        IntList symbolIndexes = null;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getColumnType(i) == ColumnType.SYMBOL) {
                if (symbolIndexes == null) {
                    symbolIndexes = new IntList();
                }
                symbolIndexes.add(i);
            }
        }

        if (symbolIndexes != null) {
            cursor.toTop();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                    int column = symbolIndexes.getQuick(i);
                    SymbolTable symbolTable = cursor.getSymbolTable(column);
                    CharSequence sym = record.getSym(column);
                    int value = record.getInt(column);
                    Assert.assertEquals(value, symbolTable.getQuick(sym));
                    TestUtils.assertEquals(sym, symbolTable.value(value));
                }
            }
        }
    }

    protected static void assertTimestampColumnValues(RecordCursorFactory factory) {
        int index = factory.getMetadata().getTimestampIndex();
        long timestamp = Long.MIN_VALUE;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                long ts = record.getTimestamp(index);
                Assert.assertTrue(timestamp <= ts);
                timestamp = ts;
            }
        }
    }

    protected static void printSqlResult(
            CharSequence expected,
            CharSequence query,
            CharSequence expectedTimestamp,
            CharSequence ddl2,
            CharSequence expected2,
            boolean supportsRandomAccess
    ) throws IOException, SqlException {
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext)) {
            assertTimestamp(expectedTimestamp, factory);
            assertCursor(expected, factory, supportsRandomAccess);
            // make sure we get the same outcome when we get factory to create new cursor
            assertCursor(expected, factory, supportsRandomAccess);
            // make sure strings, binary fields and symbols are compliant with expected record behaviour
            assertVariableColumns(factory);

            if (ddl2 != null) {
                compiler.compile(ddl2, sqlExecutionContext);
                assertCursor(expected2, factory, supportsRandomAccess);
                // and again
                assertCursor(expected2, factory, supportsRandomAccess);
            }
        }
    }

    private static void assertQuery(
            CharSequence expected,
            CharSequence query,
            @Nullable CharSequence ddl,
            @Nullable CharSequence verify,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                if (ddl != null) {
                    compiler.compile(ddl, sqlExecutionContext);
                }
                if (verify != null) {
                    printSqlResult(null, verify, expectedTimestamp, ddl2, expected2, supportsRandomAccess);
                }
                printSqlResult(expected, query, expectedTimestamp, ddl2, expected2, supportsRandomAccess);
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp) throws Exception {
        assertQuery(expected, query, ddl, null, expectedTimestamp, null, null, true);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            boolean supportsRandomAccess) throws Exception {
        assertQuery(expected, query, ddl, null, expectedTimestamp, null, null, supportsRandomAccess);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2) throws Exception {
        assertQuery(expected, query, ddl, null, expectedTimestamp, ddl2, expected2, true);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess) throws Exception {
        assertQuery(expected, query, ddl, null, expectedTimestamp, ddl2, expected2, supportsRandomAccess);
    }

    protected static void assertTimestamp(CharSequence expectedTimestamp, RecordCursorFactory factory) {
        if (expectedTimestamp == null) {
            Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
        } else {
            int index = factory.getMetadata().getColumnIndex(expectedTimestamp);
            Assert.assertNotEquals(-1, index);
            Assert.assertEquals(index, factory.getMetadata().getTimestampIndex());
            assertTimestampColumnValues(factory);
        }
    }

    @After
    public void tearDownAfterTest() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
    }

    void assertFactoryCursor(String expected, String expectedTimestamp, RecordCursorFactory factory, boolean supportsRandomAccess) throws IOException {
        assertTimestamp(expectedTimestamp, factory);
        assertCursor(expected, factory, supportsRandomAccess);
        // make sure we get the same outcome when we get factory to create new cursor
        assertCursor(expected, factory, supportsRandomAccess);
        // make sure strings, binary fields and symbols are compliant with expected record behaviour
        assertVariableColumns(factory);
    }

    protected void assertFailure(
            CharSequence query,
            @Nullable CharSequence ddl,
            int expectedPosition,
            @NotNull CharSequence expectedMessage
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                if (ddl != null) {
                    compiler.compile(ddl, sqlExecutionContext);
                }
                try {
                    compiler.compile(query, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(expectedPosition, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
                }
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp) throws IOException, SqlException {
        assertQuery(expected, query, expectedTimestamp, false);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess) throws IOException, SqlException {
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext)) {
            assertFactoryCursor(expected, expectedTimestamp, factory, supportsRandomAccess);
        }
    }

    protected void assertQueryAndCache(String expected, String query, String expectedTimestamp) throws IOException, SqlException {
        assertQueryAndCache(expected, query, expectedTimestamp, false);
    }

    protected void assertQueryAndCache(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess) throws IOException, SqlException {
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext)) {
            assertFactoryCursor(expected, expectedTimestamp, factory, supportsRandomAccess);
        }
    }
}
