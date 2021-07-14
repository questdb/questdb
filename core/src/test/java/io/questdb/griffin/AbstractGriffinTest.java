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

package io.questdb.griffin;

import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.*;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

public class AbstractGriffinTest extends AbstractCairoTest {
    private static final LongList rows = new LongList();
    private final static double EPSILON = 0.000001;
    protected static BindVariableService bindVariableService;
    protected static SqlExecutionContext sqlExecutionContext;
    protected static SqlCompiler compiler;
    protected static Metrics metrics = Metrics.enabled();

    public static void assertReader(String expected, CharSequence tableName) {
        try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            TestUtils.assertReader(expected, reader, sink);
        }
    }

    public static void assertVariableColumns(RecordCursorFactory factory, boolean checkSameStr) {
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
                                if (checkSameStr) {
                                    Assert.assertNotSame(a, b);
                                }
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

    public static boolean doubleEquals(double a, double b) {
        return doubleEquals(a, b, EPSILON);
    }

    public static boolean doubleEquals(double a, double b, double epsilon) {
        return a == b || Math.abs(a - b) < epsilon;
    }

    public static void executeInsert(String insertSql) throws SqlException {
        TestUtils.insert(compiler, sqlExecutionContext, insertSql);
    }

    @BeforeClass
    public static void setUpStatic() {
        AbstractCairoTest.setUpStatic();
        compiler = new SqlCompiler(engine);
        bindVariableService = new BindVariableServiceImpl(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(
                engine, 1, engine.getMessageBus())
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);
        bindVariableService.clear();
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractCairoTest.tearDownStatic();
        compiler.close();
    }

    protected static void assertQuery(
            Record[] expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            boolean checkSameStr,
            boolean expectSize
    ) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, null, null, checkSameStr, expectSize);
    }

    protected static void assertQuery(
            Record[] expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable Record[] expected2,
            boolean checkSameStr,
            boolean expectSize
    ) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                compiler.compile(ddl, sqlExecutionContext);
            }
            CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
            RecordCursorFactory factory = cc.getRecordCursorFactory();
            try {
                assertTimestamp(expectedTimestamp, factory);
                assertCursorRawRecords(expected, factory, checkSameStr, expectSize);
                // make sure we get the same outcome when we get factory to create new cursor
                assertCursorRawRecords(expected, factory, checkSameStr, expectSize);
                // make sure strings, binary fields and symbols are compliant with expected record behaviour
                assertVariableColumns(factory, checkSameStr);

                if (ddl2 != null) {
                    compiler.compile(ddl2, sqlExecutionContext);

                    int count = 3;
                    while (count > 0) {
                        try {
                            assertCursorRawRecords(expected2, factory, checkSameStr, expectSize);
                            // and again
                            assertCursorRawRecords(expected2, factory, checkSameStr, expectSize);
                            return;
                        } catch (ReaderOutOfDateException e) {
                            Misc.free(factory);
                            factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
                            count--;
                        }
                    }
                }
            } finally {
                Misc.free(factory);
            }
        });
    }

    protected static void assertCursorRawRecords(
            Record[] expected,
            RecordCursorFactory factory,
            boolean checkSameStr,
            boolean expectSize
    ) {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            if (expected == null) {
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());
                return;
            }

            final long rowsCount = cursor.size();
            Assert.assertEquals(rowsCount, expected.length);

            RecordMetadata metadata = factory.getMetadata();

            testSymbolAPI(metadata, cursor);
            cursor.toTop();
            testStringsLong256AndBinary(metadata, cursor, checkSameStr);

            cursor.toTop();
            final Record record = cursor.getRecord();
            Assert.assertNotNull(record);
            int expectedRow = 0;
            while (cursor.hasNext()) {
                for (int col = 0, n = metadata.getColumnCount(); col < n; col++) {
                    switch (metadata.getColumnType(col)) {
                        case ColumnType.BOOLEAN:
                            Assert.assertEquals(expected[expectedRow].getBool(col), record.getBool(col));
                            break;
                        case ColumnType.BYTE:
                            Assert.assertEquals(expected[expectedRow].getByte(col), record.getByte(col));
                            break;
                        case ColumnType.SHORT:
                            Assert.assertEquals(expected[expectedRow].getShort(col), record.getShort(col));
                            break;
                        case ColumnType.CHAR:
                            Assert.assertEquals(expected[expectedRow].getChar(col), record.getChar(col));
                            break;
                        case ColumnType.INT:
                            Assert.assertEquals(expected[expectedRow].getInt(col), record.getInt(col));
                            break;
                        case ColumnType.LONG:
                            Assert.assertEquals(expected[expectedRow].getLong(col), record.getLong(col));
                            break;
                        case ColumnType.DATE:
                            Assert.assertEquals(expected[expectedRow].getDate(col), record.getDate(col));
                            break;
                        case ColumnType.TIMESTAMP:
                            Assert.assertEquals(expected[expectedRow].getTimestamp(col), record.getTimestamp(col));
                            break;
                        case ColumnType.FLOAT:
                            Assert.assertTrue(doubleEquals(expected[expectedRow].getFloat(col), record.getFloat(col)));
                            break;
                        case ColumnType.DOUBLE:
                            Assert.assertTrue(doubleEquals(expected[expectedRow].getDouble(col), record.getDouble(col)));
                            break;
                        case ColumnType.STRING:
                            TestUtils.assertEquals(expected[expectedRow].getStr(col), record.getStr(col));
                            break;
                        case ColumnType.SYMBOL:
                            TestUtils.assertEquals(expected[expectedRow].getSym(col), record.getSym(col));
                            break;
                        case ColumnType.LONG256:
                            Long256 l1 = expected[expectedRow].getLong256A(col);
                            Long256 l2 = record.getLong256A(col);
                            Assert.assertEquals(l1.getLong0(), l2.getLong0());
                            Assert.assertEquals(l1.getLong1(), l2.getLong1());
                            Assert.assertEquals(l1.getLong2(), l2.getLong2());
                            Assert.assertEquals(l1.getLong3(), l2.getLong3());
                            break;
                        case ColumnType.BINARY:
                            TestUtils.assertEquals(expected[expectedRow].getBin(col), record.getBin(col), record.getBin(col).length());
                        default:
                            Assert.fail("Unknown column type");
                            break;
                    }
                }
                expectedRow++;
            }
            Assert.assertTrue((expectSize && rowsCount != -1) || (!expectSize && rowsCount == -1));
            Assert.assertTrue(rowsCount == -1 || expectedRow == rowsCount);
        }
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize
    ) {
        assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, false, sqlExecutionContext);
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) {
        assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable, sqlExecutionContext);
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean sizeExpected,
            boolean sizeCanBeVariable, // this means size() can either be -1 in some cases or known in others
            SqlExecutionContext sqlExecutionContext
    ) {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            if (expected == null) {
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());
                return;
            }

            TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);

            final RecordMetadata metadata = factory.getMetadata();

            testSymbolAPI(metadata, cursor);
            cursor.toTop();
            testStringsLong256AndBinary(metadata, cursor, checkSameStr);

            // test API where same record is being updated by cursor
            cursor.toTop();
            Record record = cursor.getRecord();
            Assert.assertNotNull(record);
            sink.clear();
            printer.printHeader(metadata, sink);
            long count = 0;
            long cursorSize = cursor.size();
            while (cursor.hasNext()) {
                printer.print(record, metadata, sink);
                count++;
            }

            if (!sizeCanBeVariable) {
                Assert.assertTrue((sizeExpected && cursorSize != -1) || (!sizeExpected && cursorSize <= 0));
            }
            Assert.assertTrue(cursorSize == -1 || count == cursorSize);

            TestUtils.assertEquals(expected, sink);

            if (supportsRandomAccess) {

                Assert.assertTrue(factory.recordCursorSupportsRandomAccess());

                cursor.toTop();

                sink.clear();
                rows.clear();
                while (cursor.hasNext()) {
                    rows.add(record.getRowId());
                }

                final Record rec = cursor.getRecordB();
                printer.printHeader(metadata, sink);
                for (int i = 0, n = rows.size(); i < n; i++) {
                    cursor.recordAt(rec, rows.getQuick(i));
                    printer.print(rec, metadata, sink);
                }

                TestUtils.assertEquals(expected, sink);

                sink.clear();

                final Record factRec = cursor.getRecordB();
                printer.printHeader(metadata, sink);
                for (int i = 0, n = rows.size(); i < n; i++) {
                    cursor.recordAt(factRec, rows.getQuick(i));
                    printer.print(factRec, metadata, sink);
                }

                TestUtils.assertEquals(expected, sink);

                // test that absolute positioning of record does not affect state of record cursor
                if (rows.size() > 0) {
                    sink.clear();

                    cursor.toTop();
                    int target = rows.size() / 2;
                    printer.printHeader(metadata, sink);
                    while (target-- > 0 && cursor.hasNext()) {
                        printer.print(record, metadata, sink);
                    }

                    // no obliterate record with absolute positioning
                    for (int i = 0, n = rows.size(); i < n; i++) {
                        cursor.recordAt(factRec, rows.getQuick(i));
                    }

                    // not continue normal fetch
                    while (cursor.hasNext()) {
                        printer.print(record, metadata, sink);
                    }

                    TestUtils.assertEquals(expected, sink);
                }
            } else {
                Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
                try {
                    record.getRowId();
                    Assert.fail();
                } catch (UnsupportedOperationException ignore) {
                }

                try {
                    cursor.getRecordB();
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

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            testSymbolAPI(factory.getMetadata(), cursor);
        }
    }

    private static void testStringsLong256AndBinary(RecordMetadata metadata, RecordCursor cursor, boolean checkSameStr) {
        Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                switch (metadata.getColumnType(i)) {
                    case ColumnType.STRING:
                        CharSequence s = record.getStr(i);
                        if (s != null) {
                            if (checkSameStr) {
                                Assert.assertNotSame("Expected string instances be different for getStr and getStrB", s, record.getStrB(i));
                            }
                            TestUtils.assertEquals(s, record.getStrB(i));
                            Assert.assertEquals(s.length(), record.getStrLen(i));
                        } else {
                            Assert.assertNull(record.getStrB(i));
                            Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                        }
                        break;
                    case ColumnType.BINARY:
                        BinarySequence bs = record.getBin(i);
                        if (bs != null) {
                            Assert.assertEquals(record.getBin(i).length(), record.getBinLen(i));
                        } else {
                            Assert.assertEquals(TableUtils.NULL_LEN, record.getBinLen(i));
                        }
                        break;
                    case ColumnType.LONG256:
                        Long256 l1 = record.getLong256A(i);
                        Long256 l2 = record.getLong256B(i);
                        if (l1 == Long256Impl.NULL_LONG256) {
                            Assert.assertSame(l1, l2);
                        } else {
                            Assert.assertNotSame(l1, l2);
                        }
                        Assert.assertEquals(l1.getLong0(), l2.getLong0());
                        Assert.assertEquals(l1.getLong1(), l2.getLong1());
                        Assert.assertEquals(l1.getLong2(), l2.getLong2());
                        Assert.assertEquals(l1.getLong3(), l2.getLong3());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private static void testSymbolAPI(RecordMetadata metadata, RecordCursor cursor) {
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
                    if (symbolTable instanceof StaticSymbolTable) {
                        CharSequence sym = Chars.toString(record.getSym(column));
                        int value = record.getInt(column);
                        if (((StaticSymbolTable) symbolTable).containsNullValue() && value == ((StaticSymbolTable) symbolTable).size()) {
                            Assert.assertEquals(Integer.MIN_VALUE, ((StaticSymbolTable) symbolTable).keyOf(sym));
                        } else {
                            Assert.assertEquals(value, ((StaticSymbolTable) symbolTable).keyOf(sym));
                        }
                        TestUtils.assertEquals(sym, symbolTable.valueOf(value));
                    } else {
                        final int value = record.getInt(column);
                        TestUtils.assertEquals(record.getSym(column), symbolTable.valueOf(value));
                    }
                }
            }
        }
    }

    protected static void assertTimestampColumnValues(RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext) {
        int index = factory.getMetadata().getTimestampIndex();
        long timestamp = Long.MIN_VALUE;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            long c = 0;
            while (cursor.hasNext()) {
                long ts = record.getTimestamp(index);
                if (timestamp > ts) {
                    Assert.fail("record #" + c);
                }
                timestamp = ts;
                c++;
            }
        }
    }

    protected static void printSqlResult(
            CharSequence expected,
            CharSequence query,
            CharSequence expectedTimestamp,
            boolean supportsRandomAccess,
            boolean expectSize
    ) throws SqlException {
        printSqlResult(
                expected,
                query,
                expectedTimestamp,
                null,
                null,
                supportsRandomAccess,
                true,
                expectSize,
                false,
                null
        );
    }

    protected static void printSqlResult(
            CharSequence expected,
            CharSequence query,
            CharSequence expectedTimestamp,
            CharSequence ddl2,
            CharSequence expected2,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize,
            boolean sizeCanBeVariable,
            CharSequence expectedPlan
    ) throws SqlException {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        RecordCursorFactory factory = cc.getRecordCursorFactory();
        if (expectedPlan != null) {
            sink.clear();
            factory.toSink(sink);
            TestUtils.assertEquals(expectedPlan, sink);
        }
        try {
            assertTimestamp(expectedTimestamp, factory);
            assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable);
            // make sure we get the same outcome when we get factory to create new cursor
            assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable);
            // make sure strings, binary fields and symbols are compliant with expected record behaviour
            assertVariableColumns(factory, checkSameStr);

            if (ddl2 != null) {
                compiler.compile(ddl2, sqlExecutionContext);

                int count = 3;
                while (count > 0) {
                    try {
                        assertCursor(expected2, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable);
                        // and again
                        assertCursor(expected2, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable);
                        return;
                    } catch (ReaderOutOfDateException e) {
                        Misc.free(factory);
                        factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
                        count--;
                    }
                }
            }
        } finally {
            Misc.free(factory);
        }
    }

    private static void assertQueryNoVerify(
            CharSequence expected,
            CharSequence query,
            @Nullable CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                compiler.compile(ddl, sqlExecutionContext);
            }
            printSqlResult(expected, query, expectedTimestamp, ddl2, expected2, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable, null);
        });
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp
    ) throws Exception {
        assertQueryNoVerify(expected, query, ddl, expectedTimestamp, null, null, true, true, false, false);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            boolean supportsRandomAccess
    ) throws Exception {
        assertQueryNoVerify(expected, query, ddl, expectedTimestamp, null, null, supportsRandomAccess, true, false, false);
    }

    protected static void assertQueryExpectSize(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl
    ) throws Exception {
        assertQueryNoVerify(expected, query, ddl, null, null, null, true, true, true, false);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            boolean supportsRandomAccess,
            boolean checkSameStr
    ) throws Exception {
        assertQueryNoVerify(expected, query, ddl, expectedTimestamp, null, null, supportsRandomAccess, checkSameStr, false, false);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize
    ) throws Exception {
        assertQueryNoVerify(expected, query, ddl, expectedTimestamp, null, null, supportsRandomAccess, checkSameStr, expectSize, false);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2
    ) throws Exception {
        assertQueryNoVerify(expected, query, ddl, expectedTimestamp, ddl2, expected2, true, true, false, false);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess
    ) throws Exception {
        assertQueryNoVerify(expected, query, ddl, expectedTimestamp, ddl2, expected2, supportsRandomAccess, true, false, false);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize
    ) throws Exception {
        assertQueryNoVerify(expected, query, ddl, expectedTimestamp, ddl2, expected2, supportsRandomAccess, checkSameStr, expectSize, false);
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws Exception {
        assertQueryNoVerify(expected, query, ddl, expectedTimestamp, ddl2, expected2, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable);
    }

    protected static void assertTimestamp(CharSequence expectedTimestamp, RecordCursorFactory factory) {
        assertTimestamp(expectedTimestamp, factory, sqlExecutionContext);
    }

    protected static void assertTimestamp(CharSequence expectedTimestamp, RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext) {
        if (expectedTimestamp == null) {
            Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
        } else {
            int index = factory.getMetadata().getColumnIndex(expectedTimestamp);
            Assert.assertNotEquals(-1, index);
            Assert.assertEquals(index, factory.getMetadata().getTimestampIndex());
            assertTimestampColumnValues(factory, sqlExecutionContext);
        }
    }

    void assertFactoryCursor(
            String expected,
            String expectedTimestamp,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            SqlExecutionContext sqlExecutionContext,
            boolean checkSameStr,
            boolean expectSize
    ) {
        assertFactoryCursor(expected, expectedTimestamp, factory, supportsRandomAccess, sqlExecutionContext, checkSameStr, expectSize, false);
    }

    void assertFactoryCursor(
            String expected,
            String expectedTimestamp,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            SqlExecutionContext sqlExecutionContext,
            boolean checkSameStr,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) {
        assertTimestamp(expectedTimestamp, factory, sqlExecutionContext);
        assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable, sqlExecutionContext);
        // make sure we get the same outcome when we get factory to create new cursor
        assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable, sqlExecutionContext);
        // make sure strings, binary fields and symbols are compliant with expected record behaviour
        assertVariableColumns(factory, checkSameStr);
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
                    Assert.assertEquals(Chars.toString(query), expectedPosition, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
                }
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.clear();
            }
        });
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp) throws SqlException {
        assertQuery(expected, query, expectedTimestamp, false);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess) throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, supportsRandomAccess, sqlExecutionContext);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, true, expectSize);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize, boolean sizeCanBeVariable) throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, true, expectSize, sizeCanBeVariable);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext)
            throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, true, false);
    }

    protected void assertQuery(SqlCompiler compiler, String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext)
            throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, true, false);
    }

    protected void assertQuery(
            SqlCompiler compiler,
            String expected,
            String query,
            String expectedTimestamp,
            boolean supportsRandomAccess,
            SqlExecutionContext sqlExecutionContext,
            boolean expectSize
    ) throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, true, expectSize);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext, boolean checkSameStr)
            throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, checkSameStr, false);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext, boolean checkSameStr, boolean expectSize)
            throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, checkSameStr, expectSize);
    }

    protected void assertQuery(
            SqlCompiler compiler,
            String expected,
            String query,
            String expectedTimestamp,
            SqlExecutionContext sqlExecutionContext, boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize
    ) throws SqlException {
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            assertFactoryCursor(expected, expectedTimestamp, factory, supportsRandomAccess, sqlExecutionContext, checkSameStr, expectSize);
        }
    }

    protected void assertQuery(
            SqlCompiler compiler,
            String expected,
            String query,
            String expectedTimestamp,
            SqlExecutionContext sqlExecutionContext, boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws SqlException {
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            assertFactoryCursor(expected, expectedTimestamp, factory, supportsRandomAccess, sqlExecutionContext, checkSameStr, expectSize, sizeCanBeVariable);
        }
    }

    protected void assertQueryAndCache(String expected, String query, String expectedTimestamp, boolean expectSize) throws SqlException {
        assertQueryAndCache(expected, query, expectedTimestamp, false, expectSize);
    }

    protected void assertQueryAndCache(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            assertFactoryCursor(expected, expectedTimestamp, factory, supportsRandomAccess, sqlExecutionContext, true, expectSize);
        }
    }

    protected void assertQueryPlain(
            String expected,
            String query
    ) throws SqlException {
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            assertFactoryCursor(expected, null, factory, true, sqlExecutionContext, true, true);
        }
    }

    protected void assertSql(CharSequence sql, CharSequence expected) throws SqlException {
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink,
                expected
        );
    }

    protected void assertSqlWithTypes(CharSequence sql, CharSequence expected) throws SqlException {
        TestUtils.assertSqlWithTypes(
                compiler,
                sqlExecutionContext,
                sql,
                sink,
                expected
        );
    }

    protected void createPopulateTable(
            TableModel tableModel,
            int totalRows,
            String startDate,
            int partitionCount) throws NumericException, SqlException {
        TestUtils.createPopulateTable(compiler, sqlExecutionContext, tableModel, totalRows, startDate, partitionCount);
    }
}
