/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;


import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class AbstractGriffinTest extends AbstractCairoTest {
    private static final LongList rows = new LongList();
    private final static double EPSILON = 0.000001;
    protected static BindVariableService bindVariableService;
    protected static SqlExecutionContext sqlExecutionContext;
    protected static SqlCompiler compiler;
    protected static SqlExecutionCircuitBreaker circuitBreaker;

    protected final SCSequence eventSubSequence = new SCSequence();

    public static boolean assertCursor(
            CharSequence expected,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean sizeExpected,
            boolean sizeCanBeVariable,
            RecordCursor cursor,
            RecordMetadata metadata,
            boolean framingSupported
    ) {
        return assertCursor(
                expected,
                supportsRandomAccess,
                checkSameStr,
                sizeExpected,
                sizeCanBeVariable,
                cursor,
                metadata,
                sink,
                printer,
                rows,
                framingSupported
        );
    }

    // Thread-safe cursor assertion method.
    public static boolean assertCursor(
            CharSequence expected,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean sizeExpected,
            boolean sizeCanBeVariable,
            RecordCursor cursor,
            RecordMetadata metadata,
            StringSink sink,
            RecordCursorPrinter printer,
            LongList rows,
            boolean fragmentedSymbolTables
    ) {
        if (expected == null) {
            Assert.assertFalse(cursor.hasNext());
            cursor.toTop();
            Assert.assertFalse(cursor.hasNext());
            return true;
        }

        TestUtils.assertCursor(expected, cursor, metadata, true, sink);

        testSymbolAPI(metadata, cursor, fragmentedSymbolTables);
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
            if (sizeExpected) {
                Assert.assertTrue("Concrete cursor size expected but was -1", cursorSize != -1);
            } else {
                Assert.assertTrue("Invalid/undetermined cursor size expected but was " + cursorSize, cursorSize <= 0);
            }
        }
        if (cursorSize != -1) {
            Assert.assertEquals("Actual cursor records vs cursor.size()", count, cursorSize);
        }

        TestUtils.assertEquals(expected, sink);

        if (supportsRandomAccess) {
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
        return false;
    }

    public static void assertReader(String expected, CharSequence tableName) {
        try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            TestUtils.assertReader(expected, reader, sink);
        }
    }

    public static void assertVariableColumns(RecordCursorFactory factory, boolean checkSameStr, SqlExecutionContext executionContext) {
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            RecordMetadata metadata = factory.getMetadata();
            final int columnCount = metadata.getColumnCount();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                        case ColumnType.STRING:
                            CharSequence a = record.getStr(i);
                            CharSequence b = record.getStrB(i);
                            if (a == null) {
                                Assert.assertNull(b);
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                            } else {
                                if (a instanceof AbstractCharSequence) {
                                    // AbstractCharSequence are usually mutable. We cannot have same mutable instance for A and B
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
        } catch (SqlException e) {
            e.printStackTrace();
        }
        assertFactoryMemoryUsage();
    }

    public static boolean doubleEquals(double a, double b, double epsilon) {
        return a == b || Math.abs(a - b) < epsilon;
    }

    public static boolean doubleEquals(double a, double b) {
        return doubleEquals(a, b, EPSILON);
    }

    public static void executeInsert(String insertSql) throws SqlException {
        TestUtils.insert(compiler, sqlExecutionContext, insertSql);
    }

    @BeforeClass
    public static void setUpStatic() {
        AbstractCairoTest.setUpStatic();
        compiler = new SqlCompiler(engine, null, snapshotAgent);
        bindVariableService = new BindVariableServiceImpl(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        circuitBreaker);
        bindVariableService.clear();
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractCairoTest.tearDownStatic();
        compiler.close();
        circuitBreaker = null;
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        bindVariableService.clear();
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
                compile(ddl, sqlExecutionContext);
            }
            snapshotMemoryUsage();
            CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
            RecordCursorFactory factory = cc.getRecordCursorFactory();
            try {
                assertTimestamp(expectedTimestamp, factory);
                assertCursorRawRecords(expected, factory, checkSameStr, expectSize);
                // make sure we get the same outcome when we get factory to create new cursor
                assertCursorRawRecords(expected, factory, checkSameStr, expectSize);
                // make sure strings, binary fields and symbols are compliant with expected record behaviour
                assertVariableColumns(factory, checkSameStr, sqlExecutionContext);

                if (ddl2 != null) {
                    compile(ddl2, sqlExecutionContext);

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

            testSymbolAPI(metadata, cursor, factory.fragmentedSymbolTables());
            cursor.toTop();
            testStringsLong256AndBinary(metadata, cursor, checkSameStr);

            cursor.toTop();
            final Record record = cursor.getRecord();
            Assert.assertNotNull(record);
            int expectedRow = 0;
            while (cursor.hasNext()) {
                for (int col = 0, n = metadata.getColumnCount(); col < n; col++) {
                    switch (ColumnType.tagOf(metadata.getColumnType(col))) {
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
        } catch (SqlException e) {
            e.printStackTrace();
        }
        assertFactoryMemoryUsage();
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize
    ) throws SqlException {
        assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, false, sqlExecutionContext);
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws SqlException {
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
    ) throws SqlException {
        boolean cursorAsserted;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals("supports random access", supportsRandomAccess, factory.recordCursorSupportsRandomAccess());
            cursorAsserted = assertCursor(
                    expected,
                    supportsRandomAccess,
                    checkSameStr,
                    sizeExpected,
                    sizeCanBeVariable,
                    cursor,
                    factory.getMetadata(),
                    factory.fragmentedSymbolTables()
            );
        }

        assertFactoryMemoryUsage();

        if (cursorAsserted) {
            return;
        }

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            testSymbolAPI(factory.getMetadata(), cursor, factory.fragmentedSymbolTables());
        }

        assertFactoryMemoryUsage();
    }

    private static void testStringsLong256AndBinary(RecordMetadata metadata, RecordCursor cursor, boolean checkSameStr) {
        Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                    case ColumnType.STRING:
                        CharSequence s = record.getStr(i);
                        if (s != null) {
                            CharSequence b = record.getStrB(i);
                            if (b instanceof AbstractCharSequence) {
                                // AbstractCharSequence are usually mutable. We cannot have same mutable instance for A and B
                                Assert.assertNotSame("Expected string instances be different for getStr and getStrB", s, b);
                            }
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

    private static void testSymbolAPI(RecordMetadata metadata, RecordCursor cursor, boolean fragmentedSymbolTables) {
        IntList symbolIndexes = null;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                if (symbolIndexes == null) {
                    symbolIndexes = new IntList();
                }
                symbolIndexes.add(i);
            }
        }

        if (symbolIndexes != null) {

            // create new symbol tables and make sure they are not the same
            // as the default ones

            ObjList<SymbolTable> clonedSymbolTables = new ObjList<>();
            ObjList<SymbolTable> originalSymbolTables = new ObjList<>();
            int[] symbolTableKeySnapshot = new int[symbolIndexes.size()];
            String[][] symbolTableValueSnapshot = new String[symbolIndexes.size()][];
            try {
                cursor.toTop();
                if (!fragmentedSymbolTables && cursor.hasNext()) {
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        final int columnIndex = symbolIndexes.getQuick(i);
                        originalSymbolTables.add(cursor.getSymbolTable(columnIndex));
                    }

                    // take snapshot of symbol tables
                    // multiple passes over the same cursor, if not very efficient, we
                    // can swap loops around
                    int sumOfMax = 0;
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        cursor.toTop();
                        final Record rec = cursor.getRecord();
                        final int column = symbolIndexes.getQuick(i);
                        int max = -1;
                        while (cursor.hasNext()) {
                            max = Math.max(max, rec.getInt(column));
                        }
                        String[] values = new String[max + 2];
                        final SymbolTable symbolTable = cursor.getSymbolTable(column);
                        for (int k = -1; k <= max; k++) {
                            values[k + 1] = Chars.toString(symbolTable.valueOf(k));
                        }
                        symbolTableKeySnapshot[i] = max;
                        symbolTableValueSnapshot[i] = values;
                        sumOfMax += max;
                    }

                    // We grab clones after iterating through the symbol values due to
                    // the cache warm up required by Cast*ToSymbolFunctionFactory functions.
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        final int columnIndex = symbolIndexes.getQuick(i);
                        SymbolTable tab = cursor.newSymbolTable(columnIndex);
                        Assert.assertNotNull(tab);
                        clonedSymbolTables.add(tab);
                    }

                    // Now start two threads, one will be using normal symbol table
                    // another will be using a clone. Threads will randomly check that
                    // symbol table is able to convert keys to values without problems

                    int numberOfIterations = sumOfMax * 2;
                    int symbolColumnCount = symbolIndexes.size();
                    int workerCount = 2;
                    CyclicBarrier barrier = new CyclicBarrier(workerCount);
                    SOCountDownLatch doneLatch = new SOCountDownLatch(workerCount);
                    AtomicInteger errorCount = new AtomicInteger(0);

                    // thread that is hitting clones
                    new Thread(() -> {
                        try {
                            TestUtils.await(barrier);
                            assertSymbolColumnThreadSafety(
                                    numberOfIterations,
                                    symbolColumnCount,
                                    clonedSymbolTables,
                                    symbolTableKeySnapshot,
                                    symbolTableValueSnapshot
                            );
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            e.printStackTrace();
                        } finally {
                            doneLatch.countDown();
                        }
                    }).start();

                    // thread that is hitting the original symbol tables
                    new Thread(() -> {
                        try {
                            TestUtils.await(barrier);
                            assertSymbolColumnThreadSafety(
                                    numberOfIterations,
                                    symbolColumnCount,
                                    originalSymbolTables,
                                    symbolTableKeySnapshot,
                                    symbolTableValueSnapshot
                            );
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            e.printStackTrace();
                        } finally {
                            doneLatch.countDown();
                        }
                    }).start();

                    doneLatch.await();

                    Assert.assertEquals(0, errorCount.get());
                }

                cursor.toTop();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        int column = symbolIndexes.getQuick(i);
                        SymbolTable symbolTable = cursor.getSymbolTable(column);
                        if (symbolTable instanceof StaticSymbolTable) {
                            CharSequence sym = Chars.toString(record.getSym(column));
                            int value = record.getInt(column);
                            if (((StaticSymbolTable) symbolTable).containsNullValue() && value == ((StaticSymbolTable) symbolTable).getSymbolCount()) {
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
            } finally {
                Misc.freeObjListIfCloseable(clonedSymbolTables);
            }
        }
    }

    private static void assertSymbolColumnThreadSafety(
            int numberOfIterations,
            int symbolColumnCount,
            ObjList<SymbolTable> symbolTables,
            int[] symbolTableKeySnapshot,
            String[][] symbolTableValueSnapshot
    ) {
        final Rnd rnd = new Rnd(Os.currentTimeMicros(), System.currentTimeMillis());
        for (int i = 0; i < numberOfIterations; i++) {
            int symbolColIndex = rnd.nextInt(symbolColumnCount);
            SymbolTable symbolTable = symbolTables.getQuick(symbolColIndex);
            int max = symbolTableKeySnapshot[symbolColIndex] + 1;
            // max could be -1 meaning we have nulls; max can also be 0, meaning only one symbol value
            // basing boundary on 2 we convert -1 tp 1 and 0 to 2
            int key = rnd.nextInt(max + 1) - 1;
            String expected = symbolTableValueSnapshot[symbolColIndex][key+1];
            TestUtils.assertEquals(expected, symbolTable.valueOf(key));
            // now test static symbol table
            if (expected != null && symbolTable instanceof StaticSymbolTable) {
                StaticSymbolTable staticSymbolTable = (StaticSymbolTable) symbolTable;
                Assert.assertEquals(key, staticSymbolTable.keyOf(expected));
            }
        }
    }

    protected static void assertTimestampColumnValues(RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext, boolean isAscending) throws SqlException {
        int index = factory.getMetadata().getTimestampIndex();
        long timestamp = isAscending ? Long.MIN_VALUE : Long.MAX_VALUE;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            long c = 0;
            while (cursor.hasNext()) {
                long ts = record.getTimestamp(index);
                if ((isAscending && timestamp > ts) ||
                        (!isAscending && timestamp < ts)) {

                    StringSink error = new StringSink();
                    error.put("record # ").put(c).put(" should have ").put(isAscending ? "bigger" : "smaller").put(
                            " (or equal) timestamp than the row before. Values prior=");
                    TimestampFormatUtils.appendDateTimeUSec(error, timestamp);
                    error.put(" current=");
                    TimestampFormatUtils.appendDateTimeUSec(error, ts);

                    Assert.fail(error.toString());
                }
                timestamp = ts;
                c++;
            }
        }
        assertFactoryMemoryUsage();
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
        printSqlResult(() -> expected, query, expectedTimestamp, ddl2, expected2, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable, expectedPlan);
    }

    protected static void printSqlResult(
            Supplier<? extends CharSequence> expectedSupplier,
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
        snapshotMemoryUsage();
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        RecordCursorFactory factory = cc.getRecordCursorFactory();
        if (expectedPlan != null) {
            planSink.reset();
            factory.toPlan(planSink);
            TestUtils.assertEquals(expectedPlan, planSink.getText());
        }
        try {
            assertTimestamp(expectedTimestamp, factory);
            CharSequence expected = expectedSupplier.get();
            assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable);
            // make sure we get the same outcome when we get factory to create new cursor
            assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable);
            // make sure strings, binary fields and symbols are compliant with expected record behaviour
            assertVariableColumns(factory, checkSameStr, sqlExecutionContext);

            if (ddl2 != null) {
                compile(ddl2, sqlExecutionContext);

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
                compile(ddl, sqlExecutionContext);
            }
            printSqlResult(
                    expected,
                    query,
                    expectedTimestamp,
                    ddl2,
                    expected2,
                    supportsRandomAccess,
                    checkSameStr,
                    expectSize,
                    sizeCanBeVariable,
                    null);
        });
    }

    protected static void assertQuery(CharSequence expected,
                                      CharSequence query,
                                      CharSequence ddl,
                                      @Nullable CharSequence expectedTimestamp) throws Exception {
        assertQuery(
                expected,
                query,
                ddl,
                expectedTimestamp,
                null,
                null,
                true,
                true,
                false,
                false
        );
    }

    protected static void assertQuery(CharSequence expected,
                                      CharSequence query,
                                      CharSequence ddl,
                                      @Nullable CharSequence expectedTimestamp,
                                      boolean supportsRandomAccess) throws Exception {
        assertQuery(
                expected,
                query,
                ddl,
                expectedTimestamp,
                null,
                null,
                supportsRandomAccess,
                true,
                false,
                false
        );
    }

    protected static void assertQueryExpectSize(CharSequence expected,
                                                CharSequence query,
                                                CharSequence ddl) throws Exception {
        assertQuery(
                expected,
                query,
                ddl,
                null,
                null,
                null,
                true,
                true,
                true,
                false
        );
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            boolean supportsRandomAccess,
            boolean checkSameStr
    ) throws Exception {
        assertQuery(
                expected,
                query,
                ddl,
                expectedTimestamp,
                null,
                null,
                supportsRandomAccess,
                checkSameStr,
                false,
                false
        );
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize
    ) throws Exception {
        assertQuery(
                expected,
                query,
                ddl,
                expectedTimestamp,
                null,
                null,
                supportsRandomAccess,
                checkSameStr,
                expectSize,
                false
        );
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(CharSequence expected,
                                      CharSequence query,
                                      CharSequence ddl,
                                      @Nullable CharSequence expectedTimestamp,
                                      @Nullable CharSequence ddl2,
                                      @Nullable CharSequence expected2) throws Exception {
        assertQuery(
                expected,
                query,
                ddl,
                expectedTimestamp,
                ddl2,
                expected2,
                true,
                true,
                false,
                false
        );
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(CharSequence expected,
                                      CharSequence query,
                                      CharSequence ddl,
                                      @Nullable CharSequence expectedTimestamp,
                                      @Nullable CharSequence ddl2,
                                      @Nullable CharSequence expected2,
                                      boolean supportsRandomAccess) throws Exception {
        assertQuery(
                expected,
                query,
                ddl,
                expectedTimestamp,
                ddl2,
                expected2,
                supportsRandomAccess,
                true,
                false,
                false
        );
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(CharSequence expected,
                                      CharSequence query,
                                      CharSequence ddl,
                                      @Nullable CharSequence expectedTimestamp,
                                      @Nullable CharSequence ddl2,
                                      @Nullable CharSequence expected2,
                                      boolean supportsRandomAccess,
                                      boolean checkSameStr,
                                      boolean expectSize) throws Exception {
        assertQuery(
                expected,
                query,
                ddl,
                expectedTimestamp,
                ddl2,
                expected2,
                supportsRandomAccess,
                checkSameStr,
                expectSize,
                false
        );
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(CharSequence expected,
                                      CharSequence query,
                                      CharSequence ddl,
                                      @Nullable CharSequence expectedTimestamp,
                                      @Nullable CharSequence ddl2,
                                      @Nullable CharSequence expected2,
                                      boolean supportsRandomAccess,
                                      boolean checkSameStr,
                                      boolean expectSize,
                                      boolean sizeCanBeVariable) throws Exception {
        assertQueryNoVerify(
                expected,
                query,
                ddl,
                expectedTimestamp,
                ddl2,
                expected2,
                supportsRandomAccess,
                checkSameStr,
                expectSize,
                sizeCanBeVariable);
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertTimestamp(CharSequence expectedTimestamp, RecordCursorFactory factory) throws SqlException {
        assertTimestamp(expectedTimestamp, factory, sqlExecutionContext);
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertTimestamp(CharSequence expectedTimestamp,
                                          RecordCursorFactory factory,
                                          SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (expectedTimestamp == null) {
            Assert.assertEquals("Expected -1 as timestamp index", -1, factory.getMetadata().getTimestampIndex());
        } else {
            boolean expectAscendingOrder = true;
            String tsDesc = expectedTimestamp.toString();
            int position = tsDesc.indexOf("###");
            if (position > 0) {
                expectedTimestamp = tsDesc.substring(0, position);
                expectAscendingOrder = tsDesc.substring(position + 3).equalsIgnoreCase("asc");
            }

            int index = factory.getMetadata().getColumnIndexQuiet(expectedTimestamp);
            Assert.assertTrue("Column " + expectedTimestamp + " can't be found in metadata", index > -1);
            Assert.assertNotEquals("Expected non-negative value as timestamp index", -1, index);
            Assert.assertEquals("Timestamp column index", index, factory.getMetadata().getTimestampIndex());
            assertTimestampColumnValues(factory, sqlExecutionContext, expectAscendingOrder);
        }
    }

    protected static void assertCompile(CharSequence query) throws Exception {
        assertMemoryLeak(() -> compile(query));
    }

    @NotNull
    protected static CompiledQuery compile(CharSequence query) throws SqlException {
        return compile(query, sqlExecutionContext);
    }

    @NotNull
    protected static CompiledQuery compile(CharSequence query, SqlExecutionContext executionContext) throws SqlException {
        CompiledQuery cc = compiler.compile(query, executionContext);
        if (cc.getType() == CompiledQuery.UPDATE) {
            try (UpdateOperation op = cc.getUpdateOperation()) {
                try (OperationFuture future = cc.getDispatcher().execute(op, sqlExecutionContext, null)) {
                    future.await();
                }
            }
        } else {
            try (OperationFuture future = cc.execute(null)) {
                future.await();
            }
        }
        return cc;
    }

    void assertFactoryCursor(
            String expected,
            String expectedTimestamp,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            SqlExecutionContext sqlExecutionContext,
            boolean checkSameStr,
            boolean expectSize) throws SqlException {
        assertFactoryCursor(
                expected,
                expectedTimestamp,
                factory,
                supportsRandomAccess,
                sqlExecutionContext,
                checkSameStr,
                expectSize,
                false);
    }

    void assertFactoryCursor(
            String expected,
            String expectedTimestamp,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            SqlExecutionContext executionContext,
            boolean checkSameStr,
            boolean expectSize,
            boolean sizeCanBeVariable) throws SqlException {
        assertTimestamp(expectedTimestamp, factory, executionContext);
        assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable, executionContext);
        // make sure we get the same outcome when we get factory to create new cursor
        assertCursor(expected, factory, supportsRandomAccess, checkSameStr, expectSize, sizeCanBeVariable, executionContext);
        // make sure strings, binary fields and symbols are compliant with expected record behaviour
        assertVariableColumns(factory, checkSameStr, executionContext);
    }

    protected void assertFailure(
            CharSequence query,
            @Nullable CharSequence ddl,
            int expectedPosition,
            @NotNull CharSequence expectedMessage) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                if (ddl != null) {
                    compile(ddl, sqlExecutionContext);
                }
                try {
                    compile(query, sqlExecutionContext);
                    Assert.fail("query '" + query + "' should have failed with '" + expectedMessage + "' message!");
                } catch (SqlException | ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
                    Assert.assertEquals(Chars.toString(query), expectedPosition, e.getPosition());
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

    protected void assertQuery(String expected,
                               String query,
                               String expectedTimestamp,
                               boolean supportsRandomAccess) throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, supportsRandomAccess, sqlExecutionContext);
    }

    protected void assertQuery(String expected,
                               String query,
                               String expectedTimestamp,
                               boolean supportsRandomAccess,
                               boolean expectSize) throws SqlException {
        assertQuery(
                compiler,
                expected,
                query,
                expectedTimestamp,
                sqlExecutionContext,
                supportsRandomAccess,
                true,
                expectSize);
    }

    protected void assertQuery(String expected,
                               String query,
                               String expectedTimestamp,
                               boolean supportsRandomAccess,
                               boolean expectSize,
                               boolean sizeCanBeVariable) throws SqlException {
        assertQuery(
                compiler,
                expected,
                query,
                expectedTimestamp,
                sqlExecutionContext,
                supportsRandomAccess,
                true,
                expectSize,
                sizeCanBeVariable);
    }

    protected void assertQuery(String expected,
                               String query,
                               String expectedTimestamp,
                               boolean supportsRandomAccess,
                               SqlExecutionContext sqlExecutionContext) throws SqlException {
        assertQuery(
                compiler,
                expected,
                query,
                expectedTimestamp,
                sqlExecutionContext,
                supportsRandomAccess,
                true,
                false);
    }

    protected void assertQuery(SqlCompiler compiler,
                               String expected,
                               String query,
                               String expectedTimestamp,
                               boolean supportsRandomAccess,
                               SqlExecutionContext sqlExecutionContext) throws SqlException {
        assertQuery(
                compiler,
                expected,
                query,
                expectedTimestamp,
                sqlExecutionContext,
                supportsRandomAccess,
                true,
                false);
    }

    protected void assertQuery(SqlCompiler compiler,
                               String expected,
                               String query,
                               String expectedTimestamp,
                               boolean supportsRandomAccess,
                               SqlExecutionContext sqlExecutionContext,
                               boolean expectSize) throws SqlException {
        assertQuery(
                compiler,
                expected,
                query,
                expectedTimestamp,
                sqlExecutionContext,
                supportsRandomAccess,
                true,
                expectSize);
    }

    protected void assertQuery(String expected,
                               String query,
                               String expectedTimestamp,
                               boolean supportsRandomAccess,
                               SqlExecutionContext sqlExecutionContext,
                               boolean checkSameStr) throws SqlException {
        assertQuery(
                compiler,
                expected,
                query,
                expectedTimestamp,
                sqlExecutionContext,
                supportsRandomAccess,
                checkSameStr,
                false);
    }

    protected void assertQuery(String expected,
                               String query,
                               String expectedTimestamp,
                               boolean supportsRandomAccess,
                               SqlExecutionContext sqlExecutionContext,
                               boolean checkSameStr,
                               boolean expectSize) throws SqlException {
        assertQuery(
                compiler,
                expected,
                query,
                expectedTimestamp,
                sqlExecutionContext,
                supportsRandomAccess,
                checkSameStr,
                expectSize);
    }

    protected void assertQuery(
            SqlCompiler compiler,
            String expected,
            String query,
            String expectedTimestamp,
            SqlExecutionContext sqlExecutionContext,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize
    ) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            assertFactoryCursor(
                    expected,
                    expectedTimestamp,
                    factory,
                    supportsRandomAccess,
                    sqlExecutionContext,
                    checkSameStr,
                    expectSize
            );
        }
    }

    protected void assertQuery(
            SqlCompiler compiler,
            String expected,
            String query,
            String expectedTimestamp,
            SqlExecutionContext sqlExecutionContext,
            boolean supportsRandomAccess,
            boolean checkSameStr,
            boolean expectSize,
            boolean sizeCanBeVariable) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            assertFactoryCursor(
                    expected,
                    expectedTimestamp,
                    factory,
                    supportsRandomAccess,
                    sqlExecutionContext,
                    checkSameStr,
                    expectSize,
                    sizeCanBeVariable);
        }
    }

    protected void assertQueryAndCache(String expected,
                                       String query,
                                       String expectedTimestamp,
                                       boolean expectSize) throws SqlException {
        assertQueryAndCache(expected, query, expectedTimestamp, false, expectSize);
    }

    protected void assertQueryAndCache(String expected,
                                       String query,
                                       String expectedTimestamp,
                                       boolean supportsRandomAccess,
                                       boolean expectSize) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            assertFactoryCursor(
                    expected,
                    expectedTimestamp,
                    factory,
                    supportsRandomAccess,
                    sqlExecutionContext,
                    true,
                    expectSize);
        }
    }

    protected void assertQueryPlain(String expected, String query) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            assertFactoryCursor(
                    expected,
                    null,
                    factory,
                    true,
                    sqlExecutionContext,
                    true,
                    true);
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

    protected void assertSqlRunWithJit(CharSequence query) throws Exception {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        try (RecordCursorFactory factory = cc.getRecordCursorFactory()) {
            Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
        }
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
            int partitionCount
    ) throws NumericException, SqlException {
        TestUtils.createPopulateTable(compiler, sqlExecutionContext, tableModel, totalRows, startDate, partitionCount);
    }

    protected void createPopulateTable(
            int tableId,
            TableModel tableModel,
            int totalRows,
            String startDate,
            int partitionCount
    ) throws NumericException, SqlException {
        try (
                MemoryMARW mem = Vm.getMARWInstance();
                Path path = new Path().of(configuration.getRoot()).concat(tableModel.getTableName())
        ) {
            TableUtils.createTable(configuration, mem, path, tableModel, tableId);
            compiler.compile(
                    TestUtils.insertFromSelectPopulateTableStmt(tableModel, totalRows, startDate, partitionCount),
                    sqlExecutionContext
            );
        }
    }

    protected <T extends AbstractOperation> void executeOperation(
            String query,
            short opType,
            Function<CompiledQuery, T> op
    ) throws SqlException {
        CompiledQuery cq = compiler.compile(query, sqlExecutionContext);
        Assert.assertEquals(opType, cq.getType());
        OperationDispatcher<T> dispatcher = cq.getDispatcher();
        try (
                T operation = op.apply(cq);
                OperationFuture fut = dispatcher.execute(operation, sqlExecutionContext, eventSubSequence)
        ) {
            fut.await();
        }
    }

    protected PlanSink getPlan(CharSequence query) throws SqlException {
        RecordCursorFactory factory = null;
        try {
            planSink.reset();
            factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
            factory.toPlan(planSink);
            return planSink;
        } finally {
            Misc.free(factory);
        }
    }

    protected void assertPlan(CharSequence query, CharSequence expectedPlan) throws SqlException {
        TestUtils.assertEquals(expectedPlan, getPlan(query).getText());
    }
}
