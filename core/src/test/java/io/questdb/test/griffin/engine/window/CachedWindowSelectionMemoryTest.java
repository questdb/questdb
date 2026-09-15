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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CachedWindowSelectionMemoryTest extends AbstractCairoTest {
    private static final long ROW_COUNT = 1_000_000;

    @Test
    public void testBucketSelectionEnumeration() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            for (int method = 0; method < 3; method++) {
                final String name = switch (method) {
                    case 0 -> "lttb";
                    case 1 -> "m4";
                    default -> "minmax";
                };
                final String table = "tab_" + name;
                // Keep ts non-designated so the same factory can also encounter NULL timestamps.
                execute("CREATE TABLE " + table + " AS (SELECT timestamp_sequence(0, 1000) ts, x v FROM long_sequence(5))");
                bindVariableService.setLong(0, 5);
                try (RecordCursorFactory factory = select("SELECT ts, v FROM " + table + " TIMESTAMP(ts) SUBSAMPLE " + name + "(v, $1)");
                     DirectLongList selectedRows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)) {
                    assertFusedPlan(factory, false);
                    assertSelectedRows(factory, selectedRows, true, 0, 1, 2, 3, 4);
                    bindVariableService.setLong(0, 2);
                    assertSelectedRows(factory, selectedRows, false, 0, 4);
                    bindVariableService.setLong(0, 10);
                    assertSelectedRows(factory, selectedRows, true, 0, 1, 2, 3, 4);
                    execute("UPDATE " + table + " SET v = NULL WHERE v = 3");
                    assertSelectedRows(factory, selectedRows, false, 0, 1, 3, 4);
                    execute("UPDATE " + table + " SET ts = NULL WHERE v = 1");
                    assertSelectedRows(factory, selectedRows, false, 1, 3, 4);
                    execute("UPDATE " + table + " SET v = NULL");
                    assertSelectedRows(factory, selectedRows, false);
                }
            }
        });
    }

    @Test
    public void testLttbSelectAllUnderQueryMemoryLimit() throws Exception {
        assertBucketIdentityMemory("lttb");
    }

    @Test
    public void testLttbSelectionModeReuse() throws Exception {
        assertSelectionModeReuse("lttb", 5, 2, true);
    }

    @Test
    public void testM4SelectAllUnderQueryMemoryLimit() throws Exception {
        assertBucketIdentityMemory("m4");
    }

    @Test
    public void testM4SelectionModeReuse() throws Exception {
        assertSelectionModeReuse("m4", 5, 2, true);
    }

    @Test
    public void testMinMaxSelectAllUnderQueryMemoryLimit() throws Exception {
        assertBucketIdentityMemory("minmax");
    }

    @Test
    public void testMinMaxSelectionModeReuse() throws Exception {
        assertSelectionModeReuse("minmax", 5, 2, true);
    }

    @Test
    public void testCadenceSelectAllUnderQueryMemoryLimit() throws Exception {
        assertIdentityMemory("cadence(1)", true);
    }

    @Test
    public void testCadenceWindowFilterUnderQueryMemoryLimit() throws Exception {
        assertIdentityMemory("cadence(1)", false);
    }

    @Test
    public void testUniformSelectAllUnderQueryMemoryLimit() throws Exception {
        assertIdentityMemory("uniform(1_000_000)", true);
    }

    @Test
    public void testUniformWindowFilterUnderQueryMemoryLimit() throws Exception {
        assertIdentityMemory("uniform(1_000_000)", false);
    }

    @Test
    public void testCadenceSelectionModeReuse() throws Exception {
        assertSelectionModeReuse("cadence", 1, 4);
    }

    @Test
    public void testUniformSelectionModeReuse() throws Exception {
        assertSelectionModeReuse("uniform", 5, 2);
    }

    @Test
    public void testCadenceSparseSelectionUnderQueryMemoryLimit() throws Exception {
        assertSelectionMemory("cadence(2)", true, 16_777_216L, 4096L, 2);
    }

    @Test
    public void testIdentitySelectionAcrossPartitions() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            execute("CREATE TABLE tab (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO tab VALUES ('1970-01-01', 1), ('1970-01-03', 4)");
            drainWalQueue();
            execute("INSERT INTO tab VALUES ('1970-01-02', 3), ('1970-01-01T12:00:00', 2)");
            drainWalQueue();
            final String first = "1970-01-01T00:00:00.000000Z\t1\n";
            final String second = "1970-01-01T12:00:00.000000Z\t2\n";
            final String third = "1970-01-02T00:00:00.000000Z\t3\n";
            final String last = "1970-01-03T00:00:00.000000Z\t4\n";
            for (int method = 0; method < 2; method++) {
                for (int ordered = 0; ordered < 2; ordered++) {
                    final boolean isOrdered = ordered == 1;
                    final String source = isOrdered ? "(SELECT ts, v FROM tab ORDER BY ts DESC)" : "tab";
                    try (RecordCursorFactory factory = select("SELECT ts, v FROM " + source + " SUBSAMPLE "
                            + (method == 0 ? "cadence(1)" : "uniform(4)"))) {
                        assertFusedPlan(factory, isOrdered);
                        for (int run = 0; run < 2; run++) {
                            assertSelection(factory, isOrdered, "ts\tv\n" + (isOrdered
                                    ? last + third + second + first : first + second + third + last));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testIdentitySelectionBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            execute("CREATE TABLE tab (ts TIMESTAMP, v LONG) TIMESTAMP(ts)");
            execute("INSERT INTO tab VALUES (0, 1), (1000, 2)");
            for (int rows = 0; rows <= 2; rows++) {
                final String expected = "ts\tv\n" + (rows > 0 ? "1970-01-01T00:00:00.000000Z\t1\n" : "")
                        + (rows > 1 ? "1970-01-01T00:00:00.001000Z\t2\n" : "");
                for (int method = 0; method < 6; method++) {
                    final String selection = switch (method) {
                        case 0 -> "cadence(1)";
                        case 1, 2 -> "uniform(" + (method + 1) + ")";
                        case 3 -> "lttb(v, 2)";
                        case 4 -> "m4(v, 2)";
                        default -> "minmax(v, 2)";
                    };
                    try (RecordCursorFactory factory = select("SELECT ts, v FROM tab WHERE v <= " + rows + " SUBSAMPLE " + selection)) {
                        assertFusedPlan(factory, false);
                        for (int run = 0; run < 2; run++) {
                            MemoryTracker tracker;
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                tracker = sqlExecutionContext.getMemoryTracker();
                                Assert.assertEquals(-1, cursor.size());
                                RecordCursor.Counter counter = new RecordCursor.Counter();
                                counter.add(7);
                                // calculateSize must compute the selection even before the first hasNext.
                                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                                Assert.assertEquals(7 + rows, counter.get());
                                Assert.assertFalse(cursor.hasNext());
                                cursor.toTop();
                                cursor.toTop();
                                counter.clear();
                                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                                Assert.assertEquals(rows, counter.get());
                            }
                            Assert.assertEquals(0, tracker.getUsed());
                            assertSelection(factory, false, expected);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCadenceIdentitySelectionDefaultPageMemoryLimit() throws Exception {
        assertSelectionMemory("cadence(1)", true, 16_777_216L, 0, 1);
    }

    @Test
    public void testUniformIdentitySelectionDefaultPageMemoryLimit() throws Exception {
        assertSelectionMemory("uniform(1_000_000)", true, 16_777_216L, 0, 1);
    }

    @Test
    public void testIdentitySelectionNullsAndTies() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            execute("CREATE TABLE tab (ts TIMESTAMP, v LONG) TIMESTAMP(ts)");
            execute("INSERT INTO tab VALUES (0, 1), (0, NULL), (1000, 3), (1000, 4)");
            execute("CREATE TABLE null_ts (ts TIMESTAMP, v LONG)");
            execute("INSERT INTO null_ts VALUES (NULL, 1), (0, NULL), (0, 3)");
            final String first = "1970-01-01T00:00:00.000000Z\t1\n";
            final String second = "1970-01-01T00:00:00.000000Z\tnull\n";
            final String third = "1970-01-01T00:00:00.001000Z\t3\n";
            final String last = "1970-01-01T00:00:00.001000Z\t4\n";
            for (int method = 0; method < 2; method++) {
                final String selection = method == 0 ? "cadence(1)" : "uniform(4)";
                for (int ordered = 0; ordered < 2; ordered++) {
                    final boolean isOrdered = ordered == 1;
                    final String source = isOrdered ? "(SELECT ts, v FROM tab ORDER BY ts DESC)" : "tab";
                    try (RecordCursorFactory factory = select("SELECT ts, v FROM " + source + " SUBSAMPLE " + selection)) {
                        assertFusedPlan(factory, isOrdered);
                        assertSelection(factory, isOrdered, "ts\tv\n" + (isOrdered
                                ? last + third + second + first : first + second + third + last));
                    }
                }
                assertQuery("SELECT ts, v FROM null_ts TIMESTAMP(ts) SUBSAMPLE " + selection)
                        .timestamp("ts")
                        .withPlanContaining("CachedWindowLightSelect", "unorderedFunctions:")
                        .returns("""
                                ts\tv
                                \t1
                                1970-01-01T00:00:00.000000Z\tnull
                                1970-01-01T00:00:00.000000Z\t3
                                """);
            }
        });
    }

    @Test
    public void testIdentitySelectionVariablePayload() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            execute("CREATE TABLE tab (ts TIMESTAMP, v VARCHAR, s SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO tab VALUES (0, 'alpha', 'a'), (1000, NULL, NULL), (2000, '\u03b2eta', 'b')");
            final String first = "1970-01-01T00:00:00.000000Z\talpha\ta\n";
            final String second = "1970-01-01T00:00:00.001000Z\t\t\n";
            final String last = "1970-01-01T00:00:00.002000Z\t\u03b2eta\tb\n";
            for (int method = 0; method < 2; method++) {
                for (int ordered = 0; ordered < 2; ordered++) {
                    final boolean isOrdered = ordered == 1;
                    final String source = isOrdered ? "(SELECT * FROM tab ORDER BY ts DESC)" : "tab";
                    try (RecordCursorFactory factory = select("SELECT * FROM " + source + " SUBSAMPLE "
                            + (method == 0 ? "cadence(1)" : "uniform(3)"))) {
                        assertFusedPlan(factory, isOrdered);
                        Assert.assertEquals(3, factory.getMetadata().getColumnCount());
                        Assert.assertEquals(ColumnType.TIMESTAMP, factory.getMetadata().getColumnType(0));
                        Assert.assertEquals(ColumnType.VARCHAR, factory.getMetadata().getColumnType(1));
                        Assert.assertEquals(ColumnType.SYMBOL, factory.getMetadata().getColumnType(2));
                        for (int run = 0; run < 2; run++) {
                            assertSelection(factory, isOrdered, "ts\tv\ts\n" + (isOrdered ? last + second + first : first + second + last));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSelectionComputationFailureThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096L);
            execute("CREATE TABLE tab AS (SELECT timestamp_sequence(0, 1000) ts, x v FROM long_sequence(1_000_000)) TIMESTAMP(ts)");
            bindVariableService.setLong(0, 2);
            try (RecordCursorFactory factory = select("SELECT ts, v FROM tab SUBSAMPLE cadence($1)")) {
                assertFusedPlan(factory, false);
                final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
                AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine) {
                    @Override
                    public void statefulThrowExceptionIfTripped() {
                        // Both sparse lists now exist: trip in the executor's forward validation,
                        // after pass1 and preparePass2, rather than during cursor opening.
                        if (sqlExecutionContext.getMemoryTracker().getUsed() > 16_000_000) {
                            cancel();
                        }
                        super.statefulThrowExceptionIfTripped();
                    }
                };
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                MemoryTracker tracker = null;
                try {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        tracker = sqlExecutionContext.getMemoryTracker();
                        try {
                            cursor.hasNext();
                            Assert.fail("expected cancellation during forward selection validation");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isCancellation());
                            assertFailureMethod(e, "mapSelectedRows");
                        }
                    }
                } finally {
                    breaker.reset();
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                    if (tracker != null) {
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                }
                Assert.assertEquals(0, engine.getBusyReaderCount());
                bindVariableService.setLong(0, 1);
                assertDenseSelection(factory, 1, true);
                bindVariableService.setLong(0, 2);
                assertDenseSelection(factory, 2, true);

                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 9_000_000L);
                // Opening must succeed; the sparse selector exhausts the budget in preparePass2.
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    tracker = sqlExecutionContext.getMemoryTracker();
                    try {
                        cursor.hasNext();
                        Assert.fail("expected query memory limit during computation");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        assertFailureMethod(e, "preparePass2");
                    }
                }
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                bindVariableService.setLong(0, 1);
                assertDenseSelection(factory, 1, true);
                bindVariableService.setLong(0, 2);
                assertDenseSelection(factory, 2, true);
            }
        });
    }

    @Test
    public void testCadenceSelectAllWithoutQueryMemoryLimit() throws Exception {
        assertIdentityMemory("cadence(1)", true, 0);
    }

    @Test
    public void testUniformSelectAllWithoutQueryMemoryLimit() throws Exception {
        assertIdentityMemory("uniform(1_000_000)", true, 0);
    }

    private void assertBucketIdentityMemory(String method) throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096L);
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 33_554_432L);
            execute("CREATE TABLE tab AS (SELECT timestamp_sequence(0, 1000) ts, x v FROM long_sequence(1_000_000)) TIMESTAMP(ts)");
            // Pass1 still owns row IDs, a power-of-two (ts, value) buffer and a NULL bitset.
            // Allow constant list backing, but not even one additional full ordinal list.
            final long maxBytes = ROW_COUNT * Long.BYTES + 16_777_216L + 131_072L + 1024;
            bindVariableService.setLong(0, ROW_COUNT);
            try (RecordCursorFactory factory = select("SELECT ts, v FROM tab SUBSAMPLE " + method + "(v, $1)")) {
                assertFusedPlan(factory, false);
                assertDenseSelection(factory, 1, maxBytes);
                bindVariableService.setLong(0, 2 * ROW_COUNT);
                assertDenseSelection(factory, 1, maxBytes);

                // Fail during pass1 buffer growth, then reopen the same factory in identity mode.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 16_777_216L);
                MemoryTracker tracker;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    tracker = sqlExecutionContext.getMemoryTracker();
                    try {
                        cursor.hasNext();
                        Assert.fail("expected query memory limit during pass1");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        assertFailureMethod(e, "ensureCapacity");
                    }
                }
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 33_554_432L);
                assertDenseSelection(factory, 1, maxBytes);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                assertDenseSelection(factory, 1, maxBytes);
            }
        });
    }

    private static void assertFailureMethod(CairoException exception, String method) {
        for (StackTraceElement frame : exception.getStackTrace()) {
            if (frame.getMethodName().equals(method)) {
                return;
            }
        }
        Assert.fail("failure must reach " + method + ": " + exception);
    }

    private void assertFusedPlan(RecordCursorFactory factory, boolean isOrdered) {
        TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        TestUtils.assertContains(plan.getSink(), "CachedWindowLightSelect");
        TestUtils.assertContains(plan.getSink(), isOrdered ? "orderedFunctions: [[ts]" : "unorderedFunctions:");
    }

    private void assertDenseSelection(RecordCursorFactory factory, int stride, boolean isFused) throws Exception {
        // Positional identity needs only row IDs and constant list backing. Sparse cadence
        // needs its algorithm list and one executor output list, not a traversal copy.
        assertDenseSelection(factory, stride, isFused ? (stride == 1 ? ROW_COUNT * Long.BYTES + 1024 : 16_390_000L) : 0);
    }

    private void assertDenseSelection(RecordCursorFactory factory, int stride, long maxBytes) throws Exception {
        Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
        Assert.assertEquals(2, factory.getMetadata().getColumnCount());
        Assert.assertEquals(0, factory.getMetadata().getTimestampIndex());
        Assert.assertEquals(ColumnType.TIMESTAMP, factory.getMetadata().getColumnType(0));
        Assert.assertEquals(ColumnType.LONG, factory.getMetadata().getColumnType(1));
        final long expectedRows = stride == 1 ? ROW_COUNT : ROW_COUNT / 2 + 1;
        for (int run = 0; run < 2; run++) {
            MemoryTracker tracker = null;
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                tracker = sqlExecutionContext.getMemoryTracker();
                Assert.assertNotNull(tracker);
                Assert.assertEquals(-1, cursor.size());
                // A million-row expected string obscures the native-memory regression. This
                // fixed-width oracle checks every cell twice, both recordAt records, full/remaining
                // calculateSize, metadata and a second open. Small variable payloads use returns().
                for (int pass = 0; pass < 2; pass++) {
                    long rows = 0;
                    while (cursor.hasNext()) {
                        final long ordinal = Math.min(rows * stride, ROW_COUNT - 1);
                        Assert.assertEquals(ordinal * 1000, cursor.getRecord().getTimestamp(0));
                        Assert.assertEquals(ordinal + 1, cursor.getRecord().getLong(1));
                        if (rows == 0 || rows == expectedRows / 2 || rows == expectedRows - 1) {
                            long rowId = cursor.getRecord().getRowId();
                            cursor.recordAt(cursor.getRecordB(), rowId);
                            Assert.assertEquals(ordinal * 1000, cursor.getRecordB().getTimestamp(0));
                            Assert.assertEquals(ordinal + 1, cursor.getRecordB().getLong(1));
                            cursor.recordAt(cursor.getRecord(), rowId);
                            Assert.assertEquals(ordinal + 1, cursor.getRecord().getLong(1));
                        }
                        rows++;
                    }
                    Assert.assertEquals(expectedRows, rows);
                    Assert.assertEquals(-1, cursor.size());
                    cursor.toTop();
                }
                RecordCursor.Counter counter = new RecordCursor.Counter();
                Assert.assertTrue(cursor.hasNext());
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(expectedRows - 1, counter.get());
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                counter.clear();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(expectedRows, counter.get());
                if (maxBytes > 0) {
                    Assert.assertTrue("selection charges " + tracker.getUsed() + " exceed " + maxBytes, tracker.getUsed() <= maxBytes);
                }
            } finally {
                if (tracker != null) {
                    Assert.assertEquals("cursor close must release query charges", 0, tracker.getUsed());
                }
            }
        }
    }

    private void assertSelectedRows(RecordCursorFactory factory, DirectLongList selectedRows, boolean isAllRows, long... expected) throws Exception {
        RecordCursorFactory base = factory;
        while (base != null && !(base instanceof CachedWindowLightRecordCursorFactory)) {
            base = base.getBaseFactory();
        }
        Assert.assertNotNull(base);
        WindowFunction function = ((CachedWindowLightRecordCursorFactory) base).getSingleRowSelectingFunction();
        Assert.assertNotNull(function);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            RecordCursor.Counter counter = new RecordCursor.Counter();
            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
            Assert.assertEquals(expected.length, counter.get());
            Assert.assertEquals(isAllRows, function.isSelectionAllRows());
            // The executor skips enumeration for identity, but callers may still request it.
            // Repeated calls must clear the destination and leave the selection unchanged.
            for (int run = 0; run < 2; run++) {
                selectedRows.add(-1);
                function.getSelectedRows(selectedRows);
                Assert.assertEquals(expected.length, selectedRows.size());
                for (int i = 0; i < expected.length; i++) {
                    Assert.assertEquals(expected[i], selectedRows.get(i));
                }
            }
        }
    }

    private void assertSelection(RecordCursorFactory factory, boolean isOrdered, String expected) throws Exception {
        if (isOrdered) {
            assertFactory(factory).withContext(sqlExecutionContext).timestampDesc("ts").returns(expected);
        } else {
            assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(expected);
        }
    }

    private void assertSelectionModeReuse(String method, long allParameter, long sparseParameter) throws Exception {
        assertSelectionModeReuse(method, allParameter, sparseParameter, false);
    }

    private void assertSelectionModeReuse(String method, long allParameter, long sparseParameter, boolean hasValueArg) throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096L);
            execute("CREATE TABLE tab AS (SELECT timestamp_sequence(0, 1000) ts, x v FROM long_sequence(5)) TIMESTAMP(ts)");
            final String first = "1970-01-01T00:00:00.000000Z\t1\n";
            final String second = "1970-01-01T00:00:00.001000Z\t2\n";
            final String third = "1970-01-01T00:00:00.002000Z\t3\n";
            final String fourth = "1970-01-01T00:00:00.003000Z\t4\n";
            final String last = "1970-01-01T00:00:00.004000Z\t5\n";
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                for (int ordered = 0; ordered < 2; ordered++) {
                    final boolean isOrdered = ordered == 1;
                    final String source = isOrdered ? "(SELECT ts, v FROM tab ORDER BY ts DESC)" : "tab";
                    final String query = "SELECT ts, v FROM " + source + " SUBSAMPLE " + method + "(" + (hasValueArg ? "v, " : "") + "$1)";
                    bindVariableService.setLong(0, allParameter);
                    try (SqlCompiler compiler = engine.getSqlCompiler();
                         RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                        TextPlanSink plan = new TextPlanSink();
                        plan.of(factory, sqlExecutionContext);
                        TestUtils.assertContains(plan.getSink(), light == 1 ? "CachedWindowLightSelect" : "CachedWindow");
                        TestUtils.assertContains(plan.getSink(), isOrdered ? "orderedFunctions: [[ts]" : "unorderedFunctions:");
                        final String allRows = "ts\tv\n" + (isOrdered ? last + fourth + third + second + first : first + second + third + fourth + last);
                        final String sparseRows = "ts\tv\n" + (isOrdered ? last + first : first + last);
                        for (int run = 0; run < 2; run++) {
                            bindVariableService.setLong(0, allParameter);
                            assertSelection(factory, isOrdered, allRows);
                            bindVariableService.setLong(0, sparseParameter);
                            assertSelection(factory, isOrdered, sparseRows);
                        }
                        if (hasValueArg) {
                            bindVariableService.setLong(0, allParameter);
                            execute("UPDATE tab SET v = NULL WHERE ts = 2000::TIMESTAMP");
                            assertSelection(factory, isOrdered, "ts\tv\n" + (isOrdered
                                    ? last + fourth + second + first : first + second + fourth + last));
                            execute("UPDATE tab SET v = 3 WHERE ts = 2000::TIMESTAMP");
                            assertSelection(factory, isOrdered, allRows);
                        }
                        // init() can fail after the cursor reopens its lists. The same factory must
                        // still accept an identity selection after the caller corrects the bind.
                        bindVariableService.setLong(0, 0);
                        try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                            Assert.fail("expected invalid selection parameter");
                        } catch (SqlException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), method.equals("cadence") ? "stride" : "target");
                        }
                        bindVariableService.setLong(0, allParameter);
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            while (cursor.hasNext()) {
                                // The allocation limit must reject the open or computation.
                            }
                            Assert.fail("expected query memory limit");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        }
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                        assertSelection(factory, isOrdered, allRows);
                    }
                }
            }
        });
    }

    private void assertIdentityMemory(String method, boolean isFused) throws Exception {
        assertIdentityMemory(method, isFused, 16_777_216L);
    }

    private void assertIdentityMemory(String method, boolean isFused, long memoryLimit) throws Exception {
        assertSelectionMemory(method, isFused, memoryLimit, 4096L, 1);
    }

    private void assertSelectionMemory(String method, boolean isFused, long memoryLimit, long pageSize, int stride) throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            if (pageSize > 0) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, pageSize);
            }
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, memoryLimit);
            execute("CREATE TABLE tab AS (SELECT timestamp_sequence(0, 1000) ts, x v FROM long_sequence(1_000_000)) TIMESTAMP(ts)");
            final String query = isFused
                    ? "SELECT ts, v FROM tab SUBSAMPLE " + method
                    : "SELECT ts, v FROM (SELECT ts, v, " + method + " OVER (ORDER BY ts) keep FROM tab) WHERE keep";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                TextPlanSink plan = new TextPlanSink();
                plan.of(factory, sqlExecutionContext);
                TestUtils.assertContains(plan.getSink(), isFused ? "CachedWindowLightSelect" : "Filter filter: keep");
                if (!isFused) {
                    TestUtils.assertContains(plan.getSink(), "CachedWindowLight");
                    Assert.assertFalse(plan.getSink().toString().contains("CachedWindowLightSelect"));
                }
                TestUtils.assertContains(plan.getSink(), "unorderedFunctions: [" + method.replace("_", ""));
                assertDenseSelection(factory, stride, isFused);
            }
        });
    }
}
