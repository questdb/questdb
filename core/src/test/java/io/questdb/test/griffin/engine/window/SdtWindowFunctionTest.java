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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import org.junit.Assert;
import org.junit.Test;

public class SdtWindowFunctionTest extends AbstractCairoTest {

    private static final String DDL = "create table tab (ts timestamp, val double) timestamp(ts)";
    private static final int JSON_BUFFER_SIZE = 1_048_576;
    private static final String LIFECYCLE_EXPECTED = "id\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\tfalse\n5\ttrue\n";

    @Test
    public void testPartitionExpressionCursorClosedLight() throws Exception {
        assertPartitionExpressionCursorClosed(true);
    }

    @Test
    public void testPartitionExpressionCursorClosedRegular() throws Exception {
        assertPartitionExpressionCursorClosed(false);
    }

    @Test
    public void testPartitionExpressionLifecycleControls() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, JSON_BUFFER_SIZE);
            execute("CREATE TABLE lifecycle (id INT, k INT, j VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lifecycle SELECT x::int, 1, '{\"k\":\"12345\"}', x::double, x::timestamp FROM long_sequence(5)");
            for (boolean isLight : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (boolean isSorted : new boolean[]{false, true}) {
                    String order = isSorted ? "id" : "ts";
                    Assert.assertTrue(assertLifecycleQuery("SELECT id, sdt(ts, val, 0.1) OVER (ORDER BY " + order + ") keep FROM lifecycle", isLight) < 65_536);
                    Assert.assertTrue(assertLifecycleQuery("SELECT id, sdt(ts, val, 0.1) OVER (PARTITION BY k ORDER BY " + order + ") keep FROM lifecycle", isLight) < 65_536);
                    Assert.assertTrue(assertLifecycleQuery("SELECT id, sdt(ts, val, 0.1) OVER (PARTITION BY json_extract(j, '$.k')::int ORDER BY " + order + ") keep FROM lifecycle", isLight) < 65_536);
                    Assert.assertTrue(assertLifecycleQuery("SELECT id, sdt(ts, length(json_extract(j, '$.k')), 0.1) OVER (ORDER BY " + order + ") keep FROM lifecycle", isLight) < 65_536);
                    Assert.assertTrue(assertLifecycleQuery("SELECT id, sdt(ts, length(json_extract(j, '$.k')), 0.1) OVER (PARTITION BY k ORDER BY " + order + ") keep FROM lifecycle", isLight) < 65_536);
                }
            }
        });
    }

    @Test
    public void testPartitionExpressionNullAndMultiplePartitionControls() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, JSON_BUFFER_SIZE);
            execute("CREATE TABLE lifecycle (id INT, j VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO lifecycle VALUES
                    (1, '{"k":"a"}', 1, 1::timestamp),
                    (2, '{"k":"bb"}', 2, 2::timestamp),
                    (3, NULL, 3, 3::timestamp),
                    (4, '{"k":"a"}', 4, 4::timestamp),
                    (5, '{"k":"bb"}', 5, 5::timestamp),
                    (6, NULL, 6, 6::timestamp),
                    (7, '{"k":"a"}', 7, 7::timestamp),
                    (8, '{"k":"bb"}', 8, 8::timestamp),
                    (9, NULL, 9, 9::timestamp)
                    """);
            for (boolean isLight : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (boolean isSorted : new boolean[]{false, true}) {
                    String query = "SELECT id, sdt(ts, val, 0.1) OVER (PARTITION BY abs(length(json_extract(j, '$.k')) + 1) ORDER BY "
                            + (isSorted ? "id" : "ts") + ") keep FROM lifecycle";
                    assertLifecycleQuery(query, isLight, """
                            id\tkeep
                            1\ttrue
                            2\ttrue
                            3\ttrue
                            4\tfalse
                            5\tfalse
                            6\tfalse
                            7\ttrue
                            8\ttrue
                            9\ttrue
                            """, 9, 1);
                    assertLifecycleQuery(query + " WHERE id < 0", isLight, "id\tkeep\n", 0, 1);
                    assertLifecycleQuery(query + " WHERE id = 3", isLight, "id\tkeep\n3\ttrue\n", 1, 1);
                }
            }
        });
    }

    @Test
    public void testPartitionExpressionCancellationThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, JSON_BUFFER_SIZE);
            execute("CREATE TABLE lifecycle (id INT, j VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lifecycle SELECT x::int, '{\"k\":\"12345\"}', x::double, x::timestamp FROM long_sequence(5)");
            for (boolean isLight : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (boolean isSorted : new boolean[]{false, true}) {
                    String query = "SELECT id, sdt(ts, val, 0.1) OVER (PARTITION BY length(json_extract(j, '$.k')) ORDER BY "
                            + (isSorted ? "id" : "ts") + ") keep FROM lifecycle";
                    long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
                    try (RecordCursorFactory factory = select(query)) {
                        assertLifecycleFactory(factory, isLight);
                        MemoryTracker tracker;
                        long live;
                        // No hasNext: init alone inflates both output sinks. Double close must be safe.
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            tracker = sqlExecutionContext.getMemoryTracker();
                            live = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
                            cursor.close();
                            Assert.assertEquals(2L * JSON_BUFFER_SIZE, live - Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
                            assertLifecycleClosed(baseline);
                            cursor.close();
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        assertLifecycleClosed(baseline);
                        SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
                        AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
                        ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                        try {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                tracker = sqlExecutionContext.getMemoryTracker();
                                live = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
                                breaker.cancel();
                                try {
                                    cursor.hasNext();
                                    Assert.fail("expected cancellation after partition expression init");
                                } catch (CairoException e) {
                                    Assert.assertTrue(e.isCancellation());
                                }
                            }
                            Assert.assertEquals(2L * JSON_BUFFER_SIZE, live - Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
                            assertLifecycleClosed(baseline);
                            Assert.assertEquals(0, tracker.getUsed());
                            System.out.println("SDT_CANCEL light=" + isLight + " sorted=" + isSorted + " released=" + (2L * JSON_BUFFER_SIZE));
                        } finally {
                            breaker.reset();
                            ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                        }
                        assertLifecycleResult(factory, LIFECYCLE_EXPECTED, 5, baseline, 1);
                    }
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
                }
            }
        });
    }

    @Test
    public void testPartitionExpressionMultipleOwnersCursorClosed() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, JSON_BUFFER_SIZE);
            execute("CREATE TABLE lifecycle (id INT, k INT, j VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lifecycle SELECT x::int, 1, '{\"k\":\"a\",\"other\":\"bb\"}', x::double, x::timestamp FROM long_sequence(5)");
            for (boolean isLight : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (boolean isSorted : new boolean[]{false, true}) {
                    String order = isSorted ? "id" : "ts";
                    String query = "SELECT id, sdt(ts, val, 0.1) OVER (PARTITION BY k, json_extract(j, '$.k'), json_extract(j, '$.other') ORDER BY " + order
                            + ") keep, sdt(ts, val, 0.1) OVER (PARTITION BY k, abs(length(json_extract(j, '$.k')) + 1) ORDER BY " + order
                            + ") keep2, sum(val) OVER (PARTITION BY k) total, avg(val) OVER (PARTITION BY k) mean FROM lifecycle";
                    long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
                    try (RecordCursorFactory factory = select(query)) {
                        assertLifecycleFactory(factory, isLight);
                        Assert.assertEquals(ColumnType.BOOLEAN, factory.getMetadata().getColumnType(2));
                        Assert.assertEquals(ColumnType.DOUBLE, factory.getMetadata().getColumnType(3));
                        Assert.assertEquals(ColumnType.DOUBLE, factory.getMetadata().getColumnType(4));
                        assertLifecycleResult(factory, """
                                id\tkeep\tkeep2\ttotal\tmean
                                1\ttrue\ttrue\t15.0\t3.0
                                2\tfalse\tfalse\t15.0\t3.0
                                3\tfalse\tfalse\t15.0\t3.0
                                4\tfalse\tfalse\t15.0\t3.0
                                5\ttrue\ttrue\t15.0\t3.0
                                """, 5, baseline, 3);
                    }
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
                }
            }
        });
    }

    @Test
    public void testPartitionExpressionFailedOpenThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, JSON_BUFFER_SIZE);
            execute("CREATE TABLE lifecycle (id INT, j VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lifecycle SELECT x::int, '{\"k\":\"12345\"}', x::double, x::timestamp FROM long_sequence(5)");
            for (boolean isLight : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (boolean isSorted : new boolean[]{false, true}) {
                    String query = "SELECT id, sdt(ts, val, 0.1) OVER (PARTITION BY length(json_extract(j, '$.k')) ORDER BY "
                            + (isSorted ? "id" : "ts") + ") keep FROM lifecycle";
                    long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
                    try (RecordCursorFactory factory = select(query)) {
                        assertLifecycleFactory(factory, isLight);
                        // First fail before any successful init, then fail after successful reuse.
                        for (int run = 0; run < 2; run++) {
                            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
                            try {
                                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                                    Assert.fail("expected OOM during getCursor, not during hasNext");
                                } catch (CairoException e) {
                                    Assert.assertTrue("expected OOM: " + e.getFlyweightMessage(), e.isOutOfMemory());
                                    System.out.println("SDT_FAILED_OPEN light=" + isLight + " sorted=" + isSorted + " error=" + e.getFlyweightMessage());
                                }
                                assertLifecycleClosed(baseline);
                                Assert.assertNull("failed open must unbind its query tracker", sqlExecutionContext.getMemoryTracker());
                            } finally {
                                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                            }
                            assertLifecycleResult(factory, LIFECYCLE_EXPECTED, 5, baseline, 1);
                        }
                    }
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
                }
            }
        });
    }

    @Test
    public void testPartitionExpressionEvaluationFailureThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, JSON_BUFFER_SIZE);
            execute("CREATE TABLE lifecycle (id INT, j VARCHAR, val DOUBLE, ats STRING, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lifecycle SELECT x::int, '{\"k\":\"12345\"}', 0.0, '1970-01-01T00:00:00.000000001Z', x::timestamp FROM long_sequence(5)");
            for (boolean isLight : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (boolean isSorted : new boolean[]{false, true}) {
                    bindVariableService.setBoolean(0, false);
                    String query = "SELECT id, sdt(CASE WHEN $1 THEN 'not-a-timestamp' ELSE ats END, val, 0.1) OVER (PARTITION BY length(json_extract(j, '$.k')) ORDER BY "
                            + (isSorted ? "id" : "ts") + ") keep FROM lifecycle";
                    long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
                    try (RecordCursorFactory factory = select(query)) {
                        assertLifecycleFactory(factory, isLight);
                        // Constant valid timestamps make each row a timestamp boundary.
                        String expected = "id\tkeep\n1\ttrue\n2\ttrue\n3\ttrue\n4\ttrue\n5\ttrue\n";
                        assertLifecycleResult(factory, expected, 5, baseline, 1);
                        bindVariableService.setBoolean(0, true);
                        MemoryTracker tracker;
                        long live;
                        try {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                tracker = sqlExecutionContext.getMemoryTracker();
                                live = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
                                try {
                                    cursor.hasNext();
                                    Assert.fail("expected implicit STRING timestamp conversion failure in pass1");
                                } catch (ImplicitCastException e) {
                                    Assert.assertEquals("inconvertible value: `not-a-timestamp` [STRING -> TIMESTAMP_NS]", e.getFlyweightMessage().toString());
                                    boolean hasSdtPass1 = false;
                                    for (StackTraceElement frame : e.getStackTrace()) {
                                        hasSdtPass1 |= frame.getClassName().endsWith("SdtWindowFunctionFactory$SdtOverPartitionFunction") && frame.getMethodName().equals("pass1");
                                    }
                                    // SDT pass1 evaluates the partition key before calling the timestamp getter.
                                    Assert.assertTrue("error must follow SDT partition-key evaluation", hasSdtPass1);
                                    System.out.println("SDT_EVALUATION_ERROR light=" + isLight + " sorted=" + isSorted + " afterKey=" + hasSdtPass1 + " error=" + e.getFlyweightMessage());
                                }
                            }
                            Assert.assertEquals(2L * JSON_BUFFER_SIZE, live - Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
                            assertLifecycleClosed(baseline);
                            Assert.assertEquals(0, tracker.getUsed());
                        } finally {
                            bindVariableService.setBoolean(0, false);
                        }
                        assertLifecycleResult(factory, expected, 5, baseline, 1);
                    }
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
                }
            }
        });
    }

    @Test
    public void testPartitionExpressionReopenAfterInsert() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, JSON_BUFFER_SIZE);
            for (boolean isLight : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (boolean isSorted : new boolean[]{false, true}) {
                    String table = "lifecycle_" + isLight + '_' + isSorted;
                    execute("CREATE TABLE " + table + " (id INT, j VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("INSERT INTO " + table + " VALUES (1, '{\"k\":\"a\"}', 0, '1970-01-01'), (2, NULL, 0, '1970-01-02'), (5, '{\"k\":\"a\"}', 0, '1970-01-05'), (6, NULL, 0, '1970-01-06')");
                    drainWalQueue();
                    String query = "SELECT id, sdt(ts, val, 0.1) OVER (PARTITION BY json_extract(j, '$.k') ORDER BY "
                            + (isSorted ? "id" : "ts") + ") keep FROM " + table;
                    long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
                    try (RecordCursorFactory factory = select(query)) {
                        assertLifecycleFactory(factory, isLight);
                        assertLifecycleResult(factory, "id\tkeep\n1\ttrue\n2\ttrue\n5\ttrue\n6\ttrue\n", 4, baseline, 1);
                        execute("INSERT INTO " + table + " VALUES (3, '{\"k\":\"a\"}', 0, '1970-01-03'), (4, NULL, 0, '1970-01-04'), (7, '{\"k\":\"bb\"}', 0, '1970-01-07')");
                        drainWalQueue();
                        assertLifecycleResult(factory, "id\tkeep\n1\ttrue\n2\ttrue\n3\tfalse\n4\tfalse\n5\ttrue\n6\ttrue\n7\ttrue\n", 7, baseline, 1);
                    }
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
                }
            }
        });
    }

    private void assertLifecycleSize(RecordCursorFactory factory, long expectedSize) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            RecordCursor.Counter counter = new RecordCursor.Counter();
            counter.set(7);
            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
            Assert.assertEquals(7 + expectedSize, counter.get());
            Assert.assertFalse(cursor.hasNext());
            cursor.toTop();
            long rows = 0;
            while (cursor.hasNext()) {
                rows++;
            }
            Assert.assertEquals(expectedSize, rows);
        }
    }

    private static void assertLifecycleFactory(RecordCursorFactory factory, boolean isLight) {
        RecordCursorFactory current = factory;
        Class<?> expected = isLight ? CachedWindowLightRecordCursorFactory.class : CachedWindowRecordCursorFactory.class;
        while (current != null && !expected.isInstance(current)) {
            current = current.getBaseFactory();
        }
        Assert.assertNotNull(expected.getSimpleName(), current);
        Assert.assertEquals(ColumnType.BOOLEAN, factory.getMetadata().getColumnType(1));
        TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        for (int i = 1; i <= plan.getLineCount(); i++) {
            System.out.println("SDT_PLAN " + plan.getLine(i));
        }
    }

    private void assertLifecycleResult(RecordCursorFactory factory, String expected, long rows, long baseline, int jsonFunctions) throws Exception {
        new QueryAssertion(engine, factory).withContext(sqlExecutionContext).expectSize().returns(expected);
        assertLifecycleSize(factory, rows);
        for (int run = 0; run < 3; run++) {
            long live;
            MemoryTracker tracker;
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                tracker = sqlExecutionContext.getMemoryTracker();
                assertCursorTwoPass(expected, cursor, factory.getMetadata());
                live = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
            }
            long closed = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
            Assert.assertEquals("JSON output backing released", 2L * JSON_BUFFER_SIZE * jsonFunctions, live - closed);
            assertLifecycleClosed(baseline);
            Assert.assertEquals(0, tracker.getUsed());
            System.out.println("SDT_RELEASE functions=" + jsonFunctions + " bytes=" + (live - closed) + " retained=" + (closed - baseline));
        }
    }

    private long assertLifecycleQuery(String query, boolean isLight, String expected, long rows, int jsonFunctions) throws Exception {
        long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
        long retained;
        try (RecordCursorFactory factory = select(query)) {
            assertLifecycleFactory(factory, isLight);
            assertLifecycleResult(factory, expected, rows, baseline, jsonFunctions);
            retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK) - baseline;
        }
        Assert.assertEquals("factory disposal: " + query, baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
        return retained;
    }

    private static void assertLifecycleClosed(long baseline) {
        long retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK) - baseline;
        // JSON keeps its small parser/input/path state, not its max-size output backing.
        Assert.assertTrue("cursor-scoped JSON buffers retained=" + retained, retained >= 0 && retained < 65_536);
        Assert.assertEquals("busy readers after cursor close", 0, engine.getBusyReaderCount());
    }

    private long assertLifecycleQuery(String query, boolean isLight) throws Exception {
        long maxRetained = 0;
        long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
        try (RecordCursorFactory factory = select(query)) {
            assertLifecycleFactory(factory, isLight);
            new QueryAssertion(engine, factory).withContext(sqlExecutionContext).expectSize().returns("""
                    id\tkeep
                    1\ttrue
                    2\tfalse
                    3\tfalse
                    4\tfalse
                    5\ttrue
                    """);
            assertLifecycleSize(factory, 5);
            for (int run = 0; run < 3; run++) {
                long live;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("id\tkeep\n1\ttrue\n2\tfalse\n3\tfalse\n4\tfalse\n5\ttrue\n", cursor, factory.getMetadata());
                    live = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK) - baseline;
                }
                long retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK) - baseline;
                maxRetained = Math.max(maxRetained, retained);
                System.out.println("SDT_LIFECYCLE light=" + isLight + " run=" + run + " live=" + live + " retained=" + retained + " query=" + query);
            }
        }
        Assert.assertEquals("factory disposal: " + query, baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK));
        return maxRetained;
    }

    private void assertPartitionExpressionCursorClosed(boolean isLight) throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, JSON_BUFFER_SIZE);
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
            execute("CREATE TABLE lifecycle (id INT, j VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lifecycle SELECT x::int, '{\"k\":\"12345\"}', x::double, x::timestamp FROM long_sequence(5)");
            long maxRetained = 0;
            for (boolean isSorted : new boolean[]{false, true}) {
                String query = "SELECT id, sdt(ts, val, 0.1) OVER (PARTITION BY length(json_extract(j, '$.k')) ORDER BY "
                        + (isSorted ? "id" : "ts") + ") keep FROM lifecycle";
                maxRetained = Math.max(maxRetained, assertLifecycleQuery(query, isLight, LIFECYCLE_EXPECTED, 5, 1));
            }
            // JSON retains its small path/input/parser state until factory disposal, but not
            // the two max-size output buffers. Check while factories survive each cursor close.
            Assert.assertTrue("cursor-scoped JSON buffers retained=" + maxRetained, maxRetained < 65_536);
        });
    }

    @Test
    public void testRejectsNegativeCompdev() throws Exception {
        assertQuery("select ts, sdt(ts, val, -1.0) over (order by ts) from tab")
                .ddl(DDL)
                .fails(24, "compdev must be a non-negative finite constant"); // position of the compdev arg (verified against actual)
    }

    @Test
    public void testRejectsNanCompdev() throws Exception {
        assertQuery("select ts, sdt(ts, val, cast('NaN' as double)) over (order by ts) from tab")
                .ddl(DDL)
                .fails(24, "compdev must be a non-negative finite constant");
    }

    @Test
    public void testRejectsNonConstantCompdev() throws Exception {
        // The signature's 3rd slot ('d', lowercase = constant-required) makes the parser itself
        // reject a non-constant argument before our factory's newInstance ever runs; see the
        // deviation note in task-2-report.md.
        assertQuery("select ts, sdt(ts, val, val) over (order by ts) from tab")
                .ddl(DDL)
                .fails(24, "expected: DOUBLE constant, actual: DOUBLE");
    }

    @Test
    public void testRequiresOrderBy() throws Exception {
        assertQuery("select ts, sdt(ts, val, 0.5) over () from tab")
                .ddl(DDL)
                .fails(11, "sdt() requires ORDER BY");
    }

    @Test
    public void testRejectsFraming() throws Exception {
        assertQuery("select ts, sdt(ts, val, 0.5) over (order by ts rows between 1 preceding and current row) from tab")
                .ddl(DDL)
                .fails(11, "sdt() does not support framing; remove ROWS/RANGE clause");
    }

    @Test
    public void testMonotonicRampKeepsEndpoints() throws Exception {
        assertQuery("select ts, val, sdt(ts, val, 0.5) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab select x::timestamp, x from long_sequence(5)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t1.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t2.0\tfalse\n" +
                                "1970-01-01T00:00:00.000003Z\t3.0\tfalse\n" +
                                "1970-01-01T00:00:00.000004Z\t4.0\tfalse\n" +
                                "1970-01-01T00:00:00.000005Z\t5.0\ttrue\n"
                );
    }

    @Test
    public void testWithinBandNoiseCompresses() throws Exception {
        assertQuery("select ts, val, sdt(ts, val, 0.5) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,0.0),(2::timestamp,0.1),(3::timestamp,0.0),(4::timestamp,0.1),(5::timestamp,0.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t0.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t0.1\tfalse\n" +
                                "1970-01-01T00:00:00.000003Z\t0.0\tfalse\n" +
                                "1970-01-01T00:00:00.000004Z\t0.1\tfalse\n" +
                                "1970-01-01T00:00:00.000005Z\t0.0\ttrue\n"
                );
    }

    @Test
    public void testFilteringYieldsCompressedSet() throws Exception {
        assertQuery("select ts, val from (select ts, val, sdt(ts, val, 0.5) over (order by ts) keep from tab) where keep")
                .ddl(DDL, "insert into tab select x::timestamp, x from long_sequence(5)")
                .timestamp("ts")
                .returns(
                        "ts\tval\n" +
                                "1970-01-01T00:00:00.000001Z\t1.0\n" +
                                "1970-01-01T00:00:00.000005Z\t5.0\n"
                );
    }

    @Test
    public void testPartitionsAreIndependent() throws Exception {
        // two interleaved series, each a clean ramp -> each keeps its own endpoints
        assertQuery("select ts, sym, val, sdt(ts, val, 0.5) over (partition by sym order by ts) keep from tab")
                .ddl("create table tab (ts timestamp, sym symbol, val double) timestamp(ts)",
                        "insert into tab values " +
                                "(1::timestamp,'a',1.0),(2::timestamp,'b',10.0)," +
                                "(3::timestamp,'a',2.0),(4::timestamp,'b',20.0)," +
                                "(5::timestamp,'a',3.0),(6::timestamp,'b',30.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tsym\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\tb\t10.0\ttrue\n" +
                                "1970-01-01T00:00:00.000003Z\ta\t2.0\tfalse\n" +
                                "1970-01-01T00:00:00.000004Z\tb\t20.0\tfalse\n" +
                                "1970-01-01T00:00:00.000005Z\ta\t3.0\ttrue\n" +
                                "1970-01-01T00:00:00.000006Z\tb\t30.0\ttrue\n"
                );
    }

    @Test
    public void testRespectNullsFlushesLastPointBeforeGap() throws Exception {
        // A null forces a kept boundary and resets the series; the last real
        // sample before the gap is flushed (kept), only the interior 0 drops.
        assertQuery("select ts, val, sdt(ts, val, 0.5) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,0.0),(2::timestamp,0.0),(3::timestamp,0.0),(4::timestamp,null)," +
                        "(5::timestamp,5.0),(6::timestamp,5.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t0.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t0.0\tfalse\n" +
                                "1970-01-01T00:00:00.000003Z\t0.0\ttrue\n" +
                                "1970-01-01T00:00:00.000004Z\tnull\ttrue\n" +
                                "1970-01-01T00:00:00.000005Z\t5.0\ttrue\n" +
                                "1970-01-01T00:00:00.000006Z\t5.0\ttrue\n"
                );
    }

    @Test
    public void testIgnoreNullsSkipsNull() throws Exception {
        assertQuery("select ts, val, sdt(ts, val, 0.5) ignore nulls over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,0.0),(2::timestamp,0.0),(3::timestamp,null)," +
                        "(4::timestamp,0.0),(5::timestamp,0.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t0.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t0.0\tfalse\n" +
                                "1970-01-01T00:00:00.000003Z\tnull\tfalse\n" +
                                "1970-01-01T00:00:00.000004Z\t0.0\tfalse\n" +
                                "1970-01-01T00:00:00.000005Z\t0.0\ttrue\n"
                );
    }

    @Test
    public void testSubnormalPeakKeptWhenSlopesUnderflow() throws Exception {
        // (1e-320 +/- 1e-322) / 1e6 flush to the same 0.0 slope: finite, but the corridor
        // width vanished and the doors-crossed test could never fire, silently dropping the
        // stored peak at ~50x the stated 2 * compdev reconstruction bound.
        assertQuery("select ts, val, sdt(ts, val, 1e-322) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(0::timestamp,0.0),(1_000_000::timestamp,1e-320),(2_000_000::timestamp,0.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000000Z\t0.0\ttrue\n" +
                                "1970-01-01T00:00:01.000000Z\t1.0E-320\ttrue\n" +
                                "1970-01-01T00:00:02.000000Z\t0.0\ttrue\n"
                );
    }

    @Test
    public void testExplainPlanShowsSdt() throws Exception {
        assertQuery("select ts, sym, sdt(ts, val, 0.5) over (partition by sym order by ts) from tab")
                .ddl("create table tab (ts timestamp, sym symbol, val double) timestamp(ts)")
                .noLeakCheck()
                .assertsPlan("CachedWindowLight\n" +
                        "  unorderedFunctions: [sdt(ts, val, 0.5) over (partition by [sym] order by [ts])]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testPartitionedStatefulTimestampArgInitializedAndClosed() throws Exception {
        // Same regression as testStatefulTimestampArgInitializedAndClosed, for SdtOverPartitionFunction.
        assertQuery("select id from (select id, sdt(json_extract(j, '$.x')::timestamp, val, 0.0) over (partition by sym order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, sym symbol, j varchar, val double, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 'a', '{"x":"2024-01-01T00:00:00.000000Z"}', 0.0, '2024-01-01T00:00:00.000000Z'),
                                (1, 'b', '{"x":"2024-01-01T00:00:01.000000Z"}', 0.0, '2024-01-01T00:00:01.000000Z'),
                                (2, 'a', '{"x":"2024-01-01T00:00:02.000000Z"}', 0.0, '2024-01-01T00:00:02.000000Z'),
                                (3, 'b', '{"x":"2024-01-01T00:00:03.000000Z"}', 0.0, '2024-01-01T00:00:03.000000Z'),
                                (4, 'a', '{"x":"2024-01-01T00:00:04.000000Z"}', 0.0, '2024-01-01T00:00:04.000000Z'),
                                (5, 'b', '{"x":"2024-01-01T00:00:05.000000Z"}', 0.0, '2024-01-01T00:00:05.000000Z')""")
                .returns("""
                        id
                        0
                        1
                        4
                        5
                        """);
    }

    @Test
    public void testPartitionedBackwardTsArgAboveAnchorIsABoundary() throws Exception {
        // Regression: a backward step in the ts argument that stays ABOVE the partition's
        // current anchor is a series boundary (endpoint before it stays flushed, boundary row
        // re-anchors), same as the below-anchor step. Partitions interleave so the swinging-door
        // state - including pendingTs, which the boundary guard reads - round-trips through the
        // per-partition map between the rows of each series. Partition 'a' takes the backward
        // step (all four rows are two-point-segment endpoints); flat monotonic partition 'b'
        // keeps only its endpoints, proving interior compression still works alongside.
        assertQuery("select id from (select id, sdt(ats, val, 0.5) over (partition by sym order by ts) keep from tab) where keep order by id")
                .ddl("create table tab (id int, sym symbol, ats timestamp, val double, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 'a', 0::timestamp, 0.0, 1::timestamp),
                                (1, 'b', 0::timestamp, 0.0, 2::timestamp),
                                (2, 'a', 5000::timestamp, 0.0, 3::timestamp),
                                (3, 'b', 1000::timestamp, 0.0, 4::timestamp),
                                (4, 'a', 3000::timestamp, 0.0, 5::timestamp),
                                (5, 'b', 2000::timestamp, 0.0, 6::timestamp),
                                (6, 'a', 4000::timestamp, 0.0, 7::timestamp),
                                (7, 'b', 3000::timestamp, 0.0, 8::timestamp)""")
                .returns("""
                        id
                        0
                        1
                        2
                        4
                        6
                        7
                        """);
    }

    @Test
    public void testStatefulTimestampArgInitializedAndClosed() throws Exception {
        // Regression: BaseWindowFunction inits/frees only the value arg, so sdt must handle tsArg
        // itself. json_extract builds its native JSON pointer in init() and frees it in close();
        // without init() every read returns null, every row becomes a hard boundary (all rows
        // survive the filter), and without close() the native state leaks (fails the leak check).
        assertQuery("select id from (select id, sdt(json_extract(j, '$.x')::timestamp, val, 0.0) over (order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, j varchar, val double, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, '{"x":"2024-01-01T00:00:00.000000Z"}', 0.0, '2024-01-01T00:00:00.000000Z'),
                                (1, '{"x":"2024-01-01T00:00:01.000000Z"}', 0.0, '2024-01-01T00:00:01.000000Z'),
                                (2, '{"x":"2024-01-01T00:00:02.000000Z"}', 0.0, '2024-01-01T00:00:02.000000Z')""")
                .returns("""
                        id
                        0
                        2
                        """);
    }

    @Test
    public void testNullTimestampArgIsABoundaryUnderRespectNulls() throws Exception {
        // The timestamp argument is any TIMESTAMP expression, not the designated timestamp, so
        // it can be NULL. Such a row has no position on the time axis and cannot join a
        // corridor; RESPECT NULLS keeps it as a boundary and starts a new series after it.
        assertQuery("select id from (select id, sdt(ats, val, 0.0) over (order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, val double, ats timestamp, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 0.0,  '2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                                (1, 0.0,  null,                          '2024-01-01T00:00:01.000000Z'),
                                (2, 0.0,  '2024-01-01T00:00:02.000000Z', '2024-01-01T00:00:02.000000Z'),
                                (3, 10.0, '2024-01-01T00:00:03.000000Z', '2024-01-01T00:00:03.000000Z'),
                                (4, 20.0, '2024-01-01T00:00:04.000000Z', '2024-01-01T00:00:04.000000Z'),
                                (5, 30.0, '2024-01-01T00:00:05.000000Z', '2024-01-01T00:00:05.000000Z')""")
                .returns("""
                        id
                        0
                        1
                        2
                        5
                        """);
    }

    @Test
    public void testNullTimestampArgSkippedUnderIgnoreNulls() throws Exception {
        // IGNORE NULLS drops the row outright and leaves the corridor untouched, so the series
        // spans the gap: 0,0,0 is flat, then the 10/20/30 ramp keeps only its endpoints.
        assertQuery("select id from (select id, sdt(ats, val, 0.0) ignore nulls over (order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, val double, ats timestamp, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 0.0,  '2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                                (1, 0.0,  null,                          '2024-01-01T00:00:01.000000Z'),
                                (2, 0.0,  '2024-01-01T00:00:02.000000Z', '2024-01-01T00:00:02.000000Z'),
                                (3, 10.0, '2024-01-01T00:00:03.000000Z', '2024-01-01T00:00:03.000000Z'),
                                (4, 20.0, '2024-01-01T00:00:04.000000Z', '2024-01-01T00:00:04.000000Z'),
                                (5, 30.0, '2024-01-01T00:00:05.000000Z', '2024-01-01T00:00:05.000000Z')""")
                .returns("""
                        id
                        0
                        2
                        5
                        """);
    }

    @Test
    public void testNanosBackwardJumpWiderThanLongMaxIsABoundary() throws Exception {
        // No NULLs: a long holds only 292 years of nanoseconds, so the 2100 -> 1700 step is a
        // backward span wider than Long.MAX. The subtraction wraps positive and reads as a
        // forward step, and the flat corridor then drops row 1 as interior.
        assertQuery("select id from (select id, sdt(ats, val, 0.0) over (order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, val double, ats timestamp_ns, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 0.0, '2100-01-01T00:00:00.000000000Z', '2024-01-01T00:00:00.000000Z'),
                                (1, 0.0, '1700-01-01T00:00:00.000000000Z', '2024-01-01T00:00:01.000000Z'),
                                (2, 0.0, '2150-01-01T00:00:00.000000000Z', '2024-01-01T00:00:02.000000Z')""")
                .returns("""
                        id
                        0
                        1
                        2
                        """);
    }

    @Test
    public void testPartitionedSingleRowPerPartitionKept() throws Exception {
        assertQuery("select ts, sym, val, sdt(ts, val, 0.5) over (partition by sym order by ts) keep from tab")
                .ddl("create table tab (ts timestamp, sym symbol, val double) timestamp(ts)",
                        "insert into tab values (1::timestamp,'a',5.0),(2::timestamp,'b',9.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tsym\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\ta\t5.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\tb\t9.0\ttrue\n"
                );
    }

    @Test
    public void testHugeMagnitudeChangedPointIsKept() throws Exception {
        // F3-SDT-OVERFLOW red test: (1e308 + 0.0) - (-1e308) overflows to +Inf inside
        // SwingingDoor's slope terms, so rows 2 and 3 read the same +Inf slope and the corridor
        // wrongly drops row 2 as interior. All inputs are finite and compdev is 0, so any value
        // change must be kept: the hand-derived keep set is all three rows (true slopes from the
        // anchor are 2e308 at dt=1 vs 1e308 at dt=2 - not collinear).
        assertQuery("select ts, val, sdt(ts, val, 0.0) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,-1e308),(2::timestamp,1e308),(3::timestamp,1e308)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t-1.0E308\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t1.0E308\ttrue\n" +
                                "1970-01-01T00:00:00.000003Z\t1.0E308\ttrue\n"
                );
    }

    @Test
    public void testScaledProbeSeriesKeepsAllPoints() throws Exception {
        // F3-SDT-OVERFLOW preservation control (green pre-fix, must stay green): the same shape
        // at magnitude 1 has finite slopes (2 then 1), the doors cross and all rows are kept
        assertQuery("select ts, val, sdt(ts, val, 0.0) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,-1.0),(2::timestamp,1.0),(3::timestamp,1.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t-1.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t1.0\ttrue\n" +
                                "1970-01-01T00:00:00.000003Z\t1.0\ttrue\n"
                );
    }

    @Test
    public void testLongValueGoesThroughImplicitDoubleCast() throws Exception {
        // F2-M4-LONG cast pin: sdt does NOT share BucketSelectWindowFunction's buffer. Its
        // signature is sdt(NDd) - the value slot is DOUBLE - so a LONG column reaches the
        // function through the parser's implicit LONG -> DOUBLE cast, the same SQL-level
        // semantics as writing v::double: 2^53 and 2^53 + 1 are the same double, so the
        // corridor sees a flat series. With compdev 0.5 below the ULP at 2^53 (which is 2.0),
        // both tolerance numerators collapse (nU == nL == 0) and the arithmetic cannot certify
        // the 2 * compdev bound, so sdt keeps every row (F1-SDT-CANCEL: a collapsed corridor
        // with positive compdev always restarts; the earlier middle-row drop was an artifact
        // of the deleted nU != nL exemption, not of the cast). Exact-collinearity dropping
        // remains available via compdev == 0. The integral-exactness repair to minmax/m4 must
        // not alter sdt.
        assertQuery("select ts, v, sdt(ts, v, 0.5) over (order by ts) keep from t")
                .ddl("create table t (ts timestamp, v long) timestamp(ts)",
                        """
                                insert into t values
                                (1::timestamp, 9_007_199_254_740_992),
                                (2::timestamp, 9_007_199_254_740_993),
                                (3::timestamp, 9_007_199_254_740_992)
                                """)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tv\tkeep
                        1970-01-01T00:00:00.000001Z\t9007199254740992\ttrue
                        1970-01-01T00:00:00.000002Z\t9007199254740993\ttrue
                        1970-01-01T00:00:00.000003Z\t9007199254740992\ttrue
                        """);
    }

    @Test
    public void testCancellationCollapseKeepsMidSeriesPoint() throws Exception {
        // F1-SDT-CANCEL red test: with anchor -1e20, BOTH tolerance numerators of the middle
        // point flush to exactly 1e20 - the half-ULP at 1e20 is 8192, so the SUBTRACTION
        // (value +/- compdev) - anchorValue absorbs deviation 1000 and compdev 1.0 alike.
        // Pre-fix, nU == nL exempted the division-collapse restart in SwingingDoor, the
        // zero-width corridor swallowed the middle row, and reconstruction between the kept
        // endpoints read 0.0 where the stored value is 1000.0: 500x the documented
        // 2 * compdev bound.
        // All three stored doubles are exactly representable (1e20 = 2^20 * 5^20, 5^20 < 2^53)
        // and NOT collinear, so all three rows must be kept.
        assertQuery("select ts, val, sdt(ts, val, 1.0) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,-1e20),(2::timestamp,1000.0),(3::timestamp,1e20)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t-1.0E20\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t1000.0\ttrue\n" +
                                "1970-01-01T00:00:00.000003Z\t1.0E20\ttrue\n"
                );
    }

    @Test
    public void testCancellationCollapsePartitionedKeepsMidSeriesPoint() throws Exception {
        // F1-SDT-CANCEL red test, partitioned form: the same cancellation collapse through the
        // map-backed per-partition SwingingDoor state (SdtOverPartitionFunction saves and
        // reloads the corridor between interleaved rows). Each symbol carries the
        // (-1e20, 1000, 1e20) series; every row must be kept in both partitions.
        assertQuery("select ts, sym, val, sdt(ts, val, 1.0) over (partition by sym order by ts) keep from tab")
                .ddl("create table tab (ts timestamp, sym symbol, val double) timestamp(ts)",
                        "insert into tab values " +
                                "(1::timestamp,'a',-1e20),(2::timestamp,'b',-1e20)," +
                                "(3::timestamp,'a',1000.0),(4::timestamp,'b',1000.0)," +
                                "(5::timestamp,'a',1e20),(6::timestamp,'b',1e20)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tsym\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\ta\t-1.0E20\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\tb\t-1.0E20\ttrue\n" +
                                "1970-01-01T00:00:00.000003Z\ta\t1000.0\ttrue\n" +
                                "1970-01-01T00:00:00.000004Z\tb\t1000.0\ttrue\n" +
                                "1970-01-01T00:00:00.000005Z\ta\t1.0E20\ttrue\n" +
                                "1970-01-01T00:00:00.000006Z\tb\t1.0E20\ttrue\n"
                );
    }

    @Test
    public void testCancellationCollapseAfterDoorsCrossKeepsPendingPoint() throws Exception {
        // F1-SDT-CANCEL red test for the post-cross site: values 0, -2^80, 2^80, 3*2^80 + 2^29
        // with compdev 2e8. Against the first anchor compdev survives (half-ULP at 2^80 is
        // 2^27 ~ 1.34e8 < 2e8), so the pre-cross numerators stay distinct and the doors cross
        // at the third point. The promoted anchor is -2^80, and there the re-derived numerators
        // 2^81 +/- 2e8 BOTH flush to the same double (half-ULP at 2^81 is 2^28 ~ 2.68e8 > 2e8):
        // pre-fix, nU2 == nL2 exempted the post-cross restart, the zero-width corridor
        // survived, and the fourth point slid along it, dropping the third row. A restart on the collapsed
        // corridor keeps all four rows. (A shape whose outcome hinges on the post-cross
        // exemption ALONE cannot exist: any later no-cross point against a
        // cancellation-collapsed corridor has itself-collapsed numerators, so this red flips
        // under either site's fix - it pins that the post-cross path restarts too.)
        assertQuery("select ts, sdt(ts, val, 2e8) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,0.0)," +
                        "(2::timestamp,-1.2089258196146292e24)," + // -2^80
                        "(3::timestamp,1.2089258196146292e24)," + // 2^80
                        "(4::timestamp,3.626777458843888e24)") // 3*2^80 + 2^29
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\ttrue\n" +
                                "1970-01-01T00:00:00.000003Z\ttrue\n" +
                                "1970-01-01T00:00:00.000004Z\ttrue\n"
                );
    }

    @Test
    public void testCompdevZeroDropsDoubleArithmeticCollinearPoints() throws Exception {
        // F1-SDT-CANCEL contract pin (green pre-fix, must stay green post-fix): compdev == 0
        // asks for exact-collinearity filtering, and "exact" means double arithmetic. Here
        // 1000.0 - (-1e20) and 1e20 - (-1e20) evaluate to slopes 1e20 and 1e20 in doubles, so
        // the middle row reads as exactly on the line even though the real values are not
        // collinear. The cancellation-collapse restart applies only to compdev > 0 (that
        // conjunct guards it); sdt(ts, val, 0) keeps dropping double-collinear points rather
        // than degrading to keep-everything whenever a subtraction is inexact.
        assertQuery("select ts, val, sdt(ts, val, 0.0) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,-1e20),(2::timestamp,1000.0),(3::timestamp,1e20)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t-1.0E20\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t1000.0\tfalse\n" +
                                "1970-01-01T00:00:00.000003Z\t1.0E20\ttrue\n"
                );
    }
}
