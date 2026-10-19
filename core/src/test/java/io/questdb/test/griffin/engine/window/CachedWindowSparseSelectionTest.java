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
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

public class CachedWindowSparseSelectionTest extends AbstractCairoTest {
    @Test
    public void testDenseMappingCancellationThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTable(4096);
            final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
            try (RecordCursorFactory factory = select(uniformQuery(true))) {
                // uniform(65) enumerates at checkpoint 1, translates at 2..66 and emits
                // bitmap words at 67..130. Cancel in both loops, including their last checks.
                for (int cancelAt : new int[]{2, 33, 66, 67, 99, 130}) {
                    final CountingBreaker breaker = new CountingBreaker(0);
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                    MemoryTracker tracker = null;
                    try {
                        bindVariableService.setLong(0, 65);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            tracker = sqlExecutionContext.getMemoryTracker();
                            Assert.assertTrue(cursor.hasNext());
                            breaker.checks = 0;
                            breaker.cancelAt = cancelAt;
                            try {
                                mapSelectedRows(field(findLight(factory), "cursor"));
                                Assert.fail("expected cancellation at checkpoint " + cancelAt);
                            } catch (CairoException e) {
                                Assert.assertTrue(e.isCancellation());
                                Assert.assertEquals(cancelAt, breaker.checks);
                                assertFailureMethod(e, "mapSelectedRows");
                            }
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                        breaker.setCancelledFlag(new AtomicBoolean(true));
                        Assert.assertTrue(breaker.checkIfTripped());
                        ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                        assertUniformSelection(factory, 4096, 65, true);
                    } finally {
                        ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                        if (tracker != null) {
                            Assert.assertEquals(0, tracker.getUsed());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testDenseMappingDoesNotScanInput() throws Exception {
        assertMemoryLeak(() -> {
            createTable(4096);
            // The cutoff is 64 selections. Keep the final ordinal so a sequential replay
            // must visit all 4096 rows, even though the bitmap needs only 64 words.
            bindVariableService.setLong(0, 65);
            final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
            final CountingBreaker breaker = new CountingBreaker();
            ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
            try (RecordCursorFactory factory = select(uniformQuery(false))) {
                final Object lightCursor = field(findLight(factory), "cursor");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    for (int run = 0; run < 2; run++) {
                        // Isolate remapping from buffering, sorting and pass1. Indexing the
                        // selected rows and scanning the bitmap take 130 checks, not 4161.
                        breaker.checks = 0;
                        mapSelectedRows(lightCursor);
                        Assert.assertTrue("mapping checks=" + breaker.checks, breaker.checks <= 256);
                        Assert.assertEquals(65, ((DirectLongList) field(lightCursor, "selectedRowIds")).size());
                        Assert.assertEquals("dense mapping must retain the bitmap", 64,
                                ((DirectLongList) field(lightCursor, "selectedRowBits")).getCapacity());
                    }
                }
                assertUniformSelection(factory, 4096, 65, false);
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
            }
        });
    }

    @Test
    public void testDenseMappingRejectsInvalidIndices() throws Exception {
        assertMappingRejectsInvalidIndices(65);
    }

    @Test
    public void testIndexedReadBoundsAndTraversalPosition() throws Exception {
        assertMemoryLeak(() -> {
            createTable(4096);
            try (RecordCursorFactory factory = select(uniformQuery(false));
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                final Object group = sortBuffer(field(findLight(factory), "cursor"));
                final Method getter = group.getClass().getDeclaredMethod("getRowIdAt", long.class);
                getter.setAccessible(true);
                final long position = (long) field(group, "currentAddr");
                Assert.assertEquals(4095L, getter.invoke(group, 0L));
                Assert.assertEquals(0L, getter.invoke(group, 4095L));
                Assert.assertEquals(position, field(group, "currentAddr"));
                for (long ordinal : new long[]{-1, 4096, Long.MAX_VALUE}) {
                    try {
                        getter.invoke(group, ordinal);
                        Assert.fail("expected bounds check before native read");
                    } catch (InvocationTargetException e) {
                        Assert.assertTrue(e.getCause() instanceof CairoException);
                        TestUtils.assertContains(((CairoException) e.getCause()).getFlyweightMessage(), "traversal index out of bounds");
                    }
                }
            }
        });
    }

    @Test
    public void testSparseAndDenseMappingReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTable(4096);
            for (int source = 0; source < 2; source++) {
                final boolean isPermuted = source == 1;
                try (RecordCursorFactory factory = select(uniformQuery(isPermuted))) {
                    // Insertion/merge boundary, odd merge tails and both sides of the density
                    // cutoff. Reuse the same factory across dense, identity and sparse modes.
                    for (int target : new int[]{2, 3, 16, 17, 31, 32, 33, 63, 64, 65, 2048, 4095, 4096, 3, 65, 17}) {
                        bindVariableService.setLong(0, target);
                        assertUniformSelection(factory, 4096, target, isPermuted);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            final DirectLongList bits = (DirectLongList) field(field(findLight(factory), "cursor"), "selectedRowBits");
                            Assert.assertEquals(target > 64 && target < 4096 ? 64 : 16, bits.getCapacity());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSparseMappingCancellationThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTable(131_072);
            final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
            try (RecordCursorFactory factory = select(uniformQuery(true))) {
                // uniform(1025) enumerates at checkpoints 1..2, translates at 3..4, starts
                // sorting at 5, merges at 6..27 and copies back/validates at 28..29.
                for (int cancelAt : new int[]{3, 4, 5, 7, 29}) {
                    final CountingBreaker breaker = new CountingBreaker();
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                    MemoryTracker tracker = null;
                    try {
                        bindVariableService.setLong(0, 1025);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            tracker = sqlExecutionContext.getMemoryTracker();
                            Assert.assertTrue(cursor.hasNext());
                            breaker.checks = 0;
                            breaker.cancelAt = cancelAt;
                            try {
                                mapSelectedRows(field(findLight(factory), "cursor"));
                                Assert.fail("expected cancellation at checkpoint " + cancelAt);
                            } catch (CairoException e) {
                                Assert.assertTrue(e.isCancellation());
                                Assert.assertEquals(cancelAt, breaker.checks);
                                assertFailureMethod(e, cancelAt <= 4 ? "mapSparseSelectedRows" : "sortSparseSelectedRows");
                            }
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                        // Query close releases its cancellation state. Give the old breaker a
                        // private cancelled flag so a stale init() binding fails on the next open.
                        breaker.setCancelledFlag(new AtomicBoolean(true));
                        Assert.assertTrue(breaker.checkIfTripped());
                        ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                        bindVariableService.setLong(0, 3);
                        assertUniformSelection(factory, 131_072, 3, true);
                    } finally {
                        ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                        if (tracker != null) {
                            Assert.assertEquals(0, tracker.getUsed());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSparseMappingDoesNotScanInput() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096L);
            final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
            final CountingBreaker breaker = new CountingBreaker();
            ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
            try {
                for (int rowCount : new int[]{16_384, 1_000_000}) {
                    execute("CREATE TABLE tab" + rowCount + " AS (SELECT timestamp_sequence(0, 1000) ts, x v FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts)");
                    try (RecordCursorFactory factory = select("SELECT ts, v FROM (SELECT ts, v FROM tab" + rowCount + " ORDER BY ts DESC) SUBSAMPLE uniform(3)")) {
                        final Object lightCursor = field(findLight(factory), "cursor");
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            for (int run = 0; run < 2; run++) {
                                // Isolate executor remapping, not buffering, sort construction or pass1.
                                // Count checkpoints rather than time: a replay needs N + ceil(N/64)
                                // calls; indexing and sorting three rows need only constant work.
                                breaker.checks = 0;
                                mapSelectedRows(lightCursor);
                                Assert.assertTrue("mapping checks=" + breaker.checks, breaker.checks <= 8);
                                final DirectLongList ids = (DirectLongList) field(lightCursor, "selectedRowIds");
                                Assert.assertEquals(3, ids.size());
                                Assert.assertEquals(0, ids.get(0));
                                Assert.assertEquals(rowCount / 2 - 1, ids.get(1));
                                Assert.assertEquals(rowCount - 1, ids.get(2));
                                Assert.assertEquals("sparse mapping must not grow the bitmap", 16,
                                        ((DirectLongList) field(lightCursor, "selectedRowBits")).getCapacity());
                            }
                        }
                        assertUniformSelection(factory, rowCount, 3, false);
                    }
                }
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
            }
        });
    }

    @Test
    public void testSparseMappingEmptyAndSingleSelection() throws Exception {
        assertMemoryLeak(() -> {
            createTable(4096);
            execute("UPDATE tab SET v = NULL");
            try (RecordCursorFactory factory = select("SELECT ts, v FROM (SELECT ts, v FROM tab ORDER BY ts DESC) SUBSAMPLE lttb(v, 3)")) {
                assertFactory(factory).withContext(sqlExecutionContext).timestampDesc("ts").returns("ts\tv\n");
                execute("UPDATE tab SET v = 42 WHERE ts = 1000::TIMESTAMP");
                assertFactory(factory).withContext(sqlExecutionContext).timestampDesc("ts").returns("""
                        ts\tv
                        1970-01-01T00:00:00.001000Z\t42
                        """);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    final Object lightCursor = field(findLight(factory), "cursor");
                    Assert.assertEquals(1, ((DirectLongList) field(lightCursor, "selectedRowIds")).size());
                    Assert.assertEquals(16, ((DirectLongList) field(lightCursor, "selectedRowBits")).getCapacity());
                }
                execute("UPDATE tab SET v = NULL");
                assertFactory(factory).withContext(sqlExecutionContext).timestampDesc("ts").returns("ts\tv\n");
                execute("TRUNCATE TABLE tab");
                assertFactory(factory).withContext(sqlExecutionContext).timestampDesc("ts").returns("ts\tv\n");
            }
        });
    }

    @Test
    public void testMappingMatchesFullWindowWithNullsTiesAndPayloads() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab AS (
                      SELECT CASE WHEN x % 127 = 0 THEN NULL ELSE (x / 3)::TIMESTAMP END ts,
                        CASE WHEN x % 7 = 0 THEN NULL ELSE x::DOUBLE END v,
                        CASE WHEN x % 11 = 0 THEN NULL ELSE ('row-' || x) END::STRING s,
                        CASE WHEN x % 13 = 0 THEN NULL ELSE ('value-' || x) END::VARCHAR vc,
                        x id
                      FROM long_sequence(4096)
                    )
                    """);
            for (int method = 0; method < 12; method++) {
                final boolean isDense = method >= 6;
                final String selection = switch (method) {
                    case 0 -> "uniform(3)";
                    case 1 -> "uniform(17)";
                    case 2 -> "cadence(128)";
                    case 3 -> "lttb(v, 17)";
                    case 4 -> "m4(v, 17)";
                    case 5 -> "minmax(v, 17)";
                    case 6 -> "uniform(65)";
                    case 7 -> "uniform(4095)";
                    case 8 -> "cadence(2)";
                    case 9 -> "lttb(v, 129)";
                    case 10 -> "m4(v, 129)";
                    default -> "minmax(v, 129)";
                };
                final String sql = "SELECT ts, v, s, vc, id FROM (SELECT * FROM tab TIMESTAMP(ts) ORDER BY id DESC) SUBSAMPLE " + selection;
                // The full window materializes every keep flag and filters separately. Use it
                // as the independent mapping oracle, including NULL removal and tied keys.
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "false");
                final StringSink expected = new StringSink();
                TestUtils.printSql(engine, sqlExecutionContext, sql, expected);
                assertQuery(sql).withPlanNotContaining("CachedWindowLightSelect").returns(expected);
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
                assertQuery(sql).withPlanContaining("CachedWindowLightSelect", "orderedFunctions: [[ts]").returns(expected);
                try (RecordCursorFactory factory = select(sql);
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    final Object lightCursor = field(findLight(factory), "cursor");
                    final DirectLongList ids = (DirectLongList) field(lightCursor, "selectedRowIds");
                    Assert.assertTrue(isDense ? ids.size() > 64 && ids.size() < 4096 : ids.size() > 0 && ids.size() <= 64);
                    Assert.assertEquals(isDense ? 64 : 16, ((DirectLongList) field(lightCursor, "selectedRowBits")).getCapacity());
                }
            }
        });
    }

    @Test
    public void testSparseMappingMemoryLimitThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTable(131_072);
            bindVariableService.setLong(0, 1025);
            try (RecordCursorFactory factory = select(uniformQuery(false))) {
                final long peak = sparseMappingMemory(factory);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, peak);
                Assert.assertEquals(peak, sparseMappingMemory(factory));
                assertUniformSelection(factory, 131_072, 1025, false);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, peak - 1);
                MemoryTracker tracker;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    tracker = sqlExecutionContext.getMemoryTracker();
                    try {
                        cursor.hasNext();
                        Assert.fail("expected tracked output-list allocation to exceed the limit");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        assertFailureMethod(e, "mapSparseSelectedRows");
                    }
                }
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                Assert.assertEquals(peak, sparseMappingMemory(factory));
                bindVariableService.setLong(0, 3);
                assertUniformSelection(factory, 131_072, 3, false);
            }
        });
    }

    @Test
    public void testSparseMappingRejectsInvalidIndices() throws Exception {
        assertMappingRejectsInvalidIndices(3);
    }

    @Test
    public void testSparseMergeAboveNativeSortThreshold() throws Exception {
        assertMemoryLeak(() -> {
            createTable(65_536);
            for (int source = 0; source < 2; source++) {
                final boolean isPermuted = source == 1;
                try (RecordCursorFactory factory = select(uniformQuery(isPermuted))) {
                    for (int target : new int[]{599, 600, 601, 1023, 1024, 1025, 3}) {
                        bindVariableService.setLong(0, target);
                        assertUniformSelection(factory, 65_536, target, isPermuted);
                    }
                }
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

    private static void assertMappingFailure(Object cursor, String message) throws Exception {
        try {
            mapSelectedRows(cursor);
            Assert.fail("expected " + message);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private void assertMappingRejectsInvalidIndices(int target) throws Exception {
        assertMemoryLeak(() -> {
            createTable(4096);
            bindVariableService.setLong(0, target);
            try (RecordCursorFactory factory = select(uniformQuery(false));
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                final CachedWindowLightRecordCursorFactory light = findLight(factory);
                final Object lightCursor = field(light, "cursor");
                final DirectLongList selected = (DirectLongList) field(light.getSingleRowSelectingFunction(), "selected");
                final long savedOrdinal = selected.get(1);
                for (long ordinal : new long[]{-1, 4096, Long.MAX_VALUE, selected.get(0), selected.get(2)}) {
                    try {
                        selected.set(1, ordinal);
                        assertMappingFailure(lightCursor, ordinal < 0 || ordinal >= 4096
                                ? "traversal index out of bounds" : "invalid row-selecting traversal order");
                    } finally {
                        selected.set(1, savedOrdinal);
                    }
                }
                final long nextOrdinal = selected.get(2);
                try {
                    // Swap distinct ordinals: the bitmap's duplicate check alone cannot
                    // reject a selection that violates the ascending-order contract.
                    selected.set(1, nextOrdinal);
                    selected.set(2, savedOrdinal);
                    assertMappingFailure(lightCursor, "invalid row-selecting traversal order");
                } finally {
                    selected.set(1, savedOrdinal);
                    selected.set(2, nextOrdinal);
                }
                final Object group = sortBuffer(lightCursor);
                final long entry = (long) field(group, "startAddr");
                final int entrySize = (int) field(group, "entrySize");
                final long savedRow = Unsafe.getLong(entry);
                final long duplicateRow = Unsafe.getLong(entry + savedOrdinal * entrySize);
                for (long row : new long[]{-1, 4096, duplicateRow}) {
                    try {
                        Unsafe.putLong(entry, row);
                        assertMappingFailure(lightCursor, row == duplicateRow
                                ? "invalid row-selecting traversal order" : "traversal index out of bounds");
                    } finally {
                        Unsafe.putLong(entry, savedRow);
                    }
                }
                mapSelectedRows(lightCursor);
            }
        });
    }

    private void assertUniformSelection(RecordCursorFactory factory, int rowCount, int target, boolean isPermuted) throws Exception {
        final QueryAssertion assertion = assertFactory(factory).withContext(sqlExecutionContext);
        if (!isPermuted) {
            assertion.timestampDesc("ts");
        }
        assertion.returns(uniformExpected(rowCount, target, isPermuted));
    }

    private void createTable(int rowCount) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096L);
        execute("CREATE TABLE tab AS (SELECT timestamp_sequence(0, 1000) ts, x v, ((x - 1) * 7919) % "
                + rowCount + " k FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts)");
        bindVariableService.setLong(0, 3);
    }

    private static Object field(Object object, String name) throws Exception {
        final Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    private static CachedWindowLightRecordCursorFactory findLight(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof CachedWindowLightRecordCursorFactory light) {
                return light;
            }
        }
        throw new AssertionError("expected a cached LIGHT window factory");
    }

    private static void mapSelectedRows(Object cursor) throws Exception {
        final Method method = cursor.getClass().getDeclaredMethod("mapSelectedRows");
        method.setAccessible(true);
        try {
            method.invoke(cursor);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
    }

    private static Object sortBuffer(Object cursor) throws Exception {
        final ObjList<?> buffers = (ObjList<?>) field(cursor, "sortBuffers");
        Assert.assertEquals(1, buffers.size());
        return buffers.getQuick(0);
    }

    private long sparseMappingMemory(RecordCursorFactory factory) throws Exception {
        MemoryTracker tracker = null;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            tracker = sqlExecutionContext.getMemoryTracker();
            Assert.assertTrue(cursor.hasNext());
            final long used = tracker.getUsed();
            final Object lightCursor = field(findLight(factory), "cursor");
            Assert.assertEquals(16, ((DirectLongList) field(lightCursor, "selectedRowBits")).getCapacity());
            final long mallocs = Unsafe.getMallocCount();
            final long reallocs = Unsafe.getReallocCount();
            mapSelectedRows(lightCursor);
            Assert.assertEquals("remapping must reuse existing list storage", mallocs, Unsafe.getMallocCount());
            Assert.assertEquals(reallocs, Unsafe.getReallocCount());
            Assert.assertEquals(used, tracker.getUsed());
            return used;
        } finally {
            if (tracker != null) {
                Assert.assertEquals(0, tracker.getUsed());
            }
        }
    }

    private static StringSink uniformExpected(int rowCount, int target, boolean isPermuted) {
        final boolean[] isSelected = new boolean[rowCount];
        for (int i = 0; i < target; i++) {
            isSelected[(int) ((i * (rowCount - 1L) + (target - 1) / 2) / (target - 1))] = true;
        }
        final int[] ordinalAtKey = new int[rowCount];
        if (isPermuted) {
            for (int i = 0; i < rowCount; i++) {
                ordinalAtKey[(int) (i * 7919L % rowCount)] = i;
            }
        }
        final StringSink expected = new StringSink();
        expected.put("ts\tv\n");
        for (int i = 0; i < rowCount; i++) {
            final int ordinal = isPermuted ? ordinalAtKey[i] : rowCount - 1 - i;
            if (isSelected[ordinal]) {
                MicrosFormatUtils.appendDateTimeUSec(expected, ordinal * 1000L);
                expected.put('\t').put(ordinal + 1).put('\n');
            }
        }
        return expected;
    }

    private static String uniformQuery(boolean isPermuted) {
        return "SELECT ts, v FROM (SELECT ts, v FROM tab ORDER BY " + (isPermuted ? "k" : "ts DESC") + ") SUBSAMPLE uniform($1)";
    }

    private static class CountingBreaker extends AtomicBooleanCircuitBreaker {
        private long cancelAt = Long.MAX_VALUE;
        private long checks;

        CountingBreaker() {
            this(2_000_000);
        }

        CountingBreaker(int throttle) {
            super(engine, throttle);
        }

        @Override
        public void statefulThrowExceptionIfTripped() {
            check();
            super.statefulThrowExceptionIfTripped();
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            check();
            super.statefulThrowExceptionIfTrippedTimeThrottled();
        }

        private void check() {
            if (++checks == cancelAt) {
                cancel();
            }
        }
    }
}
