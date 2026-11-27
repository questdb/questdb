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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastLongToSymbolFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A cached query keeps its functions between executions. The dictionary of a cast to SYMBOL must
 * not outlive the execution that filled it: the execution frees it when it ends, without any
 * cursor notification to the function.
 */
public class CastToSymbolExecutionReleaseTest extends AbstractCairoTest {
    private static final String[] TYPES = {
            "byte", "short", "int", "long", "float", "double", "char",
            "date", "timestamp", "timestamp_ns", "string", "varchar"
    };

    @Test
    public void testCachedQueryReleasesDictionaryAfterEachExecution() throws Exception {
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            for (String type : TYPES) {
                // 'x % 50 + 65' keeps CHAR printable; every type gets 50 distinct values.
                final String sql = "SELECT ((x % 50 + 65)::" + type + ")::symbol s FROM long_sequence(1_000)";
                try (RecordCursorFactory factory = select(sql)) {
                    for (int run = 0; run < 3; run++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            final Record record = cursor.getRecord();
                            final SymbolTable symbolTable = cursor.getSymbolTable(0);
                            int maxKey = -1;
                            while (cursor.hasNext()) {
                                final int key = record.getInt(0);
                                maxKey = Math.max(maxKey, key);
                                TestUtils.assertEquals(record.getSymA(0), symbolTable.valueOf(key));
                            }
                            Assert.assertEquals(type, 49, maxKey);
                            Assert.assertTrue(type, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > baseline);
                        }
                        // The factory stays open, as in a query cache, but holds no dictionary.
                        Assert.assertEquals(type, baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                    }
                }
            }
        });
    }

    @Test
    public void testCompileTimeEvaluationDoesNotLeak() throws Exception {
        assertMemoryLeak(() -> {
            // long_sequence() evaluates its arguments while compiling and then drops them.
            // The clock reads zero, so each cast renders "0".
            sqlExecutionContext.setNowAndFixClock(0, ColumnType.TIMESTAMP_MICRO);
            assertQuery("SELECT x FROM long_sequence(length(now()::symbol))")
                    .expectSize()
                    .returns("x\n1\n");
            assertQuery("SELECT count() FROM long_sequence(length(now()::symbol) + 1, length(now()::symbol), 1)")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
        });
    }

    @Test
    public void testExecutionWithoutRegistryDoesNotChargeTracker() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
            final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    sqlExecutionContext.getSecurityContext(),
                    1L,
                    MemoryTrackerWorkload.QUERY
            );
            sqlExecutionContext.setMemoryTracker(tracker);
            try (Function function = newCast(42)) {
                Assert.assertNull(sqlExecutionContext.getExecutionResources());
                function.init(null, sqlExecutionContext);
                Assert.assertEquals(0, function.getInt(null));
                TestUtils.assertEquals("42", ((SymbolFunction) function).valueOf(0));
                // No execution frees the dictionary before the tracker closes,
                // so the dictionary must not charge it.
                Assert.assertEquals(0, tracker.getUsed());
            } finally {
                sqlExecutionContext.setMemoryTracker(null);
                tracker.close();
            }
        });
    }

    @Test
    public void testMemoryLimitBreachReleasesDictionary() throws Exception {
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            try (RecordCursorFactory factory = select("SELECT x::symbol s FROM long_sequence(10_000)")) {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4096L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getInt(0);
                    }
                    Assert.fail("10,000 distinct values must exceed 4 KiB");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));

                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    long count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        count++;
                    }
                    Assert.assertEquals(10_000, count);
                }
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            }
        });
    }

    @Test
    public void testOwnerKeepsDictionaryUntilLastReference() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final long id = registry.registerOwner("SELECT 42::symbol", sqlExecutionContext);
            try (Function function = newCast(42)) {
                function.init(null, sqlExecutionContext);
                Assert.assertEquals(0, function.getInt(null));
                // A nested cursor of the same owner must not end the execution.
                Assert.assertEquals(id, registry.register("nested cursor", sqlExecutionContext));
                registry.unregister(id, sqlExecutionContext);
                TestUtils.assertEquals("42", ((SymbolFunction) function).valueOf(0));
                registry.unregister(id, sqlExecutionContext);
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                Assert.assertNull(((SymbolFunction) function).valueOf(0));
                Assert.assertNull(sqlExecutionContext.getExecutionResources());
            } finally {
                if (registry.getEntry(id) != null) {
                    registry.unregister(id, sqlExecutionContext);
                }
            }
        });
    }

    @Test
    public void testRegistryRollbackReleasesResources() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            try (Function function = newCast(42)) {
                registry.setListener((query, queryId, executionContext) -> {
                    try {
                        function.init(null, executionContext);
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    Assert.assertEquals(0, function.getInt(null));
                    throw new IllegalStateException("injected registration failure");
                });
                try {
                    registry.register("SELECT 42::symbol", sqlExecutionContext);
                    Assert.fail("registration must fail");
                } catch (IllegalStateException e) {
                    TestUtils.assertContains(e.getMessage(), "injected registration failure");
                } finally {
                    registry.setListener(null);
                }
                Assert.assertNull(sqlExecutionContext.getExecutionResources());
                Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                Assert.assertNull(((SymbolFunction) function).valueOf(0));
            }
        });
    }

    @Test
    public void testStaleRegistrationKeepsNewExecutionState() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final long first = registry.registerOwner("first", sqlExecutionContext);
            long second = -1;
            try (Function function = newCast(42)) {
                function.init(null, sqlExecutionContext);
                Assert.assertEquals(0, function.getInt(null));
                registry.unmountOwner(first, sqlExecutionContext);
                Assert.assertNull(sqlExecutionContext.getExecutionResources());

                // The second execution initializes the same function again while the first is still open.
                second = registry.registerOwner("second", sqlExecutionContext);
                final MemoryTracker secondTracker = sqlExecutionContext.getMemoryTracker();
                function.init(null, sqlExecutionContext);
                Assert.assertEquals(0, function.getInt(null));
                final long used = secondTracker.getUsed();
                Assert.assertTrue(used > 0);
                registry.unmountOwner(second, sqlExecutionContext);

                registry.mountOwner(first, sqlExecutionContext);
                registry.unregister(first, sqlExecutionContext);
                // The first execution no longer owns the dictionary.
                Assert.assertEquals(used, secondTracker.getUsed());
                TestUtils.assertEquals("42", ((SymbolFunction) function).valueOf(0));

                registry.mountOwner(second, sqlExecutionContext);
                registry.unregister(second, sqlExecutionContext);
                Assert.assertEquals(0, secondTracker.getUsed());
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                Assert.assertNull(((SymbolFunction) function).valueOf(0));
            } finally {
                if (registry.getEntry(first) != null) {
                    registry.unregister(first, sqlExecutionContext);
                }
                if (second > -1 && registry.getEntry(second) != null) {
                    registry.unregister(second, sqlExecutionContext);
                }
            }
        });
    }

    @Test
    public void testSuspendedOwnersKeepSeparateDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final long first = registry.registerOwner("first portal", sqlExecutionContext);
            final MemoryTracker firstTracker = sqlExecutionContext.getMemoryTracker();
            long second = -1;
            try (Function firstFunction = newCast(42); Function secondFunction = newCast(84)) {
                firstFunction.init(null, sqlExecutionContext);
                Assert.assertEquals(0, firstFunction.getInt(null));
                registry.unmountOwner(first, sqlExecutionContext);
                second = registry.registerOwner("second portal", sqlExecutionContext);
                final MemoryTracker secondTracker = sqlExecutionContext.getMemoryTracker();
                secondFunction.init(null, sqlExecutionContext);
                Assert.assertEquals(0, secondFunction.getInt(null));
                registry.unmountOwner(second, sqlExecutionContext);

                registry.mountOwner(first, sqlExecutionContext);
                TestUtils.assertEquals("42", ((SymbolFunction) firstFunction).valueOf(0));
                registry.unregister(first, sqlExecutionContext);
                Assert.assertEquals(0, firstTracker.getUsed());
                Assert.assertNull(((SymbolFunction) firstFunction).valueOf(0));
                Assert.assertTrue(secondTracker.getUsed() > 0);

                registry.mountOwner(second, sqlExecutionContext);
                TestUtils.assertEquals("84", ((SymbolFunction) secondFunction).valueOf(0));
                registry.unregister(second, sqlExecutionContext);
                Assert.assertEquals(0, secondTracker.getUsed());
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            } finally {
                if (registry.getEntry(first) != null) {
                    registry.unregister(first, sqlExecutionContext);
                }
                if (second > -1 && registry.getEntry(second) != null) {
                    registry.unregister(second, sqlExecutionContext);
                }
            }
        });
    }

    @Test
    public void testUnregisterReleasesDictionaryWithoutCursorNotification() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            try (Function function = newCast(42)) {
                for (int run = 0; run < 3; run++) {
                    final long id = registry.register("SELECT 42::symbol", sqlExecutionContext);
                    final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                    try {
                        function.init(null, sqlExecutionContext);
                        Assert.assertEquals(0, function.getInt(null));
                        Assert.assertTrue(tracker.getUsed() > 0);
                        TestUtils.assertEquals("42", ((SymbolFunction) function).valueOf(0));
                    } finally {
                        registry.unregister(id, sqlExecutionContext);
                    }
                    // The function stays open, as in a cached factory, and gets no cursorClosed().
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                    Assert.assertNull(((SymbolFunction) function).valueOf(0));
                    Assert.assertNull(sqlExecutionContext.getExecutionResources());
                }
            }
        });
    }

    private static Function newCast(long value) {
        final ObjList<Function> args = new ObjList<>();
        args.add(new LongFunction() {
            @Override
            public long getLong(Record rec) {
                return value;
            }
        });
        final IntList positions = new IntList();
        positions.add(0);
        return new CastLongToSymbolFunctionFactory().newInstance(0, args, positions, configuration, sqlExecutionContext);
    }
}
