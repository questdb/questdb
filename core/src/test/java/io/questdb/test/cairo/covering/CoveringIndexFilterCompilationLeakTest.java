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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestThrowingFilterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CoveringIndexFilterCompilationLeakTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testThreadUnsafeCoveringFilterStaysOnAsyncPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (s),
                        s STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00.000000Z', 'A', 'aa'),
                        ('2024-01-01T01:00:00.000000Z', 'B', 'bbb'),
                        ('2024-01-02T00:00:00.000000Z', 'A', ''),
                        ('2024-01-02T01:00:00.000000Z', 'A', NULL),
                        ('2024-01-02T02:00:00.000000Z', 'A', 'cccc')
                    """);
            engine.releaseAllWriters();

            // (s)::symbol is thread-unsafe rather than non-parallel: the async filter keeps it and
            // compileWorkerFiltersConditionally() hands each worker its own copy.
            for (String residualFilter : new String[]{"length(s) > 0", "length((s)::symbol) > 0"}) {
                try (RecordCursorFactory factory = select(
                        "SELECT s FROM tab WHERE sym = 'A' AND " + residualFilter
                )) {
                    Assert.assertTrue(residualFilter, containsFactory(factory, CoveringIndexRecordCursorFactory.class));
                    Assert.assertTrue(residualFilter, containsFactory(factory, AsyncFilteredRecordCursorFactory.class));
                    Assert.assertFalse(residualFilter, containsFactory(factory, FilteredRecordCursorFactory.class));
                }
            }

            assertQuery("SELECT s FROM tab WHERE sym = 'A' AND length((s)::symbol) > 0")
                    .noLeakCheck()
                    .returns("""
                            s
                            aa
                            cccc
                            """);
        });
    }

    @Test
    public void testWrapCoveringWithFilterLeakOnPartialWorkerFilterCompile() throws Exception {
        // A SELECT over a covering index with a residual filter routes through
        // wrapCoveringWithFilter, which builds an AsyncFilteredRecordCursorFactory
        // over the covering factory and compiles per-worker filter copies. The test
        // filter throws on the Nth construction so that, after the covering factory
        // and the original residual filter are already built, a per-worker compile
        // fails inside the wrapper.
        //
        // Call 1 builds the residual filter handed to wrapCoveringWithFilter.
        // Calls 2..N build per-worker copies. throwOnCall=3 lets call 2 succeed (one
        // worker filter held in the local list) and call 3 throw. The wrapper must
        // free the residual filter, the covering factory (which owns its index frame
        // factory and symbol function) and any limit function; the partial worker
        // filter is freed by compileWorkerFiltersConditionally. Each constructed
        // filter holds a native buffer, so an unfreed instance surfaces via
        // assertMemoryLeak.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab (" +
                            "ts TIMESTAMP, " +
                            "sym SYMBOL INDEX TYPE POSTING INCLUDE (price), " +
                            "price DOUBLE" +
                            ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL"
            );
            execute(
                    "INSERT INTO tab " +
                            "SELECT dateadd('h', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A', x::DOUBLE " +
                            "FROM long_sequence(20)"
            );
            engine.releaseAllWriters();

            TestThrowingFilterFunctionFactory.reset(3);

            try (
                    SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, 4);
                    RecordCursorFactory ignored = engine.select(
                            "SELECT price FROM tab WHERE sym = 'A' AND test_throwing_filter() LIMIT -3", ctx)
            ) {
                Assert.fail("expected SqlException from test_throwing_filter");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "configured to throw on call 3");
            }

            // 3 constructions total: the residual filter, the first worker copy, and
            // the third call that threw before its instance was created.
            Assert.assertEquals(3, TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get());
            // 2 instances must be closed: the residual filter (freed by the wrapper's
            // catch) and the partial worker filter (freed by
            // compileWorkerFiltersConditionally). Without the wrapper's catch the
            // residual filter leaks and this would be 1.
            Assert.assertEquals(2, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
        });
    }

    @Test
    public void testWrapAdaptiveSymbolPatternWithFilterClosesPartitionFactoryExactlyOnceOnThrow() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO tab
                    SELECT dateadd('h', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                           'A' || (x % 5),
                           x::DOUBLE
                    FROM long_sequence(20)
                    """);
            engine.releaseAllWriters();

            final int[] partitionFactoryCloseCount = new int[1];
            // Install the observer on the concrete partition-frame factory before compilation.
            // This counts actual close() invocations on the factory transferred to the adaptive
            // owner, rather than inferring them from AdaptiveSymbolPatternRecordCursorFactory.close().
            FullPartitionFrameCursorFactory.setCloseObserverForTesting(factory -> partitionFactoryCloseCount[0]++);
            try {
                final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 4) {
                    @Override
                    public boolean isParallelFilterEnabled() {
                        throw new RuntimeException("test adaptive symbol pattern wrap failure");
                    }
                };
                ctx.with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext());
                try (ctx) {
                    try (RecordCursorFactory ignored = engine.select(
                            "SELECT price FROM tab WHERE sym LIKE 'A%' AND price > 0", ctx)) {
                        Assert.fail("expected isolated adaptive-wrap failure");
                    } catch (RuntimeException e) {
                        TestUtils.assertContains(e.getMessage(), "test adaptive symbol pattern wrap failure");
                    }
                }
            } finally {
                FullPartitionFrameCursorFactory.clearCloseObserverForTesting();
            }
            Assert.assertEquals(1, partitionFactoryCloseCount[0]);
        });
    }

    private static boolean containsFactory(RecordCursorFactory factory, Class<?> factoryClass) {
        while (factory != null) {
            if (factoryClass.isInstance(factory)) {
                return true;
            }
            factory = factory.getBaseFactory();
        }
        return false;
    }
}
