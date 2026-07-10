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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.test.TestThrowingFilterFunctionFactory;
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
    public void testPatternCoveringPathFreesResidualExactlyOnceOnThrow() throws Exception {
        // Coverage for the NEW symbol-pattern COVERING route (sym LIKE '...'), the analogue of the sym='A'
        // case above but reached through tryGenerateSymbolPatternIndex's covering branch. That branch
        // transfers dfcFactory + providerFunction into coveringFactory and nulls its OWN locals, then calls
        // wrapCoveringWithFilter, which here throws on the 3rd per-worker filter compile. Its catch frees
        // coveringFactory (-> dfcFactory) and the residual filter. This asserts the residual + partial worker
        // filter are each freed EXACTLY once (CLOSE_COUNT == 2) with no native leak under assertMemoryLeak.
        //
        // NOTE on the C-A "double-free" finding: before the SqlCodeGenerator fix, the CALLER's dfcFactory
        // stayed non-null after this method threw, so generateTableQuery0's outer catch ran
        // Misc.free(dfcFactory) a SECOND time. That is a genuine ownership-invariant violation (free should
        // happen exactly once), and the fix closes it. It is NOT, however, an observable fault today: every
        // resource in the dfcFactory close chain nulls-on-free (IntervalPartitionFrameCursorFactory does
        // `cursor = Misc.free(cursor)`; RuntimeIntervalModel and AbstractPartitionFrameCursorFactory free
        // their lists via Misc.freeObjList, which nulls each entry), so the second close() is a no-op and
        // assertMemoryLeak stays balanced. This test therefore verifies the throw path stays leak-free on the
        // pattern route; the fix is a defensive correctness change guarding against a future non-idempotent
        // close in this factory hierarchy, verified separately by the ownership trace in SqlCodeGenerator.
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
                            "SELECT dateadd('h', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A' || (x % 5), x::DOUBLE " +
                            "FROM long_sequence(20)"
            );
            engine.releaseAllWriters();

            TestThrowingFilterFunctionFactory.reset(3);

            // No LIMIT: a multi-key pattern with a negative limit takes the serial fallback (no per-worker
            // filter compile); without a limit the covering route drives the async page-frame path, so
            // wrapCoveringWithFilter compiles per-worker copies and the 3rd throws. The dynamic interval
            // (ts > now()-based) exercises the interval-factory dfcFactory shape on this route.
            try (
                    SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, 4);
                    RecordCursorFactory ignored = engine.select(
                            "SELECT price FROM tab WHERE sym LIKE 'A%' AND test_throwing_filter() AND ts > dateadd('y', -10, now())", ctx)
            ) {
                Assert.fail("expected SqlException from test_throwing_filter");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "configured to throw on call 3");
            }
            // The residual + the partial worker filter close exactly once each; assertMemoryLeak confirms no
            // native imbalance on the pattern-covering throw path.
            Assert.assertEquals(2, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
        });
    }
}
