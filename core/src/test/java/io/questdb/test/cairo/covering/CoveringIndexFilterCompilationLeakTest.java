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
}
