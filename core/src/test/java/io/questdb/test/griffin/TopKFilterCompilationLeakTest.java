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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.test.TestThrowingFilterFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TopKFilterCompilationLeakTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testPerWorkerFilterLeakOnPartialCompileFailure() throws Exception {
        // Forces buildAsyncTopKOverStolenFilter to call compileWorkerFiltersConditionally
        // with sharedQueryWorkerCount > 1. The test filter throws on the Nth construction
        // call; prior calls allocate native memory tracked by assertMemoryLeak.
        //
        // Call 1 builds the original boolean filter on the recordCursorFactory.
        // Calls 2..N build per-worker copies. We configure throwOnCall=3 so call 2
        // succeeds (one worker filter constructed and held in the local list), then
        // call 3 throws. Without the try/catch in compileWorkerFiltersConditionally
        // that worker filter would never be closed and its native allocation leaks.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x, x::timestamp ts FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");

            TestThrowingFilterFunctionFactory.reset(3);

            try (
                    SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, 4);
                    RecordCursorFactory ignored = engine.select("SELECT * FROM tab WHERE test_throwing_filter() ORDER BY x LIMIT 10", ctx)
            ) {
                Assert.fail("expected SqlException from test_throwing_filter");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "configured to throw on call 3");
            }

            // 3 constructions total: one for the recordCursorFactory's own filter, one
            // for the first worker copy, one that threw before the instance was created.
            Assert.assertEquals(3, TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get());
            // 2 instances must be closed: the original filter (via outer-catch cascade
            // through filterFactory._close()) and the partial worker filter (via the
            // try/catch in compileWorkerFiltersConditionally). Without the fix this
            // would be 1.
            Assert.assertEquals(2, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
        });
    }
}
