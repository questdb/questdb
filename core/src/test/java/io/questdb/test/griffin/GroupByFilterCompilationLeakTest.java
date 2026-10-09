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
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.test.TestThrowingFilterFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GroupByFilterCompilationLeakTest extends AbstractCairoTest {
    @Override
    public void setUp() {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testKeyedWorkerFilterFailureKeepsOriginalFilterOwned() throws Exception {
        assertWorkerFilterFailure(
                "SELECT x%2 k, sum(x) s FROM tab WHERE test_throwing_filter() ORDER BY k",
                "k\ts\n0\t2550\n1\t2500\n"
        );
    }

    @Test
    public void testNotKeyedWorkerFilterFailureKeepsOriginalFilterOwned() throws Exception {
        assertWorkerFilterFailure(
                "SELECT sum(x) s FROM tab WHERE test_throwing_filter()",
                "s\n5050\n"
        );
    }

    private void assertWorkerFilterFailure(String query, String expected) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x, x::timestamp ts FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, 4);
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine)
            ) {
                // The owner filter and the first worker clone succeed; the second clone the GROUP BY prepares
                // for the filter it steals fails.
                TestThrowingFilterFunctionFactory.reset(3);
                try {
                    try (RecordCursorFactory ignored = compiler.compile(query, ctx).getRecordCursorFactory()) {
                        Assert.fail("expected worker filter compilation to fail");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "configured to throw on call 3");
                    }
                    Assert.assertEquals(3, TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get());
                    Assert.assertEquals(2, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
                } finally {
                    TestThrowingFilterFunctionFactory.reset(-1);
                }
                compiler.clear();
                try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(ctx).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
                }
                Assert.assertEquals(TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get(), TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
            }
        });
    }
}
