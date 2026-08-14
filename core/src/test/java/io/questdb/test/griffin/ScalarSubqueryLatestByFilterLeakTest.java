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
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestThrowingFilterFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression test for the {@link io.questdb.griffin.WhereClauseParser} leak where a scalar
 * sub-query timestamp bound (e.g. {@code ts > (SELECT ...)}) is transferred into a borrowed
 * {@link io.questdb.griffin.model.IntrinsicModel}, and a subsequent LATEST BY residual-filter
 * compilation throws before {@code buildIntervalModel()} hands ownership downstream. The
 * borrowed model still owns the open scalar-query factory, which the pool's {@code clear()}
 * never freed, so the compiled sub-query factory (and its native allocations) leaked.
 */
public class ScalarSubqueryLatestByFilterLeakTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testLatestByResidualFilterCompileFailureFreesScalarBoundFactory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE events (ts TIMESTAMP, sym SYMBOL INDEX, value INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO events VALUES
                        ('2024-01-01', 'a', 1),
                        ('2024-01-02', 'a', 2),
                        ('2024-01-03', 'a', 3)
                    """);
            execute("CREATE TABLE bounds (lo TIMESTAMP, sel INT) TIMESTAMP(lo) PARTITION BY DAY");
            execute("INSERT INTO bounds VALUES ('2024-01-02', 1)");

            // test_throwing_filter() never throws here (reset(0)); every construction allocates a
            // native buffer freed only on close(). It lives inside the scalar sub-query factory,
            // so its close is a proxy for the scalar-query factory being freed.
            TestThrowingFilterFunctionFactory.reset(0);
            // The outer LATEST BY residual filter (test_fault()) throws on its first compile, after
            // extract() already transferred the scalar-query factory into the borrowed model and
            // before generateLatestByTableQuery() calls buildIntervalModel().
            TestFaultFunctionFactory.armToFailAfterCompiles(0);
            try {
                final String sql = """
                        SELECT ts, sym, value
                        FROM events
                        WHERE ts > (SELECT max(lo) FROM bounds WHERE test_throwing_filter())
                          AND test_fault()
                        LATEST ON ts PARTITION BY sym
                        """;
                try {
                    execute(sql);
                    Assert.fail("expected the injected residual-filter compile failure");
                } catch (Exception expected) {
                    // expected: test_fault injected compile failure
                }

                // The scalar sub-query filter was constructed during extract() and its factory
                // must have been freed on the failed-generation path.
                Assert.assertTrue(
                        "scalar sub-query filter must be constructed",
                        TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get() >= 1
                );
                Assert.assertEquals(
                        "every constructed scalar sub-query filter must be closed (no leaked factory)",
                        TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get(),
                        TestThrowingFilterFunctionFactory.CLOSE_COUNT.get()
                );
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }
}
