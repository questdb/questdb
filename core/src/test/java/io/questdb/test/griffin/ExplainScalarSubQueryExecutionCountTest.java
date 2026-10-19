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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * EXPLAIN of a query whose designated-timestamp bound comes from a scalar sub-query
 * (for example {@code ts > (SELECT ...)}) must not execute the sub-query more times than a
 * single normal evaluation would. Historically EXPLAIN opened the base data cursor (which
 * computed the dynamic intervals once) and then re-computed the same intervals while rendering
 * the plan, executing the scalar sub-query twice (four times for a BETWEEN with two dynamic
 * bounds). This test counts the sub-query filter initializations to guard against that redundant
 * I/O without asserting any change to the rendered plan text.
 */
public class ExplainScalarSubQueryExecutionCountTest extends AbstractCairoTest {

    @Test
    public void testBetweenBoundsExplainMatchesSingleEvaluation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT timestamp_sequence(0, 2_500_000) ts FROM long_sequence(5)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE bounds AS (" +
                    "SELECT timestamp_sequence(0, 1_000_000) b FROM long_sequence(3)" +
                    ") TIMESTAMP(b) PARTITION BY DAY");

            final String query = "SELECT * FROM x WHERE ts BETWEEN " +
                    "(SELECT min(b) FROM bounds WHERE test_match()) AND " +
                    "(SELECT max(b) FROM bounds WHERE test_match())";

            final int normalInits = countSubQueryInits(query);
            final int explainInits = countSubQueryInits("EXPLAIN " + query);

            Assert.assertTrue("expected the sub-query to be evaluated at least once", normalInits > 0);
            Assert.assertEquals(
                    "EXPLAIN must evaluate the two dynamic BETWEEN bounds the same number of times as a single query evaluation",
                    normalInits,
                    explainInits
            );
        });
    }

    @Test
    public void testSingleBoundExplainMatchesSingleEvaluation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT timestamp_sequence(0, 2_500_000) ts FROM long_sequence(5)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE bounds AS (" +
                    "SELECT timestamp_sequence(0, 1_000_000) b FROM long_sequence(3)" +
                    ") TIMESTAMP(b) PARTITION BY DAY");

            final String query = "SELECT * FROM x WHERE ts > (SELECT max(b) FROM bounds WHERE test_match())";

            final int normalInits = countSubQueryInits(query);
            final int explainInits = countSubQueryInits("EXPLAIN " + query);

            Assert.assertTrue("expected the sub-query to be evaluated at least once", normalInits > 0);
            Assert.assertEquals(
                    "EXPLAIN must evaluate the dynamic bound the same number of times as a single query evaluation",
                    normalInits,
                    explainInits
            );
        });
    }

    private static int countSubQueryInits(String sql) throws Exception {
        TestMatchFunctionFactory.clear();
        try (RecordCursorFactory factory = select(sql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // drain the cursor so every stage runs; for EXPLAIN this walks the whole plan
                }
            }
        }
        return TestMatchFunctionFactory.getOpenCounter();
    }
}
