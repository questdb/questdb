/*******************************************************************************
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

import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Several Welford aggregates (stddev, variance, corr) of a covering scan's single-key frames run in one
 * fused pass (WelfordBatchFusion): their results are those of the plain scan's row-for-row aggregation.
 */
public class CoveringWelfordFusionTest extends AbstractCoveringIndexQueryTest {

    @Test
    public void testFusedAggregatesMatchTheScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT"
                    + " timestamp_sequence('2024-01-01', 2000) ts,"
                    + " rnd_symbol('A', 'B', 'C', 'D') sym,"
                    + " CASE WHEN x % 101 = 0 THEN NULL ELSE rnd_double() * 1000 + 140 END a,"
                    + " rnd_float() * 50 b,"
                    + " CASE WHEN x % 97 = 0 THEN NULL ELSE rnd_int(1, 5000, 0) END i"
                    + " FROM long_sequence(400_000)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (a, b, i)");
            final String[] queries = {
                    "SELECT sym, stddev_samp(a) s1, corr(a, b) c1, var_pop(b) v1, stddev_pop(i) s2, corr(i, a) c2 FROM t WHERE sym IN ('A', 'B', 'C', 'D') ORDER BY sym",
                    "SELECT sym, stddev(b) s1, variance(a) v1, sum(a) sa FROM t WHERE sym = 'C' ORDER BY sym",
                    "SELECT sym, corr(a, b) c1, corr(b, i) c2, count() n FROM t WHERE sym IN ('A', 'D') ORDER BY sym",
            };
            for (String q : queries) {
                final StringSink plan = new StringSink();
                printSql("EXPLAIN " + q, plan);
                TestUtils.assertContains(plan, "CoveringIndex");
                assertSameResult(q, q.replace("SELECT ", "SELECT /*+ no_index */ "));
            }
        });
    }
}
