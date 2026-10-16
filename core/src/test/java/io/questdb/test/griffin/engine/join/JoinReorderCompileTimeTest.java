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

package io.questdb.test.griffin.engine.join;

import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every non-equi RIGHT/FULL OUTER join pins all models before it, so a chain of such joins gives the
 * level a quadratic number of ordering edges. reorderTables asks isJoinedAfter about the INNER joins
 * behind each CROSS join once per CROSS join of the level, and isJoinedAfter used to rescan every
 * ordering edge at every model it visited. Compile time grew as the fifth power of the table count:
 * about 4 s for 160 tables and 2 minutes for 319, and no query timeout could stop it.
 */
public class JoinReorderCompileTimeTest extends AbstractCairoTest {

    @Test
    public void testInnerJoinKeyedToCrossJoinBehindNonEquiFullJoins() throws Exception {
        // t0 CROSS JOIN t1 JOIN t2 ON t2.k = t1.k FULL JOIN t3 ON t3.k > t2.k CROSS JOIN t4 ...
        assertCompilesInBoundedTime(80, 3, "FULL", (sb, a) -> sb
                .append(" CROSS JOIN t").append(a)
                .append(" JOIN t").append(a + 1).append(" ON t").append(a + 1).append(".k = t").append(a).append(".k")
                .append(" FULL JOIN t").append(a + 2).append(" ON t").append(a + 2).append(".k > t").append(a + 1).append(".k"));
    }

    @Test
    public void testInnerJoinKeyedToTwoTablesBehindNonEquiRightJoins() throws Exception {
        // t0 CROSS JOIN t1 JOIN t2 ON t2.k = t1.k JOIN t3 ON t3.k = t1.k AND t3.j = t2.j RIGHT JOIN t4 ON t4.k > t3.k ...
        // t3 follows t1 through t2, so isJoinedAfter keeps the key t3.k = t1.k on t3 and reorderTables
        // asks again for every CROSS join of the level.
        assertCompilesInBoundedTime(60, 4, "RIGHT", (sb, a) -> sb
                .append(" CROSS JOIN t").append(a)
                .append(" JOIN t").append(a + 1).append(" ON t").append(a + 1).append(".k = t").append(a).append(".k")
                .append(" JOIN t").append(a + 2).append(" ON t").append(a + 2).append(".k = t").append(a).append(".k AND t")
                .append(a + 2).append(".j = t").append(a + 1).append(".j")
                .append(" RIGHT JOIN t").append(a + 3).append(" ON t").append(a + 3).append(".k > t").append(a + 2).append(".k"));
    }

    private static String chain(int blocks, int blockSize, BlockAppender appender) {
        final StringBuilder sb = new StringBuilder("SELECT count(*) FROM t0");
        for (int b = 0; b < blocks; b++) {
            appender.append(sb, b * blockSize + 1);
        }
        return sb.toString();
    }

    private void assertCompilesInBoundedTime(int blocks, int blockSize, String outerJoin, BlockAppender appender) throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0, n = 1 + blocks * blockSize; i < n; i++) {
                execute("CREATE TABLE t" + i + " (k INT, j INT)");
            }
            // warm up so the measurement excludes first-compile and class-loading noise
            final String small = chain(3, blockSize, appender);
            for (int i = 0; i < 3; i++) {
                Misc.free(select(small));
            }
            final String sql = chain(blocks, blockSize, appender);
            Assert.assertTrue(sql.contains(outerJoin + " JOIN"));
            final long start = System.nanoTime();
            Misc.free(select(sql));
            final long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            // generous: the point is a bounded compile time, not a precise timing pin
            Assert.assertTrue((1 + blocks * blockSize) + "-table compile took " + elapsedMs + "ms, expected bounded", elapsedMs < 5_000);
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @FunctionalInterface
    private interface BlockAppender {
        void append(StringBuilder sb, int firstTableIndex);
    }
}
