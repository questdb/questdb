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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
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
 * <p>
 * The tests count the ordering edges that isJoinedAfter reads, which grow as the cube of the chain
 * length: a model at position p has O(p) in-edges, so one search reads O(V^2) edges, and a pass asks
 * about O(V) distinct pairs. Losing the answers memo repeats every search once per CROSS join, and
 * losing the edge index reads all O(V^2) edges at each visited model; either makes the count grow as
 * the fourth power. Doubling the chain multiplies the count by 8 in the first case and by 16 in the
 * second, so the tests require a factor below 12.
 */
public class JoinReorderCompileTimeTest extends AbstractCairoTest {

    @Test
    public void testInnerJoinKeyedToCrossJoinBehindNonEquiFullJoins() throws Exception {
        // t0 CROSS JOIN t1 JOIN t2 ON t2.k = t1.k FULL JOIN t3 ON t3.k > t2.k CROSS JOIN t4 ...
        assertEdgeReadsGrowAsCube(80, 3, "FULL", (sb, a) -> sb
                .append(" CROSS JOIN t").append(a)
                .append(" JOIN t").append(a + 1).append(" ON t").append(a + 1).append(".k = t").append(a).append(".k")
                .append(" FULL JOIN t").append(a + 2).append(" ON t").append(a + 2).append(".k > t").append(a + 1).append(".k"));
    }

    @Test
    public void testInnerJoinKeyedToTwoTablesBehindNonEquiRightJoins() throws Exception {
        // t0 CROSS JOIN t1 JOIN t2 ON t2.k = t1.k JOIN t3 ON t3.k = t1.k AND t3.j = t2.j RIGHT JOIN t4 ON t4.k > t3.k ...
        // t3 follows t1 through t2, so isJoinedAfter keeps the key t3.k = t1.k on t3 and reorderTables
        // asks again for every CROSS join of the level.
        assertEdgeReadsGrowAsCube(60, 4, "RIGHT", (sb, a) -> sb
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

    private void assertEdgeReadsGrowAsCube(int blocks, int blockSize, String outerJoin, BlockAppender appender) throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0, n = 1 + blocks * blockSize; i < n; i++) {
                execute("CREATE TABLE t" + i + " (k INT, j INT)");
            }
            final String smallSql = chain(blocks / 2, blockSize, appender);
            final String largeSql = chain(blocks, blockSize, appender);
            Assert.assertTrue(largeSql.contains(outerJoin + " JOIN"));
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final long smallReads = compileAndCountEdgeReads(compiler, smallSql);
                final long largeReads = compileAndCountEdgeReads(compiler, largeSql);
                Assert.assertTrue("the chain must reach isJoinedAfter, edge reads: " + smallReads, smallReads > 0);
                Assert.assertTrue(
                        "edge reads must grow as the cube of the chain length, "
                                + (1 + blocks / 2 * blockSize) + " tables: " + smallReads + ", "
                                + (1 + blocks * blockSize) + " tables: " + largeReads,
                        largeReads < 12 * smallReads
                );
            }
            assertQuery(largeSql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    private long compileAndCountEdgeReads(SqlCompilerImpl compiler, String sql) throws Exception {
        final long readsBefore = compiler.getKeyMoveEdgeReadsForTesting();
        final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
        Misc.free(factory);
        return compiler.getKeyMoveEdgeReadsForTesting() - readsBefore;
    }

    @FunctionalInterface
    private interface BlockAppender {
        void append(StringBuilder sb, int firstTableIndex);
    }
}
