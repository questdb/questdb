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

import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A sub-query or view whose computed columns no consumer reads leaves a projection of plain columns, which plans as the
 * table it reads. Each query compares with the query over the table, which computes nothing.
 */
public class ProjectionPruningTest extends AbstractCairoTest {

    @Test
    public void testReadComputedColumnKeepsItsProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[][] cases = {
                    {"SELECT a, c FROM v WHERE a > 5", "SELECT a, a + b c FROM t WHERE a > 5"},
                    {"SELECT c FROM (SELECT a, a + 1 c FROM t) LIMIT 3", "SELECT a + 1 c FROM t LIMIT 3"},
                    {"SELECT m.a, m.c, n.a FROM (SELECT a, ts, a + 1 c FROM t) m ASOF JOIN (SELECT a, ts FROM t) n", "SELECT m.a, m.a + 1 c, n.a FROM t m ASOF JOIN t n"}
            };
            for (String[] c : cases) {
                assertSqlCursors(c[1], c[0]);
                Assert.assertTrue(c[0], Chars.contains(plan(c[0]), "VirtualRecord"));
            }
        });
    }

    @Test
    public void testUnreadComputedColumnLeavesTheTable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[][] cases = {
                    {"SELECT a FROM v WHERE a > 5", "SELECT a FROM t WHERE a > 5", "Async"},
                    {"SELECT a FROM (SELECT a, a + 1 c FROM t) ORDER BY a DESC LIMIT 3", "SELECT a FROM t ORDER BY a DESC LIMIT 3", "Async Top K"},
                    {"SELECT a, b FROM (SELECT a, b, a + b c, ts FROM t) LIMIT 3", "SELECT a, b FROM t LIMIT 3", "Frame forward scan on: t"},
                    {"SELECT sum(a) FROM v", "SELECT sum(a) FROM t", "vectorized: true"},
                    {"SELECT a FROM (SELECT * FROM (SELECT a, a + b c FROM t)) WHERE a < 9", "SELECT a FROM t WHERE a < 9", "Async"},
                    {"SELECT m.a, n.a FROM (SELECT a, ts, a + 1 c FROM t) m ASOF JOIN (SELECT a, ts, a + 1 c FROM t) n", "SELECT m.a, n.a FROM t m ASOF JOIN t n", "AsOf Join Fast"},
                    {
                            """
                            SELECT m.ts, sum(p.x) FROM (SELECT ts, s, a + 1 c FROM t) m WINDOW JOIN p ON (m.s = p.s)
                            RANGE BETWEEN 1 SECOND PRECEDING AND 1 SECOND FOLLOWING EXCLUDE PREVAILING
                            """,
                            """
                            SELECT m.ts, sum(p.x) FROM t m WINDOW JOIN p ON (m.s = p.s)
                            RANGE BETWEEN 1 SECOND PRECEDING AND 1 SECOND FOLLOWING EXCLUDE PREVAILING
                            """,
                            "Async Window Fast Join"
                    }
            };
            for (String[] c : cases) {
                assertSqlCursors(c[1], c[0]);
                final String plan = plan(c[0]);
                Assert.assertFalse(plan, Chars.contains(plan, "VirtualRecord"));
                Assert.assertTrue(plan, Chars.contains(plan, c[2]));
                Assert.assertEquals(plan(c[1]), plan);
            }
            // SAMPLE BY over a sub-query runs serially, unlike over a table, and still reads the table directly
            final String sampleBy = "SELECT ts, sum(a) FROM v SAMPLE BY 1h";
            assertSqlCursors("SELECT ts, sum(a) FROM t SAMPLE BY 1h", sampleBy);
            Assert.assertFalse(plan(sampleBy), Chars.contains(plan(sampleBy), "VirtualRecord"));
        });
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT x a, x * 2 b, rnd_symbol('x', 'y') s, timestamp_sequence(0, 1_000_000) ts FROM long_sequence(1_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("""
                CREATE TABLE p AS (
                    SELECT x::DOUBLE x, rnd_symbol('x', 'y') s, timestamp_sequence(0, 500_000) ts FROM long_sequence(1_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE VIEW v AS (SELECT a, s, ts, a + b c FROM t)");
    }

    private static String plan(String sql) throws Exception {
        final StringSink sink = new StringSink();
        printSql("EXPLAIN " + sql, sink);
        return sink.toString();
    }
}
