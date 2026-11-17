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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LATERAL sub-queries whose bodies join relations that share column names. The rewrite of a LATERAL join moves
 * the conditions and the expressions of the body that read the outer row into the join and its projection, and
 * those then read the columns of the body through its projection: a wildcard renames the columns that share a
 * name, a qualified wildcard passes on the columns of one relation alone, and the layers above the join pass on
 * what the layers below them do. Each case checks the rows and the column names against the body run once per
 * outer row, which is what LATERAL means, and the plans pin the bodies that already pass the columns on.
 */
public class LateralJoinSharedColumnNameTest extends AbstractCairoTest {
    // The bodies of testBodyMatchesPerOuterRowReference(), with the outer columns o.k and o.m in place of {k}
    // and {m}. The tables ta, tb and tc have the same columns with values of their own, and te shares two.
    private static final String[] BODIES = {
            // a condition next to the WHERE key equality reads the column of the second table
            "SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50",
            "SELECT a.* FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50",
            "SELECT a.* FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50 AND b.id < {k} + 60",
            "SELECT b.* FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND a.v > {k} * 100",
            "SELECT b.* FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50",
            "SELECT a.id, b.y FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50",
            "SELECT *, b.id FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50",
            "SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id - a.id > {k} + 49",
            "SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND coalesce(b.id, a.id, 0) > {k} + 50",
            "SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.y = {k}",
            "SELECT * FROM ta a LEFT JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50",
            "SELECT * FROM ta a JOIN tb b ON b.x = a.x JOIN tc c ON c.x = a.x WHERE a.x = {k} AND c.id > {k} + 70",
            "SELECT * FROM ta JOIN tb ON tb.x = ta.x WHERE ta.x = {k} AND tb.id > {k} + 50",
            "SELECT * FROM ta A JOIN tb B ON B.X = A.X WHERE A.X = {k} AND B.ID > {k} + 50",
            "SELECT * FROM ta l JOIN tb b ON b.x = l.x WHERE l.x = {k} AND b.id > {k} + 50",
            "SELECT * FROM (SELECT * FROM ta) a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50",
            "SELECT * FROM ta a JOIN (SELECT * FROM tb) b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50",
            "SELECT * FROM ta a JOIN te e ON e.x = a.x WHERE a.x = {k} AND w > {k} + 50",
            "SELECT a.id AS v, b.v AS id FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50",
            // a condition reads an outer column that no WHERE equality equates
            "SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m}",
            "SELECT a.* FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m}",
            "SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k} AND a.v > {m}",
            "SELECT a.*, b.y FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k} AND a.v > {m}",
            "SELECT * FROM ta a LEFT JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m}",
            "SELECT b.*, c.id cid FROM ta a JOIN tb b ON b.x = a.x JOIN tc c ON c.x = a.x WHERE a.x = {k} AND c.v > {m}",
            "SELECT * FROM ta a JOIN (SELECT id, x, v FROM tb) b ON b.x = a.x WHERE a.x = {k} AND b.v > {m}",
            "SELECT a.* FROM ta a JOIN te e ON e.x = a.x WHERE a.x = {k} AND w > {m}",
            "SELECT a.id, b.id bid FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND CASE WHEN b.v > {m} THEN b.id ELSE a.id END > 50",
            // an expression of the projection reads the outer row
            "SELECT a.id, b.id + {m} s FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m}",
            "SELECT *, b.v + {m} s FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m}",
            "SELECT b.id + {m} s FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m}",
            "SELECT b.*, a.v + {m} s FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m}",
            "SELECT a.id, CASE WHEN b.v > {m} THEN b.id ELSE 0 END s FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND a.v > {m}",
            // the join sits under layers that pass its columns on
            "SELECT * FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k})",
            "SELECT {k} ok, * FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k})",
            "SELECT * FROM (SELECT * FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k}))",
            "SELECT * FROM (SELECT * FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m}))",
            "SELECT id1, x FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k})",
            "SELECT * FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x) WHERE x = {k} AND id1 > {k} + 50",
            "SELECT y, count(*) c FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k}) GROUP BY y",
            "SELECT y, count(*) c FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.id > {k} + 50) GROUP BY y",
            "SELECT * FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k}) s JOIN tc c ON c.x = s.x",
            "SELECT s.id1, c.id cid FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k}) s JOIN tc c ON c.x = s.x",
            "SELECT * FROM (SELECT * FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k}) s JOIN tc c ON c.x = s.x)",
            // a layer in the middle selects other columns, or reads a column that the layer below computes
            "SELECT * FROM (SELECT id FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k}))",
            "SELECT * FROM (SELECT * FROM (SELECT a.id, b.v FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k}))",
            "SELECT * FROM (SELECT * FROM (SELECT b.y * 1 yy, * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = {k}))",
            // the conditions move into the lateral join through layers that do not pass their columns on
            "SELECT * FROM (SELECT a.id FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m})",
            "SELECT id FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m})",
            "SELECT id1 + {m} s FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m})",
            // a correlated sub-query that the body joins
            "SELECT a.id, bb.id bid FROM ta a JOIN (SELECT * FROM tb b WHERE b.y = {k}) bb ON bb.x = a.x",
            // a LIMIT turns into a row number filter over the join
            "SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} ORDER BY b.id DESC, a.id DESC LIMIT 1",
            "SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = {k} AND b.v > {m} ORDER BY b.id DESC, a.id DESC LIMIT 1",
    };
    // the outer rows: id, k, m
    private static final int[][] OUTER = {{1, 1, 10}, {2, 2, 20}, {3, 3, 55}};

    @Test
    public void testBodyMatchesPerOuterRowReference() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final List<String> failures = new ArrayList<>();
            for (String body : BODIES) {
                assertBody(body, "JOIN", failures);
                assertBody(body, "LEFT JOIN", failures);
            }
            if (!failures.isEmpty()) {
                Assert.fail(failures.size() + " bodies differ from the reference:\n" + String.join("\n", failures));
            }
        });
    }

    @Test
    public void testColumnsNamedLikeBindingPlaceholders() throws Exception {
        // the rewrite names the columns that it lifts out of a body with placeholders of this prefix until it
        // knows their names; columns of the query that start with it keep their meaning
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE tn (id INT, x INT, __qdb_lateral_bind__ INT, __qdb_lateral_bind__x INT, __qdb_lateral_bind__99 INT,"
                    + " __qdb_lateral_bin_long_name INT)");
            execute("INSERT INTO tn VALUES (1, 1, 7, 8, 9, 10), (2, 2, 17, 18, 19, 20)");
            assertQuery("SELECT o.id, l.__qdb_lateral_bind__, l.__qdb_lateral_bind__x, l.__qdb_lateral_bind__99, l.__qdb_lateral_bin_long_name"
                    + " FROM o JOIN LATERAL (SELECT * FROM tn n JOIN tb b ON b.x = n.x WHERE n.x = o.k AND b.id > o.k + 50) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            id\t__qdb_lateral_bind__\t__qdb_lateral_bind__x\t__qdb_lateral_bind__99\t__qdb_lateral_bin_long_name
                            1\t7\t8\t9\t10
                            """);
        });
    }

    @Test
    public void testConditionOnTimestampOfJoinedTable() throws Exception {
        // trades and quotes share sym, venue and ts; the condition reads the ts of the quote, which the
        // wildcard of the body names ts1
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ot (id INT, s SYMBOL, ts TIMESTAMP)");
            execute("CREATE TABLE tr (sym SYMBOL, px DOUBLE, venue VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE qt (sym SYMBOL, bid DOUBLE, venue VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ot VALUES (1, 'A', '2024-01-01T00:00:01.000000Z'), (2, 'B', '2024-01-01T00:00:00.000000Z'),"
                    + " (3, 'C', '2024-01-01T00:00:05.000000Z')");
            execute("INSERT INTO tr VALUES ('A', 10.0, 'X', '2024-01-01T00:00:00.000000Z'), ('B', 20.0, 'Y', '2024-01-01T00:00:01.000000Z'),"
                    + " ('A', 11.0, 'Z', '2024-01-01T00:00:05.000000Z'), ('C', 30.0, 'X', '2024-01-01T00:00:06.000000Z')");
            execute("INSERT INTO qt VALUES ('A', 9.5, 'X', '2024-01-01T00:00:02.000000Z'), ('B', 19.5, 'Y', '2024-01-01T00:00:00.500000Z'),"
                    + " ('A', 10.5, 'Q', '2024-01-01T00:00:04.000000Z'), ('C', 29.5, 'X', '2024-01-01T00:00:03.000000Z')");
            final String body = "(SELECT * FROM tr t JOIN qt q ON q.sym = t.sym WHERE t.sym = o.s AND q.ts > o.ts) l ORDER BY 1, 5, 9";
            final String matched = """
                    id\tsym\tpx\tvenue\tts\tsym1\tbid\tvenue1\tts1
                    1\tA\t10.0\tX\t2024-01-01T00:00:00.000000Z\tA\t9.5\tX\t2024-01-01T00:00:02.000000Z
                    1\tA\t10.0\tX\t2024-01-01T00:00:00.000000Z\tA\t10.5\tQ\t2024-01-01T00:00:04.000000Z
                    1\tA\t11.0\tZ\t2024-01-01T00:00:05.000000Z\tA\t9.5\tX\t2024-01-01T00:00:02.000000Z
                    1\tA\t11.0\tZ\t2024-01-01T00:00:05.000000Z\tA\t10.5\tQ\t2024-01-01T00:00:04.000000Z
                    2\tB\t20.0\tY\t2024-01-01T00:00:01.000000Z\tB\t19.5\tY\t2024-01-01T00:00:00.500000Z
                    """;
            assertQuery("SELECT o.id, l.* FROM ot o JOIN LATERAL " + body)
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: o.ts<l.ts1")
                    .returns(matched);
            // no quote of C follows the order of outer row 3
            assertQuery("SELECT o.id, l.* FROM ot o LEFT JOIN LATERAL " + body)
                    .noLeakCheck()
                    .returns(matched + "3\t\tnull\t\t\t\tnull\t\t\n");
        });
    }

    @Test
    public void testCorrelatedSubQueryJoinedWithTable() throws Exception {
        // the key of the lateral join passes through a FROM clause that joins the correlated sub-query
        // with a table that shares its column names
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT o.id, l.id aid, l.id1 bid, l.id2 cid FROM o JOIN LATERAL"
                    + " (SELECT * FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = o.k) s JOIN tc c ON c.x = s.x) l"
                    + " ORDER BY 1, 2, 3, 4")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t1\t51\t71
                            1\t1\t51\t74
                            1\t3\t51\t71
                            1\t3\t51\t74
                            2\t1\t54\t71
                            2\t1\t54\t74
                            2\t2\t52\t72
                            2\t3\t54\t71
                            2\t3\t54\t74
                            3\t4\t53\t73
                            """);
            assertQuery("SELECT o.id, l.* FROM o JOIN LATERAL"
                    + " (SELECT s.id1, c.id cid FROM (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE b.y = o.k) s JOIN tc c ON c.x = s.x) l"
                    + " ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tid1\tcid
                            1\t51\t71
                            1\t51\t71
                            1\t51\t74
                            1\t51\t74
                            2\t52\t72
                            2\t54\t71
                            2\t54\t71
                            2\t54\t74
                            2\t54\t74
                            3\t53\t73
                            """);
        });
    }

    @Test
    public void testErrorsPointAtColumnOfBody() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the body projects id, which both tables have
            final String ambiguous = "SELECT o.id, l.* FROM o JOIN LATERAL"
                    + " (SELECT id FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = o.k AND b.id > o.k) l";
            assertException(ambiguous, ambiguous.indexOf("SELECT id") + "SELECT ".length(), "Ambiguous column [name=id]");
            // the condition next to the key equality reads a column that tb does not have
            final String unknown = "SELECT o.id, l.* FROM o JOIN LATERAL"
                    + " (SELECT * FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = o.k AND b.w > o.k) l";
            assertException(unknown, unknown.indexOf("b.w"), "Invalid column: b.w");
        });
    }

    @Test
    public void testLimitBodyHidesRowNumber() throws Exception {
        // the LIMIT of the body becomes a filter on a row number, which l.* must not return
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT o.id oid, l.* FROM o JOIN LATERAL (SELECT * FROM ta a JOIN tb b ON b.x = a.x"
                    + " WHERE a.x = o.k ORDER BY b.id DESC, a.id DESC LIMIT 1) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            oid\tid\tx\ty\tv\tid1\tx1\ty1\tv1
                            1\t3\t1\t9\t300\t54\t1\t2\t45
                            2\t2\t2\t8\t200\t52\t2\t2\t25
                            3\t4\t3\t3\t400\t53\t3\t3\t35
                            """);
            assertQuery("SELECT o.id oid, l.* FROM o JOIN LATERAL (SELECT * FROM ta a JOIN tb b ON b.x = a.x"
                    + " WHERE a.x = o.k AND b.v > o.m ORDER BY b.id DESC, a.id DESC LIMIT 1) l ORDER BY 1")
                    .noLeakCheck()
                    .returns("""
                            oid\tid\tx\ty\tv\tid1\tx1\ty1\tv1
                            1\t3\t1\t9\t300\t54\t1\t2\t45
                            2\t2\t2\t8\t200\t52\t2\t2\t25
                            """);
        });
    }

    @Test
    public void testOrderByLiftedExpression() throws Exception {
        // ORDER BY of the outer query reads the expression of the body that reads o.m, which the rewrite
        // computes in the outer query
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT o.id, l.s FROM o JOIN LATERAL"
                    + " (SELECT b.id + o.m s FROM ta a JOIN tb b ON b.x = a.x WHERE a.x = o.k AND b.v > o.m) l ORDER BY l.s DESC, o.id")
                    .noLeakCheck()
                    .returns("""
                            id\ts
                            2\t72
                            1\t64
                            1\t64
                            1\t61
                            1\t61
                            """);
        });
    }

    @Test
    public void testQualifiedWildcardBodyKeepsColumnNames() throws Exception {
        // the condition reads b.id, which a.* does not pass on; the body must still return a.id as id
        assertMemoryLeak(() -> {
            createClaimTables();
            final String body = "(SELECT a.* FROM wa a JOIN wb b ON b.x = a.x WHERE a.x = o.k AND b.id > o.k) l";
            assertQuery("SELECT o.id, l.id FROM wo o JOIN LATERAL " + body + " ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            id\tid1
                            1\t1
                            1\t100
                            2\t2
                            """);
            assertQuery("SELECT o.id oid, l.* FROM wo o JOIN LATERAL " + body + " ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            oid\tid\tx
                            1\t1\t1
                            1\t100\t1
                            2\t2\t2
                            """);
        });
    }

    @Test
    public void testSingleTableBodyPlansKeepTableScan() throws Exception {
        // the projection of the body passes the columns of the conditions on under their own names, so the
        // build side of the hash join reads the table without a projection of its own
        assertMemoryLeak(() -> {
            execute("CREATE TABLE po (id INT, sym SYMBOL, ts TIMESTAMP, lim DOUBLE)");
            execute("CREATE TABLE pt (sym SYMBOL, px DOUBLE, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String plan = """
                    SelectedRecord
                        Filter filter: o.ts<l.ts
                            Hash Join Light
                              condition: l.sym=o.sym
                              symbolKeyJoin: true
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: po
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: pt
                    """;
            assertQuery("SELECT o.id, l.* FROM po o JOIN LATERAL (SELECT * FROM pt t WHERE t.sym = o.sym AND t.ts > o.ts) l")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("SELECT o.id, l.* FROM po o JOIN LATERAL (SELECT t.px, t.qty FROM pt t WHERE t.sym = o.sym AND t.ts > o.ts) l")
                    .noLeakCheck()
                    .assertsPlan(plan);
        });
    }

    @Test
    public void testWildcardBodyFiltersColumnOfSecondTable() throws Exception {
        // the condition reads b.id, which the wildcard of the body names id1, while id is a.id
        assertMemoryLeak(() -> {
            createClaimTables();
            // b.id > o.k holds for every joined row
            assertQuery("SELECT o.id oid, l.* FROM wo o JOIN LATERAL"
                    + " (SELECT * FROM wa a JOIN wb b ON b.x = a.x WHERE a.x = o.k AND b.id > o.k) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: o.k<l.id1")
                    .returns("""
                            oid\tid\tx\tid1\tx1
                            1\t1\t1\t50\t1
                            1\t100\t1\t50\t1
                            2\t2\t2\t60\t2
                            """);
            // b.id > o.k + 49 holds for the row of outer row 2 alone
            assertQuery("SELECT o.id oid, l.* FROM wo o JOIN LATERAL"
                    + " (SELECT * FROM wa a JOIN wb b ON b.x = a.x WHERE a.x = o.k AND b.id > o.k + 49) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            oid\tid\tx\tid1\tx1
                            2\t2\t2\t60\t2
                            """);
            assertQuery("SELECT o.id oid, l.* FROM wo o LEFT JOIN LATERAL"
                    + " (SELECT * FROM wa a JOIN wb b ON b.x = a.x WHERE a.x = o.k AND b.id > o.k + 49) l ORDER BY 1, 2")
                    .noLeakCheck()
                    .returns("""
                            oid\tid\tx\tid1\tx1
                            1\tnull\tnull\tnull\tnull
                            2\t2\t2\t60\t2
                            """);
        });
    }

    // Returns the column names of the result, and adds its rows to the list
    private static String readRows(CharSequence sql, List<String> rows) throws SqlException {
        sink.clear();
        printSql(sql);
        final String[] lines = sink.toString().split("\n");
        for (int i = 1; i < lines.length; i++) {
            rows.add(lines[i]);
        }
        return lines[0];
    }

    // Compares the rows and the column names of the lateral join over the body with those of the body run
    // once per outer row, with the outer columns as literals, and the outer row NULL-extended by a LEFT join
    // when the body returns no row for it
    private void assertBody(String body, String joinType, List<String> failures) {
        final String lateral = "SELECT o.id oid, l.* FROM o " + joinType + " LATERAL ("
                + body.replace("{k}", "o.k").replace("{m}", "o.m") + ") l";
        try {
            final List<String> expectedRows = new ArrayList<>();
            String expectedHeader = null;
            for (int[] outer : OUTER) {
                final int rowCount = expectedRows.size();
                expectedHeader = readRows(
                        "SELECT " + outer[0] + " oid, l.* FROM ("
                                + body.replace("{k}", Integer.toString(outer[1])).replace("{m}", Integer.toString(outer[2]))
                                + ") l",
                        expectedRows
                );
                if (rowCount == expectedRows.size() && "LEFT JOIN".equals(joinType)) {
                    final StringBuilder nullExtended = new StringBuilder().append(outer[0]);
                    for (int i = 1, n = expectedHeader.split("\t").length; i < n; i++) {
                        nullExtended.append("\tnull");
                    }
                    expectedRows.add(nullExtended.toString());
                }
            }
            final List<String> actualRows = new ArrayList<>();
            final String actualHeader = readRows(lateral, actualRows);
            Collections.sort(expectedRows);
            Collections.sort(actualRows);
            if (!expectedHeader.equals(actualHeader) || !expectedRows.equals(actualRows)) {
                failures.add(lateral + "\n  expected: " + expectedHeader + " " + expectedRows
                        + "\n  actual:   " + actualHeader + " " + actualRows);
            }
        } catch (SqlException e) {
            failures.add(lateral + "\n  failed: " + e.getMessage());
        }
    }

    private void createClaimTables() throws Exception {
        execute("CREATE TABLE wo (id INT, k INT)");
        execute("CREATE TABLE wa (id INT, x INT)");
        execute("CREATE TABLE wb (id INT, x INT)");
        execute("INSERT INTO wo VALUES (1, 1), (2, 2)");
        execute("INSERT INTO wa VALUES (1, 1), (2, 2), (100, 1)");
        execute("INSERT INTO wb VALUES (50, 1), (60, 2)");
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE o (id INT, k INT, m INT)");
        execute("CREATE TABLE ta (id INT, x INT, y INT, v INT)");
        execute("CREATE TABLE tb (id INT, x INT, y INT, v INT)");
        execute("CREATE TABLE tc (id INT, x INT, y INT, v INT)");
        execute("CREATE TABLE te (id INT, x INT, w INT)");
        execute("INSERT INTO o VALUES (1, 1, 10), (2, 2, 20), (3, 3, 55)");
        execute("INSERT INTO ta VALUES (1, 1, 7, 100), (2, 2, 8, 200), (3, 1, 9, 300), (4, 3, 3, 400)");
        execute("INSERT INTO tb VALUES (51, 1, 1, 15), (52, 2, 2, 25), (53, 3, 3, 35), (54, 1, 2, 45)");
        execute("INSERT INTO tc VALUES (71, 1, 5, 5), (72, 2, 6, 65), (73, 3, 7, 75), (74, 1, 8, 85)");
        execute("INSERT INTO te VALUES (61, 1, 61), (62, 2, 12), (63, 3, 63), (64, 1, 14)");
    }
}
