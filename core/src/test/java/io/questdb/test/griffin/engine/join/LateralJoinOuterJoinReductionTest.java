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

import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import org.junit.Test;

/**
 * LateralJoinRewriter plans a RIGHT or FULL join of a LATERAL body whose ON clause reads the outer row
 * as an INNER join when a WHERE conjunct, or an ON conjunct of a later INNER join, drops every row that
 * the join NULL-extends. The INNER join reads each side once, where the outer join reads the side that
 * it keeps once per outer key. The tests check the plan of each rule that reduces a join and of each
 * shape that keeps it, and the rows of each query.
 * <p>
 * Per outer row, the RIGHT join of trades and refunds on t.x = r.k AND r.k = o.k returns refund 100
 * with trade 10 for o.k = 1 and refund 101 with trade 11 for o.k = 2, and NULL-extends the other
 * refunds. The NULL o.k of order 3 matches no refund, so the join NULL-extends all three there. Trade
 * 10 holds 5 in c and s, and trade 11 holds -3; a NULL-extended row holds NULL in c, and 0 in the SHORT
 * column s, which has no NULL.
 */
public class LateralJoinOuterJoinReductionTest extends AbstractCairoTest {
    // the rows of the body as written, per outer row
    private static final String ALL_ROWS = """
            id\ttid\trid
            1\tnull\t101
            1\tnull\t102
            1\t10\t100
            2\tnull\t100
            2\tnull\t102
            2\t11\t101
            3\tnull\t100
            3\tnull\t101
            3\tnull\t102
            """;
    private static final String BODY = "trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k";
    private static final String BOTH_TRADES = """
            id\ttid\trid
            1\t10\t100
            2\t11\t101
            """;
    private static final String NO_ROWS = "id\ttid\trid\n";
    // the rows that the RIGHT join NULL-extends, per outer row
    private static final String NULL_EXTENDED_ROWS = """
            id\ttid\trid
            1\tnull\t101
            1\tnull\t102
            2\tnull\t100
            2\tnull\t102
            3\tnull\t100
            3\tnull\t101
            3\tnull\t102
            """;
    private static final String[] OUTER_JOINS = {"Right Outer Join", "Full Outer Join", "Nested Loop Right Join", "Nested Loop Full Join"};
    private static final String TRADE_10 = """
            id\ttid\trid
            1\t10\t100
            """;
    private static final String TRADE_11 = """
            id\ttid\trid
            2\t11\t101
            """;

    // A column that trades does not have proves nothing, and the body fails on it as written.
    @Test
    public void testFailsOnUnknownColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(lateral(BODY + " WHERE t.nosuch > o.k"))
                    .noLeakCheck()
                    .failsWith("Invalid column: t.nosuch");
        });
    }

    // Each RIGHT join reduces only when all of them do. WHERE r.id > 0 drops the rows that the RIGHT
    // join to xs NULL-extends, as r is NULL in them, but not those that the RIGHT join to refunds
    // NULL-extends, so both keep their outer joins.
    @Test
    public void testKeepsBothRightJoinsWhenTheFirstKeepsItsRows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT o.id, l.tid, l.rid, l.v FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, x.v FROM "
                    + BODY + " RIGHT JOIN xs x ON x.k = r.k WHERE r.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort
                              keys: [id, tid, rid]
                                SelectedRecord
                                    Hash Join
                                      condition: l.__qdb_outer_ref__0_k=o.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: orders
                                        Hash
                                            SelectedRecord
                                                Filter filter: 0<r.id
                                                    Hash Right Outer Join
                                                      condition: x.__qdb_outer_ref__2_k=r.__qdb_outer_ref__1_k and x.k=r.k
                                                        Hash Right Outer Join
                                                          condition: r.k=t.x
                                                          filter: r.k=r.__qdb_outer_ref__1_k
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: trades
                                                            Hash
                                                                SelectedRecord
                                                                    Cross Join
                                                                        SelectedRecord
                                                                            GroupBy vectorized: true workers: 1
                                                                              keys: [k]
                                                                              values: [count(*)]
                                                                                PageFrame
                                                                                    Row forward scan
                                                                                    Frame forward scan on: orders
                                                                        PageFrame
                                                                            Row forward scan
                                                                            Frame forward scan on: refunds
                                                        Hash
                                                            SelectedRecord
                                                                Cross Join
                                                                    SelectedRecord
                                                                        (Shared)
                                                                            GroupBy vectorized: true workers: 1
                                                                              keys: [k]
                                                                              values: [count(*)]
                                                                                PageFrame
                                                                                    Row forward scan
                                                                                    Frame forward scan on: orders
                                                                    PageFrame
                                                                        Row forward scan
                                                                        Frame forward scan on: xs
                            """)
                    .returns("""
                            id\ttid\trid\tv
                            1\tnull\t101\t200
                            1\tnull\t102\t300
                            1\t10\t100\t100
                            2\tnull\t100\t100
                            2\tnull\t102\t300
                            2\t11\t101\t200
                            3\tnull\t100\t100
                            3\tnull\t101\t200
                            3\tnull\t102\t300
                            """);
        });
    }

    // A FULL join keeps the trades that match no refund too, with r NULL, so WHERE on t alone leaves
    // it the rows of a LEFT join, and WHERE on r alone those of a RIGHT join: the join keeps its plan.
    @Test
    public void testKeepsFullJoinRejectedOnOneSide() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(lateral("trades t FULL JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id > 0"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Full Outer Join")
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t101
                            3\t10\tnull
                            3\t11\tnull
                            """);
            assertQuery(lateral("trades t FULL JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE r.id > 0"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Full Outer Join")
                    .returns(ALL_ROWS);
        });
    }

    // A body that reads the designated timestamp of its rows keeps its outer joins, as the INNER join
    // would give its rows the timestamp of its master, which the RIGHT join does not. A window
    // function, in the body, in a joined sub-query or in a branch of UNION ALL, and LATEST ON in a
    // joined sub-query keep the RIGHT join and return the rows of the body. twap(), sparkline(), an
    // ASOF join after the RIGHT join and SAMPLE BY ... ALIGN TO FIRST OBSERVATION, which the optimiser
    // does not rewrite into GROUP BY, fail as the body does on its own; the INNER join to xs fails
    // the last one with another error, as the outer-ref join drops the timestamp of trades.
    @Test
    public void testKeepsOuterJoinsOfBodyThatReadsTheTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT o.id, l.tid, l.rid, l.rn FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, row_number() OVER () rn FROM "
                    + BODY + " WHERE t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns("""
                            id\ttid\trid\trn
                            1\t10\t100\t1
                            2\t11\t101\t1
                            """);
            assertQuery(lateral("trades t RIGHT JOIN (SELECT id, k, row_number() OVER () rn FROM refunds) r ON t.x = r.k AND r.k = o.k WHERE t.id > 0"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns(BOTH_TRADES);
            assertQuery(lateral("trades t RIGHT JOIN (SELECT * FROM refunds LATEST ON ts PARTITION BY k) r ON t.x = r.k AND r.k = o.k WHERE t.id > 0"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns(BOTH_TRADES);
            assertQuery(lateral(BODY + " WHERE t.id > 0 UNION ALL SELECT k, (row_number() OVER ())::INT FROM xs WHERE k = o.k"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns("""
                            id\ttid\trid
                            1\t1\t1
                            1\t10\t100
                            2\t2\t1
                            2\t11\t101
                            """);
            assertException(
                    "SELECT o.id, l.tw FROM orders o JOIN LATERAL (SELECT twap(t.id::double, t.ts) tw FROM " + BODY + " WHERE t.id > 0) l",
                    53,
                    "twap() requires the table's designated timestamp as the second argument"
            );
            // twap() inside an expression, as the left or right operand or as an argument of a function
            // of three arguments
            assertException(
                    "SELECT o.id, l.tw FROM orders o JOIN LATERAL (SELECT twap(t.id::double, t.ts) + 1 tw FROM " + BODY + " WHERE t.id > 0) l",
                    53,
                    "twap() requires the table's designated timestamp as the second argument"
            );
            assertException(
                    "SELECT o.id, l.tw FROM orders o JOIN LATERAL (SELECT 1 + twap(t.id::double, t.ts) tw FROM " + BODY + " WHERE t.id > 0) l",
                    57,
                    "twap() requires the table's designated timestamp as the second argument"
            );
            assertException(
                    "SELECT o.id, l.tw FROM orders o JOIN LATERAL (SELECT coalesce(twap(t.id::double, t.ts), 0.0, 1.0) tw FROM " + BODY + " WHERE t.id > 0) l",
                    62,
                    "twap() requires the table's designated timestamp as the second argument"
            );
            assertException(
                    "SELECT o.id, l.sp FROM orders o JOIN LATERAL (SELECT sparkline(t.id::double) sp FROM " + BODY + " WHERE t.id > 0) l",
                    53,
                    "sparkline() requires the base query to provide ascending designated timestamp order"
            );
            assertException(
                    lateral(BODY + " ASOF JOIN trades t2 WHERE t.id > 0"),
                    142,
                    "left side of time series join has no timestamp"
            );
            assertQuery("SELECT o.id, l.c FROM orders o JOIN LATERAL (SELECT count() c FROM trades t RIGHT JOIN xs x ON t.x = x.k AND x.k = o.k "
                    + "WHERE t.id > 0 SAMPLE BY 1h ALIGN TO FIRST OBSERVATION) l")
                    .noLeakCheck()
                    .failsWith("TIMESTAMP column is required but not provided");
            assertQuery("SELECT o.id, l.c FROM orders o JOIN LATERAL (SELECT count() c FROM trades t JOIN xs x ON t.x = x.k AND x.k = o.k "
                    + "WHERE t.id > 0 SAMPLE BY 1h ALIGN TO FIRST OBSERVATION) l")
                    .noLeakCheck()
                    .failsWith("base query does not provide designated TIMESTAMP column");
        });
    }

    // The rewriter does not reduce a RIGHT join before a LATERAL join, which pairs each row with the
    // rows of its body rather than by an ON clause.
    @Test
    public void testKeepsRightJoinBeforeNestedLateralJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT o.id, l.tid, l.rid, l.mv FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, z.mv FROM "
                    + BODY + " JOIN LATERAL (SELECT max(v) mv FROM xs WHERE xs.k = r.k) z WHERE t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns("""
                            id\ttid\trid\tmv
                            1\t10\t100\t100
                            2\t11\t101\t200
                            """);
        });
    }

    // A RIGHT join ahead of the first ON clause that reads the outer row runs once for all outer rows,
    // so it keeps its plan.
    @Test
    public void testKeepsRightJoinBeforeTheOuterRowIsRead() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT o.id, l.tid, l.rid, l.v FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, x.v FROM "
                    + "trades t RIGHT JOIN refunds r ON t.x = r.k JOIN xs x ON x.k = r.k AND x.k = o.k WHERE t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join Light\n")
                    .withPlanNotContaining("Cross Join")
                    .returns("""
                            id\ttid\trid\tv
                            1\t10\t100\t100
                            2\t11\t101\t200
                            """);
        });
    }

    // A FROM sub-query or a joined sub-query that reads the outer row keeps every join of the layer,
    // although x.v > 0 on the table xs and t.id > 0 drop the rows that the RIGHT join NULL-extends,
    // and so does a LEFT join whose ON clause reads it.
    @Test
    public void testKeepsRightJoinNextToCorrelatedSubQueryOrLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(lateral("(SELECT * FROM trades WHERE x >= o.k) t JOIN xs x ON x.k = t.x RIGHT JOIN refunds r ON x.k = r.k AND r.k = o.k WHERE x.v > 0"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns(BOTH_TRADES);
            assertQuery(lateral("trades t RIGHT JOIN (SELECT * FROM refunds WHERE k <= o.k) r ON t.x = r.k AND r.k = o.k WHERE t.id > 0"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns(BOTH_TRADES);
            assertQuery("SELECT o.id, l.tid, l.rid, l.v FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, x.v FROM "
                    + BODY + " LEFT JOIN xs x ON x.k = r.k AND x.k = o.k WHERE t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns("""
                            id\ttid\trid\tv
                            1\t10\t100\t100
                            2\t11\t101\t200
                            """);
        });
    }

    // A layer that does not read the outer row runs once for all outer rows, so its RIGHT join keeps
    // its plan.
    @Test
    public void testKeepsRightJoinOfLayerThatDoesNotReadTheOuterRow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(lateral("(SELECT t.id, r.id rid, r.k rk FROM trades t RIGHT JOIN refunds r ON t.x = r.k WHERE t.id > 0) t "
                    + "JOIN refunds r ON r.id = t.rid WHERE t.rk = o.k"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns(BOTH_TRADES);
        });
    }

    // Each conjunct keeps rows that the RIGHT join NULL-extends, for some outer row or some value, or
    // reads no column whose type the rewriter knows, so the join keeps its plan: IS NULL and != hold on
    // NULL; coalesce() turns NULL into a value; s holds 0, not NULL; an OR, and a conjunct on refunds,
    // which the join keeps whole, keep the rows; = <= and >= hold when the NULL o.k of order 3 meets
    // the NULL t.id; and the rewriter reads the type of a qualified column only.
    @Test
    public void testKeepsRightJoinOnConjunctThatMayKeepNullExtendedRows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String order3NullExtended = """
                    id\ttid\trid
                    3\tnull\t100
                    3\tnull\t101
                    3\tnull\t102
                    """;
            final String[][] cases = {
                    {"t.id IS NULL", NULL_EXTENDED_ROWS},
                    {"t.id != 10", """
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            2\tnull\t100
                            2\tnull\t102
                            2\t11\t101
                            3\tnull\t100
                            3\tnull\t101
                            3\tnull\t102
                            """},
                    {"coalesce(t.id, 10) > 5", ALL_ROWS},
                    {"t.s > -5", ALL_ROWS},
                    {"t.id > 0 OR r.id > 0", ALL_ROWS},
                    {"r.id > 0", ALL_ROWS},
                    {"t.id = o.k", order3NullExtended},
                    {"s > 0", TRADE_10},
            };
            for (String[] c : cases) {
                assertQuery(lateral(BODY + " WHERE " + c[0]))
                        .noLeakCheck()
                        .expectSize()
                        .withPlanContaining("Hash Right Outer Join")
                        .returns(c[1]);
            }
            // the plans of <= and >= against the outer key do not know their size
            final String[][] unsizedCases = {
                    {"t.id <= o.k", order3NullExtended},
                    {"t.id >= o.k", """
                            id\ttid\trid
                            1\t10\t100
                            2\t11\t101
                            3\tnull\t100
                            3\tnull\t101
                            3\tnull\t102
                            """},
            };
            for (String[] c : unsizedCases) {
                assertQuery(lateral(BODY + " WHERE " + c[0]))
                        .noLeakCheck()
                        .withPlanContaining("Hash Right Outer Join")
                        .returns(c[1]);
            }
        });
    }

    // A FULL join reduces when conjuncts drop the rows that it NULL-extends on both sides, and two RIGHT
    // joins reduce together when WHERE drops the rows that each NULL-extends: t.id > 0 drops those of
    // the RIGHT join to xs too, whose master side holds trades, whether its ON clause reads the outer
    // key or not.
    @Test
    public void testReducesFullJoinAndTwoRightJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(lateral("trades t FULL JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id > 0 AND r.id > 0"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns(BOTH_TRADES);
            assertQuery("SELECT o.id, l.tid, l.rid, l.v FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, x.v FROM "
                    + BODY + " RIGHT JOIN xs x ON x.k = r.k WHERE t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns("""
                            id\ttid\trid\tv
                            1\t10\t100\t100
                            2\t11\t101\t200
                            """);
            assertQuery("SELECT o.id, l.tid, l.rid, l.v FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, x.v FROM "
                    + BODY + " RIGHT JOIN xs x ON x.k = r.k AND x.k = o.k WHERE t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns("""
                            id\ttid\trid\tv
                            1\t10\t100\t100
                            2\t11\t101\t200
                            """);
        });
    }

    // The reduction applies to each context that holds the LATERAL join: a CTE, a view, a sub-query
    // with ORDER BY and LIMIT, a LEFT JOIN LATERAL, which keeps order 3 with NULL, UNION ALL and EXCEPT
    // of two LATERAL joins, and GROUP BY over the LATERAL rows.
    @Test
    public void testReducesRightJoinInEachContext() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String lateral = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + BODY + " WHERE t.id > 0) l";
            assertQuery("WITH q AS (" + lateral + ") SELECT * FROM q ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns(BOTH_TRADES);
            execute("CREATE VIEW v AS (" + lateral + ")");
            assertQuery("SELECT * FROM v ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns(BOTH_TRADES);
            assertQuery("SELECT * FROM (" + lateral + ") ORDER BY 1 LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns(TRADE_10);
            assertQuery("SELECT * FROM (" + lateral + ") ORDER BY 1 LIMIT -1")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns(TRADE_11);
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o LEFT JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + BODY + " WHERE t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .withPlanNotContaining("Right Outer Join", "Full Outer Join", "Nested Loop Right Join", "Nested Loop Full Join")
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            2\t11\t101
                            3\tnull\tnull
                            """);
            assertQuery("SELECT * FROM (" + lateral + " UNION ALL " + lateral + ") ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            1\t10\t100
                            2\t11\t101
                            2\t11\t101
                            """);
            assertQuery("SELECT * FROM (" + lateral + " EXCEPT SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM "
                    + BODY + " WHERE t.id > 10) l) ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns(TRADE_10);
            assertQuery("SELECT l.tid, count() c FROM (" + lateral + ") l GROUP BY l.tid ORDER BY 1")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns("""
                            tid\tc
                            10\t1
                            11\t1
                            """);
        });
    }

    // The reduction applies to every layer of the body: below GROUP BY, which returns 0 for order 3,
    // below SAMPLE BY, which the optimiser rewrites into a GROUP BY over timestamp_floor() of the
    // timestamp of trades before the rewriter runs, in each branch of UNION ALL, and in the body of a
    // LATERAL join nested in the body.
    @Test
    public void testReducesRightJoinInEachLayerOfTheBody() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT o.id, l.c FROM orders o JOIN LATERAL (SELECT count() c FROM " + BODY + " WHERE t.id > 0 SAMPLE BY 1h) l ORDER BY 1")
                    .noLeakCheck()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns("""
                            id\tc
                            1\t1
                            2\t1
                            """);
            assertQuery("SELECT o.id, l.c FROM orders o JOIN LATERAL (SELECT count(*) c FROM " + BODY + " WHERE t.id > 0) l ORDER BY 1")
                    .noLeakCheck()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns("""
                            id\tc
                            1\t1
                            2\t1
                            3\t0
                            """);
            assertQuery(lateral(BODY + " WHERE t.id > 0 UNION ALL SELECT t.id, r.id FROM " + BODY + " WHERE t.id > 10"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            2\t11\t101
                            2\t11\t101
                            """);
            assertQuery("SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT l2.tid, l2.rid FROM orders o2 JOIN LATERAL "
                    + "(SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o2.k WHERE t.id > 0) l2 "
                    + "WHERE o2.id = o.id) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns(BOTH_TRADES);
        });
    }

    // The rewriter reduces the joins of WAL tables as it does those of other tables.
    @Test
    public void testReducesRightJoinOfWalTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE trades (id INT, x INT, c INT, s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertRows();
            drainWalQueue();
            assertQuery(lateral(BODY + " WHERE t.id > o.k"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns(BOTH_TRADES);
        });
    }

    // A cached plan runs with every value of its bind variables. t.id > $1 is false on NULL whatever
    // the value, so the plan has no outer join, while t.id >= $1 holds on the NULL-extended rows for a
    // NULL value, as NULL >= NULL is true, so the RIGHT join keeps its plan.
    @Test
    public void testReducesRightJoinOnBindVariableComparison() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String reducedSql = lateral(BODY + " WHERE t.id > $1");
            bindVariableService.clear();
            bindVariableService.setInt(0, 0);
            assertQuery(reducedSql)
                    .noLeakCheck()
                    .assertsPlanNotContaining(OUTER_JOINS);
            final ObjList<BindVarTuple> reducedCases = new ObjList<>();
            reducedCases.add(BindVarTuple.ok("zero", BOTH_TRADES, service -> service.setInt(0, 0)));
            reducedCases.add(BindVarTuple.ok("NULL", NO_ROWS, service -> service.setInt(0, Numbers.INT_NULL)));
            reducedCases.add(BindVarTuple.ok("ten", TRADE_11, service -> service.setInt(0, 10)));
            reducedCases.add(BindVarTuple.ok("zero again", BOTH_TRADES, service -> service.setInt(0, 0)));
            assertQuery(reducedSql).noLeakCheck().expectSize().assertBinds(reducedCases);

            final String keptSql = lateral(BODY + " WHERE t.id >= $1");
            bindVariableService.clear();
            bindVariableService.setInt(0, 0);
            assertQuery(keptSql)
                    .noLeakCheck()
                    .assertsPlanContaining("Hash Right Outer Join");
            final ObjList<BindVarTuple> keptCases = new ObjList<>();
            keptCases.add(BindVarTuple.ok("NULL", NULL_EXTENDED_ROWS, service -> service.setInt(0, Numbers.INT_NULL)));
            keptCases.add(BindVarTuple.ok("ten", BOTH_TRADES, service -> service.setInt(0, 10)));
            keptCases.add(BindVarTuple.ok("NULL again", NULL_EXTENDED_ROWS, service -> service.setInt(0, Numbers.INT_NULL)));
            assertQuery(keptSql).noLeakCheck().expectSize().assertBinds(keptCases);
        });
    }

    // Each conjunct is false on the rows that the RIGHT join NULL-extends, so the rewriter plans the
    // join as an INNER join: < and > with a column of trades on one side, whatever the other side
    // reads, BETWEEN with the column as its value, and a conjunct that reads one column of trades and
    // constants and is false on the NULL of the column, or on the 0 of the SHORT column s, such as IN
    // with an interval. The optimiser plans NOT (t.id > 0) as 0 >= t.id, which is false on NULL. t.x = 1
    // implies the outer key 1 through the ON clause, and the filter that carries it into the outer key
    // sub-query must leave the keys of the joins as they are. A conjunct that reads no column proves
    // nothing, and the next one does.
    @Test
    public void testReducesRightJoinOnNullRejectingConjunct() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[][] cases = {
                    {"t.id > o.k", BOTH_TRADES},
                    {"o.k < t.id", BOTH_TRADES},
                    {"t.id > 0", BOTH_TRADES},
                    {"0 < t.id", BOTH_TRADES},
                    {"t.id < 11", TRADE_10},
                    {"t.c > -5", BOTH_TRADES},
                    {"t.id > (SELECT min(k) FROM xs)", BOTH_TRADES},
                    {"t.id > (SELECT min(k) FROM xs WHERE k > 5)", NO_ROWS},
                    {"t.id BETWEEN 0 AND 10", TRADE_10},
                    {"t.id BETWEEN o.k AND 100", BOTH_TRADES},
                    {"t.id IS NOT NULL", BOTH_TRADES},
                    {"t.id IN (11, 12)", TRADE_11},
                    {"t.id = 10", TRADE_10},
                    {"t.id >= 11", TRADE_11},
                    {"t.id + 1 > 11", TRADE_11},
                    {"coalesce(t.id, 0) > 5", BOTH_TRADES},
                    {"t.s > 0", TRADE_10},
                    {"t.ts IN '1970'", BOTH_TRADES},
                    {"NOT (t.id > 0)", NO_ROWS},
                    {"t.x = 1", TRADE_10},
                    {"t.x = 1 AND t.id > 0", TRADE_10},
                    {"1 < 2 AND t.id > 0", BOTH_TRADES},
            };
            for (String[] c : cases) {
                assertQuery(lateral(BODY + " WHERE " + c[0]))
                        .noLeakCheck()
                        .expectSize()
                        .withPlanNotContaining(OUTER_JOINS)
                        .returns(c[1]);
            }
        });
    }

    // A RIGHT join whose NULL-extended rows an ON conjunct of a later INNER join drops reduces as well.
    // An INNER join that keys on r alone keeps those rows, and so does the ON clause of a LEFT join,
    // which NULL-extends the rows that it does not match rather than dropping them.
    @Test
    public void testReducesRightJoinOnOnClauseOfLaterInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String select = "SELECT o.id, l.tid, l.rid, l.v FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid, x.v FROM ";
            assertQuery(select + BODY + " JOIN xs x ON x.k = r.k AND t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining(OUTER_JOINS)
                    .returns("""
                            id\ttid\trid\tv
                            1\t10\t100\t100
                            2\t11\t101\t200
                            """);
            assertQuery(select + BODY + " JOIN xs x ON x.k = r.k) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns("""
                            id\ttid\trid\tv
                            1\tnull\t101\t200
                            1\tnull\t102\t300
                            1\t10\t100\t100
                            2\tnull\t100\t100
                            2\tnull\t102\t300
                            2\t11\t101\t200
                            3\tnull\t100\t100
                            3\tnull\t101\t200
                            3\tnull\t102\t300
                            """);
            assertQuery(select + BODY + " LEFT JOIN xs x ON x.k = r.k AND t.id > 0) l ORDER BY 1, 2, 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Hash Right Outer Join")
                    .returns("""
                            id\ttid\trid\tv
                            1\tnull\t101\tnull
                            1\tnull\t102\tnull
                            1\t10\t100\t100
                            2\tnull\t100\tnull
                            2\tnull\t102\tnull
                            2\t11\t101\t200
                            3\tnull\t100\tnull
                            3\tnull\t101\tnull
                            3\tnull\t102\tnull
                            """);
        });
    }

    // The type of a column decides whether the plan has an outer join, so a compiled query that a column
    // type change invalidates plans again. WHERE t.c > -1 is false on the NULL of an INT, and true on
    // the 0 that a SHORT column holds in the NULL-extended rows. A data change keeps the plan.
    @Test
    public void testReplansAfterColumnTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = lateral(BODY + " WHERE t.c > -1");
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanNotContaining(OUTER_JOINS);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .mutateWith("INSERT INTO trades VALUES (12, 2, 0, 0, 3::timestamp)")
                    .returns(TRADE_10, """
                            id\ttid\trid
                            1\t10\t100
                            2\t12\t101
                            """);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .mutateWith("ALTER TABLE trades ALTER COLUMN c TYPE SHORT")
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            2\t12\t101
                            """, """
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t102
                            2\t12\t101
                            3\tnull\t100
                            3\tnull\t101
                            3\tnull\t102
                            """);
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanContaining("Hash Right Outer Join");
        });
    }

    // The rows of each outer row, sorted, with the columns of the body as tid and rid
    private static String lateral(String body) {
        return "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM " + body + ") l ORDER BY 1, 2, 3";
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE trades (id INT, x INT, c INT, s SHORT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        insertRows();
    }

    private void insertRows() throws Exception {
        execute("INSERT INTO orders VALUES (1, 1, 1::timestamp), (2, 2, 2::timestamp), (3, NULL, 3::timestamp)");
        execute("INSERT INTO trades VALUES (10, 1, 5, 5, 1::timestamp), (11, 2, -3, -3, 2::timestamp)");
        execute("INSERT INTO refunds VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp), (102, 3, 3::timestamp)");
        execute("CREATE TABLE xs (k INT, v INT)");
        execute("INSERT INTO xs VALUES (1, 100), (2, 200), (3, 300)");
    }
}
