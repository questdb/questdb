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

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class LateralCorrelationTest extends AbstractCairoTest {
    private static final String FULL_JOIN_CARRIER_ROWS = """
            id\taid\tbid
            1\tnull\t21
            1\t10\t20
            2\tnull\t20
            2\tnull\t21
            2\t10\tnull
            """;
    private static final String PER_OUTER_ROW_RIGHT_JOIN = """
            id\ttid\trid
            1\tnull\t101
            1\t10\t100
            2\tnull\t100
            2\tnull\t101
            """;

    @Test
    public void testAsofJoinBeforeCorrelatedRightJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO a VALUES (10, 1, '2024-01-01T00:00:01.000000Z'), (11, 2, '2024-01-01T00:00:03.000000Z')");
            execute("CREATE TABLE q (qid INT, qk INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO q VALUES
                    (50, 1, '2024-01-01T00:00:00.000000Z'),
                    (51, 2, '2024-01-01T00:00:02.000000Z'),
                    (52, 1, '2024-01-01T00:00:04.000000Z')
                    """);
            execute("CREATE TABLE r (id INT, k INT)");
            execute("INSERT INTO r VALUES (100, 1), (101, 2)");
            assertQuery("""
                    SELECT o.id, l.aid, l.qid, l.rid FROM o JOIN LATERAL (
                        SELECT a.id aid, q.qid, r.id rid
                        FROM a ASOF JOIN (SELECT qid, ts FROM q WHERE qk = o.k) q
                        RIGHT JOIN r ON r.k = a.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tqid\trid
                            1\t10\t50\t100
                            1\t11\t50\t101
                            2\t10\tnull\t100
                            2\t11\t51\t101
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.qid, l.rid FROM o JOIN LATERAL (
                        SELECT a.id aid, q.qid, r.id rid
                        FROM a ASOF JOIN q RIGHT JOIN r ON r.k = a.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tqid\trid
                            1\tnull\tnull\t101
                            1\t10\t50\t100
                            2\tnull\tnull\t100
                            2\t11\t51\t101
                            """);
        });
    }

    @Test
    public void testConsecutiveRightAndFullJoinsAfterCorrelatedOn() throws Exception {
        // #7723
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            execute("INSERT INTO xs VALUES (3, 300)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid, l.xk FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid, x.k xk FROM trades t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        RIGHT JOIN xs x ON x.k = r.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid\txk
                            1\tnull\tnull\t3
                            1\t10\t100\t1
                            2\tnull\tnull\t3
                            2\tnull\t100\t1
                            """);
            execute("INSERT INTO xs VALUES (2, 200)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid, l.xk FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid, x.k xk FROM trades t
                        FULL JOIN refunds r ON t.x = r.k AND r.k = o.k
                        RIGHT JOIN xs x ON x.k = r.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid\txk
                            1\tnull\tnull\t3
                            1\tnull\t101\t2
                            1\t10\t100\t1
                            2\tnull\tnull\t3
                            2\tnull\t100\t1
                            2\tnull\t101\t2
                            """);
        });
    }

    @Test
    public void testCorrelatedFromSubQueryWithWhereEquality() throws Exception {
        // #7727
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            assertQuery("""
                    SELECT o.id, l.sx FROM o JOIN LATERAL (
                        SELECT s.x sx FROM (SELECT id, k, x FROM a WHERE k = o.k) s WHERE s.x = o.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tsx
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, l.sx FROM o JOIN LATERAL (
                        SELECT s.x sx FROM (SELECT id, k, x FROM a WHERE k = o.k) s WHERE s.x = o.k
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tsx
                            1\t1
                            2\t2
                            """);
        });
    }

    @Test
    public void testCorrelatedPredicateWithSubQuery() throws Exception {
        // #7803 section 16
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, v INT)");
            execute("INSERT INTO o VALUES (1, 1, 5), (2, 2, 50)");
            execute("CREATE TABLE b (k INT, v INT)");
            execute("INSERT INTO b VALUES (1, 10), (2, 20)");
            execute("CREATE TABLE c (x INT)");
            execute("INSERT INTO c VALUES (1), (2)");
            assertQuery("""
                    SELECT o.id, t.v FROM o CROSS JOIN LATERAL (
                        SELECT b.v FROM b WHERE b.k = o.k AND b.v - o.v > (SELECT count() FROM c)
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv
                            1\t10
                            """);
            assertQuery("""
                    SELECT o.id, t.v FROM o CROSS JOIN LATERAL (
                        SELECT b.v FROM b WHERE b.k = o.k AND o.v > (SELECT count() FROM c)
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv
                            1\t10
                            2\t20
                            """);
            assertQuery("""
                    SELECT o.id, t.v FROM o LEFT JOIN LATERAL (
                        SELECT b.v FROM b WHERE b.k = o.k AND b.v - o.v > (SELECT count() FROM c)
                    ) t ON true ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            2\tnull
                            """);
        });
    }

    @Test
    public void testCorrelatedSubQueryAroundRightOrFullJoin() throws Exception {
        // #7724
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid, l.xk FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid, x.k xk
                        FROM trades t FULL JOIN refunds r ON r.k = t.x
                        LEFT JOIN (SELECT k, v FROM xs WHERE k = o.k) x ON x.v > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid\txk
                            1\tnull\t101\t1
                            1\t10\t100\t1
                            2\tnull\t101\tnull
                            2\t10\t100\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM trades t FULL JOIN (SELECT id, k FROM refunds WHERE k = o.k) r ON r.k = t.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            2\tnull\t101
                            2\t10\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM (SELECT id, x FROM trades WHERE x = o.k) t RIGHT JOIN refunds r ON t.x = r.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM trades t JOIN (SELECT k, v FROM xs WHERE k = o.k) x ON x.k = t.x
                        RIGHT JOIN refunds r ON r.k = t.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM (SELECT id, x FROM trades WHERE x = o.k) t
                        FULL JOIN (SELECT id, k FROM refunds WHERE k = o.k) r ON r.k = t.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            2\tnull\t101
                            """);
        });
    }

    @Test
    public void testCorrelatedUnionAllOnLeftJoinSlave() throws Exception {
        // #7803 section 5
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 2)");
            execute("CREATE TABLE c (id INT, k INT, x INT)");
            execute("INSERT INTO c VALUES (30, 1, 1), (31, 2, 2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.rid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid FROM a
                        LEFT JOIN (
                            SELECT id, k FROM b WHERE x = o.x
                            UNION ALL
                            SELECT id, k FROM c WHERE x = o.x
                        ) r ON r.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid
                            1\t10\t20
                            1\t10\t30
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            2\t11\t31
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.rid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid FROM a
                        JOIN (
                            SELECT id, k FROM b WHERE x = o.x
                            UNION ALL
                            SELECT id, k FROM c WHERE x = o.x
                        ) r ON r.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid
                            1\t10\t20
                            1\t10\t30
                            2\t11\t21
                            2\t11\t31
                            """);
        });
    }

    @Test
    public void testCorrelationAtOrBeforeSpliceJoinRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE a (id INT, g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (id INT, g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, #u33d8, '2024-01-01T00:00:01.000000Z'), (2, #v33d8, '2024-01-01T00:00:02.000000Z')");
            execute("INSERT INTO a VALUES (10, #u33d8, '2024-01-01T00:00:01.000000Z'), (11, #v33d8, '2024-01-01T00:00:03.000000Z')");
            execute("INSERT INTO b VALUES (20, #u33d8, '2024-01-01T00:00:02.000000Z'), (21, #w33d8, '2024-01-01T00:00:04.000000Z')");
            assertSpliceCorrelationRejected("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM (SELECT * FROM a WHERE id > o.id + 8) a SPLICE JOIN b
                    ) l
                    """);
            assertSpliceCorrelationRejected("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a SPLICE JOIN (SELECT * FROM b WHERE id > o.id + 18) b
                    ) l
                    """);
            assertSpliceCorrelationRejected("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a JOIN b ON b.id = o.id + 19 SPLICE JOIN b b2
                    ) l
                    """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a SPLICE JOIN b WHERE b.g = o.g
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            1\t11\t20
                            """);
        });
    }

    @Test
    public void testCountOverRightJoinWithEmptyPreservedSide() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            execute("INSERT INTO orders VALUES (3, 3)");
            assertQuery("""
                    SELECT o.id, l.c FROM orders o LEFT JOIN LATERAL (
                        SELECT count(*) c FROM trades t RIGHT JOIN (SELECT id, k FROM refunds WHERE k = o.k) r ON t.x = r.k
                    ) l ON true ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t1
                            2\t1
                            3\t0
                            """);
            assertQuery("""
                    SELECT o.id, l.c FROM orders o JOIN LATERAL (
                        SELECT count(*) c FROM trades t RIGHT JOIN (SELECT id, k FROM refunds WHERE k = o.k) r ON t.x = r.k
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t1
                            2\t1
                            3\t0
                            """);
        });
    }

    @Test
    public void testDomainOfAsofJoinedOuterColumnHoldsNull() throws Exception {
        // #7803 section 11
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, 1, '2024-01-01T00:00:01.000000Z'), (2, 2, '2024-01-01T00:00:02.000000Z')");
            execute("CREATE TABLE c (v INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO c VALUES (5, 1, '2024-01-01T00:00:00.000000Z')");
            execute("CREATE TABLE d (v INT)");
            execute("INSERT INTO d VALUES (3), (7)");
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o ASOF JOIN c ON (x) CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(c.v, 0)
                    ) t ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
        });
    }

    @Test
    public void testDomainOfLeftJoinedOuterColumnHoldsNull() throws Exception {
        // #7803 section 11
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE c (v INT, x INT)");
            execute("INSERT INTO c VALUES (5, 1)");
            execute("CREATE TABLE d (v INT)");
            execute("INSERT INTO d VALUES (3), (7)");
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o LEFT JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(c.v, 0)
                    ) t ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, t1.v cv, t2.dv FROM o LEFT JOIN LATERAL (
                        SELECT v FROM c WHERE x = o.x
                    ) t1 ON true CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(t1.v, 0)
                    ) t2 ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o LEFT JOIN (SELECT v, x FROM c) c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(c.v, 0)
                    ) t ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o LEFT JOIN c ON c.x = o.x LEFT JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(c.v, 0)
                    ) t ON true ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, t1.v cv, t2.dv FROM o LEFT JOIN LATERAL (
                        SELECT v FROM c WHERE x = o.x
                    ) t1 ON true LEFT JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(t1.v, 0)
                    ) t2 ON true ORDER BY 1, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t7
                            2\tnull\t3
                            2\tnull\t7
                            """);
            assertQuery("""
                    SELECT o.id, c.v cv, t.n FROM o LEFT JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT count(*) n FROM d WHERE v > coalesce(c.v, 0)
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tn
                            1\t5\t1
                            2\tnull\t2
                            """);
            assertQuery("""
                    SELECT o.id, t1.v cv, t2.n FROM o LEFT JOIN LATERAL (
                        SELECT v FROM c WHERE x = o.x
                    ) t1 ON true CROSS JOIN LATERAL (
                        SELECT count(*) n FROM d WHERE v > coalesce(t1.v, 0)
                    ) t2 ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tcv\tn
                            1\t5\t1
                            2\tnull\t2
                            """);
        });
    }

    @Test
    public void testDomainOfLeftJoinedOuterColumnHoldsNullForEveryKeyType() throws Exception {
        // #7803 section 11
        assertMemoryLeak(() -> {
            assertNullOuterValueFindsDomainRow("INT", "1", "2");
            assertNullOuterValueFindsDomainRow("LONG", "1", "2");
            assertNullOuterValueFindsDomainRow("DOUBLE", "1.5", "2.5");
            assertNullOuterValueFindsDomainRow("FLOAT", "1.5", "2.5");
            assertNullOuterValueFindsDomainRow("STRING", "'a'", "'b'");
            assertNullOuterValueFindsDomainRow("VARCHAR", "'a'", "'b'");
            assertNullOuterValueFindsDomainRow("SYMBOL", "'a'", "'b'");
            assertNullOuterValueFindsDomainRow("CHAR", "'a'", "'b'");
            assertNullOuterValueFindsDomainRow("UUID", "'11111111-1111-1111-1111-111111111111'", "'22222222-2222-2222-2222-222222222222'");
            assertNullOuterValueFindsDomainRow("IPV4", "'1.1.1.1'", "'2.2.2.2'");
            assertNullOuterValueFindsDomainRow("LONG256", "'0x01'", "'0x02'");
            assertNullOuterValueFindsDomainRow("DATE", "'2024-01-01T00:00:00.000Z'", "'2024-01-02T00:00:00.000Z'");
            assertNullOuterValueFindsDomainRow("TIMESTAMP", "'2024-01-01T00:00:00.000000Z'", "'2024-01-02T00:00:00.000000Z'");
            assertNullOuterValueFindsDomainRow("TIMESTAMP_NS", "'2024-01-01T00:00:00.000000000Z'", "'2024-01-02T00:00:00.000000000Z'");
        });
    }

    @Test
    public void testDomainOfOuterColumnNulledByRightOrFullJoin() throws Exception {
        // #7803 section 11
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE c (v INT, x INT)");
            execute("INSERT INTO c VALUES (5, 1), (6, 9)");
            execute("CREATE TABLE d (v INT)");
            execute("INSERT INTO d VALUES (3), (7)");
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o RIGHT JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(o.x, 0) + 1
                    ) t ORDER BY 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            1\t5\t3
                            1\t5\t7
                            null\t6\t3
                            null\t6\t7
                            """);
            assertQuery("""
                    SELECT o.id, c.v cv, t.dv FROM o FULL JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                        SELECT v dv FROM d WHERE v > coalesce(o.x, 0) + 1
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcv\tdv
                            null\t6\t3
                            null\t6\t7
                            1\t5\t3
                            1\t5\t7
                            2\tnull\t7
                            """);
        });
    }

    @Test
    public void testDuplicatedOnConjunctOfOuterJoinInBody() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            final String plan = """
                    Sort
                      keys: [id, tid, rid]
                      Project
                        columns: [o.id, l.tid, l.rid]
                        Join
                          Master o
                            Scan
                              table: orders
                              columns: [id, k]
                          INNER l
                            keys: [l.__qdb_outer_ref__0_k = o.k]
                            Project
                              columns: [t.id AS tid, r.id AS rid, r.__qdb_outer_ref__0_k]
                              Join
                                Master t
                                  Scan
                                    table: trades
                                    columns: [id, x]
                                RIGHT r
                                  keys: [r.k = t.x]
                                  on: r.k = r.__qdb_outer_ref__0_k
                                  Join
                                    Master
                                      Scan
                                        table: refunds
                                        columns: [id, k]
                                    CROSS __qdb_outer_ref__0
                                      Aggregate
                                        keys: [k AS __qdb_outer_ref__0_k]
                                        values: []
                                        Scan
                                          table: orders
                                          columns: [k]
                    """;
            final String single = """
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """;
            final String duplicated = """
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """;
            assertQuery(single).noLeakCheck().assertsLogicalPlan(plan);
            assertQuery(duplicated).noLeakCheck().assertsLogicalPlan(plan);
            assertQuery(duplicated).noLeakCheck().expectSize().returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
        });
    }

    @Test
    public void testEquatedInnerColumnsFollowedByLeftJoinsWithRedundantKeys() throws Exception {
        // #7731
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE tb (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE tc (k INT, x INT, y INT, v INT)");
            execute("CREATE TABLE td (k INT, x INT, y INT, v INT)");
            execute("INSERT INTO ta VALUES (null, 1, 1, 101), (1, 0, 3, 102), (2, 1, null, 103), (0, 1, 1, 104), (3, 0, 2, 105), (1, 3, 0, 106)");
            execute("INSERT INTO tb VALUES (0, 1, 1, 201), (0, 1, 1, 202), (null, null, 0, 203), (2, 3, 2, 204), (0, 2, 3, 205)");
            execute("INSERT INTO tc VALUES (2, 0, null, 301), (3, 2, 3, 302), (2, 3, null, 303), (3, 3, 3, 304), (0, 3, 0, 305)");
            execute("INSERT INTO td VALUES (0, null, null, 401), (0, 3, 0, 402), (0, null, 1, 403), (1, 3, 0, 404), (null, 1, 1, 405), (2, 0, 1, 406)");
            assertQuery("""
                    SELECT a.v av, t.v tv, c.v cv, d.v dv
                    FROM ta a
                    JOIN LATERAL (
                        SELECT k kk, x xx, y yy, v FROM tb WHERE k = a.x AND x = a.x
                    ) t ON t.yy = a.y
                    LEFT JOIN tc c ON c.k = t.kk AND c.k = t.xx
                    LEFT JOIN td d ON t.yy = d.y AND t.yy = d.y
                    WHERE a.x = 1
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            av\ttv\tcv\tdv
                            """);
        });
    }

    @Test
    public void testFailedCompilationFreesSharedSource() throws Exception {
        // #7835
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            final String sql1 = """
                    SELECT o.id, l.aid, l.rid, l.sid
                    FROM o
                    JOIN LATERAL (
                        SELECT a.id aid, r.id rid, s.id sid
                        FROM a
                        JOIN (SELECT id, k FROM b WHERE x != o.x) r ON r.k = a.k
                        ASOF JOIN b s
                    ) l
                    """;
            assertExceptionNoLeakCheck(sql1, sql1.indexOf("ASOF JOIN b s"), "left side of time series join has no timestamp");
            final String sql2 = """
                    EXPLAIN SELECT o.id, l.aid, l.rid, l.sid
                    FROM o
                    JOIN LATERAL (
                        SELECT a.id aid, r.id rid, s.id sid
                        FROM a
                        JOIN (SELECT id, k FROM b WHERE x != o.x) r ON r.k = a.k
                        ASOF JOIN b s
                    ) l
                    """;
            assertExceptionNoLeakCheck(sql2, sql2.indexOf("ASOF JOIN b s"), "left side of time series join has no timestamp");
        });
    }

    @Test
    public void testFullJoinCarrierForEveryOuterColumnType() throws Exception {
        assertMemoryLeak(() -> {
            assertFullJoinCarrier("BOOLEAN", "false", "true");
            assertFullJoinCarrier("BYTE", "0::byte", "1::byte");
            assertFullJoinCarrier("SHORT", "0::short", "1::short");
            assertFullJoinCarrier("CHAR", "'a'", "'b'");
            assertFullJoinCarrier("GEOHASH(5c)", "#u33d8", "#v33d8");
            assertFullJoinCarrier("GEOHASH(1c)", "#u", "#v");
            assertFullJoinCarrier("INT", "1", "2");
            assertFullJoinCarrier("LONG", "1", "2");
            assertFullJoinCarrier("DOUBLE", "1.5", "2.5");
            assertFullJoinCarrier("SYMBOL", "'s1'", "'s2'");
            assertFullJoinCarrier("VARCHAR", "'v1'", "'v2'");
            assertFullJoinCarrier("STRING", "'t1'", "'t2'");
            assertFullJoinCarrier("IPv4", "'1.1.1.1'", "'2.2.2.2'");
            assertFullJoinCarrier("UUID", "'11111111-1111-1111-1111-111111111111'", "'22222222-2222-2222-2222-222222222222'");
            assertFullJoinCarrier("LONG256", "'0x01'", "'0x02'");
            assertFullJoinCarrier("DECIMAL(18,2)", "1.5m", "2.5m");
            assertFullJoinCarrier("DECIMAL(38,2)", "1.5m", "2.5m");
            assertFullJoinCarrier("DATE", "'2024-01-01'", "'2024-01-02'");
            assertFullJoinCarrier("TIMESTAMP", "'2024-01-01T00:00:00.000001Z'", "'2024-01-02T00:00:00.000001Z'");
            assertFullJoinCarrier("TIMESTAMP_NS", "'2024-01-01T00:00:00.000000001Z'", "'2024-01-02T00:00:00.000000002Z'");
        });
    }

    @Test
    public void testFullJoinCarrierForIntervalOuterColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO o VALUES (1, '2024-01-01T00:00:01.000000Z'), (2, '2024-01-01T00:00:02.000000Z')");
            execute("CREATE TABLE a (id INT, ts TIMESTAMP)");
            execute("INSERT INTO a VALUES (10, '2024-01-01T00:00:01.000000Z')");
            execute("CREATE TABLE b (id INT, ts TIMESTAMP)");
            execute("INSERT INTO b VALUES (20, '2024-01-01T00:00:01.000000Z'), (21, '2024-01-01T00:00:02.000000Z')");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM (SELECT id, interval(ts, ts) iv FROM o) o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.ts = b.ts AND b.ts IN o.iv
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(FULL_JOIN_CARRIER_ROWS);
        });
    }

    @Test
    public void testFullJoinOnReadingOuterColumn() throws Exception {
        // #7723, #7730
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            final String expected = """
                    id\ttid\trid
                    1\tnull\t101
                    1\t10\t100
                    2\tnull\t100
                    2\tnull\t101
                    2\t10\tnull
                    """;
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t FULL JOIN refunds r ON r.k = o.k AND t.x = r.k AND t.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t FULL JOIN refunds r ON r.k = o.k AND t.x = r.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, l.c, l.ct FROM orders o JOIN LATERAL (
                        SELECT count(*) c, count(t.id) ct FROM trades t FULL JOIN refunds r ON r.k = o.k AND t.x = r.k
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc\tct
                            1\t2\t1
                            2\t3\t1
                            """);
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.x = b.k AND b.k = o.k WHERE a.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.x = b.k AND b.k = o.k WHERE a.id > 0 AND b.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testFullJoinPlanSplitsWithMarkerCarrier() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t FULL JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Sort
                              keys: [id, tid, rid]
                              Project
                                columns: [o.id, l.tid, l.rid]
                                Join
                                  Master o
                                    Scan
                                      table: orders
                                      columns: [id, k]
                                  INNER l
                                    keys: [l.__qdb_outer_ref__0_k = o.k]
                                    Project
                                      columns: [id AS tid, id1 AS rid, __qdb_outer_ref__0_k]
                                      Project
                                        columns: [t.id, r.id AS id1, case(t.__qdb_outer_ref__marker_0 = null, r.__qdb_outer_ref__0_k, t.__qdb_outer_ref__0_k) AS __qdb_outer_ref__0_k]
                                        Filter
                                          predicate: t.id > 0
                                          Join
                                            Master t
                                              Join
                                                Master
                                                  Scan
                                                    table: trades
                                                    columns: [id, x]
                                                CROSS __qdb_outer_ref__0
                                                  Aggregate
                                                    keys: [k AS __qdb_outer_ref__0_k]
                                                    values: [count() AS __qdb_outer_ref__marker_0]
                                                    Scan
                                                      table: orders
                                                      columns: [k]
                                            FULL r
                                              keys: [r.k = t.x, r.__qdb_outer_ref__0_k = t.__qdb_outer_ref__0_k]
                                              on: r.k = r.__qdb_outer_ref__0_k
                                              Join
                                                Master
                                                  Scan
                                                    table: refunds
                                                    columns: [id, k]
                                                CROSS __qdb_outer_ref__0_1
                                                  Aggregate
                                                    keys: [k AS __qdb_outer_ref__0_k]
                                                    values: []
                                                    Scan
                                                      table: orders
                                                      columns: [k]
                            """);
        });
    }

    @Test
    public void testGroupByOverJoinedCorrelatedSubQuery() throws Exception {
        // #7728
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT a.x g, count(*) c
                        FROM a CROSS JOIN (SELECT id, k, x FROM b WHERE x = o.x) s
                        GROUP BY a.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            1\t1\t1
                            1\t2\t1
                            """);
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT a.x g, count(*) c
                        FROM a JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON s.k = a.k
                        GROUP BY a.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            1\t1\t1
                            """);
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT s.x g, count(*) c
                        FROM a CROSS JOIN (SELECT id, k, x FROM b WHERE x = o.x) s
                        GROUP BY s.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            1\t1\t2
                            """);
            assertQuery("""
                    SELECT o.id, l.c FROM o JOIN LATERAL (
                        SELECT count(*) c FROM a CROSS JOIN (SELECT id, k, x FROM b WHERE x = o.x) s
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t2
                            2\t0
                            """);
        });
    }

    @Test
    public void testGroupByOverJoinWithMixedCorrelation() throws Exception {
        // #7694
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (ok INT, k INT, v INT)");
            execute("INSERT INTO o VALUES (1, 10, 1), (2, 20, 2), (3, 30, 2)");
            execute("CREATE TABLE t (ts TIMESTAMP, id INT, v INT) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:01', 1, 1), ('2024-01-01T00:00:03', 2, 2), ('2024-01-01T00:00:05', 3, 3)");
            execute("CREATE TABLE p (ts TIMESTAMP, id INT, k INT) TIMESTAMP(ts)");
            execute("INSERT INTO p VALUES ('2024-01-01T00:00:00', 1, 10), ('2024-01-01T00:00:02', 2, 20), ('2024-01-01T00:00:04', 2, 30), ('2024-01-01T00:00:06', 3, 10)");
            assertQuery("""
                    SELECT o.ok, s.id, s.c
                    FROM o JOIN LATERAL (
                        SELECT t.id, count(*) c
                        FROM t JOIN p ON t.id = p.id
                        WHERE p.k = o.k AND t.v <= o.v
                        GROUP BY t.id
                    ) s
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tc
                            1\t1\t1
                            2\t2\t1
                            3\t2\t1
                            """);
            assertQuery("""
                    SELECT o.ok, s.id, s.c
                    FROM o JOIN LATERAL (
                        SELECT t.id, count(*) c
                        FROM t LEFT JOIN p ON t.id = p.id
                        WHERE p.k = o.k AND t.v <= o.v
                        GROUP BY t.id
                    ) s
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tc
                            1\t1\t1
                            2\t2\t1
                            3\t2\t1
                            """);
        });
    }

    @Test
    public void testGroupByOverJoinWithNonEqualityCorrelation() throws Exception {
        // #7694
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT a.k g, count(*) c FROM a LEFT JOIN b ON a.x = b.k WHERE a.x < o.x GROUP BY a.k
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            2\t1\t1
                            """);
            assertQuery("""
                    SELECT o.id, l.g, l.c FROM o JOIN LATERAL (
                        SELECT a.k g, count(*) c FROM a JOIN b ON a.x = b.k WHERE a.x < o.x GROUP BY a.k
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tg\tc
                            2\t1\t1
                            """);
        });
    }

    @Test
    public void testInnerJoinKeysSharingColumnNamesInBody() throws Exception {
        // #7716
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a JOIN b ON a.x = b.k AND b.x = o.k
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            """);
        });
    }

    @Test
    public void testInnerJoinOnReadingOuterColumnBeforeRightOrFullJoin() throws Exception {
        // #7723, #7737
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k FULL JOIN refunds r ON r.k = t.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t JOIN xs x ON x.k = o.k RIGHT JOIN refunds r ON r.k = t.x WHERE r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\t10\t100
                            2\tnull\t101
                            """);
        });
    }

    @Test
    public void testJoinedCorrelatedSubQueryKeepsOnWithSecondOuterColumn() throws Exception {
        // #7725
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("INSERT INTO b VALUES (20, 5, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid, o.x ox
                        FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.k) s ON a.x = s.k
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a JOIN (SELECT id, k, x FROM b WHERE x = o.k) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\taid\tsid
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.k AND id > o.x) s ON a.x = s.k
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
        });
    }

    @Test
    public void testJoinedTablesSharingColumnNameWithTwoCorrelatedConditions() throws Exception {
        // #7836
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("CREATE TABLE d (id INT)");
            execute("CREATE TABLE o2 (oid INT, ox INT)");
            execute("INSERT INTO o VALUES (1, 10), (2, 20)");
            execute("INSERT INTO a VALUES (1, 1), (2, 2)");
            execute("INSERT INTO d VALUES (1), (2)");
            execute("INSERT INTO o2 VALUES (1, 10), (2, 20)");
            assertQuery("""
                    SELECT o.id, t.aid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE a.id = o.id AND a.k <= o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE a.id = o.id AND a.k != o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE d.id = o.id AND a.k <= o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE a.id = o.id AND a.k <= o.x
                    ) t ON true ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o2.oid, t.aid FROM o2 CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN d ON d.id = a.id WHERE a.id = o2.oid AND a.k <= o2.ox
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            oid\taid
                            1\t1
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN a d ON d.id = a.id WHERE a.id = o.id AND a.k <= o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            1\t1
                            2\t2
                            """);
        });
    }

    @Test
    public void testJoinToSubQueryWithCorrelatedOn() throws Exception {
        // #7729
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a JOIN (SELECT id, k FROM b) s ON s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            1\t11\t20
                            2\t10\t21
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a JOIN (SELECT id, k FROM b) s ON a.x = s.k AND s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a JOIN (SELECT id, k FROM b) s ON a.x = s.k AND a.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a JOIN b ON a.x = b.k AND b.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a LEFT JOIN (SELECT id, k FROM b) s ON a.x = s.k AND s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a CROSS JOIN (SELECT id, k FROM b) s WHERE a.x = s.k AND s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testKeylessAggregateReadByFilterOrJoin() throws Exception {
        // #7803 section 2
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 9)");
            execute("CREATE TABLE p (k INT, v INT)");
            execute("INSERT INTO p VALUES (0, 10)");
            execute("CREATE TABLE c (x INT)");
            execute("INSERT INTO c VALUES (1)");
            execute("CREATE TABLE s (k INT, v INT)");
            execute("INSERT INTO s VALUES (0, 500), (1, 501)");
            assertQuery("""
                    SELECT o.id, t.cnt FROM o CROSS JOIN LATERAL (
                        SELECT cnt FROM (SELECT count(*)::INT cnt FROM c WHERE x = o.x) WHERE cnt < 5
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcnt
                            1\t1
                            2\t0
                            """);
            assertQuery("""
                    SELECT o.id, t.cnt, t.sv FROM o CROSS JOIN LATERAL (
                        SELECT n1.cnt, n2.v sv
                        FROM (SELECT count(*)::INT cnt FROM c WHERE x = o.x) n1 JOIN s n2 ON n2.k = n1.cnt
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcnt\tsv
                            1\t1\t501
                            2\t0\t500
                            """);
            assertQuery("""
                    SELECT o.id, t.pv, t.cnt, t.sv FROM o CROSS JOIN LATERAL (
                        SELECT p.v pv, r.cnt, s.v sv FROM p
                        LEFT JOIN (SELECT count(*)::INT cnt FROM c WHERE x = o.x) r ON true
                        JOIN s ON s.k = r.cnt
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpv\tcnt\tsv
                            1\t10\t1\t501
                            2\t10\t0\t500
                            """);
            assertQuery("""
                    SELECT o.id, t.pv, t.cnt FROM o CROSS JOIN LATERAL (
                        SELECT p.v pv, r.cnt FROM p
                        LEFT JOIN (SELECT count(*)::INT cnt FROM c WHERE x = o.x) r ON r.cnt = p.k
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpv\tcnt
                            1\t10\tnull
                            2\t10\t0
                            """);
            assertQuery("""
                    SELECT o.id, t.pv, t.cnt, t.sv FROM o CROSS JOIN LATERAL (
                        SELECT p.v pv, r.cnt, s.v sv FROM p
                        LEFT JOIN (SELECT count(*)::INT cnt FROM c WHERE x = o.x) r ON true
                        LEFT JOIN s ON s.k = r.cnt
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpv\tcnt\tsv
                            1\t10\t1\t501
                            2\t10\t0\t500
                            """);
            assertQuery("""
                    SELECT o.id, t.pv, t.cnt FROM o CROSS JOIN LATERAL (
                        SELECT p.v pv, r.cnt FROM p
                        JOIN (SELECT count(*)::INT cnt FROM c WHERE x = o.x) r ON r.cnt = p.k
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpv\tcnt
                            2\t10\t0
                            """);
        });
    }

    @Test
    public void testLateralOnConditionKeptWithNonEqualityCorrelation() throws Exception {
        // #7803 section 1
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT, y INT)");
            execute("INSERT INTO o VALUES (1, 1, 1, 0), (2, 2, 1, 0)");
            execute("CREATE TABLE p (k INT, x INT, y INT, v INT)");
            execute("INSERT INTO p VALUES (1, 1, 5, 10), (2, 1, 5, 20)");
            assertQuery("""
                    SELECT o.id, t.v FROM o JOIN LATERAL (
                        SELECT v, k jk FROM p WHERE x = o.x AND y > o.y
                    ) t ON t.jk = o.k
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            2\t20
                            """);
            assertQuery("""
                    SELECT o.id, t.v FROM o JOIN LATERAL (
                        SELECT v, k jk FROM p WHERE x = o.x AND y > o.y
                    ) t ON t.jk = 1
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            2\t10
                            """);
            assertQuery("""
                    SELECT o.id, t.v FROM o LEFT JOIN LATERAL (
                        SELECT v, k jk FROM p WHERE x = o.x AND y > o.y
                    ) t ON t.jk = o.k
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv
                            1\t10
                            2\t20
                            """);
        });
    }

    @Test
    public void testLeftJoinedCorrelatedSubQueryAfterInnerJoinWithWhereEquality() throws Exception {
        // #7725
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE t0 (id INT, k INT, x INT)");
            execute("CREATE TABLE t4 (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (401, 0, 2), (402, NULL, 1), (403, 3, NULL), (404, 0, 3), (405, NULL, 0), (406, 3, 0)");
            execute("INSERT INTO t0 VALUES (1, 1, 3), (2, 3, 3), (3, 0, NULL)");
            execute("INSERT INTO t4 VALUES (401, 0, 2), (402, NULL, 1), (403, 3, NULL), (404, 0, 3), (405, NULL, 0), (406, 3, 0)");
            assertQuery("""
                    SELECT o.id, l.i0, l.i1, l.i2 FROM o LEFT JOIN LATERAL (
                        SELECT b0.id i0, b1.id i1, b2.id i2
                        FROM t0 b0 JOIN t4 b1 ON b0.k = b1.x
                        LEFT JOIN (SELECT id, k, x FROM t4 WHERE x = o.k) b2 ON b1.x = b2.k
                        WHERE b1.x = o.x
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ti0\ti1\ti2
                            401\tnull\tnull\tnull
                            402\t1\t402\tnull
                            403\tnull\tnull\tnull
                            404\t2\t404\t406
                            405\t3\t405\tnull
                            405\t3\t406\tnull
                            406\t3\t405\t404
                            406\t3\t406\t404
                            """);
        });
    }

    @Test
    public void testLeftJoinedCorrelatedSubQueryWithWhereEqualityOnMaster() throws Exception {
        // #7725
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("INSERT INTO b VALUES (20, 5, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.k) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.k) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ON true
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinNonKeyOnReadingOuterColumnAfterCrossJoin() throws Exception {
        // #7700
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (id INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("CREATE TABLE c (k INT)");
            execute("CREATE TABLE o (id INT, k INT)");
            execute("INSERT INTO p VALUES (1), (2)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            execute("INSERT INTO b VALUES (1, 1), (2, 2)");
            execute("INSERT INTO c VALUES (1)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c LEFT JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t10\tnull
                            1\t11\t2
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a JOIN c ON c.k = a.x LEFT JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t10\tnull
                            2\t10\tnull
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c LEFT JOIN b ON a.x = b.k AND b.id = p.id + 1
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t10\tnull
                            1\t11\t2
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.pid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                            SELECT a.id aid, b.id bid FROM a JOIN c ON c.k = o.k LEFT JOIN b ON a.x = b.k AND b.id > p.id
                        ) m
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpid\taid\tbid
                            1\t1\t10\tnull
                            1\t1\t11\t2
                            1\t2\t10\tnull
                            1\t2\t11\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinOnReadingOuterColumnAndCrossJoinedTable() throws Exception {
        // #7700
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("CREATE TABLE c (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 3)");
            execute("INSERT INTO c VALUES (30, 1, 1), (31, 3, 2)");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a CROSS JOIN b LEFT JOIN c ON b.x = c.x AND a.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            1\t10\t21\tnull
                            1\t11\t20\tnull
                            1\t11\t21\tnull
                            2\t10\t20\tnull
                            2\t10\t21\tnull
                            2\t11\t20\t30
                            2\t11\t21\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a JOIN b ON a.k = b.k LEFT JOIN c ON b.x = c.x AND a.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            1\t11\t21\tnull
                            2\t10\t20\tnull
                            2\t11\t21\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a, b LEFT JOIN c ON b.x = c.x AND a.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            1\t10\t21\tnull
                            1\t11\t20\tnull
                            1\t11\t21\tnull
                            2\t10\t20\tnull
                            2\t10\t21\tnull
                            2\t11\t20\t30
                            2\t11\t21\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid, l.cid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid, c.id cid
                        FROM a JOIN b ON b.id > a.id LEFT JOIN c ON b.x = c.x AND a.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            1\t10\t21\tnull
                            1\t11\t20\tnull
                            1\t11\t21\tnull
                            2\t10\t20\tnull
                            2\t10\t21\tnull
                            2\t11\t20\t30
                            2\t11\t21\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinOnReadingOuterColumnBeforeEquatedInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT)");
            execute("CREATE TABLE t (id INT, k INT)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2), (3, NULL)");
            execute("INSERT INTO t VALUES (10, 1), (11, 2)");
            execute("INSERT INTO a VALUES (100, 1), (101, 2), (102, NULL)");
            execute("INSERT INTO b VALUES (200, 1, 10), (201, 2, 11), (202, NULL, 10)");
            final String expected = """
                    id\ttid\taid\tbid
                    1\t10\t100\t200
                    2\t11\t101\t201
                    3\t10\t102\t202
                    """;
            assertQuery("""
                    SELECT o.id, l.tid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT t.id tid, a.id aid, b.id bid FROM t LEFT JOIN a ON a.k = o.k JOIN b ON b.x = t.id WHERE b.k = o.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("""
                    SELECT o.id, l.tid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT t.id tid, a.id aid, b.id bid FROM t LEFT JOIN a ON a.k = o.k
                        LEFT JOIN b ON b.x = t.id AND b.id > 0 WHERE b.k = o.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testLeftJoinOnWithTwoOuterColumnsEquatedToOneColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE t (id INT, k INT)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 3)");
            execute("INSERT INTO t VALUES (10, 1), (11, 2)");
            execute("INSERT INTO a VALUES (100, 1), (101, 2)");
            assertQuery("""
                    SELECT o.id, l.tid, l.aid FROM o JOIN LATERAL (
                        SELECT t.id tid, a.id aid FROM t LEFT JOIN a ON a.k = o.k AND a.k = o.x WHERE t.k = o.k AND t.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\taid
                            1\t10\t100
                            """);
        });
    }

    @Test
    public void testLimitBodySelectingCorrelatedColumnOrWildcard() throws Exception {
        // #7803 section 10
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE b (bid INT, x INT)");
            execute("INSERT INTO b VALUES (20, 1), (21, 1), (22, 2)");
            assertQuery("""
                    SELECT o.id, t.bid, t.x FROM o CROSS JOIN LATERAL (
                        SELECT bid, x FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tx
                            1\t20\t1
                            2\t22\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT * FROM b WHERE x = o.x LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT * FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.x1 FROM o CROSS JOIN LATERAL (
                        SELECT bid, x + 1 x1 FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tx1
                            1\t20\t2
                            2\t22\t3
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT s.* FROM (SELECT bid FROM b WHERE x = o.x) s LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT bid FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.bx FROM o CROSS JOIN LATERAL (
                        SELECT bid, x bx FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tbx
                            1\t20\t1
                            2\t22\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.ox FROM o CROSS JOIN LATERAL (
                        SELECT bid, x, o.x ox FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tox
                            1\t20\t1
                            2\t22\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.x FROM o LEFT JOIN LATERAL (
                        SELECT bid, x FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ON true ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tx
                            1\t20\t1
                            2\t22\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT b.* FROM b WHERE x = o.x ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
        });
    }

    @Test
    public void testLimitOrderingInBody() throws Exception {
        // #7803 section 8
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE c (v INT, x INT, w INT, g INT)");
            execute("INSERT INTO c VALUES (301, 1, 5, 1), (302, 1, 3, 1), (303, 1, 7, 2), (304, 2, 9, 1), (305, 2, 1, 2), (306, 2, 4, 2), (307, 1, 1, 2)");
            assertQuery("""
                    SELECT o.id, t.v, t.w FROM o CROSS JOIN LATERAL (
                        SELECT -v AS v, w FROM (SELECT v, w FROM c WHERE x = o.x) ORDER BY v LIMIT 2
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv\tw
                            1\t-307\t1
                            1\t-303\t7
                            2\t-306\t4
                            2\t-305\t1
                            """);
            assertQuery("""
                    SELECT o.id, t.v, t.w FROM o CROSS JOIN LATERAL (
                        SELECT w AS v, v AS w FROM (SELECT v, w FROM c WHERE x = o.x) ORDER BY v LIMIT 2
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tv\tw
                            1\t1\t307
                            1\t3\t302
                            2\t1\t305
                            2\t4\t306
                            """);
            assertQuery("""
                    SELECT o.id, t.v, t.w FROM o CROSS JOIN LATERAL (
                        SELECT v, w FROM (
                            SELECT v, w FROM (SELECT v, w FROM c WHERE x = o.x) ORDER BY w LIMIT 2
                        ) LIMIT 1000
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv\tw
                            1\t302\t3
                            1\t307\t1
                            2\t305\t1
                            2\t306\t4
                            """);
            assertQuery("""
                    SELECT o.id, t.g, t.w FROM o CROSS JOIN LATERAL (
                        SELECT g, w FROM (
                            SELECT g, first(w) w FROM (
                                SELECT g, w FROM (SELECT g, w FROM c WHERE x = o.x) ORDER BY w DESC
                            ) GROUP BY g
                        ) LIMIT 1000
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tg\tw
                            1\t1\t5
                            1\t2\t7
                            2\t1\t9
                            2\t2\t4
                            """);
            assertQuery("""
                    SELECT o.id, t.v, t.w, t.rn FROM o CROSS JOIN LATERAL (
                        SELECT v, w, rn FROM (
                            SELECT v, w, row_number() OVER () rn FROM (
                                SELECT v, w FROM (SELECT v, w FROM c WHERE x = o.x) ORDER BY w
                            )
                        ) LIMIT 1000
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv\tw\trn
                            1\t301\t5\t3
                            1\t302\t3\t2
                            1\t303\t7\t4
                            1\t307\t1\t1
                            2\t304\t9\t3
                            2\t305\t1\t1
                            2\t306\t4\t2
                            """);
        });
    }

    @Test
    public void testLimitPerOuterRowOverRightJoin() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        ORDER BY r.id DESC LIMIT 1
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            2\tnull\t101
                            """);
        });
    }

    @Test
    public void testNestedLateralReadingCorrelatedSubQueryColumn() throws Exception {
        // #7803 section 15
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 2)");
            execute("CREATE TABLE d (k INT)");
            execute("INSERT INTO d VALUES (1), (1), (2)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT q.id bid, w.c FROM (SELECT id, k FROM b WHERE x = o.x) q
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = q.k) w
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid\tc
                            1\t20\t2
                            2\t21\t1
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT q.id bid, w.c FROM b q
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = q.k) w
                        WHERE q.x = o.x
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid\tc
                            1\t20\t2
                            2\t21\t1
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT q.id bid, w.c FROM a
                        JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = q.k) w
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid\tc
                            1\t20\t2
                            2\t21\t1
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT q.id bid, w.c FROM a
                        LEFT JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = q.k) w
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid\tc
                            1\tnull\t0
                            1\t20\t2
                            2\tnull\t0
                            2\t21\t1
                            """);
        });
    }

    @Test
    public void testNestedLateralWithRightOrFullJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT)");
            execute("CREATE TABLE p (id INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("INSERT INTO p VALUES (1), (5)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            execute("INSERT INTO b VALUES (5, 1), (6, 2)");
            assertQuery("""
                    SELECT o.id, l.pid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                            SELECT a.id aid, b.id bid FROM a RIGHT JOIN b ON a.x = b.k AND b.id > p.id AND b.k = o.k
                        ) m
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpid\taid\tbid
                            1\t1\tnull\t6
                            1\t1\t10\t5
                            1\t5\tnull\t5
                            1\t5\tnull\t6
                            2\t1\tnull\t5
                            2\t1\t11\t6
                            2\t5\tnull\t5
                            2\t5\t11\t6
                            """);
            assertQuery("""
                    SELECT o.id, l.pid, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                            SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.x = b.k AND b.id > p.id AND b.k = o.k
                        ) m
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tpid\taid\tbid
                            1\t1\tnull\t6
                            1\t1\t10\t5
                            1\t1\t11\tnull
                            1\t5\tnull\t5
                            1\t5\tnull\t6
                            1\t5\t10\tnull
                            1\t5\t11\tnull
                            2\t1\tnull\t5
                            2\t1\t10\tnull
                            2\t1\t11\t6
                            2\t5\tnull\t5
                            2\t5\t10\tnull
                            2\t5\t11\t6
                            """);
        });
    }

    @Test
    public void testNoInternalColumnsInWildcardOrCreateTableAs() throws Exception {
        // #7803 section 9
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE b (bid INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO b VALUES (20, 1, '2024-01-01T00:00:00.000000Z'), (21, 1, '2024-01-01T00:00:01.000000Z'), (22, 2, '2024-01-01T00:00:02.000000Z')");
            assertQuery("""
                    SELECT o.id, t.* FROM o CROSS JOIN LATERAL (
                        SELECT * FROM (SELECT bid FROM b WHERE x = o.x) ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
            assertQuery("""
                    SELECT o.id, t.* FROM o CROSS JOIN LATERAL (
                        SELECT * FROM (SELECT bid, x bx, ts FROM b WHERE x = o.x) LATEST ON ts PARTITION BY bx
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tbx\tts
                            1\t21\t1\t2024-01-01T00:00:01.000000Z
                            2\t22\t2\t2024-01-01T00:00:02.000000Z
                            """);
            assertQuery("""
                    SELECT o.id, t.* FROM o CROSS JOIN LATERAL (
                        SELECT bid FROM b WHERE x = o.x ORDER BY bid + o.x
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t20
                            1\t21
                            2\t22
                            """);
            assertQuery("""
                    SELECT * FROM o CROSS JOIN LATERAL (
                        SELECT * FROM (SELECT bid FROM b WHERE x = o.x) ORDER BY bid LIMIT 1
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tx\tbid
                            1\t1\t20
                            2\t2\t22
                            """);
            execute("""
                    CREATE TABLE ct AS (
                        SELECT o.id, t.* FROM o CROSS JOIN LATERAL (
                            SELECT * FROM (SELECT bid FROM b WHERE x = o.x) ORDER BY bid LIMIT 1
                        ) t
                    )
                    """);
            assertQuery("SELECT \"column\", type FROM table_columns('ct')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype
                            id\tINT
                            bid\tINT
                            """);
            assertQuery("SELECT * FROM ct ORDER BY 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tbid
                            1\t20
                            2\t22
                            """);
        });
    }

    @Test
    public void testNonEquiRightJoinWithCorrelatedWhere() throws Exception {
        // #7723
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 10), (2, 20)");
            execute("INSERT INTO a VALUES (1, 1, '2024-01-01T00:00:01.000000Z'), (2, 2, '2024-01-01T00:00:03.000000Z')");
            execute("INSERT INTO b VALUES (11, 1, 10), (12, 2, 20), (13, 3, 30)");
            assertQuery("""
                    SELECT o.id, t.aid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, c.id cid FROM a RIGHT JOIN b c ON c.k > a.k WHERE c.x != o.x
                    ) t ORDER BY o.id, t.cid, t.aid
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tcid
                            1\t1\t12
                            1\t1\t13
                            1\t2\t13
                            2\tnull\t11
                            2\t1\t13
                            2\t2\t13
                            """);
        });
    }

    @Test
    public void testNullRejectingFilterOnNonNullableColumnAfterRightJoin() throws Exception {
        // #7737
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT)");
            execute("CREATE TABLE trades (id INT, x INT, s SHORT, f BOOLEAN)");
            execute("CREATE TABLE refunds (id INT, k INT)");
            execute("CREATE TABLE xs (k INT, v INT)");
            execute("INSERT INTO orders VALUES (1, 1), (2, 2)");
            execute("INSERT INTO trades VALUES (10, 1, 10, true)");
            execute("INSERT INTO refunds VALUES (100, 1), (101, 2)");
            execute("INSERT INTO xs VALUES (1, 100)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM (SELECT id, x, s FROM trades) t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.s < 11
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM (SELECT id, x, s FROM trades) t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.s IS NOT NULL
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM (SELECT id, x, f FROM trades) t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.f = false
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            2\tnull\t100
                            2\tnull\t101
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM (SELECT id, x, s FROM trades) t
                        JOIN xs x ON x.k = o.k
                        RIGHT JOIN refunds r ON r.k = t.x
                        WHERE t.s < 11
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t
                        RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        WHERE t.s < 11
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
        });
    }

    @Test
    public void testOrderByLimitOverJoinCorrelatedOnJoinedTable() throws Exception {
        // #7695
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (ok INT, k INT, v INT)");
            execute("INSERT INTO o VALUES (1, 10, 1), (2, 20, 2), (3, 30, 2)");
            execute("CREATE TABLE t (ts TIMESTAMP, id INT, v INT) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:01', 1, 1), ('2024-01-01T00:00:03', 2, 2), ('2024-01-01T00:00:05', 3, 3)");
            execute("CREATE TABLE p (ts TIMESTAMP, id INT, k INT) TIMESTAMP(ts)");
            execute("INSERT INTO p VALUES ('2024-01-01T00:00:00', 1, 10), ('2024-01-01T00:00:02', 2, 20), ('2024-01-01T00:00:04', 2, 30), ('2024-01-01T00:00:06', 3, 10)");
            assertQuery("""
                    SELECT o.ok, s.id, s.pk
                    FROM o JOIN LATERAL (
                        SELECT t.id, p.k pk
                        FROM t JOIN p ON t.id = p.id
                        WHERE p.k = o.k
                        ORDER BY t.id
                        LIMIT 1
                    ) s
                    ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tpk
                            1\t1\t10
                            2\t2\t20
                            3\t2\t30
                            """);
            assertQuery("""
                    SELECT o.ok, s.id, s.pk
                    FROM o JOIN LATERAL (
                        SELECT t.id, p.k pk
                        FROM t LEFT JOIN p ON t.id = p.id
                        WHERE p.k = o.k
                        ORDER BY t.id
                        LIMIT 1
                    ) s
                    ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tpk
                            1\t1\t10
                            2\t2\t20
                            3\t2\t30
                            """);
            assertQuery("""
                    SELECT o.ok, s.id, s.k
                    FROM o JOIN LATERAL (
                        SELECT t.id, p.k
                        FROM t ASOF JOIN p ON (id)
                        WHERE p.k = o.k
                        ORDER BY t.id
                        LIMIT 1
                    ) s
                    ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            ok\tid\tk
                            1\t1\t10
                            2\t2\t20
                            """);
        });
    }

    @Test
    public void testOtherCorrelatedConditionsKeptWhenEveryOuterColumnIsEquated() throws Exception {
        // #7731
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT, y INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 5, 5)");
            execute("INSERT INTO a VALUES (10, 1), (5, 1), (3, 5), (4, 5)");
            execute("INSERT INTO c VALUES (1), (5)");
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t5
                            1\t10
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND a.id = o.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a JOIN c ON c.k = a.x AND o.x > 1 WHERE a.x = o.x
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid
                            2\t3
                            2\t4
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND o.x > 1
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            2\t3
                            2\t4
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x
                    ) l ON true ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t5
                            1\t10
                            2\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid FROM o JOIN LATERAL (
                        SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.y
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid
                            1\t5
                            1\t10
                            """);
        });
    }

    @Test
    public void testOuterJoinOnReadingOuterColumnAfterCrossJoin() throws Exception {
        // #7700
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (id INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO p VALUES (1), (2)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            execute("INSERT INTO b VALUES (1, 1), (2, 2)");
            execute("INSERT INTO c VALUES (1)");
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c RIGHT JOIN b ON a.x = b.k AND b.id > p.id WHERE a.id > 0
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t11\t2
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c FULL JOIN b ON a.x = b.k AND b.id > p.id WHERE a.id > 0
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\t10\tnull
                            1\t11\t2
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c RIGHT JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\tnull\t1
                            1\t11\t2
                            2\tnull\t1
                            2\tnull\t2
                            """);
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c FULL JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pid\taid\tbid
                            1\tnull\t1
                            1\t10\tnull
                            1\t11\t2
                            2\tnull\t1
                            2\tnull\t2
                            2\t10\tnull
                            2\t11\tnull
                            """);
        });
    }

    @Test
    public void testOuterTimestampComparedWithTimestampNs() throws Exception {
        // #7803 section 12
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, ts TIMESTAMP)");
            execute("INSERT INTO o VALUES (1, '2024-01-01T00:00:01.000000Z'), (2, '2024-01-01T00:00:02.000000Z')");
            execute("CREATE TABLE n (v INT, tn TIMESTAMP_NS)");
            execute("INSERT INTO n VALUES (10, '2024-01-01T00:00:01.000000000Z'), (20, '2024-01-01T00:00:02.000000000Z')");
            assertQuery("""
                    SELECT o.id, t.ots, t.ol, t.ts1, t.v FROM o CROSS JOIN LATERAL (
                        SELECT o.ts ots, o.ts::LONG ol, o.ts + 1 ts1, v FROM n WHERE tn = o.ts
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tots\tol\tts1\tv
                            1\t2024-01-01T00:00:01.000000Z\t1704067201000000\t2024-01-01T00:00:01.000001Z\t10
                            2\t2024-01-01T00:00:02.000000Z\t1704067202000000\t2024-01-01T00:00:02.000001Z\t20
                            """);
        });
    }

    @Test
    public void testPositionalOrderByInLimitBody() throws Exception {
        // #7803 section 17
        assertMemoryLeak(() -> {
            assertQuery("""
                    SELECT * FROM (SELECT x v FROM long_sequence(3)) t1
                    CROSS JOIN LATERAL (
                        SELECT v FROM (SELECT x v FROM long_sequence(3)) t
                        WHERE t.v <= t1.v ORDER BY 1 DESC LIMIT 1
                    ) t2 ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            v\tv1
                            1\t1
                            2\t2
                            3\t3
                            """);
            assertQuery("""
                    SELECT * FROM (SELECT x v FROM long_sequence(3)) t1
                    CROSS JOIN LATERAL (
                        SELECT v FROM (SELECT x v FROM long_sequence(3)) t
                        WHERE t.v <= t1.v ORDER BY v DESC LIMIT 1
                    ) t2 ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            v\tv1
                            1\t1
                            2\t2
                            3\t3
                            """);
            assertQuery("""
                    SELECT * FROM (SELECT x v FROM long_sequence(3)) t1
                    CROSS JOIN LATERAL (
                        SELECT v, v * 10 w FROM (SELECT x v FROM long_sequence(3)) t
                        WHERE t.v <= t1.v ORDER BY 2 DESC, 1 LIMIT 1
                    ) t2 ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            v\tv1\tw
                            1\t1\t10
                            2\t2\t20
                            3\t3\t30
                            """);
            assertQuery("""
                    SELECT * FROM (SELECT x v FROM long_sequence(3)) t1
                    LEFT JOIN LATERAL (
                        SELECT v FROM (SELECT x v FROM long_sequence(3)) t
                        WHERE t.v <= t1.v ORDER BY 1 DESC LIMIT 1
                    ) t2 ON true ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            v\tv1
                            1\t1
                            2\t2
                            3\t3
                            """);
        });
    }

    @Test
    public void testPositionalOrderByInUnionBranch() throws Exception {
        // #7785
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (sym SYMBOL, s SYMBOL, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO k VALUES ('a', 'a', 1, '2024-01-01T00:00:00.000000Z'), ('b', 'b', 2, '2024-01-01T12:00:00.000000Z'), ('a', 'a', 3, '2024-01-02T00:00:00.000000Z')");
            assertQuery("""
                    SELECT k.sym, z.l
                    FROM k
                    CROSS JOIN LATERAL (
                        SELECT l FROM k k2 WHERE k2.sym = k.sym
                        UNION ALL
                        (SELECT l FROM k k3 WHERE k3.sym = k.sym ORDER BY 1 DESC LIMIT 1)
                    ) z
                    ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym\tl
                            a\t1
                            a\t1
                            a\t3
                            a\t3
                            a\t3
                            a\t3
                            b\t2
                            b\t2
                            """);
            assertQuery("""
                    SELECT * FROM (
                        SELECT 'x' sym, 0L l
                        UNION ALL
                        SELECT k.sym, z.l
                        FROM k
                        CROSS JOIN LATERAL (
                            SELECT l FROM k k2 WHERE k2.sym = k.sym
                            UNION ALL
                            (SELECT l FROM k k3 WHERE k3.sym = k.sym ORDER BY 1 DESC LIMIT 1)
                        ) z
                    ) ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym\tl
                            a\t1
                            a\t1
                            a\t3
                            a\t3
                            a\t3
                            a\t3
                            b\t2
                            b\t2
                            x\t0
                            """);
        });
    }

    @Test
    public void testPrefixDomainJoinsAtFirstCorrelatedStep() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (id INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT)");
            execute("CREATE TABLE c (k INT)");
            assertQuery("""
                    SELECT p.id pid, m.aid, m.bid FROM p JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a CROSS JOIN c FULL JOIN b ON a.x = b.k AND b.id > p.id
                    ) m ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Sort
                              keys: [pid, aid, bid]
                              Project
                                columns: [p.id AS pid, m.aid, m.bid]
                                Join
                                  Master p
                                    Scan
                                      table: p
                                      columns: [id]
                                  INNER m
                                    keys: [m.__qdb_outer_ref__0_id = p.id]
                                    Project
                                      columns: [id AS aid, id1 AS bid, __qdb_outer_ref__0_id]
                                      Project
                                        columns: [a.id, b.id AS id1, case(__qdb_outer_ref__0.__qdb_outer_ref__marker_0 = null, b.__qdb_outer_ref__0_id, __qdb_outer_ref__0.__qdb_outer_ref__0_id) AS __qdb_outer_ref__0_id]
                                        Join
                                          Master a
                                            Scan
                                              table: a
                                              columns: [id, x]
                                          CROSS c
                                            Scan
                                              table: c
                                              columns: []
                                          CROSS __qdb_outer_ref__0
                                            Aggregate
                                              keys: [id AS __qdb_outer_ref__0_id]
                                              values: [count() AS __qdb_outer_ref__marker_0]
                                              Scan
                                                table: p
                                                columns: [id]
                                          FULL b
                                            keys: [b.k = a.x, b.__qdb_outer_ref__0_id = __qdb_outer_ref__0.__qdb_outer_ref__0_id]
                                            on: b.id > b.__qdb_outer_ref__0_id
                                            Join
                                              Master
                                                Scan
                                                  table: b
                                                  columns: [id, k]
                                              CROSS __qdb_outer_ref__0_1
                                                Aggregate
                                                  keys: [id AS __qdb_outer_ref__0_id]
                                                  values: []
                                                  Scan
                                                    table: p
                                                    columns: [id]
                            """);
        });
    }

    @Test
    public void testRightAndFullJoinInBodyMatchedRows() throws Exception {
        // #7803
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.cid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, c.id cid, s.id sid FROM a
                        FULL JOIN b c ON c.k = a.k
                        LEFT JOIN (SELECT id, k FROM b WHERE x != o.x) s ON s.k = a.k
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tcid\tsid
                            1\t10\t20\tnull
                            1\t11\t21\t21
                            2\t10\t20\t20
                            2\t11\t21\tnull
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, c.id cid FROM a
                        FULL JOIN (SELECT id, k FROM b WHERE x = o.x) c ON c.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tcid
                            1\t10\t20
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testRightJoinBodyInUnionAllBranch() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                        UNION ALL
                        SELECT -1, x.k FROM xs x WHERE x.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\t-1\t1
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t101
                            """);
        });
    }

    @Test
    public void testRightJoinedCorrelatedSubQueryWithNullOuterKey() throws Exception {
        // #7726
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, NULL)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("INSERT INTO b VALUES (20, 5, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.id = s.id
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            """);
            execute("INSERT INTO b VALUES (21, 1, 1), (22, 7, NULL)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            2\tnull\t22
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a RIGHT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.id = s.id
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            2\tnull\t22
                            """);
        });
    }

    @Test
    public void testRightJoinOnReadingOuterColumn() throws Exception {
        // #7723
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.c FROM orders o JOIN LATERAL (
                        SELECT count(*) c FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tc
                            1\t2
                            2\t2
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND t.x = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
            assertQuery("""
                    SELECT o.id, l.rid FROM orders o JOIN LATERAL (
                        SELECT r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k WHERE t.id IS NULL
                    ) l ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\trid
                            1\t101
                            2\t100
                            2\t101
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON r.k = o.k AND t.id > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\t10\t100
                            2\tnull\t100
                            2\t10\t101
                            """);
        });
    }

    @Test
    public void testRightJoinOnReadingTwoOuterColumns() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            execute("CREATE TABLE o (id INT, k INT, lim INT)");
            execute("INSERT INTO o VALUES (1, 1, 5), (2, 1, 50)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k AND t.id > o.lim
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(PER_OUTER_ROW_RIGHT_JOIN);
        });
    }

    @Test
    public void testRightJoinPlanKeepsOnAndKeysSlaveDomain() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid
                        FROM (SELECT id, x FROM trades WHERE x = o.k) t RIGHT JOIN refunds r ON t.x = r.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Sort
                              keys: [id, tid, rid]
                              Project
                                columns: [o.id, l.tid, l.rid]
                                Join
                                  Master o
                                    Scan
                                      table: orders
                                      columns: [id, k]
                                  INNER l
                                    keys: [l.__qdb_outer_ref__0_k = o.k]
                                    Project
                                      columns: [t.id AS tid, r.id AS rid, r.__qdb_outer_ref__0_k]
                                      Join
                                        Master t
                                          Project
                                            columns: [id, x, x AS __qdb_outer_ref__0_k]
                                            Scan
                                              table: trades
                                              columns: [id, x]
                                        RIGHT r
                                          keys: [r.k = t.x, r.__qdb_outer_ref__0_k = t.__qdb_outer_ref__0_k]
                                          Join
                                            Master
                                              Scan
                                                table: refunds
                                                columns: [id, k]
                                            CROSS __qdb_outer_ref__0
                                              Aggregate
                                                keys: [k AS __qdb_outer_ref__0_k]
                                                values: []
                                                Scan
                                                  table: orders
                                                  columns: [k]
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid, l.xk FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid, x.k xk
                        FROM trades t FULL JOIN refunds r ON r.k = t.x
                        LEFT JOIN (SELECT k, v FROM xs WHERE k = o.k) x ON x.v > 0
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .assertsLogicalPlan("""
                            Sort
                              keys: [id, tid, rid]
                              Project
                                columns: [o.id, l.tid, l.rid, l.xk]
                                Join
                                  Master o
                                    Scan
                                      table: orders
                                      columns: [id, k]
                                  INNER l
                                    keys: [l.__qdb_outer_ref__0_k = o.k]
                                    Project
                                      columns: [t.id AS tid, r.id AS rid, x.k AS xk, __qdb_outer_ref__0.__qdb_outer_ref__0_k]
                                      Join
                                        Master t
                                          Scan
                                            table: trades
                                            columns: [id, x]
                                        FULL r
                                          keys: [r.k = t.x]
                                          Scan
                                            table: refunds
                                            columns: [id, k]
                                        CROSS __qdb_outer_ref__0
                                          Aggregate
                                            keys: [k AS __qdb_outer_ref__0_k]
                                            values: []
                                            Scan
                                              table: orders
                                              columns: [k]
                                        LEFT x
                                          keys: [x.__qdb_outer_ref__0_k = __qdb_outer_ref__0.__qdb_outer_ref__0_k]
                                          on: x.v > 0
                                          Project
                                            columns: [k, v, k AS __qdb_outer_ref__0_k]
                                            Scan
                                              table: xs
                                              columns: [k, v]
                            """);
        });
    }

    @Test
    public void testRightJoinToSubQueryWithCorrelatedOn() throws Exception {
        // #7729
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, x INT)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, 2)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 3)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a RIGHT JOIN (SELECT id, k FROM b) s ON a.x = s.k AND s.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\tnull\t21
                            1\t10\t20
                            2\tnull\t20
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testRightOrFullJoinWithNullOuterValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, k INT)");
            execute("CREATE TABLE trades (id INT, x INT)");
            execute("CREATE TABLE refunds (id INT, k INT)");
            execute("INSERT INTO orders VALUES (1, 1), (2, 2), (3, NULL)");
            execute("INSERT INTO trades VALUES (10, 1), (11, NULL)");
            execute("INSERT INTO refunds VALUES (100, 1), (101, 2), (102, NULL)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t101
                            2\tnull\t102
                            3\tnull\t100
                            3\tnull\t101
                            3\t11\t102
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM trades t FULL JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            1\t10\t100
                            1\t11\tnull
                            2\tnull\t100
                            2\tnull\t101
                            2\tnull\t102
                            2\t10\tnull
                            2\t11\tnull
                            3\tnull\t100
                            3\tnull\t101
                            3\t10\tnull
                            3\t11\t102
                            """);
            execute("CREATE TABLE od (id INT, d DOUBLE, s SYMBOL)");
            execute("CREATE TABLE td (id INT, d DOUBLE)");
            execute("CREATE TABLE rd (id INT, s SYMBOL, d DOUBLE)");
            execute("INSERT INTO od VALUES (1, 1.5, 'a'), (2, NULL, NULL)");
            execute("INSERT INTO td VALUES (10, 1.5), (11, NULL)");
            execute("INSERT INTO rd VALUES (100, 'a', 1.5), (101, NULL, NULL), (102, 'b', 2.5)");
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM od o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM td t RIGHT JOIN rd r ON t.d = r.d AND r.s = o.s AND r.d = o.d
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            1\t10\t100
                            2\tnull\t100
                            2\tnull\t102
                            2\t11\t101
                            """);
            assertQuery("""
                    SELECT o.id, l.tid, l.rid FROM od o JOIN LATERAL (
                        SELECT t.id tid, r.id rid FROM td t FULL JOIN rd r ON t.d = r.d AND r.s = o.s AND r.d = o.d
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ttid\trid
                            1\tnull\t101
                            1\tnull\t102
                            1\t10\t100
                            1\t11\tnull
                            2\tnull\t100
                            2\tnull\t102
                            2\t10\tnull
                            2\t11\t101
                            """);
        });
    }

    @Test
    public void testSameNamedOuterColumnsOfTwoOuterTables() throws Exception {
        // #7803 section 6
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o1 (id INT, x INT)");
            execute("INSERT INTO o1 VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE o2 (id INT, x INT)");
            execute("INSERT INTO o2 VALUES (1, 5), (2, 6)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT, y INT)");
            execute("INSERT INTO b VALUES (20, 1, 1, 5), (21, 2, 2, 6)");
            assertQuery("""
                    SELECT p.id, t.aid, t.rid FROM o1 p JOIN o2 q ON q.id = p.id CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid FROM a
                        LEFT JOIN (SELECT id, k FROM b WHERE x = p.x AND y = q.x) r ON r.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid
                            1\t10\t20
                            1\t11\tnull
                            2\t10\tnull
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT p.id, t.aid, t.rid FROM o1 p JOIN o2 q ON q.id = p.id CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid FROM a
                        JOIN (SELECT id, k FROM b WHERE x = p.x AND y = q.x) r ON r.k = a.k
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid
                            1\t10\t20
                            2\t11\t21
                            """);
        });
    }

    @Test
    public void testScalarCountBeforeRightJoinInLeftLateral() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT o.id, l.rid, l.n FROM orders o LEFT JOIN LATERAL (
                        SELECT r.id rid, c.n FROM trades t
                        CROSS JOIN (SELECT count(*) n FROM xs WHERE k = o.k) c
                        RIGHT JOIN refunds r ON t.x = r.k
                    ) l ON true ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\trid\tn
                            1\t100\t1
                            1\t101\tnull
                            2\t100\t0
                            2\t101\tnull
                            """);
        });
    }

    @Test
    public void testSharedDomainReadByMoreThanOneConsumer() throws Exception {
        // #7803 section 3
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT, y INT)");
            execute("INSERT INTO b VALUES (20, 1, 1, 5), (21, 2, 2, 6)");
            execute("CREATE TABLE c (id INT, y INT)");
            execute("INSERT INTO c VALUES (30, 5), (31, 6)");
            assertQuery("""
                    SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id bid, c.id cid FROM a
                        JOIN (SELECT id, k, y FROM b WHERE x != o.x) s ON s.k = a.x
                        LEFT JOIN c ON c.y = s.y
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t11\t21\t31
                            2\t10\t20\t30
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id bid FROM a
                        JOIN (SELECT id, k, x FROM b WHERE k = o.x OR k = o.id) s ON s.x = a.x
                        WHERE s.k = o.x
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, t.cid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT c.id cid, s.id sid FROM c
                        LEFT JOIN (
                            SELECT n1.id, n1.y FROM b n1 JOIN (SELECT k FROM b WHERE k != o.x) n2 ON n2.k = n1.k
                        ) s ON s.y = c.y
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcid\tsid
                            1\t30\tnull
                            1\t31\t21
                            2\t30\t20
                            2\t31\tnull
                            """);
            assertQuery("""
                    SELECT o.id, t.cid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT c.id cid, s.id sid FROM c
                        JOIN (
                            SELECT n1.id, n1.y FROM b n1 JOIN (SELECT k FROM b WHERE k != o.x) n2 ON n2.k = n1.k
                        ) s ON s.y = c.y
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tcid\tsid
                            1\t31\t21
                            2\t30\t20
                            """);
        });
    }

    @Test
    public void testSharedDomainSourceFreedAfterExplainAndFailedCreate() throws Exception {
        // #7803 section 3
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT, y INT)");
            execute("INSERT INTO b VALUES (20, 1, 1, 5), (21, 2, 2, 6)");
            execute("CREATE TABLE c (id INT, y INT)");
            execute("INSERT INTO c VALUES (30, 5), (31, 6)");
            printSql("""
                    EXPLAIN SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id bid, c.id cid FROM a
                        JOIN (SELECT id, k, y FROM b WHERE x != o.x) s ON s.k = a.x
                        LEFT JOIN c ON c.y = s.y
                    ) t
                    """);
            TestUtils.assertContains(sink, "Hash Left Outer Join");
            final String sql = """
                    CREATE TABLE ct AS (
                        SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                            SELECT a.id aid, s.id bid, c.id cid FROM a
                            JOIN (SELECT id, k, y FROM b WHERE x != o.x) s ON s.k = a.x
                            LEFT JOIN c ON c.y = s.y
                        ) t
                    ), CAST(no_such_col AS LONG)
                    """;
            assertExceptionNoLeakCheck(sql, sql.indexOf("no_such_col"), "CAST column doesn't exist [column=no_such_col]");
        });
    }

    @Test
    public void testSharedSymbolDomainReadByMoreThanOneConsumer() throws Exception {
        // #7803 section 3
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x SYMBOL)");
            execute("INSERT INTO o VALUES (1, '1'), (2, '2')");
            execute("CREATE TABLE a (id INT, x SYMBOL)");
            execute("INSERT INTO a VALUES (10, '1'), (11, '2')");
            execute("CREATE TABLE b (id INT, k SYMBOL, x SYMBOL, y INT)");
            execute("INSERT INTO b VALUES (20, '1', '1', 5), (21, '2', '2', 6)");
            execute("CREATE TABLE c (id INT, y INT)");
            execute("INSERT INTO c VALUES (30, 5), (31, 6)");
            assertQuery("""
                    SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id bid, c.id cid FROM a
                        JOIN (SELECT id, k, y FROM b WHERE x != o.x) s ON s.k = a.x
                        LEFT JOIN c ON c.y = s.y
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t11\t21\t31
                            2\t10\t20\t30
                            """);
        });
    }

    @Test
    public void testTwoInnerJoinedCorrelatedSubQueries() throws Exception {
        // #7803 section 4
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 1, 2)");
            execute("CREATE TABLE c (id INT, k INT, x INT)");
            execute("INSERT INTO c VALUES (30, 1, 1), (31, 1, 2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.bid, t.cid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, q.id bid, r.id cid FROM a
                        JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        JOIN (SELECT id, k FROM c WHERE x = o.x) r ON r.k = q.k
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid\tcid
                            1\t10\t20\t30
                            2\t10\t21\t31
                            """);
        });
    }

    @Test
    public void testWhereEqualityOnCorrelatedFullOrRightJoinSlave() throws Exception {
        // #7725, #7726
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o2 (id INT, k INT, x INT)");
            execute("CREATE TABLE a2 (id INT, k INT, x INT)");
            execute("CREATE TABLE b2 (id INT, k INT, x INT)");
            execute("INSERT INTO o2 VALUES (1, 5, 1)");
            execute("INSERT INTO a2 VALUES (10, 9, 9), (11, 8, 8)");
            execute("INSERT INTO b2 VALUES (20, 2, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o2 o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a2 a FULL JOIN (SELECT id, k, x FROM b2 WHERE x = o.x) s ON a.k = s.x
                        WHERE s.k < o.k
                    ) l
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\tnull\t20
                            """);
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, NULL)");
            execute("INSERT INTO a VALUES (10, 1)");
            execute("INSERT INTO b VALUES (20, 5, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a FULL JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\tnull
                            """);
            execute("INSERT INTO b VALUES (21, 1, 1), (22, 7, NULL)");
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid
                        FROM a FULL JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k
                        WHERE a.x = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t21
                            2\tnull\t22
                            """);
        });
    }

    @Test
    public void testWhereEqualityOnFirstTableWithNonEqualityJoinedSubQuery() throws Exception {
        // #7803 section 13
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, k INT, x INT)");
            execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO b VALUES (20, 1, 2), (21, 2, 1)");
            execute("CREATE TABLE d (k INT)");
            execute("INSERT INTO d VALUES (1), (1), (2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id bid FROM a
                        LEFT JOIN (SELECT id, k FROM b WHERE x != o.x) r ON r.k = a.k
                        WHERE a.x = o.x
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id bid FROM a
                        JOIN (SELECT id, k FROM b WHERE x != o.x) r ON r.k = a.k
                        WHERE a.x = o.x
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\t21
                            """);
            assertQuery("""
                    SELECT o.id, t.ax, t.c FROM o CROSS JOIN LATERAL (
                        SELECT a.id + o.x ax, w.c FROM a
                        CROSS JOIN LATERAL (SELECT count(*) c FROM d WHERE d.k = a.k) w
                        WHERE a.x = o.x
                    ) t ORDER BY 1
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tax\tc
                            1\t11\t2
                            2\t13\t1
                            """);
        });
    }

    @Test
    public void testWhereEqualityOnOuterDependentLeftJoinSlave() throws Exception {
        // #7732
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("CREATE TABLE a (id INT, x INT)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, NULL)");
            execute("INSERT INTO a VALUES (10, 1), (11, 5)");
            execute("INSERT INTO b VALUES (20, 1, 1)");
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a LEFT JOIN b ON a.x = b.k AND b.k = o.x WHERE b.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE s.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a LEFT JOIN b ON a.x = b.k AND b.k = o.x WHERE b.k = o.x
                    ) l ON true ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.sid FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a LEFT JOIN (SELECT id, k, x FROM b WHERE x = o.x) s ON a.x = s.k WHERE s.k = o.x
                    ) l ON true ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tsid
                            1\t10\t20
                            2\t10\tnull
                            2\t11\tnull
                            """);
            assertQuery("""
                    SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                        SELECT a.id aid, b.id bid FROM a LEFT JOIN b ON a.x = b.k WHERE b.k = o.x
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            2\t11\tnull
                            """);
        });
    }

    @Test
    public void testWhereEqualityOnTableJoinedAfterCorrelatedSubQuery() throws Exception {
        // #7803 section 14
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, k INT)");
            execute("INSERT INTO a VALUES (10, 1), (11, 2)");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 2)");
            execute("CREATE TABLE d (id INT, k INT, x INT)");
            execute("INSERT INTO d VALUES (40, 1, 1), (41, 2, 1), (42, 2, 2)");
            assertQuery("""
                    SELECT o.id, t.aid, t.rid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid, s.id sid FROM a
                        LEFT JOIN (SELECT id, k FROM b WHERE x = o.x) r ON r.k = a.k
                        LEFT JOIN d s ON s.k = a.k
                        WHERE s.x = o.x
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid\tsid
                            1\t10\t20\t40
                            1\t11\tnull\t41
                            2\t11\t21\t42
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, s.id sid FROM a
                        LEFT JOIN d s ON s.k = a.k
                        WHERE s.x = o.x
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\tsid
                            1\t10\t40
                            1\t11\t41
                            2\t11\t42
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.rid, t.sid FROM o CROSS JOIN LATERAL (
                        SELECT a.id aid, r.id rid, s.id sid FROM a
                        JOIN (SELECT id, k FROM b WHERE x = o.x) r ON r.k = a.k
                        LEFT JOIN d s ON s.k = a.k
                        WHERE s.x = o.x
                    ) t ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\taid\trid\tsid
                            1\t10\t20\t40
                            2\t11\t21\t42
                            """);
        });
    }

    @Test
    public void testWildcardOverFullJoinHidesCarriers() throws Exception {
        assertMemoryLeak(() -> {
            createOrdersTradesRefunds();
            assertQuery("""
                    SELECT * FROM orders o JOIN LATERAL (
                        SELECT * FROM trades t FULL JOIN refunds r ON t.x = r.k AND r.k = o.k
                    ) l ORDER BY 1, 3, 5
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tk\tid1\tx\tid11\tk1
                            1\t1\tnull\tnull\t101\t2
                            1\t1\t10\t1\t100\t1
                            2\t2\tnull\tnull\t100\t1
                            2\t2\tnull\tnull\t101\t2
                            2\t2\t10\t1\tnull\tnull
                            """);
        });
    }

    @Test
    public void testWindowAndLatestOnAboveJoinedCorrelatedSubQuery() throws Exception {
        // #7803 section 7
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, x INT)");
            execute("INSERT INTO o VALUES (1, 1), (2, 2)");
            execute("CREATE TABLE a (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO a VALUES (10, 1, '2024-01-01T00:00:00.000000Z'), (11, 2, '2024-01-01T00:00:01.000000Z')");
            execute("CREATE TABLE b (id INT, k INT, x INT)");
            execute("INSERT INTO b VALUES (20, 1, 1), (21, 2, 1), (22, 1, 2), (23, 2, 2)");
            assertQuery("""
                    SELECT o.id, t.bid, t.rn FROM o CROSS JOIN LATERAL (
                        SELECT bid, row_number() OVER (ORDER BY bid) rn FROM (
                            SELECT q.id bid FROM a JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        )
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\trn
                            1\t20\t1
                            1\t21\t2
                            2\t22\t1
                            2\t23\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.bid, t.c FROM o CROSS JOIN LATERAL (
                        SELECT bid, count(*) OVER () c FROM (
                            SELECT q.id bid FROM a JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k
                        )
                    ) t ORDER BY 1, 2
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\tbid\tc
                            1\t20\t2
                            1\t21\t2
                            2\t22\t2
                            2\t23\t2
                            """);
            assertQuery("""
                    SELECT o.id, t.aid, t.bid FROM o CROSS JOIN LATERAL (
                        SELECT aid, bid FROM (
                            (SELECT a.id aid, a.k ak, q.id bid, a.ts FROM a JOIN (SELECT id, k FROM b WHERE x = o.x) q ON q.k = a.k) TIMESTAMP(ts)
                        ) LATEST ON ts PARTITION BY ak
                    ) t ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .returns("""
                            id\taid\tbid
                            1\t10\t20
                            1\t11\t21
                            2\t10\t22
                            2\t11\t23
                            """);
        });
    }

    private void assertFullJoinCarrier(String type, String value1, String value2) throws Exception {
        execute("CREATE TABLE o (id INT, v " + type + ")");
        execute("CREATE TABLE a (id INT, v " + type + ")");
        execute("CREATE TABLE b (id INT, v " + type + ")");
        execute("INSERT INTO o VALUES (1, " + value1 + "), (2, " + value2 + ")");
        execute("INSERT INTO a VALUES (10, " + value1 + ")");
        execute("INSERT INTO b VALUES (20, " + value1 + "), (21, " + value2 + ")");
        assertQuery("""
                SELECT o.id, l.aid, l.bid FROM o JOIN LATERAL (
                    SELECT a.id aid, b.id bid FROM a FULL JOIN b ON a.v = b.v AND b.v = o.v
                ) l ORDER BY 1, 2, 3
                """)
                .noLeakCheck()
                .expectSize()
                .returns(FULL_JOIN_CARRIER_ROWS);
        execute("DROP TABLE o");
        execute("DROP TABLE a");
        execute("DROP TABLE b");
    }

    private void assertNullOuterValueFindsDomainRow(String type, String value1, String value2) throws Exception {
        execute("CREATE TABLE o (id INT, x INT)");
        execute("INSERT INTO o VALUES (1, 1), (2, 2)");
        execute("CREATE TABLE c (v " + type + ", x INT)");
        execute("INSERT INTO c VALUES (" + value1 + ", 1)");
        execute("CREATE TABLE d (id INT, v " + type + ")");
        execute("INSERT INTO d VALUES (1, " + value1 + "), (2, " + value2 + ")");
        assertQuery("""
                SELECT o.id, t.did FROM o LEFT JOIN c ON c.x = o.x CROSS JOIN LATERAL (
                    SELECT d.id did FROM d WHERE d.v != c.v
                ) t ORDER BY 1, 2
                """)
                .noLeakCheck()
                .expectSize()
                .returns("""
                        id\tdid
                        1\t2
                        2\t1
                        2\t2
                        """);
        execute("DROP TABLE o");
        execute("DROP TABLE c");
        execute("DROP TABLE d");
    }

    private void assertSpliceCorrelationRejected(String sql) throws Exception {
        assertQuery(sql).noLeakCheck().fails(sql.indexOf("SPLICE"), "outer column reference at or before a SPLICE join is not supported in a LATERAL sub-query");
    }

    private void createOrdersTradesRefunds() throws Exception {
        execute("CREATE TABLE orders (id INT, k INT)");
        execute("CREATE TABLE trades (id INT, x INT)");
        execute("CREATE TABLE refunds (id INT, k INT)");
        execute("CREATE TABLE xs (k INT, v INT)");
        execute("INSERT INTO orders VALUES (1, 1), (2, 2)");
        execute("INSERT INTO trades VALUES (10, 1)");
        execute("INSERT INTO refunds VALUES (100, 1), (101, 2)");
        execute("INSERT INTO xs VALUES (1, 100)");
    }
}
