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
import org.junit.Test;

/**
 * Correlates a LATERAL body with its master through a non-equality, so decorrelation builds its domain from
 * a copy of the master input or prefix; each master is a different kind of plan.
 */
public class LateralDomainCopyTest extends AbstractCairoTest {

    @Test
    public void testDomainCopiesAggregates() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.sym, l.c FROM (SELECT sym, sum(x) x FROM t GROUP BY sym) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 30) l ORDER BY m.sym")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            sym\tc
                            \t0
                            a\t1
                            b\t2
                            """);
            assertQuery("SELECT m.x, l.c FROM (SELECT max(x) x FROM t) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 30) l")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            x\tc
                            3.5\t1
                            """);
            assertQuery("SELECT m.k, l.c FROM (SELECT DISTINCT k FROM t) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.k * 100) l ORDER BY m.k")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            1\t1
                            2\t0
                            3\t0
                            """);
            assertQuery("SELECT m.k, m.sym, l.c FROM (SELECT DISTINCT k, sym FROM t WHERE x > 1 ORDER BY k LIMIT 2) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.k * 100) l ORDER BY m.k")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tsym\tc
                            1\ta\t1
                            2\tb\t0
                            """);
            assertQuery("SELECT m.n, l.c FROM (SELECT DISTINCT count() n FROM t GROUP BY sym) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.n * 60) l ORDER BY m.n")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            n\tc
                            1\t2
                            2\t1
                            """);
        });
    }

    @Test
    public void testDomainCopiesDecorrelatedSteps() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT t.k, l2.c FROM t CROSS JOIN LATERAL (SELECT max(y) my FROM u WHERE u.y > t.x * 50) l1 CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.px > l1.my / 10) l2 ORDER BY t.ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            1\t1
                            2\t1
                            1\t1
                            3\t0
                            """);
            assertQuery("SELECT t.k, l2.c FROM t JOIN LATERAL (SELECT y, k FROM u WHERE u.k = t.k) l1 ON l1.k = t.k CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.px > l1.y / 10) l2 ORDER BY t.ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            1\t2
                            2\t1
                            1\t2
                            """);
            assertQuery("SELECT t.k, l2.c FROM t JOIN LATERAL (SELECT y FROM u WHERE u.k = t.k) l1 ON l1.y = t.k * 100 CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.px > l1.y / 10) l2 ORDER BY t.ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            1\t2
                            2\t1
                            1\t2
                            """);
        });
    }

    @Test
    public void testDomainCopiesJoinPrefixes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT t.k, l.c FROM t JOIN u ON t.k = u.k CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.px > t.x AND q.px < u.y) l ORDER BY t.ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            1\t3
                            2\t3
                            1\t3
                            """);
            assertQuery("SELECT t.ts, l.c FROM t ASOF JOIN q ON (sym) CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > t.x + q.px * 5) l ORDER BY t.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:01.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t1
                            2024-01-01T00:00:07.000000Z\t0
                            """);
            assertQuery("SELECT t.ts, l.c FROM t ASOF JOIN q ON (sym) TOLERANCE 1s CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > t.x + q.px * 5) l ORDER BY t.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:01.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t1
                            2024-01-01T00:00:07.000000Z\t0
                            """);
            assertQuery("SELECT t.ts, l.c FROM t LT JOIN q ON (sym) CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > t.x + q.px * 5) l ORDER BY t.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:01.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t1
                            2024-01-01T00:00:07.000000Z\t0
                            """);
            assertQuery("SELECT t.ts, l.c FROM t SPLICE JOIN q CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > t.x + q.px * 5) l ORDER BY t.ts, l.c")
                    .timestamp("ts")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:00.000000Z\t2
                            2024-01-01T00:00:01.000000Z\t1
                            2024-01-01T00:00:01.000000Z\t1
                            2024-01-01T00:00:01.000000Z\t2
                            2024-01-01T00:00:03.000000Z\t1
                            2024-01-01T00:00:07.000000Z\t0
                            """);
            assertQuery("SELECT t.k, u.y, l.c FROM t CROSS JOIN u CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.px > u.y / 10 + t.k) l ORDER BY t.ts, u.ts")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\ty\tc
                            1\t100.0\t2
                            1\t200.0\t1
                            2\t100.0\t2
                            2\t200.0\t1
                            1\t100.0\t2
                            1\t200.0\t1
                            3\t100.0\t2
                            3\t200.0\t1
                            """);
        });
    }

    @Test
    public void testDomainCopiesJoinSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.k, l.c FROM (SELECT t.k, u.y FROM t JOIN u ON t.k = u.k) m CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.px > m.y / 10) l ORDER BY m.k")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            1\t2
                            1\t2
                            2\t1
                            """);
            assertQuery("SELECT m.ts, l.c FROM (SELECT t.ts, t.x, q.px FROM t ASOF JOIN q ON (sym)) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.px * 5) l ORDER BY m.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:01.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t1
                            2024-01-01T00:00:07.000000Z\t0
                            """);
            assertQuery("SELECT m.ts, l.c FROM (SELECT t.ts, q.px FROM t LT JOIN q ON (sym)) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.px * 5) l ORDER BY m.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:01.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t1
                            2024-01-01T00:00:07.000000Z\t0
                            """);
            assertQuery("SELECT m.ts, l.c FROM (SELECT t.ts, q.px FROM t SPLICE JOIN q) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.px * 5) l ORDER BY m.ts, l.c")
                    .timestamp("ts")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:00.000000Z\t2
                            2024-01-01T00:00:01.000000Z\t1
                            2024-01-01T00:00:01.000000Z\t1
                            2024-01-01T00:00:01.000000Z\t2
                            2024-01-01T00:00:03.000000Z\t1
                            2024-01-01T00:00:07.000000Z\t1
                            """);
            assertQuery("SELECT m.k, m.y, l.c FROM (SELECT t.k, u.y FROM t CROSS JOIN u) m CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.px > m.y / 10 + m.k) l ORDER BY m.k, m.y")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\ty\tc
                            1\t100.0\t2
                            1\t100.0\t2
                            1\t200.0\t1
                            1\t200.0\t1
                            2\t100.0\t2
                            2\t200.0\t1
                            3\t100.0\t2
                            3\t200.0\t1
                            """);
        });
    }

    @Test
    public void testDomainCopiesPivot() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.a, l.c FROM (SELECT * FROM t PIVOT (sum(x) FOR sym IN ('a', 'b'))) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.a * 20) l")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            a\tc
                            5.0\t1
                            """);
        });
    }

    @Test
    public void testDomainCopiesSampleByAndFill() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.ts, l.c FROM (SELECT ts, sum(x) x FROM t SAMPLE BY 2s FILL(PREV)) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 50) l ORDER BY m.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:02.000000Z\t1
                            2024-01-01T00:00:04.000000Z\t1
                            2024-01-01T00:00:06.000000Z\t0
                            """);
            assertQuery("SELECT m.ts, l.c FROM (SELECT ts, sum(x) x FROM t SAMPLE BY 2s FILL(LINEAR)) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 50) l ORDER BY m.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:02.000000Z\t1
                            2024-01-01T00:00:04.000000Z\t0
                            2024-01-01T00:00:06.000000Z\t0
                            """);
            assertQuery("SELECT m.ts, l.c FROM (SELECT ts, avg(x) x FROM t SAMPLE BY 2s ALIGN TO FIRST OBSERVATION) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 50) l ORDER BY m.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T00:00:02.000000Z\t1
                            2024-01-01T00:00:06.000000Z\t0
                            """);
        });
    }

    @Test
    public void testDomainCopiesScanFilterProjectSortLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.k, l.c FROM t m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 50) l ORDER BY m.ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            1\t2
                            2\t1
                            1\t1
                            3\t0
                            """);
            assertQuery("SELECT m.k, l.c FROM (SELECT * FROM t WHERE x > 2) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 50) l ORDER BY m.ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            2\t1
                            1\t1
                            """);
            assertQuery("SELECT m.k, l.c FROM (SELECT ts, k, x * 2 x2 FROM t) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x2 * 25) l ORDER BY m.ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            1\t2
                            2\t1
                            1\t1
                            3\t0
                            """);
            assertQuery("SELECT m.k, l.c FROM (SELECT k, x FROM t ORDER BY x DESC LIMIT 3) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 50) l ORDER BY m.k, l.c")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tc
                            1\t1
                            2\t1
                            3\t0
                            """);
        });
    }

    @Test
    public void testDomainCopiesSetOperations() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.k, m.x, l.c FROM (SELECT k, x FROM t UNION ALL SELECT k, y FROM u) m CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.px > m.x / 10) l ORDER BY m.k, m.x")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tx\tc
                            1\t1.5\t3
                            1\t3.5\t3
                            1\t100.0\t2
                            2\t2.5\t3
                            2\t200.0\t1
                            3\tnull\t0
                            """);
            assertQuery("SELECT m.sym, l.c FROM (SELECT sym FROM t UNION SELECT sym FROM q) m CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.sym <> m.sym) l ORDER BY m.sym")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            sym\tc
                            \t3
                            a\t1
                            b\t2
                            """);
            assertQuery("SELECT m.sym, l.c FROM (SELECT sym FROM t EXCEPT SELECT sym FROM q WHERE px > 15) m CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.sym <> m.sym) l ORDER BY m.sym")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            sym\tc
                            \t3
                            """);
            assertQuery("SELECT m.sym, l.c FROM (SELECT sym FROM t INTERSECT SELECT sym FROM q) m CROSS JOIN LATERAL (SELECT count() c FROM q WHERE q.sym <> m.sym) l ORDER BY m.sym")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            sym\tc
                            a\t1
                            b\t2
                            """);
        });
    }

    @Test
    public void testDomainCopiesTableFunctions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.x, l.c FROM long_sequence(4) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 50) l ORDER BY m.x")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            x\tc
                            1\t2
                            2\t1
                            3\t1
                            4\t0
                            """);
            assertQuery("SELECT m.v, l.c FROM (SELECT x * 60 v FROM long_sequence(4) WHERE x > 1) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.v) l ORDER BY m.v")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            v\tc
                            120\t1
                            180\t1
                            240\t0
                            """);
        });
    }

    @Test
    public void testDomainCopiesTemporalJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.ts, l.c FROM (SELECT t.ts, t.sym, sum(q.px) s FROM t WINDOW JOIN q ON (t.sym = q.sym) RANGE BETWEEN 2 SECOND PRECEDING AND CURRENT ROW) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.s * 5) l ORDER BY m.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-01T00:00:01.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t0
                            2024-01-01T00:00:07.000000Z\t0
                            """);
            assertQuery("SELECT m.sym, l.c FROM (SELECT t.sym, avg(q.px) a FROM t HORIZON JOIN q ON (t.sym = q.sym) RANGE FROM -2s TO 2s STEP 1s AS h) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.a * 5) l ORDER BY m.sym")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            sym\tc
                            \t0
                            a\t2
                            b\t1
                            """);
        });
    }

    @Test
    public void testDomainCopiesUnnestPrefix() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT t.k, v.value, l.c FROM t, UNNEST(t.arr) v(value) CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > v.value * 50) l ORDER BY t.ts, v.value")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tvalue\tc
                            1\t1.0\t2
                            1\t2.0\t1
                            2\t3.0\t1
                            3\t4.0\t0
                            3\t5.0\t0
                            3\t6.0\t0
                            """);
            assertQuery("SELECT t.k, v.n, l.c FROM t, UNNEST(t.arr) WITH ORDINALITY v(value, n) CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > v.value * 50 + v.n) l ORDER BY t.ts, v.n")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\tn\tc
                            1\t1\t2
                            1\t2\t1
                            2\t1\t1
                            3\t1\t0
                            3\t2\t0
                            3\t3\t0
                            """);
        });
    }

    @Test
    public void testDomainCopiesWindowAndLatestBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.k, m.rn, l.c FROM (SELECT ts, k, x, row_number() OVER (PARTITION BY sym ORDER BY ts) rn FROM t) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.rn * 100) l ORDER BY m.ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            k\trn\tc
                            1\t1\t1
                            2\t1\t1
                            1\t2\t0
                            3\t1\t1
                            """);
            assertQuery("SELECT m.sym, l.c FROM (SELECT * FROM t LATEST ON ts PARTITION BY sym) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.y > m.x * 50) l ORDER BY m.sym")
                    .noLeakCheck()
                    .withPlanContaining("__qdb_outer_ref__")
                    .returns("""
                            sym\tc
                            \t0
                            a\t1
                            b\t1
                            """);
        });
    }

    private static void createTables() throws SqlException {
        execute("""
                CREATE TABLE t (ts TIMESTAMP, k INT, sym SYMBOL, x DOUBLE, arr DOUBLE[])
                TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("""
                INSERT INTO t VALUES
                ('2024-01-01T00:00:00', 1, 'a', 1.5, ARRAY[1.0, 2.0]),
                ('2024-01-01T00:00:01', 2, 'b', 2.5, ARRAY[3.0]),
                ('2024-01-01T00:00:03', 1, 'a', 3.5, null),
                ('2024-01-01T00:00:07', 3, null, null, ARRAY[4.0, 5.0, 6.0])
                """);
        execute("CREATE TABLE q (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO q VALUES
                ('2024-01-01T00:00:00.5', 'a', 10.0),
                ('2024-01-01T00:00:02', 'b', 20.0),
                ('2024-01-01T00:00:02.5', 'a', 30.0)
                """);
        execute("CREATE TABLE u (ts TIMESTAMP, k INT, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO u VALUES ('2024-01-01T00:00:00', 1, 100.0), ('2024-01-01T00:00:05', 2, 200.0)");
    }
}
