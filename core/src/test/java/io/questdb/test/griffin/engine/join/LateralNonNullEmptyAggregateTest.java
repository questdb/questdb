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

public class LateralNonNullEmptyAggregateTest extends AbstractCairoTest {
    private static final String AGGREGATES = "isOrdered(v) o, isOrdered(ip) i, haversine_dist_deg(lat, lon, ts) h";
    private static final String EXPECTED = """
            k\to\ti\th
            1\tfalse\tfalse\t155.94336875296912
            2\ttrue\ttrue\t0.0
            3\ttrue\ttrue\t0.0
            """;

    @Test
    public void testCountNextToMarkedAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.k, l.c, l.o FROM a, LATERAL (SELECT count() c, isOrdered(v) o FROM b WHERE b.k = a.k) l ORDER BY a.k")
                    .noLeakCheck()
                    .withPlanContaining("functions: [a.k,coalesce(l.c,0),case([l.__qdb_row_marker__0,l.o,true])]")
                    .returns("""
                            k\tc\to
                            1\t2\tfalse
                            2\t0\ttrue
                            3\t1\ttrue
                            """);
        });
    }

    @Test
    public void testCountAndSumCarryNoRowMarker() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.k, l.c, l.s FROM a, LATERAL (SELECT count() c, sum(v) s FROM b WHERE b.k = a.k) l ORDER BY a.k")
                    .noLeakCheck()
                    .withPlanContaining("functions: [a.k,coalesce(l.c,0),l.s]")
                    .returns("""
                            k\tc\ts
                            1\t2\t8
                            2\t0\tnull
                            3\t1\t7
                            """);
        });
    }

    @Test
    public void testCrossLateralRestoresEmptyValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.k, l.o, l.i, l.h FROM a, LATERAL (SELECT " + AGGREGATES + " FROM b WHERE b.k = a.k) l ORDER BY a.k")
                    .noLeakCheck()
                    .returns(EXPECTED);
            assertQuery("SELECT a.k, l.o, l.i, l.h FROM a CROSS JOIN LATERAL (SELECT " + AGGREGATES + " FROM b WHERE b.k = a.k) l ORDER BY a.k")
                    .noLeakCheck()
                    .returns(EXPECTED);
        });
    }

    @Test
    public void testDirectAggregatesAgreeWithLateral() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    """
                            SELECT 1 k, * FROM (SELECT isOrdered(v) o, isOrdered(ip) i, haversine_dist_deg(lat, lon, ts) h FROM b WHERE k = 1)
                            UNION ALL
                            SELECT 2 k, * FROM (SELECT isOrdered(v) o, isOrdered(ip) i, haversine_dist_deg(lat, lon, ts) h FROM b WHERE k = 2)
                            UNION ALL
                            SELECT 3 k, * FROM (SELECT isOrdered(v) o, isOrdered(ip) i, haversine_dist_deg(lat, lon, ts) h FROM b WHERE k = 3)
                            """,
                    "SELECT a.k, l.o, l.i, l.h FROM a, LATERAL (SELECT " + AGGREGATES + " FROM b WHERE b.k = a.k) l ORDER BY a.k"
            );
        });
    }

    @Test
    public void testExpressionsOverAggregatesRestoreEmptyValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT a.k, l.n, l.h1, l.c, l.e FROM a, LATERAL (
                        SELECT NOT isOrdered(v) n, haversine_dist_deg(lat, lon, ts) + 1 h1, count() c, isOrdered(v) AND count() = 0 e
                        FROM b WHERE b.k = a.k
                    ) l ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tn\th1\tc\te
                            1\ttrue\t156.94336875296912\t2\tfalse
                            2\tfalse\t1.0\t0\ttrue
                            3\tfalse\t1.0\t1\tfalse
                            """);
        });
    }

    @Test
    public void testInnerLateralOnReadsRestoredValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.k, l.o, l.h FROM a JOIN LATERAL (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k) l ON l.o = (a.k > 0) ORDER BY a.k")
                    .noLeakCheck()
                    .returns("""
                            k\to\th
                            2\ttrue\t0.0
                            3\ttrue\t0.0
                            """);
        });
    }

    @Test
    public void testLeftLateralOnReadsRestoredValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.k, l.o, l.h FROM a LEFT JOIN LATERAL (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k) l ON l.o = (a.k > 0) ORDER BY a.k")
                    .noLeakCheck()
                    .returns("""
                            k\to\th
                            1\tfalse\tnull
                            2\ttrue\t0.0
                            3\ttrue\t0.0
                            """);
        });
    }

    @Test
    public void testLeftLateralRestoresEmptyValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.k, l.o, l.i, l.h FROM a LEFT JOIN LATERAL (SELECT " + AGGREGATES + " FROM b WHERE b.k = a.k) l ON true ORDER BY a.k")
                    .noLeakCheck()
                    .returns(EXPECTED);
        });
    }

    @Test
    public void testLimitGuardsRestoredValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.k, l.o, l.h FROM a, LATERAL (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k LIMIT 1) l ORDER BY a.k")
                    .noLeakCheck()
                    .returns("""
                            k\to\th
                            1\tfalse\t155.94336875296912
                            2\ttrue\t0.0
                            3\ttrue\t0.0
                            """);
            assertQuery("SELECT a.k, l.o, l.h FROM a, LATERAL (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k LIMIT 0) l ORDER BY a.k")
                    .noLeakCheck()
                    .returns("k\to\th\n");
            assertQuery("SELECT a.k, l.o, l.h FROM a LEFT JOIN LATERAL (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k LIMIT 0) l ON true ORDER BY a.k")
                    .noLeakCheck()
                    .returns("""
                            k\to\th
                            1\tfalse\tnull
                            2\tfalse\tnull
                            3\tfalse\tnull
                            """);
        });
    }

    @Test
    public void testNestedJoinInputRestoresEmptyValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT a.k, l.o, l.h, l.x FROM a LEFT JOIN LATERAL (
                        SELECT s.o, s.h, y.x FROM (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k) s
                        CROSS JOIN long_sequence(1) y
                    ) l ON true ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\to\th\tx
                            1\tfalse\t155.94336875296912\t1
                            2\ttrue\t0.0\t1
                            3\ttrue\t0.0\t1
                            """);
            assertQuery("""
                    SELECT a.k, l.o, l.c, l.x FROM a LEFT JOIN LATERAL (
                        SELECT s.o, s.c, y.x FROM (SELECT isOrdered(v) o, count() c FROM b WHERE b.k = a.k) s
                        CROSS JOIN long_sequence(1) y
                    ) l ON true ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\to\tc\tx
                            1\tfalse\t2\t1
                            2\ttrue\t0\t1
                            3\ttrue\t1\t1
                            """);
        });
    }

    @Test
    public void testNullOnEmptyAggregatesNextToMarkedAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT a.k, l.s, l.o, l.w, l.z, l.s1 FROM a, LATERAL (
                        SELECT sum(v) s, isOrdered(v) o, CASE WHEN isOrdered(v) THEN sum(v) ELSE -1 END w, coalesce(sum(v), 0) z, sum(v) + 1 s1
                        FROM b WHERE b.k = a.k
                    ) l ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\ts\to\tw\tz\ts1
                            1\t8\tfalse\t-1\t8\t9
                            2\tnull\ttrue\tnull\t0\tnull
                            3\t7\ttrue\t7\t7\t8
                            """);
        });
    }

    @Test
    public void testOuterColumnNextToMarkedAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.k, l.kk, l.o FROM a, LATERAL (SELECT a.k kk, isOrdered(v) o FROM b WHERE b.k = a.k) l ORDER BY a.k")
                    .noLeakCheck()
                    .returns("""
                            k\tkk\to
                            1\t1\tfalse
                            2\t2\ttrue
                            3\t3\ttrue
                            """);
        });
    }

    @Test
    public void testRightJoinAfterLateralKeepsNullExtendedRows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE c (k INT)");
            execute("INSERT INTO c VALUES (2), (9)");
            assertQuery("""
                    SELECT c.k ck, a.k, l.o, l.h FROM a CROSS JOIN LATERAL (
                        SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k
                    ) l RIGHT JOIN c ON c.k = a.k ORDER BY c.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            ck\tk\to\th
                            2\t2\ttrue\t0.0
                            9\tnull\tfalse\tnull
                            """);
        });
    }

    @Test
    public void testSetOperationBranchRestoresEmptyValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT a.k, l.o FROM a, LATERAL (
                        SELECT isOrdered(v) o FROM b WHERE b.k = a.k
                        UNION ALL
                        SELECT false o FROM long_sequence(1)
                    ) l ORDER BY a.k, l.o
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\to
                            1\tfalse
                            1\tfalse
                            2\tfalse
                            2\ttrue
                            3\tfalse
                            3\ttrue
                            """);
        });
    }

    @Test
    public void testWhereReadsRestoredValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.k, l.h FROM a, LATERAL (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k) l WHERE l.o ORDER BY a.k")
                    .noLeakCheck()
                    .returns("""
                            k\th
                            2\t0.0
                            3\t0.0
                            """);
            assertQuery("SELECT a.k, l.h FROM a, LATERAL (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k) l WHERE NOT l.o ORDER BY a.k")
                    .noLeakCheck()
                    .returns("""
                            k\th
                            1\t155.94336875296912
                            """);
            assertQuery("SELECT a.k, l.o FROM a, LATERAL (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k) l WHERE l.h = 0 ORDER BY a.k")
                    .noLeakCheck()
                    .returns("""
                            k\to
                            2\ttrue
                            3\ttrue
                            """);
        });
    }

    @Test
    public void testWrappedScalarRestoresEmptyValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT a.k, l.o, l.h FROM a, LATERAL (
                        SELECT o, h FROM (SELECT isOrdered(v) o, haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k)
                    ) l ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\to\th
                            1\tfalse\t155.94336875296912
                            2\ttrue\t0.0
                            3\ttrue\t0.0
                            """);
            assertQuery("""
                    SELECT a.k, l.d FROM a, LATERAL (
                        SELECT h * 2 d FROM (SELECT haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k)
                    ) l ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\td
                            1\t311.88673750593824
                            2\t0.0
                            3\t0.0
                            """);
            assertQuery("""
                    SELECT a.k, l.d FROM a, LATERAL (
                        SELECT h + h d FROM (SELECT haversine_dist_deg(lat, lon, ts) h FROM b WHERE b.k = a.k)
                    ) l ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\td
                            1\t311.88673750593824
                            2\t0.0
                            3\t0.0
                            """);
            assertQuery("""
                    SELECT a.k, l.s, l.o FROM a, LATERAL (
                        SELECT s, o FROM (SELECT sum(v) s, isOrdered(v) o FROM b WHERE b.k = a.k)
                    ) l ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ts\to
                            1\t8\tfalse
                            2\tnull\ttrue
                            3\t7\ttrue
                            """);
            assertQuery("""
                    SELECT a.k, l.d FROM a, LATERAL (
                        SELECT s + s d FROM (SELECT sum(v) s FROM b WHERE b.k = a.k)
                    ) l ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\td
                            1\t16
                            2\tnull
                            3\t14
                            """);
        });
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE a (k INT)");
        execute("INSERT INTO a VALUES (1), (2), (3)");
        execute("CREATE TABLE b (k INT, v LONG, ip IPV4, lat DOUBLE, lon DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO b VALUES
                    (1, 5, '1.1.1.5', 10.0, 20.0, '2024-01-01'),
                    (1, 3, '1.1.1.3', 11.0, 21.0, '2024-01-02'),
                    (3, 7, '1.1.1.7', 10.0, 20.0, '2024-01-03')
                """);
    }
}
