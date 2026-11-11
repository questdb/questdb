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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.UnaryOperator;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;

/**
 * Feeds fused hash join GROUP BY output to other operators: joins of every kind, set operations,
 * window functions, DISTINCT and outer filters, and runs it under CTEs, views, DECLARE, CTAS and
 * INSERT AS SELECT. Every query runs with the fused path off and on, and both plans must return the
 * same rows.
 */
public class HashJoinGroupByCompositionTest extends AbstractCairoTest {
    // The INNER join builds p, which has fewer rows than r, so country comes from the build's payload
    // and site from the probe.
    private static final String BY_COUNTRY = "SELECT p.country, sum(r.energy_kwh) e, count(*) n FROM r JOIN p ON r.plant_id = p.plant_id";
    private static final String BY_COUNTRY_LEFT = "SELECT p.country, sum(r.energy_kwh) e, count(*) n FROM r LEFT JOIN p ON r.plant_id = p.plant_id";
    private static final String BY_SITE = "SELECT r.site, sum(p.installed_kwp) kwp, count(*) n FROM r JOIN p ON r.plant_id = p.plant_id";
    private static final String SAMPLED = "SELECT r.reading_ts, sum(r.energy_kwh) e FROM r JOIN p ON r.plant_id = p.plant_id SAMPLE BY 12h";
    private static final String SAMPLED_BY_COUNTRY = "SELECT r.reading_ts, p.country, sum(r.energy_kwh) e FROM r JOIN p ON r.plant_id = p.plant_id SAMPLE BY 12h";
    private static final UnaryOperator<QueryAssertion> SIZED = QueryAssertion::expectSize;
    private static final UnaryOperator<QueryAssertion> UNSIZED = UnaryOperator.identity();

    // A fused SAMPLE BY sorts its rows by their timestamp, as the master of an ASOF, LT or SPLICE join needs.
    @Test
    public void testAsOfLtAndSpliceJoinsOverFusedSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                assertComposed(
                        context,
                        "SELECT s.reading_ts, s.e, c.region FROM (" + SAMPLED + ") s ASOF JOIN c",
                        1,
                        q -> q.noRandomAccess().expectSize().timestamp("reading_ts"),
                        """
                                reading_ts\te\tregion
                                2020-01-01T00:00:00.000000Z\t60.0\tcentral
                                2020-01-02T00:00:00.000000Z\t60.0\tsouth
                                2020-01-02T12:00:00.000000Z\t40.0\tnone
                                2020-01-03T00:00:00.000000Z\t56.0\tnone
                                """
                );
                assertComposed(
                        context,
                        "SELECT s.reading_ts, s.country, s.e, c.region FROM (" + SAMPLED_BY_COUNTRY
                                + ") s ASOF JOIN c ON (country) ORDER BY s.reading_ts, s.country",
                        1,
                        q -> q.expectSize().timestamp("reading_ts"),
                        """
                                reading_ts\tcountry\te\tregion
                                2020-01-01T00:00:00.000000Z\tES\t20.0\t
                                2020-01-01T00:00:00.000000Z\tIT\t40.0\t
                                2020-01-02T00:00:00.000000Z\tES\t40.0\t
                                2020-01-02T00:00:00.000000Z\tIT\t20.0\tsouth
                                2020-01-02T12:00:00.000000Z\t\t40.0\tnone
                                2020-01-03T00:00:00.000000Z\t\t1.0\tnone
                                2020-01-03T00:00:00.000000Z\tES\t50.0\tsouth
                                2020-01-03T00:00:00.000000Z\tFR\t5.0\twest
                                """
                );
                assertComposed(
                        context,
                        "SELECT s.reading_ts, s.e, c.region FROM (" + SAMPLED + ") s LT JOIN c",
                        1,
                        q -> q.noRandomAccess().expectSize().timestamp("reading_ts"),
                        """
                                reading_ts\te\tregion
                                2020-01-01T00:00:00.000000Z\t60.0\tcentral
                                2020-01-02T00:00:00.000000Z\t60.0\twest
                                2020-01-02T12:00:00.000000Z\t40.0\tsouth
                                2020-01-03T00:00:00.000000Z\t56.0\tnone
                                """
                );
                assertComposed(
                        context,
                        "SELECT s.reading_ts, s.e, c.region FROM (" + SAMPLED + ") s SPLICE JOIN c",
                        1,
                        QueryAssertion::noRandomAccess,
                        """
                                reading_ts\te\tregion
                                \tnull\tcentral
                                2020-01-01T00:00:00.000000Z\t60.0\tcentral
                                2020-01-01T00:00:00.000000Z\t60.0\twest
                                2020-01-02T00:00:00.000000Z\t60.0\tsouth
                                2020-01-02T00:00:00.000000Z\t60.0\tsouth
                                2020-01-02T12:00:00.000000Z\t40.0\tnone
                                2020-01-03T00:00:00.000000Z\t56.0\tnone
                                """
                );
                // The fused SAMPLE BY as the slave of an ASOF join.
                assertComposed(
                        context,
                        "SELECT c.ts, c.country, s.e FROM c ASOF JOIN (" + SAMPLED + ") s",
                        1,
                        q -> q.noRandomAccess().expectSize().timestamp("ts"),
                        """
                                ts\tcountry\te
                                2019-12-31T00:00:00.000000Z\tDE\tnull
                                2020-01-01T12:00:00.000000Z\tFR\t60.0
                                2020-01-02T00:00:00.000000Z\tIT\t60.0
                                2020-01-02T06:00:00.000000Z\tES\t60.0
                                2020-01-02T12:00:00.000000Z\t\t40.0
                                """
                );
            }
        });
    }

    // CTAS and INSERT AS SELECT compile their SELECT as a query does, so it fuses, and the rows it
    // writes match those of the ordinary plan.
    @Test
    public void testCreateTableAsSelectAndInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                assertQuery("CREATE TABLE plan_check AS (" + BY_COUNTRY + ")")
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlanContaining("Create table: plan_check", "Async Hash Join Group By");
                execute("CREATE TABLE by_site (site SYMBOL, kwp DOUBLE, n LONG)");
                assertQuery("INSERT INTO by_site " + BY_SITE)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlanContaining("Insert into table: by_site", "Async Hash Join Group By");
                for (String mode : new String[]{"ordinary", "fused"}) {
                    context.setParallelHashJoinGroupByEnabled(mode.equals("fused"));
                    try {
                        execute("CREATE TABLE by_country_" + mode + " AS (" + BY_COUNTRY + ")", context);
                        execute("CREATE TABLE sampled_" + mode + " AS (" + SAMPLED + ") TIMESTAMP(reading_ts) PARTITION BY DAY", context);
                        execute("CREATE TABLE by_site_" + mode + " (site SYMBOL, kwp DOUBLE, n LONG)", context);
                        execute("INSERT INTO by_site_" + mode + " " + BY_SITE, context);
                        execute("CREATE TABLE sampled_wal_" + mode + " (reading_ts TIMESTAMP, e DOUBLE) TIMESTAMP(reading_ts) PARTITION BY DAY WAL", context);
                        execute("INSERT INTO sampled_wal_" + mode + " " + SAMPLED, context);
                    } finally {
                        context.setParallelHashJoinGroupByEnabled(true);
                    }
                }
                drainWalQueue();
                for (String mode : new String[]{"ordinary", "fused"}) {
                    assertQuery("SELECT * FROM by_country_" + mode + " ORDER BY country").expectSize().returns("""
                            country\te\tn
                            \t41.0\t2
                            ES\t110.0\t5
                            FR\t5.0\t1
                            IT\t60.0\t3
                            """);
                    assertQuery("SELECT * FROM sampled_" + mode).timestamp("reading_ts").expectSize().returns("""
                            reading_ts\te
                            2020-01-01T00:00:00.000000Z\t60.0
                            2020-01-02T00:00:00.000000Z\t60.0
                            2020-01-02T12:00:00.000000Z\t40.0
                            2020-01-03T00:00:00.000000Z\t56.0
                            """);
                    assertQuery("SELECT * FROM by_site_" + mode + " ORDER BY site").expectSize().returns("""
                            site\tkwp\tn
                            \t3.0\t1
                            east\t11.0\t1
                            north\t12.0\t4
                            south\t14.0\t5
                            """);
                    assertQuery("SELECT * FROM sampled_wal_" + mode).timestamp("reading_ts").expectSize().returns("""
                            reading_ts\te
                            2020-01-01T00:00:00.000000Z\t60.0
                            2020-01-02T00:00:00.000000Z\t60.0
                            2020-01-02T12:00:00.000000Z\t40.0
                            2020-01-03T00:00:00.000000Z\t56.0
                            """);
                }
            }
        });
    }

    // A CTE inlines a copy of its model at every reference, so every reference compiles a fused operator
    // of its own. Only the LATERAL rewrite shares one model between references, which keeps the
    // ordinary plan (HashJoinGroupByPlannerTest#testSharedLateralInputKeepsOrdinaryPlan).
    @Test
    public void testCteUsedOnceAndAsBothJoinInputs() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                final String with = "WITH g AS (" + BY_COUNTRY + ") ";
                assertComposed(
                        context,
                        with + "SELECT * FROM g ORDER BY country",
                        1,
                        SIZED,
                        """
                                country\te\tn
                                \t41.0\t2
                                ES\t110.0\t5
                                FR\t5.0\t1
                                IT\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        with + "SELECT a.country, a.e, b.n FROM g a JOIN g b ON a.country = b.country ORDER BY a.country",
                        2,
                        UNSIZED,
                        """
                                country\te\tn
                                \t41.0\t2
                                ES\t110.0\t5
                                FR\t5.0\t1
                                IT\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        with + "SELECT * FROM g WHERE e > (SELECT min(e) FROM g) ORDER BY country",
                        2,
                        UNSIZED,
                        """
                                country\te\tn
                                \t41.0\t2
                                ES\t110.0\t5
                                IT\t60.0\t3
                                """
                );
                // The CTE is both inputs of the fused join: a self-join of a filtered r.
                assertComposed(
                        context,
                        """
                                WITH f AS (SELECT * FROM r WHERE energy_kwh > 5)
                                SELECT a.site, sum(b.energy_kwh) e, count(*) n FROM f a JOIN f b ON a.plant_id = b.plant_id ORDER BY a.site
                                """,
                        1,
                        SIZED,
                        """
                                site\te\tn
                                east\t57.0\t2
                                north\t70.0\t3
                                south\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        """
                                WITH j AS (SELECT p.country, r.energy_kwh FROM r JOIN p ON r.plant_id = p.plant_id)
                                SELECT country, sum(energy_kwh) e FROM j ORDER BY country
                                """,
                        1,
                        SIZED,
                        """
                                country\te
                                \t41.0
                                ES\t110.0
                                FR\t5.0
                                IT\t60.0
                                """
                );
            }
        });
    }

    // DECLARE substitutes its expressions into the query, so ones that read columns become fused keys,
    // aggregate arguments and filters.
    @Test
    public void testDeclaredColumnExpressions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                assertComposed(
                        context,
                        """
                                DECLARE @bucket := r.plant_id % 2, @doubled := r.energy_kwh * 2
                                SELECT @bucket bucket, sum(@doubled) e, count(*) n FROM r JOIN p ON r.plant_id = p.plant_id ORDER BY bucket
                                """,
                        1,
                        SIZED,
                        """
                                bucket\te\tn
                                null\t100.0\t1
                                0\t92.0\t3
                                1\t240.0\t7
                                """
                );
                assertComposed(
                        context,
                        """
                                DECLARE @k := p.country, @energy := r.energy_kwh
                                SELECT @k k, avg(r.energy_kwh) e FROM r JOIN p ON r.plant_id = p.plant_id WHERE @k <> 'FR' AND @energy > 1 ORDER BY k
                                """,
                        1,
                        SIZED,
                        """
                                k\te
                                \t40.0
                                ES\t22.0
                                IT\t20.0
                                """
                );
            }
        });
    }

    @Test
    public void testDistinctOverFusedOutput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                assertComposed(
                        context,
                        "SELECT DISTINCT n FROM (" + BY_COUNTRY + ") ORDER BY n",
                        1,
                        SIZED,
                        """
                                n
                                1
                                2
                                3
                                5
                                """
                );
                assertComposed(
                        context,
                        "SELECT DISTINCT country, n FROM (" + BY_COUNTRY_LEFT + ") WHERE n > 1 ORDER BY country",
                        1,
                        SIZED,
                        """
                                country\tn
                                \t3
                                ES\t5
                                IT\t3
                                """
                );
                assertComposed(
                        context,
                        "SELECT count(DISTINCT n) FROM (" + BY_SITE + ")",
                        1,
                        q -> q.noRandomAccess().expectSize(),
                        """
                                count_distinct
                                3
                                """
                );
            }
        });
    }

    // The ordinary hash joins hash their slave and stream their master, so fused output on either side
    // reaches both roles.
    @Test
    public void testFusedOutputOnEitherSideOfOuterJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                final String g = "(" + BY_COUNTRY + ") g";
                assertComposed(
                        context,
                        "SELECT g.country, g.e, c.region FROM " + g + " LEFT JOIN c ON g.country = c.country ORDER BY g.country",
                        1,
                        UNSIZED,
                        """
                                country\te\tregion
                                \t41.0\tnone
                                ES\t110.0\tsouth
                                FR\t5.0\twest
                                IT\t60.0\tsouth
                                """
                );
                assertComposed(
                        context,
                        "SELECT c.country, c.region, g.e FROM c LEFT JOIN " + g + " ON c.country = g.country ORDER BY c.country",
                        1,
                        UNSIZED,
                        """
                                country\tregion\te
                                \tnone\t41.0
                                DE\tcentral\tnull
                                ES\tsouth\t110.0
                                FR\twest\t5.0
                                IT\tsouth\t60.0
                                """
                );
                assertComposed(
                        context,
                        "SELECT g.country, g.e, c.region FROM " + g + " RIGHT JOIN c ON g.country = c.country ORDER BY c.country",
                        1,
                        UNSIZED,
                        """
                                country\te\tregion
                                \t41.0\tnone
                                \tnull\tcentral
                                ES\t110.0\tsouth
                                FR\t5.0\twest
                                IT\t60.0\tsouth
                                """
                );
                assertComposed(
                        context,
                        "SELECT c.country, c.region, g.e FROM c RIGHT JOIN " + g + " ON c.country = g.country ORDER BY g.country",
                        1,
                        UNSIZED,
                        """
                                country\tregion\te
                                \tnone\t41.0
                                ES\tsouth\t110.0
                                FR\twest\t5.0
                                IT\tsouth\t60.0
                                """
                );
                assertComposed(
                        context,
                        "SELECT g.country, c.country c_country, g.e, c.region FROM " + g
                                + " FULL JOIN c ON g.country = c.country ORDER BY g.country, c_country",
                        1,
                        UNSIZED,
                        """
                                country\tc_country\te\tregion
                                \t\t41.0\tnone
                                \tDE\tnull\tcentral
                                ES\tES\t110.0\tsouth
                                FR\tFR\t5.0\twest
                                IT\tIT\t60.0\tsouth
                                """
                );
                assertComposed(
                        context,
                        "SELECT c.country, g.country g_country, c.region, g.e FROM c FULL JOIN " + g
                                + " ON c.country = g.country ORDER BY c.country, g_country",
                        1,
                        UNSIZED,
                        """
                                country\tg_country\tregion\te
                                \t\tnone\t41.0
                                DE\t\tcentral\tnull
                                ES\tES\tsouth\t110.0
                                FR\tFR\twest\t5.0
                                IT\tIT\tsouth\t60.0
                                """
                );
                // INT keys, with repeated keys on the other side.
                final String byPlant = "(SELECT r.plant_id, sum(r.energy_kwh) e FROM r LEFT JOIN p ON r.plant_id = p.plant_id) g";
                assertComposed(
                        context,
                        "SELECT g.plant_id, g.e, p.country, p.installed_kwp FROM " + byPlant
                                + " FULL JOIN p ON g.plant_id = p.plant_id ORDER BY g.plant_id, p.plant_id, p.country, p.installed_kwp",
                        1,
                        UNSIZED,
                        """
                                plant_id\te\tcountry\tinstalled_kwp
                                null\t50.0\tES\t11.0
                                1\t90.0\tES\t5.0
                                1\t90.0\tES\t7.0
                                1\t90.0\tIT\tnull
                                2\t41.0\t\tnull
                                3\t30.0\tIT\t2.0
                                4\t5.0\tFR\t3.0
                                5\t7.0\t\tnull
                                """
                );
                assertComposed(
                        context,
                        "SELECT p.plant_id, p.country, p.installed_kwp, g.e FROM p LEFT JOIN " + byPlant
                                + " ON p.plant_id = g.plant_id ORDER BY p.plant_id, p.country, p.installed_kwp",
                        1,
                        UNSIZED,
                        """
                                plant_id\tcountry\tinstalled_kwp\te
                                null\tES\t11.0\t50.0
                                1\tES\t5.0\t90.0
                                1\tES\t7.0\t90.0
                                1\tIT\tnull\t90.0
                                2\t\tnull\t41.0
                                3\tIT\t2.0\t30.0
                                4\tFR\t3.0\t5.0
                                """
                );
            }
        });
    }

    @Test
    public void testFusedQueriesOnBothSidesOfOneJoinWithFiberWorkers() throws Exception {
        assertFusedQueriesOnBothSidesOfOneJoin(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFusedQueriesOnBothSidesOfOneJoinWithLegacyWorkers() throws Exception {
        assertFusedQueriesOnBothSidesOfOneJoin(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFusedQueriesOnBothSidesOfOneJoinWithOwnerWorkStealing() throws Exception {
        assertFusedQueriesOnBothSidesOfOneJoin(null);
    }

    // The optimiser rewrites the WHERE equality of a comma join into a join key.
    @Test
    public void testImplicitCommaJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                assertComposed(
                        context,
                        "SELECT p.country, sum(r.energy_kwh) e, count(*) n FROM r, p WHERE r.plant_id = p.plant_id ORDER BY country",
                        1,
                        SIZED,
                        """
                                country\te\tn
                                \t41.0\t2
                                ES\t110.0\t5
                                FR\t5.0\t1
                                IT\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        """
                                SELECT p.country, sum(r.energy_kwh) e, count(*) n FROM r, p
                                WHERE r.plant_id = p.plant_id AND r.energy_kwh > 5 AND p.country <> 'FR' ORDER BY country
                                """,
                        1,
                        SIZED,
                        """
                                country\te\tn
                                \t40.0\t1
                                ES\t110.0\t5
                                IT\t60.0\t3
                                """
                );
                // Without a key, or over three tables, the comma join keeps the ordinary plan.
                assertComposed(
                        context,
                        "SELECT count(*) n, sum(r.energy_kwh) e FROM r, p",
                        0,
                        q -> q.noRandomAccess().expectSize(),
                        """
                                n\te
                                56\t1141.0
                                """
                );
                assertComposed(
                        context,
                        """
                                SELECT s.manager, sum(r.energy_kwh) e FROM r, p, s
                                WHERE r.plant_id = p.plant_id AND r.site = s.site ORDER BY s.manager
                                """,
                        0,
                        SIZED,
                        """
                                manager\te
                                ana\t91.0
                                bo\t50.0
                                di\t70.0
                                """
                );
            }
        });
    }

    // An outer WHERE on an aggregate filters the fused output; one on a GROUP BY key may move into the
    // fused operator's build or probe filter.
    @Test
    public void testOuterWhereOverFusedOutput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                assertComposed(
                        context,
                        "SELECT * FROM (" + BY_COUNTRY + ") WHERE e > 20 AND n < 5 ORDER BY country",
                        1,
                        UNSIZED,
                        """
                                country\te\tn
                                \t41.0\t2
                                IT\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        "SELECT country, e FROM (" + BY_COUNTRY + ") WHERE country <> 'ES' ORDER BY country",
                        1,
                        SIZED,
                        """
                                country\te
                                \t41.0
                                FR\t5.0
                                IT\t60.0
                                """
                );
                assertComposed(
                        context,
                        "SELECT site, kwp FROM (" + BY_SITE + ") WHERE site IN ('north', 'east') ORDER BY site",
                        1,
                        SIZED,
                        """
                                site\tkwp
                                east\t11.0
                                north\t12.0
                                """
                );
                // A filter on the null-extended side of a LEFT join must not move below it.
                assertComposed(
                        context,
                        "SELECT * FROM (" + BY_COUNTRY_LEFT + ") WHERE country <> 'ES' ORDER BY country",
                        1,
                        SIZED,
                        """
                                country\te\tn
                                \t48.0\t3
                                FR\t5.0\t1
                                IT\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        "SELECT * FROM (" + BY_COUNTRY_LEFT + ") WHERE country IS NULL",
                        1,
                        SIZED,
                        """
                                country\te\tn
                                \t48.0\t3
                                """
                );
            }
        });
    }

    // A held factory, as a query cache keeps one, fails with TableReferenceOutOfDateException once RENAME
    // COLUMN changes the metadata of either input, as the ordinary plan's does. A new compile fuses again
    // under the new names.
    @Test
    public void testRenameColumnInvalidatesHeldFactories() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                final String[] renames = {
                        "ALTER TABLE p RENAME COLUMN country TO nation",
                        "ALTER TABLE r RENAME COLUMN plant_id TO pid"
                };
                final String[] queries = {
                        "SELECT p.country, sum(r.energy_kwh) e, count(*) n FROM r JOIN p ON r.plant_id = p.plant_id ORDER BY 1",
                        "SELECT p.nation, sum(r.energy_kwh) e, count(*) n FROM r JOIN p ON r.plant_id = p.plant_id ORDER BY 1",
                        "SELECT p.nation, sum(r.energy_kwh) e, count(*) n FROM r JOIN p ON r.pid = p.plant_id ORDER BY 1"
                };
                final String[] tables = {"p", "r"};
                final String[] staleColumns = {"p.country", "r.plant_id"};
                for (int i = 0; i < renames.length; i++) {
                    try (
                            RecordCursorFactory ordinary = select(context, queries[i], false);
                            RecordCursorFactory fusedFactory = select(context, queries[i], true)
                    ) {
                        Assert.assertTrue(plan(fusedFactory, context).contains("Async Hash Join Group By"));
                        execute(renames[i]);
                        for (RecordCursorFactory factory : new RecordCursorFactory[]{ordinary, fusedFactory}) {
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                cursor.hasNext();
                                Assert.fail(queries[i] + ": a factory over renamed columns must ask for a recompile");
                            } catch (TableReferenceOutOfDateException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "table schema has changed [table=" + tables[i]);
                            }
                        }
                        Assert.assertNull(context.getMemoryTracker());
                        Assert.assertEquals(0, fused(fusedFactory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    }
                    for (boolean isFused : new boolean[]{false, true}) {
                        context.setParallelHashJoinGroupByEnabled(isFused);
                        try {
                            assertQuery(queries[i])
                                    .withContext(context)
                                    .noLeakCheck()
                                    .fails(queries[i].indexOf(staleColumns[i]), "Invalid column: " + staleColumns[i]);
                        } finally {
                            context.setParallelHashJoinGroupByEnabled(true);
                        }
                    }
                    assertComposed(
                            context,
                            queries[i + 1],
                            1,
                            SIZED,
                            """
                                    nation\te\tn
                                    \t41.0\t2
                                    ES\t110.0\t5
                                    FR\t5.0\t1
                                    IT\t60.0\t3
                                    """
                    );
                }
            }
        });
    }

    @Test
    public void testSetOperationsOverFusedBranches() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                final String filtered = "SELECT p.country, sum(r.energy_kwh) e, count(*) n FROM r JOIN p ON r.plant_id = p.plant_id WHERE r.energy_kwh < 40";
                assertComposed(
                        context,
                        "SELECT * FROM (" + BY_COUNTRY + " UNION " + BY_COUNTRY_LEFT + ") ORDER BY country, e",
                        2,
                        UNSIZED,
                        """
                                country\te\tn
                                \t41.0\t2
                                \t48.0\t3
                                ES\t110.0\t5
                                FR\t5.0\t1
                                IT\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        "SELECT * FROM (" + BY_COUNTRY + " UNION ALL " + BY_COUNTRY_LEFT + ") ORDER BY country, e",
                        2,
                        SIZED,
                        """
                                country\te\tn
                                \t41.0\t2
                                \t48.0\t3
                                ES\t110.0\t5
                                ES\t110.0\t5
                                FR\t5.0\t1
                                FR\t5.0\t1
                                IT\t60.0\t3
                                IT\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        "SELECT * FROM (" + BY_COUNTRY + " EXCEPT " + filtered + ") ORDER BY country",
                        2,
                        UNSIZED,
                        """
                                country\te\tn
                                \t41.0\t2
                                ES\t110.0\t5
                                """
                );
                assertComposed(
                        context,
                        "SELECT * FROM (" + BY_COUNTRY + " INTERSECT " + filtered + ") ORDER BY country",
                        2,
                        UNSIZED,
                        """
                                country\te\tn
                                FR\t5.0\t1
                                IT\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        "SELECT * FROM (" + BY_COUNTRY + " UNION ALL " + BY_COUNTRY_LEFT + " UNION ALL " + filtered + ") ORDER BY country, e",
                        3,
                        SIZED,
                        """
                                country\te\tn
                                \t1.0\t1
                                \t41.0\t2
                                \t48.0\t3
                                ES\t60.0\t4
                                ES\t110.0\t5
                                ES\t110.0\t5
                                FR\t5.0\t1
                                FR\t5.0\t1
                                FR\t5.0\t1
                                IT\t60.0\t3
                                IT\t60.0\t3
                                IT\t60.0\t3
                                """
                );
            }
        });
    }

    // A join over fused SYMBOL output compares symbol keys, which the static-symbol flags of the fused
    // metadata allow, and translates them through the fused cursor's symbol tables. c and s intern
    // their texts in other orders than p and r, so wrong tables find the wrong partners.
    @Test
    public void testSymbolKeyJoinsOverFusedOutput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                assertComposed(
                        context,
                        "SELECT g.country, g.e, c.region FROM (" + BY_COUNTRY + ") g JOIN c ON g.country = c.country ORDER BY g.country",
                        1,
                        UNSIZED,
                        """
                                country\te\tregion
                                \t41.0\tnone
                                ES\t110.0\tsouth
                                FR\t5.0\twest
                                IT\t60.0\tsouth
                                """,
                        "symbolKeyJoin: true"
                );
                assertComposed(
                        context,
                        "SELECT c.country, c.region, g.e FROM c JOIN (" + BY_COUNTRY + ") g ON c.country = g.country ORDER BY c.country",
                        1,
                        UNSIZED,
                        """
                                country\tregion\te
                                \tnone\t41.0
                                ES\tsouth\t110.0
                                FR\twest\t5.0
                                IT\tsouth\t60.0
                                """,
                        "symbolKeyJoin: true"
                );
                assertComposed(
                        context,
                        "SELECT g.site, g.kwp, s.manager FROM (" + BY_SITE + ") g JOIN s ON g.site = s.site ORDER BY g.site",
                        1,
                        UNSIZED,
                        """
                                site\tkwp\tmanager
                                east\t11.0\tbo
                                north\t12.0\tdi
                                south\t14.0\tana
                                """,
                        "symbolKeyJoin: true"
                );
                assertComposed(
                        context,
                        """
                                SELECT g.site, g.country, g.e, c.region, s.manager
                                FROM (SELECT r.site, p.country, sum(r.energy_kwh) e FROM r JOIN p ON r.plant_id = p.plant_id) g
                                JOIN c ON g.country = c.country
                                JOIN s ON g.site = s.site
                                ORDER BY g.site, g.country
                                """,
                        1,
                        UNSIZED,
                        """
                                site\tcountry\te\tregion\tmanager
                                east\tES\t50.0\tsouth\tbo
                                north\t\t40.0\tnone\tdi
                                north\tES\t20.0\tsouth\tdi
                                north\tIT\t10.0\tsouth\tdi
                                south\t\t1.0\tnone\tana
                                south\tES\t40.0\tsouth\tana
                                south\tIT\t50.0\tsouth\tana
                                """,
                        "symbolKeyJoin: true"
                );
            }
        });
    }

    @Test
    public void testViews() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW by_country AS (" + BY_COUNTRY + ")");
            execute("CREATE VIEW joined AS (SELECT p.country, r.site, r.energy_kwh FROM r JOIN p ON r.plant_id = p.plant_id)");
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                assertComposed(
                        context,
                        "SELECT * FROM by_country ORDER BY country",
                        1,
                        SIZED,
                        """
                                country\te\tn
                                \t41.0\t2
                                ES\t110.0\t5
                                FR\t5.0\t1
                                IT\t60.0\t3
                                """
                );
                assertComposed(
                        context,
                        "SELECT country, sum(energy_kwh) e FROM joined ORDER BY country",
                        1,
                        SIZED,
                        """
                                country\te
                                \t41.0
                                ES\t110.0
                                FR\t5.0
                                IT\t60.0
                                """
                );
                assertComposed(
                        context,
                        "SELECT v.country, v.e, c.region FROM by_country v JOIN c ON v.country = c.country ORDER BY v.country",
                        1,
                        UNSIZED,
                        """
                                country\te\tregion
                                \t41.0\tnone
                                ES\t110.0\tsouth
                                FR\t5.0\twest
                                IT\t60.0\tsouth
                                """
                );
            }
        });
    }

    @Test
    public void testWindowFunctionsOverFusedOutput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = composingContext(engine)) {
                assertComposed(
                        context,
                        """
                                SELECT country, e, n, sum(e) OVER (ORDER BY country) running,
                                    row_number() OVER (PARTITION BY n % 2 ORDER BY e DESC) rn, rank() OVER (ORDER BY n) rk
                                FROM (""" + BY_COUNTRY + ") ORDER BY country",
                        1,
                        SIZED,
                        """
                                country\te\tn\trunning\trn\trk
                                \t41.0\t2\t41.0\t1\t2
                                ES\t110.0\t5\t151.0\t1\t4
                                FR\t5.0\t1\t156.0\t3\t1
                                IT\t60.0\t3\t216.0\t2\t3
                                """
                );
                assertComposed(
                        context,
                        """
                                SELECT reading_ts, e, avg(e) OVER (ORDER BY reading_ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) moving,
                                    lag(e) OVER (ORDER BY reading_ts) prev
                                FROM (""" + SAMPLED + ") ORDER BY reading_ts",
                        1,
                        q -> q.expectSize().timestamp("reading_ts"),
                        """
                                reading_ts\te\tmoving\tprev
                                2020-01-01T00:00:00.000000Z\t60.0\t60.0\tnull
                                2020-01-02T00:00:00.000000Z\t60.0\t60.0\t60.0
                                2020-01-02T12:00:00.000000Z\t40.0\t50.0\t60.0
                                2020-01-03T00:00:00.000000Z\t56.0\t48.0\t40.0
                                """
                );
            }
        });
    }

    // Runs the query with the fused path off, then on. Both plans must return the expected rows and pass
    // the same factory checks; the ordinary plan holds no fused node, and the fused one fusedNodes of them.
    private void assertComposed(
            SqlExecutionContextImpl context,
            String sql,
            int fusedNodes,
            UnaryOperator<QueryAssertion> properties,
            String expected,
            String... planFragments
    ) throws Exception {
        final CairoEngine db = context.getCairoEngine();
        for (boolean isFused : new boolean[]{false, true}) {
            context.setParallelHashJoinGroupByEnabled(isFused);
            try {
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final String plan = plan(factory, context);
                    Assert.assertEquals(sql + '\n' + plan, isFused ? fusedNodes : 0, fusedNodeCount(plan));
                    for (String fragment : planFragments) {
                        Assert.assertTrue(sql + '\n' + plan, plan.contains(fragment));
                    }
                }
                properties.apply(assertQuery(sql).withEngine(db).withContext(context).noLeakCheck()).returns(expected);
            } finally {
                context.setParallelHashJoinGroupByEnabled(true);
            }
        }
        Assert.assertNull(context.getMemoryTracker());
    }

    // Two fused cursors stay open at once, one per join input, and share the workers' queue.
    private void assertFusedQueriesOnBothSidesOfOneJoin(@Nullable WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ctx) -> {
                try (SqlExecutionContextImpl context = composingContext(db)) {
                    createTables(db, context);
                    assertComposed(
                            context,
                            "SELECT g.country, g.e, h.n FROM (" + BY_COUNTRY + ") g JOIN (" + BY_COUNTRY_LEFT
                                    + ") h ON g.country = h.country ORDER BY g.country",
                            2,
                            UNSIZED,
                            """
                                    country\te\tn
                                    \t41.0\t3
                                    ES\t110.0\t5
                                    FR\t5.0\t1
                                    IT\t60.0\t3
                                    """
                    );
                    assertComposed(
                            context,
                            "SELECT g.site, g.kwp, h.e FROM (" + BY_SITE
                                    + ") g LEFT JOIN (SELECT r.site, sum(r.energy_kwh) e FROM r LEFT JOIN p ON r.plant_id = p.plant_id) h"
                                    + " ON g.site = h.site ORDER BY g.site",
                            2,
                            UNSIZED,
                            """
                                    site\tkwp\te
                                    \t3.0\t5.0
                                    east\t11.0\t57.0
                                    north\t12.0\t70.0
                                    south\t14.0\t91.0
                                    """
                    );
                    assertComposed(
                            context,
                            "SELECT g.country, g.n, h.site FROM (" + BY_COUNTRY + ") g FULL JOIN (" + BY_SITE
                                    + ") h ON g.n = h.n ORDER BY g.country, h.site",
                            2,
                            UNSIZED,
                            """
                                    country\tn\tsite
                                    \t2\t
                                    \tnull\tnorth
                                    ES\t5\tsouth
                                    FR\t1\t
                                    FR\t1\teast
                                    IT\t3\t
                                    """
                    );
                    assertComposed(
                            context,
                            "SELECT g.country, g.e, t.total FROM (" + BY_COUNTRY
                                    + ") g CROSS JOIN (SELECT sum(r.energy_kwh) total FROM r JOIN p ON r.plant_id = p.plant_id) t ORDER BY g.country",
                            2,
                            SIZED,
                            """
                                    country\te\ttotal
                                    \t41.0\t216.0
                                    ES\t110.0\t216.0
                                    FR\t5.0\t216.0
                                    IT\t60.0\t216.0
                                    """
                    );
                }
            }, configuration, LOG);
        });
    }

    // Frames of one or two rows split every input into several frames.
    private static SqlExecutionContextImpl composingContext(CairoEngine db) {
        final SqlExecutionContextImpl context = context(db, 4);
        context.changePageFrameSizes(1, 2);
        return context;
    }

    private static void createTables() throws SqlException {
        createTables(engine, sqlExecutionContext);
    }

    // c and s intern their SYMBOL texts in other orders than p and r, so that a join that compares
    // symbol keys rather than texts finds the wrong partners.
    private static void createTables(CairoEngine db, SqlExecutionContext context) throws SqlException {
        final String[] ddl = {
                "CREATE TABLE r (plant_id INT, site SYMBOL, reading_ts TIMESTAMP, energy_kwh DOUBLE) TIMESTAMP(reading_ts) PARTITION BY DAY",
                "CREATE TABLE p (plant_id INT, country SYMBOL, installed_kwp DOUBLE)",
                "CREATE TABLE c (country SYMBOL, region SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                "CREATE TABLE s (site SYMBOL, manager SYMBOL)",
                """
                INSERT INTO r VALUES
                    (1, 'north', '2020-01-01T00', 10), (3, 'south', '2020-01-01T06', 30),
                    (1, 'south', '2020-01-02T00', 20), (2, 'north', '2020-01-02T12', 40),
                    (null, 'east', '2020-01-03T00', 50), (4, null, '2020-01-03T01', 5),
                    (5, 'east', '2020-01-03T02', 7), (2, 'south', '2020-01-03T03', 1)
                """,
                """
                INSERT INTO p VALUES
                    (1, 'ES', 5), (1, 'ES', 7), (1, 'IT', null), (2, null, null),
                    (null, 'ES', 11), (3, 'IT', 2), (4, 'FR', 3)
                """,
                """
                INSERT INTO c VALUES
                    ('DE', 'central', '2019-12-31'), ('FR', 'west', '2020-01-01T12'), ('IT', 'south', '2020-01-02T00'),
                    ('ES', 'south', '2020-01-02T06'), (null, 'none', '2020-01-02T12')
                """,
                "INSERT INTO s VALUES ('west', 'cy'), ('east', 'bo'), ('south', 'ana'), ('north', 'di')"
        };
        for (String sql : ddl) {
            db.execute(sql, context);
        }
    }

    // Counts the fused operators in a plan, both of a build choice included.
    private static int fusedNodeCount(String plan) {
        int count = 0;
        for (int i = plan.indexOf("Hash Join Group By"); i > -1; i = plan.indexOf("Hash Join Group By", i + 1)) {
            if (!plan.startsWith("Hash Join Group By Build Choice", i)) {
                count++;
            }
        }
        return count;
    }

    private static RecordCursorFactory select(SqlExecutionContextImpl context, String sql, boolean isFused) throws SqlException {
        context.setParallelHashJoinGroupByEnabled(isFused);
        try {
            return context.getCairoEngine().select(sql, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
    }
}
