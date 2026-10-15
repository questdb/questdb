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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Refreshes materialized views whose query is a GROUP BY over a hash join, once with the fused
 * hash join GROUP BY enabled and once with it disabled, and compares the views after every
 * refresh. The refresh context replaces the base table's interval with the bounds of each
 * refresh step, so an INNER join compiles both build orientations, and the view state caches
 * the compiled factory across refreshes.
 */
public class MatViewHashJoinGroupByTest extends AbstractCairoTest {
    private static final String ALTERNATE = "build choice, alternate";
    private static final String FUSED = "fused";
    private static final String ORDINARY = "ordinary";
    private static final String PRIMARY = "build choice, primary";
    private static final String RECOMPILE_REGEX = "compiling materialized view \\[view=mv[^,]*, attempt=1]";
    private final LogCapture capture = new LogCapture();

    @Override
    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    // An INNER join over an interval scan compiles both orientations. Incremental refreshes re-run
    // the cached factory over the buckets each insert touches, including a late row in an earlier
    // bucket and a dimension row that only later buckets see.
    @Test
    public void testInnerJoinIncrementalRefresh() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = innerJoinIncrementalRefresh(false);
            Assert.assertEquals(expected, innerJoinIncrementalRefresh(true));
        });
    }

    // A SYMBOL key translates the probe's keys into the build's dictionary, so an INNER join keeps
    // one orientation. Once the build table holds more than twice the base table's rows, the
    // cached factory throws TableReferenceOutOfDateException into the refresh retry loop, which
    // compiles the view again and builds the base table instead.
    @Test
    public void testInnerJoinSymbolKeyRecompilesOnceBuildTableOutgrowsProbe() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = innerJoinSymbolKeyGrowth(false);
            Assert.assertEquals(expected, innerJoinSymbolKeyGrowth(true));
        });
    }

    // A LEFT join builds the dimension, which the planner presized by its rows while it held no
    // more rows than the base table. Growth past twice the base table's rows recompiles the view.
    @Test
    public void testLeftJoinRecompilesOnceBuildTableOutgrowsProbe() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = leftJoinGrowth(false);
            Assert.assertEquals(expected, leftJoinGrowth(true));
        });
    }

    // Both dictionaries gain symbols between refreshes, which the cached factory must translate
    // without a recompile, and base rows without a dimension match keep NULL payloads.
    @Test
    public void testLeftJoinSymbolKeyIncrementalRefresh() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = leftJoinSymbolKeyIncrementalRefresh(false);
            Assert.assertEquals(expected, leftJoinSymbolKeyIncrementalRefresh(true));
        });
    }

    // One row per query makes each refresh step cover one bucket, so a refresh runs one factory
    // over several interval bounds. Dense buckets hold more base rows than the dimension, so they
    // build the dimension, and sparse ones build the base table's interval.
    @Test
    public void testRangeRefreshRunsOneFactoryOverManySteps() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        assertMemoryLeak(() -> {
            final String expected = rangeRefreshSteps(false);
            Assert.assertEquals(expected, rangeRefreshSteps(true));
        });
    }

    // The refresh context disables parallel GROUP BY when mat view parallel SQL is off, and the
    // fused plan requires it.
    @Test
    public void testRefreshKeepsOrdinaryPlanWithoutParallelSql() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTables("INT");
            execute("""
                    CREATE MATERIALIZED VIEW mv WITH BASE trades AS
                    SELECT t.ts, p.country, sum(t.qty) total, count() n
                    FROM trades t JOIN plants p ON t.plant_id = p.plant_id
                    SAMPLE BY 1h
                    """);
            execute("INSERT INTO trades VALUES (1, 10, '2024-01-01T00:10'), (2, 20, '2024-01-01T01:20')");
            refresh();
            assertView("""
                    ts\tcountry\ttotal\tn
                    2024-01-01T00:00:00.000000Z\tES\t10.0\t1
                    2024-01-01T01:00:00.000000Z\tIT\t20.0\t1
                    """);
            Assert.assertEquals(ORDINARY, cachedFactory());
        });
    }

    // Takes the factory that the last refresh cached in the view state and describes it. The next
    // refresh compiles the view again. The factory's plan is not printed: the ordinary hash join
    // prints its join condition from the compiler's lexer, which later compiles have overwritten.
    private static String cachedFactory() {
        final TableToken token = engine.verifyTableName("mv");
        final MatViewState state = engine.getMatViewStateStore().getViewState(token);
        Assert.assertNotNull(state);
        Assert.assertTrue(state.tryLock());
        try (RecordCursorFactory factory = state.acquireRecordFactory()) {
            Assert.assertNotNull(factory);
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                if (f instanceof HashJoinGroupByBuildChoiceRecordCursorFactory choice) {
                    return choice.isAlternateChosen() ? ALTERNATE : PRIMARY;
                }
                if (f instanceof AsyncHashJoinGroupByRecordCursorFactory) {
                    return FUSED;
                }
            }
            return ORDINARY;
        } finally {
            state.unlock();
        }
    }

    private static void createTables(String keyType) throws Exception {
        execute("CREATE TABLE trades (plant_id " + keyType + ", qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE plants (plant_id " + keyType + ", country SYMBOL, kwp DOUBLE)");
        if ("SYMBOL".equals(keyType)) {
            execute("INSERT INTO plants VALUES ('a', 'ES', 5), ('b', 'IT', 7), ('c', 'ES', null), (null, 'FR', 9)");
        } else {
            execute("INSERT INTO plants VALUES (1, 'ES', 5), (2, 'IT', 7), (3, 'ES', null), (null, 'FR', 9)");
        }
    }

    private static void dropTables() throws Exception {
        execute("DROP MATERIALIZED VIEW mv");
        execute("DROP TABLE trades");
        execute("DROP TABLE plants");
        drainWalAndMatViewQueues();
    }

    private static void refresh() {
        drainWalAndMatViewQueues();
        drainPurgeJob();
    }

    private static void setFused(boolean isFused) {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_ENABLED, Boolean.toString(isFused));
    }

    private void assertView(String expected) throws Exception {
        assertQuery("mv ORDER BY ts, country").noLeakCheck().expectSize().timestamp("ts").returns(expected);
    }

    private String innerJoinIncrementalRefresh(boolean isFused) throws Exception {
        setFused(isFused);
        final StringBuilder snapshot = new StringBuilder();
        createTables("INT");
        execute("""
                CREATE MATERIALIZED VIEW mv WITH BASE trades AS
                SELECT t.ts, p.country, sum(t.qty) total, count() n, avg(p.kwp) avg_kwp
                FROM trades t JOIN plants p ON t.plant_id = p.plant_id
                SAMPLE BY 1h
                """);
        execute("""
                INSERT INTO trades VALUES
                (1, 10, '2024-01-01T00:10'), (2, 20, '2024-01-01T00:20'), (3, 30, '2024-01-01T01:10'),
                (1, 40, '2024-01-01T02:10'), (4, 50, '2024-01-01T02:20'), (null, 60, '2024-01-01T02:30')
                """);
        snapshot(snapshot);
        capture.start();
        // A late row lands in the first bucket, so the refresh recomputes every bucket up to the new one.
        execute("INSERT INTO trades VALUES (2, 5, '2024-01-01T03:05'), (1, 15, '2024-01-01T00:40'), (3, 25, '2024-01-01T03:15')");
        snapshot(snapshot);
        // Incremental refresh sees the new plant only in the bucket that the new trade touches.
        execute("INSERT INTO plants VALUES (4, 'IT', 3)");
        execute("INSERT INTO trades VALUES (4, 70, '2024-01-01T04:00')");
        snapshot(snapshot);
        // The factory that the first refresh compiled serves the later ones.
        capture.drain();
        capture.assertNotLogged("compiling materialized view");
        capture.stop();
        // Joins match NULL keys, so the trade without a plant joins the FR plant.
        assertView("""
                ts\tcountry\ttotal\tn\tavg_kwp
                2024-01-01T00:00:00.000000Z\tES\t25.0\t2\t5.0
                2024-01-01T00:00:00.000000Z\tIT\t20.0\t1\t7.0
                2024-01-01T01:00:00.000000Z\tES\t30.0\t1\tnull
                2024-01-01T02:00:00.000000Z\tES\t40.0\t1\t5.0
                2024-01-01T02:00:00.000000Z\tFR\t60.0\t1\t9.0
                2024-01-01T03:00:00.000000Z\tES\t25.0\t1\tnull
                2024-01-01T03:00:00.000000Z\tIT\t5.0\t1\t7.0
                2024-01-01T04:00:00.000000Z\tIT\t70.0\t1\t3.0
                """);
        // The last refresh scanned one base row, fewer than the dimension's five.
        Assert.assertEquals(isFused ? ALTERNATE : ORDINARY, cachedFactory());
        dropTables();
        return snapshot.toString();
    }

    private String innerJoinSymbolKeyGrowth(boolean isFused) throws Exception {
        setFused(isFused);
        final StringBuilder snapshot = new StringBuilder();
        createTables("SYMBOL");
        execute("""
                CREATE MATERIALIZED VIEW mv WITH BASE trades AS
                SELECT t.ts, p.country, sum(t.qty) total, count() n, avg(p.kwp) avg_kwp
                FROM trades t JOIN plants p ON t.plant_id = p.plant_id
                SAMPLE BY 1h
                """);
        // Four plants and five trades: the planner builds the dimension and presizes its key table.
        execute("""
                INSERT INTO trades VALUES
                ('a', 10, '2024-01-01T00:10'), ('b', 20, '2024-01-01T00:20'), ('c', 30, '2024-01-01T01:10'),
                ('a', 40, '2024-01-01T02:10'), ('d', 50, '2024-01-01T02:20')
                """);
        capture.start();
        snapshot(snapshot);
        // Fifteen plants against six trades, more than twice as many, with symbols new to both dictionaries.
        execute("""
                INSERT INTO plants SELECT ('n' || x)::SYMBOL, CASE WHEN x % 2 = 0 THEN 'DE' ELSE 'PL' END, x
                FROM long_sequence(11)
                """);
        execute("INSERT INTO trades VALUES ('n2', 6, '2024-01-01T03:30')");
        snapshot(snapshot);
        capture.drain();
        if (isFused) {
            capture.assertLoggedRE(RECOMPILE_REGEX);
        } else {
            capture.assertNotLogged("attempt=1");
        }
        execute("INSERT INTO trades VALUES ('n3', 7, '2024-01-01T03:40'), ('b', 8, '2024-01-01T04:10')");
        snapshot(snapshot);
        capture.stop();
        assertView("""
                ts\tcountry\ttotal\tn\tavg_kwp
                2024-01-01T00:00:00.000000Z\tES\t10.0\t1\t5.0
                2024-01-01T00:00:00.000000Z\tIT\t20.0\t1\t7.0
                2024-01-01T01:00:00.000000Z\tES\t30.0\t1\tnull
                2024-01-01T02:00:00.000000Z\tES\t40.0\t1\t5.0
                2024-01-01T03:00:00.000000Z\tDE\t6.0\t1\t2.0
                2024-01-01T03:00:00.000000Z\tPL\t7.0\t1\t3.0
                2024-01-01T04:00:00.000000Z\tIT\t8.0\t1\t7.0
                """);
        Assert.assertEquals(isFused ? FUSED : ORDINARY, cachedFactory());
        dropTables();
        return snapshot.toString();
    }

    private String leftJoinGrowth(boolean isFused) throws Exception {
        setFused(isFused);
        final StringBuilder snapshot = new StringBuilder();
        createTables("INT");
        execute("""
                CREATE MATERIALIZED VIEW mv WITH BASE trades AS
                SELECT t.ts, p.country, sum(t.qty) total, count() n, count(p.kwp) kwp_count, avg(p.kwp) avg_kwp
                FROM trades t LEFT JOIN plants p ON t.plant_id = p.plant_id
                SAMPLE BY 1h
                """);
        execute("""
                INSERT INTO trades VALUES
                (1, 10, '2024-01-01T00:10'), (2, 20, '2024-01-01T00:20'), (3, 30, '2024-01-01T01:10'),
                (5, 40, '2024-01-01T02:10'), (null, 50, '2024-01-01T02:20')
                """);
        capture.start();
        snapshot(snapshot);
        // Thirty plants against six trades, more than twice as many, and plants 1 and 5 repeat.
        execute("INSERT INTO plants SELECT (x % 10)::INT, CASE WHEN x % 2 = 0 THEN 'DE' ELSE 'PL' END, x FROM long_sequence(26)");
        execute("INSERT INTO trades VALUES (5, 6, '2024-01-01T03:30')");
        snapshot(snapshot);
        capture.drain();
        if (isFused) {
            capture.assertLoggedRE(RECOMPILE_REGEX);
        } else {
            capture.assertNotLogged("attempt=1");
        }
        execute("INSERT INTO trades VALUES (1, 7, '2024-01-01T03:40'), (42, 8, '2024-01-01T04:10')");
        snapshot(snapshot);
        capture.stop();
        assertView("""
                ts\tcountry\ttotal\tn\tkwp_count\tavg_kwp
                2024-01-01T00:00:00.000000Z\tES\t10.0\t1\t1\t5.0
                2024-01-01T00:00:00.000000Z\tIT\t20.0\t1\t1\t7.0
                2024-01-01T01:00:00.000000Z\tES\t30.0\t1\t0\tnull
                2024-01-01T02:00:00.000000Z\t\t40.0\t1\t0\tnull
                2024-01-01T02:00:00.000000Z\tFR\t50.0\t1\t1\t9.0
                2024-01-01T03:00:00.000000Z\tES\t7.0\t1\t1\t5.0
                2024-01-01T03:00:00.000000Z\tPL\t39.0\t6\t6\t13.0
                2024-01-01T04:00:00.000000Z\t\t8.0\t1\t0\tnull
                """);
        Assert.assertEquals(isFused ? FUSED : ORDINARY, cachedFactory());
        dropTables();
        return snapshot.toString();
    }

    private String leftJoinSymbolKeyIncrementalRefresh(boolean isFused) throws Exception {
        setFused(isFused);
        final StringBuilder snapshot = new StringBuilder();
        createTables("SYMBOL");
        execute("""
                CREATE MATERIALIZED VIEW mv WITH BASE trades AS
                SELECT t.ts, p.country, sum(t.qty) total, count() n, count(p.kwp) kwp_count, avg(p.kwp) avg_kwp
                FROM trades t LEFT JOIN plants p ON t.plant_id = p.plant_id
                SAMPLE BY 1h
                """);
        execute("""
                INSERT INTO trades VALUES
                ('a', 10, '2024-01-01T00:10'), ('b', 20, '2024-01-01T00:20'), ('c', 30, '2024-01-01T01:10'),
                ('x', 40, '2024-01-01T01:20'), (null, 50, '2024-01-01T01:30')
                """);
        snapshot(snapshot);
        capture.start();
        // Both dictionaries grow: 'd' matches, 'y' exists only in the base table, and 'x' gains a
        // match that the bucket of its first trade does not see yet.
        execute("INSERT INTO plants VALUES ('d', 'DE', 2), ('x', 'PL', 4)");
        execute("""
                INSERT INTO trades VALUES
                ('d', 5, '2024-01-01T02:05'), ('y', 6, '2024-01-01T02:10'), ('x', 7, '2024-01-01T02:15'),
                ('a', 8, '2024-01-01T02:20')
                """);
        snapshot(snapshot);
        // A late row makes the refresh recompute every bucket from the first one, so the first
        // trade of 'x' joins its plant now.
        execute("INSERT INTO trades VALUES ('b', 9, '2024-01-01T00:50'), ('z', 11, '2024-01-01T03:00')");
        snapshot(snapshot);
        capture.drain();
        // Dictionary growth alone keeps the cached factory.
        capture.assertNotLogged("compiling materialized view");
        capture.stop();
        assertView("""
                ts\tcountry\ttotal\tn\tkwp_count\tavg_kwp
                2024-01-01T00:00:00.000000Z\tES\t10.0\t1\t1\t5.0
                2024-01-01T00:00:00.000000Z\tIT\t29.0\t2\t2\t7.0
                2024-01-01T01:00:00.000000Z\tES\t30.0\t1\t0\tnull
                2024-01-01T01:00:00.000000Z\tFR\t50.0\t1\t1\t9.0
                2024-01-01T01:00:00.000000Z\tPL\t40.0\t1\t1\t4.0
                2024-01-01T02:00:00.000000Z\t\t6.0\t1\t0\tnull
                2024-01-01T02:00:00.000000Z\tDE\t5.0\t1\t1\t2.0
                2024-01-01T02:00:00.000000Z\tES\t8.0\t1\t1\t5.0
                2024-01-01T02:00:00.000000Z\tPL\t7.0\t1\t1\t4.0
                2024-01-01T03:00:00.000000Z\t\t11.0\t1\t0\tnull
                """);
        Assert.assertEquals(isFused ? FUSED : ORDINARY, cachedFactory());
        dropTables();
        return snapshot.toString();
    }

    private String rangeRefreshSteps(boolean isFused) throws Exception {
        setFused(isFused);
        final StringBuilder snapshot = new StringBuilder();
        createTables("INT");
        execute("""
                CREATE MATERIALIZED VIEW mv WITH BASE trades AS
                SELECT t.ts, p.country, sum(t.qty) total, count() n, avg(p.kwp) avg_kwp
                FROM trades t JOIN plants p ON t.plant_id = p.plant_id
                SAMPLE BY 1h
                """);
        // Hours 0 to 2 hold eight trades each, more than the plants; hours 3 to 5 hold one.
        execute("""
                INSERT INTO trades SELECT (x % 4)::INT, x, '2024-01-01'::TIMESTAMP + ((x - 1) / 8) * 3_600_000_000L + x * 60_000_000L
                FROM long_sequence(24)
                """);
        execute("""
                INSERT INTO trades VALUES
                (1, 100, '2024-01-01T03:10'), (2, 200, '2024-01-01T04:10'), (3, 300, '2024-01-01T05:10')
                """);
        capture.start();
        snapshot(snapshot);
        capture.drain();
        capture.assertLoggedRE("refreshing materialized view \\[view=mv[^\\]]*iteratorStep=1");
        // The dimension changes, and only a range refresh recomputes the buckets that already exist.
        execute("UPDATE plants SET kwp = 11 WHERE plant_id = 1");
        execute("INSERT INTO plants VALUES (3, 'IT', 13)");
        capture.start();
        execute("REFRESH MATERIALIZED VIEW mv RANGE FROM '2024-01-01T01:00' TO '2024-01-01T04:00'");
        snapshot(snapshot);
        // The range refresh runs the cached factory over hours 1 to 4, two dense and two sparse.
        capture.drain();
        capture.assertLoggedRE("refreshing materialized view \\[view=mv[^\\]]*iteratorStep=1");
        capture.assertNotLogged("compiling materialized view");
        capture.stop();
        assertView("""
                ts\tcountry\ttotal\tn\tavg_kwp
                2024-01-01T00:00:00.000000Z\tES\t16.0\t4\t5.0
                2024-01-01T00:00:00.000000Z\tIT\t8.0\t2\t7.0
                2024-01-01T01:00:00.000000Z\tES\t48.0\t4\t11.0
                2024-01-01T01:00:00.000000Z\tIT\t50.0\t4\t10.0
                2024-01-01T02:00:00.000000Z\tES\t80.0\t4\t11.0
                2024-01-01T02:00:00.000000Z\tIT\t82.0\t4\t10.0
                2024-01-01T03:00:00.000000Z\tES\t100.0\t1\t11.0
                2024-01-01T04:00:00.000000Z\tIT\t200.0\t1\t7.0
                2024-01-01T05:00:00.000000Z\tES\t300.0\t1\tnull
                """);
        // The last step covers hour 4, which holds one trade against five plants.
        Assert.assertEquals(isFused ? ALTERNATE : ORDINARY, cachedFactory());
        // The check above dropped the cached factory, so the next refresh compiles a new one, and
        // the eight trades of hour 6 outnumber the five plants, so it builds the plants.
        execute("INSERT INTO trades SELECT (x % 4)::INT, x, '2024-01-01T06:00'::TIMESTAMP + x * 60_000_000L FROM long_sequence(8)");
        snapshot(snapshot);
        assertQuery("mv WHERE ts = '2024-01-01T06:00' ORDER BY country")
                .noLeakCheck()
                .returns("""
                        ts\tcountry\ttotal\tn\tavg_kwp
                        2024-01-01T06:00:00.000000Z\tES\t16.0\t4\t11.0
                        2024-01-01T06:00:00.000000Z\tIT\t18.0\t4\t10.0
                        """);
        Assert.assertEquals(isFused ? PRIMARY : ORDINARY, cachedFactory());
        dropTables();
        return snapshot.toString();
    }

    // Refreshes the view and appends its rows to the snapshot. A range refresh does not persist
    // its finish time, so the view may report itself as refreshing rather than valid.
    private void snapshot(StringBuilder snapshot) throws Exception {
        refresh();
        assertQuery("SELECT view_status IN ('valid', 'refreshing') is_refreshed, invalidation_reason FROM materialized_views")
                .noLeakCheck()
                .inferRandomAccess()
                .inferTimestamp()
                .sizeMayVary()
                .returns("is_refreshed\tinvalidation_reason\ntrue\t\n");
        sink.clear();
        printSql("mv ORDER BY ts, country");
        snapshot.append(sink);
    }
}
