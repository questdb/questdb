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
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.ParallelHashJoinTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The parallel hash join inside materialized views: a view whose query joins its base table, which
 * every refresh narrows to the step's interval, refreshed with the operator on and off, with
 * {@code cairo.mat.view.parallel.sql.enabled} on and off, compared after every refresh; and views
 * with an {@code EXPIRE ROWS} policy read as the probe and as the build input of a join, which
 * returns the ordinary plan's rows whether or not the view's read qualifies for the operator.
 */
public class MatViewAsyncHashJoinTest extends AbstractCairoTest {
    private static final String OPERATOR = "operator";
    private static final String ORDINARY = "ordinary";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testExpireRowsViewsAsProbeAndBuild() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
            execute("CREATE TABLE base (sym SYMBOL, k INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base SELECT ('s' || (x % 3))::SYMBOL, (x % 5)::INT, (x % 7)::DOUBLE,
                        timestamp_sequence('2024-01-01', 3_600_000_000)
                    FROM long_sequence(80)
                    """);
            drainWalAndMatViewQueues();
            execute("CREATE TABLE dim (k INT, label STRING, w LONG)");
            execute("INSERT INTO dim SELECT (x % 6)::INT, 'l' || x, x FROM long_sequence(12)");
            // A timestamp cutoff and a value WHEN policy, whose reads scan the view's frames under a
            // filter that the operator steals, and a KEEP policy, whose read keeps the latest row of
            // each key and exposes no page frames, so the join keeps the ordinary plan.
            final String[] policies = {
                    "EXPIRE ROWS WHEN ts < '2024-01-02T12:00:00.000000Z'",
                    "EXPIRE ROWS WHEN v < 2",
                    "EXPIRE ROWS KEEP LATEST PARTITION BY sym",
            };
            final boolean[] isOperator = {true, true, false};
            for (int i = 0; i < policies.length; i++) {
                execute("CREATE MATERIALIZED VIEW mv" + i + " AS (SELECT * FROM base) " + policies[i]);
            }
            drainWalAndMatViewQueues();
            for (int i = 0; i < policies.length; i++) {
                final String mv = "mv" + i;
                for (String sql : new String[]{
                        "SELECT m.ts, m.sym, m.v, d.label, d.w FROM " + mv + " m LEFT JOIN dim d ON m.k = d.k",
                        "SELECT m.ts, m.sym, m.v, d.label FROM " + mv + " m JOIN dim d ON m.k = d.k WHERE m.v > 1",
                        "SELECT d.k, d.label, m.ts, m.v FROM dim d LEFT JOIN " + mv + " m ON d.k = m.k",
                        "SELECT b.ts, b.v, m.ts, m.v FROM base b LEFT JOIN " + mv + " m ON b.k = m.k AND m.v > b.v WHERE b.ts IN '2024-01-01'",
                }) {
                    assertMatchesOrdinaryPlan(sql, isOperator[i]);
                }
            }
        });
    }

    @Test
    public void testRefreshWithAndWithoutParallelSql() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isParallelSql : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, Boolean.toString(isParallelSql));
                // A view aggregates, and these aggregates the fused hash join GROUP BY does not take,
                // so the GROUP BY runs over the join.
                for (String view : new String[]{
                        "SELECT t.ts, p.country, first(t.qty) fq, last(p.kwp) lk, count() n FROM trades t LEFT JOIN plants p ON t.plant_id = p.plant_id SAMPLE BY 1h",
                        "SELECT t.ts, p.country, first(t.qty) fq, last(p.kwp) lk, count() n FROM trades t JOIN plants p ON t.plant_id = p.plant_id SAMPLE BY 1h",
                        "SELECT t.ts, t.plant_id, last(p.country) lc, first(p.kwp) fk, count() n FROM trades t LEFT JOIN plants p ON t.plant_id = p.plant_id AND p.kwp > t.qty / 10 SAMPLE BY 30m",
                }) {
                    final String expected = refreshes(view, false, false);
                    Assert.assertEquals(view, expected, refreshes(view, true, isParallelSql));
                }
            }
        });
    }

    @Test
    public void testRefreshOfStackedJoinViews() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isParallelSql : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, Boolean.toString(isParallelSql));
                // Views whose SAMPLE BY runs over a stack of two joins, a LEFT and an INNER one, with an
                // ON residual on the second.
                for (String view : new String[]{
                        "SELECT t.ts, p.country, o.owner, first(t.qty) fq, last(p.kwp) lk, count() n FROM trades t"
                                + " LEFT JOIN plants p ON t.plant_id = p.plant_id LEFT JOIN owners o ON t.plant_id = o.plant_id SAMPLE BY 1h",
                        "SELECT t.ts, o.owner, first(p.kwp) fk, last(t.qty) lq, count() n FROM trades t"
                                + " JOIN plants p ON t.plant_id = p.plant_id LEFT JOIN owners o ON t.plant_id = o.plant_id AND o.share > t.qty / 100 SAMPLE BY 30m",
                }) {
                    final String expected = refreshes(view, false, false);
                    Assert.assertEquals(view, expected, refreshes(view, true, isParallelSql));
                    // The refreshes ran a stack of both joins.
                    if (isParallelSql) {
                        sqlExecutionContext.setParallelHashJoinEnabled(true);
                        execute("CREATE TABLE trades (plant_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                        createDimensions();
                        try (RecordCursorFactory factory = select(view)) {
                            TestUtils.assertContains(ParallelHashJoinTestUtils.plan(factory, sqlExecutionContext), "stages: 2");
                        }
                        execute("DROP TABLE trades");
                        dropDimensions();
                    }
                }
            }
        });
    }

    private static void createDimensions() throws Exception {
        execute("CREATE TABLE plants (plant_id INT, country SYMBOL, kwp DOUBLE)");
        execute("INSERT INTO plants VALUES (1, 'ES', 5), (2, 'IT', 7), (2, 'FR', 8), (3, 'ES', null), (null, 'FR', 9)");
        execute("CREATE TABLE owners (plant_id INT, owner SYMBOL, share DOUBLE)");
        execute("INSERT INTO owners VALUES (1, 'a', 0.5), (1, 'b', 0.5), (2, 'c', 1), (4, 'd', 0.25), (null, 'e', 1)");
    }

    private static void dropDimensions() throws Exception {
        execute("DROP TABLE plants");
        execute("DROP TABLE owners");
    }

    // The factory the last refresh cached in the view state, described by whether it runs the
    // operator.
    private static String cachedFactory() {
        final TableToken token = engine.verifyTableName("mv");
        final MatViewState state = engine.getMatViewStateStore().getViewState(token);
        Assert.assertNotNull(state);
        Assert.assertTrue(state.tryLock());
        try (RecordCursorFactory factory = state.acquireRecordFactory()) {
            Assert.assertNotNull(factory);
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                if (f instanceof AsyncHashJoinRecordCursorFactory) {
                    return OPERATOR;
                }
            }
            return ORDINARY;
        } finally {
            state.unlock();
        }
    }

    private static void snapshot(StringBuilder snapshot) throws Exception {
        drainWalAndMatViewQueues();
        drainPurgeJob();
        final StringSink sink = new StringSink();
        printSql("SELECT * FROM mv ORDER BY 1, 2, 3, 4, 5", sink);
        snapshot.append(sink).append("----\n");
    }

    private static void assertMatchesOrdinaryPlan(String sql, boolean isOperator) throws Exception {
        if (isOperator) {
            ParallelHashJoinTestUtils.assertParallelHashJoin(sql, sqlExecutionContext);
        } else {
            ParallelHashJoinTestUtils.assertOrdinaryPlan(sql, sqlExecutionContext);
        }
    }

    // Creates the tables and the view, writes and refreshes in steps, and returns the view after
    // every refresh. The refreshes run the operator when it is on and the refresh context runs
    // parallel SQL.
    private String refreshes(String view, boolean isOperatorEnabled, boolean isParallelSql) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_ENABLED, Boolean.toString(isOperatorEnabled));
        execute("CREATE TABLE trades (plant_id INT, qty DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        createDimensions();
        execute("CREATE MATERIALIZED VIEW mv WITH BASE trades AS (" + view + ") PARTITION BY DAY");
        final StringBuilder snapshot = new StringBuilder();
        final String expectedFactory = isOperatorEnabled && isParallelSql ? OPERATOR : ORDINARY;
        execute("""
                INSERT INTO trades VALUES
                (1, 10, '2024-01-01T00:10'), (2, 20, '2024-01-01T00:20'), (3, 30, '2024-01-01T01:10'),
                (1, 40, '2024-01-01T02:10'), (4, 50, '2024-01-01T02:20'), (null, 60, '2024-01-01T02:30')
                """);
        snapshot(snapshot);
        Assert.assertEquals(expectedFactory, cachedFactory());
        // A late row lands in the first bucket, so the refresh scans every bucket up to the new one.
        execute("INSERT INTO trades VALUES (2, 5, '2024-01-01T03:05'), (1, 15, '2024-01-01T00:40'), (3, 25, '2024-01-01T03:15')");
        snapshot(snapshot);
        Assert.assertEquals(expectedFactory, cachedFactory());
        // A new plant, which the next step's rows see.
        execute("INSERT INTO plants VALUES (4, 'IT', 3)");
        execute("INSERT INTO trades SELECT (x % 5)::INT, x, timestamp_sequence('2024-01-01T04', 60_000_000) FROM long_sequence(200)");
        snapshot(snapshot);
        Assert.assertEquals(expectedFactory, cachedFactory());
        execute("DROP MATERIALIZED VIEW mv");
        execute("DROP TABLE trades");
        dropDimensions();
        drainWalAndMatViewQueues();
        return snapshot.toString();
    }
}
