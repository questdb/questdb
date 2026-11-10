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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The plans of the parallel hash join: the shapes it takes, as EXPLAIN prints them, and every shape
 * that it leaves to the ordinary plan, each for one reason.
 */
public class AsyncHashJoinPlanTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        // read_parquet() reads files under the input root.
        inputRoot = root;
    }

    @Test
    public void testBackwardProbePlan() throws Exception {
        createTables();
        // The left input scans backward, as ORDER BY its timestamp descending asks, and the join keeps
        // its order, so the plan sorts nothing.
        assertQuery("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k ORDER BY t.ts DESC")
                .assertsPlan("""
                        SelectedRecord
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 1
                                    Probe
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: t
                                    Build
                                      joinType: left outer
                                      condition: d.k=t.k
                                      buildPayload: copied when the probe is large enough
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d
                        """);
    }

    @Test
    public void testInnerIntervalPlan() throws Exception {
        createTables();
        assertQuery("SELECT t.k, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'")
                .assertsPlan("""
                        SelectedRecord
                            Async Hash Join workers: 1
                              stages: 1
                                Probe
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                                Build
                                  joinType: inner
                                  condition: d.k=t.k
                                  buildPayload: copied when the probe is large enough
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: d
                        """);
    }

    @Test
    public void testJsonPlan() throws Exception {
        createTables();
        final StringSink sink = new StringSink();
        printSql("EXPLAIN (FORMAT JSON) SELECT t.k, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'", sink);
        final String plan = sink.toString();
        // The JSON plan nests the probe and the build node, with its attributes, under the operator.
        TestUtils.assertContains(plan, "\"Node Type\": \"Async Hash Join\"");
        TestUtils.assertContains(plan, "\"stages\":  1");
        TestUtils.assertContains(plan, "\"Node Type\": \"Probe\"");
        TestUtils.assertContains(plan, "\"Node Type\": \"Build\"");
        TestUtils.assertContains(plan, "\"joinType\": \"inner\"");
        TestUtils.assertContains(plan, "\"condition\": \"d.k=t.k\"");
        Assert.assertTrue(plan, plan.indexOf("\"Node Type\": \"Probe\"") < plan.indexOf("\"Node Type\": \"Build\""));
    }

    @Test
    public void testLeftPlan() throws Exception {
        createTables();
        // The build reads every column of d, STRING, BINARY and ARRAY ones included, which the payload
        // copy cannot hold, so the plan names no copy.
        assertQuery("SELECT * FROM t LEFT JOIN d ON t.k = d.k")
                .assertsPlan("""
                        SelectedRecord
                            Async Hash Join workers: 1
                              stages: 1
                                Probe
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                Build
                                  joinType: left outer
                                  condition: d.k=t.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: d
                        """);
        // A SYMBOL key compares the probe's keys translated into the build's dictionary, and a payload
        // of fixed-size columns alone may be copied.
        assertQuery("SELECT t.k, d2.w FROM t LEFT JOIN (SELECT s, w FROM d) d2 ON t.s = d2.s")
                .assertsPlan("""
                        SelectedRecord
                            Async Hash Join workers: 1
                              stages: 1
                                Probe
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                Build
                                  joinType: left outer
                                  condition: d2.s=t.s
                                  symbolKeyJoin: true
                                  buildPayload: copied when the probe is large enough
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: d
                        """);
    }

    @Test
    public void testPlanInsideScalarSubQuery() throws Exception {
        createTables();
        // The join's plan prints inside an aggregate of the outer GROUP BY, which prints its functions
        // over its base metadata: the join gives the plan sink back its metadata and mode, so the
        // text that follows the sub-query's plan reads as in the ordinary plan.
        final String sql = "SELECT t.k, sum(CASE WHEN t.v > (SELECT d.w FROM t t2 LEFT JOIN d ON t2.k = d.k LIMIT 1) THEN t.v ELSE 0 END) s FROM t ORDER BY 1";
        final String plan = plan(sql);
        Assert.assertTrue(plan, plan.contains("Async Hash Join"));
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        final String ordinaryPlan = plan(sql);
        Assert.assertFalse(ordinaryPlan, ordinaryPlan.contains("Async Hash Join"));
        final String tail = ordinaryPlan.substring(ordinaryPlan.lastIndexOf("Frame forward scan on: d") + "Frame forward scan on: d".length());
        // The outer aggregate's next argument resolves as column v of t.
        Assert.assertTrue(ordinaryPlan, tail.startsWith(",v,0]))]"));
        Assert.assertTrue(plan, plan.endsWith(tail));
    }

    @Test
    public void testRejectedBuildInputs() throws Exception {
        createTables();
        // A build input that exposes no page frames: a GROUP BY result, a filter that the join cannot
        // steal since it applies a LIMIT, a LATEST BY.
        assertOrdinaryPlan("SELECT t.k, d2.w FROM t LEFT JOIN (SELECT k, max(w) w FROM d) d2 ON t.k = d2.k", sqlExecutionContext);
        assertOrdinaryPlan("SELECT t.k, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 2 LIMIT 3) d2 ON t.k = d2.k", sqlExecutionContext);
        assertOrdinaryPlan("SELECT t.k, d2.w FROM t LEFT JOIN (SELECT * FROM q LATEST ON qts PARTITION BY k) d2 ON t.k = d2.k", sqlExecutionContext);
        // A build whose record cursor offers no random access: its ordinary plan chains duplicates
        // oldest first.
        assertOrdinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN '*!*d' d ON t.k = d.k", sqlExecutionContext);
        // A build that reads through a covering index.
        assertOrdinaryPlan("SELECT t.k, c.v FROM t LEFT JOIN (SELECT k, v FROM c WHERE s = 's1') c ON t.k = c.k", sqlExecutionContext);
    }

    @Test
    public void testRejectedJoinShapes() throws Exception {
        createTables();
        // RIGHT and FULL joins.
        assertOrdinaryPlan("SELECT t.k, d.w FROM t RIGHT JOIN d ON t.k = d.k", sqlExecutionContext);
        assertOrdinaryPlan("SELECT t.k, d.w FROM t FULL JOIN d ON t.k = d.k", sqlExecutionContext);
        // A LEFT join's ON condition beyond its keys whose value may change within an execution.
        assertOrdinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k AND (d.w > 2 OR systimestamp() < t.ts)", sqlExecutionContext);
        // An INNER join whose ordinary plan may swap its inputs: neither scans an interval.
        assertOrdinaryPlan("SELECT t.k, d.w FROM t JOIN d ON t.k = d.k", sqlExecutionContext);
        assertOrdinaryPlan("SELECT t.k, d.w FROM (SELECT k, ts FROM t) t JOIN d ON t.k = d.k", sqlExecutionContext);
        // BINARY and ARRAY keys, which the operator does not stage.
        assertOrdinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.b = d.b", sqlExecutionContext);
        assertOrdinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.a = d.a", sqlExecutionContext);
        // A probe column that two keys read through two encodings.
        assertOrdinaryPlan("SELECT t.k, d2.w FROM t LEFT JOIN (SELECT s s1, s::STRING s2, w FROM d) d2 ON t.s = d2.s1 AND t.s = d2.s2", sqlExecutionContext);
        // A probe input that exposes no page frames: a GROUP BY result or a filter that the join
        // cannot steal since it applies a LIMIT. The vectorized GROUP BY seeds its hash table with the
        // table's address, so its keys come in an order that may change between two executions; the
        // sort fixes it, so that the two executions that assertOrdinaryPlan() compares agree.
        assertOrdinaryPlan("SELECT t2.k, d.w FROM (SELECT k, max(v) v FROM t ORDER BY k) t2 LEFT JOIN d ON t2.k = d.k", sqlExecutionContext);
        assertOrdinaryPlan("SELECT t2.k, d.w FROM (SELECT * FROM t WHERE v > 2 LIMIT 5) t2 LEFT JOIN d ON t2.k = d.k", sqlExecutionContext);
        // The second join of a query, whose left input is a join.
        assertQuery("SELECT t.k, d.w, d2.w FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN d d2 ON t.k = d2.k")
                .assertsPlan("""
                        SelectedRecord
                            Hash Left Outer Join Light
                              condition: d2.k=t.k
                                Async Hash Join workers: 1
                                  stages: 1
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Build
                                      joinType: left outer
                                      condition: d.k=t.k
                                      buildPayload: copied when the probe is large enough
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: d
                        """);
        assertParallelHashJoin("SELECT t.k, d.w, d2.w FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN d d2 ON t.k = d2.k", sqlExecutionContext);
        // A left input without random access cannot swap, so its INNER join takes the operator.
        assertParallelHashJoin("SELECT t.k, d.w FROM '*!*t' t JOIN d ON t.k = d.k", sqlExecutionContext);
    }

    @Test
    public void testRejectedLiveViewBuildWithSymbolKey() throws Exception {
        createTables();
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, y, sum(y) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) sy FROM base");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 's1', 1.0), ('2024-01-01T00:00:30.000000Z', 's2', 2.0), ('2024-01-01T00:01:00.000000Z', 's1', 3.0)");
            drainWalQueue();
            //noinspection StatementWithEmptyBody
            for (int i = 0; i < 64 && job.run(); i++) {
            }
            drainWalQueue();
        }
        // A live view's dictionary can hold a text under two keys, so a SYMBOL key cannot translate
        // into it: the join that hashes the view keeps the ordinary plan, which translates each of
        // the view's keys through its text.
        assertOrdinaryPlan("SELECT t.k, lv.y FROM t LEFT JOIN lv ON t.s = lv.sym", sqlExecutionContext);
        // The view may stream, and a build over a table translates the view's keys into its own.
        assertParallelHashJoin("SELECT lv.y, d.w FROM lv LEFT JOIN d ON lv.sym = d.s", sqlExecutionContext);
    }

    @Test
    public void testRejectedReadParquetInnerJoin() throws Exception {
        createTables();
        sqlExecutionContext.setParallelReadParquetEnabled(true);
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader("d")
        ) {
            path.of(root).concat("d.parquet");
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
        }
        // read_parquet() answers its cursor's size itself, so the serial INNER join may swap over it,
        // which the operator does not reproduce yet: the join keeps the ordinary plan.
        assertOrdinaryPlan("SELECT t.k, p.w FROM t JOIN read_parquet('d.parquet') p ON t.k = p.k", sqlExecutionContext);
        // A LEFT join never swaps.
        assertParallelHashJoin("SELECT t.k, p.w FROM t LEFT JOIN read_parquet('d.parquet') p ON t.k = p.k", sqlExecutionContext);
    }

    @Test
    public void testRejectedSettings() throws Exception {
        createTables();
        final String sql = "SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k";
        assertParallelHashJoin(sql, sqlExecutionContext);
        // The operator is off.
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        Assert.assertFalse(plan(sql).contains("Async Hash Join"));
        // No shared query worker runs it, even with the flag on.
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 0).with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)) {
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = engine.select(sql, context)) {
                Assert.assertNull(find(factory));
                Assert.assertFalse(ParallelHashJoinTestUtils.plan(factory, context).contains("Async Hash Join"));
            }
        }
        // The compiler plans full-fat joins.
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(true);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertNull(find(factory));
            } finally {
                // The compiler goes back to the pool.
                compiler.setFullFatJoins(false);
            }
        }
    }

    @Test
    public void testUpdateFromKeepsOrdinaryPlan() throws Exception {
        createTables();
        execute("CREATE TABLE u AS (SELECT * FROM t)");
        execute("CREATE TABLE u2 AS (SELECT * FROM t)");
        // UPDATE reads the rows it updates by row id, which the operator does not serve.
        execute("UPDATE u SET v = d.w FROM d WHERE u.k = d.k");
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        execute("UPDATE u2 SET v = d.w FROM d WHERE u2.k = d.k");
        assertQuery("SELECT * FROM u")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(rows("SELECT * FROM u2"));
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT
                        (x % 5)::INT k,
                        x v,
                        ('s' || (x % 3))::SYMBOL s,
                        rnd_bin(1, 4, 1) b,
                        ARRAY[x::DOUBLE] a,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(20)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        execute("""
                CREATE TABLE d AS (
                    SELECT
                        (x % 4)::INT k,
                        x w,
                        ('s' || (x % 2))::SYMBOL s,
                        'v' || x v,
                        rnd_bin(1, 4, 1) b,
                        ARRAY[x::DOUBLE] a
                    FROM long_sequence(6)
                )
                """);
        execute("CREATE TABLE q AS (SELECT (x % 3)::INT k, x w, timestamp_sequence('2024-01-01', 600_000_000) qts FROM long_sequence(9)) TIMESTAMP(qts) PARTITION BY HOUR");
        execute("CREATE TABLE c (s SYMBOL INDEX TYPE POSTING INCLUDE (k, v), k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO c SELECT ('s' || (x % 3))::SYMBOL, (x % 4)::INT, x, timestamp_sequence('2024-01-01', 600_000_000) FROM long_sequence(12)");
        drainWalQueue();
    }

    private static String plan(String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            return ParallelHashJoinTestUtils.plan(factory, sqlExecutionContext);
        }
    }

    private static String rows(String sql) throws Exception {
        final StringSink sink = new StringSink();
        printSql(sql, sink);
        return sink.toString();
    }
}
