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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * ALTER MATERIALIZED VIEW ... AS next to row-level retention (#7263, design section 11) and at the edges of its
 * input.
 * <ul>
 *     <li>Rule 7: applying ALTER ... AS replaces only the SQL and base name of the definition, so a passthrough
 *     view keeps its EXPIRE ROWS policy, also across a restart and in SHOW CREATE.</li>
 *     <li>Rule 5: a materialized view cannot read a view with a policy; ALTER ... WITH BASE inherits that CREATE
 *     rule, and RESUME and FULL refuse such a base as refresh does.</li>
 *     <li>Bind variables stay refused, as CREATE refuses them; 4-byte UTF-8 and a SQL text of about 97 KB round-trip
 *     through the definition event and a restart; a cast the copiers lack stays refused.</li>
 * </ul>
 */
public class MatViewAlterAsEdgeCasesTest extends AbstractCairoTest {

    @Test
    public void testAlterAsKeepsExpirePolicyOfPassthroughView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                    ('2024-01-02T00:00:00.000000Z', 'a', 3.0),
                    ('2024-01-03T00:00:00.000000Z', 'b', 0.5)
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW pt AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();
            // the policy hides every row below 2.0
            assertQuery("pt ORDER BY ts, k").timestamp("ts").noLeakCheck().returns("""
                    ts\tk\tv
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    """);

            final String newSql = "SELECT ts, k, v FROM base WHERE k = 'a'";
            execute("ALTER MATERIALIZED VIEW pt AS (" + newSql + ")");
            drainWalAndMatViewQueues();
            execute("""
                    INSERT INTO base VALUES
                    ('2024-01-04T00:00:00.000000Z', 'a', 1.5),
                    ('2024-01-05T00:00:00.000000Z', 'a', 5.0),
                    ('2024-01-06T00:00:00.000000Z', 'b', 6.0)
                    """);
            drainWalAndMatViewQueues();
            // The new SQL drops b's rows from the refreshed range, and the policy still hides a's row below 2.0.
            final String visible = """
                    ts\tk\tv
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-05T00:00:00.000000Z\ta\t5.0
                    """;
            assertQuery("pt ORDER BY ts, k").timestamp("ts").noLeakCheck().returns(visible);
            final String catalogue = "view_status\tinvalidation_reason\texpire_clause\tview_sql\n"
                    + "valid\t\tv < 2.0\t" + newSql + '\n';
            assertQuery("SELECT view_status, invalidation_reason, expire_clause, view_sql FROM materialized_views()")
                    .noLeakCheck().noRandomAccess().returns(catalogue);
            sink.clear();
            printSql("SHOW CREATE MATERIALIZED VIEW pt", sink);
            TestUtils.assertContains(sink, newSql);
            TestUtils.assertContains(sink, "EXPIRE ROWS WHEN v < 2.0");

            restartEngine((restarted, context) -> {
                drainWalAndMatViewQueues(restarted);
                assertQuery("SELECT view_status, invalidation_reason, expire_clause, view_sql FROM materialized_views()")
                        .withEngine(restarted).withContext(context).noLeakCheck().noRandomAccess().returns(catalogue);
                assertQuery("pt ORDER BY ts, k").withEngine(restarted).withContext(context).timestamp("ts").noLeakCheck().returns(visible);
            });
        });
    }

    @Test
    public void testAlterAsKeepsKeepLatestPolicyOfPassthroughView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                    ('2024-01-02T00:00:00.000000Z', 'b', 2.0)
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW pt AS (SELECT * FROM base) EXPIRE ROWS KEEP LATEST PARTITION BY k");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW pt AS (SELECT ts, k, v * 10 AS v FROM base)");
            drainWalAndMatViewQueues();
            execute("""
                    INSERT INTO base VALUES
                    ('2024-01-03T00:00:00.000000Z', 'a', 3.0),
                    ('2024-01-04T00:00:00.000000Z', 'c', 4.0)
                    """);
            drainWalAndMatViewQueues();
            // The newest row of each key; the rows after the ALTER carry the new SQL's values.
            assertQuery("pt ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tk\tv
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\ta\t30.0
                    2024-01-04T00:00:00.000000Z\tc\t40.0
                    """);
            assertQuery("SELECT view_status, expire_clause FROM materialized_views()").noLeakCheck().noRandomAccess().returns("""
                    view_status\texpire_clause
                    valid\tKEEP LATEST PARTITION BY k
                    """);
        });
    }

    @Test
    public void testAlterAsLongAndMultiByteSqlRoundTrips() throws Exception {
        // The definition event carries 4-byte UTF-8 in an alias and a literal, and a SQL text of about 97 KB; the
        // stored SQL is the statement's text, and a restart reads it back.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'é𝄞', 1.0)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, k, sum(v) AS sv FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN \"цена𝄞\" DOUBLE");
            drainWalAndMatViewQueues();
            final String utf8Sql = "SELECT ts, k, sum(v) AS sv, sum(v) * 2 AS \"цена𝄞\" FROM base WHERE k <> 'ж𝄢' SAMPLE BY 1h";
            execute("ALTER MATERIALIZED VIEW mv AS (" + utf8Sql + ")");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('2024-01-01T01:00:00.000000Z', 'é𝄞', 2.0), ('2024-01-01T01:30:00.000000Z', 'ж𝄢', 7.0)");
            drainWalAndMatViewQueues();
            final String utf8Rows = """
                    ts\tk\tsv\tцена𝄞
                    2024-01-01T00:00:00.000000Z\té𝄞\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\té𝄞\t2.0\t4.0
                    """;
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(utf8Rows);
            assertQuery("SELECT view_status, view_sql FROM materialized_views()").noLeakCheck().noRandomAccess()
                    .returns("view_status\tview_sql\nvalid\t" + utf8Sql + '\n');
            restartEngine((restarted, context) -> {
                Assert.assertEquals(
                        utf8Sql,
                        restarted.getDependentViewGraph().getViewDefinition(restarted.verifyTableName("mv")).getMatViewSql()
                );
                assertQuery("mv ORDER BY ts").withEngine(restarted).withContext(context).timestamp("ts").noLeakCheck().expectSize().returns(utf8Rows);
            });

            final StringBuilder notIn = new StringBuilder();
            for (int i = 0; i < 12_000; i++) {
                if (i > 0) {
                    notIn.append(',');
                }
                notIn.append("'s").append(i).append('\'');
            }
            final String longSql = "SELECT ts, k, sum(v) AS sv FROM base WHERE k NOT IN (" + notIn + ") SAMPLE BY 1h";
            Assert.assertTrue(longSql.length() > 90_000);
            execute("ALTER MATERIALIZED VIEW mv AS (" + longSql + ")");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('2024-01-01T02:00:00.000000Z', 's1', 4.0), ('2024-01-01T02:10:00.000000Z', 'x', 5.0)");
            drainWalAndMatViewQueues();
            final String longRows = utf8Rows + """
                    2024-01-01T02:00:00.000000Z\tx\t5.0\tnull
                    """;
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(longRows);
            restartEngine((restarted, context) -> {
                final TableToken viewToken = restarted.verifyTableName("mv");
                Assert.assertEquals(longSql, restarted.getDependentViewGraph().getViewDefinition(viewToken).getMatViewSql());
                // the restarted engine refreshes with the long SQL
                restarted.execute("INSERT INTO base VALUES ('2024-01-01T03:00:00.000000Z', 's2', 6.0), ('2024-01-01T03:10:00.000000Z', 'y', 7.0)", context);
                drainWalAndMatViewQueues(restarted);
                assertQuery("SELECT ts, k, sv FROM mv WHERE ts >= '2024-01-01T03:00:00.000000Z'")
                        .withEngine(restarted).withContext(context).timestamp("ts").noLeakCheck().returns("""
                                ts\tk\tsv
                                2024-01-01T03:00:00.000000Z\ty\t7.0
                                """);
            });
        });
    }

    @Test
    public void testAlterAsRefusesCastTheCopiersLack() throws Exception {
        // A DOUBLE[] query column cannot feed a DOUBLE[][] view column: ALTER ... AS refuses at the query column and
        // writes nothing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, arr DOUBLE[], v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'a', ARRAY[1.0, 2.0], 1.5)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, k, arr, v FROM base)");
            drainWalAndMatViewQueues();
            // the view's SQL does not produce arr2 yet, so ADD COLUMN has no pair to check
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN arr2 DOUBLE[][]");
            drainWalAndMatViewQueues();
            final long viewSeqTxn = viewSeqTxn("mv");
            final String sql = "ALTER MATERIALIZED VIEW mv AS (SELECT ts, k, arr, v, arr AS arr2 FROM base)";
            assertExceptionNoLeakCheck(sql, sql.indexOf("arr AS arr2"), "inconvertible types: DOUBLE[] -> DOUBLE[][] [from=arr2, to=arr2]");
            Assert.assertEquals(viewSeqTxn, viewSeqTxn("mv"));
            assertQuery("SELECT view_status, view_sql FROM materialized_views()").noLeakCheck().noRandomAccess().returns("""
                    view_status\tview_sql
                    valid\tSELECT ts, k, arr, v FROM base
                    """);
        });
    }

    @Test
    public void testAlterWithBaseRefusesViewWithExpirePolicy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1.0), ('2024-01-02T00:00:00.000000Z', 'a', 3.0)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW pt AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 2.0");
            execute("CREATE MATERIALIZED VIEW agg AS (SELECT ts, k, sum(v) AS sv FROM base SAMPLE BY 1d) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tk\tsv
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    """;
            assertQuery("agg ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(rows);
            final long viewSeqTxn = viewSeqTxn("agg");

            final String sql = "ALTER MATERIALIZED VIEW agg WITH BASE pt AS (SELECT ts, k, sum(v) AS sv FROM pt SAMPLE BY 1d)";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("pt SAMPLE"),
                    "cannot materialize view 'agg': source materialized view 'pt' has an active EXPIRE ROWS policy"
            );
            drainWalAndMatViewQueues();
            // nothing was written: the view keeps its SQL, base, rows and state
            Assert.assertEquals(viewSeqTxn, viewSeqTxn("agg"));
            assertQuery("SELECT view_status, invalidation_reason, base_table_name, view_sql FROM materialized_views() WHERE view_name = 'agg'")
                    .noLeakCheck().noRandomAccess().returns("""
                            view_status\tinvalidation_reason\tbase_table_name\tview_sql
                            valid\t\tbase\tSELECT ts, k, sum(v) AS sv FROM base SAMPLE BY 1d
                            """);
            assertQuery("agg ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(rows);
            execute("INSERT INTO base VALUES ('2024-01-02T01:00:00.000000Z', 'a', 4.0)");
            drainWalAndMatViewQueues();
            assertQuery("agg ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tk\tsv
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\ta\t7.0
                    """);
        });
    }

    @Test
    public void testBindVariablesRefusedInCreateAndAlterAs() throws Exception {
        // A view's SQL is stored and refreshed without the statement's bind values; CREATE refuses a bind variable,
        // and ALTER ... AS refuses it the same way.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, k, v FROM base)");
            drainWalAndMatViewQueues();
            final long viewSeqTxn = viewSeqTxn("mv");
            bindVariableService.clear();
            bindVariableService.setDouble(0, 1.0);
            final String create = "CREATE MATERIALIZED VIEW mv_bind AS (SELECT ts, k, v FROM base WHERE v > $1)";
            assertExceptionNoLeakCheck(create, create.indexOf('>'), "non-deterministic function cannot be used in materialized view: >");
            final String alter = "ALTER MATERIALIZED VIEW mv AS (SELECT ts, k, v FROM base WHERE v > $1)";
            assertExceptionNoLeakCheck(alter, alter.indexOf('>'), "non-deterministic function cannot be used in materialized view: >");
            bindVariableService.clear();
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewSeqTxn, viewSeqTxn("mv"));
            assertQuery("SELECT view_name, view_status, view_sql FROM materialized_views()").noLeakCheck().noRandomAccess().returns("""
                    view_name\tview_status\tview_sql
                    mv\tvalid\tSELECT ts, k, v FROM base
                    """);
        });
    }

    @Test
    public void testResumeAndFullRefuseParentWithExpirePolicy() throws Exception {
        // A parent view gains a policy while its child is invalid. RESUME refuses the child, which cannot read the
        // parent any more, and FULL keeps the child's rows and makes the policy error its reason. Once the parent
        // drops its policy, RESUME catches the child up.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1.0), ('2024-01-01T01:00:00.000000Z', 'a', 3.0)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW parent AS (SELECT ts, k, sum(v) AS sv FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW child AS (SELECT ts, k, sum(sv) AS sv FROM parent SAMPLE BY 1d) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String childRows = """
                    ts\tk\tsv
                    2024-01-01T00:00:00.000000Z\ta\t4.0
                    """;
            assertQuery("child").timestamp("ts").noLeakCheck().expectSize().returns(childRows);
            engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("child"), "test invalidation");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW parent SET EXPIRE ROWS WHEN sv < 2.0");
            drainWalAndMatViewQueues();

            final String policyError = "cannot materialize view 'child': source materialized view 'parent' has an active EXPIRE ROWS policy";
            final String resume = "REFRESH MATERIALIZED VIEW child RESUME";
            assertExceptionNoLeakCheck(
                    resume,
                    resume.indexOf("child"),
                    "materialized view SQL does not compile, fix it with ALTER MATERIALIZED VIEW ... AS first [view=child, error=" + policyError + ']'
            );
            assertChildState("invalid", "test invalidation");

            execute("REFRESH MATERIALIZED VIEW child FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertChildState("invalid", policyError);
            // FULL compiles the SQL before it truncates the view
            assertQuery("child").timestamp("ts").noLeakCheck().expectSize().returns(childRows);

            execute("ALTER MATERIALIZED VIEW parent DROP EXPIRE");
            execute("INSERT INTO base VALUES ('2024-01-01T02:00:00.000000Z', 'a', 5.0)");
            drainWalAndMatViewQueues();
            execute(resume);
            drainWalAndMatViewQueues();
            assertChildState("valid", "");
            assertQuery("child").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tk\tsv
                    2024-01-01T00:00:00.000000Z\ta\t9.0
                    """);
        });
    }

    private void assertChildState(String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'child'")
                .noLeakCheck().noRandomAccess().returns("view_status\tinvalidation_reason\n" + status + '\t' + reason + '\n');
    }

    // Stops the engine and starts another one on the same data directory, which loads the views from disk.
    private void restartEngine(RestartedEngineCheck check) throws Exception {
        engine.releaseInactive();
        engine.releaseAllWriters();
        try (
                CairoEngine restarted = new CairoEngine(configuration);
                SqlExecutionContext restartedContext = TestUtils.createSqlExecutionCtx(restarted)
        ) {
            restarted.load();
            restarted.buildViewGraphs();
            check.run(restarted, restartedContext);
        }
    }

    private static long viewSeqTxn(String viewName) {
        return engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName(viewName)).getSeqTxn();
    }

    @FunctionalInterface
    private interface RestartedEngineCheck {
        void run(CairoEngine restarted, SqlExecutionContext context) throws Exception;
    }
}
