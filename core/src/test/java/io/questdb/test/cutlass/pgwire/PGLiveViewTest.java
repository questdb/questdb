/*******************************************************************************
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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Live views over the PostgreSQL wire protocol.
 */
public class PGLiveViewTest extends BasePGTest {
    private static final String VIEW_SELECT = "SELECT ts, x, count(*) OVER (PARTITION BY 0 ORDER BY ts "
            + "ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0";

    @Test
    public void testAlterLiveViewWalControlAsPreparedStatementsExecutedTwice() throws Exception {
        // ALTER LIVE VIEW ... SUSPEND WAL and ALTER LIVE VIEW ... RESUME WAL as prepared statements, each
        // executed twice. Neither repeat fails: the first pair pauses the view, which then serves the rows it
        // had while the base moves on, and the second pair resumes it, after which it holds every row.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " + VIEW_SELECT);
            final TableToken lvToken = engine.verifyTableName("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-04-01T00:00:00.000000Z', 1)");
                drive(job);
                try (PreparedStatement suspend = connection.prepareStatement("ALTER LIVE VIEW lv SUSPEND WAL WITH 'DISK FULL', 'manual halt'")) {
                    suspend.execute();
                    suspend.execute();
                }
                Assert.assertTrue(engine.isWalApplySuspended(lvToken));
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(lvToken));
                execute("INSERT INTO base VALUES ('2026-04-01T01:00:00.000000Z', 2)");
                drive(job);
                assertViewRows("ts\tx\trn\n2026-04-01T00:00:00.000000Z\t1\t1\n");

                try (PreparedStatement resume = connection.prepareStatement("ALTER LIVE VIEW lv RESUME WAL")) {
                    resume.execute();
                    resume.execute();
                }
                Assert.assertFalse(engine.isWalApplySuspended(lvToken));
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lvToken));
                drive(job);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, VIEW_SELECT, "lv", LOG);
                assertViewRows("ts\tx\trn\n"
                        + "2026-04-01T00:00:00.000000Z\t1\t1\n"
                        + "2026-04-01T01:00:00.000000Z\t2\t2\n");
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateWithAnchorCompileErrorInScriptReportsScriptPosition() throws Exception {
        // In simple mode the server parses the whole script, so an error position is an offset into the
        // script, one-based on the wire. A CREATE LIVE VIEW after another statement reports an error from
        // compiling its anchor at the anchor expression's root token there too.
        assertWithPgServer(CONN_AWARE_SIMPLE, (connection, _, _, _) -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String script = "SELECT 1; CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS SELECT ts, sym, sum(x) OVER w a FROM base "
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', nope))";
            try (Statement statement = connection.createStatement()) {
                try {
                    statement.execute(script);
                    Assert.fail("the script must refuse the anchor");
                } catch (PSQLException e) {
                    final ServerErrorMessage serverError = e.getServerErrorMessage();
                    Assert.assertNotNull(serverError);
                    Assert.assertEquals("Invalid column: nope", serverError.getMessage());
                    Assert.assertEquals(script.indexOf("timestamp_floor") + 1, serverError.getPosition());
                }
            }
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
        });
    }

    @Test
    public void testCreateWithAnchorCompileErrorReportsTheAnchorOnEveryExecution() throws Exception {
        // A prepared CREATE LIVE VIEW whose anchor fails to compile, executed twice, for each kind of error
        // the anchor compile raises: an SqlException, an ImplicitCastException, a CairoException, and an
        // unknown ANCHOR DAILY time zone. Both executions report the root token of the anchor expression,
        // or the ANCHOR keyword for ANCHOR DAILY, which the server sends one-based, and neither leaves a
        // view behind.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // {the anchor clause, the token it fails at, the message}
            final String[][] cases = {
                    {"ANCHOR EXPRESSION timestamp_floor('1d', nope)", "timestamp_floor", "Invalid column: nope"},
                    {"ANCHOR EXPRESSION ts::long + 'a'::int", "+", "inconvertible value: a [CHAR -> INT]"},
                    {"ANCHOR EXPRESSION ts::long + (1.00m / 0.00m)::long", "+", "'/' operation failed: Division by zero"},
                    {"ANCHOR DAILY '00:00' 'Not/AZone'", "ANCHOR", "invalid timezone: Not/AZone"}
            };
            for (String[] c : cases) {
                final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, x, sum(x) OVER w a FROM base "
                        + "WINDOW w AS (PARTITION BY x ORDER BY ts " + c[0] + ")";
                Assert.assertEquals(sql.indexOf(c[1]), sql.lastIndexOf(c[1]));
                try (PreparedStatement create = connection.prepareStatement(sql)) {
                    for (int i = 0; i < 2; i++) {
                        try {
                            create.execute();
                            Assert.fail("execution " + i + " must refuse the anchor: " + sql);
                        } catch (PSQLException e) {
                            final ServerErrorMessage serverError = e.getServerErrorMessage();
                            Assert.assertNotNull(serverError);
                            Assert.assertEquals(c[2], serverError.getMessage());
                            Assert.assertEquals(sql, sql.indexOf(c[1]) + 1, serverError.getPosition());
                        }
                        Assert.assertNull(engine.getTableTokenIfExists("lv"));
                    }
                }
            }
        });
    }

    @Test
    public void testCreateWithCompileErrorInScriptReportsScriptPosition() throws Exception {
        // In simple mode the server parses the whole script, so an error position is an offset into the
        // script, one-based on the wire. A CREATE LIVE VIEW after another statement reports an error from
        // compiling its SELECT there too.
        assertWithPgServer(CONN_AWARE_SIMPLE, (connection, _, _, _) -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String script = "SELECT 1; CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS SELECT ts, sym, rnd_double() + "
                    + "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING) s FROM base";
            try (Statement statement = connection.createStatement()) {
                try {
                    statement.execute(script);
                    Assert.fail("the script must refuse the non-deterministic function");
                } catch (PSQLException e) {
                    final ServerErrorMessage serverError = e.getServerErrorMessage();
                    Assert.assertNotNull(serverError);
                    Assert.assertEquals("non-deterministic function cannot be used in live view: rnd_double", serverError.getMessage());
                    Assert.assertEquals(script.indexOf("rnd_double") + 1, serverError.getPosition());
                }
            }
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
        });
    }

    @Test
    public void testCreateWithCompileErrorReportsStatementPositionOnEveryExecution() throws Exception {
        // A prepared CREATE LIVE VIEW whose SELECT fails to compile, executed twice, for each kind of error
        // the compile raises: an SqlException, a CairoException and an ImplicitCastException. Both
        // executions report the offending token's position in the statement, which the server sends
        // one-based, and neither leaves a view behind.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // {projection item, the token it fails at, the message}
            final String[][] cases = {
                    {"rnd_double() r", "rnd_double", "non-deterministic function cannot be used in live view: rnd_double"},
                    {"1.00m / 0.00m q", "/", "'/' operation failed: Division by zero"},
                    {"12345::DECIMAL(4,2) d", "12345", "inconvertible value: 12345 [INT -> DECIMAL(4,2)]"}
            };
            for (String[] c : cases) {
                final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, x, " + c[0] + ", "
                        + "count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base";
                Assert.assertEquals(sql.indexOf(c[1]), sql.lastIndexOf(c[1]));
                try (PreparedStatement create = connection.prepareStatement(sql)) {
                    for (int i = 0; i < 2; i++) {
                        try {
                            create.execute();
                            Assert.fail("execution " + i + " must refuse the SELECT: " + sql);
                        } catch (PSQLException e) {
                            final ServerErrorMessage serverError = e.getServerErrorMessage();
                            Assert.assertNotNull(serverError);
                            Assert.assertEquals(c[2], serverError.getMessage());
                            Assert.assertEquals(sql, sql.indexOf(c[1]) + 1, serverError.getPosition());
                        }
                        Assert.assertNull(engine.getTableTokenIfExists("lv"));
                    }
                }
            }
        });
    }

    @Test
    public void testCreateWithUnstorableColumnFailsOnEveryExecution() throws Exception {
        // A prepared CREATE LIVE VIEW whose SELECT projects an untyped NULL, executed twice. Both
        // executions refuse the column at its position, which the server sends one-based, and
        // neither leaves a view behind.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, x, NULL n, "
                    + "count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base";
            try (PreparedStatement create = connection.prepareStatement(sql)) {
                for (int i = 0; i < 2; i++) {
                    try {
                        create.execute();
                        Assert.fail("execution " + i + " must refuse the NULL column");
                    } catch (PSQLException e) {
                        final ServerErrorMessage serverError = e.getServerErrorMessage();
                        Assert.assertNotNull(serverError);
                        Assert.assertEquals(
                                "cannot create NULL-type column, please use type cast, e.g. n::type",
                                serverError.getMessage()
                        );
                        Assert.assertEquals(sql.indexOf("NULL n") + 1, serverError.getPosition());
                    }
                    Assert.assertNull(engine.getTableTokenIfExists("lv"));
                }
            }
        });
    }

    // Applies the base and runs the refresh job with the clock past FLUSH EVERY until it reports no work.
    private static void drive(LiveViewRefreshJob job) {
        for (int pass = 0; pass < 64; pass++) {
            setCurrentMicros(currentMicros + 2_000_000L);
            drainWalQueue();
            boolean isWork = false;
            for (int i = 0; i < 64 && job.run(); i++) {
                isWork = true;
            }
            drainWalQueue();
            if (!isWork) {
                return;
            }
        }
        Assert.fail("the refresh job did not quiesce");
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("SELECT ts, x, rn FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }
}
