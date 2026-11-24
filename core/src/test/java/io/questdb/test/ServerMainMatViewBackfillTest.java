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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.griffin.SqlParser;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static io.questdb.test.AbstractCairoTest.parseFloorPartialTimestamp;

public class ServerMainMatViewBackfillTest extends AbstractBootstrapTest {
    private static final long HOUR_MICROS = 3_600_000_000L;
    private static final Log LOG = LogFactory.getLog(ServerMainMatViewBackfillTest.class);

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testConcurrentBackfillAndRefreshFuzz() throws Exception {
        // Concurrency fuzz for the frozen-zone backfill path. While the worker pool runs
        // IMMEDIATE refreshes (triggered by an advancing base frontier), the test thread
        // hammers the view with frozen-zone backfills. Every backfill sits below the
        // boundary, and the boundary only rises, so the gate and the commit-time validator
        // must accept all of them and no concurrent refresh (REPLACE_RANGE + boundary-floor
        // publish) may lose or corrupt any. This stresses the validator-floor read vs
        // refresh-floor publish race and the dual-WAL-writer + apply-ordering interaction
        // that the deterministic C1/C1' tests only cover sequentially.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true"
            )) {
                final CairoEngine engine = serverMain.getEngine();
                serverMain.execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
                serverMain.execute("insert into base_price values('a', 1.0, '2024-09-10T12:00')");
                serverMain.awaitTxn("base_price", 1);
                serverMain.execute("create materialized view price_1h refresh immediate as " +
                        "select sym, last(price) price, ts from base_price sample by 1h");
                serverMain.execute("alter materialized view price_1h set refresh limit 1 hour");

                // Wait for SET REFRESH LIMIT to apply (ground-truth poll on the gate predicate).
                final TableToken viewToken = engine.verifyTableName("price_1h");
                long deadline = System.currentTimeMillis() + 60_000;
                while (!engine.isBackfillableMatView(viewToken) && System.currentTimeMillis() < deadline) {
                    Os.sleep(5);
                }
                Assert.assertTrue("SET REFRESH LIMIT did not apply in time", engine.isBackfillableMatView(viewToken));

                final int backfills = 300;
                final long bucketMicros = 3_600_000_000L; // 1h
                // Distinct frozen buckets, descending from just below the initial boundary
                // (base max 12:00, limit 1h -> boundary 11:00). The boundary only rises as
                // base advances, so every backfill stays frozen for the whole run.
                final long frozenTop = parseFloorPartialTimestamp("2024-09-10T10:00:00.000000Z");
                long advanceTs = parseFloorPartialTimestamp("2024-09-10T13:00:00.000000Z");

                int rejected = 0;
                for (int i = 0; i < backfills; i++) {
                    // Randomised cadence/step for the base frontier -> the boundary-movement
                    // pattern (and so the overlap with refreshes) varies run-to-run. Each
                    // advance triggers an IMMEDIATE auto-refresh on the worker pool, racing the
                    // backfills. Base stays below the real wall clock.
                    if (rnd.nextInt(8) == 0) {
                        serverMain.execute("insert into base_price values('a', 2.0, " + advanceTs + "::timestamp)");
                        advanceTs += bucketMicros * (1 + rnd.nextInt(4));
                    }
                    final long ts = frozenTop - (long) i * bucketMicros; // distinct, descending, all frozen
                    try {
                        serverMain.execute("insert into price_1h values('a', " + (i + 1) + ", " + ts + "::timestamp)");
                    } catch (Exception rejectedEx) {
                        // Count only an expected engine/SQL rejection; let Errors (AssertionError,
                        // OutOfMemoryError) propagate so a real failure fails the test fast instead
                        // of being miscounted as a rejection.
                        rejected++;
                    }
                }
                Assert.assertEquals("frozen backfills must never be rejected", 0, rejected);

                // Wait for all WAL (backfills + concurrent refreshes) to apply, then verify every
                // frozen backfill survived: rows below the boundary must equal the backfill count
                // (refresh only materialises managed-zone rows at-or-above 11:00).
                deadline = System.currentTimeMillis() + 120_000;
                boolean survived = false;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        serverMain.assertSql(
                                "select count() from price_1h where ts < '2024-09-10T11:00:00.000000Z'",
                                "count\n" + backfills + "\n"
                        );
                        survived = true;
                        break;
                    } catch (AssertionError e) {
                        Os.sleep(50);
                    }
                }
                Assert.assertTrue("not all frozen backfills survived concurrent refresh", survived);
            }
        });
    }

    @Test
    public void testIlpBackfillCannotAddViewColumn() throws Exception {
        // A line protocol backfill into a view with REFRESH LIMIT must not add a column the view lacks: a view's
        // columns change only through ALTER MATERIALIZED VIEW, which refuses the change on a view whose SQL relies
        // on generated column names. The line names a frozen-zone row, which the commit-time check would accept.
        TestUtils.assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true"
            )) {
                final CairoEngine engine = serverMain.getEngine();
                serverMain.execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
                serverMain.execute("insert into base_price values ('a', 1.0, '2024-09-10T09:10'), ('a', 2.0, '2024-09-10T12:00')");
                serverMain.awaitTxn("base_price", 1);
                final String[] viewNames = {"named_http", "named_tcp", "gen_http", "gen_tcp"};
                for (String viewName : viewNames) {
                    if (viewName.startsWith("gen")) {
                        // a view of the previous version, whose SQL relies on generated column names
                        SqlParser.setMatViewExplicitNamesCheckSkipped(true);
                        try {
                            serverMain.execute("create materialized view " + viewName + " as (select sym, last(price), ts from base_price sample by 1h)");
                        } finally {
                            SqlParser.setMatViewExplicitNamesCheckSkipped(false);
                        }
                    } else {
                        serverMain.execute("create materialized view " + viewName + " as (select sym, last(price) price, ts from base_price sample by 1h)");
                    }
                }
                // the first build covers every bucket; the limit then freezes the ones below 11:00
                for (String viewName : viewNames) {
                    awaitRefreshed(engine, viewName, 1, false);
                    serverMain.execute("alter materialized view " + viewName + " set refresh limit 1 hour");
                }
                for (String viewName : viewNames) {
                    awaitRefreshed(engine, viewName, 1, true);
                }

                // frozen zone: base max 12:00, limit 1h -> boundary 11:00
                final long frozenTs = parseFloorPartialTimestamp("2024-09-10T08:00");
                for (String viewName : new String[]{"named_http", "gen_http"}) {
                    try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("localhost:" + serverMain.getHttpServerPort()).build()) {
                        sender.table(viewName)
                                .symbol("sym", "a")
                                .doubleColumn("extra", 42.0)
                                .at(frozenTs, ChronoUnit.MICROS);
                        sender.flush();
                        Assert.fail("expected the line to be refused [view=" + viewName + ']');
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "table: " + viewName + ", column: extra does not exist; "
                                + "add columns to a materialized view with ALTER MATERIALIZED VIEW ... ADD COLUMN");
                    }
                }

                capture.start();
                for (String viewName : new String[]{"named_tcp", "gen_tcp"}) {
                    try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost").port(ILP_PORT).build()) {
                        sender.table(viewName)
                                .symbol("sym", "a")
                                .doubleColumn("extra", 42.0)
                                .at(frozenTs, ChronoUnit.MICROS);
                        sender.flush();
                    }
                    // the line either fails or, without the check, adds the column
                    capture.waitForRegex("could not write ILP message \\[tableName=" + viewName + "|columnName=extra");
                }
                capture.assertNotLogged("columnName=extra");

                // the next refresh: the views keep their columns and stay valid
                serverMain.execute("insert into base_price values ('a', 3.0, '2024-09-10T13:10')");
                serverMain.awaitTxn("base_price", 2);
                for (String viewName : viewNames) {
                    awaitRefreshed(engine, viewName, 2, true);
                    final String valueColumn = viewName.startsWith("gen") ? "last" : "price";
                    serverMain.assertSql(
                            "select \"column\", type from table_columns('" + viewName + "')",
                            "column\ttype\n" +
                                    "sym\tSYMBOL\n" +
                                    valueColumn + "\tDOUBLE\n" +
                                    "ts\tTIMESTAMP\n"
                    );
                    serverMain.assertSql(
                            "select view_status, invalidation_reason from materialized_views() where view_name = '" + viewName + "'",
                            "view_status\tinvalidation_reason\n" +
                                    "valid\t\n"
                    );
                    serverMain.assertSql(
                            "select * from " + viewName,
                            "sym\t" + valueColumn + "\tts\n" +
                                    "a\t1.0\t2024-09-10T09:00:00.000000Z\n" +
                                    "a\t2.0\t2024-09-10T12:00:00.000000Z\n" +
                                    "a\t3.0\t2024-09-10T13:00:00.000000Z\n"
                    );
                }
            } finally {
                capture.stop();
            }
        });
    }

    @Test
    public void testIlpBackfillRejectedAfterRefreshLimitRemoved() throws Exception {
        // A line protocol connection keeps the table it resolved, past the backfill gate, for as long as it stays open.
        // Rows it sends after ALTER MATERIALIZED VIEW ... SET REFRESH LIMIT 0 meet the commit-time check, which refuses
        // them with the SQL gate's text "[view=v]"; the line protocol gate over HTTP says "materialized view: v".
        TestUtils.assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true",
                    // a TCP connection keeps an idle table for 10 minutes rather than 500 ms, so its next line reuses it
                    PropertyKey.LINE_TCP_MIN_IDLE_MS_BEFORE_WRITER_RELEASE.getEnvVarName(), "600000"
            )) {
                final CairoEngine engine = serverMain.getEngine();
                serverMain.execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
                serverMain.execute("insert into base_price values ('a', 1.0, '2024-09-10T09:10'), ('a', 2.0, '2024-09-10T12:00')");
                serverMain.awaitTxn("base_price", 1);
                final String[] viewNames = {"v_http", "v_tcp"};
                for (String viewName : viewNames) {
                    serverMain.execute("create materialized view " + viewName + " as (select sym, last(price) price, ts from base_price sample by 1h)");
                }
                // the first build covers every bucket; the limit then freezes the ones below 11:00
                for (String viewName : viewNames) {
                    awaitRefreshed(engine, viewName, 1, false);
                    serverMain.execute("alter materialized view " + viewName + " set refresh limit 1 hour");
                }
                for (String viewName : viewNames) {
                    awaitRefreshed(engine, viewName, 1, true);
                }

                // frozen zone: base max 12:00, limit 1h -> boundary 11:00
                final long frozenTs = parseFloorPartialTimestamp("2024-09-10T08:00");
                final long managedTs = parseFloorPartialTimestamp("2024-09-10T12:00");
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + serverMain.getHttpServerPort())
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    sender.table("v_http").symbol("sym", "a").doubleColumn("price", 5.0).at(frozenTs, ChronoUnit.MICROS);
                    sender.flush();
                    setRefreshLimit(serverMain, engine, "v_http", false);

                    sender.table("v_http").symbol("sym", "a").doubleColumn("price", 99.0).at(managedTs, ChronoUnit.MICROS);
                    try {
                        sender.flush();
                        Assert.fail("the stale connection's commit must be refused");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "cannot modify materialized view [view=v_http]");
                    }
                    // the failed commit dropped the connection's table, so the retry meets the line protocol gate
                    try {
                        sender.flush();
                        Assert.fail("the retry must be refused by the gate");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "cannot modify materialized view: v_http");
                    }
                }

                capture.start();
                try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost").port(ILP_PORT).build()) {
                    sender.table("v_tcp").symbol("sym", "a").doubleColumn("price", 5.0).at(frozenTs, ChronoUnit.MICROS);
                    sender.flush();
                    awaitCount(serverMain, "select count() from v_tcp where ts = '2024-09-10T08:00:00.000000Z'", 1);
                    setRefreshLimit(serverMain, engine, "v_tcp", false);

                    sender.table("v_tcp").symbol("sym", "a").doubleColumn("price", 99.0).at(managedTs, ChronoUnit.MICROS);
                    sender.flush();
                    // the connection's commit fails: the line protocol gate fails a line without committing
                    capture.waitForRegex("commit[^\\n]*\\[table=v_tcp[^\\n]*cannot modify materialized view \\[view=v_tcp\\]");
                }

                for (String viewName : viewNames) {
                    awaitRefreshed(engine, viewName, 1, false);
                    serverMain.assertSql(
                            "select * from " + viewName,
                            """
                                    sym\tprice\tts
                                    a\t5.0\t2024-09-10T08:00:00.000000Z
                                    a\t1.0\t2024-09-10T09:00:00.000000Z
                                    a\t2.0\t2024-09-10T12:00:00.000000Z
                                    """
                    );
                    serverMain.assertSql(
                            "select view_status, invalidation_reason from materialized_views() where view_name = '" + viewName + "'",
                            "view_status\tinvalidation_reason\n" +
                                    "valid\t\n"
                    );
                }
            } finally {
                capture.stop();
            }
        });
    }

    @Test
    public void testIlpHttpStaleViewHandleRefusedBeforeOtherTables() throws Exception {
        // A keep-alive connection keeps the view it resolved past the backfill gate. Once ALTER ... SET REFRESH LIMIT 0
        // has applied, a request that writes a plain table's row and a view row through it is refused before any table
        // commits, with the gate's message, as a client error the sender does not send again. The sender's next
        // connection meets the gate itself.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true"
            )) {
                final CairoEngine engine = serverMain.getEngine();
                createBackfillableView(serverMain, engine);
                final long frozenTs = parseFloorPartialTimestamp("2024-09-10T08:00");
                try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("localhost:" + serverMain.getHttpServerPort()).build()) {
                    sender.table("v").symbol("sym", "a").doubleColumn("price", 5.0).at(frozenTs, ChronoUnit.MICROS);
                    sender.flush();
                    setRefreshLimit(serverMain, engine, "v", false);
                    final long tSeqTxn = seqTxn(engine, "t");
                    final long viewSeqTxn = seqTxn(engine, "v");

                    sender.table("t").longColumn("x", 1).at(parseFloorPartialTimestamp("2024-09-10T00:00"), ChronoUnit.MICROS);
                    sender.table("v").symbol("sym", "a").doubleColumn("price", 6.0).at(frozenTs - HOUR_MICROS, ChronoUnit.MICROS);
                    try {
                        sender.flush();
                        Assert.fail("the stale connection's request must be refused");
                    } catch (LineSenderException e) {
                        Assert.assertFalse(e.isRetryable());
                        TestUtils.assertContains(e.getMessage(), "Could not flush buffer: commit error for table: v, errno: -1, "
                                + "error: cannot modify materialized view [view=v] [http-status=400, id: ");
                        TestUtils.assertContains(e.getMessage(), ", code: invalid]");
                    }
                    Assert.assertEquals(tSeqTxn, seqTxn(engine, "t"));
                    Assert.assertEquals(viewSeqTxn, seqTxn(engine, "v"));

                    sender.reset();
                    sender.table("v").symbol("sym", "a").doubleColumn("price", 6.0).at(frozenTs - HOUR_MICROS, ChronoUnit.MICROS);
                    try {
                        sender.flush();
                        Assert.fail("the next connection must meet the gate");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "error in line 1: table: v; cannot modify materialized view: v");
                    }
                    sender.reset();
                }
                serverMain.assertSql("select count() from t", "count\n0\n");
                awaitRefreshed(engine, "v", 1, false);
                serverMain.assertSql("select * from v", """
                        sym\tprice\tts
                        a\t5.0\t2024-09-10T08:00:00.000000Z
                        a\t1.0\t2024-09-10T09:00:00.000000Z
                        a\t2.0\t2024-09-10T12:00:00.000000Z
                        """);
            }
        });
    }

    @Test
    public void testIlpTcpManagedZoneRowStillRefusedAtCommit() throws Exception {
        // ILP/TCP keeps refusing a managed-zone view row only at the view's commit: the sender is told nothing, the
        // server logs the refusal, and the view keeps its rows and stays valid. A new connection's frozen-zone row
        // lands.
        TestUtils.assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true"
            )) {
                final CairoEngine engine = serverMain.getEngine();
                createBackfillableView(serverMain, engine);
                final long viewSeqTxn = seqTxn(engine, "v");
                capture.start();
                try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost").port(ILP_PORT).build()) {
                    sender.table("v").symbol("sym", "a").doubleColumn("price", 95.0)
                            .at(parseFloorPartialTimestamp("2024-09-10T12:45"), ChronoUnit.MICROS);
                    sender.flush();
                }
                capture.waitForRegex("commit[^\\n]*\\[table=v[^\\n]*backfill row falls in or past the managed zone of materialized view \\[view=v, maxRowTs=2024-09-10T12:45:00.000000Z");
                Assert.assertEquals(viewSeqTxn, seqTxn(engine, "v"));

                try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost").port(ILP_PORT).build()) {
                    sender.table("v").symbol("sym", "a").doubleColumn("price", 7.0)
                            .at(parseFloorPartialTimestamp("2024-09-10T08:00"), ChronoUnit.MICROS);
                    sender.flush();
                }
                awaitCount(serverMain, "select count() from v where ts = '2024-09-10T08:00:00.000000Z'", 1);
                awaitRefreshed(engine, "v", 1, true);
                serverMain.assertSql("select * from v", """
                        sym\tprice\tts
                        a\t7.0\t2024-09-10T08:00:00.000000Z
                        a\t1.0\t2024-09-10T09:00:00.000000Z
                        a\t2.0\t2024-09-10T12:00:00.000000Z
                        """);
                serverMain.assertSql(
                        "select view_status, invalidation_reason from materialized_views() where view_name = 'v'",
                        "view_status\tinvalidation_reason\n" +
                                "valid\t\n"
                );
            } finally {
                capture.stop();
            }
        });
    }

    @Test
    public void testMixedTableBackfillFlushFuzz() throws Exception {
        // ILP/HTTP and QWP flushes write rows of 1-17 plain tables and 0-3 rows of a REFRESH LIMIT view, each view row
        // in the frozen or the managed zone, in random order. A flush with a managed-zone view row is refused as a
        // client error, ILP/HTTP 400 or a terminal QWP SCHEMA_MISMATCH, and commits nothing; any other flush commits
        // every row once. The tables' row counts must match that model.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final String commitMode = switch (rnd.nextInt(3)) {
            case 0 -> "nosync";
            case 1 -> "adaptive";
            default -> "sync";
        };
        final int flushCount = Integer.getInteger("questdb.fuzz.backfill.flushes", 40);
        LOG.info().$("mixed backfill flush fuzz [commitMode=").$(commitMode).$(", flushes=").$(flushCount).I$();
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true",
                    PropertyKey.CAIRO_COMMIT_MODE.getEnvVarName(), commitMode
            )) {
                final CairoEngine engine = serverMain.getEngine();
                createBackfillableView(serverMain, engine);
                // the ILP/HTTP connection's table map grows past 16 entries
                final int plainTableCount = 17;
                for (int i = 0; i < plainTableCount; i++) {
                    serverMain.execute("create table p" + i + " (x long, ts timestamp) timestamp(ts) partition by DAY WAL");
                }
                for (int i = 0; i < plainTableCount; i++) {
                    serverMain.awaitTable("p" + i);
                }
                final long[] expectedPlainRows = new long[plainTableCount];
                long expectedViewRows = 0;
                int refusedFlushes = 0;
                int acceptedFlushes = 0;
                // frozen-zone view rows fall before the view's 09:00 bucket, managed-zone ones after 11:00
                final long frozenLo = parseFloorPartialTimestamp("2024-09-08T00:00");
                final long managedLo = parseFloorPartialTimestamp("2024-09-10T11:00");
                final long plainTs = parseFloorPartialTimestamp("2024-09-10T00:00");
                final String httpAddress = "localhost:" + serverMain.getHttpServerPort();
                try (Sender httpSender = Sender.builder(Sender.Transport.HTTP).address(httpAddress).build()) {
                    for (int flush = 0; flush < flushCount; flush++) {
                        // both outcomes in every run: the first flush has a managed-zone view row, the second none
                        final int viewRowCount = flush < 2 ? 1 : rnd.nextInt(4);
                        final boolean[] isManaged = new boolean[viewRowCount];
                        boolean hasManagedRow = false;
                        for (int i = 0; i < viewRowCount; i++) {
                            isManaged[i] = flush == 0 || (flush > 1 && rnd.nextInt(3) == 0);
                            hasManagedRow |= isManaged[i];
                        }
                        // the plain tables the flush writes, and the rows of each
                        final int[] plainRows = new int[plainTableCount];
                        final int tablesInFlush = 1 + rnd.nextInt(plainTableCount);
                        int rowCount = viewRowCount;
                        for (int i = 0; i < tablesInFlush; i++) {
                            final int rows = 1 + rnd.nextInt(3);
                            plainRows[rnd.nextInt(plainTableCount)] += rows;
                            rowCount += rows;
                        }
                        // a random order of the flush's rows: -1 - i for view row i, else a plain table index
                        final int[] order = new int[rowCount];
                        int n = 0;
                        for (int i = 0; i < viewRowCount; i++) {
                            order[n++] = -1 - i;
                        }
                        for (int t = 0; t < plainTableCount; t++) {
                            for (int r = 0; r < plainRows[t]; r++) {
                                order[n++] = t;
                            }
                        }
                        for (int i = rowCount - 1; i > 0; i--) {
                            final int j = rnd.nextInt(i + 1);
                            final int swap = order[i];
                            order[i] = order[j];
                            order[j] = swap;
                        }
                        final boolean isHttp = rnd.nextBoolean();
                        if (isHttp) {
                            writeFuzzRows(httpSender, order, isManaged, flush, frozenLo, managedLo, plainTs);
                            try {
                                httpSender.flush();
                                Assert.assertFalse("a flush with a managed-zone row must be refused [flush=" + flush + ']', hasManagedRow);
                            } catch (LineSenderException e) {
                                Assert.assertTrue("only a managed-zone row refuses a flush [flush=" + flush + ", e=" + e.getMessage() + ']', hasManagedRow);
                                Assert.assertFalse(e.isRetryable());
                                TestUtils.assertContains(e.getMessage(), "backfill row falls in or past the managed zone of materialized view [view=v, ");
                                TestUtils.assertContains(e.getMessage(), "[http-status=400, ");
                                httpSender.reset();
                            }
                        } else {
                            final CompletableFuture<SenderError> errorFut = new CompletableFuture<>();
                            final QwpWebSocketSender qwpSender = (QwpWebSocketSender) Sender.builder(Sender.Transport.WEBSOCKET)
                                    .address(httpAddress)
                                    .errorHandler(errorFut::complete)
                                    .build();
                            try {
                                writeFuzzRows(qwpSender, order, isManaged, flush, frozenLo, managedLo, plainTs);
                                long fsn = -1;
                                try {
                                    fsn = qwpSender.flushAndGetSequence();
                                } catch (LineSenderServerException e) {
                                    // the I/O thread latched the terminal before the flush's own error poll ran
                                }
                                if (hasManagedRow) {
                                    final SenderError error = errorFut.get(30, TimeUnit.SECONDS);
                                    Assert.assertSame(SenderError.Category.SCHEMA_MISMATCH, error.getCategory());
                                    Assert.assertSame(SenderError.Policy.TERMINAL, error.getAppliedPolicy());
                                    TestUtils.assertContains(error.getServerMessage(), "backfill row falls in or past the managed zone of materialized view [view=v, ");
                                } else {
                                    Assert.assertTrue("the flush must be acknowledged [flush=" + flush + ']', qwpSender.awaitAckedFsn(fsn, 30_000));
                                    Assert.assertFalse(errorFut.isDone());
                                }
                            } finally {
                                try {
                                    qwpSender.close();
                                } catch (LineSenderServerException e) {
                                    // the latched terminal, asserted above
                                }
                            }
                        }
                        if (hasManagedRow) {
                            refusedFlushes++;
                        } else {
                            acceptedFlushes++;
                            expectedViewRows += viewRowCount;
                            for (int t = 0; t < plainTableCount; t++) {
                                expectedPlainRows[t] += plainRows[t];
                            }
                        }
                    }
                }
                LOG.info().$("mixed backfill flush fuzz [accepted=").$(acceptedFlushes).$(", refused=").$(refusedFlushes).I$();
                for (int t = 0; t < plainTableCount; t++) {
                    awaitCount(serverMain, "select count() from p" + t, expectedPlainRows[t]);
                }
                awaitCount(serverMain, "select count() from v where ts < '2024-09-10T09:00:00.000000Z'", expectedViewRows);
                awaitRefreshed(engine, "v", 1, true);
                serverMain.assertSql("select count() from v where ts >= '2024-09-10T09:00:00.000000Z'", "count\n2\n");
                serverMain.assertSql(
                        "select view_status, invalidation_reason from materialized_views() where view_name = 'v'",
                        "view_status\tinvalidation_reason\n" +
                                "valid\t\n"
                );
            }
        });
    }

    @Test
    public void testQwpManagedZoneRowAfterBlockAtUncommittedRowsCapIsTerminal() throws Exception {
        // A block that reaches the connection's max-uncommitted-rows cap cannot join a commit batch, so its frame
        // takes the normal path, where the appender commits the block's table at the cap before the frame's next
        // block appends. A managed-zone view block after it is refused as it appends, as a terminal SCHEMA_MISMATCH:
        // the table that committed at the cap keeps its rows once, and no replay adds them again.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true",
                    PropertyKey.QWP_MAX_UNCOMMITTED_ROWS.getEnvVarName(), "2"
            )) {
                final CairoEngine engine = serverMain.getEngine();
                createBackfillableView(serverMain, engine);
                final long tSeqTxn = seqTxn(engine, "t");
                final long viewSeqTxn = seqTxn(engine, "v");
                final CopyOnWriteArrayList<SenderError> errors = new CopyOnWriteArrayList<>();
                final CompletableFuture<SenderError> terminalFut = new CompletableFuture<>();
                final QwpWebSocketSender sender = (QwpWebSocketSender) Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + serverMain.getHttpServerPort())
                        .errorHandler(err -> {
                            errors.add(err);
                            if (err.getAppliedPolicy() == SenderError.Policy.TERMINAL) {
                                terminalFut.complete(err);
                            }
                        })
                        .build();
                try {
                    final long rowTs = parseFloorPartialTimestamp("2024-09-10T00:00");
                    sender.table("t").longColumn("x", 1).at(rowTs, ChronoUnit.MICROS);
                    sender.table("t").longColumn("x", 2).at(rowTs + 1, ChronoUnit.MICROS);
                    sender.table("v").symbol("sym", "a").doubleColumn("price", 95.0)
                            .at(parseFloorPartialTimestamp("2024-09-10T12:45"), ChronoUnit.MICROS);
                    try {
                        sender.flush();
                    } catch (LineSenderServerException e) {
                        // the I/O thread latched the terminal before the flush's own error poll ran
                    }
                    final SenderError terminal;
                    try {
                        terminal = terminalFut.get(60, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError("the managed-zone row must be refused", e);
                    }
                    Assert.assertEquals("the first error is the terminal one: " + errors, 1, errors.size());
                    Assert.assertSame(SenderError.Category.SCHEMA_MISMATCH, terminal.getCategory());
                    Assert.assertSame(SenderError.Policy.TERMINAL, terminal.getAppliedPolicy());
                    TestUtils.assertContains(terminal.getServerMessage(), "backfill row falls in or past the managed zone of materialized view [view=v, maxRowTs=2024-09-10T12:45:00.000000Z");
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderServerException e) {
                        // the latched terminal, asserted above
                    }
                }
                Assert.assertEquals(tSeqTxn + 1, seqTxn(engine, "t"));
                Assert.assertEquals(viewSeqTxn, seqTxn(engine, "v"));
                awaitCount(serverMain, "select count() from t", 2);
                serverMain.assertSql("select x, ts from t", """
                        x\tts
                        1\t2024-09-10T00:00:00.000000Z
                        2\t2024-09-10T00:00:00.000001Z
                        """);
                serverMain.assertSql("select * from v", """
                        sym\tprice\tts
                        a\t1.0\t2024-09-10T09:00:00.000000Z
                        a\t2.0\t2024-09-10T12:00:00.000000Z
                        """);
            }
        });
    }

    @Test
    public void testStaleBackfillAcrossRefreshLimitChangesFuzz() throws Exception {
        // One pg-wire connection keeps an INSERT prepared on the server while the view's REFRESH LIMIT goes back and
        // forth between 0 and 1 hour, and IMMEDIATE refreshes run as the base advances. An execution lands exactly
        // when the applied limit is 1 hour; otherwise its commit is refused with the backfill gate's message.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true"
            )) {
                final CairoEngine engine = serverMain.getEngine();
                serverMain.execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
                serverMain.execute("insert into base_price values('a', 1.0, '2024-09-10T12:00')");
                serverMain.awaitTxn("base_price", 1);
                serverMain.execute("create materialized view price_1h refresh immediate as " +
                        "select sym, last(price) price, ts from base_price sample by 1h");
                awaitRefreshed(engine, "price_1h", 1, false);
                setRefreshLimit(serverMain, engine, "price_1h", true);

                final Properties properties = new Properties();
                properties.setProperty("user", "admin");
                properties.setProperty("password", "quest");
                // the server keeps the statement prepared from its first execution
                properties.setProperty("prepareThreshold", "-1");
                int acceptedCount = 0;
                int refusedCount = 0;
                try (
                        Connection connection = DriverManager.getConnection(getPgConnectionUri(serverMain.getPgWireServerPort()), properties);
                        PreparedStatement insert = connection.prepareStatement("insert into price_1h values ('a', ?, ?)")
                ) {
                    // One frozen bucket per execution, descending from 09:00: base max 12:00 and the limit of 1 hour
                    // put the boundary at 11:00, and it only rises as the base advances.
                    long backfillTs = parseFloorPartialTimestamp("2024-09-10T09:00");
                    long advanceTs = parseFloorPartialTimestamp("2024-09-10T13:00");
                    // both outcomes in every run: an execution under the limit, then one after its removal
                    executeBackfill(insert, backfillTs, true);
                    acceptedCount++;
                    backfillTs -= HOUR_MICROS;
                    boolean isLimitSet = false;
                    setRefreshLimit(serverMain, engine, "price_1h", false);
                    executeBackfill(insert, backfillTs, false);
                    refusedCount++;
                    backfillTs -= HOUR_MICROS;

                    for (int step = 0; step < 200; step++) {
                        switch (rnd.nextInt(4)) {
                            case 0 -> {
                                isLimitSet = !isLimitSet;
                                setRefreshLimit(serverMain, engine, "price_1h", isLimitSet);
                            }
                            case 1 -> {
                                // triggers an IMMEDIATE refresh; the base stays below the real wall clock
                                serverMain.execute("insert into base_price values('a', 2.0, " + advanceTs + "::timestamp)");
                                advanceTs += HOUR_MICROS * (1 + rnd.nextInt(4));
                            }
                            default -> {
                                executeBackfill(insert, backfillTs, isLimitSet);
                                if (isLimitSet) {
                                    acceptedCount++;
                                } else {
                                    refusedCount++;
                                }
                                backfillTs -= HOUR_MICROS;
                            }
                        }
                    }
                }
                LOG.info().$("backfill executions [accepted=").$(acceptedCount).$(", refused=").$(refusedCount).I$();

                // every accepted backfill lands, and no refused one: refreshes write only buckets from 12:00 on
                awaitRefreshed(engine, "price_1h", 1, false);
                serverMain.assertSql(
                        "select count() from price_1h where ts < '2024-09-10T11:00:00.000000Z'",
                        "count\n" + acceptedCount + '\n'
                );
            }
        });
    }

    // Waits until the count query returns the expected count, as the WAL apply catches up.
    private static void awaitCount(TestServerMain serverMain, String countSql, long expectedCount) {
        final String expected = "count\n" + expectedCount + '\n';
        final long deadline = System.currentTimeMillis() + 120_000;
        while (true) {
            try {
                serverMain.assertSql(countSql, expected);
                return;
            } catch (AssertionError e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Os.sleep(50);
            }
        }
    }

    // Waits until the view has refreshed up to the base txn, its WAL has applied and, when asked, it accepts backfill.
    private static void awaitRefreshed(CairoEngine engine, String viewName, long baseTxn, boolean isBackfillable) {
        final TableToken viewToken = engine.verifyTableName(viewName);
        final long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
            final SeqTxnTracker viewTxnTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            if ((!isBackfillable || engine.isBackfillableMatView(viewToken)) && viewState != null && viewState.getLastRefreshBaseTxn() >= baseTxn
                    && viewTxnTracker.getWriterTxn() >= viewTxnTracker.getSeqTxn()) {
                return;
            }
            Os.sleep(10);
        }
        Assert.fail("materialized view did not refresh in time [view=" + viewName + ", baseTxn=" + baseTxn + ']');
    }

    // A WAL base table with rows at 09:10 and 12:00 on 2024-09-10 and a view v of its 1h buckets with REFRESH LIMIT
    // 1 HOUR: base max 12:00 puts the boundary bucket floor at 11:00. Plain WAL tables t and t2 take other rows.
    private static void createBackfillableView(TestServerMain serverMain, CairoEngine engine) {
        serverMain.execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
        serverMain.execute("insert into base_price values ('a', 1.0, '2024-09-10T09:10'), ('a', 2.0, '2024-09-10T12:00')");
        serverMain.awaitTxn("base_price", 1);
        serverMain.execute("create materialized view v as (select sym, last(price) price, ts from base_price sample by 1h)");
        awaitRefreshed(engine, "v", 1, false);
        setRefreshLimit(serverMain, engine, "v", true);
        awaitRefreshed(engine, "v", 1, true);
        serverMain.execute("create table t (x long, ts timestamp) timestamp(ts) partition by DAY WAL");
        serverMain.execute("create table t2 (x long, ts timestamp) timestamp(ts) partition by DAY WAL");
        serverMain.awaitTable("t");
        serverMain.awaitTable("t2");
    }

    private static void executeBackfill(PreparedStatement insert, long ts, boolean isAccepted) throws Exception {
        insert.setDouble(1, 1.0);
        insert.setString(2, Micros.toUSecString(ts));
        if (isAccepted) {
            Assert.assertEquals(1, insert.executeUpdate());
            return;
        }
        try {
            insert.executeUpdate();
            Assert.fail("the INSERT must fail while the view has no REFRESH LIMIT [ts=" + ts + ']');
        } catch (PSQLException e) {
            Assert.assertNotNull(e.getServerErrorMessage());
            TestUtils.assertEquals("cannot modify materialized view [view=price_1h]", e.getServerErrorMessage().getMessage());
        }
    }

    private static long seqTxn(CairoEngine engine, String tableName) {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tableName));
    }

    // Sets the view's REFRESH LIMIT to 1 hour, or removes it, and waits until the change has applied.
    private static void setRefreshLimit(TestServerMain serverMain, CairoEngine engine, String viewName, boolean isLimitSet) {
        serverMain.execute("alter materialized view " + viewName + " set refresh limit " + (isLimitSet ? "1 hour" : "0 hour"));
        final TableToken viewToken = engine.verifyTableName(viewName);
        final long deadline = System.currentTimeMillis() + 60_000;
        while (engine.isBackfillableMatView(viewToken) != isLimitSet) {
            if (System.currentTimeMillis() > deadline) {
                Assert.fail("SET REFRESH LIMIT did not apply in time [view=" + viewName + ", isLimitSet=" + isLimitSet + ']');
            }
            Os.sleep(5);
        }
    }
    // Writes a fuzz flush's rows in the given order: -1 - i is view row i, in the frozen or the managed zone, any other
    // value the index of the plain table that takes the next row. Timestamps differ per flush and row.
    private static void writeFuzzRows(Sender sender, int[] order, boolean[] isManaged, int flush, long frozenLo, long managedLo, long plainTs) {
        for (int i = 0, n = order.length; i < n; i++) {
            final int target = order[i];
            if (target < 0) {
                final int viewRow = -1 - target;
                final long ts = (isManaged[viewRow] ? managedLo + HOUR_MICROS : frozenLo) + flush * 1_000L + viewRow;
                sender.table("v").symbol("sym", "a").doubleColumn("price", flush).at(ts, ChronoUnit.MICROS);
            } else {
                sender.table("p" + target).longColumn("x", flush).at(plainTs + flush * 1_000L + i, ChronoUnit.MICROS);
            }
        }
    }

}
