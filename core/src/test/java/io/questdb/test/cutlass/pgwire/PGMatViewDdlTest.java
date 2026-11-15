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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.PGConnection;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The materialized view statements of this feature over pg-wire, driven by the PostgreSQL JDBC driver: view ADD and
 * DROP COLUMN, ALTER ... AS, REFRESH ... RESUME and view DROP PARTITION, each prepared once and executed twice, in
 * the simple and the extended protocol. A second execution either repeats the statement or fails the way the same
 * ALTER TABLE does on a table, and the connection stays usable.
 */
public class PGMatViewDdlTest extends BasePGTest {
    private static final String VIEW_ROWS = """
            ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE]
            2024-01-01 00:00:00.0,a,1.0
            2024-01-02 00:00:00.0,a,2.0
            2024-01-03 00:00:00.0,a,3.0
            2024-01-04 00:00:00.0,a,4.0
            """;

    @Test
    public void testAlterAsExecutedTwice() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createBaseAndView();
            final String newSql = "SELECT ts, sym, sum(price) * 2 AS price FROM base SAMPLE BY 1h";
            try (PreparedStatement alter = connection.prepareStatement("ALTER MATERIALIZED VIEW mv AS (" + newSql + ")")) {
                alter.execute();
                drainWalAndMatViewQueues();
                alter.execute();
                drainWalAndMatViewQueues();
            }
            assertQueryResult(connection, "SELECT view_status, view_sql FROM materialized_views()",
                    "view_status[VARCHAR],view_sql[VARCHAR]\nvalid," + newSql + '\n');
            // the existing rows stay; the next refresh uses the new SQL
            execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-05T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", VIEW_ROWS + "2024-01-05 00:00:00.0,a,10.0\n");
        });
    }

    @Test
    public void testAddColumnReExecutedAfterAlterAsChangedTypeInvalidatesAtRefresh() throws Exception {
        // A prepared ADD COLUMN compiled while the view's SQL did not produce the column executes again after ALTER
        // ... AS made the SQL produce it as a BOOLEAN, which has no cast to the column's DOUBLE. A fresh compile refuses
        // the statement. pg-wire re-runs the statement it compiled, which skips ADD COLUMN's cast check; refresh
        // then decides, as for a later base ALTER COLUMN TYPE (design section 4): the view goes invalid with the cast
        // error, keeps its rows, and its WAL keeps applying.
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> {
            createBaseAndView();
            final String addSql = "ALTER MATERIALIZED VIEW mv ADD COLUMN flag DOUBLE";
            try (PreparedStatement add = connection.prepareStatement(addSql)) {
                add.execute();
                drainWalAndMatViewQueues();
                execute("ALTER MATERIALIZED VIEW mv DROP COLUMN flag");
                execute("ALTER MATERIALIZED VIEW mv AS (SELECT ts, sym, sum(price) AS price, sum(price) > 2.5 AS flag FROM base SAMPLE BY 1h)");
                drainWalAndMatViewQueues();
                assertQueryResult(connection, "SELECT view_status, ignored_query_columns FROM materialized_views()",
                        "view_status[VARCHAR],ignored_query_columns[VARCHAR]\nvalid,flag\n");
                assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", VIEW_ROWS);
                TestUtils.assertEquals("inconvertible types: BOOLEAN -> DOUBLE [from=flag, to=flag]", freshError(connection, addSql).getMessage());
                add.execute();
                drainWalAndMatViewQueues();
            }
            assertQueryResult(connection, "SELECT \"column\", type FROM table_columns('mv')", """
                    column[VARCHAR],type[VARCHAR]
                    ts,TIMESTAMP
                    sym,SYMBOL
                    price,DOUBLE
                    flag,DOUBLE
                    """);
            execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-05T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT view_status, invalidation_reason, suspended FROM materialized_views() JOIN wal_tables() ON view_name = name",
                    "view_status[VARCHAR],invalidation_reason[VARCHAR],suspended[BIT]\ninvalid,[-1]: inconvertible types: BOOLEAN -> DOUBLE [from=flag, to=flag],false\n");
            assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", """
                    ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE],flag[DOUBLE]
                    2024-01-01 00:00:00.0,a,1.0,null
                    2024-01-02 00:00:00.0,a,2.0,null
                    2024-01-03 00:00:00.0,a,3.0,null
                    2024-01-04 00:00:00.0,a,4.0,null
                    """);
        });
    }

    @Test
    public void testAlterAsReExecutedAfterViewColumnChangeRefused() throws Exception {
        // ALTER ... AS executes when pg-wire compiles it, so a prepared one compiles again at each execution: after
        // the view replaced the column at the SQL column's position, it fails as a fresh compile does, and writes
        // nothing.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createBaseAndView();
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN extra DOUBLE");
            drainWalAndMatViewQueues();
            final String newSql = "SELECT ts, sym, sum(price) AS price, max(price) AS extra FROM base SAMPLE BY 1h";
            final String alterSql = "ALTER MATERIALIZED VIEW mv AS (" + newSql + ")";
            try (PreparedStatement alter = connection.prepareStatement(alterSql)) {
                alter.execute();
                drainWalAndMatViewQueues();
                execute("ALTER MATERIALIZED VIEW mv DROP COLUMN extra");
                execute("ALTER MATERIALIZED VIEW mv ADD COLUMN note INT");
                drainWalAndMatViewQueues();
                final long viewSeqTxn = viewSeqTxn();
                final ServerErrorMessage expected = freshError(connection, alterSql);
                TestUtils.assertContains(expected.getMessage(), "materialized view column does not match the query column at the same position");
                try {
                    alter.execute();
                    Assert.fail("the prepared ALTER ... AS must fail as a fresh compile does");
                } catch (PSQLException e) {
                    Assert.assertNotNull(e.getServerErrorMessage());
                    TestUtils.assertEquals(expected.getMessage(), e.getServerErrorMessage().getMessage());
                    Assert.assertEquals(expected.getPosition(), e.getServerErrorMessage().getPosition());
                }
                drainWalAndMatViewQueues();
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            }
            assertQueryResult(connection, "SELECT view_status, view_sql, suspended FROM materialized_views() JOIN wal_tables() ON view_name = name",
                    "view_status[VARCHAR],view_sql[VARCHAR],suspended[BIT]\nvalid," + newSql + ",false\n");
            execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-05T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", """
                    ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE],note[INTEGER]
                    2024-01-01 00:00:00.0,a,1.0,null
                    2024-01-02 00:00:00.0,a,2.0,null
                    2024-01-03 00:00:00.0,a,3.0,null
                    2024-01-04 00:00:00.0,a,4.0,null
                    2024-01-05 00:00:00.0,a,5.0,null
                    """);
        });
    }

    @Test
    public void testAlterWithBaseReExecutedAfterNewBaseGainsExpiryPolicyRefused() throws Exception {
        // A materialized view cannot read a view that has an EXPIRE ROWS policy (#7263 rule 5). A prepared ALTER ...
        // WITH BASE onto a view compiles again at each execution, so once that view has a policy it fails as a
        // fresh compile does, and the view keeps its base.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createBaseAndView();
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base2 VALUES ('b', 7.0, '2024-01-06T00:10:00.000000Z')");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv2 AS (SELECT ts, sym, sum(price) AS price FROM base2 SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String viewSql = "SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1h";
            final String alterSql = "ALTER MATERIALIZED VIEW mv WITH BASE mv2 AS (SELECT ts, sym, sum(price) AS price FROM mv2 SAMPLE BY 1h)";
            try (PreparedStatement alter = connection.prepareStatement(alterSql)) {
                alter.execute();
                drainWalAndMatViewQueues();
                assertQueryResult(connection, "SELECT base_table_name FROM materialized_views() WHERE view_name = 'mv'",
                        "base_table_name[VARCHAR]\nmv2\n");
                // a base change leaves the view invalid until RESUME SKIP BACKLOG positions it on the new base
                execute("ALTER MATERIALIZED VIEW mv WITH BASE base AS (" + viewSql + ")");
                drainWalAndMatViewQueues();
                execute("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
                execute("ALTER MATERIALIZED VIEW mv2 SET EXPIRE ROWS WHEN price < 0.0");
                drainWalAndMatViewQueues();
                assertQueryResult(connection, "SELECT view_status, base_table_name FROM materialized_views() WHERE view_name = 'mv'",
                        "view_status[VARCHAR],base_table_name[VARCHAR]\nvalid,base\n");
                final long viewSeqTxn = viewSeqTxn();
                final ServerErrorMessage expected = freshError(connection, alterSql);
                TestUtils.assertContains(expected.getMessage(), "EXPIRE ROWS");
                try {
                    alter.execute();
                    Assert.fail("the prepared ALTER ... WITH BASE must fail as a fresh compile does");
                } catch (PSQLException e) {
                    Assert.assertNotNull(e.getServerErrorMessage());
                    TestUtils.assertEquals(expected.getMessage(), e.getServerErrorMessage().getMessage());
                }
                drainWalAndMatViewQueues();
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            }
            assertQueryResult(connection, "SELECT view_status, base_table_name, view_sql, suspended FROM materialized_views() JOIN wal_tables() ON view_name = name WHERE view_name = 'mv'",
                    "view_status[VARCHAR],base_table_name[VARCHAR],view_sql[VARCHAR],suspended[BIT]\nvalid,base," + viewSql + ",false\n");
            execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-05T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT * FROM mv WHERE ts >= '2024-01-05' ORDER BY ts", """
                    ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE]
                    2024-01-05 00:00:00.0,a,5.0
                    """);
        });
    }

    @Test
    public void testColumnDdlExecutedTwiceExtended() throws Exception {
        // A re-executed prepared ADD COLUMN reaches the WAL writer, which refuses the existing column; a re-executed
        // DROP COLUMN reaches it the same way. ALTER TABLE on a WAL table behaves the same.
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> assertColumnDdlExecutedTwice(
                connection,
                "duplicate column [name=extra]",
                "cannot remove, column does not exist [table=mv, column=extra]"
        ));
    }

    @Test
    public void testColumnDdlExecutedTwiceSimple() throws Exception {
        // The simple protocol compiles each execution again, which refuses the existing or missing column.
        assertWithPgServer(Mode.SIMPLE, false, -1, (connection, _, _, _) -> assertColumnDdlExecutedTwice(
                connection,
                "column 'extra' already exists",
                "Invalid column: extra"
        ));
    }

    @Test
    public void testDropPartitionListExecutedTwice() throws Exception {
        // The second execution names a partition the view no longer has; the view's WAL applies it as a no-op.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createBaseAndView();
            try (PreparedStatement drop = connection.prepareStatement("ALTER MATERIALIZED VIEW mv DROP PARTITION LIST '2024-01-01'")) {
                drop.execute();
                drainWalAndMatViewQueues();
                drop.execute();
                drainWalAndMatViewQueues();
            }
            assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", """
                    ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE]
                    2024-01-02 00:00:00.0,a,2.0
                    2024-01-03 00:00:00.0,a,3.0
                    2024-01-04 00:00:00.0,a,4.0
                    """);
            assertQueryResult(connection, "SELECT view_status, suspended FROM materialized_views() JOIN wal_tables() ON view_name = name",
                    "view_status[VARCHAR],suspended[BIT]\nvalid,false\n");
        });
    }

    @Test
    public void testDropPartitionWhereReBound() throws Exception {
        // The prepared DROP PARTITION WHERE drops by the value bound at each execution.
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, _, _, _) -> {
            createBaseAndView();
            try (PreparedStatement drop = connection.prepareStatement("ALTER MATERIALIZED VIEW mv DROP PARTITION WHERE ts < ?")) {
                drop.setTimestamp(1, Timestamp.valueOf("2024-01-03 00:00:00"));
                drop.execute();
                drainWalAndMatViewQueues();
                assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", """
                        ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE]
                        2024-01-03 00:00:00.0,a,3.0
                        2024-01-04 00:00:00.0,a,4.0
                        """);
                drop.setTimestamp(1, Timestamp.valueOf("2024-01-04 00:00:00"));
                drop.execute();
                drainWalAndMatViewQueues();
            }
            assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", """
                    ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE]
                    2024-01-04 00:00:00.0,a,4.0
                    """);
        });
    }

    @Test
    public void testPassthroughAlterAsTransformedTimestampRefusedAtEachExecution() throws Exception {
        // A non-aggregating view's designated timestamp must be the base table's designated timestamp as it is, so
        // ALTER ... AS refuses a shifted one. A prepared one compiles again at each execution, so it fails each time as
        // a fresh compile does, writes nothing, and the view keeps refreshing with its SQL.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T10:00:00.000000Z', 'a', 1.0), ('2024-01-01T11:00:00.000000Z', 'b', 2.0)");
            drainWalQueue();
            final String viewSql = "SELECT ts, sym, price FROM base";
            execute("CREATE MATERIALIZED VIEW mv AS (" + viewSql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final long viewSeqTxn = viewSeqTxn();
            final String alterSql = "ALTER MATERIALIZED VIEW mv AS (SELECT dateadd('m', 15, ts) AS ts, sym, price FROM base)";
            final ServerErrorMessage expected = freshError(connection, alterSql);
            TestUtils.assertEquals(
                    "passthrough materialized view timestamp must reference the base table designated timestamp without transformation",
                    expected.getMessage()
            );
            // the position the server reports counts from 1
            Assert.assertEquals(alterSql.indexOf("dateadd") + 1, expected.getPosition());
            try (PreparedStatement alter = connection.prepareStatement(alterSql)) {
                for (int i = 0; i < 2; i++) {
                    try {
                        alter.execute();
                        Assert.fail("the prepared ALTER ... AS must fail as a fresh compile does");
                    } catch (PSQLException e) {
                        Assert.assertNotNull(e.getServerErrorMessage());
                        TestUtils.assertEquals(expected.getMessage(), e.getServerErrorMessage().getMessage());
                        Assert.assertEquals(expected.getPosition(), e.getServerErrorMessage().getPosition());
                    }
                }
            }
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertQueryResult(connection, "SELECT view_status, view_sql, suspended FROM materialized_views() JOIN wal_tables() ON view_name = name",
                    "view_status[VARCHAR],view_sql[VARCHAR],suspended[BIT]\nvalid," + viewSql + ",false\n");
            execute("INSERT INTO base VALUES ('2024-01-01T12:00:00.000000Z', 'c', 3.0)");
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", """
                    ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE]
                    2024-01-01 10:00:00.0,a,1.0
                    2024-01-01 11:00:00.0,b,2.0
                    2024-01-01 12:00:00.0,c,3.0
                    """);
        });
    }

    @Test
    public void testPositionalAlterAsRefusedAtEachExecution() throws Exception {
        // An unaliased reference to a column name longer than 64 characters makes the SQL copy by position, so ALTER
        // ... AS refuses one that moves a view column. A prepared one compiles again at each execution, so it fails
        // each time as a fresh compile does, and writes nothing.
        final String longName = "long_col_" + "y".repeat(56);
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            execute("CREATE TABLE base (ts TIMESTAMP, " + longName + " SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:10:00.000000Z', 'a', 1.0), ('2024-01-01T00:20:00.000000Z', 'a', 9.0)");
            drainWalQueue();
            final String viewSql = "SELECT ts, " + longName + ", max(price) AS mx, min(price) AS mn FROM base SAMPLE BY 1h";
            execute("CREATE MATERIALIZED VIEW mv AS (" + viewSql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final long viewSeqTxn = viewSeqTxn();
            final String alterSql = "ALTER MATERIALIZED VIEW mv AS (SELECT ts, " + longName + ", min(price) AS mn, max(price) AS mx FROM base SAMPLE BY 1h)";
            final ServerErrorMessage expected = freshError(connection, alterSql);
            TestUtils.assertEquals(
                    "materialized view column does not match the query column at the same position [view column=mx, query column=mn]; "
                            + "the query relies on generated column names, so refresh copies its columns by position: "
                            + "keep each view column at its position, or give the query's columns explicit names",
                    expected.getMessage()
            );
            // the position the server reports counts from 1
            Assert.assertEquals(alterSql.indexOf("min(price)") + 1, expected.getPosition());
            try (PreparedStatement alter = connection.prepareStatement(alterSql)) {
                for (int i = 0; i < 2; i++) {
                    try {
                        alter.execute();
                        Assert.fail("the prepared ALTER ... AS must fail as a fresh compile does");
                    } catch (PSQLException e) {
                        Assert.assertNotNull(e.getServerErrorMessage());
                        TestUtils.assertEquals(expected.getMessage(), e.getServerErrorMessage().getMessage());
                        Assert.assertEquals(expected.getPosition(), e.getServerErrorMessage().getPosition());
                    }
                }
            }
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertQueryResult(connection, "SELECT view_status, view_sql, suspended FROM materialized_views() JOIN wal_tables() ON view_name = name",
                    "view_status[VARCHAR],view_sql[VARCHAR],suspended[BIT]\nvalid," + viewSql + ",false\n");
            execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 'a', 2.0), ('2024-01-01T01:20:00.000000Z', 'a', 6.0)");
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT ts, mx, mn FROM mv ORDER BY ts", """
                    ts[TIMESTAMP],mx[DOUBLE],mn[DOUBLE]
                    2024-01-01 00:00:00.0,9.0,1.0
                    2024-01-01 01:00:00.0,6.0,2.0
                    """);
        });
    }

    @Test
    public void testRangeRefreshRefusesBindVariables() throws Exception {
        // REFRESH ... RANGE takes timestamp literals only, as before this feature.
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, _, _, _) -> {
            createBaseAndView();
            try (PreparedStatement range = connection.prepareStatement("REFRESH MATERIALIZED VIEW mv RANGE FROM ? TO ?")) {
                range.setTimestamp(1, Timestamp.valueOf("2024-01-01 00:00:00"));
                range.setTimestamp(2, Timestamp.valueOf("2024-01-05 00:00:00"));
                range.execute();
                Assert.fail("RANGE must refuse bind variables");
            } catch (PSQLException e) {
                TestUtils.assertContains(e.getMessage(), "invalid FROM timestamp value");
            }
            assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", VIEW_ROWS);
        });
    }

    @Test
    public void testResumeCancelledWhileWaitingForViewWal() throws Exception {
        // A PostgreSQL CancelRequest stops RESUME while it waits for the view's own WAL to apply: RESUME writes nothing,
        // leaves the view invalid and its latch free, and the connection stays usable. Run again once the WAL has
        // applied, RESUME catches the view up.
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> {
            createBaseAndView();
            final TableToken viewToken = engine.verifyTableName("mv");
            engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-05T00:10:00.000000Z')");
            drainWalQueue();
            // a view WAL transaction that RESUME waits for
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN extra DOUBLE");
            final long viewSeqTxn = viewSeqTxn();
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            final CountDownLatch isWaiting = new CountDownLatch(1);
            final CountDownLatch isCancelSent = new CountDownLatch(1);
            final AtomicBoolean isFirstWait = new AtomicBoolean(true);
            final AtomicBoolean isCancelled = new AtomicBoolean();
            // the first wait round holds RESUME until the cancel request has gone out
            state.setOnResumeWalWaitForTesting(() -> {
                if (isFirstWait.compareAndSet(true, false)) {
                    isWaiting.countDown();
                    try {
                        Assert.assertTrue(isCancelSent.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            final Thread canceller = new Thread(() -> {
                try {
                    if (isWaiting.await(30, TimeUnit.SECONDS)) {
                        ((PGConnection) connection).cancelQuery();
                        isCancelled.set(true);
                    }
                } catch (Throwable th) {
                    LOG.error().$("cancellation failed [error=").$(th).I$();
                } finally {
                    isCancelSent.countDown();
                }
            }, "pg-canceller");
            canceller.start();
            try (Statement statement = connection.createStatement()) {
                statement.execute("REFRESH MATERIALIZED VIEW mv RESUME");
                Assert.fail("RESUME must stop when cancelled");
            } catch (PSQLException e) {
                TestUtils.assertContains(e.getMessage(), "cancelled by user");
            } finally {
                canceller.join();
                state.setOnResumeWalWaitForTesting(null);
            }
            Assert.assertTrue(isCancelled.get());
            Assert.assertFalse(state.isLocked());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT view_status, invalidation_reason FROM materialized_views()",
                    "view_status[VARCHAR],invalidation_reason[VARCHAR]\ninvalid,test invalidation\n");

            try (Statement statement = connection.createStatement()) {
                statement.execute("REFRESH MATERIALIZED VIEW mv RESUME");
            }
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT view_status, invalidation_reason FROM materialized_views()",
                    "view_status[VARCHAR],invalidation_reason[VARCHAR]\nvalid,null\n");
            assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", """
                    ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE],extra[DOUBLE]
                    2024-01-01 00:00:00.0,a,1.0,null
                    2024-01-02 00:00:00.0,a,2.0,null
                    2024-01-03 00:00:00.0,a,3.0,null
                    2024-01-04 00:00:00.0,a,4.0,null
                    2024-01-05 00:00:00.0,a,5.0,null
                    """);
        });
    }

    @Test
    public void testResumeExecutedTwice() throws Exception {
        // RESUME on a valid view does nothing; RESUME SKIP BACKLOG on an invalid view makes it valid, and the second
        // execution finds it valid.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createBaseAndView();
            final TableToken viewToken = engine.verifyTableName("mv");
            final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
            try (PreparedStatement resume = connection.prepareStatement("REFRESH MATERIALIZED VIEW mv RESUME")) {
                resume.execute();
                resume.execute();
            }
            Assert.assertEquals(viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());

            engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT view_status, invalidation_reason FROM materialized_views()",
                    "view_status[VARCHAR],invalidation_reason[VARCHAR]\ninvalid,test invalidation\n");
            try (PreparedStatement resume = connection.prepareStatement("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG")) {
                resume.execute();
                drainWalAndMatViewQueues();
                resume.execute();
                drainWalAndMatViewQueues();
            }
            assertQueryResult(connection, "SELECT view_status, invalidation_reason FROM materialized_views()",
                    "view_status[VARCHAR],invalidation_reason[VARCHAR]\nvalid,null\n");
            execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-05T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", VIEW_ROWS + "2024-01-05 00:00:00.0,a,5.0\n");
        });
    }

    private static void assertColumnDdlExecutedTwice(Connection connection, String addError, String dropError) throws Exception {
        createBaseAndView();
        try (PreparedStatement add = connection.prepareStatement("ALTER MATERIALIZED VIEW mv ADD COLUMN extra INT")) {
            add.execute();
            drainWalAndMatViewQueues();
            try {
                add.execute();
                Assert.fail("the second ADD COLUMN must fail");
            } catch (PSQLException e) {
                TestUtils.assertContains(e.getMessage(), addError);
            }
            drainWalAndMatViewQueues();
        }
        assertQueryResult(connection, "SELECT \"column\", type FROM table_columns('mv')", """
                column[VARCHAR],type[VARCHAR]
                ts,TIMESTAMP
                sym,SYMBOL
                price,DOUBLE
                extra,INT
                """);
        try (PreparedStatement drop = connection.prepareStatement("ALTER MATERIALIZED VIEW mv DROP COLUMN extra")) {
            drop.execute();
            drainWalAndMatViewQueues();
            try {
                drop.execute();
                Assert.fail("the second DROP COLUMN must fail");
            } catch (PSQLException e) {
                TestUtils.assertContains(e.getMessage(), dropError);
            }
            drainWalAndMatViewQueues();
        }
        assertQueryResult(connection, "SELECT \"column\", type FROM table_columns('mv')", """
                column[VARCHAR],type[VARCHAR]
                ts,TIMESTAMP
                sym,SYMBOL
                price,DOUBLE
                """);
        // the view's WAL is not suspended, and the view still refreshes
        execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-05T00:10:00.000000Z')");
        drainWalAndMatViewQueues();
        assertQueryResult(connection, "SELECT view_status, suspended FROM materialized_views() JOIN wal_tables() ON view_name = name",
                "view_status[VARCHAR],suspended[BIT]\nvalid,false\n");
        assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", VIEW_ROWS + "2024-01-05 00:00:00.0,a,5.0\n");
    }

    // The error a fresh compile of the statement returns over the connection.
    private static ServerErrorMessage freshError(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            Assert.fail("a fresh compile must refuse: " + sql);
            return null;
        } catch (PSQLException e) {
            Assert.assertNotNull(e.getServerErrorMessage());
            return e.getServerErrorMessage();
        }
    }

    private static long viewSeqTxn() {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("mv"));
    }

    private static void assertQueryResult(Connection connection, String sql, String expected) throws Exception {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            sink.clear();
            assertResultSet(expected, sink, rs);
        }
    }

    // One base row a day for four days; the view keeps hourly sums.
    private static void createBaseAndView() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('a', 2.0, '2024-01-02T00:10:00.000000Z'),
                ('a', 3.0, '2024-01-03T00:10:00.000000Z'),
                ('a', 4.0, '2024-01-04T00:10:00.000000Z')
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
        drainWalAndMatViewQueues();
    }
}
