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

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A materialized view with REFRESH LIMIT takes backfill over pg-wire. The backfill gate checks the view when a
 * statement compiles, and the commit-time check covers what a statement compiled before ALTER MATERIALIZED VIEW ...
 * SET REFRESH LIMIT 0 writes after it: a statement pg-wire keeps prepared or serves from its insert cache, and rows an
 * open transaction holds. Either refusal carries the gate's message; the commit-time one has no position in the SQL.
 * The commit-time check also refuses rows in the view's managed zone, and it runs before any table of the transaction
 * commits.
 * SQL COPY takes no backfill: its import writes outside the view's WAL, where the commit-time check runs.
 */
public class PGMatViewBackfillTest extends BasePGTest {
    private static final String GATE_MESSAGE = "cannot modify materialized view [view=price_1h]";
    private static final String INSERT_SQL = "INSERT INTO price_1h VALUES (?, ?, ?)";
    // base max 12:00, clock 12:30 and REFRESH LIMIT 1 HOUR put the view's boundary bucket floor at 11:00
    private static final String MANAGED_ZONE_MESSAGE = "backfill row falls in or past the managed zone of materialized view [view=price_1h, "
            + "maxRowTs=2024-09-10T12:00:00.000000Z, rowBucket=[2024-09-10T12:00:00.000000Z, 2024-09-10T13:00:00.000000Z), "
            + "boundaryBucketFloor=2024-09-10T11:00:00.000000Z]; backfill bucket end must be at-or-before the boundary bucket floor";
    private static final String PLAIN_INSERT_SQL = "INSERT INTO %s VALUES (%d, '2024-09-10T00:00:00.000000Z')";
    private static final String REFRESHED_VIEW_ROWS = """
            sym[VARCHAR],price[DOUBLE],ts[TIMESTAMP]
            a,8.0,2024-09-10 10:00:00.0
            a,9.0,2024-09-10 12:00:00.0
            """;

    @BeforeClass
    public static void setUpCopyRoot() {
        // COPY FROM is disabled while cairo.sql.copy.root is blank
        inputRoot = TestUtils.getCsvRoot();
    }

    @Test
    public void testCommitRejectedAfterRefreshLimitRemoved() throws Exception {
        // An explicit transaction holds its rows until COMMIT, which checks them under the view definition applied
        // then: frozen-zone rows inserted while the view had REFRESH LIMIT are refused once the limit is gone, and the
        // connection stays usable.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createRefreshedView();
            connection.setAutoCommit(false);
            try (PreparedStatement insert = connection.prepareStatement(INSERT_SQL)) {
                Assert.assertEquals(1, executeInsert(insert, 1.0, "2024-09-10T09:00:00.000000Z"));
                Assert.assertEquals(1, executeInsert(insert, 2.0, "2024-09-10T08:00:00.000000Z"));
            }
            removeRefreshLimit();
            try {
                connection.commit();
                Assert.fail("COMMIT must fail once the view has no REFRESH LIMIT");
            } catch (PSQLException e) {
                assertGateMessage(e);
            }
            connection.rollback();
            connection.setAutoCommit(true);
            drainWalAndMatViewQueues();
            assertViewRows(connection, REFRESHED_VIEW_ROWS);
        });
    }

    @Test
    public void testCommitWithManagedZoneRowCommitsNoTable() throws Exception {
        // COMMIT refuses a transaction whose view rows reach the managed zone before any of its tables commits: the
        // WAL table that commits before the view (lower table id) and the non-WAL table that commits after it keep no
        // row, nor does the view keep the transaction's frozen-zone row. A repeated refusal behaves the same, and the
        // connection then commits a valid transaction into all three tables.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            execute("CREATE TABLE t_before (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createRefreshedView();
            execute("CREATE TABLE t_after (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            connection.setAutoCommit(false);
            for (int i = 0; i < 2; i++) {
                try (Statement statement = connection.createStatement(); PreparedStatement insert = connection.prepareStatement(INSERT_SQL)) {
                    Assert.assertEquals(1, statement.executeUpdate(PLAIN_INSERT_SQL.formatted("t_before", 1)));
                    Assert.assertEquals(1, executeInsert(insert, 1.0, "2024-09-10T09:00:00.000000Z"));
                    Assert.assertEquals(1, executeInsert(insert, 77.0, "2024-09-10T12:00:00.000000Z"));
                    Assert.assertEquals(1, statement.executeUpdate(PLAIN_INSERT_SQL.formatted("t_after", 1)));
                }
                try {
                    connection.commit();
                    Assert.fail("COMMIT must fail on a managed-zone view row");
                } catch (PSQLException e) {
                    assertManagedZoneMessage(e);
                }
                connection.rollback();
                drainWalAndMatViewQueues();
                assertPlainRows(connection, "t_before", "");
                assertPlainRows(connection, "t_after", "");
                assertViewRows(connection, REFRESHED_VIEW_ROWS);
            }

            try (Statement statement = connection.createStatement(); PreparedStatement insert = connection.prepareStatement(INSERT_SQL)) {
                Assert.assertEquals(1, statement.executeUpdate(PLAIN_INSERT_SQL.formatted("t_before", 2)));
                Assert.assertEquals(1, executeInsert(insert, 1.0, "2024-09-10T09:00:00.000000Z"));
                Assert.assertEquals(1, statement.executeUpdate(PLAIN_INSERT_SQL.formatted("t_after", 3)));
            }
            connection.commit();
            connection.setAutoCommit(true);
            drainWalAndMatViewQueues();
            assertPlainRows(connection, "t_before", "2,2024-09-10 00:00:00.0\n");
            assertPlainRows(connection, "t_after", "3,2024-09-10 00:00:00.0\n");
            assertViewRows(connection, """
                    sym[VARCHAR],price[DOUBLE],ts[TIMESTAMP]
                    a,1.0,2024-09-10 09:00:00.0
                    a,8.0,2024-09-10 10:00:00.0
                    a,9.0,2024-09-10 12:00:00.0
                    """);
        });
    }

    @Test
    public void testImplicitCommitWithManagedZoneRowCommitsNoTable() throws Exception {
        // Without BEGIN, the statements of one multi-statement query commit together at the end of the query, and a
        // managed-zone view row among them fails that commit before any table commits.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            execute("CREATE TABLE t_before (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createRefreshedView();
            execute("CREATE TABLE t_after (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (Statement statement = connection.createStatement()) {
                try {
                    statement.execute(PLAIN_INSERT_SQL.formatted("t_before", 1)
                            + "; INSERT INTO price_1h VALUES ('a', 77.0, '2024-09-10T12:00:00.000000Z'); "
                            + PLAIN_INSERT_SQL.formatted("t_after", 1));
                    Assert.fail("the query must fail on a managed-zone view row");
                } catch (PSQLException e) {
                    assertManagedZoneMessage(e);
                }
                drainWalAndMatViewQueues();
                assertPlainRows(connection, "t_before", "");
                assertPlainRows(connection, "t_after", "");
                assertViewRows(connection, REFRESHED_VIEW_ROWS);

                statement.execute(PLAIN_INSERT_SQL.formatted("t_before", 2)
                        + "; INSERT INTO price_1h VALUES ('a', 1.0, '2024-09-10T09:00:00.000000Z'); "
                        + PLAIN_INSERT_SQL.formatted("t_after", 3));
            }
            drainWalAndMatViewQueues();
            assertPlainRows(connection, "t_before", "2,2024-09-10 00:00:00.0\n");
            assertPlainRows(connection, "t_after", "3,2024-09-10 00:00:00.0\n");
            assertViewRows(connection, """
                    sym[VARCHAR],price[DOUBLE],ts[TIMESTAMP]
                    a,1.0,2024-09-10 09:00:00.0
                    a,8.0,2024-09-10 10:00:00.0
                    a,9.0,2024-09-10 12:00:00.0
                    """);
        });
    }

    @Test
    public void testPreparedCopyRejected() throws Exception {
        // A prepared COPY into a view with REFRESH LIMIT fails at each execution with the gate's message at the
        // view's name, and the connection still backfills the frozen zone with INSERT.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createRefreshedView();
            final String copySql = "COPY price_1h FROM 'test-numeric-headers.csv' WITH HEADER true";
            try (PreparedStatement copy = connection.prepareStatement(copySql)) {
                for (int i = 0; i < 2; i++) {
                    try {
                        copy.execute();
                        Assert.fail("COPY into a materialized view must be refused");
                    } catch (PSQLException e) {
                        assertGateMessage(e);
                        // the position the server reports counts from 1
                        Assert.assertEquals(copySql.indexOf("price_1h") + 1, e.getServerErrorMessage().getPosition());
                    }
                }
            }
            assertViewRows(connection, REFRESHED_VIEW_ROWS);

            try (PreparedStatement insert = connection.prepareStatement(INSERT_SQL)) {
                Assert.assertEquals(1, executeInsert(insert, 1.0, "2024-09-10T09:00:00.000000Z"));
            }
            drainWalAndMatViewQueues();
            assertViewRows(connection, """
                    sym[VARCHAR],price[DOUBLE],ts[TIMESTAMP]
                    a,1.0,2024-09-10 09:00:00.0
                    a,8.0,2024-09-10 10:00:00.0
                    a,9.0,2024-09-10 12:00:00.0
                    """);
        });
    }

    @Test
    public void testPreparedInsertRejectedAfterRefreshLimitRemoved() throws Exception {
        // A statement prepared while the view had REFRESH LIMIT, and a new statement with the same text that the
        // insert cache serves, are refused at commit once SET REFRESH LIMIT 0 has applied, and a batch of them lands
        // no row. Over the simple protocol each execution compiles afresh and meets the gate itself.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createRefreshedView();
            final String viewRows = """
                    sym[VARCHAR],price[DOUBLE],ts[TIMESTAMP]
                    a,1.0,2024-09-10 09:00:00.0
                    a,8.0,2024-09-10 10:00:00.0
                    a,9.0,2024-09-10 12:00:00.0
                    """;
            try (PreparedStatement insert = connection.prepareStatement(INSERT_SQL)) {
                Assert.assertEquals(1, executeInsert(insert, 1.0, "2024-09-10T09:00:00.000000Z"));
                drainWalAndMatViewQueues();
                assertViewRows(connection, viewRows);

                removeRefreshLimit();
                assertInsertRejected(insert, 77.0, "2024-09-10T12:00:00.000000Z");
                drainWalAndMatViewQueues();
                assertViewRows(connection, viewRows);
            }
            try (PreparedStatement insert = connection.prepareStatement(INSERT_SQL)) {
                assertInsertRejected(insert, 55.0, "2024-09-10T11:00:00.000000Z");
                drainWalAndMatViewQueues();
                assertViewRows(connection, viewRows);

                addBatch(insert, 3.0, "2024-09-10T08:00:00.000000Z");
                addBatch(insert, 4.0, "2024-09-10T07:00:00.000000Z");
                addBatch(insert, 5.0, "2024-09-10T06:00:00.000000Z");
                try {
                    insert.executeBatch();
                    Assert.fail("the batch must fail once the view has no REFRESH LIMIT");
                } catch (BatchUpdateException e) {
                    Assert.assertTrue(e.getNextException() instanceof PSQLException);
                    assertGateMessage((PSQLException) e.getNextException());
                }
                drainWalAndMatViewQueues();
                assertViewRows(connection, viewRows);
            }
        });
    }

    private static void addBatch(PreparedStatement insert, double price, String ts) throws SQLException {
        bindRow(insert, price, ts);
        insert.addBatch();
    }

    private static void assertGateMessage(PSQLException e) {
        final ServerErrorMessage serverError = e.getServerErrorMessage();
        Assert.assertNotNull(serverError);
        TestUtils.assertEquals(GATE_MESSAGE, serverError.getMessage());
    }

    private static void assertInsertRejected(PreparedStatement insert, double price, String ts) throws SQLException {
        try {
            executeInsert(insert, price, ts);
            Assert.fail("the INSERT must fail once the view has no REFRESH LIMIT [ts=" + ts + ']');
        } catch (PSQLException e) {
            assertGateMessage(e);
        }
    }

    private static void assertManagedZoneMessage(PSQLException e) {
        final ServerErrorMessage serverError = e.getServerErrorMessage();
        Assert.assertNotNull(serverError);
        TestUtils.assertEquals(MANAGED_ZONE_MESSAGE, serverError.getMessage());
    }

    private static void assertPlainRows(Connection connection, String table, String expectedRows) throws Exception {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT * FROM " + table)) {
            sink.clear();
            assertResultSet("x[BIGINT],ts[TIMESTAMP]\n" + expectedRows, sink, rs);
        }
    }

    private static void assertViewRows(Connection connection, String expected) throws Exception {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT * FROM price_1h ORDER BY ts")) {
            sink.clear();
            assertResultSet(expected, sink, rs);
        }
    }

    private static void bindRow(PreparedStatement insert, double price, String ts) throws SQLException {
        insert.setString(1, "a");
        insert.setDouble(2, price);
        insert.setString(3, ts);
    }

    // The base holds rows at 10:10 and 12:00 on 2024-09-10, the FULL refresh builds the view's 10:00 and 12:00
    // buckets, and the clock reads 12:30: REFRESH LIMIT 1 HOUR freezes the buckets that end by 11:00.
    private static void createRefreshedView() throws Exception {
        execute("CREATE TABLE base_price (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base_price VALUES ('a', 8.0, '2024-09-10T10:10'), ('a', 9.0, '2024-09-10T12:00')");
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS (SELECT sym, last(price) AS price, ts FROM base_price SAMPLE BY 1h) PARTITION BY DAY");
        drainWalAndMatViewQueues();
        execute("REFRESH MATERIALIZED VIEW price_1h FULL");
        drainWalAndMatViewQueues();
        execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
        drainWalAndMatViewQueues();
        currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30");
    }

    private static int executeInsert(PreparedStatement insert, double price, String ts) throws SQLException {
        bindRow(insert, price, ts);
        return insert.executeUpdate();
    }

    private static void removeRefreshLimit() throws Exception {
        execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 0 DAYS");
        drainWalAndMatViewQueues();
    }
}
