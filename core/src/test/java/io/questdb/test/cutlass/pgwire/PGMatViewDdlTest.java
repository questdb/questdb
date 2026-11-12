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
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

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
