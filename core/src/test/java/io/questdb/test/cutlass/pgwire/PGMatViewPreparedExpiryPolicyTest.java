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

import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * The EXPIRE ROWS checks of view column DDL hold for a statement that pg-wire keeps prepared and executes again
 * after the view changed, as they hold for a statement compiled afresh (design section 11, rule 8):
 * <ul>
 *     <li>view DROP COLUMN refuses, at the column name, a column that the view's applied EXPIRE ROWS policy reads,
 *     whatever protocol runs the statement;</li>
 *     <li>a SET EXPIRE whose scalar WHEN or KEEP policy reads a column the view no longer has never suspends the
 *     view's WAL: it is refused when it executes, or its apply is skipped, and the view stays valid, readable and
 *     current.</li>
 * </ul>
 * pgjdbc keeps a statement prepared on the server from its first execution with prepareThreshold=-1, and from its
 * fifth with the default threshold of 5; each later execution reuses the statement the server compiled.
 * <p>
 * The expected refusal is the one a fresh compile of the same statement over the same connection returns.
 */
public class PGMatViewPreparedExpiryPolicyTest extends BasePGTest {
    // pgjdbc's default prepareThreshold
    private static final int DEFAULT_PREPARE_THRESHOLD = 5;
    private static final String VIEW_ROWS = """
            ts[TIMESTAMP],sym[VARCHAR],price[DOUBLE]
            2024-01-01 00:00:00.0,a,1.0
            2024-01-02 00:00:00.0,a,2.0
            2024-01-03 00:00:00.0,a,3.0
            2024-01-04 00:00:00.0,a,4.0
            """;

    @Test
    public void testPreparedDropColumnReadByKeepHighestRefusedDefaultThreshold() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -2, (connection, _, _, _) -> assertPreparedDropColumnRefused(
                connection,
                "KEEP HIGHEST ON extra",
                DEFAULT_PREPARE_THRESHOLD,
                4
        ));
    }

    @Test
    public void testPreparedDropColumnReadByKeepHighestRefusedServerPrepared() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> assertPreparedDropColumnRefused(
                connection,
                "KEEP HIGHEST ON extra",
                1,
                4
        ));
    }

    @Test
    public void testPreparedDropColumnReadByKeepLatestRefusedDefaultThreshold() throws Exception {
        // every row has a NULL extra, so KEEP LATEST keeps the newest row
        assertWithPgServer(Mode.EXTENDED, false, -2, (connection, _, _, _) -> assertPreparedDropColumnRefused(
                connection,
                "KEEP LATEST PARTITION BY extra",
                DEFAULT_PREPARE_THRESHOLD,
                1
        ));
    }

    @Test
    public void testPreparedDropColumnReadByKeepLatestRefusedServerPrepared() throws Exception {
        assertWithPgServer(Mode.EXTENDED, false, -1, (connection, _, _, _) -> assertPreparedDropColumnRefused(
                connection,
                "KEEP LATEST PARTITION BY extra",
                1,
                1
        ));
    }

    @Test
    public void testPreparedDropColumnReadByScalarPredicateRefusedDefaultThreshold() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -2, (connection, _, _, _) -> assertPreparedDropColumnRefused(
                connection,
                "WHEN extra > 100.0",
                DEFAULT_PREPARE_THRESHOLD,
                4
        ));
    }

    @Test
    public void testPreparedDropColumnReadByScalarPredicateRefusedServerPrepared() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> assertPreparedDropColumnRefused(
                connection,
                "WHEN extra > 100.0",
                1,
                4
        ));
    }

    @Test
    public void testPreparedSetExpireKeepHighestAfterColumnDropDefaultThreshold() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -2, (connection, _, _, _) -> assertPreparedSetExpireAfterColumnDrop(
                connection,
                "KEEP HIGHEST ON extra",
                DEFAULT_PREPARE_THRESHOLD
        ));
    }

    @Test
    public void testPreparedSetExpireKeepHighestAfterColumnDropServerPrepared() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> assertPreparedSetExpireAfterColumnDrop(
                connection,
                "KEEP HIGHEST ON extra",
                1
        ));
    }

    @Test
    public void testPreparedSetExpireKeepLatestAfterColumnDropDefaultThreshold() throws Exception {
        assertWithPgServer(Mode.EXTENDED, false, -2, (connection, _, _, _) -> assertPreparedSetExpireAfterColumnDrop(
                connection,
                "KEEP LATEST PARTITION BY extra",
                DEFAULT_PREPARE_THRESHOLD
        ));
    }

    @Test
    public void testPreparedSetExpireKeepLatestAfterColumnDropServerPrepared() throws Exception {
        assertWithPgServer(Mode.EXTENDED, false, -1, (connection, _, _, _) -> assertPreparedSetExpireAfterColumnDrop(
                connection,
                "KEEP LATEST PARTITION BY extra",
                1
        ));
    }

    @Test
    public void testPreparedSetExpireScalarAfterColumnDropDefaultThreshold() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -2, (connection, _, _, _) -> assertPreparedSetExpireAfterColumnDrop(
                connection,
                "WHEN extra < 2.0",
                DEFAULT_PREPARE_THRESHOLD
        ));
    }

    @Test
    public void testPreparedSetExpireScalarAfterColumnDropServerPrepared() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> assertPreparedSetExpireAfterColumnDrop(
                connection,
                "WHEN extra < 2.0",
                1
        ));
    }

    // A prepared view DROP COLUMN, executed while no policy reads the column, executes again once the view has
    // re-added the column and a policy that reads it. It fails as a fresh compile of the statement does, and the view
    // stays readable. Once DROP EXPIRE has applied, the prepared statement drops the column.
    private static void assertPreparedDropColumnRefused(
            Connection connection,
            String policy,
            int executionsBeforePolicy,
            int visibleRowCount
    ) throws Exception {
        createBaseAndView();
        final String dropSql = "ALTER MATERIALIZED VIEW mv DROP COLUMN extra";
        try (PreparedStatement drop = connection.prepareStatement(dropSql)) {
            for (int i = 0; i < executionsBeforePolicy; i++) {
                execute("ALTER MATERIALIZED VIEW mv ADD COLUMN extra DOUBLE");
                drainWalAndMatViewQueues();
                drop.execute();
                drainWalAndMatViewQueues();
                assertColumns(connection, "");
            }
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN extra DOUBLE");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS " + policy);
            drainWalAndMatViewQueues();
            assertState(connection, policy);
            final long viewSeqTxn = viewSeqTxn();

            final ServerErrorMessage expected = freshError(connection, dropSql);
            TestUtils.assertEquals(
                    "the EXPIRE ROWS policy of the materialized view reads the column [column=extra], change the policy "
                            + "with SET EXPIRE or remove it with DROP EXPIRE first, and retry once that has applied",
                    expected.getMessage()
            );
            try {
                drop.execute();
                Assert.fail("the prepared DROP COLUMN must fail while the view's EXPIRE ROWS policy reads the column");
            } catch (PSQLException e) {
                final ServerErrorMessage actual = e.getServerErrorMessage();
                Assert.assertNotNull(actual);
                TestUtils.assertEquals(expected.getMessage(), actual.getMessage());
                Assert.assertEquals(expected.getPosition(), actual.getPosition());
            }
            drainWalAndMatViewQueues();
            // the refused statement wrote nothing to the view's WAL
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertColumns(connection, "extra,DOUBLE\n");
            assertState(connection, policy);
            assertWalApplied(connection);
            assertQueryResult(connection, "SELECT count() FROM mv", "count[BIGINT]\n" + visibleRowCount + '\n');

            // without the policy, the prepared statement drops the column
            execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
            drainWalAndMatViewQueues();
            drop.execute();
            drainWalAndMatViewQueues();
        }
        assertColumns(connection, "");
        assertState(connection, null);
        assertWalApplied(connection);
        execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-05T00:10:00.000000Z')");
        drainWalAndMatViewQueues();
        assertWalApplied(connection);
        assertState(connection, null);
        assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", VIEW_ROWS + "2024-01-05 00:00:00.0,a,5.0\n");
    }

    // A prepared SET EXPIRE, executed while the view had the column its policy reads, executes again after DROP
    // EXPIRE and a view DROP COLUMN of that column. It is refused as a fresh compile of the statement is, or its apply
    // is skipped; either way the view's WAL keeps applying, and the view stays valid, without a policy, and current.
    private static void assertPreparedSetExpireAfterColumnDrop(
            Connection connection,
            String policy,
            int executionsBeforeDrop
    ) throws Exception {
        createBaseAndView();
        execute("ALTER MATERIALIZED VIEW mv ADD COLUMN extra DOUBLE");
        drainWalAndMatViewQueues();
        final String setSql = "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS " + policy;
        try (PreparedStatement set = connection.prepareStatement(setSql)) {
            for (int i = 0; i < executionsBeforeDrop; i++) {
                set.execute();
                drainWalAndMatViewQueues();
                assertState(connection, policy);
            }
            execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN extra");
            drainWalAndMatViewQueues();
            assertColumns(connection, "");

            final ServerErrorMessage expected = freshError(connection, setSql);
            TestUtils.assertContains(expected.getMessage(), "extra");
            try {
                set.execute();
            } catch (PSQLException e) {
                final ServerErrorMessage actual = e.getServerErrorMessage();
                Assert.assertNotNull(actual);
                TestUtils.assertEquals(expected.getMessage(), actual.getMessage());
            }
            drainWalAndMatViewQueues();
        }
        assertWalApplied(connection);
        assertState(connection, null);
        assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", VIEW_ROWS);

        // refresh commits keep applying
        execute("INSERT INTO base VALUES ('a', 5.0, '2024-01-05T00:10:00.000000Z')");
        drainWalAndMatViewQueues();
        assertWalApplied(connection);
        assertState(connection, null);
        assertQueryResult(connection, "SELECT * FROM mv ORDER BY ts", VIEW_ROWS + "2024-01-05 00:00:00.0,a,5.0\n");
    }

    // The view's columns after ts, sym and price, one "name,TYPE" line each.
    private static void assertColumns(Connection connection, String extraColumns) throws Exception {
        assertQueryResult(connection, "SELECT \"column\", type FROM table_columns('mv')", """
                column[VARCHAR],type[VARCHAR]
                ts,TIMESTAMP
                sym,SYMBOL
                price,DOUBLE
                """ + extraColumns);
    }

    private static void assertQueryResult(Connection connection, String sql, String expected) throws Exception {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            sink.clear();
            assertResultSet(expected, sink, rs);
        }
    }

    // The view is valid and has the policy, or none. materialized_views() prints a WHEN policy without WHEN.
    private static void assertState(Connection connection, String policy) throws Exception {
        final String expireClause = policy != null && policy.startsWith("WHEN ") ? policy.substring("WHEN ".length()) : policy;
        assertQueryResult(
                connection,
                "SELECT view_status, invalidation_reason, expire_clause FROM materialized_views()",
                "view_status[VARCHAR],invalidation_reason[VARCHAR],expire_clause[VARCHAR]\nvalid,null," + expireClause + '\n'
        );
    }

    // The view's WAL is not suspended and has applied every transaction.
    private static void assertWalApplied(Connection connection) throws Exception {
        assertQueryResult(
                connection,
                "SELECT suspended, writerTxn = sequencerTxn applied FROM wal_tables() WHERE name = 'mv'",
                "suspended[BIT],applied[BIT]\nfalse,true\n"
        );
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
}
