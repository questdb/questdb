/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.griffin.SqlException;
import org.junit.*;
import org.postgresql.util.PSQLException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;

import static org.junit.Assert.*;

/*
    Most of the test are taken from other tests specifically PGMultiStatementMessageTest.java
    This was done to ensure the queries were correct and thus the pq metrics' tests would be accurate
    Each Query Count for start and completed is 2 higher than the ones seen in the actual test because
    Before any queries are run these two queries below are run:
    SET extra_float_digits = 3
    SET application_name = 'PostgreSQL JDBC Driver'
*/
public class PGWireMetricsQueryCounterTest extends BasePGTest {

    // The query counters in the PGWire Metrics continue to count for each test
    // so these long are trackers used to subtract the amount of queries done before the test
    // with the total of queries to give the accurate number for the test
    private long totalQueriesCompleted = 0l;
    private long totalQueriesStarted = 0l;

    @Before
    public void init() throws Exception {
        totalQueriesStarted = metrics.pgWire().startedQueriesCounter().getValue();
        totalQueriesCompleted = metrics.pgWire().completedQueriesCounter().getValue();
    }

    @Test
    public void testQueryCountAlterTableAddColumnSelectFromTableInBlock() throws Exception {
        // former test name testCreateInsertAlterTableAddColumnSelectFromTableInBlock
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE TEST(l long); INSERT INTO TEST VALUES(1); ALTER TABLE TEST ADD COLUMN s STRING; SELECT * from TEST;");
                assertResults(statement, hasResult, PGMultiStatementMessageTest.Result.ZERO, count(1), PGMultiStatementMessageTest.Result.ZERO, data(row(1L, null)));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(6, startedQueriesForTest);
                Assert.assertEquals(6, completedQueriesForTest);
            }
        });
    }

    @Test //explicit transaction + rollback on two tables
    public void testQueryCountCommitRollback() throws Exception {
        // former test name testBeginCreateInsertCommitRollbackRetainsCommittedDataOnTwoTables
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                test.connection.setAutoCommit(false);
                Statement statement = test.statement;

                boolean hasResult =
                        statement.execute("BEGIN; " +
                                "CREATE TABLE testA(l long,s string); " +
                                "CREATE TABLE testB(b byte, d double); " +
                                "INSERT INTO testA VALUES (50, 'z'); " +
                                "INSERT INTO testB VALUES (8, 1.0);" +
                                "COMMIT TRANSACTION; " +
                                "ROLLBACK TRANSACTION; /* rolls back implicit txn */" +
                                "INSERT INTO testA VALUES (29, 'g'); " +
                                "INSERT INTO testB VALUES (2, 1.0);" +
                                "SELECT * from testA;" +
                                "SELECT * from testB;");

                assertResults(statement, hasResult, zero(), zero(), zero(),
                        count(1), count(1), zero(), zero(),
                        count(1), count(1),
                        data(row(50L, "z"), row(29L, "g")), data(row((byte) 8, 1.0d), row((byte) 2, 1.0d)));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(14, startedQueriesForTest);
                Assert.assertEquals(14, completedQueriesForTest);
            }
        });
    }

    @Test
    public void testQueryCountCreateInsertAlterTableAddIndexSelectFromTable() throws Exception {
        // former test name testCreateInsertAlterTableAddIndexSelectFromTableInBlock
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long, s symbol); " +
                                "INSERT INTO test VALUES(4,'d'); " +
                                "ALTER TABLE test ALTER COLUMN s ADD INDEX; " +
                                "SELECT l,s from test;");
                assertResults(statement, hasResult, PGMultiStatementMessageTest.Result.ZERO, count(1), PGMultiStatementMessageTest.Result.ZERO, data(row(4L, "d")));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(6, startedQueriesForTest);
                Assert.assertEquals(6, completedQueriesForTest);
            }
        });
    }

    @Test //explicit transaction + commit
    public void testQueryCountCreateInsertCommitWithErrorForMoreStartedThanCompleted() throws Exception {
        // former test name testBeginCreateInsertCommitThenErrorDoesntRollBackCommittedFirstInsert
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                test.connection.setAutoCommit(false);
                Statement statement = test.statement;

                try {
                    statement.execute(
                            "BEGIN; " +
                                    "CREATE TABLE test(l long,s string); " +
                                    "INSERT INTO test VALUES (20, 'z'); " +
                                    "COMMIT; " +
                                    "DELETE FROM test; " +
                                    "INSERT INTO test VALUES (21, 'x');");
                    fail("PSQLException should be thrown");
                } catch (PSQLException e) {
                    assertEquals("ERROR: unexpected token: FROM\n  Position: 94", e.getMessage());
                }

                boolean hasResult = statement.execute("select * from test; ");
                assertResults(statement, hasResult, data(row(20L, "z")));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(10, startedQueriesForTest);
                Assert.assertEquals(9, completedQueriesForTest);
            }
        });
    }

    @Test //implicit transaction + rollback
    public void testQueryCountCreateInsertRollback() throws Exception {
        // former test name testCreateInsertRollback
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                test.connection.setAutoCommit(false);
                Statement statement = test.statement;

                boolean hasResult =
                        statement.execute("CREATE TABLE test(l long,s string); " +
                                "INSERT INTO test VALUES (19, 'k'); " +
                                "ROLLBACK TRANSACTION; " +
                                "INSERT INTO test VALUES (27, 'f'); " +
                                "SELECT * from test;");

                assertResults(statement, hasResult, count(0), count(1), count(0),
                        count(1), data(row(27L, "f")));


                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(8, startedQueriesForTest);
                Assert.assertEquals(8, completedQueriesForTest);
            }
        });
    }

    @Test //explicit transaction + commit
    public void testQueryCountErrorDoesntRollBackCommittedFirstInsertOnTwoTables() throws Exception {
        // former test name testBeginCreateInsertCommitThenErrorDoesntRollBackCommittedFirstInsertOnTwoTables
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                test.connection.setAutoCommit(false);
                Statement statement = test.statement;

                try {
                    statement.execute("BEGIN; " +
                            "CREATE TABLE testA(l long,s string); " +
                            "CREATE TABLE testB(s string,b byte);" +
                            "INSERT INTO testA VALUES (30, 'third'); " +
                            "INSERT INTO testB VALUES ('bird', 4);" +
                            "COMMIT; " +
                            "DELETE FROM testA; " +
                            "DELETE FROM testB;");
                    fail("PSQLException should be thrown");
                } catch (PSQLException e) {
                    assertEquals("ERROR: unexpected token: FROM\n  Position: 173", e.getMessage());
                }

                boolean hasResult = statement.execute("select * from testA; select  *from testB;");
                assertResults(statement, hasResult, data(row(30L, "third")), data(row("bird", (byte) 4)));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(13, startedQueriesForTest);
                Assert.assertEquals(12, completedQueriesForTest);
            }
        });
    }

    @Test  // does not count an Empty Query as a query in the metric
    public void testQueryCountForBlockWithEmptyQueryAtTheEnd() throws Exception {
        // former test name testRunBlockWithEmptyQueryAtTheEnd
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select 1;;");

                assertResults(statement, hasResult, data(row(1)));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(3, startedQueriesForTest);
                Assert.assertEquals(3, completedQueriesForTest);
            }
        });
    }

    @Test
    public void testQueryCountForSimpleStatement() throws Exception {
        // former test name testShowTableInBlock
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "SHOW TABLES; SELECT '15';");
                assertResults(statement, hasResult, data(row(configuration.getSystemTableNamePrefix() + "text_import_log")), data(row(15L)));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(4, startedQueriesForTest);
                Assert.assertEquals(4, completedQueriesForTest);
            }
        });
    }

    @Test
    public void testQueryCountForSingleBlockStatementWithSeveralQueries() throws Exception {
        // former test name testRunSeveralQueriesInSingleBlockStatementReturnsAllSelectResultsInOrder
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {

                boolean hasResult = test.statement.execute(
                        "create table test(l long, s string);" +
                                "insert into test values(1, 'a');" +
                                "insert into test values(2, 'b');" +
                                "select * from test;");

                assertResults(test.statement, hasResult, PGMultiStatementMessageTest.Result.ZERO, count(1), count(1),
                        data(row(1L, "a"), row(2L, "b")));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(6, startedQueriesForTest);
                Assert.assertEquals(6, completedQueriesForTest);
            }
        });
    }

    @Test
    public void testQueryCountForSmallQuery() throws Exception {
        // former test name testCreateInsertAsSelectReturnsProperUpdateCount
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute("CREATE TABLE test(l long, s string); " +
                        "INSERT INTO test select x,'str' from long_sequence(20);");
                assertResults(statement, hasResult, PGMultiStatementMessageTest.Result.ZERO, count(20));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(4, startedQueriesForTest);
                Assert.assertEquals(4, completedQueriesForTest);
            }
        });
    }

    @Test // does not count comment or empty statement as query in the metrics
    public void testQueryCountIgnoresEmptyStatementsAndComments() throws Exception {
        // former test name testRunSingleBlockStatementExecutesQueriesButIgnoresEmptyStatementsAndComments
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select 1;;/*multiline comment */;--single line comment\n;select 2;");

                assertResults(statement, hasResult, data(row(1)), data(row(2)));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(4, startedQueriesForTest);
                Assert.assertEquals(4, completedQueriesForTest);


            }
        });
    }

    @Test // does not single empty command in query metric
    public void testQueryCountSingleEmptyCommand() throws Exception {
        // former test name testRunSingleEmptyCommandProducesNoResult
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("");

                assertResults(statement, hasResult);

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(2, startedQueriesForTest);
                Assert.assertEquals(2, completedQueriesForTest);

            }
        });
    }

    @Test
    public void testQueryCountTruncate() throws Exception {
        // former test name testRunBlockWithCreateInsertTruncateSelectReturnsNoResult
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE TEST(l long, s string); " +
                                "INSERT INTO TEST VALUES (3, 'three'); /*some comment */" +
                                "TRUNCATE TABLE TEST;" +
                                "SELECT '1';");
                assertResults(statement, hasResult, count(0), count(1),
                        count(0), data(row("1")));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(6, startedQueriesForTest);
                Assert.assertEquals(6, completedQueriesForTest);
            }
        });
    }

    @Test // still counts a query even though it does not have a semicolon
    public void testQueryCountTwoBlocksEndWithoutSemicolon() throws Exception {
        // former test name testRunCOMMITWithSemicolonReturnsNextQueryResultOnly
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("COMMIT; select 3");
                assertResults(statement, hasResult, PGMultiStatementMessageTest.Result.ZERO, data(row(3L)));

                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(4, startedQueriesForTest);
                Assert.assertEquals(4, completedQueriesForTest);
            }
        });
    }

    @Test
    public void testQueryCountWithoutSemicolon() throws Exception {
        // former test name testRunBEGINWithoutSemicolonReturnsNoResult
        assertMemoryLeak(() -> {
            try (PGWireMetricsQueryCounterTest.PGTestSetup test = new PGWireMetricsQueryCounterTest.PGTestSetup()) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("BEGIN");
                assertResults(statement, hasResult, PGMultiStatementMessageTest.Result.ZERO);


                long startedQueriesForTest = metrics.pgWire().startedQueriesCounter().getValue() - totalQueriesStarted;
                long completedQueriesForTest = metrics.pgWire().completedQueriesCounter().getValue() - totalQueriesCompleted;
                Assert.assertEquals(3, startedQueriesForTest);
                Assert.assertEquals(3, completedQueriesForTest);
            }
        });
    }

    private static void assertResultSet(Statement s, PGMultiStatementMessageTest.Row[] rows) throws SQLException {
        ResultSet set = s.getResultSet();

        for (int rownum = 0; rownum < rows.length; rownum++) {
            PGMultiStatementMessageTest.Row row = rows[rownum];
            assertTrue("result set should have row #" + rownum + " with data " + row, set.next());

            for (int colnum = 0; colnum < row.length(); colnum++) {
                Object col = row.get(colnum);
                try {
                    if (col instanceof String) {
                        assertEquals(col, set.getString(colnum + 1));
                    } else if (col instanceof Long) {
                        assertEquals(col, set.getLong(colnum + 1));
                    } else if (col instanceof Byte) {
                        assertEquals(col, set.getByte(colnum + 1));
                    } else if (col instanceof Short) {
                        assertEquals(col, set.getShort(colnum + 1));
                    } else if (col instanceof Double) {
                        assertEquals(col, set.getDouble(colnum + 1));
                    } else {
                        assertEquals(col, set.getObject(colnum + 1));
                    }
                } catch (AssertionError ae) {
                    throw new AssertionError("row#" + rownum + " col#" + colnum + " " + ae.getMessage());
                }
            }
        }

        int rowsLeft = 0;
        while (set.next()) {
            rowsLeft++;
        }

        assertEquals("No more rows expected!", 0, rowsLeft);
    }

    private static void assertResults(Statement s, boolean hasFirstResult, PGMultiStatementMessageTest.Result... results) throws SQLException {
        if (results != null && results.length > 0) {

            if (results[0] != null) {
                try {
                    if (results[0].hasData()) {
                        assertTrue("Expected data in first result", hasFirstResult);
                        assertResultSet(s, results[0].rows);
                    } else {
                        assertFalse("Didn't expect data in first result", hasFirstResult);
                        assertEquals(results[0].updateCount, s.getUpdateCount());
                    }
                } catch (AssertionError ae) {
                    throw new AssertionError("Error asserting result#0 " + ae.getMessage(), ae);
                }
            }

            for (int i = 1; i < results.length; i++) {
                try {
                    if (results[i].hasData()) {
                        assertTrue("expected data in result", s.getMoreResults());
                        assertResultSet(s, results[i].rows);
                    } else {
                        assertFalse("didn't expect data in result #" + i, s.getMoreResults());
                        assertEquals("Expected update count", results[i].updateCount, s.getUpdateCount());
                    }
                } catch (AssertionError ae) {
                    throw new AssertionError("Error asserting result#" + i + " " + ae.getMessage(), ae);
                }
            }
        }

        //check there are no more results
        assertFalse("No more results expected", s.getMoreResults());
        assertEquals("No more results expected but got update count", -1, s.getUpdateCount());
    }

    static PGMultiStatementMessageTest.Result count(int updatedRows) {
        return new PGMultiStatementMessageTest.Result(updatedRows);
    }

    static PGMultiStatementMessageTest.Result data(PGMultiStatementMessageTest.Row... rows) {
        return new PGMultiStatementMessageTest.Result(rows);
    }

    static PGMultiStatementMessageTest.Result empty() {
        return PGMultiStatementMessageTest.Result.EMPTY;
    }

    static PGMultiStatementMessageTest.Result one() {
        return PGMultiStatementMessageTest.Result.ONE;
    }

    static PGMultiStatementMessageTest.Row row(Object... cols) {
        return new PGMultiStatementMessageTest.Row(cols);
    }

    static PGMultiStatementMessageTest.Result zero() {
        return PGMultiStatementMessageTest.Result.ZERO;
    }

    static class Result {
        //jdbc result with empty result set
        static final PGMultiStatementMessageTest.Result EMPTY = new PGMultiStatementMessageTest.Result();
        //jdbc result with no result set and update count = 1
        static final PGMultiStatementMessageTest.Result ONE = new PGMultiStatementMessageTest.Result(1);
        //jdbc result with no result set and update count = 0
        static final PGMultiStatementMessageTest.Result ZERO = new PGMultiStatementMessageTest.Result(0);
        PGMultiStatementMessageTest.Row[] rows;
        int updateCount;

        Result(PGMultiStatementMessageTest.Row... rows) {
            this.rows = rows;
        }

        Result(int updateCount) {
            this.updateCount = updateCount;
        }

        boolean hasData() {
            return rows != null;
        }
    }

    static class Row {
        Object[] cols;

        Row(Object... cols) {
            this.cols = cols;
        }

        @Override
        public String toString() {
            return "Row{" + Arrays.toString(cols) + '}';
        }

        Object get(int i) {
            return cols[i];
        }

        int length() {
            return cols.length;
        }
    }

    class PGTestSetup implements Closeable {
        final Connection connection;
        final PGWireServer server;
        final Statement statement;

        PGTestSetup(boolean useSimpleMode, long queryTimeout) throws SQLException, SqlException {
            server = createPGServer(2, queryTimeout);
            server.getWorkerPool().start(LOG);
            connection = getConnection(server.getPort(), useSimpleMode, true);
            statement = connection.createStatement();
        }

        PGTestSetup() throws SQLException, SqlException {
            this(true, Long.MAX_VALUE);
        }

        @Override
        public void close() throws IOException {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            server.getWorkerPool().halt();
            server.close();
        }
    }
}
