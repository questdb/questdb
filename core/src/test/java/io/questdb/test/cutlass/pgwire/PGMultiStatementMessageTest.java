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

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.SqlException;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PSQLException;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static io.questdb.test.cutlass.pgwire.BasePGTest.Mode.EXTENDED_FOR_PREPARED;
import static io.questdb.test.cutlass.pgwire.BasePGTest.Mode.SIMPLE;
import static org.junit.Assert.*;

/**
 * Class contains tests of PostgreSQL simple query statements containing multiple commands separated by ';'
 */
public class PGMultiStatementMessageTest extends BasePGTest {

    @Test
    public void testAsyncPGCommandBlockDoesntProduceError() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                boolean result = statement.execute(
                        "SELECT pg_advisory_unlock_all();" +
                                "CLOSE ALL;" +
                                "UNLISTEN *;" +
                                "RESET ALL;"
                );
                Assert.assertTrue(result);
                ResultSet results = statement.getResultSet();
                results.next();
                assertNull(null, results.getString(1));
            }
        });
    }

    @Test // explicit transaction + rollback on two tables
    public void testBeginCreateInsertCommitInsertRollbackRetainsOnlyCommittedDataOnTwoTables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                boolean hasResult =
                        statement.execute("BEGIN; " +
                                "CREATE TABLE testa(l long, s string);" +
                                "CREATE TABLE testb(b long, d double); " +
                                "INSERT INTO testa VALUES (150, '150'); " +
                                "INSERT INTO testb VALUES (78, 5.0);" +
                                "COMMIT TRANSACTION; " +
                                "BEGIN; " +
                                "INSERT INTO testa VALUES (29, 'g'); " +
                                "INSERT INTO testb VALUES (2, 1.0);" +
                                "ROLLBACK TRANSACTION; /* rolls back implicit txn */" +
                                "SELECT * from testa;" +
                                "SELECT * from testb;"
                        );
                assertResults(
                        statement,
                        hasResult,
                        Result.ZERO,
                        Result.ZERO,
                        Result.ZERO,
                        count(1),
                        count(1),
                        Result.ZERO,
                        Result.ZERO,
                        count(1),
                        count(1),
                        Result.ZERO,
                        data(row(150L, "150")),
                        data(row((byte) 78, 5.0d))
                );
            }
        });
    }

    @Test // explicit transaction + rollback on two tables
    public void testBeginCreateInsertCommitRollbackRetainsCommittedDataOnTwoTables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
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
                                "COMMIT;" +
                                "SELECT * from testA;" +
                                "SELECT * from testB;");

                assertResults(
                        statement,
                        hasResult,
                        zero(),
                        zero(),
                        zero(),
                        count(1),
                        count(1),
                        zero(),
                        zero(),
                        count(1),
                        count(1),
                        zero(),
                        data(row(50L, "z"), row(29L, "g")),
                        data(row((byte) 8, 1.0d), row((byte) 2, 1.0d))
                );
            }
        });
    }

    @Test // explicit transaction + commit
    public void testBeginCreateInsertCommitThenErrorDoesntRollBackCommittedFirstInsert() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                try {
                    // this is a JDBC driver quirk:
                    // in "simple" mode the server parses the 'script' and returns error position relative to
                    // the whole text, however in "extended" mode, the JDBC driver parses the 'script' and
                    // the server sees one SQL at a time. Therefore, the error position in this case
                    // is relative to the individual SQL and not the whole text.
                    statement.execute(
                            "BEGIN; " +
                                    "CREATE TABLE test(l long,s string); " +
                                    "INSERT INTO test VALUES (20, 'z'); " +
                                    "COMMIT TRANSACTION; " +
                                    "DELETE FROM1 test; " +
                                    "INSERT INTO test VALUES (21, 'x');"
                    );
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "ERROR: unexpected token [test]");
                }

                boolean hasResult = statement.execute("select * from test; ");
                assertResults(statement, hasResult, data(row(20L, "z")));
            }
        });
    }

    @Test // explicit transaction + commit
    public void testBeginCreateInsertCommitThenErrorDoesntRollBackCommittedFirstInsertOnTwoTables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                try {
                    statement.execute("BEGIN; " +
                            "CREATE TABLE testA(l long,s string); " +
                            "CREATE TABLE testB(s string,b byte);" +
                            "INSERT INTO testA VALUES (30, 'third'); " +
                            "INSERT INTO testB VALUES ('bird', 4); " +
                            "COMMIT; " +
                            "DELETE FROM testA; " +
                            "DELETE FROM testB;");
                    assertExceptionNoLeakCheck("PSQLException should be thrown");
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "ERROR: unexpected token [FROM]");
                }

                boolean hasResult = statement.execute("select * from testA; select  *from testB;");
                assertResults(statement, hasResult, data(row(30L, "third")), data(row("bird", (byte) 4)));
            }
        });
    }

    @Test // explicit transaction + rollback
    public void testBeginCreateInsertRollback() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult =
                    statement.execute("BEGIN; " +
                            "CREATE TABLE test(l long,s string); " +
                            "INSERT INTO test VALUES (19, 'k'); " +
                            "ROLLBACK TRANSACTION; " +
                            "INSERT INTO test VALUES (27, 'f'); " +
                            "COMMIT; " +
                            "SELECT * from test;");

            assertResults(
                    statement,
                    hasResult,
                    zero(),
                    zero(),
                    count(1),
                    zero(),
                    count(1),
                    zero(),
                    data(row(27L, "f"))
            );
        });
    }

    @Test // explicit transaction + rollback on two tables
    public void testBeginCreateInsertRollbackOnTwoTables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult =
                    statement.execute("BEGIN; " +
                            "CREATE TABLE testA(l long,s string); " +
                            "CREATE TABLE testB(b byte, d double); " +
                            "INSERT INTO testA VALUES (20, 'j'); " +
                            "INSERT INTO testB VALUES (1, 0.0);" +
                            "ROLLBACK TRANSACTION; " +
                            "INSERT INTO testA VALUES (29, 'g'); " +
                            "INSERT INTO testB VALUES (2, 1.0);" +
                            "COMMIT;" +
                            "SELECT * from testA;" +
                            "SELECT * from testB;");

            assertResults(
                    statement,
                    hasResult,
                    zero(),
                    zero(),
                    zero(),
                    count(1),
                    count(1),
                    zero(),
                    count(1),
                    count(1),
                    zero(),
                    data(row(29L, "g")),
                    data(row((byte) 2, 1.0d))
            );
        });
    }

    @Test
    public void testBeginReturnsZeroResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("BEGIN");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("BEGIN;");
            assertResults(statement, hasResult, Result.ZERO);
        });
    }

    @Test
    public void testBeginThenSelectReturnsSelectResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("BEGIN; select 2");
            assertResults(statement, hasResult, Result.ZERO, data(row(2L)));
        });
    }

    @Test
    public void testBlockWithEmptyQueriesAndComments() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select 1;" +
                    ";" +
                    "/*multiline comment */;" +
                    "--single line comment\n;" +
                    "select 2;");

            if (mode == SIMPLE || mode == EXTENDED_FOR_PREPARED) {
                assertResults(statement, hasResult,
                        data(row(1)),
                        data(row(2)));
            } else {
                assertResults(statement, hasResult,
                        data(row(1)),
                        Result.ZERO,
                        Result.ZERO,
                        data(row(2)));
            }
        });
    }

    @Test
    public void testCachedPgStatementReturnsDataUsingProperFormatOnRecompilation() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement stmt = connection.createStatement()) {
                boolean hasResult = stmt.execute("CREATE TABLE mytable(l int, s text);");
                assertResults(stmt, hasResult, zero());

                try (PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM mytable")) {
                    hasResult = pstmt.execute();
                    assertResults(pstmt, hasResult, empty());

                    hasResult = stmt.execute("DROP TABLE mytable; CREATE TABLE mytable(l int, s text); INSERT INTO mytable VALUES(1, 'a'); ");

                    assertResults(stmt, hasResult, zero(), zero(), one());
                }
            }

            try (Connection connection2 = getConnection(mode, port, true);
                 PreparedStatement pstmt = connection2.prepareStatement("SELECT * FROM mytable")) {

                boolean hasResult = pstmt.execute();
                assertResults(pstmt, hasResult, data(row(1L, "a")));
            }
        });
    }

    @Test
    public void testCachedTextFormatPgStatementReturnsDataUsingBinaryFormatWhenClientRequestsIt() throws Exception {
        // Exclude quirks, because they send P(arse) message for all SQL statements in the script
        // and only then (E)xecute them. This means at the time when it's parsing 'insert into mytable ...'
        // the 'mytable' table does not exist yet. because the CREATE TABLE was not yet (E)xecuted.
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            try (
                    Statement stmt = connection.createStatement()
            ) {
                connection.setAutoCommit(true);

                boolean hasResult = stmt.execute(
                        "CREATE TABLE mytable(l int, s text); " +
                                "INSERT INTO mytable VALUES(1, 'a');"
                );
                assertResults(stmt, hasResult, zero(), one());

                ((PgConnection) connection).setForceBinary(true);//force binary transfer for int column

                try (PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM mytable")) {
                    hasResult = pstmt.execute();
                    assertResults(pstmt, hasResult, data(row(1L, "a")));
                }
            }
        });
    }

    @Test
    public void testCloseReturnsZeroResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute("CLOSE");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("CLOSE;");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("CLOSE ALL");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("CLOSE ALL;");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("CLOSE XYZ");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("CLOSE XYZ;");
            assertResults(statement, hasResult, Result.ZERO);
        });
    }

    @Test
    public void testCloseThenSelectReturnsSelectResult() throws Exception {
        // legacy code fails in quirks mode, include quirks when legacy is removed
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("CLOSE ALL; select 6");
            assertResults(statement, hasResult, Result.ZERO, data(row(6L)));

            hasResult = statement.execute("CLOSE; select 7");
            assertResults(statement, hasResult, Result.ZERO, data(row(7L)));
        });
    }

    @Test
    public void testCommentOnlyQuery() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("/*comment*/");
            assertResults(statement, hasResult, Result.ZERO);
        });
    }

    @Test
    public void testCommitReturnsZeroResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute("COMMIT");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("COMMIT;");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("COMMIT TRANSACTION");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("COMMIT TRANSACTION;");
            assertResults(statement, hasResult, Result.ZERO);
        });
    }

    @Test
    public void testCommitThenSelectReturnsSelectResult() throws Exception {
        // legacy code fails in quirks mode, include quirks when legacy is removed
        assertWithPgServer(7L, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("COMMIT; select 3");
            assertResults(statement, hasResult, Result.ZERO, data(row(3L)));
        });
    }

    @Test
    public void testCreateAsSelectReturnsRightInsertCount() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute(
                    "CREATE TABLE test as (select x from long_sequence(3)); " +
                            "SELECT * from test;");

            // modern code returns row count for "create as select", while legacy does not
            assertResults(statement, hasResult,
                    count(3),
                    data(row("1"), row("2"), row("3")));
        });
    }

    @Test
    public void testCreateBeginInsertCommitInsertErrorRetainsOnlyCommittedData() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            try {
                statement.execute("CREATE TABLE mytable(l long); " +
                        "BEGIN; " +
                        "INSERT INTO mytable VALUES(1); " +
                        "COMMIT; " +
                        "INSERT INTO mytable VALUES(2); " +
                        "DELETE FROM mytable3;");
                Assert.fail();
            } catch (PSQLException e) {
                int expectedPos = mode == SIMPLE || mode == EXTENDED_FOR_PREPARED ? 115 : 9;
                assertEquals("ERROR: unexpected token [FROM]\n  Position: " + expectedPos, e.getMessage());
            }
            boolean hasResult = statement.execute("select * from mytable;");
            assertResults(statement, hasResult, data(row(1L), row(2L)));
        });
    }

    @Test
    @Ignore("non-wal table (unpartitioned) cannot be altered after insert, the plan is to decommission non-WAL tables eventually")
    // ERROR: Timeout expired on waiting for the async command execution result [instance=1]
    public void testCreateInsertAlterAddColumnThenRollbackLeavesEmptyTable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult =
                    statement.execute("CREATE TABLE mytable(l long); " +
                            "BEGIN; " +
                            "INSERT INTO mytable VALUES(27); " +
                            "ALTER TABLE mytable ADD COLUMN s string; " +
                            "ROLLBACK; " +
                            "SELECT * From mytable; ");

            assertResults(statement, hasResult,
                    Result.ZERO, Result.ZERO, count(1),
                    Result.ZERO, Result.ZERO, Result.EMPTY);
        });
    }

    @Test
    public void testCreateInsertAlterAddColumnThenRollbackLeavesEmptyTableWal() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult =
                    statement.execute(
                            "CREATE TABLE mytable(l long, t timestamp) timestamp(t) partition by hour wal; " +
                                    "BEGIN; " +
                                    "INSERT INTO mytable VALUES(27, 0); " +
                                    "ALTER TABLE mytable ADD COLUMN s string; " +
                                    "ROLLBACK; " +
                                    "SELECT * From mytable; "
                    );

            assertResults(
                    statement,
                    hasResult,
                    Result.ZERO,
                    Result.ZERO,
                    count(1),
                    Result.ZERO,
                    Result.ZERO,
                    Result.EMPTY
            );
        });
    }

    @Test
    public void testCreateInsertCommitThenErrorDoesntRollBackCommittedFirstInsert() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            try {
                statement.execute("BEGIN; " +
                        "CREATE TABLE test(l long,s string); " +
                        "INSERT INTO test VALUES (19, 'k'); " +
                        "COMMIT; " +
                        "DELETE FROM test; " +
                        "INSERT INTO test VALUES (21, 'x');");
                assertExceptionNoLeakCheck("PSQLException should be thrown");
            } catch (PSQLException e) {
                int expectedPos = mode == SIMPLE || mode == EXTENDED_FOR_PREPARED ? 94 : 9;
                assertEquals("ERROR: unexpected token [FROM]\n  Position: " + expectedPos, e.getMessage());
            }

            boolean hasResult = statement.execute("select * from test; ");
            assertResults(statement, hasResult, data(row(19L, "k")));
        });
    }

    @Test
    public void testCreateInsertCommitThenErrorDoesntRollBackCommittedFirstInsertOnTwoTables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            try {
                statement.execute("BEGIN; " +
                        "CREATE TABLE testA(l long,s string); " +
                        "CREATE TABLE testB(s symbol, sh short ); " +
                        "INSERT INTO testA VALUES (190, 'ka'); " +
                        "INSERT INTO testB VALUES ('test', 12); " +
                        "COMMIT; " +
                        "DELETE FROM testA; " +
                        "DELETE FROM testB; " +
                        "INSERT INTO testA VALUES (21, 'x');");
                assertExceptionNoLeakCheck("PSQLException should be thrown");
            } catch (PSQLException e) {
                int expectedPos = mode == SIMPLE || mode == EXTENDED_FOR_PREPARED ? 178 : 9;
                assertEquals("ERROR: unexpected token [FROM]\n  Position: " + expectedPos, e.getMessage());
            }

            boolean hasResult = statement.execute("select * from testA; select * from testB; ");
            assertResults(statement, hasResult, data(row(190L, "ka")), data(row("test", (short) 12)));
        });
    }

    @Test
    public void testCreateInsertRepairTableSelectFromTableInBlock() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute(
                    "CREATE TABLE test(l long,ts timestamp); " +
                            "INSERT INTO test VALUES(1989, 0); " +
                            "SELECT l from test;");
            assertResults(statement, hasResult, Result.ZERO, count(1), data(row(1989L)));
        });
    }

    @Test
    public void testCreateInsertRollback() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute("BEGIN; " +
                    "CREATE TABLE test(l long,s string); " +
                    "INSERT INTO test VALUES (19, 'k'); " +
                    "ROLLBACK; " +
                    "INSERT INTO test VALUES (27, 'f'); " +
                    "COMMIT; " +
                    "SELECT * from test;"
            );

            assertResults(
                    statement,
                    hasResult,
                    Result.ZERO,
                    Result.ZERO,
                    count(1),
                    Result.ZERO,
                    count(1),
                    Result.ZERO,
                    data(row(27L, "f")
                    )
            );
        });
    }

    @Test
    public void testCreateInsertRollbackOnTwoTables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);

            boolean hasResult =
                    statement.execute("BEGIN; " +
                            "CREATE TABLE testA(l long,s string); " +
                            "CREATE TABLE testB(c char, d double); " +
                            "INSERT INTO testA VALUES (198, 'cop'); " +
                            "INSERT INTO testB VALUES ('q', 2.0); " +
                            "ROLLBACK; " +
                            "INSERT INTO testA VALUES (-27, 'o'); " +
                            "INSERT INTO testB VALUES ('z', 1.0); " +
                            "SELECT * from testA; " +
                            "SELECT * from testB; ");

            assertResults(statement, hasResult,
                    Result.ZERO,
                    Result.ZERO,
                    Result.ZERO,
                    count(1),
                    count(1),
                    Result.ZERO,
                    count(1),
                    count(1),
                    data(row(-27L, "o")),
                    data(row("z", 1.0))
            );
        });
    }

    @Test
    public void testCreateInsertSelectWithFromTableInBlock() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute(
                    "CREATE TABLE test(l long,s string); " +
                            "INSERT INTO test VALUES (20, 'z'), (20, 'z'); " +
                            "WITH x AS (SELECT DISTINCT l,s FROM test) SELECT l,s from x; ");
            assertResults(statement, hasResult, Result.ZERO, count(2), data(row(20L, "z")));
        });
    }

    @Test
    @Ignore("The final SELECT doesn't observe the effect of ALTER TABLE, with or without ROLLBACK")
    public void testCreateInsertThenAlterTableRenameThenRollbackLeavesNonEmptyTable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult =
                    statement.execute("CREATE TABLE mytable(l long); " +
                            "BEGIN; " +
                            "INSERT INTO mytable VALUES(27); " +
                            "ALTER TABLE mytable rename COLUMN l to i; " +
                            "ROLLBACK; " +
                            "SELECT i FROM mytable; ");

            assertResults(statement, hasResult,
                    Result.ZERO, Result.ZERO, count(1),
                    Result.ZERO, Result.ZERO, data(row(27L)));
        });
    }

    @Test
    @Ignore("Drop can't acquire lock on table, held by the previous insert")
    public void testCreateInsertThenDropDoesNotSelfLock() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult =
                    statement.execute(
                            "BEGIN; " +
                                    "CREATE TABLE mytable(l long); " +
                                    "INSERT INTO mytable values(1); " +
                                    "DROP TABLE mytable; ");

            assertResults(statement, hasResult,
                    Result.ZERO, Result.ZERO, count(1), Result.ZERO);
        });
    }

    @Test
    public void testCreateInsertThenErrorRollsBackInsert() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);

            try {
                statement.execute("BEGIN; " +
                        "CREATE TABLE test(l long,s string); " +
                        "INSERT INTO test VALUES (20, 'z'); " +
                        "DELETE FROM test; " +
                        "COMMIT; " +
                        "INSERT INTO test VALUES (20, 'z');");
            } catch (PSQLException e) {
                int expectedPos = mode == SIMPLE || mode == EXTENDED_FOR_PREPARED ? 86 : 9;
                assertEquals("ERROR: unexpected token [FROM]\n  Position: " + expectedPos, e.getMessage());
            }

            boolean hasResult = statement.execute("select * from test; ");
            assertResults(statement, hasResult, Result.EMPTY);
        });
    }

    @Test
    public void testCreateInsertThenErrorRollsBackInsertOnTwoTables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);

            try {
                statement.execute("BEGIN; " +
                        "CREATE TABLE testA(l long,s string); " +
                        "CREATE TABLE testB(c char,i int); " +
                        "INSERT INTO testA VALUES (-1, 'z'); " +
                        "INSERT INTO testB VALUES ('a', 45); " +
                        "DELETE FROM testA; " +
                        "INSERT INTO testA VALUES (20, 'z');");
            } catch (PSQLException e) {
                int expectedPos = mode == SIMPLE || mode == EXTENDED_FOR_PREPARED ? 158 : 9;
                assertEquals("ERROR: unexpected token [FROM]\n  Position: " + expectedPos, e.getMessage());
            }

            boolean hasResult = statement.execute("select * from testA; select * from testB;");
            assertResults(statement, hasResult, Result.EMPTY, Result.EMPTY);
        });
    }

    @Test
    public void testCreateMultiInsertSelectFromTableInBlock() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute(
                    "CREATE TABLE test(l long,s string); " +
                            "INSERT INTO test VALUES (1970, 'a'), (1971, 'b') ; " +
                            "SELECT l,s from test;");
            assertResults(statement, hasResult,
                    Result.ZERO, count(2), data(row(1970L, "a"), row(1971L, "b")));
        });
    }

    @Test
    @Ignore("Truncate table fails to acquire lock taken by earlier insert in the same transaction")
    // truncate commits existing transaction and is non-transactional
    public void testCreateNormalInsertThenTruncateThenRollbackLeavesEmptyTable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult =
                    statement.execute("CREATE TABLE mytable(l long); " +
                            "BEGIN; " +
                            "INSERT INTO mytable VALUES(1); " +
                            "TRUNCATE TABLE mytable; " +
                            "ROLLBACK; " +
                            "SELECT * From mytable; ");

            assertResults(statement, hasResult,
                    Result.ZERO, Result.ZERO, count(1),
                    Result.ZERO, Result.ZERO, empty()
            );
        });
    }

    @Test
    @Ignore("Insert-as-select fails to acquire lock taken by earlier insert in the same transaction")
    // Insert-as-select is not transactional. It commits existing transaction and again after inserting data.
    public void testCreateTableInsertThenInsertAsSelectThenRollbackLeavesNonEmptyTable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult =
                    statement.execute("CREATE TABLE mytable(l long); " +
                            "BEGIN; " +
                            "INSERT INTO mytable VALUES(1); " +
                            "INSERT INTO mytable select x+1 from long_sequence(2); " +
                            "ROLLBACK; " +
                            "SELECT * From mytable; ");

            assertResults(statement, hasResult,
                    Result.ZERO, Result.ZERO, count(1), count(2),
                    Result.ZERO, data(row(1L), row(2L), row(3L))
            );
        });
    }

    @Test // test interleaved extended query execution they don't spill bind formats
    public void testDifferentExtendedQueriesExecutedInExtendedModeDoNotSpillFormats() throws Exception {
        // Exclude quirks, because they send P(arse) message for all SQL statements in the script
        // and only then (E)xecute them. This means at the time when it's parsing 'INSERT INTO mytable...;'
        // the 'mytable' table does not exist yet. because the CREATE TABLE was not yet (E)xecuted.
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            try (Statement stmt = connection.createStatement()) {
                connection.setAutoCommit(true);

                boolean hasResult = stmt.execute("CREATE TABLE mytable(l int, s text); INSERT INTO mytable VALUES(53, 'z');");
                assertResults(stmt, hasResult, zero(), one());

                PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM mytable");
                hasResult = pstmt.execute();
                assertResults(pstmt, hasResult, data(row(53L, "z")));
                pstmt.close();
            }
        });
    }

    @Test
    public void testDiscardReturnsZeroResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute("DISCARD");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("DISCARD;");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("DISCARD ALL");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("DISCARD PLANS");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("DISCARD SEQUENCES");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("DISCARD TEMPORARY");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("DISCARD TEMP");
            assertResults(statement, hasResult, Result.ZERO);
        });
    }

    @Test
    public void testDiscardThenSelectReturnsSelectResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("DISCARD ALL; select 5");
            assertResults(statement, hasResult, Result.ZERO, data(row(5L)));
        });
    }

    @Test
    public void testEmptyQueryAtTheEnd() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select 1;;");

            assertResults(statement, hasResult, data(row(1)));
        });
    }

    @Test
    public void testEmptyQueryReturnsNoResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("");
            assertResults(statement, hasResult);

            hasResult = statement.execute(" ");
            assertResults(statement, hasResult);

            hasResult = statement.execute(";");
            assertResults(statement, hasResult);

            hasResult = statement.execute(";;");
            assertResults(statement, hasResult);

            hasResult = statement.execute(" ; ");
            assertResults(statement, hasResult);

            hasResult = statement.execute(" ; ");
            assertResults(statement, hasResult);

            hasResult = statement.execute(" ; ;");
            assertResults(statement, hasResult);

        });
    }

    @Test
    public void testPgLockTwice() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean result = statement.execute("SELECT pg_advisory_unlock_all(); " +
                    "CREATE TABLE test( l long); " +
                    "INSERT INTO test VALUES(1);");
            assertResults(statement, result, data(row((String) null)), Result.ZERO, count(1));

            connection.setAutoCommit(false);
            PreparedStatement pStmt = connection.prepareStatement("select * from test;");
            result = pStmt.execute();
            assertResults(pStmt, result, data(row(1L)));
            connection.rollback();

            result = statement.execute("SELECT pg_advisory_unlock_all(); " +
                    "CLOSE ALL; " +
                    "UNLISTEN *; " +
                    "RESET ALL;");
            assertResults(statement, result, data(row((String) null)), Result.ZERO, Result.ZERO, Result.ZERO);
        });
    }

    @Test // edge case - run the same query with binary protocol in extended mode and then the same in query block
    public void testQueryExecutedInBatchModeDoesNotUseCachedStatementBinaryFormat() throws Exception {
        // Exclude quirks, because they send P(arse) message for all SQL statements in the script
        // and only then (E)xecute them. This means at the time when it's parsing 'INSERT INTO mytable...;'
        // the 'mytable' table does not exist yet. because the CREATE TABLE was not yet (E)xecuted.
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            try (Statement stmt = connection.createStatement()) {
                connection.setAutoCommit(true);

                boolean hasResult = stmt.execute("CREATE TABLE mytable(l int, s text); INSERT INTO mytable VALUES(33, 'x');");
                assertResults(stmt, hasResult, zero(), one());

                ((PgConnection) connection).setForceBinary(true);//force binary transfer for int column

                PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM mytable");
                hasResult = pstmt.execute();
                assertResults(pstmt, hasResult, data(row(33L, "x")));
                pstmt.close();

                hasResult = stmt.execute("SELECT * FROM mytable");
                assertResults(stmt, hasResult, data(row(33L, "x")));
            }
        });
    }

    @Test
    public void testQueryWithJustCommentsReturnsNoResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("/* comment */");
            assertResults(statement, hasResult);

            hasResult = statement.execute("--comment");
            assertResults(statement, hasResult);

            hasResult = statement.execute(";--comment");
            assertResults(statement, hasResult);

            hasResult = statement.execute("; /* comment */");
            assertResults(statement, hasResult);
        });
    }

    @Test
    public void testResetReturnsZeroResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute("RESET config_param");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("RESET config_param;");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("RESET ALL");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("RESET ALL;");
            assertResults(statement, hasResult, Result.ZERO);
        });
    }

    @Test
    public void testResetThenSelectReturnsSelectResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("RESET configuration_parameter; select 10");
            assertResults(statement, hasResult, Result.ZERO, data(row(10L)));

            hasResult = statement.execute("RESET ALL; select 11");
            assertResults(statement, hasResult, Result.ZERO, data(row(11L)));
        });
    }

    @Test
    public void testRestartDueToStaleCompilationDoesNotDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, Integer.MAX_VALUE - 1);
            engine.execute("create table x (ts timestamp, i int) timestamp(ts) partition by day wal", sqlExecutionContext);

            CyclicBarrier barrier = new CyclicBarrier(2);
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            new Thread(() -> {
                try {
                    while (System.nanoTime() < deadlineNanos && barrier.getNumberWaiting() == 0) {
                        engine.execute("alter table x add column distraction int", sqlExecutionContext);
                        Os.sleep(1); // give compiler a chance to compile and execute
                        if (barrier.getNumberWaiting() != 0) {
                            break;
                        }
                        engine.execute("alter table x drop column distraction", sqlExecutionContext);
                        Os.sleep(1);
                    }
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                } finally {
                    Path.clearThreadLocals();
                    try {
                        barrier.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }
            }).start();

            // the SQL includes INSERT can later test that we don't get duplicate rows
            // when SQL execution is re-started
            try (PGTestSetup test = new PGTestSetup()) {
                Statement statement = test.statement;
                for (int i = 0; i < 1000; i++) {
                    statement.execute(
                            "INSERT INTO x (ts, i) VALUES(now(), 1); " +
                                    "SELECT * FROM x; ");
                }
            } finally {
                barrier.await();
            }
            drainWalQueue();
            try (RecordCursorFactory factory = select("select count() from x", sqlExecutionContext)) {
                assertCursor("count\n" +
                                "1000\n",
                        factory,
                        false, false, true
                );
            }
        });
    }

    @Test
    public void testRollbackReturnsZeroResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("ROLLBACK");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("ROLLBACK;");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("ROLLBACK TRANSACTION");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("ROLLBACK TRANSACTION;");
            assertResults(statement, hasResult, Result.ZERO);

        });
    }

    @Test
    public void testRollbackThenSelectReturnsSelectResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("ROLLBACK TRANSACTION; select 4");
            assertResults(statement, hasResult, Result.ZERO, data(row(4L)));
        });
    }

    @Test
    public void testRunBlockWithCommentAtTheEnd() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -2, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select 1;/* comment */");
            assertResults(statement, hasResult, data(row(1)), Result.ZERO);
        });
    }

    @Test
    @Ignore
    // Legacy code fails with "table does not exist [table=TEST].
    // Modern code fails in DROP: could not lock 'TEST' [reason='busyReader']
    public void testRunBlockWithCreateInsertSelectDropReturnsSelectResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute(
                    "CREATE TABLE TEST(l long, s string); " +
                            "INSERT INTO TEST VALUES (3, 'three'); " +
                            "SELECT * from TEST;" +
                            "DROP TABLE TEST;"
            );
            assertResults(
                    statement,
                    hasResult,
                    Result.ZERO,
                    count(1),
                    data(row(3L, "three")),
                    Result.ZERO
            );
        });
    }

    @Test
    public void testRunSeveralQueriesInBlockReturnsAllSelectResultsInOrder() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute(
                    "create table test(l long, s string);" +
                            "insert into test values(1, 'a');" +
                            "insert into test values(2, 'b');" +
                            "select * from test;");


            assertResults(statement, hasResult, Result.ZERO, count(1), count(1),
                    data(row(1L, "a"), row(2L, "b"))
            );
        });
    }

    @Test
    @Ignore("table reader can't see uncommitted writes")
    public void testSelectCanSeePriorInsertInTheSameTransaction() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult =
                    statement.execute("BEGIN; " +
                            "CREATE TABLE testA(l long,s string); " +
                            "INSERT INTO testA VALUES (50, 'z'); " +
                            "INSERT INTO testA VALUES (29, 'g'); " +
                            "SELECT * from testA;");

            assertResults(statement, hasResult, zero(), zero(),
                    count(1), count(1),
                    data(row(50L, "z"), row(29L, "g"))
            );
        });
    }

    @Test
    public void testSelectWithMultiLineCommentAtEnd() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select  'hello11' /* end comment*/");
            assertResults(statement, hasResult, data(row("hello11")));
        });
    }

    @Test
    public void testSelectWithMultiLineCommentAtStart() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("/* comment here */ select  'hello9'");
            assertResults(statement, hasResult, data(row("hello9")));
        });
    }

    @Test
    public void testSelectWithMultiLineCommentInTheMiddle() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute(" select /* comment here */ 'hello10'");
            assertResults(statement, hasResult, data(row("hello10")));
        });
    }

    @Test
    public void testSelectWithSemicolonAtTheEnd() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select 'hello2';");
            assertResults(statement, hasResult, data(row("hello2")));
        });
    }

    @Test
    public void testSelectWithSemicolonInAlias() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select 'hello3' as \"alias;\" ;");
            assertResults(statement, hasResult, data(row("hello3")));
        });
    }

    @Test
    public void testSelectWithSemicolonInString() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select 'hello4;select this_is_not_a_query()'");
            assertResults(statement, hasResult, data(row("hello4;select this_is_not_a_query()")));
        });
    }

    @Test
    public void testSelectWithSingleLineCommentAtEnd() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select  'hello7' -- end comment");
            assertResults(statement, hasResult, data(row("hello7")));
        });
    }

    @Test
    public void testSelectWithSingleLineCommentAtStart() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("-- comment \n select  'hello6'");
            assertResults(statement, hasResult, data(row("hello6")));
        });
    }

    @Test
    public void testSelectWithSingleLineCommentInMiddle() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select -- comment \n 'hello5'");
            assertResults(statement, hasResult, data(row("hello5")));
        });
    }

    @Test
    public void testSelectWrappedInMultiLineCommentReturnsNoResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("/* comment start select  'hello12' */");
            assertResults(statement, hasResult);
        });
    }

    @Test
    public void testSelectWrappedInSingleLineCommentAtEndReturnsNoResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("-- commented out command select  'hello8'; ");
            assertResults(statement, hasResult);
        });
    }

    @Test
    public void testSetReturnsZeroResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("SET a = 'b'");
            assertResults(statement, hasResult, Result.ZERO);
        });
    }

    @Test
    public void testSetThenSelectReturnsSelectResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("SET a = 'b'; select 1");
            assertResults(statement, hasResult, Result.ZERO, data(row(1L)));
        });
    }

    @Test
    public void testShowTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            engine.execute("create table test (i int);", sqlExecutionContext);
            try (PGTestSetup test = new PGTestSetup()) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute("SHOW TABLES; SELECT '15';");
                assertResults(statement, hasResult, data(row("test")), data(row(15L)));
            }
        });
    }

    @Test
    public void testShowTablesThenSelect() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            engine.execute("create table test (i int);", sqlExecutionContext);
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute("SHOW TABLES; SELECT '15';");
            assertResults(statement, hasResult, data(row("test")), data(row(15L)));
        });
    }

    @Test
    public void testSingleSelectWithoutSemicolonAtTheEnd() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("select 'hello'");
            assertResults(statement, hasResult, data(row("hello")));
        });
    }

    @Test
    public void testUnlistenReturnsNoResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            boolean hasResult = statement.execute("UNLISTEN some_channel");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("UNLISTEN some_channel;");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("UNLISTEN *");
            assertResults(statement, hasResult, Result.ZERO);

            hasResult = statement.execute("UNLISTEN *;");
            assertResults(statement, hasResult, Result.ZERO);
        });
    }

    @Test
    public void testUnlistenThenSelectReturnsSelectResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("UNLISTEN channel_name; select 8");
            assertResults(statement, hasResult, Result.ZERO, data(row(8L)));

            hasResult = statement.execute("UNLISTEN *; select 9");
            assertResults(statement, hasResult, Result.ZERO, data(row(9L)));
        });
    }

    @Test
    public void testWhitespaceOnlyReturnsNoResult() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();
            boolean hasResult = statement.execute("   \n \t");
            assertResults(statement, hasResult);
        });
    }

    private static void assertResultSet(Statement s, Row[] rows) throws SQLException {
        ResultSet set = s.getResultSet();

        for (int rownum = 0; rownum < rows.length; rownum++) {
            Row row = rows[rownum];
            assertTrue("result set should have row #" + rownum + " with data " + row, set.next());

            for (int colnum = 0; colnum < row.length(); colnum++) {
                Object col = row.get(colnum);
                try {
                    assertEquals("Number of columns in result set",
                            row.length(), set.getMetaData().getColumnCount());
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

        assertEquals("Number of rows in result set", rows.length, rows.length + rowsLeft);
    }

    private static void assertResults(Statement s, boolean hasFirstResult, Result... results) throws SQLException {
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
                    throw new AssertionError("Error asserting result#0: " + ae.getMessage(), ae);
                }
            }

            for (int i = 1; i < results.length; i++) {
                try {
                    if (results[i].hasData()) {
                        assertTrue("expected data in result", s.getMoreResults());
                        assertResultSet(s, results[i].rows);
                    } else {
                        assertFalse("didn't expect data in result #" + i, s.getMoreResults());
                        assertEquals("Update count", results[i].updateCount, s.getUpdateCount());
                    }
                } catch (AssertionError ae) {
                    throw new AssertionError("Error asserting result#" + i + ": " + ae.getMessage(), ae);
                }
            }
        }

        //check there are no more results
        assertFalse("No more results expected", s.getMoreResults());
        assertEquals("Update count", -1, s.getUpdateCount());
    }

    static Result count(int updatedRows) {
        return new Result(updatedRows);
    }

    static Result data(Row... rows) {
        return new Result(rows);
    }

    static Result empty() {
        return Result.EMPTY;
    }

    static Result one() {
        return Result.ONE;
    }

    static Row row(Object... cols) {
        return new Row(cols);
    }

    static Result zero() {
        return Result.ZERO;
    }

    static class Result {
        //jdbc result with empty result set
        static final Result EMPTY = new Result();
        //jdbc result with no result set and update count = 1
        static final Result ONE = new Result(1);
        //jdbc result with no result set and update count = 0
        static final Result ZERO = new Result(0);
        Row[] rows;
        int updateCount;

        Result(Row... rows) {
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
        final PGServer server;
        final Statement statement;

        PGTestSetup(boolean useSimpleMode) throws SQLException, SqlException {
            server = createPGServer(2);
            server.getWorkerPool().start(LOG);
            connection = getConnection(server.getPort(), useSimpleMode, true);
            statement = connection.createStatement();
        }

        PGTestSetup() throws SQLException, SqlException {
            this(true);
        }

        @Override
        public void close() {
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
