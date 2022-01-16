/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.pgwire;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Class contains tests of PostgreSQL simple query statements containing multiple commands separated by ';'
 */
public class PGMultiStatementMessageTest extends BasePGTest {

    private static final Log LOG = LogFactory.getLog(PGMultiStatementMessageTest.class);

    //https://github.com/questdb/questdb/issues/1777
    //all of these commands are no-op (at the moment)
    @Test
    public void testAsyncPGCommandBlockDoesntProduceError() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGWireServer ignored = createPGServer(2);
                    final Connection connection = getConnection(true, true)
            ) {
                try (Statement statement = connection.createStatement()) {
                    boolean result = statement.execute("SELECT pg_advisory_unlock_all();\nCLOSE ALL;\nUNLISTEN *;\nRESET ALL;");
                    Assert.assertTrue(result);
                    ResultSet results = statement.getResultSet();
                    results.next();
                    assertNull(null, results.getString(1));
                }
            }
        });
    }

    @Test
    public void testRunSeveralQueriesInSingleBlockStatementReturnsAllSelectResultsInOrder() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGWireServer ignored = createPGServer(2);
                    final Connection connection = getConnection(true, true);
                    Statement statement = connection.createStatement()
            ) {
                boolean hasResult = statement.execute(
                        "create table test(l long, s string);" +
                            "insert into test values(1, 'a');" +
                            "insert into test values(2, 'b');" +
                            "select * from test;");

                assertResults( statement, hasResult, Result.ZERO, result(1), result(1),
                        result(row(1L, "a"), row(2L, "b")) );
            }
        });
    }

    @Test
    public void testRunSingleBlockStatementExecutesQueriesButIgnoresEmptyStatementsAndComments() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select 1;;/*multiline comment */;--single line comment\n;select 2;");
                
                assertResults(statement, hasResult, result(row(1)), result(row(2)));
            }
        });
    }

    @Test
    public void testRunSingleEmptyCommandProducesNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("");
                
                assertResults(statement, hasResult);
            }
        });
    }

    @Test
    public void testRunSingleCommandWithWhitespaceOnlyProducesNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("   \n \t");
                assertResults(statement, hasResult);
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWithoutSemicolonAtTheEndReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select 'hello'");
                assertResults(statement, hasResult, result( row("hello")));
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWithSemicolonAtTheEndReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select 'hello2';");
                assertResults(statement, hasResult, result(row("hello2")));
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWithSemicolonInAliasReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select 'hello3' as \"alias;\" ;");
                assertResults(statement, hasResult, result(row("hello3")));
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWithSemicolonInStringReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select 'hello4;select this_is_not_a_query()'");
                assertResults(statement, hasResult, result(row("hello4;select this_is_not_a_query()")));
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWithSingleLineCommentInMiddleReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select -- comment \n 'hello5'");
                assertResults(statement, hasResult, result(row("hello5")));
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWithSingleLineCommentAtStartReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("-- comment \n select  'hello6'");
                assertResults(statement, hasResult, result(row("hello6")));
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWithSingleLineCommentAtEndReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select  'hello7' -- end comment");
                assertResults(statement, hasResult, result(row("hello7")));
            }
        });
    }

    @Test
    public void testRunSingleSelectWrappedInSingleLineCommentAtEndReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("-- commented out command select  'hello8'; ");
                assertResults(statement, hasResult);
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWithMultiLineCommentAtStartReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("/* comment here */ select  'hello9'");
                assertResults(statement, hasResult, result(row("hello9")));
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWithMultiLineCommentInTheMiddleReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute(" select /* comment here */ 'hello10'");
                assertResults(statement, hasResult, result(row("hello10")));
            }
        });
    }
    
    @Test
    public void testRunSingleSelectCommandWithMultiLineCommentAtEndReturnsRow() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("select  'hello11' /* end comment*/");
                assertResults(statement, hasResult, result(row("hello11")));
            }
        });
    }

    @Test
    public void testRunSingleSelectCommandWrappedInMultiLineCommentReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("/* comment start select  'hello12' */");
                assertResults(statement, hasResult);
            }
        });
    }

    @Test
    public void testRunSETWithSemicolonReturnsNextQueryResultOnly() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("SET a = 'b'; select 1");
                assertResults( statement, hasResult, Result.ZERO, result(row(1L)) );
            }
        });
    }

    @Test
    public void testRunSETWithoutSemicolonReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("SET a = 'b'");
                assertResults( statement, hasResult, Result.ZERO );
            }
        });
    }

    @Test
    public void testRunBEGINWithSemicolonReturnsNextQueryResultOnly() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("BEGIN; select 2");
                assertResults( statement, hasResult, Result.ZERO, result(row(2L)) );
            }
        });
    }

    @Test
    public void testRunBEGINWithoutSemicolonReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("BEGIN");
                assertResults( statement, hasResult, Result.ZERO );
            }
        });
    }

    @Test
    public void testRunCOMMITWithSemicolonReturnsNextQueryResultOnly() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("COMMIT; select 3");
                assertResults( statement, hasResult, Result.ZERO, result(row(3L)) );
            }
        });
    }

    @Test
    public void testRunCOMMITWithoutSemicolonReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("COMMIT TRANSACTION");
                assertResults( statement, hasResult, Result.ZERO );
            }
        });
    }

    @Test
    public void testRunROLLBACKWithSemicolonReturnsNextQueryResultOnly() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("COMMIT TRANSACTION; select 4");
                assertResults( statement, hasResult, Result.ZERO, result(row(4L)) );
            }
        });
    }

    @Test
    public void testRunROLLBACKWithoutSemicolonReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("COMMIT");
                assertResults( statement, hasResult, Result.ZERO );
            }
        });
    }

    @Test
    public void testRunDISCARDWithSemicolonReturnsNextQueryResultOnly() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("DISCARD ALL; select 5");
                assertResults( statement, hasResult, Result.ZERO, result(row(5L)) );
            }
        });
    }

    @Test
    public void testRunDISCARDWithoutSemicolonReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute("DISCARD");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("DISCARD;");
                assertResults( statement, hasResult, Result.ZERO );
                
                hasResult = statement.execute("DISCARD ALL");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("DISCARD PLANS");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("DISCARD SEQUENCES");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("DISCARD TEMPORARY");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("DISCARD TEMP");
                assertResults( statement, hasResult, Result.ZERO );
            }
        });
    }

    @Test
    public void testRunCLOSEWithSemicolonReturnsNextQueryResultOnly() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("CLOSE ALL; select 6");
                assertResults( statement, hasResult, Result.ZERO, result(row(6L)) );

                hasResult = statement.execute("CLOSE; select 7");
                assertResults( statement, hasResult, Result.ZERO, result(row(7L)) );
            }
        });
    }

    @Test
    public void testRunCLOSEWithoutSemicolonReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute("CLOSE");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("CLOSE;");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("CLOSE ALL");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("CLOSE ALL;");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("CLOSE XYZ");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("CLOSE XYZ;");
                assertResults( statement, hasResult, Result.ZERO );
            }
        });
    }

    @Test
    public void testRunUNLISTENWithSemicolonReturnsNextQueryResultOnly() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("UNLISTEN channel_name; select 8");
                assertResults( statement, hasResult, Result.ZERO, result(row(8L)) );

                hasResult = statement.execute("UNLISTEN *; select 9");
                assertResults( statement, hasResult, Result.ZERO, result(row(9L)) );
            }
        });
    }

    @Test
    public void testRunUNLISTENWithoutSemicolonReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute("UNLISTEN some_channel");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("UNLISTEN some_channel;");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("UNLISTEN *");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("UNLISTEN *;");
                assertResults( statement, hasResult, Result.ZERO );
            }
        });
    }

    @Test
    public void testRunRESETWithSemicolonReturnsNextQueryResultOnly() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                boolean hasResult = statement.execute("RESET configuration_parameter; select 10");
                assertResults( statement, hasResult, Result.ZERO, result(row(10L)) );

                hasResult = statement.execute("RESET ALL; select 11");
                assertResults( statement, hasResult, Result.ZERO, result(row(11L)) );
            }
        });
    }

    @Test
    public void testRunRESETWithoutSemicolonReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute("RESET config_param");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("RESET config_param;");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("RESET ALL");
                assertResults( statement, hasResult, Result.ZERO );

                hasResult = statement.execute("RESET ALL;");
                assertResults( statement, hasResult, Result.ZERO );
            }
        });
    }
    
    @Test
    public void testRunBlockWithCreateInsertSelectDropReturnsSelectResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE TEST(l long, s string); " +
                        "INSERT INTO TEST VALUES (3, 'three'); " +
                        "SELECT * from TEST;" +
                        "DROP TABLE TEST;");
                assertResults( statement, hasResult, result(0), result(1), 
                                                     result(row(3L, "three")), result(0));
            }
        });
    }

    @Test
    public void testRunBlockWithCreateInsertTruncateSelectReturnsNoResult() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE TEST(l long, s string); " +
                                "INSERT INTO TEST VALUES (3, 'three'); /*some comment */" +
                                "TRUNCATE TABLE TEST;" +
                                "SELECT '1';");
                assertResults( statement, hasResult, result(0), result(1), 
                                                     result(0), result(row("1")));
            }
        });
    }
    
    @Test
    public void testShowTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "SHOW TABLES; SELECT '15';");
                assertResults( statement, hasResult, Result.EMPTY, result(row(15L)) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterTableAddColumnSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE TEST(l long); INSERT INTO TEST VALUES(1); ALTER TABLE TEST ADD COLUMN s STRING; SELECT * from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, result(row(1L, null)) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterTableDropColumnSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                "CREATE TABLE TEST(l long, de string); INSERT INTO TEST VALUES(1,'a'); ALTER TABLE TEST DROP COLUMN de; SELECT * from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, result(row(1L)) );
            }
        });
    }
    
    @Test
    public void testCreateInsertAlterTableRenameColumnSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE TEST(l long, de string); INSERT INTO TEST VALUES(2,'b'); ALTER TABLE TEST RENAME COLUMN de TO s; SELECT l,s from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, result(row(2L, "b")) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterTableSetSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE TEST(l long, de string); INSERT INTO TEST VALUES(3,'c'); " +
                            "ALTER TABLE TEST set param maxUncommittedRows = 150; SELECT l,de from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, result(row(3L, "c")) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterTableAddIndexSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long, s symbol); INSERT INTO test VALUES(4,'d'); " +
                            "ALTER TABLE test ALTER COLUMN s ADD INDEX; SELECT l,s from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, result(row(4L, "d")) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterTableAlterColumnCacheSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long, s symbol); INSERT INTO test VALUES(5,'e'); " +
                                "ALTER TABLE test ALTER COLUMN s cache; SELECT l,s from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, result(row(5L, "e")) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterTableAlterColumnNoCacheSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long, s symbol); INSERT INTO test VALUES(6,'f'); " +
                                "ALTER TABLE test ALTER COLUMN s nocache; SELECT l,s from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, result(row(6L, "f")) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterSystemLockAndUnlockWriterSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long, s symbol); INSERT INTO test VALUES(7,'g'); " +
                            "ALTER SYSTEM LOCK WRITER test; ALTER SYSTEM UNLOCK WRITER test; SELECT l,s from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, Result.ZERO, result(row(7L, "g")) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterTableDropPartitionListSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long,ts timestamp) timestamp(ts) partition by year; " +
                            "INSERT INTO test VALUES(1970, 0); " +
                            "INSERT INTO test VALUES(2020, to_timestamp('2020-03-01', 'yyyy-MM-dd'));" +
                            "ALTER TABLE test DROP PARTITION LIST '1970'; " +
                            "SELECT l from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), result(1), Result.ZERO, result(row(2020L)) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterTableDropPartitionList2SelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long,ts timestamp) timestamp(ts) partition by year; " +
                            "INSERT INTO test VALUES(1970, 0); " +
                            "INSERT INTO test VALUES(2020, to_timestamp('2020-03-01', 'yyyy-MM-dd'));" +
                            "INSERT INTO test VALUES(2021, to_timestamp('2021-03-01', 'yyyy-MM-dd'));" +
                            "ALTER TABLE test DROP PARTITION LIST '1970', '2020'; " +
                            "SELECT l from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), result(1),
                        result(1), Result.ZERO, result(row(2021L)) );
            }
        });
    }

    @Test
    public void testCreateInsertAlterTableDropPartitionWhereSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long,ts timestamp) timestamp(ts) partition by year; " +
                                "INSERT INTO test VALUES(1970, 0); " +
                                "INSERT INTO test VALUES(2020, to_timestamp('2020-03-01', 'yyyy-MM-dd'));" +
                                "INSERT INTO test VALUES(2021, to_timestamp('2021-03-01', 'yyyy-MM-dd'));" +
                                "ALTER TABLE test DROP PARTITION WHERE ts <= to_timestamp('2020', 'yyyy'); " +
                                "SELECT l from TEST;");
                assertResults( statement, hasResult, Result.ZERO, result(1), result(1),
                                                                  result(1), Result.ZERO, result(row(2021L)) );
            }
        });
    }

    @Test//this test confirms that command is parsed and executed properly 
    public void testCreateInsertAlterTableAttachPartitionListAndSelectFromTableInBlockFails() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                try {
                    test.statement.execute(
                "CREATE TABLE test(l long,ts timestamp) timestamp(ts) partition by year; " +
                    "INSERT INTO test VALUES(1970, 0); " +
                    "INSERT INTO test VALUES(2020, to_timestamp('2020-03-01', 'yyyy-MM-dd'));" +
                    "ALTER TABLE test ATTACH PARTITION LIST '2020';" +
                    "SELECT l from TEST;");
                    fail("PSQLException should be thrown");
                }
                catch (PSQLException e){
                    assertEquals("ERROR: failed to attach partition '2020', partition already attached to the table\n  Position: 203", e.getMessage());
                }
            }
        });
    }
    
    @Test
    public void testCreateInsertRenameTableSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long,ts timestamp); " +
                            "INSERT INTO test VALUES(1989, 0); " +
                            "RENAME TABLE test TO newtest; " +
                            "SELECT l from newtest;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, result(row(1989L)) );
            }
        });
    }

    @Test
    public void testCreateInsertDropTableSelectFromTableInBlockThrowsErrorBecauseTableDoesntExist() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;
                statement.execute(
                        "CREATE TABLE testX(l long,ts timestamp); " +
                            "INSERT INTO testX VALUES(1990, 0); " +
                            "DROP TABLE testX;" +
                            "SELECT l from testX;");
            }
            catch (PSQLException e){
                assertEquals("ERROR: table does not exist [name=testX]\n  Position: 108", e.getMessage());
            }
        });
    }

    @Test
    public void testCreateInsertRepairTableSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long,ts timestamp); " +
                            "INSERT INTO test VALUES(1989, 0); " +
                            "REPAIR TABLE test; " +
                            "SELECT l from test;");
                assertResults( statement, hasResult, Result.ZERO, result(1), Result.ZERO, result(row(1989L)) );
            }
        });
    }

    @Test
    public void testCreateMultiInsertSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long,s string); " +
                            "INSERT INTO test VALUES (1970, 'a'), (1971, 'b') ; " +
                            "SELECT l,s from test;");
                assertResults( statement, hasResult, Result.ZERO, result(2), result(row(1970L, "a"), row(1971L, "b")) );
            }
        });
    }

    @Test
    public void testCreateInsertAsSelectAndSelectFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long,s string); " + 
                            "INSERT INTO test VALUES (20, 'z'); " +
                            "INSERT INTO test select l,s from test; " +
                            "SELECT l,s from test;");
                assertResults( statement, hasResult, Result.ZERO, result(1), result(0), /*this is wrong, qdb doesn't report row count for insert as select !*/
                                                     result(row(20L, "z"), row(20L, "z")) );
            }
        });
    }

    @Test
    public void testCreateInsertSelectWithFromTableInBlock() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                Statement statement = test.statement;

                boolean hasResult = statement.execute(
                        "CREATE TABLE test(l long,s string); " +
                            "INSERT INTO test VALUES (20, 'z'), (20, 'z'); " +
                            "WITH x AS (SELECT DISTINCT l,s FROM test) SELECT l,s from x; ");
                assertResults( statement, hasResult, Result.ZERO, result(2), result(row(20L, "z")) );
            }
        });
    }

    @Test //running statements in block should create implicit transaction so first insert should be rolled back
    public void testCreateInsertThenErrorRollsBackFirstInsertAsPartOfImplicitTransaction() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                test.connection.setAutoCommit(false);
                Statement statement = test.statement;

                try {
                    statement.execute(
                            "CREATE TABLE test(l long,s string); " +
                                "INSERT INTO test VALUES (20, 'z'); " +
                                "DELETE FROM test; " +
                                "INSERT INTO test VALUES (20, 'z');");
                }
                catch (PSQLException e){
                    assertEquals("ERROR: unexpected token: test\n  Position: 84", e.getMessage());
                }

                boolean hasResult = statement.execute("select * from test; ");
                assertResults( statement, hasResult, Result.EMPTY );
            }
        });
    }

    @Test //explicit transaction + commit
    public void testBeginCreateInsertCommitThenErrorDoesntRollBackCommittedFirstInsert() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
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
                }
                catch (PSQLException e){
                    assertEquals("ERROR: unexpected token: test\n  Position: 99", e.getMessage());
                }

                boolean hasResult = statement.execute("select * from test; ");
                assertResults( statement, hasResult, result(row(20L, "z")) );
            }
        });
    }

    @Test //implicit transaction + commit
    public void testCreateInsertCommitThenErrorDoesntRollBackCommittedFirstInsert() throws Exception {
        assertMemoryLeak(() -> {
            try ( PGTestSetup test = new PGTestSetup() ) {
                test.connection.setAutoCommit(false);
                test.connection.beginRequest();
                Statement statement = test.statement;

                try {
                    statement.execute( "CREATE TABLE test(l long,s string); " +
                                        "INSERT INTO test VALUES (19, 'k'); " +
                                        "COMMIT; " +
                                        "DELETE FROM test; " +
                                        "INSERT INTO test VALUES (21, 'x');");
                    fail("PSQLException should be thrown");
                }
                catch (PSQLException e){
                    assertEquals("ERROR: unexpected token: test\n  Position: 92", e.getMessage());
                }

                boolean hasResult = statement.execute("select * from test; ");
                assertResults( statement, hasResult, result(row(19L, "k")) );
            }
        });
    }
    
     class PGTestSetup implements Closeable {
        final PGWireServer server;
        final Connection connection;
        final Statement statement;

        PGTestSetup() throws SQLException {
            server = createPGServer(2);
            connection = getConnection(true, true);
            statement = connection.createStatement();
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
            
            server.close();
        }
    }

    static class Row {
        Object[] cols;

        Row(Object... cols) {
            this.cols = cols;
        }

        int length() {
            return cols.length;
        }

        Object get(int i) {
            return cols[i];
        }
    }
    
    static class Result {
        //jdbc result with empty result set
        static final Result EMPTY = new Result();
        //jdbc result with no result set and update count = 0
        static final Result ZERO = new Result(0);
        
        Row[] rows;
        int updateCount;
        
        Result(Row... rows){
            this.rows = rows;
        }

        Result(int updateCount){
            this.updateCount = updateCount; 
        }
        
        boolean hasData(){
            return rows != null; 
        }
    }

    static Row row(Object... cols){
        return new Row(cols);
    }

    static Result result(Row... rows){
        return new Result(rows);
    }

    static Result result(int updatedRows){
        return new Result(updatedRows);
    }
    
    private static void assertResults( Statement s, boolean hasFirstResult, Result... results ) throws SQLException {
        if ( results != null && results.length > 0 ){
            
            if ( results[0] != null ){
                try {
                    if (results[0].hasData()) {
                        assertTrue( "Expected data in first result", hasFirstResult);
                        assertResultSet(s, results[0].rows);
                    } else {
                        assertFalse("Didn't expect data in first result", hasFirstResult);
                        assertEquals(results[0].updateCount, s.getUpdateCount());
                    }
                }
                catch (AssertionError ae){
                    throw new AssertionError("Error asserting result#0 " + ae.getMessage(), ae);
                }
            }
            
            for ( int i=1; i<results.length; i++){
                try {         
                    if ( results[i].hasData() ){
                        assertTrue("expected data in result", s.getMoreResults());
                        assertResultSet(s, results[i].rows);
                    }
                    else {
                        assertFalse("didn't expect data in first result", s.getMoreResults());
                        assertEquals("Expected update count",results[i].updateCount,  s.getUpdateCount());
                    }
                }catch(AssertionError ae){
                    throw new AssertionError("Error asserting result#" + i + " " + ae.getMessage(), ae);
                }
            }
        }
        
        //check there are no more results
        assertFalse( s.getMoreResults() );
        assertEquals(-1, s.getUpdateCount() );
    }

    private static void assertResultSet(Statement s, Row[] rows) throws SQLException {
        ResultSet set = s.getResultSet();

        for (int rownum = 0; rownum < rows.length; rownum++) {
            Row row = rows[rownum];
            assertTrue("result set should have row #" + rownum, set.next());

            for (int colnum = 0; colnum < row.length(); colnum++) {
                Object col = row.get(colnum);
                try {
                    if (col instanceof String) {
                        assertEquals(col, set.getString(colnum + 1));
                    } else if (col instanceof Long) {
                        assertEquals(col, set.getLong(colnum + 1));
                    } else {
                        assertEquals(col, set.getObject(colnum + 1));
                    }
                }catch(AssertionError ae){
                    throw new AssertionError("row#" + rownum + " col#" + colnum + " " + ae.getMessage());
                }
            }
        }
        
        assertFalse( "No more rows expected!", set.next() );
    }
}
