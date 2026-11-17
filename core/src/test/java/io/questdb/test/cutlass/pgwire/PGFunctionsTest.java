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

import io.questdb.std.str.StringSink;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static io.questdb.cairo.sql.SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK;

public class PGFunctionsTest extends BasePGTest {
    private static final String ALL_NULL_CASE = "(CASE WHEN x > 0 THEN NULL END)";

    @Test
    public void testAllNullCaseCastPreparedTwice() throws Exception {
        // The all-NULL CASE frees its condition, bind variable included, when it folds at compile
        // time; the prepared statement still serves NULL rows for a new bind value.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createAllNullCaseTable(connection);
            final String nullRows = """
                    x[INTEGER],v[INTEGER]
                    1,null
                    -1,null
                    null,null
                    3,null
                    """;
            try (PreparedStatement ps = connection.prepareStatement("SELECT x, (CASE WHEN x > ? THEN NULL END)::int v FROM t")) {
                ps.setInt(1, 0);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(nullRows, sink, rs);
                }
                ps.setInt(1, 5);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(nullRows, sink, rs);
                }
            }
        });
    }

    @Test
    public void testAllNullCaseWithBoundOperandPreparedTwice() throws Exception {
        // The parent's other operand is a bind variable, bound to a value and then to NULL. The
        // reference spells the CASE as the NULL literal.
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            createAllNullCaseTable(connection);
            final String sql = "SELECT x, %1$s + ? a, ? - %1$s b, %1$s = ? c FROM t";
            final StringSink expected = new StringSink();
            try (
                    PreparedStatement reference = connection.prepareStatement(String.format(sql, "NULL"));
                    PreparedStatement actual = connection.prepareStatement(String.format(sql, ALL_NULL_CASE));
                    PreparedStatement added = connection.prepareStatement("SELECT x, " + ALL_NULL_CASE + " + ? a FROM t")
            ) {
                for (int i = 0; i < 2; i++) {
                    for (int parameter = 1; parameter <= 3; parameter++) {
                        if (i == 0) {
                            reference.setInt(parameter, 5);
                            actual.setInt(parameter, 5);
                        } else {
                            reference.setNull(parameter, Types.INTEGER);
                            actual.setNull(parameter, Types.INTEGER);
                        }
                    }
                    try (ResultSet rs = reference.executeQuery()) {
                        expected.clear();
                        printToSink(expected, rs, null);
                    }
                    try (ResultSet rs = actual.executeQuery()) {
                        assertResultSet(expected, sink, rs);
                    }
                    if (i == 0) {
                        added.setInt(1, 5);
                    } else {
                        added.setNull(1, Types.INTEGER);
                    }
                    try (ResultSet rs = added.executeQuery()) {
                        assertResultSet("""
                                x[INTEGER],a[INTEGER]
                                1,null
                                -1,null
                                null,null
                                3,null
                                """, sink, rs);
                    }
                }
            }
        });
    }

    @Test
    public void testListTablesDoesntLeakMetaFds() throws Exception {
        maxQueryTime = TIMEOUT_FAIL_ON_FIRST_CHECK;
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            try (CallableStatement st1 = connection.prepareCall("create table a (i int)")) {
                st1.execute();
            }
            sink.clear();
            long openFilesBefore = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();
            // tables() honors the circuit breaker, so with a breaker that trips on the first check
            // the listing is aborted before it acquires any metadata. It must not leak metadata FDs.
            try (PreparedStatement ps = connection.prepareStatement("select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()")) {
                ps.executeQuery();
                Assert.fail("expected the query to be aborted by the circuit breaker");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
            }
            engine.releaseAllReaders();
            long openFilesAfter = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();
            Assert.assertEquals(openFilesBefore, openFilesAfter);
        });
    }

    private static void createAllNullCaseTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            statement.execute("""
                    INSERT INTO t VALUES
                      ('2026-04-01T00:00:00.000000Z', 1),
                      ('2026-04-01T00:30:00.000000Z', -1),
                      ('2026-04-01T01:00:00.000000Z', NULL),
                      ('2026-04-01T01:30:00.000000Z', 3)""");
        }
    }
}
