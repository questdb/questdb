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

import io.questdb.std.Chars;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Prepared statements with a LIMIT bind variable over an INNER join, and over a LEFT join that a WHERE on
 * its joined table filters, see JoinScanLimitTest. The rows of t0 with c > 0 have k 0, 1 and 3, and k 3,
 * the last of them, matches no t2 row, so a filter of t0 that took a negative LIMIT read only the last rows
 * of t0 and returned too few rows. Each statement runs with LIMIT values of either sign, twice.
 */
public class PGJoinScanLimitTest extends BasePGTest {

    @Test
    public void testInnerJoinLimitBindVariable() throws Exception {
        assertJoinLimits("SELECT t0.k FROM t0 JOIN t2 ON t2.k = t0.k WHERE t0.c > 0");
    }

    @Test
    public void testLeftJoinWithWhereOnJoinedTableLimitBindVariable() throws Exception {
        assertJoinLimits("SELECT t0.k FROM t0 LEFT JOIN t2 ON t2.k = t0.k WHERE t0.c > 0 AND t2.k IS NOT NULL");
    }

    private static void assertLimit(PreparedStatement statement, int limit, String expectedRows) throws SQLException {
        statement.setInt(1, limit);
        try (ResultSet rs = statement.executeQuery()) {
            assertResultSet("k[INTEGER]\n" + expectedRows, sink, rs);
        }
    }

    private static void createTables(java.sql.Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE t0 (c INT, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            statement.execute("""
                    INSERT INTO t0 VALUES
                    (1, 0, '2024-01-05T01:00:00.000000Z'),
                    (2, 1, '2024-01-05T02:00:00.000000Z'),
                    (NULL, 2, '2024-01-05T03:00:00.000000Z'),
                    (1, 3, '2024-01-06T01:00:00.000000Z')
                    """);
            statement.execute("CREATE TABLE t2 (k INT)");
            statement.execute("INSERT INTO t2 VALUES (0), (1), (2), (9)");
        }
    }

    private void assertJoinLimits(String sql) throws Exception {
        // Shared query workers make the filter of t0 an async filter, the only filter that takes a LIMIT.
        sharedQueryWorkerCount = 2;
        try {
            assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
                createTables(connection);
                try (
                        Statement statement = connection.createStatement();
                        ResultSet plan = statement.executeQuery("EXPLAIN " + sql + " LIMIT -1")
                ) {
                    sink.clear();
                    while (plan.next()) {
                        sink.put(plan.getString(1)).put('\n');
                    }
                    TestUtils.assertContains(sink, "Async");
                    TestUtils.assertContains(sink, "filter: 0<c");
                    Assert.assertFalse(sink.toString(), Chars.contains(sink, "limit:"));
                }
                try (PreparedStatement statement = connection.prepareStatement(sql + " LIMIT ?")) {
                    for (int i = 0; i < 2; i++) {
                        assertLimit(statement, 1, "0\n");
                        assertLimit(statement, -1, "1\n");
                        assertLimit(statement, -2, "0\n1\n");
                        assertLimit(statement, 3, "0\n1\n");
                        assertLimit(statement, -3, "0\n1\n");
                    }
                }
            });
        } finally {
            sharedQueryWorkerCount = 0;
        }
    }
}
