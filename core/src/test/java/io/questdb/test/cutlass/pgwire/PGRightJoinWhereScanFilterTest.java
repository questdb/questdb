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

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * Prepared statements whose WHERE filters a table before a RIGHT join, see
 * RightJoinWhereScanFilterTest for the data. Each statement runs with a value and then with NULL on
 * the same prepared statement: a copy of a conjunct filters the scan only when another conjunct of the
 * table is false on its NULL row for every value, so the NULL execution returns the rows that the
 * post-join filter alone returns.
 */
public class PGRightJoinWhereScanFilterTest extends BasePGTest {
    private static final String HEADER = "count[BIGINT]\n";

    @Test
    public void testEqualsBindVariable() throws Exception {
        // c = ? holds for NULL = NULL: r3 three times and the NULL row of t2 row 9
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            createTables(connection);
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT count(*) FROM t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t0.k WHERE t0.c = ?"
            )) {
                for (int i = 0; i < 2; i++) {
                    statement.setInt(1, 1);
                    assertCount(statement, 3);
                    statement.setNull(1, Types.INTEGER);
                    assertCount(statement, 4);
                }
            }
        });
    }

    @Test
    public void testGreaterThanBindVariable() throws Exception {
        // ts > ? is false on a NULL ts: r2 and r3 three times each, and nothing for NULL
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            createTables(connection);
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT count(*) FROM t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t0.k WHERE t0.ts > ?"
            )) {
                for (int i = 0; i < 2; i++) {
                    statement.setString(1, "2024-01-05T01:30:00.000000Z");
                    assertCount(statement, 6);
                    statement.setNull(1, Types.VARCHAR);
                    assertCount(statement, 0);
                }
            }
        });
    }

    @Test
    public void testHalfOpenRangeBindVariables() throws Exception {
        // ts < ? is false on a NULL ts, and takes ts >= ? along: r1, r2 and r3 twice each for the day,
        // r2 and r3 twice each from 01:30, and nothing when ts >= NULL fails every real row
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            createTables(connection);
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT count(*) FROM t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t1.k WHERE t0.ts >= ? AND t0.ts < ?"
            )) {
                for (int i = 0; i < 2; i++) {
                    statement.setString(1, "2024-01-05T00:00:00.000000Z");
                    statement.setString(2, "2024-01-06T00:00:00.000000Z");
                    assertCount(statement, 6);
                    statement.setString(1, "2024-01-05T01:30:00.000000Z");
                    assertCount(statement, 4);
                    statement.setNull(1, Types.VARCHAR);
                    assertCount(statement, 0);
                }
            }
        });
    }

    private static void assertCount(PreparedStatement statement, int expectedCount) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            assertResultSet(HEADER + expectedCount + "\n", sink, rs);
        }
    }

    private static void createTables(java.sql.Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE t0 (c INT, k INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            statement.execute("""
                    INSERT INTO t0 VALUES
                    (1, 0, 'a', '2024-01-05T01:00:00.000000Z'),
                    (2, 1, 'b', '2024-01-05T02:00:00.000000Z'),
                    (NULL, 2, NULL, '2024-01-05T03:00:00.000000Z'),
                    (1, 3, 'a', '2024-01-06T01:00:00.000000Z')
                    """);
            statement.execute("CREATE TABLE t1 (k INT)");
            statement.execute("INSERT INTO t1 VALUES (0), (1), (5)");
            statement.execute("CREATE TABLE t2 (k INT)");
            statement.execute("INSERT INTO t2 VALUES (0), (1), (2), (9)");
        }
    }
}
