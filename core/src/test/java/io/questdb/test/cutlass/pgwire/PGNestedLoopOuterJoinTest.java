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

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * Prepared statements over nested-loop outer joins, which the server compiles once and runs again for each
 * execution. A client with a fetch size reads the first rows of a nested-loop outer join, closes the result set
 * and runs the prepared statement again, so an execution can read a new cursor of a factory whose previous
 * cursor stopped early. The first row the join reads on its outer side matches nothing: m = 1 for LEFT and FULL,
 * s = 9 for RIGHT. A nested-loop LEFT join can also run after a RIGHT or FULL join and read it through a gate on
 * a bind variable, which the first execution of the prepared statement finds false.
 */
public class PGNestedLoopOuterJoinTest extends BasePGTest {

    @Test
    public void testExecuteAgainAfterPartialFetch() throws Exception {
        // a simple query protocol client reads every row before it returns the first one, so only the
        // extended protocol stops the cursor early
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE m (x INT)");
                statement.execute("CREATE TABLE s (y INT)");
                statement.execute("INSERT INTO m VALUES (1), (4), (3), (2)");
                statement.execute("INSERT INTO s VALUES (9), (2), (3)");
            }
            // the client fetches rows in batches only inside a transaction
            connection.setAutoCommit(false);
            assertExecuteAgainAfterPartialFetch(connection, "LEFT", """
                    1,null
                    4,2
                    4,3
                    3,2
                    2,null
                    """);
            assertExecuteAgainAfterPartialFetch(connection, "RIGHT", """
                    null,9
                    4,2
                    3,2
                    4,3
                    """);
            assertExecuteAgainAfterPartialFetch(connection, "FULL", """
                    1,null
                    4,2
                    4,3
                    3,2
                    2,null
                    null,9
                    """);
        });
    }

    @Test
    public void testKeylessLeftJoinAfterOuterJoinUnderBindVariableGate() throws Exception {
        // The LEFT JOIN to b runs after the FULL JOIN to c, which the query writes after it, and checks the outer join
        // for each of its rows, see OuterJoinNullCheck. The WHERE names a.x and folds to ? > 0, a gate over the FULL
        // join, which is false for 0 and NULL. The first execution with a false gate failed with "outer join cursor
        // is not open". In the second statement, the gate sits in the ON clause of the INNER JOIN to c2, below a RIGHT
        // JOIN to c3, which returns each c3 row with NULL in every other table while the gate is false. b row 4 has a
        // NULL y, and a.x >= b.y is true when both are NULL.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE a (k INT, x INT)");
                statement.execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
                statement.execute("CREATE TABLE b (id INT, y INT)");
                statement.execute("INSERT INTO b VALUES (1, 5), (2, 15), (3, 25), (4, NULL)");
                statement.execute("CREATE TABLE c (k INT, v INT)");
                statement.execute("INSERT INTO c VALUES (1, 100), (4, 400)");
            }
            final String header = "k[INTEGER],x[INTEGER],id[INTEGER],ck[INTEGER],v[INTEGER]\n";
            final String rows = """
                    null,null,null,4,400
                    1,10,1,1,100
                    2,20,1,null,null
                    2,20,2,null,null
                    3,30,1,null,null
                    3,30,2,null,null
                    3,30,3,null,null
                    """;
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT a.k, a.x, b.id, c.k ck, c.v FROM a LEFT JOIN b ON a.x >= b.y FULL JOIN c ON a.k = c.k"
                            + " WHERE (a.x > 0 AND false) OR ? > 0 ORDER BY k, id, ck"
            )) {
                for (int i = 0; i < 2; i++) {
                    statement.setInt(1, 0);
                    assertRows(statement, "FULL JOIN, ? = 0, round " + i, header);
                    statement.setInt(1, 1);
                    assertRows(statement, "FULL JOIN, ? = 1, round " + i, header + rows);
                    statement.setInt(1, 0);
                    assertRows(statement, "FULL JOIN, ? = 0 after 1, round " + i, header);
                    statement.setNull(1, Types.INTEGER);
                    assertRows(statement, "FULL JOIN, ? = NULL, round " + i, header);
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT a.k, b.id, c.k ck, c2.k c2k, c3.k c3k FROM a LEFT JOIN b ON a.x >= b.y FULL JOIN c ON a.k = c.k"
                            + " JOIN c c2 ON c2.k = c.k AND ((a.x > 0 AND false) OR ? > 0) RIGHT JOIN c c3 ON c3.k = c2.k ORDER BY c3k"
            )) {
                final String trailingHeader = "k[INTEGER],id[INTEGER],ck[INTEGER],c2k[INTEGER],c3k[INTEGER]\n";
                statement.setInt(1, 0);
                assertRows(statement, "RIGHT JOIN after the gate, ? = 0", trailingHeader + """
                        null,null,null,null,1
                        null,null,null,null,4
                        """);
                statement.setInt(1, 1);
                assertRows(statement, "RIGHT JOIN after the gate, ? = 1", trailingHeader + """
                        1,1,1,1,1
                        null,null,4,4,4
                        """);
            }
        });
    }

    // For each k up to the row count, fetches the first k rows one at a time and closes the result set, then
    // reads every row of the next execution of the same prepared statement.
    private static void assertExecuteAgainAfterPartialFetch(Connection connection, String joinType, String expectedRows) throws SQLException {
        final String[] rows = expectedRows.split("\n");
        try (PreparedStatement statement = connection.prepareStatement("SELECT m.x, s.y FROM m " + joinType + " JOIN s ON m.x > s.y")) {
            statement.setFetchSize(1);
            for (int k = 1; k <= rows.length; k++) {
                try (ResultSet rs = statement.executeQuery()) {
                    for (int i = 0; i < k; i++) {
                        final String message = joinType + " JOIN, row " + (i + 1) + " of a fetch of " + k + " rows";
                        Assert.assertTrue(message, rs.next());
                        Assert.assertEquals(message, rows[i], rs.getString(1) + ',' + rs.getString(2));
                    }
                }
                try (ResultSet rs = statement.executeQuery()) {
                    assertResultSet(joinType + " JOIN, after a fetch of " + k + " rows", "x[INTEGER],y[INTEGER]\n" + expectedRows, sink, rs);
                }
            }
        }
    }

    private static void assertRows(PreparedStatement statement, String message, String expected) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            assertResultSet(message, expected, sink, rs);
        }
    }
}
