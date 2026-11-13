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

/**
 * A client with a fetch size reads the first rows of a nested-loop outer join, closes the result set and runs
 * the prepared statement again. The server reuses the compiled join, so an execution can read a new cursor of
 * a factory whose previous cursor stopped early. The first row the join reads on its outer side matches
 * nothing: m = 1 for LEFT and FULL, s = 9 for RIGHT.
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
}
