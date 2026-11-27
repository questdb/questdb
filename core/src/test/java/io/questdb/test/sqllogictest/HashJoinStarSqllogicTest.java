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

package io.questdb.test.sqllogictest;

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;

/**
 * Runs {@code sql/hash_join_star.test}, the INNER and LEFT star joins in DuckDB semantics, on the
 * server that every sqllogictest file runs on, and checks over the same PostgreSQL wire connection
 * that this server plans star joins on the parallel hash join. The file's rows are what the
 * ordinary plan returns as well, so they alone cannot tell that the operator ran.
 */
public class HashJoinStarSqllogicTest extends AbstractSqllogicTestRunner {

    public HashJoinStarSqllogicTest(String testFile) {
        super(testFile);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> files() {
        return Collections.singletonList(new Object[]{"/sql/hash_join_star.test"});
    }

    @Test
    public void testServerPlansStarJoinsOnTheParallelHashJoin() throws SQLException {
        try (
                Connection connection = getConnection("admin", "quest", getPgPort());
                Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE TABLE f (k INT, j INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            statement.execute("INSERT INTO f SELECT x::INT, (x % 3)::INT, 's' || (x % 2), x::TIMESTAMP FROM long_sequence(20)");
            statement.execute("CREATE TABLE d1 (k INT, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts)");
            statement.execute("INSERT INTO d1 SELECT x::INT, 'v' || x, x::TIMESTAMP FROM long_sequence(10)");
            statement.execute("CREATE TABLE d2 (j INT, w INT, ts TIMESTAMP) TIMESTAMP(ts)");
            statement.execute("INSERT INTO d2 SELECT x::INT, x::INT, x::TIMESTAMP FROM long_sequence(5)");
            statement.execute("CREATE TABLE d3 (s SYMBOL, u INT, ts TIMESTAMP) TIMESTAMP(ts)");
            statement.execute("INSERT INTO d3 SELECT 's' || x, x::INT, x::TIMESTAMP FROM long_sequence(2)");

            // A star of an INNER join and two LEFT ones, the last one with a post-join filter: one
            // operator of three stages, whose first join may swap its inputs.
            final String plan = explain(
                    statement,
                    "SELECT f.k, d1.v, d2.w, d3.u FROM f JOIN d1 ON f.k = d1.k LEFT JOIN d2 ON f.j = d2.j"
                            + " LEFT JOIN d3 ON f.s = d3.s WHERE d3.u IS NULL OR d3.u < d2.w"
            );
            TestUtils.assertContains(plan, "Async Hash Join workers: ");
            TestUtils.assertContains(plan, "stages: 3");
            TestUtils.assertContains(plan, "inputSwap: when the left input is smaller");
            TestUtils.assertContains(plan, "postJoinFilter: ");
            Assert.assertFalse(plan, plan.contains("Hash Join Light") || plan.contains("Hash Left Outer Join"));
        }
    }

    private static String explain(Statement statement, String sql) throws SQLException {
        final StringBuilder plan = new StringBuilder();
        try (ResultSet rows = statement.executeQuery("EXPLAIN " + sql)) {
            while (rows.next()) {
                plan.append(rows.getString(1)).append('\n');
            }
        }
        return plan.toString();
    }
}
