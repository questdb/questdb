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
 * Runs {@code sql/hash_join_symbol_key.test}, INNER and LEFT joins on a SYMBOL key whose probe reads
 * few rows of a large dictionary, on the server that every sqllogictest file runs on, and checks over
 * the same PostgreSQL wire connection that this server plans them on the parallel hash join, and the
 * GROUP BY over such a join on the fused GROUP BY. The serial plans print the SYMBOL key as well, and
 * return the same rows, so the file's rows alone cannot tell that the operators ran.
 */
public class HashJoinSymbolKeySqllogicTest extends AbstractSqllogicTestRunner {

    public HashJoinSymbolKeySqllogicTest(String testFile) {
        super(testFile);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> files() {
        return Collections.singletonList(new Object[]{"/sql/hash_join_symbol_key.test"});
    }

    @Test
    public void testServerPlansSymbolKeyJoinsOnTheOperators() throws SQLException {
        try (
                Connection connection = getConnection("admin", "quest", getPgPort());
                Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE TABLE ev (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            statement.execute("INSERT INTO ev SELECT 'pad' || x, x::INT, '2023-12-31'::TIMESTAMP + x FROM long_sequence(1000)");
            statement.execute("ALTER TABLE ev DROP PARTITION LIST '2023-12-31'");
            statement.execute("INSERT INTO ev VALUES ('a', 1, '2024-01-01T00:00:00.000000Z'), ('b', 2, '2024-01-01T01:00:00.000000Z'), (NULL, 3, '2024-01-01T02:00:00.000000Z')");
            statement.execute("CREATE TABLE dm (sym SYMBOL, w INT)");
            statement.execute("INSERT INTO dm VALUES ('c', 30), ('b', 20), (NULL, 40)");
            for (String sql : new String[]{
                    "SELECT e.v, e.sym, d.w FROM ev e JOIN dm d ON e.sym = d.sym ORDER BY e.v",
                    "SELECT e.v, e.sym, d.w FROM ev e LEFT JOIN dm d ON e.sym = d.sym ORDER BY e.v",
                    "SELECT e.v, e.sym, d.w FROM ev e LEFT JOIN dm d ON e.sym = d.sym LIMIT 2",
            }) {
                final String plan = explain(statement, sql);
                TestUtils.assertContains(plan, "Async Hash Join workers: ");
                TestUtils.assertContains(plan, "symbolKeyJoin: true");
                Assert.assertFalse(plan, plan.contains("Hash Join Light") || plan.contains("Hash Left Outer Join"));
            }
            final String plan = explain(statement, "SELECT d.w, count(*) FROM ev e JOIN dm d ON e.sym = d.sym GROUP BY d.w ORDER BY d.w");
            TestUtils.assertContains(plan, "Async Hash Join Group By");
            TestUtils.assertContains(plan, "symbolKeyJoin: true");
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
