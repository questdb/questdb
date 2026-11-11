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

import io.questdb.PropertyKey;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static io.questdb.test.tools.TestUtils.assertContains;

public class PGMemoryLimitTest extends BasePGTest {
    @Test
    public void testCachedFusedJoinRecompilesOnceItsBuildTableOutgrowsTheProbe() throws Exception {
        // The fused hash join GROUP BY runs only with shared query workers.
        sharedQueryWorkerCount = 2;
        try {
            assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
                try (Statement stat = connection.createStatement()) {
                    stat.execute("CREATE TABLE f (k INT, v LONG)");
                    stat.execute("CREATE TABLE d (k INT, g INT)");
                    stat.execute("INSERT INTO d SELECT x::INT, (x % 10)::INT FROM long_sequence(5_000)");
                    stat.execute("INSERT INTO f SELECT (x % 5_000 + 1)::INT, x FROM long_sequence(1_000)");
                }
                final String sql = "SELECT d.g, sum(f.v) v FROM f JOIN d ON f.k = d.k ORDER BY d.g";
                // f has fewer rows than d, so the fused plan builds f.
                try (Statement stat = connection.createStatement(); ResultSet plan = stat.executeQuery("EXPLAIN " + sql)) {
                    sink.clear();
                    while (plan.next()) {
                        sink.put(plan.getString(1)).put('\n');
                    }
                    assertContains(sink, "Async Hash Join Group By");
                    assertContains(sink, "inputSwapped: true");
                }
                try (PreparedStatement select = connection.prepareStatement(sql)) {
                    try (ResultSet resultSet = select.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        g[INTEGER],v[BIGINT]
                                        0,50400
                                        1,50500
                                        2,49600
                                        3,49700
                                        4,49800
                                        5,49900
                                        6,50000
                                        7,50100
                                        8,50200
                                        9,50300
                                        """,
                                sink,
                                resultSet
                        );
                    }
                    try (Statement stat = connection.createStatement()) {
                        stat.execute("INSERT INTO f SELECT (x % 5_000 + 1)::INT, x FROM long_sequence(200_000)");
                    }
                    // A build of the 201_000 rows of f takes about 7 MiB, and a build of the 5_000 rows of
                    // d about 0.2 MiB. The cached factory asks for a recompile rather than build f, and
                    // the new plan builds d within the limit.
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * Numbers.SIZE_1MB);
                    try (ResultSet resultSet = select.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        g[INTEGER],v[BIGINT]
                                        0,2000130400
                                        1,2000150500
                                        2,1999969600
                                        3,1999989700
                                        4,2000009800
                                        5,2000029900
                                        6,2000050000
                                        7,2000070100
                                        8,2000090200
                                        9,2000110300
                                        """,
                                sink,
                                resultSet
                        );
                    } finally {
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                    }
                }
            });
        } finally {
            sharedQueryWorkerCount = 0;
        }
    }

    @Test
    public void testGroupingSetsCastToSymbolKeyReleasesQueryMemory() throws Exception {
        // ROLLUP evaluates its cast-to-SYMBOL key itself, and the key's dictionary is charged to the
        // query's memory tracker. Each run of the prepared statement must give that memory back when
        // its cursor closes: a tracker that goes back to the pool charged fails the next statement.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement stat = connection.createStatement()) {
                stat.execute("CREATE TABLE t AS (SELECT ('k' || (x % 3))::VARCHAR v, ('s' || (x % 2))::SYMBOL s FROM long_sequence(12))");
            }
            final String expected = """
                    k[VARCHAR],s[VARCHAR],c[BIGINT]
                    k0,null,4
                    k0,s0,2
                    k0,s1,2
                    k1,null,4
                    k1,s0,2
                    k1,s1,2
                    k2,null,4
                    k2,s0,2
                    k2,s1,2
                    """;
            try (PreparedStatement select = connection.prepareStatement("SELECT cast(v AS SYMBOL) k, s, count() c FROM t GROUP BY k, ROLLUP(s) ORDER BY k, s")) {
                for (int run = 0; run < 2; run++) {
                    try (ResultSet resultSet = select.executeQuery()) {
                        sink.clear();
                        assertResultSet(expected, sink, resultSet);
                    }
                }
            }
            try (Statement stat = connection.createStatement(); ResultSet resultSet = stat.executeQuery("SELECT count() FROM t")) {
                sink.clear();
                assertResultSet("""
                        count[BIGINT]
                        12
                        """, sink, resultSet);
            }
        });
    }

    @Test
    public void testSimpleInsertCastToSymbolDuringOpenSelectReleasesQueryMemory() throws Exception {
        // A simple-protocol INSERT runs while a SELECT on the same connection still holds its query
        // memory tracker, so the INSERT's cast-to-SYMBOL dictionary is charged to that tracker. The
        // INSERT must free the dictionary when it finishes: otherwise the SELECT gives its tracker
        // back charged, and the server drops the tracker and leaks its native block.
        // A single-choice rnd_varchar() keeps the cast from folding to a constant.
        final String insert = "INSERT INTO t VALUES (rnd_varchar('a', 'a')::SYMBOL)";
        // pgjdbc sends prepared statements over the extended protocol and plain ones as simple
        // queries; with auto-commit off, a fetch size suspends the SELECT's portal mid-result
        assertWithPgServer(Mode.EXTENDED_FOR_PREPARED, false, -1, (connection, binary, mode, port) -> {
            try (Statement stat = connection.createStatement()) {
                stat.execute("CREATE TABLE t (k SYMBOL)");
            }
            connection.setAutoCommit(false);
            try (PreparedStatement select = connection.prepareStatement("SELECT x FROM long_sequence(20)")) {
                select.setFetchSize(5);
                try (ResultSet resultSet = select.executeQuery()) {
                    for (int i = 1; i <= 3; i++) {
                        Assert.assertTrue(resultSet.next());
                        Assert.assertEquals(i, resultSet.getLong(1));
                    }
                    try (Statement stat = connection.createStatement()) {
                        for (int i = 0; i < 3; i++) {
                            Assert.assertEquals(1, stat.executeUpdate(insert));
                        }
                    }
                    // closing the result set closes the suspended portal, and the SELECT gives
                    // its tracker back
                }
            }
            connection.commit();
            connection.setAutoCommit(true);
            assertTableRows(connection, 3);
        });
        // a simple query with two statements runs the INSERT before the SELECT's cursor closes
        assertWithPgServer(Mode.SIMPLE, false, -1, (connection, binary, mode, port) -> {
            try (Statement stat = connection.createStatement()) {
                stat.execute("CREATE TABLE t (k SYMBOL)");
                for (int i = 0; i < 3; i++) {
                    stat.execute("SELECT 1; " + insert);
                }
            }
            assertTableRows(connection, 3);
        });
    }

    @Test
    public void testUpdateRecoversFromOomError() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement stat = connection.createStatement()) {
                stat.execute(
                        "create table up as" +
                                " (select timestamp_sequence(0, 1000000) ts," +
                                " x" +
                                " from long_sequence(5))" +
                                " timestamp(ts) partition by DAY;"
                );
                stat.execute(
                        "create table down as" +
                                " (select timestamp_sequence(0, 1000000) ts," +
                                " x * 100 as y" +
                                " from long_sequence(5))" +
                                " timestamp(ts) partition by DAY;"
                );
            }

            try (Statement stat = connection.createStatement()) {
                // Set RSS limit, so that the UPDATE will fail with OOM.
                Unsafe.setRssMemLimit(8_900_000);
                try {
                    stat.execute(
                            "UPDATE up SET x = y" +
                                    " FROM down " +
                                    " WHERE up.ts = down.ts and x < 4"
                    );
                    Assert.fail();
                } catch (PSQLException e) {
                    assertContains(e.getMessage(), "global RSS memory limit exceeded");
                }

                // Remove the limit and verify that the update succeeds.
                Unsafe.setRssMemLimit(0);
                stat.execute(
                        "UPDATE up SET x = y" +
                                " FROM down " +
                                " WHERE up.ts = down.ts and x < 4"
                );
                drainWalQueue();
            }
            final String expected = "ts[TIMESTAMP],x[BIGINT]\n" +
                    "1970-01-01 00:00:00.0,100\n" +
                    "1970-01-01 00:00:01.0,200\n" +
                    "1970-01-01 00:00:02.0,300\n" +
                    "1970-01-01 00:00:03.0,4\n" +
                    "1970-01-01 00:00:04.0,5\n";
            try (ResultSet resultSet = connection.prepareStatement("up").executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }
        });
    }

    @Test
    public void testUpdateRecoversFromOomErrorBindVars() throws Exception {
        // the reason "simple" is excluded is dues to PG driver sending
        // update x = '20', where x is long and '20' is, well, string. We don't support such conversion at
        // the update level yet
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement stat = connection.createStatement()) {
                stat.execute(
                        "create table up as" +
                                " (select timestamp_sequence(0, 1000000) ts," +
                                " x" +
                                " from long_sequence(5))" +
                                " timestamp(ts) partition by DAY;"
                );
                stat.execute(
                        "create table down as" +
                                " (select timestamp_sequence(0, 1000000) ts," +
                                " x * 100 as y" +
                                " from long_sequence(5))" +
                                " timestamp(ts) partition by DAY;"
                );
            }

            Unsafe.setRssMemLimit(8_900_000);
            try (
                    PreparedStatement ps = connection.prepareStatement("UPDATE up SET x = ?" +
                            " FROM down " +
                            " WHERE up.ts = down.ts and x < 4")
            ) {
                // Set RSS limit, so that the UPDATE will fail with OOM.
                try {
                    ps.setInt(1, 10);
                    ps.execute();
                    Assert.fail();
                } catch (PSQLException e) {
                    assertContains(e.getMessage(), "global RSS memory limit exceeded");
                }

                // Remove the limit and verify that the update succeeds.
                Unsafe.setRssMemLimit(0);
                ps.setInt(1, 20);
                ps.execute();
            }

            final String expected = "ts[TIMESTAMP],x[BIGINT]\n" +
                    "1970-01-01 00:00:00.0,20\n" +
                    "1970-01-01 00:00:01.0,20\n" +
                    "1970-01-01 00:00:02.0,20\n" +
                    "1970-01-01 00:00:03.0,4\n" +
                    "1970-01-01 00:00:04.0,5\n";
            try (ResultSet resultSet = connection.prepareStatement("up").executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }
        });
    }

    private void assertTableRows(Connection connection, int rowCount) throws Exception {
        try (Statement stat = connection.createStatement(); ResultSet resultSet = stat.executeQuery("SELECT k, count() c FROM t")) {
            sink.clear();
            assertResultSet("k[VARCHAR],c[BIGINT]\n" + "a," + rowCount + "\n", sink, resultSet);
        }
    }
}
