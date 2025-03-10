/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Misc;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static io.questdb.test.tools.TestUtils.unchecked;

public class PgLimitBindVariablesTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBindVariableLimitSignChange() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration("pg.select.cache.enabled=true");
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, 100);
                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT * FROM tab LIMIT ?")) {
                        stmt.setInt(1, 1);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    "QUERY PLAN[VARCHAR]\n" +
                                            "Limit lo: $0::int[1] skip-over-rows: 0 limit: 1\n" +
                                            "    PageFrame\n" +
                                            "        Row forward scan\n" +
                                            "        Frame forward scan on: tab\n",
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                        assertLo(
                                stmt,
                                -1,
                                "QUERY PLAN[VARCHAR]\n" +
                                        "Limit lo: $0::int[-1] skip-over-rows: 99 limit: 1\n" +
                                        "    PageFrame\n" +
                                        "        Row forward scan\n" +
                                        "        Frame forward scan on: tab\n");
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab LIMIT ?")) {
                        stmt.setInt(1, 1);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                            "Sym1,1,1970-01-01 00:00:20.0\n",
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                        assertLo(
                                stmt,
                                -1,
                                "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                        "Sym0,0,1970-01-01 00:00:29.9\n"
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testLowAndHighLimit() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, 100);
                try (Connection connection = getConnection(serverMain)) {
                    final PreparedStatement stmt = connection.prepareStatement("SELECT * from tab order by ts desc");
                    final ResultSet resultSet = stmt.executeQuery();
                    assertResultSet(
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym0,0,1970-01-01 00:00:29.9\n" +
                                    "Sym9,3,1970-01-01 00:00:29.8\n" +
                                    "Sym8,2,1970-01-01 00:00:29.7\n" +
                                    "Sym7,1,1970-01-01 00:00:29.6\n" +
                                    "Sym6,0,1970-01-01 00:00:29.5\n" +
                                    "Sym5,3,1970-01-01 00:00:29.4\n" +
                                    "Sym4,2,1970-01-01 00:00:29.3\n" +
                                    "Sym3,1,1970-01-01 00:00:29.2\n" +
                                    "Sym2,0,1970-01-01 00:00:29.1\n" +
                                    "Sym1,3,1970-01-01 00:00:29.0\n" +
                                    "Sym0,2,1970-01-01 00:00:28.9\n" +
                                    "Sym9,1,1970-01-01 00:00:28.8\n" +
                                    "Sym8,0,1970-01-01 00:00:28.7\n" +
                                    "Sym7,3,1970-01-01 00:00:28.6\n" +
                                    "Sym6,2,1970-01-01 00:00:28.5\n" +
                                    "Sym5,1,1970-01-01 00:00:28.4\n" +
                                    "Sym4,0,1970-01-01 00:00:28.3\n" +
                                    "Sym3,3,1970-01-01 00:00:28.2\n" +
                                    "Sym2,2,1970-01-01 00:00:28.1\n" +
                                    "Sym1,1,1970-01-01 00:00:28.0\n" +
                                    "Sym0,0,1970-01-01 00:00:27.9\n" +
                                    "Sym9,3,1970-01-01 00:00:27.8\n" +
                                    "Sym8,2,1970-01-01 00:00:27.7\n" +
                                    "Sym7,1,1970-01-01 00:00:27.6\n" +
                                    "Sym6,0,1970-01-01 00:00:27.5\n" +
                                    "Sym5,3,1970-01-01 00:00:27.4\n" +
                                    "Sym4,2,1970-01-01 00:00:27.3\n" +
                                    "Sym3,1,1970-01-01 00:00:27.2\n" +
                                    "Sym2,0,1970-01-01 00:00:27.1\n" +
                                    "Sym1,3,1970-01-01 00:00:27.0\n" +
                                    "Sym0,2,1970-01-01 00:00:26.9\n" +
                                    "Sym9,1,1970-01-01 00:00:26.8\n" +
                                    "Sym8,0,1970-01-01 00:00:26.7\n" +
                                    "Sym7,3,1970-01-01 00:00:26.6\n" +
                                    "Sym6,2,1970-01-01 00:00:26.5\n" +
                                    "Sym5,1,1970-01-01 00:00:26.4\n" +
                                    "Sym4,0,1970-01-01 00:00:26.3\n" +
                                    "Sym3,3,1970-01-01 00:00:26.2\n" +
                                    "Sym2,2,1970-01-01 00:00:26.1\n" +
                                    "Sym1,1,1970-01-01 00:00:26.0\n" +
                                    "Sym0,0,1970-01-01 00:00:25.9\n" +
                                    "Sym9,3,1970-01-01 00:00:25.8\n" +
                                    "Sym8,2,1970-01-01 00:00:25.7\n" +
                                    "Sym7,1,1970-01-01 00:00:25.6\n" +
                                    "Sym6,0,1970-01-01 00:00:25.5\n" +
                                    "Sym5,3,1970-01-01 00:00:25.4\n" +
                                    "Sym4,2,1970-01-01 00:00:25.3\n" +
                                    "Sym3,1,1970-01-01 00:00:25.2\n" +
                                    "Sym2,0,1970-01-01 00:00:25.1\n" +
                                    "Sym1,3,1970-01-01 00:00:25.0\n" +
                                    "Sym0,2,1970-01-01 00:00:24.9\n" +
                                    "Sym9,1,1970-01-01 00:00:24.8\n" +
                                    "Sym8,0,1970-01-01 00:00:24.7\n" +
                                    "Sym7,3,1970-01-01 00:00:24.6\n" +
                                    "Sym6,2,1970-01-01 00:00:24.5\n" +
                                    "Sym5,1,1970-01-01 00:00:24.4\n" +
                                    "Sym4,0,1970-01-01 00:00:24.3\n" +
                                    "Sym3,3,1970-01-01 00:00:24.2\n" +
                                    "Sym2,2,1970-01-01 00:00:24.1\n" +
                                    "Sym1,1,1970-01-01 00:00:24.0\n" +
                                    "Sym0,0,1970-01-01 00:00:23.9\n" +
                                    "Sym9,3,1970-01-01 00:00:23.8\n" +
                                    "Sym8,2,1970-01-01 00:00:23.7\n" +
                                    "Sym7,1,1970-01-01 00:00:23.6\n" +
                                    "Sym6,0,1970-01-01 00:00:23.5\n" +
                                    "Sym5,3,1970-01-01 00:00:23.4\n" +
                                    "Sym4,2,1970-01-01 00:00:23.3\n" +
                                    "Sym3,1,1970-01-01 00:00:23.2\n" +
                                    "Sym2,0,1970-01-01 00:00:23.1\n" +
                                    "Sym1,3,1970-01-01 00:00:23.0\n" +
                                    "Sym0,2,1970-01-01 00:00:22.9\n" +
                                    "Sym9,1,1970-01-01 00:00:22.8\n" +
                                    "Sym8,0,1970-01-01 00:00:22.7\n" +
                                    "Sym7,3,1970-01-01 00:00:22.6\n" +
                                    "Sym6,2,1970-01-01 00:00:22.5\n" +
                                    "Sym5,1,1970-01-01 00:00:22.4\n" +
                                    "Sym4,0,1970-01-01 00:00:22.3\n" +
                                    "Sym3,3,1970-01-01 00:00:22.2\n" +
                                    "Sym2,2,1970-01-01 00:00:22.1\n" +
                                    "Sym1,1,1970-01-01 00:00:22.0\n" +
                                    "Sym0,0,1970-01-01 00:00:21.9\n" +
                                    "Sym9,3,1970-01-01 00:00:21.8\n" +
                                    "Sym8,2,1970-01-01 00:00:21.7\n" +
                                    "Sym7,1,1970-01-01 00:00:21.6\n" +
                                    "Sym6,0,1970-01-01 00:00:21.5\n" +
                                    "Sym5,3,1970-01-01 00:00:21.4\n" +
                                    "Sym4,2,1970-01-01 00:00:21.3\n" +
                                    "Sym3,1,1970-01-01 00:00:21.2\n" +
                                    "Sym2,0,1970-01-01 00:00:21.1\n" +
                                    "Sym1,3,1970-01-01 00:00:21.0\n" +
                                    "Sym0,2,1970-01-01 00:00:20.9\n" +
                                    "Sym9,1,1970-01-01 00:00:20.8\n" +
                                    "Sym8,0,1970-01-01 00:00:20.7\n" +
                                    "Sym7,3,1970-01-01 00:00:20.6\n" +
                                    "Sym6,2,1970-01-01 00:00:20.5\n" +
                                    "Sym5,1,1970-01-01 00:00:20.4\n" +
                                    "Sym4,0,1970-01-01 00:00:20.3\n" +
                                    "Sym3,3,1970-01-01 00:00:20.2\n" +
                                    "Sym2,2,1970-01-01 00:00:20.1\n" +
                                    "Sym1,1,1970-01-01 00:00:20.0\n",
                            Misc.getThreadLocalSink(),
                            resultSet
                    );
                    stmt.close();

                    final String sql = "SELECT * from tab where status = ? order by ts desc limit ?,?";
                    runQueryWithParams(
                            connection,
                            sql,
                            1,
                            3,
                            5,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym5,1,1970-01-01 00:00:28.4\n" +
                                    "Sym1,1,1970-01-01 00:00:28.0\n"
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            1,
                            5,
                            8,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym7,1,1970-01-01 00:00:27.6\n" +
                                    "Sym3,1,1970-01-01 00:00:27.2\n" +
                                    "Sym9,1,1970-01-01 00:00:26.8\n"
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            2,
                            4,
                            5,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym2,2,1970-01-01 00:00:28.1\n"
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            2,
                            4,
                            -20,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym2,2,1970-01-01 00:00:28.1\n"
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            2,
                            4,
                            -21,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n"
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            2,
                            4,
                            -22,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n"
                    );
                }
            }
        });
    }

    @Test
    public void testLowAndHighLimitWithCache() throws Exception {
        assertMemoryLeak(() -> {
            createDummyConfiguration("pg.select.cache.enabled=true");
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, 10000);
                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT col1, sum(status) as sum, last(ts) as last FROM tab ORDER BY 1 DESC")) {
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            // control set, we will be applying limits to this dataset essentially
                            assertResultSet(
                                    "col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]\n" +
                                            "Sym9,2000,1970-01-01 00:16:59.8\n" +
                                            "Sym8,1000,1970-01-01 00:16:59.7\n" +
                                            "Sym7,2000,1970-01-01 00:16:59.6\n" +
                                            "Sym6,1000,1970-01-01 00:16:59.5\n" +
                                            "Sym5,2000,1970-01-01 00:16:59.4\n" +
                                            "Sym4,1000,1970-01-01 00:16:59.3\n" +
                                            "Sym3,2000,1970-01-01 00:16:59.2\n" +
                                            "Sym2,1000,1970-01-01 00:16:59.1\n" +
                                            "Sym1,2000,1970-01-01 00:16:59.0\n" +
                                            "Sym0,1000,1970-01-01 00:16:59.9\n",
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT col1, sum(status) as sum, last(ts) as last FROM tab ORDER BY 1 DESC LIMIT 4,-3")) {
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    "QUERY PLAN[VARCHAR]\n" +
                                            "Limit lo: 4 hi: -3 skip-over-rows: 4 limit: 3\n" +
                                            "    Sort light\n" +
                                            "      keys: [col1 desc]\n" +
                                            "        GroupBy vectorized: false\n" +
                                            "          keys: [col1]\n" +
                                            "          values: [sum(status),last(ts)]\n" +
                                            "            PageFrame\n" +
                                            "                Row forward scan\n" +
                                            "                Frame forward scan on: tab\n",
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT col1, sum(status) as sum, last(ts) as last FROM tab ORDER BY 1 DESC LIMIT ?,?")) {
                        assertLoHi(
                                stmt,
                                1,
                                3,
                                "col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]\n" +
                                        "Sym8,1000,1970-01-01 00:16:59.7\n" +
                                        "Sym7,2000,1970-01-01 00:16:59.6\n"
                        );

                        assertLoHi(
                                stmt,
                                3,
                                1,
                                "col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]\n" +
                                        "Sym8,1000,1970-01-01 00:16:59.7\n" +
                                        "Sym7,2000,1970-01-01 00:16:59.6\n"
                        );

                        assertLoHi(
                                stmt,
                                5,
                                5,
                                "col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]\n"
                        );

                        assertLoHi(
                                stmt,
                                1,
                                2,
                                "col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]\n" +
                                        "Sym8,1000,1970-01-01 00:16:59.7\n"
                        );

                        assertLoHi(
                                stmt,
                                -2,
                                -1,
                                "col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]\n" +
                                        "Sym1,2000,1970-01-01 00:16:59.0\n"
                        );

                        assertLoHi(
                                stmt,
                                -1,
                                -2,
                                "col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]\n" +
                                        "Sym1,2000,1970-01-01 00:16:59.0\n"
                        );

                        assertLoHi(
                                stmt,
                                -3,
                                -3,
                                "col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]\n"
                        );

                        assertLoHi(
                                stmt,
                                4,
                                -3,
                                "col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]\n" +
                                        "Sym5,2000,1970-01-01 00:16:59.4\n" +
                                        "Sym4,1000,1970-01-01 00:16:59.3\n" +
                                        "Sym3,2000,1970-01-01 00:16:59.2\n"
                        );
                    }

                    // check the plan
                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT col1, sum(status) as sum, last(ts) as last FROM tab ORDER BY 2 DESC LIMIT ?,?")) {
                        assertLoHi(
                                stmt,
                                1,
                                3,
                                "QUERY PLAN[VARCHAR]\n" +
                                        "Sort light lo: $0::int hi: $1::int\n" +
                                        "  keys: [sum desc]\n" +
                                        "    GroupBy vectorized: false\n" +
                                        "      keys: [col1]\n" +
                                        "      values: [sum(status),last(ts)]\n" +
                                        "        PageFrame\n" +
                                        "            Row forward scan\n" +
                                        "            Frame forward scan on: tab\n"

                        );

                        assertLoHi(
                                stmt,
                                1,
                                2,
                                "QUERY PLAN[VARCHAR]\n" +
                                        "Sort light lo: $0::int hi: $1::int\n" +
                                        "  keys: [sum desc]\n" +
                                        "    GroupBy vectorized: false\n" +
                                        "      keys: [col1]\n" +
                                        "      values: [sum(status),last(ts)]\n" +
                                        "        PageFrame\n" +
                                        "            Row forward scan\n" +
                                        "            Frame forward scan on: tab\n"

                        );

                        assertLoHi(
                                stmt,
                                -5,
                                -1,
                                "QUERY PLAN[VARCHAR]\n" +
                                        "Sort light lo: $0::int hi: $1::int\n" +
                                        "  keys: [sum desc]\n" +
                                        "    GroupBy vectorized: false\n" +
                                        "      keys: [col1]\n" +
                                        "      values: [sum(status),last(ts)]\n" +
                                        "        PageFrame\n" +
                                        "            Row forward scan\n" +
                                        "            Frame forward scan on: tab\n"

                        );

                        assertLoHi(
                                stmt,
                                -1,
                                -5,
                                "QUERY PLAN[VARCHAR]\n" +
                                        "Sort light lo: $0::int hi: $1::int\n" +
                                        "  keys: [sum desc]\n" +
                                        "    GroupBy vectorized: false\n" +
                                        "      keys: [col1]\n" +
                                        "      values: [sum(status),last(ts)]\n" +
                                        "        PageFrame\n" +
                                        "            Row forward scan\n" +
                                        "            Frame forward scan on: tab\n"

                        );
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab ORDER BY ts DESC LIMIT ?")) {
                        assertLo(
                                stmt,
                                1,
                                "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                        "Sym0,0,1970-01-01 00:16:59.9\n"
                        );

                        assertLo(
                                stmt,
                                -1,
                                "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                        "Sym1,1,1970-01-01 00:00:20.0\n"
                        );

                        assertLo(
                                stmt,
                                -10,
                                "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                        "Sym0,2,1970-01-01 00:00:20.9\n" +
                                        "Sym9,1,1970-01-01 00:00:20.8\n" +
                                        "Sym8,0,1970-01-01 00:00:20.7\n" +
                                        "Sym7,3,1970-01-01 00:00:20.6\n" +
                                        "Sym6,2,1970-01-01 00:00:20.5\n" +
                                        "Sym5,1,1970-01-01 00:00:20.4\n" +
                                        "Sym4,0,1970-01-01 00:00:20.3\n" +
                                        "Sym3,3,1970-01-01 00:00:20.2\n" +
                                        "Sym2,2,1970-01-01 00:00:20.1\n" +
                                        "Sym1,1,1970-01-01 00:00:20.0\n"
                        );
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab ORDER BY ts DESC LIMIT ?,?")) {
                        assertLoHi(
                                stmt,
                                -1,
                                -4,
                                "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                        "Sym4,0,1970-01-01 00:00:20.3\n" +
                                        "Sym3,3,1970-01-01 00:00:20.2\n" +
                                        "Sym2,2,1970-01-01 00:00:20.1\n"
                        );
                    }

                    // explain for small limit
                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT * FROM tab ORDER BY ts desc LIMIT ?")) {

                        // same as positive constant limit
                        assertLo(
                                stmt,
                                1,
                                "QUERY PLAN[VARCHAR]\n" +
                                        "Limit lo: $0::int[1] skip-over-rows: 0 limit: 1\n" +
                                        "    PageFrame\n" +
                                        "        Row backward scan\n" +
                                        "        Frame backward scan on: tab\n"
                        );

                        // same as the negative constant limit
                        assertLo(
                                stmt,
                                -1,
                                "QUERY PLAN[VARCHAR]\n" +
                                        "Limit lo: $0::int[-1] skip-over-rows: 9999 limit: 1\n" +
                                        "    PageFrame\n" +
                                        "        Row backward scan\n" +
                                        "        Frame backward scan on: tab\n"
                        );

                        assertLo(
                                stmt,
                                -10,
                                "QUERY PLAN[VARCHAR]\n" +
                                        "Limit lo: $0::int[-10] skip-over-rows: 9990 limit: 10\n" +
                                        "    PageFrame\n" +
                                        "        Row backward scan\n" +
                                        "        Frame backward scan on: tab\n"
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testLowLimitOnly() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, 25);
                try (Connection connection = getConnection(serverMain)) {

                    final PreparedStatement stmt = connection.prepareStatement("SELECT * from tab order by ts desc");
                    final ResultSet resultSet = stmt.executeQuery();
                    assertResultSet(
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym5,1,1970-01-01 00:00:22.4\n" +
                                    "Sym4,0,1970-01-01 00:00:22.3\n" +
                                    "Sym3,3,1970-01-01 00:00:22.2\n" +
                                    "Sym2,2,1970-01-01 00:00:22.1\n" +
                                    "Sym1,1,1970-01-01 00:00:22.0\n" +
                                    "Sym0,0,1970-01-01 00:00:21.9\n" +
                                    "Sym9,3,1970-01-01 00:00:21.8\n" +
                                    "Sym8,2,1970-01-01 00:00:21.7\n" +
                                    "Sym7,1,1970-01-01 00:00:21.6\n" +
                                    "Sym6,0,1970-01-01 00:00:21.5\n" +
                                    "Sym5,3,1970-01-01 00:00:21.4\n" +
                                    "Sym4,2,1970-01-01 00:00:21.3\n" +
                                    "Sym3,1,1970-01-01 00:00:21.2\n" +
                                    "Sym2,0,1970-01-01 00:00:21.1\n" +
                                    "Sym1,3,1970-01-01 00:00:21.0\n" +
                                    "Sym0,2,1970-01-01 00:00:20.9\n" +
                                    "Sym9,1,1970-01-01 00:00:20.8\n" +
                                    "Sym8,0,1970-01-01 00:00:20.7\n" +
                                    "Sym7,3,1970-01-01 00:00:20.6\n" +
                                    "Sym6,2,1970-01-01 00:00:20.5\n" +
                                    "Sym5,1,1970-01-01 00:00:20.4\n" +
                                    "Sym4,0,1970-01-01 00:00:20.3\n" +
                                    "Sym3,3,1970-01-01 00:00:20.2\n" +
                                    "Sym2,2,1970-01-01 00:00:20.1\n" +
                                    "Sym1,1,1970-01-01 00:00:20.0\n",
                            Misc.getThreadLocalSink(),
                            resultSet
                    );
                    stmt.close();

                    runQueryWithParams(
                            connection,
                            3,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym5,1,1970-01-01 00:00:22.4\n" +
                                    "Sym1,1,1970-01-01 00:00:22.0\n" +
                                    "Sym7,1,1970-01-01 00:00:21.6\n"
                    );
                    runQueryWithParams(
                            connection,
                            5,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym5,1,1970-01-01 00:00:22.4\n" +
                                    "Sym1,1,1970-01-01 00:00:22.0\n" +
                                    "Sym7,1,1970-01-01 00:00:21.6\n" +
                                    "Sym3,1,1970-01-01 00:00:21.2\n" +
                                    "Sym9,1,1970-01-01 00:00:20.8\n"
                    );
                    runQueryWithParams(
                            connection,
                            -3,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym9,1,1970-01-01 00:00:20.8\n" +
                                    "Sym5,1,1970-01-01 00:00:20.4\n" +
                                    "Sym1,1,1970-01-01 00:00:20.0\n"
                    );
                }
            }
        });
    }

    @Test
    public void testLowLimitZero() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, 25);
                try (Connection connection = getConnection(serverMain)) {

                    final PreparedStatement stmt = connection.prepareStatement("SELECT * from tab order by ts desc");
                    final ResultSet resultSet = stmt.executeQuery();
                    assertResultSet(
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym5,1,1970-01-01 00:00:22.4\n" +
                                    "Sym4,0,1970-01-01 00:00:22.3\n" +
                                    "Sym3,3,1970-01-01 00:00:22.2\n" +
                                    "Sym2,2,1970-01-01 00:00:22.1\n" +
                                    "Sym1,1,1970-01-01 00:00:22.0\n" +
                                    "Sym0,0,1970-01-01 00:00:21.9\n" +
                                    "Sym9,3,1970-01-01 00:00:21.8\n" +
                                    "Sym8,2,1970-01-01 00:00:21.7\n" +
                                    "Sym7,1,1970-01-01 00:00:21.6\n" +
                                    "Sym6,0,1970-01-01 00:00:21.5\n" +
                                    "Sym5,3,1970-01-01 00:00:21.4\n" +
                                    "Sym4,2,1970-01-01 00:00:21.3\n" +
                                    "Sym3,1,1970-01-01 00:00:21.2\n" +
                                    "Sym2,0,1970-01-01 00:00:21.1\n" +
                                    "Sym1,3,1970-01-01 00:00:21.0\n" +
                                    "Sym0,2,1970-01-01 00:00:20.9\n" +
                                    "Sym9,1,1970-01-01 00:00:20.8\n" +
                                    "Sym8,0,1970-01-01 00:00:20.7\n" +
                                    "Sym7,3,1970-01-01 00:00:20.6\n" +
                                    "Sym6,2,1970-01-01 00:00:20.5\n" +
                                    "Sym5,1,1970-01-01 00:00:20.4\n" +
                                    "Sym4,0,1970-01-01 00:00:20.3\n" +
                                    "Sym3,3,1970-01-01 00:00:20.2\n" +
                                    "Sym2,2,1970-01-01 00:00:20.1\n" +
                                    "Sym1,1,1970-01-01 00:00:20.0\n",
                            Misc.getThreadLocalSink(),
                            resultSet
                    );
                    stmt.close();

                    final String sql = "SELECT * from tab where status = ? order by ts desc limit ?,?";
                    runQueryWithParams(
                            connection,
                            sql,
                            1,
                            0,
                            1,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym5,1,1970-01-01 00:00:22.4\n"
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            3,
                            0,
                            3,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym3,3,1970-01-01 00:00:22.2\n" +
                                    "Sym9,3,1970-01-01 00:00:21.8\n" +
                                    "Sym5,3,1970-01-01 00:00:21.4\n"
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            2,
                            0,
                            3,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym2,2,1970-01-01 00:00:22.1\n" +
                                    "Sym8,2,1970-01-01 00:00:21.7\n" +
                                    "Sym4,2,1970-01-01 00:00:21.3\n"
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            1,
                            0,
                            10,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym5,1,1970-01-01 00:00:22.4\n" +
                                    "Sym1,1,1970-01-01 00:00:22.0\n" +
                                    "Sym7,1,1970-01-01 00:00:21.6\n" +
                                    "Sym3,1,1970-01-01 00:00:21.2\n" +
                                    "Sym9,1,1970-01-01 00:00:20.8\n" +
                                    "Sym5,1,1970-01-01 00:00:20.4\n" +
                                    "Sym1,1,1970-01-01 00:00:20.0\n"
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            1,
                            0,
                            -4,
                            "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                                    "Sym5,1,1970-01-01 00:00:22.4\n" +
                                    "Sym1,1,1970-01-01 00:00:22.0\n" +
                                    "Sym7,1,1970-01-01 00:00:21.6\n"
                    );
                }
            }
        });
    }

    private static void assertLo(PreparedStatement stmt, int limitLo, String expected) throws SQLException {
        stmt.clearParameters();
        stmt.setInt(1, limitLo);
        try (final ResultSet resultSet = stmt.executeQuery()) {
            assertResultSet(
                    expected,
                    Misc.getThreadLocalSink(),
                    resultSet
            );
        }
    }

    private static void assertLoHi(PreparedStatement stmt, int lo, int hi, String expected) throws SQLException {
        stmt.clearParameters();
        stmt.setInt(1, lo);
        stmt.setInt(2, hi);
        try (final ResultSet resultSet = stmt.executeQuery()) {
            assertResultSet(
                    expected,
                    Misc.getThreadLocalSink(),
                    resultSet
            );
        }
    }

    private static void createTable(ServerMain serverMain, int numOfRows) {
        final CairoEngine engine = serverMain.getEngine();
        try (
                SqlExecutionContext executionContext = new SqlExecutionContextImpl(engine, 1)
                        .with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()))
        ) {
            engine.execute(
                    "create table tab as (select concat('Sym', x%10) col1, x%4 status, timestamp_sequence(20000000, 100000) ts " +
                            "from long_sequence(" + numOfRows + ")) timestamp(ts) partition by day wal",
                    executionContext)
            ;
        } catch (SqlException e) {
            throw CairoException.critical(0).put("Could not create table: '").put(e.getFlyweightMessage());
        }
        drainWalQueue(engine);
    }

    private static Connection getConnection(ServerMain serverMain) throws SQLException {
        final int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
        final Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }

    private static void runQueryWithParams(Connection connection, String sql, int status, int limitLow, int limitHigh, String expected) throws SQLException {
        final PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, status);
        stmt.setInt(2, limitLow);
        if (limitHigh != 0) {
            stmt.setInt(3, limitHigh);
        }
        final ResultSet resultSet = stmt.executeQuery();
        assertResultSet(expected, Misc.getThreadLocalSink(), resultSet);
        stmt.close();
    }

    private static void runQueryWithParams(Connection connection, int limitLow, String expected) throws SQLException {
        runQueryWithParams(
                connection,
                "SELECT * from tab where status = ? order by ts desc limit ?",
                1,
                limitLow,
                0,
                expected
        );
    }
}
