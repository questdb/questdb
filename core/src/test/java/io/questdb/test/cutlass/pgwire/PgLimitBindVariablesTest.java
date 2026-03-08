/*******************************************************************************
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
            createDummyConfiguration(
                    "pg.select.cache.enabled=true"
            );

            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, 100);
                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT * FROM tab LIMIT ?")) {
                        stmt.setInt(1, 1);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            QUERY PLAN[VARCHAR]
                                            Limit value: $0::int[1] skip-rows: 0 take-rows: 1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                            """,
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                        assertLo(
                                stmt,
                                -1,
                                """
                                        QUERY PLAN[VARCHAR]
                                        Limit value: $0::int[-1] skip-rows: 99 take-rows: 1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                                        """);
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab LIMIT ?")) {
                        stmt.setInt(1, 1);
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                            Sym1,1,1970-01-01 00:00:20.0
                                            """,
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                        assertLo(
                                stmt,
                                -1,
                                """
                                        col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                        Sym0,0,1970-01-01 00:00:29.9
                                        """
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
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym0,0,1970-01-01 00:00:29.9
                                    Sym9,3,1970-01-01 00:00:29.8
                                    Sym8,2,1970-01-01 00:00:29.7
                                    Sym7,1,1970-01-01 00:00:29.6
                                    Sym6,0,1970-01-01 00:00:29.5
                                    Sym5,3,1970-01-01 00:00:29.4
                                    Sym4,2,1970-01-01 00:00:29.3
                                    Sym3,1,1970-01-01 00:00:29.2
                                    Sym2,0,1970-01-01 00:00:29.1
                                    Sym1,3,1970-01-01 00:00:29.0
                                    Sym0,2,1970-01-01 00:00:28.9
                                    Sym9,1,1970-01-01 00:00:28.8
                                    Sym8,0,1970-01-01 00:00:28.7
                                    Sym7,3,1970-01-01 00:00:28.6
                                    Sym6,2,1970-01-01 00:00:28.5
                                    Sym5,1,1970-01-01 00:00:28.4
                                    Sym4,0,1970-01-01 00:00:28.3
                                    Sym3,3,1970-01-01 00:00:28.2
                                    Sym2,2,1970-01-01 00:00:28.1
                                    Sym1,1,1970-01-01 00:00:28.0
                                    Sym0,0,1970-01-01 00:00:27.9
                                    Sym9,3,1970-01-01 00:00:27.8
                                    Sym8,2,1970-01-01 00:00:27.7
                                    Sym7,1,1970-01-01 00:00:27.6
                                    Sym6,0,1970-01-01 00:00:27.5
                                    Sym5,3,1970-01-01 00:00:27.4
                                    Sym4,2,1970-01-01 00:00:27.3
                                    Sym3,1,1970-01-01 00:00:27.2
                                    Sym2,0,1970-01-01 00:00:27.1
                                    Sym1,3,1970-01-01 00:00:27.0
                                    Sym0,2,1970-01-01 00:00:26.9
                                    Sym9,1,1970-01-01 00:00:26.8
                                    Sym8,0,1970-01-01 00:00:26.7
                                    Sym7,3,1970-01-01 00:00:26.6
                                    Sym6,2,1970-01-01 00:00:26.5
                                    Sym5,1,1970-01-01 00:00:26.4
                                    Sym4,0,1970-01-01 00:00:26.3
                                    Sym3,3,1970-01-01 00:00:26.2
                                    Sym2,2,1970-01-01 00:00:26.1
                                    Sym1,1,1970-01-01 00:00:26.0
                                    Sym0,0,1970-01-01 00:00:25.9
                                    Sym9,3,1970-01-01 00:00:25.8
                                    Sym8,2,1970-01-01 00:00:25.7
                                    Sym7,1,1970-01-01 00:00:25.6
                                    Sym6,0,1970-01-01 00:00:25.5
                                    Sym5,3,1970-01-01 00:00:25.4
                                    Sym4,2,1970-01-01 00:00:25.3
                                    Sym3,1,1970-01-01 00:00:25.2
                                    Sym2,0,1970-01-01 00:00:25.1
                                    Sym1,3,1970-01-01 00:00:25.0
                                    Sym0,2,1970-01-01 00:00:24.9
                                    Sym9,1,1970-01-01 00:00:24.8
                                    Sym8,0,1970-01-01 00:00:24.7
                                    Sym7,3,1970-01-01 00:00:24.6
                                    Sym6,2,1970-01-01 00:00:24.5
                                    Sym5,1,1970-01-01 00:00:24.4
                                    Sym4,0,1970-01-01 00:00:24.3
                                    Sym3,3,1970-01-01 00:00:24.2
                                    Sym2,2,1970-01-01 00:00:24.1
                                    Sym1,1,1970-01-01 00:00:24.0
                                    Sym0,0,1970-01-01 00:00:23.9
                                    Sym9,3,1970-01-01 00:00:23.8
                                    Sym8,2,1970-01-01 00:00:23.7
                                    Sym7,1,1970-01-01 00:00:23.6
                                    Sym6,0,1970-01-01 00:00:23.5
                                    Sym5,3,1970-01-01 00:00:23.4
                                    Sym4,2,1970-01-01 00:00:23.3
                                    Sym3,1,1970-01-01 00:00:23.2
                                    Sym2,0,1970-01-01 00:00:23.1
                                    Sym1,3,1970-01-01 00:00:23.0
                                    Sym0,2,1970-01-01 00:00:22.9
                                    Sym9,1,1970-01-01 00:00:22.8
                                    Sym8,0,1970-01-01 00:00:22.7
                                    Sym7,3,1970-01-01 00:00:22.6
                                    Sym6,2,1970-01-01 00:00:22.5
                                    Sym5,1,1970-01-01 00:00:22.4
                                    Sym4,0,1970-01-01 00:00:22.3
                                    Sym3,3,1970-01-01 00:00:22.2
                                    Sym2,2,1970-01-01 00:00:22.1
                                    Sym1,1,1970-01-01 00:00:22.0
                                    Sym0,0,1970-01-01 00:00:21.9
                                    Sym9,3,1970-01-01 00:00:21.8
                                    Sym8,2,1970-01-01 00:00:21.7
                                    Sym7,1,1970-01-01 00:00:21.6
                                    Sym6,0,1970-01-01 00:00:21.5
                                    Sym5,3,1970-01-01 00:00:21.4
                                    Sym4,2,1970-01-01 00:00:21.3
                                    Sym3,1,1970-01-01 00:00:21.2
                                    Sym2,0,1970-01-01 00:00:21.1
                                    Sym1,3,1970-01-01 00:00:21.0
                                    Sym0,2,1970-01-01 00:00:20.9
                                    Sym9,1,1970-01-01 00:00:20.8
                                    Sym8,0,1970-01-01 00:00:20.7
                                    Sym7,3,1970-01-01 00:00:20.6
                                    Sym6,2,1970-01-01 00:00:20.5
                                    Sym5,1,1970-01-01 00:00:20.4
                                    Sym4,0,1970-01-01 00:00:20.3
                                    Sym3,3,1970-01-01 00:00:20.2
                                    Sym2,2,1970-01-01 00:00:20.1
                                    Sym1,1,1970-01-01 00:00:20.0
                                    """,
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
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym5,1,1970-01-01 00:00:28.4
                                    Sym1,1,1970-01-01 00:00:28.0
                                    """
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            1,
                            5,
                            8,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym7,1,1970-01-01 00:00:27.6
                                    Sym3,1,1970-01-01 00:00:27.2
                                    Sym9,1,1970-01-01 00:00:26.8
                                    """
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            2,
                            4,
                            5,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym2,2,1970-01-01 00:00:28.1
                                    """
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            2,
                            4,
                            -20,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym2,2,1970-01-01 00:00:28.1
                                    """
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
            createDummyConfiguration(
                    "pg.select.cache.enabled=true",
                    "cairo.sql.parallel.filter.enabled=false",
                    "cairo.sql.parallel.groupby.enabled=false",
                    "cairo.sql.parallel.read.parquet.enabled=false"
            );
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, 10000);
                try (Connection connection = getConnection(serverMain)) {
                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT col1, sum(status) as sum, last(ts) as last FROM tab ORDER BY 1 DESC")) {
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            // control set, we will be applying limits to this dataset essentially
                            assertResultSet(
                                    """
                                            col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]
                                            Sym9,2000,1970-01-01 00:16:59.8
                                            Sym8,1000,1970-01-01 00:16:59.7
                                            Sym7,2000,1970-01-01 00:16:59.6
                                            Sym6,1000,1970-01-01 00:16:59.5
                                            Sym5,2000,1970-01-01 00:16:59.4
                                            Sym4,1000,1970-01-01 00:16:59.3
                                            Sym3,2000,1970-01-01 00:16:59.2
                                            Sym2,1000,1970-01-01 00:16:59.1
                                            Sym1,2000,1970-01-01 00:16:59.0
                                            Sym0,1000,1970-01-01 00:16:59.9
                                            """,
                                    Misc.getThreadLocalSink(),
                                    resultSet
                            );
                        }
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT col1, sum(status) as sum, last(ts) as last FROM tab ORDER BY 1 DESC LIMIT 4,-3")) {
                        try (final ResultSet resultSet = stmt.executeQuery()) {
                            assertResultSet(
                                    """
                                            QUERY PLAN[VARCHAR]
                                            Limit left: 4 right: -3 skip-rows-max: 4 take-rows: baseRows-7
                                                Sort light
                                                  keys: [col1 desc]
                                                    GroupBy vectorized: false
                                                      keys: [col1]
                                                      values: [sum(status),last(ts)]
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: tab
                                            """,
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
                                """
                                        col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]
                                        Sym8,1000,1970-01-01 00:16:59.7
                                        Sym7,2000,1970-01-01 00:16:59.6
                                        """
                        );

                        assertLoHi(
                                stmt,
                                3,
                                1,
                                """
                                        col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]
                                        Sym8,1000,1970-01-01 00:16:59.7
                                        Sym7,2000,1970-01-01 00:16:59.6
                                        """
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
                                """
                                        col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]
                                        Sym8,1000,1970-01-01 00:16:59.7
                                        """
                        );

                        assertLoHi(
                                stmt,
                                -2,
                                -1,
                                """
                                        col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]
                                        Sym1,2000,1970-01-01 00:16:59.0
                                        """
                        );

                        assertLoHi(
                                stmt,
                                -1,
                                -2,
                                """
                                        col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]
                                        Sym1,2000,1970-01-01 00:16:59.0
                                        """
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
                                """
                                        col1[VARCHAR],sum[BIGINT],last[TIMESTAMP]
                                        Sym5,2000,1970-01-01 00:16:59.4
                                        Sym4,1000,1970-01-01 00:16:59.3
                                        Sym3,2000,1970-01-01 00:16:59.2
                                        """
                        );
                    }

                    // check the plan
                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT col1, sum(status) as sum, last(ts) as last FROM tab ORDER BY 2 DESC LIMIT ?,?")) {
                        assertLoHi(
                                stmt,
                                1,
                                3,
                                """
                                        QUERY PLAN[VARCHAR]
                                        Sort light lo: $0::int hi: $1::int
                                          keys: [sum desc]
                                            GroupBy vectorized: false
                                              keys: [col1]
                                              values: [sum(status),last(ts)]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                        """

                        );

                        assertLoHi(
                                stmt,
                                1,
                                2,
                                """
                                        QUERY PLAN[VARCHAR]
                                        Sort light lo: $0::int hi: $1::int
                                          keys: [sum desc]
                                            GroupBy vectorized: false
                                              keys: [col1]
                                              values: [sum(status),last(ts)]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                        """

                        );

                        assertLoHi(
                                stmt,
                                -5,
                                -1,
                                """
                                        QUERY PLAN[VARCHAR]
                                        Sort light lo: $0::int hi: $1::int
                                          keys: [sum desc]
                                            GroupBy vectorized: false
                                              keys: [col1]
                                              values: [sum(status),last(ts)]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                        """

                        );

                        assertLoHi(
                                stmt,
                                -1,
                                -5,
                                """
                                        QUERY PLAN[VARCHAR]
                                        Sort light lo: $0::int hi: $1::int
                                          keys: [sum desc]
                                            GroupBy vectorized: false
                                              keys: [col1]
                                              values: [sum(status),last(ts)]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                        """

                        );
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab ORDER BY ts DESC LIMIT ?")) {
                        assertLo(
                                stmt,
                                1,
                                """
                                        col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                        Sym0,0,1970-01-01 00:16:59.9
                                        """
                        );

                        assertLo(
                                stmt,
                                -1,
                                """
                                        col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                        Sym1,1,1970-01-01 00:00:20.0
                                        """
                        );

                        assertLo(
                                stmt,
                                -10,
                                """
                                        col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                        Sym0,2,1970-01-01 00:00:20.9
                                        Sym9,1,1970-01-01 00:00:20.8
                                        Sym8,0,1970-01-01 00:00:20.7
                                        Sym7,3,1970-01-01 00:00:20.6
                                        Sym6,2,1970-01-01 00:00:20.5
                                        Sym5,1,1970-01-01 00:00:20.4
                                        Sym4,0,1970-01-01 00:00:20.3
                                        Sym3,3,1970-01-01 00:00:20.2
                                        Sym2,2,1970-01-01 00:00:20.1
                                        Sym1,1,1970-01-01 00:00:20.0
                                        """
                        );
                    }

                    try (final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab ORDER BY ts DESC LIMIT ?,?")) {
                        assertLoHi(
                                stmt,
                                -1,
                                -4,
                                """
                                        col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                        Sym4,0,1970-01-01 00:00:20.3
                                        Sym3,3,1970-01-01 00:00:20.2
                                        Sym2,2,1970-01-01 00:00:20.1
                                        """
                        );
                    }

                    // explain for small limit
                    try (final PreparedStatement stmt = connection.prepareStatement("explain SELECT * FROM tab ORDER BY ts desc LIMIT ?")) {

                        // same as positive constant limit
                        assertLo(
                                stmt,
                                1,
                                """
                                        QUERY PLAN[VARCHAR]
                                        Limit value: $0::int[1] skip-rows: 0 take-rows: 1
                                            PageFrame
                                                Row backward scan
                                                Frame backward scan on: tab
                                        """
                        );

                        // same as the negative constant limit
                        assertLo(
                                stmt,
                                -1,
                                """
                                        QUERY PLAN[VARCHAR]
                                        Limit value: $0::int[-1] skip-rows: 9999 take-rows: 1
                                            PageFrame
                                                Row backward scan
                                                Frame backward scan on: tab
                                        """
                        );

                        assertLo(
                                stmt,
                                -10,
                                """
                                        QUERY PLAN[VARCHAR]
                                        Limit value: $0::int[-10] skip-rows: 9990 take-rows: 10
                                            PageFrame
                                                Row backward scan
                                                Frame backward scan on: tab
                                        """
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
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym5,1,1970-01-01 00:00:22.4
                                    Sym4,0,1970-01-01 00:00:22.3
                                    Sym3,3,1970-01-01 00:00:22.2
                                    Sym2,2,1970-01-01 00:00:22.1
                                    Sym1,1,1970-01-01 00:00:22.0
                                    Sym0,0,1970-01-01 00:00:21.9
                                    Sym9,3,1970-01-01 00:00:21.8
                                    Sym8,2,1970-01-01 00:00:21.7
                                    Sym7,1,1970-01-01 00:00:21.6
                                    Sym6,0,1970-01-01 00:00:21.5
                                    Sym5,3,1970-01-01 00:00:21.4
                                    Sym4,2,1970-01-01 00:00:21.3
                                    Sym3,1,1970-01-01 00:00:21.2
                                    Sym2,0,1970-01-01 00:00:21.1
                                    Sym1,3,1970-01-01 00:00:21.0
                                    Sym0,2,1970-01-01 00:00:20.9
                                    Sym9,1,1970-01-01 00:00:20.8
                                    Sym8,0,1970-01-01 00:00:20.7
                                    Sym7,3,1970-01-01 00:00:20.6
                                    Sym6,2,1970-01-01 00:00:20.5
                                    Sym5,1,1970-01-01 00:00:20.4
                                    Sym4,0,1970-01-01 00:00:20.3
                                    Sym3,3,1970-01-01 00:00:20.2
                                    Sym2,2,1970-01-01 00:00:20.1
                                    Sym1,1,1970-01-01 00:00:20.0
                                    """,
                            Misc.getThreadLocalSink(),
                            resultSet
                    );
                    stmt.close();

                    runQueryWithParams(
                            connection,
                            3,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym5,1,1970-01-01 00:00:22.4
                                    Sym1,1,1970-01-01 00:00:22.0
                                    Sym7,1,1970-01-01 00:00:21.6
                                    """
                    );
                    runQueryWithParams(
                            connection,
                            5,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym5,1,1970-01-01 00:00:22.4
                                    Sym1,1,1970-01-01 00:00:22.0
                                    Sym7,1,1970-01-01 00:00:21.6
                                    Sym3,1,1970-01-01 00:00:21.2
                                    Sym9,1,1970-01-01 00:00:20.8
                                    """
                    );
                    runQueryWithParams(
                            connection,
                            -3,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym9,1,1970-01-01 00:00:20.8
                                    Sym5,1,1970-01-01 00:00:20.4
                                    Sym1,1,1970-01-01 00:00:20.0
                                    """
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
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym5,1,1970-01-01 00:00:22.4
                                    Sym4,0,1970-01-01 00:00:22.3
                                    Sym3,3,1970-01-01 00:00:22.2
                                    Sym2,2,1970-01-01 00:00:22.1
                                    Sym1,1,1970-01-01 00:00:22.0
                                    Sym0,0,1970-01-01 00:00:21.9
                                    Sym9,3,1970-01-01 00:00:21.8
                                    Sym8,2,1970-01-01 00:00:21.7
                                    Sym7,1,1970-01-01 00:00:21.6
                                    Sym6,0,1970-01-01 00:00:21.5
                                    Sym5,3,1970-01-01 00:00:21.4
                                    Sym4,2,1970-01-01 00:00:21.3
                                    Sym3,1,1970-01-01 00:00:21.2
                                    Sym2,0,1970-01-01 00:00:21.1
                                    Sym1,3,1970-01-01 00:00:21.0
                                    Sym0,2,1970-01-01 00:00:20.9
                                    Sym9,1,1970-01-01 00:00:20.8
                                    Sym8,0,1970-01-01 00:00:20.7
                                    Sym7,3,1970-01-01 00:00:20.6
                                    Sym6,2,1970-01-01 00:00:20.5
                                    Sym5,1,1970-01-01 00:00:20.4
                                    Sym4,0,1970-01-01 00:00:20.3
                                    Sym3,3,1970-01-01 00:00:20.2
                                    Sym2,2,1970-01-01 00:00:20.1
                                    Sym1,1,1970-01-01 00:00:20.0
                                    """,
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
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym5,1,1970-01-01 00:00:22.4
                                    """
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            3,
                            0,
                            3,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym3,3,1970-01-01 00:00:22.2
                                    Sym9,3,1970-01-01 00:00:21.8
                                    Sym5,3,1970-01-01 00:00:21.4
                                    """
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            2,
                            0,
                            3,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym2,2,1970-01-01 00:00:22.1
                                    Sym8,2,1970-01-01 00:00:21.7
                                    Sym4,2,1970-01-01 00:00:21.3
                                    """
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            1,
                            0,
                            10,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym5,1,1970-01-01 00:00:22.4
                                    Sym1,1,1970-01-01 00:00:22.0
                                    Sym7,1,1970-01-01 00:00:21.6
                                    Sym3,1,1970-01-01 00:00:21.2
                                    Sym9,1,1970-01-01 00:00:20.8
                                    Sym5,1,1970-01-01 00:00:20.4
                                    Sym1,1,1970-01-01 00:00:20.0
                                    """
                    );
                    runQueryWithParams(
                            connection,
                            sql,
                            1,
                            0,
                            -4,
                            """
                                    col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]
                                    Sym5,1,1970-01-01 00:00:22.4
                                    Sym1,1,1970-01-01 00:00:22.0
                                    Sym7,1,1970-01-01 00:00:21.6
                                    """
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
