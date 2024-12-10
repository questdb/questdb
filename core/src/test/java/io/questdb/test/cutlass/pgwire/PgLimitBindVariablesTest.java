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

import java.io.IOException;
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
    public void testLowAndHighLimit() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                createTable(serverMain, 100);
                try (Connection connection = getConnection(serverMain)) {
                    final String sql = "SELECT * from tab where status = ? order by ts desc limit ?,?";
                    runQueryWithParams(connection, sql, 1, 3, 5, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym1,1,1970-01-01 00:00:28.4\n" +
                            "Sym0,1,1970-01-01 00:00:28.0\n");
                    runQueryWithParams(connection, sql, 1, 5, 8, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym2,1,1970-01-01 00:00:27.6\n" +
                            "Sym1,1,1970-01-01 00:00:27.2\n" +
                            "Sym0,1,1970-01-01 00:00:26.8\n");
                    runQueryWithParams(connection, sql, 2, 4, 5, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym1,2,1970-01-01 00:00:28.1\n");
                    runQueryWithParams(connection, sql, 2, 4, -20, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym1,2,1970-01-01 00:00:28.1\n");
                    runQueryWithParams(connection, sql, 2, 4, -21, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n");
                    runQueryWithParams(connection, sql, 2, 4, -22, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n");
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
                    final String sql = "SELECT * from tab where status = ? order by ts desc limit ?";
                    runQueryWithParams(connection, sql, 1, 3, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym1,1,1970-01-01 00:00:22.4\n" +
                            "Sym0,1,1970-01-01 00:00:22.0\n" +
                            "Sym2,1,1970-01-01 00:00:21.6\n");
                    runQueryWithParams(connection, sql, 1, 5, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym1,1,1970-01-01 00:00:22.4\n" +
                            "Sym0,1,1970-01-01 00:00:22.0\n" +
                            "Sym2,1,1970-01-01 00:00:21.6\n" +
                            "Sym1,1,1970-01-01 00:00:21.2\n" +
                            "Sym0,1,1970-01-01 00:00:20.8\n");
                    runQueryWithParams(connection, sql, 1, -3, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym0,1,1970-01-01 00:00:20.8\n" +
                            "Sym2,1,1970-01-01 00:00:20.4\n" +
                            "Sym1,1,1970-01-01 00:00:20.0\n");
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
                    final String sql = "SELECT * from tab where status = ? order by ts desc limit ?,?";
                    runQueryWithParams(connection, sql, 1, 0, 1, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym1,1,1970-01-01 00:00:22.4\n");
                    runQueryWithParams(connection, sql, 3, 0, 3, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym2,3,1970-01-01 00:00:22.2\n" +
                            "Sym1,3,1970-01-01 00:00:21.8\n" +
                            "Sym0,3,1970-01-01 00:00:21.4\n");
                    runQueryWithParams(connection, sql, 2, 0, 3, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym1,2,1970-01-01 00:00:22.1\n" +
                            "Sym0,2,1970-01-01 00:00:21.7\n" +
                            "Sym2,2,1970-01-01 00:00:21.3\n");
                    runQueryWithParams(connection, sql, 1, 0, 10, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym1,1,1970-01-01 00:00:22.4\n" +
                            "Sym0,1,1970-01-01 00:00:22.0\n" +
                            "Sym2,1,1970-01-01 00:00:21.6\n" +
                            "Sym1,1,1970-01-01 00:00:21.2\n" +
                            "Sym0,1,1970-01-01 00:00:20.8\n" +
                            "Sym2,1,1970-01-01 00:00:20.4\n" +
                            "Sym1,1,1970-01-01 00:00:20.0\n");
                    runQueryWithParams(connection, sql, 1, 0, -4, "col1[VARCHAR],status[BIGINT],ts[TIMESTAMP]\n" +
                            "Sym1,1,1970-01-01 00:00:22.4\n" +
                            "Sym0,1,1970-01-01 00:00:22.0\n" +
                            "Sym2,1,1970-01-01 00:00:21.6\n");
                }
            }
        });
    }

    private static void createTable(ServerMain serverMain, int numOfRows) {
        final CairoEngine engine = serverMain.getEngine();
        try (
                SqlExecutionContext executionContext = new SqlExecutionContextImpl(engine, 1)
                        .with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()))
        ) {
            engine.execute("create table tab as (select concat('Sym', x%3) col1, x%4 status, timestamp_sequence(20000000, 100000) ts " +
                    "from long_sequence(" + numOfRows + ")) timestamp(ts) partition by day wal", executionContext);
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

    private static void runQueryWithParams(Connection connection, String sql, int status, int limitLow, String expected) throws SQLException, IOException {
        runQueryWithParams(connection, sql, status, limitLow, 0, expected);
    }

    private static void runQueryWithParams(Connection connection, String sql, int status, int limitLow, int limitHigh, String expected) throws SQLException, IOException {
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
}
