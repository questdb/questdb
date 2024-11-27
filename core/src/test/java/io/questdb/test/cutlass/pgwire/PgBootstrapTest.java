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
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.*;

import static io.questdb.test.cutlass.pgwire.BasePGTest.LegacyMode.LEGACY;
import static io.questdb.test.cutlass.pgwire.BasePGTest.legacyModeParams;

@RunWith(Parameterized.class)
public class PgBootstrapTest extends AbstractBootstrapTest {

    private final boolean testParamLegacyMode;

    public PgBootstrapTest(BasePGTest.LegacyMode legacyMode) {
        this.testParamLegacyMode = legacyMode == LEGACY;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return legacyModeParams();
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> {
            if (testParamLegacyMode) {
                createDummyConfiguration("pg.legacy.mode.enabled=true");
            } else {
                createDummyConfiguration();
            }
        });
        dbPath.parent().$();
    }

    @Test
    public void testClientWithEnabledTlsGetsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();

                try (Connection conn = getTlsConnection("admin", "quest", port)) {
                    conn.createStatement().execute("select 1;");
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "does not support SSL");
                }
            }
        });
    }

    @Test
    public void testDefaultUserEnabledReadOnlyUserDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables(
                    "QDB_PG_USER", "", // disables the default user
                    "QDB_PG_READONLY_USER_ENABLED", "true",
                    "QDB_PG_READONLY_USER", "roUser",
                    "QDB_PG_READONLY_PASSWORD", "roPassword"
            )
            ) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails(
                        "roUser",
                        "roPassword",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "Write permission denied",
                        "read only user should not be able to create a new table"
                );

                assertQueryFails(
                        "admin",
                        "quest",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "invalid username/password",
                        "admin user is disabled and should not be able to connect"
                );
            }
        });
    }

    @Test
    public void testInsertAsSelect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = TestServerMain.createWithManualWalRun(getServerMainArgs())) {
                serverMain.start();
                final int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                try (Connection conn = getConnection("admin", "quest", port)) {
                    conn.createStatement().execute("create table x(ts timestamp, col1 long) timestamp(ts) partition by day wal");
                    conn.createStatement().execute("create table y(ts timestamp, col1 long) timestamp(ts) partition by day wal");

                    assertEquals(10, conn.createStatement().executeUpdate("insert into x select timestamp_sequence(1677628800000000, 10000000), x from long_sequence(10)"));
                    drainWalQueue(serverMain.getEngine());
                    assertSql(conn, "x", "ts[TIMESTAMP],col1[BIGINT]\n" +
                            "2023-03-01 00:00:00.0,1\n" +
                            "2023-03-01 00:00:10.0,2\n" +
                            "2023-03-01 00:00:20.0,3\n" +
                            "2023-03-01 00:00:30.0,4\n" +
                            "2023-03-01 00:00:40.0,5\n" +
                            "2023-03-01 00:00:50.0,6\n" +
                            "2023-03-01 00:01:00.0,7\n" +
                            "2023-03-01 00:01:10.0,8\n" +
                            "2023-03-01 00:01:20.0,9\n" +
                            "2023-03-01 00:01:30.0,10\n");
                    assertSql(conn, "y", "ts[TIMESTAMP],col1[BIGINT]\n");

                    assertEquals(2, conn.createStatement().executeUpdate("insert into y select * from x where col1 > 8"));
                    drainWalQueue(serverMain.getEngine());
                    assertSql(conn, "y", "ts[TIMESTAMP],col1[BIGINT]\n" +
                            "2023-03-01 00:01:20.0,9\n" +
                            "2023-03-01 00:01:30.0,10\n");
                }
            }
        });
    }

    @Test
    public void testPgWireLoginDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables("QDB_PG_USER", "", // disables the default user
                    "QDB_PG_READONLY_USER_ENABLED", "false", // disables read-only user
                    "QDB_PG_READONLY_USER", "roUser",
                    "QDB_PG_READONLY_PASSWORD", "roPassword"
            )
            ) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails(
                        "roUser",
                        "roPassword",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "invalid username/password",
                        "read only user is disabled and should not be able to connect"
                );

                assertQueryFails(
                        "admin",
                        "quest",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "invalid username/password",
                        "admin user is disabled and should not be able to connect"
                );
            }
        });
    }

    @Test
    public void testReadOnlyPgWireContext() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables("QDB_PG_SECURITY_READONLY", "true")) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails(
                        "admin",
                        "quest",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "Write permission denied",
                        "admin should not be able to create table in read-only mode"
                );
            }
        });
    }

    @Test
    public void testReadOnlyPgWireUser() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables(
                    "QDB_PG_READONLY_USER_ENABLED", "true",
                    "QDB_PG_READONLY_USER", "roUser",
                    "QDB_PG_READONLY_PASSWORD", "roPassword"
            )
            ) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails(
                        "roUser",
                        "roPassword",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "Write permission denied",
                        "read only user should not be able to create a new table"
                );

                assertQuerySucceeds(
                        "admin",
                        "quest",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "admin should be able to create a new table"
                );
            }
        });
    }

    @Test
    public void testReadOnlyPgWireUserAndReadOnlyContext() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables(
                    "QDB_PG_READONLY_USER_ENABLED", "true",
                    "QDB_PG_READONLY_USER", "roUser",
                    "QDB_PG_READONLY_PASSWORD", "roPassword",
                    "QDB_PG_SECURITY_READONLY", "true"
            )
            ) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails(
                        "roUser",
                        "roPassword",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "Write permission denied",
                        "read only user should not be able to create a new table"
                );

                assertQueryFails(
                        "admin",
                        "quest",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "Write permission denied",
                        "admin should not be able to create table in read-only mode"
                );
            }
        });
    }

    private static Connection getTlsConnection(String username, String password, int port) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("sslmode", "require");
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }

    private void assertSql(Connection conn, String sql, String expectedResult) throws SQLException, IOException {
        final StringSink sink = Misc.getThreadLocalSink();
        sink.clear();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            BasePGTest.assertResultSet(expectedResult, sink, rs);
        }
    }
}
