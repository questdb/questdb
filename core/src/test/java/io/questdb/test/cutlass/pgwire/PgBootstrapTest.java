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
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class PgBootstrapTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testClientWithEnabledTlsGetsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();

                try (Connection conn = getTlsConnection(port)) {
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
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
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
    public void testPgWireLoginDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables("QDB_PG_USER", "", // disables the default user
                    "QDB_PG_READONLY_USER_ENABLED", "false", // disables read-only user
                    "QDB_PG_READONLY_USER", "roUser",
                    "QDB_PG_READONLY_PASSWORD", "roPassword"
            )
            ) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
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
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
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
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
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
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
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

    private static Connection getTlsConnection(int port) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "require");
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }
}
