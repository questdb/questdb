package io.questdb.test.cutlass.pgwire;

import io.questdb.Bootstrap;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.ServerMain;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PgBootstrapTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testDefaultUserEnabledReadOnlyUserDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables(
                    Map.of("QDB_PG_USER", "", // disables the default user
                            "QDB_PG_READONLY_USER_ENABLED", "true",
                            "QDB_PG_READONLY_USER", "roUser",
                            "QDB_PG_READONLY_PASSWORD", "roPassword"))
            ) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails("roUser",
                        "roPassword",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "Write permission denied",
                        "read only user should not be able to create a new table"
                );

                assertQueryFails("admin",
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
            try (ServerMain serverMain = startWithEnvVariables(
                    Map.of("QDB_PG_USER", "", // disables the default user
                            "QDB_PG_READONLY_USER_ENABLED", "false", // disables read-only user
                            "QDB_PG_READONLY_USER", "roUser",
                            "QDB_PG_READONLY_PASSWORD", "roPassword"))
            ) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails("roUser",
                        "roPassword",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "invalid username/password",
                        "read only user is disabled and should not be able to connect"
                );

                assertQueryFails("admin",
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
            try (ServerMain serverMain = startWithEnvVariables(Map.of("QDB_PG_SECURITY_READONLY", "true"))) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails("admin",
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
                    Map.of("QDB_PG_READONLY_USER_ENABLED", "true",
                            "QDB_PG_READONLY_USER", "roUser",
                            "QDB_PG_READONLY_PASSWORD", "roPassword"))
            ) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails("roUser",
                        "roPassword",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "Write permission denied",
                        "read only user should not be able to create a new table"
                );

                assertQuerySucceeds("admin",
                        "quest",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "admin should be able to create a new table");
            }
        });
    }

    @Test
    public void testReadOnlyPgWireUserAndReadOnlyContext() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables(
                    Map.of("QDB_PG_READONLY_USER_ENABLED", "true",
                            "QDB_PG_READONLY_USER", "roUser",
                            "QDB_PG_READONLY_PASSWORD", "roPassword",
                            "QDB_PG_SECURITY_READONLY", "true"))
            ) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                assertQueryFails("roUser",
                        "roPassword",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "Write permission denied",
                        "read only user should not be able to create a new table"
                );

                assertQueryFails("admin",
                        "quest",
                        port,
                        "create table x as (select * from long_sequence(1000000))",
                        "Write permission denied",
                        "admin should not be able to create table in read-only mode"
                );
            }
        });
    }

    private static void assertQueryFails(String username,
                                         String password,
                                         int port,
                                         String queryText,
                                         String exceptionMessageMustContain,
                                         String assertionFailureMessage) throws SQLException {
        try (Connection conn = getConnection(username, password, port)) {
            conn.createStatement().execute(queryText);
            Assert.fail(assertionFailureMessage);
        } catch (PSQLException e) {
            TestUtils.assertContains(e.getMessage(), exceptionMessageMustContain);
        }
    }

    private static void assertQuerySucceeds(String username,
                                            String password,
                                            int port,
                                            String queryText,
                                            String assertionFailureMessage) throws SQLException {
        try (Connection conn = getConnection(username, password, port)) {
            conn.createStatement().execute(queryText);
        } catch (PSQLException e) {
            throw new AssertionError(assertionFailureMessage, e);
        }
    }

    private static Connection getConnection(String username, String password, int port) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }

    @NotNull
    private static Bootstrap newBootstrapWithEnvVariables(Map<String, String> envs) {
        Map<String, String> env = new HashMap<>(System.getenv());

        env.putAll(envs);
        return new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        return env;
                    }
                },
                getServerMainArgs()
        );
    }

    private static ServerMain startWithEnvVariables(Map<String, String> envs) {
        ServerMain serverMain = new ServerMain(newBootstrapWithEnvVariables(envs));
        serverMain.start();
        return serverMain;
    }
}
