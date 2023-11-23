package io.questdb.test.cutlass.pgwire;

import io.questdb.*;
import io.questdb.cutlass.pgwire.CleartextPasswordPgWireAuthenticator;
import io.questdb.cutlass.pgwire.CustomCloseActionPasswordMatcherDelegate;
import io.questdb.cutlass.pgwire.PgWireAuthenticatorFactory;
import io.questdb.cutlass.pgwire.UsernamePasswordMatcher;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.LPSZ;
import io.questdb.test.BootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class PGErrorHandlingTest extends BootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testUnexpectedErrorDuringSQLExecutionHandled() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
                        return new PropServerConfiguration(
                                bootstrap.getRootDirectory(),
                                bootstrap.loadProperties(),
                                getEnv(),
                                bootstrap.getLog(),
                                bootstrap.getBuildInformation(),
                                new FilesFacadeImpl() {
                                    @Override
                                    public int openRW(LPSZ name, long opts) {
                                        if (counter.incrementAndGet() > 28) {
                                            throw new RuntimeException("Test error");
                                        }
                                        return super.openRW(name, opts);
                                    }
                                },
                                bootstrap.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration)
                        );
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (Connection conn = getConnection()) {
                    conn.createStatement().execute("create table x(y long)");
                    Assert.fail("Expected exception is missing");
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "ERROR: Test error");
                }
            }
        });
    }

    @Test
    public void testUnexpectedErrorOutsideSQLExecutionResultsInDisconnect() throws Exception {
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
                        return new PropServerConfiguration(
                                bootstrap.getRootDirectory(),
                                bootstrap.loadProperties(),
                                getEnv(),
                                bootstrap.getLog(),
                                bootstrap.getBuildInformation(),
                                FilesFacadeImpl.INSTANCE,
                                bootstrap.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration) {
                                    @Override
                                    public @NotNull PgWireAuthenticatorFactory getPgWireAuthenticatorFactory() {
                                        return (pgWireConfiguration, circuitBreaker, registry, optionsListener) -> {
                                            DirectUtf8Sink defaultUserPasswordSink = new DirectUtf8Sink(4);
                                            DirectUtf8Sink readOnlyUserPasswordSink = new DirectUtf8Sink(4);
                                            UsernamePasswordMatcher matcher = new CustomCloseActionPasswordMatcherDelegate(
                                                    ServerMain.newPgWireUsernamePasswordMatcher(pgWireConfiguration, defaultUserPasswordSink, readOnlyUserPasswordSink),
                                                    () -> {
                                                        defaultUserPasswordSink.close();
                                                        readOnlyUserPasswordSink.close();
                                                    }
                                            );

                                            return new CleartextPasswordPgWireAuthenticator(
                                                    pgWireConfiguration,
                                                    circuitBreaker,
                                                    registry,
                                                    optionsListener,
                                                    matcher,
                                                    true
                                            ) {
                                                @Override
                                                public boolean isAuthenticated() {
                                                    throw new RuntimeException("Test error");
                                                }
                                            };
                                        };
                                    }
                                }
                        );
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();
                try (Connection ignored = getConnection()) {
                    Assert.fail("Expected exception is missing");
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "The connection attempt failed.");
                }
            }
        });
    }

    private static Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", PG_PORT);
        return DriverManager.getConnection(url, properties);
    }
}
