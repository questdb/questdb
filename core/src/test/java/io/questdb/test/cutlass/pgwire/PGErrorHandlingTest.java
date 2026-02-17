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

import io.questdb.Bootstrap;
import io.questdb.FactoryProviderImpl;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropServerConfiguration;
import io.questdb.ServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.pgwire.PGAuthenticatorFactory;
import io.questdb.network.Socket;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class PGErrorHandlingTest extends AbstractBootstrapTest {

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
                                    public long openRW(LPSZ name, int opts) {
                                        if (Utf8s.endsWithAscii(name, "x" + Files.SEPARATOR + "_meta")) {
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
                    conn.createStatement().execute("create table x as (select 1L y)");
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
                                    public @NotNull PGAuthenticatorFactory getPgWireAuthenticatorFactory() {
                                        return (pgWireConfiguration, circuitBreaker, registry, optionsListener) -> new SocketAuthenticator() {

                                            @Override
                                            public void close() {
                                                Misc.free(circuitBreaker);
                                            }

                                            @Override
                                            public CharSequence getPrincipal() {
                                                return null;
                                            }

                                            @Override
                                            public long getRecvBufPos() {
                                                return 0;
                                            }

                                            @Override
                                            public long getRecvBufPseudoStart() {
                                                return 0;
                                            }

                                            @Override
                                            public int handleIO() {
                                                return 0;
                                            }

                                            @Override
                                            public void init(@NotNull Socket socket, long recvBuffer, long recvBufferLimit, long sendBuffer, long sendBufferLimit) {
                                            }

                                            @Override
                                            public boolean isAuthenticated() {
                                                throw new RuntimeException("Test error");
                                            }
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
