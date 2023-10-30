/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test;

import io.questdb.*;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerMainTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testServerMainILPDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, String> env = new HashMap<>(System.getenv());
            env.put("QDB_LINE_TCP_NET_BIND_TO", "0.0.0.0:9023");

            // Helps to test some exotic init options to increase test coverage.
            env.put("QDB_CAIRO_SQL_COPY_ROOT", dbPath.toString());
            env.put("QDB_TELEMETRY", dbPath.toString());
            env.put("QDB_TELEMETRY_DISABLE_COMPLETELY", "false");
            env.put("QDB_TELEMETRY_ENABLED", "true");

            // Global enable / disable ILP switch
            AtomicBoolean ilpEnabled = new AtomicBoolean(false);

            Bootstrap bootstrap = new Bootstrap(
                    new DefaultBootstrapConfiguration() {
                        @Override
                        public Map<String, String> getEnv() {
                            return env;
                        }

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
                                    FactoryProviderFactoryImpl.INSTANCE
                            ) {
                                @Override
                                public boolean isIlpEnabled() {
                                    return ilpEnabled.get();
                                }
                            };
                        }
                    },
                    getServerMainArgs()
            ) {
                public ServerConfiguration getConfiguration() {
                    return super.getConfiguration();
                }
            };

            ilpEnabled.set(false);
            // Start with ILP disabled
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();
                try (Sender sender = Sender.builder()
                        .address("localhost")
                        .port(9023)
                        .build()) {
                    sender.table("test").stringColumn("value", "test").atNow();
                    sender.flush();
                    Assert.fail();
                } catch (LineSenderException e) {
                    // expected
                    TestUtils.assertContains(e.getMessage(), "could not connect to host");
                }
            }

            ilpEnabled.set(true);
            // Start with ILP enabled
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();
                try (Sender sender = Sender.builder()
                        .address("localhost")
                        .port(9023)
                        .build()) {
                    sender.table("test").stringColumn("value", "test").atNow();
                    sender.flush();
                }
            }
        });
    }

    @Test
    public void testServerMainNoReStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                serverMain.start(); // <== no effect
                serverMain.close();
                try {
                    serverMain.getEngine();
                } catch (IllegalStateException ex) {
                    TestUtils.assertContains("close was called", ex.getMessage());
                }
                try {
                    serverMain.getWorkerPoolManager();
                } catch (IllegalStateException ex) {
                    TestUtils.assertContains("close was called", ex.getMessage());
                }
                serverMain.start(); // <== no effect
                serverMain.close(); // <== no effect
                serverMain.start(); // <== no effect
            }
        });
    }

    @Test
    public void testServerMainNoStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain ignore = new ServerMain(getServerMainArgs())) {
                Os.pause();
            }
        });
    }

    @Test
    public void testServerMainPgWire() throws Exception {
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection ignored = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                Os.pause();
            }
        }
    }

    @Test
    public void testServerMainStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                Assert.assertNotNull(serverMain.getConfiguration());
                Assert.assertNotNull(serverMain.getEngine());
                Assert.assertNull(serverMain.getWorkerPoolManager());
                Assert.assertFalse(serverMain.hasStarted());
                Assert.assertFalse(serverMain.hasBeenClosed());
                serverMain.start();
                Assert.assertNotNull(serverMain.getWorkerPoolManager());
            }
        });
    }

    @Test
    public void testServerMainStartHttpDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, String> env = new HashMap<>(System.getenv());
            env.put("QDB_HTTP_ENABLED", "false");
            Bootstrap bootstrap = new Bootstrap(
                    new DefaultBootstrapConfiguration() {
                        @Override
                        public Map<String, String> getEnv() {
                            return env;
                        }
                    },
                    getServerMainArgs()
            );
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                Assert.assertFalse(serverMain.getConfiguration().getHttpServerConfiguration().isEnabled());
                serverMain.start();
            }
        });
    }
}
