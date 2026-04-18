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

package io.questdb.test.cutlass.qwp.udp;

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.qwp.client.QwpUdpSender;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.network.Net;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class QwpUdpServerMainTest extends AbstractBootstrapTest {

    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int QWP_UDP_PORT = 19_007 + randomPortOffset;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testE2E_100rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.QWP_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.QWP_UDP_BIND_TO.getEnvVarName(), "0.0.0.0:" + QWP_UDP_PORT,
                    PropertyKey.QWP_UDP_OWN_THREAD.getEnvVarName(), "true",
                    PropertyKey.QWP_UDP_UNICAST.getEnvVarName(), "true",
                    PropertyKey.CAIRO_WAL_ENABLED_DEFAULT.getEnvVarName(), "true"
            )) {
                try (QwpUdpSender sender = new QwpUdpSender(
                        NetworkFacadeImpl.INSTANCE,
                        0,
                        LOCALHOST,
                        QWP_UDP_PORT,
                        0
                )) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("qwp_udp_e2e")
                                .longColumn("id", i)
                                .doubleColumn("value", i * 1.5)
                                .at(1_000_000_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTxn("qwp_udp_e2e", 1);
                serverMain.assertSql("SELECT count() FROM qwp_udp_e2e", "count\n100\n");
            }
        });
    }

    @Test
    public void testE2E_disabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            //noinspection EmptyTryBlock
            try (final TestServerMain ignored = startWithEnvVariables(
                    PropertyKey.QWP_UDP_ENABLED.getEnvVarName(), "false"
            )) {
                // server starts and shuts down without crash
            }
        });
    }

    @Test
    public void testE2E_gracefulShutdown() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startWithEnvVariables(
                    PropertyKey.QWP_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.QWP_UDP_BIND_TO.getEnvVarName(), "0.0.0.0:" + QWP_UDP_PORT,
                    PropertyKey.QWP_UDP_OWN_THREAD.getEnvVarName(), "true",
                    PropertyKey.QWP_UDP_UNICAST.getEnvVarName(), "true",
                    PropertyKey.CAIRO_WAL_ENABLED_DEFAULT.getEnvVarName(), "true"
            )) {
                try (QwpUdpSender sender = new QwpUdpSender(
                        NetworkFacadeImpl.INSTANCE,
                        0,
                        LOCALHOST,
                        QWP_UDP_PORT,
                        0
                )) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("qwp_udp_shutdown")
                                .longColumn("id", i)
                                .at(1_000_000_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                // server closes without crash or leak
            }
        });
    }

    @Test
    public void testE2E_workerPoolMode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.QWP_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.QWP_UDP_BIND_TO.getEnvVarName(), "0.0.0.0:" + QWP_UDP_PORT,
                    PropertyKey.QWP_UDP_OWN_THREAD.getEnvVarName(), "false",
                    PropertyKey.QWP_UDP_UNICAST.getEnvVarName(), "true",
                    PropertyKey.CAIRO_WAL_ENABLED_DEFAULT.getEnvVarName(), "true"
            )) {
                try (QwpUdpSender sender = new QwpUdpSender(
                        NetworkFacadeImpl.INSTANCE,
                        0,
                        LOCALHOST,
                        QWP_UDP_PORT,
                        0
                )) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("qwp_udp_pool")
                                .longColumn("id", i)
                                .doubleColumn("value", i * 1.5)
                                .at(1_000_000_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTxn("qwp_udp_pool", 1);
                serverMain.assertSql("SELECT count() FROM qwp_udp_pool", "count\n100\n");
            }
        });
    }

    @Test
    public void testE2E_workerPoolShutdownUnderLoad() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicBoolean keepSending = new AtomicBoolean(true);
            Thread senderThread = null;
            TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.QWP_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.QWP_UDP_BIND_TO.getEnvVarName(), "0.0.0.0:" + QWP_UDP_PORT,
                    PropertyKey.QWP_UDP_OWN_THREAD.getEnvVarName(), "false",
                    PropertyKey.QWP_UDP_UNICAST.getEnvVarName(), "true",
                    PropertyKey.CAIRO_WAL_ENABLED_DEFAULT.getEnvVarName(), "true"
            );
            //noinspection TryFinallyCanBeTryWithResources We're timing server shutdown
            try {
                try (QwpUdpSender sender = new QwpUdpSender(
                        NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, QWP_UDP_PORT, 0
                )) {
                    sender.table("qwp_udp_load")
                            .longColumn("id", 0)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable("qwp_udp_load");

                senderThread = new Thread(() -> {
                    try (QwpUdpSender sender = new QwpUdpSender(
                            NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, QWP_UDP_PORT, 0
                    )) {
                        long i = 1;
                        while (keepSending.get()) {
                            sender.table("qwp_udp_load")
                                    .longColumn("id", i++)
                                    .at(1_000_000_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                            sender.flush();
                        }
                    }
                });
                senderThread.start();
            } finally {
                long startNanos = System.nanoTime();
                serverMain.close();
                long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
                keepSending.set(false);
                if (senderThread != null) {
                    senderThread.join(5_000);
                }
                Assert.assertTrue(
                        "shutdown under load took " + elapsedMs + "ms, expected < 30000ms",
                        elapsedMs < 30_000
                );
            }
        });
    }
}
