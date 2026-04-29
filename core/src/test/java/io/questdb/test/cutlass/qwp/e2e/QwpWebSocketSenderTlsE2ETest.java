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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TlsProxyRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * End-to-end TLS integration tests for QwpWebSocketSender.
 * <p>
 * These tests verify that QwpWebSocketSender can connect to a TLS-enabled
 * endpoint (via TlsProxy), write data, and that the data is correctly
 * stored and queryable. The TlsProxy terminates TLS and forwards plaintext
 * to the QuestDB HTTP server.
 */
public class QwpWebSocketSenderTlsE2ETest extends AbstractBootstrapTest {

    @Rule
    public TlsProxyRule tlsProxy = TlsProxyRule.toHostAndPort("localhost", HTTP_PORT);

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testTlsAsyncMode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int tlsPort = tlsProxy.getListeningPort();

                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig("wss::addr=localhost:" + tlsPort + ";tls_verify=unsafe_off;")) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("tls_async")
                                .longColumn("id", i)
                                .doubleColumn("value", i * 1.5)
                                .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("tls_async");
                serverMain.assertSql("SELECT count() FROM tls_async", "count\n50\n");
            }
        });
    }

    @Test
    public void testTlsMultipleColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int tlsPort = tlsProxy.getListeningPort();

                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig("wss::addr=localhost:" + tlsPort + ";tls_verify=unsafe_off;")) {
                    sender.table("tls_multi_col")
                            .symbol("city", "london")
                            .doubleColumn("temperature", 18.5)
                            .longColumn("humidity", 72L)
                            .boolColumn("raining", true)
                            .stringColumn("note", "overcast skies")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tls_multi_col");
                serverMain.assertSql(
                        "SELECT city, temperature, humidity, raining, note FROM tls_multi_col",
                        """
                                city\ttemperature\thumidity\training\tnote
                                london\t18.5\t72\ttrue\tovercast skies
                                """
                );
            }
        });
    }

    @Test
    public void testTlsMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int tlsPort = tlsProxy.getListeningPort();

                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig("wss::addr=localhost:" + tlsPort + ";tls_verify=unsafe_off;")) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("tls_multi_row")
                                .longColumn("value", i)
                                .at(1_000_000_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("tls_multi_row");
                serverMain.assertSql("SELECT count() FROM tls_multi_row", "count\n100\n");
                serverMain.assertSql(
                        "SELECT sum(value) FROM tls_multi_row",
                        "sum\n4950\n"
                );
            }
        });
    }

    @Test
    public void testTlsSingleRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int tlsPort = tlsProxy.getListeningPort();

                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig("wss::addr=localhost:" + tlsPort + ";tls_verify=unsafe_off;")) {
                    sender.table("tls_single")
                            .longColumn("value", 42L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tls_single");
                serverMain.assertSql("SELECT count() FROM tls_single", "count\n1\n");
                serverMain.assertSql("SELECT value FROM tls_single", "value\n42\n");
            }
        });
    }
}
