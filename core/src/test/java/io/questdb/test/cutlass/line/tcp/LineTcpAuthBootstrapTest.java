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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.AuthUtils;
import io.questdb.client.cutlass.line.AbstractLineTcpSender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.LineTcpSenderV2;
import io.questdb.std.Files;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.PrivateKey;
import java.time.temporal.ChronoUnit;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V2;
import static io.questdb.client.Sender.Transport;
import static io.questdb.test.tools.TestUtils.assertEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

public class LineTcpAuthBootstrapTest extends AbstractBootstrapTest {
    private static final String AUTH_CONF_PATH = "conf/auth.conf";
    private static final String AUTH_KEY_ID1 = "testUser1";
    private static final String AUTH_KEY_ID2_INVALID = "invalid";
    private static final int HOST = io.questdb.client.std.Numbers.parseIPv4("127.0.0.1");
    private static final String TOKEN = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    private static final PrivateKey AUTH_PRIVATE_KEY1 = AuthUtils.toPrivateKey(TOKEN);

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.LINE_TCP_AUTH_DB_PATH.getPropertyPath() + "=" + AUTH_CONF_PATH));
        TestUtils.unchecked(LineTcpAuthBootstrapTest::createIlpConfiguration);
        dbPath.parent().$();
    }

    @Test
    public void testAuthSuccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, port, 256 * 1024)) {
                    sender.authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
                    sender.metric("test_auth_success").field("my int field", 42).$();
                    sender.flush();
                }
                serverMain.awaitTable("test_auth_success");
            }
        });
    }

    @Test
    public void testAuthWrongKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, port, 2048)) {
                    sender.authenticate(AUTH_KEY_ID2_INVALID, AUTH_PRIVATE_KEY1);
                    long deadline = System.nanoTime() + SECONDS.toNanos(30);
                    while (System.nanoTime() < deadline) {
                        sender.metric("test_auth_wrong_key").field("my int field", 42).$();
                        sender.flush();
                    }
                    fail("Client failed to detect that QuestDB server closed the connection due to wrong credentials");
                } catch (LineSenderException expected) {
                    // ignored
                }
            }
        });
    }

    @Test
    public void testBuilderAuthSuccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = Sender.builder(Transport.TCP)
                        .address("127.0.0.1:" + port)
                        .enableAuth(AUTH_KEY_ID1).authToken(TOKEN)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .build()) {
                    sender.table("test_builder_auth_success").longColumn("my int field", 42).atNow();
                    sender.flush();
                }
                serverMain.awaitTable("test_builder_auth_success");
            }
        });
    }

    @Test
    public void testBuilderAuthSuccess_confString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = Sender.fromConfig("tcp::addr=127.0.0.1:" + port
                        + ";user=" + AUTH_KEY_ID1 + ";token=" + TOKEN + ";protocol_version=2;")) {
                    sender.table("test_builder_auth_success_conf_string").longColumn("my int field", 42).atNow();
                    sender.flush();
                }
                serverMain.awaitTable("test_builder_auth_success_conf_string");
            }
        });
    }

    @Test
    public void testConfString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                String confString = "tcp::addr=127.0.0.1:" + port + ";user=" + AUTH_KEY_ID1
                        + ";token=" + TOKEN + ";protocol_version=2;";
                try (Sender sender = Sender.fromConfig(confString)) {
                    long tsMicros = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00Z");
                    sender.table("test_conf_string")
                            .longColumn("int_field", 42)
                            .boolColumn("bool_field", true)
                            .stringColumn("string_field", "foo")
                            .doubleColumn("double_field", 42.0)
                            .timestampColumn("ts_field", tsMicros, ChronoUnit.MICROS)
                            .at(tsMicros, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT int_field, bool_field, string_field, double_field, ts_field, timestamp FROM test_conf_string",
                        """
                                int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp
                                42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000Z\t2022-02-25T00:00:00.000000Z
                                """));
            }
        });
    }

    @Test
    public void testMinBufferSizeWhenAuth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                int tinyCapacity = 42;
                try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, port, tinyCapacity)) {
                    sender.authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
                    fail();
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "challenge did not fit into buffer");
                }
            }
        });
    }

    private static void createIlpConfiguration() throws FileNotFoundException, UnsupportedEncodingException {
        final String confPath = root + Files.SEPARATOR + AUTH_CONF_PATH;
        try (PrintWriter writer = new PrintWriter(confPath, CHARSET)) {
            writer.println("testUser1\tec-p-256-sha256\tAKfkxOBlqBN8uDfTxu2Oo6iNsOPBnXkEH4gt44tBJKCY\tAL7WVjoH-IfeX_CXo5G1xXKp_PqHUrdo3xeRyDuWNbBX");
        }
    }
}
