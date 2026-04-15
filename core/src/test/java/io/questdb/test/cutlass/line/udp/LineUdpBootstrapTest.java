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

package io.questdb.test.cutlass.line.udp;

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.line.LineUdpSender;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.test.tools.TestUtils.assertEventually;

public class LineUdpBootstrapTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAllColumnTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    sender.table("test_udp_types")
                            .symbol("sym", "abc")
                            .longColumn("long_col", 42)
                            .doubleColumn("double_col", 3.14)
                            .stringColumn("string_col", "hello")
                            .boolColumn("bool_col", true)
                            .timestampColumn("ts_col", 1_234_567_890L, ChronoUnit.MICROS)
                            .atNow();
                    sender.flush();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM test_udp_types",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testCloseAndAssertHelper() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    sender.table("test_udp_close")
                            .symbol("device", "dev1")
                            .longColumn("reading", 100)
                            .atNow();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM test_udp_close",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testExplicitTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");
                long ts = Instant.now().toEpochMilli() * 1000;

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    sender.table("test_udp_ts")
                            .symbol("city", "paris")
                            .longColumn("temp", 15)
                            .at(ts, ChronoUnit.MICROS);
                    sender.flush();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM test_udp_ts",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testFlushAndAssertHelper() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    sender.table("udp_helper")
                            .symbol("sensor", "s1")
                            .doubleColumn("value", 123.456)
                            .atNow();
                    sender.flush();

                    assertEventually(() -> serverMain.assertSql(
                            "SELECT count() FROM udp_helper",
                            "count\n1\n"));

                    sender.table("udp_helper")
                            .symbol("sensor", "s2")
                            .doubleColumn("value", 789.012)
                            .atNow();
                    sender.flush();

                    assertEventually(() -> serverMain.assertSql(
                            "SELECT count() FROM udp_helper",
                            "count\n2\n"));
                }
            }
        });
    }

    @Test
    public void testInstantTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");
                Instant now = Instant.now();

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    sender.table("udp_instant")
                            .symbol("city", "berlin")
                            .longColumn("temp", 20)
                            .at(now);
                    sender.flush();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM udp_instant",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testMultipleFlushes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    for (int batch = 0; batch < 5; batch++) {
                        for (int i = 0; i < 10; i++) {
                            sender.table("udp_multiflush")
                                    .symbol("batch", String.valueOf(batch))
                                    .longColumn("idx", i)
                                    .atNow();
                        }
                        sender.flush();
                    }
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM udp_multiflush",
                        "count\n50\n"));
            }
        });
    }

    @Test
    public void testMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("udp_multi")
                                .symbol("city", "city_" + i)
                                .longColumn("temp", i * 10)
                                .atNow();
                    }
                    sender.flush();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM udp_multi",
                        "count\n10\n"));
            }
        });
    }

    @Test
    public void testNullStringValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    sender.table("udp_nullstr")
                            .symbol("id", "1")
                            .stringColumn("data", null)
                            .atNow();
                    sender.flush();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM udp_nullstr",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testSimpleInsert() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    sender.table("udp_simple")
                            .symbol("city", "london")
                            .longColumn("temp", 42)
                            .atNow();
                    sender.flush();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM udp_simple",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testSpecialCharactersInSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    sender.table("udp_special")
                            .symbol("name", "hello world")
                            .symbol("path", "/path/to/file")
                            .longColumn("count", 1)
                            .atNow();
                    sender.flush();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM udp_special",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testUnicodeInString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "true",
                    PropertyKey.LINE_UDP_UNICAST.getEnvVarName(), "true"
            )) {
                int port = serverMain.getConfiguration().getLineUdpReceiverConfiguration().getPort();
                int localhost = Numbers.parseIPv4Quiet("127.0.0.1");

                try (LineUdpSender sender = new LineUdpSender(localhost, localhost, port, 2048, 1)) {
                    sender.table("udp_unicode")
                            .symbol("lang", "ja")
                            .stringColumn("text", "こんにちは世界")
                            .atNow();
                    sender.flush();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM udp_unicode",
                        "count\n1\n"));
            }
        });
    }
}
