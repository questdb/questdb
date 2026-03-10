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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_GEOHASH;

/**
 * End-to-end tests for QWP WebSocket type conversions.
 * <p>
 * Each test pre-creates a table with a specific column type, then sends data
 * with a different wire type via the {@link Sender} API. The server converts
 * the wire type to the column type, and we verify the result with SQL assertions.
 */
public class QwpWebSocketTypeConversionE2ETest extends AbstractBootstrapTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBooleanToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_bool_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_bool_str")
                            .boolColumn("col", true)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_bool_str")
                            .boolColumn("col", false)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_bool_str")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_bool_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_bool_str ORDER BY ts",
                        "col\ntrue\nfalse\n\n"
                );
            }
        });
    }

    @Test
    public void testBooleanToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_bool_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_bool_vc")
                            .boolColumn("col", false)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_bool_vc")
                            .boolColumn("col", true)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_bool_vc")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_bool_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_bool_vc ORDER BY ts",
                        "col\nfalse\ntrue\n\n"
                );
            }
        });
    }

    @Test
    public void testDecimal128ToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dec128_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dec128_str")
                            .decimalColumn("col", Decimal128.fromLong(12345, 2))  // 123.45
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dec128_str")
                            .decimalColumn("col", Decimal128.fromLong(-9999, 2))  // -99.99
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_dec128_str")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dec128_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_dec128_str ORDER BY ts",
                        """
                                col
                                123.45
                                -99.99
                                \n"""
                );
            }
        });
    }

    @Test
    public void testDecimal128ToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dec128_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dec128_vc")
                            .decimalColumn("col", Decimal128.fromLong(2500, 2))  // 25.00
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dec128_vc")
                            .decimalColumn("col", Decimal128.fromLong(-100, 2))  // -1.00
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_dec128_vc")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dec128_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_dec128_vc ORDER BY ts",
                        """
                                col
                                25.00
                                -1.00
                                \n"""
                );
            }
        });
    }

    @Test
    public void testDecimal256ToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dec256_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dec256_str")
                            .decimalColumn("col", Decimal256.fromLong(98765, 2))  // 987.65
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dec256_str")
                            .decimalColumn("col", Decimal256.fromLong(-54321, 2))  // -543.21
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_dec256_str")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dec256_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_dec256_str ORDER BY ts",
                        """
                                col
                                987.65
                                -543.21
                                \n"""
                );
            }
        });
    }

    @Test
    public void testDecimal256ToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dec256_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dec256_vc")
                            .decimalColumn("col", Decimal256.fromLong(42000, 3))  // 42.000
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dec256_vc")
                            .decimalColumn("col", Decimal256.fromLong(-7500, 3))  // -7.500
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_dec256_vc")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dec256_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_dec256_vc ORDER BY ts",
                        """
                                col
                                42.000
                                -7.500
                                \n"""
                );
            }
        });
    }

    @Test
    public void testDecimalToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dec_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dec_str")
                            .decimalColumn("col", new Decimal64(150, 2))  // 1.50
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dec_str")
                            .decimalColumn("col", new Decimal64(-375, 2))  // -3.75
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_dec_str")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dec_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_dec_str ORDER BY ts",
                        """
                                col
                                1.50
                                -3.75
                                \n"""
                );
            }
        });
    }

    @Test
    public void testDecimalToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dec_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dec_vc")
                            .decimalColumn("col", new Decimal64(250, 2))  // 2.50
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dec_vc")
                            .decimalColumn("col", new Decimal64(-100, 2))  // -1.00
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_dec_vc")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dec_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_dec_vc ORDER BY ts",
                        """
                                col
                                2.50
                                -1.00
                                \n"""
                );
            }
        });
    }

    @Test
    public void testDoubleToIntColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dbl_int (" +
                        "col INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dbl_int")
                            .doubleColumn("col", 42.0)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dbl_int")
                            .doubleColumn("col", -7.0)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_dbl_int")
                            .doubleColumn("col", 0.0)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dbl_int");
                serverMain.assertSql(
                        "SELECT col FROM tc_dbl_int ORDER BY ts",
                        """
                                col
                                42
                                -7
                                0
                                """
                );
            }
        });
    }

    @Test
    public void testDoubleToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dbl_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dbl_str")
                            .doubleColumn("col", 3.14)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dbl_str")
                            .doubleColumn("col", -2.5)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dbl_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_dbl_str ORDER BY ts",
                        """
                                col
                                3.14
                                -2.5
                                """
                );
            }
        });
    }

    @Test
    public void testDoubleToSymbolColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dbl_sym (" +
                        "col SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dbl_sym")
                            .doubleColumn("col", 1.5)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dbl_sym")
                            .doubleColumn("col", 1.5)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_dbl_sym")
                            .doubleColumn("col", 2.75)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dbl_sym");
                serverMain.assertSql(
                        "SELECT col FROM tc_dbl_sym ORDER BY ts",
                        """
                                col
                                1.5
                                1.5
                                2.75
                                """
                );
                serverMain.assertSql(
                        "SELECT count_distinct(col) FROM tc_dbl_sym",
                        "count_distinct\n2\n"
                );
            }
        });
    }

    @Test
    public void testDoubleToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_dbl_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_dbl_vc")
                            .doubleColumn("col", 99.9)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_dbl_vc")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_dbl_vc")
                            .doubleColumn("col", -0.5)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_dbl_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_dbl_vc ORDER BY ts",
                        "col\n99.9\n\n-0.5\n"
                );
            }
        });
    }

    @Test
    public void testGeoHashToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_geo_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;
                    QwpTableBuffer buf = wsSender.getTableBuffer("tc_geo_str");
                    QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("col", TYPE_GEOHASH, true);

                    // 5-bit precision, value 22 = 0b10110 → binary string "10110"
                    col.addGeoHash(0b10110L, 5);
                    sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                    col.addNull();
                    sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                    // 5-bit precision, value 31 = 0b11111 → binary string "11111"
                    col.addGeoHash(0b11111L, 5);
                    sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_geo_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_geo_str ORDER BY ts",
                        """
                                col
                                10110
                                \n11111
                                """
                );
            }
        });
    }

    @Test
    public void testGeoHashToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_geo_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;
                    QwpTableBuffer buf = wsSender.getTableBuffer("tc_geo_vc");
                    QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("col", TYPE_GEOHASH, false);

                    // 5-bit precision, value 1 = 0b00001 → binary string "00001"
                    col.addGeoHash(0b00001L, 5);
                    sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                    // 5-bit precision, value 0 = 0b00000 → binary string "00000"
                    col.addGeoHash(0b00000L, 5);
                    sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_geo_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_geo_vc ORDER BY ts",
                        """
                                col
                                00001
                                00000
                                """
                );
            }
        });
    }

    @Test
    public void testLongToDoubleColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_long_dbl (" +
                        "col DOUBLE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_long_dbl")
                            .longColumn("col", 42L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_long_dbl")
                            .longColumn("col", -100L)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_long_dbl")
                            .longColumn("col", 0L)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_long_dbl");
                serverMain.assertSql(
                        "SELECT col FROM tc_long_dbl ORDER BY ts",
                        """
                                col
                                42.0
                                -100.0
                                0.0
                                """
                );
            }
        });
    }

    @Test
    public void testLongToShortColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_long_short (" +
                        "col SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_long_short")
                            .longColumn("col", 100L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_long_short")
                            .longColumn("col", -200L)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_long_short")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_long_short");
                serverMain.assertSql(
                        "SELECT col FROM tc_long_short ORDER BY ts",
                        """
                                col
                                100
                                -200
                                0
                                """
                );
            }
        });
    }

    @Test
    public void testLongToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_long_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_long_str")
                            .longColumn("col", 12345L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_long_str")
                            .longColumn("col", -67890L)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_long_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_long_str ORDER BY ts",
                        """
                                col
                                12345
                                -67890
                                """
                );
            }
        });
    }

    @Test
    public void testLongToSymbolColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_long_sym (" +
                        "col SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_long_sym")
                            .longColumn("col", 1L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_long_sym")
                            .longColumn("col", 2L)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_long_sym")
                            .longColumn("col", 1L)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_long_sym");
                serverMain.assertSql(
                        "SELECT col FROM tc_long_sym ORDER BY ts",
                        """
                                col
                                1
                                2
                                1
                                """
                );
                serverMain.assertSql(
                        "SELECT count_distinct(col) FROM tc_long_sym",
                        "count_distinct\n2\n"
                );
            }
        });
    }

    @Test
    public void testLongToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_long_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_long_vc")
                            .longColumn("col", 999L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_long_vc")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_long_vc")
                            .longColumn("col", -1L)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_long_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_long_vc ORDER BY ts",
                        "col\n999\n\n-1\n"
                );
            }
        });
    }

    @Test
    public void testStringToBooleanColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_str_bool (" +
                        "col BOOLEAN, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_str_bool")
                            .stringColumn("col", "true")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_str_bool")
                            .stringColumn("col", "false")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_str_bool")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_str_bool");
                serverMain.assertSql(
                        "SELECT col FROM tc_str_bool ORDER BY ts",
                        """
                                col
                                true
                                false
                                false
                                """
                );
            }
        });
    }

    @Test
    public void testStringToIntColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_str_int (" +
                        "col INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_str_int")
                            .stringColumn("col", "42")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_str_int")
                            .stringColumn("col", "-7")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_str_int")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_str_int");
                serverMain.assertSql(
                        "SELECT col FROM tc_str_int ORDER BY ts",
                        """
                                col
                                42
                                -7
                                null
                                """
                );
            }
        });
    }

    @Test
    public void testStringToLongColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_str_long (" +
                        "col LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_str_long")
                            .stringColumn("col", "1000000")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_str_long")
                            .stringColumn("col", "-999")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_str_long");
                serverMain.assertSql(
                        "SELECT col FROM tc_str_long ORDER BY ts",
                        """
                                col
                                1000000
                                -999
                                """
                );
            }
        });
    }

    @Test
    public void testStringToSymbolColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_str_sym (" +
                        "col SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_str_sym")
                            .stringColumn("col", "alpha")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_str_sym")
                            .stringColumn("col", "beta")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_str_sym")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("tc_str_sym")
                            .stringColumn("col", "alpha")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_str_sym");
                serverMain.assertSql(
                        "SELECT col FROM tc_str_sym ORDER BY ts",
                        """
                                col
                                alpha
                                beta
                                \nalpha
                                """
                );
                serverMain.assertSql(
                        "SELECT count_distinct(col) FROM tc_str_sym",
                        "count_distinct\n2\n"
                );
            }
        });
    }

    @Test
    public void testStringToTimestampColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_str_ts (" +
                        "col TIMESTAMP, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_str_ts")
                            .stringColumn("col", "2024-01-15T10:30:00.000000Z")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_str_ts")
                            .stringColumn("col", "2023-06-01T00:00:00.000000Z")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_str_ts")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_str_ts");
                serverMain.assertSql(
                        "SELECT col FROM tc_str_ts ORDER BY ts",
                        """
                                col
                                2024-01-15T10:30:00.000000Z
                                2023-06-01T00:00:00.000000Z
                                \n"""
                );
            }
        });
    }

    @Test
    public void testStringToUuidColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_str_uuid (" +
                        "col UUID, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_str_uuid")
                            .stringColumn("col", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_str_uuid")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_str_uuid")
                            .stringColumn("col", "550e8400-e29b-41d4-a716-446655440000")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_str_uuid");
                serverMain.assertSql(
                        "SELECT col FROM tc_str_uuid ORDER BY ts",
                        """
                                col
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                                \n550e8400-e29b-41d4-a716-446655440000
                                """
                );
            }
        });
    }

    @Test
    public void testSymbolToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_sym_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_sym_str")
                            .symbol("col", "hello")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_sym_str")
                            .symbol("col", "world")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_sym_str")
                            .symbol("col", "hello")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_sym_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_sym_str ORDER BY ts",
                        """
                                col
                                hello
                                world
                                hello
                                """
                );
            }
        });
    }

    @Test
    public void testSymbolToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_sym_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    sender.table("tc_sym_vc")
                            .symbol("col", "foo")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_sym_vc")
                            .symbol("col", "bar")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("tc_sym_vc")
                            .symbol("col", "foo")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_sym_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_sym_vc ORDER BY ts",
                        """
                                col
                                foo
                                bar
                                foo
                                """
                );
            }
        });
    }

    @Test
    public void testTimestampToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_ts_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    // 2024-01-01T00:00:00Z in micros
                    sender.table("tc_ts_str")
                            .timestampColumn("col", 1_704_067_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    // 2024-06-15T11:30:00Z in micros
                    sender.table("tc_ts_str")
                            .timestampColumn("col", 1_718_451_000_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_ts_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_ts_str ORDER BY ts",
                        """
                                col
                                2024-01-01T00:00:00.000Z
                                2024-06-15T11:30:00.000Z
                                """
                );
            }
        });
    }

    @Test
    public void testTimestampToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_ts_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    // 2024-01-01T00:00:00Z in micros
                    sender.table("tc_ts_vc")
                            .timestampColumn("col", 1_704_067_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("tc_ts_vc")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    // 2023-12-25T15:00:00Z in micros
                    sender.table("tc_ts_vc")
                            .timestampColumn("col", 1_703_516_400_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_ts_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_ts_vc ORDER BY ts",
                        """
                                col
                                2024-01-01T00:00:00.000Z
                                \n2023-12-25T15:00:00.000Z
                                """
                );
            }
        });
    }

    @Test
    public void testUuidToStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_uuid_str (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;

                    // UUID: a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                    wsSender.table("tc_uuid_str")
                            .uuidColumn("col", 0xbb6d6bb9bd380a11L, 0xa0eebc999c0b4ef8L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    wsSender.table("tc_uuid_str")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    // UUID: 550e8400-e29b-41d4-a716-446655440000
                    wsSender.table("tc_uuid_str")
                            .uuidColumn("col", 0xa716446655440000L, 0x550e8400e29b41d4L)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_uuid_str");
                serverMain.assertSql(
                        "SELECT col FROM tc_uuid_str ORDER BY ts",
                        """
                                col
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                                
                                550e8400-e29b-41d4-a716-446655440000
                                """
                );
            }
        });
    }

    @Test
    public void testUuidToVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE tc_uuid_vc (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + httpPort + ";")) {
                    QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;

                    // UUID: 12345678-1234-5678-1234-567812345678
                    wsSender.table("tc_uuid_vc")
                            .uuidColumn("col", 0x1234567812345678L, 0x1234567812345678L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    // UUID: 00000000-0000-0000-0000-000000000001
                    wsSender.table("tc_uuid_vc")
                            .uuidColumn("col", 1L, 0L)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("tc_uuid_vc");
                serverMain.assertSql(
                        "SELECT col FROM tc_uuid_vc ORDER BY ts",
                        """
                                col
                                12345678-1234-5678-1234-567812345678
                                00000000-0000-0000-0000-000000000001
                                """
                );
            }
        });
    }
}
