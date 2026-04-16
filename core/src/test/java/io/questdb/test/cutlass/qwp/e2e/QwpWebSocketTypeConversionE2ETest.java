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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_GEOHASH;
import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_LONG;

/**
 * End-to-end tests for QWP WebSocket type conversions.
 * <p>
 * Each test pre-creates a table with a specific column type, then sends data
 * with a different wire type via the {@link Sender} API. The server converts
 * the wire type to the column type, and we verify the result with SQL assertions.
 * <p>
 * All tests use interleaved null and non-null rows (non-null, null, non-null,
 * null, non-null) to thoroughly test the omit-column-implies-null behavior
 * during type conversion.
 */
public class QwpWebSocketTypeConversionE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testBooleanToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_bool_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_bool_str")
                        .boolColumn("col", true)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_bool_str")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_bool_str")
                        .boolColumn("col", false)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_bool_str")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_bool_str")
                        .boolColumn("col", true)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_bool_str ORDER BY ts",
                    "col\ntrue\nfalse\nfalse\nfalse\ntrue\n"
            );
        });
    }

    @Test
    public void testBooleanToNumericColumns() throws Exception {
        runInContext((port) -> {
            execute("""
                    CREATE TABLE tc_bool_num (
                        b BYTE, s SHORT, i INT, l LONG, f FLOAT, d DOUBLE,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_bool_num")
                        .boolColumn("b", true)
                        .boolColumn("s", true)
                        .boolColumn("i", true)
                        .boolColumn("l", true)
                        .boolColumn("f", true)
                        .boolColumn("d", true)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                // Omit all columns → null
                sender.table("tc_bool_num")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_bool_num")
                        .boolColumn("b", false)
                        .boolColumn("s", false)
                        .boolColumn("i", false)
                        .boolColumn("l", false)
                        .boolColumn("f", false)
                        .boolColumn("d", false)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                // Omit all columns → null
                sender.table("tc_bool_num")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_bool_num")
                        .boolColumn("b", true)
                        .boolColumn("s", true)
                        .boolColumn("i", true)
                        .boolColumn("l", true)
                        .boolColumn("f", true)
                        .boolColumn("d", true)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            // Omitted boolean columns send false (0), not SQL NULL,
            // so omitted rows produce 0/0.0 across all numeric types.
            assertSql(
                    "SELECT b, s, i, l, f, d FROM tc_bool_num ORDER BY ts",
                    """
                            b\ts\ti\tl\tf\td
                            1\t1\t1\t1\t1.0\t1.0
                            0\t0\t0\t0\t0.0\t0.0
                            0\t0\t0\t0\t0.0\t0.0
                            0\t0\t0\t0\t0.0\t0.0
                            1\t1\t1\t1\t1.0\t1.0
                            """
            );
        });
    }

    @Test
    public void testBooleanToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_bool_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_bool_vc")
                        .boolColumn("col", false)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_bool_vc")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_bool_vc")
                        .boolColumn("col", true)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_bool_vc")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_bool_vc")
                        .boolColumn("col", false)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_bool_vc ORDER BY ts",
                    "col\nfalse\nfalse\ntrue\nfalse\nfalse\n"
            );
        });
    }

    @Test
    public void testCharToCharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_char_char (" +
                    "col CHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;

                wsSender.table("tc_char_char")
                        .charColumn("col", 'A')
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                wsSender.table("tc_char_char")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                wsSender.table("tc_char_char")
                        .charColumn("col", 'Z')
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                wsSender.table("tc_char_char")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                wsSender.table("tc_char_char")
                        .charColumn("col", 'm')
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_char_char ORDER BY ts",
                    "col\nA\n\nZ\n\nm\n"
            );
        });
    }

    @Test
    public void testDecimal128ToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec128_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec128_str")
                        .decimalColumn("col", Decimal128.fromLong(12_345, 2))  // 123.45
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec128_str")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec128_str")
                        .decimalColumn("col", Decimal128.fromLong(-9999, 2))  // -99.99
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec128_str")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec128_str")
                        .decimalColumn("col", Decimal128.fromLong(1, 2))  // 0.01
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec128_str ORDER BY ts",
                    "col\n123.45\n\n-99.99\n\n0.01\n"
            );
        });
    }

    @Test
    public void testDecimal128ToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec128_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec128_vc")
                        .decimalColumn("col", Decimal128.fromLong(2500, 2))  // 25.00
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec128_vc")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec128_vc")
                        .decimalColumn("col", Decimal128.fromLong(-100, 2))  // -1.00
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec128_vc")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec128_vc")
                        .decimalColumn("col", Decimal128.fromLong(5050, 2))  // 50.50
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec128_vc ORDER BY ts",
                    "col\n25.00\n\n-1.00\n\n50.50\n"
            );
        });
    }

    @Test
    public void testDecimal256ToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec256_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec256_str")
                        .decimalColumn("col", Decimal256.fromLong(98_765, 2))  // 987.65
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec256_str")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec256_str")
                        .decimalColumn("col", Decimal256.fromLong(-54_321, 2))  // -543.21
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec256_str")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec256_str")
                        .decimalColumn("col", Decimal256.fromLong(11_111, 2))  // 111.11
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec256_str ORDER BY ts",
                    "col\n987.65\n\n-543.21\n\n111.11\n"
            );
        });
    }

    @Test
    public void testDecimal256ToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec256_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec256_vc")
                        .decimalColumn("col", Decimal256.fromLong(42_000, 3))  // 42.000
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec256_vc")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec256_vc")
                        .decimalColumn("col", Decimal256.fromLong(-7500, 3))  // -7.500
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec256_vc")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec256_vc")
                        .decimalColumn("col", Decimal256.fromLong(99_999, 3))  // 99.999
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec256_vc ORDER BY ts",
                    "col\n42.000\n\n-7.500\n\n99.999\n"
            );
        });
    }

    @Test
    public void testDecimalToDecimal128Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec_dec128 (" +
                    "col DECIMAL(34, 8), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec_dec128")
                        .decimalColumn("col", Decimal128.fromLong(12_345, 2))  // 123.45
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec128")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec128")
                        .decimalColumn("col", Decimal128.fromLong(-9999, 2))  // -99.99
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec128")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec128")
                        .decimalColumn("col", Decimal128.fromLong(1, 2))  // 0.01
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec_dec128 ORDER BY ts",
                    "col\n123.45000000\n\n-99.99000000\n\n0.01000000\n"
            );
        });
    }

    @Test
    public void testDecimalToDecimal256Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec_dec256 (" +
                    "col DECIMAL(64, 16), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec_dec256")
                        .decimalColumn("col", Decimal256.fromLong(98_765, 2))  // 987.65
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec256")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec256")
                        .decimalColumn("col", Decimal256.fromLong(-54_321, 2))  // -543.21
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec256")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec256")
                        .decimalColumn("col", Decimal256.fromLong(11_111, 2))  // 111.11
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec_dec256 ORDER BY ts",
                    "col\n987.6500000000000000\n\n-543.2100000000000000\n\n111.1100000000000000\n"
            );
        });
    }

    @Test
    public void testDecimalToDecimal64Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec_dec64 (" +
                    "col DECIMAL(16, 4), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec_dec64")
                        .decimalColumn("col", new Decimal64(150, 2))  // 1.50
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec64")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec64")
                        .decimalColumn("col", new Decimal64(-375, 2))  // -3.75
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec64")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec_dec64")
                        .decimalColumn("col", new Decimal64(25, 2))  // 0.25
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec_dec64 ORDER BY ts",
                    "col\n1.5000\n\n-3.7500\n\n0.2500\n"
            );
        });
    }

    @Test
    public void testDecimalToSmallDecimalColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec_smdec (" +
                    "col DECIMAL(8, 2), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec_smdec")
                        .decimalColumn("col", new Decimal64(150, 2))  // 1.50
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec_smdec")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec_smdec")
                        .decimalColumn("col", new Decimal64(-375, 2))  // -3.75
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec_smdec")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec_smdec")
                        .decimalColumn("col", new Decimal64(25, 2))  // 0.25
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec_smdec ORDER BY ts",
                    "col\n1.50\n\n-3.75\n\n0.25\n"
            );
        });
    }

    @Test
    public void testDecimalToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec_str")
                        .decimalColumn("col", new Decimal64(150, 2))  // 1.50
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec_str")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec_str")
                        .decimalColumn("col", new Decimal64(-375, 2))  // -3.75
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec_str")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec_str")
                        .decimalColumn("col", new Decimal64(25, 2))  // 0.25
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec_str ORDER BY ts",
                    "col\n1.50\n\n-3.75\n\n0.25\n"
            );
        });
    }

    @Test
    public void testDecimalToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dec_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dec_vc")
                        .decimalColumn("col", new Decimal64(250, 2))  // 2.50
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dec_vc")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dec_vc")
                        .decimalColumn("col", new Decimal64(-100, 2))  // -1.00
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dec_vc")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dec_vc")
                        .decimalColumn("col", new Decimal64(888, 2))  // 8.88
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dec_vc ORDER BY ts",
                    "col\n2.50\n\n-1.00\n\n8.88\n"
            );
        });
    }

    @Test
    public void testDoubleToDecimal128Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dbl_dec128 (" +
                    "col DECIMAL(34, 8), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dbl_dec128")
                        .doubleColumn("col", 42.5)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec128")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec128")
                        .doubleColumn("col", -7.25)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec128")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec128")
                        .doubleColumn("col", 100.0)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dbl_dec128 ORDER BY ts",
                    "col\n42.50000000\n\n-7.25000000\n\n100.00000000\n"
            );
        });
    }

    @Test
    public void testDoubleToDecimal256Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dbl_dec256 (" +
                    "col DECIMAL(64, 16), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dbl_dec256")
                        .doubleColumn("col", 42.5)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec256")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec256")
                        .doubleColumn("col", -7.25)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec256")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec256")
                        .doubleColumn("col", 100.0)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dbl_dec256 ORDER BY ts",
                    "col\n42.5000000000000000\n\n-7.2500000000000000\n\n100.0000000000000000\n"
            );
        });
    }

    @Test
    public void testDoubleToDecimal64Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dbl_dec64 (" +
                    "col DECIMAL(16, 4), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dbl_dec64")
                        .doubleColumn("col", 42.5)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec64")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec64")
                        .doubleColumn("col", -7.25)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec64")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dbl_dec64")
                        .doubleColumn("col", 100.0)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dbl_dec64 ORDER BY ts",
                    "col\n42.5000\n\n-7.2500\n\n100.0000\n"
            );
        });
    }

    @Test
    public void testDoubleToFloatColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dbl_flt (" +
                    "col FLOAT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dbl_flt")
                        .doubleColumn("col", 42.5)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dbl_flt")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dbl_flt")
                        .doubleColumn("col", -7.25)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dbl_flt")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dbl_flt")
                        .doubleColumn("col", 3.14)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dbl_flt ORDER BY ts",
                    "col\n42.5\nnull\n-7.25\nnull\n3.14\n"
            );
        });
    }

    @Test
    public void testDoubleToIntColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dbl_int (" +
                    "col INT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dbl_int")
                        .doubleColumn("col", 42.0)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dbl_int")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dbl_int")
                        .doubleColumn("col", -7.0)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dbl_int")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dbl_int")
                        .doubleColumn("col", 0.0)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dbl_int ORDER BY ts",
                    "col\n42\nnull\n-7\nnull\n0\n"
            );
        });
    }

    @Test
    public void testDoubleToSmallDecimalColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dbl_smdec (" +
                    "col DECIMAL(8, 2), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dbl_smdec")
                        .doubleColumn("col", 42.5)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dbl_smdec")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dbl_smdec")
                        .doubleColumn("col", -7.25)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dbl_smdec")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dbl_smdec")
                        .doubleColumn("col", 100.0)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dbl_smdec ORDER BY ts",
                    "col\n42.50\n\n-7.25\n\n100.00\n"
            );
        });
    }

    @Test
    public void testDoubleToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dbl_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dbl_str")
                        .doubleColumn("col", 3.14)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dbl_str")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dbl_str")
                        .doubleColumn("col", -2.5)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dbl_str")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dbl_str")
                        .doubleColumn("col", 100.0)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dbl_str ORDER BY ts",
                    "col\n3.14\n\n-2.5\n\n100.0\n"
            );
        });
    }

    @Test
    public void testDoubleToSymbolColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dbl_sym (" +
                    "col SYMBOL, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dbl_sym")
                        .doubleColumn("col", 1.5)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dbl_sym")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dbl_sym")
                        .doubleColumn("col", 2.75)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dbl_sym")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dbl_sym")
                        .doubleColumn("col", 1.5)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dbl_sym ORDER BY ts",
                    "col\n1.5\n\n2.75\n\n1.5\n"
            );
            assertSql(
                    "SELECT count_distinct(col) FROM tc_dbl_sym",
                    "count_distinct\n2\n"
            );
        });
    }

    @Test
    public void testDoubleToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_dbl_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_dbl_vc")
                        .doubleColumn("col", 99.9)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_dbl_vc")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_dbl_vc")
                        .doubleColumn("col", -0.5)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_dbl_vc")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_dbl_vc")
                        .doubleColumn("col", 3.14)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_dbl_vc ORDER BY ts",
                    "col\n99.9\n\n-0.5\n\n3.14\n"
            );
        });
    }

    @Test
    public void testGeoHashToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_geo_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;
                QwpTableBuffer buf = wsSender.getTableBuffer("tc_geo_str");
                QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("col", TYPE_GEOHASH, true);

                // 5-bit precision, value 22 = 0b10110
                col.addGeoHash(0b10110L, 5);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                col.addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                // 5-bit precision, value 31 = 0b11111
                col.addGeoHash(0b11111L, 5);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);

                col.addNull();
                sender.at(1_000_000_000_003L, ChronoUnit.MICROS);

                // 5-bit precision, value 10 = 0b01010
                col.addGeoHash(0b01010L, 5);
                sender.at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_geo_str ORDER BY ts",
                    "col\n10110\n\n11111\n\n01010\n"
            );
        });
    }

    @Test
    public void testGeoHashToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_geo_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;
                QwpTableBuffer buf = wsSender.getTableBuffer("tc_geo_vc");
                QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("col", TYPE_GEOHASH, true);

                // 5-bit precision, value 1 = 0b00001
                col.addGeoHash(0b00001L, 5);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                col.addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                // 5-bit precision, value 0 = 0b00000
                col.addGeoHash(0b00000L, 5);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);

                col.addNull();
                sender.at(1_000_000_000_003L, ChronoUnit.MICROS);

                // 5-bit precision, value 21 = 0b10101
                col.addGeoHash(0b10101L, 5);
                sender.at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_geo_vc ORDER BY ts",
                    "col\n00001\n\n00000\n\n10101\n"
            );
        });
    }

    @Test
    public void testLongToDecimal128Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_dec128 (" +
                    "col DECIMAL(34, 8), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_long_dec128")
                        .longColumn("col", 42L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_dec128")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_long_dec128")
                        .longColumn("col", -100L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_dec128")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_long_dec128")
                        .longColumn("col", 0L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_dec128 ORDER BY ts",
                    "col\n42.00000000\n\n-100.00000000\n\n0.00000000\n"
            );
        });
    }

    @Test
    public void testLongToDecimal256Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_dec256 (" +
                    "col DECIMAL(64, 16), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_long_dec256")
                        .longColumn("col", 42L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_dec256")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_long_dec256")
                        .longColumn("col", -100L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_dec256")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_long_dec256")
                        .longColumn("col", 0L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_dec256 ORDER BY ts",
                    "col\n42.0000000000000000\n\n-100.0000000000000000\n\n0.0000000000000000\n"
            );
        });
    }

    @Test
    public void testLongToDecimal64Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_dec64 (" +
                    "col DECIMAL(16, 4), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_long_dec64")
                        .longColumn("col", 42L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_dec64")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_long_dec64")
                        .longColumn("col", -100L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_dec64")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_long_dec64")
                        .longColumn("col", 0L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_dec64 ORDER BY ts",
                    "col\n42.0000\n\n-100.0000\n\n0.0000\n"
            );
        });
    }

    @Test
    public void testLongToDesignatedTimestampMicroColumn() throws Exception {
        // Verify that sending the designated timestamp as a named LONG column
        // to a regular TIMESTAMP (microsecond) column stores values as-is,
        // with no spurious nanos-to-micros conversion.
        runInContext((port) -> {
            String table = "tc_long_designated_ts_micro";
            execute("CREATE TABLE " + table + " (" +
                    "value LONG, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;
                QwpTableBuffer buf = wsSender.getTableBuffer(table);
                QwpTableBuffer.ColumnBuffer valueCol = buf.getOrCreateColumn("value", TYPE_LONG, true);
                QwpTableBuffer.ColumnBuffer tsCol = buf.getOrCreateColumn("ts", TYPE_LONG, true);

                // 2022-02-25T00:00:00.000000Z in micros
                valueCol.addLong(42L);
                tsCol.addLong(1_645_747_200_000_000L);
                sender.atNow();

                // 2022-02-25T00:00:01.000000Z in micros
                valueCol.addLong(99L);
                tsCol.addLong(1_645_747_201_000_000L);
                sender.atNow();
            }

            drainWalQueue();
            assertSql(
                    "SELECT value, ts FROM " + table + " ORDER BY ts",
                    """
                            value\tts
                            42\t2022-02-25T00:00:00.000000Z
                            99\t2022-02-25T00:00:01.000000Z
                            """);
        });
    }

    @Test
    public void testLongToDesignatedTimestampNanoColumn() throws Exception {
        // Regression test: when a client sends the designated timestamp as a named
        // LONG column (wire type TYPE_LONG) to a TIMESTAMP_NS column, the min/max
        // computation must not apply micros-to-nanos conversion (x1000).
        runInContext((port) -> {
            String table = "tc_long_designated_ts_nano";
            execute("CREATE TABLE " + table + " (" +
                    "value LONG, " +
                    "ts TIMESTAMP_NS" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;
                QwpTableBuffer buf = wsSender.getTableBuffer(table);
                QwpTableBuffer.ColumnBuffer valueCol = buf.getOrCreateColumn("value", TYPE_LONG, true);
                QwpTableBuffer.ColumnBuffer tsCol = buf.getOrCreateColumn("ts", TYPE_LONG, true);

                // 2022-02-25T00:00:00.000000000Z in nanos
                valueCol.addLong(42L);
                tsCol.addLong(1_645_747_200_000_000_000L);
                sender.atNow();

                // 2022-02-25T00:00:01.000000000Z in nanos
                valueCol.addLong(99L);
                tsCol.addLong(1_645_747_201_000_000_000L);
                sender.atNow();
            }

            drainWalQueue();
            // The stored timestamps must be the literal nanosecond values sent.
            // BUG: the appender treats the LONG wire type as microseconds because
            // wireIsNanos is false, then tries to multiply by 1000, which overflows
            // for realistic nanosecond values and drops the rows.
            assertSql(
                    "SELECT value, ts FROM " + table + " ORDER BY ts",
                    """
                            value\tts
                            42\t2022-02-25T00:00:00.000000000Z
                            99\t2022-02-25T00:00:01.000000000Z
                            """);
        });
    }

    @Test
    public void testLongToDoubleColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_dbl (" +
                    "col DOUBLE, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_long_dbl")
                        .longColumn("col", 42L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_dbl")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_long_dbl")
                        .longColumn("col", -100L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_dbl")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_long_dbl")
                        .longColumn("col", 0L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_dbl ORDER BY ts",
                    "col\n42.0\nnull\n-100.0\nnull\n0.0\n"
            );
        });
    }

    @Test
    public void testLongToShortColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_short (" +
                    "col SHORT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_long_short")
                        .longColumn("col", 100L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_short")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_long_short")
                        .longColumn("col", -200L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_short")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_long_short")
                        .longColumn("col", 50L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_short ORDER BY ts",
                    "col\n100\n0\n-200\n0\n50\n"
            );
        });
    }

    @Test
    public void testLongToSmallDecimalColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_smdec (" +
                    "col DECIMAL(8, 2), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_long_smdec")
                        .longColumn("col", 42L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_smdec")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_long_smdec")
                        .longColumn("col", -100L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_smdec")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_long_smdec")
                        .longColumn("col", 0L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_smdec ORDER BY ts",
                    "col\n42.00\n\n-100.00\n\n0.00\n"
            );
        });
    }

    @Test
    public void testLongToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_long_str")
                        .longColumn("col", 12_345L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_str")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_long_str")
                        .longColumn("col", -67_890L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_str")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_long_str")
                        .longColumn("col", 0L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_str ORDER BY ts",
                    "col\n12345\n\n-67890\n\n0\n"
            );
        });
    }

    @Test
    public void testLongToSymbolColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_sym (" +
                    "col SYMBOL, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_long_sym")
                        .longColumn("col", 1L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_sym")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_long_sym")
                        .longColumn("col", 2L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_sym")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_long_sym")
                        .longColumn("col", 1L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_sym ORDER BY ts",
                    "col\n1\n\n2\n\n1\n"
            );
            assertSql(
                    "SELECT count_distinct(col) FROM tc_long_sym",
                    "count_distinct\n2\n"
            );
        });
    }

    @Test
    public void testLongToTimestampColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_ts (" +
                    "col TIMESTAMP, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                // 2024-01-01T00:00:00Z in micros
                sender.table("tc_long_ts")
                        .longColumn("col", 1_704_067_200_000_000L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_ts")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                // 2024-06-15T12:00:00Z in micros
                sender.table("tc_long_ts")
                        .longColumn("col", 1_718_452_800_000_000L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_ts")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                // 2023-01-01T00:00:00Z in micros
                sender.table("tc_long_ts")
                        .longColumn("col", 1_672_531_200_000_000L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_ts ORDER BY ts",
                    "col\n2024-01-01T00:00:00.000000Z\n\n2024-06-15T12:00:00.000000Z\n\n2023-01-01T00:00:00.000000Z\n"
            );
        });
    }

    @Test
    public void testLongToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_long_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_long_vc")
                        .longColumn("col", 999L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_long_vc")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_long_vc")
                        .longColumn("col", -1L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_long_vc")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_long_vc")
                        .longColumn("col", 42L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_long_vc ORDER BY ts",
                    "col\n999\n\n-1\n\n42\n"
            );
        });
    }

    @Test
    public void testStringToBooleanColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_bool (" +
                    "col BOOLEAN, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_bool")
                        .stringColumn("col", "true")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_bool")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_bool")
                        .stringColumn("col", "false")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_bool")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_bool")
                        .stringColumn("col", "true")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_bool ORDER BY ts",
                    "col\ntrue\nfalse\nfalse\nfalse\ntrue\n"
            );
        });
    }

    @Test
    public void testStringToCharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_char (" +
                    "col CHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_char")
                        .stringColumn("col", "A")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_char")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_char")
                        .stringColumn("col", "Z")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_char")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_char")
                        .stringColumn("col", "x")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_char ORDER BY ts",
                    "col\nA\n\nZ\n\nx\n"
            );
        });
    }

    @Test
    public void testStringToDecimal128Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_dec128 (" +
                    "col DECIMAL(34, 8), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_dec128")
                        .stringColumn("col", "42.5")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_dec128")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_dec128")
                        .stringColumn("col", "-7.25")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_dec128")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_dec128")
                        .stringColumn("col", "100.0")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_dec128 ORDER BY ts",
                    "col\n42.50000000\n\n-7.25000000\n\n100.00000000\n"
            );
        });
    }

    @Test
    public void testStringToDecimal256Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_dec256 (" +
                    "col DECIMAL(64, 16), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_dec256")
                        .stringColumn("col", "42.5")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_dec256")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_dec256")
                        .stringColumn("col", "-7.25")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_dec256")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_dec256")
                        .stringColumn("col", "100.0")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_dec256 ORDER BY ts",
                    "col\n42.5000000000000000\n\n-7.2500000000000000\n\n100.0000000000000000\n"
            );
        });
    }

    @Test
    public void testStringToDecimal64Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_dec64 (" +
                    "col DECIMAL(16, 4), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_dec64")
                        .stringColumn("col", "42.5")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_dec64")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_dec64")
                        .stringColumn("col", "-7.25")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_dec64")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_dec64")
                        .stringColumn("col", "100.0")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_dec64 ORDER BY ts",
                    "col\n42.5000\n\n-7.2500\n\n100.0000\n"
            );
        });
    }

    @Test
    public void testStringToGeoHashColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_geo (" +
                    "col GEOHASH(4c), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_geo")
                        .stringColumn("col", "u33d")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_geo")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_geo")
                        .stringColumn("col", "sp05")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_geo")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_geo")
                        .stringColumn("col", "9q8y")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_geo ORDER BY ts",
                    "col\nu33d\n\nsp05\n\n9q8y\n"
            );
        });
    }

    @Test
    public void testStringToIntColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_int (" +
                    "col INT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_int")
                        .stringColumn("col", "42")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_int")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_int")
                        .stringColumn("col", "-7")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_int")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_int")
                        .stringColumn("col", "0")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_int ORDER BY ts",
                    "col\n42\nnull\n-7\nnull\n0\n"
            );
        });
    }

    @Test
    public void testStringToLong256Column() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_l256 (" +
                    "col LONG256, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_l256")
                        .stringColumn("col", "0x4444444444444444333333333333333322222222222222221111111111111111")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_l256")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_l256")
                        .stringColumn("col", "0xdeadbeef00000000cafebabe0000000012345678000000009abcdef000000000")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_l256")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_l256")
                        .stringColumn("col", "0x0000000000000000000000000000000000000000000000000000000000000001")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_l256 ORDER BY ts",
                    """
                            col
                            0x4444444444444444333333333333333322222222222222221111111111111111
                            
                            0xdeadbeef00000000cafebabe0000000012345678000000009abcdef000000000
                            
                            0x01
                            """
            );
        });
    }

    @Test
    public void testStringToLongColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_long (" +
                    "col LONG, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_long")
                        .stringColumn("col", "1000000")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_long")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_long")
                        .stringColumn("col", "-999")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_long")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_long")
                        .stringColumn("col", "0")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_long ORDER BY ts",
                    "col\n1000000\nnull\n-999\nnull\n0\n"
            );
        });
    }

    @Test
    public void testStringToSmallDecimalColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_smdec (" +
                    "col DECIMAL(8, 2), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_smdec")
                        .stringColumn("col", "42.5")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_smdec")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_smdec")
                        .stringColumn("col", "-7.25")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_smdec")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_smdec")
                        .stringColumn("col", "100.0")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_smdec ORDER BY ts",
                    "col\n42.50\n\n-7.25\n\n100.00\n"
            );
        });
    }

    @Test
    public void testStringToSymbolColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_sym (" +
                    "col SYMBOL, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_sym")
                        .stringColumn("col", "alpha")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_sym")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_sym")
                        .stringColumn("col", "beta")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_sym")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_sym")
                        .stringColumn("col", "alpha")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_sym ORDER BY ts",
                    "col\nalpha\n\nbeta\n\nalpha\n"
            );
            assertSql(
                    "SELECT count_distinct(col) FROM tc_str_sym",
                    "count_distinct\n2\n"
            );
        });
    }

    @Test
    public void testStringToTimestampColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_ts (" +
                    "col TIMESTAMP, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_ts")
                        .stringColumn("col", "2024-01-15T10:30:00.000000Z")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_ts")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_ts")
                        .stringColumn("col", "2023-06-01T00:00:00.000000Z")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_ts")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_ts")
                        .stringColumn("col", "2025-12-31T23:59:59.000000Z")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_ts ORDER BY ts",
                    "col\n2024-01-15T10:30:00.000000Z\n\n2023-06-01T00:00:00.000000Z\n\n2025-12-31T23:59:59.000000Z\n"
            );
        });
    }

    @Test
    public void testStringToUuidColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_str_uuid (" +
                    "col UUID, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_str_uuid")
                        .stringColumn("col", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_str_uuid")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_str_uuid")
                        .stringColumn("col", "550e8400-e29b-41d4-a716-446655440000")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_str_uuid")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_str_uuid")
                        .stringColumn("col", "12345678-1234-5678-1234-567812345678")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_str_uuid ORDER BY ts",
                    "col\na0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n\n550e8400-e29b-41d4-a716-446655440000\n\n12345678-1234-5678-1234-567812345678\n"
            );
        });
    }

    @Test
    public void testSymbolToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_sym_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_sym_str")
                        .symbol("col", "hello")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_sym_str")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_sym_str")
                        .symbol("col", "world")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_sym_str")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_sym_str")
                        .symbol("col", "hello")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_sym_str ORDER BY ts",
                    "col\nhello\n\nworld\n\nhello\n"
            );
        });
    }

    @Test
    public void testSymbolToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_sym_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("tc_sym_vc")
                        .symbol("col", "foo")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_sym_vc")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("tc_sym_vc")
                        .symbol("col", "bar")
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_sym_vc")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                sender.table("tc_sym_vc")
                        .symbol("col", "foo")
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_sym_vc ORDER BY ts",
                    "col\nfoo\n\nbar\n\nfoo\n"
            );
        });
    }

    @Test
    public void testTimestampToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_ts_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                // 2024-01-01T00:00:00Z in micros
                sender.table("tc_ts_str")
                        .timestampColumn("col", 1_704_067_200_000_000L, ChronoUnit.MICROS)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("tc_ts_str")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                // 2024-06-15T11:30:00Z in micros
                sender.table("tc_ts_str")
                        .timestampColumn("col", 1_718_451_000_000_000L, ChronoUnit.MICROS)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.table("tc_ts_str")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                // 2023-01-01T00:00:00Z in micros
                sender.table("tc_ts_str")
                        .timestampColumn("col", 1_672_531_200_000_000L, ChronoUnit.MICROS)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_ts_str ORDER BY ts",
                    "col\n2024-01-01T00:00:00.000Z\n\n2024-06-15T11:30:00.000Z\n\n2023-01-01T00:00:00.000Z\n"
            );
        });
    }

    @Test
    public void testTimestampToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_ts_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
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

                sender.table("tc_ts_vc")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                // 2023-01-01T00:00:00Z in micros
                sender.table("tc_ts_vc")
                        .timestampColumn("col", 1_672_531_200_000_000L, ChronoUnit.MICROS)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_ts_vc ORDER BY ts",
                    "col\n2024-01-01T00:00:00.000Z\n\n2023-12-25T15:00:00.000Z\n\n2023-01-01T00:00:00.000Z\n"
            );
        });
    }

    @Test
    public void testUuidToStringColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_uuid_str (" +
                    "col STRING, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
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

                wsSender.table("tc_uuid_str")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                // UUID: 12345678-1234-5678-1234-567812345678
                wsSender.table("tc_uuid_str")
                        .uuidColumn("col", 0x1234567812345678L, 0x1234567812345678L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_uuid_str ORDER BY ts",
                    "col\na0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n\n550e8400-e29b-41d4-a716-446655440000\n\n12345678-1234-5678-1234-567812345678\n"
            );
        });
    }

    @Test
    public void testUuidToVarcharColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE tc_uuid_vc (" +
                    "col VARCHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;

                // UUID: 12345678-1234-5678-1234-567812345678
                wsSender.table("tc_uuid_vc")
                        .uuidColumn("col", 0x1234567812345678L, 0x1234567812345678L)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                wsSender.table("tc_uuid_vc")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                // UUID: 00000000-0000-0000-0000-000000000001
                wsSender.table("tc_uuid_vc")
                        .uuidColumn("col", 1L, 0L)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                wsSender.table("tc_uuid_vc")
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                // UUID: a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                wsSender.table("tc_uuid_vc")
                        .uuidColumn("col", 0xbb6d6bb9bd380a11L, 0xa0eebc999c0b4ef8L)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT col FROM tc_uuid_vc ORDER BY ts",
                    "col\n12345678-1234-5678-1234-567812345678\n\n00000000-0000-0000-0000-000000000001\n\na0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n"
            );
        });
    }
}
