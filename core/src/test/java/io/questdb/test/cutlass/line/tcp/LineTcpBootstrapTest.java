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
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.AbstractLineTcpSender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.LineTcpSenderV2;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.std.Decimal256;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.function.Consumer;

import static io.questdb.client.Sender.*;
import static io.questdb.test.tools.TestUtils.assertEventually;
import static org.junit.Assert.fail;

public class LineTcpBootstrapTest extends AbstractBootstrapTest {
    private static final int HOST = io.questdb.client.std.Numbers.parseIPv4("127.0.0.1");

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testArrayAtNow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2);
                     DoubleArray a1 = new DoubleArray(1, 1, 2, 1).setAll(1)) {
                    sender.table("test_array_at_now")
                            .symbol("x", "42i")
                            .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")
                            .longColumn("l1", 23_452_345)
                            .doubleArray("a1", a1)
                            .atNow();
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM test_array_at_now",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testArrayDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2);
                     DoubleArray a4 = new DoubleArray(1, 1, 2, 1).setAll(4);
                     DoubleArray a5 = new DoubleArray(3, 2, 1, 4, 1).setAll(5);
                     DoubleArray a6 = new DoubleArray(1, 3, 4, 2, 1, 1).setAll(6)) {
                    long ts = MicrosFormatUtils.parseTimestamp("2025-02-22T00:00:00Z");
                    double[] arr1d = createDoubleArray(5);
                    double[][] arr2d = createDoubleArray(2, 3);
                    double[][][] arr3d = createDoubleArray(1, 2, 3);
                    sender.table("test_array_double")
                            .symbol("x", "42i")
                            .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")
                            .longColumn("l1", 23_452_345)
                            .doubleArray("a1", arr1d)
                            .doubleArray("a2", arr2d)
                            .doubleArray("a3", arr3d)
                            .doubleArray("a4", a4)
                            .doubleArray("a5", a5)
                            .doubleArray("a6", a6)
                            .at(ts, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM test_array_double",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testAsciiFlagCorrectlySetForVarcharColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                CairoEngine engine = serverMain.getEngine();
                try (SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    engine.execute(
                            """
                                    CREATE TABLE 'betfairRunners' (
                                      id INT,
                                      runner VARCHAR,
                                      age BYTE,
                                      remarks SYMBOL CAPACITY 2048 CACHE INDEX CAPACITY 256,
                                      timestamp TIMESTAMP
                                    ) timestamp (timestamp) PARTITION BY MONTH WAL
                                    DEDUP UPSERT KEYS(timestamp, id);""",
                            sqlExecutionContext
                    );

                    try (Sender sender = Sender.fromConfig("http::addr=localhost:" + serverMain.getHttpServerPort() + ";")) {
                        for (int i = 0; i < 1; i++) {
                            sender.table("betfairRunners").symbol("remarks", "SAw,CkdRnUp&2").
                                    stringColumn("runner", "BallyMac Fifra")
                                    .longColumn("id", 548_738)
                                    .longColumn("age", 58)
                                    .at(MicrosFormatUtils.parseTimestamp("2024-06-30T00:00:00Z"), ChronoUnit.MICROS);
                            sender.table("betfairRunners").symbol("remarks", "Fcd-Ck1").
                                    stringColumn("runner", "BallyMac Fifra")
                                    .longColumn("id", 548_738)
                                    .longColumn("age", 58)
                                    .at(MicrosFormatUtils.parseTimestamp("2024-06-24T00:00:00Z"), ChronoUnit.MICROS);
                            sender.table("betfairRunners").symbol("remarks", "(R8) LdRnIn військові").
                                    stringColumn("runner", "BallyMac Fifra")
                                    .longColumn("id", 548_738)
                                    .longColumn("age", 58)
                                    .at(MicrosFormatUtils.parseTimestamp("2024-06-17T00:00:00Z"), ChronoUnit.MICROS);
                            sender.flush();
                        }
                    }

                    // server main already runs Apply2Wal job in the background. We have to wait for the table to catchup
                    try (RecordCursorFactory waitFact = engine.select("wal_tables where writertxn <> sequencertxn;", sqlExecutionContext)) {
                        assertEventually(() -> {
                            try {
                                try (RecordCursor cursor = waitFact.getCursor(sqlExecutionContext)) {
                                    Assert.assertFalse(cursor.hasNext());
                                }
                            } catch (SqlException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }

                    try (
                            RecordCursorFactory factory = engine.select("select distinct runner from betfairRunners", sqlExecutionContext);
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                    ) {
                        Assert.assertTrue(cursor.hasNext());
                        if (cursor.hasNext()) {
                            throw SqlException.$(0, "More than one result in record cursor. Should be one row after distinct query.");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBuilderPlainText_addressWithExplicitIpAndPort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    sender.table("test_builder_plain_text_explicit_ip_port").longColumn("my int field", 42).atNow();
                    sender.flush();
                }
                serverMain.awaitTable("test_builder_plain_text_explicit_ip_port");
            }
        });
    }

    @Test
    public void testBuilderPlainText_addressWithHostnameAndPort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = Sender.builder(Sender.Transport.TCP)
                        .address("localhost:" + port)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .build()) {
                    sender.table("test_builder_plain_text_hostname_port").longColumn("my int field", 42).atNow();
                    sender.flush();
                }
                serverMain.awaitTable("test_builder_plain_text_hostname_port");
            }
        });
    }

    @Test
    public void testBuilderPlainText_addressWithIpAndPort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                String address = "127.0.0.1:" + port;
                try (Sender sender = Sender.builder(Sender.Transport.TCP)
                        .address(address)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .build()) {
                    sender.table("test_builder_plain_text_ip_port").longColumn("my int field", 42).atNow();
                    sender.flush();
                }
                serverMain.awaitTable("test_builder_plain_text_ip_port");
            }
        });
    }

    @Test
    public void testCloseImpliesFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    sender.table("test_close_implies_flush").longColumn("my int field", 42).atNow();
                }
                serverMain.awaitTable("test_close_implies_flush");
            }
        });
    }

    @Test
    public void testConfString_autoFlushBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                String confString = "tcp::addr=localhost:" + port + ";auto_flush_bytes=1;protocol_version=2;";
                try (Sender sender = Sender.fromConfig(confString)) {
                    sender.table("test_conf_string_auto_flush_bytes").longColumn("my int field", 42).atNow();
                    sender.table("test_conf_string_auto_flush_bytes").longColumn("my int field", 42).atNow();
                    // assert before closing since close always flushes
                    serverMain.awaitTable("test_conf_string_auto_flush_bytes");
                }
            }
        });
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedInstantV1() throws Exception {
        // Designated via Instant: V1 creates all timestamp columns as TIMESTAMP (6-digit micros)
        long tsNanos = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.123123Z") * 1000 + 123;
        testCreateTimestampColumns(tsNanos, null, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123123Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedInstantV2() throws Exception {
        // Designated via Instant: V2 creates NANOS-sent columns as TIMESTAMP_NANO (9-digit nanos)
        // ts_us (MICROS) stays TIMESTAMP, ts_ms (MILLIS) stays TIMESTAMP
        long tsNanos = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.123123Z") * 1000 + 123;
        testCreateTimestampColumns(tsNanos, null, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123123123Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMicrosV1() throws Exception {
        long tsMicros = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.123456Z");
        testCreateTimestampColumns(tsMicros, ChronoUnit.MICROS, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123456Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMicrosV2() throws Exception {
        long tsMicros = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.123456Z");
        testCreateTimestampColumns(tsMicros, ChronoUnit.MICROS, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123456Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMillisV1() throws Exception {
        long tsMillis = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.123456Z") / 1000;
        testCreateTimestampColumns(tsMillis, ChronoUnit.MILLIS, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMillisV2() throws Exception {
        long tsMillis = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.123456Z") / 1000;
        testCreateTimestampColumns(tsMillis, ChronoUnit.MILLIS, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedNanosV1() throws Exception {
        long tsNanos = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.123456Z") * 1000 + 789;
        testCreateTimestampColumns(tsNanos, ChronoUnit.NANOS, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123456Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedNanosV2() throws Exception {
        long tsNanos = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.123456Z") * 1000 + 789;
        testCreateTimestampColumns(tsNanos, ChronoUnit.NANOS, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123456789Z");
    }

    @Test
    public void testDecimalDefaultValuesWithoutWal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_decimal_default_values_without_wal (
                            dec8 DECIMAL(2, 0),
                            dec16 DECIMAL(4, 1),
                            dec32 DECIMAL(8, 2),
                            dec64 DECIMAL(16, 4),
                            dec128 DECIMAL(34, 8),
                            dec256 DECIMAL(64, 16),
                            value INT,
                            ts TIMESTAMP
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V3)) {
                    sender.table("test_decimal_default_values_without_wal")
                            .longColumn("value", 1)
                            .at(100_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT dec8, dec16, dec32, dec64, dec128, dec256, value, ts FROM test_decimal_default_values_without_wal",
                        """
                                dec8\tdec16\tdec32\tdec64\tdec128\tdec256\tvalue\tts
                                \t\t\t\t\t\t1\t1970-01-01T00:00:00.100000Z
                                """));
            }
        });
    }

    @Test
    public void testDisconnectOnErrorWithWAL() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                CairoEngine engine = serverMain.getEngine();
                try (SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)) {

                    engine.execute("create table x (ts timestamp, a int) timestamp(ts) partition by day wal", sqlExecutionContext);
                }

                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost").port(port).build()) {
                    for (int i = 0; i < 1_000_000; i++) {
                        sender.table("x").stringColumn("a", "42").atNow();
                    }
                    // TCP buffering may allow the loop above to complete before the
                    // server processes the type mismatch and closes the connection.
                    // Keep flushing until the disconnect propagates to the client.
                    assertEventually(() -> {
                        try {
                            sender.table("x").stringColumn("a", "42").atNow();
                            sender.flush();
                            Assert.fail("Server did not disconnect the client");
                        } catch (LineSenderException ignored) {
                        }
                    });
                } catch (LineSenderException ignored) {
                    // Expected: server disconnects the client due to type mismatch
                }
            }
        });
    }

    @Test
    public void testDouble_edgeValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long ts = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00Z");
                    sender.table("test_double_edge_values")
                            .doubleColumn("negative_inf", Double.NEGATIVE_INFINITY)
                            .doubleColumn("positive_inf", Double.POSITIVE_INFINITY)
                            .doubleColumn("nan", Double.NaN)
                            .doubleColumn("max_value", Double.MAX_VALUE)
                            .doubleColumn("min_value", Double.MIN_VALUE)
                            .at(ts, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT negative_inf, positive_inf, nan, max_value, min_value, timestamp FROM test_double_edge_values",
                        """
                                negative_inf\tpositive_inf\tnan\tmax_value\tmin_value\ttimestamp
                                null\tnull\tnull\t1.7976931348623157E308\t5.0E-324\t2022-02-25T00:00:00.000000Z
                                """));
            }
        });
    }

    @Test
    public void testExplicitTimestampColumnIndexIsCleared() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long ts = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00Z");
                    // the poison table sets the timestamp column index explicitly
                    sender.table("test_explicit_ts_col_idx_cleared_poison")
                            .stringColumn("str_col1", "str_col1")
                            .stringColumn("str_col2", "str_col2")
                            .stringColumn("str_col3", "str_col3")
                            .stringColumn("str_col4", "str_col4")
                            .timestampColumn("timestamp", ts, ChronoUnit.MICROS)
                            .at(ts, ChronoUnit.MICROS);
                    sender.flush();
                    assertEventually(() -> serverMain.assertSql(
                            "SELECT count() FROM test_explicit_ts_col_idx_cleared_poison",
                            "count\n1\n"));

                    // the victim table does not set the timestamp column index explicitly
                    sender.table("test_explicit_ts_col_idx_cleared_victim")
                            .stringColumn("str_col1", "str_col1")
                            .at(ts, ChronoUnit.MICROS);
                    sender.flush();
                    assertEventually(() -> serverMain.assertSql(
                            "SELECT count() FROM test_explicit_ts_col_idx_cleared_victim",
                            "count\n1\n"));
                }
            }
        });
    }

    @Test
    public void testInsertBadStringIntoUuidColumn() throws Exception {
        testValueCannotBeInsertedToUuidColumn("test_insert_bad_string_into_uuid_column", "totally not a uuid");
    }

    @Test
    public void testInsertBinaryToOtherColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_insert_binary_to_other_columns (
                            x SYMBOL,
                            y VARCHAR,
                            a1 DOUBLE,
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL
                        """);
                // send text double to symbol column
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V1)) {
                    sender.table("test_insert_binary_to_other_columns")
                            .doubleColumn("x", 9999.0)
                            .stringColumn("y", "ystr")
                            .doubleColumn("a1", 1)
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    // insert binary double to symbol column
                    sender.table("test_insert_binary_to_other_columns")
                            .doubleColumn("x", 10000.0)
                            .stringColumn("y", "ystr")
                            .doubleColumn("a1", 1)
                            .at(100_000_000_001L, ChronoUnit.MICROS);
                    sender.flush();

                    // insert binary double to string column (should be rejected)
                    sender.table("test_insert_binary_to_other_columns")
                            .symbol("x", "x1")
                            .doubleColumn("y", 9999.0)
                            .doubleColumn("a1", 1)
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                    // insert string to double column (should be rejected)
                    sender.table("test_insert_binary_to_other_columns")
                            .symbol("x", "x1")
                            .stringColumn("y", "ystr")
                            .stringColumn("a1", "11.u")
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                    // insert array column to double (should be rejected)
                    sender.table("test_insert_binary_to_other_columns")
                            .symbol("x", "x1")
                            .stringColumn("y", "ystr")
                            .doubleArray("a1", new double[]{1.0, 2.0})
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT x, y, a1, timestamp FROM test_insert_binary_to_other_columns ORDER BY timestamp",
                        """
                                x\ty\ta1\ttimestamp
                                9999.0\tystr\t1.0\t1970-01-02T03:46:40.000000Z
                                10000.0\tystr\t1.0\t1970-01-02T03:46:40.000001Z
                                """));
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatBasic() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_decimal_text_format_basic (
                            price DECIMAL(10, 2),
                            quantity DECIMAL(15, 4),
                            rate DECIMAL(8, 5),
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V3)) {
                    sender.table("test_decimal_text_format_basic")
                            .decimalColumn("price", "123.45")
                            .decimalColumn("quantity", "100.0000")
                            .decimalColumn("rate", "0.12345")
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_basic")
                            .decimalColumn("price", "-45.67")
                            .decimalColumn("quantity", "-10.5000")
                            .decimalColumn("rate", "-0.00001")
                            .at(100_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_basic")
                            .decimalColumn("price", "0.01")
                            .decimalColumn("quantity", "0.0001")
                            .decimalColumn("rate", "0.00000")
                            .at(100_000_000_002L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_basic")
                            .decimalColumn("price", "999")
                            .decimalColumn("quantity", "42")
                            .decimalColumn("rate", "1")
                            .at(100_000_000_003L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT price, quantity, rate, timestamp FROM test_decimal_text_format_basic ORDER BY timestamp",
                        """
                                price\tquantity\trate\ttimestamp
                                123.45\t100.0000\t0.12345\t1970-01-02T03:46:40.000000Z
                                -45.67\t-10.5000\t-0.00001\t1970-01-02T03:46:40.000001Z
                                0.01\t0.0001\t0.00000\t1970-01-02T03:46:40.000002Z
                                999.00\t42.0000\t1.00000\t1970-01-02T03:46:40.000003Z
                                """));
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatEdgeCases() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_decimal_text_format_edge_cases (
                            value DECIMAL(20, 10),
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V3)) {
                    sender.table("test_decimal_text_format_edge_cases")
                            .decimalColumn("value", "+123.456")
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_edge_cases")
                            .decimalColumn("value", "000123.450000")
                            .at(100_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_edge_cases")
                            .decimalColumn("value", "0.0000000001")
                            .at(100_000_000_002L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_edge_cases")
                            .decimalColumn("value", "0.0")
                            .at(100_000_000_003L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_edge_cases")
                            .decimalColumn("value", "0")
                            .at(100_000_000_004L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT value, timestamp FROM test_decimal_text_format_edge_cases ORDER BY timestamp",
                        """
                                value\ttimestamp
                                123.4560000000\t1970-01-02T03:46:40.000000Z
                                123.4500000000\t1970-01-02T03:46:40.000001Z
                                0.0000000001\t1970-01-02T03:46:40.000002Z
                                0.0000000000\t1970-01-02T03:46:40.000003Z
                                0.0000000000\t1970-01-02T03:46:40.000004Z
                                """));
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatEquivalence() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_decimal_text_format_equivalence (
                            text_format DECIMAL(10, 3),
                            binary_format DECIMAL(10, 3),
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V3)) {
                    sender.table("test_decimal_text_format_equivalence")
                            .decimalColumn("text_format", "123.450")
                            .decimalColumn("binary_format", Decimal256.fromLong(123_450, 3))
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_equivalence")
                            .decimalColumn("text_format", "-45.670")
                            .decimalColumn("binary_format", Decimal256.fromLong(-45_670, 3))
                            .at(100_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_equivalence")
                            .decimalColumn("text_format", "0.001")
                            .decimalColumn("binary_format", Decimal256.fromLong(1, 3))
                            .at(100_000_000_002L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT text_format, binary_format, timestamp FROM test_decimal_text_format_equivalence ORDER BY timestamp",
                        """
                                text_format\tbinary_format\ttimestamp
                                123.450\t123.450\t1970-01-02T03:46:40.000000Z
                                -45.670\t-45.670\t1970-01-02T03:46:40.000001Z
                                0.001\t0.001\t1970-01-02T03:46:40.000002Z
                                """));
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatPrecisionOverflow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_decimal_text_format_precision_overflow (
                            x DECIMAL(6, 3),
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V3)) {
                    sender.table("test_decimal_text_format_precision_overflow")
                            .decimalColumn("x", "1000.000")
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_precision_overflow")
                            .decimalColumn("x", "12345.678")
                            .at(100_000_000_001L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT x, timestamp FROM test_decimal_text_format_precision_overflow",
                        "x\ttimestamp\n"));
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatScientificNotation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_decimal_text_format_scientific_notation (
                            large DECIMAL(15, 2),
                            small DECIMAL(20, 15),
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V3)) {
                    sender.table("test_decimal_text_format_scientific_notation")
                            .decimalColumn("large", "1.23e5")
                            .decimalColumn("small", "1.23e-10")
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_scientific_notation")
                            .decimalColumn("large", "4.56E3")
                            .decimalColumn("small", "4.56E-8")
                            .at(100_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_scientific_notation")
                            .decimalColumn("large", "-9.99e2")
                            .decimalColumn("small", "-1.5e-12")
                            .at(100_000_000_002L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT large, small, timestamp FROM test_decimal_text_format_scientific_notation ORDER BY timestamp",
                        """
                                large\tsmall\ttimestamp
                                123000.00\t0.000000000123000\t1970-01-02T03:46:40.000000Z
                                4560.00\t0.000000045600000\t1970-01-02T03:46:40.000001Z
                                -999.00\t-0.000000000001500\t1970-01-02T03:46:40.000002Z
                                """));
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatTrailingZeros() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_decimal_text_format_trailing_zeros (
                            value1 DECIMAL(10, 3),
                            value2 DECIMAL(12, 5),
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V3)) {
                    sender.table("test_decimal_text_format_trailing_zeros")
                            .decimalColumn("value1", "100.000")
                            .decimalColumn("value2", "50.00000")
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_trailing_zeros")
                            .decimalColumn("value1", "1.200")
                            .decimalColumn("value2", "0.12300")
                            .at(100_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_decimal_text_format_trailing_zeros")
                            .decimalColumn("value1", "0.100")
                            .decimalColumn("value2", "0.00100")
                            .at(100_000_000_002L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT value1, value2, timestamp FROM test_decimal_text_format_trailing_zeros ORDER BY timestamp",
                        """
                                value1\tvalue2\ttimestamp
                                100.000\t50.00000\t1970-01-02T03:46:40.000000Z
                                1.200\t0.12300\t1970-01-02T03:46:40.000001Z
                                0.100\t0.00100\t1970-01-02T03:46:40.000002Z
                                """));
            }
        });
    }

    @Test
    public void testInsertDecimals() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_insert_decimals (
                            a DECIMAL(9, 0),
                            b DECIMAL(9, 3),
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V3)) {
                    sender.table("test_insert_decimals")
                            .decimalColumn("a", Decimal256.fromLong(12_345, 0))
                            .decimalColumn("b", Decimal256.fromLong(12_345, 2))
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.table("test_insert_decimals")
                            .decimalColumn("a", Decimal256.NULL_VALUE)
                            .decimalColumn("b", Decimal256.fromLong(123_456, 3))
                            .at(100_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_insert_decimals")
                            .longColumn("a", 42)
                            .longColumn("b", 42)
                            .at(100_000_000_002L, ChronoUnit.MICROS);
                    sender.table("test_insert_decimals")
                            .stringColumn("a", "42")
                            .stringColumn("b", "42.123")
                            .at(100_000_000_003L, ChronoUnit.MICROS);
                    sender.table("test_insert_decimals")
                            .stringColumn("a", "42.0")
                            .stringColumn("b", "42.1")
                            .at(100_000_000_004L, ChronoUnit.MICROS);
                    sender.table("test_insert_decimals")
                            .doubleColumn("a", 42d)
                            .doubleColumn("b", 42.1d)
                            .at(100_000_000_005L, ChronoUnit.MICROS);
                    sender.table("test_insert_decimals")
                            .doubleColumn("a", Double.NaN)
                            .doubleColumn("b", Double.POSITIVE_INFINITY)
                            .at(100_000_000_006L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT a, b, timestamp FROM test_insert_decimals ORDER BY timestamp",
                        """
                                a\tb\ttimestamp
                                12345\t123.450\t1970-01-02T03:46:40.000000Z
                                \t123.456\t1970-01-02T03:46:40.000001Z
                                42\t42.000\t1970-01-02T03:46:40.000002Z
                                42\t42.123\t1970-01-02T03:46:40.000003Z
                                42\t42.100\t1970-01-02T03:46:40.000004Z
                                42\t42.100\t1970-01-02T03:46:40.000005Z
                                \t\t1970-01-02T03:46:40.000006Z
                                """));
            }
        });
    }

    @Test
    public void testInsertInvalidDecimals() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_invalid_decimal_test (
                            x DECIMAL(6, 3),
                            y DECIMAL(76, 73),
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V3)) {
                    sender.table("test_invalid_decimal_test")
                            .longColumn("x", 1234)
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.table("test_invalid_decimal_test")
                            .longColumn("y", 12_345)
                            .at(100_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_invalid_decimal_test")
                            .doubleColumn("x", 1.2345d)
                            .at(100_000_000_002L, ChronoUnit.MICROS);
                    sender.table("test_invalid_decimal_test")
                            .doubleColumn("x", 12345.678d)
                            .at(100_000_000_003L, ChronoUnit.MICROS);
                    sender.table("test_invalid_decimal_test")
                            .stringColumn("x", "abc")
                            .at(100_000_000_004L, ChronoUnit.MICROS);
                    sender.table("test_invalid_decimal_test")
                            .stringColumn("x", "1E8")
                            .at(100_000_000_005L, ChronoUnit.MICROS);
                    sender.table("test_invalid_decimal_test")
                            .decimalColumn("x", Decimal256.fromLong(12_345_678, 3))
                            .at(100_000_000_006L, ChronoUnit.MICROS);
                    sender.table("test_invalid_decimal_test")
                            .decimalColumn("y", Decimal256.fromLong(12_345, 0))
                            .at(100_000_000_007L, ChronoUnit.MICROS);
                    sender.flush();
                    sender.table("test_invalid_decimal_test")
                            .decimalColumn("x", Decimal256.fromLong(123_456, 4))
                            .at(100_000_000_007L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT x, y, timestamp FROM test_invalid_decimal_test",
                        "x\ty\ttimestamp\n"));
            }
        });
    }

    @Test
    public void testInsertLargeArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 10,000,000 doubles => ~80 MB binary payload.
            // Reduce writer queue capacity to avoid allocating ~64 GB of native memory for event slots.
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_TCP_MAX_MEASUREMENT_SIZE.getEnvVarName(), "100000000",
                    PropertyKey.LINE_TCP_WRITER_QUEUE_CAPACITY.getEnvVarName(), "2"
            )) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = Sender.builder(Sender.Transport.TCP)
                        .address("localhost")
                        .port(port)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .bufferCapacity(100_000_000)
                        .build()) {
                    double[] arr = createDoubleArray(10_000_000);
                    sender.table("test_arr_large_test")
                            .doubleArray("arr", arr)
                            .at(100_000_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM test_arr_large_test",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testInsertNonAsciiStringAndUuid() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_insert_non_ascii_string_and_uuid (
                            s STRING,
                            u UUID,
                            ts TIMESTAMP
                        ) TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long tsMicros = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00Z");
                    sender.table("test_insert_non_ascii_string_and_uuid")
                            .stringColumn("s", "non-ascii äöü")
                            .stringColumn("u", "11111111-2222-3333-4444-555555555555")
                            .at(tsMicros, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT s, u, ts FROM test_insert_non_ascii_string_and_uuid",
                        """
                                s\tu\tts
                                non-ascii äöü\t11111111-2222-3333-4444-555555555555\t2022-02-25T00:00:00.000000Z
                                """));
            }
        });
    }

    @Test
    public void testInsertNonAsciiStringIntoUuidColumn() throws Exception {
        testValueCannotBeInsertedToUuidColumn("test_insert_non_ascii_string_into_uuid_column",
                "11111111-1111-1111-1111-1111111111ü");
    }

    @Test
    public void testInsertStringIntoUuidColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_insert_string_into_uuid_column (
                            u1 UUID,
                            u2 UUID,
                            u3 UUID,
                            ts TIMESTAMP
                        ) TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long tsMicros = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00Z");
                    sender.table("test_insert_string_into_uuid_column")
                            .stringColumn("u1", "11111111-1111-1111-1111-111111111111")
                            // u2 empty -> insert as null
                            .stringColumn("u3", "33333333-3333-3333-3333-333333333333")
                            .at(tsMicros, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT u1, u3, ts FROM test_insert_string_into_uuid_column",
                        """
                                u1\tu3\tts
                                11111111-1111-1111-1111-111111111111\t33333333-3333-3333-3333-333333333333\t2022-02-25T00:00:00.000000Z
                                """));
            }
        });
    }

    @Test
    public void testInsertTimestampAsInstant() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_insert_timestamp_as_instant (
                            ts_col TIMESTAMP,
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    sender.table("test_insert_timestamp_as_instant")
                            .timestampColumn("ts_col", Instant.parse("2023-02-11T12:30:11.35Z"))
                            .at(Instant.parse("2022-01-10T20:40:22.54Z"));
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT ts_col, timestamp FROM test_insert_timestamp_as_instant",
                        """
                                ts_col\ttimestamp
                                2023-02-11T12:30:11.350000Z\t2022-01-10T20:40:22.540000Z
                                """));
            }
        });
    }

    @Test
    public void testInsertTimestampMiscUnits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_insert_timestamp_misc_units (
                            unit STRING,
                            ts TIMESTAMP,
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long tsMicros = MicrosFormatUtils.parseTimestamp("2023-09-18T12:01:01.01Z");
                    sender.table("test_insert_timestamp_misc_units")
                            .stringColumn("unit", "ns")
                            .timestampColumn("ts", tsMicros * 1000, ChronoUnit.NANOS)
                            .at(tsMicros * 1000, ChronoUnit.NANOS);
                    sender.table("test_insert_timestamp_misc_units")
                            .stringColumn("unit", "us")
                            .timestampColumn("ts", tsMicros, ChronoUnit.MICROS)
                            .at(tsMicros, ChronoUnit.MICROS);
                    sender.table("test_insert_timestamp_misc_units")
                            .stringColumn("unit", "ms")
                            .timestampColumn("ts", tsMicros / 1000, ChronoUnit.MILLIS)
                            .at(tsMicros / 1000, ChronoUnit.MILLIS);
                    sender.table("test_insert_timestamp_misc_units")
                            .stringColumn("unit", "s")
                            .timestampColumn("ts", tsMicros / 1_000_000, ChronoUnit.SECONDS)
                            .at(tsMicros / 1_000_000, ChronoUnit.SECONDS);
                    sender.table("test_insert_timestamp_misc_units")
                            .stringColumn("unit", "m")
                            .timestampColumn("ts", tsMicros / 60_000_000, ChronoUnit.MINUTES)
                            .at(tsMicros / 60_000_000, ChronoUnit.MINUTES);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT unit, ts, timestamp FROM test_insert_timestamp_misc_units ORDER BY timestamp",
                        """
                                unit\tts\ttimestamp
                                m\t2023-09-18T12:01:00.000000Z\t2023-09-18T12:01:00.000000Z
                                s\t2023-09-18T12:01:01.000000Z\t2023-09-18T12:01:01.000000Z
                                ns\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000Z
                                us\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000Z
                                ms\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000Z
                                """));
            }
        });
    }

    @Test
    public void testInsertTimestampNanoOverflow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_insert_timestamp_nano_overflow (
                            ts TIMESTAMP,
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long tsMicros = MicrosFormatUtils.parseTimestamp("2323-09-18T12:01:01.011568Z");
                    sender.table("test_insert_timestamp_nano_overflow")
                            .timestampColumn("ts", tsMicros, ChronoUnit.MICROS)
                            .at(tsMicros, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT ts, timestamp FROM test_insert_timestamp_nano_overflow",
                        """
                                ts\ttimestamp
                                2323-09-18T12:01:01.011568Z\t2323-09-18T12:01:01.011568Z
                                """));
            }
        });
    }

    @Test
    public void testInsertTimestampNanoUnits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("""
                        CREATE TABLE test_insert_timestamp_nano_units (
                            unit STRING,
                            ts TIMESTAMP,
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL
                        """);
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long tsNanos = MicrosFormatUtils.parseTimestamp("2023-09-18T12:01:01.011568Z") * 1000;
                    sender.table("test_insert_timestamp_nano_units")
                            .stringColumn("unit", "ns")
                            .timestampColumn("ts", tsNanos, ChronoUnit.NANOS)
                            .at(tsNanos, ChronoUnit.NANOS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT unit, ts, timestamp FROM test_insert_timestamp_nano_units",
                        """
                                unit\tts\ttimestamp
                                ns\t2023-09-18T12:01:01.011568Z\t2023-09-18T12:01:01.011568Z
                                """));
            }
        });
    }

    @Test
    public void testMaxNameLength() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                PlainTcpLineChannel channel = new PlainTcpLineChannel(NetworkFacadeImpl.INSTANCE, HOST, port, 1024);
                try (AbstractLineTcpSender sender = new LineTcpSenderV2(channel, 1024, 20)) {
                    try {
                        sender.table("table_with_long______________________name");
                        fail();
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(),
                                "table name is too long: [name = table_with_long______________________name, maxNameLength=20]");
                    }

                    try {
                        sender.table("tab")
                                .doubleColumn("column_with_long______________________name", 1.0);
                        fail();
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(),
                                "column name is too long: [name = column_with_long______________________name, maxNameLength=20]");
                    }
                }
            }
        });
    }

    @Test
    public void testMultipleVarcharCols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long ts = MicrosFormatUtils.parseTimestamp("2024-02-27T00:00:00Z");
                    sender.table("test_string_table")
                            .stringColumn("string1", "some string")
                            .stringColumn("string2", "another string")
                            .stringColumn("string3", "yet another string")
                            .at(ts, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT string1, string2, string3, timestamp FROM test_string_table",
                        """
                                string1\tstring2\tstring3\ttimestamp
                                some string\tanother string\tyet another string\t2024-02-27T00:00:00.000000Z
                                """));
            }
        });
    }

    @Test
    public void testServerIgnoresUnfinishedRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    // well-formed row first
                    sender.table("test_server_ignores_unfinished_rows").longColumn("field0", 42)
                            .longColumn("field1", 42)
                            .atNow();

                    // failed validation
                    sender.table("test_server_ignores_unfinished_rows")
                            .longColumn("field0", 42)
                            .longColumn("field1\n", 42);
                    fail("validation should have failed");
                } catch (LineSenderException e) {
                    // ignored
                }
                // make sure the 2nd unfinished row was not inserted by the server
                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM test_server_ignores_unfinished_rows",
                        "count\n1\n"));
            }
        });
    }

    @Test
    public void testSymbolCapacityReload() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                final int N = 1000;
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    Random rnd = new Random(42);
                    for (int i = 0; i < N; i++) {
                        sender.table("test_symbol_capacity_table")
                                .symbol("sym1", "sym_" + rnd.nextInt(100))
                                .symbol("sym2", "s" + rnd.nextInt(10))
                                .doubleColumn("dd", rnd.nextDouble())
                                .atNow();
                    }
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT count() FROM test_symbol_capacity_table",
                        "count\n" + N + "\n"));
            }
        });
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterBool() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.boolColumn("columnName", false));
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterDouble() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.doubleColumn("columnName", 42.0));
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterLong() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.longColumn("columnName", 42));
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterString() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.stringColumn("columnName", "42"));
    }

    @Test
    public void testTimestampIngestV1() throws Exception {
        testTimestampIngest(PROTOCOL_VERSION_V1,
                """
                        ts\tdts
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                        """,
                null
        );
    }

    @Test
    public void testTimestampIngestV2() throws Exception {
        testTimestampIngest(PROTOCOL_VERSION_V2,
                """
                        ts\tdts
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                        """,
                """
                        ts\tdts
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                        2300-11-19T10:55:24.123456Z\t2300-11-20T10:55:24.834129Z
                        """
        );
    }

    @Test
    public void testUseVarcharAsString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long ts = MicrosFormatUtils.parseTimestamp("2024-02-27T00:00:00Z");
                    String expectedValue = "čćžšđçğéíáýůř";
                    sender.table("test_varchar_string_table")
                            .stringColumn("string1", expectedValue)
                            .at(ts, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT string1, timestamp FROM test_varchar_string_table",
                        """
                                string1\ttimestamp
                                čćžšđçğéíáýůř\t2024-02-27T00:00:00.000000Z
                                """));
            }
        });
    }

    @Test
    public void testWriteAllTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long ts = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00Z");
                    sender.table("test_write_all_types")
                            .longColumn("int_field", 42)
                            .boolColumn("bool_field", true)
                            .stringColumn("string_field", "foo")
                            .doubleColumn("double_field", 42.0)
                            .timestampColumn("ts_field", ts, ChronoUnit.MICROS)
                            .at(ts, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT int_field, bool_field, string_field, double_field, ts_field, timestamp FROM test_write_all_types",
                        """
                                int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp
                                42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000Z\t2022-02-25T00:00:00.000000Z
                                """));
            }
        });
    }

    @Test
    public void testWriteLongMinMax() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long ts = MicrosFormatUtils.parseTimestamp("2023-02-22T00:00:00Z");
                    sender.table("test_long_min_max_table")
                            .longColumn("max", Long.MAX_VALUE)
                            .longColumn("min", Long.MIN_VALUE)
                            .at(ts, ChronoUnit.MICROS);
                    sender.flush();
                }
                assertEventually(() -> serverMain.assertSql(
                        "SELECT max, min, timestamp FROM test_long_min_max_table",
                        """
                                max\tmin\ttimestamp
                                9223372036854775807\tnull\t2023-02-22T00:00:00.000000Z
                                """));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T buildNestedDoubleArray(int[] shape, int currentDim, int[] indices) {
        if (currentDim == shape.length - 1) {
            double[] arr = new double[shape[currentDim]];
            for (int i = 0; i < arr.length; i++) {
                indices[currentDim] = i;
                double product = 1.0;
                for (int idx : indices) {
                    product *= (idx + 1);
                }
                arr[i] = product;
            }
            return (T) arr;
        } else {
            int dimsRemaining = shape.length - currentDim - 1;
            Class<?> componentType = double.class;
            for (int d = 0; d < dimsRemaining; d++) {
                componentType = Array.newInstance(componentType, 0).getClass();
            }
            Object arr = Array.newInstance(componentType, shape[currentDim]);
            for (int i = 0; i < shape[currentDim]; i++) {
                indices[currentDim] = i;
                Object subArr = buildNestedDoubleArray(shape, currentDim + 1, indices);
                Array.set(arr, i, subArr);
            }
            return (T) arr;
        }
    }

    private static <T> T createDoubleArray(int... shape) {
        return buildNestedDoubleArray(shape, 0, new int[shape.length]);
    }

    private static Sender createTcpSender(int port, int protocolVersion) {
        return Sender.builder(Sender.Transport.TCP)
                .address("localhost")
                .port(port)
                .protocolVersion(protocolVersion)
                .build();
    }

    private void assertSymbolsCannotBeWrittenAfterOtherType(Consumer<Sender> otherTypeWriter) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    sender.table("test_symbols_cannot_be_written_after_other_type");
                    otherTypeWriter.accept(sender);
                    try {
                        sender.symbol("name", "value");
                        fail("symbols cannot be written after any other column type");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "before any other column types");
                        sender.atNow();
                    }
                }
            }
        });
    }

    private void testCreateTimestampColumns(long timestamp, ChronoUnit unit, int protocolVersion,
                                            int[] expectedColumnTypes, String expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = createTcpSender(port, protocolVersion)) {
                    long tsNs = MicrosFormatUtils.parseTimestamp("2025-11-19T10:55:24.123456Z") * 1000 + 789;
                    long tsUs = MicrosFormatUtils.parseTimestamp("2025-11-19T10:55:24.123456Z");
                    long tsMs = MicrosFormatUtils.parseTimestamp("2025-11-19T10:55:24.123Z") / 1000;
                    Instant tsInstant = Instant.ofEpochSecond(tsNs / 1_000_000_000, tsNs % 1_000_000_000 + 10);

                    if (unit != null) {
                        sender.table("test_create_ts_cols")
                                .doubleColumn("col1", 1.111)
                                .timestampColumn("ts_ns", tsNs, ChronoUnit.NANOS)
                                .timestampColumn("ts_us", tsUs, ChronoUnit.MICROS)
                                .timestampColumn("ts_ms", tsMs, ChronoUnit.MILLIS)
                                .timestampColumn("ts_instant", tsInstant)
                                .at(timestamp, unit);
                    } else {
                        sender.table("test_create_ts_cols")
                                .doubleColumn("col1", 1.111)
                                .timestampColumn("ts_ns", tsNs, ChronoUnit.NANOS)
                                .timestampColumn("ts_us", tsUs, ChronoUnit.MICROS)
                                .timestampColumn("ts_ms", tsMs, ChronoUnit.MILLIS)
                                .timestampColumn("ts_instant", tsInstant)
                                .at(Instant.ofEpochSecond(timestamp / 1_000_000_000, timestamp % 1_000_000_000));
                    }
                    sender.flush();
                }

                assertEventually(() -> {
                    serverMain.assertSql(
                            "SELECT col1, ts_ns, ts_us, ts_ms, ts_instant, timestamp FROM test_create_ts_cols",
                            "col1\tts_ns\tts_us\tts_ms\tts_instant\ttimestamp\n" + expected + "\n");

                    CairoEngine engine = serverMain.getEngine();
                    try (TableReader reader = engine.getReader("test_create_ts_cols")) {
                        TableReaderMetadata meta = reader.getMetadata();
                        Assert.assertEquals(6, meta.getColumnCount());
                        Assert.assertEquals(ColumnType.DOUBLE, meta.getColumnType(0));
                        Assert.assertEquals(expectedColumnTypes[0], meta.getColumnType(1));
                        Assert.assertEquals(ColumnType.TIMESTAMP, meta.getColumnType(2));
                        Assert.assertEquals(ColumnType.TIMESTAMP, meta.getColumnType(3));
                        Assert.assertEquals(expectedColumnTypes[1], meta.getColumnType(4));
                        Assert.assertEquals(expectedColumnTypes[2], meta.getColumnType(5));
                    }
                });
            }
        });
    }

    private void testTimestampIngest(int protocolVersion, String expected1, String expected2) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute(
                        "CREATE TABLE test_ts_ingest (ts TIMESTAMP, dts TIMESTAMP) TIMESTAMP(dts) PARTITION BY DAY BYPASS WAL");

                try (Sender sender = createTcpSender(port, protocolVersion)) {
                    long tsNs = MicrosFormatUtils.parseTimestamp("2025-11-19T10:55:24.123456Z") * 1000 + 789;
                    long dtsNs = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.834129Z") * 1000 + 82;
                    long tsUs = MicrosFormatUtils.parseTimestamp("2025-11-19T10:55:24.123456Z");
                    long dtsUs = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.834129Z");
                    long tsMs = MicrosFormatUtils.parseTimestamp("2025-11-19T10:55:24.123Z") / 1000;
                    long dtsMs = MicrosFormatUtils.parseTimestamp("2025-11-20T10:55:24.834Z") / 1000;
                    Instant tsInstant = Instant.ofEpochSecond(tsNs / 1_000_000_000, tsNs % 1_000_000_000 + 10);
                    Instant dtsInstant = Instant.ofEpochSecond(dtsNs / 1_000_000_000, dtsNs % 1_000_000_000 + 10);

                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsNs, ChronoUnit.NANOS)
                            .at(dtsNs, ChronoUnit.NANOS);
                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsUs, ChronoUnit.MICROS)
                            .at(dtsNs, ChronoUnit.NANOS);
                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsMs, ChronoUnit.MILLIS)
                            .at(dtsNs, ChronoUnit.NANOS);

                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsNs, ChronoUnit.NANOS)
                            .at(dtsUs, ChronoUnit.MICROS);
                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsUs, ChronoUnit.MICROS)
                            .at(dtsUs, ChronoUnit.MICROS);
                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsMs, ChronoUnit.MILLIS)
                            .at(dtsUs, ChronoUnit.MICROS);

                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsNs, ChronoUnit.NANOS)
                            .at(dtsMs, ChronoUnit.MILLIS);
                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsUs, ChronoUnit.MICROS)
                            .at(dtsMs, ChronoUnit.MILLIS);
                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsMs, ChronoUnit.MILLIS)
                            .at(dtsMs, ChronoUnit.MILLIS);

                    sender.table("test_ts_ingest")
                            .timestampColumn("ts", tsInstant)
                            .at(dtsInstant);

                    sender.flush();

                    assertEventually(() -> serverMain.assertSql(
                            "SELECT ts, dts FROM test_ts_ingest",
                            expected1));

                    try {
                        // Far-future timestamp: overflows nanos in V1
                        long tsFarUs = MicrosFormatUtils.parseTimestamp("2300-11-19T10:55:24.123456Z");
                        long dtsFarUs = MicrosFormatUtils.parseTimestamp("2300-11-20T10:55:24.834129Z");
                        sender.table("test_ts_ingest")
                                .timestampColumn("ts", tsFarUs, ChronoUnit.MICROS)
                                .at(dtsFarUs, ChronoUnit.MICROS);
                        sender.flush();

                        if (expected2 == null && protocolVersion == PROTOCOL_VERSION_V1) {
                            Assert.fail("Exception expected");
                        }
                    } catch (ArithmeticException e) {
                        if (expected2 == null && protocolVersion == PROTOCOL_VERSION_V1) {
                            TestUtils.assertContains(e.getMessage(), "long overflow");
                        } else {
                            throw e;
                        }
                    }

                    if (expected2 != null) {
                        assertEventually(() -> serverMain.assertSql(
                                "SELECT ts, dts FROM test_ts_ingest",
                                expected2));
                    } else {
                        assertEventually(() -> serverMain.assertSql(
                                "SELECT ts, dts FROM test_ts_ingest",
                                expected1));
                    }
                }
            }
        });
    }

    private void testValueCannotBeInsertedToUuidColumn(String tableName, String value) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                serverMain.execute("CREATE TABLE " + tableName + " ("
                        + "u1 UUID,"
                        + "ts TIMESTAMP"
                        + ") TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL");

                // this sender fails as the string is not UUID
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long tsMicros = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00Z");
                    sender.table(tableName)
                            .stringColumn("u1", value)
                            .at(tsMicros, ChronoUnit.MICROS);
                    sender.flush();
                }

                // this sender succeeds as the string is in the UUID format
                try (Sender sender = createTcpSender(port, PROTOCOL_VERSION_V2)) {
                    long tsMicros = MicrosFormatUtils.parseTimestamp("2022-02-25T00:00:00Z");
                    sender.table(tableName)
                            .stringColumn("u1", "11111111-1111-1111-1111-111111111111")
                            .at(tsMicros, ChronoUnit.MICROS);
                    sender.flush();
                }

                assertEventually(() -> serverMain.assertSql(
                        "SELECT u1, ts FROM " + tableName,
                        """
                                u1\tts
                                11111111-1111-1111-1111-111111111111\t2022-02-25T00:00:00.000000Z
                                """));
            }
        });
    }
}
