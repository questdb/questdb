/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.client.Sender;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.line.AbstractLineTcpSender;
import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSenderV2;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Chars;
import io.questdb.std.Decimal256;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.security.PrivateKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.questdb.client.Sender.*;
import static io.questdb.test.cutlass.http.line.LineHttpSenderTest.createDoubleArray;
import static io.questdb.test.tools.TestUtils.assertContains;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

public class LineTcpSenderTest extends AbstractLineTcpReceiverTest {

    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static String AUTH_KEY_ID2_INVALID = "invalid";
    private final static int HOST = Net.parseIPv4("127.0.0.1");
    private static final Consumer<Sender> SET_TABLE_NAME_ACTION = s -> s.table("mytable");
    private final static String TOKEN = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthUtils.toPrivateKey(TOKEN);
    private final boolean walEnabled;

    public LineTcpSenderTest() {
        Rnd rnd = TestUtils.generateRandom(AbstractCairoTest.LOG);
        this.walEnabled = TestUtils.isWal(rnd);
        this.timestampType = TestUtils.getTimestampType(rnd);
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
    }

    @Test
    public void testArrayAtNow() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build();
                 DoubleArray a1 = new DoubleArray(1, 1, 2, 1).setAll(1)
            ) {
                String table = "array_at_now";
                CountDownLatch released = createTableCommitNotifier(table);
                sender.table(table)
                        .symbol("x", "42i")
                        .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")  // ensuring no array parsing for symbol
                        .longColumn("l1", 23452345)
                        .doubleArray("a1", a1)
                        .atNow();
                sender.flush();
                waitTableWriterFinish(released);
                assertTableSizeEventually(engine, table, 1);
                // @todo assert table contents, needs getArray support in TestTableReadCursor
            }
        });
    }

    @Test
    public void testArrayDouble() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build();
                 DoubleArray a4 = new DoubleArray(1, 1, 2, 1).setAll(4);
                 DoubleArray a5 = new DoubleArray(3, 2, 1, 4, 1).setAll(5);
                 DoubleArray a6 = new DoubleArray(1, 3, 4, 2, 1, 1).setAll(6)
            ) {
                String table = "array_test";
                CountDownLatch released = createTableCommitNotifier(table);
                long ts = MicrosFormatUtils.parseTimestamp("2025-02-22T00:00:00.000000Z");
                double[] arr1d = createDoubleArray(5);
                double[][] arr2d = createDoubleArray(2, 3);
                double[][][] arr3d = createDoubleArray(1, 2, 3);
                sender.table(table)
                        .symbol("x", "42i")
                        .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")  // ensuring no array parsing for symbol
                        .longColumn("l1", 23452345)
                        .doubleArray("a1", arr1d)
                        .doubleArray("a2", arr2d)
                        .doubleArray("a3", arr3d)
                        .doubleArray("a4", a4)
                        .doubleArray("a5", a5)
                        .doubleArray("a6", a6)
                        .at(ts, ChronoUnit.MICROS);
                sender.flush();
                waitTableWriterFinish(released);
                assertTableSizeEventually(engine, table, 1);
            }
        });
    }

    @Test
    public void testAuthSuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            int bufferCapacity = 256 * 1024;

            try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, bindPort, bufferCapacity)) {
                sender.authenticate(authKeyId, AUTH_PRIVATE_KEY1);
                sender.metric("mytable").field("my int field", 42).$();
                sender.flush();
            }

            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testAuthWrongKey() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            int bufferCapacity = 2048;

            try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, bindPort, bufferCapacity)) {
                sender.authenticate(AUTH_KEY_ID2_INVALID, AUTH_PRIVATE_KEY1);
                //30 seconds should be enough even on a slow CI server
                long deadline = Os.currentTimeNanos() + SECONDS.toNanos(30);
                while (Os.currentTimeNanos() < deadline) {
                    sender.metric("mytable").field("my int field", 42).$();
                    sender.flush();
                }
                fail("Client fail to detected qdb server closed a connection due to wrong credentials");
            } catch (LineSenderException expected) {
                // ignored
            }
        });
    }

    @Test
    public void testBuilderAuthSuccess() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        authKeyId = AUTH_KEY_ID1;
        String address = "127.0.0.1:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address(address)
                    .enableAuth(AUTH_KEY_ID1).authToken(TOKEN)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderAuthSuccess_confString() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        authKeyId = AUTH_KEY_ID1;
        String address = "127.0.0.1:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.fromConfig("tcp::addr=" + address + ";user=" + AUTH_KEY_ID1 + ";token=" + TOKEN + ";protocol_version=2;")) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderPlainText_addressWithExplicitIpAndPort() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderPlainText_addressWithHostnameAndPort() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String address = "localhost:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address(address)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderPlainText_addressWithIpAndPort() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String address = "127.0.0.1:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address(address)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testCannotStartNewRowBeforeClosingTheExistingAfterValidationError() {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            try {
                sender.boolColumn("col\n", true);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "name contains an illegal char");
            }
            try {
                sender.table("mytable");
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "duplicated table");
            }
        }
        assertFalse(channel.contain(new byte[]{(byte) '\n'}));
    }

    @Test
    public void testCloseIdempotent() {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        DummyLineChannel channel = new DummyLineChannel();
        AbstractLineTcpSender sender = new LineTcpSenderV2(channel, 1000, 127);
        sender.close();
        sender.close();
        assertEquals(1, channel.closeCounter);
    }

    @Test
    public void testCloseImpliesFlush() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testConfString() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            String confString = "tcp::addr=127.0.0.1:" + bindPort + ";user=" + AUTH_KEY_ID1 + ";token=" + TOKEN + ";protocol_version=2;";
            try (Sender sender = Sender.fromConfig(confString)) {

                long tsMicros = MicrosTimestampDriver.floor("2022-02-25");
                CountDownLatch released = createTableCommitNotifier("mytable");
                sender.table("mytable")
                        .longColumn("int_field", 42)
                        .boolColumn("bool_field", true)
                        .stringColumn("string_field", "foo")
                        .doubleColumn("double_field", 42.0)
                        .timestampColumn("ts_field", tsMicros, ChronoUnit.MICROS)
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.flush();
                waitTableWriterFinish(released);
            }

            assertTableSizeEventually(engine, "mytable", 1);
            try (TableReader reader = getReader("mytable")) {
                TestUtils.assertReader("""
                        int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp
                        42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000Z\t2022-02-25T00:00:00.000000Z
                        """, reader, sink);
            }
        });
    }

    @Test
    public void testConfString_autoFlushBytes() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String confString = "tcp::addr=localhost:" + bindPort + ";auto_flush_bytes=1;protocol_version=2;"; // the minimal allowed buffer size
        runInContext(r -> {
            try (Sender sender = Sender.fromConfig(confString)) {
                // just 2 rows must be enough to trigger flush
                // why not 1? the first byte of the 2nd row will flush the last byte of the 1st row
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.table("mytable").longColumn("my int field", 42).atNow();

                // make sure to assert before closing the Sender
                // since the Sender will always flush on close
                assertTableExistsEventually(engine, "mytable");
            }
        });
    }

    @Test
    public void testControlCharInColumnName() {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertControlCharacterException();
    }

    @Test
    public void testControlCharInTableName() {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertControlCharacterException();
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedInstantV1() throws Exception {
        testCreateTimestampColumns(NanosTimestampDriver.floor("2025-11-20T10:55:24.123123123Z"), null, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123123Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedInstantV2() throws Exception {
        testCreateTimestampColumns(NanosTimestampDriver.floor("2025-11-20T10:55:24.123123123Z"), null, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123123123Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMicrosV1() throws Exception {
        testCreateTimestampColumns(MicrosTimestampDriver.floor("2025-11-20T10:55:24.123456Z"), ChronoUnit.MICROS, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123456Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMicrosV2() throws Exception {
        testCreateTimestampColumns(MicrosTimestampDriver.floor("2025-11-20T10:55:24.123456Z"), ChronoUnit.MICROS, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123456Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMillisV1() throws Exception {
        testCreateTimestampColumns(MicrosTimestampDriver.floor("2025-11-20T10:55:24.123456Z") / 1000, ChronoUnit.MILLIS, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMillisV2() throws Exception {
        testCreateTimestampColumns(MicrosTimestampDriver.floor("2025-11-20T10:55:24.123456Z") / 1000, ChronoUnit.MILLIS, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedNanosV1() throws Exception {
        testCreateTimestampColumns(NanosTimestampDriver.floor("2025-11-20T10:55:24.123456789Z"), ChronoUnit.NANOS, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123456Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedNanosV2() throws Exception {
        testCreateTimestampColumns(NanosTimestampDriver.floor("2025-11-20T10:55:24.123456789Z"), ChronoUnit.NANOS, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123456789Z");
    }

    @Test
    public void testDouble_edgeValues() throws Exception {
        final boolean isMicros = ColumnType.isTimestampMicro(timestampType.getTimestampType());
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                CountDownLatch released = createTableCommitNotifier("mytable");
                long ts = isMicros ? MicrosTimestampDriver.floor("2022-02-25") : NanosTimestampDriver.floor("2022-02-25");
                sender.table("mytable")
                        .doubleColumn("negative_inf", Double.NEGATIVE_INFINITY)
                        .doubleColumn("positive_inf", Double.POSITIVE_INFINITY)
                        .doubleColumn("nan", Double.NaN)
                        .doubleColumn("max_value", Double.MAX_VALUE)
                        .doubleColumn("min_value", Double.MIN_VALUE)
                        .at(ts, isMicros ? ChronoUnit.MICROS : ChronoUnit.NANOS);
                sender.flush();
                waitTableWriterFinish(released);
                assertTableSizeEventually(engine, "mytable", 1);
                try (TableReader reader = getReader("mytable")) {
                    if (isMicros) {
                        TestUtils.assertReader("""
                                negative_inf\tpositive_inf\tnan\tmax_value\tmin_value\ttimestamp
                                null\tnull\tnull\t1.7976931348623157E308\t4.9E-324\t2022-02-25T00:00:00.000000Z
                                """, reader, sink);
                    } else {
                        TestUtils.assertReader("""
                                negative_inf\tpositive_inf\tnan\tmax_value\tmin_value\ttimestamp
                                null\tnull\tnull\t1.7976931348623157E308\t4.9E-324\t2022-02-25T00:00:00.000000000Z
                                """, reader, sink);
                    }
                }
            }
        });
    }

    @Test
    public void testExplicitTimestampColumnIndexIsCleared() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                CountDownLatch released = createTableCommitNotifier("poison");
                long ts = MicrosTimestampDriver.floor("2022-02-25");
                // the poison table sets the timestamp column index explicitly
                sender.table("poison")
                        .stringColumn("str_col1", "str_col1")
                        .stringColumn("str_col2", "str_col2")
                        .stringColumn("str_col3", "str_col3")
                        .stringColumn("str_col4", "str_col4")
                        .timestampColumn("timestamp", ts, ChronoUnit.MICROS)
                        .at(ts, ChronoUnit.MICROS);
                sender.flush();
                waitTableWriterFinish(released);
                assertTableSizeEventually(engine, "poison", 1);
                CountDownLatch released2 = createTableCommitNotifier("victim");
                // the victim table does not set the timestamp column index explicitly
                sender.table("victim")
                        .stringColumn("str_col1", "str_col1")
                        .at(ts, ChronoUnit.MICROS);
                sender.flush();
                waitTableWriterFinish(released2);

                assertTableSizeEventually(engine, "victim", 1);
            }
        });
    }

    @Test
    public void testInsertBadStringIntoUuidColumn() throws Exception {
        testValueCannotBeInsertedToUuidColumn("totally not a uuid");
    }

    @Test
    public void testInsertBinaryToOtherColumns() throws Exception {
        runInContext(r -> {
            TableModel model = new TableModel(configuration, "mytable", PartitionBy.YEAR)
                    .col("x", ColumnType.SYMBOL)
                    .col("y", ColumnType.VARCHAR)
                    .col("a1", ColumnType.DOUBLE)
                    .timestamp();
            AbstractCairoTest.create(model);
            CountDownLatch released = createTableCommitNotifier("mytable", walEnabled ? 2 : 1);
            // send text double to symbol column
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V1)
                    .build()) {
                sender.table("mytable")
                        .doubleColumn("x", 9999.0)
                        .stringColumn("y", "ystr")
                        .doubleColumn("a1", 1)
                        .at(100000000000L, ChronoUnit.MICROS);
                sender.flush();
            }
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {
                // insert binary double to symbol column
                sender.table("mytable")
                        .doubleColumn("x", 10000.0)
                        .stringColumn("y", "ystr")
                        .doubleColumn("a1", 1)
                        .at(100000000001L, ChronoUnit.MICROS);
                sender.flush();
                waitTableWriterFinish(released);

                // insert binary double to string column
                sender.table("mytable")
                        .symbol("x", "x1")
                        .doubleColumn("y", 9999.0)
                        .doubleColumn("a1", 1)
                        .at(100000000000L, ChronoUnit.MICROS);
                sender.flush();
                // insert string to double column
                sender.table("mytable")
                        .symbol("x", "x1")
                        .stringColumn("y", "ystr")
                        .stringColumn("a1", "11.u")
                        .at(100000000000L, ChronoUnit.MICROS);
                sender.flush();
                // insert array column to double
                sender.table("mytable")
                        .symbol("x", "x1")
                        .stringColumn("y", "ystr")
                        .doubleArray("a1", new double[]{1.0, 2.0})
                        .at(100000000000L, ChronoUnit.MICROS);
                sender.flush();
            }

            assertTableSizeEventually(engine, "mytable", 2);
            try (TableReader reader = getReader("mytable")) {
                TestUtils.assertReader("""
                        x\ty\ta1\ttimestamp
                        9999.0\tystr\t1.0\t1970-01-02T03:46:40.000000Z
                        10000.0\tystr\t1.0\t1970-01-02T03:46:40.000001Z
                        """, reader, sink);
            }

        });
    }

    @Test
    public void testInsertDecimalTextFormatBasic() throws Exception {
        runInContext(r -> {
            String tableName = "decimal_text_format_basic";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("price", ColumnType.getDecimalType(10, 2))
                    .col("quantity", ColumnType.getDecimalType(15, 4))
                    .col("rate", ColumnType.getDecimalType(8, 5));
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);

            CountDownLatch released = createTableCommitNotifier(tableName);

            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                // Basic positive decimal
                sender.table(tableName)
                        .decimalColumn("price", "123.45")
                        .decimalColumn("quantity", "100.0000")
                        .decimalColumn("rate", "0.12345")
                        .at(100000000000L, ChronoUnit.MICROS);

                // Negative decimal
                sender.table(tableName)
                        .decimalColumn("price", "-45.67")
                        .decimalColumn("quantity", "-10.5000")
                        .decimalColumn("rate", "-0.00001")
                        .at(100000000001L, ChronoUnit.MICROS);

                // Small values
                sender.table(tableName)
                        .decimalColumn("price", "0.01")
                        .decimalColumn("quantity", "0.0001")
                        .decimalColumn("rate", "0.00000")
                        .at(100000000002L, ChronoUnit.MICROS);

                // Integer strings (no decimal point)
                sender.table(tableName)
                        .decimalColumn("price", "999")
                        .decimalColumn("quantity", "42")
                        .decimalColumn("rate", "1")
                        .at(100000000003L, ChronoUnit.MICROS);

                sender.flush();
            }
            waitTableWriterFinish(released);

            try (TableReader reader = getReader(tableName)) {
                CharSequence suffix = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "Z\n" : "000Z\n";
                TestUtils.assertReader("price\tquantity\trate\ttimestamp\n" +
                        "123.45\t100.0000\t0.12345\t1970-01-02T03:46:40.000000" + suffix +
                        "-45.67\t-10.5000\t-0.00001\t1970-01-02T03:46:40.000001" + suffix +
                        "0.01\t0.0001\t0.00000\t1970-01-02T03:46:40.000002" + suffix +
                        "999.00\t42.0000\t1.00000\t1970-01-02T03:46:40.000003" + suffix, reader, sink);
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatEdgeCases() throws Exception {
        runInContext(r -> {
            String tableName = "decimal_text_format_edge_cases";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("value", ColumnType.getDecimalType(20, 10));
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);

            CountDownLatch released = createTableCommitNotifier(tableName);

            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                // Explicit positive sign
                sender.table(tableName)
                        .decimalColumn("value", "+123.456")
                        .at(100000000000L, ChronoUnit.MICROS);

                // Leading zeros
                sender.table(tableName)
                        .decimalColumn("value", "000123.450000")
                        .at(100000000001L, ChronoUnit.MICROS);

                // Very small value
                sender.table(tableName)
                        .decimalColumn("value", "0.0000000001")
                        .at(100000000002L, ChronoUnit.MICROS);

                // Zero with decimal point
                sender.table(tableName)
                        .decimalColumn("value", "0.0")
                        .at(100000000003L, ChronoUnit.MICROS);

                // Just zero
                sender.table(tableName)
                        .decimalColumn("value", "0")
                        .at(100000000004L, ChronoUnit.MICROS);

                sender.flush();
            }
            waitTableWriterFinish(released);

            try (TableReader reader = getReader(tableName)) {
                CharSequence suffix = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "Z\n" : "000Z\n";
                TestUtils.assertReader("value\ttimestamp\n" +
                        "123.4560000000\t1970-01-02T03:46:40.000000" + suffix +
                        "123.4500000000\t1970-01-02T03:46:40.000001" + suffix +
                        "0.0000000001\t1970-01-02T03:46:40.000002" + suffix +
                        "0.0000000000\t1970-01-02T03:46:40.000003" + suffix +
                        "0.0000000000\t1970-01-02T03:46:40.000004" + suffix, reader, sink);
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatEquivalence() throws Exception {
        runInContext(r -> {
            String tableName = "decimal_text_format_equivalence";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("text_format", ColumnType.getDecimalType(10, 3))
                    .col("binary_format", ColumnType.getDecimalType(10, 3));
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);

            CountDownLatch released = createTableCommitNotifier(tableName);

            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                // Test various values sent via both text and binary formats
                sender.table(tableName)
                        .decimalColumn("text_format", "123.450")
                        .decimalColumn("binary_format", Decimal256.fromLong(123450, 3))
                        .at(100000000000L, ChronoUnit.MICROS);

                sender.table(tableName)
                        .decimalColumn("text_format", "-45.670")
                        .decimalColumn("binary_format", Decimal256.fromLong(-45670, 3))
                        .at(100000000001L, ChronoUnit.MICROS);

                sender.table(tableName)
                        .decimalColumn("text_format", "0.001")
                        .decimalColumn("binary_format", Decimal256.fromLong(1, 3))
                        .at(100000000002L, ChronoUnit.MICROS);

                sender.flush();
            }
            waitTableWriterFinish(released);

            try (TableReader reader = getReader(tableName)) {
                CharSequence suffix = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "Z\n" : "000Z\n";
                TestUtils.assertReader("text_format\tbinary_format\ttimestamp\n" +
                        "123.450\t123.450\t1970-01-02T03:46:40.000000" + suffix +
                        "-45.670\t-45.670\t1970-01-02T03:46:40.000001" + suffix +
                        "0.001\t0.001\t1970-01-02T03:46:40.000002" + suffix, reader, sink);
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatInvalid() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                sender.table("test");
                // Test invalid characters
                try {
                    sender.decimalColumn("value", "abc");
                    Assert.fail("Letters should throw exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
                }

                // Test multiple dots
                try {
                    sender.decimalColumn("value", "12.34.56");
                    Assert.fail("Multiple dots should throw exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
                }

                // Test multiple signs
                try {
                    sender.decimalColumn("value", "+-123");
                    Assert.fail("Multiple signs should throw exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
                }

                // Test special characters
                try {
                    sender.decimalColumn("value", "12$34");
                    Assert.fail("Special characters should throw exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
                }

                // Test empty decimal
                try {
                    sender.decimalColumn("value", "");
                    Assert.fail("Empty string should throw exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
                }

                // Test invalid exponent
                try {
                    sender.decimalColumn("value", "1.23eABC");
                    Assert.fail("Invalid exponent should throw exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
                }

                // Test incomplete exponent
                try {
                    sender.decimalColumn("value", "1.23e");
                    Assert.fail("Incomplete exponent should throw exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
                }
            }
        });
    }


    @Test
    public void testInsertDecimalTextFormatPrecisionOverflow() throws Exception {
        runInContext(r -> {
            String tableName = "decimal_text_format_precision_overflow";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("x", ColumnType.getDecimalType(6, 3));
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);

            CountDownLatch released = createTableCommitNotifier(tableName);

            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                // Value that exceeds column precision (6 digits total, 3 after decimal)
                // 1000.000 has 7 digits precision, should be rejected
                sender.table(tableName)
                        .decimalColumn("x", "1000.000")
                        .at(100000000000L, ChronoUnit.MICROS);

                // Another value that exceeds precision
                sender.table(tableName)
                        .decimalColumn("x", "12345.678")
                        .at(100000000001L, ChronoUnit.MICROS);

                sender.flush();
            }
            waitTableWriterFinish(released);

            try (TableReader reader = getReader(tableName)) {
                TestUtils.assertReader("x\ttimestamp\n", reader, sink);
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatScientificNotation() throws Exception {
        runInContext(r -> {
            String tableName = "decimal_text_format_scientific_notation";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("large", ColumnType.getDecimalType(15, 2))
                    .col("small", ColumnType.getDecimalType(20, 15));
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);

            CountDownLatch released = createTableCommitNotifier(tableName);

            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                // Scientific notation with positive exponent
                sender.table(tableName)
                        .decimalColumn("large", "1.23e5")
                        .decimalColumn("small", "1.23e-10")
                        .at(100000000000L, ChronoUnit.MICROS);

                // Scientific notation with uppercase E
                sender.table(tableName)
                        .decimalColumn("large", "4.56E3")
                        .decimalColumn("small", "4.56E-8")
                        .at(100000000001L, ChronoUnit.MICROS);

                // Negative value with scientific notation
                sender.table(tableName)
                        .decimalColumn("large", "-9.99e2")
                        .decimalColumn("small", "-1.5e-12")
                        .at(100000000002L, ChronoUnit.MICROS);

                sender.flush();
            }
            waitTableWriterFinish(released);


            try (TableReader reader = getReader(tableName)) {
                CharSequence suffix = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "Z\n" : "000Z\n";
                TestUtils.assertReader("large\tsmall\ttimestamp\n" +
                        "123000.00\t0.000000000123000\t1970-01-02T03:46:40.000000" + suffix +
                        "4560.00\t0.000000045600000\t1970-01-02T03:46:40.000001" + suffix +
                        "-999.00\t-0.000000000001500\t1970-01-02T03:46:40.000002" + suffix, reader, sink);
            }
        });
    }

    @Test
    public void testInsertDecimalTextFormatTrailingZeros() throws Exception {
        runInContext(r -> {
            String tableName = "decimal_text_format_trailing_zeros";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("value1", ColumnType.getDecimalType(10, 3))
                    .col("value2", ColumnType.getDecimalType(12, 5));
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);

            CountDownLatch released = createTableCommitNotifier(tableName);

            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                // Trailing zeros should be preserved in scale
                sender.table(tableName)
                        .decimalColumn("value1", "100.000")
                        .decimalColumn("value2", "50.00000")
                        .at(100000000000L, ChronoUnit.MICROS);

                sender.table(tableName)
                        .decimalColumn("value1", "1.200")
                        .decimalColumn("value2", "0.12300")
                        .at(100000000001L, ChronoUnit.MICROS);

                sender.table(tableName)
                        .decimalColumn("value1", "0.100")
                        .decimalColumn("value2", "0.00100")
                        .at(100000000002L, ChronoUnit.MICROS);

                sender.flush();
            }
            waitTableWriterFinish(released);

            try (TableReader reader = getReader(tableName)) {
                CharSequence suffix = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "Z\n" : "000Z\n";
                TestUtils.assertReader("value1\tvalue2\ttimestamp\n" +
                        "100.000\t50.00000\t1970-01-02T03:46:40.000000" + suffix +
                        "1.200\t0.12300\t1970-01-02T03:46:40.000001" + suffix +
                        "0.100\t0.00100\t1970-01-02T03:46:40.000002" + suffix, reader, sink);
            }
        });
    }

    @Test
    public void testInsertDecimals() throws Exception {
        runInContext(r -> {
            String tableName = "decimal_test";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.getDecimalType(9, 0))
                    .col("b", ColumnType.getDecimalType(9, 3));
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);

            CountDownLatch released = createTableCommitNotifier(tableName);
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                sender.table(tableName)
                        .decimalColumn("a", Decimal256.fromLong(12345, 0))
                        .decimalColumn("b", Decimal256.fromLong(12345, 2))
                        .at(100000000000L, ChronoUnit.MICROS);

                // Decimal without rescale
                sender.table(tableName)
                        .decimalColumn("a", Decimal256.NULL_VALUE)
                        .decimalColumn("b", Decimal256.fromLong(123456, 3))
                        .at(100000000001L, ChronoUnit.MICROS);

                // Integers -> Decimal
                sender.table(tableName)
                        .longColumn("a", 42)
                        .longColumn("b", 42)
                        .at(100000000002L, ChronoUnit.MICROS);

                // Strings -> Decimal without rescale
                sender.table(tableName)
                        .stringColumn("a", "42")
                        .stringColumn("b", "42.123")
                        .at(100000000003L, ChronoUnit.MICROS);

                // Strings -> Decimal with rescale
                sender.table(tableName)
                        .stringColumn("a", "42.0")
                        .stringColumn("b", "42.1")
                        .at(100000000004L, ChronoUnit.MICROS);

                // Doubles -> Decimal
                sender.table(tableName)
                        .doubleColumn("a", 42d)
                        .doubleColumn("b", 42.1d)
                        .at(100000000005L, ChronoUnit.MICROS);

                // NaN/Inf Doubles -> Decimal
                sender.table(tableName)
                        .doubleColumn("a", Double.NaN)
                        .doubleColumn("b", Double.POSITIVE_INFINITY)
                        .at(100000000006L, ChronoUnit.MICROS);
                sender.flush();
            }
            waitTableWriterFinish(released);

            assertTableSizeEventually(engine, "decimal_test", 7);
            try (TableReader reader = getReader(tableName)) {
                CharSequence suffix = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "Z\n" : "000Z\n";
                TestUtils.assertReader("a\tb\ttimestamp\n" +
                        "12345\t123.450\t1970-01-02T03:46:40.000000" + suffix +
                        "\t123.456\t1970-01-02T03:46:40.000001" + suffix +
                        "42\t42.000\t1970-01-02T03:46:40.000002" + suffix +
                        "42\t42.123\t1970-01-02T03:46:40.000003" + suffix +
                        "42\t42.100\t1970-01-02T03:46:40.000004" + suffix +
                        "42\t42.100\t1970-01-02T03:46:40.000005" + suffix +
                        "\t\t1970-01-02T03:46:40.000006" + suffix, reader, sink);
            }
        });
    }

    @Test
    public void testInsertInvalidDecimals() throws Exception {
        runInContext(r -> {
            String tableName = "invalid_decimal_test";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("x", ColumnType.getDecimalType(6, 3))
                    .col("y", ColumnType.getDecimalType(76, 73));
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);

            CountDownLatch released = createTableCommitNotifier(tableName);
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V3)
                    .build()
            ) {
                // Integers out of bound (with scaling, 1234 becomes 1234.000 which have a precision of 7).
                sender.table(tableName)
                        .longColumn("x", 1234)
                        .at(100000000000L, ChronoUnit.MICROS);

                // Integers overbound during the rescale process.
                sender.table(tableName)
                        .longColumn("y", 12345)
                        .at(100000000001L, ChronoUnit.MICROS);

                // Floating points with a scale greater than expected.
                sender.table(tableName)
                        .doubleColumn("x", 1.2345d)
                        .at(100000000002L, ChronoUnit.MICROS);

                // Floating points with a precision greater than expected.
                sender.table(tableName)
                        .doubleColumn("x", 12345.678d)
                        .at(100000000003L, ChronoUnit.MICROS);

                // String that is not a valid decimal.
                sender.table(tableName)
                        .stringColumn("x", "abc")
                        .at(100000000004L, ChronoUnit.MICROS);

                // String that has a too big precision.
                sender.table(tableName)
                        .stringColumn("x", "1E8")
                        .at(100000000005L, ChronoUnit.MICROS);

                // Decimal with a too big precision.
                sender.table(tableName)
                        .decimalColumn("x", Decimal256.fromLong(12345678, 3))
                        .at(100000000006L, ChronoUnit.MICROS);

                // Decimal with a too big precision when scaled.
                sender.table(tableName)
                        .decimalColumn("y", Decimal256.fromLong(12345, 0))
                        .at(100000000007L, ChronoUnit.MICROS);
                sender.flush();
            }
            waitTableWriterFinish(released);

            try (TableReader reader = getReader(tableName)) {
                TestUtils.assertReader("x\ty\ttimestamp\n", reader, sink);
            }
        });
    }

    @Test
    public void testInsertLargeArray() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        Assume.assumeTrue(walEnabled);
        String tableName = "arr_large_test";
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()
            ) {
                CountDownLatch released = createTableCommitNotifier(tableName);
                double[] arr = createDoubleArray(100_000_000);
                sender.table(tableName)
                        .doubleArray("arr", arr)
                        .at(100000000000L, ChronoUnit.MICROS);
                sender.flush();
                waitTableWriterFinish(released);
            }
            drainWalQueue();
            assertTableSizeEventually(engine, tableName, 1);
        });
    }

    @Test
    public void testInsertNonAsciiStringAndUuid() throws Exception {
        // this is to check that a non-ASCII string will not prevent
        // parsing a subsequent UUID
        runInContext(r -> {
            TableModel model = new TableModel(configuration, "mytable", PartitionBy.NONE)
                    .col("s", ColumnType.STRING)
                    .col("u", ColumnType.UUID);
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);
            CountDownLatch released = createTableCommitNotifier("mytable");

            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                long tsMicros = MicrosTimestampDriver.floor("2022-02-25");
                sender.table("mytable")
                        .stringColumn("s", "non-ascii äöü")
                        .stringColumn("u", "11111111-2222-3333-4444-555555555555")
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.flush();
            }

            waitTableWriterFinish(released);
            assertTableSizeEventually(engine, "mytable", 1);
            try (TableReader reader = getReader("mytable")) {
                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                        ? """
                        s\tu\ttimestamp
                        non-ascii äöü\t11111111-2222-3333-4444-555555555555\t2022-02-25T00:00:00.000000Z
                        """
                        : """
                        s\tu\ttimestamp
                        non-ascii äöü\t11111111-2222-3333-4444-555555555555\t2022-02-25T00:00:00.000000000Z
                        """;
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    @Test
    public void testInsertNonAsciiStringIntoUuidColumn() throws Exception {
        // carefully crafted value so when encoded as UTF-8 it has the same byte length as a proper UUID
        testValueCannotBeInsertedToUuidColumn("11111111-1111-1111-1111-1111111111ü");
    }

    @Test
    public void testInsertStringIntoUuidColumn() throws Exception {
        runInContext(r -> {
            // create table with UUID column
            TableModel model = new TableModel(configuration, "mytable", PartitionBy.NONE)
                    .col("u1", ColumnType.UUID)
                    .col("u2", ColumnType.UUID)
                    .col("u3", ColumnType.UUID);
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);

            CountDownLatch released = createTableCommitNotifier("mytable");
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                long tsMicros = MicrosTimestampDriver.floor("2022-02-25");
                sender.table("mytable")
                        .stringColumn("u1", "11111111-1111-1111-1111-111111111111")
                        // u2 empty -> insert as null
                        .stringColumn("u3", "33333333-3333-3333-3333-333333333333")
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.flush();
            }

            waitTableWriterFinish(released);
            assertTableSizeEventually(engine, "mytable", 1);
            try (TableReader reader = getReader("mytable")) {
                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                        ? """
                        u1\tu2\tu3\ttimestamp
                        11111111-1111-1111-1111-111111111111\t\t33333333-3333-3333-3333-333333333333\t2022-02-25T00:00:00.000000Z
                        """
                        : """
                        u1\tu2\tu3\ttimestamp
                        11111111-1111-1111-1111-111111111111\t\t33333333-3333-3333-3333-333333333333\t2022-02-25T00:00:00.000000000Z
                        """;
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    @Test
    public void testInsertTimestampAsInstant() throws Exception {
        runInContext(r -> {
            TableModel model = new TableModel(configuration, "mytable", PartitionBy.YEAR)
                    .col("ts_col", ColumnType.TIMESTAMP);
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }

            AbstractCairoTest.create(model);
            CountDownLatch released = createTableCommitNotifier("mytable");
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                sender.table("mytable")
                        .timestampColumn("ts_col", Instant.parse("2023-02-11T12:30:11.35Z"))
                        .at(Instant.parse("2022-01-10T20:40:22.54Z"));
                sender.flush();
            }

            waitTableWriterFinish(released);
            assertTableSizeEventually(engine, "mytable", 1);
            try (TableReader reader = getReader("mytable")) {
                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                        ? """
                        ts_col\ttimestamp
                        2023-02-11T12:30:11.350000Z\t2022-01-10T20:40:22.540000Z
                        """
                        : """
                        ts_col\ttimestamp
                        2023-02-11T12:30:11.350000Z\t2022-01-10T20:40:22.540000000Z
                        """;
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    @Test
    public void testInsertTimestampMiscUnits() throws Exception {
        runInContext(r -> {
            TableModel model = new TableModel(configuration, "mytable", PartitionBy.YEAR)
                    .col("unit", ColumnType.STRING)
                    .col("ts", ColumnType.TIMESTAMP);
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);
            CountDownLatch released = createTableCommitNotifier("mytable");
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                long tsMicros = MicrosTimestampDriver.floor("2023-09-18T12:01:01.01Z");
                sender.table("mytable")
                        .stringColumn("unit", "ns")
                        .timestampColumn("ts", tsMicros * 1000, ChronoUnit.NANOS)
                        .at(tsMicros * 1000, ChronoUnit.NANOS);
                sender.table("mytable")
                        .stringColumn("unit", "us")
                        .timestampColumn("ts", tsMicros, ChronoUnit.MICROS)
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.table("mytable")
                        .stringColumn("unit", "ms")
                        .timestampColumn("ts", tsMicros / 1000, ChronoUnit.MILLIS)
                        .at(tsMicros / 1000, ChronoUnit.MILLIS);
                sender.table("mytable")
                        .stringColumn("unit", "s")
                        .timestampColumn("ts", tsMicros / Micros.SECOND_MICROS, ChronoUnit.SECONDS)
                        .at(tsMicros / Micros.SECOND_MICROS, ChronoUnit.SECONDS);
                sender.table("mytable")
                        .stringColumn("unit", "m")
                        .timestampColumn("ts", tsMicros / Micros.MINUTE_MICROS, ChronoUnit.MINUTES)
                        .at(tsMicros / Micros.MINUTE_MICROS, ChronoUnit.MINUTES);
                sender.flush();
            }

            waitTableWriterFinish(released);
            assertTableSizeEventually(engine, "mytable", 5);
            try (TableReader reader = getReader("mytable")) {
                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                        ? """
                        unit\tts\ttimestamp
                        m\t2023-09-18T12:01:00.000000Z\t2023-09-18T12:01:00.000000Z
                        s\t2023-09-18T12:01:01.000000Z\t2023-09-18T12:01:01.000000Z
                        ns\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000Z
                        us\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000Z
                        ms\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000Z
                        """
                        : """
                        unit\tts\ttimestamp
                        m\t2023-09-18T12:01:00.000000Z\t2023-09-18T12:01:00.000000000Z
                        s\t2023-09-18T12:01:01.000000Z\t2023-09-18T12:01:01.000000000Z
                        ns\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000000Z
                        us\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000000Z
                        ms\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000000Z
                        """;
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    @Test
    public void testInsertTimestampNanoOverflow() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext(r -> {
            TableModel model = new TableModel(configuration, "mytable", PartitionBy.YEAR)
                    .col("ts", ColumnType.TIMESTAMP).timestamp();
            AbstractCairoTest.create(model);
            CountDownLatch released = createTableCommitNotifier("mytable");
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                long tsMicros = MicrosTimestampDriver.floor("2323-09-18T12:01:01.011568901Z");
                sender.table("mytable")
                        .timestampColumn("ts", tsMicros, ChronoUnit.MICROS)
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.flush();
            }

            waitTableWriterFinish(released);
            assertTableSizeEventually(engine, "mytable", 1);
            try (TableReader reader = getReader("mytable")) {
                String expected = """
                        ts\ttimestamp
                        2323-09-18T12:01:01.011568Z\t2323-09-18T12:01:01.011568Z
                        """;
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    @Test
    public void testInsertTimestampNanoUnits() throws Exception {
        runInContext(r -> {
            TableModel model = new TableModel(configuration, "mytable", PartitionBy.YEAR)
                    .col("unit", ColumnType.STRING)
                    .col("ts", ColumnType.TIMESTAMP);
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);
            CountDownLatch released = createTableCommitNotifier("mytable");
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                long tsNanos = NanosTimestampDriver.floor("2023-09-18T12:01:01.011568901Z");
                sender.table("mytable")
                        .stringColumn("unit", "ns")
                        .timestampColumn("ts", tsNanos, ChronoUnit.NANOS)
                        .at(tsNanos, ChronoUnit.NANOS);
                sender.flush();
            }

            waitTableWriterFinish(released);
            assertTableSizeEventually(engine, "mytable", 1);
            try (TableReader reader = getReader("mytable")) {
                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                        ? """
                        unit\tts\ttimestamp
                        ns\t2023-09-18T12:01:01.011568Z\t2023-09-18T12:01:01.011568Z
                        """
                        : """
                        unit\tts\ttimestamp
                        ns\t2023-09-18T12:01:01.011568Z\t2023-09-18T12:01:01.011568901Z
                        """;
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    @Test
    public void testMaxNameLength() throws Exception {
        runInContext(r -> {
            PlainTcpLineChannel channel = new PlainTcpLineChannel(NetworkFacadeImpl.INSTANCE, HOST, bindPort, 1024);
            try (AbstractLineTcpSender sender = new LineTcpSenderV2(channel, 1024, 20)) {
                try {
                    sender.table("table_with_long______________________name");
                    fail();
                } catch (LineSenderException e) {
                    assertContains(e.getMessage(), "table name is too long: [name = table_with_long______________________name, maxNameLength=20]");
                }

                try {
                    sender.table("tab")
                            .doubleColumn("column_with_long______________________name", 1.0);
                    fail();
                } catch (LineSenderException e) {
                    assertContains(e.getMessage(), "column name is too long: [name = column_with_long______________________name, maxNameLength=20]");
                }
            }
        });
    }

    @Test
    public void testMinBufferSizeWhenAuth() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        int tinyCapacity = 42;
        runInContext(r -> {
            try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, bindPort, tinyCapacity)) {
                sender.authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "challenge did not fit into buffer");
            }
        });
    }

    @Test
    public void testMultipleVarcharCols() throws Exception {
        final boolean isMicros = ColumnType.isTimestampMicro(timestampType.getTimestampType());
        useLegacyStringDefault = false;
        runInContext(r -> {
            String table = "string_table";
            CountDownLatch released = createTableCommitNotifier(table);
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .build()
            ) {
                long ts = isMicros ? MicrosTimestampDriver.floor("2024-02-27") : NanosTimestampDriver.floor("2024-02-27");
                sender.table(table)
                        .stringColumn("string1", "some string")
                        .stringColumn("string2", "another string")
                        .stringColumn("string3", "yet another string")
                        .at(ts, isMicros ? ChronoUnit.MICROS : ChronoUnit.NANOS);
                sender.flush();
                waitTableWriterFinish(released);
                assertTableSizeEventually(engine, table, 1);
                try (RecordCursorFactory fac = engine.select(table, sqlExecutionContext);
                     RecordCursor cursor = fac.getCursor(sqlExecutionContext)
                ) {
                    TestUtils.assertCursor(
                            "some string:VARCHAR\tanother string:VARCHAR\tyet another string:VARCHAR\t2024-02-27T00:00:00.000000Z:TIMESTAMP\n",
                            cursor, fac.getMetadata(), false, true, sink
                    );
                }
            }
        });
    }

    @Test
    public void testServerIgnoresUnfinishedRows() throws Exception {
        Assume.assumeTrue(!walEnabled);
        String tableName = "myTable";
        runInContext(r -> {
            send(tableName, WAIT_ENGINE_TABLE_RELEASE, () -> {
                try (Sender sender = Sender.builder(Sender.Transport.TCP)
                        .address("127.0.0.1")
                        .port(bindPort)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .build()) {
                    // well-formed row first
                    sender.table(tableName).longColumn("field0", 42)
                            .longColumn("field1", 42)
                            .atNow();

                    // failed validation
                    sender.table(tableName)
                            .longColumn("field0", 42)
                            .longColumn("field1\n", 42);
                    fail("validation should have failed");
                } catch (LineSenderException e) {
                    // ignored
                }
            });
            // make sure the 2nd unfinished row was not inserted by the server
            try (
                    RecordCursorFactory factory = engine.select(tableName, sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                assertEquals(1, cursor.size());
            }
        });
    }

    @Test
    public void testSymbolCapacityReloadFuzz() throws Exception {
        // this test should pass on desktop withing 3-6s for WAL
        // without errors. Anything else including timeouts is unexpected and is a regression
        setProperty(PropertyKey.CAIRO_AUTO_SCALE_SYMBOL_CAPACITY, "true");
        String confString = "tcp::addr=localhost:" + bindPort + ";protocol_version=2;";
        final int N = 1_000_000;
        runInContext(r -> {
            try (
                    Sender sender1 = Sender.fromConfig(confString);
                    Sender sender2 = Sender.fromConfig(confString)
            ) {
                SOCountDownLatch writerIsBack = new SOCountDownLatch(1);
                engine.setPoolListener(
                        (factoryType, thread, tableToken, event, segment, position) -> {
                            if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                                writerIsBack.countDown();
                            }
                        }
                );

                AtomicBoolean walRunning = new AtomicBoolean(true);
                SOCountDownLatch doneAll = new SOCountDownLatch(3);
                SOCountDownLatch doneILP = new SOCountDownLatch(2);
                Rnd rnd = TestUtils.generateRandom(null);
                long seed1a = rnd.nextLong();
                long seed1b = rnd.nextLong();
                long seed2a = rnd.nextLong();
                long seed2b = rnd.nextLong();
                AtomicInteger errorCount = new AtomicInteger();

                new Thread(() -> {
                    try {
                        Rnd rnd1 = new Rnd(seed1a, seed1b);
                        for (int i = 0; i < N; i++) {
                            sender1.table("mytable")
                                    .symbol("sym1", rnd1.nextString(10))
                                    .symbol("sym2", rnd1.nextString(2))
                                    .doubleColumn("dd", rnd1.nextDouble())
                                    .atNow();
                        }
                        sender1.flush();
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errorCount.incrementAndGet();
                    } finally {
                        Path.clearThreadLocals();
                        doneAll.countDown();
                        doneILP.countDown();
                    }
                }).start();

                new Thread(() -> {
                    try {
                        Rnd rnd2 = new Rnd(seed2a, seed2b);
                        for (int i = 0; i < N; i++) {
                            sender2.table("mytable")
                                    .symbol("sym1", rnd2.nextString(10))
                                    .symbol("sym2", rnd2.nextString(2))
                                    .doubleColumn("dd", rnd2.nextDouble())
                                    .atNow();
                        }
                        sender2.flush();
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errorCount.incrementAndGet();
                    } finally {
                        Path.clearThreadLocals();
                        doneAll.countDown();
                        doneILP.countDown();
                    }
                }).start();

                new Thread(() -> {
                    try {
                        while (walRunning.get()) {
                            drainWalQueue();
                            Os.pause();
                        }
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errorCount.incrementAndGet();
                    } finally {
                        Path.clearThreadLocals();
                        doneAll.countDown();
                    }
                }).start();

                doneILP.await();
                walRunning.set(false);
                doneAll.await();

                Assert.assertEquals(0, errorCount.get());

                // make sure to assert before closing the Sender
                // since the Sender will always flush on close
                writerIsBack.await();
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "select count() from mytable",
                        sink,
                        "count\n" +
                                N * 2 + "\n"
                );
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
        testTimestampIngest(timestampType.getTypeName(), PROTOCOL_VERSION_V1,
                timestampType == TestTimestampType.NANO
                        ? """
                        ts\tdts
                        2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129092Z
                        """
                        : """
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
        testTimestampIngest(timestampType.getTypeName(), PROTOCOL_VERSION_V2,
                timestampType == TestTimestampType.NANO
                        ? """
                        ts\tdts
                        2025-11-19T10:55:24.123456789Z	2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123000000Z	2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123456789Z	2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123000000Z	2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123456789Z	2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123000000Z	2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123456799Z	2025-11-20T10:55:24.834129092Z
                        """
                        : """
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
                timestampType == TestTimestampType.NANO
                        ? null
                        : """
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
    public void testUnfinishedRowDoesNotContainNewLine() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            sender.boolColumn("col\n", true);
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "name contains an illegal char");
        }
        assertFalse(channel.contain(new byte[]{(byte) '\n'}));
    }

    @Test
    public void testUseAfterClose_atMicros() {
        assertExceptionOnClosedSender(s -> {
            s.table("mytable");
            s.longColumn("col", 42);
        }, s -> s.at(MicrosecondClockImpl.INSTANCE.getTicks(), ChronoUnit.MICROS));
    }

    @Test
    public void testUseAfterClose_atNow() {
        assertExceptionOnClosedSender(s -> {
            s.table("mytable");
            s.longColumn("col", 42);
        }, Sender::atNow);
    }

    @Test
    public void testUseAfterClose_boolColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.boolColumn("col", true));
    }

    @Test
    public void testUseAfterClose_doubleColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.doubleColumn("col", 42.42));
    }

    @Test
    public void testUseAfterClose_flush() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, Sender::flush);
    }

    @Test
    public void testUseAfterClose_longColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.longColumn("col", 42));
    }

    @Test
    public void testUseAfterClose_stringColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.stringColumn("col", "val"));
    }

    @Test
    public void testUseAfterClose_symbol() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.symbol("sym", "val"));
    }

    @Test
    public void testUseAfterClose_table() {
        assertExceptionOnClosedSender();
    }

    @Test
    public void testUseAfterClose_tsColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.timestampColumn("col", 0, ChronoUnit.MICROS));
    }

    @Test
    public void testUseVarcharAsString() throws Exception {
        final boolean isMicros = ColumnType.isTimestampMicro(timestampType.getTimestampType());
        useLegacyStringDefault = false;
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()
            ) {
                String table = "string_table";
                CountDownLatch released = createTableCommitNotifier(table);
                long ts = isMicros ? MicrosTimestampDriver.floor("2024-02-27") : NanosTimestampDriver.floor("2024-02-27");
                String expectedValue = "čćžšđçğéíáýůř";
                sender.table(table)
                        .stringColumn("string1", expectedValue)
                        .at(ts, isMicros ? ChronoUnit.MICROS : ChronoUnit.NANOS);
                sender.flush();
                waitTableWriterFinish(released);
                assertTableSizeEventually(engine, table, 1);
                try (RecordCursorFactory fac = engine.select(table, sqlExecutionContext);
                     RecordCursor cursor = fac.getCursor(sqlExecutionContext)
                ) {
                    String expectTs = isMicros
                            ? "\t2024-02-27T00:00:00.000000Z:TIMESTAMP\n"
                            : "\t2024-02-27T00:00:00.000000000Z:TIMESTAMP_NS\n";
                    TestUtils.assertCursor(
                            "čćžšđçğéíáýůř:" + ColumnType.nameOf(ColumnType.VARCHAR) + expectTs,
                            cursor, fac.getMetadata(), false, true, sink);
                }
            }
        });
    }

    @Test
    public void testWriteAllTypes() throws Exception {
        final boolean isMicros = ColumnType.isTimestampMicro(timestampType.getTimestampType());
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {
                CountDownLatch released = createTableCommitNotifier("mytable");
                long ts = isMicros ? MicrosTimestampDriver.floor("2022-02-25") : NanosTimestampDriver.floor("2022-02-25");
                sender.table("mytable")
                        .longColumn("int_field", 42)
                        .boolColumn("bool_field", true)
                        .stringColumn("string_field", "foo")
                        .doubleColumn("double_field", 42.0)
                        .timestampColumn("ts_field", ts, isMicros ? ChronoUnit.MICROS : ChronoUnit.NANOS)
                        .at(ts, isMicros ? ChronoUnit.MICROS : ChronoUnit.NANOS);
                sender.flush();
                waitTableWriterFinish(released);
            }

            assertTableSizeEventually(engine, "mytable", 1);
            try (TableReader reader = getReader("mytable")) {
                String expected = isMicros
                        ? """
                        int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp
                        42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000Z\t2022-02-25T00:00:00.000000Z
                        """
                        : """
                        int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp
                        42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000000Z\t2022-02-25T00:00:00.000000000Z
                        """;
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    @Test
    public void testWriteLongMinMax() throws Exception {
        final boolean isMicros = ColumnType.isTimestampMicro(timestampType.getTimestampType());
        runInContext(r -> {
            String table = "table";
            CountDownLatch released = createTableCommitNotifier("table");
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                long ts = isMicros ? MicrosTimestampDriver.floor("2023-02-22") : NanosTimestampDriver.floor("2023-02-22");
                sender.table(table)
                        .longColumn("max", Long.MAX_VALUE)
                        .longColumn("min", Long.MIN_VALUE)
                        .at(ts, isMicros ? ChronoUnit.MICROS : ChronoUnit.NANOS);
                sender.flush();
            }

            waitTableWriterFinish(released);
            assertTableSizeEventually(engine, table, 1);
            try (TableReader reader = getReader(table)) {
                String expected = isMicros
                        ? """
                        max\tmin\ttimestamp
                        9223372036854775807\tnull\t2023-02-22T00:00:00.000000Z
                        """
                        : """
                        max\tmin\ttimestamp
                        9223372036854775807\tnull\t2023-02-22T00:00:00.000000000Z
                        """;
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    private static void assertControlCharacterException() {
        DummyLineChannel channel = new DummyLineChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            sender.boolColumn("col\u0001", true);
            fail("control character in column or table name must throw exception");
        } catch (LineSenderException e) {
            String m = e.getMessage();
            assertContains(m, "name contains an illegal char");
            assertNoControlCharacter(m);
        }
    }

    private static void assertExceptionOnClosedSender(Consumer<Sender> beforeCloseAction, Consumer<Sender> afterCloseAction) {
        DummyLineChannel channel = new DummyLineChannel();
        Sender sender = new LineTcpSenderV2(channel, 1000, 127);
        beforeCloseAction.accept(sender);
        sender.close();
        try {
            afterCloseAction.accept(sender);
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "sender already closed");
        }
    }

    private static void assertExceptionOnClosedSender() {
        assertExceptionOnClosedSender(s -> {
        }, LineTcpSenderTest.SET_TABLE_NAME_ACTION);
    }

    private static void assertNoControlCharacter(CharSequence m) {
        for (int i = 0, n = m.length(); i < n; i++) {
            assertFalse(Character.isISOControl(m.charAt(i)));
        }
    }

    private static CountDownLatch createTableCommitNotifier(String tableName) {
        return createTableCommitNotifier(tableName, 1);
    }

    private static CountDownLatch createTableCommitNotifier(String tableName, int count) {
        CountDownLatch released = new CountDownLatch(count);
        engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
            if (name != null && Chars.equalsNc(name.getTableName(), tableName)) {
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                    released.countDown();
                }
            }
        });
        return released;
    }

    private void assertSymbolsCannotBeWrittenAfterOtherType(Consumer<Sender> otherTypeWriter) throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {
                sender.table("mytable");
                otherTypeWriter.accept(sender);
                try {
                    sender.symbol("name", "value");
                    fail("symbols cannot be written after any other column type");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "before any other column types");
                    sender.atNow();
                }
            }
        });
    }

    private void testCreateTimestampColumns(long timestamp, ChronoUnit unit, int protocolVersion, int[] expectedColumnTypes, String expected) throws Exception {
        runInContext(r -> {
            CountDownLatch released = createTableCommitNotifier("tab1");
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(protocolVersion)
                    .build()
            ) {
                long ts_ns = NanosTimestampDriver.floor("2025-11-19T10:55:24.123456789Z");
                long ts_us = MicrosTimestampDriver.floor("2025-11-19T10:55:24.123456Z");
                long ts_ms = MicrosTimestampDriver.floor("2025-11-19T10:55:24.123Z") / 1000;
                Instant ts_instant = Instant.ofEpochSecond(ts_ns / 1_000_000_000, ts_ns % 1_000_000_000 + 10);

                if (unit != null) {
                    sender.table("tab1")
                            .doubleColumn("col1", 1.111)
                            .timestampColumn("ts_ns", ts_ns, ChronoUnit.NANOS)
                            .timestampColumn("ts_us", ts_us, ChronoUnit.MICROS)
                            .timestampColumn("ts_ms", ts_ms, ChronoUnit.MILLIS)
                            .timestampColumn("ts_instant", ts_instant)
                            .at(timestamp, unit);
                } else {
                    sender.table("tab1")
                            .doubleColumn("col1", 1.111)
                            .timestampColumn("ts_ns", ts_ns, ChronoUnit.NANOS)
                            .timestampColumn("ts_us", ts_us, ChronoUnit.MICROS)
                            .timestampColumn("ts_ms", ts_ms, ChronoUnit.MILLIS)
                            .timestampColumn("ts_instant", ts_instant)
                            .at(Instant.ofEpochSecond(timestamp / 1_000_000_000, timestamp % 1_000_000_000));
                }

                sender.flush();
            }

            waitTableWriterFinish(released);
            assertTableSizeEventually(engine, "tab1", 1);
            assertTable("col1\tts_ns\tts_us\tts_ms\tts_instant\ttimestamp\n" + expected + "\n", "tab1");

            final TableToken tt = engine.verifyTableName("tab1");
            try (TableMetadata metadata = engine.getTableMetadata(tt)) {
                assertEquals(6, metadata.getColumnCount());
                assertEquals(ColumnType.DOUBLE, metadata.getColumnType(0));
                assertEquals(expectedColumnTypes[0], metadata.getColumnType(1));
                assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType(2));
                assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType(3));
                assertEquals(expectedColumnTypes[1], metadata.getColumnType(4));
                assertEquals(expectedColumnTypes[2], metadata.getColumnType(5));
            }
        });
    }

    private void testTimestampIngest(String timestampType, int protocolVersion, String expected1, String expected2) throws Exception {
        runInContext(r -> {
            engine.execute("create table tab (ts " + timestampType + ", dts " + timestampType + ") timestamp(dts) partition by DAY BYPASS WAL");
            assertTableExists(engine, "tab");

            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(protocolVersion)
                    .build()
            ) {
                long ts_ns = NanosTimestampDriver.floor("2025-11-19T10:55:24.123456789Z");
                long dts_ns = NanosTimestampDriver.floor("2025-11-20T10:55:24.834129082Z");
                long ts_us = MicrosTimestampDriver.floor("2025-11-19T10:55:24.123456Z");
                long dts_us = MicrosTimestampDriver.floor("2025-11-20T10:55:24.834129Z");
                long ts_ms = MicrosTimestampDriver.floor("2025-11-19T10:55:24.123Z") / 1000;
                long dts_ms = MicrosTimestampDriver.floor("2025-11-20T10:55:24.834Z") / 1000;
                Instant tsInstant_ns = Instant.ofEpochSecond(ts_ns / 1_000_000_000, ts_ns % 1_000_000_000 + 10);
                Instant dtsInstant_ns = Instant.ofEpochSecond(dts_ns / 1_000_000_000, dts_ns % 1_000_000_000 + 10);

                sender.table("tab")
                        .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                        .at(dts_ns, ChronoUnit.NANOS);
                sender.table("tab")
                        .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                        .at(dts_ns, ChronoUnit.NANOS);
                sender.table("tab")
                        .timestampColumn("ts", ts_ms, ChronoUnit.MILLIS)
                        .at(dts_ns, ChronoUnit.NANOS);

                sender.table("tab")
                        .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                        .at(dts_us, ChronoUnit.MICROS);
                sender.table("tab")
                        .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                        .at(dts_us, ChronoUnit.MICROS);
                sender.table("tab")
                        .timestampColumn("ts", ts_ms, ChronoUnit.MILLIS)
                        .at(dts_us, ChronoUnit.MICROS);

                sender.table("tab")
                        .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                        .at(dts_ms, ChronoUnit.MILLIS);
                sender.table("tab")
                        .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                        .at(dts_ms, ChronoUnit.MILLIS);
                sender.table("tab")
                        .timestampColumn("ts", ts_ms, ChronoUnit.MILLIS)
                        .at(dts_ms, ChronoUnit.MILLIS);

                sender.table("tab")
                        .timestampColumn("ts", tsInstant_ns)
                        .at(dtsInstant_ns);

                sender.flush();

                drainWalQueue(engine);
                assertTableSizeEventually(engine, "tab", 10);
                assertTable(expected1, "tab");

                try {
                    // fails for nanos, long overflow
                    long ts_tooLargeForNanos_us = MicrosTimestampDriver.floor("2300-11-19T10:55:24.123456Z");
                    long dts_tooLargeForNanos_us = MicrosTimestampDriver.floor("2300-11-20T10:55:24.834129Z");
                    sender.table("tab")
                            .timestampColumn("ts", ts_tooLargeForNanos_us, ChronoUnit.MICROS)
                            .at(dts_tooLargeForNanos_us, ChronoUnit.MICROS);
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

                drainWalQueue(engine);
                assertTableSizeEventually(engine, "tab", expected2 == null ? 10 : 11);
                assertTable(expected2 == null ? expected1 : expected2, "tab");
            }
        });
    }

    private void testValueCannotBeInsertedToUuidColumn(String value) throws Exception {
        runInContext(r -> {
            // create table with UUID column
            TableModel model = new TableModel(configuration, "mytable", PartitionBy.NONE)
                    .col("u1", ColumnType.UUID);
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                model.timestamp();
            } else {
                model.timestampNs();
            }
            AbstractCairoTest.create(model);
            CountDownLatch released = createTableCommitNotifier("mytable", walEnabled ? 2 : 1);

            // this sender fails as the string is not UUID
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                long tsMicros = MicrosTimestampDriver.floor("2022-02-25");
                sender.table("mytable")
                        .stringColumn("u1", value)
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.flush();
            }

            // this sender succeeds as the string is in the UUID format
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                long tsMicros = MicrosTimestampDriver.floor("2022-02-25");
                sender.table("mytable")
                        .stringColumn("u1", "11111111-1111-1111-1111-111111111111")
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.flush();
            }
            waitTableWriterFinish(released);

            assertTableSizeEventually(engine, "mytable", 1);
            try (TableReader reader = getReader("mytable")) {
                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                        ? """
                        u1\ttimestamp
                        11111111-1111-1111-1111-111111111111\t2022-02-25T00:00:00.000000Z
                        """
                        : """
                        u1\ttimestamp
                        11111111-1111-1111-1111-111111111111\t2022-02-25T00:00:00.000000000Z
                        """;
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    private void waitTableWriterFinish(CountDownLatch latch) throws InterruptedException {
        latch.await();
        if (walEnabled) {
            drainWalQueue();
        }
    }

    private static class DummyLineChannel implements LineChannel {
        private int closeCounter;

        @Override
        public void close() {
            closeCounter++;
        }

        @Override
        public int errno() {
            return 0;
        }

        @Override
        public int receive(long ptr, int len) {
            return 0;
        }

        @Override
        public void send(long ptr, int len) {

        }
    }
}
