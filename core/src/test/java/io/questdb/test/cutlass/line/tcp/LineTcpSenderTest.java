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
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.client.Sender;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.line.AbstractLineTcpSender;
import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSenderV2;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.PrivateKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V2;
import static io.questdb.test.cutlass.http.line.LineHttpSenderTest.createDoubleArray;
import static io.questdb.test.tools.TestUtils.assertContains;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LineTcpSenderTest extends AbstractLineTcpReceiverTest {

    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static String AUTH_KEY_ID2_INVALID = "invalid";
    private final static int HOST = Net.parseIPv4("127.0.0.1");
    private static final Consumer<Sender> SET_TABLE_NAME_ACTION = s -> s.table("mytable");
    private final static String TOKEN = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthUtils.toPrivateKey(TOKEN);
    private final boolean walEnabled;

    public LineTcpSenderTest(WalMode walMode, TestTimestampType timestampType) {
        this.walEnabled = (walMode == WalMode.WITH_WAL);
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL, TestTimestampType.MICRO}, {WalMode.NO_WAL, TestTimestampType.MICRO},
                {WalMode.WITH_WAL, TestTimestampType.NANO}, {WalMode.NO_WAL, TestTimestampType.NANO},
        });
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
                TestUtils.assertReader("int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp\n" +
                        "42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000Z\t2022-02-25T00:00:00.000000Z\n", reader, new StringSink());
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
    public void testDouble_edgeValues() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                CountDownLatch released = createTableCommitNotifier("mytable");
                long ts = MicrosTimestampDriver.floor("2022-02-25");
                sender.table("mytable")
                        .doubleColumn("negative_inf", Double.NEGATIVE_INFINITY)
                        .doubleColumn("positive_inf", Double.POSITIVE_INFINITY)
                        .doubleColumn("nan", Double.NaN)
                        .doubleColumn("max_value", Double.MAX_VALUE)
                        .doubleColumn("min_value", Double.MIN_VALUE)
                        .at(ts, ChronoUnit.MICROS);
                sender.flush();
                waitTableWriterFinish(released);
                assertTableSizeEventually(engine, "mytable", 1);
                try (TableReader reader = getReader("mytable")) {
                    if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                        TestUtils.assertReader("negative_inf\tpositive_inf\tnan\tmax_value\tmin_value\ttimestamp\n" +
                                "null\tnull\tnull\t1.7976931348623157E308\t4.9E-324\t2022-02-25T00:00:00.000000Z\n", reader, new StringSink());
                    } else {
                        TestUtils.assertReader("negative_inf\tpositive_inf\tnan\tmax_value\tmin_value\ttimestamp\n" +
                                "null\tnull\tnull\t1.7976931348623157E308\t4.9E-324\t2022-02-25T00:00:00.000000000Z\n", reader, new StringSink());
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
                        ? "s\tu\ttimestamp\n" +
                        "non-ascii äöü\t11111111-2222-3333-4444-555555555555\t2022-02-25T00:00:00.000000Z\n"
                        : "s\tu\ttimestamp\n" +
                        "non-ascii äöü\t11111111-2222-3333-4444-555555555555\t2022-02-25T00:00:00.000000000Z\n";
                TestUtils.assertReader(expected, reader, new StringSink());
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
                        ? "u1\tu2\tu3\ttimestamp\n" +
                        "11111111-1111-1111-1111-111111111111\t\t33333333-3333-3333-3333-333333333333\t2022-02-25T00:00:00.000000Z\n"
                        : "u1\tu2\tu3\ttimestamp\n" +
                        "11111111-1111-1111-1111-111111111111\t\t33333333-3333-3333-3333-333333333333\t2022-02-25T00:00:00.000000000Z\n";
                TestUtils.assertReader(expected, reader, new StringSink());
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
                        ? "ts_col\ttimestamp\n" +
                        "2023-02-11T12:30:11.350000Z\t2022-01-10T20:40:22.540000Z\n"
                        : "ts_col\ttimestamp\n" +
                        "2023-02-11T12:30:11.350000Z\t2022-01-10T20:40:22.540000000Z\n";
                TestUtils.assertReader(expected, reader, new StringSink());
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
                        ? "unit\tts\ttimestamp\n" +
                        "m\t2023-09-18T12:01:00.000000Z\t2023-09-18T12:01:00.000000Z\n" +
                        "s\t2023-09-18T12:01:01.000000Z\t2023-09-18T12:01:01.000000Z\n" +
                        "ns\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000Z\n" +
                        "us\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000Z\n" +
                        "ms\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000Z\n"
                        : "unit\tts\ttimestamp\n" +
                        "m\t2023-09-18T12:01:00.000000Z\t2023-09-18T12:01:00.000000000Z\n" +
                        "s\t2023-09-18T12:01:01.000000Z\t2023-09-18T12:01:01.000000000Z\n" +
                        "ns\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000000Z\n" +
                        "us\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000000Z\n" +
                        "ms\t2023-09-18T12:01:01.010000Z\t2023-09-18T12:01:01.010000000Z\n";
                TestUtils.assertReader(expected, reader, new StringSink());
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
                String expected = "ts\ttimestamp\n" +
                        "2323-09-18T12:01:01.011568Z\t2323-09-18T12:01:01.011568Z\n";
                TestUtils.assertReader(expected, reader, new StringSink());
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
                        ? "unit\tts\ttimestamp\n" +
                        "ns\t2023-09-18T12:01:01.011568Z\t2023-09-18T12:01:01.011568Z\n"
                        : "unit\tts\ttimestamp\n" +
                        "ns\t2023-09-18T12:01:01.011568Z\t2023-09-18T12:01:01.011568901Z\n";
                TestUtils.assertReader(expected, reader, new StringSink());
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
        useLegacyStringDefault = false;
        runInContext(r -> {
            String table = "string_table";
            CountDownLatch released = createTableCommitNotifier(table);
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .build()
            ) {
                long tsMicros = MicrosTimestampDriver.floor("2024-02-27");
                sender.table(table)
                        .stringColumn("string1", "some string")
                        .stringColumn("string2", "another string")
                        .stringColumn("string3", "yet another string")
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.flush();
                waitTableWriterFinish(released);
                assertTableSizeEventually(engine, table, 1);
                try (RecordCursorFactory fac = engine.select(table, sqlExecutionContext);
                     RecordCursor cursor = fac.getCursor(sqlExecutionContext)
                ) {
                    String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                            ? "some string:VARCHAR\tanother string:VARCHAR\tyet another string:VARCHAR\t2024-02-27T00:00:00.000000Z:TIMESTAMP\n"
                            : "some string:VARCHAR\tanother string:VARCHAR\tyet another string:VARCHAR\t2024-02-27T00:00:00.000000000Z:TIMESTAMP_NS\n";
                    TestUtils.assertCursor(
                            expected,
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
                long tsMicros = MicrosTimestampDriver.floor("2024-02-27");
                String expectedValue = "čćžšđçğéíáýůř";
                sender.table(table)
                        .stringColumn("string1", expectedValue)
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.flush();
                waitTableWriterFinish(released);
                assertTableSizeEventually(engine, table, 1);
                try (RecordCursorFactory fac = engine.select(table, sqlExecutionContext);
                     RecordCursor cursor = fac.getCursor(sqlExecutionContext)
                ) {
                    String expectTs = ColumnType.isTimestampMicro(timestampType.getTimestampType())
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
        runInContext(r -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {
                CountDownLatch released = createTableCommitNotifier("mytable");
                long tsMicros = MicrosTimestampDriver.floor("2022-02-25");
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
                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                        ? "int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp\n" +
                        "42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000Z\t2022-02-25T00:00:00.000000Z\n"
                        : "int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp\n" +
                        "42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000000Z\t2022-02-25T00:00:00.000000000Z\n";
                TestUtils.assertReader(expected, reader, new StringSink());
            }
        });
    }

    @Test
    public void testWriteLongMinMax() throws Exception {
        runInContext(r -> {
            String table = "table";
            CountDownLatch released = createTableCommitNotifier("table");
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("127.0.0.1")
                    .port(bindPort)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()) {

                long tsMicros = MicrosTimestampDriver.floor("2023-02-22");
                sender.table(table)
                        .longColumn("max", Long.MAX_VALUE)
                        .longColumn("min", Long.MIN_VALUE)
                        .at(tsMicros, ChronoUnit.MICROS);
                sender.flush();
            }

            waitTableWriterFinish(released);
            assertTableSizeEventually(engine, table, 1);
            try (TableReader reader = getReader(table)) {
                String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                        ? "max\tmin\ttimestamp\n" +
                        "9223372036854775807\tnull\t2023-02-22T00:00:00.000000Z\n"
                        : "max\tmin\ttimestamp\n" +
                        "9223372036854775807\tnull\t2023-02-22T00:00:00.000000000Z\n";
                TestUtils.assertReader(expected, reader, new StringSink());
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
                        ? "u1\ttimestamp\n" +
                        "11111111-1111-1111-1111-111111111111\t2022-02-25T00:00:00.000000Z\n"
                        : "u1\ttimestamp\n" +
                        "11111111-1111-1111-1111-111111111111\t2022-02-25T00:00:00.000000000Z\n";
                TestUtils.assertReader(expected, reader, new StringSink());
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
