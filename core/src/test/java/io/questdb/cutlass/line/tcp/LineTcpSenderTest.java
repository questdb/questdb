/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.TableReader;
import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.cutlass.line.Sender;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivateKey;
import java.util.function.Consumer;

import static io.questdb.test.tools.TestUtils.assertContains;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

public class LineTcpSenderTest extends AbstractLineTcpReceiverTest {

    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static String AUTH_KEY_ID2_INVALID = "invalid";
    private final static String TOKEN = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthDb.importPrivateKey(TOKEN);
    private final static int HOST = Net.parseIPv4("127.0.0.1");

    @Test
    public void testMinBufferSizeWhenAuth() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        int tinyCapacity = 42;
        runInContext(r -> {
            try (LineTcpSender sender = LineTcpSender.newSender(HOST, bindPort, tinyCapacity)) {
                sender.authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "challenge did not fit into buffer");
            }
        });
    }

    @Test
    public void testAuthSuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            int bufferCapacity = 256 * 1024;

            try (LineTcpSender sender = LineTcpSender.newSender(HOST, bindPort, bufferCapacity)) {
                sender.authenticate(authKeyId, AUTH_PRIVATE_KEY1);
                sender.metric("mytable").field("my int field", 42).$();
                sender.flush();
            }

            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testAuthWrongKey() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(r -> {
            int bufferCapacity = 2048;

            try (LineTcpSender sender = LineTcpSender.newSender(HOST, bindPort, bufferCapacity)) {
                sender.authenticate(AUTH_KEY_ID2_INVALID, AUTH_PRIVATE_KEY1);
                //30 seconds should be enough even on a slow CI server
                long deadline = Os.currentTimeNanos() + SECONDS.toNanos(30);
                while (Os.currentTimeNanos() < deadline) {
                    sender.metric("mytable").field("my int field", 42).$();
                    sender.flush();
                }
                fail("Client fail to detected qdb server closed a connection due to wrong credentials");
            } catch (LineSenderException expected){
                // ignored
            }
        });
    }

    @Test
    public void testBuilderAuthSuccess() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        String address = "127.0.0.1:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(address)
                    .enableAuth(AUTH_KEY_ID1).authToken(TOKEN)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderPlainText_addressWithIpAndPort() throws Exception {
        String address = "127.0.0.1:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(address)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testWriteAllTypes() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address("127.0.0.1")
                    .port(bindPort)
                    .build()) {

                long tsMicros = IntervalUtils.parseFloorPartialDate("2022-02-25");
                sender.table("mytable")
                        .longColumn("int_field", 42)
                        .boolColumn("bool_field", true)
                        .stringColumn("string_field", "foo")
                        .doubleColumn("double_field", 42.0)
                        .timestampColumn("ts_field", tsMicros)
                        .atMicros(tsMicros);
                sender.flush();
            }

            assertTableSizeEventually(engine, "mytable", 1);
            try (TableReader reader = engine.getReader(lineConfiguration.getCairoSecurityContext(), "mytable")) {
                TestUtils.assertReader("int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp\n" +
                        "42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000Z\t2022-02-25T00:00:00.000000Z\n", reader, new StringSink());
            }
        });
    }

    @Test
    public void testDouble_edgeValues() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address("127.0.0.1")
                    .port(bindPort)
                    .build()) {

                long ts = IntervalUtils.parseFloorPartialDate("2022-02-25");
                sender.table("mytable")
                        .doubleColumn("negative_inf", Double.NEGATIVE_INFINITY)
                        .doubleColumn("positive_inf", Double.POSITIVE_INFINITY)
                        .doubleColumn("nan", Double.NaN)
                        .doubleColumn("max_value", Double.MAX_VALUE)
                        .doubleColumn("min_value", Double.MIN_VALUE)
                        .atMicros(ts);
                sender.flush();

                assertTableSizeEventually(engine, "mytable", 1);
                try (TableReader reader = engine.getReader(lineConfiguration.getCairoSecurityContext(), "mytable")) {
                    TestUtils.assertReader("negative_inf\tpositive_inf\tnan\tmax_value\tmin_value\ttimestamp\n" +
                            "-Infinity\tInfinity\tNaN\t1.7976931348623157E308\t4.9E-307\t2022-02-25T00:00:00.000000Z\n", reader, new StringSink());
                }
            }
        });
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterDouble() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.doubleColumn("columnName", 42.0));
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterString() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.stringColumn("columnName", "42"));
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterBool() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.boolColumn("columnName", false));
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterLong() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.longColumn("columnName", 42));
    }

    private void assertSymbolsCannotBeWrittenAfterOtherType(Consumer<Sender> otherTypeWritter) throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address("127.0.0.1")
                    .port(bindPort)
                    .build()) {
                sender.table("mytable");
                otherTypeWritter.accept(sender);
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

    @Test
    public void testBuilderPlainText_addressWithHostnameAndPort() throws Exception {
        String address = "localhost:" + bindPort;
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address(address)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testBuilderPlainText_addressWithExplicitIpAndPort() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address("127.0.0.1")
                    .port(bindPort)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testCloseImpliesFlush() throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address("127.0.0.1")
                    .port(bindPort)
                    .build()) {
                sender.table("mytable").longColumn("my int field", 42).atNow();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }

    @Test
    public void testCloseIdempotent() {
        DummyLineChannel channel = new DummyLineChannel();

        try (LineTcpSender sender = new LineTcpSender(channel, 1000)) {
            sender.close(); // first close
            // the 2nd close is due to try-with-resource
        }
        assertTrue(channel.closed);
    }

    @Test
    public void testUseAfterClose_table() {
        DummyLineChannel channel = new DummyLineChannel();

        Sender sender = new LineTcpSender(channel, 1000);
        sender.close();

        try {
            sender.table("mytable");
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "Sender already closed");
        }
    }

    @Test
    public void testUseAfterClose_symbol() {
        DummyLineChannel channel = new DummyLineChannel();

        Sender sender = new LineTcpSender(channel, 1000);
        sender.table("mytable");
        sender.close();

        try {
            sender.symbol("sym", "val");
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "Sender already closed");
        }
    }

    @Test
    public void testUseAfterClose_tsColumn() {
        DummyLineChannel channel = new DummyLineChannel();

        Sender sender = new LineTcpSender(channel, 1000);
        sender.table("mytable");
        sender.close();

        try {
            sender.timestampColumn("col", 0);
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "Sender already closed");
        }
    }

    @Test
    public void testUseAfterClose_stringColumn() {
        DummyLineChannel channel = new DummyLineChannel();

        Sender sender = new LineTcpSender(channel, 1000);
        sender.table("mytable");
        sender.close();

        try {
            sender.stringColumn("col", "val");
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "Sender already closed");
        }
    }

    @Test
    public void testUseAfterClose_doubleColumn() {
        DummyLineChannel channel = new DummyLineChannel();

        Sender sender = new LineTcpSender(channel, 1000);
        sender.table("mytable");
        sender.close();

        try {
            sender.doubleColumn("col", 42.42);
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "Sender already closed");
        }
    }

    @Test
    public void testUseAfterClose_boolColumn() {
        DummyLineChannel channel = new DummyLineChannel();

        Sender sender = new LineTcpSender(channel, 1000);
        sender.table("mytable");
        sender.close();

        try {
            sender.boolColumn("col", true);
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "Sender already closed");
        }
    }

    @Test
    public void testUseAfterClose_longColumn() {
        DummyLineChannel channel = new DummyLineChannel();

        Sender sender = new LineTcpSender(channel, 1000);
        sender.table("mytable");
        sender.close();

        try {
            sender.longColumn("col", 42);
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "Sender already closed");
        }
    }

    @Test
    public void testUseAfterClose_atNow() {
        DummyLineChannel channel = new DummyLineChannel();

        Sender sender = new LineTcpSender(channel, 1000);
        sender.table("mytable");
        sender.longColumn("col", 42);
        sender.close();

        try {
            sender.atNow();
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "Sender already closed");
        }
    }

    @Test
    public void testUseAfterClose_atMicros() {
        DummyLineChannel channel = new DummyLineChannel();

        Sender sender = new LineTcpSender(channel, 1000);
        sender.table("mytable");
        sender.longColumn("col", 42);
        sender.close();

        try {
            sender.atMicros(MicrosecondClockImpl.INSTANCE.getTicks());
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "Sender already closed");
        }
    }


    private static class DummyLineChannel implements LineChannel {
        private boolean closed;

        @Override
        public void send(long ptr, int len) {

        }

        @Override
        public int receive(long ptr, int len) {
            return 0;
        }

        @Override
        public int errno() {
            return 0;
        }

        @Override
        public void close() {
            if (closed) {
                // currently this is not needed, because LineTcpSender double-close will result in double-free
                // and this crashes the test anyway. But we don't want to rely on this behaviour.
                // should LineTcpSender impl change we still want to catch LineChannel double-close
                throw new IllegalStateException("double close detected");
            }
            closed = true;
        }
    }
}
