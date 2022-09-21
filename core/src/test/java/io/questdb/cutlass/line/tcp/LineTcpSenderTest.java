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
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.client.Sender;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.network.Net;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

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

    private static final Consumer<Sender> SET_TABLE_NAME_ACTION = s -> s.table("mytable");

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
                        .at(tsMicros * 1000);
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
                        .at(ts * 1000);
                sender.flush();

                assertTableSizeEventually(engine, "mytable", 1);
                try (TableReader reader = engine.getReader(lineConfiguration.getCairoSecurityContext(), "mytable")) {
                    TestUtils.assertReader("negative_inf\tpositive_inf\tnan\tmax_value\tmin_value\ttimestamp\n" +
                            "-Infinity\tInfinity\tNaN\t1.7976931348623157E308\t4.9E-324\t2022-02-25T00:00:00.000000Z\n", reader, new StringSink());
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

    private void assertSymbolsCannotBeWrittenAfterOtherType(Consumer<Sender> otherTypeWriter) throws Exception {
        runInContext(r -> {
            try (Sender sender = Sender.builder()
                    .address("127.0.0.1")
                    .port(bindPort)
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
        LineTcpSender sender = new LineTcpSender(channel, 1000);
        sender.close();
        sender.close();
        assertEquals(1, channel.closeCounter);
    }

    @Test
    public void testUseAfterClose_table() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION);
    }

    @Test
    public void testUseAfterClose_symbol() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.symbol("sym", "val"));
    }

    @Test
    public void testUseAfterClose_tsColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.timestampColumn("col", 0));
    }

    @Test
    public void testUseAfterClose_stringColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.stringColumn("col", "val"));
    }

    @Test
    public void testUseAfterClose_doubleColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.doubleColumn("col", 42.42));
    }

    @Test
    public void testUseAfterClose_boolColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.boolColumn("col", true));
    }

    @Test
    public void testUseAfterClose_longColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.longColumn("col", 42));
    }

    @Test
    public void testUseAfterClose_flush() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, Sender::flush);
    }

    @Test
    public void testUseAfterClose_atNow() {
        assertExceptionOnClosedSender(s -> {
            s.table("mytable");
            s.longColumn("col", 42);
        }, Sender::atNow);
    }

    @Test
    public void testUseAfterClose_atMicros() {
        assertExceptionOnClosedSender(s -> {
            s.table("mytable");
            s.longColumn("col", 42);
        }, s -> s.at(MicrosecondClockImpl.INSTANCE.getTicks()));
    }

    @Test
    public void testControlCharInTableName() {
        assertControlCharacterException(s -> s.table("mytable\u0001"));
    }

    @Test
    public void testControlCharInColumnName() {
        assertControlCharacterException(s -> {
            s.table("mytable");
            s.boolColumn("col\u0001", true);
        });
    }

    @Test
    public void testServerIgnoresUnfinishedRows() throws Exception{
        String tableName = "myTable";
        runInContext(r -> {
            send(r, tableName, WAIT_ENGINE_TABLE_RELEASE, () -> {
                try (Sender sender = Sender.builder()
                        .address("127.0.0.1")
                        .port(bindPort)
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
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                assertEquals(1, reader.getCursor().size());
            }
        });
    }

    @Test
    public void testUnfinishedRowDoesNotContainNewLine() {
        StringChannel channel = new StringChannel();
        try (Sender sender = new LineTcpSender(channel, 1000)) {
            sender.table("mytable");
            sender.boolColumn("col\n", true);
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "name contains an illegal char");
        }
        assertFalse(Chars.contains(channel.toString(), "\n"));
    }

    @Test
    public void testCannotStartNewRowBeforeClosingTheExistingAfterValidationError() {
        StringChannel channel = new StringChannel();
        try (Sender sender = new LineTcpSender(channel, 1000)) {
            sender.table("mytable");
            try {
                sender.boolColumn("col\n", true);
                fail();
            }  catch (LineSenderException e) {
                assertContains(e.getMessage(), "name contains an illegal char");
            }
            try {
                sender.table("mytable");
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "duplicated table");
            }
        }
        assertFalse(Chars.contains(channel.toString(), "\n"));
    }

    private static void assertControlCharacterException(Consumer<Sender> senderAction) {
        DummyLineChannel channel = new DummyLineChannel();
        try (Sender sender = new LineTcpSender(channel, 1000)) {
            sender.table("mytable");
            sender.boolColumn("col\u0001", true);
            fail("control character in column or table name must throw exception");
        } catch (LineSenderException e) {
            String m = e.getMessage();
            assertContains(m, "name contains an illegal char");
            assertNoControlCharacter(m);
        }
    }

    private static void assertExceptionOnClosedSender(Consumer<Sender> afterCloseAction) {
        assertExceptionOnClosedSender(s -> {}, afterCloseAction);
    }


    private static void assertExceptionOnClosedSender(Consumer<Sender> beforeCloseAction, Consumer<Sender> afterCloseAction) {
        DummyLineChannel channel = new DummyLineChannel();
        Sender sender = new LineTcpSender(channel, 1000);
        beforeCloseAction.accept(sender);
        sender.close();
        try {
            afterCloseAction.accept(sender);
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "sender already closed");
        }
    }

    private static void assertNoControlCharacter(CharSequence m) {
        for (int i = 0, n = m.length(); i < n; i++) {
            assertFalse(Character.isISOControl(m.charAt(i)));
        }
    }


    private static class DummyLineChannel implements LineChannel {
        private int closeCounter;

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
            closeCounter++;
        }
    }
}
