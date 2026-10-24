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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.cutlass.NetUtils;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.util.function.IntConsumer;

/**
 * Plays PostgreSQL wire hex scripts against a PG server: lines starting with {@code >} are
 * sent as the client, lines starting with {@code <} are the bytes the server must answer
 * ({@link NetUtils#playScript}). Used by {@code PGJobContextTest} and the conformance kit.
 * <p>
 * The caller runs a script under {@code assertMemoryLeak}, which {@code AbstractCairoTest}
 * keeps protected to its subclasses.
 * <p>
 * For scripts written by code rather than captured from a client, the class builds the
 * client messages in hex ({@link #startupMessage}, {@link #passwordMessage},
 * {@link #queryMessage}, {@link #extendedQueryMessages}), and {@link #exchange} captures
 * what a server answers them, message by message, for a recording to compare against.
 */
public final class PGHexScripts {
    private static final Log LOG = LogFactory.getLog(PGHexScripts.class);

    private PGHexScripts() {
    }

    /**
     * Starts a PG server of {@code test} with {@code configuration} and a fixed client id and
     * secret, plays {@code script} against it and stops it.
     */
    public static void playScript(
            BasePGTest test,
            NetworkFacade clientNf,
            String script,
            PGConfiguration configuration,
            @Nullable IntConsumer afterReceive
    ) throws Exception {

        /*
            You can use Wireshark to capture and decode. You can also see executed statements in the logs.
            From a Wireshark capture you can right-click on a packet and follow conversation:

            ...n....user.xyz.database.qdb.client_encoding.UTF8.DateStyle.ISO.TimeZone.Europe/London.extra_float_digits.2..R........p....oh.R........S....TimeZone.GMT.S....application_name.QuestDB.S....server_version.11.3.S....integer_datetimes.on.S....client_encoding.UTF8.Z....IP...".SET extra_float_digits = 3...B............E...	.....S....1....2....C....SET.Z....IP...7.SET application_name = 'PostgreSQL JDBC Driver'...B............E...	.....S....1....2....C....SET.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...-S_1.select 1,2,3 from long_sequence(1)...B.....S_1.......D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IX....
        */

        try (
                PGServer server = test.createPGServer(configuration, true);
                WorkerPool workerPool = server.getWorkerPool()
        ) {
            workerPool.start(LOG);
            NetUtils.playScript(clientNf, script, "127.0.0.1", server.getPort(), afterReceive);
        }
    }

    /**
     * Sends client messages (hex) on a connected socket and returns what the server answers, up
     * to and including its first message of type {@code untilType}: {@code 'R'} after a startup
     * message, {@code 'Z'} (ReadyForQuery) after anything else. Fails when the server closes the
     * connection or does not answer within 30 seconds.
     */
    public static String exchange(NetworkFacade nf, long fd, CharSequence clientHex, char untilType) {
        final int bufSize = 4 * 1024 * 1024;
        final long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            final int sendLen = clientHex.length() / 2;
            for (int i = 0; i < sendLen; i++) {
                Unsafe.putByte(buf + i, (byte) Integer.parseInt(clientHex.subSequence(2 * i, 2 * i + 2).toString(), 16));
            }
            int sent = 0;
            while (sent < sendLen) {
                final int n = nf.sendRaw(fd, buf + sent, sendLen - sent);
                Assert.assertTrue("send failed: " + n, n >= 0);
                sent += n;
            }
            final long deadline = System.currentTimeMillis() + 30_000;
            int received = 0;
            int parsed = 0;
            while (true) {
                // complete messages so far: type byte, then a length that counts itself
                while (received - parsed >= 5) {
                    final int len = Integer.reverseBytes(Unsafe.getInt(buf + parsed + 1));
                    if (received - parsed < len + 1) {
                        break;
                    }
                    final char type = (char) Unsafe.getByte(buf + parsed);
                    parsed += len + 1;
                    if (type == untilType) {
                        final StringSink sink = new StringSink();
                        for (int i = 0; i < parsed; i++) {
                            final int b = Unsafe.getByte(buf + i) & 0xFF;
                            sink.put(Character.forDigit(b >> 4, 16)).put(Character.forDigit(b & 0xF, 16));
                        }
                        Assert.assertEquals("bytes after message " + untilType, parsed, received);
                        return sink.toString();
                    }
                }
                final int n = nf.recvRaw(fd, buf + received, bufSize - received);
                if (n < 0) {
                    Assert.fail("the server closed the connection after " + received + " bytes");
                }
                if (n == 0) {
                    if (System.currentTimeMillis() > deadline) {
                        Assert.fail("the server did not answer with message " + untilType + " within 30 seconds");
                    }
                    Os.sleep(1);
                }
                received += n;
            }
        } finally {
            Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Parse, Bind (no parameters; every result column in {@code resultFormat}, 0 text or 1
     * binary), Describe portal, Execute and Sync, for the unnamed statement and portal.
     */
    public static String extendedQueryMessages(String sql, int resultFormat) {
        final StringSink sink = new StringSink();
        // Parse: statement name, query, no parameter types
        message(sink, 'P', hexOf("") + hexOf(sql) + "0000");
        // Bind: portal, statement, no parameter formats, no parameters, one result format
        message(sink, 'B', hexOf("") + hexOf("") + "0000" + "0000" + "0001" + hex16(resultFormat));
        message(sink, 'D', "50" + hexOf(""));
        message(sink, 'E', hexOf("") + "00000000");
        message(sink, 'S', "");
        return sink.toString();
    }

    /**
     * Opens a blocking TCP connection to a server on 127.0.0.1; close it with
     * {@code nf.close(fd)}.
     */
    public static long connect(NetworkFacade nf, int port) {
        final long fd = nf.socketTcp(true);
        final long sockAddress = nf.sockaddr(Net.parseIPv4("127.0.0.1"), port);
        try {
            TestUtils.assertConnect(fd, sockAddress);
        } finally {
            nf.freeSockAddr(sockAddress);
        }
        nf.configureNonBlocking(fd);
        return fd;
    }

    public static String passwordMessage(String password) {
        final StringSink sink = new StringSink();
        message(sink, 'p', hexOf(password));
        return sink.toString();
    }

    public static String queryMessage(String sql) {
        final StringSink sink = new StringSink();
        message(sink, 'Q', hexOf(sql));
        return sink.toString();
    }

    /**
     * Splits server output (hex) into its messages (hex), each a type byte and a length that
     * counts itself.
     */
    public static ObjList<String> splitMessages(String hex) {
        final ObjList<String> messages = new ObjList<>();
        int pos = 0;
        while (pos < hex.length()) {
            final int len = Integer.parseInt(hex.substring(pos + 2, pos + 10), 16);
            final int end = pos + 2 + 2 * len;
            messages.add(hex.substring(pos, end));
            pos = end;
        }
        return messages;
    }

    /**
     * The StartupMessage of protocol 3.0 for a user and database.
     */
    public static String startupMessage(String user, String database) {
        final String body = "00030000" + hexOf("user") + hexOf(user) + hexOf("database") + hexOf(database) + "00";
        return hex32(4 + body.length() / 2) + body;
    }

    private static String hex16(int value) {
        return String.format("%04x", value & 0xFFFF);
    }

    private static String hex32(int value) {
        return String.format("%08x", value);
    }

    // a C string: UTF-8 bytes and a terminating zero
    private static String hexOf(String text) {
        final StringBuilder sb = new StringBuilder();
        for (byte b : text.getBytes(StandardCharsets.UTF_8)) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.append("00").toString();
    }

    private static void message(StringSink sink, char type, String bodyHex) {
        sink.put(Integer.toHexString(type)).put(hex32(4 + bodyHex.length() / 2)).put(bodyHex);
    }
}
