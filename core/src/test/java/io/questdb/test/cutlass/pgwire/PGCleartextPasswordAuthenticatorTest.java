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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGCleartextPasswordAuthenticator;
import io.questdb.cutlass.pgwire.PGHexTestsCircuitBreakRegistry;
import io.questdb.network.Socket;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

public class PGCleartextPasswordAuthenticatorTest extends AbstractCairoTest {
    private static final byte[] PASSWORD_MESSAGE = HexFormat.of().parseHex("700000000a717565737400");
    private static final String PASSWORD_REQUEST_HEX = "520000000800000003";

    @Test
    public void testRepeatedEmptyUserPropertyTakesOnePooledEntry() throws Exception {
        // Malformed-input injection: real clients send the user property once. The startup
        // packet repeats an empty user name, so the server must ask for a password for the
        // empty user and keep one pooled entry for it.
        assertStartupMessage(1_000, "", "", false);
    }

    @Test
    public void testRepeatedUserPropertyTakesOnePooledEntry() throws Exception {
        // Malformed-input injection: real clients send the user property once. The startup
        // packet repeats it; like PostgreSQL, the last value wins, and the server must keep
        // one pooled entry for it rather than one per occurrence.
        assertStartupMessage(1_000, "bogus", "admin", true);
    }

    private static byte[] startupMessage(int repeatedUserCount, String repeatedUser, String lastUser) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (int i = 0; i < repeatedUserCount; i++) {
            putProperty(body, "user", repeatedUser);
        }
        putProperty(body, "user", lastUser);
        putProperty(body, "database", "qdb");
        body.write(0);

        final int msgLen = 2 * Integer.BYTES + body.size();
        ByteArrayOutputStream msg = new ByteArrayOutputStream();
        putInt(msg, msgLen);
        putInt(msg, 196_608); // protocol 3.0
        msg.writeBytes(body.toByteArray());
        return msg.toByteArray();
    }

    private static void putInt(ByteArrayOutputStream out, int value) {
        out.write(value >>> 24);
        out.write(value >>> 16);
        out.write(value >>> 8);
        out.write(value);
    }

    private static void putProperty(ByteArrayOutputStream out, String name, String value) {
        out.writeBytes(name.getBytes(StandardCharsets.UTF_8));
        out.write(0);
        out.writeBytes(value.getBytes(StandardCharsets.UTF_8));
        out.write(0);
    }

    private void assertStartupMessage(
            int repeatedUserCount,
            String repeatedUser,
            String lastUser,
            boolean isPasswordAccepted
    ) throws Exception {
        assertMemoryLeak(() -> {
            final DefaultPGConfiguration configuration = new DefaultPGConfiguration();
            final int recvBufferSize = configuration.getRecvBufferSize();
            final int sendBufferSize = configuration.getSendBufferSize();
            final StubSocket socket = new StubSocket();
            final StringBuilder verifiedUser = new StringBuilder();
            long recvBuffer = 0;
            long sendBuffer = 0;
            try (
                    PGCleartextPasswordAuthenticator authenticator = new PGCleartextPasswordAuthenticator(
                            configuration,
                            null,
                            new NetworkSqlExecutionCircuitBreaker(engine, configuration.getCircuitBreakerConfiguration()),
                            PGHexTestsCircuitBreakRegistry.INSTANCE,
                            sqlTimeout -> {
                            },
                            (username, passwordPtr, passwordLen) -> {
                                verifiedUser.setLength(0);
                                verifiedUser.append(username);
                                return isPasswordAccepted ? SecurityContext.AUTH_TYPE_CREDENTIALS : SecurityContext.AUTH_TYPE_NONE;
                            },
                            false
                    )
            ) {
                recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
                sendBuffer = Unsafe.malloc(sendBufferSize, MemoryTag.NATIVE_DEFAULT);
                authenticator.init(socket, recvBuffer, recvBuffer + recvBufferSize, sendBuffer, sendBuffer + sendBufferSize);

                socket.pendingRecv = startupMessage(repeatedUserCount, repeatedUser, lastUser);
                Assert.assertEquals(SocketAuthenticator.NEEDS_READ, authenticator.handleIO());
                Assert.assertEquals(PASSWORD_REQUEST_HEX, HexFormat.of().formatHex(socket.sent.toByteArray()));
                TestUtils.assertEquals(lastUser, authenticator.getPrincipal());
                Assert.assertEquals(configuration.getCharacterStorePoolCapacity(), authenticator.getCharacterStorePoolSize());

                socket.pendingRecv = PASSWORD_MESSAGE;
                socket.sent.reset();
                final int result = authenticator.handleIO();
                TestUtils.assertEquals(lastUser, verifiedUser);
                if (isPasswordAccepted) {
                    Assert.assertEquals(SocketAuthenticator.OK, result);
                    Assert.assertTrue(authenticator.isAuthenticated());
                } else {
                    Assert.assertEquals(SocketAuthenticator.NEEDS_DISCONNECT, result);
                    Assert.assertFalse(authenticator.isAuthenticated());
                }
            } finally {
                if (recvBuffer != 0) {
                    Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
                }
                if (sendBuffer != 0) {
                    Unsafe.free(sendBuffer, sendBufferSize, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    // Transport stub: hands the authenticator the queued bytes on the next recv() and
    // records what it sends.
    private static class StubSocket implements Socket {
        private final ByteArrayOutputStream sent = new ByteArrayOutputStream();
        private byte[] pendingRecv;

        @Override
        public void close() {
        }

        @Override
        public long getFd() {
            return -1;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public boolean isMorePlaintextBuffered() {
            return false;
        }

        @Override
        public boolean isTlsSessionStarted() {
            return false;
        }

        @Override
        public void of(long fd) {
        }

        @Override
        public int recv(long bufferPtr, int bufferLen) {
            if (pendingRecv == null) {
                return 0;
            }
            Assert.assertTrue(pendingRecv.length <= bufferLen);
            for (int i = 0; i < pendingRecv.length; i++) {
                Unsafe.putByte(bufferPtr + i, pendingRecv[i]);
            }
            final int n = pendingRecv.length;
            pendingRecv = null;
            return n;
        }

        @Override
        public int send(long bufferPtr, int bufferLen) {
            for (int i = 0; i < bufferLen; i++) {
                sent.write(Unsafe.getByte(bufferPtr + i));
            }
            return bufferLen;
        }

        @Override
        public int shutdown(int how) {
            return 0;
        }

        @Override
        public void startTlsSession(@Nullable CharSequence peerName) {
        }

        @Override
        public boolean supportsTls() {
            return false;
        }

        @Override
        public int tlsIO(int readinessFlags) {
            return 0;
        }

        @Override
        public boolean wantsTlsRead() {
            return false;
        }

        @Override
        public boolean wantsTlsWrite() {
            return false;
        }
    }
}
