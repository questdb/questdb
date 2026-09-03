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

import io.questdb.PropServerConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.auth.AnonymousAuthenticator;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.line.tcp.auth.EllipticCurveAuthenticator;
import io.questdb.log.LogFactory;
import io.questdb.metrics.HealthMetrics;
import io.questdb.std.Files;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Base64;

import static io.questdb.test.cutlass.line.tcp.AbstractLineTcpReceiverTest.*;
import static org.junit.Assert.assertEquals;

public class EllipticCurveAuthConnectionContextTest extends BaseLineTcpContextTest {

    private final Rnd rnd = new Rnd();
    private int maxSendBytes = 1024;
    private Charset recvCharset = StandardCharsets.UTF_8;
    private byte[] sentBytes;

    @Before
    @Override
    public void setUp() {
        LogFactory.enableGuaranteedLogging(EllipticCurveAuthenticator.class);
        super.setUp();
        nWriterThreads = 2;
        timestampTicks = -1;
        recvBuffer = null;
        disconnected = true;
        maxRecvBufferSize.set(1024);
        maxSendBytes = 1024;
        recvCharset = StandardCharsets.UTF_8;
        floatDefaultColumnType = ColumnType.DOUBLE;
        integerDefaultColumnType = ColumnType.LONG;
        lineTcpConfiguration = createReceiverConfiguration(true, new LineTcpNetworkFacade() {
            @Override
            byte[] getBytes(String recvBuffer) {
                return recvBuffer.getBytes(recvCharset);
            }

            @Override
            public int sendRaw(long fd, long buffer, int bufferLen) {
                Assert.assertEquals(FD, fd);
                if (sentBytes != null) {
                    return 0;
                }

                if (maxSendBytes <= 0) {
                    return maxSendBytes;
                }

                int nSent = Math.min(bufferLen, maxSendBytes);
                sentBytes = new byte[nSent];
                for (int n = 0; n < nSent; n++) {
                    sentBytes[n] = Unsafe.getByte(buffer + n);
                }
                return nSent;
            }
        });
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        LogFactory.disableGuaranteedLogging(EllipticCurveAuthenticator.class);
    }

    @Test
    public void testAuthenticatorThrowsUnexpectedError() throws Exception {
        // an authenticator is pluggable, so it can throw anything, not just CairoException. The
        // context has to disconnect the client itself: by the time the IO event is dispatched the
        // connection is no longer in the dispatcher's pending list, so an exception that escapes
        // to the worker leaves nothing to close the connection, and the fd, the receive buffer
        // and one of the connection slots are lost for the lifetime of the process
        authenticatorFactoryOverride = () -> new AnonymousAuthenticator() {
            @Override
            public int handleIO() {
                throw new NullPointerException("cannot authenticate");
            }

            @Override
            public boolean isAuthenticated() {
                // route the connection through handleAuthentication() rather than the parser
                return false;
            }
        };
        final LogCapture capture = new LogCapture();
        try {
            capture.start();
            runInAuthContext(() -> {
                final HealthMetrics healthMetrics = lineTcpConfiguration.getMetrics().healthMetrics();
                final long unhandledErrorsBefore = healthMetrics.unhandledErrorsCount();
                recvBuffer = "anything\n";
                handleContextIO0();
                Assert.assertTrue(disconnected);
                // an authenticator blowing up is a server fault, unlike the client sending
                // something malformed, so this one keeps both the severity and the error counter
                // that the health check reads
                capture.waitForRegex("unhandled error while authenticating");
                capture.assertLoggedRE("C i\\.q\\.c\\.l\\.t\\.LineTcpConnectionContext \\[\\d+] unhandled error while authenticating \\[error=");
                Assert.assertEquals(unhandledErrorsBefore + 1, healthMetrics.unhandledErrorsCount());
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testBadSignature() throws Exception {
        runInAuthContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY2);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testBadUser() throws Exception {
        runInAuthContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID2, AUTH_PRIVATE_KEY2);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testDisconnectedOnChallenge1() throws Exception {
        runInAuthContext(() -> {
            maxSendBytes = 0;
            recvBuffer = AUTH_KEY_ID1 + "\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            handleContextIO0();
            Assert.assertFalse(disconnected);
            Assert.assertNull(sentBytes);
            handleContextIO0();
            Assert.assertFalse(disconnected);
            Assert.assertNull(sentBytes);
            maxSendBytes = -1;
            handleContextIO0();
            Assert.assertNull(sentBytes);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testDisconnectedOnChallenge2() throws Exception {
        runInAuthContext(() -> {
            maxSendBytes = 5;
            recvBuffer = AUTH_KEY_ID1 + "\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            handleContextIO0();
            Assert.assertEquals(maxSendBytes, sentBytes.length);
            sentBytes = null;
            Assert.assertFalse(disconnected);
            handleContextIO0();
            Assert.assertEquals(maxSendBytes, sentBytes.length);
            sentBytes = null;
            Assert.assertFalse(disconnected);
            maxSendBytes = -1;
            handleContextIO0();
            Assert.assertNull(sentBytes);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testGoodAuthentication() throws Exception {
        runInAuthContext(() -> {
            try {
                boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
                Assert.assertTrue(authSequenceCompleted);
            } catch (RuntimeException ex) {
                // Expected that Java 8 does not have SHA256withECDSAinP1363
                if (ex.getCause() instanceof NoSuchAlgorithmException && TestUtils.getJavaVersion() <= 8) {
                    return;
                }
                throw ex;
            }
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testGoodAuthenticationFragmented1() throws Exception {
        runInAuthContext(() -> {
            boolean authSequenceCompleted = authenticate(true, false, false, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testGoodAuthenticationFragmented2() throws Exception {
        runInAuthContext(() -> {
            boolean authSequenceCompleted = authenticate(false, true, false, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testGoodAuthenticationFragmented3() throws Exception {
        runInAuthContext(() -> {
            boolean authSequenceCompleted = authenticate(true, true, false, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testGoodAuthenticationFragmented4() throws Exception {
        runInAuthContext(() -> {
            boolean authSequenceCompleted = authenticate(true, false, true, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us\\ midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testGoodAuthenticationFragmented5() throws Exception {
        runInAuthContext(() -> {
            boolean authSequenceCompleted = authenticate(false, true, true, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us\\ midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testGoodAuthenticationFragmented6() throws Exception {
        runInAuthContext(() -> {
            boolean authSequenceCompleted = authenticate(true, true, true, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us\\ midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testGoodAuthenticationFragmented7() throws Exception {
        runInAuthContext(() -> {
            try {
                boolean authSequenceCompleted = authenticate(true, true, true, true, null);
                Assert.assertTrue(authSequenceCompleted);
            } catch (RuntimeException ex) {
                // Expected that Java 8 does not have SHA256withECDSAinP1363
                if (ex.getCause() instanceof NoSuchAlgorithmException && TestUtils.getJavaVersion() <= 8) {
                    return;
                }
                throw ex;
            }

            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us\\ midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testGoodAuthenticationP1363() throws Exception {
        runInAuthContext(() -> {
            try {
                boolean authSequenceCompleted = authenticate(
                        false,
                        false,
                        false,
                        true,
                        null
                );
                Assert.assertTrue(authSequenceCompleted);
            } catch (RuntimeException ex) {
                // Expected that Java 8 does not have SHA256withECDSAinP1363
                if (ex.getCause() instanceof NoSuchAlgorithmException && TestUtils.getJavaVersion() <= 8) {
                    return;
                }
                throw ex;
            }
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testGoodAuthenticationWithExtraData() throws Exception {
        runInAuthContext(() -> {
            try {
                boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1,
                        "weather,location=us-midwest temperature=82 1465839830100400200\n"
                );
                Assert.assertTrue(authSequenceCompleted);
            } catch (RuntimeException ex) {
                // Expected that Java 8 does not have SHA256withECDSAinP1363
                if (ex.getCause() instanceof NoSuchAlgorithmException && TestUtils.getJavaVersion() <= 8) {
                    return;
                }
                throw ex;
            }
            Assert.assertFalse(disconnected);
            closeContext();
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testInvalidKeyId() throws Exception {
        runInAuthContext(() -> {
            StringBuilder token = new StringBuilder("xxxxxxxx");
            while (token.length() < maxRecvBufferSize.get()) {
                token.append(token);
            }
            boolean authSequenceCompleted = authenticate(token.toString(), AUTH_PRIVATE_KEY1);
            Assert.assertFalse(authSequenceCompleted);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testJunkSignature() throws Exception {
        runInAuthContext(() -> {
            int[] junkSignatureInt = {186, 55, 135, 152, 129, 156, 1, 143, 221, 100, 197, 198, 98, 49, 222, 50, 83, 106, 199, 57, 202, 41, 47, 17, 14, 71, 80, 85, 44, 33, 56, 167, 30,
                    70, 13, 227, 59, 178, 39, 212, 84, 79, 243, 230, 112, 48, 226, 187, 190, 59, 79, 152, 31, 188, 239, 80, 158, 202, 219, 235, 44, 196, 214, 209, 32};
            byte[] junkSignature = new byte[junkSignatureInt.length];
            for (int n = 0; n < junkSignatureInt.length; n++) {
                junkSignature[n] = (byte) junkSignatureInt[n];
            }
            boolean authSequenceCompleted = authenticate(false, false, false, false, junkSignature);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testKeyIdIsMultiByteUtf8() throws Exception {
        // readKeyId() guards with Utf8s.validateUtf8() and then converts with Utf8s.toString().
        // Should the guard ever become stricter than the converter, a client whose key id holds
        // legitimate non-ASCII characters would be locked out with no backstop, so pin that a
        // valid 2-, 3- and 4-byte sequence passes it
        final String multiByteKeyId = "\u00fcser\u4e2d\ud83d\ude00";
        // readKeyId() logs the key id through $safe(), which renders a 4-byte character as '??'
        // (Utf8s.put4ByteSafe pushes the decoded surrogate pair into a Utf8Sink, and
        // Utf8Sink.put(char) replaces surrogates), so the assertion stops before the emoji.
        // readKeyId() logs this line at all only when validateUtf8() accepted the whole key id
        final String loggedKeyId = "authentication read key id [keyId=\u00fcser\u4e2d";
        final LogCapture capture = new LogCapture();
        try {
            capture.start();
            runInAuthContext(() -> {
                send(multiByteKeyId + "\n", false);
                // the authenticator answered with a challenge, so the key id cleared both the
                // guard and the conversion that follows it
                Assert.assertNotNull(readChallenge(false));
                Assert.assertFalse(disconnected);
                capture.waitForRegex("authentication read key id \\[keyId=\u00fcser\u4e2d");
                capture.assertLogged(loggedKeyId);
                capture.assertNotLogged("authentication failed, key id is not valid UTF-8");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testKeyIdIsNotValidUtf8() throws Exception {
        // a client that speaks a different (binary) protocol on the ILP port must be
        // disconnected as if the authentication failed, without an unhandled error
        final LogCapture capture = new LogCapture();
        try {
            capture.start();
            runInAuthContext(() -> {
                final HealthMetrics healthMetrics = lineTcpConfiguration.getMetrics().healthMetrics();
                final long unhandledErrorsBefore = healthMetrics.unhandledErrorsCount();
                recvCharset = StandardCharsets.ISO_8859_1;
                recvBuffer = new String(
                        new byte[]{'@', 0, 13, 0, 1, (byte) 0xC0, (byte) 0xA8, '8', 1, '\n'},
                        StandardCharsets.ISO_8859_1
                );
                handleContextIO0();
                Assert.assertTrue(disconnected);
                Assert.assertNull(sentBytes);
                // the authenticator must reject the key id itself. Letting Utf8s.toString() throw
                // instead disconnects the client just the same, so the log line is the only
                // observable that tells the two apart
                capture.waitForRegex("authentication failed, key id is not valid UTF-8 \\[keyId=|cannot convert invalid UTF-8 sequence to UTF-16");
                capture.assertLogged("authentication failed, key id is not valid UTF-8 [keyId=");
                capture.assertNotLogged("cannot convert invalid UTF-8 sequence to UTF-16");
                // a client that speaks the wrong protocol is not a server fault. The counter this
                // reads is monotonic in production, and the pessimistic health check fails /status
                // with HTTP 500 for as long as it is above zero, so a single probe incrementing it
                // here would take the instance out of service permanently
                Assert.assertEquals(unhandledErrorsBefore, healthMetrics.unhandledErrorsCount());
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testMalformedBase64Signature() throws Exception {
        // a signature line that is not valid base64 must disconnect the client as a plain
        // authentication failure, without letting a CairoException escape the context
        final LogCapture capture = new LogCapture();
        try {
            capture.start();
            runInAuthContext(() -> {
                final HealthMetrics healthMetrics = lineTcpConfiguration.getMetrics().healthMetrics();
                final long unhandledErrorsBefore = healthMetrics.unhandledErrorsCount();
                send(AUTH_KEY_ID1 + "\n", false);
                Assert.assertNotNull(readChallenge(false));
                // 5 base64 chars, so length % 4 == 1, which Chars.base64Decode rejects
                recvBuffer = "abcde\n";
                handleContextIO0();
                Assert.assertTrue(disconnected);
                Assert.assertNull(sentBytes);
                // malformed client input is not a server fault, so the context logs the failure
                // at ERROR and never at CRITICAL
                capture.waitForRegex("[EC] i\\.q\\.c\\.l\\.t\\.LineTcpConnectionContext \\[\\d+] authentication failed \\[error=invalid base64 encoding \\[string=abcde], errno=-1]");
                capture.assertLoggedRE("E i\\.q\\.c\\.l\\.t\\.LineTcpConnectionContext \\[\\d+] authentication failed \\[error=invalid base64 encoding \\[string=abcde], errno=-1]");
                capture.assertNotLogged("C i.q.c.l.t.LineTcpConnectionContext");
                // ...and for the same reason it must not move the health counter either, which the
                // CRITICAL assertion above does not cover on its own
                Assert.assertEquals(unhandledErrorsBefore, healthMetrics.unhandledErrorsCount());
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testMinBufferSizeForAuth() throws Exception {
        maxRecvBufferSize.set(PropServerConfiguration.MIN_TCP_ILP_BUF_SIZE);
        runInAuthContext(() -> {
            // this is a big-ass token (that looks like valid ILP line)
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            // asserting there is no exception out of this method
            waitForIOCompletion();
            closeContext();
            drainWalQueue();
        });
    }

    @Test
    public void testSecurityContextFactoryThrowsUnexpectedError() throws Exception {
        // the security context factory is pluggable too and it runs on the same path, one line
        // after the authenticator has accepted the client, so it can strand a connection the same
        // way an authenticator can
        authenticatorFactoryOverride = () -> new AnonymousAuthenticator() {
            private boolean authenticated;

            @Override
            public int handleIO() {
                authenticated = true;
                return SocketAuthenticator.OK;
            }

            @Override
            public boolean isAuthenticated() {
                return authenticated;
            }
        };
        securityContextFactoryOverride = (principalContext, interfaceId) -> {
            throw new NullPointerException("cannot create security context");
        };
        final LogCapture capture = new LogCapture();
        try {
            capture.start();
            runInAuthContext(() -> {
                final HealthMetrics healthMetrics = lineTcpConfiguration.getMetrics().healthMetrics();
                final long unhandledErrorsBefore = healthMetrics.unhandledErrorsCount();
                recvBuffer = "anything\n";
                handleContextIO0();
                Assert.assertTrue(disconnected);
                capture.waitForRegex("unhandled error while authenticating");
                capture.assertLoggedRE("C i\\.q\\.c\\.l\\.t\\.LineTcpConnectionContext \\[\\d+] unhandled error while authenticating \\[error=");
                Assert.assertEquals(unhandledErrorsBefore + 1, healthMetrics.unhandledErrorsCount());
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testTruncatedKeyId() throws Exception {
        runInAuthContext(() -> {
            recvBuffer = "test";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            recvBuffer = "Key";
            handleContextIO0();
            Assert.assertFalse(disconnected);
            recvBuffer = null;
            handleContextIO0();
            Assert.assertTrue(disconnected);
        });
    }

    private boolean authenticate(String authKeyId, PrivateKey authPrivateKey) {
        return authenticate(authKeyId, authPrivateKey, "");
    }

    private boolean authenticate(String authKeyId, PrivateKey authPrivateKey, String extraData) {
        return authenticate(
                authKeyId,
                authPrivateKey,
                false,
                false,
                false,
                false,
                null,
                extraData
        );
    }

    private boolean authenticate(
            boolean fragmentKeyId,
            boolean fragmentChallenge,
            boolean fragmentSignature,
            boolean useP1363Encoding,
            byte[] junkSignature
    ) {
        return authenticate(AbstractLineTcpReceiverTest.AUTH_KEY_ID1, AbstractLineTcpReceiverTest.AUTH_PRIVATE_KEY1, fragmentKeyId, fragmentChallenge, fragmentSignature, useP1363Encoding, junkSignature, "");
    }

    private boolean authenticate(
            String authKeyId,
            PrivateKey authPrivateKey,
            boolean fragmentKeyId,
            boolean fragmentChallenge,
            boolean fragmentSignature,
            boolean useP1363Encoding,
            byte[] junkSignature,
            String extraData
    ) {
        send(authKeyId + "\n", fragmentKeyId);
        byte[] challengeBytes = readChallenge(fragmentChallenge);
        if (challengeBytes == null) {
            return false;
        }
        try {
            byte[] rawSignature;
            if (junkSignature == null) {
                Signature sig = useP1363Encoding ?
                        Signature.getInstance(AuthUtils.SIGNATURE_TYPE_P1363) : Signature.getInstance(AuthUtils.SIGNATURE_TYPE_DER);
                sig.initSign(authPrivateKey);
                sig.update(challengeBytes, 0, challengeBytes.length - 1);
                rawSignature = sig.sign();
            } else {
                rawSignature = junkSignature;
            }
            byte[] signature = Base64.getEncoder().encode(rawSignature);
            send(new String(signature, Files.UTF_8) + "\n" + extraData, fragmentSignature);
            handleContextIO0();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return true;
    }

    private byte[] readChallenge(boolean fragment) {
        int nChallengeBytes = 0;
        boolean receivedChallenge = false;
        byte[] challengeBytes = null;
        do {
            if (disconnected) {
                return null;
            }
            if (fragment) {
                maxSendBytes = rnd.nextInt(10) + 1;
            }
            handleContextIO0();
            if (sentBytes != null) {
                if (challengeBytes == null) {
                    challengeBytes = sentBytes;
                } else {
                    byte[] newChallengeBytes = new byte[challengeBytes.length + sentBytes.length];
                    System.arraycopy(challengeBytes, 0, newChallengeBytes, 0, challengeBytes.length);
                    System.arraycopy(sentBytes, 0, newChallengeBytes, challengeBytes.length, sentBytes.length);
                    challengeBytes = newChallengeBytes;
                }
                sentBytes = null;
                while (nChallengeBytes < challengeBytes.length) {
                    if (challengeBytes[nChallengeBytes] == '\n') {
                        receivedChallenge = true;
                        break;
                    }
                    nChallengeBytes++;
                }
            }
        } while (!receivedChallenge);
        assertEquals(challengeBytes.length, nChallengeBytes + 1);
        return challengeBytes;
    }

    private void send(String sendStr, boolean fragmented) {
        if (fragmented) {
            int nSent = 0;
            do {
                int n = 1 + rnd.nextInt(3);
                if (n + nSent > sendStr.length()) {
                    recvBuffer = sendStr.substring(nSent);
                } else {
                    recvBuffer = sendStr.substring(nSent, nSent + n);
                }
                nSent += n;
                handleContextIO0();
            } while (nSent < sendStr.length());
        } else {
            recvBuffer = sendStr;
            handleContextIO0();
        }
    }
}
