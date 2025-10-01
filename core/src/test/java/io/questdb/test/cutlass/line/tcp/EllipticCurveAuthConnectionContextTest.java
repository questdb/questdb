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

import io.questdb.PropServerConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.std.Files;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Base64;

import static io.questdb.test.cutlass.line.tcp.AbstractLineTcpReceiverTest.*;
import static org.junit.Assert.assertEquals;

public class EllipticCurveAuthConnectionContextTest extends BaseLineTcpContextTest {

    private final Rnd rnd = new Rnd();
    private int maxSendBytes = 1024;
    private byte[] sentBytes;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        nWriterThreads = 2;
        timestampTicks = -1;
        recvBuffer = null;
        disconnected = true;
        maxRecvBufferSize.set(1024);
        maxSendBytes = 1024;
        floatDefaultColumnType = ColumnType.DOUBLE;
        integerDefaultColumnType = ColumnType.LONG;
        lineTcpConfiguration = createReceiverConfiguration(true, new LineTcpNetworkFacade() {
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
                    sentBytes[n] = Unsafe.getUnsafe().getByte(buffer + n);
                }
                return nSent;
            }
        });
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
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
