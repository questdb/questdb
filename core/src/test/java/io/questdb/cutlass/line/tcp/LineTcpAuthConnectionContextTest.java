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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.std.Files;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Base64;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class LineTcpAuthConnectionContextTest extends BaseLineTcpContextTest {
    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthDb.importPrivateKey("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48");
    private final static String AUTH_KEY_ID2 = "testUser2";
    private final static PrivateKey AUTH_PRIVATE_KEY2 = AuthDb.importPrivateKey("lwJi3TSb4G6UcHxFJmPhOTWa4BLwJOOiK76wT6Uk7pI");
    private final Random rand = new Random(0);
    private byte[] sentBytes;
    private int maxSendBytes = 1024;

    @Before
    @Override
    public void before() {
        nWriterThreads = 2;
        microSecondTicks = -1;
        recvBuffer = null;
        disconnected = true;
        netMsgBufferSize.set(1024);
        maxSendBytes = 1024;
        floatDefaultColumnType = ColumnType.DOUBLE;
        integerDefaultColumnType = ColumnType.LONG;
        lineTcpConfiguration = createReceiverConfiguration(true, new LineTcpNetworkFacade() {
            @Override
            public int send(long fd, long buffer, int bufferLen) {
                Assert.assertEquals(FD, fd);
                if (null != sentBytes) {
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            handleContextIO();
            Assert.assertFalse(disconnected);
            Assert.assertNull(sentBytes);
            handleContextIO();
            Assert.assertFalse(disconnected);
            Assert.assertNull(sentBytes);
            maxSendBytes = -1;
            handleContextIO();
            Assert.assertNull(sentBytes);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testDisconnectedOnChallenge2() throws Exception {
        runInAuthContext(() -> {
            maxSendBytes = 5;
            recvBuffer = AUTH_KEY_ID1 + "\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            handleContextIO();
            Assert.assertEquals(maxSendBytes, sentBytes.length);
            sentBytes = null;
            Assert.assertFalse(disconnected);
            handleContextIO();
            Assert.assertEquals(maxSendBytes, sentBytes.length);
            sentBytes = null;
            Assert.assertFalse(disconnected);
            maxSendBytes = -1;
            handleContextIO();
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
            handleContextIO();
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
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, true, false, false, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO();
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
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, false, true, false, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO();
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
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, true, true, false, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO();
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
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, true, false, true, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us\\ midwest temperature=82 1465839830100400200\n";
            handleContextIO();
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
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, false, true, true, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us\\ midwest temperature=82 1465839830100400200\n";
            handleContextIO();
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
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, true, true, true, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us\\ midwest temperature=82 1465839830100400200\n";
            handleContextIO();
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
                boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, true, true, true, true, null);
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
            handleContextIO();
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
                        AUTH_KEY_ID1,
                        AUTH_PRIVATE_KEY1,
                        false,
                        false,
                        false,
                        true,
                        null);
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
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testIncorrectConfig() throws Exception {
        netMsgBufferSize.set(200);
        try {
            runInAuthContext(() -> {
                recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
                handleContextIO();
                Assert.assertFalse(disconnected);
                waitForIOCompletion();
                closeContext();
                Assert.fail();
            });
        } catch (CairoException ex) {
            TestUtils.assertEquals("Minimum buffer length is 513", ex.getFlyweightMessage());
        }
    }

    @Test
    public void testInvalidKeyId() throws Exception {
        runInAuthContext(() -> {
            StringBuilder token = new StringBuilder("xxxxxxxx");
            while (token.length() < netMsgBufferSize.get()) {
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
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, false, false, false, false, junkSignature);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testTruncatedKeyId() throws Exception {
        runInAuthContext(() -> {
            recvBuffer = "test";
            handleContextIO();
            Assert.assertFalse(disconnected);
            recvBuffer = "Key";
            handleContextIO();
            Assert.assertFalse(disconnected);
            recvBuffer = null;
            handleContextIO();
            Assert.assertTrue(disconnected);
        });
    }

    private boolean authenticate(String authKeyId, PrivateKey authPrivateKey) {
        return authenticate(
                authKeyId,
                authPrivateKey,
                false,
                false,
                false,
                false,
                null);
    }

    private boolean authenticate(String authKeyId,
                                 PrivateKey authPrivateKey,
                                 boolean fragmentKeyId,
                                 boolean fragmentChallenge,
                                 boolean fragmentSignature,
                                 boolean useP1363Encoding,
                                 byte[] junkSignature) {
        send(authKeyId + "\n", fragmentKeyId);
        byte[] challengeBytes = readChallenge(fragmentChallenge);
        if (null == challengeBytes) {
            return false;
        }
        try {
            byte[] rawSignature;
            if (null == junkSignature) {
                Signature sig = useP1363Encoding ?
                        Signature.getInstance(AuthDb.SIGNATURE_TYPE_P1363) : Signature.getInstance(AuthDb.SIGNATURE_TYPE_DER);
                sig.initSign(authPrivateKey);
                sig.update(challengeBytes, 0, challengeBytes.length - 1);
                rawSignature = sig.sign();
            } else {
                rawSignature = junkSignature;
            }
            byte[] signature = Base64.getEncoder().encode(rawSignature);
            send(new String(signature, Files.UTF_8) + "\n", fragmentSignature);
            handleContextIO();
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
                maxSendBytes = rand.nextInt(10) + 1;
            }
            handleContextIO();
            if (null != sentBytes) {
                if (null == challengeBytes) {
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
                int n = 1 + rand.nextInt(3);
                if (n + nSent > sendStr.length()) {
                    recvBuffer = sendStr.substring(nSent);
                } else {
                    recvBuffer = sendStr.substring(nSent, nSent + n);
                }
                nSent += n;
                handleContextIO();
            } while (nSent < sendStr.length());
        } else {
            recvBuffer = sendStr;
            handleContextIO();
        }
    }
}
