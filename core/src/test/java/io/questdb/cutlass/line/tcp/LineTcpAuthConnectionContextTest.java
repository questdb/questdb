/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler.NetworkIOJob;
import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler.TableUpdateDetails;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.*;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;

public class LineTcpAuthConnectionContextTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpAuthConnectionContextTest.class);
    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthDb.importPrivateKey("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48");
    private final static String AUTH_KEY_ID2 = "testUser2";
    private final static PrivateKey AUTH_PRIVATE_KEY2 = AuthDb.importPrivateKey("lwJi3TSb4G6UcHxFJmPhOTWa4BLwJOOiK76wT6Uk7pI");
    private static final int FD = 1_000_000;
    private final Random rand = new Random(0);
    private final AtomicInteger netMsgBufferSize = new AtomicInteger(1024);
    private final NetworkIOJob netIoJob = new NetworkIOJob() {
        private final CharSequenceObjHashMap<TableUpdateDetails> localTableUpdateDetailsByTableName = new CharSequenceObjHashMap<>();
        private final ObjList<SymbolCache> unusedSymbolCaches = new ObjList<>();

        @Override
        public int getWorkerId() {
            return 0;
        }

        @Override
        public TableUpdateDetails getTableUpdateDetails(CharSequence tableName) {
            return localTableUpdateDetailsByTableName.get(tableName);
        }

        @Override
        public void addTableUpdateDetails(TableUpdateDetails tableUpdateDetails) {
            localTableUpdateDetailsByTableName.put(tableUpdateDetails.tableName, tableUpdateDetails);
        }

        @Override
        public ObjList<SymbolCache> getUnusedSymbolCaches() {
            return unusedSymbolCaches;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean run(int workerId) {
            Assert.fail("This is a mock job, not designed to run in a wroker pool");
            return false;
        }
    };
    private LineTcpAuthConnectionContext context;
    private LineTcpReceiverConfiguration lineTcpConfiguration;
    private LineTcpMeasurementScheduler scheduler;
    private boolean disconnected;
    private String recvBuffer;
    private byte[] sentBytes;
    private int maxSendBytes = 1024;
    private int nWriterThreads;
    private WorkerPool workerPool;
    private long microSecondTicks;

    @Before
    public void before() {
        NetworkFacade nf = new NetworkFacadeImpl() {
            @Override
            public int recv(long fd, long buffer, int bufferLen) {
                Assert.assertEquals(FD, fd);
                if (null == recvBuffer) {
                    return -1;
                }

                byte[] bytes = recvBuffer.getBytes(StandardCharsets.UTF_8);
                int n = 0;
                while (n < bufferLen && n < bytes.length) {
                    Unsafe.getUnsafe().putByte(buffer++, bytes[n++]);
                }
                recvBuffer = new String(bytes, n, bytes.length - n);
                return n;
            }

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
        };
        nWriterThreads = 2;
        microSecondTicks = -1;
        lineTcpConfiguration = new DefaultLineTcpReceiverConfiguration() {
            @Override
            public int getNetMsgBufferSize() {
                return netMsgBufferSize.get();
            }

            @Override
            public int getMaxMeasurementSize() {
                return 128;
            }

            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }

            @Override
            public MicrosecondClock getMicrosecondClock() {
                return new MicrosecondClockImpl() {
                    @Override
                    public long getTicks() {
                        if (microSecondTicks >= 0) {
                            return microSecondTicks;
                        }
                        return super.getTicks();
                    }
                };
            }

            @Override
            public String getAuthDbPath() {
                URL u = getClass().getResource("authDb.txt");
                assert u != null;
                return u.getFile();
            }
        };
    }

    @Test
    public void testBadSignature() throws Exception {
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY2);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testBadUser() throws Exception {
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID2, AUTH_PRIVATE_KEY2);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertTrue(disconnected);
        });
    }

    @Test
    public void testDisconnectedOnChallenge1() throws Exception {
        runInContext(() -> {
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
        runInContext(() -> {
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
        runInContext(() -> {
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
            assertTable(expected);
        });
    }

    @Test
    public void testGoodAuthenticationFragmented1() throws Exception {
        runInContext(() -> {
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
            assertTable(expected);
        });
    }

    @Test
    public void testGoodAuthenticationFragmented2() throws Exception {
        runInContext(() -> {
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
            assertTable(expected);
        });
    }

    @Test
    public void testGoodAuthenticationFragmented3() throws Exception {
        runInContext(() -> {
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
            assertTable(expected);
        });
    }

    @Test
    public void testGoodAuthenticationFragmented4() throws Exception {
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, true, false, true, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected);
        });
    }

    @Test
    public void testGoodAuthenticationFragmented5() throws Exception {
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, false, true, true, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected);
        });
    }

    @Test
    public void testGoodAuthenticationFragmented6() throws Exception {
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, true, true, true, false, null);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected);
        });
    }

    @Test
    public void testGoodAuthenticationFragmented7() throws Exception {
        runInContext(() -> {
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
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected);
        });
    }

    @Test
    public void testGoodAuthenticationP1363() throws Exception {
        runInContext(() -> {
            try {
                boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, false, false, false, true, null);
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
            assertTable(expected);
        });
    }

    @Test
    public void testIncorrectConfig() throws Exception {
        netMsgBufferSize.set(200);
        try {
            runInContext(() -> {
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
        runInContext(() -> {
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
        runInContext(() -> {
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
        runInContext(() -> {
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

    private void assertTable(CharSequence expected) {
        try (TableReader reader = new TableReader(configuration, "weather")) {
            assertCursorTwoPass(expected, reader.getCursor(), reader.getMetadata());
        }
    }

    private boolean authenticate(String authKeyId, PrivateKey authPrivateKey) {
        return authenticate(authKeyId, authPrivateKey, false, false, false, false, null);
    }

    private boolean authenticate(
            String authKeyId, PrivateKey authPrivateKey, boolean fragmentKeyId, boolean fragmentChallenge, boolean fragmentSignature, boolean useP1363Encoding, byte[] junkSignature
    ) {
        send(authKeyId + "\n", fragmentKeyId);
        byte[] challengeBytes = readChallenge(fragmentChallenge);
        if (null == challengeBytes) {
            return false;
        }
        try {
            byte[] rawSignature;
            if (null == junkSignature) {
                Signature sig = useP1363Encoding ? Signature.getInstance(AuthDb.SIGNATURE_TYPE_P1363) : Signature.getInstance(AuthDb.SIGNATURE_TYPE_DER);
                sig.initSign(authPrivateKey);
                sig.update(challengeBytes, 0, challengeBytes.length - 1);
                rawSignature = sig.sign();
            } else {
                rawSignature = junkSignature;
            }
            byte[] signature = Base64.getEncoder().encode(rawSignature);
            send(new String(signature, StandardCharsets.UTF_8) + "\n", fragmentSignature);
            handleContextIO();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return true;
    }

    private void closeContext() {
        if (null != scheduler) {
            workerPool.halt();
            Assert.assertFalse(context.invalid());
            Assert.assertEquals(FD, context.getFd());
            context.close();
            Assert.assertTrue(context.invalid());
            Assert.assertEquals(-1, context.getFd());
            context = null;
            scheduler.close();
            scheduler = null;
        }
    }

    private boolean handleContextIO() {
        switch (context.handleIO(netIoJob)) {
            case NEEDS_READ:
                context.getDispatcher().registerChannel(context, IOOperation.READ);
                return false;
            case NEEDS_WRITE:
                context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                return false;
            case QUEUE_FULL:
                return true;
            case NEEDS_DISCONNECT:
                context.getDispatcher().disconnect(context, IODispatcher.DISCONNECT_REASON_PROTOCOL_VIOLATION);
                return false;
        }
        return false;
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

    private void runInContext(Runnable r) throws Exception {
        assertMemoryLeak(() -> {
            setupContext(engine);
            try {
                r.run();
            } finally {
                closeContext();
            }
        });
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

    private void setupContext(CairoEngine engine) {
        workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            private final int workerCount;
            private final int[] affinityByThread;

            @Override
            public int[] getWorkerAffinity() {
                return affinityByThread;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean haltOnError() {
                return false;
            }

            {
                workerCount = nWriterThreads;
                affinityByThread = new int[workerCount];
                Arrays.fill(affinityByThread, -1);
            }
        });

        WorkerPool netIoWorkerPool = new WorkerPool(new WorkerPoolConfiguration() {
            private final int[] affinityByThread = {-1};

            @Override
            public int[] getWorkerAffinity() {
                return affinityByThread;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean haltOnError() {
                return true;
            }
        });

        scheduler = new LineTcpMeasurementScheduler(lineTcpConfiguration, engine, netIoWorkerPool, null, workerPool) {
            @Override
            protected NetworkIOJob createNetworkIOJob(IODispatcher<LineTcpConnectionContext> dispatcher, int workerId) {
                Assert.assertEquals(0, workerId);
                return netIoJob;
            }
        };

        AuthDb authDb = new AuthDb(lineTcpConfiguration);
        context = new LineTcpAuthConnectionContext(lineTcpConfiguration, authDb, scheduler);
        disconnected = false;
        recvBuffer = null;
        IODispatcher<LineTcpConnectionContext> dispatcher = new IODispatcher<LineTcpConnectionContext>() {
            @Override
            public void close() {
            }

            @Override
            public int getConnectionCount() {
                return 0;
            }

            @Override
            public void registerChannel(LineTcpConnectionContext context, int operation) {
            }

            @Override
            public boolean processIOQueue(IORequestProcessor<LineTcpConnectionContext> processor) {
                return false;
            }

            @Override
            public void disconnect(LineTcpConnectionContext context, int reason) {
                disconnected = true;
            }

            @Override
            public boolean run(int workerId) {
                return false;
            }

            @Override
            public boolean isListening() {
                return true;
            }
        };
        Assert.assertNull(context.getDispatcher());
        context.of(FD, dispatcher);
        Assert.assertFalse(context.invalid());
        Assert.assertEquals(FD, context.getFd());
        Assert.assertEquals(dispatcher, context.getDispatcher());
        workerPool.start(LOG);
    }

    private void waitForIOCompletion() {
        int maxIterations = 256;
        recvBuffer = null;
        // Guard against slow writers on disconnect
        while (maxIterations-- > 0) {
            if (!handleContextIO()) {
                break;
            }
            LockSupport.parkNanos(1_000_000);
        }
        Assert.assertTrue(maxIterations > 0);
        Assert.assertTrue(disconnected);
        // Wait for last commit
        try {
            Thread.sleep(lineTcpConfiguration.getMaintenanceInterval() + 50);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
}
