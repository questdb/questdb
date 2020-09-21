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

import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Unsafe;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.std.microtime.MicrosecondClockImpl;
import io.questdb.test.tools.TestUtils;

public class LineTcpAuthConnectionContextTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpAuthConnectionContextTest.class);
    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthDb.importPrivateKey("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48");
    private final static String AUTH_KEY_ID2 = "testUser2";
    private final static PrivateKey AUTH_PRIVATE_KEY2 = AuthDb.importPrivateKey("lwJi3TSb4G6UcHxFJmPhOTWa4BLwJOOiK76wT6Uk7pI");
    private static final int FD = 1_000_000;
    private final Random rand = new Random(0);
    private LineTcpAuthConnectionContext context;
    private LineTcpReceiverConfiguration lineTcpConfiguration;
    private LineTcpMeasurementScheduler scheduler;
    private AtomicInteger netMsgBufferSize = new AtomicInteger(1024);
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
                Assert.assertNull(sentBytes);
                if (maxSendBytes <= 0) {
                    return maxSendBytes;
                }

                int nSent = bufferLen <= maxSendBytes ? bufferLen : maxSendBytes;
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
                return u.getFile();
            }
        };
    }

    @Test
    public void testGoodAuthentication() throws Exception {
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
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
    public void testGoodAuthenticationFragmented1() throws Exception {
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, true, false);
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
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, false, true);
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
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, true, true);
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
    public void testBadUser() throws Exception {
        runInContext(() -> {
            boolean authSequenceCompleted = authenticate(AUTH_KEY_ID2, AUTH_PRIVATE_KEY2);
            Assert.assertTrue(authSequenceCompleted);
            Assert.assertTrue(disconnected);
        });
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
    public void testInvalidKeyId() throws Exception {
        runInContext(() -> {
            StringBuilder token = new StringBuilder("xxxxxxxx");
            while (token.length() < netMsgBufferSize.get()) {
                token.append(token.toString());
            }
            boolean authSequenceCompleted = authenticate(token.toString(), AUTH_PRIVATE_KEY1);
            Assert.assertFalse(authSequenceCompleted);
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
            Assert.assertEquals("Minimum buffer length is 513", ex.getFlyweightMessage().toString());
        }
    }

    private boolean authenticate(String authKeyId, PrivateKey authPrivateKey) {
        return authenticate(authKeyId, authPrivateKey, false, false);
    }

    private boolean authenticate(String authKeyId, PrivateKey authPrivateKey, boolean fragmentKeyId, boolean fragmentChallenge) {
        authKeyId += "\n";
        if (fragmentKeyId) {
            int nSent = 0;
            do {
                int n = 1 + rand.nextInt(3);
                if (n + nSent > authKeyId.length()) {
                    recvBuffer = authKeyId.substring(nSent);
                } else {
                    recvBuffer = authKeyId.substring(nSent, nSent + n);
                }
                nSent += n;
                handleContextIO();
            } while (nSent < authKeyId.length());
        } else {
            recvBuffer = authKeyId;
            handleContextIO();
        }
        byte[] challengeBytes = readChallenge(fragmentChallenge);
        if (null == challengeBytes) {
            return false;
        }
        try {
            Signature sig = Signature.getInstance(AuthDb.SIGNATURE_TYPE);
            sig.initSign(authPrivateKey);
            sig.update(challengeBytes, 0, challengeBytes.length - 1);
            byte[] rawSignature = sig.sign();
            byte[] signature = Base64.getEncoder().encode(rawSignature);
            recvBuffer = new String(signature, StandardCharsets.UTF_8) + "\n";
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

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertThat(expected, reader.getCursor(), reader.getMetadata(), true);
        }
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

    private void runInContext(Runnable r) throws Exception {
        runInContext(r, null);
    }

    private void runInContext(Runnable r, Runnable onCommitNewEvent) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                setupContext(engine, onCommitNewEvent);
                try {
                    r.run();
                } finally {
                    closeContext();
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                }
            }
        });
    }

    private void setupContext(CairoEngine engine, Runnable onCommitNewEvent) {
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
        scheduler = new LineTcpMeasurementScheduler(lineTcpConfiguration, engine, workerPool) {
            @Override
            void commitNewEvent(LineTcpMeasurementEvent event, boolean complete) {
                if (null != onCommitNewEvent) {
                    onCommitNewEvent.run();
                }
                super.commitNewEvent(event, complete);
            }
        };

        AuthDb authDb = new AuthDb(lineTcpConfiguration);
        context = new LineTcpAuthConnectionContext(lineTcpConfiguration, authDb, scheduler);
        disconnected = false;
        recvBuffer = null;
        IODispatcher<LineTcpConnectionContext> dispatcher = new IODispatcher<>() {
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
            public void disconnect(LineTcpConnectionContext context) {
                disconnected = true;
            }

            @Override
            public boolean run(int workerId) {
                return false;
            }
        };
        Assert.assertNull(context.getDispatcher());
        context.of(FD, dispatcher);
        Assert.assertFalse(context.invalid());
        Assert.assertEquals(FD, context.getFd());
        Assert.assertEquals(dispatcher, context.getDispatcher());
        workerPool.start(LOG);
    }

    private boolean handleContextIO() {
        switch (context.handleIO()) {
            case NEEDS_READ:
                context.getDispatcher().registerChannel(context, IOOperation.READ);
                return false;
            case NEEDS_WRITE:
                context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                return false;
            case NEEDS_CPU:
                return true;
            case NEEDS_DISCONNECT:
                context.getDispatcher().disconnect(context);
                return false;
        }
        return false;
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
    }
}
