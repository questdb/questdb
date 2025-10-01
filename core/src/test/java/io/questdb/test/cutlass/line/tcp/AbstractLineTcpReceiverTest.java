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

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.auth.EllipticCurveAuthenticatorFactory;
import io.questdb.cutlass.auth.LineAuthenticatorFactory;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfigurationHelper;
import io.questdb.cutlass.line.tcp.StaticChallengeResponseMatcher;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.PublicKey;

import static io.questdb.test.tools.TestUtils.assertEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AbstractLineTcpReceiverTest extends AbstractCairoTest {
    public static final String AUTH_KEY_ID1 = "testUser1";
    public static final String AUTH_KEY_ID2 = "testUser2";
    public static final String AUTH_TOKEN_KEY1 = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    public static final PrivateKey AUTH_PRIVATE_KEY1 = AuthUtils.toPrivateKey(AUTH_TOKEN_KEY1);
    public static final String AUTH_TOKEN_KEY2 = "AIZc78-On-91DLplVNtyLOmKddY0AL9mnT5onl19Vv_g";
    public static final PrivateKey AUTH_PRIVATE_KEY2 = AuthUtils.toPrivateKey(AUTH_TOKEN_KEY2);
    public static final char[] TRUSTSTORE_PASSWORD = "questdb".toCharArray();
    public static final String TRUSTSTORE_PATH = "/keystore/server.keystore";
    protected static final int WAIT_ALTER_TABLE_RELEASE = 0x4;
    protected static final int WAIT_ENGINE_TABLE_RELEASE = 0x1;
    protected static final int WAIT_ILP_TABLE_RELEASE = 0x2;
    protected static final int WAIT_NO_WAIT = 0x0;
    private final static Log LOG = LogFactory.getLog(AbstractLineTcpReceiverTest.class);
    protected final int bindPort = 9002; // Don't clash with other tests since they may run in parallel
    private final ThreadLocal<Socket> tlSocket = new ThreadLocal<>();
    protected String authKeyId = null;
    private final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
        @Override
        public @NotNull LineAuthenticatorFactory getLineAuthenticatorFactory() {
            if (authKeyId == null) {
                return super.getLineAuthenticatorFactory();
            }
            URL u = getClass().getResource("authDb.txt");
            assert u != null;
            CharSequenceObjHashMap<PublicKey> authDb = AuthUtils.loadAuthDb(u.getFile());
            return new EllipticCurveAuthenticatorFactory(() -> new StaticChallengeResponseMatcher(authDb));
        }
    };
    protected boolean autoCreateNewColumns = true;
    protected long commitIntervalDefault = 2000;
    protected double commitIntervalFraction = 0.5;
    protected boolean disconnectOnError = false;
    protected long maintenanceInterval = 25;
    protected int maxMeasurementSize = 1024;
    protected long minIdleMsBeforeWriterRelease = 30000;
    protected int msgBufferSize = 256 * 1024;
    protected NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
    protected int partitionByDefault = PartitionBy.DAY;
    protected TestTimestampType timestampType = TestTimestampType.MICRO;
    protected boolean useLegacyStringDefault = true;
    protected final LineTcpReceiverConfiguration lineConfiguration = new DefaultLineTcpReceiverConfiguration(configuration) {
        @Override
        public boolean getAutoCreateNewColumns() {
            return autoCreateNewColumns;
        }

        @Override
        public int getBindPort() {
            return bindPort;
        }

        @Override
        public long getCommitInterval() {
            return LineTcpReceiverConfigurationHelper.calcCommitInterval(
                    configuration.getO3MinLag(),
                    getCommitIntervalFraction(),
                    getCommitIntervalDefault()
            );
        }

        @Override
        public long getCommitIntervalDefault() {
            return commitIntervalDefault;
        }

        @Override
        public double getCommitIntervalFraction() {
            return commitIntervalFraction;
        }

        @Override
        public int getDefaultColumnTypeForTimestamp() {
            return timestampType.getTimestampType();
        }

        @Override
        public int getDefaultPartitionBy() {
            return partitionByDefault;
        }

        @Override
        public boolean getDisconnectOnError() {
            return disconnectOnError;
        }

        @Override
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public long getHeartbeatInterval() {
            return 15;
        }

        @Override
        public long getMaintenanceInterval() {
            return maintenanceInterval;
        }

        @Override
        public int getMaxMeasurementSize() {
            return maxMeasurementSize;
        }

        @Override
        public Clock getMicrosecondClock() {
            return testMicrosClock;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return nf;
        }

        @Override
        public int getRecvBufferSize() {
            return msgBufferSize;
        }

        @Override
        public long getWriterIdleTimeout() {
            return minIdleMsBeforeWriterRelease;
        }

        @Override
        public int getWriterQueueCapacity() {
            return 4;
        }

        @Override
        public boolean isUseLegacyStringDefault() {
            return useLegacyStringDefault;
        }
    };

    protected final WorkerPool sharedWorkerPool = new TestWorkerPool(getWorkerCount(), lineConfiguration.getMetrics());

    public static void assertTableExists(CairoEngine engine, CharSequence tableName) {
        try (Path path = new Path()) {
            assertEquals(TableUtils.TABLE_EXISTS, engine.getTableStatus(path, engine.getTableTokenIfExists(tableName)));
        }
    }

    public static void assertTableExistsEventually(CairoEngine engine, CharSequence tableName) throws Exception {
        assertEventually(() -> assertTableExists(engine, tableName));
    }

    public static void assertTableSizeEventually(CairoEngine engine, CharSequence tableName, long expectedSize) throws Exception {
        TestUtils.assertEventually(() -> {
            assertTableExists(engine, tableName);

            try (
                    TableReader reader = getReader(tableName);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                long size = cursor.size();
                assertEquals(expectedSize, size);
            } catch (EntryLockedException e) {
                // if table is busy we want to fail this round and have the assertEventually() to retry later
                fail("table +" + tableName + " is locked");
            }
        });
    }

    public static LineTcpReceiver createLineTcpReceiver(
            LineTcpReceiverConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool
    ) {
        return new LineTcpReceiver(configuration, cairoEngine, workerPool, workerPool);
    }

    @After
    public void cleanup() {
        maxMeasurementSize = 256;
        authKeyId = null;
        msgBufferSize = 256 * 1024;
        minIdleMsBeforeWriterRelease = 30000;
        maintenanceInterval = 25;
        commitIntervalFraction = 0.5;
        commitIntervalDefault = 2000;
        partitionByDefault = PartitionBy.DAY;
        disconnectOnError = false;
        nf = NetworkFacadeImpl.INSTANCE;
    }

    protected void assertTable(CharSequence expected, CharSequence tableName) {
        try (
                TableReader reader = getReader(tableName);
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            assertCursorTwoPass(expected, cursor, reader.getMetadata());
        }
    }

    protected Socket getSocket() {
        Socket socket = tlSocket.get();
        if (socket != null) {
            return socket;
        }

        socket = newSocket();

        tlSocket.set(socket);
        return socket;
    }

    protected int getWorkerCount() {
        return 1;
    }

    protected Socket newSocket() {
        final int ipv4address = Net.parseIPv4("127.0.0.1");
        final long sockaddr = Net.sockaddr(ipv4address, bindPort);
        final long fd = Net.socketTcp(true);
        final Socket socket = new Socket(sockaddr, fd);

        if (TestUtils.connect(fd, sockaddr) != 0) {
            throw new RuntimeException("could not connect, errno=" + Os.errno());
        }
        return socket;
    }

    protected void runInContext(LineTcpServerAwareContext r) throws Exception {
        runInContext(r, false, 250);
    }

    protected void runInContext(FilesFacade ff, LineTcpServerAwareContext r, boolean needMaintenanceJob, long minIdleMsBeforeWriterRelease) throws Exception {
        this.minIdleMsBeforeWriterRelease = minIdleMsBeforeWriterRelease;
        assertMemoryLeak(ff, () -> {
            try (LineTcpReceiver receiver = createLineTcpReceiver(lineConfiguration, engine, sharedWorkerPool)) {
                WorkerPoolUtils.setupWriterJobs(sharedWorkerPool, engine);
                if (needMaintenanceJob) {
                    sharedWorkerPool.assign(engine.getEngineMaintenanceJob());
                }
                sharedWorkerPool.start(LOG);
                try {
                    r.run(receiver);
                } catch (Throwable err) {
                    LOG.error().$("Stopping ILP worker pool because of an error").$(err).$();
                    throw err;
                } finally {
                    sharedWorkerPool.halt();
                    Path.clearThreadLocals();
                }
            } catch (Throwable err) {
                LOG.error().$("Stopping ILP receiver because of an error").$(err).$();
                throw err;
            }
        });
    }

    protected void runInContext(LineTcpServerAwareContext r, boolean needMaintenanceJob, long minIdleMsBeforeWriterRelease) throws Exception {
        runInContext(AbstractCairoTest.ff, r, needMaintenanceJob, minIdleMsBeforeWriterRelease);
    }

    protected void send(CharSequence tableName, int wait, Runnable sendToSocket) {
        send(wait, sendToSocket, tableName);
    }

    protected void send(int wait, Runnable sendToSocket, CharSequence... tableNames) {

        if (wait == WAIT_NO_WAIT) {
            sendToSocket.run();
            return;
        }

        ConcurrentHashMap<CharSequence> tablesToWaitFor = new ConcurrentHashMap<>();
        for (CharSequence tableName : tableNames) {
            tablesToWaitFor.put(tableName, tableName);
        }
        SOCountDownLatch releaseLatch = new SOCountDownLatch(tablesToWaitFor.size());
        try {
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                    if (name != null && tablesToWaitFor.remove(name.getTableName()) != null) {
                        releaseLatch.countDown();
                    }
                }
            });
            sendToSocket.run();
            releaseLatch.await();
        } finally {
            engine.setPoolListener(null);
        }
    }

    protected void sendToSocket(Socket socket, String lineData) {
        byte[] lineDataBytes = lineData.getBytes(StandardCharsets.UTF_8);
        long bufaddr = Unsafe.malloc(lineDataBytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int n = 0; n < lineDataBytes.length; n++) {
                Unsafe.getUnsafe().putByte(bufaddr + n, lineDataBytes[n]);
            }
            int sent = 0;
            while (sent != lineDataBytes.length) {
                int rc = Net.send(socket.fd, bufaddr + sent, lineDataBytes.length - sent);
                if (rc < 0) {
                    LOG.error().$("Data sending failed [rc=").$(rc)
                            .$(", sent=").$(sent)
                            .$(", bufferSize=").$(lineDataBytes.length)
                            .I$();
                    throw new RuntimeException("Data sending failed [rc=" + rc + "]");
                }
                sent += rc;
                if (sent != lineDataBytes.length) {
                    LOG.info().$("Data sending is in progress [rc=").$(rc)
                            .$(", sent=").$(sent)
                            .$(", bufferSize=").$(lineDataBytes.length)
                            .I$();
                }
            }
        } finally {
            Unsafe.free(bufaddr, lineDataBytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    protected void sendToSocket(String lineData) {
        try (Socket socket = getSocket()) {
            sendToSocket(socket, lineData);
        } catch (Exception e) {
            Assert.fail("Data sending failed [e=" + e + "]");
        }
    }

    @FunctionalInterface
    public interface LineTcpServerAwareContext {
        void run(LineTcpReceiver receiver) throws Exception;
    }

    protected class Socket implements AutoCloseable {
        private final long fd;
        private final long sockaddr;

        private Socket(long sockaddr, long fd) {
            this.sockaddr = sockaddr;
            this.fd = fd;
        }

        @Override
        public void close() {
            tlSocket.remove();
            Net.close(fd);
            Net.freeSockAddr(sockaddr);
        }
    }
}
