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

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.O3Utils;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.Net;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;

import java.io.Closeable;
import java.lang.ThreadLocal;
import java.net.URL;
import java.nio.charset.StandardCharsets;

class AbstractLineTcpReceiverTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(AbstractLineTcpReceiverTest.class);

    protected static final int WAIT_NO_WAIT = 0x0;
    protected static final int WAIT_ENGINE_TABLE_RELEASE = 0x1;
    protected static final int WAIT_ILP_TABLE_RELEASE = 0x2;
    protected static final int WAIT_ALTER_TABLE_RELEASE = 0x4;

    private final ThreadLocal<Socket> tlSocket = new ThreadLocal<>();

    protected final WorkerPool sharedWorkerPool = new WorkerPool(getWorkerPoolConfiguration(), metrics);
    protected WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return new WorkerPoolConfiguration() {
            private final int[] affinity = {-1};

            @Override
            public int[] getWorkerAffinity() {
                return affinity;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean haltOnError() {
                return true;
            }
        };
    }
    protected final int bindPort = 9002; // Don't clash with other tests since they may run in parallel
    private final IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration() {
        @Override
        public int getBindIPv4Address() {
            return 0;
        }

        @Override
        public int getBindPort() {
            return bindPort;
        }
    };
    protected int maxMeasurementSize = 256;
    protected String authKeyId = null;
    protected int msgBufferSize = 256 * 1024;
    protected long minIdleMsBeforeWriterRelease = 30000;
    protected long maintenanceInterval = 25;
    protected double commitIntervalFraction = 0.5;
    protected long commitIntervalDefault = 2000;
    protected boolean disconnectOnError = false;
    protected boolean symbolAsFieldSupported;

    protected final LineTcpReceiverConfiguration lineConfiguration = new DefaultLineTcpReceiverConfiguration() {
        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return ioDispatcherConfiguration;
        }

        @Override
        public int getNetMsgBufferSize() {
            return msgBufferSize;
        }

        @Override
        public int getMaxMeasurementSize() {
            return maxMeasurementSize;
        }

        @Override
        public int getWriterQueueCapacity() {
            return 4;
        }

        @Override
        public MicrosecondClock getMicrosecondClock() {
            return testMicrosClock;
        }

        @Override
        public long getMaintenanceInterval() {
            return maintenanceInterval;
        }

        @Override
        public double getCommitIntervalFraction() {
            return commitIntervalFraction;
        }

        @Override
        public long getCommitIntervalDefault() {
            return commitIntervalDefault;
        }

        @Override
        public String getAuthDbPath() {
            if (null == authKeyId) {
                return null;
            }
            URL u = getClass().getResource("authDb.txt");
            assert u != null;
            return u.getFile();
        }

        @Override
        public long getWriterIdleTimeout() {
            return minIdleMsBeforeWriterRelease;
        }

        @Override
        public boolean getDisconnectOnError() {
            return disconnectOnError;
        }

        @Override
        public boolean isSymbolAsFieldSupported() {
            return symbolAsFieldSupported;
        }
    };

    @After
    public void cleanup() {
        maxMeasurementSize = 256;
    }

    @FunctionalInterface
    public interface LineTcpServerAwareContext {
        void run(LineTcpReceiver receiver) throws Exception;
    }

    protected void runInContext(LineTcpServerAwareContext r) throws Exception {
        runInContext(r, false, 250);
    }

    protected void runInContext(LineTcpServerAwareContext r, boolean needMaintenanceJob, long minIdleMsBeforeWriterRelease) throws Exception {
        this.minIdleMsBeforeWriterRelease = minIdleMsBeforeWriterRelease;
        assertMemoryLeak(() -> {
            final Path path = new Path(4096);
            try (LineTcpReceiver receiver = LineTcpReceiver.create(lineConfiguration, sharedWorkerPool, LOG, engine, metrics)) {
                sharedWorkerPool.assignCleaner(Path.CLEANER);
                try (Closeable ignored = O3Utils.setupWorkerPool(sharedWorkerPool, engine.getMessageBus())) {
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
                        O3Utils.freeBuf();
                        Path.clearThreadLocals();
                    }
                }
            } catch (Throwable err) {
                LOG.error().$("Stopping ILP receiver because of an error").$(err).$();
                throw err;
            } finally {
                Misc.free(path);
            }
        });
    }

    protected void send(LineTcpReceiver receiver, CharSequence tableName, int wait, Runnable sendToSocket) {
        SOCountDownLatch releaseLatch = new SOCountDownLatch(1);
        final CharSequence t = tableName;
        switch (wait) {
            case WAIT_ENGINE_TABLE_RELEASE:
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (Chars.equals(tableName, name)) {
                        if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN && Chars.equals(tableName, t) ) {
                            releaseLatch.countDown();
                        }
                    }
                });
                break;
            case WAIT_ILP_TABLE_RELEASE:
                receiver.setSchedulerListener((tableName1, event) -> {
                    if (Chars.equals(tableName1, tableName1)) {
                        releaseLatch.countDown();
                    }
                });
                break;
        }

        try {
            sendToSocket.run();
            if (wait != WAIT_NO_WAIT) {
                releaseLatch.await();
            }
        } finally {
            switch (wait) {
                case WAIT_ENGINE_TABLE_RELEASE:
                    engine.setPoolListener(null);
                    break;
                case WAIT_ILP_TABLE_RELEASE:
                    receiver.setSchedulerListener(null);
                    break;
            }
        }
    }

    protected Socket getSocket() {
        Socket socket = tlSocket.get();
        if (socket != null) {
            return socket;
        }

        int ipv4address = Net.parseIPv4("127.0.0.1");
        long sockaddr = Net.sockaddr(ipv4address, bindPort);
        long fd = Net.socketTcp(true);
        socket = new Socket(sockaddr, fd);

        if (TestUtils.connect(fd, sockaddr) != 0) {
            throw new RuntimeException("could not connect, errno=" + Os.errno());
        }

        tlSocket.set(socket);
        return socket;
    }

    protected void sendToSocket(String lineData) {
        try (Socket socket = getSocket()) {
            sendToSocket(socket, lineData);
        } catch (Exception e) {
            Assert.fail("Data sending failed [e=" + e + "]");
            LOG.error().$(e).$();
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
                    throw new RuntimeException("Data sending failed [rc=" + rc + "]");
                }
                sent += rc;
            }
        } finally {
            Unsafe.free(bufaddr, lineDataBytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    protected class Socket implements AutoCloseable {
        private final long sockaddr;
        private final long fd;

        private Socket(long sockaddr, long fd) {
            this.sockaddr = sockaddr;
            this.fd = fd;
        }

        @Override
        public void close() throws Exception {
            tlSocket.set(null);
            Net.close(fd);
            Net.freeSockAddr(sockaddr);
        }
    }

    protected void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            assertCursorTwoPass(expected, reader.getCursor(), reader.getMetadata());
        }
    }
}
