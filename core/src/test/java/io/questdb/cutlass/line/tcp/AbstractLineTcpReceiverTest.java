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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.Net;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;

import java.net.URL;
import java.nio.charset.StandardCharsets;

public class AbstractLineTcpReceiverTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(AbstractLineTcpReceiverTest.class);
    protected final WorkerPool sharedWorkerPool = new WorkerPool(new WorkerPoolConfiguration() {
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
    });
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
    protected int maxMeasurementSize = 50;
    protected String authKeyId = null;
    protected int msgBufferSize = 1024;
    protected long minIdleMsBeforeWriterRelease = 30000;
    protected int aggressiveReadRetryCount = 0;
    protected final LineTcpReceiverConfiguration lineConfiguration = new DefaultLineTcpReceiverConfiguration() {
        @Override
        public IODispatcherConfiguration getNetDispatcherConfiguration() {
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
            return 25;
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
        public int getAggressiveReadRetryCount() {
            return aggressiveReadRetryCount;
        }
    };

    @After
    public void cleanup() {
        maxMeasurementSize = 50;
    }

    @FunctionalInterface
    public interface LineTcpServerAwareContext {
        void run(LineTcpReceiver receiver);
    }

    protected void runInContext(LineTcpReceiverTest.LineTcpServerAwareContext r) throws Exception {
        runInContext(r, false);
    }

    protected void runInContext(LineTcpReceiverTest.LineTcpServerAwareContext r, boolean needMaintenanceJob) throws Exception {
        minIdleMsBeforeWriterRelease = 250;
        assertMemoryLeak(() -> {
            final Path path = new Path(4096);
            try (LineTcpReceiver receiver = LineTcpReceiver.create(lineConfiguration, sharedWorkerPool, LOG, engine)) {
                sharedWorkerPool.assignCleaner(Path.CLEANER);
                if (needMaintenanceJob) {
                    sharedWorkerPool.assign(engine.getEngineMaintenanceJob());
                }
                sharedWorkerPool.start(LOG);
                try {
                    r.run(receiver);
                } catch (Throwable err) {
                    LOG.error().$("Stopping ILP worker pool because of an error").$();
                    throw err;
                } finally {
                    sharedWorkerPool.halt();
                    Path.clearThreadLocals();
                }
            } catch (Throwable err) {
                LOG.error().$("Stopping ILP receiver because of an error").$();
                throw err;
            } finally {
                Misc.free(path);
            }
        });
    }

    protected void sendToSocket(String lineData, boolean noLinger) {
        int ipv4address = Net.parseIPv4("127.0.0.1");
        long sockaddr = Net.sockaddr(ipv4address, bindPort);
        long fd = Net.socketTcp(true);
        try {
            TestUtils.assertConnect(fd, sockaddr, noLinger);
            byte[] lineDataBytes = lineData.getBytes(StandardCharsets.UTF_8);
            long bufaddr = Unsafe.malloc(lineDataBytes.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int n = 0; n < lineDataBytes.length; n++) {
                    Unsafe.getUnsafe().putByte(bufaddr + n, lineDataBytes[n]);
                }
                int rc = Net.send(fd, bufaddr, lineDataBytes.length);
                Assert.assertEquals(lineDataBytes.length, rc);
            } finally {
                Unsafe.free(bufaddr, lineDataBytes.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Net.close(fd);
            Net.freeSockAddr(sockaddr);
        }
    }
}
