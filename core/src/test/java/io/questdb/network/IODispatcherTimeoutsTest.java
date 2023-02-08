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

package io.questdb.network;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.network.IODispatcher.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class IODispatcherTimeoutsTest {
    private static final Log LOG = LogFactory.getLog(IODispatcherTimeoutsTest.class);
    private static final String TICK = "tick";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9001;
    final static IODispatcherConfiguration dispatcherConf = new DefaultIODispatcherConfiguration();
    static WorkerPool workerPool;
    final static IODispatcher<TestConnectionContext> dispatcher = IODispatchers.create(dispatcherConf, new MutableIOContextFactory<>(TestConnectionContext::new, 8));
    final static TestRequestProcessor processor = new TestRequestProcessor();
    private static long timeoutMillis = -1L;
    private static boolean readjustOnRead = false;
    private static Rnd rnd = new Rnd();
    @BeforeClass
    public static void setUpStatic() {
        workerPool = new WorkerPool(() -> 1);
        workerPool.assign(dispatcher);
        workerPool.assign((workerId, runStatus) -> dispatcher.processIOQueue(processor));
        workerPool.start();
    }

    @AfterClass
    public static void tearDownStatic() {
        workerPool.halt();
        workerPool = null;
    }

    @Test
    public void testNoTicks() throws Exception {
        timeoutMillis = 10;
        readjustOnRead = false;
        assertMemoryLeak(() -> {
           tick(100, 150);
        });
    }

    @Test
    public void testRegularTicksAndTimeouts() throws Exception {
        timeoutMillis = 10;
        readjustOnRead = false;
        assertMemoryLeak(() -> {
            tick(100, 20);
        });
    }

    @Test
    public void testAdjustNextOnEveryRead() throws Exception {
        timeoutMillis = 10;
        readjustOnRead = true;

        assertMemoryLeak(() -> {
            tick(100, 20);
        });
    }

    @Test
    public void testSeveralParallelConnection() throws Exception {
        timeoutMillis = 10;
        readjustOnRead = false;
        assertMemoryLeak(() -> {
            nParallelConnection(8, rnd);
        });
    }

    @Test
    public void testSeveralParallelConnectionAdjust() throws Exception {
        timeoutMillis = 10;
        readjustOnRead = true;
        Rnd rnd = new Rnd();
        assertMemoryLeak(() -> {
            nParallelConnection(22, rnd);
        });
    }

    private static void nParallelConnection(int N, Rnd rnd) throws InterruptedException {
        Thread[] threads = new Thread[N];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    int durationJitter = rnd.nextInt(25);
                    int delayJitter = rnd.nextInt(5);
                    tick(100 + durationJitter, 20 + delayJitter);
                }
            };
        }

        for (int i = 0; i < threads.length; i++) {
            int startJitter = rnd.nextInt(100);
            Os.sleep(startJitter);
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
           threads[i].join();
        }
        Os.sleep(100);
    }

    private static void tick(long durationMillis, long delayMillis) {
        long bufSize = TICK.length();
        long inf = Net.getAddrInfo(HOST, PORT);
        int fd = Net.socketTcp(true);
        try {
            if (Net.connectAddrInfo(fd, inf) != 0) {
                LOG.error()
                        .$("could not connect [host=").$(HOST)
                        .$(", port=").$(PORT)
                        .$(", errno=").$(Os.errno())
                        .I$();
            } else {
                long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
                Chars.asciiStrCpy(TICK, buf);
                try {
                    long durationUs = durationMillis * Timestamps.MILLI_MICROS;
                    long startUs = Os.currentTimeMicros();
                    Os.sleep(delayMillis);
                    while (Os.currentTimeMicros() - durationUs < startUs) {
                        int n = Net.send(fd, buf, TICK.length());
                        if (n < 0) {
                            LOG.error().$("connection lost").$();
                            break;
                        }
                        Assert.assertEquals(n, TICK.length());
                        Os.sleep(delayMillis);
                    }
                } finally {
                    Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
                }
            }
        } finally {
            Net.freeAddrInfo(inf);
            Net.close(fd);
            Os.sleep(100); // wait for the scheduler to close the contexts
        }
    }

    private static class TestConnectionContext extends AbstractMutableIOContext<TestConnectionContext> {
        private long nextMillis = 0;
        private long prevTimeoutMillis = 0;
        private long prevReadMillis = 0;

        @Override
        public void clear() {
            nextMillis = 0;
            prevTimeoutMillis = 0;
            LOG.debug().$("context fd: ").$(fd).$(" cleared").$();
        }

        @Override
        public void close() {
            LOG.debug().$("context fd: ").$(fd).$(" closed").$();
        }

        public void onRead() {
            final long buffer = Unsafe.malloc(TICK.length(), MemoryTag.NATIVE_DEFAULT);
            try {

                int n = Net.recv(getFd(), buffer, TICK.length());
                if (n > 0) {
                    long now = Os.currentTimeMicros() / Timestamps.MILLI_MICROS;
                    if (readjustOnRead) {
                        // in real life isTimeout and onRead will be called from different threads.
                        prevReadMillis = now;
                        nextMillis = now + timeoutMillis;
                    }
                    LOG.debug().$("read ").$(now).$();
                    getDispatcher().registerChannel(this, IOOperation.READ);
                } else {
                    getDispatcher().disconnect(this, DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV);
                }
            } finally {
                Unsafe.free(buffer, TICK.length(), MemoryTag.NATIVE_DEFAULT);
            }
        }

        public void onWrite() {
            getDispatcher().registerChannel(this, IOOperation.WRITE);
        }

        @Override
        public boolean isTimeout(long nowMillis) {
            if (nowMillis >= nextMillis) {
                nextMillis = nowMillis + timeoutMillis;
                return true;
            }
            return false;
        }

        public void onTimeout() {
            long now = MillisecondClockImpl.INSTANCE.getTicks();
            LOG.debug().$("timeout: ").$(now).$();

            long delta = now - prevTimeoutMillis;
            if (readjustOnRead) {
                // timeout should be always timeoutMillis away from the latest read event
                Assert.assertTrue(prevReadMillis == 0 || now - prevReadMillis >= timeoutMillis);
            } else {
                // not much we can guarantee here. delta can be less than timeoutMillis due to queue processing/delay,
                // but we should not lose timeouts anyway (delta <= 2 * timeoutMillis)
                Assert.assertTrue(prevTimeoutMillis == 0 || delta <= 2 * timeoutMillis);
            }
            prevTimeoutMillis = now;
        }
    }

    private static class TestRequestProcessor implements IORequestProcessor<TestConnectionContext> {
        @Override
        public boolean onRequest(int operation, TestConnectionContext context) {
            boolean io = false;

            if (IOOperation.isRead(operation)) {
                context.onRead();
                io = true;
            }
            if (IOOperation.isWrite(operation)) {
                context.onWrite();
                io = true;
            }

            if (!io) {
                // trigger if idle only
                if (IOOperation.isTimeout(operation)) {
                    context.onTimeout();
                    context.getDispatcher().registerChannel(context, IOOperation.READ);
                }
            }
            return true;
        }
    }
}
