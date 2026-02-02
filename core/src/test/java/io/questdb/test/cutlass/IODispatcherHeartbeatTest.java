/*******************************************************************************
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

package io.questdb.test.cutlass;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IOContext;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class IODispatcherHeartbeatTest {
    private static final Log LOG = LogFactory.getLog(IODispatcherHeartbeatTest.class);

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testHeartbeatIntervals() throws Exception {
        LOG.info().$("started testHeartbeatIntervals").$();

        final long heartbeatInterval = 5;
        final long tickCount = 1000;
        final long pingRndEveryN = 3;
        final int connections = 25;
        AtomicInteger connected = new AtomicInteger();

        assertMemoryLeak(() -> {
            TestClock clock = new TestClock();
            try (IODispatcher<TestContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public MillisecondClock getClock() {
                            return clock;
                        }

                        @Override
                        public long getHeartbeatInterval() {
                            return heartbeatInterval;
                        }
                    },
                    fd -> {
                        connected.incrementAndGet();
                        return new TestContext(fd, heartbeatInterval);
                    }
            )) {
                IORequestProcessor<TestContext> processor = new TestProcessor(clock);
                Rnd rnd = new Rnd();
                long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);

                long[] fds = new long[connections];
                for (int i = 0; i < fds.length; i++) {
                    long fd = Net.socketTcp(true);
                    Net.configureNonBlocking(fd);
                    fds[i] = fd;
                }

                long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                try {
                    Unsafe.getUnsafe().putByte(buf, (byte) '.');

                    for (int i = 0; i < fds.length; i++) {
                        Net.connect(fds[i], sockAddr);
                    }
                    while (connected.get() != fds.length) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(processor);
                    }

                    for (int i = 0; i < tickCount; i++) {
                        clock.setCurrent(i);
                        if (rnd.nextBoolean() && i % pingRndEveryN == 0) {
                            int idx = rnd.nextInt(fds.length);
                            Assert.assertEquals(1, Net.send(fds[idx], buf, 1));
                        }
                        dispatcher.run(0);
                        dispatcher.drainIOQueue(processor);
                    }
                } finally {
                    Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                    Net.freeSockAddr(sockAddr);

                    for (int i = 0; i < fds.length; i++) {
                        Net.close(fds[i]);
                    }
                }
            }
        });
    }

    @Test
    public void testHeartbeatsDoNotPreventIdleDisconnects() throws Exception {
        LOG.info().$("started testHeartbeatsDoNotPreventIdleDisconnects").$();

        final long heartbeatInterval = 5;
        final long heartbeatToIdleRatio = 10;
        // the extra ticks are required to detect idle connections and close them
        final long tickCount = heartbeatToIdleRatio * heartbeatInterval + 2;
        final int connections = 25;
        AtomicInteger connected = new AtomicInteger();

        assertMemoryLeak(() -> {
            TestClock clock = new TestClock();
            try (IODispatcher<TestContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public MillisecondClock getClock() {
                            return clock;
                        }

                        @Override
                        public long getHeartbeatInterval() {
                            return heartbeatInterval;
                        }

                        @Override
                        public long getTimeout() {
                            return heartbeatToIdleRatio * heartbeatInterval;
                        }
                    },
                    fd -> {
                        connected.incrementAndGet();
                        return new TestContext(fd, heartbeatInterval);
                    }
            )) {
                IORequestProcessor<TestContext> processor = new TestProcessor(clock);
                long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);

                long[] fds = new long[connections];
                for (int i = 0; i < fds.length; i++) {
                    long fd = Net.socketTcp(true);
                    Net.configureNonBlocking(fd);
                    fds[i] = fd;
                }

                long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                try {
                    for (int i = 0; i < fds.length; i++) {
                        Net.connect(fds[i], sockAddr);
                    }
                    while (connected.get() != fds.length) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(processor);
                    }

                    for (int i = 0; i < tickCount; i++) {
                        clock.setCurrent(i);
                        dispatcher.run(0);
                        dispatcher.drainIOQueue(processor);
                    }

                    TestUtils.assertEventually(() -> {
                        // Verify that all connections were closed on idle timeout.
                        for (int i = 0; i < fds.length; i++) {
                            Assert.assertTrue(NetworkFacadeImpl.INSTANCE.testConnection(fds[i], buf, 1));
                        }
                    }, 10);
                } finally {
                    Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                    Net.freeSockAddr(sockAddr);

                    for (int i = 0; i < fds.length; i++) {
                        Net.close(fds[i]);
                    }
                }
            }
        });
    }

    private static class TestClock implements MillisecondClock {
        volatile long tick = 0;

        @Override
        public long getTicks() {
            return tick;
        }

        public void setCurrent(long tick) {
            this.tick = tick;
        }
    }

    private static class TestContext extends IOContext<TestContext> {
        private final long buffer = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        private final long heartbeatInterval;
        boolean isPreviousEventHeartbeat = true;
        long previousHeartbeatTs;
        long previousReadTs;

        public TestContext(long fd, long heartbeatInterval) {
            super(PlainSocketFactory.INSTANCE, NetworkFacadeImpl.INSTANCE, LOG);
            socket.of(fd);
            this.heartbeatInterval = heartbeatInterval;
        }

        public void checkInvariant(int operation, long current) {
            if (IOOperation.HEARTBEAT == operation) {
                if (isPreviousEventHeartbeat) {
                    if (previousHeartbeatTs == 0) {
                        // +1, heartbeat triggered on the next tick
                        // +2, heartbeat recalculated on the next tick
                        Assert.assertEquals(heartbeatInterval + 1, current);
                    } else {
                        Assert.assertEquals(heartbeatInterval + 2, current - previousHeartbeatTs);
                    }
                } else {
                    Assert.assertEquals(heartbeatInterval + 2, current - previousReadTs);
                }

                previousHeartbeatTs = current;
                isPreviousEventHeartbeat = true;
            } else {
                Assert.assertEquals(1, Net.recv(getFd(), buffer, 1));
                previousReadTs = current;
                isPreviousEventHeartbeat = false;
            }
        }

        @Override
        public void close() {
            Unsafe.free(buffer, 4, MemoryTag.NATIVE_DEFAULT);
            super.close();
        }

        @Override
        public boolean invalid() {
            return false;
        }
    }

    private record TestProcessor(TestClock clock) implements IORequestProcessor<TestContext> {
        @Override
        public boolean onRequest(int operation, TestContext context, IODispatcher<TestContext> dispatcher) {
            context.checkInvariant(operation, clock.getTicks());
            dispatcher.registerChannel(context, operation);
            return true;
        }
    }
}
