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

package io.questdb.test.cutlass;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IOContext;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.SuspendEvent;
import io.questdb.network.SuspendEventFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
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
                    (fd, d) -> {
                        connected.incrementAndGet();
                        return new TestContext(fd, d, heartbeatInterval);
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
                    (fd, d) -> {
                        connected.incrementAndGet();
                        return new TestContext(fd, d, heartbeatInterval);
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

    @Test
    public void testHeartbeatsDoNotPreventSuspendEventDeadlines() throws Exception {
        LOG.info().$("started testHeartbeatsDoNotPreventSuspendEventDeadlines").$();

        final long heartbeatInterval = 5;
        final long suspendEventDeadline = 10 * heartbeatInterval;
        // the extra ticks are required to detect suspend event deadline
        final long tickCount = suspendEventDeadline + 2;
        AtomicInteger connected = new AtomicInteger();

        assertMemoryLeak(() -> {
            TestClock clock = new TestClock();
            IODispatcherConfiguration ioDispatcherConfig = new DefaultIODispatcherConfiguration() {
                @Override
                public MillisecondClock getClock() {
                    return clock;
                }

                @Override
                public long getHeartbeatInterval() {
                    return heartbeatInterval;
                }
            };
            try (IODispatcher<TestContext> dispatcher = IODispatchers.create(
                    ioDispatcherConfig,
                    (fd, d) -> {
                        connected.incrementAndGet();
                        return new TestContext(fd, d, heartbeatInterval);
                    }
            )) {
                SuspendEvent suspendEvent = SuspendEventFactory.newInstance(ioDispatcherConfig);
                suspendEvent.setDeadline(suspendEventDeadline);
                IORequestProcessor<TestContext> processor = new SuspendingTestProcessor(clock, suspendEvent);
                long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);

                long fd = Net.socketTcp(true);
                Net.configureNonBlocking(fd);

                long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                try {
                    Unsafe.getUnsafe().putByte(buf, (byte) '.');

                    Net.connect(fd, sockAddr);
                    while (connected.get() != 1) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(processor);
                    }

                    // Write to socket to generate a socket read.
                    Assert.assertEquals(1, Net.send(fd, buf, 1));

                    Os.sleep(10); // make sure the read detected on tick == 0
                    for (int i = 0; i < tickCount; i++) {
                        clock.setCurrent(i);
                        dispatcher.run(0);
                        dispatcher.drainIOQueue(processor);
                    }

                    TestUtils.assertEventually(() -> {
                        // Verify that the event is closed due to the deadline.
                        Assert.assertTrue(suspendEvent.isClosedByAtLeastOneSide());
                    }, 10);
                } finally {
                    Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                    Net.freeSockAddr(sockAddr);
                    Misc.free(suspendEvent);
                    Net.close(fd);
                }
            }
        });
    }

    @Test
    public void testSuspendEventDoesNotPreventHeartbeats() throws Exception {
        LOG.info().$("started testSuspendEventDoesNotPreventHeartbeats").$();

        final long heartbeatInterval = 5;
        final long tickCount = 1000;
        AtomicInteger connected = new AtomicInteger();

        assertMemoryLeak(() -> {
            TestClock clock = new TestClock();
            IODispatcherConfiguration ioDispatcherConfig = new DefaultIODispatcherConfiguration() {
                @Override
                public MillisecondClock getClock() {
                    return clock;
                }

                @Override
                public long getHeartbeatInterval() {
                    return heartbeatInterval;
                }
            };
            try (IODispatcher<TestContext> dispatcher = IODispatchers.create(
                    ioDispatcherConfig,
                    (fd, d) -> {
                        connected.incrementAndGet();
                        return new TestContext(fd, d, heartbeatInterval);
                    }
            )) {
                SuspendEvent suspendEvent = SuspendEventFactory.newInstance(ioDispatcherConfig);
                IORequestProcessor<TestContext> processor = new SuspendingTestProcessor(clock, suspendEvent);
                long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);

                long fd = Net.socketTcp(true);
                Net.configureNonBlocking(fd);

                long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                try {
                    Unsafe.getUnsafe().putByte(buf, (byte) '.');

                    Net.connect(fd, sockAddr);
                    while (connected.get() != 1) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(processor);
                    }

                    // Write to socket to generate a socket read.
                    Assert.assertEquals(1, Net.send(fd, buf, 1));

                    // Let the dispatcher spin and verify that heartbeats are sent.
                    AtomicInteger tick = new AtomicInteger();
                    for (; tick.get() < tickCount; tick.incrementAndGet()) {
                        clock.setCurrent(tick.get());
                        dispatcher.run(0);
                        dispatcher.drainIOQueue(processor);
                    }

                    // Trigger the event and wait until the dispatcher handles it.
                    suspendEvent.trigger();
                    TestUtils.assertEventually(() -> {
                        clock.setCurrent(tick.incrementAndGet());
                        dispatcher.run(0);
                        dispatcher.drainIOQueue(processor);
                        Assert.assertTrue(suspendEvent.isClosedByAtLeastOneSide());
                    }, 10);
                } finally {
                    Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                    Net.freeSockAddr(sockAddr);
                    Misc.free(suspendEvent);
                    Net.close(fd);
                }
            }
        });
    }

    @Test
    public void testSuspendEventDoesNotPreventIdleDisconnects() throws Exception {
        LOG.info().$("started testSuspendEventDoesNotPreventIdleDisconnects").$();

        final long heartbeatInterval = 5;
        final long heartbeatToIdleRatio = 10;
        // the extra ticks are required to detect idle connection and close it
        final long tickCount = heartbeatToIdleRatio * heartbeatInterval + 3;
        AtomicInteger connected = new AtomicInteger();

        assertMemoryLeak(() -> {
            TestClock clock = new TestClock();
            IODispatcherConfiguration ioDispatcherConfig = new DefaultIODispatcherConfiguration() {
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
            };
            try (IODispatcher<TestContext> dispatcher = IODispatchers.create(
                    ioDispatcherConfig,
                    (fd, d) -> {
                        connected.incrementAndGet();
                        return new TestContext(fd, d, heartbeatInterval);
                    }
            )) {
                SuspendEvent suspendEvent = SuspendEventFactory.newInstance(ioDispatcherConfig);
                IORequestProcessor<TestContext> processor = new SuspendingTestProcessor(clock, suspendEvent);
                long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);

                long fd = Net.socketTcp(true);
                Net.configureNonBlocking(fd);

                long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                try {
                    Unsafe.getUnsafe().putByte(buf, (byte) '.');

                    Net.connect(fd, sockAddr);
                    while (connected.get() != 1) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(processor);
                    }

                    // Write to socket to generate a socket read.
                    Assert.assertEquals(1, Net.send(fd, buf, 1));

                    Os.sleep(10); // make sure the read detected on tick == 0
                    for (int i = 0; i < tickCount; i++) {
                        clock.setCurrent(i);
                        dispatcher.run(0);
                        dispatcher.drainIOQueue(processor);
                    }

                    TestUtils.assertEventually(() -> {
                        // Verify that the connection is closed on idle timeout.
                        Assert.assertTrue(NetworkFacadeImpl.INSTANCE.testConnection(fd, buf, 1));
                        // Verify that the event is closed along with the context.
                        Assert.assertTrue(suspendEvent.isClosedByAtLeastOneSide());
                    }, 10);
                } finally {
                    Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                    Net.freeSockAddr(sockAddr);
                    Misc.free(suspendEvent);
                    Net.close(fd);
                }
            }
        });
    }

    private static class SuspendingTestProcessor implements IORequestProcessor<TestContext> {
        final TestClock clock;
        final SuspendEvent suspendEvent;
        boolean alreadySuspended;

        public SuspendingTestProcessor(TestClock clock, SuspendEvent suspendEvent) {
            this.clock = clock;
            this.suspendEvent = suspendEvent;
        }

        @Override
        public boolean onRequest(int operation, TestContext context, IODispatcher<TestContext> dispatcher) {
            context.checkInvariant(operation, clock.getTicks());
            if (operation != IOOperation.HEARTBEAT && !alreadySuspended) {
                context.suspendEvent = suspendEvent;
                alreadySuspended = true;
            }
            context.getDispatcher().registerChannel(context, operation);
            return true;
        }
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
        private final IODispatcher<TestContext> dispatcher;
        private final long heartbeatInterval;
        boolean isPreviousEventHeartbeat = true;
        long previousHeartbeatTs;
        long previousReadTs;
        SuspendEvent suspendEvent;

        public TestContext(long fd, IODispatcher<TestContext> dispatcher, long heartbeatInterval) {
            super(PlainSocketFactory.INSTANCE, NetworkFacadeImpl.INSTANCE, LOG);
            socket.of(fd);
            this.dispatcher = dispatcher;
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
        public void clearSuspendEvent() {
            suspendEvent = Misc.free(suspendEvent);
        }

        @Override
        public void close() {
            Unsafe.free(buffer, 4, MemoryTag.NATIVE_DEFAULT);
            super.close();
        }

        public IODispatcher<TestContext> getDispatcher() {
            return dispatcher;
        }

        @Override
        public SuspendEvent getSuspendEvent() {
            return suspendEvent;
        }

        @Override
        public boolean invalid() {
            return false;
        }
    }

    private static class TestProcessor implements IORequestProcessor<TestContext> {
        final TestClock clock;

        public TestProcessor(TestClock clock) {
            this.clock = clock;
        }

        @Override
        public boolean onRequest(int operation, TestContext context, IODispatcher<TestContext> dispatcher) {
            context.checkInvariant(operation, clock.getTicks());
            context.getDispatcher().registerChannel(context, operation);
            return true;
        }
    }
}
