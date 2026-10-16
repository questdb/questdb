/*+*****************************************************************************
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
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Dispatchers of several worker groups sharing one port through SO_REUSEPORT. The kernel spreads
 * connections over such listeners on Linux only, so the tests that rely on the spread run there.
 */
public class IODispatcherGroupsTest {
    private static final Log LOG = LogFactory.getLog(IODispatcherGroupsTest.class);
    // with this many connections, all of them land on one of two listeners with probability 2^-39
    private static final int SPREAD_CONNECTIONS = 40;
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(2, TimeUnit.MINUTES)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testConnectionLimitAppliesPerGroup() throws Exception {
        Assume.assumeTrue("SO_REUSEPORT spreads connections on Linux only", Os.isLinux());
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final int limit = 2;
            final ObjList<IODispatcher<TestContext>> dispatchers = new ObjList<>();
            IODispatchers.createGroups(configuration(port, limit, NetworkFacadeImpl.INSTANCE), TestContext::new, 2, dispatchers);
            final long sockAddr = Net.sockaddr("127.0.0.1", port);
            try {
                for (int round = 0; round < 2; round++) {
                    // The listen backlog equals the limit, so a full group's listener soon stops
                    // completing handshakes. Non-blocking connects keep the test from waiting on them.
                    final long[] fds = new long[SPREAD_CONNECTIONS];
                    int clientCount = 0;
                    try {
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                        while (dispatchers.getQuick(0).getConnectionCount() < limit || dispatchers.getQuick(1).getConnectionCount() < limit) {
                            Assert.assertTrue(
                                    "groups did not both reach the limit [round=" + round
                                            + ", group0=" + dispatchers.getQuick(0).getConnectionCount()
                                            + ", group1=" + dispatchers.getQuick(1).getConnectionCount()
                                            + ", clients=" + clientCount + ']',
                                    System.nanoTime() < deadline
                            );
                            if (clientCount < fds.length) {
                                fds[clientCount] = Net.socketTcp(false);
                                Net.connect(fds[clientCount], sockAddr);
                                clientCount++;
                            }
                            for (int i = 0; i < 10; i++) {
                                runOnce(dispatchers);
                            }
                        }
                        // neither group goes past its own limit
                        for (int i = 0; i < 1000; i++) {
                            runOnce(dispatchers);
                            Assert.assertEquals(limit, dispatchers.getQuick(0).getConnectionCount());
                            Assert.assertEquals(limit, dispatchers.getQuick(1).getConnectionCount());
                        }
                        Assert.assertFalse(dispatchers.getQuick(0).isListening());
                        Assert.assertFalse(dispatchers.getQuick(1).isListening());
                    } finally {
                        for (int i = 0; i < clientCount; i++) {
                            Net.close(fds[i]);
                        }
                    }
                    // the disconnects register both listeners again; they accept and drop the
                    // closed connections waiting in their backlogs, and rejoin the port's group
                    awaitConnectionCounts(dispatchers, 0, 0);
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                    while (System.nanoTime() < deadline) {
                        runOnce(dispatchers);
                    }
                    Assert.assertEquals(0, dispatchers.getQuick(0).getConnectionCount());
                    Assert.assertEquals(0, dispatchers.getQuick(1).getConnectionCount());
                    Assert.assertTrue(dispatchers.getQuick(0).isListening());
                    Assert.assertTrue(dispatchers.getQuick(1).isListening());
                }
            } finally {
                Net.freeSockAddr(sockAddr);
                free(dispatchers);
            }
        });
    }

    @Test
    public void testConnectionsSpreadOverGroups() throws Exception {
        Assume.assumeTrue("SO_REUSEPORT spreads connections on Linux only", Os.isLinux());
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final ObjList<IODispatcher<TestContext>> dispatchers = new ObjList<>();
            IODispatchers.createGroups(configuration(port, 1000, NetworkFacadeImpl.INSTANCE), TestContext::new, 2, dispatchers);
            try {
                Assert.assertEquals(2, dispatchers.size());
                Assert.assertEquals(port, dispatchers.getQuick(0).getPort());
                Assert.assertEquals(port, dispatchers.getQuick(1).getPort());
                final long[] fds = connect(port, SPREAD_CONNECTIONS);
                try {
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (dispatchers.getQuick(0).getConnectionCount() + dispatchers.getQuick(1).getConnectionCount() < SPREAD_CONNECTIONS) {
                        Assert.assertTrue("not all connections accepted", System.nanoTime() < deadline);
                        runOnce(dispatchers);
                    }
                    final int first = dispatchers.getQuick(0).getConnectionCount();
                    final int second = dispatchers.getQuick(1).getConnectionCount();
                    LOG.info().$("connections per group [first=").$(first).$(", second=").$(second).I$();
                    Assert.assertTrue("group 0 accepted no connection", first > 0);
                    Assert.assertTrue("group 1 accepted no connection", second > 0);
                } finally {
                    close(fds);
                }
                awaitConnectionCounts(dispatchers, 0, 0);
            } finally {
                free(dispatchers);
            }
        });
    }

    @Test
    public void testFailedGroupFreesEarlierGroups() throws Exception {
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final AtomicInteger binds = new AtomicInteger();
            // the port check binds first, then group 0, then group 1
            final NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public boolean bindTcp(long fd, int address, int port) {
                    return binds.incrementAndGet() != 3 && super.bindTcp(fd, address, port);
                }
            };
            final ObjList<IODispatcher<TestContext>> dispatchers = new ObjList<>();
            try {
                IODispatchers.createGroups(configuration(port, 1000, nf), TestContext::new, 2, dispatchers);
                Assert.fail();
            } catch (NetworkError e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not bind socket");
            }
            Assert.assertEquals(3, binds.get());
            Assert.assertEquals(0, dispatchers.size());
            // group 0's listener is closed: a plain listener can bind the port again
            assertPortFree(port);
        });
    }

    @Test
    public void testOneGroupDoesNotReusePort() throws Exception {
        // Windows calls setReusePort() on every listener, to release the port sooner
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final AtomicInteger reusePortCalls = new AtomicInteger();
            final NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public int setReusePort(long fd) {
                    reusePortCalls.incrementAndGet();
                    return super.setReusePort(fd);
                }
            };
            final ObjList<IODispatcher<TestContext>> dispatchers = new ObjList<>();
            IODispatchers.createGroups(configuration(port, 1000, nf), TestContext::new, 1, dispatchers);
            try {
                Assert.assertEquals(1, dispatchers.size());
                Assert.assertEquals(0, reusePortCalls.get());
            } finally {
                free(dispatchers);
            }
            IODispatchers.createGroups(configuration(port, 1000, nf), TestContext::new, 2, dispatchers);
            try {
                Assert.assertEquals(2, dispatchers.size());
                Assert.assertEquals(2, reusePortCalls.get());
            } finally {
                free(dispatchers);
            }
        });
    }

    @Test
    public void testPortHeldByAnotherGroupedServer() throws Exception {
        Assume.assumeFalse("Windows has no SO_REUSEPORT", Os.isWindows());
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final ObjList<IODispatcher<TestContext>> first = new ObjList<>();
            IODispatchers.createGroups(configuration(port, 1000, NetworkFacadeImpl.INSTANCE), TestContext::new, 2, first);
            try {
                final ObjList<IODispatcher<TestContext>> second = new ObjList<>();
                try {
                    IODispatchers.createGroups(configuration(port, 1000, NetworkFacadeImpl.INSTANCE), TestContext::new, 2, second);
                    Assert.fail();
                } catch (NetworkError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not bind socket");
                    TestUtils.assertContains(e.getFlyweightMessage(), ":" + port);
                }
                Assert.assertEquals(0, second.size());
                // a single-listener server fails the same way
                try {
                    IODispatchers.createGroups(configuration(port, 1000, NetworkFacadeImpl.INSTANCE), TestContext::new, 1, second);
                    Assert.fail();
                } catch (NetworkError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not bind socket");
                }
            } finally {
                free(first);
            }
        });
    }

    @Test
    public void testPortHeldByPlainListener() throws Exception {
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final long listener = Net.socketTcp(true);
            Assert.assertTrue(Net.bindTcp(listener, 0, port));
            Net.listen(listener, 16);
            try {
                final ObjList<IODispatcher<TestContext>> dispatchers = new ObjList<>();
                try {
                    IODispatchers.createGroups(configuration(port, 1000, NetworkFacadeImpl.INSTANCE), TestContext::new, 2, dispatchers);
                    Assert.fail();
                } catch (NetworkError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not bind socket");
                }
                Assert.assertEquals(0, dispatchers.size());
            } finally {
                Net.close(listener);
            }
        });
    }

    private static void assertPortFree(int port) {
        IODispatchers.checkPortFree(NetworkFacadeImpl.INSTANCE, 0, port, "test");
    }

    private static void awaitConnectionCounts(ObjList<IODispatcher<TestContext>> dispatchers, int first, int second) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (dispatchers.getQuick(0).getConnectionCount() != first || dispatchers.getQuick(1).getConnectionCount() != second) {
            Assert.assertTrue(
                    "connection counts did not reach [" + first + ", " + second + "]: ["
                            + dispatchers.getQuick(0).getConnectionCount() + ", "
                            + dispatchers.getQuick(1).getConnectionCount() + ']',
                    System.nanoTime() < deadline
            );
            runOnce(dispatchers);
        }
    }

    private static void close(long[] fds) {
        for (long fd : fds) {
            Net.close(fd);
        }
    }

    private static IODispatcherConfiguration configuration(int port, int limit, NetworkFacade nf) {
        return new DefaultIODispatcherConfiguration() {
            @Override
            public int getBindPort() {
                return port;
            }

            @Override
            public int getLimit() {
                return limit;
            }

            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }
        };
    }

    private static long[] connect(int port, int count) {
        final long[] fds = new long[count];
        final long sockAddr = Net.sockaddr("127.0.0.1", port);
        try {
            for (int i = 0; i < count; i++) {
                fds[i] = Net.socketTcp(true);
                TestUtils.assertConnect(fds[i], sockAddr);
            }
        } finally {
            Net.freeSockAddr(sockAddr);
        }
        return fds;
    }

    private static void free(ObjList<IODispatcher<TestContext>> dispatchers) {
        for (int i = 0, n = dispatchers.size(); i < n; i++) {
            Misc.free(dispatchers.getQuick(i));
        }
        dispatchers.clear();
    }

    private static int pickFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void runOnce(ObjList<IODispatcher<TestContext>> dispatchers) {
        for (int i = 0, n = dispatchers.size(); i < n; i++) {
            final IODispatcher<TestContext> dispatcher = dispatchers.getQuick(i);
            dispatcher.run();
            dispatcher.processIOQueue(TestContext.PROCESSOR);
        }
    }

    private static class TestContext extends IOContext<TestContext> {
        // drops a connection once its peer closes it, and keeps reading otherwise
        static final IORequestProcessor<TestContext> PROCESSOR = (operation, context, dispatcher) -> {
            if (operation == IOOperation.READ) {
                // a negative result is a disconnect, 0 means no data yet
                if (Net.recv(context.getFd(), context.buffer, 1) < 0) {
                    dispatcher.disconnect(context, IODispatcher.DISCONNECT_REASON_TEST);
                } else {
                    dispatcher.registerChannel(context, IOOperation.READ);
                }
            } else {
                dispatcher.registerChannel(context, IOOperation.READ);
            }
            return true;
        };
        private final long buffer = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);

        TestContext(long fd) {
            super(PlainSocketFactory.INSTANCE, NetworkFacadeImpl.INSTANCE, LOG);
            socket.of(fd);
        }

        @Override
        public void close() {
            Unsafe.free(buffer, 1, MemoryTag.NATIVE_DEFAULT);
            super.close();
        }

        @Override
        public boolean invalid() {
            return false;
        }
    }
}
