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
import io.questdb.network.ConnectionSteering;
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
import io.questdb.std.CpuTopology;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.network.SysfsFixture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    public TemporaryFolder temp = new TemporaryFolder();
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
    public void testFailedReopenRetries() throws Exception {
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final AtomicBoolean isBindFailing = new AtomicBoolean();
            final AtomicInteger failedBinds = new AtomicInteger();
            final NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public boolean bindTcp(long fd, int address, int port) {
                    if (isBindFailing.get()) {
                        failedBinds.incrementAndGet();
                        return false;
                    }
                    return super.bindTcp(fd, address, port);
                }
            };
            final IODispatcherConfiguration configuration = new DefaultIODispatcherConfiguration() {
                @Override
                public int getBindPort() {
                    return port;
                }

                @Override
                public int getLimit() {
                    return 1;
                }

                @Override
                public NetworkFacade getNetworkFacade() {
                    return nf;
                }

                @Override
                public long getQueueTimeout() {
                    return 1;
                }
            };
            final ObjList<IODispatcher<TestContext>> dispatchers = new ObjList<>();
            IODispatchers.createGroups(configuration, TestContext::new, 1, dispatchers);
            try {
                final IODispatcher<TestContext> dispatcher = dispatchers.getQuick(0);
                // one connection reaches the limit, and the listener closes after the queue timeout
                long[] fds = connect(port, 1);
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (dispatcher.getConnectionCount() < 1 || dispatcher.isListening()) {
                    Assert.assertTrue("the listener did not close", System.nanoTime() < deadline);
                    runOnce(dispatchers);
                    Os.sleep(2);
                    runOnce(dispatchers);
                }

                // the connection drops and the reopen fails, without an exception escaping
                isBindFailing.set(true);
                close(fds);
                while (dispatcher.getConnectionCount() > 0 || failedBinds.get() == 0) {
                    Assert.assertTrue("the reopen did not fail", System.nanoTime() < deadline);
                    runOnce(dispatchers);
                }
                Assert.assertFalse(dispatcher.isListening());

                // the dispatcher loop retries without another disconnect
                isBindFailing.set(false);
                while (!dispatcher.isListening()) {
                    Assert.assertTrue("the listener did not reopen", System.nanoTime() < deadline);
                    runOnce(dispatchers);
                    Os.sleep(10);
                }
                fds = connect(port, 1);
                try {
                    while (dispatcher.getConnectionCount() < 1) {
                        Assert.assertTrue("the reopened listener did not accept", System.nanoTime() < deadline);
                        runOnce(dispatchers);
                    }
                } finally {
                    close(fds);
                }
                while (dispatcher.getConnectionCount() > 0) {
                    Assert.assertTrue("the connection did not drop", System.nanoTime() < deadline);
                    runOnce(dispatchers);
                }
            } finally {
                free(dispatchers);
            }
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

    @Test
    public void testSteeringRedirectsFromAGroupAtItsLimit() throws Exception {
        assumeSteeringWorks();
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final ObjList<IODispatcher<TestContext>> dispatchers = new ObjList<>();
            IODispatchers.createGroups(steeringConfiguration(port, 1), TestContext::new, 2, new int[][]{{0}, {1}}, dispatchers);
            final ObjList<long[]> clients = new ObjList<>();
            try {
                // CPU 0's first connection fills group 0
                clients.add(connectFromCpu(port, 0, 1));
                awaitConnectionCounts(dispatchers, 1, 0);
                // once group 0 has stayed at its limit for the redirect delay, CPU 0's next
                // connection goes to group 1
                final long redirectAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1_200);
                while (System.nanoTime() < redirectAt) {
                    runOnce(dispatchers);
                    Os.sleep(10);
                }
                clients.add(connectFromCpu(port, 0, 1));
                awaitConnectionCounts(dispatchers, 1, 1);
            } finally {
                for (int i = 0, n = clients.size(); i < n; i++) {
                    close(clients.getQuick(i));
                }
                free(dispatchers);
            }
        });
    }

    @Test
    public void testSteeringSendsEachCpuToItsGroup() throws Exception {
        assumeSteeringWorks();
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final ObjList<IODispatcher<TestContext>> dispatchers = new ObjList<>();
            IODispatchers.createGroups(steeringConfiguration(port, 1000), TestContext::new, 2, new int[][]{{0}, {1}}, dispatchers);
            try {
                // with the kernel's hash, 20 connections all land on one listener with probability 2^-19
                final long[] fromCpu0 = connectFromCpu(port, 0, 20);
                try {
                    awaitConnectionCounts(dispatchers, 20, 0);
                    final long[] fromCpu1 = connectFromCpu(port, 1, 20);
                    try {
                        awaitConnectionCounts(dispatchers, 20, 20);
                    } finally {
                        close(fromCpu1);
                    }
                } finally {
                    close(fromCpu0);
                }
                awaitConnectionCounts(dispatchers, 0, 0);
            } finally {
                free(dispatchers);
            }
        });
    }

    private static void assertPortFree(int port) {
        IODispatchers.checkPortFree(NetworkFacadeImpl.INSTANCE, 0, port, "test");
    }

    private static void assumeSteeringWorks() {
        Assume.assumeTrue("connection steering runs on Linux only", Os.isLinux());
        Assume.assumeTrue("connection steering needs Linux 4.6", ConnectionSteering.isKernelAtLeast(System.getProperty("os.version"), 4, 6));
        final int[] pinned = new int[2];
        final Thread[] threads = new Thread[2];
        for (int cpu = 0; cpu < 2; cpu++) {
            final int c = cpu;
            threads[cpu] = new Thread(() -> pinned[c] = Os.setCurrentThreadCpus(new int[]{c}));
            threads[cpu].start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Assume.assumeTrue("the test pins clients to CPUs 0 and 1", pinned[0] == 0 && pinned[1] == 0);
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

    // connects count clients from a thread pinned to cpu, whose SYNs loopback processes on that CPU
    private static long[] connectFromCpu(int port, int cpu, int count) {
        final long[][] result = new long[1][];
        final Throwable[] failure = new Throwable[1];
        final Thread thread = new Thread(() -> {
            try {
                Assert.assertEquals(0, Os.setCurrentThreadCpus(new int[]{cpu}));
                result[0] = connect(port, count);
            } catch (Throwable th) {
                failure[0] = th;
            }
        });
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (failure[0] != null) {
            throw new AssertionError(failure[0]);
        }
        return result[0];
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

    // steering with groups on CPUs 0 and 1, over a fake receive layout whose interface interrupts
    // both CPUs once, which passes the layout check
    private IODispatcherConfiguration steeringConfiguration(int port, int limit) {
        final SysfsFixture fixture = new SysfsFixture(temp.getRoot().toPath());
        fixture.cpus(new int[]{0, 0}, 2);
        fixture.nic("eth0", 2).irq("eth0", 100, "0").irq("eth0", 101, "1");
        final CpuTopology topology = new CpuTopology(fixture.getProcRoot(), fixture.getSysRoot());
        return new DefaultIODispatcherConfiguration() {
            @Override
            public int getBindPort() {
                return port;
            }

            @Override
            public CpuTopology getConnectionSteeringTopology() {
                return topology;
            }

            @Override
            public int getLimit() {
                return limit;
            }

            @Override
            public boolean isConnectionSteeringEnabled() {
                return true;
            }
        };
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
