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

import io.questdb.Metrics;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.network.IODispatchers;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.http.HttpServerConfigurationBuilder;
import io.questdb.test.cutlass.pgwire.Port0PGConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

/**
 * HTTP, PG wire and ILP/TCP servers on a network pool of two worker groups: each server creates
 * a dispatcher per group, all listening on one port through SO_REUSEPORT. The kernel spreads
 * connections over such listeners on Linux only, so the tests that rely on the spread run there.
 */
public class ServerGroupsTest extends AbstractCairoTest {
    // with this many connections, all of them land on one of two listeners with probability 2^-39
    private static final int CONNECTIONS = 40;
    // ILP/TCP runs its IO jobs 0 to 3 on the first two workers of each group, workers 0, 1, 4 and
    // 5, so jobs 2 and 3, which serve group 1, run on workers whose index differs from theirs
    private static final int ILP_WORKERS = 8;
    private static final Log LOG = LogFactory.getLog(ServerGroupsTest.class);
    private static final int OWN_GROUP_CONNECTIONS = 8;
    private static final int OWN_GROUP_PG_CONNECTIONS = 4;
    private static final int PG_CONNECTIONS = 24;
    private static final int WORKERS = 4;
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(3, TimeUnit.MINUTES)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testHttpFailedGroupFreesEarlierGroups() throws Exception {
        assumeSpreading();
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
            final DefaultHttpServerConfiguration httpConfig = new HttpServerConfigurationBuilder()
                    .withPort(port)
                    .withNetwork(nf)
                    .build(configuration);
            final TestWorkerPool pool = new TestWorkerPool(groupedPool(WorkerPoolMode.LEGACY));
            try {
                new HttpServer(httpConfig, pool, PlainSocketFactory.INSTANCE).close();
                Assert.fail();
            } catch (NetworkError e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not bind socket");
            } finally {
                pool.halt();
            }
            Assert.assertEquals(3, binds.get());
            IODispatchers.checkPortFree(NetworkFacadeImpl.INSTANCE, 0, port, "test");
        });
    }

    @Test
    public void testHttpFibers() throws Exception {
        assertHttp(WorkerPoolMode.FIBER_HOST, true);
    }

    @Test
    public void testHttpFibersServedByOwnGroup() throws Exception {
        assertHttpServedByOwnGroup(WorkerPoolMode.FIBER_HOST, true);
    }

    @Test
    public void testHttpLegacy() throws Exception {
        assertHttp(WorkerPoolMode.LEGACY, false);
    }

    @Test
    public void testHttpLegacyServedByOwnGroup() throws Exception {
        assertHttpServedByOwnGroup(WorkerPoolMode.LEGACY, false);
    }

    @Test
    public void testHttpOnPort0IsNotSplit() throws Exception {
        assertMemoryLeak(() -> {
            final DefaultHttpServerConfiguration httpConfig = new HttpServerConfigurationBuilder()
                    .withPort(0)
                    .withNetwork(NetworkFacadeImpl.INSTANCE)
                    .build(configuration);
            final TestWorkerPool pool = new TestWorkerPool(groupedPool(WorkerPoolMode.LEGACY));
            try (HttpServer server = new HttpServer(httpConfig, pool, PlainSocketFactory.INSTANCE)) {
                Assert.assertEquals(1, server.getGroupCount());
                Assert.assertNotEquals(0, server.getPort());
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testHttpOnPort0WithFibersLaunchesOnGroupRuntimes() throws Exception {
        // a server on port 0 has a single dispatcher, polled by every worker, and each worker
        // launches connection fibers on its own group's runtime
        assertMemoryLeak(() -> {
            final DefaultHttpServerConfiguration httpConfig = new HttpServerConfigurationBuilder()
                    .withPort(0)
                    .withNetwork(NetworkFacadeImpl.INSTANCE)
                    .withFiberEnabled(true)
                    .build(configuration);
            final TestWorkerPool pool = new TestWorkerPool(groupedPool(WorkerPoolMode.FIBER_HOST));
            try (HttpServer server = new HttpServer(httpConfig, pool, PlainSocketFactory.INSTANCE)) {
                Assert.assertEquals(1, server.getGroupCount());
                Assert.assertEquals(2, server.getSelectorPoolCountForTesting());
                pool.start(LOG);
                try {
                    final long[] fds = connect(server.getPort(), CONNECTIONS);
                    try {
                        for (long fd : fds) {
                            assertHttpResponds(fd);
                        }
                    } finally {
                        close(fds);
                    }
                    final long launched = pool.getFiberRuntime(0).getLaunchCount(LaunchResult.LAUNCHED)
                            + pool.getFiberRuntime(1).getLaunchCount(LaunchResult.LAUNCHED);
                    Assert.assertTrue(launched >= CONNECTIONS);
                } finally {
                    pool.halt();
                }
            }
        });
    }

    @Test
    public void testIlpTcp() throws Exception {
        assumeSpreading();
        assertIlpTcp(
                NetworkFacadeImpl.INSTANCE,
                receiver -> awaitSpread(receiver::getGroupConnectionCount, CONNECTIONS)
        );
    }

    @Test
    public void testIlpTcpOnSecondGroupOnly() throws Exception {
        // Group 0's listener binds to an ephemeral port, so every connection lands on group 1's
        // listener, on every platform. The lines land only if some IO job serves group 1's
        // dispatcher.
        final AtomicInteger binds = new AtomicInteger();
        final NetworkFacade nf = new NetworkFacadeImpl() {
            @Override
            public boolean bindTcp(long fd, int address, int port) {
                // the port check binds first, then group 0, then group 1
                return super.bindTcp(fd, address, binds.incrementAndGet() == 2 ? 0 : port);
            }
        };
        assertIlpTcp(
                nf,
                receiver -> awaitGroupConnections(receiver::getGroupConnectionCount, 0, CONNECTIONS)
        );
        Assert.assertEquals(3, binds.get());
    }

    @Test
    public void testIlpTcpServedByOwnGroup() throws Exception {
        assertMemoryLeak(() -> {
            final TestWorkerPool networkPool = new TestWorkerPool(groupedPool(WorkerPoolMode.LEGACY, ILP_WORKERS));
            final GroupCheckingFacade nf = new GroupCheckingFacade(networkPool);
            assertIlpTcp(
                    networkPool,
                    nf,
                    port -> nf.connectToBothGroups(port, CONNECTIONS),
                    receiver -> awaitGroupConnections(receiver::getGroupConnectionCount, CONNECTIONS / 2, CONNECTIONS / 2)
            );
            nf.assertServedByOwnGroup(CONNECTIONS / 2);
        });
    }

    @Test
    public void testPgFibers() throws Exception {
        assertPg(WorkerPoolMode.FIBER_HOST, true);
    }

    @Test
    public void testPgFibersServedByOwnGroup() throws Exception {
        assertPgServedByOwnGroup(WorkerPoolMode.FIBER_HOST, true);
    }

    @Test
    public void testPgLegacy() throws Exception {
        assertPg(WorkerPoolMode.LEGACY, false);
    }

    @Test
    public void testPgLegacyServedByOwnGroup() throws Exception {
        assertPgServedByOwnGroup(WorkerPoolMode.LEGACY, false);
    }

    private static void assertHttpResponds(long fd) {
        send(fd, "GET /nowhere HTTP/1.1\r\nHost: localhost\r\n\r\n");
        final int capacity = 4096;
        final long buf = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        try {
            final int n = Net.recv(fd, buf, capacity);
            Assert.assertTrue("no response [rc=" + n + ']', n > 8);
            final byte[] bytes = new byte[8];
            for (int i = 0; i < 8; i++) {
                bytes[i] = Unsafe.getByte(buf + i);
            }
            Assert.assertEquals("HTTP/1.1", new String(bytes));
        } finally {
            Unsafe.free(buf, capacity, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assumeSpreading() {
        Assume.assumeTrue("SO_REUSEPORT spreads connections on Linux only", Os.isLinux());
    }

    // waits until group 0 holds `first` connections and group 1 holds `second`
    private static void awaitGroupConnections(IntUnaryOperator groupConnectionCount, int first, int second) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (groupConnectionCount.applyAsInt(0) != first || groupConnectionCount.applyAsInt(1) != second) {
            Assert.assertTrue(
                    "connections not accepted [group0=" + groupConnectionCount.applyAsInt(0)
                            + ", group1=" + groupConnectionCount.applyAsInt(1)
                            + ", expected0=" + first + ", expected1=" + second + ']',
                    System.nanoTime() < deadline
            );
            Os.sleep(1);
        }
    }

    // waits until the two groups hold `total` connections between them, and checks each holds some
    private static void awaitSpread(IntUnaryOperator groupConnectionCount, int total) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (groupConnectionCount.applyAsInt(0) + groupConnectionCount.applyAsInt(1) != total) {
            Assert.assertTrue(
                    "connections not accepted [group0=" + groupConnectionCount.applyAsInt(0)
                            + ", group1=" + groupConnectionCount.applyAsInt(1) + ", expected=" + total + ']',
                    System.nanoTime() < deadline
            );
            Os.sleep(1);
        }
        final int first = groupConnectionCount.applyAsInt(0);
        final int second = groupConnectionCount.applyAsInt(1);
        LOG.info().$("connections per group [group0=").$(first).$(", group1=").$(second).I$();
        Assert.assertTrue("group 0 holds no connection", first > 0);
        Assert.assertTrue("group 1 holds no connection", second > 0);
    }

    private static void close(long[] fds) {
        for (long fd : fds) {
            Net.close(fd);
        }
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

    private static WorkerPoolConfiguration groupedPool(WorkerPoolMode mode) {
        return groupedPool(mode, WORKERS);
    }

    private static WorkerPoolConfiguration groupedPool(WorkerPoolMode mode, int workerCount) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getGroupCount() {
                return 2;
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public String getPoolName() {
                return "grouped";
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return mode;
            }
        };
    }

    private static int pickFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void send(long fd, String text) {
        final int len = text.length();
        final long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Utf8s.strCpyAscii(text, len, buf);
            int sent = 0;
            while (sent < len) {
                final int rc = Net.send(fd, buf + sent, len - sent);
                Assert.assertTrue("send failed [rc=" + rc + ']', rc >= 0);
                sent += rc;
            }
        } finally {
            Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertHttp(WorkerPoolMode mode, boolean isFiberEnabled) throws Exception {
        assumeSpreading();
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final DefaultHttpServerConfiguration httpConfig = new HttpServerConfigurationBuilder()
                    .withPort(port)
                    .withNetwork(NetworkFacadeImpl.INSTANCE)
                    .withFiberEnabled(isFiberEnabled)
                    .build(configuration);
            final TestWorkerPool pool = new TestWorkerPool(groupedPool(mode));
            try (HttpServer server = new HttpServer(httpConfig, pool, PlainSocketFactory.INSTANCE)) {
                Assert.assertEquals(2, server.getGroupCount());
                Assert.assertEquals(port, server.getPort());
                pool.start(LOG);
                try {
                    final long[] fds = connect(port, CONNECTIONS);
                    try {
                        awaitSpread(server::getGroupConnectionCount, CONNECTIONS);
                        for (long fd : fds) {
                            assertHttpResponds(fd);
                        }
                        if (isFiberEnabled) {
                            // each group's connections ran on its own runtime, and its fiber steps
                            // returned their selectors to its own pool
                            Assert.assertEquals(2, server.getSelectorPoolCountForTesting());
                            for (int g = 0; g < 2; g++) {
                                final int group = g;
                                Assert.assertTrue(pool.getFiberRuntime(group).getLaunchCount(LaunchResult.LAUNCHED) > 0);
                                TestUtils.assertEventually(
                                        () -> Assert.assertTrue(server.getRecycledSelectorCountForTesting(group) > 0),
                                        10
                                );
                            }
                        }
                    } finally {
                        close(fds);
                    }
                    // another server can't join the port
                    final TestWorkerPool otherPool = new TestWorkerPool(groupedPool(mode));
                    try {
                        new HttpServer(httpConfig, otherPool, PlainSocketFactory.INSTANCE).close();
                        Assert.fail();
                    } catch (NetworkError e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not bind socket");
                    } finally {
                        otherPool.halt();
                    }
                } finally {
                    pool.halt();
                }
            }
        });
    }

    private void assertHttpServedByOwnGroup(WorkerPoolMode mode, boolean isFiberEnabled) throws Exception {
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final TestWorkerPool pool = new TestWorkerPool(groupedPool(mode));
            try {
                final GroupCheckingFacade nf = new GroupCheckingFacade(pool);
                final DefaultHttpServerConfiguration httpConfig = new HttpServerConfigurationBuilder()
                        .withPort(port)
                        .withNetwork(nf)
                        .withFiberEnabled(isFiberEnabled)
                        .build(configuration);
                try (HttpServer server = new HttpServer(httpConfig, pool, PlainSocketFactory.INSTANCE)) {
                    Assert.assertEquals(2, server.getGroupCount());
                    pool.start(LOG);
                    try {
                        final long[] fds = nf.connectToBothGroups(port, OWN_GROUP_CONNECTIONS);
                        try {
                            awaitGroupConnections(server::getGroupConnectionCount, OWN_GROUP_CONNECTIONS / 2, OWN_GROUP_CONNECTIONS / 2);
                            for (long fd : fds) {
                                assertHttpResponds(fd);
                            }
                        } finally {
                            close(fds);
                        }
                    } finally {
                        // the server's jobs run on the pool, so the pool halts before the server closes
                        pool.halt();
                    }
                }
                nf.assertServedByOwnGroup(OWN_GROUP_CONNECTIONS / 2);
            } finally {
                pool.halt();
            }
        });
    }

    private void assertIlpTcp(NetworkFacade nf, Consumer<LineTcpReceiver> awaitConnections) throws Exception {
        assertMemoryLeak(() -> assertIlpTcp(
                new TestWorkerPool(groupedPool(WorkerPoolMode.LEGACY, ILP_WORKERS)),
                nf,
                port -> connect(port, CONNECTIONS),
                awaitConnections
        ));
    }

    // Sends one line per connection to an ILP/TCP receiver on networkPool, a pool of two groups,
    // and checks that every line lands. connect opens CONNECTIONS connections given the receiver's
    // port, and awaitConnections waits until the receiver holds all of them.
    private void assertIlpTcp(
            TestWorkerPool networkPool,
            NetworkFacade nf,
            IntFunction<long[]> connect,
            Consumer<LineTcpReceiver> awaitConnections
    ) throws Exception {
        final TestWorkerPool writePool = new TestWorkerPool(1);
        try {
            final int port = pickFreePort();
            final DefaultLineTcpReceiverConfiguration lineConfig = new DefaultLineTcpReceiverConfiguration(configuration) {
                @Override
                public int getBindPort() {
                    return port;
                }

                @Override
                public NetworkFacade getNetworkFacade() {
                    return nf;
                }
            };
            try (LineTcpReceiver receiver = new LineTcpReceiver(lineConfig, engine, networkPool, writePool)) {
                Assert.assertEquals(2, receiver.getGroupCount());
                networkPool.start(LOG);
                writePool.start(LOG);
                try {
                    final long[] fds = connect.apply(port);
                    try {
                        awaitConnections.accept(receiver);
                        for (int i = 0; i < CONNECTIONS; i++) {
                            send(fds[i], "groups,conn=c" + i + " v=" + i + "i\n");
                        }
                    } finally {
                        close(fds);
                    }
                    // every line lands, so every group holding connections has its dispatcher served
                    TestUtils.assertEventually(() -> {
                        Assert.assertNotNull("table not created yet", engine.getTableTokenIfExists("groups"));
                        drainWalQueue();
                        assertQuery("select count(), count_distinct(conn), sum(v) from groups")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\tcount_distinct\tsum\n" + CONNECTIONS + "\t" + CONNECTIONS + "\t" + (CONNECTIONS * (CONNECTIONS - 1) / 2) + "\n");
                    }, 60);
                } finally {
                    // the receiver's jobs run on both pools, so the pools halt before the receiver closes
                    networkPool.halt();
                    writePool.halt();
                }
            }
        } finally {
            networkPool.halt();
            writePool.halt();
        }
    }

    private void assertPg(WorkerPoolMode mode, boolean isFiberEnabled) throws Exception {
        assumeSpreading();
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final Port0PGConfiguration pgConfig = new Port0PGConfiguration() {
                @Override
                public int getBindPort() {
                    return port;
                }

                @Override
                public boolean isFiberEnabled() {
                    return isFiberEnabled;
                }
            };
            final TestWorkerPool pool = new TestWorkerPool(groupedPool(mode));
            try (
                    DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(pgConfig, configuration);
                    PGServer server = new PGServer(pgConfig, engine, pool, registry, () -> new SqlExecutionContextImpl(engine, 1))
            ) {
                Assert.assertEquals(2, server.getGroupCount());
                Assert.assertEquals(port, server.getPort());
                Assert.assertTrue(server.isListening());
                pool.start(LOG);
                try {
                    final Properties properties = new Properties();
                    properties.setProperty("user", "admin");
                    properties.setProperty("password", "quest");
                    properties.setProperty("sslmode", "disable");
                    final Connection[] connections = new Connection[PG_CONNECTIONS];
                    try {
                        for (int i = 0; i < PG_CONNECTIONS; i++) {
                            connections[i] = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + port + "/qdb", properties);
                            try (Statement statement = connections[i].createStatement();
                                 ResultSet rs = statement.executeQuery("select " + i)) {
                                Assert.assertTrue(rs.next());
                                Assert.assertEquals(i, rs.getInt(1));
                            }
                        }
                        awaitSpread(server::getGroupConnectionCount, PG_CONNECTIONS);
                        if (isFiberEnabled) {
                            // each group's connections ran on its own runtime
                            for (int g = 0; g < 2; g++) {
                                Assert.assertTrue(pool.getFiberRuntime(g).getLaunchCount(LaunchResult.LAUNCHED) > 0);
                            }
                        }
                    } finally {
                        for (Connection connection : connections) {
                            if (connection != null) {
                                connection.close();
                            }
                        }
                    }
                } finally {
                    pool.halt();
                }
            }
        });
    }

    private void assertPgServedByOwnGroup(WorkerPoolMode mode, boolean isFiberEnabled) throws Exception {
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final TestWorkerPool pool = new TestWorkerPool(groupedPool(mode));
            try {
                final GroupCheckingFacade nf = new GroupCheckingFacade(pool);
                final Port0PGConfiguration pgConfig = new Port0PGConfiguration() {
                    @Override
                    public int getBindPort() {
                        return port;
                    }

                    @Override
                    public NetworkFacade getNetworkFacade() {
                        return nf;
                    }

                    @Override
                    public boolean isFiberEnabled() {
                        return isFiberEnabled;
                    }
                };
                try (
                        DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(pgConfig, configuration);
                        PGServer server = new PGServer(pgConfig, engine, pool, registry, () -> new SqlExecutionContextImpl(engine, 1))
                ) {
                    Assert.assertEquals(2, server.getGroupCount());
                    pool.start(LOG);
                    try {
                        final Properties properties = new Properties();
                        properties.setProperty("user", "admin");
                        properties.setProperty("password", "quest");
                        properties.setProperty("sslmode", "disable");
                        final ObjList<Connection> connections = new ObjList<>();
                        try {
                            for (int i = 0; i < OWN_GROUP_PG_CONNECTIONS; i++) {
                                final int connectionPort = i % 2 == 0 ? port : nf.getGroup1Port();
                                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + connectionPort + "/qdb", properties);
                                connections.add(connection);
                                try (Statement statement = connection.createStatement();
                                     ResultSet rs = statement.executeQuery("select " + i)) {
                                    Assert.assertTrue(rs.next());
                                    Assert.assertEquals(i, rs.getInt(1));
                                }
                            }
                            awaitGroupConnections(server::getGroupConnectionCount, OWN_GROUP_PG_CONNECTIONS / 2, OWN_GROUP_PG_CONNECTIONS / 2);
                        } finally {
                            for (int i = 0, n = connections.size(); i < n; i++) {
                                connections.getQuick(i).close();
                            }
                        }
                    } finally {
                        // the server's jobs run on the pool, so the pool halts before the server closes
                        pool.halt();
                    }
                }
                nf.assertServedByOwnGroup(OWN_GROUP_PG_CONNECTIONS / 2);
            } finally {
                pool.halt();
            }
        });
    }

    /**
     * Checks that each accept and read on a server's network sockets runs on a worker of the group
     * whose listener the network socket came from. Group 1's listener binds to an ephemeral port of
     * its own, so connections reach both groups on every platform.
     */
    private static class GroupCheckingFacade extends NetworkFacadeImpl {
        private final AtomicIntegerArray acceptCounts = new AtomicIntegerArray(2);
        private final AtomicInteger bindCount = new AtomicInteger();
        private final AtomicReference<String> failure = new AtomicReference<>();
        // listener fds and accepted connection fds, mapped to their listener's group
        private final ConcurrentHashMap<Long, Integer> fdGroups = new ConcurrentHashMap<>();
        private volatile int group1Port;
        private final WorkerPool pool;
        private final AtomicIntegerArray recvCounts = new AtomicIntegerArray(2);

        GroupCheckingFacade(WorkerPool pool) {
            this.pool = pool;
        }

        @Override
        public long accept(long serverFd) {
            final long fd = super.accept(serverFd);
            final Integer group = fdGroups.get(serverFd);
            if (fd >= 0 && group != null) {
                fdGroups.put(fd, group);
                checkWorkerGroup(group, "accept");
                acceptCounts.incrementAndGet(group);
            }
            return fd;
        }

        @Override
        public boolean bindTcp(long fd, int address, int port) {
            // the port check binds first, then group 0, then group 1
            final int bind = bindCount.incrementAndGet();
            if (bind == 2) {
                fdGroups.put(fd, 0);
            } else if (bind == 3) {
                fdGroups.put(fd, 1);
                if (!super.bindTcp(fd, address, 0)) {
                    return false;
                }
                group1Port = resolvePort(fd);
                return true;
            }
            return super.bindTcp(fd, address, port);
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            final Integer group = fdGroups.get(fd);
            if (group != null) {
                checkWorkerGroup(group, "recv");
                recvCounts.incrementAndGet(group);
            }
            return super.recvRaw(fd, buffer, bufferLen);
        }

        void assertServedByOwnGroup(int connectionsPerGroup) {
            Assert.assertNull(failure.get());
            Assert.assertEquals(3, bindCount.get());
            for (int g = 0; g < 2; g++) {
                Assert.assertEquals("group " + g + " accepts", connectionsPerGroup, acceptCounts.get(g));
                Assert.assertTrue("group " + g + " reads", recvCounts.get(g) > 0);
            }
        }

        // connects every other connection to group 1's port
        long[] connectToBothGroups(int port, int count) {
            final long[] fds = new long[count];
            final long sockAddr = Net.sockaddr("127.0.0.1", port);
            final long group1SockAddr = Net.sockaddr("127.0.0.1", group1Port);
            try {
                for (int i = 0; i < count; i++) {
                    fds[i] = Net.socketTcp(true);
                    TestUtils.assertConnect(fds[i], i % 2 == 0 ? sockAddr : group1SockAddr);
                }
            } finally {
                Net.freeSockAddr(sockAddr);
                Net.freeSockAddr(group1SockAddr);
            }
            return fds;
        }

        int getGroup1Port() {
            return group1Port;
        }

        private void checkWorkerGroup(int group, String operation) {
            final Worker worker = Worker.current();
            if (worker == null || !pool.getPoolName().equals(worker.getPoolName()) || pool.getGroup(worker.getWorkerId()) != group) {
                failure.compareAndSet(null, operation + " for group " + group + " ran on "
                        + (worker == null ? "a thread outside the pool" : worker.getPoolName() + " worker " + worker.getWorkerId()));
            }
        }
    }
}
