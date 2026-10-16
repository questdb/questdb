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
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.network.IODispatchers;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.MemoryTag;
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
import org.junit.Before;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

/**
 * HTTP, PG wire and ILP/TCP servers on a network pool of two worker groups: each server creates
 * a dispatcher per group, all listening on one port through SO_REUSEPORT. The kernel spreads
 * connections over such listeners on Linux only, so these tests run there.
 */
public class ServerGroupsTest extends AbstractCairoTest {
    // with this many connections, all of them land on one of two listeners with probability 2^-39
    private static final int CONNECTIONS = 40;
    private static final Log LOG = LogFactory.getLog(ServerGroupsTest.class);
    private static final int PG_CONNECTIONS = 24;
    private static final int WORKERS = 4;
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(3, TimeUnit.MINUTES)
            .withLookingForStuckThread(true)
            .build();

    @Before
    public void assumeLinux() {
        Assume.assumeTrue("SO_REUSEPORT spreads connections on Linux only", Os.isLinux());
    }

    @Test
    public void testHttpFailedGroupFreesEarlierGroups() throws Exception {
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
    public void testHttpLegacy() throws Exception {
        assertHttp(WorkerPoolMode.LEGACY, false);
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
    public void testIlpTcp() throws Exception {
        assertMemoryLeak(() -> {
            final int port = pickFreePort();
            final DefaultLineTcpReceiverConfiguration lineConfig = new DefaultLineTcpReceiverConfiguration(configuration) {
                @Override
                public int getBindPort() {
                    return port;
                }
            };
            final TestWorkerPool networkPool = new TestWorkerPool(groupedPool(WorkerPoolMode.LEGACY));
            final TestWorkerPool writePool = new TestWorkerPool(1);
            try (LineTcpReceiver receiver = new LineTcpReceiver(lineConfig, engine, networkPool, writePool)) {
                Assert.assertEquals(2, receiver.getGroupCount());
                networkPool.start(LOG);
                writePool.start(LOG);
                try {
                    final long[] fds = connect(port, CONNECTIONS);
                    try {
                        awaitSpread(receiver::getGroupConnectionCount, CONNECTIONS);
                        for (int i = 0; i < CONNECTIONS; i++) {
                            send(fds[i], "groups,conn=c" + i + " v=" + i + "i\n");
                        }
                    } finally {
                        close(fds);
                    }
                    // every line lands, so both groups' dispatchers have network IO jobs
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
                    networkPool.halt();
                    writePool.halt();
                }
            }
        });
    }

    @Test
    public void testPgFibers() throws Exception {
        assertPg(WorkerPoolMode.FIBER_HOST, true);
    }

    @Test
    public void testPgLegacy() throws Exception {
        assertPg(WorkerPoolMode.LEGACY, false);
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
                return WORKERS;
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

    private void assertPg(WorkerPoolMode mode, boolean isFiberEnabled) throws Exception {
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
}
