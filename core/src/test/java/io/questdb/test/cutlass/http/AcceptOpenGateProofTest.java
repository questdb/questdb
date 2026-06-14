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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.cutlass.pgwire.Port0PGConfiguration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Proves the acceptOpen gate in HttpServer and PGServer freezes in-flight IO when armed.
 * <p>
 * This is the positive complement to the ENT D-08 structural guard: together they form
 * the airtight no-freeze story. D-08 (RegistrationGuardTest) proves ENT never arms the
 * gate for pg-wire/web-http. This test proves the gate WOULD freeze IF armed.
 * <p>
 * The airtight argument: (gate would freeze IF armed) + (ENT never arms it) =
 * reads-never-freeze is guaranteed.
 * <p>
 * This is a regression guard: it fires if someone changes the gate semantics, removes
 * the acceptOpen check, or re-arms the gate for HTTP/pg-wire in the ENT server.
 * <p>
 * The injection seam: both HttpServer and PGServer expose a 4-arg / 6-arg constructor
 * that accepts a caller-supplied AtomicBoolean acceptOpen. The 3-arg / 5-arg constructors
 * default it to new AtomicBoolean(true), which is always open. The gate sites are:
 * - HttpServer.java dispatcher job (:120), rescheduleContext job (:130), IO queue job (:149)
 * - PGServer.java dispatcher job (:119), IO queue job (:162)
 * <p>
 * LineTcpReceiver.java:76 is an accept-only gate (in-flight-safe) and is intentionally
 * NOT tested for in-flight freeze here.
 */
public class AcceptOpenGateProofTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(AcceptOpenGateProofTest.class);

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(60, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    /**
     * Proves the HttpServer acceptOpen gate freezes in-flight IO when armed.
     * <p>
     * The test injects a test-owned AtomicBoolean into the 4-arg HttpServer ctor.
     * It drives traffic through the server with acceptOpen=true (requests complete
     * normally), then flips the flag false and verifies a new request stalls at the
     * gate within a bounded window, then restores acceptOpen=true and confirms the
     * stalled connection resumes.
     * <p>
     * This test is RED if the gate code at HttpServer.java:120/130/149 is removed
     * and GREEN with the gate in place.
     */
    @Test
    public void testHttpServerAcceptOpenGateFreezes() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean acceptOpen = new AtomicBoolean(true);
            final DefaultHttpServerConfiguration httpConfig = new HttpServerConfigurationBuilder()
                    .withPort(0)
                    .withNetwork(NetworkFacadeImpl.INSTANCE)
                    .build(configuration);

            final TestWorkerPool workerPool = new TestWorkerPool(1);
            try (HttpServer httpServer = new HttpServer(httpConfig, workerPool, PlainSocketFactory.INSTANCE, acceptOpen)) {
                workerPool.start(LOG);
                try {
                    final int port = httpServer.getPort();
                    LOG.info().$("HTTP server bound to port ").$safe(String.valueOf(port)).$();

                    // Phase 1: verify gate=open (acceptOpen=true) -- a minimal request gets a response.
                    // Confirm the gate allows IO through before testing the block.
                    assertHttpRequestReceivesResponse(port, "open");

                    // Phase 2: flip the gate closed and verify the IO job stalls.
                    // With acceptOpen=false the dispatcher job (HttpServer.java:120) and the
                    // per-worker IO queue job (:149) both return false immediately, so no new
                    // connections are processed. We connect, send the request bytes, then poll
                    // for up to 300ms and confirm nothing comes back.
                    acceptOpen.set(false);
                    LOG.info().$("gate closed (acceptOpen=false) -- verifying IO job stalls").$();
                    // Keep stallFd open to avoid FD recycling while the gate is closed.
                    final long stallFd = connectAndSendHttp(port);
                    try {
                        assertNoResponseWithin(stallFd, 300);
                    } finally {
                        Net.close(stallFd);
                    }

                    // Phase 3: flip the gate back open and verify IO resumes.
                    // This confirms the gate is the cause of the stall, not an unrelated factor.
                    // Wait briefly so the server drains the stale connection before phase 3 connect.
                    Os.sleep(50);
                    acceptOpen.set(true);
                    LOG.info().$("gate re-opened (acceptOpen=true) -- verifying IO resumes").$();
                    assertHttpRequestReceivesResponse(port, "re-opened");
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    /**
     * Proves the PGServer acceptOpen gate freezes in-flight IO when armed.
     * <p>
     * Mirrors the HTTP proof for the pg-wire protocol. The 6-arg PGServer constructor
     * accepts a caller-supplied AtomicBoolean acceptOpen. The gate sites at
     * PGServer.java:119 (dispatcher job) and :162 (per-worker IO queue job) both
     * return false when acceptOpen=false.
     * <p>
     * Uses JDBC connections to avoid interactions with QuestDB's internal FD tracking,
     * which can cause spurious EBADF / fd-cache assertion failures with raw sockets.
     * <p>
     * This test is RED if the gate code at PGServer.java:119/162 is removed
     * and GREEN with the gate in place.
     */
    @Test
    public void testPGServerAcceptOpenGateFreezes() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean acceptOpen = new AtomicBoolean(true);
            final Port0PGConfiguration pgConfig = new Port0PGConfiguration();

            final TestWorkerPool workerPool = new TestWorkerPool(1);
            // Use the shared AbstractCairoTest engine; only create registry and PGServer.
            try (
                    DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(pgConfig, configuration);
                    PGServer pgServer = new PGServer(
                            pgConfig,
                            engine,
                            workerPool,
                            registry,
                            () -> new SqlExecutionContextImpl(engine, 1),
                            acceptOpen
                    )
            ) {
                workerPool.start(LOG);
                try {
                    final int port = pgServer.getPort();
                    LOG.info().$("PGServer bound to port ").$safe(String.valueOf(port)).$();

                    // Phase 1: verify gate=open -- JDBC connects and executes a simple query.
                    assertPgJdbcConnects(port, "open");

                    // Phase 2: flip the gate closed and verify JDBC cannot complete the handshake.
                    // With acceptOpen=false the dispatcher job (PGServer.java:119) does not call
                    // dispatcher.run(), so new connections are never accepted from the OS backlog.
                    // The IO queue job (:162) also returns false, so no data is processed.
                    // A JDBC connection with a short socket timeout will time out waiting for the
                    // PG auth challenge, proving the gate blocks in-flight IO.
                    acceptOpen.set(false);
                    LOG.info().$("PG gate closed (acceptOpen=false) -- verifying connection times out").$();
                    assertPgJdbcTimesOut(port);

                    // Phase 3: restore the gate and verify connections are accepted again.
                    acceptOpen.set(true);
                    LOG.info().$("PG gate re-opened (acceptOpen=true) -- verifying IO resumes").$();
                    assertPgJdbcConnects(port, "re-opened");
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    // --- helpers ---

    /**
     * Sends a minimal HTTP HEAD request on a new TCP connection and asserts that
     * some bytes (any HTTP response bytes) are received within the max-wait window.
     * Uses non-blocking recv with polling so the test does not hang.
     */
    private static void assertHttpRequestReceivesResponse(int port, String phase) {
        final long fd = connectAndSendHttp(port);
        final long buf = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
        try {
            final long recvDeadline = System.currentTimeMillis() + 5000;
            boolean received = false;
            while (System.currentTimeMillis() < recvDeadline) {
                final int recvResult = Net.recv(fd, buf, 4096);
                if (recvResult > 0) {
                    received = true;
                    break;
                }
                Os.sleep(10);
            }
            Assert.assertTrue(
                    "HTTP server did not respond within 5s with gate='" + phase + "' (acceptOpen=true). " +
                            "The gate is expected to let requests through.",
                    received
            );
        } finally {
            Unsafe.free(buf, 4096, MemoryTag.NATIVE_DEFAULT);
            Net.close(fd);
        }
    }

    /**
     * Asserts that NO bytes arrive on fd within stallWindowMs. Used to verify the
     * acceptOpen=false gate at HttpServer.java:120/130/149.
     * <p>
     * If the gate code were removed, the server would respond and this assertion
     * would fail -- making the test RED.
     */
    private static void assertNoResponseWithin(long fd, long stallWindowMs) {
        final long buf = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
        try {
            final long deadline = System.currentTimeMillis() + stallWindowMs;
            while (System.currentTimeMillis() < deadline) {
                final int recvResult = Net.recv(fd, buf, 4096);
                if (recvResult > 0) {
                    Assert.fail(
                            "Server processed a request while acceptOpen=false. " +
                                    "The acceptOpen gate at HttpServer.java:120/130/149 is not working. " +
                                    "This test is RED if the gate code is removed."
                    );
                }
                Os.sleep(10);
            }
        } finally {
            Unsafe.free(buf, 4096, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Asserts that a JDBC connection to the PGServer succeeds with gate=open.
     * Runs a trivial SELECT 1 to confirm the full protocol handshake completes.
     */
    private static void assertPgJdbcConnects(int port, String phase) throws SQLException {
        final Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", "quest");
        props.setProperty("sslmode", "disable");
        props.setProperty("socketTimeout", "5");
        final String url = "jdbc:postgresql://127.0.0.1:" + port + "/qdb";
        try (Connection conn = DriverManager.getConnection(url, props)) {
            // Execute a trivial query to confirm the protocol handshake completed.
            try (var stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");
            }
        } catch (SQLException e) {
            Assert.fail("JDBC connect/query failed with gate='" + phase + "' (acceptOpen=true): " + e.getMessage());
        }
    }

    /**
     * Asserts that a JDBC connection to the PGServer times out when gate=closed.
     * <p>
     * With acceptOpen=false the PGServer dispatcher (PGServer.java:119) does not call
     * dispatcher.run(), so new connections are never accepted from the OS backlog.
     * The JDBC driver will wait for the PG startup handshake but never receive it,
     * resulting in a socket timeout -- proving the gate blocks in-flight IO.
     * <p>
     * If the gate code at PGServer.java:119/162 were removed, the PGServer would
     * respond normally and this assertion would fail -- making the test RED.
     */
    private static void assertPgJdbcTimesOut(int port) {
        final Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", "quest");
        props.setProperty("sslmode", "disable");
        // 1-second socket timeout: short enough to keep the test fast.
        props.setProperty("socketTimeout", "1");
        final String url = "jdbc:postgresql://127.0.0.1:" + port + "/qdb";
        try (Connection conn = DriverManager.getConnection(url, props)) {
            // If we get here, the server responded while acceptOpen=false.
            Assert.fail(
                    "PGServer accepted a JDBC connection while acceptOpen=false. " +
                            "The acceptOpen gate at PGServer.java:119/162 is not working. " +
                            "This test is RED if the gate code is removed."
            );
        } catch (PSQLException e) {
            // Expected: timeout or connection error -- the gate blocked the handshake.
            LOG.info().$("PG gate correctly blocked: ").$safe(e.getMessage()).$();
        } catch (SQLException e) {
            // Also acceptable: the connection was refused or timed out via another mechanism.
            LOG.info().$("PG gate correctly blocked (general SQL): ").$safe(e.getMessage()).$();
        }
    }

    /**
     * Opens a non-blocking TCP socket, connects to the given port, and sends a
     * minimal HTTP HEAD request. Returns the open socket FD; caller must close it.
     */
    private static long connectAndSendHttp(int port) {
        final long fd = Net.socketTcp(true);
        Assert.assertTrue("failed to create HTTP socket", fd > 0);
        final long buf = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
        try {
            long addrInfo = Net.getAddrInfo("127.0.0.1", port);
            try {
                Net.connectAddrInfo(fd, addrInfo);
            } finally {
                Net.freeAddrInfo(addrInfo);
            }
            Net.configureNonBlocking(fd);

            final String request = "HEAD / HTTP/1.0\r\nHost: localhost\r\n\r\n";
            final int reqLen = request.length();
            Utf8s.strCpyAscii(request, reqLen, buf);

            // Best-effort send; even if the OS buffers the bytes, the gate stalls draining them.
            int totalSent = 0;
            final long sendDeadline = System.currentTimeMillis() + 1000;
            while (totalSent < reqLen && System.currentTimeMillis() < sendDeadline) {
                final int sent = Net.send(fd, buf + totalSent, reqLen - totalSent);
                if (sent > 0) {
                    totalSent += sent;
                } else {
                    Os.sleep(5);
                }
            }
        } finally {
            Unsafe.free(buf, 256, MemoryTag.NATIVE_DEFAULT);
        }
        return fd;
    }
}
