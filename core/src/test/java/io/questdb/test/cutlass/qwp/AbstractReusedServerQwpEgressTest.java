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

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkError;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import static io.questdb.PropertyKey.HTTP_MIN_ENABLED;
import static io.questdb.PropertyKey.LINE_TCP_ENABLED;
import static io.questdb.PropertyKey.PG_ENABLED;

/**
 * Base for QWP egress tests that drive the server only through the engine and the QWP WebSocket
 * endpoint. Such tests booted a fresh {@link TestServerMain} per test, which is dominated by
 * on-disk database creation plus the recursive root wipe in {@code tearDown} - several hundred
 * milliseconds per test. This base boots a single server once for the whole class and drops the
 * user tables between tests instead.
 * <p>
 * Trade-offs and how they are handled:
 * <ul>
 *     <li>The shared server skips the PGWire, ILP and HTTP-min listeners. Egress tests only use the
 *     HTTP/QWP WebSocket port, so dropping the rest cuts per-boot work and listener resources.</li>
 *     <li>The per-test memory/FD leak check is disabled, because a persistent server's FDs and
 *     native memory do not return to a per-test baseline (e.g. async WAL purge of dropped tables).
 *     Instead a single class-level check brackets the whole class: it snapshots FD/native memory
 *     before the server boots and asserts they return to that baseline after the server is freed.
 *     FD counts must return exactly - a long-lived reused server leaking descriptors is the real
 *     risk. Native memory is checked with a small tolerance, because the persistent worker threads
 *     retain a bounded amount of thread-local {@link Path} memory that a byte-exact check would
 *     flag; a genuine leak across dozens of tests would be far larger.</li>
 *     <li>A test that needs a differently configured server calls {@link #freeSharedServer()} and
 *     boots its own; {@link #setUp()} reboots the shared instance (resetting any state the test's
 *     own server left under the same root) before the next test.</li>
 * </ul>
 */
public abstract class AbstractReusedServerQwpEgressTest extends AbstractBootstrapTest {

    private static final Log LOG = LogFactory.getLog(AbstractReusedServerQwpEgressTest.class);
    private static final long NATIVE_MEM_TOLERANCE_BYTES = 256 * 1024;
    protected static int recvChunk;
    protected static int sendChunk;
    protected static TestServerMain serverMain;
    private static long cachedFdBaseline;
    private static long fdBaseline;
    private static long memBaseline;

    @BeforeClass
    public static void setUpSharedServer() throws Exception {
        // Egress tests only use the HTTP/QWP WebSocket port; skip the listeners they never touch.
        createDummyConfiguration(
                PG_ENABLED + "=false",
                LINE_TCP_ENABLED + "=false",
                HTTP_MIN_ENABLED + "=false"
        );
        // One fragmentation chunk pair per class - the shared server pattern boots once, so we
        // can't randomize per test. Per-class variation still exercises the fragmentation paths
        // across the suite, and the seed is logged for deterministic replay.
        Rnd rnd = TestUtils.generateRandom(LOG);
        recvChunk = 1 + rnd.nextInt(500);
        sendChunk = 1 + rnd.nextInt(500);
        dbPath.parent().$();
        Path.clearThreadLocals();
        fdBaseline = Files.getOpenFileCount();
        cachedFdBaseline = Files.getOpenCachedFileCount();
        memBaseline = Unsafe.getMemUsed();
    }

    @AfterClass
    public static void tearDownSharedServer() {
        serverMain = Misc.free(serverMain);
        Path.clearThreadLocals();
        Assert.assertEquals("leaked OS file descriptors across the class", fdBaseline, Files.getOpenFileCount());
        Assert.assertEquals("leaked cached file descriptors across the class", cachedFdBaseline, Files.getOpenCachedFileCount());
        final long memGrowth = Unsafe.getMemUsed() - memBaseline;
        Assert.assertTrue("native memory grew by " + memGrowth + " bytes across the class", memGrowth < NATIVE_MEM_TOLERANCE_BYTES);
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        if (serverMain != null) {
            return;
        }
        // First test, or a previous test closed the shared server to run against its own instance.
        // Boot a fresh default-config server, then drop any tables that closed instance may have
        // left under the same root. The fragmentation env vars are class-scoped so every boot
        // (initial and post-freeSharedServer reboot) uses the same chunk sizes.
        serverMain = startServerWithRetry(
                PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(),
                Integer.toString(recvChunk),
                PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(),
                Integer.toString(sendChunk)
        );
        resetState();
    }

    @Override
    @After
    public void tearDown() {
        // Drop the test's tables instead of tearing the server down (the server is freed in
        // @AfterClass, and the root must stay around while it holds it open).
        if (serverMain != null) {
            resetState();
        }
    }

    /**
     * Frees the shared server so a single test can boot its own differently configured instance on
     * the same ports. {@link #setUp()} reboots the shared instance before the next test.
     */
    protected static void freeSharedServer() {
        serverMain = Misc.free(serverMain);
    }

    /**
     * Boots a server, retrying on a bind failure. These tests use fixed ports, and closing a server
     * does not release its listening port instantly, so a server booting right after another on the
     * same ports closed (across a class boundary, or after {@link #freeSharedServer()}) can hit
     * EADDRINUSE. Back off and retry until the port frees.
     */
    protected static TestServerMain startServerWithRetry(String... envs) {
        NetworkError lastError = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            try {
                return startWithEnvVariables(envs);
            } catch (NetworkError e) {
                // startWithEnvVariables already closed the half-started server before rethrowing.
                if (e.getMessage() == null || !Chars.contains(e.getMessage(), "could not bind socket")) {
                    throw e;
                }
                lastError = e;
                LOG.info().$("server port still in use, retrying boot [attempt=").$(attempt).$(']').$();
                Os.sleep(100);
            }
        }
        throw lastError;
    }

    protected void resetState() {
        serverMain.execute("DROP ALL TABLES");
        final CairoEngine engine = serverMain.getEngine();
        engine.releaseInactive();
        // Apply the enqueued WAL drops so the table names are free for the next test's CREATE.
        drainWalQueue(engine);
    }
}
