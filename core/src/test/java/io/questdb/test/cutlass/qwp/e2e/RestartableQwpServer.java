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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.test.mp.TestWorkerPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps an {@link HttpServer} bound to a fixed port with a worker pool and the
 * QWP WebSocket processor, so tests can stop/start it across the same port
 * without losing the underlying {@link CairoEngine} state. Single-threaded
 * worker pool keeps test scheduling deterministic.
 * <p>
 * The server also tracks the connections whose WebSocket upgrade has completed and that its
 * worker has not closed; {@link #stop()} returns how many of them it killed, and
 * {@link #awaitStableLiveConnection(long, long)} lets a test land a stop on one deliberately.
 */
public final class RestartableQwpServer implements AutoCloseable {
    static final int PORT_PICK_ATTEMPTS = 5;
    private static final Log LOG = LogFactory.getLog(RestartableQwpServer.class);
    private final CairoConfiguration cairoConfiguration;
    private final CairoEngine engine;
    private final int forceRecvFragmentationChunkSize;
    private final int forceSendFragmentationChunkSize;
    // fds of connections whose WebSocket upgrade has completed and that the worker has not yet
    // closed. Written by the single worker thread, read by test threads, and cleared by stop()
    // only after the workers have halted.
    private final Set<Long> liveFds = ConcurrentHashMap.newKeySet();
    private final int port;
    private final AtomicBoolean running = new AtomicBoolean();
    private HttpServer server;
    private TestWorkerPool workerPool;

    public RestartableQwpServer(CairoEngine engine, CairoConfiguration cairoConfiguration, int port) {
        this(engine, cairoConfiguration, port, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Overload that forces per-socket recv / send fragmentation at the HTTP
     * context level. Pass {@link Integer#MAX_VALUE} for either side to leave
     * that direction unfragmented. The chunk sizes are stored on this
     * instance, so they persist across {@link #stop()} / {@link #start()}
     * cycles -- restart-fuzz tests get the same fragmentation behaviour on
     * every restart.
     */
    public RestartableQwpServer(
            CairoEngine engine,
            CairoConfiguration cairoConfiguration,
            int port,
            int forceRecvFragmentationChunkSize,
            int forceSendFragmentationChunkSize
    ) {
        this.engine = engine;
        this.cairoConfiguration = cairoConfiguration;
        this.port = port;
        this.forceRecvFragmentationChunkSize = forceRecvFragmentationChunkSize;
        this.forceSendFragmentationChunkSize = forceSendFragmentationChunkSize;
    }

    /**
     * Picks a TCP port that a plain {@code new ServerSocket(0)} reports free on the wildcard
     * address AND that this method then also binds successfully on the loopback address. A
     * candidate a foreign process already LISTENs on at {@code 127.0.0.1} fails that second bind,
     * and this method picks another one.
     * <p>
     * The loopback probe matters because {@code Net.socketTcp(false)} sets {@code SO_REUSEADDR}
     * before {@code bind} (see {@code core/src/main/c/share/net.c}, reached through
     * {@code AbstractIODispatcher}). On macOS/BSD, the platform we verified this on, the test
     * server's wildcard bind over an existing loopback-only listener SUCCEEDS instead of failing,
     * and the kernel then routes loopback connections to the more specific socket -- our server
     * listens where nobody dials. That is how {@code QwpIngressOracleFuzzTest} once died on a
     * ws-upgrade {@code HTTP/1.1 404 Not Found} that no QuestDB server in the JVM had sent. Linux
     * is expected to behave differently: {@code inet_csk_bind_conflict} refuses a bind that
     * conflicts with a LISTENing socket even under {@code SO_REUSEADDR}. Where the kernel refuses
     * the bind, the server fails loudly at startup instead of listening in the wrong place, and
     * this probe merely spares us that failure.
     * <p>
     * Whether an ephemeral allocator ever hands out such a port at all is platform-dependent;
     * measurements on one machine disagreed and settled nothing, so this javadoc claims no rule
     * about it. The probe also leaves the TOCTOU window open, because it closes the candidate
     * before the caller binds it and the stable-port restart contract of this class rules out
     * holding it. It rejects only what is certainly wrong: a port ALREADY occupied on the
     * loopback address at the moment we hand it out.
     */
    public static int pickFreePort() throws IOException {
        return pickFreePort(RestartableQwpServer::allocateWildcardPort);
    }

    /**
     * Seam over {@link #pickFreePort()}: the caller supplies the candidate ports the loopback
     * probe then vets. Every caller outside this class's own unit test uses the no-argument
     * overload, which feeds it {@code new ServerSocket(0)}. The unit test feeds it a port it
     * holds a loopback listener on, which is the only way a test can reach the retry and
     * exhaustion branches deterministically: occupancy is the only bind failure a test can
     * arrange, and waiting for an ephemeral allocator to hand out an occupied port is a wait the
     * javadoc above explains nobody can promise ever ends. The probe's bind can fail for other
     * reasons too -- see the throw below -- but no test drives those.
     */
    static int pickFreePort(PortCandidateSupplier candidates) throws IOException {
        int port = -1;
        IOException lastError = null;
        for (int attempt = 1; attempt <= PORT_PICK_ATTEMPTS; attempt++) {
            port = candidates.next();
            try (ServerSocket loopback = new ServerSocket(port, 0, InetAddress.getLoopbackAddress())) {
                return loopback.getLocalPort();
            } catch (IOException shadowed) {
                lastError = shadowed;
                LOG.info().$("picked port is taken on the loopback address, picking another [port=").$(port)
                        .$(", attempt=").$(attempt)
                        .$(", reason=").$(shadowed.getMessage())
                        .I$();
            }
        }
        // The loopback bind can also fail for reasons other than occupancy, such as a sandbox
        // denying the bind. Carry the last failure as the cause so the caller sees which one hit.
        throw new IllegalStateException("no candidate port bound on 127.0.0.1 after "
                + PORT_PICK_ATTEMPTS + " attempts [lastPort=" + port + ']', lastError);
    }

    private static int allocateWildcardPort() throws IOException {
        try (ServerSocket wildcard = new ServerSocket(0)) {
            return wildcard.getLocalPort();
        }
    }

    /**
     * Waits until one upgraded connection has been in the live set, unchanged, for
     * {@code stableMillis}. "Live" means the WebSocket upgrade has completed and the worker has
     * entered the frame-reading path on it, so the client's I/O loop is past its connect and
     * in its send loop. The stability window lets a recycle the client had already armed before the
     * caller paused resets fire at its next barrier and reconnect before the caller acts on
     * the connection. Polls every millisecond; returns false once {@code timeoutMillis} passes.
     */
    public boolean awaitStableLiveConnection(long timeoutMillis, long stableMillis) {
        final long deadlineNanos = System.nanoTime() + timeoutMillis * 1_000_000L;
        Long stableFd = null;
        long stableSinceNanos = 0;
        while (deadlineNanos - System.nanoTime() > 0) {
            // Exactly one live fd: during a recycle the worker removes the old connection only
            // once it has processed the client's close, so the old and the new fd overlap
            // briefly, and certifying whichever iterates first could pick the dead one.
            Long fd = null;
            if (liveFds.size() == 1) {
                for (Long candidate : liveFds) {
                    fd = candidate;
                    break;
                }
            }
            if (fd != null && fd.equals(stableFd)) {
                if (System.nanoTime() - stableSinceNanos >= stableMillis * 1_000_000L) {
                    return true;
                }
            } else {
                stableFd = fd;
                stableSinceNanos = System.nanoTime();
            }
            Os.sleep(1);
        }
        return false;
    }

    @Override
    public void close() {
        if (running.get()) {
            stop();
        }
    }

    public int liveConnectionCount() {
        return liveFds.size();
    }

    public void start() throws SqlException {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("already running");
        }
        HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(
                cairoConfiguration,
                new DefaultHttpContextConfiguration() {
                    @Override
                    public int getForceRecvFragmentationChunkSize() {
                        return forceRecvFragmentationChunkSize;
                    }

                    @Override
                    public int getForceSendFragmentationChunkSize() {
                        return forceSendFragmentationChunkSize;
                    }
                }
        ) {
            @Override
            public int getBindPort() {
                return port;
            }
        };

        workerPool = new TestWorkerPool(1);
        server = new HttpServer(httpConfig, workerPool, PlainSocketFactory.INSTANCE);
        final LiveTrackingUpgradeProcessor processor = new LiveTrackingUpgradeProcessor(engine, httpConfig, liveFds);
        server.bind(new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return httpConfig.getContextPathQWP();
            }

            @Override
            public HttpRequestHandler newInstance() {
                // Production wraps the upgrade processor in QwpIngressHttpProcessor, whose
                // getProcessor() unconditionally returns its one shared instance; returning
                // ours directly keeps that shape and lets the test observe each connection.
                return requestHeader -> processor;
            }
        });
        WorkerPoolUtils.setupWriterJobs(workerPool, engine);
        workerPool.start(LOG);
    }

    /**
     * Halts the workers, then closes the server. Returns how many upgraded connections were
     * still live when the workers halted: after {@code halt()} no worker can process a client
     * close, so every fd left in the live set is a connection the shutdown below kills under
     * the client. That shutdown is the dispatcher's {@code src=shutdown} path, which never
     * calls the processor's {@code onConnectionClosed}, so the set is cleared here.
     */
    public int stop() {
        if (!running.compareAndSet(true, false)) {
            return 0;
        }
        try {
            workerPool.halt();
        } catch (Throwable t) {
            LOG.error().$("worker pool halt failed").$(t).$();
        }
        final int killed = liveFds.size();
        liveFds.clear();
        try {
            server.close();
        } catch (Throwable t) {
            LOG.error().$("server close failed").$(t).$();
        }
        server = null;
        workerPool = null;
        return killed;
    }

    /**
     * Records which connections have completed the WebSocket upgrade. {@code resumeRecv} is the
     * dispatcher's entry for every readable event after the protocol switch, and the dispatcher
     * re-enters it right after the upgrade completes, before any frame has arrived, so its first
     * call for an fd means the upgrade is done and the worker is reading frames on it. The fd is
     * added on entry rather than after {@code super} returns because a frame whose ack cannot
     * be sent at once surfaces as a backpressure exception, not a normal return. A
     * peer-disconnect event on a connection that never sent anything adds the fd and then
     * removes it through {@code onConnectionClosed} on the same worker call.
     */
    private static final class LiveTrackingUpgradeProcessor extends QwpIngressUpgradeProcessor {
        private final Set<Long> liveFds;

        private LiveTrackingUpgradeProcessor(CairoEngine engine, HttpFullFatServerConfiguration httpConfiguration, Set<Long> liveFds) {
            super(engine, httpConfiguration);
            this.liveFds = liveFds;
        }

        @Override
        public void onConnectionClosed(HttpConnectionContext context) {
            liveFds.remove(context.getFd());
            super.onConnectionClosed(context);
        }

        @Override
        public void resumeRecv(HttpConnectionContext context) throws PeerIsSlowToWriteException, ServerDisconnectException, PeerIsSlowToReadException {
            liveFds.add(context.getFd());
            super.resumeRecv(context);
        }
    }

    @FunctionalInterface
    interface PortCandidateSupplier {
        int next() throws IOException;
    }
}
