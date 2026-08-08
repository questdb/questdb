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
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.qwp.server.QwpIngressHttpProcessor;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.test.mp.TestWorkerPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps an {@link HttpServer} bound to a fixed port with a worker pool and the
 * QWP WebSocket processor, so tests can stop/start it across the same port
 * without losing the underlying {@link CairoEngine} state. Single-threaded
 * worker pool keeps test scheduling deterministic.
 */
public final class RestartableQwpServer implements AutoCloseable {
    private static final Log LOG = LogFactory.getLog(RestartableQwpServer.class);
    private static final int PORT_PICK_ATTEMPTS = 5;
    private final CairoConfiguration cairoConfiguration;
    private final CairoEngine engine;
    private final int forceRecvFragmentationChunkSize;
    private final int forceSendFragmentationChunkSize;
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
        int port = -1;
        IOException lastError = null;
        for (int attempt = 1; attempt <= PORT_PICK_ATTEMPTS; attempt++) {
            try (ServerSocket wildcard = new ServerSocket(0)) {
                port = wildcard.getLocalPort();
            }
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
        throw new IllegalStateException("could not pick a port free on both 0.0.0.0 and 127.0.0.1 after "
                + PORT_PICK_ATTEMPTS + " attempts [lastPort=" + port + ']', lastError);
    }

    @Override
    public void close() {
        if (running.get()) {
            stop();
        }
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
        server.bind(new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return httpConfig.getContextPathQWP();
            }

            @Override
            public QwpIngressHttpProcessor newInstance() {
                return new QwpIngressHttpProcessor(engine, httpConfig);
            }
        });
        WorkerPoolUtils.setupWriterJobs(workerPool, engine);
        workerPool.start(LOG);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        try {
            workerPool.halt();
        } catch (Throwable t) {
            LOG.error().$("worker pool halt failed").$(t).$();
        }
        try {
            server.close();
        } catch (Throwable t) {
            LOG.error().$("server close failed").$(t).$();
        }
        server = null;
        workerPool = null;
    }
}
