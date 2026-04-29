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
import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.test.mp.TestWorkerPool;

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
    private final CairoConfiguration cairoConfiguration;
    private final CairoEngine engine;
    private final int port;
    private final AtomicBoolean running = new AtomicBoolean();
    private HttpServer server;
    private TestWorkerPool workerPool;

    public RestartableQwpServer(CairoEngine engine, CairoConfiguration cairoConfiguration, int port) {
        this.engine = engine;
        this.cairoConfiguration = cairoConfiguration;
        this.port = port;
    }

    /** Pick a free TCP port by binding port 0 and reading what the OS gave us. */
    public static int pickFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
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
                new DefaultHttpContextConfiguration()
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
            public QwpWebSocketHttpProcessor newInstance() {
                return new QwpWebSocketHttpProcessor(engine, httpConfig);
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
