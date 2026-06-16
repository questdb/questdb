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

package io.questdb.cutlass;

import io.questdb.Metrics;
import io.questdb.ServerConfiguration;
import io.questdb.WorkerPoolManager;
import io.questdb.WorkerPoolManager.Requester;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.processors.HealthCheckProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.LineHttpProcessorImpl;
import io.questdb.cutlass.http.processors.PrometheusMetricsProcessor;
import io.questdb.cutlass.http.processors.SqlValidationProcessor;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LinuxMMLineUdpReceiver;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGHexTestsCircuitBreakRegistry;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.cutlass.qwp.server.LinuxMMQwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiverConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

public class Services {
    public static final Services INSTANCE = new Services();

    protected Services() {
    }

    @Nullable
    public HttpServer createHttpServer(
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager
    ) {
        return createHttpServer(serverConfiguration, cairoEngine, workerPoolManager, new AtomicBoolean(true));
    }

    @Nullable
    public HttpServer createHttpServer(
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager,
            AtomicBoolean acceptOpen
    ) {
        HttpFullFatServerConfiguration httpServerConfiguration = serverConfiguration.getHttpServerConfiguration();
        if (!httpServerConfiguration.isEnabled()) {
            return null;
        }

        // The pool is:
        // - DEDICATED when PropertyKey.HTTP_WORKER_COUNT is > 0
        // - SHARED otherwise
        return createHttpServer(
                serverConfiguration,
                cairoEngine,
                workerPoolManager.getSharedPoolNetwork(httpServerConfiguration, Requester.HTTP_SERVER),
                workerPoolManager.getSharedQueryWorkerCount(),
                acceptOpen
        );
    }

    @Nullable
    public HttpServer createHttpServer(
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            WorkerPool networkSharedPool,
            int sharedQueryWorkerCount
    ) {
        return createHttpServer(serverConfiguration, cairoEngine, networkSharedPool, sharedQueryWorkerCount, new AtomicBoolean(true));
    }

    @Nullable
    public HttpServer createHttpServer(
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            WorkerPool networkSharedPool,
            int sharedQueryWorkerCount,
            AtomicBoolean acceptOpen
    ) {
        final HttpFullFatServerConfiguration httpServerConfiguration = serverConfiguration.getHttpServerConfiguration();
        if (!httpServerConfiguration.isEnabled()) {
            return null;
        }

        final HttpServer server = new HttpServer(
                httpServerConfiguration,
                networkSharedPool,
                httpServerConfiguration.getFactoryProvider().getHttpSocketFactory(),
                acceptOpen
        );
        HttpServer.HttpRequestHandlerBuilder jsonQueryProcessorBuilder = () -> new JsonQueryProcessor(
                httpServerConfiguration.getJsonQueryProcessorConfiguration(),
                cairoEngine,
                sharedQueryWorkerCount
        );

        HttpServer.HttpRequestHandlerBuilder sqlValidationProcessorBuilder = () -> new SqlValidationProcessor(
                httpServerConfiguration.getJsonQueryProcessorConfiguration(),
                cairoEngine,
                sharedQueryWorkerCount
        );

        HttpServer.HttpRequestHandlerBuilder ilpV2WriteProcessorBuilder = () -> new LineHttpProcessorImpl(
                cairoEngine,
                httpServerConfiguration
        );

        HttpServer.addDefaultEndpoints(
                server,
                serverConfiguration,
                cairoEngine,
                sharedQueryWorkerCount,
                jsonQueryProcessorBuilder,
                ilpV2WriteProcessorBuilder,
                sqlValidationProcessorBuilder
        );
        return server;
    }

    @Nullable
    public LineTcpReceiver createLineTcpReceiver(
            LineTcpReceiverConfiguration config,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager
    ) {
        return createLineTcpReceiver(config, cairoEngine, workerPoolManager, new AtomicBoolean(true));
    }

    @Nullable
    public LineTcpReceiver createLineTcpReceiver(
            LineTcpReceiverConfiguration config,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager,
            AtomicBoolean acceptOpen
    ) {
        if (!config.isEnabled()) {
            return null;
        }

        // ILP TCP IO and Writer Jobs key per-worker state by the workerId they
        // are constructed with, so they require legacy worker pools (no
        // continuation wrapping, assign(int worker, Job) permitted). Wrap the
        // user-supplied configurations to force isLegacy=true and ensure
        // dedicated pools are always created (getWorkerCount() >= 1), since
        // the shared pools host continuations and would reject the per-worker
        // assignments.
        final WorkerPool sharedPoolNetwork = workerPoolManager.getSharedPoolNetwork(
                asLegacy(config.getNetworkWorkerPoolConfiguration(), "line-tcp-io"),
                Requester.LINE_TCP_IO
        );
        final WorkerPool sharedPoolWrite = workerPoolManager.getSharedPoolWrite(
                asLegacy(config.getWriterWorkerPoolConfiguration(), "line-tcp-writer"),
                Requester.LINE_TCP_WRITER
        );
        return new LineTcpReceiver(config, cairoEngine, sharedPoolNetwork, sharedPoolWrite, acceptOpen);
    }

    private static io.questdb.mp.WorkerPoolConfiguration asLegacy(
            io.questdb.mp.WorkerPoolConfiguration delegate,
            String defaultPoolName
    ) {
        return new io.questdb.mp.WorkerPoolConfiguration() {
            @Override
            public io.questdb.Metrics getMetrics() {
                return delegate.getMetrics();
            }

            @Override
            public long getNapThreshold() {
                return delegate.getNapThreshold();
            }

            @Override
            public String getPoolName() {
                String n = delegate.getPoolName();
                return n != null && !n.isEmpty() ? n : defaultPoolName;
            }

            @Override
            public long getSleepThreshold() {
                return delegate.getSleepThreshold();
            }

            @Override
            public long getSleepTimeout() {
                return delegate.getSleepTimeout();
            }

            @Override
            public int[] getWorkerAffinity() {
                return delegate.getWorkerAffinity();
            }

            @Override
            public int getWorkerCount() {
                // Force a dedicated pool: WorkerPoolManager falls back to the
                // shared (non-legacy) pool when workerCount < 1, which would
                // reject ILP's per-worker assign() calls.
                int n = delegate.getWorkerCount();
                return n > 0 ? n : 2;
            }

            @Override
            public long getYieldThreshold() {
                return delegate.getYieldThreshold();
            }

            @Override
            public boolean haltOnError() {
                return delegate.haltOnError();
            }

            @Override
            public boolean isDaemonPool() {
                return delegate.isDaemonPool();
            }

            @Override
            public boolean isEnabled() {
                return delegate.isEnabled();
            }

            @Override
            public boolean isLegacy() {
                return true;
            }

            @Override
            public int workerPoolPriority() {
                return delegate.workerPoolPriority();
            }
        };
    }

    @Nullable
    public AbstractLineProtoUdpReceiver createLineUdpReceiver(
            LineUdpReceiverConfiguration config,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager
    ) {
        return createLineUdpReceiver(config, cairoEngine, workerPoolManager, new AtomicBoolean(true));
    }

    @Nullable
    public AbstractLineProtoUdpReceiver createLineUdpReceiver(
            LineUdpReceiverConfiguration config,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager,
            AtomicBoolean acceptOpen
    ) {
        if (!config.isEnabled()) {
            return null;
        }

        // The pool is always the SHARED pool
        if (Os.isLinux()) {
            return new LinuxMMLineUdpReceiver(config, cairoEngine, workerPoolManager.getSharedPoolNetwork(), acceptOpen);
        }
        return new LineUdpReceiver(config, cairoEngine, workerPoolManager.getSharedPoolNetwork(), acceptOpen);
    }

    @Nullable
    public HttpServer createMinHttpServer(
            HttpServerConfiguration configuration,
            WorkerPoolManager workerPoolManager
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        // The pool is:
        // - SHARED if PropertyKey.HTTP_MIN_WORKER_COUNT (http.min.worker.count) <= 0
        // - DEDICATED (1 worker) otherwise
        final WorkerPool networkSharedPool = workerPoolManager.getSharedPoolNetwork(
                configuration,
                Requester.HTTP_MIN_SERVER
        );
        return createMinHttpServer(configuration, networkSharedPool);
    }

    @Nullable
    public HttpServer createMinHttpServer(HttpServerConfiguration configuration, WorkerPool workerPool) {
        if (!configuration.isEnabled()) {
            return null;
        }

        final HttpServer server = new HttpServer(
                configuration,
                workerPool,
                configuration.getFactoryProvider().getHttpMinSocketFactory()
        );
        Metrics metrics = configuration.getHttpContextConfiguration().getMetrics();
        server.bind(
                new HttpRequestHandlerFactory() {
                    @Override
                    public ObjHashSet<String> getUrls() {
                        return configuration.getContextPathStatus();
                    }

                    @Override
                    public HttpRequestHandler newInstance() {
                        return new HealthCheckProcessor(configuration);
                    }
                },
                true
        );

        if (metrics.isEnabled()) {
            final PrometheusMetricsProcessor.RequestStatePool pool = new PrometheusMetricsProcessor.RequestStatePool(
                    configuration.getWorkerCount()
            );
            server.registerClosable(pool);
            server.bind(
                    new HttpRequestHandlerFactory() {
                        @Override
                        public ObjHashSet<String> getUrls() {
                            return configuration.getContextPathMetrics();
                        }

                        @Override
                        public HttpRequestHandler newInstance() {
                            return new PrometheusMetricsProcessor(metrics, configuration, pool);
                        }
                    }
            );
        }
        return server;
    }

    public PGServer createPGWireServer(
            PGConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager
    ) {
        return createPGWireServer(configuration, cairoEngine, workerPoolManager, new AtomicBoolean(true));
    }

    public PGServer createPGWireServer(
            PGConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager,
            AtomicBoolean acceptOpen
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        // The pool is:
        // - DEDICATED when PropertyKey.PG_WORKER_COUNT is > 0
        // - SHARED otherwise
        final WorkerPool networkSharedPool = workerPoolManager.getSharedPoolNetwork(
                configuration,
                Requester.PG_WIRE_SERVER
        );

        PGCircuitBreakerRegistry registry = configuration.getDumpNetworkTraffic() ? PGHexTestsCircuitBreakRegistry.INSTANCE :
                new DefaultPGCircuitBreakerRegistry(configuration, cairoEngine.getConfiguration());

        return new PGServer(
                configuration,
                cairoEngine,
                networkSharedPool,
                registry,
                () -> new SqlExecutionContextImpl(
                        cairoEngine,
                        workerPoolManager.getSharedQueryWorkerCount()
                ),
                acceptOpen);
    }

    public QwpUdpReceiver createQwpUdpReceiver(
            QwpUdpReceiverConfiguration config,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager
    ) {
        return createQwpUdpReceiver(config, cairoEngine, workerPoolManager, new AtomicBoolean(true));
    }

    public QwpUdpReceiver createQwpUdpReceiver(
            QwpUdpReceiverConfiguration config,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager,
            AtomicBoolean acceptOpen
    ) {
        if (!config.isEnabled()) {
            return null;
        }
        WorkerPool workerPool = workerPoolManager.getSharedPoolNetwork();
        QwpUdpReceiver receiver;
        if (Os.isLinux()) {
            receiver = new LinuxMMQwpUdpReceiver(config, cairoEngine, workerPool, acceptOpen);
        } else {
            receiver = new QwpUdpReceiver(config, cairoEngine, workerPool, acceptOpen);
        }
        try {
            receiver.start();
        } catch (Throwable th) {
            Misc.free(receiver);
            throw th;
        }
        return receiver;
    }
}
