/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpHeaderParserFactory;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.processors.HealthCheckProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.LineHttpProcessorImpl;
import io.questdb.cutlass.http.processors.PrometheusMetricsProcessor;
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
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import org.jetbrains.annotations.Nullable;

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
                workerPoolManager.getSharedQueryWorkerCount()
        );
    }

    @Nullable
    public HttpServer createHttpServer(
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            WorkerPool networkSharedPool,
            int sharedQueryWorkerCount
    ) {
        final HttpFullFatServerConfiguration httpServerConfiguration = serverConfiguration.getHttpServerConfiguration();
        if (!httpServerConfiguration.isEnabled()) {
            return null;
        }

        final HttpSessionStore sessionStore = serverConfiguration.getFactoryProvider().getHttpSessionStore();
        final HttpCookieHandler cookieHandler = serverConfiguration.getFactoryProvider().getHttpCookieHandler();
        final HttpHeaderParserFactory headerParserFactory = serverConfiguration.getFactoryProvider().getHttpHeaderParserFactory();
        final HttpServer server = new HttpServer(
                httpServerConfiguration,
                networkSharedPool,
                serverConfiguration.getFactoryProvider().getHttpSocketFactory(),
                cookieHandler,
                sessionStore,
                headerParserFactory
        );
        HttpServer.HttpRequestHandlerBuilder jsonQueryProcessorBuilder = () -> new JsonQueryProcessor(
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
                ilpV2WriteProcessorBuilder
        );
        return server;
    }

    @Nullable
    public LineTcpReceiver createLineTcpReceiver(
            LineTcpReceiverConfiguration config,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager
    ) {
        if (!config.isEnabled()) {
            return null;
        }

        // The ioPool is:
        // - DEDICATED when PropertyKey.LINE_TCP_IO_WORKER_COUNT is > 0
        // - DEDICATED (2 worker) when ^ ^ is not set and host has 8 < cpus < 17
        // - DEDICATED (6 worker) when ^ ^ is not set and host has > 16 cpus
        // - SHARED otherwise

        // The sharedPoolWrite is:
        // - DEDICATED when PropertyKey.LINE_TCP_WRITER_WORKER_COUNT is > 0
        // - DEDICATED (1 worker) when ^ ^ is not set
        // - SHARED otherwise

        final WorkerPool sharedPoolNetwork = workerPoolManager.getSharedPoolNetwork(
                config.getNetworkWorkerPoolConfiguration(),
                Requester.LINE_TCP_IO
        );
        final WorkerPool sharedPoolWrite = workerPoolManager.getSharedPoolWrite(
                config.getWriterWorkerPoolConfiguration(),
                Requester.LINE_TCP_WRITER
        );
        return new LineTcpReceiver(config, cairoEngine, sharedPoolNetwork, sharedPoolWrite);
    }

    @Nullable
    public AbstractLineProtoUdpReceiver createLineUdpReceiver(
            LineUdpReceiverConfiguration config,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager
    ) {
        if (!config.isEnabled()) {
            return null;
        }

        // The pool is always the SHARED pool
        if (Os.isLinux()) {
            return new LinuxMMLineUdpReceiver(config, cairoEngine, workerPoolManager.getSharedPoolNetwork());
        }
        return new LineUdpReceiver(config, cairoEngine, workerPoolManager.getSharedPoolNetwork());
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

        final HttpServer server = new HttpServer(configuration, workerPool, configuration.getFactoryProvider().getHttpMinSocketFactory());
        Metrics metrics = configuration.getHttpContextConfiguration().getMetrics();
        server.bind(
                new HttpRequestHandlerFactory() {
                    @Override
                    public ObjList<String> getUrls() {
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
                        public ObjList<String> getUrls() {
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
                ));
    }
}
