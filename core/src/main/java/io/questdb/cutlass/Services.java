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
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.processors.HealthCheckProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.LineHttpProcessor;
import io.questdb.cutlass.http.processors.PrometheusMetricsProcessor;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LinuxMMLineUdpReceiver;
import io.questdb.cutlass.pgwire.CircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
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
            WorkerPoolManager workerPoolManager,
            Metrics metrics
    ) {
        HttpServerConfiguration httpServerConfiguration = serverConfiguration.getHttpServerConfiguration();
        if (!httpServerConfiguration.isEnabled()) {
            return null;
        }

        // The pool is:
        // - DEDICATED when PropertyKey.HTTP_WORKER_COUNT is > 0
        // - SHARED otherwise
        return createHttpServer(
                serverConfiguration,
                cairoEngine,
                workerPoolManager.getInstance(httpServerConfiguration, metrics, Requester.HTTP_SERVER),
                workerPoolManager.getSharedWorkerCount(),
                metrics
        );
    }

    @Nullable
    public HttpServer createHttpServer(
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            int sharedWorkerCount,
            Metrics metrics
    ) {
        final HttpServerConfiguration httpServerConfiguration = serverConfiguration.getHttpServerConfiguration();
        if (!httpServerConfiguration.isEnabled()) {
            return null;
        }

        final HttpCookieHandler cookieHandler = serverConfiguration.getFactoryProvider().getHttpCookieHandler();
        final HttpHeaderParserFactory headerParserFactory = serverConfiguration.getFactoryProvider().getHttpHeaderParserFactory();
        final HttpServer server = new HttpServer(
                httpServerConfiguration,
                metrics,
                workerPool,
                serverConfiguration.getFactoryProvider().getHttpSocketFactory(),
                cookieHandler,
                headerParserFactory
        );
        HttpServer.HttpRequestProcessorBuilder jsonQueryProcessorBuilder = () -> new JsonQueryProcessor(
                httpServerConfiguration.getJsonQueryProcessorConfiguration(),
                cairoEngine,
                workerPool.getWorkerCount(),
                sharedWorkerCount
        );

        HttpServer.HttpRequestProcessorBuilder ilpV2WriteProcessorBuilder = () -> new LineHttpProcessor(
                cairoEngine,
                httpServerConfiguration.getHttpContextConfiguration().getRecvBufferSize(),
                httpServerConfiguration.getHttpContextConfiguration().getSendBufferSize(),
                httpServerConfiguration.getLineHttpProcessorConfiguration()
        );

        HttpServer.addDefaultEndpoints(
                server,
                serverConfiguration,
                cairoEngine,
                workerPool,
                sharedWorkerCount,
                jsonQueryProcessorBuilder,
                ilpV2WriteProcessorBuilder
        );
        return server;
    }

    @Nullable
    public LineTcpReceiver createLineTcpReceiver(
            LineTcpReceiverConfiguration config,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager,
            Metrics metrics
    ) {
        if (!config.isEnabled()) {
            return null;
        }

        // The ioPool is:
        // - DEDICATED when PropertyKey.LINE_TCP_IO_WORKER_COUNT is > 0
        // - DEDICATED (2 worker) when ^ ^ is not set and host has 8 < cpus < 17
        // - DEDICATED (6 worker) when ^ ^ is not set and host has > 16 cpus
        // - SHARED otherwise

        // The writerPool is:
        // - DEDICATED when PropertyKey.LINE_TCP_WRITER_WORKER_COUNT is > 0
        // - DEDICATED (1 worker) when ^ ^ is not set
        // - SHARED otherwise

        final WorkerPool ioPool = workerPoolManager.getInstance(
                config.getIOWorkerPoolConfiguration(),
                metrics,
                Requester.LINE_TCP_IO
        );
        final WorkerPool writerPool = workerPoolManager.getInstance(
                config.getWriterWorkerPoolConfiguration(),
                metrics,
                Requester.LINE_TCP_WRITER
        );
        return new LineTcpReceiver(config, cairoEngine, ioPool, writerPool);
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
            return new LinuxMMLineUdpReceiver(config, cairoEngine, workerPoolManager.getSharedPool());
        }
        return new LineUdpReceiver(config, cairoEngine, workerPoolManager.getSharedPool());
    }

    @Nullable
    public HttpServer createMinHttpServer(
            HttpMinServerConfiguration configuration,
            WorkerPoolManager workerPoolManager,
            Metrics metrics
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        // The pool is:
        // - SHARED if PropertyKey.HTTP_MIN_WORKER_COUNT (http.min.worker.count) <= 0
        // - DEDICATED (1 worker) otherwise
        final WorkerPool workerPool = workerPoolManager.getInstance(
                configuration,
                metrics,
                Requester.HTTP_MIN_SERVER
        );
        return createMinHttpServer(configuration, workerPool, metrics);
    }

    @Nullable
    public HttpServer createMinHttpServer(HttpMinServerConfiguration configuration, WorkerPool workerPool, Metrics metrics) {
        if (!configuration.isEnabled()) {
            return null;
        }

        final HttpServer server = new HttpServer(configuration, metrics, workerPool, configuration.getFactoryProvider().getHttpMinSocketFactory());
        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return metrics.isEnabled() ? "/status" : "*";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new HealthCheckProcessor(configuration);
            }
        }, true);
        if (metrics.isEnabled()) {
            final PrometheusMetricsProcessor.RequestStatePool pool = new PrometheusMetricsProcessor.RequestStatePool(
                    configuration.getWorkerCount()
            );
            server.registerClosable(pool);
            server.bind(new HttpRequestProcessorFactory() {
                @Override
                public String getUrl() {
                    return "/metrics";
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return new PrometheusMetricsProcessor(metrics, configuration, pool);
                }
            });
        }
        return server;
    }

    @Nullable
    public PGWireServer createPGWireServer(
            PGWireConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager,
            Metrics metrics
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        // The pool is:
        // - DEDICATED when PropertyKey.PG_WORKER_COUNT is > 0
        // - SHARED otherwise
        final WorkerPool workerPool = workerPoolManager.getInstance(
                configuration,
                metrics,
                Requester.PG_WIRE_SERVER
        );

        CircuitBreakerRegistry registry = new CircuitBreakerRegistry(configuration, cairoEngine.getConfiguration());

        return new PGWireServer(
                configuration,
                cairoEngine,
                workerPool,
                registry,
                () -> new SqlExecutionContextImpl(
                        cairoEngine,
                        workerPool.getWorkerCount(),
                        workerPoolManager.getSharedWorkerCount()
                )
        );
    }
}
