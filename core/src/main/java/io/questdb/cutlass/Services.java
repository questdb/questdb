/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.processors.HealthCheckProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.PrometheusMetricsProcessor;
import io.questdb.cutlass.http.processors.QueryCache;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LinuxMMLineUdpReceiver;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolManager;
import io.questdb.std.Os;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public final class Services {

    @Nullable
    public static HttpServer createHttpServer(
            HttpServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager,
            @Nullable FunctionFactoryCache functionFactoryCache,
            @Nullable DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        WorkerPool workerPool = workerPoolManager.getInstance(configuration, metrics);
        return createHttpServer(
                configuration,
                cairoEngine,
                workerPool,
                workerPoolManager.hasSharedPool() ? workerPoolManager.getSharedWorkerCount() : workerPool.getWorkerCount(),
                functionFactoryCache,
                snapshotAgent,
                metrics
        );
    }

    @Nullable
    public static HttpServer createHttpServer(
            HttpServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            int sharedWorkerCount,
            @Nullable FunctionFactoryCache functionFactoryCache,
            @Nullable DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        final HttpServer server = new HttpServer(configuration, cairoEngine.getMessageBus(), metrics, workerPool);
        QueryCache.configure(configuration, metrics);
        HttpServer.HttpRequestProcessorBuilder jsonQueryProcessorBuilder = () -> new JsonQueryProcessor(
                configuration.getJsonQueryProcessorConfiguration(),
                cairoEngine,
                workerPool.getWorkerCount(),
                sharedWorkerCount,
                functionFactoryCache,
                snapshotAgent);
        HttpServer.addDefaultEndpoints(
                server,
                configuration,
                cairoEngine,
                workerPool,
                sharedWorkerCount,
                jsonQueryProcessorBuilder,
                functionFactoryCache,
                snapshotAgent
        );
        return server;
    }

    @Nullable
    public static HttpServer createMinHttpServer(
            HttpMinServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPoolManager workerPoolManager,
            Metrics metrics
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        final WorkerPool workerPool = workerPoolManager.getInstance(configuration, metrics);
        return createMinHttpServer(configuration, cairoEngine, workerPool, metrics);
    }

    @Nullable
    public static HttpServer createMinHttpServer(
            HttpMinServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            Metrics metrics
    ) {
        if (configuration.isEnabled()) {
        }

        final HttpServer server = new HttpServer(configuration, cairoEngine.getMessageBus(), metrics, workerPool);
        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new HealthCheckProcessor();
            }

            @Override
            public String getUrl() {
                return metrics.isEnabled() ? "/status" : "*";
            }
        }, true);
        if (metrics.isEnabled()) {
            server.bind(new HttpRequestProcessorFactory() {
                @Override
                public HttpRequestProcessor newInstance() {
                    return new PrometheusMetricsProcessor(metrics);
                }

                @Override
                public String getUrl() {
                    return "/metrics";
                }
            });
        }
        return server;
    }

    @Nullable
    public static PGWireServer createPGWireServer(
            PGWireConfiguration configuration,
            WorkerPoolManager workerPoolManager,
            CairoEngine cairoEngine,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics,
            PGWireServer.PGConnectionContextFactory contextFactory
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        final WorkerPool workerPool = workerPoolManager.getInstance(configuration, metrics);
        return new PGWireServer(configuration, cairoEngine, workerPool, functionFactoryCache, snapshotAgent, contextFactory);
    }

    @Nullable
    public static PGWireServer createPGWireServer(
            PGWireConfiguration configuration,
            WorkerPoolManager workerPoolManager,
            CairoEngine cairoEngine,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        final WorkerPool workerPool = workerPoolManager.getInstance(configuration, metrics);
        final int sharedWorkerCount = workerPoolManager.hasSharedPool() ? workerPoolManager.getSharedWorkerCount() : workerPool.getWorkerCount();
        return new PGWireServer(
                configuration,
                cairoEngine,
                workerPool,
                functionFactoryCache,
                snapshotAgent,
                new PGWireServer.PGConnectionContextFactory(
                        cairoEngine,
                        configuration,
                        () -> new SqlExecutionContextImpl(cairoEngine, workerPool.getWorkerCount(), sharedWorkerCount)
                )
        );
    }

    @Nullable
    public static LineTcpReceiver createLineTcpReceiver(
            LineTcpReceiverConfiguration configuration,
            WorkerPoolManager workerPoolManager,
            CairoEngine cairoEngine,
            Metrics metrics
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        final WorkerPool ioPool = workerPoolManager.getInstance(configuration.getIOWorkerPoolConfiguration(), metrics);
        final WorkerPool writerPool = workerPoolManager.getInstance(configuration.getWriterWorkerPoolConfiguration(), metrics);
        return new LineTcpReceiver(configuration, cairoEngine, ioPool, writerPool);
    }

    @Nullable
    public static AbstractLineProtoUdpReceiver createLineUdpReceiver(
            LineUdpReceiverConfiguration lineConfiguration,
            WorkerPool sharedPool,
            CairoEngine cairoEngine
    ) {
        if (!lineConfiguration.isEnabled()) {
            return null;
        }

        if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
            return new LinuxMMLineUdpReceiver(
                    lineConfiguration,
                    cairoEngine,
                    sharedPool
            );
        }
        return new LineUdpReceiver(
                lineConfiguration,
                cairoEngine,
                sharedPool
        );
    }

    private Services() {
        throw new UnsupportedOperationException("not instantiatable");
    }
}
