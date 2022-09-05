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
import io.questdb.cutlass.http.HttpMinServerConfiguration;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.HttpServerConfiguration;
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
import io.questdb.log.Log;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolManager;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public final class Services {

    @Nullable
    public static HttpServer createHttpServer(
            HttpServerConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine,
            @Nullable FunctionFactoryCache functionFactoryCache,
            @Nullable DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        return Services.createService(
                configuration,
                sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                HttpServer::create0,
                functionFactoryCache,
                snapshotAgent,
                metrics
        );
    }

    @Nullable
    public static HttpServer createMinHttpServer(
            HttpMinServerConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine,
            @Nullable FunctionFactoryCache functionFactoryCache,
            @Nullable DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        return Services.createService(
                configuration,
                sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                HttpServer::createMinHttpServer,
                functionFactoryCache,
                snapshotAgent,
                metrics
        );
    }

    @Nullable
    public static PGWireServer createPGWireServer(
            PGWireConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log log,
            CairoEngine cairoEngine,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics,
            PGWireServer.PGConnectionContextFactory contextFactory
    ) {
        return Services.createService(
                configuration,
                sharedWorkerPool,
                log,
                cairoEngine,
                (conf, engine, workerPool, local, sharedWorkerCount, functionFactoryCache1, snapshotAgent1, metrics1) -> new PGWireServer(
                        conf, engine, workerPool, local, functionFactoryCache1, snapshotAgent1, contextFactory
                ),
                functionFactoryCache,
                snapshotAgent,
                metrics
        );
    }

    @Nullable
    public static PGWireServer createPGWireServer(
            PGWireConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log log,
            CairoEngine cairoEngine,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        return Services.createService(
                configuration,
                sharedWorkerPool,
                log,
                cairoEngine,
                (conf, engine, workerPool, local, sharedWorkerCount, cache, agent, m) -> new PGWireServer(
                        conf,
                        engine,
                        workerPool,
                        local,
                        cache,
                        agent,
                        new PGWireServer.PGConnectionContextFactory(
                                engine,
                                conf,
                                workerPool.getWorkerCount(),
                                sharedWorkerCount
                        )
                ),
                functionFactoryCache,
                snapshotAgent,
                metrics
        );
    }

    @Nullable
    public static LineTcpReceiver createLineTcpReceiver(
            LineTcpReceiverConfiguration lineConfiguration,
            WorkerPool sharedWorkerPool,
            Log log,
            CairoEngine cairoEngine,
            Metrics metrics
    ) {
        if (!lineConfiguration.isEnabled()) {
            return null;
        }

        ObjList<WorkerPool> dedicatedPools = new ObjList<>(2);
        WorkerPool ioWorkerPool = WorkerPoolManager.getInstance(lineConfiguration.getIOWorkerPoolConfiguration(), metrics);
        WorkerPool writerWorkerPool = WorkerPoolManager.getInstance(lineConfiguration.getWriterWorkerPoolConfiguration(), metrics);
        if (ioWorkerPool != sharedWorkerPool) {
            dedicatedPools.add(ioWorkerPool);
        }
        if (writerWorkerPool != sharedWorkerPool) {
            dedicatedPools.add(writerWorkerPool);
        }
        return new LineTcpReceiver(lineConfiguration, cairoEngine, ioWorkerPool, writerWorkerPool, dedicatedPools);
    }

    @Nullable
    public static AbstractLineProtoUdpReceiver createLineUdpReceiver(
            LineUdpReceiverConfiguration lineConfiguration,
            WorkerPool sharedPool,
            Log log,
            CairoEngine cairoEngine,
            Metrics metrics
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


    public interface UdpReceiverFactory extends Services.ServerFactory<AbstractLineProtoUdpReceiver, LineUdpReceiverConfiguration> {
    }

    @Nullable
    private static <T extends QuietCloseable, C extends WorkerPoolConfiguration> T createService(
            C config,
            WorkerPool sharedPool,
            Log log,
            CairoEngine cairoEngine,
            ServerFactory<T, C> factory,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        final T server;
        if (config.isEnabled()) {
            boolean local = false;
            WorkerPool localPool;
            int workerCount = config.getWorkerCount();
            if (workerCount < 1 && sharedPool != null) {
                localPool = sharedPool;
            } else {
                localPool = WorkerPoolManager.getInstance(config, metrics);
                local = true;
            }

            final int sharedWorkerCount = sharedPool == null ? localPool.getWorkerCount() : sharedPool.getWorkerCount();
            server = factory.create(
                    config,
                    cairoEngine,
                    localPool,
                    local,
                    sharedWorkerCount,
                    functionFactoryCache,
                    snapshotAgent,
                    metrics
            );
            return server;
        }
        return null;
    }

    @TestOnly
    @FunctionalInterface
    public interface ServerFactory<T extends QuietCloseable, C> {
        T create(
                C configuration,
                CairoEngine engine,
                WorkerPool workerPool,
                boolean isWorkerPoolLocal,
                int sharedWorkerCount,
                @Nullable FunctionFactoryCache functionFactoryCache,
                @Nullable DatabaseSnapshotAgent snapshotAgent,
                Metrics metrics
        );
    }

    private Services() {
        throw new UnsupportedOperationException("not instantiatable");
    }
}
