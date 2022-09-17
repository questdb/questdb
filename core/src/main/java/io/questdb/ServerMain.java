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

package io.questdb;

import io.questdb.cairo.*;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.text.TextImportJob;
import io.questdb.cutlass.text.TextImportRequestJob;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolManager;
import io.questdb.std.*;

import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerMain implements QuietCloseable {
    private final PropServerConfiguration config;
    private final Log log;
    private final ObjList<QuietCloseable> toBeClosed = new ObjList<>();
    private final AtomicBoolean hasStarted = new AtomicBoolean();
    private final WorkerPoolManager workerPoolManager;
    private final CairoEngine engine;

    public ServerMain(String... args) throws SqlException {
        this(Bootstrap.withArgs(args));
    }

    public ServerMain(Bootstrap bootstrap) throws SqlException {
        this(bootstrap.getConfiguration(), bootstrap.getMetrics(), bootstrap.getLog());
    }

    public ServerMain(PropServerConfiguration config, Metrics metrics, Log log) throws SqlException {
        this.config = config;
        this.log = log;

        // create cairo engine
        final CairoConfiguration cairoConfig = config.getCairoConfiguration();
        engine = new CairoEngine(cairoConfig, metrics);
        toBeClosed.add(engine);

        // setup shared worker pool, plus dedicated pools
        final FunctionFactoryCache ffCache = new FunctionFactoryCache(
                cairoConfig,
                ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
        );
        final WorkerPool sharedPool = new WorkerPool(config.getWorkerPoolConfiguration(), metrics);
        O3Utils.setupWorkerPool(sharedPool, engine, config.getCairoConfiguration().getCircuitBreakerConfiguration(), ffCache);
        workerPoolManager = new WorkerPoolManager(sharedPool);

        // snapshots
        final DatabaseSnapshotAgent snapshotAgent = new DatabaseSnapshotAgent(engine);
        toBeClosed.add(snapshotAgent);

        // text import
        TextImportJob.assignToPool(engine.getMessageBus(), sharedPool);
        if (cairoConfig.getSqlCopyInputRoot() != null) {
            final TextImportRequestJob textImportRequestJob = new TextImportRequestJob(
                    engine,
                    // save CPU resources for collecting and processing jobs
                    Math.max(1, sharedPool.getWorkerCount() - 2),
                    ffCache
            );
            sharedPool.assign(textImportRequestJob);
            sharedPool.freeOnHalt(textImportRequestJob);
        }

        // http
        toBeClosed.add(Services.createHttpServer(
                config.getHttpServerConfiguration(),
                engine,
                workerPoolManager,
                ffCache,
                snapshotAgent,
                metrics
        ));

        // http min
        toBeClosed.add(Services.createMinHttpServer(
                config.getHttpMinServerConfiguration(),
                engine,
                workerPoolManager,
                metrics
        ));

        // pg wire
        if (config.getPGWireConfiguration().isEnabled()) {
            toBeClosed.add(Services.createPGWireServer(
                    config.getPGWireConfiguration(),
                    engine,
                    workerPoolManager,
                    ffCache,
                    snapshotAgent,
                    metrics
            ));
        }

        // ilp/tcp
        toBeClosed.add(Services.createLineTcpReceiver(
                config.getLineTcpReceiverConfiguration(),
                engine,
                workerPoolManager,
                metrics
        ));

        // ilp/udp
        toBeClosed.add(Services.createLineUdpReceiver(
                config.getLineUdpReceiverConfiguration(),
                engine,
                sharedPool
        ));

        // telemetry
        if (!cairoConfig.getTelemetryConfiguration().getDisableCompletely()) {
            final TelemetryJob telemetryJob = new TelemetryJob(engine, ffCache);
            toBeClosed.add(telemetryJob);
            if (cairoConfig.getTelemetryConfiguration().getEnabled()) {
                sharedPool.assign(telemetryJob);
            }
        }

        System.gc(); // GC 1
        log.advisoryW().$("Bootstrap complete, ready to start").$();
    }

    public void start() {
        start(false);
    }

    public void start(boolean addShutdownHook) {
        if (hasStarted.compareAndSet(false, true)) {
            if (addShutdownHook) {
                addShutdownHook();
            }
            workerPoolManager.startAll(log); // starts QuestDB's workers
            Bootstrap.logWebConsoleUrls(config, log);
            System.gc(); // final GC
            log.advisoryW().$("enjoy").$();
        }
    }

    @Override
    public void close() {
        if (hasStarted.compareAndSet(true, false)) {
            ShutdownFlag.INSTANCE.shutdown();
            workerPoolManager.closeAll();
            LogFactory.INSTANCE.flushJobsAndClose();
            Misc.freeObjListAndClear(toBeClosed);
        }
    }

    public PropServerConfiguration getConfiguration() {
        return config;
    }

    public CairoEngine getCairoEngine() {
        return engine;
    }

    public WorkerPoolManager getWorkerPoolManager() {
        return workerPoolManager;
    }

    public boolean hasStarted() {
        return hasStarted.get();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.err.println("QuestDB is shutting down...");
                close();
            } catch (Error ignore) {
                // ignore
            } finally {
                System.err.println("QuestDB is shutdown.");
            }
        }));
    }

    public static void main(String[] args) throws Exception {
        try {
            new ServerMain(args).start(true);
        } catch (Throwable thr) {
            thr.printStackTrace();
            System.exit(55);
        }
    }
}
