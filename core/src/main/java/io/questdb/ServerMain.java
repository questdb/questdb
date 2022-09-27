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
import io.questdb.griffin.engine.groupby.vect.GroupByJob;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolManager;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerMain implements Closeable {
    private final PropServerConfiguration config;
    private final Log log;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ObjList<Closeable> toBeClosed = new ObjList<>();
    private final WorkerPoolManager workerPoolManager;
    private final CairoEngine engine;
    private final FunctionFactoryCache ffCache;

    public ServerMain(String... args) {
        this(Bootstrap.withArgs(args));
    }

    public ServerMain(final Bootstrap bootstrap) {
        this(bootstrap.getConfiguration(), bootstrap.getMetrics(), bootstrap.getLog());
    }

    public ServerMain(final PropServerConfiguration config, final Metrics metrics, final Log log) {
        this.config = config;
        this.log = log;

        // create cairo engine
        final CairoConfiguration cairoConfig = config.getCairoConfiguration();
        engine = new CairoEngine(cairoConfig, metrics);
        toBeClosed(engine);

        // create function factory cache
        ffCache = new FunctionFactoryCache(
                cairoConfig,
                ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
        );

        // create the worker pool manager, and configure the shared pool
        workerPoolManager = new WorkerPoolManager(config, metrics) {
            @Override
            protected void configureSharedPool(WorkerPool sharedPool) {
                try {
                    sharedPool.assign(engine.getEngineMaintenanceJob());
                    sharedPool.assignCleaner(Path.CLEANER);
                    O3Utils.setupWorkerPool(
                            sharedPool,
                            engine,
                            config.getCairoConfiguration().getCircuitBreakerConfiguration(),
                            ffCache
                    );
                    final MessageBus messageBus = engine.getMessageBus();

                    // register jobs that help parallel execution of queries and column indexing.
                    sharedPool.assign(new ColumnIndexerJob(messageBus));
                    sharedPool.assign(new GroupByJob(messageBus));
                    sharedPool.assign(new LatestByAllIndexedJob(messageBus));

                    // text import
                    TextImportJob.assignToPool(messageBus, sharedPool);
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

                    // telemetry
                    if (!cairoConfig.getTelemetryConfiguration().getDisableCompletely()) {
                        final TelemetryJob telemetryJob = new TelemetryJob(engine, ffCache);
                        toBeClosed.add(telemetryJob);
                        if (cairoConfig.getTelemetryConfiguration().getEnabled()) {
                            sharedPool.assign(telemetryJob);
                        }
                    }
                } catch (Throwable thr) {
                    throw new Bootstrap.BootstrapException(thr);
                }
            }
        };

        // snapshots
        final DatabaseSnapshotAgent snapshotAgent = new DatabaseSnapshotAgent(engine);
        toBeClosed(snapshotAgent);

        // http
        toBeClosed(Services.createHttpServer(
                config.getHttpServerConfiguration(),
                engine,
                workerPoolManager,
                ffCache,
                snapshotAgent,
                metrics
        ));

        // http min
        toBeClosed(Services.createMinHttpServer(
                config.getHttpMinServerConfiguration(),
                engine,
                workerPoolManager,
                metrics
        ));

        // pg wire
        toBeClosed(Services.createPGWireServer(
                config.getPGWireConfiguration(),
                engine,
                workerPoolManager,
                ffCache,
                snapshotAgent,
                metrics
        ));

        // ilp/tcp
        toBeClosed(Services.createLineTcpReceiver(
                config.getLineTcpReceiverConfiguration(),
                engine,
                workerPoolManager,
                metrics
        ));

        // ilp/udp
        toBeClosed(Services.createLineUdpReceiver(
                config.getLineUdpReceiverConfiguration(),
                engine,
                workerPoolManager
        ));

        System.gc(); // GC 1
        log.advisoryW().$("bootstrap complete").$();
    }

    public void start() {
        start(false);
    }

    public void start(boolean addShutdownHook) {
        if (!closed.get() && running.compareAndSet(false, true)) {
            if (addShutdownHook) {
                addShutdownHook();
            }
            workerPoolManager.start(log);
            Bootstrap.logWebConsoleUrls(config, log);
            System.gc(); // final GC
            log.advisoryW().$("enjoy").$();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            ShutdownFlag.INSTANCE.shutdown();
            workerPoolManager.halt();
            Misc.freeObjListAndClear(toBeClosed);
            // leave hasStarted as is, to disable start
        }
        if (!running.get()) {
            // if you instantiate ServerMain it is for the purpose of running, i.e. calling start
            throw new IllegalStateException("start was not called at all");
        }
    }

    public PropServerConfiguration getConfiguration() {
        return config;
    }

    public CairoEngine getCairoEngine() {
        if (closed.get()) {
            throw new IllegalStateException("close was called");
        }
        return engine;
    }

    public WorkerPoolManager getWorkerPoolManager() {
        if (closed.get()) {
            throw new IllegalStateException("close was called");
        }
        return workerPoolManager;
    }

    public boolean hasStarted() {
        return running.get();
    }

    public boolean hasBeenClosed() {
        return closed.get();
    }

    private void toBeClosed(Closeable closeable) {
        if (closeable != null) {
            toBeClosed.add(closeable);
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.err.println("QuestDB is shutting down...");
                System.err.println("Pre-touch magic number: " + AsyncFilterAtom.PRE_TOUCH_BLACKHOLE.sum());
                close();
                LogFactory.getInstance().close(true);
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
