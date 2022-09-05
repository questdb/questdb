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
import io.questdb.griffin.engine.groupby.vect.GroupByJob;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
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
    private final AtomicBoolean isWorking = new AtomicBoolean();

    public ServerMain(String... args) throws SqlException {
        this(Bootstrap.withArgs(args));
    }

    public ServerMain(Bootstrap bootstrap) throws SqlException {
        this(bootstrap.getConfig(), bootstrap.getMetrics(), bootstrap.getLog());
    }

    public ServerMain(PropServerConfiguration config, Metrics metrics, Log log) throws SqlException {
        this.config = config;
        this.log = log;

        // create cairo engine
        final CairoConfiguration cairoConfig = config.getCairoConfiguration();
        final CairoEngine cairoEngine = new CairoEngine(cairoConfig, metrics);
        toBeClosed.add(cairoEngine);

        // setup shared worker pool
        final FunctionFactoryCache ffCache = new FunctionFactoryCache(
                cairoConfig,
                ServiceLoader.load(
                        FunctionFactory.class, FunctionFactory.class.getClassLoader()
                )
        );
        final WorkerPool sharedPool = WorkerPoolManager.createUnmanaged(
                config.getWorkerPoolConfiguration(), metrics
        ).configure(cairoEngine, ffCache, true, true);
        WorkerPoolManager.setSharedInstance(sharedPool);

        // snapshots
        final DatabaseSnapshotAgent snapshotAgent = new DatabaseSnapshotAgent(cairoEngine);
        toBeClosed.add(snapshotAgent);

        // Register jobs that help parallel execution of queries and column indexing.
        sharedPool.assign(new ColumnIndexerJob(cairoEngine.getMessageBus()));
        sharedPool.assign(new GroupByJob(cairoEngine.getMessageBus()));
        sharedPool.assign(new LatestByAllIndexedJob(cairoEngine.getMessageBus()));

        // text import
        TextImportJob.assignToPool(cairoEngine.getMessageBus(), sharedPool);
        if (cairoConfig.getSqlCopyInputRoot() != null) {
            final TextImportRequestJob textImportRequestJob = new TextImportRequestJob(
                    cairoEngine,
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
                sharedPool,
                log,
                cairoEngine,
                ffCache,
                snapshotAgent,
                metrics
        ));

        // http min
        toBeClosed.add(Services.createMinHttpServer(
                config.getHttpMinServerConfiguration(),
                sharedPool,
                log,
                cairoEngine,
                ffCache,
                snapshotAgent,
                metrics
        ));

        // pg-wire
        if (config.getPGWireConfiguration().isEnabled()) {
            toBeClosed.add(Services.createPGWireServer(
                    config.getPGWireConfiguration(),
                    sharedPool,
                    log,
                    cairoEngine,
                    ffCache,
                    snapshotAgent,
                    metrics
            ));
        }

        // ilp/tcp
        toBeClosed.add(Services.createLineTcpReceiver(
                config.getLineTcpReceiverConfiguration(),
                sharedPool,
                log,
                cairoEngine,
                metrics
        ));

        // ilp/udp
        toBeClosed.add(Services.createLineUdpReceiver(
                config.getLineUdpReceiverConfiguration(),
                sharedPool,
                log,
                cairoEngine,
                metrics
        ));

        // telemetry
        if (!cairoConfig.getTelemetryConfiguration().getDisableCompletely()) {
            final TelemetryJob telemetryJob = new TelemetryJob(cairoEngine, ffCache);
            toBeClosed.add(telemetryJob);
            if (cairoConfig.getTelemetryConfiguration().getEnabled()) {
                sharedPool.assign(telemetryJob);
            }
        }

        System.gc(); // GC 1
        log.advisoryW().$("Bootstrap complete").$();
    }

    public void start() {
        start(false);
    }

    public void start(boolean addShutdownHook) {
        if (isWorking.compareAndSet(false, true)) {
            if (addShutdownHook) {
                addShutdownHook();
            }
            log.advisoryW().$("QuestDB is starting...").$();
            WorkerPoolManager.startAll(log); // starts QuestDB's workers
            Bootstrap.logWebConsoleUrls(config, log);
            System.gc(); // final GC
            log.advisoryW().$("QuestDB is running").$();
            log.advisoryW().$("enjoy").$();
        }
    }

    @Override
    public void close() {
        if (isWorking.compareAndSet(true, false)) {
            WorkerPoolManager.closeAll();
            ShutdownFlag.INSTANCE.shutdown();
            Misc.freeObjList(toBeClosed);
            toBeClosed.clear();
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.err.println("QuestDB is shutting down...");
                close();
            } catch (Error ignore) {
                // ignore
            } finally {
                LogFactory.INSTANCE.flushJobsAndClose();
                System.err.println("QuestDB is shutdown.");
            }
        }));
    }

    public static void main(String[] args) throws Exception {
        try {
            new ServerMain(args).start(true);
        } catch (Throwable thr) {
            System.err.println(thr.getMessage());
            LogFactory.INSTANCE.flushJobsAndClose();
            thr.printStackTrace();
            System.exit(55);
        }
    }
}
