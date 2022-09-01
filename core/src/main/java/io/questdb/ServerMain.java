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
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiver;
import io.questdb.cutlass.line.udp.LinuxMMLineUdpReceiver;
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
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerMain implements Lifecycle {

    private final PropServerConfiguration config;
    private final Log log;
    private final WorkerPool workerPool;
    private final ObjList<Lifecycle> workers = new ObjList<>();
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
        this.workerPool = initWorkers(workers, config, metrics, log);
        System.gc(); // GC 1
        log.advisoryW().$("Bootstrap complete").$();
    }

    @Override
    public void start() {
        start(false);
    }

    public void start(boolean addShutdownHook) {
        if (isWorking.compareAndSet(false, true)) {
            if (addShutdownHook) {
                addShutdownHook();
            }
            log.advisoryW().$("QuestDB is starting...").$();
            for (int i = 0, limit = workers.size(); i < limit; i++) {
                workers.getQuick(i).start();
            }
            workerPool.start(log); // starts QuestDB's workers
            Bootstrap.logWebConsoleUrls(config, log);
            System.gc(); // final GC
            log.advisoryW().$("QuestDB is running").$();
            log.advisoryW().$("enjoy").$();
        }
    }

    @Override
    public void close() {
        if (isWorking.compareAndSet(true, false)) {
            workerPool.close();
            ShutdownFlag.INSTANCE.shutdown();
            Misc.freeObjList(workers);
            workers.clear();
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

    private static WorkerPool initWorkers(
            ObjList<Lifecycle> workers,
            PropServerConfiguration config,
            Metrics metrics,
            Log log
    ) throws SqlException {

        WorkerPool pool = new WorkerPool(config.getWorkerPoolConfiguration(), metrics);
        pool.assignCleaner(Path.CLEANER);

        final CairoConfiguration cairoConfig = config.getCairoConfiguration();
        final CairoEngine cairoEngine = new CairoEngine(cairoConfig, metrics);
        pool.assign(cairoEngine.getEngineMaintenanceJob());
        workers.add(cairoEngine);

        // snapshots
        final DatabaseSnapshotAgent snapshotAgent = new DatabaseSnapshotAgent(cairoEngine);
        workers.add(snapshotAgent);

        final FunctionFactoryCache functionFactoryCache = new FunctionFactoryCache(
                cairoConfig,
                ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
        );
        O3Utils.setupWorkerPool(
                pool,
                cairoEngine,
                cairoConfig.getCircuitBreakerConfiguration(),
                functionFactoryCache
        );

        // Register jobs that help parallel execution of queries and column indexing.
        pool.assign(new ColumnIndexerJob(cairoEngine.getMessageBus()));
        pool.assign(new GroupByJob(cairoEngine.getMessageBus()));
        pool.assign(new LatestByAllIndexedJob(cairoEngine.getMessageBus()));

        // text import
        TextImportJob.assignToPool(cairoEngine.getMessageBus(), pool);
        if (cairoConfig.getSqlCopyInputRoot() != null) {
            final TextImportRequestJob textImportRequestJob = new TextImportRequestJob(
                    cairoEngine,
                    // save CPU resources for collecting and processing jobs
                    Math.max(1, pool.getWorkerCount() - 2),
                    functionFactoryCache
            );
            pool.assign(textImportRequestJob);
            pool.freeOnHalt(textImportRequestJob);
        }

        // http
        workers.add(HttpServer.create(
                config.getHttpServerConfiguration(),
                pool,
                log,
                cairoEngine,
                functionFactoryCache,
                snapshotAgent,
                metrics
        ));

        // http min
        workers.add(HttpServer.createMin(
                config.getHttpMinServerConfiguration(),
                pool,
                log,
                cairoEngine,
                functionFactoryCache,
                snapshotAgent,
                metrics
        ));

        // pg-wire
//        if (config.getPGWireConfiguration().isEnabled()) {
//            workers.add(PGWireServer.create(
//                    config.getPGWireConfiguration(),
//                    pool,
//                    log,
//                    cairoEngine,
//                    functionFactoryCache,
//                    snapshotAgent,
//                    metrics
//            ));
//        }

        // ilp/udp
        if (config.getLineUdpReceiverConfiguration().isEnabled()) {
            if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
                workers.add(new LinuxMMLineUdpReceiver(
                        config.getLineUdpReceiverConfiguration(),
                        cairoEngine,
                        pool
                ));
            } else {
                workers.add(new LineUdpReceiver(
                        config.getLineUdpReceiverConfiguration(),
                        cairoEngine,
                        pool
                ));
            }
        }

        // ilp/tcp
        workers.add(LineTcpReceiver.create(
                config.getLineTcpReceiverConfiguration(),
                pool,
                cairoEngine,
                metrics
        ));

        // telemetry
        if (!cairoConfig.getTelemetryConfiguration().getDisableCompletely()) {
            final TelemetryJob telemetryJob = new TelemetryJob(cairoEngine, functionFactoryCache);
            workers.add(telemetryJob);
            if (cairoConfig.getTelemetryConfiguration().getEnabled()) {
                pool.assign(telemetryJob);
            }
        }
        return pool;
    }

    public static void main(String[] args) throws Exception {
        try {
            new ServerMain(args).start(true);
        } catch (Throwable thr) {
            System.err.println(thr.getMessage());
            LogFactory.INSTANCE.flushJobsAndClose();
            System.exit(55);
        }
    }
}
