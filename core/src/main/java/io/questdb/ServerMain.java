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
import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.cutlass.text.TextImportJob;
import io.questdb.cutlass.text.TextImportRequestJob;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.engine.groupby.vect.GroupByJob;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkError;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.*;
import java.net.*;
import java.util.*;

public class ServerMain implements QuietClosable {

    private final PropServerConfiguration config;
    private final ObjList<Closeable> workers = new ObjList<>();
    private final WorkerPool workerPool;
    private final Log log;


    public ServerMain(PropServerConfiguration config, Metrics metrics, Log log) throws Exception {
        this.config = config;
        this.log = log;
        workerPool = new WorkerPool(config.getWorkerPoolConfiguration(), metrics);
        workerPool.assignCleaner(Path.CLEANER);

        final FunctionFactoryCache functionFactoryCache = new FunctionFactoryCache(
                config.getCairoConfiguration(),
                ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
        );

        final CairoEngine cairoEngine = new CairoEngine(config.getCairoConfiguration(), metrics);
        workerPool.assign(cairoEngine.getEngineMaintenanceJob());
        workers.add(cairoEngine);

        final DatabaseSnapshotAgent snapshotAgent = new DatabaseSnapshotAgent(cairoEngine);
        workers.add(snapshotAgent);

        O3Utils.setupWorkerPool(
                workerPool,
                cairoEngine,
                config.getCairoConfiguration().getCircuitBreakerConfiguration(),
                functionFactoryCache
        );

        try {
            // Register jobs that help parallel execution of queries and column indexing.
            workerPool.assign(new ColumnIndexerJob(cairoEngine.getMessageBus()));
            workerPool.assign(new GroupByJob(cairoEngine.getMessageBus()));
            workerPool.assign(new LatestByAllIndexedJob(cairoEngine.getMessageBus()));
            TextImportJob.assignToPool(cairoEngine.getMessageBus(), workerPool);
            if (config.getCairoConfiguration().getSqlCopyInputRoot() != null) {
                final TextImportRequestJob textImportRequestJob = new TextImportRequestJob(
                        cairoEngine,
                        // save CPU resources for collecting and processing jobs
                        Math.max(1, workerPool.getWorkerCount() - 2),
                        functionFactoryCache
                );
                workerPool.assign(textImportRequestJob);
                workerPool.freeOnHalt(textImportRequestJob);
            }
            workers.add(HttpServer.create(
                    config.getHttpServerConfiguration(),
                    workerPool,
                    log,
                    cairoEngine,
                    functionFactoryCache,
                    snapshotAgent,
                    metrics
            ));
            workers.add(HttpServer.createMin(
                    config.getHttpMinServerConfiguration(),
                    workerPool,
                    log,
                    cairoEngine,
                    functionFactoryCache,
                    snapshotAgent,
                    metrics
            ));
            if (config.getPGWireConfiguration().isEnabled()) {
                workers.add(PGWireServer.create(
                        config.getPGWireConfiguration(),
                        workerPool,
                        log,
                        cairoEngine,
                        functionFactoryCache,
                        snapshotAgent,
                        metrics
                ));
            }
            if (config.getLineUdpReceiverConfiguration().isEnabled()) {
                if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
                    workers.add(new LinuxMMLineUdpReceiver(
                            config.getLineUdpReceiverConfiguration(),
                            cairoEngine,
                            workerPool
                    ));
                } else {
                    workers.add(new LineUdpReceiver(
                            config.getLineUdpReceiverConfiguration(),
                            cairoEngine,
                            workerPool
                    ));
                }
            }
            workers.add(LineTcpReceiver.create(
                    config.getLineTcpReceiverConfiguration(),
                    workerPool,
                    log,
                    cairoEngine,
                    metrics
            ));
        } catch (NetworkError e) {
            log.error().$((Sinkable) e).$();
            LogFactory.INSTANCE.flushJobsAndClose();
            System.exit(55);
        }

        boolean enableTelemetry = !config.getCairoConfiguration().getTelemetryConfiguration().getDisableCompletely();
        if (enableTelemetry) {
            final TelemetryJob telemetryJob = new TelemetryJob(cairoEngine, functionFactoryCache);
            workers.add(telemetryJob);
            if (config.getCairoConfiguration().getTelemetryConfiguration().getEnabled()) {
                workerPool.assign(telemetryJob);
            }
        }
        System.gc(); // GC 1
    }

    public ServerMain(Bootstrap bootstrap) throws Exception {
        this(bootstrap.getConfig(), bootstrap.getMetrics(), bootstrap.getLog());
    }

    public void start() throws SocketException {
        workerPool.start(log); // starts QuestDB's services
        if (config.getHttpServerConfiguration().isEnabled()) {
            logWebConsoleUrls();
        }
        System.gc(); // final GC
        log.advisoryW().$("enjoy").$();
    }

    private void logWebConsoleUrls() throws SocketException {
        final LogRecord record = log.infoW().$("web console URL(s):").$('\n').$('\n');
        final int httpBindIP = config.getHttpServerConfiguration().getDispatcherConfiguration().getBindIPv4Address();
        final int httpBindPort = config.getHttpServerConfiguration().getDispatcherConfiguration().getBindPort();
        if (httpBindIP == 0) {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface networkInterface : Collections.list(nets)) {
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                for (InetAddress inetAddress : Collections.list(inetAddresses)) {
                    if (inetAddress instanceof Inet4Address) {
                        record.$('\t').$("http://").$(inetAddress).$(':').$(httpBindPort).$('\n');
                    }
                }
            }
            record.$('\n').$();
        } else {
            record.$('\t').$("http://").$ip(httpBindIP).$(':').$(httpBindPort).$('\n').$();
        }
    }

    @Override
    public void close() {
        ShutdownFlag.INSTANCE.shutdown();
        workerPool.halt();
        Misc.freeObjList(workers);
    }

    public static void main(String[] args) throws Exception {
        try {
            Bootstrap bootstrap = Bootstrap.withArgs(args);
            bootstrap.extractSite();

            final ServerMain serverMain = new ServerMain(bootstrap);
            serverMain.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    System.err.println("QuestDB is shutting down...");
                    serverMain.close();
                } catch (Error ignore) {
                    // ignore
                } finally {
                    LogFactory.INSTANCE.flushJobsAndClose();
                    System.err.println("QuestDB is shutdown.");
                }
            }));
        } catch (Bootstrap.BootstrapException ex) {
            System.err.println(ex.getMessage());
            LogFactory.INSTANCE.flushJobsAndClose();
            System.exit(55);
        }
    }
}
