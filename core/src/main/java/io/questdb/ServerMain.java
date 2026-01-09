/*******************************************************************************
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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DataID;
import io.questdb.cairo.FlushQueryCacheJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.cutlass.text.CopyImportJob;
import io.questdb.cutlass.text.CopyImportRequestJob;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.mp.Job;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.Clock;
import io.questdb.std.Uuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.PropertyKey.*;

public class ServerMain implements Closeable {
    private final Bootstrap bootstrap;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CairoEngine engine;
    private final FreeOnExit freeOnExit = new FreeOnExit();
    private final AtomicBoolean running = new AtomicBoolean();
    protected PGServer pgServer;
    private Thread compileViewsThread;
    private HttpServer httpServer;
    private Thread hydrateMetadataThread;
    private boolean initialized;
    private WorkerPoolManager workerPoolManager;

    public ServerMain(String... args) {
        this(new Bootstrap(args));
    }

    public ServerMain(final Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
        // create cairo engine
        engine = freeOnExit(bootstrap.newCairoEngine());
        try {
            final ServerConfiguration config = bootstrap.getConfiguration();
            config.init(engine, freeOnExit);
            freeOnExit(config.getFactoryProvider());
            engine.load();
        } catch (Throwable th) {
            Misc.free(freeOnExit);
            throw th;
        }
    }

    public static ServerMain create(String root, Map<String, String> env) {
        final Map<String, String> newEnv = new HashMap<>(System.getenv());
        newEnv.putAll(env);
        PropBootstrapConfiguration bootstrapConfiguration = new PropBootstrapConfiguration() {
            @Override
            public Map<String, String> getEnv() {
                return newEnv;
            }
        };

        return new ServerMain(new Bootstrap(bootstrapConfiguration, Bootstrap.getServerMainArgs(root)));
    }

    public static ServerMain create(String root) {
        return new ServerMain(Bootstrap.getServerMainArgs(root));
    }

    public static ServerMain createWithoutWalApplyJob(String root, Map<String, String> env) {
        final Map<String, String> newEnv = new HashMap<>(System.getenv());
        newEnv.putAll(env);
        PropBootstrapConfiguration bootstrapConfiguration = new PropBootstrapConfiguration() {
            @Override
            public Map<String, String> getEnv() {
                return newEnv;
            }
        };

        return new ServerMain(new Bootstrap(bootstrapConfiguration, Bootstrap.getServerMainArgs(root))) {
            @Override
            protected void setupWalApplyJob(WorkerPool workerPool, CairoEngine engine, int sharedQueryWorkerCount) {
            }
        };
    }

    public static void main(String[] args) {
        try {
            new ServerMain(args).start(true);
        } catch (Bootstrap.BootstrapException e) {
            if (e.isSilentStacktrace()) {
                System.err.println(e.getMessage());
            } else {
                //noinspection CallToPrintStackTrace
                e.printStackTrace();
            }
            LogFactory.closeInstance();
            System.exit(55);
        } catch (Throwable thr) {
            //noinspection CallToPrintStackTrace
            thr.printStackTrace();
            LogFactory.closeInstance();
            System.exit(55);
        }
    }

    public static @NotNull String propertyPathToEnvVarName(@NotNull String propertyPath) {
        return "QDB_" + propertyPath.replace('.', '_').toUpperCase();
    }

    /**
     * Waits for startup background tasks to complete, including metadata cache
     * and recent write tracker hydration. This should be called after {@link #start()}
     * if immediate DDL operations (like DROP TABLE) are planned, to avoid conflicts
     * with background hydration threads that may hold table metadata locks.
     */
    public void awaitStartup() {
        joinThread(hydrateMetadataThread, false);
        joinThread(compileViewsThread, false);
    }

    public void awaitTable(String tableName) {
        getEngine().awaitTable(tableName, 30, TimeUnit.SECONDS);
    }

    @TestOnly
    public void awaitTxn(String tableName, long txn) {
        getEngine().awaitTxn(tableName, txn, 15, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            joinThread(hydrateMetadataThread, true);
            joinThread(compileViewsThread, true);
            System.err.println("QuestDB is shutting down...");
            System.out.println("QuestDB is shutting down...");
            if (bootstrap != null && bootstrap.getLog() != null) {
                // Still useful in case of custom logger
                bootstrap.getLog().info().$("QuestDB is shutting down...").$();
            }
            // Signal long-running task to exit ASAP
            engine.signalClose();
            if (initialized) {
                workerPoolManager.halt();
            }
            freeOnExit.close();
        }
    }

    public long getActiveConnectionCount(String processorName) {
        if (httpServer == null) {
            return 0;
        }
        return httpServer.getActiveConnectionTracker().get(processorName);
    }

    public ServerConfiguration getConfiguration() {
        return bootstrap.getConfiguration();
    }

    public CairoEngine getEngine() {
        if (closed.get()) {
            throw new IllegalStateException("close was called");
        }
        return engine;
    }

    public int getHttpServerPort() {
        if (httpServer != null) {
            return httpServer.getPort();
        }
        throw CairoException.nonCritical().put("http server is not running");
    }

    public int getPgWireServerPort() {
        if (pgServer != null) {
            return pgServer.getPort();
        }
        throw CairoException.nonCritical().put("pgwire server is not running");
    }

    public WorkerPoolManager getWorkerPoolManager() {
        if (closed.get()) {
            throw new IllegalStateException("close was called");
        }
        return workerPoolManager;
    }

    public boolean hasBeenClosed() {
        return closed.get();
    }

    public boolean hasStarted() {
        return running.get();
    }

    @TestOnly
    public void resetQueryCache() {
        pgServer.resetQueryCache();
    }

    public void start() {
        start(false);
    }

    public synchronized void start(boolean addShutdownHook) {
        if (!closed.get() && running.compareAndSet(false, true)) {
            initialize(bootstrap.getLog());

            if (addShutdownHook) {
                addShutdownHook();
            }
            workerPoolManager.start(bootstrap.getLog());
            bootstrap.logBannerAndEndpoints(webConsoleSchema());
            final DataID dataID = engine.getDataID();
            if (dataID.isInitialized()) {
                final Uuid uuid = dataID.get();
                bootstrap.getLog().advisoryW().$("data id: ").$(uuid).$();
            }
            System.gc(); // final GC
            bootstrap.getLog().advisoryW().$("enjoy").$();
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.err.println("SIGTERM received");
                System.out.println("SIGTERM received");
                // It's fine if the magic number doesn't get its way to logs.
                // We log it merely to make sure that LOAD instructions generated by
                // AsyncFilterAtom#preTouchColumns() aren't optimized away by JVM's JIT compiler.
                bootstrap.getLog().debug().$("Pre-touch magic number: ").$(AsyncFilterAtom.PRE_TOUCH_BLACK_HOLE.sum()).$();
                close();
                LogFactory.closeInstance();
            } catch (Error ignore) {
                // ignore
            } finally {
                System.err.println("QuestDB is shutdown.");
                System.out.println("QuestDB is shutdown.");
            }
        }));
    }

    private synchronized void initialize(Log log) {
        initialized = true;
        final ServerConfiguration config = bootstrap.getConfiguration();
        final CairoConfiguration cairoConfig = config.getCairoConfiguration();
        // create the worker pool manager, and configure the shared pool
        final boolean walSupported = cairoConfig.isWalSupported();
        final boolean isReadOnly = cairoConfig.isReadOnlyInstance();
        final boolean walApplyEnabled = cairoConfig.isWalApplyEnabled();
        final boolean matViewEnabled = cairoConfig.isMatViewEnabled();

        workerPoolManager = new WorkerPoolManager(config) {
            @Override
            protected void configureWorkerPools(final WorkerPool sharedPoolQuery, final WorkerPool sharedPoolWrite) {
                try {
                    Job engineMaintenanceJob = setupEngineMaintenanceJob(engine);
                    if (engineMaintenanceJob != null) {
                        sharedPoolWrite.assign(engineMaintenanceJob);
                    }
                    WorkerPoolUtils.setupAsyncMunmapJob(sharedPoolQuery, engine);
                    WorkerPoolUtils.setupQueryJobs(sharedPoolQuery, engine);

                    if (!config.getCairoConfiguration().isReadOnlyInstance()) {
                        QueryTracingJob queryTracingJob = new QueryTracingJob(engine);
                        sharedPoolQuery.assign(queryTracingJob);
                        freeOnExit(queryTracingJob);
                    }

                    if (!isReadOnly) {
                        WorkerPoolUtils.setupWriterJobs(sharedPoolWrite, engine);

                        if (walSupported) {
                            sharedPoolWrite.assign(config.getFactoryProvider().getWalJobFactory().createCheckWalTransactionsJob(engine));
                            final WalPurgeJob walPurgeJob = config.getFactoryProvider().getWalJobFactory().createWalPurgeJob(engine);
                            engine.setWalPurgeJobRunLock(walPurgeJob.getRunLock());
                            walPurgeJob.delayByHalfInterval();
                            sharedPoolWrite.assign(walPurgeJob);
                            sharedPoolWrite.freeOnExit(walPurgeJob);

                            // wal apply job in the shared pool when there is no dedicated pool
                            if (walApplyEnabled && !config.getWalApplyPoolConfiguration().isEnabled()) {
                                setupWalApplyJob(sharedPoolWrite, engine, sharedPoolQuery.getWorkerCount());
                            }
                        }

                        // text import
                        if (!Chars.empty(cairoConfig.getSqlCopyInputRoot())) {
                            CopyImportJob.assignToPool(engine.getMessageBus(), sharedPoolWrite);
                            final CopyImportRequestJob copyImportRequestJob = new CopyImportRequestJob(
                                    engine,
                                    // save CPU resources for collecting and processing jobs
                                    Math.max(1, sharedPoolWrite.getWorkerCount() - 2)
                            );
                            sharedPoolWrite.assign(copyImportRequestJob);
                            sharedPoolWrite.freeOnExit(copyImportRequestJob);
                        }
                    }

                    // export - current export implementation requires creating temporary table, can only enable on the primary instance
                    if (!Chars.empty(cairoConfig.getSqlCopyExportRoot()) && !isReadOnly) {
                        int workerCount = config.getExportPoolConfiguration().getWorkerCount();
                        if (workerCount > 0) {
                            WorkerPool exportWorkerPool = getWorkerPool(
                                    config.getExportPoolConfiguration(),
                                    Requester.EXPORT,
                                    sharedPoolQuery
                            );

                            for (int i = 0; i < workerCount; i++) {
                                final CopyExportRequestJob copyExportRequestJob = new CopyExportRequestJob(engine);
                                exportWorkerPool.assign(i, copyExportRequestJob);
                                exportWorkerPool.freeOnExit(copyExportRequestJob);
                            }
                        } else {
                            log.advisory().$("export is disabled; set ")
                                    .$(EXPORT_WORKER_COUNT.getPropertyPath())
                                    .$(" to a positive value or keep default to enable export.")
                                    .$();
                        }
                    }

                    // telemetry
                    if (!cairoConfig.getTelemetryConfiguration().getDisableCompletely()) {
                        final TelemetryJob telemetryJob = new TelemetryJob(engine);
                        freeOnExit(telemetryJob);
                        if (cairoConfig.getTelemetryConfiguration().getEnabled()) {
                            sharedPoolWrite.assign(telemetryJob);
                        }
                    }

                } catch (Throwable thr) {
                    throw new Bootstrap.BootstrapException(thr);
                }
            }
        };

        // make sure view definitions are loaded before the view compiler job is started,
        // all views have to be loaded with their dependencies before the compiler starts processing notifications
        engine.buildViewGraphs();

        if (matViewEnabled && !isReadOnly) {
            if (config.getMatViewRefreshPoolConfiguration().getWorkerCount() > 0) {
                // This starts mat view refresh jobs only when there is a dedicated pool for mat view refresh
                // this will not use shared pool write because getWorkerCount() > 0
                WorkerPool mvRefreshWorkerPool = workerPoolManager.getSharedPoolWrite(
                        config.getMatViewRefreshPoolConfiguration(),
                        WorkerPoolManager.Requester.MAT_VIEW_REFRESH
                );
                setupMatViewJobs(mvRefreshWorkerPool, engine, workerPoolManager.getSharedQueryWorkerCount());
            } else {
                log.advisory().$("mat view refresh is disabled; set ")
                        .$(MAT_VIEW_REFRESH_WORKER_COUNT.getPropertyPath())
                        .$(" to a positive value or keep default to enable mat view refresh.")
                        .$();
            }
        }

        if (config.getViewCompilerPoolConfiguration().getWorkerCount() > 0) {
            // This starts view compiler jobs only when there is a dedicated pool configured
            // this will not use shared pool write because getWorkerCount() > 0
            WorkerPool viewCompilerWorkerPool = workerPoolManager.getSharedPoolWrite(
                    config.getViewCompilerPoolConfiguration(),
                    WorkerPoolManager.Requester.VIEW_COMPILER
            );
            setupViewJobs(viewCompilerWorkerPool, engine, workerPoolManager.getSharedQueryWorkerCount());
        } else {
            log.advisory().$("view compiler job is disabled; set ")
                    .$(VIEW_COMPILER_WORKER_COUNT.getPropertyPath())
                    .$(" to a positive value or keep default to enable view compiler.")
                    .$();
        }

        if (walApplyEnabled && !isReadOnly && walSupported && config.getWalApplyPoolConfiguration().isEnabled()) {
            WorkerPool walApplyWorkerPool = workerPoolManager.getSharedPoolWrite(
                    config.getWalApplyPoolConfiguration(),
                    WorkerPoolManager.Requester.WAL_APPLY
            );
            setupWalApplyJob(walApplyWorkerPool, engine, workerPoolManager.getSharedQueryWorkerCount());
        }

        // http
        freeOnExit(httpServer = services().createHttpServer(
                config,
                engine,
                workerPoolManager
        ));

        // http min
        freeOnExit(services().createMinHttpServer(
                config.getHttpMinServerConfiguration(),
                workerPoolManager
        ));

        // pg wire
        freeOnExit(pgServer = services().createPGWireServer(
                config.getPGWireConfiguration(),
                engine,
                workerPoolManager
        ));

        workerPoolManager.getSharedPoolNetwork().assign(new FlushQueryCacheJob(
                engine.getMessageBus(),
                httpServer,
                pgServer
        ));

        if (!isReadOnly && config.getLineTcpReceiverConfiguration().isEnabled()) {
            // ilp/tcp
            freeOnExit(services().createLineTcpReceiver(
                    config.getLineTcpReceiverConfiguration(),
                    engine,
                    workerPoolManager
            ));

            // ilp/udp
            freeOnExit(services().createLineUdpReceiver(
                    config.getLineUdpReceiverConfiguration(),
                    engine,
                    workerPoolManager
            ));
        }

        // metadata and write tracker hydration
        hydrateMetadataThread = new Thread(() -> {
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.hydrateRecentWriteTracker();
        });
        hydrateMetadataThread.start();

        // populate view state store and hydrate metadata cache with view metadata
        compileViewsThread = new Thread(() -> {
            // temporary list to re-use for listing dependent views
            ObjList<TableToken> tempSink = new ObjList<>();
            try (SqlExecutionContext executionContext = engine.createViewCompilerContext(0)) {
                ViewCompilerJob.compileAllViews(engine, executionContext, tempSink);
            }
        });
        compileViewsThread.start();

        System.gc(); // GC 1
        bootstrap.getLog().advisoryW().$("server is ready to be started").$();
    }

    private void joinThread(Thread thread, boolean ignoreInterrupt) {
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                if (!ignoreInterrupt) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    protected <T extends Closeable> T freeOnExit(T closeable) {
        return freeOnExit.register(closeable);
    }

    protected Services services() {
        return Services.INSTANCE;
    }

    protected Job setupEngineMaintenanceJob(CairoEngine engine) {
        return new EngineMaintenanceJob(engine);
    }

    protected void setupMatViewJobs(WorkerPool mvWorkerPool, CairoEngine engine, int sharedQueryWorkerCount) {
        for (int i = 0, workerCount = mvWorkerPool.getWorkerCount(); i < workerCount; i++) {
            // create job per worker
            final MatViewRefreshJob matViewRefreshJob = new MatViewRefreshJob(i, engine, sharedQueryWorkerCount);
            mvWorkerPool.assign(i, matViewRefreshJob);
            mvWorkerPool.freeOnExit(matViewRefreshJob);
        }
        final MatViewTimerJob matViewTimerJob = new MatViewTimerJob(engine);
        mvWorkerPool.assign(matViewTimerJob);
    }

    protected void setupViewJobs(WorkerPool vWorkerPool, CairoEngine engine, int sharedQueryWorkerCount) {
        for (int i = 0, workerCount = vWorkerPool.getWorkerCount(); i < workerCount; i++) {
            // create job per worker
            final ViewCompilerJob viewCompilerJob = new ViewCompilerJob(i, engine, sharedQueryWorkerCount);
            vWorkerPool.assign(i, viewCompilerJob);
            vWorkerPool.freeOnExit(viewCompilerJob);
        }
    }

    protected void setupWalApplyJob(
            WorkerPool sharedPoolWrite,
            CairoEngine engine,
            int sharedQueryWorkerCount
    ) {
        for (int i = 0, workerCount = sharedPoolWrite.getWorkerCount(); i < workerCount; i++) {
            // create job per worker
            final ApplyWal2TableJob applyWal2TableJob = new ApplyWal2TableJob(engine, sharedQueryWorkerCount);
            sharedPoolWrite.assign(i, applyWal2TableJob);
            sharedPoolWrite.freeOnExit(applyWal2TableJob);
        }
    }

    protected String webConsoleSchema() {
        return "http";
    }

    public static class EngineMaintenanceJob extends SynchronizedJob {
        private final long checkInterval;
        private final Clock clock;
        private final CairoEngine engine;
        private long last = 0;

        public EngineMaintenanceJob(CairoEngine engine) {
            final CairoConfiguration configuration = engine.getConfiguration();
            this.engine = engine;
            this.clock = configuration.getMicrosecondClock();
            this.checkInterval = configuration.getIdleCheckInterval() * 1000;
        }

        @Override
        protected boolean runSerially() {
            long t = clock.getTicks();
            if (last + checkInterval < t) {
                last = t;
                return engine.releaseInactive();
            }
            return false;
        }
    }
}
