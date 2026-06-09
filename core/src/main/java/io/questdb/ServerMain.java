/*+*****************************************************************************
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
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.cutlass.qwp.server.QwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiverConfiguration;
import io.questdb.lifecycle.Component;
import io.questdb.cutlass.text.CopyImportJob;
import io.questdb.cutlass.text.CopyImportRequestJob;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.State;
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
import io.questdb.std.Uuid;
import io.questdb.std.datetime.Clock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.PropertyKey.*;

public class ServerMain implements Closeable {
    private final CairoEngine engine;
    private final Bootstrap bootstrap;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final FreeOnExit freeOnExit = new FreeOnExit();
    private final AtomicBoolean running = new AtomicBoolean();
    private WorkerPoolManager workerPoolManager;
    private Thread compileViewsThread;
    private Thread hydrateMetadataThread;
    private io.questdb.lifecycle.LifecycleOrchestrator orchestrator;

    public ServerMain(String... args) {
        this(new Bootstrap(args));
    }

    public ServerMain(final Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
        // bootstrap.newCairoEngine() returns a full-init engine in OSS (back-compat for tests).
        // Enterprise subclasses override newCairoEngine() to return a partial-init engine so that
        // completeInit() + load() are deferred to EngineEnvelope.start().
        engine = freeOnExit(bootstrap.newCairoEngine());
        try {
            final ServerConfiguration config = bootstrap.getConfiguration();
            config.init(engine, freeOnExit);
            freeOnExit(config.getFactoryProvider());
            // engine.load() is called by EngineEnvelope.start(); completeInit() is called there
            // too (or skipped if the engine was already fully initialised at construction time).
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
            // Halt the worker pool before freeing the engine so no worker thread can fire
            // a telemetry or WAL-listener callback while the engine's resources are being
            // released by freeOnExit.close() below. Without this halt, TelemetryJob.close()
            // (registered last in freeOnExit, so freed first in LIFO order) runs while the
            // shared write pool is still live -- a concurrent worker runSerially() call
            // writes to the same WAL file descriptors and causes a double-close fd race.
            // The halt is idempotent: WorkerPool.halt() is CAS-guarded, so the orchestrator's
            // later WorkerPoolManagerEnvelope.stop() halt becomes a no-op second call with
            // no behavioural effect.
            // Guard for the case where close() is called before the worker pool manager is
            // constructed (e.g. an exception during engine load). WorkerPoolManagerEnvelope.stop()
            // already applies this guard; the check here keeps the two call sites consistent.
            // Bound the halt so a wedged worker (GC-starvation, a stuck native job) cannot make
            // close() block forever. WorkerPool.halt()'s waits on started/halted were unbounded, so
            // a hung worker turned this close path into an unkillable shutdown under SIGTERM. The
            // bounded variant waits up to a shared deadline across all pools, then logs and proceeds.
            if (workerPoolManager != null) {
                workerPoolManager.halt(System.nanoTime() + WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
            freeOnExit.close();
        }
    }

    public long getActiveConnectionCount(String processorName) {
        if (orchestrator == null) {
            return 0;
        }
        Component webHttpComp = orchestrator.getComponent("web-http");
        if (webHttpComp instanceof WebHttpEnvelope) {
            HttpServer s = ((WebHttpEnvelope) webHttpComp).server;
            if (s != null) {
                return s.getActiveConnectionTracker().get(processorName);
            }
        }
        return 0;
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
        if (orchestrator != null) {
            Component c = orchestrator.getComponent("web-http");
            if (c instanceof WebHttpEnvelope) {
                HttpServer s = ((WebHttpEnvelope) c).server;
                if (s != null) {
                    return s.getPort();
                }
            }
        }
        throw CairoException.nonCritical().put("http server is not running");
    }

    /**
     * Returns the port the min-http listener is bound to, or -1 if not yet bound (envelope
     * still in INIT/STARTING) or if min-http is disabled via config. Test seam for Phase 6
     * Plan C integration tests that drive POST /lifecycle/switch via java.net.http.HttpClient
     * against the in-process server. Mirrors {@link #getHttpServerPort()} but targets the
     * min-http envelope's HttpServer.
     */
    public int getMinHttpPort() {
        if (orchestrator != null) {
            Component c = orchestrator.getComponent("min-http");
            if (c instanceof MinHttpEnvelope) {
                HttpServer s = ((MinHttpEnvelope) c).server;
                if (s != null) {
                    return s.getPort();
                }
            }
        }
        return -1;
    }

    /**
     * Returns the live {@link LifecycleOrchestrator} reference, or null if the server has
     * not yet started (orchestrator is constructed in {@link #start(boolean)}). Test seam
     * for in-process JVM integration tests that need to introspect lifecycle state.
     */
    public LifecycleOrchestrator getOrchestrator() {
        return orchestrator;
    }

    public int getPgWireServerPort() {
        if (orchestrator != null) {
            Component c = orchestrator.getComponent("pg-wire");
            if (c instanceof PgWireEnvelope) {
                PGServer s = ((PgWireEnvelope) c).server;
                if (s != null) {
                    return s.getPort();
                }
            }
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
        if (orchestrator != null) {
            Component c = orchestrator.getComponent("pg-wire");
            if (c instanceof PgWireEnvelope) {
                PGServer s = ((PgWireEnvelope) c).server;
                if (s != null) {
                    s.resetQueryCache();
                }
            }
        }
    }

    /**
     * Test-only bootstrap for per-envelope lifecycle unit tests.
     * Initialises {@link #workerPoolManager} (so the protocol envelopes obtained via
     * {@link #testNewPgWireEnvelope()} et al. can wire jobs onto its pools) and stands up a
     * minimal {@link io.questdb.lifecycle.LifecycleOrchestrator} with a stub PgWireEnvelope
     * registered (so {@link #findEnvelope(String, Class)} succeeds when WebHttpEnvelope.start()
     * looks up the cross-envelope PGServer reference).
     * <p>
     * The pools are NOT started -- assign() calls inside envelope.start() bodies remain
     * legal because WorkerPool's running flag stays false. The internal orchestrator is
     * never run; the registered PgWireEnvelope stays in INIT state, its server reference
     * stays null, and the FlushQueryCacheJob in WebHttpEnvelope.start() receives a null
     * pgServer (which is safe -- FlushQueryCacheJob handles null pg/http servers).
     * <p>
     * Production server.start() callers MUST NOT invoke this method -- it is exclusively
     * for the in-process test harness that drives individual production envelopes via
     * an external {@link io.questdb.lifecycle.LifecycleOrchestrator}.
     */
    @TestOnly
    public void testInitForEnvelopeTests() {
        if (!engine.isCompleteInitDone()) {
            engine.completeInit();
        }
        if (workerPoolManager == null) {
            constructAndAssignWorkerPoolManager(bootstrap.getLog());
        }
        if (orchestrator == null) {
            orchestrator = newOrchestrator(bootstrap.getLog(), null, null);
            freeOnExit(orchestrator);
            // Register a stub PgWireEnvelope so findEnvelope("pg-wire", PgWireEnvelope.class)
            // succeeds for tests that drive WebHttpEnvelope through start(). The envelope stays
            // in INIT state because orchestrator.run() is intentionally not invoked here.
            orchestrator.register(new PgWireEnvelope(bootstrap.getLog()));
        }
    }

    /**
     * Test-only factory: create an IlpTcpEnvelope bound to this ServerMain instance.
     * Used by per-envelope lifecycle unit tests that register envelopes directly into a
     * LifecycleTestHarness instead of going through the full bootstrap path.
     */
    @TestOnly
    public Component testNewIlpTcpEnvelope() {
        return new IlpTcpEnvelope(bootstrap.getLog());
    }

    /**
     * Test-only factory: create a PgWireEnvelope bound to this ServerMain instance.
     */
    @TestOnly
    public Component testNewPgWireEnvelope() {
        return new PgWireEnvelope(bootstrap.getLog());
    }

    /**
     * Test-only factory: create a QwipEnvelope bound to this ServerMain instance.
     */
    @TestOnly
    public Component testNewQwipEnvelope() {
        return new QwipEnvelope(bootstrap.getLog());
    }

    /**
     * Test-only factory: create a WebHttpEnvelope bound to this ServerMain instance.
     */
    @TestOnly
    public Component testNewWebHttpEnvelope() {
        return new WebHttpEnvelope(bootstrap.getLog());
    }

    public void start() {
        start(false);
    }

    public synchronized void start(boolean addShutdownHook) {
        if (!closed.get() && running.compareAndSet(false, true)) {
            orchestrator = newOrchestrator(
                    bootstrap.getLog(),
                    null,   // workerPoolManager exposed lazily after WPM envelope reaches DEGRADED
                    null    // tokio runtime -- enterprise plans (Plan 04) override registerComponents to provide
            );
            freeOnExit(orchestrator);
            if (addShutdownHook) {
                addShutdownHook();
            }
            registerComponents(orchestrator);
            orchestrator.run();   // BLOCKS until graph stable; throws LifecycleStartupException on boot-essential failure
            // Banner, DataID log, System.gc, 'enjoy' advisory all emit from the network-services envelope tail
            // (W4 -- moved verbatim from this method's :263-:272 region).
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

    /**
     * Verbatim lift of today's {@code ServerMain.initialize()} body lines :295-:398.
     * Constructs and assigns the {@link WorkerPoolManager} including all engine-derived
     * job assignments. Called from {@link WorkerPoolManagerEnvelope#start(LifecycleContext)}.
     *
     * @deprecated For new use, the lifecycle envelopes own this work.
     */
    private synchronized void constructAndAssignWorkerPoolManager(Log log) {
        final ServerConfiguration config = bootstrap.getConfiguration();
        final CairoConfiguration cairoConfig = config.getCairoConfiguration();
        final boolean walSupported = cairoConfig.isWalSupported();
        final boolean isReadOnly = cairoConfig.isReadOnlyInstance();
        final boolean walApplyEnabled = cairoConfig.isWalApplyEnabled();

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
                        sharedPoolWrite.assign(telemetryJob);
                    }

                } catch (Throwable thr) {
                    throw new Bootstrap.BootstrapException(thr);
                }
            }
        };

        // make sure view definitions are loaded before the view compiler job is started,
        // all views have to be loaded with their dependencies before the compiler starts processing notifications
        engine.buildViewGraphs();

        setupDedicatedPools(log, isReadOnly, config);

        if (walApplyEnabled && !isReadOnly && walSupported && config.getWalApplyPoolConfiguration().isEnabled()) {
            WorkerPool walApplyWorkerPool = workerPoolManager.getSharedPoolWrite(
                    config.getWalApplyPoolConfiguration(),
                    WorkerPoolManager.Requester.WAL_APPLY
            );
            setupWalApplyJob(walApplyWorkerPool, engine, workerPoolManager.getSharedQueryWorkerCount());
        }
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

    /**
     * Factory hook for binding additional handlers on the min-http server. Default no-op;
     * subclasses override to bind extra handlers (for example, downstream may register
     * additional endpoints) on the same min-http listening socket.
     */
    protected void bindAdditionalMinHttpHandlers(
            io.questdb.cutlass.http.HttpServer server,
            io.questdb.cutlass.http.HttpServerConfiguration httpMinConfig,
            io.questdb.lifecycle.LifecycleOrchestrator orch
    ) {
        // OSS default: no additional handlers.
    }

    /**
     * Factory hook for the {@code GET /lifecycle} HTTP processor. Enterprise overrides
     * to return {@code EntLifecycleProcessor} which emits role-aware JSON fields.
     */
    protected io.questdb.cutlass.http.processors.LifecycleProcessor newLifecycleProcessor(
            io.questdb.cutlass.http.HttpServerConfiguration httpMinConfig,
            io.questdb.lifecycle.LifecycleOrchestrator orch
    ) {
        return new io.questdb.cutlass.http.processors.LifecycleProcessor(httpMinConfig, orch::snapshot);
    }

    /**
     * Factory hook for the lifecycle orchestrator. Enterprise subclasses override to construct
     * an enterprise overlay (e.g. {@code EntLifecycleOrchestrator}) that carries the role-aware
     * surface absent from this OSS base.
     */
    protected io.questdb.lifecycle.LifecycleOrchestrator newOrchestrator(
            @org.jetbrains.annotations.Nullable io.questdb.log.Log log,
            @org.jetbrains.annotations.Nullable io.questdb.WorkerPoolManager workerPoolManager,
            @org.jetbrains.annotations.Nullable Object tokioRuntime
    ) {
        return new io.questdb.lifecycle.LifecycleOrchestrator(log, workerPoolManager, tokioRuntime);
    }

    /**
     * Factory hook returning the base envelopes this {@code ServerMain} contributes to the
     * lifecycle DAG. Subclasses (e.g. enterprise overlays) override to wrap or replace
     * individual envelopes without forking the {@link #registerComponents} body. Mirrors
     * the existing {@link #setupMatViewJobs}, {@link #webConsoleSchema} hook conventions.
     */
    protected ObjList<io.questdb.lifecycle.Component> baseComponents() {
        final ObjList<io.questdb.lifecycle.Component> components = new ObjList<>();
        components.add(new FactoryProviderEnvelope());
        components.add(new EngineEnvelope());
        components.add(new HydrationEnvelope());
        components.add(new MinHttpEnvelope(bootstrap.getLog()));
        components.add(new WorkerPoolManagerEnvelope(bootstrap.getLog()));
        components.add(new PgWireEnvelope(bootstrap.getLog()));
        components.add(new IlpTcpEnvelope(bootstrap.getLog()));
        components.add(new WebHttpEnvelope(bootstrap.getLog()));
        components.add(new QwipEnvelope(bootstrap.getLog()));
        return components;
    }

    /**
     * Register lifecycle components with the orchestrator. Called by
     * {@link #start(boolean)} after orchestrator construction. Subclasses
     * (e.g. {@code EntServerMain}) override {@link #baseComponents()} to wrap or
     * replace individual envelopes, and override {@link #registerComponents} to
     * add their own envelopes after invoking {@code super.registerComponents(orch)}.
     */
    protected void registerComponents(io.questdb.lifecycle.LifecycleOrchestrator orch) {
        final ObjList<io.questdb.lifecycle.Component> components = baseComponents();
        for (int i = 0, n = components.size(); i < n; i++) {
            orch.register(components.getQuick(i));
        }
    }

    protected Services services() {
        return Services.INSTANCE;
    }

    protected void setupDedicatedPools(Log log, boolean isReadOnly, ServerConfiguration config) {
        if (config.getCairoConfiguration().isMatViewEnabled() && !isReadOnly) {
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

    /**
     * Extra hard-dep names injected into the engine envelope's dependency list.
     * Default empty. Subclasses (e.g. {@code EntServerMain}) override to add
     * envelopes (e.g. {@code backup-restore}) that must reach READY before the
     * engine envelope calls {@link CairoEngine#completeInit()}. Called once from
     * {@link EngineEnvelope}'s constructor.
     */
    protected ObjList<String> engineExtraHardDeps() {
        return new ObjList<>();
    }

    /**
     * Look up an envelope by name from the live orchestrator registry and cast it to
     * the expected type. Used by WebHttpEnvelope to fetch the PgServer reference from
     * PgWireEnvelope without introducing a direct field coupling.
     */
    protected <T extends Component> T findEnvelope(String name, Class<T> type) {
        Component c = orchestrator.getComponent(name);
        return type.cast(c);
    }

    /**
     * Extra hard-dep names injected into the worker-pool-manager envelope's
     * dependency list. Default empty. Subclasses (e.g. {@code EntServerMain})
     * override to insert envelopes (e.g. {@code ent-pre-services}) that must
     * reach READY/DEGRADED before WPM begins. Called once from
     * {@link WorkerPoolManagerEnvelope}'s constructor.
     */
    protected ObjList<String> workerPoolManagerExtraHardDeps() {
        return new ObjList<>();
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

    /**
     * D-04: FactoryProvider already built in the ServerMain constructor at line :93.
     * This envelope performs a no-op state transition (INIT -> STARTING -> READY)
     * and has no hard deps.
     */
    private final class FactoryProviderEnvelope implements io.questdb.lifecycle.Component {
        private final ObjList<String> empty = new ObjList<>();

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return empty;
        }

        @Override
        public String name() {
            return "factory-provider";
        }

        @Override
        public ObjList<String> softDependencies() {
            return empty;
        }

        @Override
        public void start(io.questdb.lifecycle.LifecycleContext ctx) {
            // D-04: FactoryProvider built in ServerMain ctor at line :93. No-op state transition.
            ctx.publish(io.questdb.lifecycle.State.STARTING);
            ctx.publish(io.questdb.lifecycle.State.READY);
        }

        @Override
        public void stop() {
            // FactoryProvider closed by FreeOnExit chain registered at ServerMain.java:94. No-op here.
        }
    }

    /**
     * D-04: CairoEngine already built in the ServerMain constructor at line :90.
     * This envelope performs a no-op state transition (INIT -> STARTING -> READY)
     * and hard-deps on factory-provider.
     */
    private final class EngineEnvelope implements io.questdb.lifecycle.Component {
        private final ObjList<String> empty = new ObjList<>();
        private final ObjList<String> hardDeps;

        EngineEnvelope() {
            hardDeps = new ObjList<>();
            hardDeps.add("factory-provider");
            // engineExtraHardDeps() hook: enterprise override injects ["backup-restore"] so
            // the orchestrator gates engine.completeInit() on BackupRestoreEnvelope reaching READY.
            ObjList<String> extras = ServerMain.this.engineExtraHardDeps();
            for (int i = 0, n = extras.size(); i < n; i++) {
                hardDeps.add(extras.getQuick(i));
            }
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return "engine";
        }

        @Override
        public ObjList<String> softDependencies() {
            return empty;
        }

        @Override
        public void start(io.questdb.lifecycle.LifecycleContext ctx) {
            ctx.publish(io.questdb.lifecycle.State.STARTING);
            // Run the post-restore engine initialization then load table state.
            // completeInit() was historically called inside the CairoEngine constructor; it is
            // now deferred here so the orchestrator DAG can gate it on backup-restore READY.
            // Guard: back-compat ctors (used in test scaffolding) call completeInit=true, so
            // the engine arrives here already fully initialised -- skip to avoid double-init.
            if (!ServerMain.this.engine.isCompleteInitDone()) {
                ServerMain.this.engine.completeInit();
            }
            ServerMain.this.engine.load();
            ctx.publish(io.questdb.lifecycle.State.READY);
        }

        @Override
        public void stop() {
            // engine closed by FreeOnExit registered in ServerMain ctor. signalClose() handled by ServerMain.close().
        }
    }

    /**
     * Post-engine-READY hydration envelope. Fires the async metadata hydrator
     * (onStartupAsyncHydrator + hydrateRecentWriteTracker) and the materialised-view
     * compiler (ViewCompilerJob.compileAllViews) after the engine publishes READY.
     * <p>
     * Hard-deps on engine so the orchestrator dispatches engine to READY via
     * onDependencyState; both work units run on dedicated background threads so this
     * envelope itself reaches READY immediately and never blocks the orchestrator.
     * <p>
     * Replaces the now-deleted bindAndStartNetworkServices() / initialize() shims, which
     * had no production caller (only @Deprecated annotations + a few tests) yet remained
     * the only path that ran compileAllViews. Without this envelope, production boot
     * silently skipped view compilation -- finding #079.
     */
    private final class HydrationEnvelope implements io.questdb.lifecycle.Component {
        private final ObjList<String> empty = new ObjList<>();
        private final ObjList<String> hardDeps;

        HydrationEnvelope() {
            this.hardDeps = new ObjList<>();
            this.hardDeps.add("engine");
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return "hydration";
        }

        @Override
        public void onDependencyState(String depName, State previous, State current) {
            // Fire-once guard. The engine re-publishes READY on every role switch (a demote/promote
            // drives the engine envelope SWITCHING->READY again), and without this guard each such
            // re-publish would re-spawn the hydrator and view-compiler threads, orphaning the prior
            // thread references (single fields, never joined). The FIRST hydration after boot still
            // runs; only the redundant per-switch re-run is suppressed. This mirrors the catch-up
            // self-fire check in start(), which is already gated on hydrateMetadataThread == null.
            if ("engine".equals(depName) && current == State.READY && ServerMain.this.hydrateMetadataThread == null) {
                ServerMain.this.hydrateMetadataThread = new Thread(() -> {
                    ServerMain.this.engine.getMetadataCache().onStartupAsyncHydrator();
                    ServerMain.this.engine.hydrateRecentWriteTracker();
                });
                ServerMain.this.hydrateMetadataThread.start();

                ServerMain.this.compileViewsThread = new Thread(() -> {
                    ObjList<TableToken> tempSink = new ObjList<>();
                    try (SqlExecutionContext executionContext = ServerMain.this.engine.createViewCompilerContext(0)) {
                        ViewCompilerJob.compileAllViews(ServerMain.this.engine, executionContext, tempSink);
                    }
                });
                ServerMain.this.compileViewsThread.start();

                // GC and the "enjoy" advisory fire from the WorkerPoolManagerEnvelope's tail
                // (onStableBelow callback). No second System.gc() here -- the production tail
                // gc already runs exactly once after pool start.
                ServerMain.this.bootstrap.getLog().advisoryW().$("server is ready to be started").$();
            }
        }

        @Override
        public ObjList<String> softDependencies() {
            return empty;
        }

        @Override
        public void start(io.questdb.lifecycle.LifecycleContext ctx) {
            // The hydrator work runs on background threads from onDependencyState when engine
            // reaches READY. The envelope itself publishes READY synchronously so the orchestrator
            // does not block on it -- the background threads can outlive this start() call and the
            // ctor-owned freeOnExit chain ensures shutdown still joins them via ServerMain.close().
            ctx.publish(io.questdb.lifecycle.State.STARTING);
            ctx.publish(io.questdb.lifecycle.State.READY);
            // Catch-up: if engine reached READY synchronously inside EngineEnvelope.start()
            // before our registration completed, onDependencyState would have missed it.
            // Self-fire the hydrator now.
            if (ctx.state("engine") == State.READY && ServerMain.this.hydrateMetadataThread == null) {
                onDependencyState("engine", State.STARTING, State.READY);
            }
        }

        @Override
        public void stop() {
            joinThread(ServerMain.this.hydrateMetadataThread, true);
            joinThread(ServerMain.this.compileViewsThread, true);
        }
    }

    /**
     * ILP-TCP protocol envelope: binds the InfluxDB Line Protocol TCP and UDP receivers early,
     * then gates the accept loop on engine==READY via onDependencyState.
     * Hard-dep on worker-pool-manager; soft-dep on engine.
     * Both LineTcpReceiver and LineUdpReceiver share one acceptOpen flag.
     * They are skipped when the instance is read-only or ILP-TCP is disabled.
     */
    private final class IlpTcpEnvelope implements io.questdb.lifecycle.Component {
        private final AtomicBoolean acceptOpen = new AtomicBoolean(false);
        private volatile LifecycleContext ctxRef;
        private final ObjList<String> hardDeps;
        private AbstractLineProtoUdpReceiver lineUdpReceiver;
        private LineTcpReceiver lineTcpReceiver;
        private final Log log;
        private final ObjList<String> softDeps;

        IlpTcpEnvelope(Log log) {
            this.log = log;
            this.hardDeps = new ObjList<>();
            this.hardDeps.add("worker-pool-manager");
            this.softDeps = new ObjList<>();
            this.softDeps.add("engine");
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return "ilp-tcp";
        }

        @Override
        public void onDependencyState(String depName, State previous, State current) {
            if ("engine".equals(depName) && current == State.READY) {
                acceptOpen.set(true);
                log.info().$("ilp-tcp envelope: accept loop open").$();
                if (ctxRef != null) {
                    ctxRef.publish(State.READY);
                }
            }
        }

        @Override
        public ObjList<String> softDependencies() {
            return softDeps;
        }

        @Override
        public void start(LifecycleContext ctx) {
            this.ctxRef = ctx;
            ctx.publish(State.STARTING);
            try {
                final ServerConfiguration cfg = ServerMain.this.bootstrap.getConfiguration();
                final boolean isReadOnly = cfg.getCairoConfiguration().isReadOnlyInstance();
                if (!isReadOnly && cfg.getLineTcpReceiverConfiguration().isEnabled()) {
                    this.lineTcpReceiver = ServerMain.this.services().createLineTcpReceiver(
                            cfg.getLineTcpReceiverConfiguration(),
                            ServerMain.this.engine,
                            ServerMain.this.workerPoolManager,
                            acceptOpen);
                    this.lineUdpReceiver = ServerMain.this.services().createLineUdpReceiver(
                            cfg.getLineUdpReceiverConfiguration(),
                            ServerMain.this.engine,
                            ServerMain.this.workerPoolManager,
                            acceptOpen);
                }
                log.info().$("ilp-tcp envelope: bound, accept paused").$();
                ctx.publish(State.DEGRADED);
                // Catch-up: if engine reached READY synchronously inside EngineEnvelope.start()
                // before our ctxRef was set, the onDependencyState dispatch was lost. Self-publish now.
                if (ctx.state("engine") == State.READY) {
                    acceptOpen.set(true);
                    log.info().$("ilp-tcp envelope: accept loop open (catch-up)").$();
                    ctx.publish(State.READY);
                }
            } catch (Throwable t) {
                // #090: free partially-allocated resources in reverse-construction order before
                // rethrow. The orchestrator's close loop skips FAILED components, so without this
                // wrap a mid-body throw leaks the LineTcpReceiver / LineUdpReceiver. Mirrors the
                // WR-09 addSuppressed pattern from PrimaryRoleState.openLoops.
                if (this.lineUdpReceiver != null) {
                    try {
                        Misc.free(this.lineUdpReceiver);
                        this.lineUdpReceiver = null;
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                }
                if (this.lineTcpReceiver != null) {
                    try {
                        Misc.free(this.lineTcpReceiver);
                        this.lineTcpReceiver = null;
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                }
                throw t;
            }
        }

        @Override
        public void stop() {
            Misc.free(lineTcpReceiver);
            lineTcpReceiver = null;
            Misc.free(lineUdpReceiver);
            lineUdpReceiver = null;
        }
    }

    /**
     * Lifecycle-owned mini HTTP server envelope serving /status, /health, /ping, and /lifecycle.
     * <p>
     * Binds before engine and backup-restore so that /status returns 200 throughout a
     * multi-minute PITR restore. Owns a dedicated WorkerPool named "http-min"
     * sized by http.min.worker.count (values less than or equal to zero
     * remap to 1 with a WARN log). Hard-deps on factory-provider only.
     * <p>
     * When http.min.enabled=false the envelope publishes READY without binding.
     */
    public class MinHttpEnvelope implements io.questdb.lifecycle.Component {
        private final ObjList<String> empty = new ObjList<>();
        private final ObjList<String> hardDeps;
        private final Log log;
        private WorkerPool pool;
        protected io.questdb.cutlass.http.HttpServer server;

        public MinHttpEnvelope(Log log) {
            this.log = log;
            this.hardDeps = new ObjList<>();
            this.hardDeps.add("factory-provider");
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return "min-http";
        }

        @Override
        public ObjList<String> softDependencies() {
            return empty;
        }

        @Override
        public void start(io.questdb.lifecycle.LifecycleContext ctx) {
            ctx.publish(io.questdb.lifecycle.State.STARTING);
            try {
                final io.questdb.cutlass.http.HttpServerConfiguration httpMinConfig =
                        ServerMain.this.bootstrap.getConfiguration().getHttpMinServerConfiguration();
                if (!httpMinConfig.isEnabled()) {
                    log.info().$("min-http envelope: http.min.enabled=false, skipping bind").$();
                    ctx.publish(io.questdb.lifecycle.State.READY);
                    return;
                }
                int workerCount = httpMinConfig.getWorkerCount();
                if (workerCount <= 0) {
                    log.advisoryW().$("min-http envelope: http.min.worker.count=").$(workerCount).$(" is <= 0, remapping to 1").$();
                    workerCount = 1;
                }
                pool = new WorkerPool(new MinHttpPoolConfiguration(workerCount));
                // createMinHttpServer() calls pool.assign() on the dispatcher and reschedule jobs.
                // The pool must NOT be started yet -- assign() asserts !running. Start after bind.
                server = ServerMain.this.services().createMinHttpServer(httpMinConfig, pool);
                if (server != null) {
                    final io.questdb.lifecycle.LifecycleOrchestrator orch = ServerMain.this.orchestrator;
                    server.bind(new io.questdb.cutlass.http.HttpRequestHandlerFactory() {
                        @Override
                        public io.questdb.std.ObjHashSet<String> getUrls() {
                            return httpMinConfig.getContextPathLifecycle();
                        }

                        @Override
                        public io.questdb.cutlass.http.HttpRequestHandler newInstance() {
                            return ServerMain.this.newLifecycleProcessor(httpMinConfig, orch);
                        }
                    });
                    ServerMain.this.bindAdditionalMinHttpHandlers(server, httpMinConfig, orch);
                }
                pool.start(log);
                ctx.publish(io.questdb.lifecycle.State.READY);
            } catch (Throwable t) {
                // Free partially-allocated resources before rethrow. The orchestrator's close loop
                // skips FAILED components, so without this wrap a mid-body throw would leak the
                // HttpServer and the WorkerPool. Halt the pool BEFORE freeing the server: the
                // server captured pool slots via assign() during construction and pool.start() may
                // already have launched worker threads, so a worker can be inside the dispatcher
                // touching native FD state. Freeing the server first releases that native memory
                // while a worker may still dereference it (a boot-fail use-after-free). pool.halt()
                // joins the workers, so once it returns no thread can run against the server, making
                // it safe to free. This matches the halt-then-free discipline stop() already uses.
                // Each branch aggregates its own teardown failure into the original throwable.
                if (pool != null) {
                    try {
                        pool.halt();
                        pool = null;
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                }
                if (server != null) {
                    try {
                        Misc.free(server);
                        server = null;
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                }
                throw t;
            }
        }

        @Override
        public void stop() {
            // Halt the dedicated http-min worker pool BEFORE freeing the server. A worker thread
            // can still be inside IODispatcherWindows.runSerially() touching the dispatcher's native
            // FDSet; freeing the server first releases that native memory and the in-flight select()
            // then dereferences a freed FDSet, crashing the JVM (EXCEPTION_ACCESS_VIOLATION on
            // Windows). pool.halt() joins the worker threads, so after it returns no thread can be
            // running select(), making it safe to free the server.
            if (pool != null) {
                pool.halt();
                pool = null;
            }
            Misc.free(server);
            server = null;
        }
    }

    /**
     * WorkerPoolConfiguration for the dedicated http-min pool.
     * Pool name is "http-min"; worker count is provided at construction time.
     */
    private static final class MinHttpPoolConfiguration implements io.questdb.mp.WorkerPoolConfiguration {
        private final int workerCount;

        MinHttpPoolConfiguration(int workerCount) {
            this.workerCount = workerCount;
        }

        @Override
        public String getPoolName() {
            return "http-min";
        }

        @Override
        public int getWorkerCount() {
            return workerCount;
        }
    }

    /**
     * PG-wire protocol envelope: binds the PostgreSQL wire protocol listener early,
     * then gates the accept loop on engine==READY via onDependencyState.
     * Hard-dep on worker-pool-manager; soft-dep on engine.
     * When PG wire is disabled the envelope publishes DEGRADED and waits for engine READY.
     */
    private final class PgWireEnvelope implements io.questdb.lifecycle.Component {
        private final AtomicBoolean acceptOpen = new AtomicBoolean(false);
        private volatile LifecycleContext ctxRef;
        private final ObjList<String> hardDeps;
        private final Log log;
        private PGServer server;
        private final ObjList<String> softDeps;

        PgWireEnvelope(Log log) {
            this.log = log;
            this.hardDeps = new ObjList<>();
            this.hardDeps.add("worker-pool-manager");
            this.softDeps = new ObjList<>();
            this.softDeps.add("engine");
        }

        PGServer getPgServer() {
            return server;
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return "pg-wire";
        }

        @Override
        public void onDependencyState(String depName, State previous, State current) {
            if ("engine".equals(depName) && current == State.READY) {
                acceptOpen.set(true);
                log.info().$("pg-wire envelope: accept loop open").$();
                if (ctxRef != null) {
                    ctxRef.publish(State.READY);
                }
            }
        }

        @Override
        public ObjList<String> softDependencies() {
            return softDeps;
        }

        @Override
        public void start(LifecycleContext ctx) {
            this.ctxRef = ctx;
            ctx.publish(State.STARTING);
            final ServerConfiguration cfg = ServerMain.this.bootstrap.getConfiguration();
            this.server = ServerMain.this.services().createPGWireServer(
                    cfg.getPGWireConfiguration(),
                    ServerMain.this.engine,
                    ServerMain.this.workerPoolManager,
                    acceptOpen);
            log.info().$("pg-wire envelope: bound, accept paused").$();
            ctx.publish(State.DEGRADED);
            // Catch-up: if engine reached READY synchronously inside EngineEnvelope.start()
            // before our ctxRef was set, the onDependencyState dispatch was lost. Self-publish now.
            if (ctx.state("engine") == State.READY) {
                acceptOpen.set(true);
                log.info().$("pg-wire envelope: accept loop open (catch-up)").$();
                ctx.publish(State.READY);
            }
        }

        @Override
        public void stop() {
            Misc.free(server);
            server = null;
        }
    }

    /**
     * QWIP (QuestDB Wire Protocol UDP) envelope: binds the QWP UDP receiver early,
     * then gates the accept loop on engine==READY via onDependencyState.
     * Hard-dep on worker-pool-manager; soft-dep on engine.
     * Skipped when the instance is read-only or QWIP is disabled.
     */
    public class QwipEnvelope implements io.questdb.lifecycle.Component {
        protected final AtomicBoolean acceptOpen = new AtomicBoolean(false);
        protected volatile LifecycleContext ctxRef;
        protected final Log log;
        protected QwpUdpReceiver receiver;
        private final ObjList<String> hardDeps;
        private final ObjList<String> softDeps;

        public QwipEnvelope(Log log) {
            this.log = log;
            this.hardDeps = new ObjList<>();
            this.hardDeps.add("worker-pool-manager");
            this.softDeps = new ObjList<>();
            this.softDeps.add("engine");
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return "qwip";
        }

        @Override
        public void onDependencyState(String depName, State previous, State current) {
            if ("engine".equals(depName) && current == State.READY) {
                // Consult the role-aware seam BEFORE flipping acceptOpen. OSS default returns
                // true; ENT overlay returns role == PRIMARY so a REPLICA boot never opens the
                // accept loop (#083).
                acceptOpen.set(shouldOpenAccept(ctxRef));
                log.info().$("qwip envelope: accept loop state on engine READY [open=").$(acceptOpen.get()).$(']').$();
                if (ctxRef != null) {
                    ctxRef.publish(State.READY);
                }
            }
        }

        @TestOnly
        public boolean isAcceptOpen() {
            return acceptOpen.get();
        }

        @TestOnly
        public QwpUdpReceiver getReceiver() {
            return receiver;
        }

        /**
         * Role-aware override seam. OSS standalone returns true (accept opens whenever engine
         * is READY). Enterprise overlays return role == PRIMARY so a REPLICA boot does not
         * briefly accept QWIP UDP datagrams (#083). The gate is consulted at the moment accept
         * could open, not after-the-fact, so the brief acceptance window on REPLICA boot
         * becomes impossible by construction.
         */
        protected boolean shouldOpenAccept(LifecycleContext ctx) {
            return true;
        }

        @Override
        public ObjList<String> softDependencies() {
            return softDeps;
        }

        @Override
        public void start(LifecycleContext ctx) {
            this.ctxRef = ctx;
            ctx.publish(State.STARTING);
            final ServerConfiguration cfg = ServerMain.this.bootstrap.getConfiguration();
            final boolean isReadOnly = cfg.getCairoConfiguration().isReadOnlyInstance();
            final QwpUdpReceiverConfiguration qwpCfg = cfg.getQwpUdpReceiverConfiguration();
            if (!isReadOnly && qwpCfg != null && qwpCfg.isEnabled()) {
                this.receiver = ServerMain.this.services().createQwpUdpReceiver(
                        qwpCfg,
                        ServerMain.this.engine,
                        ServerMain.this.workerPoolManager,
                        acceptOpen);
            }
            log.info().$("qwip envelope: bound, accept paused").$();
            ctx.publish(State.DEGRADED);
            // Catch-up: if engine reached READY before our ctxRef was set, the
            // onDependencyState dispatch was lost. Self-publish now.
            if (ctx.state("engine") == State.READY) {
                // Consult the role-aware seam on catch-up too -- otherwise the catch-up branch
                // would unconditionally flip acceptOpen=true and a REPLICA boot would briefly
                // accept datagrams (#083).
                acceptOpen.set(shouldOpenAccept(ctx));
                log.info().$("qwip envelope: accept loop state on engine READY (catch-up) [open=").$(acceptOpen.get()).$(']').$();
                ctx.publish(State.READY);
            }
        }

        @Override
        public void stop() {
            Misc.free(receiver);
            receiver = null;
        }
    }

    /**
     * Web-HTTP protocol envelope: binds the full-fat HTTP server early,
     * then gates the accept loop on engine==READY via onDependencyState.
     * Hard-deps on worker-pool-manager AND pg-wire (RESEARCH Section 6: FlushQueryCacheJob
     * needs the PGServer reference from PgWireEnvelope). Soft-dep on engine.
     */
    private final class WebHttpEnvelope implements io.questdb.lifecycle.Component {
        private final AtomicBoolean acceptOpen = new AtomicBoolean(false);
        private volatile LifecycleContext ctxRef;
        private final ObjList<String> hardDeps;
        private final Log log;
        private HttpServer server;
        private final ObjList<String> softDeps;

        WebHttpEnvelope(Log log) {
            this.log = log;
            this.hardDeps = new ObjList<>();
            this.hardDeps.add("worker-pool-manager");
            this.hardDeps.add("pg-wire");
            this.softDeps = new ObjList<>();
            this.softDeps.add("engine");
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return "web-http";
        }

        @Override
        public void onDependencyState(String depName, State previous, State current) {
            if ("engine".equals(depName) && current == State.READY) {
                acceptOpen.set(true);
                log.info().$("web-http envelope: accept loop open").$();
                if (ctxRef != null) {
                    ctxRef.publish(State.READY);
                }
            }
        }

        @Override
        public ObjList<String> softDependencies() {
            return softDeps;
        }

        @Override
        public void start(LifecycleContext ctx) {
            this.ctxRef = ctx;
            ctx.publish(State.STARTING);
            try {
                final ServerConfiguration cfg = ServerMain.this.bootstrap.getConfiguration();
                this.server = ServerMain.this.services().createHttpServer(
                        cfg,
                        ServerMain.this.engine,
                        ServerMain.this.workerPoolManager,
                        acceptOpen);
                // FlushQueryCacheJob per RESEARCH Section 6: owned by web-http;
                // reads the PGServer reference from PgWireEnvelope via cross-envelope lookup.
                final PGServer pgServer = ServerMain.this.findEnvelope("pg-wire", PgWireEnvelope.class).getPgServer();
                ServerMain.this.workerPoolManager.getSharedPoolNetwork().assign(
                        new FlushQueryCacheJob(ServerMain.this.engine.getMessageBus(), this.server, pgServer));
                log.info().$("web-http envelope: bound, accept paused").$();
                ctx.publish(State.DEGRADED);
                // Catch-up: if engine reached READY synchronously inside EngineEnvelope.start()
                // before our ctxRef was set, the onDependencyState dispatch was lost. Self-publish now.
                if (ctx.state("engine") == State.READY) {
                    acceptOpen.set(true);
                    log.info().$("web-http envelope: accept loop open (catch-up)").$();
                    ctx.publish(State.READY);
                }
            } catch (Throwable t) {
                // #090: free partially-allocated resources before rethrow. The orchestrator's
                // close loop skips FAILED components, so without this wrap a mid-body throw
                // (e.g. inside findEnvelope or the FlushQueryCacheJob.assign() step) leaks the
                // HttpServer. Mirrors the WR-09 addSuppressed pattern from
                // PrimaryRoleState.openLoops.
                if (this.server != null) {
                    try {
                        Misc.free(this.server);
                        this.server = null;
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                }
                throw t;
            }
        }

        @Override
        public void stop() {
            Misc.free(server);
            server = null;
        }
    }

    /**
     * Two-stage worker-pool-manager envelope:
     * Stage 1 -- configure pools (STARTING -> DEGRADED).
     * Stage 2 -- start pool threads when network-services reaches READY (DEGRADED -> READY).
     * <p>
     * The ctor concatenates {@code ["engine"]} with
     * {@link ServerMain#workerPoolManagerExtraHardDeps()} so subclass overrides
     * participate via polymorphic dispatch on {@code ServerMain.this}.
     */
    private final class WorkerPoolManagerEnvelope implements io.questdb.lifecycle.Component {
        private final ObjList<String> empty = new ObjList<>();
        private final ObjList<String> hardDeps;
        private final Log log;

        WorkerPoolManagerEnvelope(Log log) {
            this.log = log;
            // Concatenate base ["engine"] with subclass-supplied extra deps via the
            // workerPoolManagerExtraHardDeps() hook. Polymorphic dispatch: ServerMain.this is the actual
            // concrete instance (EntServerMain when launched via EntServerMain.main), so the subclass
            // override fires and the envelope picks up enterprise-only deps like "ent-pre-services".
            ObjList<String> deps = new ObjList<>();
            deps.add("engine");
            ObjList<String> extras = ServerMain.this.workerPoolManagerExtraHardDeps();
            for (int i = 0, n = extras.size(); i < n; i++) {
                deps.add(extras.getQuick(i));
            }
            this.hardDeps = deps;
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return "worker-pool-manager";
        }

        @Override
        public ObjList<String> softDependencies() {
            return empty;
        }

        @Override
        public void start(io.questdb.lifecycle.LifecycleContext ctx) {
            ctx.publish(io.questdb.lifecycle.State.STARTING);
            // Stage 1 -- verbatim lift of today's ServerMain.initialize() body lines :295-:398:
            // anonymous WorkerPoolManager subclass with configureWorkerPools override,
            // engine.buildViewGraphs(), setupDedicatedPools(), WAL apply on dedicated pool.
            // The 'workerPoolManager' field on ServerMain is assigned here.
            ServerMain.this.constructAndAssignWorkerPoolManager(log);
            ctx.publish(io.questdb.lifecycle.State.DEGRADED);
            // Stage 2 fires when all hard-required dependents of worker-pool-manager are stable.
            // The dependents are the 4 protocol envelopes (pg-wire, ilp-tcp, web-http, qwip).
            // When all 4 publish READY the orchestrator fires this onStableBelow callback.
            ctx.onStableBelow(name(), () -> {
                ServerMain.this.workerPoolManager.start(log);
                ctx.publish(io.questdb.lifecycle.State.READY);
                // Boot-tail: logBannerAndEndpoints runs after workerPoolManager.start(log)
                // to preserve original ordering where the banner fires after worker threads start.
                ServerMain.this.bootstrap.logBannerAndEndpoints(ServerMain.this.webConsoleSchema());
                // Boot-tail: DataID advisory, final GC, enjoy advisory.
                final DataID dataID = ServerMain.this.engine.getDataID();
                if (dataID.isInitialized()) {
                    final Uuid uuid = dataID.get();
                    ServerMain.this.bootstrap.getLog().advisoryW().$("data id: ").$(uuid).$();
                }
                System.gc();
                ServerMain.this.bootstrap.getLog().advisoryW().$("enjoy").$();
            });
        }

        @Override
        public void stop() {
            // Halts the worker pool as part of the orchestrator's reverse-topo stop.
            // ServerMain.close() also calls workerPoolManager.halt() before freeOnExit.close()
            // to ensure the pool is quiesced before engine resources are freed. WorkerPool.halt()
            // is CAS-guarded, so whichever call arrives second is a no-op.
            if (ServerMain.this.workerPoolManager != null) {
                ServerMain.this.workerPoolManager.halt();
            }
        }
    }

}
