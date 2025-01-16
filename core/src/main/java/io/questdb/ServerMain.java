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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.FlushQueryCacheJob;
import io.questdb.cairo.security.ReadOnlySecurityContextFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.auth.DefaultLineAuthenticatorFactory;
import io.questdb.cutlass.auth.EllipticCurveAuthenticatorFactory;
import io.questdb.cutlass.auth.LineAuthenticatorFactory;
import io.questdb.cutlass.http.DefaultHttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpContextConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.StaticHttpAuthenticatorFactory;
import io.questdb.cutlass.line.tcp.StaticChallengeResponseMatcher;
import io.questdb.cutlass.pgwire.IPGWireServer;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.pgwire.ReadOnlyUsersAwareSecurityContextFactory;
import io.questdb.cutlass.text.CopyJob;
import io.questdb.cutlass.text.CopyRequestJob;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.log.LogFactory;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.filewatch.FileWatcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.File;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerMain implements Closeable {
    private final Bootstrap bootstrap;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CairoEngine engine;
    private final FreeOnExit freeOnExit = new FreeOnExit();
    private final AtomicBoolean running = new AtomicBoolean();
    protected IPGWireServer pgWireServer;
    private FileWatcher fileWatcher;
    private HttpServer httpServer;
    private boolean initialized;
    private WorkerPoolManager workerPoolManager;

    public ServerMain(String... args) {
        this(new Bootstrap(args));
    }

    public ServerMain(final Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
        // create cairo engine
        engine = freeOnExit.register(bootstrap.newCairoEngine());
        try {
            final ServerConfiguration config = bootstrap.getConfiguration();
            config.init(engine, freeOnExit);
            freeOnExit.register(config.getFactoryProvider());
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
            protected void setupWalApplyJob(WorkerPool workerPool, CairoEngine engine, int sharedWorkerCount) {
            }
        };
    }

    public static HttpAuthenticatorFactory getHttpAuthenticatorFactory(ServerConfiguration configuration) {
        HttpFullFatServerConfiguration httpConfig = configuration.getHttpServerConfiguration();
        String username = httpConfig.getUsername();
        if (Chars.empty(username)) {
            return DefaultHttpAuthenticatorFactory.INSTANCE;
        }
        return new StaticHttpAuthenticatorFactory(username, httpConfig.getPassword());
    }

    public static LineAuthenticatorFactory getLineAuthenticatorFactory(ServerConfiguration configuration) {
        LineAuthenticatorFactory authenticatorFactory;
        // create default authenticator for Line TCP protocol
        if (configuration.getLineTcpReceiverConfiguration().isEnabled() && configuration.getLineTcpReceiverConfiguration().getAuthDB() != null) {
            // we need "root/" here, not "root/db/"
            final String rootDir = new File(configuration.getCairoConfiguration().getRoot()).getParent();
            final String absPath = new File(rootDir, configuration.getLineTcpReceiverConfiguration().getAuthDB()).getAbsolutePath();
            CharSequenceObjHashMap<PublicKey> authDb = AuthUtils.loadAuthDb(absPath);
            authenticatorFactory = new EllipticCurveAuthenticatorFactory(() -> new StaticChallengeResponseMatcher(authDb));
        } else {
            authenticatorFactory = DefaultLineAuthenticatorFactory.INSTANCE;
        }
        return authenticatorFactory;
    }

    public static SecurityContextFactory getSecurityContextFactory(ServerConfiguration configuration) {
        boolean readOnlyInstance = configuration.getCairoConfiguration().isReadOnlyInstance();
        if (readOnlyInstance) {
            return ReadOnlySecurityContextFactory.INSTANCE;
        } else {
            PGWireConfiguration pgWireConfiguration = configuration.getPGWireConfiguration();
            HttpContextConfiguration httpContextConfiguration = configuration.getHttpServerConfiguration().getHttpContextConfiguration();
            boolean pgWireReadOnlyContext = pgWireConfiguration.readOnlySecurityContext();
            boolean pgWireReadOnlyUserEnabled = pgWireConfiguration.isReadOnlyUserEnabled();
            String pgWireReadOnlyUsername = pgWireReadOnlyUserEnabled ? pgWireConfiguration.getReadOnlyUsername() : null;
            boolean httpReadOnly = httpContextConfiguration.readOnlySecurityContext();
            return new ReadOnlyUsersAwareSecurityContextFactory(pgWireReadOnlyContext, pgWireReadOnlyUsername, httpReadOnly);
        }
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
            System.err.println("QuestDB is shutting down...");
            System.out.println("QuestDB is shutting down...");
            if (bootstrap != null && bootstrap.getLog() != null) {
                // Still useful in case of custom logger
                bootstrap.getLog().info().$("QuestDB is shutting down...").$();
            }
            if (initialized) {
                workerPoolManager.halt();
                fileWatcher = Misc.free(fileWatcher);
            }
            freeOnExit.close();
        }
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
        pgWireServer.resetQueryCache();
    }

    public void start() {
        start(false);
    }

    public synchronized void start(boolean addShutdownHook) {
        if (!closed.get() && running.compareAndSet(false, true)) {
            initialize();

            if (addShutdownHook) {
                addShutdownHook();
            }
            workerPoolManager.start(bootstrap.getLog());
            bootstrap.logBannerAndEndpoints(webConsoleSchema());
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

    private synchronized void initialize() {
        initialized = true;
        final ServerConfiguration config = bootstrap.getConfiguration();
        // create the worker pool manager, and configure the shared pool
        final boolean walSupported = config.getCairoConfiguration().isWalSupported();
        final boolean isReadOnly = config.getCairoConfiguration().isReadOnlyInstance();
        final boolean walApplyEnabled = config.getCairoConfiguration().isWalApplyEnabled();
        final CairoConfiguration cairoConfig = config.getCairoConfiguration();

        workerPoolManager = new WorkerPoolManager(config) {
            @Override
            protected void configureSharedPool(WorkerPool sharedPool) {
                try {
                    sharedPool.assign(engine.getEngineMaintenanceJob());

                    WorkerPoolUtils.setupQueryJobs(sharedPool, engine);

                    QueryTracingJob queryTracingJob = new QueryTracingJob(engine);
                    sharedPool.assign(queryTracingJob);
                    freeOnExit.register(queryTracingJob);

                    if (!isReadOnly) {
                        WorkerPoolUtils.setupWriterJobs(sharedPool, engine);

                        if (walSupported) {
                            sharedPool.assign(config.getFactoryProvider().getWalJobFactory().createCheckWalTransactionsJob(engine));
                            final WalPurgeJob walPurgeJob = config.getFactoryProvider().getWalJobFactory().createWalPurgeJob(engine);
                            engine.setWalPurgeJobRunLock(walPurgeJob.getRunLock());
                            walPurgeJob.delayByHalfInterval();
                            sharedPool.assign(walPurgeJob);
                            sharedPool.freeOnExit(walPurgeJob);

                            // wal apply job in the shared pool when there is no dedicated pool
                            if (walApplyEnabled && !config.getWalApplyPoolConfiguration().isEnabled()) {
                                setupWalApplyJob(sharedPool, engine, sharedPool.getWorkerCount());
                            }
                        }

                        // text import
                        CopyJob.assignToPool(engine.getMessageBus(), sharedPool);
                        if (!Chars.empty(cairoConfig.getSqlCopyInputRoot())) {
                            final CopyRequestJob copyRequestJob = new CopyRequestJob(
                                    engine,
                                    // save CPU resources for collecting and processing jobs
                                    Math.max(1, sharedPool.getWorkerCount() - 2)
                            );
                            sharedPool.assign(copyRequestJob);
                            sharedPool.freeOnExit(copyRequestJob);
                        }
                    }

                    // telemetry
                    if (!cairoConfig.getTelemetryConfiguration().getDisableCompletely()) {
                        final TelemetryJob telemetryJob = new TelemetryJob(engine);
                        freeOnExit.register(telemetryJob);
                        if (cairoConfig.getTelemetryConfiguration().getEnabled()) {
                            sharedPool.assign(telemetryJob);
                        }
                    }

                } catch (Throwable thr) {
                    throw new Bootstrap.BootstrapException(thr);
                }
            }
        };

        if (walApplyEnabled && !isReadOnly && walSupported && config.getWalApplyPoolConfiguration().isEnabled()) {
            WorkerPool walApplyWorkerPool = workerPoolManager.getInstance(
                    config.getWalApplyPoolConfiguration(),
                    WorkerPoolManager.Requester.WAL_APPLY
            );
            setupWalApplyJob(walApplyWorkerPool, engine, workerPoolManager.getSharedWorkerCount());
        }

        // http
        freeOnExit.register(httpServer = services().createHttpServer(
                config,
                engine,
                workerPoolManager
        ));

        // http min
        freeOnExit.register(services().createMinHttpServer(
                config.getHttpMinServerConfiguration(),
                workerPoolManager
        ));

        // pg wire
        freeOnExit.register(pgWireServer = services().createPGWireServer(
                config.getPGWireConfiguration(),
                engine,
                workerPoolManager
        ));

        workerPoolManager.getSharedPool().assign(new FlushQueryCacheJob(
                engine.getMessageBus(),
                httpServer,
                pgWireServer
        ));

        if (!isReadOnly && config.getLineTcpReceiverConfiguration().isEnabled()) {
            // ilp/tcp
            freeOnExit.register(services().createLineTcpReceiver(
                    config.getLineTcpReceiverConfiguration(),
                    engine,
                    workerPoolManager
            ));

            // ilp/udp
            freeOnExit.register(services().createLineUdpReceiver(
                    config.getLineUdpReceiverConfiguration(),
                    engine,
                    workerPoolManager
            ));
        }

        // metadata hydration
        Thread hydrateCairoMetadataThread = new Thread(engine.getMetadataCache()::onStartupAsyncHydrator);
        hydrateCairoMetadataThread.start();

        System.gc(); // GC 1
        bootstrap.getLog().advisoryW().$("server is ready to be started").$();
    }

    protected Services services() {
        return Services.INSTANCE;
    }

    protected void setupWalApplyJob(
            WorkerPool workerPool,
            CairoEngine engine,
            int sharedWorkerCount
    ) {
        for (int i = 0, workerCount = workerPool.getWorkerCount(); i < workerCount; i++) {
            // create job per worker
            final ApplyWal2TableJob applyWal2TableJob = new ApplyWal2TableJob(engine, workerCount, sharedWorkerCount);
            workerPool.assign(i, applyWal2TableJob);
            workerPool.freeOnExit(applyWal2TableJob);
        }
    }

    protected String webConsoleSchema() {
        return "http";
    }
}
