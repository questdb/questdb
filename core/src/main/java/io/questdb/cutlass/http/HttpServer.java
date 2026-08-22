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

package io.questdb.cutlass.http;

import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.AcceptGatedJob;
import io.questdb.cutlass.http.processors.ExportQueryProcessor;
import io.questdb.cutlass.http.processors.LineHttpPingProcessor;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.SettingsProcessor;
import io.questdb.cutlass.http.processors.StaticContentProcessorFactory;
import io.questdb.cutlass.http.processors.TableStatusCheckProcessor;
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.cutlass.http.processors.WarningsProcessor;
import io.questdb.cutlass.qwp.server.QwpIngressHttpProcessor;
import io.questdb.cutlass.qwp.server.egress.QwpEgressHttpProcessor;
import io.questdb.mp.ConcurrentPool;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeConfigurationListener;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.network.HeartBeatException;
import io.questdb.network.IOContextFactoryImpl;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.SocketFactory;
import io.questdb.std.AssociativeCache;
import io.questdb.std.ConcurrentAssociativeCache;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.NoOpAssociativeCache;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class HttpServer implements Closeable {
    static final NoOpAssociativeCache<RecordCursorFactory> NO_OP_CACHE = new NoOpAssociativeCache<>();
    private final AtomicBoolean acceptOpen;
    private final ActiveConnectionTracker activeConnectionTracker;
    private final ObjList<Closeable> closeables = new ObjList<>();
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final @Nullable FiberRuntime fiberRuntime;
    private final HttpContextFactory httpContextFactory;
    private final WaitProcessor rescheduleContext;
    private final AssociativeCache<RecordCursorFactory> selectCache;
    private final HttpRequestProcessorSelectorFactory selectorFactory;
    private final int workerCount;

    public HttpServer(
            HttpServerConfiguration configuration,
            WorkerPool networkSharedPool,
            SocketFactory socketFactory
    ) {
        this(configuration, networkSharedPool, socketFactory, new AtomicBoolean(true), true, null);
    }

    public HttpServer(
            HttpServerConfiguration configuration,
            WorkerPool networkSharedPool,
            SocketFactory socketFactory,
            AtomicBoolean acceptOpen
    ) {
        this(configuration, networkSharedPool, socketFactory, acceptOpen, true, null);
    }

    private HttpServer(
            HttpServerConfiguration configuration,
            WorkerPool networkSharedPool,
            SocketFactory socketFactory,
            AtomicBoolean acceptOpen,
            boolean isFiberExecutionEnabled,
            @Nullable Runnable afterSelectorPop
    ) {
        this.acceptOpen = acceptOpen;
        this.workerCount = networkSharedPool.getWorkerCount();
        IODispatcher<HttpConnectionContext> dispatcher = null;
        HttpContextFactory httpContextFactory = null;
        WaitProcessor rescheduleContext = null;
        AssociativeCache<RecordCursorFactory> selectCache = null;
        HttpRequestProcessorSelectorFactory selectorFactory = null;
        FiberRuntime fiberRuntime = null;
        try {
            if (isFiberExecutionEnabled && configuration.isFiberEnabled() && networkSharedPool.isFiberHost()) {
                fiberRuntime = networkSharedPool.getFiberRuntime();
            }
            selectorFactory = new HttpRequestProcessorSelectorFactory(
                    workerCount,
                    fiberRuntime == null ? workerCount : networkSharedPool.getFiberMaxLiveCount(),
                    afterSelectorPop
            );
            if (fiberRuntime != null) {
                fiberRuntime.registerConfigurationListener(selectorFactory);
            }
            if (configuration instanceof HttpFullFatServerConfiguration serverConfiguration
                    && serverConfiguration.isQueryCacheEnabled()) {
                selectCache = new ConcurrentAssociativeCache<>(serverConfiguration.getConcurrentCacheConfiguration());
            } else {
                selectCache = NO_OP_CACHE;
            }
            ActiveConnectionTracker activeConnectionTracker = new ActiveConnectionTracker(configuration.getHttpContextConfiguration());
            httpContextFactory = new HttpContextFactory(
                    configuration,
                    socketFactory,
                    selectCache,
                    activeConnectionTracker
            );
            dispatcher = IODispatchers.create(configuration, httpContextFactory);
            rescheduleContext = new WaitProcessor(configuration.getWaitProcessorConfiguration(), dispatcher);

            this.activeConnectionTracker = activeConnectionTracker;
            this.dispatcher = dispatcher;
            this.fiberRuntime = fiberRuntime;
            this.httpContextFactory = httpContextFactory;
            this.rescheduleContext = rescheduleContext;
            this.selectCache = selectCache;
            this.selectorFactory = selectorFactory;

            networkSharedPool.assign(new AcceptGatedJob(dispatcher, acceptOpen));
            networkSharedPool.assign(new AcceptGatedJob(rescheduleContext, acceptOpen));
            for (int i = 0; i < workerCount; i++) {
                final HttpRequestProcessorSelectorImpl selector = fiberRuntime == null
                        ? selectorFactory.getSelectorByWorker(i)
                        : null;
                networkSharedPool.assign(i, new HttpRequestJob(
                        this,
                        dispatcher,
                        rescheduleContext,
                        selectorFactory,
                        selector,
                        acceptOpen,
                        fiberRuntime
                ));
                networkSharedPool.assignThreadLocalCleaner(i, this.httpContextFactory::freeThreadLocal);
            }
        } catch (Throwable t) {
            acceptOpen.set(false);
            if (fiberRuntime != null && selectorFactory != null) {
                fiberRuntime.unregisterConfigurationListener(selectorFactory);
            }
            Misc.free(dispatcher, t);
            Misc.free(rescheduleContext, t);
            Misc.free(selectorFactory, t);
            Misc.free(httpContextFactory, t);
            if (selectCache != NO_OP_CACHE) {
                Misc.free(selectCache, t);
            }
            throw t;
        }
    }

    public static void addDefaultEndpoints(
            HttpServer server,
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            int sharedQueryWorkerCount,
            HttpRequestHandlerBuilder jsonQueryProcessorBuilder,
            HttpRequestHandlerBuilder ilpV2WriteProcessorBuilder,
            HttpRequestHandlerBuilder sqlValidationProcessorBuilder
    ) {
        final HttpFullFatServerConfiguration httpServerConfiguration = serverConfiguration.getHttpServerConfiguration();
        final LineHttpProcessorConfiguration lineHttpProcessorConfiguration = httpServerConfiguration.getLineHttpProcessorConfiguration();
        if (httpServerConfiguration.isEnabled() && lineHttpProcessorConfiguration.isEnabled()) {

            server.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpServerConfiguration.getContextPathILP();
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return ilpV2WriteProcessorBuilder.newInstance();
                }
            });

            LineHttpPingProcessor pingProcessor = new LineHttpPingProcessor(
                    httpServerConfiguration.getLineHttpProcessorConfiguration().getInfluxPingVersion()
            );
            server.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpServerConfiguration.getContextPathILPPing();
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return pingProcessor;
                }
            });

            // QWP v1 endpoint (WebSocket only)
            server.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpServerConfiguration.getContextPathQWP();
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new QwpIngressHttpProcessor(cairoEngine, httpServerConfiguration);
                }
            });

            // QWP egress endpoint (query results, WebSocket only)
            server.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpServerConfiguration.getContextPathQWPRead();
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new QwpEgressHttpProcessor(cairoEngine, httpServerConfiguration, sharedQueryWorkerCount);
                }
            });
        }

        final SettingsProcessor settingsProcessor = new SettingsProcessor(cairoEngine, serverConfiguration);
        server.bind(new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return httpServerConfiguration.getContextPathSettings();
            }

            @Override
            public HttpRequestHandler newInstance() {
                return settingsProcessor;
            }
        });

        final WarningsProcessor warningsProcessor = new WarningsProcessor(serverConfiguration.getCairoConfiguration());
        server.bind(new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return httpServerConfiguration.getContextPathWarnings();
            }

            @Override
            public HttpRequestHandler newInstance() {
                return warningsProcessor;
            }
        });

        server.bind(new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return httpServerConfiguration.getContextPathExec();
            }

            @Override
            public HttpRequestHandler newInstance() {
                return jsonQueryProcessorBuilder.newInstance();
            }
        });

        server.bind(new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return httpServerConfiguration.getContextPathImport();
            }

            @Override
            public HttpRequestHandler newInstance() {
                return new TextImportProcessor(cairoEngine, httpServerConfiguration.getJsonQueryProcessorConfiguration());
            }
        });

        server.bind(new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return httpServerConfiguration.getContextPathSqlValidation();
            }

            @Override
            public HttpRequestHandler newInstance() {
                return sqlValidationProcessorBuilder.newInstance();
            }
        });

        server.bind(new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return httpServerConfiguration.getContextPathExport();
            }

            @Override
            public HttpRequestHandler newInstance() {
                return new ExportQueryProcessor(
                        httpServerConfiguration.getJsonQueryProcessorConfiguration(),
                        cairoEngine,
                        sharedQueryWorkerCount
                );
            }
        });

        server.bind(new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return httpServerConfiguration.getContextPathTableStatus();
            }

            @Override
            public HttpRequestHandler newInstance() {
                return new TableStatusCheckProcessor(cairoEngine, httpServerConfiguration.getJsonQueryProcessorConfiguration());
            }
        });

        server.bind(new StaticContentProcessorFactory(cairoEngine, httpServerConfiguration));
    }

    public static HttpServer createMinHttpServer(
            HttpServerConfiguration configuration,
            WorkerPool networkSharedPool,
            SocketFactory socketFactory
    ) {
        return new HttpServer(
                configuration,
                networkSharedPool,
                socketFactory,
                new AtomicBoolean(true),
                false,
                null
        );
    }

    @TestOnly
    public static HttpServer createWithSelectorPopHookForTesting(
            HttpServerConfiguration configuration,
            WorkerPool networkSharedPool,
            SocketFactory socketFactory,
            Runnable afterSelectorPop
    ) {
        return new HttpServer(
                configuration,
                networkSharedPool,
                socketFactory,
                new AtomicBoolean(true),
                true,
                afterSelectorPop
        );
    }

    public static Utf8Sequence normalizeUrl(DirectUtf8String url) {
        long p = url.ptr();
        long shift = 0;
        boolean lastSlash = false;
        for (int i = 0, n = url.size(); i < n; i++) {
            byte b = url.byteAt(i);
            if (b == '/') {
                if (lastSlash) {
                    shift++;
                    continue;
                } else {
                    lastSlash = true;
                }
            } else {
                lastSlash = false;
            }
            if (shift > 0) {
                Unsafe.putByte(p + i - shift, b);
            }
        }
        url.squeezeHi(shift);
        return url;
    }

    @TestOnly
    public static boolean runFiberRequestJobForTesting(
            IODispatcher<HttpConnectionContext> dispatcher,
            WaitProcessor rescheduleContext,
            FiberRuntime runtime
    ) {
        try (HttpRequestProcessorSelectorFactory selectorFactory =
                     new HttpRequestProcessorSelectorFactory(1, 1)) {
            return new HttpRequestJob(
                    null,
                    dispatcher,
                    rescheduleContext,
                    selectorFactory,
                    null,
                    new AtomicBoolean(true),
                    runtime
            ).run();
        }
    }

    public void bind(HttpRequestHandlerFactory factory) {
        bind(factory, false);
    }

    public void bind(HttpRequestHandlerFactory factory, boolean useAsDefault) {
        selectorFactory.bind(factory, useAsDefault);
    }

    public void clearSelectCache() {
        selectCache.clear();
    }

    @Override
    public void close() {
        acceptOpen.set(false);
        if (fiberRuntime != null) {
            fiberRuntime.unregisterConfigurationListener(selectorFactory);
        }
        Throwable failure = Misc.freeBestEffort(null, dispatcher);
        failure = Misc.freeBestEffort(failure, rescheduleContext);
        failure = Misc.freeBestEffort(failure, selectorFactory);
        failure = Misc.freeObjListBestEffort(failure, closeables);
        closeables.clear();
        failure = Misc.freeBestEffort(failure, httpContextFactory);
        // NO_OP_CACHE is a JVM-wide singleton shared by every cache-disabled server.
        if (selectCache != NO_OP_CACHE) {
            failure = Misc.freeBestEffort(failure, selectCache);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    @TestOnly
    public void createSelectorForTesting() {
        Misc.free(selectorFactory.create());
    }

    @TestOnly
    public int getMaxRecycledSelectorCountForTesting() {
        return selectorFactory.maxRecycledSelectors;
    }

    @TestOnly
    public int getRecycledSelectorCountForTesting() {
        return selectorFactory.recycledSelectors.count();
    }

    @TestOnly
    public HttpRequestProcessorSelector getSelectorByWorkerForTesting(int workerIndex) {
        return selectorFactory.getSelectorByWorker(workerIndex);
    }

    @TestOnly
    public HttpRequestProcessorSelector acquireSelectorForTesting() {
        return selectorFactory.acquire();
    }

    @TestOnly
    public void releaseSelectorForTesting(HttpRequestProcessorSelector selector) {
        selectorFactory.release((HttpRequestProcessorSelectorImpl) selector);
    }

    public ActiveConnectionTracker getActiveConnectionTracker() {
        return activeConnectionTracker;
    }

    public int getPort() {
        return dispatcher.getPort();
    }

    @TestOnly
    public WaitProcessor getWaitProcessor() {
        return rescheduleContext;
    }

    public void registerClosable(Closeable closeable) {
        closeables.add(closeable);
    }

    private boolean handleClientOperation(
            HttpConnectionContext context,
            int operation,
            HttpRequestProcessorSelector selector,
            WaitProcessor rescheduleContext,
            IODispatcher<HttpConnectionContext> dispatcher
    ) {
        try {
            return context.handleClientOperation(operation, selector, rescheduleContext);
        } catch (HeartBeatException e) {
            dispatcher.registerChannel(context, IOOperation.HEARTBEAT);
        } catch (PeerIsSlowToReadException e) {
            dispatcher.registerChannel(context, IOOperation.WRITE);
        } catch (ServerDisconnectException e) {
            dispatcher.disconnect(context, context.getDisconnectReason());
        } catch (PeerIsSlowToWriteException e) {
            dispatcher.registerChannel(context, IOOperation.READ);
        }
        return false;
    }

    @FunctionalInterface
    public interface HttpRequestHandlerBuilder {
        HttpRequestHandler newInstance();
    }

    private static class HttpContextFactory extends IOContextFactoryImpl<HttpConnectionContext> {

        public HttpContextFactory(
                HttpServerConfiguration configuration,
                SocketFactory socketFactory,
                AssociativeCache<RecordCursorFactory> selectCache,
                ActiveConnectionTracker activeConnectionTracker
        ) {
            super(
                    () -> new HttpConnectionContext(
                            configuration,
                            socketFactory,
                            selectCache,
                            activeConnectionTracker
                    ),
                    configuration.getHttpContextConfiguration().getConnectionPoolInitialCapacity()
            );
        }
    }

    private static final class HttpRequestJob implements Job {
        private final AtomicBoolean acceptOpen;
        private final IODispatcher<HttpConnectionContext> dispatcher;
        private final @Nullable FiberRuntime fiberRuntime;
        private final HttpServer owner;
        private final IORequestProcessor<HttpConnectionContext> processor;
        private @Nullable Fiber reservedFiber;
        private long reservedFiberEpoch;
        private final WaitProcessor rescheduleContext;
        private final @Nullable WaitProcessor.RetryLauncher retryLauncher;
        private final HttpRequestProcessorSelectorImpl selector;
        private final HttpRequestProcessorSelectorFactory selectorFactory;

        HttpRequestJob(
                HttpServer owner,
                IODispatcher<HttpConnectionContext> dispatcher,
                WaitProcessor rescheduleContext,
                HttpRequestProcessorSelectorFactory selectorFactory,
                HttpRequestProcessorSelectorImpl selector,
                AtomicBoolean acceptOpen,
                @Nullable FiberRuntime fiberRuntime
        ) {
            this.owner = owner;
            this.dispatcher = dispatcher;
            this.rescheduleContext = rescheduleContext;
            this.selectorFactory = selectorFactory;
            this.selector = selector;
            this.acceptOpen = acceptOpen;
            this.fiberRuntime = fiberRuntime;
            if (fiberRuntime != null) {
                final FiberRuntime runtime = fiberRuntime;
                this.processor = (operation, context, disp) -> {
                    if (operation == IOOperation.HEARTBEAT) {
                        disp.registerChannel(context, IOOperation.HEARTBEAT);
                        return false;
                    }
                    Fiber fiber = reservedFiber;
                    long reservationEpoch = reservedFiberEpoch;
                    if (fiber == null) {
                        fiber = runtime.tryReserveFiber();
                        if (fiber == null) {
                            // saturated: hand the connection back to the interest list and let the worker
                            // back off, rather than leaving the event - and every heartbeat behind it - stuck
                            disp.registerChannel(context, operation);
                            return false;
                        }
                        reservationEpoch = fiber.getReservationEpoch();
                    }
                    reservedFiber = fiber;
                    reservedFiberEpoch = reservationEpoch;
                    final HttpConnectionFiberTask task = context.getFiberTask(disp, selectorFactory, rescheduleContext);
                    reservedFiber = null;
                    reservedFiberEpoch = 0;
                    return handleLaunchResult(
                            context,
                            task.launchReserved(runtime, fiber, reservationEpoch, operation)
                    );
                };
                this.retryLauncher = (fiber, reservationEpoch, retry, taskIncarnation) -> {
                    try {
                        final HttpConnectionContext context = (HttpConnectionContext) retry;
                        final HttpConnectionFiberTask task = context.getFiberTask(dispatcher, selectorFactory, rescheduleContext);
                        handleLaunchResult(
                                context,
                                task.launchRerunReserved(runtime, fiber, reservationEpoch, taskIncarnation)
                        );
                    } finally {
                        runtime.releaseReservedFiber(fiber, reservationEpoch);
                    }
                };
            } else {
                this.processor = (operation, context, disp) ->
                        owner.handleClientOperation(context, operation, this.selector, rescheduleContext, disp);
                this.retryLauncher = null;
            }
        }

        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            if (!acceptOpen.get()) {
                return false;
            }
            final FiberRuntime runtime = fiberRuntime;
            if (runtime != null) {
                boolean useful = false;
                if (dispatcher.hasPendingIOEvents()) {
                    reservedFiber = runtime.tryReserveFiber();
                    if (reservedFiber != null) {
                        reservedFiberEpoch = reservedFiber.getReservationEpoch();
                        try {
                            useful = dispatcher.processIOQueue(processor);
                        } finally {
                            final Fiber unusedFiber = reservedFiber;
                            final long unusedFiberEpoch = reservedFiberEpoch;
                            reservedFiber = null;
                            reservedFiberEpoch = 0;
                            if (unusedFiber != null) {
                                runtime.releaseReservedFiber(unusedFiber, unusedFiberEpoch);
                            }
                        }
                    }
                }
                final WaitProcessor.RetryLauncher launcher = retryLauncher;
                if (launcher == null) {
                    throw new IllegalStateException("HTTP retry launcher is not configured");
                }
                useful |= rescheduleContext.launchReruns(runtime, launcher);
                return useful;
            }
            selectorFactory.populateMissing(selector);
            boolean useful = dispatcher.processIOQueue(processor);
            useful |= rescheduleContext.runReruns(selector);
            return useful;
        }

        private boolean handleLaunchResult(HttpConnectionContext context, LaunchResult result) {
            if (result == LaunchResult.LAUNCHED
                    || result == LaunchResult.ALREADY_OWNED
                    || result == LaunchResult.STALE_INCARNATION
                    || result == LaunchResult.TERMINAL) {
                return true;
            }
            context.abandonRetry();
            dispatcher.disconnect(
                    context,
                    result == LaunchResult.QUIESCING
                            ? IODispatcher.DISCONNECT_REASON_SERVER_SHUTDOWN
                            : IODispatcher.DISCONNECT_REASON_SERVER_ERROR
            );
            return false;
        }
    }

    /**
     * Maintains a master list of {@link HttpRequestHandlerFactory}
     * registrations and creates isolated selectors for workers, continuation
     * clones, and fiber task steps. Each {@link #create()} call walks the
     * master list and calls {@code factory.newInstance()} per registered URL,
     * so every selector gets its own handler instances.
     * <p>
     * Handler ids are pre-assigned in {@link #bind(HttpRequestHandlerFactory,
     * boolean)} so that the same URL maps to the same handler id across
     * every selector this factory ever creates.
     */
    static class HttpRequestProcessorSelectorFactory implements Closeable, FiberRuntimeConfigurationListener {
        private final ObjList<FactoryHolder> factoryHolders = new ObjList<>();
        private final AtomicBoolean isClosed = new AtomicBoolean();
        private int nextHandlerId = 0;
        private volatile int publishedFactoryCount;
        // Lock-free: acquire/release run twice per HTTP I/O event on every worker, so a server-wide
        // monitor here would serialize the whole request path.
        private final ConcurrentPool<HttpRequestProcessorSelectorImpl> recycledSelectors;
        // Per-worker selectors used by the Jobs registered to the pool in legacy mode. These
        // selectors are NOT pooled -- they live for the server's lifetime so the per-worker fast
        // path doesn't have to re-acquire across iterations.
        private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
        private volatile int maxRecycledSelectors;

        HttpRequestProcessorSelectorFactory(int workerCount, int maxRecycledSelectors) {
            this(workerCount, maxRecycledSelectors, null);
        }

        HttpRequestProcessorSelectorFactory(
                int workerCount,
                int maxRecycledSelectors,
                @Nullable Runnable afterSelectorPop
        ) {
            this.maxRecycledSelectors = Math.max(1, maxRecycledSelectors);
            this.recycledSelectors = afterSelectorPop == null
                    ? new ConcurrentPool<>()
                    : new ConcurrentPool<>() {
                @Override
                public HttpRequestProcessorSelectorImpl pop() {
                    final HttpRequestProcessorSelectorImpl selector = super.pop();
                    if (selector != null) {
                        afterSelectorPop.run();
                    }
                    return selector;
                }
            };
            this.selectors = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                selectors.add(null);
            }
        }

        @Override
        public void close() {
            if (!isClosed.compareAndSet(false, true)) {
                return;
            }
            maxRecycledSelectors = 0;
            Throwable failure = Misc.freeObjListBestEffort(null, selectors);
            selectors.clear();
            failure = trimRecycledSelectors(failure);
            CairoException.rethrowCleanupFailure(failure);
        }

        public HttpRequestProcessorSelectorImpl getSelectorByWorker(int jobIndex) {
            HttpRequestProcessorSelectorImpl s = selectors.getQuick(jobIndex);
            if (s == null) {
                s = create();
                selectors.setQuick(jobIndex, s);
            } else {
                try {
                    populateMissing(s);
                } catch (Throwable th) {
                    selectors.setQuick(jobIndex, null);
                    Misc.free(s, th);
                    throw th;
                }
            }
            return s;
        }

        HttpRequestProcessorSelectorImpl acquire() {
            while (true) {
                if (isClosed.get()) {
                    throw new IllegalStateException("HTTP selector factory is closed");
                }
                final HttpRequestProcessorSelectorImpl selector = recycledSelectors.pop();
                if (selector != null) {
                    if (isClosed.get()) {
                        final IllegalStateException exception = new IllegalStateException("HTTP selector factory is closed");
                        Misc.free(selector, exception);
                        throw exception;
                    }
                    try {
                        populateMissing(selector);
                        return selector;
                    } catch (Throwable th) {
                        Misc.free(selector, th);
                        throw th;
                    }
                }
                if (recycledSelectors.count() == 0) {
                    if (isClosed.get()) {
                        throw new IllegalStateException("HTTP selector factory is closed");
                    }
                    return create();
                }
                Os.pause();
            }
        }

        synchronized void bind(HttpRequestHandlerFactory factory, boolean useAsDefault) {
            final FactoryHolder holder = new FactoryHolder(factory, useAsDefault);
            final ObjHashSet<String> urls = factory.getUrls();
            assert urls != null;
            for (int j = 0, n = urls.size(); j < n; j++) {
                holder.handlerIds.add(nextHandlerId++);
            }
            factoryHolders.add(holder);
            publishedFactoryCount = factoryHolders.size();
        }

        HttpRequestProcessorSelectorImpl create() {
            final HttpRequestProcessorSelectorImpl selector = new HttpRequestProcessorSelectorImpl();
            try {
                populateMissing(selector);
                return selector;
            } catch (Throwable th) {
                Misc.free(selector, th);
                throw th;
            }
        }

        @Override
        public void onConfigurationChanged(int maxLiveFiberCount, int maxRetainedFiberCount) {
            if (isClosed.get()) {
                return;
            }
            maxRecycledSelectors = Math.max(1, maxLiveFiberCount);
            if (isClosed.get()) {
                maxRecycledSelectors = 0;
            }
            trimRecycledSelectors();
        }

        void release(HttpRequestProcessorSelectorImpl selector) {
            final int maxRecycledSelectors = this.maxRecycledSelectors;
            final boolean isPushed;
            try {
                isPushed = recycledSelectors.tryPush(selector, maxRecycledSelectors);
            } catch (Throwable th) {
                Misc.free(selector, th);
                throw th;
            }
            if (isPushed) {
                if (maxRecycledSelectors != this.maxRecycledSelectors) {
                    trimRecycledSelectors();
                }
                return;
            }
            Misc.free(selector);
        }

        private void trimRecycledSelectors() {
            CairoException.rethrowCleanupFailure(trimRecycledSelectors(null));
        }

        private Throwable trimRecycledSelectors(Throwable failure) {
            while (recycledSelectors.count() > maxRecycledSelectors) {
                final HttpRequestProcessorSelectorImpl selector = recycledSelectors.pop();
                if (selector == null) {
                    return failure;
                }
                failure = Misc.freeBestEffort(failure, selector);
            }
            return failure;
        }

        private void populateMissing(HttpRequestProcessorSelectorImpl selector) {
            final int factoryCount = publishedFactoryCount;
            for (int i = selector.factoryCount; i < factoryCount; i++) {
                populate(selector, factoryHolders.getQuick(i));
                selector.factoryCount = i + 1;
            }
        }

        private static HttpRequestHandler getOrCreateHandler(
                HttpRequestProcessorSelectorImpl selector,
                FactoryHolder holder,
                int handlerId
        ) {
            HttpRequestHandler handler = selector.handlersByIdList.getQuiet(handlerId);
            if (handler == null) {
                handler = holder.factory.newInstance();
                try {
                    selector.handlersByIdList.extendAndSet(handlerId, handler);
                } catch (Throwable th) {
                    Misc.freeIfCloseableBestEffort(th, handler);
                    throw th;
                }
            }
            return handler;
        }

        private static void populate(HttpRequestProcessorSelectorImpl selector, FactoryHolder holder) {
            final ObjHashSet<String> urls = holder.factory.getUrls();
            for (int j = 0, n = urls.size(); j < n; j++) {
                final String url = urls.get(j);
                final int handlerId = holder.handlerIds.getQuick(j);
                if (HttpFullFatServerConfiguration.DEFAULT_PROCESSOR_URL.equals(url)) {
                    final HttpRequestHandler handler = getOrCreateHandler(selector, holder, handlerId);
                    selector.defaultRequestProcessor = handler.getDefaultProcessor();
                    selector.defaultProcessorId = handlerId;
                } else {
                    final Utf8String key = new Utf8String(url);
                    int keyIndex = selector.requestHandlerMap.keyIndex(key);
                    if (keyIndex > -1) {
                        final HttpRequestHandler requestHandler = getOrCreateHandler(selector, holder, handlerId);
                        final HttpRequestProcessor defaultProcessor = holder.useAsDefault
                                ? requestHandler.getDefaultProcessor()
                                : null;
                        selector.requestHandlerMap.putAt(keyIndex, key, new IndexedHandler(requestHandler, handlerId));
                        if (holder.useAsDefault) {
                            selector.defaultRequestProcessor = defaultProcessor;
                            selector.defaultProcessorId = handlerId;
                        }
                    }
                }
            }
        }

        private static final class FactoryHolder {
            final HttpRequestHandlerFactory factory;
            final IntList handlerIds = new IntList();
            final boolean useAsDefault;

            FactoryHolder(HttpRequestHandlerFactory factory, boolean useAsDefault) {
                this.factory = factory;
                this.useAsDefault = useAsDefault;
            }
        }
    }

    static class HttpRequestProcessorSelectorImpl implements HttpRequestProcessorSelector {

        private final ObjList<HttpRequestHandler> handlersByIdList = new ObjList<>();
        private final Utf8SequenceObjHashMap<IndexedHandler> requestHandlerMap = new Utf8SequenceObjHashMap<>();
        private int defaultProcessorId = REJECT_PROCESSOR_ID;
        private HttpRequestProcessor defaultRequestProcessor = null;
        private int factoryCount;
        private int lastSelectedHandlerId = REJECT_PROCESSOR_ID;

        @Override
        public void close() {
            final ObjHashSet<Object> dedup = new ObjHashSet<>();
            if (defaultRequestProcessor != null) {
                dedup.add(defaultRequestProcessor);
                defaultRequestProcessor = null;
            }
            for (int i = 0, n = handlersByIdList.size(); i < n; i++) {
                final HttpRequestHandler handler = handlersByIdList.getQuick(i);
                handlersByIdList.setQuick(i, null);
                if (handler != null) {
                    dedup.add(handler);
                }
            }
            handlersByIdList.clear();
            while (requestHandlerMap.size() > 0) {
                final Utf8String key = requestHandlerMap.keys().getQuick(requestHandlerMap.size() - 1);
                requestHandlerMap.removeAt(requestHandlerMap.keyIndex(key));
            }
            defaultProcessorId = REJECT_PROCESSOR_ID;
            factoryCount = 0;
            lastSelectedHandlerId = REJECT_PROCESSOR_ID;
            Throwable failure = null;
            for (int i = 0, n = dedup.size(); i < n; i++) {
                failure = Misc.freeIfCloseableBestEffort(failure, dedup.get(i));
            }
            CairoException.rethrowCleanupFailure(failure);
        }

        @Override
        public int getLastSelectedHandlerId() {
            return lastSelectedHandlerId;
        }

        @Override
        public HttpRequestProcessor resolveProcessorById(int handlerId, HttpRequestHeader header) {
            // handlerId is always produced internally by bind() (sequential non-negative int)
            // and the REJECT_PROCESSOR_ID sentinel (-1) is filtered out by the caller.
            // No bounds check: a bad ID here means a bug that should surface immediately.
            HttpRequestHandler handler = handlersByIdList.getQuick(handlerId);
            return handler != null ? handler.getProcessor(header) : null;
        }

        @Override
        public HttpRequestProcessor select(HttpRequestHeader requestHeader) {
            final Utf8Sequence normalizedUrl = normalizeUrl(requestHeader.getUrl());
            final int keyIndex = requestHandlerMap.keyIndex(normalizedUrl);
            if (keyIndex < 0) {
                IndexedHandler entry = requestHandlerMap.valueAt(keyIndex);
                lastSelectedHandlerId = entry.handlerId();
                return entry.handler().getProcessor(requestHeader);
            }
            lastSelectedHandlerId = defaultProcessorId;
            return defaultRequestProcessor;
        }
    }

    private record IndexedHandler(HttpRequestHandler handler, int handlerId) {
    }
}
