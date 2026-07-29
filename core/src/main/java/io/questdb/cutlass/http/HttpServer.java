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
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
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
        this(configuration, networkSharedPool, socketFactory, new AtomicBoolean(true));
    }

    public HttpServer(
            HttpServerConfiguration configuration,
            WorkerPool networkSharedPool,
            SocketFactory socketFactory,
            AtomicBoolean acceptOpen
    ) {
        this.acceptOpen = acceptOpen;
        this.workerCount = networkSharedPool.getWorkerCount();
        IODispatcher<HttpConnectionContext> dispatcher = null;
        HttpContextFactory httpContextFactory = null;
        WaitProcessor rescheduleContext = null;
        AssociativeCache<RecordCursorFactory> selectCache = null;
        HttpRequestProcessorSelectorFactory selectorFactory = null;
        try {
            FiberRuntime fiberRuntime = null;
            if (configuration instanceof HttpFullFatServerConfiguration serverConfiguration
                    && serverConfiguration.isFiberEnabled()
                    && networkSharedPool.isFiberHost()) {
                fiberRuntime = networkSharedPool.getFiberRuntime();
            }
            selectorFactory = new HttpRequestProcessorSelectorFactory(workerCount);
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
            this.httpContextFactory = httpContextFactory;
            this.rescheduleContext = rescheduleContext;
            this.selectCache = selectCache;
            this.selectorFactory = selectorFactory;

            networkSharedPool.assign(new AcceptGatedJob(dispatcher, acceptOpen));
            networkSharedPool.assign(new AcceptGatedJob(rescheduleContext, acceptOpen));
            for (int i = 0; i < workerCount; i++) {
                HttpRequestProcessorSelectorImpl selector = selectorFactory.getSelectorByWorker(i);
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
            Misc.free(dispatcher, t);
            Misc.free(rescheduleContext, t);
            Misc.free(selectorFactory, t);
            Misc.free(httpContextFactory, t);
            Misc.free(selectCache, t);
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
        Misc.free(dispatcher);
        Misc.free(rescheduleContext);
        Misc.free(selectorFactory);
        Misc.freeObjListAndClear(closeables);
        Misc.free(httpContextFactory);
        Misc.free(selectCache);
    }

    @TestOnly
    public void createSelectorForTesting() {
        Misc.free(selectorFactory.create());
    }

    public ActiveConnectionTracker getActiveConnectionTracker() {
        return activeConnectionTracker;
    }

    public int getPort() {
        return dispatcher.getPort();
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
        private final WaitProcessor rescheduleContext;
        private @Nullable Fiber reservedFiber;
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
                    final Fiber fiber = reservedFiber;
                    if (fiber == null) {
                        throw new IllegalStateException("HTTP I/O event has no reserved fiber");
                    }
                    final HttpConnectionFiberTask task = context.getFiberTask(disp, selectorFactory, rescheduleContext);
                    reservedFiber = null;
                    return handleLaunchResult(context, task.launchReserved(runtime, fiber, operation));
                };
                this.retryLauncher = (fiber, retry, taskIncarnation) -> {
                    boolean isReservationConsumed = false;
                    try {
                        final HttpConnectionContext context = (HttpConnectionContext) retry;
                        final HttpConnectionFiberTask task = context.getFiberTask(dispatcher, selectorFactory, rescheduleContext);
                        isReservationConsumed = true;
                        handleLaunchResult(context, task.launchRerunReserved(runtime, fiber, taskIncarnation));
                    } finally {
                        if (!isReservationConsumed) {
                            runtime.releaseReservedFiber(fiber);
                        }
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
                final Fiber fiber = runtime.tryReserveFiber();
                if (fiber != null) {
                    reservedFiber = fiber;
                    try {
                        useful = dispatcher.processIOQueue(processor);
                    } finally {
                        final Fiber unusedFiber = reservedFiber;
                        reservedFiber = null;
                        if (unusedFiber != null) {
                            runtime.releaseReservedFiber(unusedFiber);
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
            final SuspensionScope.Mode previousMode = SuspensionScope.enter(
                    SuspensionScope.Mode.BLOCKING
            );
            try {
                boolean useful = dispatcher.processIOQueue(processor);
                useful |= rescheduleContext.runReruns(selector);
                return useful;
            } finally {
                SuspensionScope.restore(previousMode);
            }
        }

        private boolean handleLaunchResult(HttpConnectionContext context, LaunchResult result) {
            if (result == LaunchResult.LAUNCHED
                    || result == LaunchResult.ALREADY_OWNED
                    || result == LaunchResult.STALE_INCARNATION
                    || result == LaunchResult.TERMINAL) {
                return true;
            }
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
    static class HttpRequestProcessorSelectorFactory implements Closeable {
        private final ObjList<FactoryHolder> factoryHolders = new ObjList<>();
        private int nextHandlerId = 0;
        private final ObjList<HttpRequestProcessorSelectorImpl> recycledSelectors = new ObjList<>();
        // Per-worker selectors used by gen-0 (the initial Jobs registered to
        // the pool). These selectors are NOT pooled -- they live for the
        // server's lifetime so the per-worker fast path doesn't have to
        // re-acquire across iterations.
        private final ObjList<HttpRequestProcessorSelectorImpl> selectors;

        HttpRequestProcessorSelectorFactory(int workerCount) {
            this.selectors = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                selectors.add(null);
            }
        }

        @Override
        public void close() {
            Misc.freeObjListAndClear(selectors);
            synchronized (recycledSelectors) {
                Misc.freeObjListAndClear(recycledSelectors);
            }
        }

        public HttpRequestProcessorSelectorImpl getSelectorByWorker(int jobIndex) {
            HttpRequestProcessorSelectorImpl s = selectors.getQuick(jobIndex);
            if (s == null) {
                s = create();
                selectors.setQuick(jobIndex, s);
            }
            return s;
        }

        HttpRequestProcessorSelectorImpl acquire() {
            synchronized (recycledSelectors) {
                final int size = recycledSelectors.size();
                if (size > 0) {
                    return recycledSelectors.popLast();
                }
            }
            return create();
        }

        void bind(HttpRequestHandlerFactory factory, boolean useAsDefault) {
            final FactoryHolder holder = new FactoryHolder(factory, useAsDefault);
            final ObjHashSet<String> urls = factory.getUrls();
            assert urls != null;
            for (int j = 0, n = urls.size(); j < n; j++) {
                holder.handlerIds.add(nextHandlerId++);
            }
            factoryHolders.add(holder);
            // Populate any selectors that already exist (eagerly created in
            // the HttpServer ctor); selectors created later via create() will
            // pick up this holder by walking factoryHolders.
            for (int i = 0, n = selectors.size(); i < n; i++) {
                HttpRequestProcessorSelectorImpl s = selectors.getQuick(i);
                if (s != null) {
                    populate(s, holder);
                }
            }
            // Selectors currently sitting in the recycle pool would also be
            // out-of-date, but pooled selectors are only valid for already-
            // bound URLs. bind() runs at server setup before any client
            // traffic reaches the recycle path, so the pool is empty here.
            synchronized (recycledSelectors) {
                assert recycledSelectors.size() == 0 : "bind() called after selector reuse began";
            }
        }

        HttpRequestProcessorSelectorImpl create() {
            final HttpRequestProcessorSelectorImpl selector = new HttpRequestProcessorSelectorImpl();
            try {
                for (int i = 0, n = factoryHolders.size(); i < n; i++) {
                    populate(selector, factoryHolders.getQuick(i));
                }
                return selector;
            } catch (Throwable th) {
                Misc.free(selector, th);
                throw th;
            }
        }

        void release(HttpRequestProcessorSelectorImpl selector) {
            synchronized (recycledSelectors) {
                recycledSelectors.add(selector);
            }
        }

        private static void populate(HttpRequestProcessorSelectorImpl selector, FactoryHolder holder) {
            final ObjHashSet<String> urls = holder.factory.getUrls();
            for (int j = 0, n = urls.size(); j < n; j++) {
                final String url = urls.get(j);
                final int handlerId = holder.handlerIds.getQuick(j);
                if (HttpFullFatServerConfiguration.DEFAULT_PROCESSOR_URL.equals(url)) {
                    final HttpRequestHandler handler = holder.factory.newInstance();
                    try {
                        selector.handlersByIdList.extendAndSet(handlerId, handler);
                    } catch (Throwable th) {
                        Misc.freeIfCloseableBestEffort(th, handler);
                        throw th;
                    }
                    selector.defaultRequestProcessor = handler.getDefaultProcessor();
                    selector.defaultProcessorId = handlerId;
                } else {
                    final Utf8String key = new Utf8String(url);
                    int keyIndex = selector.requestHandlerMap.keyIndex(key);
                    if (keyIndex > -1) {
                        final HttpRequestHandler requestHandler = holder.factory.newInstance();
                        try {
                            selector.handlersByIdList.extendAndSet(handlerId, requestHandler);
                        } catch (Throwable th) {
                            Misc.freeIfCloseableBestEffort(th, requestHandler);
                            throw th;
                        }
                        selector.requestHandlerMap.putAt(keyIndex, key, new IndexedHandler(requestHandler, handlerId));
                        if (holder.useAsDefault) {
                            selector.defaultRequestProcessor = requestHandler.getDefaultProcessor();
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
        private int lastSelectedHandlerId = REJECT_PROCESSOR_ID;

        @Override
        public void close() {
            ObjHashSet<Object> dedup = new ObjHashSet<>();
            if (defaultRequestProcessor != null) {
                dedup.add(defaultRequestProcessor);
                Misc.freeIfCloseable(defaultRequestProcessor);
            }

            for (int i = 0, n = handlersByIdList.size(); i < n; i++) {
                HttpRequestHandler handler = handlersByIdList.getQuick(i);
                if (handler != null && dedup.add(handler)) {
                    Misc.freeIfCloseable(handler);
                }
            }

            // invariant: every handler in requestHandlerMap is also included in handlersByIdList
            // thus we can close just handlers in handlersByIdList, no need to iterate over requestHandlerMap
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
