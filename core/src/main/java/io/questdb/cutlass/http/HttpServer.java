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

import java.io.Closeable;

public class HttpServer implements Closeable {
    static final NoOpAssociativeCache<RecordCursorFactory> NO_OP_CACHE = new NoOpAssociativeCache<>();
    private final ActiveConnectionTracker activeConnectionTracker;
    private final ObjList<Closeable> closeables = new ObjList<>();
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final HttpContextFactory httpContextFactory;
    private final WaitProcessor rescheduleContext;
    private final AssociativeCache<RecordCursorFactory> selectCache;
    // Per-worker selector storage with a master factory registration list.
    // Each worker's Job binds to its own selector (avoiding the per-handler
    // state aliasing that a shared selector would cause), and on continuation
    // rotation HttpRequestJob.cloneInstance() mints a fresh selector via
    // selectorFactory.create() so the captured cont's per-handler scratch is
    // not aliased by the worker's next iteration.
    private final HttpRequestProcessorSelectorFactory selectorFactory;
    private final int workerCount;

    public HttpServer(
            HttpServerConfiguration configuration,
            WorkerPool networkSharedPool,
            SocketFactory socketFactory
    ) {
        this.workerCount = networkSharedPool.getWorkerCount();
        this.selectorFactory = new HttpRequestProcessorSelectorFactory(workerCount);

        if (configuration instanceof HttpFullFatServerConfiguration serverConfiguration) {
            if (serverConfiguration.isQueryCacheEnabled()) {
                this.selectCache = new ConcurrentAssociativeCache<>(serverConfiguration.getConcurrentCacheConfiguration());
            } else {
                this.selectCache = NO_OP_CACHE;
            }
        } else {
            // Min server doesn't need select cache, so we use no-op impl.
            this.selectCache = NO_OP_CACHE;
        }

        this.activeConnectionTracker = new ActiveConnectionTracker(configuration.getHttpContextConfiguration());
        this.httpContextFactory = new HttpContextFactory(configuration, socketFactory, selectCache, activeConnectionTracker);
        this.dispatcher = IODispatchers.create(configuration, httpContextFactory);
        networkSharedPool.assign(dispatcher);
        this.rescheduleContext = new WaitProcessor(configuration.getWaitProcessorConfiguration(), dispatcher);
        networkSharedPool.assign(rescheduleContext);

        for (int i = 0; i < workerCount; i++) {
            // Eagerly materialise the per-worker selector so that the Job has
            // a usable selector reference before bind() runs. The selector is
            // empty at this point; bind() will populate every already-created
            // selector when called from HttpServer.addDefaultEndpoints later.
            // recyclableSelector=false: this gen-0 selector lives in the
            // factory's per-worker slots and must not enter the recycle pool.
            HttpRequestProcessorSelectorImpl selector = selectorFactory.getSelectorByWorker(i);
            networkSharedPool.assign(i, new HttpRequestJob(
                    this,
                    dispatcher,
                    rescheduleContext,
                    selectorFactory,
                    selector,
                    false
            ));

            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            networkSharedPool.assignThreadLocalCleaner(i, httpContextFactory::freeThreadLocal);
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
        Misc.free(dispatcher);
        Misc.free(rescheduleContext);
        Misc.free(selectorFactory);
        Misc.freeObjListAndClear(closeables);
        Misc.free(httpContextFactory);
        Misc.free(selectCache);
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
                    () -> new HttpConnectionContext(configuration, socketFactory, selectCache, activeConnectionTracker),
                    configuration.getHttpContextConfiguration().getConnectionPoolInitialCapacity()
            );
        }
    }

    /**
     * Per-worker dispatcher Job. On continuation rotation, cloneInstance()
     * mints a fresh job with a fresh selector (built via the master factory
     * list). The captured cont keeps its own selector and per-handler
     * scratch; the new generation does not alias it.
     */
    private static final class HttpRequestJob implements Job {
        private final IODispatcher<HttpConnectionContext> dispatcher;
        private final HttpServer owner;
        private final IORequestProcessor<HttpConnectionContext> processor;
        private final boolean recyclableSelector;
        private final WaitProcessor rescheduleContext;
        private final HttpRequestProcessorSelectorFactory selectorFactory;
        // Mutable so recycleInstance() can release the selector back to the
        // factory's recycle pool, and a subsequent run() can lazily
        // re-acquire one. The processor lambda below reads this.selector at
        // each invocation, so the re-acquire transparently updates dispatch.
        private HttpRequestProcessorSelectorImpl selector;

        HttpRequestJob(
                HttpServer owner,
                IODispatcher<HttpConnectionContext> dispatcher,
                WaitProcessor rescheduleContext,
                HttpRequestProcessorSelectorFactory selectorFactory,
                HttpRequestProcessorSelectorImpl selector,
                boolean recyclableSelector
        ) {
            this.owner = owner;
            this.dispatcher = dispatcher;
            this.rescheduleContext = rescheduleContext;
            this.selectorFactory = selectorFactory;
            this.selector = selector;
            this.recyclableSelector = recyclableSelector;
            // Lambda reads this.selector at invocation time (field access via
            // captured `this`), so a recycle/re-acquire cycle flows through
            // transparently.
            this.processor = (operation, context, disp) ->
                    owner.handleClientOperation(context, operation, this.selector, rescheduleContext, disp);
        }

        @Override
        public Job cloneInstance() {
            // Fresh selector + fresh handler instances for the new
            // generation. The parked cont's frame keeps the previous
            // selector reference, so no aliasing.
            return new HttpRequestJob(
                    owner,
                    dispatcher,
                    rescheduleContext,
                    selectorFactory,
                    selectorFactory.acquire(),
                    true
            );
        }

        @Override
        public void recycleInstance() {
            // gen-0 (per-worker) Jobs have recyclableSelector=false; their
            // selectors live in HttpRequestProcessorSelectorFactory.selectors
            // and must not be pooled.
            if (recyclableSelector && selector != null) {
                selectorFactory.release(selector);
                selector = null;
            }
        }

        @Override
        public boolean run(int workerId, @NotNull RunStatus runStatus) {
            if (selector == null) {
                // Snapshot reused from Worker.snapshotPool after a prior
                // recycle: re-acquire a selector for this generation.
                selector = selectorFactory.acquire();
            }
            boolean useful = dispatcher.processIOQueue(processor);
            useful |= rescheduleContext.runReruns(selector);
            return useful;
        }
    }

    /**
     * Maintains a master list of {@link HttpRequestHandlerFactory}
     * registrations and creates per-worker (or per-clone) selectors on
     * demand. Each {@link #create()} call walks the master list and calls
     * {@code factory.newInstance()} per registered URL, so every selector
     * gets its own handler instances.
     * <p>
     * Handler ids are pre-assigned in {@link #bind(HttpRequestHandlerFactory,
     * boolean)} so that the same URL maps to the same handler id across
     * every selector this factory ever creates.
     */
    private static class HttpRequestProcessorSelectorFactory implements Closeable {
        private final ObjList<FactoryHolder> factoryHolders = new ObjList<>();
        // Pool of selectors released by HttpRequestJob.recycleInstance() when a
        // continuation snapshot completes. cloneInstance() pops from here
        // before falling back to create(), so steady-state rotation amortises
        // to "pop a selector from the queue" instead of allocating a fresh
        // selector + handler set per suspend.
        private final ConcurrentPool<HttpRequestProcessorSelectorImpl> recyclePool = new ConcurrentPool<>();
        // Per-worker selectors used by gen-0 (the initial Jobs registered to
        // the pool). These selectors are NOT pooled -- they live for the
        // server's lifetime so the per-worker fast path doesn't have to
        // re-acquire across iterations.
        private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
        private int nextHandlerId = 0;

        HttpRequestProcessorSelectorFactory(int workerCount) {
            this.selectors = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                selectors.add(null);
            }
        }

        /**
         * Acquire a selector for a cont snapshot. Pops from the recycle pool
         * if non-empty, otherwise mints a fresh one populated from the master
         * factory list. The caller MUST eventually return the selector via
         * {@link #release(HttpRequestProcessorSelectorImpl)} to keep the pool
         * from being drained one-way.
         */
        HttpRequestProcessorSelectorImpl acquire() {
            HttpRequestProcessorSelectorImpl s = recyclePool.pop();
            if (s != null) {
                return s;
            }
            return create();
        }

        @Override
        public void close() {
            Misc.freeObjListAndClear(selectors);
            HttpRequestProcessorSelectorImpl s;
            while ((s = recyclePool.pop()) != null) {
                Misc.free(s);
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

        /**
         * Return a selector to the recycle pool. Called by
         * {@link HttpRequestJob#recycleInstance()} after the cont containing
         * the selector completes. Per-worker selectors stored in
         * {@link #selectors} are NEVER passed here -- their Jobs have
         * {@code recyclableSelector=false}.
         */
        void release(HttpRequestProcessorSelectorImpl selector) {
            recyclePool.push(selector);
        }

        private static void populate(HttpRequestProcessorSelectorImpl selector, FactoryHolder holder) {
            final ObjHashSet<String> urls = holder.factory.getUrls();
            for (int j = 0, n = urls.size(); j < n; j++) {
                final String url = urls.get(j);
                final int handlerId = holder.handlerIds.getQuick(j);
                if (HttpFullFatServerConfiguration.DEFAULT_PROCESSOR_URL.equals(url)) {
                    final HttpRequestHandler handler = holder.factory.newInstance();
                    selector.defaultRequestProcessor = handler.getDefaultProcessor();
                    selector.defaultProcessorId = handlerId;
                    selector.handlersByIdList.extendAndSet(handlerId, handler);
                } else {
                    final Utf8String key = new Utf8String(url);
                    int keyIndex = selector.requestHandlerMap.keyIndex(key);
                    if (keyIndex > -1) {
                        final HttpRequestHandler requestHandler = holder.factory.newInstance();
                        selector.requestHandlerMap.putAt(keyIndex, key, new IndexedHandler(requestHandler, handlerId));
                        if (holder.useAsDefault) {
                            selector.defaultRequestProcessor = requestHandler.getDefaultProcessor();
                            selector.defaultProcessorId = handlerId;
                        }
                        selector.handlersByIdList.extendAndSet(handlerId, requestHandler);
                    }
                }
            }
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
            assert recyclePool.pop() == null : "bind() called after rotation populated the recycle pool";
        }

        HttpRequestProcessorSelectorImpl create() {
            HttpRequestProcessorSelectorImpl sel = new HttpRequestProcessorSelectorImpl();
            for (int i = 0, n = factoryHolders.size(); i < n; i++) {
                populate(sel, factoryHolders.getQuick(i));
            }
            return sel;
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

    private static class HttpRequestProcessorSelectorImpl implements HttpRequestProcessorSelector {

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
