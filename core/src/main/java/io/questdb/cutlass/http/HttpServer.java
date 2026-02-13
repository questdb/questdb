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

package io.questdb.cutlass.http;

import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.processors.ExportQueryProcessor;
import io.questdb.cutlass.ilpv4.server.IlpV4WebSocketHttpProcessor;
import io.questdb.cutlass.http.processors.LineHttpPingProcessor;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.SettingsProcessor;
import io.questdb.cutlass.http.processors.StaticContentProcessorFactory;
import io.questdb.cutlass.http.processors.TableStatusCheckProcessor;
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.cutlass.http.processors.WarningsProcessor;
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
    private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
    private final int workerCount;
    private int nextHandlerId = 0;

    public HttpServer(
            HttpServerConfiguration configuration,
            WorkerPool networkSharedPool,
            SocketFactory socketFactory
    ) {
        this.workerCount = networkSharedPool.getWorkerCount();
        this.selectors = new ObjList<>(workerCount);

        for (int i = 0; i < workerCount; i++) {
            selectors.add(new HttpRequestProcessorSelectorImpl());
        }

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
            final int index = i;

            networkSharedPool.assign(i, new Job() {

                private final HttpRequestProcessorSelector selector = selectors.getQuick(index);
                private final IORequestProcessor<HttpConnectionContext> processor =
                        (operation, context, dispatcher)
                                -> handleClientOperation(context, operation, selector, rescheduleContext, dispatcher);

                @Override
                public boolean run(int workerId, @NotNull RunStatus runStatus) {
                    boolean useful = dispatcher.processIOQueue(processor);
                    useful |= rescheduleContext.runReruns(selector);
                    return useful;
                }
            });

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

            // ILP v4 endpoint (WebSocket only)
            server.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpServerConfiguration.getContextPathILPV4();
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new IlpV4WebSocketHttpProcessor(cairoEngine, httpServerConfiguration);
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
                Unsafe.getUnsafe().putByte(p + i - shift, b);
            }
        }
        url.squeezeHi(shift);
        return url;
    }

    public void bind(HttpRequestHandlerFactory factory) {
        bind(factory, false);
    }

    public void bind(HttpRequestHandlerFactory factory, boolean useAsDefault) {
        final ObjHashSet<String> urls = factory.getUrls();
        assert urls != null;
        for (int j = 0, n = urls.size(); j < n; j++) {
            final String url = urls.get(j);
            int handlerId = -1; // assigned on first worker that actually registers
            for (int i = 0; i < workerCount; i++) {
                HttpRequestProcessorSelectorImpl selector = selectors.getQuick(i);
                if (HttpFullFatServerConfiguration.DEFAULT_PROCESSOR_URL.equals(url)) {
                    if (handlerId < 0) {
                        handlerId = nextHandlerId++;
                    }
                    final HttpRequestHandler handler = factory.newInstance();
                    selector.defaultRequestProcessor = handler.getDefaultProcessor();
                    selector.defaultProcessorId = handlerId;
                    selector.handlersByIdList.extendAndSet(handlerId, handler);
                } else {
                    final Utf8String key = new Utf8String(url);
                    int keyIndex = selector.requestHandlerMap.keyIndex(key);
                    if (keyIndex > -1) {
                        if (handlerId < 0) {
                            handlerId = nextHandlerId++;
                        }
                        final HttpRequestHandler requestHandler = factory.newInstance();
                        selector.requestHandlerMap.putAt(keyIndex, key, new IndexedHandler(requestHandler, handlerId));
                        if (useAsDefault) {
                            selector.defaultRequestProcessor = requestHandler.getDefaultProcessor();
                            selector.defaultProcessorId = handlerId;
                        }
                        selector.handlersByIdList.extendAndSet(handlerId, requestHandler);
                    }
                }
            }
        }
    }

    public void clearSelectCache() {
        selectCache.clear();
    }

    @Override
    public void close() {
        Misc.free(dispatcher);
        Misc.free(rescheduleContext);
        Misc.freeObjListAndClear(selectors);
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

    private record IndexedHandler(HttpRequestHandler handler, int handlerId) {
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
}
