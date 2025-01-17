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

package io.questdb.cutlass.http;

import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.processors.LineHttpPingProcessor;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.SettingsProcessor;
import io.questdb.cutlass.http.processors.StaticContentProcessorFactory;
import io.questdb.cutlass.http.processors.TableStatusCheckProcessor;
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.cutlass.http.processors.TextQueryProcessor;
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
    private final ObjList<Closeable> closeables = new ObjList<>();
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final HttpContextFactory httpContextFactory;
    private final WaitProcessor rescheduleContext;
    private final AssociativeCache<RecordCursorFactory> selectCache;
    private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
    private final int workerCount;

    // used for min http server only
    public HttpServer(
            HttpServerConfiguration configuration,
            WorkerPool pool,
            SocketFactory socketFactory
    ) {
        this(
                configuration,
                pool,
                socketFactory,
                DefaultHttpCookieHandler.INSTANCE,
                DefaultHttpHeaderParserFactory.INSTANCE
        );
    }

    public HttpServer(
            HttpServerConfiguration configuration,
            WorkerPool pool,
            SocketFactory socketFactory,
            HttpCookieHandler cookieHandler,
            HttpHeaderParserFactory headerParserFactory
    ) {
        this.workerCount = pool.getWorkerCount();
        this.selectors = new ObjList<>(workerCount);

        for (int i = 0; i < workerCount; i++) {
            selectors.add(new HttpRequestProcessorSelectorImpl());
        }

        if (configuration instanceof HttpFullFatServerConfiguration) {
            final HttpFullFatServerConfiguration serverConfiguration = (HttpFullFatServerConfiguration) configuration;
            if (serverConfiguration.isQueryCacheEnabled()) {
                this.selectCache = new ConcurrentAssociativeCache<>(serverConfiguration.getConcurrentCacheConfiguration());
            } else {
                this.selectCache = NO_OP_CACHE;
            }
        } else {
            // Min server doesn't need select cache, so we use no-op impl.
            this.selectCache = NO_OP_CACHE;
        }

        this.httpContextFactory = new HttpContextFactory(configuration, socketFactory, cookieHandler, headerParserFactory, selectCache);
        this.dispatcher = IODispatchers.create(configuration, httpContextFactory);
        pool.assign(dispatcher);
        this.rescheduleContext = new WaitProcessor(configuration.getWaitProcessorConfiguration(), dispatcher);
        pool.assign(rescheduleContext);

        for (int i = 0; i < workerCount; i++) {
            final int index = i;

            pool.assign(i, new Job() {

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
            pool.assignThreadLocalCleaner(i, httpContextFactory::freeThreadLocal);
        }
    }

    public static void addDefaultEndpoints(
            HttpServer server,
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            int sharedWorkerCount,
            HttpRequestProcessorBuilder jsonQueryProcessorBuilder,
            HttpRequestProcessorBuilder ilpWriteProcessorBuilderV2
    ) {
        final HttpFullFatServerConfiguration httpServerConfiguration = serverConfiguration.getHttpServerConfiguration();
        final LineHttpProcessorConfiguration lineHttpProcessorConfiguration = httpServerConfiguration.getLineHttpProcessorConfiguration();
        // Disable ILP HTTP if the instance configured to be read-only for HTTP requests
        if (httpServerConfiguration.isEnabled() && lineHttpProcessorConfiguration.isEnabled() && !httpServerConfiguration.getHttpContextConfiguration().readOnlySecurityContext()) {

            server.bind(new HttpRequestProcessorFactory() {
                @Override
                public ObjList<String> getUrls() {
                    return httpServerConfiguration.getContextPathILP();
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return ilpWriteProcessorBuilderV2.newInstance();
                }
            });

            LineHttpPingProcessor pingProcessor = new LineHttpPingProcessor(
                    httpServerConfiguration.getLineHttpProcessorConfiguration().getInfluxPingVersion()
            );
            server.bind(new HttpRequestProcessorFactory() {
                @Override
                public ObjList<String> getUrls() {
                    return httpServerConfiguration.getContextPathILPPing();
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return pingProcessor;
                }
            });
        }

        final SettingsProcessor settingsProcessor = new SettingsProcessor(serverConfiguration);
        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public ObjList<String> getUrls() {
                return httpServerConfiguration.getContextPathSettings();
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return settingsProcessor;
            }
        });

        final WarningsProcessor warningsProcessor = new WarningsProcessor(serverConfiguration.getCairoConfiguration());
        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public ObjList<String> getUrls() {
                return httpServerConfiguration.getContextPathWarnings();
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return warningsProcessor;
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public ObjList<String> getUrls() {
                return httpServerConfiguration.getContextPathExec();
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return jsonQueryProcessorBuilder.newInstance();
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public ObjList<String> getUrls() {
                return httpServerConfiguration.getContextPathImport();
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TextImportProcessor(cairoEngine, httpServerConfiguration.getJsonQueryProcessorConfiguration());
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public ObjList<String> getUrls() {
                return httpServerConfiguration.getContextPathExport();
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TextQueryProcessor(
                        httpServerConfiguration.getJsonQueryProcessorConfiguration(),
                        cairoEngine,
                        workerPool.getWorkerCount(),
                        sharedWorkerCount
                );
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public ObjList<String> getUrls() {
                return httpServerConfiguration.getContextPathTableStatus();
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TableStatusCheckProcessor(cairoEngine, httpServerConfiguration.getJsonQueryProcessorConfiguration());
            }
        });

        server.bind(new StaticContentProcessorFactory(httpServerConfiguration));
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

    public void bind(HttpRequestProcessorFactory factory) {
        bind(factory, false);
    }

    public void bind(HttpRequestProcessorFactory factory, boolean useAsDefault) {
        final ObjList<String> urls = factory.getUrls();
        assert urls != null;
        for (int j = 0, n = urls.size(); j < n; j++) {
            final String url = urls.getQuick(j);
            for (int i = 0; i < workerCount; i++) {
                HttpRequestProcessorSelectorImpl selector = selectors.getQuick(i);
                if (HttpFullFatServerConfiguration.DEFAULT_PROCESSOR_URL.equals(url)) {
                    selector.defaultRequestProcessor = factory.newInstance();
                } else {
                    final Utf8String key = new Utf8String(url);
                    int keyIndex = selector.processorMap.keyIndex(key);
                    if (keyIndex > -1) {
                        final HttpRequestProcessor processor = factory.newInstance();
                        selector.processorMap.putAt(keyIndex, key, processor);
                        if (useAsDefault) {
                            selector.defaultRequestProcessor = processor;
                        }
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

    public int getPort() {
        return dispatcher.getPort();
    }

    public void registerClosable(Closeable closeable) {
        closeables.add(closeable);
    }

    private boolean handleClientOperation(HttpConnectionContext context, int operation, HttpRequestProcessorSelector selector, WaitProcessor rescheduleContext, IODispatcher<HttpConnectionContext> dispatcher) {
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
    public interface HttpRequestProcessorBuilder {
        HttpRequestProcessor newInstance();
    }

    private static class HttpContextFactory extends IOContextFactoryImpl<HttpConnectionContext> {

        public HttpContextFactory(
                HttpServerConfiguration configuration,
                SocketFactory socketFactory,
                HttpCookieHandler cookieHandler,
                HttpHeaderParserFactory headerParserFactory,
                AssociativeCache<RecordCursorFactory> selectCache
        ) {
            super(
                    () -> new HttpConnectionContext(configuration, socketFactory, cookieHandler, headerParserFactory, selectCache),
                    configuration.getHttpContextConfiguration().getConnectionPoolInitialCapacity()
            );
        }
    }

    private static class HttpRequestProcessorSelectorImpl implements HttpRequestProcessorSelector {

        private final Utf8SequenceObjHashMap<HttpRequestProcessor> processorMap = new Utf8SequenceObjHashMap<>();
        private HttpRequestProcessor defaultRequestProcessor = null;

        @Override
        public void close() {
            Misc.freeIfCloseable(defaultRequestProcessor);
            ObjList<Utf8String> processorKeys = processorMap.keys();
            for (int i = 0, n = processorKeys.size(); i < n; i++) {
                Misc.freeIfCloseable(processorMap.get(processorKeys.getQuick(i)));
            }
        }

        @Override
        public HttpRequestProcessor getDefaultProcessor() {
            return defaultRequestProcessor;
        }

        @Override
        public HttpRequestProcessor select(DirectUtf8String url) {
            return processorMap.get(normalizeUrl(url));
        }
    }
}
