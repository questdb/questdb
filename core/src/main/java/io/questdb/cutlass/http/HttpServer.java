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

import io.questdb.Metrics;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.processors.*;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.network.*;
import io.questdb.std.*;
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
            HttpMinServerConfiguration configuration,
            Metrics metrics,
            WorkerPool pool,
            SocketFactory socketFactory
    ) {
        this(
                configuration,
                metrics,
                pool,
                socketFactory,
                DefaultHttpCookieHandler.INSTANCE,
                DefaultHttpHeaderParserFactory.INSTANCE
        );
    }

    public HttpServer(
            HttpMinServerConfiguration configuration,
            Metrics metrics,
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

        if (configuration instanceof HttpServerConfiguration) {
            final HttpServerConfiguration serverConfiguration = (HttpServerConfiguration) configuration;
            if (serverConfiguration.isQueryCacheEnabled()) {
                this.selectCache = new ConcurrentAssociativeCache<>(
                        serverConfiguration.getQueryCacheBlockCount(),
                        serverConfiguration.getQueryCacheRowCount(),
                        metrics.jsonQuery().cachedQueriesGauge(),
                        metrics.jsonQuery().cacheHitCounter(),
                        metrics.jsonQuery().cacheMissCounter()
                );
            } else {
                this.selectCache = NO_OP_CACHE;
            }
        } else {
            // Min server doesn't need select cache, so we use no-op impl.
            this.selectCache = NO_OP_CACHE;
        }

        this.httpContextFactory = new HttpContextFactory(configuration, metrics, socketFactory, cookieHandler, headerParserFactory, selectCache);
        this.dispatcher = IODispatchers.create(configuration.getDispatcherConfiguration(), httpContextFactory);
        pool.assign(dispatcher);
        this.rescheduleContext = new WaitProcessor(configuration.getWaitProcessorConfiguration(), dispatcher);
        pool.assign(rescheduleContext);

        for (int i = 0; i < workerCount; i++) {
            final int index = i;

            pool.assign(i, new Job() {

                private final HttpRequestProcessorSelector selector = selectors.getQuick(index);
                private final IORequestProcessor<HttpConnectionContext> processor =
                        (operation, context, dispatcher) -> handleClientOperation(context, operation, selector, rescheduleContext, dispatcher);

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
        final HttpServerConfiguration httpServerConfiguration = serverConfiguration.getHttpServerConfiguration();
        final LineHttpProcessorConfiguration lineHttpProcessorConfiguration = httpServerConfiguration.getLineHttpProcessorConfiguration();
        // Disable ILP HTTP if the instance configured to be read-only for HTTP requests
        if (httpServerConfiguration.isEnabled() && lineHttpProcessorConfiguration.isEnabled() && !httpServerConfiguration.getHttpContextConfiguration().readOnlySecurityContext()) {
            server.bind(new HttpRequestProcessorFactory() {
                @Override
                public String getUrl() {
                    return "/write";
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return ilpWriteProcessorBuilderV2.newInstance();
                }
            });

            server.bind(new HttpRequestProcessorFactory() {
                @Override
                public String getUrl() {
                    return "/api/v2/write";
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
                public String getUrl() {
                    return "/ping";
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
            public String getUrl() {
                return "/settings";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return settingsProcessor;
            }
        });

        final WarningsProcessor warningsProcessor = new WarningsProcessor(serverConfiguration.getCairoConfiguration());
        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/warnings";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return warningsProcessor;
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/exec";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return jsonQueryProcessorBuilder.newInstance();
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/imp";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TextImportProcessor(cairoEngine, httpServerConfiguration.getJsonQueryProcessorConfiguration());
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/exp";
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
            public String getUrl() {
                return "/chk";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TableStatusCheckProcessor(cairoEngine, httpServerConfiguration.getJsonQueryProcessorConfiguration());
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new StaticContentProcessor(httpServerConfiguration);
            }
        });
    }

    public void bind(HttpRequestProcessorFactory factory) {
        bind(factory, false);
    }

    public void bind(HttpRequestProcessorFactory factory, boolean useAsDefault) {
        final String url = factory.getUrl();
        assert url != null;
        for (int i = 0; i < workerCount; i++) {
            HttpRequestProcessorSelectorImpl selector = selectors.getQuick(i);
            if (HttpServerConfiguration.DEFAULT_PROCESSOR_URL.equals(url)) {
                selector.defaultRequestProcessor = factory.newInstance();
            } else {
                final HttpRequestProcessor processor = factory.newInstance();
                selector.processorMap.put(new Utf8String(url), processor);
                if (useAsDefault) {
                    selector.defaultRequestProcessor = processor;
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
                HttpMinServerConfiguration configuration,
                Metrics metrics, SocketFactory socketFactory,
                HttpCookieHandler cookieHandler,
                HttpHeaderParserFactory headerParserFactory,
                AssociativeCache<RecordCursorFactory> selectCache
        ) {
            super(
                    () -> new HttpConnectionContext(configuration, metrics, socketFactory, cookieHandler, headerParserFactory, selectCache),
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
        public HttpRequestProcessor select(Utf8Sequence url) {
            return processorMap.get(url);
        }
    }
}
