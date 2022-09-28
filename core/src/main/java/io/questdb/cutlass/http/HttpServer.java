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

package io.questdb.cutlass.http;

import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.processors.*;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
import io.questdb.mp.Job;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.network.MutableIOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class HttpServer implements Closeable {

    private static final Log LOG = LogFactory.getLog(HttpServer.class);

    private static final WorkerPoolAwareConfiguration.ServerFactory<HttpServer, HttpServerConfiguration> CREATE0 = HttpServer::create0;
    private static final WorkerPoolAwareConfiguration.ServerFactory<HttpServer, HttpMinServerConfiguration> CREATE_MIN = HttpServer::createMin;
    private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final int workerCount;
    private final HttpContextFactory httpContextFactory;
    private final WorkerPool workerPool;
    private final WaitProcessor rescheduleContext;

    public HttpServer(HttpMinServerConfiguration configuration, MessageBus messageBus, Metrics metrics, WorkerPool pool, boolean localPool) {
        this.workerCount = pool.getWorkerCount();
        this.selectors = new ObjList<>(workerCount);

        if (localPool) {
            workerPool = pool;
        } else {
            workerPool = null;
        }
        for (int i = 0; i < workerCount; i++) {
            selectors.add(new HttpRequestProcessorSelectorImpl());
        }

        this.httpContextFactory = new HttpContextFactory(configuration.getHttpContextConfiguration(), metrics);
        this.dispatcher = IODispatchers.create(
                configuration.getDispatcherConfiguration(),
                httpContextFactory
        );
        pool.assign(dispatcher);
        this.rescheduleContext = new WaitProcessor(configuration.getWaitProcessorConfiguration());
        pool.assign(this.rescheduleContext);

        for (int i = 0; i < workerCount; i++) {
            final int index = i;

            final SCSequence queryCacheEventSubSeq = new SCSequence();
            final FanOut queryCacheEventFanOut = messageBus.getQueryCacheEventFanOut();
            queryCacheEventFanOut.and(queryCacheEventSubSeq);

            pool.assign(i, new Job() {
                private final HttpRequestProcessorSelector selector = selectors.getQuick(index);
                private final IORequestProcessor<HttpConnectionContext> processor =
                        (operation, context) -> context.handleClientOperation(operation, selector, rescheduleContext);

                @Override
                public boolean run(int workerId) {
                    long seq = queryCacheEventSubSeq.next();
                    if (seq > -1) {
                        // Queue is not empty, so flush query cache.
                        LOG.info().$("flushing HTTP server query cache [worker=").$(workerId).$(']').$();
                        QueryCache.getInstance().clear();
                        queryCacheEventSubSeq.done(seq);
                    }

                    boolean useful = dispatcher.processIOQueue(processor);
                    useful |= rescheduleContext.runReruns(selector);

                    return useful;
                }
            });

            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            pool.assign(i, () -> {
                Misc.free(selectors.getQuick(index));
                httpContextFactory.close();
                Misc.free(QueryCache.getInstance());
                messageBus.getQueryCacheEventFanOut().remove(queryCacheEventSubSeq);
                queryCacheEventSubSeq.clear();
            });
        }
    }

    public static void addDefaultEndpoints(
            HttpServer server,
            HttpServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            int sharedWorkerCount,
            HttpRequestProcessorBuilder jsonQueryProcessorBuilder,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent
    ) {
        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return jsonQueryProcessorBuilder.newInstance();
            }

            @Override
            public String getUrl() {
                return "/exec";
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new TextImportProcessor(cairoEngine);
            }

            @Override
            public String getUrl() {
                return "/imp";
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new TextQueryProcessor(
                        configuration.getJsonQueryProcessorConfiguration(),
                        cairoEngine,
                        workerPool.getWorkerCount(),
                        sharedWorkerCount,
                        functionFactoryCache,
                        snapshotAgent
                );
            }

            @Override
            public String getUrl() {
                return "/exp";
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new TableStatusCheckProcessor(cairoEngine, configuration.getJsonQueryProcessorConfiguration());
            }

            @Override
            public String getUrl() {
                return "/chk";
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new StaticContentProcessor(configuration);
            }

            @Override
            public String getUrl() {
                return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
            }
        });
    }

    @Nullable
    public static HttpServer create(
            HttpServerConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine,
            Metrics metrics
    ) {
        return create(
                configuration,
                sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                null,
                null,
                metrics
        );
    }

    @Nullable
    public static HttpServer create(
            HttpServerConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine,
            @Nullable FunctionFactoryCache functionFactoryCache,
            @Nullable DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        return WorkerPoolAwareConfiguration.create(
                configuration,
                sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                CREATE0,
                functionFactoryCache,
                snapshotAgent,
                metrics
        );
    }

    @Nullable
    public static HttpServer createMin(
            HttpMinServerConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine,
            @Nullable FunctionFactoryCache functionFactoryCache,
            @Nullable DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        return WorkerPoolAwareConfiguration.create(
                configuration,
                sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                CREATE_MIN,
                functionFactoryCache,
                snapshotAgent,
                metrics
        );
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
                selector.processorMap.put(url, processor);
                if (useAsDefault) {
                    selector.defaultRequestProcessor = processor;
                }
            }
        }
    }

    @Override
    public void close() {
        if (workerPool != null) {
            workerPool.halt();
        }
        Misc.free(httpContextFactory);
        Misc.free(dispatcher);
        Misc.free(rescheduleContext);
    }

    private static HttpServer create0(
            HttpServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            boolean localPool,
            int sharedWorkerCount,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        final HttpServer s = new HttpServer(configuration, cairoEngine.getMessageBus(), metrics, workerPool, localPool);
        QueryCache.configure(configuration, metrics);
        HttpRequestProcessorBuilder jsonQueryProcessorBuilder = () -> new JsonQueryProcessor(
                configuration.getJsonQueryProcessorConfiguration(),
                cairoEngine,
                workerPool.getWorkerCount(),
                sharedWorkerCount,
                functionFactoryCache,
                snapshotAgent);
        addDefaultEndpoints(s, configuration, cairoEngine, workerPool, sharedWorkerCount, jsonQueryProcessorBuilder, functionFactoryCache, snapshotAgent);
        return s;
    }

    private static HttpServer createMin(
            HttpMinServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            boolean localPool,
            int sharedWorkerCount,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        final HttpServer s = new HttpServer(configuration, cairoEngine.getMessageBus(), metrics, workerPool, localPool);
        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new HealthCheckProcessor();
            }

            @Override
            public String getUrl() {
                return metrics.isEnabled() ? "/status" : "*";
            }
        }, true);
        if (metrics.isEnabled()) {
            s.bind(new HttpRequestProcessorFactory() {
                @Override
                public HttpRequestProcessor newInstance() {
                    return new PrometheusMetricsProcessor(metrics);
                }

                @Override
                public String getUrl() {
                    return "/metrics";
                }
            });
        }
        return s;
    }

    public interface HttpRequestProcessorBuilder {
        HttpRequestProcessor newInstance();
    }

    private static class HttpRequestProcessorSelectorImpl implements HttpRequestProcessorSelector {

        private final CharSequenceObjHashMap<HttpRequestProcessor> processorMap = new CharSequenceObjHashMap<>();
        private HttpRequestProcessor defaultRequestProcessor = null;

        @Override
        public HttpRequestProcessor select(CharSequence url) {
            return processorMap.get(url);
        }

        @Override
        public HttpRequestProcessor getDefaultProcessor() {
            return defaultRequestProcessor;
        }

        @Override
        public void close() {
            Misc.freeIfCloseable(defaultRequestProcessor);
            ObjList<CharSequence> processorKeys = processorMap.keys();
            for (int i = 0, n = processorKeys.size(); i < n; i++) {
                Misc.freeIfCloseable(processorMap.get(processorKeys.getQuick(i)));
            }
        }
    }

    private static class HttpContextFactory extends MutableIOContextFactory<HttpConnectionContext> {
        public HttpContextFactory(HttpContextConfiguration configuration, Metrics metrics) {
            super(() -> new HttpConnectionContext(configuration, metrics),  configuration.getConnectionPoolInitialCapacity());
        }
    }
}
