/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.network.IOContextFactoryImpl;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class HttpServer implements Closeable {

    private static final Log LOG = LogFactory.getLog(HttpServer.class);
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final HttpContextFactory httpContextFactory;
    private final WaitProcessor rescheduleContext;
    private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
    private final int workerCount;

    public HttpServer(HttpMinServerConfiguration configuration, MessageBus messageBus, Metrics metrics, WorkerPool pool) {
        this.workerCount = pool.getWorkerCount();
        this.selectors = new ObjList<>(workerCount);

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
                public boolean run(int workerId, @NotNull RunStatus runStatus) {
                    long seq = queryCacheEventSubSeq.next();
                    if (seq > -1) {
                        // Queue is not empty, so flush query cache.
                        LOG.info().$("flushing HTTP server query cache [worker=").$(workerId).$(']').$();
                        QueryCache queryCache = QueryCache.getWeakThreadLocalInstance();
                        if (queryCache != null) {
                            queryCache.clear();
                        }
                        queryCacheEventSubSeq.done(seq);
                    }

                    boolean useful = dispatcher.processIOQueue(processor);
                    useful |= rescheduleContext.runReruns(selector);

                    return useful;
                }
            });

            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            pool.assignThreadLocalCleaner(i, () -> {
                httpContextFactory.freeThreadLocal();
                Misc.free(QueryCache.getWeakThreadLocalInstance());
            });

            pool.freeOnExit(() -> {
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
                return new TextImportProcessor(cairoEngine);
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
                        configuration.getJsonQueryProcessorConfiguration(),
                        cairoEngine,
                        workerPool.getWorkerCount(),
                        sharedWorkerCount,
                        functionFactoryCache,
                        snapshotAgent
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
                return new TableStatusCheckProcessor(cairoEngine, configuration.getJsonQueryProcessorConfiguration());
            }
        });

        server.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new StaticContentProcessor(configuration);
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
                selector.processorMap.put(url, processor);
                if (useAsDefault) {
                    selector.defaultRequestProcessor = processor;
                }
            }
        }
    }

    @Override
    public void close() {
        Misc.free(dispatcher);
        Misc.free(rescheduleContext);
        Misc.freeObjListAndClear(selectors);
        Misc.free(httpContextFactory);
    }

    @FunctionalInterface
    public interface HttpRequestProcessorBuilder {
        HttpRequestProcessor newInstance();
    }

    private static class HttpContextFactory extends IOContextFactoryImpl<HttpConnectionContext> {
        public HttpContextFactory(HttpContextConfiguration configuration, Metrics metrics) {
            super(() -> new HttpConnectionContext(configuration, metrics), configuration.getConnectionPoolInitialCapacity());
        }
    }

    private static class HttpRequestProcessorSelectorImpl implements HttpRequestProcessorSelector {

        private final CharSequenceObjHashMap<HttpRequestProcessor> processorMap = new CharSequenceObjHashMap<>();
        private HttpRequestProcessor defaultRequestProcessor = null;

        @Override
        public void close() {
            Misc.freeIfCloseable(defaultRequestProcessor);
            ObjList<CharSequence> processorKeys = processorMap.keys();
            for (int i = 0, n = processorKeys.size(); i < n; i++) {
                Misc.freeIfCloseable(processorMap.get(processorKeys.getQuick(i)));
            }
        }

        @Override
        public HttpRequestProcessor getDefaultProcessor() {
            return defaultRequestProcessor;
        }

        @Override
        public HttpRequestProcessor select(CharSequence url) {
            return processorMap.get(url);
        }
    }
}
