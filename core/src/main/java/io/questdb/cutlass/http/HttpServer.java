/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import java.io.Closeable;

import org.jetbrains.annotations.Nullable;

import io.questdb.MessageBus;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.WorkerPoolAwareConfiguration.ServerFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnIndexerJob;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.QueryCache;
import io.questdb.cutlass.http.processors.StaticContentProcessor;
import io.questdb.cutlass.http.processors.TableStatusCheckProcessor;
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.cutlass.http.processors.TextQueryProcessor;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.engine.groupby.vect.GroupByJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.EagerThreadSetup;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ThreadLocal;
import io.questdb.std.WeakObjectPool;

public class HttpServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(HttpServer.class);
    private static final WorkerPoolAwareConfiguration.ServerFactory<HttpServer, HttpServerConfiguration> CREATE0 = HttpServer::create0;
    private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final int workerCount;
    private final HttpContextFactory httpContextFactory;
    private final WorkerPool workerPool;

    public HttpServer(HttpServerConfiguration configuration, WorkerPool pool, boolean localPool) {
        this.workerCount = pool.getWorkerCount();
        this.selectors = new ObjList<>(workerCount);
        QueryCache.configure(configuration);

        if (localPool) {
            workerPool = pool;
        } else {
            workerPool = null;
        }
        for (int i = 0; i < workerCount; i++) {
            selectors.add(new HttpRequestProcessorSelectorImpl());
        }

        this.httpContextFactory = new HttpContextFactory(configuration);
        this.dispatcher = IODispatchers.create(
                configuration.getDispatcherConfiguration(),
                httpContextFactory
        );

        pool.assign(dispatcher);

        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            final int index = i;
            pool.assign(i, new Job() {
                private final HttpRequestProcessorSelector selector = selectors.getQuick(index);
                private final IORequestProcessor<HttpConnectionContext> processor =
                        (operation, context) -> context.handleClientOperation(operation, selector);

                @Override
                public boolean run(int workerId) {
                    return dispatcher.processIOQueue(processor);
                }
            });

            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            pool.assign(i, () -> {
                Misc.free(selectors.getQuick(index));
                httpContextFactory.closeContextPool();
                Misc.free(QueryCache.getInstance());
            });
        }
    }

    @Nullable
    public static HttpServer create(
            HttpServerConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine,
            @Nullable FunctionFactoryCache functionFactoryCache
    ) {
        return WorkerPoolAwareConfiguration.create(
                configuration,
                sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                CREATE0,
                functionFactoryCache
        );
    }

    @Nullable
    public static HttpServer create(
            HttpServerConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine
    ) {
        return WorkerPoolAwareConfiguration.create(
                configuration,
                sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                CREATE0,
                null
        );
    }

    @Nullable
    public static HttpServer create(
            HttpServerConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine,
            ServerFactory<HttpServer, HttpServerConfiguration> factory
    ) {
        return WorkerPoolAwareConfiguration.create(
                configuration,
                sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                factory,
                null
        );
    }
    
    public interface HttpRequestProcessorBuilder {
        HttpRequestProcessor newInstance();
    }

    private static HttpServer create0(
            HttpServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            boolean localPool,
            MessageBus messageBus,
            FunctionFactoryCache functionFactoryCache
    ) {
        final HttpServer s = new HttpServer(configuration, workerPool, localPool);
        QueryCache.configure(configuration);
        HttpRequestProcessorBuilder jsonQueryProcessorBuilder = () -> {
            return new JsonQueryProcessor(
                    configuration.getJsonQueryProcessorConfiguration(),
                    cairoEngine,
                    messageBus,
                    workerPool.getWorkerCount(),
                    functionFactoryCache);
        };
        addDefaultEndpoints(s, configuration, cairoEngine, workerPool, localPool, messageBus, jsonQueryProcessorBuilder, functionFactoryCache);
        return s;
    }

    public static void addDefaultEndpoints(
            HttpServer server,
            HttpServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            boolean localPool,
            MessageBus messageBus,
            HttpRequestProcessorBuilder jsonQueryProcessorBuilder,
            FunctionFactoryCache functionFactoryCache
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
                        messageBus,
                        workerPool.getWorkerCount(),
                        functionFactoryCache
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

        // jobs that help parallel execution of queries
        workerPool.assign(new ColumnIndexerJob(messageBus));
        workerPool.assign(new GroupByJob(messageBus));
     }

    public void bind(HttpRequestProcessorFactory factory) {
        final String url = factory.getUrl();
        assert url != null;
        for (int i = 0; i < workerCount; i++) {
            HttpRequestProcessorSelectorImpl selector = selectors.getQuick(i);
            if (HttpServerConfiguration.DEFAULT_PROCESSOR_URL.equals(url)) {
                selector.defaultRequestProcessor = factory.newInstance();
            } else {
                selector.processorMap.put(url, factory.newInstance());
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
            Misc.free(defaultRequestProcessor);
            ObjList<CharSequence> processorKeys = processorMap.keys();
            for (int i = 0, n = processorKeys.size(); i < n; i++) {
                Misc.free(processorMap.get(processorKeys.getQuick(i)));
            }
        }
    }

    private static class HttpContextFactory implements IOContextFactory<HttpConnectionContext>, Closeable, EagerThreadSetup {
        private final ThreadLocal<WeakObjectPool<HttpConnectionContext>> contextPool;
        private boolean closed = false;

        public HttpContextFactory(HttpServerConfiguration configuration) {
            this.contextPool = new ThreadLocal<>(() -> new WeakObjectPool<>(() ->
                    new HttpConnectionContext(configuration), configuration.getConnectionPoolInitialCapacity()));
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public HttpConnectionContext newInstance(long fd, IODispatcher<HttpConnectionContext> dispatcher) {
            return contextPool.get().pop().of(fd, dispatcher);
        }

        @Override
        public void done(HttpConnectionContext context) {
            if (closed) {
                Misc.free(context);
            } else {
                context.of(-1, null);
                contextPool.get().push(context);
                LOG.info().$("pushed").$();
            }
        }

        @Override
        public void setup() {
            contextPool.get();
        }

        private void closeContextPool() {
            Misc.free(this.contextPool.get());
            LOG.info().$("closed").$();
        }
    }
}
