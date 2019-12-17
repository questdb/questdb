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

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.processors.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.EagerThreadSetup;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

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
                public boolean run() {
                    return dispatcher.processIOQueue(processor);
                }
            });

            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            pool.assign(i, () -> {
                Misc.free(selectors.getQuick(index));
                httpContextFactory.closeContextPool();
            });
        }
    }

    @Nullable
    public static HttpServer create(
            HttpServerConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine
    ) {
        return WorkerPoolAwareConfiguration.create(
                configuration, sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                CREATE0
        );
    }

    private static HttpServer create0(
            HttpServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            boolean sharedWorkerPool
    ) {
        final HttpServer s = new HttpServer(configuration, workerPool, sharedWorkerPool);

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/exec";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new JsonQueryProcessor(
                        configuration.getJsonQueryProcessorConfiguration(),
                        cairoEngine
                );
            }
        });

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/imp";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TextImportProcessor(configuration.getTextImportProcessorConfiguration(), cairoEngine);
            }
        });

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/exp";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TextQueryProcessor(configuration.getJsonQueryProcessorConfiguration(), cairoEngine);
            }
        });

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/chk";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TableStatusCheckProcessor(cairoEngine);
            }
        });

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new StaticContentProcessor(configuration.getStaticContentProcessorConfiguration());
            }
        });
        return s;

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
