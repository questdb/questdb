/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.cutlass.http;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.processors.*;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.types.InputFormatConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.time.DateFormatFactory;
import io.questdb.std.time.DateLocaleFactory;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class HttpServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(HttpServer.class);
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
                        (operation, context, dispatcher) -> context.handleClientOperation(operation, dispatcher, selector);

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
            WorkerPool workerPool,
            Log workerPoolLog,
            CairoEngine cairoEngine
    ) throws JsonException {
        if (configuration.isEnabled()) {
            final DateFormatFactory dateFormatFactory = new DateFormatFactory();
            final io.questdb.std.microtime.DateFormatFactory timestampFormatFactory = new io.questdb.std.microtime.DateFormatFactory();
            final InputFormatConfiguration inputFormatConfiguration = new InputFormatConfiguration(
                    dateFormatFactory,
                    DateLocaleFactory.INSTANCE,
                    timestampFormatFactory,
                    io.questdb.std.microtime.DateLocaleFactory.INSTANCE
            );

            try (JsonLexer jsonLexer = new JsonLexer(configuration.getTextImportProcessorConfiguration().getTextConfiguration().getJsonCacheSize(), configuration.getTextImportProcessorConfiguration().getTextConfiguration().getJsonCacheLimit())) {
                inputFormatConfiguration.parseConfiguration(
                        jsonLexer,
                        configuration.getTextImportProcessorConfiguration().getTextConfiguration().getAdapterSetConfigurationFileName()
                );
            }

            final WorkerPool localPool;
            if (configuration.getWorkerCount() > 0) {
                localPool = new WorkerPool(new WorkerPoolConfiguration() {
                    @Override
                    public int[] getWorkerAffinity() {
                        return configuration.getWorkerAffinity();
                    }

                    @Override
                    public int getWorkerCount() {
                        return configuration.getWorkerCount();
                    }

                    @Override
                    public boolean haltOnError() {
                        return configuration.workerHaltOnError();
                    }
                });
            } else {
                localPool = workerPool;
            }
            final HttpServer httpServer = new HttpServer(configuration, localPool, localPool != workerPool);

            httpServer.bind(new HttpRequestProcessorFactory() {
                @Override
                public String getUrl() {
                    return "/exec";
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return new JsonQueryProcessor(
                            configuration.getJsonQueryProcessorConfiguration(),
                            cairoEngine,
                            inputFormatConfiguration
                    );
                }
            });

            httpServer.bind(new HttpRequestProcessorFactory() {
                @Override
                public String getUrl() {
                    return "/imp";
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return new TextImportProcessor(
                            configuration.getTextImportProcessorConfiguration(),
                            cairoEngine,
                            inputFormatConfiguration
                    );
                }
            });

            httpServer.bind(new HttpRequestProcessorFactory() {
                @Override
                public String getUrl() {
                    return "/exp";
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return new TextQueryProcessor(configuration.getJsonQueryProcessorConfiguration(), cairoEngine);
                }
            });

            httpServer.bind(new HttpRequestProcessorFactory() {
                @Override
                public String getUrl() {
                    return "/chk";
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return new TableStatusCheckProcessor(cairoEngine);
                }
            });

            httpServer.bind(new HttpRequestProcessorFactory() {
                @Override
                public String getUrl() {
                    return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return new StaticContentProcessor(configuration.getStaticContentProcessorConfiguration());
                }
            });

            if (localPool != workerPool) {
                localPool.start(workerPoolLog);
            }

            return httpServer;
        }
        return null;
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

    private static class HttpContextFactory implements IOContextFactory<HttpConnectionContext>, Closeable {
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
        public HttpConnectionContext newInstance(long fd) {
            return contextPool.get().pop().of(fd);
        }

        @Override
        public void done(HttpConnectionContext context) {
            if (closed) {
                Misc.free(context);
            } else {
                context.of(-1);
                contextPool.get().push(context);
                LOG.info().$("pushed").$();
            }
        }

        private void closeContextPool() {
            Misc.free(this.contextPool.get());
            LOG.info().$("closed").$();
        }
    }
}
