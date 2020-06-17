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

import io.questdb.MessageBus;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnIndexerJob;
import io.questdb.cairo.pool.ex.EntryUnavailableException;
import io.questdb.cutlass.http.processors.*;
import io.questdb.griffin.engine.groupby.vect.GroupByNotKeyedJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

public class HttpServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(HttpServer.class);
    private static final WorkerPoolAwareConfiguration.ServerFactory<HttpServer, HttpServerConfiguration> CREATE0 = HttpServer::create0;
    private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final int workerCount;
    private final HttpContextFactory httpContextFactory;
    private final WorkerPool workerPool;

    private static final int retryQueueLength = 4096;
    private final RingQueue<RetryHolder> retryQueue = new RingQueue<RetryHolder>(RetryHolder::new, retryQueueLength);
    private final Sequence retryPubSequence = new MPSequence(retryQueueLength);
    private final Sequence retrySubSequence = new MCSequence(retryQueueLength);

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

        retryPubSequence.then(retrySubSequence).then(retryPubSequence);
        RescheduleContext rescheduleContext = retry -> queueRetry(retry);

        for (int i = 0; i < workerCount; i++) {
            final int index = i;
            pool.assign(i, new Job() {
                private final HttpRequestProcessorSelector selector = selectors.getQuick(index);
                private final IORequestProcessor<HttpConnectionContext> processor =
                        (operation, context) -> context.handleClientOperation(operation, selector, rescheduleContext);

                @Override
                public boolean run(int workerId) {
                    boolean useful = dispatcher.processIOQueue(processor);
                    // Run retries
                    if (useful) {
                        useful |= checkReruns();
                    }
                    return useful ;//|| checkReruns();
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
            MessageBus messageBus
    ) {
        return WorkerPoolAwareConfiguration.create(
                configuration, sharedWorkerPool,
                workerPoolLog,
                cairoEngine,
                CREATE0,
                messageBus
        );
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

    private boolean checkReruns() {
        boolean rerunCompleted = false;

        boolean firstProcessed = false;
        while (true) {
            long cursor = retrySubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                LockSupport.parkNanos(1);
                continue;
            }

            // -1 = queue is empty. All done.
            if (cursor < 0) {
                break;
            }

            Retry toRun;
            try {
                toRun = retryQueue.get(cursor).retry;
            } finally {
                retrySubSequence.done(cursor);
            }

            if (toRun == RetryHolder.MARKER) {
                if (!firstProcessed) {
                    // Someone else marker stolen.
                    // Add it back so that another thread stops at some point.
                    queueRetry((RetryHolder.MARKER));
                }
                // All checked.
                break;
            }
            boolean completed = toRun.tryRerun();
            if (!completed) {
                queueRetry(toRun);
            }
            rerunCompleted |= completed;

            // Set a marker in the queue so that if it's found it means all elements checked.
            if (!firstProcessed) {
                firstProcessed = true;
                queueRetry(RetryHolder.MARKER);
            }

        }
        return rerunCompleted;
    }

    private void queueRetry(Retry retry) {
        while (true) {
            long cursor = retryPubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                LockSupport.parkNanos(1);
                continue;
            }

            // -1 = queue is empty. It means there are already too many retries waiting
            if (cursor < 0) {
                throw EntryUnavailableException.INSTANCE;
            }

            try {
                retryQueue.get(cursor).retry = retry;
                return;
            } finally {
                retryPubSequence.done(cursor);
            }
        }
    }

    private static HttpServer create0(
            HttpServerConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            boolean localPool,
            MessageBus messageBus
    ) {
        final HttpServer s = new HttpServer(configuration, workerPool, localPool);
        QueryCache.configure(configuration);

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new JsonQueryProcessor(
                        configuration.getJsonQueryProcessorConfiguration(),
                        cairoEngine,
                        messageBus,
                        workerPool.getWorkerCount()

                );
            }

            @Override
            public String getUrl() {
                return "/exec";
            }
        });

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new TextImportProcessor(cairoEngine);
            }

            @Override
            public String getUrl() {
                return "/imp";
            }
        });

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new TextQueryProcessor(
                        configuration.getJsonQueryProcessorConfiguration(),
                        cairoEngine,
                        messageBus,
                        workerPool.getWorkerCount()
                );
            }

            @Override
            public String getUrl() {
                return "/exp";
            }
        });

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new TableStatusCheckProcessor(cairoEngine, configuration.getJsonQueryProcessorConfiguration());
            }

            @Override
            public String getUrl() {
                return "/chk";
            }
        });

        s.bind(new HttpRequestProcessorFactory() {
            @Override
            public HttpRequestProcessor newInstance() {
                return new StaticContentProcessor(configuration.getStaticContentProcessorConfiguration());
            }

            @Override
            public String getUrl() {
                return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
            }
        });

        // jobs that help parallel execution of queries
        workerPool.assign(new ColumnIndexerJob(messageBus));
        workerPool.assign(new GroupByNotKeyedJob(messageBus));
        return s;

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
