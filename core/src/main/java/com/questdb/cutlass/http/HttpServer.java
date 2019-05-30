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

package com.questdb.cutlass.http;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.Job;
import com.questdb.mp.SOCountDownLatch;
import com.questdb.mp.Worker;
import com.questdb.network.IOContextFactory;
import com.questdb.network.IODispatcher;
import com.questdb.network.IODispatchers;
import com.questdb.std.*;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class HttpServer implements Closeable {
    private final static Log LOG = LogFactory.getLog(HttpServer.class);
    private final HttpServerConfiguration configuration;
    private final HttpContextFactory httpContextFactory;
    private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
    private final SOCountDownLatch workerHaltLatch;
    private final SOCountDownLatch started = new SOCountDownLatch(1);
    private final int workerCount;
    private final AtomicBoolean running = new AtomicBoolean();
    private final ObjList<HttpServerWorker> workers;
    private IODispatcher<HttpConnectionContext> dispatcher;

    public HttpServer(HttpServerConfiguration configuration) {
        this.configuration = configuration;
        this.httpContextFactory = new HttpContextFactory();

        // We want processor instances to be local to each worker
        // this will allow processors to use their member variables
        // for state. This of course it not request state, but
        // still it is better than having multiple threads hit
        // same class instance.
        this.workerCount = configuration.getWorkerCount();
        this.workers = new ObjList<>(workerCount);
        this.selectors = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            selectors.add(new HttpRequestProcessorSelectorImpl());
        }

        // halt latch that each worker will count down to let main
        // thread know that server is done and allow server shutdown
        // gracefully
        this.workerHaltLatch = new SOCountDownLatch(workerCount);
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
        halt();
        Misc.free(dispatcher);
        for (int i = 0; i < workerCount; i++) {
            HttpRequestProcessorSelectorImpl selector = selectors.getQuick(i);
            Misc.free(selector.defaultRequestProcessor);
            final ObjList<CharSequence> urls = selector.processorMap.keys();
            for (int j = 0, m = urls.size(); j < m; j++) {
                Misc.free(selector.processorMap.get(urls.getQuick(j)));
            }
        }
    }

    public SOCountDownLatch getStartedLatch() {
        return started;
    }

    public void halt() {
        if (running.compareAndSet(true, false)) {
            LOG.info().$("stopping").$();
            started.await();
            for (int i = 0; i < workerCount; i++) {
                workers.getQuick(i).halt();
            }
            workerHaltLatch.await();

            for (int i = 0; i < workerCount; i++) {
                Misc.free(workers.getQuick(i));
            }
            LOG.info().$("stopped").$();
        }
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            dispatcher = IODispatchers.create(
                    configuration.getDispatcherConfiguration(),
                    httpContextFactory
            );

            for (int i = 0; i < workerCount; i++) {
                final ObjHashSet<Job> jobs = new ObjHashSet<>();
                final int index = i;
                jobs.add(dispatcher);
                jobs.add(new Job() {
                    private final HttpRequestProcessorSelector selector = selectors.getQuick(index);

                    @Override
                    public boolean run() {
                        return dispatcher.processIOQueue(
                                (operation, context, dispatcher1)
                                        -> context.handleClientOperation(operation, dispatcher1, selector)
                        );
                    }
                });
                HttpServerWorker worker = new HttpServerWorker(
                        configuration,
                        jobs,
                        workerHaltLatch,
                        -1,
                        LOG,
                        configuration.getConnectionPoolInitialSize(),
                        // have each thread release their own processor selectors
                        // in case processors stash some of their resources in thread-local variables
                        () -> Misc.free(selectors.getQuick(index))
                );
                worker.setName("questdb-http-" + i);
                workers.add(worker);
                worker.start();
            }
            LOG.info().$("started").$();
            started.countDown();
        }
    }

    private static class HttpServerWorker extends Worker implements Closeable {
        private final WeakObjectPool<HttpConnectionContext> contextPool;

        public HttpServerWorker(
                HttpServerConfiguration configuration,
                ObjHashSet<? extends Job> jobs,
                SOCountDownLatch haltLatch,
                int affinity,
                Log log,
                int contextPoolSize,
                Runnable cleaner
        ) {
            super(jobs, haltLatch, affinity, log, cleaner);
            this.contextPool = new WeakObjectPool<>(() -> new HttpConnectionContext(configuration), contextPoolSize);
        }

        @Override
        public void close() {
            contextPool.close();
        }
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

    private class HttpContextFactory implements IOContextFactory<HttpConnectionContext> {
        @Override
        public HttpConnectionContext newInstance(long fd) {
            Thread thread = Thread.currentThread();
            assert thread instanceof HttpServerWorker;
            HttpConnectionContext connectionContext = ((HttpServerWorker) thread).contextPool.pop();
            return connectionContext.of(fd);
        }

        @Override
        public void done(HttpConnectionContext context) {
            Thread thread = Thread.currentThread();
            // it is possible that context is in transit (on a queue somewhere)
            // and server shutdown is performed by a non-worker thread
            // in this case we just close context
            if (thread instanceof HttpServerWorker) {
                ((HttpServerWorker) thread).contextPool.push(context);
            } else {
                Misc.free(context);
            }
        }
    }
}
