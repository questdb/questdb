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
import com.questdb.mp.Worker;
import com.questdb.network.IOContextFactory;
import com.questdb.network.IODispatcher;
import com.questdb.network.IODispatchers;
import com.questdb.network.NetworkFacade;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Misc;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class HttpServer implements Closeable {
    private final static Log LOG = LogFactory.getLog(HttpServer.class);
    private final HttpServerConfiguration configuration;
    private final HttpContextFactory httpContextFactory;
    private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
    private final CountDownLatch workerHaltLatch;
    private final CountDownLatch started = new CountDownLatch(1);
    private final int workerCount;
    private final AtomicBoolean running = new AtomicBoolean();
    private final ObjList<Worker> workers;
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
        this.workerHaltLatch = new CountDownLatch(workerCount);
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
        Misc.free(dispatcher);
    }

    public CountDownLatch getStartedLatch() {
        return started;
    }

    public void halt() throws InterruptedException {
        if (running.compareAndSet(true, false)) {
            started.await();
            for (int i = 0; i < workerCount; i++) {
                workers.getQuick(i).halt();
            }
            workerHaltLatch.await();
        }
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            dispatcher = IODispatchers.create(
                    configuration.getDispatcherConfiguration(),
                    httpContextFactory
            );

            final NetworkFacade nf = configuration.getDispatcherConfiguration().getNetworkFacade();

            for (int i = 0; i < workerCount; i++) {
                ObjHashSet<Job> jobs = new ObjHashSet<>();
                final int index = i;
                jobs.add(dispatcher);
                jobs.add(new Job() {
                    private final HttpRequestProcessorSelector selector = selectors.getQuick(index);

                    @Override
                    public boolean run() {
                        return dispatcher.processIOQueue(
                                (operation, context, dispatcher1)
                                        -> context.handleClientOperation(operation, nf, dispatcher1, selector)
                        );
                    }
                });
                Worker worker = new Worker(jobs, workerHaltLatch, -1, LOG);
                worker.setName("questdb-http-" + i);
                workers.add(worker);
                worker.start();
            }
            LOG.info().$("started").$();
            started.countDown();
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
    }

    private class HttpContextFactory implements IOContextFactory<HttpConnectionContext> {
        @Override
        public HttpConnectionContext newInstance(long fd) {
            return new HttpConnectionContext(configuration, fd);
        }
    }
}
