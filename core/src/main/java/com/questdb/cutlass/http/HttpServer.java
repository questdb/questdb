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
import com.questdb.mp.WorkerPool;
import com.questdb.network.IOContextFactory;
import com.questdb.network.IODispatcher;
import com.questdb.network.IODispatchers;
import com.questdb.std.ThreadLocal;
import com.questdb.std.*;

import java.io.Closeable;

public class HttpServer implements Closeable {
    private final ObjList<HttpRequestProcessorSelectorImpl> selectors;
    private static final Log LOG = LogFactory.getLog(HttpServer.class);
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final int workerCount;
    private final HttpContextFactory httpContextFactory;

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

    public HttpServer(HttpServerConfiguration configuration, WorkerPool pool) {
        this.workerCount = pool.getWorkerCount();
        this.selectors = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            selectors.add(new HttpRequestProcessorSelectorImpl());
        }

        this.httpContextFactory = new HttpContextFactory(configuration, configuration.getConnectionPoolInitialCapacity());
        this.dispatcher = IODispatchers.create(
                configuration.getDispatcherConfiguration(),
                httpContextFactory
        );

        pool.assign(dispatcher);

        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            final int index = i;
            pool.assign(i, new Job() {
                private final HttpRequestProcessorSelector selector = selectors.getQuick(index);

                @Override
                public boolean run() {
                    return dispatcher.processIOQueue(
                            (operation, context, dispatcher1)
                                    -> context.handleClientOperation(operation, dispatcher1, selector)
                    );
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

    @Override
    public void close() {
        Misc.free(httpContextFactory);
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

    private static class HttpContextFactory implements IOContextFactory<HttpConnectionContext>, Closeable {
        private final ThreadLocal<WeakObjectPool<HttpConnectionContext>> contextPool;
        private boolean closed = false;

        public HttpContextFactory(HttpServerConfiguration configuration, int poolSize) {
            this.contextPool = new ThreadLocal<>(() -> new WeakObjectPool<>(() -> new HttpConnectionContext(configuration), poolSize));
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
