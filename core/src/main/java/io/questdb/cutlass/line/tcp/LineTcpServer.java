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

package io.questdb.cutlass.line.tcp;

import java.io.Closeable;

import org.jetbrains.annotations.Nullable;

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.EagerThreadSetup;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ThreadLocal;
import io.questdb.std.WeakObjectPool;

public class LineTcpServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpServer.class);
    private final IODispatcher<LineTcpConnectionContext> dispatcher;
    private final LineTcpConnectionContextFactory contextFactory;
    private final LineTcpMeasurementScheduler scheduler;

    public LineTcpServer(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool ioWorkerPool,
            WorkerPool writerWorkerPool
    ) {
        this.contextFactory = new LineTcpConnectionContextFactory(lineConfiguration);
        this.dispatcher = IODispatchers.create(
                lineConfiguration.getNetDispatcherConfiguration(),
                contextFactory);
        ioWorkerPool.assign(dispatcher);
        scheduler = new LineTcpMeasurementScheduler(lineConfiguration, engine, writerWorkerPool);
        for (int i = 0, n = ioWorkerPool.getWorkerCount(); i < n; i++) {
            ioWorkerPool.assign(i, new Job() {
                // Context blocked on LineTcpMeasurementScheduler queue
                private final ObjList<LineTcpConnectionContext> busyContexts = new ObjList<>();
                private int busyContextIndex = 0;
                private final IORequestProcessor<LineTcpConnectionContext> onRequest = this::onRequest;

                private void onRequest(int operation, LineTcpConnectionContext context) {
                    if (handleIO(context)) {
                        busyContexts.add(context);
                        LOG.debug().$("context is waiting on a full queue [fd=").$(context.getFd()).$(']').$();
                    }
                }

                @Override
                public boolean run(int workerId) {
                    boolean busy = false;
                    while (busyContexts.size() > 0) {
                        int i = busyContextIndex % busyContexts.size();
                        LineTcpConnectionContext busyContext = busyContexts.getQuick(i);
                        if (handleIO(busyContext)) {
                            busyContextIndex++;
                            busy = true;
                            break;
                        }
                        LOG.debug().$("context is no longer waiting on a full queue [fd=").$(busyContext.getFd()).$(']').$();
                        busyContexts.remove(i);
                    }

                    if (dispatcher.processIOQueue(onRequest)) {
                        return true;
                    }
                    return busy;
                }
            });
        }

        final Closeable cleaner = contextFactory::closeContextPool;
        for (int i = 0, n = ioWorkerPool.getWorkerCount(); i < n; i++) {
            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            ioWorkerPool.assign(i, cleaner);
        }
    }

    @Nullable
    public static LineTcpServer create(
            LineTcpReceiverConfiguration lineConfiguration,
            WorkerPool sharedWorkerPool,
            Log log,
            CairoEngine cairoEngine
    ) {
        if (!lineConfiguration.isEnabled()) {
            return null;
        }

        WorkerPool ioWorkerPool = WorkerPoolAwareConfiguration.configureWorkerPool(lineConfiguration.getIOWorkerPoolConfiguration(), sharedWorkerPool);
        WorkerPool writerWorkerPool = WorkerPoolAwareConfiguration.configureWorkerPool(lineConfiguration.getWriterWorkerPoolConfiguration(), sharedWorkerPool);
        LineTcpServer lineTcpServer = new LineTcpServer(lineConfiguration, cairoEngine, ioWorkerPool, writerWorkerPool);
        if (ioWorkerPool != sharedWorkerPool) {
            ioWorkerPool.start(LOG);
        }
        if (writerWorkerPool != sharedWorkerPool) {
            writerWorkerPool.start(LOG);
        }
        return lineTcpServer;
    }

    @Override
    public void close() {
        Misc.free(scheduler);
        Misc.free(contextFactory);
        Misc.free(dispatcher);
    }

    private boolean handleIO(LineTcpConnectionContext context) {
        if (!context.invalid()) {
            switch (context.handleIO()) {
                case NEEDS_READ:
                    context.getDispatcher().registerChannel(context, IOOperation.READ);
                    return false;
                case NEEDS_WRITE:
                    context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                    return false;
                case QUEUE_FULL:
                    return true;
                case NEEDS_DISCONNECT:
                    context.getDispatcher().disconnect(context);
                    return false;
            }
        }
        return false;
    }

    private class LineTcpConnectionContextFactory implements IOContextFactory<LineTcpConnectionContext>, Closeable, EagerThreadSetup {
        private final ThreadLocal<WeakObjectPool<LineTcpConnectionContext>> contextPool;
        private boolean closed = false;

        public LineTcpConnectionContextFactory(LineTcpReceiverConfiguration configuration) {
            ObjectFactory<LineTcpConnectionContext> factory;
            if (null == configuration.getAuthDbPath()) {
                factory = () -> new LineTcpConnectionContext(configuration, scheduler);
            } else {
                AuthDb authDb = new AuthDb(configuration);
                factory = () -> new LineTcpAuthConnectionContext(configuration, authDb, scheduler);
            }

            this.contextPool = new ThreadLocal<>(() -> new WeakObjectPool<>(factory, configuration.getConnectionPoolInitialCapacity()));
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public LineTcpConnectionContext newInstance(long fd, IODispatcher<LineTcpConnectionContext> dispatcher) {
            return contextPool.get().pop().of(fd, dispatcher);
        }

        @Override
        public void done(LineTcpConnectionContext context) {
            if (closed) {
                Misc.free(context);
            } else {
                context.of(-1, null);
                contextPool.get().push(context);
                LOG.debug().$("pushed").$();
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
