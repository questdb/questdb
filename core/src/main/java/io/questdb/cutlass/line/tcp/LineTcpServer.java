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

import io.questdb.MessageBus;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.WorkerPoolAwareConfiguration.ServerFactory;
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
import io.questdb.std.ObjectFactory;
import io.questdb.std.WeakObjectPool;

public class LineTcpServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpServer.class);
    private final IODispatcher<LineTcpConnectionContext> dispatcher;
    private final LineTcpConnectionContextFactory contextFactory;
    private final LineTcpMeasurementScheduler scheduler;

    public LineTcpServer(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool workerPool,
            @Nullable MessageBus messageBus
    ) {
        this.contextFactory = new LineTcpConnectionContextFactory(lineConfiguration);
        this.dispatcher = IODispatchers.create(
                lineConfiguration.getNetDispatcherConfiguration(),
                contextFactory
        );
        workerPool.assign(dispatcher);
        scheduler = new LineTcpMeasurementScheduler(lineConfiguration, engine, workerPool, messageBus);
        workerPool.assign(new Job() {
            // Context blocked on LineTcpMeasurementScheduler queue
            private ThreadLocal<LineTcpConnectionContext> refBusyContext = new ThreadLocal<>();
            private final IORequestProcessor<LineTcpConnectionContext> onRequest = this::onRequest;

            private void onRequest(int operation, LineTcpConnectionContext context) {
                assert refBusyContext.get() == null;
                if (handleIO(context)) {
                    refBusyContext.set(context);
                }
            }

            @Override
            public boolean run(int workerId) {
                LineTcpConnectionContext busyContext = refBusyContext.get();
                if (null == busyContext) {
                    return dispatcher.processIOQueue(onRequest);
                }

                if (!handleIO(busyContext)) {
                    refBusyContext.set(null);
                    return true;
                }

                return false;
            }
        });

        final Closeable cleaner = contextFactory::closeContextPool;
        for (int i = 0, n = workerPool.getWorkerCount(); i < n; i++) {
            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            workerPool.assign(i, cleaner);
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

        ServerFactory<LineTcpServer, WorkerPoolAwareConfiguration> factory = (
                netWorkerPoolConfiguration,
                engine,
                workerPool,
                local,
                bus,
                functionFactory
        ) -> new LineTcpServer(
                lineConfiguration,
                cairoEngine,
                workerPool,
                bus
        );
        return WorkerPoolAwareConfiguration.create(
                lineConfiguration.getWorkerPoolConfiguration(),
                sharedWorkerPool,
                log,
                cairoEngine,
                factory,
                null
        );
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
                case NEEDS_CPU:
                    return true;
                case NEEDS_DISCONNECT:
                    context.getDispatcher().disconnect(context);
                    return false;
            }
        }
        return false;
    }

    private class LineTcpConnectionContextFactory implements IOContextFactory<LineTcpConnectionContext>, Closeable, EagerThreadSetup {
        private final io.questdb.std.ThreadLocal<WeakObjectPool<LineTcpConnectionContext>> contextPool;
        private boolean closed = false;

        public LineTcpConnectionContextFactory(LineTcpReceiverConfiguration configuration) {
            ObjectFactory<LineTcpConnectionContext> factory;
            if (null == configuration.getAuthDbPath()) {
                factory = () -> new LineTcpConnectionContext(configuration, scheduler);
            } else {
                AuthDb authDb = new AuthDb(configuration);
                factory = () -> new LineTcpAuthConnectionContext(configuration, authDb, scheduler);
            }

            this.contextPool = new io.questdb.std.ThreadLocal<>(() -> new WeakObjectPool<>(factory, configuration.getConnectionPoolInitialCapacity()));
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
