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
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
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
    private final ObjList<LineTcpConnectionContext> busyContexts = new ObjList<>();

    public LineTcpServer(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool workerPool,
            MessageBus messageBus
    ) {
        this.contextFactory = new LineTcpConnectionContextFactory(engine, lineConfiguration, messageBus);
        this.dispatcher = IODispatchers.create(
                lineConfiguration
                        .getNetDispatcherConfiguration(),
                contextFactory);
        workerPool.assign(dispatcher);
        scheduler = new LineTcpMeasurementScheduler(lineConfiguration, engine, workerPool);
        final IORequestProcessor<LineTcpConnectionContext> processor = (operation, context) -> {
            if (context.handleIO()) {
                busyContexts.add(context);
            }
        };
        workerPool.assign(new SynchronizedJob() {
            @Override
            protected boolean runSerially() {
                int n = busyContexts.size();
                while (n > 0) {
                    n--;
                    if (!busyContexts.getQuick(n).handleIO()) {
                        busyContexts.remove(n);
                    } else {
                        n++;
                        break;
                    }
                }

                if (n == 0) {
                    return dispatcher.processIOQueue(processor);
                }

                return true;
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

        ServerFactory<LineTcpServer, WorkerPoolAwareConfiguration> factory = (netWorkerPoolConfiguration, engine, workerPool, local, bus,
                functionfactory) -> new LineTcpServer(
                        lineConfiguration,
                        cairoEngine,
                        workerPool,
                        bus);
        return WorkerPoolAwareConfiguration.create(lineConfiguration.getWorkerPoolConfiguration(), sharedWorkerPool, log, cairoEngine, factory, null);
    }

    @Override
    public void close() {
        Misc.free(scheduler);
        Misc.free(contextFactory);
        Misc.free(dispatcher);
    }

    private class LineTcpConnectionContextFactory implements IOContextFactory<LineTcpConnectionContext>, Closeable, EagerThreadSetup {
        private final ThreadLocal<WeakObjectPool<LineTcpConnectionContext>> contextPool;
        private boolean closed = false;

        public LineTcpConnectionContextFactory(CairoEngine engine, LineTcpReceiverConfiguration configuration, @Nullable MessageBus messageBus) {
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
