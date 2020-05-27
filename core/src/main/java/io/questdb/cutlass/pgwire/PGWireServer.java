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

package io.questdb.cutlass.pgwire;

import java.io.Closeable;

import org.jetbrains.annotations.Nullable;

import io.questdb.MessageBus;
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
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.std.Misc;
import io.questdb.std.ThreadLocal;
import io.questdb.std.WeakObjectPool;

public class PGWireServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(PGWireServer.class);
    private final IODispatcher<PGConnectionContext> dispatcher;
    private final PGConnectionContextFactory contextFactory;

    public PGWireServer(
            PGWireConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool,
            MessageBus messageBus
    ) {
        this.contextFactory = new PGConnectionContextFactory(engine, configuration, messageBus, workerPool.getWorkerCount());
        this.dispatcher = IODispatchers.create(
                configuration.getDispatcherConfiguration(),
                contextFactory
        );

        workerPool.assign(dispatcher);

        for (int i = 0, n = workerPool.getWorkerCount(); i < n; i++) {
            final PGJobContext jobContext = new PGJobContext(configuration, engine);
            workerPool.assign(i, new Job() {
                private final IORequestProcessor<PGConnectionContext> processor = (operation, context) -> {
                    try {
                        jobContext.handleClientOperation(context);
                        context.getDispatcher().registerChannel(context, IOOperation.READ);
                    } catch (PeerIsSlowToWriteException e) {
                        context.getDispatcher().registerChannel(context, IOOperation.READ);
                    } catch (PeerIsSlowToReadException e) {
                        context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                    } catch (PeerDisconnectedException | BadProtocolException e) {
                        context.getDispatcher().disconnect(context);
                    }
                };

                @Override
                public boolean run(int workerId) {
                    return dispatcher.processIOQueue(processor);
                }
            });

            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            workerPool.assign(i, () -> {
                Misc.free(jobContext);
                contextFactory.closeContextPool();
            });
        }
    }

    @Nullable
    public static PGWireServer create(
            PGWireConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log log,
            CairoEngine cairoEngine,
            MessageBus messageBus
    ) {
        return WorkerPoolAwareConfiguration.create(
                configuration,
                sharedWorkerPool,
                log,
                cairoEngine,
                (conf, engine, workerPool, local, bus) -> new PGWireServer(conf, cairoEngine, workerPool, bus),
                messageBus
        );
    }

    @Override
    public void close() {
        Misc.free(contextFactory);
        Misc.free(dispatcher);
    }

    private static class PGConnectionContextFactory implements IOContextFactory<PGConnectionContext>, Closeable, EagerThreadSetup {
        private final ThreadLocal<WeakObjectPool<PGConnectionContext>> contextPool;
        private boolean closed = false;

        public PGConnectionContextFactory(CairoEngine engine, PGWireConfiguration configuration, @Nullable MessageBus messageBus, int workerCount) {
            this.contextPool = new ThreadLocal<>(() -> new WeakObjectPool<>(() ->
            new PGConnectionContext(engine, configuration, messageBus, workerCount), configuration.getConnectionPoolInitialCapacity()));
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public PGConnectionContext newInstance(long fd, IODispatcher<PGConnectionContext> dispatcher) {
            return contextPool.get().pop().of(fd, dispatcher);
        }

        @Override
        public void done(PGConnectionContext context) {
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
