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

import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.EagerThreadSetup;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.network.*;
import io.questdb.std.Misc;
import io.questdb.std.ThreadLocal;
import io.questdb.std.WeakObjectPool;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.network.IODispatcher.*;

public class PGWireServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(PGWireServer.class);
    private final IODispatcher<PGConnectionContext> dispatcher;
    private final PGConnectionContextFactory contextFactory;
    private final WorkerPool workerPool;
    private final MessageBus messageBus;

    public PGWireServer(
            PGWireConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool,
            boolean workerPoolLocal,
            MessageBus messageBus,
            FunctionFactoryCache functionFactoryCache
    ) {
        this.messageBus = messageBus;
        this.contextFactory = new PGConnectionContextFactory(engine, configuration, messageBus, workerPool.getWorkerCount());
        this.dispatcher = IODispatchers.create(
                configuration.getDispatcherConfiguration(),
                contextFactory
        );

        workerPool.assign(dispatcher);

        for (int i = 0, n = workerPool.getWorkerCount(); i < n; i++) {
            final PGJobContext jobContext = new PGJobContext(configuration, engine, messageBus, functionFactoryCache);
            workerPool.assign(i, new Job() {
                private final IORequestProcessor<PGConnectionContext> processor = (operation, context) -> {
                    try {
                        jobContext.handleClientOperation(context, operation);
                        context.getDispatcher().registerChannel(context, IOOperation.READ);
                    } catch (PeerIsSlowToWriteException e) {
                        context.getDispatcher().registerChannel(context, IOOperation.READ);
                    } catch (PeerIsSlowToReadException e) {
                        context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                    } catch (PeerDisconnectedException e) {
                        context.getDispatcher().disconnect(context, operation == IOOperation.READ ? DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV : DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND);
                    } catch (BadProtocolException e) {
                        context.getDispatcher().disconnect(context, DISCONNECT_REASON_PROTOCOL_VIOLATION);
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

        if (workerPoolLocal) {
            this.workerPool = workerPool;
        } else {
            this.workerPool = null;
        }
    }

    @Nullable
    public static PGWireServer create(
            PGWireConfiguration configuration,
            WorkerPool sharedWorkerPool,
            Log log,
            CairoEngine cairoEngine,
            FunctionFactoryCache functionFactoryCache,
            Metrics metrics
    ) {
        return WorkerPoolAwareConfiguration.create(
                configuration,
                sharedWorkerPool,
                log,
                cairoEngine,
                (conf, engine, workerPool, local, bus, functionFactoryCache1, metrics1) -> new PGWireServer(conf, cairoEngine, workerPool, local, bus, functionFactoryCache1),
                functionFactoryCache,
                metrics
        );
    }

    @Override
    public void close() {
        // worker pool will only be set if it is "local"
        if (workerPool != null) {
            workerPool.halt();
        }
        Misc.free(contextFactory);
        Misc.free(dispatcher);

        // when worker pool is not null we will also have local message bus
        if (workerPool != null) {
            Misc.free(messageBus);
        }
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
