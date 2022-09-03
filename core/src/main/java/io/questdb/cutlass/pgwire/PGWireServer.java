/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.Services;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.network.*;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.WeakMutableObjectPool;
import org.jetbrains.annotations.Nullable;


import static io.questdb.network.IODispatcher.*;

public class PGWireServer implements QuietCloseable {

    private static final Log LOG = LogFactory.getLog(PGWireServer.class);

    private final IODispatcher<PGConnectionContext> dispatcher;
    private final PGConnectionContextFactory contextFactory;
    private final WorkerPool workerPool;
    private final Metrics metrics;

    public PGWireServer(
            PGWireConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool,
            boolean workerPoolLocal,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            PGConnectionContextFactory contextFactory
    ) {
        this.contextFactory = contextFactory;
        this.dispatcher = IODispatchers.create(
                configuration.getDispatcherConfiguration(),
                contextFactory
        );
        this.metrics = engine.getMetrics();

        workerPool.assign(dispatcher);

        for (int i = 0, n = workerPool.getWorkerCount(); i < n; i++) {
            final PGJobContext jobContext = new PGJobContext(configuration, engine, functionFactoryCache, snapshotAgent);

            final SCSequence queryCacheEventSubSeq = new SCSequence();
            final FanOut queryCacheEventFanOut = engine.getMessageBus().getQueryCacheEventFanOut();
            queryCacheEventFanOut.and(queryCacheEventSubSeq);

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
                    } catch (Throwable e) { //must remain last in catch list!
                        LOG.critical().$("internal error [ex=").$(e).$(']').$();
                        // This is a critical error, so we treat it as an unhandled one.
                        metrics.healthCheck().incrementUnhandledErrors();
                        context.getDispatcher().disconnect(context, DISCONNECT_REASON_SERVER_ERROR);
                    }
                };

                @Override
                public boolean run(int workerId) {
                    long seq = queryCacheEventSubSeq.next();
                    if (seq > -1) {
                        // Queue is not empty, so flush query cache.
                        LOG.info().$("flushing PG Wire query cache [worker=").$(workerId).$(']').$();
                        jobContext.flushQueryCache();
                        queryCacheEventSubSeq.done(seq);
                    }
                    return dispatcher.processIOQueue(processor);
                }
            });

            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            workerPool.assign(i, () -> {
                Misc.free(jobContext);
                contextFactory.closeContextPool();
                engine.getMessageBus().getQueryCacheEventFanOut().remove(queryCacheEventSubSeq);
                queryCacheEventSubSeq.clear();
            });
        }

        if (workerPoolLocal) {
            this.workerPool = workerPool;
        } else {
            this.workerPool = null;
        }
    }

    @Override
    public void close() {
        // worker pool will only be set if it is "local"
        if (workerPool != null) {
            workerPool.close();
        }
        Misc.free(contextFactory);
        Misc.free(dispatcher);
    }

    public int getPort() {
        return dispatcher.getPort();
    }

    public static class PGConnectionContextFactory implements IOContextFactory<PGConnectionContext>, QuietCloseable, EagerThreadSetup {
        private final ThreadLocal<WeakMutableObjectPool<PGConnectionContext>> contextPool;
        private boolean closed = false;

        public PGConnectionContextFactory(CairoEngine engine, PGWireConfiguration configuration, int workerCount, int sharedWorkerCount) {
            this.contextPool = new ThreadLocal<>(() -> new WeakMutableObjectPool<>(() ->
                    new PGConnectionContext(engine, configuration, getSqlExecutionContext(engine, workerCount, sharedWorkerCount)), configuration.getConnectionPoolInitialCapacity()));
        }

        protected SqlExecutionContextImpl getSqlExecutionContext(CairoEngine engine, int workerCount, int sharedWorkerCount) {
            return new SqlExecutionContextImpl(engine, workerCount, sharedWorkerCount);
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
