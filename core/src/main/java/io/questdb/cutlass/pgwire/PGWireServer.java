/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
import io.questdb.mp.Job;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.network.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjectFactory;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.network.IODispatcher.*;

public class PGWireServer implements Closeable {

    private static final Log LOG = LogFactory.getLog(PGWireServer.class);

    private final IODispatcher<PGConnectionContext> dispatcher;
    private final Metrics metrics;
    private final CircuitBreakerRegistry registry;
    private final WorkerPool workerPool;

    public PGWireServer(
            PGWireConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            PGConnectionContextFactory contextFactory,
            CircuitBreakerRegistry registry
    ) {
        this.dispatcher = IODispatchers.create(
                configuration.getDispatcherConfiguration(),
                contextFactory
        );
        this.metrics = engine.getMetrics();
        this.workerPool = workerPool;
        this.registry = registry;

        workerPool.assign(dispatcher);

        for (int i = 0, n = workerPool.getWorkerCount(); i < n; i++) {
            final PGJobContext jobContext = new PGJobContext(configuration, engine, functionFactoryCache, snapshotAgent);

            final SCSequence queryCacheEventSubSeq = new SCSequence();
            final FanOut queryCacheEventFanOut = engine.getMessageBus().getQueryCacheEventFanOut();
            queryCacheEventFanOut.and(queryCacheEventSubSeq);

            workerPool.assign(i, new Job() {
                private final IORequestProcessor<PGConnectionContext> processor = (operation, context) -> {
                    try {
                        if (operation == IOOperation.HEARTBEAT) {
                            context.getDispatcher().registerChannel(context, IOOperation.HEARTBEAT);
                            return false;
                        }
                        jobContext.handleClientOperation(context, operation);
                        context.getDispatcher().registerChannel(context, IOOperation.READ);
                        return true;
                    } catch (PeerIsSlowToWriteException e) {
                        context.getDispatcher().registerChannel(context, IOOperation.READ);
                    } catch (PeerIsSlowToReadException e) {
                        context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                    } catch (QueryPausedException e) {
                        context.setSuspendEvent(e.getEvent());
                        context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                    } catch (PeerDisconnectedException e) {
                        context.getDispatcher().disconnect(
                                context,
                                operation == IOOperation.READ
                                        ? DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV
                                        : DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND
                        );
                    } catch (BadProtocolException e) {
                        context.getDispatcher().disconnect(context, DISCONNECT_REASON_PROTOCOL_VIOLATION);
                    } catch (Throwable e) { // must remain last in catch list!
                        LOG.critical().$("internal error [ex=").$(e).$(']').$();
                        // This is a critical error, so we treat it as an unhandled one.
                        metrics.health().incrementUnhandledErrors();
                        context.getDispatcher().disconnect(context, DISCONNECT_REASON_SERVER_ERROR);
                    }
                    return false;
                };

                @Override
                public boolean run(int workerId, @NotNull RunStatus runStatus) {
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
            workerPool.assignThreadLocalCleaner(i, contextFactory::freeThreadLocal);
            workerPool.freeOnExit((QuietCloseable) () -> {
                Misc.free(jobContext);
                engine.getMessageBus().getQueryCacheEventFanOut().remove(queryCacheEventSubSeq);
                queryCacheEventSubSeq.clear();
            });
        }
    }

    @Override
    public void close() {
        Misc.free(dispatcher);
        Misc.free(registry);
    }

    public int getPort() {
        return dispatcher.getPort();
    }

    @TestOnly
    public WorkerPool getWorkerPool() {
        return workerPool;
    }

    public static class PGConnectionContextFactory extends IOContextFactoryImpl<PGConnectionContext> {

        public PGConnectionContextFactory(
                CairoEngine engine,
                PGWireConfiguration configuration,
                CircuitBreakerRegistry registry,
                ObjectFactory<SqlExecutionContextImpl> executionContextObjectFactory
        ) {
            super(() -> {
                NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(configuration.getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB5);
                PGConnectionContext pgConnectionContext = new PGConnectionContext(
                        engine,
                        configuration,
                        executionContextObjectFactory.newInstance(),
                        circuitBreaker
                );
                FactoryProvider factoryProvider = configuration.getFactoryProvider();
                NetworkFacade nf = configuration.getNetworkFacade();
                Authenticator authenticator = factoryProvider.getPgWireAuthenticatorFactory().getPgWireAuthenticator(nf, configuration, circuitBreaker, registry, pgConnectionContext);
                pgConnectionContext.setAuthenticator(authenticator);
                return pgConnectionContext;
            }, configuration.getConnectionPoolInitialCapacity());
        }
    }
}
