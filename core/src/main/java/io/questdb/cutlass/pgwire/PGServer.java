/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IOContextFactoryImpl;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.std.AssociativeCache;
import io.questdb.std.ConcurrentAssociativeCache;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NoOpAssociativeCache;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.network.IODispatcher.*;

public class PGServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(PGServer.class);
    private static final NoOpAssociativeCache<TypesAndSelect> NO_OP_CACHE = new NoOpAssociativeCache<>();
    private final PGConnectionContextFactory contextFactory;
    private final IODispatcher<PGConnectionContext> dispatcher;
    private final Metrics metrics;
    private final PGCircuitBreakerRegistry registry;
    private final WorkerPool sharedPoolNetwork;
    private final AssociativeCache<TypesAndSelect> typesAndSelectCache;

    public PGServer(
            PGConfiguration configuration,
            CairoEngine engine,
            WorkerPool sharedPoolNetwork,
            PGCircuitBreakerRegistry registry,
            ObjectFactory<SqlExecutionContextImpl> executionContextObjectFactory
    ) {
        this.metrics = engine.getMetrics();
        if (configuration.isSelectCacheEnabled()) {
            this.typesAndSelectCache = new ConcurrentAssociativeCache<>(configuration.getConcurrentCacheConfiguration());
        } else {
            this.typesAndSelectCache = NO_OP_CACHE;
        }
        this.contextFactory = new PGConnectionContextFactory(
                engine,
                configuration,
                registry,
                executionContextObjectFactory,
                typesAndSelectCache
        );
        this.dispatcher = IODispatchers.create(configuration, contextFactory);
        this.sharedPoolNetwork = sharedPoolNetwork;
        this.registry = registry;

        sharedPoolNetwork.assign(dispatcher);

        for (int i = 0, n = sharedPoolNetwork.getWorkerCount(); i < n; i++) {
            sharedPoolNetwork.assign(i, new Job() {
                private final IORequestProcessor<PGConnectionContext> processor = (operation, context, dispatcher) -> {
                    try {
                        if (operation == IOOperation.HEARTBEAT) {
                            dispatcher.registerChannel(context, IOOperation.HEARTBEAT);
                            return false;
                        }
                        context.handleClientOperation(operation);
                        dispatcher.registerChannel(context, IOOperation.READ);
                        return true;
                    } catch (PeerIsSlowToWriteException e) {
                        dispatcher.registerChannel(context, IOOperation.READ);
                    } catch (PeerIsSlowToReadException e) {
                        dispatcher.registerChannel(context, IOOperation.WRITE);
                    } catch (PeerDisconnectedException e) {
                        dispatcher.disconnect(
                                context,
                                operation == IOOperation.READ
                                        ? DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV
                                        : DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND
                        );
                    } catch (PGMessageProcessingException e) {
                        LOG.error().$("protocol issue [err: `").$safe(e.getFlyweightMessage()).$("`]").$();
                        dispatcher.disconnect(context, DISCONNECT_REASON_PROTOCOL_VIOLATION);
                    } catch (Throwable e) { // must remain last in catch list!
                        LOG.critical().$("internal error [ex=").$(e).$(']').$();
                        // This is a critical error, so we treat it as an unhandled one.
                        metrics.healthMetrics().incrementUnhandledErrors();
                        dispatcher.disconnect(context, DISCONNECT_REASON_SERVER_ERROR);
                    }
                    return false;
                };

                @Override
                public boolean run(int workerId, @NotNull RunStatus runStatus) {
                    return dispatcher.processIOQueue(processor);
                }
            });

            // context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            sharedPoolNetwork.assignThreadLocalCleaner(i, contextFactory::freeThreadLocal);
        }
    }

    public void clearSelectCache() {
        typesAndSelectCache.clear();
    }

    @Override
    public void close() {
        Misc.free(dispatcher);
        Misc.free(registry);
        Misc.free(contextFactory);
        Misc.free(typesAndSelectCache);
    }

    public int getPort() {
        return dispatcher.getPort();
    }

    @TestOnly
    public WorkerPool getWorkerPool() {
        return sharedPoolNetwork;
    }

    public boolean isListening() {
        return dispatcher.isListening();
    }

    public void resetQueryCache() {
        if (typesAndSelectCache != null) {
            typesAndSelectCache.clear();
        }
    }

    private static class PGConnectionContextFactory extends IOContextFactoryImpl<PGConnectionContext> {

        public PGConnectionContextFactory(
                CairoEngine engine,
                PGConfiguration configuration,
                PGCircuitBreakerRegistry registry,
                ObjectFactory<SqlExecutionContextImpl> executionContextObjectFactory,
                AssociativeCache<TypesAndSelect> typesAndSelectCache
        ) {
            super(
                    () -> {
                        NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                                engine,
                                configuration.getCircuitBreakerConfiguration(),
                                MemoryTag.NATIVE_CB5
                        );
                        PGConnectionContext pgConnectionContext = new PGConnectionContext(
                                engine,
                                configuration,
                                executionContextObjectFactory.newInstance(),
                                circuitBreaker,
                                typesAndSelectCache
                        );
                        FactoryProvider factoryProvider = configuration.getFactoryProvider();
                        SocketAuthenticator authenticator = factoryProvider.getPgWireAuthenticatorFactory().getPgWireAuthenticator(
                                configuration,
                                circuitBreaker,
                                registry,
                                pgConnectionContext
                        );
                        pgConnectionContext.setAuthenticator(authenticator);
                        return pgConnectionContext;
                    },
                    configuration.getConnectionPoolInitialCapacity()
            );
        }
    }
}
