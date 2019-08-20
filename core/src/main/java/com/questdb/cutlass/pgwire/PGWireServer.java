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

package com.questdb.cutlass.pgwire;

import com.questdb.cairo.CairoEngine;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.Job;
import com.questdb.mp.WorkerPool;
import com.questdb.mp.WorkerPoolConfiguration;
import com.questdb.network.*;
import com.questdb.std.Misc;
import com.questdb.std.ThreadLocal;
import com.questdb.std.WeakObjectPool;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class PGWireServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(PGWireServer.class);
    private final IODispatcher<PGConnectionContext> dispatcher;
    private final PGConnectionContextFactory contextFactory;

    public PGWireServer(
            PGWireConfiguration configuration,
            CairoEngine engine,
            WorkerPool pool
    ) {
        this.contextFactory = new PGConnectionContextFactory(configuration);
        this.dispatcher = IODispatchers.create(
                configuration.getDispatcherConfiguration(),
                contextFactory
        );

        pool.assign(dispatcher);

        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            final PGJobContext jobContext = new PGJobContext(configuration, engine);
            pool.assign(i, new Job() {
                private final IORequestProcessor<PGConnectionContext> processor = (operation, context, dispatcher) -> {
                    try {
                        jobContext.handleClientOperation(context);
                        dispatcher.registerChannel(context, IOOperation.READ);
                    } catch (PeerIsSlowToWriteException e) {
                        dispatcher.registerChannel(context, IOOperation.READ);
                    } catch (PeerIsSlowToReadException e) {
                        dispatcher.registerChannel(context, IOOperation.WRITE);
                    } catch (PeerDisconnectedException | BadProtocolException e) {
                        dispatcher.disconnect(context);
                    }
                };

                @Override
                public boolean run() {
                    return dispatcher.processIOQueue(processor);
                }
            });

            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            pool.assign(i, () -> {
                Misc.free(jobContext);
                contextFactory.closeContextPool();
            });
        }
    }

    @Nullable
    public static PGWireServer create(PGWireConfiguration configuration, WorkerPool workerPool, Log log, CairoEngine cairoEngine) {
        PGWireServer pgWireServer;
        if (configuration.isEnabled()) {
            final WorkerPool localPool;
            if (configuration.getWorkerCount() > 0) {
                localPool = new WorkerPool(new WorkerPoolConfiguration() {
                    @Override
                    public int[] getWorkerAffinity() {
                        return configuration.getWorkerAffinity();
                    }

                    @Override
                    public int getWorkerCount() {
                        return configuration.getWorkerCount();
                    }

                    @Override
                    public boolean haltOnError() {
                        return configuration.workerHaltOnError();
                    }
                });
            } else {
                localPool = workerPool;
            }

            pgWireServer = new PGWireServer(configuration, cairoEngine, localPool);

            if (localPool != workerPool) {
                localPool.start(log);
            }
        } else {
            pgWireServer = null;
        }
        return pgWireServer;
    }

    @Override
    public void close() {
        Misc.free(contextFactory);
        Misc.free(dispatcher);
    }

    private static class PGConnectionContextFactory implements IOContextFactory<PGConnectionContext>, Closeable {
        private final ThreadLocal<WeakObjectPool<PGConnectionContext>> contextPool;
        private boolean closed = false;

        public PGConnectionContextFactory(PGWireConfiguration configuration) {
            this.contextPool = new ThreadLocal<>(() -> new WeakObjectPool<>(() ->
                    new PGConnectionContext(configuration), configuration.getConnectionPoolInitialCapacity()));
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public PGConnectionContext newInstance(long fd) {
            return contextPool.get().pop().of(fd);
        }

        @Override
        public void done(PGConnectionContext context) {
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
