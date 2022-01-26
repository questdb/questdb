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

package io.questdb.cutlass.line.tcp;

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.EagerThreadSetup;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class LineTcpReceiver implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpReceiver.class);
    private final IODispatcher<LineTcpConnectionContext> dispatcher;
    private final LineTcpConnectionContextFactory contextFactory;
    private final LineTcpMeasurementScheduler scheduler;
    private final ObjList<WorkerPool> dedicatedPools;

    public LineTcpReceiver(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool ioWorkerPool,
            WorkerPool writerWorkerPool,
            ObjList<WorkerPool> dedicatedPools
    ) {
        this.contextFactory = new LineTcpConnectionContextFactory(lineConfiguration);
        this.dispatcher = IODispatchers.create(
                lineConfiguration.getDispatcherConfiguration(),
                contextFactory
        );
        this.dedicatedPools = dedicatedPools;
        ioWorkerPool.assign(dispatcher);
        scheduler = new LineTcpMeasurementScheduler(lineConfiguration, engine, ioWorkerPool, dispatcher, writerWorkerPool);

        final Closeable cleaner = contextFactory::closeContextPool;
        for (int i = 0, n = ioWorkerPool.getWorkerCount(); i < n; i++) {
            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            ioWorkerPool.assign(i, cleaner);
        }
    }

    @Nullable
    public static LineTcpReceiver create(
            LineTcpReceiverConfiguration lineConfiguration,
            WorkerPool sharedWorkerPool,
            Log log,
            CairoEngine cairoEngine
    ) {
        if (!lineConfiguration.isEnabled()) {
            return null;
        }

        ObjList<WorkerPool> dedicatedPools = new ObjList<>(2);
        WorkerPool ioWorkerPool = WorkerPoolAwareConfiguration.configureWorkerPool(lineConfiguration.getIOWorkerPoolConfiguration(), sharedWorkerPool);
        WorkerPool writerWorkerPool = WorkerPoolAwareConfiguration.configureWorkerPool(lineConfiguration.getWriterWorkerPoolConfiguration(), sharedWorkerPool);
        if (ioWorkerPool != sharedWorkerPool) {
            ioWorkerPool.assignCleaner(Path.CLEANER);
            dedicatedPools.add(ioWorkerPool);
        }
        if (writerWorkerPool != sharedWorkerPool) {
            writerWorkerPool.assignCleaner(Path.CLEANER);
            dedicatedPools.add(writerWorkerPool);
        }
        LineTcpReceiver lineTcpReceiver = new LineTcpReceiver(lineConfiguration, cairoEngine, ioWorkerPool, writerWorkerPool, dedicatedPools);
        if (ioWorkerPool != sharedWorkerPool) {
            ioWorkerPool.start(log);
        }
        if (writerWorkerPool != sharedWorkerPool) {
            writerWorkerPool.start(log);
        }
        return lineTcpReceiver;
    }

    @Override
    public void close() {
        for (int n = 0, sz = dedicatedPools.size(); n < sz; n++) {
            dedicatedPools.get(n).halt();
        }
        Misc.free(scheduler);
        Misc.free(contextFactory);
        Misc.free(dispatcher);
    }

    @TestOnly
    void setSchedulerListener(SchedulerListener listener) {
        scheduler.setListener(listener);
    }

    @FunctionalInterface
    public interface SchedulerListener {
        void onEvent(CharSequence tableName, int event);
    }

    private class LineTcpConnectionContextFactory implements IOContextFactory<LineTcpConnectionContext>, Closeable, EagerThreadSetup {
        private final ThreadLocal<WeakObjectPool<LineTcpConnectionContext>> contextPool;
        private boolean closed = false;

        public LineTcpConnectionContextFactory(LineTcpReceiverConfiguration configuration) {
            ObjectFactory<LineTcpConnectionContext> factory;
            if (null == configuration.getAuthDbPath()) {
                if (configuration.getAggressiveReadRetryCount() > 0) {
                    LOG.info().$("using aggressive context").$();
                    factory = () -> new AggressiveRecvLineTcpConnectionContext(configuration, scheduler);
                } else {
                    LOG.info().$("using default context").$();
                    factory = () -> new LineTcpConnectionContext(configuration, scheduler);
                }
            } else {
                LOG.info().$("using authenticating context").$();
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
