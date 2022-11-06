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

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.MutableIOContextFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;


public class LineTcpReceiver implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpReceiver.class);

    private final IODispatcher<LineTcpConnectionContext> dispatcher;
    private final MutableIOContextFactory<LineTcpConnectionContext> contextFactory;
    private LineTcpMeasurementScheduler scheduler;
    private final Metrics metrics;

    public LineTcpReceiver(
            LineTcpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool ioWorkerPool,
            WorkerPool writerWorkerPool
    ) {
        this.scheduler = null;
        this.metrics = engine.getMetrics();
        ObjectFactory<LineTcpConnectionContext> factory;
        if (null == configuration.getAuthDbPath()) {
            LOG.info().$("using default context").$();
            factory = () -> new LineTcpConnectionContext(configuration, scheduler, metrics);
        } else {
            LOG.info().$("using authenticating context").$();
            AuthDb authDb = new AuthDb(configuration);
            factory = () -> new LineTcpAuthConnectionContext(configuration, authDb, scheduler, metrics);
        }

        this.contextFactory = new MutableIOContextFactory<>(
                factory,
                configuration.getConnectionPoolInitialCapacity()
        );
        this.dispatcher = IODispatchers.create(
                configuration.getDispatcherConfiguration(),
                contextFactory
        );
        ioWorkerPool.assign(dispatcher);
        this.scheduler = new LineTcpMeasurementScheduler(configuration, engine, ioWorkerPool, dispatcher, writerWorkerPool);

        for (int i = 0, n = ioWorkerPool.getWorkerCount(); i < n; i++) {
            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            ioWorkerPool.assignThreadLocalCleaner(i, contextFactory::freeThreadLocal);
        }
    }

    @Override
    public void close() {
        Misc.free(scheduler);
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
}
