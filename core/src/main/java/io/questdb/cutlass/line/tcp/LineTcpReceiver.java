/*+*****************************************************************************
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.AcceptGatedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IOContextFactoryImpl;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.std.Misc;
import io.questdb.std.ObjectFactory;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;


public class LineTcpReceiver implements Closeable {
    private final AtomicBoolean acceptOpen;
    private final IODispatcher<LineTcpConnectionContext> dispatcher;
    private LineTcpMeasurementScheduler scheduler;

    public LineTcpReceiver(
            LineTcpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool sharedPoolNetwork,
            WorkerPool sharedPoolWrite
    ) {
        this(configuration, engine, sharedPoolNetwork, sharedPoolWrite, new AtomicBoolean(true));
    }

    public LineTcpReceiver(
            LineTcpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool sharedPoolNetwork,
            WorkerPool sharedPoolWrite,
            AtomicBoolean acceptOpen
    ) {
        this.acceptOpen = acceptOpen;
        try {
            this.scheduler = null;
            ObjectFactory<LineTcpConnectionContext> factory;
            factory = () -> new LineTcpConnectionContext(configuration, scheduler);

            IOContextFactoryImpl<LineTcpConnectionContext> contextFactory = new IOContextFactoryImpl<>(
                    factory,
                    configuration.getConnectionPoolInitialCapacity()
            );
            this.dispatcher = IODispatchers.create(configuration, contextFactory);
            sharedPoolNetwork.assign(new AcceptGatedJob(dispatcher, acceptOpen));
            this.scheduler = new LineTcpMeasurementScheduler(configuration, engine, sharedPoolNetwork, dispatcher, sharedPoolWrite);

            for (int i = 0, n = sharedPoolNetwork.getWorkerCount(); i < n; i++) {
                // http context factory has thread local pools
                // therefore we need each thread to clean their thread locals individually
                sharedPoolNetwork.assignThreadLocalCleaner(i, contextFactory::freeThreadLocal);
            }
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public void close() {
        Misc.free(scheduler);
        Misc.free(dispatcher);
    }

}
