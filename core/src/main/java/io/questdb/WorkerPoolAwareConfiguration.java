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

package io.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.log.Log;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public interface WorkerPoolAwareConfiguration extends WorkerPoolConfiguration {

    static WorkerPool configureWorkerPool(
            WorkerPoolAwareConfiguration configuration,
            WorkerPool sharedPool
    ) {
        final WorkerPool localPool;
        if (configuration.getWorkerCount() > 0) {
            localPool = new WorkerPool(configuration);
        } else {
            localPool = sharedPool;
        }
        return localPool;
    }

    @Nullable
    static <T extends Closeable, C extends WorkerPoolAwareConfiguration> T create(
            C configuration,
            WorkerPool sharedWorkerPool,
            Log log,
            CairoEngine cairoEngine,
            ServerFactory<T, C> factory,
            MessageBus messageBus
    ) {
        final T server;
        if (configuration.isEnabled()) {

            final WorkerPool localPool = configureWorkerPool(configuration, sharedWorkerPool);
            final boolean local = localPool != sharedWorkerPool;
            final MessageBus bus;
            if (local) {
                bus = new MessageBusImpl();
            } else {
                bus = messageBus;
            }

            server = factory.create(configuration, cairoEngine, localPool, local, bus);

            if (local) {
                localPool.start(log);
            }

            return server;
        }
        return null;
    }

    boolean isEnabled();

    @FunctionalInterface
    interface ServerFactory<T extends Closeable, C> {
        T create(C configuration, CairoEngine engine, WorkerPool workerPool, boolean local, @Nullable MessageBus messageBus);
    }
}
