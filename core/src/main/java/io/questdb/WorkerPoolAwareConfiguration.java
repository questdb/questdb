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

package io.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;


public interface WorkerPoolAwareConfiguration extends WorkerPoolConfiguration {
    WorkerPoolAwareConfiguration USE_SHARED_CONFIGURATION = new WorkerPoolAwareConfiguration() {
        @Override
        public int[] getWorkerAffinity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getWorkerCount() {
            return 0;
        }

        @Override
        public boolean haltOnError() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEnabled() {
            return true;
        }
    };

    static WorkerPool configureWorkerPool(
            WorkerPoolAwareConfiguration configuration,
            WorkerPool sharedPool,
            Metrics metrics
    ) {
        WorkerPool pool = configuration.getWorkerCount() > 0 || sharedPool == null ? new WorkerPool(configuration, metrics) : sharedPool;
        pool.assignCleaner(Path.CLEANER);
        return pool;
    }

    @Nullable
    static <T extends Lifecycle, C extends WorkerPoolAwareConfiguration> T create(
            C configuration,
            WorkerPool sharedPool,
            CairoEngine cairoEngine,
            ServerFactory<T, C> factory,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics
    ) {
        final T server;
        if (configuration.isEnabled()) {
            final WorkerPool localPool = configureWorkerPool(configuration, sharedPool, metrics);
            final boolean local = localPool != sharedPool;
            final int sharedWorkerCount = sharedPool == null ? localPool.getWorkerCount() : sharedPool.getWorkerCount();
            server = factory.create(
                    configuration,
                    cairoEngine,
                    localPool,
                    local,
                    sharedWorkerCount,
                    functionFactoryCache,
                    snapshotAgent,
                    metrics
            );
            return server;
        }
        return null;
    }

    boolean isEnabled();

    @FunctionalInterface
    interface ServerFactory<T extends Lifecycle, C> {
        T create(
                C configuration,
                CairoEngine engine,
                WorkerPool workerPool,
                boolean local,
                int sharedWorkerCount,
                @Nullable FunctionFactoryCache functionFactoryCache,
                @Nullable DatabaseSnapshotAgent snapshotAgent,
                Metrics metrics
        );
    }
}
