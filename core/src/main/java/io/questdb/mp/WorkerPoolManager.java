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

package io.questdb.mp;

import io.questdb.Metrics;
import io.questdb.ServerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class WorkerPoolManager {

    private static final Log LOG = LogFactory.getLog(WorkerPoolManager.class);

    private final WorkerPool sharedPool;
    private final AtomicBoolean hasStarted = new AtomicBoolean();
    private final CharSequenceObjHashMap<WorkerPool> dedicatedPools = new CharSequenceObjHashMap<>(4);

    public WorkerPoolManager(ServerConfiguration config, Metrics metrics) throws SqlException {
        sharedPool = new WorkerPool(config.getWorkerPoolConfiguration(), metrics);
        configureSharedPool(sharedPool);
    }

    protected abstract void configureSharedPool(final WorkerPool sharedPool) throws SqlException;

    public WorkerPool getSharedPool() {
        return sharedPool;
    }

    public int getSharedWorkerCount() {
        return sharedPool.getWorkerCount();
    }

    public WorkerPool getInstance(@NotNull WorkerPoolConfiguration config, @NotNull Metrics metrics) {
        if (hasStarted.get()) {
            throw new IllegalStateException("can only get instance before start");
        }

        if (config.getWorkerCount() < 1) {
            LOG.info().$("Using pool [name=").$(sharedPool.getPoolName())
                    .$(", workers=").$(sharedPool.getWorkerCount())
                    .$("] -> SHARED")
                    .$();
            return sharedPool;
        }

        String poolName = config.getPoolName();
        WorkerPool pool = dedicatedPools.get(poolName);
        if (pool == null) {
            pool = new WorkerPool(config, metrics);
            pool.assignCleaner(Path.CLEANER);
            dedicatedPools.put(poolName, pool);
        }
        LOG.info().$("Using pool [name=").$(poolName)
                .$(", workers=").$(pool.getWorkerCount())
                .$("] -> DEDICATED")
                .$();
        return pool;
    }

    public void startAll(Log sharedPoolLog) {
        if (hasStarted.compareAndSet(false, true)) {
            sharedPool.start(sharedPoolLog);
            LOG.info().$("Started shared pool [name=").$(sharedPool.getPoolName())
                    .$(", workers=").$(sharedPool.getWorkerCount())
                    .I$();

            ObjList<CharSequence> poolNames = dedicatedPools.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.get(i);
                WorkerPool pool = dedicatedPools.get(name);
                pool.start(sharedPoolLog);
                LOG.info().$("Started dedicated pool [name=").$(name)
                        .$(", workers=").$(pool.getWorkerCount())
                        .I$();
            }
        }
    }

    public void closeAll() {
        if (hasStarted.compareAndSet(true, false)) {
            ObjList<CharSequence> poolNames = dedicatedPools.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.getQuick(i);
                WorkerPool pool = dedicatedPools.get(name);
                LOG.info().$("Closing dedicated pool [name=").$(name)
                        .$(", workers=").$(pool.getWorkerCount())
                        .I$();
                pool.close();
            }
            dedicatedPools.clear();

            LOG.info().$("Closing shared pool [name=").$(sharedPool.getPoolName())
                    .$(", workers=").$(sharedPool.getWorkerCount())
                    .I$();
            sharedPool.close();
        }
    }
}
