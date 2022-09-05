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
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class WorkerPoolManager {

    private static final Log LOG = LogFactory.getLog(WorkerPoolManager.class);
    private static final AtomicReference<WorkerPool> SHARED_POOL = new AtomicReference<>();
    private static final AtomicBoolean HAS_STARTED = new AtomicBoolean();
    private static final CharSequenceObjHashMap<WorkerPool> DEDICATED_POOLS = new CharSequenceObjHashMap<>(4);

    public static void setSharedInstance(WorkerPool sharedInstance) {
        SHARED_POOL.set(sharedInstance);
    }

    public static WorkerPool getInstance(@NotNull WorkerPoolConfiguration config, @NotNull Metrics metrics) {
        if (config.getWorkerCount() < 1) {
            WorkerPool pool = SHARED_POOL.get();
            if (pool != null) {
                LOG.info().$("Accessing pool [").$(config.getPoolName()).$("] -> SHARED").$();
                return pool;
            }
        }
        String poolName = config.getPoolName();
        WorkerPool pool = DEDICATED_POOLS.get(poolName);
        if (pool == null) {
            pool = new WorkerPool(config, metrics);
            pool.assignCleaner(Path.CLEANER);
            DEDICATED_POOLS.put(poolName, pool);
        }
        LOG.info().$("Accessing pool [").$(poolName).$(", workers=").$(config.getWorkerCount()).$("] -> DEDICATED").$();
        return pool;
    }

    public static void startAll() {
        startAll(null);
    }

    public static void startAll(Log sharedPoolLog) {
        if (HAS_STARTED.compareAndSet(false, true)) {
            WorkerPool sharedPool = SHARED_POOL.get();
            if (sharedPool != null) {
                sharedPool.start(sharedPoolLog);
            }
            LOG.info().$("Started shared pool").$();
            ObjList<CharSequence> poolNames = DEDICATED_POOLS.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.get(i);
                WorkerPool pool = DEDICATED_POOLS.get(name);
                pool.start();
                LOG.info().$("Started dedicated pool [").$(name).I$();
            }
        }
    }

    public static void closeAll() {
        if (HAS_STARTED.compareAndSet(true, false)) {
            ObjList<CharSequence> poolNames = DEDICATED_POOLS.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.get(i);
                WorkerPool pool = DEDICATED_POOLS.get(name);
                pool.close();
                DEDICATED_POOLS.remove(name);
                LOG.info().$("Closed dedicated pool [").$(name).I$();
            }
            WorkerPool sharedPool = SHARED_POOL.get();
            if (sharedPool != null) {
                sharedPool.close();
                SHARED_POOL.set(null);
                LOG.info().$("Closed shared pool").$();
            }
        }
    }

    public static WorkerPool createLoggerWorkerPool() {
        return createUnmanaged(new WorkerPoolConfiguration() {
            @Override
            public int[] getWorkerAffinity() {
                return new int[]{-1};
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean haltOnError() {
                return false;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }

            @Override
            public String getPoolName() {
                return "logging";
            }
        }, Metrics.disabled());
    }

    public static WorkerPool createUnmanaged(WorkerPoolConfiguration config, Metrics metrics) {
        return new WorkerPool(config, metrics);
    }

    private WorkerPoolManager() {
        throw new UnsupportedOperationException("not instantiatable");
    }
}
