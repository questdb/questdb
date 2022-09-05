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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerPoolManager {
    private static final Log LOG = LogFactory.getLog(WorkerPoolManager.class);
    private WorkerPool sharedPool;
    private final AtomicBoolean hasStarted = new AtomicBoolean();
    private final CharSequenceObjHashMap<WorkerPool> dedicatedPools = new CharSequenceObjHashMap<>(4);

    public WorkerPoolManager() {
        this.sharedPool = null;
    }

    public WorkerPoolManager(WorkerPool sharedPool) {
        this.sharedPool = sharedPool;
    }

    public void setSharedPool(WorkerPool sharedPool) {
        this.sharedPool = sharedPool;
    }

    public boolean hasSharedPool() {
        return sharedPool != null;
    }

    public int getSharedWorkerCount() {
        return sharedPool != null ? sharedPool.getWorkerCount() : -1;
    }

    public WorkerPool getInstance(@NotNull WorkerPoolConfiguration config, @NotNull Metrics metrics) {
        if (config.getWorkerCount() < 1) {
            WorkerPool pool = sharedPool;
            if (pool != null) {
                LOG.info().$("Accessing pool [").$(config.getPoolName()).$("] -> SHARED").$();
                return pool;
            }
        }
        String poolName = config.getPoolName();
        WorkerPool pool = dedicatedPools.get(poolName);
        if (pool == null) {
            pool = new WorkerPool(config, metrics);
            pool.assignCleaner(Path.CLEANER);
            dedicatedPools.put(poolName, pool);
        }
        LOG.info().$("Accessing pool [").$(poolName).$(", workers=").$(config.getWorkerCount()).$("] -> DEDICATED").$();
        return pool;
    }

    public void startAll() {
        startAll(null);
    }

    public void startAll(Log sharedPoolLog) {
        if (hasStarted.compareAndSet(false, true)) {
            if (sharedPool != null) {
                sharedPool.start(sharedPoolLog);
            }
            LOG.info().$("Started shared pool").$();
            ObjList<CharSequence> poolNames = dedicatedPools.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.get(i);
                WorkerPool pool = dedicatedPools.get(name);
                pool.start(sharedPoolLog);
                LOG.info().$("Started dedicated pool [").$(name).I$();
            }
        }
    }

    public void closeAll() {
        if (hasStarted.compareAndSet(true, false)) {
            ObjList<CharSequence> poolNames = dedicatedPools.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.getQuick(i);
                WorkerPool pool = dedicatedPools.get(name);
                pool.close();
                LOG.info().$("Closed dedicated pool [").$(name).I$();
            }
            dedicatedPools.clear();
            if (sharedPool != null) {
                sharedPool.close();
                sharedPool = null;
                LOG.info().$("Closed shared pool").$();
            }
        }
    }

    public static WorkerPool createLoggerWorkerPool() {
        return new WorkerPool(new WorkerPoolConfiguration() {
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
}
