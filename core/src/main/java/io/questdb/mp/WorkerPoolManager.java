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
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class WorkerPoolManager {

    private static final Log LOG = LogFactory.getLog(WorkerPoolManager.class);
    private static final AtomicReference<FunctionFactoryCache> FF_CACHE = new AtomicReference<>();
    private static final AtomicReference<WorkerPool> SHARED_POOL = new AtomicReference<>();
    private static final AtomicBoolean HAS_STARTED = new AtomicBoolean();
    private static final CharSequenceObjHashMap<WorkerPool> DEDICATED_POOLS = new CharSequenceObjHashMap<>(4);


    public static WorkerPool initSharedInstance(
            CairoEngine cairoEngine,
            WorkerPoolConfiguration config,
            Metrics metrics
    ) throws SqlException {
        if (SHARED_POOL.get() != null) {
            throw new IllegalStateException("shared pool has already been initialized");
        }
        final FunctionFactoryCache ffCache = new FunctionFactoryCache(
                cairoEngine.getConfiguration(),
                ServiceLoader.load(
                        FunctionFactory.class, FunctionFactory.class.getClassLoader()
                )
        );
        final WorkerPool sharedPool = new WorkerPool(config, metrics).configure(cairoEngine, ffCache, true);
        FF_CACHE.set(ffCache);
        SHARED_POOL.set(sharedPool);
        return sharedPool;
    }

    public static FunctionFactoryCache getFunctionFactoryCache() {
        FunctionFactoryCache ffCache = FF_CACHE.get();
        if (ffCache == null) {
            throw new IllegalStateException("shared pool has already been initialized");
        }
        return ffCache;
    }

    public static WorkerPool getInstance(@NotNull WorkerPoolConfiguration config, @NotNull Metrics metrics, boolean setCleaner) {
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
            if (setCleaner) {
                pool.assignCleaner(Path.CLEANER);
            }
            DEDICATED_POOLS.put(poolName, pool);
        }
        LOG.info().$("Accessing pool [").$(poolName).$(", workers=").$(config.getWorkerCount()).$("] -> DEDICATED").$();
        return pool;
    }

    public static void startAll(Log sharedPoolLog) {
        WorkerPool sharedPool = SHARED_POOL.get();
        if (sharedPool == null) {
            throw new IllegalStateException("shared pool has already been initialized");
        }
        if (HAS_STARTED.compareAndSet(false, true)) {
            sharedPool.start(sharedPoolLog);
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
        WorkerPool sharedPool = SHARED_POOL.get();
        if (sharedPool == null) {
            throw new IllegalStateException("shared pool has already been initialized");
        }
        if (HAS_STARTED.compareAndSet(true, false)) {
            ObjList<CharSequence> poolNames = DEDICATED_POOLS.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.get(i);
                WorkerPool pool = DEDICATED_POOLS.get(name);
                pool.close();
                LOG.info().$("Closed dedicated pool [").$(name).I$();
            }
            sharedPool.close();
            LOG.info().$("Closed shared pool").$();
        }
    }

    private WorkerPoolManager() {
        throw new UnsupportedOperationException("not instantiatable");
    }
}
