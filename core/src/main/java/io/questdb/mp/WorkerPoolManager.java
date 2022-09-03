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
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicReference;

public class WorkerPoolManager {

    private static final Log LOG = LogFactory.getLog(WorkerPoolManager.class);
    private static final AtomicReference<WorkerPool> SHARED = new AtomicReference<>();
    private static final CharSequenceObjHashMap<WorkerPool> DEDICATED = new CharSequenceObjHashMap<>(4);


    public static WorkerPool initSharedInstance(
            CairoEngine cairoEngine,
            WorkerPoolConfiguration config,
            FunctionFactoryCache functionFactoryCache,
            Metrics metrics
    ) throws SqlException {
        if (SHARED.get() != null) {
            throw new IllegalStateException("shared pool has already been initialized");
        }
        final WorkerPool sharedPool;
        SHARED.set(sharedPool = new WorkerPool(config, metrics).configure(cairoEngine, functionFactoryCache, true));
        return sharedPool;
    }

    public static WorkerPool getInstance(@NotNull WorkerPoolConfiguration config, @NotNull Metrics metrics, boolean setCleaner) {
        if (config.getWorkerCount() < 1) {
            WorkerPool pool = SHARED.get();
            if (pool != null) {
                LOG.info().$("Accessing pool [").$(config.getPoolName()).$("] -> SHARED").$();
                return pool;
            }
        }
        String poolName = config.getPoolName();
        WorkerPool pool = DEDICATED.get(poolName);
        if (pool == null) {
            DEDICATED.put(poolName, pool = new WorkerPool(config, metrics));
        }
        if (setCleaner) {
            pool.assignCleaner(Path.CLEANER);
        }
        LOG.info().$("Accessing pool [").$(poolName).$(", workers=").$(config.getWorkerCount()).$("] -> DEDICATED").$();
        return pool;
    }
}
