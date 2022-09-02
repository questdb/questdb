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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class AsyncFilterAtom implements StatefulAtom, Closeable {

    private final Function filter;
    private final ObjList<Function> perWorkerFilters;
    private final AtomicIntegerArray perWorkerLocks;
    // Used to randomize acquire attempts for work stealing threads. Accessed in a racy way, intentionally.
    private final Rnd rnd = new Rnd();

    public AsyncFilterAtom(
            @NotNull Function filter,
            @Nullable ObjList<Function> perWorkerFilters
    ) {
        this.filter = filter;
        this.perWorkerFilters = perWorkerFilters;
        if (perWorkerFilters != null) {
            perWorkerLocks = new AtomicIntegerArray(perWorkerFilters.size());
        } else {
            perWorkerLocks = null;
        }
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        filter.init(symbolTableSource, executionContext);
        if (perWorkerFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerFilters, symbolTableSource, executionContext);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
    }

    @Override
    public void close() {
        Misc.free(filter);
        Misc.freeObjList(perWorkerFilters);
    }

    public int acquireFilter(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (perWorkerFilters == null) {
            return -1;
        }
        if (workerId == -1 && owner) {
            // Owner thread is free to use the original filter anytime.
            return -1;
        }
        final int size = perWorkerFilters.size();
        workerId = workerId == -1 ? rnd.nextInt(size) : workerId;
        while (true) {
            for (int i = 0; i < size; i++) {
                int id = (i + workerId) % size;
                if (perWorkerLocks.compareAndSet(id, 0, 1)) {
                    return id;
                }
            }
            circuitBreaker.statefulThrowExceptionIfTripped();
            Os.pause();
        }
    }

    public Function getFilter(int filterId) {
        if (filterId == -1) {
            return filter;
        }
        assert perWorkerFilters != null;
        return perWorkerFilters.getQuick(filterId);
    }

    public void releaseFilter(int filterId) {
        if (filterId == -1) {
            return;
        }
        perWorkerLocks.set(filterId, 0);
    }
}
