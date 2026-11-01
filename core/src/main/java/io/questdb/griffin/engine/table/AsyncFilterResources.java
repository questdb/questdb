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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Owns filter resources until a parallel factory takes them. Each take method transfers ownership;
 * closing this holder releases only the resources that have not been taken.
 */
public class AsyncFilterResources implements QuietCloseable {
    private final @Nullable IntHashSet filterUsedColumnIndexes;
    private @Nullable ObjList<Function> bindVarFunctions;
    private @Nullable MemoryCARW bindVarMemory;
    private @Nullable CompiledCountOnlyFilter compiledCountOnlyFilter;
    private @Nullable CompiledFilter compiledFilter;
    private @Nullable Function filter;
    private @Nullable ObjList<Function> perWorkerFilters;

    /**
     * No filter.
     */
    public AsyncFilterResources() {
        filterUsedColumnIndexes = null;
    }

    public AsyncFilterResources(
            @Nullable CompiledFilter compiledFilter,
            @Nullable CompiledCountOnlyFilter compiledCountOnlyFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters
    ) {
        this.compiledFilter = compiledFilter;
        this.compiledCountOnlyFilter = compiledCountOnlyFilter;
        this.bindVarMemory = bindVarMemory;
        this.bindVarFunctions = bindVarFunctions;
        this.filter = filter;
        this.filterUsedColumnIndexes = filterUsedColumnIndexes;
        this.perWorkerFilters = perWorkerFilters;
    }

    /**
     * Takes every resource {@code source} holds, which then frees nothing.
     */
    protected AsyncFilterResources(AsyncFilterResources source) {
        this(
                source.takeCompiledFilter(),
                source.takeCompiledCountOnlyFilter(),
                source.takeBindVarMemory(),
                source.takeBindVarFunctions(),
                source.takeFilter(),
                source.getFilterUsedColumnIndexes(),
                source.takePerWorkerFilters()
        );
    }

    @Override
    public void close() {
        CairoException.rethrowCleanupFailure(freeBestEffort(null));
    }

    public @Nullable Function getFilter() {
        return filter;
    }

    public @Nullable IntHashSet getFilterUsedColumnIndexes() {
        return filterUsedColumnIndexes;
    }

    public @Nullable ObjList<Function> getPerWorkerFilters() {
        return perWorkerFilters;
    }

    public @Nullable ObjList<Function> takeBindVarFunctions() {
        final ObjList<Function> value = bindVarFunctions;
        bindVarFunctions = null;
        return value;
    }

    public @Nullable MemoryCARW takeBindVarMemory() {
        final MemoryCARW value = bindVarMemory;
        bindVarMemory = null;
        return value;
    }

    public @Nullable CompiledCountOnlyFilter takeCompiledCountOnlyFilter() {
        final CompiledCountOnlyFilter value = compiledCountOnlyFilter;
        compiledCountOnlyFilter = null;
        return value;
    }

    public @Nullable CompiledFilter takeCompiledFilter() {
        final CompiledFilter value = compiledFilter;
        compiledFilter = null;
        return value;
    }

    public @Nullable Function takeFilter() {
        final Function value = filter;
        filter = null;
        return value;
    }

    public @Nullable ObjList<Function> takePerWorkerFilters() {
        final ObjList<Function> value = perWorkerFilters;
        perWorkerFilters = null;
        return value;
    }

    /**
     * Frees what nothing took, best effort, folding each failure into {@code failure}, which it returns.
     */
    protected Throwable freeBestEffort(Throwable failure) {
        failure = Misc.freeBestEffort(failure, compiledFilter);
        compiledFilter = null;
        failure = Misc.freeBestEffort(failure, compiledCountOnlyFilter);
        compiledCountOnlyFilter = null;
        failure = Misc.freeBestEffort(failure, bindVarMemory);
        bindVarMemory = null;
        failure = Misc.freeObjListBestEffort(failure, bindVarFunctions);
        bindVarFunctions = null;
        failure = Misc.freeBestEffort(failure, filter);
        filter = null;
        failure = Misc.freeObjListBestEffort(failure, perWorkerFilters);
        perWorkerFilters = null;
        return failure;
    }
}
