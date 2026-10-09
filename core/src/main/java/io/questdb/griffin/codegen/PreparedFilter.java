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

package io.questdb.griffin.codegen;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.io.Closeable;

/**
 * The filter the generator prepares, without building a filter factory, for the parallel consumer that steals it
 * from a filter node: the filter function over the factory under the node, and, once
 * {@link FilterFactoryGenerator#prepareParallel} ran, the column set it reads, its per-worker clones and its compiled
 * JIT filter with the bind variables it reads. It owns every resource it holds until the consumer's constructor adopts
 * them, see {@link #adopt()}; {@link #close()} frees what it still holds.
 */
final class PreparedFilter implements Closeable {
    private ObjList<Function> bindVarFunctions;
    private MemoryCARW bindVarMemory;
    private IntHashSet columns;
    private CompiledCountOnlyFilter compiledCountOnlyFilter;
    private CompiledFilter compiledFilter;
    private Function filter;
    private OutputSchema input;
    private boolean isJitAllowed;
    private BoundExpression predicate;
    private ObjList<Function> workers;

    @Override
    public void close() {
        Throwable failure = Misc.freeObjListBestEffort(null, workers);
        failure = Misc.freeBestEffort(failure, compiledFilter);
        failure = Misc.freeBestEffort(failure, compiledCountOnlyFilter);
        failure = Misc.freeBestEffort(failure, bindVarMemory);
        failure = Misc.freeObjListBestEffort(failure, bindVarFunctions);
        failure = Misc.freeBestEffort(failure, filter);
        adopt();
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Hands every resource to the constructor that consumes them, including on its failure.
     */
    void adopt() {
        bindVarFunctions = null;
        bindVarMemory = null;
        columns = null;
        compiledCountOnlyFilter = null;
        compiledFilter = null;
        filter = null;
        input = null;
        isJitAllowed = false;
        predicate = null;
        workers = null;
    }

    ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    IntHashSet getColumns() {
        return columns;
    }

    CompiledCountOnlyFilter getCompiledCountOnlyFilter() {
        return compiledCountOnlyFilter;
    }

    CompiledFilter getCompiledFilter() {
        return compiledFilter;
    }

    Function getFilter() {
        return filter;
    }

    OutputSchema getInput() {
        return input;
    }

    BoundExpression getPredicate() {
        return predicate;
    }

    ObjList<Function> getWorkers() {
        return workers;
    }

    boolean isJitAllowed() {
        return isJitAllowed;
    }

    /**
     * Takes the filter function, which the holder owns from here on.
     */
    void of(BoundExpression predicate, OutputSchema input, Function filter, boolean isJitAllowed) {
        assert this.filter == null;
        this.predicate = predicate;
        this.input = input;
        this.filter = filter;
        this.isJitAllowed = isJitAllowed;
    }

    void setBindVarMemory(MemoryCARW bindVarMemory) {
        this.bindVarMemory = bindVarMemory;
    }

    void setColumns(IntHashSet columns) {
        this.columns = columns;
    }

    void setJit(CompiledFilter compiledFilter, CompiledCountOnlyFilter compiledCountOnlyFilter, ObjList<Function> bindVarFunctions) {
        this.compiledFilter = compiledFilter;
        this.compiledCountOnlyFilter = compiledCountOnlyFilter;
        this.bindVarFunctions = bindVarFunctions;
    }

    void setWorkers(ObjList<Function> workers) {
        this.workers = workers;
    }
}
