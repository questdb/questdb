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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public class LatestByCompiledFilter extends BooleanFunction implements UnaryFunction {
    public static final int BATCH_SIZE = 2048;
    private final DirectLongList auxAddresses;
    private final MemoryCARW bindVarMemory;
    private final DirectLongList dataAddresses;
    private final DirectLongList filteredRows;
    private ObjList<Function> bindVarFunctions;
    private CompiledFilter compiledFilter;
    private Function filter;

    public LatestByCompiledFilter(CairoConfiguration configuration, Function filter, CompiledFilter compiledFilter, ObjList<Function> bindVarFunctions) {
        this.filter = filter;
        this.compiledFilter = compiledFilter;
        this.bindVarFunctions = bindVarFunctions;
        bindVarMemory = Vm.getCARWInstance(configuration.getSqlJitBindVarsMemoryPageSize(),
                configuration.getSqlJitBindVarsMemoryMaxPages(), MemoryTag.NATIVE_JIT);
        filteredRows = new DirectLongList(Math.min(BATCH_SIZE, configuration.getPageFrameReduceRowIdListCapacity()), MemoryTag.NATIVE_OFFLOAD, true);
        dataAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD, true);
        auxAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD, true);
    }

    public static @Nullable DirectLongList apply(
            Function filter,
            PageFrameMemoryPool memoryPool,
            PageFrameAddressCache addressCache,
            int frameIndex,
            long rowLo,
            long rowHi
    ) {
        assert rowHi > rowLo && rowHi - rowLo <= BATCH_SIZE;
        if (filter instanceof LatestByCompiledFilter jit) {
            PageFrameMemory memory = memoryPool.navigateTo(frameIndex);
            if (!memory.hasColumnTops() && !memory.hasColumnTypeCasts()) {
                AsyncFilterUtils.applyCompiledFilter(jit.compiledFilter, jit.bindVarMemory, jit.bindVarFunctions,
                        memory, addressCache, jit.dataAddresses, jit.auxAddresses, jit.filteredRows, rowLo, rowHi - rowLo);
                return jit.filteredRows;
            }
        }
        return null;
    }

    static void addJitAttr(PlanSink sink, Function filter) {
        if (filter instanceof LatestByCompiledFilter) {
            sink.attr("jit").val(true);
        }
    }

    @Override
    public void close() {
        final Function filter = this.filter;
        this.filter = null;
        final CompiledFilter compiledFilter = this.compiledFilter;
        this.compiledFilter = null;
        final ObjList<Function> bindVarFunctions = this.bindVarFunctions;
        this.bindVarFunctions = null;
        Throwable failure = closeBuffers(null);
        failure = Misc.freeBestEffort(failure, compiledFilter);
        failure = Misc.freeObjListBestEffort(failure, bindVarFunctions);
        failure = Misc.freeBestEffort(failure, filter);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void cursorClosed() {
        if (filter != null) {
            filter.cursorClosed();
        }
    }

    @Override
    public Function getArg() {
        return filter;
    }

    @Override
    public boolean getBool(Record record) {
        return filter.getBool(record);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        filteredRows.reopen();
        dataAddresses.reopen();
        auxAddresses.reopen();
        filter.init(symbolTableSource, executionContext);
        Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
        AsyncFilterUtils.prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        filter.toPlan(sink);
    }

    private Throwable closeBuffers(Throwable failure) {
        failure = Misc.freeBestEffort(failure, filteredRows);
        failure = Misc.freeBestEffort(failure, dataAddresses);
        failure = Misc.freeBestEffort(failure, auxAddresses);
        return Misc.freeBestEffort(failure, bindVarMemory);
    }
}
