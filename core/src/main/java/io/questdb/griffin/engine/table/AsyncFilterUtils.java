/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Utils for filtering page frames.
 *
 * @see io.questdb.cairo.sql.async.PageFrameSequence
 */
public class AsyncFilterUtils {

    private AsyncFilterUtils() {
    }

    public static void applyCompiledFilter(
            @NotNull CompiledFilter compiledFilter,
            @NotNull MemoryCARW bindVarMemory,
            @NotNull ObjList<Function> bindVarFunctions,
            @NotNull PageFrameReduceTask task
    ) {
        applyCompiledFilter(null, compiledFilter, bindVarMemory, bindVarFunctions, task);
    }

    public static void applyCompiledFilter(
            @Nullable PageFrameMemory frameMemory,
            @NotNull CompiledFilter compiledFilter,
            @NotNull MemoryCARW bindVarMemory,
            @NotNull ObjList<Function> bindVarFunctions,
            @NotNull PageFrameReduceTask task
    ) {
        if (frameMemory == null) {
            task.populateJitData();
        } else {
            task.populateJitData(frameMemory);
        }
        final DirectLongList data = task.getDataAddresses();
        final DirectLongList varSizeAux = task.getAuxAddresses();
        final DirectLongList rows = task.getFilteredRows();
        long hi = compiledFilter.call(
                data.getAddress(),
                data.size(),
                varSizeAux.getAddress(),
                bindVarMemory.getAddress(),
                bindVarFunctions.size(),
                rows.getAddress(),
                task.getFrameRowCount(),
                0
        );
        rows.setPos(hi);
    }

    public static void applyFilter(
            @NotNull Function filter,
            @NotNull DirectLongList rows,
            @NotNull PageFrameMemoryRecord record,
            long frameRowCount
    ) {
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            if (filter.getBool(record)) {
                rows.add(r);
            }
        }
    }
}
