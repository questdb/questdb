/*******************************************************************************
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.CompiledFilterSymbolBindVariable;
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
                task.getFrameRowCount()
        );
        rows.setPos(hi);
    }

    public static void applyCompiledFilter(
            @NotNull CompiledFilter compiledFilter,
            @NotNull MemoryCARW bindVarMemory,
            @NotNull ObjList<Function> bindVarFunctions,
            @NotNull PageFrameMemory frameMemory,
            @NotNull PageFrameAddressCache pageAddressCache,
            @NotNull DirectLongList dataAddresses,
            @NotNull DirectLongList auxAddresses,
            @NotNull DirectLongList filteredRows,
            long frameRowCount
    ) {
        PageFrameReduceTask.populateJitAddresses(frameMemory, pageAddressCache, dataAddresses, auxAddresses);

        if (filteredRows.getCapacity() < frameRowCount) {
            filteredRows.setCapacity(frameRowCount);
        }

        long hi = compiledFilter.call(
                dataAddresses.getAddress(),
                dataAddresses.size(),
                auxAddresses.getAddress(),
                bindVarMemory.getAddress(),
                bindVarFunctions.size(),
                filteredRows.getAddress(),
                frameRowCount
        );
        filteredRows.setPos(hi);
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

    public static void prepareBindVarMemory(
            SqlExecutionContext executionContext,
            SymbolTableSource symbolTableSource,
            ObjList<Function> bindVarFunctions,
            MemoryCARW bindVarMemory
    ) throws SqlException {
        // don't trigger memory allocation if there are no variables
        if (bindVarFunctions.size() > 0) {
            bindVarMemory.truncate();
            for (int i = 0, n = bindVarFunctions.size(); i < n; i++) {
                Function function = bindVarFunctions.getQuick(i);
                writeBindVarFunction(bindVarMemory, function, symbolTableSource, executionContext);
            }
        }
    }

    private static void writeBindVarFunction(
            MemoryCARW bindVarMemory,
            Function function,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final int columnType = function.getType();
        final int columnTypeTag = ColumnType.tagOf(columnType);
        switch (columnTypeTag) {
            case ColumnType.BOOLEAN:
                bindVarMemory.putLong(function.getBool(null) ? 1 : 0);
                return;
            case ColumnType.BYTE:
                bindVarMemory.putLong(function.getByte(null));
                return;
            case ColumnType.GEOBYTE:
                bindVarMemory.putLong(function.getGeoByte(null));
                return;
            case ColumnType.SHORT:
                bindVarMemory.putLong(function.getShort(null));
                return;
            case ColumnType.GEOSHORT:
                bindVarMemory.putLong(function.getGeoShort(null));
                return;
            case ColumnType.CHAR:
                bindVarMemory.putLong(function.getChar(null));
                return;
            case ColumnType.INT:
                bindVarMemory.putLong(function.getInt(null));
                return;
            case ColumnType.IPv4:
                bindVarMemory.putLong(function.getIPv4(null));
                return;
            case ColumnType.GEOINT:
                bindVarMemory.putLong(function.getGeoInt(null));
                return;
            case ColumnType.SYMBOL:
                assert function instanceof CompiledFilterSymbolBindVariable;
                function.init(symbolTableSource, executionContext);
                bindVarMemory.putLong(function.getInt(null));
                return;
            case ColumnType.FLOAT:
                // compiled filter function will read only the first word
                bindVarMemory.putFloat(function.getFloat(null));
                bindVarMemory.putFloat(Float.NaN);
                return;
            case ColumnType.LONG:
                bindVarMemory.putLong(function.getLong(null));
                return;
            case ColumnType.GEOLONG:
                bindVarMemory.putLong(function.getGeoLong(null));
                return;
            case ColumnType.DATE:
                bindVarMemory.putLong(function.getDate(null));
                return;
            case ColumnType.TIMESTAMP:
                bindVarMemory.putLong(function.getTimestamp(null));
                return;
            case ColumnType.DOUBLE:
                bindVarMemory.putDouble(function.getDouble(null));
                return;
            case ColumnType.UUID:
                long lo = function.getLong128Lo(null);
                long hi = function.getLong128Hi(null);
                bindVarMemory.putLong128(lo, hi);
                return;
            default:
                throw SqlException.position(0).put("unsupported bind variable type: ").put(ColumnType.nameOf(columnTypeTag));
        }
    }
}
