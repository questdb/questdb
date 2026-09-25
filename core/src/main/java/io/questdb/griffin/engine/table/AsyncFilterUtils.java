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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
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
        // Each bind-variable slot is 16 bytes wide so UUIDs (i128) fit
        // alongside narrower types in the same fixed-stride layout that
        // the JIT-compiled filter expects (see read_vars_mem in jit/*.h).
        // Smaller types occupy the first 8 bytes; the second 8 bytes are
        // padding.
        final int columnType = function.getType();
        // Every arm writes the slot's leading 8 bytes and yields whether the padding word follows;
        // UUID fills all 16 bytes itself.
        final boolean isPadded = switch (ColumnTypeTag.of(columnType)) {
            case BOOLEAN -> {
                bindVarMemory.putLong(function.getBool(null) ? 1 : 0);
                yield true;
            }
            case BYTE -> {
                bindVarMemory.putLong(function.getByte(null));
                yield true;
            }
            case GEOBYTE -> {
                bindVarMemory.putLong(function.getGeoByte(null));
                yield true;
            }
            case SHORT -> {
                bindVarMemory.putLong(function.getShort(null));
                yield true;
            }
            case GEOSHORT -> {
                bindVarMemory.putLong(function.getGeoShort(null));
                yield true;
            }
            case CHAR -> {
                bindVarMemory.putLong(function.getChar(null));
                yield true;
            }
            case INT -> {
                bindVarMemory.putLong(function.getInt(null));
                yield true;
            }
            case IPv4 -> {
                bindVarMemory.putLong(function.getIPv4(null));
                yield true;
            }
            case GEOINT -> {
                bindVarMemory.putLong(function.getGeoInt(null));
                yield true;
            }
            case SYMBOL -> {
                assert function instanceof CompiledFilterSymbolBindVariable;
                function.init(symbolTableSource, executionContext);
                bindVarMemory.putLong(function.getInt(null));
                yield true;
            }
            case FLOAT -> {
                bindVarMemory.putFloat(function.getFloat(null));
                bindVarMemory.putFloat(Float.NaN);
                yield true;
            }
            case LONG -> {
                bindVarMemory.putLong(function.getLong(null));
                yield true;
            }
            case GEOLONG -> {
                bindVarMemory.putLong(function.getGeoLong(null));
                yield true;
            }
            case DATE -> {
                bindVarMemory.putLong(function.getDate(null));
                yield true;
            }
            case TIMESTAMP -> {
                bindVarMemory.putLong(function.getTimestamp(null));
                yield true;
            }
            case DOUBLE -> {
                bindVarMemory.putDouble(function.getDouble(null));
                yield true;
            }
            case UUID -> {
                bindVarMemory.putLong128(function.getLong128Lo(null), function.getLong128Hi(null));
                yield false;
            }
            case UNDEFINED, STRING, LONG256, BINARY, CURSOR, VAR_ARG, RECORD, GEOHASH, LONG128, VARCHAR, ARRAY,
                 DECIMAL8,
                 DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL, REGCLASS, REGPROCEDURE, ARRAY_STRING,
                 PARAMETER, INTERVAL, VARCHAR_SLICE, NULL, UNKNOWN ->
                    throw SqlException.position(0).put("unsupported bind variable type: ").put(ColumnType.nameOf(ColumnType.tagOf(columnType)));
        };
        if (isPadded) {
            // Pad every non-UUID slot to a fixed 16-byte stride.
            bindVarMemory.putLong(0L);
        }
    }
}
