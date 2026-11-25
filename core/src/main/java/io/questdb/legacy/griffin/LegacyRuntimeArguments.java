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


package io.questdb.legacy.griffin;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableDescriptor;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.LimitBounds;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.table.AsyncFilterResources;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * The arguments the shared runtime takes and master's planner did not compute, derived from what it does compute.
 */
public final class LegacyRuntimeArguments {

    private LegacyRuntimeArguments() {
    }

    /**
     * The column of the batch argument of each aggregate of a vectorized window join, null when the join is not
     * vectorized. Master deduplicated the arguments through {@code Function.isEquivalentTo()}, which the shared
     * runtime no longer has, so every aggregate gets a column of its own.
     */
    public static @Nullable IntList argumentColumns(boolean isVectorized, ObjList<GroupByFunction> groupByFunctions) {
        if (!isVectorized) {
            return null;
        }
        final int functionCount = groupByFunctions.size();
        final IntList columns = new IntList(functionCount);
        for (int i = 0; i < functionCount; i++) {
            columns.add(i);
        }
        return columns;
    }

    /**
     * The filter of an async factory, from the parts master's factories took one by one, which the factory takes.
     */
    public static AsyncFilterResources filterResources(
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters
    ) {
        return new AsyncFilterResources(compiledFilter, null, bindVarMemory, bindVarFunctions, filter, filterUsedColumnIndexes, perWorkerFilters);
    }

    /**
     * The bounds of a LIMIT whose functions master's factories took apart, null without either, which takes the
     * functions.
     */
    public static @Nullable LimitBounds limitBounds(@Nullable Function lo, @Nullable Function hi, int position) {
        if (lo == null) {
            // master defaults a missing lo bound to 0, so only a LIMIT with neither bound comes without one
            assert hi == null;
            return null;
        }
        return new LimitBounds(lo, hi, position);
    }

    /**
     * The descriptor of the table whose reader metadata {@code tableMetadata} is, or a copy of it.
     */
    public static TableDescriptor tableDescriptor(RecordMetadata tableMetadata) {
        final int columnCount = tableMetadata.getColumnCount();
        final ObjList<String> columnNames = new ObjList<>(columnCount);
        boolean hasConvertedColumns = false;
        for (int i = 0; i < columnCount; i++) {
            columnNames.add(tableMetadata.getColumnName(i));
            hasConvertedColumns |= tableMetadata.getColumnMetadata(i).isTypeConverted();
        }
        return new TableDescriptor(columnNames, tableMetadata.getTimestampIndex(), tableMetadata.getTimestampType(), hasConvertedColumns);
    }
}
