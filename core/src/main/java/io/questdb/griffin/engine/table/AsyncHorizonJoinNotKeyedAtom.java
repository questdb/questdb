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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Atom for non-keyed HORIZON JOIN GROUP BY that uses SimpleMapValue for aggregation.
 * <p>
 * This class extends {@link BaseAsyncHorizonJoinAtom} and adds:
 * - Per-worker SimpleMapValue instances (single aggregation slot, no keys)
 * <p>
 * Used when there are no GROUP BY keys, producing a single output row.
 */
public class AsyncHorizonJoinNotKeyedAtom extends BaseAsyncHorizonJoinAtom {
    private final SimpleMapValue ownerMapValue;
    private final ObjList<SimpleMapValue> perWorkerMapValues;

    public AsyncHorizonJoinNotKeyedAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            int masterTimestampColumnIndex,
            @NotNull LongList offsets,
            int valueCount,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterAsOfJoinMapSink,
            @Nullable RecordSink slaveAsOfJoinMapSink,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable ObjList<Function> perWorkerFilters,
            long masterTsScale,
            long slaveTsScale,
            int workerCount
    ) {
        super(
                asm,
                configuration,
                slaveFactory,
                masterTimestampColumnIndex,
                offsets,
                asOfJoinKeyTypes,
                masterAsOfJoinMapSink,
                slaveAsOfJoinMapSink,
                masterColumnCount,
                masterSymbolKeyColumnIndices,
                slaveSymbolKeyColumnIndices,
                columnSources,
                columnIndexes,
                ownerGroupByFunctions,
                perWorkerGroupByFunctions,
                compiledFilter,
                bindVarMemory,
                bindVarFunctions,
                ownerFilter,
                perWorkerFilters,
                masterTsScale,
                slaveTsScale,
                workerCount
        );

        try {
            // Per-worker SimpleMapValue instances for aggregation
            this.ownerMapValue = new SimpleMapValue(valueCount);
            this.perWorkerMapValues = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMapValues.add(new SimpleMapValue(valueCount));
            }

            // Initialize values to empty state
            resetMapValues();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Get the map value for the given slot.
     */
    public SimpleMapValue getMapValue(int slotId) {
        if (slotId == -1) {
            return ownerMapValue;
        }
        return perWorkerMapValues.getQuick(slotId);
    }

    /**
     * Get the owner map value. Thread-unsafe, should be used by query owner thread only.
     */
    public SimpleMapValue getOwnerMapValue() {
        return ownerMapValue;
    }

    /**
     * Get all per-worker map values. Thread-unsafe, should be used by query owner thread only.
     */
    public ObjList<SimpleMapValue> getPerWorkerMapValues() {
        return perWorkerMapValues;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("AsyncHorizonGroupByNotKeyedAtom");
    }

    @Override
    protected void clearAggregationState() {
        resetMapValues();
    }

    @Override
    protected void closeAggregationState() {
        Misc.free(ownerMapValue);
        Misc.freeObjList(perWorkerMapValues);
    }

    private void resetMapValues() {
        ownerFunctionUpdater.updateEmpty(ownerMapValue);
        ownerMapValue.setNew(true);
        for (int i = 0, n = perWorkerMapValues.size(); i < n; i++) {
            SimpleMapValue value = perWorkerMapValues.getQuick(i);
            ownerFunctionUpdater.updateEmpty(value);
            value.setNew(true);
        }
    }
}
