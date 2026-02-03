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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Atom for keyed HORIZON JOIN GROUP BY that uses Maps for aggregation.
 * <p>
 * This class extends {@link BaseAsyncHorizonJoinAtom} and adds:
 * - Per-worker aggregation Maps (key -> value)
 * - GROUP BY key copier for populating map keys
 */
public class AsyncHorizonJoinAtom extends BaseAsyncHorizonJoinAtom {
    private final RecordSink groupByKeyCopier;
    private final Map ownerMap;
    private final ObjList<Map> perWorkerMaps;

    public AsyncHorizonJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            int masterTimestampColumnIndex,
            @NotNull LongList offsets,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterKeyCopier,
            @Nullable RecordSink slaveKeyCopier,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            @NotNull RecordSink groupByKeyCopier,
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
                masterKeyCopier,
                slaveKeyCopier,
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
            // GROUP BY key copier for MarkoutRecord
            this.groupByKeyCopier = groupByKeyCopier;

            // Per-worker aggregation maps
            this.perWorkerMaps = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMaps.add(MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes));
            }
            this.ownerMap = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public RecordSink getGroupByKeyCopier() {
        return groupByKeyCopier;
    }

    public Map getMap(int slotId) {
        Map map;
        if (slotId == -1) {
            map = ownerMap;
        } else {
            map = perWorkerMaps.getQuick(slotId);
        }
        if (!map.isOpen()) {
            map.reopen();
        }
        return map;
    }

    /**
     * Merge all per-worker maps into the owner map.
     */
    public Map mergeOwnerMap() {
        if (!ownerMap.isOpen()) {
            ownerMap.reopen();
        }

        for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
            Map workerMap = perWorkerMaps.getQuick(i);
            if (workerMap.isOpen() && workerMap.size() > 0) {
                ownerMap.merge(workerMap, ownerFunctionUpdater);
                workerMap.close();
            }
        }

        return ownerMap;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("AsyncHorizonGroupByAtom");
    }

    @Override
    protected void clearAggregationState() {
        Misc.free(ownerMap);
        Misc.freeObjListAndKeepObjects(perWorkerMaps);
    }

    @Override
    protected void closeAggregationState() {
        Misc.free(ownerMap);
        Misc.freeObjList(perWorkerMaps);
    }
}
