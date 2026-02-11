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
import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectIntList;
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
 * - Per-worker map sinks for populating map keys (supports expression keys)
 */
public class AsyncHorizonJoinAtom extends BaseAsyncHorizonJoinAtom {
    private final ObjList<Function> ownerKeyFunctions;
    private final Map ownerMap;
    private final RecordSink ownerMapSink;
    private final ObjList<ObjList<Function>> perWorkerKeyFunctions;
    private final ObjList<RecordSink> perWorkerMapSinks;
    private final ObjList<Map> perWorkerMaps;

    public AsyncHorizonJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata markoutMetadata,
            @NotNull RecordCursorFactory slaveFactory,
            int masterTimestampColumnIndex,
            @NotNull LongList offsets,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable Class<RecordSink> masterAsOfJoinMapSinkClass,
            @Nullable Class<RecordSink> slaveAsOfJoinMapSinkClass,
            @Transient @Nullable ColumnTypes masterAsOfJoinColumnTypes,
            @Transient @Nullable ColumnFilter masterAsOfJoinColumnFilter,
            @Transient @Nullable ColumnTypes slaveAsOfJoinColumnTypes,
            @Transient @Nullable ColumnFilter slaveAsOfJoinColumnFilter,
            @Transient @Nullable BitSet asOfWriteSymbolAsString,
            @Transient @Nullable BitSet asOfWriteStringAsVarcharMaster,
            @Transient @Nullable BitSet asOfWriteStringAsVarcharSlave,
            @Transient @Nullable BitSet writeTimestampAsNanosMaster,
            @Transient @Nullable BitSet writeTimestampAsNanosSlave,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            @Transient @NotNull ListColumnFilter groupByColumnFilter,
            @NotNull ObjList<Function> keyFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
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
                masterAsOfJoinMapSinkClass,
                slaveAsOfJoinMapSinkClass,
                masterAsOfJoinColumnTypes,
                masterAsOfJoinColumnFilter,
                slaveAsOfJoinColumnTypes,
                slaveAsOfJoinColumnFilter,
                asOfWriteSymbolAsString,
                asOfWriteStringAsVarcharMaster,
                asOfWriteStringAsVarcharSlave,
                writeTimestampAsNanosMaster,
                writeTimestampAsNanosSlave,
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
            // Store key functions for init() and close()
            this.ownerKeyFunctions = keyFunctions;
            this.perWorkerKeyFunctions = perWorkerKeyFunctions;

            // Create per-worker map sinks to support expression keys
            // Each worker needs its own sink with its own key functions
            final Class<RecordSink> sinkClass = RecordSinkFactory.getInstanceClass(
                    configuration,
                    asm,
                    markoutMetadata,
                    groupByColumnFilter,
                    keyFunctions,
                    null,
                    null,
                    null,
                    null
            );
            ownerMapSink = RecordSinkFactory.getInstance(
                    sinkClass,
                    markoutMetadata,
                    groupByColumnFilter,
                    keyFunctions,
                    null,
                    null,
                    null,
                    null
            );

            if (perWorkerKeyFunctions != null) {
                perWorkerMapSinks = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerMapSinks.extendAndSet(i, RecordSinkFactory.getInstance(
                            sinkClass,
                            markoutMetadata,
                            groupByColumnFilter,
                            perWorkerKeyFunctions.getQuick(i),
                            null,
                            null,
                            null,
                            null
                    ));
                }
            } else {
                perWorkerMapSinks = null;
            }

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

    public RecordSink getMapSink(int slotId) {
        if (slotId == -1 || perWorkerMapSinks == null) {
            return ownerMapSink;
        }
        return perWorkerMapSinks.getQuick(slotId);
    }

    @Override
    public void initTimeFrameCursors(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor slavePageFrameCursor,
            PageFrameAddressCache slaveFrameAddressCache,
            DirectIntList slaveFramePartitionIndexes,
            LongList slaveFrameRowCounts,
            LongList slavePartitionTimestamps,
            LongList slavePartitionCeilings,
            int frameCount
    ) throws SqlException {
        super.initTimeFrameCursors(
                executionContext,
                masterSymbolTableSource,
                slavePageFrameCursor,
                slaveFrameAddressCache,
                slaveFramePartitionIndexes,
                slaveFrameRowCounts,
                slavePartitionTimestamps,
                slavePartitionCeilings,
                frameCount
        );

        // Initialize key functions (for expression keys) with combined symbol table source
        final HorizonJoinSymbolTableSource horizonJoinSymbolTableSource = getSymbolTableSource();
        if (ownerKeyFunctions != null) {
            Function.init(ownerKeyFunctions, horizonJoinSymbolTableSource, executionContext, null);
        }

        if (perWorkerKeyFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerKeyFunctions.size(); i < n; i++) {
                    Function.init(perWorkerKeyFunctions.getQuick(i), horizonJoinSymbolTableSource, executionContext, null);
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
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
        Misc.freeObjList(ownerKeyFunctions);
        if (perWorkerKeyFunctions != null) {
            for (int i = 0, n = perWorkerKeyFunctions.size(); i < n; i++) {
                Misc.freeObjList(perWorkerKeyFunctions.getQuick(i));
            }
        }
    }
}
