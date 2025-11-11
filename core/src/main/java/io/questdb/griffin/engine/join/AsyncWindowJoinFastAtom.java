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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectIntMultiLongHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AsyncWindowJoinFastAtom extends AbstractWindowJoinAtom {
    private final int masterSymbolIndex;
    private final DirectIntMultiLongHashMap ownerSlaveData;
    private final ObjList<DirectIntMultiLongHashMap> perWorkerSlaveData;
    private final int slaveSymbolIndex;
    // slave-to-master symbol key lookup hash table
    private final DirectIntIntHashMap slaveSymbolLookupTable;

    public AsyncWindowJoinFastAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            @Nullable Function ownerJoinFilter,
            @Nullable ObjList<Function> perWorkerJoinFilters,
            int masterSymbolIndex,
            int slaveSymbolIndex,
            long joinWindowLo,
            long joinWindowHi,
            int columnSplit,
            int masterTimestampIndex,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledMasterFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerMasterFilter,
            @Nullable ObjList<Function> perWorkerMasterFilters,
            long masterTsScale,
            long slaveTsScale,
            int workerCount
    ) {
        super(
                asm,
                configuration,
                slaveFactory,
                ownerJoinFilter,
                perWorkerJoinFilters,
                joinWindowLo,
                joinWindowHi,
                columnSplit,
                masterTimestampIndex,
                valueTypes,
                ownerGroupByFunctions,
                perWorkerGroupByFunctions,
                compiledMasterFilter,
                bindVarMemory,
                bindVarFunctions,
                ownerMasterFilter,
                perWorkerMasterFilters,
                masterTsScale,
                slaveTsScale,
                workerCount
        );

        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.masterSymbolIndex = masterSymbolIndex;
            this.slaveSymbolIndex = slaveSymbolIndex;
            this.slaveSymbolLookupTable = new DirectIntIntHashMap(16, 0.5, StaticSymbolTable.VALUE_NOT_FOUND, MemoryTag.NATIVE_UNORDERED_MAP);
            int slaveDataLen = isVectorized() ? 2 + groupByColumnIndexes.size() : 3;
            // Combined storage with 4 values: rowIds ptr, timestamps ptr, rowLos value, columnSink ptr
            this.ownerSlaveData = new DirectIntMultiLongHashMap(16, 0.5, 0, slaveDataLen, MemoryTag.NATIVE_UNORDERED_MAP);
            this.perWorkerSlaveData = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveData.extendAndSet(i, new DirectIntMultiLongHashMap(16, 0.5, 0, slaveDataLen, MemoryTag.NATIVE_UNORDERED_MAP));
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        super.clear();
        Misc.clear(ownerSlaveData);
        Misc.clearObjList(perWorkerSlaveData);
    }

    @Override
    public void close() {
        super.close();
        Misc.free(slaveSymbolLookupTable);
        Misc.free(ownerSlaveData);
        Misc.freeObjList(perWorkerSlaveData);
        Misc.clear(ownerSlaveData);
        Misc.clearObjList(perWorkerSlaveData);
    }

    public int getMasterSymbolIndex() {
        return masterSymbolIndex;
    }

    public DirectIntMultiLongHashMap getSlaveData(int slotId) {
        if (slotId == -1) {
            return ownerSlaveData;
        }
        return perWorkerSlaveData.getQuick(slotId);
    }

    public int getSlaveSymbolIndex() {
        return slaveSymbolIndex;
    }

    public DirectIntIntHashMap getSlaveSymbolLookupTable() {
        return slaveSymbolLookupTable;
    }

    public void initTimeFrameCursors(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor pageFrameCursor,
            PageFrameAddressCache frameAddressCache,
            IntList framePartitionIndexes,
            LongList frameRowCounts,
            LongList partitionTimestamps,
            int frameCount
    ) throws SqlException {
        super.initTimeFrameCursors(
                executionContext,
                masterSymbolTableSource,
                pageFrameCursor,
                frameAddressCache,
                framePartitionIndexes,
                frameRowCounts,
                partitionTimestamps,
                frameCount
        );
        final SymbolTableSource slaveSymbolTableSource = ownerSlaveTimeFrameHelper.getSymbolTableSource();
        StaticSymbolTable masterSymbolTable = (StaticSymbolTable) masterSymbolTableSource.getSymbolTable(masterSymbolIndex);
        StaticSymbolTable slaveSymbolTable = (StaticSymbolTable) slaveSymbolTableSource.getSymbolTable(slaveSymbolIndex);
        slaveSymbolLookupTable.clear();
        for (int masterKey = 0, n = masterSymbolTable.getSymbolCount(); masterKey < n; masterKey++) {
            final CharSequence masterSym = masterSymbolTable.valueOf(masterKey);
            final int slaveKey = slaveSymbolTable.keyOf(masterSym);
            if (slaveKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                slaveSymbolLookupTable.put(slaveKey + 1, masterKey);
            }
        }
        if (masterSymbolTable.containsNullValue() && slaveSymbolTable.containsNullValue()) {
            slaveSymbolLookupTable.put(0, StaticSymbolTable.VALUE_IS_NULL);
        }
    }
}
