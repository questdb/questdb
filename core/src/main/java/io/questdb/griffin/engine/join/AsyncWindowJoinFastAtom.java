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
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AsyncWindowJoinFastAtom extends AsyncWindowJoinAtom {
    // +1 symbol keys to use zero as the noKeyValue
    static final int KEY_SHIFT = 2;
    static final int NULL_KEY = 1;
    static final int SLAVE_MAP_INITIAL_CAPACITY = 16;
    static final double SLAVE_MAP_LOAD_FACTOR = 0.5;
    private final int masterSymbolIndex;
    private final WindowJoinPrevailingCache ownerPrevailingCache;
    private final DirectIntMultiLongHashMap ownerSlaveData;
    private final ObjList<WindowJoinPrevailingCache> perWorkerPrevailingCache;
    private final ObjList<DirectIntMultiLongHashMap> perWorkerSlaveData;
    private final int slaveSymbolIndex;
    // slave-to-master symbol key lookup hash table
    private final DirectIntIntHashMap slaveSymbolLookupMap;

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
            boolean includePrevailing,
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
            @Nullable IntHashSet filterUsedColumnIndexes,
            boolean vectorized,
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
                includePrevailing,
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
                filterUsedColumnIndexes,
                vectorized,
                masterTsScale,
                slaveTsScale,
                workerCount
        );

        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.masterSymbolIndex = masterSymbolIndex;
            this.slaveSymbolIndex = slaveSymbolIndex;
            this.slaveSymbolLookupMap = new DirectIntIntHashMap(
                    SLAVE_MAP_INITIAL_CAPACITY,
                    SLAVE_MAP_LOAD_FACTOR,
                    0,
                    StaticSymbolTable.VALUE_NOT_FOUND,
                    MemoryTag.NATIVE_UNORDERED_MAP
            );

            final int slaveDataLen = isVectorized() ? 2 + ownerGroupByFunctionArgs.size() : 3;
            // Combined storage with 4 values: rowIds ptr, timestamps ptr, rowLos value, columnSink ptr
            this.ownerSlaveData = new DirectIntMultiLongHashMap(
                    SLAVE_MAP_INITIAL_CAPACITY,
                    SLAVE_MAP_LOAD_FACTOR,
                    0,
                    0,
                    slaveDataLen,
                    MemoryTag.NATIVE_UNORDERED_MAP
            );
            this.perWorkerSlaveData = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveData.extendAndSet(i, new DirectIntMultiLongHashMap(
                        SLAVE_MAP_INITIAL_CAPACITY,
                        SLAVE_MAP_LOAD_FACTOR,
                        0,
                        0,
                        slaveDataLen,
                        MemoryTag.NATIVE_UNORDERED_MAP
                ));
            }

            if (includePrevailing) {
                // <symbol_key, rowid> cache for INCLUDE PREVAILING lookups
                this.ownerPrevailingCache = new WindowJoinPrevailingCache();
                this.perWorkerPrevailingCache = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerPrevailingCache.extendAndSet(i, new WindowJoinPrevailingCache());
                }
            } else {
                this.ownerPrevailingCache = null;
                this.perWorkerPrevailingCache = null;
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        super.clear();
        Misc.free(slaveSymbolLookupMap);
        Misc.free(ownerSlaveData);
        Misc.freeObjListAndKeepObjects(perWorkerSlaveData);
        Misc.free(ownerPrevailingCache);
        Misc.freeObjListAndKeepObjects(perWorkerPrevailingCache);
    }

    public void clearTemporaryData(int slotId) {
        super.clearTemporaryData(slotId);
        if (slotId == -1) {
            ownerSlaveData.clear();
        } else {
            perWorkerSlaveData.getQuick(slotId).clear();
        }
    }

    @Override
    public void close() {
        super.close();
        Misc.free(slaveSymbolLookupMap);
        Misc.free(ownerSlaveData);
        Misc.freeObjList(perWorkerSlaveData);
        Misc.free(ownerPrevailingCache);
        Misc.freeObjList(perWorkerPrevailingCache);
    }

    public int getMasterSymbolIndex() {
        return masterSymbolIndex;
    }

    public WindowJoinPrevailingCache getPrevailingCache(int slotId) {
        if (slotId == -1) {
            return ownerPrevailingCache;
        }
        return perWorkerPrevailingCache.getQuick(slotId);
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

    public DirectIntIntHashMap getSlaveSymbolLookupMap() {
        return slaveSymbolLookupMap;
    }

    public void initTimeFrameCursors(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor pageFrameCursor,
            PageFrameAddressCache frameAddressCache,
            IntList framePartitionIndexes,
            LongList frameRowCounts,
            LongList partitionTimestamps,
            LongList partitionCeilings,
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
                partitionCeilings,
                frameCount
        );

        slaveSymbolLookupMap.reopen();
        ownerSlaveData.reopen();
        for (int i = 0, n = perWorkerSlaveData.size(); i < n; i++) {
            perWorkerSlaveData.getQuick(i).reopen();
        }
        if (ownerPrevailingCache != null) {
            ownerPrevailingCache.reopen();
            for (int i = 0, n = perWorkerPrevailingCache.size(); i < n; i++) {
                perWorkerPrevailingCache.getQuick(i).reopen();
            }
        }

        final SymbolTableSource slaveSymbolTableSource = ownerSlaveTimeFrameHelper.getSymbolTableSource();
        StaticSymbolTable masterSymbolTable = (StaticSymbolTable) masterSymbolTableSource.getSymbolTable(masterSymbolIndex);
        StaticSymbolTable slaveSymbolTable = (StaticSymbolTable) slaveSymbolTableSource.getSymbolTable(slaveSymbolIndex);
        for (int masterKey = 0, n = masterSymbolTable.getSymbolCount(); masterKey < n; masterKey++) {
            final CharSequence masterSym = masterSymbolTable.valueOf(masterKey);
            final int slaveKey = slaveSymbolTable.keyOf(masterSym);
            if (slaveKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                slaveSymbolLookupMap.put(slaveKey + KEY_SHIFT, masterKey);
            }
        }
        if (masterSymbolTable.containsNullValue() && slaveSymbolTable.containsNullValue()) {
            slaveSymbolLookupMap.put(NULL_KEY, StaticSymbolTable.VALUE_IS_NULL);
        }
    }

    static int toSymbolMapKey(int key) {
        return Math.max(key + KEY_SHIFT, NULL_KEY);
    }
}
