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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameState;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectBitSet;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectIntMultiLongHashMap;
import io.questdb.std.DirectLongLongHashMap;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
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
    static final double SLAVE_MAP_LOAD_FACTOR = 0.7;
    // Symbol-equality conjuncts of the join filter, as (master column, slave column) record index
    // pairs, and per pair the master keys whose value the slave column holds at all: a master row
    // whose value is missing from the slave can match no slave row, so it has no prevailing row.
    private final IntList joinFilterSymbolPairs = new IntList();
    private final ObjList<DirectBitSet> joinFilterMasterKeysInSlave = new ObjList<>();
    private final IntList joinFilterMasterSymbolCounts = new IntList();
    private final int masterSymbolIndex;
    // Per slot: (master key, master filter key) -> the prevailing row the backward scan found for the
    // page frame being reduced. Valid when the join filter is exactly one symbol equality, so its
    // outcome on a slave row depends on those two keys only.
    private final ObjList<DirectLongLongHashMap> perWorkerPrevailingMemo = new ObjList<>();
    private DirectLongLongHashMap ownerPrevailingMemo;
    private final WindowJoinPrevailingCache ownerPrevailingCache;
    private final DirectIntMultiLongHashMap ownerSlaveData;
    private final ObjList<WindowJoinPrevailingCache> perWorkerPrevailingCache;
    // per-block last rows of each key, shared by the prevailing caches of every slot
    private final WindowJoinPrevailingSummaries prevailingSummaries;
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
            long windowLo,
            long windowHi,
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
                windowLo,
                windowHi,
                null,
                null,
                null,
                null,
                0,
                0,
                (char) 0,
                (char) 0,
                null,
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
                this.prevailingSummaries = new WindowJoinPrevailingSummaries();
                this.ownerPrevailingCache = new WindowJoinPrevailingCache();
                ownerPrevailingCache.setSummaries(prevailingSummaries);
                this.perWorkerPrevailingCache = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    final WindowJoinPrevailingCache prevailingCache = new WindowJoinPrevailingCache();
                    prevailingCache.setSummaries(prevailingSummaries);
                    perWorkerPrevailingCache.extendAndSet(i, prevailingCache);
                }
            } else {
                this.prevailingSummaries = null;
                this.ownerPrevailingCache = null;
                this.perWorkerPrevailingCache = null;
            }
        } catch (Throwable th) {
            // Free the FIELDS through the failure chain rather than calling close() bare: close()
            // rethrows its own cleanup failure, which would replace the construction failure the
            // caller has to see.
            Misc.free(this, th);
            throw th;
        }
    }

    public void clearTemporaryData(int slotId) {
        super.clearTemporaryData(slotId);
        if (slotId == -1) {
            ownerSlaveData.clear();
        } else {
            perWorkerSlaveData.getQuick(slotId).clear();
        }
        final DirectLongLongHashMap memo = getPrevailingMemo(slotId);
        if (memo != null) {
            memo.clear();
        }
    }

    /**
     * The memo key of a master row for {@link #getPrevailingMemo(int)}: its join key and the key of
     * its join filter's symbol column, both non-negative.
     */
    public long getPrevailingMemoKey(Record masterRecord, int masterKey) {
        final int filterKey = masterRecord.getInt(joinFilterSymbolPairs.getQuick(0));
        return ((long) toSymbolMapKey(masterKey) << 32) | toSymbolMapKey(filterKey);
    }

    /**
     * The slot's memo of prevailing rows found by the join-filtered backward scan in the current
     * page frame, or null when the join filter is not a single symbol equality.
     */
    public @Nullable DirectLongLongHashMap getPrevailingMemo(int slotId) {
        if (slotId == -1) {
            return ownerPrevailingMemo;
        }
        return perWorkerPrevailingMemo.getQuiet(slotId);
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

    /**
     * True when a symbol-equality conjunct of the join filter cannot hold for this master row on any
     * slave row: the master value is not NULL and the slave column's symbol table does not hold it.
     */
    public boolean isJoinFilterUnsatisfiable(Record masterRecord) {
        for (int i = 0, n = joinFilterMasterKeysInSlave.size(); i < n; i++) {
            final int masterKey = masterRecord.getInt(joinFilterSymbolPairs.getQuick(2 * i));
            // NULL, or a key the symbol table snapshot does not cover: no claim
            if (masterKey >= 0 && masterKey < joinFilterMasterSymbolCounts.getQuick(i)
                    && !joinFilterMasterKeysInSlave.getQuick(i).get(masterKey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Hands over the symbol-equality conjuncts of the join filter, found by the code generator.
     *
     * @param pairs             (master column, slave column) record index pairs
     * @param isFilterOnlyPairs true when the join filter is nothing but these conjuncts
     */
    public void setJoinFilterSymbolPairs(IntList pairs, boolean isFilterOnlyPairs) {
        joinFilterSymbolPairs.clear();
        joinFilterSymbolPairs.addAll(pairs);
        for (int i = 0, n = pairs.size() / 2; i < n; i++) {
            joinFilterMasterKeysInSlave.add(new DirectBitSet(64, MemoryTag.NATIVE_BIT_SET, true));
        }
        if (isFilterOnlyPairs && pairs.size() == 2 && ownerPrevailingMemo == null) {
            ownerPrevailingMemo = newPrevailingMemo();
            for (int i = 0, n = perWorkerSlaveData.size(); i < n; i++) {
                perWorkerPrevailingMemo.extendAndSet(i, newPrevailingMemo());
            }
        }
    }

    public DirectIntIntHashMap getSlaveSymbolLookupMap() {
        return slaveSymbolLookupMap;
    }

    public void initTimeFrameCursors(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor pageFrameCursor,
            ConcurrentTimeFrameState sharedState
    ) throws SqlException {
        super.initTimeFrameCursors(
                executionContext,
                masterSymbolTableSource,
                pageFrameCursor,
                sharedState
        );

        // The symbol lookup map, the owner and per-worker slave data maps and the prevailing caches
        // all grow with the join's symbol cardinality, so they charge the per-query tracker like the
        // allocators the parent binds in reopen(). Binding runs while each map is closed - clear()
        // releases them and drops their tracker - so every block is freed under the tracker that
        // charged it. A worker slot's map is charged to the query that acquired the slot, which is
        // the query this atom belongs to.
        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        slaveSymbolLookupMap.setMemoryTracker(memoryTracker);
        slaveSymbolLookupMap.reopen();
        ownerSlaveData.setMemoryTracker(memoryTracker);
        ownerSlaveData.reopen();
        for (int i = 0, n = perWorkerSlaveData.size(); i < n; i++) {
            final DirectIntMultiLongHashMap slaveData = perWorkerSlaveData.getQuick(i);
            slaveData.setMemoryTracker(memoryTracker);
            slaveData.reopen();
        }
        if (ownerPrevailingCache != null) {
            ownerPrevailingCache.setMemoryTracker(memoryTracker);
            ownerPrevailingCache.reopen();
            for (int i = 0, n = perWorkerPrevailingCache.size(); i < n; i++) {
                final WindowJoinPrevailingCache prevailingCache = perWorkerPrevailingCache.getQuick(i);
                prevailingCache.setMemoryTracker(memoryTracker);
                prevailingCache.reopen();
            }
        }

        final SymbolTableSource slaveSymbolTableSource = ownerSlaveTimeFrameHelper.getSymbolTableSource();
        initJoinFilterMasterKeysInSlave(masterSymbolTableSource, slaveSymbolTableSource, memoryTracker);
        if (ownerPrevailingMemo != null) {
            ownerPrevailingMemo.setMemoryTracker(memoryTracker);
            ownerPrevailingMemo.reopen();
            for (int i = 0, n = perWorkerPrevailingMemo.size(); i < n; i++) {
                final DirectLongLongHashMap memo = perWorkerPrevailingMemo.getQuick(i);
                memo.setMemoryTracker(memoryTracker);
                memo.reopen();
            }
        }
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
        if (prevailingSummaries != null) {
            // sized by the keys that can join; allocates only when a lookup first needs a block
            prevailingSummaries.of(sharedState.getFrameCount(), slaveSymbolLookupMap, memoryTracker);
        }
    }

    // Both hooks below chain their own failures for the same reason the base does: every map here
    // charges the per-query tracker and drops it on close(), so each one has to be released under
    // the tracker that charged it, and a failure on one map must not skip the rest. The base folds
    // whatever these rethrow into its own chain and reports it once every leg has run.
    @Override
    protected void clearKeyedState() {
        Throwable cleanupFailure = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, slaveSymbolLookupMap);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerSlaveData);
        cleanupFailure = Misc.freeObjListAndKeepObjectsBestEffort(cleanupFailure, perWorkerSlaveData);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerPrevailingCache);
        cleanupFailure = Misc.freeObjListAndKeepObjectsBestEffort(cleanupFailure, perWorkerPrevailingCache);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, prevailingSummaries);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerPrevailingMemo);
        cleanupFailure = Misc.freeObjListAndKeepObjectsBestEffort(cleanupFailure, perWorkerPrevailingMemo);
        cleanupFailure = Misc.freeObjListAndKeepObjectsBestEffort(cleanupFailure, joinFilterMasterKeysInSlave);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    @Override
    protected void closeKeyedState() {
        Throwable cleanupFailure = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, slaveSymbolLookupMap);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerSlaveData);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerSlaveData);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerPrevailingCache);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerPrevailingCache);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, prevailingSummaries);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerPrevailingMemo);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerPrevailingMemo);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, joinFilterMasterKeysInSlave);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    // Per symbol-equality conjunct of the join filter, which master keys the slave column holds. An
    // optimisation only: if the query's memory limit has no room for the bitsets, no master row is
    // claimed unsatisfiable and every row takes the backward scan, as without them.
    private void initJoinFilterMasterKeysInSlave(
            SymbolTableSource masterSymbolTableSource,
            SymbolTableSource slaveSymbolTableSource,
            @Nullable MemoryTracker memoryTracker
    ) {
        joinFilterMasterSymbolCounts.clear();
        final int pairCount = joinFilterMasterKeysInSlave.size();
        try {
            for (int i = 0; i < pairCount; i++) {
                final StaticSymbolTable masterTable = (StaticSymbolTable) masterSymbolTableSource.getSymbolTable(joinFilterSymbolPairs.getQuick(2 * i));
                final StaticSymbolTable slaveTable = (StaticSymbolTable) slaveSymbolTableSource.getSymbolTable(joinFilterSymbolPairs.getQuick(2 * i + 1));
                final DirectBitSet keysInSlave = joinFilterMasterKeysInSlave.getQuick(i);
                final int masterCount = masterTable.getSymbolCount();
                keysInSlave.setMemoryTracker(memoryTracker);
                keysInSlave.reserve(Math.max(1, masterCount));
                keysInSlave.clear();
                for (int key = 0; key < masterCount; key++) {
                    if (slaveTable.keyOf(masterTable.valueOf(key)) != StaticSymbolTable.VALUE_NOT_FOUND) {
                        keysInSlave.set(key);
                    }
                }
                joinFilterMasterSymbolCounts.add(masterCount);
            }
        } catch (CairoException e) {
            if (!e.isOutOfMemory()) {
                throw e;
            }
            for (int i = 0; i < pairCount; i++) {
                joinFilterMasterKeysInSlave.getQuick(i).close();
            }
            // a count of 0 makes no claim for any master key
            joinFilterMasterSymbolCounts.clear();
            joinFilterMasterSymbolCounts.setAll(pairCount, 0);
        }
    }

    private static DirectLongLongHashMap newPrevailingMemo() {
        // keys and row ids are non-negative: -1 is free as both the no-key and the no-value marker
        return new DirectLongLongHashMap(SLAVE_MAP_INITIAL_CAPACITY, SLAVE_MAP_LOAD_FACTOR, -1, -1, MemoryTag.NATIVE_UNORDERED_MAP, false);
    }

    static int toSymbolMapKey(int key) {
        return Math.max(key + KEY_SHIFT, NULL_KEY);
    }
}
