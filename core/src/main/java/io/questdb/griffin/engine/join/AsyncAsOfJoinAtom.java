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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameState;
import io.questdb.griffin.engine.table.SelectivityStats;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.griffin.engine.table.AsyncFilterUtils.prepareBindVarMemory;

/**
 * Shared and per-worker state of {@link AsyncAsOfJoinRecordCursorFactory}, a keyed ASOF JOIN on one
 * SYMBOL column that joins the page frames of the master in parallel.
 * <p>
 * Keys: every master key whose value the slave's symbol table holds (and, with a slave key filter,
 * whose slave key passes it) gets a dense "joinable slot". Two shared int arrays map the master and
 * the slave symbol keys to their slot, -1 for a key that cannot join; NULL maps to NULL. The per
 * worker span scan keeps, per slot, the last slave row id met so far in the page frame's slave span,
 * tagged with the frame's epoch so that nothing has to be cleared between frames.
 * <p>
 * Memory: the shared arrays take 4 bytes per master and per slave symbol, a worker's span state 12
 * bytes per joinable key. They are charged to the query's memory tracker when the cursor opens. When
 * the tracker refuses them, the join runs every frame in "lean" mode, which allocates nothing per key
 * (a backward scan per master row, with symbol lookups), so that a memory limit never fails a query
 * the serial ASOF JOIN would complete.
 */
public class AsyncAsOfJoinAtom implements StatefulAtom, PerWorkerLockOwner, Reopenable, Plannable {
    public static final int GATHER_NONE = -1;
    private static final int GATHER_BOOL = 0;
    private static final int GATHER_BYTE = 1;
    private static final int GATHER_CHAR = 3;
    private static final int GATHER_DATE = 10;
    private static final int GATHER_DECIMAL16 = 18;
    private static final int GATHER_DECIMAL32 = 19;
    private static final int GATHER_DECIMAL64 = 20;
    private static final int GATHER_DECIMAL8 = 17;
    private static final int GATHER_DOUBLE = 9;
    private static final int GATHER_FLOAT = 6;
    private static final int GATHER_GEOBYTE = 13;
    private static final int GATHER_GEOINT = 15;
    private static final int GATHER_GEOLONG = 16;
    private static final int GATHER_GEOSHORT = 14;
    private static final int GATHER_INT = 4;
    private static final int GATHER_IPV4 = 5;
    private static final int GATHER_LONG = 7;
    private static final int GATHER_SHORT = 2;
    private static final int GATHER_SYMBOL = 12;
    private static final int GATHER_TIMESTAMP = 11;
    private ObjList<Function> bindVarFunctions;
    private MemoryCARW bindVarMemory;
    private CompiledFilter compiledMasterFilter;
    private IntHashSet filterUsedColumnIndexes;
    // per slave column: how the span reduce gathers its value into the frame's output, GATHER_NONE
    // for a column read through the slave record (variable size and wide types)
    private final IntList gatherKinds = new IntList();
    // slave columns gathered, in slave column order, and per slave column its gather position or -1
    private final IntList gatherColumns = new IntList();
    private final IntList gatherPositions = new IntList();
    private final LongList gatherNullBits = new LongList();
    private final int masterSymbolIndex;
    private final int masterTimestampIndex;
    private final long masterTsScale;
    private Function ownerMasterFilter;
    private final WindowJoinPrevailingCache ownerPrevailingCache;
    private final SelectivityStats ownerSelectivityStats = new SelectivityStats();
    private final ConcurrentTimeFrameCursor ownerSlaveTimeFrameCursor;
    private final WindowJoinTimeFrameHelper ownerSlaveTimeFrameHelper;
    private final DirectLongList ownerSpanState = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
    private ObjList<Function> perWorkerMasterFilters;
    private final ObjList<WindowJoinPrevailingCache> perWorkerPrevailingCaches;
    private final ObjList<SelectivityStats> perWorkerSelectivityStats;
    private final ObjList<ConcurrentTimeFrameCursor> perWorkerSlaveTimeFrameCursors;
    private final ObjList<WindowJoinTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    private final ObjList<DirectLongList> perWorkerSpanStates;
    private final PerWorkerLocks perWorkerLocks;
    private final WindowJoinPrevailingSummaries prevailingSummaries;
    // span scan epochs, one per slot (the owner last); a frame's epoch tags the span state it wrote
    private final int[] slotEpochs;
    // master key + 1 -> slot, then slave key + 1 -> slot, NULL at 0; for a key that cannot join -1
    // on the master side and the span state's spare slot (joinableCount) on the slave side
    private final DirectLongList slotArrays = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
    // a filter on the slave's key column alone, folded into the slots: a slave key that fails it
    // cannot join. Owner only; evaluated once per slave key when the cursor opens.
    // true when the master filter reads the master's key column alone: then it also decides, once per
    // master key, which keys can join at all
    private boolean isMasterKeyFilter;
    private @Nullable Function slaveKeyFilter;
    private int slaveKeyFilterColumnIndex = -1;
    // slave key + KEY_SHIFT -> master key, for the prevailing cache and summaries
    private final DirectIntIntHashMap slaveSymbolLookupMap;
    private final int slaveSymbolIndex;
    private final long slaveTsScale;
    private final AtomicLong statFramesLean = new AtomicLong();
    private final AtomicLong statFramesSpan = new AtomicLong();
    private final AtomicLong statFramesWalk = new AtomicLong();
    private final AtomicLong statWalkAborts = new AtomicLong();
    // per slot (the owner last): per slave time frame, its row count, timestamp and key column
    // addresses, filled by the walk the first time it enters the frame; 0 rows = not yet known
    private final ObjList<long[]> walkFrameCaches = new ObjList<>();
    private ConcurrentTimeFrameState sharedState;
    private final long toleranceInterval;
    private boolean compiledMasterFilterSuspended;
    private int joinableCount;
    // no span state: every frame joins in lean mode (the memory tracker refused the state)
    private volatile boolean leanOnly;
    private int masterSlotCount;
    private long masterSlotsAddress;
    private MemoryTracker memoryTracker;
    private boolean skipJoin;
    private int slaveSlotCount;
    private long slaveSlotsAddress;
    private long slaveToMasterAddress;

    public AsyncAsOfJoinAtom(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            int masterSymbolIndex,
            int slaveSymbolIndex,
            int masterTimestampIndex,
            long toleranceInterval,
            long masterTsScale,
            long slaveTsScale,
            int workerCount
    ) {
        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.masterSymbolIndex = masterSymbolIndex;
            this.slaveSymbolIndex = slaveSymbolIndex;
            this.masterTimestampIndex = masterTimestampIndex;
            this.toleranceInterval = toleranceInterval;
            this.masterTsScale = masterTsScale;
            this.slaveTsScale = slaveTsScale;

            this.slaveSymbolLookupMap = new DirectIntIntHashMap(
                    AsyncWindowJoinFastAtom.SLAVE_MAP_INITIAL_CAPACITY,
                    AsyncWindowJoinFastAtom.SLAVE_MAP_LOAD_FACTOR,
                    0,
                    StaticSymbolTable.VALUE_NOT_FOUND,
                    MemoryTag.NATIVE_UNORDERED_MAP
            );
            this.ownerSlaveTimeFrameCursor = slaveFactory.newTimeFrameCursor();
            this.ownerSlaveTimeFrameHelper = new WindowJoinTimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTsScale);
            this.prevailingSummaries = new WindowJoinPrevailingSummaries();
            this.ownerPrevailingCache = new WindowJoinPrevailingCache();
            ownerPrevailingCache.setSummaries(prevailingSummaries);
            this.perWorkerSlaveTimeFrameCursors = new ObjList<>(slotCount);
            this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(slotCount);
            this.perWorkerPrevailingCaches = new ObjList<>(slotCount);
            this.perWorkerSelectivityStats = new ObjList<>(slotCount);
            this.perWorkerSpanStates = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveTimeFrameCursors.extendAndSet(i, slaveFactory.newTimeFrameCursor());
                perWorkerSlaveTimeFrameHelpers.extendAndSet(i, new WindowJoinTimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTsScale));
                final WindowJoinPrevailingCache cache = new WindowJoinPrevailingCache();
                cache.setSummaries(prevailingSummaries);
                perWorkerPrevailingCaches.extendAndSet(i, cache);
                perWorkerSelectivityStats.extendAndSet(i, new SelectivityStats());
                perWorkerSpanStates.extendAndSet(i, new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true));
            }
            this.slotEpochs = new int[slotCount + 1];
            this.perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            // what the reduce gathers per slave column
            final RecordMetadata slaveMetadata = slaveFactory.getMetadata();
            final Record nullRecord = NullRecordFactory.getInstance(slaveMetadata);
            for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
                final int kind = gatherKindOf(slaveMetadata.getColumnType(i));
                gatherKinds.add(kind);
                if (kind == GATHER_NONE) {
                    gatherPositions.add(-1);
                } else {
                    gatherPositions.add(gatherColumns.size());
                    gatherColumns.add(i);
                    gatherNullBits.add(readBits(nullRecord, i, kind));
                }
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    /**
     * Takes over the filters the code generator stole from the master and the slave, once the
     * factory is built. Cannot fail: from here on the atom owns and frees them.
     */
    public void adoptFilters(
            @Nullable CompiledFilter compiledMasterFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerMasterFilter,
            @Nullable ObjList<Function> perWorkerMasterFilters,
            @Nullable IntHashSet filterUsedColumnIndexes,
            boolean isMasterKeyFilter,
            @Nullable Function slaveKeyFilter,
            int slaveKeyFilterColumnIndex
    ) {
        this.isMasterKeyFilter = isMasterKeyFilter && ownerMasterFilter != null;
        this.compiledMasterFilter = compiledMasterFilter;
        this.bindVarMemory = bindVarMemory;
        this.bindVarFunctions = bindVarFunctions;
        this.ownerMasterFilter = ownerMasterFilter;
        this.perWorkerMasterFilters = perWorkerMasterFilters;
        this.filterUsedColumnIndexes = filterUsedColumnIndexes;
        this.slaveKeyFilter = slaveKeyFilter;
        this.slaveKeyFilterColumnIndex = slaveKeyFilterColumnIndex;
    }

    public static int gatherKindOf(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN -> GATHER_BOOL;
            case ColumnType.BYTE -> GATHER_BYTE;
            case ColumnType.SHORT -> GATHER_SHORT;
            case ColumnType.CHAR -> GATHER_CHAR;
            case ColumnType.INT -> GATHER_INT;
            case ColumnType.IPv4 -> GATHER_IPV4;
            case ColumnType.FLOAT -> GATHER_FLOAT;
            case ColumnType.LONG -> GATHER_LONG;
            case ColumnType.DOUBLE -> GATHER_DOUBLE;
            case ColumnType.DATE -> GATHER_DATE;
            case ColumnType.TIMESTAMP -> GATHER_TIMESTAMP;
            case ColumnType.SYMBOL -> GATHER_SYMBOL;
            case ColumnType.GEOBYTE -> GATHER_GEOBYTE;
            case ColumnType.GEOSHORT -> GATHER_GEOSHORT;
            case ColumnType.GEOINT -> GATHER_GEOINT;
            case ColumnType.GEOLONG -> GATHER_GEOLONG;
            case ColumnType.DECIMAL8 -> GATHER_DECIMAL8;
            case ColumnType.DECIMAL16 -> GATHER_DECIMAL16;
            case ColumnType.DECIMAL32 -> GATHER_DECIMAL32;
            case ColumnType.DECIMAL64 -> GATHER_DECIMAL64;
            default -> GATHER_NONE;
        };
    }

    /**
     * The bits a gathered column holds for a row: the value the record's getter returns, widened
     * to a long, in the column type's storage layout (its low bytes, little endian).
     */
    public static long readBits(Record record, int col, int kind) {
        return switch (kind) {
            case GATHER_BOOL -> record.getBool(col) ? 1 : 0;
            case GATHER_BYTE -> record.getByte(col);
            case GATHER_SHORT -> record.getShort(col);
            case GATHER_CHAR -> record.getChar(col);
            case GATHER_INT, GATHER_SYMBOL -> record.getInt(col);
            case GATHER_IPV4 -> record.getIPv4(col);
            case GATHER_FLOAT -> Float.floatToRawIntBits(record.getFloat(col));
            case GATHER_LONG -> record.getLong(col);
            case GATHER_DOUBLE -> Double.doubleToRawLongBits(record.getDouble(col));
            case GATHER_DATE -> record.getDate(col);
            case GATHER_TIMESTAMP -> record.getTimestamp(col);
            case GATHER_GEOBYTE -> record.getGeoByte(col);
            case GATHER_GEOSHORT -> record.getGeoShort(col);
            case GATHER_GEOINT -> record.getGeoInt(col);
            case GATHER_GEOLONG -> record.getGeoLong(col);
            case GATHER_DECIMAL8 -> record.getDecimal8(col);
            case GATHER_DECIMAL16 -> record.getDecimal16(col);
            case GATHER_DECIMAL32 -> record.getDecimal32(col);
            case GATHER_DECIMAL64 -> record.getDecimal64(col);
            default -> throw new UnsupportedOperationException();
        };
    }

    @Override
    public void clear() {
        Throwable failure = null;
        failure = Misc.freeBestEffort(failure, ownerSlaveTimeFrameCursor);
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, perWorkerSlaveTimeFrameCursors);
        failure = Misc.freeBestEffort(failure, slaveSymbolLookupMap);
        failure = Misc.freeBestEffort(failure, slotArrays);
        failure = Misc.freeBestEffort(failure, ownerSpanState);
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, perWorkerSpanStates);
        failure = Misc.freeBestEffort(failure, ownerPrevailingCache);
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, perWorkerPrevailingCaches);
        failure = Misc.freeBestEffort(failure, prevailingSummaries);
        failure = Misc.clearBestEffort(failure, ownerSelectivityStats);
        failure = Misc.clearObjListBestEffort(failure, perWorkerSelectivityStats);
        memoryTracker = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Throwable failure = null;
        failure = Misc.freeBestEffort(failure, ownerSlaveTimeFrameCursor);
        failure = Misc.freeObjListBestEffort(failure, perWorkerSlaveTimeFrameCursors);
        failure = Misc.freeBestEffort(failure, compiledMasterFilter);
        failure = Misc.freeBestEffort(failure, bindVarMemory);
        failure = Misc.freeObjListBestEffort(failure, bindVarFunctions);
        failure = Misc.freeBestEffort(failure, ownerMasterFilter);
        failure = Misc.freeObjListBestEffort(failure, perWorkerMasterFilters);
        failure = Misc.freeBestEffort(failure, slaveKeyFilter);
        failure = Misc.freeBestEffort(failure, slaveSymbolLookupMap);
        failure = Misc.freeBestEffort(failure, slotArrays);
        failure = Misc.freeBestEffort(failure, ownerSpanState);
        failure = Misc.freeObjListBestEffort(failure, perWorkerSpanStates);
        failure = Misc.freeBestEffort(failure, ownerPrevailingCache);
        failure = Misc.freeObjListBestEffort(failure, perWorkerPrevailingCaches);
        failure = Misc.freeBestEffort(failure, prevailingSummaries);
        CairoException.rethrowCleanupFailure(failure);
    }

    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public CompiledFilter getCompiledMasterFilter() {
        return compiledMasterFilterSuspended ? null : compiledMasterFilter;
    }

    public @Nullable IntHashSet getFilterUsedColumnIndexes() {
        return filterUsedColumnIndexes;
    }

    public int getGatherColumn(int i) {
        return gatherColumns.getQuick(i);
    }

    public int getGatherCount() {
        return gatherColumns.size();
    }

    public int getGatherKind(int slaveColumnIndex) {
        return gatherKinds.getQuick(slaveColumnIndex);
    }

    public long getGatherNullBits(int i) {
        return gatherNullBits.getQuick(i);
    }

    /**
     * The position of a slave column among the gathered ones, or -1 when it is read through the
     * slave record.
     */
    public int getGatherPosition(int slaveColumnIndex) {
        return gatherPositions.getQuick(slaveColumnIndex);
    }

    public int getJoinableCount() {
        return joinableCount;
    }

    /**
     * The slots of the span state: one per joinable key, and a spare one that takes the rows of the
     * slave keys that cannot join.
     */
    public int getSpanSlotCount() {
        return joinableCount + 1;
    }

    public Function getMasterFilter(int slotId) {
        if (slotId == -1 || perWorkerMasterFilters == null) {
            return ownerMasterFilter;
        }
        return perWorkerMasterFilters.getQuick(slotId);
    }

    public int getMasterSymbolIndex() {
        return masterSymbolIndex;
    }

    public int getMasterTimestampIndex() {
        return masterTimestampIndex;
    }

    public long getMasterTsScale() {
        return masterTsScale;
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    public WindowJoinPrevailingCache getPrevailingCache(int slotId) {
        return slotId == -1 ? ownerPrevailingCache : perWorkerPrevailingCaches.getQuick(slotId);
    }

    public SelectivityStats getSelectivityStats(int slotId) {
        return slotId == -1 ? ownerSelectivityStats : perWorkerSelectivityStats.getQuick(slotId);
    }

    public @Nullable Function getSlaveKeyFilter() {
        return slaveKeyFilter;
    }

    public DirectIntIntHashMap getSlaveSymbolLookupMap() {
        return slaveSymbolLookupMap;
    }

    public int getSlaveSymbolIndex() {
        return slaveSymbolIndex;
    }

    public WindowJoinTimeFrameHelper getSlaveTimeFrameHelper(int slotId) {
        return slotId == -1 ? ownerSlaveTimeFrameHelper : perWorkerSlaveTimeFrameHelpers.getQuick(slotId);
    }

    public long getSlaveTsScale() {
        return slaveTsScale;
    }

    /**
     * The span state of a slot: per span slot (see {@link #getSpanSlotCount()}), the last slave row id
     * met (8 bytes), then per span slot the row id the walk has walked back from (8 bytes), then per
     * span slot the epoch both belong to (4 bytes).
     */
    public long getSpanStateAddress(int slotId) {
        return (slotId == -1 ? ownerSpanState : perWorkerSpanStates.getQuick(slotId)).getAddress();
    }

    @TestOnly
    public long getStatFramesLean() {
        return statFramesLean.get();
    }

    @TestOnly
    public long getStatFramesSpan() {
        return statFramesSpan.get();
    }

    public long getToleranceInterval() {
        return toleranceInterval;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        memoryTracker = executionContext.getMemoryTracker();
        if (ownerMasterFilter != null) {
            ownerMasterFilter.init(symbolTableSource, executionContext);
        }
        if (perWorkerMasterFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerMasterFilters, symbolTableSource, executionContext, ownerMasterFilter);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
        if (bindVarFunctions != null) {
            Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
            compiledMasterFilterSuspended = !prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }
    }

    /**
     * Binds the slave time frame cursors and computes the joinable slots. Runs on the query's thread
     * when the cursor reads its first row.
     */
    public void initTimeFrameCursors(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor pageFrameCursor,
            ConcurrentTimeFrameState sharedState
    ) throws SqlException {
        final int timestampIndex = ownerSlaveTimeFrameCursor.getTimestampIndex();
        ownerSlaveTimeFrameCursor.of(sharedState, pageFrameCursor, timestampIndex);
        ownerSlaveTimeFrameCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
        ownerSlaveTimeFrameHelper.of(ownerSlaveTimeFrameCursor);
        for (int i = 0, n = perWorkerSlaveTimeFrameHelpers.size(); i < n; i++) {
            final ConcurrentTimeFrameCursor workerCursor = perWorkerSlaveTimeFrameCursors.getQuick(i);
            workerCursor.of(sharedState, pageFrameCursor, timestampIndex);
            workerCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            perWorkerSlaveTimeFrameHelpers.getQuick(i).of(workerCursor);
        }

        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        slaveSymbolLookupMap.setMemoryTracker(memoryTracker);
        slaveSymbolLookupMap.reopen();
        ownerPrevailingCache.setMemoryTracker(memoryTracker);
        ownerPrevailingCache.reopen();
        for (int i = 0, n = perWorkerPrevailingCaches.size(); i < n; i++) {
            final WindowJoinPrevailingCache cache = perWorkerPrevailingCaches.getQuick(i);
            cache.setMemoryTracker(memoryTracker);
            cache.reopen();
        }

        if (slaveKeyFilter != null) {
            // the filter reads the key column at its index in the filter's own metadata
            slaveKeyFilter.init(new KeyColumnSymbolTableSource(pageFrameCursor, slaveKeyFilterColumnIndex, slaveSymbolIndex), executionContext);
        }

        final StaticSymbolTable masterSymbolTable = (StaticSymbolTable) masterSymbolTableSource.getSymbolTable(masterSymbolIndex);
        final StaticSymbolTable slaveSymbolTable = pageFrameCursor.getSymbolTable(slaveSymbolIndex);
        final int masterCount = masterSymbolTable.getSymbolCount();
        final int slaveCount = slaveSymbolTable.getSymbolCount();
        leanOnly = false;
        joinableCount = 0;
        try {
            masterSlotCount = masterCount + 1;
            slaveSlotCount = slaveCount + 1;
            // master slots, slave slots, then the slave key -> master key map of the prevailing scans
            final long slotBytes = 4L * (masterSlotCount + 2L * slaveSlotCount);
            slotArrays.close();
            slotArrays.setMemoryTracker(memoryTracker);
            slotArrays.setCapacity((slotBytes + 7) >>> 3);
            masterSlotsAddress = slotArrays.getAddress();
            slaveSlotsAddress = masterSlotsAddress + 4L * masterSlotCount;
            slaveToMasterAddress = slaveSlotsAddress + 4L * slaveSlotCount;
            Vect.memset(masterSlotsAddress, 4L * (masterSlotCount + slaveSlotCount), -1);
            for (long i = 0; i < slaveSlotCount; i++) {
                Unsafe.putInt(slaveToMasterAddress + 4 * i, StaticSymbolTable.VALUE_NOT_FOUND);
            }
        } catch (CairoException e) {
            if (!e.isOutOfMemory()) {
                throw e;
            }
            slotArrays.close();
            masterSlotsAddress = 0;
            slaveSlotsAddress = 0;
            slaveToMasterAddress = 0;
            leanOnly = true;
        }

        final SlaveKeyRecord keyRecord = slaveKeyFilter != null ? new SlaveKeyRecord(slaveSymbolTable, slaveKeyFilterColumnIndex) : null;
        final SlaveKeyRecord masterKeyRecord = isMasterKeyFilter ? new SlaveKeyRecord(masterSymbolTable, masterSymbolIndex) : null;
        int slot = 0;
        for (int masterKey = 0; masterKey < masterCount; masterKey++) {
            if (!passesMasterKeyFilter(masterKeyRecord, masterKey)) {
                // no master row of this key passes the master filter
                continue;
            }
            final int slaveKey = slaveSymbolTable.keyOf(masterSymbolTable.valueOf(masterKey));
            if (slaveKey != StaticSymbolTable.VALUE_NOT_FOUND && passesKeyFilter(keyRecord, slaveKey)) {
                slaveSymbolLookupMap.put(slaveKey + AsyncWindowJoinFastAtom.KEY_SHIFT, masterKey);
                if (!leanOnly) {
                    Unsafe.putInt(masterSlotsAddress + 4L * (masterKey + 1), slot);
                    Unsafe.putInt(slaveSlotsAddress + 4L * (slaveKey + 1), slot);
                    Unsafe.putInt(slaveToMasterAddress + 4L * (slaveKey + 1), masterKey);
                }
                slot++;
            }
        }
        // NULL joins NULL, the way the serial ASOF JOIN compares symbol keys: rows above a column top
        // read as NULL too, so this does not depend on the symbol table's null flag
        if (passesMasterKeyFilter(masterKeyRecord, StaticSymbolTable.VALUE_IS_NULL) && passesKeyFilter(keyRecord, StaticSymbolTable.VALUE_IS_NULL)) {
            slaveSymbolLookupMap.put(AsyncWindowJoinFastAtom.NULL_KEY, StaticSymbolTable.VALUE_IS_NULL);
            if (!leanOnly) {
                Unsafe.putInt(masterSlotsAddress, slot);
                Unsafe.putInt(slaveSlotsAddress, slot);
                Unsafe.putInt(slaveToMasterAddress, StaticSymbolTable.VALUE_IS_NULL);
            }
            slot++;
        }
        joinableCount = slot;
        if (!leanOnly) {
            // a slave key that cannot join takes the span state's spare slot: the span scan then
            // stores every row without a branch on the key
            for (long i = 0; i < slaveSlotCount; i++) {
                final long address = slaveSlotsAddress + 4 * i;
                if (Unsafe.getInt(address) < 0) {
                    Unsafe.putInt(address, joinableCount);
                }
            }
        }

        if (!leanOnly) {
            try {
                final long stateLongs = 2L * getSpanSlotCount() + ((getSpanSlotCount() + 1) >>> 1);
                reserveSpanState(ownerSpanState, memoryTracker, stateLongs);
                for (int i = 0, n = perWorkerSpanStates.size(); i < n; i++) {
                    reserveSpanState(perWorkerSpanStates.getQuick(i), memoryTracker, stateLongs);
                }
                for (int i = 0; i < slotEpochs.length; i++) {
                    slotEpochs[i] = 0;
                }
            } catch (CairoException e) {
                if (!e.isOutOfMemory()) {
                    throw e;
                }
                ownerSpanState.close();
                for (int i = 0, n = perWorkerSpanStates.size(); i < n; i++) {
                    perWorkerSpanStates.getQuick(i).close();
                }
                leanOnly = true;
            }
        }
        // the prevailing scans look slave keys up in the dense map when there is one
        final long denseAddress = leanOnly ? 0 : slaveToMasterAddress;
        ownerPrevailingCache.setDenseLookup(denseAddress, slaveSlotCount);
        for (int i = 0, n = perWorkerPrevailingCaches.size(); i < n; i++) {
            perWorkerPrevailingCaches.getQuick(i).setDenseLookup(denseAddress, slaveSlotCount);
        }
        // sized by the joinable keys; allocates only when a lookup first needs a block
        prevailingSummaries.of(sharedState.getFrameCount(), slaveSymbolLookupMap, memoryTracker);
        this.sharedState = sharedState;
        statWalkAborts.set(0);
        final int frameCacheLength = 4 * sharedState.getFrameCount();
        for (int i = 0, n = slotEpochs.length; i < n; i++) {
            long[] cache = walkFrameCaches.getQuiet(i);
            if (cache == null || cache.length < frameCacheLength) {
                cache = new long[frameCacheLength];
                walkFrameCaches.extendAndSet(i, cache);
            } else {
                java.util.Arrays.fill(cache, 0, frameCacheLength, 0);
            }
        }
    }

    /**
     * True when frames should try the walk first: it has not been abandoned for the query often.
     */
    public boolean isWalkWorthTrying() {
        final long aborts = statWalkAborts.get();
        return aborts < 2 || aborts * 4 <= statFramesWalk.get();
    }

    public byte getSlaveFrameFormat(int frameIndex) {
        return sharedState.getAddressCache().getFrameFormat(frameIndex);
    }

    public int getSlaveFrameCount() {
        return sharedState.getFrameCount();
    }

    /**
     * Per slave time frame, 4 longs: row count (0 = not yet known), timestamp address, key address,
     * spare. A slot's own, so the walk fills it without coordination.
     */
    public long[] getWalkFrameCache(int slotId) {
        return walkFrameCaches.getQuick(slotId + 1);
    }

    public void recordFrameWalk() {
        statFramesWalk.incrementAndGet();
    }

    public void recordWalkAbort() {
        statWalkAborts.incrementAndGet();
    }

    @TestOnly
    public long getStatFramesWalk() {
        return statFramesWalk.get();
    }

    @TestOnly
    public long getStatWalkAborts() {
        return statWalkAborts.get();
    }

    public boolean isLeanOnly() {
        return leanOnly;
    }

    public boolean isSkipJoin() {
        return skipJoin;
    }

    /**
     * The joinable slot of a master symbol key, -1 when it cannot join. Not valid in lean mode.
     */
    public int masterSlotOf(int masterKey) {
        // NULL (Integer.MIN_VALUE) goes to index 0, key k to k + 1
        final int index = Math.max(masterKey + 1, 0);
        return index < masterSlotCount ? Unsafe.getInt(masterSlotsAddress + 4L * index) : -1;
    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    /**
     * Starts a frame's span scan on a slot: the epoch its span state entries are tagged with.
     */
    public int nextEpoch(int slotId) {
        final int index = slotId + 1;
        int epoch = slotEpochs[index] + 1;
        if (epoch == 0) {
            // wrapped: untag every entry
            final long address = getSpanStateAddress(slotId);
            Vect.memset(address + 16L * getSpanSlotCount(), 4L * getSpanSlotCount(), 0);
            epoch = 1;
        }
        slotEpochs[index] = epoch;
        return epoch;
    }

    public void recordFrameLean() {
        statFramesLean.incrementAndGet();
    }

    public void recordFrameSpan() {
        statFramesSpan.incrementAndGet();
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        // the maps and arrays are bound to the tracker and reopened by initTimeFrameCursors()
    }

    public void setSkipJoin(boolean skipJoin) {
        this.skipJoin = skipJoin;
    }

    public boolean shouldUseLateMaterialization(int slotId, boolean isParquetFrame) {
        if (!isParquetFrame) {
            return false;
        }
        if (filterUsedColumnIndexes == null || filterUsedColumnIndexes.size() == 0) {
            return false;
        }
        return getSelectivityStats(slotId).shouldUseLateMaterialization();
    }

    /**
     * The joinable slot of a slave symbol key, -1 when it cannot join. Not valid in lean mode.
     */
    public int slaveSlotOf(int slaveKey) {
        final int index = Math.max(slaveKey + 1, 0);
        final int slot = index < slaveSlotCount ? Unsafe.getInt(slaveSlotsAddress + 4L * index) : -1;
        return slot < joinableCount ? slot : -1;
    }

    public long getSlaveSlotsAddress() {
        return slaveSlotsAddress;
    }

    public int getSlaveSlotCount() {
        return slaveSlotCount;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (toleranceInterval != Numbers.LONG_NULL) {
            sink.attr("tolerance").val(toleranceInterval);
        }
    }

    public void toTop() {
        ownerSlaveTimeFrameHelper.toTop();
        for (int i = 0, n = perWorkerSlaveTimeFrameHelpers.size(); i < n; i++) {
            perWorkerSlaveTimeFrameHelpers.getQuick(i).toTop();
        }
    }

    private static void reserveSpanState(DirectLongList state, MemoryTracker memoryTracker, long longs) {
        state.close();
        state.setMemoryTracker(memoryTracker);
        state.setCapacity(Math.max(1, longs));
        // epochs start untagged
        Vect.memset(state.getAddress(), 8L * Math.max(1, longs), 0);
    }

    private boolean passesMasterKeyFilter(@Nullable SlaveKeyRecord keyRecord, int masterKey) {
        if (keyRecord == null) {
            return true;
        }
        keyRecord.of(masterKey);
        return ownerMasterFilter.getBool(keyRecord);
    }

    private boolean passesKeyFilter(@Nullable SlaveKeyRecord keyRecord, int slaveKey) {
        if (keyRecord == null) {
            return true;
        }
        keyRecord.of(slaveKey);
        return slaveKeyFilter.getBool(keyRecord);
    }

    /**
     * The slave's symbol tables as a filter on the key column alone sees them: the key column at
     * the index the filter reads it at.
     */
    private record KeyColumnSymbolTableSource(SymbolTableSource source, int filterColumnIndex,
                                              int keyColumnIndex) implements SymbolTableSource {
        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex != filterColumnIndex) {
                throw CairoException.nonCritical().put("slave key filter reads a column other than the key [column=").put(columnIndex).put(']');
            }
            return source.getSymbolTable(keyColumnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex != filterColumnIndex) {
                throw CairoException.nonCritical().put("slave key filter reads a column other than the key [column=").put(columnIndex).put(']');
            }
            return source.newSymbolTable(keyColumnIndex);
        }
    }

    /**
     * A row as a filter on the key column alone sees it: only the key column is readable.
     */
    private static class SlaveKeyRecord implements Record {
        private final int columnIndex;
        private final StaticSymbolTable symbolTable;
        private int key;

        SlaveKeyRecord(StaticSymbolTable symbolTable, int columnIndex) {
            this.symbolTable = symbolTable;
            this.columnIndex = columnIndex;
        }

        @Override
        public int getInt(int col) {
            assert col == columnIndex;
            return key;
        }

        @Override
        public CharSequence getSymA(int col) {
            assert col == columnIndex;
            return symbolTable.valueOf(key);
        }

        @Override
        public CharSequence getSymB(int col) {
            assert col == columnIndex;
            return symbolTable.valueBOf(key);
        }

        void of(int key) {
            this.key = key;
        }
    }
}
