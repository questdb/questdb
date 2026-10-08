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
 * Two ways to key the join, chosen when the cursor opens:
 * <ul>
 * <li><b>Parallel</b>: every master key whose value the slave's symbol table holds (and, with a slave
 * key filter, whose slave key passes it) gets a dense "joinable slot". Two shared int arrays map the
 * master and the slave symbol keys to their slot, -1 for a key that cannot join; NULL maps to NULL.
 * Building them reads every symbol of both tables, so the join takes this way only when the master
 * has enough rows to repay it ({@link #EAGER_ROWS_PER_KEY}) or the symbol tables are small.</li>
 * <li><b>Serial</b>: the query's thread joins the frames, in order, as it collects them (the workers
 * still filter the master). A master key is translated to its slave key the first time a frame meets
 * it, and the per-key state is keyed by the slave key, so the join holds the keys it meets and
 * nothing more. Taken for a master that is small against its symbol tables, and for the rest of a
 * query whose per-worker state the memory tracker refused: one state, as the serial ASOF JOIN
 * holds.</li>
 * </ul>
 * Per slot (and for the query's thread), the span scan keeps per key the last slave row id met so
 * far in the page frame's slave span, in an {@link AsyncAsOfJoinKeyTable}: allocated by the slot's
 * first frame, grown with the keys met, tagged with the frame's epoch so that nothing has to be
 * cleared between frames. All of it is charged to the query's memory tracker.
 */
public class AsyncAsOfJoinAtom implements StatefulAtom, PerWorkerLockOwner, Reopenable, Plannable {
    // the parallel way's key arrays are built when the master has at least this many rows per key
    // of the two symbol tables, or when the tables have at most EAGER_MIN_KEYS keys between them
    public static final int EAGER_MIN_KEYS = 4096;
    public static final int EAGER_ROWS_PER_KEY = 4;
    public static final int GATHER_NONE = -1;
    @TestOnly
    public static final int KEYS_AUTO = 0;
    @TestOnly
    public static final int KEYS_PARALLEL = 1;
    @TestOnly
    public static final int KEYS_SERIAL = 2;
    // tests force one way of keying the join
    @TestOnly
    public static volatile int KEYS_MODE = KEYS_AUTO;
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
    private final AsyncAsOfJoinKeyTable ownerKeyTable = new AsyncAsOfJoinKeyTable();
    private final ConcurrentTimeFrameCursor ownerSlaveTimeFrameCursor;
    private final WindowJoinTimeFrameHelper ownerSlaveTimeFrameHelper;
    private final ObjList<AsyncAsOfJoinKeyTable> perWorkerKeyTables;
    private ObjList<Function> perWorkerMasterFilters;
    private final ObjList<WindowJoinPrevailingCache> perWorkerPrevailingCaches;
    private final ObjList<SelectivityStats> perWorkerSelectivityStats;
    private final ObjList<ConcurrentTimeFrameCursor> perWorkerSlaveTimeFrameCursors;
    private final ObjList<WindowJoinTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    private final PerWorkerLocks perWorkerLocks;
    private final WindowJoinPrevailingSummaries prevailingSummaries;
    // the serial way: the prevailing scans in slave keys, master key -> slave key as met
    private final WindowJoinPrevailingCache serialPrevailingCache;
    private final AsyncAsOfJoinKeyTable serialTranslations = new AsyncAsOfJoinKeyTable();
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
    private final AtomicLong statFramesSerial = new AtomicLong();
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
    private StaticSymbolTable masterSymbolTable;
    private int masterSlotCount;
    private long masterSlotsAddress;
    private MemoryTracker memoryTracker;
    // the query's thread joins the frames: chosen when the cursor opens, or set by a worker whose
    // per-key state the memory tracker refused
    private volatile boolean serial;
    private @Nullable SlaveKeyRecord serialKeyRecord;
    private boolean skipJoin;
    private int slaveSlotCount;
    private long slaveSlotsAddress;
    private StaticSymbolTable slaveSymbolTable;
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
            this.perWorkerKeyTables = new ObjList<>(slotCount);
            // no summaries: they are sized by the joinable keys, which the serial way does not list
            this.serialPrevailingCache = new WindowJoinPrevailingCache();
            serialPrevailingCache.setIdentityLookup(true);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveTimeFrameCursors.extendAndSet(i, slaveFactory.newTimeFrameCursor());
                perWorkerSlaveTimeFrameHelpers.extendAndSet(i, new WindowJoinTimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTsScale));
                final WindowJoinPrevailingCache cache = new WindowJoinPrevailingCache();
                cache.setSummaries(prevailingSummaries);
                perWorkerPrevailingCaches.extendAndSet(i, cache);
                perWorkerSelectivityStats.extendAndSet(i, new SelectivityStats());
                perWorkerKeyTables.extendAndSet(i, new AsyncAsOfJoinKeyTable());
            }
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
        failure = Misc.freeBestEffort(failure, ownerKeyTable);
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, perWorkerKeyTables);
        failure = Misc.freeBestEffort(failure, serialTranslations);
        failure = Misc.freeBestEffort(failure, ownerPrevailingCache);
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, perWorkerPrevailingCaches);
        failure = Misc.freeBestEffort(failure, serialPrevailingCache);
        failure = Misc.freeBestEffort(failure, prevailingSummaries);
        failure = Misc.clearBestEffort(failure, ownerSelectivityStats);
        failure = Misc.clearObjListBestEffort(failure, perWorkerSelectivityStats);
        masterSymbolTable = null;
        slaveSymbolTable = null;
        serialKeyRecord = null;
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
        failure = Misc.freeBestEffort(failure, ownerKeyTable);
        failure = Misc.freeObjListBestEffort(failure, perWorkerKeyTables);
        failure = Misc.freeBestEffort(failure, serialTranslations);
        failure = Misc.freeBestEffort(failure, ownerPrevailingCache);
        failure = Misc.freeObjListBestEffort(failure, perWorkerPrevailingCaches);
        failure = Misc.freeBestEffort(failure, serialPrevailingCache);
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
     * The per-key state of a slot (-1: the query's thread), keyed by the span slot in the parallel
     * way (see {@link #getSpanSlotCount()}) and by the slave key + 1 (NULL at 0) in the serial way.
     */
    public AsyncAsOfJoinKeyTable getKeyTable(int slotId) {
        return slotId == -1 ? ownerKeyTable : perWorkerKeyTables.getQuick(slotId);
    }

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    public WindowJoinPrevailingCache getSerialPrevailingCache() {
        return serialPrevailingCache;
    }

    @TestOnly
    public long getStatFramesSerial() {
        return statFramesSerial.get();
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
     * Binds the slave time frame cursors and chooses the way the join is keyed (see the class
     * comment); the parallel way also computes the joinable slots. Runs on the query's thread when
     * the cursor reads its first row, before any frame is dispatched.
     *
     * @param masterRowCount the rows of the master's page frames, before the master filter
     */
    public void initTimeFrameCursors(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor pageFrameCursor,
            ConcurrentTimeFrameState sharedState,
            long masterRowCount
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
        serialPrevailingCache.setMemoryTracker(memoryTracker);
        serialPrevailingCache.reopen();
        serialTranslations.of(memoryTracker, 0);
        // one epoch for the query: a translation holds for all of it
        serialTranslations.nextEpoch();

        if (slaveKeyFilter != null) {
            // the filter reads the key column at its index in the filter's own metadata
            slaveKeyFilter.init(new KeyColumnSymbolTableSource(pageFrameCursor, slaveKeyFilterColumnIndex, slaveSymbolIndex), executionContext);
        }

        masterSymbolTable = (StaticSymbolTable) masterSymbolTableSource.getSymbolTable(masterSymbolIndex);
        slaveSymbolTable = pageFrameCursor.getSymbolTable(slaveSymbolIndex);
        serialKeyRecord = slaveKeyFilter != null ? new SlaveKeyRecord(slaveSymbolTable, slaveKeyFilterColumnIndex) : null;
        final int masterCount = masterSymbolTable.getSymbolCount();
        final int slaveCount = slaveSymbolTable.getSymbolCount();
        statFramesSerial.set(0);
        statFramesSpan.set(0);
        statFramesWalk.set(0);
        statWalkAborts.set(0);
        joinableCount = 0;
        masterSlotCount = 0;
        slaveSlotCount = 0;
        final int keysMode = KEYS_MODE;
        final long keyCount = (long) masterCount + slaveCount;
        boolean serial = keysMode == KEYS_SERIAL
                || (keysMode == KEYS_AUTO && keyCount > EAGER_MIN_KEYS && keyCount > masterRowCount / EAGER_ROWS_PER_KEY);
        if (!serial) {
            try {
                buildJoinableSlots(memoryTracker, masterCount, slaveCount);
            } catch (CairoException e) {
                if (!e.isOutOfMemory()) {
                    throw e;
                }
                // the memory tracker refused the key arrays: join serially, holding only the keys met
                serial = true;
            }
        }
        if (serial) {
            slotArrays.close();
            slaveSymbolLookupMap.clear();
            masterSlotsAddress = 0;
            slaveSlotsAddress = 0;
            slaveToMasterAddress = 0;
            masterSlotCount = 0;
            slaveSlotCount = 0;
            joinableCount = 0;
        }
        // the per-key state is allocated by each slot's first frame; the parallel way keys it by the
        // span slots, dense, the serial way by the slave keys met
        final int denseKeyCount = serial ? 0 : getSpanSlotCount();
        ownerKeyTable.of(memoryTracker, denseKeyCount);
        for (int i = 0, n = perWorkerKeyTables.size(); i < n; i++) {
            perWorkerKeyTables.getQuick(i).of(memoryTracker, denseKeyCount);
        }
        // the prevailing scans look slave keys up in the dense map when there is one
        final long denseAddress = serial ? 0 : slaveToMasterAddress;
        ownerPrevailingCache.setDenseLookup(denseAddress, slaveSlotCount);
        for (int i = 0, n = perWorkerPrevailingCaches.size(); i < n; i++) {
            perWorkerPrevailingCaches.getQuick(i).setDenseLookup(denseAddress, slaveSlotCount);
        }
        // sized by the joinable keys; allocates only when a lookup first needs a block
        prevailingSummaries.of(sharedState.getFrameCount(), slaveSymbolLookupMap, memoryTracker);
        this.sharedState = sharedState;
        final int frameCacheLength = 4 * sharedState.getFrameCount();
        for (int i = 0, n = perWorkerKeyTables.size() + 1; i < n; i++) {
            long[] cache = walkFrameCaches.getQuiet(i);
            if (cache == null || cache.length < frameCacheLength) {
                cache = new long[frameCacheLength];
                walkFrameCaches.extendAndSet(i, cache);
            } else {
                java.util.Arrays.fill(cache, 0, frameCacheLength, 0);
            }
        }
        this.serial = serial;
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

    /**
     * True when the query's thread joins the frames, see the class comment.
     */
    public boolean isSerial() {
        return serial;
    }

    public boolean isSkipJoin() {
        return skipJoin;
    }

    /**
     * The joinable slot of a master symbol key, -1 when it cannot join. The parallel way only.
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

    public void recordFrameSerial() {
        statFramesSerial.incrementAndGet();
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

    /**
     * From here on the query's thread joins the frames: a worker's per-key state hit the query's
     * memory limit. The frames a worker has joined stay joined.
     */
    public void switchToSerial() {
        serial = true;
    }

    /**
     * The serial way: the slave key a master key joins, {@link StaticSymbolTable#VALUE_NOT_FOUND}
     * when it cannot join (the slave's symbol table does not hold its value, or the slave key filter
     * refuses it). NULL joins NULL. Resolved once per key and query. The query's thread only.
     */
    public int translateSerial(int masterKey) {
        final int index = Math.max(masterKey + 1, 0);
        long e = serialTranslations.find(index);
        if (e != 0) {
            return (int) Unsafe.getLong(e + 8);
        }
        int slaveKey = masterKey == StaticSymbolTable.VALUE_IS_NULL
                ? StaticSymbolTable.VALUE_IS_NULL
                : slaveSymbolTable.keyOf(masterSymbolTable.valueOf(masterKey));
        if (slaveKey != StaticSymbolTable.VALUE_NOT_FOUND && !passesKeyFilter(serialKeyRecord, slaveKey)) {
            slaveKey = StaticSymbolTable.VALUE_NOT_FOUND;
        }
        e = serialTranslations.entry(index);
        Unsafe.putLong(e + 8, slaveKey);
        return slaveKey;
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
     * The joinable slot of a slave symbol key, -1 when it cannot join. The parallel way only.
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

    // the parallel way: the dense joinable slots of every key of both symbol tables
    private void buildJoinableSlots(MemoryTracker memoryTracker, int masterCount, int slaveCount) {
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
                Unsafe.putInt(masterSlotsAddress + 4L * (masterKey + 1), slot);
                Unsafe.putInt(slaveSlotsAddress + 4L * (slaveKey + 1), slot);
                Unsafe.putInt(slaveToMasterAddress + 4L * (slaveKey + 1), masterKey);
                slot++;
            }
        }
        // NULL joins NULL, the way the serial ASOF JOIN compares symbol keys: rows above a column top
        // read as NULL too, so this does not depend on the symbol table's null flag
        if (passesMasterKeyFilter(masterKeyRecord, StaticSymbolTable.VALUE_IS_NULL) && passesKeyFilter(keyRecord, StaticSymbolTable.VALUE_IS_NULL)) {
            slaveSymbolLookupMap.put(AsyncWindowJoinFastAtom.NULL_KEY, StaticSymbolTable.VALUE_IS_NULL);
            Unsafe.putInt(masterSlotsAddress, slot);
            Unsafe.putInt(slaveSlotsAddress, slot);
            Unsafe.putInt(slaveToMasterAddress, StaticSymbolTable.VALUE_IS_NULL);
            slot++;
        }
        joinableCount = slot;
        // a slave key that cannot join takes the span state's spare slot: the span scan then stores
        // every row without a branch on the key
        for (long i = 0; i < slaveSlotCount; i++) {
            final long address = slaveSlotsAddress + 4 * i;
            if (Unsafe.getInt(address) < 0) {
                Unsafe.putInt(address, joinableCount);
            }
        }
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
