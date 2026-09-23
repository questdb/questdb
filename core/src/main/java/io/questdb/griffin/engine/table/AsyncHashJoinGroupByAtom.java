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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslatingRecord;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Owns execution backing; functions, both filter contexts, the build ON filter and the build
 * scan are borrowed from the factory. Each acquired slot owns every mutable
 * probe/record/decoder/aggregate view, and each slot's probe owns the reader it reads build
 * payload columns through. init() builds on the owner once the probe frame cursor is open: it
 * walks the build scan's page frames, filters each one and keeps the key and the row id of every
 * row that passes. It binds the SYMBOL key translation there too: one cache per SYMBOL key column,
 * shared by every slot, plus a pair of symbol tables per slot. The build frames stay open until
 * clear(), because probes read payload columns through them and the translation's keyOf()
 * lookups resolve through their symbol tables. The frozen build is published by
 * UnorderedPageFrameSequence before reducers run. clear() requires all reducers to have finished
 * and output consumers to be done.
 */
public final class AsyncHashJoinGroupByAtom implements StatefulAtom, PerWorkerLockOwner {
    private final RecordCursorFactory buildFactory;
    // The build scan's WHERE filter, if any, and the owner pool that walks the build frames.
    private final AsyncFilterContext buildFilterContext;
    private final HashJoinBuildFrames buildFrames;
    // The INT layout's only build key column, -1 when the key sinks stage the key instead.
    private final int buildKeyColumn;
    // The owner's own build-side key sink; null for the INT layout, which stages nothing.
    private final RecordSink buildKeySink;
    // An outer join's ON conditions on build columns alone, which drop build rows; null without them.
    private final Function buildOnFilter;
    // The owner's view of the build frame being appended.
    private final PageFrameMemoryRecord buildRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    // The payload copy's byte bound and probe to build row ratio; see maybeCopyPayload().
    private final long copyMaxSize;
    private final double copyMinProbeRatio;
    private final AsyncFilterContext filterContext;
    private final HashJoinGroupByFunctions functions;
    private final boolean isKeyCapacityPresized;
    private final boolean isKeyStaged;
    // The INT layout's lone SYMBOL pair, whose probe keys the reducer translates per row.
    private final boolean isSymbolKey;
    private final boolean outer;
    private final PerWorkerLocks perWorkerLocks;
    // The INT layout's only probe key column, -1 when the key sinks stage the key instead.
    private final int probeKeyColumn;
    private final ObjList<HashJoinGroupByRecord> records = new ObjList<>();
    private final ObjList<Slot> slots = new ObjList<>();
    // One shared translation cache per SYMBOL key column; empty when the key has none.
    private final ObjList<SymbolKeyTranslator> symbolKeyCaches = new ObjList<>();
    private final IntList symbolKeyBuildColumns = new IntList();
    private final IntList symbolKeyProbeColumns = new IntList();
    private boolean buildFiltersInitialized;
    // The frozen build of the open cursor, whichever of the two builds produced it.
    private FrozenHashJoinBuild frozen;
    // Exactly one of the two builds exists, as isKeyStaged says.
    private IntHashJoinBuild intBuild;
    private MapHashJoinBuild mapBuild;
    private boolean isBuildUnique;
    private boolean isPayloadCopied;
    private boolean functionsInitialized;
    private boolean filtersInitialized;
    private long pairsPerCheck;
    private GroupByShardingContext shardingContext;

    AsyncHashJoinGroupByAtom(
            CairoEngine engine,
            RecordCursorFactory buildFactory,
            AsyncFilterContext buildFilterContext,
            Function buildOnFilter,
            HashJoinGroupByMetadata metadata,
            HashJoinGroupByFunctions functions,
            AsyncFilterContext filterContext,
            boolean outer,
            int workerCount
    ) {
        this.buildFactory = buildFactory;
        this.buildFilterContext = buildFilterContext;
        this.buildOnFilter = buildOnFilter;
        this.functions = functions;
        this.filterContext = filterContext;
        this.outer = outer;
        this.isKeyStaged = metadata.isKeyStaged();
        this.isSymbolKey = metadata.isSymbolKey();
        this.isKeyCapacityPresized = metadata.isKeyCapacityPresized();
        this.buildFrames = new HashJoinBuildFrames(engine.getConfiguration(), metadata.getBuildColumns(), buildFactory.getMetadata());
        this.probeKeyColumn = isKeyStaged ? -1 : metadata.getProbeKeyColumn();
        this.buildKeyColumn = isKeyStaged ? -1 : metadata.getBuildKeyColumn();
        symbolKeyProbeColumns.addAll(metadata.getSymbolKeyProbeColumns());
        symbolKeyBuildColumns.addAll(metadata.getSymbolKeyBuildColumns());
        for (int i = 0, n = symbolKeyProbeColumns.size(); i < n; i++) {
            symbolKeyCaches.add(new SymbolKeyTranslator());
        }
        CairoConfiguration configuration = engine.getConfiguration();
        this.copyMaxSize = configuration.getSqlParallelHashJoinGroupByPayloadCopyMaxSize();
        this.copyMinProbeRatio = configuration.getSqlParallelHashJoinGroupByPayloadCopyMinProbeRatio();
        perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        try {
            // Sinks read the borrowed input metadatas when they are instantiated, so every sink
            // this execution will ever need is taken here, while the inputs are still alive.
            buildKeySink = metadata.newBuildKeySink();
            final boolean hasPayload = metadata.getBuildColumns().size() > 0;
            if (isKeyStaged) {
                mapBuild = new MapHashJoinBuild(configuration, metadata.getKeyTypes(), hasPayload,
                        configuration.getSqlSmallMapKeyCapacity(), configuration.getSqlSmallMapPageSize(), 64, true);
            } else {
                intBuild = new IntHashJoinBuild(hasPayload, 64, 64, true);
            }
            if (functions.isKeyed()) {
                ObjList<GroupByFunctionsUpdater> workerUpdaters = new ObjList<>();
                for (int i = 0; i < workerCount; i++) {
                    workerUpdaters.add(functions.getUpdater(i));
                }
                shardingContext = new GroupByShardingContext(configuration, functions.getKeyTypes(),
                        functions.getValueTypes(), functions.getUpdater(-1), workerUpdaters,
                        perWorkerLocks, workerCount);
            }
            final boolean hasSymbolKey = symbolKeyProbeColumns.size() > 0;
            for (int i = -1; i < workerCount; i++) {
                // Sinks hold scratch state, so each slot probes through one of its own, and so
                // does the translating record the staged key sink reads from.
                Slot slot = new Slot(metadata.newRecord(), metadata.newProbeKeySink(),
                        hasSymbolKey && isKeyStaged
                                ? new SymbolKeyTranslatingRecord(metadata.getProbeColumnCount(), symbolKeyProbeColumns)
                                : null,
                        hasSymbolKey && !isKeyStaged ? new SymbolKeyTranslator.View() : null);
                if (!functions.isKeyed()) {
                    slot.value = new SimpleMapValue(functions.getValueTypes().getColumnCount());
                }
                slots.add(slot);
                records.add(slot.joinedRecord);
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        Throwable failure = null;
        if (functionsInitialized) {
            functionsInitialized = false;
            try {
                functions.cursorClosed();
            } catch (Throwable th) {
                failure = th;
            }
        }
        if (filtersInitialized) {
            filtersInitialized = false;
            Function ownerFilter = filterContext.getFilter(-1);
            failure = cursorClosed(failure, ownerFilter);
            for (int i = 0; i < slots.size() - 1; i++) {
                Function workerFilter = filterContext.getFilter(i);
                if (workerFilter != ownerFilter) {
                    failure = cursorClosed(failure, workerFilter);
                }
            }
        }
        if (buildFiltersInitialized) {
            buildFiltersInitialized = false;
            failure = cursorClosed(failure, buildFilterContext.getFilter(-1));
            failure = cursorClosed(failure, buildOnFilter);
        }
        for (int i = 0; i < slots.size(); i++) {
            try {
                slots.getQuick(i).clear(functions.getUpdater(i - 1));
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        try {
            filterContext.clear();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        try {
            buildFilterContext.clear();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        if (shardingContext != null) {
            try {
                shardingContext.clear();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        frozen = null;
        isBuildUnique = false;
        isPayloadCopied = false;
        failure = Misc.freeBestEffort(failure, intBuild);
        failure = Misc.freeBestEffort(failure, mapBuild);
        // The slots released their symbol tables above, so the shared caches go next.
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, symbolKeyCaches);
        buildRecord.of(null);
        // Functions, slots, filters and the build have released every symbol table view and
        // payload reader of these frames.
        try {
            buildFrames.clear();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Throwable failure = null;
        try {
            clear();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeObjListBestEffort(failure, slots);
        slots.clear();
        failure = Misc.freeBestEffort(failure, shardingContext);
        shardingContext = null;
        failure = Misc.freeBestEffort(failure, buildRecord);
        CairoException.rethrowCleanupFailure(failure);
    }

    /** The build published for the open cursor, or null when no cursor is open. */
    @TestOnly
    public FrozenHashJoinBuild getFrozenBuild() {
        return frozen;
    }

    public HashJoinGroupByFunctions getFunctions() {
        return functions;
    }

    /**
     * True when the open cursor's probes read a copy of the build's payload columns rather than the
     * columns where they live; see {@link #maybeCopyPayload(long, SqlExecutionCircuitBreaker)}.
     */
    @TestOnly
    public boolean isPayloadCopied() {
        return isPayloadCopied;
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        try {
            assert frozen == null && buildFrames.getSymbolTableSource() == null;
            build(executionContext);
            bindSymbolKeyTranslation(symbolTableSource, executionContext);
            // Join fanout is not bounded by a frame, so reducers check once per page frame of matched pairs.
            pairsPerCheck = Math.max(1, executionContext.getPageFrameMaxRows());
            if (shardingContext != null) {
                shardingContext.setMemoryTracker(executionContext.getMemoryTracker());
                shardingContext.reopen();
            }
            for (int i = 0; i < slots.size(); i++) {
                Slot slot = slots.getQuick(i);
                if (!functions.isKeyed()) {
                    functions.getUpdater(i - 1).updateEmpty(slot.value);
                    slot.value.setNew(true);
                }
                // Casting here, once per slot per execution, keeps the reducers' hot call sites
                // on a single probe implementation; see the monomorphic-reducer rule.
                final FrozenHashJoinBuild.Probe probe;
                if (isKeyStaged) {
                    if (slot.recordProbe == null) {
                        slot.recordProbe = ((FrozenHashJoinBuild.RecordKeyed) frozen).newProbe(slot.probeKeySink);
                    } else {
                        slot.recordProbe.reopen();
                    }
                    probe = slot.recordProbe;
                } else {
                    if (slot.intProbe == null) {
                        slot.intProbe = ((FrozenHashJoinBuild.IntKeyed) frozen).newProbe();
                    } else {
                        slot.intProbe.reopen();
                    }
                    probe = slot.intProbe;
                }
                slot.probeRecord.of(symbolTableSource);
                slot.joinedRecord.of(slot.probeRecord, slot.probeRecord, probe);
            }
            filtersInitialized = true;
            filterContext.initFilters(symbolTableSource, executionContext);
            functionsInitialized = true;
            functions.init(records, executionContext);
        } catch (Throwable th) {
            // The sequence closes the frame cursor when init throws. Release functions
            // while their borrowed symbol sources are still alive, then the build cursor.
            try {
                clear();
            } catch (Throwable cleanup) {
                th.addSuppressed(cleanup);
            }
            throw th;
        }
    }

    private static Throwable addFailure(Throwable failure, Throwable th) {
        if (failure == null) {
            return th;
        }
        if (failure != th) {
            failure.addSuppressed(th);
        }
        return failure;
    }

    private static Throwable cursorClosed(Throwable failure, Function function) {
        if (function != null) {
            try {
                function.cursorClosed();
            } catch (Throwable th) {
                return addFailure(failure, th);
            }
        }
        return failure;
    }

    /**
     * Sizes each SYMBOL key column's shared cache from the probe dictionary and gives every slot
     * its own pair of symbol tables over it. Symbol tables are not thread safe, so a worker
     * cannot share one; the cache it fills is a function of the two dictionaries alone, so every
     * worker does share that. The caller's failure path releases both.
     */
    private void bindSymbolKeyTranslation(SymbolTableSource probeSymbols, SqlExecutionContext executionContext) {
        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        for (int key = 0, keyCount = symbolKeyCaches.size(); key < keyCount; key++) {
            final int probeColumn = symbolKeyProbeColumns.getQuick(key);
            final int buildColumn = symbolKeyBuildColumns.getQuick(key);
            final SymbolKeyTranslator cache = symbolKeyCaches.getQuick(key);
            // The owner's own table answers for the dictionary size the cache is sized by, and
            // then stays as slot -1's, so no table is taken that a slot does not keep.
            final StaticSymbolTable ownerProbeTable = (StaticSymbolTable) probeSymbols.newSymbolTable(probeColumn);
            cache.of(ownerProbeTable.getSymbolCount(), memoryTracker, circuitBreaker);
            for (int i = 0, n = slots.size(); i < n; i++) {
                final SymbolTable probeTable = i == 0 ? ownerProbeTable : probeSymbols.newSymbolTable(probeColumn);
                slots.getQuick(i).getSymbolKeyView(key)
                        .of(cache, probeTable, (StaticSymbolTable) buildFrames.getSymbolTableSource().newSymbolTable(buildColumn));
            }
        }
    }

    /**
     * Walks the build scan's page frames on the owner, keeping the key and the row id of every row
     * that the build filters pass. The frame count is known before the first row, and so is the
     * row count of an unfiltered build, interval scans included. A filtered build stays unknown
     * and grows as it goes: the unfiltered row count only bounds it, and sizing by that bound
     * over-allocates by the filter's selectivity. The caller's failure path closes the build, the
     * filters and the frames.
     */
    private void build(SqlExecutionContext executionContext) throws SqlException {
        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        buildFrames.of(buildFactory, executionContext);
        final SymbolTableSource buildSymbols = buildFrames.getSymbolTableSource();
        buildFiltersInitialized = true;
        buildFilterContext.initFilters(buildSymbols, executionContext);
        if (buildOnFilter != null) {
            buildOnFilter.init(buildSymbols, executionContext);
        }
        buildFilterContext.initMemoryPools(buildFrames.getAddressCache(), memoryTracker);
        buildRecord.of(buildSymbols);
        final boolean isFiltered = buildFilterContext.getFilter(-1) != null || buildOnFilter != null;
        final long rowCountHint = isFiltered ? -1 : buildFrames.getRowCount();
        final long keyCountHint = getKeyCountHint(rowCountHint);
        if (isKeyStaged) {
            mapBuild.open(memoryTracker, circuitBreaker);
            mapBuild.reserve(rowCountHint, keyCountHint);
        } else {
            intBuild.open(memoryTracker, circuitBreaker);
            intBuild.reserve(rowCountHint, keyCountHint);
        }
        final PageFrameMemoryPool pool = buildFilterContext.getMemoryPool(-1);
        final DirectLongList rows = buildFilterContext.getFilteredRows(-1);
        for (int frameIndex = 0, frameCount = buildFrames.getFrameCount(); frameIndex < frameCount; frameIndex++) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
            buildRecord.init(frameMemory);
            final long rowCount = buildFrames.getFrameRowCount(frameIndex);
            try {
                if (isFiltered) {
                    filterBuildFrame(frameMemory, rowCount, rows);
                    // A SYMBOL key keeps the build's own symbol keys; the probe translates into them.
                    if (isKeyStaged) {
                        mapBuild.appendFrame(buildRecord, buildKeySink, rows);
                    } else {
                        intBuild.appendFrame(buildRecord, buildKeyColumn, rows);
                    }
                } else if (isKeyStaged) {
                    mapBuild.appendFrame(buildRecord, buildKeySink, rowCount);
                } else {
                    intBuild.appendFrame(buildRecord, buildKeyColumn, rowCount);
                }
            } finally {
                // Each frame is read once, so a decoded Parquet frame has no later use.
                pool.releaseParquetBuffers();
            }
        }
        // A build without payload columns stores no row ids and never asks the frames for one.
        frozen = isKeyStaged ? mapBuild.freeze(buildFrames) : intBuild.freeze(buildFrames);
        isBuildUnique = frozen.getRowCount() == frozen.getKeyCount();
    }

    /**
     * Leaves in {@code rows} the rows of the frame that the build's WHERE filter and ON filter
     * both pass. The compiled filter reads raw column addresses, so column tops and Parquet type
     * casts, which the record resolves per row, fall back to the interpreted filter.
     */
    private void filterBuildFrame(PageFrameMemory frameMemory, long rowCount, DirectLongList rows) {
        rows.clear();
        final Function filter = buildFilterContext.getFilter(-1);
        if (filter != null) {
            final CompiledFilter compiledFilter = buildFilterContext.getCompiledFilter();
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts()) {
                AsyncFilterUtils.applyFilter(filter, rows, buildRecord, rowCount);
            } else {
                AsyncFilterUtils.applyCompiledFilter(
                        compiledFilter,
                        buildFilterContext.getBindVarMemory(),
                        buildFilterContext.getBindVarFunctions(),
                        frameMemory,
                        buildFrames.getAddressCache(),
                        buildFilterContext.getDataAddresses(-1),
                        buildFilterContext.getAuxAddresses(-1),
                        rows,
                        rowCount
                );
            }
        }
        if (buildOnFilter != null) {
            if (filter == null) {
                AsyncFilterUtils.applyFilter(buildOnFilter, rows, buildRecord, rowCount);
            } else {
                long kept = 0;
                for (long p = 0, n = rows.size(); p < n; p++) {
                    final long row = rows.get(p);
                    buildRecord.setRowIndex(row);
                    if (buildOnFilter.getBool(buildRecord)) {
                        rows.set(kept++, row);
                    }
                }
                rows.setPos(kept);
            }
        }
    }

    /**
     * Distinct keys the key table is presized for, or -1 to let it grow. Rows bound the keys, but
     * only a build whose table is not larger than its probe's sizes by them; see
     * {@link HashJoinGroupByMetadata#isKeyCapacityPresized()}. The INT layout's lone SYMBOL key
     * holds the build's own symbol keys, so its dictionary and the null key bound it as well.
     */
    private long getKeyCountHint(long rowCount) {
        if (!isKeyCapacityPresized || rowCount < 1) {
            return -1;
        }
        if (isSymbolKey) {
            final StaticSymbolTable symbolTable = (StaticSymbolTable) buildFrames.getSymbolTableSource().getSymbolTable(buildKeyColumn);
            return Math.min(rowCount, symbolTable.getSymbolCount() + 1L);
        }
        return rowCount;
    }

    AsyncFilterContext getFilterContext() {
        return filterContext;
    }

    GroupByMapFragment getFragment(int slot) {
        return shardingContext != null ? shardingContext.getFragment(slot) : null;
    }

    long getPairsPerCheck() {
        return pairsPerCheck;
    }

    int getProbeKeyColumn() {
        return probeKeyColumn;
    }

    Slot getSlot(int slot) {
        return slots.getQuick(slot + 1);
    }

    boolean isBuildUnique() {
        return isBuildUnique;
    }

    /** True when the probe stages its key through a {@link RecordSink} rather than reading an INT. */
    boolean isKeyStaged() {
        return isKeyStaged;
    }

    boolean isOuter() {
        return outer;
    }

    /** True for the INT layout's lone SYMBOL pair, whose probe key translates before every lookup. */
    boolean isSymbolKey() {
        return isSymbolKey;
    }

    int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker breaker) {
        return workerId == -1 && owner ? -1 : perWorkerLocks.acquireSlot(workerId, breaker);
    }

    GroupByShardingContext getShardingContext() {
        return shardingContext;
    }

    public boolean isSharded() {
        return shardingContext != null && shardingContext.isSharded();
    }

    SimpleMapValue mergeScalar() {
        SimpleMapValue dest = getSlot(-1).value;
        GroupByFunctionsUpdater updater = functions.getUpdater(-1);
        for (int i = 1; i < slots.size(); i++) {
            SimpleMapValue src = slots.getQuick(i).value;
            if (!src.isNew()) {
                if (dest.isNew()) {
                    dest.copy(src);
                } else {
                    updater.merge(dest, src);
                }
                dest.setNew(false);
            }
        }
        return dest;
    }

    void release(int slot) {
        perWorkerLocks.releaseSlot(slot);
    }

    /** True when some execution may copy the build's payload columns, as EXPLAIN reports. */
    boolean canCopyPayload() {
        return buildFrames.getCopyRowSize() > 0 && copyMaxSize >= buildFrames.getCopyRowSize();
    }

    /**
     * True when this build copies its payload columns for a probe of this many rows: a payload that
     * the copy can hold, a copy within the byte bound, and a probe that holds at least the configured
     * ratio of the build's rows, so that each build row serves more than one match on average. The
     * probe's count is its frame rows, before any row filter, so a selective probe filter can copy a
     * build it need not; the byte bound caps what that costs.
     */
    static boolean isPayloadCopyWorthIt(long buildRows, int copyRowSize, long probeRows, long copyMaxSize, double copyMinProbeRatio) {
        return copyRowSize > 0
                && buildRows > 0
                && buildRows <= copyMaxSize / copyRowSize
                && probeRows >= copyMinProbeRatio * buildRows;
    }

    /**
     * Copies the build's payload columns when the probe is large enough to read each build row more
     * than once; see {@link #isPayloadCopyWorthIt}. The owner calls it once the probe's frames are
     * known and before it dispatches the probes, which then read the copy. On failure the caller
     * closes the cursor, whose clear() releases the copy.
     */
    void maybeCopyPayload(long probeRows, SqlExecutionCircuitBreaker circuitBreaker) {
        assert frozen != null && !isPayloadCopied;
        if (isPayloadCopyWorthIt(frozen.getRowCount(), buildFrames.getCopyRowSize(), probeRows, copyMaxSize, copyMinProbeRatio)) {
            buildFrames.copyPayload(frozen, circuitBreaker);
            isPayloadCopied = true;
        }
    }

    boolean shouldProbe() {
        return outer || frozen.getRowCount() > 0;
    }

    static final class Slot implements QuietCloseable {
        final HashJoinGroupByRecord joinedRecord;
        // What the staged key sink reads: the probe record, or the translating view of it.
        final Record keyRecord;
        // Null for the INT layout; the slot's record probe borrows it for its whole life.
        final RecordSink probeKeySink;
        final ProbeRecord probeRecord = new ProbeRecord();
        // The INT layout's lone SYMBOL key translates through this view; null otherwise.
        @Nullable
        final SymbolKeyTranslator.View symbolKeyView;
        // A staged key with SYMBOL columns translates through this record's views; null otherwise.
        @Nullable
        private final SymbolKeyTranslatingRecord probeKeyRecord;
        // Exactly one of the two probes exists, as the atom's isKeyStaged says.
        FrozenHashJoinBuild.IntProbe intProbe;
        FrozenHashJoinBuild.RecordProbe recordProbe;
        SimpleMapValue value;

        Slot(
                HashJoinGroupByRecord joinedRecord,
                RecordSink probeKeySink,
                @Nullable SymbolKeyTranslatingRecord probeKeyRecord,
                @Nullable SymbolKeyTranslator.View symbolKeyView
        ) {
            this.joinedRecord = joinedRecord;
            this.probeKeySink = probeKeySink;
            this.probeKeyRecord = probeKeyRecord;
            this.symbolKeyView = symbolKeyView;
            if (probeKeyRecord != null) {
                probeKeyRecord.of(probeRecord);
            }
            this.keyRecord = probeKeyRecord != null ? probeKeyRecord : probeRecord;
        }

        @Override
        public void close() {
            Throwable failure = Misc.freeBestEffort(null, value);
            value = null;
            failure = Misc.freeBestEffort(failure, intProbe);
            intProbe = null;
            failure = Misc.freeBestEffort(failure, recordProbe);
            recordProbe = null;
            failure = Misc.freeBestEffort(failure, probeKeyRecord);
            failure = Misc.freeBestEffort(failure, symbolKeyView);
            failure = Misc.freeBestEffort(failure, probeRecord);
            CairoException.rethrowCleanupFailure(failure);
        }

        /** The translation path of one SYMBOL key column, wherever this slot keeps it. */
        SymbolKeyTranslator.View getSymbolKeyView(int key) {
            return probeKeyRecord != null ? probeKeyRecord.getView(key) : symbolKeyView;
        }

        void clear(GroupByFunctionsUpdater updater) {
            if (value != null) {
                updater.updateEmpty(value);
                value.setNew(true);
            }
            joinedRecord.clear();
            probeRecord.of(null);
            // The views borrow this execution's symbol tables and read a cache the atom is
            // about to release, so they drop both here. The objects stay for the next execution.
            if (probeKeyRecord != null) {
                probeKeyRecord.close();
            }
            if (symbolKeyView != null) {
                symbolKeyView.close();
            }
            // A probe may hold native memory charged to this execution's tracker, so it
            // releases here, while that tracker is still the one that charged it. The
            // object stays: reopen() brings it back for the next execution.
            if (intProbe != null) {
                intProbe.close();
            }
            if (recordProbe != null) {
                recordProbe.close();
            }
        }
    }

    private static final class ProbeRecord extends PageFrameMemoryRecord implements SymbolTableSource {
        private SymbolTableSource source;

        ProbeRecord() {
            super(RECORD_A_LETTER);
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return super.getSymbolTable(columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return source.newSymbolTable(columnIndex);
        }

        @Override
        public void of(SymbolTableSource source) {
            super.of(source);
            this.source = source;
        }
    }
}
