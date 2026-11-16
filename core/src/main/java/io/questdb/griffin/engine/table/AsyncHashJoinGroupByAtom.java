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
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Owns execution backing; functions, both filter contexts, the build ON filter and the build
 * scan are borrowed from the factory. The build half of the execution, the build scan, its
 * filters, the hash table and the slots' probes of it, lives in a {@link HashJoinBuildStage} of
 * the atom's own, which dispatches the rounds of a parallel build through the frame sequence that
 * owns the atom: the atom is the sequence's {@link HashJoinBuildHost}. Each acquired slot owns
 * every mutable probe record and aggregate view; the stage's slot of the same index owns the
 * slot's probe of the build and the reader it reads build payload columns through.
 * <p>
 * init() runs when the cursor opens and reads no row: the stage opens the build scan, and init()
 * initializes the filters of both inputs and the functions against the two open scans. Bind
 * variables, dynamic intervals and symbol tables are ready from there on, and every SqlException
 * surfaces from getCursor(). A caller that opens the cursor and reads nothing, such as EXPLAIN or
 * LIMIT 0, pays for no more than that.
 * <p>
 * {@link #build(SqlExecutionCircuitBreaker)} runs once per execution, when the cursor is first
 * asked for a result: the stage walks the build scan's page frames, filters each one and keeps the
 * key and the row id of every row that passes, on the owner or in rounds of the frame sequence's
 * tasks; see {@link HashJoinBuildStage}. build() then has the stage give every slot its probe of
 * the frozen build, which each slot's joined record reads. Once the probe scan has cut its frames,
 * the stage binds the SYMBOL key translation for the probe's rows, see
 * {@link #bindSymbolKeyTranslation(long, SqlExecutionCircuitBreaker)}. The owner dispatches the
 * probe tasks after that, and the first task that UnorderedPageFrameSequence publishes is what
 * publishes the frozen build, its flags, the slots' probes and the translation to the reducers.
 * clear() requires all reducers to have finished and output consumers to be done.
 * <p>
 * A query that only counts the joined pairs of an INNER join reads nothing of a pair, so a build
 * that repeats a key need not hand its rows out one by one: build() has the stage replace each
 * key's rows with their count before any reducer runs, see
 * {@link HashJoinBuildStage#countKeyRows}, and the reducers add a count per probe row; see
 * {@link #isCountedByKey()}. Over a build of one row per key, a probe row that finds its key adds
 * one; see {@link #isCountedByMatch()}.
 */
public final class AsyncHashJoinGroupByAtom implements StatefulAtom, PerWorkerLockOwner, HashJoinBuildHost {
    // The value column of the lone count(*) of a query that only counts the joined pairs, -1 otherwise.
    private final int countValueIndex;
    private final AsyncFilterContext filterContext;
    private final HashJoinGroupByFunctions functions;
    // True when the query reads nothing of a joined pair but that it exists; see isPairCount().
    private final boolean isCountOnly;
    private final boolean isOuter;
    private final PerWorkerLocks perWorkerLocks;
    // This factory's own copy of the per-row aggregation step; see HashJoinGroupByRowUpdater.
    private final HashJoinGroupByRowUpdater rowUpdater = HashJoinGroupByRowUpdater.newInstance();
    private final ObjList<Slot> slots = new ObjList<>();
    // The build half of the execution, which every build round of the frame sequence runs.
    private final HashJoinBuildStage stage;
    // The sequence that owns this atom, through which a parallel build dispatches its rounds.
    private UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence;
    // True when the open cursor's build holds each key's row count in place of the key's rows.
    private boolean isCountedByKey;
    // True when the open cursor's reducers count one pair per probe row that finds its key.
    private boolean isCountedByMatch;
    private boolean isFiltersInitialized;
    private boolean isFunctionsInitialized;
    private long pairsPerCheck;
    private GroupByShardingContext shardingContext;

    AsyncHashJoinGroupByAtom(
            CairoEngine engine,
            RecordCursorFactory buildFactory,
            AsyncFilterContext buildFilterContext,
            Function buildOnFilter,
            @Nullable ObjList<Function> workerBuildOnFilters,
            HashJoinGroupByMetadata metadata,
            HashJoinGroupByFunctions functions,
            AsyncFilterContext filterContext,
            boolean isOuter,
            int workerCount
    ) {
        this.functions = functions;
        this.filterContext = filterContext;
        this.isOuter = isOuter;
        // An outer join counts a probe row without a match too, which no key's row count covers.
        this.isCountOnly = !isOuter && isPairCount(functions);
        this.countValueIndex = isCountOnly ? functions.getGroupByFunctions(-1).getQuick(0).getValueIndex() : -1;
        CairoConfiguration configuration = engine.getConfiguration();
        perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        try {
            // The stage takes every key sink this execution will ever need, while the inputs, whose
            // metadata the sinks read when they are instantiated, are still alive.
            stage = new HashJoinBuildStage(
                    configuration,
                    buildFactory,
                    buildFilterContext,
                    buildOnFilter,
                    workerBuildOnFilters,
                    metadata.getKeyLayout(),
                    metadata.getBuildColumns(),
                    metadata.getProbeColumnCount(),
                    metadata.isKeyCapacityPresized(),
                    workerCount
            );
            if (functions.isKeyed()) {
                ObjList<GroupByFunctionsUpdater> workerUpdaters = new ObjList<>();
                for (int i = 0; i < workerCount; i++) {
                    workerUpdaters.add(functions.getUpdater(i));
                }
                shardingContext = new GroupByShardingContext(configuration, functions.getKeyTypes(),
                        functions.getValueTypes(), functions.getUpdater(-1), workerUpdaters,
                        perWorkerLocks, workerCount);
            }
            // The metadata does not outlive construction, so the slots share a copy of its mapping.
            final IntList payloadBuildColumns = new IntList();
            payloadBuildColumns.addAll(metadata.getBuildColumns());
            for (int i = -1; i < workerCount; i++) {
                Slot slot = new Slot(metadata.newRecord(), metadata.getProbeColumnCount(), payloadBuildColumns);
                // The slot's staged probe key reads its probe record.
                stage.getSlot(i).ofKeySource(slot.probeRecord);
                if (!functions.isKeyed()) {
                    slot.value = new SimpleMapValue(functions.getValueTypes().getColumnCount());
                }
                slots.add(slot);
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    /**
     * See {@link HashJoinBuildStage#isParquetInProbeReach(long, long, long, long)}.
     */
    public static boolean isParquetInProbeReach(long probeRows, long buildRows, long buildKeyCount, long parquetFrameCount) {
        return HashJoinBuildStage.isParquetInProbeReach(probeRows, buildRows, buildKeyCount, parquetFrameCount);
    }

    @Override
    public void clear() {
        Throwable failure = null;
        if (isFunctionsInitialized) {
            isFunctionsInitialized = false;
            try {
                functions.cursorClosed();
            } catch (Throwable th) {
                failure = th;
            }
        }
        if (isFiltersInitialized) {
            isFiltersInitialized = false;
            Function ownerFilter = filterContext.getFilter(-1);
            failure = cursorClosed(failure, ownerFilter);
            for (int i = 0; i < slots.size() - 1; i++) {
                Function workerFilter = filterContext.getFilter(i);
                if (workerFilter != ownerFilter) {
                    failure = cursorClosed(failure, workerFilter);
                }
            }
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
        if (shardingContext != null) {
            try {
                shardingContext.clear();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        isCountedByKey = false;
        isCountedByMatch = false;
        // The functions and the slots have released every symbol table view of the build scan, so
        // the stage may close it, after its build filters, its slots and its build.
        failure = Misc.clearBestEffort(failure, stage);
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
        failure = Misc.freeBestEffort(failure, stage);
        frameSequence = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Page frames of the open cursor's build input, zero until the cursor builds.
     */
    @TestOnly
    public int getBuildFrameCount() {
        return stage.getBuildFrameCount();
    }

    /**
     * Native bytes that the slots' build key stagers hold: none once the frame tasks of a parallel
     * build are done, and none for the INT layout, which stages no key.
     */
    @TestOnly
    public long getBuildKeyStagerSizeInBytes() {
        return stage.getBuildKeyStagerSizeInBytes();
    }

    /**
     * Hash partitions of the open cursor's build: one for a build on the owner.
     */
    @TestOnly
    public int getBuildPartitionCount() {
        return stage.getPartitionCount();
    }

    /**
     * The atom's one stage, which every build round of its frame sequence runs.
     */
    @Override
    public HashJoinBuildStage getBuildingStage() {
        return stage;
    }

    /**
     * The build published for the open cursor, or null when no cursor is open, or when the open
     * cursor has not been asked for a result yet; see {@link #build(SqlExecutionCircuitBreaker)}.
     */
    @TestOnly
    public FrozenHashJoinBuild getFrozenBuild() {
        return stage.getFrozenBuild();
    }

    public HashJoinGroupByFunctions getFunctions() {
        return functions;
    }

    /**
     * The class of this factory's row updater, which no other factory shares.
     */
    @TestOnly
    public Class<?> getRowUpdaterClass() {
        return rowUpdater.getClass();
    }

    /**
     * True when the open cursor's build ran on the workers with its frames' rows written straight
     * into the frames' regions of the heap, as a build whose frames keep every row does; see
     * {@link IntHashJoinBuild#beginUnfilteredPartitioning}.
     */
    @TestOnly
    public boolean hasBuildFrameRegions() {
        return stage.hasBuildFrameRegions();
    }

    /**
     * True when the open cursor's build ran on the workers, in rounds of the frame sequence's tasks.
     */
    @TestOnly
    public boolean isBuiltInRounds() {
        return stage.isBuiltInRounds();
    }

    /**
     * True when the open cursor's build holds each key's row count in place of the key's rows: the
     * query only counts the joined pairs and the build repeats a key. The reducers then add a
     * key's count per probe row and visit no pair; see {@link FrozenHashJoinBuild#countKeyRows()}.
     * A build of one row per key keeps its rows, whose lookup finds the count already; see
     * {@link #isCountedByMatch()}.
     */
    public boolean isCountedByKey() {
        return isCountedByKey;
    }

    /**
     * True when the query only counts the joined pairs and the open cursor's build holds one row per
     * key, as an empty build does too: each probe row then adds one pair when the build holds its
     * key, and the reducers pass no pair through the row updater.
     */
    public boolean isCountedByMatch() {
        return isCountedByMatch;
    }

    /**
     * True when the open cursor's build counted its key rows on the workers, in a round of the frame
     * sequence's tasks; see {@link HashJoinBuildStage#isCountedInRounds()}.
     */
    @TestOnly
    public boolean isCountedInRounds() {
        return stage.isCountedInRounds();
    }

    /**
     * True when the open cursor's probes read a copy of the build's payload columns rather than the
     * columns where they live; see {@link #maybeCopyPayload(long, SqlExecutionCircuitBreaker)}.
     */
    @TestOnly
    public boolean isPayloadCopied() {
        return stage.isPayloadCopied();
    }

    /**
     * True when the open cursor's translation cache of the key-th SYMBOL key pair was filled from
     * the build dictionary rather than opened unresolved; see {@link SymbolKeyTranslator}. False
     * until the cursor builds, and for a factory without that many SYMBOL key pairs.
     */
    @TestOnly
    public boolean isSymbolKeyCacheFilledFromBuild(int key) {
        return stage.isSymbolKeyCacheFilledFromBuild(key);
    }

    /**
     * True when the open cursor's execution translates the key-th SYMBOL key pair through each
     * slot's own cache; see {@link HashJoinBuildStage#isSymbolKeySlotCached(int)}.
     */
    @TestOnly
    public boolean isSymbolKeySlotCached(int key) {
        return stage.isSymbolKeySlotCached(key);
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    /**
     * Opens the build scan and initializes the filters and the functions, and reads no row of
     * either input: {@link #build(SqlExecutionCircuitBreaker)} does the work that grows with the
     * build, when the cursor is first asked for a result. Everything that can throw a
     * SqlException runs here, so that it surfaces from getCursor().
     */
    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        try {
            // The open scan serves the build input's symbol tables; build() walks its frames.
            stage.init(symbolTableSource, executionContext);
            final SymbolTableSource buildSymbols = stage.getBuildSymbols();
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
                slot.bindSymbols(symbolTableSource, buildSymbols);
            }
            isFiltersInitialized = true;
            filterContext.initFilters(symbolTableSource, executionContext);
            isFunctionsInitialized = true;
            // A slot resolves a function's symbol tables through the two open scans, so the
            // functions need no frozen build and parents can resolve output symbols at once.
            functions.init(slots, executionContext);
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

    @Override
    public int maybeAcquire(int workerId, boolean isOwner, SqlExecutionCircuitBreaker breaker) {
        return workerId == -1 && isOwner ? -1 : perWorkerLocks.acquireSlot(workerId, breaker);
    }

    @Override
    public void release(int slot) {
        perWorkerLocks.releaseSlot(slot);
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
     * True when the functions only count the joined pairs: no grouping key, no post-join filter and
     * count(*) as the lone aggregate, which reads no column of either input. The number of pairs
     * is then all the query takes from the join.
     */
    private static boolean isPairCount(HashJoinGroupByFunctions functions) {
        final ObjList<GroupByFunction> groupByFunctions = functions.getGroupByFunctions(-1);
        return !functions.isKeyed()
                && functions.getFilter(-1) == null
                && groupByFunctions.size() == 1
                && groupByFunctions.getQuick(0).getClass() == CountLongConstGroupByFunction.class;
    }

    /**
     * Adds a frame's joined pairs to the slot's count(*), as the row updater's update per pair does.
     */
    void addPairCount(Slot slot, long pairCount) {
        if (pairCount > 0) {
            slot.value.addLong(countValueIndex, pairCount);
            slot.value.setNew(false);
        }
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
        return stage.getProbeKeyColumn();
    }

    HashJoinGroupByRowUpdater getRowUpdater() {
        return rowUpdater;
    }

    Slot getSlot(int slot) {
        return slots.getQuick(slot + 1);
    }

    /**
     * The stage's state of a slot: the slot's probe of the open cursor's build, and what its probe
     * keys read through.
     */
    HashJoinBuildStage.Slot getStageSlot(int slot) {
        return stage.getSlot(slot);
    }

    /**
     * Binds the sequence that owns this atom, which a parallel build dispatches its rounds through.
     */
    void bindFrameSequence(UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence) {
        this.frameSequence = frameSequence;
    }

    /**
     * Binds the SYMBOL key translation for a probe of {@code probeRowCount} rows, the probe's frame
     * rows, every one of which the reducers read: a probe of fewer rows than one per 256 keys of
     * the probe dictionary translates through each slot's own cache rather than a shared one; see
     * {@link HashJoinBuildStage#bindSymbolKeyTranslation}. The cursor calls it once per execution
     * that probes, once the probe scan has cut its frames and before the payload copy and the
     * first probe task. On failure the caller closes the cursor, whose clear() releases what the
     * stage opened.
     */
    void bindSymbolKeyTranslation(long probeRowCount, SqlExecutionCircuitBreaker circuitBreaker) {
        stage.bindSymbolKeyTranslation(probeRowCount, circuitBreaker);
    }

    /**
     * Builds the open cursor's hash table and binds every slot to it. The cursor calls it once per
     * execution, on first demand for a result, on the thread that then dispatches the probes and
     * before it dispatches any; nothing here runs when the cursor merely opens.
     * <p>
     * The stage builds, on the owner or in rounds of the frame sequence's tasks; see
     * {@link HashJoinBuildStage#build}. The count pass of a query that only counts the joined pairs
     * and the slots' probes follow, both of which need the frozen build; the SYMBOL key translation
     * waits for the probe's frames, see {@link #bindSymbolKeyTranslation}. On failure the caller
     * closes the cursor, whose clear() closes the build, the filters and the frames.
     */
    void build(SqlExecutionCircuitBreaker circuitBreaker) {
        stage.build(circuitBreaker, frameSequence);
        if (isCountOnly) {
            if (stage.isBuildUnique()) {
                // A key's rows are one row, so a lookup that finds the key finds its count.
                isCountedByMatch = true;
            } else {
                // The count runs here, so the counts are in place before the sequence publishes the
                // first probe task. A failure closes the build.
                stage.countKeyRows(frameSequence);
                isCountedByKey = true;
            }
        }
        stage.bindProbes();
        for (int i = 0; i < slots.size(); i++) {
            Slot slot = slots.getQuick(i);
            slot.joinedRecord.of(slot.probeRecord, slot.probeRecord, stage.getSlot(i - 1).getProbe());
        }
    }

    /**
     * True when the open cursor's build has more than one hash partition, so that the reducers run
     * the loops over the slots' partitioned probes.
     */
    boolean isBuildPartitioned() {
        return stage.isBuildPartitioned();
    }

    boolean isBuildUnique() {
        return stage.isBuildUnique();
    }

    /**
     * True when the query only counts the joined pairs of an INNER join, as EXPLAIN reports. Each
     * execution then takes a key's pairs from the key's row count, or, over a build of one row per
     * key, from the lookup alone; see {@link #isCountedByKey()} and {@link #isCountedByMatch()}.
     */
    boolean isCountOnly() {
        return isCountOnly;
    }

    /**
     * True when the probe stages its key through a {@link RecordSink} rather than reading an INT.
     */
    boolean isKeyStaged() {
        return stage.isKeyStaged();
    }

    boolean isOuter() {
        return isOuter;
    }

    /**
     * True for the INT layout's lone SYMBOL pair, whose probe key translates before every lookup.
     */
    boolean isSymbolKey() {
        return stage.isSymbolKey();
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

    /**
     * True when some execution may copy the build's payload columns, as EXPLAIN reports.
     */
    boolean isPayloadCopyable() {
        return stage.isPayloadCopyable();
    }

    /**
     * Copies the build's payload columns when the probe is large enough to repay the copy, on the
     * workers for a build in rounds; see {@link HashJoinBuildStage#maybeCopyPayload}. The owner calls
     * this once the probe's frames are known and before it dispatches the probes, which then read
     * the copy. On failure the caller closes the cursor, whose clear() releases the copy.
     */
    void maybeCopyPayload(long probeRows, SqlExecutionCircuitBreaker circuitBreaker) {
        stage.maybeCopyPayload(probeRows, circuitBreaker, frameSequence);
    }

    boolean isProbeNeeded() {
        return isOuter || stage.getFrozenBuild().getRowCount() > 0;
    }

    /**
     * One execution slot's mutable probe-side views. A slot is also the symbol table source that
     * its functions initialize against, over the columns of the joined record: a probe column
     * resolves through the slot's probe record and a build payload column through a table of the
     * slot's own that it takes from the open build scan. Neither needs the frozen build, so the
     * functions initialize when the cursor opens, before the build. The slot's build views and its
     * probe of the build live in the stage's slot of the same index; see
     * {@link HashJoinBuildStage.Slot}.
     */
    static final class Slot implements QuietCloseable, SymbolTableSource {
        final HashJoinGroupByRecord joinedRecord;
        final ProbeRecord probeRecord = new ProbeRecord();
        // The table of each build payload column that a function of this slot resolved, by payload
        // column; a slot's functions and its probe's reader each read through tables of their own.
        private final ObjList<SymbolTable> buildSymbolTables = new ObjList<>();
        // Build scan column of each payload column, in payload order; shared by every slot.
        private final IntList payloadBuildColumns;
        // Columns of the joined record that belong to the probe input; payload columns follow them.
        private final int probeColumnCount;
        SimpleMapValue value;
        // The open cursor's build scan, which serves this slot's build symbol tables; null otherwise.
        private SymbolTableSource buildSymbols;

        Slot(HashJoinGroupByRecord joinedRecord, int probeColumnCount, IntList payloadBuildColumns) {
            this.joinedRecord = joinedRecord;
            this.probeColumnCount = probeColumnCount;
            this.payloadBuildColumns = payloadBuildColumns;
        }

        @Override
        public void close() {
            Throwable failure = Misc.freeBestEffort(null, value);
            value = null;
            failure = Misc.freeBestEffort(failure, probeRecord);
            failure = Misc.freeObjListIfCloseableBestEffort(failure, buildSymbolTables);
            buildSymbolTables.clear();
            buildSymbols = null;
            CairoException.rethrowCleanupFailure(failure);
        }

        /**
         * The slot's table of a joined record column. A build payload column takes its table from
         * the open build scan on first request and keeps it until {@link #clear}.
         */
        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < probeColumnCount) {
                return probeRecord.getSymbolTable(columnIndex);
            }
            final int payloadColumn = columnIndex - probeColumnCount;
            SymbolTable symbolTable = buildSymbolTables.getQuiet(payloadColumn);
            if (symbolTable == null) {
                symbolTable = buildSymbols.newSymbolTable(payloadBuildColumns.getQuick(payloadColumn));
                buildSymbolTables.extendAndSet(payloadColumn, symbolTable);
            }
            return symbolTable;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return columnIndex < probeColumnCount
                    ? probeRecord.newSymbolTable(columnIndex)
                    : buildSymbols.newSymbolTable(payloadBuildColumns.getQuick(columnIndex - probeColumnCount));
        }

        /**
         * Binds the slot to the two open scans of an execution, which serve its symbol tables.
         */
        void bindSymbols(SymbolTableSource probeSymbols, SymbolTableSource buildSymbols) {
            probeRecord.of(probeSymbols);
            this.buildSymbols = buildSymbols;
        }

        void clear(GroupByFunctionsUpdater updater) {
            if (value != null) {
                updater.updateEmpty(value);
                value.setNew(true);
            }
            joinedRecord.clear();
            probeRecord.of(null);
            // The functions released their views of these tables already, and the build scan that
            // serves them closes after the slots.
            Misc.freeObjListIfCloseable(buildSymbolTables);
            buildSymbolTables.clear();
            buildSymbols = null;
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
