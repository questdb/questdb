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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.async.PageFrameRoundDispatcher;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslatingRecord;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * One hash join build: the build input's page frames, its filters, the INT or staged-key build,
 * the SYMBOL key translation, the payload copy and the per-slot probes of the frozen build. The
 * build factory, its filter context and the build ON filter with its worker copies are borrowed
 * from the operator that owns the stage; the stage owns the rest. Each slot of the stage, one per
 * worker plus the owner's, keeps the slot's build views and its bindings to the frozen build:
 * the probes, the probe key sink and the SYMBOL key translation views.
 * <p>
 * {@link #init(SymbolTableSource, SqlExecutionContext)} runs when the operator's cursor opens and
 * reads no row: it opens the build scan and initializes the build filters against it.
 * <p>
 * {@link #build(SqlExecutionCircuitBreaker, PageFrameRoundDispatcher)} runs once per execution,
 * when the cursor is first asked for a result: it walks the build scan's page frames, filters each
 * one and keeps the key and the row id of every row that passes. The owner builds alone, unless
 * the build input holds at least the configured row count; then the frames filter and sort their
 * rows on the workers, and the workers fill the hash partitions of {@link IntHashJoinBuild} or
 * {@link MapHashJoinBuild}, through rounds of tasks that the operator's dispatcher runs. Each task
 * of a round finds the stage through the {@link HashJoinBuildHost} atom of the dispatcher's
 * sequence. Then {@link #bindSymbolKeyTranslation(SqlExecutionCircuitBreaker)} binds the SYMBOL key
 * translation: one cache per SYMBOL key column, shared by every slot, plus a pair of symbol tables
 * per slot; and {@link #bindProbes()} gives every slot its probe of the frozen build. The build
 * frames stay open until {@link #clear()}, because probes read payload columns through them and the
 * translation's keyOf() lookups resolve through their symbol tables. The owner dispatches the
 * probe tasks after the build returns, and the first task that its sequence publishes is what
 * publishes the frozen build, its flags and the slots' probes to the reducers. clear() requires
 * all reducers to have finished and output consumers to be done.
 * <p>
 * A query that only counts the joined pairs reads nothing of a pair, so a build that repeats a key
 * need not hand its rows out one by one: {@link #countKeyRows(PageFrameRoundDispatcher)} has the
 * frozen build replace each key's rows with their count before any reducer runs, on the owner, or
 * for most builds of more than one partition in one more round of tasks.
 * <p>
 * The stage decides at compile time whether the key table is presized by the build's rows; see
 * {@link HashJoinGroupByMetadata#isKeyCapacityPresized()}. Every execution checks that the tables
 * have not outgrown that decision; see {@link #checkTableRowCounts}.
 */
public final class HashJoinBuildStage implements Mutable, QuietCloseable {
    private static final UnorderedPageFrameReducer BUILD_PARTITION = HashJoinBuildStage::buildPartition;
    private static final UnorderedPageFrameReducer COPY_FRAME_PAYLOAD = HashJoinBuildStage::copyFramePayload;
    private static final UnorderedPageFrameReducer COUNT_PARTITIONS = HashJoinBuildStage::countPartitions;
    private static final UnorderedPageFrameReducer PARTITION_FRAME = HashJoinBuildStage::partitionFrame;
    private final RecordCursorFactory buildFactory;
    // The build scan's WHERE filter, if any, and the owner pool that walks the build frames.
    private final AsyncFilterContext buildFilterContext;
    private final HashJoinBuildFrames buildFrames;
    // The INT layout's only build key column, -1 when the key sinks stage the key instead.
    private final int buildKeyColumn;
    // An outer join's ON conditions on build columns alone, which drop build rows; null without them.
    private final Function buildOnFilter;
    // The payload copy's byte bound, and its probe to build row ratios for a copy on the owner and
    // a copy on the workers; see maybeCopyPayload().
    private final long copyMaxSize;
    private final double copyMinProbeRatio;
    private final double copyParallelMinProbeRatio;
    private final boolean isKeyCapacityPresized;
    private final boolean isKeyStaged;
    // True when the build may run on the workers: a filter context slot and a filter of its own
    // for every worker.
    private final boolean isParallelBuildCapable;
    // The INT layout's lone SYMBOL pair, whose probe keys the reducer translates per row.
    private final boolean isSymbolKey;
    // The build input's row count, before any filter, from which the build runs on the workers.
    private final long parallelBuildMinRows;
    // The INT layout's only probe key column, -1 when the key sinks stage the key instead.
    private final int probeKeyColumn;
    // The rows each task of a parallel build's round processes, by which fiber workers batch tasks.
    private final LongList roundTaskRowCounts = new LongList();
    private final long rowsPerPartition;
    private final ObjList<Slot> slots = new ObjList<>();
    // One shared translation cache per SYMBOL key column; empty when the key has none.
    private final ObjList<SymbolKeyTranslator> symbolKeyCaches = new ObjList<>();
    private final IntList symbolKeyBuildColumns = new IntList();
    private final IntList symbolKeyProbeColumns = new IntList();
    // Each worker's copy of the build ON filter, or null when the owner's is thread safe or absent.
    @Nullable
    private final ObjList<Function> workerBuildOnFilters;
    // Read by the tasks of a parallel build's key row count: the consecutive partitions each counts.
    private int countTaskPartitionCount;
    // The frozen build of the open cursor, whichever of the two builds produced it.
    private FrozenHashJoinBuild frozen;
    // Exactly one of the two builds exists, as isKeyStaged says.
    private IntHashJoinBuild intBuild;
    // Read by the frame tasks of a parallel build: whether a frame's rows go through the filters.
    private boolean isBuildFiltered;
    private boolean isBuildFiltersInitialized;
    // True when the open cursor's build has more than one partition, whose probes are a class of their own.
    private boolean isBuildPartitioned;
    private boolean isBuildUnique;
    // True when the open cursor's build ran on the workers, which lays the heap out by partition.
    private boolean isBuiltInRounds;
    // True when the open cursor's build counted its key rows on the workers, in a round of tasks.
    private boolean isCountedInRounds;
    private boolean isPayloadCopied;
    private MapHashJoinBuild mapBuild;
    // The open cursor's tracker, which init() takes for build(): the build runs after getCursor()
    // returned, when the execution context may no longer carry it.
    @Nullable
    private MemoryTracker memoryTracker;
    // The rows of the open cursor's build that come from Parquet frames, among the rows the build
    // kept; see maybeCopyPayload().
    private long parquetBuildRows;
    // The Parquet frames that hold any of those kept rows, each of which a payload copy decodes once;
    // see maybeCopyPayload().
    private int parquetFrameCount;
    // The rows of the Parquet frames that hold any of those kept rows. A probe reader decodes the
    // payload columns of such a frame for all its rows, so these are the rows it may decode; see
    // maybeCopyPayload().
    private long parquetFrameRows;
    // The open cursor's probe scan, whose dictionaries the SYMBOL key translation is sized by.
    private SymbolTableSource probeSymbols;

    /**
     * Takes every key sink and stager the stage will ever need, so construct it while the inputs,
     * whose metadata the key layout borrows, are still alive. Each slot's staged probe key reads
     * through a record that {@link Slot#ofKeySource(Record)} binds.
     *
     * @param buildFactory          the build input's page frame scan, borrowed
     * @param buildFilterContext    the build scan's filters and memory pools, borrowed
     * @param buildOnFilter         an outer join's ON conditions on build columns alone, borrowed
     * @param workerBuildOnFilters  each worker's copy of the build ON filter, borrowed
     * @param keyLayout             the join's equality keys over the probe and the build input
     * @param payloadBuildColumns   the build columns that probes read, in payload order
     * @param probeColumnCount      the columns of the record that probe keys read
     * @param isKeyCapacityPresized true when the key table is presized by the build's rows
     * @param workerCount           the workers that may run the stage's tasks
     */
    HashJoinBuildStage(
            CairoConfiguration configuration,
            RecordCursorFactory buildFactory,
            AsyncFilterContext buildFilterContext,
            @Nullable Function buildOnFilter,
            @Nullable ObjList<Function> workerBuildOnFilters,
            HashJoinKeyLayout keyLayout,
            IntList payloadBuildColumns,
            int probeColumnCount,
            boolean isKeyCapacityPresized,
            int workerCount
    ) {
        this.buildFactory = buildFactory;
        this.buildFilterContext = buildFilterContext;
        this.buildOnFilter = buildOnFilter;
        this.workerBuildOnFilters = workerBuildOnFilters;
        this.isKeyStaged = keyLayout.isKeyStaged();
        this.isSymbolKey = keyLayout.isSymbolKey();
        this.isKeyCapacityPresized = isKeyCapacityPresized;
        // Each slot's probe reads the build's payload through a reader of its own, so the readers of
        // an execution are one per worker and the owner's, and share the Parquet decode cache.
        this.buildFrames = new HashJoinBuildFrames(
                configuration,
                payloadBuildColumns,
                buildFactory.getMetadata(),
                workerCount + 1
        );
        this.probeKeyColumn = isKeyStaged ? -1 : keyLayout.getProbeKeyColumn();
        this.buildKeyColumn = isKeyStaged ? -1 : keyLayout.getBuildKeyColumn();
        symbolKeyProbeColumns.addAll(keyLayout.getSymbolKeyProbeColumns());
        symbolKeyBuildColumns.addAll(keyLayout.getSymbolKeyBuildColumns());
        for (int i = 0, n = symbolKeyProbeColumns.size(); i < n; i++) {
            symbolKeyCaches.add(new SymbolKeyTranslator());
        }
        this.copyMaxSize = configuration.getSqlParallelHashJoinGroupByPayloadCopyMaxSize();
        this.copyMinProbeRatio = configuration.getSqlParallelHashJoinGroupByPayloadCopyMinProbeRatio();
        this.copyParallelMinProbeRatio = configuration.getSqlParallelHashJoinGroupByPayloadCopyParallelMinProbeRatio();
        this.parallelBuildMinRows = configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows();
        this.rowsPerPartition = configuration.getSqlParallelHashJoinGroupByBuildRowsPerPartition();
        final Function buildFilter = buildFilterContext.getFilter(-1);
        this.isParallelBuildCapable = buildFilterContext.getPerWorkerMemoryPools().size() >= workerCount
                && (buildFilter == null
                || buildFilter.isThreadSafe()
                || buildFilterContext.getFilter(0) != buildFilter)
                && (buildOnFilter == null
                || buildOnFilter.isThreadSafe()
                || workerBuildOnFilters != null);
        // Sinks read the borrowed input metadatas when they are instantiated, so every sink
        // this execution will ever need is taken here, while the inputs are still alive. The
        // builds, the stagers, the translating records and the frames allocate no native memory
        // until an execution opens them, so a failure here leaves nothing to release.
        final boolean hasPayload = payloadBuildColumns.size() > 0;
        if (isKeyStaged) {
            mapBuild = new MapHashJoinBuild(configuration, keyLayout.getKeyTypes(), hasPayload,
                    configuration.getSqlSmallMapKeyCapacity(), configuration.getSqlSmallMapPageSize(), 64, true);
        } else {
            intBuild = new IntHashJoinBuild(hasPayload, 64, 64, true);
        }
        final boolean hasSymbolKey = symbolKeyProbeColumns.size() > 0;
        for (int i = -1; i < workerCount; i++) {
            // Sinks hold scratch state, so each slot probes and builds through ones of its own,
            // and so does the translating record the staged key sink reads from.
            slots.add(new Slot(
                    keyLayout.newProbeKeySink(),
                    keyLayout.newBuildKeySink(),
                    isKeyStaged ? mapBuild.newKeyStager() : null,
                    hasSymbolKey && isKeyStaged
                            ? new SymbolKeyTranslatingRecord(probeColumnCount, symbolKeyProbeColumns)
                            : null,
                    hasSymbolKey && !isKeyStaged ? new SymbolKeyTranslator.View() : null
            ));
        }
    }

    /**
     * True when the matches of a probe of {@code probeRows} rows can land on as many Parquet frames
     * as a copy of the build's payload decodes, the {@code parquetFrameCount} frames that hold the
     * build's kept Parquet rows, each of which the copy decodes once. Without the copy, a probe reader
     * decodes a Parquet frame for a match on any of its rows, at most once per match, however often
     * its frames evict each other, so the probe's matches decode at most as many frames as there are
     * matches. A probe row matches the rows of one key. Both build layouts link every row they keep
     * into the chain of its key, NULL as a key of its own, which NULL probe keys match, and count each
     * key once: a build in rounds counts it in the key table of the one partition that its hash
     * selects. Each of the {@code buildKeyCount} keys therefore holds at least one of the
     * {@code buildRows} rows, no key holds more than {@code buildRows - buildKeyCount + 1} of them,
     * and the probe's matches decode at most {@code probeRows * (buildRows - buildKeyCount + 1)}
     * frames in all, whichever keys it asks for. That holds for a probe of hot keys too, such as NULL
     * or a sentinel in a column of otherwise unique keys, which the average rows per key would miss.
     * A build without Parquet frames passes, and a build without rows has no keys. A probe whose keys
     * the build holds fewer rows of decodes fewer frames than this assumes, so a build with a
     * repeated key may count as in reach of a probe that cannot decode that many frames. The product
     * compares exactly: its high word is nonzero only past any frame count, and otherwise its low
     * word holds the product, unsigned.
     */
    public static boolean isParquetInProbeReach(long probeRows, long buildRows, long buildKeyCount, long parquetFrameCount) {
        assert probeRows >= 0 && buildKeyCount >= 0 && buildKeyCount <= buildRows && (buildKeyCount > 0 || buildRows == 0) && parquetFrameCount >= 0;
        // No key holds more rows than the build's other keys leave it, one row each.
        final long maxKeyRows = buildRows - Math.max(buildKeyCount, 1) + 1;
        return Math.multiplyHigh(probeRows, maxKeyRows) != 0 || Long.compareUnsigned(probeRows * maxKeyRows, parquetFrameCount) >= 0;
    }

    /**
     * Releases what the open cursor's execution took: the build filters' per-execution state, the
     * slots' build views and probes, the frozen build, the translation caches and, last, the build
     * scan. Every reducer must have finished, and every reader of the frozen build's payload and of
     * the build scan's symbol tables must have let go of them. The objects stay for the next
     * execution.
     */
    @Override
    public void clear() {
        Throwable failure = null;
        if (isBuildFiltersInitialized) {
            isBuildFiltersInitialized = false;
            Function ownerBuildFilter = buildFilterContext.getFilter(-1);
            failure = cursorClosed(failure, ownerBuildFilter);
            for (int i = 0, n = buildFilterContext.getPerWorkerMemoryPools().size(); i < n; i++) {
                Function workerBuildFilter = buildFilterContext.getFilter(i);
                if (workerBuildFilter != ownerBuildFilter) {
                    failure = cursorClosed(failure, workerBuildFilter);
                }
            }
            failure = cursorClosed(failure, buildOnFilter);
            if (workerBuildOnFilters != null) {
                for (int i = 0, n = workerBuildOnFilters.size(); i < n; i++) {
                    failure = cursorClosed(failure, workerBuildOnFilters.getQuick(i));
                }
            }
        }
        failure = Misc.clearObjListBestEffort(failure, slots);
        failure = Misc.clearBestEffort(failure, buildFilterContext);
        frozen = null;
        isBuildFiltered = false;
        isBuildPartitioned = false;
        isBuildUnique = false;
        isBuiltInRounds = false;
        isCountedInRounds = false;
        isPayloadCopied = false;
        parquetBuildRows = 0;
        parquetFrameCount = 0;
        parquetFrameRows = 0;
        failure = Misc.freeBestEffort(failure, intBuild);
        failure = Misc.freeBestEffort(failure, mapBuild);
        // The slots released their symbol tables above, so the shared caches go next.
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, symbolKeyCaches);
        // The host's functions and slots, the filters and the build have released every symbol
        // table view and payload reader of these frames.
        failure = Misc.clearBestEffort(failure, buildFrames);
        memoryTracker = null;
        probeSymbols = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Throwable failure = Misc.clearBestEffort(null, this);
        failure = Misc.freeObjListBestEffort(failure, slots);
        slots.clear();
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * The build scan's filters and memory pools, which the stage borrows: the owner's WHERE filter,
     * if any, and each worker's copy of it, or the owner's filter itself for a filter without
     * worker copies.
     */
    @TestOnly
    public AsyncFilterContext getBuildFilterContext() {
        return buildFilterContext;
    }

    /**
     * Page frames of the open cursor's build input, zero until the cursor builds.
     */
    public int getBuildFrameCount() {
        return buildFrames.getFrameCount();
    }

    /**
     * Native bytes that the slots' build key stagers hold: none once the frame tasks of a parallel
     * build are done, and none for the INT layout, which stages no key.
     */
    public long getBuildKeyStagerSizeInBytes() {
        long size = 0;
        for (int i = 0, n = slots.size(); i < n; i++) {
            final MapHashJoinBuild.KeyStager stager = slots.getQuick(i).buildKeyStager;
            if (stager != null) {
                size += stager.getSizeInBytes();
            }
        }
        return size;
    }

    /**
     * An outer join's ON conditions on build columns alone, which the stage borrows; null without
     * them.
     */
    @TestOnly
    @Nullable
    public Function getBuildOnFilter() {
        return buildOnFilter;
    }

    /**
     * The open build scan, which serves the build input's symbol tables; null when no cursor is open.
     */
    public SymbolTableSource getBuildSymbols() {
        return buildFrames.getSymbolTableSource();
    }

    /**
     * The build published for the open cursor, or null when no cursor is open, or when the open
     * cursor has not been asked for a result yet; see
     * {@link #build(SqlExecutionCircuitBreaker, PageFrameRoundDispatcher)}.
     */
    public FrozenHashJoinBuild getFrozenBuild() {
        return frozen;
    }

    /**
     * Hash partitions of the open cursor's build: one for a build on the owner.
     */
    public int getPartitionCount() {
        return isKeyStaged ? mapBuild.getPartitionCount() : intBuild.getPartitionCount();
    }

    /**
     * The INT layout's only probe key column, -1 when the key sinks stage the key instead.
     */
    public int getProbeKeyColumn() {
        return probeKeyColumn;
    }

    /**
     * Each worker's copy of the build ON filter, which the stage borrows; null when the owner's
     * filter is thread safe or absent.
     */
    @TestOnly
    @Nullable
    public ObjList<Function> getWorkerBuildOnFilters() {
        return workerBuildOnFilters;
    }

    /**
     * True when the open cursor's build ran on the workers with its frames' rows written straight
     * into the frames' regions of the heap, as a build whose frames keep every row does; see
     * {@link IntHashJoinBuild#beginUnfilteredPartitioning}.
     */
    public boolean hasBuildFrameRegions() {
        return isKeyStaged ? mapBuild.hasFrameRegions() : intBuild.hasFrameRegions();
    }

    /**
     * Opens the build scan and initializes the build filters, and reads no row:
     * {@link #build(SqlExecutionCircuitBreaker, PageFrameRoundDispatcher)} does the work that grows
     * with the build, when the cursor is first asked for a result. Everything that can throw a
     * SqlException runs here, so that it surfaces from getCursor(). On failure the caller runs
     * {@link #clear()}, which closes what this opened.
     *
     * @param probeSymbols the open probe scan, whose dictionaries the SYMBOL key translation reads
     */
    public void init(SymbolTableSource probeSymbols, SqlExecutionContext executionContext) throws SqlException {
        assert frozen == null && buildFrames.getSymbolTableSource() == null;
        memoryTracker = executionContext.getMemoryTracker();
        this.probeSymbols = probeSymbols;
        // The open scan serves the build input's symbol tables; build() walks its frames.
        buildFrames.open(buildFactory, executionContext);
        checkTableRowCounts(probeSymbols, executionContext);
        final SymbolTableSource buildSymbols = buildFrames.getSymbolTableSource();
        isBuildFiltersInitialized = true;
        buildFilterContext.initFilters(buildSymbols, executionContext);
        if (buildOnFilter != null) {
            buildOnFilter.init(buildSymbols, executionContext);
            if (workerBuildOnFilters != null) {
                final boolean isCloneSymbolTables = executionContext.getCloneSymbolTables();
                executionContext.setCloneSymbolTables(true);
                try {
                    Function.init(workerBuildOnFilters, buildSymbols, executionContext, buildOnFilter);
                } finally {
                    executionContext.setCloneSymbolTables(isCloneSymbolTables);
                }
            }
        }
    }

    /**
     * True when the open cursor's build has more than one hash partition, so that the reducers run
     * the loops over the slots' partitioned probes.
     */
    public boolean isBuildPartitioned() {
        return isBuildPartitioned;
    }

    /**
     * True when the open cursor's build holds one row per key, as an empty build does too.
     */
    public boolean isBuildUnique() {
        return isBuildUnique;
    }

    /**
     * True when the open cursor's build ran on the workers, in rounds of the dispatcher's tasks.
     */
    public boolean isBuiltInRounds() {
        return isBuiltInRounds;
    }

    /**
     * True when the open cursor's build counted its key rows on the workers, in a round of the
     * dispatcher's tasks, each over a range of hash partitions, rather than in one pass on the owner.
     * A build of more than one partition takes the round unless its frames are too small for it;
     * see {@link IntHashJoinBuild#getKeyRowCountTaskCount()}.
     */
    public boolean isCountedInRounds() {
        return isCountedInRounds;
    }

    /**
     * True when the probe stages its key through a {@link RecordSink} rather than reading an INT.
     */
    public boolean isKeyStaged() {
        return isKeyStaged;
    }

    /**
     * True when the open cursor's probes read a copy of the build's payload columns rather than the
     * columns where they live; see {@link #maybeCopyPayload(long, SqlExecutionCircuitBreaker, PageFrameRoundDispatcher)}.
     */
    public boolean isPayloadCopied() {
        return isPayloadCopied;
    }

    /**
     * True when some execution may copy the build's payload columns, as EXPLAIN reports.
     */
    public boolean isPayloadCopyable() {
        return buildFrames.getCopyRowSize() > 0 && copyMaxSize >= buildFrames.getCopyRowSize();
    }

    /**
     * True for the INT layout's lone SYMBOL pair, whose probe key translates before every lookup.
     */
    public boolean isSymbolKey() {
        return isSymbolKey;
    }

    /**
     * True when the open cursor's translation cache of the key-th SYMBOL key pair was filled from
     * the build dictionary rather than opened unresolved; see {@link SymbolKeyTranslator}. False
     * until the cursor builds, and for a stage without that many SYMBOL key pairs.
     */
    public boolean isSymbolKeyCacheFilledFromBuild(int key) {
        return key < symbolKeyCaches.size() && symbolKeyCaches.getQuick(key).isFilledFromBuild();
    }

    /**
     * One task of a parallel build per partition: fills the partition's region of the heap, or links
     * its rows in the frames' regions, and its table, on whichever thread runs the task.
     */
    private static void buildPartition(
            int workerId,
            PageFrameMemoryRecord unused,
            int partition,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence,
            UnorderedPageFrameSequence<?> stealingSequence
    ) {
        final HashJoinBuildHost host = (HashJoinBuildHost) sequence.getAtom();
        final HashJoinBuildStage stage = host.getBuildingStage();
        // The task holds no slot resource, but every task of the sequence takes a slot, as the
        // work-stealing strategy expects of its tasks.
        final int slotId = host.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        try {
            if (stage.isKeyStaged) {
                stage.mapBuild.buildPartition(partition, breaker);
            } else {
                stage.intBuild.buildPartition(partition, breaker);
            }
        } finally {
            host.release(slotId);
        }
    }

    /**
     * One task of a parallel build's payload copy per build frame: copies the payload columns of
     * the rows the frame kept, partition by partition, through the slot's copy reader. The rows
     * all come from the one frame, so a Parquet frame decodes once per task.
     */
    private static void copyFramePayload(
            int workerId,
            PageFrameMemoryRecord unused,
            int frameIndex,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence,
            UnorderedPageFrameSequence<?> stealingSequence
    ) {
        final HashJoinBuildHost host = (HashJoinBuildHost) sequence.getAtom();
        final HashJoinBuildStage stage = host.getBuildingStage();
        final int slotId = host.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        // Copy readers are indexed from zero, the owner's first.
        final int reader = slotId + 1;
        try {
            for (int partition = 0, n = stage.getPartitionCount(); partition < n; partition++) {
                final long rowCount = stage.getSegmentRowCount(frameIndex, partition);
                if (rowCount > 0) {
                    final long lo = stage.getSegmentStart(frameIndex, partition);
                    stage.buildFrames.copyRows(reader, stage.frozen, lo, lo + rowCount, breaker);
                }
            }
        } finally {
            try {
                stage.buildFrames.releaseCopyBuffers(reader);
            } finally {
                host.release(slotId);
            }
        }
    }

    /**
     * One task of a parallel build's key row count per range of consecutive partitions: replaces the
     * links of the range's rows with chain lengths, on whichever thread runs the task.
     */
    private static void countPartitions(
            int workerId,
            PageFrameMemoryRecord unused,
            int task,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence,
            UnorderedPageFrameSequence<?> stealingSequence
    ) {
        final HashJoinBuildHost host = (HashJoinBuildHost) sequence.getAtom();
        final HashJoinBuildStage stage = host.getBuildingStage();
        // As a build partition task, it holds no slot resource but takes a slot.
        final int slotId = host.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        try {
            final int partitionLo = task * stage.countTaskPartitionCount;
            final int partitionHi = partitionLo + stage.countTaskPartitionCount;
            if (stage.isKeyStaged) {
                stage.mapBuild.countPartitionKeyRows(partitionLo, partitionHi, breaker);
            } else {
                stage.intBuild.countPartitionKeyRows(partitionLo, partitionHi, breaker);
            }
        } finally {
            host.release(slotId);
        }
    }

    private static Throwable cursorClosed(Throwable failure, Function function) {
        if (function != null) {
            try {
                function.cursorClosed();
            } catch (Throwable th) {
                return Misc.foldCleanupFailure(failure, th);
            }
        }
        return failure;
    }

    /**
     * True when this build copies its payload columns for a probe of this many rows: a payload that
     * the copy can hold, a copy within the byte bound, and a probe that the copy repays. A probe
     * repays the copy of a native build once it holds at least the given ratio of the build's rows,
     * so that its matches read enough of them. The build's rows from Parquet frames change that only
     * for a probe whose matches can land on as many of those frames as the copy decodes,
     * {@code isParquetReached}, see {@link #isParquetInProbeReach(long, long, long, long)}:
     * without the copy, every slot that probes decodes the frames its matches land on into its own
     * share of the decode cache, where the copy decodes each frame once and caches none of them for
     * the probe. While those frames fit a share, the ratio then weighs the native rows alone, so a
     * build of Parquet rows alone copies for any such probe. Once they decode past a share,
     * {@code isParquetOverReaderCache}, a slot whose matches move between them decodes them again and
     * again, so the build copies for any such probe, however many native rows it has. The matches of
     * a probe that cannot land on that many frames make the slots decode fewer frames in all than
     * the copy would, however often they decode one again, so the ratio weighs all the build's rows,
     * Parquet rows too, as it does a native build's, whatever the cache: such a build copies exactly
     * when a native build of as many rows would. A native build is always in reach, and weighs all
     * its rows. The probe's count is its frame rows, before any row filter, so a selective probe
     * filter can copy a build it need not; the byte bound caps what that costs.
     */
    private static boolean isPayloadCopyWorthIt(
            long buildRows,
            long parquetBuildRows,
            boolean isParquetReached,
            boolean isParquetOverReaderCache,
            int copyRowSize,
            long probeRows,
            long copyMaxSize,
            double copyMinProbeRatio
    ) {
        // A build with Parquet rows is in reach of a probe with rows only.
        assert !isParquetReached || parquetBuildRows == 0 || probeRows > 0;
        if (copyRowSize <= 0 || buildRows <= 0 || buildRows > copyMaxSize / copyRowSize) {
            return false;
        }
        if (!isParquetReached) {
            return probeRows >= copyMinProbeRatio * buildRows;
        }
        return isParquetOverReaderCache || probeRows >= copyMinProbeRatio * (buildRows - parquetBuildRows);
    }

    /**
     * One task of a parallel build per build frame: filters the frame on the slot's own filters and
     * sorts the rows it keeps into the frame's hash buckets.
     */
    private static void partitionFrame(
            int workerId,
            PageFrameMemoryRecord unused,
            int frameIndex,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence,
            UnorderedPageFrameSequence<?> stealingSequence
    ) {
        final HashJoinBuildHost host = (HashJoinBuildHost) sequence.getAtom();
        final HashJoinBuildStage stage = host.getBuildingStage();
        final int slotId = host.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        try {
            final PageFrameMemoryPool pool = stage.buildFilterContext.getMemoryPool(slotId);
            try {
                final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                final Slot slot = stage.getSlot(slotId);
                final PageFrameMemoryRecord record = slot.buildRecord;
                record.init(frameMemory);
                final long rowCount = stage.buildFrames.getFrameRowCount(frameIndex);
                if (stage.isBuildFiltered) {
                    final DirectLongList rows = stage.buildFilterContext.getFilteredRows(slotId);
                    stage.filterBuildFrame(slotId, frameMemory, record, rowCount, rows);
                    if (stage.isKeyStaged) {
                        stage.mapBuild.partitionFrame(frameIndex, record, slot.buildKeySink, slot.buildKeyStager, rows);
                    } else {
                        stage.intBuild.partitionFrame(frameIndex, record, stage.buildKeyColumn, rows);
                    }
                } else if (stage.isKeyStaged) {
                    stage.mapBuild.partitionFrame(frameIndex, record, slot.buildKeySink, slot.buildKeyStager, rowCount);
                } else {
                    stage.intBuild.partitionFrame(frameIndex, record, stage.buildKeyColumn, rowCount);
                }
            } finally {
                pool.releaseParquetBuffers();
            }
        } finally {
            host.release(slotId);
        }
    }

    /**
     * Builds on the workers, in rounds of the dispatcher's tasks; see {@link IntHashJoinBuild} for
     * the layout, which {@link MapHashJoinBuild} shares. One task per frame filters the frame and
     * sorts the rows it keeps by their key's hash, then the owner sizes the heap for exactly the
     * kept rows and one task per partition fills the heap's region and the key table of its
     * partition. An unfiltered build sizes the heap first, and its frame tasks place their rows in
     * it, unless a staged key is too wide for that. A round returns once all its tasks have stopped,
     * also when it throws.
     */
    private void buildInRounds(MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker,
                               PageFrameRoundDispatcher dispatcher, SymbolTableSource buildSymbols, boolean isFiltered) {
        for (int i = 0, n = slots.size(); i < n; i++) {
            slots.getQuick(i).buildRecord.of(buildSymbols);
        }
        isBuildFiltered = isFiltered;
        final int frameCount = buildFrames.getFrameCount();
        roundTaskRowCounts.clear();
        for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
            roundTaskRowCounts.add(buildFrames.getFrameRowCount(frameIndex));
        }
        // Frames that keep every row may go straight to the heap, which takes their rows up front.
        if (isKeyStaged) {
            mapBuild.open(memoryTracker, circuitBreaker);
            if (isFiltered) {
                mapBuild.beginPartitioning(frameCount, buildFrames.getRowCount(), rowsPerPartition);
            } else {
                mapBuild.beginUnfilteredPartitioning(roundTaskRowCounts, rowsPerPartition);
            }
        } else {
            intBuild.open(memoryTracker, circuitBreaker);
            if (isFiltered) {
                intBuild.beginPartitioning(frameCount, buildFrames.getRowCount(), rowsPerPartition);
            } else {
                intBuild.beginUnfilteredPartitioning(roundTaskRowCounts, rowsPerPartition);
            }
        }
        dispatcher.dispatchRoundAndAwait(PARTITION_FRAME, roundTaskRowCounts);
        final int partitionCount;
        if (isKeyStaged) {
            // Each stager's scratch holds a frame's keys once more, and the chunks hold them all now,
            // so the scratches go before the partitions' maps take their memory.
            for (int i = 0, n = slots.size(); i < n; i++) {
                slots.getQuick(i).buildKeyStager.close();
            }
            // The kept rows are exact here, filtered builds included, so they may presize the tables.
            partitionCount = mapBuild.planPartitions(rowsPerPartition, getKeyCountHint(mapBuild.getPartitionedRowCount()));
        } else {
            partitionCount = intBuild.planPartitions(rowsPerPartition, getKeyCountHint(intBuild.getPartitionedRowCount()));
        }
        roundTaskRowCounts.clear();
        for (int partition = 0; partition < partitionCount; partition++) {
            roundTaskRowCounts.add(getPartitionRowCount(partition));
        }
        dispatcher.dispatchRoundAndAwait(BUILD_PARTITION, roundTaskRowCounts);
        frozen = isKeyStaged ? mapBuild.freezePartitioned(buildFrames) : intBuild.freezePartitioned(buildFrames);
        isBuiltInRounds = true;
        isBuildPartitioned = partitionCount > 1;
        if (buildFrames.hasParquetFrames()) {
            // A frame's kept rows are its segments, one per partition.
            for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
                if (buildFrames.getAddressCache().getFrameFormat(frameIndex) != PartitionFormat.NATIVE) {
                    long keptRows = 0;
                    for (int partition = 0, n = getPartitionCount(); partition < n; partition++) {
                        keptRows += getSegmentRowCount(frameIndex, partition);
                    }
                    countParquetFrame(frameIndex, keptRows);
                }
            }
        }
    }

    // Walks the build frames on the owner, appending each frame's kept rows as it goes.
    private void buildOnOwner(MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker,
                              SymbolTableSource buildSymbols, boolean isFiltered) {
        final Slot owner = getSlot(-1);
        final PageFrameMemoryRecord buildRecord = owner.buildRecord;
        buildRecord.of(buildSymbols);
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
                    filterBuildFrame(-1, frameMemory, buildRecord, rowCount, rows);
                    // A SYMBOL key keeps the build's own symbol keys; the probe translates into them.
                    if (isKeyStaged) {
                        mapBuild.appendFrame(buildRecord, owner.buildKeySink, rows);
                    } else {
                        intBuild.appendFrame(buildRecord, buildKeyColumn, rows);
                    }
                } else if (isKeyStaged) {
                    mapBuild.appendFrame(buildRecord, owner.buildKeySink, rowCount);
                } else {
                    intBuild.appendFrame(buildRecord, buildKeyColumn, rowCount);
                }
                // The address cache gives the frame format that the payload readers go by.
                if (buildFrames.getAddressCache().getFrameFormat(frameIndex) != PartitionFormat.NATIVE) {
                    countParquetFrame(frameIndex, isFiltered ? rows.size() : rowCount);
                }
            } finally {
                // Each frame is read once, so a decoded Parquet frame has no later use.
                pool.releaseParquetBuffers();
            }
        }
        // A build without payload columns stores no row ids and never asks the frames for one.
        frozen = isKeyStaged ? mapBuild.freeze(buildFrames) : intBuild.freeze(buildFrames);
    }

    /**
     * Throws {@link TableReferenceOutOfDateException}, on which the query caches and the other
     * holders of a compiled factory compile the query again, when the tables have changed size
     * since the planner chose from their sizes, so much that the choice now costs far more memory
     * than a new compile's plan would. The planner compared the tables' {@link TableReader#size()},
     * and so does this check, through the readers of the two open scans:
     * <ul>
     * <li>The planner presizes the key table by the build's rows when the build table had no more
     * rows than the probe table, as every INNER join that builds its smaller table does. Once the
     * build table holds more than twice the probe table's rows, an INNER join builds more than
     * twice the rows that a new compile would, and any join sizes its key table for rows whose
     * keys repeat. The factor of two keeps tables of about equal size from recompiling every few
     * inserts. The alternate orientation of {@link HashJoinGroupByBuildChoiceRecordCursorFactory}
     * builds the table that the primary probes, which held at least as many rows at compile time.
     * The planner presizes its key table only when the two tables held equal rows, and this rule
     * then checks the alternate as it checks the primary. An outer join's ON condition on the
     * build alone that reads a variable-size column, and does more than compare columns,
     * constants and bind variables, takes the fused plan only under the presize condition, since
     * the build runs it on every build row, and so does one that passes a STRING, VARCHAR, BINARY
     * or ARRAY value that it computes from a build column to anything but a comparison, so the
     * new compile also returns such a query to the ordinary plan.</li>
     * <li>A RIGHT join takes the fused plan only under the presize condition, so the first rule
     * returns it to the ordinary plan once its build table holds more than twice the preserved
     * table's rows. A reused factory keeps building the larger table while it holds between one
     * and two times the preserved table's rows.</li>
     * </ul>
     * A new compile reads the same sizes and passes this check, so a recompile repeats only when
     * the tables change again in between. An input that is not a table scan offers no reader, and
     * keeps the compile-time choice. So does an execution whose context
     * {@link SqlExecutionContext#isTableRowCountChangeTolerated() tolerates} row count changes,
     * such as a queued export, which runs a factory that nothing compiles again.
     */
    private void checkTableRowCounts(SymbolTableSource probeScan, SqlExecutionContext executionContext) {
        if (executionContext.isTableRowCountChangeTolerated()
                || !(probeScan instanceof TablePageFrameCursor probeCursor)
                || !(buildFrames.getSymbolTableSource() instanceof TablePageFrameCursor buildCursor)) {
            return;
        }
        final TableReader probeReader = probeCursor.getTableReader();
        final TableReader buildReader = buildCursor.getTableReader();
        // An open table scan always holds its reader.
        assert probeReader != null && buildReader != null;
        final long buildRows = buildReader.size();
        // Row counts stay far below 2^62, so twice the probe rows cannot overflow.
        if (isKeyCapacityPresized && buildRows > 2 * probeReader.size()) {
            throw TableReferenceOutOfDateException.of(buildReader.getTableToken());
        }
    }

    /**
     * Counts the key rows of a build of more than one partition on the workers, in one more round of
     * tasks, each over as many consecutive partitions, in place of the owner's single pass. A key's
     * rows lie in one partition and link only to each other, so each task counts its partitions'
     * rows alone, in the order that their build tasks linked them; see {@link IntHashJoinBuild}. The
     * round returns once all its tasks have stopped, also when it throws, and its return makes their
     * writes visible to the owner, which publishes them to the probes with the first probe task, as
     * it publishes the build's own. Only a round whose every task returned ends the count; any
     * failure closes the build, as the owner's failed pass does, so no probe reads a half counted
     * heap.
     */
    private void countKeyRowsInRounds(int taskCount, PageFrameRoundDispatcher dispatcher) {
        countTaskPartitionCount = getPartitionCount() / taskCount;
        roundTaskRowCounts.clear();
        for (int task = 0; task < taskCount; task++) {
            long rowCount = 0;
            for (int partition = task * countTaskPartitionCount, n = partition + countTaskPartitionCount; partition < n; partition++) {
                rowCount += getPartitionRowCount(partition);
            }
            roundTaskRowCounts.add(rowCount);
        }
        try {
            dispatcher.dispatchRoundAndAwait(COUNT_PARTITIONS, roundTaskRowCounts);
            if (isKeyStaged) {
                mapBuild.endPartitionedKeyRowCount();
            } else {
                intBuild.endPartitionedKeyRowCount();
            }
        } catch (Throwable th) {
            // Some partitions may hold counts and others links.
            if (isKeyStaged) {
                mapBuild.close();
            } else {
                intBuild.close();
            }
            throw th;
        }
        isCountedInRounds = true;
    }

    // Counts the rows that a Parquet build frame kept, and the frame and its rows when it kept any: a
    // probe reader decodes the payload columns of the whole frame for a match on any of its kept
    // rows, and a payload copy decodes them once.
    private void countParquetFrame(int frameIndex, long keptRows) {
        parquetBuildRows += keptRows;
        if (keptRows > 0) {
            parquetFrameCount++;
            parquetFrameRows += buildFrames.getFrameRowCount(frameIndex);
        }
    }

    /**
     * Leaves in {@code rows} the rows of the frame that the build's WHERE filter and ON filter
     * both pass, evaluated on the slot's own copies of them. The compiled filter reads raw column
     * addresses, so column tops and Parquet type casts, which the record resolves per row, fall
     * back to the interpreted filter.
     */
    private void filterBuildFrame(int slotId, PageFrameMemory frameMemory, PageFrameMemoryRecord record, long rowCount, DirectLongList rows) {
        rows.clear();
        final Function filter = buildFilterContext.getFilter(slotId);
        if (filter != null) {
            final CompiledFilter compiledFilter = buildFilterContext.getCompiledFilter();
            if (compiledFilter == null
                    || frameMemory.hasColumnTops()
                    || frameMemory.hasColumnTypeCasts()) {
                AsyncFilterUtils.applyFilter(filter, rows, record, rowCount);
            } else {
                AsyncFilterUtils.applyCompiledFilter(
                        compiledFilter,
                        buildFilterContext.getBindVarMemory(),
                        buildFilterContext.getBindVarFunctions(),
                        frameMemory,
                        buildFrames.getAddressCache(),
                        buildFilterContext.getDataAddresses(slotId),
                        buildFilterContext.getAuxAddresses(slotId),
                        rows,
                        rowCount
                );
            }
        }
        final Function onFilter = slotId == -1 || workerBuildOnFilters == null ? buildOnFilter : workerBuildOnFilters.getQuick(slotId);
        if (onFilter != null) {
            if (filter == null) {
                AsyncFilterUtils.applyFilter(onFilter, rows, record, rowCount);
            } else {
                long kept = 0;
                for (long p = 0, n = rows.size(); p < n; p++) {
                    final long row = rows.get(p);
                    record.setRowIndex(row);
                    if (onFilter.getBool(record)) {
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

    private int getKeyRowCountTaskCount() {
        return isKeyStaged ? mapBuild.getKeyRowCountTaskCount() : intBuild.getKeyRowCountTaskCount();
    }

    // Where each frame's rows of each partition lie in the open cursor's heap.
    private long getPartitionRowCount(int partition) {
        return isKeyStaged ? mapBuild.getPartitionRowCount(partition) : intBuild.getPartitionRowCount(partition);
    }

    private long getSegmentRowCount(int frameIndex, int partition) {
        return isKeyStaged ? mapBuild.getSegmentRowCount(frameIndex, partition) : intBuild.getSegmentRowCount(frameIndex, partition);
    }

    private long getSegmentStart(int frameIndex, int partition) {
        return isKeyStaged ? mapBuild.getSegmentStart(frameIndex, partition) : intBuild.getSegmentStart(frameIndex, partition);
    }

    /**
     * Gives every slot its probe of the frozen build, which {@link Slot#getProbe()} then returns.
     * Runs once per execution, after the build and the SYMBOL key translation, on the thread that
     * then dispatches the probes.
     */
    void bindProbes() {
        for (int i = 0, n = slots.size(); i < n; i++) {
            final Slot slot = slots.getQuick(i);
            // Casting here, once per slot per execution, keeps the reducers' hot call sites
            // on a single probe implementation; see the monomorphic-reducer rule.
            if (isKeyStaged) {
                if (isBuildPartitioned) {
                    if (slot.partitionedRecordProbe == null) {
                        slot.partitionedRecordProbe = ((FrozenHashJoinBuild.RecordKeyed) frozen).newProbe(slot.probeKeySink);
                    } else {
                        slot.partitionedRecordProbe.reopen();
                    }
                    slot.probe = slot.partitionedRecordProbe;
                } else {
                    if (slot.recordProbe == null) {
                        slot.recordProbe = ((FrozenHashJoinBuild.RecordKeyed) frozen).newProbe(slot.probeKeySink);
                    } else {
                        slot.recordProbe.reopen();
                    }
                    slot.probe = slot.recordProbe;
                }
            } else if (isBuildPartitioned) {
                if (slot.partitionedIntProbe == null) {
                    slot.partitionedIntProbe = ((FrozenHashJoinBuild.IntKeyed) frozen).newProbe();
                } else {
                    slot.partitionedIntProbe.reopen();
                }
                slot.probe = slot.partitionedIntProbe;
            } else {
                if (slot.intProbe == null) {
                    slot.intProbe = ((FrozenHashJoinBuild.IntKeyed) frozen).newProbe();
                } else {
                    slot.intProbe.reopen();
                }
                slot.probe = slot.intProbe;
            }
        }
    }

    /**
     * Sizes each SYMBOL key column's shared cache from the probe dictionary, fills it from the
     * build dictionary when that one is small, and gives every slot its own pair of symbol tables
     * over it. Symbol tables are not thread safe, so a worker cannot share one; the cache is a
     * function of the two dictionaries alone, so every worker does share that. On failure the
     * caller runs {@link #clear()}, which releases both. Runs once per execution, after the build
     * and before {@link #bindProbes()}.
     */
    void bindSymbolKeyTranslation(SqlExecutionCircuitBreaker circuitBreaker) {
        for (int key = 0, keyCount = symbolKeyCaches.size(); key < keyCount; key++) {
            final int probeColumn = symbolKeyProbeColumns.getQuick(key);
            final int buildColumn = symbolKeyBuildColumns.getQuick(key);
            final SymbolKeyTranslator cache = symbolKeyCaches.getQuick(key);
            // The owner's own tables answer for the dictionaries the cache is sized and filled
            // by, and then stay as slot -1's, so no table is taken that a slot does not keep.
            final StaticSymbolTable ownerProbeTable = (StaticSymbolTable) probeSymbols.newSymbolTable(probeColumn);
            final StaticSymbolTable ownerBuildTable = (StaticSymbolTable) buildFrames.getSymbolTableSource().newSymbolTable(buildColumn);
            // The probe's shared table is the dictionary that its views read: a table's symbol map,
            // or the in-memory overlay of a live view, which can hold a text under two keys.
            final boolean isProbeDictionaryEligible = SymbolKeyTranslator.isEligibleProbeDictionary(probeSymbols.getSymbolTable(probeColumn));
            cache.of(ownerProbeTable, ownerBuildTable, isProbeDictionaryEligible, memoryTracker, circuitBreaker);
            for (int i = 0, n = slots.size(); i < n; i++) {
                final SymbolTable probeTable = i == 0 ? ownerProbeTable : probeSymbols.newSymbolTable(probeColumn);
                final StaticSymbolTable buildTable = i == 0
                        ? ownerBuildTable
                        : (StaticSymbolTable) buildFrames.getSymbolTableSource().newSymbolTable(buildColumn);
                slots.getQuick(i).getSymbolKeyView(key).of(cache, probeTable, buildTable);
            }
        }
    }

    /**
     * Builds the open cursor's hash table. The host calls it once per execution, on first demand
     * for a result, on the thread that then dispatches the probes and before it dispatches any;
     * nothing here runs when the cursor merely opens.
     * <p>
     * Walks the build scan's page frames, keeping the key and the row id of every row that the
     * build filters pass. The frame count is known before the first row, and so is the row count
     * of an unfiltered build, interval scans included. The owner walks the frames itself, unless
     * the frames hold enough rows to build on the workers; see {@link #buildInRounds}. There, every
     * build knows its exact row count before it sizes anything. On the owner, a filtered build stays
     * unknown and grows as it goes: the unfiltered row count only bounds it, and sizing by that bound
     * over-allocates by the filter's selectivity. On failure the caller runs {@link #clear()}, which
     * closes the build, the filters and the frames.
     *
     * @param dispatcher runs the rounds of a parallel build, through a sequence whose atom is the
     *                   {@link HashJoinBuildHost} that returns this stage as the building one
     */
    void build(SqlExecutionCircuitBreaker circuitBreaker, PageFrameRoundDispatcher dispatcher) {
        assert frozen == null && buildFrames.getSymbolTableSource() != null && parquetBuildRows == 0 && parquetFrameCount == 0
                && parquetFrameRows == 0;
        buildFrames.loadFrames();
        final SymbolTableSource buildSymbols = buildFrames.getSymbolTableSource();
        buildFilterContext.initMemoryPools(buildFrames.getAddressCache(), memoryTracker);
        final boolean isFiltered = buildFilterContext.getFilter(-1) != null || buildOnFilter != null;
        final int frameCount = buildFrames.getFrameCount();
        if (isParallelBuildCapable && frameCount > 0 && buildFrames.getRowCount() >= parallelBuildMinRows) {
            buildInRounds(memoryTracker, circuitBreaker, dispatcher, buildSymbols, isFiltered);
        } else {
            buildOnOwner(memoryTracker, circuitBreaker, buildSymbols, isFiltered);
        }
        isBuildUnique = frozen.getRowCount() == frozen.getKeyCount();
    }

    /**
     * Has the frozen build replace each key's rows with their count, for a query that reads
     * nothing of a joined pair but that it exists; see {@link FrozenHashJoinBuild#countKeyRows()}.
     * The count runs here, so the counts are in place before the host publishes the first probe
     * task: on the workers for a build of more than one partition whose rows the tasks walk in long
     * enough runs, see {@link #countKeyRowsInRounds}, and on the owner otherwise. A failure closes
     * the build.
     */
    void countKeyRows(PageFrameRoundDispatcher dispatcher) {
        final int countTaskCount = isBuildPartitioned ? getKeyRowCountTaskCount() : 1;
        if (countTaskCount > 1) {
            countKeyRowsInRounds(countTaskCount, dispatcher);
        } else {
            frozen.countKeyRows();
        }
    }

    /**
     * The state of a slot: -1 for the owner, and 0 to the worker count less one for the workers.
     */
    Slot getSlot(int slotId) {
        return slots.getQuick(slotId + 1);
    }

    /**
     * Copies the build's payload columns when the probe is large enough to repay the copy; see
     * {@link #isPayloadCopyWorthIt}. A build in rounds copies on the workers, which also run the
     * probe that the copy speeds up, so a smaller probe repays it than repays the owner's copy,
     * which runs alone before the workers probe: each takes its own ratio. For a probe whose matches
     * can land on as many Parquet frames as the copy decodes, the ratio weighs only the native rows
     * that the build kept, although the copy holds every kept row: a build that reads a few Parquet
     * row groups among many native rows, or whose filter keeps none of its Parquet rows, copies only
     * for a probe that would repay the copy of its native rows alone, while a build of Parquet rows
     * alone copies for any such probe. A build whose Parquet frames with kept rows decode past a
     * probe reader's share of the decode cache, see
     * {@link HashJoinBuildFrames#isReaderCacheExceeded(long)}, copies for any such probe, however
     * many native rows it has. A reader decodes the payload columns alone, so a join key or a column
     * that only the build's filters read adds nothing to that test. A smaller probe weighs all the
     * kept rows, as for a native build. The build's key and row counts bound the build rows that one
     * probe row can match, whichever key it asks for, and build() counts the Parquet frames that hold
     * kept rows, once per execution; see {@link #isParquetInProbeReach(long, long, long, long)}. The
     * owner calls this once the probe's frames are known and before it dispatches the probes, which
     * then read the copy. On failure the caller runs {@link #clear()}, which releases the copy.
     */
    void maybeCopyPayload(long probeRows, SqlExecutionCircuitBreaker circuitBreaker, PageFrameRoundDispatcher dispatcher) {
        assert frozen != null && !isPayloadCopied;
        final double minProbeRatio = isBuiltInRounds ? copyParallelMinProbeRatio : copyMinProbeRatio;
        final long buildRows = frozen.getRowCount();
        final boolean isParquetReached = isParquetInProbeReach(probeRows, buildRows, frozen.getKeyCount(), parquetFrameCount);
        final boolean isParquetOverReaderCache = isParquetReached
                && parquetBuildRows > 0
                && buildFrames.isReaderCacheExceeded(parquetFrameRows);
        if (isPayloadCopyWorthIt(buildRows, parquetBuildRows, isParquetReached, isParquetOverReaderCache, buildFrames.getCopyRowSize(), probeRows, copyMaxSize, minProbeRatio)) {
            if (isBuiltInRounds) {
                // A parallel build keeps each partition's rows in one region of the heap, so a pass in
                // heap order would walk the frames once per partition, or it keeps each frame's rows
                // in bucket order. Each frame copies the rows it kept instead, on the workers.
                buildFrames.beginCopy(frozen, slots.size(), circuitBreaker);
                roundTaskRowCounts.clear();
                for (int frameIndex = 0, n = buildFrames.getFrameCount(); frameIndex < n; frameIndex++) {
                    long rows = 0;
                    for (int partition = 0, partitionCount = getPartitionCount(); partition < partitionCount; partition++) {
                        rows += getSegmentRowCount(frameIndex, partition);
                    }
                    roundTaskRowCounts.add(rows);
                }
                dispatcher.dispatchRoundAndAwait(COPY_FRAME_PAYLOAD, roundTaskRowCounts);
                buildFrames.publishCopy();
            } else {
                buildFrames.copyPayload(frozen, circuitBreaker);
            }
            isPayloadCopied = true;
        }
    }

    /**
     * One slot's build views and its bindings to the frozen build: the record that the slot's build
     * tasks read frames through, its key sinks and stager, its probe and the views that translate
     * its probe's SYMBOL keys. The probes and the views stay across executions: the next execution
     * reopens or rebinds them.
     */
    static final class Slot implements Mutable, QuietCloseable {
        // Null for the INT layout; the slot's own sink and stager of a staged build key, which a
        // frame task of a parallel build, or the owner's build, stages through.
        @Nullable
        final RecordSink buildKeySink;
        @Nullable
        final MapHashJoinBuild.KeyStager buildKeyStager;
        // The slot's view of the build frame it filters or appends.
        final PageFrameMemoryRecord buildRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        // Null for the INT layout; the slot's record probe borrows it for its whole life.
        @Nullable
        final RecordSink probeKeySink;
        // The INT layout's lone SYMBOL key translates through this view; null otherwise.
        @Nullable
        final SymbolKeyTranslator.View symbolKeyView;
        // A staged key with SYMBOL columns translates through this record's views; null otherwise.
        @Nullable
        private final SymbolKeyTranslatingRecord probeKeyRecord;
        // The INT layout's probe of a serial build, or of a parallel build of one partition; null for
        // a staged key, whose probe is the record probe.
        FrozenHashJoinBuild.IntProbe intProbe;
        // What the staged key sink reads: the key source record, or the translating view of it.
        Record keyRecord;
        // The INT layout's probe of a parallel build of more than one partition.
        FrozenHashJoinBuild.IntProbe partitionedIntProbe;
        // A staged key's probe of a parallel build of more than one partition.
        FrozenHashJoinBuild.RecordProbe partitionedRecordProbe;
        FrozenHashJoinBuild.RecordProbe recordProbe;
        // Whichever of the four probes bindProbes() bound for the open cursor's build; null otherwise.
        private FrozenHashJoinBuild.Probe probe;

        private Slot(
                @Nullable RecordSink probeKeySink,
                @Nullable RecordSink buildKeySink,
                @Nullable MapHashJoinBuild.KeyStager buildKeyStager,
                @Nullable SymbolKeyTranslatingRecord probeKeyRecord,
                @Nullable SymbolKeyTranslator.View symbolKeyView
        ) {
            this.probeKeySink = probeKeySink;
            this.buildKeySink = buildKeySink;
            this.buildKeyStager = buildKeyStager;
            this.probeKeyRecord = probeKeyRecord;
            this.symbolKeyView = symbolKeyView;
        }

        @Override
        public void clear() {
            buildRecord.of(null);
            probe = null;
            // The views borrow this execution's symbol tables and read a cache the stage is
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
            if (partitionedIntProbe != null) {
                partitionedIntProbe.close();
            }
            if (recordProbe != null) {
                recordProbe.close();
            }
            if (partitionedRecordProbe != null) {
                partitionedRecordProbe.close();
            }
            // The stager holds the last key it staged, and its scratch, in memory this execution
            // charged; a parallel build closed it after its frame tasks already.
            if (buildKeyStager != null) {
                buildKeyStager.close();
            }
        }

        @Override
        public void close() {
            Throwable failure = Misc.freeBestEffort(null, intProbe);
            intProbe = null;
            failure = Misc.freeBestEffort(failure, partitionedIntProbe);
            partitionedIntProbe = null;
            failure = Misc.freeBestEffort(failure, recordProbe);
            recordProbe = null;
            failure = Misc.freeBestEffort(failure, partitionedRecordProbe);
            partitionedRecordProbe = null;
            probe = null;
            failure = Misc.freeBestEffort(failure, buildKeyStager);
            failure = Misc.freeBestEffort(failure, probeKeyRecord);
            failure = Misc.freeBestEffort(failure, symbolKeyView);
            failure = Misc.freeBestEffort(failure, buildRecord);
            CairoException.rethrowCleanupFailure(failure);
        }

        /**
         * The probe of the open cursor's frozen build that {@link #bindProbes()} gave this slot.
         */
        FrozenHashJoinBuild.Probe getProbe() {
            return probe;
        }

        /**
         * Binds the record whose columns this slot's probe keys read: the staged key sink reads it,
         * through the views that translate its SYMBOL keys when the key has any. The INT layout
         * reads its key straight off the probe's own record, so it reads nothing through this one.
         */
        void ofKeySource(Record keySource) {
            if (probeKeyRecord != null) {
                probeKeyRecord.of(keySource);
                keyRecord = probeKeyRecord;
            } else {
                keyRecord = keySource;
            }
        }

        /**
         * The translation path of one SYMBOL key column, wherever this slot keeps it.
         */
        private SymbolKeyTranslator.View getSymbolKeyView(int key) {
            return probeKeyRecord != null ? probeKeyRecord.getView(key) : symbolKeyView;
        }
    }
}
