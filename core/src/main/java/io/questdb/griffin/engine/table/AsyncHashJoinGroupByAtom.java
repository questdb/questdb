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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
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
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslatingRecord;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
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
 * payload columns through.
 * <p>
 * init() runs when the cursor opens and reads no row: it opens the build scan and initializes
 * the filters of both inputs and the functions against the two open scans. Bind variables,
 * dynamic intervals and symbol tables are ready from there on, and every SqlException surfaces
 * from getCursor(). A caller that opens the cursor and reads nothing, such as EXPLAIN or LIMIT 0,
 * pays for no more than that.
 * <p>
 * {@link #build(SqlExecutionCircuitBreaker)} runs once per execution, when the cursor is first
 * asked for a result: it walks the build scan's page frames, filters each one and keeps the key
 * and the row id of every row that passes.
 * The owner builds alone, unless the build input holds at least the configured row count; then
 * the frames filter and sort their rows on the workers, and the workers fill the hash partitions
 * of {@link IntHashJoinBuild} or {@link MapHashJoinBuild}, through rounds of the frame sequence's
 * tasks. build() binds the SYMBOL key translation too: one cache per SYMBOL key column,
 * shared by every slot, plus a pair of symbol tables per slot. Last, it gives every slot its
 * probe of the frozen build. The build frames stay open until
 * clear(), because probes read payload columns through them and the translation's keyOf()
 * lookups resolve through their symbol tables. The owner dispatches the probe tasks after build()
 * returns, and the first task that UnorderedPageFrameSequence publishes is what publishes the
 * frozen build, its flags and the slots' probes to the reducers. clear() requires all reducers
 * to have finished and output consumers to be done.
 * <p>
 * A query that only counts the joined pairs of an INNER join reads nothing of a pair, so a build
 * that repeats a key need not hand its rows out one by one: build() has the frozen build replace
 * each key's rows with their count before any reducer runs, on the owner, or for most builds of
 * more than one partition in one more round of tasks, and the reducers add a count per probe
 * row; see {@link #isCountedByKey()}. Over a build of one row per key, a probe row that finds its
 * key adds one; see {@link #isCountedByMatch()}.
 */
public final class AsyncHashJoinGroupByAtom implements StatefulAtom, PerWorkerLockOwner {
    private static final UnorderedPageFrameReducer BUILD_PARTITION = AsyncHashJoinGroupByAtom::buildPartition;
    private static final UnorderedPageFrameReducer COPY_FRAME_PAYLOAD = AsyncHashJoinGroupByAtom::copyFramePayload;
    private static final UnorderedPageFrameReducer COUNT_PARTITIONS = AsyncHashJoinGroupByAtom::countPartitions;
    private static final UnorderedPageFrameReducer PARTITION_FRAME = AsyncHashJoinGroupByAtom::partitionFrame;
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
    // The value column of the lone count(*) of a query that only counts the joined pairs, -1 otherwise.
    private final int countValueIndex;
    private final AsyncFilterContext filterContext;
    private final HashJoinGroupByFunctions functions;
    // True when the query reads nothing of a joined pair but that it exists; see isPairCount().
    private final boolean isCountOnly;
    private final boolean isKeyCapacityPresized;
    private final boolean isKeyStaged;
    private final boolean isOuter;
    // True when the build may run on the workers: a filter context slot and a filter of its own
    // for every worker.
    private final boolean isParallelBuildCapable;
    // The INT layout's lone SYMBOL pair, whose probe keys the reducer translates per row.
    private final boolean isSymbolKey;
    // The RIGHT join bound in build table rows, Long.MAX_VALUE for the joins without one.
    private final long maxBuildTableRows;
    // The build input's row count, before any filter, from which the build runs on the workers.
    private final long parallelBuildMinRows;
    private final PerWorkerLocks perWorkerLocks;
    // The INT layout's only probe key column, -1 when the key sinks stage the key instead.
    private final int probeKeyColumn;
    // The rows each task of a parallel build's round processes, by which fiber workers batch tasks.
    private final LongList roundTaskRowCounts = new LongList();
    // This factory's own copy of the per-row aggregation step; see HashJoinGroupByRowUpdater.
    private final HashJoinGroupByRowUpdater rowUpdater = HashJoinGroupByRowUpdater.newInstance();
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
    private MapHashJoinBuild mapBuild;
    // Read by the frame tasks of a parallel build: whether a frame's rows go through the filters.
    private boolean isBuildFiltered;
    private boolean isBuildFiltersInitialized;
    // True when the open cursor's build has more than one partition, whose probes are a class of their own.
    private boolean isBuildPartitioned;
    private boolean isBuildUnique;
    // True when the open cursor's build ran on the workers, which lays the heap out by partition.
    private boolean isBuiltInRounds;
    // True when the open cursor's build holds each key's row count in place of the key's rows.
    private boolean isCountedByKey;
    // True when the open cursor's reducers count one pair per probe row that finds its key.
    private boolean isCountedByMatch;
    // True when the open cursor's build counted its key rows on the workers, in a round of tasks.
    private boolean isCountedInRounds;
    private boolean isFiltersInitialized;
    private boolean isFunctionsInitialized;
    private boolean isPayloadCopied;
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
    // The sequence that owns this atom, through which a parallel build dispatches its rounds.
    private UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence;
    // The open cursor's tracker, which init() takes for build(): the build runs after getCursor()
    // returned, when the execution context may no longer carry it.
    @Nullable
    private MemoryTracker memoryTracker;
    private long pairsPerCheck;
    // The open cursor's probe scan, whose dictionaries build() sizes the SYMBOL key translation by.
    private SymbolTableSource probeSymbols;
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
        this.buildFactory = buildFactory;
        this.buildFilterContext = buildFilterContext;
        this.buildOnFilter = buildOnFilter;
        this.workerBuildOnFilters = workerBuildOnFilters;
        this.functions = functions;
        this.filterContext = filterContext;
        this.isOuter = isOuter;
        this.isKeyStaged = metadata.isKeyStaged();
        this.isSymbolKey = metadata.isSymbolKey();
        this.isKeyCapacityPresized = metadata.isKeyCapacityPresized();
        this.maxBuildTableRows = metadata.getMaxBuildTableRows();
        // An outer join counts a probe row without a match too, which no key's row count covers.
        this.isCountOnly = !isOuter && isPairCount(functions);
        this.countValueIndex = isCountOnly ? functions.getGroupByFunctions(-1).getQuick(0).getValueIndex() : -1;
        // Each slot's probe reads the build's payload through a reader of its own, so the readers of
        // an execution are one per worker and the owner's, and share the Parquet decode cache.
        this.buildFrames = new HashJoinBuildFrames(
                engine.getConfiguration(),
                metadata.getBuildColumns(),
                buildFactory.getMetadata(),
                workerCount + 1
        );
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
        this.copyParallelMinProbeRatio = configuration.getSqlParallelHashJoinGroupByPayloadCopyParallelMinProbeRatio();
        this.parallelBuildMinRows = configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows();
        this.rowsPerPartition = configuration.getSqlParallelHashJoinGroupByBuildRowsPerPartition();
        final Function buildFilter = buildFilterContext.getFilter(-1);
        this.isParallelBuildCapable = buildFilterContext.getPerWorkerMemoryPools().size() >= workerCount
                && (buildFilter == null || buildFilter.isThreadSafe() || buildFilterContext.getFilter(0) != buildFilter)
                && (buildOnFilter == null || buildOnFilter.isThreadSafe() || workerBuildOnFilters != null);
        perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        try {
            // Sinks read the borrowed input metadatas when they are instantiated, so every sink
            // this execution will ever need is taken here, while the inputs are still alive.
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
            // The metadata does not outlive construction, so the slots share a copy of its mapping.
            final IntList payloadBuildColumns = new IntList();
            payloadBuildColumns.addAll(metadata.getBuildColumns());
            for (int i = -1; i < workerCount; i++) {
                // Sinks hold scratch state, so each slot probes and builds through ones of its own,
                // and so does the translating record the staged key sink reads from.
                Slot slot = new Slot(metadata.newRecord(), metadata.newProbeKeySink(), metadata.newBuildKeySink(),
                        isKeyStaged ? mapBuild.newKeyStager() : null,
                        hasSymbolKey && isKeyStaged
                                ? new SymbolKeyTranslatingRecord(metadata.getProbeColumnCount(), symbolKeyProbeColumns)
                                : null,
                        hasSymbolKey && !isKeyStaged ? new SymbolKeyTranslator.View() : null,
                        metadata.getProbeColumnCount(), payloadBuildColumns);
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
        isBuildFiltered = false;
        isBuildPartitioned = false;
        isBuildUnique = false;
        isBuiltInRounds = false;
        isCountedByKey = false;
        isCountedByMatch = false;
        isCountedInRounds = false;
        isPayloadCopied = false;
        parquetBuildRows = 0;
        parquetFrameCount = 0;
        parquetFrameRows = 0;
        failure = Misc.freeBestEffort(failure, intBuild);
        failure = Misc.freeBestEffort(failure, mapBuild);
        // The slots released their symbol tables above, so the shared caches go next.
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, symbolKeyCaches);
        // Functions, slots, filters and the build have released every symbol table view and
        // payload reader of these frames.
        try {
            buildFrames.clear();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        memoryTracker = null;
        probeSymbols = null;
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
        frameSequence = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Page frames of the open cursor's build input, zero until the cursor builds.
     */
    @TestOnly
    public int getBuildFrameCount() {
        return buildFrames.getFrameCount();
    }

    /**
     * Native bytes that the slots' build key stagers hold: none once the frame tasks of a parallel
     * build are done, and none for the INT layout, which stages no key.
     */
    @TestOnly
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
     * Hash partitions of the open cursor's build: one for a build on the owner.
     */
    @TestOnly
    public int getBuildPartitionCount() {
        return getPartitionCount();
    }

    /**
     * The build published for the open cursor, or null when no cursor is open, or when the open
     * cursor has not been asked for a result yet; see {@link #build(SqlExecutionCircuitBreaker)}.
     */
    @TestOnly
    public FrozenHashJoinBuild getFrozenBuild() {
        return frozen;
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
        return isKeyStaged ? mapBuild.hasFrameRegions() : intBuild.hasFrameRegions();
    }

    /**
     * True when the open cursor's build ran on the workers, in rounds of the frame sequence's tasks.
     */
    @TestOnly
    public boolean isBuiltInRounds() {
        return isBuiltInRounds;
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
     * sequence's tasks, each over a range of hash partitions, rather than in one pass on the owner.
     * A build of more than one partition takes the round unless its frames are too small for it;
     * see {@link IntHashJoinBuild#getKeyRowCountTaskCount()}.
     */
    @TestOnly
    public boolean isCountedInRounds() {
        return isCountedInRounds;
    }

    /**
     * True when the open cursor's probes read a copy of the build's payload columns rather than the
     * columns where they live; see {@link #maybeCopyPayload(long, SqlExecutionCircuitBreaker)}.
     */
    @TestOnly
    public boolean isPayloadCopied() {
        return isPayloadCopied;
    }

    /**
     * True when the open cursor's translation cache of the key-th SYMBOL key pair was filled from
     * the build dictionary rather than opened unresolved; see {@link SymbolKeyTranslator}. False
     * until the cursor builds, and for a factory without that many SYMBOL key pairs.
     */
    @TestOnly
    public boolean isSymbolKeyCacheFilledFromBuild(int key) {
        return key < symbolKeyCaches.size() && symbolKeyCaches.getQuick(key).isFilledFromBuild();
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
            assert frozen == null && buildFrames.getSymbolTableSource() == null;
            memoryTracker = executionContext.getMemoryTracker();
            probeSymbols = symbolTableSource;
            // The open scan serves the build input's symbol tables; build() walks its frames.
            buildFrames.open(buildFactory, executionContext);
            checkTableRowCounts(symbolTableSource, executionContext);
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
        final AsyncHashJoinGroupByAtom atom = (AsyncHashJoinGroupByAtom) sequence.getAtom();
        // The task holds no slot resource, but every task of the sequence takes a slot, as the
        // work-stealing strategy expects of its tasks.
        final int slotId = atom.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        try {
            if (atom.isKeyStaged) {
                atom.mapBuild.buildPartition(partition, breaker);
            } else {
                atom.intBuild.buildPartition(partition, breaker);
            }
        } finally {
            atom.release(slotId);
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
        final AsyncHashJoinGroupByAtom atom = (AsyncHashJoinGroupByAtom) sequence.getAtom();
        final int slotId = atom.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        // Copy readers are indexed from zero, the owner's first.
        final int reader = slotId + 1;
        try {
            for (int partition = 0, n = atom.getPartitionCount(); partition < n; partition++) {
                final long rowCount = atom.getSegmentRowCount(frameIndex, partition);
                if (rowCount > 0) {
                    final long lo = atom.getSegmentStart(frameIndex, partition);
                    atom.buildFrames.copyRows(reader, atom.frozen, lo, lo + rowCount, breaker);
                }
            }
        } finally {
            try {
                atom.buildFrames.releaseCopyBuffers(reader);
            } finally {
                atom.release(slotId);
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
        final AsyncHashJoinGroupByAtom atom = (AsyncHashJoinGroupByAtom) sequence.getAtom();
        // As a build partition task, it holds no slot resource but takes a slot.
        final int slotId = atom.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        try {
            final int partitionLo = task * atom.countTaskPartitionCount;
            final int partitionHi = partitionLo + atom.countTaskPartitionCount;
            if (atom.isKeyStaged) {
                atom.mapBuild.countPartitionKeyRows(partitionLo, partitionHi, breaker);
            } else {
                atom.intBuild.countPartitionKeyRows(partitionLo, partitionHi, breaker);
            }
        } finally {
            atom.release(slotId);
        }
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
        final AsyncHashJoinGroupByAtom atom = (AsyncHashJoinGroupByAtom) sequence.getAtom();
        final int slotId = atom.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        try {
            final PageFrameMemoryPool pool = atom.buildFilterContext.getMemoryPool(slotId);
            try {
                final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                final Slot slot = atom.getSlot(slotId);
                final PageFrameMemoryRecord record = slot.buildRecord;
                record.init(frameMemory);
                final long rowCount = atom.buildFrames.getFrameRowCount(frameIndex);
                if (atom.isBuildFiltered) {
                    final DirectLongList rows = atom.buildFilterContext.getFilteredRows(slotId);
                    atom.filterBuildFrame(slotId, frameMemory, record, rowCount, rows);
                    if (atom.isKeyStaged) {
                        atom.mapBuild.partitionFrame(frameIndex, record, slot.buildKeySink, slot.buildKeyStager, rows);
                    } else {
                        atom.intBuild.partitionFrame(frameIndex, record, atom.buildKeyColumn, rows);
                    }
                } else if (atom.isKeyStaged) {
                    atom.mapBuild.partitionFrame(frameIndex, record, slot.buildKeySink, slot.buildKeyStager, rowCount);
                } else {
                    atom.intBuild.partitionFrame(frameIndex, record, atom.buildKeyColumn, rowCount);
                }
            } finally {
                pool.releaseParquetBuffers();
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Sizes each SYMBOL key column's shared cache from the probe dictionary, fills it from the
     * build dictionary when that one is small, and gives every slot its own pair of symbol tables
     * over it. Symbol tables are not thread safe, so a worker cannot share one; the cache is a
     * function of the two dictionaries alone, so every worker does share that. The caller's
     * failure path releases both.
     */
    private void bindSymbolKeyTranslation(SqlExecutionCircuitBreaker circuitBreaker) {
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
     * Builds on the workers, in rounds of the frame sequence's tasks; see {@link IntHashJoinBuild}
     * for the layout, which {@link MapHashJoinBuild} shares. One task per frame filters the frame
     * and sorts the rows it keeps by their key's hash, then the owner sizes the heap for exactly the
     * kept rows and one task per partition fills the heap's region and the key table of its
     * partition. An unfiltered build sizes the heap first, and its frame tasks place their rows in
     * it, unless a staged key is too wide for that. A round returns once all its tasks have stopped,
     * also when it throws.
     */
    private void buildInRounds(MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker,
                               SymbolTableSource buildSymbols, boolean isFiltered) {
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
        frameSequence.dispatchRoundAndAwait(PARTITION_FRAME, roundTaskRowCounts);
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
        frameSequence.dispatchRoundAndAwait(BUILD_PARTITION, roundTaskRowCounts);
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
     * build alone that reads a variable-size column, and does more than compare columns, constants
     * and bind variables, takes the fused plan only under the presize condition, since the build
     * runs it on every build row, so the new compile also returns such a query to the ordinary
     * plan.</li>
     * <li>A RIGHT join keeps the ordinary plan when its build table holds more rows than the bound
     * allows, and an execution holds the build table to the same bound.</li>
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
        if (probeReader == null || buildReader == null) {
            return;
        }
        final long buildRows = buildReader.size();
        // Row counts stay far below 2^62, so twice the probe rows cannot overflow.
        if (buildRows > maxBuildTableRows || (isKeyCapacityPresized && buildRows > 2 * probeReader.size())) {
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
    private void countKeyRowsInRounds(int taskCount) {
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
            frameSequence.dispatchRoundAndAwait(COUNT_PARTITIONS, roundTaskRowCounts);
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
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts()) {
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

    // Partitions of the open cursor's build, and where each frame's rows of each lie in its heap.
    private int getPartitionCount() {
        return isKeyStaged ? mapBuild.getPartitionCount() : intBuild.getPartitionCount();
    }

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
        return probeKeyColumn;
    }

    HashJoinGroupByRowUpdater getRowUpdater() {
        return rowUpdater;
    }

    Slot getSlot(int slot) {
        return slots.getQuick(slot + 1);
    }

    /**
     * Binds the sequence that owns this atom, which a parallel build dispatches its rounds through.
     */
    void bindFrameSequence(UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence) {
        this.frameSequence = frameSequence;
    }

    /**
     * Builds the open cursor's hash table and binds every slot to it. The cursor calls it once per
     * execution, on first demand for a result, on the thread that then dispatches the probes and
     * before it dispatches any; nothing here runs when the cursor merely opens.
     * <p>
     * Walks the build scan's page frames, keeping the key and the row id of every row that the
     * build filters pass. The frame count is known before the first row, and so is the row count
     * of an unfiltered build, interval scans included. The owner walks the frames itself, unless
     * the frames hold enough rows to build on the workers; see {@link #buildInRounds}. There, every
     * build knows its exact row count before it sizes anything. On the owner, a filtered build stays
     * unknown and grows as it goes: the unfiltered row count only bounds it, and sizing by that bound
     * over-allocates by the filter's selectivity. The count pass, the SYMBOL key translation and the
     * slots' probes follow, all of which need the frozen build. On failure the caller closes the
     * cursor, whose clear() closes the build, the filters and the frames.
     */
    void build(SqlExecutionCircuitBreaker circuitBreaker) {
        assert frozen == null && buildFrames.getSymbolTableSource() != null && parquetBuildRows == 0 && parquetFrameCount == 0
                && parquetFrameRows == 0;
        buildFrames.loadFrames();
        final SymbolTableSource buildSymbols = buildFrames.getSymbolTableSource();
        buildFilterContext.initMemoryPools(buildFrames.getAddressCache(), memoryTracker);
        final boolean isFiltered = buildFilterContext.getFilter(-1) != null || buildOnFilter != null;
        final int frameCount = buildFrames.getFrameCount();
        if (isParallelBuildCapable && frameCount > 0 && buildFrames.getRowCount() >= parallelBuildMinRows) {
            buildInRounds(memoryTracker, circuitBreaker, buildSymbols, isFiltered);
        } else {
            buildOnOwner(memoryTracker, circuitBreaker, buildSymbols, isFiltered);
        }
        isBuildUnique = frozen.getRowCount() == frozen.getKeyCount();
        if (isCountOnly) {
            if (isBuildUnique) {
                // A key's rows are one row, so a lookup that finds the key finds its count.
                isCountedByMatch = true;
            } else {
                // The count runs here, so the counts are in place before the sequence publishes the
                // first probe task: on the workers for a build of more than one partition whose rows
                // the tasks walk in long enough runs, and on the owner otherwise. A failure closes
                // the build.
                final int countTaskCount = isBuildPartitioned ? getKeyRowCountTaskCount() : 1;
                if (countTaskCount > 1) {
                    countKeyRowsInRounds(countTaskCount);
                } else {
                    frozen.countKeyRows();
                }
                isCountedByKey = true;
            }
        }
        bindSymbolKeyTranslation(circuitBreaker);
        for (int i = 0; i < slots.size(); i++) {
            Slot slot = slots.getQuick(i);
            // Casting here, once per slot per execution, keeps the reducers' hot call sites
            // on a single probe implementation; see the monomorphic-reducer rule.
            final FrozenHashJoinBuild.Probe probe;
            if (isKeyStaged) {
                if (isBuildPartitioned) {
                    if (slot.partitionedRecordProbe == null) {
                        slot.partitionedRecordProbe = ((FrozenHashJoinBuild.RecordKeyed) frozen).newProbe(slot.probeKeySink);
                    } else {
                        slot.partitionedRecordProbe.reopen();
                    }
                    probe = slot.partitionedRecordProbe;
                } else {
                    if (slot.recordProbe == null) {
                        slot.recordProbe = ((FrozenHashJoinBuild.RecordKeyed) frozen).newProbe(slot.probeKeySink);
                    } else {
                        slot.recordProbe.reopen();
                    }
                    probe = slot.recordProbe;
                }
            } else if (isBuildPartitioned) {
                if (slot.partitionedIntProbe == null) {
                    slot.partitionedIntProbe = ((FrozenHashJoinBuild.IntKeyed) frozen).newProbe();
                } else {
                    slot.partitionedIntProbe.reopen();
                }
                probe = slot.partitionedIntProbe;
            } else {
                if (slot.intProbe == null) {
                    slot.intProbe = ((FrozenHashJoinBuild.IntKeyed) frozen).newProbe();
                } else {
                    slot.intProbe.reopen();
                }
                probe = slot.intProbe;
            }
            slot.joinedRecord.of(slot.probeRecord, slot.probeRecord, probe);
        }
    }

    /**
     * True when the open cursor's build has more than one hash partition, so that the reducers run
     * the loops over the slots' partitioned probes.
     */
    boolean isBuildPartitioned() {
        return isBuildPartitioned;
    }

    boolean isBuildUnique() {
        return isBuildUnique;
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
        return isKeyStaged;
    }

    boolean isOuter() {
        return isOuter;
    }

    /**
     * True for the INT layout's lone SYMBOL pair, whose probe key translates before every lookup.
     */
    boolean isSymbolKey() {
        return isSymbolKey;
    }

    int maybeAcquire(int workerId, boolean isOwner, SqlExecutionCircuitBreaker breaker) {
        return workerId == -1 && isOwner ? -1 : perWorkerLocks.acquireSlot(workerId, breaker);
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

    /**
     * True when some execution may copy the build's payload columns, as EXPLAIN reports.
     */
    boolean isPayloadCopyable() {
        return buildFrames.getCopyRowSize() > 0 && copyMaxSize >= buildFrames.getCopyRowSize();
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
    static boolean isPayloadCopyWorthIt(
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
     * then read the copy. On failure the caller closes the cursor, whose clear() releases the copy.
     */
    void maybeCopyPayload(long probeRows, SqlExecutionCircuitBreaker circuitBreaker) {
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
                frameSequence.dispatchRoundAndAwait(COPY_FRAME_PAYLOAD, roundTaskRowCounts);
                buildFrames.publishCopy();
            } else {
                buildFrames.copyPayload(frozen, circuitBreaker);
            }
            isPayloadCopied = true;
        }
    }

    boolean isProbeNeeded() {
        return isOuter || frozen.getRowCount() > 0;
    }

    /**
     * One execution slot's mutable views. A slot is also the symbol table source that its
     * functions initialize against, over the columns of the joined record: a probe column
     * resolves through the slot's probe record and a build payload column through a table of the
     * slot's own that it takes from the open build scan. Neither needs the frozen build, so the
     * functions initialize when the cursor opens, before the build.
     */
    static final class Slot implements QuietCloseable, SymbolTableSource {
        // Null for the INT layout; the slot's own sink and stager of a staged build key, which a
        // frame task of a parallel build, or the owner's build, stages through.
        @Nullable
        final RecordSink buildKeySink;
        @Nullable
        final MapHashJoinBuild.KeyStager buildKeyStager;
        // The slot's view of the build frame it filters or appends.
        final PageFrameMemoryRecord buildRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        final HashJoinGroupByRecord joinedRecord;
        // What the staged key sink reads: the probe record, or the translating view of it.
        final Record keyRecord;
        // Null for the INT layout; the slot's record probe borrows it for its whole life.
        final RecordSink probeKeySink;
        final ProbeRecord probeRecord = new ProbeRecord();
        // The INT layout's lone SYMBOL key translates through this view; null otherwise.
        @Nullable
        final SymbolKeyTranslator.View symbolKeyView;
        // The table of each build payload column that a function of this slot resolved, by payload
        // column; a slot's functions and its probe's reader each read through tables of their own.
        private final ObjList<SymbolTable> buildSymbolTables = new ObjList<>();
        // Build scan column of each payload column, in payload order; shared by every slot.
        private final IntList payloadBuildColumns;
        // Columns of the joined record that belong to the probe input; payload columns follow them.
        private final int probeColumnCount;
        // A staged key with SYMBOL columns translates through this record's views; null otherwise.
        @Nullable
        private final SymbolKeyTranslatingRecord probeKeyRecord;
        // The INT layout's probe of a serial build, or of a parallel build of one partition; null for
        // a staged key, whose probe is the record probe.
        FrozenHashJoinBuild.IntProbe intProbe;
        // The INT layout's probe of a parallel build of more than one partition.
        FrozenHashJoinBuild.IntProbe partitionedIntProbe;
        // A staged key's probe of a parallel build of more than one partition.
        FrozenHashJoinBuild.RecordProbe partitionedRecordProbe;
        FrozenHashJoinBuild.RecordProbe recordProbe;
        SimpleMapValue value;
        // The open cursor's build scan, which serves this slot's build symbol tables; null otherwise.
        private SymbolTableSource buildSymbols;

        Slot(
                HashJoinGroupByRecord joinedRecord,
                RecordSink probeKeySink,
                @Nullable RecordSink buildKeySink,
                @Nullable MapHashJoinBuild.KeyStager buildKeyStager,
                @Nullable SymbolKeyTranslatingRecord probeKeyRecord,
                @Nullable SymbolKeyTranslator.View symbolKeyView,
                int probeColumnCount,
                IntList payloadBuildColumns
        ) {
            this.joinedRecord = joinedRecord;
            this.probeKeySink = probeKeySink;
            this.buildKeySink = buildKeySink;
            this.buildKeyStager = buildKeyStager;
            this.probeKeyRecord = probeKeyRecord;
            this.symbolKeyView = symbolKeyView;
            this.probeColumnCount = probeColumnCount;
            this.payloadBuildColumns = payloadBuildColumns;
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
            failure = Misc.freeBestEffort(failure, partitionedIntProbe);
            partitionedIntProbe = null;
            failure = Misc.freeBestEffort(failure, recordProbe);
            recordProbe = null;
            failure = Misc.freeBestEffort(failure, partitionedRecordProbe);
            partitionedRecordProbe = null;
            failure = Misc.freeBestEffort(failure, buildKeyStager);
            failure = Misc.freeBestEffort(failure, probeKeyRecord);
            failure = Misc.freeBestEffort(failure, symbolKeyView);
            failure = Misc.freeBestEffort(failure, probeRecord);
            failure = Misc.freeBestEffort(failure, buildRecord);
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

        /**
         * The translation path of one SYMBOL key column, wherever this slot keeps it.
         */
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
            buildRecord.of(null);
            // The functions released their views of these tables already, and the build scan that
            // serves them closes after the slots.
            Misc.freeObjListIfCloseable(buildSymbolTables);
            buildSymbolTables.clear();
            buildSymbols = null;
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
