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
import io.questdb.cairo.sql.PageFrameCursor;
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
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
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

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * One hash join build: the build input's page frames, its filters, the INT or staged-key build,
 * the SYMBOL key translation, the payload copy and the per-slot probes of the frozen build. The
 * build factory, its filter context and the build ON filter with its worker copies are borrowed
 * from the operator that owns the stage; the stage owns the rest. Each slot of the stage, one per
 * worker plus the owner's, keeps the slot's build views and its bindings to the frozen build:
 * the probes, the probe key sink and the SYMBOL key translation views.
 * <p>
 * {@link #init(SymbolTableSource, SqlExecutionContext)} runs when the operator's cursor opens and
 * reads no row: it opens the build scan and initializes the build filters against it. The fused
 * GROUP BY builds in {@code ORDER_ASC}: its scan and every frame's rows run from the first row.
 * <p>
 * {@link #build(SqlExecutionCircuitBreaker, PageFrameRoundDispatcher)} runs once per execution,
 * when the cursor is first asked for a result: it walks the build scan's page frames, filters each
 * one and keeps the key and the row id of every row that passes. The owner builds alone, unless
 * the build input holds at least the configured row count, or spans more than one frame under a
 * WHERE filter that runs interpreted, whose cost per row its rows do not tell; then the frames
 * filter and sort their rows on the workers, and the workers fill the hash partitions of
 * {@link IntHashJoinBuild} or {@link MapHashJoinBuild}, through rounds of tasks that the
 * operator's dispatcher runs. Each task
 * of a round finds the stage through the {@link HashJoinBuildHost} atom of the dispatcher's
 * sequence. A parallel hash join first prepares every stage's build, see {@link #prepareBuild()},
 * and builds its stages that are too small for rounds of their own at once, when it has more than
 * one, in one round with one task per stage, each on the slot that its task takes; see
 * {@link #BUILD_STAGE}. Then {@link #bindProbes()} gives every slot its probe of the frozen build.
 * Once the probe scan has cut its frames, whose rows bound the keys that the execution translates,
 * {@link #bindSymbolKeyTranslation(long, boolean, SqlExecutionCircuitBreaker)} binds the SYMBOL key
 * translation: per SYMBOL key column, one cache shared by every slot, or, for a probe of few rows
 * next to the probe dictionary, a lazy cache of each slot's own, plus a pair of symbol tables per
 * slot. The build frames stay open until {@link #clear()}, because probes read payload columns
 * through them and the translation's keyOf() lookups resolve through their symbol tables. The owner
 * dispatches the probe tasks after the translation is bound, and the first task that its sequence
 * publishes is what publishes the frozen build, its flags, the slots' probes and the translation to
 * the reducers. clear() requires all reducers to have finished and output consumers to be done.
 * <p>
 * A query that only counts the joined pairs reads nothing of a pair, so a build that repeats a key
 * need not hand its rows out one by one: {@link #countKeyRows(PageFrameRoundDispatcher)} has the
 * frozen build replace each key's rows with their count before any reducer runs, on the owner, or
 * for most builds of more than one partition in one more round of tasks.
 * <p>
 * The fused GROUP BY's stage decides at compile time whether the key table is presized by the
 * build's rows; see {@link HashJoinGroupByMetadata#isKeyCapacityPresized()}. Every execution checks
 * that the tables have not outgrown that decision; see {@link #checkTableRowCounts}. A parallel hash
 * join's stage decides it in every execution instead, from the same table sizes, so its executions
 * need no such check; see {@link #isKeyCapacityPresizedFor}.
 * <p>
 * A parallel hash join's stage may run the leading conjuncts of a LEFT join's ON residual that read
 * the build alone over the build's rows, as the fused GROUP BY runs its ON condition on the build
 * alone; see {@link HashJoinStageSpec.BuildOnFilter}. Each execution decides whether it does, see
 * {@link #isBuildOnFilterApplied()}: always, or, for conjuncts that the size rule governs, only
 * when the presize decision holds. A build whose conjuncts throw on a row gives them up for the
 * execution rather than fail it, since the serial join may never run them over that row; the
 * fused GROUP BY's build fails instead.
 * <p>
 * A parallel hash join's stage builds in its input's scan order, so that each key's rows chain as
 * the serial hash join chains them over the input's cursor, and keeps one more slot than the
 * workers and the owner: the output slot, whose probe and payload reader the join's consumer
 * positions on the matches that the workers found; see {@link #getOutputSlot()}.
 * <p>
 * A later stage of a parallel hash join whose first join may swap its inputs reads its probe keys
 * off the left input's columns from one of two key sources, as each execution's orientation has
 * it: the probe scan's record, or, once the execution swaps the first join's inputs, the first
 * stage's payload reader, which lays the columns out as the left input as compiled lists them rather
 * than as its frames hold them. Such a stage keeps a second key layout for the second source, see
 * {@link #setKeySourceSwapped(boolean)}: a staged key then takes a key sink, a translating record
 * and record probes of each layout in every slot that probes, and an INT key reads the column that
 * its stepper picks. The build itself, and the SYMBOL key translation, whose dictionaries are the
 * left input's in either orientation, stay as they are.
 */
public final class HashJoinBuildStage implements Mutable, QuietCloseable {
    /**
     * One task per small stage of a parallel hash join's build round: builds the stage alone, on the
     * slot the task takes; see {@link HashJoinBuildRoundAtom#buildConcurrently}.
     */
    static final UnorderedPageFrameReducer BUILD_STAGE = HashJoinBuildStage::buildStage;
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
    // The order the build scan opens and walks its frames' rows in: ORDER_ASC or ORDER_DESC.
    private final int buildOrder;
    // Read per execution for the properties that a server reloads, such as the SYMBOL key
    // translation's slot cache capacity.
    private final CairoConfiguration configuration;
    // The payload copy's byte bound, and its probe to build row ratios for a copy on the owner and
    // a copy on the workers; see maybeCopyPayload().
    private final long copyMaxSize;
    private final double copyMinProbeRatio;
    private final double copyParallelMinProbeRatio;
    // True when an execution runs the build ON filter only when the key table is presized, as a
    // parallel hash join's stage does for conjuncts that the size rule governs.
    private final boolean isBuildOnFilterSized;
    // The fused GROUP BY's compile-time presize decision; see isKeyCapacityPresizedPerExecution.
    private final boolean isCompiledKeyCapacityPresized;
    // True when every execution decides from the open scans' table sizes whether the key table is
    // presized, as a parallel hash join does, rather than keep the compile-time decision.
    private final boolean isKeyCapacityPresizedPerExecution;
    private final boolean isKeyStaged;
    // True when the build may run on the workers: a filter context slot and a filter of its own
    // for every worker.
    private final boolean isParallelBuildCapable;
    // The INT layout's lone SYMBOL pair, whose probe keys the reducer translates per row.
    private final boolean isSymbolKey;
    // The build input's row count, before any filter, from which the build runs on the workers. A
    // build that spans more than one frame under a WHERE filter that runs interpreted runs there
    // whatever its rows; see prepareBuild().
    private final long parallelBuildMinRows;
    // The INT layout's only probe key column, -1 when the key sinks stage the key instead.
    private final int probeKeyColumn;
    // The slots that the build's own tasks run on: the owner's and one per worker.
    private final int roundSlotCount;
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
    // The builds the stage ran, over all its executions; see getBuildCount().
    private long buildCount;
    // The passes that indexed the chain lengths of the stage's builds, over all its executions; see
    // getChainLengthIndexCount().
    private long chainLengthIndexCount;
    // Read by the tasks of a parallel build's key row count: the consecutive partitions each counts.
    private int countTaskPartitionCount;
    // The frozen build of the open cursor, whichever of the two builds produced it.
    private FrozenHashJoinBuild frozen;
    // Exactly one of the two builds exists, as isKeyStaged says.
    private IntHashJoinBuild intBuild;
    // Read by the frame tasks of a parallel build, and by the task of a build alone: whether a
    // frame's rows go through the filters.
    private boolean isBuildFiltered;
    private boolean isBuildFiltersInitialized;
    // True when a parallel hash join's build gave the build ON filter up, once it threw on a row; a
    // task that sees it runs the filter over no more frames. Any task of the build's rounds may set
    // it, and the owner reads it once the rounds returned.
    private volatile boolean isBuildOnFilterAbandoned;
    // True when the open cursor's build runs the build ON filter over its rows, as init() decided.
    private boolean isBuildOnFilterActive;
    // True when the open cursor's build has more than one partition, whose probes are a class of their own.
    private boolean isBuildPartitioned;
    // True once prepareBuild() loaded the open cursor's build frames.
    private boolean isBuildPrepared;
    private boolean isBuildUnique;
    // True when the open cursor's build ran on the workers, which lays the heap out by partition.
    private boolean isBuiltInRounds;
    // True when the open cursor's build counted its key rows on the workers, in a round of tasks.
    private boolean isCountedInRounds;
    // The open cursor's presize decision; see isKeyCapacityPresizedPerExecution.
    private boolean isKeyCapacityPresized;
    // True when the open cursor's execution reads the probe keys from the second key source, whose
    // key layout the slots' swap sinks, translating records and record probes follow; see
    // setKeySourceSwapped().
    private boolean isKeySourceSwapped;
    // True when the open cursor's build runs in rounds of its own, as prepareBuild() decided.
    private boolean isLargeBuild;
    private boolean isPayloadCopied;
    // The presize hints of a build alone, which prepareBuild() takes on the owner: the rows the build
    // keeps, -1 when a filter thins them, and the keys among them, -1 when the key table grows.
    private long keyCountHint;
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
    // The rounds of tasks the stage dispatched, over all its executions; see getRoundCount().
    private long roundCount;
    // See keyCountHint.
    private long rowCountHint;

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
        this(configuration, buildFactory, buildFilterContext, buildOnFilter, workerBuildOnFilters, false, keyLayout,
                payloadBuildColumns, probeColumnCount, null, 0, isKeyCapacityPresized, false, ORDER_ASC, workerCount, 0);
    }

    /**
     * The stage of a parallel hash join without an ON filter on build columns alone: it builds in the
     * build input's scan order, decides in every execution whether the key table is presized, see
     * {@link #isKeyCapacityPresizedFor}, and keeps an output slot for the join's consumer, see
     * {@link #getOutputSlot()}. Every build column is a payload column.
     *
     * @param buildOrder the build input's scan order, {@code ORDER_ASC} or {@code ORDER_DESC}
     */
    HashJoinBuildStage(
            CairoConfiguration configuration,
            RecordCursorFactory buildFactory,
            AsyncFilterContext buildFilterContext,
            HashJoinKeyLayout keyLayout,
            IntList payloadBuildColumns,
            int probeColumnCount,
            int buildOrder,
            int workerCount
    ) {
        this(configuration, buildFactory, buildFilterContext, null, null, false, keyLayout, payloadBuildColumns,
                probeColumnCount, null, 0, false, true, buildOrder, workerCount, 1);
    }

    /**
     * A stage of a parallel hash join, as
     * {@link #HashJoinBuildStage(CairoConfiguration, RecordCursorFactory, AsyncFilterContext, HashJoinKeyLayout, IntList, int, int, int)}
     * takes its arguments, with the leading conjuncts of a LEFT join's ON residual that read the build
     * alone, which an execution may run over the build's rows, see {@link #isBuildOnFilterApplied()},
     * and, for a later stage of a join whose first join may swap its inputs, the key layout of the
     * second key source; see {@link #setKeySourceSwapped(boolean)}.
     *
     * @param buildOnFilter            the leading conjuncts of the ON residual that read the build
     *                                 alone, compiled against the build scan's columns, borrowed; null
     *                                 without them
     * @param workerBuildOnFilters     each worker's copy of them, borrowed; null when they are thread
     *                                 safe or absent
     * @param isBuildOnFilterSized     true when an execution runs them over the build's rows only when
     *                                 it presizes the key table
     * @param swapKeySourceLayout      the keys with the probe side read from the second key source:
     *                                 the same build keys, of the same types; null for a stage with
     *                                 one key source
     * @param swapKeySourceColumnCount the columns of the second key source
     */
    HashJoinBuildStage(
            CairoConfiguration configuration,
            RecordCursorFactory buildFactory,
            AsyncFilterContext buildFilterContext,
            @Nullable Function buildOnFilter,
            @Nullable ObjList<Function> workerBuildOnFilters,
            boolean isBuildOnFilterSized,
            HashJoinKeyLayout keyLayout,
            IntList payloadBuildColumns,
            int probeColumnCount,
            @Nullable HashJoinKeyLayout swapKeySourceLayout,
            int swapKeySourceColumnCount,
            int buildOrder,
            int workerCount
    ) {
        this(configuration, buildFactory, buildFilterContext, buildOnFilter, workerBuildOnFilters, isBuildOnFilterSized, keyLayout,
                payloadBuildColumns, probeColumnCount, swapKeySourceLayout, swapKeySourceColumnCount, false, true, buildOrder, workerCount, 1);
    }

    private HashJoinBuildStage(
            CairoConfiguration configuration,
            RecordCursorFactory buildFactory,
            AsyncFilterContext buildFilterContext,
            @Nullable Function buildOnFilter,
            @Nullable ObjList<Function> workerBuildOnFilters,
            boolean isBuildOnFilterSized,
            HashJoinKeyLayout keyLayout,
            IntList payloadBuildColumns,
            int probeColumnCount,
            @Nullable HashJoinKeyLayout swapKeySourceLayout,
            int swapKeySourceColumnCount,
            boolean isKeyCapacityPresized,
            boolean isKeyCapacityPresizedPerExecution,
            int buildOrder,
            int workerCount,
            int outputSlotCount
    ) {
        assert buildOrder == ORDER_ASC || buildOrder == ORDER_DESC;
        this.configuration = configuration;
        // The two layouts stage the same build keys, as the same types, so the build is one.
        assert swapKeySourceLayout == null || swapKeySourceLayout.isKeyStaged() == keyLayout.isKeyStaged();
        assert swapKeySourceLayout == null || swapKeySourceLayout.isSymbolKey() == keyLayout.isSymbolKey();
        assert swapKeySourceLayout == null || swapKeySourceLayout.getSymbolKeyProbeColumns().size() == keyLayout.getSymbolKeyProbeColumns().size();
        this.buildFactory = buildFactory;
        this.buildFilterContext = buildFilterContext;
        // Only a parallel hash join's stage decides per execution whether it runs the build ON filter.
        assert !isBuildOnFilterSized || (buildOnFilter != null && isKeyCapacityPresizedPerExecution);
        this.buildOnFilter = buildOnFilter;
        this.workerBuildOnFilters = workerBuildOnFilters;
        this.isBuildOnFilterSized = isBuildOnFilterSized;
        this.isKeyStaged = keyLayout.isKeyStaged();
        this.isSymbolKey = keyLayout.isSymbolKey();
        this.isCompiledKeyCapacityPresized = isKeyCapacityPresized;
        this.isKeyCapacityPresizedPerExecution = isKeyCapacityPresizedPerExecution;
        this.buildOrder = buildOrder;
        this.roundSlotCount = workerCount + 1;
        // Each slot's probe reads the build's payload through a reader of its own, so the readers of
        // an execution are one per worker, the owner's and the output slot's, and share the Parquet
        // decode cache.
        this.buildFrames = new HashJoinBuildFrames(
                configuration,
                payloadBuildColumns,
                buildFactory.getMetadata(),
                roundSlotCount + outputSlotCount
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
        for (int i = -1; i < workerCount + outputSlotCount; i++) {
            // Sinks hold scratch state, so each slot probes and builds through ones of its own,
            // and so does the translating record the staged key sink reads from. The output slot
            // looks no key up, so only the slots that probe take the second key source's.
            final boolean isSwapSlot = swapKeySourceLayout != null && isKeyStaged && i < workerCount;
            slots.add(new Slot(
                    keyLayout.newProbeKeySink(),
                    keyLayout.newBuildKeySink(),
                    isKeyStaged ? mapBuild.newKeyStager() : null,
                    hasSymbolKey && isKeyStaged
                            ? new SymbolKeyTranslatingRecord(probeColumnCount, symbolKeyProbeColumns)
                            : null,
                    hasSymbolKey && !isKeyStaged ? new SymbolKeyTranslator.View() : null,
                    isSwapSlot ? swapKeySourceLayout.newProbeKeySink() : null,
                    isSwapSlot && hasSymbolKey
                            ? new SymbolKeyTranslatingRecord(swapKeySourceColumnCount, swapKeySourceLayout.getSymbolKeyProbeColumns())
                            : null
            ));
        }
    }

    /**
     * True when an execution of a parallel hash join presizes its key table by the build's rows: both
     * scans read a table, and the build's table holds no more rows than the probe's, as
     * {@link TableReader#size()} counts them at the time the scans opened. Rows bound the keys, but a
     * build whose table outgrew the probe's often repeats its keys, so it lets its key table grow
     * instead, and so does a build over a scan without a table, such as {@code read_parquet()}, whose
     * size is not known before its frames load. A live view's scan counts the rows of its table. The
     * decision only sizes the key table, so it never changes a result, and an execution whose tables
     * changed size since the query compiled decides again from their new sizes.
     */
    public static boolean isKeyCapacityPresizedFor(SymbolTableSource probeScan, SymbolTableSource buildScan) {
        if (!(probeScan instanceof TablePageFrameCursor probeCursor) || !(buildScan instanceof TablePageFrameCursor buildCursor)) {
            return false;
        }
        final TableReader probeReader = probeCursor.getTableReader();
        final TableReader buildReader = buildCursor.getTableReader();
        // An open table scan always holds its reader.
        assert probeReader != null && buildReader != null;
        return buildReader.size() <= probeReader.size();
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
     * True when a build copies its payload columns for a probe of this many rows: a payload that
     * the copy can hold, a copy within the byte bound, and a probe that the copy repays. A payload
     * that the copy cannot hold, one with a STRING, VARCHAR, BINARY or ARRAY column, has a copy row
     * size of zero, see {@link HashJoinBuildFrames#getCopyRowSize()}, so it never copies, whatever
     * the probe and the Parquet decode cache: its readers read the columns where they live. A probe
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
    public static boolean isPayloadCopyWorthIt(
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
     * Tells a filter of the open cursor's execution, if any, that the cursor closed, and folds what
     * that throws into the cleanup failure so far, so that the caller goes on releasing the rest.
     */
    static Throwable cursorClosed(Throwable failure, @Nullable Function function) {
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
        isBuildOnFilterActive = false;
        isBuildOnFilterAbandoned = false;
        isBuildPartitioned = false;
        isBuildUnique = false;
        isBuiltInRounds = false;
        isCountedInRounds = false;
        isKeyCapacityPresized = false;
        isBuildPrepared = false;
        isLargeBuild = false;
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
     * The builds the stage ran since it was constructed, one per execution that asked for a result:
     * a test reads it before and after executions, to count the builds of each.
     */
    @TestOnly
    public long getBuildCount() {
        return buildCount;
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
     * The passes that indexed the chain lengths of the stage's builds since it was constructed, at
     * most one per execution, for a count of a parallel hash join's rows; see
     * {@link #indexChainLengths(SqlExecutionCircuitBreaker)}. A test reads it before and after
     * executions, to count the passes of each.
     */
    @TestOnly
    public long getChainLengthIndexCount() {
        return chainLengthIndexCount;
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
     * Times the open cursor's INT key tables grew past their size as they filled, each a rehash of
     * every key the table held: none for a table presized for the build's keys. Zero for a staged
     * key, whose map is not counted.
     */
    @TestOnly
    public int getKeyTableGrowCount() {
        return isKeyStaged ? 0 : intBuild.getKeyTableGrowCount();
    }

    /**
     * Hash partitions of the open cursor's build: one for a build on the owner.
     */
    public int getPartitionCount() {
        return isKeyStaged ? mapBuild.getPartitionCount() : intBuild.getPartitionCount();
    }

    /**
     * Bytes of the open cursor's payload copy, which the execution's tracker counts; zero without one.
     */
    @TestOnly
    public long getPayloadCopySize() {
        return buildFrames.getCopySize();
    }

    /**
     * The payload reader of a slot's probe of the open cursor's build: -1 for the owner's, 0 to the
     * worker count less one for the workers', and, for a parallel hash join's stage, the worker count
     * for the output slot's, which the join's consumer reads through. Null for a slot that has no
     * probe yet. A test reads it while the cursor is open.
     */
    @TestOnly
    public HashJoinPayloadSource.Reader getPayloadReader(int slotId) {
        final FrozenHashJoinBuild.Probe probe = slots.getQuick(slotId + 1).getProbe();
        return probe != null ? (HashJoinPayloadSource.Reader) probe.getRecord() : null;
    }

    /**
     * The times that the payload readers of the open cursor's probes, every slot's, bound their page
     * frame records since the probes were created; see
     * {@link HashJoinBuildFrames#getRecordBindCount(HashJoinPayloadSource.Reader)}. A slot that has not
     * probed adds nothing. A test reads it while the cursor is open and its tasks are done.
     */
    @TestOnly
    public long getPayloadRecordBindCount() {
        long count = 0;
        for (int i = 0, n = slots.size(); i < n; i++) {
            final FrozenHashJoinBuild.Probe probe = slots.getQuick(i).getProbe();
            if (probe != null) {
                count += HashJoinBuildFrames.getRecordBindCount((HashJoinPayloadSource.Reader) probe.getRecord());
            }
        }
        return count;
    }

    /**
     * The INT layout's only probe key column, -1 when the key sinks stage the key instead.
     */
    public int getProbeKeyColumn() {
        return probeKeyColumn;
    }

    /**
     * The rounds of tasks that the stage dispatched through its round dispatcher since it was
     * constructed: two for a parallel build, one for a payload copy in rounds and one for a key row
     * count in rounds. A test reads it before and after executions, to count the rounds of each.
     */
    @TestOnly
    public long getRoundCount() {
        return roundCount;
    }

    /**
     * Bytes of the open cursor's shared translation cache of the key-th SYMBOL key pair, which the
     * execution's tracker counts: four per probe key, and none when the slots translate through
     * caches of their own.
     */
    @TestOnly
    public long getSymbolKeyCacheSizeInBytes(int key) {
        return symbolKeyCaches.getQuick(key).getSizeInBytes();
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
        assert frozen == null && buildFrames.getSymbolTableSource() == null && !isKeyCapacityPresizedPerExecution;
        memoryTracker = executionContext.getMemoryTracker();
        this.probeSymbols = probeSymbols;
        // The open scan serves the build input's symbol tables; build() walks its frames.
        buildFrames.open(buildFactory, buildOrder, executionContext);
        isKeyCapacityPresized = isCompiledKeyCapacityPresized;
        checkTableRowCounts(probeSymbols, executionContext);
        isBuildOnFilterActive = buildOnFilter != null;
        initBuildFilters(executionContext);
    }

    /**
     * Adopts the build scan's page frame cursor, which the caller opened in the build input's scan
     * order, decides whether this execution presizes the key table, see
     * {@link #isKeyCapacityPresizedFor}, and initializes the build filters; it reads no row, as
     * {@link #init(SymbolTableSource, SqlExecutionContext)} reads none. The stage owns the cursor from
     * here on, whether this returns or throws; on failure the caller runs {@link #clear()}, which
     * closes it with the rest of what this opened.
     *
     * @param probeSymbols the open probe scan, whose dictionaries the SYMBOL key translation reads
     * @param buildCursor  the open page frame cursor of the build input
     */
    public void init(SymbolTableSource probeSymbols, SqlExecutionContext executionContext, PageFrameCursor buildCursor) throws SqlException {
        assert frozen == null && buildFrames.getSymbolTableSource() == null && isKeyCapacityPresizedPerExecution;
        memoryTracker = executionContext.getMemoryTracker();
        this.probeSymbols = probeSymbols;
        buildFrames.open(buildCursor, buildFactory.getMetadata(), executionContext);
        isKeyCapacityPresized = isKeyCapacityPresizedFor(probeSymbols, buildCursor);
        // The size rule reads the sizes that the presize decision reads.
        isBuildOnFilterActive = buildOnFilter != null && (!isBuildOnFilterSized || isKeyCapacityPresized);
        initBuildFilters(executionContext);
    }

    /**
     * True when the open cursor's build ran the build ON filter over its rows: the build has one, the
     * execution runs it, as {@link #init(SymbolTableSource, SqlExecutionContext, PageFrameCursor)}
     * decided, and, for a parallel hash join, the filter threw on none of the rows it ran over, which
     * would have made the build give it up. The pairs of a build that ran it run the rest of the ON
     * residual alone, and the pairs of any other build the whole residual. Read once the build ran.
     */
    public boolean isBuildOnFilterApplied() {
        return isBuildOnFilterActive && !isBuildOnFilterAbandoned;
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
     * True when the open cursor's build presizes its key table by the build's rows; see
     * {@link #isKeyCapacityPresizedFor} for a stage that decides it per execution.
     */
    @TestOnly
    public boolean isKeyCapacityPresized() {
        return isKeyCapacityPresized;
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
     * True when the open cursor's execution translates the key-th SYMBOL key pair through each
     * slot's own cache, as a probe of few rows next to the probe dictionary does; see
     * {@link SymbolKeyTranslator}. False until the translation is bound, and for a stage without
     * that many SYMBOL key pairs.
     */
    @TestOnly
    public boolean isSymbolKeySlotCached(int key) {
        return key < symbolKeyCaches.size() && symbolKeyCaches.getQuick(key).isSlotCached();
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
     * The one task of a small stage in a parallel hash join's round of concurrent builds: builds the
     * stage alone, as the owner builds a small stage, but on the slot that the task takes, with the
     * slot's own filters, memory pool and build views, so that the stages of the round build at once
     * on as many threads. A stage takes part only when it can build on the workers, see
     * {@link #isConcurrentBuildCapable()}, and each stage has one task, so no two tasks share a
     * stage's slot.
     */
    private static void buildStage(
            int workerId,
            PageFrameMemoryRecord unused,
            int task,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence,
            UnorderedPageFrameSequence<?> stealingSequence
    ) {
        final HashJoinBuildRoundAtom host = (HashJoinBuildRoundAtom) sequence.getAtom();
        final HashJoinBuildStage stage = host.getConcurrentStage(task);
        final int slotId = host.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        try {
            stage.buildOnOwner(slotId, breaker);
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

    /**
     * True when a failure of a parallel hash join's build ON filter on a row leaves the filter to
     * the pairs, see {@link #applyAbandonableBuildOnFilter}: whatever a function raises for a value
     * that it cannot convert, compute or compare, rather than a cancellation or a memory limit
     * breach, which fail the build as they fail any, or a JVM error other than a stack overflow.
     * Functions raise such failures in many forms: regexp_replace() raises its own as critical
     * errors, and a regex raises a stack overflow on a long value, which unwinds the stack to the
     * build's loop. Should a failure of the environment pass for one, the pairs raise it again, as
     * they run the filter where the serial join does.
     */
    private static boolean isAbandonable(Throwable th) {
        if (th instanceof CairoException e) {
            return !e.isInterruption() && !e.isOutOfMemory();
        }
        return th instanceof RuntimeException || th instanceof StackOverflowError;
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
                        stage.mapBuild.partitionFrame(frameIndex, record, slot.buildKeySink, slot.buildKeyStager, rows, stage.buildOrder);
                    } else {
                        stage.intBuild.partitionFrame(frameIndex, record, stage.buildKeyColumn, rows, stage.buildOrder);
                    }
                } else if (stage.isKeyStaged) {
                    stage.mapBuild.partitionFrame(frameIndex, record, slot.buildKeySink, slot.buildKeyStager, rowCount, stage.buildOrder);
                } else {
                    stage.intBuild.partitionFrame(frameIndex, record, stage.buildKeyColumn, rowCount, stage.buildOrder);
                }
            } finally {
                pool.releaseParquetBuffers();
            }
        } finally {
            host.release(slotId);
        }
    }

    /**
     * Runs a parallel hash join's build ON filter over the frame's rows, or over the rows its WHERE
     * filter kept, and leaves in {@code rows} the ones it passes, ascending. Once the filter fails on
     * a row, see {@link #isAbandonable}, the build gives it up for the execution, see
     * {@link HashJoinStageSpec.BuildOnFilter}: it keeps that row and every later one of the frame, and
     * marks the filter abandoned, so that later frames run it no more. The slot's copy of the filter
     * that failed runs no more in the execution.
     *
     * @param isPreFiltered true when {@code rows} holds the rows that the WHERE filter kept, false
     *                      when it is empty and the filter runs over the frame's rows
     */
    private void applyAbandonableBuildOnFilter(
            Function onFilter,
            boolean isPreFiltered,
            DirectLongList rows,
            PageFrameMemoryRecord record,
            long rowCount
    ) {
        final long n = isPreFiltered ? rows.size() : rowCount;
        long kept = 0;
        long p = 0;
        for (; p < n; p++) {
            final long row = isPreFiltered ? rows.get(p) : p;
            record.setRowIndex(row);
            final boolean isPassed;
            try {
                isPassed = onFilter.getBool(record);
            } catch (Throwable th) {
                if (!isAbandonable(th)) {
                    throw th;
                }
                break;
            }
            if (isPassed) {
                if (isPreFiltered) {
                    rows.set(kept++, row);
                } else {
                    rows.add(row);
                }
            }
        }
        if (p < n) {
            isBuildOnFilterAbandoned = true;
            for (; p < n; p++) {
                if (isPreFiltered) {
                    rows.set(kept++, rows.get(p));
                } else {
                    rows.add(p);
                }
            }
        }
        if (isPreFiltered) {
            rows.setPos(kept);
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
    private void buildInRounds(SqlExecutionCircuitBreaker circuitBreaker, PageFrameRoundDispatcher dispatcher) {
        assert isLargeBuild;
        final SymbolTableSource buildSymbols = buildFrames.getSymbolTableSource();
        for (int i = 0, n = slots.size(); i < n; i++) {
            slots.getQuick(i).buildRecord.of(buildSymbols);
        }
        final boolean isFiltered = isBuildFiltered;
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
        roundCount++;
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
        roundCount++;
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

    // Walks the build frames alone, appending each frame's kept rows as it goes, on one slot: the
    // owner's, -1, or the slot of a task in a round of concurrent builds, whose own breaker the build
    // then checks. The build is small, so prepareBuild() took its presize hints on the owner.
    private void buildOnOwner(int slotId, SqlExecutionCircuitBreaker circuitBreaker) {
        assert !isLargeBuild;
        final boolean isFiltered = isBuildFiltered;
        final Slot slot = getSlot(slotId);
        final PageFrameMemoryRecord buildRecord = slot.buildRecord;
        buildRecord.of(buildFrames.getSymbolTableSource());
        if (isKeyStaged) {
            mapBuild.open(memoryTracker, circuitBreaker);
            mapBuild.reserve(rowCountHint, keyCountHint);
        } else {
            intBuild.open(memoryTracker, circuitBreaker);
            intBuild.reserve(rowCountHint, keyCountHint);
        }
        final PageFrameMemoryPool pool = buildFilterContext.getMemoryPool(slotId);
        final DirectLongList rows = buildFilterContext.getFilteredRows(slotId);
        for (int frameIndex = 0, frameCount = buildFrames.getFrameCount(); frameIndex < frameCount; frameIndex++) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
            buildRecord.init(frameMemory);
            final long rowCount = buildFrames.getFrameRowCount(frameIndex);
            try {
                if (isFiltered) {
                    filterBuildFrame(slotId, frameMemory, buildRecord, rowCount, rows);
                    // A SYMBOL key keeps the build's own symbol keys; the probe translates into them.
                    if (isKeyStaged) {
                        mapBuild.appendFrame(buildRecord, slot.buildKeySink, rows, buildOrder);
                    } else {
                        intBuild.appendFrame(buildRecord, buildKeyColumn, rows, buildOrder);
                    }
                } else if (isKeyStaged) {
                    mapBuild.appendFrame(buildRecord, slot.buildKeySink, rowCount, buildOrder);
                } else {
                    intBuild.appendFrame(buildRecord, buildKeyColumn, rowCount, buildOrder);
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
            roundCount++;
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
     * back to the interpreted filter. The ON filter runs only in an execution that runs it, see
     * {@link #isBuildOnFilterApplied()}, and a parallel hash join's build that gave it up keeps the
     * frame's rows that the WHERE filter kept, or all of them.
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
        if (!isBuildOnFilterActive) {
            // The WHERE filter alone filters the build, so it ran above.
            assert filter != null;
            return;
        }
        final Function onFilter = slotId == -1 || workerBuildOnFilters == null ? buildOnFilter : workerBuildOnFilters.getQuick(slotId);
        if (isKeyCapacityPresizedPerExecution) {
            // A parallel hash join's stage.
            if (!isBuildOnFilterAbandoned) {
                applyAbandonableBuildOnFilter(onFilter, filter != null, rows, record, rowCount);
            } else if (filter == null) {
                for (long r = 0; r < rowCount; r++) {
                    rows.add(r);
                }
            }
        } else {
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
     * {@link HashJoinGroupByMetadata#isKeyCapacityPresized()}, and, for a stage that decides it per
     * execution, {@link #isKeyCapacityPresizedFor}. The INT layout's lone SYMBOL key
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

    private void initBuildFilters(SqlExecutionContext executionContext) throws SqlException {
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

    // True when the build input's WHERE filter runs interpreted: the JIT did not compile it, as with a
    // regex, a LIKE, or any filter with the JIT off. Its cost per row has no bound, so the input's rows
    // do not measure the build's work, and the ordinary plan ran it on the workers through Async Filter.
    private boolean isBuildFilterInterpreted() {
        return buildFilterContext.getFilter(-1) != null && buildFilterContext.getCompiledFilter() == null;
    }

    /**
     * Gives every slot its probe of the frozen build, which {@link Slot#getProbe()} then returns.
     * Runs once per execution, after the build, on the thread that then dispatches the probes.
     */
    void bindProbes() {
        for (int i = 0, n = slots.size(); i < n; i++) {
            final Slot slot = slots.getQuick(i);
            // A probe makes its payload reader when it is first bound, and keeps it. The output
            // slot's readers read the payload of every row the join returns, so they take the whole
            // Parquet decode budget, as the serial hash join's reader does, and the other slots'
            // readers their shares of it.
            buildFrames.setOutputReaders(i >= roundSlotCount);
            // Casting here, once per slot per execution, keeps the reducers' hot call sites
            // on a single probe implementation; see the monomorphic-reducer rule.
            if (isKeyStaged && isKeySourceSwapped && slot.swapProbeKeySink != null) {
                // A record probe borrows its key sink for life, so the second key source's layout
                // keeps record probes of its own.
                if (isBuildPartitioned) {
                    if (slot.swapPartitionedRecordProbe == null) {
                        slot.swapPartitionedRecordProbe = ((FrozenHashJoinBuild.RecordKeyed) frozen).newProbe(slot.swapProbeKeySink);
                    } else {
                        slot.swapPartitionedRecordProbe.reopen();
                    }
                    slot.probe = slot.swapPartitionedRecordProbe;
                } else {
                    if (slot.swapRecordProbe == null) {
                        slot.swapRecordProbe = ((FrozenHashJoinBuild.RecordKeyed) frozen).newProbe(slot.swapProbeKeySink);
                    } else {
                        slot.swapRecordProbe.reopen();
                    }
                    slot.probe = slot.swapRecordProbe;
                }
            } else if (isKeyStaged) {
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
        buildFrames.setOutputReaders(false);
    }

    /**
     * Opens each SYMBOL key column's translation for a probe of {@code probeRowCount} rows, and
     * gives every slot its own pair of symbol tables over it. A probe that reads enough rows per
     * key of the probe dictionary, by the operator's rule, see
     * {@link SymbolKeyTranslator#of(StaticSymbolTable, StaticSymbolTable, boolean, long, boolean, int, MemoryTracker, SqlExecutionCircuitBreaker)},
     * gets a shared cache sized by that dictionary, filled from the build dictionary when that one
     * is small; any other leaves each slot to translate through a lazy cache of its own, of at most
     * {@code cairo.sql.join.symbol.translation.cache.capacity} entries, which the server may
     * reload, so every execution reads it. Symbol tables are not
     * thread safe, so a worker cannot share one; the shared cache is a function of the two
     * dictionaries alone, so every worker does share that. On failure the caller runs
     * {@link #clear()}, which releases both. Runs once per execution, after the build and once the
     * probe scan has cut its frames, on the thread that then dispatches the probes, before the
     * payload copy and the first probe task.
     *
     * @param probeRowCount      the probe's frame rows, or fewer when the execution reads fewer first
     * @param isParallelHashJoin true for a parallel hash join's stage, false for the fused GROUP BY's
     */
    void bindSymbolKeyTranslation(long probeRowCount, boolean isParallelHashJoin, SqlExecutionCircuitBreaker circuitBreaker) {
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
            cache.of(ownerProbeTable, ownerBuildTable, isProbeDictionaryEligible, probeRowCount, isParallelHashJoin,
                    configuration.getSqlJoinSymbolTranslationCacheCapacity(), memoryTracker, circuitBreaker);
            for (int i = 0, n = slots.size(); i < n; i++) {
                final SymbolTable probeTable = i == 0 ? ownerProbeTable : probeSymbols.newSymbolTable(probeColumn);
                final StaticSymbolTable buildTable = i == 0
                        ? ownerBuildTable
                        : (StaticSymbolTable) buildFrames.getSymbolTableSource().newSymbolTable(buildColumn);
                slots.getQuick(i).getSymbolKeyView(key, isKeySourceSwapped).of(cache, probeTable, buildTable);
            }
        }
    }

    /**
     * Picks the key source that the open cursor's execution reads probe keys from, for a later stage
     * of a parallel hash join whose first join may swap its inputs: the probe scan's record, or, true,
     * the first stage's payload reader, as the second key layout lays it out. The SYMBOL key
     * translation and the record probes then follow that layout; see
     * {@link Slot#ofSwapKeySource(Record)}. The owner calls it before the execution's build.
     */
    void setKeySourceSwapped(boolean isKeySourceSwapped) {
        assert frozen == null;
        this.isKeySourceSwapped = isKeySourceSwapped;
    }

    /**
     * Builds the open cursor's hash table. The host calls it once per execution, on first demand
     * for a result, on the thread that then dispatches the probes and before it dispatches any;
     * nothing here runs when the cursor merely opens.
     * <p>
     * Walks the build scan's page frames, keeping the key and the row id of every row that the
     * build filters pass. The frame count is known before the first row, and so is the row count
     * of an unfiltered build, interval scans included. The owner walks the frames itself, unless
     * the frames hold enough rows to build on the workers, or more than one of them run a WHERE
     * filter that runs interpreted; see {@link #prepareBuild()} and {@link #buildInRounds}. There,
     * every build knows its exact row count before it sizes anything. On the owner, a filtered build stays
     * unknown and grows as it goes: the unfiltered row count only bounds it, and sizing by that bound
     * over-allocates by the filter's selectivity. On failure the caller runs {@link #clear()}, which
     * closes the build, the filters and the frames.
     *
     * @param dispatcher runs the rounds of a parallel build, through a sequence whose atom is the
     *                   {@link HashJoinBuildHost} that returns this stage as the building one
     */
    void build(SqlExecutionCircuitBreaker circuitBreaker, PageFrameRoundDispatcher dispatcher) {
        // A parallel hash join prepares every stage's build before it builds any.
        if (!isBuildPrepared) {
            prepareBuild();
        }
        if (isLargeBuild) {
            buildInRounds(circuitBreaker, dispatcher);
        } else {
            buildOnOwner(-1, circuitBreaker);
        }
        finishBuild();
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
     * Ends the open cursor's build, once the build ran, whichever way it ran: on the owner, in rounds
     * of its own, or as a task of a round of concurrent builds, whose end made the build's writes
     * visible to the owner; {@link #build} ends its own.
     */
    void finishBuild() {
        assert frozen != null;
        isBuildUnique = frozen.getRowCount() == frozen.getKeyCount();
    }

    /**
     * The build input's rows before any filter, once {@link #prepareBuild()} loaded its frames: the
     * rows that a task of a round of concurrent builds walks, by which the fiber dispatcher batches
     * the round's tasks.
     */
    long getBuildRowCount() {
        return buildFrames.getRowCount();
    }

    /**
     * The slot of a parallel hash join's consumer, which positions the slot's probe and payload
     * reader on the matches that the workers found; the stage's own tasks never run on it.
     */
    Slot getOutputSlot() {
        assert slots.size() > roundSlotCount : "the stage has no output slot";
        return slots.getQuick(roundSlotCount);
    }

    /**
     * The state of a slot: -1 for the owner, and 0 to the worker count less one for the workers.
     */
    Slot getSlot(int slotId) {
        return slots.getQuick(slotId + 1);
    }

    /**
     * Indexes the chain lengths of the open cursor's build for a count of a parallel hash join's
     * rows, unless a count of the same execution indexed them before; see
     * {@link FrozenHashJoinBuild#indexChainLengths(SqlExecutionCircuitBreaker)}. The owner calls it
     * after the build and before it dispatches the count's first task, which publishes the index to
     * the workers, with the breaker of its own execution, which the pass checks.
     */
    void indexChainLengths(SqlExecutionCircuitBreaker circuitBreaker) {
        if (!frozen.hasChainLengthIndex()) {
            frozen.indexChainLengths(circuitBreaker);
            chainLengthIndexCount++;
        }
    }

    /**
     * True when the stage can build alone on a worker's slot, in a round of concurrent builds: its
     * build scan has a memory pool for every worker, and a filter of its own for every worker, or a
     * thread safe one, as a build in rounds of its own needs.
     */
    boolean isConcurrentBuildCapable() {
        return isParallelBuildCapable;
    }

    /**
     * True when the open cursor's build runs in rounds of its own, as {@link #prepareBuild()}
     * decided.
     */
    boolean isLargeBuild() {
        return isLargeBuild;
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
        maybeCopyPayload(probeRows, false, circuitBreaker, dispatcher);
    }

    /**
     * Copies the build's payload columns for a parallel hash join, as
     * {@link #maybeCopyPayload(long, SqlExecutionCircuitBreaker, PageFrameRoundDispatcher)} copies
     * them for the fused GROUP BY, unless a filter that the join stole from its probe input keeps
     * the probe's rows: the probe's count is its frame rows, which such a filter thins, often to a
     * few, whose matches then read too few build rows to repay the copy. Such a probe copies only a
     * build whose Parquet frames with kept rows decode past a probe reader's share of the decode
     * cache, see {@link HashJoinBuildFrames#isReaderCacheExceeded(long)}, which its matches could land
     * on as often as the copy decodes them, since the readers would otherwise decode them again and
     * again; and only within the copy's byte bound.
     *
     * @param isProbeFiltered true when the execution's probe input runs a filter that the join stole
     */
    void maybeCopyPayload(long probeRows, boolean isProbeFiltered, SqlExecutionCircuitBreaker circuitBreaker, PageFrameRoundDispatcher dispatcher) {
        assert frozen != null && !isPayloadCopied;
        final double minProbeRatio = isBuiltInRounds ? copyParallelMinProbeRatio : copyMinProbeRatio;
        final long buildRows = frozen.getRowCount();
        final boolean isParquetReached = isParquetInProbeReach(probeRows, buildRows, frozen.getKeyCount(), parquetFrameCount);
        final boolean isParquetOverReaderCache = isParquetReached
                && parquetBuildRows > 0
                && buildFrames.isReaderCacheExceeded(parquetFrameRows);
        if ((!isProbeFiltered || isParquetOverReaderCache)
                && isPayloadCopyWorthIt(buildRows, parquetBuildRows, isParquetReached, isParquetOverReaderCache, buildFrames.getCopyRowSize(), probeRows, copyMaxSize, minProbeRatio)) {
            if (isBuiltInRounds) {
                // A parallel build keeps each partition's rows in one region of the heap, so a pass in
                // heap order would walk the frames once per partition, or it keeps each frame's rows
                // in bucket order. Each frame copies the rows it kept instead, on the workers.
                buildFrames.beginCopy(frozen, roundSlotCount, circuitBreaker);
                roundTaskRowCounts.clear();
                for (int frameIndex = 0, n = buildFrames.getFrameCount(); frameIndex < n; frameIndex++) {
                    long rows = 0;
                    for (int partition = 0, partitionCount = getPartitionCount(); partition < partitionCount; partition++) {
                        rows += getSegmentRowCount(frameIndex, partition);
                    }
                    roundTaskRowCounts.add(rows);
                }
                roundCount++;
                dispatcher.dispatchRoundAndAwait(COPY_FRAME_PAYLOAD, roundTaskRowCounts);
                buildFrames.publishCopy();
            } else {
                buildFrames.copyPayload(frozen, circuitBreaker);
            }
            isPayloadCopied = true;
        }
    }

    /**
     * Loads the build scan's page frames and decides how the open cursor's build runs, on the owner
     * and before any of its rows is read: in rounds of its own, on the workers, when it can and its
     * input holds at least the configured rows, or spans more than one frame under a WHERE filter
     * that the JIT did not compile, such as a regex or a LIKE; and alone otherwise, on the owner,
     * or, for a parallel hash join with several small stages, as a task of a round that builds them
     * at once; see {@link #BUILD_STAGE}. Such a filter's cost per row has no bound, so the input's
     * rows do not measure the build's work, and the plan without the operator ran the filter on the
     * workers; a filter that the JIT compiled, a build ON filter alone and a build of one frame,
     * whose one task would run on one thread anyway, leave the decision to the rows, by which a
     * small build saves the rounds' fixed cost. A build alone takes its presize hints here, on the
     * owner, since the key hint of a SYMBOL key reads the build scan's dictionary. The caller then builds, see
     * {@link #build}, which also prepares a build that nothing prepared, and {@link #BUILD_STAGE},
     * after which it runs {@link #finishBuild()}. On failure the caller runs {@link #clear()}, which
     * closes what this opened.
     *
     * @return true when the build runs in rounds of its own
     */
    boolean prepareBuild() {
        assert !isBuildPrepared && frozen == null && buildFrames.getSymbolTableSource() != null && parquetBuildRows == 0
                && parquetFrameCount == 0 && parquetFrameRows == 0;
        isBuildPrepared = true;
        buildCount++;
        buildFrames.loadFrames();
        buildFilterContext.initMemoryPools(buildFrames.getAddressCache(), memoryTracker);
        isBuildFiltered = buildFilterContext.getFilter(-1) != null || isBuildOnFilterActive;
        final int frameCount = buildFrames.getFrameCount();
        isLargeBuild = isParallelBuildCapable && frameCount > 0
                && (buildFrames.getRowCount() >= parallelBuildMinRows || (frameCount > 1 && isBuildFilterInterpreted()));
        if (!isLargeBuild) {
            rowCountHint = isBuildFiltered ? -1 : buildFrames.getRowCount();
            keyCountHint = getKeyCountHint(rowCountHint);
        }
        return isLargeBuild;
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
        // The staged probe key sink of the second key source's layout, which the slot's swap record
        // probes borrow for life; null for the INT layout, for a stage with one key source and for
        // the output slot, which looks no key up.
        @Nullable
        final RecordSink swapProbeKeySink;
        // The INT layout's lone SYMBOL key translates through this view; null otherwise.
        @Nullable
        final SymbolKeyTranslator.View symbolKeyView;
        // A staged key with SYMBOL columns translates through this record's views; null otherwise.
        @Nullable
        private final SymbolKeyTranslatingRecord probeKeyRecord;
        // The same over the second key source, as its layout names the SYMBOL key columns; null when
        // the slot has no swap sink or the key no SYMBOL column.
        @Nullable
        private final SymbolKeyTranslatingRecord swapProbeKeyRecord;
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
        // What the staged key sink of the second key source reads: that source, or the translating
        // view of it; see ofSwapKeySource().
        Record swapKeyRecord;
        // The record probes of the second key source's layout, partitioned and not, which borrow the
        // swap sink; null until an execution reads its keys from that source.
        FrozenHashJoinBuild.RecordProbe swapPartitionedRecordProbe;
        FrozenHashJoinBuild.RecordProbe swapRecordProbe;
        // Whichever of the six probes bindProbes() bound for the open cursor's build; null otherwise.
        private FrozenHashJoinBuild.Probe probe;

        private Slot(
                @Nullable RecordSink probeKeySink,
                @Nullable RecordSink buildKeySink,
                @Nullable MapHashJoinBuild.KeyStager buildKeyStager,
                @Nullable SymbolKeyTranslatingRecord probeKeyRecord,
                @Nullable SymbolKeyTranslator.View symbolKeyView,
                @Nullable RecordSink swapProbeKeySink,
                @Nullable SymbolKeyTranslatingRecord swapProbeKeyRecord
        ) {
            this.probeKeySink = probeKeySink;
            this.buildKeySink = buildKeySink;
            this.buildKeyStager = buildKeyStager;
            this.probeKeyRecord = probeKeyRecord;
            this.symbolKeyView = symbolKeyView;
            this.swapProbeKeySink = swapProbeKeySink;
            this.swapProbeKeyRecord = swapProbeKeyRecord;
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
            if (swapProbeKeyRecord != null) {
                swapProbeKeyRecord.close();
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
            if (swapRecordProbe != null) {
                swapRecordProbe.close();
            }
            if (swapPartitionedRecordProbe != null) {
                swapPartitionedRecordProbe.close();
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
            failure = Misc.freeBestEffort(failure, swapRecordProbe);
            swapRecordProbe = null;
            failure = Misc.freeBestEffort(failure, swapPartitionedRecordProbe);
            swapPartitionedRecordProbe = null;
            probe = null;
            failure = Misc.freeBestEffort(failure, buildKeyStager);
            failure = Misc.freeBestEffort(failure, probeKeyRecord);
            failure = Misc.freeBestEffort(failure, swapProbeKeyRecord);
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
         * Binds the second key source, as {@link #ofKeySource(Record)} binds the first, which
         * {@link #swapKeyRecord} then reads for the staged key sink of the second layout. The INT
         * layout reads its key straight off the source, at the column that its stepper picks.
         */
        void ofSwapKeySource(Record keySource) {
            if (swapProbeKeyRecord != null) {
                swapProbeKeyRecord.of(keySource);
                swapKeyRecord = swapProbeKeyRecord;
            } else {
                swapKeyRecord = keySource;
            }
        }

        /**
         * The translation path of one SYMBOL key column, wherever this slot keeps it for the key
         * source that the execution reads: the translating record of the second source when the
         * slot has one, the first's otherwise. The views of both translate alike, by key ordinal.
         */
        private SymbolKeyTranslator.View getSymbolKeyView(int key, boolean isKeySourceSwapped) {
            if (isKeySourceSwapped && swapProbeKeyRecord != null) {
                return swapProbeKeyRecord.getView(key);
            }
            return probeKeyRecord != null ? probeKeyRecord.getView(key) : symbolKeyView;
        }
    }
}
