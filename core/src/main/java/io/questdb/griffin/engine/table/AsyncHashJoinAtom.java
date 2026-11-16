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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * The execution state of a parallel hash join's probe: every stage's build, the slots that join
 * probe frames, one per worker and the owner's, and the ring of joined tuples that the frames'
 * tasks write and the consumer reads; see {@link HashJoinTupleRing}. The atom owns the stages and
 * borrows the inputs, their filter contexts and the stages' steppers from the join's spec.
 * <p>
 * {@link #init(SymbolTableSource, SqlExecutionContext)} runs when the cursor opens and reads no row:
 * the first stage adopts the build scan that the factory opened, see {@link #ofOrientation}, every
 * later stage opens its own, each stage initializes its filters, and the slots bind to the probe
 * scan. {@link #build} runs once per execution, when the cursor is first asked for a row: every
 * stage builds, a large one in rounds of the join's build sequence, one at a time, and the small
 * ones on the owner, or at once, in one round of that sequence with one task per stage; then each
 * binds its SYMBOL key translation and hands every slot its probe of the build. The owner dispatches
 * the probe tasks after the build returns, and the first task that the frame sequence publishes is
 * what publishes the builds and the slots' probes to the workers.
 * <p>
 * The first join of a swap candidate, see {@link AsyncHashJoinSpec#isSwapCandidate()}, has two runtime
 * stages: the one that hashes its right input, and the one that hashes its left input, which an
 * execution that swaps the inputs builds instead, while the right input's frames go to the probe
 * tasks. Each execution picks one orientation, and the stage, the probe scan, the steppers, the
 * joined records and the joined rows' symbol tables follow it until the next execution;
 * {@link #getStage(int)} returns the execution's stages. The later stages of such a stack read their
 * keys off the left input's columns, which a swapped execution holds in the first stage's build: each
 * slot then reads them from its own payload reader of the first stage, which the first stage's
 * stepper positions on every one of its matches before it hands the match on, so that the workers
 * walk the first stage's matches whenever later stages follow it. The later stages translate their
 * SYMBOL keys from the left input's dictionaries and presize their key tables by the left input's
 * table in either orientation: the probe scan holds them in the query's, the first stage's build scan
 * in a swapped one.
 * <p>
 * A probe task joins one frame, see {@link #probeFrame}: it keeps the frame's rows that the probe
 * filter passes, if the probe input has one, and for each kept row, in the probe scan's order, the
 * first stage's stepper looks the row's key up and hands each match on to the next stage's stepper,
 * and so on, every later stage's key read off the probe row too, or, in a swapped execution, off the
 * first stage's current match, which holds the left input's row, and the last stage appends the row
 * and every stage's entry to the frame's ring entry, which the consumer reads in frame order. A stage
 * whose pairs run an ON residual or a post-join filter runs them over each pair on the slot's flat
 * joined record, with the slot's copies of the filters, and the workers walk the matches of every
 * stage up to the last such stage, whose filters may read them; the stages after it hand their chains
 * to the consumer. The leading conjuncts of a LEFT join's ON residual that read the build alone may
 * filter the build's rows instead, see {@link HashJoinStageSpec.BuildOnFilter}: an execution whose
 * build ran them runs the rest of the residual over the pairs, none when they make up the whole
 * residual, and any other one the whole residual, so each execution decides, once its builds are
 * done, which stages walk their matches; see {@link #build}. The task stops at a probe row's
 * boundary once the frame's tuples reach the task's cap,
 * {@code max(cairo.sql.parallel.hash.join.task.output.max.rows, the frame's rows)}, and records in the
 * ring where the frame's continuation resumes: the consumer, once it has read those tuples, joins the
 * rest of the frame in chunks of the same cap on the owner's slot, see {@link #continueFrame}. One
 * probe row's pairs always finish in one chunk, so a key with more matches than the cap overruns it
 * by its own matches, in memory that the execution's tracker counts. To keep that rare, the builds
 * tell the cursor how many rows of a probe frame fit the cap at the fan-out they lead one to expect,
 * down to 1,000 rows a frame, see {@link #getProbeFrameRowCap()}, before the probe scan cuts its
 * frames. Under a LIMIT, the task of the last frame of the first window joins only the rows that the
 * LIMIT may need, and leaves the rest to the consumer's continuation, whose chunks then double; see
 * {@link #setRowCappedFrame}.
 * <p>
 * A count of the join's rows dispatches count-only tasks, which walk the stages as a probe task
 * does, but count the frame's joined rows instead of writing tuples, and leave the ring alone; see
 * {@link #probeFrame}. A frame whose tuples a count meets before it read them all also counts the
 * rest of the frame, which its task left to a continuation, on the owner's slot, see
 * {@link #countFrameRest}. Before its first task, the owner indexes the chain lengths of the builds
 * whose stages hand their chains on, see {@link #indexChainLengths}, so that a count adds the rows
 * of a key's chain with one read, however long the chain.
 * <p>
 * A Parquet probe frame is decoded late, as {@code Async Filter} decodes its frames: the task first
 * decodes the columns that it reads itself, those of the probe filter, the stages' keys and their
 * filters, see {@link AsyncHashJoinSpec#newProbeTaskColumns(boolean)}, and once it has joined the
 * frame, the other columns only for the rows that the consumer reads, the probe rows of its tuples
 * and the rows that a continuation may join, if any; a frame whose rows all fail the probe filter or
 * miss an INNER join decodes no more. A count-only task's frame never decodes the rest, which no one
 * reads. As for {@code Async Filter}, each slot keeps the share of its frames' rows that the consumer
 * read, and once that share stops being low, the slot decodes its frames whole, in one pass.
 * <p>
 * Each stage probes through a hidden-class copy of the stepper template of its own, see
 * {@link HashJoinStageStepper}, whose code starts cold in every new factory, while the shared
 * template's instance that every stage also keeps runs code that the JVM has long compiled. A
 * factory's tasks and continuations therefore run the shared template until the consumer has
 * collected {@value #STEPPER_WARM_UP_ROWS} probe rows over the factory's executions, which repay
 * the copies' warm-up, and the copies from then on: each task, count-only task and continuation
 * picks one by the rows collected when it starts, see {@link #addProbedRows(long)}. Both write the
 * same tuples and counts, so an execution may switch from one to the other between two of them.
 * <p>
 * {@link #clear()} requires every task to have finished and the consumer to be done with the tuples
 * and the payload readers.
 */
public final class AsyncHashJoinAtom implements StatefulAtom, PerWorkerLockOwner {
    /**
     * What {@link #getStepperRunCount(int, boolean)} counts: probe tasks that write tuples,
     * count-only tasks, continuation chunks and the counts of the rest of a frame that a count meets.
     */
    public static final int STEPPER_RUN_CONTINUATION = 2;
    public static final int STEPPER_RUN_COUNTED_REST = 3;
    public static final int STEPPER_RUN_COUNT_TASK = 1;
    public static final int STEPPER_RUN_TASK = 0;
    /**
     * The probe rows that a factory's executions collect before its tasks and continuations switch
     * from the shared stepper template to the stages' hidden copies; see the class documentation.
     * A fresh factory's copies ran about 100 ms behind the template in a benchmark, still after
     * 10,000,000 probe rows, and gain about 2 ns per probe row once compiled.
     */
    public static final long STEPPER_WARM_UP_ROWS = 50_000_000;
    // The fewest rows that the fan-out of the builds lowers a probe frame's rows to, or the context's
    // cairo.sql.page.frame.min.rows when that is fewer; see getProbeFrameRowCap(). A frame costs the
    // owner a few microseconds of dispatch, collection and ring work, so frames of this many rows cost
    // a few nanoseconds per probe row, also when the builds lead the join to expect far more matches
    // per probe row than the rows meet.
    private static final long FAN_OUT_FRAME_MIN_ROWS = 1_000;
    // Set by tests alone; see AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting().
    private static volatile long stepperWarmUpRowsForTesting = STEPPER_WARM_UP_ROWS;
    // The runtime stages in the execution's orientation: those of the query's, the first one swapped
    // for the stage that hashes the left input when the execution swaps the inputs.
    private final ObjList<HashJoinBuildStage> executionStages = new ObjList<>();
    // The joined rows' symbol tables, which the stages' filters initialize against.
    private final JoinedSymbols joinedSymbols;
    // The input the join streams as the query is written, which a swapped execution hashes instead.
    private final HashJoinInput leftInput;
    private final PerWorkerLocks perWorkerLocks;
    // The first stage's build input, which a swapped execution streams instead.
    private final HashJoinInput rightInput;
    private final HashJoinTupleRing ring;
    // For each record of the joined row in the query's orientation, see
    // AsyncHashJoinSpec.getColumnSource(), the open scan whose frames hold its columns in the
    // execution's orientation: 0 for the probe scan, k + 1 for the execution's stage k's build scan.
    // A swapped execution trades the first two.
    private final int[] scanBySource;
    private final ObjList<Slot> slots = new ObjList<>();
    // The probe scan's frame columns that a probe task reads in the query's orientation, which a
    // Parquet frame decodes first; null when a task decodes whole frames. See
    // AsyncHashJoinSpec.newProbeTaskColumns().
    @Nullable
    private final IntHashSet probeTaskColumnsA;
    // The same in a swapped execution; null when a task decodes whole frames or the join never swaps.
    @Nullable
    private final IntHashSet probeTaskColumnsB;
    // The stages as compiled, which the atom borrows for their join types and steppers.
    private final ObjList<HashJoinStageSpec> specs = new ObjList<>();
    // The runtime stages in the query's orientation: the first one hashes the right input.
    private final ObjList<HashJoinBuildStage> stages = new ObjList<>();
    // The first stage of a swapped execution, which hashes the left input; null when the join never
    // swaps.
    @Nullable
    private final HashJoinBuildStage swapStage;
    // The probe rows the factory's executions collect before its tasks switch to the stages' hidden
    // stepper copies; see STEPPER_WARM_UP_ROWS.
    private final long stepperWarmUpRows;
    // The tuples a task writes before it leaves the rest of its frame to a continuation, unless the
    // frame has more rows; see cairo.sql.parallel.hash.join.task.output.max.rows.
    private final long taskOutputMaxRows;
    private final int workerCount;
    // The chunks that continued frames past their tasks' caps in the open cursor's execution.
    private int continuationCount;
    // True from the moment init() initializes the stages' filters until clear() releases them.
    private boolean isJoinFilterInitialized;
    // True when the execution's probe scan runs backward.
    private boolean isProbeDescending;
    // True from the moment init() initializes the probe filter until clear() releases it.
    private boolean isProbeFilterInitialized;
    // True when the execution's probe input has a filter, whose kept rows the tasks join.
    private boolean isProbeFiltered;
    // True when the execution hashes the left input and streams the right one.
    private boolean isSwapped;
    // The context's cairo.sql.page.frame.min.rows in the open cursor's execution, below which the
    // fan-out of the builds lowers no probe frame's rows when it is fewer than
    // FAN_OUT_FRAME_MIN_ROWS; see getProbeFrameRowCap().
    private long pageFrameMinRows;
    // The pairs an enumerated stage walks between two checks of the circuit breaker.
    private long pairsPerCheck = 1;
    // The first stage's build scan, which the factory opened before the probe scan, from the moment
    // the execution picks its orientation until the stage adopts it in init(); null otherwise.
    private PageFrameCursor pendingBuildCursor;
    // The scans of the first join's inputs, which the factory opened, the right one first, from the
    // moment it opens them until it picks the execution's orientation; null otherwise.
    private PageFrameCursor pendingLeftCursor;
    private PageFrameCursor pendingRightCursor;
    // The columns of the execution's probe scan.
    private int probeColumnCount;
    // The probe input's stolen filter, with its worker copies and JIT code, which the atom borrows;
    // a context without a filter when the probe input has none. The left input's in the query's
    // orientation, the right input's in a swapped execution. A probe task also lists the rows of its
    // Parquet frame that the consumer reads in its slot's row list, and keeps the share of them in
    // its slot's selectivity statistics, since the join reads its frames through the tasks' memory
    // rather than the context's pools and lists.
    private AsyncFilterContext probeFilterContext;
    // The execution's probe task columns: probeTaskColumnsA or, swapped, probeTaskColumnsB.
    @Nullable
    private IntHashSet probeTaskColumns;
    // The frame whose task joins no more than rowCappedFrameRows of its rows, the last frame of the
    // first window under a LIMIT, -1 for none; see setRowCappedFrame().
    private int rowCappedFrame = -1;
    private long rowCappedFrameRows;
    // True once the factory's executions collected the warm-up's probe rows, from which point the
    // tasks and continuations that start run the stages' hidden stepper copies; the owner sets it,
    // and every task reads it.
    private volatile boolean isWarmedUp;
    // The probe rows of the frames that the consumer collected over the factory's executions.
    private long probedRowCount;
    // The first stage's stepper in the execution's orientation: its hidden copy, and the shared
    // template's instance, which the tasks run until the factory is warmed up.
    private HashJoinStageStepper stepper;
    private HashJoinStageStepper templateStepper;

    /**
     * Borrows the spec's inputs, filter contexts and steppers. The key layouts borrow the inputs'
     * metadata, so construct the atom while the inputs are open. Nothing here allocates native
     * memory, which the stages, the slots and the ring take only when an execution opens them, so a
     * failure leaves nothing to release.
     */
    AsyncHashJoinAtom(CairoConfiguration configuration, AsyncHashJoinSpec spec, int workerCount) {
        this.workerCount = workerCount;
        this.taskOutputMaxRows = configuration.getSqlParallelHashJoinTaskOutputMaxRows();
        this.stepperWarmUpRows = stepperWarmUpRowsForTesting;
        this.isWarmedUp = stepperWarmUpRows <= 0;
        this.joinedSymbols = new JoinedSymbols(spec, this);
        final int stageCount = spec.getStageCount();
        this.leftInput = spec.getLeftInput();
        this.rightInput = spec.getStage(0).getBuildInput();
        this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        this.ring = new HashJoinTupleRing(configuration, stageCount);
        this.scanBySource = new int[stageCount + 1];
        final int probeColumnCount = leftInput.getMetadata().getColumnCount();
        // A swapped execution's later stages read the left input's columns from the first stage's
        // payload, which holds them in the order the left input as compiled lists them.
        final int leftPayloadColumnCount = leftInput.getColumnMap().size();
        for (int k = 0; k < stageCount; k++) {
            final HashJoinStageSpec stageSpec = spec.getStage(k);
            final HashJoinInput buildInput = stageSpec.getBuildInput();
            final HashJoinKeyLayout keyLayout = stageSpec.getKeyLayout();
            final HashJoinBuildStage stage = new HashJoinBuildStage(
                    configuration,
                    buildInput.getFactory(),
                    buildInput.getFilterContext(),
                    stageSpec.getBuildOnFilter(),
                    stageSpec.getWorkerBuildOnFilters(),
                    stageSpec.isBuildOnFilterSized(),
                    keyLayout,
                    stageSpec.getPayloadColumns(),
                    probeColumnCount,
                    stageSpec.getSwapKeySourceLayout(),
                    leftPayloadColumnCount,
                    buildInput.getOrder(),
                    workerCount
            );
            stages.add(stage);
            executionStages.add(stage);
            specs.add(stageSpec);
            scanBySource[k + 1] = k + 1;
            stageSpec.setPayloadCopyable(stage.isPayloadCopyable());
            // Each stage's matches go on to the next stage's stepper, the hidden copies' to the next
            // copy and the shared template's to the next stage's instance of it. Which steppers walk
            // their matches, each execution decides; see ofEnumeration().
            final HashJoinStageSpec nextSpec = k + 1 < stageCount ? spec.getStage(k + 1) : null;
            ofStepper(stageSpec.getStepper(), keyLayout, stageSpec.isOuter(), nextSpec != null ? nextSpec.getStepper() : null);
            ofStepper(stageSpec.getTemplateStepper(), keyLayout, stageSpec.isOuter(), nextSpec != null ? nextSpec.getTemplateStepper() : null);
        }
        final HashJoinStageSpec firstSpec = specs.getQuick(0);
        final HashJoinKeyLayout swapKeyLayout = firstSpec.getSwapKeyLayout();
        if (swapKeyLayout != null) {
            // Swapped, the first stage hashes every column of the left input, in the left input's
            // scan order, and its keys read the right input's frames, which the probe tasks join.
            swapStage = new HashJoinBuildStage(
                    configuration,
                    leftInput.getFactory(),
                    leftInput.getFilterContext(),
                    swapKeyLayout,
                    leftInput.getColumnMap(),
                    rightInput.getMetadata().getColumnCount(),
                    leftInput.getOrder(),
                    workerCount
            );
            final HashJoinStageStepper swapStepper = firstSpec.getSwapStepper();
            final HashJoinStageStepper swapTemplateStepper = firstSpec.getSwapTemplateStepper();
            assert swapStepper != null && swapTemplateStepper != null && !firstSpec.isOuter();
            final HashJoinStageSpec secondSpec = stageCount > 1 ? spec.getStage(1) : null;
            ofStepper(swapStepper, swapKeyLayout, false, secondSpec != null ? secondSpec.getStepper() : null);
            ofStepper(swapTemplateStepper, swapKeyLayout, false, secondSpec != null ? secondSpec.getTemplateStepper() : null);
        } else {
            swapStage = null;
        }
        probeTaskColumnsA = spec.newProbeTaskColumns(false);
        probeTaskColumnsB = swapStage != null ? spec.newProbeTaskColumns(true) : null;
        for (int i = -1; i < workerCount; i++) {
            final Slot slot = new Slot(stageCount, spec.newJoinedRecord());
            for (int k = 0; k < stageCount; k++) {
                // The slot's staged probe keys read its probe record.
                stages.getQuick(k).getSlot(i).ofKeySource(slot.probeRecord);
                final HashJoinStageSpec stageSpec = specs.getQuick(k);
                slot.getStageProbe(k).ofFilters(stageSpec.getOnFilter(i), stageSpec.getPostFilter(i));
            }
            if (swapStage != null) {
                // Swapped, the probe record reads the right input's frames, which the keys read.
                swapStage.getSlot(i).ofKeySource(slot.probeRecord);
            }
            slots.add(slot);
        }
        ofOrientation0(false);
    }

    /**
     * True when the execution of the frame sequence was cancelled: by the circuit breaker, which
     * cancels the sequence here unless the execution is uninterruptible, or before, by another task,
     * the consumer or a failure. The reduce jobs check the breaker before each frame; a stage that
     * walks its matches checks it every so many pairs too, since a frame's pairs are not bounded by
     * its rows, and the consumer checks it before each chunk of a frame's continuation.
     */
    static boolean isInterrupted(SqlExecutionCircuitBreaker circuitBreaker, PageFrameSequence<?> frameSequence) {
        if (!frameSequence.isUninterruptible()) {
            final int state = circuitBreaker.getState(frameSequence.getStartTime(), frameSequence.getCircuitBreaker().getFd());
            if (state != SqlExecutionCircuitBreaker.STATE_OK) {
                frameSequence.cancel(state);
            }
        }
        return !frameSequence.isActive();
    }

    /**
     * Sets the probe rows after which the factories that compile from here on switch from the shared
     * stepper template to their stages' hidden copies, {@link #STEPPER_WARM_UP_ROWS} unless a test
     * lowers it; 0 runs the copies from the first task.
     */
    @TestOnly
    static void setStepperWarmUpRows(long rows) {
        stepperWarmUpRowsForTesting = rows;
    }

    @Override
    public void clear() {
        Throwable failure = Misc.freeBestEffort(null, pendingBuildCursor);
        pendingBuildCursor = null;
        failure = Misc.freeBestEffort(failure, pendingLeftCursor);
        pendingLeftCursor = null;
        failure = Misc.freeBestEffort(failure, pendingRightCursor);
        pendingRightCursor = null;
        if (isJoinFilterInitialized) {
            isJoinFilterInitialized = false;
            for (int k = 0, n = specs.size(); k < n; k++) {
                final HashJoinStageSpec stageSpec = specs.getQuick(k);
                failure = cursorClosed(failure, stageSpec.getOnFilter(-1), stageSpec.getWorkerOnFilters());
                failure = cursorClosed(failure, stageSpec.getRemainingOnFilter(-1), stageSpec.getWorkerRemainingOnFilters());
                failure = cursorClosed(failure, stageSpec.getPostFilter(-1), stageSpec.getWorkerPostFilters());
            }
        }
        if (isProbeFilterInitialized) {
            isProbeFilterInitialized = false;
            final Function ownerFilter = probeFilterContext.getFilter(-1);
            failure = HashJoinBuildStage.cursorClosed(failure, ownerFilter);
            for (int i = 0; i < workerCount; i++) {
                final Function workerFilter = probeFilterContext.getFilter(i);
                if (workerFilter != ownerFilter) {
                    failure = HashJoinBuildStage.cursorClosed(failure, workerFilter);
                }
            }
        }
        // The filter context's lists shrink back to their initial capacity for the next execution.
        failure = Misc.clearBestEffort(failure, probeFilterContext);
        failure = Misc.clearBestEffort(failure, ring);
        rowCappedFrame = -1;
        // The slots let go of the probes and the probe scan's symbol tables before the stages close
        // the builds, and the frame sequence closes the probe scan after the atom.
        failure = Misc.clearObjListBestEffort(failure, slots);
        failure = Misc.clearObjListBestEffort(failure, stages);
        failure = Misc.clearBestEffort(failure, swapStage);
        joinedSymbols.probeSymbols = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Throwable failure = Misc.clearBestEffort(null, this);
        failure = Misc.freeObjListBestEffort(failure, slots);
        slots.clear();
        failure = Misc.freeObjListBestEffort(failure, stages);
        stages.clear();
        failure = Misc.freeBestEffort(failure, swapStage);
        failure = Misc.freeBestEffort(failure, ring);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * The chunks that the consumer joined to continue frames whose tasks stopped at their cap of
     * tuples, in the last execution since its cursor opened.
     */
    @TestOnly
    public int getContinuationCount() {
        return continuationCount;
    }

    /**
     * The probe columns that the last execution's tasks decoded since its cursor opened, summed over
     * the Parquet probe frames they read: for a frame decoded late, the columns the task reads, see
     * {@link #getProbeTaskColumns()}, and, if the consumer reads any of the frame's rows, the probe
     * scan's other columns; for a frame decoded whole, every column of the probe scan.
     */
    @TestOnly
    public long getDecodedProbeColumnCount() {
        long count = 0;
        for (int i = 0, n = slots.size(); i < n; i++) {
            count += slots.getQuick(i).decodedColumnCount;
        }
        return count;
    }

    /**
     * The probe rows, or the rows the probe filter kept, that the last execution's tasks and
     * continuation chunks joined into tuples since its cursor opened; a count joins none.
     */
    @TestOnly
    public long getJoinedProbeRowCount() {
        long count = 0;
        for (int i = 0, n = slots.size(); i < n; i++) {
            count += slots.getQuick(i).joinedRowCount;
        }
        return count;
    }

    /**
     * The Parquet probe frames that the last execution's tasks read since its cursor opened.
     */
    @TestOnly
    public long getParquetProbeFrameCount() {
        long count = 0;
        for (int i = 0, n = slots.size(); i < n; i++) {
            count += slots.getQuick(i).parquetFrameCount;
        }
        return count;
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    /**
     * The probe scan's frame columns that the open cursor's tasks read themselves, and decode first in
     * a Parquet frame; null when they decode whole frames. See
     * {@link AsyncHashJoinSpec#newProbeTaskColumns(boolean)}.
     */
    @Nullable
    @TestOnly
    public IntHashSet getProbeTaskColumns() {
        return probeTaskColumns;
    }

    /**
     * The probe rows of the frames that the consumer collected over the factory's executions, which
     * the stepper warm-up counts; see {@link #STEPPER_WARM_UP_ROWS}.
     */
    @TestOnly
    public long getProbedRowCount() {
        return probedRowCount;
    }

    /**
     * The ring of the joined tuples that the probe tasks write, one entry per frame in flight.
     */
    public HashJoinTupleRing getRing() {
        return ring;
    }

    /**
     * The runtime state of the k-th join in the execution's orientation: its build, its payload
     * readers and its slots' probes. A swapped execution's first stage hashes the left input.
     */
    public HashJoinBuildStage getStage(int stage) {
        return executionStages.getQuick(stage);
    }

    /**
     * The frames and chunks of the last execution since its cursor opened that ran the shared
     * stepper template, or, {@code isWarm}, the stages' hidden copies, of one kind:
     * {@link #STEPPER_RUN_TASK} for the probe tasks that wrote tuples, {@link #STEPPER_RUN_COUNT_TASK}
     * for the count-only tasks, {@link #STEPPER_RUN_CONTINUATION} for the continuation chunks and
     * {@link #STEPPER_RUN_COUNTED_REST} for the rest of a frame that a count met, which it counted on
     * the owner's slot.
     */
    @TestOnly
    public long getStepperRunCount(int kind, boolean isWarm) {
        long count = 0;
        for (int i = 0, n = slots.size(); i < n; i++) {
            count += slots.getQuick(i).stepperRunCounts[kind * 2 + (isWarm ? 1 : 0)];
        }
        return count;
    }

    /**
     * Adopts the first stage's build scan, binds every slot to the probe scan and initializes the
     * probe filter, and reads no row of either scan: {@link #build} does the work that grows with the
     * builds, when the cursor is first asked for a row. Everything that can throw a SqlException runs
     * here, so that it surfaces from getCursor(). On failure the factory closes the cursor, which
     * resets the frame sequence, whose reset runs {@link #clear()}: it releases what this took, the
     * build scan included.
     *
     * @param symbolTableSource the open probe scan
     */
    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        final PageFrameCursor buildCursor = pendingBuildCursor;
        pendingBuildCursor = null;
        // The stage owns the build scan from here on, whether its init returns or throws.
        getStage(0).init(symbolTableSource, executionContext, buildCursor);
        // A later stage's keys read the left input's columns, which the probe scan holds, or, in a
        // swapped execution, the first stage's build scan, which is then the left input's scan. The
        // stage translates its SYMBOL keys from that scan's dictionaries and presizes its key table by
        // its table, and it opens its own build scan, in its input's scan order.
        final SymbolTableSource leftSymbols = isSwapped ? getStage(0).getBuildSymbols() : symbolTableSource;
        for (int k = 1, n = executionStages.size(); k < n; k++) {
            final HashJoinInput buildInput = specs.getQuick(k).getBuildInput();
            final PageFrameCursor laterBuildCursor = buildInput.getFactory().getPageFrameCursor(executionContext, buildInput.getOrder());
            // The stage owns the build scan from here on, whether its init returns or throws.
            executionStages.getQuick(k).init(leftSymbols, executionContext, laterBuildCursor);
        }
        for (int i = 0, n = slots.size(); i < n; i++) {
            final Slot slot = slots.getQuick(i);
            slot.probeRecord.of(symbolTableSource);
            // The stages bind their payload readers to the record when they build.
            slot.record.setOrientation(isSwapped);
            slot.decodedColumnCount = 0;
            slot.joinedRowCount = 0;
            slot.parquetFrameCount = 0;
            Arrays.fill(slot.stepperRunCounts, 0);
        }
        // The workers' copies of the probe filter take symbol tables of their own, and the bind
        // variables of a compiled filter go to its bind variable memory, for this execution's values.
        isProbeFilterInitialized = true;
        probeFilterContext.initFilters(symbolTableSource, executionContext);
        // The stages' filters read the joined row, whose columns resolve their symbol tables through
        // the open probe scan and the stages' open build scans; each worker's copies take tables of
        // their own. A stage walks this many of its pairs between two checks of the breaker, as the
        // fused GROUP BY's reducers do.
        pairsPerCheck = Math.max(1, executionContext.getPageFrameMaxRows());
        pageFrameMinRows = Math.max(1, executionContext.getPageFrameMinRows());
        continuationCount = 0;
        joinedSymbols.probeSymbols = symbolTableSource;
        isJoinFilterInitialized = true;
        for (int k = 0, n = specs.size(); k < n; k++) {
            final HashJoinStageSpec stageSpec = specs.getQuick(k);
            initFilter(stageSpec.getOnFilter(-1), stageSpec.getWorkerOnFilters(), executionContext);
            initFilter(stageSpec.getRemainingOnFilter(-1), stageSpec.getWorkerRemainingOnFilters(), executionContext);
            initFilter(stageSpec.getPostFilter(-1), stageSpec.getWorkerPostFilters(), executionContext);
        }
    }

    /**
     * True when the open cursor's execution hashes the left input and streams the right one.
     */
    public boolean isSwapped() {
        return isSwapped;
    }

    /**
     * Takes the slot that a probe task runs on: -1 for the owner thread running a task of its own
     * sequence, which runs as worker -1, and a worker's slot otherwise.
     */
    public int maybeAcquire(int workerId, boolean isOwner, SqlExecutionCircuitBreaker breaker) {
        assert !isOwner || workerId == -1;
        return isOwner ? -1 : perWorkerLocks.acquireSlot(workerId, breaker);
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    /**
     * Counts the rows of a probe frame that the consumer collected toward the factory's stepper
     * warm-up: once the factory's executions collected {@link #STEPPER_WARM_UP_ROWS} probe rows, the
     * tasks and continuation chunks that start from then on run the stages' hidden stepper copies
     * rather than the shared template. The owner calls it for every frame it collects, whatever the
     * frame's task did with it, in every execution, so that a cached factory goes on from where its
     * last execution left off.
     */
    void addProbedRows(long rows) {
        probedRowCount += rows;
        if (!isWarmedUp && probedRowCount >= stepperWarmUpRows) {
            isWarmedUp = true;
        }
    }

    /**
     * Builds every stage and binds every slot to the builds. The cursor calls it once per execution,
     * on first demand for a row, on the thread that then dispatches the probes and before it
     * dispatches any; nothing here runs when the cursor merely opens. Returns false when the build of
     * any INNER join has no row, so that the join returns no row and needs no probe task. Every stage
     * builds even then, as every serial join of a stack builds its map before it reads its master, so
     * that a build that fails fails in both plans. On failure the caller closes the cursor, whose
     * clear() closes the builds.
     * <p>
     * Every stage first loads its build's frames, which tells a large build, one that runs in rounds
     * of its own, see {@link HashJoinBuildStage#prepareBuild()}, from a small one: a large build
     * holds at least the configured rows, or filters more than one frame through a WHERE filter that
     * runs interpreted, which its frame tasks then run on the workers rather than one task per stage
     * on one thread. The small builds run first: more than one build at once, in one round with one
     * task per stage, see
     * {@link HashJoinBuildRoundAtom#buildConcurrently}, and a lone one on the owner. The large builds
     * then run one at a time, in stage order, each through rounds of its own, which already spread one
     * build over the workers.
     * <p>
     * Once every stage built, each binds its slots to the filters that its pairs run in the
     * execution, which its build decided: the rest of a LEFT join's ON residual after the conjuncts
     * that the build ran over its rows, if it ran them, and the whole residual otherwise. Then the
     * execution's steppers learn which stages walk their matches, those up to the last one whose
     * pairs run a filter, before {@link #getProbeFrameRowCap()}, {@link #indexChainLengths} and the
     * tasks read it.
     *
     * @param buildRounds the task-only sequence that runs the rounds of the builds
     */
    boolean build(SqlExecutionCircuitBreaker circuitBreaker, UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds) {
        final HashJoinBuildRoundAtom roundAtom = buildRounds.getAtom();
        final int stageCount = executionStages.size();
        int smallCount = 0;
        for (int k = 0; k < stageCount; k++) {
            final HashJoinBuildStage stage = executionStages.getQuick(k);
            // The planner gives every input a memory pool per worker and, for a filter that is not
            // thread safe, a copy of it per worker, a build ON filter included, so any stage can
            // build on a worker's slot.
            assert stage.isConcurrentBuildCapable();
            if (!stage.prepareBuild()) {
                smallCount++;
            }
        }
        // A round of one task would only add the dispatch's cost to a build on the owner.
        final boolean isConcurrent = smallCount > 1;
        if (isConcurrent) {
            for (int k = 0; k < stageCount; k++) {
                final HashJoinBuildStage stage = executionStages.getQuick(k);
                if (!stage.isLargeBuild()) {
                    roundAtom.addConcurrentStage(stage);
                }
            }
            roundAtom.buildConcurrently(buildRounds);
        }
        for (int k = 0; k < stageCount; k++) {
            final HashJoinBuildStage stage = executionStages.getQuick(k);
            if (!stage.isLargeBuild()) {
                if (isConcurrent) {
                    // The round built the stage.
                    stage.finishBuild();
                } else {
                    stage.build(circuitBreaker, buildRounds);
                }
            }
        }
        for (int k = 0; k < stageCount; k++) {
            final HashJoinBuildStage stage = executionStages.getQuick(k);
            if (stage.isLargeBuild()) {
                // The rounds of a parallel build find the stage through the sequence's atom.
                roundAtom.setBuildingStage(stage);
                stage.build(circuitBreaker, buildRounds);
            }
        }
        boolean hasRows = true;
        // The last stage whose pairs run a filter in the execution, -1 for none.
        int lastFilteredStage = -1;
        for (int k = 0; k < stageCount; k++) {
            final HashJoinBuildStage stage = executionStages.getQuick(k);
            final HashJoinStageSpec stageSpec = specs.getQuick(k);
            // A build that ran the leading conjuncts of the ON residual over its rows leaves the pairs
            // the rest of the residual; any other build, the whole residual. A swapped execution's
            // first stage is an INNER join's, which has no residual.
            final boolean isApplied = stage.isBuildOnFilterApplied();
            if (stageSpec.getBuildOnFilter() != null) {
                for (int i = -1; i < workerCount; i++) {
                    getSlot(i).getStageProbe(k).ofFilters(
                            isApplied ? stageSpec.getRemainingOnFilter(i) : stageSpec.getOnFilter(i),
                            stageSpec.getPostFilter(i)
                    );
                }
            }
            if (stageSpec.hasPairFilter(isApplied)) {
                lastFilteredStage = k;
            }
            stage.bindSymbolKeyTranslation(circuitBreaker);
            stage.bindProbes();
            // Swapped, a later stage's keys read the left input's columns off the first stage's
            // payload reader, which the first stage bound to each slot before.
            final boolean isKeySourceSwapped = isSwapped && k > 0;
            for (int i = -1; i < workerCount; i++) {
                getSlot(i).bindStage(k, stage, stage.getSlot(i), isKeySourceSwapped);
            }
            hasRows &= specs.getQuick(k).isOuter() || stage.getFrozenBuild().getRowCount() > 0;
        }
        ofEnumeration(lastFilteredStage);
        return hasRows;
    }

    /**
     * Joins the next chunk of a frame whose task, or whose last chunk, stopped at the tuple cap, on
     * the owner's slot, -1, into the frame's ring entry, which the consumer has read: the rest of the
     * frame's rows, or of the rows the probe filter kept, from the ring's resume position on, until
     * the chunk's tuples reach the cap. The consumer calls it between two calls to the frame
     * sequence's {@code next()}, the only place where the owner otherwise runs tasks on that slot,
     * since the sequence's {@code dispatchAhead()} runs none, while it holds the frame's task, whose
     * frame memory and kept rows stay put until the consumer collects it.
     *
     * @return the chunk's tuples, or {@link HashJoinStageStepper#CANCELLED} when the execution was
     * cancelled, which leaves nothing to read
     */
    long continueFrame(PageFrameReduceTask task, SqlExecutionCircuitBreaker circuitBreaker) {
        final int frameIndex = task.getFrameIndex();
        final long resumePosition = ring.getResumePosition(frameIndex);
        assert resumePosition > 0;
        final Slot slot = getSlot(-1);
        slot.probeRecord.init(task.getFrameMemory());
        // A frame without a probe filter has no kept rows: its continuation walks its rows.
        final DirectLongList rows = ring.getKeptRowCount(frameIndex) < 0 ? null : task.getFilteredRows();
        continuationCount++;
        // The rest of the first window's last frame under a LIMIT goes on only as far as the
        // consumer reads it: each chunk joins as many rows as the frame joined before it.
        final long rowLimit = frameIndex == rowCappedFrame ? resumePosition : Long.MAX_VALUE;
        return joinRows(slot, task, rows, resumePosition, rowLimit, isWarmedUp, STEPPER_RUN_CONTINUATION, circuitBreaker);
    }

    /**
     * The joined rows' symbol tables, as the open scans whose frames hold each column serve them, in
     * the execution's orientation; see {@link JoinedSymbols}.
     */
    SymbolTableSource getJoinedSymbols() {
        return joinedSymbols;
    }

    /**
     * The most rows of a probe frame whose tuples fit a task's cap,
     * {@code cairo.sql.parallel.hash.join.task.output.max.rows}, at the fan-out that the execution's
     * builds lead one to expect: the cap divided by the product of the rows per key of each build
     * whose matches the workers walk in the execution, and no fewer than 1,000 rows, or
     * {@code cairo.sql.page.frame.min.rows} when that is fewer. A probe row that meets a key yields as
     * many pairs as the key has rows, while the stages that hand their chains to the consumer write
     * one entry per probe row, so their builds do not count.
     * Returns {@code Long.MAX_VALUE} when no such build has more rows than keys. The owner calls it
     * once the builds are done, before the probe scan cuts its frames, so that a frame of duplicate
     * matches leaves no rest to a serial continuation.
     */
    long getProbeFrameRowCap() {
        double fanOut = 1;
        for (int k = 0, n = executionStages.size(); k < n; k++) {
            final HashJoinStageStepper stageStepper = k == 0 ? stepper : specs.getQuick(k).getStepper();
            if (stageStepper.isEnumerated) {
                final FrozenHashJoinBuild build = executionStages.getQuick(k).getFrozenBuild();
                final long keyCount = build.getKeyCount();
                // An empty build of a LEFT join yields one row per probe row.
                if (keyCount > 0) {
                    fanOut *= (double) build.getRowCount() / keyCount;
                }
            }
        }
        if (fanOut <= 1) {
            return Long.MAX_VALUE;
        }
        return Math.max(Math.min(pageFrameMinRows, FAN_OUT_FRAME_MIN_ROWS), (long) (taskOutputMaxRows / fanOut));
    }

    Slot getSlot(int slotId) {
        return slots.getQuick(slotId + 1);
    }

    /**
     * Indexes the chain lengths of the builds of the stages that hand their chains on in the
     * execution, see {@link HashJoinStageStepper}, and whose builds repeat a key, for a count of the
     * join's rows: the count's tasks and the consumer then read a chain's length rather than walk the
     * chain; see {@link FrozenHashJoinBuild#indexChainLengths(SqlExecutionCircuitBreaker)}. The
     * other stages' counts walk no chain: a stage whose stepper walks its key's matches runs its
     * filters over each pair, as the serial join does, and a build of one row per key has no chain.
     * A stage indexes its build once per execution, so a count after {@code toTop()} finds the index.
     * The owner calls it in a count, once the builds are done and before it dispatches the count's
     * first task, with its execution's breaker, which the passes check.
     */
    void indexChainLengths(SqlExecutionCircuitBreaker circuitBreaker) {
        for (int k = 0, n = executionStages.size(); k < n; k++) {
            // The stepper of the execution's orientation, as getProbeFrameRowCap() picks it.
            final HashJoinStageStepper stageStepper = k == 0 ? stepper : specs.getQuick(k).getStepper();
            final HashJoinBuildStage stage = executionStages.getQuick(k);
            if (!stageStepper.isEnumerated && !stage.isBuildUnique()) {
                stage.indexChainLengths(circuitBreaker);
            }
        }
    }

    /**
     * Copies each stage's payload columns when the probe is large enough to repay the copy, which
     * the consumer then reads for the joined rows, and the workers for the stages' filters and, in a
     * swapped execution, for the later stages' keys. A probe whose filter the join stole copies a
     * stage only when the stage's Parquet frames decode past a probe reader's share of the decode
     * cache: the frame rows overstate the rows such a filter keeps. See
     * {@link HashJoinBuildStage#maybeCopyPayload}.
     * The owner calls this once the probe's frames are known and before it dispatches the probes,
     * which then read the copies.
     */
    void maybeCopyPayload(long probeRows, SqlExecutionCircuitBreaker circuitBreaker, UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds) {
        final HashJoinBuildRoundAtom roundAtom = buildRounds.getAtom();
        for (int k = 0, n = executionStages.size(); k < n; k++) {
            final HashJoinBuildStage stage = executionStages.getQuick(k);
            roundAtom.setBuildingStage(stage);
            stage.maybeCopyPayload(probeRows, isProbeFiltered, circuitBreaker, buildRounds);
        }
    }

    /**
     * Takes the scans of the first join's inputs, which the factory opened, the right one first, so
     * that {@link #clear()} closes them while the factory has not handed them on; see
     * {@link #ofOrientation(boolean)}.
     */
    void ofInputCursors(PageFrameCursor leftCursor, PageFrameCursor rightCursor) {
        assert pendingLeftCursor == null && pendingRightCursor == null && pendingBuildCursor == null;
        pendingLeftCursor = leftCursor;
        pendingRightCursor = rightCursor;
    }

    /**
     * Picks the execution's orientation: in the query's, the first stage hashes the right input's
     * scan and the probe tasks join the left input's frames; swapped, the first stage hashes the left
     * input's scan and the probe tasks join the right input's frames. Keeps the scan the stage hashes
     * until {@link #init} hands it to the stage, and hands over the probe scan, which the caller binds
     * the frame sequence to; nothing here throws.
     *
     * @return the probe scan, which the caller then owns
     */
    PageFrameCursor ofOrientation(boolean isSwapped) {
        assert !isSwapped || swapStage != null : "the join never swaps its inputs";
        ofOrientation0(isSwapped);
        final PageFrameCursor probeCursor;
        if (isSwapped) {
            pendingBuildCursor = pendingLeftCursor;
            probeCursor = pendingRightCursor;
        } else {
            pendingBuildCursor = pendingRightCursor;
            probeCursor = pendingLeftCursor;
        }
        pendingLeftCursor = null;
        pendingRightCursor = null;
        return probeCursor;
    }

    /**
     * Counts, on the owner's slot, -1, the joined rows of the rest of a frame whose task, or whose
     * last continuation chunk, stopped at the tuple cap: the frame's rows, or the rows its probe
     * filter kept, from the ring's resume position on, as {@link #continueFrame} would join them, but
     * into a count, with no cap and no tuple written. The consumer calls it, while it holds the
     * frame's task, for a count that begins while the frame's tuples are still to be read, between
     * two calls to the frame sequence's {@code next()}, as it calls {@link #continueFrame}.
     *
     * @return the joined rows, or {@link HashJoinStageStepper#CANCELLED} when the execution was
     * cancelled
     */
    long countFrameRest(PageFrameReduceTask task, SqlExecutionCircuitBreaker circuitBreaker) {
        final int frameIndex = task.getFrameIndex();
        final long resumePosition = ring.getResumePosition(frameIndex);
        assert resumePosition > 0;
        final Slot slot = getSlot(-1);
        slot.probeRecord.init(task.getFrameMemory());
        // A frame without a probe filter has no kept rows: its rest is a range of its rows.
        final DirectLongList rows = ring.getKeptRowCount(frameIndex) < 0 ? null : task.getFilteredRows();
        return countRows(slot, task, rows, resumePosition, isWarmedUp, STEPPER_RUN_COUNTED_REST, circuitBreaker);
    }

    /**
     * Joins one probe frame on a slot: reads the frame through the task's memory, so that the
     * consumer reads the probe rows where the task read them, keeps the rows that the probe filter
     * passes in the task's filtered rows, and writes the frame's tuples into its ring entry, in the
     * probe scan's row order, up to the task's cap. The tuples' count goes to the task, and where the
     * frame's continuation resumes to the ring.
     * <p>
     * A count-only task, which a count of the join's rows dispatches, counts the frame's joined rows
     * into the task instead, see {@link HashJoinStageStepper#countRows}, with no cap. It never touches
     * the ring, neither its tuples nor its resume positions: a count-only {@code next()} may pass a
     * wider dispatch limit than the ring's, so that a count-only frame can run while the consumer
     * still reads the tuples of a frame that shares its entry. Its probe filter writes its kept rows
     * into the task's list, as any task's does, through the compiled filter's overload that takes the
     * list and reserves a row per frame row in it.
     * <p>
     * A Parquet frame decodes the columns that the task reads first, and the others once the frame is
     * joined, only for the rows that the consumer reads, unless the slot's frames lately passed too
     * many rows on to the consumer; a count-only task's frame decodes the task's columns alone. See
     * the class documentation.
     */
    void probeFrame(int slotId, PageFrameReduceTask task, SqlExecutionCircuitBreaker circuitBreaker) {
        final Slot slot = getSlot(slotId);
        // The task runs the stepper that the factory's warm-up picks as it starts.
        final boolean isWarm = isWarmedUp;
        final boolean isParquetFrame = task.isParquetFrame();
        final IntHashSet taskColumns = probeTaskColumns;
        final boolean isLateMaterialized = isParquetFrame
                && taskColumns != null
                && (task.isCountOnly() || probeFilterContext.getSelectivityStats(slotId).shouldUseLateMaterialization());
        final PageFrameMemory frameMemory = isLateMaterialized ? task.populateFrameMemory(taskColumns) : task.populateFrameMemory();
        if (isParquetFrame) {
            slot.parquetFrameCount++;
            slot.decodedColumnCount += isLateMaterialized ? taskColumns.size() : probeColumnCount;
        }
        slot.probeRecord.init(frameMemory);
        final DirectLongList rows;
        if (isProbeFiltered) {
            rows = task.getFilteredRows();
            filterProbeFrame(slotId, task, frameMemory, slot.probeRecord, rows, task.getFrameRowCount());
        } else {
            rows = null;
        }
        if (task.isCountOnly()) {
            // A cancelled task's count is not read: the consumer drops the frames of a cancelled
            // execution.
            task.setFilteredRowCount(Math.max(0, countRows(slot, task, rows, 0, isWarm, STEPPER_RUN_COUNT_TASK, circuitBreaker)));
            return;
        }
        // The first window's last frame under a LIMIT joins only the rows the LIMIT may need, and
        // leaves the rest to the consumer's continuation.
        final long rowLimit = task.getFrameIndex() == rowCappedFrame ? rowCappedFrameRows : Long.MAX_VALUE;
        final long tupleCount = joinRows(slot, task, rows, 0, rowLimit, isWarm, STEPPER_RUN_TASK, circuitBreaker);
        // A cancelled task's tuples are not read: the consumer drops the frames of a cancelled
        // execution, and neither are the rest of its frame's columns.
        task.setFilteredRowCount(Math.max(0, tupleCount));
        if (isParquetFrame && tupleCount != HashJoinStageStepper.CANCELLED) {
            decodeConsumerColumns(slotId, slot, task, rows, isLateMaterialized);
        }
    }

    /**
     * Caps the rows that the task of a frame joins, for the rest of the execution: the last frame of
     * the first window under a LIMIT joins only the rows the LIMIT may need, and leaves the rest of
     * them to the consumer, see {@link #continueFrame}, whose chunks then join as many rows as the
     * frame joined before each. The owner calls it once per execution, before it dispatches the
     * first task, which publishes it to the workers; the execution's {@link #clear()} removes it.
     *
     * @param frameIndex the frame, -1 for none
     * @param rowLimit   the most rows, or rows the probe filter kept, that the frame's task joins
     */
    void setRowCappedFrame(int frameIndex, long rowLimit) {
        assert frameIndex == -1 || rowLimit > 0;
        rowCappedFrame = frameIndex;
        rowCappedFrameRows = rowLimit;
    }

    /**
     * Rewinds the owner's filters, as the filters that the join stole or absorbed rewind when their
     * cursors go back to the top: the probe filter and each stage's ON residual and post-join filter.
     * The workers' copies stay as they are, as theirs do.
     */
    void toTop() {
        toTop(probeFilterContext.getFilter(-1));
        for (int k = 0, n = specs.size(); k < n; k++) {
            final HashJoinStageSpec stageSpec = specs.getQuick(k);
            toTop(stageSpec.getOnFilter(-1));
            toTop(stageSpec.getRemainingOnFilter(-1));
            toTop(stageSpec.getPostFilter(-1));
        }
    }

    // Binds a stage's stepper to what its lookup reads, as the stage's key layout lays the key out,
    // and to the stepper of the next stage, null for the last one.
    private static void ofStepper(
            HashJoinStageStepper stepper,
            HashJoinKeyLayout keyLayout,
            boolean isOuter,
            @Nullable HashJoinStageStepper next
    ) {
        final int keyKind = keyLayout.isKeyStaged()
                ? HashJoinStageStepper.KEY_STAGED
                : keyLayout.isSymbolKey() ? HashJoinStageStepper.KEY_SYMBOL : HashJoinStageStepper.KEY_INT;
        stepper.of(keyKind, keyLayout.isKeyStaged() ? -1 : keyLayout.getProbeKeyColumn(), isOuter, next);
    }

    // Tells the owner's filter and its worker copies, if any, that the cursor closed.
    private static Throwable cursorClosed(Throwable failure, @Nullable Function ownerFilter, @Nullable ObjList<Function> workerFilters) {
        failure = HashJoinBuildStage.cursorClosed(failure, ownerFilter);
        if (workerFilters != null) {
            for (int i = 0, n = workerFilters.size(); i < n; i++) {
                failure = HashJoinBuildStage.cursorClosed(failure, workerFilters.getQuick(i));
            }
        }
        return failure;
    }

    private static void toTop(@Nullable Function filter) {
        if (filter != null) {
            filter.toTop();
        }
    }

    /**
     * Leaves in {@code rows}, ascending, the frame's rows that the probe filter passes, on the slot's
     * own copy of the filter. The rows go to the task's list rather than the slot's, because the
     * frame's continuation reads them again after the slot has moved on to other frames. The
     * compiled filter writes up to a row per frame row into the list it is given, which this list
     * holds room for, and reads raw column addresses, so column tops and Parquet type casts, which
     * the record resolves per row, fall back to the interpreted filter.
     */
    private void filterProbeFrame(
            int slotId,
            PageFrameReduceTask task,
            PageFrameMemory frameMemory,
            PageFrameMemoryRecord probeRecord,
            DirectLongList rows,
            long rowCount
    ) {
        rows.clear();
        final CompiledFilter compiledFilter = probeFilterContext.getCompiledFilter();
        if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts()) {
            AsyncFilterUtils.applyFilter(probeFilterContext.getFilter(slotId), rows, probeRecord, rowCount);
        } else {
            AsyncFilterUtils.applyCompiledFilter(
                    compiledFilter,
                    probeFilterContext.getBindVarMemory(),
                    probeFilterContext.getBindVarFunctions(),
                    frameMemory,
                    task.getFrameSequence().getPageFrameAddressCache(),
                    probeFilterContext.getDataAddresses(slotId),
                    probeFilterContext.getAuxAddresses(slotId),
                    rows,
                    rowCount
            );
        }
    }

    /**
     * Lists, ascending and once each, the rows of a joined frame that the consumer reads: the probe
     * rows of the frame's tuples and, when the task left the rest of the frame to a continuation, the
     * rows that the continuation joins, all of the frame's rows or of the rows its probe filter kept
     * from the ring's resume position on, in the probe scan's order. The tuples hold their probe rows
     * in that order, ascending or, for a backward scan, descending, and each probe row's tuples one
     * after another. The rows a continuation joins come after the joined ones in the scan's order,
     * so they hold the frame's highest rows, or, for a backward scan, its lowest.
     *
     * @param keptRows the rows the probe filter kept, ascending; null without a probe filter
     * @param sink     the list of the rows, or null to count them only
     * @return the rows
     */
    private long collectConsumerRows(PageFrameReduceTask task, @Nullable DirectLongList keptRows, @Nullable DirectLongList sink) {
        final int frameIndex = task.getFrameIndex();
        final DirectLongList tuples = ring.getTuples(frameIndex);
        final int stride = ring.getTupleStride();
        final long tupleCount = tuples.size() / stride;
        final long resumePosition = ring.getResumePosition(frameIndex);
        final long rowCount = keptRows != null ? keptRows.size() : task.getFrameRowCount();
        final long restCount = resumePosition < 0 ? 0 : rowCount - resumePosition;
        if (sink != null) {
            sink.clear();
            sink.ensureCapacity(Math.min(tupleCount, rowCount) + restCount);
        }
        long count = 0;
        if (isProbeDescending) {
            // The rest of a backward scan's frame holds its lowest rows.
            for (long i = 0; i < restCount; i++) {
                count++;
                if (sink != null) {
                    sink.add(keptRows != null ? keptRows.get(i) : i);
                }
            }
            long lastRow = -1;
            for (long t = tupleCount - 1; t > -1; t--) {
                final long row = tuples.get(t * stride);
                if (row != lastRow) {
                    lastRow = row;
                    count++;
                    if (sink != null) {
                        sink.add(row);
                    }
                }
            }
        } else {
            long lastRow = -1;
            for (long t = 0; t < tupleCount; t++) {
                final long row = tuples.get(t * stride);
                if (row != lastRow) {
                    lastRow = row;
                    count++;
                    if (sink != null) {
                        sink.add(row);
                    }
                }
            }
            for (long p = rowCount - restCount; p < rowCount; p++) {
                count++;
                if (sink != null) {
                    sink.add(keptRows != null ? keptRows.get(p) : p);
                }
            }
        }
        return count;
    }

    /**
     * Once a task joined a Parquet frame, decodes the frame's other columns, those that only the
     * consumer reads, for the rows it reads, if the task decoded the frame late, and records in the
     * slot's selectivity statistics the share of the frame's rows that the consumer reads, which
     * decides whether the slot's next frames decode late. Decodes nothing when the consumer reads
     * no row of the frame.
     *
     * @param keptRows the rows the probe filter kept, null without a probe filter
     */
    private void decodeConsumerColumns(int slotId, Slot slot, PageFrameReduceTask task, @Nullable DirectLongList keptRows, boolean isLateMaterialized) {
        // The slot's row list of the probe input's context serves no other purpose while a task joins.
        final DirectLongList consumerRows = isLateMaterialized ? probeFilterContext.getFilteredRows(slotId) : null;
        final long consumerRowCount = collectConsumerRows(task, keptRows, consumerRows);
        probeFilterContext.getSelectivityStats(slotId).update(consumerRowCount, task.getFrameRowCount());
        if (consumerRows != null) {
            final IntHashSet taskColumns = probeTaskColumns;
            assert taskColumns != null;
            if (task.populateRemainingColumns(taskColumns, consumerRows, true)) {
                slot.decodedColumnCount += probeColumnCount - taskColumns.size();
            } else {
                // Only a frame whose rows the consumer reads none of decodes no more: the task's
                // columns are never all of the frame's.
                assert consumerRowCount == 0;
            }
        }
    }

    // Initializes the owner's filter, if any, against the joined rows' symbol tables, and its
    // workers' copies, which take tables of their own.
    private void initFilter(@Nullable Function ownerFilter, @Nullable ObjList<Function> workerFilters, SqlExecutionContext executionContext) throws SqlException {
        if (ownerFilter == null) {
            return;
        }
        ownerFilter.init(joinedSymbols, executionContext);
        if (workerFilters != null) {
            final boolean isCloneSymbolTables = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(workerFilters, joinedSymbols, executionContext, ownerFilter);
            } finally {
                executionContext.setCloneSymbolTables(isCloneSymbolTables);
            }
        }
    }

    /**
     * Counts the joined rows of a frame's rows, or of the rows its probe filter kept, from the given
     * position in the probe scan's order to the end of the frame, on a slot; touches no ring entry.
     *
     * @param rows   the rows the probe filter kept, null without a probe filter
     * @param isWarm true to run the stages' hidden stepper copies, false for the shared template's
     * @param kind   what the slot's stepper run counters count this as; see
     *               {@link #getStepperRunCount(int, boolean)}
     * @return the joined rows, or {@link HashJoinStageStepper#CANCELLED} when the execution was
     * cancelled
     */
    private long countRows(
            Slot slot,
            PageFrameReduceTask task,
            @Nullable DirectLongList rows,
            long from,
            boolean isWarm,
            int kind,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final HashJoinStageStepper stepper = isWarm ? this.stepper : templateStepper;
        slot.countStepperRun(kind, stepper != templateStepper);
        slot.ofCount(task.getFrameSequence(), circuitBreaker, pairsPerCheck);
        try {
            return rows != null
                    ? stepper.countRows(slot, rows, from, isProbeDescending)
                    : stepper.countRows(slot, task.getFrameRowCount(), from, isProbeDescending);
        } finally {
            slot.of(null, null, null, 0);
        }
    }

    /**
     * Joins a frame's rows, or the rows its probe filter kept, from the given position in the probe
     * scan's order, on a slot, into the frame's ring entry: no more than {@code rowLimit} rows, and
     * it stops early at the first probe row's boundary where the entry holds the task's cap of
     * tuples: the frame's rows, or {@code cairo.sql.parallel.hash.join.task.output.max.rows} if that
     * is more. Records in the ring the frame's kept-row count and the position its continuation
     * resumes at, -1 once every row is joined or when the execution was cancelled.
     *
     * @param rows     the rows the probe filter kept, null without a probe filter
     * @param rowLimit the most rows to join, {@code Long.MAX_VALUE} for the rest of the frame
     * @param isWarm   true to run the stages' hidden stepper copies, false for the shared template's
     * @param kind     what the slot's stepper run counters count this as; see
     *                 {@link #getStepperRunCount(int, boolean)}
     * @return the tuples, or {@link HashJoinStageStepper#CANCELLED} when the execution was cancelled
     */
    private long joinRows(
            Slot slot,
            PageFrameReduceTask task,
            @Nullable DirectLongList rows,
            long from,
            long rowLimit,
            boolean isWarm,
            int kind,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final HashJoinStageStepper stepper = isWarm ? this.stepper : templateStepper;
        slot.countStepperRun(kind, stepper != templateStepper);
        final int frameIndex = task.getFrameIndex();
        final long frameRowCount = task.getFrameRowCount();
        final long rowCount = rows != null ? rows.size() : frameRowCount;
        final long keptRowCount = rows != null ? rowCount : -1;
        final long to = rowLimit < rowCount - from ? from + rowLimit : rowCount;
        final long tupleCap = Math.max(taskOutputMaxRows, frameRowCount) * ring.getTupleStride();
        // Most probe rows yield one tuple, so the rows to join size the entry, which grows as
        // fan-out fills it.
        final DirectLongList tuples = ring.openTuples(frameIndex, to - from);
        slot.of(tuples, task.getFrameSequence(), circuitBreaker, pairsPerCheck);
        final long position;
        try {
            position = rows != null
                    ? stepper.probeRows(slot, rows, from, to, isProbeDescending, tupleCap)
                    : stepper.probeRows(slot, rowCount, from, to, isProbeDescending, tupleCap);
        } finally {
            slot.of(null, null, null, 0);
        }
        final boolean isCancelled = position == HashJoinStageStepper.CANCELLED;
        ring.setResumePosition(frameIndex, keptRowCount, isCancelled || position == rowCount ? -1 : position);
        if (isCancelled) {
            return HashJoinStageStepper.CANCELLED;
        }
        slot.joinedRowCount += position - from;
        return tuples.size() / ring.getTupleStride();
    }

    // Decides which stages' steppers walk their matches in the execution, the hidden copies and the
    // shared template's instances alike, in the execution's orientation: every stage up to the last
    // one whose pairs run a filter, since a filter reads the joined row, the columns of every stage
    // before its own included, so those stages' payload readers must sit on the current match; the
    // stages after it hand their chains to the consumer. Swapped, the later stages read their keys
    // from the first stage's payload reader, which only a walk of the first stage's matches positions
    // on each of them, so the workers walk them when any later stage follows, whether or not a filter
    // reads them.
    private void ofEnumeration(int lastFilteredStage) {
        final int stageCount = specs.size();
        final boolean isFirstEnumerated = lastFilteredStage > -1 || (isSwapped && stageCount > 1);
        stepper.ofEnumerated(isFirstEnumerated);
        templateStepper.ofEnumerated(isFirstEnumerated);
        for (int k = 1; k < stageCount; k++) {
            final HashJoinStageSpec stageSpec = specs.getQuick(k);
            final boolean isEnumerated = k <= lastFilteredStage;
            stageSpec.getStepper().ofEnumerated(isEnumerated);
            stageSpec.getTemplateStepper().ofEnumerated(isEnumerated);
        }
    }

    // Points the probe side at the orientation: the probe input's scan order and filter, the first
    // stage's stepper, and the key source of every later stage: the probe record, or, swapped, the
    // first stage's payload, whose columns sit elsewhere.
    private void ofOrientation0(boolean isSwapped) {
        this.isSwapped = isSwapped;
        executionStages.setQuick(0, isSwapped ? swapStage : stages.getQuick(0));
        scanBySource[0] = isSwapped ? 1 : 0;
        scanBySource[1] = isSwapped ? 0 : 1;
        final HashJoinInput probeInput = isSwapped ? rightInput : leftInput;
        probeFilterContext = probeInput.getFilterContext();
        probeTaskColumns = isSwapped ? probeTaskColumnsB : probeTaskColumnsA;
        probeColumnCount = probeInput.getMetadata().getColumnCount();
        isProbeFiltered = probeInput.hasStolenFilter();
        isProbeDescending = probeInput.getOrder() == ORDER_DESC;
        final HashJoinStageSpec firstSpec = specs.getQuick(0);
        stepper = isSwapped ? firstSpec.getSwapStepper() : firstSpec.getStepper();
        templateStepper = isSwapped ? firstSpec.getSwapTemplateStepper() : firstSpec.getTemplateStepper();
        if (swapStage != null) {
            for (int k = 1, n = specs.size(); k < n; k++) {
                final HashJoinStageSpec stageSpec = specs.getQuick(k);
                final HashJoinKeyLayout keyLayout = isSwapped ? stageSpec.getSwapKeySourceLayout() : stageSpec.getKeyLayout();
                assert keyLayout != null;
                final int keyColumn = keyLayout.isKeyStaged() ? -1 : keyLayout.getProbeKeyColumn();
                stageSpec.getStepper().ofKeyColumn(keyColumn);
                stageSpec.getTemplateStepper().ofKeyColumn(keyColumn);
                stages.getQuick(k).setKeySourceSwapped(isSwapped);
            }
        }
    }

    /**
     * One slot's probe-side state: the record it reads probe frames through, the flat joined record
     * that the stages' filters read, its bindings to every stage's build, and the tuple it assembles
     * for the current probe row.
     */
    static final class Slot implements Mutable, QuietCloseable {
        // The current probe row's entry for each stage; see HashJoinStageStepper.
        final long[] entries;
        final PageFrameMemoryRecord probeRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        // The joined row of the current probe row and the stages' current matches.
        final MultiHashJoinRecord record;
        // The frames and chunks that the slot joined or counted in the open cursor's execution, by
        // kind and stepper: kind * 2 for the shared template's, kind * 2 + 1 for the hidden copies';
        // see AsyncHashJoinAtom.getStepperRunCount().
        final long[] stepperRunCounts = new long[8];
        private final ObjList<StageProbe> stageProbes = new ObjList<>();
        // The probe columns that the slot's tasks decoded in the open cursor's execution, summed over
        // its Parquet frames; see AsyncHashJoinAtom.getDecodedProbeColumnCount().
        long decodedColumnCount;
        // True while the slot counts a frame's joined rows rather than writes its tuples; see
        // ofCount().
        boolean isCountOnly;
        // The rows the slot's tasks and chunks joined into tuples in the open cursor's execution;
        // see AsyncHashJoinAtom.getJoinedProbeRowCount().
        long joinedRowCount;
        // The joined rows a count has met so far.
        long pairCount;
        // The rows that each tuple that comes out of the last stage stands for in a count: the
        // product of the chain lengths of the deferred stages that the current probe row passed.
        long pairMultiplier;
        // The pairs left before the next check of the circuit breaker.
        long pairsUntilCheck;
        // The Parquet frames that the slot's tasks read in the open cursor's execution.
        long parquetFrameCount;
        // The current probe row, by its index in the frame.
        long probeRow;
        // The ring entry of the frame the slot joins; null between frames.
        DirectLongList tuples;
        // The breaker and the sequence of the frame the slot joins; null between frames.
        private SqlExecutionCircuitBreaker circuitBreaker;
        private PageFrameSequence<?> frameSequence;
        private long pairsPerCheck;

        Slot(int stageCount, MultiHashJoinRecord record) {
            entries = new long[stageCount];
            for (int k = 0; k < stageCount; k++) {
                stageProbes.add(new StageProbe());
            }
            this.record = record;
            record.setProbeRecord(probeRecord);
        }

        @Override
        public void clear() {
            Misc.clearObjList(stageProbes);
            // The probe scan's symbol tables go here, before the frame sequence closes the scan.
            probeRecord.of(null);
            tuples = null;
            isCountOnly = false;
            circuitBreaker = null;
            frameSequence = null;
        }

        @Override
        public void close() {
            clear();
            probeRecord.close();
        }

        StageProbe getStageProbe(int stage) {
            return stageProbes.getQuick(stage);
        }

        // Counts a frame or chunk that the slot joins or counts, by its kind and the stepper it runs:
        // the stages' own, hidden copies, or the shared template's instances.
        void countStepperRun(int kind, boolean isCopy) {
            stepperRunCounts[kind * 2 + (isCopy ? 1 : 0)]++;
        }

        /**
         * True when the execution was cancelled, as {@link AsyncHashJoinAtom#isInterrupted} tells;
         * re-arms the countdown of pairs to the next check.
         */
        boolean isInterrupted() {
            pairsUntilCheck = pairsPerCheck;
            return AsyncHashJoinAtom.isInterrupted(circuitBreaker, frameSequence);
        }

        // Appends the current probe row and its entry for every stage to the frame's tuples.
        void appendTuple() {
            final DirectLongList tuples = this.tuples;
            tuples.add(probeRow);
            for (long entry : entries) {
                tuples.add(entry);
            }
        }

        // Binds the slot to the k-th stage's build for the open cursor's execution, casting the
        // stage slot's probe once, so that the stepper's call sites see one probe class. A later
        // stage of a swapped execution reads its keys off the slot's payload reader of the first
        // stage, which the slot bound before.
        void bindStage(int k, HashJoinBuildStage stage, HashJoinBuildStage.Slot stageSlot, boolean isKeySourceSwapped) {
            final StageProbe stageProbe = stageProbes.getQuick(k);
            final FrozenHashJoinBuild.Probe probe = stageSlot.getProbe();
            if (stage.isKeyStaged()) {
                stageProbe.recordProbe = (FrozenHashJoinBuild.RecordProbe) probe;
            } else {
                stageProbe.intProbe = (FrozenHashJoinBuild.IntProbe) probe;
            }
            if (isKeySourceSwapped) {
                final HashJoinPayloadSource.Reader keySource = stageProbes.getQuick(0).reader;
                assert k > 0 && keySource != null;
                stageSlot.ofSwapKeySource(keySource);
                stageProbe.keyRecord = stageSlot.swapKeyRecord;
                stageProbe.keySource = keySource;
            } else {
                stageProbe.keyRecord = stageSlot.keyRecord;
                stageProbe.keySource = probeRecord;
            }
            stageProbe.symbolKeyView = stageSlot.symbolKeyView;
            stageProbe.reader = (HashJoinPayloadSource.Reader) probe.getRecord();
            stageProbe.isBuildUnique = stage.isBuildUnique();
            // The stage's filters read its matches through the probe's payload reader.
            record.setStageRecord(k, stageProbe.reader);
        }

        // Binds the slot to the frame it joins, or, given nulls, unbinds it.
        void of(DirectLongList tuples, PageFrameSequence<?> frameSequence, SqlExecutionCircuitBreaker circuitBreaker, long pairsPerCheck) {
            this.tuples = tuples;
            this.frameSequence = frameSequence;
            this.circuitBreaker = circuitBreaker;
            this.pairsPerCheck = pairsPerCheck;
            this.pairsUntilCheck = pairsPerCheck;
            this.isCountOnly = false;
        }

        // Binds the slot to a frame whose joined rows it counts, from zero, rather than writes.
        void ofCount(PageFrameSequence<?> frameSequence, SqlExecutionCircuitBreaker circuitBreaker, long pairsPerCheck) {
            of(null, frameSequence, circuitBreaker, pairsPerCheck);
            this.isCountOnly = true;
            this.pairCount = 0;
            this.pairMultiplier = 1;
        }
    }

    /**
     * A slot's binding to one stage's build for the open cursor's execution: its probe, which
     * positions its payload reader on each match, and what the probe's key reads; and the slot's
     * copies of the stage's filters, which stay bound across executions.
     */
    static final class StageProbe implements Mutable {
        FrozenHashJoinBuild.IntProbe intProbe;
        // True when the build holds one row per key, so that a lookup positions the payload itself.
        boolean isBuildUnique;
        // What a staged key's sink reads: the key source, or a view that translates its SYMBOL keys.
        Record keyRecord;
        // The record whose columns the stage's key reads.
        Record keySource;
        // The slot's copy of the stage's ON residual and post-join filter; null for none.
        Function onFilter;
        Function postFilter;
        HashJoinPayloadSource.Reader reader;
        FrozenHashJoinBuild.RecordProbe recordProbe;
        SymbolKeyTranslator.View symbolKeyView;

        @Override
        public void clear() {
            intProbe = null;
            recordProbe = null;
            keyRecord = null;
            keySource = null;
            symbolKeyView = null;
            reader = null;
            isBuildUnique = false;
        }

        void ofFilters(@Nullable Function onFilter, @Nullable Function postFilter) {
            this.onFilter = onFilter;
            this.postFilter = postFilter;
        }
    }

    /**
     * The symbol tables of the joined rows' columns, which the stages' filters initialize against: a
     * column resolves through the open scan whose frames hold it, by its frame column. In the query's
     * orientation, a column of the left input resolves through the probe scan, and a stage's column
     * through the stage's build scan. A swapped execution's probe scan reads the right input, whose
     * columns are the first stage's, and its first stage's build scan reads the left input. A worker's
     * copy of a filter takes fresh tables.
     */
    private static final class JoinedSymbols implements SymbolTableSource {
        private final AsyncHashJoinAtom atom;
        private final AsyncHashJoinSpec spec;
        // The open probe scan; null while no cursor is open.
        private SymbolTableSource probeSymbols;

        JoinedSymbols(AsyncHashJoinSpec spec, AsyncHashJoinAtom atom) {
            this.spec = spec;
            this.atom = atom;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            final int source = spec.getColumnSource(columnIndex);
            return getScan(source).getSymbolTable(getFrameColumn(source, columnIndex));
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            final int source = spec.getColumnSource(columnIndex);
            return getScan(source).newSymbolTable(getFrameColumn(source, columnIndex));
        }

        // The column of the frames that hold the column, which the query's column map names: a frame
        // column of the left input, or a stage's payload column, as the stage's build input lists it.
        private int getFrameColumn(int source, int columnIndex) {
            final int column = spec.getColumnIndex(columnIndex);
            return source == 0 ? column : spec.getStage(source - 1).getPayloadColumns().getQuick(column);
        }

        // The open scan whose frames hold the columns of the source in the query's orientation.
        private SymbolTableSource getScan(int source) {
            final int scan = atom.scanBySource[source];
            return scan == 0 ? probeSymbols : atom.getStage(scan - 1).getBuildSymbols();
        }
    }
}
