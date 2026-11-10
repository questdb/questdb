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
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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
 * stage builds, in stage order, on the owner or in rounds of the join's build sequence, binds its
 * SYMBOL key translation and hands every slot its probe of the build. The owner dispatches the probe
 * tasks after the build returns, and the first task that the frame sequence publishes is what
 * publishes the builds and the slots' probes to the workers.
 * <p>
 * The first join of a swap candidate, see {@link AsyncHashJoinSpec#isSwapCandidate()}, has two runtime
 * stages: the one that hashes its right input, and the one that hashes its left input, which an
 * execution that swaps the inputs builds instead, while the right input's frames go to the probe
 * tasks. Each execution picks one orientation, and the stage, the probe scan, the steppers, the
 * joined records and the joined rows' symbol tables follow it until the next execution;
 * {@link #getStage(int)} returns the execution's stages.
 * <p>
 * A probe task joins one frame, see {@link #probeFrame}: it keeps the frame's rows that the probe
 * filter passes, if the probe input has one, and for each kept row, in the probe scan's order, the
 * first stage's stepper looks the row's key up and hands each match on to the next stage's stepper,
 * and so on, every later stage's key read off the probe row too, and the last stage appends the row
 * and every stage's entry to the frame's ring entry, which the consumer reads in frame order. A stage
 * with an ON residual or a post-join filter runs them over each pair on the slot's flat joined
 * record, with the slot's copies of the filters, and the workers walk the matches of every stage up
 * to the last such stage, whose filters may read them; the stages after it hand their chains to the
 * consumer. The task stops at a probe row's boundary once the frame's tuples reach the task's cap,
 * {@code max(cairo.sql.parallel.hash.join.task.output.max.rows, the frame's rows)}, and records in the
 * ring where the frame's continuation resumes: the consumer, once it has read those tuples, joins the
 * rest of the frame in chunks of the same cap on the owner's slot, see {@link #continueFrame}. One
 * probe row's pairs always finish in one chunk, so a key with more matches than the cap overruns it
 * by its own matches, in memory that the execution's tracker counts.
 * <p>
 * {@link #clear()} requires every task to have finished and the consumer to be done with the tuples
 * and the payload readers.
 */
public final class AsyncHashJoinAtom implements StatefulAtom, PerWorkerLockOwner {
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
    // The stages as compiled, which the atom borrows for their join types and steppers.
    private final ObjList<HashJoinStageSpec> specs = new ObjList<>();
    // The runtime stages in the query's orientation: the first one hashes the right input.
    private final ObjList<HashJoinBuildStage> stages = new ObjList<>();
    // The first stage of a swapped execution, which hashes the left input; null when the join never
    // swaps.
    @Nullable
    private final HashJoinBuildStage swapStage;
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
    // The pairs an enumerated stage walks between two checks of the circuit breaker.
    private long pairsPerCheck = 1;
    // The first stage's build scan, which the factory opened before the probe scan, from the moment
    // the execution picks its orientation until the stage adopts it in init(); null otherwise.
    private PageFrameCursor pendingBuildCursor;
    // The scans of the first join's inputs, which the factory opened, the right one first, from the
    // moment it opens them until it picks the execution's orientation; null otherwise.
    private PageFrameCursor pendingLeftCursor;
    private PageFrameCursor pendingRightCursor;
    // The probe input's stolen filter, with its worker copies and JIT code, which the atom borrows;
    // a context without a filter when the probe input has none. The left input's in the query's
    // orientation, the right input's in a swapped execution.
    private AsyncFilterContext probeFilterContext;
    // The first stage's stepper in the execution's orientation.
    private HashJoinStageStepper stepper;

    /**
     * Borrows the spec's inputs, filter contexts and steppers. The key layouts borrow the inputs'
     * metadata, so construct the atom while the inputs are open. Nothing here allocates native
     * memory, which the stages, the slots and the ring take only when an execution opens them, so a
     * failure leaves nothing to release.
     */
    AsyncHashJoinAtom(CairoConfiguration configuration, AsyncHashJoinSpec spec, int workerCount) {
        this.workerCount = workerCount;
        this.taskOutputMaxRows = configuration.getSqlParallelHashJoinTaskOutputMaxRows();
        this.joinedSymbols = new JoinedSymbols(spec, this);
        final int stageCount = spec.getStageCount();
        this.leftInput = spec.getLeftInput();
        this.rightInput = spec.getStage(0).getBuildInput();
        this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        this.ring = new HashJoinTupleRing(configuration, stageCount);
        this.scanBySource = new int[stageCount + 1];
        final int probeColumnCount = leftInput.getMetadata().getColumnCount();
        // The stages up to the last one whose pairs run a filter walk their matches on the workers:
        // a filter reads the joined row, the columns of every stage before its own included, so those
        // stages' payload readers must sit on the current match. The stages after it hand their
        // chains to the consumer.
        int lastEnumeratedStage = -1;
        for (int k = 0; k < stageCount; k++) {
            if (spec.getStage(k).hasPairFilter()) {
                lastEnumeratedStage = k;
            }
        }
        for (int k = 0; k < stageCount; k++) {
            final HashJoinStageSpec stageSpec = spec.getStage(k);
            final HashJoinInput buildInput = stageSpec.getBuildInput();
            final HashJoinKeyLayout keyLayout = stageSpec.getKeyLayout();
            final HashJoinBuildStage stage = new HashJoinBuildStage(
                    configuration,
                    buildInput.getFactory(),
                    buildInput.getFilterContext(),
                    keyLayout,
                    stageSpec.getPayloadColumns(),
                    probeColumnCount,
                    buildInput.getOrder(),
                    workerCount
            );
            stages.add(stage);
            executionStages.add(stage);
            specs.add(stageSpec);
            scanBySource[k + 1] = k + 1;
            stageSpec.setPayloadCopyable(stage.isPayloadCopyable());
            // Each stage's matches go on to the next stage's stepper.
            ofStepper(
                    stageSpec.getStepper(),
                    keyLayout,
                    stageSpec.isOuter(),
                    k <= lastEnumeratedStage,
                    k + 1 < stageCount ? spec.getStage(k + 1).getStepper() : null
            );
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
            assert swapStepper != null && !firstSpec.isOuter();
            // A join that may swap its inputs runs one stage; see AsyncHashJoinSpec.addStage().
            assert stageCount == 1;
            ofStepper(swapStepper, swapKeyLayout, false, firstSpec.hasPairFilter(), null);
        } else {
            swapStage = null;
        }
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

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
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
        // A later stage's keys read the left input's columns, which the probe scan holds, so it
        // translates its SYMBOL keys from the probe scan's dictionaries and presizes its key table by
        // the probe scan's table; it opens its own build scan, in its input's scan order.
        for (int k = 1, n = executionStages.size(); k < n; k++) {
            final HashJoinInput buildInput = specs.getQuick(k).getBuildInput();
            final PageFrameCursor laterBuildCursor = buildInput.getFactory().getPageFrameCursor(executionContext, buildInput.getOrder());
            // The stage owns the build scan from here on, whether its init returns or throws.
            executionStages.getQuick(k).init(symbolTableSource, executionContext, laterBuildCursor);
        }
        for (int i = 0, n = slots.size(); i < n; i++) {
            final Slot slot = slots.getQuick(i);
            slot.probeRecord.of(symbolTableSource);
            // The stages bind their payload readers to the record when they build.
            slot.record.setOrientation(isSwapped);
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
        continuationCount = 0;
        joinedSymbols.probeSymbols = symbolTableSource;
        isJoinFilterInitialized = true;
        for (int k = 0, n = specs.size(); k < n; k++) {
            final HashJoinStageSpec stageSpec = specs.getQuick(k);
            initFilter(stageSpec.getOnFilter(-1), stageSpec.getWorkerOnFilters(), executionContext);
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
     * Builds every stage, in stage order, and binds every slot to the builds. The cursor calls it
     * once per execution, on first demand for a row, on the thread that then dispatches the probes
     * and before it dispatches any; nothing here runs when the cursor merely opens. Returns false
     * when the build of any INNER join has no row, so that the join returns no row and needs no
     * probe task. Every stage builds even then, as every serial join of a stack builds its map before
     * it reads its master, so that a build that fails fails in both plans. On failure the caller
     * closes the cursor, whose clear() closes the builds.
     *
     * @param buildRounds the task-only sequence that runs the rounds of a stage's parallel build
     */
    boolean build(SqlExecutionCircuitBreaker circuitBreaker, UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds) {
        boolean hasRows = true;
        final HashJoinBuildRoundAtom roundAtom = buildRounds.getAtom();
        for (int k = 0, n = executionStages.size(); k < n; k++) {
            final HashJoinBuildStage stage = executionStages.getQuick(k);
            // The rounds of a parallel build find the stage through the sequence's atom.
            roundAtom.setBuildingStage(stage);
            stage.build(circuitBreaker, buildRounds);
            stage.bindSymbolKeyTranslation(circuitBreaker);
            stage.bindProbes();
            for (int i = -1; i < workerCount; i++) {
                getSlot(i).bindStage(k, stage, stage.getSlot(i));
            }
            hasRows &= specs.getQuick(k).isOuter() || stage.getFrozenBuild().getRowCount() > 0;
        }
        return hasRows;
    }

    /**
     * Joins the next chunk of a frame whose task, or whose last chunk, stopped at the tuple cap, on
     * the owner's slot, -1, into the frame's ring entry, which the consumer has read: the rest of the
     * frame's rows, or of the rows the probe filter kept, from the ring's resume position on, until
     * the chunk's tuples reach the cap. The consumer calls it between two calls to the frame
     * sequence's {@code next()}, the only place where the owner otherwise runs tasks on that slot,
     * while it holds the frame's task, whose frame memory and kept rows stay put until the consumer
     * collects it.
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
        return joinRows(slot, task, rows, resumePosition, circuitBreaker);
    }

    /**
     * The joined rows' symbol tables, as the open scans whose frames hold each column serve them, in
     * the execution's orientation; see {@link JoinedSymbols}.
     */
    SymbolTableSource getJoinedSymbols() {
        return joinedSymbols;
    }

    Slot getSlot(int slotId) {
        return slots.getQuick(slotId + 1);
    }

    /**
     * Copies each stage's payload columns when the probe is large enough to repay the copy; see
     * {@link HashJoinBuildStage#maybeCopyPayload}. The owner calls this once the probe's frames are
     * known and before it dispatches the probes, which then read the copies.
     */
    void maybeCopyPayload(long probeRows, SqlExecutionCircuitBreaker circuitBreaker, UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds) {
        final HashJoinBuildRoundAtom roundAtom = buildRounds.getAtom();
        for (int k = 0, n = executionStages.size(); k < n; k++) {
            final HashJoinBuildStage stage = executionStages.getQuick(k);
            roundAtom.setBuildingStage(stage);
            stage.maybeCopyPayload(probeRows, circuitBreaker, buildRounds);
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
     * Joins one probe frame on a slot: reads the frame through the task's memory, so that the
     * consumer reads the probe rows where the task read them, keeps the rows that the probe filter
     * passes in the task's filtered rows, and writes the frame's tuples into its ring entry, in the
     * probe scan's row order, up to the task's cap. The tuples' count goes to the task, and where the
     * frame's continuation resumes to the ring.
     */
    void probeFrame(int slotId, PageFrameReduceTask task, SqlExecutionCircuitBreaker circuitBreaker) {
        final Slot slot = getSlot(slotId);
        final PageFrameMemory frameMemory = task.populateFrameMemory();
        slot.probeRecord.init(frameMemory);
        final DirectLongList rows;
        if (isProbeFiltered) {
            rows = task.getFilteredRows();
            filterProbeFrame(slotId, task, frameMemory, slot.probeRecord, rows, task.getFrameRowCount());
        } else {
            rows = null;
        }
        final long tupleCount = joinRows(slot, task, rows, 0, circuitBreaker);
        // A cancelled task's tuples are not read: the consumer drops the frames of a cancelled
        // execution.
        task.setFilteredRowCount(Math.max(0, tupleCount));
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
            toTop(stageSpec.getPostFilter(-1));
        }
    }

    // Binds a stage's stepper to what its lookup reads, as the stage's key layout lays the key out,
    // and to the stepper of the next stage, null for the last one.
    private static void ofStepper(
            HashJoinStageStepper stepper,
            HashJoinKeyLayout keyLayout,
            boolean isOuter,
            boolean isEnumerated,
            @Nullable HashJoinStageStepper next
    ) {
        final int keyKind = keyLayout.isKeyStaged()
                ? HashJoinStageStepper.KEY_STAGED
                : keyLayout.isSymbolKey() ? HashJoinStageStepper.KEY_SYMBOL : HashJoinStageStepper.KEY_INT;
        stepper.of(keyKind, keyLayout.isKeyStaged() ? -1 : keyLayout.getProbeKeyColumn(), isOuter, isEnumerated, next);
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
     * Joins a frame's rows, or the rows its probe filter kept, from the given position in the probe
     * scan's order, on a slot, into the frame's ring entry, and stops at the first probe row's
     * boundary where the entry holds the task's cap of tuples: the frame's rows, or
     * {@code cairo.sql.parallel.hash.join.task.output.max.rows} if that is more. Records in the ring
     * the frame's kept-row count and the position its continuation resumes at, -1 once every row is
     * joined or when the execution was cancelled.
     *
     * @param rows the rows the probe filter kept, null without a probe filter
     * @return the tuples, or {@link HashJoinStageStepper#CANCELLED} when the execution was cancelled
     */
    private long joinRows(Slot slot, PageFrameReduceTask task, @Nullable DirectLongList rows, long from, SqlExecutionCircuitBreaker circuitBreaker) {
        final int frameIndex = task.getFrameIndex();
        final long frameRowCount = task.getFrameRowCount();
        final long rowCount = rows != null ? rows.size() : frameRowCount;
        final long keptRowCount = rows != null ? rowCount : -1;
        final long tupleCap = Math.max(taskOutputMaxRows, frameRowCount) * ring.getTupleStride();
        // Most probe rows yield one tuple, so the rows left to join size the entry, which grows as
        // fan-out fills it.
        final DirectLongList tuples = ring.openTuples(frameIndex, rowCount - from);
        slot.of(tuples, task.getFrameSequence(), circuitBreaker, pairsPerCheck);
        final long position;
        try {
            position = rows != null
                    ? stepper.probeRows(slot, rows, from, isProbeDescending, tupleCap)
                    : stepper.probeRows(slot, rowCount, from, isProbeDescending, tupleCap);
        } finally {
            slot.of(null, null, null, 0);
        }
        final boolean isCancelled = position == HashJoinStageStepper.CANCELLED;
        ring.setResumePosition(frameIndex, keptRowCount, isCancelled || position == rowCount ? -1 : position);
        return isCancelled ? HashJoinStageStepper.CANCELLED : tuples.size() / ring.getTupleStride();
    }

    // Points the probe side at the orientation: the probe input's scan order and filter, and the first
    // stage's stepper.
    private void ofOrientation0(boolean isSwapped) {
        this.isSwapped = isSwapped;
        executionStages.setQuick(0, isSwapped ? swapStage : stages.getQuick(0));
        scanBySource[0] = isSwapped ? 1 : 0;
        scanBySource[1] = isSwapped ? 0 : 1;
        final HashJoinInput probeInput = isSwapped ? rightInput : leftInput;
        probeFilterContext = probeInput.getFilterContext();
        isProbeFiltered = probeInput.hasStolenFilter();
        isProbeDescending = probeInput.getOrder() == ORDER_DESC;
        final HashJoinStageSpec firstSpec = specs.getQuick(0);
        stepper = isSwapped ? firstSpec.getSwapStepper() : firstSpec.getStepper();
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
        private final ObjList<StageProbe> stageProbes = new ObjList<>();
        // The pairs left before the next check of the circuit breaker.
        long pairsUntilCheck;
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
        // stage slot's probe once, so that the stepper's call sites see one probe class.
        void bindStage(int k, HashJoinBuildStage stage, HashJoinBuildStage.Slot stageSlot) {
            final StageProbe stageProbe = stageProbes.getQuick(k);
            final FrozenHashJoinBuild.Probe probe = stageSlot.getProbe();
            if (stage.isKeyStaged()) {
                stageProbe.recordProbe = (FrozenHashJoinBuild.RecordProbe) probe;
            } else {
                stageProbe.intProbe = (FrozenHashJoinBuild.IntProbe) probe;
            }
            stageProbe.keyRecord = stageSlot.keyRecord;
            stageProbe.keySource = probeRecord;
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
