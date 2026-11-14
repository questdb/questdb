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

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.mp.SCSequence;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An INNER or LEFT OUTER equi hash join that runs on the shared query workers: the build input is
 * hashed, in parallel when it is large, and the probe input's page frames are joined on the workers,
 * one task per frame, while the consumer reads the joined rows in the probe scan's order. The rows,
 * their order, their types and their symbol tables are those of the serial hash join: the serial
 * join's master columns come first and then every column of its slave, the probe rows come in the
 * order the left input's cursor yields them and each probe row's matches come newest first, in the
 * order the build input's cursor yields them, as the serial join's chains list them.
 * <p>
 * The later joins of a star query, whose keys read the left input's columns alone, run as later
 * stages of the same operator, see {@link AsyncHashJoinSpec#addStage}: a probe task joins each of a
 * probe row's matches of one stage with the next stage, so the rows come in the order of the serial
 * joins' nested loops, and every stage builds, in stage order, before the first probe task runs. A
 * stack whose first join swaps its inputs in an execution, as described below, streams the first
 * join's right input, and its later stages read their keys from each of the first stage's matches,
 * the left input's rows, as the serial later joins read them from the swapped first join's rows.
 * <p>
 * The factory owns the join's spec, see {@link AsyncHashJoinSpec}: the inputs, their filter
 * contexts, the stages and the joined rows' metadata. It answers the order questions a parent asks a
 * join, {@link #followedOrderByAdvice()} and {@link #getScanDirection()}, as its left input as
 * compiled answered them, and reports no random access, as the serial join does. An answer that
 * constrains the order fixes it, as it fixes the serial join's.
 * <p>
 * {@link #getCursor} opens the first join's right input, then its left input, each in its own scan
 * order, and reads no row: the cursor builds on first demand for a row, so EXPLAIN and
 * {@code LIMIT 0} pay for no more than the two open scans. An INNER join whose order no parent fixed
 * swaps its inputs, as the serial hash join does, when both report a size and the left one is the
 * smaller: it then hashes the left input and streams the right one, whose rows come in the right
 * input's order, each with its matches newest first, in the order the left input's cursor yields
 * them. The joined rows keep their columns, the left input's first. The sizes are those that the
 * inputs' record cursors would report, see {@link HashJoinInput#getSerialSize}, and every execution
 * of a cached factory decides again, from the sizes of its own open scans.
 * <p>
 * A query whose LIMIT applies to the join's rows gives the join that LIMIT, see {@link #ofLimit}:
 * an execution under a positive LIMIT first joins only the probe rows that the LIMIT may need, see
 * {@link AsyncHashJoinRecordCursor}. The LIMIT itself stays with its own factory over the join.
 */
public final class AsyncHashJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer REDUCER = AsyncHashJoinRecordCursorFactory::probeFrame;
    @TestOnly
    private static final AtomicInteger constructionsBeforeFailureForTesting = new AtomicInteger();
    @TestOnly
    private static volatile RuntimeException constructorFailureForTesting;
    private final SCSequence collectSubSeq = new SCSequence();
    private final int workerCount;
    private UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds;
    private AsyncHashJoinRecordCursor cursor;
    private PageFrameSequence<AsyncHashJoinAtom> frameSequence;
    // The LIMIT that the optimiser advised the join's model, until the planner hands it over, see
    // ofLimit(); a planning-time reference to a pooled node, which the factory does not own.
    private ExpressionNode limitAdvice;
    // The LIMIT of the query over the join, null when it states none.
    private Function limitLoFunction;
    private AsyncHashJoinSpec spec;

    /**
     * Takes ownership of the spec once the constructor returns; on failure the spec stays with the
     * caller, and the constructor releases only what it created.
     */
    public AsyncHashJoinRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull AsyncHashJoinSpec spec,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(spec.getMetadata());
        PageFrameSequence<AsyncHashJoinAtom> frameSequence = null;
        UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds = null;
        try {
            // The sequences take their atoms on entry, also on constructor failure.
            frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    new AsyncHashJoinAtom(configuration, spec, workerCount),
                    REDUCER,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_HASH_JOIN
            );
            // A sequence that runs the builds' rounds alone binds no frames and runs no frame task.
            buildRounds = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    new HashJoinBuildRoundAtom(configuration, workerCount),
                    null,
                    workerCount
            );
            final RuntimeException constructorFailure = constructorFailureForTesting;
            if (constructorFailure != null && constructionsBeforeFailureForTesting.getAndDecrement() <= 0) {
                throw constructorFailure;
            }
            cursor = new AsyncHashJoinRecordCursor(spec);
        } catch (Throwable th) {
            Misc.free(buildRounds, th);
            Misc.free(frameSequence, th);
            throw th;
        }
        this.frameSequence = frameSequence;
        this.buildRounds = buildRounds;
        this.workerCount = workerCount;
        // Assigned last: _close() frees the spec, which the caller frees on failure.
        this.spec = spec;
    }

    /**
     * Answers as the left input as compiled answered, as the serial join asks its master, unless a
     * stage runs a post-join filter: the serial plan's filter over a join answers false without
     * asking the join, and so does this join once it absorbed such a filter. An advice that the left
     * input followed fixes the join's order, which rules the swap of its inputs out, as it does the
     * serial join's.
     */
    @Override
    public boolean followedOrderByAdvice() {
        if (spec.hasPostFilter()) {
            return false;
        }
        final boolean isFollowed = spec.isLeftOrderByAdviceFollowed();
        spec.markOrderDetermined(isFollowed);
        return isFollowed;
    }

    @Override
    @TestOnly
    public AsyncHashJoinAtom getAtom() {
        return frameSequence.getAtom();
    }

    /**
     * The atom of the sequence that runs the rounds of the stages' parallel builds.
     */
    @TestOnly
    public HashJoinBuildRoundAtom getBuildRoundAtom() {
        return buildRounds.getAtom();
    }

    /**
     * The frames whose count-only tasks the last execution's row counts collected, since its cursor
     * opened; see {@code RecordCursor.calculateSize()}.
     */
    @TestOnly
    public int getCountOnlyFrameCount() {
        return cursor.getCountOnlyFrameCount();
    }

    /**
     * The continuations of frames that the last execution's row counts counted on the owner rather
     * than joined, since its cursor opened.
     */
    @TestOnly
    public int getCountedContinuationCount() {
        return cursor.getCountedContinuationCount();
    }

    /**
     * The frames of tuple-carrying tasks, dispatched before a row count began, whose rest the last
     * execution's row counts counted, since its cursor opened.
     */
    @TestOnly
    public int getCountedTupleFrameCount() {
        return cursor.getCountedTupleFrameCount();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.close();
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        final long limitRows = getLimitRows(executionContext);
        cursor.open(frameSequence, buildRounds);
        try {
            // Consult the breaker at open, so a join over empty inputs still observes cancellation.
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            buildRounds.ofRounds(executionContext);
            final AsyncHashJoinAtom atom = frameSequence.getAtom();
            // The right input's scan opens first, as the serial join opens its slave first. The atom
            // keeps both scans until it hands them on, and the cursor's close() closes them otherwise.
            final HashJoinInput rightInput = spec.getStage(0).getBuildInput();
            final HashJoinInput leftInput = spec.getLeftInput();
            final PageFrameCursor rightCursor = rightInput.getFactory().getPageFrameCursor(executionContext, rightInput.getOrder());
            PageFrameCursor leftCursor = null;
            try {
                leftCursor = leftInput.getFactory().getPageFrameCursor(executionContext, leftInput.getOrder());
            } finally {
                atom.ofInputCursors(leftCursor, rightCursor);
            }
            // The serial join swaps its inputs by the sizes their record cursors report, once both
            // cursors are open, unless a parent fixed its order.
            final boolean isSwapped = spec.isInputSwapPossible()
                    && isLeftInputSmaller(leftInput.getSerialSize(leftCursor), rightInput.getSerialSize(rightCursor));
            final HashJoinInput probeInput = isSwapped ? rightInput : leftInput;
            final PageFrameCursor probeCursor = atom.ofOrientation(isSwapped);
            // The sequence owns the probe scan from here on, whether this returns or throws.
            frameSequence.of(probeCursor, probeInput.getMetadata(), executionContext, collectSubSeq);
            cursor.of(frameSequence, buildRounds, circuitBreaker, limitRows);
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor, th);
            throw th;
        }
    }

    /**
     * The probe frames that the frame sequence handed out, to the workers or to the owner, since the
     * last execution's cursor opened or went back to the top.
     */
    @TestOnly
    public int getDispatchedFrameCount() {
        return frameSequence.getDispatchedFrameCount();
    }

    /**
     * The lo-only LIMIT that the optimiser advised the join's model, as {@link #ofLimitAdvice} took
     * it, or null once {@link #ofLimit} took the LIMIT.
     */
    @Nullable
    public ExpressionNode getLimitAdvice() {
        return limitAdvice;
    }

    /**
     * Probe frames reported through the frame sequence: zero until an execution prepares the probe's
     * dispatch, which an INNER join over an empty build never does.
     */
    @TestOnly
    public int getProbeFrameCount() {
        return frameSequence.getFrameCount();
    }

    /**
     * Answers as the left input as compiled answered, as the serial join asks its master. A scan
     * direction other than {@code SCAN_DIRECTION_OTHER} fixes the join's order, which rules the swap
     * of its inputs out, as it does the serial join's.
     */
    @Override
    public int getScanDirection() {
        final int scanDirection = spec.getLeftScanDirection();
        spec.markOrderDetermined(scanDirection != SCAN_DIRECTION_OTHER);
        return scanDirection;
    }

    /**
     * The join's spec, which the factory owns; the planner reads it to decide whether a later join
     * can become a stage of this one.
     */
    public AsyncHashJoinSpec getSpec() {
        return spec;
    }

    /**
     * The class of the stage's stepper, a hidden copy of the stepper template that no other factory
     * shares.
     */
    @TestOnly
    public Class<?> getStepperClass(int stage) {
        return spec.getStage(stage).getStepper().getClass();
    }

    /**
     * Frees what the factory's executions use, its cursor, its frame sequences and their atoms, and
     * keeps the spec, which a caller took with {@link #stealSpec()} to build another join from:
     * one that absorbs a post-join filter into its last stage, or a later join as a stage of its own.
     */
    @Override
    public void halfClose() {
        Throwable failure = Misc.freeBestEffort(null, cursor);
        cursor = null;
        failure = Misc.freeBestEffort(failure, frameSequence);
        frameSequence = null;
        failure = Misc.freeBestEffort(failure, buildRounds);
        buildRounds = null;
        // The LIMIT belongs to this join's rows, not to the spec's.
        failure = Misc.freeBestEffort(failure, limitLoFunction);
        limitLoFunction = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Takes the LIMIT of the query over the join, which the factory then owns, as
     * {@code AsyncFilteredRecordCursorFactory} takes the LIMIT of a query over its filter. The
     * planner hands it over from the model that states the LIMIT, once that model's SELECT list
     * compiled, to the join that the optimiser advised that LIMIT, see {@link #ofLimitAdvice}, and
     * the advice is then spent. Each execution reads the LIMIT once; a positive LIMIT narrows the
     * execution's first window, see {@link AsyncHashJoinRecordCursor}, and any other leaves the
     * execution as it is. The LIMIT itself stays with its own factory, so the join returns its rows
     * whatever the LIMIT reads.
     */
    public void ofLimit(@Nullable Function limitLoFunction) {
        assert this.limitLoFunction == null;
        this.limitLoFunction = limitLoFunction;
        limitAdvice = null;
    }

    /**
     * Takes the lo-only LIMIT that the optimiser advised the join's model, once the join is final:
     * the LIMIT of the model that states it, which applies to the join's rows. The planner hands the
     * LIMIT over, see {@link #ofLimit}, only when it parses the LIMIT of that very model, after its
     * SELECT list, so that the LIMIT reads its bind variables as the SELECT list typed them.
     */
    public void ofLimitAdvice(@Nullable ExpressionNode limitAdvice) {
        this.limitAdvice = limitAdvice;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    /**
     * Hands the spec over to the caller, which then owns it: the factory no longer frees it. The
     * caller then half-closes the factory, see {@link #halfClose()}, while the spec's inputs are
     * open, since the atoms borrow them.
     */
    public AsyncHashJoinSpec stealSpec() {
        final AsyncHashJoinSpec spec = this.spec;
        this.spec = null;
        return spec;
    }

    @Override
    public void toPlan(PlanSink sink) {
        final HashJoinInput leftInput = spec.getLeftInput();
        // A compiled probe filter runs as JIT code, which the operator's name reports, as the
        // filters it stole the probe filter from do.
        sink.type(leftInput.isFilterCompiled() ? "Async JIT Hash Join" : "Async Hash Join");
        sink.meta("workers").val(workerCount);
        sink.attr("stages").val(spec.getStageCount());
        // This plan can print inside another operator's attribute, e.g. as a scalar sub-query, while
        // that operator has set the sink's metadata or base-metadata mode for its own columns. The
        // plan resolves the probe filter's columns against the probe scan, prints its children as at
        // the plan's root, and then gives the caller back its metadata and mode for the columns that
        // follow the sub-query.
        final RecordMetadata callerMetadata = sink.getMetadata();
        final boolean isCallerUsingBaseMetadata = sink.getUseBaseMetadata();
        sink.useBaseMetadata(false);
        try {
            sink.setMetadata(leftInput.getMetadata());
            sink.optAttr("filter", leftInput.getFilterContext().getFilter(-1));
            sink.setMetadata(null);
            sink.child("Probe", leftInput.getFactory());
            for (int k = 0, n = spec.getStageCount(); k < n; k++) {
                sink.child(spec.getStage(k));
            }
        } finally {
            sink.setMetadata(callerMetadata);
            sink.useBaseMetadata(isCallerUsingBaseMetadata);
        }
    }

    /**
     * A join reads externally if any of its inputs does; {@code getBaseFactory()} cannot say so,
     * since it names one input, and this join keeps its default of null, as the serial joins do.
     */
    @Override
    public boolean usesExternalDataSource() {
        return spec.usesExternalDataSource();
    }

    /**
     * Makes the constructor throw this failure once it created its sequences, so that a test can fail
     * a construction where it has native memory to release; null removes it.
     */
    @TestOnly
    public static void setConstructorFailureForTesting(@Nullable RuntimeException failure) {
        setConstructorFailureForTesting(failure, 0);
    }

    /**
     * Makes the constructor throw this failure, as {@link #setConstructorFailureForTesting(RuntimeException)}
     * does, from the construction after the given number of constructions on: a join that absorbs a
     * post-join filter, or a later join, is constructed once more for each.
     */
    @TestOnly
    public static void setConstructorFailureForTesting(@Nullable RuntimeException failure, int constructionsBeforeFailure) {
        constructionsBeforeFailureForTesting.set(constructionsBeforeFailure);
        constructorFailureForTesting = failure;
    }

    /**
     * Makes every stage take the shared stepper template rather than a hidden copy of it, as a JVM
     * that cannot define hidden classes does; coverage tools, which do not see hidden classes, then
     * see the template's code run.
     */
    @TestOnly
    public static void setStepperCopiesEnabledForTesting(boolean isEnabled) {
        HashJoinStageStepper.setCopiesEnabled(isEnabled);
    }

    /**
     * Sets the probe rows after which the factories that compile from here on switch from the shared
     * stepper template to their stages' hidden copies, {@link AsyncHashJoinAtom#STEPPER_WARM_UP_ROWS}
     * unless a test lowers it, as this does; 0 runs the copies from the first task.
     */
    @TestOnly
    public static void setStepperWarmUpRowsForTesting(long rows) {
        AsyncHashJoinAtom.setStepperWarmUpRows(rows);
    }

    /**
     * The serial hash join's rule: it hashes its left input and streams its right one when both
     * report a size and the left one is the smaller.
     */
    private static boolean isLeftInputSmaller(long leftSize, long rightSize) {
        return leftSize > 0 && rightSize > 0 && leftSize < rightSize;
    }

    private static void probeFrame(
            int workerId,
            @NotNull PageFrameMemoryRecord unused,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final PageFrameSequence<AsyncHashJoinAtom> sequence = task.getFrameSequence(AsyncHashJoinAtom.class);
        final AsyncHashJoinAtom atom = sequence.getAtom();
        final int slotId = atom.maybeAcquire(workerId, stealingFrameSequence == sequence, circuitBreaker);
        // The slot is held from here on, so everything below belongs inside the try that releases it:
        // reading the frame decodes Parquet and can breach the per-query memory limit, and so can the
        // frame's tuples.
        try {
            atom.probeFrame(slotId, task, circuitBreaker);
        } finally {
            atom.release(slotId);
        }
    }

    // The rows of the query's LIMIT for an execution, Long.MAX_VALUE without a positive one: a NULL,
    // zero or negative LIMIT leaves the execution as it is.
    private long getLimitRows(SqlExecutionContext executionContext) throws SqlException {
        if (limitLoFunction == null) {
            return Long.MAX_VALUE;
        }
        limitLoFunction.init(null, executionContext);
        final long limitRows = limitLoFunction.getLong(null);
        return limitRows > 0 ? limitRows : Long.MAX_VALUE;
    }

    @Override
    protected void _close() {
        Throwable failure = Misc.freeBestEffort(null, cursor);
        cursor = null;
        failure = Misc.freeBestEffort(failure, frameSequence);
        frameSequence = null;
        failure = Misc.freeBestEffort(failure, buildRounds);
        buildRounds = null;
        failure = Misc.freeBestEffort(failure, limitLoFunction);
        limitLoFunction = null;
        // The atoms borrowed the spec's inputs, which go last.
        failure = Misc.freeBestEffort(failure, spec);
        spec = null;
        CairoException.rethrowCleanupFailure(failure);
    }
}
