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
 * A LIMIT that bounds the rows that its query reads of the join gives the join that LIMIT, see
 * {@link #ofLimit}: the LIMIT of the join's own query, and one that reaches the join through
 * projections, sub-queries and CTEs, the master side of ASOF and LT joins, the branches of a UNION
 * ALL and runtime-constant filters. An execution under a LIMIT that reads at most so many rows, a
 * positive {@code LIMIT n} or a {@code LIMIT lo, hi} of no negative bound, first joins only the
 * probe rows that the LIMIT may need, see {@link AsyncHashJoinRecordCursor}. The LIMIT itself stays
 * with its own factory over the join. Under any other operator, which may read more of the join's
 * rows than it returns, such as a sort, a filter, a window function or another join, the join holds
 * no LIMIT.
 * <p>
 * A join whose rows are the query's own, one for one, see {@link #markClientRowSource()}, takes the
 * rows that the client reads first, see {@link SqlExecutionContext#getFirstReadRows()}, as such a
 * LIMIT: an HTTP page, a CSV export's limit or a PostgreSQL Execute's row limit then first joins
 * only the probe rows that the page may need, and a client that reads on, as a PostgreSQL portal
 * does, reads on into the full window. The rows that the client reads first reach the joins that
 * a LIMIT of the query would reach, through the same operators.
 */
public final class AsyncHashJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer REDUCER = AsyncHashJoinRecordCursorFactory::probeFrame;
    @TestOnly
    private static final AtomicInteger constructionsBeforeFailureForTesting = new AtomicInteger();
    @TestOnly
    private static volatile RuntimeException constructorFailureForTesting;
    @TestOnly
    private static volatile long lastFirstWindowRowsForTesting = -1;
    private final SCSequence collectSubSeq = new SCSequence();
    private final int workerCount;
    private UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds;
    private AsyncHashJoinRecordCursor cursor;
    private PageFrameSequence<AsyncHashJoinAtom> frameSequence;
    // True when the query's first rows need at most as many rows of the join; see
    // markClientRowSource().
    private boolean isClientRowSource;
    // The upper bound of the LIMIT of the query over the join, null when it states none.
    private Function limitHiFunction;
    // The LIMIT of the query over the join, or its lower bound, null when it states none.
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
            // The sequences take their atoms on entry, also on constructor failure. Every probe
            // frame is a task of its own, so that the dispatch limit keeps the frames in flight
            // within the tuple ring's entries; see HashJoinTupleRing.
            frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    new AsyncHashJoinAtom(configuration, spec, workerCount),
                    REDUCER,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_HASH_JOIN,
                    true
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
        long limitRows = getLimitRows(executionContext);
        if (isClientRowSource) {
            final long firstReadRows = executionContext.getFirstReadRows();
            if (firstReadRows > 0) {
                limitRows = Math.min(limitRows, firstReadRows);
            }
        }
        lastFirstWindowRowsForTesting = limitRows;
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
     * Probe frames reported through the frame sequence: zero until an execution prepares the probe's
     * dispatch, which an INNER join over an empty build never does.
     */
    @TestOnly
    public int getProbeFrameCount() {
        return frameSequence.getFrameCount();
    }

    /**
     * The rows of the execution's probe frame, as the probe scan cut it; see {@link #getProbeFrameCount()}.
     */
    @TestOnly
    public long getProbeFrameRowCount(int frameIndex) {
        return frameSequence.getFrameRowCount(frameIndex);
    }

    /**
     * The probe frames whose tasks finished since the last execution's cursor opened or went back to
     * the top, on a worker, stolen by the owner or reduced locally.
     */
    @TestOnly
    public int getReducedFrameCount() {
        return frameSequence.getReduceFinishedCounter().get();
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
     * The class of the stepper that the stage's tasks run from here on: once the factory is warmed
     * up, the stage's own copy, a hidden class that no other factory shares, and before, the shared
     * stepper template; see {@link AsyncHashJoinAtom#getStepperClass(int)}.
     */
    @TestOnly
    public Class<?> getStepperClass(int stage) {
        return getAtom().getStepperClass(stage);
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
        failure = Misc.freeBestEffort(failure, limitHiFunction);
        limitHiFunction = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * False once the cursor's execution was cancelled: by its close(), by a check of the circuit
     * breaker or by a task's failure; true while it runs. It stays false after the close(), the
     * tasks that workers ran included, until the next execution or toTop() starts.
     */
    @TestOnly
    public boolean isExecutionActive() {
        return frameSequence.isActive();
    }

    /**
     * True once the planner marked the join as the source of the query's rows, see
     * {@link #markClientRowSource()}.
     */
    @TestOnly
    public boolean isClientRowSource() {
        return isClientRowSource;
    }

    /**
     * Marks the join as a source of the query's rows: the planner found it under operators that
     * read at most one row of it per row they return, those through which a LIMIT of the query
     * reaches it too, such as projections, the master side of an ASOF or LT join or a branch of a
     * UNION ALL, so the first rows that the client reads need at most as many rows of the join.
     * Each execution then takes the rows that the client reads first, see
     * {@link SqlExecutionContext#getFirstReadRows()}, as the rows its first window may need, or the
     * LIMIT's rows when fewer; see {@link AsyncHashJoinRecordCursor}. Every operator on the way
     * opens the join's cursor as it opens its own, while the hint holds. The join of any other
     * query, under a sort, a GROUP BY, a filter or a LIMIT, for example, may yield more rows than
     * the client reads, and never reads the hint.
     */
    public void markClientRowSource() {
        isClientRowSource = true;
    }

    /**
     * Takes the LIMIT of a query over the join, which the factory then owns, as
     * {@code AsyncFilteredRecordCursorFactory} takes the LIMIT of a query over its filter. The
     * planner hands it over from the model that states the LIMIT, once that model's SELECT list
     * compiled, so that the LIMIT reads its bind variables as the SELECT list typed them, to every
     * join whose rows the LIMIT bounds: each operator between the LIMIT and the join reads at most
     * one row of the join per row it returns. Each execution reads the LIMIT once: one that reads at
     * most so many of the join's rows narrows the execution's first window to them, see
     * {@link AsyncHashJoinRecordCursor}, and any other leaves the execution as it is. The LIMIT
     * itself stays with its own factory, so the join returns its rows whatever the LIMIT reads. A
     * LIMIT of two bounds hands its upper one over next, see {@link #ofLimitHi}. A join takes at most
     * one LIMIT: a LIMIT of a query nearer the join keeps its own factory between the join and any
     * LIMIT above it.
     *
     * @param limitLoFunction the LIMIT, or its lower bound, which reads 0 when the LIMIT states
     *                        only its upper one
     */
    public void ofLimit(Function limitLoFunction) {
        assert this.limitLoFunction == null;
        this.limitLoFunction = limitLoFunction;
    }

    /**
     * Takes the upper bound of the LIMIT whose lower bound {@link #ofLimit} took, which the factory
     * then owns; null for a LIMIT of one bound.
     */
    public void ofLimitHi(@Nullable Function limitHiFunction) {
        assert this.limitLoFunction != null && this.limitHiFunction == null;
        this.limitHiFunction = limitHiFunction;
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
     * Makes the stages of the factories that define their stepper copies from here on, as they warm
     * up, take instances of the shared stepper template rather than hidden copies of it, as a JVM that
     * cannot define hidden classes does; coverage tools, which do not see hidden classes, then see the
     * template's code run.
     */
    @TestOnly
    public static void setStepperCopiesEnabledForTesting(boolean isEnabled) {
        HashJoinStageStepper.setCopiesEnabled(isEnabled);
    }

    /**
     * Runs the hook before a factory defines each of its stepper copies, the stages' in stage order
     * and then the swapped first stage's, on the thread that defines them: the owner as it collects
     * the frame that completes the factory's warm-up, or the compiling thread when there is no
     * warm-up. A test thus fails a definition with what the hook throws, or holds the owner inside
     * the definition while workers start tasks; null removes it.
     */
    @TestOnly
    public static void setStepperCopyHookForTesting(@Nullable Runnable hook) {
        HashJoinStageStepper.setNewInstanceHook(hook);
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
     * The rows that the first window of the last execution of any join since the last call was cut
     * for, Long.MAX_VALUE when nothing narrowed it, and -1 when no join executed; the next call
     * returns -1 until a join executes. The rows are its LIMIT's, or the rows that the client reads
     * first, when fewer. Tests that drive a join through a protocol read it, since they cannot reach
     * the factory.
     */
    @TestOnly
    public static long takeLastFirstWindowRowsForTesting() {
        final long rows = lastFirstWindowRowsForTesting;
        lastFirstWindowRowsForTesting = -1;
        return rows;
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

    // The most rows of the join that the query's LIMIT reads in an execution, Long.MAX_VALUE when it
    // cannot tell. LIMIT n reads n rows when n is positive; a NULL, zero or negative one leaves the
    // execution as it is. LIMIT lo, hi returns the rows from lo to hi, its bounds swapped when lo is
    // the larger, see LimitRecordCursorFactory, so it reads at most the larger bound when neither is
    // negative; a negative bound, NULL included, counts rows from the end, which takes them all.
    private long getLimitRows(SqlExecutionContext executionContext) throws SqlException {
        if (limitLoFunction == null) {
            return Long.MAX_VALUE;
        }
        limitLoFunction.init(null, executionContext);
        long limitRows = limitLoFunction.getLong(null);
        if (limitHiFunction != null) {
            limitHiFunction.init(null, executionContext);
            final long hi = limitHiFunction.getLong(null);
            limitRows = limitRows < 0 || hi < 0 ? -1 : Math.max(limitRows, hi);
        }
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
        failure = Misc.freeBestEffort(failure, limitHiFunction);
        limitHiFunction = null;
        // The atoms borrowed the spec's inputs, which go last.
        failure = Misc.freeBestEffort(failure, spec);
        spec = null;
        CairoException.rethrowCleanupFailure(failure);
    }
}
