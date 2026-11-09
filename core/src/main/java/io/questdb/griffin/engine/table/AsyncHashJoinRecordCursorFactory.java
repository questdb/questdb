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

/**
 * An INNER or LEFT OUTER equi hash join that runs on the shared query workers: the build input is
 * hashed, in parallel when it is large, and the probe input's page frames are joined on the workers,
 * one task per frame, while the consumer reads the joined rows in the probe scan's order. The rows,
 * their order, their types and their symbol tables are those of the serial hash join: the serial
 * join's master columns come first and then every column of its slave, the probe rows come in the
 * order the left input's cursor yields them and each probe row's matches come newest first, in the
 * order the build input's cursor yields them, as the serial join's chains list them.
 * <p>
 * The factory owns the join's spec, see {@link AsyncHashJoinSpec}: the inputs, their filter
 * contexts, the stages and the joined rows' metadata. It answers the order questions a parent asks a
 * join, {@link #followedOrderByAdvice()} and {@link #getScanDirection()}, as its left input as
 * compiled answered them, and reports no random access, as the serial join does.
 * <p>
 * {@link #getCursor} opens the build scan, then the probe scan, each in its own scan order, and
 * reads no row: the cursor builds on first demand for a row, so EXPLAIN and {@code LIMIT 0} pay for
 * no more than the two open scans.
 */
public final class AsyncHashJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer REDUCER = AsyncHashJoinRecordCursorFactory::probeFrame;
    @TestOnly
    private static volatile RuntimeException constructorFailureForTesting;
    private final SCSequence collectSubSeq = new SCSequence();
    private final int workerCount;
    private UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds;
    private AsyncHashJoinRecordCursor cursor;
    private PageFrameSequence<AsyncHashJoinAtom> frameSequence;
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
            if (constructorFailure != null) {
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
     * Answers as the left input as compiled answered, as the serial join asks its master.
     */
    @Override
    public boolean followedOrderByAdvice() {
        return spec.isLeftOrderByAdviceFollowed();
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

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.close();
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        cursor.open(frameSequence, buildRounds);
        try {
            // Consult the breaker at open, so a join over empty inputs still observes cancellation.
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            buildRounds.ofRounds(executionContext);
            final AsyncHashJoinAtom atom = frameSequence.getAtom();
            // The build scan opens first, as the serial join opens its slave first. The atom keeps it
            // until the first stage adopts it, and the cursor's close() closes it otherwise.
            final HashJoinInput buildInput = spec.getStage(0).getBuildInput();
            atom.ofBuildCursor(buildInput.getFactory().getPageFrameCursor(executionContext, buildInput.getOrder()));
            final HashJoinInput leftInput = spec.getLeftInput();
            final PageFrameCursor probeCursor = leftInput.getFactory().getPageFrameCursor(executionContext, leftInput.getOrder());
            // The sequence owns the probe scan from here on, whether this returns or throws.
            frameSequence.of(probeCursor, leftInput.getMetadata(), executionContext, collectSubSeq);
            cursor.of(frameSequence, buildRounds, circuitBreaker);
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor, th);
            throw th;
        }
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
     * Answers as the left input as compiled answered, as the serial join asks its master.
     */
    @Override
    public int getScanDirection() {
        return spec.getLeftScanDirection();
    }

    /**
     * The class of the stage's stepper, a hidden copy of the stepper template that no other factory
     * shares.
     */
    @TestOnly
    public Class<?> getStepperClass(int stage) {
        return spec.getStage(stage).getStepper().getClass();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Hash Join");
        sink.meta("workers").val(workerCount);
        sink.attr("stages").val(spec.getStageCount());
        // This plan can print inside another operator's attribute, e.g. as a scalar sub-query, while
        // that operator has set the sink's metadata or base-metadata mode for its own columns. The
        // plan prints its children as at the plan's root, and then gives the caller back its
        // metadata and mode for the columns that follow the sub-query.
        final RecordMetadata callerMetadata = sink.getMetadata();
        final boolean isCallerUsingBaseMetadata = sink.getUseBaseMetadata();
        sink.useBaseMetadata(false);
        try {
            sink.setMetadata(null);
            sink.child("Probe", spec.getLeftInput().getFactory());
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
            atom.probeFrame(slotId, task);
        } finally {
            atom.release(slotId);
        }
    }

    @Override
    protected void _close() {
        Throwable failure = Misc.freeBestEffort(null, cursor);
        cursor = null;
        failure = Misc.freeBestEffort(failure, frameSequence);
        frameSequence = null;
        failure = Misc.freeBestEffort(failure, buildRounds);
        buildRounds = null;
        // The atoms borrowed the spec's inputs, which go last.
        failure = Misc.freeBestEffort(failure, spec);
        spec = null;
        CairoException.rethrowCleanupFailure(failure);
    }
}
