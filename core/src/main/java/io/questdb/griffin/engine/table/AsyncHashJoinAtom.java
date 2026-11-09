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
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * The execution state of a parallel hash join's probe: every stage's build, the slots that join
 * probe frames, one per worker and the owner's, and the ring of joined tuples that the frames'
 * tasks write and the consumer reads; see {@link HashJoinTupleRing}. The atom owns the stages and
 * borrows the inputs, their filter contexts and the stages' steppers from the join's spec.
 * <p>
 * {@link #init(SymbolTableSource, SqlExecutionContext)} runs when the cursor opens and reads no row:
 * the first stage adopts the build scan that the factory opened, see {@link #ofBuildCursor}, and
 * initializes its filters, and the slots bind to the probe scan. {@link #build} runs once per
 * execution, when the cursor is first asked for a row: every stage builds, on the owner or in rounds
 * of the join's build sequence, binds its SYMBOL key translation and hands every slot its probe of
 * the build. The owner dispatches the probe tasks after the build returns, and the first task that
 * the frame sequence publishes is what publishes the builds and the slots' probes to the workers.
 * <p>
 * A probe task joins one frame, see {@link #probeFrame}: for each probe row, in the probe scan's
 * order, the stage's stepper looks the row's key up and appends the row and the stage's entry to the
 * frame's ring entry, which the consumer reads in frame order. {@link #clear()} requires every task
 * to have finished and the consumer to be done with the tuples and the payload readers.
 */
public final class AsyncHashJoinAtom implements StatefulAtom, PerWorkerLockOwner {
    private final boolean isProbeDescending;
    private final PerWorkerLocks perWorkerLocks;
    private final HashJoinTupleRing ring;
    private final ObjList<Slot> slots = new ObjList<>();
    // The stages as compiled, which the atom borrows for their join types and steppers.
    private final ObjList<HashJoinStageSpec> specs = new ObjList<>();
    private final ObjList<HashJoinBuildStage> stages = new ObjList<>();
    private final int workerCount;
    // The first stage's build scan, which the factory opened before the probe scan, until the stage
    // adopts it in init(); null otherwise.
    private PageFrameCursor pendingBuildCursor;

    /**
     * Borrows the spec's inputs, filter contexts and steppers. The key layouts borrow the inputs'
     * metadata, so construct the atom while the inputs are open. Nothing here allocates native
     * memory, which the stages, the slots and the ring take only when an execution opens them, so a
     * failure leaves nothing to release.
     */
    AsyncHashJoinAtom(CairoConfiguration configuration, AsyncHashJoinSpec spec, int workerCount) {
        this.workerCount = workerCount;
        final int stageCount = spec.getStageCount();
        this.isProbeDescending = spec.getLeftInput().getOrder() == ORDER_DESC;
        this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        this.ring = new HashJoinTupleRing(configuration, stageCount);
        final int probeColumnCount = spec.getLeftInput().getMetadata().getColumnCount();
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
            specs.add(stageSpec);
            stageSpec.setPayloadCopyable(stage.isPayloadCopyable());
            final int keyKind = keyLayout.isKeyStaged()
                    ? HashJoinStageStepper.KEY_STAGED
                    : keyLayout.isSymbolKey() ? HashJoinStageStepper.KEY_SYMBOL : HashJoinStageStepper.KEY_INT;
            stageSpec.getStepper().of(keyKind, keyLayout.isKeyStaged() ? -1 : keyLayout.getProbeKeyColumn(), stageSpec.isOuter());
        }
        for (int i = -1; i < workerCount; i++) {
            final Slot slot = new Slot(stageCount);
            for (int k = 0; k < stageCount; k++) {
                // The slot's staged probe keys read its probe record.
                stages.getQuick(k).getSlot(i).ofKeySource(slot.probeRecord);
            }
            slots.add(slot);
        }
    }

    @Override
    public void clear() {
        Throwable failure = Misc.freeBestEffort(null, pendingBuildCursor);
        pendingBuildCursor = null;
        failure = Misc.clearBestEffort(failure, ring);
        // The slots let go of the probes and the probe scan's symbol tables before the stages close
        // the builds, and the frame sequence closes the probe scan after the atom.
        failure = Misc.clearObjListBestEffort(failure, slots);
        failure = Misc.clearObjListBestEffort(failure, stages);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Throwable failure = Misc.clearBestEffort(null, this);
        failure = Misc.freeObjListBestEffort(failure, slots);
        slots.clear();
        failure = Misc.freeObjListBestEffort(failure, stages);
        stages.clear();
        failure = Misc.freeBestEffort(failure, ring);
        CairoException.rethrowCleanupFailure(failure);
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
     * The runtime state of the k-th join: its build, its payload readers and its slots' probes.
     */
    public HashJoinBuildStage getStage(int stage) {
        return stages.getQuick(stage);
    }

    /**
     * Adopts the first stage's build scan and binds every slot to the probe scan, and reads no row of
     * either: {@link #build} does the work that grows with the builds, when the cursor is first asked
     * for a row. Everything that can throw a SqlException runs here, so that it surfaces from
     * getCursor(). On failure the factory closes the cursor, which resets the frame sequence, whose
     * reset runs {@link #clear()}: it releases what this took, the build scan included.
     *
     * @param symbolTableSource the open probe scan
     */
    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        final PageFrameCursor buildCursor = pendingBuildCursor;
        pendingBuildCursor = null;
        // The stage owns the build scan from here on, whether its init returns or throws.
        stages.getQuick(0).init(symbolTableSource, executionContext, buildCursor);
        for (int i = 0, n = slots.size(); i < n; i++) {
            slots.getQuick(i).probeRecord.of(symbolTableSource);
        }
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
     * Builds every stage and binds every slot to the builds. The cursor calls it once per execution,
     * on first demand for a row, on the thread that then dispatches the probes and before it
     * dispatches any; nothing here runs when the cursor merely opens. Returns false when an INNER
     * join's build has no row, so that the join returns no row and needs no probe task. On failure
     * the caller closes the cursor, whose clear() closes the builds.
     *
     * @param buildRounds the task-only sequence that runs the rounds of a stage's parallel build
     */
    boolean build(SqlExecutionCircuitBreaker circuitBreaker, UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds) {
        boolean hasRows = true;
        final HashJoinBuildRoundAtom roundAtom = buildRounds.getAtom();
        for (int k = 0, n = stages.size(); k < n; k++) {
            final HashJoinBuildStage stage = stages.getQuick(k);
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
        for (int k = 0, n = stages.size(); k < n; k++) {
            final HashJoinBuildStage stage = stages.getQuick(k);
            roundAtom.setBuildingStage(stage);
            stage.maybeCopyPayload(probeRows, circuitBreaker, buildRounds);
        }
    }

    /**
     * Takes the first stage's build scan, which the factory opened before the probe scan, until
     * {@link #init} hands it to the stage; {@link #clear()} closes a scan that no stage adopted.
     */
    void ofBuildCursor(PageFrameCursor buildCursor) {
        assert pendingBuildCursor == null;
        pendingBuildCursor = buildCursor;
    }

    /**
     * Joins one probe frame on a slot: reads the frame through the task's memory, so that the
     * consumer reads the probe rows where the task read them, and writes the frame's tuples into its
     * ring entry, in the probe scan's row order. The tuples' count goes to the task.
     */
    void probeFrame(int slotId, PageFrameReduceTask task) {
        final Slot slot = getSlot(slotId);
        slot.probeRecord.init(task.populateFrameMemory());
        final int frameIndex = task.getFrameIndex();
        final long rowCount = task.getFrameRowCount();
        // A probe row yields at most one tuple, so the frame's rows bound its tuples.
        final DirectLongList tuples = ring.openTuples(frameIndex, rowCount);
        slot.tuples = tuples;
        try {
            specs.getQuick(0).getStepper().probeRows(slot, rowCount, isProbeDescending);
        } finally {
            slot.tuples = null;
        }
        task.setFilteredRowCount(tuples.size() / ring.getTupleStride());
    }

    /**
     * One slot's probe-side state: the record it reads probe frames through, its bindings to every
     * stage's build, and the tuple it assembles for the current probe row.
     */
    static final class Slot implements Mutable, QuietCloseable {
        // The current probe row's entry for each stage; see HashJoinStageStepper.
        final long[] entries;
        final PageFrameMemoryRecord probeRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        private final ObjList<StageProbe> stageProbes = new ObjList<>();
        // The current probe row, by its index in the frame.
        long probeRow;
        // The ring entry of the frame the slot joins; null between frames.
        DirectLongList tuples;

        Slot(int stageCount) {
            entries = new long[stageCount];
            for (int k = 0; k < stageCount; k++) {
                stageProbes.add(new StageProbe());
            }
        }

        @Override
        public void clear() {
            Misc.clearObjList(stageProbes);
            // The probe scan's symbol tables go here, before the frame sequence closes the scan.
            probeRecord.of(null);
            tuples = null;
        }

        @Override
        public void close() {
            clear();
            probeRecord.close();
        }

        StageProbe getStageProbe(int stage) {
            return stageProbes.getQuick(stage);
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
        }
    }

    /**
     * A slot's binding to one stage's build for the open cursor's execution: its probe, which
     * positions its payload reader on each match, and what the probe's key reads.
     */
    static final class StageProbe implements Mutable {
        FrozenHashJoinBuild.IntProbe intProbe;
        // True when the build holds one row per key, so that a lookup positions the payload itself.
        boolean isBuildUnique;
        // What a staged key's sink reads: the key source, or a view that translates its SYMBOL keys.
        Record keyRecord;
        // The record whose columns the stage's key reads.
        Record keySource;
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
    }
}
