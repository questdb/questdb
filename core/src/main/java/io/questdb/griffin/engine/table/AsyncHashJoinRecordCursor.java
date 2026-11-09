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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;

/**
 * The consumer of a parallel hash join: it builds the stages on first demand for a row, dispatches
 * the probe frames' tasks through the ordered frame sequence and reads the joined tuples that each
 * task wrote, frame by frame, in the order of the probe scan. A tuple names its probe row by its
 * index in the frame, which the consumer reads through the task's frame memory, and its stage's
 * match by an entry: a locator, which positions the stage's output payload reader at the matched
 * build row, a LEFT join's miss, which reads the stage's columns as typed NULLs, or the head of a
 * chain of several matches, which the consumer walks itself, in chain order, with the stage's
 * output probe; see {@link HashJoinStageStepper}. The consumer hashes nothing and evaluates no key.
 * <p>
 * The cursor passes every {@code next()} the execution's effective dispatch limit, the size of the
 * atom's tuple ring, which keeps frames in flight from sharing an entry; see
 * {@link HashJoinTupleRing}. {@code toTop()} probes again over the same builds.
 */
class AsyncHashJoinRecordCursor implements NoRandomAccessRecordCursor {
    // The columns of the left input, which come first in the joined row.
    private final int columnSplit;
    // For each column of the left input, the probe frame column that holds it.
    private final IntList leftColumnMap;
    // For each payload column of the first stage, the build frame column that holds it.
    private final IntList payloadColumns;
    private final PageFrameMemoryRecord probeRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    private final MultiHashJoinRecord record;
    private boolean allFramesActive;
    private AsyncHashJoinAtom atom;
    private UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds;
    // The output probe while the cursor walks a chain of matches; null otherwise.
    private FrozenHashJoinBuild.Probe chainProbe;
    private SqlExecutionCircuitBreaker circuitBreaker;
    // The current frame's task, -1 when the cursor holds none.
    private long cursor = -1;
    private int dispatchLimit;
    private int frameIndex;
    // The last frame's index once the cursor prepared the dispatch, -1 before.
    private int frameLimit;
    private PageFrameSequence<AsyncHashJoinAtom> frameSequence;
    private boolean isBuilt;
    private boolean isOpen;
    // The first stage's output probe and the payload reader it positions.
    private FrozenHashJoinBuild.Probe outputProbe;
    private HashJoinPayloadSource.Reader outputReader;
    private long tupleCount;
    private long tupleIndex;
    private int tupleStride;
    private DirectLongList tuples;

    AsyncHashJoinRecordCursor(AsyncHashJoinSpec spec) {
        final HashJoinInput leftInput = spec.getLeftInput();
        assert spec.getStageCount() == 1;
        final HashJoinStageSpec stage = spec.getStage(0);
        this.leftColumnMap = leftInput.getColumnMap();
        this.columnSplit = leftColumnMap.size();
        this.payloadColumns = stage.getPayloadColumns();
        final int columnCount = spec.getMetadata().getColumnCount();
        final IntList columnSources = new IntList(columnCount);
        final IntList columnIndexes = new IntList(columnCount);
        for (int c = 0; c < columnSplit; c++) {
            columnSources.add(0);
            columnIndexes.add(leftColumnMap.getQuick(c));
        }
        // The payload reader reads its payload columns by their payload index.
        for (int c = columnSplit; c < columnCount; c++) {
            columnSources.add(1);
            columnIndexes.add(c - columnSplit);
        }
        final ObjList<ColumnTypes> payloadTypes = new ObjList<>();
        payloadTypes.add(stage.getBuildInput().getMetadata());
        this.record = new MultiHashJoinRecord(columnSources, columnIndexes, payloadTypes, null, null, null);
        record.setProbeRecord(probeRecord);
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            try {
                collectCursor(true);
                if (frameLimit > -1) {
                    // The rows the consumer did not read are not needed: the frames in flight that no
                    // worker started are dropped rather than joined.
                    frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                    frameSequence.await();
                }
            } finally {
                try {
                    // The sequence's reset runs the atom's clear(), which closes the builds, their
                    // readers and the build scans, and then the sequence closes the probe scan.
                    frameSequence.reset();
                } finally {
                    buildRounds.reset();
                    probeRecord.of(null);
                    chainProbe = null;
                    outputProbe = null;
                    outputReader = null;
                    tuples = null;
                    circuitBreaker = null;
                }
            }
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        if (columnIndex < columnSplit) {
            return frameSequence.getSymbolTableSource().getSymbolTable(leftColumnMap.getQuick(columnIndex));
        }
        return atom.getStage(0).getBuildSymbols().getSymbolTable(payloadColumns.getQuick(columnIndex - columnSplit));
    }

    @Override
    public boolean hasNext() {
        if (!isBuilt) {
            build();
        }
        if (chainProbe != null) {
            if (chainProbe.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                chainProbe.next();
                return true;
            }
            chainProbe = null;
        }
        if (tupleIndex < tupleCount) {
            positionTuple();
            return true;
        }
        collectCursor(false);
        if (frameIndex < frameLimit) {
            fetchNextFrame();
            if (tupleIndex < tupleCount) {
                positionTuple();
                return true;
            }
        }
        if (!allFramesActive) {
            throw frameSequence.buildInterruptionException();
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        if (columnIndex < columnSplit) {
            return frameSequence.getSymbolTableSource().newSymbolTable(leftColumnMap.getQuick(columnIndex));
        }
        return atom.getStage(0).getBuildSymbols().newSymbolTable(payloadColumns.getQuick(columnIndex - columnSplit));
    }

    @Override
    public long preComputedStateSize() {
        return isBuilt ? 1 : 0;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        collectCursor(false);
        frameSequence.toTop();
        resetFrames();
    }

    private void build() {
        try {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            // The builds complete on this thread before the first probe task is published, which is
            // what publishes them to the workers.
            // An INNER join whose build has no row returns none, so the execution dispatches no probe
            // task: with no frame to read, the cursor ends at once.
            if (atom.build(circuitBreaker, buildRounds)) {
                frameSequence.prepareForDispatch();
                frameLimit = frameSequence.getFrameCount() - 1;
                long probeRows = 0;
                for (int i = 0, n = frameSequence.getFrameCount(); i < n; i++) {
                    probeRows += frameSequence.getFrameRowCount(i);
                }
                atom.maybeCopyPayload(probeRows, circuitBreaker, buildRounds);
                final HashJoinTupleRing ring = atom.getRing();
                ring.of(frameSequence);
                dispatchLimit = ring.getDispatchLimit();
                tupleStride = ring.getTupleStride();
                outputProbe = atom.getStage(0).getOutputSlot().getProbe();
                outputReader = (HashJoinPayloadSource.Reader) outputProbe.getRecord();
                record.setStageRecord(0, outputReader);
            }
            isBuilt = true;
        } catch (Throwable th) {
            // The cursor closes here, and with it the builds, so that a failed build holds no memory.
            Misc.free(this, th);
            throw th;
        }
    }

    private void collectCursor(boolean forceCollect) {
        if (cursor > -1) {
            frameSequence.collect(cursor, forceCollect);
            cursor = -1;
            // The record reads the task's frame memory, which the task releases on collection.
            probeRecord.clear();
            tuples = null;
        }
    }

    private void fetchNextFrame() {
        do {
            cursor = frameSequence.next(dispatchLimit, false);
            if (cursor > -1) {
                final PageFrameReduceTask task = frameSequence.getTask(cursor);
                if (task.hasError()) {
                    throw task.buildError();
                }
                allFramesActive &= frameSequence.isActive();
                frameIndex = task.getFrameIndex();
                tupleIndex = 0;
                tupleCount = task.getFilteredRowCount();
                if (tupleCount > 0 && frameSequence.isActive()) {
                    tuples = atom.getRing().getTuples(frameIndex);
                    probeRecord.init(task.getFrameMemory());
                    return;
                }
                // A cancelled or failed sequence's frames hold nothing to read.
                tupleCount = 0;
                collectCursor(false);
            } else {
                // next() returns -2 for a sequence without frames, which the cursor never asks, and
                // for one that the fiber dispatcher stopped without an error once every frame it
                // dispatched was collected, which only close() does here.
                assert cursor == -1 : "the probe's frame sequence stopped dispatching";
                Os.pause();
            }
        } while (frameIndex < frameLimit);
    }

    // Positions the record at the next tuple: the probe row, and the first stage's match.
    private void positionTuple() {
        final long base = tupleIndex++ * tupleStride;
        probeRecord.setRowIndex(tuples.get(base));
        final long entry = tuples.get(base + 1);
        if (entry > HashJoinStageStepper.NO_MATCH) {
            outputReader.positionLocator(entry);
            record.setHasMatch(0, true);
        } else if (entry == HashJoinStageStepper.NO_MATCH) {
            record.setHasMatch(0, false);
        } else {
            // A key of several matches: the worker left its chain to the consumer, which walks it
            // in chain order, the order the serial join returns the matches in.
            outputProbe.ofChain(HashJoinStageStepper.toChainHead(entry));
            outputProbe.next();
            record.setHasMatch(0, true);
            chainProbe = outputProbe;
        }
    }

    private void resetFrames() {
        frameIndex = -1;
        tupleIndex = 0;
        tupleCount = 0;
        chainProbe = null;
        allFramesActive = true;
    }

    /**
     * Binds the cursor to an execution whose frame sequence the factory has bound; the cursor
     * builds on the first call to {@link #hasNext()}.
     */
    void of(
            PageFrameSequence<AsyncHashJoinAtom> frameSequence,
            UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        this.frameSequence = frameSequence;
        this.buildRounds = buildRounds;
        this.atom = frameSequence.getAtom();
        this.circuitBreaker = circuitBreaker;
        probeRecord.of(frameSequence.getSymbolTableSource());
        isBuilt = false;
        frameLimit = -1;
        resetFrames();
    }

    /**
     * Marks the cursor open, so that {@link #close()} releases what the factory binds next, also
     * when the binding fails.
     */
    void open(PageFrameSequence<AsyncHashJoinAtom> frameSequence, UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds) {
        this.frameSequence = frameSequence;
        this.buildRounds = buildRounds;
        cursor = -1;
        frameLimit = -1;
        isOpen = true;
    }
}
