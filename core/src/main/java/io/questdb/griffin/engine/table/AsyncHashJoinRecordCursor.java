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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;

/**
 * The consumer of a parallel hash join: it builds the stages on first demand for a row, dispatches
 * the probe frames' tasks through the ordered frame sequence and reads the joined tuples that each
 * task wrote, frame by frame, in the order of the probe scan. A tuple names its probe row by its
 * index in the frame, which the consumer reads through the task's frame memory, and each stage's
 * match by an entry: a locator, which positions the stage's output payload reader at the matched
 * build row, a LEFT join's miss, which reads the stage's columns as typed NULLs, or the head of a
 * chain of several matches, which the consumer walks itself, in chain order, with the stage's
 * output probe; see {@link HashJoinStageStepper}. A tuple with several such chains yields every
 * combination of their matches, odometer-style, the last stage's chain fastest, which is the order
 * of the serial joins' nested loops. The consumer hashes nothing and evaluates no key.
 * <p>
 * A task that reached its cap of tuples left the rest of its frame to the consumer, which, once it
 * has read the task's tuples, joins the frame's next chunk itself, on the owner's slot, and reads
 * that, until the frame is done; see {@link AsyncHashJoinAtom#continueFrame}. Before it reads a
 * task, and before it runs a chunk, the consumer checks that the execution was not cancelled, and
 * drops the frame if it was: the interruption then surfaces once the frames in flight are collected.
 * <p>
 * The cursor passes every {@code next()} the execution's effective dispatch limit, the size of the
 * atom's tuple ring, which keeps frames in flight from sharing an entry; see
 * {@link HashJoinTupleRing}. {@code toTop()} probes again over the same builds, in the same
 * orientation.
 * <p>
 * An execution that swapped the first join's inputs, see {@link AsyncHashJoinAtom#isSwapped()}, probes
 * the right input's frames and reads the left input's columns from the first stage's matches; the
 * joined row keeps its columns, and each column's symbol tables come from the open scan whose frames
 * hold it.
 * <p>
 * {@link #calculateSize} counts the rows still to come without reading them: it dispatches
 * count-only tasks, which count each frame's joined rows on the workers, see
 * {@link AsyncHashJoinAtom#probeFrame}, and walks the chains that a tuple leaves to the consumer only
 * to count their links, which leaves the builds as they are, so that {@code toTop()} reads the rows
 * again. A count that follows some rows first counts what the tuple-carrying tasks dispatched before
 * it left: the current tuple's chains' combinations after the current one, the current frame's
 * tuples not yet read, the tuples of every such task it collects, chains included, and the rows that
 * each of those frames left to a continuation, which it counts on the owner's slot rather than
 * joins.
 */
class AsyncHashJoinRecordCursor implements NoRandomAccessRecordCursor {
    // The chain heads of the current tuple's stages that left their chains to the consumer, in
    // stage order, as many as chainStageCount.
    private final long[] chainHeads;
    // The stages of those chains, ascending.
    private final int[] chainStages;
    // Each stage's output probe, which walks its chains, and the payload reader it positions.
    private final ObjList<FrozenHashJoinBuild.Probe> outputProbes = new ObjList<>();
    private final ObjList<HashJoinPayloadSource.Reader> outputReaders = new ObjList<>();
    private final PageFrameMemoryRecord probeRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    private final MultiHashJoinRecord record;
    private final int stageCount;
    private boolean allFramesActive;
    private AsyncHashJoinAtom atom;
    private UnorderedPageFrameSequence<HashJoinBuildRoundAtom> buildRounds;
    // The chains the cursor walks for the current tuple; 0 when it walks none.
    private int chainStageCount;
    private SqlExecutionCircuitBreaker circuitBreaker;
    // What the open execution's counts met: frames of count-only tasks, continuations, and frames of
    // tuple-carrying tasks; see the factory's test accessors.
    private int countOnlyFrameCount;
    private int countedContinuationCount;
    private int countedTupleFrameCount;
    // The current frame's task, -1 when the cursor holds none.
    private long cursor = -1;
    private int dispatchLimit;
    private int frameIndex;
    // The last frame's index once the cursor prepared the dispatch, -1 before.
    private int frameLimit;
    private PageFrameSequence<AsyncHashJoinAtom> frameSequence;
    private boolean isBuilt;
    private boolean isOpen;
    private long tupleCount;
    private long tupleIndex;
    private int tupleStride;
    private DirectLongList tuples;

    AsyncHashJoinRecordCursor(AsyncHashJoinSpec spec) {
        this.stageCount = spec.getStageCount();
        this.chainHeads = new long[stageCount];
        this.chainStages = new int[stageCount];
        outputProbes.setPos(stageCount);
        outputReaders.setPos(stageCount);
        // A LEFT join's miss reads the payload columns as typed NULLs, by their payload index.
        this.record = spec.newJoinedRecord();
        record.setProbeRecord(probeRecord);
    }

    /**
     * Counts the rows that {@link #hasNext()} would still return, without reading them, and leaves
     * the cursor at its end, so that {@code toTop()} reads every row again. The builds run first if no
     * row was read yet, and stay as they are. The count checks the execution's breaker, as
     * {@link #hasNext()} does, whichever breaker the caller passes.
     */
    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (!isBuilt) {
            build();
        }
        if (chainStageCount > 0) {
            counter.add(countRemainingChainMatches());
        }
        if (cursor > -1) {
            // The rest of the frame the cursor holds, which a tuple-carrying task joined.
            countTupleFrame(counter);
        }
        while (frameIndex < frameLimit) {
            cursor = frameSequence.next(Integer.MAX_VALUE, true);
            if (cursor > -1) {
                final PageFrameReduceTask task = frameSequence.getTask(cursor);
                if (task.hasError()) {
                    throw task.buildError();
                }
                allFramesActive &= frameSequence.isActive();
                frameIndex = task.getFrameIndex();
                // A cancelled or failed sequence's frames hold nothing to count.
                if (frameSequence.isActive()) {
                    if (task.isCountOnly()) {
                        countOnlyFrameCount++;
                        counter.add(task.getFilteredRowCount());
                    } else {
                        // A task that the cursor dispatched before the count began carries tuples.
                        tuples = atom.getRing().getTuples(frameIndex);
                        tupleIndex = 0;
                        tupleCount = task.getFilteredRowCount();
                        countTupleFrame(counter);
                    }
                }
                collectCursor(false);
                this.circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            } else {
                // The sequence has frames, and only close() stops the fiber dispatcher; see
                // fetchNextFrame().
                assert cursor == -1 : "the probe's frame sequence stopped dispatching";
                Os.pause();
            }
        }
        tupleIndex = 0;
        tupleCount = 0;
        if (!allFramesActive) {
            throw frameSequence.buildInterruptionException();
        }
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
                    chainStageCount = 0;
                    outputProbes.setAll(stageCount, null);
                    outputReaders.setAll(stageCount, null);
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

    /**
     * Resolves through the open scan whose frames hold the column in the execution's orientation:
     * the probe scan, or the build scan of the stage that reads it.
     */
    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return atom.getJoinedSymbols().getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isBuilt) {
            build();
        }
        if (chainStageCount > 0 && nextChainMatch()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            return true;
        }
        while (true) {
            if (tupleIndex < tupleCount) {
                positionTuple();
                return true;
            }
            // A frame whose task stopped at its cap of tuples goes on in another chunk.
            if (cursor > -1 && continueFrame()) {
                continue;
            }
            collectCursor(false);
            if (frameIndex >= frameLimit) {
                break;
            }
            fetchNextFrame();
            if (tupleIndex >= tupleCount) {
                // The frames ran out without a tuple.
                break;
            }
        }
        if (!allFramesActive) {
            throw frameSequence.buildInterruptionException();
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return atom.getJoinedSymbols().newSymbolTable(columnIndex);
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
        atom.toTop();
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
                for (int k = 0; k < stageCount; k++) {
                    final FrozenHashJoinBuild.Probe outputProbe = atom.getStage(k).getOutputSlot().getProbe();
                    final HashJoinPayloadSource.Reader outputReader = (HashJoinPayloadSource.Reader) outputProbe.getRecord();
                    outputProbes.setQuick(k, outputProbe);
                    outputReaders.setQuick(k, outputReader);
                    record.setStageRecord(k, outputReader);
                }
            }
            isBuilt = true;
        } catch (Throwable th) {
            // The cursor closes here, and with it the builds, so that a failed build holds no memory.
            Misc.free(this, th);
            throw th;
        }
    }

    /**
     * Joins the next chunk of the frame the cursor holds, if its task, or its last chunk, stopped at
     * the cap, on the owner's slot, and returns true with the chunk's tuples to read. Returns false
     * when the frame is done, and when the execution was cancelled, which drops the frame.
     */
    private boolean continueFrame() {
        if (atom.getRing().getResumePosition(frameIndex) < 0) {
            return false;
        }
        // The breaker is checked before each chunk, as the reduce jobs check it before each frame,
        // and the chunk checks it every so many pairs.
        if (!AsyncHashJoinAtom.isInterrupted(circuitBreaker, frameSequence)) {
            final long chunkTupleCount = atom.continueFrame(frameSequence.getTask(cursor), circuitBreaker);
            if (chunkTupleCount != HashJoinStageStepper.CANCELLED) {
                tuples = atom.getRing().getTuples(frameIndex);
                tupleIndex = 0;
                tupleCount = chunkTupleCount;
                return true;
            }
        }
        // A cancelled execution's frames hold nothing to read.
        allFramesActive = false;
        tupleCount = 0;
        return false;
    }

    // The links of a chain that a tuple left to the consumer: the rows of its key. Walks the chain
    // with the stage's output probe, which changes no link.
    private long countChain(int stage, long head) {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        final FrozenHashJoinBuild.Probe probe = outputProbes.getQuick(stage);
        probe.ofChain(head);
        long length = 0;
        while (probe.hasNext()) {
            probe.next();
            length++;
        }
        return length;
    }

    /**
     * The combinations of the current tuple's chains that come after the current one,
     * odometer-style, see {@link #nextChainMatch()}: for each chain, the matches after its current
     * one, times the combinations of the chains after it. Walks the chains no further than to count
     * them, and walks none of them from then on.
     */
    private long countRemainingChainMatches() {
        long remaining = 0;
        long laterCombinations = 1;
        for (int i = chainStageCount - 1; i > -1; i--) {
            final FrozenHashJoinBuild.Probe probe = outputProbes.getQuick(chainStages[i]);
            long laterMatches = 0;
            while (probe.hasNext()) {
                probe.next();
                laterMatches++;
            }
            remaining += laterMatches * laterCombinations;
            if (i > 0) {
                laterCombinations *= countChain(chainStages[i], chainHeads[i]);
            }
        }
        chainStageCount = 0;
        return remaining;
    }

    /**
     * Counts the rest of a frame that a tuple-carrying task joined, which the cursor holds: the rows
     * of its tuples not yet read, each as many as the product of its chains' lengths, and the rows
     * that the task, or the last chunk, left to a continuation, which the owner counts rather than
     * joins. Then collects the frame.
     */
    private void countTupleFrame(Counter counter) {
        countedTupleFrameCount++;
        long rows = 0;
        for (; tupleIndex < tupleCount; tupleIndex++) {
            final long base = tupleIndex * tupleStride;
            long tupleRows = 1;
            for (int k = 0; k < stageCount; k++) {
                final long entry = tuples.get(base + 1 + k);
                if (entry < HashJoinStageStepper.NO_MATCH) {
                    tupleRows *= countChain(k, HashJoinStageStepper.toChainHead(entry));
                }
            }
            rows += tupleRows;
        }
        counter.add(rows);
        if (atom.getRing().getResumePosition(frameIndex) > -1) {
            countedContinuationCount++;
            // The breaker is checked before the rest, as before a chunk of a continuation.
            final long restRows = AsyncHashJoinAtom.isInterrupted(circuitBreaker, frameSequence)
                    ? HashJoinStageStepper.CANCELLED
                    : atom.countFrameRest(frameSequence.getTask(cursor), circuitBreaker);
            if (restRows == HashJoinStageStepper.CANCELLED) {
                allFramesActive = false;
            } else {
                counter.add(restRows);
            }
        }
        collectCursor(false);
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
                // Only a count dispatches count-only tasks, and it collects every frame.
                assert !task.isCountOnly();
                allFramesActive &= frameSequence.isActive();
                frameIndex = task.getFrameIndex();
                tupleIndex = 0;
                tupleCount = task.getFilteredRowCount();
                if (tupleCount > 0 && frameSequence.isActive()) {
                    tuples = atom.getRing().getTuples(frameIndex);
                    probeRecord.init(task.getFrameMemory());
                    return;
                }
                // A cancelled or failed sequence's frames hold nothing to read. A task stops at its
                // cap only once it wrote tuples, so a frame without any left nothing to continue.
                assert tupleCount > 0 || !frameSequence.isActive() || atom.getRing().getResumePosition(frameIndex) < 0;
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

    /**
     * Moves the current tuple's chains to their next combination of matches, odometer-style: the
     * last stage's chain moves on, and once a chain runs out, the chain of the stage before it moves
     * on and every later chain starts over from its head. Returns false, and walks no chain from then
     * on, once every combination was read.
     */
    private boolean nextChainMatch() {
        for (int i = chainStageCount - 1; i > -1; i--) {
            final FrozenHashJoinBuild.Probe probe = outputProbes.getQuick(chainStages[i]);
            if (probe.hasNext()) {
                probe.next();
                for (int j = i + 1; j < chainStageCount; j++) {
                    final FrozenHashJoinBuild.Probe laterProbe = outputProbes.getQuick(chainStages[j]);
                    laterProbe.ofChain(chainHeads[j]);
                    laterProbe.next();
                }
                return true;
            }
        }
        chainStageCount = 0;
        return false;
    }

    // Positions the record at the next tuple: the probe row, and each stage's match, or the first
    // match of each chain that a stage left to the consumer.
    private void positionTuple() {
        final long base = tupleIndex++ * tupleStride;
        probeRecord.setRowIndex(tuples.get(base));
        chainStageCount = 0;
        for (int k = 0; k < stageCount; k++) {
            final long entry = tuples.get(base + 1 + k);
            if (entry > HashJoinStageStepper.NO_MATCH) {
                outputReaders.getQuick(k).positionLocator(entry);
                record.setHasMatch(k, true);
            } else if (entry == HashJoinStageStepper.NO_MATCH) {
                record.setHasMatch(k, false);
            } else {
                // A key of several matches: the worker left its chain to the consumer, which walks it
                // in chain order, the order the serial join returns the matches in.
                final long head = HashJoinStageStepper.toChainHead(entry);
                final FrozenHashJoinBuild.Probe outputProbe = outputProbes.getQuick(k);
                outputProbe.ofChain(head);
                outputProbe.next();
                record.setHasMatch(k, true);
                chainStages[chainStageCount] = k;
                chainHeads[chainStageCount++] = head;
            }
        }
    }

    private void resetFrames() {
        frameIndex = -1;
        tupleIndex = 0;
        tupleCount = 0;
        chainStageCount = 0;
        allFramesActive = true;
    }

    // The frames whose count-only tasks the open execution's counts collected.
    int getCountOnlyFrameCount() {
        return countOnlyFrameCount;
    }

    // The continuations that the open execution's counts counted rather than joined.
    int getCountedContinuationCount() {
        return countedContinuationCount;
    }

    // The frames of tuple-carrying tasks whose rest the open execution's counts counted.
    int getCountedTupleFrameCount() {
        return countedTupleFrameCount;
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
        // The build binds the stages' payload readers to the record.
        record.setOrientation(atom.isSwapped());
        probeRecord.of(frameSequence.getSymbolTableSource());
        isBuilt = false;
        frameLimit = -1;
        countOnlyFrameCount = 0;
        countedContinuationCount = 0;
        countedTupleFrameCount = 0;
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
