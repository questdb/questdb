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
 * The cursor passes every {@code next()} the execution's effective dispatch limit, the window of
 * the atom's tuple ring, which keeps frames in flight from sharing an entry; see
 * {@link HashJoinTupleRing}. Once {@code next()} handed it a frame, the cursor dispatches the frames
 * after it up to the same window, see {@link PageFrameSequence#dispatchAhead(int)}, so that the
 * workers join them while it reads the frame: {@code next()} alone publishes frames only when none
 * is ready to collect. The ring's window is dynamic: each frame the cursor has read tells the
 * ring how much memory its entry took, and the window follows the largest, so that frames whose
 * tuples are few, under a selective probe filter, keep more workers busy within the ring's budget.
 * {@code toTop()} probes again over the same builds, in the same orientation.
 * <p>
 * The rows of every probe frame that the cursor collects count toward the factory's stepper
 * warm-up, over all its executions, after which the tasks run the stages' hidden stepper copies
 * rather than the shared template; see {@link AsyncHashJoinAtom#addProbedRows(long)}.
 * <p>
 * Once the builds are done, and before the probe scan cuts its frames, the cursor caps the rows of
 * the probe's native frames at what a task's cap of tuples holds at the fan-out that the builds of
 * the stages whose matches the workers walk lead one to expect, see
 * {@link AsyncHashJoinAtom#getProbeFrameRowCap()}, so that duplicate matches rarely leave the rest of
 * a frame to a serial continuation.
 * <p>
 * Under a LIMIT that bounds the rows that the cursor's reader reads, which the join takes from its
 * own query or from one over it, see {@link AsyncHashJoinRecordCursorFactory#ofLimit}, the
 * cursor first dispatches only the leading frames whose rows reach the LIMIT, if they fit the
 * window, and the task of the last of them joins only as many of its rows as the LIMIT needs; the
 * consumer, if it reads further, continues that frame itself, in chunks that double, before the
 * window opens. A LIMIT met by the first rows thus joins about as many rows as it returns, where a
 * full window would join a frame per worker. The rows that the client reads first narrow the first
 * window as a LIMIT does, see {@link AsyncHashJoinRecordCursorFactory#markClientRowSource()}; a
 * client that has read them, and reads on, reads the rest of the join, so the window opens in full
 * as soon as it asks for the first row past them, and the workers join the frames after the capped
 * one while the consumer continues it.
 * <p>
 * An execution that swapped the first join's inputs, see {@link AsyncHashJoinAtom#isSwapped()}, probes
 * the right input's frames and reads the left input's columns from the first stage's matches; the
 * joined row keeps its columns, and each column's symbol tables come from the open scan whose frames
 * hold it.
 * <p>
 * {@link #calculateSize} counts the rows still to come without reading them: it dispatches
 * count-only tasks, which count each frame's joined rows on the workers, see
 * {@link AsyncHashJoinAtom#probeFrame}, and reads the length of each chain that a tuple leaves to the
 * consumer off an index that the builds take first, see {@link AsyncHashJoinAtom#indexChainLengths},
 * which leaves their links as they are, so that {@code toTop()} reads the rows again. A count that
 * follows some rows first counts what the tuple-carrying tasks dispatched before it left: the
 * current tuple's chains' combinations after the current one, the current frame's tuples not yet
 * read, the tuples of every such task it collects, chains included, and the rows that each of those
 * frames left to a continuation, which it counts on the owner's slot rather than joins.
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
    // The last frame of the first window under a LIMIT, -1 for none; see firstWindow().
    private int firstWindowEnd = -1;
    private int frameIndex;
    // The last frame's index once the cursor prepared the dispatch, -1 before.
    private int frameLimit;
    private PageFrameSequence<AsyncHashJoinAtom> frameSequence;
    // The frame whose ring entry the cursor reads, which it hands back to the ring before it
    // collects the frame, -1 for none.
    private int heldEntryFrame = -1;
    // False once a frame the cursor collected came from a cancelled or failed execution.
    private boolean isAllFramesActive;
    private boolean isBuilt;
    private boolean isOpen;
    // The rows of the query's LIMIT, or that the client reads first, Long.MAX_VALUE when neither
    // bounds them.
    private long limitRows = Long.MAX_VALUE;
    // The rows that hasNext() returned since the cursor opened or went back to the top.
    private long returnedRows;
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
     * row was read yet, and stay as they are. While rows are left to count, the builds whose stages
     * hand their chains on index their chain lengths, once per execution, before the count dispatches
     * its first task. The count checks the execution's breaker, as {@link #hasNext()} does,
     * whichever breaker the caller passes.
     */
    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (!isBuilt) {
            build();
        }
        // Rows are left to count: those of the frame that the cursor holds, which the current tuple's
        // chains imply, or of frames not yet collected.
        if (cursor > -1 || frameIndex < frameLimit) {
            atom.indexChainLengths(this.circuitBreaker);
        }
        if (chainStageCount > 0) {
            addRows(counter, countRemainingChainMatches());
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
                isAllFramesActive &= frameSequence.isActive();
                frameIndex = task.getFrameIndex();
                // A cancelled or failed sequence's frames hold nothing to count.
                if (frameSequence.isActive()) {
                    if (task.isCountOnly()) {
                        countOnlyFrameCount++;
                        addRows(counter, task.getFilteredRowCount());
                    } else {
                        // A task that the cursor dispatched before the count began carries tuples.
                        tuples = atom.getRing().getTuples(frameIndex);
                        heldEntryFrame = frameIndex;
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
        if (!isAllFramesActive) {
            throw frameSequence.buildInterruptionException();
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            try {
                try {
                    collectCursor(true);
                } finally {
                    // The rows the consumer did not read are not needed: the frames in flight that no
                    // worker started are dropped rather than joined. The frames that workers run are
                    // awaited even when the collection fails, as defining the stepper copies may,
                    // since the reset frees what they read.
                    if (frameLimit > -1) {
                        frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                        frameSequence.await();
                    }
                }
            } finally {
                try {
                    // The sequence's reset runs the atom's clear(), which closes the builds, their
                    // readers and the build scans, and then the sequence closes the probe scan.
                    frameSequence.reset();
                } finally {
                    buildRounds.reset();
                    probeRecord.of(null);
                    heldEntryFrame = -1;
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
            returnedRows++;
            return true;
        }
        while (true) {
            if (tupleIndex < tupleCount) {
                positionTuple();
                returnedRows++;
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
            if (cursor == -1) {
                // The frames ran out without a tuple to read or a frame to continue.
                break;
            }
        }
        if (!isAllFramesActive) {
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

    // Adds joined rows to a count, which fails rather than wrap past what a LONG holds: the counter
    // takes the rows once their sum with its count fits.
    private static void addRows(Counter counter, long rows) {
        final long count = counter.get();
        counter.add(HashJoinStageStepper.addRowCounts(count, rows) - count);
    }

    private void build() {
        try {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            // The builds complete on this thread before the first probe task is published, which is
            // what publishes them to the workers.
            // An INNER join whose build has no row returns none, so the execution dispatches no probe
            // task: with no frame to read, the cursor ends at once.
            if (atom.build(circuitBreaker, buildRounds)) {
                // The builds tell how many matches a probe row meets, which the frames' rows follow.
                frameSequence.setPageFrameMaxRows(atom.getProbeFrameRowCap());
                frameSequence.prepareForDispatch();
                frameLimit = frameSequence.getFrameCount() - 1;
                long probeRows = 0;
                for (int i = 0, n = frameSequence.getFrameCount(); i < n; i++) {
                    probeRows += frameSequence.getFrameRowCount(i);
                }
                // The frames' rows, or the LIMIT's when fewer, bound the keys that the execution
                // translates first.
                atom.bindSymbolKeyTranslation(Math.min(probeRows, limitRows), circuitBreaker);
                atom.maybeCopyPayload(probeRows, circuitBreaker, buildRounds);
                final HashJoinTupleRing ring = atom.getRing();
                ring.of(frameSequence, true);
                firstWindow(ring.getDispatchLimit());
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
            if (frameIndex == firstWindowEnd && returnedRows >= limitRows) {
                // The reader has read the rows that the first window was cut for and asks for more:
                // it reads on, so the frames after this one go out now, within the full window,
                // and the workers join them while the consumer continues this one. A reader that
                // still lacks some of those rows, which an INNER join or a probe filter dropped,
                // continues this frame first.
                frameSequence.dispatchAhead(atom.getRing().getDispatchLimit());
            }
            final long chunkTupleCount = atom.continueFrame(frameSequence.getTask(cursor), circuitBreaker);
            if (chunkTupleCount != HashJoinStageStepper.CANCELLED) {
                tuples = atom.getRing().getTuples(frameIndex);
                tupleIndex = 0;
                tupleCount = chunkTupleCount;
                return true;
            }
        }
        // A cancelled execution's frames hold nothing to read.
        isAllFramesActive = false;
        tupleCount = 0;
        return false;
    }

    // The rows of a chain that a tuple left to the consumer: the rows of its key, which the stage's
    // output probe reads off the build's chain length index.
    private long countChain(int stage, long head) {
        return outputProbes.getQuick(stage).chainLength(head);
    }

    /**
     * The combinations of the current tuple's chains that come after the current one,
     * odometer-style, see {@link #nextChainMatch()}: for each chain, the matches after its current
     * one, times the combinations of the chains after it. Reads the chains' lengths off the builds'
     * index, and walks none of the chains from then on. Fails rather than wrap once the combinations
     * pass what a LONG holds; a chain at its last match adds none, however many the chains after it
     * have.
     */
    private long countRemainingChainMatches() {
        long remaining = 0;
        long laterCombinations = 1;
        for (int i = chainStageCount - 1; i > -1; i--) {
            final FrozenHashJoinBuild.Probe probe = outputProbes.getQuick(chainStages[i]);
            // The matches that a walk from where the probe stands would visit.
            final long laterMatches = probe.chainLength(probe.chainHead());
            if (laterMatches > 0) {
                remaining = HashJoinStageStepper.addRowCounts(
                        remaining,
                        HashJoinStageStepper.multiplyRowCounts(laterMatches, laterCombinations)
                );
            }
            if (i > 0) {
                laterCombinations = HashJoinStageStepper.multiplyRowCounts(
                        laterCombinations,
                        countChain(chainStages[i], chainHeads[i])
                );
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
                    tupleRows = HashJoinStageStepper.multiplyRowCounts(
                            tupleRows,
                            countChain(k, HashJoinStageStepper.toChainHead(entry))
                    );
                }
            }
            rows = HashJoinStageStepper.addRowCounts(rows, tupleRows);
        }
        addRows(counter, rows);
        if (atom.getRing().getResumePosition(frameIndex) > -1) {
            countedContinuationCount++;
            // The breaker is checked before the rest, as before a chunk of a continuation.
            final long restRows = AsyncHashJoinAtom.isInterrupted(circuitBreaker, frameSequence)
                    ? HashJoinStageStepper.CANCELLED
                    : atom.countFrameRest(frameSequence.getTask(cursor), circuitBreaker);
            if (restRows == HashJoinStageStepper.CANCELLED) {
                isAllFramesActive = false;
            } else {
                addRows(counter, restRows);
            }
        }
        collectCursor(false);
    }

    private void collectCursor(boolean isForceCollect) {
        if (heldEntryFrame > -1) {
            // The ring's window follows the entries the cursor read.
            final int entryFrame = heldEntryFrame;
            heldEntryFrame = -1;
            atom.getRing().release(entryFrame);
        }
        if (cursor > -1) {
            try {
                // The frame's rows count toward the factory's stepper warm-up, whatever its task did,
                // before the collection lets the frames after it start. The warm-up may define the
                // stepper copies, and a failure to define them still collects the frame.
                atom.addProbedRows(frameSequence.getTask(cursor).getFrameRowCount());
            } finally {
                frameSequence.collect(cursor, isForceCollect);
                cursor = -1;
                // The record reads the task's frame memory, which the task releases on collection.
                probeRecord.clear();
                tuples = null;
            }
        }
    }

    private void fetchNextFrame() {
        do {
            cursor = frameSequence.next(getDispatchLimit(frameIndex + 1), false);
            if (cursor > -1) {
                final PageFrameReduceTask task = frameSequence.getTask(cursor);
                if (task.hasError()) {
                    throw task.buildError();
                }
                // Only a count dispatches count-only tasks, and it collects every frame.
                assert !task.isCountOnly();
                isAllFramesActive &= frameSequence.isActive();
                frameIndex = task.getFrameIndex();
                // The frames after this one go out now, within the window that the next() that
                // fetched it allowed, so that the workers join them while the consumer reads this
                // one, rather than once the consumer has drained the window.
                frameSequence.dispatchAhead(getDispatchLimit(frameIndex));
                tupleIndex = 0;
                tupleCount = task.getFilteredRowCount();
                if (frameSequence.isActive()) {
                    // The task of a frame of an active sequence ran, and wrote the frame's entry.
                    heldEntryFrame = frameIndex;
                    // A task stops at its cap of tuples only once it wrote some, but the capped
                    // task of a LIMIT's first window may leave the rest of its frame to a
                    // continuation without any.
                    if (tupleCount > 0 || atom.getRing().getResumePosition(frameIndex) > -1) {
                        tuples = atom.getRing().getTuples(frameIndex);
                        probeRecord.init(task.getFrameMemory());
                        return;
                    }
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

    /**
     * Picks the first window of an execution under a LIMIT: the leading frames whose rows reach the
     * LIMIT, if they fit the ring's window, and caps the task of the last of them at the rows that
     * the LIMIT still needs after the frames before it. The cursor dispatches no later frame until
     * it has read that one; see {@link #getDispatchLimit(int)}. The frames' rows stand for the
     * rows a frame yields, though a probe filter keeps fewer and an INNER join may drop some: a
     * LIMIT that they do not meet reads on, through the capped frame's continuation, into the full
     * window.
     */
    private void firstWindow(int window) {
        firstWindowEnd = -1;
        if (limitRows != Long.MAX_VALUE) {
            long rows = 0;
            for (int f = 0, n = Math.min(window, frameSequence.getFrameCount()); f < n; f++) {
                final long frameRows = frameSequence.getFrameRowCount(f);
                if (frameRows >= limitRows - rows) {
                    firstWindowEnd = f;
                    atom.setRowCappedFrame(f, limitRows - rows);
                    return;
                }
                rows += frameRows;
            }
        }
    }

    // The dispatch limit of the next() that fetches the given frame: the ring's window, which the
    // first window under a LIMIT narrows to the frames up to its last one.
    private int getDispatchLimit(int nextFrameIndex) {
        final int window = atom.getRing().getDispatchLimit();
        return nextFrameIndex <= firstWindowEnd ? Math.min(window, firstWindowEnd + 1 - nextFrameIndex) : window;
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
        returnedRows = 0;
        tupleIndex = 0;
        tupleCount = 0;
        chainStageCount = 0;
        isAllFramesActive = true;
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
            SqlExecutionCircuitBreaker circuitBreaker,
            long limitRows
    ) {
        this.frameSequence = frameSequence;
        this.buildRounds = buildRounds;
        this.atom = frameSequence.getAtom();
        this.circuitBreaker = circuitBreaker;
        this.limitRows = limitRows;
        this.firstWindowEnd = -1;
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
