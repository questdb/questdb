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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.HashJoinTupleRing;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.table.HashJoinTupleRing.computeDispatchLimit;
import static io.questdb.griffin.engine.table.HashJoinTupleRing.computeRingBudget;

/**
 * The tuple ring of a parallel hash join under a real ordered {@link PageFrameSequence}: a test
 * reducer writes each probe frame's tuples into the frame's ring entry as the join's probe reducer
 * does, and the owner reads them back in frame order as the join's cursor does, with
 * {@code next()} alone, or dispatching the frames after each frame it takes up to the window, see
 * {@link PageFrameSequence#dispatchAhead(int)}.
 * <p>
 * The test atom checks the ring's safety argument directly. A task claims its entry before it
 * writes it and the owner releases the entry once it has read it, before it collects the frame,
 * so two frames that shared an entry while both were in flight would find the entry claimed. Each
 * task also checks that its frame lies inside the window the owner's dispatch limit allows. The
 * owner then checks that the entry holds exactly the frame's tuples, and the frame's resume
 * position and kept-row count.
 * <p>
 * The probe table has 40 frames of 10 rows each, so that the ring wraps around many times; with
 * two stages a frame's tuples take 10 * 3 * 8 = 240 bytes.
 */
public class HashJoinTupleRingTest extends AbstractCairoTest {
    private static final int FRAME_COUNT = 40;
    private static final int FRAME_ROWS = 10;
    private static final long FRAME_BYTES = FRAME_ROWS * 3 * Long.BYTES;
    private static final int FREE = -1;
    private static final long LARGE_RING = 1L << 30;
    private static final Log LOG = LogFactory.getLog(HashJoinTupleRingTest.class);
    private static final int MAX_ENTRIES = 64;
    private static final int STAGE_COUNT = 2;
    private static final int TUPLE_STRIDE = STAGE_COUNT + 1;
    private static final int WORKER_COUNT = 4;

    @Test
    public void testCountOnlyTasksWithWiderLimitOnOwnerAlone() throws Exception {
        assertCountOnlyTasksWithWiderLimit(null);
    }

    @Test
    public void testCountOnlyTasksWithWiderLimitOnThreadWorkers() throws Exception {
        assertCountOnlyTasksWithWiderLimit(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testDispatchLimitAtEachBound() {
        // The configured dispatch limit binds.
        Assert.assertEquals(2, computeDispatchLimit(2, 32, 100 * FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        Assert.assertEquals(31, computeDispatchLimit(31, 32, 100 * FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        // A limit of 0, which reduces every frame locally, or below leaves one entry.
        Assert.assertEquals(1, computeDispatchLimit(1, 32, 100 * FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        Assert.assertEquals(1, computeDispatchLimit(0, 32, 100 * FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        Assert.assertEquals(1, computeDispatchLimit(-1, 32, 100 * FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        Assert.assertEquals(1, computeDispatchLimit(Integer.MIN_VALUE, 32, 100 * FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        // The queue capacity binds a dispatch limit above it.
        Assert.assertEquals(32, computeDispatchLimit(33, 32, 100 * FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        Assert.assertEquals(32, computeDispatchLimit(Integer.MAX_VALUE, 32, Long.MAX_VALUE, FRAME_ROWS, TUPLE_STRIDE));
        // The ring budget binds in whole frames.
        Assert.assertEquals(3, computeDispatchLimit(100, 32, 3 * FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        Assert.assertEquals(2, computeDispatchLimit(100, 32, 3 * FRAME_BYTES - 1, FRAME_ROWS, TUPLE_STRIDE));
        Assert.assertEquals(2, computeDispatchLimit(100, 32, 2 * FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        Assert.assertEquals(1, computeDispatchLimit(100, 32, FRAME_BYTES, FRAME_ROWS, TUPLE_STRIDE));
        // A budget below one frame leaves one entry.
        Assert.assertEquals(1, computeDispatchLimit(100, 32, FRAME_BYTES - 1, FRAME_ROWS, TUPLE_STRIDE));
        Assert.assertEquals(1, computeDispatchLimit(100, 32, 0, FRAME_ROWS, TUPLE_STRIDE));
        // Each stage widens a tuple, so the same budget holds fewer frames of a wider stack.
        Assert.assertEquals(5, computeDispatchLimit(100, 32, 5 * FRAME_ROWS * 9 * Long.BYTES, FRAME_ROWS, 9));
        Assert.assertEquals(15, computeDispatchLimit(100, 32, 5 * FRAME_ROWS * 9 * Long.BYTES, FRAME_ROWS, 3));
        // The design's figures: 32 frames of 1M rows at stride 3 is 768 MB, which holds all 32
        // frames of a two-stage stack, but only 10 of an eight-stage one.
        Assert.assertEquals(32, computeDispatchLimit(32, 256, 768_000_000L, 1_000_000, 3));
        Assert.assertEquals(10, computeDispatchLimit(32, 256, 768_000_000L, 1_000_000, 9));
        // Without probe frames the budget bounds nothing.
        Assert.assertEquals(8, computeDispatchLimit(8, 32, 0, 0, TUPLE_STRIDE));
        // A huge budget over tiny frames stays within the limit.
        Assert.assertEquals(8, computeDispatchLimit(8, 32, Long.MAX_VALUE, 1, TUPLE_STRIDE));
        // A frame too large to size in bytes fits no budget; one just below that fits once.
        final long rowBytes = TUPLE_STRIDE * Long.BYTES;
        Assert.assertEquals(1, computeDispatchLimit(8, 32, Long.MAX_VALUE, Long.MAX_VALUE, TUPLE_STRIDE));
        Assert.assertEquals(1, computeDispatchLimit(8, 32, Long.MAX_VALUE, Long.MAX_VALUE / rowBytes + 1, TUPLE_STRIDE));
        Assert.assertEquals(1, computeDispatchLimit(8, 32, Long.MAX_VALUE, Long.MAX_VALUE / rowBytes, TUPLE_STRIDE));
        Assert.assertEquals(2, computeDispatchLimit(8, 32, Long.MAX_VALUE, Long.MAX_VALUE / rowBytes / 2, TUPLE_STRIDE));
    }

    @Test
    public void testEmptyProbeHasNoFramesToSizeTheRing() throws Exception {
        assertMemoryLeak(() -> withRing(null, 4, 1, -1, "empty", (engine, compiler, context, sequence, factory) -> {
            final RingAtom atom = sequence.getAtom();
            bind(sequence, factory, context, null);
            try {
                Assert.assertEquals(0, sequence.getFrameCount());
                // A budget below one byte bounds no frame: the dispatch limit alone sizes the ring.
                Assert.assertEquals(4, atom.ring.getDispatchLimit());
                Assert.assertEquals(-2, sequence.next(4, false));
                Assert.assertEquals(0, atom.ring.getAllocatedBytes());
            } finally {
                sequence.await();
                sequence.reset();
            }
            Assert.assertEquals(0, atom.ring.getDispatchLimit());
        }));
    }

    @Test
    public void testEntriesNeverSharedAtEachDispatchLimitOnFiberWorkers() throws Exception {
        assertEntriesNeverSharedAtEachDispatchLimit(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testEntriesNeverSharedAtEachDispatchLimitOnOwnerAlone() throws Exception {
        assertEntriesNeverSharedAtEachDispatchLimit(null);
    }

    @Test
    public void testEntriesNeverSharedAtEachDispatchLimitOnThreadWorkers() throws Exception {
        assertEntriesNeverSharedAtEachDispatchLimit(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testEntriesNeverSharedWhenDispatchingAheadOnFiberWorkers() throws Exception {
        assertEntriesNeverSharedWhenDispatchingAhead(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testEntriesNeverSharedWhenDispatchingAheadOnOwnerAlone() throws Exception {
        assertEntriesNeverSharedWhenDispatchingAhead(null);
    }

    @Test
    public void testEntriesNeverSharedWhenDispatchingAheadOnThreadWorkers() throws Exception {
        assertEntriesNeverSharedWhenDispatchingAhead(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testLocalTasksOnOwnerAlone() throws Exception {
        assertLocalTasks(null);
    }

    @Test
    public void testLocalTasksOnThreadWorkers() throws Exception {
        assertLocalTasks(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testLocalTasksWithFramesDispatchedAheadOnOwnerAlone() throws Exception {
        assertLocalTasksWithFramesDispatchedAhead(null);
    }

    @Test
    public void testLocalTasksWithFramesDispatchedAheadOnThreadWorkers() throws Exception {
        assertLocalTasksWithFramesDispatchedAhead(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testMemoryCountedByTrackerAndFreedOnClear() throws Exception {
        assertMemoryLeak(() -> withRing(WorkerPoolMode.LEGACY, 4, LARGE_RING, -1, "t", (engine, compiler, context, sequence, factory) -> {
            final RingAtom atom = sequence.getAtom();
            // The owner holds frame 0 until the whole first window is joined, so that workers write
            // entries too.
            atom.isAwaitingFullWindow = true;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0)) {
                try {
                    for (int execution = 0; execution < 2; execution++) {
                        final long tagBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
                        bind(sequence, factory, context, tracker);
                        try {
                            Assert.assertEquals(4, atom.ring.getDispatchLimit());
                            // Binding creates the entries but allocates nothing.
                            Assert.assertEquals(0, atom.ring.getAllocatedBytes());
                            Assert.assertEquals(0, tracker.getUsed());

                            readFrames(sequence, Integer.MAX_VALUE, Integer.MAX_VALUE, 0);
                            // Every entry holds one frame's tuples, one per row: 240 bytes each,
                            // charged to the execution's tracker, on the threads that wrote them.
                            final long ringBytes = 4 * FRAME_BYTES;
                            Assert.assertEquals(ringBytes, atom.ring.getAllocatedBytes());
                            Assert.assertEquals(ringBytes, tracker.getUsed());
                            Assert.assertEquals(ringBytes, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - tagBytes);
                            Assert.assertTrue(atom.hasSeenFullWindow);
                            Assert.assertTrue(atom.workerTasks.get() > 0);

                            // toTop() keeps the ring bound and its entries, which the next pass reuses.
                            sequence.toTop();
                            atom.startPass();
                            Assert.assertEquals(4, atom.ring.getDispatchLimit());
                            readFrames(sequence, Integer.MAX_VALUE, Integer.MAX_VALUE, 0);
                            Assert.assertEquals(ringBytes, atom.ring.getAllocatedBytes());
                            Assert.assertEquals(ringBytes, tracker.getUsed());
                        } finally {
                            sequence.await();
                            sequence.reset();
                        }
                        // clear(), which the sequence's reset() runs, frees every entry.
                        Assert.assertEquals(0, atom.ring.getAllocatedBytes());
                        Assert.assertEquals(0, atom.ring.getDispatchLimit());
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertEquals(tagBytes, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
                    }
                } finally {
                    context.setMemoryTracker(null);
                }
            }
        }));
    }

    @Test
    public void testMemoryLimitBreachWhileEntryGrowsOnOwnerAlone() throws Exception {
        assertMemoryLimitBreachWhileEntryGrows(null);
    }

    @Test
    public void testMemoryLimitBreachWhileEntryGrowsOnThreadWorkers() throws Exception {
        assertMemoryLimitBreachWhileEntryGrows(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testMisuseAsserts() throws Exception {
        assertMemoryLeak(() -> withRing(null, 4, LARGE_RING, -1, "t", (engine, compiler, context, sequence, factory) -> {
            final HashJoinTupleRing ring = sequence.getAtom().ring;
            bind(sequence, factory, context, null);
            try {
                Assert.assertEquals(4, ring.getDispatchLimit());
                // Binding again needs clear() first.
                assertFails(() -> ring.of(sequence), "clear() the ring before binding it to another execution");
                Assert.assertEquals(4, ring.getDispatchLimit());
                // Frame 4 maps to frame 0's entry: while frame 0 holds it, frame 4 reads nothing.
                final DirectLongList tuples = ring.openTuples(0, 1);
                tuples.add(7);
                ring.setResumePosition(0, 5, 2);
                assertFails(() -> ring.getTuples(4), "frame 4 shares ring entry 0 with frame 0");
                assertFails(() -> ring.getKeptRowCount(4), "frame 4 shares ring entry 0 with frame 0");
                assertFails(() -> ring.getResumePosition(4), "frame 4 shares ring entry 0 with frame 0");
                assertFails(() -> ring.setResumePosition(4, 1, 1), "frame 4 shares ring entry 0 with frame 0");
                // Frame 0 still reads its own state, and an entry nobody opened holds no frame.
                Assert.assertSame(tuples, ring.getTuples(0));
                Assert.assertEquals(1, ring.getTuples(0).size());
                Assert.assertEquals(5, ring.getKeptRowCount(0));
                Assert.assertEquals(2, ring.getResumePosition(0));
                assertFails(() -> ring.getTuples(1), "frame 1 shares ring entry 1 with frame -1");
                // The open allocated room for one tuple.
                Assert.assertEquals(TUPLE_STRIDE * Long.BYTES, ring.getAllocatedBytes());
            } finally {
                sequence.await();
                sequence.reset();
            }
            Assert.assertEquals(0, ring.getAllocatedBytes());
        }));
    }

    @Test
    public void testRingBudgetBoundsTheWindowOnOwnerAlone() throws Exception {
        assertRingBudgetBoundsTheWindow(null);
    }

    @Test
    public void testRingBudgetBoundsTheWindowOnThreadWorkers() throws Exception {
        assertRingBudgetBoundsTheWindow(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testRingBudgetCappedAtQuarterOfQueryMemoryLimit() {
        // No per-query limit: the configured budget.
        Assert.assertEquals(1_000, computeRingBudget(1_000, 0));
        Assert.assertEquals(1_000, computeRingBudget(1_000, -1));
        // A quarter of the limit above the budget leaves the budget, at or below it caps it.
        Assert.assertEquals(1_000, computeRingBudget(1_000, 8_000));
        Assert.assertEquals(1_000, computeRingBudget(1_000, 4_000));
        Assert.assertEquals(999, computeRingBudget(1_000, 3_999));
        Assert.assertEquals(1, computeRingBudget(1_000, 4));
        Assert.assertEquals(0, computeRingBudget(1_000, 3));
        Assert.assertEquals(0, computeRingBudget(1_000, 1));
        Assert.assertEquals(0, computeRingBudget(0, 8_000));
        Assert.assertEquals(Long.MAX_VALUE / 4, computeRingBudget(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    private static void assertFails(Runnable operation, String message) {
        final AssertionError error = Assert.assertThrows(AssertionError.class, operation::run);
        TestUtils.assertContains(error.getMessage(), message);
    }

    private static void assertNoMismatch(RingAtom atom) {
        final String mismatch = atom.mismatch.get();
        if (mismatch != null) {
            Assert.fail(mismatch);
        }
    }

    // The owner holds a frame until the workers have joined the given frames, the ones before the
    // frame it holds and the ones of the window, so that all of the window's entries are claimed at
    // once, each by a frame of its own.
    private static void awaitFullWindow(RingAtom atom, int reducedFrames, int windowFrames) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (atom.reducedTupleFrames.get() < reducedFrames) {
            Assert.assertTrue("the workers did not join the window", System.nanoTime() < deadline);
            Os.pause();
        }
        int claimed = 0;
        for (int entry = 0; entry < MAX_ENTRIES; entry++) {
            if (atom.entryOwners.get(entry) != FREE) {
                claimed++;
            }
        }
        Assert.assertEquals(windowFrames, claimed);
        atom.hasSeenFullWindow = true;
    }

    // Waits until the workers joined every frame that the sequence dispatched.
    private static void awaitReducedFrames(PageFrameSequence<RingAtom> sequence) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (sequence.getReduceFinishedCounter().get() < sequence.getDispatchedFrameCount()) {
            Assert.assertTrue("the workers did not join the frames dispatched", System.nanoTime() < deadline);
            Os.pause();
        }
    }

    // Binds the sequence to an execution over the factory's frames and the ring to the sequence once
    // the sequence counted its frames, as the join's cursor does once the builds are done, but with a
    // fixed window of R entries, where the join's cursor binds a dynamic one. The execution charges
    // its memory to the tracker; the query registry clears the context's tracker when an execution
    // ends, so every execution sets it again.
    private static void bind(
            PageFrameSequence<RingAtom> sequence,
            RecordCursorFactory factory,
            SqlExecutionContext context,
            @Nullable MemoryTracker tracker
    ) throws Exception {
        final RingAtom atom = sequence.getAtom();
        context.setMemoryTracker(tracker);
        sequence.of(factory, context, atom.collectSubSeq, ORDER_ASC);
        Assert.assertSame(tracker, sequence.getMemoryTracker());
        sequence.prepareForDispatch();
        for (int i = 0, n = sequence.getFrameCount(); i < n; i++) {
            Assert.assertEquals(FRAME_ROWS, sequence.getFrameRowCount(i));
        }
        atom.ring.of(sequence);
        atom.startPass();
    }

    private static int copiesOf(RingAtom atom, int frameIndex, long row) {
        if (frameIndex == atom.hotFrame) {
            return atom.hotCopies;
        }
        return 1 + (int) ((frameIndex + row) % atom.fanOut);
    }

    private static long keptRowCountOf(int frameIndex) {
        // Every third frame pretends it had no probe filter.
        return frameIndex % 3 == 0 ? -1 : FRAME_ROWS - frameIndex % 3;
    }

    private static long locatorOf(long x, int copy, int stage) {
        return x * 1_000 + copy * 10L + stage;
    }

    // Reads every frame of a bound sequence in frame order, as the join's cursor does: frames
    // before localFrom dispatched with the ring's limit and later ones with limit 0, which makes
    // the sequence reduce them locally once nothing is in flight; frames from countOnlyFrom on as
    // count-only tasks under countOnlyLimit. Returns how many frames the sequence reduced locally.
    private static int readFrames(PageFrameSequence<RingAtom> sequence, int localFrom, int countOnlyFrom, int countOnlyLimit) {
        final RingAtom atom = sequence.getAtom();
        final HashJoinTupleRing ring = atom.ring;
        final int dispatchLimit = ring.getDispatchLimit();
        int localTasks = 0;
        int frameIndex = 0;
        while (frameIndex < sequence.getFrameCount()) {
            final boolean isCountOnly = frameIndex >= countOnlyFrom;
            final int limit = isCountOnly ? countOnlyLimit : frameIndex >= localFrom ? 0 : dispatchLimit;
            final long cursor = sequence.next(limit, isCountOnly);
            if (cursor == -1) {
                Os.pause();
                continue;
            }
            Assert.assertTrue("cursor " + cursor, cursor > -1);
            final PageFrameReduceTask task = sequence.getTask(cursor);
            if (task.hasError()) {
                throw task.buildError();
            }
            assertNoMismatch(atom);
            Assert.assertEquals(frameIndex, task.getFrameIndex());
            if (cursor == Long.MAX_VALUE) {
                localTasks++;
            }
            if (task.isCountOnly()) {
                Assert.assertEquals(tupleCountOf(atom, frameIndex), task.getFilteredRowCount());
            } else {
                if (isCountOnly) {
                    // A tuple task dispatched before count-only reading began: count the count-only
                    // frames that map to its entry and ran before the owner read it.
                    for (int other = frameIndex + dispatchLimit; other < FRAME_COUNT; other += dispatchLimit) {
                        if (atom.countOnlyRan.get(other) == 1) {
                            atom.countOnlyOverlaps.incrementAndGet();
                        }
                    }
                }
                if (frameIndex == 0 && atom.isAwaitingFullWindow) {
                    final int windowFrames = Math.min(dispatchLimit, sequence.getFrameCount());
                    awaitFullWindow(atom, windowFrames, windowFrames);
                }
                verifyTuples(atom, frameIndex, task.getFilteredRowCount());
            }
            atom.lastCollectedFrame = frameIndex;
            sequence.collect(cursor, false);
            frameIndex++;
        }
        assertNoMismatch(atom);
        return localTasks;
    }

    // Reads every frame of a bound sequence in frame order under the ring's limit, as the join's
    // cursor does: once next() hands it a frame, the owner dispatches the frames after it up to the
    // limit, so that as many frames are in flight as the limit allows, the one it reads included.
    // With workers, the owner first waits until they joined every frame dispatched, so that next()
    // finds each frame ready and only dispatchAhead() sends the frames after it, and holds frame 1
    // until the workers joined the frames ahead of it. Returns how many frames the sequence reduced
    // locally.
    private static int readFramesDispatchingAhead(PageFrameSequence<RingAtom> sequence, boolean isAwaitingWorkers) {
        final RingAtom atom = sequence.getAtom();
        final int dispatchLimit = atom.ring.getDispatchLimit();
        int localTasks = 0;
        int frameIndex = 0;
        while (frameIndex < sequence.getFrameCount()) {
            if (isAwaitingWorkers) {
                awaitReducedFrames(sequence);
            }
            final long cursor = sequence.next(dispatchLimit, false);
            if (cursor == -1) {
                Os.pause();
                continue;
            }
            Assert.assertTrue("cursor " + cursor, cursor > -1);
            final PageFrameReduceTask task = sequence.getTask(cursor);
            if (task.hasError()) {
                throw task.buildError();
            }
            assertNoMismatch(atom);
            Assert.assertEquals(frameIndex, task.getFrameIndex());
            if (cursor == Long.MAX_VALUE) {
                localTasks++;
            }
            sequence.dispatchAhead(dispatchLimit);
            final int dispatchedFrames = Math.min(FRAME_COUNT, frameIndex + dispatchLimit);
            Assert.assertEquals(dispatchedFrames, sequence.getDispatchedFrameCount());
            if (frameIndex == 1 && isAwaitingWorkers) {
                // Frame 0's entry is free again, and frame 1 and the frames ahead of it claim one
                // entry each.
                awaitFullWindow(atom, dispatchedFrames, dispatchedFrames - 1);
            }
            verifyTuples(atom, frameIndex, task.getFilteredRowCount());
            atom.lastCollectedFrame = frameIndex;
            sequence.collect(cursor, false);
            frameIndex++;
        }
        assertNoMismatch(atom);
        return localTasks;
    }

    // Reads every frame of a bound sequence in frame order under the ring's limit, as
    // readFramesDispatchingAhead() does, up to localFrom. From there on the owner passes next() a
    // limit of 0, as on a reduce queue that other queries keep full: next() hands it the frames still
    // in flight, and once none is, reduces the next frame locally. While the owner holds that local
    // frame, it dispatches the frames after it up to the ring's limit, as the join's cursor does on a
    // queue that has room again, and then collects them under the limit of 0, so that the frame after
    // them is local again. With workers, the owner holds each local frame until they joined the
    // frames ahead of it. Returns how many frames the sequence reduced locally.
    private static int readFramesDispatchingAheadOfLocalFrames(
            PageFrameSequence<RingAtom> sequence,
            int localFrom,
            boolean isAwaitingWorkers
    ) {
        final RingAtom atom = sequence.getAtom();
        final int dispatchLimit = atom.ring.getDispatchLimit();
        int localTasks = 0;
        int frameIndex = 0;
        while (frameIndex < sequence.getFrameCount()) {
            final boolean isQueueFull = frameIndex >= localFrom;
            // Once the owner has collected every frame dispatched, a limit of 0 leaves next() no
            // frame to collect or to publish, so it reduces the frame locally.
            final boolean isLocal = isQueueFull && sequence.getDispatchedFrameCount() == frameIndex;
            final long cursor = sequence.next(isQueueFull ? 0 : dispatchLimit, false);
            if (cursor == -1) {
                Os.pause();
                continue;
            }
            Assert.assertTrue("cursor " + cursor, cursor > -1);
            final PageFrameReduceTask task = sequence.getTask(cursor);
            if (task.hasError()) {
                throw task.buildError();
            }
            assertNoMismatch(atom);
            Assert.assertEquals(frameIndex, task.getFrameIndex());
            Assert.assertEquals("local task of frame " + frameIndex, isLocal, cursor == Long.MAX_VALUE);
            if (!isQueueFull || isLocal) {
                final int ownerTasks = atom.ownerTasks.get();
                sequence.dispatchAhead(dispatchLimit);
                final int dispatchedFrames = Math.min(FRAME_COUNT, frameIndex + dispatchLimit);
                Assert.assertEquals("frame " + frameIndex, dispatchedFrames, sequence.getDispatchedFrameCount());
                // dispatchAhead() runs no task on the owner.
                Assert.assertEquals(ownerTasks, atom.ownerTasks.get());
                if (isLocal) {
                    localTasks++;
                    if (isAwaitingWorkers) {
                        // The owner holds the local frame, and its entry, until the workers joined
                        // the frames ahead of it, each into an entry of its own.
                        awaitFullWindow(atom, dispatchedFrames, dispatchedFrames - frameIndex);
                    } else {
                        // Without workers, the frames ahead wait in the queue: the frames up to the
                        // local one ran, and no other.
                        Assert.assertEquals(frameIndex + 1, sequence.getReduceFinishedCounter().get());
                    }
                }
            }
            verifyTuples(atom, frameIndex, task.getFilteredRowCount());
            atom.lastCollectedFrame = frameIndex;
            sequence.collect(cursor, false);
            frameIndex++;
        }
        assertNoMismatch(atom);
        return localTasks;
    }

    private static long resumePositionOf(int frameIndex) {
        return frameIndex % 4 == 1 ? frameIndex : -1;
    }

    // The test reducer: joins a probe frame into its ring entry as the join's probe reducer does,
    // after it checked that the frame lies in the owner's window and claimed the entry; a
    // count-only task only counts and never touches the ring.
    private static void reduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final PageFrameSequence<RingAtom> sequence = task.getFrameSequence(RingAtom.class);
        final RingAtom atom = sequence.getAtom();
        final HashJoinTupleRing ring = atom.ring;
        final int frameIndex = task.getFrameIndex();
        final long frameRows = task.getFrameRowCount();
        if (workerId == -1) {
            atom.ownerTasks.incrementAndGet();
            if (stealingFrameSequence != sequence) {
                atom.fail("the owner reduced frame " + frameIndex + " for another sequence");
            }
        } else {
            atom.workerTasks.incrementAndGet();
        }
        final int dispatchLimit = ring.getDispatchLimit();
        // Frames the owner has not collected yet: this one and every one before it back to the
        // last collected one.
        final int window = frameIndex - atom.lastCollectedFrame;
        if (task.isCountOnly()) {
            long tupleCount = 0;
            for (long row = 0; row < frameRows; row++) {
                tupleCount += copiesOf(atom, frameIndex, row);
            }
            task.setFilteredRowCount(tupleCount);
            if (window > dispatchLimit) {
                atom.countOnlyBeyondWindow.incrementAndGet();
            }
            atom.countOnlyRan.set(frameIndex, 1);
            return;
        }
        if (window > dispatchLimit) {
            atom.fail("frame " + frameIndex + " ran with " + window + " frames uncollected, over the ring's " + dispatchLimit);
        }
        final int entry = frameIndex % dispatchLimit;
        final int holder = atom.entryOwners.compareAndExchange(entry, FREE, frameIndex);
        if (holder != FREE) {
            atom.fail("frame " + frameIndex + " shares ring entry " + entry + " with frame " + holder);
        }
        record.init(task.populateFrameMemory());
        final DirectLongList tuples = ring.openTuples(frameIndex, frameRows);
        for (long row = 0; row < frameRows; row++) {
            record.setRowIndex(row);
            final long x = record.getLong(0);
            for (int copy = 0, n = copiesOf(atom, frameIndex, row); copy < n; copy++) {
                tuples.add(row);
                for (int stage = 0; stage < STAGE_COUNT; stage++) {
                    tuples.add(locatorOf(x, copy, stage));
                }
            }
        }
        ring.setResumePosition(frameIndex, keptRowCountOf(frameIndex), resumePositionOf(frameIndex));
        task.setFilteredRowCount(tuples.size() / ring.getTupleStride());
        atom.reducedTupleFrames.incrementAndGet();
    }

    private static long tupleCountOf(RingAtom atom, int frameIndex) {
        long tupleCount = 0;
        for (long row = 0; row < FRAME_ROWS; row++) {
            tupleCount += copiesOf(atom, frameIndex, row);
        }
        return tupleCount;
    }

    // The frame's entry holds exactly its tuples: its x values come from the table's frames, 10
    // rows each in timestamp order, so frame f row r holds x = f * 10 + r + 1.
    private static void verifyTuples(RingAtom atom, int frameIndex, long tupleCount) {
        final HashJoinTupleRing ring = atom.ring;
        final DirectLongList tuples = ring.getTuples(frameIndex);
        Assert.assertEquals(tupleCountOf(atom, frameIndex), tupleCount);
        Assert.assertEquals(tupleCount * TUPLE_STRIDE, tuples.size());
        long p = 0;
        for (int row = 0; row < FRAME_ROWS; row++) {
            final long x = (long) frameIndex * FRAME_ROWS + row + 1;
            for (int copy = 0, n = copiesOf(atom, frameIndex, row); copy < n; copy++) {
                Assert.assertEquals(row, tuples.get(p++));
                for (int stage = 0; stage < STAGE_COUNT; stage++) {
                    Assert.assertEquals(locatorOf(x, copy, stage), tuples.get(p++));
                }
            }
        }
        Assert.assertEquals(keptRowCountOf(frameIndex), ring.getKeptRowCount(frameIndex));
        Assert.assertEquals(resumePositionOf(frameIndex), ring.getResumePosition(frameIndex));
        // The owner has read the entry; it releases it before it collects the frame.
        final int entry = frameIndex % ring.getDispatchLimit();
        Assert.assertEquals(frameIndex, atom.entryOwners.get(entry));
        atom.entryOwners.set(entry, FREE);
        atom.tupleFramesRead++;
    }

    private void assertCountOnlyTasksWithWiderLimit(@Nullable WorkerPoolMode mode) throws Exception {
        // With a dispatch limit of 2, tuple frames 0 to 10 go out two at a time. The owner then
        // reads the remaining frames as count-only tasks under a limit of 1000: the 29 frames left
        // fit the reduce queue, so they go out at once, while tuple frame 10 is still unread, and
        // count-only frames 12, 14, ... map to its entry.
        assertMemoryLeak(() -> withRing(mode, 2, LARGE_RING, -1, "t", (engine, compiler, context, sequence, factory) -> {
            final RingAtom atom = sequence.getAtom();
            atom.fanOut = 3;
            bind(sequence, factory, context, null);
            try {
                Assert.assertEquals(2, atom.ring.getDispatchLimit());
                readFrames(sequence, Integer.MAX_VALUE, 10, 1_000);
            } finally {
                sequence.await();
                sequence.reset();
            }
            if (mode == null) {
                // Frames 0 to 9 were read as tuples, and so was frame 10, which went out with frame
                // 9; the rest were counted.
                Assert.assertEquals(11, atom.tupleFramesRead);
                // The owner alone reduces in queue order every frame it dispatched before it reads
                // tuple frame 10: the count-only frames on its entry ran first, beyond the ring's
                // window, and left the entry to frame 10.
                Assert.assertEquals(14, atom.countOnlyOverlaps.get());
                Assert.assertEquals(28, atom.countOnlyBeyondWindow.get());
            } else {
                // Frame 10 went out as a tuple task only if a worker had not joined frame 9 yet
                // when the owner asked for it.
                Assert.assertTrue("tuple frames: " + atom.tupleFramesRead, atom.tupleFramesRead == 10 || atom.tupleFramesRead == 11);
            }
        }));
    }

    private void assertEntriesNeverSharedAtEachDispatchLimit(@Nullable WorkerPoolMode mode) throws Exception {
        // A reduce queue of 8: a limit of 100 is above it, so the queue's capacity sizes the ring.
        final int[][] limitsAndEntries = {{0, 1}, {1, 1}, {2, 2}, {100, 8}};
        for (int[] limitAndEntries : limitsAndEntries) {
            assertMemoryLeak(() -> withRing(mode, limitAndEntries[0], LARGE_RING, 8, "t", (engine, compiler, context, sequence, factory) -> {
                final RingAtom atom = sequence.getAtom();
                atom.fanOut = 3;
                // With workers, the owner holds frame 0 until the whole first window is joined.
                atom.isAwaitingFullWindow = mode != null;
                for (int execution = 0; execution < 2; execution++) {
                    bind(sequence, factory, context, null);
                    try {
                        Assert.assertEquals(limitAndEntries[1], atom.ring.getDispatchLimit());
                        Assert.assertEquals(0, readFrames(sequence, Integer.MAX_VALUE, Integer.MAX_VALUE, 0));
                    } finally {
                        sequence.await();
                        sequence.reset();
                    }
                    Assert.assertEquals(FRAME_COUNT, atom.tupleFramesRead);
                    if (mode == null) {
                        // The owner alone steals every frame it published.
                        Assert.assertEquals(FRAME_COUNT, atom.ownerTasks.get());
                        Assert.assertEquals(0, atom.workerTasks.get());
                    } else {
                        Assert.assertTrue(atom.hasSeenFullWindow);
                        // With two entries or more, the owner held a frame while the workers
                        // joined the rest of the window.
                        if (limitAndEntries[1] > 1) {
                            Assert.assertTrue(atom.workerTasks.get() > 0);
                        }
                    }
                }
            }));
        }
    }

    private void assertEntriesNeverSharedWhenDispatchingAhead(@Nullable WorkerPoolMode mode) throws Exception {
        // A reduce queue of 8: a limit of 100 is above it, so the queue's capacity sizes the ring.
        final int[][] limitsAndEntries = {{1, 1}, {2, 2}, {3, 3}, {7, 7}, {100, 8}};
        for (int[] limitAndEntries : limitsAndEntries) {
            assertMemoryLeak(() -> withRing(mode, limitAndEntries[0], LARGE_RING, 8, "t", (engine, compiler, context, sequence, factory) -> {
                final RingAtom atom = sequence.getAtom();
                atom.fanOut = 3;
                for (int execution = 0; execution < 2; execution++) {
                    bind(sequence, factory, context, null);
                    try {
                        Assert.assertEquals(limitAndEntries[1], atom.ring.getDispatchLimit());
                        Assert.assertEquals(0, readFramesDispatchingAhead(sequence, mode != null));
                    } finally {
                        sequence.await();
                        sequence.reset();
                    }
                    Assert.assertEquals(FRAME_COUNT, atom.tupleFramesRead);
                    if (mode == null) {
                        // The owner alone steals every frame it published.
                        Assert.assertEquals(FRAME_COUNT, atom.ownerTasks.get());
                        Assert.assertEquals(0, atom.workerTasks.get());
                    } else {
                        Assert.assertTrue(atom.hasSeenFullWindow);
                        Assert.assertEquals(FRAME_COUNT, atom.ownerTasks.get() + atom.workerTasks.get());
                        // A window of one frame holds the frame the owner reads and none ahead of it,
                        // so each next() publishes the frame it returns, which the owner may steal.
                        // A wider window has the owner steal frames of the first window alone, which
                        // the first next() publishes: it finds every later frame ready.
                        if (limitAndEntries[1] > 1) {
                            Assert.assertTrue("owner tasks: " + atom.ownerTasks.get(), atom.ownerTasks.get() <= limitAndEntries[1]);
                        }
                    }
                }
            }));
        }
    }

    private void assertLocalTasks(@Nullable WorkerPoolMode mode) throws Exception {
        // From frame 20 on the owner passes a limit of 0, below the ring's 4: it first collects the
        // frames still in flight, then reduces every later frame locally into its entry.
        assertMemoryLeak(() -> withRing(mode, 4, LARGE_RING, -1, "t", (engine, compiler, context, sequence, factory) -> {
            final RingAtom atom = sequence.getAtom();
            atom.fanOut = 2;
            bind(sequence, factory, context, null);
            final int localTasks;
            try {
                Assert.assertEquals(4, atom.ring.getDispatchLimit());
                localTasks = readFrames(sequence, 20, Integer.MAX_VALUE, 0);
            } finally {
                sequence.await();
                sequence.reset();
            }
            Assert.assertEquals(FRAME_COUNT, atom.tupleFramesRead);
            // Frames 20 to 22 at most went out with the ring's limit before frame 20 was read.
            Assert.assertTrue("local tasks: " + localTasks, localTasks >= FRAME_COUNT - 20 - 3);
            Assert.assertTrue("local tasks: " + localTasks, localTasks <= FRAME_COUNT - 20);
        }));
    }

    private void assertLocalTasksWithFramesDispatchedAhead(@Nullable WorkerPoolMode mode) throws Exception {
        // Up to frame 19 the owner dispatches ahead of each frame, so frames 20 to 18 + limit are in
        // flight once it has read frame 19. From frame 20 on it passes next() a limit of 0: it
        // collects those, reduces frame 19 + limit locally and dispatches the limit - 1 frames after
        // it, collects those, and so on. The local frames are 19 + k * limit for k from 1 up to
        // frame 39: 20 / limit of them, rounded down. A reduce queue of 8: a limit of 100 is above
        // it, so the queue's capacity sizes the ring.
        final int[][] limitsEntriesAndLocalTasks = {{1, 1, 20}, {2, 2, 10}, {3, 3, 6}, {7, 7, 2}, {100, 8, 2}};
        for (int[] limitEntriesAndLocalTasks : limitsEntriesAndLocalTasks) {
            assertMemoryLeak(() -> withRing(mode, limitEntriesAndLocalTasks[0], LARGE_RING, 8, "t", (engine, compiler, context, sequence, factory) -> {
                final RingAtom atom = sequence.getAtom();
                atom.fanOut = 3;
                for (int execution = 0; execution < 2; execution++) {
                    bind(sequence, factory, context, null);
                    final int localTasks;
                    final int dispatchedFrames;
                    final int reducedFrames;
                    try {
                        Assert.assertEquals(limitEntriesAndLocalTasks[1], atom.ring.getDispatchLimit());
                        localTasks = readFramesDispatchingAheadOfLocalFrames(sequence, 20, mode != null);
                    } finally {
                        sequence.await();
                        dispatchedFrames = sequence.getDispatchedFrameCount();
                        reducedFrames = sequence.getReduceFinishedCounter().get();
                        sequence.reset();
                    }
                    Assert.assertEquals(limitEntriesAndLocalTasks[2], localTasks);
                    Assert.assertEquals(FRAME_COUNT, atom.tupleFramesRead);
                    // Every frame dispatched ran, the local ones included, and reset() freed the ring.
                    Assert.assertEquals(FRAME_COUNT, dispatchedFrames);
                    Assert.assertEquals(FRAME_COUNT, reducedFrames);
                    Assert.assertEquals(0, atom.ring.getAllocatedBytes());
                    if (mode == null) {
                        // The owner alone reduces the local frames and steals every other one.
                        Assert.assertEquals(FRAME_COUNT, atom.ownerTasks.get());
                        Assert.assertEquals(0, atom.workerTasks.get());
                    } else {
                        Assert.assertEquals(FRAME_COUNT, atom.ownerTasks.get() + atom.workerTasks.get());
                    }
                }
            }));
        }
    }

    private void assertMemoryLimitBreachWhileEntryGrows(@Nullable WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> withRing(mode, 8, LARGE_RING, -1, "t", (engine, compiler, context, sequence, factory) -> {
            final RingAtom atom = sequence.getAtom();
            // Frame 3 joins each row 40 times: 10 * 40 * 3 longs, 9,600 bytes.
            atom.hotFrame = 3;
            atom.hotCopies = 40;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1_000)) {
                try {
                    bind(sequence, factory, context, tracker);
                    try {
                        // A quarter of the limit, 250 bytes, holds one frame of 240 bytes.
                        Assert.assertEquals(1, atom.ring.getDispatchLimit());
                        final CairoException error = Assert.assertThrows(
                                CairoException.class,
                                () -> readFrames(sequence, Integer.MAX_VALUE, Integer.MAX_VALUE, 0)
                        );
                        Assert.assertTrue(error.isOutOfMemory());
                        TestUtils.assertContains(error.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(error.getFlyweightMessage(), "workload=QUERY");
                        // Frames 0 to 2 were read; frame 3's entry grew past the limit.
                        Assert.assertEquals(3, atom.tupleFramesRead);
                        Assert.assertTrue(tracker.getUsed() > 0);
                    } finally {
                        sequence.await();
                        sequence.reset();
                    }
                    // The failed execution leaves nothing behind.
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, atom.ring.getAllocatedBytes());

                    // The next execution, without the limit, joins every frame, the hot one included.
                    tracker.setLimit(0);
                    bind(sequence, factory, context, tracker);
                    try {
                        Assert.assertEquals(8, atom.ring.getDispatchLimit());
                        readFrames(sequence, Integer.MAX_VALUE, Integer.MAX_VALUE, 0);
                        Assert.assertEquals(FRAME_COUNT, atom.tupleFramesRead);
                        Assert.assertTrue(tracker.getUsed() >= 7 * FRAME_BYTES + 40 * FRAME_BYTES);
                    } finally {
                        sequence.await();
                        sequence.reset();
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    context.setMemoryTracker(null);
                }
            }
            // The table still serves queries.
            try (
                    RecordCursorFactory countFactory = compiler.compile("SELECT count() FROM t", context).getRecordCursorFactory();
                    RecordCursor cursor = countFactory.getCursor(context)
            ) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(FRAME_COUNT * FRAME_ROWS, cursor.getRecord().getLong(0));
            }
        }));
    }

    private void assertRingBudgetBoundsTheWindow(@Nullable WorkerPoolMode mode) throws Exception {
        // The configured budget: three frames' worth, a byte less, and less than one frame.
        final long[][] budgetsAndEntries = {{3 * FRAME_BYTES, 3}, {3 * FRAME_BYTES - 1, 2}, {FRAME_BYTES - 1, 1}};
        for (long[] budgetAndEntries : budgetsAndEntries) {
            assertMemoryLeak(() -> withRing(mode, 8, budgetAndEntries[0], -1, "t", (engine, compiler, context, sequence, factory) -> {
                final RingAtom atom = sequence.getAtom();
                bind(sequence, factory, context, null);
                try {
                    Assert.assertEquals(budgetAndEntries[1], atom.ring.getDispatchLimit());
                    readFrames(sequence, Integer.MAX_VALUE, Integer.MAX_VALUE, 0);
                } finally {
                    sequence.await();
                    sequence.reset();
                }
                Assert.assertEquals(FRAME_COUNT, atom.tupleFramesRead);
            }));
        }
        // A per-query memory limit caps a large configured budget at a quarter of the limit: four
        // times three frames leaves three entries, and the execution fits the limit.
        assertMemoryLeak(() -> withRing(mode, 8, LARGE_RING, -1, "t", (engine, compiler, context, sequence, factory) -> {
            final RingAtom atom = sequence.getAtom();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(4 * 3 * FRAME_BYTES)) {
                try {
                    bind(sequence, factory, context, tracker);
                    try {
                        Assert.assertEquals(3, atom.ring.getDispatchLimit());
                        readFrames(sequence, Integer.MAX_VALUE, Integer.MAX_VALUE, 0);
                        Assert.assertEquals(3 * FRAME_BYTES, tracker.getUsed());
                    } finally {
                        sequence.await();
                        sequence.reset();
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    context.setMemoryTracker(null);
                }
            }
            Assert.assertEquals(FRAME_COUNT, atom.tupleFramesRead);
        }));
    }

    // Runs the code against a probe table of 40 frames of 10 rows, or an empty one, a sequence with
    // the test reducer, and a ring configured with the given dispatch limit, ring budget and, when
    // positive, reduce queue capacity.
    private void withRing(
            @Nullable WorkerPoolMode mode,
            int dispatchLimit,
            long ringMaxSize,
            int queueCapacity,
            String tableName,
            RingCode code
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, dispatchLimit);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, ringMaxSize);
        if (queueCapacity > 0) {
            setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, queueCapacity);
        }
        final int workerCount = mode == null ? 1 : WORKER_COUNT;
        TestUtils.execute(mode == null ? null : new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, context) -> {
            final CairoConfiguration dbConfiguration = db.getConfiguration();
            Assert.assertEquals(dispatchLimit, dbConfiguration.getSqlParallelFilterDispatchLimit());
            if (queueCapacity > 0) {
                Assert.assertEquals(queueCapacity, dbConfiguration.getPageFrameReduceQueueCapacity());
            }
            // 400 rows over four daily partitions, split into frames of 10 rows.
            db.execute("""
                    CREATE TABLE t AS (
                        SELECT x, timestamp_sequence('2024-01-01', 864_000_000L) ts FROM long_sequence(400)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """, context);
            db.execute("CREATE TABLE empty (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", context);
            context.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
            try (
                    RecordCursorFactory factory = compiler.compile(tableName, context).getRecordCursorFactory();
                    PageFrameSequence<RingAtom> sequence = new PageFrameSequence<>(
                            db,
                            dbConfiguration,
                            db.getMessageBus(),
                            new RingAtom(dbConfiguration),
                            HashJoinTupleRingTest::reduce,
                            () -> new PageFrameReduceTask(dbConfiguration, MemoryTag.NATIVE_OFFLOAD),
                            workerCount,
                            PageFrameReduceTask.TYPE_HASH_JOIN
                    )
            ) {
                code.run(db, compiler, context, sequence, factory);
            } finally {
                context.restoreToDefaultPageFrameSizes();
                // The next engine of the same test opens the same database.
                db.execute("DROP TABLE t", context);
                db.execute("DROP TABLE empty", context);
            }
        }, configuration, LOG);
    }

    @FunctionalInterface
    private interface RingCode {
        void run(
                CairoEngine engine,
                SqlCompiler compiler,
                SqlExecutionContext context,
                PageFrameSequence<RingAtom> sequence,
                RecordCursorFactory factory
        ) throws Exception;
    }

    /**
     * The join atom's part under test, its tuple ring, and the bookkeeping that checks it: which
     * frame holds each entry, where tasks ran, and which count-only frames ran when.
     */
    private static final class RingAtom implements StatefulAtom {
        private final SCSequence collectSubSeq = new SCSequence();
        private final AtomicInteger countOnlyBeyondWindow = new AtomicInteger();
        private final AtomicInteger countOnlyOverlaps = new AtomicInteger();
        private final AtomicIntegerArray countOnlyRan = new AtomicIntegerArray(FRAME_COUNT);
        private final AtomicIntegerArray entryOwners = new AtomicIntegerArray(MAX_ENTRIES);
        private final AtomicReference<String> mismatch = new AtomicReference<>();
        private final AtomicInteger ownerTasks = new AtomicInteger();
        private final AtomicInteger reducedTupleFrames = new AtomicInteger();
        private final HashJoinTupleRing ring;
        private final AtomicInteger workerTasks = new AtomicInteger();
        private volatile int fanOut = 1;
        private boolean hasSeenFullWindow;
        private volatile int hotCopies;
        private volatile int hotFrame = -1;
        private boolean isAwaitingFullWindow;
        private volatile int lastCollectedFrame = -1;
        private int tupleFramesRead;

        private RingAtom(CairoConfiguration configuration) {
            this.ring = new HashJoinTupleRing(configuration, STAGE_COUNT);
        }

        @Override
        public void clear() {
            ring.clear();
        }

        @Override
        public void close() {
            ring.close();
        }

        private void fail(String message) {
            mismatch.compareAndSet(null, message);
        }

        // Before each pass over the frames: every entry free, every count reset.
        private void startPass() {
            for (int entry = 0; entry < MAX_ENTRIES; entry++) {
                entryOwners.set(entry, FREE);
            }
            for (int frame = 0; frame < FRAME_COUNT; frame++) {
                countOnlyRan.set(frame, 0);
            }
            countOnlyBeyondWindow.set(0);
            countOnlyOverlaps.set(0);
            hasSeenFullWindow = false;
            lastCollectedFrame = -1;
            mismatch.set(null);
            ownerTasks.set(0);
            reducedTupleFrames.set(0);
            tupleFramesRead = 0;
            workerTasks.set(0);
        }
    }
}
