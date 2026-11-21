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

package io.questdb.test.cairo.sql.async;

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Dispatches frame ranges, forward and reverse, over frames that form multi-frame row-group runs, the
 * way a negative LIMIT over an async filter reads its newest frames and then ranges of older frames.
 * <p>
 * A frame of a window that requires materialization joins the run of the previous frame when that frame
 * belongs to the same window of the same partition and requires materialization too. Only a partition
 * with deltas has such windows, which open source builds never create, so the test hands the sequence
 * synthetic frames that carry a partition frame state. The reducer never decodes a frame: it reports the
 * frame index and the frame row count as its filtered rows, or no rows for frames 2, 7 and 12, so the test
 * can check which frame each task covers, in which order the worker reduces the frames of a run, and which
 * frame and rows each collect presents.
 * <p>
 * Frame layout (partition, window, window requires materialization), and the runs it forms:
 * <pre>
 *  frame  0: p0 w0 -    run A
 *  frame  1: p0 w1 +    run B
 *  frame  2: p0 w1 +    run B
 *  frame  3: p0 w1 +    run B
 *  frame  4: p0 w1 +    run B
 *  frame  5: p0 w2 +    run C (one frame)
 *  frame  6: p0 w3 +    run D
 *  frame  7: p0 w3 +    run D
 *  frame  8: p0 w3 +    run D
 *  frame  9: p0 w4 -    run E
 *  frame 10: p0 w4 -    run F (same window as E, but no materialization)
 *  frame 11: p0 w5 +    run G
 *  frame 12: p0 w5 +    run G
 *  frame 13: p1 w5 +    run H (same window id as G, other partition)
 *  frame 14: p1 w5 +    run H
 *  frame 15: p1 w6 +    run I (one frame)
 * </pre>
 * Frame i holds 3 * (i + 1) rows, so every frame has its own row count.
 */
public class PageFrameSequenceRangeTest extends AbstractCairoTest {
    private static final boolean[][] DELTA_WINDOWS = {
            {false, true, true, true, false, true, false},
            {false, false, false, false, false, true, true},
    };
    private static final int FRAME_COUNT = 16;
    private static final int[] FRAME_PARTITIONS = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1};
    private static final int[] FRAME_WINDOWS = {0, 1, 1, 1, 1, 2, 3, 3, 3, 4, 4, 5, 5, 5, 5, 6};
    private static final int RUN_COUNT = 9;
    private static final long STATE_SIZE = PartitionFrameState.HEADER_BYTES
            + (long) DELTA_WINDOWS[0].length * PartitionFrameState.SUMMARY_LONGS * Long.BYTES;
    private static final int WINDOW_COUNT = DELTA_WINDOWS[0].length;
    // Pairs of (task index, frame index), in the order the reducers saw them.
    private final IntList reducedFrames = new IntList();

    @Before
    public void setUp() {
        // A small reduce queue makes the tasks of a range fill the queue, so the owner also reduces
        // tasks locally and steals them from the queue.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        super.setUp();
    }

    @Test
    public void testAbandonedReverseRangeMidRun() throws Exception {
        // The LIMIT can rewind the filter while the cursor holds a run part-way: it collects the held frame
        // and calls toTop() for another range, which must release the held run and read the new range whole.
        assertWithAndWithoutWorkers((engine, ctx, dispatchLimit) -> {
            try (Fixture fixture = new Fixture(engine, ctx)) {
                final PageFrameSequence<StatefulAtom> sequence = fixture.sequence;
                readRange(sequence, 0, FRAME_COUNT - 1, dispatchLimit);
                // run D, from frame 8 down to frame 6, is the first task of the range
                sequence.toTop(8, 1);
                long cursor = nextCursor(sequence, dispatchLimit);
                Assert.assertEquals(8, sequence.getTask(cursor).getFrameIndex());
                sequence.collect(cursor, false);
                cursor = nextCursor(sequence, dispatchLimit);
                Assert.assertEquals(7, sequence.getTask(cursor).getFrameIndex());
                sequence.collect(cursor, false);
                readRange(sequence, 4, 2, dispatchLimit);
                // the rewind may also come before the cursor collects the frame it reads
                sequence.toTop(14, 11);
                cursor = nextCursor(sequence, dispatchLimit);
                Assert.assertEquals(14, sequence.getTask(cursor).getFrameIndex());
                readRange(sequence, 15, 0, dispatchLimit);
            }
        });
    }

    @Test
    public void testForwardRanges() throws Exception {
        assertWithAndWithoutWorkers((engine, ctx, dispatchLimit) -> {
            try (Fixture fixture = new Fixture(engine, ctx)) {
                final PageFrameSequence<StatefulAtom> sequence = fixture.sequence;
                // the first dispatch, before any toTop(), covers every frame
                assertRange(sequence, 0, FRAME_COUNT - 1, dispatchLimit);
                Assert.assertEquals(RUN_COUNT, sequence.getTaskCount());
                readRange(sequence, 0, FRAME_COUNT - 1, dispatchLimit);
                readRange(sequence, 2, 7, dispatchLimit);
                readRange(sequence, 3, 3, dispatchLimit);
                readRange(sequence, 1, 4, dispatchLimit);
                readRange(sequence, 12, 14, dispatchLimit);
                readRange(sequence, 9, 10, dispatchLimit);
                readRange(sequence, 15, 15, dispatchLimit);
            }
        });
    }

    @Test
    public void testOlderRangesOfNegativeLimit() throws Exception {
        // The negative LIMIT reads every frame, then ranges of older frames, each from its oldest frame down
        // to the frame next to the frames read before, see AsyncFilteredNegativeLimitRecordCursor.toTop().
        // The ranges cut runs B, D and H, so a run spreads over two consecutive ranges, and each range must
        // read only its own frames of the run.
        assertWithAndWithoutWorkers((engine, ctx, dispatchLimit) -> {
            try (Fixture fixture = new Fixture(engine, ctx)) {
                final PageFrameSequence<StatefulAtom> sequence = fixture.sequence;
                readRange(sequence, 0, FRAME_COUNT - 1, dispatchLimit);
                readRange(sequence, 2, 1, dispatchLimit);
                readRange(sequence, 7, 3, dispatchLimit);
                readRange(sequence, 13, 8, dispatchLimit);
                readRange(sequence, 15, 14, dispatchLimit);
                // a range made of single frames
                readRange(sequence, 0, 0, dispatchLimit);
            }
        });
    }

    @Test
    public void testReverseRanges() throws Exception {
        assertWithAndWithoutWorkers((engine, ctx, dispatchLimit) -> {
            try (Fixture fixture = new Fixture(engine, ctx)) {
                final PageFrameSequence<StatefulAtom> sequence = fixture.sequence;
                readRange(sequence, 0, FRAME_COUNT - 1, dispatchLimit);
                // every frame, newest first
                readRange(sequence, FRAME_COUNT - 1, 0, dispatchLimit);
                // starts and ends inside run B
                readRange(sequence, 3, 2, dispatchLimit);
                // starts inside run D and ends inside run B
                readRange(sequence, 7, 2, dispatchLimit);
                // starts at the last frame of run D and ends at the first frame of run B
                readRange(sequence, 8, 1, dispatchLimit);
                // starts at the first frame of run D and ends at the last frame of run B
                readRange(sequence, 6, 4, dispatchLimit);
                // one frame inside a run, a one-frame run, and a frame of no run
                readRange(sequence, 3, 3, dispatchLimit);
                readRange(sequence, 5, 5, dispatchLimit);
                readRange(sequence, 0, 0, dispatchLimit);
                // frames of one window that do not form a run
                readRange(sequence, 10, 9, dispatchLimit);
                // runs of the same window id in two partitions
                readRange(sequence, 14, 11, dispatchLimit);
                readRange(sequence, 13, 12, dispatchLimit);
                // starts at the last frame and ends inside the first multi-frame run
                readRange(sequence, 15, 3, dispatchLimit);
            }
        });
    }

    private static void appendRun(ObjList<IntList> tasks, int frame, int previousFrame) {
        if (tasks.size() == 0 || !isSameRun(previousFrame, frame)) {
            tasks.add(new IntList());
        }
        tasks.getLast().add(frame);
    }

    private static void assertCollectedFrame(String range, PageFrameReduceTask task, int frame) {
        Assert.assertFalse(range, task.hasError());
        Assert.assertEquals(range, frame, task.getFrameIndex());
        Assert.assertEquals(range, frameRowCount(frame), task.getFrameRowCount());
        final DirectLongList rows = task.getFilteredRows();
        if (hasFilteredRows(frame)) {
            Assert.assertEquals(range, 2, task.getFilteredRowCount());
            Assert.assertEquals(range, 2, rows.size());
            Assert.assertEquals(range, frame, rows.get(0));
            Assert.assertEquals(range, frameRowCount(frame), rows.get(1));
        } else {
            Assert.assertEquals(range, 0, task.getFilteredRowCount());
            Assert.assertEquals(range, 0, rows.size());
        }
    }

    // The tasks a range dispatches, in order, each with its frames in the order the range reads them. The
    // model groups the range's consecutive frames by the run rule, independently of the sequence.
    private static ObjList<IntList> expectedTasks(int firstFrame, int lastFrame) {
        final ObjList<IntList> tasks = new ObjList<>();
        final int step = lastFrame < firstFrame ? -1 : 1;
        for (int frame = firstFrame; ; frame += step) {
            appendRun(tasks, frame, frame - step);
            if (frame == lastFrame) {
                return tasks;
            }
        }
    }

    private static long frameRowCount(int frame) {
        return 3L * (frame + 1);
    }

    private static boolean hasFilteredRows(int frame) {
        return frame % 5 != 2;
    }

    private static boolean isDelta(int frame) {
        return DELTA_WINDOWS[FRAME_PARTITIONS[frame]][FRAME_WINDOWS[frame]];
    }

    private static boolean isSameRun(int frame, int otherFrame) {
        return isDelta(frame) && isDelta(otherFrame)
                && FRAME_PARTITIONS[frame] == FRAME_PARTITIONS[otherFrame]
                && FRAME_WINDOWS[frame] == FRAME_WINDOWS[otherFrame];
    }

    private static WorkerPool newFiberHostPool() {
        return new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "fiber-host";
            }

            @Override
            public int getWorkerCount() {
                return 4;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }
        });
    }

    private static long nextCursor(PageFrameSequence<StatefulAtom> sequence, int dispatchLimit) {
        long cursor;
        while ((cursor = sequence.next(dispatchLimit, false)) == -1) {
            Os.pause();
        }
        Assert.assertTrue("no task to collect", cursor > -1);
        return cursor;
    }

    private void assertRange(PageFrameSequence<StatefulAtom> sequence, int firstFrame, int lastFrame, int dispatchLimit) {
        final ObjList<IntList> expected = expectedTasks(firstFrame, lastFrame);
        final String range = "range [" + firstFrame + ", " + lastFrame + ']';
        Assert.assertEquals(range, expected.size(), sequence.getTaskCount());
        final IntList expectedFrames = new IntList();
        for (int i = 0, n = expected.size(); i < n; i++) {
            expectedFrames.addAll(expected.getQuick(i));
        }
        // the collector sees the range's frames one at a time, in range order
        for (int i = 0, n = expectedFrames.size(); i < n; i++) {
            final int frame = expectedFrames.getQuick(i);
            final long cursor = nextCursor(sequence, dispatchLimit);
            assertCollectedFrame(range, sequence.getTask(cursor), frame);
            // presenting the same frame again changes nothing
            assertCollectedFrame(range, sequence.getTask(cursor), frame);
            sequence.collect(cursor, false);
        }
        // one task per run portion inside the range, its frames reduced in range order on one worker
        final ObjList<IntList> actual = new ObjList<>();
        synchronized (reducedFrames) {
            for (int i = 0, n = reducedFrames.size(); i < n; i += 2) {
                final int taskIndex = reducedFrames.getQuick(i);
                while (actual.size() <= taskIndex) {
                    actual.add(new IntList());
                }
                actual.getQuick(taskIndex).add(reducedFrames.getQuick(i + 1));
            }
        }
        Assert.assertEquals(range, expected.toString(), actual.toString());
    }

    private void assertWithAndWithoutWorkers(SequenceAssertion assertion) throws Exception {
        // A dispatch limit of 0, the default of a server without query workers, has the owner reduce every
        // task itself through its local task.
        for (int dispatchLimit : new int[]{0, 1, Integer.MAX_VALUE}) {
            // the owner reduces every task: it publishes the tasks, steals them back or reduces them locally
            assertMemoryLeak(() -> assertion.run(engine, sqlExecutionContext, dispatchLimit));
            // workers reduce the tasks the owner publishes
            assertMemoryLeak(() -> TestUtils.execute(
                    new WorkerPool(() -> 4),
                    (engine, compiler, ctx) -> assertion.run(engine, ctx, dispatchLimit),
                    configuration,
                    LOG
            ));
            // fibers of a fiber-host pool reduce the tasks the owner publishes
            assertMemoryLeak(() -> TestUtils.execute(
                    newFiberHostPool(),
                    (engine, compiler, ctx) -> assertion.run(engine, ctx, dispatchLimit),
                    configuration,
                    LOG
            ));
        }
    }

    private void readRange(PageFrameSequence<StatefulAtom> sequence, int firstFrame, int lastFrame, int dispatchLimit) {
        sequence.toTop(firstFrame, lastFrame);
        synchronized (reducedFrames) {
            reducedFrames.clear();
        }
        assertRange(sequence, firstFrame, lastFrame, dispatchLimit);
    }

    @FunctionalInterface
    private interface SequenceAssertion {
        void run(CairoEngine engine, SqlExecutionContext ctx, int dispatchLimit) throws Exception;
    }

    // Partition frame states of the layout: window w of partition p requires materialization when
    // DELTA_WINDOWS[p][w] is set.
    private static class FrameStates implements AutoCloseable {
        private final long[] states = new long[DELTA_WINDOWS.length];

        private FrameStates() {
            for (int p = 0; p < DELTA_WINDOWS.length; p++) {
                final long state = Unsafe.calloc(STATE_SIZE, MemoryTag.NATIVE_DEFAULT);
                states[p] = state;
                final long summaries = state + PartitionFrameState.HEADER_BYTES;
                // header cells: window summaries address, window count, logical rows, has custom frames
                Unsafe.putLong(state, summaries);
                Unsafe.putLong(state + Long.BYTES, WINDOW_COUNT);
                Unsafe.putLong(state + 3 * Long.BYTES, 1);
                for (int w = 0; w < WINDOW_COUNT; w++) {
                    final long summary = summaries + (long) w * PartitionFrameState.SUMMARY_LONGS * Long.BYTES;
                    Unsafe.putLong(
                            summary + PartitionFrameState.SUMMARY_FLAGS * Long.BYTES,
                            DELTA_WINDOWS[p][w] ? PartitionFrameState.WINDOW_HAS_DELTA : 0
                    );
                }
            }
        }

        @Override
        public void close() {
            for (int p = 0; p < states.length; p++) {
                if (states[p] != 0) {
                    Unsafe.free(states[p], STATE_SIZE, MemoryTag.NATIVE_DEFAULT);
                    states[p] = 0;
                }
            }
        }

        private long get(int partition) {
            return states[partition];
        }
    }

    // One query over the synthetic frames: the factory, the frame sequence prepared for dispatch, and the
    // frame states. The reducer reports each frame's index and row count as its filtered rows, except for
    // the frames hasFilteredRows() rejects, which it reports without rows.
    private class Fixture implements AutoCloseable {
        private final RunFrameFactory factory;
        private final PageFrameSequence<StatefulAtom> sequence;
        private final FrameStates states;

        private Fixture(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
            final CairoConfiguration configuration = engine.getConfiguration();
            states = new FrameStates();
            factory = new RunFrameFactory(states);
            sequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (workerId, record, task, circuitBreaker, stealingFrameSequence) -> {
                        final int frame = task.getFrameIndex();
                        synchronized (reducedFrames) {
                            reducedFrames.add(task.getTaskIndex());
                            reducedFrames.add(frame);
                        }
                        if (hasFilteredRows(frame)) {
                            task.getFilteredRows().add(frame);
                            task.getFilteredRows().add(task.getFrameRowCount());
                            task.setFilteredRowCount(2);
                        }
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    4,
                    PageFrameReduceTask.TYPE_FILTER
            );
            synchronized (reducedFrames) {
                reducedFrames.clear();
            }
            try {
                sequence.of(factory, ctx, new SCSequence(), PartitionFrameCursorFactory.ORDER_ASC);
                sequence.prepareForDispatch();
            } catch (Throwable th) {
                close();
                throw th;
            }
            Assert.assertEquals(FRAME_COUNT, sequence.getFrameCount());
        }

        @Override
        public void close() {
            try {
                sequence.await();
                sequence.reset();
            } finally {
                Misc.free(sequence);
                Misc.free(factory);
                states.close();
            }
        }
    }

    private static class RunFrame implements PageFrame {
        private int frame;
        private long partitionLo;
        private long state;

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getColumnCount() {
            return 1;
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getParquetRowGroup() {
            return FRAME_WINDOWS[frame];
        }

        @Override
        public int getParquetRowGroupHi() {
            return (int) partitionLo + (int) frameRowCount(frame);
        }

        @Override
        public int getParquetRowGroupLo() {
            return (int) partitionLo;
        }

        @Override
        public long getPartitionFrameState() {
            return state;
        }

        @Override
        public long getPartitionHi() {
            return partitionLo + frameRowCount(frame);
        }

        @Override
        public int getPartitionIndex() {
            return FRAME_PARTITIONS[frame];
        }

        @Override
        public long getPartitionLo() {
            return partitionLo;
        }
    }

    private static class RunFrameCursor implements PageFrameCursor {
        private final ColumnMapping columnMapping = new ColumnMapping();
        private final RunFrame frame = new RunFrame();
        private final FrameStates states;
        private int nextFrame;
        private long partitionLo;

        private RunFrameCursor(FrameStates states) {
            this.states = states;
            columnMapping.addColumn(0, 0, 0);
        }

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
        }

        @Override
        public void close() {
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return columnMapping;
        }

        @Override
        public long getRemainingRowsInInterval() {
            return 0;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public boolean hasCustomFrames() {
            return true;
        }

        @Override
        public boolean isExternal() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            if (nextFrame == FRAME_COUNT) {
                return null;
            }
            if (nextFrame > 0 && FRAME_PARTITIONS[nextFrame] != FRAME_PARTITIONS[nextFrame - 1]) {
                partitionLo = 0;
            }
            frame.frame = nextFrame;
            frame.partitionLo = partitionLo;
            frame.state = states.get(FRAME_PARTITIONS[nextFrame]);
            partitionLo += frameRowCount(nextFrame);
            nextFrame++;
            return frame;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public boolean supportsSizeCalculation() {
            return false;
        }

        @Override
        public void toTop() {
            nextFrame = 0;
            partitionLo = 0;
        }
    }

    private static class RunFrameFactory extends AbstractRecordCursorFactory {
        private final RunFrameCursor cursor;

        private RunFrameFactory(FrameStates states) {
            super(metadata());
            this.cursor = new RunFrameCursor(states);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public boolean supportsPageFrameCursor() {
            return true;
        }

        private static GenericRecordMetadata metadata() {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("v", ColumnType.LONG));
            return metadata;
        }
    }
}
