/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.engine.join.TimeFrameHelper;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimeFrameHelperTest {

    @Test
    public void testBinarySearchFuzz() {
        final int N = 10_000;
        Rnd rnd = TestUtils.generateRandom(null);

        TimeFrameHelper helper = new TimeFrameHelper(1, 1);
        for (int i = 0; i < N; i++) {
            var timestamps = new long[rnd.nextInt(1000) + 1];
            for (int j = 0; j < timestamps.length; j++) {
                timestamps[j] = rnd.nextPositiveLong() % Long.MAX_VALUE;
            }
            Arrays.sort(timestamps);

            try (var cursor = new MockTimeFrameCursor(List.of(frame(timestamps, timestamps[0], timestamps[timestamps.length - 1])), 0)) {
                helper.of(cursor);

                long expectedTimestampIdx;
                long timestampHi;
                long timestampLo;
                long rowLo;

                if (rnd.nextBoolean()) {
                    // Generate a timestamp that cannot be found in the frame
                    expectedTimestampIdx = Long.MIN_VALUE;
                    rowLo = 0;

                    int idx = rnd.nextInt(timestamps.length);
                    timestampLo = timestamps[idx] + 1;
                    timestampHi = idx >= (timestamps.length - 1) ? Long.MAX_VALUE : timestamps[idx + 1] - 1;
                    if (timestampHi < timestampLo) {
                        continue;
                    }
                } else {
                    expectedTimestampIdx = rnd.nextInt(timestamps.length);
                    long expectedTimestamp = timestamps[(int) expectedTimestampIdx];
                    if (expectedTimestampIdx > 0) {
                        long prev = timestamps[(int) expectedTimestampIdx - 1];
                        long diff = expectedTimestamp - prev;
                        timestampLo = diff > 0 ? prev + rnd.nextPositiveLong() % diff + 1 : prev;
                    } else {
                        timestampLo = -1;
                    }

                    if (expectedTimestampIdx > 0 && rnd.nextBoolean()) {
                        rowLo = rnd.nextPositiveLong() % expectedTimestampIdx;
                    } else {
                        rowLo = 0;
                    }

                    timestampHi = expectedTimestamp + rnd.nextPositiveLong() % Long.MAX_VALUE;
                    if (timestampHi < 0) {
                        timestampHi = Long.MAX_VALUE;
                    }
                }

                cursor.next();
                cursor.open();
                long actual = helper.binarySearch(timestampLo, timestampHi, rowLo);

                Assert.assertEquals(
                        String.format("Assertion failed at iteration %d - expected %d but got %d", i, expectedTimestampIdx, actual),
                        expectedTimestampIdx,
                        actual
                );
            }
        }
    }

    @Test
    public void testBinarySearchReturnsFirstDuplicateTimestamp() {
        long[] timestamps = new long[80];
        Arrays.fill(timestamps, 8);

        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(timestamps)), 0);
        TimeFrameHelper helper = new TimeFrameHelper(1, 1);
        helper.of(cursor);

        cursor.next();
        cursor.open();

        Assert.assertEquals(0, helper.binarySearch(7, 9, 0));
        Assert.assertEquals(Long.MIN_VALUE, helper.binarySearch(5, 6, 0));
    }

    @Test
    public void testFindRowLoHonorsTimestampScale() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(1, 2, 3)), 0);
        TimeFrameHelper helper = new TimeFrameHelper(1, 1_000);
        helper.of(cursor);

        long row = helper.findRowLo(2_000, 2_500);

        Assert.assertEquals(1, row);
        Assert.assertEquals(2, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoReturnsFirstRowWhenFrameStartsInsideInterval() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30)), 0);
        TimeFrameHelper helper = new TimeFrameHelper(2, 1);
        helper.of(cursor);

        long row = helper.findRowLo(5, 15);

        Assert.assertEquals(0, row);
        Assert.assertEquals(10, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(1, cursor.getOpenCount(0));
    }

    @Test
    public void testFindRowLoReturnsMinValueWhenNoValueInInterval() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30)), 0);
        TimeFrameHelper helper = new TimeFrameHelper(1, 1);
        helper.of(cursor);

        Assert.assertEquals(Long.MIN_VALUE, helper.findRowLo(65, 70));
        Assert.assertEquals(0, cursor.getOpenCount(0));
    }

    @Test
    public void testFindRowLoSkipsFramesOutsideInterval() {
        FrameData leftFrame = frame(new long[]{1, 2}, 1, 2);
        FrameData targetFrame = frame(new long[]{10, 20, 30}, 10, 35);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(leftFrame, targetFrame), 0);
        TimeFrameHelper helper = new TimeFrameHelper(2, 1);
        helper.of(cursor);

        long row = helper.findRowLo(15, 25);

        Assert.assertEquals(0, cursor.getOpenCount(0));
        Assert.assertEquals(1, cursor.getOpenCount(1));
        Assert.assertEquals(1, row);
        Assert.assertEquals(20, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testNextFrameUsesTimestampHighBoundary() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30)), 0);
        TimeFrameHelper helper = new TimeFrameHelper(1, 1);
        helper.of(cursor);

        Assert.assertFalse(helper.nextFrame(5));

        helper.toTop();
        Assert.assertTrue(helper.nextFrame(25));
        Assert.assertEquals(1, cursor.getOpenCount(0));
    }

    @Test
    public void testToTopClearsBookmark() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30, 40)), 0);
        TimeFrameHelper helper = new TimeFrameHelper(1, 1);
        helper.of(cursor);

        helper.findRowLo(5, 15);
        helper.toTop();
        int jumpCountBefore = cursor.getJumpCount();

        helper.findRowLo(25, 35);

        Assert.assertEquals(jumpCountBefore, cursor.getJumpCount());
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));
    }

    private static FrameData frame(long... timestamps) {
        return frame(timestamps, timestamps[0], timestamps[timestamps.length - 1]);
    }

    private static FrameData frame(long[] timestamps, long estimateLo, long estimateHi) {
        return new FrameData(timestamps, estimateLo, estimateHi);
    }

    private static final class FrameData {
        private final long actualHi;
        private final long actualLo;
        private final long estimateHi;
        private final long estimateLo;
        private final long[] timestamps;

        FrameData(long[] timestamps, long estimateLo, long estimateHi) {
            this.timestamps = timestamps;
            this.estimateLo = estimateLo;
            this.estimateHi = estimateHi;
            this.actualLo = timestamps[0];
            this.actualHi = timestamps[timestamps.length - 1] + 1;
        }
    }

    private static final class MockRecord implements Record {
        private final MockTimeFrameCursor cursor;
        private int frameIndex = -1;
        private int rowIndex = -1;

        MockRecord(MockTimeFrameCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public long getLong(int col) {
            if (frameIndex < 0 || rowIndex < 0) {
                throw new IllegalStateException("Record not positioned");
            }
            return cursor.timestampAt(frameIndex, rowIndex, col);
        }

        void position(int frameIndex, int rowIndex) {
            this.frameIndex = frameIndex;
            this.rowIndex = rowIndex;
        }
    }

    private static final class MockTimeFrameCursor implements TimeFrameCursor {
        private final List<FrameData> frames;
        private final List<Integer> jumpHistory = new ArrayList<>();
        private final int[] openCounts;
        private final MockRecord record;
        private final TimeFrame timeFrame = new TimeFrame();
        private final int timestampIndex;
        private int currentFrame = -1;

        MockTimeFrameCursor(List<FrameData> frames, int timestampIndex) {
            this.frames = frames;
            this.timestampIndex = timestampIndex;
            this.record = new MockRecord(this);
            this.openCounts = new int[frames.size()];
            toTop();
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public TimeFrame getTimeFrame() {
            return timeFrame;
        }

        @Override
        public int getTimestampIndex() {
            return timestampIndex;
        }

        @Override
        public void jumpTo(int frameIndex) {
            currentFrame = frameIndex;
            jumpHistory.add(frameIndex);
            loadEstimate(frameIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public boolean next() {
            int frameIndex = timeFrame.getFrameIndex() + 1;
            if (frameIndex < frames.size()) {
                currentFrame = frameIndex;
                loadEstimate(frameIndex);
                return true;
            }
            timeFrame.ofEstimate(frames.size(), Long.MIN_VALUE, Long.MIN_VALUE);
            currentFrame = -1;
            return false;
        }

        @Override
        public long open() {
            if (currentFrame < 0 || currentFrame >= frames.size()) {
                return 0;
            }
            FrameData data = frames.get(currentFrame);
            timeFrame.ofOpen(data.actualLo, data.actualHi, 0, data.timestamps.length);
            openCounts[currentFrame]++;
            return data.timestamps.length;
        }

        @Override
        public boolean prev() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordAt(Record record, long rowId) {
            int frameIndex = Rows.toPartitionIndex(rowId);
            int rowIndex = (int) Rows.toLocalRowID(rowId);
            currentFrame = frameIndex;
            loadEstimate(frameIndex);
            FrameData data = frames.get(frameIndex);
            timeFrame.ofOpen(data.actualLo, data.actualHi, 0, data.timestamps.length);
            ((MockRecord) record).position(frameIndex, rowIndex);
        }

        @Override
        public void recordAtRowIndex(Record record, long rowIndex) {
            int frameIndex = currentFrame >= 0 ? currentFrame : timeFrame.getFrameIndex();
            ((MockRecord) record).position(frameIndex, (int) rowIndex);
        }

        @Override
        public void toTop() {
            timeFrame.clear();
            currentFrame = -1;
        }

        private void loadEstimate(int frameIndex) {
            FrameData data = frames.get(frameIndex);
            timeFrame.ofEstimate(frameIndex, data.estimateLo, data.estimateHi);
        }

        int getJumpCount() {
            return jumpHistory.size();
        }

        int getOpenCount(int frameIndex) {
            return openCounts[frameIndex];
        }

        long timestampAt(int frameIndex, int rowIndex, int columnIndex) {
            if (columnIndex != timestampIndex) {
                throw new IllegalArgumentException("Unexpected column index: " + columnIndex);
            }
            return frames.get(frameIndex).timestamps[rowIndex];
        }
    }
}
