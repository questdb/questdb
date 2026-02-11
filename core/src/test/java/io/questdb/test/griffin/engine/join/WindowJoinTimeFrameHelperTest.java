/*******************************************************************************
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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.engine.join.WindowJoinTimeFrameHelper;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WindowJoinTimeFrameHelperTest {

    @Test
    public void testBinarySearchFuzz() {
        final int N = 10_000;
        Rnd rnd = TestUtils.generateRandom(null);

        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(1, 1);
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
                long actual = helper.binarySearch(timestampLo, timestampHi, rowLo, false);

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
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(1, 1);
        helper.of(cursor);

        cursor.next();
        cursor.open();

        Assert.assertEquals(0, helper.binarySearch(7, 9, 0, false));
        Assert.assertEquals(Long.MIN_VALUE, helper.binarySearch(5, 6, 0, true));
    }

    @Test
    public void testFindRowLoHonorsTimestampScale() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(1, 2, 3)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(1, 1_000);
        helper.of(cursor);

        long row = helper.findRowLo(2_000, 2_500);

        Assert.assertEquals(1, row);
        Assert.assertEquals(2, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoPrevailingCandidateAllRowsBelowTimestampLo() {
        // Test case: all rows in frame are < timestampLo
        // Frame has [10, 20, 30], search for [50, 60]
        // Prevailing should be the last row (timestamp 30)
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(10, 1);
        helper.of(cursor);
        long row = helper.findRowLo(50, 60, true);
        Assert.assertEquals(Long.MIN_VALUE, row);
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(2, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateAtFrameStart() {
        FrameData frame1 = frame(new long[]{10, 20}, 10, 20);
        FrameData frame2 = frame(new long[]{30, 40, 50}, 30, 50);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);

        long row = helper.findRowLo(25, 45, true);
        Assert.assertEquals(0, row);
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(1, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateBasic() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30, 40)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLo(25, 35, true);
        Assert.assertEquals(2, row);
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(1, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateBinarySearchReturnsMinValue() {
        // Test case: binarySearch finds first row >= timestampLo but it's > timestampHi
        // Large frame to trigger binary search
        long[] timestamps = new long[100];
        for (int i = 0; i < 100; i++) {
            timestamps[i] = i * 10;
        }
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(timestamps, 0, 990)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1); // small lookahead to trigger binary search
        helper.of(cursor);
        // Search for [555, 558] - first >= 555 is 560 (row 56), but 560 > 558
        long row = helper.findRowLo(555, 558, true);
        Assert.assertEquals(Long.MIN_VALUE, row);
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(55, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateCrossFrame() {
        FrameData frame1 = frame(new long[]{10, 20, 30}, 10, 30);
        FrameData frame2 = frame(new long[]{100, 110, 120}, 100, 120);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLo(50, 110, true);
        Assert.assertEquals(0, row);
        Assert.assertEquals(100, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(2, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateExactMatch() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30, 40)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLo(20, 35, true);
        Assert.assertEquals(1, row);
        Assert.assertEquals(20, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(0, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateFrameStartsAfterTimestampLo() {
        // Test case: frame.timestampLo > timestampLo, but frame.timestampLo <= timestampHi
        // Frame1: [10, 20], Frame2: [40, 50, 60], search for [30, 100]
        // frame2.timestampLo (40) > timestampLo (30), return first row of frame2
        // Prevailing should be frame1's last row (timestamp 20)
        FrameData frame1 = frame(new long[]{10, 20}, 10, 20);
        FrameData frame2 = frame(new long[]{40, 50, 60}, 40, 60);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(10, 1);
        helper.of(cursor);
        long row = helper.findRowLo(30, 100, true);
        Assert.assertEquals(0, row);
        Assert.assertEquals(40, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(1, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateFrameStartsAtTimestampLo() {
        // Test case: frame.timestampLo >= timestampLo, so we return first row
        // Frame1: [10, 20], Frame2: [30, 40, 50], search for [30, 100]
        // frame2.timestampLo (30) >= timestampLo (30), return first row of frame2
        // Prevailing should be frame1's last row (timestamp 20)
        FrameData frame1 = frame(new long[]{10, 20}, 10, 20);
        FrameData frame2 = frame(new long[]{30, 40, 50}, 30, 50);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(10, 1);
        helper.of(cursor);

        long row = helper.findRowLo(30, 100, true);
        Assert.assertEquals(0, row); // first row of frame2 (local index)
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));
        // Prevailing should be frame1's last row
        Assert.assertEquals(0, helper.getPrevailingFrameIndex()); // frame1
        Assert.assertEquals(1, helper.getPrevailingRowIndex()); // frame1's last row (local index)
    }

    @Test
    public void testFindRowLoPrevailingCandidateLinearScanReturnsMinValue() {
        // Test case: linearScan finds first row >= timestampLo but it's > timestampHi
        // Frame has [10, 20, 30], search for [15, 18] - first >= 15 is 20, but 20 > 18
        // Prevailing should be the row before 20, which is row 0 (timestamp 10)
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(10, 1); // large lookahead to use linearScan
        helper.of(cursor);
        long row = helper.findRowLo(15, 18, true);
        Assert.assertEquals(Long.MIN_VALUE, row);
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(0, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateMultipleFramesNoDataInRange() {
        FrameData frame1 = frame(new long[]{10, 20}, 10, 20);
        FrameData frame2 = frame(new long[]{30, 40}, 30, 40);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLo(50, 60, true);
        Assert.assertEquals(Long.MIN_VALUE, row);
        Assert.assertEquals(1, helper.getPrevailingFrameIndex());
        Assert.assertEquals(1, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateNoDataInRange() {
        FrameData frame1 = frame(new long[]{10, 20, 30}, 10, 30);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame1), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLo(50, 60, true);
        Assert.assertEquals(Long.MIN_VALUE, row);
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(2, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateNoPrevailing() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);

        long row = helper.findRowLo(5, 15, true);
        Assert.assertEquals(0, row);
        Assert.assertEquals(10, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(-1, helper.getPrevailingFrameIndex());
        Assert.assertEquals(Long.MIN_VALUE, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateNoPreviousFrame() {
        // Test case: no previous frame, frame.timestampLo >= timestampLo
        // Frame: [30, 40, 50], search for [30, 100]
        // frame.timestampLo (30) >= timestampLo (30), return first row
        // No prevailing because there's no previous frame
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(30, 40, 50)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(10, 1);
        helper.of(cursor);

        long row = helper.findRowLo(30, 100, true);
        Assert.assertEquals(0, row); // first row
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(-1, helper.getPrevailingFrameIndex());
        Assert.assertEquals(Long.MIN_VALUE, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoPrevailingCandidateWithBinarySearch() {
        long[] timestamps = new long[100];
        for (int i = 0; i < 100; i++) {
            timestamps[i] = i * 10;
        }
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(timestamps, 0, 990)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1); // small lookahead to trigger binary search
        helper.of(cursor);
        long row = helper.findRowLo(555, 600, true);
        Assert.assertEquals(56, row);
        Assert.assertEquals(560, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(0, helper.getPrevailingFrameIndex());
        Assert.assertEquals(55, helper.getPrevailingRowIndex());
    }

    @Test
    public void testFindRowLoReturnsFirstRowWhenFrameStartsInsideInterval() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);

        long row = helper.findRowLo(5, 15);

        Assert.assertEquals(0, row);
        Assert.assertEquals(10, helper.getRecord().getTimestamp(0));
        Assert.assertEquals(1, cursor.getOpenCount(0));
    }

    @Test
    public void testFindRowLoReturnsMinValueWhenNoValueInInterval() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(1, 1);
        helper.of(cursor);

        Assert.assertEquals(Long.MIN_VALUE, helper.findRowLo(65, 70));
        Assert.assertEquals(0, cursor.getOpenCount(0));
    }

    @Test
    public void testFindRowLoSkipsFramesOutsideInterval() {
        FrameData leftFrame = frame(new long[]{1, 2}, 1, 2);
        FrameData targetFrame = frame(new long[]{10, 20, 30}, 10, 35);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(leftFrame, targetFrame), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);

        long row = helper.findRowLo(15, 25);

        Assert.assertEquals(0, cursor.getOpenCount(0));
        Assert.assertEquals(1, cursor.getOpenCount(1));
        Assert.assertEquals(1, row);
        Assert.assertEquals(20, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithIncludePrevailingNoPrevailing() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30, 40)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLo(5, 15);
        Assert.assertEquals(0, row);
        Assert.assertEquals(10, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithIncludePrevailingReturnsExactMatch() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30, 40)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLoWithPrevailing(20, 35);
        helper.recordAtRowIndex(row);

        Assert.assertEquals(1, row);
        Assert.assertEquals(20, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithIncludePrevailingReturnsPrevailing() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30, 40)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLoWithPrevailing(25, 35);
        helper.recordAtRowIndex(row);

        Assert.assertEquals(1, row);
        Assert.assertEquals(20, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingBinarySearch() {
        long[] timestamps = new long[100];
        for (int i = 0; i < 100; i++) {
            timestamps[i] = i * 10;
        }
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(timestamps, 0, 990)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1); // trigger binary search
        helper.of(cursor);
        long row = helper.findRowLoWithPrevailing(555, 600);
        helper.recordAtRowIndex(row);

        Assert.assertEquals(55, row);
        Assert.assertEquals(550, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingCrossFrame() {
        FrameData frame1 = frame(new long[]{10, 20}, 10, 20);
        FrameData frame2 = frame(new long[]{100, 200, 300}, 100, 300);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLoWithPrevailing(50, 150);
        helper.recordAtRowIndex(row);

        Assert.assertEquals(1, row);
        Assert.assertEquals(20, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingCrossFrameAllFramesBeforeInterval() {
        FrameData frame1 = frame(new long[]{10, 20}, 10, 20);
        FrameData frame2 = frame(new long[]{30, 40}, 30, 40);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLoWithPrevailing(50, 60);
        helper.recordAtRowIndex(row);

        Assert.assertEquals(1, row);
        Assert.assertEquals(40, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingCrossFrameBookmark() {
        FrameData frame1 = frame(new long[]{10, 20, 30}, 10, 30);
        FrameData frame2 = frame(new long[]{40, 50, 60}, 40, 60);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);

        long row = helper.findRowLoWithPrevailing(35, 55);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(2, row);
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));

        row = helper.findRowLoWithPrevailing(40, 55);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(0, row);
        Assert.assertEquals(40, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingCrossFrameExactMatchInSecondFrame() {
        FrameData frame1 = frame(new long[]{10, 20}, 10, 20);
        FrameData frame2 = frame(new long[]{100, 200, 300}, 100, 300);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);

        long row = helper.findRowLoWithPrevailing(100, 250);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(0, row);
        Assert.assertEquals(100, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingCrossFrameNoPrevailing() {
        FrameData frame1 = frame(new long[]{10, 20}, 10, 20);
        FrameData frame2 = frame(new long[]{100, 200, 300}, 100, 300);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLoWithPrevailing(5, 15);
        helper.recordAtRowIndex(row);

        Assert.assertEquals(0, row);
        Assert.assertEquals(10, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingCrossFrameThreeFrames() {
        FrameData frame1 = frame(new long[]{10, 20}, 10, 20);
        FrameData frame2 = frame(new long[]{50, 60}, 50, 60);
        FrameData frame3 = frame(new long[]{100, 200, 300}, 100, 300);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2, frame3), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        long row = helper.findRowLoWithPrevailing(80, 150);
        helper.recordAtRowIndex(row);

        Assert.assertEquals(1, row);
        Assert.assertEquals(60, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingEmptyInterval() {
        // Interval doesn't intersect with any frame but prevailing exists
        FrameData frame1 = frame(new long[]{10, 20, 30}, 10, 30);
        FrameData frame2 = frame(new long[]{100, 110, 120}, 100, 120);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);
        // Interval in gap between frames
        long row = helper.findRowLoWithPrevailing(50, 80);
        helper.recordAtRowIndex(row);

        Assert.assertEquals(2, row); // frame1's last row as prevailing
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingFuzz() {
        final int N = 10_000;
        Rnd rnd = TestUtils.generateRandom(null);

        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        for (int i = 0; i < N; i++) {
            var timestamps = new long[rnd.nextInt(1000) + 1];
            for (int j = 0; j < timestamps.length; j++) {
                timestamps[j] = rnd.nextPositiveLong() % Long.MAX_VALUE;
            }
            Arrays.sort(timestamps);

            try (var cursor = new MockTimeFrameCursor(List.of(frame(timestamps, timestamps[0], timestamps[timestamps.length - 1])), 0)) {
                helper.of(cursor);
                long timestampLo = rnd.nextPositiveLong() % (timestamps[timestamps.length - 1] + 100);
                long timestampHi = timestampLo + rnd.nextPositiveLong() % 1000;
                long expectedRowIdx = Long.MIN_VALUE;
                for (int j = timestamps.length - 1; j >= 0; j--) {
                    if (timestamps[j] <= timestampLo) {
                        expectedRowIdx = j;
                        break;
                    }
                }
                if (expectedRowIdx == Long.MIN_VALUE) {
                    for (int j = 0; j < timestamps.length; j++) {
                        if (timestamps[j] >= timestampLo && timestamps[j] <= timestampHi) {
                            expectedRowIdx = j;
                            break;
                        }
                    }
                }

                long actual = helper.findRowLoWithPrevailing(timestampLo, timestampHi);

                Assert.assertEquals(
                        String.format("Assertion failed at iteration %d - expected %d but got %d (timestampLo=%d, timestampHi=%d)",
                                i, expectedRowIdx, actual, timestampLo, timestampHi),
                        expectedRowIdx,
                        actual
                );
            }
        }
    }

    @Test
    public void testFindRowLoWithPrevailingHonorsTimestampScale() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(1, 2, 3)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1_000);
        helper.of(cursor);
        long row = helper.findRowLoWithPrevailing(2_500, 3_500);
        helper.recordAtRowIndex(row);

        Assert.assertEquals(1, row);
        Assert.assertEquals(2, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingMultiFrameEdgeCases() {
        FrameData frame1 = frame(new long[]{10, 20, 30}, 10, 30);
        FrameData frame2 = frame(new long[]{40, 50, 60}, 40, 60);
        FrameData frame3 = frame(new long[]{70, 80, 90}, 70, 90);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2, frame3), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);

        // timestampLo equals frame1's end
        long row = helper.findRowLoWithPrevailing(30, 45);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(2, row);
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));

        // timestampLo equals frame2's start (exact match)
        row = helper.findRowLoWithPrevailing(40, 55);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(0, row);
        Assert.assertEquals(40, helper.getRecord().getTimestamp(0));

        // timestampLo in gap after frame2
        row = helper.findRowLoWithPrevailing(65, 75);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(2, row);
        Assert.assertEquals(60, helper.getRecord().getTimestamp(0));

        // timestampLo equals frame3's start (exact match)
        row = helper.findRowLoWithPrevailing(70, 85);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(0, row);
        Assert.assertEquals(70, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testFindRowLoWithPrevailingMultiFrameSequentialBookmark() {
        FrameData frame1 = frame(new long[]{10, 20, 30}, 10, 30);
        FrameData frame2 = frame(new long[]{40, 50, 60}, 40, 60);
        FrameData frame3 = frame(new long[]{100, 110, 120}, 100, 120);
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(Arrays.asList(frame1, frame2, frame3), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(2, 1);
        helper.of(cursor);

        // 1. No prevailing, return first in interval
        long row = helper.findRowLoWithPrevailing(5, 15);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(0, row);
        Assert.assertEquals(10, helper.getRecord().getTimestamp(0));

        // 2. Exact match at frame1 boundary
        row = helper.findRowLoWithPrevailing(10, 25);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(0, row);
        Assert.assertEquals(10, helper.getRecord().getTimestamp(0));

        // 3. In middle of frame1
        row = helper.findRowLoWithPrevailing(15, 25);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(0, row);
        Assert.assertEquals(10, helper.getRecord().getTimestamp(0));

        // 4. Exact match at frame1's last row
        row = helper.findRowLoWithPrevailing(30, 45);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(2, row);
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));

        // 5. Gap between frame1 and frame2
        row = helper.findRowLoWithPrevailing(35, 45);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(2, row);
        Assert.assertEquals(30, helper.getRecord().getTimestamp(0));

        // 6. Exact match at frame2 start
        row = helper.findRowLoWithPrevailing(40, 55);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(0, row);
        Assert.assertEquals(40, helper.getRecord().getTimestamp(0));

        // 7. Exact match at frame2 end
        row = helper.findRowLoWithPrevailing(60, 80);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(2, row);
        Assert.assertEquals(60, helper.getRecord().getTimestamp(0));

        // 8. Large gap between frame2 and frame3
        row = helper.findRowLoWithPrevailing(80, 105);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(2, row);
        Assert.assertEquals(60, helper.getRecord().getTimestamp(0));

        // 9. Exact match at frame3 start
        row = helper.findRowLoWithPrevailing(100, 115);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(0, row);
        Assert.assertEquals(100, helper.getRecord().getTimestamp(0));

        // 10. Past all frames
        row = helper.findRowLoWithPrevailing(150, 200);
        helper.recordAtRowIndex(row);
        Assert.assertEquals(2, row);
        Assert.assertEquals(120, helper.getRecord().getTimestamp(0));
    }

    @Test
    public void testNextFrameUsesTimestampHighBoundary() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(1, 1);
        helper.of(cursor);

        Assert.assertFalse(helper.nextFrame(5));

        helper.toTop();
        Assert.assertTrue(helper.nextFrame(25));
        Assert.assertEquals(1, cursor.getOpenCount(0));
    }

    @Test
    public void testToTopClearsBookmark() {
        MockTimeFrameCursor cursor = new MockTimeFrameCursor(List.of(frame(10, 20, 30, 40)), 0);
        WindowJoinTimeFrameHelper helper = new WindowJoinTimeFrameHelper(1, 1);
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
