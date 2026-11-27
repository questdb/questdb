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

package io.questdb.test.griffin.model;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;

import java.util.Random;

public class IntervalUtilsTest {
    private static final Log LOG = LogFactory.getLog(IntervalUtilsTest.class);
    private final StringSink sink = new StringSink();

    @Test
    public void testIntersectEmpty() {
        LongList intervals = new LongList();
        // A

        // B
        add(intervals, -20, -2);
        add(intervals, 1, 2);

        runTestIntersectInplace(intervals, 0, "");
    }

    @Test
    public void testIntersectInplace() {
        LongList intervals = new LongList();
        // A
        add(intervals, -1, 10);

        // B
        add(intervals, 1, 2);
        add(intervals, 3, 4);

        runTestIntersectInplace(intervals, 2, "[1,4]");
    }

    @Test
    public void testIntersectInplace2() {
        LongList intervals = new LongList();
        // A
        add(intervals, -1, 10);

        // B
        add(intervals, 1, 2);
        add(intervals, 4, 5);

        runTestIntersectInplace(intervals, 2, "[1,2], [4,5]");
    }

    @Test
    public void testIntersectInplace3() {
        LongList intervals = new LongList();
        // A
        add(intervals, -5, -3);
        add(intervals, -1, 10);
        add(intervals, 12, 13);

        // B
        add(intervals, -20, -10);
        add(intervals, 1, 2);
        add(intervals, 4, 5);
        add(intervals, 7, 7);
        add(intervals, 9, 12);

        runTestIntersectInplace(intervals, 6, "[1,2], [4,5], [7,7], [9,10], [12,12]");
    }

    @Test
    public void testIntersectInplace4() {
        LongList intervals = new LongList();
        // A
        add(intervals, -5, -3);
        add(intervals, -1, 10);
        add(intervals, 12, 13);

        // B
        add(intervals, -20, -2);
        add(intervals, 1, 2);
        add(intervals, 4, 5);
        add(intervals, 7, 7);
        add(intervals, 9, 12);

        runTestIntersectInplace(intervals, 6, "[-5,-3], [1,2], [4,5], [7,7], [9,10], [12,12]");
    }

    @Test
    public void testIntersectInplaceLong() {
        LongList intervals = new LongList();
        // A
        add(intervals, -1, 10);
        add(intervals, 12, 13);

        // B
        add(intervals, 1, 2);
        add(intervals, 4, 5);
        add(intervals, 7, 7);
        add(intervals, 9, 12);

        runTestIntersectInplace(intervals, 4, "[1,2], [4,5], [7,7], [9,10], [12,12]");
    }

    @Test
    public void testIntersectInplaceCopiedIntervalAboveNextB() {
        // This test covers line 328 in intersectInPlace:
        // When an A interval is copied to the end (because it would be overwritten),
        // and the copied interval is then compared with a later B interval
        // where aHi < bLo (a fully above b), we increment aUpper.
        LongList intervals = new LongList();
        // A
        add(intervals, 0, 5);
        add(intervals, 10, 15);

        // B
        add(intervals, 0, 2);  // intersects with A[0]
        add(intervals, 20, 25);  // no intersection with A[1] or copied A[0]

        // When A[0] intersects B[0], A[0] is copied to end since writePoint==aLower.
        // Then the copied interval [0,5] is compared with B[1]=[20,25].
        // Since 5 < 20, aHi < bLo triggers the aUpper++ branch.
        runTestIntersectInplace(intervals, 4, "[0,2]");
    }

    @Test
    public void testIntersectInplaceCopiedIntervalPartialOverlap() {
        // This test covers line 341 in intersectInPlace:
        // When a copied A interval intersects a B interval where aHi < bHi
        // (b hanging lower than a), we increment aUpper.
        LongList intervals = new LongList();
        // A
        add(intervals, 0, 5);
        add(intervals, 100, 110);

        // B
        add(intervals, 0, 2);  // intersects A[0], aHi(5) >= bHi(2), so intervalB++, A[0] copied
        add(intervals, 4, 20); // intersects copied A[0]=[0,5] where aHi(5) < bHi(20)

        // Iteration 1: A[0]=[0,5] intersects B[0]=[0,2]. Since aHi >= bHi, intervalB++.
        //              A[0] is copied to end, aUpperSize increases.
        // Iteration 2: Copied interval [0,5] intersects B[1]=[4,20].
        //              aHi(5) < bHi(20), so aUpper++ is triggered (line 341).
        // Note: gap between [0,2] and [4,5] prevents merging by append().
        runTestIntersectInplace(intervals, 4, "[0,2], [4,5]");
    }

    @Test
    public void testIntersectRandomInplaceVsNonInplace() {
        long seed = System.currentTimeMillis();
        Random r = new Random(seed);
        LongList intervals = new LongList();
        int aSize = r.nextInt(100) + 10;
        int bSize = r.nextInt(100);

        long aPos = r.nextInt(1000) - r.nextInt(1000);
        long bPos = r.nextInt(1000) - r.nextInt(1000);

        // A
        for (int i = 0; i < aSize; i++) {
            add(intervals, aPos, aPos += r.nextInt(100));
            aPos += r.nextInt(100);
        }

        // B
        LongList bIntervals = new LongList();
        for (int i = 0; i < bSize; i++) {
            add(bIntervals, bPos, bPos += r.nextInt(100));
            bPos += r.nextInt(100);
        }

        LongList expected = new LongList();
        // non-in place algo, supposed to be correct
        intersect(intervals, bIntervals, expected);
        String expectedStr = toIntervalString(expected, 0);

        intervals.add(bIntervals);
        runTestIntersectInplace(intervals, aSize * 2, expectedStr);
    }

    @Test
    public void testInvertEmpty() {
        // the complement of no interval at all is every timestamp, see testInvertFullRange() for back
        runTestInvertInplace(new LongList(), 0, "[null,9223372036854775807]");

        LongList intervals = new LongList();
        // A
        add(intervals, 1, 2);

        // B is empty
        runTestInvertInplace(intervals, 2, "[null,9223372036854775807]");
    }

    @Test
    public void testInvertFullRange() {
        LongList intervals = new LongList();
        // B
        add(intervals, Long.MIN_VALUE, Long.MAX_VALUE);

        IntervalUtils.invert(intervals, 0);
        Assert.assertEquals(0, intervals.size());
    }

    @Test
    public void testInvertSimple() {
        LongList intervals = new LongList();
        // A
        add(intervals, -20, -2);

        // B
        add(intervals, 1, 2);

        runTestInvertInplace(intervals, 2, "[null,0], [3,9223372036854775807]");
    }

    @Test
    public void testInvertWithNegativeInfinity() {
        LongList intervals = new LongList();
        // A
        intervals.add(1);

        // B
        add(intervals, 2, 100);
        add(intervals, 200, Long.MAX_VALUE);

        runTestInvertInplace(intervals, 1, "[null,1], [101,199]");
    }

    @Test
    public void testInvertWithPositiveInfinity() {
        LongList intervals = new LongList();
        // A

        // B
        add(intervals, Long.MIN_VALUE, 2);
        add(intervals, 100, 200);

        runTestInvertInplace(intervals, 0, "[3,99], [201,9223372036854775807]");
    }

    @Test
    public void testIsInEmptyIntervalList() {
        LongList intervals = new LongList();
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 123));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 123));
    }

    @Test
    public void testIsInListWithEvenNumberOfIntervals() {
        LongList intervals = new LongList();
        add(intervals, 100, 102);
        add(intervals, 122, 124);

        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 99));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 99));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 101));
        Assert.assertEquals(0, IntervalUtils.findInterval(intervals, 101));

        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 103));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 103));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 122));
        Assert.assertEquals(1, IntervalUtils.findInterval(intervals, 122));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 123));
        Assert.assertEquals(1, IntervalUtils.findInterval(intervals, 123));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 124));
        Assert.assertEquals(1, IntervalUtils.findInterval(intervals, 124));

        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 125));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 125));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 100));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 102));
    }

    @Test
    public void testIsInListWithOddNumberOfIntervals() {
        LongList intervals = new LongList();
        add(intervals, 100, 102);
        add(intervals, 122, 124);
        add(intervals, 150, 155);

        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 99));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 99));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 101));
        Assert.assertEquals(0, IntervalUtils.findInterval(intervals, 101));

        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 103));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 103));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 123));
        Assert.assertEquals(1, IntervalUtils.findInterval(intervals, 123));

        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 125));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 125));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 151));
        Assert.assertEquals(2, IntervalUtils.findInterval(intervals, 151));

        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 156));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 156));
    }

    @Test
    public void testIsInListWithOneInterval() {
        LongList intervals = new LongList();
        add(intervals, 100, 102);

        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 99));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 99));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 100));
        Assert.assertEquals(0, IntervalUtils.findInterval(intervals, 100));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 101));
        Assert.assertEquals(0, IntervalUtils.findInterval(intervals, 101));

        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 102));
        Assert.assertEquals(0, IntervalUtils.findInterval(intervals, 102));

        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 103));
        Assert.assertEquals(-1, IntervalUtils.findInterval(intervals, 103));
    }

    @Test
    public void testLastAContainsWhoelBUnionAllAfterB() {
        LongList intervals = new LongList();
        // A
        add(intervals, 1, 2);
        add(intervals, 3, 4);
        add(intervals, 50, 250);

        // B
        add(intervals, 100, 101);
        add(intervals, 200, 201);
        add(intervals, 205, 206);

        runTestUnionInPlace(intervals, 6, "[1,2], [3,4], [50,250]");
    }

    @Test
    public void testParseFloorPartialTimestamp_truncateNanos() throws NumericException {
        long expected = MicrosTimestampDriver.floor("2019-01-01T00:00:00.123456Z");
        assertParseFloorPartialTimestampEquals(expected, "2019-01-01T00:00:00.123456789Z");
        assertParseFloorPartialTimestampEquals(expected, "2019-01-01T00:00:00.12345678Z");
        assertParseFloorPartialTimestampEquals(expected, "2019-01-01T00:00:00.1234567Z");

        // with offset
        expected = MicrosTimestampDriver.floor("2019-01-01T00:00:00.123456+01:00");
        assertParseFloorPartialTimestampEquals(expected, "2019-01-01T00:00:00.123456789+01:00");
        assertParseFloorPartialTimestampEquals(expected, "2019-01-01T00:00:00.12345678+01:00");
        assertParseFloorPartialTimestampEquals(expected, "2019-01-01T00:00:00.1234567+01:00");
    }

    @Test
    public void testSortAndUnionInPlaceEmptyAndSingleBatch() {
        LongList intervals = new LongList();
        IntervalUtils.sortAndUnionInPlace(intervals, 0);
        Assert.assertEquals(0, intervals.size());

        add(intervals, 5, 6);
        IntervalUtils.sortAndUnionInPlace(intervals, 0);
        TestUtils.assertEquals("[5,6]", toIntervalString(intervals, 0));

        // a prefix with an empty batch is a no-op
        IntervalUtils.sortAndUnionInPlace(intervals, 2);
        TestUtils.assertEquals("[5,6]", toIntervalString(intervals, 0));
    }

    @Test(timeout = 300_000)
    public void testSortAndUnionInPlaceLargeAscendingBatch() {
        // complexity canary for the ascending IN-list / OR-ed disjunct shape: before
        // LongGroupSort switched to introsort with a sorted-input fast-path, this
        // already-sorted batch cost O(D^2) comparisons - minutes of CPU at this size.
        // The generous timeout, orders of magnitude above the healthy sub-second
        // runtime, turns such a regression into a prompt red failure instead of a
        // CI hang.
        final int count = 200_000;

        // disjoint ascending intervals stay untouched
        LongList intervals = new LongList(2 * count);
        for (int i = 0; i < count; i++) {
            intervals.add(10L * i, 10L * i + 5);
        }
        IntervalUtils.sortAndUnionInPlace(intervals, 0);
        Assert.assertEquals(2 * count, intervals.size());
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(10L * i, intervals.getQuick(2 * i));
            Assert.assertEquals(10L * i + 5, intervals.getQuick(2 * i + 1));
        }

        // overlapping ascending intervals collapse into a single one
        intervals.clear();
        for (int i = 0; i < count; i++) {
            intervals.add(2L * i, 2L * i + 3);
        }
        IntervalUtils.sortAndUnionInPlace(intervals, 0);
        Assert.assertEquals(2, intervals.size());
        Assert.assertEquals(0, intervals.getQuick(0));
        Assert.assertEquals(2L * (count - 1) + 3, intervals.getQuick(1));
    }

    @Test
    public void testSortAndUnionInPlaceMatchesIncrementalUnionRandomized() {
        Random rnd = new Random(42);
        for (int iter = 0; iter < 500; iter++) {
            LongList prefix = randomMergedRegion(rnd, 4);
            int batchIntervals = rnd.nextInt(12);
            LongList batch = new LongList();
            for (int i = 0; i < batchIntervals; i++) {
                long lo = rnd.nextInt(120);
                // unordered, overlapping, touching and duplicated intervals
                batch.add(lo, lo + rnd.nextInt(8));
            }

            // reference: pre-batching evaluation merged every interval incrementally
            LongList incremental = new LongList();
            incremental.add(prefix, 0, prefix.size());
            for (int i = 0; i < batch.size(); i += 2) {
                int divider = incremental.size();
                incremental.add(batch.getQuick(i), batch.getQuick(i + 1));
                IntervalUtils.unionInPlace(incremental, divider);
            }

            // batched: sort-and-union the batch, then merge once - must be identical
            LongList batched = new LongList();
            batched.add(prefix, 0, prefix.size());
            int runStart = batched.size();
            batched.add(batch, 0, batch.size());
            IntervalUtils.sortAndUnionInPlace(batched, runStart);
            IntervalUtils.unionInPlace(batched, runStart);

            Assert.assertEquals(
                    "iteration " + iter,
                    toIntervalString(incremental, 0),
                    toIntervalString(batched, 0)
            );
        }
    }

    @Test
    public void testSortAndUnionInPlacePreservesPrefixAndCoalescesBatch() {
        LongList intervals = new LongList();
        // merged prefix - must stay untouched even though it lies below the batch range
        add(intervals, 0, 1);
        // unordered batch
        add(intervals, 50, 60);
        add(intervals, 10, 20);
        add(intervals, 15, 30);
        add(intervals, 61, 70);
        add(intervals, 20, 25);
        IntervalUtils.sortAndUnionInPlace(intervals, 2);
        // [61,70] stays separate from [50,60]: like unionInPlace, adjacent intervals do not merge
        TestUtils.assertEquals("[0,1], [10,30], [50,60], [61,70]", toIntervalString(intervals, 0));
    }

    @Test
    public void testUnionAllAfterB() {
        LongList intervals = new LongList();
        // A
        add(intervals, 100, 101);
        add(intervals, 200, 201);
        add(intervals, 205, 206);

        // B
        add(intervals, 1, 2);
        add(intervals, 3, 4);
        add(intervals, 6, 7);

        runTestUnionInPlace(intervals, 6, "[1,2], [3,4], [6,7], [100,101], [200,201], [205,206]");
    }

    @Test
    public void testUnionAppendedIntervals() {
        // unionAppendedIntervals() leaves the list as unionBracketExpandedIntervals() from startIndex
        // does, where the intervals before mergedEnd are the output of an earlier union. It counts the
        // new intervals it sorts, the merged ones it merges them with, the merged ones after those that
        // it moves, and the merged ones it compares with to find where the new ones land: the last one,
        // and those of a binary search when the new ones sort before the last one.
        final long[] merged = {0, 9, 20, 29, 40, 49};
        // after all: the new one, [40,49], and the comparison with it
        assertUnionAppended(null, merged, new long[]{60, 70}, "[0,9], [20,29], [40,49], [60,70]", 3);
        // adjacent to the last one, overlapping it, and inside it
        assertUnionAppended(null, merged, new long[]{50, 55}, "[0,9], [20,29], [40,55]", 3);
        assertUnionAppended(null, merged, new long[]{45, 60}, "[0,9], [20,29], [40,60]", 3);
        assertUnionAppended(null, merged, new long[]{42, 44}, "[0,9], [20,29], [40,49]", 3);
        // between two: the new one, [0,9], [20,29] and [40,49] moved right, and the last one and two
        // of the binary search compared
        assertUnionAppended(null, merged, new long[]{12, 15}, "[0,9], [12,15], [20,29], [40,49]", 7);
        // bridging two: the new one, [0,9] and [20,29], [40,49] moved left, three compared
        assertUnionAppended(null, merged, new long[]{10, 19}, "[0,29], [40,49]", 7);
        // reaching every merged one after it, the last one by a chain after the new one
        assertUnionAppended(null, merged, new long[]{5, 45}, "[0,49]", 7);
        // before all: the new one, the three merged ones moved right, three compared
        assertUnionAppended(null, merged, new long[]{-5, -2}, "[-5,-2], [0,9], [20,29], [40,49]", 7);
        // adjacent to the first one, which keeps the number of intervals: nothing moves
        assertUnionAppended(null, merged, new long[]{-5, -1}, "[-5,9], [20,29], [40,49]", 5);
        // a duplicate: the new one, [0,9] and [20,29], three compared
        assertUnionAppended(null, merged, new long[]{20, 29}, "[0,9], [20,29], [40,49]", 6);
        // two new ones in reverse order, spread over the list: two sorted, three merged, three compared
        assertUnionAppended(null, merged, new long[]{60, 70, 12, 15}, "[0,9], [12,15], [20,29], [40,49], [60,70]", 8);
        // no new one
        assertUnionAppended(null, merged, new long[0], "[0,9], [20,29], [40,49]", 0);
        // no merged one: a union of all
        assertUnionAppended(null, new long[0], new long[]{20, 29, 0, 9, 5, 25}, "[0,29]", 3);
        // an inverted merged interval that the new one reaches by adjacency
        assertUnionAppended(null, new long[]{10, 5}, new long[]{8, 9}, "[8,9]", 3);
        assertUnionAppended(null, new long[]{0, 9}, new long[]{10, 12}, "[0,12]", 3);
        // a merged interval up to Long.MAX_VALUE takes every one after it, and one right before
        // Long.MAX_VALUE takes Long.MAX_VALUE by adjacency, without overflow
        assertUnionAppended(null, new long[]{0, 9, 20, Long.MAX_VALUE}, new long[]{30, 40}, "[0,9], [20," + Long.MAX_VALUE + "]", 3);
        assertUnionAppended(null, new long[]{0, 9, 20, Long.MAX_VALUE - 1}, new long[]{Long.MAX_VALUE, Long.MAX_VALUE}, "[0,9], [20," + Long.MAX_VALUE + "]", 3);
        // Long.MIN_VALUE renders as null
        assertUnionAppended(null, new long[]{Long.MIN_VALUE, -10}, new long[]{Long.MIN_VALUE, 5}, "[null,5]", 3);
        // the intervals before startIndex stay as they are, though the new one overlaps them
        assertUnionAppended(new long[]{5, 25}, merged, new long[]{12, 15}, "[5,25], [0,9], [12,15], [20,29], [40,49]", 7);
        // equal inverted intervals stay apart: the new one goes first, the two merged ones move right,
        // two compared
        assertUnionAppended(null, new long[]{10, 5, 10, 5}, new long[]{10, 5}, "[10,5], [10,5], [10,5]", 5);
        // the order is by lo, then by hi: an order by lo alone would put [10,6] first, or start the
        // merge at [10,20] and take the inverted [10,5] into it
        assertUnionAppended(null, new long[]{10, 5, 10, 8}, new long[]{10, 6}, "[10,5], [10,6], [10,8]", 5);
        assertUnionAppended(null, new long[]{10, 5}, new long[]{10, 20}, "[10,5], [10,20]", 3);
    }

    @Test
    public void testUnionAppendedIntervalsMatchesUnionOfAll() {
        // Random merged intervals, the output of a union, with new intervals after them that land
        // before, between, in and after them, touch them, bridge or absorb them, repeat them, are
        // inverted, or reach Long.MIN_VALUE or Long.MAX_VALUE, and intervals before startIndex.
        // unionAppendedIntervals() must leave the list exactly as a union of all intervals from
        // startIndex does. Both lists hold the same longs, also past their ends, so they compare the
        // same values where a union of no interval exposes two of those.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final LongList merged = new LongList();
        final LongList appended = new LongList();
        // the shapes, counted from the inputs
        int landingFirstCount = 0;
        int landingMiddleCount = 0;
        int landingLastCount = 0;
        int moreWithLaterCount = 0;
        int fewerWithLaterCount = 0;
        int sameWithLaterCount = 0;
        int chainCount = 0;
        int noAppendedCount = 0;
        int noMergedCount = 0;
        int invertedCount = 0;
        int minMaxCount = 0;
        int duplicateCount = 0;
        for (int i = 0; i < 100_000; i++) {
            merged.clear();
            for (int j = 0, n = rnd.nextInt(31); j < n; j++) {
                addRandomInterval(rnd, merged);
            }
            if (merged.size() > 0) {
                IntervalUtils.unionBracketExpandedIntervalsForTesting(merged, 0);
            }
            appended.clear();
            for (int j = 0, n = rnd.nextInt(7); j < n; j++) {
                addRandomAppendedInterval(rnd, merged, appended);
            }
            final LongList intervals = new LongList();
            final LongList reference = new LongList();
            for (int j = 0, n = rnd.nextInt(3); j < n; j++) {
                final long lo = rnd.nextInt(100);
                intervals.add(lo, lo + 5);
                reference.add(lo, lo + 5);
            }
            final int startIndex = intervals.size();
            intervals.addAll(merged);
            reference.addAll(merged);
            final int mergedEnd = intervals.size();
            intervals.addAll(appended);
            reference.addAll(appended);

            IntervalUtils.unionBracketExpandedIntervalsForTesting(reference, startIndex);
            final int count = IntervalUtils.unionAppendedIntervalsForTesting(intervals, startIndex, mergedEnd);
            final String expected = toIntervalString(reference, 0);
            final String actual = toIntervalString(intervals, 0);
            if (!expected.equals(actual)) {
                Assert.fail("iteration " + i + ", head " + startIndex / 2 + ", merged " + toIntervalString(merged, 0)
                        + ", appended " + toIntervalString(appended, 0) + "\nexpected: " + expected + "\nactual:   " + actual);
            }
            final int mergedCount = merged.size() / 2;
            final int appendedCount = appended.size() / 2;
            Assert.assertTrue("iteration " + i + ", count " + count, count >= appendedCount && count <= 3 * (mergedCount + appendedCount));

            if (mergedCount == 0) {
                noMergedCount += appendedCount > 0 ? 1 : 0;
            } else if (appendedCount == 0) {
                noAppendedCount++;
            } else {
                // the merged intervals that sort before the first new one, and whether the last merged
                // one sorts after every new one
                int firstAppended = 0;
                int lastAppended = 0;
                for (int j = 1; j < appendedCount; j++) {
                    if (isIntervalBefore(appended, j, appended, firstAppended)) {
                        firstAppended = j;
                    }
                    if (isIntervalBefore(appended, lastAppended, appended, j)) {
                        lastAppended = j;
                    }
                }
                int landing = 0;
                while (landing < mergedCount && isIntervalBefore(merged, landing, appended, firstAppended)) {
                    landing++;
                }
                if (landing == 0) {
                    landingFirstCount++;
                } else if (landing < mergedCount) {
                    landingMiddleCount++;
                } else {
                    landingLastCount++;
                }
                final int resultCount = (reference.size() - startIndex) / 2;
                if (isIntervalBefore(appended, lastAppended, merged, mergedCount - 1)) {
                    if (resultCount > mergedCount) {
                        moreWithLaterCount++;
                    } else if (resultCount < mergedCount) {
                        fewerWithLaterCount++;
                    } else {
                        sameWithLaterCount++;
                    }
                }
                if (appendedCount == 1 && resultCount <= mergedCount - 1) {
                    // the new interval took two merged ones or more
                    chainCount++;
                }
                for (int j = 0; j < appendedCount; j++) {
                    for (int k = 0; k < mergedCount; k++) {
                        if (appended.getQuick(2 * j) == merged.getQuick(2 * k) && appended.getQuick(2 * j + 1) == merged.getQuick(2 * k + 1)) {
                            duplicateCount++;
                            j = appendedCount;
                            break;
                        }
                    }
                }
            }
            boolean isInverted = false;
            boolean isMinMax = false;
            for (LongList list : new LongList[]{merged, appended}) {
                for (int j = 0, n = list.size(); j < n; j += 2) {
                    isInverted |= list.getQuick(j) > list.getQuick(j + 1);
                    isMinMax |= list.getQuick(j) == Long.MIN_VALUE || list.getQuick(j + 1) == Long.MAX_VALUE;
                }
            }
            invertedCount += isInverted ? 1 : 0;
            minMaxCount += isMinMax ? 1 : 0;
        }
        LOG.info().$("union of appended intervals [landingFirst=").$(landingFirstCount)
                .$(", landingMiddle=").$(landingMiddleCount)
                .$(", landingLast=").$(landingLastCount)
                .$(", moreWithLater=").$(moreWithLaterCount)
                .$(", fewerWithLater=").$(fewerWithLaterCount)
                .$(", sameWithLater=").$(sameWithLaterCount)
                .$(", chain=").$(chainCount)
                .$(", noAppended=").$(noAppendedCount)
                .$(", noMerged=").$(noMergedCount)
                .$(", inverted=").$(invertedCount)
                .$(", minMax=").$(minMaxCount)
                .$(", duplicate=").$(duplicateCount)
                .I$();
        Assert.assertTrue(landingFirstCount > 0);
        Assert.assertTrue(landingMiddleCount > 0);
        Assert.assertTrue(landingLastCount > 0);
        Assert.assertTrue(moreWithLaterCount > 0);
        Assert.assertTrue(fewerWithLaterCount > 0);
        Assert.assertTrue(sameWithLaterCount > 0);
        Assert.assertTrue(chainCount > 0);
        Assert.assertTrue(noAppendedCount > 0);
        Assert.assertTrue(noMergedCount > 0);
        Assert.assertTrue(invertedCount > 0);
        Assert.assertTrue(minMaxCount > 0);
        Assert.assertTrue(duplicateCount > 0);
    }

    @Test
    public void testUnionBracketExpandedIntervalsAdjacentPairCoalesces() {
        // lo == prevHi + 1 at a normal boundary - the second disjunct of the guard merges it
        LongList intervals = new LongList();
        add(intervals, 10, 20);
        add(intervals, 21, 30);
        IntervalUtils.unionBracketExpandedIntervalsForTesting(intervals, 0);
        TestUtils.assertEquals("[10,30]", toIntervalString(intervals, 0));

        // adjacency into an interval whose hi is Long.MAX_VALUE (prevHi here is finite)
        intervals.clear();
        add(intervals, 10, 20);
        add(intervals, 21, Long.MAX_VALUE);
        IntervalUtils.unionBracketExpandedIntervalsForTesting(intervals, 0);
        TestUtils.assertEquals("[10," + Long.MAX_VALUE + "]", toIntervalString(intervals, 0));
    }

    @Test
    public void testUnionBracketExpandedIntervalsGapOfTwoStaysSeparate() {
        // lo == prevHi + 2 - not adjacent, must not coalesce
        LongList intervals = new LongList();
        add(intervals, 10, 20);
        add(intervals, 22, 30);
        IntervalUtils.unionBracketExpandedIntervalsForTesting(intervals, 0);
        TestUtils.assertEquals("[10,20], [22,30]", toIntervalString(intervals, 0));
    }

    @Test
    public void testUnionBracketExpandedIntervalsMaxValueContainmentShapes() {
        // both intervals reach Long.MAX_VALUE - the earlier one contains the later one
        LongList intervals = new LongList();
        add(intervals, 0, Long.MAX_VALUE);
        add(intervals, 5, Long.MAX_VALUE);
        IntervalUtils.unionBracketExpandedIntervalsForTesting(intervals, 0);
        TestUtils.assertEquals("[0," + Long.MAX_VALUE + "]", toIntervalString(intervals, 0));

        // the middle interval opens the union up to Long.MAX_VALUE; the later finite
        // interval overlaps (lo <= prevHi) and must fold in without prevHi + 1 wrapping
        intervals.clear();
        add(intervals, 5, 10);
        add(intervals, 8, Long.MAX_VALUE);
        add(intervals, 20, 25);
        IntervalUtils.unionBracketExpandedIntervalsForTesting(intervals, 0);
        TestUtils.assertEquals("[5," + Long.MAX_VALUE + "]", toIntervalString(intervals, 0));
    }

    @Test
    public void testUnionBracketExpandedIntervalsMaxValueOverlapCoalesces() {
        // The overflow boundary: after sorting, the merge pass sees prevHi == Long.MAX_VALUE.
        // The old guard (lo <= prevHi + 1) wrapped prevHi + 1 to Long.MIN_VALUE and failed to
        // coalesce; the current guard checks lo <= prevHi first and merges. The prefix below
        // the batch must stay untouched, and the batch arrives unordered to exercise the sort.
        LongList intervals = new LongList();
        add(intervals, 0, 1);
        add(intervals, 200, 300);
        add(intervals, 100, Long.MAX_VALUE);
        IntervalUtils.unionBracketExpandedIntervalsForTesting(intervals, 2);
        TestUtils.assertEquals("[0,1], [100," + Long.MAX_VALUE + "]", toIntervalString(intervals, 0));
    }

    @Test
    public void testUnionEmpty1() {
        LongList intervals = new LongList();
        // A
        add(intervals, -1, 1);
        add(intervals, 2, 3);

        runTestUnionInPlace(intervals, 4, "[-1,1], [2,3]");
    }

    @Test
    public void testUnionEmpty2() {
        LongList intervals = new LongList();
        // A
        runTestUnionInPlace(intervals, 0, "");
    }

    @Test
    public void testUnionInPlaceSimple1() {
        LongList intervals = new LongList();
        // A
        add(intervals, -1, 10);

        // B
        add(intervals, 1, 2);
        add(intervals, 3, 4);
        add(intervals, 15, 16);

        runTestUnionInPlace(intervals, 2, "[-1,10], [15,16]");
    }

    @Test
    public void testUnionInPlaceSimple2() {
        LongList intervals = new LongList();
        // A
        add(intervals, -1, 1);
        add(intervals, 2, 3);

        // B
        add(intervals, 1, 2);
        add(intervals, 3, 4);
        add(intervals, 6, 7);

        runTestUnionInPlace(intervals, 4, "[-1,4], [6,7]");
    }

    @Test
    public void testUnionInPlaceAdjacentRegionsStaySeparate() {
        LongList intervals = new LongList();
        // A
        add(intervals, 1, 2);
        // B - begins right after A ends; the coalescing rule (lo <= prevHi) keeps them separate
        add(intervals, 3, 4);
        runTestUnionInPlace(intervals, 2, "[1,2], [3,4]");
    }

    @Test
    public void testUnionInPlaceMaxValueBounds() {
        LongList intervals = new LongList();
        // A
        add(intervals, 10, Long.MAX_VALUE);
        // B
        add(intervals, Long.MIN_VALUE, 7);
        // Long.MIN_VALUE is LONG_NULL and renders as "null" in the interval string
        runTestUnionInPlace(intervals, 2, "[null,7], [10," + Long.MAX_VALUE + "]");
    }

    @Test
    public void testUnionInPlaceRandomizedEquivalence() {
        Random rnd = new Random(42);
        for (int iter = 0; iter < 500; iter++) {
            LongList combined = randomMergedRegion(rnd, 8);
            int divider = combined.size();
            LongList b = randomMergedRegion(rnd, 8);
            combined.add(b, 0, b.size());

            LongList expected = new LongList();
            naiveUnion(combined, expected);

            IntervalUtils.unionInPlace(combined, divider);
            Assert.assertEquals(
                    "iteration " + iter,
                    toIntervalString(expected, 0),
                    toIntervalString(combined, 0)
            );
        }
    }

    @Test
    public void testUnionInPlaceTouchingRegionsMerge() {
        LongList intervals = new LongList();
        // A
        add(intervals, 1, 2);
        // B - shares a bound with A's last interval and must coalesce
        add(intervals, 2, 5);
        runTestUnionInPlace(intervals, 2, "[1,5]");
    }

    /**
     * Adds an interval of a new batch: mostly one at, next to, over, in or between the merged
     * intervals, else a random one.
     */
    private static void addRandomAppendedInterval(Rnd rnd, LongList merged, LongList appended) {
        final int mergedCount = merged.size() / 2;
        if (mergedCount == 0 || rnd.nextInt(8) == 0) {
            addRandomInterval(rnd, appended);
            return;
        }
        final int k = rnd.nextInt(mergedCount);
        final long lo = merged.getQuick(2 * k);
        final long hi = merged.getQuick(2 * k + 1);
        final long lastHi = merged.getQuick(merged.size() - 1);
        switch (rnd.nextInt(10)) {
            // after the last one, with a gap
            case 0 -> appended.add(lastHi + 2 + rnd.nextInt(5), lastHi + 8 + rnd.nextInt(5));
            // adjacent after one
            case 1 -> appended.add(hi + 1, hi + 1 + rnd.nextInt(4));
            // overlapping one
            case 2 -> appended.add(lo + rnd.nextInt(3), hi + rnd.nextInt(6));
            // inside one, inverted when it is short
            case 3 -> appended.add(lo + 1, hi - 1);
            // between one and the next
            case 4 -> appended.add(hi + 2, k + 1 < mergedCount ? merged.getQuick(2 * k + 2) - 2 : hi + 3);
            // before the first one, adjacent or with a gap
            case 5 -> appended.add(merged.getQuick(0) - 5 - rnd.nextInt(3), merged.getQuick(0) - 1 - rnd.nextInt(2));
            // bridging one and up to three after it
            case 6 -> {
                final int last = Math.min(mergedCount - 1, k + 1 + rnd.nextInt(3));
                appended.add(lo + rnd.nextInt(2), merged.getQuick(2 * last) + rnd.nextInt(2));
            }
            // from one to past the last one
            case 7 -> appended.add(lo, lastHi + rnd.nextInt(4));
            // a duplicate
            case 8 -> appended.add(lo, hi);
            // inverted
            default -> appended.add(lo + rnd.nextInt(5), lo - 1 - rnd.nextInt(3));
        }
    }

    /**
     * Adds a random interval over a small range, now and then inverted, or at Long.MIN_VALUE or
     * Long.MAX_VALUE.
     */
    private static void addRandomInterval(Rnd rnd, LongList out) {
        long lo = rnd.nextInt(100);
        long hi = lo + rnd.nextInt(12) - 2;
        switch (rnd.nextInt(40)) {
            case 0 -> lo = Long.MIN_VALUE;
            case 1 -> hi = Long.MAX_VALUE;
            case 2 -> {
                lo = Long.MIN_VALUE;
                hi = Long.MIN_VALUE;
            }
            case 3 -> {
                lo = Long.MAX_VALUE;
                hi = Long.MAX_VALUE;
            }
            default -> {
            }
        }
        out.add(lo, hi);
    }

    private static void assertParseFloorPartialTimestampEquals(long expectedTimestamp, CharSequence actual) throws NumericException {
        Assert.assertEquals(expectedTimestamp, MicrosTimestampDriver.floor(actual));
    }

    private static long getIntervalHi(LongList intervals, int pos) {
        return intervals.getQuick((pos << 1) + 1);
    }

    private static long getIntervalLo(LongList intervals, int pos) {
        return intervals.getQuick(pos << 1);
    }

    /**
     * Returns whether interval i of a sorts before interval j of b in the order of a union: by lo,
     * then by hi.
     */
    private static boolean isIntervalBefore(LongList a, int i, LongList b, int j) {
        final long loA = a.getQuick(2 * i);
        final long loB = b.getQuick(2 * j);
        return loA < loB || (loA == loB && a.getQuick(2 * i + 1) < b.getQuick(2 * j + 1));
    }

    /**
     * This is alternative intersect implementation used to be in main code base
     * but not used anymore and refactored to the tests code for comparison with in place intersect method.
     * <p>
     * Intersects two lists of intervals and returns result list. Both lists are expected
     * to be chronologically ordered and result list will be ordered as well.
     *
     * @param a   list of intervals
     * @param b   list of intervals
     * @param out intersection target
     */
    private static void intersect(LongList a, LongList b, LongList out) {
        final int sizeA = a.size() / 2;
        final int sizeB = b.size() / 2;
        int intervalA = 0;
        int intervalB = 0;

        while (intervalA != sizeA && intervalB != sizeB) {
            long aLo = getIntervalLo(a, intervalA);
            long aHi = getIntervalHi(a, intervalA);

            long bLo = getIntervalLo(b, intervalB);
            long bHi = getIntervalHi(b, intervalB);

            // a fully above b
            if (aHi < bLo) {
                // a loses
                intervalA++;
            } else if (getIntervalLo(a, intervalA) > getIntervalHi(b, intervalB)) {
                // a fully below b
                // b loses
                intervalB++;
            } else {
                append(out, Math.max(aLo, bLo), Math.min(aHi, bHi));

                if (aHi < bHi) {
                    // b hanging lower than a
                    // a loses
                    intervalA++;
                } else {
                    // otherwise a lower than b
                    // a loses
                    intervalB++;
                }
            }
        }
    }

    /**
     * Reference union: sort all intervals by lo then coalesce on overlap-or-touch
     * ({@code lo <= prevHi}), independently of the production merge implementations.
     */
    private static void naiveUnion(LongList intervals, LongList out) {
        final ArrayList<long[]> pairs = new ArrayList<>();
        for (int i = 0; i < intervals.size(); i += 2) {
            pairs.add(new long[]{intervals.getQuick(i), intervals.getQuick(i + 1)});
        }
        pairs.sort(Comparator.<long[]>comparingLong(p -> p[0]).thenComparingLong(p -> p[1]));
        out.clear();
        for (int i = 0, n = pairs.size(); i < n; i++) {
            long lo = pairs.get(i)[0];
            long hi = pairs.get(i)[1];
            if (out.size() > 0 && lo <= out.getQuick(out.size() - 1)) {
                if (hi > out.getQuick(out.size() - 1)) {
                    out.setQuick(out.size() - 1, hi);
                }
            } else {
                out.add(lo, hi);
            }
        }
    }

    /**
     * Generates a sorted region whose intervals never overlap or touch, i.e. a valid merged
     * accumulator under the {@code lo <= prevHi} coalescing rule. Adjacent intervals
     * ({@code lo == prevHi + 1}) are legal and deliberately included.
     */
    private static LongList randomMergedRegion(Random rnd, int maxIntervals) {
        LongList list = new LongList();
        int n = rnd.nextInt(maxIntervals + 1);
        long lo = rnd.nextInt(50);
        for (int i = 0; i < n; i++) {
            long hi = lo + rnd.nextInt(10);
            list.add(lo, hi);
            lo = hi + 1 + rnd.nextInt(10);
        }
        return list;
    }

    private void add(LongList intervals, long lo, long hi) {
        intervals.add(lo);
        intervals.add(hi);
    }

    /**
     * Asserts that unionAppendedIntervals() leaves the head, the merged intervals and the appended
     * ones after them as the expected list, the same as a union of all intervals after the head does,
     * and the number of intervals it counts.
     */
    private void assertUnionAppended(long[] head, long[] merged, long[] appended, String expected, int expectedCount) {
        // the merged intervals are the output of a union, which another union leaves as they are
        final LongList union = new LongList();
        for (long value : merged) {
            union.add(value);
        }
        final String mergedText = toIntervalString(union, 0);
        if (union.size() > 0) {
            IntervalUtils.unionBracketExpandedIntervalsForTesting(union, 0);
        }
        TestUtils.assertEquals(mergedText, toIntervalString(union, 0));

        final LongList intervals = new LongList();
        final LongList reference = new LongList();
        for (long[] values : new long[][]{head, merged, appended}) {
            if (values != null) {
                for (long value : values) {
                    intervals.add(value);
                    reference.add(value);
                }
            }
        }
        final int startIndex = head != null ? head.length : 0;
        IntervalUtils.unionBracketExpandedIntervalsForTesting(reference, startIndex);
        final int count = IntervalUtils.unionAppendedIntervalsForTesting(intervals, startIndex, startIndex + merged.length);
        TestUtils.assertEquals(expected, toIntervalString(intervals, 0));
        TestUtils.assertEquals(toIntervalString(reference, 0), toIntervalString(intervals, 0));
        Assert.assertEquals(expected, expectedCount, count);
    }

    private void runTestIntersectInplace(LongList intervals, int divider, String expected) {
        LongList copy = new LongList();
        copy.add(intervals, divider, intervals.size());
        copy.add(intervals, 0, divider);

        IntervalUtils.intersectInPlace(intervals, divider);
        TestUtils.assertEquals(expected, toIntervalString(intervals, 0));

        IntervalUtils.intersectInPlace(copy, copy.size() - divider);
        TestUtils.assertEquals(expected, toIntervalString(copy, 0));
    }

    private void runTestInvertInplace(LongList intervals, int divider, String expected) {
        LongList toInvertExtracted = new LongList(intervals);

        IntervalUtils.invert(intervals, divider);
        TestUtils.assertEquals(expected, toIntervalString(intervals, divider));

        LongList copy1 = new LongList(toInvertExtracted);
        IntervalUtils.invert(copy1, divider);
        TestUtils.assertEquals(expected, toIntervalString(copy1, divider));

        // Double invert must be same as in the beginning
        IntervalUtils.invert(copy1, divider);
        TestUtils.assertEquals(toIntervalString(toInvertExtracted, divider), toIntervalString(copy1, divider));
    }

    private void runTestUnionInPlace(LongList intervals, int divider, String expected) {
        LongList copy = new LongList();
        copy.add(intervals, divider, intervals.size());
        copy.add(intervals, 0, divider);

        IntervalUtils.unionInPlace(intervals, divider);
        TestUtils.assertEquals(expected, toIntervalString(intervals, 0));

        IntervalUtils.unionInPlace(copy, copy.size() - divider);
        TestUtils.assertEquals(expected, toIntervalString(copy, 0));
    }

    private String toIntervalString(LongList intervals, int divider) {
        sink.clear();
        for (int i = divider; i < intervals.size(); ) {
            if (i > divider) {
                sink.put(", ");
            }
            sink.put('[').put(intervals.get(i++)).put(',').put(intervals.get(i++)).put(']');
        }
        return sink.toString();
    }

    static void append(LongList list, long lo, long hi) {
        int n = list.size();
        if (n > 0) {
            long prevHi = list.getQuick(n - 1) + 1;
            if (prevHi >= lo) {
                list.setQuick(n - 1, hi);
                return;
            }
        }

        list.add(lo);
        list.add(hi);
    }
}
