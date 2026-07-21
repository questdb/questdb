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
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;

import java.util.Random;

public class IntervalUtilsTest {
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

    @Test
    public void testSortAndUnionInPlaceLargeAscendingBatch() {
        // complexity canary for the ascending IN-list / OR-ed disjunct shape: before
        // LongGroupSort switched to introsort with a sorted-input fast-path, this
        // already-sorted batch cost O(D^2) comparisons - minutes of CPU at this size.
        // No wall-clock assertion: a regression shows up as a hung test.
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
    public void testUnionBracketExpandedIntervalsAdjacentPairCoalesces() {
        // lo == prevHi + 1 at a normal boundary - the second disjunct of the guard merges it
        LongList intervals = new LongList();
        add(intervals, 10, 20);
        add(intervals, 21, 30);
        unionBracketExpandedIntervals(intervals, 0);
        TestUtils.assertEquals("[10,30]", toIntervalString(intervals, 0));

        // adjacency into an interval whose hi is Long.MAX_VALUE (prevHi here is finite)
        intervals.clear();
        add(intervals, 10, 20);
        add(intervals, 21, Long.MAX_VALUE);
        unionBracketExpandedIntervals(intervals, 0);
        TestUtils.assertEquals("[10," + Long.MAX_VALUE + "]", toIntervalString(intervals, 0));
    }

    @Test
    public void testUnionBracketExpandedIntervalsGapOfTwoStaysSeparate() {
        // lo == prevHi + 2 - not adjacent, must not coalesce
        LongList intervals = new LongList();
        add(intervals, 10, 20);
        add(intervals, 22, 30);
        unionBracketExpandedIntervals(intervals, 0);
        TestUtils.assertEquals("[10,20], [22,30]", toIntervalString(intervals, 0));
    }

    @Test
    public void testUnionBracketExpandedIntervalsMaxValueContainmentShapes() {
        // both intervals reach Long.MAX_VALUE - the earlier one contains the later one
        LongList intervals = new LongList();
        add(intervals, 0, Long.MAX_VALUE);
        add(intervals, 5, Long.MAX_VALUE);
        unionBracketExpandedIntervals(intervals, 0);
        TestUtils.assertEquals("[0," + Long.MAX_VALUE + "]", toIntervalString(intervals, 0));

        // the middle interval opens the union up to Long.MAX_VALUE; the later finite
        // interval overlaps (lo <= prevHi) and must fold in without prevHi + 1 wrapping
        intervals.clear();
        add(intervals, 5, 10);
        add(intervals, 8, Long.MAX_VALUE);
        add(intervals, 20, 25);
        unionBracketExpandedIntervals(intervals, 0);
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
        unionBracketExpandedIntervals(intervals, 2);
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

    private static void unionBracketExpandedIntervals(LongList out, int startIndex) {
        // IntervalUtils.unionBracketExpandedIntervals is package-private in
        // io.questdb.griffin.model; both modules are open, so reflection reaches it
        try {
            Method method = IntervalUtils.class.getDeclaredMethod("unionBracketExpandedIntervals", LongList.class, int.class);
            method.setAccessible(true);
            method.invoke(null, out, startIndex);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
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
