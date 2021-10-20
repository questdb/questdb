/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.model;

import io.questdb.std.LongList;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class IntervalUtilsTest {
    private final StringSink sink = new StringSink();

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
    public void testUnionInplaceSimple1() {
        LongList intervals = new LongList();
        // A
        add(intervals, -1, 10);

        // B
        add(intervals, 1, 2);
        add(intervals, 3, 4);
        add(intervals, 15, 16);

        runTestUnionInplace(intervals, 2, "[-1,10], [15,16]");
    }

    @Test
    public void testUnionInplaceSimple2() {
        LongList intervals = new LongList();
        // A
        add(intervals, -1, 1);
        add(intervals, 2, 3);

        // B
        add(intervals, 1, 2);
        add(intervals, 3, 4);
        add(intervals, 6, 7);

        runTestUnionInplace(intervals, 4, "[-1,4], [6,7]");
    }

    @Test
    public void testUnionEmpty1() {
        LongList intervals = new LongList();
        // A
        add(intervals, -1, 1);
        add(intervals, 2, 3);

        runTestUnionInplace(intervals, 4, "[-1,1], [2,3]");
    }

    @Test
    public void testUnionEmpty2() {
        LongList intervals = new LongList();
        // A
        runTestUnionInplace(intervals, 0, "");
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

        runTestUnionInplace(intervals, 6, "[1,2], [3,4], [6,7], [100,101], [200,201], [205,206]");
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

        runTestUnionInplace(intervals, 6, "[1,2], [3,4], [50,250]");
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
    public void testIntersectEmpty() {
        LongList intervals = new LongList();
        // A

        // B
        add(intervals, -20, -2);
        add(intervals, 1, 2);

        runTestIntersectInplace(intervals, 0, "");
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
        LongList bItervals = new LongList();
        for (int i = 0; i < bSize; i++) {
            add(bItervals, bPos, bPos += r.nextInt(100));
            bPos += r.nextInt(100);
        }

        LongList expected = new LongList();
        // non-in place algo, supposed to be correct
        intersect(intervals, bItervals, expected);
        String expectedStr = toIntervalString(expected, 0);

        intervals.add(bItervals);
        runTestIntersectInplace(intervals, aSize * 2, expectedStr);
    }

    @Test
    public void testInvertSimple() {
        LongList intervals = new LongList();
        // A
        add(intervals, -20, -2);

        // B
        add(intervals, 1, 2);

        runTestInvertInplace(intervals, 2, "[NaN,0], [3,9223372036854775807]");
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
    public void testInvertWithNegativeInfinity() {
        LongList intervals = new LongList();
        // A
        intervals.add(1);

        // B
        add(intervals, 2, 100);
        add(intervals, 200, Long.MAX_VALUE);

        runTestInvertInplace(intervals, 1, "[NaN,1], [101,199]");
    }

    @Test
    public void testIsInEmptyIntervalList() {
        LongList intervals = new LongList();
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 123));
    }

    @Test
    public void testIsInListWithOneInterval() {
        LongList intervals = new LongList();
        add(intervals, 100, 102);
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 99));
        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 100));
        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 101));
        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 102));
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 103));
    }

    @Test
    public void testIsInListWithEvenNumberOfIntervals() {
        LongList intervals = new LongList();
        add(intervals, 100, 102);
        add(intervals, 122, 124);
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 99));
        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 101));
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 103));
        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 123));
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 125));
    }

    @Test
    public void testIsInListWithOddNumberOfIntervals() {
        LongList intervals = new LongList();
        add(intervals, 100, 102);
        add(intervals, 122, 124);
        add(intervals, 150, 155);
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 99));
        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 101));
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 103));
        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 123));
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 125));
        Assert.assertTrue(IntervalUtils.isInIntervals(intervals, 151));
        Assert.assertFalse(IntervalUtils.isInIntervals(intervals, 156));
    }

    private void runTestIntersectInplace(LongList intervals, int divider, String expected) {
        LongList copy = new LongList();
        copy.add(intervals, divider, intervals.size());
        copy.add(intervals, 0, divider);

        IntervalUtils.intersectInplace(intervals, divider);
        TestUtils.assertEquals(expected, toIntervalString(intervals, 0));

        IntervalUtils.intersectInplace(copy, copy.size() - divider);
        TestUtils.assertEquals(expected, toIntervalString(copy, 0));
    }

    private void runTestUnionInplace(LongList intervals, int divider, String expected) {
        LongList copy = new LongList();
        copy.add(intervals, divider, intervals.size());
        copy.add(intervals, 0, divider);

        IntervalUtils.unionInplace(intervals, divider);
        TestUtils.assertEquals(expected, toIntervalString(intervals, 0));

        IntervalUtils.unionInplace(copy, copy.size() - divider);
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

    private void add(LongList intervals, long lo, long hi) {
        intervals.add(lo);
        intervals.add(hi);
    }


    public static long getIntervalHi(LongList intervals, int pos) {
        return intervals.getQuick((pos << 1) + 1);
    }

    public static long getIntervalLo(LongList intervals, int pos) {
        return intervals.getQuick(pos << 1);
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
    public static void intersect(LongList a, LongList b, LongList out) {
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
}