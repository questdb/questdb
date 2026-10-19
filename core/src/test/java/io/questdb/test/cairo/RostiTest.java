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

package io.questdb.test.cairo;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;


public class RostiTest extends AbstractCairoTest {

    private static final int VALUE_OFFSET = 1;

    @Test
    public void testEveryWrapUpSweepSurvivesNullKeyInsertResize() throws Exception {
        // wrapUp() populates the null-key slot and then sweeps the map, NULLing every group whose
        // count is still 0. The populate inserts, and an insert past the growth threshold resizes:
        // that reallocates ctrl_ and slots_, frees the old block and grows capacity_. A sweep
        // reading a snapshot taken before the populate walks freed memory and leaves the live
        // count-0 groups behind.
        //
        // Each of the nine sweeping wrapUps seeds its own slot layout, so the assertion they all
        // share is expressed in the one term they have in common: the sweep visits every live
        // group, and an untouched group is one still holding the value initRosti() wrote. The
        // sum(long) case additionally pins the swept sentinel and the null group's own value.
        //
        // Capacity is ceilPow2(16) - 1 = 15 and the threshold is capacity - capacity/8 = 14, so
        // the liveKeys sweep below straddles the resize.
        assertMemoryLeak(() -> {
            final ObjList<WrapUpCase> cases = wrapUpCases();
            for (int i = 0, n = cases.size(); i < n; i++) {
                final WrapUpCase c = cases.getQuick(i);
                boolean hasResizedInWrapUp = false;
                for (int liveKeys = 8; liveKeys <= 20; liveKeys++) {
                    hasResizedInWrapUp |= assertWrapUpSweepsEveryGroup(c, liveKeys, false);
                }
                // Only one point in the sweep puts the resize inside wrapUp(); above it the live
                // keys resize first. Fail loudly rather than silently degenerate if the growth
                // math moves.
                Assert.assertTrue("no iteration resized inside wrapUp() [case=" + c.name
                        + "] -- widen the liveKeys sweep", hasResizedInWrapUp);
            }
        });
    }

    @Test
    public void testFailedResetLeavesTheMapReportingItsEntries() throws Exception {
        // reset() shrinks a map by building a fresh arena, and that allocation can fail. It used to
        // empty the map before trying, so a failed reset left size_ at 0 while the old arena still
        // held every entry. GroupByRecordCursorFactory skips maps that report fewer than one entry
        // when merging worker shards, so those groups vanished from the result.
        assertMemoryLeak(() -> {
            final long pRosti = allocCompensatedSumRosti();
            try {
                Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, Rosti.getInitialValueSlot(pRosti, 0), 1));
                Assert.assertEquals(1, Rosti.getSize(pRosti));

                Rosti.enableOOMOnMalloc();
                try {
                    Assert.assertFalse(Rosti.reset(pRosti, 16));
                } finally {
                    Rosti.disableOOMOnMalloc();
                }

                Assert.assertEquals(1, Rosti.getSize(pRosti));
                // A successful reset afterwards still empties it, so the failure changed nothing
                // beyond leaving the map as it was.
                Assert.assertTrue(Rosti.reset(pRosti, 16));
                Assert.assertEquals(0, Rosti.getSize(pRosti));
            } finally {
                Rosti.free(pRosti);
            }
        });
    }

    @Test
    public void testKeyedIntKSumDoubleMergeCarriesDestinationCompensation() throws Exception {
        // Each shard holds a Kahan (sum, c) pair standing for sum - c, and merge() folds shard B
        // into shard A. The step has to subtract A's own pending correction as well as B's:
        // reading only B's dropped whatever A had accumulated, and ksum reports the running sum
        // alone, so the loss lands straight in the query result. Every value here is a small
        // dyadic rational, so the arithmetic is exact.
        assertMemoryLeak(() -> {
            final long pRostiA = allocCompensatedSumRosti();
            try {
                final long pRostiB = allocCompensatedSumRosti();
                try {
                    Assert.assertTrue(Rosti.keyedIntDistinct(pRostiA, Rosti.getInitialValueSlot(pRostiA, 0), 1));
                    Assert.assertTrue(Rosti.keyedIntDistinct(pRostiB, Rosti.getInitialValueSlot(pRostiB, 0), 1));
                    // A stands for 8.0 - 0.25, B for 2.0 - 0.5.
                    seedSingleSlot(pRostiA, VALUE_OFFSET, 8.0, 0.25, 2);
                    seedSingleSlot(pRostiB, VALUE_OFFSET, 2.0, 0.5, 1);

                    Assert.assertTrue(Rosti.keyedIntKSumDoubleMerge(pRostiA, pRostiB, VALUE_OFFSET));

                    Assert.assertEquals(9.25, readSingleSlotDouble(pRostiA, VALUE_OFFSET), 0.0);
                    // A third shard merging into A reads this field, as does wrapUp()'s populate
                    // branch, so the fresh correction has to replace the one merge just consumed.
                    Assert.assertEquals(0.0, readSingleSlotDouble(pRostiA, VALUE_OFFSET + 1), 0.0);
                } finally {
                    Rosti.free(pRostiB);
                }
            } finally {
                Rosti.free(pRostiA);
            }
        });
    }

    @Test
    public void testKeyedIntKSumDoubleWrapUpKeepsTheSlotSelfConsistent() throws Exception {
        // Pins an internal invariant of the ksum slot rather than a query result: the Kahan step
        // writes the new compensation back beside the value it belongs to, so the pair keeps
        // standing for sum - c. Seeding a NON-ZERO c is what makes that visible -- the step starts
        // from y = valueAtNull - c, so a zero seed leaves the read dead.
        //
        // 2^53 has an ulp of 2, so adding y = 1.0 - 0.5 rounds straight back off and the residual
        // is exactly -0.5.
        assertMemoryLeak(() -> {
            final long pRosti = allocCompensatedSumRosti();
            try {
                Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, Rosti.getInitialValueSlot(pRosti, 0), 1));
                seedSingleSlot(pRosti, VALUE_OFFSET, 9_007_199_254_740_992.0, 0.5, 2);

                Assert.assertTrue(Rosti.keyedIntKSumDoubleWrapUp(pRosti, VALUE_OFFSET, 1.0, 3));

                Assert.assertEquals(9_007_199_254_740_992.0, readSingleSlotDouble(pRosti, VALUE_OFFSET), 0.0);
                Assert.assertEquals(-0.5, readSingleSlotDouble(pRosti, VALUE_OFFSET + 1), 0.0);
            } finally {
                Rosti.free(pRosti);
            }
        });
    }

    @Test
    public void testKeyedIntNSumDoubleMergeCarriesSourceCompensation() throws Exception {
        // merge() is key-agnostic, so this needs no column top at all: the key is INT_NULL only
        // because it is the convenient seed.
        assertMemoryLeak(() -> {
            // abs(sum) >= abs(d): first arm of the Neumaier step.
            assertMergeCarriesCompensation(8.0, 0.25, 2.0, 0.5, 10.75, 0.75);
            // abs(sum) < abs(d): second arm.
            assertMergeCarriesCompensation(2.0, 0.25, 8.0, 0.5, 10.75, 0.75);
        });
    }

    @Test
    public void testKeyedIntNSumDoubleMergeComparesAddendMagnitude() throws Exception {
        // The Neumaier step picks its arm by comparing the running sum against the addend, and the
        // comparison has to be between MAGNITUDES: abs(sum) >= abs(d). Compare abs(sum) against a
        // raw d and every negative addend takes the first arm, which then computes the correction
        // from the wrong pair. vec_agg_vanilla.cpp and vec_agg.cpp already had the magnitude form,
        // so the two paths disagreed on the same data.
        //
        // 2^60 has an ulp of 256, so the destination's 1.0 rounds straight off the merged sum and
        // owes a correction of exactly 1. Only the second arm produces it: (d - t) + sum is
        // (-2^60 + 2^60) + 1. The first arm computes (sum - t) + d = (1 + 2^60) + (-2^60), and
        // 1 + 2^60 rounds back to 2^60, so it contributes 0 and the destination's 1.0 is lost.
        //
        // Both slots also carry their own non-zero compensation, so the expected 1.75 is the seeded
        // 0.25 plus the step's 1 plus the source's 0.5, and the wrong arm leaves 0.75.
        assertMemoryLeak(() -> assertMergeCarriesCompensation(
                1.0, 0.25, -1_152_921_504_606_846_976.0, 0.5, -1_152_921_504_606_846_976.0, 1.75));
    }

    @Test
    public void testKeyedIntNSumDoubleWrapUpComparesAddendMagnitude() throws Exception {
        // The mirror of testKeyedIntNSumDoubleMergeComparesAddendMagnitude on wrapUp()'s populate
        // branch, where the addend is the null group's running sum handed over by the Java side. It
        // is an ordinary DOUBLE column value, so it is negative as often as not, and comparing
        // abs(sum) against a raw valueAtNull sends every negative one down the first arm.
        //
        // Same arithmetic: the slot's 1.0 rounds off a sum of -2^60 and owes a correction of
        // exactly 1, which only (valueAtNull - t) + sum produces. The first arm's
        // (sum - t) + valueAtNull is (1 + 2^60) + (-2^60), and 1 + 2^60 rounds back to 2^60, so it
        // contributes 0. Expected 1.75 is the seeded 0.25 plus the step's 1 plus the incoming 0.5;
        // the wrong arm leaves 0.75.
        assertMemoryLeak(() -> assertWrapUpFoldsIntoSeededSlot(
                1.0, 0.25, -1_152_921_504_606_846_976.0, 0.5, -1_152_921_504_606_846_976.0, 1.75));
    }

    @Test
    public void testKeyedIntNSumDoubleWrapUpFoldsCompensationIntoExistingNullSlot() throws Exception {
        // nsum's null-group total is valueAtNull + valueAtNullC: the Java side hands over a
        // running sum and a separate Neumaier compensation term. wrapUp() must fold both into
        // a slot that already holds its own (sum, c) pair.
        //
        // A SQL-level test cannot pin this: computeSum() folds each worker's compensation into
        // its own sum before the cross-worker pass, so a single populated worker slot yields a
        // compensation of exactly 0, and with several the residual depends on nondeterministic
        // frame-to-worker assignment. Driving the JNI function is the only deterministic way.
        //
        // Every value below is a small dyadic rational, so all the arithmetic is exact and the
        // expected totals are unambiguous. Both seeds carry a non-zero sum AND a non-zero
        // compensation, so the assertion also fails if wrapUp() overwrites the slot's
        // compensation instead of adding to it.
        assertMemoryLeak(() -> {
            // abs(sum) >= abs(valueAtNull): takes the first arm of the Neumaier step.
            assertWrapUpFoldsIntoSeededSlot(8.0, 0.25, 2.0, 0.5, 10.75, 0.75);
            // abs(sum) < abs(valueAtNull): takes the second arm.
            assertWrapUpFoldsIntoSeededSlot(2.0, 0.25, 8.0, 0.5, 10.75, 0.75);
        });
    }

    @Test
    public void testKeyedIntNSumDoubleWrapUpFoldsCompensationIntoFreshNullSlot() throws Exception {
        // No pre-existing slot, so wrapUp() takes its insert branch, which stores valueAtNull and
        // valueAtNullC in separate fields. What makes 2.5 come back is the ORDER of the two halves
        // of wrapUp: the sweep folds c into the value, so it has to run after the populate. Swap
        // them and the fresh slot's compensation is never folded and the answer is 2.0 -- that
        // order swap, not the insert branch's storing of c, is what this pins.
        assertMemoryLeak(() -> {
            final long pRosti = allocCompensatedSumRosti();
            try {
                Assert.assertTrue(Rosti.keyedIntNSumDoubleWrapUp(pRosti, VALUE_OFFSET, 2.0, 1, 0.5));

                Assert.assertEquals(2.5, readSingleSlotDouble(pRosti, VALUE_OFFSET), 0.0);
            } finally {
                Rosti.free(pRosti);
            }
        });
    }

    @Test
    public void testKeyedIntSumIntMergeKeepsPartialsAboveIntRange() throws Exception {
        // sum(int) and avg(int) accumulate into a 64-bit slot, so a worker shard's partial can
        // exceed int range long before the total does. Driven through JNI because nothing pins a
        // group's rows to a chosen shard, so a SQL test would often skip the merge altogether.
        //
        // 3_000_000_000 sign-extends from its low 32 bits to -1_294_967_296, so both branches
        // below land far from the correct answer when the slot is read as jint.
        assertMemoryLeak(() -> {
            // Key present in both shards: the accumulate branch.
            assertSumIntMergeKeepsPartial(true, 4_000_000_000L, 3);
            // Key present only in the source shard: the assign branch, which is the ordinary
            // case when a group's rows never reached the destination shard.
            assertSumIntMergeKeepsPartial(false, 3_000_000_000L, 2);
        });
    }

    @Test
    public void testPrintRosti() {
        long pRosti = Rosti.alloc(new SingleColumnType(ColumnType.INT), 1024);
        try {
            Rosti.printRosti(pRosti);
        } finally {
            Rosti.free(pRosti);
        }
    }

    @Test
    public void testResetRestoresTheGrowthThreshold() throws Exception {
        // reset() shrinks the map by rebuilding its arena, and initialize_slots() recomputes
        // growth_left_ as CapacityToGrowth(capacity) - size_ while size_ still holds the OLD entry
        // count. reset() repairs that by recomputing growth_left_ once it has zeroed size_; drop
        // the repair and the subtraction below is 14 - 20 on a uint64_t, which wraps to ~1.8e19.
        // The map then never resizes and the second batch of inserts runs off the end of a
        // 15-slot arena.
        //
        // Rosti.reset(pRosti, 16) shrinks capacity to ceilPow2(16) - 1 = 15, whose growth
        // threshold is capacity - capacity/8 = 14. So the map has to hold more than 14 entries
        // going in for the subtraction to wrap, and take more than 14 afterwards for the wrap to
        // show. A reset of a map holding one entry computes 14 - 1 and stays harmless either way.
        assertMemoryLeak(() -> {
            final int keyCount = 20;
            final long keysSize = 4L * keyCount;
            final long pKeys = Unsafe.malloc(keysSize, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < keyCount; i++) {
                    Unsafe.putInt(pKeys + 4L * i, i + 1);
                }
                final long pRosti = Rosti.alloc(types(ColumnType.LONG), 64);
                Assert.assertNotEquals(0, pRosti);
                long recordedSize = Rosti.getAllocMemory(pRosti);
                try {
                    Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, pKeys, keyCount));
                    Assert.assertEquals(keyCount, Rosti.getSize(pRosti));

                    Assert.assertTrue(Rosti.reset(pRosti, 16));
                    Assert.assertEquals(0, Rosti.getSize(pRosti));
                    // reset() records its own shrink, so the baseline for the inserts below moves.
                    recordedSize = Rosti.getAllocMemory(pRosti);
                    final long capacityAfterReset = Rosti.getCapacity(pRosti);

                    Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, pKeys, keyCount));

                    Assert.assertTrue("the map never resized [capacity=" + capacityAfterReset + ']',
                            Rosti.getCapacity(pRosti) > capacityAfterReset);
                    Assert.assertEquals(keyCount, Rosti.getSize(pRosti));
                    assertHoldsEveryKey(pRosti, keyCount);
                } finally {
                    // Rosti.alloc() and Rosti.reset() each recorded the size current at the time,
                    // and Rosti.free() subtracts the current one, so the growth these inserts
                    // caused has to be recorded too. Production does this via the same helper,
                    // through RostiAllocFacade.
                    Rosti.updateMemoryUsage(pRosti, recordedSize);
                    Rosti.free(pRosti);
                }
            } finally {
                Unsafe.free(pKeys, keysSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWrapUpPopulatesAnExistingEmptyNullSlotBeforeSweepingIt() throws Exception {
        // The null-key slot can already be present when wrapUp() runs -- the keyed aggregation
        // creates it from a stored NULL int key -- and it then carries a count of 0, exactly what
        // the sweep NULLs. wrapUp() populates first and sweeps second, so the slot gains its value
        // and its count before the sweep tests that count. Sweeping first NULLs the value and the
        // populate then adds into the sentinel, leaving the null group at LONG_NULL + 7.
        //
        // liveKeys stays below the growth threshold here: with the slot already present the
        // populate finds it instead of inserting, so this case cannot resize inside wrapUp().
        assertMemoryLeak(() -> assertWrapUpSweepsEveryGroup(sumLongWrapUpCase(), 8, true));
    }

    // ksum and nsum share this slot layout, so both drive the same helper.
    private static long allocCompensatedSumRosti() {
        // Running sum, Neumaier compensation, count.
        final long pRosti = Rosti.alloc(types(ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.LONG), 64);
        Assert.assertNotEquals(0, pRosti);
        // Mirrors GroupByRecordCursorFactory's null-key setup and
        // NSumDoubleVectorAggregateFunction.initRosti().
        Unsafe.putInt(Rosti.getInitialValueSlot(pRosti, 0), Numbers.INT_NULL);
        initCompensatedSumSlot(pRosti);
        return pRosti;
    }

    // sum(int) and avg(int) share this slot layout and the same merge.
    private static long allocSumIntRosti() {
        // Running sum -- 64-bit even though the column is INT -- and count.
        final long pRosti = Rosti.alloc(types(ColumnType.LONG, ColumnType.LONG), 64);
        Assert.assertNotEquals(0, pRosti);
        // Mirrors GroupByRecordCursorFactory's null-key setup and
        // SumIntVectorAggregateFunction.initRosti().
        Unsafe.putInt(Rosti.getInitialValueSlot(pRosti, 0), Numbers.INT_NULL);
        Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0);
        Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);
        return pRosti;
    }

    // Asserts the map holds keys 1..keyCount, each exactly once, and nothing else.
    private static void assertHoldsEveryKey(long pRosti, int keyCount) {
        final long ctrl = Rosti.getCtrl(pRosti);
        final long slots = Rosti.getSlots(pRosti);
        final long shift = Rosti.getSlotShift(pRosti);
        final long capacity = Rosti.getCapacity(pRosti);
        final boolean[] hasSeenKey = new boolean[keyCount + 1];
        int liveSlots = 0;
        for (long i = 0; i < capacity; i++) {
            if (Unsafe.getByte(ctrl + i) > -1) {
                final int key = Unsafe.getInt(slots + (i << shift));
                liveSlots++;
                Assert.assertTrue("unexpected key [key=" + key + ']', key >= 1 && key <= keyCount);
                Assert.assertFalse("duplicate key [key=" + key + ']', hasSeenKey[key]);
                hasSeenKey[key] = true;
            }
        }
        Assert.assertEquals("live slot count", keyCount, liveSlots);
        for (int key = 1; key <= keyCount; key++) {
            Assert.assertTrue("key missing [key=" + key + ']', hasSeenKey[key]);
        }
    }

    private static void assertMergeCarriesCompensation(
            double sumA,
            double compensationA,
            double sumB,
            double compensationB,
            double expected,
            double expectedCompensation
    ) {
        final long pRostiA = allocCompensatedSumRosti();
        try {
            final long pRostiB = allocCompensatedSumRosti();
            try {
                Assert.assertTrue(Rosti.keyedIntDistinct(pRostiA, Rosti.getInitialValueSlot(pRostiA, 0), 1));
                Assert.assertTrue(Rosti.keyedIntDistinct(pRostiB, Rosti.getInitialValueSlot(pRostiB, 0), 1));
                seedSingleSlot(pRostiA, VALUE_OFFSET, sumA, compensationA, 2);
                seedSingleSlot(pRostiB, VALUE_OFFSET, sumB, compensationB, 1);

                Assert.assertTrue(Rosti.keyedIntNSumDoubleMerge(pRostiA, pRostiB, VALUE_OFFSET));
                // valueAtNullCount 0 skips the populate, so this only runs the sweep that folds
                // the merged compensation into the value -- what the cursor later reads.
                Assert.assertTrue(Rosti.keyedIntNSumDoubleWrapUp(pRostiA, VALUE_OFFSET, 0.0, 0, 0.0));

                Assert.assertEquals(expected, readSingleSlotDouble(pRostiA, VALUE_OFFSET), 0.0);
                // The sweep folds the compensation into the value and leaves the field itself
                // alone, so this reads what the merge step decided. The step's own contribution
                // can never show up in the value: it is the rounding residual of a single
                // addition, so at most half an ulp of that value, and folding it back rounds
                // straight off again. Pinning the field is the only way to see which arm ran.
                Assert.assertEquals(expectedCompensation, readSingleSlotDouble(pRostiA, VALUE_OFFSET + 1), 0.0);
            } finally {
                Rosti.free(pRostiB);
            }
        } finally {
            Rosti.free(pRostiA);
        }
    }

    private static void assertSumIntMergeKeepsPartial(boolean hasDestinationSlot, long expectedSum, long expectedCount) {
        final long pRostiA = allocSumIntRosti();
        try {
            final long pRostiB = allocSumIntRosti();
            try {
                if (hasDestinationSlot) {
                    Assert.assertTrue(Rosti.keyedIntDistinct(pRostiA, Rosti.getInitialValueSlot(pRostiA, 0), 1));
                    seedSumIntSlot(pRostiA, 1_000_000_000L, 1);
                }
                Assert.assertTrue(Rosti.keyedIntDistinct(pRostiB, Rosti.getInitialValueSlot(pRostiB, 0), 1));
                seedSumIntSlot(pRostiB, 3_000_000_000L, 2);

                Assert.assertTrue(Rosti.keyedIntSumIntMerge(pRostiA, pRostiB, VALUE_OFFSET));

                Assert.assertEquals(expectedSum, readSingleSlotLong(pRostiA, VALUE_OFFSET));
                Assert.assertEquals(expectedCount, readSingleSlotLong(pRostiA, VALUE_OFFSET + 1));
            } finally {
                Rosti.free(pRostiB);
            }
        } finally {
            Rosti.free(pRostiA);
        }
    }

    private static void assertWrapUpFoldsIntoSeededSlot(
            double seedSum,
            double seedCompensation,
            double valueAtNull,
            double valueAtNullC,
            double expected,
            double expectedCompensation
    ) {
        final long pRosti = allocCompensatedSumRosti();
        try {
            // The insert GroupByRecordCursorFactory performs once when a frame's key column is a column top.
            // A stored NULL int key reaches the same slot through kIntNSumDouble, which is why
            // wrapUp()'s merge branch was already reachable before that insert existed.
            Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, Rosti.getInitialValueSlot(pRosti, 0), 1));
            seedSingleSlot(pRosti, VALUE_OFFSET, seedSum, seedCompensation, 2);

            Assert.assertTrue(Rosti.keyedIntNSumDoubleWrapUp(pRosti, VALUE_OFFSET, valueAtNull, 3, valueAtNullC));

            Assert.assertEquals(expected, readSingleSlotDouble(pRosti, VALUE_OFFSET), 0.0);
            // The sweep folds the compensation into the value and leaves the field itself alone,
            // so this reads what the populate step decided. The step's own contribution can never
            // show up in the value: it is the rounding residual of a single addition, so at most
            // half an ulp of that value, and folding it back rounds straight off again. Pinning
            // the field is the only way to see which arm ran.
            Assert.assertEquals(expectedCompensation, readSingleSlotDouble(pRosti, VALUE_OFFSET + 1), 0.0);
        } finally {
            Rosti.free(pRosti);
        }
    }

    // Seeds liveKeys empty groups, wraps up with a null value the populate branch acts on, and
    // checks that the sweep reached every live group. Returns whether the populate resized the
    // map, which is the arrangement the caller is really after. hasPreSeededNullKey inserts the
    // null key up front, so the populate finds an existing count-0 slot instead of inserting one.
    private static boolean assertWrapUpSweepsEveryGroup(WrapUpCase c, int liveKeys, boolean hasPreSeededNullKey) {
        // The key buffer first: an allocation failure between the two would strand whichever came
        // before it, and the rosti is the one nothing else would free.
        final long keysSize = 4L * liveKeys;
        final long pKeys = Unsafe.malloc(keysSize, MemoryTag.NATIVE_DEFAULT);
        final long pRosti;
        final long sizeAtAlloc;
        try {
            pRosti = Rosti.alloc(c.types, 16);
            Assert.assertNotEquals(0, pRosti);
            sizeAtAlloc = Rosti.getAllocMemory(pRosti);
        } catch (Throwable th) {
            Unsafe.free(pKeys, keysSize, MemoryTag.NATIVE_DEFAULT);
            throw th;
        }
        try {
            Unsafe.putInt(Rosti.getInitialValueSlot(pRosti, 0), Numbers.INT_NULL);
            c.initializer.init(pRosti);
            // What an untouched group's value field looks like, which is what the sweep must
            // overwrite. Read after the initializer, so each case supplies its own.
            final long emptyValue = Unsafe.getLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET));

            if (hasPreSeededNullKey) {
                Assert.assertTrue(c.name, Rosti.keyedIntDistinct(pRosti, Rosti.getInitialValueSlot(pRosti, 0), 1));
            }
            for (int i = 0; i < liveKeys; i++) {
                Unsafe.putInt(pKeys + 4L * i, i + 1);
            }
            Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, pKeys, liveKeys));

            final long sizeBeforeWrapUp = Rosti.getAllocMemory(pRosti);
            Assert.assertTrue(c.name, c.invoker.wrapUp(pRosti));
            final boolean hasResizedInWrapUp = Rosti.getAllocMemory(pRosti) > sizeBeforeWrapUp;

            final long ctrl = Rosti.getCtrl(pRosti);
            final long slots = Rosti.getSlots(pRosti);
            final long shift = Rosti.getSlotShift(pRosti);
            final long capacity = Rosti.getCapacity(pRosti);
            final int valueField = slotFieldOffset(pRosti, VALUE_OFFSET);
            int liveSlots = 0;
            int sweptGroups = 0;
            boolean hasNullKey = false;
            for (long i = 0; i < capacity; i++) {
                if (Unsafe.getByte(ctrl + i) > -1) {
                    final long slot = slots + (i << shift);
                    final long value = Unsafe.getLong(slot + valueField);
                    liveSlots++;
                    if (Unsafe.getInt(slot) == Numbers.INT_NULL) {
                        hasNullKey = true;
                        if (c.hasPinnedValues) {
                            Assert.assertEquals("null group value [case=" + c.name
                                    + ", liveKeys=" + liveKeys + ']', c.nullGroupValue, value);
                        }
                    } else {
                        Assert.assertNotEquals("group left un-swept [case=" + c.name
                                        + ", liveKeys=" + liveKeys + ", key=" + Unsafe.getInt(slot) + ']',
                                emptyValue, value);
                        if (c.hasPinnedValues && value == c.sweptValue) {
                            sweptGroups++;
                        }
                    }
                }
            }
            Assert.assertTrue("null-key group missing [case=" + c.name + ", liveKeys=" + liveKeys + ']', hasNullKey);
            Assert.assertEquals("live slot count [case=" + c.name + ", liveKeys=" + liveKeys + ']',
                    liveKeys + 1, liveSlots);
            if (c.hasPinnedValues) {
                // Every seeded key had count 0, so the sweep owed each one its NULL sentinel.
                Assert.assertEquals("swept groups [case=" + c.name + ", liveKeys=" + liveKeys + ']',
                        liveKeys, sweptGroups);
            }
            return hasResizedInWrapUp;
        } finally {
            Unsafe.free(pKeys, keysSize, MemoryTag.NATIVE_DEFAULT);
            // Rosti.alloc() recorded the pre-resize size and Rosti.free() subtracts the current
            // one, so the growth these inserts caused has to be recorded too. Production does
            // this via the same helper, through RostiAllocFacade.
            Rosti.updateMemoryUsage(pRosti, sizeAtAlloc);
            Rosti.free(pRosti);
        }
    }

    private static long findSingleSlot(long pRosti) {
        final long ctrl = Rosti.getCtrl(pRosti);
        final long slots = Rosti.getSlots(pRosti);
        final long shift = Rosti.getSlotShift(pRosti);
        final long capacity = Rosti.getCapacity(pRosti);
        long slot = 0;
        int liveSlots = 0;
        for (long i = 0; i < capacity; i++) {
            if (Unsafe.getByte(ctrl + i) > -1) {
                slot = slots + (i << shift);
                liveSlots++;
            }
        }
        Assert.assertEquals("expected exactly one live slot", 1, liveSlots);
        return slot;
    }

    private static void initCompensatedSumSlot(long pRosti) {
        Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0.0);
        Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0.0);
        Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 2), 0);
    }

    private static double readSingleSlotDouble(long pRosti, int valueOffset) {
        return Unsafe.getDouble(findSingleSlot(pRosti) + slotFieldOffset(pRosti, valueOffset));
    }

    private static long readSingleSlotLong(long pRosti, int valueOffset) {
        return Unsafe.getLong(findSingleSlot(pRosti) + slotFieldOffset(pRosti, valueOffset));
    }

    private static void seedSingleSlot(long pRosti, int valueOffset, double sum, double compensation, long count) {
        final long slot = findSingleSlot(pRosti);
        Unsafe.putDouble(slot + slotFieldOffset(pRosti, valueOffset), sum);
        Unsafe.putDouble(slot + slotFieldOffset(pRosti, valueOffset + 1), compensation);
        Unsafe.putLong(slot + slotFieldOffset(pRosti, valueOffset + 2), count);
    }

    private static void seedSumIntSlot(long pRosti, long sum, long count) {
        final long slot = findSingleSlot(pRosti);
        Unsafe.putLong(slot + slotFieldOffset(pRosti, VALUE_OFFSET), sum);
        Unsafe.putLong(slot + slotFieldOffset(pRosti, VALUE_OFFSET + 1), count);
    }

    private static int slotFieldOffset(long pRosti, int columnIndex) {
        return Unsafe.getInt(Rosti.getValueOffsets(pRosti) + columnIndex * 4L);
    }

    // The one case whose whole slot reads back as a raw long, so it can pin the swept sentinel
    // and the null group's own value on top of the property every case shares.
    private static WrapUpCase sumLongWrapUpCase() {
        return new WrapUpCase(
                "sum(long)",
                types(ColumnType.LONG, ColumnType.LONG),
                pRosti -> {
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0);
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);
                },
                pRosti -> Rosti.keyedIntSumLongWrapUp(pRosti, VALUE_OFFSET, 7, 1)
        ).pinValues(7, Numbers.LONG_NULL);
    }

    private static ArrayColumnTypes types(int... valueTypes) {
        final ArrayColumnTypes types = new ArrayColumnTypes();
        types.add(ColumnType.INT); // key
        for (int valueType : valueTypes) {
            types.add(valueType);
        }
        return types;
    }

    // One entry per sweeping wrapUp, each mirroring its aggregate's pushValueTypes() and
    // initRosti(). The null value passed to each wrapUp is one its populate branch acts on, so
    // every case inserts the null key and can resize while doing it.
    private static ObjList<WrapUpCase> wrapUpCases() {
        final ObjList<WrapUpCase> cases = new ObjList<>();
        cases.add(sumLongWrapUpCase());
        cases.add(new WrapUpCase(
                "sum(long256)",
                types(ColumnType.LONG256, ColumnType.LONG),
                pRosti -> {
                    final long slot = Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET);
                    for (int i = 0; i < 4; i++) {
                        Unsafe.putLong(slot + (long) i * Long.BYTES, 0);
                    }
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);
                },
                pRosti -> Rosti.keyedIntSumLong256WrapUp(pRosti, VALUE_OFFSET, 7, 0, 0, 0, 1)
        ));
        cases.add(new WrapUpCase(
                "sum(double)",
                types(ColumnType.DOUBLE, ColumnType.LONG),
                pRosti -> {
                    Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0);
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);
                },
                pRosti -> Rosti.keyedIntSumDoubleWrapUp(pRosti, VALUE_OFFSET, 7.0, 1)
        ));
        cases.add(new WrapUpCase(
                "ksum(double)",
                types(ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.LONG),
                RostiTest::initCompensatedSumSlot,
                pRosti -> Rosti.keyedIntKSumDoubleWrapUp(pRosti, VALUE_OFFSET, 7.0, 1)
        ));
        cases.add(new WrapUpCase(
                "nsum(double)",
                types(ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.LONG),
                RostiTest::initCompensatedSumSlot,
                pRosti -> Rosti.keyedIntNSumDoubleWrapUp(pRosti, VALUE_OFFSET, 7.0, 1, 0.0)
        ));
        cases.add(new WrapUpCase(
                "avg(int)",
                types(ColumnType.DOUBLE, ColumnType.LONG),
                pRosti -> {
                    // avg spaces its accumulator out as a long and the sweep replaces it with the
                    // quotient, so initRosti() writes longs into a DOUBLE slot on purpose.
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0);
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);
                },
                pRosti -> Rosti.keyedIntAvgLongWrapUp(pRosti, VALUE_OFFSET, 7.0, 1)
        ));
        cases.add(new WrapUpCase(
                "avg(long)",
                types(ColumnType.LONG, ColumnType.LONG, ColumnType.LONG),
                pRosti -> {
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0);
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 2), 0);
                },
                pRosti -> Rosti.keyedIntAvgLongLongWrapUp(pRosti, VALUE_OFFSET, 7.0, 1)
        ));
        cases.add(new WrapUpCase(
                "avg(double)",
                types(ColumnType.DOUBLE, ColumnType.LONG),
                pRosti -> {
                    Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0);
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);
                },
                pRosti -> Rosti.keyedIntAvgDoubleWrapUp(pRosti, VALUE_OFFSET, 7.0, 1)
        ));
        cases.add(new WrapUpCase(
                "min(double)",
                types(ColumnType.DOUBLE),
                pRosti -> Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), Double.POSITIVE_INFINITY),
                pRosti -> Rosti.keyedIntMinDoubleWrapUp(pRosti, VALUE_OFFSET, 7.0)
        ));
        cases.add(new WrapUpCase(
                "max(double)",
                types(ColumnType.DOUBLE),
                pRosti -> Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), Double.NEGATIVE_INFINITY),
                pRosti -> Rosti.keyedIntMaxDoubleWrapUp(pRosti, VALUE_OFFSET, 7.0)
        ));
        return cases;
    }

    @FunctionalInterface
    private interface RostiInitializer {
        void init(long pRosti);
    }

    @FunctionalInterface
    private interface WrapUpInvoker {
        boolean wrapUp(long pRosti);
    }

    private static class WrapUpCase {
        private final RostiInitializer initializer;
        private final WrapUpInvoker invoker;
        private final String name;
        private final ArrayColumnTypes types;
        private boolean hasPinnedValues;
        private long nullGroupValue;
        private long sweptValue;

        private WrapUpCase(String name, ArrayColumnTypes types, RostiInitializer initializer, WrapUpInvoker invoker) {
            this.name = name;
            this.types = types;
            this.initializer = initializer;
            this.invoker = invoker;
        }

        // Pins what the null group and a swept group hold once wrapUp() returns, for the cases
        // whose value field reads back as a raw long.
        private WrapUpCase pinValues(long nullGroupValue, long sweptValue) {
            this.hasPinnedValues = true;
            this.nullGroupValue = nullGroupValue;
            this.sweptValue = sweptValue;
            return this;
        }
    }
}
