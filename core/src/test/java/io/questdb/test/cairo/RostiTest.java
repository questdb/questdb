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
                    Assert.assertEquals(0.0, readSingleSlotDouble(pRostiA, VALUE_OFFSET + 1), 0.0);
                    Assert.assertEquals(3, readSingleSlotLong(pRostiA, VALUE_OFFSET + 2));
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
        // Pins two internal invariants of the ksum slot rather than a query result: ksum reports
        // the running sum alone, and the sweep reads the count only to test it against zero, so
        // neither field below reaches a user today.
        //
        // The compensation must be written back, or the slot is left claiming a correction for a
        // value it no longer sits beside. Seeding a NON-ZERO c is what makes that visible: the
        // Kahan step starts from y = valueAtNull - c, so a zero seed leaves that read dead.
        //
        // The count must gain the whole folded count. valueAtNullCount is the number of column-top
        // page frames the non-keyed accumulator absorbed -- aggregate() bumps it once per frame,
        // not once per row -- so it does not match the keyed path's per-row count either way;
        // += 1 dropped all but the first frame, and this keeps it monotone in what was folded in.
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
                Assert.assertEquals(5, readSingleSlotLong(pRosti, VALUE_OFFSET + 2));
            } finally {
                Rosti.free(pRosti);
            }
        });
    }

    @Test
    public void testKeyedIntNSumDoubleMergeCarriesSourceCompensation() throws Exception {
        // Each worker shard accumulates its own (sum, c) pair, and merge() folds shard B into
        // shard A. B's total is its sum plus its compensation, so dropping the compensation
        // loses real value. merge() is key-agnostic, so this needs no column top at all -- it
        // is the ordinary multi-worker path taken whenever one group's rows span two shards.
        // (The key here happens to be INT_NULL only because it is the convenient seed.)
        assertMemoryLeak(() -> {
            // abs(sum) >= d: first arm of the Neumaier step.
            assertMergeCarriesCompensation(8.0, 0.25, 2.0, 0.5, 10.75);
            // abs(sum) < d: second arm.
            assertMergeCarriesCompensation(2.0, 0.25, 8.0, 0.5, 10.75);
        });
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
            // abs(sum) >= valueAtNull: takes the first arm of the Neumaier step.
            assertWrapUpFoldsIntoSeededSlot(8.0, 0.25, 2.0, 0.5, 10.75);
            // abs(sum) < valueAtNull: takes the second arm.
            assertWrapUpFoldsIntoSeededSlot(2.0, 0.25, 8.0, 0.5, 10.75);
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
        // group's rows to a chosen shard: the slot comes from whichever pool thread dequeues the
        // task, plus the inline work-stealing path. A SQL test could never fail spuriously -- a
        // single-shard group skips the merge entirely -- but it would often pass without
        // exercising the merge at all.
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
    public void testEveryWrapUpSweepSurvivesNullKeyInsertResize() throws Exception {
        // Same property as testKeyedIntSumLongWrapUpSweepSurvivesNullKeyInsertResize, across all
        // nine sweeping wrapUps rather than the one. Each seeds its own slot layout, so the
        // assertion is expressed in the one term they share: the sweep visits every live group,
        // and an empty group is one the sweep has not yet touched, still holding the value
        // initRosti() wrote. A sweep walking a snapshot taken before the populate resized would
        // leave the live ones behind.
        assertMemoryLeak(() -> {
            final ObjList<WrapUpCase> cases = wrapUpCases();
            for (int i = 0, n = cases.size(); i < n; i++) {
                final WrapUpCase c = cases.getQuick(i);
                boolean hasResizedInWrapUp = false;
                for (int liveKeys = 8; liveKeys <= 20; liveKeys++) {
                    hasResizedInWrapUp |= assertWrapUpSweepsEveryGroup(c, liveKeys);
                }
                Assert.assertTrue("no iteration resized inside wrapUp() [case=" + c.name
                        + "] -- widen the liveKeys sweep", hasResizedInWrapUp);
            }
        });
    }

    @Test
    public void testKeyedIntSumLongWrapUpSweepSurvivesNullKeyInsertResize() throws Exception {
        // wrapUp() populates the null-key slot and then sweeps the map, NULLing every group
        // whose count is still 0. The populate inserts, and an insert past the growth threshold
        // resizes: that reallocates ctrl_ and slots_, frees the old block and doubles capacity_.
        // A sweep reading a snapshot taken before the populate walks freed memory and leaves the
        // live count-0 groups un-NULLed.
        //
        // Capacity is ceilPow2(16) - 1 = 15 and the threshold is capacity - capacity/8 = 14, so
        // the sweep below straddles the resize. This case also pins the swept value itself
        // (LONG_NULL) and the null group's own value, which the all-wrapUps sweep above does not.
        assertMemoryLeak(() -> {
            boolean hasResizedInWrapUp = false;
            for (int liveKeys = 8; liveKeys <= 20; liveKeys++) {
                hasResizedInWrapUp |= assertSweepNullsEmptyGroupsAcrossResize(liveKeys);
            }
            // Only one point in the sweep puts the resize inside wrapUp(); above it the inserts
            // resize first. Fail loudly rather than silently degenerate if the growth math moves.
            Assert.assertTrue("no iteration resized inside wrapUp() -- widen the liveKeys sweep",
                    hasResizedInWrapUp);
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

    private static void initCompensatedSumSlot(long pRosti) {
        Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0.0);
        Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0.0);
        Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 2), 0);
    }

    private static ArrayColumnTypes types(int... valueTypes) {
        final ArrayColumnTypes types = new ArrayColumnTypes();
        types.add(ColumnType.INT); // key
        for (int valueType : valueTypes) {
            types.add(valueType);
        }
        return types;
    }

    // ksum and nsum share this slot layout, so both drive the same helper.
    private static long allocCompensatedSumRosti() {
        final ArrayColumnTypes types = new ArrayColumnTypes();
        types.add(ColumnType.INT);      // key
        types.add(ColumnType.DOUBLE);   // running sum
        types.add(ColumnType.DOUBLE);   // Neumaier compensation
        types.add(ColumnType.LONG);     // count
        final long pRosti = Rosti.alloc(types, 64);
        Assert.assertNotEquals(0, pRosti);
        // Mirrors GroupByRecordCursorFactory's null-key setup and
        // NSumDoubleVectorAggregateFunction.initRosti().
        Unsafe.putInt(Rosti.getInitialValueSlot(pRosti, 0), Numbers.INT_NULL);
        Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0.0);
        Unsafe.putDouble(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0.0);
        Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 2), 0);
        return pRosti;
    }

    // sum(int) and avg(int) share this slot layout and the same merge.
    private static long allocSumIntRosti() {
        final ArrayColumnTypes types = new ArrayColumnTypes();
        types.add(ColumnType.INT);      // key
        types.add(ColumnType.LONG);     // running sum, 64-bit even though the column is INT
        types.add(ColumnType.LONG);     // count
        final long pRosti = Rosti.alloc(types, 64);
        Assert.assertNotEquals(0, pRosti);
        // Mirrors GroupByRecordCursorFactory's null-key setup and
        // SumIntVectorAggregateFunction.initRosti().
        Unsafe.putInt(Rosti.getInitialValueSlot(pRosti, 0), Numbers.INT_NULL);
        Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0);
        Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);
        return pRosti;
    }

    private static void assertMergeCarriesCompensation(
            double sumA,
            double compensationA,
            double sumB,
            double compensationB,
            double expected
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

    // Seeds liveKeys empty groups, wraps up with a null value that has to be inserted, and checks
    // that the sweep reached every live group. Returns whether the populate resized the map, which
    // is the arrangement the caller is really after.
    private static boolean assertWrapUpSweepsEveryGroup(WrapUpCase c, int liveKeys) {
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
            boolean hasNullKey = false;
            for (long i = 0; i < capacity; i++) {
                if (Unsafe.getByte(ctrl + i) > -1) {
                    final long slot = slots + (i << shift);
                    liveSlots++;
                    if (Unsafe.getInt(slot) == Numbers.INT_NULL) {
                        hasNullKey = true;
                    } else {
                        Assert.assertNotEquals("group left un-swept [case=" + c.name
                                        + ", liveKeys=" + liveKeys + ", key=" + Unsafe.getInt(slot) + ']',
                                emptyValue, Unsafe.getLong(slot + valueField));
                    }
                }
            }
            Assert.assertTrue("null-key group missing [case=" + c.name + ", liveKeys=" + liveKeys + ']', hasNullKey);
            Assert.assertEquals("live slot count [case=" + c.name + ", liveKeys=" + liveKeys + ']',
                    liveKeys + 1, liveSlots);
            return hasResizedInWrapUp;
        } finally {
            Unsafe.free(pKeys, keysSize, MemoryTag.NATIVE_DEFAULT);
            Rosti.updateMemoryUsage(pRosti, sizeAtAlloc);
            Rosti.free(pRosti);
        }
    }

    private static boolean assertSweepNullsEmptyGroupsAcrossResize(int liveKeys) {
        final ArrayColumnTypes types = new ArrayColumnTypes();
        types.add(ColumnType.INT);      // key
        types.add(ColumnType.LONG);     // sum
        types.add(ColumnType.LONG);     // count
        // The key buffer first: an allocation failure between the two would strand whichever came
        // before it, and the rosti is the one nothing else would free.
        final long keysSize = 4L * liveKeys;
        final long pKeys = Unsafe.malloc(keysSize, MemoryTag.NATIVE_DEFAULT);
        final long pRosti;
        final long sizeAtAlloc;
        try {
            pRosti = Rosti.alloc(types, 16);
            Assert.assertNotEquals(0, pRosti);
            sizeAtAlloc = Rosti.getAllocMemory(pRosti);
        } catch (Throwable th) {
            Unsafe.free(pKeys, keysSize, MemoryTag.NATIVE_DEFAULT);
            throw th;
        }
        try {
            // Mirrors SumLongVectorAggregateFunction.initRosti() plus the factory's null key.
            Unsafe.putInt(Rosti.getInitialValueSlot(pRosti, 0), Numbers.INT_NULL);
            Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0);
            Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);

            for (int i = 0; i < liveKeys; i++) {
                Unsafe.putInt(pKeys + 4L * i, i + 1);
            }
            Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, pKeys, liveKeys));

            // The null key is absent, so the populate must insert it -- resizing once the live
            // keys have filled the map to its growth threshold.
            final long sizeBeforeWrapUp = Rosti.getAllocMemory(pRosti);
            Assert.assertTrue(Rosti.keyedIntSumLongWrapUp(pRosti, VALUE_OFFSET, 7, 1));
            final boolean hasResizedInWrapUp = Rosti.getAllocMemory(pRosti) > sizeBeforeWrapUp;

            final long ctrl = Rosti.getCtrl(pRosti);
            final long slots = Rosti.getSlots(pRosti);
            final long shift = Rosti.getSlotShift(pRosti);
            final long capacity = Rosti.getCapacity(pRosti);
            final int valueField = slotFieldOffset(pRosti, VALUE_OFFSET);
            int liveSlots = 0;
            int nulledGroups = 0;
            boolean hasNullKey = false;
            for (long i = 0; i < capacity; i++) {
                if (Unsafe.getByte(ctrl + i) > -1) {
                    final long slot = slots + (i << shift);
                    final long value = Unsafe.getLong(slot + valueField);
                    liveSlots++;
                    if (Unsafe.getInt(slot) == Numbers.INT_NULL) {
                        hasNullKey = true;
                        Assert.assertEquals(7, value);
                    } else if (value == Numbers.LONG_NULL) {
                        nulledGroups++;
                    }
                }
            }
            Assert.assertTrue("null-key group missing [liveKeys=" + liveKeys + ']', hasNullKey);
            Assert.assertEquals("live slot count [liveKeys=" + liveKeys + ']', liveKeys + 1, liveSlots);
            // Every seeded key had count 0, so the sweep owed each one a NULL.
            Assert.assertEquals("swept groups [liveKeys=" + liveKeys + ']', liveKeys, nulledGroups);
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

    private static void assertWrapUpFoldsIntoSeededSlot(
            double seedSum,
            double seedCompensation,
            double valueAtNull,
            double valueAtNullC,
            double expected
    ) {
        final long pRosti = allocCompensatedSumRosti();
        try {
            // The insert GroupByRecordCursorFactory performs once when a frame's key column is a column top.
            // A stored NULL int key reaches the same slot through kIntNSumDouble, which is why
            // wrapUp()'s merge branch was already reachable before that insert existed.
            Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, Rosti.getInitialValueSlot(pRosti, 0), 1));
            seedSingleSlot(pRosti, VALUE_OFFSET, seedSum, seedCompensation, 2);

            // Folding three frames' worth, so the count is pinned the way ksum's is: the slot has
            // to gain the whole valueAtNullCount, not one.
            Assert.assertTrue(Rosti.keyedIntNSumDoubleWrapUp(pRosti, VALUE_OFFSET, valueAtNull, 3, valueAtNullC));

            Assert.assertEquals(expected, readSingleSlotDouble(pRosti, VALUE_OFFSET), 0.0);
            Assert.assertEquals(5, readSingleSlotLong(pRosti, VALUE_OFFSET + 2));
        } finally {
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

    // One entry per sweeping wrapUp, each mirroring its aggregate's pushValueTypes() and
    // initRosti(). The null value passed to each wrapUp is one its populate branch acts on, so
    // every case inserts the null key and can resize while doing it.
    private static ObjList<WrapUpCase> wrapUpCases() {
        final ObjList<WrapUpCase> cases = new ObjList<>();
        cases.add(new WrapUpCase(
                "sum(long)",
                types(ColumnType.LONG, ColumnType.LONG),
                pRosti -> {
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET), 0);
                    Unsafe.putLong(Rosti.getInitialValueSlot(pRosti, VALUE_OFFSET + 1), 0);
                },
                pRosti -> Rosti.keyedIntSumLongWrapUp(pRosti, VALUE_OFFSET, 7, 1)
        ));
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

    private static int slotFieldOffset(long pRosti, int columnIndex) {
        return Unsafe.getInt(Rosti.getValueOffsets(pRosti) + columnIndex * 4L);
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

        private WrapUpCase(String name, ArrayColumnTypes types, RostiInitializer initializer, WrapUpInvoker invoker) {
            this.name = name;
            this.types = types;
            this.initializer = initializer;
            this.invoker = invoker;
        }
    }
}
