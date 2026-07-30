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
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;


public class RostiTest extends AbstractCairoTest {

    private static final int VALUE_OFFSET = 1;

    @Test
    public void testKeyedIntKSumDoubleWrapUpAddsWholeFoldedCount() throws Exception {
        // valueAtNullCount is the number of column-top page frames the non-keyed accumulator
        // absorbed -- aggregate() bumps it once per frame, not once per row -- so it does not
        // match the keyed path's per-row count either way. += 1 dropped all but the first frame.
        // Nothing reads the field except the sweep's count == 0 test, so no query result
        // changes; this keeps it monotone in what was actually folded in.
        assertMemoryLeak(() -> {
            final long pRosti = allocCompensatedSumRosti();
            try {
                Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, Rosti.getInitialValueSlot(pRosti, 0), 1));

                Assert.assertTrue(Rosti.keyedIntKSumDoubleWrapUp(pRosti, VALUE_OFFSET, 2.5, 3));

                Assert.assertEquals(3, readSingleSlotLong(pRosti, VALUE_OFFSET + 2));
                Assert.assertEquals(2.5, readSingleSlotDouble(pRosti, VALUE_OFFSET), 0.0);
            } finally {
                Rosti.free(pRosti);
            }
        });
    }

    @Test
    public void testKeyedIntKSumDoubleWrapUpWritesBackCompensation() throws Exception {
        // Kahan keeps the answer in the running sum and the pending correction in c, so wrapUp()
        // must write c back rather than leave the slot claiming a correction for a value it no
        // longer sits beside. 2^53 + 1 rounds to 2^53, leaving a residual of exactly -1.
        assertMemoryLeak(() -> {
            final long pRosti = allocCompensatedSumRosti();
            try {
                Assert.assertTrue(Rosti.keyedIntDistinct(pRosti, Rosti.getInitialValueSlot(pRosti, 0), 1));
                seedSingleSlot(pRosti, VALUE_OFFSET, 9_007_199_254_740_992.0, 0.0, 2);

                Assert.assertTrue(Rosti.keyedIntKSumDoubleWrapUp(pRosti, VALUE_OFFSET, 1.0, 1));

                // The value is unchanged by the rounding, so only the compensation shows the bug.
                Assert.assertEquals(9_007_199_254_740_992.0, readSingleSlotDouble(pRosti, VALUE_OFFSET), 0.0);
                Assert.assertEquals(-1.0, readSingleSlotDouble(pRosti, VALUE_OFFSET + 1), 0.0);
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
        // No pre-existing slot, so wrapUp() takes its insert branch. That branch always stored
        // valueAtNullC; the merge branch above did not. Both must agree on the same inputs.
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
    public void testKeyedIntSumLongWrapUpSweepSurvivesNullKeyInsertResize() throws Exception {
        // wrapUp() populates the null-key slot and then sweeps the map, NULLing every group
        // whose count is still 0. The populate inserts, and an insert past the growth threshold
        // resizes: that reallocates ctrl_ and slots_, frees the old block and doubles capacity_.
        // A sweep reading a snapshot taken before the populate walks freed memory and leaves the
        // live count-0 groups un-NULLed.
        //
        // Capacity is ceilPow2(16) - 1 = 15 and the threshold is capacity - capacity/8 = 14, so
        // the sweep below straddles the resize.
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

    private static boolean assertSweepNullsEmptyGroupsAcrossResize(int liveKeys) {
        final ArrayColumnTypes types = new ArrayColumnTypes();
        types.add(ColumnType.INT);      // key
        types.add(ColumnType.LONG);     // sum
        types.add(ColumnType.LONG);     // count
        final long pRosti = Rosti.alloc(types, 16);
        Assert.assertNotEquals(0, pRosti);
        final long sizeAtAlloc = Rosti.getAllocMemory(pRosti);
        final long keysSize = 4L * liveKeys;
        final long pKeys = Unsafe.malloc(keysSize, MemoryTag.NATIVE_DEFAULT);
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

            Assert.assertTrue(Rosti.keyedIntNSumDoubleWrapUp(pRosti, VALUE_OFFSET, valueAtNull, 1, valueAtNullC));

            Assert.assertEquals(expected, readSingleSlotDouble(pRosti, VALUE_OFFSET), 0.0);
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

    private static int slotFieldOffset(long pRosti, int columnIndex) {
        return Unsafe.getInt(Rosti.getValueOffsets(pRosti) + columnIndex * 4L);
    }
}
