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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.engine.table.HashJoinTupleList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The ring entry of a parallel hash join's tuples: a tuple of a probe row and an entry per stage, in
 * slots of 4 bytes, which sign-extend as they are read and report each value that does not fit, or
 * of 8 bytes; the memory that grows as a slot appends, keeps the tuples, and charges the execution's
 * tracker; and the append address that a slot hands back once it is done.
 */
public class HashJoinTupleListTest extends AbstractCairoTest {
    // A tuple of a probe row and two stages' entries.
    private static final int NARROW_TUPLE_BYTES = 3 * Integer.BYTES;
    // 64 less the 32 bits of a narrow slot.
    private static final int NARROW_SHIFT = Integer.SIZE;
    private static final int WIDE_TUPLE_BYTES = 3 * Long.BYTES;

    @Test
    public void testCloseReleasesTheMemoryAndClearKeepsIt() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE)) {
                final HashJoinTupleList list = new HashJoinTupleList();
                list.of(true, NARROW_TUPLE_BYTES, NARROW_SHIFT, tracker);
                list.open(4);
                Assert.assertEquals(0, list.put(1, new long[]{2, 3}));
                Assert.assertEquals(4 * NARROW_TUPLE_BYTES, tracker.getUsed());
                // clear() forgets the tuples and keeps the memory.
                list.clear();
                Assert.assertEquals(0, list.size());
                Assert.assertEquals(list.getAddress(), list.getAppendAddress());
                Assert.assertEquals(4 * NARROW_TUPLE_BYTES, list.getCapacityBytes());
                list.close();
                Assert.assertEquals(0, list.getCapacityBytes());
                Assert.assertEquals(0, list.getAddress());
                Assert.assertEquals(0, tracker.getUsed());
                // Closing a closed list does nothing.
                list.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testGrowKeepsTheTuples() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isNarrow : new boolean[]{true, false}) {
                final int tupleBytes = isNarrow ? NARROW_TUPLE_BYTES : WIDE_TUPLE_BYTES;
                try (HashJoinTupleList list = new HashJoinTupleList()) {
                    list.of(isNarrow, tupleBytes, isNarrow ? NARROW_SHIFT : 0, null);
                    list.open(1);
                    Assert.assertEquals(tupleBytes, list.getCapacityBytes());
                    final long[] entries = new long[2];
                    for (int i = 0; i < 100; i++) {
                        entries[0] = i * 7L;
                        entries[1] = -2 - i;
                        Assert.assertEquals(0, list.put(i, entries));
                        // The memory doubles from one tuple's room, to the smallest power of two
                        // of tuples that holds them: 1, 2, 4, 4, 8, ..., 128.
                        final long capacityTuples = i == 0 ? 1 : Long.highestOneBit(i) << 1;
                        Assert.assertEquals(capacityTuples * tupleBytes, list.getCapacityBytes());
                    }
                    Assert.assertEquals(128L * tupleBytes, list.getCapacityBytes());
                    Assert.assertEquals(100, list.size());
                    Assert.assertEquals(100L * tupleBytes, list.getAppendAddress() - list.getAddress());
                    for (int i = 0; i < 100; i++) {
                        final long tupleAddress = list.getTupleAddress(i);
                        Assert.assertEquals(list.getAddress() + (long) i * tupleBytes, tupleAddress);
                        Assert.assertEquals(i, list.getProbeRow(tupleAddress));
                        Assert.assertEquals(i * 7L, list.getEntry(tupleAddress, 0));
                        Assert.assertEquals(-2 - i, list.getEntry(tupleAddress, 1));
                    }
                }
            }
        });
    }

    @Test
    public void testGrowHandsBackTheAppendAddress() throws Exception {
        assertMemoryLeak(() -> {
            try (HashJoinTupleList list = new HashJoinTupleList()) {
                list.of(true, NARROW_TUPLE_BYTES, NARROW_SHIFT, null);
                list.open(2);
                list.put(5, new long[]{6, -1});
                // A writer that kept its own append address, one tuple in, grows the list from there:
                // the list takes the address, keeps the tuple, and returns where the next one goes.
                final long next = list.grow(list.getAddress() + NARROW_TUPLE_BYTES);
                Assert.assertEquals(4 * NARROW_TUPLE_BYTES, list.getCapacityBytes());
                Assert.assertEquals(list.getAddress() + NARROW_TUPLE_BYTES, next);
                Assert.assertEquals(next, list.getAppendAddress());
                Assert.assertEquals(1, list.size());
                Assert.assertEquals(5, list.getProbeRow(list.getAddress()));
                Assert.assertEquals(-1, list.getEntry(list.getAddress(), 1));
            }
        });
    }

    @Test
    public void testMemoryLimitBreachLeavesTheTuples() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(2 * NARROW_TUPLE_BYTES)) {
                final HashJoinTupleList list = new HashJoinTupleList();
                try {
                    list.of(true, NARROW_TUPLE_BYTES, NARROW_SHIFT, tracker);
                    // Room for three tuples does not fit, and leaves the list closed.
                    assertOutOfMemory(() -> list.open(3));
                    Assert.assertEquals(0, list.getCapacityBytes());
                    Assert.assertEquals(0, tracker.getUsed());
                    list.open(2);
                    list.put(1, new long[]{10, -1});
                    list.put(2, new long[]{20, -2});
                    // The third tuple doubles the memory past the limit: the list keeps its tuples and
                    // its memory, and takes the tuple once the limit allows it.
                    assertOutOfMemory(() -> list.put(3, new long[]{30, -3}));
                    Assert.assertEquals(2, list.size());
                    Assert.assertEquals(2 * NARROW_TUPLE_BYTES, list.getCapacityBytes());
                    Assert.assertEquals(2 * NARROW_TUPLE_BYTES, tracker.getUsed());
                    Assert.assertEquals(20, list.getEntry(list.getTupleAddress(1), 0));
                    tracker.setLimit(Long.MAX_VALUE);
                    Assert.assertEquals(0, list.put(3, new long[]{30, -3}));
                    Assert.assertEquals(3, list.size());
                    Assert.assertEquals(4 * NARROW_TUPLE_BYTES, tracker.getUsed());
                    for (int i = 0; i < 3; i++) {
                        final long tupleAddress = list.getTupleAddress(i);
                        Assert.assertEquals(i + 1, list.getProbeRow(tupleAddress));
                        Assert.assertEquals((i + 1) * 10L, list.getEntry(tupleAddress, 0));
                        Assert.assertEquals(-(i + 1), list.getEntry(tupleAddress, 1));
                    }
                } finally {
                    list.close();
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testNarrowSlotsOfFewerBits() throws Exception {
        assertMemoryLeak(() -> {
            try (HashJoinTupleList list = new HashJoinTupleList()) {
                // Slots of 8 bits, as a test makes them: from -128 to 127.
                list.of(true, NARROW_TUPLE_BYTES, Long.SIZE - 8, null);
                list.open(8);
                Assert.assertEquals(0, list.put(127, new long[]{127, -128}));
                Assert.assertEquals(0, list.put(0, new long[]{0, -1}));
                Assert.assertNotEquals(0, list.put(128, new long[]{0, -1}));
                Assert.assertNotEquals(0, list.put(0, new long[]{128, -1}));
                Assert.assertNotEquals(0, list.put(0, new long[]{0, -129}));
                // The slots keep their 4 bytes: a value that fits 8 bits reads back as written, and
                // so does one that does not but fits 4 bytes.
                final long tupleAddress = list.getTupleAddress(0);
                Assert.assertEquals(127, list.getProbeRow(tupleAddress));
                Assert.assertEquals(127, list.getEntry(tupleAddress, 0));
                Assert.assertEquals(-128, list.getEntry(tupleAddress, 1));
                Assert.assertEquals(128, list.getProbeRow(list.getTupleAddress(2)));
                Assert.assertEquals(-129, list.getEntry(list.getTupleAddress(4), 1));
                Assert.assertEquals(Long.SIZE - 8, list.getNarrowShift());
            }
        });
    }

    @Test
    public void testNarrowSlotsReportEveryValueThatDoesNotFit() throws Exception {
        assertMemoryLeak(() -> {
            try (HashJoinTupleList list = new HashJoinTupleList()) {
                list.of(true, NARROW_TUPLE_BYTES, NARROW_SHIFT, null);
                list.open(16);
                final long[] misfits = {1L << 31, -(1L << 31) - 1, 1L << 32, Long.MAX_VALUE, Long.MIN_VALUE};
                for (long misfit : misfits) {
                    final String label = Long.toString(misfit);
                    // In the probe row's slot, and in each stage's.
                    Assert.assertNotEquals(label, 0, list.put(misfit, new long[]{0, -1}));
                    Assert.assertNotEquals(label, 0, list.put(0, new long[]{misfit, -1}));
                    Assert.assertNotEquals(label, 0, list.put(0, new long[]{0, misfit}));
                }
                Assert.assertEquals(15, list.size());
            }
        });
    }

    @Test
    public void testNarrowSlotsRoundTripTheirBoundsAndSigns() throws Exception {
        assertMemoryLeak(() -> {
            try (HashJoinTupleList list = new HashJoinTupleList()) {
                list.of(true, NARROW_TUPLE_BYTES, NARROW_SHIFT, null);
                Assert.assertTrue(list.isNarrow());
                Assert.assertEquals(NARROW_TUPLE_BYTES, list.getTupleBytes());
                list.open(4);
                // Probe rows and locators from 0 to INT's largest, a LEFT join's miss, -1, and tagged
                // chain heads from -2 to INT's smallest: each reads back with its sign.
                final long[][] tuples = {
                        {0, 0, -1},
                        {Integer.MAX_VALUE, Integer.MAX_VALUE, -2},
                        {1, -1, Integer.MIN_VALUE},
                        {Integer.MAX_VALUE - 1, Integer.MIN_VALUE + 1, Integer.MAX_VALUE},
                };
                for (long[] tuple : tuples) {
                    Assert.assertEquals(0, list.put(tuple[0], new long[]{tuple[1], tuple[2]}));
                }
                Assert.assertEquals(4, list.size());
                for (int i = 0; i < tuples.length; i++) {
                    final long tupleAddress = list.getTupleAddress(i);
                    Assert.assertEquals(list.getAddress() + (long) i * NARROW_TUPLE_BYTES, tupleAddress);
                    Assert.assertEquals(tuples[i][0], list.getProbeRow(tupleAddress));
                    Assert.assertEquals(tuples[i][0], HashJoinTupleList.getProbeRow(tupleAddress, true));
                    for (int stage = 0; stage < 2; stage++) {
                        Assert.assertEquals(tuples[i][stage + 1], list.getEntry(tupleAddress, stage));
                        Assert.assertEquals(tuples[i][stage + 1], HashJoinTupleList.getEntry(tupleAddress, stage, true));
                    }
                }
            }
        });
    }

    @Test
    public void testOpenKeepsLargerMemoryAndReplacesSmaller() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 HashJoinTupleList list = new HashJoinTupleList()) {
                list.of(false, WIDE_TUPLE_BYTES, 0, tracker);
                // Room for no tuple still takes one tuple's.
                list.open(0);
                Assert.assertEquals(WIDE_TUPLE_BYTES, list.getCapacityBytes());
                list.open(8);
                Assert.assertEquals(8 * WIDE_TUPLE_BYTES, list.getCapacityBytes());
                list.put(1, new long[]{2, 3});
                // Less room than the list holds empties it and keeps its memory.
                list.open(3);
                Assert.assertEquals(0, list.size());
                Assert.assertEquals(8 * WIDE_TUPLE_BYTES, list.getCapacityBytes());
                // More room replaces the memory with exactly that room.
                list.open(20);
                Assert.assertEquals(0, list.size());
                Assert.assertEquals(20 * WIDE_TUPLE_BYTES, list.getCapacityBytes());
                Assert.assertEquals(20 * WIDE_TUPLE_BYTES, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSetAppendAddressTakesTupleBoundariesWithinTheMemory() throws Exception {
        assertMemoryLeak(() -> {
            try (HashJoinTupleList list = new HashJoinTupleList()) {
                list.of(true, NARROW_TUPLE_BYTES, NARROW_SHIFT, null);
                list.open(4);
                final long address = list.getAddress();
                list.setAppendAddress(address + 3 * NARROW_TUPLE_BYTES);
                Assert.assertEquals(3, list.size());
                list.setAppendAddress(address + 4 * NARROW_TUPLE_BYTES);
                Assert.assertEquals(4, list.size());
                // Mid-tuple, before the memory and past it.
                Assert.assertThrows(AssertionError.class, () -> list.setAppendAddress(address + NARROW_TUPLE_BYTES / 2));
                Assert.assertThrows(AssertionError.class, () -> list.setAppendAddress(address - NARROW_TUPLE_BYTES));
                Assert.assertThrows(AssertionError.class, () -> list.setAppendAddress(address + 5 * NARROW_TUPLE_BYTES));
                Assert.assertEquals(4, list.size());
            }
        });
    }

    @Test
    public void testWideSlotsRoundTripEveryLong() throws Exception {
        assertMemoryLeak(() -> {
            try (HashJoinTupleList list = new HashJoinTupleList()) {
                list.of(false, WIDE_TUPLE_BYTES, 0, null);
                Assert.assertFalse(list.isNarrow());
                list.open(2);
                Assert.assertEquals(0, list.put(Long.MAX_VALUE, new long[]{1L << 44, Long.MIN_VALUE}));
                Assert.assertEquals(0, list.put(1L << 31, new long[]{-1, -(1L << 31) - 1}));
                long tupleAddress = list.getTupleAddress(0);
                Assert.assertEquals(Long.MAX_VALUE, list.getProbeRow(tupleAddress));
                Assert.assertEquals(1L << 44, list.getEntry(tupleAddress, 0));
                Assert.assertEquals(Long.MIN_VALUE, HashJoinTupleList.getEntry(tupleAddress, 1, false));
                tupleAddress = list.getTupleAddress(1);
                Assert.assertEquals(1L << 31, HashJoinTupleList.getProbeRow(tupleAddress, false));
                Assert.assertEquals(-1, list.getEntry(tupleAddress, 0));
                Assert.assertEquals(-(1L << 31) - 1, list.getEntry(tupleAddress, 1));
            }
        });
    }

    private static void assertOutOfMemory(Runnable allocation) {
        final CairoException e = Assert.assertThrows(CairoException.class, allocation::run);
        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
    }
}
