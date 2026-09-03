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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointMutationArena;
import io.questdb.std.IntObjHashMap;
import io.questdb.test.tools.LimitedMemoryTracker;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class LiveViewCheckpointMutationArenaTest {
    private static final byte[] NO_BYTES = new byte[0];

    @Test
    public void testArenaGrowthFailureReleasesTrackerAndCanBeReused() {
        try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1)) {
            final LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena(tracker);
            try {
                try {
                    arena.put(intKey(1), NO_BYTES);
                    Assert.fail();
                } catch (CairoException ignored) {
                }
                tracker.setLimit(Long.MAX_VALUE);
                arena.clear();
                arena.put(intKey(1), NO_BYTES);
                arena.sortAndValidateForTest();
                Assert.assertTrue(tracker.getUsed() > 0);
            } finally {
                arena.close();
            }
            Assert.assertEquals(0, tracker.getUsed());
        }
    }

    @Test
    public void testDuplicateRejected() {
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
            arena.put(intKey(1), NO_BYTES);
            arena.put(intKey(1), NO_BYTES);
            try {
                arena.sortAndValidateForTest();
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(e.getFlyweightMessage().toString().contains("duplicate"));
            }
        }
    }

    @Test
    public void testEmptySingleSortedReverseAndHighCardinality() {
        assertSorted(0, false);
        assertSorted(1, false);
        assertSorted(1_000, false);
        assertSorted(1_000, true);
        assertSorted(1_000_000, true);
    }

    @Test
    public void testAnchorKeyLengthBoundaryIsValidatedBeforeAppend() {
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
            arena.putAnchor(new byte[1 << 20], 42);
            arena.sortAndValidateForTest();
            Assert.assertEquals(1, arena.getMutationCount());

            arena.clear();
            try {
                arena.putAnchor(new byte[(1 << 20) + 1], 42);
                Assert.fail("expected oversized anchor key rejection");
            } catch (CairoException e) {
                Assert.assertTrue(e.getFlyweightMessage().toString().contains("partition key length out of bounds"));
            }
            Assert.assertEquals("validation must run before native append", 0, arena.getMutationCount());
        }
    }

    @Test
    public void testFrozenBytePoolReusesWarmedWidthsAcrossPermutations() throws Exception {
        final Class<?> poolClass = Class.forName("io.questdb.cairo.lv.LiveViewCheckpointByteArrayPool");
        final Constructor<?> constructor = poolClass.getDeclaredConstructor();
        final Method next = poolClass.getDeclaredMethod("next", int.class);
        final Method reset = poolClass.getDeclaredMethod("reset");
        constructor.setAccessible(true);
        next.setAccessible(true);
        reset.setAccessible(true);
        final Object pool = constructor.newInstance();

        final byte[] width1a = (byte[]) next.invoke(pool, 1);
        final byte[] width2 = (byte[]) next.invoke(pool, 2);
        final byte[] width1b = (byte[]) next.invoke(pool, 1);
        reset.invoke(pool);

        Assert.assertSame(width2, next.invoke(pool, 2));
        final byte[] permutedWidth1a = (byte[]) next.invoke(pool, 1);
        final byte[] permutedWidth1b = (byte[]) next.invoke(pool, 1);
        Assert.assertTrue(permutedWidth1a == width1a || permutedWidth1a == width1b);
        Assert.assertTrue(permutedWidth1b == width1a || permutedWidth1b == width1b);
        Assert.assertNotSame(permutedWidth1a, permutedWidth1b);

        reset.invoke(pool);
        final byte[] nextWidth1a = (byte[]) next.invoke(pool, 1);
        final byte[] nextWidth1b = (byte[]) next.invoke(pool, 1);
        Assert.assertTrue(nextWidth1a == width1a || nextWidth1a == width1b);
        Assert.assertTrue(nextWidth1b == width1a || nextWidth1b == width1b);
        Assert.assertNotSame(nextWidth1a, nextWidth1b);
        Assert.assertSame(width2, next.invoke(pool, 2));
    }

    @Test
    public void testFrozenBytePoolRetainsExactWidthsAcrossEpochRollover() throws Exception {
        final Class<?> poolClass = Class.forName("io.questdb.cairo.lv.LiveViewCheckpointByteArrayPool");
        final Constructor<?> constructor = poolClass.getDeclaredConstructor();
        final Field epoch = poolClass.getDeclaredField("epoch");
        final Field poolsByWidth = poolClass.getDeclaredField("poolsByWidth");
        final Method next = poolClass.getDeclaredMethod("next", int.class);
        final Method reset = poolClass.getDeclaredMethod("reset");
        constructor.setAccessible(true);
        epoch.setAccessible(true);
        poolsByWidth.setAccessible(true);
        next.setAccessible(true);
        reset.setAccessible(true);
        final Object pool = constructor.newInstance();

        final byte[] width7a = (byte[]) next.invoke(pool, 7);
        final byte[] width7b = (byte[]) next.invoke(pool, 7);
        final byte[] width11 = (byte[]) next.invoke(pool, 11);
        final IntObjHashMap<?> widths = (IntObjHashMap<?>) poolsByWidth.get(pool);
        final Object width7Pool = widths.get(7);
        final Object width11Pool = widths.get(11);
        final Field arrays = width7Pool.getClass().getDeclaredField("arrays");
        arrays.setAccessible(true);
        final Object width7Arrays = arrays.get(width7Pool);
        final Object width11Arrays = arrays.get(width11Pool);

        epoch.setInt(pool, -1);
        reset.invoke(pool);

        Assert.assertSame("rollover must retain the width-7 bucket", width7Pool, widths.get(7));
        Assert.assertSame("rollover must retain the width-11 bucket", width11Pool, widths.get(11));
        Assert.assertSame("rollover must retain the width-7 array list", width7Arrays, arrays.get(widths.get(7)));
        Assert.assertSame("rollover must retain the width-11 array list", width11Arrays, arrays.get(widths.get(11)));
        Assert.assertSame(width7a, next.invoke(pool, 7));
        Assert.assertSame(width7b, next.invoke(pool, 7));
        Assert.assertSame(width11, next.invoke(pool, 11));
    }

    @Test
    public void testSortIsReusedUntilArenaChanges() {
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
            arena.put(intKey(2), NO_BYTES);
            arena.put(intKey(1), NO_BYTES);
            Assert.assertEquals(2, arena.sortAndValidateForTest());

            Assert.assertEquals("an unchanged arena must retain its validated order", 0, arena.sortAndValidateForTest());

            arena.put(intKey(3), NO_BYTES);
            Assert.assertEquals("appending must invalidate the retained order", 3, arena.sortAndValidateForTest());
            Assert.assertEquals(1, arena.getSortedMutationIndex(0));
            Assert.assertEquals(0, arena.getSortedMutationIndex(1));
            Assert.assertEquals(2, arena.getSortedMutationIndex(2));

            arena.put(intKey(3), NO_BYTES);
            try {
                arena.sortAndValidateForTest();
                Assert.fail("expected duplicate rejection after append");
            } catch (CairoException e) {
                Assert.assertTrue(e.getFlyweightMessage().toString().contains("duplicate"));
            }
        }
    }

    private static void assertSorted(int count, boolean reverse) {
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
            final byte[] key = new byte[Integer.BYTES];
            for (int i = 0; i < count; i++) {
                final int value = reverse ? count - i - 1 : i;
                putIntKey(key, value);
                arena.put(key, NO_BYTES);
            }
            arena.sortAndValidateForTest();
            Assert.assertEquals(count, arena.getMutationCount());
            for (int i = 1; i < count; i++) {
                Assert.assertTrue(arena.compareSortedKeysForTest(i - 1, i) < 0);
            }
            if (count > 0) {
                Assert.assertEquals(reverse ? count - 1 : 0, arena.getSortedMutationIndex(0));
                Assert.assertEquals(reverse ? 0 : count - 1, arena.getSortedMutationIndex(count - 1));
            }
            arena.clear();
            arena.put(intKey(7), NO_BYTES);
            arena.sortAndValidateForTest();
            Assert.assertEquals(1, arena.getMutationCount());
        }
    }

    private static byte[] intKey(int value) {
        final byte[] key = new byte[Integer.BYTES];
        putIntKey(key, value);
        return key;
    }

    private static void putIntKey(byte[] key, int value) {
        key[0] = (byte) (value >>> 24);
        key[1] = (byte) (value >>> 16);
        key[2] = (byte) (value >>> 8);
        key[3] = (byte) value;
    }


    @Test
    public void testTwoSortedRunsMergeWithoutHeapsort() {
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
            // Removals first, then puts, each run in key order and interleaved across runs -
            // the shape a seal after a frontier sweep hands over.
            for (int i = 0; i < 1000; i++) {
                arena.remove(intKey(3 * i + 1));
            }
            for (int i = 0; i < 1000; i++) {
                arena.put(intKey(3 * i), NO_BYTES);
                arena.put(intKey(3 * i + 2), NO_BYTES);
            }
            arena.sortAndValidateForTest();
            for (int i = 1; i < arena.getMutationCount(); i++) {
                Assert.assertTrue(arena.compareSortedKeysForTest(i - 1, i) < 0);
            }
            // Every mutation is present exactly once.
            final boolean[] seen = new boolean[3000];
            for (int i = 0; i < arena.getMutationCount(); i++) {
                seen[arena.getSortedMutationIndex(i)] = true;
            }
            for (boolean s : seen) {
                Assert.assertTrue(s);
            }
        }
    }

    @Test
    public void testDuplicateAcrossTwoRunsRejected() {
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
            arena.remove(intKey(5));
            arena.remove(intKey(9));
            arena.put(intKey(1), NO_BYTES);
            arena.put(intKey(9), NO_BYTES);
            try {
                arena.sortAndValidateForTest();
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(e.getFlyweightMessage().toString().contains("duplicate"));
            }
        }
    }

    @Test
    public void testThreeRunsFallBackToFullSort() {
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
            for (int run = 0; run < 3; run++) {
                for (int i = 0; i < 100; i++) {
                    arena.put(intKey(3 * i + run), NO_BYTES);
                }
            }
            arena.sortAndValidateForTest();
            for (int i = 1; i < arena.getMutationCount(); i++) {
                Assert.assertTrue(arena.compareSortedKeysForTest(i - 1, i) < 0);
            }
        }
    }
}
