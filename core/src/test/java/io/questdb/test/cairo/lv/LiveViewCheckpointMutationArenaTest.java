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
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

public class LiveViewCheckpointMutationArenaTest {
    // The widest key or scalar one mutation may carry.
    private static final int MAX_FIELD_BYTES = 1 << 20;
    private static final byte[] NO_BYTES = new byte[0];
    private static final LiveViewCheckpointStatePageRef[] NO_REFS = new LiveViewCheckpointStatePageRef[0];

    @Test
    public void testArenaGrowsPastTwoGiBUntilTheTrackerLimit() throws Exception {
        // The ceiling this guards was a fixed page count, which no platform changes. Crossing
        // it holds 2 GiB of resident native memory, and past 2^31 the tracker limit makes the
        // arena grow in exact 4 KiB steps: glibc remaps the block for each step, while other
        // allocators may copy all 2 GiB per step. Run on Linux only.
        Assume.assumeTrue(Os.isLinux());
        TestUtils.assertMemoryLeak(() -> {
            // The fillers end exactly at offset 2^31, where 524_288 pages of 4 KiB used to stop
            // the arena. The limit sits just above that, as a view's refresh limit would.
            final int fillerCount = (int) ((1L << 31) / MAX_FIELD_BYTES);
            final long limit = (1L << 31) + (32L << 20);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(limit)) {
                try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena(tracker)) {
                    final byte[] key = new byte[MAX_FIELD_BYTES];
                    // Descending prefixes, so the sort has to move every entry.
                    for (int i = 0; i < fillerCount; i++) {
                        putIntKey(key, fillerCount - i);
                        LiveViewCheckpointTestKeys.put(arena, key, NO_BYTES);
                    }
                    // Two probes wholly above 2^31 that differ only in their last byte and sort
                    // ahead of every filler, so ordering them reads each probe to its end.
                    putIntKey(key, 0);
                    key[MAX_FIELD_BYTES - 1] = 2;
                    LiveViewCheckpointTestKeys.put(arena, key, NO_BYTES);
                    key[MAX_FIELD_BYTES - 1] = 1;
                    LiveViewCheckpointTestKeys.put(arena, key, NO_BYTES);
                    Assert.assertTrue("the tracker must carry the staged bytes", tracker.getUsed() > 1L << 31);

                    final int count = fillerCount + 2;
                    Assert.assertEquals(count, arena.sortAndValidateForTest());
                    Assert.assertEquals(fillerCount + 1, arena.getSortedMutationIndex(0));
                    Assert.assertEquals(fillerCount, arena.getSortedMutationIndex(1));
                    for (int i = 2; i < count; i++) {
                        Assert.assertEquals(fillerCount + 1 - i, arena.getSortedMutationIndex(i));
                    }
                    Assert.assertTrue(arena.compareSortedKeysForTest(0, 1) < 0);

                    // The configured limit, not a page count, is what stops the arena, and it
                    // stops it with the tracker's own breach.
                    key[MAX_FIELD_BYTES - 1] = 0;
                    boolean isBreached = false;
                    for (int i = 1; i <= 64 && !isBreached; i++) {
                        putIntKey(key, fillerCount + i);
                        try {
                            LiveViewCheckpointTestKeys.put(arena, key, NO_BYTES);
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                            isBreached = true;
                        }
                    }
                    Assert.assertTrue("expected the tracker limit to stop the arena", isBreached);
                    Assert.assertTrue(tracker.getUsed() <= limit);

                    arena.clear();
                    LiveViewCheckpointTestKeys.put(arena, intKey(1), NO_BYTES);
                    Assert.assertEquals(1, arena.sortAndValidateForTest());
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testArenaGrowthFailureReleasesTrackerAndCanBeReused() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1)) {
                final LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena(tracker);
                try {
                    try {
                        LiveViewCheckpointTestKeys.put(arena, intKey(1), NO_BYTES);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }
                    tracker.setLimit(Long.MAX_VALUE);
                    arena.clear();
                    LiveViewCheckpointTestKeys.put(arena, intKey(1), NO_BYTES);
                    arena.sortAndValidateForTest();
                    Assert.assertTrue(tracker.getUsed() > 0);
                } finally {
                    arena.close();
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testDuplicateRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
                LiveViewCheckpointTestKeys.put(arena, intKey(1), NO_BYTES);
                LiveViewCheckpointTestKeys.put(arena, intKey(1), NO_BYTES);
                try {
                    arena.sortAndValidateForTest();
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString().contains("duplicate"));
                }
            }
        });
    }

    @Test
    public void testEmptySingleSortedReverseAndHighCardinality() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertSorted(0, false);
            assertSorted(1, false);
            assertSorted(1_000, false);
            assertSorted(1_000, true);
            assertSorted(1_000_000, true);
        });
    }

    @Test
    public void testKeyLengthBoundaryIsValidatedBeforeAppend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final byte[] anchorState = new byte[Long.BYTES];
            try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
                LiveViewCheckpointTestKeys.put(arena, new byte[1 << 20], anchorState);
                arena.sortAndValidateForTest();
                Assert.assertEquals(1, arena.getMutationCount());

                arena.clear();
                try {
                    LiveViewCheckpointTestKeys.put(arena, new byte[(1 << 20) + 1], anchorState);
                    Assert.fail("expected oversized key rejection");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString().contains("partition key length out of bounds"));
                }
                Assert.assertEquals("validation must run before native append", 0, arena.getMutationCount());
            }
        });
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
    public void testKeysStageTheSameBytesWhereverTheyAreCopiedFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Keys of 0 to 40 bytes, many sharing a prefix and many carrying high-bit bytes, staged
            // once each from a copy of its own and once as a slice of one block that packs them
            // back to back, through every operation. Both arenas must stage exactly each key's
            // bytes and sort them the same way, and that order must be the unsigned
            // byte-by-byte-then-length order a persisted map is sorted in.
            final int keyCount = 2_000;
            final Rnd rnd = new Rnd(42, 7);
            final byte[][] keys = new byte[keyCount][];
            final long[] offsets = new long[keyCount];
            long totalBytes = 0;
            for (int i = 0; i < keyCount; i++) {
                final int length = rnd.nextInt(41);
                final byte[] key = new byte[length];
                for (int b = 0; b < length; b++) {
                    key[b] = (byte) (rnd.nextBoolean() ? 0x80 | rnd.nextInt(128) : rnd.nextInt(3));
                }
                if (length >= Integer.BYTES) {
                    // A distinct tail keeps every key unique however short its alphabet.
                    putIntKey(key, i);
                    reverse(key);
                }
                keys[i] = key;
                offsets[i] = totalBytes;
                totalBytes += length;
            }
            // Short keys can collide; keep the first of each.
            final boolean[] isDuplicate = new boolean[keyCount];
            for (int i = 0; i < keyCount; i++) {
                for (int j = 0; j < i && !isDuplicate[i]; j++) {
                    if (!isDuplicate[j] && Arrays.equals(keys[i], keys[j])) {
                        isDuplicate[i] = true;
                    }
                }
            }
            final long source = Unsafe.malloc(Math.max(1, totalBytes), MemoryTag.NATIVE_DEFAULT);
            // The scalar the native puts stage, rewritten before each.
            final long scalar = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            try (LiveViewCheckpointMutationArena copiedArena = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointMutationArena nativeArena = new LiveViewCheckpointMutationArena()) {
                for (int i = 0; i < keyCount; i++) {
                    for (int b = 0; b < keys[i].length; b++) {
                        Unsafe.putByte(source + offsets[i] + b, keys[i][b]);
                    }
                }
                // The key each mutation index stages, in staging order.
                final int[] stagedKeyIndexes = new int[keyCount];
                int staged = 0;
                for (int i = 0; i < keyCount; i++) {
                    if (isDuplicate[i]) {
                        continue;
                    }
                    final long address = source + offsets[i];
                    final int length = keys[i].length;
                    switch (i % 4) {
                        case 0 -> {
                            LiveViewCheckpointTestKeys.put(copiedArena, keys[i], intKey(i));
                            copyIn(intKey(i), scalar);
                            nativeArena.put(address, length, scalar, Integer.BYTES);
                        }
                        case 1 -> {
                            LiveViewCheckpointTestKeys.put(copiedArena, keys[i], intKey(i), NO_REFS);
                            copyIn(intKey(i), scalar);
                            nativeArena.put(address, length, scalar, Integer.BYTES, NO_REFS);
                        }
                        case 2 -> {
                            LiveViewCheckpointTestKeys.remove(copiedArena, keys[i]);
                            nativeArena.remove(address, length);
                        }
                        default -> {
                            LiveViewCheckpointTestKeys.domain(copiedArena, keys[i]);
                            nativeArena.domain(address, length);
                        }
                    }
                    stagedKeyIndexes[staged++] = i;
                }
                Assert.assertEquals(staged, copiedArena.sortAndValidateForTest());
                Assert.assertEquals(staged, nativeArena.sortAndValidateForTest());
                int previous = -1;
                for (int s = 0; s < staged; s++) {
                    final int mutationIndex = nativeArena.getSortedMutationIndex(s);
                    Assert.assertEquals(copiedArena.getSortedMutationIndex(s), mutationIndex);
                    final int length = nativeArena.getKeyLengthForTest(mutationIndex);
                    Assert.assertEquals(copiedArena.getKeyLengthForTest(mutationIndex), length);
                    final byte[] nativeStaged = stagedKey(nativeArena, mutationIndex);
                    Assert.assertArrayEquals(keys[stagedKeyIndexes[mutationIndex]], nativeStaged);
                    Assert.assertArrayEquals(stagedKey(copiedArena, mutationIndex), nativeStaged);
                    Assert.assertArrayEquals(stagedScalar(copiedArena, mutationIndex), stagedScalar(nativeArena, mutationIndex));
                    if (previous > -1) {
                        Assert.assertTrue(
                                "sorted keys must be strictly increasing unsigned, then by length [at=" + s + ']',
                                compareUnsigned(stagedKey(nativeArena, previous), nativeStaged) < 0
                        );
                    }
                    previous = mutationIndex;
                }
            } finally {
                Unsafe.free(scalar, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(source, Math.max(1, totalBytes), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNativeKeyIsValidatedAndMustNotAliasItsArena() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long length = (1 << 20) + 1;
            final long source = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
            try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
                Vect.memset(source, length, 1);
                arena.put(source, 1 << 20, 0, 0);
                Assert.assertEquals(1, arena.getMutationCount());
                try {
                    arena.put(source, (1 << 20) + 1, 0, 0);
                    Assert.fail("expected oversized key rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "partition key length out of bounds");
                }
                Assert.assertEquals("validation must run before native append", 1, arena.getMutationCount());

                // A key the arena already stages cannot be the source of another: the copy may
                // grow, and so move, the very memory it reads.
                final long staged = arena.getKeyAddressForTest(0);
                try {
                    arena.remove(staged, 16);
                    Assert.fail("expected the self-alias guard to reject the key");
                } catch (AssertionError e) {
                    TestUtils.assertContains(e.getMessage(), "aliases its own arena");
                }
                Assert.assertEquals("the rejected key must not be staged", 1, arena.getMutationCount());
            } finally {
                Unsafe.free(source, length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNativeScalarIsValidatedAndMustNotAliasItsArena() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long length = MAX_FIELD_BYTES + 1;
            final long source = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
            try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
                Vect.memset(source, length, 3);
                final byte[] key = intKey(1);
                LiveViewCheckpointTestKeys.put(arena, key, NO_BYTES);
                arena.put(source, Integer.BYTES, source, MAX_FIELD_BYTES);
                Assert.assertEquals(2, arena.getMutationCount());
                Assert.assertEquals(MAX_FIELD_BYTES, arena.getScalarLengthForTest(1));
                try {
                    arena.put(source + 1, Integer.BYTES, source, MAX_FIELD_BYTES + 1, NO_REFS);
                    Assert.fail("expected oversized scalar rejection");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "partition scalar state length out of bounds");
                }
                Assert.assertEquals("validation must run before native append", 2, arena.getMutationCount());

                // A scalar the arena already stages cannot be the source of another put: the key
                // copy that goes first may grow, and so move, the very memory the scalar names.
                final long staged = arena.getScalarAddressForTest(1);
                final AssertionError e = Assert.assertThrows(
                        AssertionError.class,
                        () -> arena.put(source + 2, Integer.BYTES, staged, 16)
                );
                TestUtils.assertContains(e.getMessage(), "scalar aliases its own arena");
                Assert.assertEquals("the rejected scalar must not be staged", 2, arena.getMutationCount());
            } finally {
                Unsafe.free(source, length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNativeScalarsStageTheSameBytesAsHeapScalars() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Scalars of 0 to 300 bytes, staged once from a heap array and once from native
            // memory, through both put shapes. The two arenas must stage exactly the same key,
            // scalar and reference bytes for every mutation, which is what the partition-map
            // writer copies into a page.
            final int mutationCount = 1_000;
            final Rnd rnd = new Rnd(42, 7);
            final int maxWidth = 300;
            final long source = Unsafe.malloc(maxWidth, MemoryTag.NATIVE_DEFAULT);
            final LiveViewCheckpointStatePageRef[] refs = {
                    new LiveViewCheckpointStatePageRef().of(7, 64, 8, 8, 0x31, 0, 1, 0)
            };
            try (LiveViewCheckpointMutationArena heapArena = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointMutationArena nativeArena = new LiveViewCheckpointMutationArena()) {
                final byte[][] scalars = new byte[mutationCount][];
                for (int i = 0; i < mutationCount; i++) {
                    final byte[] scalar = new byte[rnd.nextInt(maxWidth + 1)];
                    for (int b = 0; b < scalar.length; b++) {
                        scalar[b] = (byte) rnd.nextInt();
                    }
                    scalars[i] = scalar;
                    for (int b = 0; b < scalar.length; b++) {
                        Unsafe.putByte(source + b, scalar[b]);
                    }
                    final byte[] key = intKey(i);
                    if ((i & 1) == 0) {
                        LiveViewCheckpointTestKeys.put(heapArena, key, scalar);
                        LiveViewCheckpointTestKeys.put(nativeArena, key, source, scalar.length);
                    } else {
                        LiveViewCheckpointTestKeys.put(heapArena, key, scalar, refs);
                        LiveViewCheckpointTestKeys.put(nativeArena, key, source, scalar.length, refs);
                    }
                }
                Assert.assertEquals(mutationCount, heapArena.getMutationCount());
                Assert.assertEquals(mutationCount, nativeArena.getMutationCount());
                Assert.assertEquals(mutationCount, heapArena.sortAndValidateForTest());
                Assert.assertEquals(mutationCount, nativeArena.sortAndValidateForTest());
                for (int i = 0; i < mutationCount; i++) {
                    Assert.assertEquals(heapArena.getSortedMutationIndex(i), nativeArena.getSortedMutationIndex(i));
                    Assert.assertArrayEquals(stagedKey(heapArena, i), stagedKey(nativeArena, i));
                    Assert.assertArrayEquals("scalar [mutation=" + i + ']', scalars[i], stagedScalar(nativeArena, i));
                    Assert.assertArrayEquals(stagedScalar(heapArena, i), stagedScalar(nativeArena, i));
                }
            } finally {
                Unsafe.free(source, maxWidth, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSortIsReusedUntilArenaChanges() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
                LiveViewCheckpointTestKeys.put(arena, intKey(2), NO_BYTES);
                LiveViewCheckpointTestKeys.put(arena, intKey(1), NO_BYTES);
                Assert.assertEquals(2, arena.sortAndValidateForTest());

                Assert.assertEquals("an unchanged arena must retain its validated order", 0, arena.sortAndValidateForTest());

                LiveViewCheckpointTestKeys.put(arena, intKey(3), NO_BYTES);
                Assert.assertEquals("appending must invalidate the retained order", 3, arena.sortAndValidateForTest());
                Assert.assertEquals(1, arena.getSortedMutationIndex(0));
                Assert.assertEquals(0, arena.getSortedMutationIndex(1));
                Assert.assertEquals(2, arena.getSortedMutationIndex(2));

                LiveViewCheckpointTestKeys.put(arena, intKey(3), NO_BYTES);
                try {
                    arena.sortAndValidateForTest();
                    Assert.fail("expected duplicate rejection after append");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString().contains("duplicate"));
                }
            }
        });
    }

    private static void assertSorted(int count, boolean reverse) {
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
            final byte[] key = new byte[Integer.BYTES];
            for (int i = 0; i < count; i++) {
                final int value = reverse ? count - i - 1 : i;
                putIntKey(key, value);
                LiveViewCheckpointTestKeys.put(arena, key, NO_BYTES);
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
            LiveViewCheckpointTestKeys.put(arena, intKey(7), NO_BYTES);
            arena.sortAndValidateForTest();
            Assert.assertEquals(1, arena.getMutationCount());
        }
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        final int n = Math.min(left.length, right.length);
        for (int i = 0; i < n; i++) {
            final int cmp = Integer.compare(left[i] & 0xff, right[i] & 0xff);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static void copyIn(byte[] bytes, long address) {
        for (int b = 0; b < bytes.length; b++) {
            Unsafe.putByte(address + b, bytes[b]);
        }
    }

    private static byte[] stagedKey(LiveViewCheckpointMutationArena arena, int mutationIndex) {
        final byte[] key = new byte[arena.getKeyLengthForTest(mutationIndex)];
        for (int b = 0; b < key.length; b++) {
            key[b] = Unsafe.getByte(arena.getKeyAddressForTest(mutationIndex) + b);
        }
        return key;
    }

    private static byte[] stagedScalar(LiveViewCheckpointMutationArena arena, int mutationIndex) {
        final byte[] scalar = new byte[arena.getScalarLengthForTest(mutationIndex)];
        for (int b = 0; b < scalar.length; b++) {
            scalar[b] = Unsafe.getByte(arena.getScalarAddressForTest(mutationIndex) + b);
        }
        return scalar;
    }

    private static byte[] intKey(int value) {
        final byte[] key = new byte[Integer.BYTES];
        putIntKey(key, value);
        return key;
    }

    private static void reverse(byte[] key) {
        for (int i = 0, j = key.length - 1; i < j; i++, j--) {
            final byte swap = key[i];
            key[i] = key[j];
            key[j] = swap;
        }
    }

    private static void putIntKey(byte[] key, int value) {
        key[0] = (byte) (value >>> 24);
        key[1] = (byte) (value >>> 16);
        key[2] = (byte) (value >>> 8);
        key[3] = (byte) value;
    }
}
