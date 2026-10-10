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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;

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

    @Test
    public void testSortedTailMergeEdgeCases() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final byte[] empty = {};
            final byte[] a = {1};
            final byte[] a0 = {1, 0};
            final byte[] a00 = {1, 0, 0};
            final byte[] a1 = {1, 1};
            final byte[] b = {2};
            final byte[] c = {3};
            final byte[] high = {(byte) 0x80};
            // Keys that agree on their first eight bytes, which the comparator reads as one
            // word, and differ only past it or only in length.
            final byte[] wide = {9, 9, 9, 9, 9, 9, 9, 9};
            final byte[] wide0 = {9, 9, 9, 9, 9, 9, 9, 9, 0};
            final byte[] wide00 = {9, 9, 9, 9, 9, 9, 9, 9, 0, 0};
            final byte[] wideHigh = {9, 9, 9, 9, 9, 9, 9, 9, (byte) 0xff};

            // Nothing staged at all, and nothing staged since the last sort.
            Assert.assertEquals(SortPath.NOTHING_TO_SORT, assertMatchesFromScratchSort(keys()));
            Assert.assertEquals(SortPath.NOTHING_TO_SORT, assertMatchesFromScratchSort(keys(b, a), keys()));

            // No retained order to merge into, however the keys arrive.
            Assert.assertEquals(SortPath.FROM_SCRATCH, assertMatchesFromScratchSort(keys(a, b)));
            Assert.assertEquals(SortPath.FROM_SCRATCH, assertMatchesFromScratchSort(keys(), keys(a, b)));
            Assert.assertEquals(SortPath.FROM_SCRATCH, assertMatchesFromScratchSort(keys(), keys(b, a)));
            Assert.assertEquals(SortPath.FROM_SCRATCH, assertMatchesFromScratchSort(keys(), keys(a, a)));

            // An ascending tail wholly above, wholly below and interleaved with the retained keys.
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(b, a), keys(c, high)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(high, c), keys(a, b)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(c, a), keys(b, high)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(high, b), keys(a, c)));

            // A one-key tail at the front, in the middle and at the back.
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(b), keys(a)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(a), keys(b)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(high, c, b), keys(a)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(high, c, a), keys(b)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(c, b, a), keys(high)));

            // Keys that are prefixes of one another, the empty key among them.
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(a00, a), keys(empty, a0, a1)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(a1, empty, a0), keys(a, a00)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(wide0, wideHigh), keys(wide, wide00)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(wide00, wide), keys(wide0, wideHigh)));

            // Two merges in a row, and a merge on top of an order a rebuild produced.
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(c), keys(a, high), keys(empty, b)));
            Assert.assertEquals(SortPath.MERGED, assertMatchesFromScratchSort(keys(c), keys(high, a), keys(empty, b)));

            // A tail out of order, at its first pair and at its last.
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(c), keys(b, a)));
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(a), keys(b, high, c)));
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(wide), keys(wide00, wide0)));

            // An ascending tail that repeats the first, a middle and the last retained key.
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(c, a), keys(a, b)));
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(c, b, a), keys(b)));
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(c, a), keys(b, c)));
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(empty), keys(empty)));

            // A tail that repeats a key of its own: side by side, apart, and beside a retained one.
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(a), keys(b, b)));
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(a), keys(b, c, b)));
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(b), keys(a, b, b)));
            Assert.assertEquals(SortPath.REBUILT, assertMatchesFromScratchSort(keys(wide), keys(wide0, wide0)));
        });
    }

    @Test
    public void testSortedTailMergeFuzz() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Sorts a batch, stages a tail and sorts again, against a second arena that stages
            // the same mutations and sorts once. Whatever the tail holds, both must end with
            // the same order or the same duplicate report.
            final Rnd rnd = TestUtils.generateRandom(null);
            final int iterations = 600;
            int largestMergedKeyCount = 0;
            int mergedCount = 0;
            int rebuiltCount = 0;
            for (int iteration = 0; iteration < iterations; iteration++) {
                // One iteration in 59 is large. That period shares no factor with the six tail
                // shapes below, so the large iterations take every one of them in turn.
                final int keyCount = iteration % 59 == 58 ? 1_000 + rnd.nextInt(3_000) : 2 + rnd.nextInt(150);
                final byte[][] all = randomDistinctKeys(rnd, keyCount);
                final int batchCount = 1 + rnd.nextInt(keyCount - 1);
                final byte[][] batch = Arrays.copyOfRange(all, 0, batchCount);
                final byte[][] rest = Arrays.copyOfRange(all, batchCount, keyCount);
                final byte[][] ascending = rest.clone();
                Arrays.sort(ascending, LiveViewCheckpointMutationArenaTest::compareUnsigned);
                final SortPath path;
                switch (iteration % 6) {
                    case 0 -> path = assertMatchesFromScratchSort(batch, ascending);
                    case 1 -> {
                        // Two ascending tails, each merged into what the sort before it left.
                        final int middleCount = rnd.nextInt(rest.length);
                        final byte[][] middle = Arrays.copyOfRange(rest, 0, middleCount);
                        final byte[][] tail = Arrays.copyOfRange(rest, middleCount, rest.length);
                        Arrays.sort(middle, LiveViewCheckpointMutationArenaTest::compareUnsigned);
                        Arrays.sort(tail, LiveViewCheckpointMutationArenaTest::compareUnsigned);
                        path = assertMatchesFromScratchSort(batch, middle, tail);
                    }
                    // In the order the keys were drawn, which is ascending only by chance.
                    case 2 -> path = assertMatchesFromScratchSort(batch, rest);
                    case 3 -> {
                        // Ascending, with one retained key at its place in the order.
                        final byte[][] tail = Arrays.copyOf(ascending, ascending.length + 1);
                        tail[ascending.length] = batch[rnd.nextInt(batchCount)];
                        Arrays.sort(tail, LiveViewCheckpointMutationArenaTest::compareUnsigned);
                        path = assertMatchesFromScratchSort(batch, tail);
                    }
                    case 4 -> {
                        // Ascending, with one of its own keys twice: side by side, or at the end.
                        final byte[][] tail = Arrays.copyOf(ascending, ascending.length + 1);
                        tail[ascending.length] = ascending[rnd.nextInt(ascending.length)];
                        if (rnd.nextBoolean()) {
                            Arrays.sort(tail, LiveViewCheckpointMutationArenaTest::compareUnsigned);
                        }
                        path = assertMatchesFromScratchSort(batch, tail);
                    }
                    default -> {
                        final byte[][] descending = new byte[ascending.length][];
                        for (int i = 0; i < ascending.length; i++) {
                            descending[i] = ascending[ascending.length - 1 - i];
                        }
                        path = assertMatchesFromScratchSort(batch, descending);
                    }
                }
                if (path == SortPath.MERGED) {
                    mergedCount++;
                    largestMergedKeyCount = Math.max(largestMergedKeyCount, keyCount);
                } else if (path == SortPath.REBUILT) {
                    rebuiltCount++;
                }
            }
            // Two cases in six stage a tail the merge must take, and two a tail it must refuse.
            Assert.assertTrue("merge-eligible tails [count=" + mergedCount + ']', mergedCount >= iterations / 3);
            Assert.assertTrue("tails the merge must refuse [count=" + rebuiltCount + ']', rebuiltCount >= iterations / 3);
            Assert.assertTrue(
                    "a large iteration must complete a merge [largestMergedKeyCount=" + largestMergedKeyCount + ']',
                    largestMergedKeyCount >= 1_000
            );
        });
    }

    @Test
    public void testSortedTailMergeSurvivesOrdinalGrowthFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The ordinal list starts at 64 slots, so the 65th mutation is the first whose sort
            // has to grow it, and the retained order is what that growth must not disturb.
            final int retainedCount = 64;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE)) {
                try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena(tracker);
                     LiveViewCheckpointTestKeys key = new LiveViewCheckpointTestKeys()) {
                    // Even keys, descending, so the retained order is the reverse of the staging order.
                    for (int i = 0; i < retainedCount; i++) {
                        key.of(intKey(2 * (retainedCount - i)));
                        arena.put(key.address(), key.length(), 0, 0);
                    }
                    Assert.assertEquals(retainedCount, arena.sortAndValidateForTest());
                    // One key that sorts second.
                    key.of(intKey(3));
                    arena.remove(key.address(), key.length());

                    tracker.setLimit(tracker.getUsed());
                    try {
                        arena.sortAndValidateForTest();
                        Assert.fail("expected the tracker limit to stop the ordinal list from growing");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                    }
                    for (int i = 0; i < retainedCount; i++) {
                        Assert.assertEquals(
                                "a failed growth must leave the retained order as the last sort validated it",
                                retainedCount - 1 - i,
                                arena.getSortedMutationIndex(i)
                        );
                    }

                    tracker.setLimit(Long.MAX_VALUE);
                    arena.resetSortComparisonCountForTest();
                    Assert.assertEquals(retainedCount + 1, arena.sortAndValidateForTest());
                    final long comparisons = arena.getSortComparisonCountForTest();
                    // The arena counts comparisons only while JVM assertions are enabled, and
                    // the bound below would hold for a count that never advanced.
                    Assert.assertTrue("the comparison count needs JVM assertions enabled (-ea)", comparisons > 0);
                    Assert.assertTrue(
                            "the retry must merge the one new key into the retained order [comparisons=" + comparisons + ']',
                            comparisons <= retainedCount
                    );
                    Assert.assertEquals(retainedCount - 1, arena.getSortedMutationIndex(0));
                    Assert.assertEquals(retainedCount, arena.getSortedMutationIndex(1));
                    for (int i = 2; i <= retainedCount; i++) {
                        Assert.assertEquals(retainedCount - i, arena.getSortedMutationIndex(i));
                    }
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSortedTailMergesInLinearComparisons() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The shape a complete window-root build stages: a batch in arrival order, sorted so
            // the walk of the predecessor map can probe it, then one removal for every key the
            // batch lacks, in the ascending order the walk visits them. Even keys make the batch
            // and odd keys the removals, so the two interleave key by key.
            final int batchCount = 4_096;
            final int removalCount = 4_096;
            final int total = batchCount + removalCount;
            final Rnd rnd = new Rnd(42, 7);
            final int[] batchKeys = new int[batchCount];
            for (int i = 0; i < batchCount; i++) {
                batchKeys[i] = 2 * i;
            }
            for (int i = batchCount - 1; i > 0; i--) {
                final int j = rnd.nextInt(i + 1);
                final int swap = batchKeys[i];
                batchKeys[i] = batchKeys[j];
                batchKeys[j] = swap;
            }
            try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointTestKeys key = new LiveViewCheckpointTestKeys()) {
                for (int i = 0; i < batchCount; i++) {
                    key.of(intKey(batchKeys[i]));
                    if ((i & 1) == 0) {
                        arena.put(key.address(), key.length(), 0, 0);
                    } else {
                        arena.domain(key.address(), key.length());
                    }
                }
                Assert.assertEquals(batchCount, arena.sortAndValidateForTest());
                for (int i = 0; i < removalCount; i++) {
                    key.of(intKey(2 * i + 1));
                    arena.remove(key.address(), key.length());
                }

                arena.resetSortComparisonCountForTest();
                Assert.assertEquals(total, arena.sortAndValidateForTest());
                final long comparisons = arena.getSortComparisonCountForTest();
                // The arena counts comparisons only while JVM assertions are enabled, and the
                // bound below would hold for a count that never advanced.
                Assert.assertTrue("the comparison count needs JVM assertions enabled (-ea)", comparisons > 0);
                // One pass proves the removals ascend and one merges them into the batch. A
                // sort of all the keys from scratch makes over ten times as many comparisons.
                final long limit = batchCount + 2L * removalCount;
                Assert.assertTrue(
                        "sorted removals must merge into the sorted batch in one pass [comparisons=" + comparisons
                                + ", limit=" + limit + ']',
                        comparisons <= limit
                );
                for (int i = 0; i < total; i++) {
                    final int mutationIndex = arena.getSortedMutationIndex(i);
                    Assert.assertEquals("a removal must sort at an odd position [at=" + i + ']', (i & 1) == 1, mutationIndex >= batchCount);
                    Assert.assertArrayEquals(intKey(i), stagedKey(arena, mutationIndex));
                }
            }
        });
    }

    @Test
    public void testStatePageRefCountBoundaryIsValidatedBeforeAppend() throws Exception {
        // A partition names at most 65,536 state page references, the format's limit. The
        // arena stages a put at the limit, and rejects one more reference before it copies
        // anything.
        TestUtils.assertMemoryLeak(() -> {
            final int maxRefs = 65_536;
            final LiveViewCheckpointStatePageRef[] refs = new LiveViewCheckpointStatePageRef[maxRefs + 1];
            Arrays.fill(refs, new LiveViewCheckpointStatePageRef().of(7, 64, 8, 8, 0x31, 0, 1, 0));
            try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
                LiveViewCheckpointTestKeys.put(arena, intKey(1), NO_BYTES, Arrays.copyOf(refs, maxRefs));
                Assert.assertEquals(1, arena.sortAndValidateForTest());

                arena.clear();
                try {
                    LiveViewCheckpointTestKeys.put(arena, intKey(1), NO_BYTES, refs);
                    Assert.fail("expected a reference count past the format limit to be rejected");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCritical());
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "too many live view checkpoint partition state page references"
                    );
                }
                Assert.assertEquals("validation must run before native append", 0, arena.getMutationCount());

                // The rejection leaves the arena usable.
                LiveViewCheckpointTestKeys.put(arena, intKey(2), NO_BYTES, Arrays.copyOf(refs, 1));
                Assert.assertEquals(1, arena.sortAndValidateForTest());
            }
        });
    }

    /**
     * Stages {@code rounds} into one arena, sorting it after every round, and into a second
     * arena that sorts once after the last round: the from-scratch reference. Asserts that
     * the last incremental sort and the reference end the same way - with the same order,
     * which is the ascending unsigned one, or with the same duplicate report - and charge
     * their trackers the same.
     * <p>
     * Every round but the last holds distinct keys, so only the last sort can fail. The
     * path that sort took shows in its key comparisons, set against the reference's: none
     * when the last round staged nothing, the same when it had no retained order to build
     * on, fewer when it merged its tail into the retained order, and more when it tried the
     * merge, gave it up and sorted from scratch.
     * <p>
     * Fails when that path is not the one the last round calls for, and when a merge makes
     * more comparisons than one pass over the tail and one over the retained order take.
     * The failure names the rounds, unless they hold more keys than a message should carry.
     *
     * @return the path the last sort took, as its comparison count shows it
     */
    private static SortPath assertMatchesFromScratchSort(byte[][]... rounds) {
        final byte[][] tail = rounds[rounds.length - 1];
        int total = 0;
        for (byte[][] round : rounds) {
            total += round.length;
        }
        final int retainedCount = total - tail.length;
        final byte[][] staged = new byte[total][];
        final HashSet<ByteBuffer> retainedKeys = new HashSet<>();
        int stagedCount = 0;
        for (int r = 0; r < rounds.length - 1; r++) {
            for (byte[] key : rounds[r]) {
                staged[stagedCount++] = key;
                Assert.assertTrue("only the last round may repeat a key", retainedKeys.add(ByteBuffer.wrap(key)));
            }
        }
        final HashSet<ByteBuffer> tailKeys = new HashSet<>();
        boolean hasDuplicate = false;
        boolean isTailMergeable = true;
        for (int i = 0; i < tail.length; i++) {
            staged[stagedCount++] = tail[i];
            final ByteBuffer key = ByteBuffer.wrap(tail[i]);
            if (retainedKeys.contains(key) || !tailKeys.add(key)) {
                hasDuplicate = true;
                isTailMergeable = false;
            }
            if (i > 0 && compareUnsigned(tail[i - 1], tail[i]) >= 0) {
                isTailMergeable = false;
            }
        }
        final SortPath expectedPath;
        if (tail.length == 0) {
            expectedPath = SortPath.NOTHING_TO_SORT;
        } else if (retainedCount == 0) {
            expectedPath = SortPath.FROM_SCRATCH;
        } else {
            expectedPath = isTailMergeable ? SortPath.MERGED : SortPath.REBUILT;
        }

        final SortPath path;
        try (LimitedMemoryTracker incrementalTracker = new LimitedMemoryTracker(Long.MAX_VALUE);
             LimitedMemoryTracker fromScratchTracker = new LimitedMemoryTracker(Long.MAX_VALUE)) {
            try (LiveViewCheckpointMutationArena incremental = new LiveViewCheckpointMutationArena(incrementalTracker);
                 LiveViewCheckpointMutationArena fromScratch = new LiveViewCheckpointMutationArena(fromScratchTracker);
                 LiveViewCheckpointTestKeys key = new LiveViewCheckpointTestKeys()) {
                int mutationIndex = 0;
                for (int r = 0; r < rounds.length; r++) {
                    for (byte[] roundKey : rounds[r]) {
                        key.of(roundKey);
                        // Every operation sorts by its key alone.
                        switch (mutationIndex++ % 3) {
                            case 0 -> {
                                incremental.put(key.address(), key.length(), 0, 0);
                                fromScratch.put(key.address(), key.length(), 0, 0);
                            }
                            case 1 -> {
                                incremental.domain(key.address(), key.length());
                                fromScratch.domain(key.address(), key.length());
                            }
                            default -> {
                                incremental.remove(key.address(), key.length());
                                fromScratch.remove(key.address(), key.length());
                            }
                        }
                    }
                    if (r < rounds.length - 1) {
                        Assert.assertEquals(
                                rounds[r].length == 0 ? 0 : mutationIndex,
                                incremental.sortAndValidateForTest()
                        );
                    }
                }

                incremental.resetSortComparisonCountForTest();
                final String incrementalOutcome = sortOutcome(incremental);
                final long incrementalComparisons = incremental.getSortComparisonCountForTest();
                fromScratch.resetSortComparisonCountForTest();
                final String fromScratchOutcome = sortOutcome(fromScratch);
                final long fromScratchComparisons = fromScratch.getSortComparisonCountForTest();

                if (hasDuplicate) {
                    TestUtils.assertContains(
                            fromScratchOutcome,
                            "error=duplicate live view checkpoint partition mutation key [left="
                    );
                    Assert.assertEquals(fromScratchOutcome, incrementalOutcome);
                    assertSameOrder(fromScratch, incremental, total);
                    // The rejection is not a one-off: the arena reports the same pair again.
                    Assert.assertEquals(fromScratchOutcome, sortOutcome(incremental));
                } else {
                    Assert.assertEquals("sorted=" + total, fromScratchOutcome);
                    Assert.assertEquals(tail.length == 0 ? "sorted=0" : fromScratchOutcome, incrementalOutcome);
                }
                assertSameOrder(fromScratch, incremental, total);
                if (!hasDuplicate) {
                    byte[] previous = null;
                    for (int i = 0; i < total; i++) {
                        final int sorted = incremental.getSortedMutationIndex(i);
                        Assert.assertArrayEquals(staged[sorted], stagedKey(incremental, sorted));
                        Assert.assertTrue(
                                "sorted keys must be strictly increasing unsigned, then by length [at=" + i + ']',
                                previous == null || compareUnsigned(previous, staged[sorted]) < 0
                        );
                        previous = staged[sorted];
                    }
                }
                Assert.assertEquals(
                        "both arenas must hold the same native capacity",
                        fromScratchTracker.getUsed(),
                        incrementalTracker.getUsed()
                );

                if (incrementalComparisons == 0 && "sorted=0".equals(incrementalOutcome)) {
                    path = SortPath.NOTHING_TO_SORT;
                } else if (incrementalComparisons < fromScratchComparisons) {
                    path = SortPath.MERGED;
                } else if (incrementalComparisons == fromScratchComparisons) {
                    path = SortPath.FROM_SCRATCH;
                } else {
                    path = SortPath.REBUILT;
                }
                // One pass proves the tail ascends, one merges it into the retained order.
                final long mergeLimit = retainedCount + 2L * tail.length - 2;
                if (path != expectedPath || (path == SortPath.MERGED && incrementalComparisons > mergeLimit)) {
                    Assert.fail("the last sort must take the path its tail calls for, and a merge must stay within its limit"
                            + " [expected=" + expectedPath
                            + ", took=" + path
                            + ", retained=" + retainedCount
                            + ", tail=" + tail.length
                            + ", comparisons=" + incrementalComparisons
                            + ", fromScratchComparisons=" + fromScratchComparisons
                            + ", mergeLimit=" + mergeLimit
                            // The fuzz stages thousands of keys, which its seed reproduces.
                            + ", rounds=" + (total <= 256 ? Arrays.deepToString(rounds) : "omitted")
                            + ']');
                }
            }
            Assert.assertEquals(0, incrementalTracker.getUsed());
            Assert.assertEquals(0, fromScratchTracker.getUsed());
        }
        return path;
    }

    private static void assertSameOrder(
            LiveViewCheckpointMutationArena expected,
            LiveViewCheckpointMutationArena actual,
            int count
    ) {
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(
                    "sorted mutation index [at=" + i + ']',
                    expected.getSortedMutationIndex(i),
                    actual.getSortedMutationIndex(i)
            );
        }
    }

    private static void assertSorted(int count, boolean isReverse) {
        try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena()) {
            final byte[] key = new byte[Integer.BYTES];
            for (int i = 0; i < count; i++) {
                final int value = isReverse ? count - i - 1 : i;
                putIntKey(key, value);
                LiveViewCheckpointTestKeys.put(arena, key, NO_BYTES);
            }
            arena.sortAndValidateForTest();
            Assert.assertEquals(count, arena.getMutationCount());
            for (int i = 1; i < count; i++) {
                Assert.assertTrue(arena.compareSortedKeysForTest(i - 1, i) < 0);
            }
            if (count > 0) {
                Assert.assertEquals(isReverse ? count - 1 : 0, arena.getSortedMutationIndex(0));
                Assert.assertEquals(isReverse ? 0 : count - 1, arena.getSortedMutationIndex(count - 1));
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

    private static byte[][] keys(byte[]... keys) {
        return keys;
    }

    /**
     * @return {@code count} distinct keys in random order, drawn from a five-byte alphabet
     * that spans the sign bit. Three in four are at most five bytes long, so many are
     * prefixes of others; the rest share one of two eight-byte heads, so they differ only
     * past the word the comparator reads first, or only in length.
     */
    private static byte[][] randomDistinctKeys(Rnd rnd, int count) {
        final byte[] alphabet = {0, 1, 0x7f, (byte) 0x80, (byte) 0xff};
        final HashSet<ByteBuffer> seen = new HashSet<>();
        final byte[][] keys = new byte[count][];
        int n = 0;
        while (n < count) {
            final byte[] key;
            if (rnd.nextInt(4) == 0) {
                key = new byte[Long.BYTES + rnd.nextInt(12)];
                Arrays.fill(key, 0, Long.BYTES, rnd.nextBoolean() ? (byte) 0x80 : 1);
                for (int b = Long.BYTES; b < key.length; b++) {
                    key[b] = alphabet[rnd.nextInt(alphabet.length)];
                }
            } else {
                key = new byte[rnd.nextInt(6)];
                for (int b = 0; b < key.length; b++) {
                    key[b] = alphabet[rnd.nextInt(alphabet.length)];
                }
            }
            if (seen.add(ByteBuffer.wrap(key))) {
                keys[n++] = key;
            }
        }
        return keys;
    }

    private static String sortOutcome(LiveViewCheckpointMutationArena arena) {
        try {
            return "sorted=" + arena.sortAndValidateForTest();
        } catch (CairoException e) {
            return "error=" + e.getFlyweightMessage();
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

    /**
     * What a sort that follows an earlier sort of the same arena does with the mutations
     * staged in between.
     */
    private enum SortPath {
        // The caller staged nothing since the last sort, so the retained order stands.
        NOTHING_TO_SORT,
        // The arena retained no order, so the sort starts from scratch.
        FROM_SCRATCH,
        // The tail arrived in ascending order and repeats no key, so one pass merges it in.
        MERGED,
        // The tail is out of order or repeats a key, so the merge gives way to a sort from scratch.
        REBUILT
    }
}
