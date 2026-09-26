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
import io.questdb.cairo.lv.LiveViewCheckpointBinaryKeyIndex;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The native partition index a freeze scratch and a chained repair capture keep over their
 * frozen keys must answer exactly as the heap index it replaced for partition keys: the same
 * value for the same key under the same two qualifiers, nothing for another qualifier, a
 * shorter key or a key never put, and an overwrite on a repeated put - across every rehash.
 * It must also hold what it promises natively: a reserved table does not regrow, every byte
 * comes back on release and close, and a growth the process cannot allocate leaves every entry
 * it already holds. And it must stay dense without slowing its lookups: at most three slots a
 * key at any size, grown or reserved, and a few probes a lookup at its fullest, even over keys
 * that differ only in their leading zero bytes.
 */
public class LiveViewCheckpointKeyIndexTest {
    private static final Method ARENA_APPEND;
    private static final Method ARENA_CLOSE;
    private static final Constructor<?> ARENA_CONSTRUCTOR;
    private static final Method INDEX_CLEAR;
    private static final Method INDEX_CLOSE;
    private static final Constructor<?> INDEX_CONSTRUCTOR;
    private static final Method INDEX_GET;
    private static final Method INDEX_HIT_PROBES;
    private static final Method INDEX_MISS_PROBES;
    private static final Method INDEX_PUT;
    private static final Method INDEX_RELEASE;
    private static final Method INDEX_RESERVE;
    private static final Method INDEX_SIZE;
    private static final int KEY_COUNT = 50_000;
    private static final int KEY_LENGTH = 6;
    private static final int KEY_SHAPE_COUNT = 5;
    // Three of the index's 24-byte slots: a table that keeps at least about a third of its
    // slots full. The bound applies from MIN_MEASURED_KEYS on, past the smallest table.
    private static final long MAX_BYTES_PER_KEY = 72;
    // Linear probing at a load of 0.7 averages 2.17 slots a hit and 6.06 a miss when the
    // hash spreads the keys evenly, and at 0.72 it averages 2.29 and 6.88. Pooled over the
    // 70 tables testAFullTableFindsOrMissesAKeyInAFewProbes builds, 1,000 randomly seeded
    // variants of the index's hash averaged at most 2.19 and 6.16, so the pooled bounds hold
    // the index to the cost of an even spread at its load. One table strays much further:
    // the worst of the 70 reached 3.12 and 12.06 under those variants. The per-table bounds
    // are therefore loose, and catch a hash that piles one key shape into long runs of
    // occupied slots.
    private static final double MAX_HIT_PROBES = 4;
    private static final double MAX_MISS_PROBES = 16;
    private static final double MAX_POOLED_HIT_PROBES = 2.25;
    private static final double MAX_POOLED_MISS_PROBES = 6.5;
    private static final int MIN_MEASURED_KEYS = 1_000;

    static {
        // The arena and the index are package-private to io.questdb.cairo.lv.
        try {
            final Class<?> arena = Class.forName("io.questdb.cairo.lv.LiveViewCheckpointKeyArena");
            final Class<?> index = Class.forName("io.questdb.cairo.lv.LiveViewCheckpointKeyIndex");
            ARENA_CONSTRUCTOR = accessible(arena.getDeclaredConstructor());
            ARENA_APPEND = accessible(arena.getDeclaredMethod("append", long.class, int.class));
            ARENA_CLOSE = accessible(arena.getDeclaredMethod("close"));
            INDEX_CONSTRUCTOR = accessible(index.getDeclaredConstructor(arena));
            INDEX_CLEAR = accessible(index.getDeclaredMethod("clear"));
            INDEX_CLOSE = accessible(index.getDeclaredMethod("close"));
            INDEX_GET = accessible(index.getDeclaredMethod("get", int.class, int.class, long.class, int.class));
            INDEX_HIT_PROBES = accessible(index.getDeclaredMethod("getHitProbeAverageForTest"));
            INDEX_MISS_PROBES = accessible(index.getDeclaredMethod("getMissProbeAverageForTest"));
            INDEX_PUT = accessible(index.getDeclaredMethod("put", int.class, int.class, long.class, int.class));
            INDEX_RELEASE = accessible(index.getDeclaredMethod("release"));
            INDEX_RESERVE = accessible(index.getDeclaredMethod("reserve", long.class));
            INDEX_SIZE = accessible(index.getDeclaredMethod("size"));
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Test
    public void testAFullTableFindsOrMissesAKeyInAFewProbes() throws Exception {
        // The price of a dense table is a longer probe: a lookup walks the run of occupied
        // slots from its key's home slot. A complete freeze looks every key up again, per
        // function, to find the partitions the previous root holds and this seal dropped,
        // and a chained capture's lookups mostly miss. At the fullest a table gets before
        // it doubles, the average lookup must still end within a few slots, for the key
        // shapes a live view partitions by, each under several function namespaces. Pooled
        // over all those tables, it must cost what an even spread of keys costs at that load.
        TestUtils.assertMemoryLeak(() -> {
            final int maxEntries = 100_000;
            double pooledHitProbes = 0;
            double pooledMissProbes = 0;
            long pooledEntries = 0;
            for (int shape = 0; shape < KEY_SHAPE_COUNT; shape++) {
                for (int namespaceCount : new int[]{1, 3}) {
                    final int keyCount = maxEntries / namespaceCount;
                    final Object arena = ARENA_CONSTRUCTOR.newInstance();
                    try (LiveViewCheckpointTestKeys probe = new LiveViewCheckpointTestKeys()) {
                        final long[] handles = new long[keyCount];
                        for (int i = 0; i < keyCount; i++) {
                            probe.of(shapedKey(shape, i));
                            handles[i] = (long) ARENA_APPEND.invoke(arena, probe.address(), probe.length());
                        }
                        // The entry counts at which the table doubled; the fullest table is
                        // the one an entry fewer holds.
                        final IntList doublings = new IntList();
                        final Object grown = INDEX_CONSTRUCTOR.newInstance(arena);
                        try {
                            long bytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                            for (int i = 0, n = keyCount * namespaceCount; i < n; i++) {
                                INDEX_PUT.invoke(grown, i % namespaceCount, 0, handles[i / namespaceCount], i);
                                final long nextBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                                if (nextBytes > bytes && i + 1 > MIN_MEASURED_KEYS) {
                                    doublings.add(i + 1);
                                }
                                bytes = nextBytes;
                            }
                        } finally {
                            INDEX_CLOSE.invoke(grown);
                        }
                        Assert.assertTrue(doublings.size() > 0);
                        for (int d = 0, m = doublings.size(); d < m; d++) {
                            final int entryCount = doublings.getQuick(d) - 1;
                            final Object index = INDEX_CONSTRUCTOR.newInstance(arena);
                            try {
                                for (int i = 0; i < entryCount; i++) {
                                    INDEX_PUT.invoke(index, i % namespaceCount, 0, handles[i / namespaceCount], i);
                                }
                                final double hitProbes = (double) INDEX_HIT_PROBES.invoke(index);
                                final double missProbes = (double) INDEX_MISS_PROBES.invoke(index);
                                final String table = " [shape=" + shape + ", namespaces=" + namespaceCount
                                        + ", entries=" + entryCount + ", hitProbes=" + hitProbes
                                        + ", missProbes=" + missProbes + ']';
                                Assert.assertTrue("a hit must take a few probes" + table, hitProbes <= MAX_HIT_PROBES);
                                Assert.assertTrue("a miss must take a few probes" + table, missProbes <= MAX_MISS_PROBES);
                                pooledHitProbes += hitProbes * entryCount;
                                pooledMissProbes += missProbes * entryCount;
                                pooledEntries += entryCount;
                            } finally {
                                INDEX_CLOSE.invoke(index);
                            }
                        }
                    } finally {
                        ARENA_CLOSE.invoke(arena);
                    }
                }
            }
            pooledHitProbes /= pooledEntries;
            pooledMissProbes /= pooledEntries;
            final String pooled = " [entries=" + pooledEntries + ", hitProbes=" + pooledHitProbes
                    + ", missProbes=" + pooledMissProbes + ']';
            Assert.assertTrue(
                    "a hit must cost what an even spread of keys costs" + pooled,
                    pooledHitProbes <= MAX_POOLED_HIT_PROBES
            );
            Assert.assertTrue(
                    "a miss must cost what an even spread of keys costs" + pooled,
                    pooledMissProbes <= MAX_POOLED_MISS_PROBES
            );
        });
    }

    @Test
    public void testAGrowingTableSpendsAtMostThreeSlotsOnEachKey() throws Exception {
        // A freeze scratch's index holds an entry per key per function a complete freeze
        // images, and a parked repair capture keeps its scratch's index, and a chained one
        // two more, for as long as it waits. So the table's bytes per key are what a wide
        // key domain costs while it waits. A table that grows as it fills must stay about a
        // third full or more at every size past the smallest.
        TestUtils.assertMemoryLeak(() -> {
            final int keyCount = 100_000;
            final Object arena = ARENA_CONSTRUCTOR.newInstance();
            final Object index = INDEX_CONSTRUCTOR.newInstance(arena);
            try (LiveViewCheckpointTestKeys probe = new LiveViewCheckpointTestKeys()) {
                final long[] handles = appendKeys(arena, probe, keyCount);
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                for (int i = 0; i < keyCount; i++) {
                    INDEX_PUT.invoke(index, i & 7, 0, handles[i], i);
                    final int size = i + 1;
                    if (size >= MIN_MEASURED_KEYS) {
                        assertBytesPerKey(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline, size);
                    }
                }
                Assert.assertEquals(keyCount, (int) INDEX_SIZE.invoke(index));
            } finally {
                INDEX_CLOSE.invoke(index);
                ARENA_CLOSE.invoke(arena);
            }
        });
    }

    @Test
    public void testAGrowthTheProcessCannotAllocateLeavesEveryEntry() throws Exception {
        // The index is process memory tagged NATIVE_LIVE_VIEW_IN_MEM and charges no view's
        // refresh tracker, so the limit its growth meets is the RSS ceiling. A refused
        // growth fails the put that triggered it at the allocation, before the old table
        // goes, so every entry already indexed still answers and the table keeps its bytes.
        TestUtils.assertMemoryLeak(() -> {
            final Object arena = ARENA_CONSTRUCTOR.newInstance();
            final Object index = INDEX_CONSTRUCTOR.newInstance(arena);
            try (LiveViewCheckpointTestKeys probe = new LiveViewCheckpointTestKeys()) {
                final long[] handles = appendKeys(arena, probe, 1_000);
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                for (int i = 0; i < 100; i++) {
                    INDEX_PUT.invoke(index, 0, 0, handles[i], i);
                }
                final long held = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline;
                Assert.assertTrue(held > 0);
                // The next doubling cannot fit under a ceiling at the current usage.
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                try {
                    for (int i = 100; i < 1_000; i++) {
                        INDEX_PUT.invoke(index, 0, 0, handles[i], i);
                    }
                    Assert.fail("expected the RSS ceiling to refuse the index's growth");
                } catch (InvocationTargetException e) {
                    Assert.assertTrue(e.getCause() instanceof CairoException);
                    Assert.assertTrue(((CairoException) e.getCause()).isOutOfMemory());
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
                Assert.assertEquals(held, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline);
                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(i, get(index, probe.of(key(i)), 0, 0));
                }
                INDEX_RELEASE.invoke(index);
                Assert.assertEquals(
                        "a release must hand every byte back",
                        baseline,
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM)
                );
            } finally {
                INDEX_CLOSE.invoke(index);
                ARENA_CLOSE.invoke(arena);
            }
        });
    }

    @Test
    public void testAReservedTableDoesNotRegrowAndARehashKeepsEveryEntry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Object arena = ARENA_CONSTRUCTOR.newInstance();
            final Object index = INDEX_CONSTRUCTOR.newInstance(arena);
            try (LiveViewCheckpointTestKeys probe = new LiveViewCheckpointTestKeys()) {
                final long[] handles = appendKeys(arena, probe, 1_000);
                final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                INDEX_RESERVE.invoke(index, 1_000L);
                INDEX_PUT.invoke(index, 0, 0, handles[0], 0);
                final long reserved = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - before;
                Assert.assertTrue("the reserved table must be allocated at the first put", reserved > 0);
                for (int i = 1; i < 1_000; i++) {
                    INDEX_PUT.invoke(index, 0, 0, handles[i], i);
                }
                Assert.assertEquals(
                        "a table reserved for its keys must not regrow while they fill it",
                        reserved,
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - before
                );
                // A reserve over a non-empty table rehashes it and keeps what it holds.
                INDEX_RESERVE.invoke(index, 4_000L);
                Assert.assertEquals(1_000, (int) INDEX_SIZE.invoke(index));
                for (int i = 0; i < 1_000; i++) {
                    Assert.assertEquals(i, get(index, probe.of(key(i)), 0, 0));
                }
                // A reserve below what the table holds changes nothing.
                final long grown = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                INDEX_RESERVE.invoke(index, 10L);
                Assert.assertEquals(grown, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
            } finally {
                INDEX_CLOSE.invoke(index);
                ARENA_CLOSE.invoke(arena);
            }
        });
    }

    @Test
    public void testAReservedTableSpendsAtMostThreeSlotsOnEachKey() throws Exception {
        // A complete freeze reserves the table for the keys it is about to image, so the
        // presized table must be as dense as a grown one, at either edge of every doubling,
        // and must hold every one of those keys in the table its first put allocates. The
        // pairs 1_432/1_433, 2_866/2_867, 45_874/45_875 and 91_749/91_750 are the most keys
        // 2^11, 2^12, 2^16 and 2^17 slots hold and the fewest that need twice the slots. A
        // table reserved one doubling too small for the second of a pair regrows on its last
        // put and ends at the right size, so only the bytes the first put allocated show it.
        TestUtils.assertMemoryLeak(() -> {
            final int[] keyCounts = {
                    1_000, 1_023, 1_024, 1_025, 1_432, 1_433, 2_047, 2_048, 2_866, 2_867,
                    45_874, 45_875, 65_535, 65_536, 91_749, 91_750
            };
            final Object arena = ARENA_CONSTRUCTOR.newInstance();
            try (LiveViewCheckpointTestKeys probe = new LiveViewCheckpointTestKeys()) {
                final long[] handles = appendKeys(arena, probe, keyCounts[keyCounts.length - 1]);
                for (int keyCount : keyCounts) {
                    final Object index = INDEX_CONSTRUCTOR.newInstance(arena);
                    try {
                        final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                        INDEX_RESERVE.invoke(index, (long) keyCount);
                        INDEX_PUT.invoke(index, 0, 0, handles[0], 0);
                        final long reserved = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline;
                        for (int i = 1; i < keyCount; i++) {
                            INDEX_PUT.invoke(index, i & 7, 0, handles[i], i);
                        }
                        final long held = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline;
                        Assert.assertEquals(
                                "a table reserved for its keys must not regrow while they fill it [keys=" + keyCount + ']',
                                reserved,
                                held
                        );
                        Assert.assertEquals(keyCount, (int) INDEX_SIZE.invoke(index));
                        assertBytesPerKey(held, keyCount);
                    } finally {
                        INDEX_CLOSE.invoke(index);
                    }
                }
            } finally {
                ARENA_CLOSE.invoke(arena);
            }
        });
    }

    @Test
    public void testIndexAnswersAsTheHeapIndexThroughEveryRehash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Object arena = ARENA_CONSTRUCTOR.newInstance();
            final Object index = INDEX_CONSTRUCTOR.newInstance(arena);
            final LiveViewCheckpointBinaryKeyIndex oracle = new LiveViewCheckpointBinaryKeyIndex();
            try (LiveViewCheckpointTestKeys probe = new LiveViewCheckpointTestKeys()) {
                final long[] handles = appendKeys(arena, probe, KEY_COUNT);
                for (int i = 0; i < KEY_COUNT; i++) {
                    INDEX_PUT.invoke(index, i & 7, i & 3, handles[i], i);
                    oracle.put(i & 7, i & 3, key(i), i);
                }
                Assert.assertEquals(oracle.size(), (int) INDEX_SIZE.invoke(index));
                for (int i = 0; i < KEY_COUNT; i++) {
                    final byte[] key = key(i);
                    probe.of(key);
                    Assert.assertEquals(oracle.get(i & 7, i & 3, key), get(index, probe, i & 7, i & 3));
                    Assert.assertEquals(-1, get(index, probe, (i & 7) + 8, i & 3));
                    Assert.assertEquals(-1, get(index, probe, i & 7, (i & 3) + 4));
                    Assert.assertEquals(-1, (int) INDEX_GET.invoke(index, i & 7, i & 3, probe.address(), KEY_LENGTH - 1));
                }
                // Keys never put, of the indexed width.
                for (int i = KEY_COUNT; i < KEY_COUNT + 1_000; i++) {
                    Assert.assertEquals(-1, get(index, probe.of(key(i)), i & 7, i & 3));
                }
                // A repeated put replaces the value an equal key maps to, whichever copy of the
                // key the arena holds it under.
                final long[] duplicates = appendKeys(arena, probe, 100);
                for (int i = 0; i < 100; i++) {
                    INDEX_PUT.invoke(index, i & 7, i & 3, duplicates[i], KEY_COUNT + i);
                }
                Assert.assertEquals(KEY_COUNT, (int) INDEX_SIZE.invoke(index));
                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(KEY_COUNT + i, get(index, probe.of(key(i)), i & 7, i & 3));
                }
                INDEX_CLEAR.invoke(index);
                Assert.assertEquals(0, (int) INDEX_SIZE.invoke(index));
                Assert.assertEquals(-1, get(index, probe.of(key(0)), 0, 0));
            } finally {
                INDEX_CLOSE.invoke(index);
                ARENA_CLOSE.invoke(arena);
            }
        });
    }

    @Test
    public void testKeysThatDifferOnlyInLeadingZeroBytesSpreadOverTheTable() throws Exception {
        // Leading zero bytes add nothing to the polynomial the index hashes key bytes with, so
        // the all-zero keys of every length, or the LONG 1 behind any number of zero LONGs,
        // hash their bytes alike. Each is a key of its own: it must find its own value, and
        // the index must spread them as it spreads any keys, not pile them into one run.
        TestUtils.assertMemoryLeak(() -> {
            final int keyCount = 1_000;
            for (boolean hasLongTail : new boolean[]{false, true}) {
                final Object arena = ARENA_CONSTRUCTOR.newInstance();
                final Object index = INDEX_CONSTRUCTOR.newInstance(arena);
                try (LiveViewCheckpointTestKeys probe = new LiveViewCheckpointTestKeys()) {
                    for (int i = 0; i < keyCount; i++) {
                        probe.of(zeroLedKey(hasLongTail, i));
                        INDEX_PUT.invoke(index, 0, 0, (long) ARENA_APPEND.invoke(arena, probe.address(), probe.length()), i);
                    }
                    Assert.assertEquals(keyCount, (int) INDEX_SIZE.invoke(index));
                    for (int i = 0; i < keyCount; i++) {
                        Assert.assertEquals(i, get(index, probe.of(zeroLedKey(hasLongTail, i)), 0, 0));
                    }
                    final double hitProbes = (double) INDEX_HIT_PROBES.invoke(index);
                    final double missProbes = (double) INDEX_MISS_PROBES.invoke(index);
                    final String table = " [hasLongTail=" + hasLongTail + ", hitProbes=" + hitProbes
                            + ", missProbes=" + missProbes + ']';
                    Assert.assertTrue("a hit must take a few probes" + table, hitProbes <= MAX_HIT_PROBES);
                    Assert.assertTrue("a miss must take a few probes" + table, missProbes <= MAX_MISS_PROBES);
                } finally {
                    INDEX_CLOSE.invoke(index);
                    ARENA_CLOSE.invoke(arena);
                }
            }
        });
    }

    @Test
    public void testReleaseAndCloseFreeEveryNativeByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Object arena = ARENA_CONSTRUCTOR.newInstance();
            final Object index = INDEX_CONSTRUCTOR.newInstance(arena);
            try (LiveViewCheckpointTestKeys probe = new LiveViewCheckpointTestKeys()) {
                final long[] handles = appendKeys(arena, probe, 2_000);
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
                for (int i = 0; i < 2_000; i++) {
                    INDEX_PUT.invoke(index, 1, 2, handles[i], i);
                }
                Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) > baseline);
                INDEX_RELEASE.invoke(index);
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
                Assert.assertEquals(0, (int) INDEX_SIZE.invoke(index));
                Assert.assertEquals(-1, get(index, probe.of(key(0)), 1, 2));
                // Released, not closed: it indexes again.
                INDEX_PUT.invoke(index, 1, 2, handles[7], 7);
                Assert.assertEquals(7, get(index, probe.of(key(7)), 1, 2));
                INDEX_CLOSE.invoke(index);
                INDEX_CLOSE.invoke(index);
                INDEX_RELEASE.invoke(index);
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
                try {
                    INDEX_PUT.invoke(index, 1, 2, handles[0], -1);
                    Assert.fail("a negative value is the index's miss and must be refused");
                } catch (InvocationTargetException e) {
                    Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
                }
            } finally {
                INDEX_CLOSE.invoke(index);
                ARENA_CLOSE.invoke(arena);
            }
        });
    }

    private static <T extends java.lang.reflect.AccessibleObject> T accessible(T member) {
        member.setAccessible(true);
        return member;
    }

    /**
     * Appends keys {@code 0 .. count - 1} to {@code arena}, copied from native memory as a
     * freeze copies them out of its key buffer.
     */
    private static long[] appendKeys(Object arena, LiveViewCheckpointTestKeys keys, int count) throws Exception {
        final long[] handles = new long[count];
        for (int i = 0; i < count; i++) {
            keys.of(key(i));
            handles[i] = (long) ARENA_APPEND.invoke(arena, keys.address(), keys.length());
        }
        return handles;
    }

    private static void assertBytesPerKey(long heldBytes, int keyCount) {
        if (heldBytes > MAX_BYTES_PER_KEY * keyCount) {
            Assert.fail(
                    "the index must spend at most " + MAX_BYTES_PER_KEY + " bytes on each key [keys=" + keyCount
                            + ", bytes=" + heldBytes + ", bytesPerKey=" + (double) heldBytes / keyCount + ']'
            );
        }
    }

    private static int get(Object index, LiveViewCheckpointTestKeys probe, int namespace, int version) throws Exception {
        return (int) INDEX_GET.invoke(index, namespace, version, probe.address(), probe.length());
    }

    /**
     * A key whose leading bytes repeat across the set, so probing meets real collisions.
     */
    private static byte[] key(int value) {
        return new byte[]{
                (byte) (value & 7),
                (byte) value,
                (byte) (value >>> 8),
                (byte) (value >>> 16),
                (byte) (value >>> 24),
                (byte) 0x80
        };
    }

    private static byte[] longKey(long value) {
        final byte[] bytes = new byte[Long.BYTES];
        for (int i = 0; i < Long.BYTES; i++) {
            bytes[i] = (byte) (value >>> (8 * i));
        }
        return bytes;
    }

    /**
     * Key {@code value} in one of the byte shapes a freeze encodes a partition key in.
     */
    private static byte[] shapedKey(int shape, int value) {
        return switch (shape) {
            case 0 -> key(value);
            // INT, little endian.
            case 1 -> new byte[]{(byte) value, (byte) (value >>> 8), (byte) (value >>> 16), (byte) (value >>> 24)};
            // LONG ids.
            case 2 -> longKey(value);
            // TIMESTAMP: hours in micros, whose low bits are all zero.
            case 3 -> longKey(value * 3_600_000_000L);
            default -> {
                // SYMBOL, frozen as STRING: an int length, then UTF-16LE chars.
                final String s = "sym_" + value;
                final byte[] bytes = new byte[Integer.BYTES + 2 * s.length()];
                bytes[0] = (byte) s.length();
                for (int i = 0; i < s.length(); i++) {
                    bytes[Integer.BYTES + 2 * i] = (byte) s.charAt(i);
                }
                yield bytes;
            }
        };
    }

    /**
     * A key of {@code zeroCount} zero bytes, or, with a LONG tail, of {@code zeroCount} zero
     * LONGs followed by the LONG 1.
     */
    private static byte[] zeroLedKey(boolean hasLongTail, int zeroCount) {
        if (!hasLongTail) {
            return new byte[zeroCount];
        }
        final byte[] bytes = new byte[Long.BYTES * (zeroCount + 1)];
        bytes[Long.BYTES * zeroCount] = 1;
        return bytes;
    }
}
