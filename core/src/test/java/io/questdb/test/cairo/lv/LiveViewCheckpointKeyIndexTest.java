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
 * it already holds.
 */
public class LiveViewCheckpointKeyIndexTest {
    private static final Method ARENA_APPEND;
    private static final Method ARENA_CLOSE;
    private static final Constructor<?> ARENA_CONSTRUCTOR;
    private static final Method INDEX_CLEAR;
    private static final Method INDEX_CLOSE;
    private static final Constructor<?> INDEX_CONSTRUCTOR;
    private static final Method INDEX_GET;
    private static final Method INDEX_PUT;
    private static final Method INDEX_RELEASE;
    private static final Method INDEX_RESERVE;
    private static final Method INDEX_SIZE;
    private static final int KEY_COUNT = 50_000;
    private static final int KEY_LENGTH = 6;

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
            INDEX_PUT = accessible(index.getDeclaredMethod("put", int.class, int.class, long.class, int.class));
            INDEX_RELEASE = accessible(index.getDeclaredMethod("release"));
            INDEX_RESERVE = accessible(index.getDeclaredMethod("reserve", long.class));
            INDEX_SIZE = accessible(index.getDeclaredMethod("size"));
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
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
}
