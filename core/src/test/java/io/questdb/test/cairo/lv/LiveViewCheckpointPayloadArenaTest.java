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
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The native arena checkpoint payloads move into must keep every promise a heap array made
 * and the few a native region adds. A payload's handle names its bytes across every growth
 * of the region, while an address is only a view of the current region. A reserved record
 * reads as zeroes wherever its encoder writes nothing, however dirty the bytes a cleared
 * arena reuses, because the persisted image depends on those zeroes. The arena copies a
 * payload exactly and refuses one it holds itself. It grows once to a presized capacity and
 * not again while that capacity lasts. A failed allocation leaves every payload it already
 * holds. And every byte comes back on release and on close.
 */
public class LiveViewCheckpointPayloadArenaTest {
    private static final Method ADDRESS;
    private static final Method APPEND;
    private static final Method BYTES_OFFSET;
    private static final Method CAPACITY;
    private static final Method CLEAR;
    private static final Method CLOSE;
    private static final Constructor<?> CONSTRUCTOR;
    private static final Method ENSURE_CAPACITY;
    private static final Method LENGTH;
    private static final Method MEMORY;
    private static final long NO_PAYLOAD;
    private static final long PAGE_SIZE = 4096;
    private static final Method PAYLOAD_COUNT;
    private static final Method RECORD_BYTES;
    private static final Method RELEASE;
    private static final Method RESERVE;
    private static final Method SIZE;

    static {
        // The arena is package-private to io.questdb.cairo.lv.
        try {
            final Class<?> arena = Class.forName("io.questdb.cairo.lv.LiveViewCheckpointPayloadArena");
            CONSTRUCTOR = accessible(arena.getDeclaredConstructor());
            ADDRESS = accessible(arena.getDeclaredMethod("address", long.class));
            APPEND = accessible(arena.getDeclaredMethod("append", long.class, int.class));
            BYTES_OFFSET = accessible(arena.getDeclaredMethod("bytesOffset", long.class));
            CAPACITY = accessible(arena.getDeclaredMethod("capacity"));
            CLEAR = accessible(arena.getDeclaredMethod("clear"));
            CLOSE = accessible(arena.getDeclaredMethod("close"));
            ENSURE_CAPACITY = accessible(arena.getDeclaredMethod("ensureCapacity", long.class));
            LENGTH = accessible(arena.getDeclaredMethod("length", long.class));
            MEMORY = accessible(arena.getDeclaredMethod("memory"));
            PAYLOAD_COUNT = accessible(arena.getDeclaredMethod("payloadCount"));
            RECORD_BYTES = accessible(arena.getDeclaredMethod("recordBytes", int.class));
            RELEASE = accessible(arena.getDeclaredMethod("release"));
            RESERVE = accessible(arena.getDeclaredMethod("reserve", int.class));
            SIZE = accessible(arena.getDeclaredMethod("size"));
            NO_PAYLOAD = accessible(arena.getDeclaredField("NO_PAYLOAD")).getLong(null);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Test
    public void testAFailedGrowthLeavesEveryPayloadItHolds() throws Exception {
        // The arena is process memory tagged NATIVE_LIVE_VIEW_IN_MEM and charges no view's
        // refresh tracker, so the limit its growth meets is the RSS ceiling. A refused growth
        // fails the reserve or append that asked for it before the append offset moves: the
        // arena keeps its size, count, capacity and every payload, and it frees on close as
        // if nothing had happened.
        TestUtils.assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            final int sourceLength = 3 * (int) PAGE_SIZE;
            final long source = Unsafe.malloc(sourceLength, MemoryTag.NATIVE_DEFAULT);
            try (Arena arena = new Arena()) {
                final Rnd rnd = new Rnd(11, 17);
                fillRandom(rnd, source, sourceLength);
                final LongList handles = new LongList();
                for (int i = 0; i < 50; i++) {
                    handles.add(arena.append(source + i, 16 + i));
                }
                final long size = arena.size();
                final long capacity = arena.capacity();
                Assert.assertEquals(capacity, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline);

                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                try {
                    try {
                        arena.reserve((int) capacity);
                        Assert.fail("expected the RSS ceiling to refuse the reserve's growth");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                    }
                    try {
                        arena.append(source, sourceLength);
                        Assert.fail("expected the RSS ceiling to refuse the append's growth");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                    }
                } finally {
                    Unsafe.setRssMemLimit(0);
                }

                Assert.assertEquals("a refused growth must not move the append offset", size, arena.size());
                Assert.assertEquals(handles.size(), arena.payloadCount());
                Assert.assertEquals(capacity, arena.capacity());
                for (int i = 0, n = handles.size(); i < n; i++) {
                    assertPayload(arena, handles.getQuick(i), source + i, 16 + i);
                }
                // The next record lands right after the last one the failures left intact.
                Assert.assertEquals(size, arena.append(source, 8));
            } finally {
                Unsafe.free(source, sourceLength, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
        });
    }

    @Test
    public void testAppendCopiesExactlyAndRefusesItsOwnBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int maxLength = 300;
            final long source = Unsafe.malloc(maxLength, MemoryTag.NATIVE_DEFAULT);
            try (Arena arena = new Arena()) {
                final Rnd rnd = new Rnd(3, 5);
                long expectedHandle = 0;
                for (int length = 1; length <= maxLength; length++) {
                    fillRandom(rnd, source, length);
                    final long handle = arena.append(source, length);
                    Assert.assertEquals("a handle is its record's offset", expectedHandle, handle);
                    assertPayload(arena, handle, source, length);
                    assertRecordTail(arena, handle, length);
                    expectedHandle += Arena.recordBytes(length);
                }
                Assert.assertEquals(expectedHandle, arena.size());
                Assert.assertEquals(maxLength, arena.payloadCount());

                // A payload the arena already holds cannot be the source of another: the copy
                // may grow, and so move, the very memory it reads.
                final long held = arena.address(0);
                final AssertionError e = Assert.assertThrows(AssertionError.class, () -> arena.append(held, 1));
                TestUtils.assertContains(e.getMessage(), "aliases its own arena");
                Assert.assertEquals("the rejected payload must not be staged", expectedHandle, arena.size());
                Assert.assertEquals(maxLength, arena.payloadCount());
            } finally {
                Unsafe.free(source, maxLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testClearKeepsCapacityReleaseFreesAndCloseIsIdempotent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            final long source = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            final Arena arena = new Arena();
            try {
                Assert.assertEquals("a new arena allocates nothing", 0, arena.capacity());
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
                Vect.memset(source, 64, 0x5a);
                for (int i = 0; i < 1_000; i++) {
                    arena.append(source, 64);
                }
                final long capacity = arena.capacity();
                Assert.assertTrue(capacity >= 1_000 * Arena.recordBytes(64));
                Assert.assertEquals(capacity, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline);

                arena.clear();
                Assert.assertEquals(0, arena.size());
                Assert.assertEquals(0, arena.payloadCount());
                Assert.assertEquals("a clear keeps the capacity", capacity, arena.capacity());
                Assert.assertEquals(capacity, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline);
                Assert.assertEquals("a cleared arena starts over at offset zero", 0, arena.append(source, 64));
                Assert.assertEquals(capacity, arena.capacity());

                arena.release();
                Assert.assertEquals(0, arena.capacity());
                Assert.assertEquals(0, arena.size());
                Assert.assertEquals(0, arena.payloadCount());
                Assert.assertEquals("a release must hand every byte back", baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));

                // A released arena stays usable: the next record allocates afresh.
                final long handle = arena.append(source, 64);
                Assert.assertEquals(0, handle);
                assertPayload(arena, handle, source, 64);
                Assert.assertEquals(PAGE_SIZE, arena.capacity());
                arena.release();
                arena.release();
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));

                arena.append(source, 64);
            } finally {
                arena.close();
                Unsafe.free(source, 64, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals("a close must hand every byte back", baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
            arena.close();
            Assert.assertEquals(0, arena.capacity());
            Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
        });
    }

    @Test
    public void testEnsureCapacityGrowsOnceAndOnlyWhenNeeded() throws Exception {
        // A presize takes the region straight to the pages it names: five pages here, where
        // growth record by record would have doubled through one, two, four and eight. The
        // records it was sized for then land without growing it again, and a presize that
        // already fits is a no-op.
        TestUtils.assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            try (Arena arena = new Arena()) {
                arena.ensureCapacity(0);
                Assert.assertEquals("a presize of nothing allocates nothing", 0, arena.capacity());

                final int payloadLength = 56;
                final long recordBytes = Arena.recordBytes(payloadLength);
                final int recordCount = (int) (5 * PAGE_SIZE / recordBytes);
                arena.ensureCapacity(recordCount * recordBytes);
                final long capacity = arena.capacity();
                Assert.assertEquals(5 * PAGE_SIZE, capacity);
                Assert.assertEquals(capacity, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - baseline);

                for (int i = 0; i < recordCount; i++) {
                    arena.reserve(payloadLength);
                    arena.ensureCapacity(0);
                }
                Assert.assertEquals(recordCount * recordBytes, arena.size());
                Assert.assertEquals("the presized records must not grow the arena again", capacity, arena.capacity());
                arena.ensureCapacity(capacity - arena.size());
                Assert.assertEquals("a presize that fits must be a no-op", capacity, arena.capacity());

                // One record past the presize grows the region once more.
                arena.ensureCapacity(capacity - arena.size() + 1);
                Assert.assertTrue(arena.capacity() > capacity);
                Assert.assertEquals(recordCount * recordBytes, arena.size());
                Assert.assertEquals(recordCount, arena.payloadCount());
            }
            Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
        });
    }

    @Test
    public void testHandlesNameTheirPayloadsAcrossEveryGrowth() throws Exception {
        // Thousands of payloads of every width from 1 to 257 bytes grow the region several
        // times over. The handles taken before each growth still name exactly the payloads
        // they named, and an address is always derived from the region as it is now.
        TestUtils.assertMemoryLeak(() -> {
            final int maxLength = 257;
            final int payloadCount = 4_000;
            final long source = Unsafe.malloc((long) payloadCount + maxLength, MemoryTag.NATIVE_DEFAULT);
            try (Arena arena = new Arena()) {
                fillRandom(new Rnd(7, 13), source, payloadCount + maxLength);
                final LongList handles = new LongList();
                long growthCount = 0;
                long capacity = arena.capacity();
                for (int i = 0; i < payloadCount; i++) {
                    handles.add(arena.append(source + i, lengthOf(i, maxLength)));
                    if (arena.capacity() != capacity) {
                        growthCount++;
                        capacity = arena.capacity();
                    }
                }
                Assert.assertTrue("the case must grow the region more than once, grew " + growthCount, growthCount > 3);
                final MemoryR memory = arena.memory();
                for (int i = 0; i < payloadCount; i++) {
                    final long handle = handles.getQuick(i);
                    Assert.assertTrue("a handle must never be NO_PAYLOAD", handle >= 0 && handle != NO_PAYLOAD);
                    Assert.assertEquals("a record starts on an eight-byte boundary", 0, handle & 7);
                    Assert.assertEquals(memory.addressOf(arena.bytesOffset(handle)), arena.address(handle));
                    assertPayload(arena, handle, source + i, lengthOf(i, maxLength));
                    assertRecordTail(arena, handle, lengthOf(i, maxLength));
                }
            } finally {
                Unsafe.free(source, (long) payloadCount + maxLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNoPayloadNeverNamesARecord() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertEquals(-1, NO_PAYLOAD);
            try (Arena arena = new Arena()) {
                Assert.assertEquals("the first record starts at offset zero, not at the sentinel", 0, arena.reserve(1));
                AssertionError e = Assert.assertThrows(AssertionError.class, () -> arena.address(NO_PAYLOAD));
                TestUtils.assertContains(e.getMessage(), "payload handle outside its arena");
                e = Assert.assertThrows(AssertionError.class, () -> arena.length(NO_PAYLOAD));
                TestUtils.assertContains(e.getMessage(), "payload handle outside its arena");
                // A handle past the last record, and one inside a record rather than at its
                // start, are rejected the same way.
                e = Assert.assertThrows(AssertionError.class, () -> arena.address(arena.size()));
                TestUtils.assertContains(e.getMessage(), "payload handle outside its arena");
                e = Assert.assertThrows(AssertionError.class, () -> arena.address(4));
                TestUtils.assertContains(e.getMessage(), "payload handle outside its arena");
                e = Assert.assertThrows(AssertionError.class, () -> arena.reserve(0));
                TestUtils.assertContains(e.getMessage(), "must not be empty");
                Assert.assertEquals(1, arena.payloadCount());
            }
        });
    }

    @Test
    public void testReserveZeroFillsTheWholeRecordOverDirtyBytes() throws Exception {
        // An encoder may leave part of its payload unwritten and rely on it reading as zero,
        // as the heap arrays it replaces did. A cleared arena reuses bytes earlier payloads
        // wrote, so a reserve must zero its whole record - the header's pad word, the
        // payload and the tail padding - rather than trust what is there.
        TestUtils.assertMemoryLeak(() -> {
            try (Arena arena = new Arena()) {
                for (int i = 0; i < 200; i++) {
                    arena.reserve(1 + i % 61);
                }
                final long size = arena.size();
                final long capacity = arena.capacity();
                Vect.memset(arena.memory().addressOf(0), capacity, 0xff);
                arena.clear();
                Assert.assertEquals(capacity, arena.capacity());

                long expectedHandle = 0;
                for (int i = 0; expectedHandle < size; i++) {
                    final int length = 1 + (i * 7) % 29;
                    final long handle = arena.reserve(length);
                    Assert.assertEquals(expectedHandle, handle);
                    Assert.assertEquals(length, arena.length(handle));
                    final long address = arena.address(handle);
                    for (int b = 0; b < length; b++) {
                        Assert.assertEquals("payload byte " + b + " of record " + i, 0, Unsafe.getByte(address + b));
                    }
                    assertRecordTail(arena, handle, length);
                    expectedHandle += Arena.recordBytes(length);
                }
                // An appended payload after a clear pads with zeroes too.
                arena.clear();
                final long source = Unsafe.malloc(3, MemoryTag.NATIVE_DEFAULT);
                try {
                    Vect.memset(source, 3, 0x11);
                    final long handle = arena.append(source, 3);
                    assertPayload(arena, handle, source, 3);
                    assertRecordTail(arena, handle, 3);
                } finally {
                    Unsafe.free(source, 3, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static <T extends AccessibleObject> T accessible(T member) {
        member.setAccessible(true);
        return member;
    }

    private static void assertPayload(Arena arena, long handle, long expected, int length) {
        Assert.assertEquals(length, arena.length(handle));
        Assert.assertTrue("payload bytes must be copied exactly", Vect.memeq(arena.address(handle), expected, length));
    }

    /**
     * Requires the record's header pad word and every padding byte past the payload to be
     * zero: both are part of the persisted image's zero-fill contract.
     */
    private static void assertRecordTail(Arena arena, long handle, int length) {
        final long record = arena.memory().addressOf(handle);
        Assert.assertEquals(length, Unsafe.getInt(record));
        Assert.assertEquals("header pad word", 0, Unsafe.getInt(record + Integer.BYTES));
        final long recordBytes = Arena.recordBytes(length);
        Assert.assertEquals(0, recordBytes & 7);
        for (long b = 2L * Integer.BYTES + length; b < recordBytes; b++) {
            Assert.assertEquals("padding byte " + b, 0, Unsafe.getByte(record + b));
        }
    }

    private static void fillRandom(Rnd rnd, long address, long length) {
        for (long i = 0; i < length; i++) {
            Unsafe.putByte(address + i, rnd.nextByte());
        }
    }

    private static int lengthOf(int index, int maxLength) {
        return 1 + (index * 31) % maxLength;
    }

    /**
     * A typed face over the package-private arena, so the cases read as calls rather than as
     * reflection. A failure inside the arena comes out as itself.
     */
    private static final class Arena implements QuietCloseable {
        private final Object arena;

        private Arena() {
            try {
                arena = CONSTRUCTOR.newInstance();
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(e);
            }
        }

        static long recordBytes(int payloadLength) {
            return (long) invoke(RECORD_BYTES, null, payloadLength);
        }

        @Override
        public void close() {
            invoke(CLOSE, arena);
        }

        long address(long handle) {
            return (long) invoke(ADDRESS, arena, handle);
        }

        long append(long address, int length) {
            return (long) invoke(APPEND, arena, address, length);
        }

        long bytesOffset(long handle) {
            return (long) invoke(BYTES_OFFSET, arena, handle);
        }

        long capacity() {
            return (long) invoke(CAPACITY, arena);
        }

        void clear() {
            invoke(CLEAR, arena);
        }

        void ensureCapacity(long additionalBytes) {
            invoke(ENSURE_CAPACITY, arena, additionalBytes);
        }

        int length(long handle) {
            return (int) invoke(LENGTH, arena, handle);
        }

        MemoryR memory() {
            return (MemoryR) invoke(MEMORY, arena);
        }

        int payloadCount() {
            return (int) invoke(PAYLOAD_COUNT, arena);
        }

        void release() {
            invoke(RELEASE, arena);
        }

        long reserve(int length) {
            return (long) invoke(RESERVE, arena, length);
        }

        long size() {
            return (long) invoke(SIZE, arena);
        }

        private static Object invoke(Method method, Object target, Object... args) {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                if (cause instanceof Error error) {
                    throw error;
                }
                throw new AssertionError(cause);
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }
    }
}
