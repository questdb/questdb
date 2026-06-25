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

package io.questdb.std;

import io.questdb.cairo.CairoException;
import org.jetbrains.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.LongAdder;

import static io.questdb.std.MemoryTag.NATIVE_DEFAULT;

@SuppressWarnings("removal")
public final class Unsafe {
    // The various _ADDR fields are `long` in Java, but they are `* mut usize` in Rust, or `size_t*` in C.
    // These are off-heap allocated atomic counters for memory usage tracking.

    public static final long BYTE_OFFSET;
    public static final long BYTE_SCALE;
    public static final long INT_OFFSET;
    public static final long INT_SCALE;
    public static final long LONG_OFFSET;
    public static final long LONG_SCALE;
    // The `{used, limit}` counters backing a MemoryTracker occupy the first 16
    // bytes and must match `struct MemoryTracker` in `allocator.rs` (Rust reads
    // `used` at offset 0, `limit` at offset 8). The block is allocated a full
    // cache line wide with the counters at offset 0 so each pooled tracker owns
    // its own line: a query's workers updating `used` cannot invalidate an
    // unrelated concurrent query's counter (cross-query false sharing). Rust
    // views only the 16-byte head, never the trailing padding.
    public static final long MEMORY_TRACKER_BLOCK_SIZE = Misc.CACHE_LINE_SIZE;
    public static final long MEMORY_TRACKER_LIMIT_OFFSET = 8;
    public static final long MEMORY_TRACKER_USED_OFFSET = 0;
    private static final LongAdder[] COUNTERS = new LongAdder[MemoryTag.SIZE];
    private static final long FREE_COUNT_ADDR;
    private static final long MALLOC_COUNT_ADDR;
    private static final long[] NATIVE_ALLOCATORS = new long[MemoryTag.SIZE - NATIVE_DEFAULT];
    private static final long[] NATIVE_MEM_COUNTER_ADDRS = new long[MemoryTag.SIZE];
    private static final long NON_RSS_MEM_USED_ADDR;
    // Size of the native QdbAllocator block. Layout: {mem_tracking, memory_tracker, tagged_used, memory_tag}.
    // Must match `struct QdbAllocator` (#[repr(C, packed)]) in `allocator.rs`.
    private static final long QDB_ALLOCATOR_SIZE = 8 + 8 + 8 + 4;
    private static final long REALLOC_COUNT_ADDR;
    private static final long RSS_MEM_LIMIT_ADDR;
    private static final long RSS_MEM_USED_ADDR;
    private static final sun.misc.Unsafe UNSAFE;
    private static final AnonymousClassDefiner anonymousClassDefiner;

    private Unsafe() {
    }

    public static long allocateMemory(long size) {
        return UNSAFE.allocateMemory(size);
    }

    public static int arrayBaseOffset(Class<?> arrayClass) {
        return UNSAFE.arrayBaseOffset(arrayClass);
    }

    public static long arrayGetVolatile(long[] array, int index) {
        assert index > -1 && index < array.length;
        return UNSAFE.getLongVolatile(array, LONG_OFFSET + ((long) index << LONG_SCALE));
    }

    public static int arrayGetVolatile(int[] array, int index) {
        assert index > -1 && index < array.length;
        return UNSAFE.getIntVolatile(array, INT_OFFSET + ((long) index << INT_SCALE));
    }

    public static int arrayIndexScale(Class<?> arrayClass) {
        return UNSAFE.arrayIndexScale(arrayClass);
    }

    /**
     * This call has Atomic*#lazySet / memory_order_release semantics.
     *
     * @param array array to put into
     * @param index index
     * @param value value to put
     */
    public static void arrayPutOrdered(long[] array, int index, long value) {
        assert index > -1 && index < array.length;
        UNSAFE.putOrderedLong(array, LONG_OFFSET + ((long) index << LONG_SCALE), value);
    }

    /**
     * This call has Atomic*#lazySet / memory_order_release semantics.
     *
     * @param array array to put into
     * @param index index
     * @param value value to put
     */
    public static void arrayPutOrdered(int[] array, int index, int value) {
        assert index > -1 && index < array.length;
        UNSAFE.putOrderedInt(array, INT_OFFSET + ((long) index << INT_SCALE), value);
    }

    public static int byteArrayGetInt(byte[] array, int index) {
        assert index > -1 && index < array.length - 3;
        return UNSAFE.getInt(array, BYTE_OFFSET + index);
    }

    public static long byteArrayGetLong(byte[] array, int index) {
        assert index > -1 && index < array.length - 7;
        return UNSAFE.getLong(array, BYTE_OFFSET + index);
    }

    public static short byteArrayGetShort(byte[] array, int index) {
        assert index > -1 && index < array.length - 1;
        return UNSAFE.getShort(array, BYTE_OFFSET + index);
    }

    public static long calloc(long size, int memoryTag) {
        long ptr = malloc(size, memoryTag);
        Vect.memset(ptr, size, 0);
        return ptr;
    }

    public static boolean cas(Object o, long offset, long expected, long value) {
        return UNSAFE.compareAndSwapLong(o, offset, expected, value);
    }

    public static boolean cas(Object o, long offset, int expected, int value) {
        return UNSAFE.compareAndSwapInt(o, offset, expected, value);
    }

    public static boolean cas(Object o, long offset, Object expected, Object value) {
        return UNSAFE.compareAndSwapObject(o, offset, expected, value);
    }

    public static boolean cas(long[] array, int index, long expected, long value) {
        assert index > -1 && index < array.length;
        return Unsafe.cas(array, Unsafe.LONG_OFFSET + (((long) index) << Unsafe.LONG_SCALE), expected, value);
    }

    public static void copyMemory(long srcAddress, long destAddress, long bytes) {
        UNSAFE.copyMemory(srcAddress, destAddress, bytes);
    }

    public static void copyMemory(Object srcBase, long srcOffset, Object destBase, long destOffset, long bytes) {
        UNSAFE.copyMemory(srcBase, srcOffset, destBase, destOffset, bytes);
    }

    /**
     * Defines a class but does not make it known to the class loader or system dictionary.
     * <p>
     * Equivalent to {@code Unsafe#defineAnonymousClass} and {@code Lookup#defineHiddenClass}, except that
     * it does not support constant pool patches.
     *
     * @param hostClass context for linkage, access control, protection domain, and class loader
     * @param data      bytes of a class file
     * @return Java Class for the given bytecode
     */
    @Nullable
    public static Class<?> defineAnonymousClass(Class<?> hostClass, byte[] data) {
        return anonymousClassDefiner.define(hostClass, data);
    }

    public static long free(long ptr, long size, int memoryTag) {
        if (ptr != 0) {
            UNSAFE.freeMemory(ptr);
            incrFreeCount();
            recordMemAlloc(-size, memoryTag);
        }
        return 0;
    }

    /**
     * Tracker-aware variant. A {@code null} tracker degrades to the
     * global-only {@link #free(long, long, int)} variant.
     */
    public static long free(long ptr, long size, int memoryTag, @Nullable MemoryTracker tracker) {
        if (tracker == null) {
            return free(ptr, size, memoryTag);
        }
        if (ptr != 0) {
            UNSAFE.freeMemory(ptr);
            incrFreeCount();
            recordMemAlloc(-size, memoryTag);
            recordPerQueryMemAlloc(-size, tracker.nativeAddress());
        }
        return 0;
    }

    public static void freeMemory(long ptr) {
        UNSAFE.freeMemory(ptr);
    }

    public static int getAndAddInt(Object o, long offset, int delta) {
        return UNSAFE.getAndAddInt(o, offset, delta);
    }

    public static long getAndAddLong(Object o, long offset, long delta) {
        return UNSAFE.getAndAddLong(o, offset, delta);
    }

    public static boolean getBool(long address) {
        return UNSAFE.getByte(address) == 1;
    }

    public static boolean getBoolean(Object o, long offset) {
        return UNSAFE.getBoolean(o, offset);
    }

    public static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    public static char getChar(long address) {
        return UNSAFE.getChar(address);
    }

    public static double getDouble(long address) {
        return UNSAFE.getDouble(address);
    }

    public static long getFieldOffset(Class<?> clazz, String name) {
        try {
            return UNSAFE.objectFieldOffset(clazz.getDeclaredField(name));
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static float getFloat(long address) {
        return UNSAFE.getFloat(address);
    }

    public static long getFreeCount() {
        return UNSAFE.getLongVolatile(null, FREE_COUNT_ADDR);
    }

    public static int getInt(long address) {
        return UNSAFE.getInt(address);
    }

    public static int getInt(Object o, long offset) {
        return UNSAFE.getInt(o, offset);
    }

    public static int getIntVolatile(Object o, long offset) {
        return UNSAFE.getIntVolatile(o, offset);
    }

    public static long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    public static long getLong(Object o, long offset) {
        return UNSAFE.getLong(o, offset);
    }

    public static long getLongVolatile(long address) {
        return UNSAFE.getLongVolatile(null, address);
    }

    public static long getMallocCount() {
        return UNSAFE.getLongVolatile(null, MALLOC_COUNT_ADDR);
    }

    /**
     * Get the total memory used by the process, this includes both resident memory
     * and that assigned to memory mapped files.
     */
    public static long getMemUsed() {
        return getNonRssMemUsed() + getRssMemUsed();
    }

    public static long getMemUsedByTag(int memoryTag) {
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        return COUNTERS[memoryTag].sum() + UNSAFE.getLongVolatile(null, NATIVE_MEM_COUNTER_ADDRS[memoryTag]);
    }

    /**
     * Returns a `*const QdbAllocator` for use in Rust.
     */
    public static long getNativeAllocator(int memoryTag) {
        return NATIVE_ALLOCATORS[memoryTag - NATIVE_DEFAULT];
    }

    /**
     * Tracker-aware variant. Returns a `*const QdbAllocator` whose
     * `memory_tracker` field points at the given tracker's native block, so
     * Rust-side allocations charge both the global counter and the
     * per-workload counter. A {@code null} tracker degrades to the global-only
     * {@link #getNativeAllocator(int)}.
     * <p>
     * The returned pointer is owned by the tracker and remains valid until
     * the tracker's owning provider is closed.
     */
    public static long getNativeAllocator(int memoryTag, @Nullable MemoryTracker tracker) {
        if (tracker == null) {
            return getNativeAllocator(memoryTag);
        }
        return tracker.getOrCreateNativeAllocator(memoryTag);
    }

    public static long getNonRssMemUsed() {
        return UNSAFE.getLongVolatile(null, NON_RSS_MEM_USED_ADDR);
    }

    public static Object getObject(Object o, long offset) {
        return UNSAFE.getObject(o, offset);
    }

    public static Object getObjectVolatile(Object o, long offset) {
        return UNSAFE.getObjectVolatile(o, offset);
    }

    public static long getReallocCount() {
        return UNSAFE.getLongVolatile(null, REALLOC_COUNT_ADDR);
    }

    public static long getRssMemLimit() {
        return UNSAFE.getLongVolatile(null, RSS_MEM_LIMIT_ADDR);
    }

    public static long getRssMemUsed() {
        return UNSAFE.getLongVolatile(null, RSS_MEM_USED_ADDR);
    }

    public static short getShort(long address) {
        return UNSAFE.getShort(address);
    }

    public static sun.misc.Unsafe getUnsafe() {
        return UNSAFE;
    }

    public static void incrFreeCount() {
        UNSAFE.getAndAddLong(null, FREE_COUNT_ADDR, 1);
    }

    public static void incrMallocCount() {
        UNSAFE.getAndAddLong(null, MALLOC_COUNT_ADDR, 1);
    }

    public static void incrReallocCount() {
        UNSAFE.getAndAddLong(null, REALLOC_COUNT_ADDR, 1);
    }

    public static void loadFence() {
        UNSAFE.loadFence();
    }

    public static long malloc(long size, int memoryTag) {
        try {
            assert memoryTag >= MemoryTag.NATIVE_PATH;
            checkAllocLimit(size, memoryTag);
            long ptr = UNSAFE.allocateMemory(size);
            recordMemAlloc(size, memoryTag);
            incrMallocCount();
            return ptr;
        } catch (OutOfMemoryError oom) {
            CairoException e = CairoException.nonCritical().setOutOfMemory(true)
                    .put("sun.misc.Unsafe.allocateMemory() OutOfMemoryError [RSS_MEM_USED=")
                    .put(getRssMemUsed())
                    .put(", size=")
                    .put(size)
                    .put(", memoryTag=").put(memoryTag)
                    .put("], original message: ")
                    .put(oom.getMessage());
            System.err.println(e.getFlyweightMessage());
            throw e;
        }
    }

    /**
     * Tracker-aware variant. Performs both global and per-workload limit
     * checks before allocating; on success, updates both counters. A
     * {@code null} tracker degrades to the global-only
     * {@link #malloc(long, int)} variant.
     * <p>
     * On global breach throws the existing global-RSS message; on per-workload
     * breach throws a distinct {@code "query memory limit exceeded"} message
     * carrying the workload, query id, limit, used, requested size, and
     * memory tag. Both throw via
     * {@code CairoException.nonCritical().setOutOfMemory(true)}.
     */
    public static long malloc(long size, int memoryTag, @Nullable MemoryTracker tracker) {
        if (tracker == null) {
            return malloc(size, memoryTag);
        }
        try {
            assert memoryTag >= MemoryTag.NATIVE_PATH;
            // Resolve the tracker's native {used, limit} block once and share it between the
            // pre-alloc check and the post-alloc record, instead of re-reading it through
            // getLimit()/getUsed()/nativeAddress().
            final long trackerBase = tracker.nativeAddress();
            checkAllocLimit(size, memoryTag);
            checkPerQueryAllocLimit(size, memoryTag, trackerBase, tracker);
            long ptr = UNSAFE.allocateMemory(size);
            recordMemAlloc(size, memoryTag);
            recordPerQueryMemAlloc(size, trackerBase);
            incrMallocCount();
            return ptr;
        } catch (OutOfMemoryError oom) {
            CairoException e = CairoException.nonCritical().setOutOfMemory(true)
                    .put("sun.misc.Unsafe.allocateMemory() OutOfMemoryError [workload=")
                    .put(tracker.getWorkload().name())
                    .put(", queryId=").put(tracker.getQueryId())
                    .put(", trackerUsed=").put(tracker.getUsed())
                    .put(", trackerLimit=").put(tracker.getLimit())
                    .put(", RSS_MEM_USED=").put(getRssMemUsed())
                    .put(", size=").put(size)
                    .put(", memoryTag=").put(memoryTag)
                    .put("], original message: ")
                    .put(oom.getMessage());
            System.err.println(e.getFlyweightMessage());
            throw e;
        }
    }

    public static long objectFieldOffset(Field f) {
        return UNSAFE.objectFieldOffset(f);
    }

    public static void putBoolean(Object o, long offset, boolean value) {
        UNSAFE.putBoolean(o, offset, value);
    }

    public static void putByte(long address, byte value) {
        UNSAFE.putByte(address, value);
    }

    public static void putChar(long address, char value) {
        UNSAFE.putChar(address, value);
    }

    public static void putDouble(long address, double value) {
        UNSAFE.putDouble(address, value);
    }

    public static void putFloat(long address, float value) {
        UNSAFE.putFloat(address, value);
    }

    public static void putInt(long address, int value) {
        UNSAFE.putInt(address, value);
    }

    public static void putInt(Object o, long offset, int value) {
        UNSAFE.putInt(o, offset, value);
    }

    public static void putLong(long address, long value) {
        UNSAFE.putLong(address, value);
    }

    public static void putLong(Object o, long offset, long value) {
        UNSAFE.putLong(o, offset, value);
    }

    public static void putLongVolatile(long address, long value) {
        UNSAFE.putLongVolatile(null, address, value);
    }

    public static void putObject(Object o, long offset, Object value) {
        UNSAFE.putObject(o, offset, value);
    }

    public static void putObjectVolatile(Object o, long offset, Object value) {
        UNSAFE.putObjectVolatile(o, offset, value);
    }

    public static void putOrderedInt(Object o, long offset, int value) {
        UNSAFE.putOrderedInt(o, offset, value);
    }

    public static void putOrderedLong(Object o, long offset, long value) {
        UNSAFE.putOrderedLong(o, offset, value);
    }

    public static void putShort(long address, short value) {
        UNSAFE.putShort(address, value);
    }

    public static long realloc(long address, long oldSize, long newSize, int memoryTag) {
        try {
            assert memoryTag >= MemoryTag.NATIVE_PATH;
            checkAllocLimit(-oldSize + newSize, memoryTag);
            long ptr = UNSAFE.reallocateMemory(address, newSize);
            recordMemAlloc(-oldSize + newSize, memoryTag);
            incrReallocCount();
            return ptr;
        } catch (OutOfMemoryError oom) {
            CairoException e = CairoException.nonCritical().setOutOfMemory(true)
                    .put("sun.misc.Unsafe.reallocateMemory() OutOfMemoryError [RSS_MEM_USED=")
                    .put(getRssMemUsed())
                    .put(", oldSize=")
                    .put(oldSize)
                    .put(", newSize=")
                    .put(newSize)
                    .put(", memoryTag=").put(memoryTag)
                    .put("], original message: ")
                    .put(oom.getMessage());
            System.err.println(e.getFlyweightMessage());
            throw e;
        }
    }

    /**
     * Tracker-aware variant with strong-exception safety. Both limit checks
     * run against the size delta before any reallocation. On breach the
     * overload throws and {@code address} continues to point at the
     * pre-realloc block; the caller still owns it. Counters are updated only
     * after both checks pass and {@link sun.misc.Unsafe#reallocateMemory}
     * succeeds, so a per-workload breach never has to roll back a global
     * counter update. A {@code null} tracker degrades to the global-only
     * {@link #realloc(long, long, long, int)} variant.
     */
    public static long realloc(long address, long oldSize, long newSize, int memoryTag, @Nullable MemoryTracker tracker) {
        if (tracker == null) {
            return realloc(address, oldSize, newSize, memoryTag);
        }
        try {
            assert memoryTag >= MemoryTag.NATIVE_PATH;
            final long delta = newSize - oldSize;
            // Resolve the tracker's native {used, limit} block once; see malloc() above.
            final long trackerBase = tracker.nativeAddress();
            checkAllocLimit(delta, memoryTag);
            checkPerQueryAllocLimit(delta, memoryTag, trackerBase, tracker);
            long ptr = UNSAFE.reallocateMemory(address, newSize);
            recordMemAlloc(delta, memoryTag);
            recordPerQueryMemAlloc(delta, trackerBase);
            incrReallocCount();
            return ptr;
        } catch (OutOfMemoryError oom) {
            CairoException e = CairoException.nonCritical().setOutOfMemory(true)
                    .put("sun.misc.Unsafe.reallocateMemory() OutOfMemoryError [workload=")
                    .put(tracker.getWorkload().name())
                    .put(", queryId=").put(tracker.getQueryId())
                    .put(", trackerUsed=").put(tracker.getUsed())
                    .put(", trackerLimit=").put(tracker.getLimit())
                    .put(", RSS_MEM_USED=").put(getRssMemUsed())
                    .put(", oldSize=").put(oldSize)
                    .put(", newSize=").put(newSize)
                    .put(", memoryTag=").put(memoryTag)
                    .put("], original message: ")
                    .put(oom.getMessage());
            System.err.println(e.getFlyweightMessage());
            throw e;
        }
    }

    public static void recordMemAlloc(long size, int memoryTag) {
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        COUNTERS[memoryTag].add(size);
        if (memoryTag >= MemoryTag.NATIVE_DEFAULT) {
            final long mem = UNSAFE.getAndAddLong(null, RSS_MEM_USED_ADDR, size) + size;
            assert mem >= 0 : "unexpected RSS mem: " + mem + ", size: " + size + ", memoryTag:" + memoryTag;
        } else {
            final long mem = UNSAFE.getAndAddLong(null, NON_RSS_MEM_USED_ADDR, size) + size;
            assert mem >= 0 : "unexpected non-RSS mem: " + mem + ", size: " + size + ", memoryTag:" + memoryTag;
        }
    }

    public static void setMemory(long address, long bytes, byte value) {
        UNSAFE.setMemory(address, bytes, value);
    }

    public static void setRssMemLimit(long limit) {
        UNSAFE.putLongVolatile(null, RSS_MEM_LIMIT_ADDR, limit);
    }

    public static void storeFence() {
        UNSAFE.storeFence();
    }

    private static void checkAllocLimit(long size, int memoryTag) {
        if (size <= 0) {
            return;
        }
        // Don't check limits for mmap'd memory
        final long rssMemLimit = getRssMemLimit();
        if (rssMemLimit > 0 && memoryTag >= NATIVE_DEFAULT) {
            long usage = getRssMemUsed();
            if (usage + size > rssMemLimit) {
                throw CairoException.nonCritical().setOutOfMemory(true)
                        .put("global RSS memory limit exceeded [usage=")
                        .put(usage)
                        .put(", RSS_MEM_LIMIT=").put(rssMemLimit)
                        .put(", size=").put(size)
                        .put(", memoryTag=").put(memoryTag)
                        .put(']');
            }
        }
    }

    private static void checkPerQueryAllocLimit(long size, int memoryTag, long trackerBase, MemoryTracker tracker) {
        if (size <= 0 || trackerBase == 0) {
            return;
        }
        final long limit = getLongVolatile(trackerBase + MEMORY_TRACKER_LIMIT_OFFSET);
        if (limit > 0) {
            final long used = getLongVolatile(trackerBase + MEMORY_TRACKER_USED_OFFSET);
            if (used + size > limit) {
                throw CairoException.nonCritical().setOutOfMemory(true)
                        .put("query memory limit exceeded [workload=").put(tracker.getWorkload().name())
                        .put(", queryId=").put(tracker.getQueryId())
                        .put(", limit=").put(limit)
                        .put(", used=").put(used)
                        .put(", size=").put(size)
                        .put(", memoryTag=").put(memoryTag)
                        .put(']');
            }
        }
    }

    /**
     * Allocate a new native allocator object and return its pointer
     */
    private static long constructNativeAllocator(long nativeMemCountersArray, int memoryTag, long memoryTrackerAddress) {
        // See `allocator.rs` for the definition of `QdbAllocator`. Layout:
        //   { mem_tracking: *const MemTracking, memory_tracker: *const MemoryTracker,
        //     tagged_used: *const AtomicUsize, memory_tag: i32 }
        // We construct here via `Unsafe` to avoid having initialization order issues with `Os.java`.
        // `memoryTrackerAddress == 0` means no per-workload tracker is attached.
        final long addr = UNSAFE.allocateMemory(QDB_ALLOCATOR_SIZE);
        Vect.memset(addr, QDB_ALLOCATOR_SIZE, 0);
        UNSAFE.putLong(addr, nativeMemCountersArray);
        UNSAFE.putLong(addr + 8, memoryTrackerAddress);
        UNSAFE.putLong(addr + 16, NATIVE_MEM_COUNTER_ADDRS[memoryTag]);
        UNSAFE.putInt(addr + 24, memoryTag);
        return addr;
    }

    // most significant bit
    private static int msb(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    // Updates the per-query counter on every tracked alloc/free, with no
    // limit > 0 gate (unlike checkPerQueryAllocLimit), so query_activity
    // reports live usage even for unlimited queries. See QueryRegistry.register().
    private static void recordPerQueryMemAlloc(long size, long trackerBase) {
        if (trackerBase == 0) {
            return;
        }
        final long usedAddr = trackerBase + MEMORY_TRACKER_USED_OFFSET;
        final long mem = UNSAFE.getAndAddLong(null, usedAddr, size) + size;
        assert mem >= 0 : "unexpected per-query mem: " + mem + ", size: " + size;
        if (mem < 0) {
            // Release-build safety net mirroring Rust's saturating_decrement (allocator.rs):
            // a malloc/free asymmetry must not leave the shared counter negative. Java reads
            // `used` as a signed long, so a negative value silently defeats the per-query
            // limit on this side, while Rust reads the same word as an unsigned usize (~2^64)
            // and spuriously fails every later allocation for the whole workload. Clamp at 0
            // so the asymmetry degrades to "limit ineffective" on both sides instead of
            // inverting it. The assert above still fires loudly under -ea, so a regression is
            // never silent in tests/CI; this only rescues release builds. The corrective add
            // leaves the same brief window for a concurrent reader to observe the negative
            // value as Rust's optimistic fetch_sub does.
            UNSAFE.getAndAddLong(null, usedAddr, -mem);
        }
    }

    /**
     * Builds a fresh {@code QdbAllocator} bound to {@code tracker} for the
     * given memory tag. Used by {@link MemoryTracker} to lazily back its
     * per-tag Rust allocator pointers.
     */
    static long constructTrackerNativeAllocator(MemoryTracker tracker, int memoryTag) {
        assert memoryTag >= NATIVE_DEFAULT;
        // The `MemTracking` struct starts at the same address as RSS_MEM_USED_ADDR;
        // see the layout comment in the static initializer.
        return constructNativeAllocator(RSS_MEM_USED_ADDR, memoryTag, tracker.nativeAddress());
    }

    /**
     * Symmetric counterpart to {@link #constructTrackerNativeAllocator}.
     */
    static void freeTrackerNativeAllocator(long addr) {
        if (addr != 0) {
            UNSAFE.freeMemory(addr);
        }
    }

    interface AnonymousClassDefiner {
        Class<?> define(Class<?> hostClass, byte[] data);
    }

    /**
     * Based on {@code MethodHandles.Lookup#defineHiddenClass}.
     */
    static class MethodHandlesClassDefiner implements AnonymousClassDefiner {
        private static Method defineMethod;
        private static Object hiddenClassOptions;
        private static Object lookupBase;
        private static long lookupOffset;

        @Nullable
        public static MethodHandlesClassDefiner newInstance() {
            if (defineMethod == null) {
                try {
                    Field trustedLookupField = MethodHandles.Lookup.class.getDeclaredField("IMPL_LOOKUP");
                    lookupBase = UNSAFE.staticFieldBase(trustedLookupField);
                    lookupOffset = UNSAFE.staticFieldOffset(trustedLookupField);
                    hiddenClassOptions = hiddenClassOptions("NESTMATE");
                    defineMethod = MethodHandles.Lookup.class
                            .getMethod("defineHiddenClass", byte[].class, boolean.class, hiddenClassOptions.getClass());
                } catch (ReflectiveOperationException e) {
                    return null;
                }
            }
            return new MethodHandlesClassDefiner();
        }

        @Override
        public Class<?> define(Class<?> hostClass, byte[] data) {
            try {
                MethodHandles.Lookup trustedLookup = (MethodHandles.Lookup) UNSAFE.getObject(lookupBase, lookupOffset);
                MethodHandles.Lookup definedLookup =
                        (MethodHandles.Lookup) defineMethod.invoke(trustedLookup.in(hostClass), data, false, hiddenClassOptions);
                return definedLookup.lookupClass();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        private static Object hiddenClassOptions(String... options) throws ClassNotFoundException {
            @SuppressWarnings("rawtypes")
            Class optionClass = Class.forName(MethodHandles.Lookup.class.getName() + "$ClassOption");
            Object classOptions = Array.newInstance(optionClass, options.length);
            for (int i = 0; i < options.length; i++) {
                Array.set(classOptions, i, Enum.valueOf(optionClass, options[i]));
            }
            return classOptions;
        }
    }

    /**
     * Based on {@code Unsafe#defineAnonymousClass}.
     */
    static class UnsafeClassDefiner implements AnonymousClassDefiner {

        private static Method defineMethod;

        @Nullable
        public static UnsafeClassDefiner newInstance() {
            if (defineMethod == null) {
                try {
                    defineMethod = sun.misc.Unsafe.class
                            .getMethod("defineAnonymousClass", Class.class, byte[].class, Object[].class);
                } catch (ReflectiveOperationException e) {
                    return null;
                }
            }
            return new UnsafeClassDefiner();
        }

        @Override
        public Class<?> define(Class<?> hostClass, byte[] data) {
            try {
                return (Class<?>) defineMethod.invoke(UNSAFE, hostClass, data, null);
            } catch (Exception e) {
                e.printStackTrace(System.out);
                return null;
            }
        }
    }

    static {
        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) theUnsafe.get(null);

            BYTE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            BYTE_SCALE = msb(UNSAFE.arrayIndexScale(byte[].class));

            INT_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
            INT_SCALE = msb(UNSAFE.arrayIndexScale(int[].class));

            LONG_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
            LONG_SCALE = msb(UNSAFE.arrayIndexScale(long[].class));

            AnonymousClassDefiner classDefiner = UnsafeClassDefiner.newInstance();
            if (classDefiner == null) {
                classDefiner = MethodHandlesClassDefiner.newInstance();
            }
            if (classDefiner == null) {
                throw new InstantiationException("failed to initialize class definer");
            }
            anonymousClassDefiner = classDefiner;
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }

        // A single allocation for all the off-heap native memory counters.
        // Might help with locality, given they're often incremented together.
        // All initial values set to 0.
        final long nativeMemCountersArraySize = (6 + COUNTERS.length) * 8;
        final long nativeMemCountersArray = UNSAFE.allocateMemory(nativeMemCountersArraySize);
        long ptr = nativeMemCountersArray;
        Vect.memset(nativeMemCountersArray, nativeMemCountersArraySize, 0);

        // N.B.: The layout here is also used in `allocator.rs` for the Rust side.
        // See: `struct MemTracking`.
        RSS_MEM_USED_ADDR = ptr;
        ptr += 8;
        RSS_MEM_LIMIT_ADDR = ptr;
        ptr += 8;
        MALLOC_COUNT_ADDR = ptr;
        ptr += 8;
        REALLOC_COUNT_ADDR = ptr;
        ptr += 8;
        FREE_COUNT_ADDR = ptr;
        ptr += 8;
        NON_RSS_MEM_USED_ADDR = ptr;
        ptr += 8;
        for (int i = 0; i < COUNTERS.length; i++) {
            COUNTERS[i] = new LongAdder();
            NATIVE_MEM_COUNTER_ADDRS[i] = ptr;
            ptr += 8;
        }
        for (int memoryTag = NATIVE_DEFAULT; memoryTag < MemoryTag.SIZE; ++memoryTag) {
            NATIVE_ALLOCATORS[memoryTag - NATIVE_DEFAULT] = constructNativeAllocator(
                    nativeMemCountersArray, memoryTag, 0L);
        }
    }
}
