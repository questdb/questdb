/*******************************************************************************
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

// @formatter:off
import io.questdb.cairo.CairoException;
import org.jetbrains.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.LongAdder;

import static io.questdb.std.MemoryTag.NATIVE_DEFAULT;

public final class Unsafe {
    // The various _ADDR fields are `long` in Java, but they are `* mut usize` in Rust, or `size_t*` in C.
    // These are off-heap allocated atomic counters for memory usage tracking.

    public static final long BYTE_OFFSET;
    public static final long BYTE_SCALE;
    public static final long INT_OFFSET;
    public static final long INT_SCALE;
    public static final Module JAVA_BASE_MODULE = System.class.getModule();
    public static final long LONG_OFFSET;
    public static final long LONG_SCALE;
    private static final LongAdder[] COUNTERS = new LongAdder[MemoryTag.SIZE];
    private static final long FREE_COUNT_ADDR;
    private static final long MALLOC_COUNT_ADDR;
    private static final long[] NATIVE_ALLOCATORS = new long[MemoryTag.SIZE - NATIVE_DEFAULT];
    private static final long[] NATIVE_MEM_COUNTER_ADDRS = new long[MemoryTag.SIZE];
    private static final long NON_RSS_MEM_USED_ADDR;
    private static final long OVERRIDE;
    private static final long REALLOC_COUNT_ADDR;
    private static final long RSS_MEM_LIMIT_ADDR;
    private static final long RSS_MEM_USED_ADDR;
    private static final sun.misc.Unsafe UNSAFE;
    private static final AnonymousClassDefiner anonymousClassDefiner;
    private static final Method implAddExports;

    private Unsafe() {
    }

    public static void addExports(Module from, Module to, String packageName) {
        try {
            implAddExports.invoke(from, packageName, to);
        } catch (ReflectiveOperationException e) {
            e.printStackTrace(System.out);
        }
    }

    public static long arrayGetVolatile(long[] array, int index) {
        assert index > -1 && index < array.length;
        return Unsafe.getUnsafe().getLongVolatile(array, LONG_OFFSET + ((long) index << LONG_SCALE));
    }

    public static int arrayGetVolatile(int[] array, int index) {
        assert index > -1 && index < array.length;
        return Unsafe.getUnsafe().getIntVolatile(array, INT_OFFSET + ((long) index << INT_SCALE));
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
        Unsafe.getUnsafe().putOrderedLong(array, LONG_OFFSET + ((long) index << LONG_SCALE), value);
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
        Unsafe.getUnsafe().putOrderedInt(array, INT_OFFSET + ((long) index << INT_SCALE), value);
    }

    public static int byteArrayGetInt(byte[] array, int index) {
        assert index > -1 && index < array.length - 3;
        return Unsafe.getUnsafe().getInt(array, BYTE_OFFSET + index);
    }

    public static long byteArrayGetLong(byte[] array, int index) {
        assert index > -1 && index < array.length - 7;
        return Unsafe.getUnsafe().getLong(array, BYTE_OFFSET + index);
    }

    public static short byteArrayGetShort(byte[] array, int index) {
        assert index > -1 && index < array.length - 1;
        return Unsafe.getUnsafe().getShort(array, BYTE_OFFSET + index);
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

    public static boolean cas(long[] array, int index, long expected, long value) {
        assert index > -1 && index < array.length;
        return Unsafe.cas(array, Unsafe.LONG_OFFSET + (((long) index) << Unsafe.LONG_SCALE), expected, value);
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
            Unsafe.getUnsafe().freeMemory(ptr);
            incrFreeCount();
            recordMemAlloc(-size, memoryTag);
        }
        return 0;
    }

    public static boolean getBool(long address) {
        return UNSAFE.getByte(address) == 1;
    }

    public static long getFieldOffset(Class<?> clazz, String name) {
        try {
            return UNSAFE.objectFieldOffset(clazz.getDeclaredField(name));
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static long getFreeCount() {
        return UNSAFE.getLongVolatile(null, FREE_COUNT_ADDR);
    }

    public static long getMallocCount() {
        return UNSAFE.getLongVolatile(null, MALLOC_COUNT_ADDR);
    }

    /**
     * Get the total memory used by the process, this includes both resident memory
     * and that assigned to memory mapped files.
     */
    public static long getMemUsed() {
        return UNSAFE.getLongVolatile(null, NON_RSS_MEM_USED_ADDR) +
                UNSAFE.getLongVolatile(null, RSS_MEM_USED_ADDR);
    }

    public static long getMemUsedByTag(int memoryTag) {
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        return COUNTERS[memoryTag].sum() + UNSAFE.getLongVolatile(null, NATIVE_MEM_COUNTER_ADDRS[memoryTag]);
    }

    /** Returns a `*const QdbAllocator` for use in Rust. */
    public static long getNativeAllocator(int memoryTag) {
        return NATIVE_ALLOCATORS[memoryTag - NATIVE_DEFAULT];
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

    /**
     * Equivalent to {@link AccessibleObject#setAccessible(boolean) AccessibleObject.setAccessible(true)}, except that
     * it does not produce an illegal access error or warning.
     *
     * @param accessibleObject the instance to make accessible
     */
    public static void makeAccessible(AccessibleObject accessibleObject) {
        UNSAFE.putBooleanVolatile(accessibleObject, OVERRIDE, true);
    }

    public static long malloc(long size, int memoryTag) {
        try {
            assert memoryTag >= MemoryTag.NATIVE_PATH;
            checkAllocLimit(size, memoryTag);
            long ptr = Unsafe.getUnsafe().allocateMemory(size);
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

    public static long realloc(long address, long oldSize, long newSize, int memoryTag) {
        try {
            assert memoryTag >= MemoryTag.NATIVE_PATH;
            checkAllocLimit(-oldSize + newSize, memoryTag);
            long ptr = Unsafe.getUnsafe().reallocateMemory(address, newSize);
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

    public static void setRssMemLimit(long limit) {
        UNSAFE.putLongVolatile(null, RSS_MEM_LIMIT_ADDR, limit);
    }

    private static long AccessibleObject_override_fieldOffset() {
        if (isJava8Or11()) {
            return getFieldOffset(AccessibleObject.class, "override");
        }
        // From Java 12 onwards, AccessibleObject#override is protected and cannot be accessed reflectively.
        boolean is32BitJVM = is32BitJVM();
        if (is32BitJVM) {
            return 8L;
        }
        if (getOrdinaryObjectPointersCompressionStatus(is32BitJVM)) {
            return 12L;
        }
        return 16L;
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

    /** Allocate a new native allocator object and return its pointer */
    private static long constructNativeAllocator(long nativeMemCountersArray, int memoryTag) {
        // See `allocator.rs` for the definition of `QdbAllocator`.
        // We construct here via `Unsafe` to avoid having initialization order issues with `Os.java`.
        final long allocSize = 8 + 8 + 4;  // two longs, one int
        final long addr = UNSAFE.allocateMemory(allocSize);
        Vect.memset(addr, allocSize, 0);
        UNSAFE.putLong(addr, nativeMemCountersArray);
        UNSAFE.putLong(addr + 8, NATIVE_MEM_COUNTER_ADDRS[memoryTag]);
        UNSAFE.putInt(addr + 16, memoryTag);
        return addr;
    }

    private static boolean getOrdinaryObjectPointersCompressionStatus(boolean is32BitJVM) {
        class Probe {
            @SuppressWarnings("unused")
            private int intField; // Accessed through reflection

            boolean probe() {
                long offset = getFieldOffset(Probe.class, "intField");
                if (offset == 8L) {
                    assert is32BitJVM;
                    return false;
                }
                if (offset == 12L) {
                    return true;
                }
                if (offset == 16L) {
                    return false;
                }
                throw new AssertionError(offset);
            }
        }
        return new Probe().probe();
    }

    private static boolean is32BitJVM() {
        String sunArchDataModel = System.getProperty("sun.arch.data.model");
        return sunArchDataModel.equals("32");
    }

    private static boolean isJava8Or11() {
        String javaVersion = System.getProperty("java.version");
        return javaVersion.startsWith("11") || javaVersion.startsWith("1.8");
    }

    // most significant bit
    private static int msb(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
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

            BYTE_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(byte[].class);
            BYTE_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(byte[].class));

            INT_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(int[].class);
            INT_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(int[].class));

            LONG_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(long[].class);
            LONG_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(long[].class));

            OVERRIDE = AccessibleObject_override_fieldOffset();
            implAddExports = Module.class.getDeclaredMethod("implAddExports", String.class, Module.class);

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
        makeAccessible(implAddExports);

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
                    nativeMemCountersArray, memoryTag);
        }
    }
}
