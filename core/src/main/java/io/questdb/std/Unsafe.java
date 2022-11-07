/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import org.jetbrains.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class Unsafe {
    public static final long BYTE_OFFSET;
    public static final long BYTE_SCALE;
    public static final long INT_OFFSET;
    public static final long INT_SCALE;
    public static final long LONG_OFFSET;
    public static final long LONG_SCALE;
    static final AtomicLong MEM_USED = new AtomicLong(0);
    private static final sun.misc.Unsafe UNSAFE;
    private static final AtomicLong MALLOC_COUNT = new AtomicLong(0);
    private static final AtomicLong REALLOC_COUNT = new AtomicLong(0);
    private static final AtomicLong FREE_COUNT = new AtomicLong(0);
    //#if jdk.version!=8
    private static final long OVERRIDE;
    private static final Method implAddExports;
    //#endif
    private static final AnonymousClassDefiner anonymousClassDefiner;
    private static final LongAdder[] COUNTERS = new LongAdder[MemoryTag.SIZE];

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

            //#if jdk.version!=8
            OVERRIDE = AccessibleObject_override_fieldOffset();
            implAddExports = Module.class.getDeclaredMethod("implAddExports", String.class, Module.class);
            //#endif

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
        //#if jdk.version!=8
        makeAccessible(implAddExports);
        //#endif

        for (int i = 0; i < COUNTERS.length; i++) {
           COUNTERS[i] = new LongAdder();
        }
    }

    //#if jdk.version!=8
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

    private static boolean isJava8Or11() {
        String javaVersion = System.getProperty("java.version");
        return javaVersion.startsWith("11") || javaVersion.startsWith("1.8");
    }

    private static boolean is32BitJVM() {
        String sunArchDataModel = System.getProperty("sun.arch.data.model");
        return sunArchDataModel.equals("32");
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
    //#endif

    private Unsafe() {
    }

    public static long arrayGetVolatile(long[] array, int index) {
        assert index > -1 && index < array.length;
        return Unsafe.getUnsafe().getLongVolatile(array, LONG_OFFSET + ((long) index << LONG_SCALE));
    }

    public static void arrayPutOrdered(long[] array, int index, long value) {
        assert index > -1 && index < array.length;
        Unsafe.getUnsafe().putOrderedLong(array, LONG_OFFSET + ((long) index << LONG_SCALE), value);
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

    public static long free(long ptr, long size, int memoryTag) {
        if (ptr != 0) {
            getUnsafe().freeMemory(ptr);
            FREE_COUNT.incrementAndGet();
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

    public static long swapEndianness(long value) {
        long b0 = value & 0xff;
        long b1 = (value >> 8) & 0xff;
        long b2 = (value >> 16) & 0xff;
        long b3 = (value >> 24) & 0xff;
        long b4 = (value >> 32) & 0xff;
        long b5 = (value >> 40) & 0xff;
        long b6 = (value >> 48) & 0xff;
        long b7 = (value >> 56) & 0xff;
        return (b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) | (b4 << 24) | (b5 << 16) | (b6 << 8) | b7;
    }

    public static long getFreeCount() {
        return FREE_COUNT.get();
    }

    public static long getMallocCount() {
        return MALLOC_COUNT.get();
    }

    public static long getReallocCount() {
        return REALLOC_COUNT.get();
    }

    public static long getMemUsed() {
        return MEM_USED.get();
    }

    public static long getMemUsedByTag(int memoryTag) {
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        return COUNTERS[memoryTag].sum();
    }

    public static sun.misc.Unsafe getUnsafe() {
        return UNSAFE;
    }

    public static long malloc(long size, int memoryTag) {
        try {
            long ptr = getUnsafe().allocateMemory(size);
            recordMemAlloc(size, memoryTag);
            MALLOC_COUNT.incrementAndGet();
            return ptr;
        } catch (OutOfMemoryError oom) {
            System.err.printf(
                    "Unsafe.malloc() OutOfMemoryError, mem_used=%d, size=%d, memoryTag=%d",
                    MEM_USED.get(), size, memoryTag);
            throw oom;
        }
    }

    public static long realloc(long address, long oldSize, long newSize, int memoryTag) {
        try {
            long ptr = getUnsafe().reallocateMemory(address, newSize);
            recordMemAlloc(-oldSize + newSize, memoryTag);
            REALLOC_COUNT.incrementAndGet();
            return ptr;
        } catch (OutOfMemoryError oom) {
            System.err.printf(
                    "Unsafe.realloc() OutOfMemoryError, mem_used=%d, old_size=%d, new_size=%d, memoryTag=%d",
                    MEM_USED.get(), oldSize, newSize, memoryTag);
            throw oom;
        }
    }

    public static void recordMemAlloc(long size, int memoryTag) {
        long mem = MEM_USED.addAndGet(size);
        assert mem >= 0;
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        COUNTERS[memoryTag].add(size);
    }

    //most significant bit
    private static int msb(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    /**
     * Defines a class but does not make it known to the class loader or system dictionary.
     *
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

    //#if jdk.version!=8
    /**
     * Equivalent to {@link AccessibleObject#setAccessible(boolean) AccessibleObject.setAccessible(true)}, except that
     * it does not produce an illegal access error or warning.
     *
     * @param accessibleObject the instance to make accessible
     */
    public static void makeAccessible(AccessibleObject accessibleObject) {
        UNSAFE.putBooleanVolatile(accessibleObject, OVERRIDE, true);
    }

    public static void addExports(Module from, Module to, String packageName) {
        try {
            implAddExports.invoke(from, packageName, to);
        } catch (ReflectiveOperationException e) {
            e.printStackTrace();
        }
    }

    public static final Module JAVA_BASE_MODULE = System.class.getModule();
    //#endif

    interface AnonymousClassDefiner {
        Class<?> define(Class<?> hostClass, byte[] data);
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
                e.printStackTrace();
                return null;
            }
        }
    }

    /**
     * Based on {@code MethodHandles.Lookup#defineHiddenClass}.
     */
    static class MethodHandlesClassDefiner implements AnonymousClassDefiner {

        private static Object lookupBase;
        private static long lookupOffset;
        private static Object hiddenClassOptions;
        private static Method defineMethod;

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
                e.printStackTrace();
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
}
