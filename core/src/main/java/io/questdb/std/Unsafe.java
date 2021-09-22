/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class Unsafe {
    public static final long INT_OFFSET;
    public static final long INT_SCALE;
    public static final long LONG_OFFSET;
    public static final long LONG_SCALE;
    static final AtomicLong MEM_USED = new AtomicLong(0);
    private static final sun.misc.Unsafe UNSAFE;
    private static final AtomicLong MALLOC_COUNT = new AtomicLong(0);
    private static final AtomicLong FREE_COUNT = new AtomicLong(0);
    //#if jdk.version!=8
    private static final long OVERRIDE;
    private static final Method implAddExports;
    //#endif
    private static final LongAdder[] COUNTERS = new LongAdder[MemoryTag.SIZE];

    static {
        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) theUnsafe.get(null);

            INT_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(int[].class);
            INT_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(int[].class));

            LONG_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(long[].class);
            LONG_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(long[].class));
            //#if jdk.version!=8
            OVERRIDE = AccessibleObject_override_fieldOffset();
            implAddExports = Module.class.getDeclaredMethod("implAddExports", String.class, Module.class);
            //#endif
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

    public static void free(long ptr, long size, int memoryTag) {
        getUnsafe().freeMemory(ptr);
        FREE_COUNT.incrementAndGet();
        recordMemAlloc(-size, memoryTag);
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
        return FREE_COUNT.get();
    }

    public static long getMallocCount() {
        return MALLOC_COUNT.get();
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
        long ptr = getUnsafe().allocateMemory(size);
        recordMemAlloc(size, memoryTag);
        MALLOC_COUNT.incrementAndGet();
        return ptr;
    }

    public static long realloc(long address, long oldSize, long newSize, int memoryTag) {
        long ptr = getUnsafe().reallocateMemory(address, newSize);
        recordMemAlloc(-oldSize + newSize, memoryTag);
        return ptr;
    }

    public static void recordMemAlloc(long size, int memoryTag) {
        long mem = MEM_USED.addAndGet(size);
        assert mem >= 0;
        assert  memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        COUNTERS[memoryTag].add(size);
        assert COUNTERS[memoryTag].sum() >= 0;
    }

    private static int msb(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
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
}
