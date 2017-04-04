/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.misc;

import com.questdb.ex.FatalError;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

public final class Unsafe {
    public static final long CHAR_OFFSET;
    public static final long BYTE_OFFSET;
    public final static int CACHE_LINE_SIZE = 64;
    public static final long INT_OFFSET;
    public static final long INT_SCALE;
    public static final long LONG_OFFSET;
    public static final long LONG_SCALE;
    private static final sun.misc.Unsafe UNSAFE;
    private static final long OBJ_OFFSET;
    private static final long OBJ_SCALE;
    private static final long BOOL_OFFSET;
    private static final long BOOL_SCALE;
    private static final AtomicLong MEM_USED = new AtomicLong(0);
    private static final AtomicLong MALLOC_COUNT = new AtomicLong(0);
    private static final AtomicLong FREE_COUNT = new AtomicLong(0);

    private Unsafe() {
    }

    @SuppressWarnings("unchecked")
    public static <T> T arrayGet(T[] array, int index) {
        return (T) Unsafe.getUnsafe().getObject(array, OBJ_OFFSET + (index << OBJ_SCALE));
    }

    public static int arrayGet(int[] array, int index) {
        return Unsafe.getUnsafe().getInt(array, INT_OFFSET + (index << INT_SCALE));
    }

    public static boolean arrayGet(boolean[] array, int index) {
        return Unsafe.getUnsafe().getBoolean(array, BOOL_OFFSET + (index << BOOL_SCALE));
    }

    public static long arrayGet(long[] array, int index) {
        return Unsafe.getUnsafe().getLong(array, LONG_OFFSET + (index << LONG_SCALE));
    }

    public static long arrayGetVolatile(long[] array, int index) {
        return Unsafe.getUnsafe().getLongVolatile(array, LONG_OFFSET + (index << LONG_SCALE));
    }

    public static <T> void arrayPut(T[] array, int index, T obj) {
        Unsafe.getUnsafe().putObject(array, OBJ_OFFSET + (index << OBJ_SCALE), obj);
    }

    public static void arrayPut(int[] array, int index, int value) {
        Unsafe.getUnsafe().putInt(array, INT_OFFSET + (index << INT_SCALE), value);
    }

    public static void arrayPut(boolean[] array, int index, boolean value) {
        Unsafe.getUnsafe().putBoolean(array, BOOL_OFFSET + (index << BOOL_SCALE), value);
    }

    public static void arrayPut(long[] array, long index, long value) {
        Unsafe.getUnsafe().putLong(array, LONG_OFFSET + (index << LONG_SCALE), value);
    }

    public static void arrayPutOrdered(long[] array, long index, long value) {
        Unsafe.getUnsafe().putOrderedLong(array, LONG_OFFSET + (index << LONG_SCALE), value);
    }

    public static boolean cas(long[] array, int index, long expected, long value) {
        return Unsafe.getUnsafe().compareAndSwapLong(array, Unsafe.LONG_OFFSET + (((long) index) << Unsafe.LONG_SCALE), expected, value);
    }

    public static void free(long ptr, long size) {
        getUnsafe().freeMemory(ptr);
        FREE_COUNT.incrementAndGet();
        MEM_USED.addAndGet(-size);
    }

    public static boolean getBool(long address) {
        return UNSAFE.getByte(address) == 1;
    }

    public static long getFieldOffset(Class clazz, String name) throws NoSuchFieldException {
        Field f = clazz.getDeclaredField(name);
        f.setAccessible(true);
        return UNSAFE.objectFieldOffset(f);
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

    public static sun.misc.Unsafe getUnsafe() {
        return UNSAFE;
    }

    public static long malloc(long size) {
        long ptr = getUnsafe().allocateMemory(size);
        MEM_USED.addAndGet(size);
        MALLOC_COUNT.incrementAndGet();
        return ptr;
    }

    private static int msb(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    static {
        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) theUnsafe.get(null);
            OBJ_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(Object[].class);
            OBJ_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(Object[].class));

            INT_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(int[].class);
            INT_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(int[].class));

            LONG_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(long[].class);
            LONG_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(long[].class));

            CHAR_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(char[].class);
            BYTE_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(byte[].class);

            BOOL_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(boolean[].class);
            BOOL_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(boolean[].class));
        } catch (Exception e) {
            throw new FatalError(e);
        }
    }

}
