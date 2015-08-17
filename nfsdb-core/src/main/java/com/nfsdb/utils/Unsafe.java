/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.utils;

import com.nfsdb.exceptions.JournalRuntimeException;

import java.lang.reflect.Field;

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


    private Unsafe() {
    }

    @SuppressWarnings("unchecked")
    public static <T> T arrayGet(T[] array, int index) {
        return (T) Unsafe.getUnsafe().getObject(array, OBJ_OFFSET + (index * OBJ_SCALE));
    }

    public static int arrayGet(int[] array, int index) {
        return Unsafe.getUnsafe().getInt(array, INT_OFFSET + (index * INT_SCALE));
    }

    public static boolean arrayGet(boolean[] array, int index) {
        return Unsafe.getUnsafe().getBoolean(array, BOOL_OFFSET + (index * BOOL_SCALE));
    }

    public static long arrayGet(long[] array, int index) {
        return Unsafe.getUnsafe().getLong(array, LONG_OFFSET + (index * LONG_SCALE));
    }

    public static <T> void arrayPut(T[] array, int index, T obj) {
        Unsafe.getUnsafe().putObject(array, OBJ_OFFSET + index * OBJ_SCALE, obj);
    }

    public static void arrayPut(int[] array, int index, int value) {
        Unsafe.getUnsafe().putInt(array, INT_OFFSET + index * INT_SCALE, value);
    }

    public static void arrayPut(boolean[] array, int index, boolean value) {
        Unsafe.getUnsafe().putBoolean(array, BOOL_OFFSET + index * BOOL_SCALE, value);
    }

    public static void arrayPut(long[] array, long index, long value) {
        Unsafe.getUnsafe().putLong(array, LONG_OFFSET + index * LONG_SCALE, value);
    }

    public static sun.misc.Unsafe getUnsafe() {
        return UNSAFE;
    }

    static {
        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) theUnsafe.get(null);
            OBJ_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(Object[].class);
            OBJ_SCALE = Unsafe.getUnsafe().arrayIndexScale(Object[].class);

            INT_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(int[].class);
            INT_SCALE = Unsafe.getUnsafe().arrayIndexScale(int[].class);

            LONG_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(long[].class);
            LONG_SCALE = Unsafe.getUnsafe().arrayIndexScale(long[].class);

            CHAR_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(char[].class);
            BYTE_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(byte[].class);

            BOOL_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(boolean[].class);
            BOOL_SCALE = Unsafe.getUnsafe().arrayIndexScale(boolean[].class);
        } catch (Exception e) {
            throw new JournalRuntimeException(e);
        }
    }
}
