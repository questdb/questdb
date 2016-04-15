/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.misc;

import com.nfsdb.ex.FatalError;

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

    public static boolean getBool(long address) {
        return UNSAFE.getByte(address) == 1;
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
            throw new FatalError(e);
        }
    }
}
