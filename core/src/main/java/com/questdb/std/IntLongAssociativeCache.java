/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.std;

public class IntLongAssociativeCache {

    public static final long NO_VALUE = -1L;
    public static final int UNUSED_KEY = 0;
    private static final int MIN_BLOCKS = 2;
    private static final int NOT_FOUND = -1;
    private static final int MINROWS = 16;
    private final int[] keys;
    private final long[] values;
    private final int rmask;
    private final int bmask;
    private final int blocks;
    private final int bshift;

    public IntLongAssociativeCache(int blocks, int rows) {
        this.blocks = Math.max(MIN_BLOCKS, Numbers.ceilPow2(blocks));
        rows = Math.max(MINROWS, Numbers.ceilPow2(rows));

        int size = rows * this.blocks;
        if (size < 0) {
            throw new OutOfMemoryError();
        }
        this.keys = new int[size];
        this.values = new long[size];
        this.rmask = rows - 1;
        this.bmask = this.blocks - 1;
        this.bshift = Numbers.msb(this.blocks);
    }

    public long peek(int key) {
        int index = getIndex(key);
        if (index == NOT_FOUND) {
            return NO_VALUE;
        }
        return Unsafe.arrayGet(values, index);
    }

    public long poll(int key) {
        int index = getIndex(key);
        if (index == NOT_FOUND) {
            return NO_VALUE;
        }
        long value = Unsafe.arrayGet(values, index);
//        Unsafe.arrayPut(values, index, 0);
        Unsafe.arrayPut(keys, index, UNUSED_KEY);
        return value;
    }

    public int put(int key, long value) {
        int lo = lo(key);
        int ok = Unsafe.arrayGet(keys, lo + bmask);
        System.arraycopy(keys, lo, keys, lo + 1, bmask);
        System.arraycopy(values, lo, values, lo + 1, bmask);
        Unsafe.arrayPut(keys, lo, key);
        Unsafe.arrayPut(values, lo, value);
        return ok;
    }

    private int getIndex(int key) {
        int lo = lo(key);
        for (int i = lo, hi = lo + blocks; i < hi; i++) {
            int k = Unsafe.arrayGet(keys, i);
            if (k == UNUSED_KEY) {
                return NOT_FOUND;
            }

            if (k == key) {
                return i;
            }
        }
        return NOT_FOUND;
    }

    private int lo(int key) {
        return (key & rmask) << bshift;
    }
}