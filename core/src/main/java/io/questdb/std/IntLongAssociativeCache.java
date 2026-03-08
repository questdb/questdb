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

public class IntLongAssociativeCache {

    public static final long NO_VALUE = -1L;
    public static final int UNUSED_KEY = 0;
    private static final int MIN_BLOCKS = 1;
    private static final int MIN_ROWS = 1;
    private static final int NOT_FOUND = -1;
    private final int blocks;
    private final int bmask;
    private final int bshift;
    private final int[] keys;
    private final int rmask;
    private final long[] values;

    public IntLongAssociativeCache(int blocks, int rows) {
        this.blocks = Math.max(MIN_BLOCKS, Numbers.ceilPow2(blocks));
        rows = Math.max(MIN_ROWS, Numbers.ceilPow2(rows));

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
        return values[index];
    }

    public long poll(int key) {
        int index = getIndex(key);
        if (index == NOT_FOUND) {
            return NO_VALUE;
        }
        long value = values[index];
        keys[index] = UNUSED_KEY;
        return value;
    }

    public int put(int key, long value) {
        int lo = lo(key);
        int ok = keys[lo + bmask];
        System.arraycopy(keys, lo, keys, lo + 1, bmask);
        System.arraycopy(values, lo, values, lo + 1, bmask);
        keys[lo] = key;
        values[lo] = value;
        return ok;
    }

    private int getIndex(int key) {
        int lo = lo(key);
        for (int i = lo, hi = lo + blocks; i < hi; i++) {
            int k = keys[i];
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