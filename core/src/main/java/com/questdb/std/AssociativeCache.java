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

import java.io.Closeable;

public class AssociativeCache<V> implements Closeable {

    private static final int MIN_BLOCKS = 2;
    private static final int NOT_FOUND = -1;
    private static final int MINROWS = 16;
    private final CharSequence[] keys;
    private final V[] values;
    private final int rmask;
    private final int bmask;
    private final int blocks;
    private final int bshift;

    @SuppressWarnings("unchecked")
    public AssociativeCache(int blocks, int rows) {
        this.blocks = Math.max(MIN_BLOCKS, Numbers.ceilPow2(blocks));
        rows = Math.max(MINROWS, Numbers.ceilPow2(rows));

        int size = rows * this.blocks;
        if (size < 0) {
            throw new OutOfMemoryError();
        }
        this.keys = new CharSequence[size];
        this.values = (V[]) new Object[size];
        this.rmask = rows - 1;
        this.bmask = this.blocks - 1;
        this.bshift = Numbers.msb(this.blocks);
    }

    @Override
    public void close() {
        clear();
    }

    public V peek(CharSequence key) {
        int index = getIndex(key);
        if (index == NOT_FOUND) {
            return null;
        }
        return Unsafe.arrayGet(values, index);
    }

    public V poll(CharSequence key) {
        int index = getIndex(key);
        if (index == NOT_FOUND) {
            return null;
        }
        V value = Unsafe.arrayGet(values, index);
        Unsafe.arrayPut(values, index, null);
        Unsafe.arrayPut(keys, index, null);
        return value;
    }

    public CharSequence put(CharSequence key, V value) {
        int lo = lo(key);
        CharSequence ok = Unsafe.arrayGet(keys, lo + bmask);
        if (ok != null) {
            free(lo + bmask);
        }
        System.arraycopy(keys, lo, keys, lo + 1, bmask);
        System.arraycopy(values, lo, values, lo + 1, bmask);
        Unsafe.arrayPut(keys, lo, key.toString());
        Unsafe.arrayPut(values, lo, value);
        return ok;
    }

    private void clear() {
        for (int i = 0, n = keys.length; i < n; i++) {
            if (keys[i] != null) {
                keys[i] = null;
                free(i);
            }
        }
    }

    private void free(int lo) {
        Unsafe.arrayPut(values, lo, Misc.free(Unsafe.arrayGet(values, lo)));
    }

    private int getIndex(CharSequence key) {
        int lo = lo(key);
        for (int i = lo, hi = lo + blocks; i < hi; i++) {
            CharSequence k = Unsafe.arrayGet(keys, i);
            if (k == null) {
                return NOT_FOUND;
            }

            if (Chars.equals(k, key)) {
                return i;
            }
        }
        return NOT_FOUND;
    }

    private int lo(CharSequence key) {
        return (Chars.hashCode(key) & rmask) << bshift;
    }
}