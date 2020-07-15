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
        if (index != NOT_FOUND) {
            return values[index];
        }
        return null;
    }

    public V poll(CharSequence key) {
        int index = getIndex(key);
        if (index == NOT_FOUND) {
            return null;
        }
        V value = values[index];
        values[index] = null;
        // do not null value to avoid creating another immutable key
        return value;
    }

    public CharSequence put(CharSequence key, V value) {
        final int lo = lo(key);
        final CharSequence outgoingKey = keys[lo + bmask];
        if (outgoingKey != null) {
            free(lo + bmask);
        }

        if (Chars.equalsNc(key, keys[lo])) {
            values[lo] = value;
        } else {
            System.arraycopy(keys, lo, keys, lo + 1, bmask);
            System.arraycopy(values, lo, values, lo + 1, bmask);
            if (value == null) {
                keys[lo] = null;
            } else {
                keys[lo] = Chars.toString(key);
            }
            values[lo] = value;
        }
        return outgoingKey;
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
        values[lo] = Misc.free(values[lo]);
    }

    private int getIndex(CharSequence key) {
        int lo = lo(key);
        for (int i = lo, hi = lo + blocks; i < hi; i++) {
            CharSequence k = keys[i];
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
        return (Hash.spread(Chars.hashCode(key)) & rmask) << bshift;
    }
}