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

package com.questdb.std;

import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.std.str.CharSink;
import com.questdb.txt.sink.StringSink;

import java.util.Arrays;


public class LongHashSet implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final long noEntryValue = -1L;
    private final double loadFactor;
    private final LongList list;
    private long[] keys;
    private int free;
    private int capacity;
    private int mask;

    public LongHashSet() {
        this(8);
    }

    public LongHashSet(int initialCapacity) {
        this(initialCapacity, 0.5f);
    }

    @SuppressWarnings("unchecked")
    private LongHashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.list = new LongList(initialCapacity);
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        this.loadFactor = loadFactor;
        keys = new long[capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity)];
        mask = keys.length - 1;
        free = this.capacity = initialCapacity;
        clear();
    }

    public boolean add(long key) {
        boolean r = insertKey(key);
        if (r) {
            list.add(key);
            if (free == 0) {
                rehash();
            }
        }
        return r;
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, noEntryValue);
        list.clear();
    }

    public boolean contains(long key) {
        int index = (int) (key & mask);
        return Unsafe.arrayGet(keys, index) != noEntryValue && (key == Unsafe.arrayGet(keys, index) || key == Unsafe.arrayGet(keys, index)) || probeContains(key, index);
    }

    public long get(int index) {
        return list.getQuick(index);
    }

    public void remove(long key) {
        if (list.remove(key)) {
            int index = (int) (key & mask);
            if (key == Unsafe.arrayGet(keys, index)) {
                Unsafe.arrayPut(keys, index, noEntryValue);
                free++;
            } else {
                probeRemove(key, index);
            }
        }
    }

    public int size() {
        return capacity - free;
    }

    @Override
    public String toString() {
        StringSink sink = new StringSink();
        toString(sink);
        return sink.toString();
    }

    private boolean insertKey(long key) {
        int index = (int) (key & mask);
        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            free--;
            return true;
        }
        return Unsafe.arrayGet(keys, index) != key && probeInsert(key, index);
    }

    private boolean probeContains(long key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return false;
            }

            if (key == Unsafe.arrayGet(keys, index)) {
                return true;
            }
        } while (true);
    }

    private boolean probeInsert(long key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                free--;
                return true;
            }

            if (key == Unsafe.arrayGet(keys, index)) {
                return false;
            }
        } while (true);
    }

    private void probeRemove(long key, int index) {
        int i = index;
        do {
            index = (index + 1) & mask;
            if (key == Unsafe.arrayGet(keys, index)) {
                Unsafe.arrayPut(keys, index, noEntryValue);
                free++;
                break;
            }
        } while (i != index);
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {
        int newCapacity = keys.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);

        long[] oldKeys = keys;
        this.keys = new long[newCapacity];
        Arrays.fill(keys, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryValue) {
                insertKey(Unsafe.arrayGet(oldKeys, i));
            }
        }
    }

    private void toString(CharSink sink) {
        sink.put('[');
        boolean needComma = false;
        for (int i = 0, n = keys.length; i < n; i++) {
            if (keys[i] != noEntryValue) {
                if (needComma) {
                    sink.put(',');
                }
                sink.put(keys[i]);

                if (!needComma) {
                    needComma = true;
                }
            }
        }

        sink.put(']');
    }
}
