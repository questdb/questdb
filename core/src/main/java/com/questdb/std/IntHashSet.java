/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

import java.util.Arrays;


public class IntHashSet implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private final int noEntryValue;
    private final double loadFactor;
    private final IntList list;
    private int[] keys;
    private int free;
    private int capacity;
    private int mask;

    public IntHashSet() {
        this(8);
    }

    public IntHashSet(int initialCapacity) {
        this(initialCapacity, 0.5f, -1);
    }

    @SuppressWarnings("unchecked")
    public IntHashSet(int initialCapacity, double loadFactor, int noEntryValue) {
        this.noEntryValue = noEntryValue;
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.list = new IntList(initialCapacity);
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        this.loadFactor = loadFactor;
        keys = new int[capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity)];
        mask = keys.length - 1;
        free = this.capacity = initialCapacity;
        clear();
    }

    public boolean add(int key) {
        int index = keyIndex(key);
        if (index < 0) {
            return false;
        }
        addAt(index, key);
        return true;
    }

    public void addAt(int index, int key) {
        Unsafe.arrayPut(keys, index, key);
        list.add(key);
        if (--free == 0) {
            rehash();
        }
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, noEntryValue);
        list.clear();
    }

    public boolean contains(int key) {
        return keyIndex(key) < 0;
    }

    public boolean excludes(int key) {
        return keyIndex(key) > -1;
    }

    public int get(int index) {
        return list.getQuick(index);
    }

    public int keyIndex(int key) {
        int index = key & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            return index;
        }

        if (Unsafe.arrayGet(keys, index) == key) {
            return -index - 1;
        }

        return probe0(key, index);
    }

    public void remove(int key) {
        int index = keyIndex(key);
        if (index < 0) {
            assert list.remove(key);
            Unsafe.arrayPut(keys, -index - 1, noEntryValue);
            free++;
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

    private int probe0(int key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return index;
            }

            if (key == Unsafe.arrayGet(keys, index)) {
                return -index - 1;
            }
        } while (true);
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {
        int newCapacity = keys.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);

        int[] oldKeys = keys;
        this.keys = new int[newCapacity];
        Arrays.fill(keys, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryValue) {
                add(Unsafe.arrayGet(oldKeys, i));
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
