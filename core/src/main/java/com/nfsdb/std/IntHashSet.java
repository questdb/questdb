/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

package com.nfsdb.std;

import com.nfsdb.io.sink.CharSink;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;

import java.util.Arrays;


public class IntHashSet implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final int noEntryValue = -1;
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
        this(initialCapacity, 0.5f);
    }

    @SuppressWarnings("unchecked")
    private IntHashSet(int initialCapacity, double loadFactor) {
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

    public boolean contains(int key) {
        int index = key & mask;
        return Unsafe.arrayGet(keys, index) != noEntryValue && (key == Unsafe.arrayGet(keys, index) || key == Unsafe.arrayGet(keys, index)) || probeContains(key, index);
    }

    public int get(int index) {
        return list.getQuick(index);
    }

    public void remove(int key) {
        if (list.remove(key)) {
            int index = key & mask;
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

    private boolean insertKey(int key) {
        int index = key & mask;
        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            free--;
            return true;
        }
        return Unsafe.arrayGet(keys, index) != key && probeInsert(key, index);
    }

    private boolean probeContains(int key, int index) {
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

    private boolean probeInsert(int key, int index) {
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

    private void probeRemove(int key, int index) {
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

        int[] oldKeys = keys;
        this.keys = new int[newCapacity];
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
