/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.str.DirectByteCharSequence;

import java.util.Arrays;

/**
 * A copy implementation of CharSequenceIntHashMap. It is there to work with concrete classes
 * and avoid incurring performance penalty of megamorphic virtual calls. These calls originate
 * from calling charAt() on CharSequence interface. C2 compiler cannot inline these calls due to
 * multiple implementations of charAt(). It resorts to a virtual method call via itable. ILP is the
 * main victim of itable, suffering from non-deterministic performance loss. With this specific
 * implementation of the map C2 compiler seems to be able to inline chatAt() calls and itables are
 * no longer present in the async profiler.
 */
public class DirectByteCharSequenceIntHashMap implements Mutable {
    public static final int NO_ENTRY_VALUE = -1;
    protected static final int MIN_INITIAL_CAPACITY = 16;
    protected final double loadFactor;
    private final int noEntryValue;
    private final ObjList<String> list;
    protected String[] keys;
    protected int mask;
    protected int free;
    protected int capacity;
    private int[] values;

    public DirectByteCharSequenceIntHashMap() {
        this(8);
    }

    public DirectByteCharSequenceIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, NO_ENTRY_VALUE);
    }

    public DirectByteCharSequenceIntHashMap(int initialCapacity, double loadFactor, int noEntryValue) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        this.free = this.capacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.loadFactor = loadFactor;
        int len = Numbers.ceilPow2((int) (this.capacity / loadFactor));
        this.keys = new String[len];
        this.mask = len - 1;
        this.noEntryValue = noEntryValue;
        this.list = new ObjList<>(capacity);
        values = new int[keys.length];
        clear();
    }

    @Override
    public final void clear() {
        Arrays.fill(keys, null);
        free = this.capacity;
        list.clear();
        Arrays.fill(values, noEntryValue);
    }

    public boolean contains(DirectByteCharSequence key) {
        return keyIndex(key) < 0;
    }

    public boolean excludes(DirectByteCharSequence key) {
        return keyIndex(key) > -1;
    }

    public boolean excludes(DirectByteCharSequence key, int lo, int hi) {
        return keyIndex(key, lo, hi) > -1;
    }

    public int get(DirectByteCharSequence key) {
        return valueAt(keyIndex(key));
    }

    public int get(String key) {
        return valueAt(keyIndex(key));
    }

    public int keyIndex(DirectByteCharSequence key) {
        int index = Hash.spread(Chars.hashCode(key)) & mask;

        if (keys[index] == null) {
            return index;
        }

        if (Chars.equals(key, keys[index])) {
            return -index - 1;
        }

        return probe(key, index);
    }

    public int keyIndex(String key) {
        int index = Hash.spread(Chars.hashCode(key)) & mask;

        if (keys[index] == null) {
            return index;
        }

        if (Chars.equals(key, keys[index])) {
            return -index - 1;
        }

        return probe(key, index);
    }

    public int keyIndex(DirectByteCharSequence key, int lo, int hi) {
        int index = Hash.spread(Chars.hashCode(key, lo, hi)) & mask;

        if (keys[index] == null) {
            return index;
        }
        CharSequence cs = keys[index];
        if (Chars.equals(key, lo, hi, cs, 0, cs.length())) {
            return -index - 1;
        }
        return probe(key, lo, hi, index);
    }

    public int remove(DirectByteCharSequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            removeAt(index);
            return -index - 1;
        }
        return -1;
    }

    public void removeAt(int index) {
        if (index < 0) {
            int index1 = -index - 1;
            CharSequence key = keys[index1];
            int from = -index - 1;
            erase(from);
            free++;

            // after we have freed up a slot
            // consider non-empty keys directly below
            // they may have been a direct hit but because
            // directly hit slot wasn't empty these keys would
            // have moved.
            //
            // After slot if freed these keys require re-hash
            from = (from + 1) & mask;
            for (
                    CharSequence k = keys[from];
                    k != null;
                    from = (from + 1) & mask, k = keys[from]
            ) {
                int idealHit = Hash.spread(Chars.hashCode(k)) & mask;
                if (idealHit != from) {
                    int to;
                    if (keys[idealHit] != null) {
                        to = probe(k, idealHit);
                    } else {
                        to = idealHit;
                    }

                    if (to > -1) {
                        move(from, to);
                    }
                }
            }
            list.remove(key);
        }
    }

    public int size() {
        return capacity - free;
    }

    private void erase(int index) {
        keys[index] = null;
        values[index] = noEntryValue;
    }

    private void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }

    public ObjList<String> keys() {
        return list;
    }

    public boolean put(String key, int value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, String key, int value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }

        putAt0(index, key, value);
        list.add(key);
        return true;
    }

    public int valueAt(int index) {
        int index1 = -index - 1;
        return index < 0 ? values[index1] : noEntryValue;
    }

    public int valueQuick(int index) {
        return valueAt(keyIndex(list.getQuick(index)));
    }

    private int probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == null) {
                return index;
            }
            if (Chars.equals(key, keys[index])) {
                return -index - 1;
            }
        } while (true);
    }

    private int probe(CharSequence key, int lo, int hi, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == null) {
                return index;
            }
            CharSequence cs = keys[index];
            if (Chars.equals(key, lo, hi, cs, 0, cs.length())) {
                return -index - 1;
            }
        } while (true);
    }

    private void putAt0(int index, String key, int value) {
        keys[index] = key;
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    private void rehash() {
        int[] oldValues = values;
        String[] oldKeys = keys;
        int size = capacity - free;
        capacity = capacity * 2;
        free = capacity - size;
        mask = Numbers.ceilPow2((int) (capacity / loadFactor)) - 1;
        this.keys = new String[mask + 1];
        this.values = new int[mask + 1];
        for (int i = oldKeys.length - 1; i > -1; i--) {
            String key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                values[index] = oldValues[i];
            }
        }
    }
}