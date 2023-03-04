/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.str.ByteCharSequence;
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
 * <p>
 * This map is optimized for ASCII and UTF8 DirectByteCharSequence lookups.
 */
public class ByteCharSequenceIntHashMap implements Mutable {

    public static final int NO_ENTRY_VALUE = -1;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private final int initialCapacity;
    private final double loadFactor;
    private final int noEntryValue;
    private int capacity;
    private int free;
    private int[] hashCodes;
    private ByteCharSequence[] keys;
    private int mask;
    private int[] values;

    public ByteCharSequenceIntHashMap() {
        this(8);
    }

    public ByteCharSequenceIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, NO_ENTRY_VALUE);
    }

    public ByteCharSequenceIntHashMap(int initialCapacity, double loadFactor, int noEntryValue) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        free = capacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.initialCapacity = capacity;
        this.loadFactor = loadFactor;
        int len = Numbers.ceilPow2((int) (capacity / loadFactor));
        keys = new ByteCharSequence[len];
        hashCodes = new int[len];
        mask = len - 1;
        this.noEntryValue = noEntryValue;
        values = new int[keys.length];
        clear();
    }

    public int capacity() {
        return capacity;
    }

    @Override
    public final void clear() {
        Arrays.fill(keys, null);
        Arrays.fill(hashCodes, 0);
        free = capacity;
        Arrays.fill(values, noEntryValue);
    }

    public boolean contains(DirectByteCharSequence key) {
        return keyIndex(key) < 0;
    }

    public boolean excludes(DirectByteCharSequence key) {
        return keyIndex(key) > -1;
    }

    public int get(DirectByteCharSequence key) {
        return valueAt(keyIndex(key));
    }

    public int get(ByteCharSequence key) {
        return valueAt(keyIndex(key));
    }

    public int keyIndex(DirectByteCharSequence key) {
        int hashCode = Hash.hashMem32(key);
        int index = hashCode & mask;

        if (keys[index] == null) {
            return index;
        }

        if (hashCode == hashCodes[index] && Chars.equals(key, keys[index])) {
            return -index - 1;
        }

        return probe(key, hashCode, index);
    }

    public int keyIndex(ByteCharSequence key) {
        int hashCode = Hash.hashMem32(key);
        int index = hashCode & mask;

        if (keys[index] == null) {
            return index;
        }

        if (hashCode == hashCodes[index] && Chars.equals(key, keys[index])) {
            return -index - 1;
        }

        return probe(key, hashCode, index);
    }

    public boolean put(ByteCharSequence key, int value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, ByteCharSequence key, int value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }

        putAt0(index, key, value);
        return true;
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
                    ByteCharSequence key = keys[from];
                    key != null;
                    from = (from + 1) & mask, key = keys[from]
            ) {
                int hashCode = Hash.hashMem32(key);
                int idealHit = hashCode & mask;
                if (idealHit != from) {
                    int to;
                    if (keys[idealHit] != null) {
                        to = probe(key, hashCode, idealHit);
                    } else {
                        to = idealHit;
                    }

                    if (to > -1) {
                        move(from, to);
                    }
                }
            }
        }
    }

    public void reset() {
        if (capacity == initialCapacity) {
            clear();
        } else {
            free = capacity = initialCapacity;
            int len = Numbers.ceilPow2((int) (capacity / loadFactor));
            keys = new ByteCharSequence[len];
            hashCodes = new int[len];
            mask = len - 1;
            values = new int[keys.length];
            clear();
        }
    }

    public int size() {
        return capacity - free;
    }

    public int valueAt(int index) {
        int index1 = -index - 1;
        return index < 0 ? values[index1] : noEntryValue;
    }

    private void erase(int index) {
        keys[index] = null;
        hashCodes[index] = 0;
        values[index] = noEntryValue;
    }

    private int hash(CharSequence k) {
        return Chars.hashCode(k) & mask;
    }

    private void move(int from, int to) {
        keys[to] = keys[from];
        hashCodes[to] = hashCodes[from];
        values[to] = values[from];
        erase(from);
    }

    private int probe(DirectByteCharSequence key, long hashCode, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == null) {
                return index;
            }
            if (hashCode == hashCodes[index] && Chars.equals(key, keys[index])) {
                return -index - 1;
            }
        } while (true);
    }

    private int probe(ByteCharSequence key, long hashCode, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == null) {
                return index;
            }
            if (hashCode == hashCodes[index] && Chars.equals(key, keys[index])) {
                return -index - 1;
            }
        } while (true);
    }

    private void putAt0(int index, ByteCharSequence key, int value) {
        keys[index] = key;
        hashCodes[index] = Hash.hashMem32(key);
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    private void rehash() {
        int[] oldValues = values;
        ByteCharSequence[] oldKeys = keys;
        int[] oldHashCodes = hashCodes;
        int size = capacity - free;
        capacity = capacity * 2;
        free = capacity - size;
        mask = Numbers.ceilPow2((int) (capacity / loadFactor)) - 1;
        keys = new ByteCharSequence[mask + 1];
        hashCodes = new int[mask + 1];
        values = new int[mask + 1];
        for (int i = oldKeys.length - 1; i > -1; i--) {
            ByteCharSequence key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                hashCodes[index] = oldHashCodes[i];
                values[index] = oldValues[i];
            }
        }
    }
}
