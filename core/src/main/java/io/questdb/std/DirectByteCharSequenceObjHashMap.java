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
 * A copy implementation of CharSequenceObjHashMap. It is there to work with concrete classes
 * and avoid incurring performance penalty of megamorphic virtual calls. These calls originate
 * from calling charAt() on CharSequence interface. C2 compiler cannot inline these calls due to
 * multiple implementations of charAt(). It resorts to a virtual method call via itable. ILP is the
 * main victim of itable, suffering from non-deterministic performance loss. With this specific
 * implementation of the map C2 compiler seems to be able to inline chatAt() calls and itables are
 * no longer present in the async profiler.
 */
public class DirectByteCharSequenceObjHashMap<V> implements Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private final ObjList<String> list;
    private final double loadFactor;
    private int capacity;
    private int free;
    private String[] keys;
    private int mask;
    private V[] values;

    public DirectByteCharSequenceObjHashMap() {
        this(8);
    }

    public DirectByteCharSequenceObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    private DirectByteCharSequenceObjHashMap(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        free = this.capacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.loadFactor = loadFactor;
        int len = Numbers.ceilPow2((int) (this.capacity / loadFactor));
        keys = new String[len];
        mask = len - 1;
        list = new ObjList<>(capacity);
        values = (V[]) new Object[keys.length];
        clear();
    }

    @Override
    public void clear() {
        Arrays.fill(keys, null);
        free = this.capacity;
        list.clear();
    }

    public boolean contains(DirectByteCharSequence key) {
        return keyIndex(key) < 0;
    }

    public boolean excludes(DirectByteCharSequence key) {
        return keyIndex(key) > -1;
    }

    public V get(DirectByteCharSequence key) {
        return valueAt(keyIndex(key));
    }

    public V get(String key) {
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

    public ObjList<String> keys() {
        return list;
    }

    public boolean put(String key, V value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, String key, V value) {
        assert value != null;
        if (putAt0(index, key, value)) {
            list.add(key);
            return true;
        }
        return false;
    }

    public int remove(String key) {
        int index = keyIndex(key);
        if (index < 0) {
            removeAt(index);
            return -index - 1;
        }
        return -1;
    }

    public void removeAt(int index) {
        if (index < 0) {
            String key = keys[-index - 1];
            removeAt0(index);
            list.remove(key);
        }
    }

    public void removeAt0(int index) {
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
                    String key = keys[from];
                    key != null;
                    from = (from + 1) & mask, key = keys[from]
            ) {
                int idealHit = Hash.spread(Chars.hashCode(key)) & mask;
                if (idealHit != from) {
                    int to;
                    if (keys[idealHit] != null) {
                        to = probe(key, idealHit);
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

    public int size() {
        return capacity - free;
    }

    public V valueAt(int index) {
        return index < 0 ? valueAtQuick(index) : null;
    }

    public V valueAtQuick(int index) {
        return values[-index - 1];
    }

    public V valueQuick(int index) {
        return get(list.getQuick(index));
    }

    private int probe(DirectByteCharSequence key, int index) {
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

    private int probe(String key, int index) {
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

    private boolean putAt0(int index, String key, V value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        } else {
            keys[index] = key;
            values[index] = value;
            if (--free == 0) {
                rehash();
            }
            return true;
        }
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        V[] oldValues = values;
        String[] oldKeys = keys;
        this.keys = new String[len];
        this.values = (V[]) new Object[len];
        Arrays.fill(keys, null);
        mask = len - 1;

        free -= size;
        for (int i = oldKeys.length; i-- > 0; ) {
            String key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                values[index] = oldValues[i];
            }
        }
    }

    protected void erase(int index) {
        keys[index] = null;
        values[index] = null;
    }

    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }
}