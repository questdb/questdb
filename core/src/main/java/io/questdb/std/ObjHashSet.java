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

import org.jetbrains.annotations.NotNull;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;

import static io.questdb.std.MapUtil.shouldMoveToFillGap;


public class ObjHashSet<T> extends AbstractSet<T> implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final Object noEntryKey = new Object();
    private final ObjList<T> list;
    private final double loadFactor;
    private int capacity;
    private int free;
    private T[] keys;
    private int mask;

    public ObjHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    public ObjHashSet(int initialCapacity) {
        this(initialCapacity, 0.4f, 0.3f);
    }

    @SuppressWarnings("unchecked")
    private ObjHashSet(int initialCapacity, double loadFactor, double hashFactor) {
        assert loadFactor > 0 && loadFactor < 1d;
        assert hashFactor > 0 && hashFactor < 1d;

        initialCapacity = (int) (initialCapacity * (1 + hashFactor));
        this.capacity = Math.max(initialCapacity, MIN_INITIAL_CAPACITY);
        this.loadFactor = loadFactor;
        keys = (T[]) new Object[Numbers.ceilPow2(capacity)];
        mask = keys.length - 1;
        this.list = new ObjList<>(free);
        clear();
    }

    public boolean add(T key) {
        return addAt(keyIndex(key), key);
    }

    public void addAll(ObjHashSet<? extends T> that) {
        for (int i = 0, n = that.size(); i < n; i++) {
            this.add(that.get(i));
        }
    }

    public void addAll(ObjList<? extends T> that) {
        for (int i = 0, n = that.size(); i < n; i++) {
            this.add(that.getQuick(i));
        }
    }

    public boolean addAt(int index, T key) {
        if (addAt0(index, key)) {
            list.add(key);
            return true;
        }
        return false;
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, noEntryKey);
        list.clear();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        return keyIndex((T) o) < 0;
    }

    public T get(int index) {
        return list.getQuick(index);
    }

    public ObjList<T> getList() {
        return list;
    }

    @Override
    @NotNull
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    public int keyIndex(T key) {
        int index = idx(key);

        T kv = keys[index];
        if (kv == noEntryKey) {
            return index;
        }

        if (kv == key || key.equals(kv)) {
            return -index - 1;
        }

        return probe(key, index);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object key) {
        int keyIndex = keyIndex((T) key);
        if (keyIndex < 0) {
            list.remove(keys[-keyIndex - 1]);
            removeAt(keyIndex);
            return true;
        }
        return false;
    }

    public int size() {
        return capacity - free;
    }

    @Override
    public String toString() {
        return list.toString();
    }

    private boolean addAt0(int index, T key) {
        if (index > -1) {
            keys[index] = key;
            if (--free == 0) {
                rehash();
            }
            return true;
        }
        return false;
    }

    private void erase(int index) {
        ((Object[]) keys)[index] = noEntryKey;
    }

    private int idx(T key) {
        return key == null ? 0 : (Hash.spread(key.hashCode()) & mask);
    }

    private void move(int from, int to) {
        keys[to] = keys[from];
        erase(from);
    }

    private int probe(T key, int index) {
        do {
            index = (index + 1) & mask;
            final T kv = keys[index];
            if (kv == noEntryKey) {
                return index;
            }
            if (kv == key || key.equals(kv)) {
                return -index - 1;
            }
        } while (true);
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {
        free = capacity = this.capacity * 2;
        T[] oldKeys = keys;
        this.keys = (T[]) new Object[Numbers.ceilPow2((int) (this.capacity / loadFactor))];
        Arrays.fill(keys, noEntryKey);
        mask = keys.length - 1;

        for (int i = oldKeys.length; i-- > 0; ) {
            T key = oldKeys[i];
            if (key != noEntryKey) {
                addAt0(keyIndex(key), key);
            }
        }
    }

    private void removeAt(int index) {
        if (index < 0) {
            int from = -index - 1;
            erase(from);
            free++;
            compactProbeSequence(from);
        }
    }

    /**
     * When a slot is freed, we examine the non-empty entries that follow it.
     * Some of them may have originally hashed to this slot but were displaced
     * because it was occupied. Once the slot becomes free, such entries
     * may need to be moved backward to preserve correct lookup semantics.
     */
    private void compactProbeSequence(int deletedPosition) {
        int gapPos = deletedPosition;
        int scanPos = (gapPos + 1) & mask;

        // Scan forward until we hit an empty slot (end of probe sequence)
        for (T key = keys[scanPos];
             key != noEntryKey;
             scanPos = (scanPos + 1) & mask, key = keys[scanPos]) {

            long idealPos = Hash.spread(key.hashCode()) & mask;

            if (shouldMoveToFillGap(scanPos, idealPos, gapPos)) {
                move(scanPos, gapPos);
                gapPos = scanPos;
            }
        }
    }
}
