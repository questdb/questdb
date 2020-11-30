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

import java.util.Arrays;


public class CharSequenceHashSet extends AbstractCharSequenceHashSet {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private final ObjList<CharSequence> list;
    private boolean hasNull = false;

    public CharSequenceHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public CharSequenceHashSet(CharSequenceHashSet that) {
        this(that.capacity, that.loadFactor);
        addAll(that);
    }

    private CharSequenceHashSet(int initialCapacity) {
        this(initialCapacity, 0.4);
    }

    private CharSequenceHashSet(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        this.list = new ObjList<>(free);
        clear();
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key immutable sequence of characters.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(CharSequence key) {
        if (key == null) {
            return addNull();
        }

        int index = keyIndex(key);
        if (index < 0) {
            return false;
        }

        addAt(index, key);
        return true;
    }

    public final void addAll(CharSequenceHashSet that) {
        for (int i = 0, k = that.size(); i < k; i++) {
            add(that.get(i));
        }
    }

    public void addAt(int index, CharSequence key) {
        final String s = Chars.toString(key);
        keys[index] = s;
        list.add(s);
        if (--free < 1) {
            rehash();
        }
    }

    public boolean addNull() {
        if (hasNull) {
            return false;
        }
        --free;
        hasNull = true;
        list.add(null);
        return true;
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, null);
        list.clear();
        hasNull = false;
    }

    public boolean excludes(CharSequence key) {
        return key == null ? !hasNull : keyIndex(key) > -1;
    }

    public int remove(CharSequence key) {
        if (key == null) {
            return removeNull();
        }

        int keyIndex = keyIndex(key);
        if (keyIndex < 0) {
            removeAt(keyIndex);
            return -keyIndex - 1;
        }
        return -1;
    }

    public int getListIndexAt(int keyIndex) {
        int index = -keyIndex - 1;
        return list.indexOf(keys[index]);
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
    }

    public CharSequence keyAt(int index) {
        int index1 = -index - 1;
        return keys[index1];
    }

    public boolean contains(CharSequence key) {
        return key == null ? hasNull : keyIndex(key) < 0;
    }

    public CharSequence get(int index) {
        return list.getQuick(index);
    }

    public CharSequence getLast() {
        return list.getLast();
    }

    public void removeAt(int index) {
        if (index < 0) {
            int index1 = -index - 1;
            CharSequence key = keys[index1];
            super.removeAt(index);
            list.remove(key);
        }
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        erase(from);
    }

    public int removeNull() {
        if (hasNull) {
            hasNull = false;
            int index = list.remove(null);
            free++;
            return index;
        }
        return -1;
    }

    @Override
    public String toString() {
        return list.toString();
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        this.keys = new CharSequence[len];
        mask = len - 1;
        // todo: this is new instance, its already null initialized
        Arrays.fill(keys, null);
        int n = list.size();
        free -= n;
        for (int i = 0; i < n; i++) {
            final CharSequence key = list.getQuick(i);
            keys[keyIndex(key)] = key;
        }
    }
}