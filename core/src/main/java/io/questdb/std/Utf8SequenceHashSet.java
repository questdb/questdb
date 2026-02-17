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

import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public class Utf8SequenceHashSet extends AbstractUtf8SequenceHashSet implements Sinkable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private final ObjList<Utf8Sequence> list;
    private boolean hasNull = false;

    public Utf8SequenceHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public Utf8SequenceHashSet(Utf8SequenceHashSet that) {
        this(that.capacity, that.loadFactor);
        addAll(that);
    }

    public Utf8SequenceHashSet(int initialCapacity) {
        this(initialCapacity, 0.4);
    }

    public Utf8SequenceHashSet(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        list = new ObjList<>(free);
        clear();
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key immutable sequence of characters.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(@Nullable Utf8Sequence key) {
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

    public final void addAll(@NotNull Utf8SequenceHashSet that) {
        for (int i = 0, k = that.size(); i < k; i++) {
            add(that.get(i));
        }
    }

    public void addAt(int index, @NotNull Utf8Sequence key) {
        final Utf8String s = Utf8s.toUtf8String(key);
        keys[index] = s;
        hashCodes[index] = Utf8s.hashCode(key);
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

    @Override
    public final void clear() {
        Arrays.fill(keys, null);
        free = capacity;
        list.clear();
        hasNull = false;
    }

    @Override
    public boolean contains(@Nullable Utf8Sequence key) {
        return key == null ? hasNull : keyIndex(key) < 0;
    }

    @Override
    public boolean excludes(@Nullable Utf8Sequence key) {
        return key == null ? !hasNull : keyIndex(key) > -1;
    }

    public Utf8Sequence get(int index) {
        return list.getQuick(index);
    }

    public Utf8Sequence getLast() {
        return list.getLast();
    }

    public ObjList<Utf8Sequence> getList() {
        return list;
    }

    @Override
    public Utf8Sequence keyAt(int index) {
        int index1 = -index - 1;
        return keys[index1];
    }

    @Override
    public int remove(@Nullable Utf8Sequence key) {
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

    @Override
    public void removeAt(int index) {
        if (index < 0) {
            Utf8Sequence key = keys[-index - 1];
            super.removeAt(index);
            list.remove(key);
        }
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
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(list);
    }

    @Override
    public String toString() {
        return list.toString();
    }

    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        Utf8Sequence[] oldKeys = keys;
        int[] oldHashCodes = hashCodes;
        this.keys = new Utf8Sequence[len];
        this.hashCodes = new int[len];
        Arrays.fill(keys, null);
        mask = len - 1;

        free -= size;
        for (int i = oldKeys.length; i-- > 0; ) {
            Utf8Sequence key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                hashCodes[index] = oldHashCodes[i];
            }
        }
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
        hashCodes[index] = 0;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        hashCodes[to] = hashCodes[from];
        erase(from);
    }
}
