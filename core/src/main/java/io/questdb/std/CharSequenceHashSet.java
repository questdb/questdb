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

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public class CharSequenceHashSet extends AbstractCharSequenceHashSet implements Sinkable {
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

    public CharSequenceHashSet(int initialCapacity, double loadFactor) {
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
    public boolean add(@Nullable CharSequence key) {
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

    /**
     * Adds a UTF-8 sequence to the hash set preserving key uniqueness.
     * <p>
     * For ASCII sequences, uses {@link Utf8Sequence#asAsciiCharSequence()} to avoid
     * String allocation. For non-ASCII sequences, converts to String via {@code toString()}.
     *
     * @param seq the UTF-8 sequence to add (can be a flyweight object)
     * @return false if the key is already in the set, true otherwise
     */
    public boolean add(@Nullable Utf8Sequence seq) {
        if (seq == null) {
            return addNull();
        }

        // Convert to String for storage and to compute UTF-16 compatible hash
        CharSequence charSequence;
        if (seq.isAscii()) {
            charSequence = seq.asAsciiCharSequence();
        } else {
            charSequence = seq.toString();
        }
        int index = keyIndex(charSequence);
        if (index < 0) {
            return false;
        }

        addAt(index, charSequence);
        return true;
    }

    public final void addAll(@NotNull CharSequenceHashSet that) {
        for (int i = 0, k = that.size(); i < k; i++) {
            add(that.get(i));
        }
    }

    public void addAt(int index, @NotNull CharSequence key) {
        final String s = Chars.toString(key);
        keys[index] = s;
        list.add(s);
        if (--free < 1) {
            rehash();
        }
    }

    public void addAtWithBorrowed(int index, @NotNull CharSequence key) {
        keys[index] = key;
        list.add(key);
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
        free = capacity;
        Arrays.fill(keys, null);
        list.clear();
        hasNull = false;
    }

    @Override
    public boolean contains(@Nullable CharSequence key) {
        return key == null ? hasNull : keyIndex(key) < 0;
    }

    @Override
    public boolean excludes(@Nullable CharSequence key) {
        return key == null ? !hasNull : keyIndex(key) > -1;
    }

    public CharSequence get(int index) {
        return list.getQuick(index);
    }

    public CharSequence getLast() {
        return list.getLast();
    }

    public ObjList<CharSequence> getList() {
        return list;
    }

    public int getListIndexAt(int keyIndex) {
        int index = -keyIndex - 1;
        return list.indexOf(keys[index]);
    }

    public int getListIndexOf(@NotNull CharSequence cs) {
        return getListIndexAt(keyIndex(cs));
    }

    @Override
    public CharSequence keyAt(int index) {
        int index1 = -index - 1;
        return keys[index1];
    }

    @Override
    public int remove(@Nullable CharSequence key) {
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
            int index1 = -index - 1;
            CharSequence key = keys[index1];
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
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        this.keys = new CharSequence[len];
        mask = len - 1;
        int n = list.size();
        free -= n;
        for (int i = 0; i < n; i++) {
            final CharSequence key = list.getQuick(i);
            keys[keyIndex(key)] = key;
        }
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        erase(from);
    }
}
