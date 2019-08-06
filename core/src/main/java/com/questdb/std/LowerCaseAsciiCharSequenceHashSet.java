/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

public class LowerCaseAsciiCharSequenceHashSet extends AbstractLowerCaseAsciiCharSequenceHashSet {

    private static final int MIN_INITIAL_CAPACITY = 16;

    public LowerCaseAsciiCharSequenceHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    private LowerCaseAsciiCharSequenceHashSet(int initialCapacity) {
        this(initialCapacity, 0.4);
    }

    private LowerCaseAsciiCharSequenceHashSet(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        clear();
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key immutable sequence of characters.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(CharSequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            return false;
        }

        addAt(index, key);
        return true;
    }

    public void addAt(int index, CharSequence key) {
        String s = key.toString().toLowerCase();
        Unsafe.arrayPut(keys, index, s);
        if (--free < 1) {
            rehash();
        }
    }

    public boolean contains(CharSequence key) {
        return keyIndex(key) < 0;
    }

    public CharSequence keyAt(int index) {
        return Unsafe.arrayGet(keys, -index - 1);
    }

    @Override
    protected void erase(int index) {
        Unsafe.arrayPut(keys, index, noEntryKey);
    }

    @Override
    protected void move(int from, int to) {
        Unsafe.arrayPut(keys, to, Unsafe.arrayGet(keys, from));
        erase(from);
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        final int size = size();
        mask = newCapacity - 1;
        free = capacity = newCapacity;
        int arrayCapacity = (int) (newCapacity / loadFactor);

        CharSequence[] newKeys = new CharSequence[arrayCapacity];
        CharSequence[] oldKeys = keys;
        this.keys = newKeys;
        free -= size;
        for (int i = 0, n = oldKeys.length; i < n; i++) {
            CharSequence key = Unsafe.arrayGet(oldKeys, i);
            if (key != null) {
                int keyIndex = keyIndex(key);
                Unsafe.arrayPut(keys, keyIndex, key);
            }
        }
    }
}