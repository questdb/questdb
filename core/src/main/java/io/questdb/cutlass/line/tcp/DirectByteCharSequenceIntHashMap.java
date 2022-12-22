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

package io.questdb.cutlass.line.tcp;

import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
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
 * Important note:
 * This map is optimized for ASCII and UTF8 DirectByteCharSequence lookups. Lookups of UTF16
 * strings (j.l.String) with non-ASCII chars will not work correctly, so make sure to re-encode
 * the string in UTF8.
 */
public class DirectByteCharSequenceIntHashMap implements Mutable {

    public static final int NO_ENTRY_VALUE = -1;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final ThreadLocal<StringUtf8MemoryAccessor> stringUtf8MemoryAccessor = new ThreadLocal<>(StringUtf8MemoryAccessor::new);

    private final ObjList<String> list;
    private final double loadFactor;
    private final int noEntryValue;
    private int capacity;
    private int free;
    private long[] hashCodes;
    private String[] keys;
    private int mask;
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

        free = capacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.loadFactor = loadFactor;
        int len = Numbers.ceilPow2((int) (capacity / loadFactor));
        keys = new String[len];
        hashCodes = new long[len];
        mask = len - 1;
        this.noEntryValue = noEntryValue;
        list = new ObjList<>(capacity);
        values = new int[keys.length];
        clear();
    }

    /**
     * Assumes that the string contains ASCII chars only. Strings with non-ASCII chars
     * are also supported, yet with higher chances of hash code collisions.
     */
    public static long xxHash64(String str) {
        return Hash.xxHash64(0, str.length(), 0, stringUtf8MemoryAccessor.get().of(str));
    }

    @Override
    public final void clear() {
        Arrays.fill(keys, null);
        Arrays.fill(hashCodes, 0);
        free = capacity;
        list.clear();
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

    public int get(String key) {
        return valueAt(keyIndex(key));
    }

    public int keyIndex(DirectByteCharSequence key) {
        long hashCode = Hash.xxHash64(key);
        int index = (int) (hashCode & mask);

        if (keys[index] == null) {
            return index;
        }

        if (hashCode == hashCodes[index] && Chars.equals(key, keys[index])) {
            return -index - 1;
        }

        return probe(key, hashCode, index);
    }

    public int keyIndex(String key) {
        long hashCode = xxHash64(key);
        int index = (int) (hashCode & mask);

        if (keys[index] == null) {
            return index;
        }

        if (hashCode == hashCodes[index] && Chars.equals(key, keys[index])) {
            return -index - 1;
        }

        return probe(key, hashCode, index);
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
                    String k = keys[from];
                    k != null;
                    from = (from + 1) & mask, k = keys[from]
            ) {
                long hashCode = xxHash64(k);
                int idealHit = (int) (hashCode & mask);
                if (idealHit != from) {
                    int to;
                    if (keys[idealHit] != null) {
                        to = probe(k, hashCode, idealHit);
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

    public int valueAt(int index) {
        int index1 = -index - 1;
        return index < 0 ? values[index1] : noEntryValue;
    }

    public int valueQuick(int index) {
        return valueAt(keyIndex(list.getQuick(index)));
    }

    private void erase(int index) {
        keys[index] = null;
        hashCodes[index] = 0;
        values[index] = noEntryValue;
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

    private int probe(String key, long hashCode, int index) {
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

    private void putAt0(int index, String key, int value) {
        keys[index] = key;
        hashCodes[index] = xxHash64(key);
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    private void rehash() {
        int[] oldValues = values;
        String[] oldKeys = keys;
        long[] oldHashCodes = hashCodes;
        int size = capacity - free;
        capacity = capacity * 2;
        free = capacity - size;
        mask = Numbers.ceilPow2((int) (capacity / loadFactor)) - 1;
        keys = new String[mask + 1];
        hashCodes = new long[mask + 1];
        values = new int[mask + 1];
        for (int i = oldKeys.length - 1; i > -1; i--) {
            String key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                hashCodes[index] = oldHashCodes[i];
                values[index] = oldValues[i];
            }
        }
    }

    /**
     * This memory accessor interprets string as a sequence of UTF8 bytes.
     * It means that each string's char is trimmed to a byte when calculating
     * the hash code.
     */
    public static class StringUtf8MemoryAccessor implements Hash.MemoryAccessor {

        private String str;

        @Override
        public byte getByte(long offset) {
            return (byte) str.charAt((int) offset);
        }

        @Override
        public int getInt(long offset) {
            final int index = (int) offset;
            final int n = byteAsUnsignedInt(index)
                    | (byteAsUnsignedInt(index + 1) << 8)
                    | (byteAsUnsignedInt(index + 2) << 16)
                    | (byteAsUnsignedInt(index + 3) << 24);
            return Unsafe.isLittleEndian() ? n : Integer.reverseBytes(n);
        }

        @Override
        public long getLong(long offset) {
            final int index = (int) offset;
            final long n = byteAsUnsignedLong(index)
                    | (byteAsUnsignedLong(index + 1) << 8)
                    | (byteAsUnsignedLong(index + 2) << 16)
                    | (byteAsUnsignedLong(index + 3) << 24)
                    | (byteAsUnsignedLong(index + 4) << 32)
                    | (byteAsUnsignedLong(index + 5) << 40)
                    | (byteAsUnsignedLong(index + 6) << 48)
                    | (byteAsUnsignedLong(index + 7) << 56);
            return Unsafe.isLittleEndian() ? n : Long.reverseBytes(n);
        }

        public int length() {
            return str.length();
        }

        public StringUtf8MemoryAccessor of(String str) {
            this.str = str;
            return this;
        }

        private int byteAsUnsignedInt(int index) {
            return ((byte) str.charAt(index)) & 0xff;
        }

        private long byteAsUnsignedLong(int index) {
            return (long) ((byte) str.charAt(index)) & 0xff;
        }
    }
}
