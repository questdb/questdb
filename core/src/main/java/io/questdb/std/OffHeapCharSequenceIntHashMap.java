/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.str.Utf8s;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.std.MemoryTag.NATIVE_DEFAULT;

public class OffHeapCharSequenceIntHashMap implements Mutable, Closeable {
    public static final int NO_ENTRY_VALUE = -1;
    private static final long EMPTY_KEY_PTR = 0L;
    // Hash table entry layout (32 bytes per entry)
    // 0-7: key pointer (long)
    // 8-11: key char length (int)
    // 12-15: key utf8 byte length (int)
    // 16-19: value (int)
    // 20-23: hash code (int)
    // 24-31: padding (8 bytes) - for alignment
    private static final int ENTRY_SIZE = 32;
    private static final int HASH_OFFSET = 20;
    private static final int KEY_CHAR_LEN_OFFSET = 8;
    private static final int KEY_PTR_OFFSET = 0;
    private static final int KEY_UTF8_LEN_OFFSET = 12;
    private static final int VALUE_OFFSET = 16;
    private final ObjList<CharSequence> keyList = new ObjList<>();
    private final double loadFactor;
    private final int memoryTag;
    private final int noEntryValue;
    private int capacity;
    private int free;
    // Key storage area
    private long keyStorageAddress;
    private long keyStorageCapacity;
    private long keyStorageUsed;
    private int mask;
    private int size;
    // Hash table storage
    private long tableAddress;

    public OffHeapCharSequenceIntHashMap() {
        this(8);
    }

    public OffHeapCharSequenceIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.4, NO_ENTRY_VALUE);
    }

    public OffHeapCharSequenceIntHashMap(int initialCapacity, double loadFactor, int noEntryValue) {
        this(initialCapacity, loadFactor, noEntryValue, NATIVE_DEFAULT);
    }

    public OffHeapCharSequenceIntHashMap(int initialCapacity, double loadFactor, int noEntryValue, int memoryTag) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        this.loadFactor = loadFactor;
        this.noEntryValue = noEntryValue;
        this.memoryTag = memoryTag;

        int cap = initialCapacity < 16 ? 16 : Numbers.ceilPow2(initialCapacity);
        int tableSize = Numbers.ceilPow2((int) (cap / loadFactor));

        this.capacity = cap;
        this.mask = tableSize - 1;
        this.free = cap;
        this.size = 0;

        // Allocate hash table
        long tableBytes = (long) tableSize * ENTRY_SIZE;
        this.tableAddress = Unsafe.malloc(tableBytes, memoryTag);

        // Allocate key storage (start with 64KB)
        this.keyStorageCapacity = 64L * 1024;
        this.keyStorageAddress = Unsafe.malloc(keyStorageCapacity, memoryTag);
        this.keyStorageUsed = 0;

        clear();
    }

    @Override
    public final void clear() {
        size = 0;
        free = capacity;
        keyStorageUsed = 0;
        keyList.clear();

        // Clear hash table by zeroing all key pointers
        long tableBytes = (long) (mask + 1) * ENTRY_SIZE;
        Vect.memset(tableAddress, tableBytes, 0);
    }

    @Override
    public void close() {
        if (tableAddress != 0) {
            long tableBytes = (long) (mask + 1) * ENTRY_SIZE;
            tableAddress = Unsafe.free(tableAddress, tableBytes, memoryTag);
        }

        if (keyStorageAddress != 0) {
            keyStorageAddress = Unsafe.free(keyStorageAddress, keyStorageCapacity, memoryTag);
        }

        capacity = 0;
        mask = 0;
        size = 0;
        free = 0;
        keyStorageCapacity = 0;
        keyStorageUsed = 0;
    }

    public int get(@NotNull CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int keyIndex(@NotNull CharSequence key) {
        int hash = hashCode(key);
        int index = hash & mask;

        long entryAddr = tableAddress + (long) index * ENTRY_SIZE;
        long keyPtr = Unsafe.getUnsafe().getLong(entryAddr + KEY_PTR_OFFSET);

        if (keyPtr == EMPTY_KEY_PTR) {
            return index;
        }

        if (keyEquals(key, entryAddr)) {
            return -index - 1;
        }

        return probe(key, hash, index);
    }

    public ObjList<CharSequence> keys() {
        return keyList;
    }

    public boolean put(@NotNull CharSequence key, int value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, @NotNull CharSequence key, int value) {
        if (index < 0) {
            // Key exists, update value
            int idx = -index - 1;
            long entryAddr = tableAddress + (long) idx * ENTRY_SIZE;
            Unsafe.getUnsafe().putInt(entryAddr + VALUE_OFFSET, value);
            return false;
        }

        putAt0(index, key, value);
        return true;
    }

    public void putIfAbsent(@NotNull CharSequence key, int value) {
        int index = keyIndex(key);
        if (index > -1) {
            putAt0(index, key, value);
        }
    }

    public int size() {
        return size;
    }

    public int valueAt(int index) {
        if (index < 0) {
            int idx = -index - 1;
            long entryAddr = tableAddress + (long) idx * ENTRY_SIZE;
            return Unsafe.getUnsafe().getInt(entryAddr + VALUE_OFFSET);
        }
        return noEntryValue;
    }

    private void encodeUtf8ToNativeMemory(@NotNull CharSequence key, long targetAddress) {
        // Create a minimal Utf8Sink that writes directly to native memory
        MemoryUtf8Sink sink = new MemoryUtf8Sink(targetAddress);

        // Use QuestDB's zero-allocation UTF-8 encoding
        final int len = key.length();
        int i = 0;
        while (i < len) {
            char c = key.charAt(i++);
            if (c < 128) {
                sink.put((byte) c);
            } else {
                i = Utf8s.encodeUtf16Char(sink, key, len, i, c);
            }
        }
    }

    private void expandKeyStorage(long additionalBytes) {
        long newCapacity = Math.max(keyStorageCapacity * 2, keyStorageCapacity + additionalBytes + 1024);
        long newAddress = Unsafe.malloc(newCapacity, memoryTag);

        // Copy existing data
        if (keyStorageUsed > 0) {
            Vect.memcpy(newAddress, keyStorageAddress, keyStorageUsed);
        }

        // Free old storage
        Unsafe.free(keyStorageAddress, keyStorageCapacity, memoryTag);

        keyStorageAddress = newAddress;
        keyStorageCapacity = newCapacity;
    }

    private int hashCode(@NotNull CharSequence key) {
        return Hash.spread(Chars.hashCode(key));
    }

    private boolean keyEquals(@NotNull CharSequence key, long entryAddr) {
        return keyEqualsUtf8(key, entryAddr);
    }

    private boolean keyEqualsUtf8(@NotNull CharSequence key, long entryAddr) {
        long keyPtr = Unsafe.getUnsafe().getLong(entryAddr + KEY_PTR_OFFSET);
        int charLen = Unsafe.getUnsafe().getInt(entryAddr + KEY_CHAR_LEN_OFFSET);
        int utf8Len = Unsafe.getUnsafe().getInt(entryAddr + KEY_UTF8_LEN_OFFSET);

        if (key.length() != charLen) {
            return false;
        }

        // Create a temporary sink to encode the key and compare byte by byte
        TempMemoryUtf8Sink tempSink = new TempMemoryUtf8Sink();
        final int len = key.length();
        int i = 0;
        int byteIndex = 0;

        while (i < len && byteIndex < utf8Len) {
            char c = key.charAt(i++);
            if (c < 128) {
                if ((byte) c != Unsafe.getUnsafe().getByte(keyPtr + byteIndex)) {
                    return false;
                }
                byteIndex++;
            } else {
                // For non-ASCII characters, encode and compare
                tempSink.clear();
                i = Utf8s.encodeUtf16Char(tempSink, key, len, i, c);
                for (int j = 0; j < tempSink.size() && byteIndex < utf8Len; j++) {
                    if (tempSink.getByte(j) != Unsafe.getUnsafe().getByte(keyPtr + byteIndex)) {
                        return false;
                    }
                    byteIndex++;
                }
            }
        }

        return byteIndex == utf8Len;
    }

    private int probe(@NotNull CharSequence key, int hash, int index) {
        do {
            index = (index + 1) & mask;
            long entryAddr = tableAddress + (long) index * ENTRY_SIZE;
            long keyPtr = Unsafe.getUnsafe().getLong(entryAddr + KEY_PTR_OFFSET);

            if (keyPtr == EMPTY_KEY_PTR) {
                return index;
            }

            if (keyEquals(key, entryAddr)) {
                return -index - 1;
            }
        } while (true);
    }

    private void putAt0(int index, @NotNull CharSequence key, int value) {
        // Store key in off-heap storage
        int utf8Len = Utf8s.utf8Bytes(key);
        long keyPtr = storeKey(key, utf8Len);
        int charLen = key.length();
        int hash = hashCode(key);

        // Store entry in hash table
        long entryAddr = tableAddress + (long) index * ENTRY_SIZE;
        Unsafe.getUnsafe().putLong(entryAddr + KEY_PTR_OFFSET, keyPtr);
        Unsafe.getUnsafe().putInt(entryAddr + KEY_CHAR_LEN_OFFSET, charLen);
        Unsafe.getUnsafe().putInt(entryAddr + KEY_UTF8_LEN_OFFSET, utf8Len);
        Unsafe.getUnsafe().putInt(entryAddr + VALUE_OFFSET, value);
        Unsafe.getUnsafe().putInt(entryAddr + HASH_OFFSET, hash);

        // Add to key list for iteration order (like the heap implementation)
        keyList.add(key.toString());

        size++;
        if (--free == 0) {
            rehash();
        }
    }

    private void rehash() {
        // Save old table data
        long oldTableAddress = tableAddress;
        int oldTableSize = mask + 1;

        // Create new larger table
        capacity = capacity * 2;
        free = capacity - size;
        int newTableSize = Numbers.ceilPow2((int) (capacity / loadFactor));
        mask = newTableSize - 1;

        long newTableBytes = (long) newTableSize * ENTRY_SIZE;
        tableAddress = Unsafe.malloc(newTableBytes, memoryTag);
        Vect.memset(tableAddress, newTableBytes, 0);

        // Rehash all entries
        for (int i = 0; i < oldTableSize; i++) {
            long oldEntryAddr = oldTableAddress + (long) i * ENTRY_SIZE;
            long keyPtr = Unsafe.getUnsafe().getLong(oldEntryAddr + KEY_PTR_OFFSET);

            if (keyPtr != EMPTY_KEY_PTR) {
                int charLen = Unsafe.getUnsafe().getInt(oldEntryAddr + KEY_CHAR_LEN_OFFSET);
                int utf8Len = Unsafe.getUnsafe().getInt(oldEntryAddr + KEY_UTF8_LEN_OFFSET);
                int value = Unsafe.getUnsafe().getInt(oldEntryAddr + VALUE_OFFSET);
                int hash = Unsafe.getUnsafe().getInt(oldEntryAddr + HASH_OFFSET);

                // Find new position
                int newIndex = hash & mask;
                long newEntryAddr = tableAddress + (long) newIndex * ENTRY_SIZE;

                while (Unsafe.getUnsafe().getLong(newEntryAddr + KEY_PTR_OFFSET) != EMPTY_KEY_PTR) {
                    newIndex = (newIndex + 1) & mask;
                    newEntryAddr = tableAddress + (long) newIndex * ENTRY_SIZE;
                }

                // Copy entry
                Unsafe.getUnsafe().putLong(newEntryAddr + KEY_PTR_OFFSET, keyPtr);
                Unsafe.getUnsafe().putInt(newEntryAddr + KEY_CHAR_LEN_OFFSET, charLen);
                Unsafe.getUnsafe().putInt(newEntryAddr + KEY_UTF8_LEN_OFFSET, utf8Len);
                Unsafe.getUnsafe().putInt(newEntryAddr + VALUE_OFFSET, value);
                Unsafe.getUnsafe().putInt(newEntryAddr + HASH_OFFSET, hash);
            }
        }

        // Free old table
        long oldTableBytes = (long) oldTableSize * ENTRY_SIZE;
        Unsafe.free(oldTableAddress, oldTableBytes, memoryTag);
    }

    private long storeKey(@NotNull CharSequence key, int utf8Len) {
        // Ensure we have enough storage space
        if (keyStorageUsed + utf8Len > keyStorageCapacity) {
            expandKeyStorage(utf8Len);
        }

        // Store UTF-8 bytes directly to native memory using zero-allocation encoding
        long keyPtr = keyStorageAddress + keyStorageUsed;
        encodeUtf8ToNativeMemory(key, keyPtr);
        keyStorageUsed += utf8Len;

        return keyPtr;
    }

    // Minimal Utf8Sink implementation that writes directly to native memory
    private static class MemoryUtf8Sink implements Utf8Sink {
        private final long address;
        private int position;

        MemoryUtf8Sink(long address) {
            this.address = address;
            this.position = 0;
        }

        @Override
        public Utf8Sink put(byte b) {
            Unsafe.getUnsafe().putByte(address + position, b);
            position++;
            return this;
        }

        @Override
        public Utf8Sink put(@NotNull CharSequence cs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Utf8Sink put(@NotNull CharSequence cs, int start, int end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Utf8Sink put(char c) {
            if (c < 128) {
                put((byte) c);
            } else {
                throw new UnsupportedOperationException();
            }
            return this;
        }

        @Override
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            if (us != null) {
                final int size = us.size();
                for (int i = 0; i < size; i++) {
                    put(us.byteAt(i));
                }
            }
            return this;
        }

        @Override
        public Utf8Sink putAscii(@NotNull CharSequence cs) {
            for (int i = 0; i < cs.length(); i++) {
                putAscii(cs.charAt(i));
            }
            return this;
        }

        @Override
        public Utf8Sink putAscii(char c) {
            put((byte) c);
            return this;
        }

        @Override
        public Utf8Sink putNonAscii(long lo, long hi) {
            for (long p = lo; p < hi; p++) {
                put(Unsafe.getUnsafe().getByte(p));
            }
            return this;
        }
    }

    // Temporary sink for encoding comparisons
    private static class TempMemoryUtf8Sink implements Utf8Sink {
        private final byte[] buffer = new byte[8]; // Max UTF-8 char is 4 bytes, but allow some extra
        private int position;

        @Override
        public Utf8Sink put(byte b) {
            buffer[position++] = b;
            return this;
        }

        @Override
        public Utf8Sink put(@NotNull CharSequence cs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Utf8Sink put(@NotNull CharSequence cs, int start, int end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Utf8Sink put(char c) {
            if (c < 128) {
                put((byte) c);
            } else {
                throw new UnsupportedOperationException();
            }
            return this;
        }

        @Override
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            if (us != null) {
                final int size = us.size();
                for (int i = 0; i < size; i++) {
                    put(us.byteAt(i));
                }
            }
            return this;
        }

        @Override
        public Utf8Sink putAscii(@NotNull CharSequence cs) {
            for (int i = 0; i < cs.length(); i++) {
                putAscii(cs.charAt(i));
            }
            return this;
        }

        @Override
        public Utf8Sink putAscii(char c) {
            put((byte) c);
            return this;
        }

        @Override
        public Utf8Sink putNonAscii(long lo, long hi) {
            for (long p = lo; p < hi; p++) {
                put(Unsafe.getUnsafe().getByte(p));
            }
            return this;
        }

        void clear() {
            position = 0;
        }

        byte getByte(int index) {
            return buffer[index];
        }

        int size() {
            return position;
        }
    }
}