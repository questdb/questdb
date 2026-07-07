/*+*****************************************************************************
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

import io.questdb.cairo.Reopenable;

import java.io.Closeable;

public class DirectBitSet implements Mutable, Closeable, Reopenable {
    public static final int BITS_PER_WORD = 64;
    private static final int DEFAULT_CAPACITY_BITS = 16 * BITS_PER_WORD;
    private static final int WORD_BYTES_SHIFT = 3;
    private final int initialNBits;
    private final int memoryTag;
    private long address;
    private int wordCount;

    public DirectBitSet() {
        this(DEFAULT_CAPACITY_BITS, MemoryTag.NATIVE_BIT_SET, false);
    }

    public DirectBitSet(int nBits) {
        this(nBits, MemoryTag.NATIVE_BIT_SET, false);
    }

    public DirectBitSet(int nBits, int memoryTag, boolean keepClosed) {
        assert nBits > 0;
        this.initialNBits = nBits;
        this.memoryTag = memoryTag;
        if (!keepClosed) {
            allocate(requiredWordCount(nBits));
        }
    }

    public long capacity() {
        return (long) wordCount * BITS_PER_WORD;
    }

    @Override
    public void clear() {
        if (address != 0) {
            Vect.memset(address, (long) wordCount << WORD_BYTES_SHIFT, 0);
        }
    }

    @Override
    public void close() {
        if (address != 0) {
            address = Unsafe.free(address, (long) wordCount << WORD_BYTES_SHIFT, memoryTag);
            wordCount = 0;
        }
    }

    public boolean get(int bitIndex) {
        int wi = wordIndex(bitIndex);
        if (wi >= wordCount) {
            return false;
        }
        long word = Unsafe.getLong(address + ((long) wi << WORD_BYTES_SHIFT));
        return (word & 1L << bitIndex) != 0L;
    }

    public boolean getAndSet(int bitIndex) {
        int wi = wordIndex(bitIndex);
        checkCapacity(wi + 1);
        long wordAddr = address + ((long) wi << WORD_BYTES_SHIFT);
        long word = Unsafe.getLong(wordAddr);
        long mask = 1L << bitIndex;
        if ((word & mask) != 0L) {
            return true;
        }
        Unsafe.putLong(wordAddr, word | mask);
        return false;
    }

    public int nextSetBit(int fromIndex) {
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        int wi = wordIndex(fromIndex);
        if (wi >= wordCount) {
            return -1;
        }
        long word = Unsafe.getLong(address + ((long) wi << WORD_BYTES_SHIFT)) & (-1L << fromIndex);
        while (true) {
            if (word != 0L) {
                return (wi << 6) + Long.numberOfTrailingZeros(word);
            }
            if (++wi >= wordCount) {
                return -1;
            }
            word = Unsafe.getLong(address + ((long) wi << WORD_BYTES_SHIFT));
        }
    }

    @Override
    public void reopen() {
        if (address == 0) {
            allocate(requiredWordCount(initialNBits));
        }
    }

    public void reserve(int nBits) {
        assert nBits > 0;
        int wc = requiredWordCount(nBits);
        if (address == 0) {
            allocate(wc);
        } else if (wordCount < wc) {
            long oldBytes = (long) wordCount << WORD_BYTES_SHIFT;
            long newBytes = (long) wc << WORD_BYTES_SHIFT;
            address = Unsafe.realloc(address, oldBytes, newBytes, memoryTag);
            Vect.memset(address + oldBytes, newBytes - oldBytes, 0);
            wordCount = wc;
        }
    }

    public void resetCapacity() {
        int targetWc = requiredWordCount(initialNBits);
        long newBytes = (long) targetWc << WORD_BYTES_SHIFT;
        if (address == 0) {
            address = Unsafe.malloc(newBytes, memoryTag);
        } else if (wordCount != targetWc) {
            address = Unsafe.realloc(address, (long) wordCount << WORD_BYTES_SHIFT, newBytes, memoryTag);
        }
        wordCount = targetWc;
        Vect.memset(address, newBytes, 0);
    }

    public void set(int bitIndex) {
        int wi = wordIndex(bitIndex);
        checkCapacity(wi + 1);
        long wordAddr = address + ((long) wi << WORD_BYTES_SHIFT);
        long word = Unsafe.getLong(wordAddr);
        Unsafe.putLong(wordAddr, word | 1L << bitIndex);
    }

    public void unset(int bitIndex) {
        int wi = wordIndex(bitIndex);
        if (wi >= wordCount) {
            return;
        }
        long wordAddr = address + ((long) wi << WORD_BYTES_SHIFT);
        long word = Unsafe.getLong(wordAddr);
        Unsafe.putLong(wordAddr, word & ~(1L << bitIndex));
    }

    private static int requiredWordCount(int nBits) {
        return wordIndex(nBits - 1) + 1;
    }

    private static int wordIndex(int bitIndex) {
        return bitIndex >> 6;
    }

    private void allocate(int wc) {
        long bytes = (long) wc << WORD_BYTES_SHIFT;
        address = Unsafe.malloc(bytes, memoryTag);
        wordCount = wc;
        Vect.memset(address, bytes, 0);
    }

    private void checkCapacity(int wordsRequired) {
        if (wordCount < wordsRequired) {
            int newWc = Math.max(wordCount << 1, wordsRequired);
            long oldBytes = (long) wordCount << WORD_BYTES_SHIFT;
            long newBytes = (long) newWc << WORD_BYTES_SHIFT;
            address = Unsafe.realloc(address, oldBytes, newBytes, memoryTag);
            Vect.memset(address + oldBytes, newBytes - oldBytes, 0);
            wordCount = newWc;
        }
    }
}
