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

import java.util.Arrays;

/**
 * Simplified variant of {@link java.util.BitSet}.
 */
public class BitSet implements Mutable {
    public static final int BITS_PER_WORD = 64;
    private final int initialNBits;
    private long[] words;

    public BitSet() {
        this(16 * BITS_PER_WORD);
    }

    public BitSet(int nBits) {
        this.initialNBits = nBits;
        this.words = new long[wordIndex(nBits - 1) + 1];
    }

    public long capacity() {
        return (long) words.length * BITS_PER_WORD;
    }

    @Override
    public void clear() {
        Arrays.fill(words, 0);
    }

    public boolean get(int bitIndex) {
        int wordIndex = wordIndex(bitIndex);
        return wordIndex < words.length && (words[wordIndex] & 1L << bitIndex) != 0L;
    }

    /**
     * Sets the given bit to 1 and returns its old value.
     */
    public boolean getAndSet(int bitIndex) {
        int wordIndex = wordIndex(bitIndex);
        checkCapacity(wordIndex + 1);
        boolean old = (words[wordIndex] & 1L << bitIndex) != 0L;
        words[wordIndex] |= 1L << bitIndex;
        return old;
    }

    public void resetCapacity() {
        this.words = new long[wordIndex(initialNBits - 1) + 1];
    }

    /**
     * Sets the given bit to 1.
     */
    public void set(int bitIndex) {
        int wordIndex = wordIndex(bitIndex);
        checkCapacity(wordIndex + 1);
        words[wordIndex] |= 1L << bitIndex;
    }

    /**
     * Sets the given bit to 0.
     */
    public void unset(int bitIndex) {
        int wordIndex = wordIndex(bitIndex);
        checkCapacity(wordIndex + 1);
        words[wordIndex] &= ~(1L << bitIndex);
    }

    private static int wordIndex(int bitIndex) {
        return bitIndex >> 6;
    }

    private void checkCapacity(int wordsRequired) {
        if (words.length < wordsRequired) {
            int newLen = Math.max(2 * words.length, wordsRequired);
            words = Arrays.copyOf(words, newLen);
        }
    }
}
