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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.sql.SymbolTable;

/**
 * Membership bitset over the keys of one symbol column, the representation both the Java and the
 * JIT-compiled {@code symbol IN (...)} filters test a row's key against.
 * <p>
 * Bit 0 stands for NULL ({@link SymbolTable#VALUE_IS_NULL}, {@code Integer.MIN_VALUE}) and bit
 * {@code k + 1} for key {@code k >= 0}, so {@link #bitIndex(int)} maps a key branch-free:
 * {@code (key + 1) & ~(key >> 31)}. Every negative key collapses to bit 0, which is sound because
 * NULL is the only negative key a symbol column stores. A key past the last bit - one the list
 * did not name when it was resolved, including a key a concurrent writer appended since - tests
 * false. The JIT backends ({@code jit/x86.h}, {@code jit/avx2.h}) compute the same index and read
 * the words as little-endian 32-bit halves of these longs.
 */
public final class SymbolKeyBitSet {
    private int bitCount;
    private long[] words = new long[1];

    /**
     * Bit index for a symbol key, see the class comment. Negative for {@code Integer.MAX_VALUE},
     * which no symbol column can hold.
     */
    public static int bitIndex(int key) {
        return (key + 1) & ~(key >> 31);
    }

    /**
     * Number of bits a set needs to cover keys up to {@code maxKey}, NULL included. {@code maxKey}
     * of -1 (no non-null key) needs the NULL bit only.
     */
    public static long bitsFor(int maxKey) {
        return (long) maxKey + 2;
    }

    /**
     * Whether a set covering keys up to {@code maxKey} fits under the
     * {@code cairo.sql.symbol.in.bitset.max.keys} cap: every key the list resolves to must be
     * below {@code maxKeys}, so the set holds at most {@code maxKeys + 1} bits (one per key, plus
     * NULL), about {@code maxKeys / 8} bytes. The JIT and the Java filter apply this same test
     * to the largest key a list resolves to; it does not depend on how many symbols the column
     * holds.
     */
    public static boolean fitsCap(int maxKey, int maxKeys) {
        return bitsFor(maxKey) - 1 <= maxKeys;
    }

    /**
     * Adds a resolved key. {@link SymbolTable#VALUE_NOT_FOUND} and any other negative non-NULL
     * key name no row and are ignored. The caller must have sized the set with {@link #reset(int)}
     * to cover the key.
     */
    public void add(int key) {
        if (key >= 0 || key == SymbolTable.VALUE_IS_NULL) {
            final int idx = bitIndex(key);
            assert idx < bitCount;
            words[idx >>> 6] |= 1L << idx;
        }
    }

    public boolean contains(int key) {
        final int idx = bitIndex(key);
        return idx >= 0 && idx < bitCount && (words[idx >>> 6] & (1L << idx)) != 0;
    }

    /**
     * Empties the set and sizes it to cover keys up to {@code maxKey}.
     */
    public void reset(int maxKey) {
        final long bits = bitsFor(maxKey);
        assert bits <= Integer.MAX_VALUE;
        bitCount = (int) bits;
        final int wordCount = (bitCount + 63) >>> 6;
        if (words.length < wordCount) {
            words = new long[wordCount];
        } else {
            for (int i = 0; i < wordCount; i++) {
                words[i] = 0;
            }
        }
    }
}
