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

package io.questdb.std.str;

import io.questdb.std.Unsafe;
import io.questdb.std.bytes.ByteSequence;
import org.jetbrains.annotations.NotNull;

/**
 * A sequence of UTF-8 bytes.
 */
public interface Utf8Sequence extends ByteSequence {

    /**
     * Returns a CharSequence view of the sequence.
     * The view will be only valid if the sequence contains ASCII chars only.
     *
     * @return CharSequence view of the sequence
     */
    @NotNull
    CharSequence asAsciiCharSequence();

    /**
     * Returns byte at index.
     * Note: Unchecked bounds.
     *
     * @param index byte index
     * @return byte at index
     */
    byte byteAt(int index);

    default int intAt(int offset) {
        int result = 0;
        result |= byteAt(offset) & 0xff;
        result |= (byteAt(offset + 1) & 0xff) << 8;
        result |= (byteAt(offset + 2) & 0xff) << (8 * 2);
        result |= (byteAt(offset + 3) & 0xff) << (8 * 3);
        return result;
    }

    /**
     * Returns `true` if it's guaranteed that the contents of this UTF-8 sequence are
     * all ASCII characters. Returning `false` does not guarantee anything.
     */
    default boolean isAscii() {
        return false;
    }

    /**
     * Returns true if the pointer returned by {@link #ptr()} method is stable during a query execution.
     * Stable is defined as:
     * - the pointer remains valid for the duration of the query execution
     * - the sequence of bytes pointed to by the pointer does not change during the query execution
     */
    default boolean isStable() {
        return false;
    }

    /**
     * Returns eight bytes of the UTF-8 sequence located at the provided
     * byte offset, packed into a single `long` value. The bytes are arranged
     * in little-endian order. The method does not check bounds and will
     * load the memory from any provided offset.
     *
     * @param offset offset of the first byte to load
     * @return byte at offset
     */
    default long longAt(int offset) {
        long result = 0;
        result |= byteAt(offset) & 0xffL;
        result |= (byteAt(offset + 1) & 0xffL) << 8;
        result |= (byteAt(offset + 2) & 0xffL) << (8 * 2);
        result |= (byteAt(offset + 3) & 0xffL) << (8 * 3);
        result |= (byteAt(offset + 4) & 0xffL) << (8 * 4);
        result |= (byteAt(offset + 5) & 0xffL) << (8 * 5);
        result |= (byteAt(offset + 6) & 0xffL) << (8 * 6);
        result |= (byteAt(offset + 7) & 0xffL) << (8 * 7);
        return result;
    }

    /**
     * For off-heap sequences returns address of the first character.
     * For on-heap sequences returns -1.
     */
    default long ptr() {
        return -1;
    }

    default short shortAt(int offset) {
        int result = 0;
        result |= byteAt(offset) & 0xff;
        result |= (byteAt(offset + 1) & 0xff) << 8;
        return (short) result;
    }

    /**
     * Number of bytes in the string.
     * <p>
     * This is NOT the number of 16-bit chars or code points in the string.
     * This is named `size` instead of `length` to avoid collision withs the `CharSequence` interface.
     */
    int size();

    /**
     * Number of bytes contiguously addressable bytes at the end of the sequence.
     * This is useful if we need to access the data zero-copy via simd instructions.
     * <p>
     * The returned value, is the number of addressable bytes past `hi()`.
     */
    default long tailPadding() {
        return 0;
    }

    default void writeTo(long addr, int lo, int hi) {
        int i = lo;
        for (int n = hi - 7; i < n; i += 8, addr += 8) {
            Unsafe.getUnsafe().putLong(addr, longAt(i));
        }
        for (; i < hi; i++, addr++) {
            Unsafe.getUnsafe().putByte(addr, byteAt(i));
        }
    }

    /**
     * Returns up to 6 initial bytes of this UTF-8 sequence (less if it's shorter)
     * packed into a zero-padded long value, in little-endian order. This prefix is
     * stored inline in the auxiliary vector of a VARCHAR column, so asking for it is a
     * matter of optimized data access. This is not a general access method, it
     * shouldn't be called unless looking to optimize the access of the VARCHAR column.
     */
    default long zeroPaddedSixPrefix() {
        return Utf8s.zeroPaddedSixPrefix(this);
    }
}
