/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import org.jetbrains.annotations.NotNull;

/**
 * A sequence of UTF-8 bytes.
 */
public interface Utf8Sequence {

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
        for (int i = offset; i < offset + Long.BYTES; i++) {
            result |= (long) (byteAt(i) & 0xff) << (8 * (i - offset));
        }
        return result;
    }

    /**
     * Returns the first 6 bytes of this UTF-8 sequence. This prefix is stored inline
     * in the auxiliary vector of a VARCHAR column, so asking for it is a matter of
     * optimized data access. This is not a general access method, it is implemented
     * only where needed to support the optimization.
     */
    default long zeroPaddedSixPrefix() {
        throw new UnsupportedOperationException();
    }

    /**
     * Called as a part of equality check that has already ensured the two strings
     * have the same byte size. This is especially relevant when comparing two values
     * from a VARCHAR column: same size guarantees they are either both inlined or
     * both not inlined, which means the same `Utf8Sequence` implementation is on
     * both sides.
     */
    default boolean equalsAssumingSameSize(Utf8Sequence other) {
        for (int i = 0; i < size(); i++) {
            if (byteAt(i) != other.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns `true` if it's guaranteed that the contents of this UTF-8 sequence are
     * all ASCII characters. Returning `false` does not guarantee anything.
     */
    default boolean isAscii() {
        return false;
    }

    /**
     * Number of bytes in the string.
     * <p>
     * This is NOT the number of 16-bit chars or code points in the string.
     * This is named `size` instead of `length` to avoid collision withs the `CharSequence` interface.
     */
    int size();

    default void writeTo(long addr, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            Unsafe.getUnsafe().putByte(addr++, byteAt(i));
        }
    }
}
