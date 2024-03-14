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
     * @return true if all characters in the string are ASCII. This is a best-effort flag, i.e. it may have
     * false value while the actual string has ASCII characters only.
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
