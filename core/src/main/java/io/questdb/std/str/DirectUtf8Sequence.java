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
import io.questdb.std.bytes.DirectByteSequence;

/**
 * Read-only interface for a UTF-8 string with native ptr access.
 */
public interface DirectUtf8Sequence extends Utf8Sequence, DirectByteSequence {
    /**
     * Returns byte at index.
     * Note: Unchecked bounds.
     *
     * @param index byte index
     * @return byte at index
     */
    default byte byteAt(long index) {
        return DirectByteSequence.super.byteAt(index);
    }

    /**
     * Address one past the last character.
     */
    default long hi() {
        return DirectByteSequence.super.hi();
    }

    /**
     * Address of the first character (alias of `.ptr()`).
     */
    default long lo() {
        return DirectByteSequence.super.lo();
    }

    /**
     * Address of the first character.
     */
    long ptr();
}
