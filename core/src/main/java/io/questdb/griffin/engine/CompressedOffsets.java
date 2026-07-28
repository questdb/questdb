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

package io.questdb.griffin.engine;

/**
 * Heap offsets compressed into a 32-bit int by scaling them down to their alignment.
 * <p>
 * The stored int is <strong>unsigned</strong>: every offset from the 8GB mark upwards has its top
 * bit set. Consumers must therefore test a sentinel - an empty slot or a chain end - for equality
 * and never as {@code < 0}, and must widen with {@link Integer#toUnsignedLong(int)} rather than a
 * plain cast. Reading such an offset as signed yields a negative value that a {@code < 0} test
 * silently mistakes for the sentinel.
 * <p>
 * The 4-byte-aligned pair below backs the value chains of
 * {@link io.questdb.griffin.engine.orderby.LongTreeChain},
 * {@link io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain} and
 * {@link io.questdb.griffin.engine.join.LongChain}, which held byte-identical copies of it. Keeping
 * one copy keeps the unsigned contract, and any future correction to it, in a single place.
 */
public final class CompressedOffsets {

    private CompressedOffsets() {
    }

    /**
     * Compresses a 4-byte-aligned heap offset. Addresses up to 16GB round trip.
     */
    public static int compressAligned4(long rawOffset) {
        return (int) (rawOffset >> 2);
    }

    /**
     * Widens a 4-byte-aligned compressed offset back to a byte offset, treating it as unsigned.
     */
    public static long uncompressAligned4(int offset) {
        return Integer.toUnsignedLong(offset) << 2;
    }
}
