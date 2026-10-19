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
 * The stored int is <strong>unsigned</strong>: the 4-byte-aligned pair sets the top bit from the
 * 8GB mark upwards, the 8-byte-aligned pairs from 16GB. Consumers must therefore test a sentinel -
 * an empty slot or a chain end - for equality and never as {@code < 0}, and must widen with
 * {@link Integer#toUnsignedLong(int)} rather than a plain cast. Reading such an offset as signed
 * yields a negative value that a {@code < 0} test silently mistakes for the sentinel.
 * <p>
 * The 4-byte-aligned pair below backs the value chains of
 * {@link io.questdb.griffin.engine.orderby.LongTreeChain},
 * {@link io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain} and
 * {@link io.questdb.griffin.engine.join.LongChain}, and the 8-byte-aligned pair backs
 * {@link io.questdb.griffin.engine.AbstractRedBlackTree}'s key heap. All four held byte-identical
 * copies of this arithmetic. Keeping one copy keeps the unsigned contract, and any future
 * correction to it, in a single place.
 * <p>
 * The biased 8-byte-aligned trio backs {@link io.questdb.cairo.map.OrderedMap}'s hash table. Its
 * offsets carry a {@code +1} bias so that 0 can mark an empty slot, which makes it a third encoding
 * rather than a duplicate - but the unsigned contract above binds it just as hard, and its
 * emptiness sentinel lives here so a test can pin it at the boundary without a 16GB heap.
 */
public final class CompressedOffsets {
    /**
     * The largest heap {@link #compressAligned4(long)} addresses: {@code (2^32 - 2) * 4}. One step
     * below the all-ones encoding that consumers reserve as their chain-end sentinel, so no offset
     * inside a heap of this size can collide with it. A heap allocated above this silently truncates
     * the offsets of everything in its top region, which is why both the tree constructor and the
     * configuration validation reject such a page rather than wait for the heap to fill.
     */
    public static final long MAX_ALIGNED4_HEAP_SIZE = (Integer.toUnsignedLong(-1) - 1) << 2;
    /**
     * The largest heap {@link #compressAligned8(long)} and {@link #compressBiased8(long)} address:
     * {@code (2^32 - 2) * 8}. Both encodings share the bound - the bias moves which encoding is
     * reserved, not how far the 32 bits reach. See {@link #MAX_ALIGNED4_HEAP_SIZE}.
     */
    public static final long MAX_ALIGNED8_HEAP_SIZE = (Integer.toUnsignedLong(-1) - 1) << 3;

    private CompressedOffsets() {
    }

    /**
     * Compresses a 4-byte-aligned heap offset. Offsets round trip up to {@code (2^32 - 2) * 4},
     * one step below the all-ones encoding that consumers reserve as their chain-end sentinel.
     */
    public static int compressAligned4(long rawOffset) {
        return (int) (rawOffset >> 2);
    }

    /**
     * Compresses an 8-byte-aligned heap offset. Offsets round trip up to {@code (2^32 - 2) * 8},
     * one step below the all-ones encoding that consumers reserve as their empty-block sentinel.
     */
    public static int compressAligned8(long rawOffset) {
        return (int) (rawOffset >> 3);
    }

    /**
     * Compresses an 8-byte-aligned heap offset and adds a {@code +1} bias, reserving 0 to mark an
     * empty hash table slot. Offsets round trip up to {@code (2^32 - 2) * 8}.
     */
    public static int compressBiased8(long rawOffset) {
        return (int) ((rawOffset >> 3) + 1);
    }

    /**
     * Tests a biased 8-byte-aligned slot for emptiness. The bias makes 0 the only empty encoding,
     * so this has to be {@code == 0} and never {@code <= 0}: the smallest offset whose compressed
     * form has its top bit set already sits 17,179,869,176 bytes into the heap, and a signed test
     * reads every entry from there upwards as an empty slot.
     */
    public static boolean isEmptyBiased8(int offset) {
        return offset == 0;
    }

    /**
     * Widens a 4-byte-aligned compressed offset back to a byte offset, treating it as unsigned.
     */
    public static long uncompressAligned4(int offset) {
        return Integer.toUnsignedLong(offset) << 2;
    }

    /**
     * Widens an 8-byte-aligned compressed offset back to a byte offset, treating it as unsigned.
     */
    public static long uncompressAligned8(int offset) {
        return Integer.toUnsignedLong(offset) << 3;
    }

    /**
     * Widens a biased 8-byte-aligned compressed offset back to a byte offset, treating it as
     * unsigned and removing the {@code +1} bias. Callers must have established that the slot is
     * occupied, i.e. that {@link #isEmptyBiased8(int)} is false.
     */
    public static long uncompressBiased8(int offset) {
        return (Integer.toUnsignedLong(offset) - 1) << 3;
    }
}
