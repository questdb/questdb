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

package io.questdb.cairo.lv;

import io.questdb.std.Hash;
import io.questdb.std.Unsafe;

import java.nio.ByteOrder;

/**
 * Hash, equality and order over encoded checkpoint partition keys held in native memory.
 * A key passes as an {@code (address, length)} pair that is valid only for the call: a
 * callee that keeps the key copies it into memory it owns, and a caller never holds an
 * address across an append to the memory it came from, because that memory moves when it
 * grows.
 * <p>
 * Each result equals what the same bytes give as a heap array. {@link #hashCode} equals
 * {@code Arrays.hashCode(bytes)} and {@link #hash} equals
 * {@code Hash.spread(Arrays.hashCode(bytes))}, so a hash table keeps the slot order it
 * had over heap keys. {@link #compare} orders exactly as
 * {@link LiveViewCheckpointMetadata#compareBytes}, unsigned byte by byte and then by
 * length, which is the order every persisted partition map is sorted in.
 * <p>
 * The compare and equality loops stay in Java and read eight bytes at a time: a key is
 * tens of bytes, so a native call per probe would cost more than the comparison.
 */
final class LiveViewCheckpointKeys {
    private static final boolean IS_LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    private LiveViewCheckpointKeys() {
    }

    /**
     * @return -1, 0 or 1 as the left key sorts before, equal to or after the right one
     */
    static int compare(long left, int leftLength, long right, int rightLength) {
        final int n = Math.min(leftLength, rightLength);
        int i = 0;
        if (IS_LITTLE_ENDIAN) {
            for (; i <= n - Long.BYTES; i += Long.BYTES) {
                final long l = Unsafe.getLong(left + i);
                final long r = Unsafe.getLong(right + i);
                if (l != r) {
                    // Byte-reversing a little-endian word puts its first byte most
                    // significant, so an unsigned word compare is the byte compare.
                    return Long.compareUnsigned(Long.reverseBytes(l), Long.reverseBytes(r)) < 0 ? -1 : 1;
                }
            }
        }
        for (; i < n; i++) {
            final int l = Unsafe.getByte(left + i) & 0xff;
            final int r = Unsafe.getByte(right + i) & 0xff;
            if (l != r) {
                return l < r ? -1 : 1;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    static boolean equals(long left, int leftLength, long right, int rightLength) {
        if (leftLength != rightLength) {
            return false;
        }
        int i = 0;
        for (; i <= leftLength - Long.BYTES; i += Long.BYTES) {
            if (Unsafe.getLong(left + i) != Unsafe.getLong(right + i)) {
                return false;
            }
        }
        for (; i < leftLength; i++) {
            if (Unsafe.getByte(left + i) != Unsafe.getByte(right + i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return {@code Hash.spread} of {@link #hashCode}, the slot hash an open-addressed
     * table over heap keys used
     */
    static int hash(long address, int length) {
        return Hash.spread(hashCode(address, length));
    }

    /**
     * @return the same value {@code Arrays.hashCode} gives for these bytes as an array
     */
    static int hashCode(long address, int length) {
        int h = 1;
        for (int i = 0; i < length; i++) {
            h = 31 * h + Unsafe.getByte(address + i);
        }
        return h;
    }
}
