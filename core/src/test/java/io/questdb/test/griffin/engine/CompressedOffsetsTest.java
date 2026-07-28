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

package io.questdb.test.griffin.engine;

import io.questdb.griffin.engine.CompressedOffsets;
import org.junit.Assert;
import org.junit.Test;

public class CompressedOffsetsTest {

    @Test
    public void testAligned4RoundTripsAboveSignedIntRange() {
        // Compressed offsets are unsigned 32-bit and 4-byte scaled. Offsets at or above 2^31 * 4
        // set the top bit of the raw int; reading them back as a signed int yielded a negative
        // offset, so a consumer walked 8GB below its heap. Unlike OrderedMap there is no +1 bias,
        // so offset 0 legitimately compresses to 0 and only -1 is reserved as a sentinel.
        final long chainValueSize = 12;
        final long maxHeapSize = (Integer.toUnsignedLong(-1) - 1) << 2; // (2^32 - 2) * 4
        final long lastSignedOffset = ((long) Integer.MAX_VALUE) << 2;  // compresses to Integer.MAX_VALUE
        final long firstUnsignedOffset = (1L << 31) << 2;               // compresses to Integer.MIN_VALUE
        final long lastValueOffset = maxHeapSize - chainValueSize;      // last offset a value can start at
        final long[] offsets = {
                0,
                4,
                1L << 30,
                lastSignedOffset,
                firstUnsignedOffset,
                3L << 32, // mid-unsigned range: compresses negative, but to neither boundary
                lastValueOffset,
        };
        for (long offset : offsets) {
            final int rawOffset = CompressedOffsets.compressAligned4(offset);
            Assert.assertEquals("offset " + offset, offset, CompressedOffsets.uncompressAligned4(rawOffset));
        }

        // The upper half of the range is exactly what the signed reading got wrong.
        Assert.assertTrue(CompressedOffsets.compressAligned4(lastSignedOffset) > 0);
        Assert.assertTrue(CompressedOffsets.compressAligned4(firstUnsignedOffset) < 0);
        Assert.assertTrue(CompressedOffsets.compressAligned4(lastValueOffset) < 0);

        // The chain-end sentinel decodes to 4 bytes past the largest addressable heap, which is
        // what makes it safe to reserve: no legal offset can produce it.
        Assert.assertEquals(maxHeapSize + 4, CompressedOffsets.uncompressAligned4(-1));
        for (long offset : offsets) {
            Assert.assertNotEquals("offset " + offset + " must not compress to the chain-end sentinel",
                    -1, CompressedOffsets.compressAligned4(offset));
        }
    }

    @Test
    public void testAligned8RoundTripsAboveSignedIntRange() {
        // The 8-byte-scaled pair backs AbstractRedBlackTree's key heap, which addresses blocks
        // rather than chain values. Same unsigned contract as the 4-byte pair, twice the reach:
        // reading a top-bit-set offset as signed put every node accessor 32GB below the heap.
        final long blockSize = 24;
        final long maxKeyHeapSize = (Integer.toUnsignedLong(-1) - 1) << 3; // (2^32 - 2) * 8
        final long lastSignedOffset = ((long) Integer.MAX_VALUE) << 3;     // compresses to Integer.MAX_VALUE
        final long firstUnsignedOffset = (1L << 31) << 3;                  // compresses to Integer.MIN_VALUE
        final long lastBlockOffset = maxKeyHeapSize - blockSize;           // last offset a block can start at
        final long[] offsets = {
                0,
                8,
                1L << 30,
                lastSignedOffset,
                firstUnsignedOffset,
                3L << 33, // mid-unsigned range: compresses negative, but to neither boundary
                lastBlockOffset,
        };
        for (long offset : offsets) {
            final int rawOffset = CompressedOffsets.compressAligned8(offset);
            Assert.assertEquals("offset " + offset, offset, CompressedOffsets.uncompressAligned8(rawOffset));
        }

        // The upper half of the range is exactly what the signed reading got wrong.
        Assert.assertTrue(CompressedOffsets.compressAligned8(lastSignedOffset) > 0);
        Assert.assertTrue(CompressedOffsets.compressAligned8(firstUnsignedOffset) < 0);
        Assert.assertTrue(CompressedOffsets.compressAligned8(lastBlockOffset) < 0);

        // The empty-block sentinel decodes to 8 bytes past the largest addressable heap, so no
        // legal block offset can collide with it.
        Assert.assertEquals(maxKeyHeapSize + 8, CompressedOffsets.uncompressAligned8(-1));
        for (long offset : offsets) {
            Assert.assertNotEquals("offset " + offset + " must not compress to the EMPTY sentinel",
                    -1, CompressedOffsets.compressAligned8(offset));
        }
    }

    @Test
    public void testBiased8RoundTripsAboveSignedIntRange() {
        // The biased pair backs OrderedMap's hash table: the +1 reserves 0 for an empty slot, so
        // every legal offset - including 0 itself - has to compress to something else. Reading a
        // top-bit-set offset as signed put the probe, rehash and merge scans 16GB below the heap.
        final long entrySize = 16;
        final long maxHeapSize = (Integer.toUnsignedLong(-1) - 1) << 3; // (2^32 - 2) * 8
        final long lastSignedOffset = (Integer.MAX_VALUE - 1L) << 3;    // compresses to Integer.MAX_VALUE
        final long firstUnsignedOffset = ((long) Integer.MAX_VALUE) << 3; // compresses to Integer.MIN_VALUE
        final long lastEntryOffset = maxHeapSize - entrySize;           // last offset an entry can start at
        final long[] offsets = {
                0,
                8,
                1L << 30,
                lastSignedOffset,
                firstUnsignedOffset,
                1L << 34,
                lastEntryOffset,
        };
        for (long offset : offsets) {
            final int rawOffset = CompressedOffsets.compressBiased8(offset);
            Assert.assertFalse("offset " + offset + " must not compress to the empty marker",
                    CompressedOffsets.isEmptyBiased8(rawOffset));
            Assert.assertEquals("offset " + offset, offset, CompressedOffsets.uncompressBiased8(rawOffset));
        }

        // The upper half of the range is exactly what the signed reading got wrong.
        Assert.assertTrue(CompressedOffsets.compressBiased8(lastSignedOffset) > 0);
        Assert.assertTrue(CompressedOffsets.compressBiased8(firstUnsignedOffset) < 0);
        Assert.assertTrue(CompressedOffsets.compressBiased8(lastEntryOffset) < 0);
    }

    @Test
    public void testIsEmptyBiased8RejectsSignedTest() {
        // OrderedMap's eight slot scans - probe0, probeReadOnly, both rehash loops, and the dest
        // and source halves of both merges - all route through isEmptyBiased8. Seven of them have a
        // test that goes red when the predicate regresses. The exception is mergeVarSizeKey()'s
        // source scan: the smallest raw offset a signed test misreads decodes to 17,179,869,176
        // bytes, and that loop reads the key length off the source address before it probes, so
        // planting one dereferences 16GB past the heap instead of failing an assertion. Pin the
        // predicate itself here, which is the cover that scan can actually get.
        Assert.assertTrue(CompressedOffsets.isEmptyBiased8(0));

        final int firstTopBitSet = Integer.MIN_VALUE; // 0x8000_0000
        Assert.assertEquals(17_179_869_176L, CompressedOffsets.uncompressBiased8(firstTopBitSet));
        final int[] occupied = {1, 2, 1 << 30, Integer.MAX_VALUE, firstTopBitSet, firstTopBitSet + 1, -1};
        for (int rawOffset : occupied) {
            Assert.assertFalse(
                    "raw offset " + Integer.toUnsignedString(rawOffset) + " marks a live entry",
                    CompressedOffsets.isEmptyBiased8(rawOffset)
            );
        }
    }
}
