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

package io.questdb.cairo.idx;

import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Constants and encoding/decoding utilities for Frame of Reference (FOR) bitpacking.
 * <p>
 * FOR compression stores row IDs in fixed-size blocks. Each block contains:
 * - A minimum value (reference frame)
 * - A bit-width for offsets
 * - Bit-packed offsets from the minimum
 * <p>
 * This achieves excellent compression for sequential/near-sequential data while
 * enabling fast SIMD-friendly decoding.
 * <p>
 * Block Layout (BLOCK_CAPACITY values per block):
 * <pre>
 * [minValue: 8B][bitWidth: 1B][valueCount: 2B][padding: 1B][packed offsets...]
 * </pre>
 * <p>
 * Key File Layout (.fk):
 * <pre>
 * [Header 64B: sig, seq, valMemSize, keyCount, seqCheck, maxVal]
 * [Key Entry 0: valueCount(8), firstBlockOffset(8), lastValue(8), blockCount(4), countCheck(4)]
 * [Key Entry 1: ...]
 * </pre>
 * <p>
 * Value File Layout (.fv):
 * <pre>
 * [Block 0 for Key 0: header + packed data]
 * [Block 1 for Key 0: ...]
 * [Block 0 for Key 1: ...]
 * </pre>
 */
public final class BitpackUtils {

    // Block configuration
    public static final int BLOCK_CAPACITY = 128;  // Values per block (power of 2 for fast division)
    public static final int BLOCK_HEADER_SIZE = 12; // minValue(8) + bitWidth(1) + valueCount(2) + padding(1)

    // Key file header layout (64 bytes reserved, 8-byte aligned)
    public static final int KEY_FILE_RESERVED = 64;
    // Bytes 48-63 reserved for future use
    // Signature for FOR-encoded index (distinct from legacy 0xfa and delta 0xfc)
    public static final byte SIGNATURE = (byte) 0xfd;

    /**
     * Calculates the number of bits needed to represent a value.
     */
    public static int bitsNeeded(long value) {
        if (value == 0) {
            return 1; // Need at least 1 bit
        }
        return 64 - Long.numberOfLeadingZeros(value);
    }

    /**
     * Calculates the total block size in bytes.
     */
    public static int blockSize(int valueCount, int bitWidth) {
        return BLOCK_HEADER_SIZE + packedDataSize(valueCount, bitWidth);
    }

    /**
     * Generates the key file name for a FOR bitmap index.
     */
    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".fk");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    /**
     * Calculates the packed data size in bytes for a block.
     */
    public static int packedDataSize(int valueCount, int bitWidth) {
        return (int) (((long) valueCount * bitWidth + 7) / 8);
    }

    /**
     * Unpacks all values from a block into an array.
     *
     * @param srcAddr    source memory address of packed data
     * @param valueCount number of values to unpack
     * @param bitWidth   bits per offset
     * @param minValue   minimum value to add back
     * @param destAddr   destination memory address for unpacked values
     */
    public static void unpackAllValues(long srcAddr, int valueCount, int bitWidth, long minValue, long destAddr) {
        if (PostingIndexNative.isNativeAvailable() && valueCount > 0 && bitWidth > 0 && bitWidth <= 32) {
            PostingIndexNative.unpackAllValuesNative(srcAddr, valueCount, bitWidth, minValue, destAddr);
            return;
        }
        long buffer = 0;
        int bufferBits = 0;
        int srcOffset = 0;
        long mask = (1L << bitWidth) - 1;
        if (bitWidth == 64) {
            mask = -1L;
        }
        int totalBytes = packedDataSize(valueCount, bitWidth);

        long spillBits = 0;
        int spillCount = 0;
        for (int i = 0; i < valueCount; i++) {
            // Ensure we have enough bits in buffer
            if (bufferBits < bitWidth) {
                if (bufferBits == 0 && srcOffset + 8 <= totalBytes) {
                    // Fast path: buffer is empty and at least 8 bytes remain — read a full long
                    buffer = Unsafe.getUnsafe().getLong(srcAddr + srcOffset);
                    bufferBits = 64;
                    srcOffset += 8;
                } else {
                    // Slow path: read byte-by-byte near the end or when buffer has residual bits.
                    while (bufferBits < bitWidth && srcOffset < totalBytes) {
                        long b = Unsafe.getUnsafe().getByte(srcAddr + srcOffset) & 0xFFL;
                        srcOffset++;
                        if (bufferBits <= 56) {
                            buffer |= (b << bufferBits);
                            bufferBits += 8;
                        } else {
                            // bufferBits > 56: only (64 - bufferBits) bits of the byte fit.
                            int fitBits = 64 - bufferBits;
                            buffer |= (b << bufferBits);
                            // Save the high bits that didn't fit for the next value
                            spillBits = b >>> fitBits;
                            spillCount = 8 - fitBits;
                            bufferBits = 64;
                        }
                    }
                }
            }

            // Extract offset and add minValue
            long offset = buffer & mask;
            Unsafe.getUnsafe().putLong(destAddr + (long) i * Long.BYTES, minValue + offset);

            // Remove used bits
            if (bitWidth < 64) {
                buffer >>>= bitWidth;
            } else {
                buffer = 0;
            }
            bufferBits -= bitWidth;

            // Fold in any spill bits from a byte that didn't fully fit
            if (spillCount > 0) {
                buffer |= (spillBits << bufferBits);
                bufferBits += spillCount;
                spillCount = 0;
            }
        }
    }

    /**
     * Unpacks a single value from bit-packed data using Unsafe.
     *
     * @param srcAddr  source memory address of packed data
     * @param index    index of value to unpack (0-based)
     * @param bitWidth bits per offset
     * @param minValue minimum value to add back
     * @return unpacked value
     */
    public static long unpackValue(long srcAddr, int index, int bitWidth, long minValue) {
        long bitOffset = (long) index * bitWidth;
        int byteOffset = (int) (bitOffset / 8);
        int bitShift = (int) (bitOffset % 8);

        // First byte contributes (8 - bitShift) bits after dropping the skipped prefix.
        // Subsequent bytes each contribute 8 bits at the next free position. This keeps
        // the maximum left-shift at <= 56, avoiding the Java shift-mod-64 overflow that
        // would otherwise drop the high bits when bitShift + bitWidth > 64.
        long value = (Unsafe.getUnsafe().getByte(srcAddr + byteOffset) & 0xFFL) >>> bitShift;
        int valueBits = 8 - bitShift;
        int byteIdx = 1;
        while (valueBits < bitWidth) {
            long b = Unsafe.getUnsafe().getByte(srcAddr + byteOffset + byteIdx) & 0xFFL;
            value |= (b << valueBits);
            valueBits += 8;
            byteIdx++;
        }

        long mask = (bitWidth == 64) ? -1L : (1L << bitWidth) - 1;
        return minValue + (value & mask);
    }

    /**
     * Batch-unpacks values from bit-packed data starting at an arbitrary index.
     * Uses native AVX2 path when available for byte-aligned widths (8/16/32-bit),
     * falling back to Java scalar for non-aligned widths.
     *
     * @param srcAddr    source memory address of packed data
     * @param startIndex index of the first value to unpack (0-based)
     * @param valueCount number of values to unpack
     * @param bitWidth   bits per offset
     * @param minValue   minimum value to add back
     * @param destAddr   destination memory address for unpacked values
     */
    public static void unpackValuesFrom(long srcAddr, int startIndex, int valueCount, int bitWidth, long minValue, long destAddr) {
        if (valueCount == 0) {
            return;
        }
        // Dispatch to native for all widths 1-32 — AVX2 widen+store is faster than Java Unsafe.putLong.
        if (PostingIndexNative.isNativeAvailable() && bitWidth > 0 && bitWidth <= 32) {
            PostingIndexNative.unpackValuesFrom(srcAddr, startIndex, valueCount, bitWidth, minValue, destAddr);
            return;
        }
        long mask = (1L << bitWidth) - 1;
        if (bitWidth == 64) {
            mask = -1L;
        }

        // Total packed data size (needed for bounds check on long reads)
        int totalBytes = packedDataSize(startIndex + valueCount, bitWidth);

        // Seek to the byte containing the first value's bits
        long bitPos = (long) startIndex * bitWidth;
        int srcOffset = (int) (bitPos / 8);
        int skipBits = (int) (bitPos % 8);

        // Pre-fill buffer past the skip bits. Cap reads at 64 bufferBits so
        // a 9th byte never collides with `b << 64` (which is a no-op in
        // Java because the shift count is masked to 6 bits and would
        // silently corrupt the low byte). After >>> skipBits the buffer
        // can carry at most 64-skipBits useful bits; the main loop's
        // spill mechanism reads any remaining bytes for the first value.
        long buffer = 0;
        int bufferBits = 0;
        if (skipBits == 0 && srcOffset + 8 <= totalBytes) {
            // Fast path: aligned start, read a full long
            buffer = Unsafe.getUnsafe().getLong(srcAddr + srcOffset);
            bufferBits = 64;
            srcOffset += 8;
        } else {
            int target = Math.min(skipBits + bitWidth, 64);
            while (bufferBits < target && srcOffset < totalBytes) {
                buffer |= ((Unsafe.getUnsafe().getByte(srcAddr + srcOffset) & 0xFFL) << bufferBits);
                bufferBits += 8;
                srcOffset++;
            }
            buffer >>>= skipBits;
            bufferBits -= skipBits;
        }

        long spillBits = 0;
        int spillCount = 0;
        for (int i = 0; i < valueCount; i++) {
            if (bufferBits < bitWidth) {
                if (bufferBits == 0 && srcOffset + 8 <= totalBytes) {
                    buffer = Unsafe.getUnsafe().getLong(srcAddr + srcOffset);
                    bufferBits = 64;
                    srcOffset += 8;
                } else {
                    while (bufferBits < bitWidth && srcOffset < totalBytes) {
                        long b = Unsafe.getUnsafe().getByte(srcAddr + srcOffset) & 0xFFL;
                        srcOffset++;
                        if (bufferBits <= 56) {
                            buffer |= (b << bufferBits);
                            bufferBits += 8;
                        } else {
                            int fitBits = 64 - bufferBits;
                            buffer |= (b << bufferBits);
                            spillBits = b >>> fitBits;
                            spillCount = 8 - fitBits;
                            bufferBits = 64;
                        }
                    }
                }
            }
            Unsafe.getUnsafe().putLong(destAddr + (long) i * Long.BYTES, minValue + (buffer & mask));
            if (bitWidth < 64) {
                buffer >>>= bitWidth;
            } else {
                buffer = 0;
            }
            bufferBits -= bitWidth;

            if (spillCount > 0) {
                buffer |= (spillBits << bufferBits);
                bufferBits += spillCount;
                spillCount = 0;
            }
        }
    }

    /**
     * Generates the value file name for a FOR bitmap index.
     */
    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".fv");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }
}
