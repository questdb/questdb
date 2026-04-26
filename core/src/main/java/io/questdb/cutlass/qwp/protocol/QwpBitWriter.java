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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.cairo.CairoException;
import io.questdb.std.Unsafe;

/**
 * Bit-level writer for QWP v1 protocol (server-side mirror of the client
 * {@code QwpBitWriter}). Used by {@link QwpGorillaEncoder} to emit Gorilla
 * delta-of-delta compressed timestamp columns in {@code RESULT_BATCH} frames.
 * <p>
 * Bits are packed LSB-first within each byte. The buffer accumulates up to 64
 * bits before flushing. All writes target direct memory.
 * <p>
 * Overflow raises {@link CairoException} so the egress error classifier maps
 * it to {@code STATUS_INTERNAL_ERROR}. Callers must pre-validate the required
 * byte count via {@link QwpGorillaEncoder#calculateEncodedSizeIfSupported} and
 * allocate the buffer accordingly; a thrown exception indicates an encoder
 * bug, not user data corruption.
 */
public class QwpBitWriter {

    // Buffer for accumulating bits before writing
    private long bitBuffer;
    // Number of bits currently in the buffer (0-63)
    private int bitsInBuffer;
    private long currentAddress;
    private long endAddress;
    private long startAddress;

    /**
     * Creates a new bit writer. Call {@link #reset} before use.
     */
    public QwpBitWriter() {
    }

    /**
     * Finishes writing and returns the number of bytes written since reset.
     */
    public int finish() {
        flush();
        return (int) (currentAddress - startAddress);
    }

    /**
     * Flushes any remaining bits in the buffer to memory. Partial trailing bits
     * get padded with zeros to byte alignment. Must be called before reading
     * output or getting the final position.
     */
    public void flush() {
        if (bitsInBuffer > 0) {
            if (currentAddress >= endAddress) {
                throw CairoException.critical(0).put("QWP egress: QwpBitWriter buffer overflow on flush");
            }
            Unsafe.putByte(currentAddress++, (byte) bitBuffer);
            bitBuffer = 0;
            bitsInBuffer = 0;
        }
    }

    /**
     * Returns the current write position (address). Call {@link #flush} first
     * to ensure all buffered bits are materialised.
     */
    public long getPosition() {
        return currentAddress;
    }

    /**
     * Resets the writer to write to the specified memory region.
     *
     * @param address  the starting address
     * @param capacity the maximum number of bytes to write
     */
    public void reset(long address, long capacity) {
        this.startAddress = address;
        this.currentAddress = address;
        this.endAddress = address + capacity;
        this.bitBuffer = 0;
        this.bitsInBuffer = 0;
    }

    /**
     * Writes a single bit.
     *
     * @param bit the bit value (0 or 1, only LSB is used)
     */
    public void writeBit(int bit) {
        writeBits(bit & 1, 1);
    }

    /**
     * Writes multiple bits from the given value (LSB-first).
     *
     * @param value   the value containing the bits (LSB-aligned)
     * @param numBits number of bits to write (1-64)
     */
    public void writeBits(long value, int numBits) {
        if (numBits <= 0 || numBits > 64) {
            throw CairoException.critical(0)
                    .put("QWP egress: QwpBitWriter writeBits numBits out of range [numBits=")
                    .put(numBits).put(']');
        }

        if (numBits < 64) {
            value &= (1L << numBits) - 1;
        }

        int bitsToWrite = numBits;

        while (bitsToWrite > 0) {
            int availableInBuffer = 64 - bitsInBuffer;
            int bitsThisRound = Math.min(bitsToWrite, availableInBuffer);

            long mask = bitsThisRound == 64 ? -1L : (1L << bitsThisRound) - 1;
            bitBuffer |= (value & mask) << bitsInBuffer;
            bitsInBuffer += bitsThisRound;
            value >>>= bitsThisRound;
            bitsToWrite -= bitsThisRound;

            while (bitsInBuffer >= 8) {
                if (currentAddress >= endAddress) {
                    throw CairoException.critical(0).put("QWP egress: QwpBitWriter buffer overflow on write");
                }
                Unsafe.putByte(currentAddress++, (byte) bitBuffer);
                bitBuffer >>>= 8;
                bitsInBuffer -= 8;
            }
        }
    }

    /**
     * Writes a signed value using two's complement representation.
     */
    public void writeSigned(long value, int numBits) {
        // Two's complement is automatic in Java for the bit pattern
        writeBits(value, numBits);
    }
}
