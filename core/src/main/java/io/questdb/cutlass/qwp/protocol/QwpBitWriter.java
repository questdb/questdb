/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.Unsafe;

/**
 * Bit-level writer for ILP v4 protocol.
 * <p>
 * This class writes bits to a buffer in LSB-first order within each byte.
 * Bits are packed sequentially, spanning byte boundaries as needed.
 * <p>
 * The implementation buffers up to 64 bits before flushing to the output buffer
 * to minimize memory operations. All writes are to direct memory for performance.
 * <p>
 * Usage pattern:
 * <pre>
 * QwpBitWriter writer = new QwpBitWriter();
 * writer.reset(address, capacity);
 * writer.writeBits(value, numBits);
 * writer.writeBits(value2, numBits2);
 * writer.flush(); // must call before reading output
 * long bytesWritten = writer.getPosition() - address;
 * </pre>
 */
public class QwpBitWriter {

    private long startAddress;
    private long currentAddress;
    private long endAddress;

    // Buffer for accumulating bits before writing
    private long bitBuffer;
    // Number of bits currently in the buffer (0-63)
    private int bitsInBuffer;

    /**
     * Creates a new bit writer. Call {@link #reset} before use.
     */
    public QwpBitWriter() {
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
     * Returns the current write position (address).
     * Note: Call {@link #flush()} first to ensure all buffered bits are written.
     *
     * @return the current address after all written data
     */
    public long getPosition() {
        return currentAddress;
    }

    /**
     * Returns the number of bits that have been written (including buffered bits).
     *
     * @return total bits written since reset
     */
    public long getTotalBitsWritten() {
        return (currentAddress - startAddress) * 8L + bitsInBuffer;
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
     * Writes multiple bits from the given value.
     * <p>
     * Bits are taken from the LSB of the value. For example, if value=0b1101
     * and numBits=4, the bits written are 1, 0, 1, 1 (LSB to MSB order).
     *
     * @param value   the value containing the bits (LSB-aligned)
     * @param numBits number of bits to write (1-64)
     */
    public void writeBits(long value, int numBits) {
        if (numBits <= 0 || numBits > 64) {
            return;
        }

        // Mask the value to only include the requested bits
        if (numBits < 64) {
            value &= (1L << numBits) - 1;
        }

        int bitsToWrite = numBits;

        while (bitsToWrite > 0) {
            // How many bits can we fit in current buffer (max 64 total)
            int availableInBuffer = 64 - bitsInBuffer;
            int bitsThisRound = Math.min(bitsToWrite, availableInBuffer);

            // Add bits to the buffer
            long mask = bitsThisRound == 64 ? -1L : (1L << bitsThisRound) - 1;
            bitBuffer |= (value & mask) << bitsInBuffer;
            bitsInBuffer += bitsThisRound;
            value >>>= bitsThisRound;
            bitsToWrite -= bitsThisRound;

            // Flush complete bytes from the buffer
            while (bitsInBuffer >= 8) {
                if (currentAddress < endAddress) {
                    Unsafe.getUnsafe().putByte(currentAddress++, (byte) bitBuffer);
                }
                bitBuffer >>>= 8;
                bitsInBuffer -= 8;
            }
        }
    }

    /**
     * Writes a signed value using two's complement representation.
     *
     * @param value   the signed value
     * @param numBits number of bits to use for the representation
     */
    public void writeSigned(long value, int numBits) {
        // Two's complement is automatic in Java for the bit pattern
        writeBits(value, numBits);
    }

    /**
     * Flushes any remaining bits in the buffer to memory.
     * <p>
     * If there are partial bits (less than 8), they are written as the last byte
     * with the remaining high bits set to zero.
     * <p>
     * Must be called before reading the output or getting the final position.
     */
    public void flush() {
        if (bitsInBuffer > 0 && currentAddress < endAddress) {
            Unsafe.getUnsafe().putByte(currentAddress++, (byte) bitBuffer);
            bitBuffer = 0;
            bitsInBuffer = 0;
        }
    }

    /**
     * Finishes writing and returns the number of bytes written since reset.
     * <p>
     * This method flushes any remaining bits and returns the total byte count.
     *
     * @return bytes written since reset
     */
    public int finish() {
        flush();
        return (int) (currentAddress - startAddress);
    }

    /**
     * Returns the number of bits remaining in the partial byte buffer.
     * This is 0 after a flush or when aligned on a byte boundary.
     *
     * @return bits in buffer (0-7)
     */
    public int getBitsInBuffer() {
        return bitsInBuffer;
    }

    /**
     * Aligns the writer to the next byte boundary by padding with zeros.
     * If already byte-aligned, this is a no-op.
     */
    public void alignToByte() {
        if (bitsInBuffer > 0) {
            flush();
        }
    }

    /**
     * Writes a complete byte, ensuring byte alignment first.
     *
     * @param value the byte value
     */
    public void writeByte(int value) {
        alignToByte();
        if (currentAddress < endAddress) {
            Unsafe.getUnsafe().putByte(currentAddress++, (byte) value);
        }
    }

    /**
     * Writes a complete 32-bit integer in little-endian order, ensuring byte alignment first.
     *
     * @param value the integer value
     */
    public void writeInt(int value) {
        alignToByte();
        if (currentAddress + 4 <= endAddress) {
            Unsafe.getUnsafe().putInt(currentAddress, value);
            currentAddress += 4;
        }
    }

    /**
     * Writes a complete 64-bit long in little-endian order, ensuring byte alignment first.
     *
     * @param value the long value
     */
    public void writeLong(long value) {
        alignToByte();
        if (currentAddress + 8 <= endAddress) {
            Unsafe.getUnsafe().putLong(currentAddress, value);
            currentAddress += 8;
        }
    }
}
