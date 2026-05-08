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

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

/**
 * Bit-level reader for QWP v1 protocol.
 * <p>
 * This class reads bits from a buffer in LSB-first order within each byte.
 * Bits are read sequentially, spanning byte boundaries as needed.
 * <p>
 * The implementation buffers bytes to minimize memory reads.
 * <p>
 * Usage pattern:
 * <pre>
 * QwpBitReader reader = new QwpBitReader();
 * reader.reset(address, length);
 * int bit = reader.readBit();
 * long value = reader.readBits(numBits);
 * long signedValue = reader.readSigned(numBits);
 * </pre>
 */
public class QwpBitReader {

    // Buffer for reading bits
    private long bitBuffer;
    // Number of bits currently available in the buffer (0-64)
    private int bitsInBuffer;
    private long currentAddress;
    private long endAddress;
    // Total bits available for reading (from reset)
    private long totalBitsAvailable;
    // Total bits already consumed
    private long totalBitsRead;

    /**
     * Creates a new bit reader. Call {@link #reset} before use.
     */
    public QwpBitReader() {
    }

    /**
     * Returns the current position in bits from the start.
     *
     * @return bits read since reset
     */
    public long getBitPosition() {
        return totalBitsRead;
    }

    /**
     * Peeks at the next bit without consuming it.
     *
     * @return 0 or 1, or -1 if no more bits
     */
    @TestOnly
    public int peekBit() {
        if (totalBitsRead >= totalBitsAvailable) {
            return -1;
        }
        if (!ensureBits(1)) {
            return -1;
        }
        return (int) (bitBuffer & 1);
    }

    /**
     * Reads a single bit.
     *
     * @return 0 or 1
     * @throws QwpParseException if no more bits available
     */
    public int readBit() throws QwpParseException {
        if (totalBitsRead >= totalBitsAvailable) {
            throw QwpParseException.bitReadOverflow();
        }
        if (!ensureBits(1)) {
            throw QwpParseException.bitReadOverflow();
        }

        int bit = (int) (bitBuffer & 1);
        bitBuffer >>>= 1;
        bitsInBuffer--;
        totalBitsRead++;
        return bit;
    }

    /**
     * Reads multiple bits and returns them as a long (unsigned).
     * <p>
     * Bits are returned LSB-aligned. For example, reading 4 bits might return
     * 0b1101 where bit 0 is the first bit read.
     *
     * @param numBits number of bits to read (1-64)
     * @return the value formed by the bits (unsigned)
     * @throws QwpParseException if not enough bits available
     */
    public long readBits(int numBits) throws QwpParseException {
        if (numBits <= 0) {
            return 0;
        }
        if (numBits > 64) {
            throw new AssertionError("Asked to read more than 64 bits into a long");
        }
        if (totalBitsRead + numBits > totalBitsAvailable) {
            throw QwpParseException.bitReadOverflow();
        }

        long result = 0;
        int bitsRemaining = numBits;
        int resultShift = 0;

        while (bitsRemaining > 0) {
            if (bitsInBuffer == 0) {
                if (!ensureBits(Math.min(bitsRemaining, 64))) {
                    throw QwpParseException.bitReadOverflow();
                }
            }

            int bitsToTake = Math.min(bitsRemaining, bitsInBuffer);
            long mask = bitsToTake == 64 ? -1L : (1L << bitsToTake) - 1;
            result |= (bitBuffer & mask) << resultShift;

            bitBuffer >>>= bitsToTake;
            bitsInBuffer -= bitsToTake;
            bitsRemaining -= bitsToTake;
            resultShift += bitsToTake;
        }

        totalBitsRead += numBits;
        return result;
    }

    /**
     * Reads multiple bits and interprets them as a signed value using two's complement.
     *
     * @param numBits number of bits to read (1-64)
     * @return the signed value
     * @throws QwpParseException if not enough bits available
     */
    public long readSigned(int numBits) throws QwpParseException {
        long unsigned = readBits(numBits);
        // Sign extend: if the high bit (bit numBits-1) is set, extend the sign
        if (numBits < 64 && (unsigned & (1L << (numBits - 1))) != 0) {
            // Set all bits above numBits to 1
            unsigned |= -1L << numBits;
        }
        return unsigned;
    }

    /**
     * Resets the reader to read from the specified memory region.
     *
     * @param address the starting address
     * @param length  the number of bytes available to read
     */
    public void reset(long address, long length) {
        this.currentAddress = address;
        this.endAddress = address + length;
        this.bitBuffer = 0;
        this.bitsInBuffer = 0;
        this.totalBitsAvailable = length * 8L;
        this.totalBitsRead = 0;
    }

    /**
     * Skips the specified number of bits.
     *
     * @param numBits bits to skip
     * @throws QwpParseException if not enough bits available
     */
    @TestOnly
    public void skipBits(int numBits) throws QwpParseException {
        if (totalBitsRead + numBits > totalBitsAvailable) {
            throw QwpParseException.bitReadOverflow();
        }

        // Fast path: skip bits in current buffer
        if (numBits <= bitsInBuffer) {
            bitBuffer >>>= numBits;
            bitsInBuffer -= numBits;
            totalBitsRead += numBits;
            return;
        }

        // Consume all buffered bits
        int bitsToSkip = numBits - bitsInBuffer;
        totalBitsRead += bitsInBuffer;
        bitsInBuffer = 0;
        bitBuffer = 0;

        // Skip whole bytes
        int bytesToSkip = bitsToSkip / 8;
        currentAddress += bytesToSkip;
        totalBitsRead += bytesToSkip * 8L;

        // Handle remaining bits
        int remainingBits = bitsToSkip % 8;
        if (remainingBits > 0) {
            ensureBits(remainingBits);
            bitBuffer >>>= remainingBits;
            bitsInBuffer -= remainingBits;
            totalBitsRead += remainingBits;
        }
    }

    /**
     * Ensures the buffer has at least the requested number of bits.
     * Loads more bytes from memory if needed.
     *
     * @param bitsNeeded minimum bits required in buffer
     * @return true if sufficient bits available, false otherwise
     */
    private boolean ensureBits(int bitsNeeded) {
        while (bitsInBuffer < bitsNeeded && bitsInBuffer <= 56 && currentAddress < endAddress) {
            byte b = Unsafe.getByte(currentAddress++);
            bitBuffer |= (long) (b & 0xFF) << bitsInBuffer;
            bitsInBuffer += 8;
        }
        return bitsInBuffer >= bitsNeeded;
    }
}
