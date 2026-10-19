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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoException;
import io.questdb.cutlass.qwp.protocol.QwpBitReader;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class QwpBitReaderTest {

    @Test
    public void testPeekBit() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeBits(0b101, 3);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            // Peek should return first bit (1) without consuming
            Assert.assertEquals(1, reader.peekBit());
            Assert.assertEquals(1, reader.peekBit()); // Still 1

            // Now read to consume
            Assert.assertEquals(1, reader.readBit());

            // Peek next bit (0)
            Assert.assertEquals(0, reader.peekBit());
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPeekBitAtEnd() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeBits(0xFF, 8);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            // Consume all 8 bits
            reader.readBits(8);

            // Peek should return -1 when all bits are consumed
            Assert.assertEquals(-1, reader.peekBit());
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRandomRoundTrip() throws QwpParseException {
        long addr = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
        try {
            Random random = new Random(42);
            QwpBitWriter writer = new QwpBitWriter();
            QwpBitReader reader = new QwpBitReader();

            // Store what we wrote
            int[] bitWidths = new int[100];
            long[] values = new long[100];

            writer.reset(addr, 1024);
            for (int i = 0; i < 100; i++) {
                int bits = random.nextInt(32) + 1; // 1-32 bits
                long value = random.nextLong() & ((1L << bits) - 1);
                bitWidths[i] = bits;
                values[i] = value;
                writer.writeBits(value, bits);
            }
            writer.flush();

            // Read back and verify
            reader.reset(addr, writer.getPosition() - addr);
            for (int i = 0; i < 100; i++) {
                long readValue = reader.readBits(bitWidths[i]);
                Assert.assertEquals("Mismatch at index " + i, values[i], readValue);
            }
        } finally {
            Unsafe.free(addr, 1024, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReadBeyondWritten() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeBits(0xFF, 8);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            // Read the 8 bits successfully
            reader.readBits(8);

            // Now try to read more - should fail
            try {
                reader.readBit();
                Assert.fail("Should have thrown exception");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.BIT_READ_OVERFLOW, e.getErrorCode());
            }
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReadBitsOverflow() {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeBits(0xFF, 8);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            try {
                reader.readBits(16); // only 8 bits available
                Assert.fail("Should have thrown QwpParseException");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.BIT_READ_OVERFLOW, e.getErrorCode());
            }
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReadBitsReturnsZeroForZeroBits() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeBits(0xFF, 8);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            Assert.assertEquals(0, reader.readBits(0));
            Assert.assertEquals(0, reader.readBits(-1));
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReadBitsThrowsAbove64() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeBits(0xFF, 8);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            boolean assertErrorThrown = false;
            try {
                reader.readBits(65);
            } catch (AssertionError e) {
                assertErrorThrown = true;
            }
            if (!assertErrorThrown) {
                Assert.fail("Should have thrown AssertionError");
            }
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReadSigned64Bits() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeSigned(-1L, 64);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            // numBits == 64 skips sign extension; the raw 64-bit value is returned
            Assert.assertEquals(-1L, reader.readSigned(64));
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRoundTripMixedBitWidths() throws QwpParseException {
        long addr = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitReader reader = getQwpBitReader(addr);

            Assert.assertEquals(0, reader.readBit());
            Assert.assertEquals(0b10, reader.readBits(2));
            Assert.assertEquals(-5, reader.readSigned(7));
            Assert.assertEquals(0b110, reader.readBits(3));
            Assert.assertEquals(100, reader.readSigned(9));
            Assert.assertEquals(0b1111, reader.readBits(4));
            Assert.assertEquals(-1_000_000, reader.readSigned(32));
        } finally {
            Unsafe.free(addr, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSkipBits() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeBits(0xFF, 8);
            writer.writeBits(0xAB, 8);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            // Skip first 8 bits
            reader.skipBits(8);

            // Should now read the second byte
            Assert.assertEquals(0xAB, reader.readBits(8));
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSkipBitsFastPath() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            // Write pattern: 3 bits (101) + 3 bits to skip (010) + 4 bits (1100)
            writer.writeBits(0b101, 3);
            writer.writeBits(0b010, 3);
            writer.writeBits(0b1100, 4);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            // Read first 3 bits to load bytes into the buffer
            Assert.assertEquals(0b101, reader.readBits(3));

            // Skip 3 bits (fast path: these are already in the buffer)
            reader.skipBits(3);

            // Verify we're at the right position by reading the next 4 bits
            Assert.assertEquals(0b1100, reader.readBits(4));
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSkipBitsOverflow() {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeBits(0xFF, 8);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            try {
                reader.skipBits(16); // only 8 bits available
                Assert.fail("Should have thrown QwpParseException");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.BIT_READ_OVERFLOW, e.getErrorCode());
            }
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSkipBitsWithRemainder() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            // Write 24 bits: 13 to skip, then 11 to read
            writer.writeBits(0, 13);
            writer.writeBits(0b10110100101, 11);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            // Skip 13 bits: bytesToSkip=1, remainingBits=5
            reader.skipBits(13);

            Assert.assertEquals(13, reader.getBitPosition());
            Assert.assertEquals(0b10110100101, reader.readBits(11));
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTotalBitsTracking() throws QwpParseException {
        long addr = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 64);

            writer.writeBits(0, 5);
            Assert.assertEquals(5, writer.getTotalBitsWritten());

            writer.writeBits(0, 10);
            Assert.assertEquals(15, writer.getTotalBitsWritten());

            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            Assert.assertEquals(0, reader.getBitPosition());
            reader.readBits(5);
            Assert.assertEquals(5, reader.getBitPosition());
            reader.readBits(10);
            Assert.assertEquals(15, reader.getBitPosition());
        } finally {
            Unsafe.free(addr, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteBitsThrowsOnOverflow() {
        long ptr = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(ptr, 4);
            // Fill the buffer (32 bits = 4 bytes)
            writer.writeBits(0xFFFF_FFFFL, 32);
            // Next write should throw — buffer is full
            try {
                writer.writeBits(1, 8);
                Assert.fail("expected CairoException on buffer overflow");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("buffer overflow"));
            }
        } finally {
            Unsafe.free(ptr, 4, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteBitsWithinCapacitySucceeds() {
        long ptr = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(ptr, 8);
            writer.writeBits(0xDEAD_BEEF_CAFE_BABEL, 64);
            writer.flush();
            Assert.assertEquals(8, writer.getPosition() - ptr);
            Assert.assertEquals(0xDEAD_BEEF_CAFE_BABEL, Unsafe.getLong(ptr));
        } finally {
            Unsafe.free(ptr, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteByteThrowsOnOverflow() {
        long ptr = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(ptr, 1);
            writer.writeByte(0x42);
            try {
                writer.writeByte(0x43);
                Assert.fail("expected CairoException on buffer overflow");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("buffer overflow"));
            }
        } finally {
            Unsafe.free(ptr, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteIntThrowsOnOverflow() {
        long ptr = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(ptr, 4);
            writer.writeInt(42);
            try {
                writer.writeInt(99);
                Assert.fail("expected CairoException on buffer overflow");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("buffer overflow"));
            }
        } finally {
            Unsafe.free(ptr, 4, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteLongThrowsOnOverflow() {
        long ptr = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(ptr, 8);
            writer.writeLong(42L);
            try {
                writer.writeLong(99L);
                Assert.fail("expected CairoException on buffer overflow");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("buffer overflow"));
            }
        } finally {
            Unsafe.free(ptr, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteRead64Bits() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);

            long value = 0x123456789ABCDEF0L;
            writer.writeBits(value, 64);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            Assert.assertEquals(value, reader.readBits(64));
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteReadCrossByteBoundary() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);

            // Write 12 bits (crosses byte boundary)
            writer.writeBits(0xABC, 12);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            Assert.assertEquals(0xABC, reader.readBits(12));
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteReadMultipleBits() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);

            // Write 4 bits: 1101 (13)
            writer.writeBits(0b1101, 4);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            // Read should return the same value
            long value = reader.readBits(4);
            Assert.assertEquals(0b1101, value);
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteReadSignedValues() throws QwpParseException {
        long addr = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitReader reader = getBitReader(addr);

            Assert.assertEquals(-1, reader.readSigned(7));
            Assert.assertEquals(5, reader.readSigned(7));
            Assert.assertEquals(-100, reader.readSigned(9));
        } finally {
            Unsafe.free(addr, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteReadSingleBit() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);
            writer.writeBit(1);
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);
            Assert.assertEquals(1, reader.readBit());
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static @NotNull QwpBitReader getBitReader(long addr) {
        QwpBitWriter writer = new QwpBitWriter();
        writer.reset(addr, 32);

        // Write some signed values using different bit widths
        writer.writeSigned(-1, 7);   // -1 in 7-bit two's complement
        writer.writeSigned(5, 7);    // 5 in 7-bit two's complement
        writer.writeSigned(-100, 9); // -100 in 9-bit two's complement
        writer.flush();

        QwpBitReader reader = new QwpBitReader();
        reader.reset(addr, writer.getPosition() - addr);
        return reader;
    }

    private static @NotNull QwpBitReader getQwpBitReader(long addr) {
        QwpBitWriter writer = new QwpBitWriter();
        writer.reset(addr, 64);

        // Write various bit widths (simulating Gorilla timestamp encoding)
        writer.writeBit(0);           // 1 bit: delta==0 indicator
        writer.writeBits(0b10, 2);    // 2 bits: prefix for 7-bit bucket
        writer.writeSigned(-5, 7);    // 7 bits: signed delta
        writer.writeBits(0b110, 3);   // 3 bits: prefix for 9-bit bucket
        writer.writeSigned(100, 9);   // 9 bits: signed delta
        writer.writeBits(0b1111, 4);  // 4 bits: prefix for 32-bit bucket
        writer.writeSigned(-1_000_000, 32); // 32 bits: signed delta
        writer.flush();

        QwpBitReader reader = new QwpBitReader();
        reader.reset(addr, writer.getPosition() - addr);
        return reader;
    }

    private static class QwpBitWriter {

        private long bitBuffer;
        private int bitsInBuffer;
        private long currentAddress;
        private long endAddress;
        private long startAddress;

        void alignToByte() {
            if (bitsInBuffer > 0) {
                flush();
            }
        }

        void flush() {
            if (bitsInBuffer > 0) {
                if (currentAddress >= endAddress) {
                    throw CairoException.critical(0).put("QwpBitWriter buffer overflow");
                }
                Unsafe.putByte(currentAddress++, (byte) bitBuffer);
                bitBuffer = 0;
                bitsInBuffer = 0;
            }
        }

        long getPosition() {
            return currentAddress;
        }

        long getTotalBitsWritten() {
            return (currentAddress - startAddress) * 8L + bitsInBuffer;
        }

        void reset(long address, long capacity) {
            this.startAddress = address;
            this.currentAddress = address;
            this.endAddress = address + capacity;
            this.bitBuffer = 0;
            this.bitsInBuffer = 0;
        }

        void writeBit(int bit) {
            writeBits(bit & 1, 1);
        }

        void writeBits(long value, int numBits) {
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
                    if (currentAddress >= endAddress) {
                        throw CairoException.critical(0).put("QwpBitWriter buffer overflow");
                    }
                    Unsafe.putByte(currentAddress++, (byte) bitBuffer);
                    bitBuffer >>>= 8;
                    bitsInBuffer -= 8;
                }
            }
        }

        void writeByte(int value) {
            alignToByte();
            if (currentAddress >= endAddress) {
                throw CairoException.critical(0).put("QwpBitWriter buffer overflow");
            }
            Unsafe.putByte(currentAddress++, (byte) value);
        }

        void writeInt(int value) {
            alignToByte();
            if (currentAddress + 4 > endAddress) {
                throw CairoException.critical(0).put("QwpBitWriter buffer overflow");
            }
            Unsafe.putInt(currentAddress, value);
            currentAddress += 4;
        }

        void writeLong(long value) {
            alignToByte();
            if (currentAddress + 8 > endAddress) {
                throw CairoException.critical(0).put("QwpBitWriter buffer overflow");
            }
            Unsafe.putLong(currentAddress, value);
            currentAddress += 8;
        }

        void writeSigned(long value, int numBits) {
            writeBits(value, numBits);
        }
    }
}
