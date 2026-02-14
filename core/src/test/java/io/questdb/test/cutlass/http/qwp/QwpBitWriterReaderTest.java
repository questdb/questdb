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

package io.questdb.test.cutlass.http.qwp;

import io.questdb.cutlass.qwp.protocol.QwpBitReader;
import io.questdb.cutlass.qwp.protocol.QwpBitWriter;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class QwpBitWriterReaderTest {

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
    public void testWriteReadSignedValues() throws QwpParseException {
        long addr = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 32);

            // Write some signed values using different bit widths
            writer.writeSigned(-1, 7);   // -1 in 7-bit two's complement
            writer.writeSigned(5, 7);    // 5 in 7-bit two's complement
            writer.writeSigned(-100, 9); // -100 in 9-bit two's complement
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            Assert.assertEquals(-1, reader.readSigned(7));
            Assert.assertEquals(5, reader.readSigned(7));
            Assert.assertEquals(-100, reader.readSigned(9));
        } finally {
            Unsafe.free(addr, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBitAlignment() {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);

            // Write 5 bits, then align to byte
            writer.writeBits(0b10101, 5);
            writer.alignToByte();

            // Now write a full byte
            writer.writeByte(0x42);
            writer.flush();

            // Verify the alignment worked
            Assert.assertEquals(2, writer.getPosition() - addr);
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFlushPartialByte() {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);

            // Write only 3 bits
            writer.writeBits(0b101, 3);
            Assert.assertEquals(3, writer.getBitsInBuffer());

            writer.flush();
            Assert.assertEquals(0, writer.getBitsInBuffer());

            // Verify we wrote 1 byte
            Assert.assertEquals(1, writer.getPosition() - addr);

            // Verify the byte value (3 LSB bits should be 101, upper bits 0)
            Assert.assertEquals(0b00000101, Unsafe.getUnsafe().getByte(addr) & 0xFF);
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
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
    public void testRoundTripMixedBitWidths() throws QwpParseException {
        long addr = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 64);

            // Write various bit widths (simulating Gorilla timestamp encoding)
            writer.writeBit(0);           // 1 bit: delta==0 indicator
            writer.writeBits(0b10, 2);    // 2 bits: prefix for 7-bit bucket
            writer.writeSigned(-5, 7);    // 7 bits: signed delta
            writer.writeBits(0b110, 3);   // 3 bits: prefix for 9-bit bucket
            writer.writeSigned(100, 9);   // 9 bits: signed delta
            writer.writeBits(0b1111, 4);  // 4 bits: prefix for 32-bit bucket
            writer.writeSigned(-1000000, 32); // 32 bits: signed delta
            writer.flush();

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            Assert.assertEquals(0, reader.readBit());
            Assert.assertEquals(0b10, reader.readBits(2));
            Assert.assertEquals(-5, reader.readSigned(7));
            Assert.assertEquals(0b110, reader.readBits(3));
            Assert.assertEquals(100, reader.readSigned(9));
            Assert.assertEquals(0b1111, reader.readBits(4));
            Assert.assertEquals(-1000000, reader.readSigned(32));
        } finally {
            Unsafe.free(addr, 64, MemoryTag.NATIVE_DEFAULT);
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
    public void testWriteLongValue() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);

            long value = 0xDEADBEEFCAFEBABEL;
            writer.writeLong(value);

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            Assert.assertEquals(value, reader.readLong());
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteIntValue() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBitWriter writer = new QwpBitWriter();
            writer.reset(addr, 16);

            int value = 0x12345678;
            writer.writeInt(value);

            QwpBitReader reader = new QwpBitReader();
            reader.reset(addr, writer.getPosition() - addr);

            Assert.assertEquals(value, reader.readInt());
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

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
}
