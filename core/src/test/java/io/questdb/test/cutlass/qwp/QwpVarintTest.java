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

import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class QwpVarintTest {

    private final QwpVarint.DecodeResult result = new QwpVarint.DecodeResult();

    @Test
    public void testDecodeFromDirectMemory() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encode using byte array, decode from direct memory
            byte[] buf = new byte[10];
            int len = QwpVarint.encode(buf, 0, 300);

            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(addr + i, buf[i]);
            }

            long decoded = QwpVarint.decode(addr, addr + len);
            Assert.assertEquals(300, decoded);
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeIncompleteVarint() {
        // Byte with continuation bit set but no following byte
        byte[] buf = new byte[]{(byte) 0x80};
        try {
            QwpVarint.decode(buf, 0, 1, result);
            Assert.fail("Should have thrown exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INCOMPLETE_VARINT, e.getErrorCode());
        }
    }

    @Test
    public void testDecodeOverflow() {
        // Create a buffer with too many continuation bytes (>10)
        byte[] buf = new byte[12];
        for (int i = 0; i < 11; i++) {
            buf[i] = (byte) 0x80;
        }
        buf[11] = 0x01;

        try {
            QwpVarint.decode(buf, 0, 12, result);
            Assert.fail("Should have thrown exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.VARINT_OVERFLOW, e.getErrorCode());
        }
    }

    @Test
    public void testDecodeOverflowOn10thByteSilentlyDropsBits() {
        // The 10th byte is decoded at shift=63, so (long)(b & 0x7F) << 63
        // can only preserve bit 0. Bits 1-6 are shifted beyond 64-bit range
        // and silently lost. A valid 64-bit LEB128 can only have 0x00 or 0x01
        // as the 10th byte's data nibble. Any higher value represents a number
        // exceeding 64 bits and must be rejected.

        // 9 continuation bytes with zero data + 10th byte = 0x02 (data nibble = 2)
        // This encodes the value 2 * 2^63 = 2^64, which does not fit in a long.
        // Current code silently decodes it to 0 instead of throwing overflow.
        byte[] buf = new byte[]{
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x02
        };
        try {
            QwpVarint.decode(buf, 0, 10, result);
            Assert.fail("10th byte with data nibble 0x02 should overflow");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.VARINT_OVERFLOW, e.getErrorCode());
        }

        // 9 bytes of 0xFF (all data bits set + continuation) + 10th byte = 0x03.
        // With 0x01 the value is 0xFFFFFFFFFFFFFFFF (-1 unsigned), which is valid.
        // With 0x03 the value would be 0xFFFFFFFFFFFFFFFF + 2^64 — doesn't fit.
        // Current code silently decodes both 0x01 and 0x03 to the same value (-1).
        byte[] buf2 = new byte[]{
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x03
        };
        try {
            QwpVarint.decode(buf2, 0, 10, result);
            Assert.fail("10th byte with data nibble 0x03 should overflow");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.VARINT_OVERFLOW, e.getErrorCode());
        }

        // 10th byte with maximum data nibble 0x7F — clearly overflows
        byte[] buf3 = new byte[]{
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x7F
        };
        try {
            QwpVarint.decode(buf3, 0, 10, result);
            Assert.fail("10th byte with data nibble 0x7F should overflow");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.VARINT_OVERFLOW, e.getErrorCode());
        }
    }

    @Test
    public void testDecodeResult() throws QwpParseException {
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 300);

        QwpVarint.decode(buf, 0, len, result);

        Assert.assertEquals(300, result.value);
        Assert.assertEquals(len, result.bytesRead);
    }

    @Test
    public void testDecodeResultFromDirectMemory() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddr = QwpVarint.encode(addr, 999_999);
            int expectedLen = (int) (endAddr - addr);

            QwpVarint.DecodeResult result = new QwpVarint.DecodeResult();
            QwpVarint.decode(addr, endAddr, result);

            Assert.assertEquals(999_999, result.value);
            Assert.assertEquals(expectedLen, result.bytesRead);
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeResultFromDirectMemory_fastPath() throws QwpParseException {
        // Test the fast-path (single-byte, values 0-127) in decode(long, long, DecodeResult)
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpVarint.DecodeResult result = new QwpVarint.DecodeResult();

            // Value 0
            Unsafe.getUnsafe().putByte(addr, (byte) 0x00);
            QwpVarint.decode(addr, addr + 1, result);
            Assert.assertEquals(0, result.value);
            Assert.assertEquals(1, result.bytesRead);

            // Value 1
            Unsafe.getUnsafe().putByte(addr, (byte) 0x01);
            QwpVarint.decode(addr, addr + 1, result);
            Assert.assertEquals(1, result.value);
            Assert.assertEquals(1, result.bytesRead);

            // Value 127 (max single-byte)
            Unsafe.getUnsafe().putByte(addr, (byte) 0x7F);
            QwpVarint.decode(addr, addr + 1, result);
            Assert.assertEquals(127, result.value);
            Assert.assertEquals(1, result.bytesRead);

            // Value 128 (min two-byte, exercises decodeMultiByte)
            long endAddr = QwpVarint.encode(addr, 128);
            QwpVarint.decode(addr, endAddr, result);
            Assert.assertEquals(128, result.value);
            Assert.assertEquals(2, result.bytesRead);

            // Consistency: decode(long, long) and decode(long, long, DecodeResult) agree
            long[] values = {0, 1, 42, 127, 128, 255, 16_383, 16_384, 999_999};
            for (long value : values) {
                endAddr = QwpVarint.encode(addr, value);
                long direct = QwpVarint.decode(addr, endAddr);
                QwpVarint.decode(addr, endAddr, result);
                Assert.assertEquals("Mismatch for value " + value, direct, result.value);
            }
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeResultReuse() throws QwpParseException {
        byte[] buf = new byte[10];
        QwpVarint.DecodeResult result = new QwpVarint.DecodeResult();

        // First decode
        int len1 = QwpVarint.encode(buf, 0, 100);
        QwpVarint.decode(buf, 0, len1, result);
        Assert.assertEquals(100, result.value);

        // Reuse for second decode
        result.reset();
        int len2 = QwpVarint.encode(buf, 0, 50_000);
        QwpVarint.decode(buf, 0, len2, result);
        Assert.assertEquals(50_000, result.value);
    }

    @Test
    public void testDecodeValid10thByte() throws QwpParseException {
        // 10th byte with data nibble 0x01 is valid — encodes values with bit 63 set.
        // This is the encoding of Long.MIN_VALUE (0x8000000000000000 unsigned).
        byte[] buf = new byte[]{
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01
        };
        QwpVarint.decode(buf, 0, 10, result);
        Assert.assertEquals(Long.MIN_VALUE, result.value);

        // 10th byte with data nibble 0x00 is valid (redundant encoding of 0).
        byte[] buf2 = new byte[]{
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x00
        };
        QwpVarint.decode(buf2, 0, 10, result);
        Assert.assertEquals(0, result.value);
    }

    @Test
    public void testEncodeDecode127() throws QwpParseException {
        // 127 is the maximum 1-byte value
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 127);
        Assert.assertEquals(1, len);
        Assert.assertEquals(0x7F, buf[0] & 0xFF);
        QwpVarint.decode(buf, 0, len, result);
        Assert.assertEquals(127, result.value);
    }

    @Test
    public void testEncodeDecode128() throws QwpParseException {
        // 128 is the minimum 2-byte value
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 128);
        Assert.assertEquals(2, len);
        Assert.assertEquals(0x80, buf[0] & 0xFF); // 0 + continuation bit
        Assert.assertEquals(0x01, buf[1] & 0xFF); // 1
        QwpVarint.decode(buf, 0, len, result);
        Assert.assertEquals(128, result.value);
    }

    @Test
    public void testEncodeDecode16383() throws QwpParseException {
        // 16383 (0x3FFF) is the maximum 2-byte value
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 16_383);
        Assert.assertEquals(2, len);
        Assert.assertEquals(0xFF, buf[0] & 0xFF); // 127 + continuation bit
        Assert.assertEquals(0x7F, buf[1] & 0xFF); // 127
        QwpVarint.decode(buf, 0, len, result);
        Assert.assertEquals(16_383, result.value);
    }

    @Test
    public void testEncodeDecode16384() throws QwpParseException {
        // 16384 (0x4000) is the minimum 3-byte value
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 16_384);
        Assert.assertEquals(3, len);
        QwpVarint.decode(buf, 0, len, result);
        Assert.assertEquals(16_384, result.value);
    }

    @Test
    public void testEncodeDecodeZero() throws QwpParseException {
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 0);
        Assert.assertEquals(1, len);
        Assert.assertEquals(0x00, buf[0] & 0xFF);
        QwpVarint.decode(buf, 0, len, result);
        Assert.assertEquals(0, result.value);
    }

    @Test
    public void testEncodeLargeValues() throws QwpParseException {
        byte[] buf = new byte[10];

        // Test various powers of 2
        long[] values = {
                1L << 20,  // ~1M
                1L << 30,  // ~1B
                1L << 40,  // ~1T
                1L << 50,
                1L << 60,
                Long.MAX_VALUE
        };

        for (long value : values) {
            int len = QwpVarint.encode(buf, 0, value);
            Assert.assertTrue(len > 0 && len <= 10);
            QwpVarint.decode(buf, 0, len, result);
            Assert.assertEquals(value, result.value);
        }
    }

    @Test
    public void testEncodeSpecificValues() throws QwpParseException {
        // Test values from the spec
        byte[] buf = new byte[10];

        // 300 = 0b100101100
        // Should encode as: 0xAC (0b10101100 = 44 + 128), 0x02 (0b00000010)
        int len = QwpVarint.encode(buf, 0, 300);
        Assert.assertEquals(2, len);
        Assert.assertEquals(0xAC, buf[0] & 0xFF);
        Assert.assertEquals(0x02, buf[1] & 0xFF);

        // Verify decode
        QwpVarint.decode(buf, 0, len, result);
        Assert.assertEquals(300, result.value);
    }

    @Test
    public void testEncodeToDirectMemory() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddr = QwpVarint.encode(addr, 12_345);
            int len = (int) (endAddr - addr);
            Assert.assertTrue(len > 0);

            // Read back and verify
            long decoded = QwpVarint.decode(addr, endAddr);
            Assert.assertEquals(12_345, decoded);
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodedLength() {
        Assert.assertEquals(1, QwpVarint.encodedLength(0));
        Assert.assertEquals(1, QwpVarint.encodedLength(1));
        Assert.assertEquals(1, QwpVarint.encodedLength(127));
        Assert.assertEquals(2, QwpVarint.encodedLength(128));
        Assert.assertEquals(2, QwpVarint.encodedLength(16_383));
        Assert.assertEquals(3, QwpVarint.encodedLength(16_384));
        // Long.MAX_VALUE = 0x7FFFFFFFFFFFFFFF (63 bits) needs ceil(63/7) = 9 bytes
        Assert.assertEquals(9, QwpVarint.encodedLength(Long.MAX_VALUE));
        // Test that actual encoding matches
        byte[] buf = new byte[10];
        int actualLen = QwpVarint.encode(buf, 0, Long.MAX_VALUE);
        Assert.assertEquals(actualLen, QwpVarint.encodedLength(Long.MAX_VALUE));
    }

    @Test
    public void testRoundTripRandomValues() throws QwpParseException {
        byte[] buf = new byte[10];
        Random random = new Random(42); // Fixed seed for reproducibility

        for (int i = 0; i < 1000; i++) {
            long value = random.nextLong() & Long.MAX_VALUE; // Only positive values
            int len = QwpVarint.encode(buf, 0, value);
            QwpVarint.decode(buf, 0, len, result);
            Assert.assertEquals("Failed for value: " + value, value, result.value);
        }
    }
}
