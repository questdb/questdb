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

import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class QwpVarintTest {

    @Test
    public void testEncodeDecodeZero() throws QwpParseException {
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 0);
        Assert.assertEquals(1, len);
        Assert.assertEquals(0x00, buf[0] & 0xFF);
        Assert.assertEquals(0, QwpVarint.decode(buf, 0, len));
    }

    @Test
    public void testEncodeDecode127() throws QwpParseException {
        // 127 is the maximum 1-byte value
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 127);
        Assert.assertEquals(1, len);
        Assert.assertEquals(0x7F, buf[0] & 0xFF);
        Assert.assertEquals(127, QwpVarint.decode(buf, 0, len));
    }

    @Test
    public void testEncodeDecode128() throws QwpParseException {
        // 128 is the minimum 2-byte value
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 128);
        Assert.assertEquals(2, len);
        Assert.assertEquals(0x80, buf[0] & 0xFF); // 0 + continuation bit
        Assert.assertEquals(0x01, buf[1] & 0xFF); // 1
        Assert.assertEquals(128, QwpVarint.decode(buf, 0, len));
    }

    @Test
    public void testEncodeDecode16383() throws QwpParseException {
        // 16383 (0x3FFF) is the maximum 2-byte value
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 16383);
        Assert.assertEquals(2, len);
        Assert.assertEquals(0xFF, buf[0] & 0xFF); // 127 + continuation bit
        Assert.assertEquals(0x7F, buf[1] & 0xFF); // 127
        Assert.assertEquals(16383, QwpVarint.decode(buf, 0, len));
    }

    @Test
    public void testEncodeDecode16384() throws QwpParseException {
        // 16384 (0x4000) is the minimum 3-byte value
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 16384);
        Assert.assertEquals(3, len);
        Assert.assertEquals(16384, QwpVarint.decode(buf, 0, len));
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
            Assert.assertEquals(value, QwpVarint.decode(buf, 0, len));
        }
    }

    @Test
    public void testRoundTripRandomValues() throws QwpParseException {
        byte[] buf = new byte[10];
        Random random = new Random(42); // Fixed seed for reproducibility

        for (int i = 0; i < 1000; i++) {
            long value = random.nextLong() & Long.MAX_VALUE; // Only positive values
            int len = QwpVarint.encode(buf, 0, value);
            long decoded = QwpVarint.decode(buf, 0, len);
            Assert.assertEquals("Failed for value: " + value, value, decoded);
        }
    }

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
    public void testEncodeToDirectMemory() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddr = QwpVarint.encode(addr, 12345);
            int len = (int) (endAddr - addr);
            Assert.assertTrue(len > 0);

            // Read back and verify
            long decoded = QwpVarint.decode(addr, endAddr);
            Assert.assertEquals(12345, decoded);
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeIncompleteVarint() {
        // Byte with continuation bit set but no following byte
        byte[] buf = new byte[]{(byte) 0x80};
        try {
            QwpVarint.decode(buf, 0, 1);
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
            QwpVarint.decode(buf, 0, 12);
            Assert.fail("Should have thrown exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.VARINT_OVERFLOW, e.getErrorCode());
        }
    }

    @Test
    public void testEncodedLength() {
        Assert.assertEquals(1, QwpVarint.encodedLength(0));
        Assert.assertEquals(1, QwpVarint.encodedLength(1));
        Assert.assertEquals(1, QwpVarint.encodedLength(127));
        Assert.assertEquals(2, QwpVarint.encodedLength(128));
        Assert.assertEquals(2, QwpVarint.encodedLength(16383));
        Assert.assertEquals(3, QwpVarint.encodedLength(16384));
        // Long.MAX_VALUE = 0x7FFFFFFFFFFFFFFF (63 bits) needs ceil(63/7) = 9 bytes
        Assert.assertEquals(9, QwpVarint.encodedLength(Long.MAX_VALUE));
        // Test that actual encoding matches
        byte[] buf = new byte[10];
        int actualLen = QwpVarint.encode(buf, 0, Long.MAX_VALUE);
        Assert.assertEquals(actualLen, QwpVarint.encodedLength(Long.MAX_VALUE));
    }

    @Test
    public void testDecodeResult() throws QwpParseException {
        byte[] buf = new byte[10];
        int len = QwpVarint.encode(buf, 0, 300);

        QwpVarint.DecodeResult result = new QwpVarint.DecodeResult();
        QwpVarint.decode(buf, 0, len, result);

        Assert.assertEquals(300, result.value);
        Assert.assertEquals(len, result.bytesRead);
    }

    @Test
    public void testDecodeResultFromDirectMemory() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddr = QwpVarint.encode(addr, 999999);
            int expectedLen = (int) (endAddr - addr);

            QwpVarint.DecodeResult result = new QwpVarint.DecodeResult();
            QwpVarint.decode(addr, endAddr, result);

            Assert.assertEquals(999999, result.value);
            Assert.assertEquals(expectedLen, result.bytesRead);
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
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
        Assert.assertEquals(300, QwpVarint.decode(buf, 0, len));
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
        int len2 = QwpVarint.encode(buf, 0, 50000);
        QwpVarint.decode(buf, 0, len2, result);
        Assert.assertEquals(50000, result.value);
    }
}
