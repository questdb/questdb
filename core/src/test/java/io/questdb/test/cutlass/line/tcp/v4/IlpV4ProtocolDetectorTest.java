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

package io.questdb.test.cutlass.line.tcp.v4;

import io.questdb.cutlass.line.tcp.v4.IlpV4ProtocolDetector;
import io.questdb.cutlass.line.tcp.v4.IlpV4ProtocolDetector.DetectionResult;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class IlpV4ProtocolDetectorTest {

    // ==================== ILP v4 Handshake Detection ====================

    @Test
    public void testDetectIlpV4Request() {
        // "ILP?" magic = 0x3F504C49 little-endian
        byte[] buf = new byte[]{'I', 'L', 'P', '?', 1, 1, 0, 0};
        Assert.assertEquals(DetectionResult.V4_HANDSHAKE, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    @Test
    public void testDetectIlpV4RequestDirectMemory() {
        long address = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 'I');
            Unsafe.getUnsafe().putByte(address + 1, (byte) 'L');
            Unsafe.getUnsafe().putByte(address + 2, (byte) 'P');
            Unsafe.getUnsafe().putByte(address + 3, (byte) '?');
            Unsafe.getUnsafe().putInt(address + 4, 0);

            Assert.assertEquals(DetectionResult.V4_HANDSHAKE, IlpV4ProtocolDetector.detect(address, 8));
        } finally {
            Unsafe.free(address, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== ILP v4 Direct Detection ====================

    @Test
    public void testDetectIlpV4Direct() {
        // "ILP4" magic = 0x34504C49 little-endian
        byte[] buf = new byte[]{'I', 'L', 'P', '4', 0, 0, 0, 0};
        Assert.assertEquals(DetectionResult.V4_DIRECT, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    @Test
    public void testDetectIlpV4DirectDirectMemory() {
        long address = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 'I');
            Unsafe.getUnsafe().putByte(address + 1, (byte) 'L');
            Unsafe.getUnsafe().putByte(address + 2, (byte) 'P');
            Unsafe.getUnsafe().putByte(address + 3, (byte) '4');
            Unsafe.getUnsafe().putInt(address + 4, 0);

            Assert.assertEquals(DetectionResult.V4_DIRECT, IlpV4ProtocolDetector.detect(address, 8));
        } finally {
            Unsafe.free(address, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Text Protocol Detection ====================

    @Test
    public void testDetectTextProtocol() {
        // Text protocol starts with table name (ASCII letter)
        byte[] buf = "weather,location=us temp=82".getBytes();
        Assert.assertEquals(DetectionResult.TEXT_PROTOCOL, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    @Test
    public void testDetectTextProtocolLowercase() {
        byte[] buf = "measurements,city=nyc value=100".getBytes();
        Assert.assertEquals(DetectionResult.TEXT_PROTOCOL, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    @Test
    public void testDetectTextProtocolUppercase() {
        byte[] buf = "Weather,location=us temp=82".getBytes();
        Assert.assertEquals(DetectionResult.TEXT_PROTOCOL, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    @Test
    public void testDetectTextProtocolDirectMemory() {
        byte[] data = "table_name,tag=val field=1".getBytes();
        long address = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < data.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, data[i]);
            }
            Assert.assertEquals(DetectionResult.TEXT_PROTOCOL, IlpV4ProtocolDetector.detect(address, data.length));
        } finally {
            Unsafe.free(address, data.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Partial Data Detection ====================

    @Test
    public void testDetectWithPartialDataEmpty() {
        byte[] buf = new byte[0];
        Assert.assertEquals(DetectionResult.NEED_MORE_DATA, IlpV4ProtocolDetector.detect(buf, 0, 0));
    }

    @Test
    public void testDetectWithPartialDataOneByteI() {
        // Single 'I' byte - could be ILP v4 or text, need more data
        byte[] buf = new byte[]{'I'};
        Assert.assertEquals(DetectionResult.NEED_MORE_DATA, IlpV4ProtocolDetector.detect(buf, 0, 1));
    }

    @Test
    public void testDetectWithPartialDataOneByteNonI() {
        // Single non-'I' letter - definitely text protocol
        byte[] buf = new byte[]{'t'};
        Assert.assertEquals(DetectionResult.TEXT_PROTOCOL, IlpV4ProtocolDetector.detect(buf, 0, 1));
    }

    @Test
    public void testDetectWithPartialDataTwoBytes() {
        byte[] buf = new byte[]{'I', 'L'};
        Assert.assertEquals(DetectionResult.NEED_MORE_DATA, IlpV4ProtocolDetector.detect(buf, 0, 2));
    }

    @Test
    public void testDetectWithPartialDataThreeBytes() {
        byte[] buf = new byte[]{'I', 'L', 'P'};
        Assert.assertEquals(DetectionResult.NEED_MORE_DATA, IlpV4ProtocolDetector.detect(buf, 0, 3));
    }

    @Test
    public void testDetectEmptyBuffer() {
        Assert.assertEquals(DetectionResult.NEED_MORE_DATA, IlpV4ProtocolDetector.detect(new byte[0], 0, 0));
    }

    @Test
    public void testDetectEmptyBufferDirectMemory() {
        long address = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(DetectionResult.NEED_MORE_DATA, IlpV4ProtocolDetector.detect(address, 0));
        } finally {
            Unsafe.free(address, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Binary Garbage Detection ====================

    @Test
    public void testDetectBinaryGarbage() {
        // Random binary data not starting with ASCII letter
        byte[] buf = new byte[]{0x00, 0x01, 0x02, 0x03};
        Assert.assertEquals(DetectionResult.UNKNOWN, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    @Test
    public void testDetectBinaryGarbageHighBytes() {
        byte[] buf = new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xFD, (byte) 0xFC};
        Assert.assertEquals(DetectionResult.UNKNOWN, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    @Test
    public void testDetectBinaryGarbageDirectMemory() {
        long address = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            // Use bytes that are not ASCII letters
            Unsafe.getUnsafe().putByte(address, (byte) 0x00);
            Unsafe.getUnsafe().putByte(address + 1, (byte) 0x01);
            Unsafe.getUnsafe().putByte(address + 2, (byte) 0x02);
            Unsafe.getUnsafe().putByte(address + 3, (byte) 0x03);
            Assert.assertEquals(DetectionResult.UNKNOWN, IlpV4ProtocolDetector.detect(address, 4));
        } finally {
            Unsafe.free(address, 4, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDetectBinaryGarbageDigit() {
        // Starts with digit - not valid
        byte[] buf = new byte[]{'1', '2', '3', '4'};
        Assert.assertEquals(DetectionResult.UNKNOWN, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    // ==================== Helper Method Tests ====================

    @Test
    public void testIsV4Protocol() {
        Assert.assertTrue(IlpV4ProtocolDetector.isV4Protocol(DetectionResult.V4_HANDSHAKE));
        Assert.assertTrue(IlpV4ProtocolDetector.isV4Protocol(DetectionResult.V4_DIRECT));
        Assert.assertFalse(IlpV4ProtocolDetector.isV4Protocol(DetectionResult.TEXT_PROTOCOL));
        Assert.assertFalse(IlpV4ProtocolDetector.isV4Protocol(DetectionResult.NEED_MORE_DATA));
        Assert.assertFalse(IlpV4ProtocolDetector.isV4Protocol(DetectionResult.UNKNOWN));
    }

    @Test
    public void testPeekMagicByteArray() {
        byte[] buf = new byte[]{'I', 'L', 'P', '4'};
        int magic = IlpV4ProtocolDetector.peekMagic(buf, 0);
        Assert.assertEquals(0x34504C49, magic); // "ILP4" little-endian
    }

    @Test
    public void testPeekMagicDirectMemory() {
        long address = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 'I');
            Unsafe.getUnsafe().putByte(address + 1, (byte) 'L');
            Unsafe.getUnsafe().putByte(address + 2, (byte) 'P');
            Unsafe.getUnsafe().putByte(address + 3, (byte) '?');

            int magic = IlpV4ProtocolDetector.peekMagic(address);
            Assert.assertEquals(0x3F504C49, magic); // "ILP?" little-endian
        } finally {
            Unsafe.free(address, 4, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Offset Tests ====================

    @Test
    public void testDetectWithOffset() {
        // Prepend some garbage, then ILP4 magic
        byte[] buf = new byte[]{0x00, 0x00, 'I', 'L', 'P', '4', 0, 0, 0, 0};
        Assert.assertEquals(DetectionResult.V4_DIRECT, IlpV4ProtocolDetector.detect(buf, 2, 8));
    }

    @Test
    public void testDetectTextProtocolWithOffset() {
        byte[] buf = new byte[]{0x00, 0x00, 't', 'a', 'b', 'l', 'e'};
        Assert.assertEquals(DetectionResult.TEXT_PROTOCOL, IlpV4ProtocolDetector.detect(buf, 2, 5));
    }

    // ==================== Edge Cases ====================

    @Test
    public void testDetectIlpResponseMagic() {
        // "ILP!" is a response magic, not request.
        // Since it starts with 'I' (ASCII letter), it's treated as TEXT_PROTOCOL.
        // This is acceptable because the server would never receive "ILP!" from client
        // unless the client is confused. The detector's job is to detect what the CLIENT sends.
        byte[] buf = new byte[]{'I', 'L', 'P', '!', 1, 0, 0, 0};
        Assert.assertEquals(DetectionResult.TEXT_PROTOCOL, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    @Test
    public void testDetectIlpFallbackMagic() {
        // "ILP0" is fallback response magic.
        // Since it starts with 'I' (ASCII letter), it's treated as TEXT_PROTOCOL.
        byte[] buf = new byte[]{'I', 'L', 'P', '0', 0, 0, 0, 0};
        Assert.assertEquals(DetectionResult.TEXT_PROTOCOL, IlpV4ProtocolDetector.detect(buf, 0, buf.length));
    }

    @Test
    public void testMinBytesForDetection() {
        Assert.assertEquals(4, IlpV4ProtocolDetector.MIN_BYTES_FOR_DETECTION);
    }
}
