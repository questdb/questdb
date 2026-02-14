/*******************************************************************************
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

package io.questdb.test.cutlass.line.websocket;

import io.questdb.client.cutlass.ilpv4.client.NativeBufferWriter;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for NativeBufferWriter.
 */
public class NativeBufferWriterTest {

    @Test
    public void testWriteByte() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putByte((byte) 0x42);
            Assert.assertEquals(1, writer.getPosition());
            Assert.assertEquals((byte) 0x42, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
        }
    }

    @Test
    public void testWriteShort() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putShort((short) 0x1234);
            Assert.assertEquals(2, writer.getPosition());
            // Little-endian
            Assert.assertEquals((byte) 0x34, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            Assert.assertEquals((byte) 0x12, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
        }
    }

    @Test
    public void testWriteInt() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putInt(0x12345678);
            Assert.assertEquals(4, writer.getPosition());
            // Little-endian
            Assert.assertEquals(0x12345678, Unsafe.getUnsafe().getInt(writer.getBufferPtr()));
        }
    }

    @Test
    public void testWriteLong() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putLong(0x123456789ABCDEF0L);
            Assert.assertEquals(8, writer.getPosition());
            Assert.assertEquals(0x123456789ABCDEF0L, Unsafe.getUnsafe().getLong(writer.getBufferPtr()));
        }
    }

    @Test
    public void testWriteLongBigEndian() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putLongBE(0x0102030405060708L);
            Assert.assertEquals(8, writer.getPosition());
            // Check big-endian byte order
            long ptr = writer.getBufferPtr();
            Assert.assertEquals((byte) 0x01, Unsafe.getUnsafe().getByte(ptr));
            Assert.assertEquals((byte) 0x02, Unsafe.getUnsafe().getByte(ptr + 1));
            Assert.assertEquals((byte) 0x03, Unsafe.getUnsafe().getByte(ptr + 2));
            Assert.assertEquals((byte) 0x04, Unsafe.getUnsafe().getByte(ptr + 3));
            Assert.assertEquals((byte) 0x05, Unsafe.getUnsafe().getByte(ptr + 4));
            Assert.assertEquals((byte) 0x06, Unsafe.getUnsafe().getByte(ptr + 5));
            Assert.assertEquals((byte) 0x07, Unsafe.getUnsafe().getByte(ptr + 6));
            Assert.assertEquals((byte) 0x08, Unsafe.getUnsafe().getByte(ptr + 7));
        }
    }

    @Test
    public void testWriteFloat() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putFloat(3.14f);
            Assert.assertEquals(4, writer.getPosition());
            Assert.assertEquals(3.14f, Unsafe.getUnsafe().getFloat(writer.getBufferPtr()), 0.0001f);
        }
    }

    @Test
    public void testWriteDouble() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putDouble(3.14159265359);
            Assert.assertEquals(8, writer.getPosition());
            Assert.assertEquals(3.14159265359, Unsafe.getUnsafe().getDouble(writer.getBufferPtr()), 0.0000000001);
        }
    }

    @Test
    public void testWriteVarintSmall() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            // Single byte for values < 128
            writer.putVarint(127);
            Assert.assertEquals(1, writer.getPosition());
            Assert.assertEquals((byte) 127, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
        }
    }

    @Test
    public void testWriteVarintMedium() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            // Two bytes for 128
            writer.putVarint(128);
            Assert.assertEquals(2, writer.getPosition());
            // LEB128: 128 = 0x80 0x01
            Assert.assertEquals((byte) 0x80, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            Assert.assertEquals((byte) 0x01, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
        }
    }

    @Test
    public void testWriteVarintLarge() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            // Test larger value
            writer.putVarint(16384);
            Assert.assertEquals(3, writer.getPosition());
            // LEB128: 16384 = 0x80 0x80 0x01
            Assert.assertEquals((byte) 0x80, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            Assert.assertEquals((byte) 0x80, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            Assert.assertEquals((byte) 0x01, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
        }
    }

    @Test
    public void testWriteString() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putString("hello");
            // Length (1 byte varint) + 5 bytes
            Assert.assertEquals(6, writer.getPosition());
            // Check length
            Assert.assertEquals((byte) 5, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            // Check content
            Assert.assertEquals((byte) 'h', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            Assert.assertEquals((byte) 'e', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
            Assert.assertEquals((byte) 'l', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 3));
            Assert.assertEquals((byte) 'l', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 4));
            Assert.assertEquals((byte) 'o', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 5));
        }
    }

    @Test
    public void testWriteEmptyString() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putString("");
            Assert.assertEquals(1, writer.getPosition());
            Assert.assertEquals((byte) 0, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
        }
    }

    @Test
    public void testWriteNullString() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putString(null);
            Assert.assertEquals(1, writer.getPosition());
            Assert.assertEquals((byte) 0, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
        }
    }

    @Test
    public void testWriteUtf8Ascii() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putUtf8("ABC");
            Assert.assertEquals(3, writer.getPosition());
            Assert.assertEquals((byte) 'A', Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            Assert.assertEquals((byte) 'B', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            Assert.assertEquals((byte) 'C', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
        }
    }

    @Test
    public void testWriteUtf8TwoByte() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            // ñ is 2 bytes in UTF-8
            writer.putUtf8("ñ");
            Assert.assertEquals(2, writer.getPosition());
            Assert.assertEquals((byte) 0xC3, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            Assert.assertEquals((byte) 0xB1, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
        }
    }

    @Test
    public void testWriteUtf8ThreeByte() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            // € is 3 bytes in UTF-8
            writer.putUtf8("€");
            Assert.assertEquals(3, writer.getPosition());
            Assert.assertEquals((byte) 0xE2, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            Assert.assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            Assert.assertEquals((byte) 0xAC, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
        }
    }

    @Test
    public void testUtf8Length() {
        Assert.assertEquals(0, NativeBufferWriter.utf8Length(null));
        Assert.assertEquals(0, NativeBufferWriter.utf8Length(""));
        Assert.assertEquals(5, NativeBufferWriter.utf8Length("hello"));
        Assert.assertEquals(2, NativeBufferWriter.utf8Length("ñ"));
        Assert.assertEquals(3, NativeBufferWriter.utf8Length("€"));
    }

    @Test
    public void testReset() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putInt(12345);
            Assert.assertEquals(4, writer.getPosition());
            writer.reset();
            Assert.assertEquals(0, writer.getPosition());
            // Can write again
            writer.putByte((byte) 0xFF);
            Assert.assertEquals(1, writer.getPosition());
        }
    }

    @Test
    public void testPatchInt() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putInt(0);  // Placeholder at offset 0
            writer.putInt(100);  // At offset 4
            writer.patchInt(0, 42);  // Patch first int
            Assert.assertEquals(42, Unsafe.getUnsafe().getInt(writer.getBufferPtr()));
            Assert.assertEquals(100, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + 4));
        }
    }

    @Test
    public void testGrowBuffer() {
        try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
            // Write more than initial capacity
            for (int i = 0; i < 100; i++) {
                writer.putLong(i);
            }
            Assert.assertEquals(800, writer.getPosition());
            // Verify data
            for (int i = 0; i < 100; i++) {
                Assert.assertEquals(i, Unsafe.getUnsafe().getLong(writer.getBufferPtr() + i * 8));
            }
        }
    }

    @Test
    public void testPutBlockOfBytes() {
        try (NativeBufferWriter writer = new NativeBufferWriter();
             NativeBufferWriter source = new NativeBufferWriter()) {
            // Prepare source data
            source.putByte((byte) 1);
            source.putByte((byte) 2);
            source.putByte((byte) 3);
            source.putByte((byte) 4);

            // Copy to writer
            writer.putBlockOfBytes(source.getBufferPtr(), 4);
            Assert.assertEquals(4, writer.getPosition());
            Assert.assertEquals((byte) 1, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            Assert.assertEquals((byte) 2, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            Assert.assertEquals((byte) 3, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
            Assert.assertEquals((byte) 4, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 3));
        }
    }

    @Test
    public void testMultipleWrites() {
        try (NativeBufferWriter writer = new NativeBufferWriter()) {
            writer.putByte((byte) 'I');
            writer.putByte((byte) 'L');
            writer.putByte((byte) 'P');
            writer.putByte((byte) '4');
            writer.putByte((byte) 1);  // Version
            writer.putByte((byte) 0);  // Flags
            writer.putShort((short) 1);  // Table count
            writer.putInt(0);  // Payload length placeholder

            Assert.assertEquals(12, writer.getPosition());

            // Verify ILP4 header
            Assert.assertEquals((byte) 'I', Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            Assert.assertEquals((byte) 'L', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            Assert.assertEquals((byte) 'P', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
            Assert.assertEquals((byte) '4', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 3));
        }
    }
}
