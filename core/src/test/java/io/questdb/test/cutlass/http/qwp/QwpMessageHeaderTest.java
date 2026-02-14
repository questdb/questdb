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

import io.questdb.cutlass.qwp.protocol.QwpMessageHeader;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

public class QwpMessageHeaderTest {

    @Test
    public void testValidHeader() throws QwpParseException {
        byte[] header = createValidHeader(1, 0, 5, 1000);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);

        Assert.assertEquals(MAGIC_MESSAGE, h.getMagic());
        Assert.assertEquals(1, h.getVersion());
        Assert.assertEquals(0, h.getFlags());
        Assert.assertEquals(5, h.getTableCount());
        Assert.assertEquals(1000, h.getPayloadLength());
        Assert.assertEquals(12 + 1000, h.getTotalLength());
    }

    @Test
    public void testMagicBytes() throws QwpParseException {
        // Valid magic
        byte[] header = createValidHeader(1, 0, 1, 100);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);
        Assert.assertEquals(MAGIC_MESSAGE, h.getMagic());
        Assert.assertTrue(QwpMessageHeader.isMessageMagic(h.getMagic()));
    }

    @Test
    public void testInvalidMagicVariations() {
        QwpMessageHeader h = new QwpMessageHeader();

        // Wrong first byte
        byte[] header1 = createValidHeader(1, 0, 1, 100);
        header1[0] = 'X';
        assertInvalidMagic(h, header1);

        // Wrong last byte
        byte[] header2 = createValidHeader(1, 0, 1, 100);
        header2[3] = '5';
        assertInvalidMagic(h, header2);

        // All zeros
        byte[] header3 = new byte[12];
        assertInvalidMagic(h, header3);

        // Capability request magic (different protocol message)
        byte[] header4 = createValidHeader(1, 0, 1, 100);
        header4[3] = '?';
        assertInvalidMagic(h, header4);
    }

    @Test
    public void testVersion1() throws QwpParseException {
        byte[] header = createValidHeader(1, 0, 1, 100);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);
        Assert.assertEquals(1, h.getVersion());
    }

    @Test
    public void testUnsupportedVersion() {
        QwpMessageHeader h = new QwpMessageHeader();

        // Version 0
        byte[] header0 = createValidHeader(0, 0, 1, 100);
        try {
            h.parse(header0, 0, header0.length);
            Assert.fail("Expected exception for version 0");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.UNSUPPORTED_VERSION, e.getErrorCode());
        }

        // Version 2 (future)
        byte[] header2 = createValidHeader(2, 0, 1, 100);
        try {
            h.parse(header2, 0, header2.length);
            Assert.fail("Expected exception for version 2");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.UNSUPPORTED_VERSION, e.getErrorCode());
        }
    }

    @Test
    public void testFlagLZ4() throws QwpParseException {
        byte[] header = createValidHeader(1, FLAG_LZ4, 1, 100);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);

        Assert.assertTrue(h.isLZ4Compressed());
        Assert.assertFalse(h.isZstdCompressed());
        Assert.assertTrue(h.isCompressed());
        Assert.assertFalse(h.isGorillaEnabled());
    }

    @Test
    public void testFlagZstd() throws QwpParseException {
        byte[] header = createValidHeader(1, FLAG_ZSTD, 1, 100);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);

        Assert.assertFalse(h.isLZ4Compressed());
        Assert.assertTrue(h.isZstdCompressed());
        Assert.assertTrue(h.isCompressed());
        Assert.assertFalse(h.isGorillaEnabled());
    }

    @Test
    public void testFlagGorilla() throws QwpParseException {
        byte[] header = createValidHeader(1, FLAG_GORILLA, 1, 100);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);

        Assert.assertFalse(h.isLZ4Compressed());
        Assert.assertFalse(h.isZstdCompressed());
        Assert.assertFalse(h.isCompressed());
        Assert.assertTrue(h.isGorillaEnabled());
    }

    @Test
    public void testFlagCombinations() throws QwpParseException {
        // LZ4 + Gorilla
        byte[] header = createValidHeader(1, (byte) (FLAG_LZ4 | FLAG_GORILLA), 1, 100);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);

        Assert.assertTrue(h.isLZ4Compressed());
        Assert.assertFalse(h.isZstdCompressed());
        Assert.assertTrue(h.isCompressed());
        Assert.assertTrue(h.isGorillaEnabled());
    }

    @Test
    public void testInvalidFlagsBothCompression() {
        // Both LZ4 and Zstd set is invalid
        byte[] header = createValidHeader(1, (byte) (FLAG_LZ4 | FLAG_ZSTD), 1, 100);
        QwpMessageHeader h = new QwpMessageHeader();
        try {
            h.parse(header, 0, header.length);
            Assert.fail("Expected exception for both compression flags");
        } catch (QwpParseException e) {
            // Expected
        }
    }

    @Test
    public void testTableCountZero() throws QwpParseException {
        byte[] header = createValidHeader(1, 0, 0, 0);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);
        Assert.assertEquals(0, h.getTableCount());
    }

    @Test
    public void testTableCountMax() throws QwpParseException {
        byte[] header = createValidHeader(1, 0, 65535, 100);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);
        Assert.assertEquals(65535, h.getTableCount());
    }

    @Test
    public void testPayloadLengthZero() throws QwpParseException {
        byte[] header = createValidHeader(1, 0, 1, 0);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);
        Assert.assertEquals(0, h.getPayloadLength());
        Assert.assertEquals(12, h.getTotalLength());
    }

    @Test
    public void testPayloadLengthMax() throws QwpParseException {
        // Default max is 16MB
        byte[] header = createValidHeader(1, 0, 1, DEFAULT_MAX_BATCH_SIZE);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);
        Assert.assertEquals(DEFAULT_MAX_BATCH_SIZE, h.getPayloadLength());
    }

    @Test
    public void testPayloadLengthExceedsLimit() {
        byte[] header = createValidHeader(1, 0, 1, DEFAULT_MAX_BATCH_SIZE + 1);
        QwpMessageHeader h = new QwpMessageHeader();
        try {
            h.parse(header, 0, header.length);
            Assert.fail("Expected exception for oversized payload");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.PAYLOAD_TOO_LARGE, e.getErrorCode());
        }
    }

    @Test
    public void testCustomPayloadLimit() throws QwpParseException {
        byte[] header = createValidHeader(1, 0, 1, 1000);
        QwpMessageHeader h = new QwpMessageHeader();
        h.setMaxPayloadLength(500);

        try {
            h.parse(header, 0, header.length);
            Assert.fail("Expected exception for oversized payload with custom limit");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.PAYLOAD_TOO_LARGE, e.getErrorCode());
        }

        // Now increase limit
        h.setMaxPayloadLength(2000);
        h.parse(header, 0, header.length);
        Assert.assertEquals(1000, h.getPayloadLength());
    }

    @Test
    public void testHeaderTooShort() {
        QwpMessageHeader h = new QwpMessageHeader();

        // Various too-short lengths
        for (int len = 0; len < 12; len++) {
            byte[] header = new byte[len];
            try {
                h.parse(header, 0, len);
                Assert.fail("Expected exception for length " + len);
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.HEADER_TOO_SHORT, e.getErrorCode());
            }
        }
    }

    @Test
    public void testParseFromDirectBuffer() throws QwpParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] header = createValidHeader(1, FLAG_GORILLA, 10, 5000);
            for (int i = 0; i < header.length; i++) {
                Unsafe.getUnsafe().putByte(addr + i, header[i]);
            }

            QwpMessageHeader h = new QwpMessageHeader();
            h.parse(addr, header.length);

            Assert.assertEquals(1, h.getVersion());
            Assert.assertTrue(h.isGorillaEnabled());
            Assert.assertEquals(10, h.getTableCount());
            Assert.assertEquals(5000, h.getPayloadLength());
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseFromHeapBuffer() throws QwpParseException {
        byte[] header = createValidHeader(1, 0, 3, 500);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);

        Assert.assertEquals(3, h.getTableCount());
        Assert.assertEquals(500, h.getPayloadLength());
    }

    @Test
    public void testParseFromOffset() throws QwpParseException {
        byte[] buffer = new byte[20];
        // Put header at offset 5
        byte[] header = createValidHeader(1, 0, 7, 700);
        System.arraycopy(header, 0, buffer, 5, header.length);

        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(buffer, 5, 12);

        Assert.assertEquals(7, h.getTableCount());
        Assert.assertEquals(700, h.getPayloadLength());
    }

    @Test
    public void testHeaderToString() throws QwpParseException {
        byte[] header = createValidHeader(1, (byte) (FLAG_LZ4 | FLAG_GORILLA), 5, 1234);
        QwpMessageHeader h = new QwpMessageHeader();
        h.parse(header, 0, header.length);

        String str = h.toString();
        Assert.assertTrue(str.contains("ILP4"));
        Assert.assertTrue(str.contains("version=1"));
        Assert.assertTrue(str.contains("[LZ4]"));
        Assert.assertTrue(str.contains("[Gorilla]"));
        Assert.assertTrue(str.contains("tableCount=5"));
        Assert.assertTrue(str.contains("payloadLength=1234"));
    }

    @Test
    public void testReset() throws QwpParseException {
        byte[] header1 = createValidHeader(1, FLAG_LZ4, 10, 5000);
        byte[] header2 = createValidHeader(1, FLAG_ZSTD, 3, 100);

        QwpMessageHeader h = new QwpMessageHeader();

        h.parse(header1, 0, header1.length);
        Assert.assertTrue(h.isLZ4Compressed());
        Assert.assertEquals(10, h.getTableCount());

        h.reset();

        h.parse(header2, 0, header2.length);
        Assert.assertTrue(h.isZstdCompressed());
        Assert.assertEquals(3, h.getTableCount());
    }

    @Test
    public void testReadMagicStatic() {
        byte[] buf = new byte[]{'I', 'L', 'P', '4', 0, 0, 0, 0};
        int magic = QwpMessageHeader.readMagic(buf, 0);
        Assert.assertEquals(MAGIC_MESSAGE, magic);
        Assert.assertTrue(QwpMessageHeader.isMessageMagic(magic));

        buf[3] = '?';
        magic = QwpMessageHeader.readMagic(buf, 0);
        Assert.assertEquals(MAGIC_CAPABILITY_REQUEST, magic);
        Assert.assertTrue(QwpMessageHeader.isCapabilityRequestMagic(magic));
    }

    @Test
    public void testReadMagicFromDirectMemory() {
        long addr = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(addr, (byte) 'I');
            Unsafe.getUnsafe().putByte(addr + 1, (byte) 'L');
            Unsafe.getUnsafe().putByte(addr + 2, (byte) 'P');
            Unsafe.getUnsafe().putByte(addr + 3, (byte) '4');

            int magic = QwpMessageHeader.readMagic(addr);
            Assert.assertEquals(MAGIC_MESSAGE, magic);
        } finally {
            Unsafe.free(addr, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testLargePayloadLength() throws QwpParseException {
        // Test max uint32 value (but within our limit)
        QwpMessageHeader h = new QwpMessageHeader();
        h.setMaxPayloadLength(0xFFFFFFFFL);

        byte[] header = new byte[12];
        header[0] = 'I';
        header[1] = 'L';
        header[2] = 'P';
        header[3] = '4';
        header[4] = 1; // version
        header[5] = 0; // flags
        header[6] = 1; // table count lo
        header[7] = 0; // table count hi
        header[8] = (byte) 0xFF; // payload length (all 1s)
        header[9] = (byte) 0xFF;
        header[10] = (byte) 0xFF;
        header[11] = (byte) 0x7F; // 0x7FFFFFFF = 2GB (just under max int)

        h.parse(header, 0, header.length);
        Assert.assertEquals(0x7FFFFFFFL, h.getPayloadLength());
    }

    // ==================== Helper Methods ====================

    private byte[] createValidHeader(int version, int flags, int tableCount, long payloadLength) {
        byte[] header = new byte[12];
        // Magic: "ILP4"
        header[0] = 'I';
        header[1] = 'L';
        header[2] = 'P';
        header[3] = '4';
        // Version
        header[4] = (byte) version;
        // Flags
        header[5] = (byte) flags;
        // Table count (little-endian uint16)
        header[6] = (byte) (tableCount & 0xFF);
        header[7] = (byte) ((tableCount >> 8) & 0xFF);
        // Payload length (little-endian uint32)
        header[8] = (byte) (payloadLength & 0xFF);
        header[9] = (byte) ((payloadLength >> 8) & 0xFF);
        header[10] = (byte) ((payloadLength >> 16) & 0xFF);
        header[11] = (byte) ((payloadLength >> 24) & 0xFF);
        return header;
    }

    private void assertInvalidMagic(QwpMessageHeader h, byte[] header) {
        try {
            h.parse(header, 0, header.length);
            Assert.fail("Expected exception for invalid magic");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_MAGIC, e.getErrorCode());
        }
    }
}
