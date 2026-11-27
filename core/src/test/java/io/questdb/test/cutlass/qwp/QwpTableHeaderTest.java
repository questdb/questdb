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

import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableHeader;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class QwpTableHeaderTest {

    @Test
    public void testColumnCountExceedsMax() {
        // Test with column count exceeding maximum
        int columnCount = QwpConstants.MAX_COLUMNS_PER_TABLE + 1;
        byte[] buf = encodeTableHeader("test", 100, columnCount);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);
            Assert.fail("Expected exception for column count exceeding max");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("column count exceeds maximum"));
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testColumnCountMax() throws QwpParseException {
        // Test with maximum column count
        int columnCount = QwpConstants.MAX_COLUMNS_PER_TABLE;
        byte[] buf = encodeTableHeader("test", 100, columnCount);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(columnCount, header.getColumnCount());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testColumnCountVarint() throws QwpParseException {
        // Test column count that requires multi-byte varint
        int columnCount = 500;
        byte[] buf = encodeTableHeader("test", 100, columnCount);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(columnCount, header.getColumnCount());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testColumnCountZero() throws QwpParseException {
        // Zero columns might be valid for empty table blocks
        byte[] buf = encodeTableHeader("test", 10, 0);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(0, header.getColumnCount());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testColumnCountWithSignBitSet() {
        long columnCount = 0x8000000001000000L;
        byte[] buf = encodeTableHeader("test", 0, columnCount);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);
            Assert.fail("Expected exception for unsigned column count exceeding max");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.COLUMN_COUNT_EXCEEDED, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains(Long.toUnsignedString(columnCount)));
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseMultipleTableHeaders() throws QwpParseException {
        // Create buffer with two consecutive table headers
        byte[] header1 = encodeTableHeader("table1", 100, 5);
        byte[] header2 = encodeTableHeader("table2", 200, 10);

        byte[] combined = new byte[header1.length + header2.length];
        System.arraycopy(header1, 0, combined, 0, header1.length);
        System.arraycopy(header2, 0, combined, header1.length, header2.length);

        long address = copyToDirectMemory(combined);
        try {
            QwpTableHeader header = new QwpTableHeader();

            // Parse first header
            header.parse(address, combined.length);
            Assert.assertEquals("table1", header.getTableName());
            Assert.assertEquals(100, header.getRowCount());
            Assert.assertEquals(5, header.getColumnCount());
            int consumed1 = header.getBytesConsumed();
            Assert.assertEquals(header1.length, consumed1);

            // Parse second header
            header.reset();
            header.parse(address + consumed1, combined.length - consumed1);
            Assert.assertEquals("table2", header.getTableName());
            Assert.assertEquals(200, header.getRowCount());
            Assert.assertEquals(10, header.getColumnCount());
        } finally {
            Unsafe.free(address, combined.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseTableName() throws QwpParseException {
        String tableName = "weather";
        byte[] buf = encodeTableHeader(tableName, 100, 5);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(tableName, header.getTableName());
            Assert.assertEquals(100, header.getRowCount());
            Assert.assertEquals(5, header.getColumnCount());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseTruncatedBuffer() {
        byte[] buf = encodeTableHeader("test", 100, 5);
        // Truncate the buffer
        byte[] truncated = new byte[buf.length / 2];
        System.arraycopy(buf, 0, truncated, 0, truncated.length);

        long address = copyToDirectMemory(truncated);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, truncated.length);
            Assert.fail("Expected exception for truncated buffer");
        } catch (QwpParseException e) {
            // Expected
        } finally {
            Unsafe.free(address, truncated.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReset() throws QwpParseException {
        byte[] buf = encodeTableHeader("test", 100, 5);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals("test", header.getTableName());

            header.reset();

            Assert.assertNull(header.getTableName());
            Assert.assertEquals(0, header.getRowCount());
            Assert.assertEquals(0, header.getColumnCount());
            Assert.assertEquals(0, header.getBytesConsumed());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRowCountExceedsDefaultLimit() {
        long rowCount = (long) QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE + 1;
        byte[] buf = encodeTableHeader("test", rowCount, 5);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);
            Assert.fail("Expected exception for row count exceeding default limit");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.ROW_COUNT_EXCEEDED, e.getErrorCode());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRowCountExceedsIntMax() {
        // Row count just above Integer.MAX_VALUE must be rejected
        long rowCount = (long) Integer.MAX_VALUE + 1;
        byte[] buf = encodeTableHeader("test", rowCount, 5);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);
            Assert.fail("Expected exception for row count exceeding int range");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.ROW_COUNT_EXCEEDED, e.getErrorCode());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRowCountLarge() {
        // Very large row count must be rejected
        long rowCount = Long.MAX_VALUE >> 1;
        byte[] buf = encodeTableHeader("test", rowCount, 5);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);
            Assert.fail("Expected exception for row count exceeding int range");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.ROW_COUNT_EXCEEDED, e.getErrorCode());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRowCountMax() throws QwpParseException {
        // Test with maximum row count
        long rowCount = QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE;
        byte[] buf = encodeTableHeader("test", rowCount, 5);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(rowCount, header.getRowCount());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRowCountRespectsConfiguredLimit() throws QwpParseException {
        byte[] accepted = encodeTableHeader("test", 10, 5);
        long acceptedAddress = copyToDirectMemory(accepted);
        try {
            QwpTableHeader header = new QwpTableHeader(10);
            header.parse(acceptedAddress, accepted.length);
            Assert.assertEquals(10, header.getRowCount());
        } finally {
            Unsafe.free(acceptedAddress, accepted.length, MemoryTag.NATIVE_DEFAULT);
        }

        byte[] rejected = encodeTableHeader("test", 11, 5);
        long rejectedAddress = copyToDirectMemory(rejected);
        try {
            QwpTableHeader header = new QwpTableHeader(10);
            header.parse(rejectedAddress, rejected.length);
            Assert.fail("Expected exception for row count exceeding configured limit");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.ROW_COUNT_EXCEEDED, e.getErrorCode());
        } finally {
            Unsafe.free(rejectedAddress, rejected.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRowCountVarint() throws QwpParseException {
        // Test with a row count that requires multiple varint bytes
        long rowCount = 1_000_000;
        byte[] buf = encodeTableHeader("test", rowCount, 5);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(rowCount, header.getRowCount());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRowCountWithSignBitSet() {
        long rowCount = 0x8000000080000000L;
        byte[] buf = encodeTableHeader("test", rowCount, 1);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);
            Assert.fail("Expected exception for unsigned row count exceeding max");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.ROW_COUNT_EXCEEDED, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains(Long.toUnsignedString(rowCount)));
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRowCountZero() throws QwpParseException {
        byte[] buf = encodeTableHeader("test", 0, 5);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(0, header.getRowCount());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Verifies that the table header parser rejects a name length varint
     * whose value exceeds int range, even when the truncated (int) value
     * would fall within the valid [1, 127] range. Without the fix, the
     * varint value 0x100000001L (4294967297) would silently truncate to 1,
     * bypassing the length check.
     */
    @Test
    public void testTableNameLengthVarintOverflow() {
        // Encode a name length varint of (1L << 32) + 1 = 0x100000001L.
        // When truncated to int, this becomes 1, which would pass the
        // MAX_TABLE_NAME_LENGTH (127) check.
        long overflowLen = (1L << 32) + 1;
        byte[] varintBuf = new byte[QwpVarint.MAX_VARINT_BYTES];
        int varintLen = QwpVarint.encode(varintBuf, 0, overflowLen);

        // Build a buffer: overflowing name length varint + dummy byte
        byte[] buf = new byte[varintLen + 1];
        System.arraycopy(varintBuf, 0, buf, 0, varintLen);
        buf[varintLen] = 'x'; // dummy byte in case the length passes

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);
            Assert.fail("Expected QwpParseException for name length varint exceeding int range");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("table name too long"));
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTableNameAsciiFlag() throws QwpParseException {
        // The table cache folds only ASCII case of a name flagged ASCII, so a non-ASCII
        // name must never carry the flag. An ASCII name carries it, which spares the
        // cache a scan of the name per block.
        assertAsciiFlag("weather", true);
        assertAsciiFlag("a".repeat(QwpConstants.MAX_TABLE_NAME_LENGTH), true);
        assertAsciiFlag("wetterÄ", false);
        assertAsciiFlag("Äwetter", false);
        assertAsciiFlag("a".repeat(20) + "ж" + "a".repeat(20), false);
        assertAsciiFlag("😀", false);
    }

    @Test
    public void testTableNameEmpty() {
        // Empty table name should fail
        byte[] buf = new byte[10];
        int offset = 0;
        offset = QwpVarint.encode(buf, offset, 0); // name length = 0
        offset = QwpVarint.encode(buf, offset, 100);
        offset = QwpVarint.encode(buf, offset, 5);

        long address = copyToDirectMemory(buf, offset);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, offset);
            Assert.fail("Expected exception for empty table name");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("empty table name"));
        } finally {
            Unsafe.free(address, offset, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTableNameMaxLength() throws QwpParseException {
        // Create table name at max length
        String tableName = "a".repeat(QwpConstants.MAX_TABLE_NAME_LENGTH);
        byte[] buf = encodeTableHeader(tableName, 1, 1);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(tableName, header.getTableName());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTableNameTooLong() {
        // Create table name exceeding max length
        String tableName = "a".repeat(QwpConstants.MAX_TABLE_NAME_LENGTH + 1);
        byte[] buf = encodeTableHeader(tableName, 1, 1);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);
            Assert.fail("Expected exception for table name too long");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("table name too long"));
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTableNameUnicode() throws QwpParseException {
        // Table name with unicode characters
        String tableName = "测量数据";
        byte[] buf = encodeTableHeader(tableName, 10, 2);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(tableName, header.getTableName());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTableNameInvalidUtf8() {
        // The table lookup decoded a malformed name up to its first bad byte and wrote to
        // the table those chars name.
        final byte[][] names = {
                // a two-byte sequence cut short
                {'t', (byte) 0xC3},
                // a lead byte followed by ASCII
                {'t', (byte) 0xC3, 'z'},
                // a continuation byte without a lead
                {'t', (byte) 0x80},
                {(byte) 0xBF, 't'},
                // overlong encodings of '/'
                {'t', (byte) 0xC0, (byte) 0xAF},
                {'t', (byte) 0xE0, (byte) 0x80, (byte) 0xAF},
                {'t', (byte) 0xF0, (byte) 0x80, (byte) 0x80, (byte) 0xAF},
                // a UTF-16 surrogate, U+D800
                {'t', (byte) 0xED, (byte) 0xA0, (byte) 0x80},
                // a four-byte sequence cut short, at the end and before ASCII
                {'t', (byte) 0xF0, (byte) 0x9F, (byte) 0x98},
                {'t', (byte) 0xF0, (byte) 0x9F, (byte) 0x98, 'z'},
                // above U+10FFFF
                {'t', (byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80},
                // the five-byte form
                {'t', (byte) 0xF8, (byte) 0x88, (byte) 0x80, (byte) 0x80, (byte) 0x80},
                {'t', (byte) 0xFF},
                // past the first eight bytes, which an ASCII check reads as one word
                {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', (byte) 0xC3},
        };
        final QwpTableHeader header = new QwpTableHeader();
        for (byte[] name : names) {
            final byte[] buf = encodeTableHeader(name, 1, 1);
            final long address = copyToDirectMemory(buf);
            try {
                header.parse(address, buf.length);
                Assert.fail("must refuse " + Arrays.toString(name));
            } catch (QwpParseException e) {
                Assert.assertEquals(Arrays.toString(name), QwpParseException.ErrorCode.INVALID_UTF8, e.getErrorCode());
                Assert.assertEquals(Arrays.toString(name), "table name is not valid UTF-8", e.getMessage());
                // The refused name is gone: getTableName() could not decode it.
                Assert.assertEquals(0, header.getTableNameUtf8().size());
                Assert.assertNull(header.getTableName());
            } finally {
                Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testTableNameValidUtf8Boundaries() throws QwpParseException {
        // The first and last code point of each UTF-8 length, the last BMP char before
        // the surrogates, the first after them, and a supplementary pair.
        final String[] names = {
                "t\u007F",
                "t\u0080",
                "t\u07FF",
                "t\u0800",
                "t\uD7FF",
                "t\uE000",
                "t\uFFFF",
                "t" + new String(Character.toChars(0x10000)),
                "t" + new String(Character.toChars(0x10FFFF)),
                "t😀Ä",
        };
        for (String name : names) {
            final byte[] buf = encodeTableHeader(name, 1, 1);
            final long address = copyToDirectMemory(buf);
            try {
                final QwpTableHeader header = new QwpTableHeader();
                header.parse(address, buf.length);
                Assert.assertEquals(name, header.getTableName());
                Assert.assertEquals(buf.length, header.getBytesConsumed());
            } finally {
                Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testTableNameUtf8() throws QwpParseException {
        // UTF-8 table name with non-ASCII characters
        String tableName = "données_météo";
        byte[] buf = encodeTableHeader(tableName, 50, 3);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);

            Assert.assertEquals(tableName, header.getTableName());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertAsciiFlag(String tableName, boolean isAscii) throws QwpParseException {
        final byte[] buf = encodeTableHeader(tableName, 1, 1);
        final long address = copyToDirectMemory(buf);
        try {
            final QwpTableHeader header = new QwpTableHeader();
            header.parse(address, buf.length);
            Assert.assertEquals(tableName, isAscii, header.getTableNameUtf8().isAscii());
            Assert.assertEquals(tableName, header.getTableName());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long copyToDirectMemory(byte[] buf) {
        return copyToDirectMemory(buf, buf.length);
    }

    private static long copyToDirectMemory(byte[] buf, int length) {
        long address = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < length; i++) {
            Unsafe.putByte(address + i, buf[i]);
        }
        return address;
    }

    private byte[] encodeTableHeader(String tableName, long rowCount, long columnCount) {
        return encodeTableHeader(tableName.getBytes(StandardCharsets.UTF_8), rowCount, columnCount);
    }

    private byte[] encodeTableHeader(byte[] nameBytes, long rowCount, long columnCount) {
        int size = QwpVarint.encodedLength(nameBytes.length) +
                nameBytes.length +
                QwpVarint.encodedLength(rowCount) +
                QwpVarint.encodedLength(columnCount);

        byte[] buf = new byte[size];
        int offset = 0;
        offset = QwpVarint.encode(buf, offset, nameBytes.length);
        System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
        offset += nameBytes.length;
        offset = QwpVarint.encode(buf, offset, rowCount);
        QwpVarint.encode(buf, offset, columnCount);

        return buf;
    }
}
