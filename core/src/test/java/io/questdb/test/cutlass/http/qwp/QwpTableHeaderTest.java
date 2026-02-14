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

import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableHeader;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class QwpTableHeaderTest {

    // ==================== Table Name Tests ====================

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
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < QwpConstants.MAX_TABLE_NAME_LENGTH; i++) {
            sb.append('a');
        }
        String tableName = sb.toString();
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
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < QwpConstants.MAX_TABLE_NAME_LENGTH + 1; i++) {
            sb.append('a');
        }
        String tableName = sb.toString();
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

    // ==================== Row Count Tests ====================

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
    public void testRowCountLarge() throws QwpParseException {
        // Test with very large row count
        long rowCount = Long.MAX_VALUE >> 1; // Very large but valid
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

    // ==================== Column Count Tests ====================

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

    // ==================== Multiple Headers Tests ====================

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

    // ==================== Encoding Tests ====================

    @Test
    public void testEncodeDecodeRoundTrip() throws QwpParseException {
        QwpTableHeader original = new QwpTableHeader();
        original.setTableName("sensors");
        original.setRowCount(12345);
        original.setColumnCount(8);

        byte[] buf = new byte[original.encodedSize()];
        int written = original.encode(buf, 0);
        Assert.assertEquals(buf.length, written);

        long address = copyToDirectMemory(buf);
        try {
            QwpTableHeader parsed = new QwpTableHeader();
            parsed.parse(address, buf.length);

            Assert.assertEquals(original.getTableName(), parsed.getTableName());
            Assert.assertEquals(original.getRowCount(), parsed.getRowCount());
            Assert.assertEquals(original.getColumnCount(), parsed.getColumnCount());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeDirectMemory() throws QwpParseException {
        QwpTableHeader original = new QwpTableHeader();
        original.setTableName("metrics");
        original.setRowCount(500);
        original.setColumnCount(3);

        int size = original.encodedSize();
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddress = original.encode(address);
            Assert.assertEquals(size, endAddress - address);

            QwpTableHeader parsed = new QwpTableHeader();
            parsed.parse(address, size);

            Assert.assertEquals(original.getTableName(), parsed.getTableName());
            Assert.assertEquals(original.getRowCount(), parsed.getRowCount());
            Assert.assertEquals(original.getColumnCount(), parsed.getColumnCount());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Error Cases ====================

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

    // ==================== Helper Methods ====================

    private static long copyToDirectMemory(byte[] buf) {
        return copyToDirectMemory(buf, buf.length);
    }

    private static long copyToDirectMemory(byte[] buf, int length) {
        long address = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < length; i++) {
            Unsafe.getUnsafe().putByte(address + i, buf[i]);
        }
        return address;
    }

    private byte[] encodeTableHeader(String tableName, long rowCount, int columnCount) {
        byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);
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
