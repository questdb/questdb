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

package io.questdb.test.cutlass.http.ilpv4;

import io.questdb.cutlass.http.ilpv4.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.http.ilpv4.IlpV4Constants.*;

/**
 * Tests for IlpV4MessageDecoder.
 */
public class IlpV4MessageDecoderTest {

    // ==================== Basic Decode Tests ====================

    @Test
    public void testDecodeEmptyMessage() throws IlpV4ParseException {
        // Message with 0 tables
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeMessageHeader(address, 0, (byte) 0, 0);
            int length = (int) (pos - address);

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            IlpV4DecodedMessage message = decoder.decode(address, length);

            Assert.assertEquals(0, message.getTableCount());
            Assert.assertTrue(message.isEmpty());
            Assert.assertEquals(0, message.getTotalRowCount());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeSingleTableMessage() throws IlpV4ParseException {
        int size = 500;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Build payload first to know its size
            long payloadStart = address + HEADER_SIZE;
            long pos = payloadStart;

            // Table block: "test" with 1 row, 1 INT column
            pos = encodeTableHeader(pos, "test", 1, 1);
            pos = encodeFullSchema(pos, new String[]{"value"}, new byte[]{TYPE_INT}, new boolean[]{false});
            Unsafe.getUnsafe().putInt(pos, 42);
            pos += 4;

            int payloadLength = (int) (pos - payloadStart);

            // Now write message header
            encodeMessageHeader(address, 1, (byte) 0, payloadLength);

            int totalLength = HEADER_SIZE + payloadLength;

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            IlpV4DecodedMessage message = decoder.decode(address, totalLength);

            Assert.assertEquals(1, message.getTableCount());
            Assert.assertFalse(message.isEmpty());
            Assert.assertEquals(1, message.getTotalRowCount());

            IlpV4DecodedTableBlock block = message.getTableBlock(0);
            Assert.assertEquals("test", block.getTableName());
            Assert.assertEquals(42, block.getColumn(0).getInt(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeMultiTableMessage() throws IlpV4ParseException {
        int size = 1000;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Build payload with 3 tables
            long payloadStart = address + HEADER_SIZE;
            long pos = payloadStart;

            // Table 1: "users" with 2 rows
            pos = encodeTableHeader(pos, "users", 2, 1);
            pos = encodeFullSchema(pos, new String[]{"id"}, new byte[]{TYPE_INT}, new boolean[]{false});
            Unsafe.getUnsafe().putInt(pos, 1);
            pos += 4;
            Unsafe.getUnsafe().putInt(pos, 2);
            pos += 4;

            // Table 2: "events" with 3 rows
            pos = encodeTableHeader(pos, "events", 3, 1);
            pos = encodeFullSchema(pos, new String[]{"count"}, new byte[]{TYPE_LONG}, new boolean[]{false});
            for (int i = 0; i < 3; i++) {
                Unsafe.getUnsafe().putLong(pos, i * 100L);
                pos += 8;
            }

            // Table 3: "metrics" with 1 row
            pos = encodeTableHeader(pos, "metrics", 1, 1);
            pos = encodeFullSchema(pos, new String[]{"value"}, new byte[]{TYPE_DOUBLE}, new boolean[]{false});
            Unsafe.getUnsafe().putDouble(pos, 3.14159);
            pos += 8;

            int payloadLength = (int) (pos - payloadStart);
            encodeMessageHeader(address, 3, (byte) 0, payloadLength);

            int totalLength = HEADER_SIZE + payloadLength;

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            IlpV4DecodedMessage message = decoder.decode(address, totalLength);

            Assert.assertEquals(3, message.getTableCount());
            Assert.assertEquals(6, message.getTotalRowCount()); // 2 + 3 + 1

            // Verify each table
            Assert.assertEquals("users", message.getTableBlock(0).getTableName());
            Assert.assertEquals(2, message.getTableBlock(0).getRowCount());
            Assert.assertEquals("events", message.getTableBlock(1).getTableName());
            Assert.assertEquals(3, message.getTableBlock(1).getRowCount());
            Assert.assertEquals("metrics", message.getTableBlock(2).getTableName());
            Assert.assertEquals(1, message.getTableBlock(2).getRowCount());

            // Verify values
            Assert.assertEquals(1, message.getTableBlock(0).getColumn(0).getInt(0));
            Assert.assertEquals(2, message.getTableBlock(0).getColumn(0).getInt(1));
            Assert.assertEquals(100L, message.getTableBlock(1).getColumn(0).getLong(1));
            Assert.assertEquals(3.14159, message.getTableBlock(2).getColumn(0).getDouble(0), 0.00001);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeWithGorillaTimestamps() throws IlpV4ParseException {
        int size = 1000;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Build payload with Gorilla timestamps
            long payloadStart = address + HEADER_SIZE;
            long pos = payloadStart;

            int rowCount = 5;
            pos = encodeTableHeader(pos, "timeseries", rowCount, 1);
            pos = encodeFullSchema(pos, new String[]{"ts"}, new byte[]{TYPE_TIMESTAMP}, new boolean[]{false});

            // Encode timestamps with Gorilla
            long[] timestamps = {
                    1704067200000000L,
                    1704067201000000L,
                    1704067202000000L,
                    1704067203000000L,
                    1704067204000000L
            };
            pos = IlpV4TimestampDecoder.encodeGorilla(pos, timestamps, null);

            int payloadLength = (int) (pos - payloadStart);
            encodeMessageHeader(address, 1, FLAG_GORILLA, payloadLength);

            int totalLength = HEADER_SIZE + payloadLength;

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            IlpV4DecodedMessage message = decoder.decode(address, totalLength);

            Assert.assertTrue(message.isGorillaEnabled());
            Assert.assertEquals(1, message.getTableCount());

            IlpV4DecodedColumn col = message.getTableBlock(0).getColumn(0);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(timestamps[i], col.getTimestamp(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Iterator Tests ====================

    @Test
    public void testDecodeIterateTableBlocks() throws IlpV4ParseException {
        int size = 800;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Build payload with 4 tables
            long payloadStart = address + HEADER_SIZE;
            long pos = payloadStart;

            String[] tableNames = {"t1", "t2", "t3", "t4"};
            for (String tableName : tableNames) {
                pos = encodeTableHeader(pos, tableName, 1, 1);
                pos = encodeFullSchema(pos, new String[]{"x"}, new byte[]{TYPE_INT}, new boolean[]{false});
                Unsafe.getUnsafe().putInt(pos, tableName.hashCode());
                pos += 4;
            }

            int payloadLength = (int) (pos - payloadStart);
            encodeMessageHeader(address, 4, (byte) 0, payloadLength);

            int totalLength = HEADER_SIZE + payloadLength;

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            IlpV4DecodedMessage message = decoder.decode(address, totalLength);

            // Test iteration
            int count = 0;
            for (IlpV4DecodedTableBlock block : message) {
                Assert.assertEquals(tableNames[count], block.getTableName());
                count++;
            }
            Assert.assertEquals(4, count);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testGetTableBlockByName() throws IlpV4ParseException {
        int size = 600;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long payloadStart = address + HEADER_SIZE;
            long pos = payloadStart;

            // Two tables with different names
            pos = encodeTableHeader(pos, "orders", 1, 1);
            pos = encodeFullSchema(pos, new String[]{"id"}, new byte[]{TYPE_INT}, new boolean[]{false});
            Unsafe.getUnsafe().putInt(pos, 123);
            pos += 4;

            pos = encodeTableHeader(pos, "products", 1, 1);
            pos = encodeFullSchema(pos, new String[]{"id"}, new byte[]{TYPE_INT}, new boolean[]{false});
            Unsafe.getUnsafe().putInt(pos, 456);
            pos += 4;

            int payloadLength = (int) (pos - payloadStart);
            encodeMessageHeader(address, 2, (byte) 0, payloadLength);

            int totalLength = HEADER_SIZE + payloadLength;

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            IlpV4DecodedMessage message = decoder.decode(address, totalLength);

            Assert.assertEquals(123, message.getTableBlock("orders").getColumn(0).getInt(0));
            Assert.assertEquals(456, message.getTableBlock("products").getColumn(0).getInt(0));
            Assert.assertNull(message.getTableBlock("nonexistent"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Flag Tests ====================

    @Test
    public void testDecodeAllFlagCombinations() throws IlpV4ParseException {
        // Test that flags are correctly preserved
        byte[] flagValues = {
                0x00,  // no flags
                FLAG_GORILLA,  // only Gorilla
        };
        // Note: Compression flags would throw since compression is not yet supported

        for (byte flags : flagValues) {
            int size = 200;
            long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                long payloadStart = address + HEADER_SIZE;
                long pos = payloadStart;

                pos = encodeTableHeader(pos, "test", 1, 1);
                pos = encodeFullSchema(pos, new String[]{"v"}, new byte[]{TYPE_INT}, new boolean[]{false});
                Unsafe.getUnsafe().putInt(pos, 1);
                pos += 4;

                int payloadLength = (int) (pos - payloadStart);
                encodeMessageHeader(address, 1, flags, payloadLength);

                int totalLength = HEADER_SIZE + payloadLength;

                IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
                IlpV4DecodedMessage message = decoder.decode(address, totalLength);

                Assert.assertEquals(flags, message.getFlags());
                Assert.assertEquals((flags & FLAG_GORILLA) != 0, message.isGorillaEnabled());
            } finally {
                Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testDecodePayloadLengthMismatch() {
        int size = 200;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Claim payload is 1000 bytes, but only provide 50
            encodeMessageHeader(address, 1, (byte) 0, 1000);

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            Assert.assertThrows(IlpV4ParseException.class, () ->
                    decoder.decode(address, 50));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeTableCountMismatch() {
        int size = 200;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Build payload with 1 table but claim 3
            long payloadStart = address + HEADER_SIZE;
            long pos = payloadStart;

            pos = encodeTableHeader(pos, "test", 1, 1);
            pos = encodeFullSchema(pos, new String[]{"v"}, new byte[]{TYPE_INT}, new boolean[]{false});
            Unsafe.getUnsafe().putInt(pos, 1);
            pos += 4;

            int payloadLength = (int) (pos - payloadStart);
            encodeMessageHeader(address, 3, (byte) 0, payloadLength); // Claim 3 tables!

            int totalLength = HEADER_SIZE + payloadLength;

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            Assert.assertThrows(IlpV4ParseException.class, () ->
                    decoder.decode(address, totalLength));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeTruncatedMessage() {
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Only write partial header
            Unsafe.getUnsafe().putInt(address, MAGIC_MESSAGE);
            // Version, flags, etc. missing

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            Assert.assertThrows(IlpV4ParseException.class, () ->
                    decoder.decode(address, 6)); // Not enough for full header
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInvalidMagic() {
        int size = 50;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Write wrong magic
            Unsafe.getUnsafe().putInt(address, 0xDEADBEEF);
            Unsafe.getUnsafe().putByte(address + 4, VERSION_1);
            Unsafe.getUnsafe().putByte(address + 5, (byte) 0);
            Unsafe.getUnsafe().putShort(address + 6, (short) 0);
            Unsafe.getUnsafe().putInt(address + 8, 0);

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            Assert.assertThrows(IlpV4ParseException.class, () ->
                    decoder.decode(address, HEADER_SIZE));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeCompressionNotSupported() {
        int size = 50;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Message with LZ4 compression flag
            encodeMessageHeader(address, 0, FLAG_LZ4, 0);

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            Assert.assertThrows(IlpV4ParseException.class, () ->
                    decoder.decode(address, HEADER_SIZE));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Large Message Tests ====================

    @Test
    public void testDecodeLargeMessage() throws IlpV4ParseException {
        int tableCount = 50;
        int rowsPerTable = 1000;
        int columnsPerTable = 5;

        // Estimate size: header + tables * (tableHeader + schema + data)
        int estimatedSize = HEADER_SIZE + tableCount * (100 + columnsPerTable * 20 + rowsPerTable * columnsPerTable * 8);
        long address = Unsafe.malloc(estimatedSize, MemoryTag.NATIVE_DEFAULT);
        try {
            long payloadStart = address + HEADER_SIZE;
            long pos = payloadStart;

            String[] colNames = {"c0", "c1", "c2", "c3", "c4"};
            byte[] colTypes = {TYPE_LONG, TYPE_LONG, TYPE_LONG, TYPE_LONG, TYPE_LONG};
            boolean[] nullables = {false, false, false, false, false};

            for (int t = 0; t < tableCount; t++) {
                pos = encodeTableHeader(pos, "table" + t, rowsPerTable, columnsPerTable);
                pos = encodeFullSchema(pos, colNames, colTypes, nullables);

                // Write column data
                for (int c = 0; c < columnsPerTable; c++) {
                    for (int r = 0; r < rowsPerTable; r++) {
                        Unsafe.getUnsafe().putLong(pos, (long) t * 1000000 + c * 1000 + r);
                        pos += 8;
                    }
                }
            }

            int payloadLength = (int) (pos - payloadStart);
            encodeMessageHeader(address, tableCount, (byte) 0, payloadLength);

            int totalLength = HEADER_SIZE + payloadLength;

            IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
            IlpV4DecodedMessage message = decoder.decode(address, totalLength);

            Assert.assertEquals(tableCount, message.getTableCount());
            Assert.assertEquals((long) tableCount * rowsPerTable, message.getTotalRowCount());

            // Verify a few values
            Assert.assertEquals(0L, message.getTableBlock(0).getColumn(0).getLong(0));
            Assert.assertEquals(49_000_000L + 4 * 1000 + 999,
                    message.getTableBlock(49).getColumn(4).getLong(999));
        } finally {
            Unsafe.free(address, estimatedSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Byte Array Tests ====================

    @Test
    public void testDecodeFromByteArray() throws IlpV4ParseException {
        // Build message in byte array
        byte[] buf = new byte[500];
        int offset = 0;

        // Message header
        buf[offset++] = 'I';
        buf[offset++] = 'L';
        buf[offset++] = 'P';
        buf[offset++] = '4';
        buf[offset++] = VERSION_1;
        buf[offset++] = 0; // flags
        buf[offset++] = 1; // table count low
        buf[offset++] = 0; // table count high

        // Will fill payload length later
        int payloadLengthOffset = offset;
        offset += 4;

        int payloadStart = offset;

        // Table block using byte-level encoding
        // Table name: "test"
        buf[offset++] = 4; // name length
        buf[offset++] = 't';
        buf[offset++] = 'e';
        buf[offset++] = 's';
        buf[offset++] = 't';
        buf[offset++] = 1; // row count
        buf[offset++] = 1; // column count

        // Schema
        buf[offset++] = IlpV4Schema.SCHEMA_MODE_FULL;
        buf[offset++] = 1; // column name length
        buf[offset++] = 'v';
        buf[offset++] = TYPE_INT;

        // Column data: 42 in little-endian
        buf[offset++] = 42;
        buf[offset++] = 0;
        buf[offset++] = 0;
        buf[offset++] = 0;

        int payloadLength = offset - payloadStart;

        // Fill payload length (little-endian)
        buf[payloadLengthOffset] = (byte) payloadLength;
        buf[payloadLengthOffset + 1] = (byte) (payloadLength >> 8);
        buf[payloadLengthOffset + 2] = (byte) (payloadLength >> 16);
        buf[payloadLengthOffset + 3] = (byte) (payloadLength >> 24);

        IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
        IlpV4DecodedMessage message = decoder.decode(buf, 0, offset);

        Assert.assertEquals(1, message.getTableCount());
        Assert.assertEquals("test", message.getTableBlock(0).getTableName());
        Assert.assertEquals(42, message.getTableBlock(0).getColumn(0).getInt(0));
    }

    // ==================== Memory Management Tests ====================

    @Test
    public void testDecoderReuse() throws IlpV4ParseException {
        IlpV4MessageDecoder decoder = new IlpV4MessageDecoder();
        int size = 300;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Decode multiple messages with same decoder
            for (int i = 0; i < 10; i++) {
                long payloadStart = address + HEADER_SIZE;
                long pos = payloadStart;

                pos = encodeTableHeader(pos, "test" + i, 1, 1);
                pos = encodeFullSchema(pos, new String[]{"v"}, new byte[]{TYPE_INT}, new boolean[]{false});
                Unsafe.getUnsafe().putInt(pos, i);
                pos += 4;

                int payloadLength = (int) (pos - payloadStart);
                encodeMessageHeader(address, 1, (byte) 0, payloadLength);

                int totalLength = HEADER_SIZE + payloadLength;

                decoder.reset();
                IlpV4DecodedMessage message = decoder.decode(address, totalLength);

                Assert.assertEquals("test" + i, message.getTableBlock(0).getTableName());
                Assert.assertEquals(i, message.getTableBlock(0).getColumn(0).getInt(0));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Helper Methods ====================

    private long encodeMessageHeader(long address, int tableCount, byte flags, int payloadLength) {
        Unsafe.getUnsafe().putInt(address, MAGIC_MESSAGE);
        Unsafe.getUnsafe().putByte(address + HEADER_OFFSET_VERSION, VERSION_1);
        Unsafe.getUnsafe().putByte(address + HEADER_OFFSET_FLAGS, flags);
        Unsafe.getUnsafe().putShort(address + HEADER_OFFSET_TABLE_COUNT, (short) tableCount);
        Unsafe.getUnsafe().putInt(address + HEADER_OFFSET_PAYLOAD_LENGTH, payloadLength);
        return address + HEADER_SIZE;
    }

    private long encodeTableHeader(long address, String tableName, int rowCount, int columnCount) {
        byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);
        long pos = address;

        pos = IlpV4Varint.encode(pos, nameBytes.length);
        for (byte b : nameBytes) {
            Unsafe.getUnsafe().putByte(pos++, b);
        }
        pos = IlpV4Varint.encode(pos, rowCount);
        pos = IlpV4Varint.encode(pos, columnCount);

        return pos;
    }

    private long encodeFullSchema(long address, String[] names, byte[] types, boolean[] nullables) {
        long pos = address;

        Unsafe.getUnsafe().putByte(pos++, IlpV4Schema.SCHEMA_MODE_FULL);

        for (int i = 0; i < names.length; i++) {
            byte[] nameBytes = names[i].getBytes(StandardCharsets.UTF_8);
            pos = IlpV4Varint.encode(pos, nameBytes.length);
            for (byte b : nameBytes) {
                Unsafe.getUnsafe().putByte(pos++, b);
            }

            byte typeCode = types[i];
            if (nullables[i]) {
                typeCode |= 0x80;
            }
            Unsafe.getUnsafe().putByte(pos++, typeCode);
        }

        return pos;
    }
}
