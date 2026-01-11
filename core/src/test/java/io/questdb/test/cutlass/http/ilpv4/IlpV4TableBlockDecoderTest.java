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
 * Tests for IlpV4TableBlockDecoder.
 */
public class IlpV4TableBlockDecoderTest {

    // ==================== Basic Decode Tests ====================

    @Test
    public void testDecodeEmptyTableBlock() throws IlpV4ParseException {
        // Empty table block: table name + 0 rows + 0 columns + schema mode (but no columns)
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // Table name: "test"
            byte[] tableName = "test".getBytes(StandardCharsets.UTF_8);
            pos = IlpV4Varint.encode(pos, tableName.length);
            for (byte b : tableName) {
                Unsafe.getUnsafe().putByte(pos++, b);
            }

            // Row count: 0
            pos = IlpV4Varint.encode(pos, 0);

            // Column count: 0
            pos = IlpV4Varint.encode(pos, 0);

            // Schema mode: full (0x00)
            Unsafe.getUnsafe().putByte(pos++, IlpV4Schema.SCHEMA_MODE_FULL);

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals("test", block.getTableName());
            Assert.assertEquals(0, block.getRowCount());
            Assert.assertEquals(0, block.getColumnCount());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeSingleRowSingleColumn() throws IlpV4ParseException {
        int size = 200;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeTableHeader(address, "metrics", 1, 1);

            // Schema: one LONG column
            pos = encodeFullSchema(pos, new String[]{"value"}, new byte[]{TYPE_LONG}, new boolean[]{false});

            // Column data: single LONG value (42)
            Unsafe.getUnsafe().putLong(pos, 42L);
            pos += 8;

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals("metrics", block.getTableName());
            Assert.assertEquals(1, block.getRowCount());
            Assert.assertEquals(1, block.getColumnCount());
            Assert.assertEquals("value", block.getColumn(0).getName());
            Assert.assertEquals(42L, block.getColumn(0).getLong(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeSingleRowMultipleColumns() throws IlpV4ParseException {
        int size = 500;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeTableHeader(address, "sensors", 1, 3);

            // Schema: LONG, DOUBLE, BOOLEAN
            String[] names = {"id", "temperature", "active"};
            byte[] types = {TYPE_LONG, TYPE_DOUBLE, TYPE_BOOLEAN};
            boolean[] nullables = {false, false, false};
            pos = encodeFullSchema(pos, names, types, nullables);

            // Column 1: LONG (123)
            Unsafe.getUnsafe().putLong(pos, 123L);
            pos += 8;

            // Column 2: DOUBLE (25.5)
            Unsafe.getUnsafe().putDouble(pos, 25.5);
            pos += 8;

            // Column 3: BOOLEAN (true - encoded as run-length: 0x01, count=1)
            // Boolean encoding: bit-packed with possible run-length for large runs
            // For single value, we use bit-packed: 1 byte with bit 0 = 1
            Unsafe.getUnsafe().putByte(pos, (byte) 1);
            pos += 1;

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals("sensors", block.getTableName());
            Assert.assertEquals(1, block.getRowCount());
            Assert.assertEquals(3, block.getColumnCount());

            Assert.assertEquals(123L, block.getColumn(0).getLong(0));
            Assert.assertEquals(25.5, block.getColumn(1).getDouble(0), 0.0001);
            Assert.assertTrue(block.getColumn(2).getBoolean(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeMultipleRows() throws IlpV4ParseException {
        int size = 500;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int rowCount = 5;
            long pos = encodeTableHeader(address, "data", rowCount, 1);

            // Schema: one INT column
            pos = encodeFullSchema(pos, new String[]{"count"}, new byte[]{TYPE_INT}, new boolean[]{false});

            // Column data: 5 INT values
            int[] values = {10, 20, 30, 40, 50};
            for (int value : values) {
                Unsafe.getUnsafe().putInt(pos, value);
                pos += 4;
            }

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals("data", block.getTableName());
            Assert.assertEquals(5, block.getRowCount());
            Assert.assertEquals(1, block.getColumnCount());

            IlpV4DecodedColumn col = block.getColumn(0);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], col.getInt(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== All Column Types Tests ====================

    @Test
    public void testDecodeAllFixedWidthTypes() throws IlpV4ParseException {
        int size = 2000;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Test fixed-width types: BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE
            String[] names = {"col_byte", "col_short", "col_int", "col_long", "col_float", "col_double", "col_date"};
            byte[] types = {TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_DATE};
            boolean[] nullables = {false, false, false, false, false, false, false};

            long pos = encodeTableHeader(address, "types_test", 1, names.length);
            pos = encodeFullSchema(pos, names, types, nullables);

            // Encode values
            Unsafe.getUnsafe().putByte(pos, (byte) 42);
            pos += 1;
            Unsafe.getUnsafe().putShort(pos, (short) 1234);
            pos += 2;
            Unsafe.getUnsafe().putInt(pos, 567890);
            pos += 4;
            Unsafe.getUnsafe().putLong(pos, 1234567890123L);
            pos += 8;
            Unsafe.getUnsafe().putFloat(pos, 3.14f);
            pos += 4;
            Unsafe.getUnsafe().putDouble(pos, 2.71828);
            pos += 8;
            Unsafe.getUnsafe().putLong(pos, 1704067200000000L); // Timestamp in micros
            pos += 8;

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals(7, block.getColumnCount());
            Assert.assertEquals((byte) 42, block.getColumn(0).getByte(0));
            Assert.assertEquals((short) 1234, block.getColumn(1).getShort(0));
            Assert.assertEquals(567890, block.getColumn(2).getInt(0));
            Assert.assertEquals(1234567890123L, block.getColumn(3).getLong(0));
            Assert.assertEquals(3.14f, block.getColumn(4).getFloat(0), 0.0001f);
            Assert.assertEquals(2.71828, block.getColumn(5).getDouble(0), 0.00001);
            Assert.assertEquals(1704067200000000L, block.getColumn(6).getDate(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeUuidColumn() throws IlpV4ParseException {
        int size = 200;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeTableHeader(address, "uuids", 1, 1);
            pos = encodeFullSchema(pos, new String[]{"uuid_col"}, new byte[]{TYPE_UUID}, new boolean[]{false});

            // UUID: 16 bytes, big-endian (hi first, then lo)
            long hi = 0x0123456789ABCDEFL;
            long lo = 0xFEDCBA9876543210L;
            // Write in big-endian format (decoder uses Long.reverseBytes)
            Unsafe.getUnsafe().putLong(pos, Long.reverseBytes(hi));  // hi first (big-endian)
            pos += 8;
            Unsafe.getUnsafe().putLong(pos, Long.reverseBytes(lo));  // lo second (big-endian)
            pos += 8;

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals(hi, block.getColumn(0).getUuidHi(0));
            Assert.assertEquals(lo, block.getColumn(0).getUuidLo(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeBooleanColumn() throws IlpV4ParseException {
        int size = 200;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int rowCount = 8;
            long pos = encodeTableHeader(address, "bools", rowCount, 1);
            pos = encodeFullSchema(pos, new String[]{"flag"}, new byte[]{TYPE_BOOLEAN}, new boolean[]{false});

            // Boolean encoding: bit-packed
            // Values: true, false, true, true, false, false, true, false
            // Bits:   1,    0,     1,    1,    0,     0,     1,    0
            // Byte:   0b01001101 = 0x4D
            Unsafe.getUnsafe().putByte(pos, (byte) 0x4D);
            pos += 1;

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            boolean[] expected = {true, false, true, true, false, false, true, false};
            IlpV4DecodedColumn col = block.getColumn(0);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals("Row " + i, expected[i], col.getBoolean(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Schema Tests ====================

    @Test
    public void testDecodeWithFullSchema() throws IlpV4ParseException {
        int size = 300;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeTableHeader(address, "full_schema", 1, 2);
            pos = encodeFullSchema(pos, new String[]{"a", "b"}, new byte[]{TYPE_INT, TYPE_LONG}, new boolean[]{false, false});

            Unsafe.getUnsafe().putInt(pos, 100);
            pos += 4;
            Unsafe.getUnsafe().putLong(pos, 200L);
            pos += 8;

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals("a", block.getColumn(0).getName());
            Assert.assertEquals("b", block.getColumn(1).getName());
            Assert.assertEquals(100, block.getColumn(0).getInt(0));
            Assert.assertEquals(200L, block.getColumn(1).getLong(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeWithSchemaReference() throws IlpV4ParseException {
        int size = 500;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // First, create a full schema message to cache
            long pos1 = encodeTableHeader(address, "cached_table", 1, 1);
            long schemaStart = pos1;
            pos1 = encodeFullSchema(pos1, new String[]{"value"}, new byte[]{TYPE_INT}, new boolean[]{false});
            Unsafe.getUnsafe().putInt(pos1, 42);
            pos1 += 4;

            int length1 = (int) (pos1 - address);

            IlpV4SchemaCache cache = new IlpV4SchemaCache();
            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder(cache);

            // Decode first message to cache schema
            IlpV4DecodedTableBlock block1 = decoder.decode(address, address + length1, false);
            Assert.assertEquals(42, block1.getColumn(0).getInt(0));

            // Create the schema to get its hash
            IlpV4ColumnDef[] cols = new IlpV4ColumnDef[]{new IlpV4ColumnDef("value", TYPE_INT, false)};
            IlpV4Schema schema = IlpV4Schema.create(cols);
            long schemaHash = schema.getSchemaHash();

            // Now create a message with schema reference
            long address2 = address + 256; // Use different area
            pos1 = encodeTableHeader(address2, "cached_table", 1, 1);

            // Schema reference mode (0x01) + hash
            Unsafe.getUnsafe().putByte(pos1, IlpV4Schema.SCHEMA_MODE_REFERENCE);
            pos1 += 1;
            Unsafe.getUnsafe().putLong(pos1, schemaHash);
            pos1 += 8;

            // Column data
            Unsafe.getUnsafe().putInt(pos1, 99);
            pos1 += 4;

            int length2 = (int) (pos1 - address2);

            // Decode second message using cached schema
            IlpV4DecodedTableBlock block2 = decoder.decode(address2, address2 + length2, false);
            Assert.assertEquals(99, block2.getColumn(0).getInt(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeSchemaReferenceNotCached() {
        int size = 200;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeTableHeader(address, "uncached", 1, 1);

            // Schema reference mode with unknown hash
            Unsafe.getUnsafe().putByte(pos, IlpV4Schema.SCHEMA_MODE_REFERENCE);
            pos += 1;
            Unsafe.getUnsafe().putLong(pos, 0x12345678DEADBEEFL);
            pos += 8;

            int length = (int) (pos - address);

            IlpV4SchemaCache cache = new IlpV4SchemaCache();
            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder(cache);

            Assert.assertThrows(IlpV4ParseException.class, () ->
                    decoder.decode(address, address + length, false));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Nullable Tests ====================

    @Test
    public void testDecodeMixedNullableNonNullable() throws IlpV4ParseException {
        int size = 500;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int rowCount = 4;
            long pos = encodeTableHeader(address, "nullable_test", rowCount, 2);

            // Column 1: INT (non-nullable), Column 2: LONG (nullable)
            pos = encodeFullSchema(pos,
                    new String[]{"required", "optional"},
                    new byte[]{TYPE_INT, (byte) (TYPE_LONG | 0x80)},  // nullable flag
                    new boolean[]{false, true});

            // Column 1: 4 INT values (non-nullable, no bitmap)
            int[] intValues = {1, 2, 3, 4};
            for (int v : intValues) {
                Unsafe.getUnsafe().putInt(pos, v);
                pos += 4;
            }

            // Column 2: nullable LONG
            // Null bitmap first: rows 1 and 3 are null (bits 1 and 3)
            // Bitmap byte: 0b00001010 = 0x0A
            Unsafe.getUnsafe().putByte(pos, (byte) 0x0A);
            pos += 1;

            // Only NON-NULL values are written (no placeholders for nulls)
            Unsafe.getUnsafe().putLong(pos, 100L);  // row 0
            pos += 8;
            // row 1 is null - NO space used
            Unsafe.getUnsafe().putLong(pos, 300L);  // row 2
            pos += 8;
            // row 3 is null - NO space used

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            // Verify non-nullable column
            IlpV4DecodedColumn intCol = block.getColumn(0);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertFalse(intCol.isNull(i));
                Assert.assertEquals(intValues[i], intCol.getInt(i));
            }

            // Verify nullable column
            IlpV4DecodedColumn longCol = block.getColumn(1);
            Assert.assertFalse(longCol.isNull(0));
            Assert.assertEquals(100L, longCol.getLong(0));
            Assert.assertTrue(longCol.isNull(1));
            Assert.assertFalse(longCol.isNull(2));
            Assert.assertEquals(300L, longCol.getLong(2));
            Assert.assertTrue(longCol.isNull(3));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Column Access Tests ====================

    @Test
    public void testColumnAccessByIndex() throws IlpV4ParseException {
        int size = 300;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeTableHeader(address, "indexed", 1, 3);
            pos = encodeFullSchema(pos,
                    new String[]{"first", "second", "third"},
                    new byte[]{TYPE_INT, TYPE_INT, TYPE_INT},
                    new boolean[]{false, false, false});

            Unsafe.getUnsafe().putInt(pos, 111);
            pos += 4;
            Unsafe.getUnsafe().putInt(pos, 222);
            pos += 4;
            Unsafe.getUnsafe().putInt(pos, 333);
            pos += 4;

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals(111, block.getColumn(0).getInt(0));
            Assert.assertEquals(222, block.getColumn(1).getInt(0));
            Assert.assertEquals(333, block.getColumn(2).getInt(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testColumnAccessByName() throws IlpV4ParseException {
        int size = 300;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeTableHeader(address, "named", 1, 3);
            pos = encodeFullSchema(pos,
                    new String[]{"alpha", "beta", "gamma"},
                    new byte[]{TYPE_INT, TYPE_INT, TYPE_INT},
                    new boolean[]{false, false, false});

            Unsafe.getUnsafe().putInt(pos, 1);
            pos += 4;
            Unsafe.getUnsafe().putInt(pos, 2);
            pos += 4;
            Unsafe.getUnsafe().putInt(pos, 3);
            pos += 4;

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals(1, block.getColumn("alpha").getInt(0));
            Assert.assertEquals(2, block.getColumn("beta").getInt(0));
            Assert.assertEquals(3, block.getColumn("gamma").getInt(0));
            Assert.assertNull(block.getColumn("nonexistent"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeColumnOrderPreserved() throws IlpV4ParseException {
        int size = 400;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            String[] names = {"z", "y", "x", "w", "v"};
            byte[] types = {TYPE_INT, TYPE_INT, TYPE_INT, TYPE_INT, TYPE_INT};
            boolean[] nullables = {false, false, false, false, false};

            long pos = encodeTableHeader(address, "ordered", 1, 5);
            pos = encodeFullSchema(pos, names, types, nullables);

            for (int i = 0; i < 5; i++) {
                Unsafe.getUnsafe().putInt(pos, i * 10);
                pos += 4;
            }

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            for (int i = 0; i < 5; i++) {
                Assert.assertEquals(names[i], block.getColumn(i).getName());
                Assert.assertEquals(i * 10, block.getColumn(i).getInt(0));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Iterator Tests ====================

    @Test
    public void testRowIteration() throws IlpV4ParseException {
        int size = 500;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int rowCount = 5;
            long pos = encodeTableHeader(address, "rows", rowCount, 2);
            pos = encodeFullSchema(pos,
                    new String[]{"id", "value"},
                    new byte[]{TYPE_INT, TYPE_LONG},
                    new boolean[]{false, false});

            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putInt(pos, i);
                pos += 4;
            }
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putLong(pos, i * 100L);
                pos += 8;
            }

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            IlpV4DecodedTableBlock.RowIterator rows = block.rows();
            int count = 0;
            while (rows.hasNext()) {
                int row = rows.next();
                Assert.assertEquals(row, rows.getInt(0));
                Assert.assertEquals(row * 100L, rows.getLong(1));
                count++;
            }
            Assert.assertEquals(rowCount, count);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testColumnIteration() throws IlpV4ParseException {
        int size = 300;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            String[] names = {"a", "b", "c"};
            byte[] types = {TYPE_INT, TYPE_INT, TYPE_INT};
            boolean[] nullables = {false, false, false};

            long pos = encodeTableHeader(address, "cols", 1, 3);
            pos = encodeFullSchema(pos, names, types, nullables);

            for (int i = 0; i < 3; i++) {
                Unsafe.getUnsafe().putInt(pos, (i + 1) * 100);
                pos += 4;
            }

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            IlpV4DecodedTableBlock.ColumnIterator cols = block.columns();
            int count = 0;
            while (cols.hasNext()) {
                IlpV4DecodedColumn col = cols.next();
                Assert.assertEquals(names[count], col.getName());
                Assert.assertEquals((count + 1) * 100, col.getInt(0));
                count++;
            }
            Assert.assertEquals(3, count);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testDecodeIncompleteData() {
        int size = 50;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeTableHeader(address, "incomplete", 1, 1);
            pos = encodeFullSchema(pos, new String[]{"value"}, new byte[]{TYPE_LONG}, new boolean[]{false});
            // Don't write column data - incomplete

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            Assert.assertThrows(IlpV4ParseException.class, () ->
                    decoder.decode(address, address + length, false));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeSchemaReferenceModeWithoutCache() {
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = encodeTableHeader(address, "no_cache", 1, 1);

            // Schema reference mode
            Unsafe.getUnsafe().putByte(pos, IlpV4Schema.SCHEMA_MODE_REFERENCE);
            pos += 1;
            Unsafe.getUnsafe().putLong(pos, 0x1234567890ABCDEFL);
            pos += 8;

            int length = (int) (pos - address);

            // Decoder without cache
            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            Assert.assertThrows(IlpV4ParseException.class, () ->
                    decoder.decode(address, address + length, false));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Large Block Tests ====================

    @Test
    public void testDecodeLargeBlock() throws IlpV4ParseException {
        int rowCount = 10000;
        int columnCount = 10;
        int estimatedSize = 1000 + rowCount * columnCount * 8;
        long address = Unsafe.malloc(estimatedSize, MemoryTag.NATIVE_DEFAULT);
        try {
            String[] names = new String[columnCount];
            byte[] types = new byte[columnCount];
            boolean[] nullables = new boolean[columnCount];
            for (int i = 0; i < columnCount; i++) {
                names[i] = "col" + i;
                types[i] = TYPE_LONG;
                nullables[i] = false;
            }

            long pos = encodeTableHeader(address, "large_table", rowCount, columnCount);
            pos = encodeFullSchema(pos, names, types, nullables);

            // Encode column data
            for (int col = 0; col < columnCount; col++) {
                for (int row = 0; row < rowCount; row++) {
                    Unsafe.getUnsafe().putLong(pos, (long) col * rowCount + row);
                    pos += 8;
                }
            }

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false);

            Assert.assertEquals(rowCount, block.getRowCount());
            Assert.assertEquals(columnCount, block.getColumnCount());

            // Verify some values
            for (int col = 0; col < columnCount; col++) {
                IlpV4DecodedColumn decodedCol = block.getColumn(col);
                Assert.assertEquals((long) col * rowCount, decodedCol.getLong(0));
                Assert.assertEquals((long) col * rowCount + rowCount - 1, decodedCol.getLong(rowCount - 1));
            }
        } finally {
            Unsafe.free(address, estimatedSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Timestamp with Gorilla Tests ====================

    @Test
    public void testDecodeTimestampWithGorilla() throws IlpV4ParseException {
        int size = 500;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int rowCount = 5;
            long pos = encodeTableHeader(address, "timestamps", rowCount, 1);
            pos = encodeFullSchema(pos, new String[]{"ts"}, new byte[]{TYPE_TIMESTAMP}, new boolean[]{false});

            // Encode timestamps using Gorilla compression
            long[] timestamps = {
                    1704067200000000L,
                    1704067201000000L,
                    1704067202000000L,
                    1704067203000000L,
                    1704067204000000L
            };
            pos = IlpV4TimestampDecoder.encodeGorilla(pos, timestamps, null);

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, true); // Gorilla enabled

            IlpV4DecodedColumn col = block.getColumn(0);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(timestamps[i], col.getTimestamp(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeTimestampWithoutGorilla() throws IlpV4ParseException {
        int size = 500;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int rowCount = 3;
            long pos = encodeTableHeader(address, "raw_timestamps", rowCount, 1);
            pos = encodeFullSchema(pos, new String[]{"ts"}, new byte[]{TYPE_TIMESTAMP}, new boolean[]{false});

            // Write raw LONG values (not Gorilla encoded)
            long[] timestamps = {1704067200000000L, 1704067201000000L, 1704067202000000L};
            for (long ts : timestamps) {
                Unsafe.getUnsafe().putLong(pos, ts);
                pos += 8;
            }

            int length = (int) (pos - address);

            IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder();
            IlpV4DecodedTableBlock block = decoder.decode(address, address + length, false); // Gorilla disabled

            IlpV4DecodedColumn col = block.getColumn(0);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(timestamps[i], col.getTimestamp(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Helper Methods ====================

    private long encodeTableHeader(long address, String tableName, int rowCount, int columnCount) {
        byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);
        long pos = address;

        // Table name
        pos = IlpV4Varint.encode(pos, nameBytes.length);
        for (byte b : nameBytes) {
            Unsafe.getUnsafe().putByte(pos++, b);
        }

        // Row count
        pos = IlpV4Varint.encode(pos, rowCount);

        // Column count
        pos = IlpV4Varint.encode(pos, columnCount);

        return pos;
    }

    private long encodeFullSchema(long address, String[] names, byte[] types, boolean[] nullables) {
        long pos = address;

        // Schema mode: full
        Unsafe.getUnsafe().putByte(pos++, IlpV4Schema.SCHEMA_MODE_FULL);

        // Column definitions
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
