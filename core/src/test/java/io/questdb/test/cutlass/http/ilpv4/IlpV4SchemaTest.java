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

import io.questdb.cutlass.ilpv4.protocol.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class IlpV4SchemaTest {

    // ==================== Full Schema Parsing ====================

    @Test
    public void testParseFullSchema() throws IlpV4ParseException {
        // Create a schema with multiple columns
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef("temperature", IlpV4Constants.TYPE_DOUBLE),
                new IlpV4ColumnDef("humidity", IlpV4Constants.TYPE_FLOAT),
                new IlpV4ColumnDef("timestamp", IlpV4Constants.TYPE_TIMESTAMP)
        };

        IlpV4Schema original = IlpV4Schema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, buf.length, 3);

        Assert.assertFalse(result.isReference);
        Assert.assertNotNull(result.schema);
        Assert.assertEquals(3, result.schema.getColumnCount());
        Assert.assertEquals("temperature", result.schema.getColumn(0).getName());
        Assert.assertEquals(IlpV4Constants.TYPE_DOUBLE, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testParseSingleColumn() throws IlpV4ParseException {
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef("value", IlpV4Constants.TYPE_LONG)
        };

        IlpV4Schema original = IlpV4Schema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, buf.length, 1);

        Assert.assertEquals(1, result.schema.getColumnCount());
        Assert.assertEquals("value", result.schema.getColumn(0).getName());
        Assert.assertEquals(IlpV4Constants.TYPE_LONG, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testParseMultipleColumns() throws IlpV4ParseException {
        IlpV4ColumnDef[] columns = new IlpV4ColumnDef[10];
        for (int i = 0; i < 10; i++) {
            columns[i] = new IlpV4ColumnDef("col" + i, IlpV4Constants.TYPE_INT);
        }

        IlpV4Schema original = IlpV4Schema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, buf.length, 10);

        Assert.assertEquals(10, result.schema.getColumnCount());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals("col" + i, result.schema.getColumn(i).getName());
        }
    }

    @Test
    public void testParseAllColumnTypes() throws IlpV4ParseException {
        // Test all 15 type codes (0x01-0x0F)
        byte[] types = {
                IlpV4Constants.TYPE_BOOLEAN,
                IlpV4Constants.TYPE_BYTE,
                IlpV4Constants.TYPE_SHORT,
                IlpV4Constants.TYPE_INT,
                IlpV4Constants.TYPE_LONG,
                IlpV4Constants.TYPE_FLOAT,
                IlpV4Constants.TYPE_DOUBLE,
                IlpV4Constants.TYPE_STRING,
                IlpV4Constants.TYPE_SYMBOL,
                IlpV4Constants.TYPE_TIMESTAMP,
                IlpV4Constants.TYPE_DATE,
                IlpV4Constants.TYPE_UUID,
                IlpV4Constants.TYPE_LONG256,
                IlpV4Constants.TYPE_GEOHASH,
                IlpV4Constants.TYPE_VARCHAR
        };

        IlpV4ColumnDef[] columns = new IlpV4ColumnDef[types.length];
        for (int i = 0; i < types.length; i++) {
            columns[i] = new IlpV4ColumnDef("col_" + IlpV4Constants.getTypeName(types[i]), types[i]);
        }

        IlpV4Schema original = IlpV4Schema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, buf.length, types.length);

        Assert.assertEquals(types.length, result.schema.getColumnCount());
        for (int i = 0; i < types.length; i++) {
            Assert.assertEquals(types[i], result.schema.getColumn(i).getTypeCode());
        }
    }

    @Test
    public void testParseNullableType() throws IlpV4ParseException {
        // Test nullable flag (high bit set)
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef("nullable_col", (byte) (IlpV4Constants.TYPE_DOUBLE | 0x80))
        };

        IlpV4Schema original = IlpV4Schema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, buf.length, 1);

        Assert.assertTrue(result.schema.getColumn(0).isNullable());
        Assert.assertEquals(IlpV4Constants.TYPE_DOUBLE, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testColumnNameEmpty() throws IlpV4ParseException {
        // Empty column name with TIMESTAMP type is valid - used for designated timestamp
        byte[] buf = new byte[10];
        int offset = 0;
        buf[offset++] = IlpV4Schema.SCHEMA_MODE_FULL;
        offset = IlpV4Varint.encode(buf, offset, 0); // empty name length
        buf[offset++] = IlpV4Constants.TYPE_TIMESTAMP;

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, offset, 1);
        Assert.assertEquals("", result.schema.getColumn(0).getName());
        Assert.assertEquals(IlpV4Constants.TYPE_TIMESTAMP, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testColumnNameMaxLength() throws IlpV4ParseException {
        // Create column name at max length
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < IlpV4Constants.MAX_COLUMN_NAME_LENGTH; i++) {
            sb.append('x');
        }
        String longName = sb.toString();

        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef(longName, IlpV4Constants.TYPE_STRING)
        };

        IlpV4Schema original = IlpV4Schema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, buf.length, 1);
        Assert.assertEquals(longName, result.schema.getColumn(0).getName());
    }

    @Test
    public void testColumnNameTooLong() {
        // Manually encode a schema with too-long column name
        String longName = new String(new char[IlpV4Constants.MAX_COLUMN_NAME_LENGTH + 1]).replace('\0', 'x');
        byte[] nameBytes = longName.getBytes(StandardCharsets.UTF_8);

        byte[] buf = new byte[256];
        int offset = 0;
        buf[offset++] = IlpV4Schema.SCHEMA_MODE_FULL;
        offset = IlpV4Varint.encode(buf, offset, nameBytes.length);
        System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
        offset += nameBytes.length;
        buf[offset++] = IlpV4Constants.TYPE_INT;

        try {
            IlpV4Schema.parse(buf, 0, offset, 1);
            Assert.fail("Expected exception for column name too long");
        } catch (IlpV4ParseException e) {
            Assert.assertTrue(e.getMessage().contains("column name too long"));
        }
    }

    @Test
    public void testColumnNameUtf8() throws IlpV4ParseException {
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef("température", IlpV4Constants.TYPE_DOUBLE),
                new IlpV4ColumnDef("数据", IlpV4Constants.TYPE_STRING)
        };

        IlpV4Schema original = IlpV4Schema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, buf.length, 2);

        Assert.assertEquals("température", result.schema.getColumn(0).getName());
        Assert.assertEquals("数据", result.schema.getColumn(1).getName());
    }

    // ==================== Schema Reference Mode ====================

    @Test
    public void testSchemaReferenceMode() throws IlpV4ParseException {
        long schemaHash = 0x123456789ABCDEF0L;
        byte[] buf = new byte[9];
        IlpV4Schema.encodeReference(buf, 0, schemaHash);

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, buf.length, 0);

        Assert.assertTrue(result.isReference);
        Assert.assertNull(result.schema);
        Assert.assertEquals(schemaHash, result.schemaHash);
        Assert.assertEquals(9, result.bytesConsumed);
    }

    @Test
    public void testSchemaHashExtraction() throws IlpV4ParseException {
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef("col1", IlpV4Constants.TYPE_INT),
                new IlpV4ColumnDef("col2", IlpV4Constants.TYPE_DOUBLE)
        };

        IlpV4Schema schema = IlpV4Schema.create(columns);
        long hash1 = schema.getSchemaHash();

        // Same columns should produce same hash
        IlpV4Schema schema2 = IlpV4Schema.create(columns);
        Assert.assertEquals(hash1, schema2.getSchemaHash());

        // Different columns should produce different hash
        IlpV4ColumnDef[] columns2 = {
                new IlpV4ColumnDef("col1", IlpV4Constants.TYPE_INT),
                new IlpV4ColumnDef("col3", IlpV4Constants.TYPE_DOUBLE)
        };
        IlpV4Schema schema3 = IlpV4Schema.create(columns2);
        Assert.assertNotEquals(hash1, schema3.getSchemaHash());
    }

    @Test
    public void testUnknownSchemaMode() {
        byte[] buf = new byte[]{0x05, 0x00, 0x00, 0x00}; // Unknown mode 0x05

        try {
            IlpV4Schema.parse(buf, 0, buf.length, 1);
            Assert.fail("Expected exception for unknown schema mode");
        } catch (IlpV4ParseException e) {
            Assert.assertTrue(e.getMessage().contains("unknown schema mode"));
        }
    }

    @Test
    public void testInvalidColumnType() {
        // Manually encode a schema with invalid type code
        byte[] buf = new byte[10];
        int offset = 0;
        buf[offset++] = IlpV4Schema.SCHEMA_MODE_FULL;
        buf[offset++] = 3; // name length
        buf[offset++] = 'c';
        buf[offset++] = 'o';
        buf[offset++] = 'l';
        buf[offset++] = 0x1F; // Invalid type code (> 0x0F)

        try {
            IlpV4Schema.parse(buf, 0, offset, 1);
            Assert.fail("Expected exception for invalid column type");
        } catch (IlpV4ParseException e) {
            Assert.assertTrue(e.getMessage().contains("invalid column type"));
        }
    }

    // ==================== Direct Memory Tests ====================

    @Test
    public void testParseDirectMemory() throws IlpV4ParseException {
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef("temp", IlpV4Constants.TYPE_DOUBLE),
                new IlpV4ColumnDef("ts", IlpV4Constants.TYPE_TIMESTAMP)
        };

        IlpV4Schema original = IlpV4Schema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        long address = Unsafe.malloc(buf.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < buf.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, buf[i]);
            }

            IlpV4Schema.ParseResult result = IlpV4Schema.parse(address, buf.length, 2);

            Assert.assertFalse(result.isReference);
            Assert.assertEquals(2, result.schema.getColumnCount());
            Assert.assertEquals("temp", result.schema.getColumn(0).getName());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeDirectMemory() throws IlpV4ParseException {
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef("value", IlpV4Constants.TYPE_LONG)
        };

        IlpV4Schema original = IlpV4Schema.create(columns);
        int size = original.encodedSize();

        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddress = original.encode(address);
            Assert.assertEquals(size, endAddress - address);

            IlpV4Schema.ParseResult result = IlpV4Schema.parse(address, size, 1);
            Assert.assertEquals("value", result.schema.getColumn(0).getName());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeReferenceDirectMemory() throws IlpV4ParseException {
        long schemaHash = 0xDEADBEEFCAFEBABEL;
        int size = 9; // mode byte + 8-byte hash

        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddress = IlpV4Schema.encodeReference(address, schemaHash);
            Assert.assertEquals(size, endAddress - address);

            IlpV4Schema.ParseResult result = IlpV4Schema.parse(address, size, 0);
            Assert.assertTrue(result.isReference);
            Assert.assertEquals(schemaHash, result.schemaHash);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Round-trip Tests ====================

    @Test
    public void testEncodeDecodeRoundTrip() throws IlpV4ParseException {
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef("id", IlpV4Constants.TYPE_LONG),
                new IlpV4ColumnDef("name", IlpV4Constants.TYPE_STRING),
                new IlpV4ColumnDef("price", (byte) (IlpV4Constants.TYPE_DOUBLE | 0x80)), // nullable
                new IlpV4ColumnDef("ts", IlpV4Constants.TYPE_TIMESTAMP)
        };

        IlpV4Schema original = IlpV4Schema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        IlpV4Schema.ParseResult result = IlpV4Schema.parse(buf, 0, buf.length, 4);

        Assert.assertEquals(original.getSchemaHash(), result.schema.getSchemaHash());
        Assert.assertEquals(original.getColumnCount(), result.schema.getColumnCount());

        for (int i = 0; i < original.getColumnCount(); i++) {
            Assert.assertEquals(original.getColumn(i).getName(), result.schema.getColumn(i).getName());
            Assert.assertEquals(original.getColumn(i).getTypeCode(), result.schema.getColumn(i).getTypeCode());
            Assert.assertEquals(original.getColumn(i).isNullable(), result.schema.getColumn(i).isNullable());
        }
    }

    // ==================== IlpV4ColumnDef Tests ====================

    @Test
    public void testColumnDefEquality() {
        IlpV4ColumnDef col1 = new IlpV4ColumnDef("name", IlpV4Constants.TYPE_STRING);
        IlpV4ColumnDef col2 = new IlpV4ColumnDef("name", IlpV4Constants.TYPE_STRING);
        IlpV4ColumnDef col3 = new IlpV4ColumnDef("name", IlpV4Constants.TYPE_INT);

        Assert.assertEquals(col1, col2);
        Assert.assertNotEquals(col1, col3);
    }

    @Test
    public void testColumnDefNullableEquality() {
        IlpV4ColumnDef col1 = new IlpV4ColumnDef("name", IlpV4Constants.TYPE_STRING, false);
        IlpV4ColumnDef col2 = new IlpV4ColumnDef("name", IlpV4Constants.TYPE_STRING, true);

        Assert.assertNotEquals(col1, col2);
    }

    @Test
    public void testColumnDefWireTypeCode() {
        IlpV4ColumnDef nonNullable = new IlpV4ColumnDef("col", IlpV4Constants.TYPE_INT, false);
        IlpV4ColumnDef nullable = new IlpV4ColumnDef("col", IlpV4Constants.TYPE_INT, true);

        Assert.assertEquals(IlpV4Constants.TYPE_INT, nonNullable.getWireTypeCode());
        Assert.assertEquals((byte) (IlpV4Constants.TYPE_INT | 0x80), nullable.getWireTypeCode());
    }

    @Test
    public void testColumnDefFixedWidth() {
        IlpV4ColumnDef intCol = new IlpV4ColumnDef("col", IlpV4Constants.TYPE_INT);
        IlpV4ColumnDef stringCol = new IlpV4ColumnDef("col", IlpV4Constants.TYPE_STRING);

        Assert.assertTrue(intCol.isFixedWidth());
        Assert.assertEquals(4, intCol.getFixedWidth());

        Assert.assertFalse(stringCol.isFixedWidth());
        Assert.assertEquals(-1, stringCol.getFixedWidth());
    }

    @Test
    public void testColumnDefToString() {
        IlpV4ColumnDef col = new IlpV4ColumnDef("temperature", IlpV4Constants.TYPE_DOUBLE);
        String str = col.toString();
        Assert.assertTrue(str.contains("temperature"));
        Assert.assertTrue(str.contains("DOUBLE"));
    }

    @Test
    public void testColumnDefNullableToString() {
        IlpV4ColumnDef col = new IlpV4ColumnDef("value", IlpV4Constants.TYPE_DOUBLE, true);
        String str = col.toString();
        Assert.assertTrue(str.contains("?"));
    }

    @Test
    public void testColumnDefValidation() throws IlpV4ParseException {
        IlpV4ColumnDef valid = new IlpV4ColumnDef("col", IlpV4Constants.TYPE_INT);
        valid.validate(); // Should not throw

        IlpV4ColumnDef invalid = new IlpV4ColumnDef("col", (byte) 0x00);
        try {
            invalid.validate();
            Assert.fail("Expected exception for invalid type");
        } catch (IlpV4ParseException e) {
            Assert.assertTrue(e.getMessage().contains("invalid column type"));
        }
    }
}
