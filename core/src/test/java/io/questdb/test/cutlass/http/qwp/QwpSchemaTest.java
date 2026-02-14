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

import io.questdb.cutlass.qwp.protocol.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class QwpSchemaTest {

    // ==================== Full Schema Parsing ====================

    @Test
    public void testParseFullSchema() throws QwpParseException {
        // Create a schema with multiple columns
        QwpColumnDef[] columns = {
                new QwpColumnDef("temperature", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("humidity", QwpConstants.TYPE_FLOAT),
                new QwpColumnDef("timestamp", QwpConstants.TYPE_TIMESTAMP)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 3);

        Assert.assertFalse(result.isReference);
        Assert.assertNotNull(result.schema);
        Assert.assertEquals(3, result.schema.getColumnCount());
        Assert.assertEquals("temperature", result.schema.getColumn(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_DOUBLE, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testParseSingleColumn() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("value", QwpConstants.TYPE_LONG)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 1);

        Assert.assertEquals(1, result.schema.getColumnCount());
        Assert.assertEquals("value", result.schema.getColumn(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_LONG, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testParseMultipleColumns() throws QwpParseException {
        QwpColumnDef[] columns = new QwpColumnDef[10];
        for (int i = 0; i < 10; i++) {
            columns[i] = new QwpColumnDef("col" + i, QwpConstants.TYPE_INT);
        }

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 10);

        Assert.assertEquals(10, result.schema.getColumnCount());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals("col" + i, result.schema.getColumn(i).getName());
        }
    }

    @Test
    public void testParseAllColumnTypes() throws QwpParseException {
        // Test all 15 type codes (0x01-0x0F)
        byte[] types = {
                QwpConstants.TYPE_BOOLEAN,
                QwpConstants.TYPE_BYTE,
                QwpConstants.TYPE_SHORT,
                QwpConstants.TYPE_INT,
                QwpConstants.TYPE_LONG,
                QwpConstants.TYPE_FLOAT,
                QwpConstants.TYPE_DOUBLE,
                QwpConstants.TYPE_STRING,
                QwpConstants.TYPE_SYMBOL,
                QwpConstants.TYPE_TIMESTAMP,
                QwpConstants.TYPE_DATE,
                QwpConstants.TYPE_UUID,
                QwpConstants.TYPE_LONG256,
                QwpConstants.TYPE_GEOHASH,
                QwpConstants.TYPE_VARCHAR
        };

        QwpColumnDef[] columns = new QwpColumnDef[types.length];
        for (int i = 0; i < types.length; i++) {
            columns[i] = new QwpColumnDef("col_" + QwpConstants.getTypeName(types[i]), types[i]);
        }

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, types.length);

        Assert.assertEquals(types.length, result.schema.getColumnCount());
        for (int i = 0; i < types.length; i++) {
            Assert.assertEquals(types[i], result.schema.getColumn(i).getTypeCode());
        }
    }

    @Test
    public void testParseNullableType() throws QwpParseException {
        // Test nullable flag (high bit set)
        QwpColumnDef[] columns = {
                new QwpColumnDef("nullable_col", (byte) (QwpConstants.TYPE_DOUBLE | 0x80))
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 1);

        Assert.assertTrue(result.schema.getColumn(0).isNullable());
        Assert.assertEquals(QwpConstants.TYPE_DOUBLE, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testColumnNameEmpty() throws QwpParseException {
        // Empty column name with TIMESTAMP type is valid - used for designated timestamp
        byte[] buf = new byte[10];
        int offset = 0;
        buf[offset++] = QwpSchema.SCHEMA_MODE_FULL;
        offset = QwpVarint.encode(buf, offset, 0); // empty name length
        buf[offset++] = QwpConstants.TYPE_TIMESTAMP;

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, offset, 1);
        Assert.assertEquals("", result.schema.getColumn(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testColumnNameMaxLength() throws QwpParseException {
        // Create column name at max length
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < QwpConstants.MAX_COLUMN_NAME_LENGTH; i++) {
            sb.append('x');
        }
        String longName = sb.toString();

        QwpColumnDef[] columns = {
                new QwpColumnDef(longName, QwpConstants.TYPE_STRING)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 1);
        Assert.assertEquals(longName, result.schema.getColumn(0).getName());
    }

    @Test
    public void testColumnNameTooLong() {
        // Manually encode a schema with too-long column name
        String longName = new String(new char[QwpConstants.MAX_COLUMN_NAME_LENGTH + 1]).replace('\0', 'x');
        byte[] nameBytes = longName.getBytes(StandardCharsets.UTF_8);

        byte[] buf = new byte[256];
        int offset = 0;
        buf[offset++] = QwpSchema.SCHEMA_MODE_FULL;
        offset = QwpVarint.encode(buf, offset, nameBytes.length);
        System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
        offset += nameBytes.length;
        buf[offset++] = QwpConstants.TYPE_INT;

        try {
            QwpSchema.parse(buf, 0, offset, 1);
            Assert.fail("Expected exception for column name too long");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("column name too long"));
        }
    }

    @Test
    public void testColumnNameUtf8() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("température", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("数据", QwpConstants.TYPE_STRING)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 2);

        Assert.assertEquals("température", result.schema.getColumn(0).getName());
        Assert.assertEquals("数据", result.schema.getColumn(1).getName());
    }

    // ==================== Schema Reference Mode ====================

    @Test
    public void testSchemaReferenceMode() throws QwpParseException {
        long schemaHash = 0x123456789ABCDEF0L;
        byte[] buf = new byte[9];
        QwpSchema.encodeReference(buf, 0, schemaHash);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 0);

        Assert.assertTrue(result.isReference);
        Assert.assertNull(result.schema);
        Assert.assertEquals(schemaHash, result.schemaHash);
        Assert.assertEquals(9, result.bytesConsumed);
    }

    @Test
    public void testSchemaHashExtraction() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("col1", QwpConstants.TYPE_INT),
                new QwpColumnDef("col2", QwpConstants.TYPE_DOUBLE)
        };

        QwpSchema schema = QwpSchema.create(columns);
        long hash1 = schema.getSchemaHash();

        // Same columns should produce same hash
        QwpSchema schema2 = QwpSchema.create(columns);
        Assert.assertEquals(hash1, schema2.getSchemaHash());

        // Different columns should produce different hash
        QwpColumnDef[] columns2 = {
                new QwpColumnDef("col1", QwpConstants.TYPE_INT),
                new QwpColumnDef("col3", QwpConstants.TYPE_DOUBLE)
        };
        QwpSchema schema3 = QwpSchema.create(columns2);
        Assert.assertNotEquals(hash1, schema3.getSchemaHash());
    }

    @Test
    public void testUnknownSchemaMode() {
        byte[] buf = new byte[]{0x05, 0x00, 0x00, 0x00}; // Unknown mode 0x05

        try {
            QwpSchema.parse(buf, 0, buf.length, 1);
            Assert.fail("Expected exception for unknown schema mode");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("unknown schema mode"));
        }
    }

    @Test
    public void testInvalidColumnType() {
        // Manually encode a schema with invalid type code
        byte[] buf = new byte[10];
        int offset = 0;
        buf[offset++] = QwpSchema.SCHEMA_MODE_FULL;
        buf[offset++] = 3; // name length
        buf[offset++] = 'c';
        buf[offset++] = 'o';
        buf[offset++] = 'l';
        buf[offset++] = 0x1F; // Invalid type code (> 0x0F)

        try {
            QwpSchema.parse(buf, 0, offset, 1);
            Assert.fail("Expected exception for invalid column type");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("invalid column type"));
        }
    }

    // ==================== Direct Memory Tests ====================

    @Test
    public void testParseDirectMemory() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("temp", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("ts", QwpConstants.TYPE_TIMESTAMP)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        long address = Unsafe.malloc(buf.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < buf.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, buf[i]);
            }

            QwpSchema.ParseResult result = QwpSchema.parse(address, buf.length, 2);

            Assert.assertFalse(result.isReference);
            Assert.assertEquals(2, result.schema.getColumnCount());
            Assert.assertEquals("temp", result.schema.getColumn(0).getName());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeDirectMemory() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("value", QwpConstants.TYPE_LONG)
        };

        QwpSchema original = QwpSchema.create(columns);
        int size = original.encodedSize();

        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddress = original.encode(address);
            Assert.assertEquals(size, endAddress - address);

            QwpSchema.ParseResult result = QwpSchema.parse(address, size, 1);
            Assert.assertEquals("value", result.schema.getColumn(0).getName());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeReferenceDirectMemory() throws QwpParseException {
        long schemaHash = 0xDEADBEEFCAFEBABEL;
        int size = 9; // mode byte + 8-byte hash

        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddress = QwpSchema.encodeReference(address, schemaHash);
            Assert.assertEquals(size, endAddress - address);

            QwpSchema.ParseResult result = QwpSchema.parse(address, size, 0);
            Assert.assertTrue(result.isReference);
            Assert.assertEquals(schemaHash, result.schemaHash);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Round-trip Tests ====================

    @Test
    public void testEncodeDecodeRoundTrip() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("id", QwpConstants.TYPE_LONG),
                new QwpColumnDef("name", QwpConstants.TYPE_STRING),
                new QwpColumnDef("price", (byte) (QwpConstants.TYPE_DOUBLE | 0x80)), // nullable
                new QwpColumnDef("ts", QwpConstants.TYPE_TIMESTAMP)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize()];
        original.encode(buf, 0);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 4);

        Assert.assertEquals(original.getSchemaHash(), result.schema.getSchemaHash());
        Assert.assertEquals(original.getColumnCount(), result.schema.getColumnCount());

        for (int i = 0; i < original.getColumnCount(); i++) {
            Assert.assertEquals(original.getColumn(i).getName(), result.schema.getColumn(i).getName());
            Assert.assertEquals(original.getColumn(i).getTypeCode(), result.schema.getColumn(i).getTypeCode());
            Assert.assertEquals(original.getColumn(i).isNullable(), result.schema.getColumn(i).isNullable());
        }
    }

    // ==================== QwpColumnDef Tests ====================

    @Test
    public void testColumnDefEquality() {
        QwpColumnDef col1 = new QwpColumnDef("name", QwpConstants.TYPE_STRING);
        QwpColumnDef col2 = new QwpColumnDef("name", QwpConstants.TYPE_STRING);
        QwpColumnDef col3 = new QwpColumnDef("name", QwpConstants.TYPE_INT);

        Assert.assertEquals(col1, col2);
        Assert.assertNotEquals(col1, col3);
    }

    @Test
    public void testColumnDefNullableEquality() {
        QwpColumnDef col1 = new QwpColumnDef("name", QwpConstants.TYPE_STRING, false);
        QwpColumnDef col2 = new QwpColumnDef("name", QwpConstants.TYPE_STRING, true);

        Assert.assertNotEquals(col1, col2);
    }

    @Test
    public void testColumnDefWireTypeCode() {
        QwpColumnDef nonNullable = new QwpColumnDef("col", QwpConstants.TYPE_INT, false);
        QwpColumnDef nullable = new QwpColumnDef("col", QwpConstants.TYPE_INT, true);

        Assert.assertEquals(QwpConstants.TYPE_INT, nonNullable.getWireTypeCode());
        Assert.assertEquals((byte) (QwpConstants.TYPE_INT | 0x80), nullable.getWireTypeCode());
    }

    @Test
    public void testColumnDefFixedWidth() {
        QwpColumnDef intCol = new QwpColumnDef("col", QwpConstants.TYPE_INT);
        QwpColumnDef stringCol = new QwpColumnDef("col", QwpConstants.TYPE_STRING);

        Assert.assertTrue(intCol.isFixedWidth());
        Assert.assertEquals(4, intCol.getFixedWidth());

        Assert.assertFalse(stringCol.isFixedWidth());
        Assert.assertEquals(-1, stringCol.getFixedWidth());
    }

    @Test
    public void testColumnDefToString() {
        QwpColumnDef col = new QwpColumnDef("temperature", QwpConstants.TYPE_DOUBLE);
        String str = col.toString();
        Assert.assertTrue(str.contains("temperature"));
        Assert.assertTrue(str.contains("DOUBLE"));
    }

    @Test
    public void testColumnDefNullableToString() {
        QwpColumnDef col = new QwpColumnDef("value", QwpConstants.TYPE_DOUBLE, true);
        String str = col.toString();
        Assert.assertTrue(str.contains("?"));
    }

    @Test
    public void testColumnDefValidation() throws QwpParseException {
        QwpColumnDef valid = new QwpColumnDef("col", QwpConstants.TYPE_INT);
        valid.validate(); // Should not throw

        QwpColumnDef invalid = new QwpColumnDef("col", (byte) 0x00);
        try {
            invalid.validate();
            Assert.fail("Expected exception for invalid type");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("invalid column type"));
        }
    }
}
