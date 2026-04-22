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

import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSchema;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class QwpSchemaTest {

    private static final int TEST_SCHEMA_ID = 42;

    @Test
    public void testColumnDefEquality() {
        QwpColumnDef col1 = new QwpColumnDef("name", QwpConstants.TYPE_STRING);
        QwpColumnDef col2 = new QwpColumnDef("name", QwpConstants.TYPE_STRING);
        QwpColumnDef col3 = new QwpColumnDef("name", QwpConstants.TYPE_INT);

        Assert.assertEquals(col1, col2);
        Assert.assertNotEquals(col1, col3);
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

    @Test
    public void testColumnDefWireTypeCode() {
        QwpColumnDef col = new QwpColumnDef("col", QwpConstants.TYPE_INT);
        Assert.assertEquals(QwpConstants.TYPE_INT, col.getWireTypeCode());

        // Wire type code equals the type code
        Assert.assertEquals(QwpConstants.TYPE_INT, col.getTypeCode());
    }

    @Test
    public void testColumnNameEmpty() throws QwpParseException {
        // Empty column name with TIMESTAMP type is valid - used for designated timestamp
        byte[] buf = new byte[10];
        int offset = 0;
        buf[offset++] = QwpSchema.SCHEMA_MODE_FULL;
        offset = QwpVarint.encode(buf, offset, 0); // schemaId = 0
        offset = QwpVarint.encode(buf, offset, 0); // empty name length
        buf[offset++] = QwpConstants.TYPE_TIMESTAMP;

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, offset, 1);
        Assert.assertEquals("", result.schema.getColumn(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP, result.schema.getColumn(0).getTypeCode());
        Assert.assertEquals(0, result.schemaId);
    }

    @Test
    public void testColumnNameMaxLength() throws QwpParseException {
        // Create column name at max length
        String longName = "x".repeat(QwpConstants.MAX_COLUMN_NAME_LENGTH);
        QwpColumnDef[] columns = {new QwpColumnDef(longName, QwpConstants.TYPE_STRING)};

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize(TEST_SCHEMA_ID)];
        original.encode(buf, 0, TEST_SCHEMA_ID);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 1);
        Assert.assertEquals(longName, result.schema.getColumn(0).getName());
        Assert.assertEquals(TEST_SCHEMA_ID, result.schemaId);
    }

    @Test
    public void testColumnNameTooLong() {
        // Manually encode a schema with too-long column name
        String longName = new String(new char[QwpConstants.MAX_COLUMN_NAME_LENGTH + 1]).replace('\0', 'x');
        byte[] nameBytes = longName.getBytes(StandardCharsets.UTF_8);

        byte[] buf = new byte[256];
        int offset = 0;
        buf[offset++] = QwpSchema.SCHEMA_MODE_FULL;
        offset = QwpVarint.encode(buf, offset, 0); // schemaId
        offset = QwpVarint.encode(buf, offset, nameBytes.length);
        System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
        offset += nameBytes.length;
        buf[offset++] = QwpConstants.TYPE_INT;

        try {
            QwpSchema.parse(buf, 0, offset, 1);
            Assert.fail("Expected exception for column name too long");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("invalid column name length"));
        }
    }

    @Test
    public void testColumnNameUtf8() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("température", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("数据", QwpConstants.TYPE_STRING)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize(TEST_SCHEMA_ID)];
        original.encode(buf, 0, TEST_SCHEMA_ID);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 2);

        Assert.assertEquals("température", result.schema.getColumn(0).getName());
        Assert.assertEquals("数据", result.schema.getColumn(1).getName());
    }

    @Test
    public void testEncodeDecodeRoundTrip() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("id", QwpConstants.TYPE_LONG),
                new QwpColumnDef("name", QwpConstants.TYPE_STRING),
                new QwpColumnDef("price", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("ts", QwpConstants.TYPE_TIMESTAMP)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize(TEST_SCHEMA_ID)];
        original.encode(buf, 0, TEST_SCHEMA_ID);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 4);

        Assert.assertFalse(result.isReference);
        Assert.assertEquals(TEST_SCHEMA_ID, result.schemaId);
        Assert.assertEquals(original.getColumnCount(), result.schema.getColumnCount());

        for (int i = 0; i < original.getColumnCount(); i++) {
            Assert.assertEquals(original.getColumn(i).getName(), result.schema.getColumn(i).getName());
            Assert.assertEquals(original.getColumn(i).getTypeCode(), result.schema.getColumn(i).getTypeCode());
            Assert.assertEquals(original.getColumn(i).getWireTypeCode(), result.schema.getColumn(i).getWireTypeCode());
        }
    }

    @Test
    public void testEncodeDirectMemory() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("value", QwpConstants.TYPE_LONG)
        };

        QwpSchema original = QwpSchema.create(columns);
        int size = original.encodedSize(TEST_SCHEMA_ID);

        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddress = original.encode(address, TEST_SCHEMA_ID);
            Assert.assertEquals(size, endAddress - address);

            QwpSchema.ParseResult result = QwpSchema.parse(address, size, 1);
            Assert.assertEquals("value", result.schema.getColumn(0).getName());
            Assert.assertEquals(TEST_SCHEMA_ID, result.schemaId);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeReferenceDirectMemory() throws QwpParseException {
        int schemaId = 7;
        int size = 1 + QwpVarint.encodedLength(schemaId); // mode byte + varint

        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long endAddress = QwpSchema.encodeReference(address, schemaId);
            Assert.assertEquals(size, endAddress - address);

            QwpSchema.ParseResult result = QwpSchema.parse(address, size, 0);
            Assert.assertTrue(result.isReference);
            Assert.assertEquals(schemaId, result.schemaId);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInvalidColumnType() {
        // Manually encode a schema with invalid type code
        byte[] buf = new byte[10];
        int offset = 0;
        buf[offset++] = QwpSchema.SCHEMA_MODE_FULL;
        offset = QwpVarint.encode(buf, offset, 0); // schemaId
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
        byte[] buf = new byte[original.encodedSize(TEST_SCHEMA_ID)];
        original.encode(buf, 0, TEST_SCHEMA_ID);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, types.length);

        Assert.assertEquals(types.length, result.schema.getColumnCount());
        for (int i = 0; i < types.length; i++) {
            Assert.assertEquals(types[i], result.schema.getColumn(i).getTypeCode());
        }
    }

    @Test
    public void testParseDirectMemory() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("temp", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("ts", QwpConstants.TYPE_TIMESTAMP)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize(TEST_SCHEMA_ID)];
        original.encode(buf, 0, TEST_SCHEMA_ID);

        long address = Unsafe.malloc(buf.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < buf.length; i++) {
                Unsafe.putByte(address + i, buf[i]);
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
    public void testParseFullSchema() throws QwpParseException {
        // Create a schema with multiple columns
        QwpColumnDef[] columns = {
                new QwpColumnDef("temperature", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("humidity", QwpConstants.TYPE_FLOAT),
                new QwpColumnDef("timestamp", QwpConstants.TYPE_TIMESTAMP)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize(TEST_SCHEMA_ID)];
        original.encode(buf, 0, TEST_SCHEMA_ID);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 3);

        Assert.assertFalse(result.isReference);
        Assert.assertNotNull(result.schema);
        Assert.assertEquals(3, result.schema.getColumnCount());
        Assert.assertEquals("temperature", result.schema.getColumn(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_DOUBLE, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testParseMultipleColumns() throws QwpParseException {
        QwpColumnDef[] columns = new QwpColumnDef[10];
        for (int i = 0; i < 10; i++) {
            columns[i] = new QwpColumnDef("col" + i, QwpConstants.TYPE_INT);
        }

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize(TEST_SCHEMA_ID)];
        original.encode(buf, 0, TEST_SCHEMA_ID);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 10);

        Assert.assertEquals(10, result.schema.getColumnCount());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals("col" + i, result.schema.getColumn(i).getName());
        }
    }

    @Test
    public void testParseSingleColumn() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("value", QwpConstants.TYPE_LONG)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize(TEST_SCHEMA_ID)];
        original.encode(buf, 0, TEST_SCHEMA_ID);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 1);

        Assert.assertEquals(1, result.schema.getColumnCount());
        Assert.assertEquals("value", result.schema.getColumn(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_LONG, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testParseTypeCodeRoundTrip() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("col", QwpConstants.TYPE_DOUBLE)
        };

        QwpSchema original = QwpSchema.create(columns);
        byte[] buf = new byte[original.encodedSize(TEST_SCHEMA_ID)];
        original.encode(buf, 0, TEST_SCHEMA_ID);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 1);

        Assert.assertEquals(QwpConstants.TYPE_DOUBLE, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testSchemaIdPreservedInParse() throws QwpParseException {
        QwpColumnDef[] columns = {
                new QwpColumnDef("col1", QwpConstants.TYPE_INT),
                new QwpColumnDef("col2", QwpConstants.TYPE_DOUBLE)
        };

        QwpSchema schema = QwpSchema.create(columns);

        // Encode with schemaId=5
        byte[] buf = new byte[schema.encodedSize(5)];
        schema.encode(buf, 0, 5);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 2);
        Assert.assertFalse(result.isReference);
        Assert.assertEquals(5, result.schemaId);

        // Encode with schemaId=200 (larger varint)
        byte[] buf2 = new byte[schema.encodedSize(200)];
        schema.encode(buf2, 0, 200);

        QwpSchema.ParseResult result2 = QwpSchema.parse(buf2, 0, buf2.length, 2);
        Assert.assertEquals(200, result2.schemaId);
    }

    @Test
    public void testSchemaReferenceMode() throws QwpParseException {
        int schemaId = 13;
        byte[] buf = new byte[1 + QwpVarint.encodedLength(schemaId)];
        QwpSchema.encodeReference(buf, 0, schemaId);

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 0);

        Assert.assertTrue(result.isReference);
        Assert.assertNull(result.schema);
        Assert.assertEquals(schemaId, result.schemaId);
        Assert.assertEquals(buf.length, result.bytesConsumed);
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
}
