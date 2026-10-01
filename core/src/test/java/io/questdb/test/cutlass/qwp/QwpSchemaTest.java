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

    @Test
    public void testColumnDefEquality() {
        QwpColumnDef col1 = new QwpColumnDef("name", QwpConstants.TYPE_VARCHAR);
        QwpColumnDef col2 = new QwpColumnDef("name", QwpConstants.TYPE_VARCHAR);
        QwpColumnDef col3 = new QwpColumnDef("name", QwpConstants.TYPE_INT);

        Assert.assertEquals(col1, col2);
        Assert.assertNotEquals(col1, col3);
    }

    @Test
    public void testColumnDefFixedWidth() {
        QwpColumnDef intCol = new QwpColumnDef("col", QwpConstants.TYPE_INT);
        QwpColumnDef stringCol = new QwpColumnDef("col", QwpConstants.TYPE_VARCHAR);

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
        offset = QwpVarint.encode(buf, offset, 0); // empty name length
        buf[offset++] = QwpConstants.TYPE_TIMESTAMP;

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, offset, 1);
        Assert.assertEquals("", result.schema.getColumn(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testColumnNameMaxLength() throws QwpParseException {
        // Create column name at max length
        String longName = "x".repeat(QwpConstants.MAX_COLUMN_NAME_LENGTH);
        byte[] buf = encodeColumns(new QwpColumnDef(longName, QwpConstants.TYPE_VARCHAR));

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
        byte[] buf = encodeColumns(
                new QwpColumnDef("température", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("数据", QwpConstants.TYPE_VARCHAR)
        );

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 2);

        Assert.assertEquals("température", result.schema.getColumn(0).getName());
        Assert.assertEquals("数据", result.schema.getColumn(1).getName());
    }

    @Test
    public void testInvalidColumnType() {
        // Manually encode a schema with invalid type code
        byte[] buf = new byte[10];
        int offset = 0;
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
        byte[] types = {
                QwpConstants.TYPE_BOOLEAN,
                QwpConstants.TYPE_BYTE,
                QwpConstants.TYPE_SHORT,
                QwpConstants.TYPE_INT,
                QwpConstants.TYPE_LONG,
                QwpConstants.TYPE_FLOAT,
                QwpConstants.TYPE_DOUBLE,
                QwpConstants.TYPE_VARCHAR,
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

        byte[] buf = encodeColumns(columns);
        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, types.length);

        Assert.assertEquals(types.length, result.schema.getColumnCount());
        for (int i = 0; i < types.length; i++) {
            Assert.assertEquals(types[i], result.schema.getColumn(i).getTypeCode());
        }
    }

    @Test
    public void testParseDirectMemory() throws QwpParseException {
        byte[] buf = encodeColumns(
                new QwpColumnDef("temp", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("ts", QwpConstants.TYPE_TIMESTAMP)
        );

        long address = Unsafe.malloc(buf.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < buf.length; i++) {
                Unsafe.putByte(address + i, buf[i]);
            }

            QwpSchema.ParseResult result = QwpSchema.parse(address, buf.length, 2);

            Assert.assertEquals(2, result.schema.getColumnCount());
            Assert.assertEquals("temp", result.schema.getColumn(0).getName());
        } finally {
            Unsafe.free(address, buf.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseMultipleColumns() throws QwpParseException {
        QwpColumnDef[] columns = new QwpColumnDef[10];
        for (int i = 0; i < 10; i++) {
            columns[i] = new QwpColumnDef("col" + i, QwpConstants.TYPE_INT);
        }

        byte[] buf = encodeColumns(columns);
        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 10);

        Assert.assertEquals(10, result.schema.getColumnCount());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals("col" + i, result.schema.getColumn(i).getName());
        }
    }

    @Test
    public void testParseSchema() throws QwpParseException {
        // Create a schema with multiple columns
        byte[] buf = encodeColumns(
                new QwpColumnDef("temperature", QwpConstants.TYPE_DOUBLE),
                new QwpColumnDef("humidity", QwpConstants.TYPE_FLOAT),
                new QwpColumnDef("timestamp", QwpConstants.TYPE_TIMESTAMP)
        );

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 3);

        Assert.assertNotNull(result.schema);
        Assert.assertEquals(3, result.schema.getColumnCount());
        Assert.assertEquals("temperature", result.schema.getColumn(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_DOUBLE, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testParseSingleColumn() throws QwpParseException {
        byte[] buf = encodeColumns(new QwpColumnDef("value", QwpConstants.TYPE_LONG));

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 1);

        Assert.assertEquals(1, result.schema.getColumnCount());
        Assert.assertEquals("value", result.schema.getColumn(0).getName());
        Assert.assertEquals(QwpConstants.TYPE_LONG, result.schema.getColumn(0).getTypeCode());
    }

    @Test
    public void testParseTypeCode() throws QwpParseException {
        byte[] buf = encodeColumns(new QwpColumnDef("col", QwpConstants.TYPE_DOUBLE));

        QwpSchema.ParseResult result = QwpSchema.parse(buf, 0, buf.length, 1);

        Assert.assertEquals(QwpConstants.TYPE_DOUBLE, result.schema.getColumn(0).getTypeCode());
    }

    // Builds the inline column-descriptor wire bytes (per column: name_len varint,
    // UTF-8 name, type code) that QwpSchema.parse consumes. Production no longer
    // encodes schemas through QwpSchema -- egress inlines its own emit in
    // QwpResultBatchBuffer -- so this byte builder lives in the parser's test.
    private static byte[] encodeColumns(QwpColumnDef... columns) {
        int size = 0;
        for (QwpColumnDef col : columns) {
            byte[] nameBytes = col.getNameUtf8();
            size += QwpVarint.encodedLength(nameBytes.length) + nameBytes.length + 1;
        }
        byte[] buf = new byte[size];
        int offset = 0;
        for (QwpColumnDef col : columns) {
            byte[] nameBytes = col.getNameUtf8();
            offset = QwpVarint.encode(buf, offset, nameBytes.length);
            System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
            offset += nameBytes.length;
            buf[offset++] = col.getWireTypeCode();
        }
        return buf;
    }
}
