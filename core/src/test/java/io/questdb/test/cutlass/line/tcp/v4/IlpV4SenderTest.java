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

package io.questdb.test.cutlass.line.tcp.v4;

import io.questdb.cutlass.line.tcp.v4.*;
import org.junit.Test;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;
import static org.junit.Assert.*;

/**
 * Tests for IlpV4Sender and related encoding classes.
 */
public class IlpV4SenderTest {

    // ==================== Message Encoder Tests ====================

    @Test
    public void testEncoderWriteHeader() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            encoder.writeHeader(2, 1000);

            byte[] data = encoder.toByteArray();
            assertEquals(HEADER_SIZE, data.length);

            // Magic "ILP4"
            assertEquals('I', data[0]);
            assertEquals('L', data[1]);
            assertEquals('P', data[2]);
            assertEquals('4', data[3]);

            // Version
            assertEquals(VERSION_1, data[4]);

            // Table count (little-endian)
            assertEquals(2, data[6] & 0xFF);
            assertEquals(0, data[7] & 0xFF);

            // Payload length (little-endian)
            assertEquals(1000 & 0xFF, data[8] & 0xFF);
            assertEquals((1000 >> 8) & 0xFF, data[9] & 0xFF);
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderWriteVarint() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            // Small value (1 byte)
            encoder.writeVarint(127);
            assertEquals(1, encoder.getPosition());

            // Medium value (2 bytes)
            encoder.reset();
            encoder.writeVarint(128);
            assertEquals(2, encoder.getPosition());

            // Large value
            encoder.reset();
            encoder.writeVarint(16384);
            assertEquals(3, encoder.getPosition());
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderWriteString() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            encoder.writeString("test");

            byte[] data = encoder.toByteArray();
            assertEquals(5, data.length); // 1 byte length + 4 bytes "test"
            assertEquals(4, data[0]); // length
            assertEquals('t', data[1]);
            assertEquals('e', data[2]);
            assertEquals('s', data[3]);
            assertEquals('t', data[4]);
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderWriteEmptyString() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            encoder.writeString("");

            byte[] data = encoder.toByteArray();
            assertEquals(1, data.length);
            assertEquals(0, data[0]); // zero length
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderWriteNullBitmap() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            boolean[] nulls = {true, false, true, false, false, false, false, true};
            encoder.writeNullBitmap(nulls, 8);

            byte[] data = encoder.toByteArray();
            assertEquals(1, data.length);
            // bit 0, 2, 7 set = 0b10000101 = 0x85
            assertEquals((byte) 0x85, data[0]);
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderWriteBooleanColumn() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            boolean[] values = {true, false, true, true, false, true, false, false};
            encoder.writeBooleanColumn(values, 8);

            byte[] data = encoder.toByteArray();
            assertEquals(1, data.length);
            // bits 0, 2, 3, 5 set = 0b00101101 = 0x2D
            assertEquals((byte) 0x2D, data[0]);
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderWriteLongColumn() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            long[] values = {123456789L, -987654321L};
            encoder.writeLongColumn(values, 2);

            byte[] data = encoder.toByteArray();
            assertEquals(16, data.length);
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderWriteDoubleColumn() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            double[] values = {3.14159, -2.71828};
            encoder.writeDoubleColumn(values, 2);

            byte[] data = encoder.toByteArray();
            assertEquals(16, data.length);
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderBufferGrowth() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder(16); // Start small
        try {
            // Write more than initial capacity
            for (int i = 0; i < 100; i++) {
                encoder.writeLong(i);
            }
            assertEquals(800, encoder.getPosition());
            assertTrue(encoder.getCapacity() >= 800);
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderGorillaFlag() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            assertFalse(encoder.isGorillaEnabled());

            encoder.setGorillaEnabled(true);
            assertTrue(encoder.isGorillaEnabled());

            encoder.setGorillaEnabled(false);
            assertFalse(encoder.isGorillaEnabled());
        } finally {
            encoder.close();
        }
    }

    // ==================== Table Buffer Tests ====================

    @Test
    public void testTableBufferBasic() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test_table");

        assertEquals("test_table", buffer.getTableName());
        assertEquals(0, buffer.getRowCount());
        assertEquals(0, buffer.getColumnCount());
    }

    @Test
    public void testTableBufferAddColumns() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");

        IlpV4TableBuffer.ColumnBuffer col1 = buffer.getOrCreateColumn("id", TYPE_LONG, false);
        IlpV4TableBuffer.ColumnBuffer col2 = buffer.getOrCreateColumn("value", TYPE_DOUBLE, true);

        assertEquals(2, buffer.getColumnCount());
        assertEquals("id", buffer.getColumn(0).getName());
        assertEquals("value", buffer.getColumn(1).getName());
    }

    @Test
    public void testTableBufferAddRow() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");

        IlpV4TableBuffer.ColumnBuffer idCol = buffer.getOrCreateColumn("id", TYPE_LONG, false);
        IlpV4TableBuffer.ColumnBuffer valueCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, true);

        idCol.addLong(1);
        valueCol.addDouble(3.14);
        buffer.nextRow();

        idCol.addLong(2);
        valueCol.addDouble(2.71);
        buffer.nextRow();

        assertEquals(2, buffer.getRowCount());
        assertEquals(2, idCol.getSize());
        assertEquals(2, valueCol.getSize());
    }

    @Test
    public void testTableBufferSymbolColumn() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");

        IlpV4TableBuffer.ColumnBuffer symbolCol = buffer.getOrCreateColumn("city", TYPE_SYMBOL, true);

        symbolCol.addSymbol("London");
        buffer.nextRow();
        symbolCol.addSymbol("Paris");
        buffer.nextRow();
        symbolCol.addSymbol("London"); // Repeat
        buffer.nextRow();

        assertEquals(3, buffer.getRowCount());
    }

    @Test
    public void testTableBufferNullValues() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");

        IlpV4TableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("value", TYPE_DOUBLE, true);

        col.addDouble(1.0);
        buffer.nextRow();
        col.addNull();
        buffer.nextRow();
        col.addDouble(3.0);
        buffer.nextRow();

        assertTrue(col.hasNulls());
        boolean[] nulls = col.getNullBitmap();
        assertFalse(nulls[0]);
        assertTrue(nulls[1]);
        assertFalse(nulls[2]);
    }

    @Test
    public void testTableBufferReset() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");

        IlpV4TableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("id", TYPE_LONG, false);
        col.addLong(1);
        buffer.nextRow();
        col.addLong(2);
        buffer.nextRow();

        assertEquals(2, buffer.getRowCount());

        buffer.reset();

        assertEquals(0, buffer.getRowCount());
        assertEquals(1, buffer.getColumnCount()); // Column def preserved
    }

    @Test
    public void testTableBufferClear() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");

        buffer.getOrCreateColumn("id", TYPE_LONG, false);
        buffer.nextRow();

        buffer.clear();

        assertEquals(0, buffer.getRowCount());
        assertEquals(0, buffer.getColumnCount());
    }

    @Test
    public void testTableBufferSchemaHash() {
        IlpV4TableBuffer buffer1 = new IlpV4TableBuffer("test");
        buffer1.getOrCreateColumn("id", TYPE_LONG, false);
        buffer1.getOrCreateColumn("value", TYPE_DOUBLE, true);

        IlpV4TableBuffer buffer2 = new IlpV4TableBuffer("test2");
        buffer2.getOrCreateColumn("id", TYPE_LONG, false);
        buffer2.getOrCreateColumn("value", TYPE_DOUBLE, true);

        // Same schema should produce same hash
        assertEquals(buffer1.getSchemaHash(), buffer2.getSchemaHash());
    }

    @Test
    public void testTableBufferDifferentSchemaHash() {
        IlpV4TableBuffer buffer1 = new IlpV4TableBuffer("test");
        buffer1.getOrCreateColumn("id", TYPE_LONG, false);

        IlpV4TableBuffer buffer2 = new IlpV4TableBuffer("test");
        buffer2.getOrCreateColumn("id", TYPE_INT, false); // Different type

        // Different schema should produce different hash
        assertNotEquals(buffer1.getSchemaHash(), buffer2.getSchemaHash());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTableBufferTypeMismatch() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");

        buffer.getOrCreateColumn("id", TYPE_LONG, false);
        buffer.getOrCreateColumn("id", TYPE_INT, false); // Same name, different type
    }

    // ==================== Encoding Integration Tests ====================

    @Test
    public void testTableBufferEncode() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test_table");

        IlpV4TableBuffer.ColumnBuffer idCol = buffer.getOrCreateColumn("id", TYPE_LONG, false);
        IlpV4TableBuffer.ColumnBuffer valueCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);

        idCol.addLong(1);
        valueCol.addDouble(1.5);
        buffer.nextRow();

        idCol.addLong(2);
        valueCol.addDouble(2.5);
        buffer.nextRow();

        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            buffer.encode(encoder, false, false);
            assertTrue("Encoded data should be non-empty", encoder.getPosition() > 0);
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testTableBufferEncodeWithSchemaRef() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test");
        buffer.getOrCreateColumn("id", TYPE_LONG, false).addLong(1);
        buffer.nextRow();

        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            buffer.encode(encoder, true, false); // Use schema reference
            assertTrue(encoder.getPosition() > 0);
        } finally {
            encoder.close();
        }
    }

    // ==================== All Types Tests ====================

    @Test
    public void testTableBufferAllTypes() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("all_types");

        buffer.getOrCreateColumn("bool", TYPE_BOOLEAN, false).addBoolean(true);
        buffer.getOrCreateColumn("byte", TYPE_BYTE, false).addByte((byte) 42);
        buffer.getOrCreateColumn("short", TYPE_SHORT, false).addShort((short) 1000);
        buffer.getOrCreateColumn("int", TYPE_INT, false).addInt(100000);
        buffer.getOrCreateColumn("long", TYPE_LONG, false).addLong(9999999999L);
        buffer.getOrCreateColumn("float", TYPE_FLOAT, false).addFloat(3.14f);
        buffer.getOrCreateColumn("double", TYPE_DOUBLE, false).addDouble(2.71828);
        buffer.getOrCreateColumn("string", TYPE_STRING, true).addString("hello");
        buffer.getOrCreateColumn("symbol", TYPE_SYMBOL, true).addSymbol("world");
        buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(System.currentTimeMillis() * 1000);
        buffer.getOrCreateColumn("uuid", TYPE_UUID, true).addUuid(0x0102030405060708L, 0x090A0B0C0D0E0F10L);

        buffer.nextRow();

        assertEquals(1, buffer.getRowCount());
        assertEquals(11, buffer.getColumnCount());

        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            buffer.encode(encoder, false, false);
            assertTrue(encoder.getPosition() > 0);
        } finally {
            encoder.close();
        }
    }

    // ==================== String Column Tests ====================

    @Test
    public void testEncoderStringColumn() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            String[] strings = {"hello", "world", ""};
            encoder.writeStringColumn(strings, 3);

            assertTrue(encoder.getPosition() > 0);
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testEncoderSymbolColumn() {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            int[] indices = {0, 1, 0, 2};
            String[] dictionary = {"a", "b", "c"};
            encoder.writeSymbolColumn(indices, dictionary, 4);

            assertTrue(encoder.getPosition() > 0);
        } finally {
            encoder.close();
        }
    }

    // ==================== Large Data Tests ====================

    @Test
    public void testTableBufferLargeDataset() {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("large");

        IlpV4TableBuffer.ColumnBuffer idCol = buffer.getOrCreateColumn("id", TYPE_LONG, false);
        IlpV4TableBuffer.ColumnBuffer valueCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);
        IlpV4TableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, false);

        int rowCount = 10000;
        long baseTs = 1000000000L;

        for (int i = 0; i < rowCount; i++) {
            idCol.addLong(i);
            valueCol.addDouble(i * 0.1);
            tsCol.addLong(baseTs + i * 1000);
            buffer.nextRow();
        }

        assertEquals(rowCount, buffer.getRowCount());

        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            buffer.encode(encoder, false, true); // With Gorilla
            assertTrue(encoder.getPosition() > 0);
        } finally {
            encoder.close();
        }
    }
}
