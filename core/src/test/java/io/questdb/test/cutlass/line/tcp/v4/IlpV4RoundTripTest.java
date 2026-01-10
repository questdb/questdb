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
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Round-trip tests for ILP v4 encoding and decoding.
 * <p>
 * These tests verify that data encoded by the sender components can be
 * correctly decoded by the receiver components.
 */
public class IlpV4RoundTripTest {

    // ==================== Single Table Round Trip ====================

    @Test
    public void testSingleRowRoundTrip() throws Exception {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("test_table");

        // Add data
        buffer.getOrCreateColumn("id", TYPE_LONG, false).addLong(42);
        buffer.getOrCreateColumn("value", TYPE_DOUBLE, false).addDouble(3.14159);
        buffer.getOrCreateColumn("name", TYPE_STRING, true).addString("hello");
        buffer.getOrCreateColumn("tag", TYPE_SYMBOL, true).addSymbol("world");
        buffer.nextRow();

        // Encode
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            // Reserve header space
            int headerPos = encoder.getPosition();
            encoder.setPosition(headerPos + HEADER_SIZE);

            // Encode table
            buffer.encode(encoder, false, false);

            // Write header
            int payloadLen = encoder.getPosition() - headerPos - HEADER_SIZE;
            int endPos = encoder.getPosition();
            encoder.setPosition(headerPos);
            encoder.writeHeader(1, payloadLen);
            encoder.setPosition(endPos);

            // Get encoded bytes
            byte[] encoded = encoder.toByteArray();

            // Decode header
            IlpV4MessageHeader header = new IlpV4MessageHeader();
            header.parse(encoded, 0, HEADER_SIZE);

            Assert.assertEquals(1, header.getTableCount());
            Assert.assertEquals(payloadLen, header.getPayloadLength());

            // Decode table block
            IlpV4TableBlockDecoder tableDecoder = new IlpV4TableBlockDecoder();
            long address = Unsafe.malloc(encoded.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < encoded.length; i++) {
                    Unsafe.getUnsafe().putByte(address + i, encoded[i]);
                }

                IlpV4DecodedTableBlock block = tableDecoder.decode(
                        address + HEADER_SIZE,
                        address + HEADER_SIZE + payloadLen,
                        false // no gorilla
                );

                Assert.assertEquals("test_table", block.getTableName().toString());
                Assert.assertEquals(1, block.getRowCount());
                Assert.assertEquals(4, block.getColumnCount());

                // Verify column values
                Assert.assertEquals(42L, block.getColumn(0).getLong(0));
                Assert.assertEquals(3.14159, block.getColumn(1).getDouble(0), 0.00001);
                Assert.assertEquals("hello", block.getColumn(2).getString(0).toString());
            } finally {
                Unsafe.free(address, encoded.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testMultiRowRoundTrip() throws Exception {
        int rowCount = 100;
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("multi_row");

        IlpV4TableBuffer.ColumnBuffer idCol = buffer.getOrCreateColumn("id", TYPE_LONG, false);
        IlpV4TableBuffer.ColumnBuffer valueCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);
        IlpV4TableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, false);

        for (int i = 0; i < rowCount; i++) {
            idCol.addLong(i);
            valueCol.addDouble(i * 0.1);
            tsCol.addLong(1000000000L + i * 1000L);
            buffer.nextRow();
        }

        // Encode
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            int headerPos = encoder.getPosition();
            encoder.setPosition(headerPos + HEADER_SIZE);
            buffer.encode(encoder, false, false);
            int payloadLen = encoder.getPosition() - headerPos - HEADER_SIZE;
            int endPos = encoder.getPosition();
            encoder.setPosition(headerPos);
            encoder.writeHeader(1, payloadLen);
            encoder.setPosition(endPos);

            byte[] encoded = encoder.toByteArray();

            // Decode
            IlpV4TableBlockDecoder tableDecoder = new IlpV4TableBlockDecoder();
            long address = Unsafe.malloc(encoded.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < encoded.length; i++) {
                    Unsafe.getUnsafe().putByte(address + i, encoded[i]);
                }

                IlpV4DecodedTableBlock block = tableDecoder.decode(
                        address + HEADER_SIZE, address + HEADER_SIZE + payloadLen, false);

                Assert.assertEquals(rowCount, block.getRowCount());

                // Verify values
                for (int i = 0; i < rowCount; i++) {
                    Assert.assertEquals(i, block.getColumn(0).getLong(i));
                    Assert.assertEquals(i * 0.1, block.getColumn(1).getDouble(i), 0.00001);
                    Assert.assertEquals(1000000000L + i * 1000L, block.getColumn(2).getLong(i));
                }
            } finally {
                Unsafe.free(address, encoded.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            encoder.close();
        }
    }

    // ==================== Type Round Trip Tests ====================

    @Test
    public void testBooleanRoundTrip() throws Exception {
        testTypeRoundTrip(TYPE_BOOLEAN, new Object[]{true, false, true, true, false},
                (block, idx, expected) -> Assert.assertEquals((Boolean) expected, block.getColumn(0).getBoolean(idx)));
    }

    @Test
    public void testByteRoundTrip() throws Exception {
        testTypeRoundTrip(TYPE_BYTE, new Object[]{(byte) 0, (byte) 127, (byte) -128, (byte) 42, (byte) -1},
                (block, idx, expected) -> Assert.assertEquals((Byte) expected, (Byte) block.getColumn(0).getByte(idx)));
    }

    @Test
    public void testShortRoundTrip() throws Exception {
        testTypeRoundTrip(TYPE_SHORT, new Object[]{(short) 0, (short) 32767, (short) -32768, (short) 1000, (short) -1},
                (block, idx, expected) -> Assert.assertEquals((Short) expected, (Short) block.getColumn(0).getShort(idx)));
    }

    @Test
    public void testIntRoundTrip() throws Exception {
        testTypeRoundTrip(TYPE_INT, new Object[]{0, Integer.MAX_VALUE, Integer.MIN_VALUE, 123456, -654321},
                (block, idx, expected) -> Assert.assertEquals((Integer) expected, (Integer) block.getColumn(0).getInt(idx)));
    }

    @Test
    public void testLongRoundTrip() throws Exception {
        testTypeRoundTrip(TYPE_LONG, new Object[]{0L, Long.MAX_VALUE, Long.MIN_VALUE, 9999999999L, -9999999999L},
                (block, idx, expected) -> Assert.assertEquals((Long) expected, (Long) block.getColumn(0).getLong(idx)));
    }

    @Test
    public void testFloatRoundTrip() throws Exception {
        testTypeRoundTrip(TYPE_FLOAT, new Object[]{0.0f, Float.MAX_VALUE, Float.MIN_VALUE, 3.14f, -2.71f},
                (block, idx, expected) -> Assert.assertEquals((Float) expected, block.getColumn(0).getFloat(idx), 0.0001f));
    }

    @Test
    public void testDoubleRoundTrip() throws Exception {
        testTypeRoundTrip(TYPE_DOUBLE, new Object[]{0.0, Double.MAX_VALUE, Double.MIN_VALUE, Math.PI, -Math.E},
                (block, idx, expected) -> Assert.assertEquals((Double) expected, block.getColumn(0).getDouble(idx), 0.0000001));
    }

    @Test
    public void testStringRoundTrip() throws Exception {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("string_test");
        IlpV4TableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("str", TYPE_STRING, true);

        String[] values = {"hello", "world", "", "with spaces", "unicode: こんにちは"};
        for (String v : values) {
            col.addString(v);
            buffer.nextRow();
        }

        byte[] encoded = encodeTable(buffer);
        IlpV4DecodedTableBlock block = decodeTable(encoded);

        Assert.assertEquals(values.length, block.getRowCount());
        for (int i = 0; i < values.length; i++) {
            Assert.assertEquals(values[i], block.getColumn(0).getString(i).toString());
        }
    }

    @Test
    public void testSymbolRoundTrip() throws Exception {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("symbol_test");
        IlpV4TableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("sym", TYPE_SYMBOL, true);

        String[] values = {"cat_a", "cat_b", "cat_a", "cat_c", "cat_b", "cat_a"};
        for (String v : values) {
            col.addSymbol(v);
            buffer.nextRow();
        }

        byte[] encoded = encodeTable(buffer);
        IlpV4DecodedTableBlock block = decodeTable(encoded);

        Assert.assertEquals(values.length, block.getRowCount());
        for (int i = 0; i < values.length; i++) {
            Assert.assertEquals(values[i], block.getColumn(0).getSymbol(i).toString());
        }
    }

    @Test
    public void testTimestampRoundTrip() throws Exception {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("ts_test");
        IlpV4TableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, false);

        long[] values = {0L, 1000000000000L, Long.MAX_VALUE - 1, 1609459200000000L, 946684800000000L};
        for (long v : values) {
            col.addLong(v);
            buffer.nextRow();
        }

        byte[] encoded = encodeTable(buffer);
        IlpV4DecodedTableBlock block = decodeTable(encoded);

        Assert.assertEquals(values.length, block.getRowCount());
        for (int i = 0; i < values.length; i++) {
            Assert.assertEquals(values[i], block.getColumn(0).getLong(i));
        }
    }

    @Test
    public void testTimestampWithGorillaRoundTrip() throws Exception {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("gorilla_test");
        IlpV4TableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, false);

        // Constant interval - best case for Gorilla
        long baseTs = 1000000000000L;
        int rowCount = 100;
        for (int i = 0; i < rowCount; i++) {
            col.addLong(baseTs + i * 1000L);
            buffer.nextRow();
        }

        // Encode with Gorilla
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        encoder.setGorillaEnabled(true);
        try {
            int headerPos = encoder.getPosition();
            encoder.setPosition(headerPos + HEADER_SIZE);
            buffer.encode(encoder, false, true); // useGorilla = true
            int payloadLen = encoder.getPosition() - headerPos - HEADER_SIZE;
            int endPos = encoder.getPosition();
            encoder.setPosition(headerPos);
            encoder.writeHeader(1, payloadLen);
            encoder.setPosition(endPos);

            byte[] encoded = encoder.toByteArray();

            // Decode with Gorilla
            IlpV4DecodedTableBlock block = decodeTable(encoded, true);

            Assert.assertEquals(rowCount, block.getRowCount());
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(baseTs + i * 1000L, block.getColumn(0).getLong(i));
            }
        } finally {
            encoder.close();
        }
    }

    // ==================== Null Value Round Trip ====================

    @Test
    public void testNullableColumnRoundTrip() throws Exception {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("null_test");
        IlpV4TableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("value", TYPE_DOUBLE, true);

        // Mix of values and nulls
        col.addDouble(1.0);
        buffer.nextRow();
        col.addNull();
        buffer.nextRow();
        col.addDouble(3.0);
        buffer.nextRow();
        col.addNull();
        buffer.nextRow();
        col.addDouble(5.0);
        buffer.nextRow();

        byte[] encoded = encodeTable(buffer);
        IlpV4DecodedTableBlock block = decodeTable(encoded);

        Assert.assertEquals(5, block.getRowCount());
        IlpV4DecodedColumn colDecoded = block.getColumn(0);

        Assert.assertFalse(colDecoded.isNull(0));
        Assert.assertEquals(1.0, colDecoded.getDouble(0), 0.001);

        Assert.assertTrue(colDecoded.isNull(1));

        Assert.assertFalse(colDecoded.isNull(2));
        Assert.assertEquals(3.0, colDecoded.getDouble(2), 0.001);

        Assert.assertTrue(colDecoded.isNull(3));

        Assert.assertFalse(colDecoded.isNull(4));
        Assert.assertEquals(5.0, colDecoded.getDouble(4), 0.001);
    }

    // ==================== Schema Reference Round Trip ====================

    @Test
    public void testSchemaReferenceRoundTrip() throws Exception {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("schema_ref_test");
        buffer.getOrCreateColumn("id", TYPE_LONG, false).addLong(1);
        buffer.getOrCreateColumn("value", TYPE_DOUBLE, false).addDouble(2.0);
        buffer.nextRow();

        // Get schema hash
        long schemaHash = buffer.getSchemaHash();

        // First, send with full schema to populate cache
        byte[] fullEncoded = encodeTable(buffer, false);
        IlpV4SchemaCache cache = new IlpV4SchemaCache(100);
        IlpV4DecodedTableBlock block1 = decodeTableWithCache(fullEncoded, cache, false);

        // Cache the schema (decoder already cached it, but let's be explicit)
        cache.put("schema_ref_test", IlpV4Schema.create(block1.getSchema()));

        // Now encode with schema reference
        buffer.reset();
        buffer.getOrCreateColumn("id", TYPE_LONG, false).addLong(2);
        buffer.getOrCreateColumn("value", TYPE_DOUBLE, false).addDouble(4.0);
        buffer.nextRow();

        byte[] refEncoded = encodeTable(buffer, true); // useSchemaRef = true

        // Decode with cache lookup
        IlpV4DecodedTableBlock block2 = decodeTableWithCache(refEncoded, cache, false);

        Assert.assertEquals(1, block2.getRowCount());
        Assert.assertEquals(2L, block2.getColumn(0).getLong(0));
        Assert.assertEquals(4.0, block2.getColumn(1).getDouble(0), 0.001);
    }

    // ==================== Large Data Round Trip ====================

    @Test
    public void testLargeDataRoundTrip() throws Exception {
        int rowCount = 10000;
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("large_test");

        IlpV4TableBuffer.ColumnBuffer idCol = buffer.getOrCreateColumn("id", TYPE_LONG, false);
        IlpV4TableBuffer.ColumnBuffer valueCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);
        IlpV4TableBuffer.ColumnBuffer nameCol = buffer.getOrCreateColumn("name", TYPE_STRING, true);
        IlpV4TableBuffer.ColumnBuffer tagCol = buffer.getOrCreateColumn("tag", TYPE_SYMBOL, true);

        for (int i = 0; i < rowCount; i++) {
            idCol.addLong(i);
            valueCol.addDouble(i * 0.001);
            nameCol.addString("row_" + i);
            tagCol.addSymbol("tag_" + (i % 100));
            buffer.nextRow();
        }

        byte[] encoded = encodeTable(buffer);
        IlpV4DecodedTableBlock block = decodeTable(encoded);

        Assert.assertEquals(rowCount, block.getRowCount());

        // Spot check some values
        Assert.assertEquals(0L, block.getColumn(0).getLong(0));
        Assert.assertEquals(5000L, block.getColumn(0).getLong(5000));
        Assert.assertEquals(9999L, block.getColumn(0).getLong(9999));
    }

    // ==================== Helper Methods ====================

    private interface ValueAsserter {
        void assertValue(IlpV4DecodedTableBlock block, int index, Object expected);
    }

    private void testTypeRoundTrip(byte type, Object[] values, ValueAsserter asserter) throws Exception {
        IlpV4TableBuffer buffer = new IlpV4TableBuffer("type_test");
        IlpV4TableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("col", type, false);

        for (Object value : values) {
            if (value instanceof Boolean) {
                col.addBoolean((Boolean) value);
            } else if (value instanceof Byte) {
                col.addByte((Byte) value);
            } else if (value instanceof Short) {
                col.addShort((Short) value);
            } else if (value instanceof Integer) {
                col.addInt((Integer) value);
            } else if (value instanceof Long) {
                col.addLong((Long) value);
            } else if (value instanceof Float) {
                col.addFloat((Float) value);
            } else if (value instanceof Double) {
                col.addDouble((Double) value);
            }
            buffer.nextRow();
        }

        byte[] encoded = encodeTable(buffer);
        IlpV4DecodedTableBlock block = decodeTable(encoded);

        Assert.assertEquals(values.length, block.getRowCount());
        for (int i = 0; i < values.length; i++) {
            asserter.assertValue(block, i, values[i]);
        }
    }

    private byte[] encodeTable(IlpV4TableBuffer buffer) {
        return encodeTable(buffer, false);
    }

    private byte[] encodeTable(IlpV4TableBuffer buffer, boolean useSchemaRef) {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            int headerPos = encoder.getPosition();
            encoder.setPosition(headerPos + HEADER_SIZE);
            buffer.encode(encoder, useSchemaRef, false);
            int payloadLen = encoder.getPosition() - headerPos - HEADER_SIZE;
            int endPos = encoder.getPosition();
            encoder.setPosition(headerPos);
            encoder.writeHeader(1, payloadLen);
            encoder.setPosition(endPos);
            return encoder.toByteArray();
        } finally {
            encoder.close();
        }
    }

    private IlpV4DecodedTableBlock decodeTable(byte[] encoded) throws IlpV4ParseException {
        return decodeTable(encoded, false);
    }

    private IlpV4DecodedTableBlock decodeTable(byte[] encoded, boolean gorilla) throws IlpV4ParseException {
        return decodeTableWithCache(encoded, null, gorilla);
    }

    private IlpV4DecodedTableBlock decodeTableWithCache(byte[] encoded, IlpV4SchemaCache cache, boolean gorilla)
            throws IlpV4ParseException {
        IlpV4MessageHeader header = new IlpV4MessageHeader();
        header.parse(encoded, 0, HEADER_SIZE);

        IlpV4TableBlockDecoder decoder = new IlpV4TableBlockDecoder(cache);
        long address = Unsafe.malloc(encoded.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < encoded.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, encoded[i]);
            }
            long start = address + HEADER_SIZE;
            long limit = start + header.getPayloadLength();
            return decoder.decode(start, limit, gorilla);
        } finally {
            Unsafe.free(address, encoded.length, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
