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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Tests ILP v4 encode-decode integration without the network layer.
 * <p>
 * These tests verify that data encoded by IlpV4TableBuffer/IlpV4MessageEncoder
 * can be decoded by IlpV4TableBlockDecoder correctly.
 */
public class IlpV4WalIntegrationTest extends AbstractCairoTest {

    private static final int HEADER_SIZE = 12;

    @Test
    public void testSingleRowEncodeDecodeWithTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Encode a single row with various types
            IlpV4TableBuffer buffer = new IlpV4TableBuffer("test_types");
            buffer.getOrCreateColumn("city", TYPE_SYMBOL, false).addSymbol("London");
            buffer.getOrCreateColumn("temperature", TYPE_DOUBLE, false).addDouble(23.5);
            buffer.getOrCreateColumn("humidity", TYPE_LONG, false).addLong(65);
            buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000000000000L);
            buffer.nextRow();

            // Encode and decode
            byte[] encoded = encodeBuffer(buffer, false, false);
            IlpV4DecodedTableBlock decoded = decodeMessage(encoded);

            // Verify
            Assert.assertEquals("test_types", decoded.getTableName());
            Assert.assertEquals(1, decoded.getRowCount());
            Assert.assertEquals(4, decoded.getColumnCount());
            Assert.assertEquals("London", decoded.getColumn(0).getSymbol(0));
            Assert.assertEquals(23.5, decoded.getColumn(1).getDouble(0), 0.001);
            Assert.assertEquals(65L, decoded.getColumn(2).getLong(0));
            Assert.assertEquals(1000000000000L, decoded.getColumn(3).getTimestamp(0));
        });
    }

    @Test
    public void testMultiRowEncodeDecodeWithTypes() throws Exception {
        assertMemoryLeak(() -> {
            IlpV4TableBuffer buffer = new IlpV4TableBuffer("test_multi");

            for (int i = 0; i < 100; i++) {
                buffer.getOrCreateColumn("id", TYPE_SYMBOL, false).addSymbol("row" + i);
                buffer.getOrCreateColumn("value", TYPE_LONG, false).addLong(i);
                buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000000000000L + i * 1000000L);
                buffer.nextRow();
            }

            byte[] encoded = encodeBuffer(buffer, false, false);
            IlpV4DecodedTableBlock decoded = decodeMessage(encoded);

            Assert.assertEquals(100, decoded.getRowCount());
            Assert.assertEquals("row0", decoded.getColumn(0).getSymbol(0));
            Assert.assertEquals("row99", decoded.getColumn(0).getSymbol(99));
            Assert.assertEquals(0L, decoded.getColumn(1).getLong(0));
            Assert.assertEquals(99L, decoded.getColumn(1).getLong(99));
        });
    }

    @Test
    public void testAllDataTypesEncoding() throws Exception {
        assertMemoryLeak(() -> {
            IlpV4TableBuffer buffer = new IlpV4TableBuffer("all_types");
            buffer.getOrCreateColumn("bool_col", TYPE_BOOLEAN, false).addBoolean(true);
            buffer.getOrCreateColumn("byte_col", TYPE_BYTE, false).addByte((byte) 42);
            buffer.getOrCreateColumn("short_col", TYPE_SHORT, false).addShort((short) 1234);
            buffer.getOrCreateColumn("int_col", TYPE_INT, false).addInt(123456);
            buffer.getOrCreateColumn("long_col", TYPE_LONG, false).addLong(9999999999L);
            buffer.getOrCreateColumn("float_col", TYPE_FLOAT, false).addFloat(2.71828f);
            buffer.getOrCreateColumn("double_col", TYPE_DOUBLE, false).addDouble(3.14159265359);
            buffer.getOrCreateColumn("string_col", TYPE_STRING, false).addString("hello world");
            buffer.getOrCreateColumn("symbol_col", TYPE_SYMBOL, false).addSymbol("sym_value");
            buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000000000000L);
            buffer.nextRow();

            byte[] encoded = encodeBuffer(buffer, false, false);
            IlpV4DecodedTableBlock decoded = decodeMessage(encoded);

            Assert.assertEquals(10, decoded.getColumnCount());
            Assert.assertTrue(decoded.getColumn(0).getBoolean(0));
            Assert.assertEquals(42, decoded.getColumn(1).getByte(0));
            Assert.assertEquals(1234, decoded.getColumn(2).getShort(0));
            Assert.assertEquals(123456, decoded.getColumn(3).getInt(0));
            Assert.assertEquals(9999999999L, decoded.getColumn(4).getLong(0));
            Assert.assertEquals(2.71828f, decoded.getColumn(5).getFloat(0), 0.0001f);
            Assert.assertEquals(3.14159265359, decoded.getColumn(6).getDouble(0), 0.00000001);
            Assert.assertEquals("hello world", decoded.getColumn(7).getString(0));
            Assert.assertEquals("sym_value", decoded.getColumn(8).getSymbol(0));
            Assert.assertEquals(1000000000000L, decoded.getColumn(9).getTimestamp(0));
        });
    }

    @Test
    public void testNullableColumns() throws Exception {
        assertMemoryLeak(() -> {
            IlpV4TableBuffer buffer = new IlpV4TableBuffer("nullable_test");

            // Row 1: all values present
            buffer.getOrCreateColumn("tag", TYPE_SYMBOL, false).addSymbol("full");
            buffer.getOrCreateColumn("value", TYPE_DOUBLE, false).addDouble(1.0);
            buffer.getOrCreateColumn("name", TYPE_STRING, true).addString("first");
            buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000000000000L);
            buffer.nextRow();

            // Row 2: name is null
            buffer.getOrCreateColumn("tag", TYPE_SYMBOL, false).addSymbol("partial");
            buffer.getOrCreateColumn("value", TYPE_DOUBLE, false).addDouble(2.0);
            buffer.getOrCreateColumn("name", TYPE_STRING, true).addNull();
            buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000001000000L);
            buffer.nextRow();

            byte[] encoded = encodeBuffer(buffer, false, false);
            IlpV4DecodedTableBlock decoded = decodeMessage(encoded);

            Assert.assertEquals(2, decoded.getRowCount());
            Assert.assertFalse(decoded.getColumn(2).isNull(0)); // first row has value
            Assert.assertTrue(decoded.getColumn(2).isNull(1));   // second row is null
        });
    }

    @Test
    public void testGorillaTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            IlpV4TableBuffer buffer = new IlpV4TableBuffer("gorilla_test");

            long baseTs = 1000000000000L;
            for (int i = 0; i < 100; i++) {
                buffer.getOrCreateColumn("value", TYPE_LONG, false).addLong(i);
                buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(baseTs + i * 1000000L);
                buffer.nextRow();
            }

            byte[] encoded = encodeBuffer(buffer, false, true); // Enable Gorilla
            IlpV4DecodedTableBlock decoded = decodeMessage(encoded, true);

            Assert.assertEquals(100, decoded.getRowCount());
            for (int i = 0; i < 100; i++) {
                Assert.assertEquals(baseTs + i * 1000000L, decoded.getColumn(1).getTimestamp(i));
            }
        });
    }

    @Test
    public void testLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            IlpV4TableBuffer buffer = new IlpV4TableBuffer("large_test");

            int rowCount = 5000;
            for (int i = 0; i < rowCount; i++) {
                buffer.getOrCreateColumn("partition", TYPE_SYMBOL, false).addSymbol("p" + (i % 10));
                buffer.getOrCreateColumn("id", TYPE_LONG, false).addLong(i);
                buffer.getOrCreateColumn("value", TYPE_DOUBLE, false).addDouble(i * 0.1);
                buffer.getOrCreateColumn("data", TYPE_STRING, false).addString("row_" + i);
                buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000000000000L + i * 1000000L);
                buffer.nextRow();
            }

            byte[] encoded = encodeBuffer(buffer, false, false);
            IlpV4DecodedTableBlock decoded = decodeMessage(encoded);

            Assert.assertEquals(rowCount, decoded.getRowCount());
            Assert.assertEquals(0L, decoded.getColumn(1).getLong(0));
            Assert.assertEquals((long) (rowCount - 1), decoded.getColumn(1).getLong(rowCount - 1));
        });
    }

    @Test
    public void testUnicodeStrings() throws Exception {
        assertMemoryLeak(() -> {
            IlpV4TableBuffer buffer = new IlpV4TableBuffer("unicode_test");

            buffer.getOrCreateColumn("lang", TYPE_SYMBOL, false).addSymbol("English");
            buffer.getOrCreateColumn("text", TYPE_STRING, false).addString("Hello world");
            buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000000000000L);
            buffer.nextRow();

            buffer.getOrCreateColumn("lang", TYPE_SYMBOL, false).addSymbol("French");
            buffer.getOrCreateColumn("text", TYPE_STRING, false).addString("Bonjour");
            buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000001000000L);
            buffer.nextRow();

            byte[] encoded = encodeBuffer(buffer, false, false);
            IlpV4DecodedTableBlock decoded = decodeMessage(encoded);

            Assert.assertEquals(2, decoded.getRowCount());
            Assert.assertEquals("Hello world", decoded.getColumn(1).getString(0));
            Assert.assertEquals("Bonjour", decoded.getColumn(1).getString(1));
        });
    }

    @Test
    public void testSchemaReferenceMode() throws Exception {
        assertMemoryLeak(() -> {
            IlpV4TableBuffer buffer = new IlpV4TableBuffer("schema_ref_test");

            buffer.getOrCreateColumn("id", TYPE_LONG, false).addLong(1);
            buffer.getOrCreateColumn("value", TYPE_DOUBLE, false).addDouble(42.0);
            buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000000000000L);
            buffer.nextRow();

            // First: encode with full schema
            byte[] encodedFull = encodeBuffer(buffer, false, false);

            // Decode first message (populates cache)
            IlpV4SchemaCache cache = new IlpV4SchemaCache(256);
            IlpV4DecodedTableBlock decoded1 = decodeMessageWithCache(encodedFull, cache, false);
            Assert.assertEquals(1, decoded1.getRowCount());

            // Second row
            buffer.getOrCreateColumn("id", TYPE_LONG, false).addLong(2);
            buffer.getOrCreateColumn("value", TYPE_DOUBLE, false).addDouble(84.0);
            buffer.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false).addLong(1000001000000L);
            buffer.nextRow();

            // Encode with schema reference
            byte[] encodedRef = encodeBuffer(buffer, true, false);

            // Decode with cache
            IlpV4DecodedTableBlock decoded2 = decodeMessageWithCache(encodedRef, cache, false);
            Assert.assertEquals(2, decoded2.getRowCount());
        });
    }

    // Helper methods

    private byte[] encodeBuffer(IlpV4TableBuffer buffer, boolean useSchemaRef, boolean useGorilla) {
        IlpV4MessageEncoder encoder = new IlpV4MessageEncoder();
        try {
            encoder.reset();
            encoder.writeHeader(1, 0);
            buffer.encode(encoder, useSchemaRef, useGorilla);
            encoder.updatePayloadLength(encoder.getPosition() - HEADER_SIZE);
            return encoder.toByteArray();
        } finally {
            encoder.close();
        }
    }

    private IlpV4DecodedTableBlock decodeMessage(byte[] encoded) throws IlpV4ParseException {
        return decodeMessage(encoded, false);
    }

    private IlpV4DecodedTableBlock decodeMessage(byte[] encoded, boolean gorillaEnabled) throws IlpV4ParseException {
        return decodeMessageWithCache(encoded, null, gorillaEnabled);
    }

    private IlpV4DecodedTableBlock decodeMessageWithCache(byte[] encoded, IlpV4SchemaCache cache, boolean gorillaEnabled)
            throws IlpV4ParseException {
        IlpV4SchemaCache schemaCache = cache != null ? cache : new IlpV4SchemaCache(256);
        IlpV4TableBlockDecoder tableDecoder = new IlpV4TableBlockDecoder(schemaCache);

        long address = Unsafe.malloc(encoded.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < encoded.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, encoded[i]);
            }

            IlpV4MessageHeader header = new IlpV4MessageHeader();
            header.parse(address, HEADER_SIZE);

            long start = address + HEADER_SIZE;
            long limit = start + header.getPayloadLength();
            return tableDecoder.decode(start, limit, gorillaEnabled);
        } finally {
            Unsafe.free(address, encoded.length, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
