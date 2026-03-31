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

import io.questdb.cutlass.qwp.protocol.QwpConstants;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

public class QwpConstantsTest {

    @Test
    public void testDefaultLimits() {
        Assert.assertEquals(16 * 1024 * 1024, DEFAULT_MAX_BATCH_SIZE);
        Assert.assertEquals(1_000_000, DEFAULT_MAX_ROWS_PER_TABLE);
        Assert.assertEquals(2048, MAX_COLUMNS_PER_TABLE);
        Assert.assertEquals(64 * 1024, DEFAULT_INITIAL_RECV_BUFFER_SIZE);
        Assert.assertEquals(4, DEFAULT_MAX_IN_FLIGHT_BATCHES);
    }

    @Test
    public void testFlagBitPositions() {
        // Verify flag bits are at correct positions
        Assert.assertEquals(0x01, FLAG_LZ4);
        Assert.assertEquals(0x02, FLAG_ZSTD);
        Assert.assertEquals(0x04, FLAG_GORILLA);
        Assert.assertEquals(0x03, FLAG_COMPRESSION_MASK);
        Assert.assertEquals(0x08, FLAG_DELTA_SYMBOL_DICT);
    }

    @Test
    public void testGetFixedTypeSize() {
        Assert.assertEquals(0, QwpConstants.getFixedTypeSize(TYPE_BOOLEAN)); // Bit-packed
        Assert.assertEquals(1, QwpConstants.getFixedTypeSize(TYPE_BYTE));
        Assert.assertEquals(2, QwpConstants.getFixedTypeSize(TYPE_SHORT));
        Assert.assertEquals(2, QwpConstants.getFixedTypeSize(TYPE_CHAR));
        Assert.assertEquals(4, QwpConstants.getFixedTypeSize(TYPE_INT));
        Assert.assertEquals(8, QwpConstants.getFixedTypeSize(TYPE_LONG));
        Assert.assertEquals(4, QwpConstants.getFixedTypeSize(TYPE_FLOAT));
        Assert.assertEquals(8, QwpConstants.getFixedTypeSize(TYPE_DOUBLE));
        Assert.assertEquals(8, QwpConstants.getFixedTypeSize(TYPE_TIMESTAMP));
        Assert.assertEquals(8, QwpConstants.getFixedTypeSize(TYPE_TIMESTAMP_NANOS));
        Assert.assertEquals(8, QwpConstants.getFixedTypeSize(TYPE_DATE));
        Assert.assertEquals(16, QwpConstants.getFixedTypeSize(TYPE_UUID));
        Assert.assertEquals(32, QwpConstants.getFixedTypeSize(TYPE_LONG256));
        Assert.assertEquals(8, QwpConstants.getFixedTypeSize(TYPE_DECIMAL64));
        Assert.assertEquals(16, QwpConstants.getFixedTypeSize(TYPE_DECIMAL128));
        Assert.assertEquals(32, QwpConstants.getFixedTypeSize(TYPE_DECIMAL256));

        Assert.assertEquals(-1, QwpConstants.getFixedTypeSize(TYPE_STRING));
        Assert.assertEquals(-1, QwpConstants.getFixedTypeSize(TYPE_SYMBOL));
        Assert.assertEquals(-1, QwpConstants.getFixedTypeSize(TYPE_DOUBLE_ARRAY));
        Assert.assertEquals(-1, QwpConstants.getFixedTypeSize(TYPE_LONG_ARRAY));
    }

    @Test
    public void testGetTypeName() {
        Assert.assertEquals("BOOLEAN", QwpConstants.getTypeName(TYPE_BOOLEAN));
        Assert.assertEquals("INT", QwpConstants.getTypeName(TYPE_INT));
        Assert.assertEquals("STRING", QwpConstants.getTypeName(TYPE_STRING));
        Assert.assertEquals("TIMESTAMP", QwpConstants.getTypeName(TYPE_TIMESTAMP));
        Assert.assertEquals("TIMESTAMP_NANOS", QwpConstants.getTypeName(TYPE_TIMESTAMP_NANOS));
        Assert.assertEquals("DOUBLE_ARRAY", QwpConstants.getTypeName(TYPE_DOUBLE_ARRAY));
        Assert.assertEquals("LONG_ARRAY", QwpConstants.getTypeName(TYPE_LONG_ARRAY));
        Assert.assertEquals("DECIMAL64", QwpConstants.getTypeName(TYPE_DECIMAL64));
        Assert.assertEquals("DECIMAL128", QwpConstants.getTypeName(TYPE_DECIMAL128));
        Assert.assertEquals("DECIMAL256", QwpConstants.getTypeName(TYPE_DECIMAL256));
        Assert.assertEquals("CHAR", QwpConstants.getTypeName(TYPE_CHAR));

        // Type codes with high bit set are unknown — the high bit is not used
        byte badInt = (byte) (TYPE_INT | 0x80);
        Assert.assertTrue(QwpConstants.getTypeName(badInt).startsWith("UNKNOWN"));
    }

    @Test
    public void testHeaderSize() {
        Assert.assertEquals(12, HEADER_SIZE);
    }

    @Test
    public void testIsFixedWidthType() {
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_BOOLEAN));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_BYTE));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_SHORT));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_CHAR));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_INT));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_LONG));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_FLOAT));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_DOUBLE));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_TIMESTAMP));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_TIMESTAMP_NANOS));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_DATE));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_UUID));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_LONG256));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_DECIMAL64));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_DECIMAL128));
        Assert.assertTrue(QwpConstants.isFixedWidthType(TYPE_DECIMAL256));

        Assert.assertFalse(QwpConstants.isFixedWidthType(TYPE_STRING));
        Assert.assertFalse(QwpConstants.isFixedWidthType(TYPE_SYMBOL));
        Assert.assertFalse(QwpConstants.isFixedWidthType(TYPE_GEOHASH));
        Assert.assertFalse(QwpConstants.isFixedWidthType(TYPE_VARCHAR));
        Assert.assertFalse(QwpConstants.isFixedWidthType(TYPE_DOUBLE_ARRAY));
        Assert.assertFalse(QwpConstants.isFixedWidthType(TYPE_LONG_ARRAY));
    }

    @Test
    public void testMagicBytesCapabilityRequest() {
        // "ILP?" in ASCII
        byte[] expected = new byte[]{'I', 'L', 'P', '?'};
        Assert.assertEquals((byte) (MAGIC_CAPABILITY_REQUEST & 0xFF), expected[0]);
        Assert.assertEquals((byte) ((MAGIC_CAPABILITY_REQUEST >> 8) & 0xFF), expected[1]);
        Assert.assertEquals((byte) ((MAGIC_CAPABILITY_REQUEST >> 16) & 0xFF), expected[2]);
        Assert.assertEquals((byte) ((MAGIC_CAPABILITY_REQUEST >> 24) & 0xFF), expected[3]);
    }

    @Test
    public void testMagicBytesCapabilityResponse() {
        // "ILP!" in ASCII
        byte[] expected = new byte[]{'I', 'L', 'P', '!'};
        Assert.assertEquals((byte) (MAGIC_CAPABILITY_RESPONSE & 0xFF), expected[0]);
        Assert.assertEquals((byte) ((MAGIC_CAPABILITY_RESPONSE >> 8) & 0xFF), expected[1]);
        Assert.assertEquals((byte) ((MAGIC_CAPABILITY_RESPONSE >> 16) & 0xFF), expected[2]);
        Assert.assertEquals((byte) ((MAGIC_CAPABILITY_RESPONSE >> 24) & 0xFF), expected[3]);
    }

    @Test
    public void testMagicBytesFallback() {
        // "ILP0" in ASCII
        byte[] expected = new byte[]{'I', 'L', 'P', '0'};
        Assert.assertEquals((byte) (MAGIC_FALLBACK & 0xFF), expected[0]);
        Assert.assertEquals((byte) ((MAGIC_FALLBACK >> 8) & 0xFF), expected[1]);
        Assert.assertEquals((byte) ((MAGIC_FALLBACK >> 16) & 0xFF), expected[2]);
        Assert.assertEquals((byte) ((MAGIC_FALLBACK >> 24) & 0xFF), expected[3]);
    }

    @Test
    public void testMagicBytesValue() {
        // "QWP1" in ASCII: Q=0x51, W=0x57, P=0x50, 1=0x31
        // Little-endian: 0x31505751
        Assert.assertEquals(0x31505751, MAGIC_MESSAGE);

        // Verify ASCII encoding
        byte[] expected = new byte[]{'Q', 'W', 'P', '1'};
        Assert.assertEquals((byte) (MAGIC_MESSAGE & 0xFF), expected[0]);
        Assert.assertEquals((byte) ((MAGIC_MESSAGE >> 8) & 0xFF), expected[1]);
        Assert.assertEquals((byte) ((MAGIC_MESSAGE >> 16) & 0xFF), expected[2]);
        Assert.assertEquals((byte) ((MAGIC_MESSAGE >> 24) & 0xFF), expected[3]);
    }

    @Test
    public void testSchemaModes() {
        Assert.assertEquals(0x00, SCHEMA_MODE_FULL);
        Assert.assertEquals(0x01, SCHEMA_MODE_REFERENCE);
    }

    @Test
    public void testStatusCodes() {
        Assert.assertEquals(0x00, STATUS_OK);
        Assert.assertEquals(0x01, STATUS_PARTIAL);
        Assert.assertEquals(0x02, STATUS_SCHEMA_REQUIRED);
        Assert.assertEquals(0x03, STATUS_SCHEMA_MISMATCH);
        Assert.assertEquals(0x04, STATUS_TABLE_NOT_FOUND);
        Assert.assertEquals(0x05, STATUS_PARSE_ERROR);
        Assert.assertEquals(0x06, STATUS_INTERNAL_ERROR);
        Assert.assertEquals(0x07, STATUS_OVERLOADED);
    }

    @Test
    public void testTypeCodes() {
        // Verify type codes match specification
        Assert.assertEquals(0x01, TYPE_BOOLEAN);
        Assert.assertEquals(0x02, TYPE_BYTE);
        Assert.assertEquals(0x03, TYPE_SHORT);
        Assert.assertEquals(0x04, TYPE_INT);
        Assert.assertEquals(0x05, TYPE_LONG);
        Assert.assertEquals(0x06, TYPE_FLOAT);
        Assert.assertEquals(0x07, TYPE_DOUBLE);
        Assert.assertEquals(0x08, TYPE_STRING);
        Assert.assertEquals(0x09, TYPE_SYMBOL);
        Assert.assertEquals(0x0A, TYPE_TIMESTAMP);
        Assert.assertEquals(0x0B, TYPE_DATE);
        Assert.assertEquals(0x0C, TYPE_UUID);
        Assert.assertEquals(0x0D, TYPE_LONG256);
        Assert.assertEquals(0x0E, TYPE_GEOHASH);
        Assert.assertEquals(0x0F, TYPE_VARCHAR);
        Assert.assertEquals(0x10, TYPE_TIMESTAMP_NANOS);
        Assert.assertEquals(0x11, TYPE_DOUBLE_ARRAY);
        Assert.assertEquals(0x12, TYPE_LONG_ARRAY);
        Assert.assertEquals(0x13, TYPE_DECIMAL64);
        Assert.assertEquals(0x14, TYPE_DECIMAL128);
        Assert.assertEquals(0x15, TYPE_DECIMAL256);
        Assert.assertEquals(0x16, TYPE_CHAR);
    }
}
