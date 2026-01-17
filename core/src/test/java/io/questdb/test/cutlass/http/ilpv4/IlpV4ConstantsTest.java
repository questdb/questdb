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

import io.questdb.cutlass.ilpv4.protocol.IlpV4Constants;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.ilpv4.protocol.IlpV4Constants.*;

public class IlpV4ConstantsTest {

    @Test
    public void testMagicBytesValue() {
        // "ILP4" in ASCII: I=0x49, L=0x4C, P=0x50, 4=0x34
        // Little-endian: 0x34504C49
        Assert.assertEquals(0x34504C49, MAGIC_MESSAGE);

        // Verify ASCII encoding
        byte[] expected = new byte[]{'I', 'L', 'P', '4'};
        Assert.assertEquals(expected[0], (byte) (MAGIC_MESSAGE & 0xFF));
        Assert.assertEquals(expected[1], (byte) ((MAGIC_MESSAGE >> 8) & 0xFF));
        Assert.assertEquals(expected[2], (byte) ((MAGIC_MESSAGE >> 16) & 0xFF));
        Assert.assertEquals(expected[3], (byte) ((MAGIC_MESSAGE >> 24) & 0xFF));
    }

    @Test
    public void testMagicBytesCapabilityRequest() {
        // "ILP?" in ASCII
        byte[] expected = new byte[]{'I', 'L', 'P', '?'};
        Assert.assertEquals(expected[0], (byte) (MAGIC_CAPABILITY_REQUEST & 0xFF));
        Assert.assertEquals(expected[1], (byte) ((MAGIC_CAPABILITY_REQUEST >> 8) & 0xFF));
        Assert.assertEquals(expected[2], (byte) ((MAGIC_CAPABILITY_REQUEST >> 16) & 0xFF));
        Assert.assertEquals(expected[3], (byte) ((MAGIC_CAPABILITY_REQUEST >> 24) & 0xFF));
    }

    @Test
    public void testMagicBytesCapabilityResponse() {
        // "ILP!" in ASCII
        byte[] expected = new byte[]{'I', 'L', 'P', '!'};
        Assert.assertEquals(expected[0], (byte) (MAGIC_CAPABILITY_RESPONSE & 0xFF));
        Assert.assertEquals(expected[1], (byte) ((MAGIC_CAPABILITY_RESPONSE >> 8) & 0xFF));
        Assert.assertEquals(expected[2], (byte) ((MAGIC_CAPABILITY_RESPONSE >> 16) & 0xFF));
        Assert.assertEquals(expected[3], (byte) ((MAGIC_CAPABILITY_RESPONSE >> 24) & 0xFF));
    }

    @Test
    public void testMagicBytesFallback() {
        // "ILP0" in ASCII
        byte[] expected = new byte[]{'I', 'L', 'P', '0'};
        Assert.assertEquals(expected[0], (byte) (MAGIC_FALLBACK & 0xFF));
        Assert.assertEquals(expected[1], (byte) ((MAGIC_FALLBACK >> 8) & 0xFF));
        Assert.assertEquals(expected[2], (byte) ((MAGIC_FALLBACK >> 16) & 0xFF));
        Assert.assertEquals(expected[3], (byte) ((MAGIC_FALLBACK >> 24) & 0xFF));
    }

    @Test
    public void testHeaderSize() {
        Assert.assertEquals(12, HEADER_SIZE);
    }

    @Test
    public void testFlagBitPositions() {
        // Verify flag bits are at correct positions
        Assert.assertEquals(0x01, FLAG_LZ4);
        Assert.assertEquals(0x02, FLAG_ZSTD);
        Assert.assertEquals(0x04, FLAG_GORILLA);
        Assert.assertEquals(0x03, FLAG_COMPRESSION_MASK);
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
    }

    @Test
    public void testNullableFlag() {
        Assert.assertEquals((byte) 0x80, TYPE_NULLABLE_FLAG);
        Assert.assertEquals(0x7F, TYPE_MASK);

        // Test nullable type extraction
        byte nullableInt = (byte) (TYPE_INT | TYPE_NULLABLE_FLAG);
        Assert.assertEquals(TYPE_INT, nullableInt & TYPE_MASK);
        Assert.assertTrue((nullableInt & TYPE_NULLABLE_FLAG) != 0);
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
    public void testIsFixedWidthType() {
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_BOOLEAN));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_BYTE));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_SHORT));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_INT));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_LONG));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_FLOAT));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_DOUBLE));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_TIMESTAMP));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_DATE));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_UUID));
        Assert.assertTrue(IlpV4Constants.isFixedWidthType(TYPE_LONG256));

        Assert.assertFalse(IlpV4Constants.isFixedWidthType(TYPE_STRING));
        Assert.assertFalse(IlpV4Constants.isFixedWidthType(TYPE_SYMBOL));
        Assert.assertFalse(IlpV4Constants.isFixedWidthType(TYPE_GEOHASH));
        Assert.assertFalse(IlpV4Constants.isFixedWidthType(TYPE_VARCHAR));
    }

    @Test
    public void testGetFixedTypeSize() {
        Assert.assertEquals(0, IlpV4Constants.getFixedTypeSize(TYPE_BOOLEAN)); // Bit-packed
        Assert.assertEquals(1, IlpV4Constants.getFixedTypeSize(TYPE_BYTE));
        Assert.assertEquals(2, IlpV4Constants.getFixedTypeSize(TYPE_SHORT));
        Assert.assertEquals(4, IlpV4Constants.getFixedTypeSize(TYPE_INT));
        Assert.assertEquals(8, IlpV4Constants.getFixedTypeSize(TYPE_LONG));
        Assert.assertEquals(4, IlpV4Constants.getFixedTypeSize(TYPE_FLOAT));
        Assert.assertEquals(8, IlpV4Constants.getFixedTypeSize(TYPE_DOUBLE));
        Assert.assertEquals(8, IlpV4Constants.getFixedTypeSize(TYPE_TIMESTAMP));
        Assert.assertEquals(8, IlpV4Constants.getFixedTypeSize(TYPE_DATE));
        Assert.assertEquals(16, IlpV4Constants.getFixedTypeSize(TYPE_UUID));
        Assert.assertEquals(32, IlpV4Constants.getFixedTypeSize(TYPE_LONG256));

        Assert.assertEquals(-1, IlpV4Constants.getFixedTypeSize(TYPE_STRING));
        Assert.assertEquals(-1, IlpV4Constants.getFixedTypeSize(TYPE_SYMBOL));
    }

    @Test
    public void testGetTypeName() {
        Assert.assertEquals("BOOLEAN", IlpV4Constants.getTypeName(TYPE_BOOLEAN));
        Assert.assertEquals("INT", IlpV4Constants.getTypeName(TYPE_INT));
        Assert.assertEquals("STRING", IlpV4Constants.getTypeName(TYPE_STRING));
        Assert.assertEquals("TIMESTAMP", IlpV4Constants.getTypeName(TYPE_TIMESTAMP));

        // Test nullable types
        byte nullableInt = (byte) (TYPE_INT | TYPE_NULLABLE_FLAG);
        Assert.assertEquals("INT?", IlpV4Constants.getTypeName(nullableInt));

        byte nullableString = (byte) (TYPE_STRING | TYPE_NULLABLE_FLAG);
        Assert.assertEquals("STRING?", IlpV4Constants.getTypeName(nullableString));
    }

    @Test
    public void testDefaultLimits() {
        Assert.assertEquals(16 * 1024 * 1024, DEFAULT_MAX_BATCH_SIZE);
        Assert.assertEquals(256, DEFAULT_MAX_TABLES_PER_BATCH);
        Assert.assertEquals(1_000_000, DEFAULT_MAX_ROWS_PER_TABLE);
        Assert.assertEquals(2048, MAX_COLUMNS_PER_TABLE);
        Assert.assertEquals(64 * 1024, DEFAULT_INITIAL_RECV_BUFFER_SIZE);
        Assert.assertEquals(4, DEFAULT_MAX_IN_FLIGHT_BATCHES);
    }

    @Test
    public void testSchemaModes() {
        Assert.assertEquals(0x00, SCHEMA_MODE_FULL);
        Assert.assertEquals(0x01, SCHEMA_MODE_REFERENCE);
    }
}
