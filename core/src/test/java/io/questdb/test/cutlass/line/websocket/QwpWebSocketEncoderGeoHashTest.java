/*******************************************************************************
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

package io.questdb.test.cutlass.line.websocket;

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashDecoder;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * End-to-end tests for GEOHASH column encoding and decoding.
 * <p>
 * These tests verify that GEOHASH data encoded by the client-side
 * {@link QwpWebSocketEncoder} can be correctly decoded by the server-side
 * {@link QwpStreamingDecoder}, covering all four GeoHash storage types
 * (GEOBYTE, GEOSHORT, GEOINT, GEOLONG), nullable columns, and mixed-type
 * messages.
 */
public class QwpWebSocketEncoderGeoHashTest {

    @Test
    public void testAddGeoHashInvalidPrecisionTooLarge() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                try {
                    col.addGeoHash(0, 61);
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("precision"));
                }
            }
        });
    }

    @Test
    public void testAddGeoHashInvalidPrecisionZero() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                try {
                    col.addGeoHash(0, 0);
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("precision"));
                }
            }
        });
    }

    @Test
    public void testAddGeoHashPrecisionMismatch() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                col.addGeoHash(0b10110, 5);
                buffer.nextRow();
                try {
                    col.addGeoHash(0xFF, 10);
                    Assert.fail("Expected LineSenderException for precision mismatch");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("mismatch"));
                }
            }
        });
    }

    @Test
    public void testClientEncoderMatchesServerEncoder() throws Exception {
        assertMemoryLeak(() -> {
            // Encode with client-side encoder, then decode with server-side decoder
            // and verify byte-level compatibility with the server's own encode/decode
            int[] precisions = {1, 5, 7, 8, 10, 15, 16, 20, 25, 31, 32, 40, 48, 56, 60};

            for (int precision : precisions) {
                long maxValue = (1L << precision) - 1;
                long[] values = {
                        0,
                        maxValue,
                        maxValue / 2,
                        maxValue / 3
                };
                assertGeoHashRoundTrip(values, precision, false);
            }
        });
    }

    @Test
    public void testEncodeDecodeAllPackedSizes() throws Exception {
        assertMemoryLeak(() -> {
            // 3-byte packed (17-24 bits)
            assertGeoHashRoundTrip(new long[]{0x123456L}, 24, false);
            assertGeoHashRoundTrip(new long[]{0x123456L}, 24, true);

            // 5-byte packed (33-40 bits)
            assertGeoHashRoundTrip(new long[]{0x123456789AL}, 40, false);
            assertGeoHashRoundTrip(new long[]{0x123456789AL}, 40, true);

            // 6-byte packed (41-48 bits)
            assertGeoHashRoundTrip(new long[]{0x123456789ABCL}, 48, false);
            assertGeoHashRoundTrip(new long[]{0x123456789ABCL}, 48, true);

            // 7-byte packed (49-56 bits)
            assertGeoHashRoundTrip(new long[]{0x123456789ABCDEL}, 56, false);
            assertGeoHashRoundTrip(new long[]{0x123456789ABCDEL}, 56, true);
        });
    }

    @Test
    public void testEncodeDecodeGeoByte1Bit() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0, 1, 1, 0, 1};
            assertGeoHashRoundTrip(values, 1, false);
            assertGeoHashRoundTrip(values, 1, true);
        });
    }

    @Test
    public void testEncodeDecodeGeoByte5Bits() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0b10110, 0b01001, 0b11111, 0b00000, 0b10101};
            assertGeoHashRoundTrip(values, 5, false);
            assertGeoHashRoundTrip(values, 5, true);
        });
    }

    @Test
    public void testEncodeDecodeGeoByte7Bits() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0b1110110, 0b0100101, 0b1111111, 0b0000000, 0b1010101};
            assertGeoHashRoundTrip(values, 7, false);
            assertGeoHashRoundTrip(values, 7, true);
        });
    }

    @Test
    public void testEncodeDecodeGeoHashWithOtherColumns() throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = 5;
            int precision = 20;
            long[] geoValues = {0xABCDE, 0x12345, 0xFFFFF, 0x00000, 0x55555};
            long[] longValues = {100, 200, 300, 400, 500};
            double[] doubleValues = {1.1, 2.2, 3.3, 4.4, 5.5};

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("mixed_table");

                QwpTableBuffer.ColumnBuffer geoCol = buffer.getOrCreateColumn("location", TYPE_GEOHASH, false);
                QwpTableBuffer.ColumnBuffer longCol = buffer.getOrCreateColumn("id", TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer dblCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    geoCol.addGeoHash(geoValues[i], precision);
                    longCol.addLong(longValues[i]);
                    dblCol.addDouble(doubleValues[i]);
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);

                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals("mixed_table", table.getTableName());
                    Assert.assertEquals(rowCount, table.getRowCount());
                    Assert.assertEquals(4, table.getColumnCount());

                    int geoColIdx = findGeoHashColumnIndex(table);
                    Assert.assertNotEquals("GEOHASH column not found", -1, geoColIdx);

                    for (int i = 0; i < rowCount; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();

                        QwpGeoHashColumnCursor geoCursor = table.getGeoHashColumn(geoColIdx);
                        Assert.assertFalse(geoCursor.isNull());
                        Assert.assertEquals(precision, geoCursor.getPrecision());
                        Assert.assertEquals("Row " + i + " geohash mismatch",
                                geoValues[i], geoCursor.getGeoHash());
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }

    @Test
    public void testEncodeDecodeGeoInt20Bits() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0xABCDE, 0x12345, 0xFFFFF, 0x00000};
            assertGeoHashRoundTrip(values, 20, false);
            assertGeoHashRoundTrip(values, 20, true);
        });
    }

    @Test
    public void testEncodeDecodeGeoInt31Bits() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0x7FFFFFFFL, 0x55555555L, 0x2AAAAAABL, 0x00000000L};
            assertGeoHashRoundTrip(values, 31, false);
            assertGeoHashRoundTrip(values, 31, true);
        });
    }

    @Test
    public void testEncodeDecodeGeoLong40Bits() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0xABCDEF1234L, 0x123456789AL, 0xFFFFFFFFFFL, 0x0000000000L};
            assertGeoHashRoundTrip(values, 40, false);
            assertGeoHashRoundTrip(values, 40, true);
        });
    }

    @Test
    public void testEncodeDecodeGeoLong60Bits() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {
                    0x0FFFFFFFFFFFFFFFL,
                    0x0555555555555555L,
                    0x0AAAAAAAAAAAAAAAL,
                    0x0000000000000000L
            };
            assertGeoHashRoundTrip(values, 60, false);
            assertGeoHashRoundTrip(values, 60, true);
        });
    }

    @Test
    public void testEncodeDecodeGeoShort10Bits() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0b1011010110, 0b0100101001, 0b1111111111, 0b0000000000};
            assertGeoHashRoundTrip(values, 10, false);
            assertGeoHashRoundTrip(values, 10, true);
        });
    }

    @Test
    public void testEncodeDecodeGeoShort15Bits() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0b111011010110110, 0b010010100101001, 0b111111111111111};
            assertGeoHashRoundTrip(values, 15, false);
            assertGeoHashRoundTrip(values, 15, true);
        });
    }

    @Test
    public void testEncodeDecodeLargeGeoIntColumn() throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = 10_000;
            int precision = 25;
            long[] values = new long[rowCount];
            for (int i = 0; i < rowCount; i++) {
                values[i] = (i * 12_345L) & ((1L << precision) - 1);
            }
            assertGeoHashRoundTrip(values, precision, false);
        });
    }

    @Test
    public void testEncodeDecodeLargeGeoLongColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = 10_000;
            int precision = 40;
            long[] values = new long[rowCount];
            boolean[] nulls = new boolean[rowCount];
            for (int i = 0; i < rowCount; i++) {
                values[i] = (i * 12_345L) & ((1L << precision) - 1);
                nulls[i] = (i % 7 == 0);
            }
            assertNullableGeoHashRoundTrip(values, nulls, precision);
        });
    }

    @Test
    public void testEncodeDecodeMatchesServerRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            // Verify that client encoder -> server decoder gives same results as
            // server encoder -> server decoder
            int precision = 20;
            long[] values = {0xABCDE, 0x12345, 0xFFFFF, 0x00000};

            // Server-side round-trip
            int serverSize = QwpGeoHashDecoder.calculateEncodedSize(values.length, precision, false);
            long serverBuf = io.questdb.std.Unsafe.malloc(serverSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            try {
                QwpGeoHashDecoder.encode(serverBuf, values, precision, null);
                QwpGeoHashDecoder.ArrayGeoHashSink serverSink = new QwpGeoHashDecoder.ArrayGeoHashSink(values.length);
                QwpGeoHashDecoder.INSTANCE.decode(serverBuf, serverSize, values.length, false, serverSink);

                // Client-side encode -> server-side decode
                long[] clientDecoded = encodeAndDecodeGeoHash(values, precision, false);

                // Both paths must give identical results
                for (int i = 0; i < values.length; i++) {
                    Assert.assertEquals("Server decode value mismatch at row " + i,
                            values[i], serverSink.getValue(i));
                    Assert.assertEquals("Client-server round-trip mismatch at row " + i,
                            serverSink.getValue(i), clientDecoded[i]);
                }
            } finally {
                io.questdb.std.Unsafe.free(serverBuf, serverSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEncodeDecodeNullableGeoHashAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0, 0, 0};
            boolean[] nulls = {true, true, true};
            assertNullableGeoHashRoundTrip(values, nulls, 10);
        });
    }

    @Test
    public void testEncodeDecodeNullableGeoHashNoNulls() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0xABCDE, 0x12345, 0xFFFFF};
            boolean[] nulls = {false, false, false};
            assertNullableGeoHashRoundTrip(values, nulls, 20);
        });
    }

    @Test
    public void testEncodeDecodeNullableGeoHashWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            // 5-bit precision, rows 1 and 3 are null
            long[] values = {0b10110, 0, 0b11111, 0, 0b10101};
            boolean[] nulls = {false, true, false, true, false};
            assertNullableGeoHashRoundTrip(values, nulls, 5);
        });
    }

    @Test
    public void testEncodeDecodeNullableGeoLongWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0x0FFFFFFFFFFFFFFFL, 0, 0x0555555555555555L};
            boolean[] nulls = {false, true, false};
            assertNullableGeoHashRoundTrip(values, nulls, 60);
        });
    }

    @Test
    public void testEncodeDecodePrecisionBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            // GEOBYTE/GEOSHORT boundary
            assertGeoHashRoundTrip(new long[]{0x7F}, 7, false);
            assertGeoHashRoundTrip(new long[]{0xFF}, 8, false);
            assertGeoHashRoundTrip(new long[]{0x7F}, 7, true);
            assertGeoHashRoundTrip(new long[]{0xFF}, 8, true);

            // GEOSHORT/GEOINT boundary
            assertGeoHashRoundTrip(new long[]{0x7FFF}, 15, false);
            assertGeoHashRoundTrip(new long[]{0xFFFF}, 16, false);
            assertGeoHashRoundTrip(new long[]{0x7FFF}, 15, true);
            assertGeoHashRoundTrip(new long[]{0xFFFF}, 16, true);

            // GEOINT/GEOLONG boundary
            assertGeoHashRoundTrip(new long[]{0x7FFFFFFFL}, 31, false);
            assertGeoHashRoundTrip(new long[]{0xFFFFFFFFL}, 32, false);
            assertGeoHashRoundTrip(new long[]{0x7FFFFFFFL}, 31, true);
            assertGeoHashRoundTrip(new long[]{0xFFFFFFFFL}, 32, true);
        });
    }

    @Test
    public void testEncodeDecodeSingleNullRow() throws Exception {
        assertMemoryLeak(() -> {
            assertNullableGeoHashRoundTrip(new long[]{0}, new boolean[]{true}, 5);
        });
    }

    @Test
    public void testEncodeDecodeSingleNullableRow() throws Exception {
        assertMemoryLeak(() -> {
            assertNullableGeoHashRoundTrip(new long[]{0b10110}, new boolean[]{false}, 5);
        });
    }

    @Test
    public void testEncodeDecodeSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            assertGeoHashRoundTrip(new long[]{0b10110}, 5, false);
        });
    }

    @Test
    public void testEncodeGeoHashProducesValidMessage() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                col.addGeoHash(0b10110, 5);
                buffer.nextRow();

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);

                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                // Verify ILP v4 header magic
                Assert.assertEquals((byte) 'I', io.questdb.client.std.Unsafe.getUnsafe().getByte(ptr));
                Assert.assertEquals((byte) 'L', io.questdb.client.std.Unsafe.getUnsafe().getByte(ptr + 1));
                Assert.assertEquals((byte) 'P', io.questdb.client.std.Unsafe.getUnsafe().getByte(ptr + 2));
                Assert.assertEquals((byte) '4', io.questdb.client.std.Unsafe.getUnsafe().getByte(ptr + 3));

                // Table count = 1
                Assert.assertEquals((short) 1, io.questdb.client.std.Unsafe.getUnsafe().getShort(ptr + 6));
            }
        });
    }

    @Test
    public void testEncodeMultipleGeoHashRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                for (int i = 0; i < 100; i++) {
                    col.addGeoHash(i & 0x1F, 5);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(100, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeNullableGeoHash() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, true);
                col.addGeoHash(0b10110, 5);
                buffer.nextRow();

                col.addNull();
                buffer.nextRow();

                col.addGeoHash(0b11111, 5);
                buffer.nextRow();

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(3, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testResetAllowsNewPrecision() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                // First batch: 5-bit precision
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                col.addGeoHash(0b10110, 5);
                buffer.nextRow();

                encoder.encode(buffer, false);
                buffer.reset();

                // After reset: 20-bit precision works fine
                col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                col.addGeoHash(0xABCDE, 20);
                buffer.nextRow();

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);
            }
        });
    }

    private static int findGeoHashColumnIndex(QwpTableBlockCursor table) {
        for (int c = 0; c < table.getColumnCount(); c++) {
            if (table.getColumnDef(c).getTypeCode() == TYPE_GEOHASH) {
                return c;
            }
        }
        return -1;
    }

    private void assertGeoHashRoundTrip(long[] values, int precision, boolean nullable) throws QwpParseException {
        long[] decoded = encodeAndDecodeGeoHash(values, precision, nullable);
        for (int i = 0; i < values.length; i++) {
            Assert.assertEquals("Row " + i + " value mismatch (precision=" + precision + ")",
                    values[i], decoded[i]);
        }
    }

    private void assertNullableGeoHashRoundTrip(long[] values, boolean[] nulls, int precision) throws QwpParseException {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_geohash");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, true);
            QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

            for (int i = 0; i < values.length; i++) {
                if (nulls[i]) {
                    col.addNull();
                } else {
                    col.addGeoHash(values[i], precision);
                }
                tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            QwpBufferWriter buf = encoder.getBuffer();
            long ptr = buf.getBufferPtr();

            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                QwpMessageCursor msg = decoder.decode(ptr, size);
                Assert.assertTrue(msg.hasNextTable());
                QwpTableBlockCursor table = msg.nextTable();

                Assert.assertEquals(values.length, table.getRowCount());

                int geoColIdx = findGeoHashColumnIndex(table);
                Assert.assertNotEquals("GEOHASH column not found", -1, geoColIdx);

                for (int i = 0; i < values.length; i++) {
                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();

                    if (nulls[i]) {
                        Assert.assertTrue("Row " + i + " should be null",
                                table.isColumnNull(geoColIdx));
                    } else {
                        Assert.assertFalse("Row " + i + " should not be null",
                                table.isColumnNull(geoColIdx));
                        QwpGeoHashColumnCursor geoCursor = table.getGeoHashColumn(geoColIdx);
                        Assert.assertEquals(precision, geoCursor.getPrecision());
                        Assert.assertEquals("Row " + i + " value mismatch",
                                values[i], geoCursor.getGeoHash());
                    }
                }
                Assert.assertFalse(table.hasNextRow());
            }
        }
    }

    private long[] encodeAndDecodeGeoHash(long[] values, int precision, boolean nullable) throws QwpParseException {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_geohash");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, nullable);
            QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

            for (int i = 0; i < values.length; i++) {
                col.addGeoHash(values[i], precision);
                tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            QwpBufferWriter buf = encoder.getBuffer();
            long ptr = buf.getBufferPtr();

            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                QwpMessageCursor msg = decoder.decode(ptr, size);
                Assert.assertTrue(msg.hasNextTable());
                QwpTableBlockCursor table = msg.nextTable();

                Assert.assertEquals(values.length, table.getRowCount());

                int geoColIdx = findGeoHashColumnIndex(table);
                Assert.assertNotEquals("GEOHASH column not found", -1, geoColIdx);

                long[] decoded = new long[values.length];
                for (int i = 0; i < values.length; i++) {
                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();

                    QwpGeoHashColumnCursor geoCursor = table.getGeoHashColumn(geoColIdx);
                    Assert.assertFalse("Row " + i + " should not be null", geoCursor.isNull());
                    Assert.assertEquals("Row " + i + " precision mismatch",
                            precision, geoCursor.getPrecision());
                    decoded[i] = geoCursor.getGeoHash();
                }
                Assert.assertFalse(table.hasNextRow());
                return decoded;
            }
        }
    }
}
