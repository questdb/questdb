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

import io.questdb.cutlass.qwp.protocol.QwpColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Fuzz test that generates valid QWP messages with random parameters, then:
 * <ol>
 *   <li>Truncates them at every byte position and verifies graceful QwpParseException</li>
 *   <li>Corrupts random bytes and verifies no crashes or OOB reads</li>
 * </ol>
 * The goal is to find missing bounds checks in the parser. A crash, OOB read,
 * or unchecked exception (NPE, AIOOBE, etc.) at any truncation/corruption
 * point indicates a validation gap.
 */
public class QwpCursorBoundsCheckFuzzTest {

    // Column types safe to generate without complex dictionary/connection state.
    // Grouped by cursor type for readability.
    private static final byte[] FUZZABLE_TYPES = {
            // Boolean cursor
            TYPE_BOOLEAN,
            // Fixed-width cursor (various sizes: 1, 2, 4, 8, 16, 32)
            TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG,
            TYPE_FLOAT, TYPE_DOUBLE, TYPE_DATE, TYPE_UUID,
            TYPE_LONG256, TYPE_CHAR,
            // Timestamp cursor (uncompressed)
            TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS,
            // String cursor
            TYPE_VARCHAR,
            // Symbol cursor (per-column dictionary mode)
            TYPE_SYMBOL,
            // GeoHash cursor
            TYPE_GEOHASH,
            // Decimal cursor (8, 16, 32 bytes)
            TYPE_DECIMAL64, TYPE_DECIMAL128, TYPE_DECIMAL256,
            // Array cursor (1D arrays)
            TYPE_DOUBLE_ARRAY,
    };
    private static final Log LOG = LogFactory.getLog(QwpCursorBoundsCheckFuzzTest.class);

    @Test
    public void testByteCorruption() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG, 42661209174423L, 1775678559100L);
            int iterations = 50;
            int corruptionsPerMessage = 30;

            for (int iter = 0; iter < iterations; iter++) {
                byte[] message = generateValidMessage(rnd);
                verifyFullParse(message);

                for (int c = 0; c < corruptionsPerMessage; c++) {
                    byte[] corrupted = message.clone();
                    // Corrupt 1-3 bytes in the payload area
                    int nCorrupt = 1 + rnd.nextInt(3);
                    for (int i = 0; i < nCorrupt; i++) {
                        int pos = rnd.nextInt(corrupted.length);
                        corrupted[pos] = (byte) rnd.nextInt(256);
                    }

                    long address = Unsafe.malloc(corrupted.length, MemoryTag.NATIVE_DEFAULT);
                    try {
                        copyToNative(corrupted, address);
                        parseAndIterate(address, corrupted.length);
                    } catch (QwpParseException ignored) {
                        // Expected: parser rejects corrupted data
                    } catch (IllegalStateException ignored) {
                        // Also acceptable: e.g. "No more tables" from corrupted table count
                    } catch (AssertionError ignored) {
                        // Java assertions from debug checks
                    } catch (Throwable t) {
                        Assert.fail(
                                "Unexpected " + t.getClass().getSimpleName()
                                        + " at corruption iter=" + iter + " c=" + c
                                        + ": " + t.getMessage()
                        );
                    } finally {
                        Unsafe.free(address, corrupted.length, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            }
        });
    }

    @Test
    public void testTruncationAtEveryBytePosition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            int iterations = 50;

            for (int iter = 0; iter < iterations; iter++) {
                byte[] message = generateValidMessage(rnd);
                verifyFullParse(message);

                // Allocate native memory for the full message once, then pass shorter lengths.
                long address = Unsafe.malloc(message.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    copyToNative(message, address);

                    for (int truncLen = 0; truncLen < message.length; truncLen++) {
                        try {
                            parseAndIterate(address, truncLen);
                        } catch (QwpParseException ignored) {
                            // Expected: parser rejects truncated data
                        } catch (IllegalStateException ignored) {
                            // Also acceptable: e.g. "No more tables" from corrupted table count
                        } catch (AssertionError ignored) {
                            // Java assertions from debug checks
                        } catch (Throwable t) {
                            Assert.fail(
                                    "Unexpected " + t.getClass().getSimpleName()
                                            + " at truncLen=" + truncLen
                                            + " (full=" + message.length + ")"
                                            + " iter=" + iter
                                            + ": " + t.getMessage()
                            );
                        }
                    }
                } finally {
                    Unsafe.free(address, message.length, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static void copyToNative(byte[] src, long address) {
        for (int i = 0; i < src.length; i++) {
            Unsafe.putByte(address + i, src[i]);
        }
    }

    /**
     * Generates a valid QWP message with 1-3 tables, random columns, random row count.
     */
    private static byte[] generateValidMessage(Rnd rnd) {
        int tableCount = 1 + rnd.nextInt(3); // 1-3 tables

        ByteArrayOutputStream payload = new ByteArrayOutputStream(512);
        for (int t = 0; t < tableCount; t++) {
            int rowCount = rnd.nextInt(20); // 0-19 rows
            int columnCount = 1 + rnd.nextInt(6); // 1-6 columns

            byte[] columnTypes = new byte[columnCount];
            String[] columnNames = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnTypes[i] = FUZZABLE_TYPES[rnd.nextInt(FUZZABLE_TYPES.length)];
                columnNames[i] = "c" + i;
            }

            String tableName = "t" + rnd.nextInt(100);
            writeTablePayload(payload, rnd, tableName, rowCount, columnCount, columnTypes, columnNames);
        }

        byte[] payloadBytes = payload.toByteArray();

        // Build full message: header + payload
        byte[] message = new byte[HEADER_SIZE + payloadBytes.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[HEADER_OFFSET_VERSION] = VERSION_1;
        message[HEADER_OFFSET_FLAGS] = 0;
        message[HEADER_OFFSET_TABLE_COUNT] = (byte) tableCount;
        message[HEADER_OFFSET_TABLE_COUNT + 1] = (byte) (tableCount >>> 8);
        message[HEADER_OFFSET_PAYLOAD_LENGTH] = (byte) payloadBytes.length;
        message[HEADER_OFFSET_PAYLOAD_LENGTH + 1] = (byte) (payloadBytes.length >>> 8);
        message[HEADER_OFFSET_PAYLOAD_LENGTH + 2] = (byte) (payloadBytes.length >>> 16);
        message[HEADER_OFFSET_PAYLOAD_LENGTH + 3] = (byte) (payloadBytes.length >>> 24);

        System.arraycopy(payloadBytes, 0, message, HEADER_SIZE, payloadBytes.length);
        return message;
    }

    /**
     * Parses the message and iterates all tables and rows.
     * This exercises the full parse path including advanceRow() on each cursor.
     */
    private static void parseAndIterate(long address, int length) throws QwpParseException {
        QwpMessageCursor cursor = new QwpMessageCursor();
        cursor.of(address, length, null, null);

        while (cursor.hasNextTable()) {
            QwpTableBlockCursor table = cursor.nextTable();

            // Iterate all rows to exercise advanceRow() on each column cursor
            int columnCount = table.getColumnCount();
            int rowCount = table.getRowCount();
            for (int row = 0; row < rowCount; row++) {
                for (int col = 0; col < columnCount; col++) {
                    QwpColumnCursor colCursor = table.getColumn(col);
                    colCursor.advanceRow();
                }
            }
        }
    }

    /**
     * Verifies the generated message is valid by parsing it fully.
     */
    private static void verifyFullParse(byte[] message) throws QwpParseException {
        long address = Unsafe.malloc(message.length, MemoryTag.NATIVE_DEFAULT);
        try {
            copyToNative(message, address);
            parseAndIterate(address, message.length);
        } finally {
            Unsafe.free(address, message.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void writeArrayColumnData(ByteArrayOutputStream out, Rnd rnd, int valueCount) {
        // For each non-null row: nDims(1 byte) + dims(nDims * 4 bytes) + values(elementCount * 8 bytes)
        for (int v = 0; v < valueCount; v++) {
            int nDims = 1; // keep it simple: 1D arrays
            out.write(nDims);
            int dimSize = rnd.nextInt(4); // 0-3 elements
            writeInt32LE(out, dimSize);
            // Write dimSize * 8 bytes of random data
            writeFixedWidthColumnData(out, rnd, dimSize, 8);
        }
    }

    private static void writeBooleanColumnData(ByteArrayOutputStream out, Rnd rnd, int valueCount) {
        int bitmapSize = (valueCount + 7) / 8;
        for (int i = 0; i < bitmapSize; i++) {
            out.write(rnd.nextInt(256));
        }
    }

    private static void writeColumnData(ByteArrayOutputStream out, Rnd rnd, byte typeCode, int rowCount) {
        // Generate null bitmap randomly
        boolean hasNulls = rowCount > 0 && rnd.nextBoolean();
        byte[] nullBitmap;
        int nullCount = 0;

        if (hasNulls) {
            out.write(1); // null bitmap present
            int bitmapSize = (rowCount + 7) / 8;
            nullBitmap = new byte[bitmapSize];
            for (int i = 0; i < bitmapSize; i++) {
                nullBitmap[i] = (byte) rnd.nextInt(256);
            }
            // Ensure trailing bits beyond rowCount are zero
            if (rowCount % 8 != 0) {
                int mask = (1 << (rowCount % 8)) - 1;
                nullBitmap[bitmapSize - 1] &= (byte) mask;
            }
            for (int row = 0; row < rowCount; row++) {
                if ((nullBitmap[row / 8] & (1 << (row % 8))) != 0) {
                    nullCount++;
                }
            }
            out.write(nullBitmap, 0, bitmapSize);
        } else {
            out.write(0); // no null bitmap
        }

        int valueCount = rowCount - nullCount;

        switch (typeCode) {
            case TYPE_BOOLEAN -> writeBooleanColumnData(out, rnd, valueCount);
            case TYPE_BYTE -> writeFixedWidthColumnData(out, rnd, valueCount, 1);
            case TYPE_SHORT, TYPE_CHAR -> writeFixedWidthColumnData(out, rnd, valueCount, 2);
            case TYPE_INT, TYPE_FLOAT -> writeFixedWidthColumnData(out, rnd, valueCount, 4);
            case TYPE_LONG, TYPE_DOUBLE, TYPE_DATE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS ->
                    writeFixedWidthColumnData(out, rnd, valueCount, 8);
            case TYPE_UUID -> writeFixedWidthColumnData(out, rnd, valueCount, 16);
            case TYPE_LONG256 -> writeFixedWidthColumnData(out, rnd, valueCount, 32);
            case TYPE_VARCHAR -> writeStringColumnData(out, rnd, valueCount);
            case TYPE_SYMBOL -> writeSymbolColumnData(out, rnd, valueCount);
            case TYPE_GEOHASH -> writeGeoHashColumnData(out, rnd, valueCount);
            case TYPE_DECIMAL64 -> writeDecimalColumnData(out, rnd, valueCount, 8);
            case TYPE_DECIMAL128 -> writeDecimalColumnData(out, rnd, valueCount, 16);
            case TYPE_DECIMAL256 -> writeDecimalColumnData(out, rnd, valueCount, 32);
            case TYPE_DOUBLE_ARRAY, TYPE_LONG_ARRAY -> writeArrayColumnData(out, rnd, valueCount);
            default -> throw new IllegalArgumentException("unsupported type: " + typeCode);
        }
    }

    private static void writeDecimalColumnData(ByteArrayOutputStream out, Rnd rnd, int valueCount, int valueSize) {
        int scale = rnd.nextInt(20); // 0-19
        out.write(scale);
        writeFixedWidthColumnData(out, rnd, valueCount, valueSize);
    }

    private static void writeFixedWidthColumnData(ByteArrayOutputStream out, Rnd rnd, int valueCount, int valueSize) {
        for (int v = 0; v < valueCount; v++) {
            for (int b = 0; b < valueSize; b++) {
                out.write(rnd.nextInt(256));
            }
        }
    }

    private static void writeGeoHashColumnData(ByteArrayOutputStream out, Rnd rnd, int valueCount) {
        int precision = 1 + rnd.nextInt(60); // 1-60 bits
        writeVarint(out, precision);
        int valueSize = (precision + 7) / 8;
        writeFixedWidthColumnData(out, rnd, valueCount, valueSize);
    }

    private static void writeInt32LE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    private static void writeStringColumnData(ByteArrayOutputStream out, Rnd rnd, int valueCount) {
        // Build string data and offset array
        int[] offsets = new int[valueCount + 1];
        ByteArrayOutputStream stringData = new ByteArrayOutputStream();
        offsets[0] = 0;
        for (int i = 0; i < valueCount; i++) {
            int strLen = rnd.nextInt(20); // 0-19 bytes per string
            for (int b = 0; b < strLen; b++) {
                // ASCII printable chars to avoid UTF-8 issues
                stringData.write(0x20 + rnd.nextInt(95));
            }
            offsets[i + 1] = offsets[i] + strLen;
        }

        // Write offset array: (valueCount+1) * 4 bytes, little-endian
        for (int i = 0; i <= valueCount; i++) {
            writeInt32LE(out, offsets[i]);
        }
        // Write string data
        byte[] strBytes = stringData.toByteArray();
        out.write(strBytes, 0, strBytes.length);
    }

    private static void writeSymbolColumnData(ByteArrayOutputStream out, Rnd rnd, int valueCount) {
        // Per-column dictionary mode: varint(dictSize) + dict entries + varint indices
        int dictSize = 1 + rnd.nextInt(5); // 1-5 entries
        writeVarint(out, dictSize);
        for (int d = 0; d < dictSize; d++) {
            int strLen = 1 + rnd.nextInt(8); // 1-8 bytes
            writeVarint(out, strLen);
            for (int b = 0; b < strLen; b++) {
                out.write(0x61 + rnd.nextInt(26)); // a-z
            }
        }
        // Varint-encoded indices referencing dictionary
        for (int v = 0; v < valueCount; v++) {
            writeVarint(out, rnd.nextInt(dictSize));
        }
    }

    private static void writeTablePayload(
            ByteArrayOutputStream out,
            Rnd rnd,
            String tableName,
            int rowCount,
            int columnCount,
            byte[] columnTypes,
            String[] columnNames
    ) {
        // Table header: varint(nameLen) + name + varint(rowCount) + varint(columnCount)
        byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);
        writeVarint(out, nameBytes.length);
        out.write(nameBytes, 0, nameBytes.length);
        writeVarint(out, rowCount);
        writeVarint(out, columnCount);

        // Schema: full mode (0x00) + varint(schemaId=0) + column definitions
        out.write(SCHEMA_MODE_FULL);
        writeVarint(out, 0); // schemaId
        for (int i = 0; i < columnCount; i++) {
            byte[] colNameBytes = columnNames[i].getBytes(StandardCharsets.UTF_8);
            writeVarint(out, colNameBytes.length);
            out.write(colNameBytes, 0, colNameBytes.length);
            out.write(columnTypes[i]);
        }

        // Column data
        for (int i = 0; i < columnCount; i++) {
            writeColumnData(out, rnd, columnTypes[i], rowCount);
        }
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        while (value > 0x7F) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) (value & 0x7F));
    }
}
