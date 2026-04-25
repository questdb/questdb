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

import io.questdb.cairo.CairoException;
import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpBooleanColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpDecimalColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSchema;
import io.questdb.cutlass.qwp.protocol.QwpSchemaRegistry;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Verifies that all column cursors validate data bounds during initialization.
 * Each cursor's of() method receives dataLength and throws QwpParseException
 * when the wire data is truncated or contains out-of-bounds offsets.
 */
public class QwpCursorBoundsCheckTest {
    private static final int CORRUPTIONS_PER_MESSAGE = 10;
    private static final int FUZZ_MAX_ROWS_PER_TABLE = 256;
    private static final int FUZZ_MESSAGE_ITERATIONS = 1_000;

    @Test
    public void testArrayCursorRejectsTruncatedData() throws Exception {
        assertMemoryLeak(() -> {
            int bufferSize = 5;
            long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, bufferSize, (byte) 0);
                Unsafe.putByte(address, (byte) 0); // no null bitmap
                Unsafe.putByte(address + 1, (byte) 1); // nDims=1

                QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
                cursor.of(address, bufferSize, 1, TYPE_DOUBLE_ARRAY);
                Assert.fail("expected QwpParseException for truncated array data");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("truncated"));
            } finally {
                Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testBooleanCursorRejectsInflatedRowCount() throws Exception {
        assertMemoryLeak(() -> {
            int bufferSize = 8;
            long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, bufferSize, (byte) 0);
                // null bitmap flag is already 0 from setMemory

                QwpBooleanColumnCursor cursor = new QwpBooleanColumnCursor();
                cursor.of(address, bufferSize, 1000);
                Assert.fail("expected QwpParseException for inflated rowCount");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("truncated"));
            } finally {
                Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDecimalCursorRejectsInflatedRowCount() throws Exception {
        assertMemoryLeak(() -> {
            int bufferSize = 16;
            long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, bufferSize, (byte) 0);

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(address, bufferSize, 100, TYPE_DECIMAL64);
                Assert.fail("expected QwpParseException for inflated rowCount");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("truncated"));
            } finally {
                Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Demonstrates that the Gorilla decoder's byte boundary is not tightly
     * enforced: when the Gorilla segment is corrupted, the decoder silently
     * reads bits from subsequent column data without raising an error.
     * <p>
     * Setup: a buffer containing a Gorilla-encoded timestamp column (3 values,
     * monotonic timestamps → DoD=0 → 1 bit of Gorilla data) followed by an
     * int column's data (simulating consecutive columns in a table block).
     * <p>
     * When the single Gorilla byte is corrupted to 0xFF, the decoder reads
     * prefix '1111' (36-bit path) and consumes 5 bytes instead of 1, spilling
     * into the int column data. The returned consumed count is inflated,
     * the decoded timestamp is garbage, yet no exception is thrown.
     */
    @Test
    public void testGorillaCursorOverReadsIntoNextColumnOnCorruptedData() throws Exception {
        assertMemoryLeak(() -> {
            // Gorilla-encoded timestamp column with 3 values: 1000, 2000, 3000
            // Delta = 1000 for both, DoD = 0 → encoded as single '0' bit
            long ts1 = 1000L;
            long ts2 = 2000L;
            // ts3 = 3000 → DoD = 0

            // Wire layout for timestamp column:
            //   [null flag: 1B] [encoding: 1B] [first ts: 8B] [second ts: 8B] [gorilla: 1B]
            int tsColumnSize = 1 + 1 + 8 + 8 + 1; // = 19 bytes

            // Simulated int column data after the timestamp column (3 rows × 4 bytes)
            int intColumnSize = 1 + 3 * 4; // null flag + values = 13 bytes

            int totalSize = tsColumnSize + intColumnSize;
            long address = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, totalSize, (byte) 0);

                // Write timestamp column wire data
                long pos = address;
                Unsafe.putByte(pos, (byte) 0);         // no null bitmap
                pos++;
                Unsafe.putByte(pos, QwpTimestampColumnCursor.ENCODING_GORILLA);
                pos++;
                Unsafe.putLong(pos, ts1);              // first timestamp
                pos += 8;
                Unsafe.putLong(pos, ts2);              // second timestamp
                pos += 8;
                Unsafe.putByte(pos, (byte) 0);         // Gorilla: 1 bit '0' (DoD=0)
                pos++;

                // Write int column data (simulating next column in table block)
                Unsafe.putByte(pos, (byte) 0);         // no null bitmap
                pos++;
                Unsafe.putInt(pos, 100);
                pos += 4;
                Unsafe.putInt(pos, 200);
                pos += 4;
                Unsafe.putInt(pos, 300);

                // Step 1: Parse with valid data — verify correct behavior
                QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
                // dataLength = totalSize mimics QwpTableBlockCursor passing remaining block bytes
                int consumed = cursor.of(address, totalSize, 3, TYPE_TIMESTAMP, true);
                Assert.assertEquals("valid Gorilla data should consume exactly the timestamp column",
                        tsColumnSize, consumed);

                // Verify decoded values
                cursor.advanceRow();
                Assert.assertEquals(ts1, cursor.getTimestamp());
                cursor.advanceRow();
                Assert.assertEquals(ts2, cursor.getTimestamp());
                cursor.advanceRow();
                Assert.assertEquals(3000L, cursor.getTimestamp());

                // Step 2: Corrupt the Gorilla byte to 0xFF
                // Prefix '1111' triggers 36-bit read (4 prefix + 32 data bits = 5 bytes)
                long gorillaByteAddr = address + 1 + 1 + 8 + 8; // skip flag, encoding, ts1, ts2
                Unsafe.putByte(gorillaByteAddr, (byte) 0xFF);

                cursor = new QwpTimestampColumnCursor();
                int corruptedConsumed = cursor.of(address, totalSize, 3, TYPE_TIMESTAMP, true);

                // The decoder reads 36 bits (5 bytes) instead of 1 bit (1 byte),
                // consuming 4 bytes of the int column's data
                Assert.assertTrue(
                        "corrupted Gorilla data should cause over-read beyond timestamp column boundary"
                                + " [expected consumed > " + tsColumnSize + ", actual=" + corruptedConsumed + "]",
                        corruptedConsumed > tsColumnSize
                );

                // The decoded third timestamp is garbage (built from int column bits)
                cursor.advanceRow(); // ts1 — still correct (stored uncompressed)
                Assert.assertEquals(ts1, cursor.getTimestamp());
                cursor.advanceRow(); // ts2 — still correct (stored uncompressed)
                Assert.assertEquals(ts2, cursor.getTimestamp());
                cursor.advanceRow(); // ts3 — garbage from corrupted Gorilla decode
                Assert.assertNotEquals(
                        "corrupted Gorilla data should produce wrong timestamp",
                        3000L, cursor.getTimestamp()
                );

                // No QwpParseException was thrown — the over-read is silent.
                // The tight Gorilla bound (maxGorillaBytes = ceil(36*1/8) = 5)
                // limits the spill to at most 4 bytes past the legitimate 1 byte.
                // Without the bound, a larger buffer would allow unlimited spill.
            } finally {
                Unsafe.free(address, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFixedWidthCursorRejectsInflatedRowCount() throws Exception {
        assertMemoryLeak(() -> {
            int bufferSize = 32;
            long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, bufferSize, (byte) 0);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(address, bufferSize, 1000, TYPE_LONG);
                Assert.fail("expected QwpParseException for inflated rowCount");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("truncated"));
            } finally {
                Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMessageCursorRejectsOversizedRowCountBeforeColumnParsing() throws Exception {
        assertMemoryLeak(() -> {
            byte[] payload = encodeTableHeaderPayload(2_000_000_000L, 1);
            byte[] message = new byte[HEADER_SIZE + payload.length];
            int offset = 0;

            message[offset++] = 'Q';
            message[offset++] = 'W';
            message[offset++] = 'P';
            message[offset++] = '1';
            message[offset++] = 1;
            message[offset++] = FLAG_GORILLA;
            message[offset++] = 1;
            message[offset++] = 0;
            message[offset++] = (byte) payload.length;
            message[offset++] = (byte) (payload.length >>> 8);
            message[offset++] = (byte) (payload.length >>> 16);
            message[offset++] = (byte) (payload.length >>> 24);
            System.arraycopy(payload, 0, message, offset, payload.length);

            long address = Unsafe.malloc(message.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < message.length; i++) {
                    Unsafe.putByte(address + i, message[i]);
                }

                QwpMessageCursor cursor = new QwpMessageCursor();
                cursor.of(address, message.length, null, null);

                try {
                    cursor.nextTable();
                    Assert.fail("expected QwpParseException for oversized rowCount");
                } catch (QwpParseException e) {
                    Assert.assertEquals(QwpParseException.ErrorCode.ROW_COUNT_EXCEEDED, e.getErrorCode());
                }
            } catch (QwpParseException e) {
                Assert.fail("message header should parse successfully before table validation: " + e.getMessage());
            } finally {
                Unsafe.free(address, message.length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMessageCursorRejectsPayloadLongerThanMessageLength() throws Exception {
        assertMemoryLeak(() -> {
            int messageLength = HEADER_SIZE;
            long address = Unsafe.malloc(messageLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, messageLength, (byte) 0);
                Unsafe.putByte(address, (byte) 'Q');
                Unsafe.putByte(address + 1, (byte) 'W');
                Unsafe.putByte(address + 2, (byte) 'P');
                Unsafe.putByte(address + 3, (byte) '1');
                Unsafe.putByte(address + 4, (byte) 1);
                Unsafe.putShort(address + 6, (short) 1);
                Unsafe.putInt(address + 8, 1);

                QwpMessageCursor cursor = new QwpMessageCursor();
                try {
                    cursor.of(address, messageLength, null, null);
                    Assert.fail("expected QwpParseException for truncated message payload");
                } catch (QwpParseException e) {
                    Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
                    Assert.assertTrue(e.getMessage().contains("payload exceeds available data"));
                }
            } finally {
                Unsafe.free(address, messageLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMessageCursorRejectsSchemaReferenceWithMismatchedColumnCount() throws Exception {
        assertMemoryLeak(() -> {
            byte[] registeredPayload = encodeTablePayloadWithFullSchema();
            byte[] registeredMessage = wrapSingleTableMessage(registeredPayload);

            byte[] referencePayload = encodeTablePayloadWithSchemaReference();
            byte[] referenceMessage = wrapSingleTableMessage(referencePayload);

            long registeredAddress = Unsafe.malloc(registeredMessage.length, MemoryTag.NATIVE_DEFAULT);
            long referenceAddress = Unsafe.malloc(referenceMessage.length, MemoryTag.NATIVE_DEFAULT);
            try {
                copyToNative(registeredMessage, registeredAddress);
                copyToNative(referenceMessage, referenceAddress);

                QwpSchemaRegistry registry = new QwpSchemaRegistry();
                QwpMessageCursor cursor = new QwpMessageCursor();

                cursor.of(registeredAddress, registeredMessage.length, registry, null);
                cursor.nextTable();

                cursor.clear();
                cursor.of(referenceAddress, referenceMessage.length, registry, null);
                try {
                    cursor.nextTable();
                    Assert.fail("expected QwpParseException for schema reference column count mismatch");
                } catch (QwpParseException e) {
                    Assert.assertEquals(QwpParseException.ErrorCode.SCHEMA_MISMATCH, e.getErrorCode());
                    Assert.assertTrue(e.getMessage().contains("schema column count mismatch"));
                }
            } finally {
                Unsafe.free(registeredAddress, registeredMessage.length, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(referenceAddress, referenceMessage.length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPayloadByteCorruptionDoesNotEscapeParser() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(null);
            long seed0 = rnd.getSeed0();
            long seed1 = rnd.getSeed1();

            for (int iter = 0; iter < FUZZ_MESSAGE_ITERATIONS; iter++) {
                byte[] message = generateValidMessage(rnd, iter);
                Assert.assertTrue("generated message must contain a payload", message.length > HEADER_SIZE);
                verifyFullParse(message);

                for (int corruption = 0; corruption < CORRUPTIONS_PER_MESSAGE; corruption++) {
                    byte[] corrupted = message.clone();
                    int nCorrupt = 1 + rnd.nextInt(3);
                    for (int i = 0; i < nCorrupt; i++) {
                        int pos = HEADER_SIZE + rnd.nextInt(corrupted.length - HEADER_SIZE);
                        corrupted[pos] = (byte) rnd.nextInt(256);
                    }

                    long address = Unsafe.malloc(corrupted.length, MemoryTag.NATIVE_DEFAULT);
                    try {
                        copyToNative(corrupted, address);
                        parseAndIterate(address, corrupted.length);
                    } catch (QwpParseException | CairoException ignored) {
                        // Expected outcomes: malformed wire data should be rejected,
                    } catch (Throwable t) {
                        Assert.fail(
                                "Unexpected " + t.getClass().getSimpleName()
                                        + " at iter=" + iter
                                        + " corruption=" + corruption
                                        + " seeds=[" + seed0 + ',' + seed1 + ']'
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
    public void testStringCursorRejectsAttackerControlledOffset() throws Exception {
        assertMemoryLeak(() -> {
            // 1 non-null string row: flag(0) + offset array (8 bytes) + string data (5 bytes)
            int legitimateSize = 14;
            int bufferSize = 256;
            long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, bufferSize, (byte) 0);
                // byte 0: null bitmap flag = 0 (already zero from setMemory)
                Unsafe.putInt(address + 1, 0);
                Unsafe.putInt(address + 5, 5);

                QwpStringColumnCursor cursor = new QwpStringColumnCursor();
                int consumed = cursor.of(address, legitimateSize, 1, TYPE_STRING);
                Assert.assertEquals(14, consumed);

                // Attacker sets offset[1] = 200 — claims 200 bytes of string data
                Unsafe.putInt(address + 5, 200);

                cursor = new QwpStringColumnCursor();
                try {
                    cursor.of(address, legitimateSize, 1, TYPE_STRING);
                    Assert.fail("expected QwpParseException for out-of-bounds string offset");
                } catch (QwpParseException e) {
                    Assert.assertTrue(e.getMessage().contains("truncated"));
                }
            } finally {
                Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testStringCursorRejectsInflatedRowCount() throws Exception {
        assertMemoryLeak(() -> {
            int bufferSize = 16;
            long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, bufferSize, (byte) 0);

                QwpStringColumnCursor cursor = new QwpStringColumnCursor();
                // rowCount=100 needs (101)*4 = 404 bytes for offset array alone
                cursor.of(address, bufferSize, 100, TYPE_STRING);
                Assert.fail("expected QwpParseException for inflated rowCount");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("truncated"));
            } finally {
                Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Reads a column value and returns a checksum contribution.
     * The returned value is accumulated by the caller to prevent
     * the JIT from erasing the getter calls as dead code.
     */
    private static long consumeColumnValue(QwpTableBlockCursor table, int col) {
        if (table.isColumnNull(col)) {
            return 0;
        }

        long acc = 0;
        byte typeCode = table.getColumnDef(col).getTypeCode();
        switch (typeCode) {
            case TYPE_BOOLEAN:
                acc = table.getBooleanColumn(col).getValue() ? 1 : 0;
                break;
            case TYPE_BYTE:
            case TYPE_SHORT:
            case TYPE_CHAR:
            case TYPE_INT:
            case TYPE_LONG:
            case TYPE_DATE:
                acc = table.getFixedWidthColumn(col).getLong();
                break;
            case TYPE_FLOAT:
            case TYPE_DOUBLE:
                acc = Double.doubleToRawLongBits(table.getFixedWidthColumn(col).getDouble());
                break;
            case TYPE_UUID: {
                QwpFixedWidthColumnCursor cursor = table.getFixedWidthColumn(col);
                acc = cursor.getUuidHi() ^ cursor.getUuidLo();
                break;
            }
            case TYPE_LONG256: {
                QwpFixedWidthColumnCursor cursor = table.getFixedWidthColumn(col);
                acc = cursor.getLong256_0() ^ cursor.getLong256_1()
                        ^ cursor.getLong256_2() ^ cursor.getLong256_3();
                break;
            }
            case TYPE_STRING:
            case TYPE_VARCHAR:
                acc = table.getStringColumn(col).getUtf8Value().size();
                break;
            case TYPE_SYMBOL: {
                acc = table.getSymbolColumn(col).getSymbolIndex();
                break;
            }
            case TYPE_GEOHASH:
                acc = table.getGeoHashColumn(col).getGeoHash();
                break;
            case TYPE_DECIMAL64:
                acc = table.getDecimalColumn(col).getDecimal64();
                break;
            case TYPE_DECIMAL128: {
                QwpDecimalColumnCursor cursor = table.getDecimalColumn(col);
                acc = cursor.getDecimal128Hi() ^ cursor.getDecimal128Lo();
                break;
            }
            case TYPE_DECIMAL256: {
                QwpDecimalColumnCursor cursor = table.getDecimalColumn(col);
                acc = cursor.getDecimal256Hh() ^ cursor.getDecimal256Hl()
                        ^ cursor.getDecimal256Lh() ^ cursor.getDecimal256Ll();
                break;
            }
            case TYPE_DOUBLE_ARRAY:
            case TYPE_LONG_ARRAY: {
                QwpArrayColumnCursor cursor = table.getArrayColumn(col);
                for (int d = 0; d < cursor.getNDims(); d++) {
                    acc += cursor.getDimSize(d);
                }
                acc += cursor.getTotalElements();
                break;
            }
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
                acc = table.getTimestampColumn(col).getTimestamp();
                break;
            default:
                throw new AssertionError("unsupported fuzz column type: " + typeCode);
        }
        return acc;
    }

    private static byte[] copyFromNative(long address, int length) {
        byte[] dst = new byte[length];
        for (int i = 0; i < length; i++) {
            dst[i] = Unsafe.getByte(address + i);
        }
        return dst;
    }

    private static void copyToNative(byte[] src, long address) {
        for (int i = 0; i < src.length; i++) {
            Unsafe.putByte(address + i, src[i]);
        }
    }

    private static byte[] encodeTableHeaderPayload(long rowCount, int columnCount) {
        byte[] nameBytes = "test".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int size = QwpVarint.encodedLength(nameBytes.length)
                + nameBytes.length
                + QwpVarint.encodedLength(rowCount)
                + QwpVarint.encodedLength(columnCount);
        byte[] buf = new byte[size];
        int offset = 0;
        offset = QwpVarint.encode(buf, offset, nameBytes.length);
        System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
        offset += nameBytes.length;
        offset = QwpVarint.encode(buf, offset, rowCount);
        QwpVarint.encode(buf, offset, columnCount);
        return buf;
    }

    private static byte[] encodeTablePayloadWithFullSchema() {
        byte[] header = encodeTableHeaderPayload(1, 1);
        QwpSchema schema = QwpSchema.create(new QwpColumnDef[]{new QwpColumnDef("a", QwpConstants.TYPE_LONG)});
        byte[] schemaBytes = new byte[schema.encodedSize(0)];
        schema.encode(schemaBytes, 0, 0);
        byte[] columnData = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0};

        byte[] payload = new byte[header.length + schemaBytes.length + columnData.length];
        int offset = 0;
        System.arraycopy(header, 0, payload, offset, header.length);
        offset += header.length;
        System.arraycopy(schemaBytes, 0, payload, offset, schemaBytes.length);
        offset += schemaBytes.length;
        System.arraycopy(columnData, 0, payload, offset, columnData.length);
        return payload;
    }

    private static byte[] encodeTablePayloadWithSchemaReference() {
        byte[] header = encodeTableHeaderPayload(1, 2);
        byte[] schemaRef = new byte[1 + QwpVarint.encodedLength(0)];
        QwpSchema.encodeReference(schemaRef, 0, 0);

        byte[] payload = new byte[header.length + schemaRef.length];
        System.arraycopy(header, 0, payload, 0, header.length);
        System.arraycopy(schemaRef, 0, payload, header.length, schemaRef.length);
        return payload;
    }

    private static byte[] generateValidMessage(Rnd rnd, int iter) {
        try (QwpTableBuffer buffer = new QwpTableBuffer("fuzz_" + iter);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled((iter & 1) != 0);
            populateFuzzTable(buffer, rnd, iter);

            int size = encoder.encode(buffer, false);
            QwpBufferWriter writer = encoder.getBuffer();
            return copyFromNative(writer.getBufferPtr(), size);
        }
    }

    private static long parseAndIterate(long address, int length) throws QwpParseException {
        QwpMessageCursor cursor = new QwpMessageCursor(FUZZ_MAX_ROWS_PER_TABLE);
        cursor.of(address, length, new QwpSchemaRegistry(), new ObjList<>());

        long checksum = 0;
        while (cursor.hasNextTable()) {
            QwpTableBlockCursor table = cursor.nextTable();
            while (table.hasNextRow()) {
                table.nextRow();
                for (int col = 0; col < table.getColumnCount(); col++) {
                    checksum ^= consumeColumnValue(table, col);
                }
            }
        }
        return checksum;
    }

    private static void populateFuzzTable(QwpTableBuffer buffer, Rnd rnd, int iter) {
        QwpTableBuffer.ColumnBuffer boolCol = buffer.getOrCreateColumn("b_bool", TYPE_BOOLEAN, true);
        QwpTableBuffer.ColumnBuffer byteCol = buffer.getOrCreateColumn("b_byte", TYPE_BYTE, false);
        QwpTableBuffer.ColumnBuffer shortCol = buffer.getOrCreateColumn("b_short", TYPE_SHORT, false);
        QwpTableBuffer.ColumnBuffer charCol = buffer.getOrCreateColumn("b_char", TYPE_CHAR, false);
        QwpTableBuffer.ColumnBuffer intCol = buffer.getOrCreateColumn("b_int", TYPE_INT, false);
        QwpTableBuffer.ColumnBuffer floatCol = buffer.getOrCreateColumn("b_float", TYPE_FLOAT, false);
        QwpTableBuffer.ColumnBuffer longCol = buffer.getOrCreateColumn("b_long", TYPE_LONG, false);
        QwpTableBuffer.ColumnBuffer dateCol = buffer.getOrCreateColumn("b_date", TYPE_DATE, true);
        QwpTableBuffer.ColumnBuffer doubleCol = buffer.getOrCreateColumn("b_double", TYPE_DOUBLE, false);
        QwpTableBuffer.ColumnBuffer symCol = buffer.getOrCreateColumn("b_sym", TYPE_SYMBOL, true);
        QwpTableBuffer.ColumnBuffer strCol = buffer.getOrCreateColumn("b_str", TYPE_VARCHAR, true);
        QwpTableBuffer.ColumnBuffer varcharCol = buffer.getOrCreateColumn("b_varchar", TYPE_VARCHAR, false);
        QwpTableBuffer.ColumnBuffer uuidCol = buffer.getOrCreateColumn("b_uuid", TYPE_UUID, true);
        QwpTableBuffer.ColumnBuffer long256Col = buffer.getOrCreateColumn("b_l256", TYPE_LONG256, true);
        QwpTableBuffer.ColumnBuffer geoCol = buffer.getOrCreateColumn("b_geo", TYPE_GEOHASH, true);
        QwpTableBuffer.ColumnBuffer dec64Col = buffer.getOrCreateColumn("b_dec64", TYPE_DECIMAL64, true);
        QwpTableBuffer.ColumnBuffer dec128Col = buffer.getOrCreateColumn("b_dec128", TYPE_DECIMAL128, true);
        QwpTableBuffer.ColumnBuffer dec256Col = buffer.getOrCreateColumn("b_dec256", TYPE_DECIMAL256, true);
        QwpTableBuffer.ColumnBuffer doubleArrayCol = buffer.getOrCreateColumn("b_darr", TYPE_DOUBLE_ARRAY, true);
        QwpTableBuffer.ColumnBuffer longArrayCol = buffer.getOrCreateColumn("b_larr", TYPE_LONG_ARRAY, true);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("b_ts", TYPE_TIMESTAMP, true);
        QwpTableBuffer.ColumnBuffer tsNsCol = buffer.getOrCreateColumn("b_tsns", TYPE_TIMESTAMP_NANOS, true);
        QwpTableBuffer.ColumnBuffer designatedTsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        int rowCount = 3 + rnd.nextInt(4);
        long baseMicros = 1_705_276_800_000_000L + iter * 1_000L;
        long baseNanos = 1_705_276_800_000_000_000L + iter * 1_000_000L;

        for (int row = 0; row < rowCount; row++) {
            if (((row + iter) & 1) == 0) {
                boolCol.addBoolean(rnd.nextBoolean());
            } else {
                boolCol.addNull();
            }
            byteCol.addByte((byte) (rnd.nextInt(128) - 64));
            shortCol.addShort((short) (1_000 + rnd.nextInt(2_000)));
            charCol.addShort((short) ('A' + (row + iter) % 26));
            intCol.addInt(10_000 + rnd.nextInt(50_000));
            floatCol.addFloat(1.5f + row + rnd.nextInt(100) / 100.0f);
            longCol.addLong(100_000L + rnd.nextLong(1_000_000L));

            if (row % 3 == 0) {
                dateCol.addNull();
            } else {
                dateCol.addLong(1_705_276_800_000L + row * 60_000L);
            }

            doubleCol.addDouble(2.5 + row + rnd.nextInt(1_000) / 100.0);

            if ((row & 1) == 0) {
                symCol.addSymbol("sym_" + ((iter + row) % 4));
            } else {
                symCol.addNull();
            }

            if (row % 3 == 1) {
                strCol.addNull();
            } else {
                strCol.addString("s_" + iter + '_' + row + '_' + rnd.nextInt(10_000));
            }
            varcharCol.addString("v_" + iter + '_' + row);

            if ((row & 1) == 0) {
                uuidCol.addUuid(rnd.nextLong(), rnd.nextLong());
                long256Col.addLong256(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                geoCol.addGeoHash(rnd.nextGeoHashLong(30), 30);
                dec64Col.addDecimal64(new Decimal64((long) rnd.nextInt(100_000) - 50_000L, 2));
                dec128Col.addDecimal128(Decimal128.fromLong((long) rnd.nextInt(1_000_000) - 500_000L, 4));
                dec256Col.addDecimal256(Decimal256.fromLong((long) rnd.nextInt(10_000_000) - 5_000_000L, 5));
                doubleArrayCol.addDoubleArray(new double[]{row + 0.1, row + 0.2, rnd.nextInt(100) / 10.0});
                longArrayCol.addLongArray(new long[]{row, row + 1L, rnd.nextLong(1_000L)});
                tsCol.addLong(baseMicros + row * 1_000L + rnd.nextInt(50));
                tsNsCol.addLong(baseNanos + row * 1_000_000L + rnd.nextInt(1_000));
            } else {
                uuidCol.addNull();
                long256Col.addNull();
                geoCol.addNull();
                dec64Col.addNull();
                dec128Col.addNull();
                dec256Col.addNull();
                doubleArrayCol.addNull();
                longArrayCol.addNull();
                tsCol.addNull();
                tsNsCol.addNull();
            }

            designatedTsCol.addLong(1_000_000L + iter * 10_000L + row * 1_000L);
            buffer.nextRow();
        }
    }

    private static void verifyFullParse(byte[] message) throws QwpParseException {
        long address = Unsafe.malloc(message.length, MemoryTag.NATIVE_DEFAULT);
        try {
            copyToNative(message, address);
            long checksum = parseAndIterate(address, message.length);
            // Assert that parsing actually consumed values (guards against JIT dead-code elimination)
            Assert.assertNotEquals("valid message must produce a non-zero checksum", 0, checksum);
        } finally {
            Unsafe.free(address, message.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] wrapSingleTableMessage(byte[] payload) {
        byte[] message = new byte[HEADER_SIZE + payload.length];
        int offset = 0;
        message[offset++] = 'Q';
        message[offset++] = 'W';
        message[offset++] = 'P';
        message[offset++] = '1';
        message[offset++] = 1;
        message[offset++] = FLAG_GORILLA;
        message[offset++] = 1;
        message[offset++] = 0;
        message[offset++] = (byte) payload.length;
        message[offset++] = (byte) (payload.length >>> 8);
        message[offset++] = (byte) (payload.length >>> 16);
        message[offset++] = (byte) (payload.length >>> 24);
        System.arraycopy(payload, 0, message, offset, payload.length);
        return message;
    }
}
