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

import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpNullBitmap;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpSymbolDecoderTest {

    @Test
    public void testDecodeEmptySymbolColumn() throws Exception {
        // no null bitmap + empty dictionary (varint 0), 0 rows.
        int allocSize = 1 + QwpVarint.encodedLength(0); // flag byte + dictionary size
        long address = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            QwpVarint.encode(address + 1, 0); // empty dictionary
            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            int consumed = cursor.of(address, allocSize, 0);
            Assert.assertEquals(allocSize, consumed);
        } finally {
            Unsafe.free(address, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeMultipleSymbols() throws Exception {
        String[] values = {"apple", "banana", "cherry", "date"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDecodeSingleSymbol() throws Exception {
        String[] values = {"symbol_a"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDeltaSymbolDictExcessiveStartId() throws Exception {
        // A malicious client sends deltaStartId = 100_001, deltaCount = 0.
        // The while loop in parseDeltaSymbolDict() grows connectionSymbolDict
        // to 100K entries without question. With larger values (e.g. 2 billion),
        // this exhausts heap memory (DoS). The parser should reject oversized
        // delta dictionaries with QwpParseException.
        assertMemoryLeak(() -> {
            int deltaStartId = 1_000_001;
            int deltaCount = 0;

            int payloadSize = QwpVarint.encodedLength(deltaStartId) + QwpVarint.encodedLength(deltaCount);
            int totalSize = HEADER_SIZE + payloadSize;
            long address = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putInt(address + HEADER_OFFSET_MAGIC, MAGIC_MESSAGE);
                Unsafe.putByte(address + HEADER_OFFSET_VERSION, VERSION_1);
                Unsafe.putByte(address + HEADER_OFFSET_FLAGS, FLAG_DELTA_SYMBOL_DICT);
                Unsafe.putShort(address + HEADER_OFFSET_TABLE_COUNT, (short) 0);
                Unsafe.putInt(address + HEADER_OFFSET_PAYLOAD_LENGTH, payloadSize);

                long pos = address + HEADER_SIZE;
                pos = QwpVarint.encode(pos, deltaStartId);
                QwpVarint.encode(pos, deltaCount);

                QwpMessageCursor cursor = new QwpMessageCursor();
                ObjList<String> connectionDict = new ObjList<>();
                try {
                    cursor.of(address, totalSize, null, connectionDict);
                    Assert.fail("Expected QwpParseException for excessive delta symbol dictionary size");
                } catch (QwpParseException e) {
                    Assert.assertTrue(e.getMessage().contains("delta symbol dictionary"));
                }
            } finally {
                Unsafe.free(address, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaSymbolDictIntegerOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long deltaStartId = Integer.MAX_VALUE;
            int deltaCount = 1;

            byte[] symbolBytes = "x".getBytes(StandardCharsets.UTF_8);
            int payloadSize = QwpVarint.encodedLength(deltaStartId)
                    + QwpVarint.encodedLength(deltaCount)
                    + QwpVarint.encodedLength(symbolBytes.length)
                    + symbolBytes.length;
            int totalSize = HEADER_SIZE + payloadSize;
            long address = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putInt(address + HEADER_OFFSET_MAGIC, MAGIC_MESSAGE);
                Unsafe.putByte(address + HEADER_OFFSET_VERSION, VERSION_1);
                Unsafe.putByte(address + HEADER_OFFSET_FLAGS, FLAG_DELTA_SYMBOL_DICT);
                Unsafe.putShort(address + HEADER_OFFSET_TABLE_COUNT, (short) 0);
                Unsafe.putInt(address + HEADER_OFFSET_PAYLOAD_LENGTH, payloadSize);

                long pos = address + HEADER_SIZE;
                pos = QwpVarint.encode(pos, deltaStartId);
                pos = QwpVarint.encode(pos, deltaCount);
                pos = QwpVarint.encode(pos, symbolBytes.length);
                for (byte b : symbolBytes) {
                    Unsafe.putByte(pos++, b);
                }

                QwpMessageCursor cursor = new QwpMessageCursor();
                ObjList<String> connectionDict = new ObjList<>();
                try {
                    cursor.of(address, totalSize, null, connectionDict);
                    Assert.fail("Expected QwpParseException for integer overflow in delta symbol dictionary");
                } catch (QwpParseException e) {
                    Assert.assertTrue(e.getMessage().contains("delta symbol dictionary"));
                }
            } finally {
                Unsafe.free(address, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDictionaryEmpty() throws Exception {
        int rowCount = 3;
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // null bitmap present
            Unsafe.putByte(pos, (byte) 1);
            pos++;

            // Null bitmap (all nulls)
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            QwpNullBitmapTestUtil.fillAllNull(pos, rowCount);
            pos += bitmapSize;

            // Empty dictionary (size = 0)
            pos = QwpVarint.encode(pos, 0);

            // No value indices needed: all rows are null (skipped by bitmap)

            int actualSize = (int) (pos - address);
            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, actualSize, rowCount);

            for (int i = 0; i < rowCount; i++) {
                boolean isNull = cursor.advanceRow();
                Assert.assertTrue("Row " + i + " should be null", isNull);
                Assert.assertTrue(cursor.isNull());
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDictionaryLarge() throws Exception {
        int dictSize = 1000;
        String[] values = new String[dictSize];
        for (int i = 0; i < dictSize; i++) {
            values[i] = "symbol_" + i;
        }
        assertRoundTrip(values, null);
    }

    @Test
    public void testDictionaryParsing() throws Exception {
        String[] values = {"a", "b", "a", "b", "c", "a"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDictionarySizeRejectsNegativeVarintValue() {
        int size = 32;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;
            Unsafe.putByte(pos++, (byte) 0);
            pos = QwpVarint.encode(pos, Long.MIN_VALUE);

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            try {
                cursor.of(address, (int) (pos - address), 0);
                Assert.fail("Expected QwpParseException for negative dictionary size varint");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("dictionary size out of int range"));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDictionaryStringLengthRejectsNegativeVarintValue() {
        int size = 64;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;
            Unsafe.putByte(pos++, (byte) 0);
            pos = QwpVarint.encode(pos, 1);
            pos = QwpVarint.encode(pos, -1L);

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            try {
                cursor.of(address, (int) (pos - address), 0);
                Assert.fail("Expected QwpParseException for negative dictionary string length varint");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("dictionary string length out of int range"));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInsufficientDataForDictionary() {
        int size = 6;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;
            // no null bitmap
            Unsafe.putByte(pos, (byte) 0);
            pos++;
            // Dictionary size = 1
            pos = QwpVarint.encode(pos, 1);
            // String length = 100 (but we don't have that much data)
            QwpVarint.encode(pos, 100);

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, size, 1);
            Assert.fail("Expected QwpParseException");
        } catch (QwpParseException e) {
            Assert.assertFalse(e.getMessage().isEmpty());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInvalidDictionaryIndex() {
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // no null bitmap
            Unsafe.putByte(pos, (byte) 0);
            pos++;

            // Dictionary: 1 entry
            pos = QwpVarint.encode(pos, 1);

            // Entry "a"
            byte[] aBytes = "a".getBytes(StandardCharsets.UTF_8);
            pos = QwpVarint.encode(pos, aBytes.length);
            for (byte b : aBytes) {
                Unsafe.putByte(pos++, b);
            }

            // Value: index 5 (invalid, only 1 entry in dictionary)
            pos = QwpVarint.encode(pos, 5);

            int actualSize = (int) (pos - address);
            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, actualSize, 1);
            cursor.advanceRow();
            Assert.fail("Expected QwpParseException for out-of-bounds dictionary index");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("symbol index out of range"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInvalidDictionaryIndexDeltaMode() {
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // no null bitmap
            Unsafe.putByte(pos, (byte) 0);
            pos++;

            // Value: index 3 (invalid, connection dictionary has only 2 entries)
            pos = QwpVarint.encode(pos, 3);

            int actualSize = (int) (pos - address);
            ObjList<String> connectionDict = new ObjList<>();
            connectionDict.add("alpha");
            connectionDict.add("beta");

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, actualSize, 1, connectionDict);
            cursor.advanceRow();
            Assert.fail("Expected QwpParseException for out-of-bounds delta dictionary index");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("symbol index out of range"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSymbolIndexMapping() throws Exception {
        String[] values = {"same", "same", "same", "same"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testSymbolIndexRejectsNegativeVarintValue() {
        int size = 64;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            Unsafe.putByte(pos++, (byte) 0);
            pos = QwpVarint.encode(pos, 1);
            pos = QwpVarint.encode(pos, 1);
            Unsafe.putByte(pos++, (byte) 'a');
            pos = QwpVarint.encode(pos, Long.MIN_VALUE);

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            // Validation now happens in of() instead of advanceRow()
            cursor.of(address, (int) (pos - address), 1);
            Assert.fail("Expected QwpParseException for negative symbol index varint");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("symbol index out of int range"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSymbolLargeColumn() throws Exception {
        int rowCount = 10_000;
        int uniqueSymbols = 100;
        String[] values = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = "sym_" + (i % uniqueSymbols);
        }
        assertRoundTrip(values, null);
    }

    @Test
    public void testSymbolNullIndex() {
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // no null bitmap
            Unsafe.putByte(pos, (byte) 0);
            pos++;

            // Dictionary: 2 entries
            pos = QwpVarint.encode(pos, 2);

            // Entry "a"
            byte[] aBytes = "a".getBytes(StandardCharsets.UTF_8);
            pos = QwpVarint.encode(pos, aBytes.length);
            for (byte b : aBytes) {
                Unsafe.putByte(pos++, b);
            }

            // Entry "b"
            byte[] bBytes = "b".getBytes(StandardCharsets.UTF_8);
            pos = QwpVarint.encode(pos, bBytes.length);
            for (byte b : bBytes) {
                Unsafe.putByte(pos++, b);
            }

            // Values: index 0 for row 0, then index 1 for row 1
            pos = QwpVarint.encode(pos, 0);
            pos = QwpVarint.encode(pos, 1);

            int actualSize = (int) (pos - address);
            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, actualSize, 2);

            // Row 0: "a"
            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals("a", Objects.toString(cursor.getSymbolCharSequence()));
            Assert.assertEquals(0, cursor.getSymbolIndex());

            // Row 1: "b"
            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals("b", Objects.toString(cursor.getSymbolCharSequence()));
            Assert.assertEquals(1, cursor.getSymbolIndex());
        } catch (QwpParseException e) {
            Assert.fail("Unexpected parse exception: " + e.getMessage());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSymbolRepeatedValues() throws Exception {
        String[] symbols = {"low", "medium", "high"};
        int rowCount = 1000;
        String[] values = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = symbols[i % 3];
        }
        assertRoundTrip(values, null);
    }

    @Test
    public void testSymbolUtf8() throws Exception {
        String[] values = {"日本語", "中文", "한국어", "Ελληνικά"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testSymbolWithNulls() throws Exception {
        String[] values = {"a", null, "b", null};
        boolean[] nulls = {false, true, false, true};
        assertRoundTrip(values, nulls);
    }

    private static int findSymbolColumnIndex(QwpTableBlockCursor table) {
        for (int c = 0; c < table.getColumnCount(); c++) {
            if (table.getColumnDef(c).getTypeCode() == TYPE_SYMBOL) {
                return c;
            }
        }
        return -1;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(String[] values, boolean[] nulls, boolean useNullBitmap) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_symbol");
        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_SYMBOL, useNullBitmap);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
        for (int i = 0; i < values.length; i++) {
            if (useNullBitmap && nulls[i]) {
                col.addNull();
            } else {
                col.addSymbol(values[i]);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private void assertRoundTrip(String[] values, boolean[] nulls) throws Exception {
        assertMemoryLeak(() -> {
            boolean useNullBitmap = nulls != null;
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = getQwpTableBuffer(values, nulls, useNullBitmap);
                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();
                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();
                    Assert.assertEquals(values.length, table.getRowCount());
                    int colIdx = findSymbolColumnIndex(table);
                    Assert.assertNotEquals("SYMBOL column not found", -1, colIdx);
                    for (int i = 0; i < values.length; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();
                        if (useNullBitmap && nulls[i]) {
                            Assert.assertTrue("Row " + i + " should be null",
                                    table.isColumnNull(colIdx));
                        } else {
                            Assert.assertFalse("Row " + i + " should not be null",
                                    table.isColumnNull(colIdx));
                            QwpSymbolColumnCursor cursor = table.getSymbolColumn(colIdx);
                            Assert.assertEquals("Row " + i + " value mismatch",
                                    values[i], Objects.toString(cursor.getSymbolCharSequence()));
                        }
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }
}
