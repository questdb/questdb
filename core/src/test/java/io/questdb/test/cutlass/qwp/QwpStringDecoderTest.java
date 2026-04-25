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
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpStringDecoderTest {

    @Test
    public void testDecodeAllEmptyStrings() throws Exception {
        assertRoundTrip(new String[]{"", "", ""}, null);
    }

    @Test
    public void testDecodeAllNulls() throws Exception {
        assertRoundTrip(
                new String[]{"", "", ""},
                new boolean[]{true, true, true}
        );
    }

    @Test
    public void testDecodeEmptyStringColumn() throws QwpParseException {
        // no null bitmap + offset array (1 entry for 0 value-rows)
        int allocSize = 1 + 4; // flag byte + (0+1)*4
        long address = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            Unsafe.putInt(address + 1, 0); // single offset entry = 0
            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            int consumed = cursor.of(address, allocSize, 0, TYPE_STRING);
            Assert.assertEquals(allocSize, consumed);
        } finally {
            Unsafe.free(address, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeEmptyStrings() throws Exception {
        assertRoundTrip(new String[]{"", "nonempty", "", ""}, null);
    }

    @Test
    public void testDecodeMultipleStrings() throws Exception {
        assertRoundTrip(new String[]{"hello", "world", "test", "strings"}, null);
    }

    @Test
    public void testDecodeSingleString() throws Exception {
        assertRoundTrip(new String[]{"hello"}, null);
    }

    @Test
    public void testDecodeUtf8MultiByte() throws Exception {
        assertRoundTrip(new String[]{
                "A",           // 1 byte
                "é",      // 2 bytes
                "€",      // 3 bytes
                "\uD834\uDD1E" // 4 bytes (musical G clef)
        }, null);
    }

    @Test
    public void testDecodeUtf8Strings() throws Exception {
        assertRoundTrip(
                new String[]{"Hello 世界", "Привет мир", "こんにちは", "\uD83C\uDF89\uD83C\uDF8A"},
                null
        );
    }

    @Test
    public void testDecodeVarcharSameAsString() throws Exception {
        assertRoundTrip(new String[]{"varchar", "test", "values"}, null);
    }

    @Test
    public void testDecodeWithNulls() throws Exception {
        assertRoundTrip(
                new String[]{"hello", "", "world", ""},
                new boolean[]{false, true, false, true}
        );
    }

    @Test
    public void testInsufficientDataForNullBitmap() {
        // null bitmap flag=1 but only 1 byte left, not enough for bitmap (100 rows need 13 bytes)
        int rowCount = 100;
        int size = 2; // 1 byte flag + 1 byte (not enough for bitmap)
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 1); // null bitmap present
            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            cursor.of(address, size, rowCount, TYPE_STRING);
            Assert.fail("expected QwpParseException for truncated null bitmap");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInsufficientDataForOffsetArray() {
        // no null bitmap + 10 rows need (10+1)*4 = 44 bytes for offset array, but we only provide 5
        int rowCount = 10;
        int size = 6; // 1 byte flag + 5 bytes
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            cursor.of(address, size, rowCount, TYPE_STRING);
            Assert.fail("expected QwpParseException for truncated offset array");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNonMonotonicOffsets() throws QwpParseException {
        // no null bitmap + offsets go backwards in the middle: 0, 10, 5, 10.
        int rowCount = 3;
        int offsetArraySize = (rowCount + 1) * 4;
        int size = 1 + offsetArraySize + 10; // flag byte + offset array + string data
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            Unsafe.putInt(address + 1, 0);
            Unsafe.putInt(address + 1 + 4, 10);
            Unsafe.putInt(address + 1 + 8, 5); // Goes backward
            Unsafe.putInt(address + 1 + 12, 10);

            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            cursor.of(address, size, rowCount, TYPE_STRING);

            // Row 0: offset 0..10 is valid (stringDataLength=10)
            cursor.advanceRow();
            Assert.assertEquals(10, cursor.getUtf8Value().size());

            // Row 1: offset 10..5 goes backward, must throw
            try {
                cursor.advanceRow();
                Assert.fail("expected QwpParseException for non-monotonic offset array");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY, e.getErrorCode());
                Assert.assertTrue(e.getMessage().contains("exceeds string data size"));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testOffsetArrayRowExceedsDeclaredStringData() throws QwpParseException {
        // Corrupted middle offset claims 200 bytes for row 0, but the declared
        // string data section is only 5 bytes long. The backing allocation is
        // larger to keep the test safe even before the fix.
        int rowCount = 2;
        int offsetArraySize = (rowCount + 1) * 4;
        int logicalSize = 1 + offsetArraySize + 5;
        int allocSize = 256;
        long address = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.setMemory(address, allocSize, (byte) 0);
            Unsafe.putByte(address, (byte) 0);
            Unsafe.putInt(address + 1, 0);
            Unsafe.putInt(address + 1 + 4, 200);
            Unsafe.putInt(address + 1 + 8, 5);

            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            cursor.of(address, logicalSize, rowCount, TYPE_STRING);
            try {
                cursor.advanceRow();
                Assert.fail("expected QwpParseException for out-of-bounds intermediate string offset");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY, e.getErrorCode());
                Assert.assertTrue(e.getMessage().contains("exceeds string data size"));
            }
        } finally {
            Unsafe.free(address, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testOffsetArrayFirstOffsetMustBeZero() {
        // no null bitmap + non-zero first offset
        int rowCount = 2;
        int offsetArraySize = (rowCount + 1) * 4;
        int size = 1 + offsetArraySize + 5; // flag byte + offset array + string data
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            Unsafe.putInt(address + 1, 5); // Should be 0
            Unsafe.putInt(address + 1 + 4, 5);
            Unsafe.putInt(address + 1 + 8, 5);

            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            try {
                cursor.of(address, size, rowCount, TYPE_STRING);
                Assert.fail("expected QwpParseException for invalid first string offset");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY, e.getErrorCode());
                Assert.assertTrue(e.getMessage().contains("offset[0]"));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testOffsetArrayOutOfBounds() {
        // no null bitmap + last offset exceeds available data
        int rowCount = 1;
        int offsetArraySize = (rowCount + 1) * 4;
        int size = 1 + offsetArraySize + 5; // flag byte + offset array + 5 bytes string data
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            Unsafe.putInt(address + 1, 0);
            Unsafe.putInt(address + 1 + 4, 100); // Claims 100 bytes, but only 5 available

            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            cursor.of(address, size, rowCount, TYPE_STRING);
            Assert.fail("expected QwpParseException for out-of-bounds string data");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testOffsetArrayTailOverflowIsRejectedDuringInitialization() {
        int rowCount = 1;
        int offsetArraySize = (rowCount + 1) * 4;
        int size = 1 + offsetArraySize + 5;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0);
            Unsafe.putInt(address + 1, 0);
            Unsafe.putInt(address + 1 + 4, Integer.MAX_VALUE);

            QwpStringColumnCursor cursor = new QwpStringColumnCursor();
            try {
                cursor.of(address, size, rowCount, TYPE_STRING);
                Assert.fail("expected QwpParseException for overflowed string data tail");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
                Assert.assertTrue(e.getMessage().contains("truncated"));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStringLargeColumn() throws Exception {
        int rowCount = 10_000;
        String[] values = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = "string_" + i;
        }
        assertRoundTrip(values, null);
    }

    @Test
    public void testStringMaxLength() throws Exception {
        assertRoundTrip(new String[]{"x".repeat(10_000)}, null);
    }

    private static int findStringColumnIndex(QwpTableBlockCursor table) {
        for (int c = 0; c < table.getColumnCount(); c++) {
            byte code = table.getColumnDef(c).getTypeCode();
            if (code == TYPE_VARCHAR) {
                return c;
            }
        }
        return -1;
    }

    private void assertRoundTrip(String[] values, boolean[] nulls) throws Exception {
        assertMemoryLeak(() -> {
            boolean useNullBitmap = nulls != null;
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_string");
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_VARCHAR, useNullBitmap);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

                for (int i = 0; i < values.length; i++) {
                    if (useNullBitmap && nulls[i]) {
                        col.addNull();
                    } else {
                        col.addString(values[i]);
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

                    int colIdx = findStringColumnIndex(table);
                    Assert.assertNotEquals("STRING column not found", -1, colIdx);

                    for (int i = 0; i < values.length; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();

                        if (useNullBitmap && nulls[i]) {
                            Assert.assertTrue("Row " + i + " should be null",
                                    table.isColumnNull(colIdx));
                        } else {
                            Assert.assertFalse("Row " + i + " should not be null",
                                    table.isColumnNull(colIdx));
                            QwpStringColumnCursor cursor = table.getStringColumn(colIdx);
                            Assert.assertEquals("Row " + i + " value mismatch",
                                    values[i], cursor.getUtf8Value().toString());
                        }
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }
}
