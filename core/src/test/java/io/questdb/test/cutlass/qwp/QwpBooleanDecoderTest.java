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
import io.questdb.cutlass.qwp.protocol.QwpBooleanColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_BOOLEAN;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpBooleanDecoderTest {

    @Test
    public void testBitOrderLsbFirst() throws QwpParseException {
        // Hand-craft: no-null-bitmap flag + 1 byte with only bit 0 set: 0b00000001
        int size = 2;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            Unsafe.getUnsafe().putByte(address + 1, (byte) 0b00000001);

            QwpBooleanColumnCursor cursor = new QwpBooleanColumnCursor();
            cursor.of(address, size, 8);

            // Bit 0 should be true, bits 1-7 should be false
            cursor.advanceRow();
            Assert.assertTrue(cursor.getValue());
            for (int i = 1; i < 8; i++) {
                cursor.advanceRow();
                Assert.assertFalse("Bit " + i + " should be false", cursor.getValue());
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBitOrderMsbOfByte() throws QwpParseException {
        // Hand-craft: no-null-bitmap flag + 1 byte with only bit 7 set: 0b10000000
        int size = 2;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            Unsafe.getUnsafe().putByte(address + 1, (byte) 0b10000000);

            QwpBooleanColumnCursor cursor = new QwpBooleanColumnCursor();
            cursor.of(address, size, 8);

            // Bits 0-6 should be false, bit 7 should be true
            for (int i = 0; i < 7; i++) {
                cursor.advanceRow();
                Assert.assertFalse("Bit " + i + " should be false", cursor.getValue());
            }
            cursor.advanceRow();
            Assert.assertTrue(cursor.getValue());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeAllFalse() throws Exception {
        boolean[] values = {false, false, false, false, false, false, false, false};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDecodeAllNulls() throws Exception {
        boolean[] values = {false, false, false, false};
        boolean[] nulls = {true, true, true, true};
        assertRoundTrip(values, nulls);
    }

    @Test
    public void testDecodeAllTrue() throws Exception {
        boolean[] values = {true, true, true, true, true, true, true, true};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDecodeEmptyColumn() throws QwpParseException {
        // Even for 0 rows, the cursor reads the null bitmap flag
        int size = 1;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            QwpBooleanColumnCursor cursor = new QwpBooleanColumnCursor();
            int consumed = cursor.of(address, size, 0);
            Assert.assertEquals(1, consumed);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForNullBitmap() {
        // null bitmap flag=1 but only 1 byte left, not enough for bitmap (10 rows need 2 bytes)
        int size = 2;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 1); // null bitmap present
            QwpBooleanColumnCursor cursor = new QwpBooleanColumnCursor();
            cursor.of(address, size, 10);
            Assert.fail("expected QwpParseException for truncated null bitmap");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForValues() {
        // no null bitmap, then 16 rows need 2 bytes for value bits, we only provide 1
        int size = 2;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            QwpBooleanColumnCursor cursor = new QwpBooleanColumnCursor();
            cursor.of(address, size, 16);
            Assert.fail("expected QwpParseException for truncated value bitmap");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeLargeColumn() throws Exception {
        int rowCount = 100_000;
        boolean[] values = new boolean[rowCount];

        // Pattern: every 3rd value is true
        for (int i = 0; i < rowCount; i++) {
            values[i] = (i % 3) == 0;
        }

        assertRoundTrip(values, null);
    }

    @Test
    public void testDecodeMixedValues() throws Exception {
        boolean[] values = {true, false, true, false, true, false, true, false};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDecodePartialByte() throws Exception {
        // 10 values = 2 bytes, only 2 bits used in second byte
        boolean[] values = {true, false, true, false, true, false, true, false, true, false};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDecodeSingleValue() throws Exception {
        boolean[] values = {true};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDecodeWithNulls() throws Exception {
        boolean[] values = {true, false, false, true, false};
        boolean[] nulls = {false, true, false, true, false};
        assertRoundTrip(values, nulls);
    }

    private static int findBooleanColumnIndex(QwpTableBlockCursor table) {
        for (int c = 0; c < table.getColumnCount(); c++) {
            if (table.getColumnDef(c).getTypeCode() == TYPE_BOOLEAN) {
                return c;
            }
        }
        return -1;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(boolean[] values, boolean[] nulls, boolean useNullBitmap) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_bool");
        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_BOOLEAN, useNullBitmap);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
        for (int i = 0; i < values.length; i++) {
            if (useNullBitmap && nulls[i]) {
                col.addNull();
            } else {
                col.addBoolean(values[i]);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private void assertRoundTrip(boolean[] values, boolean[] nulls) throws Exception {
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
                    int colIdx = findBooleanColumnIndex(table);
                    Assert.assertNotEquals(-1, colIdx);
                    for (int i = 0; i < values.length; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();
                        if (useNullBitmap && nulls[i]) {
                            Assert.assertTrue("Row " + i + " should be null", table.isColumnNull(colIdx));
                        } else {
                            Assert.assertFalse("Row " + i + " should not be null", table.isColumnNull(colIdx));
                            QwpBooleanColumnCursor cursor = table.getBooleanColumn(colIdx);
                            Assert.assertEquals("Row " + i + " value mismatch", values[i], cursor.getValue());
                        }
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }
}
