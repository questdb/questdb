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

package io.questdb.test.cutlass.http.qwp;

import io.questdb.cutlass.qwp.protocol.QwpBooleanDecoder;
import io.questdb.cutlass.qwp.protocol.QwpColumnDecoder;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class QwpBooleanDecoderTest {

    // ==================== Basic Boolean Tests ====================

    @Test
    public void testDecodeAllTrue() throws QwpParseException {
        boolean[] values = {true, true, true, true, true, true, true, true};
        int rowCount = values.length;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize; // non-nullable
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, null);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(true, sink.getValue(i));
                Assert.assertFalse(sink.isNull(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeAllFalse() throws QwpParseException {
        boolean[] values = {false, false, false, false, false, false, false, false};
        int rowCount = values.length;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, null);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(false, sink.getValue(i));
                Assert.assertFalse(sink.isNull(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeMixedValues() throws QwpParseException {
        boolean[] values = {true, false, true, false, true, false, true, false};
        int rowCount = values.length;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, null);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Nullable Boolean Tests ====================

    @Test
    public void testDecodeWithNulls() throws QwpParseException {
        boolean[] values = {true, false, false, true, false};
        boolean[] nulls = {false, true, false, true, false};
        int rowCount = values.length;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize * 2; // null bitmap + value bitmap
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, nulls);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, true, sink);

            Assert.assertEquals(size, consumed);

            Assert.assertEquals(true, sink.getValue(0));
            Assert.assertFalse(sink.isNull(0));

            Assert.assertTrue(sink.isNull(1));

            Assert.assertEquals(false, sink.getValue(2));
            Assert.assertFalse(sink.isNull(2));

            Assert.assertTrue(sink.isNull(3));

            Assert.assertEquals(false, sink.getValue(4));
            Assert.assertFalse(sink.isNull(4));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeAllNulls() throws QwpParseException {
        boolean[] values = {false, false, false, false};
        boolean[] nulls = {true, true, true, true};
        int rowCount = values.length;

        int nullBitmapSize = (rowCount + 7) / 8;
        int valueBitmapSize = 0; // No non-null values, so 0 bytes for value bits
        int size = nullBitmapSize + valueBitmapSize;
        int bufferSize = nullBitmapSize * 2; // Allocate max possible
        long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, nulls);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpBooleanDecoder.INSTANCE.decode(address, bufferSize, rowCount, true, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertTrue(sink.isNull(i));
            }
        } finally {
            Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Partial Byte Tests ====================

    @Test
    public void testDecodePartialByte() throws QwpParseException {
        // 10 values = 2 bytes, only 2 bits used in second byte
        boolean[] values = {true, false, true, false, true, false, true, false, true, false};
        int rowCount = values.length;

        int bitmapSize = (rowCount + 7) / 8;
        Assert.assertEquals(2, bitmapSize);

        int size = bitmapSize;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, null);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeSingleValue() throws QwpParseException {
        boolean[] values = {true};
        int rowCount = 1;

        int bitmapSize = 1;
        int size = bitmapSize;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, null);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            Assert.assertEquals(true, sink.getValue(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Empty Column Tests ====================

    @Test
    public void testDecodeEmptyColumn() throws QwpParseException {
        QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(0);
        int consumed = QwpBooleanDecoder.INSTANCE.decode(0, 0, 0, false, sink);
        Assert.assertEquals(0, consumed);
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testDecodeInsufficientDataForNullBitmap() {
        QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(10);

        // 10 rows need 2 bytes for null bitmap, but we only provide 1
        long address = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.INSTANCE.decode(address, 1, 10, true, sink);
            Assert.fail("Expected exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        } finally {
            Unsafe.free(address, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForValues() {
        QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(16);

        // 16 rows need 2 bytes for value bits, we only provide 1
        long address = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.INSTANCE.decode(address, 1, 16, false, sink);
            Assert.fail("Expected exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        } finally {
            Unsafe.free(address, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Byte Array Encoding Tests ====================

    @Test
    public void testEncodeToByteArray() throws QwpParseException {
        boolean[] values = {true, false, true, true, false, false, true, false};
        boolean[] nulls = {false, true, false, false, true, false, false, false};
        int rowCount = values.length;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize * 2;
        byte[] buf = new byte[size];

        int offset = QwpBooleanDecoder.encode(buf, 0, values, nulls);
        Assert.assertEquals(size, offset);

        // Decode from byte array by copying to direct memory
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(address + i, buf[i]);
            }

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, true, sink);

            Assert.assertEquals(true, sink.getValue(0));
            Assert.assertTrue(sink.isNull(1));
            Assert.assertEquals(true, sink.getValue(2));
            Assert.assertEquals(true, sink.getValue(3));
            Assert.assertTrue(sink.isNull(4));
            Assert.assertEquals(false, sink.getValue(5));
            Assert.assertEquals(true, sink.getValue(6));
            Assert.assertEquals(false, sink.getValue(7));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Large Column Tests ====================

    @Test
    public void testDecodeLargeColumn() throws QwpParseException {
        int rowCount = 100000;
        boolean[] values = new boolean[rowCount];

        // Pattern: every 3rd value is true
        for (int i = 0; i < rowCount; i++) {
            values[i] = (i % 3) == 0;
        }

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, null);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);

            // Verify pattern
            Assert.assertEquals(true, sink.getValue(0));
            Assert.assertEquals(false, sink.getValue(1));
            Assert.assertEquals(false, sink.getValue(2));
            Assert.assertEquals(true, sink.getValue(3));
            Assert.assertEquals(true, sink.getValue(99999)); // 99999 % 3 == 0
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Expected Size Tests ====================

    @Test
    public void testExpectedSize() {
        // Non-nullable: just value bitmap
        Assert.assertEquals(1, QwpBooleanDecoder.INSTANCE.expectedSize(1, false));
        Assert.assertEquals(1, QwpBooleanDecoder.INSTANCE.expectedSize(8, false));
        Assert.assertEquals(2, QwpBooleanDecoder.INSTANCE.expectedSize(9, false));
        Assert.assertEquals(2, QwpBooleanDecoder.INSTANCE.expectedSize(16, false));
        Assert.assertEquals(125, QwpBooleanDecoder.INSTANCE.expectedSize(1000, false));

        // Nullable: null bitmap + value bitmap
        Assert.assertEquals(2, QwpBooleanDecoder.INSTANCE.expectedSize(1, true));
        Assert.assertEquals(2, QwpBooleanDecoder.INSTANCE.expectedSize(8, true));
        Assert.assertEquals(4, QwpBooleanDecoder.INSTANCE.expectedSize(9, true));
        Assert.assertEquals(4, QwpBooleanDecoder.INSTANCE.expectedSize(16, true));
        Assert.assertEquals(250, QwpBooleanDecoder.INSTANCE.expectedSize(1000, true));
    }

    // ==================== Bit Order Tests ====================

    @Test
    public void testBitOrderLsbFirst() throws QwpParseException {
        // Verify LSB-first bit ordering
        boolean[] values = {true, false, false, false, false, false, false, false}; // Only bit 0 set
        int rowCount = values.length;

        int size = 1;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, null);

            // Raw byte should be 0b00000001
            byte b = Unsafe.getUnsafe().getByte(address);
            Assert.assertEquals(0b00000001, b & 0xFF);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(true, sink.getValue(0));
            for (int i = 1; i < 8; i++) {
                Assert.assertEquals(false, sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBitOrderMsbOfByte() throws QwpParseException {
        // Set bit 7 (MSB of first byte)
        boolean[] values = {false, false, false, false, false, false, false, true};
        int rowCount = values.length;

        int size = 1;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpBooleanDecoder.encode(address, values, null);

            // Raw byte should be 0b10000000
            byte b = Unsafe.getUnsafe().getByte(address);
            Assert.assertEquals(0b10000000, b & 0xFF);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            QwpBooleanDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            for (int i = 0; i < 7; i++) {
                Assert.assertEquals(false, sink.getValue(i));
            }
            Assert.assertEquals(true, sink.getValue(7));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
