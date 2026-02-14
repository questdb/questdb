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

import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpStringDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class QwpStringDecoderTest {

    // ==================== Empty Column Tests ====================

    @Test
    public void testDecodeEmptyStringColumn() throws QwpParseException {
        QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(0);
        int consumed = QwpStringDecoder.INSTANCE.decode(0, 0, 0, false, sink);
        Assert.assertEquals(0, consumed);
    }

    // ==================== Single String Tests ====================

    @Test
    public void testDecodeSingleString() throws QwpParseException {
        String[] values = {"hello"};

        int size = QwpStringDecoder.encodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = QwpStringDecoder.encode(address, values, null);
            Assert.assertEquals(size, end - address);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(1);
            int consumed = QwpStringDecoder.INSTANCE.decode(address, size, 1, false, sink);

            Assert.assertEquals(size, consumed);
            Assert.assertEquals("hello", sink.getValue(0));
            Assert.assertFalse(sink.isNull(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Multiple Strings Tests ====================

    @Test
    public void testDecodeMultipleStrings() throws QwpParseException {
        String[] values = {"hello", "world", "test", "strings"};
        int rowCount = values.length;

        int size = QwpStringDecoder.encodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, values, null);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            int consumed = QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], sink.getValue(i));
                Assert.assertFalse(sink.isNull(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Empty Strings Tests ====================

    @Test
    public void testDecodeEmptyStrings() throws QwpParseException {
        String[] values = {"", "nonempty", "", ""};
        int rowCount = values.length;

        int size = QwpStringDecoder.encodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, values, null);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            int consumed = QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            Assert.assertEquals("", sink.getValue(0));
            Assert.assertEquals("nonempty", sink.getValue(1));
            Assert.assertEquals("", sink.getValue(2));
            Assert.assertEquals("", sink.getValue(3));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeAllEmptyStrings() throws QwpParseException {
        String[] values = {"", "", ""};
        int rowCount = values.length;

        int size = QwpStringDecoder.encodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, values, null);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            int consumed = QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals("", sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== UTF-8 Tests ====================

    @Test
    public void testDecodeUtf8Strings() throws QwpParseException {
        String[] values = {"Hello 世界", "Привет мир", "こんにちは", "🎉🎊"};
        int rowCount = values.length;

        int size = QwpStringDecoder.encodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, values, null);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            int consumed = QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeUtf8MultiByte() throws QwpParseException {
        // Test various UTF-8 byte lengths: 1, 2, 3, 4 bytes per character
        String[] values = {
                "A",           // 1 byte
                "é",           // 2 bytes
                "€",           // 3 bytes
                "𝄞"            // 4 bytes (musical G clef)
        };
        int rowCount = values.length;

        int size = QwpStringDecoder.encodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, values, null);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Null Handling Tests ====================

    @Test
    public void testDecodeWithNulls() throws QwpParseException {
        String[] values = {"hello", null, "world", null};
        boolean[] nulls = {false, true, false, true};
        int rowCount = values.length;

        // Replace nulls with empty strings for encoding
        String[] encodeValues = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            encodeValues[i] = values[i] != null ? values[i] : "";
        }

        int size = QwpStringDecoder.encodedSize(encodeValues, nulls);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, encodeValues, nulls);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            int consumed = QwpStringDecoder.INSTANCE.decode(address, size, rowCount, true, sink);

            Assert.assertEquals(size, consumed);
            Assert.assertEquals("hello", sink.getValue(0));
            Assert.assertFalse(sink.isNull(0));
            Assert.assertTrue(sink.isNull(1));
            Assert.assertEquals("world", sink.getValue(2));
            Assert.assertFalse(sink.isNull(2));
            Assert.assertTrue(sink.isNull(3));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeAllNulls() throws QwpParseException {
        String[] values = {"", "", ""};
        boolean[] nulls = {true, true, true};
        int rowCount = values.length;

        int size = QwpStringDecoder.encodedSize(values, nulls);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, values, nulls);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            QwpStringDecoder.INSTANCE.decode(address, size, rowCount, true, sink);

            for (int i = 0; i < rowCount; i++) {
                Assert.assertTrue(sink.isNull(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Offset Validation Tests ====================

    @Test
    public void testOffsetArrayFirstOffsetMustBeZero() {
        // Manually create invalid data with non-zero first offset
        int rowCount = 2;
        int offsetArraySize = (rowCount + 1) * 4;
        int size = offsetArraySize; // no string data
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Write invalid first offset (non-zero)
            Unsafe.getUnsafe().putInt(address, 5); // Should be 0
            Unsafe.getUnsafe().putInt(address + 4, 5);
            Unsafe.getUnsafe().putInt(address + 8, 5);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);
            Assert.fail("Expected exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("first offset"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testOffsetArrayOutOfBounds() {
        // Create data where last offset exceeds available data
        int rowCount = 1;
        int offsetArraySize = (rowCount + 1) * 4;
        int size = offsetArraySize + 5; // Only 5 bytes of string data
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Offsets: 0, 100 (but we only have 5 bytes)
            Unsafe.getUnsafe().putInt(address, 0);
            Unsafe.getUnsafe().putInt(address + 4, 100);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);
            Assert.fail("Expected exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNonMonotonicOffsets() {
        // Create data where offsets go backwards
        int rowCount = 2;
        int offsetArraySize = (rowCount + 1) * 4;
        int size = offsetArraySize + 10;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Offsets: 0, 10, 5 (non-monotonic)
            Unsafe.getUnsafe().putInt(address, 0);
            Unsafe.getUnsafe().putInt(address + 4, 10);
            Unsafe.getUnsafe().putInt(address + 8, 5); // Goes backward

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);
            Assert.fail("Expected exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("non-monotonic"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Large Column Tests ====================

    @Test
    public void testStringLargeColumn() throws QwpParseException {
        int rowCount = 10000;
        String[] values = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = "string_" + i;
        }

        int size = QwpStringDecoder.encodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, values, null);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            int consumed = QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            // Verify some values
            Assert.assertEquals("string_0", sink.getValue(0));
            Assert.assertEquals("string_5000", sink.getValue(5000));
            Assert.assertEquals("string_9999", sink.getValue(9999));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStringMaxLength() throws QwpParseException {
        // Test with a moderately long string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append("x");
        }
        String[] values = {sb.toString()};

        int size = QwpStringDecoder.encodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, values, null);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(1);
            int consumed = QwpStringDecoder.INSTANCE.decode(address, size, 1, false, sink);

            Assert.assertEquals(size, consumed);
            Assert.assertEquals(10000, sink.getValue(0).length());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== VARCHAR Same As String Test ====================

    @Test
    public void testDecodeVarcharSameAsString() throws QwpParseException {
        // VARCHAR uses the same format as STRING
        String[] values = {"varchar", "test", "values"};
        int rowCount = values.length;

        int size = QwpStringDecoder.encodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.encode(address, values, null);

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Byte Array Encoding Test ====================

    @Test
    public void testEncodeToByteArray() throws QwpParseException {
        String[] values = {"hello", "world"};
        int rowCount = values.length;

        int size = QwpStringDecoder.encodedSize(values, null);
        byte[] buf = new byte[size];

        int offset = QwpStringDecoder.encode(buf, 0, values, null);
        Assert.assertEquals(size, offset);

        // Decode from byte array by copying to direct memory
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(address + i, buf[i]);
            }

            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals("hello", sink.getValue(0));
            Assert.assertEquals("world", sink.getValue(1));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testInsufficientDataForOffsetArray() {
        int rowCount = 10;
        int size = 5; // Not enough for offset array (needs 44 bytes)
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            QwpStringDecoder.INSTANCE.decode(address, size, rowCount, false, sink);
            Assert.fail("Expected exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInsufficientDataForNullBitmap() {
        int rowCount = 100;
        int size = 5; // Not enough for null bitmap (needs 13 bytes)
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpStringDecoder.ArrayStringSink sink = new QwpStringDecoder.ArrayStringSink(rowCount);
            QwpStringDecoder.INSTANCE.decode(address, size, rowCount, true, sink);
            Assert.fail("Expected exception");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
