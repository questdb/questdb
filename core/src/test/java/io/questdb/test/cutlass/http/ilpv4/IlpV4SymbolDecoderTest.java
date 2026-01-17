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

import io.questdb.cutlass.ilpv4.protocol.IlpV4ParseException;
import io.questdb.cutlass.ilpv4.protocol.IlpV4SymbolDecoder;
import io.questdb.cutlass.ilpv4.protocol.IlpV4Varint;
import static io.questdb.cutlass.ilpv4.protocol.IlpV4SymbolDecoder.NULL_SYMBOL_INDEX;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class IlpV4SymbolDecoderTest {

    // ==================== Empty Column Tests ====================

    @Test
    public void testDecodeEmptySymbolColumn() throws IlpV4ParseException {
        IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(0);
        int consumed = IlpV4SymbolDecoder.INSTANCE.decode(0, 0, 0, false, sink);
        Assert.assertEquals(0, consumed);
    }

    // ==================== Single Symbol Tests ====================

    @Test
    public void testDecodeSingleSymbol() throws IlpV4ParseException {
        String[] values = {"symbol_a"};

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4SymbolDecoder.encode(address, values, null);
            int actualSize = (int) (end - address);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(1);
            int consumed = IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, 1, false, sink);

            Assert.assertEquals(actualSize, consumed);
            Assert.assertEquals("symbol_a", sink.getValue(0));
            Assert.assertEquals(0, sink.getIndex(0));
            Assert.assertFalse(sink.isNull(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Multiple Symbols Tests ====================

    @Test
    public void testDecodeMultipleSymbols() throws IlpV4ParseException {
        String[] values = {"apple", "banana", "cherry", "date"};
        int rowCount = values.length;

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4SymbolDecoder.encode(address, values, null);
            int actualSize = (int) (end - address);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(rowCount);
            int consumed = IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, rowCount, false, sink);

            Assert.assertEquals(actualSize, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], sink.getValue(i));
                Assert.assertEquals(i, sink.getIndex(i)); // Each unique value gets its own index
                Assert.assertFalse(sink.isNull(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Dictionary Tests ====================

    @Test
    public void testDictionaryParsing() throws IlpV4ParseException {
        // Values with duplicates should share dictionary entries
        String[] values = {"a", "b", "a", "b", "c", "a"};
        int rowCount = values.length;

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4SymbolDecoder.encode(address, values, null);
            int actualSize = (int) (end - address);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(rowCount);
            int consumed = IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, rowCount, false, sink);

            Assert.assertEquals(actualSize, consumed);
            // Verify values
            Assert.assertEquals("a", sink.getValue(0));
            Assert.assertEquals("b", sink.getValue(1));
            Assert.assertEquals("a", sink.getValue(2));
            Assert.assertEquals("b", sink.getValue(3));
            Assert.assertEquals("c", sink.getValue(4));
            Assert.assertEquals("a", sink.getValue(5));

            // Verify indices - duplicates should have same index
            Assert.assertEquals(0, sink.getIndex(0)); // "a"
            Assert.assertEquals(1, sink.getIndex(1)); // "b"
            Assert.assertEquals(0, sink.getIndex(2)); // "a"
            Assert.assertEquals(1, sink.getIndex(3)); // "b"
            Assert.assertEquals(2, sink.getIndex(4)); // "c"
            Assert.assertEquals(0, sink.getIndex(5)); // "a"
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDictionaryEmpty() throws IlpV4ParseException {
        // Manually create encoding with empty dictionary
        // This can happen if all values are null
        int rowCount = 3;
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // Null bitmap (all nulls)
            int bitmapSize = (rowCount + 7) / 8;
            for (int i = 0; i < bitmapSize; i++) {
                Unsafe.getUnsafe().putByte(pos + i, (byte) 0xFF);
            }
            pos += bitmapSize;

            // Empty dictionary (size = 0)
            pos = IlpV4Varint.encode(pos, 0);

            // Value indices (all null = max varint)
            for (int i = 0; i < rowCount; i++) {
                pos = IlpV4Varint.encode(pos, NULL_SYMBOL_INDEX);
            }

            int actualSize = (int) (pos - address);
            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(rowCount);
            IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, rowCount, true, sink);

            for (int i = 0; i < rowCount; i++) {
                Assert.assertTrue(sink.isNull(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDictionaryLarge() throws IlpV4ParseException {
        // 1000 unique symbols
        int dictSize = 1000;
        String[] values = new String[dictSize];
        for (int i = 0; i < dictSize; i++) {
            values[i] = "symbol_" + i;
        }

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4SymbolDecoder.encode(address, values, null);
            int actualSize = (int) (end - address);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(dictSize);
            int consumed = IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, dictSize, false, sink);

            Assert.assertEquals(actualSize, consumed);
            for (int i = 0; i < dictSize; i++) {
                Assert.assertEquals("symbol_" + i, sink.getValue(i));
                Assert.assertEquals(i, sink.getIndex(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Symbol Index Mapping Tests ====================

    @Test
    public void testSymbolIndexMapping() throws IlpV4ParseException {
        // All same value should reference same dictionary entry
        String[] values = {"same", "same", "same", "same"};
        int rowCount = values.length;

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4SymbolDecoder.encode(address, values, null);
            int actualSize = (int) (end - address);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(rowCount);
            IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, rowCount, false, sink);

            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals("same", sink.getValue(i));
                Assert.assertEquals(0, sink.getIndex(i)); // All should be index 0
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Null Handling Tests ====================

    @Test
    public void testSymbolWithNulls() throws IlpV4ParseException {
        // Using null bitmap
        String[] values = {"a", null, "b", null};
        boolean[] nulls = {false, true, false, true};
        int rowCount = values.length;

        // Replace nulls for encoding
        String[] encodeValues = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            encodeValues[i] = values[i] != null ? values[i] : "";
        }

        int size = IlpV4SymbolDecoder.maxEncodedSize(encodeValues, nulls);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4SymbolDecoder.encode(address, encodeValues, nulls);
            int actualSize = (int) (end - address);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(rowCount);
            IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, rowCount, true, sink);

            Assert.assertEquals("a", sink.getValue(0));
            Assert.assertFalse(sink.isNull(0));
            Assert.assertTrue(sink.isNull(1));
            Assert.assertEquals("b", sink.getValue(2));
            Assert.assertFalse(sink.isNull(2));
            Assert.assertTrue(sink.isNull(3));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSymbolNullIndex() throws IlpV4ParseException {
        // Test null using max varint index (alternative to bitmap)
        String[] values = {"a", "b"};

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Manually encode with null index for second row
            long pos = address;

            // Dictionary: 2 entries
            pos = IlpV4Varint.encode(pos, 2);

            // Entry "a"
            byte[] aBytes = "a".getBytes(StandardCharsets.UTF_8);
            pos = IlpV4Varint.encode(pos, aBytes.length);
            for (byte b : aBytes) {
                Unsafe.getUnsafe().putByte(pos++, b);
            }

            // Entry "b"
            byte[] bBytes = "b".getBytes(StandardCharsets.UTF_8);
            pos = IlpV4Varint.encode(pos, bBytes.length);
            for (byte b : bBytes) {
                Unsafe.getUnsafe().putByte(pos++, b);
            }

            // Values: index 0, null (max varint)
            pos = IlpV4Varint.encode(pos, 0);
            pos = IlpV4Varint.encode(pos, NULL_SYMBOL_INDEX);

            int actualSize = (int) (pos - address);
            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(2);
            IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, 2, false, sink);

            Assert.assertEquals("a", sink.getValue(0));
            Assert.assertFalse(sink.isNull(0));
            Assert.assertTrue(sink.isNull(1));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Repeated Values Tests ====================

    @Test
    public void testSymbolRepeatedValues() throws IlpV4ParseException {
        // High cardinality with many repeats
        String[] symbols = {"low", "medium", "high"};
        int rowCount = 1000;
        String[] values = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = symbols[i % 3];
        }

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4SymbolDecoder.encode(address, values, null);
            int actualSize = (int) (end - address);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(rowCount);
            int consumed = IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, rowCount, false, sink);

            Assert.assertEquals(actualSize, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(symbols[i % 3], sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== UTF-8 Tests ====================

    @Test
    public void testSymbolUtf8() throws IlpV4ParseException {
        String[] values = {"日本語", "中文", "한국어", "Ελληνικά"};
        int rowCount = values.length;

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4SymbolDecoder.encode(address, values, null);
            int actualSize = (int) (end - address);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(rowCount);
            IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, rowCount, false, sink);

            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testInvalidDictionaryIndex() {
        // Create encoding where index exceeds dictionary size
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // Dictionary: 1 entry
            pos = IlpV4Varint.encode(pos, 1);

            // Entry "a"
            byte[] aBytes = "a".getBytes(StandardCharsets.UTF_8);
            pos = IlpV4Varint.encode(pos, aBytes.length);
            for (byte b : aBytes) {
                Unsafe.getUnsafe().putByte(pos++, b);
            }

            // Value: index 5 (invalid, only 1 entry)
            pos = IlpV4Varint.encode(pos, 5);

            int actualSize = (int) (pos - address);
            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(1);
            IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, 1, false, sink);
            Assert.fail("Expected exception");
        } catch (IlpV4ParseException e) {
            Assert.assertEquals(IlpV4ParseException.ErrorCode.INVALID_DICTIONARY_INDEX, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("index out of bounds"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInsufficientDataForDictionary() {
        // Create truncated data
        int size = 5;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;
            // Dictionary size = 1
            pos = IlpV4Varint.encode(pos, 1);
            // String length = 100 (but we don't have that much data)
            IlpV4Varint.encode(pos, 100);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(1);
            IlpV4SymbolDecoder.INSTANCE.decode(address, size, 1, false, sink);
            Assert.fail("Expected exception");
        } catch (IlpV4ParseException e) {
            Assert.assertEquals(IlpV4ParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Large Column Tests ====================

    @Test
    public void testSymbolLargeColumn() throws IlpV4ParseException {
        // 10000 rows with 100 unique symbols
        int rowCount = 10000;
        int uniqueSymbols = 100;
        String[] values = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = "sym_" + (i % uniqueSymbols);
        }

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4SymbolDecoder.encode(address, values, null);
            int actualSize = (int) (end - address);

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(rowCount);
            int consumed = IlpV4SymbolDecoder.INSTANCE.decode(address, actualSize, rowCount, false, sink);

            Assert.assertEquals(actualSize, consumed);
            // Verify some values
            Assert.assertEquals("sym_0", sink.getValue(0));
            Assert.assertEquals("sym_50", sink.getValue(50));
            Assert.assertEquals("sym_99", sink.getValue(99));
            Assert.assertEquals("sym_0", sink.getValue(100));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Byte Array Encoding Tests ====================

    @Test
    public void testEncodeToByteArray() throws IlpV4ParseException {
        String[] values = {"foo", "bar", "foo"};
        int rowCount = values.length;

        int size = IlpV4SymbolDecoder.maxEncodedSize(values, null);
        byte[] buf = new byte[size];

        int offset = IlpV4SymbolDecoder.encode(buf, 0, values, null);
        Assert.assertTrue(offset <= size);

        // Decode from byte array by copying to direct memory
        long address = Unsafe.malloc(offset, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < offset; i++) {
                Unsafe.getUnsafe().putByte(address + i, buf[i]);
            }

            IlpV4SymbolDecoder.ArraySymbolSink sink = new IlpV4SymbolDecoder.ArraySymbolSink(rowCount);
            IlpV4SymbolDecoder.INSTANCE.decode(address, offset, rowCount, false, sink);

            Assert.assertEquals("foo", sink.getValue(0));
            Assert.assertEquals("bar", sink.getValue(1));
            Assert.assertEquals("foo", sink.getValue(2));
            Assert.assertEquals(sink.getIndex(0), sink.getIndex(2)); // Same index for "foo"
        } finally {
            Unsafe.free(address, offset, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
