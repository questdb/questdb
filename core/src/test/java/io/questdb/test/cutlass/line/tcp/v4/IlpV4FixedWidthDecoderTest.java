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

package io.questdb.test.cutlass.line.tcp.v4;

import io.questdb.cutlass.line.tcp.v4.IlpV4ColumnDecoder;
import io.questdb.cutlass.line.tcp.v4.IlpV4FixedWidthDecoder;
import io.questdb.cutlass.line.tcp.v4.IlpV4ParseException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

public class IlpV4FixedWidthDecoderTest {

    // ==================== Byte Column Tests ====================

    @Test
    public void testDecodeByteColumn() throws IlpV4ParseException {
        byte[] values = {1, 2, 3, -128, 127, 0};
        int rowCount = values.length;

        int size = rowCount; // 1 byte per value
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encode
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putByte(address + i, values[i]);
            }

            // Decode
            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_BYTE);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], (byte) sink.getValue(i));
                Assert.assertFalse(sink.isNull(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeByteColumnWithNulls() throws IlpV4ParseException {
        byte[] values = {1, 0, 3, 0, 5};
        boolean[] nulls = {false, true, false, true, false};
        int rowCount = values.length;

        long[] longValues = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            longValues[i] = values[i];
        }

        // Count non-null values
        int nullCount = 0;
        for (boolean isNull : nulls) {
            if (isNull) nullCount++;
        }
        int valueCount = rowCount - nullCount;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize + valueCount; // Only non-null values take space
        int bufferSize = bitmapSize + rowCount; // Allocate max possible
        long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = IlpV4FixedWidthDecoder.encode(address, longValues, nulls, TYPE_BYTE);
            Assert.assertEquals(size, end - address);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_BYTE);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, bufferSize, rowCount, true, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                if (nulls[i]) {
                    Assert.assertTrue(sink.isNull(i));
                } else {
                    Assert.assertEquals(values[i], (byte) sink.getValue(i));
                    Assert.assertFalse(sink.isNull(i));
                }
            }
        } finally {
            Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Short Column Tests ====================

    @Test
    public void testDecodeShortColumn() throws IlpV4ParseException {
        short[] values = {1, 256, -1, Short.MAX_VALUE, Short.MIN_VALUE, 0};
        int rowCount = values.length;

        int size = rowCount * 2;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putShort(address + i * 2, values[i]);
            }

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_SHORT);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], (short) sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeShortColumnWithNulls() throws IlpV4ParseException {
        short[] values = {100, 0, 300};
        boolean[] nulls = {false, true, false};
        int rowCount = values.length;

        long[] longValues = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            longValues[i] = values[i];
        }

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize + rowCount * 2;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encode(address, longValues, nulls, TYPE_SHORT);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_SHORT);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            decoder.decode(address, size, rowCount, true, sink);

            Assert.assertEquals(values[0], (short) sink.getValue(0));
            Assert.assertTrue(sink.isNull(1));
            Assert.assertEquals(values[2], (short) sink.getValue(2));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Int Column Tests ====================

    @Test
    public void testDecodeIntColumn() throws IlpV4ParseException {
        int[] values = {1, 65536, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 0};
        int rowCount = values.length;

        int size = rowCount * 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putInt(address + i * 4, values[i]);
            }

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_INT);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], (int) sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeIntColumnWithNulls() throws IlpV4ParseException {
        int[] values = {100, 0, 300, 0};
        boolean[] nulls = {false, true, false, true};
        int rowCount = values.length;

        long[] longValues = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            longValues[i] = values[i];
        }

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize + rowCount * 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encode(address, longValues, nulls, TYPE_INT);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_INT);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            decoder.decode(address, size, rowCount, true, sink);

            Assert.assertEquals(100, (int) sink.getValue(0));
            Assert.assertTrue(sink.isNull(1));
            Assert.assertEquals(300, (int) sink.getValue(2));
            Assert.assertTrue(sink.isNull(3));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Long Column Tests ====================

    @Test
    public void testDecodeLongColumn() throws IlpV4ParseException {
        long[] values = {1L, 4294967296L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 0L};
        int rowCount = values.length;

        int size = rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putLong(address + i * 8, values[i]);
            }

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_LONG);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], (long) sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeLongColumnWithNulls() throws IlpV4ParseException {
        long[] values = {100L, 0L, 300L};
        boolean[] nulls = {false, true, false};
        int rowCount = values.length;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize + rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encode(address, values, nulls, TYPE_LONG);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_LONG);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            decoder.decode(address, size, rowCount, true, sink);

            Assert.assertEquals(100L, (long) sink.getValue(0));
            Assert.assertTrue(sink.isNull(1));
            Assert.assertEquals(300L, (long) sink.getValue(2));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Float Column Tests ====================

    @Test
    public void testDecodeFloatColumn() throws IlpV4ParseException {
        float[] values = {1.5f, -2.5f, 0.0f, Float.MAX_VALUE, Float.MIN_VALUE};
        int rowCount = values.length;

        int size = rowCount * 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = IlpV4FixedWidthDecoder.encodeFloats(address, values, null);
            Assert.assertEquals(size, pos - address);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_FLOAT);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], (float) sink.getValue(i), 0.0001f);
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeFloatColumnSpecialValues() throws IlpV4ParseException {
        float[] values = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY};
        int rowCount = values.length;

        int size = rowCount * 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encodeFloats(address, values, null);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_FLOAT);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            decoder.decode(address, size, rowCount, false, sink);

            Assert.assertTrue(Float.isNaN((float) sink.getValue(0)));
            Assert.assertEquals(Float.POSITIVE_INFINITY, (float) sink.getValue(1), 0.0f);
            Assert.assertEquals(Float.NEGATIVE_INFINITY, (float) sink.getValue(2), 0.0f);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeFloatColumnWithNulls() throws IlpV4ParseException {
        float[] values = {1.5f, 0.0f, 3.5f};
        boolean[] nulls = {false, true, false};
        int rowCount = values.length;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize + rowCount * 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encodeFloats(address, values, nulls);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_FLOAT);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            decoder.decode(address, size, rowCount, true, sink);

            Assert.assertEquals(1.5f, (float) sink.getValue(0), 0.0001f);
            Assert.assertTrue(sink.isNull(1));
            Assert.assertEquals(3.5f, (float) sink.getValue(2), 0.0001f);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Double Column Tests ====================

    @Test
    public void testDecodeDoubleColumn() throws IlpV4ParseException {
        double[] values = {1.5, -2.5, 0.0, Double.MAX_VALUE, Double.MIN_VALUE};
        int rowCount = values.length;

        int size = rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = IlpV4FixedWidthDecoder.encodeDoubles(address, values, null);
            Assert.assertEquals(size, pos - address);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_DOUBLE);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], (double) sink.getValue(i), 0.0001);
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeDoubleColumnSpecialValues() throws IlpV4ParseException {
        double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
        int rowCount = values.length;

        int size = rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encodeDoubles(address, values, null);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_DOUBLE);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            decoder.decode(address, size, rowCount, false, sink);

            Assert.assertTrue(Double.isNaN((double) sink.getValue(0)));
            Assert.assertEquals(Double.POSITIVE_INFINITY, (double) sink.getValue(1), 0.0);
            Assert.assertEquals(Double.NEGATIVE_INFINITY, (double) sink.getValue(2), 0.0);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeDoubleColumnWithNulls() throws IlpV4ParseException {
        double[] values = {1.5, 0.0, 3.5};
        boolean[] nulls = {false, true, false};
        int rowCount = values.length;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize + rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encodeDoubles(address, values, nulls);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_DOUBLE);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            decoder.decode(address, size, rowCount, true, sink);

            Assert.assertEquals(1.5, (double) sink.getValue(0), 0.0001);
            Assert.assertTrue(sink.isNull(1));
            Assert.assertEquals(3.5, (double) sink.getValue(2), 0.0001);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Date/Timestamp Column Tests ====================

    @Test
    public void testDecodeDateColumn() throws IlpV4ParseException {
        long[] values = {0L, 86400000L, 1609459200000L, -86400000L}; // epoch, 1 day, 2021-01-01, -1 day
        int rowCount = values.length;

        int size = rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encode(address, values, null, TYPE_DATE);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_DATE);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], (long) sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeTimestampColumn() throws IlpV4ParseException {
        long[] values = {0L, 1000000000L, 1609459200000000L}; // microseconds
        int rowCount = values.length;

        int size = rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encode(address, values, null, TYPE_TIMESTAMP);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_TIMESTAMP);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(values[i], (long) sink.getValue(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== UUID Column Tests ====================

    @Test
    public void testDecodeUuidColumn() throws IlpV4ParseException {
        // UUIDs are 16 bytes each, big-endian
        long[] hiValues = {0x0123456789ABCDEFL, 0xFEDCBA9876543210L, 0L};
        long[] loValues = {0xFEDCBA9876543210L, 0x0123456789ABCDEFL, 0L};
        int rowCount = hiValues.length;

        int size = rowCount * 16;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encodeUuids(address, hiValues, loValues, null);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_UUID);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                long[] uuid = (long[]) sink.getValue(i);
                Assert.assertEquals(hiValues[i], uuid[0]);
                Assert.assertEquals(loValues[i], uuid[1]);
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeUuidColumnWithNulls() throws IlpV4ParseException {
        long[] hiValues = {0x0123456789ABCDEFL, 0L, 0xABCDEF0123456789L};
        long[] loValues = {0xFEDCBA9876543210L, 0L, 0x9876543210ABCDEFL};
        boolean[] nulls = {false, true, false};
        int rowCount = hiValues.length;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize + rowCount * 16;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4FixedWidthDecoder.encodeUuids(address, hiValues, loValues, nulls);

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_UUID);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            decoder.decode(address, size, rowCount, true, sink);

            long[] uuid0 = (long[]) sink.getValue(0);
            Assert.assertEquals(hiValues[0], uuid0[0]);
            Assert.assertEquals(loValues[0], uuid0[1]);

            Assert.assertTrue(sink.isNull(1));

            long[] uuid2 = (long[]) sink.getValue(2);
            Assert.assertEquals(hiValues[2], uuid2[0]);
            Assert.assertEquals(loValues[2], uuid2[1]);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== LONG256 Column Tests ====================

    @Test
    public void testDecodeLong256Column() throws IlpV4ParseException {
        // LONG256 is 32 bytes each, big-endian
        int rowCount = 2;
        int size = rowCount * 32;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encode manually: big-endian
            // Row 0: 0x0123...FEDC...
            Unsafe.getUnsafe().putLong(address, Long.reverseBytes(0x0123456789ABCDEFL));
            Unsafe.getUnsafe().putLong(address + 8, Long.reverseBytes(0xFEDCBA9876543210L));
            Unsafe.getUnsafe().putLong(address + 16, Long.reverseBytes(0x1111111122222222L));
            Unsafe.getUnsafe().putLong(address + 24, Long.reverseBytes(0x3333333344444444L));

            // Row 1: all zeros
            for (int i = 0; i < 32; i++) {
                Unsafe.getUnsafe().putByte(address + 32 + i, (byte) 0);
            }

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_LONG256);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);

            long[] v0 = (long[]) sink.getValue(0);
            Assert.assertEquals(0x0123456789ABCDEFL, v0[0]);
            Assert.assertEquals(0xFEDCBA9876543210L, v0[1]);
            Assert.assertEquals(0x1111111122222222L, v0[2]);
            Assert.assertEquals(0x3333333344444444L, v0[3]);

            long[] v1 = (long[]) sink.getValue(1);
            Assert.assertEquals(0L, v1[0]);
            Assert.assertEquals(0L, v1[1]);
            Assert.assertEquals(0L, v1[2]);
            Assert.assertEquals(0L, v1[3]);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Empty Column Tests ====================

    @Test
    public void testDecodeEmptyColumn() throws IlpV4ParseException {
        IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_LONG);
        IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(0);

        int consumed = decoder.decode(0, 0, 0, false, sink);
        Assert.assertEquals(0, consumed);
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testDecodeInsufficientDataForNullBitmap() {
        IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_LONG);
        IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(10);

        // 10 rows need 2 bytes for null bitmap, but we only provide 1
        long address = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            decoder.decode(address, 1, 10, true, sink);
            Assert.fail("Expected exception");
        } catch (IlpV4ParseException e) {
            Assert.assertEquals(IlpV4ParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        } finally {
            Unsafe.free(address, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForValues() {
        IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_LONG);
        IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(10);

        // 10 long values need 80 bytes, we provide less
        int size = 40;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            decoder.decode(address, size, 10, false, sink);
            Assert.fail("Expected exception");
        } catch (IlpV4ParseException e) {
            Assert.assertEquals(IlpV4ParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTypeForBooleanDecoder() {
        // Boolean should use IlpV4BooleanDecoder
        new IlpV4FixedWidthDecoder(TYPE_BOOLEAN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTypeForVariableWidth() {
        // Variable-width types not supported
        new IlpV4FixedWidthDecoder(TYPE_STRING);
    }

    // ==================== Large Column Tests ====================

    @Test
    public void testDecodeLargeColumn() throws IlpV4ParseException {
        int rowCount = 100000;
        int size = rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Fill with sequential values
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putLong(address + (long) i * 8, i);
            }

            IlpV4FixedWidthDecoder decoder = new IlpV4FixedWidthDecoder(TYPE_LONG);
            IlpV4ColumnDecoder.ArrayColumnSink sink = new IlpV4ColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = decoder.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);

            // Verify some values
            Assert.assertEquals(0L, (long) sink.getValue(0));
            Assert.assertEquals(50000L, (long) sink.getValue(50000));
            Assert.assertEquals(99999L, (long) sink.getValue(99999));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Expected Size Tests ====================

    @Test
    public void testExpectedSize() {
        IlpV4FixedWidthDecoder byteDecoder = new IlpV4FixedWidthDecoder(TYPE_BYTE);
        Assert.assertEquals(10, byteDecoder.expectedSize(10, false));
        Assert.assertEquals(12, byteDecoder.expectedSize(10, true)); // 2 byte bitmap + 10

        IlpV4FixedWidthDecoder longDecoder = new IlpV4FixedWidthDecoder(TYPE_LONG);
        Assert.assertEquals(80, longDecoder.expectedSize(10, false));
        Assert.assertEquals(82, longDecoder.expectedSize(10, true)); // 2 byte bitmap + 80

        IlpV4FixedWidthDecoder uuidDecoder = new IlpV4FixedWidthDecoder(TYPE_UUID);
        Assert.assertEquals(160, uuidDecoder.expectedSize(10, false));
        Assert.assertEquals(162, uuidDecoder.expectedSize(10, true));

        IlpV4FixedWidthDecoder long256Decoder = new IlpV4FixedWidthDecoder(TYPE_LONG256);
        Assert.assertEquals(320, long256Decoder.expectedSize(10, false));
        Assert.assertEquals(322, long256Decoder.expectedSize(10, true));
    }
}
