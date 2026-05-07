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

import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_DOUBLE_ARRAY;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_LONG_ARRAY;

public class QwpArrayColumnCursorTest {

    @Test
    public void testAdvanceRowReturnsNullForNullRow() throws QwpParseException {
        // 2 rows: row 0 is null, row 1 has a 1D double array [1.0, 2.0]
        // Layout: flag(1) + bitmap(1) + nDims(1) + dim(4) + values(16) = 23 bytes
        int bufferSize = 23;
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            long p = addr;
            Unsafe.putByte(p++, (byte) 1);      // null bitmap flag
            Unsafe.putByte(p++, (byte) 0x01);    // bitmap: row 0 is null
            Unsafe.putByte(p++, (byte) 1);       // row 1: nDims=1
            Unsafe.putInt(p, 2);                 // row 1: dim[0]=2
            p += 4;
            Unsafe.putDouble(p, 1.0);            // row 1: value[0]
            p += 8;
            Unsafe.putDouble(p, 2.0);            // row 1: value[1]

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 2, TYPE_DOUBLE_ARRAY);

            // Row 0: null
            Assert.assertTrue(cursor.advanceRow());
            Assert.assertTrue(cursor.isNull());
            Assert.assertEquals(0, cursor.getNDims());
            Assert.assertEquals(0, cursor.getTotalElements());
            Assert.assertEquals(0, cursor.getValuesAddress());

            // Row 1: non-null [1.0, 2.0]
            Assert.assertFalse(cursor.advanceRow());
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(1, cursor.getNDims());
            Assert.assertEquals(2, cursor.getDimSize(0));
            Assert.assertEquals(2, cursor.getTotalElements());
            Assert.assertNotEquals(0, cursor.getValuesAddress());
            Assert.assertEquals(1.0, Unsafe.getDouble(cursor.getValuesAddress()), 0.0);
            Assert.assertEquals(2.0, Unsafe.getDouble(cursor.getValuesAddress() + 8), 0.0);
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEnsureRowCapacityGrows() throws QwpParseException {
        // 65 rows to exceed INITIAL_ROW_CAPACITY of 64
        int rowCount = 65;
        int perRow = 1 + 4 + 8; // nDims(1) + dim(4) + value(8)
        int bufferSize = 1 + rowCount * perRow;
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            long p = addr;
            Unsafe.putByte(p++, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putByte(p++, (byte) 1);   // nDims=1
                Unsafe.putInt(p, 1);              // dim[0]=1
                p += 4;
                Unsafe.putDouble(p, i);  // value
                p += 8;
            }

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, rowCount, TYPE_DOUBLE_ARRAY);

            // Advance through all rows and verify each one
            for (int i = 0; i < rowCount; i++) {
                Assert.assertFalse("Row " + i + " should not be null", cursor.advanceRow());
                Assert.assertFalse(cursor.isNull());
                Assert.assertEquals(1, cursor.getNDims());
                Assert.assertEquals(1, cursor.getTotalElements());
                Assert.assertEquals(i, Unsafe.getDouble(cursor.getValuesAddress()), 0.0);
            }
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testGetTypeCode() throws QwpParseException {
        // 1 row, 1D, 1 element
        int bufferSize = 14; // flag(1) + nDims(1) + dim(4) + value(8)
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(addr, (byte) 0);
            Unsafe.putByte(addr + 1, (byte) 1);
            Unsafe.putInt(addr + 2, 1);
            Unsafe.putDouble(addr + 6, 42.0);

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 1, TYPE_DOUBLE_ARRAY);
            Assert.assertEquals(TYPE_DOUBLE_ARRAY, cursor.getTypeCode());
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testLongArrayType() throws QwpParseException {
        // 1 row, 1D, 1 element with TYPE_LONG_ARRAY
        int bufferSize = 14;
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(addr, (byte) 0);
            Unsafe.putByte(addr + 1, (byte) 1);
            Unsafe.putInt(addr + 2, 1);
            Unsafe.putLong(addr + 6, 99L);

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 1, TYPE_LONG_ARRAY);

            Assert.assertFalse(cursor.isDoubleArray());
            Assert.assertEquals(TYPE_LONG_ARRAY, cursor.getTypeCode());

            cursor.advanceRow();
            Assert.assertEquals(99L, Unsafe.getLong(cursor.getValuesAddress()));
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMultiDimensionalArray() throws QwpParseException {
        // 1 row, 3D array [2][3][4] = 24 elements
        int totalElements = 2 * 3 * 4;
        int bufferSize = 1 + 1 + 3 * 4 + totalElements * 8;
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            long p = addr;
            Unsafe.putByte(p++, (byte) 0);
            Unsafe.putByte(p++, (byte) 3);   // nDims=3
            Unsafe.putInt(p, 2);          // dim[0]=2
            p += 4;
            Unsafe.putInt(p, 3);          // dim[1]=3
            p += 4;
            Unsafe.putInt(p, 4);          // dim[2]=4
            p += 4;
            for (int i = 0; i < totalElements; i++) {
                Unsafe.putDouble(p, i);
                p += 8;
            }

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 1, TYPE_DOUBLE_ARRAY);
            cursor.advanceRow();

            Assert.assertEquals(3, cursor.getNDims());
            Assert.assertEquals(2, cursor.getDimSize(0));
            Assert.assertEquals(3, cursor.getDimSize(1));
            Assert.assertEquals(4, cursor.getDimSize(2));
            Assert.assertEquals(totalElements, cursor.getTotalElements());

            long valAddr = cursor.getValuesAddress();
            Assert.assertEquals(0.0, Unsafe.getDouble(valAddr), 0.0);
            Assert.assertEquals(23.0, Unsafe.getDouble(valAddr + 23 * 8), 0.0);
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsElementCountOverflow() {
        // 1 row, 2D with dimensions 65536 x 65536 overflows int
        int bufferSize = 10; // flag(1) + nDims(1) + 2*dim(8)
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(addr, (byte) 0);
            Unsafe.putByte(addr + 1, (byte) 2);
            Unsafe.putInt(addr + 2, 65_536);
            Unsafe.putInt(addr + 6, 65_536);

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 1, TYPE_DOUBLE_ARRAY);
            Assert.fail("expected QwpParseException for element count overflow");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("overflow"));
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsEmptyData() {
        long addr = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, 0, 1, TYPE_DOUBLE_ARRAY);
            Assert.fail("expected QwpParseException for empty data");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("expected null bitmap flag"));
        } finally {
            Unsafe.free(addr, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsInvalidDimensionCountTooHigh() {
        int bufferSize = 2;
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(addr, (byte) 0);
            Unsafe.putByte(addr + 1, (byte) 33);

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 1, TYPE_DOUBLE_ARRAY);
            Assert.fail("expected QwpParseException for invalid nDims");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("invalid array dimensions"));
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsInvalidDimensionCountZero() {
        int bufferSize = 2;
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(addr, (byte) 0);
            Unsafe.putByte(addr + 1, (byte) 0);

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 1, TYPE_DOUBLE_ARRAY);
            Assert.fail("expected QwpParseException for zero nDims");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("invalid array dimensions"));
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsNegativeDimensionSize() {
        int bufferSize = 6; // flag(1) + nDims(1) + dim(4)
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(addr, (byte) 0);
            Unsafe.putByte(addr + 1, (byte) 1);
            Unsafe.putInt(addr + 2, -1);

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 1, TYPE_DOUBLE_ARRAY);
            Assert.fail("expected QwpParseException for negative dimension size");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("invalid array dimension size"));
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsTruncatedNullBitmap() {
        // rowCount=8 needs 1-byte bitmap, but only the flag byte is provided
        int bufferSize = 1;
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(addr, (byte) 1); // has null bitmap

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 8, TYPE_DOUBLE_ARRAY);
            Assert.fail("expected QwpParseException for truncated null bitmap");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("expected null bitmap"));
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsTruncatedRowData() {
        // 1 row, no null bitmap, data ends right after flag byte
        int bufferSize = 1;
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(addr, (byte) 0);

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 1, TYPE_DOUBLE_ARRAY);
            Assert.fail("expected QwpParseException for truncated row data");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("expected nDims byte"));
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsTruncatedValues() {
        // 1 row, nDims=1, dim[0]=2 needs 16 bytes of values but data ends after shape
        int bufferSize = 6; // flag(1) + nDims(1) + dim(4)
        long addr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(addr, (byte) 0);
            Unsafe.putByte(addr + 1, (byte) 1);
            Unsafe.putInt(addr + 2, 2);

            QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
            cursor.of(addr, bufferSize, 1, TYPE_DOUBLE_ARRAY);
            Assert.fail("expected QwpParseException for truncated values");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("expected 2 values"));
        } finally {
            Unsafe.free(addr, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
