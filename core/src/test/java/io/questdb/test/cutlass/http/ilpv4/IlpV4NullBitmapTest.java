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

import io.questdb.cutlass.http.ilpv4.IlpV4NullBitmap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class IlpV4NullBitmapTest {

    // ==================== Size Calculation Tests ====================

    @Test
    public void testBitmapSizeCalculation() {
        Assert.assertEquals(0, IlpV4NullBitmap.sizeInBytes(0));
        Assert.assertEquals(1, IlpV4NullBitmap.sizeInBytes(1));
        Assert.assertEquals(1, IlpV4NullBitmap.sizeInBytes(7));
        Assert.assertEquals(1, IlpV4NullBitmap.sizeInBytes(8));
        Assert.assertEquals(2, IlpV4NullBitmap.sizeInBytes(9));
        Assert.assertEquals(2, IlpV4NullBitmap.sizeInBytes(16));
        Assert.assertEquals(3, IlpV4NullBitmap.sizeInBytes(17));
        Assert.assertEquals(125, IlpV4NullBitmap.sizeInBytes(1000));
        Assert.assertEquals(125000, IlpV4NullBitmap.sizeInBytes(1000000));
    }

    // ==================== Empty Bitmap Tests ====================

    @Test
    public void testEmptyBitmap() {
        Assert.assertEquals(0, IlpV4NullBitmap.sizeInBytes(0));
    }

    // ==================== All Nulls Tests ====================

    @Test
    public void testAllNulls() {
        int rowCount = 16;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4NullBitmap.fillAllNull(address, rowCount);
            Assert.assertTrue(IlpV4NullBitmap.allNull(address, rowCount));
            Assert.assertEquals(rowCount, IlpV4NullBitmap.countNulls(address, rowCount));

            for (int i = 0; i < rowCount; i++) {
                Assert.assertTrue(IlpV4NullBitmap.isNull(address, i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testAllNullsPartialByte() {
        // Test with row count not divisible by 8
        int rowCount = 10;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4NullBitmap.fillAllNull(address, rowCount);
            Assert.assertTrue(IlpV4NullBitmap.allNull(address, rowCount));
            Assert.assertEquals(rowCount, IlpV4NullBitmap.countNulls(address, rowCount));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== No Nulls Tests ====================

    @Test
    public void testNoNulls() {
        int rowCount = 16;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4NullBitmap.fillNoneNull(address, rowCount);
            Assert.assertTrue(IlpV4NullBitmap.noneNull(address, rowCount));
            Assert.assertEquals(0, IlpV4NullBitmap.countNulls(address, rowCount));

            for (int i = 0; i < rowCount; i++) {
                Assert.assertFalse(IlpV4NullBitmap.isNull(address, i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Mixed Nulls Tests ====================

    @Test
    public void testMixedNulls() {
        int rowCount = 20;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4NullBitmap.fillNoneNull(address, rowCount);

            // Set specific rows as null: 0, 2, 5, 19
            IlpV4NullBitmap.setNull(address, 0);
            IlpV4NullBitmap.setNull(address, 2);
            IlpV4NullBitmap.setNull(address, 5);
            IlpV4NullBitmap.setNull(address, 19);

            Assert.assertTrue(IlpV4NullBitmap.isNull(address, 0));
            Assert.assertFalse(IlpV4NullBitmap.isNull(address, 1));
            Assert.assertTrue(IlpV4NullBitmap.isNull(address, 2));
            Assert.assertFalse(IlpV4NullBitmap.isNull(address, 3));
            Assert.assertFalse(IlpV4NullBitmap.isNull(address, 4));
            Assert.assertTrue(IlpV4NullBitmap.isNull(address, 5));
            Assert.assertTrue(IlpV4NullBitmap.isNull(address, 19));

            Assert.assertEquals(4, IlpV4NullBitmap.countNulls(address, rowCount));
            Assert.assertFalse(IlpV4NullBitmap.allNull(address, rowCount));
            Assert.assertFalse(IlpV4NullBitmap.noneNull(address, rowCount));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Bit Order Tests (LSB First) ====================

    @Test
    public void testBitmapBitOrder() {
        // Test LSB-first bit ordering
        int rowCount = 8;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4NullBitmap.fillNoneNull(address, rowCount);

            // Set bit 0 (LSB)
            IlpV4NullBitmap.setNull(address, 0);
            byte b = Unsafe.getUnsafe().getByte(address);
            Assert.assertEquals(0b00000001, b & 0xFF);

            // Set bit 7 (MSB of first byte)
            IlpV4NullBitmap.setNull(address, 7);
            b = Unsafe.getUnsafe().getByte(address);
            Assert.assertEquals(0b10000001, b & 0xFF);

            // Set bit 3
            IlpV4NullBitmap.setNull(address, 3);
            b = Unsafe.getUnsafe().getByte(address);
            Assert.assertEquals(0b10001001, b & 0xFF);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Byte Alignment Tests ====================

    @Test
    public void testBitmapByteAlignment() {
        // Test that bits 8-15 go into second byte
        int rowCount = 16;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4NullBitmap.fillNoneNull(address, rowCount);

            // Set bit 8 (first bit of second byte)
            IlpV4NullBitmap.setNull(address, 8);
            Assert.assertEquals(0, Unsafe.getUnsafe().getByte(address) & 0xFF);
            Assert.assertEquals(0b00000001, Unsafe.getUnsafe().getByte(address + 1) & 0xFF);

            // Set bit 15 (last bit of second byte)
            IlpV4NullBitmap.setNull(address, 15);
            Assert.assertEquals(0b10000001, Unsafe.getUnsafe().getByte(address + 1) & 0xFF);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Partial Last Byte Tests ====================

    @Test
    public void testBitmapWithPartialLastByte() {
        // 10 rows = 2 bytes, but only 2 bits used in second byte
        int rowCount = 10;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        Assert.assertEquals(2, size);

        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4NullBitmap.fillNoneNull(address, rowCount);

            // Set row 9 (bit 1 of second byte)
            IlpV4NullBitmap.setNull(address, 9);
            Assert.assertTrue(IlpV4NullBitmap.isNull(address, 9));
            Assert.assertEquals(0b00000010, Unsafe.getUnsafe().getByte(address + 1) & 0xFF);

            Assert.assertEquals(1, IlpV4NullBitmap.countNulls(address, rowCount));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Clear Null Tests ====================

    @Test
    public void testClearNull() {
        int rowCount = 8;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4NullBitmap.fillAllNull(address, rowCount);
            Assert.assertTrue(IlpV4NullBitmap.isNull(address, 3));

            IlpV4NullBitmap.clearNull(address, 3);
            Assert.assertFalse(IlpV4NullBitmap.isNull(address, 3));
            Assert.assertEquals(7, IlpV4NullBitmap.countNulls(address, rowCount));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Byte Array Tests ====================

    @Test
    public void testByteArrayOperations() {
        int rowCount = 16;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        byte[] bitmap = new byte[size];
        int offset = 0;

        IlpV4NullBitmap.fillNoneNull(bitmap, offset, rowCount);

        IlpV4NullBitmap.setNull(bitmap, offset, 0);
        IlpV4NullBitmap.setNull(bitmap, offset, 5);
        IlpV4NullBitmap.setNull(bitmap, offset, 15);

        Assert.assertTrue(IlpV4NullBitmap.isNull(bitmap, offset, 0));
        Assert.assertFalse(IlpV4NullBitmap.isNull(bitmap, offset, 1));
        Assert.assertTrue(IlpV4NullBitmap.isNull(bitmap, offset, 5));
        Assert.assertTrue(IlpV4NullBitmap.isNull(bitmap, offset, 15));

        Assert.assertEquals(3, IlpV4NullBitmap.countNulls(bitmap, offset, rowCount));

        IlpV4NullBitmap.clearNull(bitmap, offset, 5);
        Assert.assertFalse(IlpV4NullBitmap.isNull(bitmap, offset, 5));
        Assert.assertEquals(2, IlpV4NullBitmap.countNulls(bitmap, offset, rowCount));
    }

    @Test
    public void testByteArrayWithOffset() {
        int rowCount = 8;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        byte[] bitmap = new byte[10 + size]; // Extra padding
        int offset = 5; // Start at offset 5

        IlpV4NullBitmap.fillNoneNull(bitmap, offset, rowCount);
        IlpV4NullBitmap.setNull(bitmap, offset, 3);

        Assert.assertTrue(IlpV4NullBitmap.isNull(bitmap, offset, 3));
        Assert.assertFalse(IlpV4NullBitmap.isNull(bitmap, offset, 4));
    }

    // ==================== Large Bitmap Tests ====================

    @Test
    public void testLargeBitmap() {
        int rowCount = 100000;
        int size = IlpV4NullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4NullBitmap.fillNoneNull(address, rowCount);

            // Set every 100th row as null
            int expectedNulls = 0;
            for (int i = 0; i < rowCount; i += 100) {
                IlpV4NullBitmap.setNull(address, i);
                expectedNulls++;
            }

            Assert.assertEquals(expectedNulls, IlpV4NullBitmap.countNulls(address, rowCount));

            // Verify some random positions
            Assert.assertTrue(IlpV4NullBitmap.isNull(address, 0));
            Assert.assertTrue(IlpV4NullBitmap.isNull(address, 100));
            Assert.assertTrue(IlpV4NullBitmap.isNull(address, 99900));
            Assert.assertFalse(IlpV4NullBitmap.isNull(address, 1));
            Assert.assertFalse(IlpV4NullBitmap.isNull(address, 99));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
