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

import io.questdb.cutlass.qwp.protocol.QwpNullBitmap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class QwpNullBitmapTest {

    @Test
    public void testAllNulls() {
        int rowCount = 16;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpNullBitmapTestUtil.fillAllNull(address, rowCount);
            Assert.assertTrue(QwpNullBitmapTestUtil.allNull(address, rowCount));
            Assert.assertEquals(rowCount, QwpNullBitmap.countNulls(address, rowCount));

            for (int i = 0; i < rowCount; i++) {
                Assert.assertTrue(QwpNullBitmap.isNull(address, i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testAllNullsPartialByte() {
        // Test with row count not divisible by 8
        int rowCount = 10;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpNullBitmapTestUtil.fillAllNull(address, rowCount);
            Assert.assertTrue(QwpNullBitmapTestUtil.allNull(address, rowCount));
            Assert.assertEquals(rowCount, QwpNullBitmap.countNulls(address, rowCount));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBitmapBitOrder() {
        // Test LSB-first bit ordering
        int rowCount = 8;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpNullBitmapTestUtil.fillNoneNull(address, rowCount);

            // Set bit 0 (LSB)
            QwpNullBitmapTestUtil.setNull(address, 0);
            byte b = Unsafe.getByte(address);
            Assert.assertEquals(0b00000001, b & 0xFF);

            // Set bit 7 (MSB of first byte)
            QwpNullBitmapTestUtil.setNull(address, 7);
            b = Unsafe.getByte(address);
            Assert.assertEquals(0b10000001, b & 0xFF);

            // Set bit 3
            QwpNullBitmapTestUtil.setNull(address, 3);
            b = Unsafe.getByte(address);
            Assert.assertEquals(0b10001001, b & 0xFF);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBitmapByteAlignment() {
        // Test that bits 8-15 go into second byte
        int rowCount = 16;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpNullBitmapTestUtil.fillNoneNull(address, rowCount);

            // Set bit 8 (first bit of second byte)
            QwpNullBitmapTestUtil.setNull(address, 8);
            Assert.assertEquals(0, Unsafe.getByte(address) & 0xFF);
            Assert.assertEquals(0b00000001, Unsafe.getByte(address + 1) & 0xFF);

            // Set bit 15 (last bit of second byte)
            QwpNullBitmapTestUtil.setNull(address, 15);
            Assert.assertEquals(0b10000001, Unsafe.getByte(address + 1) & 0xFF);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBitmapSizeCalculation() {
        Assert.assertEquals(0, QwpNullBitmap.sizeInBytes(0));
        Assert.assertEquals(1, QwpNullBitmap.sizeInBytes(1));
        Assert.assertEquals(1, QwpNullBitmap.sizeInBytes(7));
        Assert.assertEquals(1, QwpNullBitmap.sizeInBytes(8));
        Assert.assertEquals(2, QwpNullBitmap.sizeInBytes(9));
        Assert.assertEquals(2, QwpNullBitmap.sizeInBytes(16));
        Assert.assertEquals(3, QwpNullBitmap.sizeInBytes(17));
        Assert.assertEquals(125, QwpNullBitmap.sizeInBytes(1000));
        Assert.assertEquals(125_000, QwpNullBitmap.sizeInBytes(1_000_000));
    }

    @Test
    public void testBitmapWithPartialLastByte() {
        // 10 rows = 2 bytes, but only 2 bits used in second byte
        int rowCount = 10;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        Assert.assertEquals(2, size);

        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpNullBitmapTestUtil.fillNoneNull(address, rowCount);

            // Set row 9 (bit 1 of second byte)
            QwpNullBitmapTestUtil.setNull(address, 9);
            Assert.assertTrue(QwpNullBitmap.isNull(address, 9));
            Assert.assertEquals(0b00000010, Unsafe.getByte(address + 1) & 0xFF);

            Assert.assertEquals(1, QwpNullBitmap.countNulls(address, rowCount));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testByteArrayOperations() {
        int rowCount = 16;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        byte[] bitmap = new byte[size];
        int offset = 0;

        QwpNullBitmapTestUtil.fillNoneNull(bitmap, offset, rowCount);

        QwpNullBitmapTestUtil.setNull(bitmap, offset, 0);
        QwpNullBitmapTestUtil.setNull(bitmap, offset, 5);
        QwpNullBitmapTestUtil.setNull(bitmap, offset, 15);

        Assert.assertTrue(QwpNullBitmapTestUtil.isNull(bitmap, offset, 0));
        Assert.assertFalse(QwpNullBitmapTestUtil.isNull(bitmap, offset, 1));
        Assert.assertTrue(QwpNullBitmapTestUtil.isNull(bitmap, offset, 5));
        Assert.assertTrue(QwpNullBitmapTestUtil.isNull(bitmap, offset, 15));

        Assert.assertEquals(3, QwpNullBitmapTestUtil.countNulls(bitmap, offset, rowCount));

        QwpNullBitmapTestUtil.clearNull(bitmap, offset, 5);
        Assert.assertFalse(QwpNullBitmapTestUtil.isNull(bitmap, offset, 5));
        Assert.assertEquals(2, QwpNullBitmapTestUtil.countNulls(bitmap, offset, rowCount));
    }

    @Test
    public void testByteArrayWithOffset() {
        int rowCount = 8;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        byte[] bitmap = new byte[10 + size]; // Extra padding
        int offset = 5; // Start at offset 5

        QwpNullBitmapTestUtil.fillNoneNull(bitmap, offset, rowCount);
        QwpNullBitmapTestUtil.setNull(bitmap, offset, 3);

        Assert.assertTrue(QwpNullBitmapTestUtil.isNull(bitmap, offset, 3));
        Assert.assertFalse(QwpNullBitmapTestUtil.isNull(bitmap, offset, 4));
    }

    @Test
    public void testClearNull() {
        int rowCount = 8;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpNullBitmapTestUtil.fillAllNull(address, rowCount);
            Assert.assertTrue(QwpNullBitmap.isNull(address, 3));

            QwpNullBitmapTestUtil.clearNull(address, 3);
            Assert.assertFalse(QwpNullBitmap.isNull(address, 3));
            Assert.assertEquals(7, QwpNullBitmap.countNulls(address, rowCount));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEmptyBitmap() {
        Assert.assertEquals(0, QwpNullBitmap.sizeInBytes(0));
    }

    @Test
    public void testLargeBitmap() {
        int rowCount = 100_000;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpNullBitmapTestUtil.fillNoneNull(address, rowCount);

            // Set every 100th row as null
            int expectedNulls = 0;
            for (int i = 0; i < rowCount; i += 100) {
                QwpNullBitmapTestUtil.setNull(address, i);
                expectedNulls++;
            }

            Assert.assertEquals(expectedNulls, QwpNullBitmap.countNulls(address, rowCount));

            // Verify some random positions
            Assert.assertTrue(QwpNullBitmap.isNull(address, 0));
            Assert.assertTrue(QwpNullBitmap.isNull(address, 100));
            Assert.assertTrue(QwpNullBitmap.isNull(address, 99_900));
            Assert.assertFalse(QwpNullBitmap.isNull(address, 1));
            Assert.assertFalse(QwpNullBitmap.isNull(address, 99));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMixedNulls() {
        int rowCount = 20;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpNullBitmapTestUtil.fillNoneNull(address, rowCount);

            // Set specific rows as null: 0, 2, 5, 19
            QwpNullBitmapTestUtil.setNull(address, 0);
            QwpNullBitmapTestUtil.setNull(address, 2);
            QwpNullBitmapTestUtil.setNull(address, 5);
            QwpNullBitmapTestUtil.setNull(address, 19);

            Assert.assertTrue(QwpNullBitmap.isNull(address, 0));
            Assert.assertFalse(QwpNullBitmap.isNull(address, 1));
            Assert.assertTrue(QwpNullBitmap.isNull(address, 2));
            Assert.assertFalse(QwpNullBitmap.isNull(address, 3));
            Assert.assertFalse(QwpNullBitmap.isNull(address, 4));
            Assert.assertTrue(QwpNullBitmap.isNull(address, 5));
            Assert.assertTrue(QwpNullBitmap.isNull(address, 19));

            Assert.assertEquals(4, QwpNullBitmap.countNulls(address, rowCount));
            Assert.assertFalse(QwpNullBitmapTestUtil.allNull(address, rowCount));
            Assert.assertFalse(QwpNullBitmapTestUtil.noneNull(address, rowCount));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNoNulls() {
        int rowCount = 16;
        int size = QwpNullBitmap.sizeInBytes(rowCount);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpNullBitmapTestUtil.fillNoneNull(address, rowCount);
            Assert.assertTrue(QwpNullBitmapTestUtil.noneNull(address, rowCount));
            Assert.assertEquals(0, QwpNullBitmap.countNulls(address, rowCount));

            for (int i = 0; i < rowCount; i++) {
                Assert.assertFalse(QwpNullBitmap.isNull(address, i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
