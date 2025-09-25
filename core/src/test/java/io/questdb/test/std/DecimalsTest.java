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

package io.questdb.test.std;

import io.questdb.cairo.vm.MemoryPARWImpl;
import io.questdb.std.Decimals;
import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Decimal256 storage with variable byte length based on precision
 */
public class DecimalsTest extends AbstractTest {
    @Test
    public void testAssertNullMinimalValues() {
        // This test asserts that null values in small decimals (8-64) are the minimum values of their
        // respective integers.
        // Some functions (like abs) rely on that to work properly with null values.
        Assert.assertEquals(Byte.MIN_VALUE, Decimals.DECIMAL8_NULL);
        Assert.assertEquals(Short.MIN_VALUE, Decimals.DECIMAL16_NULL);
        Assert.assertEquals(Integer.MIN_VALUE, Decimals.DECIMAL32_NULL);
        Assert.assertEquals(Long.MIN_VALUE, Decimals.DECIMAL64_NULL);
    }

    @Test
    public void testGetStorageSizeCombinatorics() {
        // Combinations of precision -> storage size needed (in bytes)
        int[][] combinations = {
                {1, 0},
                {2, 0},
                {3, 1},
                {4, 1},
                {5, 2},
                {9, 2},
                {10, 3},
                {18, 3},
                {19, 4},
                {38, 4},
                {39, 5},
                {76, 5},
        };

        for (int[] combination : combinations) {
            int precision = combination[0];
            int storageSizeNeeded = combination[1];
            Assert.assertEquals("Expected " + storageSizeNeeded + " pow 2 bytes needed for decimal of precision " + precision, storageSizeNeeded, Decimals.getStorageSizePow2(precision));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetStorageSizeInvalid() {
        int ignored = Decimals.getStorageSizePow2(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetStorageSizeInvalidTooBig() {
        int ignored = Decimals.getStorageSizePow2(100);
    }

    @Test
    public void testPutDecimal128() {
        for (int i = 0; i < 16; i++) {
            try (MemoryPARWImpl mem = new MemoryPARWImpl(8, 256, MemoryTag.NATIVE_DEFAULT)) {
                long high = 0x0102030405060708L;
                long low = 0x090A0B0C0D0E0F10L;
                mem.putDecimal128(i, high, low);

                Assert.assertEquals("Unexpected high value with offset " + i, high, mem.getDecimal128Hi(i));
                Assert.assertEquals("Unexpected low value with offset " + i, low, mem.getDecimal128Lo(i));
            }
        }
    }

    @Test
    public void testPutDecimal128WithoutOffset() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(8, 256, MemoryTag.NATIVE_DEFAULT)) {
            long high = 0x0102030405060708L;
            long low = 0x090A0B0C0D0E0F10L;
            mem.putDecimal128(high, low);

            Assert.assertEquals("Unexpected high value", high, mem.getDecimal128Hi(0));
            Assert.assertEquals("Unexpected low value", low, mem.getDecimal128Lo(0));
        }
    }

    @Test
    public void testPutDecimal256() {
        for (int i = 0; i < 32; i++) {
            try (MemoryPARWImpl mem = new MemoryPARWImpl(8, 256, MemoryTag.NATIVE_DEFAULT)) {
                long hh = 0x0102030405060708L;
                long hl = 0x090A0B0C0D0E0F10L;
                long lh = 0x1112131415161718L;
                long ll = 0x191A1B1C1D1E1F20L;
                mem.putDecimal256(i, hh, hl, lh, ll);

                Assert.assertEquals("Unexpected hh value with offset " + i, hh, mem.getDecimal256HH(i));
                Assert.assertEquals("Unexpected hl value with offset " + i, hl, mem.getDecimal256HL(i));
                Assert.assertEquals("Unexpected lh value with offset " + i, lh, mem.getDecimal256LH(i));
                Assert.assertEquals("Unexpected ll value with offset " + i, ll, mem.getDecimal256LL(i));
            }
        }
    }

    @Test
    public void testPutDecimal256WithoutOffset() {
        try (MemoryPARWImpl mem = new MemoryPARWImpl(8, 256, MemoryTag.NATIVE_DEFAULT)) {
            long hh = 0x0102030405060708L;
            long hl = 0x090A0B0C0D0E0F10L;
            long lh = 0x1112131415161718L;
            long ll = 0x191A1B1C1D1E1F20L;
            mem.putDecimal256(hh, hl, lh, ll);

            Assert.assertEquals("Unexpected hh value", hh, mem.getDecimal256HH(0));
            Assert.assertEquals("Unexpected hl value", hl, mem.getDecimal256HL(0));
            Assert.assertEquals("Unexpected lh value", lh, mem.getDecimal256LH(0));
            Assert.assertEquals("Unexpected ll value", ll, mem.getDecimal256LL(0));
        }
    }
}