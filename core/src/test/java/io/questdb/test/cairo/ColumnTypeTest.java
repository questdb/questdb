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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.std.Decimals;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ColumnTypeTest {
    public short getExpectedTag(int precision) {
        int size = Decimals.getStorageSizePow2(precision);
        switch (size) {
            case 0:
                return ColumnType.DECIMAL8;
            case 1:
                return ColumnType.DECIMAL16;
            case 2:
                return ColumnType.DECIMAL32;
            case 3:
                return ColumnType.DECIMAL64;
            case 4:
                return ColumnType.DECIMAL128;
            default:
                return ColumnType.DECIMAL256;
        }
    }

    @Test
    public void testArrayWithWeakDims() {
        int arrayType = ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true);
        Assert.assertTrue(ColumnType.isArray(arrayType));
        Assert.assertTrue(ColumnType.isArrayWithWeakDims(arrayType));
        // arrays with weak dimensions are considered undefined
        Assert.assertTrue(ColumnType.isUndefined(arrayType));
        Assert.assertEquals(ColumnType.DOUBLE, ColumnType.decodeArrayElementType(arrayType));
        Assert.assertEquals(-1, ColumnType.decodeWeakArrayDimensionality(arrayType));

        arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 5);
        Assert.assertTrue(ColumnType.isArray(arrayType));
        Assert.assertFalse(ColumnType.isArrayWithWeakDims(arrayType));
        Assert.assertFalse(ColumnType.isUndefined(arrayType));
        Assert.assertEquals(ColumnType.DOUBLE, ColumnType.decodeArrayElementType(arrayType));
        Assert.assertEquals(5, ColumnType.decodeWeakArrayDimensionality(arrayType));

        arrayType = ColumnType.encodeArrayType(ColumnType.LONG, 7, false);
        Assert.assertTrue(ColumnType.isArray(arrayType));
        Assert.assertFalse(ColumnType.isArrayWithWeakDims(arrayType));
        Assert.assertFalse(ColumnType.isUndefined(arrayType));
        Assert.assertEquals(ColumnType.LONG, ColumnType.decodeArrayElementType(arrayType));
        Assert.assertEquals(7, ColumnType.decodeWeakArrayDimensionality(arrayType));
    }

    @Test
    public void testDecimalDefaultType() {
        Assert.assertEquals(ColumnType.DECIMAL_DEFAULT_TYPE_TAG, ColumnType.tagOf(ColumnType.DECIMAL_DEFAULT_TYPE));
    }

    @Test
    public void testGetDecimalTypeCombinatorics() {
        // Combinations of precision, scale -> expected type
        int[][] combinations = {
                {1, 0, 0x00000100 | (int) ColumnType.DECIMAL8},
                {1, 1, 0x00040100 | (int) ColumnType.DECIMAL8},
                {3, 2, 0x00080300 | (int) ColumnType.DECIMAL16},
                {6, 0, 0x00000600 | (int) ColumnType.DECIMAL32},
                {7, 4, 0x00100700 | (int) ColumnType.DECIMAL32},
                {12, 12, 0x00300C00 | (int) ColumnType.DECIMAL64},
                {18, 0, 0x00001200 | (int) ColumnType.DECIMAL64},
                {18, 18, 0x00481200 | (int) ColumnType.DECIMAL64},
                {30, 4, 0x00101E00 | (int) ColumnType.DECIMAL128},
                {30, 30, 0x00781E00 | (int) ColumnType.DECIMAL128},
                {42, 4, 0x00102A00 | (int) ColumnType.DECIMAL256},
                {55, 45, 0x00B43700 | (int) ColumnType.DECIMAL256},
                {76, 10, 0x00284C00 | (int) ColumnType.DECIMAL256},
        };

        for (int[] combination : combinations) {
            int precision = combination[0];
            int scale = combination[1];
            int expectedType = combination[2];
            int type = ColumnType.getDecimalType(precision, scale);
            Assert.assertEquals(String.format("Failure with precision: %d and scale: %d. Expected 0x%08x but was 0x%08x", precision, scale, expectedType, type), expectedType, type);
        }
    }

    @Test
    public void testGetDecimalTypeFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        final int iterations = 1_000;
        for (int i = 0; i < iterations; i++) {
            int precision = rnd.nextInt(Decimals.MAX_PRECISION - 1) + 1;
            int scale = rnd.nextInt(Decimals.MAX_SCALE - 1) + 1;
            int type = ColumnType.getDecimalType(precision, scale);

            int p = ColumnType.getDecimalPrecision(type);
            Assert.assertEquals(String.format("Failed at iteration %d, expected precision to be %d not %d", i, precision, p), precision, p);

            int s = ColumnType.getDecimalScale(type);
            Assert.assertEquals(String.format("Failed at iteration %d, expected scale to be %d not %d", i, scale, s), scale, s);

            short tag = ColumnType.tagOf(type);
            short expectedTag = getExpectedTag(precision);
            Assert.assertEquals(String.format("Failed at iteration %d, expected tag to be %d not %d", i, expectedTag, tag), expectedTag, tag);

            Assert.assertTrue(ColumnType.isDecimal(type));
        }
    }

    @Test
    public void testIsDecimalInvalid() {
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.BOOLEAN));
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.DOUBLE));
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.VARCHAR));
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.INTERVAL));
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.GEOHASH));
    }
}
