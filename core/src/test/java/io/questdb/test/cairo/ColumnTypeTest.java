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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.*;
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

    @Test
    public void testIsBuiltInWideningCastContract() {
        // Contract: if isBuiltInWideningCast(from, to) returns true,
        // then calling the corresponding getter on a Function of type 'from'
        // to retrieve value as type 'to' must NOT throw UnsupportedOperationException

        final short[] types = {
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.CHAR,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.DATE,
                ColumnType.TIMESTAMP,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.STRING,
                ColumnType.NULL
        };

        int violations = 0;
        int unexpectedlySupported = 0;
        StringBuilder violationDetails = new StringBuilder();
        StringBuilder unexpectedlySupportedDetails = new StringBuilder();

        for (short fromType : types) {
            for (short toType : types) {
                boolean isBuiltInWidening = ColumnType.isBuiltInWideningCast(fromType, toType);
                Function testFunc = createTestFunction(fromType);

                if (testFunc == null) {
                    continue; // Skip types we can't test
                }

                boolean throwsUnsupported = false;
                String exceptionMessage = null;

                try {
                    callGetterForType(testFunc, toType);
                } catch (ImplicitCastException e) {
                    // ImplicitCastException means the conversion is supported but the value failed
                    // This is acceptable - types are compatible, just this specific value can't convert
                    // Example: CHAR 'A' -> BYTE throws ImplicitCastException, but CHAR -> BYTE is supported
                } catch (UnsupportedOperationException e) {
                    // UnsupportedOperationException means the types are fundamentally incompatible
                    throwsUnsupported = true;
                    exceptionMessage = e.getMessage();
                }

                // Check contract violation: isBuiltInWidening claims true but getter throws
                if (isBuiltInWidening && throwsUnsupported) {
                    violations++;
                    violationDetails.append(String.format(
                            "\n  VIOLATION: isBuiltInWideningCast(%s, %s) = true, but getter throws UnsupportedOperationException: %s",
                            ColumnType.nameOf(fromType),
                            ColumnType.nameOf(toType),
                            exceptionMessage
                    ));
                }

                // Check inverse: getter works but isBuiltInWidening returns false
                // This is informational - might indicate missing optimization or intentional design
                if (!isBuiltInWidening && !throwsUnsupported && fromType != toType) {
                    unexpectedlySupported++;
                    unexpectedlySupportedDetails.append(String.format(
                            "\n  INFO: isBuiltInWideningCast(%s, %s) = false, but getter works without UnsupportedOperationException",
                            ColumnType.nameOf(fromType),
                            ColumnType.nameOf(toType)
                    ));
                }
            }
        }

        if (violations > 0) {
            Assert.fail("Found " + violations + " contract violations:" + violationDetails);
        }

        // Print informational findings
        if (unexpectedlySupported > 0) {
            System.out.println("\n=== Informational: Getters that work but aren't marked as isBuiltInWideningCast ===");
            System.out.println("Found " + unexpectedlySupported + " cases:" + unexpectedlySupportedDetails);
            System.out.println("\nThese conversions work but may require cast wrappers or are intentionally not optimized.");
        }
    }

    private Function createTestFunction(short type) {
        return switch (type) {
            case ColumnType.BOOLEAN -> BooleanConstant.TRUE;
            case ColumnType.BYTE -> new ByteConstant((byte) 42);
            case ColumnType.SHORT -> new ShortConstant((short) 42);
            case ColumnType.CHAR -> new CharConstant('A');
            case ColumnType.INT -> new IntConstant(42);
            case ColumnType.LONG -> new LongConstant(42L);
            case ColumnType.DATE -> new DateConstant(42L);
            case ColumnType.TIMESTAMP -> new TimestampConstant(42L, ColumnType.TIMESTAMP_MICRO);
            case ColumnType.FLOAT -> new FloatConstant(42.0f);
            case ColumnType.DOUBLE -> new DoubleConstant(42.0);
            case ColumnType.STRING -> new StrConstant("42");
            case ColumnType.NULL -> NullConstant.NULL;
            default -> null;
        };
    }

    @Test
    public void foo() {
        Assert.assertFalse(ColumnType.isAssignableFrom(ColumnType.BOOLEAN, ColumnType.INT));
    }

    private void callGetterForType(Function func, short type) {
        switch (type) {
            case ColumnType.BOOLEAN -> func.getBool(null);
            case ColumnType.BYTE -> func.getByte(null);
            case ColumnType.SHORT -> func.getShort(null);
            case ColumnType.CHAR -> func.getChar(null);
            case ColumnType.INT -> func.getInt(null);
            case ColumnType.LONG -> func.getLong(null);
            case ColumnType.DATE -> func.getDate(null);
            case ColumnType.TIMESTAMP -> func.getTimestamp(null);
            case ColumnType.FLOAT -> func.getFloat(null);
            case ColumnType.DOUBLE -> func.getDouble(null);
            case ColumnType.STRING -> func.getStrA(null);
        }
    }
}
