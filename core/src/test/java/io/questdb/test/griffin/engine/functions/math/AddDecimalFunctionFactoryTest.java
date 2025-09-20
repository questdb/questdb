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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.math.AddDecimalFunctionFactory;
import io.questdb.std.Decimals;
import org.junit.Test;

public class AddDecimalFunctionFactoryTest extends ArithmeticDecimalFunctionFactoryTest {
    private final AddDecimalFunctionFactory factory = new AddDecimalFunctionFactory();

    @Test
    public void testAddDecimal128Overflow() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal128Constant(Long.MAX_VALUE, Long.MAX_VALUE, ColumnType.getDecimalType(37, 0)),
                new Decimal128Constant(0, 1, ColumnType.getDecimalType(37, 0)),
                "'+' operation failed: Overflow in addition: result exceeds maximum precision"
        );
    }

    @Test
    public void testAddDecimal128Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)), // 1.00
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)), // 2.00
                0, 0, 0, 300, ColumnType.getDecimalType(21, 2)
        );
    }

    @Test
    public void testAddDecimal128WithCarry() throws SqlException {
        // Test values that require carry operation
        createFunctionAndAssert(
                new Decimal128Constant(0, -1, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 1, ColumnType.getDecimalType(20, 2)),
                0, 0, 1, 0, ColumnType.getDecimalType(21, 2)
        );
    }

    @Test
    public void testAddDecimal128WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 150, ColumnType.getDecimalType(19, 2)), // 1.50
                new Decimal128Constant(0, 5, ColumnType.getDecimalType(19, 1)), // 0.5
                0, 0, 0, 200, ColumnType.getDecimalType(21, 2)
        );
    }

    @Test
    public void testAddDecimal128WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                ColumnType.getDecimalType(23, 2)
        );
    }

    @Test
    public void testAddDecimal16Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)), // 1.00
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)), // 2.00
                0, 0, 0, 300, ColumnType.getDecimalType(5, 2)
        );
    }

    @Test
    public void testAddDecimal16WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal16Constant((short) 150, ColumnType.getDecimalType(4, 2)), // 1.50
                new Decimal16Constant((short) 5, ColumnType.getDecimalType(4, 1)), // 0.5
                0, 0, 0, 200, ColumnType.getDecimalType(6, 2)
        );
    }

    @Test
    public void testAddDecimal16WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                ColumnType.getDecimalType(7, 2)
        );
    }

    @Test
    public void testAddDecimal256MaxPrecision() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)),
                0, 0, 0, 300, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)
        );
    }

    @Test
    public void testAddDecimal256Negative() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 1, ColumnType.getDecimalType(40, 0)),
                new Decimal256Constant(-1, -1, -1, -1, ColumnType.getDecimalType(40, 0)),
                0, 0, 0, 0, ColumnType.getDecimalType(41, 0)
        );
    }

    @Test
    public void testAddDecimal256Overflow() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal256Constant(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)),
                new Decimal256Constant(0, 0, 0, 1, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)),
                "'+' operation failed: Overflow in addition: result exceeds maximum precision"
        );
    }

    @Test
    public void testAddDecimal256Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)), // 1.00
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)), // 2.00
                0, 0, 0, 300, ColumnType.getDecimalType(41, 2)
        );
    }

    @Test
    public void testAddDecimal256WithCarry() throws SqlException {
        // Test values that require carry operation
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, -1, ColumnType.getDecimalType(39, 0)),
                new Decimal256Constant(0, 0, 0, 1, ColumnType.getDecimalType(39, 0)),
                0, 0, 1, 0, ColumnType.getDecimalType(40, 0)
        );
    }

    @Test
    public void testAddDecimal256WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 0)
                ),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                ColumnType.getDecimalType(43, 2)
        );
    }

    @Test
    public void testAddDecimal32Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)), // 1.00
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)), // 2.00
                0, 0, 0, 300, ColumnType.getDecimalType(9, 2)
        );
    }

    @Test
    public void testAddDecimal32WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal32Constant(150, ColumnType.getDecimalType(9, 2)), // 1.50
                new Decimal32Constant(5, ColumnType.getDecimalType(9, 1)), // 0.5
                0, 0, 0, 200, ColumnType.getDecimalType(11, 2)
        );
    }

    @Test
    public void testAddDecimal32WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal32Constant(100, ColumnType.getDecimalType(9, 2)),
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(9, 0)),
                ColumnType.getDecimalType(12, 2)
        );
    }

    @Test
    public void testAddDecimal64Overflow() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal64Constant(Long.MAX_VALUE, ColumnType.getDecimalType(17, 0)),
                new Decimal64Constant(1, ColumnType.getDecimalType(17, 0)),
                "'+' operation failed: Overflow in addition: result exceeds 64-bit capacity"
        );
    }

    @Test
    public void testAddDecimal64Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)), // 1.00
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)), // 2.00
                0, 0, 0, 300, ColumnType.getDecimalType(11, 2)
        );
    }

    @Test
    public void testAddDecimal64WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal64Constant(150, ColumnType.getDecimalType(14, 2)), // 1.50
                new Decimal64Constant(5, ColumnType.getDecimalType(14, 1)), // 0.5
                0, 0, 0, 200, ColumnType.getDecimalType(16, 2)
        );
    }

    @Test
    public void testAddDecimal64WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal64Constant(100, ColumnType.getDecimalType(15, 2)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(15, 0)),
                ColumnType.getDecimalType(18, 2)
        );
    }

    @Test
    public void testAddDecimal8Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)), // 1.0
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1)), // 2.0
                0, 0, 0, 30, ColumnType.getDecimalType(3, 1)
        );
    }

    @Test
    public void testAddDecimal8WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 15, ColumnType.getDecimalType(2, 2)), // 0.15
                new Decimal8Constant((byte) 5, ColumnType.getDecimalType(2, 1)), // 0.5
                0, 0, 0, 65, ColumnType.getDecimalType(4, 2)
        );
    }

    @Test
    public void testAddDecimal8WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                ColumnType.getDecimalType(4, 1)
        );
    }

    @Test
    public void testAddMixedDecimalTypes() throws SqlException {
        // Test adding different decimal sizes
        createFunctionAndAssert(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)),
                1, 1, 2, 2, ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssert(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal64Constant(1, ColumnType.getDecimalType(10, 2)),
                1, 1, 1, 2, ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssert(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal32Constant(1, ColumnType.getDecimalType(8, 2)),
                1, 1, 1, 2, ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssert(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal16Constant((short) 1, ColumnType.getDecimalType(4, 2)),
                1, 1, 1, 2, ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssert(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(2, 2)),
                1, 1, 1, 2, ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssert(
                new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)),
                new Decimal64Constant(1, ColumnType.getDecimalType(10, 2)),
                0, 0, 1, 2, ColumnType.getDecimalType(21, 2)
        );

        createFunctionAndAssert(
                new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)),
                new Decimal32Constant(1, ColumnType.getDecimalType(8, 2)),
                0, 0, 1, 2, ColumnType.getDecimalType(21, 2)
        );

        createFunctionAndAssert(
                new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)),
                new Decimal16Constant((short) 1, ColumnType.getDecimalType(4, 2)),
                0, 0, 1, 2, ColumnType.getDecimalType(21, 2)
        );

        createFunctionAndAssert(
                new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)),
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(2, 2)),
                0, 0, 1, 2, ColumnType.getDecimalType(21, 2)
        );

        createFunctionAndAssert(
                new Decimal64Constant(1, ColumnType.getDecimalType(15, 2)),
                new Decimal32Constant(1, ColumnType.getDecimalType(8, 2)),
                0, 0, 0, 2, ColumnType.getDecimalType(16, 2)
        );

        createFunctionAndAssert(
                new Decimal64Constant(1, ColumnType.getDecimalType(15, 2)),
                new Decimal16Constant((short) 1, ColumnType.getDecimalType(4, 2)),
                0, 0, 0, 2, ColumnType.getDecimalType(16, 2)
        );

        createFunctionAndAssert(
                new Decimal64Constant(1, ColumnType.getDecimalType(15, 2)),
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(2, 2)),
                0, 0, 0, 2, ColumnType.getDecimalType(16, 2)
        );
    }

    @Test
    public void testAddMixedDecimalTypesNull() throws SqlException {
        // Test adding different decimal sizes
        createFunctionAndAssertNull(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 2)),
                ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssertNull(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssertNull(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 2)),
                ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssertNull(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 2)),
                ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssertNull(
                new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)),
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 2)),
                ColumnType.getDecimalType(41, 2)
        );

        createFunctionAndAssertNull(
                new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                ColumnType.getDecimalType(21, 2)
        );

        createFunctionAndAssertNull(
                new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)),
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 2)),
                ColumnType.getDecimalType(21, 2)
        );

        createFunctionAndAssertNull(
                new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)),
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 2)),
                ColumnType.getDecimalType(21, 2)
        );

        createFunctionAndAssertNull(
                new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)),
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 2)),
                ColumnType.getDecimalType(21, 2)
        );

        createFunctionAndAssertNull(
                new Decimal64Constant(1, ColumnType.getDecimalType(15, 2)),
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 2)),
                ColumnType.getDecimalType(16, 2)
        );

        createFunctionAndAssertNull(
                new Decimal64Constant(1, ColumnType.getDecimalType(15, 2)),
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 2)),
                ColumnType.getDecimalType(16, 2)
        );

        createFunctionAndAssertNull(
                new Decimal64Constant(1, ColumnType.getDecimalType(15, 2)),
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 2)),
                ColumnType.getDecimalType(16, 2)
        );
    }

    @Test
    public void testAddRandomValues() throws SqlException {
        // Test with some random but predictable values
        for (int i = 1; i < 50; i++) {
            long val1 = i * 13;
            long val2 = i * 7;
            int scale = i % 3;

            createFunctionAndAssert(
                    new Decimal128Constant(0, val1, ColumnType.getDecimalType(21, scale)),
                    new Decimal128Constant(0, val2, ColumnType.getDecimalType(21, scale)),
                    0, 0, 0, val1 + val2, ColumnType.getDecimalType(22, scale)
            );
        }
    }

    @Test
    public void testAddWithPrecisionScaling() throws SqlException {
        // Test that precision and scale are handled correctly
        // 12.34 + 56.7 = 69.04
        createFunctionAndAssert(
                new Decimal128Constant(0, 1234, ColumnType.getDecimalType(22, 2)),
                new Decimal128Constant(0, 567, ColumnType.getDecimalType(22, 1)),
                0, 0, 0, 6904, ColumnType.getDecimalType(24, 2)
        );
    }

    @Override
    protected FunctionFactory getFactory() {
        return factory;
    }
}
