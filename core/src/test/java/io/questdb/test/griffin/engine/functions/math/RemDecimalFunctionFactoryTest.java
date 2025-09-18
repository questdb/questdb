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
import io.questdb.griffin.engine.functions.math.RemDecimalFunctionFactory;
import io.questdb.std.Decimals;
import org.junit.Test;

public class RemDecimalFunctionFactoryTest extends ArithmeticDecimalFunctionFactoryTest {
    private final RemDecimalFunctionFactory factory = new RemDecimalFunctionFactory();

    @Test
    public void testRemDecimal128DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)),
                new Decimal128Constant(0, 0, ColumnType.getDecimalType(19, 2)),
                "'%' operation failed: Division by zero"
        );
    }

    @Test
    public void testRemDecimal128Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 500, ColumnType.getDecimalType(20, 2)), // 5.00
                new Decimal128Constant(0, 300, ColumnType.getDecimalType(20, 2)), // 3.00
                0, 0, 0, 200, ColumnType.getDecimalType(20, 2) // 2.00
        );
    }

    @Test
    public void testRemDecimal128WithCarry() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 1000, ColumnType.getDecimalType(20, 2)), // 10.00
                new Decimal128Constant(0, 300, ColumnType.getDecimalType(20, 2)), // 3.00
                0, 0, 0, 100, ColumnType.getDecimalType(20, 2) // 1.00
        );
    }

    @Test
    public void testRemDecimal128WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 500, ColumnType.getDecimalType(19, 2)), // 5.00
                new Decimal128Constant(0, 15, ColumnType.getDecimalType(19, 1)), // 1.5
                0, 0, 0, 50, ColumnType.getDecimalType(20, 2) // 0.50
        );
    }

    @Test
    public void testRemDecimal128WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                ColumnType.getDecimalType(22, 2)
        );
    }

    @Test
    public void testRemDecimal16DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 2)),
                "'%' operation failed: Division by zero"
        );
    }

    @Test
    public void testRemDecimal16Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal16Constant((short) 500, ColumnType.getDecimalType(4, 2)), // 5.00
                new Decimal16Constant((short) 300, ColumnType.getDecimalType(4, 2)), // 3.00
                0, 0, 0, 200, ColumnType.getDecimalType(4, 2) // 2.00
        );
    }

    @Test
    public void testRemDecimal16WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal16Constant((short) 500, ColumnType.getDecimalType(4, 2)), // 5.00
                new Decimal16Constant((short) 15, ColumnType.getDecimalType(4, 1)), // 1.5
                0, 0, 0, 50, ColumnType.getDecimalType(5, 2) // 0.50
        );
    }

    @Test
    public void testRemDecimal16WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                ColumnType.getDecimalType(6, 2)
        );
    }

    @Test
    public void testRemDecimal256DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 0, ColumnType.getDecimalType(40, 2)),
                "'%' operation failed: Division by zero"
        );
    }

    @Test
    public void testRemDecimal256MaxPrecision() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 500, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)),
                new Decimal256Constant(0, 0, 0, 300, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)),
                0, 0, 0, 200, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)
        );
    }

    @Test
    public void testRemDecimal256Negative() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, -10, ColumnType.getDecimalType(40, 0)), // -10
                new Decimal256Constant(0, 0, 0, 1, ColumnType.getDecimalType(40, 0)), // 1
                0, 0, 0, 0, ColumnType.getDecimalType(40, 0) // 0
        );
    }

    @Test
    public void testRemDecimal256Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 500, ColumnType.getDecimalType(40, 2)), // 5.00
                new Decimal256Constant(0, 0, 0, 300, ColumnType.getDecimalType(40, 2)), // 3.00
                0, 0, 0, 200, ColumnType.getDecimalType(40, 2) // 2.00
        );
    }

    @Test
    public void testRemDecimal256WithCarry() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 1, 0, ColumnType.getDecimalType(39, 0)), // large number
                new Decimal256Constant(0, 0, 0, 3, ColumnType.getDecimalType(39, 0)), // 3
                0, 0, 0, 1, ColumnType.getDecimalType(39, 0) // 1
        );
    }

    @Test
    public void testRemDecimal256WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 0)
                ),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                ColumnType.getDecimalType(42, 2)
        );
    }

    @Test
    public void testRemDecimal32DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(0, ColumnType.getDecimalType(8, 2)),
                "'%' operation failed: Division by zero"
        );
    }

    @Test
    public void testRemDecimal32Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal32Constant(500, ColumnType.getDecimalType(8, 2)), // 5.00
                new Decimal32Constant(300, ColumnType.getDecimalType(8, 2)), // 3.00
                0, 0, 0, 200, ColumnType.getDecimalType(8, 2) // 2.00
        );
    }

    @Test
    public void testRemDecimal32WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal32Constant(500, ColumnType.getDecimalType(9, 2)), // 5.00
                new Decimal32Constant(15, ColumnType.getDecimalType(9, 1)), // 1.5
                0, 0, 0, 50, ColumnType.getDecimalType(10, 2) // 0.50
        );
    }

    @Test
    public void testRemDecimal32WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal32Constant(100, ColumnType.getDecimalType(9, 2)),
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(9, 0)),
                ColumnType.getDecimalType(11, 2)
        );
    }

    @Test
    public void testRemDecimal64DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(0, ColumnType.getDecimalType(10, 2)),
                "'%' operation failed: Division by zero"
        );
    }

    @Test
    public void testRemDecimal64Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal64Constant(500, ColumnType.getDecimalType(10, 2)), // 5.00
                new Decimal64Constant(300, ColumnType.getDecimalType(10, 2)), // 3.00
                0, 0, 0, 200, ColumnType.getDecimalType(10, 2) // 2.00
        );
    }

    @Test
    public void testRemDecimal64WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal64Constant(500, ColumnType.getDecimalType(14, 2)), // 5.00
                new Decimal64Constant(15, ColumnType.getDecimalType(14, 1)), // 1.5
                0, 0, 0, 50, ColumnType.getDecimalType(15, 2) // 0.50
        );
    }

    @Test
    public void testRemDecimal64WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal64Constant(100, ColumnType.getDecimalType(15, 2)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(15, 0)),
                ColumnType.getDecimalType(17, 2)
        );
    }

    @Test
    public void testRemDecimal8DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 1)),
                "'%' operation failed: Division by zero"
        );
    }

    @Test
    public void testRemDecimal8Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 50, ColumnType.getDecimalType(2, 1)), // 5.0
                new Decimal8Constant((byte) 30, ColumnType.getDecimalType(2, 1)), // 3.0
                0, 0, 0, 20, ColumnType.getDecimalType(2, 1) // 2.0
        );
    }

    @Test
    public void testRemDecimal8WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 50, ColumnType.getDecimalType(2, 2)), // 0.50
                new Decimal8Constant((byte) 3, ColumnType.getDecimalType(2, 1)), // 0.3
                0, 0, 0, 20, ColumnType.getDecimalType(3, 2) // 0.20
        );
    }

    @Test
    public void testRemDecimal8WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                ColumnType.getDecimalType(3, 1)
        );
    }

    @Test
    public void testRemMixedDecimalTypes() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(1, 1, 1, 5, ColumnType.getDecimalType(40, 2)),
                new Decimal128Constant(0, 1, ColumnType.getDecimalType(20, 2)),
                0, 0, 0, 0, ColumnType.getDecimalType(40, 2)
        );

        createFunctionAndAssert(
                new Decimal256Constant(1, 1, 1, 11, ColumnType.getDecimalType(40, 2)),
                new Decimal64Constant(2, ColumnType.getDecimalType(10, 2)),
                0, 0, 0, 1, ColumnType.getDecimalType(40, 2)
        );
    }

    @Test
    public void testRemRandomValues() throws SqlException {
        for (int i = 2; i < 50; i++) {
            long dividend = i * 13;
            long divisor = i % 7 + 1; // ensure divisor is not zero
            long remainder = dividend % divisor;
            int scale = i % 3;

            createFunctionAndAssert(
                    new Decimal128Constant(0, dividend, ColumnType.getDecimalType(21, scale)),
                    new Decimal128Constant(0, divisor, ColumnType.getDecimalType(21, scale)),
                    0, 0, 0, remainder, ColumnType.getDecimalType(21, scale)
            );
        }
    }

    @Test
    public void testRemWithPrecisionScaling() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 1234, ColumnType.getDecimalType(22, 2)), // 12.34
                new Decimal128Constant(0, 567, ColumnType.getDecimalType(22, 1)), // 56.7
                // 12.34 % 56.7 = 12.34
                0, 0, 0, 1234, ColumnType.getDecimalType(23, 2)
        );
    }

    @Override
    protected FunctionFactory getFactory() {
        return factory;
    }
}
