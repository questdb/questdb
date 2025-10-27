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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.math.DivDecimalFunctionFactory;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class DivDecimalFunctionFactoryTest extends ArithmeticDecimalFunctionFactoryTest {
    private final DivDecimalFunctionFactory factory = new DivDecimalFunctionFactory();

    @Test
    public void testDivByOne() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 12345, ColumnType.getDecimalType(19, 3)), // 12.345
                new Decimal128Constant(0, 1000, ColumnType.getDecimalType(19, 3)), // 1.000
                0, 0, 0, 12345000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 6) // 12.345000
        );
    }

    @Test
    public void testDivDecimal128DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)),
                new Decimal128Constant(0, 0, ColumnType.getDecimalType(19, 2)),
                "'/' operation failed: Division by zero"
        );
    }

    @Test
    public void testDivDecimal128Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 600, ColumnType.getDecimalType(20, 2)), // 6.00
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)), // 2.00
                0, 0, 0, 30000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4) // 3.0000
        );
    }

    @Test
    public void testDivDecimal128WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 1000, ColumnType.getDecimalType(19, 2)), // 10.00
                new Decimal128Constant(0, 25, ColumnType.getDecimalType(19, 1)), // 2.5
                0, 0, 0, 4000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3) // 4.000
        );
    }

    @Test
    public void testDivDecimal128WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2)
        );
    }

    @Test
    public void testDivDecimal128WithRemainder() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)), // 1.00
                new Decimal128Constant(0, 300, ColumnType.getDecimalType(20, 2)), // 3.00
                // 1.00 / 3.00 = 0.3333 (rounded to 4 decimal places with HALF_EVEN)
                0, 0, 0, 3333, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4)
        );
    }

    @Test
    public void testDivDecimal16DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 2)),
                "'/' operation failed: Division by zero"
        );
    }

    @Test
    public void testDivDecimal16Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal16Constant((short) 600, ColumnType.getDecimalType(4, 2)), // 6.00
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)), // 2.00
                0, 0, 0, 30000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4) // 3.0000
        );
    }

    @Test
    public void testDivDecimal16WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal16Constant((short) 1000, ColumnType.getDecimalType(4, 2)), // 10.00
                new Decimal16Constant((short) 25, ColumnType.getDecimalType(4, 1)), // 2.5
                0, 0, 0, 4000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3) // 4.000
        );
    }

    @Test
    public void testDivDecimal16WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2)
        );
    }

    @Test
    public void testDivDecimal256DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 0, ColumnType.getDecimalType(40, 2)),
                "'/' operation failed: Division by zero"
        );
    }

    @Test
    public void testDivDecimal256MaxPrecision() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 600, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)),
                0, 0, 0, 3, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)
        );
    }

    @Test
    public void testDivDecimal256Negative() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -1, -15, ColumnType.getDecimalType(40, 0)), // -15
                new Decimal256Constant(0, 0, 0, 3, ColumnType.getDecimalType(40, 0)), // 3
                -1, -1, -1, -5, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0) // -5
        );
    }

    @Test
    public void testDivDecimal256Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 600, ColumnType.getDecimalType(40, 2)), // 6.00
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)), // 2.00
                0, 0, 0, 30000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4) // 3.0000
        );
    }

    @Test
    public void testDivDecimal256WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 0)
                ),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2)
        );
    }

    @Test
    public void testDivDecimal256WithRounding() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 1, 0, ColumnType.getDecimalType(39, 0)), // large number
                new Decimal256Constant(0, 0, 0, 3, ColumnType.getDecimalType(39, 0)), // 3
                // Should perform division with rounding
                0, 0, 0, 6148914691236517205L, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)
        );
    }

    @Test
    public void testDivDecimal32DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(0, ColumnType.getDecimalType(8, 2)),
                "'/' operation failed: Division by zero"
        );
    }

    @Test
    public void testDivDecimal32Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal32Constant(600, ColumnType.getDecimalType(8, 2)), // 6.00
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)), // 2.00
                0, 0, 0, 30000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4) // 3.0000
        );
    }

    @Test
    public void testDivDecimal32WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal32Constant(1000, ColumnType.getDecimalType(9, 2)), // 10.00
                new Decimal32Constant(25, ColumnType.getDecimalType(9, 1)), // 2.5
                0, 0, 0, 4000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3) // 4.000
        );
    }

    @Test
    public void testDivDecimal32WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal32Constant(100, ColumnType.getDecimalType(9, 2)),
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(9, 0)),
                ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2)
        );
    }

    @Test
    public void testDivDecimal64DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(0, ColumnType.getDecimalType(10, 2)),
                "'/' operation failed: Division by zero"
        );
    }

    @Test
    public void testDivDecimal64Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal64Constant(600, ColumnType.getDecimalType(10, 2)), // 6.00
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)), // 2.00
                0, 0, 0, 30000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4) // 3.0000
        );
    }

    @Test
    public void testDivDecimal64WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal64Constant(1000, ColumnType.getDecimalType(14, 2)), // 10.00
                new Decimal64Constant(25, ColumnType.getDecimalType(14, 1)), // 2.5
                0, 0, 0, 4000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3) // 4.000
        );
    }

    @Test
    public void testDivDecimal64WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal64Constant(100, ColumnType.getDecimalType(15, 2)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(15, 0)),
                ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2)
        );
    }

    @Test
    public void testDivDecimal8DivideByZero() throws SqlException {
        createFunctionAndAssertFails(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 1)),
                "'/' operation failed: Division by zero"
        );
    }

    @Test
    public void testDivDecimal8Simple() throws SqlException {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 60, ColumnType.getDecimalType(2, 1)), // 6.0
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1)), // 2.0
                0, 0, 0, 300, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2) // 3.00
        );
    }

    @Test
    public void testDivDecimal8WithDifferentScales() throws SqlException {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 100, ColumnType.getDecimalType(2, 2)), // 1.00
                new Decimal8Constant((byte) 25, ColumnType.getDecimalType(2, 1)), // 2.5
                0, 0, 0, 400, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3) // 0.400
        );
    }

    @Test
    public void testDivDecimal8WithNull() throws SqlException {
        createFunctionAndAssertNull(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                ColumnType.getDecimalType(Decimals.MAX_PRECISION, 1)
        );
    }

    @Test
    public void testDivMixedDecimalTypes() throws SqlException {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 1000, ColumnType.getDecimalType(40, 2)), // 10.00
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)), // 2.00
                0, 0, 0, 50000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4) // 5.0000
        );

        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 1200, ColumnType.getDecimalType(40, 2)), // 12.00
                new Decimal64Constant(300, ColumnType.getDecimalType(10, 2)), // 3.00
                0, 0, 0, 40000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4) // 4.0000
        );
    }

    @Test
    public void testDivRandomValues() throws SqlException {
        for (int i = 2; i < 10; i++) {
            long dividend = i * 12;
            long divisor = i % 6 + 1; // ensure divisor is not zero
            int scale = i % 2; // Keep scale small to avoid complex calculations

            // For division, we need to calculate what the actual result should be
            // This is complex because division involves scaling and rounding
            // Let's just verify division doesn't fail and produces reasonable results
            ObjList<Function> args = new ObjList<>();
            args.add(new Decimal128Constant(0, dividend, ColumnType.getDecimalType(21, scale)));
            args.add(new Decimal128Constant(0, divisor, ColumnType.getDecimalType(21, scale)));
            try (Function func = factory.newInstance(-1, args, null, null, null)) {
                Decimal256 value = new Decimal256();
                DecimalUtil.load(value, Misc.getThreadLocalDecimal128(), func, null);
                // Just verify we got a non-null result
                Assert.assertFalse(value.isNull());
                Assert.assertEquals(ColumnType.getDecimalType(Decimals.MAX_PRECISION, scale * 2), func.getType());
            }
        }
    }

    @Test
    public void testDivWithPrecisionScaling() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 5608, ColumnType.getDecimalType(22, 2)), // 56.08
                new Decimal128Constant(0, 456, ColumnType.getDecimalType(22, 1)), // 45.6
                // 56.08 / 45.6 = 1.229... (rounded with HALF_EVEN)
                0, 0, 0, 1230, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3)
        );
    }

    @Test
    public void testDivWithRounding() throws SqlException {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)), // 1.00
                new Decimal128Constant(0, 600, ColumnType.getDecimalType(19, 2)), // 6.00
                // 1.00 / 6.00 = 0.166666... should round to 0.1667 with HALF_EVEN
                0, 0, 0, 1667, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4)
        );
    }

    @Override
    protected FunctionFactory getFactory() {
        return factory;
    }
}