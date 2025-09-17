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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.math.MulDecimalFunctionFactory;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;
import org.junit.Test;

public class MulDecimalFunctionFactoryTest extends ArithmeticDecimalFunctionFactoryTest {
    private static final MulDecimalFunctionFactory factory = new MulDecimalFunctionFactory();

    @Test
    public void testMulByOne() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 12345, ColumnType.getDecimalType(19, 3))); // 12.345
        args.add(new Decimal128Constant(0, 1000, ColumnType.getDecimalType(19, 3))); // 1.000
        createFunctionAndAssert(args, 0, 0, 0, 12345000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 6)); // 12.345000
    }

    @Test
    public void testMulByZero() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2))); // 1.00
        args.add(new Decimal128Constant(0, 0, ColumnType.getDecimalType(19, 2))); // 0.00
        createFunctionAndAssert(args, 0, 0, 0, 0, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4)); // 0.0000
    }

    @Test
    public void testMulDecimal128Overflow() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(Long.MAX_VALUE, Long.MAX_VALUE, ColumnType.getDecimalType(19, 0)));
        args.add(new Decimal128Constant(Long.MAX_VALUE, Long.MAX_VALUE, ColumnType.getDecimalType(19, 0)));
        createFunctionAndAssertFails(args, "'*' operation failed: Overflow in multiplication: result exceeds maximum precision");
    }

    @Test
    public void testMulDecimal128Simple() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 300, ColumnType.getDecimalType(20, 2))); // 3.00
        args.add(new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 60000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4)); // 6.0000
    }

    @Test
    public void testMulDecimal128WithCarry() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 100000, ColumnType.getDecimalType(20, 3))); // 100.000
        args.add(new Decimal128Constant(0, 1000, ColumnType.getDecimalType(20, 3))); // 1.000
        createFunctionAndAssert(args, 0, 0, 0, 100000000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 6)); // 100.000000
    }

    @Test
    public void testMulDecimal128WithDifferentScales() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 250, ColumnType.getDecimalType(19, 2))); // 2.50
        args.add(new Decimal128Constant(0, 40, ColumnType.getDecimalType(19, 1))); // 4.0
        createFunctionAndAssert(args, 0, 0, 0, 10000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3)); // 10.000
    }

    @Test
    public void testMulDecimal128WithNull() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)));
        args.add(new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2));
    }

    @Test
    public void testMulDecimal16Simple() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 300, ColumnType.getDecimalType(4, 2))); // 3.00
        args.add(new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 60000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4)); // 6.0000
    }

    @Test
    public void testMulDecimal16WithDifferentScales() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 250, ColumnType.getDecimalType(4, 2))); // 2.50
        args.add(new Decimal16Constant((short) 40, ColumnType.getDecimalType(4, 1))); // 4.0
        createFunctionAndAssert(args, 0, 0, 0, 10000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3)); // 10.000
    }

    @Test
    public void testMulDecimal16WithNull() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)));
        args.add(new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2));
    }

    @Test
    public void testMulDecimal256MaxPrecision() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, 3, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)));
        args.add(new Decimal256Constant(0, 0, 0, 2, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)));
        createFunctionAndAssert(args, 0, 0, 0, 6, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0));
    }

    @Test
    public void testMulDecimal256Negative() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(-1, -1, -1, -5, ColumnType.getDecimalType(40, 0))); // -5
        args.add(new Decimal256Constant(0, 0, 0, 3, ColumnType.getDecimalType(40, 0))); // 3
        createFunctionAndAssert(args, -1, -1, -1, -15, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)); // -15
    }

    @Test
    public void testMulDecimal256Overflow() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)));
        args.add(new Decimal256Constant(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)));
        createFunctionAndAssertFails(args, "'*' operation failed: Overflow in multiplication (256-bit × 256-bit): product exceeds 256-bit capacity");
    }

    @Test
    public void testMulDecimal256Simple() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, 300, ColumnType.getDecimalType(40, 2))); // 3.00
        args.add(new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 60000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4)); // 6.0000
    }

    @Test
    public void testMulDecimal256WithCarry() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 1, 0, ColumnType.getDecimalType(39, 0))); // large number
        args.add(new Decimal256Constant(0, 0, 0, 2, ColumnType.getDecimalType(39, 0))); // 2
        createFunctionAndAssert(args, 0, 0, 2, 0, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 0)); // doubled
    }

    @Test
    public void testMulDecimal256WithNull() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                ColumnType.getDecimalType(40, 0))
        );
        args.add(new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2));
    }

    @Test
    public void testMulDecimal32Simple() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(300, ColumnType.getDecimalType(8, 2))); // 3.00
        args.add(new Decimal32Constant(200, ColumnType.getDecimalType(8, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 60000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4)); // 6.0000
    }

    @Test
    public void testMulDecimal32WithDifferentScales() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(250, ColumnType.getDecimalType(9, 2))); // 2.50
        args.add(new Decimal32Constant(40, ColumnType.getDecimalType(9, 1))); // 4.0
        createFunctionAndAssert(args, 0, 0, 0, 10000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3)); // 10.000
    }

    @Test
    public void testMulDecimal32WithNull() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(100, ColumnType.getDecimalType(9, 2)));
        args.add(new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(9, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2));
    }

    @Test
    public void testMulDecimal64Simple() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(300, ColumnType.getDecimalType(10, 2))); // 3.00
        args.add(new Decimal64Constant(200, ColumnType.getDecimalType(10, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 60000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4)); // 6.0000
    }

    @Test
    public void testMulDecimal64WithDifferentScales() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(250, ColumnType.getDecimalType(14, 2))); // 2.50
        args.add(new Decimal64Constant(40, ColumnType.getDecimalType(14, 1))); // 4.0
        createFunctionAndAssert(args, 0, 0, 0, 10000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3)); // 10.000
    }

    @Test
    public void testMulDecimal64WithNull() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(100, ColumnType.getDecimalType(15, 2)));
        args.add(new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(15, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2));
    }

    @Test
    public void testMulDecimal8Simple() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 30, ColumnType.getDecimalType(2, 1))); // 3.0
        args.add(new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1))); // 2.0
        createFunctionAndAssert(args, 0, 0, 0, 600, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 2)); // 6.00
    }

    @Test
    public void testMulDecimal8WithDifferentScales() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 25, ColumnType.getDecimalType(2, 2))); // 0.25
        args.add(new Decimal8Constant((byte) 40, ColumnType.getDecimalType(2, 1))); // 4.0
        createFunctionAndAssert(args, 0, 0, 0, 1000, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3)); // 1.000
    }

    @Test
    public void testMulDecimal8WithNull() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)));
        args.add(new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 1));
    }

    @Test
    public void testMulMixedDecimalTypes() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(1, 1, 1, 5, ColumnType.getDecimalType(40, 2)));
        args.add(new Decimal128Constant(0, 2, ColumnType.getDecimalType(20, 2)));
        createFunctionAndAssert(args, 2, 2, 2, 10, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4));

        args.clear();
        args.add(new Decimal256Constant(1, 1, 1, 3, ColumnType.getDecimalType(40, 2)));
        args.add(new Decimal64Constant(3, ColumnType.getDecimalType(10, 2)));
        createFunctionAndAssert(args, 3, 3, 3, 9, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 4));
    }

    @Test
    public void testMulRandomValues() throws SqlException {
        for (int i = 1; i < 20; i++) {
            long val1 = i * 5;
            long val2 = i * 3;
            long result = val1 * val2;
            int scale = i % 3;

            ObjList<Function> args = new ObjList<>();
            args.add(new Decimal128Constant(0, val1, ColumnType.getDecimalType(21, scale)));
            args.add(new Decimal128Constant(0, val2, ColumnType.getDecimalType(21, scale)));
            createFunctionAndAssert(args, 0, 0, 0, result, ColumnType.getDecimalType(Decimals.MAX_PRECISION, scale * 2));
        }
    }

    @Test
    public void testMulWithPrecisionScaling() throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 123, ColumnType.getDecimalType(22, 2))); // 1.23
        args.add(new Decimal128Constant(0, 456, ColumnType.getDecimalType(22, 1))); // 45.6
        // 1.23 * 45.6 = 56.088
        createFunctionAndAssert(args, 0, 0, 0, 56088, ColumnType.getDecimalType(Decimals.MAX_PRECISION, 3));
    }

    @Override
    protected FunctionFactory getFactory() {
        return factory;
    }
}