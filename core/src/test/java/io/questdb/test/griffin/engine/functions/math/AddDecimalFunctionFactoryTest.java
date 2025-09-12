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
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.math.AddDecimalFunctionFactory;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class AddDecimalFunctionFactoryTest extends AbstractTest {
    private static final AddDecimalFunctionFactory factory = new AddDecimalFunctionFactory();

    @Test
    public void testAddDecimal128Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2))); // 1.00
        args.add(new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 300, ColumnType.getDecimalType(21, 2));
    }

    @Test
    public void testAddDecimal128WithCarry() {
        // Test values that require carry operation
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, -1, ColumnType.getDecimalType(20, 2)));
        args.add(new Decimal128Constant(0, 1, ColumnType.getDecimalType(20, 2)));
        createFunctionAndAssert(args, 0, 0, 1, 0, ColumnType.getDecimalType(21, 2));
    }

    @Test
    public void testAddDecimal128WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 150, ColumnType.getDecimalType(19, 2))); // 1.50
        args.add(new Decimal128Constant(0, 5, ColumnType.getDecimalType(19, 1))); // 0.5
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(20, 2));
    }

    @Test
    public void testAddDecimal128WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 100, ColumnType.getDecimalType(19, 2)));
        args.add(new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(21, 2));
    }

    @Test
    public void testAddDecimal16Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2))); // 1.00
        args.add(new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 300, ColumnType.getDecimalType(5, 2));
    }

    @Test
    public void testAddDecimal16WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 150, ColumnType.getDecimalType(4, 2))); // 1.50
        args.add(new Decimal16Constant((short) 5, ColumnType.getDecimalType(4, 1))); // 0.5
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(5, 2));
    }

    @Test
    public void testAddDecimal16WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)));
        args.add(new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(5, 2));
    }

    @Test
    public void testAddDecimal256MaxPrecision() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)));
        args.add(new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE)));
        createFunctionAndAssert(args, 0, 0, 0, 300, ColumnType.getDecimalType(Decimals.MAX_PRECISION, Decimals.MAX_SCALE));
    }

    @Test
    public void testAddDecimal256Negative() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, 1, ColumnType.getDecimalType(40, 0)));
        args.add(new Decimal256Constant(-1, -1, -1, -1, ColumnType.getDecimalType(40, 0)));
        createFunctionAndAssert(args, 0, 0, 0, 0, ColumnType.getDecimalType(41, 0));
    }

    @Test
    public void testAddDecimal256Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2))); // 1.00
        args.add(new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 300, ColumnType.getDecimalType(41, 2));
    }

    @Test
    public void testAddDecimal256WithCarry() {
        // Test values that require carry operation
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(0, 0, 0, -1, ColumnType.getDecimalType(39, 0)));
        args.add(new Decimal256Constant(0, 0, 0, 1, ColumnType.getDecimalType(39, 0)));
        createFunctionAndAssert(args, 0, 0, 1, 0, ColumnType.getDecimalType(40, 0));
    }

    @Test
    public void testAddDecimal256WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                ColumnType.getDecimalType(40, 0))
        );
        args.add(new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(41, 2));
    }

    @Test
    public void testAddDecimal32Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(100, ColumnType.getDecimalType(8, 2))); // 1.00
        args.add(new Decimal32Constant(200, ColumnType.getDecimalType(8, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 300, ColumnType.getDecimalType(9, 2));
    }

    @Test
    public void testAddDecimal32WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(150, ColumnType.getDecimalType(9, 2))); // 1.50
        args.add(new Decimal32Constant(5, ColumnType.getDecimalType(9, 1))); // 0.5
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(10, 2));
    }

    @Test
    public void testAddDecimal32WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal32Constant(100, ColumnType.getDecimalType(9, 2)));
        args.add(new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(9, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(10, 2));
    }

    @Test
    public void testAddDecimal64Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(100, ColumnType.getDecimalType(10, 2))); // 1.00
        args.add(new Decimal64Constant(200, ColumnType.getDecimalType(10, 2))); // 2.00
        createFunctionAndAssert(args, 0, 0, 0, 300, ColumnType.getDecimalType(11, 2));
    }

    @Test
    public void testAddDecimal64WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(150, ColumnType.getDecimalType(14, 2))); // 1.50
        args.add(new Decimal64Constant(5, ColumnType.getDecimalType(14, 1))); // 0.5
        createFunctionAndAssert(args, 0, 0, 0, 200, ColumnType.getDecimalType(15, 2));
    }

    @Test
    public void testAddDecimal64WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal64Constant(100, ColumnType.getDecimalType(15, 2)));
        args.add(new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(15, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(16, 2));
    }

    @Test
    public void testAddDecimal8Simple() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1))); // 1.0
        args.add(new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1))); // 2.0
        createFunctionAndAssert(args, 0, 0, 0, 30, ColumnType.getDecimalType(3, 1));
    }

    @Test
    public void testAddDecimal8WithDifferentScales() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 15, ColumnType.getDecimalType(2, 2))); // 0.15
        args.add(new Decimal8Constant((byte) 5, ColumnType.getDecimalType(2, 1))); // 0.5
        createFunctionAndAssert(args, 0, 0, 0, 65, ColumnType.getDecimalType(3, 2));
    }

    @Test
    public void testAddDecimal8WithNull() {
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)));
        args.add(new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)));
        createFunctionAndAssertNull(args, ColumnType.getDecimalType(3, 1));
    }

    @Test
    public void testAddMixedDecimalTypes() {
        // Test adding different decimal sizes
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)));
        args.add(new Decimal128Constant(1, 1, ColumnType.getDecimalType(20, 2)));
        createFunctionAndAssert(args, 1, 1, 2, 2, ColumnType.getDecimalType(41, 2));

        args.clear();
        args.add(new Decimal256Constant(1, 1, 1, 1, ColumnType.getDecimalType(40, 2)));
        args.add(new Decimal64Constant(1, ColumnType.getDecimalType(10, 2)));
        createFunctionAndAssert(args, 1, 1, 1, 2, ColumnType.getDecimalType(41, 2));
    }

    @Test
    public void testAddRandomValues() {
        // Test with some random but predictable values
        for (int i = 1; i < 50; i++) {
            long val1 = i * 13;
            long val2 = i * 7;
            int scale = i % 3;

            ObjList<Function> args = new ObjList<>();
            args.add(new Decimal128Constant(0, val1, ColumnType.getDecimalType(21, scale)));
            args.add(new Decimal128Constant(0, val2, ColumnType.getDecimalType(21, scale)));
            createFunctionAndAssert(args, 0, 0, 0, val1 + val2, ColumnType.getDecimalType(22, scale));
        }
    }

    @Test
    public void testAddWithPrecisionScaling() {
        // Test that precision and scale are handled correctly
        ObjList<Function> args = new ObjList<>();
        args.add(new Decimal128Constant(0, 1234, ColumnType.getDecimalType(22, 2)));
        args.add(new Decimal128Constant(0, 567, ColumnType.getDecimalType(22, 1)));
        // 12.34 + 56.7 = 69.04
        createFunctionAndAssert(args, 0, 0, 0, 6904, ColumnType.getDecimalType(23, 2));
    }

    private void createFunctionAndAssert(ObjList<Function> args, long hh, long hl, long lh, long ll, int expectedType) {
        try (Function func = factory.newInstance(-1, args, null, null, null)) {
            Assert.assertEquals(hh, func.getDecimal256HH(null));
            Assert.assertEquals(hl, func.getDecimal256HL(null));
            Assert.assertEquals(lh, func.getDecimal256LH(null));
            Assert.assertEquals(ll, func.getDecimal256LL(null));
            Assert.assertEquals(expectedType, func.getType());
        }
    }

    private void createFunctionAndAssertNull(ObjList<Function> args, int expectedType) {
        createFunctionAndAssert(
                args,
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                expectedType
        );
    }
}
