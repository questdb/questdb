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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.lt.LtDecimalFunctionFactory;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LtDecimalFunctionFactoryTest extends AbstractCairoTest {
    private static final LtDecimalFunctionFactory factory = new LtDecimalFunctionFactory();
    private final ObjList<Function> args = new ObjList<>();

    @Test
    public void testLtDecimal128DifferentValues() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal128EqualValues() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal128MaxValue() {
        createFunctionAndAssert(
                new Decimal128Constant(Long.MAX_VALUE, Long.MAX_VALUE - 1, ColumnType.getDecimalType(37, 0)),
                new Decimal128Constant(Long.MAX_VALUE, Long.MAX_VALUE, ColumnType.getDecimalType(37, 0)),
                true
        );
    }

    @Test
    public void testLtDecimal128MinValue() {
        createFunctionAndAssert(
                new Decimal128Constant(Long.MIN_VALUE, Long.MIN_VALUE, ColumnType.getDecimalType(37, 0)),
                new Decimal128Constant(Long.MIN_VALUE, Long.MIN_VALUE + 1, ColumnType.getDecimalType(37, 0)),
                true
        );
    }

    @Test
    public void testLtDecimal128NegativeValues() {
        createFunctionAndAssert(
                new Decimal128Constant(-1, -200, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(-1, -100, ColumnType.getDecimalType(20, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal128NegativeVsPositive() {
        createFunctionAndAssert(
                new Decimal128Constant(-1, -100, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal128NullVsValue() {
        createFunctionAndAssert(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal128VsDecimal256() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal128WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 10, ColumnType.getDecimalType(20, 1)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal128Constant(0, 1000, ColumnType.getDecimalType(20, 3)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal128WithNull() {
        createFunctionAndAssert(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal128Zero() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 0, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 1, ColumnType.getDecimalType(20, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal16DifferentValues() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal16EqualValues() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal16MaxValue() {
        createFunctionAndAssert(
                new Decimal16Constant((short) (Short.MAX_VALUE - 1), ColumnType.getDecimalType(4, 0)),
                new Decimal16Constant(Short.MAX_VALUE, ColumnType.getDecimalType(4, 0)),
                true
        );
    }

    @Test
    public void testLtDecimal16NegativeValues() {
        createFunctionAndAssert(
                new Decimal16Constant((short) -200, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) -100, ColumnType.getDecimalType(4, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal16NullVsValue() {
        createFunctionAndAssert(
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal16VsDecimal128() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal16VsDecimal256() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal16VsDecimal32() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal16VsDecimal64() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal16WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 10, ColumnType.getDecimalType(4, 1)),
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 1000, ColumnType.getDecimalType(4, 3)),
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal16WithNull() {
        createFunctionAndAssert(
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal16Zero() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) 1, ColumnType.getDecimalType(4, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal256DifferentValues() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal256EqualValues() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal256NegativeValues() {
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -1, -200, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(-1, -1, -1, -100, ColumnType.getDecimalType(40, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal256NegativeVsPositive() {
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -1, -100, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal256NullVsValue() {
        createFunctionAndAssert(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 0)
                ),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal256WithCarry() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, Long.MAX_VALUE, ColumnType.getDecimalType(40, 0)),
                new Decimal256Constant(0, 0, 1, 0, ColumnType.getDecimalType(40, 0)),
                true
        );
    }

    @Test
    public void testLtDecimal256WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 10, ColumnType.getDecimalType(40, 1)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 1000, ColumnType.getDecimalType(40, 3)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal256WithNull() {
        createFunctionAndAssert(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 0)
                ),
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 0)
                ),
                false
        );
    }

    @Test
    public void testLtDecimal256Zero() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 0, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 1, ColumnType.getDecimalType(40, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal32DifferentValues() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal32EqualValues() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal32MaxValue() {
        createFunctionAndAssert(
                new Decimal32Constant(Integer.MAX_VALUE - 1, ColumnType.getDecimalType(9, 0)),
                new Decimal32Constant(Integer.MAX_VALUE, ColumnType.getDecimalType(9, 0)),
                true
        );
    }

    @Test
    public void testLtDecimal32NegativeValues() {
        createFunctionAndAssert(
                new Decimal32Constant(-200, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(-100, ColumnType.getDecimalType(8, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal32NullVsValue() {
        createFunctionAndAssert(
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 0)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal32VsDecimal128() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal32VsDecimal256() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal32VsDecimal64() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal32WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal32Constant(10, ColumnType.getDecimalType(8, 1)),
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal32Constant(1000, ColumnType.getDecimalType(8, 3)),
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal32WithNull() {
        createFunctionAndAssert(
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 0)),
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal32Zero() {
        createFunctionAndAssert(
                new Decimal32Constant(0, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(1, ColumnType.getDecimalType(8, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal64DifferentValues() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal64EqualValues() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal64LargeValues() {
        createFunctionAndAssert(
                new Decimal64Constant(1000000, ColumnType.getDecimalType(15, 2)),
                new Decimal64Constant(2000000, ColumnType.getDecimalType(15, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal64Constant(2000000, ColumnType.getDecimalType(15, 2)),
                new Decimal64Constant(1000000, ColumnType.getDecimalType(15, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal64MaxValue() {
        createFunctionAndAssert(
                new Decimal64Constant(Long.MAX_VALUE - 1, ColumnType.getDecimalType(18, 0)),
                new Decimal64Constant(Long.MAX_VALUE, ColumnType.getDecimalType(18, 0)),
                true
        );
    }

    @Test
    public void testLtDecimal64NegativeValues() {
        createFunctionAndAssert(
                new Decimal64Constant(-200, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(-100, ColumnType.getDecimalType(10, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal64NullVsValue() {
        createFunctionAndAssert(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 0)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal64VsDecimal128() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal64VsDecimal256() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal64WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal64Constant(10, ColumnType.getDecimalType(10, 1)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal64Constant(1000, ColumnType.getDecimalType(10, 3)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal64WithNull() {
        createFunctionAndAssert(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 0)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal64Zero() {
        createFunctionAndAssert(
                new Decimal64Constant(0, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(1, ColumnType.getDecimalType(10, 2)),
                true
        );
    }

    @Test
    public void testLtDecimal8DifferentValues() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1)),
                true
        );
    }

    @Test
    public void testLtDecimal8EqualValues() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                false
        );
    }

    @Test
    public void testLtDecimal8MaxValue() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) (Byte.MAX_VALUE - 1), ColumnType.getDecimalType(2, 0)),
                new Decimal8Constant(Byte.MAX_VALUE, ColumnType.getDecimalType(2, 0)),
                true
        );
    }

    @Test
    public void testLtDecimal8NegativeValues() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) -20, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) -10, ColumnType.getDecimalType(2, 1)),
                true
        );
    }

    @Test
    public void testLtDecimal8NullVsValue() {
        createFunctionAndAssert(
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal8VsDecimal128() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal8VsDecimal16() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1)),
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal8VsDecimal256() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal8VsDecimal32() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal8VsDecimal64() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal8WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 2)),
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(2, 1)),
                false
        );
    }

    @Test
    public void testLtDecimal8WithNull() {
        createFunctionAndAssert(
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                false
        );
    }

    @Test
    public void testLtDecimal8Zero() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(2, 1)),
                true
        );
    }

    @Test
    public void testLtRandomValues() {
        for (int i = 1; i < 50; i++) {
            long val = i * 13;
            int scale = i % 3;

            createFunctionAndAssert(
                    new Decimal128Constant(0, val, ColumnType.getDecimalType(21, scale)),
                    new Decimal128Constant(0, val + 1, ColumnType.getDecimalType(21, scale)),
                    true
            );
            createFunctionAndAssert(
                    new Decimal128Constant(0, val + 1, ColumnType.getDecimalType(21, scale)),
                    new Decimal128Constant(0, val, ColumnType.getDecimalType(21, scale)),
                    false
            );
        }
    }

    private void createFunctionAndAssert(Function left, Function right, boolean expected) {
        args.clear();
        args.add(left);
        args.add(right);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            boolean result = func.getBool(null);
            Assert.assertEquals(expected, result);
        }

        // Check that left => right returns the opposite result (except for null cases)
        if (isNonNull(left) && isNonNull(right)) {
            try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
                ((NegatableBooleanFunction) func).setNegated();
                boolean result = func.getBool(null);
                Assert.assertEquals(!expected, result);
            }
        } else {
            // Check that right < left is false in the null case
            args.clear();
            args.add(right);
            args.add(left);
            try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
                boolean result = func.getBool(null);
                Assert.assertFalse(expected);
                Assert.assertFalse(result);
            }
        }
    }

    private boolean isNonNull(Function func) {
        Assert.assertTrue(func.isConstant());
        return !func.isNullConstant();
    }
}
