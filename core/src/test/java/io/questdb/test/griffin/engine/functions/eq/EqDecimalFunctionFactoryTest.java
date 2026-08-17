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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.columns.DecimalColumn;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.eq.EqDecimalFunctionFactory;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class EqDecimalFunctionFactoryTest extends AbstractCairoTest {
    // widest precision of each decimal storage width
    private static final int[] WIDTH_PRECISIONS = {2, 4, 9, 18, 38, 76};
    private final ObjList<Function> args = new ObjList<>();
    private final EqDecimalFunctionFactory factory = new EqDecimalFunctionFactory();

    @Test
    public void testEqDecimal128DifferentValues() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal128EqualValues() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal128MaxValue() {
        createFunctionAndAssert(
                new Decimal128Constant(Long.MAX_VALUE, Long.MAX_VALUE, ColumnType.getDecimalType(37, 0)),
                new Decimal128Constant(Long.MAX_VALUE, Long.MAX_VALUE, ColumnType.getDecimalType(37, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal128MinValue() {
        createFunctionAndAssert(
                new Decimal128Constant(Long.MIN_VALUE, Long.MIN_VALUE, ColumnType.getDecimalType(37, 0)),
                new Decimal128Constant(Long.MIN_VALUE, Long.MIN_VALUE, ColumnType.getDecimalType(37, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal128NegativeValues() {
        createFunctionAndAssert(
                new Decimal128Constant(-1, -100, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(-1, -100, ColumnType.getDecimalType(20, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal128NegativeVsPositive() {
        createFunctionAndAssert(
                new Decimal128Constant(-1, -100, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal128NullVsValue() {
        createFunctionAndAssert(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 0)),
                false
        );
    }

    @Test
    public void testEqDecimal128SlowPathBothNull() {
        // Different scales force the slow path (Decimal128Func extends
        // CompareDecimal128Function). Equality must keep its QuestDB-specific
        // semantic that NULL = NULL is true, matching the UnscaledDecimal128Func
        // fast path.
        createFunctionAndAssert(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 4)),
                true
        );
    }

    @Test
    public void testEqDecimal128SlowPathNegatedBothNullIsFalse() {
        // Negated `=` (i.e. `!=`) on the Decimal128 slow path must mirror the
        // fast-path semantic: NULL = NULL is true, so NULL != NULL is false.
        assertNegated(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 4)),
                false
        );
    }

    @Test
    public void testEqDecimal128SlowPathNegatedNullVsValueIsTrue() {
        // `!=` of NULL and a non-null value is true on the slow path.
        assertNegated(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 4)),
                true
        );
    }

    @Test
    public void testEqDecimal128SlowPathNullVsValue() {
        // Different scales force the slow path. NULL on one side compares unequal
        // to a non-null value, matching the fast-path bit-equality semantic.
        createFunctionAndAssert(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 4)),
                false
        );
    }

    @Test
    public void testEqDecimal128VsDecimal256() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal128WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 10, ColumnType.getDecimalType(20, 1)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal128Constant(0, 1000, ColumnType.getDecimalType(20, 3)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal128WithNull() {
        createFunctionAndAssert(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal128Zero() {
        createFunctionAndAssert(
                new Decimal128Constant(0, 0, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 0, ColumnType.getDecimalType(20, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal16DifferentValues() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal16EqualValues() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal16MaxValue() {
        createFunctionAndAssert(
                new Decimal16Constant(Short.MAX_VALUE, ColumnType.getDecimalType(4, 0)),
                new Decimal16Constant(Short.MAX_VALUE, ColumnType.getDecimalType(4, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal16MinValue() {
        createFunctionAndAssert(
                new Decimal16Constant(Short.MIN_VALUE, ColumnType.getDecimalType(4, 0)),
                new Decimal16Constant(Short.MIN_VALUE, ColumnType.getDecimalType(4, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal16NegativeValues() {
        createFunctionAndAssert(
                new Decimal16Constant((short) -100, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) -100, ColumnType.getDecimalType(4, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal16NullVsValue() {
        createFunctionAndAssert(
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 0)),
                false
        );
    }

    @Test
    public void testEqDecimal16VsDecimal128() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal16VsDecimal256() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal16VsDecimal32() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal16VsDecimal64() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal16WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 10, ColumnType.getDecimalType(4, 1)),
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal16Constant((short) 1000, ColumnType.getDecimalType(4, 3)),
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal16WithNull() {
        createFunctionAndAssert(
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(4, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal16Zero() {
        createFunctionAndAssert(
                new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 2)),
                new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal256DifferentValues() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal256EqualValues() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal256NegativeValues() {
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -1, -100, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(-1, -1, -1, -100, ColumnType.getDecimalType(40, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal256NegativeVsPositive() {
        createFunctionAndAssert(
                new Decimal256Constant(-1, -1, -1, -100, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal256NullVsValue() {
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
    public void testEqDecimal256SlowPathBothNull() {
        // Different scales force the slow path (Decimal256Func). NULL = NULL is true.
        createFunctionAndAssert(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 2)
                ),
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 4)
                ),
                true
        );
    }

    @Test
    public void testEqDecimal256SlowPathNegatedBothNullIsFalse() {
        // Negated `=` (i.e. `!=`) on the Decimal256 slow path must mirror the
        // fast-path semantic: NULL = NULL is true, so NULL != NULL is false.
        assertNegated(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 2)
                ),
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 4)
                ),
                false
        );
    }

    @Test
    public void testEqDecimal256SlowPathNegatedNullVsValueIsTrue() {
        // `!=` of NULL and a non-null value is true on the slow path.
        assertNegated(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 2)
                ),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 4)),
                true
        );
    }

    @Test
    public void testEqDecimal256SlowPathNullVsValue() {
        // Different scales force the slow path; NULL = value must be false.
        createFunctionAndAssert(
                new Decimal256Constant(
                        Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL,
                        ColumnType.getDecimalType(40, 2)
                ),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 4)),
                false
        );
    }

    @Test
    public void testEqDecimal256WithCarry() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 1, 0, ColumnType.getDecimalType(40, 0)),
                new Decimal256Constant(0, 0, 1, 0, ColumnType.getDecimalType(40, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal256WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 10, ColumnType.getDecimalType(40, 1)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 1000, ColumnType.getDecimalType(40, 3)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal256WithNull() {
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
                true
        );
    }

    @Test
    public void testEqDecimal256Zero() {
        createFunctionAndAssert(
                new Decimal256Constant(0, 0, 0, 0, ColumnType.getDecimalType(40, 2)),
                new Decimal256Constant(0, 0, 0, 0, ColumnType.getDecimalType(40, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal32DifferentValues() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal32EqualValues() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal32MaxValue() {
        createFunctionAndAssert(
                new Decimal32Constant(Integer.MAX_VALUE, ColumnType.getDecimalType(9, 0)),
                new Decimal32Constant(Integer.MAX_VALUE, ColumnType.getDecimalType(9, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal32MinValue() {
        createFunctionAndAssert(
                new Decimal32Constant(Integer.MIN_VALUE, ColumnType.getDecimalType(9, 0)),
                new Decimal32Constant(Integer.MIN_VALUE, ColumnType.getDecimalType(9, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal32NegativeValues() {
        createFunctionAndAssert(
                new Decimal32Constant(-100, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(-100, ColumnType.getDecimalType(8, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal32NullVsValue() {
        createFunctionAndAssert(
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 0)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 0)),
                false
        );
    }

    @Test
    public void testEqDecimal32VsDecimal128() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal32VsDecimal256() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal32VsDecimal64() {
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal32WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal32Constant(10, ColumnType.getDecimalType(8, 1)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal32Constant(1000, ColumnType.getDecimalType(8, 3)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal32WithNull() {
        createFunctionAndAssert(
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 0)),
                new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(8, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal32Zero() {
        createFunctionAndAssert(
                new Decimal32Constant(0, ColumnType.getDecimalType(8, 2)),
                new Decimal32Constant(0, ColumnType.getDecimalType(8, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal64DifferentValues() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal64EqualValues() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal64LargeValues() {
        createFunctionAndAssert(
                new Decimal64Constant(1000000, ColumnType.getDecimalType(15, 2)),
                new Decimal64Constant(1000000, ColumnType.getDecimalType(15, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal64Constant(1000000, ColumnType.getDecimalType(15, 2)),
                new Decimal64Constant(2000000, ColumnType.getDecimalType(15, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal64MaxValue() {
        createFunctionAndAssert(
                new Decimal64Constant(Long.MAX_VALUE, ColumnType.getDecimalType(18, 0)),
                new Decimal64Constant(Long.MAX_VALUE, ColumnType.getDecimalType(18, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal64MinValue() {
        createFunctionAndAssert(
                new Decimal64Constant(Long.MIN_VALUE, ColumnType.getDecimalType(18, 0)),
                new Decimal64Constant(Long.MIN_VALUE, ColumnType.getDecimalType(18, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal64NegativeValues() {
        createFunctionAndAssert(
                new Decimal64Constant(-100, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(-100, ColumnType.getDecimalType(10, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal64NullVsValue() {
        createFunctionAndAssert(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 0)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 0)),
                false
        );
    }

    @Test
    public void testEqDecimal64SlowPathBothNull() {
        // Different scales force the slow path (Decimal64Func). NULL = NULL is true.
        createFunctionAndAssert(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 4)),
                true
        );
    }

    @Test
    public void testEqDecimal64SlowPathNullVsValue() {
        // Different scales force the slow path; NULL = value must be false.
        createFunctionAndAssert(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 4)),
                false
        );
    }

    @Test
    public void testEqDecimal64VsDecimal128() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal64VsDecimal256() {
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal64WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal64Constant(10, ColumnType.getDecimalType(10, 1)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal64Constant(1000, ColumnType.getDecimalType(10, 3)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal64WithNull() {
        createFunctionAndAssert(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 0)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal64Zero() {
        createFunctionAndAssert(
                new Decimal64Constant(0, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(0, ColumnType.getDecimalType(10, 2)),
                true
        );
    }

    @Test
    public void testEqDecimal8DifferentValues() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 20, ColumnType.getDecimalType(2, 1)),
                false
        );
    }

    @Test
    public void testEqDecimal8EqualValues() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                true
        );
    }

    @Test
    public void testEqDecimal8MaxValue() {
        createFunctionAndAssert(
                new Decimal8Constant(Byte.MAX_VALUE, ColumnType.getDecimalType(2, 0)),
                new Decimal8Constant(Byte.MAX_VALUE, ColumnType.getDecimalType(2, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal8MinValue() {
        createFunctionAndAssert(
                new Decimal8Constant(Byte.MIN_VALUE, ColumnType.getDecimalType(2, 0)),
                new Decimal8Constant(Byte.MIN_VALUE, ColumnType.getDecimalType(2, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal8NegativeValues() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) -10, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) -10, ColumnType.getDecimalType(2, 1)),
                true
        );
    }

    @Test
    public void testEqDecimal8NullVsValue() {
        createFunctionAndAssert(
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 0)),
                false
        );
    }

    @Test
    public void testEqDecimal8VsDecimal128() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal128Constant(0, 200, ColumnType.getDecimalType(20, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal8VsDecimal16() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal16Constant((short) 100, ColumnType.getDecimalType(4, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal16Constant((short) 200, ColumnType.getDecimalType(4, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal8VsDecimal256() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal256Constant(0, 0, 0, 100, ColumnType.getDecimalType(40, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal256Constant(0, 0, 0, 200, ColumnType.getDecimalType(40, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal8VsDecimal32() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal32Constant(100, ColumnType.getDecimalType(8, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal32Constant(200, ColumnType.getDecimalType(8, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal8VsDecimal64() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 1)),
                new Decimal64Constant(200, ColumnType.getDecimalType(10, 2)),
                false
        );
    }

    @Test
    public void testEqDecimal8WithDifferentScales() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 2)),
                true
        );
        createFunctionAndAssert(
                new Decimal8Constant((byte) 10, ColumnType.getDecimalType(2, 2)),
                new Decimal8Constant((byte) 1, ColumnType.getDecimalType(2, 1)),
                true
        );
    }

    @Test
    public void testEqDecimal8WithNull() {
        createFunctionAndAssert(
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 0)),
                true
        );
    }

    @Test
    public void testEqDecimal8Zero() {
        createFunctionAndAssert(
                new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 1)),
                new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 1)),
                true
        );
    }

    @Test
    public void testEqRandomValues() {
        for (int i = 1; i < 50; i++) {
            long val = i * 13;
            int scale = i % 3;

            createFunctionAndAssert(
                    new Decimal128Constant(0, val, ColumnType.getDecimalType(21, scale)),
                    new Decimal128Constant(0, val, ColumnType.getDecimalType(21, scale)),
                    true
            );
            createFunctionAndAssert(
                    new Decimal128Constant(0, val, ColumnType.getDecimalType(21, scale)),
                    new Decimal128Constant(0, val + 1, ColumnType.getDecimalType(21, scale)),
                    false
            );
        }
    }

    @Test
    public void testEqScaleAlignmentAcrossJoinedTables() throws Exception {
        // decimals can only key a join when both types match exactly, so mixed scales reach `=` as a
        // cross join predicate instead
        assertQuery("select l.id lid, r.id rid, l.a = r.b eq from eq_join_l l cross join eq_join_r r order by lid, rid")
                .ddl(
                        "create table eq_join_l (id int, a decimal(38,0))",
                        "insert into eq_join_l values (1, " + nines(38) + "m), (2, 1m)",
                        "create table eq_join_r (id int, b decimal(38,1))",
                        "insert into eq_join_r values (1, " + nines(37) + ".9m), (2, 1.0m)"
                )
                .expectSize()
                .returns("lid\trid\teq\n" +
                        "1\t1\tfalse\n" +
                        "1\t2\tfalse\n" +
                        "2\t1\tfalse\n" +
                        "2\t2\ttrue\n");
    }

    @Test
    public void testEqScaleAlignmentAcrossWidths() throws Exception {
        for (int leftPrecision : WIDTH_PRECISIONS) {
            for (int rightPrecision : WIDTH_PRECISIONS) {
                assertEqualityAtWidths(leftPrecision, rightPrecision);
            }
        }
    }

    @Test
    public void testEqScaleAlignmentMaxPrecisionRescaleOverflow() throws Exception {
        // scaling a 76-digit operand up by one leaves the Decimal256 range, so the ordering
        // comes from compareTo's guard rather than from the aligned values
        assertQuery("select a = b eq, a != b ne from eq_max order by id")
                .ddl(
                        "create table eq_max (id int, a decimal(76,0), b decimal(76,1))",
                        "insert into eq_max values " +
                                "(1, " + nines(76) + "m, " + nines(75) + ".9m)," +
                                "(2, -" + nines(76) + "m, -" + nines(75) + ".9m)," +
                                "(3, 1m, 1.0m)"
                )
                .expectSize()
                .returns("eq\tne\n" +
                        "false\ttrue\n" +
                        "false\ttrue\n" +
                        "true\tfalse\n");
    }

    @Test
    public void testEqScaleAlignmentNulls() throws Exception {
        assertQuery("select a = b eq, a != b ne from eq_nulls order by id")
                .ddl(
                        "create table eq_nulls (id int, a decimal(18,0), b decimal(18,1))",
                        "insert into eq_nulls values " +
                                "(1, null, " + nines(17) + ".9m)," +
                                "(2, " + nines(18) + "m, null)," +
                                "(3, null, null)"
                )
                .expectSize()
                .returns("eq\tne\n" +
                        "false\ttrue\n" +
                        "false\ttrue\n" +
                        "true\tfalse\n");
    }

    @Test
    public void testEqSlowPathComparatorWidth() {
        // the comparator width follows the wider operand; mixed scales never widen it
        assertSelectedFunction(ColumnType.getDecimalType(18, 0), ColumnType.getDecimalType(18, 1), "Decimal64Func");
        assertSelectedFunction(ColumnType.getDecimalType(38, 0), ColumnType.getDecimalType(38, 1), "Decimal128Func");
        assertSelectedFunction(ColumnType.getDecimalType(76, 0), ColumnType.getDecimalType(76, 1), "Decimal256Func");
    }

    private static String nines(int count) {
        final StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append('9');
        }
        return sb.toString();
    }

    private void assertEqualityAtWidths(int leftPrecision, int rightPrecision) throws Exception {
        final String table = "eq_cmp_" + leftPrecision + "_" + rightPrecision;
        // a holds max(leftPrecision) integer digits, b one fractional digit: aligning them needs
        // leftPrecision + 1 digits, which no longer fits the operand width
        final String maxA = nines(leftPrecision);
        final String maxB = nines(rightPrecision - 1) + ".9";
        final BigDecimal[][] rows = {
                {new BigDecimal(maxA), new BigDecimal(maxB)},
                {new BigDecimal(maxA).negate(), new BigDecimal(maxB).negate()},
                {new BigDecimal(maxA), new BigDecimal(maxB).negate()},
                {new BigDecimal(maxA).negate(), new BigDecimal(maxB)},
                {BigDecimal.ONE, BigDecimal.ONE}
        };
        final StringBuilder expected = new StringBuilder("eq\tne\tqe\tqn\n");
        for (BigDecimal[] row : rows) {
            final boolean eq = row[0].compareTo(row[1]) == 0;
            expected.append(eq).append('\t').append(!eq).append('\t')
                    .append(eq).append('\t').append(!eq).append('\n');
        }
        assertQuery("select a = b eq, a != b ne, b = a qe, b != a qn from " + table + " order by id")
                .ddl(
                        "create table " + table + " (id int, a decimal(" + leftPrecision + ",0), b decimal(" + rightPrecision + ",1))",
                        "insert into " + table + " values " +
                                "(1, " + maxA + "m, " + maxB + "m)," +
                                "(2, -" + maxA + "m, -" + maxB + "m)," +
                                "(3, " + maxA + "m, -" + maxB + "m)," +
                                "(4, -" + maxA + "m, " + maxB + "m)," +
                                "(5, 1m, 1.0m)"
                )
                .expectSize()
                .returns(expected);
    }

    private void assertNegated(Function left, Function right, boolean expected) {
        args.clear();
        args.add(left);
        args.add(right);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            ((NegatableBooleanFunction) func).setNegated();
            Assert.assertEquals(expected, func.getBool(null));
        }
    }

    /**
     * Pins the implementation the factory picks for a type pair, in both operand orders.
     */
    private void assertSelectedFunction(int leftType, int rightType, String expectedFunc) {
        for (int swap = 0; swap < 2; swap++) {
            args.clear();
            args.add(DecimalColumn.newInstance(0, swap == 0 ? leftType : rightType));
            args.add(DecimalColumn.newInstance(1, swap == 0 ? rightType : leftType));
            try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
                Assert.assertEquals(expectedFunc, func.getClass().getSimpleName());
            }
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

        // right = left should return the same result
        args.clear();
        args.add(right);
        args.add(left);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            boolean result = func.getBool(null);
            Assert.assertEquals(expected, result);
        }
    }
}
