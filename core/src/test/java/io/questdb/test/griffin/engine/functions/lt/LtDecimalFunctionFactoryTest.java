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

package io.questdb.test.griffin.engine.functions.lt;

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
import io.questdb.griffin.engine.functions.lt.LtDecimalFunctionFactory;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class LtDecimalFunctionFactoryTest extends AbstractCairoTest {
    // widest precision of each decimal storage width
    private static final int[] WIDTH_PRECISIONS = {2, 4, 9, 18, 38, 76};
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
    public void testLtDecimal128SlowPathBothNull() {
        // Different scales force the slow path (CompareDecimal128Function). For
        // `<` both NULL must evaluate to false; the negated `>=` case is checked
        // separately in testGeDecimal128SlowPathBothNullIsTrue.
        createFunctionAndAssert(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 4)),
                false
        );
    }

    @Test
    public void testLtDecimal128SlowPathNegatedBothNullIsTrue() {
        // Slow path (CompareDecimal128Function) with `>=`: same convention as
        // the fast path -- two NULLs compare equal, one-sided NULL is false.
        assertNegated(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 4)),
                true
        );
        assertNegated(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 4)),
                false
        );
    }

    @Test
    public void testLtDecimal128SlowPathNullVsValue() {
        // Different scales force the slow path. Without the NULL guard
        // Decimal128.compareTo(NULL, value) returns -1, which would make `c1 < value`
        // evaluate to true on a NULL c1.
        createFunctionAndAssert(
                new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(20, 2)),
                new Decimal128Constant(0, 100, ColumnType.getDecimalType(20, 4)),
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
    public void testLtDecimal256SlowPathBothNull() {
        // Different scales force the slow path (CompareDecimal256Function).
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
                false
        );
    }

    @Test
    public void testLtDecimal256SlowPathNegatedBothNullIsTrue() {
        // Slow path (CompareDecimal256Function) with `>=`: same convention as
        // the fast path -- two NULLs compare equal, one-sided NULL is false.
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
                true
        );
        assertNegated(
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
    public void testLtDecimal256SlowPathNullVsValue() {
        // Different scales force the slow path. NULL on either side must produce false.
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
    public void testLtDecimal64NegatedBothNullIsTrue() {
        // Fast path (UnscaledDecimal64Func) with `>=`: two NULLs must compare
        // equal so that `>=(null,null)` is true, matching `=(null,null)=true`
        // and the convention used for STRING/LONG. One-sided NULL stays false.
        assertNegated(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                true
        );
        assertNegated(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 2)),
                false
        );
    }

    @Test
    public void testLtDecimal64SlowPathBothNull() {
        // Different scales force the slow path (CompareDecimal64Function).
        createFunctionAndAssert(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 4)),
                false
        );
    }

    @Test
    public void testLtDecimal64SlowPathNegatedBothNullIsTrue() {
        // Slow path (CompareDecimal64Function) with `>=`: same convention as
        // the fast path -- two NULLs compare equal, one-sided NULL is false.
        assertNegated(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 4)),
                true
        );
        assertNegated(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 4)),
                false
        );
    }

    @Test
    public void testLtDecimal64SlowPathNullVsValue() {
        // Different scales force the slow path. NULL on either side must produce false.
        createFunctionAndAssert(
                new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(10, 2)),
                new Decimal64Constant(100, ColumnType.getDecimalType(10, 4)),
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
    public void testLtFastPathSameTagDifferentPrecision() throws Exception {
        // operands sharing a storage tag and a scale compare unscaled, whatever their precisions
        assertFastPathAtWidths(1, 2, 0, "UnscaledDecimal8Func");
        assertFastPathAtWidths(3, 4, 1, "UnscaledDecimal16Func");
        assertFastPathAtWidths(5, 9, 2, "UnscaledDecimal32Func");
        assertFastPathAtWidths(10, 18, 2, "UnscaledDecimal64Func");
        assertFastPathAtWidths(19, 38, 2, "UnscaledDecimal128Func");
        assertFastPathAtWidths(39, 76, 2, "UnscaledDecimal256Func");
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

    @Test
    public void testLtScaleAlignmentAcrossWidths() throws Exception {
        for (int leftPrecision : WIDTH_PRECISIONS) {
            for (int rightPrecision : WIDTH_PRECISIONS) {
                assertOrderingAtWidths(leftPrecision, rightPrecision);
            }
        }
    }

    @Test
    public void testLtScaleAlignmentMaxPrecisionOrderBy() throws Exception {
        assertQuery("select a from lt_order order by a")
                .ddl(
                        "create table lt_order (a decimal(76,0), b decimal(76,1))",
                        "insert into lt_order values " +
                                "(1m, 1.0m)," +
                                "(" + nines(76) + "m, " + nines(75) + ".9m)," +
                                "(-" + nines(76) + "m, -" + nines(75) + ".9m)"
                )
                .expectSize()
                .returns("a\n" +
                        "-" + nines(76) + "\n" +
                        "1\n" +
                        nines(76) + "\n");
        assertQuery("select a from lt_order where a < b order by a")
                .returns("a\n" +
                        "-" + nines(76) + "\n");
    }

    @Test
    public void testLtScaleAlignmentNulls() throws Exception {
        assertQuery("select a < b lt, a <= b le, a > b gt, a >= b ge from lt_nulls order by id")
                .ddl(
                        "create table lt_nulls (id int, a decimal(18,0), b decimal(18,1))",
                        "insert into lt_nulls values " +
                                "(1, null, " + nines(17) + ".9m)," +
                                "(2, " + nines(18) + "m, null)," +
                                "(3, null, null)"
                )
                .expectSize()
                .returns("lt\tle\tgt\tge\n" +
                        "false\tfalse\tfalse\tfalse\n" +
                        "false\tfalse\tfalse\tfalse\n" +
                        "false\ttrue\tfalse\ttrue\n");
    }

    @Test
    public void testLtSlowPathComparatorWidth() {
        // the comparator width follows the wider operand; mixed scales never widen it
        assertSelectedFunction(ColumnType.getDecimalType(18, 0), ColumnType.getDecimalType(18, 1), "Decimal64Func");
        assertSelectedFunction(ColumnType.getDecimalType(38, 0), ColumnType.getDecimalType(38, 1), "Decimal128Func");
        assertSelectedFunction(ColumnType.getDecimalType(76, 0), ColumnType.getDecimalType(76, 1), "Decimal256Func");
    }

    private static String literal(BigDecimal value) {
        return value == null ? "null" : value.toPlainString() + "m";
    }

    private static BigDecimal maxValue(int precision, int scale) {
        return new BigDecimal(nines(precision)).movePointLeft(scale);
    }

    private static String nines(int count) {
        final StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append('9');
        }
        return sb.toString();
    }

    private void assertFastPathAtWidths(int leftPrecision, int rightPrecision, int scale, String expectedFunc) throws Exception {
        final int leftType = ColumnType.getDecimalType(leftPrecision, scale);
        final int rightType = ColumnType.getDecimalType(rightPrecision, scale);
        Assert.assertEquals(ColumnType.tagOf(leftType), ColumnType.tagOf(rightType));
        assertSelectedFunction(leftType, rightType, expectedFunc);

        final BigDecimal unit = BigDecimal.ONE.movePointLeft(scale);
        final BigDecimal maxLeft = maxValue(leftPrecision, scale);
        final BigDecimal maxRight = maxValue(rightPrecision, scale);
        final BigDecimal[][] rows = {
                {maxLeft, maxLeft},
                {maxLeft, maxLeft.subtract(unit)},
                {maxLeft.subtract(unit), maxLeft},
                {maxLeft.negate(), maxRight},
                {maxLeft, maxRight.negate()},
                {null, unit},
                {unit, null},
                {null, null}
        };

        final String table = "lt_fast_" + leftPrecision + "_" + rightPrecision + "_" + scale;
        final StringBuilder insert = new StringBuilder("INSERT INTO ").append(table).append(" VALUES ");
        final StringBuilder expected = new StringBuilder("lt\tle\tgt\tge\n");
        for (int i = 0; i < rows.length; i++) {
            final BigDecimal a = rows[i][0];
            final BigDecimal b = rows[i][1];
            if (i > 0) {
                insert.append(',');
            }
            insert.append('(').append(i).append(", ").append(literal(a)).append(", ").append(literal(b)).append(')');
            if (a == null || b == null) {
                // two NULLs compare equal, a one-sided NULL is neither below nor above
                final boolean bothNull = a == null && b == null;
                expected.append("false\t").append(bothNull).append("\tfalse\t").append(bothNull).append('\n');
            } else {
                final int cmp = a.compareTo(b);
                expected.append(cmp < 0).append('\t').append(cmp <= 0).append('\t')
                        .append(cmp > 0).append('\t').append(cmp >= 0).append('\n');
            }
        }

        assertQuery("SELECT a < b lt, a <= b le, a > b gt, a >= b ge FROM " + table + " ORDER BY id")
                .ddl(
                        "CREATE TABLE " + table + " (id INT, a DECIMAL(" + leftPrecision + "," + scale + ")"
                                + ", b DECIMAL(" + rightPrecision + "," + scale + "))",
                        insert.toString()
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

    private void assertOrderingAtWidths(int leftPrecision, int rightPrecision) throws Exception {
        final String table = "lt_cmp_" + leftPrecision + "_" + rightPrecision;
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
        final StringBuilder expected = new StringBuilder("lt\tle\tgt\tge\n");
        for (BigDecimal[] row : rows) {
            final int cmp = row[0].compareTo(row[1]);
            expected.append(cmp < 0).append('\t')
                    .append(cmp <= 0).append('\t')
                    .append(cmp > 0).append('\t')
                    .append(cmp >= 0).append('\n');
        }
        assertQuery("select a < b lt, a <= b le, a > b gt, a >= b ge from " + table + " order by id")
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
