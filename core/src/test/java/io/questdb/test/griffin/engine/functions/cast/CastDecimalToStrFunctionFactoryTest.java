/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.cast.CastDecimalToStrFunctionFactory;
import io.questdb.griffin.engine.functions.constants.StrTypeConstant;
import io.questdb.griffin.engine.functions.decimal.ToDecimalFunction;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class CastDecimalToStrFunctionFactoryTest extends AbstractCairoTest {
    private final ObjList<Function> args = new ObjList<>();
    private final Decimal256 decimal256 = new Decimal256();
    private final CastDecimalToStrFunctionFactory factory = new CastDecimalToStrFunctionFactory();

    @Test
    public void testBasic() {
        decimal256.ofString("1234.56m");
        for (int precision = 6; precision <= Decimals.MAX_PRECISION; precision++) {
            createFunctionAndAssert("1234.56", precision);
        }
    }

    @Test
    public void testCastDecimalNullToStr() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            """
                                    cast
                                    
                                    """,
                            "with data as (select cast(null as decimal(10,2)) d) select cast(d as string) from data"
                    );

                    assertSql(
                            """
                                    cast
                                    
                                    """,
                            "with data as (select cast(null as decimal(30,2)) d) select cast(d as string) from data"
                    );

                    assertSql(
                            """
                                    cast
                                    
                                    """,
                            "with data as (select cast(null as decimal(60,2)) d) select cast(d as string) from data"
                    );

                    // Also test constant null
                    assertSql(
                            """
                                    cast
                                    
                                    """,
                            "select cast(cast(null as decimal(10,2)) as string)"
                    );
                }
        );
    }

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Basic decimal to string conversions
                    assertSql(
                            """
                                    cast
                                    123.45
                                    """,
                            "select cast(123.45m as string)"
                    );

                    assertSql(
                            """
                                    cast
                                    -123.45
                                    """,
                            "select cast(-123.45m as string)"
                    );

                    // Zero with decimal places
                    assertSql(
                            """
                                    cast
                                    0.00
                                    """,
                            "select cast(0.00m as string)"
                    );

                    // Different decimal types
                    assertSql(
                            """
                                    cast
                                    99
                                    """,
                            "select cast(99m as string)"
                    );

                    assertSql(
                            """
                                    cast
                                    12345.67
                                    """,
                            "select cast(12345.67m as string)"
                    );

                    assertSql(
                            """
                                    cast
                                    123456789.123456
                                    """,
                            "select cast(123456789.123456m as string)"
                    );
                }
        );
    }

    @Test
    public void testCastExplains() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Constant folding test
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: ['123.45']
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN select cast(123.45m as string) from long_sequence(1)");

                    // Runtime value test
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::string]
                                        VirtualRecord
                                          functions: [123.45]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT 123.45m AS value) SELECT cast(value as string) FROM data");
                }
        );
    }

    @Test
    public void testNull() throws SqlException {
        decimal256.ofNull();
        for (int precision = 1; precision <= Decimals.MAX_PRECISION; precision++) {
            createFunctionAndAssert(null, precision);
        }
    }

    private void createFunctionAndAssert(CharSequence expected, int precision) {
        int type = ColumnType.getDecimalType(precision, decimal256.getScale());
        Function constant;
        if (decimal256.isNull()) {
            constant = DecimalUtil.createNullDecimalConstant(precision, 0);
        } else {
            constant = DecimalUtil.createDecimalConstant(decimal256.getHh(), decimal256.getHl(), decimal256.getLh(), decimal256.getLl(), precision, decimal256.getScale());
        }

        createFunctionAndAssert(constant, expected);
        createFunctionAndAssert(new RuntimeDecimalFunction(type), expected);
    }

    private void createFunctionAndAssert(Function left, CharSequence expected) {
        args.clear();
        args.add(left);
        args.add(StrTypeConstant.INSTANCE);
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            CharSequence strA = func.getStrA(null);
            CharSequence strB = func.getStrB(null);
            if (expected == null) {
                Assert.assertNull(strA);
                Assert.assertNull(strB);
            } else {
                Assert.assertEquals(expected.toString(), strA.toString());
                Assert.assertEquals(expected.toString(), strB.toString());
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    private class RuntimeDecimalFunction extends ToDecimalFunction {
        public RuntimeDecimalFunction(int type) {
            super(type);
        }

        @Override
        protected boolean store(Record rec) {
            if (decimal256.isNull()) {
                return false;
            }
            decimal.copyFrom(decimal256);
            return true;
        }
    }
}