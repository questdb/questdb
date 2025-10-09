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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CastStrToDecimalFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastExplains() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime value needs parsing
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(5,2)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['123.45']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT '123.45' AS value) SELECT cast(value as DECIMAL(5, 2)) FROM data");

                    // Runtime value without scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(5,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['123']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT '123' AS value) SELECT cast(value as DECIMAL(5, 0)) FROM data");

                    // Expression should be constant folded
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [1.00]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast('1' as DECIMAL(5, 2))");
                }
        );
    }

    @Test
    public void testCastExplainsForDifferentDecimalTypes() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 (uses CastStrToDecimalFunctionFactory)
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(2,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['99']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT '99' AS value) SELECT cast(value as DECIMAL(2)) FROM data");

                    // DECIMAL16
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(4,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['9999']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT '9999' AS value) SELECT cast(value as DECIMAL(4)) FROM data");

                    // DECIMAL32
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(9,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['999999999']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT '999999999' AS value) SELECT cast(value as DECIMAL(9)) FROM data");

                    // DECIMAL64
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(18,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['999999999999999999']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT '999999999999999999' AS value) SELECT cast(value as DECIMAL(18)) FROM data");

                    // DECIMAL128
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(19,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['9223372036854775807']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT '9223372036854775807' AS value) SELECT cast(value as DECIMAL(19)) FROM data");

                    // DECIMAL256
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(40,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['9223372036854775807']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT '9223372036854775807' AS value) SELECT cast(value as DECIMAL(40)) FROM data");

                    // DECIMAL8 with scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(2,1)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['9']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT '9' AS value) SELECT cast(value as DECIMAL(2,1)) FROM data");

                    // Constant folding for all decimal types
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast('99' as DECIMAL(2))");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [9999]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast('9999' as DECIMAL(4))");

                    // Constant folding with scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [9.0]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast('9' as DECIMAL(2,1))");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99.00]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast('99' as DECIMAL(4,2))");
                }
        );
    }

    @Test
    public void testCastHighScaleLowPrecision() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "0.00\n",
                            "select cast('0' as DECIMAL(3,2))"
                    );

                    // Any non-zero value should overflow
                    assertException(
                            "select cast('1' as DECIMAL(2,2))",
                            12,
                            "inconvertible value: `1` [STRING -> DECIMAL(2,2)]"
                    );

                    assertException(
                            "select cast('-1' as DECIMAL(2,2))",
                            12,
                            "inconvertible value: `-1` [STRING -> DECIMAL(2,2)]"
                    );
                }
        );
    }

    @Test
    public void testCastInvalidFormat() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertException(
                            "select cast('abc' as DECIMAL(5,2))",
                            12,
                            "inconvertible value: `abc` [STRING -> DECIMAL(5,2)]"
                    );

                    assertException(
                            "select cast('12.34.56' as DECIMAL(5,2))",
                            12,
                            "inconvertible value: `12.34.56` [STRING -> DECIMAL(5,2)]"
                    );

                    assertException(
                            "select cast('not_a_number' as DECIMAL(10))",
                            12,
                            "inconvertible value: `not_a_number` [STRING -> DECIMAL(10,0)]"
                    );

                    assertException(
                            "select cast('abc' as DECIMAL(35,2))",
                            12,
                            "inconvertible value: `abc` [STRING -> DECIMAL(35,2)]"
                    );

                    assertException(
                            "select cast('12.34.56' as DECIMAL(35,2))",
                            12,
                            "inconvertible value: `12.34.56` [STRING -> DECIMAL(35,2)]"
                    );

                    assertException(
                            "select cast('not_a_number' as DECIMAL(37))",
                            12,
                            "inconvertible value: `not_a_number` [STRING -> DECIMAL(37,0)]"
                    );

                    assertException(
                            "select cast('abc' as DECIMAL(55,2))",
                            12,
                            "inconvertible value: `abc` [STRING -> DECIMAL(55,2)]"
                    );

                    assertException(
                            "select cast('12.34.56' as DECIMAL(55,2))",
                            12,
                            "inconvertible value: `12.34.56` [STRING -> DECIMAL(55,2)]"
                    );

                    assertException(
                            "select cast('not_a_number' as DECIMAL(70))",
                            12,
                            "inconvertible value: `not_a_number` [STRING -> DECIMAL(70,0)]"
                    );
                }
        );
    }

    @Test
    public void testCastLargeScaleValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Test with large scale values
                    assertSql(
                            "cast\n" +
                                    "1.0000000000\n",
                            "select cast('1' as DECIMAL(20,10))"
                    );

                    assertSql(
                            "cast\n" +
                                    "123.000000000000000000\n",
                            "select cast('123' as DECIMAL(21,18))"
                    );
                }
        );
    }

    @Test
    public void testCastMaxStringValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max long value to decimal with sufficient precision
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807\n",
                            "select cast('9223372036854775807' as DECIMAL(19))"
                    );

                    // Min long value to decimal with sufficient precision
                    assertSql(
                            "cast\n" +
                                    "-9223372036854775807\n",
                            "select cast('-9223372036854775807' as DECIMAL(19))"
                    );

                    // Max long with scale requires higher precision
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807.00\n",
                            "select cast('9223372036854775807' as DECIMAL(21,2))"
                    );
                }
        );
    }

    @Test
    public void testCastNegativeValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast('-1' as DECIMAL(2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123.00\n",
                            "select cast('-123' as DECIMAL(5,2))"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertException(
                            "select cast('10000' as DECIMAL(4))",
                            12,
                            "inconvertible value: `10000` [STRING -> DECIMAL(4,0)]"
                    );

                    assertException(
                            "select cast('-10000' as DECIMAL(4))",
                            12,
                            "inconvertible value: `-10000` [STRING -> DECIMAL(4,0)]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertException(
                            "select cast('10000000000' as DECIMAL(9))",
                            12,
                            "inconvertible value: `10000000000` [STRING -> DECIMAL(9,0)]"
                    );

                    assertException(
                            "select cast('-10000000000' as DECIMAL(9))",
                            12,
                            "inconvertible value: `-10000000000` [STRING -> DECIMAL(9,0)]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal8() throws Exception {
        testConstantCastOverflow("128", "DECIMAL(2)", "inconvertible value: `128` [STRING -> DECIMAL(2,0)]");
        testConstantCastOverflow("-129", "DECIMAL(2)", "inconvertible value: `-129` [STRING -> DECIMAL(2,0)]");
    }

    @Test
    public void testCastOverflowWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // 100 with scale 2 requires precision of at least 5 (100.00)
                    assertException(
                            "select cast('100' as DECIMAL(4,2))",
                            12,
                            "inconvertible value: `100` [STRING -> DECIMAL(4,2)]"
                    );

                    // 1000 with scale 3 requires precision of at least 7 (1000.000)
                    assertException(
                            "select cast('1000' as DECIMAL(5,3))",
                            12,
                            "inconvertible value: `1000` [STRING -> DECIMAL(5,3)]"
                    );
                }
        );
    }

    @Test
    public void testCastSignedStrings() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast('+123' as DECIMAL(3))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-456\n",
                            "select cast('-456' as DECIMAL(3))"
                    );

                    assertSql(
                            "cast\n" +
                                    "78.90\n",
                            "select cast('+78.90' as DECIMAL(4,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-12.34\n",
                            "select cast('-12.34' as DECIMAL(4,2))"
                    );
                }
        );
    }

    @Test
    public void testCastStringWithDecimals() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "123.45\n",
                            "select cast('123.45' as DECIMAL(5,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123.45\n",
                            "select cast('-123.45' as DECIMAL(5,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "0.00\n",
                            "select cast('0.00' as DECIMAL(3,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "999.999\n",
                            "select cast('999.999' as DECIMAL(6,3))"
                    );
                }
        );
    }

    @Test
    public void testCastToDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807\n",
                            "select cast('9223372036854775807' as DECIMAL(19))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9223372036854775807\n",
                            "select cast('-9223372036854775807' as DECIMAL(19))"
                    );

                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast(null as string) as DECIMAL(19))"
                    );
                }
        );
    }

    @Test
    public void testCastToDecimal16() throws Exception {
        testConstantCastWithNull("9999", "9999", "DECIMAL(4)");
        testConstantCast("-9999", "-9999", "DECIMAL(4)");
    }

    @Test
    public void testCastToDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807\n",
                            "select cast('9223372036854775807' as DECIMAL(40))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9223372036854775807\n",
                            "select cast('-9223372036854775807' as DECIMAL(40))"
                    );

                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast(null as string) as DECIMAL(40))"
                    );
                }
        );
    }

    @Test
    public void testCastToDecimal32() throws Exception {
        testConstantCastWithNull("999999999", "999999999", "DECIMAL(9)");
        testConstantCast("-999999999", "-999999999", "DECIMAL(9)");
    }

    @Test
    public void testCastToDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "999999999999999999\n",
                            "select cast('999999999999999999' as DECIMAL(18))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-999999999999999999\n",
                            "select cast('-999999999999999999' as DECIMAL(18))"
                    );

                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast(null as string) as DECIMAL(18))"
                    );
                }
        );
    }

    @Test
    public void testCastToDecimal8() throws Exception {
        testConstantCastWithNull("99", "99", "DECIMAL(2)");
        testConstantCast("-99", "-99", "DECIMAL(2)");
    }

    @Test
    public void testCastWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Cast '123' to DECIMAL(5,2) should result in 123.00
                    assertSql(
                            "cast\n" +
                                    "123.00\n",
                            "select cast('123' as DECIMAL(5,2))"
                    );

                    // Cast '99' to DECIMAL(4,2) should result in 99.00
                    assertSql(
                            "cast\n" +
                                    "99.00\n",
                            "select cast('99' as DECIMAL(4,2))"
                    );

                    // Cast '-99' to DECIMAL(4,2) should result in -99.00
                    assertSql(
                            "cast\n" +
                                    "-99.00\n",
                            "select cast('-99' as DECIMAL(4,2))"
                    );

                    // Cast '0' to DECIMAL(5,3) should result in 0.000
                    assertSql(
                            "cast\n" +
                                    "0.000\n",
                            "select cast('0' as DECIMAL(5,3))"
                    );
                }
        );
    }

    @Test
    public void testCastZeroWithDifferentScales() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast('0' as DECIMAL(5,0))"
                    );

                    assertSql(
                            "cast\n" +
                                    "0.0\n",
                            "select cast('0' as DECIMAL(5,1))"
                    );

                    assertSql(
                            "cast\n" +
                                    "0.00\n",
                            "select cast('0' as DECIMAL(5,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "0.000\n",
                            "select cast('0' as DECIMAL(5,3))"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowScaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL(4,2) - max value is 99.99
                    assertException(
                            "WITH data AS (SELECT '100' AS value) SELECT cast(value as DECIMAL(4,2)) FROM data",
                            49,
                            "inconvertible value: `100` [STRING -> DECIMAL(4,2)]"
                    );

                    // Runtime overflow for DECIMAL(5,3) - max value is 99.999
                    assertException(
                            "WITH data AS (SELECT '100' AS value) SELECT cast(value as DECIMAL(5,3)) FROM data",
                            49,
                            "inconvertible value: `100` [STRING -> DECIMAL(5,3)]"
                    );

                    // Runtime overflow for DECIMAL(32,2)
                    assertException(
                            "WITH data AS (SELECT '1000000000000000000000000000000' AS value) SELECT cast(value as DECIMAL(32,2)) FROM data",
                            77,
                            "inconvertible value: `1000000000000000000000000000000` [STRING -> DECIMAL(32,2)]"
                    );

                    // Runtime overflow for DECIMAL(62,2)
                    assertException(
                            "WITH data AS (SELECT '1000000000000000000000000000000000000000000000000000000000000' AS value) SELECT cast(value as DECIMAL(62,2)) FROM data",
                            107,
                            "inconvertible value: `1000000000000000000000000000000000000000000000000000000000000` [STRING -> DECIMAL(62,2)]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowUnscaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL(2)
                    assertException(
                            "WITH data AS (SELECT '128' AS value) SELECT cast(value as DECIMAL(2)) FROM data",
                            49,
                            "inconvertible value: `128` [STRING -> DECIMAL(2,0)]"
                    );

                    // Runtime overflow for DECIMAL(4)
                    assertException(
                            "WITH data AS (SELECT '10000' AS value) SELECT cast(value as DECIMAL(4)) FROM data",
                            51,
                            "inconvertible value: `10000` [STRING -> DECIMAL(4,0)]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "92233720368547758\t92233720368547758.00\n" +
                                    "-92233720368547758\t-92233720368547758.00\n" +
                                    "12345678901234567\t12345678901234567.00\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '92233720368547758' value UNION ALL SELECT '-92233720368547758' UNION ALL SELECT '12345678901234567' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(19,2)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "99\t99.00\n" +
                                    "-99\t-99.00\n" +
                                    "12\t12.00\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '99' value UNION ALL SELECT '-99' UNION ALL SELECT '12' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(4,2)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "92233720368547758\t92233720368547758.0000000000\n" +
                                    "-92233720368547758\t-92233720368547758.0000000000\n" +
                                    "12345678901234567\t12345678901234567.0000000000\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '92233720368547758' value UNION ALL SELECT '-92233720368547758' UNION ALL SELECT '12345678901234567' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(40,10)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "999999\t999999.000\n" +
                                    "-999999\t-999999.000\n" +
                                    "123456\t123456.000\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '999999' value UNION ALL SELECT '-999999' UNION ALL SELECT '123456' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(9,3)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "999999999999\t999999999999.000000\n" +
                                    "-999999999999\t-999999999999.000000\n" +
                                    "123456789012\t123456789012.000000\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '999999999999' value UNION ALL SELECT '-999999999999' UNION ALL SELECT '123456789012' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(18,6)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "9\t9.0\n" +
                                    "-9\t-9.0\n" +
                                    "0\t0.0\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '9' value UNION ALL SELECT '-9' UNION ALL SELECT '0' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(2,1)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "9223372036854775807\t9223372036854775807\n" +
                                    "-9223372036854775807\t-9223372036854775807\n" +
                                    "1234567890123456789\t1234567890123456789\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '9223372036854775807' value UNION ALL SELECT '-9223372036854775807' UNION ALL SELECT '1234567890123456789' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(19)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "9999\t9999\n" +
                                    "-9999\t-9999\n" +
                                    "1234\t1234\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '9999' value UNION ALL SELECT '-9999' UNION ALL SELECT '1234' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(4)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "9223372036854775807\t9223372036854775807\n" +
                                    "-9223372036854775807\t-9223372036854775807\n" +
                                    "1234567890123456789\t1234567890123456789\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '9223372036854775807' value UNION ALL SELECT '-9223372036854775807' UNION ALL SELECT '1234567890123456789' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(40)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "999999999\t999999999\n" +
                                    "-999999999\t-999999999\n" +
                                    "123456789\t123456789\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '999999999' value UNION ALL SELECT '-999999999' UNION ALL SELECT '123456789' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(9)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "999999999999999999\t999999999999999999\n" +
                                    "-999999999999999999\t-999999999999999999\n" +
                                    "123456789012345678\t123456789012345678\n" +
                                    "\t\n",
                            "WITH data AS (SELECT '999999999999999999' value UNION ALL SELECT '-999999999999999999' UNION ALL SELECT '123456789012345678' UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(18)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal8() throws Exception {
        testRuntimeCast(
                new String[]{"99", "-99", "0", null},
                new String[]{"99", "-99", "0", null},
                "DECIMAL(2)"
        );
    }

    private void testConstantCast(String inputString, String expectedOutput, String targetType) throws Exception {
        assertMemoryLeak(
                () -> assertSql(
                        "cast\n" + expectedOutput + "\n",
                        "select cast('" + inputString + "' as " + targetType + ")"
                )
        );
    }

    private void testConstantCastOverflow(String inputString, String targetType, String expectedErrorFragment) throws Exception {
        assertMemoryLeak(
                () -> assertException(
                        "select cast('" + inputString + "' as " + targetType + ")",
                        12,
                        expectedErrorFragment
                )
        );
    }

    private void testConstantCastWithNull(String inputString, String expectedOutput, String targetType) throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" + expectedOutput + "\n",
                            "select cast('" + inputString + "' as " + targetType + ")"
                    );
                    assertSql(
                            "cast\n\n",
                            "select cast(cast(null as string) as " + targetType + ")"
                    );
                }
        );
    }

    private void testRuntimeCast(String[] inputValues, String[] expectedOutputs, String targetType) throws Exception {
        assertMemoryLeak(
                () -> {
                    StringBuilder query = new StringBuilder("WITH data AS (");
                    for (int i = 0; i < inputValues.length; i++) {
                        if (i > 0) query.append(" UNION ALL ");
                        if (inputValues[i] == null) {
                            query.append("SELECT null");
                        } else {
                            query.append("SELECT '").append(inputValues[i]).append("'");
                        }
                        query.append(" value");
                    }
                    query.append(") SELECT value, cast(value as ").append(targetType).append(") as decimal_value FROM data");

                    StringBuilder expected = new StringBuilder("value\tdecimal_value\n");
                    for (int i = 0; i < inputValues.length; i++) {
                        expected.append(inputValues[i] == null ? "" : inputValues[i])
                                .append("\t")
                                .append(expectedOutputs[i] == null ? "" : expectedOutputs[i])
                                .append("\n");
                    }

                    assertSql(expected.toString(), query.toString());
                }
        );
    }
}