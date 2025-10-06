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

public class CastVarcharToDecimalFunctionFactoryTest extends AbstractCairoTest {

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
                            "EXPLAIN WITH data AS (SELECT cast('123.45' as varchar) AS value) SELECT cast(value as DECIMAL(5, 2)) FROM data");

                    // Runtime value without scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(5,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['123']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast('123' as varchar) AS value) SELECT cast(value as DECIMAL(5, 0)) FROM data");

                    // Expression should be constant folded
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [1.00]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(cast('1' as varchar) as DECIMAL(5, 2))");
                }
        );
    }

    @Test
    public void testCastExplainsForDifferentDecimalTypes() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 (uses CastVarcharToDecimalFunctionFactory)
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(2,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['99']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast('99' as varchar) AS value) SELECT cast(value as DECIMAL(2)) FROM data");

                    // DECIMAL16
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(4,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['9999']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast('9999' as varchar) AS value) SELECT cast(value as DECIMAL(4)) FROM data");

                    // DECIMAL32
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(9,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['999999999']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast('999999999' as varchar) AS value) SELECT cast(value as DECIMAL(9)) FROM data");

                    // DECIMAL64
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(18,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['999999999999999999']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast('999999999999999999' as varchar) AS value) SELECT cast(value as DECIMAL(18)) FROM data");

                    // DECIMAL128
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(19,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['9223372036854775807']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast('9223372036854775807' as varchar) AS value) SELECT cast(value as DECIMAL(19)) FROM data");

                    // DECIMAL256
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(40,0)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['9223372036854775807']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast('9223372036854775807' as varchar) AS value) SELECT cast(value as DECIMAL(40)) FROM data");

                    // DECIMAL8 with scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::DECIMAL(2,1)]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: ['9']\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast('9' as varchar) AS value) SELECT cast(value as DECIMAL(2,1)) FROM data");

                    // Constant folding for all decimal types
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(cast('99' as varchar) as DECIMAL(2))");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [9999]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(cast('9999' as varchar) as DECIMAL(4))");

                    // Constant folding with scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [9.0]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(cast('9' as varchar) as DECIMAL(2,1))");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99.00]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(cast('99' as varchar) as DECIMAL(4,2))");
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
                            "select cast(cast('0' as varchar) as DECIMAL(3,2))"
                    );

                    // Any non-zero value should overflow
                    assertException(
                            "select cast(cast('1' as varchar) as DECIMAL(2,2))",
                            12,
                            "inconvertible value: `1` [VARCHAR -> DECIMAL(2,2)]"
                    );

                    assertException(
                            "select cast(cast('-1' as varchar) as DECIMAL(2,2))",
                            12,
                            "inconvertible value: `-1` [VARCHAR -> DECIMAL(2,2)]"
                    );
                }
        );
    }

    @Test
    public void testCastInvalidFormat() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertException(
                            "select cast(cast('abc' as varchar) as DECIMAL(5,2))",
                            12,
                            "inconvertible value: `abc` [VARCHAR -> DECIMAL(5,2)]"
                    );

                    assertException(
                            "select cast(cast('12.34.56' as varchar) as DECIMAL(5,2))",
                            12,
                            "inconvertible value: `12.34.56` [VARCHAR -> DECIMAL(5,2)]"
                    );

                    assertException(
                            "select cast(cast('not_a_number' as varchar) as DECIMAL(10))",
                            12,
                            "inconvertible value: `not_a_number` [VARCHAR -> DECIMAL(10,0)]"
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
                            "select cast(cast('1' as varchar) as DECIMAL(20,10))"
                    );

                    assertSql(
                            "cast\n" +
                                    "123.000000000000000000\n",
                            "select cast(cast('123' as varchar) as DECIMAL(21,18))"
                    );
                }
        );
    }

    @Test
    public void testCastMaxVarcharValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max long value to decimal with sufficient precision
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807\n",
                            "select cast(cast('9223372036854775807' as varchar) as DECIMAL(19))"
                    );

                    // Min long value to decimal with sufficient precision
                    assertSql(
                            "cast\n" +
                                    "-9223372036854775807\n",
                            "select cast(cast('-9223372036854775807' as varchar) as DECIMAL(19))"
                    );

                    // Max long with scale requires higher precision
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807.00\n",
                            "select cast(cast('9223372036854775807' as varchar) as DECIMAL(21,2))"
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
                            "select cast(cast('-1' as varchar) as DECIMAL(2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123.00\n",
                            "select cast(cast('-123' as varchar) as DECIMAL(5,2))"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertException(
                            "select cast(cast('10000' as varchar) as DECIMAL(4))",
                            12,
                            "inconvertible value: `10000` [VARCHAR -> DECIMAL(4,0)]"
                    );

                    assertException(
                            "select cast(cast('-10000' as varchar) as DECIMAL(4))",
                            12,
                            "inconvertible value: `-10000` [VARCHAR -> DECIMAL(4,0)]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertException(
                            "select cast(cast('10000000000' as varchar) as DECIMAL(9))",
                            12,
                            "inconvertible value: `10000000000` [VARCHAR -> DECIMAL(9,0)]"
                    );

                    assertException(
                            "select cast(cast('-10000000000' as varchar) as DECIMAL(9))",
                            12,
                            "inconvertible value: `-10000000000` [VARCHAR -> DECIMAL(9,0)]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertException(
                            "select cast(cast('128' as varchar) as DECIMAL(2))",
                            12,
                            "inconvertible value: `128` [VARCHAR -> DECIMAL(2,0)]"
                    );

                    assertException(
                            "select cast(cast('-129' as varchar) as DECIMAL(2))",
                            12,
                            "inconvertible value: `-129` [VARCHAR -> DECIMAL(2,0)]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // 100 with scale 2 requires precision of at least 5 (100.00)
                    assertException(
                            "select cast(cast('100' as varchar) as DECIMAL(4,2))",
                            12,
                            "inconvertible value: `100` [VARCHAR -> DECIMAL(4,2)]"
                    );

                    // 1000 with scale 3 requires precision of at least 7 (1000.000)
                    assertException(
                            "select cast(cast('1000' as varchar) as DECIMAL(5,3))",
                            12,
                            "inconvertible value: `1000` [VARCHAR -> DECIMAL(5,3)]"
                    );
                }
        );
    }

    @Test
    public void testCastSignedVarchars() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast(cast('+123' as varchar) as DECIMAL(3))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-456\n",
                            "select cast(cast('-456' as varchar) as DECIMAL(3))"
                    );

                    assertSql(
                            "cast\n" +
                                    "78.90\n",
                            "select cast(cast('+78.90' as varchar) as DECIMAL(4,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-12.34\n",
                            "select cast(cast('-12.34' as varchar) as DECIMAL(4,2))"
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
                            "select cast(cast('9223372036854775807' as varchar) as DECIMAL(19))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9223372036854775807\n",
                            "select cast(cast('-9223372036854775807' as varchar) as DECIMAL(19))"
                    );

                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast(null as varchar) as DECIMAL(19))"
                    );
                }
        );
    }

    @Test
    public void testCastToDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "9999\n",
                            "select cast(cast('9999' as varchar) as DECIMAL(4))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9999\n",
                            "select cast(cast('-9999' as varchar) as DECIMAL(4))"
                    );

                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast(null as varchar) as DECIMAL(4))"
                    );
                }
        );
    }

    @Test
    public void testCastToDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807\n",
                            "select cast(cast('9223372036854775807' as varchar) as DECIMAL(40))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9223372036854775807\n",
                            "select cast(cast('-9223372036854775807' as varchar) as DECIMAL(40))"
                    );

                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast(null as varchar) as DECIMAL(40))"
                    );
                }
        );
    }

    @Test
    public void testCastToDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "999999999\n",
                            "select cast(cast('999999999' as varchar) as DECIMAL(9))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-999999999\n",
                            "select cast(cast('-999999999' as varchar) as DECIMAL(9))"
                    );

                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast(null as varchar) as DECIMAL(9))"
                    );
                }
        );
    }

    @Test
    public void testCastToDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "999999999999999999\n",
                            "select cast(cast('999999999999999999' as varchar) as DECIMAL(18))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-999999999999999999\n",
                            "select cast(cast('-999999999999999999' as varchar) as DECIMAL(18))"
                    );

                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast(null as varchar) as DECIMAL(18))"
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
    public void testCastVarcharWithDecimals() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "123.45\n",
                            "select cast(cast('123.45' as varchar) as DECIMAL(5,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123.45\n",
                            "select cast(cast('-123.45' as varchar) as DECIMAL(5,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "0.00\n",
                            "select cast(cast('0.00' as varchar) as DECIMAL(3,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "999.999\n",
                            "select cast(cast('999.999' as varchar) as DECIMAL(6,3))"
                    );
                }
        );
    }

    @Test
    public void testCastWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Cast '123' to DECIMAL(5,2) should result in 123.00
                    assertSql(
                            "cast\n" +
                                    "123.00\n",
                            "select cast(cast('123' as varchar) as DECIMAL(5,2))"
                    );

                    // Cast '99' to DECIMAL(4,2) should result in 99.00
                    assertSql(
                            "cast\n" +
                                    "99.00\n",
                            "select cast(cast('99' as varchar) as DECIMAL(4,2))"
                    );

                    // Cast '-99' to DECIMAL(4,2) should result in -99.00
                    assertSql(
                            "cast\n" +
                                    "-99.00\n",
                            "select cast(cast('-99' as varchar) as DECIMAL(4,2))"
                    );

                    // Cast '0' to DECIMAL(5,3) should result in 0.000
                    assertSql(
                            "cast\n" +
                                    "0.000\n",
                            "select cast(cast('0' as varchar) as DECIMAL(5,3))"
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
                            "select cast(cast('0' as varchar) as DECIMAL(5,0))"
                    );

                    assertSql(
                            "cast\n" +
                                    "0.0\n",
                            "select cast(cast('0' as varchar) as DECIMAL(5,1))"
                    );

                    assertSql(
                            "cast\n" +
                                    "0.00\n",
                            "select cast(cast('0' as varchar) as DECIMAL(5,2))"
                    );

                    assertSql(
                            "cast\n" +
                                    "0.000\n",
                            "select cast(cast('0' as varchar) as DECIMAL(5,3))"
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
                            "WITH data AS (SELECT cast('100' as varchar) AS value) SELECT cast(value as DECIMAL(4,2)) FROM data",
                            66,
                            "inconvertible value: `100` [VARCHAR -> DECIMAL(4,2)]"
                    );

                    // Runtime overflow for DECIMAL(5,3) - max value is 99.999
                    assertException(
                            "WITH data AS (SELECT cast('100' as varchar) AS value) SELECT cast(value as DECIMAL(5,3)) FROM data",
                            66,
                            "inconvertible value: `100` [VARCHAR -> DECIMAL(5,3)]"
                    );

                    // Runtime overflow for DECIMAL(32,2)
                    assertException(
                            "WITH data AS (SELECT cast('1000000000000000000000000000000' as varchar) AS value) SELECT cast(value as DECIMAL(32,2)) FROM data",
                            94,
                            "inconvertible value: `1000000000000000000000000000000` [VARCHAR -> DECIMAL(32,2)]"
                    );

                    // Runtime overflow for DECIMAL(62,2)
                    assertException(
                            "WITH data AS (SELECT cast('1000000000000000000000000000000000000000000000000000000000000' as varchar) AS value) SELECT cast(value as DECIMAL(62,2)) FROM data",
                            124,
                            "inconvertible value: `1000000000000000000000000000000000000000000000000000000000000` [VARCHAR -> DECIMAL(62,2)]"
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
                            "WITH data AS (SELECT cast('128' as varchar) AS value) SELECT cast(value as DECIMAL(2)) FROM data",
                            66,
                            "inconvertible value: `128` [VARCHAR -> DECIMAL(2,0)]"
                    );

                    // Runtime overflow for DECIMAL(4)
                    assertException(
                            "WITH data AS (SELECT cast('10000' as varchar) AS value) SELECT cast(value as DECIMAL(4)) FROM data",
                            68,
                            "inconvertible value: `10000` [VARCHAR -> DECIMAL(4,0)]"
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
                            "WITH data AS (SELECT cast('92233720368547758' as varchar) value UNION ALL SELECT cast('-92233720368547758' as varchar) UNION ALL SELECT cast('12345678901234567' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('99' as varchar) value UNION ALL SELECT cast('-99' as varchar) UNION ALL SELECT cast('12' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('92233720368547758' as varchar) value UNION ALL SELECT cast('-92233720368547758' as varchar) UNION ALL SELECT cast('12345678901234567' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('999999' as varchar) value UNION ALL SELECT cast('-999999' as varchar) UNION ALL SELECT cast('123456' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('999999999999' as varchar) value UNION ALL SELECT cast('-999999999999' as varchar) UNION ALL SELECT cast('123456789012' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('9' as varchar) value UNION ALL SELECT cast('-9' as varchar) UNION ALL SELECT cast('0' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('9223372036854775807' as varchar) value UNION ALL SELECT cast('-9223372036854775807' as varchar) UNION ALL SELECT cast('1234567890123456789' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('9999' as varchar) value UNION ALL SELECT cast('-9999' as varchar) UNION ALL SELECT cast('1234' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('9223372036854775807' as varchar) value UNION ALL SELECT cast('-9223372036854775807' as varchar) UNION ALL SELECT cast('1234567890123456789' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('999999999' as varchar) value UNION ALL SELECT cast('-999999999' as varchar) UNION ALL SELECT cast('123456789' as varchar) UNION ALL SELECT null) " +
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
                            "WITH data AS (SELECT cast('999999999999999999' as varchar) value UNION ALL SELECT cast('-999999999999999999' as varchar) UNION ALL SELECT cast('123456789012345678' as varchar) UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(18)) as decimal_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Use WITH clause to create runtime values (non-constant)
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "99\t99\n" +
                                    "-99\t-99\n" +
                                    "0\t0\n" +
                                    "\t\n",
                            "WITH data AS (SELECT cast('99' as varchar) value UNION ALL SELECT cast('-99' as varchar) UNION ALL SELECT cast('0' as varchar) UNION ALL SELECT null) " +
                                    "SELECT value, cast(value as DECIMAL(2)) as decimal_value FROM data"
                    );
                }
        );
    }

    private void testConstantCast(String inputString, String expectedOutput, String targetType) throws Exception {
        assertMemoryLeak(
                () -> assertSql(
                        "cast\n" + expectedOutput + "\n",
                        "select cast(cast('" + inputString + "' as varchar) as " + targetType + ")"
                )
        );
    }

    private void testConstantCastWithNull(String inputString, String expectedOutput, String targetType) throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" + expectedOutput + "\n",
                            "select cast(cast('" + inputString + "' as varchar) as " + targetType + ")"
                    );
                    assertSql(
                            "cast\n\n",
                            "select cast(cast(null as varchar) as " + targetType + ")"
                    );
                }
        );
    }
}