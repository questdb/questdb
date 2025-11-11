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

public class CastDecimalToIntFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Truncates decimal part
                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast(123.45m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast(123.99m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123.45m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123.99m as int)"
                    );

                    // Zero with decimal places
                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.99m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.01m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(-0.99m as int)"
                    );
                }
        );
    }

    @Test
    public void testCastExplains() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime value with scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::int]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [123.45]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT 123.45m AS value) SELECT cast(value as int) FROM data");

                    // Runtime value without scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::int]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [123]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT 123m AS value) SELECT cast(value as int) FROM data");

                    // Expression should be constant folded
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [123]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(123.45m as int)");
                }
        );
    }

    @Test
    public void testCastExplainsForDifferentDecimalTypes() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::int]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [99]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(99m as DECIMAL(2)) AS value) SELECT cast(value as int) FROM data");

                    // DECIMAL16 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::int]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [9999]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(9999m as DECIMAL(4)) AS value) SELECT cast(value as int) FROM data");

                    // DECIMAL32 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::int]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [999999999]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(999999999m as DECIMAL(9)) AS value) SELECT cast(value as int) FROM data");

                    // DECIMAL64 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::int]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [2147483647]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(2147483647m as DECIMAL(10)) AS value) SELECT cast(value as int) FROM data");

                    // DECIMAL128 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::int]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [2147483647]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(2147483647m as DECIMAL(19)) AS value) SELECT cast(value as int) FROM data");

                    // DECIMAL256 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::int]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [2147483647]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(2147483647m as DECIMAL(40)) AS value) SELECT cast(value as int) FROM data");

                    // With scale - tests ScaledDecimalFunction
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::int]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [99.50]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) AS value) SELECT cast(value as int) FROM data");

                    // Constant folding for all decimal types
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(99m as int)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [9999]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(9999m as int)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [999999999]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(999999999m as int)");

                    // Constant folding with scale (truncation)
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [123]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(123.45m as int)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(99.99m as int)");
                }
        );
    }

    @Test
    public void testCastFromDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "2147483647\n",
                            "select cast(cast(2147483647m as DECIMAL(19)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-2147483647\n",
                            "select cast(cast(-2147483647m as DECIMAL(19)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(19)) as int)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "9999\n",
                            "select cast(cast(9999m as DECIMAL(4)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9999\n",
                            "select cast(cast(-9999m as DECIMAL(4)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(4)) as int)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "2147483647\n",
                            "select cast(cast(2147483647m as DECIMAL(40)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-2147483647\n",
                            "select cast(cast(-2147483647m as DECIMAL(40)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(40)) as int)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "999999999\n",
                            "select cast(cast(999999999m as DECIMAL(9)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-999999999\n",
                            "select cast(cast(-999999999m as DECIMAL(9)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(9)) as int)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "2147483647\n",
                            "select cast(cast(2147483647m as DECIMAL(18)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-2147483647\n",
                            "select cast(cast(-2147483647m as DECIMAL(18)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(18)) as int)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "99\n",
                            "select cast(cast(99m as DECIMAL(2)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-99\n",
                            "select cast(cast(-99m as DECIMAL(2)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(0m as DECIMAL(2)) as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(2)) as int)"
                    );
                }
        );
    }

    @Test
    public void testCastMaxIntValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max int value from decimal
                    assertSql(
                            "cast\n" +
                                    "2147483647\n",
                            "select cast(2147483647m as int)"
                    );

                    // Max int value minus 1
                    assertSql(
                            "cast\n" +
                                    "2147483646\n",
                            "select cast(2147483646m as int)"
                    );

                    // Min int value + 1 (to avoid overflow with negation)
                    assertSql(
                            "cast\n" +
                                    "-2147483647\n",
                            "select cast(-2147483647m as int)"
                    );

                    // With scale - truncated
                    assertSql(
                            "cast\n" +
                                    "2147483647\n",
                            "select cast(2147483647.99m as int)"
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
                            "select cast(-1m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-999999999\n",
                            "select cast(-999999999m as int)"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for int
                    assertException(
                            "select cast(cast(9223372036854775807m as DECIMAL(19)) as int)",
                            7,
                            "inconvertible value: 9223372036854775807 [DECIMAL(19,0) -> INT]"
                    );

                    assertException(
                            "select cast(cast(-9223372036854775808m as DECIMAL(19)) as int)",
                            7,
                            "inconvertible value: -9223372036854775808 [DECIMAL(19,0) -> INT]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for int
                    assertException(
                            "select cast(cast(99999999999999999999m as DECIMAL(40)) as int)",
                            7,
                            "inconvertible value: 99999999999999999999 [DECIMAL(40,0) -> INT]"
                    );

                    assertException(
                            "select cast(cast(-99999999999999999999m as DECIMAL(40)) as int)",
                            7,
                            "inconvertible value: -99999999999999999999 [DECIMAL(40,0) -> INT]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for int
                    assertException(
                            "select cast(cast(2147483648m as DECIMAL(10)) as int)",
                            7,
                            "inconvertible value: 2147483648 [DECIMAL(10,0) -> INT]"
                    );

                    assertException(
                            "select cast(cast(-2147483649m as DECIMAL(10)) as int)",
                            7,
                            "inconvertible value: -2147483649 [DECIMAL(10,0) -> INT]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for int
                    assertException(
                            "select cast(cast(9999999999m as DECIMAL(18)) as int)",
                            7,
                            "inconvertible value: 9999999999 [DECIMAL(18,0) -> INT]"
                    );

                    assertException(
                            "select cast(cast(-9999999999m as DECIMAL(18)) as int)",
                            7,
                            "inconvertible value: -9999999999 [DECIMAL(18,0) -> INT]"
                    );
                }
        );
    }

    @Test
    public void testCastZeroValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.0m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.00m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.000m as int)"
                    );
                }
        );
    }

    @Test
    public void testImplicitCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    // No implicit cast from decimal to int in arithmetic
                    // Must use explicit cast
                    assertSql(
                            "column\n" +
                                    "1234667\n",
                            "select cast(1234567m as int) + 100"
                    );

                    // Runtime conversion
                    assertSql(
                            "column\n" +
                                    "1234667\n",
                            "with data as (select 1234567m x) select cast(x as int) + 100 from data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowScaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow with scaled decimal
                    assertException(
                            "WITH data AS (SELECT cast(2147483648.5m as DECIMAL(11,1)) AS value) SELECT cast(value as int) FROM data",
                            75,
                            "inconvertible value: 2147483648.5 [DECIMAL(11,1) -> INT]"
                    );

                    assertException(
                            "WITH data AS (SELECT cast(-2147483649.5m as DECIMAL(11,1)) AS value) SELECT cast(value as int) FROM data",
                            76,
                            "inconvertible value: -2147483649.5 [DECIMAL(11,1) -> INT]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowUnscaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL32
                    assertException(
                            "WITH data AS (SELECT cast(2147483648m as DECIMAL(10)) AS value) SELECT cast(value as int) FROM data",
                            71,
                            "inconvertible value: 2147483648 [DECIMAL(10,0) -> INT]"
                    );

                    // Runtime overflow for DECIMAL64
                    assertException(
                            "WITH data AS (SELECT cast(9999999999m as DECIMAL(18)) AS value) SELECT cast(value as int) FROM data",
                            71,
                            "inconvertible value: 9999999999 [DECIMAL(18,0) -> INT]"
                    );

                    // Runtime overflow for DECIMAL128
                    assertException(
                            "WITH data AS (SELECT cast(9223372036854775807m as DECIMAL(19)) AS value) SELECT cast(value as int) FROM data",
                            80,
                            "inconvertible value: 9223372036854775807 [DECIMAL(19,0) -> INT]"
                    );

                    // Runtime overflow for DECIMAL256
                    assertException(
                            "WITH data AS (SELECT cast(99999999999999999999m as DECIMAL(40)) AS value) SELECT cast(value as int) FROM data",
                            81,
                            "inconvertible value: 99999999999999999999 [DECIMAL(40,0) -> INT]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "21474836.99\t21474836\n" +
                                    "-21474836.99\t-21474836\n" +
                                    "12345678.89\t12345678\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(21474836.99m as DECIMAL(20,2)) value " +
                                    "UNION ALL SELECT cast(-21474836.99m as DECIMAL(20,2)) " +
                                    "UNION ALL SELECT cast(12345678.89m as DECIMAL(20,2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(20,2))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "99.50\t99\n" +
                                    "-99.50\t-99\n" +
                                    "12.99\t12\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) value " +
                                    "UNION ALL SELECT cast(-99.5m as DECIMAL(4,2)) " +
                                    "UNION ALL SELECT cast(12.99m as DECIMAL(4,2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(4,2))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "21474836.9999999999\t21474836\n" +
                                    "-21474836.9999999999\t-21474836\n" +
                                    "12345678.1234567890\t12345678\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(21474836.9999999999m as DECIMAL(40,10)) value " +
                                    "UNION ALL SELECT cast(-21474836.9999999999m as DECIMAL(40,10)) " +
                                    "UNION ALL SELECT cast(12345678.1234567890m as DECIMAL(40,10)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(40,10))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "999999.999\t999999\n" +
                                    "-999999.999\t-999999\n" +
                                    "123456.789\t123456\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(999999.999m as DECIMAL(9,3)) value " +
                                    "UNION ALL SELECT cast(-999999.999m as DECIMAL(9,3)) " +
                                    "UNION ALL SELECT cast(123456.789m as DECIMAL(9,3)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(9, 3))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "2147483.999999\t2147483\n" +
                                    "-2147483.999999\t-2147483\n" +
                                    "1234567.890123\t1234567\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(2147483.999999m as DECIMAL(13,6)) value " +
                                    "UNION ALL SELECT cast(-2147483.999999m as DECIMAL(13,6)) " +
                                    "UNION ALL SELECT cast(1234567.890123m as DECIMAL(13,6)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(13, 6))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "9.9\t9\n" +
                                    "-9.9\t-9\n" +
                                    "0.5\t0\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(9.9m as DECIMAL(2,1)) value " +
                                    "UNION ALL SELECT cast(-9.9m as DECIMAL(2,1)) " +
                                    "UNION ALL SELECT cast(0.5m as DECIMAL(2,1)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(2,1))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "2147483647\t2147483647\n" +
                                    "-2147483647\t-2147483647\n" +
                                    "1234567890\t1234567890\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(2147483647m as DECIMAL(19)) value " +
                                    "UNION ALL SELECT cast(-2147483647m as DECIMAL(19)) " +
                                    "UNION ALL SELECT cast(1234567890m as DECIMAL(19)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(19))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "9999\t9999\n" +
                                    "-9999\t-9999\n" +
                                    "1234\t1234\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(9999m as DECIMAL(4)) value " +
                                    "UNION ALL SELECT cast(-9999m as DECIMAL(4)) " +
                                    "UNION ALL SELECT cast(1234m as DECIMAL(4)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(4))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "2147483647\t2147483647\n" +
                                    "-2147483647\t-2147483647\n" +
                                    "1234567890\t1234567890\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(2147483647m as DECIMAL(40)) value " +
                                    "UNION ALL SELECT cast(-2147483647m as DECIMAL(40)) " +
                                    "UNION ALL SELECT cast(1234567890m as DECIMAL(40)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(40))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "999999999\t999999999\n" +
                                    "-999999999\t-999999999\n" +
                                    "123456789\t123456789\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(999999999m as DECIMAL(9)) value " +
                                    "UNION ALL SELECT cast(-999999999m as DECIMAL(9)) " +
                                    "UNION ALL SELECT cast(123456789m as DECIMAL(9)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(9))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "2147483647\t2147483647\n" +
                                    "-2147483647\t-2147483647\n" +
                                    "1234567890\t1234567890\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(2147483647m as DECIMAL(18)) value " +
                                    "UNION ALL SELECT cast(-2147483647m as DECIMAL(18)) " +
                                    "UNION ALL SELECT cast(1234567890m as DECIMAL(18)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(18))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tint_value\n" +
                                    "99\t99\n" +
                                    "-99\t-99\n" +
                                    "0\t0\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(99m as DECIMAL(2)) value " +
                                    "UNION ALL SELECT cast(-99m as DECIMAL(2)) " +
                                    "UNION ALL SELECT cast(0m as DECIMAL(2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(2))) " +
                                    "SELECT value, cast(value as int) as int_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testTruncationBehavior() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Positive values - truncate towards zero
                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.1m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.5m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.9m as int)"
                    );

                    // Negative values - truncate towards zero
                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.1m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.5m as int)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.9m as int)"
                    );
                }
        );
    }
}