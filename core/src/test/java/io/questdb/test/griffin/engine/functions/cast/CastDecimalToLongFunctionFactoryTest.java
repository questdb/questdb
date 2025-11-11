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

public class CastDecimalToLongFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Truncates decimal part
                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast(123.45m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast(123.99m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123.45m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123.99m as long)"
                    );

                    // Zero with decimal places
                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.99m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.01m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(-0.99m as long)"
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
                                    "  functions: [value::long]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [123.45]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT 123.45m AS value) SELECT cast(value as long) FROM data");

                    // Runtime value without scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::long]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [123]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT 123m AS value) SELECT cast(value as long) FROM data");

                    // Expression should be constant folded
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [123L]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(123.45m as long)");
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
                                    "  functions: [value::long]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [99]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(99m as DECIMAL(2)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL16 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::long]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [9999]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(9999m as DECIMAL(4)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL32 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::long]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [999999999]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(999999999m as DECIMAL(9)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL64 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::long]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [999999999999999999]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(999999999999999999m as DECIMAL(18)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL128 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::long]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [9223372036854775807]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(9223372036854775807m as DECIMAL(19)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL256 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::long]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [9223372036854775807]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(9223372036854775807m as DECIMAL(40)) AS value) SELECT cast(value as long) FROM data");

                    // With scale - tests ScaledDecimalFunction
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::long]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [99.50]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) AS value) SELECT cast(value as long) FROM data");

                    // Constant folding for all decimal types
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99L]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(99m as long)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [9999L]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(9999m as long)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [999999999L]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(999999999m as long)");

                    // Constant folding with scale (truncation)
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [123L]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(123.45m as long)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99L]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(99.99m as long)");
                }
        );
    }

    @Test
    public void testCastFromDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807\n",
                            "select cast(cast(9223372036854775807m as DECIMAL(19)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9223372036854775807\n",
                            "select cast(cast(-9223372036854775807m as DECIMAL(19)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(19)) as long)"
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
                            "select cast(cast(9999m as DECIMAL(4)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9999\n",
                            "select cast(cast(-9999m as DECIMAL(4)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(4)) as long)"
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
                                    "9223372036854775807\n",
                            "select cast(cast(9223372036854775807m as DECIMAL(40)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9223372036854775807\n",
                            "select cast(cast(-9223372036854775807m as DECIMAL(40)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(40)) as long)"
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
                            "select cast(cast(999999999m as DECIMAL(9)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-999999999\n",
                            "select cast(cast(-999999999m as DECIMAL(9)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(9)) as long)"
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
                                    "999999999999999999\n",
                            "select cast(cast(999999999999999999m as DECIMAL(18)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-999999999999999999\n",
                            "select cast(cast(-999999999999999999m as DECIMAL(18)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(18)) as long)"
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
                            "select cast(cast(99m as DECIMAL(2)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-99\n",
                            "select cast(cast(-99m as DECIMAL(2)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(0m as DECIMAL(2)) as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "null\n",
                            "select cast(cast(null as DECIMAL(2)) as long)"
                    );
                }
        );
    }

    @Test
    public void testCastMaxLongValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max long value from decimal
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807\n",
                            "select cast(9223372036854775807m as long)"
                    );

                    // Max long value minus 1
                    assertSql(
                            "cast\n" +
                                    "9223372036854775806\n",
                            "select cast(9223372036854775806m as long)"
                    );

                    // Min long value + 1 (to avoid overflow with negation)
                    assertSql(
                            "cast\n" +
                                    "-9223372036854775807\n",
                            "select cast(-9223372036854775807m as long)"
                    );

                    // With scale - truncated
                    assertSql(
                            "cast\n" +
                                    "9223372036854775807\n",
                            "select cast(9223372036854775807.99m as long)"
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
                            "select cast(-1m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-999999999\n",
                            "select cast(-999999999m as long)"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for long
                    assertException(
                            "select cast(cast(9223372036854775808m as DECIMAL(19)) as long)",
                            7,
                            "inconvertible value: 9223372036854775808 [DECIMAL(19,0) -> LONG]"
                    );

                    assertException(
                            "select cast(cast(-9223372036854775809m as DECIMAL(19)) as long)",
                            7,
                            "inconvertible value: -9223372036854775809 [DECIMAL(19,0) -> LONG]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for long
                    assertException(
                            "select cast(cast(99999999999999999999m as DECIMAL(40)) as long)",
                            7,
                            "inconvertible value: 99999999999999999999 [DECIMAL(40,0) -> LONG]"
                    );

                    assertException(
                            "select cast(cast(-99999999999999999999m as DECIMAL(40)) as long)",
                            7,
                            "inconvertible value: -99999999999999999999 [DECIMAL(40,0) -> LONG]"
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
                            "select cast(0m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.0m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.00m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.000m as long)"
                    );
                }
        );
    }

    @Test
    public void testImplicitCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    // No implicit cast from decimal to long in arithmetic
                    // Must use explicit cast
                    assertSql(
                            "column\n" +
                                    "8888888\n",
                            "select cast(1234567m as long) + 7654321L"
                    );

                    // Runtime conversion
                    assertSql(
                            "column\n" +
                                    "8888888\n",
                            "with data as (select 1234567m x) select cast(x as long) + 7654321L from data"
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
                            "WITH data AS (SELECT cast(9223372036854775808.5m as DECIMAL(20,1)) AS value) SELECT cast(value as long) FROM data",
                            84,
                            "inconvertible value: 9223372036854775808.5 [DECIMAL(20,1) -> LONG]"
                    );

                    assertException(
                            "WITH data AS (SELECT cast(-9223372036854775809.5m as DECIMAL(20,1)) AS value) SELECT cast(value as long) FROM data",
                            85,
                            "inconvertible value: -9223372036854775809.5 [DECIMAL(20,1) -> LONG]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowUnscaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL128
                    assertException(
                            "WITH data AS (SELECT cast(9223372036854775808m as DECIMAL(19)) AS value) SELECT cast(value as long) FROM data",
                            80,
                            "inconvertible value: 9223372036854775808 [DECIMAL(19,0) -> LONG]"
                    );

                    // Runtime overflow for DECIMAL256
                    assertException(
                            "WITH data AS (SELECT cast(99999999999999999999m as DECIMAL(40)) AS value) SELECT cast(value as long) FROM data",
                            81,
                            "inconvertible value: 99999999999999999999 [DECIMAL(40,0) -> LONG]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "92233720368547758.99\t92233720368547758\n" +
                                    "-92233720368547758.99\t-92233720368547758\n" +
                                    "12345678901234567.89\t12345678901234567\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(92233720368547758.99m as DECIMAL(20,2)) value " +
                                    "UNION ALL SELECT cast(-92233720368547758.99m as DECIMAL(20,2)) " +
                                    "UNION ALL SELECT cast(12345678901234567.89m as DECIMAL(20,2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(20,2))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "99.50\t99\n" +
                                    "-99.50\t-99\n" +
                                    "12.99\t12\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) value " +
                                    "UNION ALL SELECT cast(-99.5m as DECIMAL(4,2)) " +
                                    "UNION ALL SELECT cast(12.99m as DECIMAL(4,2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(4,2))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "92233720368547758.9999999999\t92233720368547758\n" +
                                    "-92233720368547758.9999999999\t-92233720368547758\n" +
                                    "12345678901234567.1234567890\t12345678901234567\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(92233720368547758.9999999999m as DECIMAL(40,10)) value " +
                                    "UNION ALL SELECT cast(-92233720368547758.9999999999m as DECIMAL(40,10)) " +
                                    "UNION ALL SELECT cast(12345678901234567.1234567890m as DECIMAL(40,10)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(40,10))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "999999.999\t999999\n" +
                                    "-999999.999\t-999999\n" +
                                    "123456.789\t123456\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(999999.999m as DECIMAL(9,3)) value " +
                                    "UNION ALL SELECT cast(-999999.999m as DECIMAL(9,3)) " +
                                    "UNION ALL SELECT cast(123456.789m as DECIMAL(9,3)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(9, 3))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "999999999999.999999\t999999999999\n" +
                                    "-999999999999.999999\t-999999999999\n" +
                                    "123456789012.345678\t123456789012\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(999999999999.999999m as DECIMAL(18,6)) value " +
                                    "UNION ALL SELECT cast(-999999999999.999999m as DECIMAL(18,6)) " +
                                    "UNION ALL SELECT cast(123456789012.345678m as DECIMAL(18,6)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(18, 6))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "9.9\t9\n" +
                                    "-9.9\t-9\n" +
                                    "0.5\t0\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(9.9m as DECIMAL(2,1)) value " +
                                    "UNION ALL SELECT cast(-9.9m as DECIMAL(2,1)) " +
                                    "UNION ALL SELECT cast(0.5m as DECIMAL(2,1)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(2,1))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "9223372036854775807\t9223372036854775807\n" +
                                    "-9223372036854775807\t-9223372036854775807\n" +
                                    "1234567890123456789\t1234567890123456789\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(9223372036854775807m as DECIMAL(19)) value " +
                                    "UNION ALL SELECT cast(-9223372036854775807m as DECIMAL(19)) " +
                                    "UNION ALL SELECT cast(1234567890123456789m as DECIMAL(19)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(19))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "9999\t9999\n" +
                                    "-9999\t-9999\n" +
                                    "1234\t1234\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(9999m as DECIMAL(4)) value " +
                                    "UNION ALL SELECT cast(-9999m as DECIMAL(4)) " +
                                    "UNION ALL SELECT cast(1234m as DECIMAL(4)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(4))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "9223372036854775807\t9223372036854775807\n" +
                                    "-9223372036854775807\t-9223372036854775807\n" +
                                    "1234567890123456789\t1234567890123456789\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(9223372036854775807m as DECIMAL(40)) value " +
                                    "UNION ALL SELECT cast(-9223372036854775807m as DECIMAL(40)) " +
                                    "UNION ALL SELECT cast(1234567890123456789m as DECIMAL(40)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(40))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "999999999\t999999999\n" +
                                    "-999999999\t-999999999\n" +
                                    "123456789\t123456789\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(999999999m as DECIMAL(9)) value " +
                                    "UNION ALL SELECT cast(-999999999m as DECIMAL(9)) " +
                                    "UNION ALL SELECT cast(123456789m as DECIMAL(9)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(9))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "999999999999999999\t999999999999999999\n" +
                                    "-999999999999999999\t-999999999999999999\n" +
                                    "123456789012345678\t123456789012345678\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(999999999999999999m as DECIMAL(18)) value " +
                                    "UNION ALL SELECT cast(-999999999999999999m as DECIMAL(18)) " +
                                    "UNION ALL SELECT cast(123456789012345678m as DECIMAL(18)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(18))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tlong_value\n" +
                                    "99\t99\n" +
                                    "-99\t-99\n" +
                                    "0\t0\n" +
                                    "\tnull\n",
                            "WITH data AS (SELECT cast(99m as DECIMAL(2)) value " +
                                    "UNION ALL SELECT cast(-99m as DECIMAL(2)) " +
                                    "UNION ALL SELECT cast(0m as DECIMAL(2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(2))) " +
                                    "SELECT value, cast(value as long) as long_value FROM data"
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
                            "select cast(1.1m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.5m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.9m as long)"
                    );

                    // Negative values - truncate towards zero
                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.1m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.5m as long)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.9m as long)"
                    );
                }
        );
    }
}