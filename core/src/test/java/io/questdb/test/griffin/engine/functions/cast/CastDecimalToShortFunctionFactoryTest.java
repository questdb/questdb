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

public class CastDecimalToShortFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Truncates decimal part
                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast(123.45m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast(123.99m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123.45m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123.99m as short)"
                    );

                    // Zero with decimal places
                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.99m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.01m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(-0.99m as short)"
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
                                    "  functions: [value::short]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [123.45]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT 123.45m AS value) SELECT cast(value as short) FROM data");

                    // Runtime value without scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::short]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [123]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT 123m AS value) SELECT cast(value as short) FROM data");

                    // Expression should be constant folded
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [123]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(123.45m as short)");
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
                                    "  functions: [value::short]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [99]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(99m as DECIMAL(2)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL16 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::short]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [9999]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(9999m as DECIMAL(4)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL32 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::short]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [32767]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(32767m as DECIMAL(5)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL64 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::short]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [32767]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(32767m as DECIMAL(10)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL128 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::short]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [32767]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(32767m as DECIMAL(19)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL256 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::short]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [32767]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(32767m as DECIMAL(40)) AS value) SELECT cast(value as short) FROM data");

                    // With scale - tests ScaledDecimalFunction
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::short]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [99.50]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) AS value) SELECT cast(value as short) FROM data");

                    // Constant folding for all decimal types
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(99m as short)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [9999]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(9999m as short)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [32767]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(32767m as short)");

                    // Constant folding with scale (truncation)
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [123]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(123.45m as short)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(99.99m as short)");
                }
        );
    }

    @Test
    public void testCastFromDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "32767\n",
                            "select cast(cast(32767m as DECIMAL(19)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-32767\n",
                            "select cast(cast(-32767m as DECIMAL(19)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(19)) as short)"
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
                            "select cast(cast(9999m as DECIMAL(4)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9999\n",
                            "select cast(cast(-9999m as DECIMAL(4)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(4)) as short)"
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
                                    "32767\n",
                            "select cast(cast(32767m as DECIMAL(40)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-32767\n",
                            "select cast(cast(-32767m as DECIMAL(40)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(40)) as short)"
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
                                    "32767\n",
                            "select cast(cast(32767m as DECIMAL(9)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-32767\n",
                            "select cast(cast(-32767m as DECIMAL(9)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(9)) as short)"
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
                                    "32767\n",
                            "select cast(cast(32767m as DECIMAL(18)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-32767\n",
                            "select cast(cast(-32767m as DECIMAL(18)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(18)) as short)"
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
                            "select cast(cast(99m as DECIMAL(2)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-99\n",
                            "select cast(cast(-99m as DECIMAL(2)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(0m as DECIMAL(2)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(2)) as short)"
                    );
                }
        );
    }

    @Test
    public void testCastMaxShortValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max short value from decimal
                    assertSql(
                            "cast\n" +
                                    "32767\n",
                            "select cast(32767m as short)"
                    );

                    // Max short value minus 1
                    assertSql(
                            "cast\n" +
                                    "32766\n",
                            "select cast(32766m as short)"
                    );

                    // Min short value + 1 (to avoid overflow with negation)
                    assertSql(
                            "cast\n" +
                                    "-32767\n",
                            "select cast(-32767m as short)"
                    );

                    // With scale - truncated
                    assertSql(
                            "cast\n" +
                                    "32767\n",
                            "select cast(32767.99m as short)"
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
                            "select cast(-1m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-9999\n",
                            "select cast(-9999m as short)"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertException(
                            "select cast(cast(2147483647m as DECIMAL(19)) as short)",
                            7,
                            "inconvertible value: 2147483647 [DECIMAL(19,0) -> SHORT]"
                    );

                    assertException(
                            "select cast(cast(-2147483648m as DECIMAL(19)) as short)",
                            7,
                            "inconvertible value: -2147483648 [DECIMAL(19,0) -> SHORT]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertException(
                            "select cast(cast(32768m as DECIMAL(5)) as short)",
                            7,
                            "inconvertible value: 32768 [DECIMAL(5,0) -> SHORT]"
                    );

                    assertException(
                            "select cast(cast(-32769m as DECIMAL(5)) as short)",
                            7,
                            "inconvertible value: -32769 [DECIMAL(5,0) -> SHORT]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertException(
                            "select cast(cast(999999999999m as DECIMAL(40)) as short)",
                            7,
                            "inconvertible value: 999999999999 [DECIMAL(40,0) -> SHORT]"
                    );

                    assertException(
                            "select cast(cast(-999999999999m as DECIMAL(40)) as short)",
                            7,
                            "inconvertible value: -999999999999 [DECIMAL(40,0) -> SHORT]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertException(
                            "select cast(cast(100000m as DECIMAL(6)) as short)",
                            7,
                            "inconvertible value: 100000 [DECIMAL(6,0) -> SHORT]"
                    );

                    assertException(
                            "select cast(cast(-100000m as DECIMAL(6)) as short)",
                            7,
                            "inconvertible value: -100000 [DECIMAL(6,0) -> SHORT]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertException(
                            "select cast(cast(999999999m as DECIMAL(18)) as short)",
                            7,
                            "inconvertible value: 999999999 [DECIMAL(18,0) -> SHORT]"
                    );

                    assertException(
                            "select cast(cast(-999999999m as DECIMAL(18)) as short)",
                            7,
                            "inconvertible value: -999999999 [DECIMAL(18,0) -> SHORT]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 can hold max 99, which fits in SHORT
                    assertSql(
                            "cast\n" +
                                    "99\n",
                            "select cast(cast(99m as DECIMAL(2)) as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-99\n",
                            "select cast(cast(-99m as DECIMAL(2)) as short)"
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
                            "select cast(0m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.0m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.00m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.000m as short)"
                    );
                }
        );
    }

    @Test
    public void testImplicitCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    // No implicit cast from decimal to short in arithmetic
                    // Must use explicit cast
                    assertSql(
                            "column\n" +
                                    "199\n",
                            "select cast(99m as short) + cast(100 as short)"
                    );

                    // Runtime conversion
                    assertSql(
                            "column\n" +
                                    "199\n",
                            "with data as (select 99m x) select cast(x as short) + cast(100 as short) from data"
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
                            "WITH data AS (SELECT cast(32768.5m as DECIMAL(6,1)) AS value) SELECT cast(value as short) FROM data",
                            69,
                            "inconvertible value: 32768.5 [DECIMAL(6,1) -> SHORT]"
                    );

                    assertException(
                            "WITH data AS (SELECT cast(-32769.5m as DECIMAL(6,1)) AS value) SELECT cast(value as short) FROM data",
                            70,
                            "inconvertible value: -32769.5 [DECIMAL(6,1) -> SHORT]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowUnscaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL16
                    assertException(
                            "WITH data AS (SELECT cast(32768m as DECIMAL(5)) AS value) SELECT cast(value as short) FROM data",
                            65,
                            "inconvertible value: 32768 [DECIMAL(5,0) -> SHORT]"
                    );

                    // Runtime overflow for DECIMAL32
                    assertException(
                            "WITH data AS (SELECT cast(100000m as DECIMAL(6)) AS value) SELECT cast(value as short) FROM data",
                            66,
                            "inconvertible value: 100000 [DECIMAL(6,0) -> SHORT]"
                    );

                    // Runtime overflow for DECIMAL64
                    assertException(
                            "WITH data AS (SELECT cast(999999999m as DECIMAL(18)) AS value) SELECT cast(value as short) FROM data",
                            70,
                            "inconvertible value: 999999999 [DECIMAL(18,0) -> SHORT]"
                    );

                    // Runtime overflow for DECIMAL128
                    assertException(
                            "WITH data AS (SELECT cast(2147483647m as DECIMAL(19)) AS value) SELECT cast(value as short) FROM data",
                            71,
                            "inconvertible value: 2147483647 [DECIMAL(19,0) -> SHORT]"
                    );

                    // Runtime overflow for DECIMAL256
                    assertException(
                            "WITH data AS (SELECT cast(999999999999m as DECIMAL(40)) AS value) SELECT cast(value as short) FROM data",
                            73,
                            "inconvertible value: 999999999999 [DECIMAL(40,0) -> SHORT]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "32766.99\t32766\n" +
                                    "-32766.99\t-32766\n" +
                                    "12345.67\t12345\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(32766.99m as DECIMAL(20,2)) value " +
                                    "UNION ALL SELECT cast(-32766.99m as DECIMAL(20,2)) " +
                                    "UNION ALL SELECT cast(12345.67m as DECIMAL(20,2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(20,2))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "99.50\t99\n" +
                                    "-99.50\t-99\n" +
                                    "12.99\t12\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) value " +
                                    "UNION ALL SELECT cast(-99.5m as DECIMAL(4,2)) " +
                                    "UNION ALL SELECT cast(12.99m as DECIMAL(4,2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(4,2))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "32766.9999999999\t32766\n" +
                                    "-32766.9999999999\t-32766\n" +
                                    "12345.1234567890\t12345\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(32766.9999999999m as DECIMAL(40,10)) value " +
                                    "UNION ALL SELECT cast(-32766.9999999999m as DECIMAL(40,10)) " +
                                    "UNION ALL SELECT cast(12345.1234567890m as DECIMAL(40,10)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(40,10))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "32766.999\t32766\n" +
                                    "-32766.999\t-32766\n" +
                                    "12345.678\t12345\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(32766.999m as DECIMAL(8,3)) value " +
                                    "UNION ALL SELECT cast(-32766.999m as DECIMAL(8,3)) " +
                                    "UNION ALL SELECT cast(12345.678m as DECIMAL(8,3)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(8, 3))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "32766.999999\t32766\n" +
                                    "-32766.999999\t-32766\n" +
                                    "12345.678901\t12345\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(32766.999999m as DECIMAL(11,6)) value " +
                                    "UNION ALL SELECT cast(-32766.999999m as DECIMAL(11,6)) " +
                                    "UNION ALL SELECT cast(12345.678901m as DECIMAL(11,6)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(11, 6))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "9.9\t9\n" +
                                    "-9.9\t-9\n" +
                                    "0.5\t0\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(9.9m as DECIMAL(2,1)) value " +
                                    "UNION ALL SELECT cast(-9.9m as DECIMAL(2,1)) " +
                                    "UNION ALL SELECT cast(0.5m as DECIMAL(2,1)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(2,1))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "32767\t32767\n" +
                                    "-32767\t-32767\n" +
                                    "12345\t12345\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(32767m as DECIMAL(19)) value " +
                                    "UNION ALL SELECT cast(-32767m as DECIMAL(19)) " +
                                    "UNION ALL SELECT cast(12345m as DECIMAL(19)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(19))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "9999\t9999\n" +
                                    "-9999\t-9999\n" +
                                    "1234\t1234\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(9999m as DECIMAL(4)) value " +
                                    "UNION ALL SELECT cast(-9999m as DECIMAL(4)) " +
                                    "UNION ALL SELECT cast(1234m as DECIMAL(4)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(4))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "32767\t32767\n" +
                                    "-32767\t-32767\n" +
                                    "12345\t12345\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(32767m as DECIMAL(40)) value " +
                                    "UNION ALL SELECT cast(-32767m as DECIMAL(40)) " +
                                    "UNION ALL SELECT cast(12345m as DECIMAL(40)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(40))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "32767\t32767\n" +
                                    "-32767\t-32767\n" +
                                    "12345\t12345\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(32767m as DECIMAL(9)) value " +
                                    "UNION ALL SELECT cast(-32767m as DECIMAL(9)) " +
                                    "UNION ALL SELECT cast(12345m as DECIMAL(9)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(9))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "32767\t32767\n" +
                                    "-32767\t-32767\n" +
                                    "12345\t12345\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(32767m as DECIMAL(18)) value " +
                                    "UNION ALL SELECT cast(-32767m as DECIMAL(18)) " +
                                    "UNION ALL SELECT cast(12345m as DECIMAL(18)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(18))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tshort_value\n" +
                                    "99\t99\n" +
                                    "-99\t-99\n" +
                                    "0\t0\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(99m as DECIMAL(2)) value " +
                                    "UNION ALL SELECT cast(-99m as DECIMAL(2)) " +
                                    "UNION ALL SELECT cast(0m as DECIMAL(2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(2))) " +
                                    "SELECT value, cast(value as short) as short_value FROM data"
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
                            "select cast(1.1m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.5m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.9m as short)"
                    );

                    // Negative values - truncate towards zero
                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.1m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.5m as short)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.9m as short)"
                    );
                }
        );
    }
}