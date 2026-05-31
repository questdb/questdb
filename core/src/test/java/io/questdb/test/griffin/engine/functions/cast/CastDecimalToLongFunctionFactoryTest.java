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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CastDecimalToLongFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Truncates decimal part
                    assertQuery("select cast(123.45m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123
                                    """);

                    assertQuery("select cast(123.99m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123
                                    """);

                    assertQuery("select cast(-123.45m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123
                                    """);

                    assertQuery("select cast(-123.99m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123
                                    """);

                    // Zero with decimal places
                    assertQuery("select cast(0.99m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(0.01m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(-0.99m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);
                }
        );
    }

    @Test
    public void testCastExplains() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime value with scale
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::long]
                                        VirtualRecord
                                          functions: [123.45]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT 123.45m AS value) SELECT cast(value as long) FROM data");

                    // Runtime value without scale
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::long]
                                        VirtualRecord
                                          functions: [123]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT 123m AS value) SELECT cast(value as long) FROM data");

                    // Expression should be constant folded
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [123L]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(123.45m as long)");
                }
        );
    }

    @Test
    public void testCastExplainsForDifferentDecimalTypes() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::long]
                                        VirtualRecord
                                          functions: [99]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(99m as DECIMAL(2)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL16 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::long]
                                        VirtualRecord
                                          functions: [9999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(9999m as DECIMAL(4)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL32 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::long]
                                        VirtualRecord
                                          functions: [999999999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(999999999m as DECIMAL(9)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL64 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::long]
                                        VirtualRecord
                                          functions: [999999999999999999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(999999999999999999m as DECIMAL(18)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL128 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::long]
                                        VirtualRecord
                                          functions: [9223372036854775807]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(9223372036854775807m as DECIMAL(19)) AS value) SELECT cast(value as long) FROM data");

                    // DECIMAL256 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::long]
                                        VirtualRecord
                                          functions: [9223372036854775807]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(9223372036854775807m as DECIMAL(40)) AS value) SELECT cast(value as long) FROM data");

                    // With scale - tests ScaledDecimalFunction
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::long]
                                        VirtualRecord
                                          functions: [99.50]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) AS value) SELECT cast(value as long) FROM data");

                    // Constant folding for all decimal types
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [99L]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(99m as long)");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [9999L]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(9999m as long)");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [999999999L]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(999999999m as long)");

                    // Constant folding with scale (truncation)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [123L]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(123.45m as long)");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [99L]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(99.99m as long)");
                }
        );
    }

    @Test
    public void testCastFromDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(9223372036854775807m as DECIMAL(19)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9223372036854775807
                                    """);

                    assertQuery("select cast(cast(-9223372036854775807m as DECIMAL(19)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9223372036854775807
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(19)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    null
                                    """);
                }
        );
    }

    @Test
    public void testCastFromDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(9999m as DECIMAL(4)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9999
                                    """);

                    assertQuery("select cast(cast(-9999m as DECIMAL(4)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9999
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(4)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    null
                                    """);
                }
        );
    }

    @Test
    public void testCastFromDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(9223372036854775807m as DECIMAL(40)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9223372036854775807
                                    """);

                    assertQuery("select cast(cast(-9223372036854775807m as DECIMAL(40)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9223372036854775807
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(40)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    null
                                    """);
                }
        );
    }

    @Test
    public void testCastFromDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(999999999m as DECIMAL(9)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    999999999
                                    """);

                    assertQuery("select cast(cast(-999999999m as DECIMAL(9)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -999999999
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(9)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    null
                                    """);
                }
        );
    }

    @Test
    public void testCastFromDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(999999999999999999m as DECIMAL(18)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    999999999999999999
                                    """);

                    assertQuery("select cast(cast(-999999999999999999m as DECIMAL(18)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -999999999999999999
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(18)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    null
                                    """);
                }
        );
    }

    @Test
    public void testCastFromDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(99m as DECIMAL(2)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99
                                    """);

                    assertQuery("select cast(cast(-99m as DECIMAL(2)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -99
                                    """);

                    assertQuery("select cast(cast(0m as DECIMAL(2)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(2)) as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    null
                                    """);
                }
        );
    }

    @Test
    public void testCastMaxLongValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max long value from decimal
                    assertQuery("select cast(9223372036854775807m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9223372036854775807
                                    """);

                    // Max long value minus 1
                    assertQuery("select cast(9223372036854775806m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9223372036854775806
                                    """);

                    // Min long value + 1 (to avoid overflow with negation)
                    assertQuery("select cast(-9223372036854775807m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9223372036854775807
                                    """);

                    // With scale - truncated
                    assertQuery("select cast(9223372036854775807.99m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9223372036854775807
                                    """);
                }
        );
    }

    @Test
    public void testCastNegativeValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(-1m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1
                                    """);

                    assertQuery("select cast(-123m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123
                                    """);

                    assertQuery("select cast(-999999999m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -999999999
                                    """);
                }
        );
    }

    @Test
    public void testCastOverflowDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for long
                    assertQuery("select cast(cast(9223372036854775808m as DECIMAL(19)) as long)")
                            .fails(7, "inconvertible value: 9223372036854775808 [DECIMAL(19,0) -> LONG]");

                    assertQuery("select cast(cast(-9223372036854775809m as DECIMAL(19)) as long)")
                            .fails(7, "inconvertible value: -9223372036854775809 [DECIMAL(19,0) -> LONG]");
                }
        );
    }

    @Test
    public void testCastOverflowDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for long
                    assertQuery("select cast(cast(99999999999999999999m as DECIMAL(40)) as long)")
                            .fails(7, "inconvertible value: 99999999999999999999 [DECIMAL(40,0) -> LONG]");

                    assertQuery("select cast(cast(-99999999999999999999m as DECIMAL(40)) as long)")
                            .fails(7, "inconvertible value: -99999999999999999999 [DECIMAL(40,0) -> LONG]");
                }
        );
    }

    @Test
    public void testCastZeroValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(0m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(0.0m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(0.00m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(0.000m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);
                }
        );
    }

    @Test
    public void testImplicitCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    // No implicit cast from decimal to long in arithmetic
                    // Must use explicit cast
                    assertQuery("select cast(1234567m as long) + 7654321L")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    column
                                    8888888
                                    """);

                    // Runtime conversion
                    assertQuery("with data as (select 1234567m x) select cast(x as long) + 7654321L from data")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    column
                                    8888888
                                    """);
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowScaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow with scaled decimal
                    assertQuery("WITH data AS (SELECT cast(9223372036854775808.5m as DECIMAL(20,1)) AS value) SELECT cast(value as long) FROM data")
                            .fails(84, "inconvertible value: 9223372036854775808.5 [DECIMAL(20,1) -> LONG]");

                    assertQuery("WITH data AS (SELECT cast(-9223372036854775809.5m as DECIMAL(20,1)) AS value) SELECT cast(value as long) FROM data")
                            .fails(85, "inconvertible value: -9223372036854775809.5 [DECIMAL(20,1) -> LONG]");
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowUnscaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL128
                    assertQuery("WITH data AS (SELECT cast(9223372036854775808m as DECIMAL(19)) AS value) SELECT cast(value as long) FROM data")
                            .fails(80, "inconvertible value: 9223372036854775808 [DECIMAL(19,0) -> LONG]");

                    // Runtime overflow for DECIMAL256
                    assertQuery("WITH data AS (SELECT cast(99999999999999999999m as DECIMAL(40)) AS value) SELECT cast(value as long) FROM data")
                            .fails(81, "inconvertible value: 99999999999999999999 [DECIMAL(40,0) -> LONG]");
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(92233720368547758.99m as DECIMAL(20,2)) value " +
                                "UNION ALL SELECT cast(-92233720368547758.99m as DECIMAL(20,2)) " +
                                "UNION ALL SELECT cast(12345678901234567.89m as DECIMAL(20,2)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(20,2))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                92233720368547758.99\t92233720368547758
                                -92233720368547758.99\t-92233720368547758
                                12345678901234567.89\t12345678901234567
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) value " +
                                "UNION ALL SELECT cast(-99.5m as DECIMAL(4,2)) " +
                                "UNION ALL SELECT cast(12.99m as DECIMAL(4,2)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(4,2))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                99.50\t99
                                -99.50\t-99
                                12.99\t12
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(92233720368547758.9999999999m as DECIMAL(40,10)) value " +
                                "UNION ALL SELECT cast(-92233720368547758.9999999999m as DECIMAL(40,10)) " +
                                "UNION ALL SELECT cast(12345678901234567.1234567890m as DECIMAL(40,10)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(40,10))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                92233720368547758.9999999999\t92233720368547758
                                -92233720368547758.9999999999\t-92233720368547758
                                12345678901234567.1234567890\t12345678901234567
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(999999.999m as DECIMAL(9,3)) value " +
                                "UNION ALL SELECT cast(-999999.999m as DECIMAL(9,3)) " +
                                "UNION ALL SELECT cast(123456.789m as DECIMAL(9,3)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(9, 3))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                999999.999\t999999
                                -999999.999\t-999999
                                123456.789\t123456
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(999999999999.999999m as DECIMAL(18,6)) value " +
                                "UNION ALL SELECT cast(-999999999999.999999m as DECIMAL(18,6)) " +
                                "UNION ALL SELECT cast(123456789012.345678m as DECIMAL(18,6)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(18, 6))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                999999999999.999999\t999999999999
                                -999999999999.999999\t-999999999999
                                123456789012.345678\t123456789012
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(9.9m as DECIMAL(2,1)) value " +
                                "UNION ALL SELECT cast(-9.9m as DECIMAL(2,1)) " +
                                "UNION ALL SELECT cast(0.5m as DECIMAL(2,1)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(2,1))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                9.9\t9
                                -9.9\t-9
                                0.5\t0
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(9223372036854775807m as DECIMAL(19)) value " +
                                "UNION ALL SELECT cast(-9223372036854775807m as DECIMAL(19)) " +
                                "UNION ALL SELECT cast(1234567890123456789m as DECIMAL(19)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(19))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                9223372036854775807\t9223372036854775807
                                -9223372036854775807\t-9223372036854775807
                                1234567890123456789\t1234567890123456789
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(9999m as DECIMAL(4)) value " +
                                "UNION ALL SELECT cast(-9999m as DECIMAL(4)) " +
                                "UNION ALL SELECT cast(1234m as DECIMAL(4)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(4))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                9999\t9999
                                -9999\t-9999
                                1234\t1234
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(9223372036854775807m as DECIMAL(40)) value " +
                                "UNION ALL SELECT cast(-9223372036854775807m as DECIMAL(40)) " +
                                "UNION ALL SELECT cast(1234567890123456789m as DECIMAL(40)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(40))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                9223372036854775807\t9223372036854775807
                                -9223372036854775807\t-9223372036854775807
                                1234567890123456789\t1234567890123456789
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(999999999m as DECIMAL(9)) value " +
                                "UNION ALL SELECT cast(-999999999m as DECIMAL(9)) " +
                                "UNION ALL SELECT cast(123456789m as DECIMAL(9)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(9))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                999999999\t999999999
                                -999999999\t-999999999
                                123456789\t123456789
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(999999999999999999m as DECIMAL(18)) value " +
                                "UNION ALL SELECT cast(-999999999999999999m as DECIMAL(18)) " +
                                "UNION ALL SELECT cast(123456789012345678m as DECIMAL(18)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(18))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                999999999999999999\t999999999999999999
                                -999999999999999999\t-999999999999999999
                                123456789012345678\t123456789012345678
                                \tnull
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(99m as DECIMAL(2)) value " +
                                "UNION ALL SELECT cast(-99m as DECIMAL(2)) " +
                                "UNION ALL SELECT cast(0m as DECIMAL(2)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(2))) " +
                                "SELECT value, cast(value as long) as long_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tlong_value
                                99\t99
                                -99\t-99
                                0\t0
                                \tnull
                                """)
        );
    }

    @Test
    public void testTruncationBehavior() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Positive values - truncate towards zero
                    assertQuery("select cast(1.1m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1
                                    """);

                    assertQuery("select cast(1.5m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1
                                    """);

                    assertQuery("select cast(1.9m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1
                                    """);

                    // Negative values - truncate towards zero
                    assertQuery("select cast(-1.1m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1
                                    """);

                    assertQuery("select cast(-1.5m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1
                                    """);

                    assertQuery("select cast(-1.9m as long)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1
                                    """);
                }
        );
    }
}