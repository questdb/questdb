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

public class CastIntToDecimalFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastExplains() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime value needs scaling
                    assertSql("""
                            QUERY PLAN
                            VirtualRecord
                              functions: [value::DECIMAL(5,2)]
                                VirtualRecord
                                  functions: [123]
                                    long_sequence count: 1
                            """, "EXPLAIN WITH data AS (SELECT cast(123 as int) AS value) SELECT cast(value as DECIMAL(5, 2)) FROM data");

                    // Runtime value doesn't need scaling
                    assertSql("""
                            QUERY PLAN
                            VirtualRecord
                              functions: [value::DECIMAL(5,0)]
                                VirtualRecord
                                  functions: [123]
                                    long_sequence count: 1
                            """, "EXPLAIN WITH data AS (SELECT cast(123 as int) AS value) SELECT cast(value as DECIMAL(5, 0)) FROM data");

                    // Expression should be constant folded
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [1.00]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(cast(1 as int) as DECIMAL(5, 2))");
                }
        );
    }

    @Test
    public void testCastExplainsForDifferentDecimalTypes() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 unscaled (uses CastDecimal64UnscaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(2,0)]
                                        VirtualRecord
                                          functions: [99]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(99 as int) AS value) SELECT cast(value as DECIMAL(2)) FROM data");

                    // DECIMAL16 unscaled (uses CastDecimal64UnscaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(4,0)]
                                        VirtualRecord
                                          functions: [9999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(9999 as int) AS value) SELECT cast(value as DECIMAL(4)) FROM data");

                    // DECIMAL32 unscaled (uses CastDecimal64UnscaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(9,0)]
                                        VirtualRecord
                                          functions: [999999999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(999999999 as int) AS value) SELECT cast(value as DECIMAL(9)) FROM data");

                    // DECIMAL64 unscaled (uses CastDecimal64UnscaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(10,0)]
                                        VirtualRecord
                                          functions: [2147483647]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(2147483647 as int) AS value) SELECT cast(value as DECIMAL(10)) FROM data");

                    // DECIMAL128 unscaled (uses CastDecimal128UnscaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(19,0)]
                                        VirtualRecord
                                          functions: [2147483647]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(2147483647 as int) AS value) SELECT cast(value as DECIMAL(19)) FROM data");

                    // DECIMAL256 unscaled (uses CastDecimal256UnscaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(40,0)]
                                        VirtualRecord
                                          functions: [2147483647]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(2147483647 as int) AS value) SELECT cast(value as DECIMAL(40)) FROM data");

                    // DECIMAL8 scaled (uses CastDecimalScaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(2,1)]
                                        VirtualRecord
                                          functions: [9]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(9 as int) AS value) SELECT cast(value as DECIMAL(2,1)) FROM data");

                    // DECIMAL16 scaled (uses CastDecimalScaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(4,2)]
                                        VirtualRecord
                                          functions: [99]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(99 as int) AS value) SELECT cast(value as DECIMAL(4,2)) FROM data");

                    // DECIMAL32 scaled (uses CastDecimalScaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(9,3)]
                                        VirtualRecord
                                          functions: [999999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(999999 as int) AS value) SELECT cast(value as DECIMAL(9,3)) FROM data");

                    // DECIMAL64 scaled (uses CastDecimalScaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(10,3)]
                                        VirtualRecord
                                          functions: [2147483]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(2147483 as int) AS value) SELECT cast(value as DECIMAL(10,3)) FROM data");

                    // DECIMAL128 scaled (uses CastDecimalScaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(19,2)]
                                        VirtualRecord
                                          functions: [21474836]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(21474836 as int) AS value) SELECT cast(value as DECIMAL(19,2)) FROM data");

                    // DECIMAL256 scaled (uses CastDecimalScaledFunc)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::DECIMAL(40,10)]
                                        VirtualRecord
                                          functions: [21474836]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(21474836 as int) AS value) SELECT cast(value as DECIMAL(40,10)) FROM data");

                    // Constant folding for all decimal types
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [99]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(cast(99 as int) as DECIMAL(2))");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [9999]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(cast(9999 as int) as DECIMAL(4))");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [999999999]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(cast(999999999 as int) as DECIMAL(9))");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [2147483647]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(cast(2147483647 as int) as DECIMAL(10))");

                    // Constant folding with scale
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [9.0]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(cast(9 as int) as DECIMAL(2,1))");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [99.00]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(cast(99 as int) as DECIMAL(4,2))");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [999999.000]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(cast(999999 as int) as DECIMAL(9,3))");
                }
        );
    }

    @Test
    public void testCastHighScaleLowPrecision() throws Exception {
        assertMemoryLeak(
                () -> {
                    // When scale equals precision, only 0 can be represented
                    assertQuery("select cast(cast(0 as int) as DECIMAL(2,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.00
                                    """);

                    // Any non-zero value should overflow
                    assertQuery("select cast(cast(1 as int) as DECIMAL(2,2))")
                            .fails(12, "inconvertible value: 1 [INT -> DECIMAL(2,2)]");

                    assertQuery("select cast(cast(-1 as int) as DECIMAL(2,2))")
                            .fails(12, "inconvertible value: -1 [INT -> DECIMAL(2,2)]");
                }
        );
    }

    @Test
    public void testCastLargeScaleValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Test with large scale values
                    assertQuery("select cast(cast(1 as int) as DECIMAL(20,10))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.0000000000
                                    """);

                    assertQuery("select cast(cast(123 as int) as DECIMAL(21,18))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123.000000000000000000
                                    """);
                }
        );
    }

    @Test
    public void testCastMaxIntValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max int value to decimal with sufficient precision
                    assertQuery("select cast(2147483647 as DECIMAL(10))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    2147483647
                                    """);

                    // Min int value to decimal with sufficient precision
                    assertQuery("select cast(-2147483647 as DECIMAL(10))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -2147483647
                                    """);

                    // Max int with scale requires higher precision
                    assertQuery("select cast(2147483647 as DECIMAL(12,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    2147483647.00
                                    """);
                }
        );
    }

    @Test
    public void testCastNegativeValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(-1 as int) as DECIMAL(2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1
                                    """);

                    assertQuery("select cast(cast(-123 as int) as DECIMAL(5,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123.00
                                    """);
                }
        );
    }

    @Test
    public void testCastOverflowDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(10000 as int) as DECIMAL(4))")
                            .fails(12, "inconvertible value: 10000 [INT -> DECIMAL(4,0)]");

                    assertQuery("select cast(cast(-10000 as int) as DECIMAL(4))")
                            .fails(12, "inconvertible value: -10000 [INT -> DECIMAL(4,0)]");
                }
        );
    }

    @Test
    public void testCastOverflowDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(1000000000 as int) as DECIMAL(9))")
                            .fails(12, "inconvertible value: 1000000000 [INT -> DECIMAL(9,0)]");

                    assertQuery("select cast(cast(-1000000000 as int) as DECIMAL(9))")
                            .fails(12, "inconvertible value: -1000000000 [INT -> DECIMAL(9,0)]");
                }
        );
    }

    @Test
    public void testCastOverflowDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(128 as int) as DECIMAL(2))")
                            .fails(12, "inconvertible value: 128 [INT -> DECIMAL(2,0)]");

                    assertQuery("select cast(cast(-129 as int) as DECIMAL(2))")
                            .fails(12, "inconvertible value: -129 [INT -> DECIMAL(2,0)]");
                }
        );
    }

    @Test
    public void testCastOverflowWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // 100 with scale 2 requires precision of at least 5 (100.00)
                    assertQuery("select cast(cast(100 as int) as DECIMAL(4,2))")
                            .fails(12, "inconvertible value: 100 [INT -> DECIMAL(4,2)]");

                    // 1000 with scale 3 requires precision of at least 7 (1000.000)
                    assertQuery("select cast(cast(1000 as int) as DECIMAL(5,3))")
                            .fails(12, "inconvertible value: 1000 [INT -> DECIMAL(5,3)]");
                }
        );
    }

    @Test
    public void testCastToDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(2147483647 as int) as DECIMAL(19))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    2147483647
                                    """);

                    assertQuery("select cast(cast(-2147483647 as int) as DECIMAL(19))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -2147483647
                                    """);

                    assertQuery("select cast(cast(null as int) as DECIMAL(19))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(9999 as int) as DECIMAL(4))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9999
                                    """);

                    assertQuery("select cast(cast(-9999 as int) as DECIMAL(4))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9999
                                    """);

                    assertQuery("select cast(cast(null as int) as DECIMAL(4))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(2147483647 as int) as DECIMAL(40))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    2147483647
                                    """);

                    assertQuery("select cast(cast(-2147483647 as int) as DECIMAL(40))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -2147483647
                                    """);

                    assertQuery("select cast(cast(null as int) as DECIMAL(40))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(999999999 as int) as DECIMAL(9))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    999999999
                                    """);

                    assertQuery("select cast(cast(-999999999 as int) as DECIMAL(9))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -999999999
                                    """);

                    assertQuery("select cast(cast(null as int) as DECIMAL(9))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(2147483647 as int) as DECIMAL(10))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    2147483647
                                    """);

                    assertQuery("select cast(cast(-2147483647 as int) as DECIMAL(10))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -2147483647
                                    """);

                    assertQuery("select cast(cast(null as int) as DECIMAL(10))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal8() throws Exception {
        testConstantCastWithNull();
        testConstantCast();
    }

    @Test
    public void testCastWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Cast 123 to DECIMAL(5,2) should result in 123.00
                    assertQuery("select cast(cast(123 as int) as DECIMAL(5,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123.00
                                    """);

                    // Cast 99 to DECIMAL(4,2) should result in 99.00
                    assertQuery("select cast(cast(99 as int) as DECIMAL(4,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99.00
                                    """);

                    // Cast -99 to DECIMAL(4,2) should result in -99.00
                    assertQuery("select cast(cast(-99 as int) as DECIMAL(4,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -99.00
                                    """);

                    // Cast 0 to DECIMAL(5,3) should result in 0.000
                    assertQuery("select cast(cast(0 as int) as DECIMAL(5,3))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.000
                                    """);
                }
        );
    }

    @Test
    public void testCastZeroWithDifferentScales() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(0 as int) as DECIMAL(5,0))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(cast(0 as int) as DECIMAL(5,1))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    assertQuery("select cast(cast(0 as int) as DECIMAL(5,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.00
                                    """);

                    assertQuery("select cast(cast(0 as int) as DECIMAL(5,3))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.000
                                    """);
                }
        );
    }

    @Test
    public void testImplicitCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Constant folded
                    assertQuery("select 1234567::int + 7654321m")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    column
                                    8888888
                                    """);

                    // Runtime discovered
                    assertQuery("with data as (select 1234567::int x) select x + 7654321m from data")
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
                    // Runtime overflow for DECIMAL(4,2) - max value is 99.99
                    assertQuery("WITH data AS (SELECT cast(100 as int) AS value) SELECT cast(value as DECIMAL(4,2)) FROM data")
                            .fails(60, "inconvertible value: 100 [INT -> DECIMAL(4,2)]");

                    // Runtime overflow for DECIMAL(5,3) - max value is 99.999
                    assertQuery("WITH data AS (SELECT cast(100 as int) AS value) SELECT cast(value as DECIMAL(5,3)) FROM data")
                            .fails(60, "inconvertible value: 100 [INT -> DECIMAL(5,3)]");
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowUnscaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL(2)
                    assertQuery("WITH data AS (SELECT cast(128 as int) AS value) SELECT cast(value as DECIMAL(2)) FROM data")
                            .fails(60, "inconvertible value: 128 [INT -> DECIMAL(2,0)]");

                    // Runtime overflow for DECIMAL(4)
                    assertQuery("WITH data AS (SELECT cast(10000 as int) AS value) SELECT cast(value as DECIMAL(4)) FROM data")
                            .fails(62, "inconvertible value: 10000 [INT -> DECIMAL(4,0)]");
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(21474836 as int) value UNION ALL SELECT cast(-21474836 as int) UNION ALL SELECT cast(12345678 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(19,2)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                21474836\t21474836.00
                                -21474836\t-21474836.00
                                12345678\t12345678.00
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(99 as int) value UNION ALL SELECT cast(-99 as int) UNION ALL SELECT cast(12 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(4,2)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                99\t99.00
                                -99\t-99.00
                                12\t12.00
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(21474836 as int) value UNION ALL SELECT cast(-21474836 as int) UNION ALL SELECT cast(12345678 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(40,10)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                21474836\t21474836.0000000000
                                -21474836\t-21474836.0000000000
                                12345678\t12345678.0000000000
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(999999 as int) value UNION ALL SELECT cast(-999999 as int) UNION ALL SELECT cast(123456 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(9,3)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                999999\t999999.000
                                -999999\t-999999.000
                                123456\t123456.000
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(2147483 as int) value UNION ALL SELECT cast(-2147483 as int) UNION ALL SELECT cast(1234567 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(10,3)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                2147483\t2147483.000
                                -2147483\t-2147483.000
                                1234567\t1234567.000
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(9 as int) value UNION ALL SELECT cast(-9 as int) UNION ALL SELECT cast(0 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(2,1)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                9\t9.0
                                -9\t-9.0
                                0\t0.0
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(2147483647 as int) value UNION ALL SELECT cast(-2147483647 as int) UNION ALL SELECT cast(1234567890 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(19)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                2147483647\t2147483647
                                -2147483647\t-2147483647
                                1234567890\t1234567890
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(9999 as int) value UNION ALL SELECT cast(-9999 as int) UNION ALL SELECT cast(1234 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(4)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                9999\t9999
                                -9999\t-9999
                                1234\t1234
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(2147483647 as int) value UNION ALL SELECT cast(-2147483647 as int) UNION ALL SELECT cast(1234567890 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(40)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                2147483647\t2147483647
                                -2147483647\t-2147483647
                                1234567890\t1234567890
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(999999999 as int) value UNION ALL SELECT cast(-999999999 as int) UNION ALL SELECT cast(123456789 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(9)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                999999999\t999999999
                                -999999999\t-999999999
                                123456789\t123456789
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(2147483647 as int) value UNION ALL SELECT cast(-2147483647 as int) UNION ALL SELECT cast(1234567890 as int) UNION ALL SELECT null) " +
                        "SELECT value, cast(value as DECIMAL(10)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                2147483647\t2147483647
                                -2147483647\t-2147483647
                                1234567890\t1234567890
                                null\t
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Use WITH clause to create runtime values (non-constant)
                    assertQuery("WITH data AS (SELECT cast(99 as int) value UNION ALL SELECT cast(-99 as int) UNION ALL SELECT cast(0 as int) UNION ALL SELECT null) " +
                            "SELECT value, cast(value as DECIMAL(2)) as decimal_value FROM data")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    value\tdecimal_value
                                    99\t99
                                    -99\t-99
                                    0\t0
                                    null\t
                                    """);
                }
        );
    }

    private void testConstantCast() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("select cast(" + -99 + " as " + "DECIMAL(2)" + ")")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                cast
                                -99
                                """)
        );
    }

    private void testConstantCastWithNull() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(" + 99 + " as " + "DECIMAL(2)" + ")")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99
                                    """);
                    assertQuery("select cast(cast(null as int) as " + "DECIMAL(2)" + ")")
                            .noLeakCheck()
                            .expectSize()
                            .returns("cast\n\n");
                }
        );
    }
}