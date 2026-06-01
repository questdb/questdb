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

public class CastDecimalToShortFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Truncates decimal part
                    assertQuery("select cast(123.45m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123
                                    """);

                    assertQuery("select cast(123.99m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123
                                    """);

                    assertQuery("select cast(-123.45m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123
                                    """);

                    assertQuery("select cast(-123.99m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123
                                    """);

                    // Zero with decimal places
                    assertQuery("select cast(0.99m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(0.01m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(-0.99m as short)")
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
                                      functions: [value::short]
                                        VirtualRecord
                                          functions: [123.45]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT 123.45m AS value) SELECT cast(value as short) FROM data");

                    // Runtime value without scale
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::short]
                                        VirtualRecord
                                          functions: [123]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT 123m AS value) SELECT cast(value as short) FROM data");

                    // Expression should be constant folded
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [123]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(123.45m as short)");
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
                                      functions: [value::short]
                                        VirtualRecord
                                          functions: [99]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(99m as DECIMAL(2)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL16 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::short]
                                        VirtualRecord
                                          functions: [9999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(9999m as DECIMAL(4)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL32 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::short]
                                        VirtualRecord
                                          functions: [32767]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(32767m as DECIMAL(5)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL64 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::short]
                                        VirtualRecord
                                          functions: [32767]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(32767m as DECIMAL(10)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL128 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::short]
                                        VirtualRecord
                                          functions: [32767]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(32767m as DECIMAL(19)) AS value) SELECT cast(value as short) FROM data");

                    // DECIMAL256 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::short]
                                        VirtualRecord
                                          functions: [32767]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(32767m as DECIMAL(40)) AS value) SELECT cast(value as short) FROM data");

                    // With scale - tests ScaledDecimalFunction
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::short]
                                        VirtualRecord
                                          functions: [99.50]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) AS value) SELECT cast(value as short) FROM data");

                    // Constant folding for all decimal types
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [99]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(99m as short)");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [9999]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(9999m as short)");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [32767]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(32767m as short)");

                    // Constant folding with scale (truncation)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [123]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(123.45m as short)");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [99]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(99.99m as short)");
                }
        );
    }

    @Test
    public void testCastFromDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(32767m as DECIMAL(19)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    assertQuery("select cast(cast(-32767m as DECIMAL(19)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32767
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(19)) as short)")
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
    public void testCastFromDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(9999m as DECIMAL(4)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9999
                                    """);

                    assertQuery("select cast(cast(-9999m as DECIMAL(4)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9999
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(4)) as short)")
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
    public void testCastFromDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(32767m as DECIMAL(40)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    assertQuery("select cast(cast(-32767m as DECIMAL(40)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32767
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(40)) as short)")
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
    public void testCastFromDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(32767m as DECIMAL(9)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    assertQuery("select cast(cast(-32767m as DECIMAL(9)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32767
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(9)) as short)")
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
    public void testCastFromDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(32767m as DECIMAL(18)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    assertQuery("select cast(cast(-32767m as DECIMAL(18)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32767
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(18)) as short)")
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
    public void testCastFromDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(99m as DECIMAL(2)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99
                                    """);

                    assertQuery("select cast(cast(-99m as DECIMAL(2)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -99
                                    """);

                    assertQuery("select cast(cast(0m as DECIMAL(2)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(2)) as short)")
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
    public void testCastMaxShortValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max short value from decimal
                    assertQuery("select cast(32767m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    // Max short value minus 1
                    assertQuery("select cast(32766m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32766
                                    """);

                    // Min short value + 1 (to avoid overflow with negation)
                    assertQuery("select cast(-32767m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32767
                                    """);

                    // With scale - truncated
                    assertQuery("select cast(32767.99m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);
                }
        );
    }

    @Test
    public void testCastNegativeValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(-1m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1
                                    """);

                    assertQuery("select cast(-123m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123
                                    """);

                    assertQuery("select cast(-9999m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9999
                                    """);
                }
        );
    }

    @Test
    public void testCastOverflowDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertQuery("select cast(cast(2147483647m as DECIMAL(19)) as short)")
                            .fails(7, "inconvertible value: 2147483647 [DECIMAL(19,0) -> SHORT]");

                    assertQuery("select cast(cast(-2147483648m as DECIMAL(19)) as short)")
                            .fails(7, "inconvertible value: -2147483648 [DECIMAL(19,0) -> SHORT]");
                }
        );
    }

    @Test
    public void testCastOverflowDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertQuery("select cast(cast(32768m as DECIMAL(5)) as short)")
                            .fails(7, "inconvertible value: 32768 [DECIMAL(5,0) -> SHORT]");

                    assertQuery("select cast(cast(-32769m as DECIMAL(5)) as short)")
                            .fails(7, "inconvertible value: -32769 [DECIMAL(5,0) -> SHORT]");
                }
        );
    }

    @Test
    public void testCastOverflowDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertQuery("select cast(cast(999999999999m as DECIMAL(40)) as short)")
                            .fails(7, "inconvertible value: 999999999999 [DECIMAL(40,0) -> SHORT]");

                    assertQuery("select cast(cast(-999999999999m as DECIMAL(40)) as short)")
                            .fails(7, "inconvertible value: -999999999999 [DECIMAL(40,0) -> SHORT]");
                }
        );
    }

    @Test
    public void testCastOverflowDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertQuery("select cast(cast(100000m as DECIMAL(6)) as short)")
                            .fails(7, "inconvertible value: 100000 [DECIMAL(6,0) -> SHORT]");

                    assertQuery("select cast(cast(-100000m as DECIMAL(6)) as short)")
                            .fails(7, "inconvertible value: -100000 [DECIMAL(6,0) -> SHORT]");
                }
        );
    }

    @Test
    public void testCastOverflowDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for short
                    assertQuery("select cast(cast(999999999m as DECIMAL(18)) as short)")
                            .fails(7, "inconvertible value: 999999999 [DECIMAL(18,0) -> SHORT]");

                    assertQuery("select cast(cast(-999999999m as DECIMAL(18)) as short)")
                            .fails(7, "inconvertible value: -999999999 [DECIMAL(18,0) -> SHORT]");
                }
        );
    }

    @Test
    public void testCastOverflowDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 can hold max 99, which fits in SHORT
                    assertQuery("select cast(cast(99m as DECIMAL(2)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99
                                    """);

                    assertQuery("select cast(cast(-99m as DECIMAL(2)) as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -99
                                    """);
                }
        );
    }

    @Test
    public void testCastZeroValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(0m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(0.0m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(0.00m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);

                    assertQuery("select cast(0.000m as short)")
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
                    // No implicit cast from decimal to short in arithmetic
                    // Must use explicit cast
                    assertQuery("select cast(99m as short) + cast(100 as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    column
                                    199
                                    """);

                    // Runtime conversion
                    assertQuery("with data as (select 99m x) select cast(x as short) + cast(100 as short) from data")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    column
                                    199
                                    """);
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowScaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow with scaled decimal
                    assertQuery("WITH data AS (SELECT cast(32768.5m as DECIMAL(6,1)) AS value) SELECT cast(value as short) FROM data")
                            .fails(69, "inconvertible value: 32768.5 [DECIMAL(6,1) -> SHORT]");

                    assertQuery("WITH data AS (SELECT cast(-32769.5m as DECIMAL(6,1)) AS value) SELECT cast(value as short) FROM data")
                            .fails(70, "inconvertible value: -32769.5 [DECIMAL(6,1) -> SHORT]");
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowUnscaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL16
                    assertQuery("WITH data AS (SELECT cast(32768m as DECIMAL(5)) AS value) SELECT cast(value as short) FROM data")
                            .fails(65, "inconvertible value: 32768 [DECIMAL(5,0) -> SHORT]");

                    // Runtime overflow for DECIMAL32
                    assertQuery("WITH data AS (SELECT cast(100000m as DECIMAL(6)) AS value) SELECT cast(value as short) FROM data")
                            .fails(66, "inconvertible value: 100000 [DECIMAL(6,0) -> SHORT]");

                    // Runtime overflow for DECIMAL64
                    assertQuery("WITH data AS (SELECT cast(999999999m as DECIMAL(18)) AS value) SELECT cast(value as short) FROM data")
                            .fails(70, "inconvertible value: 999999999 [DECIMAL(18,0) -> SHORT]");

                    // Runtime overflow for DECIMAL128
                    assertQuery("WITH data AS (SELECT cast(2147483647m as DECIMAL(19)) AS value) SELECT cast(value as short) FROM data")
                            .fails(71, "inconvertible value: 2147483647 [DECIMAL(19,0) -> SHORT]");

                    // Runtime overflow for DECIMAL256
                    assertQuery("WITH data AS (SELECT cast(999999999999m as DECIMAL(40)) AS value) SELECT cast(value as short) FROM data")
                            .fails(73, "inconvertible value: 999999999999 [DECIMAL(40,0) -> SHORT]");
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(32766.99m as DECIMAL(20,2)) value " +
                        "UNION ALL SELECT cast(-32766.99m as DECIMAL(20,2)) " +
                        "UNION ALL SELECT cast(12345.67m as DECIMAL(20,2)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(20,2))) " +
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                32766.99\t32766
                                -32766.99\t-32766
                                12345.67\t12345
                                \t0
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
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                99.50\t99
                                -99.50\t-99
                                12.99\t12
                                \t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(32766.9999999999m as DECIMAL(40,10)) value " +
                        "UNION ALL SELECT cast(-32766.9999999999m as DECIMAL(40,10)) " +
                        "UNION ALL SELECT cast(12345.1234567890m as DECIMAL(40,10)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(40,10))) " +
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                32766.9999999999\t32766
                                -32766.9999999999\t-32766
                                12345.1234567890\t12345
                                \t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(32766.999m as DECIMAL(8,3)) value " +
                        "UNION ALL SELECT cast(-32766.999m as DECIMAL(8,3)) " +
                        "UNION ALL SELECT cast(12345.678m as DECIMAL(8,3)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(8, 3))) " +
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                32766.999\t32766
                                -32766.999\t-32766
                                12345.678\t12345
                                \t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(32766.999999m as DECIMAL(11,6)) value " +
                        "UNION ALL SELECT cast(-32766.999999m as DECIMAL(11,6)) " +
                        "UNION ALL SELECT cast(12345.678901m as DECIMAL(11,6)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(11, 6))) " +
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                32766.999999\t32766
                                -32766.999999\t-32766
                                12345.678901\t12345
                                \t0
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
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                9.9\t9
                                -9.9\t-9
                                0.5\t0
                                \t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(32767m as DECIMAL(19)) value " +
                        "UNION ALL SELECT cast(-32767m as DECIMAL(19)) " +
                        "UNION ALL SELECT cast(12345m as DECIMAL(19)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(19))) " +
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                32767\t32767
                                -32767\t-32767
                                12345\t12345
                                \t0
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
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                9999\t9999
                                -9999\t-9999
                                1234\t1234
                                \t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(32767m as DECIMAL(40)) value " +
                        "UNION ALL SELECT cast(-32767m as DECIMAL(40)) " +
                        "UNION ALL SELECT cast(12345m as DECIMAL(40)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(40))) " +
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                32767\t32767
                                -32767\t-32767
                                12345\t12345
                                \t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(32767m as DECIMAL(9)) value " +
                        "UNION ALL SELECT cast(-32767m as DECIMAL(9)) " +
                        "UNION ALL SELECT cast(12345m as DECIMAL(9)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(9))) " +
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                32767\t32767
                                -32767\t-32767
                                12345\t12345
                                \t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(32767m as DECIMAL(18)) value " +
                        "UNION ALL SELECT cast(-32767m as DECIMAL(18)) " +
                        "UNION ALL SELECT cast(12345m as DECIMAL(18)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(18))) " +
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                32767\t32767
                                -32767\t-32767
                                12345\t12345
                                \t0
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
                        "SELECT value, cast(value as short) as short_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tshort_value
                                99\t99
                                -99\t-99
                                0\t0
                                \t0
                                """)
        );
    }

    @Test
    public void testTruncationBehavior() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Positive values - truncate towards zero
                    assertQuery("select cast(1.1m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1
                                    """);

                    assertQuery("select cast(1.5m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1
                                    """);

                    assertQuery("select cast(1.9m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1
                                    """);

                    // Negative values - truncate towards zero
                    assertQuery("select cast(-1.1m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1
                                    """);

                    assertQuery("select cast(-1.5m as short)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1
                                    """);

                    assertQuery("select cast(-1.9m as short)")
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