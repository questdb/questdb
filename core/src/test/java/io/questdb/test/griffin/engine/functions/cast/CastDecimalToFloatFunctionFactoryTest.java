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

public class CastDecimalToFloatFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Basic decimal to float conversions
                    assertSql(
                            """
                                    cast
                                    123.45
                                    """,
                            "select cast(123.45m as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    -123.45
                                    """,
                            "select cast(-123.45m as float)"
                    );

                    // Zero with decimal places
                    assertSql(
                            """
                                    cast
                                    0.0
                                    """,
                            "select cast(0.00m as float)"
                    );

                    // Different decimal types
                    assertSql(
                            """
                                    cast
                                    99.0
                                    """,
                            "select cast(99m as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    12345.67
                                    """,
                            "select cast(12345.67m as float)"
                    );
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
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [123.45]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT 123.45m AS value) SELECT cast(value as float) FROM data");

                    // Runtime value without scale
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [123]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT 123m AS value) SELECT cast(value as float) FROM data");

                    // Expression should be constant folded
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [123.44999694824219f]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(123.45m as float)");
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
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [99]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(99m as DECIMAL(2)) AS value) SELECT cast(value as float) FROM data");

                    // DECIMAL16 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [9999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(9999m as DECIMAL(4)) AS value) SELECT cast(value as float) FROM data");

                    // DECIMAL32 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [999999999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(999999999m as DECIMAL(9)) AS value) SELECT cast(value as float) FROM data");

                    // DECIMAL64 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [999999999999999999]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(999999999999999999m as DECIMAL(18)) AS value) SELECT cast(value as float) FROM data");

                    // DECIMAL128 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [12345678901234567890]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(12345678901234567890m as DECIMAL(20)) AS value) SELECT cast(value as float) FROM data");

                    // DECIMAL256 unscaled
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [12345678901234567890]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(12345678901234567890m as DECIMAL(40)) AS value) SELECT cast(value as float) FROM data");

                    // With scale - tests ScaledDecimalFunction
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [99.50]
                                            long_sequence count: 1
                                    """,
                            "EXPLAIN WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) AS value) SELECT cast(value as float) FROM data");

                    // Constant folding for all decimal types
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [99.0f]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(99m as float)");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [9999.0f]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(9999m as float)");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [1.0E9f]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(999999999m as float)");

                    // Constant folding with scale (no truncation for float)
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [123.44999694824219f]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(123.45m as float)");

                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [99.98999786376953f]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(99.99m as float)");
                }
        );
    }

    @Test
    public void testCastFromDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            """
                                    cast
                                    1.2345679E19
                                    """,
                            "select cast(cast(12345678901234567890m as DECIMAL(20)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    -1.2345679E19
                                    """,
                            "select cast(cast(-12345678901234567890m as DECIMAL(20)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    null
                                    """,
                            "select cast(cast(null as DECIMAL(20)) as float)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            """
                                    cast
                                    9999.0
                                    """,
                            "select cast(cast(9999m as DECIMAL(4)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    -9999.0
                                    """,
                            "select cast(cast(-9999m as DECIMAL(4)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    null
                                    """,
                            "select cast(cast(null as DECIMAL(4)) as float)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            """
                                    cast
                                    1.2345679E19
                                    """,
                            "select cast(cast(12345678901234567890m as DECIMAL(40)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    -1.2345679E19
                                    """,
                            "select cast(cast(-12345678901234567890m as DECIMAL(40)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    null
                                    """,
                            "select cast(cast(null as DECIMAL(40)) as float)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            """
                                    cast
                                    1.0E9
                                    """,
                            "select cast(cast(999999999m as DECIMAL(9)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    -1.0E9
                                    """,
                            "select cast(cast(-999999999m as DECIMAL(9)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    null
                                    """,
                            "select cast(cast(null as DECIMAL(9)) as float)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            """
                                    cast
                                    1.0E18
                                    """,
                            "select cast(cast(999999999999999999m as DECIMAL(18)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    -1.0E18
                                    """,
                            "select cast(cast(-999999999999999999m as DECIMAL(18)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    null
                                    """,
                            "select cast(cast(null as DECIMAL(18)) as float)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            """
                                    cast
                                    99.0
                                    """,
                            "select cast(cast(99m as DECIMAL(2)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    -99.0
                                    """,
                            "select cast(cast(-99m as DECIMAL(2)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    0.0
                                    """,
                            "select cast(cast(0m as DECIMAL(2)) as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    null
                                    """,
                            "select cast(cast(null as DECIMAL(2)) as float)"
                    );
                }
        );
    }

    @Test
    public void testCastLargeDecimalValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Very large values
                    assertSql(
                            """
                                    cast
                                    1.234568E15
                                    """,
                            "select cast(1234567890123456m as float)"
                    );

                    // Very small values
                    assertSql(
                            """
                                    cast
                                    1.2345E-10
                                    """,
                            "select cast(0.00000000012345m as float)"
                    );
                }
        );
    }

    @Test
    public void testCastNegativeValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            """
                                    cast
                                    -1.0
                                    """,
                            "select cast(-1m as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    -123.0
                                    """,
                            "select cast(-123m as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    -1.0E9
                                    """,
                            "select cast(-999999999m as float)"
                    );
                }
        );
    }

    @Test
    public void testCastPrecisionLoss() throws Exception {
        assertMemoryLeak(
                () -> {
                    // High precision decimal that may lose precision when cast to float
                    assertSql(
                            """
                                    cast
                                    1.2345679E8
                                    """,
                            "select cast(123456789.123456789m as float)"
                    );
                }
        );
    }

    @Test
    public void testCastRuntimeScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> assertSql(
                        """
                                value\tfloat_value
                                99.50\t99.5
                                -99.50\t-99.5
                                12.99\t12.99
                                \tnull
                                """,
                        "WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) value " +
                                "UNION ALL SELECT cast(-99.5m as DECIMAL(4,2)) " +
                                "UNION ALL SELECT cast(12.99m as DECIMAL(4,2)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(4,2))) " +
                                "SELECT value, cast(value as float) as float_value FROM data"
                )
        );
    }

    @Test
    public void testCastRuntimeScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertSql(
                        """
                                value\tfloat_value
                                999999.999\t1000000.0
                                -999999.999\t-1000000.0
                                123456.789\t123456.79
                                \tnull
                                """,
                        "WITH data AS (SELECT cast(999999.999m as DECIMAL(9,3)) value " +
                                "UNION ALL SELECT cast(-999999.999m as DECIMAL(9,3)) " +
                                "UNION ALL SELECT cast(123456.789m as DECIMAL(9,3)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(9, 3))) " +
                                "SELECT value, cast(value as float) as float_value FROM data"
                )
        );
    }

    @Test
    public void testCastRuntimeScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertSql(
                        """
                                value\tfloat_value
                                999999999999.999999\t1.0E12
                                -999999999999.999999\t-1.0E12
                                123456789012.345678\t1.2345679E11
                                \tnull
                                """,
                        "WITH data AS (SELECT cast(999999999999.999999m as DECIMAL(18,6)) value " +
                                "UNION ALL SELECT cast(-999999999999.999999m as DECIMAL(18,6)) " +
                                "UNION ALL SELECT cast(123456789012.345678m as DECIMAL(18,6)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(18, 6))) " +
                                "SELECT value, cast(value as float) as float_value FROM data"
                )
        );
    }

    @Test
    public void testCastRuntimeScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> assertSql(
                        """
                                value\tfloat_value
                                9.9\t9.9
                                -9.9\t-9.9
                                0.5\t0.5
                                \tnull
                                """,
                        "WITH data AS (SELECT cast(9.9m as DECIMAL(2,1)) value " +
                                "UNION ALL SELECT cast(-9.9m as DECIMAL(2,1)) " +
                                "UNION ALL SELECT cast(0.5m as DECIMAL(2,1)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(2,1))) " +
                                "SELECT value, cast(value as float) as float_value FROM data"
                )
        );
    }

    @Test
    public void testCastRuntimeUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> assertSql(
                        """
                                value\tfloat_value
                                99\t99.0
                                -99\t-99.0
                                0\t0.0
                                \tnull
                                """,
                        "WITH data AS (SELECT cast(99m as DECIMAL(2)) value " +
                                "UNION ALL SELECT cast(-99m as DECIMAL(2)) " +
                                "UNION ALL SELECT cast(0m as DECIMAL(2)) " +
                                "UNION ALL SELECT cast(null as DECIMAL(2))) " +
                                "SELECT value, cast(value as float) as float_value FROM data"
                )
        );
    }

    @Test
    public void testCastZeroValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            """
                                    cast
                                    0.0
                                    """,
                            "select cast(0m as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    0.0
                                    """,
                            "select cast(0.0m as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    0.0
                                    """,
                            "select cast(0.00m as float)"
                    );

                    assertSql(
                            """
                                    cast
                                    0.0
                                    """,
                            "select cast(0.000m as float)"
                    );
                }
        );
    }
}