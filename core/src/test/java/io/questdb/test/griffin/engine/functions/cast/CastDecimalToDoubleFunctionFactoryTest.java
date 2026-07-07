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

public class CastDecimalToDoubleFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Basic decimal to double conversions
                    assertQuery("select cast(123.45m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123.45
                                    """);

                    assertQuery("select cast(-123.45m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123.45
                                    """);

                    // Zero with decimal places
                    assertQuery("select cast(0.00m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    // Different decimal types
                    assertQuery("select cast(99m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99.0
                                    """);

                    assertQuery("select cast(12345.67m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    12345.67
                                    """);
                }
        );
    }

    @Test
    public void testCastExplains() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime value with scale
                    assertQuery("WITH data AS (SELECT 123.45m AS value) SELECT cast(value as double) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::double]
                                        VirtualRecord
                                          functions: [123.45]
                                            long_sequence count: 1
                                    """);

                    // Runtime value without scale
                    assertQuery("WITH data AS (SELECT 123m AS value) SELECT cast(value as double) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::double]
                                        VirtualRecord
                                          functions: [123]
                                            long_sequence count: 1
                                    """);

                    // Expression should be constant folded
                    assertQuery("SELECT cast(123.45m as double)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [123.45]
                                        long_sequence count: 1
                                    """);
                }
        );
    }

    @Test
    public void testCastExplainsForDifferentDecimalTypes() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 unscaled
                    assertQuery("WITH data AS (SELECT cast(99m as DECIMAL(2)) AS value) SELECT cast(value as double) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::double]
                                        VirtualRecord
                                          functions: [99]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL16 unscaled
                    assertQuery("WITH data AS (SELECT cast(9999m as DECIMAL(4)) AS value) SELECT cast(value as double) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::double]
                                        VirtualRecord
                                          functions: [9999]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL32 unscaled
                    assertQuery("WITH data AS (SELECT cast(999999999m as DECIMAL(9)) AS value) SELECT cast(value as double) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::double]
                                        VirtualRecord
                                          functions: [999999999]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL64 unscaled
                    assertQuery("WITH data AS (SELECT cast(999999999999999999m as DECIMAL(18)) AS value) SELECT cast(value as double) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::double]
                                        VirtualRecord
                                          functions: [999999999999999999]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL128 unscaled
                    assertQuery("WITH data AS (SELECT cast(12345678901234567890m as DECIMAL(20)) AS value) SELECT cast(value as double) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::double]
                                        VirtualRecord
                                          functions: [12345678901234567890]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL256 unscaled
                    assertQuery("WITH data AS (SELECT cast(12345678901234567890m as DECIMAL(40)) AS value) SELECT cast(value as double) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::double]
                                        VirtualRecord
                                          functions: [12345678901234567890]
                                            long_sequence count: 1
                                    """);

                    // With scale - tests ScaledDecimalFunction
                    assertQuery("WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) AS value) SELECT cast(value as double) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::double]
                                        VirtualRecord
                                          functions: [99.50]
                                            long_sequence count: 1
                                    """);

                    // Constant folding for all decimal types
                    assertQuery("SELECT cast(99m as double)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [99.0]
                                        long_sequence count: 1
                                    """);

                    assertQuery("SELECT cast(9999m as double)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [9999.0]
                                        long_sequence count: 1
                                    """);

                    assertQuery("SELECT cast(999999999m as double)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [9.99999999E8]
                                        long_sequence count: 1
                                    """);

                    // Constant folding with scale (no truncation for double)
                    assertQuery("SELECT cast(123.45m as double)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [123.45]
                                        long_sequence count: 1
                                    """);

                    assertQuery("SELECT cast(99.99m as double)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [99.99]
                                        long_sequence count: 1
                                    """);
                }
        );
    }

    @Test
    public void testCastFromDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(12345678901234567890m as DECIMAL(20)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.2345678901234567E19
                                    """);

                    assertQuery("select cast(cast(-12345678901234567890m as DECIMAL(20)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.2345678901234567E19
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(20)) as double)")
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
                    assertQuery("select cast(cast(9999m as DECIMAL(4)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9999.0
                                    """);

                    assertQuery("select cast(cast(-9999m as DECIMAL(4)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9999.0
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(4)) as double)")
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
                    assertQuery("select cast(cast(12345678901234567890m as DECIMAL(40)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.2345678901234567E19
                                    """);

                    assertQuery("select cast(cast(-12345678901234567890m as DECIMAL(40)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.2345678901234567E19
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(40)) as double)")
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
                    assertQuery("select cast(cast(999999999m as DECIMAL(9)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9.99999999E8
                                    """);

                    assertQuery("select cast(cast(-999999999m as DECIMAL(9)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9.99999999E8
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(9)) as double)")
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
                    assertQuery("select cast(cast(999999999999999999m as DECIMAL(18)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.0E18
                                    """);

                    assertQuery("select cast(cast(-999999999999999999m as DECIMAL(18)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.0E18
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(18)) as double)")
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
                    assertQuery("select cast(cast(99m as DECIMAL(2)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99.0
                                    """);

                    assertQuery("select cast(cast(-99m as DECIMAL(2)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -99.0
                                    """);

                    assertQuery("select cast(cast(0m as DECIMAL(2)) as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(2)) as double)")
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
    public void testCastLargeDecimalValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Very large values
                    assertQuery("select cast(1234567890123456m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.234567890123456E15
                                    """);

                    // Very small values
                    assertQuery("select cast(0.00000000012345m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.2345E-10
                                    """);
                }
        );
    }

    @Test
    public void testCastNegativeValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(-1m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.0
                                    """);

                    assertQuery("select cast(-123m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123.0
                                    """);

                    assertQuery("select cast(-999999999m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9.99999999E8
                                    """);
                }
        );
    }

    @Test
    public void testCastPrecisionLoss() throws Exception {
        assertMemoryLeak(
                () -> {
                    // High precision decimal that may lose precision when cast to double
                    assertQuery("select cast(123456789.123456789m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.2345678912345679E8
                                    """);
                }
        );
    }

    @Test
    public void testCastRuntimeScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) value " +
                        "UNION ALL SELECT cast(-99.5m as DECIMAL(4,2)) " +
                        "UNION ALL SELECT cast(12.99m as DECIMAL(4,2)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(4,2))) " +
                        "SELECT value, cast(value as double) as double_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdouble_value
                                99.50\t99.5
                                -99.50\t-99.5
                                12.99\t12.99
                                \tnull
                                """)
        );
    }

    @Test
    public void testCastRuntimeScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(999999.999m as DECIMAL(9,3)) value " +
                        "UNION ALL SELECT cast(-999999.999m as DECIMAL(9,3)) " +
                        "UNION ALL SELECT cast(123456.789m as DECIMAL(9,3)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(9, 3))) " +
                        "SELECT value, cast(value as double) as double_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdouble_value
                                999999.999\t999999.999
                                -999999.999\t-999999.999
                                123456.789\t123456.789
                                \tnull
                                """)
        );
    }

    @Test
    public void testCastRuntimeScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(999999999999.999999m as DECIMAL(18,6)) value " +
                        "UNION ALL SELECT cast(-999999999999.999999m as DECIMAL(18,6)) " +
                        "UNION ALL SELECT cast(123456789012.345678m as DECIMAL(18,6)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(18, 6))) " +
                        "SELECT value, cast(value as double) as double_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdouble_value
                                999999999999.999999\t1.0E12
                                -999999999999.999999\t-1.0E12
                                123456789012.345678\t1.2345678901234567E11
                                \tnull
                                """)
        );
    }

    @Test
    public void testCastRuntimeScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(9.9m as DECIMAL(2,1)) value " +
                        "UNION ALL SELECT cast(-9.9m as DECIMAL(2,1)) " +
                        "UNION ALL SELECT cast(0.5m as DECIMAL(2,1)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(2,1))) " +
                        "SELECT value, cast(value as double) as double_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdouble_value
                                9.9\t9.9
                                -9.9\t-9.9
                                0.5\t0.5
                                \tnull
                                """)
        );
    }

    @Test
    public void testCastRuntimeUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT cast(99m as DECIMAL(2)) value " +
                        "UNION ALL SELECT cast(-99m as DECIMAL(2)) " +
                        "UNION ALL SELECT cast(0m as DECIMAL(2)) " +
                        "UNION ALL SELECT cast(null as DECIMAL(2))) " +
                        "SELECT value, cast(value as double) as double_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdouble_value
                                99\t99.0
                                -99\t-99.0
                                0\t0.0
                                \tnull
                                """)
        );
    }

    @Test
    public void testCastZeroValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(0m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    assertQuery("select cast(0.0m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    assertQuery("select cast(0.00m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    assertQuery("select cast(0.000m as double)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);
                }
        );
    }
}