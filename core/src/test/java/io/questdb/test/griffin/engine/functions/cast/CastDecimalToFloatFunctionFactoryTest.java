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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class CastDecimalToFloatFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Basic decimal to float conversions
                    assertQuery("select cast(123.45m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123.45
                                    """);

                    assertQuery("select cast(-123.45m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123.45
                                    """);

                    // Zero with decimal places
                    assertQuery("select cast(0.00m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    // Different decimal types
                    assertQuery("select cast(99m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99.0
                                    """);

                    assertQuery("select cast(12345.67m as float)")
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
                    assertQuery("WITH data AS (SELECT 123.45m AS value) SELECT cast(value as float) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [123.45]
                                            long_sequence count: 1
                                    """);

                    // Runtime value without scale
                    assertQuery("WITH data AS (SELECT 123m AS value) SELECT cast(value as float) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [123]
                                            long_sequence count: 1
                                    """);

                    // Expression should be constant folded
                    assertQuery("SELECT cast(123.45m as float)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [123.44999694824219f]
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
                    assertQuery("WITH data AS (SELECT cast(99m as DECIMAL(2)) AS value) SELECT cast(value as float) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [99]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL16 unscaled
                    assertQuery("WITH data AS (SELECT cast(9999m as DECIMAL(4)) AS value) SELECT cast(value as float) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [9999]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL32 unscaled
                    assertQuery("WITH data AS (SELECT cast(999999999m as DECIMAL(9)) AS value) SELECT cast(value as float) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [999999999]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL64 unscaled
                    assertQuery("WITH data AS (SELECT cast(999999999999999999m as DECIMAL(18)) AS value) SELECT cast(value as float) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [999999999999999999]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL128 unscaled
                    assertQuery("WITH data AS (SELECT cast(12345678901234567890m as DECIMAL(20)) AS value) SELECT cast(value as float) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [12345678901234567890]
                                            long_sequence count: 1
                                    """);

                    // DECIMAL256 unscaled
                    assertQuery("WITH data AS (SELECT cast(12345678901234567890m as DECIMAL(40)) AS value) SELECT cast(value as float) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [12345678901234567890]
                                            long_sequence count: 1
                                    """);

                    // With scale - tests ScaledDecimalFunction
                    assertQuery("WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) AS value) SELECT cast(value as float) FROM data")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [value::float]
                                        VirtualRecord
                                          functions: [99.50]
                                            long_sequence count: 1
                                    """);

                    // Constant folding for all decimal types
                    assertQuery("SELECT cast(99m as float)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [99.0f]
                                        long_sequence count: 1
                                    """);

                    assertQuery("SELECT cast(9999m as float)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [9999.0f]
                                        long_sequence count: 1
                                    """);

                    assertQuery("SELECT cast(999999999m as float)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [1.0E9f]
                                        long_sequence count: 1
                                    """);

                    // Constant folding with scale (no truncation for float)
                    assertQuery("SELECT cast(123.45m as float)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [123.44999694824219f]
                                        long_sequence count: 1
                                    """);

                    assertQuery("SELECT cast(99.99m as float)")
                            .noLeakCheck()
                            .assertsPlan("""
                                    VirtualRecord
                                      functions: [99.98999786376953f]
                                        long_sequence count: 1
                                    """);
                }
        );
    }

    @Test
    public void testCastFloatRangeBoundary() throws Exception {
        assertQuery("select cast(340282346638528859811704183484516925440m as float) atMax, " +
                "cast(-340282346638528859811704183484516925440m as float) atMaxNeg, " +
                "cast(340282346638528900000000000000000000000m as float) justAboveMax, " +
                "cast(340282356779733600000000000000000000000m as float) aboveRoundingThreshold, " +
                "cast(cast(340282346638528859811704183484516925440m as double) as float) atMaxViaDouble, " +
                "cast(cast(340282346638528900000000000000000000000m as double) as float) justAboveMaxViaDouble")
                .expectSize()
                .returns("""
                        atMax\tatMaxNeg\tjustAboveMax\taboveRoundingThreshold\tatMaxViaDouble\tjustAboveMaxViaDouble
                        3.4028235E38\t-3.4028235E38\tnull\tnull\t3.4028235E38\tnull
                        """);
    }

    @Test
    public void testCastFromDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(cast(12345678901234567890m as DECIMAL(20)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.2345679E19
                                    """);

                    assertQuery("select cast(cast(-12345678901234567890m as DECIMAL(20)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.2345679E19
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(20)) as float)")
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
                    assertQuery("select cast(cast(9999m as DECIMAL(4)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9999.0
                                    """);

                    assertQuery("select cast(cast(-9999m as DECIMAL(4)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9999.0
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(4)) as float)")
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
                    assertQuery("select cast(cast(12345678901234567890m as DECIMAL(40)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.2345679E19
                                    """);

                    assertQuery("select cast(cast(-12345678901234567890m as DECIMAL(40)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.2345679E19
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(40)) as float)")
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
                    assertQuery("select cast(cast(999999999m as DECIMAL(9)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.0E9
                                    """);

                    assertQuery("select cast(cast(-999999999m as DECIMAL(9)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.0E9
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(9)) as float)")
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
                    assertQuery("select cast(cast(999999999999999999m as DECIMAL(18)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.0E18
                                    """);

                    assertQuery("select cast(cast(-999999999999999999m as DECIMAL(18)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.0E18
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(18)) as float)")
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
                    assertQuery("select cast(cast(99m as DECIMAL(2)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99.0
                                    """);

                    assertQuery("select cast(cast(-99m as DECIMAL(2)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -99.0
                                    """);

                    assertQuery("select cast(cast(0m as DECIMAL(2)) as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    assertQuery("select cast(cast(null as DECIMAL(2)) as float)")
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
                    assertQuery("select cast(1234567890123456m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.234568E15
                                    """);

                    // Very small values
                    assertQuery("select cast(0.00000000012345m as float)")
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
                    assertQuery("select cast(-1m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.0
                                    """);

                    assertQuery("select cast(-123m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123.0
                                    """);

                    assertQuery("select cast(-999999999m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -1.0E9
                                    """);
                }
        );
    }

    @Test
    public void testCastPrecisionLoss() throws Exception {
        assertMemoryLeak(
                () -> {
                    // High precision decimal that may lose precision when cast to float
                    assertQuery("select cast(123456789.123456789m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    1.2345679E8
                                    """);
                }
        );
    }

    @Test
    public void testCastRoundsOnceBelowFloatMidpoint() throws Exception {
        assertMemoryLeak(
                () -> {
                    // every value sits just below the midpoint of 1.0000001 and 1.0000002, so it rounds down;
                    // rounding through a double lands on the midpoint and rounds up instead
                    assertQuery("select cast(cast(1.00000017881393432m as DECIMAL(18,17)) as float) d64, " +
                            "cast(cast(-1.00000017881393432m as DECIMAL(18,17)) as float) d64Neg, " +
                            "cast(cast(1.0000001788139343261718749999999999999m as DECIMAL(38,37)) as float) d128, " +
                            "cast(cast(-1.0000001788139343261718749999999999999m as DECIMAL(38,37)) as float) d128Neg, " +
                            "cast(cast(1.00000017881393432617187499999999999999m as DECIMAL(39,38)) as float) d256, " +
                            "cast(cast(-1.00000017881393432617187499999999999999m as DECIMAL(39,38)) as float) d256Neg")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    d64\td64Neg\td128\td128Neg\td256\td256Neg
                                    1.0000001\t-1.0000001\t1.0000001\t-1.0000001\t1.0000001\t-1.0000001
                                    """);

                    assertQuery("select cast(a as float) d64, cast(b as float) d128, cast(c as float) d256 from midpoints")
                            .ddl(
                                    "create table midpoints (a DECIMAL(18,17), b DECIMAL(38,37), c DECIMAL(39,38))",
                                    """
                                            insert into midpoints values
                                            (1.00000017881393432m, 1.0000001788139343261718749999999999999m, 1.00000017881393432617187499999999999999m),
                                            (-1.00000017881393432m, -1.0000001788139343261718749999999999999m, -1.00000017881393432617187499999999999999m),
                                            (null, null, null)"""
                            )
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    d64\td128\td256
                                    1.0000001\t1.0000001\t1.0000001
                                    -1.0000001\t-1.0000001\t-1.0000001
                                    null\tnull\tnull
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
                        "SELECT value, cast(value as float) as float_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tfloat_value
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
                        "SELECT value, cast(value as float) as float_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tfloat_value
                                999999.999\t1000000.0
                                -999999.999\t-1000000.0
                                123456.789\t123456.79
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
                        "SELECT value, cast(value as float) as float_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tfloat_value
                                999999999999.999999\t1.0E12
                                -999999999999.999999\t-1.0E12
                                123456789012.345678\t1.2345679E11
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
                        "SELECT value, cast(value as float) as float_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tfloat_value
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
                        "SELECT value, cast(value as float) as float_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tfloat_value
                                99\t99.0
                                -99\t-99.0
                                0\t0.0
                                \tnull
                                """)
        );
    }

    @Test
    public void testCastOutOfFloatRangeReturnsNull() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(340282400000000000000000000000000000000m as float) above, " +
                            "cast(-340282400000000000000000000000000000000m as float) below, " +
                            "cast(340282000000000000000000000000000000000m as float) inRange, " +
                            "cast(-340282000000000000000000000000000000000m as float) inRangeNeg, " +
                            "cast(9999999999999999999999999999999999999999999999999999999999999999999999999999m as float) maxDecimal, " +
                            "cast(cast(null as DECIMAL(39,0)) as float) nullDecimal")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    above\tbelow\tinRange\tinRangeNeg\tmaxDecimal\tnullDecimal
                                    null\tnull\t3.40282E38\t-3.40282E38\tnull\tnull
                                    """);
                }
        );
    }

    @Test
    public void testCastOutOfFloatRangeReturnsNullFromColumn() throws Exception {
        assertQuery("select v, cast(v as float) float_value from x")
                .ddl(
                        "create table x (v DECIMAL(76,2))",
                        "insert into x values (340282400000000000000000000000000000000.00m), " +
                                "(-340282400000000000000000000000000000000.00m), " +
                                "(340282000000000000000000000000000000000.00m), " +
                                "(340282346638528859811704183484516925440.00m), " +
                                "(-340282346638528859811704183484516925440.00m), " +
                                "(340282346638528900000000000000000000000.00m), " +
                                "(123.45m), (-0.25m), (null)"
                )
                .expectSize()
                .returns("""
                        v\tfloat_value
                        340282400000000000000000000000000000000.00\tnull
                        -340282400000000000000000000000000000000.00\tnull
                        340282000000000000000000000000000000000.00\t3.40282E38
                        340282346638528859811704183484516925440.00\t3.4028235E38
                        -340282346638528859811704183484516925440.00\t-3.4028235E38
                        340282346638528900000000000000000000000.00\tnull
                        123.45\t123.45
                        -0.25\t-0.25
                        \tnull
                        """);
    }

    @Test
    public void testCastSmallestMagnitudes() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(0.00000000000000000000000000000000000001m as float) smallest, " +
                            "cast(-0.00000000000000000000000000000000000001m as float) smallestNeg")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    smallest\tsmallestNeg
                                    1.0E-38\t-1.0E-38
                                    """);

                    assertQuery("WITH data AS (SELECT 0.00000000000000000000000000000000000001m value " +
                            "UNION ALL SELECT -0.00000000000000000000000000000000000001m " +
                            "UNION ALL SELECT cast(null as DECIMAL(39,38))) " +
                            "SELECT cast(value as float) float_value FROM data")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    float_value
                                    1.0E-38
                                    -1.0E-38
                                    null
                                    """);

                    // the subnormal pair is above half of Float.MIN_VALUE and keeps the smallest subnormal,
                    // the underflowing pair is below it and collapses to a signed zero
                    assertQuery("select cast(0.000000000000000000000000000000000000000000000700649232162408536m as float) subnormal, " +
                            "cast(-0.000000000000000000000000000000000000000000000700649232162408536m as float) subnormalNeg, " +
                            "cast(0.0000000000000000000000000000000000000000000000000000000000000000000000000001m as float) underflow, " +
                            "cast(-0.0000000000000000000000000000000000000000000000000000000000000000000000000001m as float) underflowNeg")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    subnormal\tsubnormalNeg\tunderflow\tunderflowNeg
                                    1.4E-45\t-1.4E-45\t0.0\t-0.0
                                    """);
                }
        );
    }

    @Test
    public void testCastMatchesBigDecimalBitForBit() throws Exception {
        assertMemoryLeak(() -> {
            final BigDecimal maxFloat = new BigDecimal(Float.MAX_VALUE);
            final int[] rangeCounts = new int[2];
            for (int[] decimalType : CastDecimalToDoubleFunctionFactoryTest.DECIMAL_TYPES) {
                final int precision = decimalType[0];
                final int scale = decimalType[1];
                final List<BigInteger> values = CastDecimalToDoubleFunctionFactoryTest.unscaledValues(precision);
                final String tableName = "f" + precision + "_" + scale;

                execute("create table " + tableName + " (v DECIMAL(" + precision + "," + scale + "))");
                final StringBuilder insert = new StringBuilder("insert into ").append(tableName).append(" values ");
                for (int i = 0, n = values.size(); i < n; i++) {
                    if (i > 0) {
                        insert.append(',');
                    }
                    insert.append('(').append(new BigDecimal(values.get(i)).movePointLeft(scale).toPlainString()).append("m)");
                }
                execute(insert);

                try (
                        RecordCursorFactory factory = select("select cast(v as float) from " + tableName);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final Record record = cursor.getRecord();
                    for (int i = 0, n = values.size(); i < n; i++) {
                        final BigInteger unscaled = values.get(i);
                        Assert.assertTrue(cursor.hasNext());
                        final BigDecimal value = new BigDecimal(unscaled).movePointLeft(scale);
                        final boolean isOutOfFloatRange = value.abs().compareTo(maxFloat) > 0;
                        final float expected = isOutOfFloatRange ? Float.NaN : value.floatValue();
                        Assert.assertEquals(
                                "DECIMAL(" + precision + "," + scale + ") unscaled=" + unscaled,
                                Float.floatToRawIntBits(expected),
                                Float.floatToRawIntBits(record.getFloat(0))
                        );
                        rangeCounts[isOutOfFloatRange ? 1 : 0]++;
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            Assert.assertTrue(rangeCounts[0] > 0);
            Assert.assertTrue(rangeCounts[1] > 0);
        });
    }

    @Test
    public void testCastZeroValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(0m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    assertQuery("select cast(0.0m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    assertQuery("select cast(0.00m as float)")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.0
                                    """);

                    assertQuery("select cast(0.000m as float)")
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