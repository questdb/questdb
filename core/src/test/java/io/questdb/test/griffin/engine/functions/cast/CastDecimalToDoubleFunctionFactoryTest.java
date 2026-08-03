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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

public class CastDecimalToDoubleFunctionFactoryTest extends AbstractCairoTest {
    // one entry per storage width, plus scales straddling the exact-division limit of 22
    static final int[][] DECIMAL_TYPES = {
            {2, 0}, {2, 2},
            {4, 0}, {4, 3},
            {9, 0}, {9, 4},
            {18, 0}, {18, 6}, {18, 18},
            {38, 0}, {38, 21}, {38, 22}, {38, 23}, {38, 38},
            {76, 0}, {76, 21}, {76, 22}, {76, 23}, {76, 40}, {76, 76},
    };
    private static final long MAX_EXACT_UNSCALED = 1L << 53;

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
    public void testCastLargestDecimal256Magnitudes() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("select cast(9999999999999999999999999999999999999999999999999999999999999999999999999999m as double) maxDecimal, " +
                        "cast(-9999999999999999999999999999999999999999999999999999999999999999999999999999m as double) minDecimal, " +
                        "cast(340282400000000000000000000000000000000m as double) aboveFloatRange, " +
                        "cast(-340282400000000000000000000000000000000m as double) belowFloatRange, " +
                        "cast(cast(null as DECIMAL(39,0)) as double) nullDecimal")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                maxDecimal\tminDecimal\taboveFloatRange\tbelowFloatRange\tnullDecimal
                                1.0E76\t-1.0E76\t3.402824E38\t-3.402824E38\tnull
                                """)
        );
    }

    @Test
    public void testCastLargestDecimal256MagnitudesFromColumn() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("select v, cast(v as double) double_value from y")
                        .ddl(
                                "create table y (v DECIMAL(76,2))",
                                "insert into y values (340282400000000000000000000000000000000.00m), " +
                                        "(-340282400000000000000000000000000000000.00m), " +
                                        "(123.45m), (-0.25m), (null)"
                        )
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                v\tdouble_value
                                340282400000000000000000000000000000000.00\t3.402824E38
                                -340282400000000000000000000000000000000.00\t-3.402824E38
                                123.45\t123.45
                                -0.25\t-0.25
                                \tnull
                                """)
        );
    }

    @Test
    public void testCastSmallestMagnitudes() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(0.00000000000000000000000000000000000001m as double) smallest, " +
                            "cast(-0.00000000000000000000000000000000000001m as double) smallestNeg")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    smallest\tsmallestNeg
                                    1.0E-38\t-1.0E-38
                                    """);

                    assertQuery("WITH data AS (SELECT 0.00000000000000000000000000000000000001m value " +
                            "UNION ALL SELECT -0.00000000000000000000000000000000000001m " +
                            "UNION ALL SELECT cast(null as DECIMAL(39,38))) " +
                            "SELECT cast(value as double) double_value FROM data")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    double_value
                                    1.0E-38
                                    -1.0E-38
                                    null
                                    """);
                }
        );
    }

    @Test
    public void testCastMatchesBigDecimalBitForBit() throws Exception {
        assertMemoryLeak(() -> {
            final int[] pathCounts = new int[2];
            for (int[] decimalType : DECIMAL_TYPES) {
                final int precision = decimalType[0];
                final int scale = decimalType[1];
                final List<BigInteger> values = unscaledValues(precision);
                final String tableName = "d" + precision + "_" + scale;

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
                        RecordCursorFactory factory = select("select cast(v as double) from " + tableName);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final Record record = cursor.getRecord();
                    for (int i = 0, n = values.size(); i < n; i++) {
                        final BigInteger unscaled = values.get(i);
                        Assert.assertTrue(cursor.hasNext());
                        final double expected = new BigDecimal(unscaled).movePointLeft(scale).doubleValue();
                        Assert.assertEquals(
                                "DECIMAL(" + precision + "," + scale + ") unscaled=" + unscaled,
                                Double.doubleToRawLongBits(expected),
                                Double.doubleToRawLongBits(record.getDouble(0))
                        );
                        pathCounts[usesExactShortcut(unscaled, scale) ? 0 : 1]++;
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            Assert.assertTrue(pathCounts[0] > 0);
            Assert.assertTrue(pathCounts[1] > 0);
        });
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

    /**
     * Values that straddle the exact-division limits in both directions, so that every decimal
     * type exercises both the exact shortcut and the decimal text fallback.
     */
    static List<BigInteger> unscaledValues(int precision) {
        final BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
        final LinkedHashSet<BigInteger> values = new LinkedHashSet<>();
        values.add(BigInteger.ZERO);
        values.add(max);
        values.add(max.negate());
        for (long seed : new long[]{1, 7, 99, 123456789, MAX_EXACT_UNSCALED - 1, MAX_EXACT_UNSCALED, MAX_EXACT_UNSCALED + 1, Long.MAX_VALUE}) {
            final BigInteger value = BigInteger.valueOf(seed);
            if (value.compareTo(max) <= 0) {
                values.add(value);
                values.add(value.negate());
            }
        }
        final Random rnd = new Random(precision);
        for (int i = 0; i < 12; i++) {
            BigInteger value = new BigInteger(1 + rnd.nextInt(max.bitLength()), rnd);
            if (value.compareTo(max) > 0) {
                continue;
            }
            values.add(rnd.nextBoolean() ? value : value.negate());
        }
        return new ArrayList<>(values);
    }

    static boolean usesExactShortcut(BigInteger unscaled, int scale) {
        return scale <= 22 && unscaled.abs().compareTo(BigInteger.valueOf(MAX_EXACT_UNSCALED)) <= 0;
    }
}
