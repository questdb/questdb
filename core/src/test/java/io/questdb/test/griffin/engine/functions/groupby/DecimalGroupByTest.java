/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.AvgDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.AvgDecimalRescaleGroupByFunctionFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class DecimalGroupByTest extends AbstractCairoTest {
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();

    public DecimalGroupByTest() {
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2);
        super.setUp();
    }

    @Test
    public void testDecimal128Avg() {
        // We write 10k rows, out of these:
        //   - We split the content in 3 symbols: 'a', 'b' and 'c'
        //   - 'c' is filled with null only (to ensure that null propagates to the end).
        //   - With CAIRO_SQL_PAGE_FRAME_MAX_ROWS=64, we have 1 page frame out of 4 that is filled with null decimals
        //   - The rest is filled with an incremental counter (0...9999) with a null value every 4th row
        // With these rules:
        //  - a: avg([(null frame), 1, 4, 7, 10, null, 16, 19, ..., 592, (null_frame), ..., 7420]) = 3710.5
        //  - b: avg([(null frame), 2, 5, 8, 11, null, 17, 20, ..., 593, (null_frame), ..., 7421]) = 3711.5
        //  - c: avg([null, ..., null]) = null
        testParallelQuery("""
                        CREATE TABLE x AS (
                          SELECT
                            timestamp_sequence(0, 1000000) AS ts,
                            (CASE
                                WHEN x%3 = 1 THEN 'a'
                                WHEN x%3 = 2 THEN 'b'
                                ELSE 'c'
                            END)::symbol AS sym,
                            (CASE
                                WHEN x%3 = 0 THEN NULL
                                WHEN x%15 >= 12 THEN NULL
                                WHEN x%900 <= 225 THEN NULL
                                ELSE abs(x - (1+x/900)*225)
                            END)::decimal(38, 2) as dec128
                          FROM long_sequence(10_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                """
                        SELECT sym, avg(dec128) AS avg_dec128
                        FROM x
                        ORDER BY sym
                        """,
                """
                        sym\tavg_dec128
                        a\t3710.50
                        b\t3711.50
                        c\t
                        """
        );
    }

    @Test
    public void testDecimal128AvgIter() {
        var values = new Decimal128[]{
                Decimal128.fromLong(999, 0),
                Decimal128.fromLong(123456789, 0),
                new Decimal128(54210108624275221L, -1, 0),
                Decimal128.NULL_VALUE
        };
        var precisions = new int[]{20, 26, 38};
        var scales = new int[]{0, 3, 5, 8, 18, 28, 33};
        var targetScales = new int[]{0, 3, 9, 18, 28, 38};
        for (var src : values) {
            for (var dst : values) {
                for (var precision : precisions) {
                    if (precision == 20 && (src.getHigh() > 99 || dst.getHigh() > 99)) {
                        continue;
                    } else if (precision == 26 && (src.getHigh() > 123456789 || dst.getHigh() > 123456789)) {
                        continue;
                    }
                    for (var scale : scales) {
                        if (scale >= precision) {
                            continue;
                        }

                        for (var targetScale : targetScales) {
                            // We're adding 20k times the value, adding at least 5 of precision.
                            if (precision + 5 - scale + targetScale >= 76) {
                                continue;
                            }
                            assertAvg(
                                    precision,
                                    scale,
                                    targetScale,
                                    src,
                                    dst
                            );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testDecimal16Avg() {
        // We write 10k rows, out of these:
        //   - We split the content in 3 symbols: 'a', 'b' and 'c'
        //   - 'c' is filled with null only (to ensure that null propagates to the end).
        //   - With CAIRO_SQL_PAGE_FRAME_MAX_ROWS=64, we have 1 page frame out of 4 that is filled with null decimals
        //   - The rest is filled with an incremental counter (0...9999) with a null value every 4th row
        // With these rules:
        //  - a: avg([(null frame), 1, 4, 7, 10, null, 16, 19, ..., 592, (null_frame), ..., 7420]) = 3710.5 => 3710
        //  - b: avg([(null frame), 2, 5, 8, 11, null, 17, 20, ..., 593, (null_frame), ..., 7421]) = 3711.5 => 3712
        //  - c: avg([null, ..., null]) = null
        testParallelQuery("""
                        CREATE TABLE x AS (
                          SELECT
                            timestamp_sequence(0, 1000000) AS ts,
                            (CASE
                                WHEN x%3 = 1 THEN 'a'
                                WHEN x%3 = 2 THEN 'b'
                                ELSE 'c'
                            END)::symbol AS sym,
                            (CASE
                                WHEN x%3 = 0 THEN NULL
                                WHEN x%15 >= 12 THEN NULL
                                WHEN x%900 <= 225 THEN NULL
                                ELSE (x - (1+x/900)*225)
                            END)::decimal(4, 0) as dec16
                          FROM long_sequence(10_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                """
                        SELECT sym, avg(dec16) AS avg_dec16
                        FROM x
                        ORDER BY sym
                        """,
                """
                        sym\tavg_dec16
                        a\t3710
                        b\t3712
                        c\t
                        """
        );
    }

    @Test
    public void testDecimal16AvgIter() {
        var values = new long[]{-12, 31, 1234, Decimals.DECIMAL64_NULL};
        var precisions = new int[]{3, 4};
        var scales = new int[]{0, 2};
        var targetScales = new int[]{0, 3, 9, 18, 28, 38};
        for (var src : values) {
            for (var dst : values) {
                for (var precision : precisions) {
                    if (precision == 3 && (src > 999 || dst > 999)) {
                        continue;
                    }

                    for (var scale : scales) {
                        for (var targetScale : targetScales) {
                            assertAvg(
                                    precision,
                                    scale,
                                    targetScale,
                                    src,
                                    dst
                            );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testDecimal256Avg() {
        // We write 10k rows, out of these:
        //   - We split the content in 3 symbols: 'a', 'b' and 'c'
        //   - 'c' is filled with null only (to ensure that null propagates to the end).
        //   - With CAIRO_SQL_PAGE_FRAME_MAX_ROWS=64, we have 1 page frame out of 4 that is filled with null decimals
        //   - The rest is filled with an incremental counter (0...9999) with a null value every 4th row
        // With these rules:
        //  - a: avg([(null frame), 1, 4, 7, 10, null, 16, 19, ..., 592, (null_frame), ..., 7420]) = 3710.5
        //  - b: avg([(null frame), 2, 5, 8, 11, null, 17, 20, ..., 593, (null_frame), ..., 7421]) = 3711.5
        //  - c: avg([null, ..., null]) = null
        testParallelQuery("""
                        CREATE TABLE x AS (
                          SELECT
                            timestamp_sequence(0, 1000000) AS ts,
                            (CASE
                                WHEN x%3 = 1 THEN 'a'
                                WHEN x%3 = 2 THEN 'b'
                                ELSE 'c'
                            END)::symbol AS sym,
                            (CASE
                                WHEN x%3 = 0 THEN NULL
                                WHEN x%15 >= 12 THEN NULL
                                WHEN x%900 <= 225 THEN NULL
                                ELSE abs(x - (1+x/900)*225)
                            END)::decimal(76, 2) as dec256
                          FROM long_sequence(10_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                """
                        SELECT sym, avg(dec256) AS avg_dec256
                        FROM x
                        ORDER BY sym
                        """,
                """
                        sym\tavg_dec256
                        a\t3710.50
                        b\t3711.50
                        c\t
                        """
        );
    }

    @Test
    public void testDecimal256AvgIter() {
        var values = new Decimal256[]{
                Decimal256.fromLong(999, 0),
                Decimal256.fromLong(123456789, 0),
                new Decimal256(123, 456, 789, 123, 0),
                Decimal256.NULL_VALUE
        };
        var precisions = new int[]{40, 56, 76};
        var scales = new int[]{0, 3, 5, 8, 18, 28, 45};
        var targetScales = new int[]{0, 3, 9, 18, 28, 38, 56};
        for (var src : values) {
            for (var dst : values) {
                for (var precision : precisions) {
                    if (precision < 76 && (src.getHh() > 0 || dst.getHh() > 0)) {
                        continue;
                    }
                    for (var scale : scales) {
                        if (scale >= precision) {
                            continue;
                        }

                        for (var targetScale : targetScales) {
                            // We're adding 20k times the value, adding at least 5 of precision.
                            if (precision + 5 - scale + targetScale >= 76) {
                                continue;
                            }
                            assertAvg(
                                    precision,
                                    scale,
                                    targetScale,
                                    src,
                                    dst
                            );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testDecimal32Avg() {
        // We write 10k rows, out of these:
        //   - We split the content in 3 symbols: 'a', 'b' and 'c'
        //   - 'c' is filled with null only (to ensure that null propagates to the end).
        //   - With CAIRO_SQL_PAGE_FRAME_MAX_ROWS=64, we have 1 page frame out of 4 that is filled with null decimals
        //   - The rest is filled with an incremental counter (0...9999) with a null value every 4th row
        // With these rules:
        //  - a: avg([(null frame), 1, 4, 7, 10, null, 16, 19, ..., 592, (null_frame), ..., 7420]) = 3710.5
        //  - b: avg([(null frame), 2, 5, 8, 11, null, 17, 20, ..., 593, (null_frame), ..., 7421]) = 3711.5
        //  - c: avg([null, ..., null]) = null
        testParallelQuery("""
                        CREATE TABLE x AS (
                          SELECT
                            timestamp_sequence(0, 1000000) AS ts,
                            (CASE
                                WHEN x%3 = 1 THEN 'a'
                                WHEN x%3 = 2 THEN 'b'
                                ELSE 'c'
                            END)::symbol AS sym,
                            (CASE
                                WHEN x%3 = 0 THEN NULL
                                WHEN x%15 >= 12 THEN NULL
                                WHEN x%900 <= 225 THEN NULL
                                ELSE (x - (1+x/900)*225)
                            END)::decimal(9, 2) as dec32
                          FROM long_sequence(10_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                """
                        SELECT sym, avg(dec32) AS avg_dec32
                        FROM x
                        ORDER BY sym
                        """,
                """
                        sym\tavg_dec32
                        a\t3710.50
                        b\t3711.50
                        c\t
                        """
        );
    }

    @Test
    public void testDecimal32AvgIter() {
        var values = new long[]{123, 999, 999_999_999, Decimals.DECIMAL64_NULL};
        var precisions = new int[]{5, 9};
        var scales = new int[]{0, 3};
        var targetScales = new int[]{0, 3, 9, 18, 28, 30};
        for (var src : values) {
            for (var dst : values) {
                for (var precision : precisions) {
                    if (precision < 9 && (src == 999_999_999 || dst == 999_999_999)) {
                        continue;
                    }

                    for (var scale : scales) {
                        for (var targetScale : targetScales) {
                            assertAvg(
                                    precision,
                                    scale,
                                    targetScale,
                                    src,
                                    dst
                            );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testDecimal64Avg() {
        // We write 10k rows, out of these:
        //   - We split the content in 3 symbols: 'a', 'b' and 'c'
        //   - 'c' is filled with null only (to ensure that null propagates to the end).
        //   - With CAIRO_SQL_PAGE_FRAME_MAX_ROWS=64, we have 1 page frame out of 4 that is filled with null decimals
        //   - The rest is filled with an incremental counter (0...9999) with a null value every 4th row
        // With these rules:
        //  - a: avg([(null frame), 1, 4, 7, 10, null, 16, 19, ..., 592, (null_frame), ..., 7420]) = 3710.5
        //  - b: avg([(null frame), 2, 5, 8, 11, null, 17, 20, ..., 593, (null_frame), ..., 7421]) = 3711.5
        //  - c: avg([null, ..., null]) = null
        testParallelQuery("""
                        CREATE TABLE x AS (
                          SELECT
                            timestamp_sequence(0, 1000000) AS ts,
                            (CASE
                                WHEN x%3 = 1 THEN 'a'
                                WHEN x%3 = 2 THEN 'b'
                                ELSE 'c'
                            END)::symbol AS sym,
                            (CASE
                                WHEN x%3 = 0 THEN NULL
                                WHEN x%15 >= 12 THEN NULL
                                WHEN x%900 <= 225 THEN NULL
                                ELSE abs(x - (1+x/900)*225)
                            END)::decimal(18, 2) as dec64
                          FROM long_sequence(10_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                """
                        SELECT sym, avg(dec64) AS avg_dec64
                        FROM x
                        ORDER BY sym
                        """,
                """
                        sym\tavg_dec64
                        a\t3710.50
                        b\t3711.50
                        c\t
                        """
        );
    }

    @Test
    public void testDecimal64AvgIter() {
        var values = new long[]{999L, 999_999_999, 999_999_999_999_999_999L, Decimals.DECIMAL64_NULL};
        var precisions = new int[]{10, 18};
        var scales = new int[]{0, 3, 5, 8, 12, 14, 16};
        var targetScales = new int[]{0, 3, 5, 8, 16, 28};
        for (var src : values) {
            for (var dst : values) {
                for (var precision : precisions) {
                    if (precision < 18 && (src == 999_999_999_999_999_999L || dst == 999_999_999_999_999_999L)) {
                        continue;
                    }

                    for (var scale : scales) {
                        if (scale >= precision) {
                            continue;
                        }
                        for (var targetScale : targetScales) {
                            assertAvg(
                                    precision,
                                    scale,
                                    targetScale,
                                    src,
                                    dst
                            );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testDecimal8Avg() {
        // We write 10k rows, out of these:
        //   - We split the content in 3 symbols: 'a', 'b' and 'c'
        //   - 'c' is filled with null only (to ensure that null propagates to the end).
        //   - With CAIRO_SQL_PAGE_FRAME_MAX_ROWS=64, we have 1 page frame out of 4 that is filled with null decimals
        //   - The rest is filled with an incremental counter (0...9999) modulo 100 with a null value every 4th row
        // With these rules:
        //  - a: avg([(null frame), 1, 4, 7, 10, null, 16, 19, ..., 592%100, (null_frame), ..., 7420%100]) ~= 48.87 => 49
        //  - b: avg([(null frame), 2, 5, 8, 11, null, 17, 20, ..., 593%100, (null_frame), ..., 7421%100]) ~= 49.14 => 49
        //  - c: avg([null, ..., null]) = null
        testParallelQuery("""
                        CREATE TABLE x AS (
                          SELECT
                            timestamp_sequence(0, 1000000) AS ts,
                            (CASE
                                WHEN x%3 = 1 THEN 'a'
                                WHEN x%3 = 2 THEN 'b'
                                ELSE 'c'
                            END)::symbol AS sym,
                            (CASE
                                WHEN x%3 = 0 THEN NULL
                                WHEN x%15 >= 12 THEN NULL
                                WHEN x%900 <= 225 THEN NULL
                                ELSE (x - (1+x/900)*225)%100
                            END)::decimal(4, 0) as dec8
                          FROM long_sequence(10_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                """
                        SELECT sym, avg(dec8) AS avg_dec8
                        FROM x
                        ORDER BY sym
                        """,
                """
                        sym\tavg_dec8
                        a\t49
                        b\t49
                        c\t
                        """
        );
    }

    @Test
    public void testDecimal8AvgIter() {
        var values = new long[]{-12, 31, Decimals.DECIMAL64_NULL};
        var precisions = new int[]{2};
        var scales = new int[]{0, 1};
        var targetScales = new int[]{0, 3, 9, 18, 28, 38};
        for (var src : values) {
            for (var dst : values) {
                for (var precision : precisions) {
                    for (var scale : scales) {
                        for (var targetScale : targetScales) {
                            assertAvg(
                                    precision,
                                    scale,
                                    targetScale,
                                    src,
                                    dst
                            );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testDecimalRescaleVariants() {
        testParallelQuery("""
                        CREATE TABLE x AS (
                          SELECT
                            timestamp_sequence(0, 1000000) AS ts,
                            (CASE
                                WHEN x%3 = 1 THEN 'a'
                                WHEN x%3 = 2 THEN 'b'
                                ELSE 'c'
                            END)::symbol AS sym,
                            (CASE
                                WHEN x%3 = 0 OR x%9 = 1 THEN NULL
                                WHEN x%3 = 1 THEN 42
                                ELSE 84
                            END)::decimal(2, 0) AS d8,
                            (CASE
                                WHEN x%3 = 0 OR x%9 = 2 THEN NULL
                                WHEN x%3 = 1 THEN '123.4'
                                ELSE '567.8'
                            END)::decimal(4, 1) AS d16,
                            (CASE
                                WHEN x%3 = 0 OR x%9 = 3 THEN NULL
                                WHEN x%3 = 1 THEN '1234.56'
                                ELSE '6543.21'
                            END)::decimal(7, 2) AS d32,
                            (CASE
                                WHEN x%3 = 0 OR x%9 = 4 THEN NULL
                                WHEN x%3 = 1 THEN '12345.678'
                                ELSE '98765.432'
                            END)::decimal(15, 3) AS d64,
                            (CASE
                                WHEN x%3 = 0 OR x%9 = 5 THEN NULL
                                WHEN x%3 = 1 THEN '123456789.1234'
                                ELSE '987654321.9876'
                            END)::decimal(32, 4) AS d128,
                            (CASE
                                WHEN x%3 = 0 OR x%9 = 6 THEN NULL
                                WHEN x%3 = 1 THEN '123456789012345.12345'
                                ELSE '987654321098765.98765'
                            END)::decimal(70, 5) AS d256
                          FROM long_sequence(10_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                """
                        SELECT
                          sym,
                          avg(d8, 0) AS avg_d8_s0,
                          avg(d8, 17) AS avg_d8_s17,
                          avg(d8, 37) AS avg_d8_s37,
                          avg(d16, 1) AS avg_d16_s1,
                          avg(d16, 16) AS avg_d16_s16,
                          avg(d16, 36) AS avg_d16_s36,
                          avg(d32, 2) AS avg_d32_s2,
                          avg(d32, 34) AS avg_d32_s34,
                          avg(d64, 3) AS avg_d64_s3,
                          avg(d64, 27) AS avg_d64_s27,
                          avg(d128, 10) AS avg_d128_s10,
                          avg(d256, 11) AS avg_d256_s11
                        FROM x
                        ORDER BY sym
                        """,
                """
                        sym\tavg_d8_s0\tavg_d8_s17\tavg_d8_s37\tavg_d16_s1\tavg_d16_s16\tavg_d16_s36\tavg_d32_s2\tavg_d32_s34\tavg_d64_s3\tavg_d64_s27\tavg_d128_s10\tavg_d256_s11
                        a\t42\t42.00000000000000000\t42.0000000000000000000000000000000000000\t123.4\t123.4000000000000000\t123.400000000000000000000000000000000000\t1234.56\t1234.5600000000000000000000000000000000\t12345.678\t12345.678000000000000000000000000\t123456789.1234000000\t123456789012345.12345000000
                        b\t84\t84.00000000000000000\t84.0000000000000000000000000000000000000\t567.8\t567.8000000000000000\t567.800000000000000000000000000000000000\t6543.21\t6543.2100000000000000000000000000000000\t98765.432\t98765.432000000000000000000000000\t987654321.9876000000\t987654321098765.98765000000
                        c\t\t\t\t\t\t\t\t\t\t\t\t
                        """
        );
    }

    private void assertAvg(int precision, int scale, int targetScale, long src, long dst) {
        var src256 = new Decimal256();
        if (src == Decimals.DECIMAL64_NULL) {
            src256.ofRawNull();
        } else {
            src256.ofRaw(src);
        }

        var dst256 = new Decimal256();
        if (dst == Decimals.DECIMAL64_NULL) {
            dst256.ofRawNull();
        } else {
            dst256.ofRaw(dst);
        }

        assertAvg(precision, scale, targetScale, src256, dst256);
    }

    private void assertAvg(int precision, int scale, int targetScale, Decimal128 src, Decimal128 dst) {
        var src256 = new Decimal256();
        if (src.isNull()) {
            src256.ofRawNull();
        } else {
            src256.ofRaw(src.getHigh(), src.getLow());
        }

        var dst256 = new Decimal256();
        if (dst.isNull()) {
            dst256.ofRawNull();
        } else {
            dst256.ofRaw(dst.getHigh(), dst.getLow());
        }

        assertAvg(precision, scale, targetScale, src256, dst256);
    }

    private void assertAvg(int precision, int scale, int targetScale, Decimal256 src, Decimal256 dst) {
        src.setScale(scale);
        dst.setScale(scale);

        try (var loader = new DecimalLoader(precision, scale)) {
            GroupByFunction func;
            if (scale == targetScale) {
                func = AvgDecimalGroupByFunctionFactory.newInstance(loader, 0);
            } else {
                func = AvgDecimalRescaleGroupByFunctionFactory.newInstance(loader, 0, targetScale);
            }

            var types = new ArrayColumnTypes();
            func.initValueTypes(types);

            var srcMap = buildMap(types.getColumnCount(), func, loader, src, 10000);
            var dstMap = buildMap(types.getColumnCount(), func, loader, dst, 10000);
            func.merge(dstMap, srcMap);

            // Compute the expected value
            var sum = BigDecimal.ZERO;
            int count = 0;
            if (!src.isNull()) {
                sum = sum.add(src.toBigDecimal().multiply(BigDecimal.valueOf(10000)));
                count += 10000;
            }
            if (!dst.isNull()) {
                sum = sum.add(dst.toBigDecimal().multiply(BigDecimal.valueOf(10000)));
                count += 10000;
            }

            if (count == 0) {
                // Both src and dest are null, we expect a null result
                DecimalUtil.load(decimal256, decimal128, func, dstMap, func.getType());
                Assert.assertTrue(decimal256.isNull());
            } else {
                var expected = sum.divide(BigDecimal.valueOf(count), targetScale, RoundingMode.HALF_EVEN);
                DecimalUtil.load(decimal256, decimal128, func, dstMap, func.getType());
                Assert.assertEquals(
                        String.format("result mismatch, expected %s but got %s [src=%s, dst=%s, precision=%d, scale=%d, targetScale=%d]",
                                expected,
                                decimal256,
                                src,
                                dst,
                                precision,
                                scale,
                                targetScale
                        ),
                        expected, decimal256.toBigDecimal());
            }
        }
    }

    private MapValue buildMap(
            int columnCount,
            GroupByFunction func,
            DecimalLoader loader,
            Decimal256 value,
            int count
    ) {
        var mapValue = new SimpleMapValue(columnCount);
        loader.value = value;
        func.computeFirst(mapValue, null, 0);
        for (int i = 1; i < count; i++) {
            func.computeNext(mapValue, null, i);
        }
        return mapValue;
    }

    private void testParallelQuery(String ddl, String query, String expected) {
        try {
            assertMemoryLeak(() -> {
                final WorkerPool pool = new WorkerPool(() -> 1);

                TestUtils.execute(
                        pool,
                        (engine, compiler, sqlExecutionContext) -> {
                            execute(
                                    compiler,
                                    ddl,
                                    sqlExecutionContext
                            );
                            TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                        },
                        configuration,
                        LOG
                );
            });
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    private static class DecimalLoader extends DecimalFunction {
        public Decimal256 value = new Decimal256();

        public DecimalLoader(int precision, int scale) {
            super(ColumnType.getDecimalType(precision, scale));
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
                return;
            }
            sink.ofRaw(
                    value.getLh(),
                    value.getLl()
            );
        }

        @Override
        public short getDecimal16(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            return (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
        }

        @Override
        public int getDecimal32(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            return (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL64_NULL;
            }
            return value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (value.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            return (byte) value.getLl();
        }
    }
}
