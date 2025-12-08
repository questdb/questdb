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
import io.questdb.griffin.engine.functions.groupby.SumDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedList;

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
    public void testAvgFuzz() {
        Rnd rnd = TestUtils.generateRandom(LOG);

        for (int i = 0; i < 1_000; i++) {
            int precision = rnd.nextInt(72) + 1;
            int scale = rnd.nextInt(precision);
            int targetScale = rnd.nextInt(precision);
            if (precision - scale + targetScale + 5 >= 76) {
                continue;
            }
            try {
                assertAvgFuzz(rnd, precision, scale, targetScale, 3, 10, 1000);
            } catch (Throwable th) {
                System.err.printf("Failed at iteration %d with precision %d, scale %d and targetScale %d\n", i, precision, scale, targetScale);
                throw th;
            }
        }
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
    public void testDecimal128SumIter() {
        var values = new Decimal128[]{
                Decimal128.fromLong(999, 0),
                Decimal128.fromLong(123456789, 0),
                new Decimal128(54210108624275221L, -1, 0),
                Decimal128.NULL_VALUE
        };
        var precisions = new int[]{20, 26, 38};
        var scales = new int[]{0, 3, 5, 8, 18, 28, 33};
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

                        assertSum(
                                precision,
                                scale,
                                src,
                                dst
                        );
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
    public void testDecimal16SumIter() {
        var values = new long[]{-12, 31, 1234, Decimals.DECIMAL64_NULL};
        var precisions = new int[]{3, 4};
        var scales = new int[]{0, 2};
        for (var src : values) {
            for (var dst : values) {
                for (var precision : precisions) {
                    if (precision == 3 && (src > 999 || dst > 999)) {
                        continue;
                    }

                    for (var scale : scales) {
                        assertSum(
                                precision,
                                scale,
                                src,
                                dst
                        );
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
    public void testDecimal256SumIter() {
        var values = new Decimal256[]{
                Decimal256.fromLong(999, 0),
                Decimal256.fromLong(123456789, 0),
                new Decimal256(123, 456, 789, 123, 0),
                Decimal256.NULL_VALUE
        };
        var precisions = new int[]{40, 56, 76};
        var scales = new int[]{0, 3, 5, 8, 18, 28, 45};
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

                        assertSum(
                                precision,
                                scale,
                                src,
                                dst
                        );
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
    public void testDecimal32SumIter() {
        var values = new long[]{123, 999, 999_999_999, Decimals.DECIMAL64_NULL};
        var precisions = new int[]{5, 9};
        var scales = new int[]{0, 3};
        for (var src : values) {
            for (var dst : values) {
                for (var precision : precisions) {
                    if (precision < 9 && (src == 999_999_999 || dst == 999_999_999)) {
                        continue;
                    }

                    for (var scale : scales) {
                        assertSum(
                                precision,
                                scale,
                                src,
                                dst
                        );
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
    public void testDecimal64SumIter() {
        var values = new long[]{999L, 999_999_999, 999_999_999_999_999_999L, Decimals.DECIMAL64_NULL};
        var precisions = new int[]{10, 18};
        var scales = new int[]{0, 3, 5, 8, 12, 14, 16};
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
                        assertSum(
                                precision,
                                scale,
                                src,
                                dst
                        );
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
    public void testDecimal8SumIter() {
        var values = new long[]{-12, 31, Decimals.DECIMAL64_NULL};
        var precisions = new int[]{2};
        var scales = new int[]{0, 1};
        for (var src : values) {
            for (var dst : values) {
                for (var precision : precisions) {
                    for (var scale : scales) {
                        assertSum(
                                precision,
                                scale,
                                src,
                                dst
                        );
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

    @Test
    public void testSumFuzz() {
        Rnd rnd = TestUtils.generateRandom(LOG);

        for (int i = 0; i < 1_000; i++) {
            int precision = rnd.nextInt(72) + 1;
            int scale = rnd.nextInt(precision);
            try {
                assertSumFuzz(rnd, precision, scale, 3, 10, 1000);
            } catch (Throwable th) {
                System.err.printf("Failed at iteration %d with precision %d and scale %d\n", i, precision, scale);
                throw th;
            }
        }
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

        try (var loader = new StaticDecimalLoader(precision, scale)) {
            GroupByFunction func;
            if (scale == targetScale) {
                func = AvgDecimalGroupByFunctionFactory.newInstance(loader, 0);
            } else {
                func = AvgDecimalRescaleGroupByFunctionFactory.newInstance(loader, 0, targetScale);
            }

            var types = new ArrayColumnTypes();
            func.initValueTypes(types);

            loader.value = src;
            var srcMap = buildMap(types.getColumnCount(), func, 10000);
            loader.value = dst;
            var dstMap = buildMap(types.getColumnCount(), func, 10000);
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

    private void assertAvgFuzz(Rnd rnd, int precision, int scale, int targetScale, int nanRate, int nMaps, int maxRowsPerMap) {
        try (var loader = new RandomDecimalLoader(rnd, precision, scale, nanRate)) {
            GroupByFunction func;
            if (scale == targetScale) {
                func = AvgDecimalGroupByFunctionFactory.newInstance(loader, 0);
            } else {
                func = AvgDecimalRescaleGroupByFunctionFactory.newInstance(loader, 0, targetScale);
            }

            var types = new ArrayColumnTypes();
            func.initValueTypes(types);

            var maps = new LinkedList<MapValue>();
            loader.reset();
            for (int i = 0; i < nMaps; i++) {
                var map = buildMap(types.getColumnCount(), func, rnd.nextPositiveInt() % maxRowsPerMap);
                maps.push(map);
            }

            while (maps.size() > 1) {
                var m1 = maps.pop();
                var m2 = maps.pop();
                func.merge(m1, m2);
                maps.addLast(m1);
            }

            var m1 = maps.pop();
            DecimalUtil.load(decimal256, decimal128, func, m1, func.getType());
            if (loader.getCount() == 0) {
                // Both src and dest are null, we expect a null result
                Assert.assertTrue(decimal256.isNull());
            } else {
                var sum = loader.getAccumulator();
                var expected = sum.divide(BigDecimal.valueOf(loader.getCount()), targetScale, RoundingMode.HALF_EVEN);
                Assert.assertEquals(
                        String.format("result mismatch, expected %s but got %s [precision=%d, scale=%d, targetScale=%d]",
                                expected,
                                decimal256,
                                precision,
                                scale,
                                targetScale
                        ),
                        expected, decimal256.toBigDecimal());
            }
        }
    }

    private void assertSum(int precision, int scale, Decimal128 src, Decimal128 dst) {
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

        assertSum(precision, scale, src256, dst256);
    }

    private void assertSum(int precision, int scale, long src, long dst) {
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

        assertSum(precision, scale, src256, dst256);
    }

    private void assertSum(int precision, int scale, Decimal256 src, Decimal256 dst) {
        src.setScale(scale);
        dst.setScale(scale);

        try (var loader = new StaticDecimalLoader(precision, scale)) {
            GroupByFunction func;
            func = SumDecimalGroupByFunctionFactory.newInstance(loader, 0);

            var types = new ArrayColumnTypes();
            func.initValueTypes(types);

            loader.value = src;
            var srcMap = buildMap(types.getColumnCount(), func, 10000);
            loader.value = dst;
            var dstMap = buildMap(types.getColumnCount(), func, 10000);
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

            DecimalUtil.load(decimal256, decimal128, func, dstMap, func.getType());
            if (count == 0) {
                // Both src and dest are null, we expect a null result
                Assert.assertTrue(decimal256.isNull());
            } else {
                Assert.assertEquals(
                        String.format("result mismatch, expected %s but got %s [precision=%d, scale=%d, src=%s, dst=%s]",
                                sum,
                                decimal256,
                                precision,
                                scale,
                                src,
                                dst
                        ),
                        sum, decimal256.toBigDecimal());
            }
        }
    }

    private void assertSumFuzz(Rnd rnd, int precision, int scale, int nanRate, int nMaps, int maxRowsPerMap) {
        try (var loader = new RandomDecimalLoader(rnd, precision, scale, nanRate)) {
            GroupByFunction func;
            func = SumDecimalGroupByFunctionFactory.newInstance(loader, 0);

            var types = new ArrayColumnTypes();
            func.initValueTypes(types);

            var maps = new LinkedList<Triple<MapValue, BigDecimal, Integer>>();
            for (int i = 0; i < nMaps; i++) {
                loader.reset();
                var map = buildMap(types.getColumnCount(), func, rnd.nextPositiveInt() % maxRowsPerMap);
                var acc = loader.getAccumulator();
                var count = loader.getCount();
                // Check that the accumulation worked properly on a per-map basis
                DecimalUtil.load(decimal256, decimal128, func, map, func.getType());
                if (count == 0) {
                    Assert.assertTrue(decimal256.isNull());
                } else {
                    Assert.assertEquals(
                            String.format("result mismatch, expected %s but got %s [precision=%d, scale=%d] when computing map %d",
                                    acc,
                                    decimal256,
                                    precision,
                                    scale,
                                    i
                            ),
                            acc, decimal256.toBigDecimal());
                }

                var triple = new Triple(map, acc, count);
                maps.push(triple);
            }

            int mapIter = 0;
            while (maps.size() > 1) {
                var m1 = maps.pop();
                var m2 = maps.pop();
                func.merge(m1.a, m2.a);

                var sum = m1.b.add(m2.b);
                int count = m1.c + m2.c;

                // Check that the merge worked properly
                DecimalUtil.load(decimal256, decimal128, func, m1.a, func.getType());
                if (count == 0) {
                    Assert.assertTrue(decimal256.isNull());
                } else {
                    Assert.assertEquals(
                            String.format("result mismatch, expected %s but got %s [precision=%d, scale=%d] at merge step %d",
                                    sum,
                                    decimal256,
                                    precision,
                                    scale,
                                    mapIter
                            ),
                            sum, decimal256.toBigDecimal());
                }

                mapIter++;
                m1.b = sum;
                m1.c = count;
                maps.addLast(m1);
            }

            var m1 = maps.pop();
            DecimalUtil.load(decimal256, decimal128, func, m1.a, func.getType());
            if (m1.c == 0) {
                // Both src and dest are null, we expect a null result
                Assert.assertTrue(decimal256.isNull());
            } else {
                Assert.assertEquals(
                        String.format("result mismatch, expected %s but got %s [precision=%d, scale=%d]",
                                m1.b,
                                decimal256,
                                precision,
                                scale
                        ),
                        m1.b, decimal256.toBigDecimal());
            }
        }
    }

    private MapValue buildMap(
            int columnCount,
            GroupByFunction func,
            int count
    ) {
        var mapValue = new SimpleMapValue(columnCount);
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

    private static class RandomDecimalLoader extends DecimalFunction {
        private final long hhRange;
        private final long highRange;
        private final long hlRange;
        private final int nanRate;
        private final int range;
        private final Rnd rnd;
        private final int scale;
        private BigDecimal accumulator = BigDecimal.ZERO;
        private int count = 0;

        public RandomDecimalLoader(Rnd rnd, int precision, int scale, int nanRate) {
            super(ColumnType.getDecimalType(precision, scale));
            this.rnd = rnd;
            this.nanRate = nanRate;
            this.scale = scale;
            switch (ColumnType.tagOf(type)) {
                case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32 -> {
                    hhRange = hlRange = highRange = 0;
                    range = (int) Numbers.getMaxValue(precision);
                }
                case ColumnType.DECIMAL64 -> {
                    hhRange = hlRange = range = 0;
                    highRange = Numbers.getMaxValue(precision);
                }
                case ColumnType.DECIMAL128 -> {
                    hhRange = hlRange = range = 0;
                    final int highPrec = precision - Numbers.getPrecision(Long.MAX_VALUE) - 1;
                    highRange = highPrec > 0 ? Numbers.getMaxValue(highPrec) : 0;
                }
                default -> {
                    highRange = range = 0;
                    final int maxLongPrecision = Numbers.getPrecision(Long.MAX_VALUE);
                    final int hhPrecision = Math.max(precision - 3 * maxLongPrecision - 1, 0);
                    hhRange = hhPrecision > 0 ? Numbers.getMaxValue(hhPrecision) : 0;
                    final int hlPrecision = Math.max(precision - 2 * maxLongPrecision - 1, 0);
                    hlRange = hlPrecision > 0 ? Numbers.getMaxValue(hlPrecision) : 0;
                }
            }
        }

        public BigDecimal getAccumulator() {
            return accumulator;
        }

        public int getCount() {
            return count;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (nanRate != 0 && rnd.nextInt() % nanRate == 0) {
                sink.ofRawNull();
                return;
            }
            count++;
            sink.ofRaw(
                    highRange > 0 ? rnd.nextPositiveLong() % highRange : 0,
                    rnd.nextLong()
            );
            sink.setScale(scale);
            accumulator = accumulator.add(sink.toBigDecimal());
        }

        @Override
        public short getDecimal16(Record rec) {
            if (nanRate != 0 && rnd.nextInt() % nanRate == 0) {
                return Decimals.DECIMAL16_NULL;
            }
            count++;
            short v = (short) (rnd.nextPositiveInt() % range);
            accumulator = accumulator.add(BigDecimal.valueOf(v, scale));
            return v;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            if (nanRate != 0 && rnd.nextInt() % nanRate == 0) {
                sink.ofRawNull();
                return;
            }
            count++;
            sink.ofRaw(
                    hhRange > 0 ? rnd.nextPositiveLong() % hhRange : 0,
                    hlRange > 0 ? rnd.nextPositiveLong() % hlRange : 0,
                    rnd.nextLong(),
                    rnd.nextLong()
            );
            sink.setScale(scale);
            accumulator = accumulator.add(sink.toBigDecimal());
        }

        @Override
        public int getDecimal32(Record rec) {
            if (nanRate != 0 && rnd.nextInt() % nanRate == 0) {
                return Decimals.DECIMAL32_NULL;
            }
            count++;
            int v = rnd.nextPositiveInt() % range;
            accumulator = accumulator.add(BigDecimal.valueOf(v, scale));
            return v;
        }

        @Override
        public long getDecimal64(Record rec) {
            if (nanRate != 0 && rnd.nextInt() % nanRate == 0) {
                return Decimals.DECIMAL64_NULL;
            }
            count++;
            long v = rnd.nextPositiveLong() % highRange;
            accumulator = accumulator.add(BigDecimal.valueOf(v, scale));
            return v;
        }

        @Override
        public byte getDecimal8(Record rec) {
            if (nanRate != 0 && rnd.nextInt() % nanRate == 0) {
                return Decimals.DECIMAL8_NULL;
            }
            count++;
            byte v = (byte) (rnd.nextPositiveInt() % range);
            accumulator = accumulator.add(BigDecimal.valueOf(v, scale));
            return v;
        }

        public void reset() {
            accumulator = BigDecimal.ZERO;
            count = 0;
        }
    }

    private static class StaticDecimalLoader extends DecimalFunction {
        public Decimal256 value = new Decimal256();

        public StaticDecimalLoader(int precision, int scale) {
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

    private class Triple<A, B, C> {
        public A a;
        public B b;
        public C c;

        public Triple(A a, B b, C c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
    }
}
