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

package io.questdb.test.griffin.engine.window;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Covers max/min/first_value/last_value/nth_value over a DATE argument. These resolve to the
 * dedicated DATE window factories (max(M)/min(M)/...), which store and emit values in DATE
 * milliseconds. Before the DATE factories existed a DATE argument fell through to the TIMESTAMP
 * factory, which threw UnsupportedOperationException on the streaming read path and silently
 * returned values 1000x in the future on the cached read path (microseconds read back as
 * milliseconds).
 * <p>
 * Each DATE value below is a distinct day with a matching millisecond component, so a unit u maps
 * to {@code 2020-01-0u T00:00:00.00u Z}. The 3-digit millisecond rendering also pins the result
 * column type to DATE: a TIMESTAMP result would render 6 fractional digits and fail the assertion.
 */
public class WindowDateFunctionTest extends AbstractCairoTest {

    private static final String CREATE_T =
            "CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, d DATE) TIMESTAMP(ts) PARTITION BY HOUR";
    // single partition 'a', ordered by ts, units 6, 4, 8, 2, 10
    private static final String INSERT_5 =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', '2020-01-06T00:00:00.006Z'::date), " +
                    "('2024-01-01T00:01:00', 'a', '2020-01-04T00:00:00.004Z'::date), " +
                    "('2024-01-01T00:02:00', 'a', '2020-01-08T00:00:00.008Z'::date), " +
                    "('2024-01-01T00:03:00', 'a', '2020-01-02T00:00:00.002Z'::date), " +
                    "('2024-01-01T00:04:00', 'a', '2020-01-10T00:00:00.010Z'::date)";
    // single partition 'a', NULLs at rows 0, 2, 4; units 6, 8 at rows 1, 3
    private static final String INSERT_5_WITH_NULL =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', null), " +
                    "('2024-01-01T00:01:00', 'a', '2020-01-06T00:00:00.006Z'::date), " +
                    "('2024-01-01T00:02:00', 'a', null), " +
                    "('2024-01-01T00:03:00', 'a', '2020-01-08T00:00:00.008Z'::date), " +
                    "('2024-01-01T00:04:00', 'a', null)";
    // two partitions; 'a' (ts 00,02,04) units 6, 8, 4; 'b' (ts 01,03,05) units 4, 2, 6
    private static final String INSERT_6_PART =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', '2020-01-06T00:00:00.006Z'::date), " +
                    "('2024-01-01T00:01:00', 'b', '2020-01-04T00:00:00.004Z'::date), " +
                    "('2024-01-01T00:02:00', 'a', '2020-01-08T00:00:00.008Z'::date), " +
                    "('2024-01-01T00:03:00', 'b', '2020-01-02T00:00:00.002Z'::date), " +
                    "('2024-01-01T00:04:00', 'a', '2020-01-04T00:00:00.004Z'::date), " +
                    "('2024-01-01T00:05:00', 'b', '2020-01-06T00:00:00.006Z'::date)";

    @Test
    public void testFirstValueIgnoreNullsRunning() throws Exception {
        assertQuery("SELECT ts, first_value(d) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:03:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:04:00.000000Z\t2020-01-06T00:00:00.006Z
                        """);
    }

    @Test
    public void testFirstValueOverPartitionCached() throws Exception {
        // whole-partition frame on the cached path: first row (ts order) of each partition
        assertQuery("SELECT ts, grp, first_value(d) OVER (PARTITION BY grp) f FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tf
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-04T00:00:00.004Z
                        """);
    }

    @Test
    public void testFirstValueRespectNullsRunning() throws Exception {
        // default RESPECT NULLS: first row of the frame is NULL, so every running result is NULL
        assertQuery("SELECT ts, first_value(d) OVER (ORDER BY ts) f FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t
                        2024-01-01T00:02:00.000000Z\t
                        2024-01-01T00:03:00.000000Z\t
                        2024-01-01T00:04:00.000000Z\t
                        """);
    }

    @Test
    public void testFirstValueRunning() throws Exception {
        assertQuery("SELECT ts, first_value(d) OVER (ORDER BY ts) f FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:03:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:04:00.000000Z\t2020-01-06T00:00:00.006Z
                        """);
    }

    @Test
    public void testLagOverPartition() throws Exception {
        assertQuery("SELECT ts, grp, lag(d) OVER (PARTITION BY grp ORDER BY ts) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t
                        2024-01-01T00:01:00.000000Z\tb\t
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.002Z
                        """);
    }

    @Test
    public void testLastValueIgnoreNullsRunning() throws Exception {
        assertQuery("SELECT ts, last_value(d) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.008Z
                        """);
    }

    @Test
    public void testLastValueOverPartitionCached() throws Exception {
        // whole-partition frame on the cached path: last row (ts order) of each partition
        assertQuery("SELECT ts, grp, last_value(d) OVER (PARTITION BY grp) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-06T00:00:00.006Z
                        """);
    }

    @Test
    public void testLastValueRunning() throws Exception {
        // default frame ends at the current row, so last_value is the current row's value
        assertQuery("SELECT ts, last_value(d) OVER (ORDER BY ts) l FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000Z\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:03:00.000000Z\t2020-01-02T00:00:00.002Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.010Z
                        """);
    }

    @Test
    public void testLeadOverPartition() throws Exception {
        assertQuery("SELECT ts, grp, lead(d) OVER (PARTITION BY grp ORDER BY ts) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-02T00:00:00.002Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:04:00.000000Z\ta\t
                        2024-01-01T00:05:00.000000Z\tb\t
                        """);
    }

    @Test
    public void testMaxOverPartitionCached() throws Exception {
        // PARTITION BY with no ORDER BY takes the two-pass cached path; before the fix this
        // returned dates ~1000x in the future
        assertQuery("SELECT ts, grp, max(d) OVER (PARTITION BY grp) m FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tm
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-06T00:00:00.006Z
                        """);
    }

    @Test
    public void testMaxOverWholeResultSet() throws Exception {
        assertQuery("SELECT max(d) OVER () m FROM t LIMIT 1")
                .ddl(CREATE_T, INSERT_5)
                .returns("""
                        m
                        2020-01-10T00:00:00.010Z
                        """);
    }

    @Test
    public void testMaxPartitionOrderRunning() throws Exception {
        assertQuery("SELECT ts, grp, max(d) OVER (PARTITION BY grp ORDER BY ts) m FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tm
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-06T00:00:00.006Z
                        """);
    }

    @Test
    public void testMaxRangeFrame() throws Exception {
        assertQuery("SELECT ts, max(d) OVER (ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) m FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.010Z
                        """);
    }

    @Test
    public void testMaxRangeFrameNanoTimestamp() throws Exception {
        // designated timestamp in nanoseconds; the DATE value stays in milliseconds and the
        // RANGE frame still orders by the nanosecond timestamp
        assertQuery("SELECT ts, max(d) OVER (ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) m FROM tn")
                .ddl(
                        "CREATE TABLE tn (ts TIMESTAMP_NS, d DATE) TIMESTAMP(ts) PARTITION BY HOUR",
                        "INSERT INTO tn VALUES " +
                                "('2024-01-01T00:00:00', '2020-01-06T00:00:00.006Z'::date), " +
                                "('2024-01-01T00:01:00', '2020-01-04T00:00:00.004Z'::date), " +
                                "('2024-01-01T00:02:00', '2020-01-08T00:00:00.008Z'::date), " +
                                "('2024-01-01T00:03:00', '2020-01-02T00:00:00.002Z'::date), " +
                                "('2024-01-01T00:04:00', '2020-01-10T00:00:00.010Z'::date)"
                )
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:03:00.000000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:04:00.000000000Z\t2020-01-10T00:00:00.010Z
                        """);
    }

    @Test
    public void testMaxRowsFrame() throws Exception {
        assertQuery("SELECT ts, max(d) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.010Z
                        """);
    }

    @Test
    public void testMaxRunning() throws Exception {
        assertQuery("SELECT ts, max(d) OVER (ORDER BY ts) m FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.010Z
                        """);
    }

    @Test
    public void testMaxWithNulls() throws Exception {
        assertQuery("SELECT ts, max(d) OVER (ORDER BY ts) m FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.008Z
                        """);
    }

    @Test
    public void testMinOverPartitionCached() throws Exception {
        assertQuery("SELECT ts, grp, min(d) OVER (PARTITION BY grp) m FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tm
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-02T00:00:00.002Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-02T00:00:00.002Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.002Z
                        """);
    }

    @Test
    public void testMinRunning() throws Exception {
        assertQuery("SELECT ts, min(d) OVER (ORDER BY ts) m FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000Z\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:02:00.000000Z\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:03:00.000000Z\t2020-01-02T00:00:00.002Z
                        2024-01-01T00:04:00.000000Z\t2020-01-02T00:00:00.002Z
                        """);
    }

    @Test
    public void testNthValueOverPartitionCached() throws Exception {
        // 2nd row (in ts order) of each partition: 'a' -> 8, 'b' -> 2
        assertQuery("SELECT ts, grp, nth_value(d, 2) OVER (PARTITION BY grp) n FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tn
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-02T00:00:00.002Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-02T00:00:00.002Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.008Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.002Z
                        """);
    }

    @Test
    public void testNthValueRunning() throws Exception {
        assertQuery("SELECT ts, nth_value(d, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tn
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:02:00.000000Z\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:03:00.000000Z\t2020-01-04T00:00:00.004Z
                        2024-01-01T00:04:00.000000Z\t2020-01-04T00:00:00.004Z
                        """);
    }

    @Test
    public void testTimestampWindowCastToDate() throws Exception {
        // casting a TIMESTAMP window result to DATE reaches WindowTimestampFunction.getDate(), which
        // converts timestamp ticks to DATE milliseconds; a naive passthrough would be ~1000x off
        assertQuery("SELECT ts, cast(max(v) OVER (ORDER BY ts) AS date) m FROM tt")
                .ddl(
                        "CREATE TABLE tt (ts TIMESTAMP, v TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR",
                        "INSERT INTO tt VALUES " +
                                "('2024-01-01T00:00:00', '2020-01-06T00:00:00.006Z'), " +
                                "('2024-01-01T00:01:00', '2020-01-08T00:00:00.008Z')"
                )
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.006Z
                        2024-01-01T00:01:00.000000Z\t2020-01-08T00:00:00.008Z
                        """);
    }
}
