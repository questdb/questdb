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
 * Covers max/min/first_value/last_value/nth_value over a TIMESTAMP argument. These resolve to the
 * dedicated TIMESTAMP window factories (max(N)/min(N)/...), which store and emit values in
 * timestamp microseconds. This mirrors {@link WindowDateFunctionTest} for the TIMESTAMP variants;
 * the logical results are identical, only the rendered precision differs (6 fractional digits).
 */
public class WindowTimestampFunctionTest extends AbstractCairoTest {

    private static final String CREATE_T =
            "CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR";
    // single partition 'a', ordered by ts, units 6, 4, 8, 2, 10
    private static final String INSERT_5 =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', '2020-01-06T00:00:00.000000Z'), " +
                    "('2024-01-01T00:01:00', 'a', '2020-01-04T00:00:00.000000Z'), " +
                    "('2024-01-01T00:02:00', 'a', '2020-01-08T00:00:00.000000Z'), " +
                    "('2024-01-01T00:03:00', 'a', '2020-01-02T00:00:00.000000Z'), " +
                    "('2024-01-01T00:04:00', 'a', '2020-01-10T00:00:00.000000Z')";
    // single partition 'a', NULLs at rows 0, 2, 4; units 6, 8 at rows 1, 3
    private static final String INSERT_5_WITH_NULL =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', null), " +
                    "('2024-01-01T00:01:00', 'a', '2020-01-06T00:00:00.000000Z'), " +
                    "('2024-01-01T00:02:00', 'a', null), " +
                    "('2024-01-01T00:03:00', 'a', '2020-01-08T00:00:00.000000Z'), " +
                    "('2024-01-01T00:04:00', 'a', null)";
    // two partitions; 'a' (ts 00,02,04) units 6, 8, 4; 'b' (ts 01,03,05) units 4, 2, 6
    private static final String INSERT_6_PART =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', '2020-01-06T00:00:00.000000Z'), " +
                    "('2024-01-01T00:01:00', 'b', '2020-01-04T00:00:00.000000Z'), " +
                    "('2024-01-01T00:02:00', 'a', '2020-01-08T00:00:00.000000Z'), " +
                    "('2024-01-01T00:03:00', 'b', '2020-01-02T00:00:00.000000Z'), " +
                    "('2024-01-01T00:04:00', 'a', '2020-01-04T00:00:00.000000Z'), " +
                    "('2024-01-01T00:05:00', 'b', '2020-01-06T00:00:00.000000Z')";
    // two partitions with NULLs; 'a' (ts 00,02,04) null, 6, 8; 'b' (ts 01,03,05) 4, null, 2
    private static final String INSERT_6_PART_NULL =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', null), " +
                    "('2024-01-01T00:01:00', 'b', '2020-01-04T00:00:00.000000Z'), " +
                    "('2024-01-01T00:02:00', 'a', '2020-01-06T00:00:00.000000Z'), " +
                    "('2024-01-01T00:03:00', 'b', null), " +
                    "('2024-01-01T00:04:00', 'a', '2020-01-08T00:00:00.000000Z'), " +
                    "('2024-01-01T00:05:00', 'b', '2020-01-02T00:00:00.000000Z')";

    @Test
    public void testFirstValueCurrentRow() throws Exception {
        // ROWS BETWEEN CURRENT ROW AND CURRENT ROW: the value is always the current row
        assertQuery("SELECT ts, first_value(v) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueIgnoreNullsOverPartition() throws Exception {
        // whole-partition frame, IGNORE NULLS: first non-null row (ts order) of each partition
        assertQuery("SELECT ts, grp, first_value(v) IGNORE NULLS OVER (PARTITION BY grp) f FROM t")
                .ddl(CREATE_T, INSERT_6_PART_NULL)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tf
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueIgnoreNullsOverWholeResultSet() throws Exception {
        // IGNORE NULLS over the whole result set: the first non-null value (unit 6 at row 1)
        assertQuery("SELECT ts, first_value(v) IGNORE NULLS OVER () f FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-06T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueIgnoreNullsPartitionRangeFrame() throws Exception {
        // bounded RANGE frame within each partition, IGNORE NULLS: first non-null row in [ts-2min, ts]
        assertQuery("SELECT ts, grp, first_value(v) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '2' MINUTE PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_6_PART_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tf
                        2024-01-01T00:00:00.000000Z\ta\t
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueIgnoreNullsPartitionRowsFrame() throws Exception {
        // bounded ROWS frame within each partition, IGNORE NULLS: first non-null row in [i-1, i]
        assertQuery("SELECT ts, grp, first_value(v) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_6_PART_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tf
                        2024-01-01T00:00:00.000000Z\ta\t
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueIgnoreNullsRangeFrame() throws Exception {
        // bounded RANGE frame, IGNORE NULLS: first non-null row in [ts-1min, ts]
        assertQuery("SELECT ts, first_value(v) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueIgnoreNullsRowsFrame() throws Exception {
        // bounded ROWS frame, IGNORE NULLS: first non-null row in [i-1, i]
        assertQuery("SELECT ts, first_value(v) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueIgnoreNullsRunning() throws Exception {
        assertQuery("SELECT ts, first_value(v) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-06T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueOverPartitionCached() throws Exception {
        // whole-partition frame on the cached path: first row (ts order) of each partition
        assertQuery("SELECT ts, grp, first_value(v) OVER (PARTITION BY grp) f FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tf
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValuePartitionRangeFrame() throws Exception {
        // bounded RANGE frame within each partition: first row in [ts-2min, ts]
        assertQuery("SELECT ts, grp, first_value(v) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '2' MINUTE PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tf
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValuePartitionRowsFrame() throws Exception {
        // bounded ROWS frame within each partition: first row in [i-1, i]
        assertQuery("SELECT ts, grp, first_value(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tf
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueRangeFrame() throws Exception {
        // bounded RANGE frame: first row in [ts-1min, ts]
        assertQuery("SELECT ts, first_value(v) OVER (ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueRespectNullsRunning() throws Exception {
        // default RESPECT NULLS: first row of the frame is NULL, so every running result is NULL
        assertQuery("SELECT ts, first_value(v) OVER (ORDER BY ts) f FROM t")
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
    public void testFirstValueRowsExcludingCurrentRow() throws Exception {
        // ROWS frame entirely before the current row: [i-2, i-1]
        assertQuery("SELECT ts, first_value(v) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) f FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueRowsFrame() throws Exception {
        // bounded ROWS frame: first row in [i-1, i]
        assertQuery("SELECT ts, first_value(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testFirstValueRunning() throws Exception {
        assertQuery("SELECT ts, first_value(v) OVER (ORDER BY ts) f FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tf
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-06T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLagOverPartition() throws Exception {
        assertQuery("SELECT ts, grp, lag(v) OVER (PARTITION BY grp ORDER BY ts) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t
                        2024-01-01T00:01:00.000000Z\tb\t
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueIgnoreNullsOverPartition() throws Exception {
        // whole-partition frame, IGNORE NULLS: last non-null row (ts order) of each partition
        assertQuery("SELECT ts, grp, last_value(v) IGNORE NULLS OVER (PARTITION BY grp) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART_NULL)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueIgnoreNullsOverWholeResultSet() throws Exception {
        // IGNORE NULLS over the whole result set: the last non-null value (unit 8 at row 3)
        assertQuery("SELECT ts, last_value(v) IGNORE NULLS OVER () l FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueIgnoreNullsPartitionRangeFrame() throws Exception {
        // bounded RANGE frame within each partition, IGNORE NULLS: last non-null row in [ts-2min, ts]
        assertQuery("SELECT ts, grp, last_value(v) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '2' MINUTE PRECEDING AND CURRENT ROW) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueIgnoreNullsPartitionRowsFrame() throws Exception {
        // bounded ROWS frame within each partition, IGNORE NULLS: last non-null row in [i-1, i]
        assertQuery("SELECT ts, grp, last_value(v) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueIgnoreNullsRangeFrame() throws Exception {
        // bounded RANGE frame, IGNORE NULLS: last non-null row in [ts-1min, ts]
        assertQuery("SELECT ts, last_value(v) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) l FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueIgnoreNullsRowsFrame() throws Exception {
        // bounded ROWS frame, IGNORE NULLS: last non-null row in [i-1, i]
        assertQuery("SELECT ts, last_value(v) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueIgnoreNullsRunning() throws Exception {
        assertQuery("SELECT ts, last_value(v) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueOverPartitionCached() throws Exception {
        // whole-partition frame on the cached path: last row (ts order) of each partition
        assertQuery("SELECT ts, grp, last_value(v) OVER (PARTITION BY grp) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-06T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueOverWholeResultSet() throws Exception {
        // last_value over the whole result set: the last row's value (unit 10)
        assertQuery("SELECT ts, last_value(v) OVER () l FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t2020-01-10T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-10T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-10T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-10T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValuePartitionRangeFrame() throws Exception {
        // bounded RANGE frame within each partition: the frame ends at the current row, so the
        // last value is the current row's value
        assertQuery("SELECT ts, grp, last_value(v) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '2' MINUTE PRECEDING AND CURRENT ROW) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-06T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValuePartitionRowsExcludingCurrentRow() throws Exception {
        // ROWS frame entirely before the current row within each partition: [i-2, i-1]
        assertQuery("SELECT ts, grp, last_value(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t
                        2024-01-01T00:01:00.000000Z\tb\t
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueRowsExcludingCurrentRow() throws Exception {
        // ROWS frame entirely before the current row: [i-2, i-1]
        assertQuery("SELECT ts, last_value(v) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueRangeFrame() throws Exception {
        // bounded RANGE frame: the frame ends at the current row, so the last value is the
        // current row's value
        assertQuery("SELECT ts, last_value(v) OVER (ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) l FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLastValueRunning() throws Exception {
        // default frame ends at the current row, so last_value is the current row's value
        assertQuery("SELECT ts, last_value(v) OVER (ORDER BY ts) l FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tl
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.000000Z
                        """);
    }

    @Test
    public void testLeadOverPartition() throws Exception {
        assertQuery("SELECT ts, grp, lead(v) OVER (PARTITION BY grp ORDER BY ts) l FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tl
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t
                        2024-01-01T00:05:00.000000Z\tb\t
                        """);
    }

    @Test
    public void testMaxOverPartitionCached() throws Exception {
        // PARTITION BY with no ORDER BY takes the two-pass cached path
        assertQuery("SELECT ts, grp, max(v) OVER (PARTITION BY grp) m FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tm
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-06T00:00:00.000000Z
                        """);
    }

    @Test
    public void testMaxOverWholeResultSet() throws Exception {
        assertQuery("SELECT max(v) OVER () m FROM t LIMIT 1")
                .ddl(CREATE_T, INSERT_5)
                .returns("""
                        m
                        2020-01-10T00:00:00.000000Z
                        """);
    }

    @Test
    public void testMaxPartitionOrderRunning() throws Exception {
        assertQuery("SELECT ts, grp, max(v) OVER (PARTITION BY grp ORDER BY ts) m FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tgrp\tm
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-06T00:00:00.000000Z
                        """);
    }

    @Test
    public void testMaxRangeFrame() throws Exception {
        assertQuery("SELECT ts, max(v) OVER (ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) m FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.000000Z
                        """);
    }

    @Test
    public void testMaxRowsFrame() throws Exception {
        assertQuery("SELECT ts, max(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.000000Z
                        """);
    }

    @Test
    public void testMaxRunning() throws Exception {
        assertQuery("SELECT ts, max(v) OVER (ORDER BY ts) m FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-10T00:00:00.000000Z
                        """);
    }

    @Test
    public void testMaxWithNulls() throws Exception {
        assertQuery("SELECT ts, max(v) OVER (ORDER BY ts) m FROM t")
                .ddl(CREATE_T, INSERT_5_WITH_NULL)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-08T00:00:00.000000Z
                        """);
    }

    @Test
    public void testMinOverPartitionCached() throws Exception {
        assertQuery("SELECT ts, grp, min(v) OVER (PARTITION BY grp) m FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tm
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testMinRunning() throws Exception {
        assertQuery("SELECT ts, min(v) OVER (ORDER BY ts) m FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tm
                        2024-01-01T00:00:00.000000Z\t2020-01-06T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testNthValueOverPartitionCached() throws Exception {
        // 2nd row (in ts order) of each partition: 'a' -> 8, 'b' -> 2
        assertQuery("SELECT ts, grp, nth_value(v, 2) OVER (PARTITION BY grp) n FROM t")
                .ddl(CREATE_T, INSERT_6_PART)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tgrp\tn
                        2024-01-01T00:00:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:01:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\ta\t2020-01-08T00:00:00.000000Z
                        2024-01-01T00:05:00.000000Z\tb\t2020-01-02T00:00:00.000000Z
                        """);
    }

    @Test
    public void testNthValueRunning() throws Exception {
        assertQuery("SELECT ts, nth_value(v, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n FROM t")
                .ddl(CREATE_T, INSERT_5)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tn
                        2024-01-01T00:00:00.000000Z\t
                        2024-01-01T00:01:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:02:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:03:00.000000Z\t2020-01-04T00:00:00.000000Z
                        2024-01-01T00:04:00.000000Z\t2020-01-04T00:00:00.000000Z
                        """);
    }

}
