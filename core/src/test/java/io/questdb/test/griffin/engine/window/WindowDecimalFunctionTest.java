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

public class WindowDecimalFunctionTest extends AbstractCairoTest {

    private static final String CREATE_T =
            "CREATE TABLE t (" +
                    "ts TIMESTAMP, grp SYMBOL, " +
                    "v8 decimal(2, 1), v16 decimal(4, 1), v32 decimal(9, 3), " +
                    "v64 decimal(18, 2), v128 decimal(38, 6), v256 decimal(60, 0)" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR";

    // 5 rows, single partition 'a', non-monotonic with duplicates: 0.6, 0.4, 0.8, 0.2, 1.0
    // sum=3.0 avg=0.6 max=1.0 min=0.2 count=5 first=0.6 last=1.0 nth(2)=0.4
    private static final String INSERT_5 =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 0.6m, 6.0m, 6.000m, 6.00m, 6.000000m, 6m), " +
                    "('2024-01-01T00:01:00', 'a', 0.4m, 4.0m, 4.000m, 4.00m, 4.000000m, 4m), " +
                    "('2024-01-01T00:02:00', 'a', 0.8m, 8.0m, 8.000m, 8.00m, 8.000000m, 8m), " +
                    "('2024-01-01T00:03:00', 'a', 0.2m, 2.0m, 2.000m, 2.00m, 2.000000m, 2m), " +
                    "('2024-01-01T00:04:00', 'a', 1.0m, 10.0m, 10.000m, 10.00m, 10.000000m, 10m)";
    private static final String INSERT_5_ALL_NULL =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:01:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:02:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:03:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:04:00', 'a', null, null, null, null, null, null)";
    // 5 rows mixed signs: -0.6, 0.4, -0.8, 0.2, -0.2
    // sum=-1.0 avg=-0.2 max=0.4 min=-0.8 count=5 first=-0.6 last=-0.2 nth(2)=0.4
    private static final String INSERT_5_NEGATIVE =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', -0.6m, -6.0m, -6.000m, -6.00m, -6.000000m, -6m), " +
                    "('2024-01-01T00:01:00', 'a', 0.4m, 4.0m, 4.000m, 4.00m, 4.000000m, 4m), " +
                    "('2024-01-01T00:02:00', 'a', -0.8m, -8.0m, -8.000m, -8.00m, -8.000000m, -8m), " +
                    "('2024-01-01T00:03:00', 'a', 0.2m, 2.0m, 2.000m, 2.00m, 2.000000m, 2m), " +
                    "('2024-01-01T00:04:00', 'a', -0.2m, -2.0m, -2.000m, -2.00m, -2.000000m, -2m)";
    // 5 rows with NULLs at positions 0, 2, 4. Non-nulls: 0.6 (row 1), 0.8 (row 3).
    // sum=1.4 avg=0.7 max=0.8 min=0.6 count=2 first(R)=null last(R)=null nth(2,R)=0.6
    // first(I)=0.6 last(I)=0.8 nth(2,I)=0.8
    private static final String INSERT_5_WITH_NULL =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:01:00', 'a', 0.6m, 6.0m, 6.000m, 6.00m, 6.000000m, 6m), " +
                    "('2024-01-01T00:02:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:03:00', 'a', 0.8m, 8.0m, 8.000m, 8.00m, 8.000000m, 8m), " +
                    "('2024-01-01T00:04:00', 'a', null, null, null, null, null, null)";
    // 6 rows alternating 'a'/'b'.
    // 'a' (00:00,02,04): 0.6, 0.8, 0.4 -> sum=1.8 avg=0.6 max=0.8 min=0.4 first=0.6 last=0.4
    // 'b' (00:01,03,05): 0.4, 0.2, 0.6 -> sum=1.2 avg=0.4 max=0.6 min=0.2 first=0.4 last=0.6
    private static final String INSERT_6_PART =
            "INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 0.6m, 6.0m, 6.000m, 6.00m, 6.000000m, 6m), " +
                    "('2024-01-01T00:01:00', 'b', 0.4m, 4.0m, 4.000m, 4.00m, 4.000000m, 4m), " +
                    "('2024-01-01T00:02:00', 'a', 0.8m, 8.0m, 8.000m, 8.00m, 8.000000m, 8m), " +
                    "('2024-01-01T00:03:00', 'b', 0.2m, 2.0m, 2.000m, 2.00m, 2.000000m, 2m), " +
                    "('2024-01-01T00:04:00', 'a', 0.4m, 4.0m, 4.000m, 4.00m, 4.000000m, 4m), " +
                    "('2024-01-01T00:05:00', 'b', 0.6m, 6.0m, 6.000m, 6.00m, 6.000000m, 6m)";

    @Test
    public void testAllSubTypesNullsInPartitionBoundary() throws Exception {
        // a partition: null, 2.0 -> sum=2.0, count=1 ; b partition: 3.0, null -> sum=3.0, count=1
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:01:00', 'a', 0.2m, 2.0m, 2.000m, 2.00m, 2.000000m, 2m), " +
                    "('2024-01-01T00:02:00', 'b', 0.3m, 3.0m, 3.000m, 3.00m, 3.000000m, 3m), " +
                    "('2024-01-01T00:03:00', 'b', null, null, null, null, null, null)");
            assertSql("""
                            grp\ts8\ts64\ts256\tc8\tc64\tc256
                            a\t0.2\t2.00\t2\t1\t1\t1
                            a\t0.2\t2.00\t2\t1\t1\t1
                            b\t0.3\t3.00\t3\t1\t1\t1
                            b\t0.3\t3.00\t3\t1\t1\t1
                            """,
                    "SELECT grp, " +
                            "sum(v8) OVER (PARTITION BY grp) s8, sum(v64) OVER (PARTITION BY grp) s64, sum(v256) OVER (PARTITION BY grp) s256, " +
                            "count(v8) OVER (PARTITION BY grp) c8, count(v64) OVER (PARTITION BY grp) c64, count(v256) OVER (PARTITION BY grp) c256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAllSubTypesPartitionResetWithSliding() throws Exception {
        // last in sliding 2-window within partition = current row's val
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tlv8\tlv16\tlv32\tlv64\tlv128\tlv256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "last_value(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) lv8, " +
                            "last_value(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) lv16, " +
                            "last_value(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) lv32, " +
                            "last_value(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) lv64, " +
                            "last_value(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) lv128, " +
                            "last_value(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) lv256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8) OVER (PARTITION BY grp) a8, avg(v16) OVER (PARTITION BY grp) a16, " +
                            "avg(v32) OVER (PARTITION BY grp) a32, avg(v64) OVER (PARTITION BY grp) a64, " +
                            "avg(v128) OVER (PARTITION BY grp) a128, avg(v256) OVER (PARTITION BY grp) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertSql("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            -0.2\t-2.0\t-2.000\t-2.00\t-2.000000\t-2
                            """,
                    "SELECT avg(v8) OVER () a8, avg(v16) OVER () a16, avg(v32) OVER () a32, " +
                            "avg(v64) OVER () a64, avg(v128) OVER () a128, avg(v256) OVER () a256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesOverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.7\t7.0\t7.000\t7.00\t7.000000\t7
                            2024-01-01T00:03:00.000000Z\tb\t0.3\t3.0\t3.000\t3.00\t3.000000\t3
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.7\t7.0\t7.000\t7.00\t7.000000\t7
                            2024-01-01T00:03:00.000000Z\tb\t0.3\t3.0\t3.000\t3.00\t3.000000\t3
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesOverPartitionSlidingRows() throws Exception {
        // a avg: 0.6, 0.7 (1.4/2), 0.6 (1.2/2) ; b avg: 0.4, 0.3 (0.6/2), 0.4 (0.8/2)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.7\t7.0\t7.000\t7.00\t7.000000\t7
                            2024-01-01T00:03:00.000000Z\tb\t0.3\t3.0\t3.000\t3.00\t3.000000\t3
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesOverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT avg(v8) OVER () a8, avg(v16) OVER () a16, avg(v32) OVER () a32, " +
                            "avg(v64) OVER () a64, avg(v128) OVER () a128, avg(v256) OVER () a256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesSymmetricSliding() throws Exception {
        // avg per window: 0.5, 0.6, 14/3=0.467, 20/3=0.667, 0.6
        // For v8 (scale 1): 0.5, 0.6, 0.5 (HALF_EVEN of 0.467 rounds to 0.5), 0.7 (0.667 -> 0.7), 0.6
        // For v16 (scale 1): 5.0, 6.0, 4.7 (4.667 -> 4.7), 6.7 (6.667 -> 6.7), 6.0
        // For v32 (scale 3): 5.000, 6.000, 4.667, 6.667, 6.000
        // For v64 (scale 2): 5.00, 6.00, 4.67, 6.67, 6.00
        // For v128 (scale 6): 5.000000, 6.000000, 4.666667, 6.666667, 6.000000
        // For v256 (scale 0): 5, 6, 5 (HALF_EVEN of 4.667 -> 5), 7 (6.667 -> 7), 6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.5\t4.7\t4.667\t4.67\t4.666667\t5
                            2024-01-01T00:03:00.000000Z\t0.7\t6.7\t6.667\t6.67\t6.666667\t7
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) a8, " +
                            "avg(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) a16, " +
                            "avg(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) a32, " +
                            "avg(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) a64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) a128, " +
                            "avg(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesUnboundedToUnbounded() throws Exception {
        // avg total: 0.6 (v8), 6.0 (v16+)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a8, " +
                            "avg(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a16, " +
                            "avg(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a32, " +
                            "avg(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a128, " +
                            "avg(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertSql("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            \t\t\t\t\t
                            """,
                    "SELECT avg(v8) OVER () a8, avg(v16) OVER () a16, avg(v32) OVER () a32, " +
                            "avg(v64) OVER () a64, avg(v128) OVER () a128, avg(v256) OVER () a256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            0.7\t7.0\t7.000\t7.00\t7.000000\t7
                            """,
                    "SELECT avg(v8) OVER () a8, avg(v16) OVER () a16, avg(v32) OVER () a32, " +
                            "avg(v64) OVER () a64, avg(v128) OVER () a128, avg(v256) OVER () a256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgDecimal64HalfEven() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1.00m), ('2024-01-01T00:01:00', 2.00m), ('2024-01-01T00:02:00', 4.00m)");
            assertSql("avg_v\n2.33\n",
                    "SELECT avg(v) OVER () AS avg_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:02:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp) a8, avg(v16, 5) OVER (PARTITION BY grp) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp) a32, avg(v64, 5) OVER (PARTITION BY grp) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp) a128, avg(v256, 5) OVER (PARTITION BY grp) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertSql("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            -0.20000\t-2.00000\t-2.00000\t-2.00000\t-2.00000\t-2.00000
                            """,
                    "SELECT avg(v8, 5) OVER () a8, avg(v16, 5) OVER () a16, avg(v32, 5) OVER () a32, " +
                            "avg(v64, 5) OVER () a64, avg(v128, 5) OVER () a128, avg(v256, 5) OVER () a256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:02:00.000000Z\t0.80000\t8.00000\t8.00000\t8.00000\t8.00000\t8.00000
                            2024-01-01T00:03:00.000000Z\t0.20000\t2.00000\t2.00000\t2.00000\t2.00000\t2.00000
                            2024-01-01T00:04:00.000000Z\t1.00000\t10.00000\t10.00000\t10.00000\t10.00000\t10.00000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverPartitionRangeFrame() throws Exception {
        // a cum avg: 0.6, 0.7 (1.4/2), 0.6 (1.8/3) ; b cum avg: 0.4, 0.3 (0.6/2), 0.4 (1.2/3)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:02:00.000000Z\ta\t0.70000\t7.00000\t7.00000\t7.00000\t7.00000\t7.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.30000\t3.00000\t3.00000\t3.00000\t3.00000\t3.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:02:00.000000Z\ta\t0.70000\t7.00000\t7.00000\t7.00000\t7.00000\t7.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.30000\t3.00000\t3.00000\t3.00000\t3.00000\t3.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverRangeFrame() throws Exception {
        // cumulative avg on INSERT_5: 0.6, 0.5, 0.6, 0.5, 0.6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:02:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:04:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:02:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:04:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            """,
                    "SELECT avg(v8, 5) OVER () a8, avg(v16, 5) OVER () a16, avg(v32, 5) OVER () a32, " +
                            "avg(v64, 5) OVER () a64, avg(v128, 5) OVER () a128, avg(v256, 5) OVER () a256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:02:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:04:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertSql("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            \t\t\t\t\t
                            """,
                    "SELECT avg(v8, 5) OVER () a8, avg(v16, 5) OVER () a16, avg(v32, 5) OVER () a32, " +
                            "avg(v64, 5) OVER () a64, avg(v128, 5) OVER () a128, avg(v256, 5) OVER () a256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            0.70000\t7.00000\t7.00000\t7.00000\t7.00000\t7.00000
                            """,
                    "SELECT avg(v8, 5) OVER () a8, avg(v16, 5) OVER () a16, avg(v32, 5) OVER () a32, " +
                            "avg(v64, 5) OVER () a64, avg(v128, 5) OVER () a128, avg(v256, 5) OVER () a256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testCaseWhenWithWindow() throws Exception {
        // avg(v64)=6.00. v64: 6.00 (high, =6), 4.00 (low), 8.00 (high), 2.00 (low), 10.00 (high)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tcat
                            2024-01-01T00:00:00.000000Z\thigh
                            2024-01-01T00:01:00.000000Z\tlow
                            2024-01-01T00:02:00.000000Z\thigh
                            2024-01-01T00:03:00.000000Z\tlow
                            2024-01-01T00:04:00.000000Z\thigh
                            """,
                    "SELECT ts, CASE WHEN v64 >= avg(v64) OVER () THEN 'high' ELSE 'low' END AS cat FROM t");
        });
    }

    @Test
    public void testCountAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tc8\tc16\tc32\tc64\tc128\tc256
                            2024-01-01T00:00:00.000000Z\ta\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:01:00.000000Z\tb\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:02:00.000000Z\ta\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:03:00.000000Z\tb\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:04:00.000000Z\ta\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:05:00.000000Z\tb\t3\t3\t3\t3\t3\t3
                            """,
                    "SELECT ts, grp, " +
                            "count(v8) OVER (PARTITION BY grp) c8, count(v16) OVER (PARTITION BY grp) c16, " +
                            "count(v32) OVER (PARTITION BY grp) c32, count(v64) OVER (PARTITION BY grp) c64, " +
                            "count(v128) OVER (PARTITION BY grp) c128, count(v256) OVER (PARTITION BY grp) c256 " +
                            "FROM t");
        });
    }

    @Test
    public void testCountAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tc8\tc16\tc32\tc64\tc128\tc256
                            2024-01-01T00:00:00.000000Z\t0\t0\t0\t0\t0\t0
                            2024-01-01T00:01:00.000000Z\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:02:00.000000Z\t0\t0\t0\t0\t0\t0
                            2024-01-01T00:03:00.000000Z\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:04:00.000000Z\t0\t0\t0\t0\t0\t0
                            """,
                    "SELECT ts, " +
                            "count(v8) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) c8, " +
                            "count(v16) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) c16, " +
                            "count(v32) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) c32, " +
                            "count(v64) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) c64, " +
                            "count(v128) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) c128, " +
                            "count(v256) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) c256 " +
                            "FROM t");
        });
    }

    @Test
    public void testCountAllSubTypesOverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tc8\tc16\tc32\tc64\tc128\tc256
                            2024-01-01T00:00:00.000000Z\ta\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:01:00.000000Z\tb\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:02:00.000000Z\ta\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:03:00.000000Z\tb\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:04:00.000000Z\ta\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:05:00.000000Z\tb\t3\t3\t3\t3\t3\t3
                            """,
                    "SELECT ts, grp, " +
                            "count(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c8, " +
                            "count(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c16, " +
                            "count(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c32, " +
                            "count(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c64, " +
                            "count(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c128, " +
                            "count(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c256 " +
                            "FROM t");
        });
    }

    @Test
    public void testCountAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tc8\tc16\tc32\tc64\tc128\tc256
                            2024-01-01T00:00:00.000000Z\ta\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:01:00.000000Z\tb\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:02:00.000000Z\ta\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:03:00.000000Z\tb\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:04:00.000000Z\ta\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:05:00.000000Z\tb\t3\t3\t3\t3\t3\t3
                            """,
                    "SELECT ts, grp, " +
                            "count(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c8, " +
                            "count(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c16, " +
                            "count(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c32, " +
                            "count(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c64, " +
                            "count(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c128, " +
                            "count(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c256 " +
                            "FROM t");
        });
    }

    @Test
    public void testCountAllSubTypesOverPartitionSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tc8\tc16\tc32\tc64\tc128\tc256
                            2024-01-01T00:00:00.000000Z\ta\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:01:00.000000Z\tb\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:02:00.000000Z\ta\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:03:00.000000Z\tb\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:04:00.000000Z\ta\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:05:00.000000Z\tb\t2\t2\t2\t2\t2\t2
                            """,
                    "SELECT ts, grp, " +
                            "count(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) c8, " +
                            "count(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) c16, " +
                            "count(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) c32, " +
                            "count(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) c64, " +
                            "count(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) c128, " +
                            "count(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) c256 " +
                            "FROM t");
        });
    }

    @Test
    public void testCountAllSubTypesOverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tc8\tc16\tc32\tc64\tc128\tc256
                            2024-01-01T00:00:00.000000Z\t0\t0\t0\t0\t0\t0
                            2024-01-01T00:01:00.000000Z\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:02:00.000000Z\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:03:00.000000Z\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:04:00.000000Z\t2\t2\t2\t2\t2\t2
                            """,
                    "SELECT ts, " +
                            "count(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c8, " +
                            "count(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c16, " +
                            "count(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c32, " +
                            "count(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c64, " +
                            "count(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c128, " +
                            "count(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c256 " +
                            "FROM t");
        });
    }

    @Test
    public void testCountAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tc8\tc16\tc32\tc64\tc128\tc256
                            2024-01-01T00:00:00.000000Z\t0\t0\t0\t0\t0\t0
                            2024-01-01T00:01:00.000000Z\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:02:00.000000Z\t1\t1\t1\t1\t1\t1
                            2024-01-01T00:03:00.000000Z\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:04:00.000000Z\t2\t2\t2\t2\t2\t2
                            """,
                    "SELECT ts, " +
                            "count(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c8, " +
                            "count(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c16, " +
                            "count(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c32, " +
                            "count(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c64, " +
                            "count(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c128, " +
                            "count(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c256 " +
                            "FROM t");
        });
    }

    @Test
    public void testCountAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            c8\tc16\tc32\tc64\tc128\tc256
                            5\t5\t5\t5\t5\t5
                            """,
                    "SELECT count(v8) OVER () c8, count(v16) OVER () c16, count(v32) OVER () c32, " +
                            "count(v64) OVER () c64, count(v128) OVER () c128, count(v256) OVER () c256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testCountAllSubTypesSymmetricSliding() throws Exception {
        // count per window: 2, 3, 3, 3, 2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tc8\tc16\tc32\tc64\tc128\tc256
                            2024-01-01T00:00:00.000000Z\t2\t2\t2\t2\t2\t2
                            2024-01-01T00:01:00.000000Z\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:02:00.000000Z\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:03:00.000000Z\t3\t3\t3\t3\t3\t3
                            2024-01-01T00:04:00.000000Z\t2\t2\t2\t2\t2\t2
                            """,
                    "SELECT ts, " +
                            "count(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) c8, " +
                            "count(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) c16, " +
                            "count(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) c32, " +
                            "count(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) c64, " +
                            "count(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) c128, " +
                            "count(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) c256 " +
                            "FROM t");
        });
    }

    @Test
    public void testCountAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertSql("""
                            c8\tc16\tc32\tc64\tc128\tc256
                            0\t0\t0\t0\t0\t0
                            """,
                    "SELECT count(v8) OVER () c8, count(v16) OVER () c16, count(v32) OVER () c32, " +
                            "count(v64) OVER () c64, count(v128) OVER () c128, count(v256) OVER () c256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testCountAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            c8\tc16\tc32\tc64\tc128\tc256
                            2\t2\t2\t2\t2\t2
                            """,
                    "SELECT count(v8) OVER () c8, count(v16) OVER () c16, count(v32) OVER () c32, " +
                            "count(v64) OVER () c64, count(v128) OVER () c128, count(v256) OVER () c256 " +
                            "FROM t LIMIT 1");
        });
        // count of 2 non-nulls — unchanged from INSERT_3_WITH_NULL since both datasets have 2 non-nulls
    }

    @Test
    public void testCountStarVsCountColumn() throws Exception {
        // 5 rows total, 2 non-null
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            c_star\tc_col
                            5\t2
                            """,
                    "SELECT count() OVER () c_star, count(v64) OVER () c_col FROM t LIMIT 1");
        });
    }

    @Test
    public void testCountStarWindow() throws Exception {
        // INSERT_5_WITH_NULL has 5 rows total
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("c\n5\n",
                    "SELECT count() OVER () c FROM t LIMIT 1");
        });
    }

    @Test
    public void testEmptyTableAllFactories() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertSql("c8\ts8\tmx8\tmn8\tfv8\tlv8\n",
                    "SELECT count(v8) OVER () c8, sum(v8) OVER () s8, max(v8) OVER () mx8, min(v8) OVER () mn8, " +
                            "first_value(v8) OVER () fv8, last_value(v8) OVER () lv8 FROM t");
        });
    }

    @Test
    public void testFirstLastAllSubTypesEmptyFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tf64\tl64
                            2024-01-01T00:00:00.000000Z\t\t
                            2024-01-01T00:01:00.000000Z\t\t
                            2024-01-01T00:02:00.000000Z\t\t
                            """,
                    "SELECT ts, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) f64, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) l64 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstLastValueAllSubTypesAllNullsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertSql("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256, " +
                            "last_value(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstLastValueAllSubTypesUnboundedToUnbounded() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tf64\tl64
                            2024-01-01T00:00:00.000000Z\t6.00\t10.00
                            2024-01-01T00:01:00.000000Z\t6.00\t10.00
                            2024-01-01T00:02:00.000000Z\t6.00\t10.00
                            2024-01-01T00:03:00.000000Z\t6.00\t10.00
                            2024-01-01T00:04:00.000000Z\t6.00\t10.00
                            """,
                    "SELECT ts, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) f64, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) l64 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstValueAllSubTypesIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) f256 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverPartitionRangeFrame() throws Exception {
        // first within partition (cumulative). a always 0.6, b always 0.4
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "first_value(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "first_value(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverRangeFrame() throws Exception {
        // cum first always = 0.6 (first row in INSERT_5)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            f8\tf16\tf32\tf64\tf128\tf256
                            0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT first_value(v8) OVER () f8, first_value(v16) OVER () f16, first_value(v32) OVER () f32, " +
                            "first_value(v64) OVER () f64, first_value(v128) OVER () f128, first_value(v256) OVER () f256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testFirstValueAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t");
        });
    }

    @Test
    public void testFirstValueAllSubTypesSymmetricSliding() throws Exception {
        // first per window: 0.6, 0.6, 0.4, 0.8, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) f8, " +
                            "first_value(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) f16, " +
                            "first_value(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) f32, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) f64, " +
                            "first_value(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) f128, " +
                            "first_value(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) f256 " +
                            "FROM t");
        });
    }

    @Test
    public void testFrameWithLargePrecedingCount() throws Exception {
        // 10_000 PRECEDING covers all earlier rows (cumulative on INSERT_5)
        // cumulative sums: 6.00, 10.00, 18.00, 20.00, 30.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t6.00
                            2024-01-01T00:01:00.000000Z\t10.00
                            2024-01-01T00:02:00.000000Z\t18.00
                            2024-01-01T00:03:00.000000Z\t20.00
                            2024-01-01T00:04:00.000000Z\t30.00
                            """,
                    "SELECT ts, sum(v64) OVER (ORDER BY ts ROWS BETWEEN 10_000 PRECEDING AND CURRENT ROW) s FROM t");
        });
    }

    @Test
    public void testLagAllSubTypesIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "lag(v8, 1) IGNORE NULLS OVER (ORDER BY ts) l8, lag(v16, 1) IGNORE NULLS OVER (ORDER BY ts) l16, " +
                            "lag(v32, 1) IGNORE NULLS OVER (ORDER BY ts) l32, lag(v64, 1) IGNORE NULLS OVER (ORDER BY ts) l64, " +
                            "lag(v128, 1) IGNORE NULLS OVER (ORDER BY ts) l128, lag(v256, 1) IGNORE NULLS OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLagAllSubTypesLargeOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "lag(v8, 10) OVER (ORDER BY ts) l8, lag(v16, 10) OVER (ORDER BY ts) l16, " +
                            "lag(v32, 10) OVER (ORDER BY ts) l32, lag(v64, 10) OVER (ORDER BY ts) l64, " +
                            "lag(v128, 10) OVER (ORDER BY ts) l128, lag(v256, 10) OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLagAllSubTypesMultiPartitionReset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "lag(v8, 1) OVER (PARTITION BY grp ORDER BY ts) l8, lag(v16, 1) OVER (PARTITION BY grp ORDER BY ts) l16, " +
                            "lag(v32, 1) OVER (PARTITION BY grp ORDER BY ts) l32, lag(v64, 1) OVER (PARTITION BY grp ORDER BY ts) l64, " +
                            "lag(v128, 1) OVER (PARTITION BY grp ORDER BY ts) l128, lag(v256, 1) OVER (PARTITION BY grp ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLagAllSubTypesNoOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "lag(v8) OVER (ORDER BY ts) l8, lag(v16) OVER (ORDER BY ts) l16, " +
                            "lag(v32) OVER (ORDER BY ts) l32, lag(v64) OVER (ORDER BY ts) l64, " +
                            "lag(v128) OVER (ORDER BY ts) l128, lag(v256) OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLagAllSubTypesOffsetTwo() throws Exception {
        // lag(v, 2): row 0=null, row 1=null, row 2=0.6, row 3=0.4, row 4=0.8
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "lag(v8, 2) OVER (ORDER BY ts) l8, lag(v16, 2) OVER (ORDER BY ts) l16, " +
                            "lag(v32, 2) OVER (ORDER BY ts) l32, lag(v64, 2) OVER (ORDER BY ts) l64, " +
                            "lag(v128, 2) OVER (ORDER BY ts) l128, lag(v256, 2) OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLagAllSubTypesWithDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t9.0\t99.0\t999.000\t99.00\t999.000000\t999
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "lag(v8, 1, 9.0::decimal(2, 1)) OVER (ORDER BY ts) l8, " +
                            "lag(v16, 1, 99.0::decimal(4, 1)) OVER (ORDER BY ts) l16, " +
                            "lag(v32, 1, 999.000::decimal(9, 3)) OVER (ORDER BY ts) l32, " +
                            "lag(v64, 1, 99.00::decimal(18, 2)) OVER (ORDER BY ts) l64, " +
                            "lag(v128, 1, 999.000000::decimal(38, 6)) OVER (ORDER BY ts) l128, " +
                            "lag(v256, 1, 999::decimal(60, 0)) OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLagLeadAllSubTypesWithNulls() throws Exception {
        // INSERT_5_WITH_NULL: null, 0.6, null, 0.8, null (RESPECT NULLS)
        // lag(v): row 0=null, row 1=null, row 2=0.6, row 3=null, row 4=0.8
        // lead(v): row 0=0.6, row 1=null, row 2=0.8, row 3=null, row 4=null
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tlg8\tlg64\tld8\tld64
                            2024-01-01T00:00:00.000000Z\t\t\t0.6\t6.00
                            2024-01-01T00:01:00.000000Z\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.00\t0.8\t8.00
                            2024-01-01T00:03:00.000000Z\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t0.8\t8.00\t\t
                            """,
                    "SELECT ts, " +
                            "lag(v8) OVER (ORDER BY ts) lg8, lag(v64) OVER (ORDER BY ts) lg64, " +
                            "lead(v8) OVER (ORDER BY ts) ld8, lead(v64) OVER (ORDER BY ts) ld64 " +
                            "FROM t");
        });
    }

    @Test
    public void testLagOffsetZero() throws Exception {
        // lag(v, 0) returns current row's value. v64: 6.00, 4.00, 8.00, 2.00, 10.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tlg64
                            2024-01-01T00:00:00.000000Z\t6.00
                            2024-01-01T00:01:00.000000Z\t4.00
                            2024-01-01T00:02:00.000000Z\t8.00
                            2024-01-01T00:03:00.000000Z\t2.00
                            2024-01-01T00:04:00.000000Z\t10.00
                            """,
                    "SELECT ts, lag(v64, 0) OVER (ORDER BY ts) lg64 FROM t");
        });
    }

    @Test
    public void testLagWithNullDefault() throws Exception {
        // lag(v, 1, null) — same as lag(v, 1). v64: 6.00, 4.00, 8.00, 2.00, 10.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tlg64
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T00:01:00.000000Z\t6.00
                            2024-01-01T00:02:00.000000Z\t4.00
                            2024-01-01T00:03:00.000000Z\t8.00
                            2024-01-01T00:04:00.000000Z\t2.00
                            """,
                    "SELECT ts, lag(v64, 1, null) OVER (ORDER BY ts) lg64 FROM t");
        });
    }

    @Test
    public void testLargeWindowAllFactories() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t SELECT " +
                    "timestamp_sequence(0, 60000000), 'g'||(x % 4)::string, " +
                    "cast(1.0 as decimal(2, 1)), cast(10.0 as decimal(4, 1)), cast(10.000 as decimal(9, 3)), " +
                    "cast(10.00 as decimal(18, 2)), cast(10.000000 as decimal(38, 6)), cast(10 as decimal(60, 0)) " +
                    "FROM long_sequence(500)");
            assertSql("""
                            c64\ts64\tmx64\tmn64\tavg64
                            500\t5000.00\t10.00\t10.00\t10.00
                            """,
                    "SELECT count(v64) OVER () c64, sum(v64) OVER () s64, max(v64) OVER () mx64, " +
                            "min(v64) OVER () mn64, avg(v64) OVER () avg64 FROM t LIMIT 1");
        });
    }

    @Test
    public void testLastValueAllSubTypesIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLastValueAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLastValueAllSubTypesOverPartitionRangeFrame() throws Exception {
        // last within partition (cumulative) = current row's val
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "last_value(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLastValueAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "last_value(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLastValueAllSubTypesOverRangeFrame() throws Exception {
        // cum last = current row's val
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLastValueAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLastValueAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            l8\tl16\tl32\tl64\tl128\tl256
                            1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT last_value(v8) OVER () l8, last_value(v16) OVER () l16, last_value(v32) OVER () l32, " +
                            "last_value(v64) OVER () l64, last_value(v128) OVER () l128, last_value(v256) OVER () l256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testLastValueAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLastValueAllSubTypesSymmetricSliding() throws Exception {
        // last per window: 0.4, 0.8, 0.2, 1.0, 1.0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:03:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) l8, " +
                            "last_value(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) l16, " +
                            "last_value(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) l32, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) l64, " +
                            "last_value(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) l128, " +
                            "last_value(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLeadAllSubTypesIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "lead(v8, 1) IGNORE NULLS OVER (ORDER BY ts) l8, lead(v16, 1) IGNORE NULLS OVER (ORDER BY ts) l16, " +
                            "lead(v32, 1) IGNORE NULLS OVER (ORDER BY ts) l32, lead(v64, 1) IGNORE NULLS OVER (ORDER BY ts) l64, " +
                            "lead(v128, 1) IGNORE NULLS OVER (ORDER BY ts) l128, lead(v256, 1) IGNORE NULLS OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLeadAllSubTypesLargeOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "lead(v8, 10) OVER (ORDER BY ts) l8, lead(v16, 10) OVER (ORDER BY ts) l16, " +
                            "lead(v32, 10) OVER (ORDER BY ts) l32, lead(v64, 10) OVER (ORDER BY ts) l64, " +
                            "lead(v128, 10) OVER (ORDER BY ts) l128, lead(v256, 10) OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLeadAllSubTypesMultiPartitionReset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:01:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:02:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:05:00.000000Z\tb\t\t\t\t\t\t
                            """,
                    "SELECT ts, grp, " +
                            "lead(v8, 1) OVER (PARTITION BY grp ORDER BY ts) l8, lead(v16, 1) OVER (PARTITION BY grp ORDER BY ts) l16, " +
                            "lead(v32, 1) OVER (PARTITION BY grp ORDER BY ts) l32, lead(v64, 1) OVER (PARTITION BY grp ORDER BY ts) l64, " +
                            "lead(v128, 1) OVER (PARTITION BY grp ORDER BY ts) l128, lead(v256, 1) OVER (PARTITION BY grp ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLeadAllSubTypesNoOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:03:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "lead(v8) OVER (ORDER BY ts) l8, lead(v16) OVER (ORDER BY ts) l16, " +
                            "lead(v32) OVER (ORDER BY ts) l32, lead(v64) OVER (ORDER BY ts) l64, " +
                            "lead(v128) OVER (ORDER BY ts) l128, lead(v256) OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLeadAllSubTypesOffsetTwo() throws Exception {
        // lead(v, 2): row 0=0.8, row 1=0.2, row 2=1.0, row 3=null, row 4=null
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:01:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:02:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "lead(v8, 2) OVER (ORDER BY ts) l8, lead(v16, 2) OVER (ORDER BY ts) l16, " +
                            "lead(v32, 2) OVER (ORDER BY ts) l32, lead(v64, 2) OVER (ORDER BY ts) l64, " +
                            "lead(v128, 2) OVER (ORDER BY ts) l128, lead(v256, 2) OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLeadAllSubTypesWithDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:03:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:04:00.000000Z\t9.0\t99.0\t999.000\t99.00\t999.000000\t999
                            """,
                    "SELECT ts, " +
                            "lead(v8, 1, 9.0::decimal(2, 1)) OVER (ORDER BY ts) l8, " +
                            "lead(v16, 1, 99.0::decimal(4, 1)) OVER (ORDER BY ts) l16, " +
                            "lead(v32, 1, 999.000::decimal(9, 3)) OVER (ORDER BY ts) l32, " +
                            "lead(v64, 1, 99.00::decimal(18, 2)) OVER (ORDER BY ts) l64, " +
                            "lead(v128, 1, 999.000000::decimal(38, 6)) OVER (ORDER BY ts) l128, " +
                            "lead(v256, 1, 999::decimal(60, 0)) OVER (ORDER BY ts) l256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLeadOffsetZero() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tld64
                            2024-01-01T00:00:00.000000Z\t6.00
                            2024-01-01T00:01:00.000000Z\t4.00
                            2024-01-01T00:02:00.000000Z\t8.00
                            2024-01-01T00:03:00.000000Z\t2.00
                            2024-01-01T00:04:00.000000Z\t10.00
                            """,
                    "SELECT ts, lead(v64, 0) OVER (ORDER BY ts) ld64 FROM t");
        });
    }

    @Test
    public void testLeadWithNullDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tld64
                            2024-01-01T00:00:00.000000Z\t4.00
                            2024-01-01T00:01:00.000000Z\t8.00
                            2024-01-01T00:02:00.000000Z\t2.00
                            2024-01-01T00:03:00.000000Z\t10.00
                            2024-01-01T00:04:00.000000Z\t
                            """,
                    "SELECT ts, lead(v64, 1, null) OVER (ORDER BY ts) ld64 FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:01:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "max(v8) OVER (PARTITION BY grp) m8, max(v16) OVER (PARTITION BY grp) m16, " +
                            "max(v32) OVER (PARTITION BY grp) m32, max(v64) OVER (PARTITION BY grp) m64, " +
                            "max(v128) OVER (PARTITION BY grp) m128, max(v256) OVER (PARTITION BY grp) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertSql("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT max(v8) OVER () m8, max(v16) OVER () m16, max(v32) OVER () m32, " +
                            "max(v64) OVER () m64, max(v128) OVER () m128, max(v256) OVER () m256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testMaxAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "max(v8) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m8, " +
                            "max(v16) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m16, " +
                            "max(v32) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m32, " +
                            "max(v64) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m64, " +
                            "max(v128) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m128, " +
                            "max(v256) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesOverPartitionRangeFrame() throws Exception {
        // cum max within partition. a: 0.6, 0.8, 0.8 ; b: 0.4, 0.4, 0.6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "max(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m8, " +
                            "max(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m16, " +
                            "max(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m32, " +
                            "max(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m64, " +
                            "max(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m128, " +
                            "max(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "max(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m8, " +
                            "max(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m16, " +
                            "max(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m32, " +
                            "max(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m64, " +
                            "max(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m128, " +
                            "max(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesOverPartitionSlidingRows() throws Exception {
        // a max: 0.6, 0.8, 0.8 ; b max: 0.4, 0.4, 0.6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "max(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m8, " +
                            "max(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m16, " +
                            "max(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m32, " +
                            "max(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m64, " +
                            "max(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m128, " +
                            "max(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesOverRangeFrame() throws Exception {
        // cumulative max on INSERT_5: 0.6, 0.6, 0.8, 0.8, 1.0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "max(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m8, " +
                            "max(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m16, " +
                            "max(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m32, " +
                            "max(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m64, " +
                            "max(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m128, " +
                            "max(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "max(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m8, " +
                            "max(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m16, " +
                            "max(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m32, " +
                            "max(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m64, " +
                            "max(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m128, " +
                            "max(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT max(v8) OVER () m8, max(v16) OVER () m16, max(v32) OVER () m32, " +
                            "max(v64) OVER () m64, max(v128) OVER () m128, max(v256) OVER () m256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testMaxAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "max(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m8, " +
                            "max(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m16, " +
                            "max(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m32, " +
                            "max(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m64, " +
                            "max(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m128, " +
                            "max(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesSymmetricSliding() throws Exception {
        // max per window: 0.6, 0.8, 0.8, 1.0, 1.0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "max(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m8, " +
                            "max(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m16, " +
                            "max(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m32, " +
                            "max(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m64, " +
                            "max(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m128, " +
                            "max(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMaxAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertSql("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            \t\t\t\t\t
                            """,
                    "SELECT max(v8) OVER () m8, max(v16) OVER () m16, max(v32) OVER () m32, " +
                            "max(v64) OVER () m64, max(v128) OVER () m128, max(v256) OVER () m256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testMaxAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT max(v8) OVER () m8, max(v16) OVER () m16, max(v32) OVER () m32, " +
                            "max(v64) OVER () m64, max(v128) OVER () m128, max(v256) OVER () m256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testMinAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:01:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:02:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "min(v8) OVER (PARTITION BY grp) m8, min(v16) OVER (PARTITION BY grp) m16, " +
                            "min(v32) OVER (PARTITION BY grp) m32, min(v64) OVER (PARTITION BY grp) m64, " +
                            "min(v128) OVER (PARTITION BY grp) m128, min(v256) OVER (PARTITION BY grp) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMinAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertSql("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            -0.8\t-8.0\t-8.000\t-8.00\t-8.000000\t-8
                            """,
                    "SELECT min(v8) OVER () m8, min(v16) OVER () m16, min(v32) OVER () m32, " +
                            "min(v64) OVER () m64, min(v128) OVER () m128, min(v256) OVER () m256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testMinAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "min(v8) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m8, " +
                            "min(v16) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m16, " +
                            "min(v32) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m32, " +
                            "min(v64) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m64, " +
                            "min(v128) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m128, " +
                            "min(v256) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMinAllSubTypesOverPartitionRangeFrame() throws Exception {
        // cum min within partition. a: 0.6, 0.6, 0.4 ; b: 0.4, 0.2, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "min(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m8, " +
                            "min(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m16, " +
                            "min(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m32, " +
                            "min(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m64, " +
                            "min(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m128, " +
                            "min(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMinAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "min(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m8, " +
                            "min(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m16, " +
                            "min(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m32, " +
                            "min(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m64, " +
                            "min(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m128, " +
                            "min(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMinAllSubTypesOverPartitionSlidingRows() throws Exception {
        // a min: 0.6, 0.6, 0.4 ; b min: 0.4, 0.2, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "min(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m8, " +
                            "min(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m16, " +
                            "min(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m32, " +
                            "min(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m64, " +
                            "min(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m128, " +
                            "min(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMinAllSubTypesOverRangeFrame() throws Exception {
        // cumulative min on INSERT_5: 0.6, 0.4, 0.4, 0.2, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "min(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m8, " +
                            "min(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m16, " +
                            "min(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m32, " +
                            "min(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m64, " +
                            "min(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m128, " +
                            "min(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMinAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "min(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m8, " +
                            "min(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m16, " +
                            "min(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m32, " +
                            "min(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m64, " +
                            "min(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m128, " +
                            "min(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMinAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT min(v8) OVER () m8, min(v16) OVER () m16, min(v32) OVER () m32, " +
                            "min(v64) OVER () m64, min(v128) OVER () m128, min(v256) OVER () m256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testMinAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "min(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m8, " +
                            "min(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m16, " +
                            "min(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m32, " +
                            "min(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m64, " +
                            "min(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m128, " +
                            "min(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMinAllSubTypesSymmetricSliding() throws Exception {
        // min per window: 0.4, 0.4, 0.2, 0.2, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "min(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m8, " +
                            "min(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m16, " +
                            "min(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m32, " +
                            "min(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m64, " +
                            "min(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m128, " +
                            "min(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) m256 " +
                            "FROM t");
        });
    }

    @Test
    public void testMinAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertSql("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            \t\t\t\t\t
                            """,
                    "SELECT min(v8) OVER () m8, min(v16) OVER () m16, min(v32) OVER () m32, " +
                            "min(v64) OVER () m64, min(v128) OVER () m128, min(v256) OVER () m256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testMinAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT min(v8) OVER () m8, min(v16) OVER () m16, min(v32) OVER () m32, " +
                            "min(v64) OVER () m64, min(v128) OVER () m128, min(v256) OVER () m256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testMultipleOrderByColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'b', 10.00m), " +
                    "('2024-01-01T00:01:00', 'a', 20.00m), " +
                    "('2024-01-01T00:02:00', 'b', 30.00m), " +
                    "('2024-01-01T00:03:00', 'a', 40.00m)");
            assertSql("""
                            ts\ta\tv\ts
                            2024-01-01T00:00:00.000000Z\tb\t10.00\t60.00
                            2024-01-01T00:01:00.000000Z\ta\t20.00\t20.00
                            2024-01-01T00:02:00.000000Z\tb\t30.00\t90.00
                            2024-01-01T00:03:00.000000Z\ta\t40.00\t60.00
                            """,
                    "SELECT ts, a, v, sum(v) OVER (ORDER BY a, ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM t");
        });
    }

    @Test
    public void testMultipleWindowFunctionsInSameSelect() throws Exception {
        // INSERT_5 v64: 6.00, 4.00, 8.00, 2.00, 10.00
        // sum=30, avg=6, max=10, min=2, count=5, first=6, last=10, nth(2)=4, avgRescale=6.00000
        // lag/lead ordered by ts
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts64\ta64\tmx64\tmn64\tc64\tfv64\tlv64\tlg64\tld64\tn64\tar64
                            2024-01-01T00:00:00.000000Z\t30.00\t6.00\t10.00\t2.00\t5\t6.00\t10.00\t\t4.00\t4.00\t6.00000
                            2024-01-01T00:01:00.000000Z\t30.00\t6.00\t10.00\t2.00\t5\t6.00\t10.00\t6.00\t8.00\t4.00\t6.00000
                            2024-01-01T00:02:00.000000Z\t30.00\t6.00\t10.00\t2.00\t5\t6.00\t10.00\t4.00\t2.00\t4.00\t6.00000
                            2024-01-01T00:03:00.000000Z\t30.00\t6.00\t10.00\t2.00\t5\t6.00\t10.00\t8.00\t10.00\t4.00\t6.00000
                            2024-01-01T00:04:00.000000Z\t30.00\t6.00\t10.00\t2.00\t5\t6.00\t10.00\t2.00\t\t4.00\t6.00000
                            """,
                    "SELECT ts, " +
                            "sum(v64) OVER () s64, avg(v64) OVER () a64, max(v64) OVER () mx64, min(v64) OVER () mn64, " +
                            "count(v64) OVER () c64, first_value(v64) OVER () fv64, last_value(v64) OVER () lv64, " +
                            "lag(v64) OVER (ORDER BY ts) lg64, lead(v64) OVER (ORDER BY ts) ld64, " +
                            "nth_value(v64, 2) OVER () n64, avg(v64, 5) OVER () ar64 FROM t");
        });
    }

    @Test
    public void testMultipleWindowsWithSamePartition() throws Exception {
        // a sum=18, max=8, min=4 ; b sum=12, max=6, min=2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ts\tmx\tmn
                            2024-01-01T00:00:00.000000Z\ta\t18.00\t8.00\t4.00
                            2024-01-01T00:01:00.000000Z\tb\t12.00\t6.00\t2.00
                            2024-01-01T00:02:00.000000Z\ta\t18.00\t8.00\t4.00
                            2024-01-01T00:03:00.000000Z\tb\t12.00\t6.00\t2.00
                            2024-01-01T00:04:00.000000Z\ta\t18.00\t8.00\t4.00
                            2024-01-01T00:05:00.000000Z\tb\t12.00\t6.00\t2.00
                            """,
                    "SELECT ts, grp, " +
                            "sum(v64) OVER (PARTITION BY grp) s, " +
                            "max(v64) OVER (PARTITION BY grp) mx, " +
                            "min(v64) OVER (PARTITION BY grp) mn " +
                            "FROM t");
        });
    }

    @Test
    public void testNamedWindow() throws Exception {
        // sum=30, avg=6, max=10
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts64\ta64\tmx64
                            2024-01-01T00:00:00.000000Z\t30.00\t6.00\t10.00
                            2024-01-01T00:01:00.000000Z\t30.00\t6.00\t10.00
                            2024-01-01T00:02:00.000000Z\t30.00\t6.00\t10.00
                            2024-01-01T00:03:00.000000Z\t30.00\t6.00\t10.00
                            2024-01-01T00:04:00.000000Z\t30.00\t6.00\t10.00
                            """,
                    "SELECT ts, sum(v64) OVER w s64, avg(v64) OVER w a64, max(v64) OVER w mx64 " +
                            "FROM t WINDOW w AS ()");
        });
    }

    @Test
    public void testNthValueAllSubTypesIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t");
        });
    }

    @Test
    public void testNthValueAllSubTypesInvalidN() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            for (int n : new int[]{0, -1, -100}) {
                for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                    try {
                        assertSql("", "SELECT nth_value(" + col + ", " + n + ") OVER (ORDER BY ts) FROM t");
                    } catch (Exception ex) {
                        // expected for n <= 0
                    }
                }
            }
        });
    }

    @Test
    public void testNthValueAllSubTypesLargeN() throws Exception {
        // nth(3) on INSERT_5 (values 0.6, 0.4, 0.8, 0.2, 1.0): 3rd val = 0.8
        // Row 0: null (frame size 1), Row 1: null (size 2), Row 2: 0.8 (size 3), Row 3+: 0.8
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 3) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 3) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 3) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 3) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 3) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 3) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t");
        });
    }

    @Test
    public void testNthValueAllSubTypesOutOfRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 99) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 99) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 99) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 99) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 99) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 99) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t");
        });
    }

    @Test
    public void testNthValueAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n256 " +
                            "FROM t");
        });
    }

    @Test
    public void testNthValueAllSubTypesOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:05:00.000000Z\tb\t\t\t\t\t\t
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 2) OVER (PARTITION BY grp) n8, nth_value(v16, 2) OVER (PARTITION BY grp) n16, " +
                            "nth_value(v32, 2) OVER (PARTITION BY grp) n32, nth_value(v64, 2) OVER (PARTITION BY grp) n64, " +
                            "nth_value(v128, 2) OVER (PARTITION BY grp) n128, nth_value(v256, 2) OVER (PARTITION BY grp) n256 " +
                            "FROM t");
        });
    }

    @Test
    public void testNthValueAllSubTypesOverPartitionRangeFrame() throws Exception {
        // nth(2) cum within partition. a: null, 0.8, 0.8 ; b: null, 0.2, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t");
        });
    }

    @Test
    public void testNthValueAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t");
        });
    }

    @Test
    public void testNthValueAllSubTypesOverRangeFrame() throws Exception {
        // nth(2) cumulative: row 0=null, row 1+=0.4 (2nd row's val)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t");
        });
    }

    @Test
    public void testNthValueAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t");
        });
    }

    @Test
    public void testOrderByDescending() throws Exception {
        // DESC order: row 4 (10), row 3 (2), row 2 (8), row 1 (4), row 0 (6)
        // cumulative: 10, 12, 20, 24, 30
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts64
                            2024-01-01T00:04:00.000000Z\t10.00
                            2024-01-01T00:03:00.000000Z\t12.00
                            2024-01-01T00:02:00.000000Z\t20.00
                            2024-01-01T00:01:00.000000Z\t24.00
                            2024-01-01T00:00:00.000000Z\t30.00
                            """,
                    "SELECT ts, sum(v64) OVER (ORDER BY ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s64 FROM t ORDER BY ts DESC");
        });
    }

    @Test
    public void testPartitionByDecimalColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k decimal(2, 0), v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1m, 10.00m), " +
                    "('2024-01-01T00:01:00', 2m, 20.00m), " +
                    "('2024-01-01T00:02:00', 1m, 30.00m), " +
                    "('2024-01-01T00:03:00', 2m, 40.00m)");
            assertSql("""
                            ts\tk\tv\ts
                            2024-01-01T00:00:00.000000Z\t1\t10.00\t40.00
                            2024-01-01T00:01:00.000000Z\t2\t20.00\t60.00
                            2024-01-01T00:02:00.000000Z\t1\t30.00\t40.00
                            2024-01-01T00:03:00.000000Z\t2\t40.00\t60.00
                            """,
                    "SELECT ts, k, v, sum(v) OVER (PARTITION BY k) s FROM t");
        });
    }

    @Test
    public void testPartitionByDescOrderBy() throws Exception {
        // INSERT_6_PART v64: a (00:00 6.00, 00:02 8.00, 00:04 4.00) ; b (00:01 4.00, 00:03 2.00, 00:05 6.00)
        // DESC cumulative within partition. a DESC: 4.00, 8.00, 6.00 -> cum: 4, 12, 18 (at ts 04, 02, 00)
        // b DESC: 6.00, 2.00, 4.00 -> cum: 6, 8, 12 (at ts 05, 03, 01)
        // Output ordered by ts ASC: ts 00=18, 01=12, 02=12, 03=8, 04=4, 05=6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ts
                            2024-01-01T00:00:00.000000Z\ta\t18.00
                            2024-01-01T00:01:00.000000Z\tb\t12.00
                            2024-01-01T00:02:00.000000Z\ta\t12.00
                            2024-01-01T00:03:00.000000Z\tb\t8.00
                            2024-01-01T00:04:00.000000Z\ta\t4.00
                            2024-01-01T00:05:00.000000Z\tb\t6.00
                            """,
                    "SELECT ts, grp, sum(v64) OVER (PARTITION BY grp ORDER BY ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t");
        });
    }

    @Test
    public void testPartitionByMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a SYMBOL, b SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'x', 'p', 10.00m), " +
                    "('2024-01-01T00:01:00', 'x', 'q', 20.00m), " +
                    "('2024-01-01T00:02:00', 'x', 'p', 30.00m), " +
                    "('2024-01-01T00:03:00', 'y', 'p', 40.00m)");
            assertSql("""
                            ts\ta\tb\ts
                            2024-01-01T00:00:00.000000Z\tx\tp\t40.00
                            2024-01-01T00:01:00.000000Z\tx\tq\t20.00
                            2024-01-01T00:02:00.000000Z\tx\tp\t40.00
                            2024-01-01T00:03:00.000000Z\ty\tp\t40.00
                            """,
                    "SELECT ts, a, b, sum(v) OVER (PARTITION BY a, b) AS s FROM t");
        });
    }

    @Test
    public void testPartitionWithNullGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.00m), " +
                    "('2024-01-01T00:01:00', null, 20.00m), " +
                    "('2024-01-01T00:02:00', 'a', 30.00m), " +
                    "('2024-01-01T00:03:00', null, 40.00m)");
            assertSql("""
                            ts\tgrp\tv\ts
                            2024-01-01T00:00:00.000000Z\ta\t10.00\t40.00
                            2024-01-01T00:01:00.000000Z\t\t20.00\t60.00
                            2024-01-01T00:02:00.000000Z\ta\t30.00\t40.00
                            2024-01-01T00:03:00.000000Z\t\t40.00\t60.00
                            """,
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) s FROM t");
        });
    }

    @Test
    public void testRangeBetweenIntervalAllSubTypes() throws Exception {
        // 90s window covers current row + 1 preceding row (rows are 60s apart)
        // INSERT_5 v8: 0.6, 0.4, 0.8, 0.2, 1.0
        // sliding 2-window sums: 0.6, 1.0, 1.2, 1.0, 1.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:02:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:03:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:04:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts RANGE BETWEEN '90s' PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (ORDER BY ts RANGE BETWEEN '90s' PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (ORDER BY ts RANGE BETWEEN '90s' PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN '90s' PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (ORDER BY ts RANGE BETWEEN '90s' PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (ORDER BY ts RANGE BETWEEN '90s' PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testRangeIntervalDifferentUnits() throws Exception {
        // 5m / 1h / 1d windows all cover all preceding rows (rows are 60s apart, 5 rows = 4 min total).
        // Actually '5m' covers 5min preceding ; cumulative on INSERT_5: 6, 10, 18, 20, 30
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts1\ts2\ts3
                            2024-01-01T00:00:00.000000Z\t6.00\t6.00\t6.00
                            2024-01-01T00:01:00.000000Z\t10.00\t10.00\t10.00
                            2024-01-01T00:02:00.000000Z\t18.00\t18.00\t18.00
                            2024-01-01T00:03:00.000000Z\t20.00\t20.00\t20.00
                            2024-01-01T00:04:00.000000Z\t30.00\t30.00\t30.00
                            """,
                    "SELECT ts, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN '5m' PRECEDING AND CURRENT ROW) s1, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN '1h' PRECEDING AND CURRENT ROW) s2, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN '1d' PRECEDING AND CURRENT ROW) s3 " +
                            "FROM t");
        });
    }

    @Test
    public void testSingleRowAllFactories() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.0m, 10.0m, 10.000m, 10.00m, 10.000000m, 10m)");
            assertSql("""
                            c8\ts8\tmx8\tmn8\tfv8\tlv8\tavg8
                            1\t1.0\t1.0\t1.0\t1.0\t1.0\t1.0
                            """,
                    "SELECT count(v8) OVER () c8, sum(v8) OVER () s8, max(v8) OVER () mx8, min(v8) OVER () mn8, " +
                            "first_value(v8) OVER () fv8, last_value(v8) OVER () lv8, avg(v8) OVER () avg8 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\ta\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:01:00.000000Z\tb\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:02:00.000000Z\ta\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:03:00.000000Z\tb\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:04:00.000000Z\ta\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:05:00.000000Z\tb\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            """,
                    "SELECT ts, grp, " +
                            "sum(v8) OVER (PARTITION BY grp) s8, sum(v16) OVER (PARTITION BY grp) s16, " +
                            "sum(v32) OVER (PARTITION BY grp) s32, sum(v64) OVER (PARTITION BY grp) s64, " +
                            "sum(v128) OVER (PARTITION BY grp) s128, sum(v256) OVER (PARTITION BY grp) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertSql("""
                            s8\ts16\ts32\ts64\ts128\ts256
                            -1.0\t-10.0\t-10.000\t-10.00\t-10.000000\t-10
                            """,
                    "SELECT sum(v8) OVER () s8, sum(v16) OVER () s16, sum(v32) OVER () s32, " +
                            "sum(v64) OVER () s64, sum(v128) OVER () s128, sum(v256) OVER () s256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesOverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t1.4\t14.0\t14.000\t14.00\t14.000000\t14
                            2024-01-01T00:03:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\ta\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:05:00.000000Z\tb\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            """,
                    "SELECT ts, grp, " +
                            "sum(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t1.4\t14.0\t14.000\t14.00\t14.000000\t14
                            2024-01-01T00:03:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\ta\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:05:00.000000Z\tb\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            """,
                    "SELECT ts, grp, " +
                            "sum(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesOverPartitionSlidingRows() throws Exception {
        // INSERT_6_PART: a: 0.6,0.8,0.4 at rows 0,2,4 ; b: 0.4,0.2,0.6 at rows 1,3,5
        // Sliding 2-window within partition:
        // a: 0.6, 1.4 (0.6+0.8), 1.2 (0.8+0.4)
        // b: 0.4, 0.6 (0.4+0.2), 0.8 (0.2+0.6)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t1.4\t14.0\t14.000\t14.00\t14.000000\t14
                            2024-01-01T00:03:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\ta\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:05:00.000000Z\tb\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, grp, " +
                            "sum(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesOverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:02:00.000000Z\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:03:00.000000Z\t2.0\t20.0\t20.000\t20.00\t20.000000\t20
                            2024-01-01T00:04:00.000000Z\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesOverUnboundedPartitionRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\ta\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:01:00.000000Z\tb\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:02:00.000000Z\ta\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:03:00.000000Z\tb\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:04:00.000000Z\ta\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:05:00.000000Z\tb\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            """,
                    "SELECT ts, grp, " +
                            "sum(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s8, " +
                            "sum(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s16, " +
                            "sum(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s32, " +
                            "sum(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s64, " +
                            "sum(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s128, " +
                            "sum(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:02:00.000000Z\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:03:00.000000Z\t2.0\t20.0\t20.000\t20.00\t20.000000\t20
                            2024-01-01T00:04:00.000000Z\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            s8\ts16\ts32\ts64\ts128\ts256
                            3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            """,
                    "SELECT sum(v8) OVER () s8, sum(v16) OVER () s16, sum(v32) OVER () s32, " +
                            "sum(v64) OVER () s64, sum(v128) OVER () s128, sum(v256) OVER () s256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:02:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:03:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:04:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesSymmetricSliding() throws Exception {
        // INSERT_5 vals 0.6, 0.4, 0.8, 0.2, 1.0 — windows: [0,1] [0,1,2] [1,2,3] [2,3,4] [3,4]
        // sums: 1.0, 1.8, 1.4, 2.0, 1.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:01:00.000000Z\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:02:00.000000Z\t1.4\t14.0\t14.000\t14.00\t14.000000\t14
                            2024-01-01T00:03:00.000000Z\t2.0\t20.0\t20.000\t20.00\t20.000000\t20
                            2024-01-01T00:04:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) s8, " +
                            "sum(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) s16, " +
                            "sum(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) s32, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) s64, " +
                            "sum(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) s128, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesUnboundedToUnbounded() throws Exception {
        // sum total: 3.0 (v8), 30.0 (v16+)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            2024-01-01T00:01:00.000000Z\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            2024-01-01T00:02:00.000000Z\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            2024-01-01T00:03:00.000000Z\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            2024-01-01T00:04:00.000000Z\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s8, " +
                            "sum(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s16, " +
                            "sum(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s32, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s64, " +
                            "sum(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s128, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertSql("""
                            s8\ts16\ts32\ts64\ts128\ts256
                            \t\t\t\t\t
                            """,
                    "SELECT sum(v8) OVER () s8, sum(v16) OVER () s16, sum(v32) OVER () s32, " +
                            "sum(v64) OVER () s64, sum(v128) OVER () s128, sum(v256) OVER () s256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            s8\ts16\ts32\ts64\ts128\ts256
                            1.4\t14.0\t14.000\t14.00\t14.000000\t14
                            """,
                    "SELECT sum(v8) OVER () s8, sum(v16) OVER () s16, sum(v32) OVER () s32, " +
                            "sum(v64) OVER () s64, sum(v128) OVER () s128, sum(v256) OVER () s256 " +
                            "FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumDecimal128AccumulatorPromotion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 999999999999999999m), " +
                    "('2024-01-01T00:01:00', 999999999999999999m)");
            assertSql("s\n1999999999999999998\n",
                    "SELECT sum(v) OVER () s FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumDecimal128LargeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 99999999999999999999999999999999999999m), " +
                    "('2024-01-01T00:01:00', 1m)");
            assertSql("sum_v\n100000000000000000000000000000000000000\n",
                    "SELECT sum(v) OVER () AS sum_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumDecimal256NearOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(70, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1000000000000000000000000000000000000000000000000000000000000000000000m), " +
                    "('2024-01-01T00:01:00', 1m)");
            assertSql("sum_v\n1000000000000000000000000000000000000000000000000000000000000000000001\n",
                    "SELECT sum(v) OVER () AS sum_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumDecimalWithExtremeMix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', -1m), " +
                    "('2024-01-01T00:01:00', 1m), " +
                    "('2024-01-01T00:02:00', -1m), " +
                    "('2024-01-01T00:03:00', 1m)");
            assertSql("s\n0\n",
                    "SELECT sum(v) OVER () s FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumOnExpressionInWindow() throws Exception {
        // sum(v64 * 2) = 2 * sum = 60.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t60.00
                            2024-01-01T00:01:00.000000Z\t60.00
                            2024-01-01T00:02:00.000000Z\t60.00
                            2024-01-01T00:03:00.000000Z\t60.00
                            2024-01-01T00:04:00.000000Z\t60.00
                            """,
                    "SELECT ts, sum(v64 * 2::decimal(18, 2)) OVER () s FROM t");
        });
    }

    @Test
    public void testWindowAfterWhereFilter() throws Exception {
        // Filter v64 > 6: rows ts=02 (8), ts=04 (10). Sum = 18
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts64
                            2024-01-01T00:02:00.000000Z\t18.00
                            2024-01-01T00:04:00.000000Z\t18.00
                            """,
                    "SELECT ts, sum(v64) OVER () s64 FROM t WHERE v64 > 6.00::decimal(18, 2)");
        });
    }

    @Test
    public void testWindowAggregateOverMultipleRows() throws Exception {
        // INSERT_6_PART: a sum 18.00, b sum 12.00, total 30.00
        // ratio a = 18/30 = 0.6 ; ratio b = 12/30 = 0.4
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ts_grp\ts_total\tratio
                            2024-01-01T00:00:00.000000Z\ta\t18.00\t30.00\t0.6
                            2024-01-01T00:01:00.000000Z\tb\t12.00\t30.00\t0.4
                            2024-01-01T00:02:00.000000Z\ta\t18.00\t30.00\t0.6
                            2024-01-01T00:03:00.000000Z\tb\t12.00\t30.00\t0.4
                            2024-01-01T00:04:00.000000Z\ta\t18.00\t30.00\t0.6
                            2024-01-01T00:05:00.000000Z\tb\t12.00\t30.00\t0.4
                            """,
                    "SELECT ts, grp, " +
                            "sum(v64) OVER (PARTITION BY grp) s_grp, " +
                            "sum(v64) OVER () s_total, " +
                            "(sum(v64) OVER (PARTITION BY grp))::double / sum(v64) OVER ()::double ratio " +
                            "FROM t");
        });
    }

    @Test
    public void testWindowAvgPrecisionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1m), ('2024-01-01T00:01:00', 2m), ('2024-01-01T00:02:00', 3m)");
            assertSql("avg_v\n2\n",
                    "SELECT avg(v) OVER () AS avg_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testWindowAvgRescaleHigherScale() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1m), ('2024-01-01T00:01:00', 2m), ('2024-01-01T00:02:00', 4m)");
            assertSql("avg_v\n2.33333\n",
                    "SELECT avg(v, 5) OVER () AS avg_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testWindowAvgRescaleZeroScale() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 5)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1.00000m), ('2024-01-01T00:01:00', 2.00000m), ('2024-01-01T00:02:00', 4.00000m)");
            assertSql("avg_v\n2\n",
                    "SELECT avg(v, 0) OVER () AS avg_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testWindowDifferenceBetweenTwoWindows() throws Exception {
        // total=30, cumsum: 6, 10, 18, 20, 30 ; remaining: 24, 20, 12, 10, 0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tcumsum\ttotal\tremaining
                            2024-01-01T00:00:00.000000Z\t6.00\t30.00\t24.00
                            2024-01-01T00:01:00.000000Z\t10.00\t30.00\t20.00
                            2024-01-01T00:02:00.000000Z\t18.00\t30.00\t12.00
                            2024-01-01T00:03:00.000000Z\t20.00\t30.00\t10.00
                            2024-01-01T00:04:00.000000Z\t30.00\t30.00\t0.00
                            """,
                    "SELECT ts, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumsum, " +
                            "sum(v64) OVER () total, " +
                            "sum(v64) OVER () - sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) remaining " +
                            "FROM t");
        });
    }

    @Test
    public void testWindowFunctionInWhereForbidden() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            try {
                assertSql("", "SELECT * FROM t WHERE sum(v64) OVER () > 10.00::decimal(18, 2)");
            } catch (Exception ex) {
                // expected: window functions are not allowed in WHERE
            }
        });
    }

    @Test
    public void testWindowFunctionResultUsedInOrderBy() throws Exception {
        // a sum 18, b sum 12 -> sorted DESC by s_grp: all a (18) first, then all b (12)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            grp\ts_grp
                            a\t18.00
                            a\t18.00
                            a\t18.00
                            b\t12.00
                            b\t12.00
                            b\t12.00
                            """,
                    "SELECT grp, sum(v64) OVER (PARTITION BY grp) s_grp FROM t ORDER BY s_grp DESC");
        });
    }

    @Test
    public void testWindowInArithmeticExpression() throws Exception {
        // v64: 6.00, 4.00, 8.00, 2.00, 10.00 ; avg=6.00 ; diff: 0, -2, 2, -4, 4
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tdiff64
                            2024-01-01T00:00:00.000000Z\t0.00
                            2024-01-01T00:01:00.000000Z\t-2.00
                            2024-01-01T00:02:00.000000Z\t2.00
                            2024-01-01T00:03:00.000000Z\t-4.00
                            2024-01-01T00:04:00.000000Z\t4.00
                            """,
                    "SELECT ts, v64 - avg(v64) OVER () AS diff64 FROM t");
        });
    }

    @Test
    public void testWindowInCte() throws Exception {
        // sum=30.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts64
                            2024-01-01T00:02:00.000000Z\t30.00
                            """,
                    "WITH w AS (SELECT ts, sum(v64) OVER () s64 FROM t) " +
                            "SELECT ts, s64 FROM w WHERE ts = '2024-01-01T00:02:00'");
        });
    }

    @Test
    public void testWindowInSubquery() throws Exception {
        // INSERT_5: cumulative sum 6.00, 10.00, 18.00, 20.00, 30.00 -> > 10.00 are rows 2,3,4
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tv64\ts64
                            2024-01-01T00:02:00.000000Z\t8.00\t18.00
                            2024-01-01T00:03:00.000000Z\t2.00\t20.00
                            2024-01-01T00:04:00.000000Z\t10.00\t30.00
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, v64, sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s64 FROM t" +
                            ") WHERE s64 > 10.00::decimal(18, 2)");
        });
    }

    @Test
    public void testWindowMaxMinFlippingValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 50.00m), " +
                    "('2024-01-01T00:01:00', 10.00m), " +
                    "('2024-01-01T00:02:00', 30.00m), " +
                    "('2024-01-01T00:03:00', 20.00m), " +
                    "('2024-01-01T00:04:00', 40.00m)");
            assertSql("""
                            ts\tmx\tmn
                            2024-01-01T00:00:00.000000Z\t50.00\t50.00
                            2024-01-01T00:01:00.000000Z\t50.00\t10.00
                            2024-01-01T00:02:00.000000Z\t50.00\t10.00
                            2024-01-01T00:03:00.000000Z\t30.00\t20.00
                            2024-01-01T00:04:00.000000Z\t40.00\t20.00
                            """,
                    "SELECT ts, " +
                            "max(v) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) mx, " +
                            "min(v) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) mn " +
                            "FROM t");
        });
    }

    @Test
    public void testWindowMixedD128AndD256InSelect() throws Exception {
        // sum=30, max=10, min=2 (all sub-types)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts128\ts256\tmx128\tmn256
                            2024-01-01T00:00:00.000000Z\t30.000000\t30\t10.000000\t2
                            2024-01-01T00:01:00.000000Z\t30.000000\t30\t10.000000\t2
                            2024-01-01T00:02:00.000000Z\t30.000000\t30\t10.000000\t2
                            2024-01-01T00:03:00.000000Z\t30.000000\t30\t10.000000\t2
                            2024-01-01T00:04:00.000000Z\t30.000000\t30\t10.000000\t2
                            """,
                    "SELECT ts, " +
                            "sum(v128) OVER () s128, sum(v256) OVER () s256, " +
                            "max(v128) OVER () mx128, min(v256) OVER () mn256 FROM t");
        });
    }

    @Test
    public void testWindowMixedSubTypesInOnePartition() throws Exception {
        // a sum: 1.8, b sum: 1.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\ts8\ts32\ts128
                            2024-01-01T00:00:00.000000Z\ta\t1.8\t18.000\t18.000000
                            2024-01-01T00:01:00.000000Z\tb\t1.2\t12.000\t12.000000
                            2024-01-01T00:02:00.000000Z\ta\t1.8\t18.000\t18.000000
                            2024-01-01T00:03:00.000000Z\tb\t1.2\t12.000\t12.000000
                            2024-01-01T00:04:00.000000Z\ta\t1.8\t18.000\t18.000000
                            2024-01-01T00:05:00.000000Z\tb\t1.2\t12.000\t12.000000
                            """,
                    "SELECT ts, grp, " +
                            "sum(v8) OVER (PARTITION BY grp) s8, " +
                            "sum(v32) OVER (PARTITION BY grp) s32, " +
                            "sum(v128) OVER (PARTITION BY grp) s128 " +
                            "FROM t");
        });
    }

    @Test
    public void testWindowMultipliedByConstant() throws Exception {
        // 2 * avg(v64) = 2 * 6.00 = 12.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tdouble_avg
                            2024-01-01T00:00:00.000000Z\t12.00
                            2024-01-01T00:01:00.000000Z\t12.00
                            2024-01-01T00:02:00.000000Z\t12.00
                            2024-01-01T00:03:00.000000Z\t12.00
                            2024-01-01T00:04:00.000000Z\t12.00
                            """,
                    "SELECT ts, 2 * avg(v64) OVER () AS double_avg FROM t");
        });
    }

    @Test
    public void testWindowOnComputedPartitionExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k INT, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1, 10.00m), " +
                    "('2024-01-01T00:01:00', 2, 20.00m), " +
                    "('2024-01-01T00:02:00', 3, 30.00m), " +
                    "('2024-01-01T00:03:00', 4, 40.00m)");
            assertSql("""
                            ts\tk\tv\ts
                            2024-01-01T00:00:00.000000Z\t1\t10.00\t40.00
                            2024-01-01T00:01:00.000000Z\t2\t20.00\t60.00
                            2024-01-01T00:02:00.000000Z\t3\t30.00\t40.00
                            2024-01-01T00:03:00.000000Z\t4\t40.00\t60.00
                            """,
                    "SELECT ts, k, v, sum(v) OVER (PARTITION BY k % 2) s FROM t");
        });
    }

    @Test
    public void testWindowOnDecimalAddition() throws Exception {
        // sum(v + v) = 2 * sum = 60.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t60.00
                            2024-01-01T00:01:00.000000Z\t60.00
                            2024-01-01T00:02:00.000000Z\t60.00
                            2024-01-01T00:03:00.000000Z\t60.00
                            2024-01-01T00:04:00.000000Z\t60.00
                            """,
                    "SELECT ts, sum(v64 + v64) OVER () s FROM t");
        });
    }

    @Test
    public void testWindowOnDerivedColumn() throws Exception {
        // vx2 = v64 * 2 = 12, 8, 16, 4, 20 ; sum=60.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tvx2\ts
                            2024-01-01T00:00:00.000000Z\t12.00\t60.00
                            2024-01-01T00:01:00.000000Z\t8.00\t60.00
                            2024-01-01T00:02:00.000000Z\t16.00\t60.00
                            2024-01-01T00:03:00.000000Z\t4.00\t60.00
                            2024-01-01T00:04:00.000000Z\t20.00\t60.00
                            """,
                    "SELECT ts, vx2, sum(vx2) OVER () s FROM " +
                            "(SELECT ts, v64 * 2::decimal(18, 2) vx2 FROM t)");
        });
    }

    @Test
    public void testWindowOnFilteredView() throws Exception {
        // Filter v64 > 10.00 — none of INSERT_5 v64 values (6, 4, 8, 2, 10) are > 10, so empty
        // Actually 10 is not > 10. So filter on > 10 yields nothing. Change to >= 8 to be more interesting.
        // Filter v64 >= 8: rows ts=02 (8.00), ts=04 (10.00). Sum = 18.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tv64\ts
                            2024-01-01T00:02:00.000000Z\t8.00\t18.00
                            2024-01-01T00:04:00.000000Z\t10.00\t18.00
                            """,
                    "SELECT ts, v64, sum(v64) OVER () s FROM (SELECT * FROM t WHERE v64 >= 8.00::decimal(18, 2))");
        });
    }

    @Test
    public void testWindowOnGroupByExpressionInSubquery() throws Exception {
        // GROUP BY grp: a sum=18.00, b sum=12.00. Rolling sum: a=18, then a+b=30
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            grp\ttotal\trolling
                            a\t18.00\t18.00
                            b\t12.00\t30.00
                            """,
                    "SELECT grp, sum(v64) total, sum(sum(v64)) OVER (ORDER BY grp) rolling FROM t GROUP BY grp");
        });
    }

    @Test
    public void testWindowOnInsertSelect() throws Exception {
        // sum=30.00, 5 rows
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            execute("CREATE TABLE r (ts TIMESTAMP, s decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO r SELECT ts, sum(v64) OVER () FROM t");
            assertSql("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t30.00
                            2024-01-01T00:01:00.000000Z\t30.00
                            2024-01-01T00:02:00.000000Z\t30.00
                            2024-01-01T00:03:00.000000Z\t30.00
                            2024-01-01T00:04:00.000000Z\t30.00
                            """,
                    "SELECT ts, s FROM r");
        });
    }

    @Test
    public void testWindowOnTimestampPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-02T00:00:00', 20.00m), " +
                    "('2024-01-03T00:00:00', 30.00m)");
            assertSql("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t10.00
                            2024-01-02T00:00:00.000000Z\t30.00
                            2024-01-03T00:00:00.000000Z\t60.00
                            """,
                    "SELECT ts, sum(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM t");
        });
    }

    @Test
    public void testWindowOnlyOneNullValue() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', null, null, null, null, null, null)");
            assertSql("""
                            s64\ta64\tc64\tmx64\tmn64
                            \t\t0\t\t
                            """,
                    "SELECT sum(v64) OVER () s64, avg(v64) OVER () a64, count(v64) OVER () c64, " +
                            "max(v64) OVER () mx64, min(v64) OVER () mn64 FROM t LIMIT 1");
        });
    }

    @Test
    public void testWindowOrderByDecimalColumn() throws Exception {
        // INSERT_5 v64: at ts 00=6.00, 01=4.00, 02=8.00, 03=2.00, 04=10.00
        // Sorted by v64 ASC: ts 03=2.00, 01=4.00, 00=6.00, 02=8.00, 04=10.00
        // Cumulative sums in v64 ASC order: 2, 6, 12, 20, 30
        // But output ordering is by ts (no ORDER BY in outer query) — questdb returns in insertion order ts ASC
        // Per row by ts: ts 00 -> position 3 (cum 12), ts 01 -> pos 2 (cum 6), ts 02 -> pos 4 (cum 20), ts 03 -> pos 1 (cum 2), ts 04 -> pos 5 (cum 30)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\tv64\tcs
                            2024-01-01T00:00:00.000000Z\t6.00\t12.00
                            2024-01-01T00:01:00.000000Z\t4.00\t6.00
                            2024-01-01T00:02:00.000000Z\t8.00\t20.00
                            2024-01-01T00:03:00.000000Z\t2.00\t2.00
                            2024-01-01T00:04:00.000000Z\t10.00\t30.00
                            """,
                    "SELECT ts, v64, sum(v64) OVER (ORDER BY v64 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cs FROM t");
        });
    }

    @Test
    public void testWindowSumPlusLiteral() throws Exception {
        // sum=30.00, +1.00 = 31.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts_plus_1
                            2024-01-01T00:00:00.000000Z\t31.00
                            2024-01-01T00:01:00.000000Z\t31.00
                            2024-01-01T00:02:00.000000Z\t31.00
                            2024-01-01T00:03:00.000000Z\t31.00
                            2024-01-01T00:04:00.000000Z\t31.00
                            """,
                    "SELECT ts, sum(v64) OVER () + 1.00::decimal(18, 2) s_plus_1 FROM t");
        });
    }

    @Test
    public void testWindowWithCastInSelect() throws Exception {
        // sum=30.00 cast to double = 30.0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts_as_double
                            2024-01-01T00:00:00.000000Z\t30.0
                            2024-01-01T00:01:00.000000Z\t30.0
                            2024-01-01T00:02:00.000000Z\t30.0
                            2024-01-01T00:03:00.000000Z\t30.0
                            2024-01-01T00:04:00.000000Z\t30.0
                            """,
                    "SELECT ts, (sum(v64) OVER ())::double s_as_double FROM t");
        });
    }

    @Test
    public void testWindowWithDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 10.00m), " +
                    "('2024-01-01T00:02:00', 20.00m)");
            assertSql("s\n40.00\n",
                    "SELECT DISTINCT sum(v) OVER () s FROM t");
        });
    }

    @Test
    public void testWindowWithJoin() throws Exception {
        // INSERT_5 all in partition 'a' (5 rows). sum=30.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            execute("CREATE TABLE m (k SYMBOL, label STRING)");
            execute("INSERT INTO m VALUES ('a', 'group-a')");
            assertSql("""
                            ts\tlabel\ts64
                            2024-01-01T00:00:00.000000Z\tgroup-a\t30.00
                            2024-01-01T00:01:00.000000Z\tgroup-a\t30.00
                            2024-01-01T00:02:00.000000Z\tgroup-a\t30.00
                            2024-01-01T00:03:00.000000Z\tgroup-a\t30.00
                            2024-01-01T00:04:00.000000Z\tgroup-a\t30.00
                            """,
                    "SELECT t.ts, m.label, sum(t.v64) OVER () s64 FROM t JOIN m ON t.grp = m.k");
        });
    }

    @Test
    public void testWindowWithLimit() throws Exception {
        // sum=30 for all rows; LIMIT 2 just shows first 2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts64
                            2024-01-01T00:00:00.000000Z\t30.00
                            2024-01-01T00:01:00.000000Z\t30.00
                            """,
                    "SELECT ts, sum(v64) OVER () s64 FROM t LIMIT 2");
        });
    }

    @Test
    public void testWindowWithManyDistinctPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k INT, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60_000_000), x::int, cast(1.00 as decimal(18, 2)) FROM long_sequence(100)");
            assertSql("c\n100\n",
                    "SELECT count(s) c FROM (SELECT sum(v) OVER (PARTITION BY k) s FROM t)");
        });
    }

    @Test
    public void testWindowWithUnionAll() throws Exception {
        // sum(v64) = 30.00 ; 5 rows + 5 rows = 10
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t30.00
                            2024-01-01T00:01:00.000000Z\t30.00
                            2024-01-01T00:02:00.000000Z\t30.00
                            2024-01-01T00:03:00.000000Z\t30.00
                            2024-01-01T00:04:00.000000Z\t30.00
                            2024-01-01T00:00:00.000000Z\t30.00
                            2024-01-01T00:01:00.000000Z\t30.00
                            2024-01-01T00:02:00.000000Z\t30.00
                            2024-01-01T00:03:00.000000Z\t30.00
                            2024-01-01T00:04:00.000000Z\t30.00
                            """,
                    "SELECT ts, sum(v64) OVER () s FROM t UNION ALL SELECT ts, sum(v64) OVER () s FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesForwardOnlyFrame() throws Exception {
        // ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING -- sum of remaining future rows
        // INSERT_5 v8: 0.6, 0.4, 0.8, 0.2, 1.0
        // Row 0: 0.4+0.8+0.2+1.0 = 2.4 ; Row 1: 2.0 ; Row 2: 1.2 ; Row 3: 1.0 ; Row 4: null
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts64\ts256
                            2024-01-01T00:00:00.000000Z\t2.4\t24.00\t24
                            2024-01-01T00:01:00.000000Z\t2.0\t20.00\t20
                            2024-01-01T00:02:00.000000Z\t1.2\t12.00\t12
                            2024-01-01T00:03:00.000000Z\t1.0\t10.00\t10
                            2024-01-01T00:04:00.000000Z\t\t\t
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) s8, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) s64, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesPastOnlyFrame() throws Exception {
        // ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- sum of past, excluding current
        // Row 0: null ; Row 1: 0.6 ; Row 2: 1.0 ; Row 3: 1.8 ; Row 4: 2.0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts64\ts256
                            2024-01-01T00:00:00.000000Z\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.00\t6
                            2024-01-01T00:02:00.000000Z\t1.0\t10.00\t10
                            2024-01-01T00:03:00.000000Z\t1.8\t18.00\t18
                            2024-01-01T00:04:00.000000Z\t2.0\t20.00\t20
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) s8, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) s64, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testSumAllSubTypesPureForwardFrame() throws Exception {
        // ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING -- next 2 rows only
        // Row 0: 0.4+0.8=1.2 ; Row 1: 0.8+0.2=1.0 ; Row 2: 0.2+1.0=1.2 ; Row 3: 1.0 ; Row 4: null
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts64\ts256
                            2024-01-01T00:00:00.000000Z\t1.2\t12.00\t12
                            2024-01-01T00:01:00.000000Z\t1.0\t10.00\t10
                            2024-01-01T00:02:00.000000Z\t1.2\t12.00\t12
                            2024-01-01T00:03:00.000000Z\t1.0\t10.00\t10
                            2024-01-01T00:04:00.000000Z\t\t\t
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) s8, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) s64, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) s256 " +
                            "FROM t");
        });
    }

    @Test
    public void testLagLeadAllSubTypesPartitionReset() throws Exception {
        // Within-partition lag/lead for non-D64 sub-types
        // a (ts 00,02,04): 0.6, 0.8, 0.4 -> lag: null, 0.6, 0.8 ; lead: 0.8, 0.4, null
        // b (ts 01,03,05): 0.4, 0.2, 0.6 -> lag: null, 0.4, 0.2 ; lead: 0.2, 0.6, null
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertSql("""
                            ts\tgrp\tlg8\tlg256\tld8\tld256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t0.8\t8
                            2024-01-01T00:01:00.000000Z\tb\t\t\t0.2\t2
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6\t0.4\t4
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4\t0.6\t6
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8\t\t
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2\t\t
                            """,
                    "SELECT ts, grp, " +
                            "lag(v8, 1) OVER (PARTITION BY grp ORDER BY ts) lg8, " +
                            "lag(v256, 1) OVER (PARTITION BY grp ORDER BY ts) lg256, " +
                            "lead(v8, 1) OVER (PARTITION BY grp ORDER BY ts) ld8, " +
                            "lead(v256, 1) OVER (PARTITION BY grp ORDER BY ts) ld256 " +
                            "FROM t");
        });
    }

    @Test
    public void testAvgRescaleTruncatingScale() throws Exception {
        // avg(v64, 0) -- truncate from scale 2 to scale 0. avg=6.00 -> 6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("avg_v\n6\n",
                    "SELECT avg(v64, 0) OVER () AS avg_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgRescaleSameScale() throws Exception {
        // avg(v64, 2) -- same as input scale (no-op rescale)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("avg_v\n6.00\n",
                    "SELECT avg(v64, 2) OVER () AS avg_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumDecimal256NearMaxPrecision() throws Exception {
        // D256 supports up to 76-77 digit precision. Sum two near-max values.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(70, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 5000000000000000000000000000000000000000000000000000000000000000000000m), " +
                    "('2024-01-01T00:01:00', 5000000000000000000000000000000000000000000000000000000000000000000000m)");
            assertSql("s\n10000000000000000000000000000000000000000000000000000000000000000000000\n",
                    "SELECT sum(v) OVER () s FROM t LIMIT 1");
        });
    }

    @Test
    public void testAvgDecimal8PromotedAccumulator() throws Exception {
        // D8 (precision 2, max 99) summed beyond range -- uses promoted accumulator internally
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60000000), cast(9 as decimal(2, 0)) FROM long_sequence(50)");
            assertSql("avg_v\n9\n",
                    "SELECT avg(v) OVER () AS avg_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testFirstValueAllSubTypesDuplicateValues() throws Exception {
        // first_value stably returns the first row's value regardless of duplicates
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("f8\tf256\n0.6\t6\n",
                    "SELECT first_value(v8) OVER () f8, first_value(v256) OVER () f256 FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumNullsFirstOrderBy() throws Exception {
        // INSERT_5_WITH_NULL v64: null@00, 6.00@01, null@02, 8.00@03, null@04
        // ORDER BY v64 NULLS FIRST, ts -- processing order: null@00, null@02, null@04, 6.00@01, 8.00@03
        // Cumulative sums in window order: null, null, null, 6.00, 14.00
        // Display by ts ASC: row at each ts shows the sum computed at its window position
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tv64\ts
                            2024-01-01T00:00:00.000000Z\t\t
                            2024-01-01T00:01:00.000000Z\t6.00\t6.00
                            2024-01-01T00:02:00.000000Z\t\t
                            2024-01-01T00:03:00.000000Z\t8.00\t14.00
                            2024-01-01T00:04:00.000000Z\t\t
                            """,
                    "SELECT ts, v64, " +
                            "sum(v64) OVER (ORDER BY v64 NULLS FIRST, ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s " +
                            "FROM t ORDER BY ts");
        });
    }

    @Test
    public void testSumNullsLastOrderBy() throws Exception {
        // ORDER BY v64 ASC NULLS LAST, ts -- processing order: 6.00@01, 8.00@03, null@00, null@02, null@04
        // Cumulative sums in window order: 6.00, 14.00, 14.00, 14.00, 14.00
        // Display by ts:
        //   ts 00 (null, 3rd in window order) -> 14.00
        //   ts 01 (6.00, 1st) -> 6.00
        //   ts 02 (null, 4th) -> 14.00
        //   ts 03 (8.00, 2nd) -> 14.00
        //   ts 04 (null, 5th) -> 14.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tv64\ts
                            2024-01-01T00:00:00.000000Z\t\t14.00
                            2024-01-01T00:01:00.000000Z\t6.00\t6.00
                            2024-01-01T00:02:00.000000Z\t\t14.00
                            2024-01-01T00:03:00.000000Z\t8.00\t14.00
                            2024-01-01T00:04:00.000000Z\t\t14.00
                            """,
                    "SELECT ts, v64, " +
                            "sum(v64) OVER (ORDER BY v64 NULLS LAST, ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s " +
                            "FROM t ORDER BY ts");
        });
    }

    @Test
    public void testSumDescNullsLastOrderBy() throws Exception {
        // ORDER BY v64 DESC NULLS LAST, ts -- processing order: 8.00@03, 6.00@01, null@00, null@02, null@04
        // Cumulative sums in window order: 8.00, 14.00, 14.00, 14.00, 14.00
        // Display by ts:
        //   ts 00 (null, 3rd) -> 14.00
        //   ts 01 (6.00, 2nd) -> 14.00
        //   ts 02 (null, 4th) -> 14.00
        //   ts 03 (8.00, 1st) -> 8.00
        //   ts 04 (null, 5th) -> 14.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tv64\ts
                            2024-01-01T00:00:00.000000Z\t\t14.00
                            2024-01-01T00:01:00.000000Z\t6.00\t14.00
                            2024-01-01T00:02:00.000000Z\t\t14.00
                            2024-01-01T00:03:00.000000Z\t8.00\t8.00
                            2024-01-01T00:04:00.000000Z\t\t14.00
                            """,
                    "SELECT ts, v64, " +
                            "sum(v64) OVER (ORDER BY v64 DESC NULLS LAST, ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s " +
                            "FROM t ORDER BY ts");
        });
    }

    @Test
    public void testFirstValueNullsFirst() throws Exception {
        // ORDER BY v64 NULLS FIRST -- first row in window is null. first_value RESPECT NULLS = null for all rows.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tfv
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T00:01:00.000000Z\t
                            2024-01-01T00:02:00.000000Z\t
                            2024-01-01T00:03:00.000000Z\t
                            2024-01-01T00:04:00.000000Z\t
                            """,
                    "SELECT ts, " +
                            "first_value(v64) OVER (ORDER BY v64 NULLS FIRST, ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) fv " +
                            "FROM t ORDER BY ts");
        });
    }

    @Test
    public void testFirstValueNullsLastIgnoreNulls() throws Exception {
        // ORDER BY v64 NULLS LAST -- first row in window is 6.00 (smallest non-null).
        // first_value IGNORE NULLS at each row:
        //   ts 00 (null, 3rd in window order) -> 6.00 (first non-null seen up to its window position)
        //   ts 01 (6.00, 1st) -> 6.00
        //   ts 02 (null, 4th) -> 6.00
        //   ts 03 (8.00, 2nd) -> 6.00
        //   ts 04 (null, 5th) -> 6.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertSql("""
                            ts\tfv
                            2024-01-01T00:00:00.000000Z\t6.00
                            2024-01-01T00:01:00.000000Z\t6.00
                            2024-01-01T00:02:00.000000Z\t6.00
                            2024-01-01T00:03:00.000000Z\t6.00
                            2024-01-01T00:04:00.000000Z\t6.00
                            """,
                    "SELECT ts, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY v64 NULLS LAST, ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) fv " +
                            "FROM t ORDER BY ts");
        });
    }

    @Test
    public void testSumAllSubTypesRangeFrameForwardOnly() throws Exception {
        // RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING -- current + future
        // Row 0: 3.0 ; Row 1: 2.4 ; Row 2: 2.0 ; Row 3: 1.2 ; Row 4: 1.0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertSql("""
                            ts\ts8\ts64\ts256
                            2024-01-01T00:00:00.000000Z\t3.0\t30.00\t30
                            2024-01-01T00:01:00.000000Z\t2.4\t24.00\t24
                            2024-01-01T00:02:00.000000Z\t2.0\t20.00\t20
                            2024-01-01T00:03:00.000000Z\t1.2\t12.00\t12
                            2024-01-01T00:04:00.000000Z\t1.0\t10.00\t10
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) s8, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) s64, " +
                            "sum(v256) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) s256 " +
                            "FROM t");
        });
    }
}
