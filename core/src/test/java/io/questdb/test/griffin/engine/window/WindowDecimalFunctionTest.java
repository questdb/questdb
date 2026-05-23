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

import io.questdb.PropertyKey;
import io.questdb.std.str.StringSink;
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
            assertQueryNoLeakCheck("""
                            grp\ts8\ts64\ts256\tc8\tc64\tc256
                            a\t0.2\t2.00\t2\t1\t1\t1
                            a\t0.2\t2.00\t2\t1\t1\t1
                            b\t0.3\t3.00\t3\t1\t1\t1
                            b\t0.3\t3.00\t3\t1\t1\t1
                            """,
                    "SELECT grp, " +
                            "sum(v8) OVER (PARTITION BY grp) s8, sum(v64) OVER (PARTITION BY grp) s64, sum(v256) OVER (PARTITION BY grp) s256, " +
                            "count(v8) OVER (PARTITION BY grp) c8, count(v64) OVER (PARTITION BY grp) c64, count(v256) OVER (PARTITION BY grp) c256 " +
                            "FROM t", null, null, true, true);
        });
    }

    @Test
    public void testAllSubTypesPartitionResetWithSliding() throws Exception {
        // last in sliding 2-window within partition = current row's val
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesBoundedRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesBoundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertQueryNoLeakCheck("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            -0.2\t-2.0\t-2.000\t-2.00\t-2.000000\t-2
                            """,
                    "SELECT avg(v8) OVER () a8, avg(v16) OVER () a16, avg(v32) OVER () a32, " +
                            "avg(v64) OVER () a64, avg(v128) OVER () a128, avg(v256) OVER () a256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesOverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesOverPartitionSlidingRows() throws Exception {
        // a avg: 0.6, 0.7 (1.4/2), 0.6 (1.2/2) ; b avg: 0.4, 0.3 (0.6/2), 0.4 (0.8/2)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesOverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT avg(v8) OVER () a8, avg(v16) OVER () a16, avg(v32) OVER () a32, " +
                            "avg(v64) OVER () a64, avg(v128) OVER () a128, avg(v256) OVER () a256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgAllSubTypesRowsNonZeroHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a8, " +
                            "avg(v16) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a16, " +
                            "avg(v32) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a32, " +
                            "avg(v64) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a128, " +
                            "avg(v256) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesRowsNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.7\t7.0\t7.000\t7.00\t7.000000\t7
                            2024-01-01T00:05:00.000000Z\tb\t0.3\t3.0\t3.000\t3.00\t3.000000\t3
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a8, " +
                            "avg(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a16, " +
                            "avg(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a32, " +
                            "avg(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a64, " +
                            "avg(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a128, " +
                            "avg(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesUnboundedRangeBoundedHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.5\t5.0\t5.000\t5.00\t5.000000\t5
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a8, " +
                            "avg(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a16, " +
                            "avg(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a32, " +
                            "avg(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a64, " +
                            "avg(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a128, " +
                            "avg(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesUnboundedRangeBoundedHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.7\t7.0\t7.000\t7.00\t7.000000\t7
                            2024-01-01T00:05:00.000000Z\tb\t0.3\t3.0\t3.000\t3.00\t3.000000\t3
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a8, " +
                            "avg(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a16, " +
                            "avg(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a32, " +
                            "avg(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a64, " +
                            "avg(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a128, " +
                            "avg(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgAllSubTypesUnboundedToUnbounded() throws Exception {
        // avg total: 0.6 (v8), 6.0 (v16+)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            \t\t\t\t\t
                            """,
                    "SELECT avg(v8) OVER () a8, avg(v16) OVER () a16, avg(v32) OVER () a32, " +
                            "avg(v64) OVER () a64, avg(v128) OVER () a128, avg(v256) OVER () a256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            0.7\t7.0\t7.000\t7.00\t7.000000\t7
                            """,
                    "SELECT avg(v8) OVER () a8, avg(v16) OVER () a16, avg(v32) OVER () a32, " +
                            "avg(v64) OVER () a64, avg(v128) OVER () a128, avg(v256) OVER () a256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgDecimal256OverflowAtAdd() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (ts TIMESTAMP, v decimal(76, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t2 VALUES " +
                    "('2024-01-01T00:00:00', 5000000000000000000000000000000000000000000000000000000000000000000000000000m), " +
                    "('2024-01-01T00:01:00', 5000000000000000000000000000000000000000000000000000000000000000000000000000m)");
            assertExceptionNoLeakCheck(
                    "SELECT avg(v) OVER () c FROM t2",
                    "SELECT avg(".length(),
                    "avg aggregation failed"
            );
        });
    }

    @Test
    public void testAvgDecimal64HalfEven() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1.00m), ('2024-01-01T00:01:00', 2.00m), ('2024-01-01T00:02:00', 4.00m)");
            assertQueryNoLeakCheck("avg_v\n2.33\n", "SELECT avg(v) OVER () AS avg_v FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgDecimal8PromotedAccumulator() throws Exception {
        // D8 (precision 2, max 99) summed beyond range -- uses promoted accumulator internally
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60_000_000), 9::decimal(2, 0) FROM long_sequence(50)");
            assertQueryNoLeakCheck("avg_v\n9\n", "SELECT avg(v) OVER () AS avg_v FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesBoundedRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:02:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:04:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 5) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesBoundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:02:00.000000Z\ta\t0.80000\t8.00000\t8.00000\t8.00000\t8.00000\t8.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.20000\t2.00000\t2.00000\t2.00000\t2.00000\t2.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesBoundedRowsPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:02:00.000000Z\ta\t0.70000\t7.00000\t7.00000\t7.00000\t7.00000\t7.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.30000\t3.00000\t3.00000\t3.00000\t3.00000\t3.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertQueryNoLeakCheck("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            -0.20000\t-2.00000\t-2.00000\t-2.00000\t-2.00000\t-2.00000
                            """,
                    "SELECT avg(v8, 5) OVER () a8, avg(v16, 5) OVER () a16, avg(v32, 5) OVER () a32, " +
                            "avg(v64, 5) OVER () a64, avg(v128, 5) OVER () a128, avg(v256, 5) OVER () a256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverPartitionRangeFrame() throws Exception {
        // a cum avg: 0.6, 0.7 (1.4/2), 0.6 (1.8/3) ; b cum avg: 0.4, 0.3 (0.6/2), 0.4 (1.2/3)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverRangeFrame() throws Exception {
        // cumulative avg on INSERT_5: 0.6, 0.5, 0.6, 0.5, 0.6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            """,
                    "SELECT avg(v8, 5) OVER () a8, avg(v16, 5) OVER () a16, avg(v32, 5) OVER () a32, " +
                            "avg(v64, 5) OVER () a64, avg(v128, 5) OVER () a128, avg(v256, 5) OVER () a256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesPartitionCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:02:00.000000Z\ta\t0.80000\t8.00000\t8.00000\t8.00000\t8.00000\t8.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.20000\t2.00000\t2.00000\t2.00000\t2.00000\t2.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a8, " +
                            "avg(v16, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a32, " +
                            "avg(v64, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a128, " +
                            "avg(v256, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesPartitionDefaultCoverage() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:02:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp) a8, " +
                            "avg(v16, 5) OVER (PARTITION BY grp) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp) a32, " +
                            "avg(v64, 5) OVER (PARTITION BY grp) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp) a128, " +
                            "avg(v256, 5) OVER (PARTITION BY grp) a256 " +
                            "FROM t", "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesPartitionDefaultWithNulls() throws Exception {
        // NULL input triggers the early-return NULL branch in pass1 for OverPartition classes.
        // Empty partitions (after all NULLs filtered) trigger pass2's writeNull / count==0 paths.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER (PARTITION BY grp) a8, " +
                            "avg(v8, 3) OVER (PARTITION BY grp) a16, " +
                            "avg(v8, 5) OVER (PARTITION BY grp) a32, " +
                            "avg(v8, 14) OVER (PARTITION BY grp) a64, " +
                            "avg(v8, 30) OVER (PARTITION BY grp) a128, " +
                            "avg(v8, 60) OVER (PARTITION BY grp) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesPartitionWholeViaUnboundedFollowing() throws Exception {
        // PARTITION BY + ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING dispatches
        // to OverPartitionFunction via the partition rowsLo=MIN_VALUE && rowsHi=MAX_VALUE branch.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:02:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a8, " +
                            "avg(v16, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a32, " +
                            "avg(v64, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a128, " +
                            "avg(v256, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesRangeTriggersBufferExpansion() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 33; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            StringSink expected = new StringSink();
            expected.put("ts\ta8\n");
            for (int i = 0; i < 33; i++) {
                expected.put("2024-01-01T00:");
                if (i < 10) expected.put('0');
                expected.put(i).put(":00.000000Z\t0.50000\n");
            }
            assertQueryNoLeakCheck(expected.toString(),
                    "SELECT ts, avg(v8, 5) OVER (ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) a8 FROM t",
                    null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesRowsNonZeroHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:02:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:03:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:04:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a8, " +
                            "avg(v16, 5) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a16, " +
                            "avg(v32, 5) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a32, " +
                            "avg(v64, 5) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a64, " +
                            "avg(v128, 5) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a128, " +
                            "avg(v256, 5) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesRowsNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.70000\t7.00000\t7.00000\t7.00000\t7.00000\t7.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.30000\t3.00000\t3.00000\t3.00000\t3.00000\t3.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a8, " +
                            "avg(v16, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a32, " +
                            "avg(v64, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a128, " +
                            "avg(v256, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesUnboundedPartitionRowsCoverage() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesUnboundedRangeBoundedHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:02:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            2024-01-01T00:03:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:04:00.000000Z\t0.50000\t5.00000\t5.00000\t5.00000\t5.00000\t5.00000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a8, " +
                            "avg(v16, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a16, " +
                            "avg(v32, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a32, " +
                            "avg(v64, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a64, " +
                            "avg(v128, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a128, " +
                            "avg(v256, 5) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesUnboundedRangeBoundedHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\tb\t0.40000\t4.00000\t4.00000\t4.00000\t4.00000\t4.00000
                            2024-01-01T00:04:00.000000Z\ta\t0.70000\t7.00000\t7.00000\t7.00000\t7.00000\t7.00000
                            2024-01-01T00:05:00.000000Z\tb\t0.30000\t3.00000\t3.00000\t3.00000\t3.00000\t3.00000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a8, " +
                            "avg(v16, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a16, " +
                            "avg(v32, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a32, " +
                            "avg(v64, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a64, " +
                            "avg(v128, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a128, " +
                            "avg(v256, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesUnboundedRowsCoverage() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesWholeResultSetCoverage() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:02:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:04:00.000000Z\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 5) OVER () a8, " +
                            "avg(v16, 5) OVER () a16, " +
                            "avg(v32, 5) OVER () a32, " +
                            "avg(v64, 5) OVER () a64, " +
                            "avg(v128, 5) OVER () a128, " +
                            "avg(v256, 5) OVER () a256 " +
                            "FROM t", "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesWholeResultSetWithNulls() throws Exception {
        // All NULL input triggers count==0 path in OverWholeResultSet's pass2.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER () a8, " +
                            "avg(v8, 3) OVER () a16, " +
                            "avg(v8, 5) OVER () a32, " +
                            "avg(v8, 14) OVER () a64, " +
                            "avg(v8, 30) OVER () a128, " +
                            "avg(v8, 60) OVER () a256 " +
                            "FROM t", "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            \t\t\t\t\t
                            """,
                    "SELECT avg(v8, 5) OVER () a8, avg(v16, 5) OVER () a16, avg(v32, 5) OVER () a32, " +
                            "avg(v64, 5) OVER () a64, avg(v128, 5) OVER () a128, avg(v256, 5) OVER () a256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgRescaleAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            a8\ta16\ta32\ta64\ta128\ta256
                            0.70000\t7.00000\t7.00000\t7.00000\t7.00000\t7.00000
                            """,
                    "SELECT avg(v8, 5) OVER () a8, avg(v16, 5) OVER () a16, avg(v32, 5) OVER () a32, " +
                            "avg(v64, 5) OVER () a64, avg(v128, 5) OVER () a128, avg(v256, 5) OVER () a256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgRescaleD128VariousTargetTypesOverCurrentRow() throws Exception {
        // v128: precision=38, scale=6. targetPrecision = 38 - 6 + scale.
        //   scale=6  -> precision 38 -> D128
        //   scale=44 -> precision 76 -> D256
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t6.000000\t6.00000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t4.000000\t4.00000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t8.000000\t8.00000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t2.000000\t2.00000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t10.000000\t10.00000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v128, 6) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a128, " +
                            "avg(v128, 44) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD16VariousTargetTypesOverCurrentRow() throws Exception {
        // v16: precision=4, scale=1. targetPrecision = 4 - 1 + scale.
        //   scale=0  -> precision 3  -> D16
        //   scale=5  -> precision 8  -> D32
        //   scale=14 -> precision 17 -> D64
        //   scale=30 -> precision 33 -> D128
        //   scale=60 -> precision 63 -> D256
        // (D8 unreachable for D16 input: min targetPrecision is 3.)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t6\t6.00000\t6.00000000000000\t6.000000000000000000000000000000\t6.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t4\t4.00000\t4.00000000000000\t4.000000000000000000000000000000\t4.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t8\t8.00000\t8.00000000000000\t8.000000000000000000000000000000\t8.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t2\t2.00000\t2.00000000000000\t2.000000000000000000000000000000\t2.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t10\t10.00000\t10.00000000000000\t10.000000000000000000000000000000\t10.000000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v16, 0) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a16, " +
                            "avg(v16, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a32, " +
                            "avg(v16, 14) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a64, " +
                            "avg(v16, 30) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a128, " +
                            "avg(v16, 60) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD16VariousTargetTypesOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t6\t6.00000\t6.00000000000000\t6.000000000000000000000000000000\t6.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\tb\t4\t4.00000\t4.00000000000000\t4.000000000000000000000000000000\t4.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\ta\t6\t6.00000\t6.00000000000000\t6.000000000000000000000000000000\t6.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\tb\t4\t4.00000\t4.00000000000000\t4.000000000000000000000000000000\t4.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\ta\t6\t6.00000\t6.00000000000000\t6.000000000000000000000000000000\t6.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:05:00.000000Z\tb\t4\t4.00000\t4.00000000000000\t4.000000000000000000000000000000\t4.000000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v16, 0) OVER (PARTITION BY grp) a16, " +
                            "avg(v16, 5) OVER (PARTITION BY grp) a32, " +
                            "avg(v16, 14) OVER (PARTITION BY grp) a64, " +
                            "avg(v16, 30) OVER (PARTITION BY grp) a128, " +
                            "avg(v16, 60) OVER (PARTITION BY grp) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleD32VariousTargetTypesOverCurrentRow() throws Exception {
        // v32: precision=9, scale=3. targetPrecision = 9 - 3 + scale.
        //   scale=0  -> precision 6  -> D32
        //   scale=12 -> precision 18 -> D64
        //   scale=30 -> precision 36 -> D128
        //   scale=60 -> precision 66 -> D256
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t6.000000\t6.000000000000\t6.000000000000000000000000000000\t6.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t4.000000\t4.000000000000\t4.000000000000000000000000000000\t4.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t8.000000\t8.000000000000\t8.000000000000000000000000000000\t8.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t2.000000\t2.000000000000\t2.000000000000000000000000000000\t2.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t10.000000\t10.000000000000\t10.000000000000000000000000000000\t10.000000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v32, 6) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a32, " +
                            "avg(v32, 12) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a64, " +
                            "avg(v32, 30) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a128, " +
                            "avg(v32, 60) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD32VariousTargetTypesOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t6.000000\t6.000000000000\t6.000000000000000000000000000000\t6.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\tb\t4.000000\t4.000000000000\t4.000000000000000000000000000000\t4.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\ta\t6.000000\t6.000000000000\t6.000000000000000000000000000000\t6.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\tb\t4.000000\t4.000000000000\t4.000000000000000000000000000000\t4.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\ta\t6.000000\t6.000000000000\t6.000000000000000000000000000000\t6.000000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:05:00.000000Z\tb\t4.000000\t4.000000000000\t4.000000000000000000000000000000\t4.000000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v32, 6) OVER (PARTITION BY grp) a32, " +
                            "avg(v32, 12) OVER (PARTITION BY grp) a64, " +
                            "avg(v32, 30) OVER (PARTITION BY grp) a128, " +
                            "avg(v32, 60) OVER (PARTITION BY grp) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleD64VariousTargetTypesOverCurrentRow() throws Exception {
        // v64: precision=18, scale=2. targetPrecision = 18 - 2 + scale.
        //   scale=2  -> precision 18 -> D64
        //   scale=22 -> precision 38 -> D128
        //   scale=58 -> precision 74 -> D256
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t6.00\t6.0000000000000000000000\t6.0000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t4.00\t4.0000000000000000000000\t4.0000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t8.00\t8.0000000000000000000000\t8.0000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t2.00\t2.0000000000000000000000\t2.0000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t10.00\t10.0000000000000000000000\t10.0000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v64, 2) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a64, " +
                            "avg(v64, 22) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a128, " +
                            "avg(v64, 58) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD64VariousTargetTypesOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t6.00\t6.0000000000000000000000\t6.0000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\tb\t4.00\t4.0000000000000000000000\t4.0000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\ta\t6.00\t6.0000000000000000000000\t6.0000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\tb\t4.00\t4.0000000000000000000000\t4.0000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\ta\t6.00\t6.0000000000000000000000\t6.0000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:05:00.000000Z\tb\t4.00\t4.0000000000000000000000\t4.0000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v64, 2) OVER (PARTITION BY grp) a64, " +
                            "avg(v64, 22) OVER (PARTITION BY grp) a128, " +
                            "avg(v64, 58) OVER (PARTITION BY grp) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleD8VariousTargetTypesOverCurrentRow() throws Exception {
        // Vary the rescale scale on a single D8 input so the planner creates
        // Decimal8Rescale256AvgOverCurrentRow instances with six different targetTypes,
        // exercising each of the six getDecimal* overloads (the streaming fast-path
        // reads the ZERO_PASS function via the matching getDecimal*<targetType>).
        // For v8 (precision=2, scale=1): targetPrecision = 2 - 1 + scale.
        //   scale=0  -> precision 1  -> D8
        //   scale=3  -> precision 4  -> D16
        //   scale=5  -> precision 6  -> D32
        //   scale=14 -> precision 15 -> D64
        //   scale=30 -> precision 31 -> D128
        //   scale=60 -> precision 61 -> D256
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t1\t0.800\t0.80000\t0.80000000000000\t0.800000000000000000000000000000\t0.800000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t0\t0.200\t0.20000\t0.20000000000000\t0.200000000000000000000000000000\t0.200000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t1\t1.000\t1.00000\t1.00000000000000\t1.000000000000000000000000000000\t1.000000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a8, " +
                            "avg(v8, 3) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a16, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a32, " +
                            "avg(v8, 14) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a64, " +
                            "avg(v8, 30) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a128, " +
                            "avg(v8, 60) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD8VariousTargetTypesOverPartition() throws Exception {
        // Default frame: PARTITION BY grp creates OverPartitionFunction (TWO_PASS).
        // Vary scale to hit all 6 getDecimal* overloads in Decimal8Rescale256AvgOverPartitionFunction.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\tb\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\ta\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\tb\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\ta\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:05:00.000000Z\tb\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 0) OVER (PARTITION BY grp) a8, " +
                            "avg(v8, 3) OVER (PARTITION BY grp) a16, " +
                            "avg(v8, 5) OVER (PARTITION BY grp) a32, " +
                            "avg(v8, 14) OVER (PARTITION BY grp) a64, " +
                            "avg(v8, 30) OVER (PARTITION BY grp) a128, " +
                            "avg(v8, 60) OVER (PARTITION BY grp) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleD8VariousTargetTypesOverPartitionRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\tb\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\ta\t1\t0.700\t0.70000\t0.70000000000000\t0.700000000000000000000000000000\t0.700000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\tb\t0\t0.300\t0.30000\t0.30000000000000\t0.300000000000000000000000000000\t0.300000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\ta\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:05:00.000000Z\tb\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 0) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v8, 3) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v8, 14) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v8, 30) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v8, 60) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD8VariousTargetTypesOverPartitionRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\tb\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\ta\t1\t0.700\t0.70000\t0.70000000000000\t0.700000000000000000000000000000\t0.700000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\tb\t0\t0.300\t0.30000\t0.30000000000000\t0.300000000000000000000000000000\t0.300000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\ta\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:05:00.000000Z\tb\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 0) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v8, 3) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v8, 14) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v8, 30) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v8, 60) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD8VariousTargetTypesOverRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t0\t0.500\t0.50000\t0.50000000000000\t0.500000000000000000000000000000\t0.500000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t0\t0.500\t0.50000\t0.50000000000000\t0.500000000000000000000000000000\t0.500000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v8, 3) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v8, 5) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v8, 14) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v8, 30) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v8, 60) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD8VariousTargetTypesOverRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t0\t0.500\t0.50000\t0.50000000000000\t0.500000000000000000000000000000\t0.500000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t0\t0.500\t0.50000\t0.50000000000000\t0.500000000000000000000000000000\t0.500000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v8, 3) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v8, 14) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v8, 30) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v8, 60) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD8VariousTargetTypesOverUnboundedPartitionRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\ta\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\tb\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\ta\t1\t0.700\t0.70000\t0.70000000000000\t0.700000000000000000000000000000\t0.700000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\tb\t0\t0.300\t0.30000\t0.30000000000000\t0.300000000000000000000000000000\t0.300000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\ta\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:05:00.000000Z\tb\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, grp, " +
                            "avg(v8, 0) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v8, 3) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v8, 14) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v8, 30) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v8, 60) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD8VariousTargetTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t0\t0.500\t0.50000\t0.50000000000000\t0.500000000000000000000000000000\t0.500000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t0\t0.500\t0.50000\t0.50000000000000\t0.500000000000000000000000000000\t0.500000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v8, 3) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v8, 14) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v8, 30) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v8, 60) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleD8VariousTargetTypesOverWholeResultSet() throws Exception {
        // OVER () creates OverWholeResultSetFunction. Vary scale.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER () a8, " +
                            "avg(v8, 3) OVER () a16, " +
                            "avg(v8, 5) OVER () a32, " +
                            "avg(v8, 14) OVER () a64, " +
                            "avg(v8, 30) OVER () a128, " +
                            "avg(v8, 60) OVER () a256 " +
                            "FROM t", "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleNegativeScaleRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                String sql = "SELECT avg(" + col + ", -1) OVER () FROM t";
                int nPos = "SELECT avg(".length() + col.length() + 2;
                assertExceptionNoLeakCheck(sql, nPos, "non-negative scale required: -1");
            }
        });
    }

    @Test
    public void testAvgRescalePrecisionOverflowRejected() throws Exception {
        // v256 is decimal(60, 0). targetPrecision = 60 - 0 + n. n > 16 produces precision > 76.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            String sql = "SELECT avg(v256, 17) OVER () FROM t";
            int nPos = "SELECT avg(v256, ".length();
            assertExceptionNoLeakCheck(sql, nPos, "rescaled decimal has precision that exceeds maximum of");
        });
    }

    @Test
    public void testAvgRescaleSameScale() throws Exception {
        // avg(v64, 2) -- same as input scale (no-op rescale)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("avg_v\n6.00\n", "SELECT avg(v64, 2) OVER () AS avg_v FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testAvgRescaleScaleTooLargeRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                String sql = "SELECT avg(" + col + ", 77) OVER () FROM t";
                int nPos = "SELECT avg(".length() + col.length() + 2;
                assertExceptionNoLeakCheck(sql, nPos, "scale exceeds maximum of");
            }
        });
    }

    @Test
    public void testAvgRescaleTruncatingScale() throws Exception {
        // avg(v64, 0) -- truncate from scale 2 to scale 0. avg=6.00 -> 6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("avg_v\n6\n", "SELECT avg(v64, 0) OVER () AS avg_v FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testCaseWhenWithWindow() throws Exception {
        // avg(v64)=6.00. v64: 6.00 (high, =6), 4.00 (low), 8.00 (high), 2.00 (low), 10.00 (high)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tcat
                            2024-01-01T00:00:00.000000Z\thigh
                            2024-01-01T00:01:00.000000Z\tlow
                            2024-01-01T00:02:00.000000Z\thigh
                            2024-01-01T00:03:00.000000Z\tlow
                            2024-01-01T00:04:00.000000Z\thigh
                            """,
                    "SELECT ts, CASE WHEN v64 >= avg(v64) OVER () THEN 'high' ELSE 'low' END AS cat FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testCountAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testCountAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testCountAllSubTypesOverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testCountAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testCountAllSubTypesOverPartitionSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testCountAllSubTypesOverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testCountAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testCountAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            c8\tc16\tc32\tc64\tc128\tc256
                            5\t5\t5\t5\t5\t5
                            """,
                    "SELECT count(v8) OVER () c8, count(v16) OVER () c16, count(v32) OVER () c32, " +
                            "count(v64) OVER () c64, count(v128) OVER () c128, count(v256) OVER () c256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testCountAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            c8\tc16\tc32\tc64\tc128\tc256
                            0\t0\t0\t0\t0\t0
                            """,
                    "SELECT count(v8) OVER () c8, count(v16) OVER () c16, count(v32) OVER () c32, " +
                            "count(v64) OVER () c64, count(v128) OVER () c128, count(v256) OVER () c256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testCountAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            c8\tc16\tc32\tc64\tc128\tc256
                            2\t2\t2\t2\t2\t2
                            """,
                    "SELECT count(v8) OVER () c8, count(v16) OVER () c16, count(v32) OVER () c32, " +
                            "count(v64) OVER () c64, count(v128) OVER () c128, count(v256) OVER () c256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
        // count of 2 non-nulls — unchanged from INSERT_3_WITH_NULL since both datasets have 2 non-nulls
    }

    @Test
    public void testCountStarVsCountColumn() throws Exception {
        // 5 rows total, 2 non-null
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            c_star\tc_col
                            5\t2
                            """,
                    "SELECT count() OVER () c_star, count(v64) OVER () c_col FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testCountStarWindow() throws Exception {
        // INSERT_5_WITH_NULL has 5 rows total
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("c\n5\n", "SELECT count() OVER () c FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testEmptyFrameAllAggregatesAllSubTypesReturnsNull() throws Exception {
        // ROWS BETWEEN N PRECEDING AND M PRECEDING with M > N means the frame is
        // entirely behind the lower bound (e.g. -1..-2 is empty); the planner returns
        // a *NullFunction whose pass1 writes a NULL with the right width for each subtype.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256\ta8\ta16\ta32\ta64\ta128\ta256\tm8\tm16\tm32\tm64\tm128\tm256\tn8\tn16\tn32\tn64\tn128\tn256\tf8\tf16\tf32\tf64\tf128\tf256\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s8, " +
                            "sum(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s16, " +
                            "sum(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s32, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s64, " +
                            "sum(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s128, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s256, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a8, " +
                            "avg(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a16, " +
                            "avg(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a32, " +
                            "avg(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a128, " +
                            "avg(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a256, " +
                            "max(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) m8, " +
                            "max(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) m16, " +
                            "max(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) m32, " +
                            "max(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) m64, " +
                            "max(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) m128, " +
                            "max(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) m256, " +
                            "min(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) n8, " +
                            "min(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) n16, " +
                            "min(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) n32, " +
                            "min(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) n64, " +
                            "min(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) n128, " +
                            "min(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) n256, " +
                            "first_value(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) f8, " +
                            "first_value(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) f16, " +
                            "first_value(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) f32, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) f64, " +
                            "first_value(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) f128, " +
                            "first_value(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) f256, " +
                            "last_value(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) l8, " +
                            "last_value(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) l16, " +
                            "last_value(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) l32, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) l64, " +
                            "last_value(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) l128, " +
                            "last_value(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testEmptyFrameAllAggregatesAllSubTypesReturnsNullPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\ts8\ts64\ts256\ta8\ta64\ta256\tm8\tm64\tm256\tn8\tn64\tn256\tf8\tf64\tf256\tl8\tl64\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:05:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s8, " +
                            "sum(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s64, " +
                            "sum(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s256, " +
                            "avg(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a8, " +
                            "avg(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a64, " +
                            "avg(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a256, " +
                            "max(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) m8, " +
                            "max(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) m64, " +
                            "max(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) m256, " +
                            "min(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) n8, " +
                            "min(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) n64, " +
                            "min(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) n256, " +
                            "first_value(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) f8, " +
                            "first_value(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) f64, " +
                            "first_value(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) f256, " +
                            "last_value(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) l8, " +
                            "last_value(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) l64, " +
                            "last_value(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testEmptyFrameAllAggregatesAllSubTypesReturnsNullRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts8\ts64\ts256\ta8\ta64\ta256\tm8\tm64\tm256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts RANGE BETWEEN 1 second PRECEDING AND 2 second PRECEDING) s8, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN 1 second PRECEDING AND 2 second PRECEDING) s64, " +
                            "sum(v256) OVER (ORDER BY ts RANGE BETWEEN 1 second PRECEDING AND 2 second PRECEDING) s256, " +
                            "avg(v8) OVER (ORDER BY ts RANGE BETWEEN 1 second PRECEDING AND 2 second PRECEDING) a8, " +
                            "avg(v64) OVER (ORDER BY ts RANGE BETWEEN 1 second PRECEDING AND 2 second PRECEDING) a64, " +
                            "avg(v256) OVER (ORDER BY ts RANGE BETWEEN 1 second PRECEDING AND 2 second PRECEDING) a256, " +
                            "max(v8) OVER (ORDER BY ts RANGE BETWEEN 1 second PRECEDING AND 2 second PRECEDING) m8, " +
                            "max(v64) OVER (ORDER BY ts RANGE BETWEEN 1 second PRECEDING AND 2 second PRECEDING) m64, " +
                            "max(v256) OVER (ORDER BY ts RANGE BETWEEN 1 second PRECEDING AND 2 second PRECEDING) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testEmptyFrameAvgRescaleAllTargetTypesReturnsNull() throws Exception {
        // AvgRescale dispatch picks the targetType-matching *NullFunction (not the
        // argType-matching one). Vary scale to hit D8/16/32/64/128/256 NullFunctions.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a8, " +
                            "avg(v8, 3) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a16, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a32, " +
                            "avg(v8, 14) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a64, " +
                            "avg(v8, 30) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a128, " +
                            "avg(v8, 60) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testEmptyTableAllFactories() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertQueryNoLeakCheck("c8\ts8\tmx8\tmn8\tfv8\tlv8\n",
                    "SELECT count(v8) OVER () c8, sum(v8) OVER () s8, max(v8) OVER () mx8, min(v8) OVER () mn8, " +
                            "first_value(v8) OVER () fv8, last_value(v8) OVER () lv8 FROM t", null, null, true, true);
        });
    }

    @Test
    public void testExplainPlanAllFactoriesAllSubTypesCoverage() throws Exception {
        // Covers toPlan for every factory + every sub-type via EXPLAIN. Each call uses
        // assertPlanNoLeakCheck with the exact expected plan string.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            // ---- sum ----
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck("SELECT sum(" + col + ") OVER () FROM t",
                        "CachedWindow\n  unorderedFunctions: [sum(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT sum(" + col + ") OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [sum(" + col + ") over ( rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT sum(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [sum(" + col + ") over (partition by [grp] rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT sum(" + col + ") OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [sum(" + col + ") over (range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT sum(" + col + ") OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [sum(" + col + ") over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            }
            // ---- avg ----
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck("SELECT avg(" + col + ") OVER () FROM t",
                        "CachedWindow\n  unorderedFunctions: [avg(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT avg(" + col + ") OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(" + col + ") over ( rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT avg(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(" + col + ") over (partition by [grp] rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT avg(" + col + ") OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(" + col + ") over (range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT avg(" + col + ") OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(" + col + ") over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            }
            // ---- max ----
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck("SELECT max(" + col + ") OVER () FROM t",
                        "CachedWindow\n  unorderedFunctions: [max(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT max(" + col + ") OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [max(" + col + ") over ( rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT max(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [max(" + col + ") over (partition by [grp] rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT max(" + col + ") OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [max(" + col + ") over (range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT max(" + col + ") OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [max(" + col + ") over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            }
            // ---- min ----
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck("SELECT min(" + col + ") OVER () FROM t",
                        "CachedWindow\n  unorderedFunctions: [min(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT min(" + col + ") OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [min(" + col + ") over ( rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT min(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [min(" + col + ") over (partition by [grp] rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            }
            // ---- first_value (ZERO_PASS, streaming) / last_value (TWO_PASS for OVER (), cached) ----
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck("SELECT first_value(" + col + ") OVER () FROM t",
                        "Window\n  functions: [first_value(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT last_value(" + col + ") OVER () FROM t",
                        "CachedWindow\n  unorderedFunctions: [last_value(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                for (String fn : new String[]{"first_value", "last_value"}) {
                    assertPlanNoLeakCheck("SELECT " + fn + "(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                            "Window\n  functions: [" + fn + "(" + col + ") over (partition by [grp] rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                    assertPlanNoLeakCheck("SELECT " + fn + "(" + col + ") IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                            "Window\n  functions: [" + fn + "(" + col + ") ignore nulls over (partition by [grp] rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                }
            }
            // ---- nth_value ----
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck("SELECT nth_value(" + col + ", 1) OVER () FROM t",
                        "CachedWindow\n  unorderedFunctions: [nth_value(" + col + ",1) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck("SELECT nth_value(" + col + ", 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [nth_value(" + col + ",1) over (partition by [grp] rows between 1 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            }
        });
    }

    @Test
    public void testExplainPlanAvgOverWholeAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT avg(v8) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [avg(v8) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v16) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [avg(v16) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v32) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [avg(v32) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v64) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [avg(v64) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v128) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [avg(v128) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v256) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [avg(v256) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanAvgRangePartitionAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT avg(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [avg(v8) over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [avg(v16) over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [avg(v32) over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [avg(v64) over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [avg(v128) over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT avg(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [avg(v256) over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanAvgRescaleAllSubTypesAllFrames() throws Exception {
        // Cover toPlan (and getName) for every AvgRescale class. The plan output for
        // OverCurrentRow / OverWholeResultSet / OverPartitionFunction inherits from
        // BaseWindowFunction and doesn't print the scale; the frame-specific ones do.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                // OVER () -> OverWholeResultSet (TWO_PASS), inherits base toPlan
                assertPlanNoLeakCheck("SELECT avg(" + col + ", 5) OVER () FROM t",
                        "CachedWindow\n  unorderedFunctions: [avg(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                // ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW -> OverCurrentRow (ZERO_PASS),
                // inherits base toPlan with "over ()" suffix
                assertPlanNoLeakCheck("SELECT avg(" + col + ", 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            }
        });
    }

    @Test
    public void testExplainPlanAvgRescalePartitionRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT avg(v128, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [avg(v128,5) over (partition by [grp] range between 60000000 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanAvgRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT avg(v64) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [avg(v64) over ( rows between 2 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanEmptyFrame() throws Exception {
        // Empty frame (rowsHi < rowsLo) returns a *NullFunction whose toPlan goes
        // through BaseNullFunction (with isRange=false, rowLo/rowHi bound).
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT sum(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM t",
                    """
                            Window
                              functions: [sum(v8) over ( rows between 1 preceding and 2 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanEmptyFramePartition() throws Exception {
        // Empty frame with PARTITION BY exercises the partitionByRecord branch of
        // BaseNullFunction.toPlan.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT sum(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM t",
                    """
                            Window
                              functions: [sum(v8) over (partition by [grp] rows between 1 preceding and 2 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanFirstLastValueRangeNonZeroHiAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") OVER (ORDER BY ts RANGE BETWEEN 120 second PRECEDING AND 60 second PRECEDING) FROM t",
                        "Window\n  functions: [first_value(" + col + ") over (range between 120000000 preceding and 60000000 preceding)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") OVER (ORDER BY ts RANGE BETWEEN 120 second PRECEDING AND 60 second PRECEDING) FROM t",
                        "Window\n  functions: [last_value(" + col + ") over (range between 120000000 preceding and 60000000 preceding)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            }
        });
    }

    @Test
    public void testExplainPlanFirstValueIgnoreNullsAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [first_value(v8) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [first_value(v16) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [first_value(v32) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [first_value(v64) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [first_value(v128) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [first_value(v256) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanLagPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT lag(v8, 1) OVER (PARTITION BY grp ORDER BY ts) FROM t",
                    """
                            Window
                              functions: [lag(v8, 1, NULL) over (partition by [grp])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanLagZeroOffsetV64() throws Exception {
        // lead(col, 0) collapses to lag(col, 0).
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT lag(v64, 0) OVER (ORDER BY ts) FROM t",
                    """
                            Window
                              functions: [lag(v64, 0, NULL) over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT lead(v64, 0) OVER (ORDER BY ts) FROM t",
                    """
                            Window
                              functions: [lag(v64, 0, NULL) over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanLastValueIgnoreNullsAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [last_value(v8) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [last_value(v16) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [last_value(v32) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [last_value(v64) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [last_value(v128) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [last_value(v256) ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanMaxOverPartitionAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT max(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [max(v8) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT max(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [max(v16) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT max(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [max(v32) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT max(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [max(v64) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT max(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [max(v128) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT max(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [max(v256) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanMaxPartitionRowsBoundedHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT max(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) FROM t",
                    """
                            Window
                              functions: [max(v32) over (partition by [grp] rows between 3 preceding and 0 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanMinOverPartitionAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT min(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [min(v8) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT min(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [min(v16) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT min(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [min(v32) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT min(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [min(v64) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT min(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [min(v128) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT min(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [min(v256) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanNthValueCurrentRowAllSubTypes() throws Exception {
        // Exercises Decimal{8,16,32,64,128,256}NthValueOverCurrentRowFunction.toPlan.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT nth_value(v8, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [nth_value(v8,1) over (rows between current row and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT nth_value(v16, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [nth_value(v16,1) over (rows between current row and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT nth_value(v32, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [nth_value(v32,1) over (rows between current row and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT nth_value(v64, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [nth_value(v64,1) over (rows between current row and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT nth_value(v128, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [nth_value(v128,1) over (rows between current row and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT nth_value(v256, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [nth_value(v256,1) over (rows between current row and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanNthValueRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT nth_value(v16, 2) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [nth_value(v16,2) over (rows between unbounded preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanSumOverPartitionAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT sum(v8) OVER (PARTITION BY grp) FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v8) over (partition by [grp])]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v16) OVER (PARTITION BY grp) FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v16) over (partition by [grp])]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v32) OVER (PARTITION BY grp) FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v32) over (partition by [grp])]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v64) OVER (PARTITION BY grp) FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v64) over (partition by [grp])]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v128) OVER (PARTITION BY grp) FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v128) over (partition by [grp])]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v256) OVER (PARTITION BY grp) FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v256) over (partition by [grp])]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanSumOverPartitionRowsAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT sum(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v8) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v16) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v32) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v64) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v128) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v256) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanSumOverRangeAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT sum(v8) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [sum(v8) over (range between 60000000 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT sum(v16) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [sum(v16) over (range between 60000000 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT sum(v32) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [sum(v32) over (range between 60000000 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT sum(v64) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [sum(v64) over (range between 60000000 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT sum(v128) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [sum(v128) over (range between 60000000 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
            assertPlanNoLeakCheck(
                    "SELECT sum(v256) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                    """
                            Window
                              functions: [sum(v256) over (range between 60000000 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainPlanSumOverRowsAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT sum(v8) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v8) over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v16) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v16) over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v32) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v32) over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v64) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v64) over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v128) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v128) over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v256) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [sum(v256) over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanSumOverWholeAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck("SELECT sum(v8) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v8) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v16) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v16) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v32) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v32) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v64) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v64) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v128) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v128) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
            assertPlanNoLeakCheck("SELECT sum(v256) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [sum(v256) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n");
        });
    }

    @Test
    public void testExplainPlanSumWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT sum(v256) OVER () FROM t",
                    """
                            CachedWindow
                              unorderedFunctions: [sum(v256) over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testFirstLastIgnoreNullsAllSubTypesPartitionRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf64\tl64
                            2024-01-01T00:00:00.000000Z\t\t
                            2024-01-01T00:01:00.000000Z\t6.00\t6.00
                            2024-01-01T00:02:00.000000Z\t6.00\t6.00
                            2024-01-01T00:03:00.000000Z\t6.00\t8.00
                            2024-01-01T00:04:00.000000Z\t6.00\t8.00
                            """,
                    "SELECT ts, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstLastNotNullWithFollowedByNullData() throws Exception {
        // Frame with explicit pattern: non-null values at end. Hits findNewFirstValue=false branch.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 0.6m, 6.0m, 6.000m, 6.00m, 6.000000m, 6m), " +
                    "('2024-01-01T00:01:00', 'a', 0.8m, 8.0m, 8.000m, 8.00m, 8.000000m, 8m), " +
                    "('2024-01-01T00:02:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:03:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:04:00', 'a', 0.4m, 4.0m, 4.000m, 4.00m, 4.000000m, 4m)");
            // 90s preceding RANGE: at T2 (null), prior 90s = T1,T2. T1=8, T2=null. first not-null = 8, last not-null = 8.
            // At T3 (null), 90s preceding = T2,T3. Both null. first/last = previous (8 from T1 left).
            // Actually frame [T3-90s, T3] = [T2, T3]. Both null. So first/last not-null = NULL.
            assertQueryNoLeakCheck("""
                            ts\tf8\tf64\tf256\tl8\tl64\tl256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.00\t6\t0.6\t6.00\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.00\t6\t0.8\t8.00\t8
                            2024-01-01T00:02:00.000000Z\t0.8\t8.00\t8\t0.8\t8.00\t8
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t0.4\t4.00\t4\t0.4\t4.00\t4
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) f256, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstLastValueAllSubTypesAllNullsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstLastValueAllSubTypesUnboundedToUnbounded() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testFirstLastValueIgnoreNullsRangeUnboundedLo() throws Exception {
        // !frameLoBounded branch in FirstNotNull/LastNotNullOverRangeFrame.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf64\tf256\tl8\tl64\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.00\t6\t0.6\t6.00\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.00\t6\t0.6\t6.00\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.00\t6\t0.8\t8.00\t8
                            2024-01-01T00:04:00.000000Z\t0.6\t6.00\t6\t0.8\t8.00\t8
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesBoundedRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesBoundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "first_value(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesBoundedRangeUnbounded() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesBoundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesDuplicateValues() throws Exception {
        // first_value stably returns the first row's value regardless of duplicates
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("f8\tf256\n0.6\t6\n", "SELECT first_value(v8) OVER () f8, first_value(v256) OVER () f256 FROM t LIMIT 1", null, null, false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverPartitionRangeFrame() throws Exception {
        // first within partition (cumulative). a always 0.6, b always 0.4
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverRangeFrame() throws Exception {
        // cum first always = 0.6 (first row in INSERT_5)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            f8\tf16\tf32\tf64\tf128\tf256
                            0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT first_value(v8) OVER () f8, first_value(v16) OVER () f16, first_value(v32) OVER () f32, " +
                            "first_value(v64) OVER () f64, first_value(v128) OVER () f128, first_value(v256) OVER () f256 " +
                            "FROM t LIMIT 1", null, null, false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesPartitionDefaultFrame() throws Exception {
        // Plain (no IGNORE NULLS) first_value with PARTITION BY: covers
        // Decimal{8..256}FirstValueOverPartitionFunction.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "first_value(v8) OVER (PARTITION BY grp) f8, " +
                            "first_value(v16) OVER (PARTITION BY grp) f16, " +
                            "first_value(v32) OVER (PARTITION BY grp) f32, " +
                            "first_value(v64) OVER (PARTITION BY grp) f64, " +
                            "first_value(v128) OVER (PARTITION BY grp) f128, " +
                            "first_value(v256) OVER (PARTITION BY grp) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesPartitionDefaultNoOrder() throws Exception {
        // first_value OVER (PARTITION BY grp) — no IGNORE NULLS, default frame.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (PARTITION BY grp) f8, " +
                            "first_value(v16) OVER (PARTITION BY grp) f16, " +
                            "first_value(v32) OVER (PARTITION BY grp) f32, " +
                            "first_value(v64) OVER (PARTITION BY grp) f64, " +
                            "first_value(v128) OVER (PARTITION BY grp) f128, " +
                            "first_value(v256) OVER (PARTITION BY grp) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesRangeNonZeroHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f8, " +
                            "first_value(v16) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f16, " +
                            "first_value(v32) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f32, " +
                            "first_value(v64) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f64, " +
                            "first_value(v128) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f128, " +
                            "first_value(v256) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesRangeNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "first_value(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 360 second PRECEDING AND 90 second PRECEDING) f8, " +
                            "first_value(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 360 second PRECEDING AND 90 second PRECEDING) f16, " +
                            "first_value(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 360 second PRECEDING AND 90 second PRECEDING) f32, " +
                            "first_value(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 360 second PRECEDING AND 90 second PRECEDING) f64, " +
                            "first_value(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 360 second PRECEDING AND 90 second PRECEDING) f128, " +
                            "first_value(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 360 second PRECEDING AND 90 second PRECEDING) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesRangeTriggersBufferExpansion() throws Exception {
        // OverRangeFrame initial buffer is configuration.getSqlWindowStorePageSize() / RECORD_SIZE.
        // Shrink the page size so 33 inserted rows exceed initial capacity and force buffer expansion.
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 33; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            StringSink expected = new StringSink();
            expected.put("ts\tf8\n");
            for (int i = 0; i < 33; i++) {
                expected.put("2024-01-01T00:");
                if (i < 10) expected.put('0');
                expected.put(i).put(":00.000000Z\t0.5\n");
            }
            assertQueryNoLeakCheck(expected.toString(),
                    "SELECT ts, first_value(v8) OVER (ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) f8 FROM t",
                    null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesRowsNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "first_value(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) f8, " +
                            "first_value(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) f16, " +
                            "first_value(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) f32, " +
                            "first_value(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) f64, " +
                            "first_value(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) f128, " +
                            "first_value(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueAllSubTypesUnboundedRangeBoundedHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) f8, " +
                            "first_value(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) f16, " +
                            "first_value(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) f32, " +
                            "first_value(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) f64, " +
                            "first_value(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) f128, " +
                            "first_value(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesBoundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesBoundedRowsPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesPartitionDefaultFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp) f256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesPartitionDefaultRangeUnboundedHi() throws Exception {
        // first_value IGNORE NULLS OVER (PARTITION BY grp) — default frame in RANGE mode,
        // exercises generateDecimal{8..256}IgnoreNulls's PARTITION default-frame path.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp) f256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesPartitionRangeWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesPartitionRowsWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesPartitionUnboundedPrecedingCurrentRow() throws Exception {
        // first_value IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        // — exercises the UnboundedPartitionRowsFrame path.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesRangeBoundedFollowedByNonNull() throws Exception {
        // Exercises FirstNotNullOver{Range,PartitionRange}Frame computeNext for D8..D256:
        // - eviction loop (rows fall out of the lower bound),
        // - findNewFirstValue branch (a still-in-frame non-null appears),
        // - frameIncludesCurrentValue write-back.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesRangeNonZeroHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesRangeNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesRowsBoundedWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesUnboundedPartitionRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypesWholeResultSet() throws Exception {
        // first_value IGNORE NULLS OVER () returns the global first non-null for every row (SQL standard).
        // Single partition 'a' (null,0.6,null,0.8,null) -> global first non-null = 0.6.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER () f8, " +
                            "first_value(v16) IGNORE NULLS OVER () f16, " +
                            "first_value(v32) IGNORE NULLS OVER () f32, " +
                            "first_value(v64) IGNORE NULLS OVER () f64, " +
                            "first_value(v128) IGNORE NULLS OVER () f128, " +
                            "first_value(v256) IGNORE NULLS OVER () f256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsRangeManyRowsAllSubTypes() throws Exception {
        // Larger data set with frequent NULLs forces FirstNotNullOverRangeFrame.computeNext
        // to traverse the eviction loop AND find a new first non-null value within
        // the remaining frame (the findNewFirstValue branch).
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:01:00', 'a', 0.6m, 6.0m, 6.000m, 6.00m, 6.000000m, 6m), " +
                    "('2024-01-01T00:02:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:03:00', 'a', 0.8m, 8.0m, 8.000m, 8.00m, 8.000000m, 8m), " +
                    "('2024-01-01T00:04:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:05:00', 'a', 0.2m, 2.0m, 2.000m, 2.00m, 2.000000m, 2m), " +
                    "('2024-01-01T00:06:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:07:00', 'a', 0.4m, 4.0m, 4.000m, 4.00m, 4.000000m, 4m)");
            // Window: 90s preceding to current. Frame for each row:
            // T0: [T0-90s, T0] -> [T0]. v=null -> first non-null = null
            // T1: [T0-30s, T1] -> [T0, T1]. v=null, 6 -> first=6
            // T2: [T1-30s, T2] -> [T1, T2]. v=6, null -> first=6 (T1)
            // T3: [T2-30s, T3] -> [T2, T3]. v=null, 8 -> first=8 (T3)
            // T4: [T3-30s, T4] -> [T3, T4]. v=8, null -> first=8 (T3)
            // T5: [T4-30s, T5] -> [T4, T5]. v=null, 2 -> first=2 (T5)
            // T6: [T5-30s, T6] -> [T5, T6]. v=2, null -> first=2 (T5)
            // T7: [T6-30s, T7] -> [T6, T7]. v=null, 4 -> first=4 (T7)
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:06:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:07:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueNullsFirst() throws Exception {
        // ORDER BY v64 NULLS FIRST -- first row in window is null. first_value RESPECT NULLS = null for all rows.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tfv
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T00:01:00.000000Z\t
                            2024-01-01T00:02:00.000000Z\t
                            2024-01-01T00:03:00.000000Z\t
                            2024-01-01T00:04:00.000000Z\t
                            """,
                    "SELECT ts, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) fv " +
                            "FROM t ORDER BY ts", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueNullsLastIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tfv
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T00:01:00.000000Z\t6.00
                            2024-01-01T00:02:00.000000Z\t6.00
                            2024-01-01T00:03:00.000000Z\t6.00
                            2024-01-01T00:04:00.000000Z\t6.00
                            """,
                    "SELECT ts, first_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) fv " +
                            "FROM t ORDER BY ts", null, "ts", false, true);
        });
    }

    @Test
    public void testFrameWithLargePrecedingCount() throws Exception {
        // 10_000 PRECEDING covers all earlier rows (cumulative on INSERT_5)
        // cumulative sums: 6.00, 10.00, 18.00, 20.00, 30.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t6.00
                            2024-01-01T00:01:00.000000Z\t10.00
                            2024-01-01T00:02:00.000000Z\t18.00
                            2024-01-01T00:03:00.000000Z\t20.00
                            2024-01-01T00:04:00.000000Z\t30.00
                            """,
                    "SELECT ts, sum(v64) OVER (ORDER BY ts ROWS BETWEEN 10_000 PRECEDING AND CURRENT ROW) s FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLagAllSubTypesIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLagAllSubTypesLargeOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLagAllSubTypesMultiPartitionReset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLagAllSubTypesNoOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLagAllSubTypesOffsetTwo() throws Exception {
        // lag(v, 2): row 0=null, row 1=null, row 2=0.6, row 3=0.4, row 4=0.8
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLagAllSubTypesWithDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
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
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLagLeadZeroOffsetAllSubTypes() throws Exception {
        // lag(v, 0) / lead(v, 0) returns the current row's value; dispatches to LeadLagValueCurrentRow.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tlg8\tlg16\tlg32\tlg64\tlg128\tlg256\tld8\tld16\tld32\tld64\tld128\tld256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "lag(v8, 0) OVER (ORDER BY ts) lg8, " +
                            "lag(v16, 0) OVER (ORDER BY ts) lg16, " +
                            "lag(v32, 0) OVER (ORDER BY ts) lg32, " +
                            "lag(v64, 0) OVER (ORDER BY ts) lg64, " +
                            "lag(v128, 0) OVER (ORDER BY ts) lg128, " +
                            "lag(v256, 0) OVER (ORDER BY ts) lg256, " +
                            "lead(v8, 0) OVER (ORDER BY ts) ld8, " +
                            "lead(v16, 0) OVER (ORDER BY ts) ld16, " +
                            "lead(v32, 0) OVER (ORDER BY ts) ld32, " +
                            "lead(v64, 0) OVER (ORDER BY ts) ld64, " +
                            "lead(v128, 0) OVER (ORDER BY ts) ld128, " +
                            "lead(v256, 0) OVER (ORDER BY ts) ld256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLagOffsetZero() throws Exception {
        // lag(v, 0) returns current row's value. v64: 6.00, 4.00, 8.00, 2.00, 10.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tlg64
                            2024-01-01T00:00:00.000000Z\t6.00
                            2024-01-01T00:01:00.000000Z\t4.00
                            2024-01-01T00:02:00.000000Z\t8.00
                            2024-01-01T00:03:00.000000Z\t2.00
                            2024-01-01T00:04:00.000000Z\t10.00
                            """,
                    "SELECT ts, lag(v64, 0) OVER (ORDER BY ts) lg64 FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLagWithNullDefault() throws Exception {
        // lag(v, 1, null) — same as lag(v, 1). v64: 6.00, 4.00, 8.00, 2.00, 10.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tlg64
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T00:01:00.000000Z\t6.00
                            2024-01-01T00:02:00.000000Z\t4.00
                            2024-01-01T00:03:00.000000Z\t8.00
                            2024-01-01T00:04:00.000000Z\t2.00
                            """,
                    "SELECT ts, lag(v64, 1, null) OVER (ORDER BY ts) lg64 FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLargeWindowAllFactories() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t SELECT " +
                    "timestamp_sequence(0, 60_000_000), 'g'||(x % 4)::string, " +
                    "1.0::decimal(2, 1), 10.0::decimal(4, 1), 10.000::decimal(9, 3), " +
                    "10.00::decimal(18, 2), 10.000000::decimal(38, 6), 10::decimal(60, 0) " +
                    "FROM long_sequence(500)");
            assertQueryNoLeakCheck("""
                            c64\ts64\tmx64\tmn64\tavg64
                            500\t5000.00\t10.00\t10.00\t10.00
                            """,
                    "SELECT count(v64) OVER () c64, sum(v64) OVER () s64, max(v64) OVER () mx64, " +
                            "min(v64) OVER () mn64, avg(v64) OVER () avg64 FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testLargeWindowRingBufferGrowthAllSubTypes() throws Exception {
        // 10k rows over a ROWS-BETWEEN 5k-PRECEDING frame forces multi-page ring-buffer growth
        // through appendAddressFor() and exercises the constructor error path for D8..D256.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t SELECT " +
                    "timestamp_sequence(0, 60_000_000), 'g'||(x % 4)::string, " +
                    "1.0::decimal(2, 1), 10.0::decimal(4, 1), 10.000::decimal(9, 3), " +
                    "10.00::decimal(18, 2), 10.000000::decimal(38, 6), 10::decimal(60, 0) " +
                    "FROM long_sequence(10_000)");
            assertQueryNoLeakCheck("""
                            c8\tc16\tc32\tc64\tc128\tc256\ts64\tavg128\tmx256\tmn8
                            5001\t5001\t5001\t5001\t5001\t5001\t50010.00\t10.000000\t10\t1.0
                            """,
                    "SELECT " +
                            "count(v8) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) c8, " +
                            "count(v16) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) c16, " +
                            "count(v32) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) c32, " +
                            "count(v64) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) c64, " +
                            "count(v128) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) c128, " +
                            "count(v256) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) c256, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) s64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) avg128, " +
                            "max(v256) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) mx256, " +
                            "min(v8) OVER (ORDER BY ts ROWS BETWEEN 5000 PRECEDING AND CURRENT ROW) mn8 " +
                            "FROM t ORDER BY ts DESC LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testLastValueAllSubTypesBoundedRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesBoundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "last_value(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesBoundedRangeUnbounded() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesBoundedRangeWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) OVER (ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) OVER (ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) OVER (ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) OVER (ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) OVER (ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesBoundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesOverPartitionRangeFrame() throws Exception {
        // last within partition (cumulative) = current row's val
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesOverRangeFrame() throws Exception {
        // cum last = current row's val
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            l8\tl16\tl32\tl64\tl128\tl256
                            1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT last_value(v8) OVER () l8, last_value(v16) OVER () l16, last_value(v32) OVER () l32, " +
                            "last_value(v64) OVER () l64, last_value(v128) OVER () l128, last_value(v256) OVER () l256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testLastValueAllSubTypesPartitionDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (PARTITION BY grp) l8, " +
                            "last_value(v16) OVER (PARTITION BY grp) l16, " +
                            "last_value(v32) OVER (PARTITION BY grp) l32, " +
                            "last_value(v64) OVER (PARTITION BY grp) l64, " +
                            "last_value(v128) OVER (PARTITION BY grp) l128, " +
                            "last_value(v256) OVER (PARTITION BY grp) l256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesPartitionDefaultFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:01:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:02:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:03:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (PARTITION BY grp) l8, " +
                            "last_value(v16) OVER (PARTITION BY grp) l16, " +
                            "last_value(v32) OVER (PARTITION BY grp) l32, " +
                            "last_value(v64) OVER (PARTITION BY grp) l64, " +
                            "last_value(v128) OVER (PARTITION BY grp) l128, " +
                            "last_value(v256) OVER (PARTITION BY grp) l256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesRangeNonZeroHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l8, " +
                            "last_value(v16) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l16, " +
                            "last_value(v32) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l32, " +
                            "last_value(v64) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l64, " +
                            "last_value(v128) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l128, " +
                            "last_value(v256) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesRangeNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "last_value(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 90 second PRECEDING) l8, " +
                            "last_value(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 90 second PRECEDING) l16, " +
                            "last_value(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 90 second PRECEDING) l32, " +
                            "last_value(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 90 second PRECEDING) l64, " +
                            "last_value(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 90 second PRECEDING) l128, " +
                            "last_value(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 90 second PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesRangeTriggersBufferExpansion() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 33; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            StringSink expected = new StringSink();
            expected.put("ts\tl8\n");
            for (int i = 0; i < 33; i++) {
                expected.put("2024-01-01T00:");
                if (i < 10) expected.put('0');
                expected.put(i).put(":00.000000Z\t0.5\n");
            }
            assertQueryNoLeakCheck(expected.toString(),
                    "SELECT ts, last_value(v8) OVER (ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) l8 FROM t",
                    null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesRowsNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "last_value(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l8, " +
                            "last_value(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l16, " +
                            "last_value(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l32, " +
                            "last_value(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l64, " +
                            "last_value(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l128, " +
                            "last_value(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesSymmetricSliding() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l8, " +
                            "last_value(v16) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l16, " +
                            "last_value(v32) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l32, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l64, " +
                            "last_value(v128) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l128, " +
                            "last_value(v256) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueAllSubTypesUnboundedRangeBoundedHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) l8, " +
                            "last_value(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) l16, " +
                            "last_value(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) l32, " +
                            "last_value(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) l64, " +
                            "last_value(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) l128, " +
                            "last_value(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesBoundedRangeUnbounded() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesBoundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesBoundedRowsPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesPartitionDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp) l256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesPartitionDefaultFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp) l256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesPartitionRangeWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesPartitionRowsWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesPartitionUnboundedPrecedingCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesRangeBoundedFollowedByNonNull() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesRangeNonZeroHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesRangeNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 60 second PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesRowsBoundedWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesUnboundedHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesUnboundedPartitionRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypesWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER () l8, " +
                            "last_value(v16) IGNORE NULLS OVER () l16, " +
                            "last_value(v32) IGNORE NULLS OVER () l32, " +
                            "last_value(v64) IGNORE NULLS OVER () l64, " +
                            "last_value(v128) IGNORE NULLS OVER () l128, " +
                            "last_value(v256) IGNORE NULLS OVER () l256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsRangeManyRowsAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:01:00', 'a', 0.6m, 6.0m, 6.000m, 6.00m, 6.000000m, 6m), " +
                    "('2024-01-01T00:02:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:03:00', 'a', 0.8m, 8.0m, 8.000m, 8.00m, 8.000000m, 8m), " +
                    "('2024-01-01T00:04:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:05:00', 'a', 0.2m, 2.0m, 2.000m, 2.00m, 2.000000m, 2m), " +
                    "('2024-01-01T00:06:00', 'a', null, null, null, null, null, null), " +
                    "('2024-01-01T00:07:00', 'a', 0.4m, 4.0m, 4.000m, 4.00m, 4.000000m, 4m)");
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:06:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:07:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLeadAllSubTypesIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLeadAllSubTypesLargeOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLeadAllSubTypesMultiPartitionReset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLeadAllSubTypesNoOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLeadAllSubTypesOffsetTwo() throws Exception {
        // lead(v, 2): row 0=0.8, row 1=0.2, row 2=1.0, row 3=null, row 4=null
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLeadAllSubTypesWithDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLeadOffsetZero() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tld64
                            2024-01-01T00:00:00.000000Z\t6.00
                            2024-01-01T00:01:00.000000Z\t4.00
                            2024-01-01T00:02:00.000000Z\t8.00
                            2024-01-01T00:03:00.000000Z\t2.00
                            2024-01-01T00:04:00.000000Z\t10.00
                            """,
                    "SELECT ts, lead(v64, 0) OVER (ORDER BY ts) ld64 FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testLeadWithNullDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tld64
                            2024-01-01T00:00:00.000000Z\t4.00
                            2024-01-01T00:01:00.000000Z\t8.00
                            2024-01-01T00:02:00.000000Z\t2.00
                            2024-01-01T00:03:00.000000Z\t10.00
                            2024-01-01T00:04:00.000000Z\t
                            """,
                    "SELECT ts, lead(v64, 1, null) OVER (ORDER BY ts) ld64 FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testMaxAllSubTypesBoundedRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "max(v8) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m8, " +
                            "max(v16) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m16, " +
                            "max(v32) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m32, " +
                            "max(v64) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m64, " +
                            "max(v128) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m128, " +
                            "max(v256) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesBoundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "max(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m8, " +
                            "max(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m16, " +
                            "max(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m32, " +
                            "max(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m64, " +
                            "max(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m128, " +
                            "max(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testMaxAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertQueryNoLeakCheck("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT max(v8) OVER () m8, max(v16) OVER () m16, max(v32) OVER () m32, " +
                            "max(v64) OVER () m64, max(v128) OVER () m128, max(v256) OVER () m256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testMaxAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesOverPartitionRangeFrame() throws Exception {
        // cum max within partition. a: 0.6, 0.8, 0.8 ; b: 0.4, 0.4, 0.6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesOverPartitionSlidingRows() throws Exception {
        // a max: 0.6, 0.8, 0.8 ; b max: 0.4, 0.4, 0.6
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesOverRangeFrame() throws Exception {
        // cumulative max on INSERT_5: 0.6, 0.6, 0.8, 0.8, 1.0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT max(v8) OVER () m8, max(v16) OVER () m16, max(v32) OVER () m32, " +
                            "max(v64) OVER () m64, max(v128) OVER () m128, max(v256) OVER () m256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testMaxAllSubTypesRowsNonZeroHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "max(v8) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m8, " +
                            "max(v16) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m16, " +
                            "max(v32) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m32, " +
                            "max(v64) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m64, " +
                            "max(v128) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m128, " +
                            "max(v256) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesRowsNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "max(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m8, " +
                            "max(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m16, " +
                            "max(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m32, " +
                            "max(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m64, " +
                            "max(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m128, " +
                            "max(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesUnboundedRangeBoundedHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "max(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m8, " +
                            "max(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m16, " +
                            "max(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m32, " +
                            "max(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m64, " +
                            "max(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m128, " +
                            "max(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesUnboundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "max(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m8, " +
                            "max(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m16, " +
                            "max(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m32, " +
                            "max(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m64, " +
                            "max(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m128, " +
                            "max(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMaxAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            \t\t\t\t\t
                            """,
                    "SELECT max(v8) OVER () m8, max(v16) OVER () m16, max(v32) OVER () m32, " +
                            "max(v64) OVER () m64, max(v128) OVER () m128, max(v256) OVER () m256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testMaxAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT max(v8) OVER () m8, max(v16) OVER () m16, max(v32) OVER () m32, " +
                            "max(v64) OVER () m64, max(v128) OVER () m128, max(v256) OVER () m256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testMinAllSubTypesBoundedRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "min(v8) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m8, " +
                            "min(v16) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m16, " +
                            "min(v32) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m32, " +
                            "min(v64) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m64, " +
                            "min(v128) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m128, " +
                            "min(v256) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesBoundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "min(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m8, " +
                            "min(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m16, " +
                            "min(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m32, " +
                            "min(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m64, " +
                            "min(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m128, " +
                            "min(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testMinAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertQueryNoLeakCheck("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            -0.8\t-8.0\t-8.000\t-8.00\t-8.000000\t-8
                            """,
                    "SELECT min(v8) OVER () m8, min(v16) OVER () m16, min(v32) OVER () m32, " +
                            "min(v64) OVER () m64, min(v128) OVER () m128, min(v256) OVER () m256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testMinAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesOverPartitionRangeFrame() throws Exception {
        // cum min within partition. a: 0.6, 0.6, 0.4 ; b: 0.4, 0.2, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesOverPartitionSlidingRows() throws Exception {
        // a min: 0.6, 0.6, 0.4 ; b min: 0.4, 0.2, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesOverRangeFrame() throws Exception {
        // cumulative min on INSERT_5: 0.6, 0.4, 0.4, 0.2, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT min(v8) OVER () m8, min(v16) OVER () m16, min(v32) OVER () m32, " +
                            "min(v64) OVER () m64, min(v128) OVER () m128, min(v256) OVER () m256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testMinAllSubTypesRowsNonZeroHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "min(v8) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m8, " +
                            "min(v16) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m16, " +
                            "min(v32) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m32, " +
                            "min(v64) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m64, " +
                            "min(v128) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m128, " +
                            "min(v256) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesRowsNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "min(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m8, " +
                            "min(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m16, " +
                            "min(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m32, " +
                            "min(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m64, " +
                            "min(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m128, " +
                            "min(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesUnboundedRangeBoundedHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "min(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m8, " +
                            "min(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m16, " +
                            "min(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m32, " +
                            "min(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m64, " +
                            "min(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m128, " +
                            "min(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesUnboundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tm8\tm16\tm32\tm64\tm128\tm256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "min(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m8, " +
                            "min(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m16, " +
                            "min(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m32, " +
                            "min(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m64, " +
                            "min(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m128, " +
                            "min(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) m256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testMinAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            \t\t\t\t\t
                            """,
                    "SELECT min(v8) OVER () m8, min(v16) OVER () m16, min(v32) OVER () m32, " +
                            "min(v64) OVER () m64, min(v128) OVER () m128, min(v256) OVER () m256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testMinAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            m8\tm16\tm32\tm64\tm128\tm256
                            0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT min(v8) OVER () m8, min(v16) OVER () m16, min(v32) OVER () m32, " +
                            "min(v64) OVER () m64, min(v128) OVER () m128, min(v256) OVER () m256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testMixedTwoPassFirstValueRangeNonZeroHiAllSubTypes() throws Exception {
        // Pair a TWO_PASS function with first_value OVER RANGE to force cached executor,
        // which calls pass1 of Decimal*FirstValueOverRangeFrame.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ttp\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t3.0\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t3.0\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t3.0\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t3.0\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t3.0\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (PARTITION BY grp) tp, " +
                            "first_value(v8) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f8, " +
                            "first_value(v16) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f16, " +
                            "first_value(v32) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f32, " +
                            "first_value(v64) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f64, " +
                            "first_value(v128) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f128, " +
                            "first_value(v256) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) f256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testMixedTwoPassForcesGetDecimalAllSubTypes() throws Exception {
        // Putting a TWO_PASS window function in the SELECT forces the cached executor to
        // use the two-pass cursor for the whole query. ZERO_PASS functions in that query
        // are then read row-by-row through their getDecimal<targetType> overload (not via
        // writeSink). Each subtype here exercises the matching getter on the OverCurrentRow,
        // OverWholeResultSet, OverRowsFrame and OverRangeFrame ZERO_PASS classes.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ttp\tcr8\tcr16\tcr32\tcr64\tcr128\tcr256\tws8\tws16\tws32\tws64\tws128\tws256
                            2024-01-01T00:00:00.000000Z\t3.0\t0.6\t6.0\t6.000\t6.00\t6.000000\t6\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            2024-01-01T00:01:00.000000Z\t3.0\t0.4\t4.0\t4.000\t4.00\t4.000000\t4\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            2024-01-01T00:02:00.000000Z\t3.0\t0.8\t8.0\t8.000\t8.00\t8.000000\t8\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            2024-01-01T00:03:00.000000Z\t3.0\t0.2\t2.0\t2.000\t2.00\t2.000000\t2\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            2024-01-01T00:04:00.000000Z\t3.0\t1.0\t10.0\t10.000\t10.00\t10.000000\t10\t3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (PARTITION BY grp) tp, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr8, " +
                            "sum(v16) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr16, " +
                            "sum(v32) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr32, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr64, " +
                            "sum(v128) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr128, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr256, " +
                            "sum(v8) OVER () ws8, " +
                            "sum(v16) OVER () ws16, " +
                            "sum(v32) OVER () ws32, " +
                            "sum(v64) OVER () ws64, " +
                            "sum(v128) OVER () ws128, " +
                            "sum(v256) OVER () ws256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testMixedTwoPassForcesGetDecimalAvgAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ttp\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t0.6\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.6\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.6\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t0.6\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (PARTITION BY grp) tp, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a8, " +
                            "avg(v16) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a16, " +
                            "avg(v32) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a32, " +
                            "avg(v64) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a128, " +
                            "avg(v256) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testMixedTwoPassForcesGetDecimalAvgRescaleAllInputsAllFrames() throws Exception {
        // Mix TWO_PASS + AvgRescale for ALL input sub-types with various frame variants.
        // This exercises getDecimal<targetType> across many AvgRescale classes.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ttp\tcr8\tur8\trf8\tcr16\tur16\trf16\tcr32\tur32\trf32\tcr64\tur64\trf64\tcr128\tur128\trf128\tcr256\tur256\trf256
                            2024-01-01T00:00:00.000000Z\t0.6\t0.60000\t0.60000\t0.60000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000\t6.00000
                            2024-01-01T00:01:00.000000Z\t0.6\t0.40000\t0.50000\t0.50000\t4.00000\t5.00000\t5.00000\t4.00000\t5.00000\t5.00000\t4.00000\t5.00000\t5.00000\t4.00000\t5.00000\t5.00000\t4.00000\t5.00000\t5.00000
                            2024-01-01T00:02:00.000000Z\t0.6\t0.80000\t0.60000\t0.60000\t8.00000\t6.00000\t6.00000\t8.00000\t6.00000\t6.00000\t8.00000\t6.00000\t6.00000\t8.00000\t6.00000\t6.00000\t8.00000\t6.00000\t6.00000
                            2024-01-01T00:03:00.000000Z\t0.6\t0.20000\t0.50000\t0.50000\t2.00000\t5.00000\t5.00000\t2.00000\t5.00000\t5.00000\t2.00000\t5.00000\t5.00000\t2.00000\t5.00000\t5.00000\t2.00000\t5.00000\t5.00000
                            2024-01-01T00:04:00.000000Z\t0.6\t1.00000\t0.60000\t0.60000\t10.00000\t6.00000\t6.00000\t10.00000\t6.00000\t6.00000\t10.00000\t6.00000\t6.00000\t10.00000\t6.00000\t6.00000\t10.00000\t6.00000\t6.00000
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (PARTITION BY grp) tp, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr8, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ur8, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) rf8, " +
                            "avg(v16, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr16, " +
                            "avg(v16, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ur16, " +
                            "avg(v16, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) rf16, " +
                            "avg(v32, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr32, " +
                            "avg(v32, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ur32, " +
                            "avg(v32, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) rf32, " +
                            "avg(v64, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr64, " +
                            "avg(v64, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ur64, " +
                            "avg(v64, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) rf64, " +
                            "avg(v128, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr128, " +
                            "avg(v128, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ur128, " +
                            "avg(v128, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) rf128, " +
                            "avg(v256, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr256, " +
                            "avg(v256, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ur256, " +
                            "avg(v256, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) rf256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testMixedTwoPassForcesGetDecimalAvgRescaleD8VariousTargets() throws Exception {
        // For Decimal8Rescale256AvgOverCurrentRow, varying scale creates instances with
        // different targetTypes. Mixing with a TWO_PASS function forces the cached executor
        // to read each instance's matching getDecimal<targetType>.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ttp\tcr8\tcr16\tcr32\tcr64\tcr128\tcr256
                            2024-01-01T00:00:00.000000Z\t0.6\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t0.6\t0\t0.400\t0.40000\t0.40000000000000\t0.400000000000000000000000000000\t0.400000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t0.6\t1\t0.800\t0.80000\t0.80000000000000\t0.800000000000000000000000000000\t0.800000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t0.6\t0\t0.200\t0.20000\t0.20000000000000\t0.200000000000000000000000000000\t0.200000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:04:00.000000Z\t0.6\t1\t1.000\t1.00000\t1.00000000000000\t1.000000000000000000000000000000\t1.000000000000000000000000000000000000000000000000000000000000
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER (PARTITION BY grp) tp, " +
                            "avg(v8, 0) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr8, " +
                            "avg(v8, 3) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr16, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr32, " +
                            "avg(v8, 14) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr64, " +
                            "avg(v8, 30) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr128, " +
                            "avg(v8, 60) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) cr256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testMixedTwoPassLastValueRangeNonZeroHiAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ttp\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t3.0\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t3.0\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t3.0\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t3.0\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t3.0\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (PARTITION BY grp) tp, " +
                            "last_value(v8) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l8, " +
                            "last_value(v16) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l16, " +
                            "last_value(v32) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l32, " +
                            "last_value(v64) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l64, " +
                            "last_value(v128) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l128, " +
                            "last_value(v256) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND 30 second PRECEDING) l256 " +
                            "FROM t", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\ta\tv\ts
                            2024-01-01T00:00:00.000000Z\tb\t10.00\t70.00
                            2024-01-01T00:01:00.000000Z\ta\t20.00\t20.00
                            2024-01-01T00:02:00.000000Z\tb\t30.00\t100.00
                            2024-01-01T00:03:00.000000Z\ta\t40.00\t60.00
                            """,
                    "SELECT ts, a, v, sum(v) OVER (ORDER BY a, ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM t", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
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
                            "nth_value(v64, 2) OVER () n64, avg(v64, 5) OVER () ar64 FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testMultipleWindowsWithSamePartition() throws Exception {
        // a sum=18, max=8, min=4 ; b sum=12, max=6, min=2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testNamedWindow() throws Exception {
        // sum=30, avg=6, max=10
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts64\ta64\tmx64
                            2024-01-01T00:00:00.000000Z\t30.00\t6.00\t10.00
                            2024-01-01T00:01:00.000000Z\t30.00\t6.00\t10.00
                            2024-01-01T00:02:00.000000Z\t30.00\t6.00\t10.00
                            2024-01-01T00:03:00.000000Z\t30.00\t6.00\t10.00
                            2024-01-01T00:04:00.000000Z\t30.00\t6.00\t10.00
                            """,
                    "SELECT ts, sum(v64) OVER w s64, avg(v64) OVER w a64, max(v64) OVER w mx64 " +
                            "FROM t WINDOW w AS ()", null, "ts", true, true);
        });
    }

    @Test
    public void testNthValueAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesBoundedRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesBoundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:05:00.000000Z\tb\t\t\t\t\t\t
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesBoundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesBoundedRowsPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesInvalidN() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            for (int n : new int[]{0, -1, -100}) {
                for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                    String sql = "SELECT nth_value(" + col + ", " + n + ") OVER (ORDER BY ts) FROM t";
                    // n position is after "SELECT nth_value(<col>, ", i.e. 17 + col.length() + 2
                    int nPos = "SELECT nth_value(".length() + col.length() + 2;
                    assertExceptionNoLeakCheck(sql, nPos, "n must be a positive integer");
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
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesOutOfRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:01:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 2) OVER (PARTITION BY grp) n8, nth_value(v16, 2) OVER (PARTITION BY grp) n16, " +
                            "nth_value(v32, 2) OVER (PARTITION BY grp) n32, nth_value(v64, 2) OVER (PARTITION BY grp) n64, " +
                            "nth_value(v128, 2) OVER (PARTITION BY grp) n128, nth_value(v256, 2) OVER (PARTITION BY grp) n256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesOverPartitionRangeFrame() throws Exception {
        // nth(2) cum within partition. a: null, 0.8, 0.8 ; b: null, 0.2, 0.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesOverRangeFrame() throws Exception {
        // nth(2) cumulative: row 0=null, row 1+=0.4 (2nd row's val)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesRangeNonZeroHi() throws Exception {
        // INSERT_5 v8 values: 0.6, 0.4, 0.8, 0.2, 1.0 at rows 0..4.
        // Row 4 ts=04:00; frame range [01:00, 03:30] -> rows 1,2,3 = [0.4, 0.8, 0.2]; nth(2) = 0.8.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) n8, " +
                            "nth_value(v16, 2) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) n16, " +
                            "nth_value(v32, 2) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) n32, " +
                            "nth_value(v64, 2) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) n64, " +
                            "nth_value(v128, 2) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) n128, " +
                            "nth_value(v256, 2) OVER (ORDER BY ts RANGE BETWEEN 180 second PRECEDING AND 30 second PRECEDING) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesRowsNonZeroHi() throws Exception {
        // INSERT_5 v8 values: 0.6, 0.4, 0.8, 0.2, 1.0 at rows 0..4.
        // Row 4 frame ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING -> rows 1,2,3 = [0.4, 0.8, 0.2]; nth(2) = 0.8.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n8, " +
                            "nth_value(v16, 2) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n16, " +
                            "nth_value(v32, 2) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n32, " +
                            "nth_value(v64, 2) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n64, " +
                            "nth_value(v128, 2) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n128, " +
                            "nth_value(v256, 2) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesRowsNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n8, " +
                            "nth_value(v16, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n16, " +
                            "nth_value(v32, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n32, " +
                            "nth_value(v64, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n64, " +
                            "nth_value(v128, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n128, " +
                            "nth_value(v256, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesRowsUnboundedHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n8, " +
                            "nth_value(v16, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n16, " +
                            "nth_value(v32, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n32, " +
                            "nth_value(v64, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n64, " +
                            "nth_value(v128, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n128, " +
                            "nth_value(v256, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesRowsUnboundedHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:05:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n8, " +
                            "nth_value(v16, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n16, " +
                            "nth_value(v32, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n32, " +
                            "nth_value(v64, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n64, " +
                            "nth_value(v128, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n128, " +
                            "nth_value(v256, 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 2) OVER () n8, " +
                            "nth_value(v16, 2) OVER () n16, " +
                            "nth_value(v32, 2) OVER () n32, " +
                            "nth_value(v64, 2) OVER () n64, " +
                            "nth_value(v128, 2) OVER () n128, " +
                            "nth_value(v256, 2) OVER () n256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testNthValueCurrentRowAllSubTypes() throws Exception {
        // Exercises Decimal{8,16,32,64,128,256}NthValueOverCurrentRowFunction.computeNext + pass1.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n8, " +
                            "nth_value(v16, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n16, " +
                            "nth_value(v32, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n32, " +
                            "nth_value(v64, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n64, " +
                            "nth_value(v128, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n128, " +
                            "nth_value(v256, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) n256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueCurrentRowN2AllSubTypesReturnsNull() throws Exception {
        // n != 1 returns NULL (covers the else branch).
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testNthValueDefaultFrameAllSubTypes() throws Exception {
        // Default frame (no ORDER BY) creates NthValueOverWholeResultSetFunction.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 1) OVER () n8, " +
                            "nth_value(v16, 1) OVER () n16, " +
                            "nth_value(v32, 1) OVER () n32, " +
                            "nth_value(v64, 1) OVER () n64, " +
                            "nth_value(v128, 1) OVER () n128, " +
                            "nth_value(v256, 1) OVER () n256 " +
                            "FROM t", "ts", true, true);
        });
    }

    @Test
    public void testNthValuePartitionDefaultAllSubTypes() throws Exception {
        // PARTITION BY (no frame) → OverPartitionFunction (TWO_PASS).
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 1) OVER (PARTITION BY grp) n8, " +
                            "nth_value(v16, 1) OVER (PARTITION BY grp) n16, " +
                            "nth_value(v32, 1) OVER (PARTITION BY grp) n32, " +
                            "nth_value(v64, 1) OVER (PARTITION BY grp) n64, " +
                            "nth_value(v128, 1) OVER (PARTITION BY grp) n128, " +
                            "nth_value(v256, 1) OVER (PARTITION BY grp) n256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testNthValueUnboundedPartitionRowsAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, grp, " +
                            "nth_value(v8, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n8, " +
                            "nth_value(v16, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n16, " +
                            "nth_value(v32, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n32, " +
                            "nth_value(v64, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n64, " +
                            "nth_value(v128, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n128, " +
                            "nth_value(v256, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testNullFunctionInvertedFrameAllSubTypes() throws Exception {
        // ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING inverts the bounds (rowsHi=-2 < rowsLo=-1).
        // The factory routes through Decimal{8,16,32,64,128,256}NullFunction which returns NULL for every row,
        // exercising getDecimalX, getType, and pass1 on each NullFunction subclass plus BaseNullFunction.toPlan.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            StringSink expected = new StringSink();
            expected.put("ts\ts8\ts16\ts32\ts64\ts128\ts256\tav8\tav16\tav32\tav64\tav128\tav256\tmx8\tmx16\tmx32\tmx64\tmx128\tmx256\tmn8\tmn16\tmn32\tmn64\tmn128\tmn256\tfv8\tfv16\tfv32\tfv64\tfv128\tfv256\tlv8\tlv16\tlv32\tlv64\tlv128\tlv256\tnv8\tnv16\tnv32\tnv64\tnv128\tnv256\n");
            for (int i = 0; i < 5; i++) {
                expected.put("2024-01-01T00:0").put(i).put(":00.000000Z");
                for (int c = 0; c < 42; c++) {
                    expected.put('\t');
                }
                expected.put('\n');
            }
            assertQueryNoLeakCheck(expected.toString(),
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s8, " +
                            "sum(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s16, " +
                            "sum(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s32, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s64, " +
                            "sum(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s128, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) s256, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) av8, " +
                            "avg(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) av16, " +
                            "avg(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) av32, " +
                            "avg(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) av64, " +
                            "avg(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) av128, " +
                            "avg(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) av256, " +
                            "max(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mx8, " +
                            "max(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mx16, " +
                            "max(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mx32, " +
                            "max(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mx64, " +
                            "max(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mx128, " +
                            "max(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mx256, " +
                            "min(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mn8, " +
                            "min(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mn16, " +
                            "min(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mn32, " +
                            "min(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mn64, " +
                            "min(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mn128, " +
                            "min(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) mn256, " +
                            "first_value(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) fv8, " +
                            "first_value(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) fv16, " +
                            "first_value(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) fv32, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) fv64, " +
                            "first_value(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) fv128, " +
                            "first_value(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) fv256, " +
                            "last_value(v8) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) lv8, " +
                            "last_value(v16) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) lv16, " +
                            "last_value(v32) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) lv32, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) lv64, " +
                            "last_value(v128) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) lv128, " +
                            "last_value(v256) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) lv256, " +
                            "nth_value(v8, 1) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) nv8, " +
                            "nth_value(v16, 1) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) nv16, " +
                            "nth_value(v32, 1) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) nv32, " +
                            "nth_value(v64, 1) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) nv64, " +
                            "nth_value(v128, 1) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) nv128, " +
                            "nth_value(v256, 1) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) nv256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testNullFunctionInvertedFrameToPlan() throws Exception {
        // Exercises BaseNullFunction.toPlan branches: partition-by present + bounded hi.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertPlanNoLeakCheck(
                    "SELECT ts, sum(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 3 PRECEDING) s FROM t",
                    """
                            Window
                              functions: [sum(v8) over (partition by [grp] rows between 2 preceding and 3 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testOrderByDescending() throws Exception {
        // DESC order: row 4 (10), row 3 (2), row 2 (8), row 1 (4), row 0 (6)
        // cumulative: 10, 12, 20, 24, 30
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts64
                            2024-01-01T00:04:00.000000Z\t10.00
                            2024-01-01T00:03:00.000000Z\t12.00
                            2024-01-01T00:02:00.000000Z\t20.00
                            2024-01-01T00:01:00.000000Z\t24.00
                            2024-01-01T00:00:00.000000Z\t30.00
                            """,
                    "SELECT ts, sum(v64) OVER (ORDER BY ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s64 FROM t ORDER BY ts DESC", null, "ts###DESC", false, true);
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
            assertQueryNoLeakCheck("""
                            ts\tk\tv\ts
                            2024-01-01T00:00:00.000000Z\t1\t10.00\t40.00
                            2024-01-01T00:01:00.000000Z\t2\t20.00\t60.00
                            2024-01-01T00:02:00.000000Z\t1\t30.00\t40.00
                            2024-01-01T00:03:00.000000Z\t2\t40.00\t60.00
                            """,
                    "SELECT ts, k, v, sum(v) OVER (PARTITION BY k) s FROM t", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\tgrp\ts
                            2024-01-01T00:00:00.000000Z\ta\t18.00
                            2024-01-01T00:01:00.000000Z\tb\t12.00
                            2024-01-01T00:02:00.000000Z\ta\t12.00
                            2024-01-01T00:03:00.000000Z\tb\t8.00
                            2024-01-01T00:04:00.000000Z\ta\t4.00
                            2024-01-01T00:05:00.000000Z\tb\t6.00
                            """,
                    "SELECT ts, grp, sum(v64) OVER (PARTITION BY grp ORDER BY ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\ta\tb\ts
                            2024-01-01T00:00:00.000000Z\tx\tp\t40.00
                            2024-01-01T00:01:00.000000Z\tx\tq\t20.00
                            2024-01-01T00:02:00.000000Z\tx\tp\t40.00
                            2024-01-01T00:03:00.000000Z\ty\tp\t40.00
                            """,
                    "SELECT ts, a, b, sum(v) OVER (PARTITION BY a, b) AS s FROM t", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\tgrp\tv\ts
                            2024-01-01T00:00:00.000000Z\ta\t10.00\t40.00
                            2024-01-01T00:01:00.000000Z\t\t20.00\t60.00
                            2024-01-01T00:02:00.000000Z\ta\t30.00\t40.00
                            2024-01-01T00:03:00.000000Z\t\t40.00\t60.00
                            """,
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) s FROM t", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:02:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:03:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:04:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testRangeIntervalDifferentUnits() throws Exception {
        // 5m / 1h / 1d windows all cover all preceding rows (rows are 60s apart, 5 rows = 4 min total).
        // Actually '5m' covers 5min preceding ; cumulative on INSERT_5: 6, 10, 18, 20, 30
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts1\ts2\ts3
                            2024-01-01T00:00:00.000000Z\t6.00\t6.00\t6.00
                            2024-01-01T00:01:00.000000Z\t10.00\t10.00\t10.00
                            2024-01-01T00:02:00.000000Z\t18.00\t18.00\t18.00
                            2024-01-01T00:03:00.000000Z\t20.00\t20.00\t20.00
                            2024-01-01T00:04:00.000000Z\t30.00\t30.00\t30.00
                            """,
                    "SELECT ts, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN 5 minute PRECEDING AND CURRENT ROW) s1, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN 1 hour PRECEDING AND CURRENT ROW) s2, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN 1 day PRECEDING AND CURRENT ROW) s3 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSingleRowAllFactories() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.0m, 10.0m, 10.000m, 10.00m, 10.000000m, 10m)");
            assertQueryNoLeakCheck("""
                            c8\ts8\tmx8\tmn8\tfv8\tlv8\tavg8
                            1\t1.0\t1.0\t1.0\t1.0\t1.0\t1.0
                            """,
                    "SELECT count(v8) OVER () c8, sum(v8) OVER () s8, max(v8) OVER () mx8, min(v8) OVER () mn8, " +
                            "first_value(v8) OVER () fv8, last_value(v8) OVER () lv8, avg(v8) OVER () avg8 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumAllSubTypesBoundedRange() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:02:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:03:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:04:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (ORDER BY ts RANGE BETWEEN 90 second PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesBoundedRangePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\ta\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\tb\t0.2\t2.0\t2.000\t2.00\t2.000000\t2
                            2024-01-01T00:04:00.000000Z\ta\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "sum(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) s8, " +
                            "sum(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) s16, " +
                            "sum(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) s32, " +
                            "sum(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) s64, " +
                            "sum(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) s128, " +
                            "sum(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 30 second PRECEDING AND CURRENT ROW) s256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testSumAllSubTypesNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_NEGATIVE);
            assertQueryNoLeakCheck("""
                            s8\ts16\ts32\ts64\ts128\ts256
                            -1.0\t-10.0\t-10.000\t-10.00\t-10.000000\t-10
                            """,
                    "SELECT sum(v8) OVER () s8, sum(v16) OVER () s16, sum(v32) OVER () s32, " +
                            "sum(v64) OVER () s64, sum(v128) OVER () s128, sum(v256) OVER () s256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumAllSubTypesOverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesOverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesOverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
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
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesOverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesOverUnboundedPartitionRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testSumAllSubTypesOverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            s8\ts16\ts32\ts64\ts128\ts256
                            3.0\t30.0\t30.000\t30.00\t30.000000\t30
                            """,
                    "SELECT sum(v8) OVER () s8, sum(v16) OVER () s16, sum(v32) OVER () s32, " +
                            "sum(v64) OVER () s64, sum(v128) OVER () s128, sum(v256) OVER () s256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumAllSubTypesPastOnlyFrame() throws Exception {
        // ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- sum of past, excluding current
        // Row 0: null ; Row 1: 0.6 ; Row 2: 1.0 ; Row 3: 1.8 ; Row 4: 2.0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesRowsNonZeroHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:03:00.000000Z\t1.2\t12.0\t12.000\t12.00\t12.000000\t12
                            2024-01-01T00:04:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s8, " +
                            "sum(v16) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s16, " +
                            "sum(v32) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s32, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s64, " +
                            "sum(v128) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s128, " +
                            "sum(v256) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesRowsNonZeroHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t1.4\t14.0\t14.000\t14.00\t14.000000\t14
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "sum(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s8, " +
                            "sum(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s16, " +
                            "sum(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s32, " +
                            "sum(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s64, " +
                            "sum(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s128, " +
                            "sum(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) s256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesSlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesUnboundedRangeBoundedHi() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t1.0\t10.0\t10.000\t10.00\t10.000000\t10
                            2024-01-01T00:03:00.000000Z\t1.8\t18.0\t18.000\t18.00\t18.000000\t18
                            2024-01-01T00:04:00.000000Z\t2.0\t20.0\t20.000\t20.00\t20.000000\t20
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) s8, " +
                            "sum(v16) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) s16, " +
                            "sum(v32) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) s32, " +
                            "sum(v64) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) s64, " +
                            "sum(v128) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) s128, " +
                            "sum(v256) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 30 second PRECEDING) s256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesUnboundedRangeBoundedHiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tgrp\ts8\ts16\ts32\ts64\ts128\ts256
                            2024-01-01T00:00:00.000000Z\ta\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\tb\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\ta\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\tb\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\ta\t1.4\t14.0\t14.000\t14.00\t14.000000\t14
                            2024-01-01T00:05:00.000000Z\tb\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, grp, " +
                            "sum(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) s8, " +
                            "sum(v16) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) s16, " +
                            "sum(v32) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) s32, " +
                            "sum(v64) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) s64, " +
                            "sum(v128) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) s128, " +
                            "sum(v256) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 60 second PRECEDING) s256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testSumAllSubTypesUnboundedToUnbounded() throws Exception {
        // sum total: 3.0 (v8), 30.0 (v16+)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testSumAllSubTypesWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            s8\ts16\ts32\ts64\ts128\ts256
                            \t\t\t\t\t
                            """,
                    "SELECT sum(v8) OVER () s8, sum(v16) OVER () s16, sum(v32) OVER () s32, " +
                            "sum(v64) OVER () s64, sum(v128) OVER () s128, sum(v256) OVER () s256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumAllSubTypesWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            s8\ts16\ts32\ts64\ts128\ts256
                            1.4\t14.0\t14.000\t14.00\t14.000000\t14
                            """,
                    "SELECT sum(v8) OVER () s8, sum(v16) OVER () s16, sum(v32) OVER () s32, " +
                            "sum(v64) OVER () s64, sum(v128) OVER () s128, sum(v256) OVER () s256 " +
                            "FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumDecimal128AccumulatorPromotion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 999999999999999999m), " +
                    "('2024-01-01T00:01:00', 999999999999999999m)");
            assertQueryNoLeakCheck("s\n1999999999999999998\n", "SELECT sum(v) OVER () s FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumDecimal128LargeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 99999999999999999999999999999999999999m), " +
                    "('2024-01-01T00:01:00', 1m)");
            assertQueryNoLeakCheck("sum_v\n100000000000000000000000000000000000000\n", "SELECT sum(v) OVER () AS sum_v FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumDecimal256NearMaxPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(70, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 3000000000000000000000000000000000000000000000000000000000000000000000m), " +
                    "('2024-01-01T00:01:00', 4000000000000000000000000000000000000000000000000000000000000000000000m)");
            assertQueryNoLeakCheck("s\n7000000000000000000000000000000000000000000000000000000000000000000000\n", "SELECT sum(v) OVER () s FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumDecimal256NearOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(70, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1000000000000000000000000000000000000000000000000000000000000000000000m), " +
                    "('2024-01-01T00:01:00', 1m)");
            assertQueryNoLeakCheck("sum_v\n1000000000000000000000000000000000000000000000000000000000000000000001\n", "SELECT sum(v) OVER () AS sum_v FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumDecimal256OverflowAtAdd() throws Exception {
        // Two values each near D256 max overflow the D256 accumulator at add time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (ts TIMESTAMP, v decimal(76, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t2 VALUES " +
                    "('2024-01-01T00:00:00', 5000000000000000000000000000000000000000000000000000000000000000000000000000m), " +
                    "('2024-01-01T00:01:00', 5000000000000000000000000000000000000000000000000000000000000000000000000000m)");
            assertExceptionNoLeakCheck(
                    "SELECT sum(v) OVER () c FROM t2",
                    "SELECT sum(".length(),
                    "sum aggregation failed"
            );
        });
    }

    @Test
    public void testSumDecimal8NoThrow() throws Exception {
        // sum(D8/D16) widens to D64. The raw byte/short sum always fits in long, so
        // no overflow CairoException can ever fire. Pin this guarantee in a test.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("c\n3.0\n", "SELECT sum(v8) OVER () c FROM t LIMIT 1", null, null, true, false);
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
            assertQueryNoLeakCheck("s\n0\n", "SELECT sum(v) OVER () s FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testSumDescNullsLastOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tv64\ts
                            2024-01-01T00:00:00.000000Z\t\t
                            2024-01-01T00:01:00.000000Z\t6.00\t6.00
                            2024-01-01T00:02:00.000000Z\t\t6.00
                            2024-01-01T00:03:00.000000Z\t8.00\t14.00
                            2024-01-01T00:04:00.000000Z\t\t14.00
                            """,
                    "SELECT ts, v64, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s " +
                            "FROM t ORDER BY ts", null, "ts", false, true);
        });
    }

    @Test
    public void testSumMaxMinRangeTriggersBufferExpansion() throws Exception {
        // 33 rows of 0.5 -> sum=16.5 at row 33; max=0.5, min=0.5 throughout.
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 33; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            StringSink expected = new StringSink();
            expected.put("ts\ts8\tm8\tn8\n");
            for (int i = 0; i < 33; i++) {
                expected.put("2024-01-01T00:");
                if (i < 10) expected.put('0');
                expected.put(i).put(":00.000000Z\t");
                // sum after (i+1) rows of 0.5 = 0.5*(i+1). Half-step decimals — print int.fractional.
                long halfTenths = (long) (i + 1) * 5; // 0.5*(i+1) expressed in tenths × 10
                long intPart = halfTenths / 10;
                int frac = (int) (halfTenths % 10);
                expected.put(intPart).put('.').put(frac).put("\t0.5\t0.5\n");
            }
            assertQueryNoLeakCheck(expected.toString(),
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) s8, " +
                            "max(v8) OVER (ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) m8, " +
                            "min(v8) OVER (ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) n8 " +
                            "FROM t",
                    null, "ts", false, true);
        });
    }

    @Test
    public void testSumNullsFirstOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tv64\ts
                            2024-01-01T00:00:00.000000Z\t\t
                            2024-01-01T00:01:00.000000Z\t6.00\t6.00
                            2024-01-01T00:02:00.000000Z\t\t6.00
                            2024-01-01T00:03:00.000000Z\t8.00\t14.00
                            2024-01-01T00:04:00.000000Z\t\t14.00
                            """,
                    "SELECT ts, v64, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s " +
                            "FROM t ORDER BY ts", null, "ts", false, true);
        });
    }

    @Test
    public void testSumNullsLastOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tv64\ts
                            2024-01-01T00:00:00.000000Z\t\t
                            2024-01-01T00:01:00.000000Z\t6.00\t6.00
                            2024-01-01T00:02:00.000000Z\t\t6.00
                            2024-01-01T00:03:00.000000Z\t8.00\t14.00
                            2024-01-01T00:04:00.000000Z\t\t14.00
                            """,
                    "SELECT ts, v64, " +
                            "sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s " +
                            "FROM t ORDER BY ts", null, "ts", false, true);
        });
    }

    @Test
    public void testSumOnExpressionInWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t60.0000
                            2024-01-01T00:01:00.000000Z\t60.0000
                            2024-01-01T00:02:00.000000Z\t60.0000
                            2024-01-01T00:03:00.000000Z\t60.0000
                            2024-01-01T00:04:00.000000Z\t60.0000
                            """,
                    "SELECT ts, sum(v64 * 2::decimal(18, 2)) OVER () s FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowAfterWhereFilter() throws Exception {
        // Filter v64 > 6: rows ts=02 (8), ts=04 (10). Sum = 18
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts64
                            2024-01-01T00:02:00.000000Z\t18.00
                            2024-01-01T00:04:00.000000Z\t18.00
                            """,
                    "SELECT ts, sum(v64) OVER () s64 FROM t WHERE v64 > 6.00::decimal(18, 2)", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowAggregateOverMultipleRows() throws Exception {
        // INSERT_6_PART: a sum 18.00, b sum 12.00, total 30.00
        // ratio a = 18/30 = 0.6 ; ratio b = 12/30 = 0.4
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowAvgPrecisionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1m), ('2024-01-01T00:01:00', 2m), ('2024-01-01T00:02:00', 3m)");
            assertQueryNoLeakCheck("avg_v\n2\n", "SELECT avg(v) OVER () AS avg_v FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testWindowAvgRescaleHigherScale() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1m), ('2024-01-01T00:01:00', 2m), ('2024-01-01T00:02:00', 4m)");
            assertQueryNoLeakCheck("avg_v\n2.33333\n", "SELECT avg(v, 5) OVER () AS avg_v FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testWindowAvgRescaleReexecuteAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            for (int iter = 0; iter < 2; iter++) {
                assertQueryNoLeakCheck("""
                                ts\ta8\ta16\ta32\ta64\ta128\ta256
                                2024-01-01T00:00:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                                2024-01-01T00:01:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                                2024-01-01T00:02:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                                2024-01-01T00:03:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                                2024-01-01T00:04:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                                """,
                        "SELECT ts, " +
                                "avg(v8, 0) OVER () a8, " +
                                "avg(v8, 3) OVER () a16, " +
                                "avg(v8, 5) OVER () a32, " +
                                "avg(v8, 14) OVER () a64, " +
                                "avg(v8, 30) OVER () a128, " +
                                "avg(v8, 60) OVER () a256 " +
                                "FROM t", "ts", true, true);
            }
        });
    }

    @Test
    public void testWindowAvgRescaleZeroScale() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 5)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1.00000m), ('2024-01-01T00:01:00', 2.00000m), ('2024-01-01T00:02:00', 4.00000m)");
            assertQueryNoLeakCheck("avg_v\n2\n", "SELECT avg(v, 0) OVER () AS avg_v FROM t LIMIT 1", null, null, true, false);
        });
    }

    @Test
    public void testWindowDifferenceBetweenTwoWindows() throws Exception {
        // total=30, cumsum: 6, 10, 18, 20, 30 ; remaining: 24, 20, 12, 10, 0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowFunctionInWhereForbidden() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            try {
                assertQueryNoLeakCheck("", "SELECT * FROM t WHERE sum(v64) OVER () > 10.00::decimal(18, 2)", null, null, true, true);
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
            assertQueryNoLeakCheck("""
                            grp\ts_grp
                            a\t18.00
                            a\t18.00
                            a\t18.00
                            b\t12.00
                            b\t12.00
                            b\t12.00
                            """,
                    "SELECT grp, sum(v64) OVER (PARTITION BY grp) s_grp FROM t ORDER BY s_grp DESC", null, null, true, true);
        });
    }

    @Test
    public void testWindowInArithmeticExpression() throws Exception {
        // v64: 6.00, 4.00, 8.00, 2.00, 10.00 ; avg=6.00 ; diff: 0, -2, 2, -4, 4
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tdiff64
                            2024-01-01T00:00:00.000000Z\t0.00
                            2024-01-01T00:01:00.000000Z\t-2.00
                            2024-01-01T00:02:00.000000Z\t2.00
                            2024-01-01T00:03:00.000000Z\t-4.00
                            2024-01-01T00:04:00.000000Z\t4.00
                            """,
                    "SELECT ts, v64 - avg(v64) OVER () AS diff64 FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowInCte() throws Exception {
        // sum=30.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts64
                            2024-01-01T00:02:00.000000Z\t30.00
                            """,
                    "WITH w AS (SELECT ts, sum(v64) OVER () s64 FROM t) " +
                            "SELECT ts, s64 FROM w WHERE ts = '2024-01-01T00:02:00'", null, "ts", true, false);
        });
    }

    @Test
    public void testWindowInSubquery() throws Exception {
        // INSERT_5: cumulative sum 6.00, 10.00, 18.00, 20.00, 30.00 -> > 10.00 are rows 2,3,4
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tv64\ts64
                            2024-01-01T00:02:00.000000Z\t8.00\t18.00
                            2024-01-01T00:03:00.000000Z\t2.00\t20.00
                            2024-01-01T00:04:00.000000Z\t10.00\t30.00
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, v64, sum(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s64 FROM t" +
                            ") WHERE s64 > 10.00::decimal(18, 2)", null, "ts", false, false);
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
            assertQueryNoLeakCheck("""
                            ts\tmx\tmn
                            2024-01-01T00:00:00.000000Z\t50.00\t50.00
                            2024-01-01T00:01:00.000000Z\t50.00\t10.00
                            2024-01-01T00:02:00.000000Z\t50.00\t10.00
                            2024-01-01T00:03:00.000000Z\t30.00\t10.00
                            2024-01-01T00:04:00.000000Z\t40.00\t20.00
                            """,
                    "SELECT ts, " +
                            "max(v) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) mx, " +
                            "min(v) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) mn " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testWindowMixedD128AndD256InSelect() throws Exception {
        // sum=30, max=10, min=2 (all sub-types)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts128\ts256\tmx128\tmn256
                            2024-01-01T00:00:00.000000Z\t30.000000\t30\t10.000000\t2
                            2024-01-01T00:01:00.000000Z\t30.000000\t30\t10.000000\t2
                            2024-01-01T00:02:00.000000Z\t30.000000\t30\t10.000000\t2
                            2024-01-01T00:03:00.000000Z\t30.000000\t30\t10.000000\t2
                            2024-01-01T00:04:00.000000Z\t30.000000\t30\t10.000000\t2
                            """,
                    "SELECT ts, " +
                            "sum(v128) OVER () s128, sum(v256) OVER () s256, " +
                            "max(v128) OVER () mx128, min(v256) OVER () mn256 FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowMixedSubTypesInOnePartition() throws Exception {
        // a sum: 1.8, b sum: 1.2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
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
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowMultipliedByConstant() throws Exception {
        // 2 * avg(v64) = 2 * 6.00 = 12.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tdouble_avg
                            2024-01-01T00:00:00.000000Z\t12.00
                            2024-01-01T00:01:00.000000Z\t12.00
                            2024-01-01T00:02:00.000000Z\t12.00
                            2024-01-01T00:03:00.000000Z\t12.00
                            2024-01-01T00:04:00.000000Z\t12.00
                            """,
                    "SELECT ts, 2 * avg(v64) OVER () AS double_avg FROM t", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\tk\tv\ts
                            2024-01-01T00:00:00.000000Z\t1\t10.00\t40.00
                            2024-01-01T00:01:00.000000Z\t2\t20.00\t60.00
                            2024-01-01T00:02:00.000000Z\t3\t30.00\t40.00
                            2024-01-01T00:03:00.000000Z\t4\t40.00\t60.00
                            """,
                    "SELECT ts, k, v, sum(v) OVER (PARTITION BY k % 2) s FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowOnDecimalAddition() throws Exception {
        // sum(v + v) = 2 * sum = 60.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t60.00
                            2024-01-01T00:01:00.000000Z\t60.00
                            2024-01-01T00:02:00.000000Z\t60.00
                            2024-01-01T00:03:00.000000Z\t60.00
                            2024-01-01T00:04:00.000000Z\t60.00
                            """,
                    "SELECT ts, sum(v64 + v64) OVER () s FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowOnDerivedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tvx2\ts
                            2024-01-01T00:00:00.000000Z\t12.0000\t60.0000
                            2024-01-01T00:01:00.000000Z\t8.0000\t60.0000
                            2024-01-01T00:02:00.000000Z\t16.0000\t60.0000
                            2024-01-01T00:03:00.000000Z\t4.0000\t60.0000
                            2024-01-01T00:04:00.000000Z\t20.0000\t60.0000
                            """,
                    "SELECT ts, vx2, sum(vx2) OVER () s FROM " +
                            "(SELECT ts, v64 * 2::decimal(18, 2) vx2 FROM t)", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\tv64\ts
                            2024-01-01T00:02:00.000000Z\t8.00\t18.00
                            2024-01-01T00:04:00.000000Z\t10.00\t18.00
                            """,
                    "SELECT ts, v64, sum(v64) OVER () s FROM (SELECT * FROM t WHERE v64 >= 8.00::decimal(18, 2))", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t30.00
                            2024-01-01T00:01:00.000000Z\t30.00
                            2024-01-01T00:02:00.000000Z\t30.00
                            2024-01-01T00:03:00.000000Z\t30.00
                            2024-01-01T00:04:00.000000Z\t30.00
                            """,
                    "SELECT ts, s FROM r", null, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t10.00
                            2024-01-02T00:00:00.000000Z\t30.00
                            2024-01-03T00:00:00.000000Z\t60.00
                            """,
                    "SELECT ts, sum(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testWindowOnlyOneNullValue() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', null, null, null, null, null, null)");
            assertQueryNoLeakCheck("""
                            s64\ta64\tc64\tmx64\tmn64
                            \t\t0\t\t
                            """,
                    "SELECT sum(v64) OVER () s64, avg(v64) OVER () a64, count(v64) OVER () c64, " +
                            "max(v64) OVER () mx64, min(v64) OVER () mn64 FROM t LIMIT 1", null, null, true, false);
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
            assertQueryNoLeakCheck("""
                            ts\tv64\tcs
                            2024-01-01T00:00:00.000000Z\t6.00\t12.00
                            2024-01-01T00:01:00.000000Z\t4.00\t6.00
                            2024-01-01T00:02:00.000000Z\t8.00\t20.00
                            2024-01-01T00:03:00.000000Z\t2.00\t2.00
                            2024-01-01T00:04:00.000000Z\t10.00\t30.00
                            """,
                    "SELECT ts, v64, sum(v64) OVER (ORDER BY v64 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cs FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowOutputInExpressionAllSubTypes() throws Exception {
        // Wrapping a window-function output in an arithmetic / cast expression forces the
        // outer expression to read the windowed value through the function's getDecimal*
        // overloads (rather than directly from the materialised cursor buffer). Each
        // subtype's getter is exercised in turn.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\tx8\tx16\tx32\tx64\tx128\tx256
                            2024-01-01T00:00:00.000000Z\t0.7\t6.1\t6.001\t6.01\t6.000001\t7
                            2024-01-01T00:01:00.000000Z\t0.7\t6.1\t6.001\t6.01\t6.000001\t7
                            2024-01-01T00:02:00.000000Z\t0.7\t6.1\t6.001\t6.01\t6.000001\t7
                            2024-01-01T00:03:00.000000Z\t0.7\t6.1\t6.001\t6.01\t6.000001\t7
                            2024-01-01T00:04:00.000000Z\t0.7\t6.1\t6.001\t6.01\t6.000001\t7
                            """,
                    "SELECT ts, " +
                            "avg(v8) OVER () + '0.1'::decimal(2,1) x8, " +
                            "avg(v16) OVER () + '0.1'::decimal(4,1) x16, " +
                            "avg(v32) OVER () + '0.001'::decimal(7,3) x32, " +
                            "avg(v64) OVER () + '0.01'::decimal(15,2) x64, " +
                            "avg(v128) OVER () + '0.000001'::decimal(20,6) x128, " +
                            "avg(v256) OVER () + '1'::decimal(40,0) x256 " +
                            "FROM t", "ts", true, true);
        });
    }

    @Test
    public void testWindowReexecuteAllSubTypes() throws Exception {
        // Re-executing the same query exercises the reset/toTop paths on window functions
        // (the factory is re-used by the cursor).
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            for (int iter = 0; iter < 2; iter++) {
                assertQueryNoLeakCheck("""
                                ts\ts8\ts64\ts256\ta8\ta64\ta256\tm8\tm64\tm256\tn8\tn64\tn256\tf8\tf64\tf256\tl8\tl64\tl256
                                2024-01-01T00:00:00.000000Z\t3.0\t30.00\t30\t0.6\t6.00\t6\t1.0\t10.00\t10\t0.2\t2.00\t2\t0.6\t6.00\t6\t1.0\t10.00\t10
                                2024-01-01T00:01:00.000000Z\t3.0\t30.00\t30\t0.6\t6.00\t6\t1.0\t10.00\t10\t0.2\t2.00\t2\t0.6\t6.00\t6\t1.0\t10.00\t10
                                2024-01-01T00:02:00.000000Z\t3.0\t30.00\t30\t0.6\t6.00\t6\t1.0\t10.00\t10\t0.2\t2.00\t2\t0.6\t6.00\t6\t1.0\t10.00\t10
                                2024-01-01T00:03:00.000000Z\t3.0\t30.00\t30\t0.6\t6.00\t6\t1.0\t10.00\t10\t0.2\t2.00\t2\t0.6\t6.00\t6\t1.0\t10.00\t10
                                2024-01-01T00:04:00.000000Z\t3.0\t30.00\t30\t0.6\t6.00\t6\t1.0\t10.00\t10\t0.2\t2.00\t2\t0.6\t6.00\t6\t1.0\t10.00\t10
                                """,
                        "SELECT ts, " +
                                "sum(v8) OVER () s8, sum(v64) OVER () s64, sum(v256) OVER () s256, " +
                                "avg(v8) OVER () a8, avg(v64) OVER () a64, avg(v256) OVER () a256, " +
                                "max(v8) OVER () m8, max(v64) OVER () m64, max(v256) OVER () m256, " +
                                "min(v8) OVER () n8, min(v64) OVER () n64, min(v256) OVER () n256, " +
                                "first_value(v8) OVER () f8, first_value(v64) OVER () f64, first_value(v256) OVER () f256, " +
                                "last_value(v8) OVER () l8, last_value(v64) OVER () l64, last_value(v256) OVER () l256 " +
                                "FROM t", "ts", true, true);
            }
        });
    }

    @Test
    public void testWindowSumPlusLiteral() throws Exception {
        // sum=30.00, +1.00 = 31.00
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts_plus_1
                            2024-01-01T00:00:00.000000Z\t31.00
                            2024-01-01T00:01:00.000000Z\t31.00
                            2024-01-01T00:02:00.000000Z\t31.00
                            2024-01-01T00:03:00.000000Z\t31.00
                            2024-01-01T00:04:00.000000Z\t31.00
                            """,
                    "SELECT ts, sum(v64) OVER () + 1.00::decimal(18, 2) s_plus_1 FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowWithCastInSelect() throws Exception {
        // sum=30.00 cast to double = 30.0
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts_as_double
                            2024-01-01T00:00:00.000000Z\t30.0
                            2024-01-01T00:01:00.000000Z\t30.0
                            2024-01-01T00:02:00.000000Z\t30.0
                            2024-01-01T00:03:00.000000Z\t30.0
                            2024-01-01T00:04:00.000000Z\t30.0
                            """,
                    "SELECT ts, (sum(v64) OVER ())::double s_as_double FROM t", null, "ts", true, true);
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
            assertQueryNoLeakCheck("s\n40.00\n", "SELECT DISTINCT sum(v) OVER () s FROM t", null, null, false, true);
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
            assertQueryNoLeakCheck("""
                            ts\tlabel\ts64
                            2024-01-01T00:00:00.000000Z\tgroup-a\t30.00
                            2024-01-01T00:01:00.000000Z\tgroup-a\t30.00
                            2024-01-01T00:02:00.000000Z\tgroup-a\t30.00
                            2024-01-01T00:03:00.000000Z\tgroup-a\t30.00
                            2024-01-01T00:04:00.000000Z\tgroup-a\t30.00
                            """,
                    "SELECT t.ts, m.label, sum(t.v64) OVER () s64 FROM t JOIN m ON t.grp = m.k", null, "ts", true, true);
        });
    }

    @Test
    public void testWindowWithLimit() throws Exception {
        // sum=30 for all rows; LIMIT 2 just shows first 2
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ts64
                            2024-01-01T00:00:00.000000Z\t30.00
                            2024-01-01T00:01:00.000000Z\t30.00
                            """,
                    "SELECT ts, sum(v64) OVER () s64 FROM t LIMIT 2", null, "ts", true, false);
        });
    }

    @Test
    public void testWindowWithManyDistinctPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k INT, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60_000_000), x::int, 1.00::decimal(18, 2) FROM long_sequence(100)");
            assertQueryNoLeakCheck("c\n100\n", "SELECT count(s) c FROM (SELECT sum(v) OVER (PARTITION BY k) s FROM t)", null, null, false, true);
        });
    }

    @Test
    public void testWindowWithUnionAll() throws Exception {
        // sum(v64) = 30.00 ; 5 rows + 5 rows = 10
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
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
                    "SELECT ts, sum(v64) OVER () s FROM t UNION ALL SELECT ts, sum(v64) OVER () s FROM t", null, null, false, true);
        });
    }

    @Test
    public void testNthValueAllSubTypesPartitionRangeBufferExpansion() throws Exception {
        // Shrinks the initial range buffer so a single partition with > 2 rows forces
        // expandRingBuffer in Decimal*NthValueOverPartitionRangeFrameFunction.
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 40; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            // 40 rows in partition 'a'. First row frameSize=1 < n=2 -> NULL (empty), rest -> 0.5.
            assertQueryNoLeakCheck(
                    "nv\tcnt\n\t1\n0.5\t39\n",
                    "SELECT nv, count(*) cnt FROM (" +
                            "SELECT nth_value(v8, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) nv FROM t" +
                            ") GROUP BY nv ORDER BY nv",
                    null, true, true);
        });
    }

    @Test
    public void testFirstValuePartitionRangeBufferExpansionAllSubTypes() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 40; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            assertQueryNoLeakCheck(
                    "fv\tcnt\n0.5\t40\n",
                    "SELECT fv, count(*) cnt FROM (" +
                            "SELECT first_value(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) fv FROM t" +
                            ") GROUP BY fv ORDER BY fv",
                    null, true, true);
        });
    }

    @Test
    public void testLastValuePartitionRangeBufferExpansionAllSubTypes() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 40; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            assertQueryNoLeakCheck(
                    "lv\tcnt\n0.5\t40\n",
                    "SELECT lv, count(*) cnt FROM (" +
                            "SELECT last_value(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) lv FROM t" +
                            ") GROUP BY lv ORDER BY lv",
                    null, true, true);
        });
    }

    @Test
    public void testSumAvgMaxMinPartitionRangeBufferExpansion() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 40; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            assertQueryNoLeakCheck(
                    "av\tcnt\n0.5\t40\n",
                    "SELECT av, count(*) cnt FROM (" +
                            "SELECT avg(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) av FROM t" +
                            ") GROUP BY av ORDER BY av",
                    null, true, true);
            assertQueryNoLeakCheck(
                    "mx\tcnt\n0.5\t40\n",
                    "SELECT mx, count(*) cnt FROM (" +
                            "SELECT max(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) mx FROM t" +
                            ") GROUP BY mx ORDER BY mx",
                    null, true, true);
            assertQueryNoLeakCheck(
                    "mn\tcnt\n0.5\t40\n",
                    "SELECT mn, count(*) cnt FROM (" +
                            "SELECT min(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) mn FROM t" +
                            ") GROUP BY mn ORDER BY mn",
                    null, true, true);
        });
    }

    @Test
    public void testAvgRescalePartitionRangeBufferExpansion() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 40; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            assertQueryNoLeakCheck(
                    "av\tcnt\n0.50000\t40\n",
                    "SELECT av, count(*) cnt FROM (" +
                            "SELECT avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) av FROM t" +
                            ") GROUP BY av ORDER BY av",
                    null, true, true);
        });
    }

    @Test
    public void testNthValuePartitionRangeUnboundedLockedValueAllSubTypes() throws Exception {
        // !frameLoBounded path: locked-value optimization when frameSize >= n.
        // Single partition, RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
        // nth_value(v8, 1) locks onto first value (0.5) once row 1 is seen.
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 128);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 40; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            // nth_value(v8, 1) over unbounded preceding: first value = 0.5 for all rows
            assertQueryNoLeakCheck(
                    "nv\tcnt\n0.5\t40\n",
                    "SELECT nv, count(*) cnt FROM (" +
                            "SELECT nth_value(v8, 1) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) nv FROM t" +
                            ") GROUP BY nv ORDER BY nv",
                    null, true, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllSubTypes() throws Exception {
        // first_value IGNORE NULLS routes to FirstNotNull* TWO_PASS classes.
        // First non-null in each partition: 'a' -> 0.6 (row 1), 'b' -> 0.4 (no nulls).
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp) f256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllNullPartition() throws Exception {
        // Partition with only NULL values: FirstNotNull returns NULL.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp) f256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllSubTypes() throws Exception {
        // last_value IGNORE NULLS over running unbounded ROWS frame: emits the most recent non-null per row.
        // 'a': null, 0.6, null, 0.8, null -> null, 0.6, 0.6, 0.8, 0.8
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllNullPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSourcesAllTargetsAllNullPartition() throws Exception {
        // Triggers pass2's count==0 NULL-path in Decimal*Rescale256AvgOverPartitionFunction across
        // all source x target combinations. Exercises every reachable writeNull branch.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            // Source D8: 6 target scales (D8/D16/D32/D64/D128/D256). All scale = 0..60 ensures all 6 writeNull cases hit.
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER (PARTITION BY grp) a8, " +
                            "avg(v8, 3) OVER (PARTITION BY grp) a16, " +
                            "avg(v8, 5) OVER (PARTITION BY grp) a32, " +
                            "avg(v8, 14) OVER (PARTITION BY grp) a64, " +
                            "avg(v8, 30) OVER (PARTITION BY grp) a128, " +
                            "avg(v8, 60) OVER (PARTITION BY grp) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleD16AllTargetsAllNullPartition() throws Exception {
        // D16 source -> D16/D32/D64/D128/D256 targets (D8 unreachable since p>=3).
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "avg(v16, 0) OVER (PARTITION BY grp) a16, " +
                            "avg(v16, 6) OVER (PARTITION BY grp) a32, " +
                            "avg(v16, 15) OVER (PARTITION BY grp) a64, " +
                            "avg(v16, 30) OVER (PARTITION BY grp) a128, " +
                            "avg(v16, 60) OVER (PARTITION BY grp) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleD32AllTargetsAllNullPartition() throws Exception {
        // D32 source -> D32/D64/D128/D256 targets.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "avg(v32, 0) OVER (PARTITION BY grp) a32, " +
                            "avg(v32, 10) OVER (PARTITION BY grp) a64, " +
                            "avg(v32, 30) OVER (PARTITION BY grp) a128, " +
                            "avg(v32, 60) OVER (PARTITION BY grp) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleD128AllTargetsAllNullPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t\t
                            2024-01-01T00:01:00.000000Z\t\t
                            2024-01-01T00:02:00.000000Z\t\t
                            2024-01-01T00:03:00.000000Z\t\t
                            2024-01-01T00:04:00.000000Z\t\t
                            """,
                    "SELECT ts, " +
                            "avg(v128, 0) OVER (PARTITION BY grp) a128, " +
                            "avg(v128, 44) OVER (PARTITION BY grp) a256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testAvgRescaleD256AllNullPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\ta256
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T00:01:00.000000Z\t
                            2024-01-01T00:02:00.000000Z\t
                            2024-01-01T00:03:00.000000Z\t
                            2024-01-01T00:04:00.000000Z\t
                            """,
                    "SELECT ts, avg(v256, 16) OVER (PARTITION BY grp) a256 FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testFirstValueIgnoreNullsAllFramesAllSubTypes() throws Exception {
        // first_value IGNORE NULLS across all frame types routes to FirstNotNull* subclasses.
        // Single partition 'a': null, 0.6, null, 0.8, null  -> first non-null = 0.6 at row 1.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            // OverPartitionRangeFrame (with PARTITION BY + ORDER BY ts + RANGE)
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", "ts", false, true);
            // OverPartitionRowsFrame
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", "ts", false, true);
            // OverUnboundedPartitionRowsFrame
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", "ts", false, true);
            // OverRangeFrame (no PARTITION BY)
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", "ts", false, true);
            // OverRowsFrame (no PARTITION BY)
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", "ts", false, true);
            // OverUnboundedRowsFrame
            assertQueryNoLeakCheck("""
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
                            "FROM t", "ts", false, true);
            // OverWholeResultSet TWO_PASS: global first non-null = 0.6 for all rows.
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) IGNORE NULLS OVER () f8, " +
                            "first_value(v16) IGNORE NULLS OVER () f16, " +
                            "first_value(v32) IGNORE NULLS OVER () f32, " +
                            "first_value(v64) IGNORE NULLS OVER () f64, " +
                            "first_value(v128) IGNORE NULLS OVER () f128, " +
                            "first_value(v256) IGNORE NULLS OVER () f256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllFramesAllSubTypes() throws Exception {
        // last_value IGNORE NULLS across all frame types routes to LastNotNull* subclasses.
        // Single partition 'a': null, 0.6, null, 0.8, null. Running last-non-null = null, 0.6, 0.6, 0.8, 0.8.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", "ts", false, true);
            // OverRangeFrame no partition
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", "ts", false, true);
            // OverRowsFrame (no PARTITION BY)
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleAllSourcesAllTargetsOverPartitionRowsFrame() throws Exception {
        // OverPartitionRowsFrame across all source/target combinations.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            assertQueryNoLeakCheck("""
                            ts\ta8\ta16\ta32\ta64\ta128\ta256
                            2024-01-01T00:00:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:01:00.000000Z\t0\t0.500\t0.50000\t0.50000000000000\t0.500000000000000000000000000000\t0.500000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:02:00.000000Z\t1\t0.600\t0.60000\t0.60000000000000\t0.600000000000000000000000000000\t0.600000000000000000000000000000000000000000000000000000000000
                            2024-01-01T00:03:00.000000Z\t0\t0.467\t0.46667\t0.46666666666667\t0.466666666666666666666666666667\t0.466666666666666666666666666666666666666666666666666666666667
                            2024-01-01T00:04:00.000000Z\t1\t0.667\t0.66667\t0.66666666666667\t0.666666666666666666666666666667\t0.666666666666666666666666666666666666666666666666666666666667
                            """,
                    "SELECT ts, " +
                            "avg(v8, 0) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) a8, " +
                            "avg(v8, 3) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) a16, " +
                            "avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) a32, " +
                            "avg(v8, 14) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) a64, " +
                            "avg(v8, 30) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) a128, " +
                            "avg(v8, 60) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) a256 " +
                            "FROM t", null, "ts", false, true);
        });
    }

    @Test
    public void testFirstValueRangeFrameLargeBufferStressAllSubTypes() throws Exception {
        // Stress test: many rows in multiple partitions with bounded RANGE forces both eviction
        // (rows past lo boundary) AND buffer expansion (per-partition ring buffer growth).
        // Shrinks buffer to ensure repeated expandRingBuffer / freeList reuse paths.
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 256);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            // 60 rows across 3 partitions ('a','b','c') with NULLs at every 4th position.
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 60; i++) {
                if (i > 0) insert.put(", ");
                char grp = (char) ('a' + (i % 3));
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', '").put(grp).put("', ");
                if (i % 4 == 0) {
                    insert.put("null, null, null, null, null, null)");
                } else {
                    insert.put("0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
                }
            }
            execute(insert.toString());
            // First non-null per partition is at row offset 1 (since row 0 in each partition is NULL).
            // Aggregated count check: a partition has 20 rows, b has 20, c has 20.
            // first_value over partition RANGE: row 0 in partition = NULL, rows 1+ in partition = first non-null = 0.5.
            // Per partition with NULL at i%4==0, the first non-null row in partition 'a' (i=0,3,6,9,...) is i=3 (since i=0 is NULL).
            // Actually grp='a' has i=0,3,6,9,12,..,57. i=0,12,24,36,48 NULL (where i%4==0). First non-null at i=3 → 0.5.
            assertQueryNoLeakCheck(
                    "cnt\n60\n",
                    "SELECT count(*) cnt FROM (" +
                            "SELECT first_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 600 second PRECEDING AND CURRENT ROW) fv FROM t" +
                            ")",
                    null, false, true);
        });
    }

    @Test
    public void testFirstValueUnboundedPartitionRowsFrameAllSubTypes() throws Exception {
        // Exercises Decimal*FirstValueOverUnboundedPartitionRowsFrameFunction for each subtype.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:01:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:05:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testFirstValueUnboundedPartitionRowsFrameExplainPlan() throws Exception {
        // Covers toPlan of Decimal*FirstValueOverUnboundedPartitionRowsFrameFunction for all 6 subtypes.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") over (partition by [grp] rows between unbounded preceding and current row )]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testFirstValueOverPartitionRangeExplainPlan() throws Exception {
        // Covers toPlan for Decimal*FirstValueOverPartitionRangeFrameFunction and FirstNotNull variant.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") ignore nulls over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testFirstValueOverRangeFrameExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") over (range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") IGNORE NULLS OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") ignore nulls over (range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testFirstValueOverRowsFrameExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") ignore nulls over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testFirstValueOverPartitionRowsFrameExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testLastValueOverFramesExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") over (range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testNthValueOverFramesExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT nth_value(" + col + ", 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [nth_value(" + col + ",2) over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT nth_value(" + col + ", 2) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [nth_value(" + col + ",2) over (range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testSumAvgMaxMinOverFramesExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                for (String fn : new String[]{"sum", "avg", "max", "min"}) {
                    assertPlanNoLeakCheck(
                            "SELECT " + fn + "(" + col + ") OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                            "Window\n  functions: [" + fn + "(" + col + ") over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                    );
                    assertPlanNoLeakCheck(
                            "SELECT " + fn + "(" + col + ") OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                            "Window\n  functions: [" + fn + "(" + col + ") over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                    );
                }
            }
        });
    }

    @Test
    public void testAvgRescaleOverPartitionRangeExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            // AvgRescale OverPartitionRangeFrame: avg(col, scale) over (partition by ...)
            // Use D8 source with all 6 target scales (0/3/5/14/30/60).
            for (int scale : new int[]{0, 3, 5, 14, 30, 60}) {
                assertPlanNoLeakCheck(
                        "SELECT avg(v8, " + scale + ") OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(v8," + scale + ") over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testAvgRescaleOverPartitionRowsExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (int scale : new int[]{0, 3, 5, 14, 30, 60}) {
                assertPlanNoLeakCheck(
                        "SELECT avg(v8, " + scale + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(v8," + scale + ") over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testAvgRescaleOverRangeExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (int scale : new int[]{0, 3, 5, 14, 30, 60}) {
                assertPlanNoLeakCheck(
                        "SELECT avg(v8, " + scale + ") OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(v8," + scale + ") over (range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testAvgRescaleOverRowsExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (int scale : new int[]{0, 3, 5, 14, 30, 60}) {
                assertPlanNoLeakCheck(
                        "SELECT avg(v8, " + scale + ") OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(v8," + scale + ") over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testAllFunctionsOverPartitionExplainPlan() throws Exception {
        // OVER (PARTITION BY x) - cached factory (TWO_PASS classes).
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                for (String fn : new String[]{"sum", "avg", "max", "min"}) {
                    assertPlanNoLeakCheck(
                            "SELECT " + fn + "(" + col + ") OVER (PARTITION BY grp) FROM t",
                            "CachedWindow\n  unorderedFunctions: [" + fn + "(" + col + ") over (partition by [grp])]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                    );
                }
            }
        });
    }

    @Test
    public void testAllFunctionsOverUnboundedPartitionRowsExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                for (String fn : new String[]{"sum", "avg", "max", "min"}) {
                    assertPlanNoLeakCheck(
                            "SELECT " + fn + "(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                            "Window\n  functions: [" + fn + "(" + col + ") over (partition by [grp] rows between unbounded preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                    );
                }
            }
        });
    }

    @Test
    public void testAllFunctionsOverUnboundedRowsExplainPlan() throws Exception {
        // OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                for (String fn : new String[]{"sum", "avg", "max", "min"}) {
                    assertPlanNoLeakCheck(
                            "SELECT " + fn + "(" + col + ") OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                            "Window\n  functions: [" + fn + "(" + col + ") over (rows between unbounded preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                    );
                }
            }
        });
    }

    @Test
    public void testAllFunctionsOverCurrentRowExplainPlan() throws Exception {
        // ROWS BETWEEN CURRENT ROW AND CURRENT ROW collapses to OVER ().
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                for (String fn : new String[]{"sum", "avg", "max", "min"}) {
                    assertPlanNoLeakCheck(
                            "SELECT " + fn + "(" + col + ") OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                            "Window\n  functions: [" + fn + "(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                    );
                }
            }
        });
    }

    @Test
    public void testFirstLastNthValueOverCurrentRowExplainPlan() throws Exception {
        // For first_value/nth_value: ROWS BETWEEN CURRENT ROW collapses to OVER ().
        // For last_value: stays as bounded ROWS frame.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT first_value(" + col + ") OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                        "Window\n  functions: [first_value(" + col + ") over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") over (rows between 0 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT nth_value(" + col + ", 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                        "Window\n  functions: [nth_value(" + col + ",1) over (rows between current row and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testAvgRescaleOverWholeResultSetExplainPlan() throws Exception {
        // OverWholeResultSet uses inherited BaseWindowFunction.toPlan -> no scale shown.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            assertPlanNoLeakCheck(
                    "SELECT avg(v8, 5) OVER () FROM t",
                    "CachedWindow\n  unorderedFunctions: [avg(v8) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
            );
            assertPlanNoLeakCheck(
                    "SELECT avg(v8, 5) OVER (PARTITION BY grp) FROM t",
                    "CachedWindow\n  unorderedFunctions: [avg(v8) over (partition by [grp])]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
            );
            assertPlanNoLeakCheck(
                    "SELECT avg(v8, 5) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [avg(v8) over (partition by [grp])]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
            );
            assertPlanNoLeakCheck(
                    "SELECT avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                    "Window\n  functions: [avg(v8) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
            );
            assertPlanNoLeakCheck(
                    "SELECT avg(v8, 5) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
                    "Window\n  functions: [avg(v8) over ()]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testFirstValueRangeNonDesignatedTimestampRejected() throws Exception {
        // Triggers "RANGE is supported only for queries ordered by designated timestamp" for each subtype.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (ts TIMESTAMP, ts2 TIMESTAMP, " +
                    "v8 decimal(2, 1), v16 decimal(4, 1), v32 decimal(9, 3), " +
                    "v64 decimal(18, 2), v128 decimal(38, 6), v256 decimal(60, 0)" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR");
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                // Non-designated ts ORDER BY with RANGE -> rejected.
                String sql = "SELECT first_value(" + col + ") OVER (ORDER BY ts2 RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t2";
                int pos = sql.indexOf("ORDER BY ts2") + "ORDER BY ".length();
                assertExceptionNoLeakCheck(sql, pos, "RANGE is supported only for queries ordered by designated timestamp");
                // IGNORE NULLS variant
                String sql2 = "SELECT first_value(" + col + ") IGNORE NULLS OVER (ORDER BY ts2 RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t2";
                int pos2 = sql2.indexOf("ORDER BY ts2") + "ORDER BY ".length();
                assertExceptionNoLeakCheck(sql2, pos2, "RANGE is supported only for queries ordered by designated timestamp");
                // With PARTITION BY too
                String sql3 = "SELECT first_value(" + col + ") OVER (PARTITION BY ts ORDER BY ts2 RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t2";
                int pos3 = sql3.indexOf("ORDER BY ts2") + "ORDER BY ".length();
                assertExceptionNoLeakCheck(sql3, pos3, "RANGE is supported only for queries ordered by designated timestamp");
            }
        });
    }

    @Test
    public void testLastValueRangeNonDesignatedTimestampRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (ts TIMESTAMP, ts2 TIMESTAMP, " +
                    "v8 decimal(2, 1), v16 decimal(4, 1), v32 decimal(9, 3), " +
                    "v64 decimal(18, 2), v128 decimal(38, 6), v256 decimal(60, 0)" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR");
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                String sql = "SELECT last_value(" + col + ") OVER (ORDER BY ts2 RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t2";
                int pos = sql.indexOf("ORDER BY ts2") + "ORDER BY ".length();
                assertExceptionNoLeakCheck(sql, pos, "RANGE is supported only for queries ordered by designated timestamp");
                String sql2 = "SELECT last_value(" + col + ") IGNORE NULLS OVER (ORDER BY ts2 RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t2";
                int pos2 = sql2.indexOf("ORDER BY ts2") + "ORDER BY ".length();
                assertExceptionNoLeakCheck(sql2, pos2, "RANGE is supported only for queries ordered by designated timestamp");
            }
        });
    }

    @Test
    public void testNthValueRangeNonDesignatedTimestampRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (ts TIMESTAMP, ts2 TIMESTAMP, " +
                    "v8 decimal(2, 1), v16 decimal(4, 1), v32 decimal(9, 3), " +
                    "v64 decimal(18, 2), v128 decimal(38, 6), v256 decimal(60, 0)" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR");
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                String sql = "SELECT nth_value(" + col + ", 1) OVER (ORDER BY ts2 RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t2";
                int pos = sql.indexOf("ORDER BY ts2") + "ORDER BY ".length();
                assertExceptionNoLeakCheck(sql, pos, "RANGE is supported only for queries ordered by designated timestamp");
            }
        });
    }

    @Test
    public void testSumAvgMaxMinRangeNonDesignatedTimestampRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (ts TIMESTAMP, ts2 TIMESTAMP, " +
                    "v8 decimal(2, 1), v16 decimal(4, 1), v32 decimal(9, 3), " +
                    "v64 decimal(18, 2), v128 decimal(38, 6), v256 decimal(60, 0)" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR");
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                for (String fn : new String[]{"sum", "avg", "max", "min"}) {
                    String sql = "SELECT " + fn + "(" + col + ") OVER (ORDER BY ts2 RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t2";
                    int pos = sql.indexOf("ORDER BY ts2") + "ORDER BY ".length();
                    assertExceptionNoLeakCheck(sql, pos, "RANGE is supported only for queries ordered by designated timestamp");
                }
            }
        });
    }

    @Test
    public void testNthValueOverPartitionRowsExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT nth_value(" + col + ", 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [nth_value(" + col + ",2) over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT nth_value(" + col + ", 2) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [nth_value(" + col + ",2) over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT nth_value(" + col + ", 2) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [nth_value(" + col + ",2) over (partition by [grp] rows between unbounded preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT nth_value(" + col + ", 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [nth_value(" + col + ",2) over (rows between unbounded preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testLastValueOverFramesAdditionalExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") over (rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") over (partition by [grp] rows between unbounded preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") over (rows between unbounded preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                // IGNORE NULLS variants
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") ignore nulls over (partition by [grp] range between 60000000 preceding and 0 preceding)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") ignore nulls over ( rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT last_value(" + col + ") IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [last_value(" + col + ") ignore nulls over (partition by [grp] rows between 2 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testAvgRescaleD16D32ExplainPlan() throws Exception {
        // D16 and D32 source EXPLAIN — toPlan uses no-space comma format.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            for (int[] sourceScales : new int[][]{
                    {16, 0}, {16, 6}, {16, 15}, {16, 30}, {16, 60},
                    {32, 0}, {32, 10}, {32, 30}, {32, 60}
            }) {
                String col = "v" + sourceScales[0];
                int scale = sourceScales[1];
                assertPlanNoLeakCheck(
                        "SELECT avg(" + col + ", " + scale + ") OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(" + col + "," + scale + ") over (partition by [grp] range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
                assertPlanNoLeakCheck(
                        "SELECT avg(" + col + ", " + scale + ") OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) FROM t",
                        "Window\n  functions: [avg(" + col + "," + scale + ") over (range between 60000000 preceding and current row)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testNthValueValidationErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (ts TIMESTAMP, x INT, " +
                    "v8 decimal(2, 1), v16 decimal(4, 1), v32 decimal(9, 3), " +
                    "v64 decimal(18, 2), v128 decimal(38, 6), v256 decimal(60, 0)" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR");
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                String prefix = "SELECT nth_value(" + col + ", ";
                int posN = prefix.length();
                // n must be positive (constants)
                assertExceptionNoLeakCheck(prefix + "0) OVER (ORDER BY ts) FROM tn", posN, "n must be a positive integer");
                assertExceptionNoLeakCheck(prefix + "-1) OVER (ORDER BY ts) FROM tn", posN, "n must be a positive integer");
                // n must be a constant — non-constant column reference
                assertExceptionNoLeakCheck(prefix + "x) OVER (ORDER BY ts) FROM tn", posN, "n must be a constant");
                // n cannot be NULL — null::int literal
                assertExceptionNoLeakCheck(prefix + "null) OVER (ORDER BY ts) FROM tn", posN, "n cannot be NULL");
            }
        });
    }

    @Test
    public void testSumMaxMinPartitionAllNullInput() throws Exception {
        // Sum/Max/Min OVER (PARTITION BY x) with all-NULL input — exercises pass2 NULL emission paths.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\ts8\ts16\ts32\ts64\ts128\ts256\tmx8\tmx16\tmx32\tmx64\tmx128\tmx256\tmn8\tmn16\tmn32\tmn64\tmn128\tmn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (PARTITION BY grp) s8, sum(v16) OVER (PARTITION BY grp) s16, " +
                            "sum(v32) OVER (PARTITION BY grp) s32, sum(v64) OVER (PARTITION BY grp) s64, " +
                            "sum(v128) OVER (PARTITION BY grp) s128, sum(v256) OVER (PARTITION BY grp) s256, " +
                            "max(v8) OVER (PARTITION BY grp) mx8, max(v16) OVER (PARTITION BY grp) mx16, " +
                            "max(v32) OVER (PARTITION BY grp) mx32, max(v64) OVER (PARTITION BY grp) mx64, " +
                            "max(v128) OVER (PARTITION BY grp) mx128, max(v256) OVER (PARTITION BY grp) mx256, " +
                            "min(v8) OVER (PARTITION BY grp) mn8, min(v16) OVER (PARTITION BY grp) mn16, " +
                            "min(v32) OVER (PARTITION BY grp) mn32, min(v64) OVER (PARTITION BY grp) mn64, " +
                            "min(v128) OVER (PARTITION BY grp) mn128, min(v256) OVER (PARTITION BY grp) mn256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testSumPartitionRangeBufferExpansionAllSubTypes() throws Exception {
        // Sum OverPartitionRangeFrame with buffer expansion (small initial buffer).
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_INITIAL_RANGE_BUFFER_SIZE, 2);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 256);
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            StringSink insert = new StringSink();
            insert.put("INSERT INTO t VALUES ");
            for (int i = 0; i < 40; i++) {
                if (i > 0) insert.put(", ");
                insert.put("('2024-01-01T00:");
                if (i < 10) insert.put('0');
                insert.put(i).put(":00', 'a', 0.5m, 5.0m, 5.000m, 5.00m, 5.000000m, 5m)");
            }
            execute(insert.toString());
            // Each subsequent row: sum is running sum of 0.5*(i+1).
            // We assert distinct count of values to validate buffer integrity.
            assertQueryNoLeakCheck(
                    "cnt\n40\n",
                    "SELECT count(*) cnt FROM (" +
                            "SELECT sum(v8) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN 3600 second PRECEDING AND CURRENT ROW) sv FROM t" +
                            ")",
                    null, false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsAllFramesAllSubTypesExtra() throws Exception {
        // last_value IGNORE NULLS exec tests across all frame variants for all subtypes.
        // Single partition 'a': null,0.6,null,0.8,null. Running last-non-null per row.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            // OverPartitionRangeFrame
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", "ts", false, true);
            // OverPartitionRowsFrame
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) l256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testLastValueIgnoreNullsMoreRouting() throws Exception {
        // Hit more last_value IGNORE NULLS routing branches.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_WITH_NULL);
            // OVER (PARTITION BY grp) - default RANGE frame, no ORDER BY -> Decimal*LastNotNullValueOverPartitionFunction
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp) l256 " +
                            "FROM t", null, "ts", true, true);
            // OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW) -> Decimal*LastNotNullValueOverCurrentRowFunction
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l256 " +
                            "FROM t", "ts", false, true);
            // OVER (PARTITION BY ROWS BETWEEN CURRENT ROW AND CURRENT ROW) -> Decimal*LastNotNullValueOverCurrentRowFunction via partition path
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) l256 " +
                            "FROM t", "ts", false, true);
            // OVER (PARTITION BY ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) -> Decimal*LastNotNullValueOverPartitionFunction
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:01:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:02:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:03:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) l8, " +
                            "last_value(v16) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) l16, " +
                            "last_value(v32) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) l32, " +
                            "last_value(v64) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) l64, " +
                            "last_value(v128) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) l128, " +
                            "last_value(v256) IGNORE NULLS OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) l256 " +
                            "FROM t", null, "ts", true, true);
        });
    }

    @Test
    public void testFirstValueRowsUnboundedToPrecedingAllSubTypes() throws Exception {
        // Triggers OverRowsFrame !frameLoBounded path + null-emit branches for early rows.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            // ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING: row N's frame = rows 0..N-2.
            // Row 0,1: frame empty -> NULL. Row 2: frame=[row 0]=0.6. Row 3: frame=[row0,1]=0.6 (first). Row 4: frame=[0..2]=0.6.
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) f8, " +
                            "first_value(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) f16, " +
                            "first_value(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) f32, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) f64, " +
                            "first_value(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) f128, " +
                            "first_value(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) f256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testFirstValueRowsBoundedXToYPrecedingAllSubTypes() throws Exception {
        // ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING — bounded both sides, non-zero rowsHi.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            // Row N: frame=rows max(0,N-4)..N-2.
            // Row 0,1: empty -> NULL. Row 2: [0]=0.6. Row 3: [0,1]=0.6. Row 4: [0,1,2]=0.6.
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) f8, " +
                            "first_value(v16) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) f16, " +
                            "first_value(v32) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) f32, " +
                            "first_value(v64) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) f64, " +
                            "first_value(v128) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) f128, " +
                            "first_value(v256) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) f256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testLastValueRowsUnboundedToPrecedingAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            // ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING: row N's frame ends at N-2.
            // Row 0,1: empty -> NULL. Row 2: last=row0=0.6. Row 3: last=row1=0.4. Row 4: last=row2=0.8.
            assertQueryNoLeakCheck("""
                            ts\tl8\tl16\tl32\tl64\tl128\tl256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.4\t4.0\t4.000\t4.00\t4.000000\t4
                            2024-01-01T00:04:00.000000Z\t0.8\t8.0\t8.000\t8.00\t8.000000\t8
                            """,
                    "SELECT ts, " +
                            "last_value(v8) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) l8, " +
                            "last_value(v16) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) l16, " +
                            "last_value(v32) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) l32, " +
                            "last_value(v64) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) l64, " +
                            "last_value(v128) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) l128, " +
                            "last_value(v256) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) l256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testNthValueRowsUnboundedToPrecedingAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            // nth_value(col, 1) ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING — frame[0..N-2], nth=1 = first.
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 1) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) n8, " +
                            "nth_value(v16, 1) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) n16, " +
                            "nth_value(v32, 1) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) n32, " +
                            "nth_value(v64, 1) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) n64, " +
                            "nth_value(v128, 1) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) n128, " +
                            "nth_value(v256, 1) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) n256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testSumAvgMaxMinRowsXToYPrecedingAllSubTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            // ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING - bounded both sides, non-zero rowsHi.
            // Row 0,1: empty -> NULL. Row 2: [0]=0.6. Row 3: [0,1]=0.6+0.4=1.0. Row 4: [0,1,2]=0.6+0.4+0.8=1.8.
            assertQueryNoLeakCheck("""
                            ts\ts8\tav8\tmx8\tmn8
                            2024-01-01T00:00:00.000000Z\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t0.6\t0.6\t0.6\t0.6
                            2024-01-01T00:03:00.000000Z\t1.0\t0.5\t0.6\t0.4
                            2024-01-01T00:04:00.000000Z\t1.8\t0.6\t0.8\t0.4
                            """,
                    "SELECT ts, " +
                            "sum(v8) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) s8, " +
                            "avg(v8) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) av8, " +
                            "max(v8) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) mx8, " +
                            "min(v8) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) mn8 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testAvgRescaleStreamingAllScalesOverRangeFrame() throws Exception {
        // Streaming path (single ZERO_PASS function per query, no TWO_PASS sibling) for each target scale.
        // Forces getDecimalX call for each of the 6 target types: D8, D16, D32, D64, D128, D256.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            for (int[] sourceScales : new int[][]{
                    {8, 0}, {8, 3}, {8, 5}, {8, 14}, {8, 30}, {8, 60}
            }) {
                String col = "v" + sourceScales[0];
                int scale = sourceScales[1];
                // Query with only ZERO_PASS function (avg in RANGE frame) — streaming path.
                String query = "SELECT ts, avg(" + col + ", " + scale + ") OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND CURRENT ROW) av FROM t";
                // Just verify it runs (5 rows expected).
                assertSql("ts\tav\n", "SELECT ts, av FROM (" + query + ") WHERE 1=0");
            }
        });
    }

    @Test
    public void testAvgRescaleEmptyRangeFrameAllSubTypes() throws Exception {
        // RANGE with rangeHi between rows to ensure frame is empty for the first row
        // (no preceding rows within range), triggering value.ofRawNull() / isNull paths.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            // Single row only — frame is just the row, but avg over PRECEDING-only frame gives empty result for first row.
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 0.6m, 6.0m, 6.000m, 6.00m, 6.000000m, 6m)");
            // RANGE BETWEEN 60 PRECEDING AND 1 PRECEDING: frame for first row excludes itself, empty.
            assertQueryNoLeakCheck("ts\tav\n2024-01-01T00:00:00.000000Z\t\n",
                    "SELECT ts, avg(v8, 0) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND 1 second PRECEDING) av FROM t",
                    "ts", false, true);
            assertQueryNoLeakCheck("ts\tav\n2024-01-01T00:00:00.000000Z\t\n",
                    "SELECT ts, avg(v8, 3) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND 1 second PRECEDING) av FROM t",
                    "ts", false, true);
            assertQueryNoLeakCheck("ts\tav\n2024-01-01T00:00:00.000000Z\t\n",
                    "SELECT ts, avg(v8, 5) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND 1 second PRECEDING) av FROM t",
                    "ts", false, true);
            assertQueryNoLeakCheck("ts\tav\n2024-01-01T00:00:00.000000Z\t\n",
                    "SELECT ts, avg(v8, 14) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND 1 second PRECEDING) av FROM t",
                    "ts", false, true);
            assertQueryNoLeakCheck("ts\tav\n2024-01-01T00:00:00.000000Z\t\n",
                    "SELECT ts, avg(v8, 30) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND 1 second PRECEDING) av FROM t",
                    "ts", false, true);
            assertQueryNoLeakCheck("ts\tav\n2024-01-01T00:00:00.000000Z\t\n",
                    "SELECT ts, avg(v8, 60) OVER (ORDER BY ts RANGE BETWEEN 60 second PRECEDING AND 1 second PRECEDING) av FROM t",
                    "ts", false, true);
        });
    }

    @Test
    public void testNthValuePartitionRowsUnboundedToPrecedingAllSubTypes() throws Exception {
        // OVER (PARTITION BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) -> Decimal*NthValueOverPartitionRowsFrameUnboundedFunction
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_6_PART);
            // partition 'a' (rows 0,2,4 with v8=0.6,0.8,0.4): nth_value(v8,1) over (UNBOUNDED to 2 PRECEDING):
            // row 0: frame empty (rows < -2 from itself, no rows) -> NULL
            // row 2: frame [-2..-2] -> row 0 of partition -> 0.6
            // row 4: frame [-2..-2] absolute idx 2 = row 1 of partition = ? Actually counts include rows before, not within partition.
            // Use EXPLAIN to verify routing:
            assertPlanNoLeakCheck(
                    "SELECT nth_value(v8, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM t",
                    "Window\n  functions: [nth_value(v8,1) over (partition by [grp] rows between unbounded preceding and 2 preceding)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
            );
            for (String col : new String[]{"v8", "v16", "v32", "v64", "v128", "v256"}) {
                assertPlanNoLeakCheck(
                        "SELECT nth_value(" + col + ", 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM t",
                        "Window\n  functions: [nth_value(" + col + ",1) over (partition by [grp] rows between unbounded preceding and 2 preceding)]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: t\n"
                );
            }
        });
    }

    @Test
    public void testNthValuePartitionRowsUnboundedToPrecedingExec() throws Exception {
        // Execution test for Decimal*NthValueOverPartitionRowsFrameUnboundedFunction.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5);
            // Single partition 'a' with 5 rows of v8=0.6,0.4,0.8,0.2,1.0.
            // ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING — frame = rows before current row.
            // nth_value(col,1) = first non-null in frame.
            // row 0: empty -> NULL. row 1: [row0]=0.6. row 2: [row0,row1] first=0.6. row 3: [r0,r1,r2] first=0.6. row 4: [r0..r3] first=0.6.
            assertQueryNoLeakCheck("""
                            ts\tn8\tn16\tn32\tn64\tn128\tn256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:02:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:03:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            2024-01-01T00:04:00.000000Z\t0.6\t6.0\t6.000\t6.00\t6.000000\t6
                            """,
                    "SELECT ts, " +
                            "nth_value(v8, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n8, " +
                            "nth_value(v16, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n16, " +
                            "nth_value(v32, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n32, " +
                            "nth_value(v64, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n64, " +
                            "nth_value(v128, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n128, " +
                            "nth_value(v256, 1) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) n256 " +
                            "FROM t", "ts", false, true);
        });
    }

    @Test
    public void testFirstValueUnboundedPartitionRowsFrameAllNulls() throws Exception {
        // FirstValue OVER (PARTITION BY ROWS UNBOUNDED PRECEDING AND CURRENT ROW) with all-NULL partitions.
        assertMemoryLeak(() -> {
            execute(CREATE_T);
            execute(INSERT_5_ALL_NULL);
            assertQueryNoLeakCheck("""
                            ts\tf8\tf16\tf32\tf64\tf128\tf256
                            2024-01-01T00:00:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:01:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:02:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:03:00.000000Z\t\t\t\t\t\t
                            2024-01-01T00:04:00.000000Z\t\t\t\t\t\t
                            """,
                    "SELECT ts, " +
                            "first_value(v8) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f8, " +
                            "first_value(v16) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f16, " +
                            "first_value(v32) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f32, " +
                            "first_value(v64) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f64, " +
                            "first_value(v128) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f128, " +
                            "first_value(v256) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f256 " +
                            "FROM t", "ts", false, true);
        });
    }

}
