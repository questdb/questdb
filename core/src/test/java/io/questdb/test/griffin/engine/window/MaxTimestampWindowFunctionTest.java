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

package io.questdb.test.griffin.engine.window;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class MaxTimestampWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testMaxTimestampOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tC\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER () as max_ts FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampWithPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_by_grp\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tC\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp) as max_ts_by_grp FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 3, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 7, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 9, 'C')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_cumulative\n" +
                            "2021-01-01T00:00:00.000000Z\t5\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t3\tB\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t7\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t2\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t9\tC\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts) as max_ts_cumulative FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithPartitionByAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 6, 'C')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_running\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-06T00:00:00.000000Z\t6\tC\t2021-01-06T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts) as max_ts_running FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithRowsBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_window\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as max_ts_window FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithRangeBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T12:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-02T12:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_range\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-01T12:00:00.000000Z\t2\tA\t2021-01-01T12:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t3\tA\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-02T12:00:00.000000Z\t4\tA\t2021-01-02T12:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t5\tA\t2021-01-03T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN '1' DAY PRECEDING AND CURRENT ROW) as max_ts_range FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol, other_ts timestamp) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A', '2021-01-01T12:00:00.000000Z')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A', null)");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A', '2021-01-03T12:00:00.000000Z')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A', null)");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A', '2021-01-05T12:00:00.000000Z')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tother_ts\tmax_other_ts\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T12:00:00.000000Z\t2021-01-05T12:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tA\t\t2021-01-05T12:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T12:00:00.000000Z\t2021-01-05T12:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tA\t\t2021-01-05T12:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T12:00:00.000000Z\t2021-01-05T12:00:00.000000Z\n",
                    "SELECT ts, val, grp, other_ts, max(other_ts) OVER () as max_other_ts FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampWithMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp1 symbol, grp2 symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A', 'X')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A', 'Y')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'B', 'X')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B', 'Y')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A', 'X')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 6, 'B', 'X')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp1\tgrp2\tmax_ts_by_grps\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\tX\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tA\tY\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tB\tX\t2021-01-06T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tB\tY\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\tX\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-06T00:00:00.000000Z\t6\tB\tX\t2021-01-06T00:00:00.000000Z\n",
                    "SELECT ts, val, grp1, grp2, max(ts) OVER (PARTITION BY grp1, grp2) as max_ts_by_grps FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampWithUnboundedPreceding() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_unbounded\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_ts_unbounded FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithEmptyPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts\n",
                    "SELECT ts, val, grp, max(ts) OVER () as max_ts FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampWithSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER () as max_ts FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampWithDuplicateTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-01T00:00:00.000000Z\t2\tB\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t4\tB\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t5\tC\t2021-01-03T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER () as max_ts FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampWithOrderByDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_desc\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tC\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts DESC) as max_ts_desc FROM tab ORDER BY ts DESC",
                    "ts###desc",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithComplexFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'B')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 6, 'B')");
            execute("insert into tab values ('2021-01-07T00:00:00.000000Z', 7, 'C')");
            execute("insert into tab values ('2021-01-08T00:00:00.000000Z', 8, 'C')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_complex\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tB\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-06T00:00:00.000000Z\t6\tB\t2021-01-06T00:00:00.000000Z\n" +
                            "2021-01-07T00:00:00.000000Z\t7\tC\t2021-01-07T00:00:00.000000Z\n" +
                            "2021-01-08T00:00:00.000000Z\t8\tC\t2021-01-08T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as max_ts_complex FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab select timestamp_sequence(0, 1000000), x, rnd_symbol('A','B','C','D','E') from long_sequence(100000)");

            assertQueryNoLeakCheck(
                    "cnt\n" +
                            "100000\n",
                    "SELECT count(*) as cnt FROM (SELECT ts, max(ts) OVER (PARTITION BY grp) as max_ts FROM tab)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    "grp\tmax_ts\n" +
                            "A\t2021-01-03T00:00:00.000000Z\n" +
                            "B\t2021-01-04T00:00:00.000000Z\n" +
                            "C\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT DISTINCT grp, max_ts FROM (SELECT grp, max(ts) OVER (PARTITION BY grp) as max_ts FROM tab) ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab1 (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("create table tab2 (ts timestamp, val int, grp symbol) timestamp(ts)");
            
            execute("insert into tab1 values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab1 values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab1 values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            
            execute("insert into tab2 values ('2021-01-02T00:00:00.000000Z', 10, 'A')");
            execute("insert into tab2 values ('2021-01-03T00:00:00.000000Z', 20, 'B')");
            execute("insert into tab2 values ('2021-01-04T00:00:00.000000Z', 30, 'A')");

            assertSql(
                    "t1_ts\tt1_val\tt1_grp\tt2_ts\tt2_val\tttt\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-02T00:00:00.000000Z\t10\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-04T00:00:00.000000Z\t30\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-03T00:00:00.000000Z\t20\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-02T00:00:00.000000Z\t10\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-04T00:00:00.000000Z\t30\t2021-01-04T00:00:00.000000Z\n",
                    "SELECT t1.ts as t1_ts, t1.val as t1_val, t1.grp as t1_grp, t2.ts as t2_ts, t2.val as t2_val, " +
                            "CASE WHEN t1.ts > t2.ts THEN t1.ts ELSE t2.ts END as ttt " +
                            "FROM tab1 t1 JOIN tab2 t2 ON t1.grp = t2.grp ORDER BY t1.ts, t2.ts"
            );
            assertQueryNoLeakCheck(
                            "t1_ts\tt1_val\tt1_grp\tt2_ts\tt2_val\tmax_ts\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-02T00:00:00.000000Z\t10\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-04T00:00:00.000000Z\t30\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-03T00:00:00.000000Z\t20\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-02T00:00:00.000000Z\t10\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-04T00:00:00.000000Z\t30\t2021-01-04T00:00:00.000000Z\n",
                    "SELECT t1.ts as t1_ts, t1.val as t1_val, t1.grp as t1_grp, t2.ts as t2_ts, t2.val as t2_val, " +
                            "max(CASE WHEN t1.ts > t2.ts THEN t1.ts ELSE t2.ts END) OVER (PARTITION BY t1.grp) as max_ts " +
                            "FROM tab1 t1 JOIN tab2 t2 ON t1.grp = t2.grp ORDER BY t1.ts, t2.ts",
                    "t1_ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampWithMixedFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T06:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-01T12:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T18:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-02T06:00:00.000000Z', 6, 'A')");
            execute("insert into tab values ('2021-01-02T12:00:00.000000Z', 7, 'A')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_rows\tmax_range\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-01T06:00:00.000000Z\t2\tA\t2021-01-01T06:00:00.000000Z\t2021-01-01T06:00:00.000000Z\n" +
                            "2021-01-01T12:00:00.000000Z\t3\tA\t2021-01-01T12:00:00.000000Z\t2021-01-01T12:00:00.000000Z\n" +
                            "2021-01-01T18:00:00.000000Z\t4\tA\t2021-01-01T18:00:00.000000Z\t2021-01-01T18:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t5\tA\t2021-01-02T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-02T06:00:00.000000Z\t6\tA\t2021-01-02T06:00:00.000000Z\t2021-01-02T06:00:00.000000Z\n" +
                            "2021-01-02T12:00:00.000000Z\t7\tA\t2021-01-02T12:00:00.000000Z\t2021-01-02T12:00:00.000000Z\n",
                    "SELECT ts, val, grp, " +
                            "max(ts) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as max_rows, " +
                            "max(ts) OVER (ORDER BY ts RANGE BETWEEN '6' HOUR PRECEDING AND CURRENT ROW) as max_range " +
                            "FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampWithRowsPrecedingOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_preceding\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as max_ts_preceding FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampOnNonDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', '2021-01-05T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', '2021-01-04T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', '2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', '2021-01-02T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', '2021-01-01T00:00:00.000000Z', 5, 'C')");

            assertQueryNoLeakCheck(
                    "ts\tother_ts\tval\tgrp\tmax_other_ts\n" +
                            "2021-01-01T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2021-01-04T00:00:00.000000Z\t2\tB\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z\t4\tB\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z\t5\tC\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, other_ts, val, grp, max(other_ts) OVER () as max_other_ts FROM tab",
                    "ts",
                    true,
                    false
            );

            assertQueryNoLeakCheck(
                    "ts\tother_ts\tval\tgrp\tmax_other_ts_by_grp\n" +
                            "2021-01-01T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2021-01-04T00:00:00.000000Z\t2\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z\t5\tC\t2021-01-01T00:00:00.000000Z\n",
                    "SELECT ts, other_ts, val, grp, max(other_ts) OVER (PARTITION BY grp) as max_other_ts_by_grp FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampRangeOnNonDesignatedTimestampFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val int) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', '2021-01-01T12:00:00.000000Z', 1)");
            
            // This should fail because we're using RANGE with ORDER BY on a non-designated timestamp
            assertExceptionNoLeakCheck(
                    "SELECT ts, other_ts, val, max(ts) OVER (ORDER BY other_ts RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW) FROM tab",
                    49,
                    "RANGE is supported only for queries ordered by designated timestamp"
            );
        });
    }

    @Test 
    public void testMaxTimestampRangeWithSpecificBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T06:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-01T12:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T18:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-02T06:00:00.000000Z', 6, 'A')");

            // Test range with 12 hours preceding to current row (not unbounded)
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_12h\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-01T06:00:00.000000Z\t2\tA\t2021-01-01T06:00:00.000000Z\n" +
                            "2021-01-01T12:00:00.000000Z\t3\tA\t2021-01-01T12:00:00.000000Z\n" +
                            "2021-01-01T18:00:00.000000Z\t4\tA\t2021-01-01T18:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t5\tA\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-02T06:00:00.000000Z\t6\tA\t2021-01-02T06:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN '12' HOUR PRECEDING AND CURRENT ROW) as max_ts_12h FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithPartitionAndSpecificBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T06:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-01T12:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T18:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-02T06:00:00.000000Z', 6, 'B')");

            // Test partitioned range with specific bounds - this exercises the MaxMinOverPartitionRangeFrameFunction
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_by_grp_6h\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-01T06:00:00.000000Z\t2\tB\t2021-01-01T06:00:00.000000Z\n" +
                            "2021-01-01T12:00:00.000000Z\t3\tA\t2021-01-01T12:00:00.000000Z\n" +
                            "2021-01-01T18:00:00.000000Z\t4\tB\t2021-01-01T18:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t5\tA\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-02T06:00:00.000000Z\t6\tB\t2021-01-02T06:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '6' HOUR PRECEDING AND CURRENT ROW) as max_ts_by_grp_6h FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithBoundedFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T01:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-01T02:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T03:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-01T04:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-01T05:00:00.000000Z', 6, 'A')");

            // Test range between x preceding and y preceding (bounded frame - not touching current row)
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_bounded\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t\n" +
                            "2021-01-01T01:00:00.000000Z\t2\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-01T02:00:00.000000Z\t3\tA\t2021-01-01T01:00:00.000000Z\n" +
                            "2021-01-01T03:00:00.000000Z\t4\tA\t2021-01-01T02:00:00.000000Z\n" +
                            "2021-01-01T04:00:00.000000Z\t5\tA\t2021-01-01T03:00:00.000000Z\n" +
                            "2021-01-01T05:00:00.000000Z\t6\tA\t2021-01-01T04:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND '1' HOUR PRECEDING) as max_ts_bounded FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithPartitionedBoundedFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T01:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-01T02:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T03:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-01T04:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-01T05:00:00.000000Z', 6, 'B')");

            // Test partitioned range with bounded frame - this exercises the dequeMem path
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_part_bounded\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t\n" +
                            "2021-01-01T01:00:00.000000Z\t2\tB\t\n" +
                            "2021-01-01T02:00:00.000000Z\t3\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-01T03:00:00.000000Z\t4\tB\t2021-01-01T01:00:00.000000Z\n" +
                            "2021-01-01T04:00:00.000000Z\t5\tA\t2021-01-01T02:00:00.000000Z\n" +
                            "2021-01-01T05:00:00.000000Z\t6\tB\t2021-01-01T03:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND '1' HOUR PRECEDING) as max_ts_part_bounded FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithLargerTimeIntervals() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 6, 'A')");
            execute("insert into tab values ('2021-01-07T00:00:00.000000Z', 7, 'A')");

            // Test with day intervals to exercise larger time ranges
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_3d\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-06T00:00:00.000000Z\t6\tA\t2021-01-06T00:00:00.000000Z\n" +
                            "2021-01-07T00:00:00.000000Z\t7\tA\t2021-01-07T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN '3' DAY PRECEDING AND CURRENT ROW) as max_ts_3d FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeWithMicrosecondPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000100Z', 2, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000200Z', 3, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000300Z', 4, 'A')");
            execute("insert into tab values ('2021-01-01T00:00:00.000400Z', 5, 'A')");

            // Test with microsecond precision to exercise fine-grained time windows
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_micro\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-01T00:00:00.000100Z\t2\tA\t2021-01-01T00:00:00.000100Z\n" +
                            "2021-01-01T00:00:00.000200Z\t3\tA\t2021-01-01T00:00:00.000200Z\n" +
                            "2021-01-01T00:00:00.000300Z\t4\tA\t2021-01-01T00:00:00.000300Z\n" +
                            "2021-01-01T00:00:00.000400Z\t5\tA\t2021-01-01T00:00:00.000400Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts RANGE BETWEEN 150 MICROSECOND PRECEDING AND CURRENT ROW) as max_ts_micro FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRowsUnboundedPrecedingToCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW - exercises MaxMinOverUnboundedPartitionRowsFrameFunction
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_unbounded_rows\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_ts_unbounded_rows FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRowsCurrentRowOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test ROWS BETWEEN CURRENT ROW AND CURRENT ROW - exercises MaxMinOverCurrentRowFunction
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_current_only\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) as max_ts_current_only FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRowsWholePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING - exercises MaxMinOverPartitionFunction
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_whole_partition\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_ts_whole_partition FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampRowsFrameCombinations() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test multiple ROWS frame variations in one query to ensure different code paths work together
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tunbounded_to_current\tcurrent_only\twhole_partition\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\t2021-01-01T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tA\t2021-01-02T00:00:00.000000Z\t2021-01-02T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\t2021-01-03T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tA\t2021-01-04T00:00:00.000000Z\t2021-01-04T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as unbounded_to_current, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) as current_only, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as whole_partition " +
                            "FROM tab",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testMaxTimestampRowsUnboundedWithoutPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 2, 'B')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 4, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 5, 'A')");

            // Test without PARTITION BY to exercise different code path
            assertQueryNoLeakCheck(
                    "ts\tval\tgrp\tmax_ts_no_partition\n" +
                            "2021-01-01T00:00:00.000000Z\t1\tA\t2021-01-01T00:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t2\tB\t2021-01-02T00:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t3\tA\t2021-01-03T00:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t4\tB\t2021-01-04T00:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t5\tA\t2021-01-05T00:00:00.000000Z\n",
                    "SELECT ts, val, grp, max(ts) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_ts_no_partition FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxNonDesignatedTimestampWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val int, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', '2021-01-01T12:00:00.000000Z', 1, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', null, 2, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', '2021-01-03T08:00:00.000000Z', 3, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', null, 4, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', '2021-01-05T16:00:00.000000Z', 5, 'A')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', null, 6, 'A')");

            // Test max() on non-designated timestamp column containing nulls
            assertQueryNoLeakCheck(
                    "ts\tother_ts\tval\tgrp\tmax_other_ts\n" +
                            "2021-01-01T00:00:00.000000Z\t2021-01-01T12:00:00.000000Z\t1\tA\t2021-01-05T16:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t\t2\tA\t2021-01-05T16:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t2021-01-03T08:00:00.000000Z\t3\tA\t2021-01-05T16:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t\t4\tA\t2021-01-05T16:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t2021-01-05T16:00:00.000000Z\t5\tA\t2021-01-05T16:00:00.000000Z\n" +
                            "2021-01-06T00:00:00.000000Z\t\t6\tA\t2021-01-05T16:00:00.000000Z\n",
                    "SELECT ts, other_ts, val, grp, max(other_ts) OVER (PARTITION BY grp) as max_other_ts FROM tab",
                    "ts",
                    true,
                    false
            );

            // Test with ORDER BY on non-designated timestamp with nulls
            assertQueryNoLeakCheck(
                    "ts\tother_ts\tval\tgrp\tmax_other_ts_running\n" +
                            "2021-01-01T00:00:00.000000Z\t2021-01-01T12:00:00.000000Z\t1\tA\t2021-01-01T12:00:00.000000Z\n" +
                            "2021-01-02T00:00:00.000000Z\t\t2\tA\t2021-01-01T12:00:00.000000Z\n" +
                            "2021-01-03T00:00:00.000000Z\t2021-01-03T08:00:00.000000Z\t3\tA\t2021-01-03T08:00:00.000000Z\n" +
                            "2021-01-04T00:00:00.000000Z\t\t4\tA\t2021-01-03T08:00:00.000000Z\n" +
                            "2021-01-05T00:00:00.000000Z\t2021-01-05T16:00:00.000000Z\t5\tA\t2021-01-05T16:00:00.000000Z\n" +
                            "2021-01-06T00:00:00.000000Z\t\t6\tA\t2021-01-05T16:00:00.000000Z\n",
                    "SELECT ts, other_ts, val, grp, max(other_ts) OVER (PARTITION BY grp ORDER BY ts ROWS UNBOUNDED PRECEDING) as max_other_ts_running FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeFrameBufferOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            
            // Create many partitions (>40) with dense timestamps to trigger deque resize
            // Each partition will have many overlapping frames requiring deque growth
            execute("insert into tab select " +
                    "dateadd('s', ((x-1) * 10)::int, '2021-01-01T00:00:00.000000Z'::timestamp) as ts, " +
                    "x as val, " +
                    "'grp_' || ((x-1) % 100) as grp " +  // 100 different partitions
                    "from long_sequence(50000)");

            // Test with wide time window that will cause deque to hold many max values simultaneously
            // With 10-second intervals and 1-hour window, each partition will have ~360 overlapping frames
            assertQueryNoLeakCheck(
                    "cnt\tpartitions\n" +
                            "50000\t100\n",
                    "SELECT count(*) as cnt, count(distinct grp), count(max_ts_large_window)  as partitions FROM (" +
                            "SELECT ts, val, grp, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW) as max_ts_large_window " +
                            "FROM tab" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeFrameDequeOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            
            // Create scenario that will trigger deque overflow in bounded range frames
            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "x as val, " +
                    "case when x % 5 = 0 then 'A' else 'B' end as grp " +
                    "from long_sequence(50000)");

            // Test bounded range frame that exercises dequeMem overflow handling
            assertQueryNoLeakCheck(
                    "cnt\tmax_val\n" +
                            "50000\t50000\n",
                    "SELECT count(*) as cnt, max(val) as max_val FROM (" +
                            "SELECT ts, val, grp, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND '1' HOUR PRECEDING) as max_ts_bounded " +
                            "FROM tab" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeFrameWithManyPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            
            // Create many partitions to test Map resizing and partition management
            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "x as val, " +
                    "'grp_' || (x % 1000) as grp " +
                    "from long_sequence(25000)");

            // Test with many partitions to exercise Map operations in MaxMinOverPartitionRangeFrameFunction
            assertQueryNoLeakCheck(
                    "partition_count\ttotal_rows\n" +
                            "1000\t25000\n",
                    "SELECT count(distinct grp) as partition_count, count(*) as total_rows FROM (" +
                            "SELECT ts, val, grp, " +
                            "max(ts) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN '30' MINUTE PRECEDING AND CURRENT ROW) as max_ts " +
                            "FROM tab" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxNonDesignatedTimestampWithManyNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, other_ts timestamp, val long, grp symbol) timestamp(ts)");
            
            // Create large dataset with many nulls to test null handling in max()
            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "case when x % 3 = 0 then null else dateadd('h', (x % 24)::int, '2021-01-01T00:00:00.000000Z') end as other_ts, " +
                    "x as val, " +
                    "case when x % 2 = 0 then 'A' else 'B' end as grp " +
                    "from long_sequence(10000)");

            // Verify max() correctly handles nulls in large dataset
            assertQueryNoLeakCheck(
                    "grp\tnon_null_count\tmax_other_ts\n" +
                            "A\t3334\t2021-01-01T22:00:00.000000Z\n" +
                            "B\t3333\t2021-01-01T23:00:00.000000Z\n",
                    "SELECT grp, " +
                            "count(other_ts) as non_null_count, " +
                            "max(other_ts) as max_other_ts " +
                            "FROM tab GROUP BY grp ORDER BY grp",
                    null,
                    true,
                    true
            );

            // Test window function with nulls
            assertQueryNoLeakCheck(
                    "grp\tmax_window_ts\n" +
                            "A\t2021-01-01T22:00:00.000000Z\n" +
                            "B\t2021-01-01T23:00:00.000000Z\n",
                    "SELECT DISTINCT grp, max_window_ts FROM (" +
                            "SELECT grp, max(other_ts) OVER (PARTITION BY grp) as max_window_ts FROM tab" +
                            ") ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxTimestampRangeFrameEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            
            // Create edge case scenario: overlapping time ranges, identical timestamps
            execute("insert into tab values " +
                    "('2021-01-01T12:00:00.000000Z', 1, 'A'), " +
                    "('2021-01-01T12:00:00.000000Z', 2, 'A'), " + // duplicate timestamp
                    "('2021-01-01T12:00:01.000000Z', 3, 'A'), " +
                    "('2021-01-01T12:00:01.000000Z', 4, 'A'), " + // duplicate timestamp
                    "('2021-01-01T12:00:02.000000Z', 5, 'A'), " +
                    "('2021-01-01T12:00:03.000000Z', 6, 'A'), " +
                    "('2021-01-01T12:00:04.000000Z', 7, 'A'), " +
                    "('2021-01-01T12:00:05.000000Z', 8, 'A')");

            // Test precise boundary conditions in range frames
            assertQueryNoLeakCheck(
                    "ts\tval\tmax_ts_precise\n" +
                            "2021-01-01T12:00:00.000000Z\t1\t2021-01-01T12:00:02.000000Z\n" +
                            "2021-01-01T12:00:00.000000Z\t2\t2021-01-01T12:00:02.000000Z\n" +
                            "2021-01-01T12:00:01.000000Z\t3\t2021-01-01T12:00:03.000000Z\n" +
                            "2021-01-01T12:00:01.000000Z\t4\t2021-01-01T12:00:03.000000Z\n" +
                            "2021-01-01T12:00:02.000000Z\t5\t2021-01-01T12:00:04.000000Z\n" +
                            "2021-01-01T12:00:03.000000Z\t6\t2021-01-01T12:00:05.000000Z\n" +
                            "2021-01-01T12:00:04.000000Z\t7\t2021-01-01T12:00:05.000000Z\n" +
                            "2021-01-01T12:00:05.000000Z\t8\t2021-01-01T12:00:05.000000Z\n",
                    "SELECT ts, val, max(ts) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND '2' SECOND FOLLOWING) as max_ts_precise FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }
}