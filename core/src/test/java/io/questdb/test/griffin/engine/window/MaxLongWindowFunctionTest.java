/*******************************************************************************
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

public class MaxLongWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testMaxLongLargeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 9223372036854775806L, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 9223372036854775807L, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', -9223372036854775807L, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_val
                            2021-01-01T00:00:00.000000Z\t9223372036854775806\tA\t9223372036854775807
                            2021-01-02T00:00:00.000000Z\t9223372036854775807\tA\t9223372036854775807
                            2021-01-03T00:00:00.000000Z\t-9223372036854775807\tA\t9223372036854775807
                            """,
                    "SELECT ts, val, grp, max(val) OVER (PARTITION BY grp) as max_val FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxLongOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 100, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 300, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 200, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 150, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 250, 'B')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 180, 'B')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_val
                            2021-01-01T00:00:00.000000Z\t100\tA\t300
                            2021-01-02T00:00:00.000000Z\t300\tA\t300
                            2021-01-03T00:00:00.000000Z\t200\tA\t300
                            2021-01-04T00:00:00.000000Z\t150\tB\t250
                            2021-01-05T00:00:00.000000Z\t250\tB\t250
                            2021-01-06T00:00:00.000000Z\t180\tB\t250
                            """,
                    "SELECT ts, val, grp, max(val) OVER (PARTITION BY grp) as max_val FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxLongOverPartitionOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 100, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 300, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 200, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 150, 'B')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 250, 'B')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 180, 'B')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_val
                            2021-01-01T00:00:00.000000Z\t100\tA\t100
                            2021-01-02T00:00:00.000000Z\t300\tA\t300
                            2021-01-03T00:00:00.000000Z\t200\tA\t300
                            2021-01-04T00:00:00.000000Z\t150\tB\t150
                            2021-01-05T00:00:00.000000Z\t250\tB\t250
                            2021-01-06T00:00:00.000000Z\t180\tB\t250
                            """,
                    "SELECT ts, val, grp, max(val) OVER (PARTITION BY grp ORDER BY ts) as max_val FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxLongOverPartitionRowsBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 100, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 300, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 200, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 400, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 150, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_val
                            2021-01-01T00:00:00.000000Z\t100\tA\t100
                            2021-01-02T00:00:00.000000Z\t300\tA\t300
                            2021-01-03T00:00:00.000000Z\t200\tA\t300
                            2021-01-04T00:00:00.000000Z\t400\tA\t400
                            2021-01-05T00:00:00.000000Z\t150\tA\t400
                            """,
                    "SELECT ts, val, grp, max(val) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN 1 PRECEDING AND Current row) as max_val FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxLongOverPartitionRowsCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 100, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 300, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 200, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_val
                            2021-01-01T00:00:00.000000Z\t100\tA\t100
                            2021-01-02T00:00:00.000000Z\t300\tA\t300
                            2021-01-03T00:00:00.000000Z\t200\tA\t200
                            """,
                    "SELECT ts, val, grp, max(val) OVER (PARTITION BY grp ORDER BY ts ROWS CURRENT ROW) as max_val FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testMaxLongOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 100, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 300, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 200, 'B')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 150, 'B')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_val
                            2021-01-01T00:00:00.000000Z\t100\tA\t300
                            2021-01-02T00:00:00.000000Z\t300\tA\t300
                            2021-01-03T00:00:00.000000Z\t200\tB\t300
                            2021-01-04T00:00:00.000000Z\t150\tB\t300
                            """,
                    "SELECT ts, val, grp, max(val) OVER () as max_val FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxLongWithEmptyPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 100, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 200, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_val
                            2021-01-01T00:00:00.000000Z\t100\tA\t200
                            2021-01-02T00:00:00.000000Z\t200\tA\t200
                            """,
                    "SELECT ts, val, grp, max(val) OVER (PARTITION BY grp) as max_val FROM tab WHERE grp = 'A'",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxLongWithManyNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, other_val long, grp symbol) timestamp(ts)");

            // Create large dataset with many nulls to test null handling in max()
            execute("insert into tab select " +
                    "dateadd('s', x::int, '2021-01-01T00:00:00.000000Z')::timestamp as ts, " +
                    "x as val, " +
                    "case when x % 3 = 0 then null else (x * 2) end as other_val, " +
                    "case when x % 2 = 0 then 'A' else 'B' end as grp " +
                    "from long_sequence(10000)");

            // Verify max() correctly handles nulls in large dataset
            assertQueryNoLeakCheck(
                    """
                            grp\tnon_null_count\tmax_other_val
                            A\t3334\t20000
                            B\t3333\t19994
                            """,
                    "SELECT grp, " +
                            "count(other_val) as non_null_count, " +
                            "max(other_val) as max_other_val " +
                            "FROM tab GROUP BY grp ORDER BY grp",
                    null,
                    true,
                    true
            );

            // Test window function with nulls
            assertQueryNoLeakCheck(
                    """
                            grp\tmax_window_val
                            A\t20000
                            B\t19994
                            """,
                    "SELECT DISTINCT grp, max_window_val FROM (" +
                            "SELECT grp, max(other_val) OVER (PARTITION BY grp) as max_window_val FROM tab" +
                            ") ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxLongWithNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', -100, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', -300, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', -50, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 0, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 25, 'A')");

            assertQueryNoLeakCheck(
                    """
                            ts\tval\tgrp\tmax_val
                            2021-01-01T00:00:00.000000Z\t-100\tA\t25
                            2021-01-02T00:00:00.000000Z\t-300\tA\t25
                            2021-01-03T00:00:00.000000Z\t-50\tA\t25
                            2021-01-04T00:00:00.000000Z\t0\tA\t25
                            2021-01-05T00:00:00.000000Z\t25\tA\t25
                            """,
                    "SELECT ts, val, grp, max(val) OVER (PARTITION BY grp) as max_val FROM tab",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxLongWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, val long, other_val long, grp symbol) timestamp(ts)");
            execute("insert into tab values ('2021-01-01T00:00:00.000000Z', 100, 500, 'A')");
            execute("insert into tab values ('2021-01-02T00:00:00.000000Z', 200, null, 'A')");
            execute("insert into tab values ('2021-01-03T00:00:00.000000Z', 300, 800, 'A')");
            execute("insert into tab values ('2021-01-04T00:00:00.000000Z', 400, null, 'A')");
            execute("insert into tab values ('2021-01-05T00:00:00.000000Z', 500, 1600, 'A')");
            execute("insert into tab values ('2021-01-06T00:00:00.000000Z', 600, null, 'A')");

            // Test max() on long column containing nulls
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tother_val\tgrp\tmax_other_val
                            2021-01-01T00:00:00.000000Z\t100\t500\tA\t1600
                            2021-01-02T00:00:00.000000Z\t200\tnull\tA\t1600
                            2021-01-03T00:00:00.000000Z\t300\t800\tA\t1600
                            2021-01-04T00:00:00.000000Z\t400\tnull\tA\t1600
                            2021-01-05T00:00:00.000000Z\t500\t1600\tA\t1600
                            2021-01-06T00:00:00.000000Z\t600\tnull\tA\t1600
                            """,
                    "SELECT ts, val, other_val, grp, max(other_val) OVER (PARTITION BY grp) as max_other_val FROM tab",
                    "ts",
                    true,
                    true
            );

            // Test with ORDER BY on long column with nulls
            assertQueryNoLeakCheck(
                    """
                            ts\tval\tother_val\tgrp\tmax_other_val
                            2021-01-01T00:00:00.000000Z\t100\t500\tA\t500
                            2021-01-02T00:00:00.000000Z\t200\tnull\tA\t500
                            2021-01-03T00:00:00.000000Z\t300\t800\tA\t800
                            2021-01-04T00:00:00.000000Z\t400\tnull\tA\t800
                            2021-01-05T00:00:00.000000Z\t500\t1600\tA\t1600
                            2021-01-06T00:00:00.000000Z\t600\tnull\tA\t1600
                            """,
                    "SELECT ts, val, other_val, grp, max(other_val) OVER (PARTITION BY grp ORDER BY ts) as max_other_val FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }
}
