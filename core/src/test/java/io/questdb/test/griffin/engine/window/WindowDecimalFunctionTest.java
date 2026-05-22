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

    @Test
    public void testAvgDecimal64OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.00m), " +
                    "('2024-01-01T00:01:00', 'a', 20.00m), " +
                    "('2024-01-01T00:02:00', 'b', 30.00m), " +
                    "('2024-01-01T00:03:00', 'a', 30.00m), " +
                    "('2024-01-01T00:04:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.00\t20.00\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.00\t20.00\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.00\t35.00\n" +
                            "2024-01-01T00:03:00.000000Z\ta\t30.00\t20.00\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t40.00\t35.00\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64OverRowsBetweenPrecedingAndCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 20.00m), " +
                    "('2024-01-01T00:02:00', 30.00m), " +
                    "('2024-01-01T00:03:00', 40.00m)");
            assertSql("ts\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t10.00\n" +
                            "2024-01-01T00:01:00.000000Z\t20.00\t15.00\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t25.00\n" +
                            "2024-01-01T00:03:00.000000Z\t40.00\t35.00\n",
                    "SELECT ts, v, avg(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64OverUnboundedPreceding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 20.00m), " +
                    "('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t10.00\n" +
                            "2024-01-01T00:01:00.000000Z\t20.00\t15.00\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, avg(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', null), " +
                    "('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t20.00\n" +
                            "2024-01-01T00:01:00.000000Z\t\t20.00\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, avg(v) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal64Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 20.00m), " +
                    "('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t20.0000\n" +
                            "2024-01-01T00:01:00.000000Z\t20.00\t20.0000\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t20.0000\n",
                    "SELECT ts, v, avg(v, 4) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testCountDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.000000m), " +
                    "('2024-01-01T00:01:00', null), " +
                    "('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tc\n" +
                            "2024-01-01T00:00:00.000000Z\t10.000000\t2\n" +
                            "2024-01-01T00:01:00.000000Z\t\t2\n" +
                            "2024-01-01T00:02:00.000000Z\t30.000000\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal16() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.0m), " +
                    "('2024-01-01T00:01:00', null), " +
                    "('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tc\n" +
                            "2024-01-01T00:00:00.000000Z\t10.0\t2\n" +
                            "2024-01-01T00:01:00.000000Z\t\t2\n" +
                            "2024-01-01T00:02:00.000000Z\t30.0\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10m), " +
                    "('2024-01-01T00:01:00', null), " +
                    "('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tc\n" +
                            "2024-01-01T00:00:00.000000Z\t10\t2\n" +
                            "2024-01-01T00:01:00.000000Z\t\t2\n" +
                            "2024-01-01T00:02:00.000000Z\t30\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal32() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.000m), " +
                    "('2024-01-01T00:01:00', null), " +
                    "('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tc\n" +
                            "2024-01-01T00:00:00.000000Z\t10.000\t2\n" +
                            "2024-01-01T00:01:00.000000Z\t\t2\n" +
                            "2024-01-01T00:02:00.000000Z\t30.000\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', null), " +
                    "('2024-01-01T00:02:00', 30.00m), " +
                    "('2024-01-01T00:03:00', null)");
            assertSql("ts\tv\tc\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t2\n" +
                            "2024-01-01T00:01:00.000000Z\t\t2\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t2\n" +
                            "2024-01-01T00:03:00.000000Z\t\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal8() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1.0m), " +
                    "('2024-01-01T00:01:00', null), " +
                    "('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tc\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\t2\n" +
                            "2024-01-01T00:01:00.000000Z\t\t2\n" +
                            "2024-01-01T00:02:00.000000Z\t3.0\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64OverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 20.00m), " +
                    "('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tfv\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t10.00\n" +
                            "2024-01-01T00:01:00.000000Z\t20.00\t20.00\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t30.00\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.00m), " +
                    "('2024-01-01T00:01:00', 'a', 20.00m), " +
                    "('2024-01-01T00:02:00', 'b', 30.00m), " +
                    "('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tfv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.00\t10.00\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.00\t30.00\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.00\t30.00\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64WithIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', null), " +
                    "('2024-01-01T00:01:00', 20.00m), " +
                    "('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tfv\n" +
                            "2024-01-01T00:00:00.000000Z\t\t20.00\n" +
                            "2024-01-01T00:01:00.000000Z\t20.00\t20.00\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, first_value(v) IGNORE NULLS OVER () AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal8OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 1.0m), " +
                    "('2024-01-01T00:01:00', 'a', 2.0m), " +
                    "('2024-01-01T00:02:00', 'b', 3.0m), " +
                    "('2024-01-01T00:03:00', 'b', 4.0m)");
            assertSql("ts\tgrp\tv\tfv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t1.0\t1.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t2.0\t1.0\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t3.0\t3.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t4.0\t3.0\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp) AS fv FROM t");
        });
    }

    @Test
    public void testLagDecimal64Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 20.00m), " +
                    "('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlag_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t\n" +
                            "2024-01-01T00:01:00.000000Z\t20.00\t10.00\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, lag(v) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLagDecimal64WithDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 20.00m)");
            assertSql("ts\tv\tlag_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t99.99\n" +
                            "2024-01-01T00:01:00.000000Z\t20.00\t10.00\n",
                    "SELECT ts, v, lag(v, 1, 99.99::decimal(18, 2)) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLagDecimal128Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.000000m), " +
                    "('2024-01-01T00:01:00', 20.000000m), " +
                    "('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tlag_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.000000\t\n" +
                            "2024-01-01T00:01:00.000000Z\t20.000000\t10.000000\n" +
                            "2024-01-01T00:02:00.000000Z\t30.000000\t20.000000\n",
                    "SELECT ts, v, lag(v) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLagDecimal16Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.0m), " +
                    "('2024-01-01T00:01:00', 20.0m), " +
                    "('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tlag_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.0\t\n" +
                            "2024-01-01T00:01:00.000000Z\t20.0\t10.0\n" +
                            "2024-01-01T00:02:00.000000Z\t30.0\t20.0\n",
                    "SELECT ts, v, lag(v) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLagDecimal256Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10m), " +
                    "('2024-01-01T00:01:00', 20m), " +
                    "('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tlag_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10\t\n" +
                            "2024-01-01T00:01:00.000000Z\t20\t10\n" +
                            "2024-01-01T00:02:00.000000Z\t30\t20\n",
                    "SELECT ts, v, lag(v) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLagDecimal32Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.000m), " +
                    "('2024-01-01T00:01:00', 20.000m), " +
                    "('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tlag_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.000\t\n" +
                            "2024-01-01T00:01:00.000000Z\t20.000\t10.000\n" +
                            "2024-01-01T00:02:00.000000Z\t30.000\t20.000\n",
                    "SELECT ts, v, lag(v) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLagDecimal8Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1.0m), " +
                    "('2024-01-01T00:01:00', 2.0m), " +
                    "('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tlag_v\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\t\n" +
                            "2024-01-01T00:01:00.000000Z\t2.0\t1.0\n" +
                            "2024-01-01T00:02:00.000000Z\t3.0\t2.0\n",
                    "SELECT ts, v, lag(v) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLagDecimal8OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 1.0m), " +
                    "('2024-01-01T00:01:00', 'a', 2.0m), " +
                    "('2024-01-01T00:02:00', 'b', 3.0m), " +
                    "('2024-01-01T00:03:00', 'a', 4.0m), " +
                    "('2024-01-01T00:04:00', 'b', 5.0m)");
            assertSql("ts\tgrp\tv\tlag_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t1.0\t\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t2.0\t1.0\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t3.0\t\n" +
                            "2024-01-01T00:03:00.000000Z\ta\t4.0\t2.0\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t5.0\t3.0\n",
                    "SELECT ts, grp, v, lag(v) OVER (PARTITION BY grp) AS lag_v FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.00m), " +
                    "('2024-01-01T00:01:00', 'a', 20.00m), " +
                    "('2024-01-01T00:02:00', 'b', 30.00m), " +
                    "('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tlv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.00\t20.00\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.00\t20.00\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.00\t40.00\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.00\t40.00\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp) AS lv FROM t");
        });
    }

    @Test
    public void testLeadDecimal64Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 20.00m), " +
                    "('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlead_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t20.00\n" +
                            "2024-01-01T00:01:00.000000Z\t20.00\t30.00\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, lead(v) OVER () AS lead_v FROM t");
        });
    }

    @Test
    public void testLastValueDecimal32OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000m), " +
                    "('2024-01-01T00:01:00', 'a', 20.000m), " +
                    "('2024-01-01T00:02:00', 'b', 30.000m), " +
                    "('2024-01-01T00:03:00', 'b', 40.000m)");
            assertSql("ts\tgrp\tv\tlv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000\t20.000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.000\t20.000\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.000\t40.000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.000\t40.000\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp) AS lv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal256OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10m), " +
                    "('2024-01-01T00:01:00', 'a', 20m), " +
                    "('2024-01-01T00:02:00', 'b', 30m), " +
                    "('2024-01-01T00:03:00', 'b', 40m)");
            assertSql("ts\tgrp\tv\tlv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10\t20\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20\t20\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30\t40\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40\t40\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp) AS lv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal128OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000000m), " +
                    "('2024-01-01T00:01:00', 'a', 20.000000m), " +
                    "('2024-01-01T00:02:00', 'b', 30.000000m), " +
                    "('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tlv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000000\t20.000000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.000000\t20.000000\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.000000\t40.000000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.000000\t40.000000\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp) AS lv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal16OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.0m), " +
                    "('2024-01-01T00:01:00', 'a', 20.0m), " +
                    "('2024-01-01T00:02:00', 'b', 30.0m), " +
                    "('2024-01-01T00:03:00', 'b', 40.0m)");
            assertSql("ts\tgrp\tv\tlv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.0\t20.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.0\t20.0\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.0\t40.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.0\t40.0\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp) AS lv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal8OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 1.0m), " +
                    "('2024-01-01T00:01:00', 'a', 2.0m), " +
                    "('2024-01-01T00:02:00', 'b', 3.0m), " +
                    "('2024-01-01T00:03:00', 'b', 4.0m)");
            assertSql("ts\tgrp\tv\tlv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t1.0\t2.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t2.0\t2.0\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t3.0\t4.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t4.0\t4.0\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp) AS lv FROM t");
        });
    }

    @Test
    public void testLeadDecimal16Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.0m), " +
                    "('2024-01-01T00:01:00', 20.0m), " +
                    "('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tlead_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.0\t20.0\n" +
                            "2024-01-01T00:01:00.000000Z\t20.0\t30.0\n" +
                            "2024-01-01T00:02:00.000000Z\t30.0\t\n",
                    "SELECT ts, v, lead(v) OVER () AS lead_v FROM t");
        });
    }

    @Test
    public void testLeadDecimal32Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.000m), " +
                    "('2024-01-01T00:01:00', 20.000m), " +
                    "('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tlead_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.000\t20.000\n" +
                            "2024-01-01T00:01:00.000000Z\t20.000\t30.000\n" +
                            "2024-01-01T00:02:00.000000Z\t30.000\t\n",
                    "SELECT ts, v, lead(v) OVER () AS lead_v FROM t");
        });
    }

    @Test
    public void testLeadDecimal8Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1.0m), " +
                    "('2024-01-01T00:01:00', 2.0m), " +
                    "('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tlead_v\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\t2.0\n" +
                            "2024-01-01T00:01:00.000000Z\t2.0\t3.0\n" +
                            "2024-01-01T00:02:00.000000Z\t3.0\t\n",
                    "SELECT ts, v, lead(v) OVER () AS lead_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.00m), " +
                    "('2024-01-01T00:01:00', 'a', 30.00m), " +
                    "('2024-01-01T00:02:00', 'a', 20.00m), " +
                    "('2024-01-01T00:03:00', 'b', 40.00m), " +
                    "('2024-01-01T00:04:00', 'b', 50.00m)");
            assertSql("ts\tgrp\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.00\t30.00\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30.00\t30.00\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t20.00\t30.00\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.00\t50.00\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50.00\t50.00\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 30.00m), " +
                    "('2024-01-01T00:02:00', 20.00m), " +
                    "('2024-01-01T00:03:00', 40.00m)");
            assertSql("ts\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t10.00\n" +
                            "2024-01-01T00:01:00.000000Z\t30.00\t30.00\n" +
                            "2024-01-01T00:02:00.000000Z\t20.00\t30.00\n" +
                            "2024-01-01T00:03:00.000000Z\t40.00\t40.00\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal16OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.0m), " +
                    "('2024-01-01T00:01:00', 'a', 30.0m), " +
                    "('2024-01-01T00:02:00', 'a', 20.0m), " +
                    "('2024-01-01T00:03:00', 'b', 40.0m), " +
                    "('2024-01-01T00:04:00', 'b', 50.0m)");
            assertSql("ts\tgrp\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.0\t30.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30.0\t30.0\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t20.0\t30.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.0\t50.0\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50.0\t50.0\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal32OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000m), " +
                    "('2024-01-01T00:01:00', 'a', 30.000m), " +
                    "('2024-01-01T00:02:00', 'a', 20.000m), " +
                    "('2024-01-01T00:03:00', 'b', 40.000m), " +
                    "('2024-01-01T00:04:00', 'b', 50.000m)");
            assertSql("ts\tgrp\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000\t30.000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30.000\t30.000\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t20.000\t30.000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.000\t50.000\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50.000\t50.000\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal128SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.000000m), " +
                    "('2024-01-01T00:01:00', 30.000000m), " +
                    "('2024-01-01T00:02:00', 20.000000m), " +
                    "('2024-01-01T00:03:00', 40.000000m)");
            assertSql("ts\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.000000\t10.000000\n" +
                            "2024-01-01T00:01:00.000000Z\t30.000000\t30.000000\n" +
                            "2024-01-01T00:02:00.000000Z\t20.000000\t30.000000\n" +
                            "2024-01-01T00:03:00.000000Z\t40.000000\t40.000000\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal128OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000000m), " +
                    "('2024-01-01T00:01:00', 'a', 30.000000m), " +
                    "('2024-01-01T00:02:00', 'a', 20.000000m), " +
                    "('2024-01-01T00:03:00', 'b', 40.000000m), " +
                    "('2024-01-01T00:04:00', 'b', 50.000000m)");
            assertSql("ts\tgrp\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000000\t30.000000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30.000000\t30.000000\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t20.000000\t30.000000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.000000\t50.000000\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50.000000\t50.000000\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal256SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10m), " +
                    "('2024-01-01T00:01:00', 30m), " +
                    "('2024-01-01T00:02:00', 20m), " +
                    "('2024-01-01T00:03:00', 40m)");
            assertSql("ts\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10\t10\n" +
                            "2024-01-01T00:01:00.000000Z\t30\t30\n" +
                            "2024-01-01T00:02:00.000000Z\t20\t30\n" +
                            "2024-01-01T00:03:00.000000Z\t40\t40\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal256OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10m), " +
                    "('2024-01-01T00:01:00', 'a', 30m), " +
                    "('2024-01-01T00:02:00', 'a', 20m), " +
                    "('2024-01-01T00:03:00', 'b', 40m), " +
                    "('2024-01-01T00:04:00', 'b', 50m)");
            assertSql("ts\tgrp\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10\t30\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30\t30\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t20\t30\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40\t50\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50\t50\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal8OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 1.0m), " +
                    "('2024-01-01T00:01:00', 'a', 3.0m), " +
                    "('2024-01-01T00:02:00', 'a', 2.0m), " +
                    "('2024-01-01T00:03:00', 'b', 4.0m), " +
                    "('2024-01-01T00:04:00', 'b', 5.0m)");
            assertSql("ts\tgrp\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t1.0\t3.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t3.0\t3.0\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t2.0\t3.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t4.0\t5.0\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t5.0\t5.0\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal8SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1.0m), " +
                    "('2024-01-01T00:01:00', 3.0m), " +
                    "('2024-01-01T00:02:00', 2.0m), " +
                    "('2024-01-01T00:03:00', 4.0m)");
            assertSql("ts\tv\tmax_v\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\t1.0\n" +
                            "2024-01-01T00:01:00.000000Z\t3.0\t3.0\n" +
                            "2024-01-01T00:02:00.000000Z\t2.0\t3.0\n" +
                            "2024-01-01T00:03:00.000000Z\t4.0\t4.0\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal64OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.00m), " +
                    "('2024-01-01T00:01:00', 'a', 30.00m), " +
                    "('2024-01-01T00:02:00', 'a', 20.00m), " +
                    "('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tmin_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30.00\t10.00\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t20.00\t10.00\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.00\t40.00\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp) AS min_v FROM t");
        });
    }

    @Test
    public void testMinDecimal8OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 1.0m), " +
                    "('2024-01-01T00:01:00', 'a', 3.0m), " +
                    "('2024-01-01T00:02:00', 'a', 2.0m), " +
                    "('2024-01-01T00:03:00', 'b', 4.0m)");
            assertSql("ts\tgrp\tv\tmin_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t1.0\t1.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t3.0\t1.0\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t2.0\t1.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t4.0\t4.0\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp) AS min_v FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.00m), " +
                    "('2024-01-01T00:01:00', 'a', 20.00m), " +
                    "('2024-01-01T00:02:00', 'a', 30.00m), " +
                    "('2024-01-01T00:03:00', 'b', 40.00m), " +
                    "('2024-01-01T00:04:00', 'b', 50.00m)");
            assertSql("ts\tgrp\tv\tnv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.00\t20.00\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.00\t20.00\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t30.00\t20.00\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.00\t50.00\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50.00\t50.00\n",
                    "SELECT ts, grp, v, nth_value(v, 2) OVER (PARTITION BY grp) AS nv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal32OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000m), " +
                    "('2024-01-01T00:01:00', 'a', 20.000m), " +
                    "('2024-01-01T00:02:00', 'a', 30.000m), " +
                    "('2024-01-01T00:03:00', 'b', 40.000m), " +
                    "('2024-01-01T00:04:00', 'b', 50.000m)");
            assertSql("ts\tgrp\tv\tnv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000\t20.000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.000\t20.000\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t30.000\t20.000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.000\t50.000\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50.000\t50.000\n",
                    "SELECT ts, grp, v, nth_value(v, 2) OVER (PARTITION BY grp) AS nv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal256OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10m), " +
                    "('2024-01-01T00:01:00', 'a', 20m), " +
                    "('2024-01-01T00:02:00', 'a', 30m), " +
                    "('2024-01-01T00:03:00', 'b', 40m), " +
                    "('2024-01-01T00:04:00', 'b', 50m)");
            assertSql("ts\tgrp\tv\tnv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10\t20\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20\t20\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t30\t20\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40\t50\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50\t50\n",
                    "SELECT ts, grp, v, nth_value(v, 2) OVER (PARTITION BY grp) AS nv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal128OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000000m), " +
                    "('2024-01-01T00:01:00', 'a', 20.000000m), " +
                    "('2024-01-01T00:02:00', 'a', 30.000000m), " +
                    "('2024-01-01T00:03:00', 'b', 40.000000m), " +
                    "('2024-01-01T00:04:00', 'b', 50.000000m)");
            assertSql("ts\tgrp\tv\tnv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000000\t20.000000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.000000\t20.000000\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t30.000000\t20.000000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.000000\t50.000000\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50.000000\t50.000000\n",
                    "SELECT ts, grp, v, nth_value(v, 2) OVER (PARTITION BY grp) AS nv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal16OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.0m), " +
                    "('2024-01-01T00:01:00', 'a', 20.0m), " +
                    "('2024-01-01T00:02:00', 'a', 30.0m), " +
                    "('2024-01-01T00:03:00', 'b', 40.0m), " +
                    "('2024-01-01T00:04:00', 'b', 50.0m)");
            assertSql("ts\tgrp\tv\tnv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.0\t20.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.0\t20.0\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t30.0\t20.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.0\t50.0\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t50.0\t50.0\n",
                    "SELECT ts, grp, v, nth_value(v, 2) OVER (PARTITION BY grp) AS nv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal8OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 1.0m), " +
                    "('2024-01-01T00:01:00', 'a', 2.0m), " +
                    "('2024-01-01T00:02:00', 'a', 3.0m), " +
                    "('2024-01-01T00:03:00', 'b', 4.0m), " +
                    "('2024-01-01T00:04:00', 'b', 5.0m)");
            assertSql("ts\tgrp\tv\tnv\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t1.0\t2.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t2.0\t2.0\n" +
                            "2024-01-01T00:02:00.000000Z\ta\t3.0\t2.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t4.0\t5.0\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t5.0\t5.0\n",
                    "SELECT ts, grp, v, nth_value(v, 2) OVER (PARTITION BY grp) AS nv FROM t");
        });
    }

    @Test
    public void testAvgDecimal32OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000m), " +
                    "('2024-01-01T00:01:00', 'a', 30.000m), " +
                    "('2024-01-01T00:02:00', 'b', 50.000m), " +
                    "('2024-01-01T00:03:00', 'b', 70.000m)");
            assertSql("ts\tgrp\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000\t20.000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30.000\t20.000\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t50.000\t60.000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t70.000\t60.000\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal256OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10m), " +
                    "('2024-01-01T00:01:00', 'a', 30m), " +
                    "('2024-01-01T00:02:00', 'b', 50m), " +
                    "('2024-01-01T00:03:00', 'b', 70m)");
            assertSql("ts\tgrp\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10\t20\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30\t20\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t50\t60\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t70\t60\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal128OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000000m), " +
                    "('2024-01-01T00:01:00', 'a', 30.000000m), " +
                    "('2024-01-01T00:02:00', 'b', 50.000000m), " +
                    "('2024-01-01T00:03:00', 'b', 70.000000m)");
            assertSql("ts\tgrp\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000000\t20.000000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30.000000\t20.000000\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t50.000000\t60.000000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t70.000000\t60.000000\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal16OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.0m), " +
                    "('2024-01-01T00:01:00', 'a', 30.0m), " +
                    "('2024-01-01T00:02:00', 'b', 50.0m), " +
                    "('2024-01-01T00:03:00', 'b', 70.0m)");
            assertSql("ts\tgrp\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.0\t20.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t30.0\t20.0\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t50.0\t60.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t70.0\t60.0\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal8OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 1.0m), " +
                    "('2024-01-01T00:01:00', 'a', 3.0m), " +
                    "('2024-01-01T00:02:00', 'b', 5.0m), " +
                    "('2024-01-01T00:03:00', 'b', 7.0m)");
            assertSql("ts\tgrp\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t1.0\t2.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t3.0\t2.0\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t5.0\t6.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t7.0\t6.0\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal16Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.0m), " +
                    "('2024-01-01T00:01:00', 20.0m), " +
                    "('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.0\t20.0000\n" +
                            "2024-01-01T00:01:00.000000Z\t20.0\t20.0000\n" +
                            "2024-01-01T00:02:00.000000Z\t30.0\t20.0000\n",
                    "SELECT ts, v, avg(v, 4) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal8Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1.0m), " +
                    "('2024-01-01T00:01:00', 2.0m), " +
                    "('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tavg_v\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\t2.0000\n" +
                            "2024-01-01T00:01:00.000000Z\t2.0\t2.0000\n" +
                            "2024-01-01T00:02:00.000000Z\t3.0\t2.0000\n",
                    "SELECT ts, v, avg(v, 4) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testSumDecimal64OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.00m), " +
                    "('2024-01-01T00:01:00', 'a', 20.00m), " +
                    "('2024-01-01T00:02:00', 'b', 30.00m), " +
                    "('2024-01-01T00:03:00', 'a', 30.00m), " +
                    "('2024-01-01T00:04:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tsum_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.00\t60.00\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.00\t60.00\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.00\t70.00\n" +
                            "2024-01-01T00:03:00.000000Z\ta\t30.00\t60.00\n" +
                            "2024-01-01T00:04:00.000000Z\tb\t40.00\t70.00\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal64SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', 20.00m), " +
                    "('2024-01-01T00:02:00', 30.00m), " +
                    "('2024-01-01T00:03:00', 40.00m)");
            assertSql("ts\tv\tsum_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t10.00\n" +
                            "2024-01-01T00:01:00.000000Z\t20.00\t30.00\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t50.00\n" +
                            "2024-01-01T00:03:00.000000Z\t40.00\t70.00\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal32OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000m), " +
                    "('2024-01-01T00:01:00', 'a', 20.000m), " +
                    "('2024-01-01T00:02:00', 'b', 30.000m), " +
                    "('2024-01-01T00:03:00', 'b', 40.000m)");
            assertSql("ts\tgrp\tv\tsum_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000\t30.000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.000\t30.000\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.000\t70.000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.000\t70.000\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal256OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10m), " +
                    "('2024-01-01T00:01:00', 'a', 20m), " +
                    "('2024-01-01T00:02:00', 'b', 30m), " +
                    "('2024-01-01T00:03:00', 'b', 40m)");
            assertSql("ts\tgrp\tv\tsum_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10\t30\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20\t30\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30\t70\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40\t70\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal128OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.000000m), " +
                    "('2024-01-01T00:01:00', 'a', 20.000000m), " +
                    "('2024-01-01T00:02:00', 'b', 30.000000m), " +
                    "('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tsum_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.000000\t30.000000\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.000000\t30.000000\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.000000\t70.000000\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.000000\t70.000000\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal16OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 10.0m), " +
                    "('2024-01-01T00:01:00', 'a', 20.0m), " +
                    "('2024-01-01T00:02:00', 'b', 30.0m), " +
                    "('2024-01-01T00:03:00', 'b', 40.0m)");
            assertSql("ts\tgrp\tv\tsum_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t10.0\t30.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t20.0\t30.0\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t30.0\t70.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t40.0\t70.0\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal8OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', 1.0m), " +
                    "('2024-01-01T00:01:00', 'a', 2.0m), " +
                    "('2024-01-01T00:02:00', 'b', 3.0m), " +
                    "('2024-01-01T00:03:00', 'b', 4.0m)");
            assertSql("ts\tgrp\tv\tsum_v\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t1.0\t3.0\n" +
                            "2024-01-01T00:01:00.000000Z\ta\t2.0\t3.0\n" +
                            "2024-01-01T00:02:00.000000Z\tb\t3.0\t7.0\n" +
                            "2024-01-01T00:03:00.000000Z\tb\t4.0\t7.0\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 10.00m), " +
                    "('2024-01-01T00:01:00', null), " +
                    "('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tsum_v\n" +
                            "2024-01-01T00:00:00.000000Z\t10.00\t40.00\n" +
                            "2024-01-01T00:01:00.000000Z\t\t40.00\n" +
                            "2024-01-01T00:02:00.000000Z\t30.00\t40.00\n",
                    "SELECT ts, v, sum(v) OVER () AS sum_v FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal16OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.0m), ('2024-01-01T00:01:00', 'a', 20.0m), ('2024-01-01T00:02:00', 'b', 30.0m)");
            assertSql("ts\tgrp\tv\tfv\n2024-01-01T00:00:00.000000Z\ta\t10.0\t10.0\n2024-01-01T00:01:00.000000Z\ta\t20.0\t10.0\n2024-01-01T00:02:00.000000Z\tb\t30.0\t30.0\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal32OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000m), ('2024-01-01T00:01:00', 'a', 20.000m), ('2024-01-01T00:02:00', 'b', 30.000m)");
            assertSql("ts\tgrp\tv\tfv\n2024-01-01T00:00:00.000000Z\ta\t10.000\t10.000\n2024-01-01T00:01:00.000000Z\ta\t20.000\t10.000\n2024-01-01T00:02:00.000000Z\tb\t30.000\t30.000\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal128OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'a', 20.000000m), ('2024-01-01T00:02:00', 'b', 30.000000m)");
            assertSql("ts\tgrp\tv\tfv\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\ta\t20.000000\t10.000000\n2024-01-01T00:02:00.000000Z\tb\t30.000000\t30.000000\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal256OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10m), ('2024-01-01T00:01:00', 'a', 20m), ('2024-01-01T00:02:00', 'b', 30m)");
            assertSql("ts\tgrp\tv\tfv\n2024-01-01T00:00:00.000000Z\ta\t10\t10\n2024-01-01T00:01:00.000000Z\ta\t20\t10\n2024-01-01T00:02:00.000000Z\tb\t30\t30\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp) AS fv FROM t");
        });
    }

    @Test
    public void testMinDecimal16OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30.0m), ('2024-01-01T00:01:00', 'a', 10.0m), ('2024-01-01T00:02:00', 'b', 40.0m)");
            assertSql("ts\tgrp\tv\tmin_v\n2024-01-01T00:00:00.000000Z\ta\t30.0\t10.0\n2024-01-01T00:01:00.000000Z\ta\t10.0\t10.0\n2024-01-01T00:02:00.000000Z\tb\t40.0\t40.0\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp) AS min_v FROM t");
        });
    }

    @Test
    public void testMinDecimal32OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30.000m), ('2024-01-01T00:01:00', 'a', 10.000m), ('2024-01-01T00:02:00', 'b', 40.000m)");
            assertSql("ts\tgrp\tv\tmin_v\n2024-01-01T00:00:00.000000Z\ta\t30.000\t10.000\n2024-01-01T00:01:00.000000Z\ta\t10.000\t10.000\n2024-01-01T00:02:00.000000Z\tb\t40.000\t40.000\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp) AS min_v FROM t");
        });
    }

    @Test
    public void testMinDecimal128OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30.000000m), ('2024-01-01T00:01:00', 'a', 10.000000m), ('2024-01-01T00:02:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tmin_v\n2024-01-01T00:00:00.000000Z\ta\t30.000000\t10.000000\n2024-01-01T00:01:00.000000Z\ta\t10.000000\t10.000000\n2024-01-01T00:02:00.000000Z\tb\t40.000000\t40.000000\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp) AS min_v FROM t");
        });
    }

    @Test
    public void testMinDecimal256OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30m), ('2024-01-01T00:01:00', 'a', 10m), ('2024-01-01T00:02:00', 'b', 40m)");
            assertSql("ts\tgrp\tv\tmin_v\n2024-01-01T00:00:00.000000Z\ta\t30\t10\n2024-01-01T00:01:00.000000Z\ta\t10\t10\n2024-01-01T00:02:00.000000Z\tb\t40\t40\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp) AS min_v FROM t");
        });
    }

    @Test
    public void testLeadDecimal128Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tlead_v\n2024-01-01T00:00:00.000000Z\t10.000000\t20.000000\n2024-01-01T00:01:00.000000Z\t20.000000\t30.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t\n",
                    "SELECT ts, v, lead(v) OVER () AS lead_v FROM t");
        });
    }

    @Test
    public void testLeadDecimal256Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tlead_v\n2024-01-01T00:00:00.000000Z\t10\t20\n2024-01-01T00:01:00.000000Z\t20\t30\n2024-01-01T00:02:00.000000Z\t30\t\n",
                    "SELECT ts, v, lead(v) OVER () AS lead_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal32Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', 20.000m), ('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.000\t20.0000\n2024-01-01T00:01:00.000000Z\t20.000\t20.0000\n2024-01-01T00:02:00.000000Z\t30.000\t20.0000\n",
                    "SELECT ts, v, avg(v, 4) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal128Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.000000\t20.00000000\n2024-01-01T00:01:00.000000Z\t20.000000\t20.00000000\n2024-01-01T00:02:00.000000Z\t30.000000\t20.00000000\n",
                    "SELECT ts, v, avg(v, 8) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal256Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10\t20.00\n2024-01-01T00:01:00.000000Z\t20\t20.00\n2024-01-01T00:02:00.000000Z\t30\t20.00\n",
                    "SELECT ts, v, avg(v, 2) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20.00\t\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, first_value(v) OVER () AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.00\t30.00\n2024-01-01T00:01:00.000000Z\t\t30.00\n2024-01-01T00:02:00.000000Z\t30.00\t30.00\n",
                    "SELECT ts, v, last_value(v) OVER () AS lv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t\t\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, nth_value(v, 2) OVER () AS nv FROM t");
        });
    }

    @Test
    public void testLagDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlag_v\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t\t10.00\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, lag(v) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLeadDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlead_v\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t\t30.00\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, lead(v) OVER () AS lead_v FROM t");
        });
    }

    @Test
    public void testMinDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t\t10.00\n2024-01-01T00:02:00.000000Z\t30.00\t10.00\n",
                    "SELECT ts, v, min(v) OVER () AS min_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10.00\t30.00\n2024-01-01T00:01:00.000000Z\t\t30.00\n2024-01-01T00:02:00.000000Z\t30.00\t30.00\n",
                    "SELECT ts, v, max(v) OVER () AS max_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.00\t20.0000\n2024-01-01T00:01:00.000000Z\t\t20.0000\n2024-01-01T00:02:00.000000Z\t30.00\t20.0000\n",
                    "SELECT ts, v, avg(v, 4) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', null)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.00\t20.00\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\t\t20.00\n",
                    "SELECT ts, v, last_value(v) IGNORE NULLS OVER () AS lv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t\t30.00\n2024-01-01T00:01:00.000000Z\t20.00\t30.00\n2024-01-01T00:02:00.000000Z\t30.00\t30.00\n",
                    "SELECT ts, v, nth_value(v, 2) IGNORE NULLS OVER () AS nv FROM t");
        });
    }

    @Test
    public void testLagDecimal64IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlag_v\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t\t10.00\n2024-01-01T00:02:00.000000Z\t30.00\t10.00\n",
                    "SELECT ts, v, lag(v) IGNORE NULLS OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLeadDecimal64IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlead_v\n2024-01-01T00:00:00.000000Z\t10.00\t30.00\n2024-01-01T00:01:00.000000Z\t\t30.00\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, lead(v) IGNORE NULLS OVER () AS lead_v FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t10.00\n2024-01-01T00:02:00.000000Z\t30.00\t10.00\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\t30.00\t30.00\n",
                    "SELECT ts, v, last_value(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testMinDecimal64SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.00m), ('2024-01-01T00:01:00', 10.00m), ('2024-01-01T00:02:00', 20.00m), ('2024-01-01T00:03:00', 40.00m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30.00\t30.00\n2024-01-01T00:01:00.000000Z\t10.00\t10.00\n2024-01-01T00:02:00.000000Z\t20.00\t10.00\n2024-01-01T00:03:00.000000Z\t40.00\t20.00\n",
                    "SELECT ts, v, min(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testCountDecimal64OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', null), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t10.00\t1\n2024-01-01T00:01:00.000000Z\ta\t\t1\n2024-01-01T00:02:00.000000Z\tb\t30.00\t2\n2024-01-01T00:03:00.000000Z\tb\t40.00\t2\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp) AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal64SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t10.00\t1\n2024-01-01T00:01:00.000000Z\t\t1\n2024-01-01T00:02:00.000000Z\t30.00\t1\n",
                    "SELECT ts, v, count(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS c FROM t");
        });
    }

    @Test
    public void testSumDecimal64OverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.00\t60.00\n2024-01-01T00:01:00.000000Z\t20.00\t60.00\n2024-01-01T00:02:00.000000Z\t30.00\t60.00\n",
                    "SELECT ts, v, sum(v) OVER () AS sum_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64OverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 30.00m), ('2024-01-01T00:02:00', 20.00m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10.00\t30.00\n2024-01-01T00:01:00.000000Z\t30.00\t30.00\n2024-01-01T00:02:00.000000Z\t20.00\t30.00\n",
                    "SELECT ts, v, max(v) OVER () AS max_v FROM t");
        });
    }

    @Test
    public void testSumDecimal64SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 42.00m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t42.00\t42.00\n",
                    "SELECT ts, v, sum(v) OVER () AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 42.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t42.00\t42.00\n",
                    "SELECT ts, v, avg(v) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testCountDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t\t0\n2024-01-01T00:01:00.000000Z\t\t0\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testSumDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t\t\n",
                    "SELECT ts, v, sum(v) OVER () AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t\t\n",
                    "SELECT ts, v, avg(v) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t\t\n",
                    "SELECT ts, v, max(v) OVER () AS max_v FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64OverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n",
                    "SELECT ts, v, last_value(v) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64OverCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n",
                    "SELECT ts, v, nth_value(v, 1) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, nth_value(v, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testMaxDecimal64OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 30.00m), ('2024-01-01T00:02:00', 20.00m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t30.00\t30.00\n2024-01-01T00:02:00.000000Z\t20.00\t30.00\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal64OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.00m), ('2024-01-01T00:01:00', 10.00m), ('2024-01-01T00:02:00', 20.00m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30.00\t30.00\n2024-01-01T00:01:00.000000Z\t10.00\t10.00\n2024-01-01T00:02:00.000000Z\t20.00\t10.00\n",
                    "SELECT ts, v, min(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testSumDecimal64OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t30.00\n2024-01-01T00:02:00.000000Z\t30.00\t60.00\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal64OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 30.00m), ('2024-01-01T00:02:00', 'b', 50.00m), ('2024-01-01T00:03:00', 'b', 70.00m)");
            assertSql("ts\tgrp\tv\tavg_v\n2024-01-01T00:00:00.000000Z\ta\t10.00\t20.0000\n2024-01-01T00:01:00.000000Z\ta\t30.00\t20.0000\n2024-01-01T00:02:00.000000Z\tb\t50.00\t60.0000\n2024-01-01T00:03:00.000000Z\tb\t70.00\t60.0000\n",
                    "SELECT ts, grp, v, avg(v, 4) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal64SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.00\t10.0000\n2024-01-01T00:01:00.000000Z\t20.00\t15.0000\n2024-01-01T00:02:00.000000Z\t30.00\t25.0000\n",
                    "SELECT ts, v, avg(v, 4) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal64OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.00\t10.0000\n2024-01-01T00:01:00.000000Z\t20.00\t15.0000\n2024-01-01T00:02:00.000000Z\t30.00\t20.0000\n",
                    "SELECT ts, v, avg(v, 4) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testLastValueDecimal16WithIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', 20.0m), ('2024-01-01T00:02:00', null)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.0\t20.0\n2024-01-01T00:01:00.000000Z\t20.0\t20.0\n2024-01-01T00:02:00.000000Z\t\t20.0\n",
                    "SELECT ts, v, last_value(v) IGNORE NULLS OVER () AS lv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal32WithIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', 20.000m), ('2024-01-01T00:02:00', null)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.000\t20.000\n2024-01-01T00:01:00.000000Z\t20.000\t20.000\n2024-01-01T00:02:00.000000Z\t\t20.000\n",
                    "SELECT ts, v, last_value(v) IGNORE NULLS OVER () AS lv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal128WithIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', null)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.000000\t20.000000\n2024-01-01T00:01:00.000000Z\t20.000000\t20.000000\n2024-01-01T00:02:00.000000Z\t\t20.000000\n",
                    "SELECT ts, v, last_value(v) IGNORE NULLS OVER () AS lv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal256WithIgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', null)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10\t20\n2024-01-01T00:01:00.000000Z\t20\t20\n2024-01-01T00:02:00.000000Z\t\t20\n",
                    "SELECT ts, v, last_value(v) IGNORE NULLS OVER () AS lv FROM t");
        });
    }

    @Test
    public void testLagDecimal128WithDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m)");
            assertSql("ts\tv\tlag_v\n2024-01-01T00:00:00.000000Z\t10.000000\t99.999999\n2024-01-01T00:01:00.000000Z\t20.000000\t10.000000\n",
                    "SELECT ts, v, lag(v, 1, 99.999999::decimal(38, 6)) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64WithLargeN() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t20.00\t\n",
                    "SELECT ts, v, nth_value(v, 99) OVER () AS nv FROM t");
        });
    }

    @Test
    public void testCountDecimal64NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t10.00\t3\n2024-01-01T00:01:00.000000Z\t20.00\t3\n2024-01-01T00:02:00.000000Z\t30.00\t3\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testSumDecimal64MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.00m), ('2024-01-01T00:01:00', 'b', 2.00m), ('2024-01-01T00:02:00', 'c', 3.00m), ('2024-01-01T00:03:00', 'a', 4.00m), ('2024-01-01T00:04:00', 'b', 5.00m), ('2024-01-01T00:05:00', 'c', 6.00m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t1.00\t5.00\n2024-01-01T00:01:00.000000Z\tb\t2.00\t7.00\n2024-01-01T00:02:00.000000Z\tc\t3.00\t9.00\n2024-01-01T00:03:00.000000Z\ta\t4.00\t5.00\n2024-01-01T00:04:00.000000Z\tb\t5.00\t7.00\n2024-01-01T00:05:00.000000Z\tc\t6.00\t9.00\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.00m), ('2024-01-01T00:01:00', 'b', 2.00m), ('2024-01-01T00:02:00', 'c', 9.00m), ('2024-01-01T00:03:00', 'a', 4.00m), ('2024-01-01T00:04:00', 'b', 5.00m), ('2024-01-01T00:05:00', 'c', 1.00m)");
            assertSql("ts\tgrp\tv\tmax_v\n2024-01-01T00:00:00.000000Z\ta\t1.00\t4.00\n2024-01-01T00:01:00.000000Z\tb\t2.00\t5.00\n2024-01-01T00:02:00.000000Z\tc\t9.00\t9.00\n2024-01-01T00:03:00.000000Z\ta\t4.00\t4.00\n2024-01-01T00:04:00.000000Z\tb\t5.00\t5.00\n2024-01-01T00:05:00.000000Z\tc\t1.00\t9.00\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp) AS max_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'b', 20.00m), ('2024-01-01T00:02:00', 'a', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tavg_v\n2024-01-01T00:00:00.000000Z\ta\t10.00\t20.00\n2024-01-01T00:01:00.000000Z\tb\t20.00\t30.00\n2024-01-01T00:02:00.000000Z\ta\t30.00\t20.00\n2024-01-01T00:03:00.000000Z\tb\t40.00\t30.00\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testSumDecimal64LargeWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60000000), cast(1.00 as decimal(18, 2)) FROM long_sequence(1000)");
            assertSql("sum_v\n1000.00\n",
                    "SELECT sum(v) OVER () AS sum_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testCountDecimal64LargeWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60000000), cast(x as decimal(18, 2)) FROM long_sequence(500)");
            assertSql("c\n500\n",
                    "SELECT count(v) OVER () AS c FROM t LIMIT 1");
        });
    }

    @Test
    public void testMaxDecimal64LargeSlidingWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60000000), cast((x % 100) as decimal(18, 2)) FROM long_sequence(300)");
            assertSql("max_v\n99.00\n",
                    "SELECT max(v) OVER (ORDER BY ts ROWS BETWEEN 200 PRECEDING AND CURRENT ROW) AS max_v FROM t LIMIT 1 OFFSET 299");
        });
    }

    @Test
    public void testFirstValueDecimal64MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.00m), ('2024-01-01T00:01:00', 'b', 2.00m), ('2024-01-01T00:02:00', 'a', 3.00m), ('2024-01-01T00:03:00', 'b', 4.00m)");
            assertSql("ts\tgrp\tv\tfv\n2024-01-01T00:00:00.000000Z\ta\t1.00\t1.00\n2024-01-01T00:01:00.000000Z\tb\t2.00\t2.00\n2024-01-01T00:02:00.000000Z\ta\t3.00\t1.00\n2024-01-01T00:03:00.000000Z\tb\t4.00\t2.00\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.00m), ('2024-01-01T00:01:00', 'b', 2.00m), ('2024-01-01T00:02:00', 'a', 3.00m), ('2024-01-01T00:03:00', 'b', 4.00m)");
            assertSql("ts\tgrp\tv\tlv\n2024-01-01T00:00:00.000000Z\ta\t1.00\t3.00\n2024-01-01T00:01:00.000000Z\tb\t2.00\t4.00\n2024-01-01T00:02:00.000000Z\ta\t3.00\t3.00\n2024-01-01T00:03:00.000000Z\tb\t4.00\t4.00\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp) AS lv FROM t");
        });
    }

    @Test
    public void testLagDecimal64MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.00m), ('2024-01-01T00:01:00', 'b', 2.00m), ('2024-01-01T00:02:00', 'a', 3.00m), ('2024-01-01T00:03:00', 'b', 4.00m)");
            assertSql("ts\tgrp\tv\tlag_v\n2024-01-01T00:00:00.000000Z\ta\t1.00\t\n2024-01-01T00:01:00.000000Z\tb\t2.00\t\n2024-01-01T00:02:00.000000Z\ta\t3.00\t1.00\n2024-01-01T00:03:00.000000Z\tb\t4.00\t2.00\n",
                    "SELECT ts, grp, v, lag(v) OVER (PARTITION BY grp) AS lag_v FROM t");
        });
    }

    @Test
    public void testLeadDecimal64MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.00m), ('2024-01-01T00:01:00', 'b', 2.00m), ('2024-01-01T00:02:00', 'a', 3.00m), ('2024-01-01T00:03:00', 'b', 4.00m)");
            assertSql("ts\tgrp\tv\tlead_v\n2024-01-01T00:00:00.000000Z\ta\t1.00\t3.00\n2024-01-01T00:01:00.000000Z\tb\t2.00\t4.00\n2024-01-01T00:02:00.000000Z\ta\t3.00\t\n2024-01-01T00:03:00.000000Z\tb\t4.00\t\n",
                    "SELECT ts, grp, v, lead(v) OVER (PARTITION BY grp) AS lead_v FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64EmptyFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t20.00\t\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64EmptyFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t20.00\t\n",
                    "SELECT ts, v, last_value(v) OVER (ORDER BY ts ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) AS lv FROM t");
        });
    }

    @Test
    public void testCountDecimal8MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.0m), ('2024-01-01T00:01:00', 'b', null), ('2024-01-01T00:02:00', 'a', 2.0m), ('2024-01-01T00:03:00', 'b', 3.0m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t1.0\t2\n2024-01-01T00:01:00.000000Z\tb\t\t1\n2024-01-01T00:02:00.000000Z\ta\t2.0\t2\n2024-01-01T00:03:00.000000Z\tb\t3.0\t1\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp) AS c FROM t");
        });
    }

    @Test
    public void testAvgDecimal64OverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.00\t20.00\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, avg(v) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testCountDecimal64OverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t10.00\t3\n2024-01-01T00:01:00.000000Z\t20.00\t3\n2024-01-01T00:02:00.000000Z\t30.00\t3\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testMinDecimal64OverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.00m), ('2024-01-01T00:01:00', 10.00m), ('2024-01-01T00:02:00', 20.00m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30.00\t10.00\n2024-01-01T00:01:00.000000Z\t10.00\t10.00\n2024-01-01T00:02:00.000000Z\t20.00\t10.00\n",
                    "SELECT ts, v, min(v) OVER () AS min_v FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 42.00m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t42.00\t42.00\n",
                    "SELECT ts, v, first_value(v) OVER () AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 42.00m)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t42.00\t42.00\n",
                    "SELECT ts, v, last_value(v) OVER () AS lv FROM t");
        });
    }

    @Test
    public void testLagDecimal64SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 42.00m)");
            assertSql("ts\tv\tlag_v\n2024-01-01T00:00:00.000000Z\t42.00\t\n",
                    "SELECT ts, v, lag(v) OVER () AS lag_v FROM t");
        });
    }

    @Test
    public void testLeadDecimal64SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 42.00m)");
            assertSql("ts\tv\tlead_v\n2024-01-01T00:00:00.000000Z\t42.00\t\n",
                    "SELECT ts, v, lead(v) OVER () AS lead_v FROM t");
        });
    }

    @Test
    public void testCountDecimal64SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 42.00m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t42.00\t1\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testMaxDecimal64SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 42.00m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t42.00\t42.00\n",
                    "SELECT ts, v, max(v) OVER () AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal64SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 42.00m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t42.00\t42.00\n",
                    "SELECT ts, v, min(v) OVER () AS min_v FROM t");
        });
    }

    @Test
    public void testSumDecimal8SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1.0m), ('2024-01-01T00:01:00', 2.0m), ('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t1.0\t1.0\n2024-01-01T00:01:00.000000Z\t2.0\t3.0\n2024-01-01T00:02:00.000000Z\t3.0\t5.0\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal16SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', 20.0m), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.0\t10.0\n2024-01-01T00:01:00.000000Z\t20.0\t30.0\n2024-01-01T00:02:00.000000Z\t30.0\t50.0\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal32SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', 20.000m), ('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.000\t10.000\n2024-01-01T00:01:00.000000Z\t20.000\t30.000\n2024-01-01T00:02:00.000000Z\t30.000\t50.000\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal128SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\t20.000000\t30.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t50.000000\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal256SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10\t10\n2024-01-01T00:01:00.000000Z\t20\t30\n2024-01-01T00:02:00.000000Z\t30\t50\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal8SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1.0m), ('2024-01-01T00:01:00', 3.0m), ('2024-01-01T00:02:00', 5.0m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t1.0\t1.0\n2024-01-01T00:01:00.000000Z\t3.0\t2.0\n2024-01-01T00:02:00.000000Z\t5.0\t4.0\n",
                    "SELECT ts, v, avg(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal16SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', 30.0m), ('2024-01-01T00:02:00', 50.0m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.0\t10.0\n2024-01-01T00:01:00.000000Z\t30.0\t20.0\n2024-01-01T00:02:00.000000Z\t50.0\t40.0\n",
                    "SELECT ts, v, avg(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal32SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', 30.000m), ('2024-01-01T00:02:00', 50.000m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.000\t10.000\n2024-01-01T00:01:00.000000Z\t30.000\t20.000\n2024-01-01T00:02:00.000000Z\t50.000\t40.000\n",
                    "SELECT ts, v, avg(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal128SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 30.000000m), ('2024-01-01T00:02:00', 50.000000m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\t30.000000\t20.000000\n2024-01-01T00:02:00.000000Z\t50.000000\t40.000000\n",
                    "SELECT ts, v, avg(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal256SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 30m), ('2024-01-01T00:02:00', 50m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10\t10\n2024-01-01T00:01:00.000000Z\t30\t20\n2024-01-01T00:02:00.000000Z\t50\t40\n",
                    "SELECT ts, v, avg(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testMinDecimal16SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.0m), ('2024-01-01T00:01:00', 10.0m), ('2024-01-01T00:02:00', 20.0m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30.0\t30.0\n2024-01-01T00:01:00.000000Z\t10.0\t10.0\n2024-01-01T00:02:00.000000Z\t20.0\t10.0\n",
                    "SELECT ts, v, min(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testMinDecimal32SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.000m), ('2024-01-01T00:01:00', 10.000m), ('2024-01-01T00:02:00', 20.000m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30.000\t30.000\n2024-01-01T00:01:00.000000Z\t10.000\t10.000\n2024-01-01T00:02:00.000000Z\t20.000\t10.000\n",
                    "SELECT ts, v, min(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testMinDecimal128SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.000000m), ('2024-01-01T00:01:00', 10.000000m), ('2024-01-01T00:02:00', 20.000000m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30.000000\t30.000000\n2024-01-01T00:01:00.000000Z\t10.000000\t10.000000\n2024-01-01T00:02:00.000000Z\t20.000000\t10.000000\n",
                    "SELECT ts, v, min(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testMinDecimal256SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30m), ('2024-01-01T00:01:00', 10m), ('2024-01-01T00:02:00', 20m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30\t30\n2024-01-01T00:01:00.000000Z\t10\t10\n2024-01-01T00:02:00.000000Z\t20\t10\n",
                    "SELECT ts, v, min(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal16SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', 30.0m), ('2024-01-01T00:02:00', 20.0m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10.0\t10.0\n2024-01-01T00:01:00.000000Z\t30.0\t30.0\n2024-01-01T00:02:00.000000Z\t20.0\t30.0\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal32SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', 30.000m), ('2024-01-01T00:02:00', 20.000m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10.000\t10.000\n2024-01-01T00:01:00.000000Z\t30.000\t30.000\n2024-01-01T00:02:00.000000Z\t20.000\t30.000\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testNthValueDecimal16OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', 20.0m), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t10.0\t\n2024-01-01T00:01:00.000000Z\t20.0\t20.0\n2024-01-01T00:02:00.000000Z\t30.0\t20.0\n",
                    "SELECT ts, v, nth_value(v, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal8OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1.0m), ('2024-01-01T00:01:00', 2.0m), ('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t1.0\t1.0\n2024-01-01T00:01:00.000000Z\t2.0\t1.0\n2024-01-01T00:02:00.000000Z\t3.0\t1.0\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal16OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', 20.0m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t10.0\t10.0\n2024-01-01T00:01:00.000000Z\t20.0\t10.0\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal128OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\t20.000000\t10.000000\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal256OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t10\t10\n2024-01-01T00:01:00.000000Z\t20\t10\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal128SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\t20.000000\t20.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t30.000000\n",
                    "SELECT ts, v, last_value(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal256SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10\t10\n2024-01-01T00:01:00.000000Z\t20\t20\n2024-01-01T00:02:00.000000Z\t30\t30\n",
                    "SELECT ts, v, last_value(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal128OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t10.000000\t\n2024-01-01T00:01:00.000000Z\t20.000000\t20.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t20.000000\n",
                    "SELECT ts, v, nth_value(v, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal256OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t10\t\n2024-01-01T00:01:00.000000Z\t20\t20\n2024-01-01T00:02:00.000000Z\t30\t20\n",
                    "SELECT ts, v, nth_value(v, 2) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testCountDecimal64MultiPartitionWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', null), ('2024-01-01T00:02:00', 'b', null), ('2024-01-01T00:03:00', 'b', null), ('2024-01-01T00:04:00', 'c', 50.00m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t10.00\t1\n2024-01-01T00:01:00.000000Z\ta\t\t1\n2024-01-01T00:02:00.000000Z\tb\t\t0\n2024-01-01T00:03:00.000000Z\tb\t\t0\n2024-01-01T00:04:00.000000Z\tc\t50.00\t1\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp) AS c FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64MultiPartitionWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', null), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', null), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tfv\n2024-01-01T00:00:00.000000Z\ta\t\t\n2024-01-01T00:01:00.000000Z\ta\t20.00\t\n2024-01-01T00:02:00.000000Z\tb\t\t\n2024-01-01T00:03:00.000000Z\tb\t40.00\t\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp) AS fv FROM t");
        });
    }

    @Test
    public void testAvgDecimal64LargeWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60000000), cast(2.00 as decimal(18, 2)) FROM long_sequence(500)");
            assertSql("avg_v\n2.00\n",
                    "SELECT avg(v) OVER () AS avg_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testMinDecimal64LargeSlidingWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60000000), cast((x % 100) as decimal(18, 2)) FROM long_sequence(300)");
            assertSql("min_v\n0.00\n",
                    "SELECT min(v) OVER (ORDER BY ts ROWS BETWEEN 200 PRECEDING AND CURRENT ROW) AS min_v FROM t LIMIT 1 OFFSET 299");
        });
    }

    @Test
    public void testCountDecimal128MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'b', null), ('2024-01-01T00:02:00', 'a', 20.000000m), ('2024-01-01T00:03:00', 'b', 30.000000m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t2\n2024-01-01T00:01:00.000000Z\tb\t\t1\n2024-01-01T00:02:00.000000Z\ta\t20.000000\t2\n2024-01-01T00:03:00.000000Z\tb\t30.000000\t1\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp) AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal256MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10m), ('2024-01-01T00:01:00', 'b', null), ('2024-01-01T00:02:00', 'a', 20m), ('2024-01-01T00:03:00', 'b', 30m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t10\t2\n2024-01-01T00:01:00.000000Z\tb\t\t1\n2024-01-01T00:02:00.000000Z\ta\t20\t2\n2024-01-01T00:03:00.000000Z\tb\t30\t1\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp) AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal16OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t10.0\t1\n2024-01-01T00:01:00.000000Z\t\t1\n2024-01-01T00:02:00.000000Z\t30.0\t2\n",
                    "SELECT ts, v, count(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM t");
        });
    }

    @Test
    public void testMaxDecimal64UnboundedFollowing() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.00m), ('2024-01-01T00:01:00', 10.00m), ('2024-01-01T00:02:00', 20.00m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t30.00\t30.00\n2024-01-01T00:01:00.000000Z\t10.00\t30.00\n2024-01-01T00:02:00.000000Z\t20.00\t30.00\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_v FROM t");
        });
    }

    @Test
    public void testSumDecimal64FullUnbounded() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.00\t60.00\n2024-01-01T00:01:00.000000Z\t20.00\t60.00\n2024-01-01T00:02:00.000000Z\t30.00\t60.00\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal8WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1.0m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t1.0\t2.0\n2024-01-01T00:01:00.000000Z\t\t2.0\n2024-01-01T00:02:00.000000Z\t3.0\t2.0\n",
                    "SELECT ts, v, avg(v) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testSumDecimal8WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1.0m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t1.0\t4.0\n2024-01-01T00:01:00.000000Z\t\t4.0\n2024-01-01T00:02:00.000000Z\t3.0\t4.0\n",
                    "SELECT ts, v, sum(v) OVER () AS sum_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal16WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10.0\t30.0\n2024-01-01T00:01:00.000000Z\t\t30.0\n2024-01-01T00:02:00.000000Z\t30.0\t30.0\n",
                    "SELECT ts, v, max(v) OVER () AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal32WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10.000\t30.000\n2024-01-01T00:01:00.000000Z\t\t30.000\n2024-01-01T00:02:00.000000Z\t30.000\t30.000\n",
                    "SELECT ts, v, max(v) OVER () AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal128WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10.000000\t30.000000\n2024-01-01T00:01:00.000000Z\t\t30.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t30.000000\n",
                    "SELECT ts, v, max(v) OVER () AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal256WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10\t30\n2024-01-01T00:01:00.000000Z\t\t30\n2024-01-01T00:02:00.000000Z\t30\t30\n",
                    "SELECT ts, v, max(v) OVER () AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal128WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.000000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 10.000000m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30.000000\t10.000000\n2024-01-01T00:01:00.000000Z\t\t10.000000\n2024-01-01T00:02:00.000000Z\t10.000000\t10.000000\n",
                    "SELECT ts, v, min(v) OVER () AS min_v FROM t");
        });
    }

    @Test
    public void testMinDecimal256WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 10m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30\t10\n2024-01-01T00:01:00.000000Z\t\t10\n2024-01-01T00:02:00.000000Z\t10\t10\n",
                    "SELECT ts, v, min(v) OVER () AS min_v FROM t");
        });
    }

    @Test
    public void testSumDecimal128WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.000000\t40.000000\n2024-01-01T00:01:00.000000Z\t\t40.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t40.000000\n",
                    "SELECT ts, v, sum(v) OVER () AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal256WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10\t40\n2024-01-01T00:01:00.000000Z\t\t40\n2024-01-01T00:02:00.000000Z\t30\t40\n",
                    "SELECT ts, v, sum(v) OVER () AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal128WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.000000\t20.000000\n2024-01-01T00:01:00.000000Z\t\t20.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t20.000000\n",
                    "SELECT ts, v, avg(v) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal256WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10\t20\n2024-01-01T00:01:00.000000Z\t\t20\n2024-01-01T00:02:00.000000Z\t30\t20\n",
                    "SELECT ts, v, avg(v) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testCountDecimal16WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t10.0\t2\n2024-01-01T00:01:00.000000Z\t\t2\n2024-01-01T00:02:00.000000Z\t30.0\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal32WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t10.000\t2\n2024-01-01T00:01:00.000000Z\t\t2\n2024-01-01T00:02:00.000000Z\t30.000\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal128WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t10.000000\t2\n2024-01-01T00:01:00.000000Z\t\t2\n2024-01-01T00:02:00.000000Z\t30.000000\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal256WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t10\t2\n2024-01-01T00:01:00.000000Z\t\t2\n2024-01-01T00:02:00.000000Z\t30\t2\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal8MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.0m), ('2024-01-01T00:01:00', 'b', 2.0m), ('2024-01-01T00:02:00', 'a', 3.0m), ('2024-01-01T00:03:00', 'b', 4.0m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t1.0\t2\n2024-01-01T00:01:00.000000Z\tb\t2.0\t2\n2024-01-01T00:02:00.000000Z\ta\t3.0\t2\n2024-01-01T00:03:00.000000Z\tb\t4.0\t2\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp) AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal16MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.0m), ('2024-01-01T00:01:00', 'b', 20.0m), ('2024-01-01T00:02:00', 'a', 30.0m), ('2024-01-01T00:03:00', 'b', 40.0m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t10.0\t2\n2024-01-01T00:01:00.000000Z\tb\t20.0\t2\n2024-01-01T00:02:00.000000Z\ta\t30.0\t2\n2024-01-01T00:03:00.000000Z\tb\t40.0\t2\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp) AS c FROM t");
        });
    }

    @Test
    public void testCountDecimal32MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000m), ('2024-01-01T00:01:00', 'b', 20.000m), ('2024-01-01T00:02:00', 'a', 30.000m), ('2024-01-01T00:03:00', 'b', 40.000m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t10.000\t2\n2024-01-01T00:01:00.000000Z\tb\t20.000\t2\n2024-01-01T00:02:00.000000Z\ta\t30.000\t2\n2024-01-01T00:03:00.000000Z\tb\t40.000\t2\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp) AS c FROM t");
        });
    }

    @Test
    public void testSumDecimal16MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.0m), ('2024-01-01T00:01:00', 'b', 20.0m), ('2024-01-01T00:02:00', 'a', 30.0m), ('2024-01-01T00:03:00', 'b', 40.0m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10.0\t40.0\n2024-01-01T00:01:00.000000Z\tb\t20.0\t60.0\n2024-01-01T00:02:00.000000Z\ta\t30.0\t40.0\n2024-01-01T00:03:00.000000Z\tb\t40.0\t60.0\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal32MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000m), ('2024-01-01T00:01:00', 'b', 20.000m), ('2024-01-01T00:02:00', 'a', 30.000m), ('2024-01-01T00:03:00', 'b', 40.000m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10.000\t40.000\n2024-01-01T00:01:00.000000Z\tb\t20.000\t60.000\n2024-01-01T00:02:00.000000Z\ta\t30.000\t40.000\n2024-01-01T00:03:00.000000Z\tb\t40.000\t60.000\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal128MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'b', 20.000000m), ('2024-01-01T00:02:00', 'a', 30.000000m), ('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t40.000000\n2024-01-01T00:01:00.000000Z\tb\t20.000000\t60.000000\n2024-01-01T00:02:00.000000Z\ta\t30.000000\t40.000000\n2024-01-01T00:03:00.000000Z\tb\t40.000000\t60.000000\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal256MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10m), ('2024-01-01T00:01:00', 'b', 20m), ('2024-01-01T00:02:00', 'a', 30m), ('2024-01-01T00:03:00', 'b', 40m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10\t40\n2024-01-01T00:01:00.000000Z\tb\t20\t60\n2024-01-01T00:02:00.000000Z\ta\t30\t40\n2024-01-01T00:03:00.000000Z\tb\t40\t60\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp) AS sum_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal128MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'b', 20.000000m), ('2024-01-01T00:02:00', 'a', 30.000000m), ('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tmax_v\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t30.000000\n2024-01-01T00:01:00.000000Z\tb\t20.000000\t40.000000\n2024-01-01T00:02:00.000000Z\ta\t30.000000\t30.000000\n2024-01-01T00:03:00.000000Z\tb\t40.000000\t40.000000\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal256MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10m), ('2024-01-01T00:01:00', 'b', 20m), ('2024-01-01T00:02:00', 'a', 30m), ('2024-01-01T00:03:00', 'b', 40m)");
            assertSql("ts\tgrp\tv\tmax_v\n2024-01-01T00:00:00.000000Z\ta\t10\t30\n2024-01-01T00:01:00.000000Z\tb\t20\t40\n2024-01-01T00:02:00.000000Z\ta\t30\t30\n2024-01-01T00:03:00.000000Z\tb\t40\t40\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp) AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal128MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'b', 20.000000m), ('2024-01-01T00:02:00', 'a', 30.000000m), ('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tmin_v\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\tb\t20.000000\t20.000000\n2024-01-01T00:02:00.000000Z\ta\t30.000000\t10.000000\n2024-01-01T00:03:00.000000Z\tb\t40.000000\t20.000000\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp) AS min_v FROM t");
        });
    }

    @Test
    public void testMinDecimal256MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10m), ('2024-01-01T00:01:00', 'b', 20m), ('2024-01-01T00:02:00', 'a', 30m), ('2024-01-01T00:03:00', 'b', 40m)");
            assertSql("ts\tgrp\tv\tmin_v\n2024-01-01T00:00:00.000000Z\ta\t10\t10\n2024-01-01T00:01:00.000000Z\tb\t20\t20\n2024-01-01T00:02:00.000000Z\ta\t30\t10\n2024-01-01T00:03:00.000000Z\tb\t40\t20\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp) AS min_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal128MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'b', 20.000000m), ('2024-01-01T00:02:00', 'a', 30.000000m), ('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tavg_v\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t20.000000\n2024-01-01T00:01:00.000000Z\tb\t20.000000\t30.000000\n2024-01-01T00:02:00.000000Z\ta\t30.000000\t20.000000\n2024-01-01T00:03:00.000000Z\tb\t40.000000\t30.000000\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal256MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10m), ('2024-01-01T00:01:00', 'b', 20m), ('2024-01-01T00:02:00', 'a', 30m), ('2024-01-01T00:03:00', 'b', 40m)");
            assertSql("ts\tgrp\tv\tavg_v\n2024-01-01T00:00:00.000000Z\ta\t10\t20\n2024-01-01T00:01:00.000000Z\tb\t20\t30\n2024-01-01T00:02:00.000000Z\ta\t30\t20\n2024-01-01T00:03:00.000000Z\tb\t40\t30\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal16OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', 20.0m), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.0\t20.00000\n2024-01-01T00:01:00.000000Z\t20.0\t20.00000\n2024-01-01T00:02:00.000000Z\t30.0\t20.00000\n",
                    "SELECT ts, v, avg(v, 5) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal32OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', 20.000m), ('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.000\t20.00000\n2024-01-01T00:01:00.000000Z\t20.000\t20.00000\n2024-01-01T00:02:00.000000Z\t30.000\t20.00000\n",
                    "SELECT ts, v, avg(v, 5) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal128OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.000000\t20.00000\n2024-01-01T00:01:00.000000Z\t20.000000\t20.00000\n2024-01-01T00:02:00.000000Z\t30.000000\t20.00000\n",
                    "SELECT ts, v, avg(v, 5) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal256OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10\t20.00000\n2024-01-01T00:01:00.000000Z\t20\t20.00000\n2024-01-01T00:02:00.000000Z\t30\t20.00000\n",
                    "SELECT ts, v, avg(v, 5) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal8OverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1.0m), ('2024-01-01T00:01:00', 2.0m), ('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t1.0\t2.00000\n2024-01-01T00:01:00.000000Z\t2.0\t2.00000\n2024-01-01T00:02:00.000000Z\t3.0\t2.00000\n",
                    "SELECT ts, v, avg(v, 5) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, first_value(v) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', null)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\t\t20.00\n",
                    "SELECT ts, v, last_value(v) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testLagDecimal64IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlg\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t\t\n2024-01-01T00:02:00.000000Z\t30.00\t10.00\n",
                    "SELECT ts, v, lag(v, 1) IGNORE NULLS OVER (ORDER BY ts) AS lg FROM t");
        });
    }

    @Test
    public void testLeadDecimal64IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tld\n2024-01-01T00:00:00.000000Z\t10.00\t30.00\n2024-01-01T00:01:00.000000Z\t\t30.00\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, lead(v, 1) IGNORE NULLS OVER (ORDER BY ts) AS ld FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64IgnoreNullsExtra() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20.00\t\n2024-01-01T00:02:00.000000Z\t30.00\t30.00\n",
                    "SELECT ts, v, nth_value(v, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testSumDecimal64OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t30.00\n2024-01-01T00:02:00.000000Z\t30.00\t60.00\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal8OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1.0m), ('2024-01-01T00:01:00', 2.0m), ('2024-01-01T00:02:00', 3.0m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t1.0\t1.0\n2024-01-01T00:01:00.000000Z\t2.0\t3.0\n2024-01-01T00:02:00.000000Z\t3.0\t6.0\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal16OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', 20.0m), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.0\t10.0\n2024-01-01T00:01:00.000000Z\t20.0\t30.0\n2024-01-01T00:02:00.000000Z\t30.0\t60.0\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal32OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', 20.000m), ('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.000\t10.000\n2024-01-01T00:01:00.000000Z\t20.000\t30.000\n2024-01-01T00:02:00.000000Z\t30.000\t60.000\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal128OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\t20.000000\t30.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t60.000000\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal256OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t10\t10\n2024-01-01T00:01:00.000000Z\t20\t30\n2024-01-01T00:02:00.000000Z\t30\t60\n",
                    "SELECT ts, v, sum(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t15.00\n2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.00m), ('2024-01-01T00:01:00', 10.00m), ('2024-01-01T00:02:00', 20.00m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t30.00\t30.00\n2024-01-01T00:01:00.000000Z\t10.00\t30.00\n2024-01-01T00:02:00.000000Z\t20.00\t30.00\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal64OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 30.00m), ('2024-01-01T00:01:00', 10.00m), ('2024-01-01T00:02:00', 20.00m)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t30.00\t30.00\n2024-01-01T00:01:00.000000Z\t10.00\t10.00\n2024-01-01T00:02:00.000000Z\t20.00\t10.00\n",
                    "SELECT ts, v, min(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testCountDecimal64OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t10.00\t1\n2024-01-01T00:01:00.000000Z\t\t1\n2024-01-01T00:02:00.000000Z\t30.00\t2\n",
                    "SELECT ts, v, count(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t10.00\n2024-01-01T00:02:00.000000Z\t30.00\t10.00\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\t30.00\t30.00\n",
                    "SELECT ts, v, last_value(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64OverRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, nth_value(v, 2) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testSumDecimal64OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\ta\t20.00\t30.00\n2024-01-01T00:02:00.000000Z\tb\t30.00\t30.00\n2024-01-01T00:03:00.000000Z\tb\t40.00\t70.00\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 30.00m), ('2024-01-01T00:02:00', 'b', 40.00m), ('2024-01-01T00:03:00', 'b', 60.00m)");
            assertSql("ts\tgrp\tv\tavg_v\n2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\ta\t30.00\t20.00\n2024-01-01T00:02:00.000000Z\tb\t40.00\t40.00\n2024-01-01T00:03:00.000000Z\tb\t60.00\t50.00\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30.00m), ('2024-01-01T00:01:00', 'a', 10.00m), ('2024-01-01T00:02:00', 'b', 20.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tmax_v\n2024-01-01T00:00:00.000000Z\ta\t30.00\t30.00\n2024-01-01T00:01:00.000000Z\ta\t10.00\t30.00\n2024-01-01T00:02:00.000000Z\tb\t20.00\t20.00\n2024-01-01T00:03:00.000000Z\tb\t40.00\t40.00\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal64OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30.00m), ('2024-01-01T00:01:00', 'a', 10.00m), ('2024-01-01T00:02:00', 'b', 40.00m), ('2024-01-01T00:03:00', 'b', 20.00m)");
            assertSql("ts\tgrp\tv\tmin_v\n2024-01-01T00:00:00.000000Z\ta\t30.00\t30.00\n2024-01-01T00:01:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:02:00.000000Z\tb\t40.00\t40.00\n2024-01-01T00:03:00.000000Z\tb\t20.00\t20.00\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tfv\n2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\ta\t20.00\t10.00\n2024-01-01T00:02:00.000000Z\tb\t30.00\t30.00\n2024-01-01T00:03:00.000000Z\tb\t40.00\t30.00\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tlv\n2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\ta\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\tb\t30.00\t30.00\n2024-01-01T00:03:00.000000Z\tb\t40.00\t40.00\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testCountDecimal64OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t10.00\t1\n2024-01-01T00:01:00.000000Z\ta\t20.00\t2\n2024-01-01T00:02:00.000000Z\tb\t30.00\t1\n2024-01-01T00:03:00.000000Z\tb\t40.00\t2\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal16IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20.0m), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20.0\t20.0\n2024-01-01T00:02:00.000000Z\t30.0\t20.0\n",
                    "SELECT ts, v, first_value(v) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal32IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20.000m), ('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20.000\t20.000\n2024-01-01T00:02:00.000000Z\t30.000\t20.000\n",
                    "SELECT ts, v, first_value(v) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal128IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20.000000\t20.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t20.000000\n",
                    "SELECT ts, v, first_value(v) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal256IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20\t20\n2024-01-01T00:02:00.000000Z\t30\t20\n",
                    "SELECT ts, v, first_value(v) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testLagDecimal16IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tlg\n2024-01-01T00:00:00.000000Z\t10.0\t\n2024-01-01T00:01:00.000000Z\t\t\n2024-01-01T00:02:00.000000Z\t30.0\t10.0\n",
                    "SELECT ts, v, lag(v, 1) IGNORE NULLS OVER (ORDER BY ts) AS lg FROM t");
        });
    }

    @Test
    public void testLagDecimal32IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.000m)");
            assertSql("ts\tv\tlg\n2024-01-01T00:00:00.000000Z\t10.000\t\n2024-01-01T00:01:00.000000Z\t\t\n2024-01-01T00:02:00.000000Z\t30.000\t10.000\n",
                    "SELECT ts, v, lag(v, 1) IGNORE NULLS OVER (ORDER BY ts) AS lg FROM t");
        });
    }

    @Test
    public void testLagDecimal128IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tlg\n2024-01-01T00:00:00.000000Z\t10.000000\t\n2024-01-01T00:01:00.000000Z\t\t\n2024-01-01T00:02:00.000000Z\t30.000000\t10.000000\n",
                    "SELECT ts, v, lag(v, 1) IGNORE NULLS OVER (ORDER BY ts) AS lg FROM t");
        });
    }

    @Test
    public void testLagDecimal256IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tlg\n2024-01-01T00:00:00.000000Z\t10\t\n2024-01-01T00:01:00.000000Z\t\t\n2024-01-01T00:02:00.000000Z\t30\t10\n",
                    "SELECT ts, v, lag(v, 1) IGNORE NULLS OVER (ORDER BY ts) AS lg FROM t");
        });
    }

    @Test
    public void testLeadDecimal16IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.0m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tld\n2024-01-01T00:00:00.000000Z\t10.0\t30.0\n2024-01-01T00:01:00.000000Z\t\t30.0\n2024-01-01T00:02:00.000000Z\t30.0\t\n",
                    "SELECT ts, v, lead(v, 1) IGNORE NULLS OVER (ORDER BY ts) AS ld FROM t");
        });
    }

    @Test
    public void testLeadDecimal128IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tld\n2024-01-01T00:00:00.000000Z\t10.000000\t30.000000\n2024-01-01T00:01:00.000000Z\t\t30.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t\n",
                    "SELECT ts, v, lead(v, 1) IGNORE NULLS OVER (ORDER BY ts) AS ld FROM t");
        });
    }

    @Test
    public void testLeadDecimal256IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tld\n2024-01-01T00:00:00.000000Z\t10\t30\n2024-01-01T00:01:00.000000Z\t\t30\n2024-01-01T00:02:00.000000Z\t30\t\n",
                    "SELECT ts, v, lead(v, 1) IGNORE NULLS OVER (ORDER BY ts) AS ld FROM t");
        });
    }

    @Test
    public void testNthValueDecimal16IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20.0m), ('2024-01-01T00:02:00', 30.0m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20.0\t\n2024-01-01T00:02:00.000000Z\t30.0\t30.0\n",
                    "SELECT ts, v, nth_value(v, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal128IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20.000000\t\n2024-01-01T00:02:00.000000Z\t30.000000\t30.000000\n",
                    "SELECT ts, v, nth_value(v, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal256IgnoreNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t20\t\n2024-01-01T00:02:00.000000Z\t30\t30\n",
                    "SELECT ts, v, nth_value(v, 2) IGNORE NULLS OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal64WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.00\t20.00000\n2024-01-01T00:01:00.000000Z\t\t20.00000\n2024-01-01T00:02:00.000000Z\t30.00\t20.00000\n",
                    "SELECT ts, v, avg(v, 5) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal64MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'b', 20.00m), ('2024-01-01T00:02:00', 'a', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tavg_v\n2024-01-01T00:00:00.000000Z\ta\t10.00\t20.00000\n2024-01-01T00:01:00.000000Z\tb\t20.00\t30.00000\n2024-01-01T00:02:00.000000Z\ta\t30.00\t20.00000\n2024-01-01T00:03:00.000000Z\tb\t40.00\t30.00000\n",
                    "SELECT ts, grp, v, avg(v, 5) OVER (PARTITION BY grp) AS avg_v FROM t");
        });
    }

    @Test
    public void testCountDecimal64EmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            assertSql("ts\tv\tc\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testSumDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', null)");
            assertSql("ts\tv\tsum_v\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t\t\n2024-01-01T00:02:00.000000Z\t\t\n",
                    "SELECT ts, v, sum(v) OVER () AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t\t\n",
                    "SELECT ts, v, avg(v) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t\t\n",
                    "SELECT ts, v, max(v) OVER () AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null)");
            assertSql("ts\tv\tmin_v\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t\t\n",
                    "SELECT ts, v, min(v) OVER () AS min_v FROM t");
        });
    }

    @Test
    public void testCountDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null), ('2024-01-01T00:02:00', null)");
            assertSql("ts\tv\tc\n2024-01-01T00:00:00.000000Z\t\t0\n2024-01-01T00:01:00.000000Z\t\t0\n2024-01-01T00:02:00.000000Z\t\t0\n",
                    "SELECT ts, v, count(v) OVER () AS c FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t\t\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', null), ('2024-01-01T00:01:00', null)");
            assertSql("ts\tv\tlv\n2024-01-01T00:00:00.000000Z\t\t\n2024-01-01T00:01:00.000000Z\t\t\n",
                    "SELECT ts, v, last_value(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testSumDecimal64OverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\ta\t20.00\t30.00\n2024-01-01T00:02:00.000000Z\tb\t30.00\t30.00\n2024-01-01T00:03:00.000000Z\tb\t40.00\t70.00\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64OverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 30.00m), ('2024-01-01T00:02:00', 'b', 40.00m), ('2024-01-01T00:03:00', 'b', 60.00m)");
            assertSql("ts\tgrp\tv\tavg_v\n2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\ta\t30.00\t20.00\n2024-01-01T00:02:00.000000Z\tb\t40.00\t40.00\n2024-01-01T00:03:00.000000Z\tb\t60.00\t50.00\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal64OverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30.00m), ('2024-01-01T00:01:00', 'a', 10.00m), ('2024-01-01T00:02:00', 'b', 20.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tmax_v\n2024-01-01T00:00:00.000000Z\ta\t30.00\t30.00\n2024-01-01T00:01:00.000000Z\ta\t10.00\t30.00\n2024-01-01T00:02:00.000000Z\tb\t20.00\t20.00\n2024-01-01T00:03:00.000000Z\tb\t40.00\t40.00\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal64OverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30.00m), ('2024-01-01T00:01:00', 'a', 10.00m), ('2024-01-01T00:02:00', 'b', 40.00m), ('2024-01-01T00:03:00', 'b', 20.00m)");
            assertSql("ts\tgrp\tv\tmin_v\n2024-01-01T00:00:00.000000Z\ta\t30.00\t30.00\n2024-01-01T00:01:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:02:00.000000Z\tb\t40.00\t40.00\n2024-01-01T00:03:00.000000Z\tb\t20.00\t20.00\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testCountDecimal64OverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tc\n2024-01-01T00:00:00.000000Z\ta\t10.00\t1\n2024-01-01T00:01:00.000000Z\ta\t20.00\t2\n2024-01-01T00:02:00.000000Z\tb\t30.00\t1\n2024-01-01T00:03:00.000000Z\tb\t40.00\t2\n",
                    "SELECT ts, grp, v, count(v) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal64OverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tfv\n2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\ta\t20.00\t10.00\n2024-01-01T00:02:00.000000Z\tb\t30.00\t30.00\n2024-01-01T00:03:00.000000Z\tb\t40.00\t30.00\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal64OverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tlv\n2024-01-01T00:00:00.000000Z\ta\t10.00\t10.00\n2024-01-01T00:01:00.000000Z\ta\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\tb\t30.00\t30.00\n2024-01-01T00:03:00.000000Z\tb\t40.00\t40.00\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64OverPartitionRangeFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.00m), ('2024-01-01T00:01:00', 'a', 20.00m), ('2024-01-01T00:02:00', 'b', 30.00m), ('2024-01-01T00:03:00', 'b', 40.00m)");
            assertSql("ts\tgrp\tv\tnv\n2024-01-01T00:00:00.000000Z\ta\t10.00\t\n2024-01-01T00:01:00.000000Z\ta\t20.00\t20.00\n2024-01-01T00:02:00.000000Z\tb\t30.00\t\n2024-01-01T00:03:00.000000Z\tb\t40.00\t40.00\n",
                    "SELECT ts, grp, v, nth_value(v, 2) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }

    @Test
    public void testSumDecimal8OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(2, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 1.0m), ('2024-01-01T00:01:00', 'a', 2.0m), ('2024-01-01T00:02:00', 'b', 3.0m), ('2024-01-01T00:03:00', 'b', 4.0m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t1.0\t1.0\n2024-01-01T00:01:00.000000Z\ta\t2.0\t3.0\n2024-01-01T00:02:00.000000Z\tb\t3.0\t3.0\n2024-01-01T00:03:00.000000Z\tb\t4.0\t7.0\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal128OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'a', 20.000000m), ('2024-01-01T00:02:00', 'b', 30.000000m), ('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\ta\t20.000000\t30.000000\n2024-01-01T00:02:00.000000Z\tb\t30.000000\t30.000000\n2024-01-01T00:03:00.000000Z\tb\t40.000000\t70.000000\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal256OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10m), ('2024-01-01T00:01:00', 'a', 20m), ('2024-01-01T00:02:00', 'b', 30m), ('2024-01-01T00:03:00', 'b', 40m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10\t10\n2024-01-01T00:01:00.000000Z\ta\t20\t30\n2024-01-01T00:02:00.000000Z\tb\t30\t30\n2024-01-01T00:03:00.000000Z\tb\t40\t70\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal128OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30.000000m), ('2024-01-01T00:01:00', 'a', 10.000000m), ('2024-01-01T00:02:00', 'b', 20.000000m), ('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tmax_v\n2024-01-01T00:00:00.000000Z\ta\t30.000000\t30.000000\n2024-01-01T00:01:00.000000Z\ta\t10.000000\t30.000000\n2024-01-01T00:02:00.000000Z\tb\t20.000000\t20.000000\n2024-01-01T00:03:00.000000Z\tb\t40.000000\t40.000000\n",
                    "SELECT ts, grp, v, max(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMinDecimal128OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 30.000000m), ('2024-01-01T00:01:00', 'a', 10.000000m), ('2024-01-01T00:02:00', 'b', 40.000000m), ('2024-01-01T00:03:00', 'b', 20.000000m)");
            assertSql("ts\tgrp\tv\tmin_v\n2024-01-01T00:00:00.000000Z\ta\t30.000000\t30.000000\n2024-01-01T00:01:00.000000Z\ta\t10.000000\t10.000000\n2024-01-01T00:02:00.000000Z\tb\t40.000000\t40.000000\n2024-01-01T00:03:00.000000Z\tb\t20.000000\t20.000000\n",
                    "SELECT ts, grp, v, min(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal128OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'a', 30.000000m), ('2024-01-01T00:02:00', 'b', 40.000000m), ('2024-01-01T00:03:00', 'b', 60.000000m)");
            assertSql("ts\tgrp\tv\tavg_v\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\ta\t30.000000\t20.000000\n2024-01-01T00:02:00.000000Z\tb\t40.000000\t40.000000\n2024-01-01T00:03:00.000000Z\tb\t60.000000\t50.000000\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal256OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10m), ('2024-01-01T00:01:00', 'a', 30m), ('2024-01-01T00:02:00', 'b', 40m), ('2024-01-01T00:03:00', 'b', 60m)");
            assertSql("ts\tgrp\tv\tavg_v\n2024-01-01T00:00:00.000000Z\ta\t10\t10\n2024-01-01T00:01:00.000000Z\ta\t30\t20\n2024-01-01T00:02:00.000000Z\tb\t40\t40\n2024-01-01T00:03:00.000000Z\tb\t60\t50\n",
                    "SELECT ts, grp, v, avg(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal128OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'a', 20.000000m), ('2024-01-01T00:02:00', 'b', 30.000000m), ('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tfv\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\ta\t20.000000\t10.000000\n2024-01-01T00:02:00.000000Z\tb\t30.000000\t30.000000\n2024-01-01T00:03:00.000000Z\tb\t40.000000\t30.000000\n",
                    "SELECT ts, grp, v, first_value(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testLastValueDecimal128OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000000m), ('2024-01-01T00:01:00', 'a', 20.000000m), ('2024-01-01T00:02:00', 'b', 30.000000m), ('2024-01-01T00:03:00', 'b', 40.000000m)");
            assertSql("ts\tgrp\tv\tlv\n2024-01-01T00:00:00.000000Z\ta\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\ta\t20.000000\t20.000000\n2024-01-01T00:02:00.000000Z\tb\t30.000000\t30.000000\n2024-01-01T00:03:00.000000Z\tb\t40.000000\t40.000000\n",
                    "SELECT ts, grp, v, last_value(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lv FROM t");
        });
    }

    @Test
    public void testSumDecimal16OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(4, 1)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.0m), ('2024-01-01T00:01:00', 'a', 20.0m), ('2024-01-01T00:02:00', 'b', 30.0m), ('2024-01-01T00:03:00', 'b', 40.0m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10.0\t10.0\n2024-01-01T00:01:00.000000Z\ta\t20.0\t30.0\n2024-01-01T00:02:00.000000Z\tb\t30.0\t30.0\n2024-01-01T00:03:00.000000Z\tb\t40.0\t70.0\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testSumDecimal32OverPartitionRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(9, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'a', 10.000m), ('2024-01-01T00:01:00', 'a', 20.000m), ('2024-01-01T00:02:00', 'b', 30.000m), ('2024-01-01T00:03:00', 'b', 40.000m)");
            assertSql("ts\tgrp\tv\tsum_v\n2024-01-01T00:00:00.000000Z\ta\t10.000\t10.000\n2024-01-01T00:01:00.000000Z\ta\t20.000\t30.000\n2024-01-01T00:02:00.000000Z\tb\t30.000\t30.000\n2024-01-01T00:03:00.000000Z\tb\t40.000\t70.000\n",
                    "SELECT ts, grp, v, sum(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64Precision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1.00m), ('2024-01-01T00:01:00', 2.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t1.00\t1.50\n2024-01-01T00:01:00.000000Z\t2.00\t1.50\n",
                    "SELECT ts, v, avg(v) OVER () AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgDecimal64HalfEven() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1.00m), ('2024-01-01T00:01:00', 2.00m), ('2024-01-01T00:02:00', 4.00m)");
            assertSql("avg_v\n2.33\n",
                    "SELECT avg(v) OVER () AS avg_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testSumDecimal64LargePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp SYMBOL, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT timestamp_sequence(0, 60000000), 'g' || (x % 4)::string, cast(1.00 as decimal(18, 2)) FROM long_sequence(1000)");
            assertSql("sum_v\n250.00\n",
                    "SELECT sum(v) OVER (PARTITION BY grp) AS sum_v FROM t LIMIT 1");
        });
    }

    @Test
    public void testFirstValueDecimal128SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 20.000000m), ('2024-01-01T00:02:00', 30.000000m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\t20.000000\t10.000000\n2024-01-01T00:02:00.000000Z\t30.000000\t20.000000\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testFirstValueDecimal256SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 20m), ('2024-01-01T00:02:00', 30m)");
            assertSql("ts\tv\tfv\n2024-01-01T00:00:00.000000Z\t10\t10\n2024-01-01T00:01:00.000000Z\t20\t10\n2024-01-01T00:02:00.000000Z\t30\t20\n",
                    "SELECT ts, v, first_value(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS fv FROM t");
        });
    }

    @Test
    public void testMaxDecimal128SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(38, 6)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.000000m), ('2024-01-01T00:01:00', 30.000000m), ('2024-01-01T00:02:00', 20.000000m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10.000000\t10.000000\n2024-01-01T00:01:00.000000Z\t30.000000\t30.000000\n2024-01-01T00:02:00.000000Z\t20.000000\t30.000000\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testMaxDecimal256SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(60, 0)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10m), ('2024-01-01T00:01:00', 30m), ('2024-01-01T00:02:00', 20m)");
            assertSql("ts\tv\tmax_v\n2024-01-01T00:00:00.000000Z\t10\t10\n2024-01-01T00:01:00.000000Z\t30\t30\n2024-01-01T00:02:00.000000Z\t20\t30\n",
                    "SELECT ts, v, max(v) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal64SlidingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 30.00m), ('2024-01-01T00:02:00', 50.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.00\t10.00000\n2024-01-01T00:01:00.000000Z\t30.00\t20.00000\n2024-01-01T00:02:00.000000Z\t50.00\t40.00000\n",
                    "SELECT ts, v, avg(v, 5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testAvgRescaleDecimal64OverUnboundedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tavg_v\n2024-01-01T00:00:00.000000Z\t10.00\t10.00000\n2024-01-01T00:01:00.000000Z\t20.00\t15.00000\n2024-01-01T00:02:00.000000Z\t30.00\t20.00000\n",
                    "SELECT ts, v, avg(v, 5) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v FROM t");
        });
    }

    @Test
    public void testLagDecimal64NoOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlg\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t20.00\t10.00\n2024-01-01T00:02:00.000000Z\t30.00\t20.00\n",
                    "SELECT ts, v, lag(v) OVER (ORDER BY ts) AS lg FROM t");
        });
    }

    @Test
    public void testLeadDecimal64NoOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tld\n2024-01-01T00:00:00.000000Z\t10.00\t20.00\n2024-01-01T00:01:00.000000Z\t20.00\t30.00\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, lead(v) OVER (ORDER BY ts) AS ld FROM t");
        });
    }

    @Test
    public void testLagDecimal64LargeOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tlg\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t20.00\t\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, lag(v, 10) OVER (ORDER BY ts) AS lg FROM t");
        });
    }

    @Test
    public void testLeadDecimal64LargeOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tld\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t20.00\t\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, lead(v, 10) OVER (ORDER BY ts) AS ld FROM t");
        });
    }

    @Test
    public void testNthValueDecimal64OutOfRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v decimal(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10.00m), ('2024-01-01T00:01:00', 20.00m), ('2024-01-01T00:02:00', 30.00m)");
            assertSql("ts\tv\tnv\n2024-01-01T00:00:00.000000Z\t10.00\t\n2024-01-01T00:01:00.000000Z\t20.00\t\n2024-01-01T00:02:00.000000Z\t30.00\t\n",
                    "SELECT ts, v, nth_value(v, 99) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nv FROM t");
        });
    }
}
