/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class WindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testAverageOverGroups() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");
            insert("insert into tab select x::timestamp, x/4, x%5 from long_sequence(7)");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts groups unbounded preceding) from tab",
                    17, "function not implemented for given window parameters");
        });
    }

    @Test
    public void testAverageOverNonPartitionedRangeWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            //default buffer size holds 65k entries
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");
            insert("insert into tab select x::timestamp, x/4, x from long_sequence(40000)");
            //trigger removal of rows below lo boundary AND resize of buffer
            insert("insert into tab select (100000+x)::timestamp, x/4, x from long_sequence(90000)");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.189996Z\t22499\t89996\t49996.0\n" +
                            "1970-01-01T00:00:00.189997Z\t22499\t89997\t49997.0\n" +
                            "1970-01-01T00:00:00.189998Z\t22499\t89998\t49998.0\n" +
                            "1970-01-01T00:00:00.189999Z\t22499\t89999\t49999.0\n" +
                            "1970-01-01T00:00:00.190000Z\t22500\t90000\t50000.0\n",
                    "select * from (select ts, i, j, avg(j) over (order by ts range between 80000 preceding and current row) from tab) limit -5");

            ddl("truncate table tab");
            // trigger buffer resize
            insert("insert into tab select (100000+x)::timestamp, x/4, x from long_sequence(90000)");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.189996Z\t22499\t89996\t49996.0\n" +
                            "1970-01-01T00:00:00.189997Z\t22499\t89997\t49997.0\n" +
                            "1970-01-01T00:00:00.189998Z\t22499\t89998\t49998.0\n" +
                            "1970-01-01T00:00:00.189999Z\t22499\t89999\t49999.0\n" +
                            "1970-01-01T00:00:00.190000Z\t22500\t90000\t50000.0\n",
                    "select * from (select ts, i, j, avg(j) over (order by ts range between 80000 preceding and current row) from tab) limit -5");
        });
    }

    @Test
    public void testAverageOverNonPartitionedRowsWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            insert("insert into tab select x::timestamp, x/10000, x from long_sequence(39999)");
            insert("insert into tab select (100000+x)::timestamp, (100000+x)%4, (100000+x) from long_sequence(4*90000)");

            String expected = "ts\tj\tavg\n" +
                    "1970-01-01T00:00:00.460000Z\t460000\t420000.0\n";

            // cross-check with moving average re-write using aggregate functions
            assertSql(expected,
                    " select max(ts) as ts, max(j) j, avg(j) as avg from " +
                            "( select ts, i, j, row_number() over (order by ts desc) as rn from tab order by ts desc) " +
                            "where rn between 1 and 80001 ");

            assertSql(expected,
                    "select * from (" +
                            "select * from (select ts, j, avg(j) over (order by ts rows between 80000 preceding and current row) from tab) limit -1) ");
        });
    }

    @Test
    public void testAverageOverNonPartitionedRowsWithLargeFrameRandomData() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            insert("insert into tab select x::timestamp, x/10000, x from long_sequence(39999)");
            insert("insert into tab select (100000+x)::timestamp, rnd_long(1,10000,10), rnd_long(1,100000,10) from long_sequence(1000000)");

            String expected = "ts\tavg\n" +
                    "1970-01-01T00:00:01.100000Z\t49980.066958378644\n";

            // cross-check with moving average re-write using aggregate functions
            assertSql(expected,
                    " select max(ts) as ts, avg(j) as avg from " +
                            "( select ts, i, j, row_number() over (order by ts desc) as rn from tab order by ts desc) " +
                            "where rn between 1 and 80001 ");

            assertSql(expected,
                    "select * from (" +
                            "select * from (select ts, avg(j) over (order by ts rows between 80000 preceding and current row) from tab) limit -1) ");
        });
    }

    @Test
    public void testAverageOverPartitionedRangeWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            //default buffer size holds 65k entries in total, 32 per partition, see CairoConfiguration.getSqlWindowInitialRangeBufferSize()
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            // trigger per-partition buffers growth and free list usage
            insert("insert into tab select x::timestamp, x/10000, x from long_sequence(39999)");
            //trigger removal of rows below lo boundary AND resize of buffer
            insert("insert into tab select (100000+x)::timestamp, (100000+x)%4, (100000+x) from long_sequence(4*90000)");

            String expected = "ts\ti\tj\tavg\n" +
                    "1970-01-01T00:00:00.460000Z\t0\t460000\t420000.0\n" +
                    "1970-01-01T00:00:00.459997Z\t1\t459997\t419997.0\n" +
                    "1970-01-01T00:00:00.459998Z\t2\t459998\t419998.0\n" +
                    "1970-01-01T00:00:00.459999Z\t3\t459999\t419999.0\n";

            // cross-check with moving average re-write using aggregate functions
            assertSql(expected,
                    " select max(data.ts) as ts, data.i as i, max(data.j) as j, avg(data.j) as avg from " +
                            "( select i, max(ts) as max from tab group by i) cnt " +
                            "join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                            "group by data.i");

            assertSql(expected,
                    "select * from (select * from (select ts, i, j, avg(j) over (partition by i order by ts range between 80000 preceding and current row) from tab) limit -4) order by i");
        });
    }

    @Test
    public void testAverageOverPartitionedRangeWithLargeFrameRandomData() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");
            insert("insert into tab select (100000+x)::timestamp, rnd_long(1,20,10), rnd_long(1,1000,5) from long_sequence(1000000)");

            String expected = "ts\ti\tavg\n" +
                    "1970-01-01T00:00:01.099967Z\tNaN\t495.40261282660333\n" +
                    "1970-01-01T00:00:01.099995Z\t1\t495.08707124010556\n" +
                    "1970-01-01T00:00:01.099973Z\t2\t506.5011448196909\n" +
                    "1970-01-01T00:00:01.099908Z\t3\t505.95267958950967\n" +
                    "1970-01-01T00:00:01.099977Z\t4\t501.16155593412833\n" +
                    "1970-01-01T00:00:01.099994Z\t5\t494.87667161961366\n" +
                    "1970-01-01T00:00:01.099991Z\t6\t500.67453098351336\n" +
                    "1970-01-01T00:00:01.099998Z\t7\t497.7231450719823\n" +
                    "1970-01-01T00:00:01.099997Z\t8\t498.6340425531915\n" +
                    "1970-01-01T00:00:01.099992Z\t9\t499.1758750361585\n" +
                    "1970-01-01T00:00:01.099989Z\t10\t500.3242937853107\n" +
                    "1970-01-01T00:00:01.099976Z\t11\t501.4019192774485\n" +
                    "1970-01-01T00:00:01.099984Z\t12\t489.8953058321479\n" +
                    "1970-01-01T00:00:01.099952Z\t13\t500.65723270440253\n" +
                    "1970-01-01T00:00:01.099996Z\t14\t506.8769141866513\n" +
                    "1970-01-01T00:00:01.100000Z\t15\t497.0794058840331\n" +
                    "1970-01-01T00:00:01.099979Z\t16\t499.3338209479228\n" +
                    "1970-01-01T00:00:01.099951Z\t17\t492.7804469273743\n" +
                    "1970-01-01T00:00:01.099999Z\t18\t501.4806333050608\n" +
                    "1970-01-01T00:00:01.099957Z\t19\t501.01901034386356\n" +
                    "1970-01-01T00:00:01.099987Z\t20\t498.1350566366541\n";

            // cross-check with moving average re-write using aggregate functions
            assertSql(expected,
                    " select max(data.ts) as ts, data.i as i, avg(data.j) as avg from " +
                            "( select i, max(ts) as max from tab group by i) cnt " +
                            "join tab data on cnt.i = data.i and data.ts >= (cnt.max - 80000) " +
                            "group by data.i " +
                            "order by data.i ");

            assertSql(expected,
                    "select last(ts) as ts, i, last(avg) as avg from " +
                            "(select * from (select ts, i, avg(j) over (partition by i order by ts range between 80000 preceding and current row) avg " +
                            "from tab ) " +
                            "limit -100 )" +
                            "order by i");
        });
    }

    @Test
    public void testAverageOverPartitionedRowsWithLargeFrame() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            insert("insert into tab select x::timestamp, x/10000, x from long_sequence(39999)");
            insert("insert into tab select (100000+x)::timestamp, (100000+x)%4, (100000+x) from long_sequence(4*90000)");

            String expected = "ts\ti\tj\tavg\n" +
                    "1970-01-01T00:00:00.460000Z\t0\t460000\t300000.0\n" +
                    "1970-01-01T00:00:00.459997Z\t1\t459997\t299997.0\n" +
                    "1970-01-01T00:00:00.459998Z\t2\t459998\t299998.0\n" +
                    "1970-01-01T00:00:00.459999Z\t3\t459999\t299999.0\n";

            // cross-check with moving average re-write using aggregate functions
            assertSql(expected,
                    " select max(ts) as ts, i, max(j) j, avg(j) as avg from " +
                            "( select ts, i, j, row_number() over (partition by i order by ts desc) as rn from tab order by ts desc) " +
                            "where rn between 1 and 80001 " +
                            "group by i " +
                            "order by i");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.460000Z\t0\t460000\t300000.0\n" +
                            "1970-01-01T00:00:00.459997Z\t1\t459997\t299997.0\n" +
                            "1970-01-01T00:00:00.459998Z\t2\t459998\t299998.0\n" +
                            "1970-01-01T00:00:00.459999Z\t3\t459999\t299999.0\n",
                    "select * from (" +
                            "select * from (select ts, i, j, avg(j) over (partition by i order by ts rows between 80000 preceding and current row) from tab) limit -4) " +
                            "order by i");
        });
    }

    @Test
    public void testAverageOverRange() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab_big (ts timestamp, i long, j long) timestamp(ts)");
            insert("insert into tab_big select (x*1000000)::timestamp, x/4, x%5 from long_sequence(10)");

            // tests when frame doesn't end on current row and time gaps between values are bigger than hi bound
            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:01.000000Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:02.000000Z\t0\t2\t1.0\n" +
                            "1970-01-01T00:00:03.000000Z\t0\t3\t1.5\n" +
                            "1970-01-01T00:00:04.000000Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:05.000000Z\t1\t0\t4.0\n" +
                            "1970-01-01T00:00:06.000000Z\t1\t1\t2.0\n" +
                            "1970-01-01T00:00:07.000000Z\t1\t2\t1.6666666666666667\n" +
                            "1970-01-01T00:00:08.000000Z\t2\t3\tNaN\n" +
                            "1970-01-01T00:00:09.000000Z\t2\t4\t3.0\n" +
                            "1970-01-01T00:00:10.000000Z\t2\t0\t3.5\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts range between unbounded preceding and 1 preceding) from tab_big");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:10.000000Z\t2\t0\tNaN\n" +
                            "1970-01-01T00:00:09.000000Z\t2\t4\t0.0\n" +
                            "1970-01-01T00:00:08.000000Z\t2\t3\t2.0\n" +
                            "1970-01-01T00:00:07.000000Z\t1\t2\tNaN\n" +
                            "1970-01-01T00:00:06.000000Z\t1\t1\t2.0\n" +
                            "1970-01-01T00:00:05.000000Z\t1\t0\t1.5\n" +
                            "1970-01-01T00:00:04.000000Z\t1\t4\t1.0\n" +
                            "1970-01-01T00:00:03.000000Z\t0\t3\tNaN\n" +
                            "1970-01-01T00:00:02.000000Z\t0\t2\t3.0\n" +
                            "1970-01-01T00:00:01.000000Z\t0\t1\t2.5\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) from tab_big order by ts desc");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:01.000000Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:02.000000Z\t0\t2\t1.0\n" +
                            "1970-01-01T00:00:03.000000Z\t0\t3\t1.5\n" +
                            "1970-01-01T00:00:04.000000Z\t1\t4\t2.0\n" +
                            "1970-01-01T00:00:05.000000Z\t1\t0\t2.5\n" +
                            "1970-01-01T00:00:06.000000Z\t1\t1\t2.0\n" +
                            "1970-01-01T00:00:07.000000Z\t1\t2\t1.8333333333333333\n" +
                            "1970-01-01T00:00:08.000000Z\t2\t3\t1.8571428571428572\n" +
                            "1970-01-01T00:00:09.000000Z\t2\t4\t2.0\n" +
                            "1970-01-01T00:00:10.000000Z\t2\t0\t2.2222222222222223\n",
                    "select ts, i, j, avg(j) over (order by ts range between unbounded preceding and 1 preceding) from tab_big");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:10.000000Z\t2\t0\tNaN\n" +
                            "1970-01-01T00:00:09.000000Z\t2\t4\t0.0\n" +
                            "1970-01-01T00:00:08.000000Z\t2\t3\t2.0\n" +
                            "1970-01-01T00:00:07.000000Z\t1\t2\t2.3333333333333335\n" +
                            "1970-01-01T00:00:06.000000Z\t1\t1\t2.25\n" +
                            "1970-01-01T00:00:05.000000Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:04.000000Z\t1\t4\t1.6666666666666667\n" +
                            "1970-01-01T00:00:03.000000Z\t0\t3\t2.0\n" +
                            "1970-01-01T00:00:02.000000Z\t0\t2\t2.125\n" +
                            "1970-01-01T00:00:01.000000Z\t0\t1\t2.111111111111111\n",
                    "select ts, i, j, avg(j) over (order by ts desc range between unbounded preceding and 1 preceding) from tab_big order by ts desc");

            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");
            insert("insert into tab select x::timestamp, x/4, x%5 from long_sequence(7)");

            // tests for between X preceding and [Y preceding | current row]
            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.8571428571428572\n",
                    "select ts, i, j, avg(j) over () from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.75\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.75\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.75\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.75\n",
                    "select ts, i, j, avg(j) over (partition by i) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.5\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t0.5\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.5\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts range between 1 microsecond preceding and current row) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tNaN\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t4.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts rows between 4 preceding and 2 preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tNaN\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tNaN\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tNaN\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts rows between 20 preceding and 10 preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.5\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t3.0\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts desc range between 4 microseconds preceding and 2 preceding) from tab order by ts desc");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.5\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.75\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.5\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts desc range between 4 preceding and current row) from tab order by ts desc");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts desc range between 0 preceding and current row) from tab order by ts desc");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts asc range between 0 preceding and current row) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.6666666666666667\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.75\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts asc range between unbounded preceding and current row) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.5\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.75\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.5\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts desc range between unbounded preceding and current row) from tab order by ts desc");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.5\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t4.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t2.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.6666666666666667\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts asc range between unbounded preceding and 1 preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t2.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.5\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t3.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.5\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) from tab order by ts desc");

            //all nulls because values never enter the frame
            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tNaN\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tNaN\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tNaN\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts asc range between unbounded preceding and 10 preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tNaN\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts desc range between unbounded preceding and 10 preceding) from tab order by ts desc");

            //with duplicate timestamp values (but still unique within partition)
            ddl("create table dups(ts timestamp, i long, j long) timestamp(ts) partition by year");
            insert("insert into dups select (x/2)::timestamp, x%2, x%5 from long_sequence(10)");

            assertSql("ts\ti\tj\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t2\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t3\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t4\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t1\n" +
                            "1970-01-01T00:00:00.000003Z\t1\t2\n" +
                            "1970-01-01T00:00:00.000004Z\t0\t3\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\n" +
                            "1970-01-01T00:00:00.000005Z\t0\t0\n",
                    "select * from dups");

            String dupResult = "ts\ti\tj\tavg\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t2\t2.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t3\t2.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t4\t3.0\n" +
                    "1970-01-01T00:00:00.000002Z\t1\t0\t1.3333333333333333\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t1\t2.3333333333333335\n" +
                    "1970-01-01T00:00:00.000003Z\t1\t2\t1.5\n" +
                    "1970-01-01T00:00:00.000004Z\t0\t3\t2.5\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\t2.0\n" +
                    "1970-01-01T00:00:00.000005Z\t0\t0\t2.0\n";

            assertSql(dupResult,
                    "select ts, i, j, avg(j) over (partition by i order by ts range between 4 preceding and current row) from dups");

            assertSql(dupResult,
                    "select ts, i, j, avg(j) over (partition by i order by ts range between 4 preceding and current row) from dups order by ts");

            assertSql(dupResult,
                    "select ts, i, j, avg(j) over (partition by i order by ts range between unbounded preceding and current row) from dups order by ts");

            String dupResult2 = "ts\ti\tj\tavg\n" +
                    "1970-01-01T00:00:00.000005Z\t0\t0\t0.0\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                    "1970-01-01T00:00:00.000004Z\t0\t3\t1.5\n" +
                    "1970-01-01T00:00:00.000003Z\t1\t2\t3.0\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t1\t1.3333333333333333\n" +
                    "1970-01-01T00:00:00.000002Z\t1\t0\t2.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t4\t2.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t3\t2.25\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t2\t2.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t2.0\n";

            assertSql(dupResult2,
                    "select ts, i, j, avg(j) over (partition by i order by ts desc range between 4 preceding and current row) from dups order by ts desc");

            assertSql(dupResult2,
                    "select ts, i, j, avg(j) over (partition by i order by ts desc range between unbounded preceding and current row) from dups order by ts desc");

            //with duplicate timestamp values (including ts duplicates within partition)
            ddl("create table dups2(ts timestamp, i long, j long, n long) timestamp(ts) partition by year");
            insert("insert into dups2 select (x/4)::timestamp, x%2, x%5, x from long_sequence(10)");

            assertSql("ts\ti\tj\tn\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t4\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t6\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t8\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t10\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t1\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t3\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t5\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t7\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t9\n",
                    "select * from dups2 order by i, n");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.5\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t3.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t1.5\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t0.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t4.0\n",
                    "select ts, i, j, avg from ( select ts, i, j, n, avg(j) over (partition by i order by ts range between 0 preceding and current row) as avg from dups2 limit 10) order by i, n");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t3.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t2.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t1.5\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t2.5\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\n",
                    "select ts, i, j, avg from ( select ts, i, j, n, avg(j) over (partition by i order by ts desc range between 0 preceding and current row) as avg from dups2 order by ts desc limit 10) order by i desc, n desc");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t3.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.3333333333333335\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t2.6666666666666665\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t1.3333333333333333\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t1.5\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t2.0\n",
                    "select ts,i,j,avg from ( " +
                            "select ts, i, j,n, avg(j) over (partition by i order by ts range between 1 preceding and current row) as avg from dups2 limit 10" +
                            ") order by i, n");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t3.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t1.6666666666666667\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t1.5\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t0.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t1.5\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.3333333333333333\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t2.0\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.3333333333333335\n",
                    "select ts,i,j,avg from ( " +
                            "select ts, i, j,n, avg(j) over (partition by i order by ts desc range between 1 preceding and current row) as avg from dups2 order by ts desc limit 10" +
                            ") order by i desc, n desc");

            String dupResult3 = "ts\ti\tj\tavg\n" +
                    "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t4\t3.0\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\t2.3333333333333335\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t3\t2.5\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t0\t2.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t3\t2.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t0\t1.3333333333333333\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t2\t1.5\n" +
                    "1970-01-01T00:00:00.000002Z\t1\t4\t2.0\n";

            assertSql(dupResult3,
                    "select ts,i,j,avg from ( " +
                            "select ts, i, j, n, avg(j) over (partition by i order by ts range between 4 preceding and current row) avg from dups2 order by ts limit 10" +
                            ") order by i, n");

            assertSql(dupResult3,
                    "select ts,i,j,avg from ( " +
                            "select ts, i, j, n, avg(j) over (partition by i order by ts range between unbounded preceding and current row) avg from dups2 order by ts limit 10" +
                            ") order by i, n");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\t2.3333333333333335\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\t2.3333333333333335\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\tNaN\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\t1.5\n",
                    "select ts,i,j,avg from ( " +
                            "select ts, i, j, n, avg(j) over (partition by i order by ts range between unbounded preceding and 1 preceding) avg from dups2 order by ts limit 10" +
                            ") order by i, n");

            String dupResult4 = "ts\ti\tj\tavg\n" +
                    "1970-01-01T00:00:00.000002Z\t1\t4\t4.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t2\t3.0\n" +
                    "1970-01-01T00:00:00.000001Z\t1\t0\t2.0\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t3\t2.25\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t2.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t0\t0.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t3\t1.5\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\t1.3333333333333333\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t4\t2.0\n" +
                    "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\n";
            assertSql(dupResult4,
                    "select ts,i,j,avg from ( " +
                            "select ts, i, j, n, avg(j) over (partition by i order by ts desc range between 4 preceding and current row) avg from dups2 order by ts desc limit 10" +
                            ") order by i desc, n desc");

            assertSql(dupResult4,
                    "select ts,i,j,avg from ( " +
                            "select ts, i, j, n, avg(j) over (partition by i order by ts desc range between unbounded preceding and current row) avg from dups2 order by ts desc limit 10" +
                            ") order by i desc, n desc");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000002Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t2\t4.0\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t0\t4.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t3\t2.0\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t2.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t0\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t3\tNaN\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.5\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t4\t1.5\n" +
                            "1970-01-01T00:00:00.000000Z\t0\t2\t2.0\n",
                    "select ts,i,j,avg from ( " +
                            "select ts, i, j, n, avg(j) over (partition by i order by ts desc range between unbounded preceding and 1 preceding) avg from dups2 order by ts desc limit 10" +
                            ") order by i desc, n desc");

            // table without designated timestamp
            ddl("create table nodts(ts timestamp, i long, j long)");
            insert("insert into nodts select (x/2)::timestamp, x%2, x%5 from long_sequence(10)");

            // timestamp ascending order is declared using timestamp(ts) clause
            assertSql(dupResult,
                    "select ts, i, j, avg(j) over (partition by i order by ts range between 4 preceding and current row) from nodts timestamp(ts)");

            assertSql(dupResult,
                    "select ts, i, j, avg(j) over (partition by i order by ts range between unbounded preceding and current row) from nodts timestamp(ts)");
        });
    }

    @Test
    public void testAverageOverRangeIsOnlySupportedOverDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // table without designated timestamp
            ddl("create table nodts(ts timestamp, i long, j long)");

            assertException("select ts, i, j, avg(j) over (partition by i order by ts range between 4 preceding and current row) from nodts",
                    54, "RANGE is supported only for queries ordered by designated timestamp");

            // while it's possible to declare ascending designated timestamp order, it's not possible to declare descending order
            assertException("select ts, i, j, avg(j) over (partition by i order by ts desc range between 4 preceding and current row) from nodts timestamp(ts)",
                    54, "RANGE is supported only for queries ordered by designated timestamp");

            //table with designated timestamp
            ddl("create table tab (ts timestamp, i long, j long, otherTs timestamp) timestamp(ts) partition by month");

            assertException("select ts, i, j, avg(i) over (partition by i order by j desc range between unbounded preceding and 10 microsecond preceding) " +
                            "from tab order by ts desc",
                    54, "RANGE is supported only for queries ordered by designated timestamp");

            assertException("select ts, i, j, avg(i) over (partition by i order by j range 10 microsecond preceding) from tab",
                    54, "RANGE is supported only for queries ordered by designated timestamp");

            // order by column_number doesn't work with in over clause so 1 is treated as integer constant
            assertException("select ts, i, j, avg(i) over (partition by i order by 1 range 10 microsecond preceding) from tab",
                    54, "RANGE is supported only for queries ordered by designated timestamp");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts+i range 10 microsecond preceding) from tab",
                    56, "RANGE is supported only for queries ordered by designated timestamp");

            assertException("select ts, i, j, avg(i) over (partition by i order by otherTs range 10 microsecond preceding) from tab",
                    54, "RANGE is supported only for queries ordered by designated timestamp");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts range 10 microsecond preceding) from tab timestamp(otherTs)",
                    54, "RANGE is supported only for queries ordered by designated timestamp");

            assertException("select ts, i, j, avg(i) over (partition by i order by otherTs desc range 10 microsecond preceding) from tab timestamp(otherTs)",
                    54, "RANGE is supported only for queries ordered by designated timestamp");
        });
    }

    @Test
    public void testAverageOverRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long, d double) timestamp(ts)");
            insert("insert into tab select x::timestamp, x/4, x%5, x%5 from long_sequence(7)");

            assertSql("ts\ti\tj\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\n",
                    "select ts, i, j from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t2.5\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.8333333333333333\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.8571428571428572\n",
                    "select ts, i, j, avg(d) over (order by ts rows unbounded preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.5\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.4\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.5\n",
                    "select ts, i, j, avg(j) over (order by i, j rows unbounded preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n",
                    "select ts, i, j, avg(d) over (order by ts rows current row) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n",
                    "select ts, i, j, avg(d) over (order by ts desc rows current row) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.5\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t2.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.5\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t2.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.8333333333333333\n",
                    "select ts, i, j, avg(d) over (order by ts rows between unbounded preceding and 1 preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.5\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t3.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.3333333333333335\n",
                    "select ts, i, j, avg(d) over (order by ts rows between 4 preceding and 2 preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.3333333333333335\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.6666666666666667\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.5\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tNaN\n",
                    "select ts, i, j, avg(d) over (order by ts desc rows between 4 preceding and 2 preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.8571428571428572\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.8571428571428572\n",
                    "select ts, i, j, avg(d) over (order by i rows between unbounded preceding and unbounded following) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t2.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t1.75\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t1.75\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.75\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.75\n",
                    "select ts, i, j, avg(d) over (partition by i rows between unbounded preceding and unbounded following) from tab");

            String rowsResult1 = "ts\ti\tj\tavg\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                    "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\n" +
                    "1970-01-01T00:00:00.000006Z\t1\t1\t1.6666666666666667\n" +
                    "1970-01-01T00:00:00.000007Z\t1\t2\t1.75\n";

            assertSql(rowsResult1, "select ts, i, j, avg(d) over (partition by i order by ts rows unbounded preceding) from tab");
            assertSql(rowsResult1, "select ts, i, j, avg(j) over (partition by i order by ts rows unbounded preceding) from tab");
            assertSql(rowsResult1, "select ts, i, j, avg(j) over (partition by i rows unbounded preceding) from tab");
            assertSql(rowsResult1, "select ts, i, j, avg(j) over (partition by i rows between unbounded preceding and current row) from tab");
            assertSql(rowsResult1, "select ts, i, j, avg(j) over (partition by i order by ts rows between 10 preceding and current row) from tab");
            assertSql(rowsResult1, "select ts, i, j, avg(j) over (partition by i order by ts rows between 3 preceding and current row) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.5\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t0.5\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.5\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts rows between 1 preceding and current row)  from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\t1.5\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t2.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\t2.0\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t1.6666666666666667\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t1.0\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts rows between 2 preceding and current row) from tab");

            String result2 = "ts\ti\tj\tavg\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t2\t1.0\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t3\t1.5\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\tNaN\n" +
                    "1970-01-01T00:00:00.000005Z\t1\t0\t4.0\n" +
                    "1970-01-01T00:00:00.000006Z\t1\t1\t2.0\n" +
                    "1970-01-01T00:00:00.000007Z\t1\t2\t0.5\n";
            assertSql(result2, "select ts, i, j, avg(j) over (partition by i order by ts rows between 2 preceding and 1 preceding) from tab");
            assertSql(result2, "select ts, i, j, avg(j) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row) from tab");
            assertSql(result2, "select ts, i, j, avg(j) over (partition by i order by ts rows between 2 preceding and current row exclude current row) from tab");

            //partitions are smaller than 10 elements so avg is all nulls
            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\tNaN\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tNaN\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\tNaN\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts rows between 20 preceding and 10 preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tNaN\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t4.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts rows between unbounded preceding and 2 preceding) from tab");

            assertSql("ts\ti\tj\tavg\n" +
                            "1970-01-01T00:00:00.000001Z\t0\t1\tNaN\n" +
                            "1970-01-01T00:00:00.000002Z\t0\t2\tNaN\n" +
                            "1970-01-01T00:00:00.000003Z\t0\t3\t1.0\n" +
                            "1970-01-01T00:00:00.000004Z\t1\t4\tNaN\n" +
                            "1970-01-01T00:00:00.000005Z\t1\t0\tNaN\n" +
                            "1970-01-01T00:00:00.000006Z\t1\t1\t4.0\n" +
                            "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n",
                    "select ts, i, j, avg(j) over (partition by i order by ts rows between 10000 preceding and 2 preceding) from tab");

            // here avg returns j as double because it processes current row only
            assertSql("ts\ti\tj\tavg\n" +
                    "1970-01-01T00:00:00.000001Z\t0\t1\t1.0\n" +
                    "1970-01-01T00:00:00.000002Z\t0\t2\t2.0\n" +
                    "1970-01-01T00:00:00.000003Z\t0\t3\t3.0\n" +
                    "1970-01-01T00:00:00.000004Z\t1\t4\t4.0\n" +
                    "1970-01-01T00:00:00.000005Z\t1\t0\t0.0\n" +
                    "1970-01-01T00:00:00.000006Z\t1\t1\t1.0\n" +
                    "1970-01-01T00:00:00.000007Z\t1\t2\t2.0\n", "select ts, i, j, avg(j) over (partition by i order by ts rows current row) from tab");

            // test with dependencies not included on column list + column reorder + sort
            assertSql("avg\tts\ti\tj\n" +
                            "1.0\t1970-01-01T00:00:00.000001Z\t0\t1\n" +
                            "1.5\t1970-01-01T00:00:00.000002Z\t0\t2\n" +
                            "2.5\t1970-01-01T00:00:00.000003Z\t0\t3\n" +
                            "4.0\t1970-01-01T00:00:00.000004Z\t1\t4\n" +
                            "2.0\t1970-01-01T00:00:00.000005Z\t1\t0\n" +
                            "0.5\t1970-01-01T00:00:00.000006Z\t1\t1\n" +
                            "1.5\t1970-01-01T00:00:00.000007Z\t1\t2\n",
                    "select avg(j) over (partition by i order by ts rows between 1 preceding and current row), ts, i, j from tab");

            assertSql("avg\ti\tj\n" +
                            "1.0\t0\t1\n" +
                            "1.5\t0\t2\n" +
                            "2.5\t0\t3\n" +
                            "4.0\t1\t4\n" +
                            "2.0\t1\t0\n" +
                            "0.5\t1\t1\n" +
                            "1.5\t1\t2\n",
                    "select avg(j) over (partition by i order by ts rows between 1 preceding and current row), i, j from tab");

            assertSql("avg\n" +
                            "1.5\n" +
                            "2.5\n" +
                            "3.0\n" +
                            "2.0\n" +
                            "0.5\n" +
                            "1.5\n" +
                            "2.0\n",
                    "select avg(j) over (partition by i order by ts desc rows between 1 preceding and current row) from tab");

            assertSql("avg\n" +
                            "1.5\n" +
                            "2.5\n" +
                            "3.0\n" +
                            "2.0\n" +
                            "0.5\n" +
                            "1.5\n" +
                            "2.0\n",
                    "select avg(j) over (partition by i order by ts desc rows between 1 preceding and current row) from tab order by ts");

            assertSql("avg\ti\tj\n" +
                            "1.0\t0\t1\n" +
                            "1.5\t0\t2\n" +
                            "2.5\t0\t3\n" +
                            "0.0\t1\t0\n" +
                            "0.5\t1\t1\n" +
                            "1.5\t1\t2\n" +
                            "3.0\t1\t4\n",
                    "select avg(j) over (partition by i order by j, i  desc rows between 1 preceding and current row), i, j from tab order by i,j");
        });
    }

    @Test
    public void testAverageOverRowsRejectsCurrentRowFrameExcludingCurrentRow() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts rows current row exclude current row) from tab", 82, "end of window is higher than start of window due to exclusion mode");
        });
    }

    @Test
    public void testAverageRejectsFramesThatUseFollowing() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");
            insert("insert into tab select x::timestamp, x/4, x%5 from long_sequence(7)");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts rows between 10 following and 20 following) from tab", 73, "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts rows between 10 preceding and 1 following) from tab", 89, "frame end supports _number_ PRECEDING and CURRENT ROW only");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts rows between 10 preceding and unbounded following) from tab", 97, "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts groups between 10 following and 20 following) from tab", 75, "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts groups between 10 preceding and 1 following) from tab", 91, "frame end supports _number_ PRECEDING and CURRENT ROW only");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts groups between 10 preceding and unbounded following) from tab", 99, "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts range between 10 following and 20 following) from tab", 74, "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts range between 10 preceding and 1 following) from tab", 90, "frame end supports _number_ PRECEDING and CURRENT ROW only");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts range between 10 preceding and unbounded following) from tab", 98, "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING");
        });
    }

    @Test
    public void testAverageResolvesSymbolTables() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table  cpu ( hostname symbol, usage_system double )");
            insert("insert into cpu select rnd_symbol('A', 'B', 'C'), x from long_sequence(1000)");

            assertSql("hostname\tusage_system\tavg\n" +
                            "A\t1.0\t1.0\n" +
                            "A\t2.0\t1.5\n" +
                            "B\t3.0\t3.0\n" +
                            "C\t4.0\t4.0\n" +
                            "C\t5.0\t4.5\n" +
                            "C\t6.0\t5.0\n" +
                            "C\t7.0\t5.5\n" +
                            "B\t8.0\t5.5\n" +
                            "A\t9.0\t4.0\n" +
                            "B\t10.0\t7.0\n",
                    "select hostname, usage_system, avg(usage_system) over(partition by hostname rows between 50 preceding and current row) " +
                            "from cpu limit 10;");
        });
    }

    @Ignore
    @Test
    public void testAvgFailsInNonWindowContext() throws Exception {
        assertException(
                "select avg(price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                7,
                "window function called in non-window context, make sure to add OVER clause"
        );
    }

    @Test
    public void testAvgFunctionDontAcceptFollowingInNonDefaultFrameDefinition() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            assertException("select avg(j) over (partition by i rows between 10 following and 20 following) from tab",
                    51, "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only");
            assertException("select avg(j) over (partition by i rows between current row and 10 following) from tab",
                    67, "frame end supports _number_ PRECEDING and CURRENT ROW only");
        });
    }

    @Test
    public void testPartitionByAndOrderByColumnPushdown() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");
            insert("insert into tab select x::timestamp, x/4, x%5 from long_sequence(7)");

            //row_number()
            assertSql("row_number\n" +
                            "3\n" +
                            "2\n" +
                            "1\n" +
                            "4\n" +
                            "3\n" +
                            "2\n" +
                            "1\n",
                    "select row_number() over (partition by i order by ts desc) " +
                            "from tab order by ts asc");

            assertSql("row_number\n" +
                            "1\n" +
                            "2\n" +
                            "3\n" +
                            "4\n" +
                            "1\n" +
                            "2\n" +
                            "3\n",
                    "select row_number() over (partition by i order by ts desc)" +
                            "from tab order by ts desc");

            assertSql("row_number\n" +
                            "1\n" +
                            "2\n" +
                            "3\n" +
                            "1\n" +
                            "2\n" +
                            "3\n" +
                            "4\n",
                    "select row_number() over (partition by i order by ts asc) " +
                            "from tab order by ts asc");

            assertSql("row_number\n" +
                            "4\n" +
                            "3\n" +
                            "2\n" +
                            "1\n" +
                            "3\n" +
                            "2\n" +
                            "1\n",
                    "select row_number() over (partition by i order by ts asc) " +
                            "from tab order by ts desc");

            assertSql("row_number\n" +
                            "3\n" +
                            "2\n" +
                            "1\n" +
                            "4\n" +
                            "3\n" +
                            "2\n" +
                            "1\n",
                    "select row_number() over (partition by i order by i, j asc) " +
                            "from tab order by ts desc");

            assertPlan("select row_number() over (partition by i order by ts asc), " +
                            "   avg(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   rank() over (partition by i order by j asc) " +
                            "from tab " +
                            "order by ts asc",
                    "SelectedRecord\n" +
                            "    CachedWindow\n" +
                            "      orderedFunctions: [[j] => [rank() over (partition by [i])],[ts desc] => [avg(j) over (partition by [i] rows between unbounded preceding and current row )]]\n" +
                            "      unorderedFunctions: [row_number() over (partition by [i])]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n");

            //avg(), row_number() and rank()
            assertSql("row_number\tavg\trank\n" +
                            "1\t2.0\t1\n" +
                            "2\t2.5\t2\n" +
                            "3\t3.0\t3\n" +
                            "1\t1.75\t4\n" +
                            "2\t1.0\t1\n" +
                            "3\t1.5\t2\n" +
                            "4\t2.0\t3\n",
                    "select row_number() over (partition by i order by ts asc), " +
                            "   avg(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   rank() over (partition by i order by j asc) " +
                            "from tab " +
                            "order by ts asc");

            assertSql("row_number\tavg\trank\n" +
                            "4\t2.0\t3\n" +
                            "3\t1.5\t2\n" +
                            "2\t1.0\t1\n" +
                            "1\t1.75\t4\n" +
                            "3\t3.0\t3\n" +
                            "2\t2.5\t2\n" +
                            "1\t2.0\t1\n",
                    "select row_number() over (partition by i order by ts asc), " +
                            "   avg(j) over (partition by i order by ts desc rows between unbounded preceding and current row)," +
                            "   rank() over (partition by i order by j asc) " +
                            "from tab " +
                            "order by ts desc");
        });
    }

    @Test
    public void testRankFailsInNonWindowContext() throws Exception {
        assertException(
                "select rank(), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                7,
                "window function called in non-window context, make sure to add OVER clause"
        );
    }

    @Test
    public void testRankWithNoPartitionByAndNoOrderByWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "1\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRankWithNoPartitionByAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "3\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "7\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "7\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "3\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "3\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "3\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "7\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "7\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionAndOrderByNonSymbol() throws Exception {
        assertQuery("rank\tprice\tts\n" +
                        "1\t42\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t42\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t42\t1970-01-03T07:33:20.000000Z\n" +
                        "4\t42\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t42\t1970-01-05T15:06:40.000000Z\n" +
                        "6\t42\t1970-01-06T18:53:20.000000Z\n" +
                        "7\t42\t1970-01-07T22:40:00.000000Z\n" +
                        "8\t42\t1970-01-09T02:26:40.000000Z\n" +
                        "9\t42\t1970-01-10T06:13:20.000000Z\n" +
                        "10\t42\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by price order by ts), price, ts from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " 42 price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                false,
                true
        );
    }

    @Test
    public void testRankWithPartitionAndOrderBySymbolNoWildcard() throws Exception {
        assertQuery("rank\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n",
                "select rank() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionAndOrderBySymbolWildcardFirst() throws Exception {
        assertQuery("price\tsymbol\tts\trank\n" +
                        "0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\t1\n" +
                        "0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\t1\n" +
                        "0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\t1\n" +
                        "0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\t1\n" +
                        "0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\t1\n" +
                        "0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\t1\n" +
                        "0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\t1\n" +
                        "0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\t1\n" +
                        "0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\t1\n" +
                        "0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\t1\n",
                "select *, rank() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "1\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndMultiOrderWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "4\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "4\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by symbol, price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndNoOrderWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "2\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "2\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "3\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "4\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "3\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "4\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "2\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                false,
                true
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndOrderByIntPriceDescWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "2\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "2\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "1\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "2\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "2\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "2\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "2\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by price desc), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndOrderByIntPriceWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "4\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "4\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRankWithPartitionBySymbolAndOrderByPriceWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "2\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "6\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "2\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "4\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "3\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "5\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberFailsInNonWindowContext() throws Exception {
        assertException(
                "select row_number(), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                7,
                "window function called in non-window context, make sure to add OVER clause"
        );
    }

    @Test
    public void testRowNumberWithFilter() throws Exception {
        assertQuery("author\tsym\tcommits\trk\n" +
                        "user2\tETH\t3\t2\n" +
                        "user1\tETH\t3\t1\n",
                "with active_devs as (" +
                        "    select author, sym, count() as commits" +
                        "    from dev_stats" +
                        "    where author is not null and author != 'github-actions[bot]'" +
                        "    order by commits desc" +
                        "    limit 100" +
                        "), active_ranked as (" +
                        "    select author, sym, commits, row_number() over (partition by sym order by commits desc) as rk" +
                        "    from active_devs" +
                        ") select * from active_ranked where sym = 'ETH'",
                "create table dev_stats as " +
                        "(" +
                        "select" +
                        " rnd_symbol('ETH','BTC') sym," +
                        " rnd_symbol('user1','user2') author," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndDifferentOrder() throws Exception {
        assertQuery("x\ty\trn\n" +
                        "1\t1\t10\n" +
                        "2\t0\t5\n" +
                        "3\t1\t9\n" +
                        "4\t0\t4\n" +
                        "5\t1\t8\n" +
                        "6\t0\t3\n" +
                        "7\t1\t7\n" +
                        "8\t0\t2\n" +
                        "9\t1\t6\n" +
                        "10\t0\t1\n",
                "select *, row_number() over (order by y asc, x desc) as rn from tab order by x asc",
                "create table tab as (select x, x%2 y from long_sequence(10))",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndNoOrderInSubQuery() throws Exception {
        assertQuery("symbol\trn\n" +
                        "CC\t2\n" +
                        "BB\t3\n" +
                        "CC\t4\n" +
                        "AA\t5\n" +
                        "BB\t6\n" +
                        "CC\t7\n" +
                        "BB\t8\n" +
                        "BB\t9\n" +
                        "BB\t10\n" +
                        "BB\t11\n",
                "select symbol, rn + 1 as rn from (select symbol, row_number() over() as rn from trades)",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                false,
                true
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndNoOrderWildcardLast() throws Exception {
        assertQuery("row_number\tprice\tsymbol\tts\n" +
                        "1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "4\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "6\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "7\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "8\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "9\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "10\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                false,
                true
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery("row_number\tprice\tsymbol\tts\n" +
                        "10\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "7\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "9\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "6\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "8\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "5\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "4\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "3\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "2\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndSameOrderFollowedByBaseFactory() throws Exception {
        assertQuery("ts\ts\trn\n" +
                        "1970-01-01T00:00:00.000000Z\ta\t1\n" +
                        "1970-01-02T03:46:40.000000Z\ta\t2\n" +
                        "1970-01-10T06:13:20.000000Z\ta\t3\n" +
                        "1970-01-03T07:33:20.000000Z\tb\t4\n" +
                        "1970-01-09T02:26:40.000000Z\tb\t5\n" +
                        "1970-01-11T10:00:00.000000Z\tb\t6\n" +
                        "1970-01-04T11:20:00.000000Z\tc\t7\n" +
                        "1970-01-05T15:06:40.000000Z\tc\t8\n" +
                        "1970-01-06T18:53:20.000000Z\tc\t9\n" +
                        "1970-01-07T22:40:00.000000Z\tc\t10\n",
                "select *, row_number() over (order by s) as rn from tab where ts in ('1970-01') order by s",
                "create table tab as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) ts," +
                        " rnd_symbol('a','b','c') s" +
                        " from long_sequence(10)" +
                        "), index(s) timestamp(ts) partition by month",
                null,
                false,
                true
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndSameOrderNotFollowedByBaseFactory() throws Exception {
        assertQuery("ts\ts\trn\n" +
                        "1970-01-10T06:13:20.000000Z\ta\t1\n" +
                        "1970-01-02T03:46:40.000000Z\ta\t2\n" +
                        "1970-01-01T00:00:00.000000Z\ta\t3\n" +
                        "1970-01-11T10:00:00.000000Z\tb\t4\n" +
                        "1970-01-09T02:26:40.000000Z\tb\t5\n" +
                        "1970-01-03T07:33:20.000000Z\tb\t6\n" +
                        "1970-01-07T22:40:00.000000Z\tc\t7\n" +
                        "1970-01-06T18:53:20.000000Z\tc\t8\n" +
                        "1970-01-05T15:06:40.000000Z\tc\t9\n" +
                        "1970-01-04T11:20:00.000000Z\tc\t10\n",
                "select *, row_number() over (order by s) as rn from tab where ts in ('1970-01') order by s",
                "create table tab as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) ts," +
                        " rnd_symbol('a','b','c') s" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by month",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderByNonSymbol() throws Exception {
        assertQuery("row_number\tprice\tts\n" +
                        "1\t42\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t42\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t42\t1970-01-03T07:33:20.000000Z\n" +
                        "4\t42\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t42\t1970-01-05T15:06:40.000000Z\n" +
                        "6\t42\t1970-01-06T18:53:20.000000Z\n" +
                        "7\t42\t1970-01-07T22:40:00.000000Z\n" +
                        "8\t42\t1970-01-09T02:26:40.000000Z\n" +
                        "9\t42\t1970-01-10T06:13:20.000000Z\n" +
                        "10\t42\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (partition by price order by ts), price, ts from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " 42 price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                false,
                true
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolNoWildcard() throws Exception {
        assertQuery("row_number\n" +
                        "3\n" +
                        "6\n" +
                        "2\n" +
                        "1\n" +
                        "5\n" +
                        "1\n" +
                        "4\n" +
                        "3\n" +
                        "2\n" +
                        "1\n",
                "select row_number() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolWildcardFirst() throws Exception {
        assertQuery("price\tsymbol\tts\trow_number\n" +
                        "0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\t3\n" +
                        "0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\t6\n" +
                        "0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\t2\n" +
                        "0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\t1\n" +
                        "0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\t5\n" +
                        "0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\t1\n" +
                        "0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\t4\n" +
                        "0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\t3\n" +
                        "0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\t2\n" +
                        "0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\t1\n",
                "select *, row_number() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery("row_number\tprice\tsymbol\tts\n" +
                        "3\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "6\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "2\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "4\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "3\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "2\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (partition by symbol order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                false
        );
    }

    @Test
    public void testWindowBufferExceedsLimit() throws Exception {
        configOverrideSqlWindowStorePageSize(4096);
        configOverrideSqlWindowStoreMaxPages(10);

        try {
            assertMemoryLeak(() -> {
                ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");
                insert("insert into tab select x::timestamp, 1, x from long_sequence(100000)");

                //TODO: improve error message and position
                assertException("select avg(j) over (partition by i rows between 100001 preceding and current row) from tab",
                        0, "Maximum number of pages (10) breached in VirtualMemory");
            });
        } finally {
            //disable
            configOverrideSqlWindowStorePageSize(0);
            configOverrideSqlWindowStoreMaxPages(0);
        }
    }

    @Test
    public void testWindowContextCleanup() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades as " +
                    "(" +
                    "select" +
                    " rnd_int(1,2,3) price," +
                    " rnd_symbol('AA','BB','CC') symbol," +
                    " timestamp_sequence(0, 100000000000) ts" +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by day", sqlExecutionContext);

            final String query = "select symbol, price, row_number() over (partition by symbol order by price) from trades";
            final String expected = "symbol\tprice\trow_number\n" +
                    "BB\t1\t1\n" +
                    "CC\t2\t2\n" +
                    "AA\t2\t1\n" +
                    "CC\t1\t1\n" +
                    "BB\t2\t2\n";
            assertSql(expected, query);

            // WindowContext should be properly clean up when we try to execute the next query.

            try {
                ddl("select row_number() from trades", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(7, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "window function called in non-window context, make sure to add OVER clause");
            }
        });
    }

    @Test
    public void testWindowFunctionDoesSortIfOrderByIsNotCompatibleWithBaseQuery() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long, sym symbol index) timestamp(ts)");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts desc rows between 1 preceding and current row) from tab",
                    "CachedWindow\n" +
                            "  orderedFunctions: [[ts desc] => [avg(1) over (partition by [i] rows between 1 preceding and current row)]]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts asc rows between 1 preceding and current row)  from tab order by ts desc",
                    "CachedWindow\n" +
                            "  orderedFunctions: [[ts] => [avg(1) over (partition by [i] rows between 1 preceding and current row)]]\n" +
                            "    DataFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts asc rows between 1 preceding and current row)  from tab where sym in ( 'A', 'B') ",
                    "CachedWindow\n" +
                            "  orderedFunctions: [[ts] => [avg(1) over (partition by [i] rows between 1 preceding and current row)]]\n" +
                            "    FilterOnValues symbolOrder: desc\n" +
                            "        Cursor-order scan\n" +
                            "            Index forward scan on: sym deferred: true\n" +
                            "              filter: sym='A'\n" +
                            "            Index forward scan on: sym deferred: true\n" +
                            "              filter: sym='B'\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts desc rows between 1 preceding and current row)  from tab where sym = 'A'",
                    "CachedWindow\n" +
                            "  orderedFunctions: [[ts desc] => [avg(1) over (partition by [i] rows between 1 preceding and current row)]]\n" +
                            "    DeferredSingleSymbolFilterDataFrame\n" +
                            "        Index forward scan on: sym deferred: true\n" +
                            "          filter: sym='A'\n" +
                            "        Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testWindowFunctionDoesntSortIfOrderByIsCompatibleWithBaseQuery() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long, sym symbol index) timestamp(ts)");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts rows between 1 preceding and current row)  from tab",
                    "Window\n" +
                            "  functions: [avg(1) over (partition by [i] rows between 1 preceding and current row)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts rows between 1 preceding and current row)  from tab order by ts asc",
                    "Window\n" +
                            "  functions: [avg(1) over (partition by [i] rows between 1 preceding and current row)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts desc rows between 1 preceding and current row)  from tab order by ts desc",
                    "Window\n" +
                            "  functions: [avg(1) over (partition by [i] rows between 1 preceding and current row)]\n" +
                            "    DataFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts asc rows between 1 preceding and current row)  from tab where sym = 'A'",
                    "Window\n" +
                            "  functions: [avg(1) over (partition by [i] rows between 1 preceding and current row)]\n" +
                            "    DeferredSingleSymbolFilterDataFrame\n" +
                            "        Index forward scan on: sym deferred: true\n" +
                            "          filter: sym='A'\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts asc rows between 1 preceding and current row) " +
                            "from tab where sym in ( 'A', 'B') order by ts asc",
                    "Window\n" +
                            "  functions: [avg(1) over (partition by [i] rows between 1 preceding and current row)]\n" +
                            "    FilterOnValues\n" +
                            "        Table-order scan\n" +
                            "            Index forward scan on: sym deferred: true\n" +
                            "              filter: sym='A'\n" +
                            "            Index forward scan on: sym deferred: true\n" +
                            "              filter: sym='B'\n" +
                            "        Frame forward scan on: tab\n");
        });
    }
}
