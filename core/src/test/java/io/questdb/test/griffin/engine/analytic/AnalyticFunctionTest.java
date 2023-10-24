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

package io.questdb.test.griffin.engine.analytic;

import io.questdb.test.AbstractCairoTest;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class AnalyticFunctionTest extends AbstractCairoTest {

    @Test
    public void testAnalyticContextCleanup() throws Exception {
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

            // AnalyticContext should be properly clean up when we try to execute the next query.

            try {
                ddl("select row_number() from trades", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(7, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "analytic function called in non-analytic context, make sure to add OVER clause");
            }
        });
    }

    @Test
    public void testAnalyticFunctionDoesSortIfOrderByIsNotCompatibleWithBaseQuery() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long, sym symbol index) timestamp(ts)");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts desc rows between 1 preceding and current row) from tab",
                    "CachedAnalytic\n" +
                            "  orderedFunctions: [[ts desc] => [avg(1) over (partition by [i])]]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts asc rows between 1 preceding and current row)  from tab order by ts desc",
                    "CachedAnalytic\n" +
                            "  orderedFunctions: [[ts] => [avg(1) over (partition by [i])]]\n" +
                            "    DataFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts asc rows between 1 preceding and current row)  from tab where sym in ( 'A', 'B') ",
                    "CachedAnalytic\n" +
                            "  orderedFunctions: [[ts] => [avg(1) over (partition by [i])]]\n" +
                            "    FilterOnValues symbolOrder: desc\n" +
                            "        Cursor-order scan\n" +
                            "            Index forward scan on: sym deferred: true\n" +
                            "              filter: sym='A'\n" +
                            "            Index forward scan on: sym deferred: true\n" +
                            "              filter: sym='B'\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts desc rows between 1 preceding and current row)  from tab where sym = 'A'",
                    "CachedAnalytic\n" +
                            "  orderedFunctions: [[ts desc] => [avg(1) over (partition by [i])]]\n" +
                            "    DeferredSingleSymbolFilterDataFrame\n" +
                            "        Index forward scan on: sym deferred: true\n" +
                            "          filter: sym='A'\n" +
                            "        Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testAnalyticFunctionDoesntSortIfOrderByIsCompatibleWithBaseQuery() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long, sym symbol index) timestamp(ts)");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts rows between 1 preceding and current row)  from tab",
                    "Analytic\n" +
                            "  functions: [avg(1) over (partition by [i])]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts rows between 1 preceding and current row)  from tab order by ts asc",
                    "Analytic\n" +
                            "  functions: [avg(1) over (partition by [i])]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts desc rows between 1 preceding and current row)  from tab order by ts desc",
                    "Analytic\n" +
                            "  functions: [avg(1) over (partition by [i])]\n" +
                            "    DataFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts asc rows between 1 preceding and current row)  from tab where sym = 'A'",
                    "Analytic\n" +
                            "  functions: [avg(1) over (partition by [i])]\n" +
                            "    DeferredSingleSymbolFilterDataFrame\n" +
                            "        Index forward scan on: sym deferred: true\n" +
                            "          filter: sym='A'\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, avg(1) over (partition by i order by ts asc rows between 1 preceding and current row) " +
                            "from tab where sym in ( 'A', 'B') order by ts asc",
                    "Analytic\n" +
                            "  functions: [avg(1) over (partition by [i])]\n" +
                            "    FilterOnValues\n" +
                            "        Table-order scan\n" +
                            "            Index forward scan on: sym deferred: true\n" +
                            "              filter: sym='A'\n" +
                            "            Index forward scan on: sym deferred: true\n" +
                            "              filter: sym='B'\n" +
                            "        Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testAverageOverGroups() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");
            insert("insert into tab select x::timestamp, x/4, x%5 from long_sequence(7)");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts groups unbounded preceding) from tab", 17, "function not implemented for given window paramters");
        });
    }

    @Test
    public void testAverageOverRange() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");
            insert("insert into tab select x::timestamp, x/4, x%5 from long_sequence(7)");


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
                    "1970-01-01T00:00:00.000007Z\t1\t2\t1.75\n", "select ts, i, j, avg(j) over (partition by i) from tab");


            assertException("select ts, i, j, avg(i) over (partition by i order by ts range unbounded preceding) from tab", 17, "function not implemented for given window paramters");
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

            assertException("select ts, i, j, avg(i) over (partition by i order by ts rows between 10 following and 20 following) from tab", 73, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts rows between 10 preceding and 1 following) from tab", 89, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts rows between 10 preceding and unbounded following) from tab", 97, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts groups between 10 following and 20 following) from tab", 75, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts groups between 10 preceding and 1 following) from tab", 91, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts groups between 10 preceding and unbounded following) from tab", 99, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");

            assertException("select ts, i, j, avg(i) over (partition by i order by ts range between 10 following and 20 following) from tab", 74, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts range between 10 preceding and 1 following) from tab", 90, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");
            assertException("select ts, i, j, avg(i) over (partition by i order by ts range between 10 preceding and unbounded following) from tab", 98, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");
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
    public void testAvgFailsInNonAnalyticContext() throws Exception {
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
                "analytic function called in non-analytic context, make sure to add OVER clause"
        );
    }

    @Test
    public void testAvgFunctionDontAcceptFollowingInNonDefaultFrameDefinition() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            assertException("select avg(j) over (partition by i rows between 10 following and 20 following) from tab", 51, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");
            assertException("select avg(j) over (partition by i rows between current row and 10 following) from tab", 67, "frame boundaries that use FOLLOWING other than UNBOUNDED are not supported");
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
                            "3\n" +
                            "2\n" +
                            "1\n" +
                            "4\n" +
                            "3\n" +
                            "2\n" +
                            "1\n",
                    "select row_number() over (partition by j order by i desc), i " +
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
                            "    CachedAnalytic\n" +
                            "      orderedFunctions: [[j] => [rank() over (partition by [i])],[ts desc] => [avg(j) over (partition by [i])]]\n" +
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
    public void testRankFailsInNonAnalyticContext() throws Exception {
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
                "analytic function called in non-analytic context, make sure to add OVER clause"
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
    public void testRowNumberFailsInNonAnalyticContext() throws Exception {
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
                "analytic function called in non-analytic context, make sure to add OVER clause"
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
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            assertException("select avg(j) over (partition by i rows between 10001 preceding and current row) from tab",
                    7, "window buffer size exceeds configured limit [maxSize=10000,actual=10001]");
        });
    }
}
