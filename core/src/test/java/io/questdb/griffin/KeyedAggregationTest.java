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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.groupby.vect.GroupByJob;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.*;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.*;

public class KeyedAggregationTest extends AbstractGriffinTest {

    @Test
    public void testAggByHourReoptimization() throws Exception {
        assertQuery("hour\tsum\n" +
                        "14\t2.0\n",
                "SELECT hour(ts), sum(amount)\n" +
                        "FROM trades\n" +
                        "WHERE symbol = 'BTC-USD' " +
                        "AND ts > dateadd('d', -1, to_timestamp('2018-01-02T01:30:40', 'yyyy-MM-ddTHH:mm:ss') )\n" +
                        "ORDER BY hour ASC",
                "CREATE TABLE trades AS " +
                        "(select 'BTC-USD'::symbol symbol, 'Buy'::symbol side, 1.0 price, 2.0 amount, to_timestamp('2018-01-01T14:11:40', 'yyyy-MM-ddTHH:mm:ss') as ts " +
                        " from long_sequence(1) ) " +
                        " timestamp(ts) ",
                null,
                true,
                true,
                true);
    }

    @Test
    public void testCountAggregationWithConst() throws Exception {
        try (TableModel tt1 = new TableModel(configuration, "tt1", PartitionBy.DAY)) {
            tt1.col("tts", ColumnType.LONG).timestamp("ts");
            createPopulateTable(tt1, 100, "2020-01-01", 2);
        }

        String expected = "ts\tcount\n" +
                "2020-01-01T00:28:47.990000Z:TIMESTAMP\t51:LONG\n" +
                "2020-01-02T00:28:47.990000Z:TIMESTAMP\t49:LONG\n";

        String sql = "select ts, count() from tt1 SAMPLE BY d";

        assertSqlWithTypes(sql, expected);
    }

    @Test
    public void testCountAggregations() throws Exception {
        try (TableModel tt1 = new TableModel(configuration, "tt1", PartitionBy.NONE)) {
            tt1.col("tts", ColumnType.LONG);
            CairoTestUtils.create(tt1);
        }

        String expected = "max\tcount\n" +
                "NaN:LONG\t0:LONG\n";
        String sql = "select max(tts), count() from tt1";

        assertSqlWithTypes(sql, expected);
        assertSqlWithTypes(sql + " where now() > '1000-01-01'", expected);

        expected = "count\n" +
                "0:LONG\n";
        sql = "select count() from tt1";
        assertSqlWithTypes(sql, expected);
        assertSqlWithTypes(sql + " where now() > '1000-01-01'", expected);
    }

    @Test
    public void testCountAggregationsWithTypes() throws Exception {
        String[] aggregateFunctions = {"count_distinct"};
        TypeVal[] aggregateColTypes = {
                new TypeVal(ColumnType.STRING, "0:LONG"),
                new TypeVal(ColumnType.SYMBOL, "0:LONG"),
                new TypeVal(ColumnType.LONG256, "0:LONG"),
        };

        testAggregations(aggregateFunctions, aggregateColTypes);
    }

    @Test
    public void testCountCaseInsensitive() throws Exception {
        try (TableModel tt1 = new TableModel(configuration, "tt1", PartitionBy.DAY)) {
            tt1.col("tts", ColumnType.LONG).timestamp("ts")
                    .col("ID", ColumnType.LONG);
            createPopulateTable(tt1, 100, "2020-01-01", 2);
        }

        String expected = "ts\tcount\n" +
                "2020-01-01T00:28:47.990000Z:TIMESTAMP\t1:LONG\n" +
                "2020-01-01T00:57:35.980000Z:TIMESTAMP\t1:LONG\n";

        String sql = "select ts, count() from tt1 WHERE id > 0 LIMIT 2";

        assertSqlWithTypes(sql, expected);
    }

    @Test
    public void testFirstLastAggregations() throws Exception {
        String[] aggregateFunctions = {"first", "last"};
        TypeVal[] aggregateColTypes = {
                new TypeVal(ColumnType.SYMBOL, ":SYMBOL"),
                new TypeVal(ColumnType.BYTE, "0:BYTE"),
                new TypeVal(ColumnType.CHAR, ":CHAR"),
                new TypeVal(ColumnType.SHORT, "0:SHORT"),
                new TypeVal(ColumnType.INT, "NaN:INT"),
                new TypeVal(ColumnType.LONG, "NaN:LONG"),
                new TypeVal(ColumnType.DATE, ":DATE"),
                new TypeVal(ColumnType.TIMESTAMP, ":TIMESTAMP"),
                new TypeVal(ColumnType.FLOAT, "NaN:FLOAT"),
                new TypeVal(ColumnType.DOUBLE, "NaN:DOUBLE"),
                new TypeVal(ColumnType.getGeoHashTypeWithBits(3), ":GEOHASH(3b)"),
                new TypeVal(ColumnType.getGeoHashTypeWithBits(10), ":GEOHASH(2c)"),
                new TypeVal(ColumnType.getGeoHashTypeWithBits(20), ":GEOHASH(4c)"),
                new TypeVal(ColumnType.getGeoHashTypeWithBits(60), ":GEOHASH(12c)")};

        testAggregations(aggregateFunctions, aggregateColTypes);
    }

    @Test
    public void testFirstLastAggregationsNotSupported() {
        String[] aggregateFunctions = {"first"};
        TypeVal[] aggregateColTypes = {
                new TypeVal(ColumnType.BINARY, ":BINARY"),};

        try {
            testAggregations(aggregateFunctions, aggregateColTypes);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "unexpected argument for function: first");
        }
    }

    @Test
    public void testGroupByWithIndexedSymbolKey1() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE records (\n" +
                    "  ts TIMESTAMP,\n" +
                    "  account_uuid SYMBOL INDEX,\n" +
                    "  requests LONG\n" +
                    ") timestamp (ts)");

            compile("insert into records select dateadd('m',x::int,'2023-02-01T00:00:00.000000'), 's' || x/100, x/100 from long_sequence(399)");

            String query = "select account_uuid, " +
                    "     sum(requests) request_count " +
                    "from records " +
                    "where ts > '2023-02-01' " +
                    "    and ts < '2023-02-02' " +
                    "order by 1 asc";

            assertPlan(query,
                    "Sort light\n" +
                            "  keys: [account_uuid]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      keys: [account_uuid]\n" +
                            "      values: [sum(requests)]\n" +
                            "        SortedSymbolIndex\n" +
                            "            Index forward scan on: account_uuid\n" +
                            "              symbolOrder: asc\n" +
                            "            Interval forward scan on: records\n" +
                            "              intervals: [static=[1675209600000001,1675295999999999]\n");

            assertQuery("account_uuid\trequest_count\n" +
                    "s0\t0\n" +
                    "s1\t100\n" +
                    "s2\t200\n" +
                    "s3\t300\n", query, null, true, true, false);
        });
    }

    @Test
    public void testHourDouble() throws Exception {
        assertQuery(
                "hour\tsum\tksum\tnsum\tmin\tmax\tavg\tmax1\tmin1\n" +
                        "0\t15104.996921874175\t15104.996921874172\t15104.996921874175\t1.8362081935174857E-5\t0.999916269120484\t0.5030304023536092\t1970-01-01T00:59:59.900000Z\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t15097.837814466568\t15097.837814466722\t15097.837814466713\t3.921217994906634E-5\t0.9999575311567217\t0.50183938223256\t1970-01-01T01:59:59.900000Z\t1970-01-01T01:00:00.000000Z\n" +
                        "2\t11641.765471468498\t11641.76547146845\t11641.765471468458\t1.8566421983501336E-5\t0.9999768905891359\t0.500376750256533\t1970-01-01T02:46:39.900000Z\t1970-01-01T02:00:00.000000Z\n",
                "select hour(ts), sum(val), ksum(val), nsum(val), min(val), max(val), avg(val), max(ts), min(ts) from tab order by 1",
                "create table tab as (select timestamp_sequence(0, 100000) ts, rnd_double(2) val from long_sequence(100000))",
                null, true, true, true
        );
    }

    @Test
    public void testHourFiltered() throws Exception {
        assertQuery("hour\tcount\n" +
                        "0\t17902\n" +
                        "1\t17892\n" +
                        "2\t14056\n",
                "select hour(ts), count() from tab where val < 0.5",
                "create table tab as (select timestamp_sequence(0, 100000) ts, rnd_double() val from long_sequence(100000))",
                null, true, true, true
        );
    }

    @Test
    public void testHourFilteredJoin() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "hour\tcount\n" +
                    "0\t122\n";

            compiler.compile("create table x as (select timestamp_sequence(0, 1000000) ts, rnd_int(0,100,0) val from long_sequence(100))", sqlExecutionContext);
            compiler.compile("create table y as (select timestamp_sequence(0, 1000000) ts, rnd_int(0,100,0) val from long_sequence(200))", sqlExecutionContext);
            compiler.compile("create table z as (select timestamp_sequence(0, 1000000) ts, rnd_int(0,100,0) val from long_sequence(300))", sqlExecutionContext);

            assertQuery(expected, "select hour(ts), count from " +
                    "(select z.ts, z.val from x join y on y.val = x.val join z on (val) where x.val > 50)" +
                    " where val > 70 order by 1", null, true, true);
        });
    }

    @Test
    public void testHourFilteredUnion() throws Exception {
        assertQuery("hour\tcount\n" +
                        "0\t7\n" +
                        "1\t2\n" +
                        "2\t4\n" +
                        "3\t1\n" +
                        "4\t3\n" +
                        "5\t4\n" +
                        "6\t1\n" +
                        "7\t4\n" +
                        "8\t3\n" +
                        "9\t3\n" +
                        "10\t4\n" +
                        "11\t2\n" +
                        "12\t1\n" +
                        "13\t3\n" +
                        "14\t5\n" +
                        "15\t4\n" +
                        "16\t3\n" +
                        "18\t3\n" +
                        "19\t3\n" +
                        "20\t5\n" +
                        "21\t4\n" +
                        "22\t1\n" +
                        "23\t2\n",

                "select hour(ts), count from " +
                        "(select * from tab where ts in '1970-01-01' union all  select * from tab where ts in '1970-04-26')" +
                        "where val < 0.5 order by 1",
                "create table tab as (select timestamp_sequence(0, 1000000000) ts, rnd_double() val from long_sequence(100000))",
                null, true, true, true
        );
    }

    @Test
    public void testHourInt() throws Exception {
        assertQuery(
                "hour\tcount\tsum\tmin\tmax\tavg\n" +
                        "0\t36000\t13332495967\t-995\t889975\t445441.0466406067\n" +
                        "1\t36000\t13360114022\t-950\t889928\t444359.54307190847\n" +
                        "2\t28000\t10420189893\t-914\t889980\t444528.3858623779\n",
                "select hour(ts), count(), sum(val), min(val), max(val), avg(val) from tab order by 1",
                "create table tab as (select timestamp_sequence(0, 100000) ts, rnd_int(-998, 889991, 2) val from long_sequence(100000))",
                null, true, true, true
        );
    }

    @Test
    public void testHourLong() throws Exception {
        assertQuery(
                "hour\tcount\tsum\tmin\tmax\tavg\n" +
                        "0\t36000\t13265789485\t-988\t889951\t443212.3712872941\n" +
                        "1\t36000\t13359838134\t-997\t889948\t444350.36699261627\n" +
                        "2\t28000\t10444993989\t-992\t889982\t445586.53594129946\n",
                "select hour(ts), count(), sum(val), min(val), max(val), avg(val) from tab order by 1",
                "create table tab as (select timestamp_sequence(0, 100000) ts, rnd_long(-998, 889991, 2) val from long_sequence(100000))",
                null, true, true, true
        );
    }

    @Test
    public void testHourLong256() throws Exception {
        assertQuery(
                "hour\tcount\tsum\n" +
                        "0\t36000\t0x464fffffffffffff7360\n" +
                        "1\t36000\t0x464fffffffffffff7360\n" +
                        "2\t28000\t0x36afffffffffffff92a0\n",
                "select hour(ts), count(), sum(val) from tab order by 1",
                "create table tab as (select timestamp_sequence(0, 100000) ts, cast(9223372036854775807 as long256) val from long_sequence(100000))",
                null, true, true, true
        );
    }

    @Test
    public void testHourLongMissingFunctions() throws Exception {
        assertQuery(
                "hour\tksum\tnsum\n" +
                        "0\t1.3265789485E10\t1.3265789485E10\n" +
                        "1\t1.3359838134E10\t1.3359838134E10\n" +
                        "2\t1.0444993989E10\t1.0444993989E10\n",
                "select hour(ts), ksum(val), nsum(val) from tab order by 1",
                "create table tab as (select timestamp_sequence(0, 100000) ts, rnd_long(-998, 889991, 2) val from long_sequence(100000))",
                null, true, true, true
        );
    }

    @Test
    public void testHourPossibleBugfix() throws Exception {
        assertQuery(
                "hour\tsum\tksum\tnsum\tmin\tmax\tavg\n" +
                        "0\t13265789485\t1.3265789485E10\t1.3265789485E10\t-988\t889951\t443212.3712872941\n" +
                        "1\t13359838134\t1.3359838134E10\t1.3359838134E10\t-997\t889948\t444350.36699261627\n" +
                        "2\t10444993989\t1.0444993989E10\t1.0444993989E10\t-992\t889982\t445586.53594129946\n",
                "select hour(ts), sum(val), ksum(val), nsum(val), min(val), max(val), avg(val) from tab order by 1",
                "create table tab as (select timestamp_sequence(0, 100000) ts, rnd_long(-998, 889991, 2) val from long_sequence(100000))",
                null, true, true, true
        );
    }

    @Test
    public void testIntSymbolAddBothMidTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column s2 symbol", sqlExecutionContext);
            compile("alter table tab add column val double", sqlExecutionContext);
            compiler.compile("insert into tab select null, rnd_symbol('a1','a2','a3', null), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);

            assertSql(
                    "select s2, sum(val) from tab order by s2",
                    "s2\tsum\n" +
                            "\t104083.77969067449\n" +
                            "a1\t103982.62399952614\n" +
                            "a2\t104702.89752880299\n" +
                            "a3\t104299.02298329721\n"
            );
        });
    }

    @Test
    public void testIntSymbolAddKeyMidTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1, rnd_double(2) val from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column s2 symbol cache", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('s1','s2','s3', null), rnd_double(2), rnd_symbol('a1','a2','a3', null) s2 from long_sequence(1000000)", sqlExecutionContext);

            try (
                    RecordCursorFactory factory = compiler.compile("select s2, sum(val) from tab order by s2", sqlExecutionContext).getRecordCursorFactory()
            ) {
                Record[] expected = new Record[]{
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 520447.6629968713;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return null;
                            }
                        },
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 104308.65839619507;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return "a1";
                            }
                        },
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 104559.2867475151;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return "a2";
                            }
                        },
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 104044.11326997809;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return "a3";
                            }
                        },
                };
                assertCursorRawRecords(expected, factory, false);
            }
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableAvgDate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val date", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(-200, 100000, 2) from long_sequence(1000000)", sqlExecutionContext);
            String expected = "s1\tavg\n" +
                    "\t49882.75926752372\n" +
                    "a1\t49866.12261939713\n" +
                    "a2\t49846.02279713851\n" +
                    "a3\t49881.23256562667\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, avg(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableAvgDouble() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val double", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tavg\n" +
                    "\t0.5004990367891637\n" +
                    "a1\t0.5000679244171367\n" +
                    "a2\t0.5009444360765845\n" +
                    "a3\t0.5009102098429884\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";
            assertSql(
                    "select s1, avg(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableAvgInt() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val int", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_int(0, 100000, 2) from long_sequence(1000000)", sqlExecutionContext);
            String expected = "s1\tavg\n" +
                    "\t49985.893055775494\n" +
                    "a1\t50088.55552935175\n" +
                    "a2\t49983.07087654782\n" +
                    "a3\t50056.666352728615\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, avg(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableAvgLong() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val long", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(-200, 100000, 2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tavg\n" +
                    "\t49882.75926752372\n" +
                    "a1\t49866.12261939713\n" +
                    "a2\t49846.02279713851\n" +
                    "a3\t49881.23256562667\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";
            assertSql(
                    "select s1, avg(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableAvgTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val timestamp", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(-200, 100000, 2) from long_sequence(1000000)", sqlExecutionContext);
            String expected = "s1\tavg\n" +
                    "\t49882.75926752372\n" +
                    "a1\t49866.12261939713\n" +
                    "a2\t49846.02279713851\n" +
                    "a3\t49881.23256562667\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, avg(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableCount() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val long", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(33, 889992, 2) from long_sequence(1000000)", sqlExecutionContext);
            String expected = "s1\tcount\n" +
                    "\t500194\n" +
                    "a1\t248976\n" +
                    "a2\t250638\n" +
                    "a3\t250099\n" +
                    "s1\t249898\n" +
                    "s2\t250010\n" +
                    "s3\t250185\n";

            assertSql(
                    "select s1, count() from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableKSumDouble() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val double", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);
            String expected = "s1\tksum\n" +
                    "\t104083.7796906751\n" +
                    "a1\t103982.62399952601\n" +
                    "a2\t104702.89752880314\n" +
                    "a3\t104299.02298329599\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, ksum(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableMaxDate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val date", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(33, 889992, 2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tmax\n" +
                    "\t1970-01-01T00:14:49.988Z\n" +
                    "a1\t1970-01-01T00:14:49.992Z\n" +
                    "a2\t1970-01-01T00:14:49.982Z\n" +
                    "a3\t1970-01-01T00:14:49.988Z\n" +
                    "s1\t\n" +
                    "s2\t\n" +
                    "s3\t\n";
            assertSql(
                    "select s1, max(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableMaxDouble() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val double", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);
            String expected = "s1\tmax\n" +
                    "\t0.9999983440839832\n" +
                    "a1\t0.9999894690287568\n" +
                    "a2\t0.9999985075169716\n" +
                    "a3\t0.9999835673064604\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, max(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableMaxInt() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val int", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_int(33, 889992, 2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tmax\n" +
                    "\t889990\n" +
                    "a1\t889991\n" +
                    "a2\t889988\n" +
                    "a3\t889992\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, max(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableMaxLong() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val long", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(33, 889992, 2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tmax\n" +
                    "\t889988\n" +
                    "a1\t889992\n" +
                    "a2\t889982\n" +
                    "a3\t889988\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, max(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableMaxTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val timestamp", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(33, 889992, 2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tmax\n" +
                    "\t1970-01-01T00:00:00.889988Z\n" +
                    "a1\t1970-01-01T00:00:00.889992Z\n" +
                    "a2\t1970-01-01T00:00:00.889982Z\n" +
                    "a3\t1970-01-01T00:00:00.889988Z\n" +
                    "s1\t\n" +
                    "s2\t\n" +
                    "s3\t\n";
            assertSql(
                    "select s1, max(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableMinDouble() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val double", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tmin\n" +
                    "\t3.2007200990724627E-6\n" +
                    "a1\t1.400472531098984E-5\n" +
                    "a2\t1.0686711945373517E-6\n" +
                    "a3\t8.125933586233813E-6\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, min(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableMinInt() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val int", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_int(33, 889992, 2) from long_sequence(1000000)", sqlExecutionContext);
            String expected = "s1\tmin\n" +
                    "\t33\n" +
                    "a1\t33\n" +
                    "a2\t40\n" +
                    "a3\t34\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, min(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableMinLong() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val long", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(33, 889992, 2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tmin\n" +
                    "\t36\n" +
                    "a1\t35\n" +
                    "a2\t39\n" +
                    "a3\t39\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, min(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableNSumDouble() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val double", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);
            String expected = "s1\tnsum\n" +
                    "\t104083.77969067496\n" +
                    "a1\t103982.62399952546\n" +
                    "a2\t104702.89752880397\n" +
                    "a3\t104299.02298329656\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";


            assertSql(
                    "select s1, nsum(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableSumDate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val date", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(0, 100000, 2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tsum\n" +
                    "\t1970-05-01T15:06:23.318Z\n" +
                    "a1\t1970-05-01T04:03:16.338Z\n" +
                    "a2\t1970-05-01T17:13:47.313Z\n" +
                    "a3\t1970-05-01T06:34:46.269Z\n" +
                    "s1\t\n" +
                    "s2\t\n" +
                    "s3\t\n";
            assertSql(
                    "select s1, sum(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableSumDouble() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val double", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tsum\n" +
                    "\t104083.77969067449\n" +
                    "a1\t103982.62399952614\n" +
                    "a2\t104702.89752880299\n" +
                    "a3\t104299.02298329721\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";
            assertSql(
                    "select s1, sum(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableSumInt() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val int", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_int(-100, 100, 2) from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s1\tsum\n" +
                    "\t-2472\n" +
                    "a1\t-5133\n" +
                    "a2\t-18204\n" +
                    "a3\t175\n" +
                    "s1\tNaN\n" +
                    "s2\tNaN\n" +
                    "s3\tNaN\n";

            assertSql(
                    "select s1, sum(val) from tab order by s1",
                    expected
            );
        });
    }

    @Test
    public void testIntSymbolAddValueMidTableSumLong() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compile("alter table tab add column val long", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_long(0, 100000, 2) from long_sequence(1000000)", sqlExecutionContext);

            assertSql(
                    "select s1, sum(val) from tab order by s1",
                    "s1\tsum\n" +
                            "\t10422383318\n" +
                            "a1\t10382596338\n" +
                            "a2\t10430027313\n" +
                            "a3\t10391686269\n" +
                            "s1\tNaN\n" +
                            "s2\tNaN\n" +
                            "s3\tNaN\n"
            );
        });
    }

    @Test
    public void testIntSymbolResolution() throws Exception {
        assertQuery(
                "s2\tsum\n" +
                        "\t104119.880948161\n" +
                        "a1\t103804.62242300605\n" +
                        "a2\t104433.68659571148\n" +
                        "a3\t104341.28852517322\n",
                "select s2, sum(val) from tab order by s2",
                "create table tab as (select rnd_symbol('s1','s2','s3', null) s1, rnd_symbol('a1','a2','a3', null) s2, rnd_double(2) val from long_sequence(1000000))",
                null, true, true, true
        );
    }

    @Test
    public void testIntSymbolSumAddKeyPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1, rnd_double(2) val, timestamp_sequence(0, 1000000) t from long_sequence(1000000)) timestamp(t) partition by DAY", sqlExecutionContext);
            compile("alter table tab add column s2 symbol cache", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('s1','s2','s3', null), rnd_double(2), timestamp_sequence(cast('1970-01-13T00:00:00.000000Z' as timestamp), 1000000), rnd_symbol('a1','a2','a3', null) s2 from long_sequence(1000000)", sqlExecutionContext);

            String expected = "s2\tsum\n" +
                    "\t520447.6629969\n" +
                    "a1\t104308.6583962\n" +
                    "a2\t104559.2867475\n" +
                    "a3\t104044.11327\n";

            // test with key falling within null columns
            assertSql("select s2, round(sum(val), 7) sum from tab order by s2", expected);
        });
    }

    @Test
    public void testIntSymbolSumAddKeyTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1, rnd_double(2) val, timestamp_sequence(0, 1000000) t from long_sequence(1000000)) timestamp(t) partition by DAY", sqlExecutionContext);
            compile("alter table tab add column s2 symbol cache", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('s1','s2','s3', null), rnd_double(2), timestamp_sequence(cast('1970-01-13T00:00:00.000000Z' as timestamp), 1000000), rnd_symbol('a1','a2','a3', null) s2 from long_sequence(1000000)", sqlExecutionContext);

            // test with key falling within null columns
            try (
                    RecordCursorFactory factory = compiler.compile("select s2, sum(val) from tab where t >= '1970-01-04T12:01' and t < '1970-01-07T11:00' order by s2", sqlExecutionContext).getRecordCursorFactory()
            ) {
                Record[] expected = new Record[]{
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 106413.99769604905;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return null;
                            }
                        },
                };
                assertCursorRawRecords(expected, factory, false);
            }

            /// test key on overlap
            try (
                    RecordCursorFactory factory = compiler.compile("select s2, sum(val) from tab where t >= '1970-01-12T12:01' and t < '1970-01-14T11:00' order by s2", sqlExecutionContext).getRecordCursorFactory()
            ) {
                Record[] expected = new Record[]{
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 15636.977658744854;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return null;
                            }
                        },
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 13073.816187889399;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return "a1";
                            }
                        },
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 13240.269899560482;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return "a2";
                            }
                        },
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 13223.021189180576;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return "a3";
                            }
                        },
                };
                assertCursorRawRecords(expected, factory, false);
            }
        });
    }

    @Test
    public void testIntSymbolSumAddValueTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1, timestamp_sequence(0, 1000000) t from long_sequence(1000000)) timestamp(t) partition by DAY", sqlExecutionContext);
            compile("alter table tab add column val double ", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('s1','s2','s3', null), timestamp_sequence(cast('1970-01-13T00:00:00.000000Z' as timestamp), 1000000), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);


            // test with key falling within null columns
            assertSql(
                    "select s1, sum(val) from tab where t > '1970-01-04T12:00' and t < '1970-01-07T11:00' order by s1",
                    "s1\tsum\n" +
                            "\tNaN\n" +
                            "s1\tNaN\n" +
                            "s2\tNaN\n" +
                            "s3\tNaN\n"
            );

            assertSql(
                    "select s1, round(sum(val), 8) as sum from tab where t > '1970-01-12T12:00' and t < '1970-01-14T11:00' order by s1",
                    "s1\tsum\n" +
                            "\t13168.08843159\n" +
                            "s1\t12972.77827527\n" +
                            "s2\t13388.11832829\n" +
                            "s3\t12929.34474745\n"
            );
        });
    }

    @Test
    public void testIntSymbolSumTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1, rnd_double(2) val, timestamp_sequence(0, 1000000) t from long_sequence(1000000)) timestamp(t) partition by DAY", sqlExecutionContext);
            compile("alter table tab add column s2 symbol cache", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('s1','s2','s3', null), rnd_double(2), timestamp_sequence(cast('1970-01-13T00:00:00.000000Z' as timestamp), 1000000), rnd_symbol('a1','a2','a3', null) s2 from long_sequence(1000000)", sqlExecutionContext);

            // test with key falling within null columns
            try (
                    RecordCursorFactory factory = compiler.compile("select s2, sum(val) from tab order by s2", sqlExecutionContext).getRecordCursorFactory()
            ) {
                Record[] expected = new Record[]{
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 520447.6629968692;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return null;
                            }
                        },
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 104308.65839619662;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return "a1";
                            }
                        },
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 104559.28674751727;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return "a2";
                            }
                        },
                        new Record() {
                            @Override
                            public double getDouble(int col) {
                                return 104044.11326997768;
                            }

                            @Override
                            public CharSequence getSym(int col) {
                                return "a3";
                            }
                        },
                };
                assertCursorRawRecords(expected, factory, false);
            }
        });
    }

    @Test
    public void testMinMaxAggregations() throws Exception {
        String[] aggregateFunctions = {"max", "min"};
        TypeVal[] aggregateColTypes = {
                new TypeVal(ColumnType.BYTE, "NaN:INT"),
                new TypeVal(ColumnType.CHAR, ":CHAR"),
                new TypeVal(ColumnType.SHORT, "NaN:INT"),
                new TypeVal(ColumnType.INT, "NaN:INT"),
                new TypeVal(ColumnType.LONG, "NaN:LONG"),
                new TypeVal(ColumnType.DATE, ":DATE"),
                new TypeVal(ColumnType.TIMESTAMP, ":TIMESTAMP"),
                new TypeVal(ColumnType.FLOAT, "NaN:FLOAT"),
                new TypeVal(ColumnType.DOUBLE, "NaN:DOUBLE")};

        testAggregations(aggregateFunctions, aggregateColTypes);
    }

    @Test
    public void testNonRostiWithManyAggregateFunctions1() throws Exception {
        executeWithPool(1, 32, KeyedAggregationTest::runGroupByWithAgg);
    }

    @Test
    public void testNonRostiWithManyAggregateFunctions2() throws Exception {
        executeWithPool(4, 32, KeyedAggregationTest::runGroupByWithAgg);
    }

    //test rosti failing to alloc in parallel aggregate computation
    @Test
    public void testOOMInRostiAggCalcResetsAllocatedNativeMemoryToMinSizes() throws Exception {
        RostiAllocFacade rostiAllocFacade = new RostiAllocFacadeImpl() {
            @Override
            public void clear(long pRosti) {
                Rosti.enableOOMOnMalloc();
                super.clear(pRosti);
            }

            @Override
            public boolean reset(long pRosti, int toSize) {
                Rosti.disableOOMOnMalloc();
                return super.reset(pRosti, toSize);
            }
        };

        executeWithPool(4, 16, rostiAllocFacade, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            compiler.compile("create table tab as (select rnd_double() d, cast(x as int) i, x l from long_sequence(100000))", sqlExecutionContext);
            String query = "select i, sum(d) from tab group by i";

            assertRostiMemory(compiler, query, sqlExecutionContext);
        });
    }

    @Test
    public void testOOMInRostiInitRelasesAllocatedNativeMemory() throws Exception {
        RostiAllocFacade rostiAllocFacade = new RostiAllocFacadeImpl() {
            int counter = 0;

            @Override
            public long alloc(ColumnTypes types, long capacity) {
                if (++counter == 4) {
                    return 0;
                } else {
                    return super.alloc(types, capacity);
                }
            }
        };

        executeWithPool(4, 16, rostiAllocFacade, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            compiler.compile("create table tab as (select rnd_double() d, cast(x as int) i, x l from long_sequence(1000))", sqlExecutionContext);
            long memBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ROSTI);
            try {
                assertQuery(compiler, "", "select i, sum(d) from tab group by i", null, true, sqlExecutionContext, true);
                Assert.fail();
            } catch (OutOfMemoryError oome) {
                long memAfter = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ROSTI);
                MatcherAssert.assertThat(memAfter, is(equalTo(memBefore)));
            }
        });
    }

    //Test triggers OOM during merge.
    //It's complex because it has to force scheduler (via custom rosti alloc facade) to split work evenly by having one thread wait until other rosti is big enough.
    //This triggers merge of two rostis that in turn forces rosti resize and only then hits OOM in native code.
    @Test
    public void testOOMInRostiMergeResetsAllocatedNativeMemoryToMinSizes() throws Exception {
        final int WORKER_COUNT = 2;
        pageFrameMaxRows = 1000;//if it's default (1mil) then rosti could create single task for whole table data

        RostiAllocFacade rostiAllocFacade = new RostiAllocFacadeImpl() {
            final long[] pRostis = new long[2];
            final AtomicInteger sizeCounter = new AtomicInteger(0);
            int rostis;

            @Override
            public long alloc(ColumnTypes types, long capacity) {
                long result = super.alloc(types, capacity);
                pRostis[rostis++] = result;
                return result;
            }

            @Override
            public long getSize(long pRosti) {
                int currentValue = sizeCounter.incrementAndGet();
                if (currentValue == WORKER_COUNT + 1) {
                    Rosti.enableOOMOnMalloc();
                }
                return super.getSize(pRosti);
            }

            @Override
            public boolean reset(long pRosti, int toSize) {
                Rosti.disableOOMOnMalloc();
                return super.reset(pRosti, toSize);
            }

            @Override
            public void updateMemoryUsage(long pRosti, long oldSize) {
                long otherProsti = pRosti == pRostis[0] ? pRostis[1] : pRostis[0];
                long sleepStart = System.currentTimeMillis();

                if (Rosti.getSize(pRosti) == 1000) {
                    while (Rosti.getSize(otherProsti) < 1000) {
                        Os.sleep(1);
                        if ((System.currentTimeMillis() - sleepStart) > 30000) {
                            throw new RuntimeException("Timed out waiting for rosti to consume data!");
                        }
                    }
                }

                super.updateMemoryUsage(pRosti, oldSize);
            }
        };

        executeWithPool(WORKER_COUNT, 64, rostiAllocFacade, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            compiler.compile("create table tab as (select rnd_double() d, cast(x as int) i, x l from long_sequence(2000))", sqlExecutionContext);
            String query = "select i, sum(l) from tab group by i";

            assertRostiMemory(compiler, query, sqlExecutionContext);
        });
    }

    //triggers OOM in wrapUp() wiht multiple workers
    //some implementations of wrapUp() add null entry and could trigger resize for columns added after table creation (with column tops)
    @Test
    public void testOOMInRostiWrapUpWithMultipleWorkersResetsAllocatedNativeMemoryToMinSizes() throws Exception {
        final int WORKER_COUNT = 2;
        final AtomicInteger sizeCounter = new AtomicInteger(0);

        final RostiAllocFacade raf = new RostiAllocFacadeImpl() {
            @Override
            public long getSize(long pRosti) {
                int currentValue = sizeCounter.incrementAndGet();
                if (currentValue == WORKER_COUNT + 1) {
                    Rosti.enableOOMOnMalloc();
                }
                return super.getSize(pRosti);
            }

            @Override
            public boolean reset(long pRosti, int toSize) {
                Rosti.disableOOMOnMalloc();
                return super.reset(pRosti, toSize);
            }

            @Override
            public void updateMemoryUsage(long pRosti, long oldSize) {
                super.updateMemoryUsage(pRosti, oldSize);
            }
        };

        executeWithPool(WORKER_COUNT, 64, raf, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            compiler.compile("create table tab as " +
                    "(select rnd_double() d, x, " +
                    "rnd_int() i, " +
                    "rnd_long() l, " +
                    "rnd_date(to_date('2015', 'yyyy'), to_date('2022', 'yyyy'), 0) dat, " +
                    "rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2022','yyyy'),0) tstmp " +
                    "from long_sequence(100))", sqlExecutionContext);
            compiler.compile("alter table tab add column s symbol cache", sqlExecutionContext).execute(null).await();
            compiler.compile("insert into tab select rnd_double(), " +
                    "x + 1000,   " +
                    "rnd_int() i, " +
                    "rnd_long() l, " +
                    "rnd_date(to_date('2015', 'yyyy'), to_date('2022', 'yyyy'), 0) dat, " +
                    "rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2022','yyyy'),0) tstmp, " +
                    "cast('s' || x as symbol)  from long_sequence(896)", sqlExecutionContext);

            final String[] functions = {"sum(d)", "min(d)", "max(d)", "avg(d)", "nsum(d)", "ksum(d)",
                    "sum(tstmp)", "min(tstmp)", "max(tstmp)",
                    "sum(l)", "min(l)", "max(l)", "avg(l)",
                    "sum(i)", "min(i)", "min(i)", "avg(i)",
                    "sum(dat)", "min(dat)", "max(dat)"};

            for (String function : functions) {
                String query = "select s, " + function + " from tab group by s";
                LOG.infoW().$(function).$();
                //String query = "select s, sum(d) from tab group by s";
                assertRostiMemory(compiler, query, sqlExecutionContext);
                sizeCounter.set(0);
            }
        });
    }

    @Test
    public void testOOMInRostiWrapUpWithOneWorkerResetsAllocatedNativeMemoryToMinSizes() throws Exception {
        final int WORKER_COUNT = 1;

        RostiAllocFacade rostiAllocFacade = new RostiAllocFacadeImpl() {
            int memCounter;

            @Override
            public boolean reset(long pRosti, int toSize) {
                Rosti.disableOOMOnMalloc();
                return super.reset(pRosti, toSize);
            }

            @Override
            public void updateMemoryUsage(long pRosti, long oldSize) {
                memCounter++;
                super.updateMemoryUsage(pRosti, oldSize);
                if (memCounter == 1) {
                    Rosti.enableOOMOnMalloc();
                }
            }
        };

        executeWithPool(WORKER_COUNT, 64, rostiAllocFacade, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            compiler.compile("create table tab as (select rnd_double() d, x from long_sequence(100))", sqlExecutionContext);
            compiler.compile("alter table tab add column s symbol cache", sqlExecutionContext).execute(null).await();
            compiler.compile("insert into tab select rnd_double(), x + 1000, cast('s' || x as symbol)  from long_sequence(896)", sqlExecutionContext);

            assertRostiMemory(compiler, "select s, sum(d) from tab group by s", sqlExecutionContext);
        });
    }

    @Test
    public void testRostiReallocation() throws Exception {
        configOverrideRostiAllocFacade(
                new RostiAllocFacadeImpl() {
                    @Override
                    public long alloc(ColumnTypes types, long ignore) {
                        // force resize
                        return super.alloc(types, 64);
                    }
                }
        );

        assertMemoryLeak(() -> {
            String ddl = "create table tab as  (select cast(x as int) x1, cast(x as date) dt from long_sequence(1500))";
            String sql = "select distinct count, count1 from (select x1, count(*), count(*) from tab group by x1)";
            assertQuery(
                    "count\tcount1\n1\t1\n",
                    sql,
                    ddl,
                    null, true, true, false
            );
        });
    }

    @Test
    public void testRostiWithColTopsAndIdleWorkers() throws Exception {
        executeWithPool(4, 16, KeyedAggregationTest::runCountTestWithColTops);
    }

    @Test
    public void testRostiWithColTopsAndManyWorkers() throws Exception {
        executeWithPool(4, 32, KeyedAggregationTest::runGroupByTest);
    }

    @Test
    public void testRostiWithColTopsAndNoWorkers() throws Exception {
        executeWithPool(0, 0, KeyedAggregationTest::runGroupByTest);
    }

    @Test
    public void testRostiWithIdleWorkers() throws Exception {
        executeWithPool(4, 16, KeyedAggregationTest::runGroupByTest);
    }

    @Test
    public void testRostiWithIdleWorkers2() throws Exception {
        executeWithPool(4, 1, KeyedAggregationTest::runGroupByTest);
    }

    @Test
    public void testRostiWithKeyColTopsAndIdleWorkers() throws Exception {
        executeWithPool(4, 16, KeyedAggregationTest::runCountTestWithKeyColTops);
    }

    @Test
    public void testRostiWithKeyColTopsAndManyWorkers() throws Exception {
        executeWithPool(4, 32, KeyedAggregationTest::runCountTestWithKeyColTops);
    }

    @Test
    public void testRostiWithKeyColTopsAndNoWorkers() throws Exception {
        executeWithPool(0, 0, KeyedAggregationTest::runCountTestWithKeyColTops);
    }

    @Test
    public void testRostiWithManyAggregateFunctions1() throws Exception {
        executeWithPool(1, 32, KeyedAggregationTest::runGroupByIntWithAgg);
    }

    @Test
    public void testRostiWithManyAggregateFunctions2() throws Exception {
        executeWithPool(2, 32, KeyedAggregationTest::runGroupByIntWithAgg);
    }

    @Test
    public void testRostiWithManyWorkers() throws Exception {
        executeWithPool(4, 32, KeyedAggregationTest::runGroupByTest);
    }

    @Test
    public void testRostiWithNoWorkers() throws Exception {
        executeWithPool(0, 0, KeyedAggregationTest::runGroupByTest);
    }

    @Test
    public void testStrFunctionKey() throws Exception {
        // An important aspect of this test is that both replace() and count_distinct()
        // functions use the same column as the input.
        assertQuery("replace\tcount\tcount_distinct\n" +
                        "foobaz\t63\t2\n" +
                        "bazbaz\t37\t1\n",
                "select replace(s, 'bar', 'baz'), count(), count_distinct(s) from tab",
                "create table tab as (select timestamp_sequence(0, 100000) ts, rnd_str('foobar','foobaz','barbaz') s from long_sequence(100))",
                null, true, true, true
        );
    }

    @Test
    public void testStrFunctionKeyReverseOrder() throws Exception {
        // An important aspect of this test is that both replace() and count_distinct()
        // functions use the same column as the input.
        assertQuery("count_distinct\tcount\treplace\n" +
                        "2\t63\tfoobaz\n" +
                        "1\t37\tbazbaz\n",
                "select count_distinct(s), count(), replace(s, 'bar', 'baz') from tab",
                "create table tab as (select timestamp_sequence(0, 100000) ts, rnd_str('foobar','foobaz','barbaz') s from long_sequence(100))",
                null, true, true, true
        );
    }

    @Test
    public void testSumInTimestampRange() throws Exception {
        long step = 1000000L;
        long count = 1000000L;
        long increment = count / 10;
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1, 0.5 val, timestamp_sequence(0, " + step + ") t from long_sequence(" + count + ")) timestamp(t) partition by DAY", sqlExecutionContext);

            for (long ts = 0; ts < count; ts += increment) {
                String value = String.valueOf((ts - 1) * 0.5);
                String expected = "s\n" +
                        (ts > 0 ? value : "NaN") + "\n";
                assertSql("select sum(val) s from tab where t >= CAST(" + step + " AS TIMESTAMP) AND t < CAST(" + (ts * step) + " AS TIMESTAMP)",
                        expected
                );
            }
        });
    }

    @Test
    public void testSumInTimestampRangeWithColTop() throws Exception {
        final long step = 100000L;
        final long count = 1000000L;
        final long increment = count / 10;
        assertMemoryLeak(() -> {
            String createSql = "create table tab as (select rnd_symbol('s1','s2','s3', null) s1, 0.5 val, timestamp_sequence(0, " + step + ") t from long_sequence(" + count + ")) timestamp(t) partition by DAY";
            compiler.compile(createSql, sqlExecutionContext);
            compile("alter table tab add val2 DOUBLE", sqlExecutionContext);
            String insetSql = "insert into tab select rnd_symbol('s1','s2','s3', null) s1, 0.5 val, timestamp_sequence(" + count * step + ", " + step + ") t, 1 val2 from long_sequence(" + count + ")";
            compiler.compile(insetSql, sqlExecutionContext);

            // Move upper timestamp boundary
            // [step, ts * step)
            for (long ts = increment; ts < 2 * count; ts += increment) {
                String expected = "s1\ts2\n" +
                        ((ts - 1) * 0.5) + "\t" + (ts <= count ? "NaN" : (ts - count) * 1.0) + "\n";
                assertSql(
                        "select sum(val) s1,  sum(val2) s2 from tab where t >= CAST(" + step + " AS TIMESTAMP) AND t < CAST(" + (ts * step) + " AS TIMESTAMP)",
                        expected
                );
            }

            // Move lower timestamp boundary
            // [ts * count, 2 * step * count - 1) time range
            for (long ts = 0; ts < 2 * count; ts += increment) {
                String expected = "s1\ts2\n" +
                        ((2 * count - ts - 1) * 0.5) + "\t" + (ts < count ? (count - 1) * 1.0 : (2 * count - ts - 1) * 1.0) + "\n";
                assertSql(
                        "select sum(val) s1, sum(val2) s2 from tab where t >= CAST(" + (ts * step) + " AS TIMESTAMP) AND t < CAST(" + ((2 * count - 1) * step) + " AS TIMESTAMP)",
                        expected
                );
            }
        });
    }

    private static CompiledQuery compile(SqlCompiler compiler, CharSequence query, SqlExecutionContext executionContext) throws SqlException {
        CompiledQuery cc = compiler.compile(query, executionContext);
        try (OperationFuture future = cc.execute(null)) {
            future.await();
        }
        return cc;
    }

    private static String getColumnName(int type) {
        String typeStr = ColumnType.nameOf(type);
        return "c" + typeStr.replace("(", "").replace(")", "");
    }

    private static void runCountTestWithColTops(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception {
        compile(compiler, "create table x ( tstmp timestamp ) timestamp (tstmp) partition by hour", sqlExecutionContext);
        compile(compiler, "insert into x values  (0::timestamp), (1::timestamp), (3600L*1000000::timestamp) ", sqlExecutionContext);
        compile(compiler, "alter table x add column k int", sqlExecutionContext);
        compile(compiler, "insert into x values ((1+3600L*1000000)::timestamp, 3), (2*3600L*1000000::timestamp, 4), ((1+2*3600L*1000000)::timestamp, 5), (3*3600L*1000000::timestamp, 0) ", sqlExecutionContext);
        compile(compiler, "alter table x add column i int, l long, d double, dat date, ts timestamp", sqlExecutionContext);
        compile(compiler, "insert into x values ((1+3*3600L*1000000)::timestamp,1, null,null, null, null,null), " +
                " ((2+3*3600L*1000000)::timestamp,2, 8,8, 8.0, cast(8 as date), 8::timestamp)," +
                " ((1+4*3600L*1000000)::timestamp,3, null,null, null, null, null)," +
                " ((2+4*3600L*1000000)::timestamp,4, 10,10, 10.0, cast(10 as date), 10::timestamp) ", sqlExecutionContext);

        snapshotMemoryUsage();
        CompiledQuery query = compiler.compile("select k, " +
                "count(1) c1, " +
                "count(*) cstar, " +
                "count(i) ci, " +
                "count(l) cl, " +
                "count(d) cd, " +
                "count(dat) cdat, " +
                "count(ts) cts " +
                "from x order by k", sqlExecutionContext);
        try {
            assertCursor("k\tc1\tcstar\tci\tcl\tcd\tcdat\tcts\n" +
                    "NaN\t3\t3\t0\t0\t0\t0\t0\n" +
                    "0\t1\t1\t0\t0\t0\t0\t0\n" +
                    "1\t1\t1\t0\t0\t0\t0\t0\n" +
                    "2\t1\t1\t1\t1\t1\t1\t1\n" +
                    "3\t2\t2\t0\t0\t0\t0\t0\n" +
                    "4\t2\t2\t1\t1\t1\t1\t1\n" +
                    "5\t1\t1\t0\t0\t0\t0\t0\n", query.getRecordCursorFactory(), true, true, false, sqlExecutionContext);
        } finally {
            Misc.free(query.getRecordCursorFactory());
        }

        snapshotMemoryUsage();
        query = compiler.compile("select hour(tstmp), " +
                "count(1) c1, " +
                "count(*) cstar, " +
                "count(i) ci, " +
                "count(l) cl " +
                "from x " +
                "order by 1", sqlExecutionContext);
        try {
            assertCursor("hour\tc1\tcstar\tci\tcl\n" +
                    "0\t2\t2\t0\t0\n" +
                    "1\t2\t2\t0\t0\n" +
                    "2\t2\t2\t0\t0\n" +
                    "3\t3\t3\t1\t1\n" +
                    "4\t2\t2\t1\t1\n", query.getRecordCursorFactory(), true, true, false, sqlExecutionContext);
        } finally {
            Misc.free(query.getRecordCursorFactory());
        }
    }

    private static void runCountTestWithKeyColTops(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception {
        compile(compiler, "create table x ( tstmp timestamp ) timestamp (tstmp) partition by hour", sqlExecutionContext);
        compile(compiler, "insert into x values  (0::timestamp), (1::timestamp), (3600L*1000000::timestamp) ", sqlExecutionContext);
        compile(compiler, "alter table x add column i int, l long, d double, dat date, ts timestamp", sqlExecutionContext);
        compile(compiler, "insert into x values ((1+3600L*1000000)::timestamp,null,null,null,null,null), ((2*3600L*1000000)::timestamp,5,5, 5.0, cast(5 as date), 5::timestamp)", sqlExecutionContext);
        compile(compiler, "alter table x add column k int", sqlExecutionContext);
        compile(compiler, "insert into x values ((1+2*3600L*1000000)::timestamp, null, null, null, null, null, 6)", sqlExecutionContext);

        snapshotMemoryUsage();
        CompiledQuery query = compiler.compile("select k, " +
                "count(1) c1, " +
                "count(*) cstar, " +
                "count(i) ci, " +
                "count(l) cl " +
                "from x order by k", sqlExecutionContext);
        try {
            assertCursor("k\tc1\tcstar\tci\tcl\n" +
                    "NaN\t5\t5\t1\t1\n" +
                    "6\t1\t1\t0\t0\n", query.getRecordCursorFactory(), true, true, false, sqlExecutionContext);
        } finally {
            Misc.free(query.getRecordCursorFactory());
        }

        snapshotMemoryUsage();
        query = compiler.compile("select hour(tstmp), " +
                "count(1) c1, " +
                "count(*) cstar, " +
                "count(i) ci, " +
                "count(l) cl, " +
                "count(d) cd, " +
                "count(dat) cdat, " +
                "count(ts) cts " +
                "from x " +
                "order by 1", sqlExecutionContext);
        try {
            assertCursor("hour\tc1\tcstar\tci\tcl\tcd\tcdat\tcts\n" +
                    "0\t2\t2\t0\t0\t0\t0\t0\n" +
                    "1\t2\t2\t0\t0\t0\t0\t0\n" +
                    "2\t2\t2\t1\t1\t1\t1\t1\n", query.getRecordCursorFactory(), true, true, false, sqlExecutionContext);
        } finally {
            Misc.free(query.getRecordCursorFactory());
        }
    }

    private static void runGroupByIntWithAgg(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        compiler.compile("create table tab as " +
                "( select cast(x as int) i, " +
                "x as l, " +
                "cast(x as date) dat, " +
                "cast(x as timestamp) ts, " +
                "cast(x as double) d, " +
                "rnd_long256() l256  " +
                "from long_sequence(1000));", sqlExecutionContext);

        snapshotMemoryUsage();
        CompiledQuery query = compiler.compile("select count(*) cnt from " +
                "(select i, count(*), min(i), avg(i), max(i), sum(i), " +
                "min(l), avg(l), max(l), sum(l), " +
                "min(dat), avg(dat), max(dat), sum(dat), " +
                "min(ts), avg(ts), max(ts), sum(ts), " +
                "min(d), avg(d), max(d), sum(d), nsum(d), ksum(d)," +
                "sum(l256), count(i), count(l) from tab group by i )", sqlExecutionContext);

        try {
            assertCursor("cnt\n1000\n", query.getRecordCursorFactory(), false, true, false, sqlExecutionContext);
        } finally {
            Misc.free(query.getRecordCursorFactory());
        }
    }

    private static void runGroupByTest(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        compiler.compile("create table tab as  (select cast(x as int) x1, cast(x as date) dt from long_sequence(1000000))", sqlExecutionContext);
        snapshotMemoryUsage();
        CompiledQuery query = compiler.compile("select count(*) cnt from (select x1, count(*), count(*) from tab group by x1)", sqlExecutionContext);

        try {
            assertCursor("cnt\n1000000\n", query.getRecordCursorFactory(), false, true, false, sqlExecutionContext);
        } finally {
            Misc.free(query.getRecordCursorFactory());
        }
    }

    private static void runGroupByWithAgg(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        compiler.compile("create table tab as " +
                "( select cast(x as int) i, " +
                "x as l, " +
                "cast(x as date) dat, " +
                "cast(x as timestamp) ts, " +
                "cast(x as double) d, " +
                "rnd_long256() l256  " +
                "from long_sequence(1000));", sqlExecutionContext);

        snapshotMemoryUsage();
        CompiledQuery query = compiler.compile("select count(*) cnt from " +
                "(select count(*), min(i), avg(i), max(i), sum(i), " +
                "min(l), avg(l), max(l), sum(l), " +
                "min(dat), avg(dat), max(dat), sum(dat), " +
                "min(ts), avg(ts), max(ts), sum(ts), " +
                "min(d), avg(d), max(d), sum(d), nsum(d), ksum(d)," +
                "sum(l256), count(i), count(l) from tab )", sqlExecutionContext);

        try {
            assertCursor("cnt\n1\n", query.getRecordCursorFactory(), false, true, false, sqlExecutionContext);
        } finally {
            Misc.free(query.getRecordCursorFactory());
        }
    }

    private void assertRostiMemory(SqlCompiler compiler, String query, SqlExecutionContext sqlExecutionContext) throws SqlException {
        long memBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ROSTI);
        try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            try {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                }
                Assert.fail("Cursor should throw OOM for " + query + " !");
            } catch (OutOfMemoryError oome) {
                long memAfter = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ROSTI);
                MatcherAssert.assertThat(memAfter, is(lessThan(memBefore + 10 * 1024)));//rostis are minimized on OOM
            }
        } finally {
            Rosti.disableOOMOnMalloc();
        }
    }

    private void executeWithPool(
            int workerCount,
            int queueSize,
            CustomisableRunnable runnable) throws Exception {
        executeWithPool(workerCount, queueSize, RostiAllocFacadeImpl.INSTANCE, runnable);
    }

    private void executeWithPool(
            int workerCount,
            int queueSize,
            RostiAllocFacade rostiAllocFacade,
            CustomisableRunnable runnable
    ) throws Exception {
        // we need to create entire engine
        assertMemoryLeak(() -> {
            if (workerCount > 0) {
                WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
                    @Override
                    public long getSleepTimeout() {
                        return 1;
                    }

                    @Override
                    public int getWorkerCount() {
                        return workerCount - 1;
                    }
                });

                final CairoConfiguration configuration1 = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public RostiAllocFacade getRostiAllocFacade() {
                        return rostiAllocFacade;
                    }

                    @Override
                    public int getSqlPageFrameMaxRows() {
                        return configuration.getSqlPageFrameMaxRows();
                    }

                    @Override
                    public int getVectorAggregateQueueCapacity() {
                        return queueSize;
                    }
                };

                execute(pool, runnable, configuration1);
            } else {
                final CairoConfiguration configuration1 = new DefaultTestCairoConfiguration(root);
                execute(null, runnable, configuration1);
            }
        });
    }

    private void testAggregations(String[] aggregateFunctions, TypeVal[] aggregateColTypes) throws SqlException {
        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        StringBuilder resultHeader = new StringBuilder();
        StringBuilder resultData = new StringBuilder();

        try (TableModel tt1 = new TableModel(configuration, "tt1", PartitionBy.NONE)) {
            for (TypeVal colType : aggregateColTypes) {
                tt1.col(colType.colName, colType.columnType);
            }

            CairoTestUtils.create(tt1);
        }

        for (TypeVal colType : aggregateColTypes) {

            for (String func : aggregateFunctions) {
                sql.setLength(7);
                resultHeader.setLength(0);
                resultData.setLength(0);

                String typeStr = getColumnName(colType.columnType);
                sql.append(func).append("(").append(colType.funcArg).append(") ").append(func).append(typeStr);
                sql.append(" from tt1");

                resultHeader.append(func).append(typeStr);
                resultData.append(colType.emtpyValue);

                String expected = resultHeader.append("\n").append(resultData).append("\n").toString();
                assertSqlWithTypes(sql.toString(), expected);

                // Force to go to not-vector execution
                assertSqlWithTypes(sql + " where now() > '1000-01-01'", expected);
            }
        }
    }

    protected static void execute(
            @Nullable WorkerPool pool,
            CustomisableRunnable runnable,
            CairoConfiguration configuration
    ) throws Exception {
        final int workerCount = pool == null ? 1 : pool.getWorkerCount() + 1;

        try (final CairoEngine engine = new CairoEngine(configuration);
             final SqlCompiler compiler = new SqlCompiler(engine)) {
            //workerCount - 1
            try (final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount)) {
                try {
                    if (pool != null) {
                        GroupByJob job = new GroupByJob(engine.getMessageBus());
                        pool.assign(job);
                        pool.start(LOG);
                    }

                    runnable.run(engine, compiler, sqlExecutionContext);
                    Assert.assertEquals("busy writer", 0, engine.getBusyWriterCount());
                    Assert.assertEquals("busy reader", 0, engine.getBusyReaderCount());
                } finally {
                    if (pool != null) {
                        pool.halt();
                    }
                }
            }
        }
    }

    private static class TypeVal {
        public final String colName;
        public final int columnType;
        public final String emtpyValue;
        public final String funcArg;

        public TypeVal(int type, String val) {
            columnType = type;
            emtpyValue = val;
            this.colName = getColumnName(type);
            this.funcArg = this.colName;
        }
    }
}

