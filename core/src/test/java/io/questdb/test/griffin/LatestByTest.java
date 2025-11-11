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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LatestByTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public LatestByTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testLatestByAllFilteredReentrant() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table zyzy as (\n" +
                            "  select \n" +
                            "  timestamp_sequence(1,1000)::" + timestampType.getTypeName() + " ts,\n" +
                            "  rnd_int(0,5,0) a,\n" +
                            "  rnd_int(0,5,0) b,\n" +
                            "  rnd_int(0,5,0) c,\n" +
                            "  rnd_int(0,5,0) x,\n" +
                            "  rnd_int(0,5,0) y,\n" +
                            "  rnd_int(0,5,0) z,\n" +
                            "  from long_sequence(100)\n" +
                            ") timestamp(ts);\n"
            );
            assertQuery(
                    "x\tohoh\n" +
                            "15\t29\n" +
                            "17\t26\n" +
                            "9\t29\n" +
                            "7\t25\n",
                    "select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y) and b = 3 latest on ts partition by x;",
                    true
            );
        });
    }

    @Test
    public void testLatestByAllFilteredResolvesSymbol() throws Exception {
        executeWithRewriteTimestamp(
                "CREATE TABLE history_P4v (\n" +
                        "  devid SYMBOL,\n" +
                        "  address SHORT,\n" +
                        "  value SHORT,\n" +
                        "  value_decimal BYTE,\n" +
                        "  created_at DATE,\n" +
                        "  ts #TIMESTAMP\n" +
                        ") timestamp(ts) PARTITION BY DAY;",
                timestampType.getTypeName()
        );

        assertQuery(
                "devid\taddress\tvalue\tvalue_decimal\tcreated_at\tts\n",
                "SELECT * FROM history_P4v\n" +
                        "WHERE\n" +
                        "  devid = 'LLLAHFZHYA'\n" +
                        "LATEST ON ts PARTITION BY address",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testLatestByAllIndexedIndexReaderGetsReloaded() throws Exception {
        final int iterations = 100;
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE e ( \n" +
                            "  ts #TIMESTAMP, \n" +
                            "  sym SYMBOL CAPACITY 32768 INDEX CAPACITY 4 \n" +
                            ") TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            executeWithRewriteTimestamp("CREATE TABLE p ( \n" +
                            "  ts #TIMESTAMP, \n" +
                            "  sym SYMBOL CAPACITY 32768 CACHE INDEX CAPACITY 4, \n" +
                            "  lon FLOAT, \n" +
                            "  lat FLOAT, \n" +
                            "  g3 geohash(3c) \n" +
                            ") TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );

            long timestamp = 1625853700000000L;
            for (int i = 0; i < iterations; i++) {
                LOG.info().$("Iteration: ").$(i).$();

                execute("INSERT INTO e VALUES(CAST(" + timestamp + " as TIMESTAMP), '42')");
                execute("INSERT INTO p VALUES(CAST(" + timestamp + " as TIMESTAMP), '42', 142.31, 42.31, #xpt)");

                String query = "SELECT count() FROM \n" +
                        "( \n" +
                        "  ( \n" +
                        "    SELECT ts ts_p, sym, lon, lat, g3 \n" +
                        "    FROM p \n" +
                        "    WHERE ts >= cast(" + timestamp + " AS timestamp) \n" +
                        "      AND g3 within(#xpk, #xpm, #xps, #xpt) \n" +
                        "    LATEST ON ts PARTITION BY sym \n" +
                        "  ) \n" +
                        "  WHERE lon >= 142.0 AND lon <= 143.0 \n" +
                        "    AND lat >= 42.0 AND lat <= 43.0 \n" +
                        ") \n" +
                        "JOIN \n" +
                        "( \n" +
                        "  SELECT ts ts_e, sym \n" +
                        "  FROM e \n" +
                        "  WHERE ts >= cast(" + timestamp + " AS timestamp) \n" +
                        "  LATEST ON ts PARTITION BY sym \n" +
                        ") \n" +
                        "ON (sym)";
                assertQuery(
                        "count\n" +
                                "1\n",
                        query,
                        null,
                        false,
                        true
                );

                timestamp += 10000L;
            }
        });
    }

    @Test
    public void testLatestByAllIndexedWithPrefixes() throws Exception {
        configOverrideUseWithinLatestByOptimisation();

        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table pos_test\n" +
                            "( \n" +
                            "  ts #TIMESTAMP,\n" +
                            "  device_id symbol index,\n" +
                            "  g8c geohash(8c)\n" +
                            ") timestamp(ts) partition by day;",
                    timestampType.getTypeName()
            );

            execute(
                    "insert into pos_test values " +
                            "('2021-09-02T00:00:00.000000', 'device_1', #46swgj10)," +
                            "('2021-09-02T00:00:00.000001', 'device_2', #46swgj10)," +
                            "('2021-09-02T00:00:00.000002', 'device_1', #46swgj12)"
            );

            String query = "SELECT *\n" +
                    "FROM pos_test\n" +
                    "WHERE g8c within(#46swgj10)\n" +
                    "and ts in '2021-09-02'\n" +
                    "LATEST ON ts \n" +
                    "PARTITION BY device_id";

            assertPlanNoLeakCheck(
                    query,
                    "LatestByAllIndexed\n" +
                            "    Async index backward scan on: device_id workers: 2\n" +
                            "      filter: g8c within(\"0010000110110001110001111100010000100000\")\n" +
                            "    Interval backward scan on: pos_test\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"2021-09-02T00:00:00.000000Z\",\"2021-09-02T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"2021-09-02T00:00:00.000000000Z\",\"2021-09-02T23:59:59.999999999Z\")]\n")
            );

            // prefix filter is applied AFTER latest on
            assertQuery(
                    "ts\tdevice_id\tg8c\n" +
                            "2021-09-02T00:00:00.000001" + getTimestampSuffix(timestampType.getTypeName()) + "\tdevice_2\t46swgj10\n",
                    query,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tb\n",
                    "select ts, s from t " +
                            "where s in ('a', 'b') " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByMultipleSymbolsDoesNotNeedFullScan1() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");
            execute("insert into t values ('e', 'f', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "1970-01-02T18:00:00.000000" + suffix + "\td\ta\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\tc\ta\n",
                    "select ts, s2, s from t " +
                            "where s = 'a' and s2 in ('c', 'd') " +
                            "latest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByMultipleSymbolsDoesNotNeedFullScan2() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");
            execute("insert into t values ('a', 'e', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\tb\n",
                    "select ts, s2, s from t " +
                            "where s2 = 'c' " +
                            "latest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByMultipleSymbolsDoesNotNeedFullScan3() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");
            execute("insert into t values ('a', 'e', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\ts2\tts\n" +
                            "a\tc\t1970-01-02T23:00:00.000000" + suffix + "\n" +
                            "b\tc\t1970-01-03T00:00:00.000000" + suffix + "\n" +
                            "a\td\t1970-01-02T18:00:00.000000" + suffix + "\n" +
                            "b\td\t1970-01-02T19:00:00.000000" + suffix + "\n",
                    "select * from t where s2 = 'c' latest on ts partition by s, s2 " +
                            "union all " +
                            "select * from t where s2 = 'd' latest on ts partition by s, s2",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testLatestByMultipleSymbolsUnfilteredDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "1970-01-02T18:00:00.000000" + suffix + "\td\ta\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\td\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\tb\n",
                    "select ts, s2, s from t " +
                            "latest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByMultipleSymbolsWithNullInSymbolsDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(100)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\ts2\ts3\tts\n" +
                            "\tc\t\t1970-01-03T19:00:00.000000" + suffix + "\n" +
                            "b\tc\t\t1970-01-04T00:00:00.000000" + suffix + "\n" +
                            "\t\td\t1970-01-04T05:00:00.000000" + suffix + "\n" +
                            "a\t\t\t1970-01-04T07:00:00.000000" + suffix + "\n" +
                            "a\t\td\t1970-01-04T11:00:00.000000" + suffix + "\n" +
                            "a\tc\t\t1970-01-04T17:00:00.000000" + suffix + "\n" +
                            "b\tc\td\t1970-01-04T20:00:00.000000" + suffix + "\n" +
                            "b\t\t\t1970-01-04T23:00:00.000000" + suffix + "\n" +
                            "\t\t\t1970-01-05T00:00:00.000000" + suffix + "\n" +
                            "a\tc\td\t1970-01-05T01:00:00.000000" + suffix + "\n" +
                            "b\t\td\t1970-01-05T02:00:00.000000" + suffix + "\n" +
                            "\tc\td\t1970-01-05T03:00:00.000000" + suffix + "\n",
                    "t " +
                            "where s in ('a', 'b', null) " +
                            "latest on ts partition by s3, s2, s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByMultipleSymbolsWithNullInSymbolsUnfilteredDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(100)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\ts2\ts3\tts\n" +
                            "\tc\t\t1970-01-03T19:00:00.000000" + suffix + "\n" +
                            "b\tc\t\t1970-01-04T00:00:00.000000" + suffix + "\n" +
                            "\t\td\t1970-01-04T05:00:00.000000" + suffix + "\n" +
                            "a\t\t\t1970-01-04T07:00:00.000000" + suffix + "\n" +
                            "a\t\td\t1970-01-04T11:00:00.000000" + suffix + "\n" +
                            "a\tc\t\t1970-01-04T17:00:00.000000" + suffix + "\n" +
                            "b\tc\td\t1970-01-04T20:00:00.000000" + suffix + "\n" +
                            "b\t\t\t1970-01-04T23:00:00.000000" + suffix + "\n" +
                            "\t\t\t1970-01-05T00:00:00.000000" + suffix + "\n" +
                            "a\tc\td\t1970-01-05T01:00:00.000000" + suffix + "\n" +
                            "b\t\td\t1970-01-05T02:00:00.000000" + suffix + "\n" +
                            "\tc\td\t1970-01-05T03:00:00.000000" + suffix + "\n",
                    "t " +
                            "latest on ts partition by s3, s2, s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByPartitionByByte() throws Exception {
        testLatestByPartitionBy("byte", "1", "2");
    }

    @Test
    public void testLatestByPartitionByDate() throws Exception {
        testLatestByPartitionBy("date", "'2020-05-05T00:00:00.000Z'", "'2020-05-06T00:00:00.000Z'");
    }

    @Test
    public void testLatestByPartitionByDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table forecasts (when #TIMESTAMP, ts #TIMESTAMP, temperature double) timestamp(ts) partition by day", timestampType.getTypeName());

            // forecasts for 2020-05-05
            execute("insert into forecasts values " +
                    "  ('2020-05-05', '2020-05-02', 40), " +
                    "  ('2020-05-05', '2020-05-03', 41), " +
                    "  ('2020-05-05', '2020-05-04', 42)"
            );

            // forecasts for 2020-05-06
            execute("insert into forecasts values " +
                    "  ('2020-05-06', '2020-05-01', 140), " +
                    "  ('2020-05-06', '2020-05-03', 141), " +
                    "  ('2020-05-06', '2020-05-05', 142), " +// this row has the same ts as following one and will be de-duped
                    "  ('2020-05-07', '2020-05-05', 143)"
            );

            // PARTITION BY <DESIGNATED_TIMESTAMP> is perhaps a bit silly, but it is a valid query. so let's check it's working as expected
            String query = "select when, ts, temperature from forecasts latest on ts partition by ts";
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "when\tts\ttemperature\n" +
                    "2020-05-06T00:00:00.000000" + suffix + "\t2020-05-01T00:00:00.000000" + suffix + "\t140.0\n" +
                    "2020-05-05T00:00:00.000000" + suffix + "\t2020-05-02T00:00:00.000000" + suffix + "\t40.0\n" +
                    "2020-05-06T00:00:00.000000" + suffix + "\t2020-05-03T00:00:00.000000" + suffix + "\t141.0\n" +
                    "2020-05-05T00:00:00.000000" + suffix + "\t2020-05-04T00:00:00.000000" + suffix + "\t42.0\n" +
                    "2020-05-07T00:00:00.000000" + suffix + "\t2020-05-05T00:00:00.000000" + suffix + "\t143.0\n";

            assertQuery(expected, query, "ts", true, true);
        });
    }

    @Test
    public void testLatestByPartitionByDouble() throws Exception {
        testLatestByPartitionBy("double", "0.0", "1.0");
    }

    @Test
    public void testLatestByPartitionByFloat() throws Exception {
        testLatestByPartitionBy("float", "0.0", "1.0");
    }

    @Test
    public void testLatestByPartitionByGeoByte() throws Exception {
        testLatestByPartitionBy("geohash(1c)", "#u", "#v");
    }

    @Test
    public void testLatestByPartitionByGeoInt() throws Exception {
        testLatestByPartitionBy("geohash(4c)", "#uuuu", "#vvvv");
    }

    @Test
    public void testLatestByPartitionByGeoLong() throws Exception {
        testLatestByPartitionBy("geohash(7c)", "#uuuuuuu", "#vvvvvvv");
    }

    @Test
    public void testLatestByPartitionByGeoShort() throws Exception {
        testLatestByPartitionBy("geohash(2c)", "#uu", "#vv");
    }

    @Test
    public void testLatestByPartitionByTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table forecasts (when  #TIMESTAMP, version #TIMESTAMP, temperature double) timestamp(version) partition by day", timestampType.getTypeName());

            // forecasts for 2020-05-05
            execute("insert into forecasts values " +
                    "  ('2020-05-05', '2020-05-02', 40), " +
                    "  ('2020-05-05', '2020-05-03', 41), " +
                    "  ('2020-05-05', '2020-05-04', 42)"
            );

            // forecasts for 2020-05-06
            execute("insert into forecasts values " +
                    "  ('2020-05-06', '2020-05-01', 140), " +
                    "  ('2020-05-06', '2020-05-03', 141), " +
                    "  ('2020-05-06', '2020-05-05', 142)"
            );

            String query = "select when, version, temperature from forecasts latest on version partition by when";
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "when\tversion\ttemperature\n" +
                    "2020-05-05T00:00:00.000000" + suffix + "\t2020-05-04T00:00:00.000000" + suffix + "\t42.0\n" +
                    "2020-05-06T00:00:00.000000" + suffix + "\t2020-05-05T00:00:00.000000" + suffix + "\t142.0\n";

            assertQuery(expected, query, "version", true, true);
        });
    }

    @Test
    public void testLatestBySubQueryInitializesSymbolTables() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE 'offer_exchanges' (" +
                    "pair SYMBOL CAPACITY 100000 INDEX, " +
                    "rate DOUBLE, " +
                    "volume_a DOUBLE, " +
                    "volume_b DOUBLE, " +
                    "buyer STRING, " +
                    "seller STRING, " +
                    "taker STRING, " +
                    "provider STRING, " +
                    "autobridged STRING, " +
                    "tx_hash STRING, " +
                    "ledger_index INT, " +
                    "sequence INT, " +
                    "ts #TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH;", timestampType.getTypeName());
            execute("insert into offer_exchanges values ('abc', 1.1, 1.1, 1.1, 'abc', 'def', 'zxy', 'a', 'some hash', 'foo', 123, 5, '2024-01-29T15:00:00.000Z')");
            execute("insert into offer_exchanges values ('abc', 1.1, 1.1, 1.1, 'abc', 'def', 'zxy', 'a', 'some hash', 'foo', 123, 5, '2024-01-30T15:01:00.000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "pair\topen\tclose\tlow\thigh\tbase_volume\tcounter_volume\texchanges\tprev_rate\tprev_ts\n" +
                            "abc\t1.1\t1.1\t1.1\t1.1\t1.1\t1.1\t1\t1.1\t2024-01-29T15:00:00.000000" + suffix + "\n",
                    "WITH first_selection as (" +
                            "  SELECT pair, first(rate) AS open, last(rate) AS close, min(rate) AS low, max(rate) AS high, " +
                            "         sum(volume_a) AS base_volume, sum(volume_b) AS counter_volume, count(*) AS exchanges " +
                            "  FROM 'offer_exchanges' " +
                            "  WHERE ts >= '2024-01-30T15:00:00.000Z'" +
                            "), " +
                            "second_selection as (" +
                            "  SELECT pair, rate as prev_rate, ts as prev_ts " +
                            "  FROM 'offer_exchanges' " +
                            "  WHERE ts < '2024-01-30T15:00:00.000Z' and pair in (SELECT pair FROM first_selection) " +
                            "  LATEST ON ts PARTITION BY pair " +
                            ") " +
                            "SELECT first_selection.pair, first_selection.open, first_selection.close, first_selection.low, first_selection.high," +
                            "       first_selection.base_volume, first_selection.counter_volume, first_selection.exchanges, second_selection.prev_rate, " +
                            "       second_selection.prev_ts " +
                            "FROM first_selection " +
                            "JOIN second_selection on (pair);",
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testLatestBySymbolDifferentBindingService() throws Exception {
        // Test that a parametrized latest-by <symbol_column> is re-initialized to a different parameter value
        // when the query is re-executed with a different binding variable service

        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            // we'll use the global 'bindVariableService' to compile the query
            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 final RecordCursorFactory factory = CairoEngine.select(compiler, "select ts, s from t " +
                         "where s = :sym " +
                         "latest on ts partition by s", sqlExecutionContext)) {


                // sanity check: verify it returns the expected result when using a new binding variable service
                // with the same value for the parameter as was injected into the global binding variable service
                try (SqlExecutionContextImpl localContext = new SqlExecutionContextImpl(engine, 1)) {
                    BindVariableServiceImpl localBindings = new BindVariableServiceImpl(configuration);
                    localContext.with(localBindings);
                    localBindings.setStr("sym", "c");
                    assertFactoryCursor(
                            "ts\ts\n" +
                                    "1970-01-03T00:00:00.000000" + suffix + "\tc\n",
                            "ts",
                            factory,
                            true,
                            localContext,
                            false,
                            false
                    );
                }

                // re-execute with a different binding variable service and a different value
                // this must yield a different result
                try (SqlExecutionContextImpl localContext = new SqlExecutionContextImpl(engine, 1)) {
                    BindVariableServiceImpl localBindings = new BindVariableServiceImpl(configuration);
                    localContext.with(localBindings);
                    localBindings.setStr("sym", "a");

                    assertFactoryCursor(
                            "ts\ts\n" +
                                    "1970-01-02T23:00:00.000000" + suffix + "\ta\n",
                            "ts",
                            factory,
                            true,
                            localContext,
                            false,
                            false
                    );
                }
            }
        });
    }

    @Test
    public void testLatestBySymbolEmpty() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan any partition, searched symbol values don't exist in symbol table
                    if (Utf8s.containsAscii(name, "1970-01-01") || Utf8s.containsAscii(name, "1970-01-02")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('g', 'd', 'f') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(40)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery(
                    "x\ts\tts\n",
                    "t where s in ('a', 'b') latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestBySymbolManyDistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol(10000, 1, 15, 1000) s, " +
                    "timestamp_sequence(0, 1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(1000000)" +
                    ") timestamp(ts) partition by DAY");

            String distinctSymbols = selectDistinctSym();

            engine.releaseInactive();

            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in other partitions
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "min\tmax\n" +
                            "1970-01-11T15:33:16.000000" + suffix + "\t1970-01-12T13:46:39.000000" + suffix + "\n",
                    "select min(ts), max(ts) from (select ts, x, s from t latest on ts partition by s)",
                    null,
                    false,
                    true
            );

            assertQuery(
                    "min\tmax\n" +
                            "1970-01-11T16:57:53.000000" + suffix + "\t1970-01-12T13:46:05.000000" + suffix + "\n",
                    "select min(ts), max(ts) from (" +
                            "select ts, x, s " +
                            "from t " +
                            "where s in (" + distinctSymbols + ") " +
                            "latest on ts partition by s" +
                            ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testLatestBySymbolUnfilteredDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\tx\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\t47\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\t48\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\t49\t\n",
                    "select ts, x, s from t latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestBySymbolWithNoNulls() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "  select " +
                    "    x, " +
                    "    rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                    "    timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "  from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\tx\ts\n" +
                            "1970-01-02T17:00:00.000000" + suffix + "\t42\td\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\t44\te\n" +
                            "1970-01-02T21:00:00.000000" + suffix + "\t46\tc\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\t47\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\t48\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\t49\tf\n",
                    "select ts, x, s from t latest on ts partition by s",
                    "ts",
                    true,
                    true
            );

            assertQuery(
                    "ts\tx\ts\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\t49\tf\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\t44\te\n" +
                            "1970-01-02T17:00:00.000000" + suffix + "\t42\td\n" +
                            "1970-01-02T21:00:00.000000" + suffix + "\t46\tc\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\t47\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\t48\ta\n",
                    "select ts, x, s from t latest on ts partition by s order by s desc",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByValueEmptyTableExcludedValueFilter() throws Exception {
        executeWithRewriteTimestamp(
                "create table a ( sym symbol, ts #TIMESTAMP ) timestamp(ts) partition by day",
                timestampType.getTypeName()
        );
        assertQuery(
                "sym\tts\n",
                "select sym, ts from a where sym != 'x' latest on ts partition by sym",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testLatestByValueEmptyTableNoFilter() throws Exception {
        executeWithRewriteTimestamp(
                "create table a ( sym symbol, ts #TIMESTAMP ) timestamp(ts) partition by day",
                timestampType.getTypeName()
        );
        assertQuery(
                "sym\tts\n",
                "select sym, ts from a latest on ts partition by sym",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testLatestByValuesFilteredResolvesSymbol() throws Exception {
        executeWithRewriteTimestamp(
                "create table a ( i int, s symbol, ts #TIMESTAMP ) timestamp(ts)",
                timestampType.getTypeName()
        );
        assertQuery(
                "s\ti\tts\n",
                "select s, i, ts " +
                        "from a " +
                        "where s in (select distinct s from a) " +
                        "and s = 'ABC' " +
                        "latest on ts partition by s",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testLatestByWithDeferredNonExistingSymbolOnNonEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE tab (ts #TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n", timestampType.getTypeName());
            execute("insert into tab\n" +
                    "select dateadd('h', -x::int, now()), rnd_symbol('ap', 'btc'), rnd_int(1,1000,0)\n" +
                    "from long_sequence(1000);");

            assertQuery("id\tv\tr_1M\n",
                    "with r as (select id, value v from tab where id = 'apc' || rnd_int() LATEST ON ts PARTITION BY id),\n" +
                            "     rr as (select id, value v from tab where id = 'apc' || rnd_int() and ts <= dateadd('d', -7, now())  LATEST ON ts PARTITION BY id)\n" +
                            "        select r.id, r.v, cast((r.v - rr.v) as float) r_1M\n" +
                            "        from r\n" +
                            "        join rr on id\n", null, false, false
            );
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            bindVariableService.setStr("sym3", "b");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n",
                    "select ts, s from t " +
                            "where s in (:sym1, :sym2) and s != :sym3 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariablesEmptyResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "a");
            assertQuery(
                    "ts\ts\n",
                    "select ts, s from t " +
                            "where s = :sym1 and s != :sym2 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariablesIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    "), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            bindVariableService.setStr("sym3", "b");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n",
                    "select ts, s from t " +
                            "where s in (:sym1, :sym2) and s != :sym3 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariablesNonEmptyResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n",
                    "select ts, s from t " +
                            "where s = :sym1 and s != :sym2 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testLatestByWithInAndNotInBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n",
                    "select ts, s from t " +
                            "where s in ('a', 'b', 'c') and s != :sym " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );

            bindVariableService.clear();
            bindVariableService.setStr("sym1", null);
            bindVariableService.setStr("sym2", "a");
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValuesFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            bindVariableService.setStr("sym3", "d");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T14:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-02T16:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) and s2 = :sym3 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValuesIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    "), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );

            bindVariableService.clear();
            bindVariableService.setStr("sym1", null);
            bindVariableService.setStr("sym2", "a");
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValuesIndexedFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    "), index(s), index(s2) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            bindVariableService.setStr("sym3", "d");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T14:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-02T16:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) and s2 = :sym3 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n",
                    "select ts, s from t " +
                            "where s <> :sym " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesSingleValueIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    "), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n",
                    "select ts, s from t " +
                            "where s <> :sym " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestByWithStaticNonExistingSymbolOnNonEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n");
            execute("insert into tab\n" +
                    "select dateadd('h', -x::int, now()), rnd_symbol('ap', 'btc'), rnd_int(1,1000,0)\n" +
                    "from long_sequence(1000);");

            assertQuery("id\tv\tr_1M\n",
                    "with r as (select id, value v from tab where id = 'apc' LATEST ON ts PARTITION BY id),\n" +
                            "     rr as (select id, value v from tab where id = 'apc' and ts <= dateadd('d', -7, now())  LATEST ON ts PARTITION BY id)\n" +
                            "        select r.id, r.v, cast((r.v - rr.v) as float) r_1M\n" +
                            "        from r\n" +
                            "        join rr on id\n", null, false, false
            );
        });
    }

    @Test
    public void testLatestByWithSymbolOnEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n");

            assertQuery("id\tv\tr_1M\n",
                    "with r as (select id, value v from tab where id = 'apc' LATEST ON ts PARTITION BY id),\n" +
                            "        rr as (select id, value v from tab where id = 'apc' and ts <= dateadd('d', -7, now())  LATEST ON ts PARTITION BY id)\n" +
                            "        select r.id, r.v, cast((r.v - rr.v) as float) r_1M\n" +
                            "        from r\n" +
                            "        join rr on id\n", null, false, false
            );
        });
    }

    @Test
    public void testLatestOnVarchar() throws Exception {
        String suffix = getTimestampSuffix(timestampType.getTypeName());
        assertQuery(
                "x\tv\tts\n" +
                        "42\tb\t1970-01-02T17:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n",
                "t " +
                        "where v in ('a', 'b', 'd') and x%2 = 0 " +
                        "latest on ts partition by v",
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_varchar('a', 'b', 'c', null) v, " +
                        "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "insert into t values (1000, 'd', '1970-01-02T20:00')",
                "x\tv\tts\n" +
                        "42\tb\t1970-01-02T17:00:00.000000" + suffix + "\n" +
                        "1000\td\t1970-01-02T20:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLatestOnVarcharNonAscii() throws Exception {
        String suffix = getTimestampSuffix(timestampType.getTypeName());
        assertQuery(
                "x\tv\tts\n" +
                        "14\t\t1970-01-01T13:00:00.000000" + suffix + "\n" +
                        "17\tраз\t1970-01-01T16:00:00.000000" + suffix + "\n" +
                        "19\tдва\t1970-01-01T18:00:00.000000" + suffix + "\n" +
                        "20\tтри\t1970-01-01T19:00:00.000000" + suffix + "\n",
                "select * " +
                        "from t " +
                        "latest on ts partition by v",
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_varchar('раз', 'два', 'три', null) v, " +
                        "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                        "from long_sequence(20)" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testLatestWithFilterByDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "x\ts\tts\n" +
                            "44\tb\t1970-01-02T19:00:00.000000" + suffix + "\n" +
                            "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n",
                    "t " +
                            "where s in ('a', 'b') and x%2 = 0 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestWithFilterByDoesNotNeedFullScanValueNotInSymbolTable() throws Exception {
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // Query should not scan the first partition
                // all the latest values are in the second, third partition
                if (Utf8s.containsAscii(name, "1970-01-01")) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        String suffix = getTimestampSuffix(timestampType.getTypeName());
        assertQuery(
                "x\ts\tts\n" +
                        "44\tb\t1970-01-02T19:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n",
                "t " +
                        "where s in ('a', 'b', 'c') and x%2 = 0 " +
                        "latest on ts partition by s",
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "insert into t values (1000, 'c', '1970-01-02T20:00')",
                "x\ts\tts\n" +
                        "44\tb\t1970-01-02T19:00:00.000000" + suffix + "\n" +
                        "1000\tc\t1970-01-02T20:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLatestWithJoinIndexed() throws Exception {
        testLatestByWithJoin(true);
    }

    @Test
    public void testLatestWithJoinNonIndexed() throws Exception {
        testLatestByWithJoin(false);
    }

    @Test
    public void testLatestWithNullInSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "x\ts\tts\n" +
                            "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n" +
                            "49\t\t1970-01-03T00:00:00.000000" + suffix + "\n",
                    "t where s in ('a', null) latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLatestWithoutSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "x\ts\tts\n" +
                            "35\ta\t1970-01-02T10:00:00.000000" + suffix + "\n" +
                            "47\tb\t1970-01-02T22:00:00.000000" + suffix + "\n" +
                            "49\t\t1970-01-03T00:00:00.000000" + suffix + "\n",
                    "t where x%2 = 1 latest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSymbolInPredicate_singleElement() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE table trades(symbol symbol, side symbol, timestamp #TIMESTAMP) timestamp(timestamp);", timestampType.getTypeName());
            execute("insert into trades VALUES ('BTC', 'buy', 1609459199000000::timestamp);");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "symbol\tside\ttimestamp\n" +
                    "BTC\tbuy\t2020-12-31T23:59:59.000000" + suffix + "\n";
            String query = "SELECT * FROM trades\n" +
                    "WHERE symbol in ('BTC') and side in 'buy'\n" +
                    "LATEST ON timestamp PARTITION BY symbol;";
            assertSql(expected, query);
        });
    }

    private String selectDistinctSym() throws SqlException {
        StringSink sink = new StringSink();
        try (RecordCursorFactory factory = select("select distinct s from t order by s limit " + 500)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    if (i++ > 0) {
                        sink.put(',');
                    }
                    sink.put('\'').put(record.getSymA(0)).put('\'');
                }
            }
        }
        return sink.toString();
    }

    private void testLatestByPartitionBy(String partitionByType, String valueA, String valueB) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table forecasts " +
                    "( when " + partitionByType + ", " +
                    "version #TIMESTAMP, " +
                    "temperature double) timestamp(version) partition by day", timestampType.getTypeName());
            execute("insert into forecasts values " +
                    "  (" + valueA + ", '2020-05-02', 40), " +
                    "  (" + valueA + ", '2020-05-03', 41), " +
                    "  (" + valueA + ", '2020-05-04', 42), " +
                    "  (" + valueB + ", '2020-05-01', 140), " +
                    "  (" + valueB + ", '2020-05-03', 141), " +
                    "  (" + valueB + ", '2020-05-05', 142)"
            );

            String query = "select when, version, temperature from forecasts latest on version partition by when";
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "when\tversion\ttemperature\n" +
                    valueA.replaceAll("['#]", "") + "\t2020-05-04T00:00:00.000000" + suffix + "\t42.0\n" +
                    valueB.replaceAll("['#]", "") + "\t2020-05-05T00:00:00.000000" + suffix + "\t142.0\n";

            assertQueryNoLeakCheck(expected, query, "version", true, true);
        });
    }

    private void testLatestByWithJoin(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table r (symbol symbol, value long, ts #TIMESTAMP)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", timestampType.getTypeName());
            execute("insert into r values ('xyz', 1, '2022-11-02T01:01:01')");
            executeWithRewriteTimestamp("create table t (symbol symbol, value long, ts #TIMESTAMP)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", timestampType.getTypeName());
            execute("insert into t values ('xyz', 42, '2022-11-02T01:01:01')");

            String query = "with r as (select symbol, value v from r where symbol = 'xyz' latest on ts partition by symbol),\n" +
                    " t as (select symbol, value v from t where symbol = 'xyz' latest on ts partition by symbol)\n" +
                    "select r.symbol, r.v subscribers, t.v followers\n" +
                    "from r\n" +
                    "join t on symbol";
            try (RecordCursorFactory factory = select(query)) {
                assertCursor(
                        "symbol\tsubscribers\tfollowers\n" +
                                "xyz\t1\t42\n",
                        factory,
                        false,
                        false
                );
            }
        });
    }
}
