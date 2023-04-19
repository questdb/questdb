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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Test;

public class LatestByTest extends AbstractGriffinTest {

    @Test
    public void testLatestByAllFilteredResolvesSymbol() throws Exception {
        assertQuery("devid\taddress\tvalue\tvalue_decimal\tcreated_at\tts\n",
                "SELECT * FROM history_P4v\n" +
                        "WHERE\n" +
                        "  devid = 'LLLAHFZHYA'\n" +
                        "LATEST ON ts PARTITION BY address",
                "CREATE TABLE history_P4v (\n" +
                        "  devid SYMBOL,\n" +
                        "  address SHORT,\n" +
                        "  value SHORT,\n" +
                        "  value_decimal BYTE,\n" +
                        "  created_at DATE,\n" +
                        "  ts TIMESTAMP\n" +
                        ") timestamp(ts) PARTITION BY DAY;", "ts", true, false);
    }

    @Test
    public void testLatestByAllIndexedIndexReaderGetsReloaded() throws Exception {
        final int iterations = 100;
        assertMemoryLeak(() -> {
            compile("CREATE TABLE e ( \n" +
                    "  ts TIMESTAMP, \n" +
                    "  sym SYMBOL CAPACITY 32768 INDEX CAPACITY 4 \n" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            compile("CREATE TABLE p ( \n" +
                    "  ts TIMESTAMP, \n" +
                    "  sym SYMBOL CAPACITY 32768 CACHE INDEX CAPACITY 4, \n" +
                    "  lon FLOAT, \n" +
                    "  lat FLOAT, \n" +
                    "  g3 geohash(3c) \n" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");

            long timestamp = 1625853700000000L;
            for (int i = 0; i < iterations; i++) {
                LOG.info().$("Iteration: ").$(i).$();

                executeInsert("INSERT INTO e VALUES(CAST(" + timestamp + " as TIMESTAMP), '42')");
                executeInsert("INSERT INTO p VALUES(CAST(" + timestamp + " as TIMESTAMP), '42', 142.31, 42.31, #xpt)");

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
                assertQuery("count\n" +
                                "1\n",
                        query,
                        null,
                        false,
                        true);

                timestamp += 10000L;
            }
        });
    }

    @Test
    public void testLatestByAllIndexedWithPrefixes() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table pos_test\n" +
                    "( \n" +
                    "  ts timestamp,\n" +
                    "  device_id symbol index,\n" +
                    "  g8c geohash(8c)\n" +
                    ") timestamp(ts) partition by day;");

            compile("insert into pos_test values " +
                    "('2021-09-02T00:00:00.000000', 'device_1', #46swgj10)," +
                    "('2021-09-02T00:00:00.000001', 'device_2', #46swgj10)," +
                    "('2021-09-02T00:00:00.000002', 'device_1', #46swgj12)");

            String query = "SELECT *\n" +
                    "FROM pos_test\n" +
                    "WHERE g8c within(#46swgj10)\n" +
                    "and ts in '2021-09-02'\n" +
                    "LATEST ON ts \n" +
                    "PARTITION BY device_id";

            assertPlan(query,
                    "LatestByAllIndexed\n" +
                            "    Index backward scan on: device_id parallel: true\n" +
                            "      filter: g8c within(\"0010000110110001110001111100010000100000\")\n" +
                            "    Interval backward scan on: pos_test\n" +
                            "      intervals: [(\"2021-09-02T00:00:00.000000Z\",\"2021-09-02T23:59:59.999999Z\")]\n");

            //prefix filter is applied AFTER latest on 
            assertQuery("ts\tdevice_id\tg8c\n" +
                            "2021-09-02T00:00:00.000001Z\tdevice_2\t46swgj10\n",
                    query, "ts", true, true);
        });
    }

    @Test
    public void testLatestByDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("ts\ts\n" +
                            "1970-01-02T23:00:00.000000Z\ta\n" +
                            "1970-01-03T00:00:00.000000Z\tb\n",
                    "select ts, s from t " +
                            "where s in ('a', 'b') " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByMultipleSymbolsDoesNotNeedFullScan1() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");
            executeInsert("insert into t values ('e', 'f', '1970-01-01T01:01:01.000000Z')");

            assertQuery("ts\ts2\ts\n" +
                            "1970-01-02T18:00:00.000000Z\td\ta\n" +
                            "1970-01-02T23:00:00.000000Z\tc\ta\n",
                    "select ts, s2, s from t " +
                            "where s = 'a' and s2 in ('c', 'd') " +
                            "latest on ts partition by s, s2",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByMultipleSymbolsDoesNotNeedFullScan2() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");
            executeInsert("insert into t values ('a', 'e', '1970-01-01T01:01:01.000000Z')");

            assertQuery("ts\ts2\ts\n" +
                            "1970-01-02T23:00:00.000000Z\tc\ta\n" +
                            "1970-01-03T00:00:00.000000Z\tc\tb\n",
                    "select ts, s2, s from t " +
                            "where s2 = 'c' " +
                            "latest on ts partition by s, s2",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByMultipleSymbolsDoesNotNeedFullScan3() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");
            executeInsert("insert into t values ('a', 'e', '1970-01-01T01:01:01.000000Z')");

            assertQuery("s\ts2\tts\n" +
                            "a\tc\t1970-01-02T23:00:00.000000Z\n" +
                            "b\tc\t1970-01-03T00:00:00.000000Z\n" +
                            "a\td\t1970-01-02T18:00:00.000000Z\n" +
                            "b\td\t1970-01-02T19:00:00.000000Z\n",
                    "select * from t where s2 = 'c' latest on ts partition by s, s2 " +
                            "union all " +
                            "select * from t where s2 = 'd' latest on ts partition by s, s2",
                    null,
                    false,
                    true);
        });
    }

    @Test
    public void testLatestByMultipleSymbolsUnfilteredDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("ts\ts2\ts\n" +
                            "1970-01-02T18:00:00.000000Z\td\ta\n" +
                            "1970-01-02T19:00:00.000000Z\td\tb\n" +
                            "1970-01-02T23:00:00.000000Z\tc\ta\n" +
                            "1970-01-03T00:00:00.000000Z\tc\tb\n",
                    "select ts, s2, s from t " +
                            "latest on ts partition by s, s2",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByMultipleSymbolsWithNullInSymbolsDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(100)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("s\ts2\ts3\tts\n" +
                            "\tc\t\t1970-01-03T19:00:00.000000Z\n" +
                            "b\tc\t\t1970-01-04T00:00:00.000000Z\n" +
                            "\t\td\t1970-01-04T05:00:00.000000Z\n" +
                            "a\t\t\t1970-01-04T07:00:00.000000Z\n" +
                            "a\t\td\t1970-01-04T11:00:00.000000Z\n" +
                            "a\tc\t\t1970-01-04T17:00:00.000000Z\n" +
                            "b\tc\td\t1970-01-04T20:00:00.000000Z\n" +
                            "b\t\t\t1970-01-04T23:00:00.000000Z\n" +
                            "\t\t\t1970-01-05T00:00:00.000000Z\n" +
                            "a\tc\td\t1970-01-05T01:00:00.000000Z\n" +
                            "b\t\td\t1970-01-05T02:00:00.000000Z\n" +
                            "\tc\td\t1970-01-05T03:00:00.000000Z\n",
                    "t " +
                            "where s in ('a', 'b', null) " +
                            "latest on ts partition by s3, s2, s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByMultipleSymbolsWithNullInSymbolsUnfilteredDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(100)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("s\ts2\ts3\tts\n" +
                            "\tc\t\t1970-01-03T19:00:00.000000Z\n" +
                            "b\tc\t\t1970-01-04T00:00:00.000000Z\n" +
                            "\t\td\t1970-01-04T05:00:00.000000Z\n" +
                            "a\t\t\t1970-01-04T07:00:00.000000Z\n" +
                            "a\t\td\t1970-01-04T11:00:00.000000Z\n" +
                            "a\tc\t\t1970-01-04T17:00:00.000000Z\n" +
                            "b\tc\td\t1970-01-04T20:00:00.000000Z\n" +
                            "b\t\t\t1970-01-04T23:00:00.000000Z\n" +
                            "\t\t\t1970-01-05T00:00:00.000000Z\n" +
                            "a\tc\td\t1970-01-05T01:00:00.000000Z\n" +
                            "b\t\td\t1970-01-05T02:00:00.000000Z\n" +
                            "\tc\td\t1970-01-05T03:00:00.000000Z\n",
                    "t " +
                            "latest on ts partition by s3, s2, s",
                    "ts",
                    true,
                    true);
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
        compile("create table forecasts (when timestamp, version timestamp, temperature double) timestamp(version) partition by day");

        // forecasts for 2020-05-05
        compile("insert into forecasts values " +
                "  ('2020-05-05', '2020-05-02', 40), " +
                "  ('2020-05-05', '2020-05-03', 41), " +
                "  ('2020-05-05', '2020-05-04', 42)"
        );

        // forecasts for 2020-05-06
        compile("insert into forecasts values " +
                "  ('2020-05-06', '2020-05-01', 140), " +
                "  ('2020-05-06', '2020-05-03', 141), " +
                "  ('2020-05-06', '2020-05-05', 142), " +// this row has the same ts as following one and will be de-duped
                "  ('2020-05-07', '2020-05-05', 143)"
        );

        // PARTITION BY <DESIGNATED_TIMESTAMP> is perhaps a bit silly, but it is a valid query. so let's check it's working as expected
        String query = "select when, version, temperature from forecasts latest on version partition by version";
        String expected = "when\tversion\ttemperature\n" +
                "2020-05-06T00:00:00.000000Z\t2020-05-01T00:00:00.000000Z\t140.0\n" +
                "2020-05-05T00:00:00.000000Z\t2020-05-02T00:00:00.000000Z\t40.0\n" +
                "2020-05-05T00:00:00.000000Z\t2020-05-03T00:00:00.000000Z\t41.0\n" +
                "2020-05-05T00:00:00.000000Z\t2020-05-04T00:00:00.000000Z\t42.0\n" +
                "2020-05-07T00:00:00.000000Z\t2020-05-05T00:00:00.000000Z\t143.0\n";

        assertQuery(expected, query, "version", true, true);
    }

    @Test
    public void testLatestByPartitionByDouble() throws Exception {
        testLatestByPartitionBy("double", "0.0", "1.0");
    }

    @Test
    public void testLatestByPartitionByFloat() throws Exception {
        testLatestByPartitionBy("float", "0.0000", "1.0000");
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
        compile("create table forecasts (when timestamp, version timestamp, temperature double) timestamp(version) partition by day");

        // forecasts for 2020-05-05
        compile("insert into forecasts values " +
                "  ('2020-05-05', '2020-05-02', 40), " +
                "  ('2020-05-05', '2020-05-03', 41), " +
                "  ('2020-05-05', '2020-05-04', 42)"
        );

        // forecasts for 2020-05-06
        compile("insert into forecasts values " +
                "  ('2020-05-06', '2020-05-01', 140), " +
                "  ('2020-05-06', '2020-05-03', 141), " +
                "  ('2020-05-06', '2020-05-05', 142)"
        );

        String query = "select when, version, temperature from forecasts latest on version partition by when";
        String expected = "when\tversion\ttemperature\n" +
                "2020-05-05T00:00:00.000000Z\t2020-05-04T00:00:00.000000Z\t42.0\n" +
                "2020-05-06T00:00:00.000000Z\t2020-05-05T00:00:00.000000Z\t142.0\n";

        assertQuery(expected, query, "version", true, true);
    }

    @Test
    public void testLatestBySymbolEmpty() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan any partition, searched symbol values don't exist in symbol table
                    if (Chars.contains(name, "1970-01-01") || Chars.contains(name, "1970-01-02")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('g', 'd', 'f') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(40)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("x\ts\tts\n",
                    "t where s in ('a', 'b') latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestBySymbolManyDistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol(10000, 1, 15, 1000) s, " +
                    "timestamp_sequence(0, 1000*1000L) ts " +
                    "from long_sequence(1000000)" +
                    ") timestamp(ts) partition by DAY");

            String distinctSymbols = selectDistinctSym();

            engine.releaseInactive();

            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in other partitions
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            assertQuery("min\tmax\n" +
                            "1970-01-11T15:33:16.000000Z\t1970-01-12T13:46:39.000000Z\n",
                    "select min(ts), max(ts) from (select ts, x, s from t latest on ts partition by s)",
                    null,
                    false,
                    true);

            assertQuery("min\tmax\n" +
                            "1970-01-11T16:57:53.000000Z\t1970-01-12T13:46:05.000000Z\n",
                    "select min(ts), max(ts) from (" +
                            "select ts, x, s " +
                            "from t " +
                            "where s in (" + distinctSymbols + ") " +
                            "latest on ts partition by s" +
                            ")",
                    null,
                    false,
                    true);
        });
    }

    @Test
    public void testLatestBySymbolUnfilteredDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("ts\tx\ts\n" +
                            "1970-01-02T22:00:00.000000Z\t47\tb\n" +
                            "1970-01-02T23:00:00.000000Z\t48\ta\n" +
                            "1970-01-03T00:00:00.000000Z\t49\t\n",
                    "select ts, x, s from t latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestBySymbolWithNoNulls() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("ts\tx\ts\n" +
                            "1970-01-02T17:00:00.000000Z\t42\td\n" +
                            "1970-01-02T19:00:00.000000Z\t44\te\n" +
                            "1970-01-02T21:00:00.000000Z\t46\tc\n" +
                            "1970-01-02T22:00:00.000000Z\t47\tb\n" +
                            "1970-01-02T23:00:00.000000Z\t48\ta\n" +
                            "1970-01-03T00:00:00.000000Z\t49\tf\n",
                    "select ts, x, s from t latest on ts partition by s",
                    "ts",
                    true,
                    true);

            assertQuery("ts\tx\ts\n" +
                            "1970-01-03T00:00:00.000000Z\t49\tf\n" +
                            "1970-01-02T19:00:00.000000Z\t44\te\n" +
                            "1970-01-02T17:00:00.000000Z\t42\td\n" +
                            "1970-01-02T21:00:00.000000Z\t46\tc\n" +
                            "1970-01-02T22:00:00.000000Z\t47\tb\n" +
                            "1970-01-02T23:00:00.000000Z\t48\ta\n",
                    "select ts, x, s from t latest on ts partition by s order by s desc",
                    null,
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByValuesFilteredResolvesSymbol() throws Exception {
        assertQuery("s\ti\tts\n",
                "select s, i, ts " +
                        "from a " +
                        "where s in (select distinct s from a) " +
                        "and s = 'ABC' " +
                        "latest on ts partition by s",
                "create table a ( i int, s symbol, ts timestamp ) timestamp(ts)", "ts", true, false);
    }

    @Test
    public void testLatestByWithDeferredNonExistingSymbolOnNonEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab (ts TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n");
            compile("insert into tab\n" +
                    "select dateadd('h', -x::int, now()), rnd_symbol('ap', 'btc'), rnd_int(1,1000,0)\n" +
                    "from long_sequence(1000);");

            assertQuery("id\tv\tr_1M\n",
                    "with r as (select id, value v from tab where id = 'apc' || rnd_int() LATEST ON ts PARTITION BY id),\n" +
                            "     rr as (select id, value v from tab where id = 'apc' || rnd_int() and ts <= dateadd('d', -7, now())  LATEST ON ts PARTITION BY id)\n" +
                            "        select r.id, r.v, cast((r.v - rr.v) as float) r_1M\n" +
                            "        from r\n" +
                            "        join rr on id\n", null, false, false);
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            bindVariableService.setStr("sym3", "b");
            assertQuery("ts\ts\n" +
                            "1970-01-02T23:00:00.000000Z\ta\n",
                    "select ts, s from t " +
                            "where s in (:sym1, :sym2) and s != :sym3 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariablesEmptyResultSet() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "a");
            assertQuery("ts\ts\n",
                    "select ts, s from t " +
                            "where s = :sym1 and s != :sym2 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    false);
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariablesIndexed() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    "), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            bindVariableService.setStr("sym3", "b");
            assertQuery("ts\ts\n" +
                            "1970-01-02T23:00:00.000000Z\ta\n",
                    "select ts, s from t " +
                            "where s in (:sym1, :sym2) and s != :sym3 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariablesNonEmptyResultSet() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            assertQuery("ts\ts\n" +
                            "1970-01-02T23:00:00.000000Z\ta\n",
                    "select ts, s from t " +
                            "where s = :sym1 and s != :sym2 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    false);
        });
    }

    @Test
    public void testLatestByWithInAndNotInBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");
            assertQuery("ts\ts\n" +
                            "1970-01-02T22:00:00.000000Z\tb\n" +
                            "1970-01-02T23:00:00.000000Z\ta\n",
                    "select ts, s from t " +
                            "where s in ('a', 'b', 'c') and s != :sym " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValues() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            assertQuery("ts\ts\n" +
                            "1970-01-02T22:00:00.000000Z\tb\n" +
                            "1970-01-02T23:00:00.000000Z\ta\n" +
                            "1970-01-03T00:00:00.000000Z\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);

            bindVariableService.clear();
            bindVariableService.setStr("sym1", null);
            bindVariableService.setStr("sym2", "a");
            assertQuery("ts\ts\n" +
                            "1970-01-02T22:00:00.000000Z\tb\n" +
                            "1970-01-03T00:00:00.000000Z\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValuesFilter() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            bindVariableService.setStr("sym3", "d");
            assertQuery("ts\ts\n" +
                            "1970-01-02T14:00:00.000000Z\ta\n" +
                            "1970-01-02T16:00:00.000000Z\tb\n" +
                            "1970-01-02T19:00:00.000000Z\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) and s2 = :sym3 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValuesIndexed() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    "), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            assertQuery("ts\ts\n" +
                            "1970-01-02T22:00:00.000000Z\tb\n" +
                            "1970-01-02T23:00:00.000000Z\ta\n" +
                            "1970-01-03T00:00:00.000000Z\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);

            bindVariableService.clear();
            bindVariableService.setStr("sym1", null);
            bindVariableService.setStr("sym2", "a");
            assertQuery("ts\ts\n" +
                            "1970-01-02T22:00:00.000000Z\tb\n" +
                            "1970-01-03T00:00:00.000000Z\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValuesIndexedFilter() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    "), index(s), index(s2) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            bindVariableService.setStr("sym3", "d");
            assertQuery("ts\ts\n" +
                            "1970-01-02T14:00:00.000000Z\ta\n" +
                            "1970-01-02T16:00:00.000000Z\tb\n" +
                            "1970-01-02T19:00:00.000000Z\tc\n",
                    "select ts, s from t " +
                            "where s not in (:sym1, :sym2) and s2 = :sym3 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");
            assertQuery("ts\ts\n" +
                            "1970-01-02T22:00:00.000000Z\tb\n" +
                            "1970-01-02T23:00:00.000000Z\ta\n",
                    "select ts, s from t " +
                            "where s <> :sym " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesSingleValueIndexed() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L) ts from long_sequence(49)" +
                    "), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");
            assertQuery("ts\ts\n" +
                            "1970-01-02T22:00:00.000000Z\tb\n" +
                            "1970-01-02T23:00:00.000000Z\ta\n",
                    "select ts, s from t " +
                            "where s <> :sym " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestByWithStaticNonExistingSymbolOnNonEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab (ts TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n");
            compile("insert into tab\n" +
                    "select dateadd('h', -x::int, now()), rnd_symbol('ap', 'btc'), rnd_int(1,1000,0)\n" +
                    "from long_sequence(1000);");

            assertQuery("id\tv\tr_1M\n",
                    "with r as (select id, value v from tab where id = 'apc' LATEST ON ts PARTITION BY id),\n" +
                            "     rr as (select id, value v from tab where id = 'apc' and ts <= dateadd('d', -7, now())  LATEST ON ts PARTITION BY id)\n" +
                            "        select r.id, r.v, cast((r.v - rr.v) as float) r_1M\n" +
                            "        from r\n" +
                            "        join rr on id\n", null, false, false);
        });
    }

    @Test
    public void testLatestByWithSymbolOnEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab (ts TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n");

            assertQuery("id\tv\tr_1M\n",
                    "with r as (select id, value v from tab where id = 'apc' LATEST ON ts PARTITION BY id),\n" +
                            "        rr as (select id, value v from tab where id = 'apc' and ts <= dateadd('d', -7, now())  LATEST ON ts PARTITION BY id)\n" +
                            "        select r.id, r.v, cast((r.v - rr.v) as float) r_1M\n" +
                            "        from r\n" +
                            "        join rr on id\n", null, false, false);
        });
    }

    @Test
    public void testLatestWithFilterByDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("x\ts\tts\n" +
                            "44\tb\t1970-01-02T19:00:00.000000Z\n" +
                            "48\ta\t1970-01-02T23:00:00.000000Z\n",
                    "t " +
                            "where s in ('a', 'b') and x%2 = 0 " +
                            "latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestWithFilterByDoesNotNeedFullScanValueNotInSymbolTable() throws Exception {
        ff = new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
                // Query should not scan the first partition
                // all the latest values are in the second, third partition
                if (Chars.contains(name, "1970-01-01")) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        assertQuery13("x\ts\tts\n" +
                        "44\tb\t1970-01-02T19:00:00.000000Z\n" +
                        "48\ta\t1970-01-02T23:00:00.000000Z\n",
                "t " +
                        "where s in ('a', 'b', 'c') and x%2 = 0 " +
                        "latest on ts partition by s",
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "insert into t values (1000, 'c', '1970-01-02T20:00')",
                "x\ts\tts\n" +
                        "44\tb\t1970-01-02T19:00:00.000000Z\n" +
                        "1000\tc\t1970-01-02T20:00:00.000000Z\n" +
                        "48\ta\t1970-01-02T23:00:00.000000Z\n",
                true,
                true);
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
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("x\ts\tts\n" +
                            "48\ta\t1970-01-02T23:00:00.000000Z\n" +
                            "49\t\t1970-01-03T00:00:00.000000Z\n",
                    "t where s in ('a', null) latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testLatestWithoutSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Chars.contains(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            compile("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("x\ts\tts\n" +
                            "35\ta\t1970-01-02T10:00:00.000000Z\n" +
                            "47\tb\t1970-01-02T22:00:00.000000Z\n" +
                            "49\t\t1970-01-03T00:00:00.000000Z\n",
                    "t where x%2 = 1 latest on ts partition by s",
                    "ts",
                    true,
                    true);
        });
    }

    @Test
    public void testSymbolInPredicate_singleElement() throws Exception {
        assertMemoryLeak(() -> {
            String createStmt = "CREATE table trades(symbol symbol, side symbol, timestamp timestamp) timestamp(timestamp);";
            compiler.compile(createStmt, sqlExecutionContext);
            executeInsert("insert into trades VALUES ('BTC', 'buy', 1609459199000000);");
            String expected = "symbol\tside\ttimestamp\n" +
                    "BTC\tbuy\t2020-12-31T23:59:59.000000Z\n";
            String query = "SELECT * FROM trades\n" +
                    "WHERE symbol in ('BTC') and side in 'buy'\n" +
                    "LATEST ON timestamp PARTITION BY symbol;";
            assertSql(query, expected);
        });
    }

    private String selectDistinctSym() throws SqlException {
        StringSink sink = new StringSink();
        try (RecordCursorFactory factory = compiler.compile("select distinct s from t order by s limit " + 500, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    if (i++ > 0) {
                        sink.put(',');
                    }
                    sink.put('\'').put(record.getSym(0)).put('\'');
                }
            }
        }
        return sink.toString();
    }

    private void testLatestByPartitionBy(String partitionByType, String valueA, String valueB) throws SqlException {
        compile("create table forecasts " +
                "( when " + partitionByType + ", " +
                "version timestamp, " +
                "temperature double) timestamp(version) partition by day");
        compile("insert into forecasts values " +
                "  (" + valueA + ", '2020-05-02', 40), " +
                "  (" + valueA + ", '2020-05-03', 41), " +
                "  (" + valueA + ", '2020-05-04', 42), " +
                "  (" + valueB + ", '2020-05-01', 140), " +
                "  (" + valueB + ", '2020-05-03', 141), " +
                "  (" + valueB + ", '2020-05-05', 142)"
        );

        String query = "select when, version, temperature from forecasts latest on version partition by when";
        String expected = "when\tversion\ttemperature\n" +
                valueA.replaceAll("'|#", "") + "\t2020-05-04T00:00:00.000000Z\t42.0\n" +
                valueB.replaceAll("'|#", "") + "\t2020-05-05T00:00:00.000000Z\t142.0\n";

        assertQuery(expected, query, "version", true, true);
    }

    private void testLatestByWithJoin(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table r (symbol symbol, value long, ts timestamp)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", sqlExecutionContext);
            executeInsert("insert into r values ('xyz', 1, '2022-11-02T01:01:01')");
            compiler.compile("create table t (symbol symbol, value long, ts timestamp)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", sqlExecutionContext);
            executeInsert("insert into t values ('xyz', 42, '2022-11-02T01:01:01')");

            String query = "with r as (select symbol, value v from r where symbol = 'xyz' latest on ts partition by symbol),\n" +
                    " t as (select symbol, value v from t where symbol = 'xyz' latest on ts partition by symbol)\n" +
                    "select r.symbol, r.v subscribers, t.v followers\n" +
                    "from r\n" +
                    "join t on symbol";
            try (
                    RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()
            ) {
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
