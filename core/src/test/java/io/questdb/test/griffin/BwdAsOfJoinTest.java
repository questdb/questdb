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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class BwdAsOfJoinTest extends AbstractCairoTest {

    /*
        SELECT * FROM t1
        "ts","i","s"
        "2022-10-05T08:15:00.000000Z",0,"a"
        "2022-10-05T08:17:00.000000Z",1,"b"
        "2022-10-05T08:21:00.000000Z",2,"c"
        "2022-10-10T01:01:00.000000Z",3,"d"

        SELECT * FROM t2
        "ts","i","s"
        "2022-10-05T08:18:00.000000Z",4,"e"
        "2022-10-05T08:19:00.000000Z",5,"f"
        "2023-10-05T09:00:00.000000Z",6,"g"
        "2023-10-06T01:00:00.000000Z",7,"h"
     */


    static final String ORACLE = "ts\ti\ts\tts1\ti1\ts1\n" +
            "2023-10-06T01:00:00.000000Z\t7\th\t2022-10-10T01:01:00.000000Z\t3\td\n" +
            "2023-10-05T09:00:00.000000Z\t6\tg\t2022-10-10T01:01:00.000000Z\t3\td\n" +
            "2022-10-05T08:19:00.000000Z\t5\tf\t2022-10-05T08:17:00.000000Z\t1\tb\n" +
            "2022-10-05T08:18:00.000000Z\t4\te\t2022-10-05T08:17:00.000000Z\t1\tb\n";

    public void createTables() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");

            insert("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a'), ('2022-10-05T08:17:00.000000Z', 1, 'b'), ('2022-10-05T08:21:00.000000Z', 2, 'c'), ('2022-10-10T01:01:00.000000Z', 3, 'd');");
            insert("INSERT INTO t2 values ('2022-10-05T08:18:00.000000Z', 4, 'e'), ('2022-10-05T08:19:00.000000Z', 5, 'f'), ('2023-10-05T09:00:00.000000Z', 6, 'g'), ('2023-10-06T01:00:00.000000Z', 7, 'h');");
        });
    }

    @Test
    public void testBwdAsOfJoinAliasDuplication() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "CREATE TABLE fx_rate (" +
                            "    ts TIMESTAMP, " +
                            "    code SYMBOL CAPACITY 128 NOCACHE, " +
                            "    rate INT" +
                            ") timestamp(ts)",
                    sqlExecutionContext
            );
            insert("INSERT INTO fx_rate values ('2022-10-05T04:00:00.000000Z', '1001', 10);");

            ddl(
                    "CREATE TABLE trades (" +
                            "    ts TIMESTAMP, " +
                            "    price INT, " +
                            "    qty INT, " +
                            "    flag INT, " +
                            "    fx_rate_code SYMBOL CAPACITY 128 NOCACHE" +
                            ") timestamp(ts);",
                    sqlExecutionContext
            );
            insert("INSERT INTO trades values ('2022-10-05T08:15:00.000000Z', 100, 500, 0, '1001');");
            insert("INSERT INTO trades values ('2022-10-05T08:16:00.000000Z', 100, 500, 1, '1001');");
            insert("INSERT INTO trades values ('2022-10-05T08:16:00.000000Z', 100, 500, 2, '1001');");

            String query =
                    "SELECT\n" +
                            "  SUM(CASE WHEN t.flag = 0 THEN 0.9 * (t.price * f.rate) ELSE 0.0 END)," +
                            "  SUM(CASE WHEN t.flag = 1 THEN 0.7 * (t.price * f.rate) ELSE 0.0 END)," +
                            "  SUM(CASE WHEN t.flag = 2 THEN 0.2 * (t.price * f.rate) ELSE 0.0 END)" +
                            "FROM  " +
                            "  (select * from trades order by ts desc) t " +
                            "ASOF JOIN (select * from fx_rate order by ts desc) f on f.code = t.fx_rate_code";

            String expected = "SUM\tSUM1\tSUM2\n" +
                    "900.0\t700.0\t200.0\n";

            assertPlanNoLeakCheck(query, "GroupBy vectorized: false\n" +
                    "  values: [sum(case([0.9*price*rate,0.0,flag])),sum(case([0.7*price*rate,0.0,flag])),sum(case([0.2*price*rate,0.0,flag]))]\n" +
                    "    SelectedRecord\n" +
                    "        AsOf Join Light\n" +
                    "          condition: f.code=t.fx_rate_code\n" +
                    "            SelectedRecord\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: trades\n" +
                    "            SelectedRecord\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: fx_rate\n");

            printSqlResult(expected, query, null, false, true);
        });
    }

    @Test
    public void testBwdAsOfJoinBasicExample() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = "(select ts, i, s, from t2 order by ts desc) \n" +
                    "asof join\n" +
                    "(select ts, i, s, from t1 order by ts desc) ";

            assertPlanNoLeakCheck(query, "SelectedRecord\n" +
                    "    Bwd AsOf Join\n" +
                    "        DataFrame\n" +
                    "            Row backward scan\n" +
                    "            Frame backward scan on: t2\n" +
                    "        DataFrame\n" +
                    "            Row backward scan\n" +
                    "            Frame backward scan on: t1\n");

            assertQuery(ORACLE, query, "ts###desc", false, true);
        });
    }

    @Test
    public void testBwdAsOfJoinCombinedWithInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t1 as (select x as id, cast(x as timestamp) ts from long_sequence(5)) timestamp(ts) partition by day;");
            ddl("create table t2 as (select x as id, cast(x as timestamp) ts from long_sequence(5)) timestamp(ts) partition by day;");
            ddl("create table t3 (id long, ts timestamp) timestamp(ts) partition by day;");

            final String query = "SELECT *\n" +
                    "FROM (\n" +
                    "  (t1 INNER JOIN t2 ON id) \n" +
                    "  ASOF JOIN (SELECT * FROM t3 ORDER BY ts DESC) ON id\n" +
                    "ORDER BY t1.ts DESC" +
                    ");";
            final String expected = "id\tts\tid1\tts1\tid2\tts2\n" +
                    "5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\tnull\t\n" +
                    "4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\tnull\t\n" +
                    "3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\tnull\t\n" +
                    "2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\tnull\t\n" +
                    "1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\tnull\t\n";

            assertPlanNoLeakCheck(query, "SelectedRecord\n" +
                    "    AsOf Join Light\n" +
                    "      condition: _xQdbA3.id=t1.id\n" +
                    "        Hash Join Light\n" +
                    "          condition: t2.id=t1.id\n" +
                    "            DataFrame\n" +
                    "                Row backward scan\n" +
                    "                Frame backward scan on: t1\n" +
                    "            Hash\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: t2\n" +
                    "        Sort light\n" +
                    "          keys: [ts desc]\n" +
                    "            DataFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: t3\n");

            printSqlResult(expected, query, "ts###desc", false, true);
        });
    }

    @Test
    public void testBwdAsOfJoinInvalidSyntax() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = "select * from t2\n" +
                    "asof join t1\n" +
                    "order by ts desc";

            assertException(query, 17, "mismatched ordering of tables in time series join. left: DESC right: ASC");
        });
    }

    @Test
    public void testBwdAsOfJoinOracle() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = "select * from (\n" +
                    "select * from t2\n" +
                    "asof join t1\n" +
                    ") order by ts desc";

            assertPlanNoLeakCheck(query, "Sort\n" +
                    "  keys: [ts desc]\n" +
                    "    SelectedRecord\n" +
                    "        AsOf Join Fast Scan\n" +
                    "            DataFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: t2\n" +
                    "            DataFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: t1\n");

            assertQuery(ORACLE, query, "ts###desc", true, true);
        });
    }

    @Test
    public void testBwdAsofJoinMultipleOrderings() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = "select * from t2\n" +
                    "asof join t1\n" +
                    "order by ts desc, t1.ts desc";

            assertPlanNoLeakCheck(query, "Sort\n" +
                    "  keys: [ts desc, ts1 desc]\n" +
                    "    SelectedRecord\n" +
                    "        AsOf Join Fast Scan\n" +
                    "            DataFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: t2\n" +
                    "            DataFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: t1\n");

            assertQuery(ORACLE, query, "ts###desc", true, true);
        });
    }
}
