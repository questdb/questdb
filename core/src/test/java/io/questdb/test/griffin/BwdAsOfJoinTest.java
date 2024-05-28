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
