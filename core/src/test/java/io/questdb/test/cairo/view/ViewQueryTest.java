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

package io.questdb.test.cairo.view;

import org.junit.Test;

public class ViewQueryTest extends AbstractViewTest {

    @Test
    public void testCreateViewAndSelectWithDeclare() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "DECLARE @x := k, @z := 'hohoho' select ts, @x, @z as red, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            String query = VIEW1;
            assertQueryAndPlan(
                    "ts\tk\tred\tv_max\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\thohoho\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\thohoho\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\thohoho\t8\n",
                    query,
                    "QUERY PLAN\n" +
                            "VirtualRecord\n" +
                            "  functions: [ts,k,'hohoho',v_max]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,k]\n" +
                            "      values: [max(v)]\n" +
                            "      filter: 5<v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n"
            );

            query = "DECLARE @x := 1, @y := 2 select ts, @x as one, @y * v_max from " + VIEW1 + " where v_max > 6";
            assertQueryAndPlan(
                    "ts\tone\tcolumn\n" +
                            "1970-01-01T00:01:10.000000Z\t1\t14\n" +
                            "1970-01-01T00:01:20.000000Z\t1\t16\n",
                    query,
                    null,
                    true,
                    false,
                    "QUERY PLAN\n" +
                            "VirtualRecord\n" +
                            "  functions: [ts,1,2*v_max]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [ts,v_max]\n" +
                            "        Filter filter: 6<v_max\n" +
                            "            Async Group By workers: 1\n" +
                            "              keys: [ts,k]\n" +
                            "              values: [max(v)]\n" +
                            "              filter: 5<v\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: table1\n"
            );
        });
    }

    @Test
    public void testSelectViewFields() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            String query = VIEW1;
            assertQueryAndPlan(
                    "ts\tk\tv_max\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    query,
                    "QUERY PLAN\n" +
                            "Async Group By workers: 1\n" +
                            "  keys: [ts,k]\n" +
                            "  values: [max(v)]\n" +
                            "  filter: 5<v\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: table1\n"
            );

            query = "select ts, v_max from " + VIEW1;
            assertQueryAndPlan(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:00.000000Z\t6\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    query,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,k]\n" +
                            "      values: [max(v)]\n" +
                            "      filter: 5<v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n"
            );
        });
    }

    @Test
    public void testSelectWithDeclare() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            String query = VIEW1;
            assertQueryAndPlan(
                    "ts\tk\tv_max\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    query,
                    "QUERY PLAN\n" +
                            "Async Group By workers: 1\n" +
                            "  keys: [ts,k]\n" +
                            "  values: [max(v)]\n" +
                            "  filter: 5<v\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: table1\n"
            );

            query = "DECLARE @x := 1, @y := 2 select ts, @x as one, @y * v_max from " + VIEW1 + " where v_max > 6";
            assertQueryAndPlan(
                    "ts\tone\tcolumn\n" +
                            "1970-01-01T00:01:10.000000Z\t1\t14\n" +
                            "1970-01-01T00:01:20.000000Z\t1\t16\n",
                    query,
                    null,
                    true,
                    false,
                    "QUERY PLAN\n" +
                            "VirtualRecord\n" +
                            "  functions: [ts,1,2*v_max]\n" +
                            "    Filter filter: 6<v_max\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [ts,k]\n" +
                            "          values: [max(v)]\n" +
                            "          filter: 5<v\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: table1\n"
            );
        });
    }

    @Test
    public void testSpecifyTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            assertQueryAndPlan(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "(select v1.ts, v1.v_max from " + VIEW1 + " v1 where v_max > 6) timestamp(ts)",
                    "ts",
                    true,
                    false,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    SelectedRecord\n" +
                            "        Filter filter: 6<v_max\n" +
                            "            Async Group By workers: 1\n" +
                            "              keys: [ts,k]\n" +
                            "              values: [max(v)]\n" +
                            "              filter: 5<v\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: table1\n"
            );
        });
    }

    @Test
    public void testViewJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            createView(VIEW2, query2, TABLE2);

            assertQueryAndPlan(
                    "ts\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\t5\n" +
                            "1970-01-01T00:01:00.000000Z\t6\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "select v1.ts, v_max from " + VIEW1 + " v1 join " + TABLE2 + " t2 on t2.v = v1.v_max",
                    null,
                    false,
                    true,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: t2.v=v1.v_max\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [ts,k]\n" +
                            "          values: [max(v)]\n" +
                            "          filter: 4<v\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: table1\n" +
                            "        Hash\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: table2\n"
            );

            assertQueryAndPlan(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "select t1.ts, v_max from " + TABLE1 + " t1 join (" + VIEW1 + " where v_max > 6) t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: t2.v_max=t1.v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n" +
                            "        Hash\n" +
                            "            SelectedRecord\n" +
                            "                Filter filter: 6<v_max\n" +
                            "                    Async Group By workers: 1\n" +
                            "                      keys: [ts,k]\n" +
                            "                      values: [max(v)]\n" +
                            "                      filter: 4<v\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: table1\n"
            );

            assertQueryAndPlan(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "with t2 as (" + VIEW1 + " where v_max > 6) select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max", "ts",
                    false,
                    true,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: t2.v_max=t1.v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n" +
                            "        Hash\n" +
                            "            SelectedRecord\n" +
                            "                Filter filter: 6<v_max\n" +
                            "                    Async Group By workers: 1\n" +
                            "                      keys: [ts,k]\n" +
                            "                      values: [max(v)]\n" +
                            "                      filter: 4<v\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: table1\n"
            );

            assertQueryAndPlan(
                    "ts\tk\tv_max\tts1\tk1\tv_max1\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\t5\t1970-01-01T00:00:50.000000Z\tk5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\t1970-01-01T00:01:00.000000Z\tk6\t6\n",
                    VIEW1 + " v11 join " + VIEW1 + " v12 on v_max where v12.v_max < 7",
                    null,
                    false,
                    true,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: v12.v_max=v11.v_max\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [ts,k]\n" +
                            "          values: [max(v)]\n" +
                            "          filter: 4<v\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: table1\n" +
                            "        Hash\n" +
                            "            Filter filter: v_max<7\n" +
                            "                Async Group By workers: 1\n" +
                            "                  keys: [ts,k]\n" +
                            "                  values: [max(v)]\n" +
                            "                  filter: 4<v\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: table1\n"
            );

            assertQueryAndPlan(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "with t2 as (" + VIEW1 + " v11 join " + VIEW1 + " v12 on v_max where v12.v_max > 6) select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Hash Join\n" +
                            "      condition: t2.v_max=t1.v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n" +
                            "        Hash\n" +
                            "            SelectedRecord\n" +
                            "                Hash Join Light\n" +
                            "                  condition: v12.v_max=v11.v_max\n" +
                            "                    Async Group By workers: 1\n" +
                            "                      keys: [ts,k]\n" +
                            "                      values: [max(v)]\n" +
                            "                      filter: 4<v\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: table1\n" +
                            "                    Hash\n" +
                            "                        Filter filter: 6<v_max\n" +
                            "                            Async Group By workers: 1\n" +
                            "                              keys: [ts,k]\n" +
                            "                              values: [max(v)]\n" +
                            "                              filter: 4<v\n" +
                            "                                PageFrame\n" +
                            "                                    Row forward scan\n" +
                            "                                    Frame forward scan on: table1\n"
            );
        });
    }

    @Test
    public void testViewUnion() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            createView(VIEW2, query2, TABLE2);

            assertQueryAndPlan(
                    "ts\tk2\tv_max\n" +
                            "1970-01-01T00:01:20.000000Z\tk2_8\t8\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\t5\n",
                    VIEW2 + " where v_max > 7 union " + VIEW1 + " where k = 'k5'",
                    null,
                    false,
                    false,
                    "QUERY PLAN\n" +
                            "Union\n" +
                            "    Filter filter: 7<v_max\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [ts,k2]\n" +
                            "          values: [max(v)]\n" +
                            "          filter: 6<v\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: table2\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,k]\n" +
                            "      values: [max(v)]\n" +
                            "      filter: (4<v and k='k5')\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n"
            );

            assertQueryAndPlan(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n" +
                            "1970-01-01T00:00:50.000000Z\t5\n",
                    "(select ts, v_max from " + VIEW2 + " where v_max > 6) union (select ts, v_max from " + VIEW1 + " where k = 'k5')",
                    null,
                    false,
                    false,
                    "QUERY PLAN\n" +
                            "Union\n" +
                            "    SelectedRecord\n" +
                            "        Filter filter: 6<v_max\n" +
                            "            Async Group By workers: 1\n" +
                            "              keys: [ts,k2]\n" +
                            "              values: [max(v)]\n" +
                            "              filter: 6<v\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: table2\n" +
                            "    SelectedRecord\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [ts,k]\n" +
                            "          values: [max(v)]\n" +
                            "          filter: (4<v and k='k5')\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: table1\n"
            );

            assertQueryAndPlan(
                    "ts\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\t5\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "with t2 as (" + VIEW2 + " where v_max > 7 union " + VIEW1 + " where k = 'k5') select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Hash Join\n" +
                            "      condition: t2.v_max=t1.v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n" +
                            "        Hash\n" +
                            "            Union\n" +
                            "                Filter filter: 7<v_max\n" +
                            "                    Async Group By workers: 1\n" +
                            "                      keys: [ts,k2]\n" +
                            "                      values: [max(v)]\n" +
                            "                      filter: 6<v\n" +
                            "                        PageFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: table2\n" +
                            "                Async Group By workers: 1\n" +
                            "                  keys: [ts,k]\n" +
                            "                  values: [max(v)]\n" +
                            "                  filter: (4<v and k='k5')\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: table1\n"
            );
        });
    }

    @Test
    public void testViewWithAlias() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            assertQueryAndPlan(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "select v1.ts, v1.v_max from " + VIEW1 + " v1 where v_max > 6",
                    null,
                    true,
                    false,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Filter filter: 6<v_max\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [ts,k]\n" +
                            "          values: [max(v)]\n" +
                            "          filter: 5<v\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: table1\n"
            );
        });
    }
}
