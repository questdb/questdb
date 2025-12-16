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
    public void testCreateConstantView() throws Exception {
        final String query1 = "select 42 as col";
        createView(VIEW1, query1);

        assertQueryNoLeakCheck(
                """
                        col
                        42
                        """,
                VIEW1
        );
    }

    @Test
    public void testDeclareInViewDefinition() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "DECLARE @x := k, @z := 'hohoho' select ts, @x, @z as red, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            String query = VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tk\tred\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\thohoho\t6
                            1970-01-01T00:01:10.000000Z\tk7\thohoho\t7
                            1970-01-01T00:01:20.000000Z\tk8\thohoho\t8
                            """,
                    query,
                    """
                            QUERY PLAN
                            VirtualRecord
                              functions: [ts,k,'hohoho',v_max]
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  values: [max(v)]
                                  filter: 5<v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                            """,
                    VIEW1
            );

            query = "DECLARE @x := 1, @y := 2 select ts, @x as one, @y * v_max from " + VIEW1 + " where v_max > 6";
            assertQueryAndPlan(
                    """
                            ts\tone\tcolumn
                            1970-01-01T00:01:10.000000Z\t1\t14
                            1970-01-01T00:01:20.000000Z\t1\t16
                            """,
                    query,
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            VirtualRecord
                              functions: [ts,1,2*v_max]
                                VirtualRecord
                                  functions: [ts,v_max]
                                    Filter filter: 6<v_max
                                        Async Group By workers: 1
                                          keys: [ts]
                                          values: [max(v)]
                                          filter: 5<v
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testDeclareParameterizedView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "DECLARE @x := 6 select ts, v from " + TABLE1 + " where v = @x";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();
            assertViewDefinition(VIEW1, query1, TABLE1);
            assertViewDefinitionFile(VIEW1, query1);
            assertViewState(VIEW1);

            String query = VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            """,
                    query,
                    "ts",
                    true,
                    false,
                    """
                            QUERY PLAN
                            Async Filter workers: 1
                              filter: v=6
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            query = "DECLARE @x := 5 " + VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tv
                            1970-01-01T00:00:50.000000Z\t5
                            """,
                    query,
                    "ts",
                    true,
                    false,
                    """
                            QUERY PLAN
                            Async Filter workers: 1
                              filter: v=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testNonAsciiTableAndViewNames() throws Exception {
        assertMemoryLeak(() -> {
            final String TABLE1_1 = "Részvény_áíóúüűöő";
            final String TABLE1_2 = "RÉSZVÉNY_ÁÍÓÚÜŰÖŐ";
            final String TABLE2_1 = "Aкции_ягоды";
            final String TABLE2_2 = "AКЦИИ_ЯГОДЫ";
            final String VIEW1 = "股票";
            final String VIEW2 = "स्टॉक_के_शेयर";

            createTable(TABLE1_1);
            createTable(TABLE2_1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1_2 + " where v > 4";
            createView(VIEW1, query1, TABLE1_2);

            final String query2 = "select ts, k2, max(v) as v_max from '" + TABLE2_2 + "' where v > 6";
            createView(VIEW2, query2, TABLE2_2);

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "with t2 as (" + VIEW2 + " where v_max > 7 union " + VIEW1 + " where k = 'k5') select t1.ts, v_max from " + TABLE1_2 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: Részvény_áíóúüűöő
                                    Hash
                                        Union
                                            Filter filter: 7<v_max
                                                Async Group By workers: 1
                                                  keys: [ts,k2]
                                                  values: [max(v)]
                                                  filter: 6<v
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: Aкции_ягоды
                                            Async Group By workers: 1
                                              keys: [ts,k]
                                              values: [max(v)]
                                              filter: (4<v and k='k5')
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: Részvény_áíóúüűöő
                            """,
                    VIEW1, VIEW2
            );
        });
    }

    @Test
    public void testSampleByOrdeByForceDesignatedTimestampMix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE eq_equities_market_data (" +
                    "timestamp TIMESTAMP, " +
                    "symbol SYMBOL, " +
                    "venue SYMBOL, " +
                    "asks DOUBLE[][], bids DOUBLE[][]" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY");
            execute("INSERT INTO eq_equities_market_data VALUES " +
                    "(0, 'HSBC', 'LSE', ARRAY[ [11.4, 12], [10.3, 15] ], ARRAY[ [21.1, 31], [20.1, 21] ]), " +
                    "(1, 'HSBC', 'HKG', ARRAY[ [11.5, 13], [10.4, 14] ], ARRAY[ [21.2, 32], [20.2, 22] ]), " +
                    "(2, 'BAC', 'NYSE', ARRAY[ [11.6, 17], [10.5, 15] ], ARRAY[ [21.3, 33], [20.3, 23] ]), " +
                    "(3, 'HSBC', 'LSE', ARRAY[ [11.2, 30], [10.2, 16] ], ARRAY[ [21.4, 34], [20.4, 24] ]), " +
                    "(4, 'BAC', 'NYSE', ARRAY[ [11.4, 20], [10.4,  7] ], ARRAY[ [21.5, 35], [20.5, 25] ]), " +
                    "(5, 'MQG', 'ASX', ARRAY[ [16.0,  3], [15.0,  2] ], ARRAY[ [15.6, 36], [14.6, 26] ])"
            );
            drainWalQueue();

            createView(VIEW1, """
                    select timestamp, symbol, count(bids[1][1]) as total
                    from eq_equities_market_data
                    where symbol = 'HSBC'
                    sample by 10s
                    order by timestamp desc
                    """);

            createView(VIEW2, """
                    (view1 order by timestamp) timestamp(timestamp)
                    """);

            assertQueryAndCache(
                    """
                            timestamp\tcount
                            1970-01-01T00:00:00.000000Z\t1
                            """,
                    """
                            select timestamp, count() from view2
                            sample by 10m
                            """,
                    "timestamp",
                    false,
                    false
            );
        });
    }

    @Test
    public void testSelectFromViewWithDeclare() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            String query = VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    query,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: 5<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            query = "DECLARE @x := 1, @y := 2 select ts, @x as one, @y * v_max from " + VIEW1 + " where v_max > 6";
            assertQueryAndPlan(
                    """
                            ts\tone\tcolumn
                            1970-01-01T00:01:10.000000Z\t1\t14
                            1970-01-01T00:01:20.000000Z\t1\t16
                            """,
                    query,
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            VirtualRecord
                              functions: [ts,1,2*v_max]
                                Filter filter: 6<v_max
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: 5<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                            """,
                    VIEW1
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
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    query,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: 5<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            query = "select ts, v_max from " + VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    query,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  values: [max(v)]
                                  filter: 5<v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testSelectViewMixedCase() throws Exception {
        assertMemoryLeak(() -> {
            final String TABLE1_1 = "taBLe1";
            final String TABLE1_2 = "TABLe1";
            createTable(TABLE1_1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1_2 + " where v > 5";
            final String VIEW1_1 = "viEw1";
            final String VIEW1_2 = "ViEW1";
            final String VIEW1_3 = "vIeW1";
            createView(VIEW1_1, query1, TABLE1_2);

            String query = VIEW1_2;
            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    query,
                    "QUERY PLAN\n" +
                            "Async Group By workers: 1\n" +
                            "  keys: [ts,k]\n" +
                            "  values: [max(v)]\n" +
                            "  filter: 5<v\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: " + TABLE1_1 + "\n",
                    VIEW1
            );

            query = "select ts, v_max from " + VIEW1_3;
            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    query,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,k]\n" +
                            "      values: [max(v)]\n" +
                            "      filter: 5<v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: " + TABLE1_1 + "\n",
                    VIEW1
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
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "(select v1.ts, v1.v_max from " + VIEW1 + " v1 where v_max > 6) timestamp(ts)",
                    "ts",
                    true,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                SelectedRecord
                                    Filter filter: 6<v_max
                                        Async Group By workers: 1
                                          keys: [ts,k]
                                          values: [max(v)]
                                          filter: 5<v
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testViewAllowNonDetermisticFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // rnd_byte() is technically a non-deterministic function
            String view = "select * from " + TABLE1 + " where rnd_byte() >= 0";

            createView(VIEW1, view, TABLE1);

            assertQueryNoLeakCheck("""
                            ts	k	k2	v
                            1970-01-01T00:00:00.000000Z	k0	k2_0	0
                            1970-01-01T00:00:10.000000Z	k1	k2_1	1
                            1970-01-01T00:00:20.000000Z	k2	k2_2	2
                            1970-01-01T00:00:30.000000Z	k3	k2_3	3
                            1970-01-01T00:00:40.000000Z	k4	k2_4	4
                            1970-01-01T00:00:50.000000Z	k5	k2_5	5
                            1970-01-01T00:01:00.000000Z	k6	k2_6	6
                            1970-01-01T00:01:10.000000Z	k7	k2_7	7
                            1970-01-01T00:01:20.000000Z	k8	k2_8	8
                            """,
                    VIEW1, "ts", true, sqlExecutionContext);
        });
    }

    @Test
    public void testViewFilterPushedDownToTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: 5<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            """,
                    VIEW1 + " where k = 'k6'",
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: (5<v and k='k6')
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1 + " where k in ('k6', 'k8')",
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: (5<v and k in [k6,k8])
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    "(" + VIEW1 + " where k in ('k6', 'k8')) where k = 'k8'",
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: (5<v and k in [k6,k8] and k='k8')
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
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

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "select v1.ts, v_max from " + VIEW1 + " v1 join " + TABLE2 + " t2 on t2.v = v1.v_max",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: t2.v=v1.v_max
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: 4<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table2
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "select t1.ts, v_max from " + TABLE1 + " t1 join (" + VIEW1 + " where v_max > 6) t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        SelectedRecord
                                            Filter filter: 6<v_max
                                                Async Group By workers: 1
                                                  keys: [ts,k]
                                                  values: [max(v)]
                                                  filter: 4<v
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "with t2 as (" + VIEW1 + " where v_max > 6) select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max", "ts",
                    false,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        SelectedRecord
                                            Filter filter: 6<v_max
                                                Async Group By workers: 1
                                                  keys: [ts,k]
                                                  values: [max(v)]
                                                  filter: 4<v
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max\tts1\tk1\tv_max1
                            1970-01-01T00:00:50.000000Z\tk5\t5\t1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6\t1970-01-01T00:01:00.000000Z\tk6\t6
                            """,
                    VIEW1 + " v11 join " + VIEW1 + " v12 on v_max where v12.v_max < 7",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: v12.v_max=v11.v_max
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: 4<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                                    Hash
                                        Filter filter: v_max<7
                                            Async Group By workers: 1
                                              keys: [ts,k]
                                              values: [max(v)]
                                              filter: 4<v
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "with t2 as (" + VIEW1 + " v11 join " + VIEW1 + " v12 on v_max where v12.v_max > 6) select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        SelectedRecord
                                            Hash Join Light
                                              condition: v12.v_max=v11.v_max
                                                Async Group By workers: 1
                                                  keys: [ts,k]
                                                  values: [max(v)]
                                                  filter: 4<v
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: table1
                                                Hash
                                                    Filter filter: 6<v_max
                                                        Async Group By workers: 1
                                                          keys: [ts,k]
                                                          values: [max(v)]
                                                          filter: 4<v
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: table1
                            """,
                    VIEW1
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
                    """
                            ts\tk2\tv_max
                            1970-01-01T00:01:20.000000Z\tk2_8\t8
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            """,
                    VIEW2 + " where v_max > 7 union " + VIEW1 + " where k = 'k5'",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            Union
                                Filter filter: 7<v_max
                                    Async Group By workers: 1
                                      keys: [ts,k2]
                                      values: [max(v)]
                                      filter: 6<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table2
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  values: [max(v)]
                                  filter: (4<v and k='k5')
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                            """,
                    VIEW1, VIEW2
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            1970-01-01T00:00:50.000000Z\t5
                            """,
                    "(select ts, v_max from " + VIEW2 + " where v_max > 6) union (select ts, v_max from " + VIEW1 + " where k = 'k5')",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            Union
                                SelectedRecord
                                    Filter filter: 6<v_max
                                        Async Group By workers: 1
                                          keys: [ts,k2]
                                          values: [max(v)]
                                          filter: 6<v
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: table2
                                SelectedRecord
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: (4<v and k='k5')
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                            """,
                    VIEW1, VIEW2
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "with t2 as (" + VIEW2 + " where v_max > 7 union " + VIEW1 + " where k = 'k5') select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        Union
                                            Filter filter: 7<v_max
                                                Async Group By workers: 1
                                                  keys: [ts,k2]
                                                  values: [max(v)]
                                                  filter: 6<v
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: table2
                                            Async Group By workers: 1
                                              keys: [ts,k]
                                              values: [max(v)]
                                              filter: (4<v and k='k5')
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: table1
                            """,
                    VIEW1, VIEW2
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
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "select v1.ts, v1.v_max from " + VIEW1 + " v1 where v_max > 6",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Filter filter: 6<v_max
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: 5<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }
}
