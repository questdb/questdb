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

public class ViewsFunctionTest extends AbstractViewTest {

    @Test
    public void testViewsConsistentWithMatViewsAndTablesCommands() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1750345200000000L);

            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = VIEW1 + " where v_max > 6";
            createView(VIEW2, query2, TABLE1, VIEW1);

            final String query3 = "select ts, k, max(v) as v_max from " + TABLE1 + " sample by 1m";
            createMatView(VIEW3, query3);

            final String query4 = "select ts, avg(v) as v_avg from " + TABLE2 + " sample by 15m";
            createMatView(VIEW4, query4);

            assertQueryAndPlan(
                    """
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time
                            view2\tview1 where v_max > 6\tview2~4\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\t2025-06-19T15:00:00.000000Z
                            """,
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            assertQueryAndPlan(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_period_hi\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            view4\timmediate\ttable2\t2025-06-19T15:00:00.000000Z\t2025-06-19T15:00:00.000000Z\tselect ts, avg(v) as v_avg from table2 sample by 15m\tview4~6\t\tvalid\t\t9\t9\t0\t\t\t\t0\t\t0\t\t0\t
                            view3\timmediate\ttable1\t2025-06-19T15:00:00.000000Z\t2025-06-19T15:00:00.000000Z\tselect ts, k, max(v) as v_max from table1 sample by 1m\tview3~5\t\tvalid\t\t9\t9\t0\t\t\t\t0\t\t0\t\t0\t
                            """,
                    "materialized_views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            materialized_views()
                            """
            );

            assertQueryAndPlan(
                    """
                            id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\tttlValue\tttlUnit\ttable_type
                            6\tview4\tts\tDAY\t1000\t-1\ttrue\tview4~6\tfalse\t0\tHOUR\tM
                            5\tview3\tts\tDAY\t1000\t-1\ttrue\tview3~5\tfalse\t0\tHOUR\tM
                            4\tview2\t\tN/A\t-1\t-1\ttrue\tview2~4\tfalse\t0\tHOUR\tV
                            3\tview1\t\tN/A\t-1\t-1\ttrue\tview1~3\tfalse\t0\tHOUR\tV
                            2\ttable2\tts\tDAY\t1000\t300000000\ttrue\ttable2~2\tfalse\t0\tHOUR\tT
                            1\ttable1\tts\tDAY\t1000\t300000000\ttrue\ttable1~1\tfalse\t0\tHOUR\tT
                            """,
                    "tables()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            tables()
                            """
            );

            assertQueryAndPlan(
                    """
                            table_name
                            view4
                            view3
                            view2
                            view1
                            table2
                            table1
                            """,
                    "all_tables()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            all_tables()
                            """
            );
        });
    }

    @Test
    public void testViewsStatement() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1750345200000000L);

            createTable(TABLE1);
            createTable(TABLE2);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = VIEW1 + " where v_max > 6";
            createView(VIEW2, query2, TABLE1, VIEW1);

            final String query3 = VIEW2 + " where v_max > 7";
            createView(VIEW3, query3, TABLE1, VIEW1, VIEW2);

            final String query4 = "select date_trunc('hour', ts), avg(v) as v_avg from " + TABLE2;
            createView(VIEW4, query4, TABLE2);

            assertQueryAndPlan(
                    """
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time
                            view2\tview1 where v_max > 6\tview2~4\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view4\tselect date_trunc('hour', ts), avg(v) as v_avg from table2\tview4~6\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view3\tview2 where v_max > 7\tview3~5\t\tvalid\t2025-06-19T15:00:00.000000Z
                            """,
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            execute("DROP VIEW " + VIEW2);
            execute("DROP VIEW " + VIEW4);

            assertQueryAndPlan(
                    """
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time
                            view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view3\tview2 where v_max > 7\tview3~5\t\tvalid\t2025-06-19T15:00:00.000000Z
                            """,
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            execute("DROP VIEW " + VIEW1);
            execute("DROP VIEW " + VIEW3);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );
        });
    }
}
