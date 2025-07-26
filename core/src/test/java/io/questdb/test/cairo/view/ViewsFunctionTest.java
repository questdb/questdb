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
                    "QUERY PLAN\n" +
                            "views()\n"
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
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            "view2\tview1 where v_max > 6\tview2~4\t\tvalid\t2025-06-19T15:00:00.000000Z\n" +
                            "view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\t2025-06-19T15:00:00.000000Z\n" +
                            "view4\tselect date_trunc('hour', ts), avg(v) as v_avg from table2\tview4~6\t\tvalid\t2025-06-19T15:00:00.000000Z\n" +
                            "view3\tview2 where v_max > 7\tview3~5\t\tvalid\t2025-06-19T15:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    "QUERY PLAN\n" +
                            "views()\n"
            );

            execute("DROP VIEW " + VIEW2);
            execute("DROP VIEW " + VIEW4);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            "view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\t2025-06-19T15:00:00.000000Z\n" +
                            "view3\tview2 where v_max > 7\tview3~5\t\tvalid\t2025-06-19T15:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    "QUERY PLAN\n" +
                            "views()\n"
            );

            execute("DROP VIEW " + VIEW1);
            execute("DROP VIEW " + VIEW3);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n",
                    "views()",
                    null,
                    false,
                    false,
                    "QUERY PLAN\n" +
                            "views()\n"
            );
        });
    }
}
