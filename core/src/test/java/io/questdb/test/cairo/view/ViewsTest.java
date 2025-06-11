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

public class ViewsTest extends AbstractViewTest {

    @Test
    public void testViews() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            drainWalQueue();

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n",
                    "views()",
                    null,
                    false,
                    false,
                    "QUERY PLAN\n" +
                            "views()\n"
            );

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW1, query1);
            assertViewDefinitionFile(VIEW1, query1);
            assertViewStateFile(VIEW1);

            final String query2 = VIEW1 + " where v_max > 6";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW2, query2);
            assertViewDefinitionFile(VIEW2, query2);
            assertViewStateFile(VIEW2);

            final String query3 = VIEW2 + " where v_max > 7";
            execute("CREATE VIEW " + VIEW3 + " AS (" + query3 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW3, query3);
            assertViewDefinitionFile(VIEW3, query3);
            assertViewStateFile(VIEW3);

            final String query4 = "select date_trunc('hour', ts), avg(v) as v_avg from " + TABLE2;
            execute("CREATE VIEW " + VIEW4 + " AS (" + query4 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW4, query4);
            assertViewDefinitionFile(VIEW4, query4);
            assertViewStateFile(VIEW4);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n" +
                            "view2\tview1 where v_max > 6\tview2~4\t\tvalid\n" +
                            "view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\n" +
                            "view4\tselect date_trunc('hour', ts), avg(v) as v_avg from table2\tview4~6\t\tvalid\n" +
                            "view3\tview2 where v_max > 7\tview3~5\t\tvalid\n",
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
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n" +
                            "view1\tselect ts, k, max(v) as v_max from table1 where v > 4\tview1~3\t\tvalid\n" +
                            "view3\tview2 where v_max > 7\tview3~5\t\tvalid\n",
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
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n",
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
