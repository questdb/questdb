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

import io.questdb.griffin.SqlException;
import org.junit.Test;

public class ViewInvalidationTest extends AbstractViewTest {

    @Test
    public void testBrokenViewsAreInvalidatedRecursively() throws Exception {
        String viewQuery1 = "select ts, k, max(v) as value from " + TABLE1 + " where v > 4";
        String viewQuery2 = "select ts, k, min(v) as value from " + TABLE2 + " where v > 6";
        String viewQuery3 = VIEW1 + " union " + VIEW2;
        String breakingSql = "RENAME TABLE " + TABLE1 + " TO " + TABLE3;
        String fixingSql = "RENAME TABLE " + TABLE3 + " TO " + TABLE1;
        String expectedErrorMessage = "table does not exist [table=" + TABLE1 + "]";

        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            // create views
            createView(VIEW1, viewQuery1);
            createView(VIEW2, viewQuery2);
            createView(VIEW3, viewQuery3);

            compileView(VIEW1);
            compileView(VIEW2);
            compileView(VIEW3);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n" +
                            "view1\t" + viewQuery1 + "\tview1~3\t\tvalid\n" +
                            "view2\t" + viewQuery2 + "\tview2~4\t\tvalid\n" +
                            "view3\t" + viewQuery3 + "\tview3~5\t\tvalid\n",
                    "views() order by view_name",
                    null,
                    true,
                    false,
                    "QUERY PLAN\n" +
                            "Sort\n" +
                            "  keys: [view_name]\n" +
                            "    views()\n"
            );

            // breaking views by renaming the table
            execute(breakingSql);
            drainWalQueue();

            // automatic view invalidation is switched off in test, so have to query the view
            // to be able to detect that it does not work anymore
            // it is enough to detect the problem with VIEW1, because we invalidate all dependent
            // views recursively too, and VIEW3 depends on VIEW1
            detectInvalidView(expectedErrorMessage);
            drainViewQueue();
            drainWalQueue();

            assertViewDefinition(VIEW1, viewQuery1);
            assertViewDefinitionFile(VIEW1, viewQuery1);
            assertViewStateFile(VIEW1, expectedErrorMessage);

            assertViewDefinition(VIEW2, viewQuery2);
            assertViewDefinitionFile(VIEW2, viewQuery2);
            assertViewStateFile(VIEW2);

            assertViewDefinition(VIEW3, viewQuery3);
            assertViewDefinitionFile(VIEW3, viewQuery3);
            assertViewStateFile(VIEW3, expectedErrorMessage);

            compileView(VIEW1, expectedErrorMessage);
            compileView(VIEW2);
            compileView(VIEW3, expectedErrorMessage);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n" +
                            "view1\tselect ts, k, max(v) as value from table1 where v > 4\tview1~3\ttable does not exist [table=table1]\tinvalid\n" +
                            "view2\tselect ts, k, min(v) as value from table2 where v > 6\tview2~4\t\tvalid\n" +
                            "view3\tview1 union view2\tview3~5\ttable does not exist [table=table1]\tinvalid\n",
                    "views() order by view_name",
                    null,
                    true,
                    false,
                    "QUERY PLAN\n" +
                            "Sort\n" +
                            "  keys: [view_name]\n" +
                            "    views()\n"
            );

            // fixing views by rename the table back to the original name
            execute(fixingSql);
            drainWalQueue();

            // automatic view reset is switched off in test, so have to compile
            // the view manually to be able to fix it.
            // it is enough to compile VIEW1, because VIEW3 is dependent on it, and if
            // VIEW1 compiles successfully, its children will be compiled recursively too
            fixInvalidView();
            drainViewQueue();
            drainWalQueue();

            assertViewDefinition(VIEW1, viewQuery1);
            assertViewDefinitionFile(VIEW1, viewQuery1);
            assertViewStateFile(VIEW1);

            assertViewDefinition(VIEW2, viewQuery2);
            assertViewDefinitionFile(VIEW2, viewQuery2);
            assertViewStateFile(VIEW2);

            assertViewDefinition(VIEW3, viewQuery3);
            assertViewDefinitionFile(VIEW3, viewQuery3);
            assertViewStateFile(VIEW3);

            compileView(VIEW1);
            compileView(VIEW2);
            compileView(VIEW3);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n" +
                            "view1\tselect ts, k, max(v) as value from table1 where v > 4\tview1~3\t\tvalid\n" +
                            "view2\tselect ts, k, min(v) as value from table2 where v > 6\tview2~4\t\tvalid\n" +
                            "view3\tview1 union view2\tview3~5\t\tvalid\n",
                    "views() order by view_name",
                    null,
                    true,
                    false,
                    "QUERY PLAN\n" +
                            "Sort\n" +
                            "  keys: [view_name]\n" +
                            "    views()\n"
            );
        });
    }

    @Test
    public void testDroppedColumnInsideFunctionInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(sqrt(v)) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " DROP COLUMN v",
                "alter table " + TABLE1 + " add column v long",
                "Invalid column: v"
        );
    }

    @Test
    public void testDroppedColumnInsideOperationInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(5 * v) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " DROP COLUMN v",
                "alter table " + TABLE1 + " add column v long",
                "Invalid column: v"
        );
    }

    @Test
    public void testDroppedColumnInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " DROP COLUMN k",
                "alter table " + TABLE1 + " add column k symbol",
                "Invalid column: k"
        );
    }

    @Test
    public void testDroppedTableInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4",
                "DROP TABLE " + TABLE1,
                "create table if not exists " + TABLE1 +
                        " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                        " timestamp(ts) partition by day wal",
                "table does not exist [table=" + TABLE1 + "]"
        );
    }

    @Test
    public void testRenamedColumnInsideFunctionInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(sqrt(v)) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN v to v_renamed",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN v_renamed to v",
                "Invalid column: v"
        );
    }

    @Test
    public void testRenamedColumnInsideOperationInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(5 * v) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN v to v_renamed",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN v_renamed to v",
                "Invalid column: v"
        );
    }

    @Test
    public void testRenamedColumnInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN k to k_renamed",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN k_renamed to k",
                "Invalid column: k"
        );
    }

    @Test
    public void testRenamedTableInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4",
                "RENAME TABLE " + TABLE1 + " TO " + TABLE3,
                "RENAME TABLE " + TABLE3 + " TO " + TABLE1,
                "table does not exist [table=" + TABLE1 + "]"
        );
    }

    private void testViewInvalidated(String viewQuery, String breakingSql, String fixingSql, String expectedErrorMessage) throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            // creating view
            createView(VIEW1, viewQuery);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n" +
                            "view1\t" + viewQuery + "\tview1~3\t\tvalid\n",
                    "views()",
                    null,
                    false,
                    false,
                    "QUERY PLAN\n" +
                            "views()\n"
            );

            compileView(VIEW1);

            // breaking view
            execute(breakingSql);
            drainWalQueue();

            detectInvalidView(expectedErrorMessage);
            drainViewQueue();
            drainWalQueue();

            assertViewDefinition(VIEW1, viewQuery);
            assertViewDefinitionFile(VIEW1, viewQuery);
            assertViewStateFile(VIEW1, expectedErrorMessage);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n" +
                            "view1\t" + viewQuery + "\tview1~3\t" + expectedErrorMessage + "\tinvalid\n",
                    "views()",
                    null,
                    false,
                    false,
                    "QUERY PLAN\n" +
                            "views()\n"
            );

            compileView(VIEW1, expectedErrorMessage);

            // fixing view
            execute(fixingSql);
            drainWalQueue();

            fixInvalidView();
            drainViewQueue();
            drainWalQueue();

            assertViewDefinition(VIEW1, viewQuery);
            assertViewDefinitionFile(VIEW1, viewQuery);
            assertViewStateFile(VIEW1);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n" +
                            "view1\t" + viewQuery + "\tview1~3\t\tvalid\n",
                    "views()",
                    null,
                    false,
                    false,
                    "QUERY PLAN\n" +
                            "views()\n"
            );

            compileView(VIEW1);
        });
    }

    protected void detectInvalidView(String expectedErrorMessage) {
        // no-op
        // invalid views are automatically detected by default
    }

    protected void fixInvalidView() throws SqlException {
        // no-op
        // invalid views are automatically fixed by default
    }
}
