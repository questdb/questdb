/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

/**
 * A view's persisted dependency map must list every base table the view can ultimately scan,
 * including tables reachable only through a sub-query embedded in an expression
 * (e.g. {@code WHERE col IN (SELECT ... FROM t)}). Missing entries later trip the per-column
 * authorization of the scan, which looks the table up in that map.
 */
public class ViewDependencySubqueryTest extends AbstractViewTest {

    @Test
    public void testTableReachedOnlyViaInSubqueryIsCaptured() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            // table2 is referenced ONLY inside the WHERE .. IN sub-query
            createView(
                    VIEW1,
                    "SELECT ts, k, v FROM " + TABLE1 + " WHERE k IN (SELECT k FROM " + TABLE2 + ")",
                    TABLE1, TABLE2
            );
        });
    }

    @Test
    public void testTableReachedOnlyViaNotInSubqueryIsCaptured() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            createView(
                    VIEW1,
                    "SELECT ts, k, v FROM " + TABLE1 + " WHERE k NOT IN (SELECT k FROM " + TABLE2 + ")",
                    TABLE1, TABLE2
            );
        });
    }

    @Test
    public void testSubqueryInsideCteIsCaptured() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            createTable(TABLE3);
            // table3 is reached only through a sub-query nested two levels deep inside a CTE
            createView(
                    VIEW1,
                    "WITH cte AS (" +
                            "  SELECT ts, k, v FROM " + TABLE1 +
                            "  WHERE k IN (SELECT k FROM " + TABLE2 +
                            "              WHERE k2 IN (SELECT k2 FROM " + TABLE3 + "))" +
                            ") SELECT * FROM cte",
                    TABLE1, TABLE2, TABLE3
            );
        });
    }

    @Test
    public void testSubqueryReachingTableViaAnotherViewIsCaptured() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            // a view whose own body reaches table2 only via a sub-query
            execute("CREATE VIEW " + VIEW2 + " AS (SELECT ts, k, v FROM " + TABLE1 +
                    " WHERE k IN (SELECT k FROM " + TABLE2 + "))");
            drainWalAndViewQueues();
            // the dependent view expands view2; table2 (the hidden base) must still surface
            createView(
                    VIEW1,
                    "SELECT k, count() c FROM " + VIEW2 + " GROUP BY k",
                    VIEW2, TABLE1, TABLE2
            );
        });
    }

    @Test
    public void testReadingViewWithSubqueryDependencyDoesNotThrow() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            createView(
                    VIEW1,
                    "SELECT ts, k, v FROM " + TABLE1 + " WHERE k IN (SELECT k FROM " + TABLE2 + ")",
                    TABLE1, TABLE2
            );
            // sanity: the view reads back cleanly (the per-column scan authorization can resolve every base table)
            assertQuery("SELECT * FROM " + VIEW1 + " WHERE v < 0")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tk\tv\n");
        });
    }
}
