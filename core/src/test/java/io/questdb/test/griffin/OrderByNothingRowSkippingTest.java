/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * These tests cover select on unsorted table without designated timestamp or sort clause.
 * It was added to make sure sorting is not applied by accident.
 * Note: this might turn into a flaky test one day because if SQL doesn't specify order then any order is allowed
 * and limit clause makes little sense when table is neither ordered physically nor by an order by clause.
 */
public class OrderByNothingRowSkippingTest extends AbstractCairoTest {

    @Test
    public void testSelectAll() throws Exception {
        prepare_unordered_noTs_table();
        assertQuery(
                "l\n1\n4\n7\n9\n3\n6\n10\n8\n2\n5\n",
                "select l from tab",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSelectFirstN() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n1\n4\n7\n", "select l from tab limit 3", true);
    }

    @Test
    public void testSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n", "select l from tab limit 8,8", true);
    }

    @Test
    public void testSelectLastN() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n8\n2\n5\n", "select l from tab limit -3", true);
    }

    @Test
    public void testSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n", "select l from tab limit -8,-8", true);
    }

    @Test
    public void testSelectMiddleNfromBothDirections() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n3\n6\n", "select l from tab limit 4,-4", true);
    }

    @Test
    public void testSelectMiddleNfromEnd() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n7\n9\n3\n", "select l from tab limit -8,-5", true);
    }

    @Test
    public void testSelectMiddleNfromStart() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n6\n10\n8\n", "select l from tab limit 5,8", true);
    }

    @Test
    public void testSelectNbeforeStartReturnsEmptyResult() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n", "select l from tab limit -11,-15", true);
    }

    @Test
    public void testSelectNbeyondEndreturnsEmptyResult() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n", "select l from tab limit 11,12", true);
    }

    @Test
    public void testSelectNintersectingEnd() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n2\n5\n", "select l from tab limit 8,12", true);
    }

    @Test
    public void testSelectNintersectingStart() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n1\n4\n", "select l from tab limit -12,-8", true);
    }

    // table with x reflecting timestamp position  in descending order
    private void prepare_unordered_noTs_table() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP);");
        runInserts("insert into tab(l, ts) values (1, to_timestamp('2022-01-10T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );",
                "insert into tab(l, ts) values (4, to_timestamp('2022-01-07T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );",
                "insert into tab(l, ts) values (7, to_timestamp('2022-01-04T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );",
                "insert into tab(l, ts) values (9, to_timestamp('2022-01-02T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );",
                "insert into tab(l, ts) values (3, to_timestamp('2022-01-08T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );",
                "insert into tab(l, ts) values (6, to_timestamp('2022-01-05T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );",
                "insert into tab(l, ts) values (10,to_timestamp('2022-01-01T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );",
                "insert into tab(l, ts) values (8, to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );",
                "insert into tab(l, ts) values (2, to_timestamp('2022-01-09T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );",
                "insert into tab(l, ts) values (5, to_timestamp('2022-01-06T00:00:00', 'yyyy-MM-ddTHH:mm:ss') );");
    }

    private void runInserts(String... statements) throws Exception {
        assertMemoryLeak(() -> {
            for (String query : statements) {
                execute(query);
            }
        });
    }

    private void runQueries(String... queries) throws Exception {
        assertMemoryLeak(() -> {
            for (String query : queries) {
                execute(query);
            }
        });
    }
}
