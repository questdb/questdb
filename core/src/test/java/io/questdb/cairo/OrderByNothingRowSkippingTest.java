/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

/*
 * This class tests select on unsorted table without designated timestamp or sort clause .
 * It was added to make sure sorting is not applied by accident.
 * Note: this might turn into a flaky test one day because if SQL doesn't specify order then any order is allowed
 * and limit clause makes little sense when table is neither ordered physically nor by an order by clause
 * */
public class OrderByNothingRowSkippingTest extends AbstractGriffinTest {

    @Test
    public void test_unorderedNoDesignatedTsTable_select_all() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n1\n4\n7\n9\n3\n6\n10\n8\n2\n5\n", "select l from tab");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_first_N() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n1\n4\n7\n", "select l from tab limit 3");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_middle_N_from_start() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n6\n10\n8\n", "select l from tab limit 5,8");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_N_beyond_end_returns_empty_result() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n", "select l from tab limit 11,12");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_N_before_start_returns_empty_result() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n", "select l from tab limit -11,-15");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_last_N() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n8\n2\n5\n", "select l from tab limit -3");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_middle_N_from_end() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n7\n9\n3\n", "select l from tab limit -8,-5");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_middle_N_from_both_directions() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n3\n6\n", "select l from tab limit 4,-4");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_first_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n", "select l from tab limit 8,8");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_last_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n", "select l from tab limit -8,-8");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_N_intersecting_end() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n2\n5\n", "select l from tab limit 8,12");
    }

    @Test
    public void test_unorderedNoDesignatedTsTable_select_N_intersecting_start() throws Exception {
        prepare_unordered_noTs_table();

        assertQuery("l\n1\n4\n", "select l from tab limit -12,-8");
    }

    //table with x reflecting timestamp position  in descending order
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
                executeInsert(query);
            }
        });
    }

    private void runQueries(String... queries) throws Exception {
        assertMemoryLeak(() -> {
            for (String query : queries) {
                compiler.compile(query, sqlExecutionContext);
            }
        });
    }

    private void assertQuery(String expected, String query) throws Exception {
        assertQuery(expected,
                query,
                null, null, true, false, true);
    }
}
