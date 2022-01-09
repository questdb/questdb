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

/**
 * This class tests row skipping (in ascending order) optimizations for tables:
 * - with and without designated timestamps,
 * - non-partitioned and partitioned .
 */
public class OrderByDescRowSkippingTest extends AbstractGriffinTest {

    //normal table without designated timestamp with rows (including duplicates) in descending order
    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_all() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n10\n10\n9\n9\n8\n8\n7\n7\n6\n6\n5\n5\n4\n4\n3\n3\n2\n2\n1\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_first_N() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n10\n10\n9\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_middle_N_from_start() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n3\n2\n2\n", "select l from tab order by ts desc limit 15,18");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_N_beyond_end_returns_empty_result() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n", "select l from tab order by ts desc limit 21,22");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_N_before_start_returns_empty_result() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n", "select l from tab order by ts desc limit -25,-21");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_last_N() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n2\n1\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_middle_N_from_end() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n9\n9\n8\n", "select l from tab order by ts desc limit -18,-15");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_middle_N_from_both_directions() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 9,-9");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_N_intersecting_end() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n1\n1\n", "select l from tab order by ts desc limit 18,22");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_N_intersecting_start() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n10\n10\n", "select l from tab order by ts desc limit -22,-18");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_first_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void test_noDesignatedTsTableWithDuplicates_select_last_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_no_designated_ts_table_with_duplicates();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    //creates test table in descending and then ascending order order 10,9,..,1, 1,2,..,10
    private void prepare_no_designated_ts_table_with_duplicates() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP);",
                "insert into tab " +
                        "  select 11-x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:10', 'yyyy-MM-ddTHH:mm:ss'), -1000000) " +
                        "  from long_sequence(10);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:01', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);");
    }

    //normal table without designated timestamp with rows in ascending
    @Test
    public void test_noDesignatedTsTable_select_all() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void test_noDesignatedTsTable_select_first_N() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void test_noDesignatedTsTable_select_middle_N_from_start() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8");
    }

    @Test
    public void test_noDesignatedTsTable_select_N_beyond_end_returns_empty_result() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void test_noDesignatedTsTable_select_N_before_start_returns_empty_result() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void test_noDesignatedTsTable_select_last_N() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void test_noDesignatedTsTable_select_middle_N_from_end() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void test_noDesignatedTsTable_select_middle_N_from_both_directions() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void test_noDesignatedTsTable_select_first_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void test_noDesignatedTsTable_select_last_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    @Test
    public void test_noDesignatedTsTable_select_N_intersecting_end() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void test_noDesignatedTsTable_select_N_intersecting_start() throws Exception {
        prepare_non_designated_ts_table();

        assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8");
    }

    private void prepare_non_designated_ts_table() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);");
    }

    //empty table with designated timestamp
    @Test
    public void test_emptyTable_select_all_returns_no_rows() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc");
    }

    @Test
    public void test_emptyTable_select_first_N_returns_no_rows() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void test_emptyTable_select_middle_N_from_start_returns_no_rows() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 5,8");
    }


    @Test
    public void test_emptyTable_select_N_beyond_end_returns_empty_result() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void test_emptyTable_select_N_before_start_returns_empty_result() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void test_emptyTable_select_N_intersecting_end_returns_empty_result() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void test_emptyTable_select_N_intersecting_start_returns_empty_result() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -12,-8");
    }

    @Test
    public void test_emptyTable_select_last_N_returns_no_rows() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void test_emptyTable_select_middle_N_from_end_returns_no_rows() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void test_emptyTable_select_middle_N_from_both_directions() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void test_emptyTable_select_first_N_with_same_lo_hi_returns_no_rows() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void test_emptyTable_select_last_N_with_same_lo_hi_returns_no_rows() throws Exception {
        create_empty_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    private void create_empty_table() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts);");
    }

    //regular table with designated timestamp and  one partition
    @Test
    public void test_normalTable_select_all() throws Exception {
        prepare_normal_table();

        assertQuery("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void test_normalTable_select_first_N() throws Exception {
        prepare_normal_table();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void test_normalTable_select_middle_N_from_start() throws Exception {
        prepare_normal_table();

        assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8");
    }

    @Test
    public void test_normalTable_select_N_beyond_end_returns_empty_result() throws Exception {
        prepare_normal_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void test_normalTable_select_N_before_start_returns_empty_result() throws Exception {
        prepare_normal_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void test_normalTable_select_last_N() throws Exception {
        prepare_normal_table();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void test_normalTable_select_middle_N_from_end() throws Exception {
        prepare_normal_table();

        assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void test_normalTable_select_middle_N_from_both_directions() throws Exception {
        prepare_normal_table();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void test_normalTable_select_first_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_normal_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void test_normalTable_select_last_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_normal_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    @Test
    public void test_normalTable_select_N_intersecting_end() throws Exception {
        prepare_normal_table();

        assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void test_normalTable_select_N_intersecting_start() throws Exception {
        prepare_normal_table();

        assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8");
    }

    private void prepare_normal_table() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);");
    }

    //partitioned table with designated timestamp and two partitions, 5 rows per partition
    @Test
    public void test_2partitions_select_all() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void test_2partitions_select_first_N() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void test_2partitions_select_middle_N_from_start() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8");
    }

    @Test
    public void test_2partitions_select_N_beyond_end_returns_empty_result() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void test_2partitions_select_N_before_start_returns_empty_result() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void test_2partitions_select_last_N() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void test_2partitions_select_middle_N_from_end() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void test_2partitions_select_middle_N_from_both_directions() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void test_2partitions_select_first_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void test_2partitions_select_last_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    @Test
    public void test_2partitions_select_N_intersecting_end() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void test_2partitions_select_N_intersecting_start() throws Exception {
        prepare_2partitions_table();

        assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8");
    }

    private void prepare_2partitions_table() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 17280000000) " +
                        "  from long_sequence(10);");
    }

    //partitioned table with designated timestamp and 10 partitions one row per partition
    @Test
    public void test_partitionPerRow_select_all() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void test_partitionPerRow_select_first_N() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void test_partitionPerRow_select_middle_N_from_start() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8");
    }

    @Test
    public void test_partitionPerRow_select_N_beyond_end_returns_empty_result() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void test_partitionPerRow_select_N_before_start_returns_empty_result() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void test_partitionPerRow_select_last_N() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void test_partitionPerRow_select_middle_N_from_end() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void test_partitionPerRow_select_middle_N_from_both_directions() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void test_partitionPerRow_select_first_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void test_partitionPerRow_select_last_N_with_same_lo_hi_returns_no_rows() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    @Test
    public void test_partitionPerRow_select_N_intersecting_end() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void test_partitionPerRow_select_N_intersecting_start() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8");
    }

    private void prepare_partitionPerRow_table() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");
    }

    //special cases
    @Test
    public void test_partitionPerRow_select_first_N_ordered_by_multiple_columns() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc, l desc limit 3");
    }

    @Test
    public void test_partitionPerRow_select_last_N_ordered_by_multiple_columns() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc, l desc limit -3");
    }

    @Test
    public void test_partitionPerRow_select_first_N_ordered_by_nonTs_column() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by l desc limit 3");
    }

    @Test
    public void test_partitionPerRow_select_last_N_ordered_by_nonTs_column() throws Exception {
        prepare_partitionPerRow_table();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by l desc limit -3");
    }

    @Test
    public void test_partitionPerRow_select_first_N_with_different_case_in_order_by() throws Exception {//here
        prepare_partitionPerRow_table_with_long_names();

        assertQuery("record_type\n10\n9\n8\n7\n6\n", "select record_type from trips order by created_ON desc limit 5");
    }

    @Test
    public void test_partitionPerRow_select_first_N_with_different_case_in_select_and_order_by() throws Exception {
        prepare_partitionPerRow_table_with_long_names();

        assertQuery("record_Type\tCREATED_on\n" + DATA, "select record_Type, CREATED_on from trips order by created_ON desc limit 5");
    }

    @Test
    public void test_partitionPerRow_select_first_N_with_different_case_in_select_and_order_by_v2() throws Exception {
        prepare_partitionPerRow_table_with_long_names();

        assertQuery("record_Type\tCREATED_ON\n" + DATA, "select record_Type, CREATED_ON from trips order by created_on desc limit 5");
    }

    @Test
    public void test_partitionPerRow_select_first_N_with_different_case_in_select_and_order_by_with_alias() throws Exception {
        prepare_partitionPerRow_table_with_long_names();

        assertQuery("record_Type\tcre_on\n" + DATA, "select record_Type, CREATED_ON as cre_on from trips order by created_on desc limit 5");
    }

    @Test
    public void test_partitionPerRow_select_first_N_with_different_case_in_select_and_order_by_with_alias_v2() throws Exception {
        prepare_partitionPerRow_table_with_long_names();

        assertQuery("record_Type\tcre_on\n" + DATA, "select record_Type, CREATED_ON cre_on from trips order by created_on desc limit 5");
    }

    @Test
    public void test_partitionPerRow_select_first_N_with_different_name_in_subquery() throws Exception {
        prepare_partitionPerRow_table_with_long_names();

        assertQuery(EXPECTED, "select rectype, creaton from " +
                "( select record_Type as rectype, CREATED_ON creaton from trips order by created_on desc limit 5)");
    }

    @Test
    public void test_partitionPerRow_select_first_N_with_different_name_in_subquery_v2() throws Exception {
        prepare_partitionPerRow_table_with_long_names();

        assertQuery(EXPECTED, "select rectype, creaton from " +
                "( select record_Type as rectype, CREATED_ON creaton " +
                "from trips " +
                "order by created_on desc) " +
                "limit 5");
    }

    @Test
    public void test_partitionPerRow_select_first_N_with_different_name_in_subquery_v3() throws Exception {
        prepare_partitionPerRow_table_with_long_names();

        assertQuery(EXPECTED, "select rectype, creaton from " +
                "( select record_Type as rectype, CREATED_ON creaton from trips) " +
                "order by creaton desc limit 5");
    }

    static final String DATA = "10\t2022-01-13T10:00:00.000000Z\n" +
            "9\t2022-01-12T06:13:20.000000Z\n" +
            "8\t2022-01-11T02:26:40.000000Z\n" +
            "7\t2022-01-09T22:40:00.000000Z\n" +
            "6\t2022-01-08T18:53:20.000000Z\n";
    static final String EXPECTED = "rectype\tcreaton\n" + DATA;

    private void prepare_partitionPerRow_table_with_long_names() throws Exception {
        runQueries("CREATE TABLE trips(record_type long, created_on TIMESTAMP) timestamp(created_on) partition by day;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");
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
