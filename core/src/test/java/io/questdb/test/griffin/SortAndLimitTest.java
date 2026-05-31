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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SortAndLimitTest extends AbstractCairoTest {

    @Test
    public void testInsertAndSelectDesc_Lo_10_Hi_20_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l desc limit 10,20")
                .expectSize()
                .returns("l\n990\n989\n988\n987\n986\n985\n984\n983\n982\n981\n");
    }

    // randomized cases - descending order
    @Test
    public void testInsertAndSelectDesc_Lo_10_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l desc limit 10")
                .expectSize()
                .returns("l\n1000\n999\n998\n997\n996\n995\n994\n993\n992\n991\n");
    }

    @Test
    public void testInsertAndSelectDesc_Lo_990_Hi_minus5_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l desc limit 990,-5")
                .expectSize()
                .returns("l\n10\n9\n8\n7\n6\n");
    }

    @Test
    public void testInsertAndSelectDesc_Lo_minus10_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l desc limit -10")
                .expectSize()
                .returns("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n");
    }

    @Test
    public void testInsertAndSelectDesc_Lo_minus20_Hi_minus10_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l desc limit -20,-10")
                .expectSize()
                .returns("l\n20\n19\n18\n17\n16\n15\n14\n13\n12\n11\n");
    }

    @Test
    public void testInsertAndSelect_Bottom_5_returns_0_records_because_output_is_empty() throws Exception {
        runQueries("create table sorttest(i int);");

        assertQuery("select i from sorttest order by i limit 3")
                .returns("i\n");
    }

    @Test
    public void testInsertAndSelect_Bottom_5_returns_first_3_records_because_output_is_smaller_than_limit() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(3);"
        );

        assertQuery("select i from sorttest order by i limit 3")
                .expectSize()
                .returns("i\n1\n2\n3\n");
    }

    @Test
    public void testInsertAndSelect_Bottom_5_returns_first_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(10);"
        );

        assertQuery("select i from sorttest order by i limit 5")
                .expectSize()
                .returns("i\n1\n2\n3\n4\n5\n");
    }

    @Test
    public void testInsertAndSelect_Lo_10_Hi_20_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l limit 10,20")
                .expectSize()
                .returns("l\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n");
    }

    // randomized cases - ascending order
    @Test
    public void testInsertAndSelect_Lo_10_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l limit 10")
                .expectSize()
                .returns("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n");
    }

    @Test
    public void testInsertAndSelect_Lo_1_Hi_minus_1_returns_all_records_except_first_and_last_one() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(10);"
        );

        assertQuery("select i from sorttest order by i limit 1,-1")
                .expectSize()
                .returns("i\n2\n3\n4\n5\n6\n7\n8\n9\n");
    }

    @Test
    public void testInsertAndSelect_Lo_2_Hi_5_returns_0_records_because_output_is_0_size() throws Exception {
        runQueries("create table sorttest(i int);");

        assertQuery("select i from sorttest order by i limit 2,5")
                .returns("i\n");
    }

    @Test
    public void testInsertAndSelect_Lo_2_Hi_5_returns_0_records_because_output_is_smaller_than_limit() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(2);"
        );

        assertQuery("select i from sorttest order by i limit 2,5")
                .returns("i\n");
    }

    @Test
    public void testInsertAndSelect_Lo_2_Hi_5_returns_middle_2_records_because_output_is_smaller_than_limit() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(4);"
        );

        assertQuery("select i from sorttest order by i limit 2,5")
                .expectSize()
                .returns("i\n3\n4\n");
    }

    @Test
    public void testInsertAndSelect_Lo_2_Hi_5_returns_middle_3_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(10);"
        );

        assertQuery("select i from sorttest order by i limit 2,5")
                .expectSize()
                .returns("i\n3\n4\n5\n");
    }

    @Test
    public void testInsertAndSelect_Lo_990_Hi_minus5_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l limit 990,-5")
                .expectSize()
                .returns("l\n991\n992\n993\n994\n995\n");
    }

    @Test
    public void testInsertAndSelect_Lo_minus10_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l limit -10")
                .expectSize()
                .returns("l\n991\n992\n993\n994\n995\n996\n997\n998\n999\n1000\n");
    }

    @Test
    public void testInsertAndSelect_Lo_minus20_Hi_minus10_on_table_with_random_order() throws Exception {
        prepareRandomOrderTable();

        assertQuery("select l from sorttest order by l limit -20,-10")
                .expectSize()
                .returns("l\n981\n982\n983\n984\n985\n986\n987\n988\n989\n990\n");
    }

    @Test
    public void testInsertAndSelect_Top_5_returns_0_records_because_table_is_empty() throws Exception {
        runQueries("create table sorttest(i int);");

        assertQuery("select i from sorttest order by i limit -5")
                .returns("i\n");
    }

    @Test
    public void testInsertAndSelect_Top_5_returns_last_3_records_because_output_is_smaller_than_limit() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(3);"
        );

        assertQuery("select i from sorttest order by i limit -5")
                .expectSize()
                .returns("i\n1\n2\n3\n");
    }

    @Test
    public void testInsertAndSelect_Top_5_returns_last_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(10);"
        );

        assertQuery("select i from sorttest order by i limit -5")
                .expectSize()
                .returns("i\n6\n7\n8\n9\n10\n");
    }

    @Test
    public void testInsert_10k_records_AndSelect_Bottom_5_returns_first_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(10000);"
        );

        assertQuery("select i from sorttest order by i limit 5")
                .expectSize()
                .returns("i\n1\n2\n3\n4\n5\n");
    }

    @Test
    public void testInsert_10k_records_AndSelect_Top_5_returns_last_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(10000);"
        );

        assertQuery("select i from sorttest order by i limit -5")
                .expectSize()
                .returns("i\n9996\n9997\n9998\n9999\n10000\n");
    }

    @Test
    public void testInsert_1k_records_And_Select_Middle_5() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(1000);"
        );

        assertQuery("select i from sorttest order by i limit 994, -1")
                .expectSize()
                .returns("i\n995\n996\n997\n998\n999\n");
    }

    @Test
    public void testInsert_random_10k_records_AndSelect_Bottom_5_returns_first_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(5);",
                "insert into sorttest select rnd_int(6, 10000, 0) from long_sequence(10000);"
        );

        assertQuery("select i from sorttest order by i limit 5")
                .expectSize()
                .returns("i\n1\n2\n3\n4\n5\n");
    }

    @Test
    public void testInsert_random_10k_records_AndSelect_Bottom_5_returns_last_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select rnd_int(6, 10000, 0) from long_sequence(10000);",
                "insert into sorttest select x from long_sequence(5);"
        );

        assertQuery("select i from sorttest order by i limit 5")
                .expectSize()
                .returns("i\n1\n2\n3\n4\n5\n");
    }

    @Test
    public void testInsert_random_10k_records_AndSelect_Top_5_returns_first_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select 9995 + x from long_sequence(5);",
                "insert into sorttest select rnd_int(1, 9995, 0) from long_sequence(10000);"
        );

        assertQuery("select i from sorttest order by i limit -5")
                .expectSize()
                .returns("i\n9996\n9997\n9998\n9999\n10000\n");
    }

    @Test
    public void testInsert_random_10k_records_AndSelect_Top_5_returns_last_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select rnd_int(1, 9995, 0) from long_sequence(10000);",
                "insert into sorttest select 9995 + x from long_sequence(5);"
        );

        assertQuery("select i from sorttest order by i limit -5")
                .expectSize()
                .returns("i\n9996\n9997\n9998\n9999\n10000\n");
    }

    @Test
    public void testInsert_with_duplicates_AndSelect_Bottom_5_returns_first_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(10);",
                "insert into sorttest select x from long_sequence(10);"
        );

        assertQuery("select i from sorttest order by i limit 5")
                .expectSize()
                .returns("i\n1\n1\n2\n2\n3\n");
    }

    @Test
    public void testInsert_with_duplicates_AndSelect_Top_5_returns_last_5_records() throws Exception {
        runQueries("create table sorttest(i int);",
                "insert into sorttest select x from long_sequence(10);",
                "insert into sorttest select x from long_sequence(10);"
        );

        assertQuery("select i from sorttest order by i limit -5")
                .expectSize()
                .returns("i\n8\n9\n9\n10\n10\n");
    }

private void prepareRandomOrderTable() throws Exception {
        runQueries(
                "CREATE TABLE sorttest (l long, ts TIMESTAMP) timestamp(ts) partition by year;",
                """
                        insert into sorttest\s
                          select x,
                          rnd_timestamp(
                            to_timestamp('2015', 'yyyy'),
                            to_timestamp('2016', 'yyyy'),
                            0)
                          from long_sequence(1000);"""
        );
    }

    private void runQueries(String... queries) throws Exception {
        assertMemoryLeak(() -> {
            for (String query : queries) {
                execute(query);
            }
        });
    }
}
