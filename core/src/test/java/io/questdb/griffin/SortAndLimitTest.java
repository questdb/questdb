package io.questdb.griffin;

import org.junit.Test;

/**
 *
 */
public class SortAndLimitTest extends AbstractGriffinTest {

    @Test
    public void testInsert_random_10k_records_AndSelect_Bottom_5_returns_last_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select rnd_int(6, 10000, 0) from long_sequence( 10000 );",
                "insert into sorttest select x from long_sequence( 5 );");

        assertQuery("i\n1\n2\n3\n4\n5\n", "select i from sorttest order by i limit 5");
    }

    @Test
    public void testInsert_random_10k_records_AndSelect_Bottom_5_returns_first_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 5 );",
                "insert into sorttest select rnd_int(6, 10000, 0) from long_sequence( 10000 );");

        assertQuery("i\n1\n2\n3\n4\n5\n", "select i from sorttest order by i limit 5");
    }

    @Test
    public void testInsert_random_10k_records_AndSelect_Top_5_returns_first_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select 9995 + x from long_sequence( 5 );",
                "insert into sorttest select rnd_int(1, 9995, 0) from long_sequence( 10000 );");

        assertQuery("i\n9996\n9997\n9998\n9999\n10000\n", "select i from sorttest order by i limit -5");
    }

    @Test
    public void testInsert_random_10k_records_AndSelect_Top_5_returns_last_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select rnd_int(1, 9995, 0) from long_sequence( 10000 );",
                "insert into sorttest select 9995 + x from long_sequence( 5 );");

        assertQuery("i\n9996\n9997\n9998\n9999\n10000\n", "select i from sorttest order by i limit -5");
    }

    @Test
    public void testInsert_10k_records_AndSelect_Bottom_5_returns_first_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 10000 );");

        assertQuery("i\n1\n2\n3\n4\n5\n", "select i from sorttest order by i limit 5");
    }

    @Test
    public void testInsert_10k_records_AndSelect_Top_5_returns_last_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 10000 );");

        assertQuery("i\n9996\n9997\n9998\n9999\n10000\n", "select i from sorttest order by i limit -5");
    }

    @Test
    public void testInsert_1k_records_And_Select_Middle_5() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 1000 );");

        assertQuery("i\n995\n996\n997\n998\n999\n", "select i from sorttest order by i limit 994, -1");
    }

    @Test
    public void testInsertAndSelect_Bottom_5_returns_0_records_because_output_is_empty() throws Exception {
        runQueries("create table sorttest(i int);");

        assertQuery("i\n", "select i from sorttest order by i limit 3");
    }

    @Test
    public void testInsertAndSelect_Bottom_5_returns_first_3_records_because_output_is_smaller_than_limit() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 3 );");

        assertQuery("i\n1\n2\n3\n", "select i from sorttest order by i limit 3");
    }

    @Test
    public void testInsertAndSelect_Bottom_5_returns_first_5_records() throws Exception {
        runQueries("create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 10 );");

        assertQuery("i\n1\n2\n3\n4\n5\n", "select i from sorttest order by i limit 5");
    }

    @Test
    public void testInsert_with_duplicates_AndSelect_Bottom_5_returns_first_5_records() throws Exception {
        runQueries(
                "create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 10 );",
                "insert into sorttest select x from long_sequence( 10 );");

        assertQuery("i\n1\n1\n2\n2\n3\n", "select i from sorttest order by i limit 5");
    }

    @Test
    public void testInsert_with_duplicates_AndSelect_Top_5_returns_last_5_records() throws Exception {
        runQueries("create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 10 );",
                "insert into sorttest select x from long_sequence( 10 );");

        assertQuery("i\n8\n9\n9\n10\n10\n", "select i from sorttest order by i limit -5");
    }

    @Test
    public void testInsertAndSelect_Top_5_returns_last_5_records() throws Exception {
        runQueries("create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 10 );");

        assertQuery("i\n6\n7\n8\n9\n10\n", "select i from sorttest order by i limit -5");
    }

    @Test
    public void testInsertAndSelect_Top_5_returns_last_3_records_because_output_is_smaller_than_limit() throws Exception {
        runQueries("create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 3 );");

        assertQuery("i\n1\n2\n3\n", "select i from sorttest order by i limit -5");
    }

    @Test
    public void testInsertAndSelect_Top_5_returns_0_records_because_table_is_empty() throws Exception {
        runQueries("create table sorttest(i int);");

        assertQuery("i\n", "select i from sorttest order by i limit -5");
    }

    @Test
    public void testInsertAndSelect_Lo_2_Hi_5_returns_middle_3_records() throws Exception {
        runQueries("create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 10 );");

        assertQuery("i\n3\n4\n5\n", "select i from sorttest order by i limit 2,5");
    }

    @Test
    public void testInsertAndSelect_Lo_2_Hi_5_returns_middle_2_records_because_output_is_smaller_than_limit() throws Exception {
        runQueries("create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 4 );");

        assertQuery("i\n3\n4\n", "select i from sorttest order by i limit 2,5");
    }

    @Test
    public void testInsertAndSelect_Lo_2_Hi_5_returns_0_records_because_output_is_smaller_than_limit() throws Exception {
        runQueries("create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 2 );");

        assertQuery("i\n", "select i from sorttest order by i limit 2,5");
    }

    @Test
    public void testInsertAndSelect_Lo_2_Hi_5_returns_0_records_because_output_is_0_size() throws Exception {
        runQueries("create table sorttest(i int);");

        assertQuery("i\n", "select i from sorttest order by i limit 2,5");
    }

    @Test
    public void testInsertAndSelect_Lo_1_Hi_minus_1_returns_all_records_except_first_and_last_one() throws Exception {
        runQueries("create table sorttest(i int);",
                "insert into sorttest select x from long_sequence( 10 );");

        assertQuery("i\n2\n3\n4\n5\n6\n7\n8\n9\n", "select i from sorttest order by i limit 1,-1");
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
