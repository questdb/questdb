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

import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

public class ProjectionReferenceTest extends AbstractCairoTest {

    private Rnd rnd;

    @Before
    public void setUp() {
        super.setUp();
        rnd = new Rnd();
    }

    @Test
    public void testAsofJoinSimple() throws Exception {
        execute("create table events (symbol string, value int, ts timestamp) timestamp(ts)");
        execute("create table quotes (symbol string, quote int, ts timestamp) timestamp(ts)");

        execute("insert into events values ('A', 100, '2025-01-01T10:00:00.000Z'), ('A', 200, '2025-01-01T10:05:00.000Z')");
        execute("insert into quotes values ('A', 10, '2025-01-01T09:59:00.000Z'), ('A', 20, '2025-01-01T10:03:00.000Z')");

        // Simple ASOF JOIN without projection references
        assertQuery(
                """
                        symbol\tvalue\tquote\tsum
                        A\t100\t10\t110
                        A\t200\t20\t220
                        """,
                "select e.symbol, e.value, q.quote, e.value + q.quote as sum " +
                        "from events e asof join quotes q on e.symbol = q.symbol",
                false,
                true
        );
    }

    @Test
    public void testBindingVars() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0, 1);

            assertQuery(
                    """
                            b\tinc
                            1\t2
                            1\t2
                            1\t2
                            """,
                    "select $1 as b, b + 1 as inc from long_sequence(3)",
                    true
            );

            // we can use a projected column inside an expression
            assertQuery(
                    """
                            b\tinc
                            2\t3
                            3\t4
                            4\t5
                            """,
                    "select $1 + x as b, b + 1 as inc from long_sequence(3)",
                    true
            );

            // we prioritise base column over projection
            assertQuery(
                    """
                            x\tx_orig
                            1\t1
                            1\t2
                            1\t3
                            """,
                    "select $1 as x, x as x_orig from long_sequence(3)",
                    true
            );

            assertQuery(
                    """
                            x\tx_orig
                            2\t1
                            3\t2
                            4\t3
                            """,
                    "select $1 + x as x, x as x_orig from long_sequence(3)",
                    true
            );

            assertQuery(
                    """
                            i\tc
                            1\t2
                            2\t3
                            3\t4
                            """,
                    "select x as i, $1 + i c from long_sequence(3)",
                    true
            );
        });
    }

    @Test
    public void testColumnAsColumnReference() throws Exception {
        assertSql(
                """
                        k\tk1
                        1\t1
                        2\t2
                        3\t3
                        4\t4
                        5\t5
                        6\t6
                        7\t7
                        8\t8
                        9\t9
                        10\t10
                        """,
                "select x k, k from long_sequence(10)"
        );
    }

    @Test
    public void testColumnAsColumnReferencePreferBaseTable() throws Exception {
        assertSql(
                """
                        x\tx1
                        1\t1
                        2\t2
                        3\t3
                        4\t4
                        5\t5
                        6\t6
                        7\t7
                        8\t8
                        9\t9
                        10\t10
                        """,
                "select a x, x from (select x a, x b, x from long_sequence(10))"
        );
    }

    @Test
    public void testInnerJoinSimple() throws Exception {
        execute("create table t1 (id int, val int)");
        execute("create table t2 (id int, val int)");
        execute("insert into t1 values (1, 10), (2, 20)");
        execute("insert into t2 values (1, 100), (2, 200)");

        // Simple join without projection references to ensure JOIN works
        assertQuery(
                """
                        id\tval1\tval2\tsum
                        1\t10\t100\t110
                        2\t20\t200\t220
                        """,
                "select t1.id, t1.val as val1, t2.val as val2, t1.val + t2.val as sum " +
                        "from t1 inner join t2 on t1.id = t2.id",
                false,
                false
        );
    }

    @Test
    public void testJoinWithProjectionReference() throws Exception {
        execute("create table orders (id int, amount int)");
        execute("create table customers (id int, name string)");
        execute("insert into orders values (1, 100), (2, 200)");
        execute("insert into customers values (1, 'Alice'), (2, 'Bob')");

        assertQuery(
                """
                        order_id\tcustomer_name\tamount\ttax\ttotal
                        1\tAlice\t100\t10.0\t110.0
                        2\tBob\t200\t20.0\t220.0
                        """,
                "select" +
                        " o.id as order_id," +
                        " c.name as customer_name," +
                        " o.amount," +
                        " o.amount * 0.1 as tax," +
                        " o.amount + tax as total" +
                        " from orders o join customers c on o.id = c.id",
                false,
                false
        );
    }

    @Test
    public void testJsonProjectionInOrderByWithByte() throws SqlException {
        testJsonProjectionInOrderByWith0("""
                        name\tval\tdoubled
                        C\t1\t2
                        C\t6\t12
                        C\t18\t36
                        C\t20\t40
                        C\t33\t66
                        C\t39\t78
                        C\t42\t84
                        C\t48\t96
                        C\t71\t142
                        C\t71\t142
                        """,
                """
                        QUERY PLAN
                        Radix sort light
                          keys: [doubled]
                            VirtualRecord
                              functions: [name,memoize(json_extract()::byte),memoize(val*2)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: items
                        """,
                "byte");
    }

    @Test
    public void testJsonProjectionInOrderByWithDouble() throws SqlException {
        testJsonProjectionInOrderByWithF("double");
    }

    @Test
    public void testJsonProjectionInOrderByWithFloat() throws SqlException {
        testJsonProjectionInOrderByWithF("float");
    }

    @Test
    public void testJsonProjectionInOrderByWithInt() throws SqlException {
        testJsonProjectionInOrderByWithI("int");
    }

    @Test
    public void testJsonProjectionInOrderByWithLong() throws SqlException {
        testJsonProjectionInOrderByWithI("long");
    }

    @Test
    public void testJsonProjectionInOrderByWithShort() throws SqlException {
        testJsonProjectionInOrderByWithI("short");
    }

    @Test
    public void testMultipleLevelProjections() throws Exception {
        execute("create table data (x int)");
        execute("insert into data values (1), (2), (3)");

        assertQuery(
                """
                        x\ta\tb\tc\td
                        1\t2\t4\t8\t16
                        2\t3\t5\t9\t17
                        3\t4\t6\t10\t18
                        """,
                "select x, x + 1 as a, a + 2 as b, b + 4 as c, c + 8 as d from data",
                true
        );
    }

    @Test
    public void testNestedSubquerySimple() throws Exception {
        execute("create table base (id int, value int)");
        execute("insert into base values (1, 10), (2, 20), (3, 30)");

        // Test projection references across subquery boundaries
        assertQuery(
                """
                        id\tdoubled
                        1\t20
                        2\t40
                        3\t60
                        """,
                "select id, doubled from (select id, value * 2 as doubled from base)",
                true
        );
    }

    @Test
    public void testOrderBy() throws Exception {
        // note: ordering prioritises projected columns over base columns, this is intentional and is consistent with DuckDB
        execute("create table trades (symbol string, price double, ts timestamp) timestamp(ts)");
        execute("insert into trades values ('A', 1, '2025-01-01T10:00:00.000Z'), ('B', 2, '2025-01-01T10:05:00.000Z')");
        assertQuery(
                """
                        symbol\torig_price\tprice
                        B\t2.0\t-2.0
                        A\t1.0\t-1.0
                        """,
                "select symbol, price as orig_price, -price as price from trades order by price limit 10",
                true
        );
    }

    @Test
    public void testPreferBaseColumnOverProjectionVanilla() throws Exception {
        execute("create table temp (x int)");
        execute("insert into temp values (1), (2), (3)");
        assertQuery(
                """
                        x\tcolumn
                        11\t-4
                        12\t-3
                        13\t-2
                        """,
                "select x + 10 x, x - 5 from temp",
                true
        );
    }

    @Test
    public void testProjectionAliasPreference() throws Exception {
        execute("create table test (a int, b int)");
        execute("insert into test values (5, 10), (15, 20)");

        // Verify that when we create an alias with the same name as a column,
        // references still use the original column (not the alias)
        assertQuery(
                """
                        a\tb\toriginal_a
                        15\t10\t5
                        35\t20\t15
                        """,
                "select a + b as a, b, a as original_a from test",
                true
        );
    }

    @Test
    public void testProjectionInOrderByWithBoolean() throws Exception {
        execute("create table items (name string, value boolean)");
        execute("insert into items values ('C', true), ('A', false), ('B', false)");

        allowFunctionMemoization();

        assertSql(
                """
                        name\tv\tvalue\tvv
                        A\tfalse\ttrue\tfalse
                        B\tfalse\ttrue\tfalse
                        C\ttrue\ttrue\ttrue
                        """,
                "select name, value v, true value, (rnd_boolean() or value) vv from items order by 4"
        );
    }

    @Test
    public void testProjectionInOrderByWithByte() throws Exception {
        testProjectionInOrderByWithInt("byte");
    }

    @Test
    public void testProjectionInOrderByWithDate() throws Exception {
        testProjectionInOrderByWith0(
                """
                        name\tvalue\tdoubled
                        B\t1970-01-01T00:00:00.020Z\t1.6973928465121335
                        A\t1970-01-01T00:00:00.010Z\t2.246301342497259
                        C\t1970-01-01T00:00:00.030Z\t19.823333682561998
                        """,
                "date"
        );
    }

    @Test
    public void testProjectionInOrderByWithDouble() throws Exception {
        testProjectionInOrderByWithF("double");
    }

    @Test
    public void testProjectionInOrderByWithFloat() throws Exception {
        testProjectionInOrderByWithF("float");
    }

    @Test
    public void testProjectionInOrderByWithInt() throws Exception {
        testProjectionInOrderByWithInt("int");
    }

    @Test
    public void testProjectionInOrderByWithLong() throws Exception {
        testProjectionInOrderByWithInt("long");
    }

    @Test
    public void testProjectionInOrderByWithShort() throws Exception {
        testProjectionInOrderByWithInt("short");
    }

    @Test
    public void testProjectionInOrderByWithString() throws Exception {
        execute("create table items (name string, value string)");
        execute("insert into items values ('C', 'zebra'), ('A', 'apple'), ('B', 'banana')");

        allowFunctionMemoization();

        assertQuery(
                """
                        name\tvalue\tupper\tconcat
                        A\tapple\tAPPLE\tAPPLE_UPPER
                        B\tbanana\tBANANA\tBANANA_UPPER
                        C\tzebra\tZEBRA\tZEBRA_UPPER
                        """,
                "select name, value, upper(value) as upper, upper || '_UPPER' as concat from items order by upper",
                true
        );
    }

    @Test
    public void testProjectionInOrderByWithSymbol() throws Exception {
        execute("create table items (name string, value symbol)");
        execute("insert into items values ('C', 'zebra'), ('A', 'apple'), ('B', 'banana')");

        allowFunctionMemoization();

        assertQuery(
                """
                        name\tvalue\tupper\tconcat
                        A\tapple\tAPPLE\tAPPLE_UPPER
                        B\tbanana\tBANANA\tBANANA_UPPER
                        C\tzebra\tZEBRA\tZEBRA_UPPER
                        """,
                "select name, value, upper(value)::symbol as upper, upper || '_UPPER' as concat from items order by upper",
                true
        );
    }

    @Test
    public void testProjectionInOrderByWithTimestamp() throws Exception {
        testProjectionInOrderByWith0(
                """
                        name\tvalue\tdoubled
                        B\t1970-01-01T00:00:00.000020Z\t1.6973928465121335
                        A\t1970-01-01T00:00:00.000010Z\t2.246301342497259
                        C\t1970-01-01T00:00:00.000030Z\t19.823333682561998
                        """,
                "timestamp"
        );
    }

    @Test
    public void testProjectionInOrderByWithVarchar() throws Exception {
        execute("create table items (name string, value varchar)");
        execute("insert into items values ('C', 'zebra'), ('A', 'apple'), ('B', 'banana')");

        allowFunctionMemoization();

        assertQuery(
                """
                        name\tvalue\tupper\tconcat
                        A\tapple\tAPPLE\tAPPLE_UPPER
                        B\tbanana\tBANANA\tBANANA_UPPER
                        C\tzebra\tZEBRA\tZEBRA_UPPER
                        """,
                "select name, value, upper(value) as upper, upper || '_UPPER' as concat from items order by upper",
                true
        );
    }

    @Test
    public void testProjectionInWhereClause() throws Exception {
        execute("create table data (x int, y int)");
        execute("insert into data values (1, 10), (2, 20), (3, 30), (4, 40)");

        // Test that WHERE clause uses base columns, not projections
        assertQuery(
                """
                        x
                        22
                        33
                        44
                        """,
                "select x + y as x from data where x > 1",
                false
        );
    }

    @Test
    public void testProjectionSymbolAccess() throws Exception {
        assertSql(
                """
                        a\tconcat\tp\tb
                        abc\tabc--\t1\t3.0
                        fgk\tfgk--\t2\t4.0
                        fgk\tfgk--\t3\t5.0
                        abc\tfgk--\t4\t6.0
                        abc\tabc--\t5\t7.0
                        abc\tabc--\t6\t8.0
                        abc\tfgk--\t7\t9.0
                        fgk\tabc--\t8\t10.0
                        abc\tfgk--\t9\t11.0
                        fgk\tabc--\t10\t12.0
                        """,
                "select rnd_symbol('abc', 'fgk') a, a || '--', x p, p + 2.0 b from long_sequence(10);"
        );
    }

    @Test
    public void testProjectionWithArithmetic() throws Exception {
        execute("create table numbers (n int)");
        execute("insert into numbers values (10), (20), (30)");

        // Test that projection references work with various arithmetic operations
        assertQuery(
                """
                        n\tdouble_n\ttriple_n\thalf_of_double
                        10\t20\t30\t10
                        20\t40\t60\t20
                        30\t60\t90\t30
                        """,
                "select n, n * 2 as double_n, double_n + n as triple_n, double_n / 2 as half_of_double from numbers",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testProjectionWithArray() throws Exception {
        execute("create table items (name string, value double[][])");
        execute("insert into items values ('C', ARRAY[[3.0, 6], [9.0, 12]]), ('A', ARRAY[[1.0, 2], [3.0, 4]]), ('B', ARRAY[[2.0, 4], [6.0, 8]])");

        allowFunctionMemoization();

        assertQuery(
                """
                        name	value	first_row	second_row_first_elem	first_elem	doubled
                        A	[[1.0,2.0],[3.0,4.0]]	[1.0,2.0]	3.0	1.0	2.0
                        B	[[2.0,4.0],[6.0,8.0]]	[2.0,4.0]	6.0	2.0	4.0
                        C	[[3.0,6.0],[9.0,12.0]]	[3.0,6.0]	9.0	3.0	6.0
                        """,
                "select name, value, value[1] as first_row, value[2, 1] as second_row_first_elem, first_row[1] as first_elem, first_elem * 2 as doubled from items order by second_row_first_elem",
                true
        );
    }

    @Test
    public void testProjectionWithCase() throws Exception {
        execute("create table grades (score int)");
        execute("insert into grades values (95), (85), (75), (65)");

        assertQuery(
                """
                        score\tgrade\tpass_status
                        95\tA\tPASS
                        85\tB\tPASS
                        75\tC\tPASS
                        65\tD\tFAIL
                        """,
                "select score, " +
                        "case when score >= 90 then 'A' " +
                        "     when score >= 80 then 'B' " +
                        "     when score >= 70 then 'C' " +
                        "     else 'D' end as grade, " +
                        "case when grade in ('A', 'B', 'C') then 'PASS' else 'FAIL' end as pass_status " +
                        "from grades",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSimpleProjectionChain() throws Exception {
        execute("create table data (x int)");
        execute("insert into data values (1), (2), (3)");

        // Test simple chaining: x -> a -> b
        assertQuery(
                """
                        x\ta\tb
                        1\t2\t4
                        2\t3\t5
                        3\t4\t6
                        """,
                "select x, x + 1 as a, a + 2 as b from data",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testTopDownDiamondProjectionReferences() throws Exception {
        execute("CREATE TABLE data (x INT)");
        execute("INSERT INTO data VALUES (10), (20), (30)");

        assertQuery(
                """
                        sum
                        129
                        """,
                "select sum(c) from (" +
                        "select x, x + 1 as a, x + 2 as b, a + b as c from data" +
                        ")",
                false,
                true
        );
    }

    @Test
    public void testTopDownMultipleColumnsReferenceSameColumn() throws Exception {
        execute("CREATE TABLE data (x INT, y INT)");
        execute("INSERT INTO data VALUES (10, 2), (20, 4), (30, 6)");
        assertQuery(
                """
                        sum	sum1	sum2
                        144	216	288
                        """,
                "select sum(b), sum(c), sum(d) from (" +
                        "select x, x + y as a, a * 2 as b, a * 3 as c, a * 4 as d from data" +
                        ")",
                false,
                true
        );
    }

    @Test
    public void testTopDownNestedSubqueries() throws Exception {
        execute("CREATE TABLE data (x INT)");
        execute("INSERT INTO data VALUES (1), (2), (3)");

        assertQuery(
                """
                        sum
                        18
                        """,
                "select sum(c) from (" +
                        "select b, b + 1 as c from (" +
                        "select x, x + 1 as a, a + 2 as b from data" +
                        ")" +
                        ")",
                false,
                true
        );
    }

    @Test
    public void testTopDownProjectionReferenceInSubquery() throws Exception {
        execute("CREATE TABLE core_price (" +
                "    timestamp TIMESTAMP," +
                "    symbol SYMBOL," +
                "    bid_price DOUBLE," +
                "    bid_volume LONG," +
                "    ask_price DOUBLE," +
                "    ask_volume LONG" +
                ") timestamp(timestamp)");
        execute("INSERT INTO core_price VALUES " +
                "('2025-01-01T00:00:00.000000Z', 'A', 100.0, 10, 101.0, 20)," +
                "('2025-01-01T00:00:01.000000Z', 'B', 200.0, 30, 201.0, 40)");

        assertQuery(
                """
                        avg
                        100.0
                        """,
                "select avg(schmalolzers) from (" +
                        "select timestamp, bid_volume * 1.0 / ask_volume as lolzings, lolzings * bid_price as schmalolzers from core_price" +
                        ")",
                false,
                true
        );
    }

    @Test
    public void testTopDownProjectionWithFunction() throws Exception {
        execute("CREATE TABLE data (x INT)");
        execute("INSERT INTO data VALUES (4), (9), (16)");

        assertQuery(
                """
                        sum
                        18.0
                        """,
                "select sum(b) from (" +
                        "select x, sqrt(x) as a, a * 2 as b from data" +
                        ")",
                false,
                true
        );
    }

    @Test
    public void testTopDownSelectSpecificColumnsFromProjectionChain() throws Exception {
        execute("CREATE TABLE data (x INT)");
        execute("INSERT INTO data VALUES (1), (2), (3)");

        assertQuery(
                """
                        x	b	d
                        1	4	10
                        2	5	11
                        3	6	12
                        """,
                "select x, b, d from (" +
                        "select x, x + 1 as a, a + 2 as b, b + 1 as c, c + 5 as d from data" +
                        ")",
                true,
                true
        );
    }

    @Test
    public void testUnionAll() throws Exception {
        // note: different types in union all -> it also exercises type coercion
        execute("create table temp (x int)");
        execute("create table temp2 (x long)");
        execute("insert into temp values (1), (2), (3)");
        execute("insert into temp2 values (4), (5), (6)");

        assertQuery(
                """
                        x\tdec
                        2\t0
                        3\t1
                        4\t2
                        5\t3
                        6\t4
                        7\t5
                        """,
                "select x + 1 as x, x - 1 as dec from temp union all select x + 1 as x, x - 1 from temp2",
                null,
                null,
                false,
                true
        );
    }

    @Test
    public void testUnion_overlappingOnAllColumns() throws Exception {
        execute("create table temp (x int)");
        execute("create table temp2 (x long)");
        execute("insert into temp values (1), (2), (3)");
        execute("insert into temp2 values (2), (3), (4)");

        assertQuery(
                """
                        x\tdec
                        2\t0
                        3\t1
                        4\t2
                        5\t3
                        """,
                "select x + 1 as x, x - 1 as dec from temp union select x + 1 as x, x - 1 from temp2",
                null,
                null,
                false,
                false
        );
    }

    @Test
    public void testUnion_overlappingOnProjectedColumnOnly() throws Exception {
        execute("create table temp (x int)");
        execute("create table temp2 (x long)");
        execute("insert into temp values (1), (2), (3)");
        execute("insert into temp2 values (4), (5), (6)");

        // overlapping rows with different types
        assertQuery(
                """
                        x\tb
                        -1\ttrue
                        -2\ttrue
                        -3\ttrue
                        -4\ttrue
                        -5\ttrue
                        -6\ttrue
                        """,
                "select -x as x, x > 0 as b from temp union select -x as x, x > 0 from temp2",
                null,
                null,
                false,
                false
        );
    }

    @Test
    public void testVanilla() throws Exception {
        execute("create table tmp as" +
                " (select" +
                " rnd_double() a," +
                " timestamp_sequence('2025-06-22'::timestamp, 150099) ts" +
                " from long_sequence(10)" +
                ") timestamp(ts) partition by hour");
        assertQuery(
                """
                        i\tcolumn
                        1.3215555788374664\t2.3215555788374664
                        0.4492602684994518\t1.4492602684994518
                        0.16973928465121335\t1.1697392846512134
                        0.59839809192369\t1.59839809192369
                        0.4089488367575551\t1.4089488367575551
                        1.3017188051710602\t2.30171880517106
                        1.684682184176669\t2.684682184176669
                        1.9712581691748525\t2.9712581691748525
                        0.44904681712176453\t1.4490468171217645
                        1.0187654003234814\t2.018765400323481
                        """,
                "select a * 2 i, i + 1 from tmp;",
                true
        );
    }

    @Test
    public void testVirtualFunctionAsColumnReference() throws Exception {
        assertSql(
                """
                        k\tk1
                        -1148479919\t315515119
                        1548800834\t-727724770
                        73575702\t-948263338
                        1326447243\t592859672
                        1868723707\t-847531047
                        -1191262515\t-2041844971
                        -1436881713\t-1575378702
                        806715482\t1545253513
                        1569490117\t1573662098
                        -409854404\t339631475
                        """,
                "select rnd_int() + 1 k, k from long_sequence(10)"
        );
    }

    @Test
    public void testVirtualFunctionAsColumnReferencePreferBaseTable() throws Exception {
        assertSql(
                """
                        x\tx1
                        -1148479919\t1
                        315515119\t2
                        1548800834\t3
                        -727724770\t4
                        73575702\t5
                        -948263338\t6
                        1326447243\t7
                        592859672\t8
                        1868723707\t9
                        -847531047\t10
                        """,
                "select rnd_int() + 1 x, x from long_sequence(10)"
        );
    }

    @Test
    public void testWindowFunction() throws Exception {
        execute("create table tmp as (select rnd_symbol('abc', 'cde') sym, rnd_double() price from long_sequence(20))");
        assertQuery(
                """
                        sym\ti\tprev
                        abc\t-0.8043224099968393\tnull
                        cde\t-0.08486964232560668\tnull
                        abc\t-0.0843832076262595\t-0.8043224099968393
                        abc\t-0.6508594025855301\t-0.0843832076262595
                        abc\t-0.7905675319675964\t-0.6508594025855301
                        abc\t-0.22452340856088226\t-0.7905675319675964
                        cde\t-0.3491070363730514\t-0.08486964232560668
                        cde\t-0.7611029514995744\t-0.3491070363730514
                        cde\t-0.4217768841969397\t-0.7611029514995744
                        abc\t-0.0367581207471136\t-0.22452340856088226
                        cde\t-0.6276954028373309\t-0.4217768841969397
                        cde\t-0.6778564558839208\t-0.6276954028373309
                        cde\t-0.8756771741121929\t-0.6778564558839208
                        abc\t-0.8799634725391621\t-0.0367581207471136
                        cde\t-0.5249321062686694\t-0.8756771741121929
                        abc\t-0.7675673070796104\t-0.8799634725391621
                        cde\t-0.21583224269349388\t-0.5249321062686694
                        cde\t-0.15786635599554755\t-0.21583224269349388
                        abc\t-0.1911234617573182\t-0.7675673070796104
                        cde\t-0.5793466326862211\t-0.15786635599554755
                        """,
                "select sym, -price i, lag(i) over (partition by sym) prev from tmp",
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionPreferBaseTable() throws Exception {
        execute("create table tmp as (select rnd_symbol('abc', 'cde') sym, rnd_double() price from long_sequence(20))");
        assertQuery(
                """
                        sym\tprice\tprev
                        abc\t-0.8043224099968393\tnull
                        cde\t-0.08486964232560668\tnull
                        abc\t-0.0843832076262595\t0.8043224099968393
                        abc\t-0.6508594025855301\t0.0843832076262595
                        abc\t-0.7905675319675964\t0.6508594025855301
                        abc\t-0.22452340856088226\t0.7905675319675964
                        cde\t-0.3491070363730514\t0.08486964232560668
                        cde\t-0.7611029514995744\t0.3491070363730514
                        cde\t-0.4217768841969397\t0.7611029514995744
                        abc\t-0.0367581207471136\t0.22452340856088226
                        cde\t-0.6276954028373309\t0.4217768841969397
                        cde\t-0.6778564558839208\t0.6276954028373309
                        cde\t-0.8756771741121929\t0.6778564558839208
                        abc\t-0.8799634725391621\t0.0367581207471136
                        cde\t-0.5249321062686694\t0.8756771741121929
                        abc\t-0.7675673070796104\t0.8799634725391621
                        cde\t-0.21583224269349388\t0.5249321062686694
                        cde\t-0.15786635599554755\t0.21583224269349388
                        abc\t-0.1911234617573182\t0.7675673070796104
                        cde\t-0.5793466326862211\t0.15786635599554755
                        """,
                "select sym, -price price, lag(price) over (partition by sym) prev from tmp",
                false,
                true
        );
    }

    private void testJsonProjectionInOrderByWith0(String expectedResult, String expectedPlan, String typeToExtract) throws SqlException {
        execute("create table items (name string, value varchar)");
        for (int i = 0; i < 10; i++) {
            int id = rnd.nextInt(100);
            String json = "{ \"name\": \"B\", \"value\": " + id + " }";
            execute("insert into items values ('C', '" + json + "')");
        }

        allowFunctionMemoization();
        String query = "select name, json_extract(value, '.value')::" + typeToExtract + " as val, val * 2 as doubled from items order by doubled";
        assertQuery(expectedResult,
                query,
                true,
                true);

        assertQuery(expectedPlan,
                "EXPLAIN " + query,
                false,
                true);
    }

    private void testJsonProjectionInOrderByWithF(String type) throws SqlException {
        testJsonProjectionInOrderByWith0("""
                        name\tval\tdoubled
                        C\t1.0\t2.0
                        C\t6.0\t12.0
                        C\t18.0\t36.0
                        C\t20.0\t40.0
                        C\t33.0\t66.0
                        C\t39.0\t78.0
                        C\t42.0\t84.0
                        C\t48.0\t96.0
                        C\t71.0\t142.0
                        C\t71.0\t142.0
                        """,
                """
                        QUERY PLAN
                        Sort light
                          keys: [doubled]
                            VirtualRecord
                              functions: [name,memoize(json_extract()),memoize(val*2)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: items
                        """,
                type);
    }

    private void testJsonProjectionInOrderByWithI(String type) throws SqlException {
        testJsonProjectionInOrderByWith0("""
                        name\tval\tdoubled
                        C\t1\t2
                        C\t6\t12
                        C\t18\t36
                        C\t20\t40
                        C\t33\t66
                        C\t39\t78
                        C\t42\t84
                        C\t48\t96
                        C\t71\t142
                        C\t71\t142
                        """,
                """
                        QUERY PLAN
                        Radix sort light
                          keys: [doubled]
                            VirtualRecord
                              functions: [name,memoize(json_extract()),memoize(val*2)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: items
                        """,
                type);
    }

    private void testProjectionInOrderByWith0(String expected, String type) throws SqlException {
        execute("create table items (name string, value " + type + ")");
        execute("insert into items values ('C', 30), ('A', 10), ('B', 20)");

        allowFunctionMemoization();
        assertSql(expected, "select name, value, value * rnd_double() as doubled from items order by doubled");
    }

    private void testProjectionInOrderByWithF(String type) throws SqlException {
        testProjectionInOrderByWith0(
                """
                        name\tvalue\tdoubled
                        B\t20.0\t1.6973928465121335
                        A\t10.0\t2.246301342497259
                        C\t30.0\t19.823333682561998
                        """,
                type
        );
    }

    private void testProjectionInOrderByWithInt(String type) throws SqlException {
        testProjectionInOrderByWith0(
                """
                        name\tvalue\tdoubled
                        B\t20\t1.6973928465121335
                        A\t10\t2.246301342497259
                        C\t30\t19.823333682561998
                        """,
                type
        );
    }
}