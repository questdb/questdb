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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.BindVarTuple;
import org.junit.Assert;
import org.junit.Test;

public class GroupByRewriteTest extends AbstractCairoTest {

    @Test
    public void testRewriteAggregateDoesNotCreateDuplicateKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym symbol, price double, amount double, ts timestamp) timestamp(ts) partition by day;");
            execute("CREATE TABLE trades2 (sym symbol, price double, amount double, ts timestamp) timestamp(ts) partition by day;");

            // key first
            assertQuery("SELECT ts, price, price / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price/sum]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key first, aliased
            assertQuery("SELECT ts, PricE as price0, price / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price0,price0/sum]
                                Async Group By workers: 1
                                  keys: [ts,price0]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key first, multiple column occurrences
            assertQuery("SELECT ts, price, (price + price) / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price+price/sum]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key first, multiple keys, multiple column occurrences
            assertQuery("SELECT ts, price, price as price0, (price + price) / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price,price+price/sum]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key first, aliased, multiple column occurrences
            assertQuery("SELECT ts, price as price0, (price + price) / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price0,price0+price0/sum]
                                Async Group By workers: 1
                                  keys: [ts,price0]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            // key second
            assertQuery("SELECT ts, price / sum(amount), price FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price/sum,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, aliased
            assertQuery("SELECT ts, price / sum(amount), PricE as price0 FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price/sum,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, aliased, multiple columns
            assertQuery("SELECT ts, sym price, price / sum(amount), price price1 FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price1/sum,price1]
                                Async Group By workers: 1
                                  keys: [ts,price,price1]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, multiple column occurrences
            assertQuery("SELECT ts, (price + price) / sum(amount), price FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price+price/sum,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, multiple keys, multiple column occurrences
            assertQuery("SELECT ts, (price + price) / sum(amount), price, price as price0 FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price+price/sum,price,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, aliased, multiple column occurrences
            assertQuery("SELECT ts, (price + price) / sum(amount), price as price0 FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price+price/sum,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            // joined tables with same column names - the rewrite should not deduplicate the keys
            assertQuery("SELECT t1.ts, t1.price, t2.price / sum(t1.amount) FROM trades t1 JOIN trades2 t2 ON (sym);")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price1/sum]
                                GroupBy vectorized: false
                                  keys: [ts,price,price1]
                                  values: [sum(amount)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: t2.sym=t1.sym
                                          symbolKeyJoin: true
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades2
                            """);
        });
    }

    @Test
    public void testRewriteAggregateExtractsConstantKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price double, amount double, ts timestamp) timestamp(ts) partition by day;");
            assertQuery("SELECT 42, 'foobar', amount, sum(price) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [42,'foobar',amount,sum]
                                Async Group By workers: 1
                                  keys: [amount]
                                  values: [sum(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteAggregateOnJoin1() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( ax int, aid int );");
            execute("INSERT INTO taba values (1,1), (2,2)");
            execute("CREATE TABLE tabb ( bx int, bid int );");
            execute("INSERT INTO tabb values (3,1), (4,2)");

            assertQuery("SELECT sum(ax), sum(bx), sum(ax+10), sum(bx+10) " +
                    "FROM taba " +
                    "join tabb on aid = bid")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            sum\tsum1\tsum2\tsum3
                            3\t7\t23\t27
                            """);
        });
    }

    @Test
    public void testRewriteAggregateOnJoin3() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, aid int );");
            execute("CREATE TABLE tabb ( x int, bid int );");
        });

        assertQuery("SELECT sum(tabc.x*1),sum(x), sum(ax+10), sum(bx+10) " +
                "FROM taba " +
                "join tabb on aid = bid")
                .fails(11, "Invalid table name or alias");
    }

    @Test
    public void testRewriteAggregateOnJoin4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, aid int );");
            execute("CREATE TABLE tabb ( x int, bid int );");
            assertQuery("SELECT sum(taba.k*1),sum(x), sum(ax+10), sum(bx+10) " +
                    "FROM taba " +
                    "join tabb on aid = bid")
                    .fails(11, "Invalid column: taba.k");
        });
    }

    @Test
    public void testRewriteAggregateOnJoinFailsOnAmbiguousColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("  CREATE TABLE taba ( x int, aid int );");
            execute("  CREATE TABLE tabb ( x int, bid int );");
            assertQuery("SELECT sum(x*1),sum(x), sum(ax+10), sum(bx+10) " +
                    "FROM taba " +
                    "join tabb on aid = bid")
                    .fails(11, "Ambiguous column [name=x]");
        });
    }

    @Test
    public void testRewriteAggregateOnOrderBySumBadQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE telemetry (created timestamp)");
            assertQuery("SELECT telemetry.created FROM telemetry ORDER BY SUM(1, 1 IN (telemetry.created), 1);")
                    .noLeakCheck()
                    .fails(49, "there is no matching function `SUM` with the argument types: (INT, BOOLEAN, INT)");
        });
    }

    @Test
    public void testSumDecimalOperandMatchesLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (2_147_483_647), (1), (null)");
            String expected = """
                    a\tb\tc\td\te\tf
                    1073741824.00\t1073741824.00\t2147483649.00\t2147483649.00\t2147483647.00\t-2147483647.00
                    """;
            assertSumArithmetic("0.50::DECIMAL(3, 2)", expected);
            bindVariableService.setStr("k", "0.50");
            assertSumArithmetic(":k::DECIMAL(3, 2)", expected);
        });
    }

    @Test
    public void testSumEmptyAndNullInputs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            bindVariableService.setInt("k", 2);
            String expected = "a\tb\tc\td\te\tf\nnull\tnull\tnull\tnull\tnull\tnull\n";
            assertSumArithmetic("2", expected);
            assertSumArithmetic(":k", expected);
            execute("INSERT INTO t VALUES (null), (null)");
            assertSumArithmetic("2", expected);
            assertSumArithmetic(":k", expected);
        });
    }

    @Test
    public void testSumFloatingPointAdditionKeepsPerRowRounding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (i LONG, d DOUBLE, f FLOAT)");
            execute("""
                    INSERT INTO t VALUES
                    (10_000_000_000_000_000, 1e16, 1e8),
                    (-10_000_000_000_000_000, -1e16, -1e8)
                    """);
            bindVariableService.setDouble("d", 1);
            bindVariableService.setFloat("f", 1);
            // Per-row addition rounds away the constant. Hoisting it would change 0 to 2,
            // including when a floating-point operand promotes an integer column.
            assertQuery("""
                    SELECT SUM(i + 1.0) AS int_literal, SUM(i + :d) AS int_bind,
                           SUM(d + 1.0) AS double_literal, SUM(d + :d) AS double_bind,
                           SUM(f + 1::FLOAT4) AS float_literal, SUM(f + :f) AS float_bind,
                           SUM(i) + COUNT(i) * 1.0 AS hoisted
                    FROM t
                    """)
                    .noLeakCheck().noRandomAccess().expectSize()
                    .columnType(4, ColumnType.FLOAT).columnType(5, ColumnType.FLOAT).returns("""
                            int_literal\tint_bind\tdouble_literal\tdouble_bind\tfloat_literal\tfloat_bind\thoisted
                            0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t2.0
                            """);
        });
    }

    @Test
    public void testSumIndexedBindTypeInference() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT)");
            execute("INSERT INTO t VALUES (30_000)");
            bindVariableService.clear();
            try (RecordCursorFactory factory = select("SELECT SUM(x * $1) AS s FROM t")) {
                // An untyped operand uses the existing floating-point overload. Do not force
                // it to an integer type just to make it eligible for the aggregate rewrite.
                Assert.assertEquals(ColumnType.FLOAT, bindVariableService.getFunction(0).getType());
                bindVariableService.setFloat(0, 2);
                new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                        .noRandomAccess().expectSize().returns("s\n60000.0\n");
                bindVariableService.setFloat(0, 3);
                new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                        .noRandomAccess().expectSize().returns("s\n90000.0\n");
            }
        });
    }

    @Test
    public void testSumIndexedCastBindTypeInference() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT)");
            execute("INSERT INTO t VALUES (30_000)");
            bindVariableService.clear();
            try (RecordCursorFactory factory = select("SELECT SUM($1::INT * x) AS s FROM t")) {
                Assert.assertEquals(ColumnType.DOUBLE, bindVariableService.getFunction(0).getType());
                bindVariableService.setDouble(0, 927_094);
                new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                        .noRandomAccess().expectSize().returns("s\n27812820000\n");
            }
        });
    }

    @Test
    public void testSumIntegerBindArithmeticMatchesLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (2_147_483_647), (null), (1)");
            String expected = """
                    a\tb\tc\td\te\tf
                    4294967296\t4294967296\t2147483652\t2147483652\t2147483644\t-2147483644
                    """;
            assertSumArithmetic("2", expected);
            assertSumArithmetic("2::INT", expected);

            bindVariableService.setByte("b", (byte) 2);
            assertSumArithmetic(":b", expected);
            bindVariableService.setShort("h", (short) 2);
            assertSumArithmetic(":h", expected);
            bindVariableService.setInt("k", 2);
            assertSumArithmetic(":k", expected);
            bindVariableService.setInt(0, 2);
            assertSumArithmetic("$1", expected);
            bindVariableService.setStr("s", "2");
            assertSumArithmetic(":s::BYTE", expected);
            assertSumArithmetic(":s::SHORT", expected);
            assertSumArithmetic(":s::INT", expected);
            assertSumArithmetic(":s::LONG", expected);
            assertSumArithmetic("(:s::SHORT)::INT", expected);
        });
    }

    @Test
    public void testSumLongOverflowMatchesLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            execute("INSERT INTO t VALUES (-4_611_686_018_427_387_904), (1), (null)");
            // Match the existing literal rewrite even when the per-row product would land on LONG_NULL.
            String expected = """
                    a\tb\tc\td\te\tf
                    -9223372036854775806\t-9223372036854775806\t-4611686018427387899\t-4611686018427387899\t-4611686018427387907\t4611686018427387907
                    """;
            assertSumArithmetic("2", expected);
            bindVariableService.setLong("k", 2);
            assertSumArithmetic(":k", expected);
            bindVariableService.setStr("s", "2");
            assertSumArithmetic(":s::LONG", expected);
        });
    }

    @Test
    public void testSumLongResultOverflowMatchesLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            execute("INSERT INTO t VALUES (9_223_372_036_854_775_806), (1), (null)");
            // The outer multiplication and addition overflow LONG; both spellings must wrap alike.
            String expected = """
                    a\tb\tc\td\te\tf
                    -2\t-2\t-9223372036854775805\t-9223372036854775805\t9223372036854775803\t-9223372036854775803
                    """;
            assertSumArithmetic("2", expected);
            bindVariableService.setLong("k", 2);
            assertSumArithmetic(":k", expected);
            bindVariableService.setStr("s", "2");
            assertSumArithmetic(":s::LONG", expected);
        });
    }

    @Test
    public void testSumNonIntegerAndRowDependentOperands() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT, y INT)");
            execute("INSERT INTO t VALUES (30_000, 927_094)");
            bindVariableService.setDouble("d", 927_094.25);
            bindVariableService.setStr("s", "927094.25");
            assertQuery("SELECT SUM(:d * x) a, SUM(:s::DOUBLE * x) b, SUM(y::INT * x) c FROM t")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("a\tb\tc\n2.78128275E10\t2.78128275E10\t2043016224\n");
        });
    }

    @Test
    public void testSumOfAddition1() throws Exception {
        assertAggQuery("""
                        r
                        65
                        """,
                "select sum(x+1) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfAddition2() throws Exception {
        assertAggQuery("""
                        r
                        65
                        """,
                "select sum(1+x) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfAdditionOfDouble1() throws Exception {
        assertAggQuery(
                """
                        r
                        66.0
                        """,
                "select sum(d+1) r from y",
                "create table y as ( select x + 0.1d as d from long_sequence(10) )"
        );
    }

    @Test // all values except first overflow to Infinity, sum overflows to null
    public void testSumOfAdditionOfDouble2() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(d+1) r from y",
                "create table y as ( select 1.7E308 * x as d  from long_sequence(10) )"
        );
    }

    @Test // all values except first are null and thus ignored
    public void testSumOfAdditionOfDouble3() throws Exception {
        assertAggQuery(
                """
                        r
                        2.0
                        """,
                "select sum(d+1) r from y",
                "create table y as ( select (1.7E308 * x)/(1.7E308*x) as d  from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfAdditionOfShort() throws Exception {
        assertAggQuery(
                """
                        r
                        65
                        """,
                "select sum(x+1) r from y",
                "create table y as ( select x::short x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfAdditionOverflow1() throws Exception {
        assertAggQuery(
                """
                        r
                        -9223372036854775805
                        """,
                "select sum(x+9223372036854775807) r from y",
                "create table y as ( select x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfAdditionOverflow2() throws Exception {
        assertAggQuery(
                """
                        r
                        -9223372036854775805
                        """,
                "select sum(x) + 9223372036854775807*3 r from y",
                "create table y as ( select x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfAdditionWithNull() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(x+null) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );

        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(null+x) r from y",
                null
        );
    }

    // multiplication
    @Test
    public void testSumOfMultiplication1() throws Exception {
        assertAggQuery(
                """
                        r
                        55
                        """,
                "select sum(x*1) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfMultiplication2() throws Exception {
        assertAggQuery(
                """
                        r
                        55
                        """,
                "select sum(1*x) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfMultiplicationOfDouble1() throws Exception {
        assertAggQuery(
                """
                        r
                        112.00000000000001
                        """,
                "select sum(d*2) r from y",
                "create table y as ( select x + 0.1d as d from long_sequence(10) )"
        );
    }

    @Test // all values except first overflow to Infinity, sum overflows to null
    public void testSumOfMultiplicationOfDouble2() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(d*2) r from y",
                "create table y as ( select (1.7E308/2)*x as d  from long_sequence(10) )"
        );
    }

    @Test // all values except first are null and thus ignored
    public void testSumOfMultiplicationOfDouble3() throws Exception {
        assertAggQuery(
                """
                        r
                        2.0
                        """,
                "select sum(d*2) r from y",
                "create table y as ( select (1.7E308 * x)/(1.7E308*x) as d  from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfMultiplicationOverflow1() throws Exception {
        assertAggQuery(
                """
                        r
                        -6
                        """,
                "select sum(x*9223372036854775807) r from y",
                "create table y as ( select x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfMultiplicationOverflow2() throws Exception {
        assertAggQuery(
                """
                        r
                        -6
                        """,
                "select sum(x) * 9223372036854775807 r from y",
                "create table y as ( select x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfMultiplicationWithNull() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(x*null) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );

        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(null*x) r from y",
                null
        );
    }

    // subtraction
    @Test
    public void testSumOfSubtraction1() throws Exception {
        assertAggQuery(
                """
                        r
                        45
                        """,
                "select sum(x-1) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfSubtraction2() throws Exception {
        assertAggQuery(
                """
                        r
                        -45
                        """,
                "select sum(1-x) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfSubtractionOfDouble1() throws Exception {
        assertAggQuery(
                """
                        r
                        46.0
                        """,
                "select sum(d-1) r from y",
                "create table y as ( select x + 0.1d as d from long_sequence(10) )"
        );
    }

    @Test // all values except first overflow to Infinity, sum overflows to null
    public void testSumOfSubtractionOfDouble2() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(d-1) r from y",
                "create table y as ( select -1.7E308 * x as d  from long_sequence(10) )"
        );
    }

    @Test // all values except first are null and thus ignored
    public void testSumOfSubtractionOfDouble3() throws Exception {
        assertAggQuery(
                """
                        r
                        0.0
                        """,
                "select sum(d-1) r from y",
                "create table y as ( select (1.7E308 * x)/(1.7E308 * x) as d from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfSubtractionOfShort() throws Exception {
        assertAggQuery(
                """
                        r
                        45
                        """,
                "select sum(x-1) r from y",
                "create table y as ( select x::short x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfSubtractionOverflow1() throws Exception {
        assertAggQuery(
                """
                        r
                        9223372036854775805
                        """,
                "select sum(x-9223372036854775807) r from y",
                "create table y as ( select -x x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfSubtractionOverflow2() throws Exception {
        assertAggQuery(
                """
                        r
                        9223372036854775805
                        """,
                "select sum(x) - 9223372036854775807*3 r from y",
                "create table y as ( select -x x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfSubtractionWithNull() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(x-null) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );

        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(null-x) r from y",
                null
        );
    }

    @Test
    public void testSumRebindIntegerMultiplier() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT)");
            execute("INSERT INTO t VALUES (30_000)");
            ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok("initial null", "s\nnull\n", b -> b.setInt("k", Numbers.INT_NULL)));
            cases.add(BindVarTuple.ok("small", "s\n60000\n", b -> b.setInt("k", 2)));
            cases.add(BindVarTuple.ok("overflow", "s\n27812820000\n", b -> b.setInt("k", 927_094)));
            cases.add(BindVarTuple.ok("negative", "s\n-27812820000\n", b -> b.setInt("k", -927_094)));
            cases.add(BindVarTuple.ok("zero", "s\n0\n", b -> b.setInt("k", 0)));
            cases.add(BindVarTuple.ok("null again", "s\nnull\n", b -> b.setInt("k", Numbers.INT_NULL)));
            assertQuery("SELECT SUM(:k * x) AS s FROM t")
                    .noLeakCheck().noRandomAccess().expectSize().assertBinds(cases);
        });
    }

    @Test
    public void testSumRebindLongMultiplierThroughOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            execute("INSERT INTO t VALUES (4_611_686_018_427_387_903), (1), (null)");
            assertQuery("SELECT SUM(x * 2) AS s FROM t")
                    .noLeakCheck().noRandomAccess().expectSize().returns("s\nnull\n");
            assertQuery("SELECT SUM(x * 3) AS s FROM t")
                    .noLeakCheck().noRandomAccess().expectSize().returns("s\n-4611686018427387904\n");

            ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok("fits", "s\n4611686018427387904\n", b -> b.setLong("k", 1)));
            cases.add(BindVarTuple.ok("null sentinel", "s\nnull\n", b -> b.setLong("k", 2)));
            cases.add(BindVarTuple.ok("wrapped", "s\n-4611686018427387904\n", b -> b.setLong("k", 3)));
            cases.add(BindVarTuple.ok("zero", "s\n0\n", b -> b.setLong("k", 0)));
            cases.add(BindVarTuple.ok("bound null", "s\nnull\n", b -> b.setLong("k", Numbers.LONG_NULL)));
            cases.add(BindVarTuple.ok("fits again", "s\n4611686018427387904\n", b -> b.setLong("k", 1)));
            assertQuery("SELECT SUM(x * :k) AS s FROM t")
                    .noLeakCheck().noRandomAccess().expectSize().assertBinds(cases);
        });
    }

    @Test
    public void testSumRebindStringCastMultiplier() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT)");
            execute("INSERT INTO t VALUES (30_000)");
            ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok("small", "s\n60000\n", b -> b.setStr("k", "2")));
            cases.add(BindVarTuple.ok("overflow", "s\n27812820000\n", b -> b.setStr("k", "927094")));
            cases.add(BindVarTuple.ok("negative", "s\n-27812820000\n", b -> b.setStr("k", "-927094")));
            cases.add(BindVarTuple.ok("null", "s\nnull\n", b -> b.setStr("k", null)));
            cases.add(BindVarTuple.ok("small again", "s\n60000\n", b -> b.setStr("k", "2")));
            assertQuery("SELECT SUM(:k::INT * x) AS s FROM t")
                    .noLeakCheck().noRandomAccess().expectSize().assertBinds(cases);
        });
    }

    @Test
    public void testSumSampleByBindMatchesLiteralOnOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c1 SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (30_000, '2024-01-01'), (-30_000, '2024-01-02')");
            String expected = """
                    k_a\tagg_a\tts_a
                    HQDB\t-27812820000\t2024-01-02T00:00:00.000000Z
                    HQDB\t27812820000\t2024-01-01T00:00:00.000000Z
                    """;
            assertQuery("SELECT 'HQDB' AS k_a, SUM(927094 * c1) AS agg_a, ts AS ts_a FROM t SAMPLE BY 1d ORDER BY 2 ASC, 3")
                    .noLeakCheck().expectSize().returns(expected);
            bindVariableService.setStr("b0", "HQDB");
            bindVariableService.setStr("b1", "927094");
            assertQuery("SELECT :b0::STRING AS k_a, SUM(:b1::INT * c1) AS agg_a, ts AS ts_a FROM t SAMPLE BY 1d ORDER BY 2 ASC, 3")
                    .noLeakCheck().expectSize().withPlanContaining("values: [sum(c1)]").returns(expected);
        });
    }

    @Test
    public void testSumSampleByFillKeepsPerRowArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (30_000, '2024-01-01'), (30_000, '2024-01-03')");
            String expected = """
                    s\tts
                    2043016224\t2024-01-01T00:00:00.000000Z
                    7\t2024-01-02T00:00:00.000000Z
                    2043016224\t2024-01-03T00:00:00.000000Z
                    """;
            assertQuery("SELECT SUM(927094 * x) s, ts FROM t SAMPLE BY 1d FILL(7)")
                    .noLeakCheck().timestamp("ts").noRandomAccess().returns(expected);
            bindVariableService.setStr("k", "927094");
            assertQuery("SELECT SUM(:k::INT * x) s, ts FROM t SAMPLE BY 1d FILL(7)")
                    .noLeakCheck().timestamp("ts").noRandomAccess().returns(expected);
        });
    }

    private void assertAggQuery(
            String expected,
            String query,
            String ddl
    ) throws Exception {
        assertQuery(query)
                .ddl(ddl)
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }

    private void assertSumArithmetic(String operand, String expected) throws Exception {
        assertQuery("SELECT SUM(x * " + operand + ") a, SUM(" + operand + " * x) b, "
                + "SUM(x + " + operand + ") c, SUM(" + operand + " + x) d, "
                + "SUM(x - " + operand + ") e, SUM(" + operand + " - x) f FROM t")
                .noLeakCheck().noRandomAccess().expectSize().returns(expected);
    }
}
