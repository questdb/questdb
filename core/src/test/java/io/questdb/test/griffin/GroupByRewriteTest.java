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
    public void testSumDeclaredOperandReusedOutsideAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (1), (1), (2)");
            bindVariableService.setInt("k", 3);
            bindVariableService.setStr("s", "3");
            for (String operand : new String[]{"3", ":k", ":s::INT"}) {
                final String declare = "DECLARE @v := x * " + operand + ", @s := SUM(x * " + operand + ") ";
                assertQuery(declare + "SELECT @v AS v, SUM(@v) AS s FROM t ORDER BY v")
                        .noLeakCheck().expectSize().returns("""
                                v\ts
                                3\t6
                                6\t6
                                """);
                assertQuery(declare + "SELECT @v AS v, SUM(@v) AS s FROM t GROUP BY @v ORDER BY v")
                        .noLeakCheck().expectSize().returns("""
                                v\ts
                                3\t6
                                6\t6
                                """);
                assertQuery(declare + "SELECT SUM(@v) AS s FROM t WHERE @v > 3")
                        .noLeakCheck().noRandomAccess().expectSize().returns("s\n6\n");
                assertQuery(declare + "SELECT SUM(@v) AS s, AVG(@v) AS a FROM t")
                        .noLeakCheck().noRandomAccess().expectSize().withPlanContaining("values: [sum(x),")
                        .returns("""
                                s\ta
                                12\t4.0
                                """);
                assertQuery(declare + "SELECT @s AS a, @s AS b FROM t")
                        .noLeakCheck().noRandomAccess().expectSize().returns("""
                                a\tb
                                12\t12
                                """);
            }
        });
    }

    @Test
    public void testSumIntOperandMatchesLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            bindVariableService.setByte("b", (byte) 2);
            bindVariableService.setShort("h", (short) 2);
            bindVariableService.setInt("k", 2);
            bindVariableService.setInt("n", -2);
            bindVariableService.setStr("s", "2");
            bindVariableService.setStr("m", "-2");
            bindVariableService.setInt(0, 2);
            String empty = "a\tb\tc\td\te\tf\nnull\tnull\tnull\tnull\tnull\tnull\n";
            assertSumArithmetic(empty, "2", ":k", "$1");
            execute("INSERT INTO t VALUES (null)");
            assertSumArithmetic(empty, "2", ":k", "$1");

            // Every column overflows or underflows INT when the arithmetic stays per row.
            execute("INSERT INTO t VALUES (2_147_483_647), (-2_147_483_647), (-2_147_483_647)");
            assertSumArithmetic("""
                            a\tb\tc\td\te\tf
                            -4294967294\t-4294967294\t-2147483641\t-2147483641\t-2147483653\t2147483653
                            """,
                    "2", "2::INT", ":b", ":h", ":k", "$1",
                    ":s::BYTE", ":s::SHORT", ":s::INT", ":s::LONG", "(:s::SHORT)::INT"
            );
            bindVariableService.setInt(0, -2);
            assertSumArithmetic("""
                            a\tb\tc\td\te\tf
                            4294967294\t4294967294\t-2147483653\t-2147483653\t-2147483641\t2147483641
                            """,
                    "-2", "-(-(-2))", "(-2)::INT", ":n", "-:k", "$1", ":m::INT", "-:s::INT"
            );
        });
    }

    @Test
    public void testSumLongOperandMatchesLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            execute("INSERT INTO t VALUES (-4_611_686_018_427_387_904), (1), (null)");
            bindVariableService.setLong("k", 2);
            bindVariableService.setLong("n", -2);
            bindVariableService.setStr("s", "2");
            bindVariableService.setStr("m", "-2");
            // Per row, x * 2 lands on the LONG NULL sentinel; the literal rewrite still counts that row.
            assertSumArithmetic("""
                            a\tb\tc\td\te\tf
                            -9223372036854775806\t-9223372036854775806\t-4611686018427387899\t-4611686018427387899\t-4611686018427387907\t4611686018427387907
                            """,
                    "2", ":k", ":s::LONG"
            );
            assertSumArithmetic("""
                            a\tb\tc\td\te\tf
                            9223372036854775806\t9223372036854775806\t-4611686018427387907\t-4611686018427387907\t-4611686018427387899\t4611686018427387899
                            """,
                    "-2", "-2L", ":n", "-:k", ":m::LONG"
            );
        });
    }

    @Test
    public void testSumNonIntegerAndRowDependentOperands() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (i LONG, x SHORT, y INT)");
            execute("""
                    INSERT INTO t VALUES
                    (10_000_000_000_000_000, 30_000, 927_094),
                    (-10_000_000_000_000_000, 1, 0)
                    """);
            bindVariableService.setDouble("d", 1);
            bindVariableService.setFloat("f", 1);
            // Per-row arithmetic rounds the constant away; hoisting it would turn 0.0 into 2.0.
            assertQuery("""
                    SELECT SUM(i + 1.0) a, SUM(i + :d) b, SUM(i + :f) c, SUM(i + 1::DOUBLE) d,
                           SUM(i + -:d) e, SUM(i + -1::DOUBLE) f, SUM(x * y::INT) g, SUM(x * -y) h,
                           SUM(x / 2) i, SUM(x % 7) j
                    FROM t
                    """)
                    .noLeakCheck().noRandomAccess().expectSize().returns("""
                            a\tb\tc\td\te\tf\tg\th\ti\tj
                            0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t2043016224\t-2043016224\t15000\t6
                            """);
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
    public void testSumRebindMultiplier() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT)");
            execute("INSERT INTO t VALUES (30_000)");
            ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok("initial null", "a\tb\tc\nnull\tnull\tnull\n", b -> {
                b.setInt("k", Numbers.INT_NULL);
                b.setStr("s", null);
            }));
            cases.add(BindVarTuple.ok("small", "a\tb\tc\n60000\t-60000\t60000\n", b -> {
                b.setInt("k", 2);
                b.setStr("s", "2");
            }));
            cases.add(BindVarTuple.ok("overflow", "a\tb\tc\n27812820000\t-27812820000\t27812820000\n", b -> {
                b.setInt("k", 927_094);
                b.setStr("s", "927094");
            }));
            cases.add(BindVarTuple.ok("negative", "a\tb\tc\n-27812820000\t27812820000\t-27812820000\n", b -> {
                b.setInt("k", -927_094);
                b.setStr("s", "-927094");
            }));
            cases.add(BindVarTuple.ok("null again", "a\tb\tc\nnull\tnull\tnull\n", b -> {
                b.setInt("k", Numbers.INT_NULL);
                b.setStr("s", null);
            }));
            assertQuery("SELECT SUM(:k * x) a, SUM(x * -:k) b, SUM(:s::INT * x) c FROM t")
                    .noLeakCheck().noRandomAccess().expectSize().assertBinds(cases);
        });
    }

    @Test
    public void testSumSampleByBindMatchesLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (30_000, '2024-01-01'), (30_000, '2024-01-03')");
            bindVariableService.setStr("k", "927094");
            bindVariableService.setStr("n", "-927094");
            for (String operand : new String[]{"927_094", ":k::INT"}) {
                assertQuery("SELECT SUM(" + operand + " * x) s, ts FROM t SAMPLE BY 1d")
                        .noLeakCheck().timestamp("ts").expectSize().withPlanContaining("values: [sum(x)]").returns("""
                                s\tts
                                27812820000\t2024-01-01T00:00:00.000000Z
                                27812820000\t2024-01-03T00:00:00.000000Z
                                """);
                // FILL keeps the per-row arithmetic so that fill values keep their meaning.
                assertQuery("SELECT SUM(" + operand + " * x) s, ts FROM t SAMPLE BY 1d FILL(7)")
                        .noLeakCheck().timestamp("ts").noRandomAccess().returns("""
                                s\tts
                                2043016224\t2024-01-01T00:00:00.000000Z
                                7\t2024-01-02T00:00:00.000000Z
                                2043016224\t2024-01-03T00:00:00.000000Z
                                """);
            }
            for (String operand : new String[]{"-927_094", ":n::INT"}) {
                assertQuery("SELECT SUM(" + operand + " * x) s, ts FROM t SAMPLE BY 1d")
                        .noLeakCheck().timestamp("ts").expectSize().withPlanContaining("values: [sum(x)]").returns("""
                                s\tts
                                -27812820000\t2024-01-01T00:00:00.000000Z
                                -27812820000\t2024-01-03T00:00:00.000000Z
                                """);
            }
        });
    }

    @Test
    public void testSumUntypedIndexedBind() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT)");
            execute("INSERT INTO t VALUES (30_000)");
            // The compiler infers an untyped bind's type, so the first compilation keeps per-row arithmetic.
            assertUntypedBind("SELECT SUM(x * $1) s FROM t", ColumnType.FLOAT, "2", "s\n60000.0\n");
            assertUntypedBind("SELECT $1 b, SUM(x * $1::INT) a FROM t", ColumnType.DOUBLE, "2.5", "b\ta\n2.5\t60000\n");
            assertUntypedBind("SELECT SUM(x - $1::SHORT) s, MIN(x * $1) m FROM t", ColumnType.DOUBLE, "2.5", "s\tm\n29998\t75000.0\n");
            assertUntypedBind("SELECT SUM($1::INT * x) s FROM t", ColumnType.DOUBLE, "927094", "s\n2043016224\n");
            // A recompilation sees the inferred type and moves the multiplication outside the aggregate.
            try (RecordCursorFactory factory = select("SELECT SUM($1::INT * x) s FROM t")) {
                new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                        .noRandomAccess().expectSize().returns("s\n27812820000\n");
            }
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

    private void assertSumArithmetic(String expected, String... operands) throws Exception {
        for (String operand : operands) {
            assertQuery("SELECT SUM(x * " + operand + ") a, SUM(" + operand + " * x) b, "
                    + "SUM(x + " + operand + ") c, SUM(" + operand + " + x) d, "
                    + "SUM(x - " + operand + ") e, SUM(" + operand + " - x) f FROM t")
                    .noLeakCheck().noRandomAccess().expectSize().returns(expected);
        }
    }

    private void assertUntypedBind(String query, int expectedBindType, String value, String expected) throws Exception {
        bindVariableService.clear();
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertEquals(expectedBindType, bindVariableService.getFunction(0).getType());
            bindVariableService.setStr(0, value);
            new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                    .noRandomAccess().expectSize().returns(expected);
        }
    }
}
