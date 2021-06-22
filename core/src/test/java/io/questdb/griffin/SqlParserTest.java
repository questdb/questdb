/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class SqlParserTest extends AbstractGriffinTest {

    @Test
    public void test2Between() throws Exception {
        assertQuery("select-choose t from (select [t, tt] from x where t between ('2020-01-01','2021-01-02') and tt between ('2021-01-02','2021-01-31'))",
                "select t from x where t between '2020-01-01' and '2021-01-02' and tt between '2021-01-02' and '2021-01-31'",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testAggregateFunctionExpr() throws SqlException {
        assertQuery(
                "select-group-by sum(max(x) + 2) sum, f from (select-virtual [x, f(x) f] x, f(x) f from (select [x] from long_sequence(10)))",
                "select sum(max(x) + 2), f(x) from long_sequence(10)"
        );
    }

    @Test
    public void testOuterJoinRightPredicate() throws SqlException {
        assertQuery(
                "select-choose x, y from (select [x] from l outer join select [y] from r on r.y = l.x post-join-where y > 0)",
                "select x, y\n" +
                        "from l left join r on l.x = r.y\n" +
                        "where y > 0",
                modelOf("l").col("x", ColumnType.INT),
                modelOf("r").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoinRightPredicate1() throws SqlException {
        assertQuery(
                "select-choose x, y from (select [x] from l outer join select [y] from r on r.y = l.x post-join-where y > 0 or y > 10)",
                "select x, y\n" +
                        "from l left join r on l.x = r.y\n" +
                        "where y > 0 or y > 10",
                modelOf("l").col("x", ColumnType.INT),
                modelOf("r").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testAliasSecondJoinTable() throws SqlException {
        assertQuery(
                "select-choose tx.a a, tx.b b from (select [a, b, xid] from x tx outer join select [yid, a, b] from y ty on yid = xid post-join-where ty.a = 1 or ty.b = 2) tx",
                "select tx.a, tx.b from x as tx left join y as ty on xid = yid where ty.a = 1 or ty.b=2",
                modelOf("x").col("xid", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT),
                modelOf("y").col("yid", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testAliasTopJoinTable() throws SqlException {
        assertQuery(
                "select-choose tx.a a, tx.b b from (select [a, b, xid] from x tx outer join select [yid] from y ty on yid = xid where a = 1 or b = 2) tx",
                "select tx.a, tx.b from x as tx left join y as ty on xid = yid where tx.a = 1 or tx.b=2",
                modelOf("x").col("xid", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT),
                modelOf("y").col("yid", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testAliasWithKeyword() throws Exception {
        assertQuery("select-choose x from (select [x] from x as where x > 1) as",
                "x \"as\" where x > 1",
                modelOf("x").col("x", ColumnType.INT));
    }

    @Test
    public void testColumnAliasDoubleQuoted() throws Exception {
        assertQuery("select-choose x aaaasssss from (select [x] from x where x > 1)",
                "select x \"aaaasssss\" from x where x > 1",
                modelOf("x").col("x", ColumnType.INT));
    }

    @Test
    public void testAliasWithSpaceDoubleQuote() throws Exception {
        assertQuery("select-choose x from (select [x] from x 'b a' where x > 1) 'b a'",
                "x \"b a\" where x > 1",
                modelOf("x").col("x", ColumnType.INT));
    }

    @Test
    public void testAliasWithSpaceX() throws Exception {
        assertSyntaxError("from x 'a b' where x > 1", 7, "unexpected");
    }

    @Test
    public void testAmbiguousColumn() throws Exception {
        assertSyntaxError("orders join customers on customerId = customerId", 25, "Ambiguous",
                modelOf("orders").col("customerId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testAnalyticFunctionReferencesSameColumnAsVirtual() throws Exception {
        assertQuery(
                "select-analytic a, b1, f(c) f over (partition by b11 order by ts) from (select-virtual [a, concat(b,'abc') b1, c, b1 b11, ts] a, concat(b,'abc') b1, c, b1 b11, ts from (select-choose [a, b, c, b b1, ts] a, b, c, b b1, ts from (select [a, b, c, ts] from xyz k timestamp (ts)) k) k) k",
                "select a, concat(k.b, 'abc') b1, f(c) over (partition by k.b order by k.ts) from xyz k",
                modelOf("xyz")
                        .col("c", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("a", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testAnalyticLiteralAfterFunction() throws Exception {
        assertQuery(
                "select-analytic a, b1, f(c) f over (partition by b11 order by ts), b from (select-virtual [a, concat(b,'abc') b1, c, b1 b11, ts, b1 b] a, concat(b,'abc') b1, c, b1 b11, ts, b1 b from (select-choose [a, b, c, b b1, ts] a, b, c, b b1, ts from (select [a, b, c, ts] from xyz k timestamp (ts)) k) k) k",
                "select a, concat(k.b, 'abc') b1, f(c) over (partition by k.b order by k.ts), b from xyz k",
                modelOf("xyz")
                        .col("c", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("a", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testAnalyticOrderDirection() throws Exception {
        assertQuery(
                "select-analytic a, b, f(c) my over (partition by b order by ts desc, x, y) from (select [a, b, c, ts, x, y] from xyz timestamp (ts))",
                "select a,b, f(c) over (partition by b order by ts desc, x asc, y) my from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testAnalyticPartitionByMultiple() throws Exception {
        assertQuery(
                "select-analytic a, b, f(c) my over (partition by b, a order by ts), d(c) d over () from (select [a, b, c, ts] from xyz timestamp (ts))",
                "select a,b, f(c) over (partition by b, a order by ts) my, d(c) over() from xyz",
                modelOf("xyz")
                        .col("c", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("a", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testAsOfJoin() throws SqlException {
        assertQuery("select-choose t.timestamp timestamp, t.tag tag, q.timestamp timestamp1 from (select [timestamp, tag] from trades t timestamp (timestamp) asof join select [timestamp] from quotes q timestamp (timestamp) where tag = null) t",
                "trades t ASOF JOIN quotes q WHERE tag = null",
                modelOf("trades").timestamp().col("tag", ColumnType.SYMBOL),
                modelOf("quotes").timestamp()
        );
    }

    @Test
    public void testAsOfJoinColumnAliasNull() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c asof join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = null) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " asof join orders o on c.customerId = o.customerId) " +
                        " where kk = null limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testAsOfJoinOrder() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, e.employeeId employeeId, o.customerId customerId1 from (select [customerId] from customers c asof join select [employeeId] from employees e on e.employeeId = c.customerId join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " asof join employees e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId",
                modelOf("customers").col("customerId", ColumnType.SYMBOL),
                modelOf("employees").col("employeeId", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.SYMBOL));
    }

    @Test
    public void testAsOfJoinOuterWhereClause() throws Exception {
        assertQuery(
                "select-choose trade_time, quote_time, trade_price, trade_size, quote_price, quote_size from (select-choose [trade.ts trade_time, book.ts quote_time, trade.price trade_price, trade.size trade_size, book.price quote_price, book.size quote_size] trade.ts trade_time, book.ts quote_time, trade.price trade_price, trade.size trade_size, book.price quote_price, book.size quote_size from (select [ts, price, size, sym] from trade asof join select [ts, price, size, sym] from book on book.sym = trade.sym post-join-where book.price != NaN))",
                "select * from \n" +
                        "(\n" +
                        "select \n" +
                        "trade.ts as trade_time,\n" +
                        "book.ts as quote_time,\n" +
                        "trade.price as trade_price,\n" +
                        "trade.size as trade_size,\n" +
                        "book.price as quote_price,\n" +
                        "book.size as quote_size\n" +
                        "from\n" +
                        "trade asof join book\n" +
                        "where trade.sym = book.sym\n" +
                        ")\n" +
                        "where quote_price != NaN;",
                modelOf("trade")
                        .col("ts", ColumnType.TIMESTAMP)
                        .col("sym", ColumnType.SYMBOL)
                        .col("price", ColumnType.DOUBLE)
                        .col("size", ColumnType.LONG),
                modelOf("book")
                        .col("ts", ColumnType.TIMESTAMP)
                        .col("sym", ColumnType.SYMBOL)
                        .col("price", ColumnType.DOUBLE)
                        .col("size", ColumnType.LONG)
        );
    }

    @Test
    public void testAsOfJoinSubQuery() throws Exception {
        // execution order must be (src: SQL Server)
        //        1. FROM
        //        2. ON
        //        3. JOIN
        //        4. WHERE
        //        5. GROUP BY
        //        6. WITH CUBE or WITH ROLLUP
        //        7. HAVING
        //        8. SELECT
        //        9. DISTINCT
        //        10. ORDER BY
        //        11. TOP
        //
        // which means "where" clause for "e" table has to be explicitly as post-join-where
        assertQuery(
                "select-choose c.customerId customerId, e.blah blah, e.lastName lastName, e.employeeId employeeId, e.timestamp timestamp, o.customerId customerId1 from (select [customerId] from customers c asof join select [blah, lastName, employeeId, timestamp] from (select-virtual ['1' blah, lastName, employeeId, timestamp] '1' blah, lastName, employeeId, timestamp from (select [lastName, employeeId, timestamp] from employees) order by lastName) e on e.employeeId = c.customerId post-join-where e.lastName = 'x' and e.blah = 'y' join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " asof join (select '1' blah, lastName, employeeId, timestamp from employees order by lastName) e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId where e.lastName = 'x' and e.blah = 'y'",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP),
                modelOf("orders")
                        .col("customerId", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testAsOfJoinSubQueryInnerPredicates() throws Exception {
        // which means "where" clause for "e" table has to be explicitly as post-join-where
        assertQuery(
                "select-choose c.customerId customerId, e.blah blah, e.lastName lastName, e.employeeId employeeId, e.timestamp timestamp, o.customerId customerId1 from (select [customerId] from customers c asof join select [blah, lastName, employeeId, timestamp] from (select-virtual ['1' blah, lastName, employeeId, timestamp] '1' blah, lastName, employeeId, timestamp from (select [lastName, employeeId, timestamp] from employees where lastName = 'x') where blah = 'y' order by lastName) e on e.employeeId = c.customerId join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " asof join (select '1' blah, lastName, employeeId, timestamp from employees order by lastName) e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId and e.lastName = 'x' and e.blah = 'y'",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP),
                modelOf("orders")
                        .col("customerId", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testAsOfJoinSubQuerySimpleAlias() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, a.blah blah, a.lastName lastName, a.customerId customerId1, a.timestamp timestamp from (select [customerId] from customers c asof join select [blah, lastName, customerId, timestamp] from (select-virtual ['1' blah, lastName, customerId, timestamp] '1' blah, lastName, customerId, timestamp from (select-choose [lastName, employeeId customerId, timestamp] lastName, employeeId customerId, timestamp from (select [lastName, employeeId, timestamp] from employees)) order by lastName) a on a.customerId = c.customerId) c",
                "customers c" +
                        " asof join (select '1' blah, lastName, employeeId customerId, timestamp from employees order by lastName) a on (customerId)",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testAsOfJoinSubQuerySimpleNoAlias() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, _xQdbA0.blah blah, _xQdbA0.lastName lastName, _xQdbA0.customerId customerId1, _xQdbA0.timestamp timestamp from (select [customerId] from customers c asof join select [blah, lastName, customerId, timestamp] from (select-virtual ['1' blah, lastName, customerId, timestamp] '1' blah, lastName, customerId, timestamp from (select-choose [lastName, employeeId customerId, timestamp] lastName, employeeId customerId, timestamp from (select [lastName, employeeId, timestamp] from employees)) order by lastName) _xQdbA0 on _xQdbA0.customerId = c.customerId) c",
                "customers c" +
                        " asof join (select '1' blah, lastName, employeeId customerId, timestamp from employees order by lastName) on (customerId)",
                modelOf("customers").col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testBadTableExpression() throws Exception {
        assertSyntaxError(")", 0, "table name expected");
    }

    @Test
    public void testBetween() throws Exception {
        assertQuery("select-choose t from (select [t] from x where t between ('2020-01-01','2021-01-02'))",
                "x where t between '2020-01-01' and '2021-01-02'",
                modelOf("x").col("t", ColumnType.TIMESTAMP));
    }

    @Test
    public void testBetweenInsideCast() throws Exception {
        assertQuery("select-virtual cast(t between (cast('2020-01-01',TIMESTAMP),'2021-01-02'),INT) + 1 column from (select [t] from x)",
                "select CAST(t between CAST('2020-01-01' AS TIMESTAMP) and '2021-01-02' AS INT) + 1 from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testBetweenUnfinished() throws Exception {
        assertSyntaxError("select tt from x where t between '2020-01-01'",
                25,
                "too few arguments for 'between' [found=2,expected=3]",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testBetweenWithCase() throws Exception {
        assertQuery("select-virtual case(t between (cast('2020-01-01',TIMESTAMP),'2021-01-02'),'a','b') case from (select [t] from x)",
                "select case when t between CAST('2020-01-01' AS TIMESTAMP) and '2021-01-02' then 'a' else 'b' end from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testBetweenWithCast() throws Exception {
        assertQuery("select-choose t from (select [t] from x where t between (cast('2020-01-01',TIMESTAMP),'2021-01-02'))",
                "select t from x where t between CAST('2020-01-01' AS TIMESTAMP) and '2021-01-02'",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testBetweenWithCastAndSum() throws Exception {
        assertQuery("select-choose tt from (select [tt, t] from x where t between ('2020-01-01',now() + cast(NULL,LONG)))",
                "select tt from x where t between '2020-01-01' and (now() + CAST(NULL AS LONG))",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testBetweenWithCastAndSum2() throws Exception {
        assertQuery("select-choose tt from (select [tt, t] from x where t between (now() + cast(NULL,LONG),'2020-01-01'))",
                "select tt from x where t between (now() + CAST(NULL AS LONG)) and '2020-01-01'",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testBlockCommentAtMiddle() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where /*this is a random comment */a > 1) 'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT));
    }

    @Test
    public void testBlockCommentNested() throws Exception {
        assertQuery("select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) /* comment /* ok */  whatever */'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT));
    }

    @Test
    public void testBlockCommentUnclosed() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) 'b a' where x > 1 /* this block comment",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT));
    }

    @Test
    public void testCaseAndLimit() throws SqlException {
        assertQuery("select-virtual 'table' kind from (tab) limit 10",
                "    select case a \n" +
                        "    else 'table'\n" +
                        "    end kind from tab limit 10\n",
                modelOf("tab").col("a", ColumnType.CHAR).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseImpossibleRewrite1() throws SqlException {
        // referenced columns in 'when' clauses are different
        assertQuery(
                "select-virtual case(a = 1,'A',2 = b,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when 2 = b then 'B' else 'C' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseImpossibleRewrite2() throws SqlException {
        // 'when' is non-constant
        assertQuery(
                "select-virtual case(a = 1,'A',2 + b = a,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when 2 + b = a then 'B' else 'C' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseNoElseClause() throws SqlException {
        // referenced columns in 'when' clauses are different
        assertQuery(
                "select-virtual case(a = 1,'A',2 = b,'B') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when 2 = b then 'B' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseNoWhen() throws SqlException {
        assertQuery(
                "select-virtual 'table' kind from (tab)",
                "    select case a \n" +
                        "    else 'table'\n" +
                        "    end kind from tab\n",
                modelOf("tab").col("a", ColumnType.CHAR).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseNoWhenBinary() throws SqlException {
        assertQuery(
                "select-virtual 2 + 5 kind from (tab)",
                "    select case a \n" +
                        "    else 2 + 5\n" +
                        "    end kind from tab\n",
                modelOf("tab").col("a", ColumnType.CHAR).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseToSwitchExpression() throws SqlException {
        assertQuery(
                "select-virtual switch(a,1,'A',2,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when a = 2 then 'B' else 'C' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testCaseToSwitchExpression2() throws SqlException {
        // this test has inverted '=' arguments but should still be rewritten to 'switch'
        assertQuery(
                "select-virtual switch(a,1,'A',2,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case when a = 1 then 'A' when 2 = a then 'B' else 'C' end+1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testColumnTopToBottom() throws SqlException {
        assertQuery(
                "select-choose x.i i, x.sym sym, x.amt amt, price, x.timestamp timestamp, y.timestamp timestamp1 from (select [i, sym, amt, timestamp] from x timestamp (timestamp) splice join select [price, timestamp, sym2, trader] from y timestamp (timestamp) on y.sym2 = x.sym post-join-where trader = 'ABC')",
                "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x splice join y on y.sym2 = x.sym where trader = 'ABC'",
                modelOf("x")
                        .col("i", ColumnType.INT)
                        .col("sym", ColumnType.SYMBOL)
                        .col("amt", ColumnType.DOUBLE)
                        .col("comment", ColumnType.STRING)
                        .col("venue", ColumnType.SYMBOL)
                        .timestamp(),
                modelOf("y")
                        .col("price", ColumnType.DOUBLE)
                        .col("sym2", ColumnType.SYMBOL)
                        .col("trader", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testColumnsOfSimpleSelectWithSemicolon() throws SqlException {
        assertColumnNames("select 1;", "1");
        assertColumnNames("select 1, 1, 1;", "1", "11", "12");
    }

    @Test
    public void testConcat3Args() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',x,'3') concat from (select [x] from tab)",
                "select 1, x, concat('2', x, '3') from tab",
                modelOf("tab").col("x", ColumnType.STRING)
        );
    }

    @Test
    public void testConcatSimple() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',x) concat from (select [x] from tab)",
                "select 1, x, '2' || x from tab",
                modelOf("tab").col("x", ColumnType.STRING)
        );
    }

    @Test
    public void testConsistentColumnOrder() throws SqlException {
        assertQuery(
                "select-choose rnd_int, rnd_int1, rnd_boolean, rnd_str, rnd_double, rnd_float, rnd_short, rnd_short1, rnd_date, rnd_timestamp, rnd_symbol, rnd_long, rnd_long1, ts, rnd_byte, rnd_bin from (select-virtual [rnd_int() rnd_int, rnd_int(0,30,2) rnd_int1, rnd_boolean() rnd_boolean, rnd_str(3,3,2) rnd_str, rnd_double(2) rnd_double, rnd_float(2) rnd_float, rnd_short(10,1024) rnd_short, rnd_short() rnd_short1, rnd_date(to_date('2015','yyyy'),to_date('2016','yyyy'),2) rnd_date, rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) rnd_timestamp, rnd_symbol(4,4,4,2) rnd_symbol, rnd_long(100,200,2) rnd_long, rnd_long() rnd_long1, timestamp_sequence(0,1000000000) ts, rnd_byte(2,50) rnd_byte, rnd_bin(10,20,2) rnd_bin] rnd_int() rnd_int, rnd_int(0,30,2) rnd_int1, rnd_boolean() rnd_boolean, rnd_str(3,3,2) rnd_str, rnd_double(2) rnd_double, rnd_float(2) rnd_float, rnd_short(10,1024) rnd_short, rnd_short() rnd_short1, rnd_date(to_date('2015','yyyy'),to_date('2016','yyyy'),2) rnd_date, rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) rnd_timestamp, rnd_symbol(4,4,4,2) rnd_symbol, rnd_long(100,200,2) rnd_long, rnd_long() rnd_long1, timestamp_sequence(0,1000000000) ts, rnd_byte(2,50) rnd_byte, rnd_bin(10,20,2) rnd_bin from (long_sequence(20)))",
                "select * from (select" +
                        " rnd_int()," +
                        " rnd_int(0, 30, 2)," +
                        " rnd_boolean()," +
                        " rnd_str(3,3,2)," +
                        " rnd_double(2)," +
                        " rnd_float(2)," +
                        " rnd_short(10,1024)," +
                        " rnd_short()," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " rnd_symbol(4,4,4,2)," +
                        " rnd_long(100,200,2)," +
                        " rnd_long()," +
                        " timestamp_sequence(0, 1000000000) ts," +
                        " rnd_byte(2,50)," +
                        " rnd_bin(10, 20, 2)" +
                        " from long_sequence(20))"
        );
    }

    @Test
    public void testConstantFunctionAsArg() throws Exception {
        assertQuery("select-choose customerId from (select [customerId] from customers where f(1.2) > 1)",
                "select * from customers where f(1.2) > 1",
                modelOf("customers").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testCount() throws Exception {
        assertQuery("select-group-by customerId, count() count from (select-choose [c.customerId customerId] c.customerId customerId from (select [customerId] from customers c outer join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = NaN) c) c",
                "select c.customerId, count() from customers c" +
                        " outer join orders o on c.customerId = o.customerId " +
                        " where o.customerId = NaN",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("product", ColumnType.STRING)
        );
    }

    @Test
    public void testCountStarRewrite() throws SqlException {
        assertQuery(
                "select-group-by count() count, x from (select [x] from long_sequence(10))",
                "select count(*), x from long_sequence(10)"
        );
    }

    @Test
    public void testCountStarRewriteLeaveArgs() throws SqlException {
        assertQuery(
                "select-group-by x, count(2 * x) count from (select [x] from long_sequence(10))",
                "select x, count(2*x) from long_sequence(10)"
        );
    }

    @Test
    public void testCreateAsSelectInvalidIndex() throws Exception {
        assertSyntaxError(
                "create table X as ( select a, b, c from tab ), index(x)",
                53,
                "Invalid column",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateAsSelectMissingTimestamp() throws Exception {
        assertSyntaxError(
                "create table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 100000000000l) t from long_sequence(100000))) partition by DAY",
                0,
                "timestamp is not defined"
        );
    }

    @Test
    public void testCreateAsSelectTimestampNotRequired() throws SqlException {
        assertCreateTable(
                "create table tst as (select-choose a, b, t from (select-virtual [rnd_int() a, rnd_double() b, timestamp_sequence(0,100000000000l) t] rnd_int() a, rnd_double() b, timestamp_sequence(0,100000000000l) t from (long_sequence(100000))))",
                "create table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 100000000000l) t from long_sequence(100000)))"
        );
    }

    @Test
    public void testCreateNameDot() throws Exception {
        assertSyntaxError(
                "create table . as ( select a, b, c from tab )",
                13,
                "'.' is an invalid table name",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateNameFullOfHacks() throws Exception {
        assertSyntaxError(
                "create table '../../../' as ( select a, b, c from tab )",
                13,
                "'.' is not allowed",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateNameWithDot() throws Exception {
        assertSyntaxError(
                "create table X.y as ( select a, b, c from tab )",
                14,
                "unexpected token: .",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTable() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 128 cache," +
                        " z STRING," +
                        " y BOOLEAN) timestamp(t) partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH");
    }

    @Test
    public void testCreateTableAsSelect() throws SqlException {
        assertCreateTable(
                "create table X as (select-choose a, b, c from (select [a, b, c] from tab))",
                "create table X as ( select a, b, c from tab )",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTableAsSelectIndex() throws SqlException {
        assertCreateTable(
                "create table X as (select-choose a, b, c from (select [a, b, c] from tab)), index(b capacity 256)",
                "create table X as ( select a, b, c from tab ), index(b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableAsSelectIndexCapacity() throws SqlException {
        assertCreateTable(
                "create table X as (select-choose a, b, c from (select [a, b, c] from tab)), index(b capacity 64)",
                "create table X as ( select a, b, c from tab ), index(b capacity 64)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableAsSelectTimestamp() throws SqlException {
        assertCreateTable(
                "create table X as (select-choose a, b, c from (select [a, b, c] from tab)) timestamp(b)",
                "create table X as ( select a, b, c from tab ) timestamp(b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.DOUBLE)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableBadColumnDef() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP blah, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR index",
                61,
                "',' or ')' expected"
        );
    }

    @Test
    public void testCreateTableCacheCapacity() throws SqlException {
        assertCreateTable("create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 64 cache," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by YEAR",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL CAPACITY 64 CACHE, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "TIMESTAMP(t) " +
                        "PARTITION BY YEAR");
    }

    @Test
    public void testCreateTableCastCapacityDef() throws SqlException {
        assertCreateTable(
                "create table x as (select-choose a, b, c from (select [a, b, c] from tab)), cast(a as DOUBLE:35), cast(c as SYMBOL:54 capacity 16 cache)",
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 16)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastDef() throws SqlException {
        // these numbers in expected string are position of type keyword
        assertCreateTable(
                "create table x as (select-choose a, b, c from (select [a, b, c] from tab)), cast(a as DOUBLE:35), cast(c as SYMBOL:54 capacity 128 cache)",
                "create table x as (tab), cast(a as double), cast(c as symbol)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastDefSymbolCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 1100000000)",
                70,
                "max symbol capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastDefSymbolCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity -10)",
                70,
                "min symbol capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache index capacity 100000000)",
                96,
                "max index block capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache index capacity 1)",
                96,
                "min index block capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexDef() throws SqlException {
        assertCreateTable(
                "create table x as (select-choose a, b, c from (select [a, b, c] from tab)), cast(a as DOUBLE:35), cast(c as SYMBOL:54 capacity 32 nocache index capacity 512)",
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache index capacity 300)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexInvalidCapacity() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache index capacity -)",
                97,
                "bad integer",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastIndexNegativeCapacity() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20 nocache index capacity -3)",
                96,
                "min index block capacity is",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastRoundedSymbolCapacityDef() throws SqlException {
        // 20 is rounded to next power of 2, which is 32
        assertCreateTable(
                "create table x as (select-choose a, b, c from (select [a, b, c] from tab)), cast(a as DOUBLE:35), cast(c as SYMBOL:54 capacity 32 cache)",
                "create table x as (tab), cast(a as double), cast(c as symbol capacity 20)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)

        );
    }

    @Test
    public void testCreateTableCastUnsupportedType() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(b as integer)",
                35,
                "unsupported column type",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTableDuplicateCast() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(b as double), cast(b as long)",
                49,
                "duplicate cast",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTableDuplicateColumn() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "t BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR",
                122,
                "Duplicate column"
        );
    }

    @Test
    public void testCreateTableForKafka() throws SqlException {
        assertCreateTable(
                "create table quickstart-events4 (" +
                        "flag BOOLEAN, " +
                        "id8 SHORT, " +
                        "id16 SHORT, " +
                        "id32 INT, " +
                        "id64 LONG, " +
                        "idFloat FLOAT, " +
                        "idDouble DOUBLE, " +
                        "idBytes STRING, " +
                        "msg STRING)",
                "CREATE TABLE \"quickstart-events4\" (\n" +
                        "\"flag\" BOOLEAN NOT NULL,\n" +
                        "\"id8\" SMALLINT NOT NULL,\n" +
                        "\"id16\" SMALLINT NOT NULL,\n" +
                        "\"id32\" INT NOT NULL,\n" +
                        "\"id64\" BIGINT NOT NULL,\n" +
                        "\"idFloat\" REAL NOT NULL,\n" +
                        "\"idDouble\" DOUBLE PRECISION NOT NULL,\n" +
                        "\"idBytes\" BYTEA NOT NULL,\n" +
                        "\"msg\" TEXT NULL)");
    }

    @Test
    public void testCreateTableIf() throws Exception {
        assertSyntaxError("create table if", 15, "'not' expected");
    }

    @Test
    public void testCreateTableIfNot() throws Exception {
        assertSyntaxError("create table if not", 19, "'exists' expected");
    }

    @Test
    public void testCreateTableIfNotTable() throws Exception {
        assertSyntaxError("create table if not x", 20, "'if not exists' expected");
    }

    @Test
    public void testCreateTableIfTable() throws Exception {
        assertSyntaxError("create table if x", 16, "'if not exists' expected");
    }

    @Test
    public void testCreateTableInPlaceIndex() throws SqlException {
        assertCreateTable("create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 128 cache index capacity 256," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by YEAR",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " + // <-- index here
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR");
    }

    @Test
    public void testCreateTableInPlaceIndexAndBlockSize() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 128 cache index capacity 128," +
                        " z STRING," +
                        " y BOOLEAN) timestamp(t) partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity 128, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH");
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity 10000000, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                122,
                "max index block capacity is"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityInvalid() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity -, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                123,
                "bad integer"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity 2, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                122,
                "min index block capacity is"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityLow2() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity -9, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                122,
                "min index block capacity is"
        );
    }

    @Test
    public void testCreateTableInPlaceIndexCapacityRounding() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 128 cache index capacity 128," +
                        " z STRING," +
                        " y BOOLEAN) timestamp(t) partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL index capacity 120, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH");
    }

    @Test
    public void testCreateTableInvalidCapacity() throws Exception {
        assertSyntaxError(
                "create table x (a symbol capacity z)",
                34,
                "bad integer"
        );
    }

    @Test
    public void testCreateTableInvalidColumnInIndex() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN), " +
                        "index(k) " +
                        "timestamp(t) " +
                        "partition by YEAR",
                109,
                "Invalid column"
        );
    }

    @Test
    public void testCreateTableInvalidColumnType() throws Exception {
        assertSyntaxError(
                "create table tab (a int, b integer)",
                27,
                "unsupported column type"
        );
    }

    @Test
    public void testCreateTableInvalidPartitionBy() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by EPOCH",
                128,
                "'NONE', 'DAY', 'MONTH' or 'YEAR' expected"
        );
    }

    @Test
    public void testCreateTableInvalidTimestampColumn() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(zyz) " +
                        "partition by YEAR",
                112,
                "Invalid column"
        );
    }

    @Test
    public void testCreateTableMisplacedCastCapacity() throws Exception {
        assertSyntaxError(
                "create table x as (tab), cast(a as double capacity 16)",
                42,
                "')' expected",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.STRING)
        );
    }

    @Test
    public void testCreateTableMisplacedCastDef() throws Exception {
        assertSyntaxError(
                "create table tab (a int, b long), cast (a as double)",
                34,
                "cast is only supported"
        );
    }

    @Test
    public void testCreateTableMissingColumnDef() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN, ) " +
                        "timestamp(t) " +
                        "partition by YEAR index",
                102,
                "missing column definition"
        );
    }

    @Test
    public void testCreateTableMissingDef() throws Exception {
        assertSyntaxError("create table xyx", 16, "'(' or 'as' expected");
    }

    @Test
    public void testCreateTableMissingName() throws Exception {
        assertSyntaxError("create table ", 13, "table name or 'if' expected");
    }

    @Test
    public void testCreateTableNoCache() throws SqlException {
        assertCreateTable("create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 128 nocache," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by YEAR",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL NOCACHE, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "TIMESTAMP(t) " +
                        "PARTITION by YEAR");
    }

    @Test
    public void testGroupByConstantMatchingColumnName() throws SqlException {
        assertQuery(
                "select-virtual 'nts' nts, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select 'nts', min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP));
    }

    @Test
    public void testGroupByConstantFunctionMatchingColumnName() throws SqlException {
        assertQuery(
                "select-virtual now() now, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select now(), min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP));
    }

    @Test
    public void testGroupByConstant1() throws SqlException {
        assertQuery(
                "select-virtual 'nts' nts, now() now, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select 'nts', now(), min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP));
    }

    @Test
    public void testGroupByConstant2() throws SqlException {
        assertQuery(
                "select-virtual min, 'a' a from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select min(nts), 'a' from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP));
    }

    @Test
    public void testGroupByConstant3() throws SqlException {
        assertQuery(
                "select-virtual 1 + 1 column, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select 1+1, min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP));
    }

    @Test
    public void testGroupByConstant4() throws SqlException {
        assertQuery(
                "select-virtual min, 1 + 2 * 3 column from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select min(nts), 1 + 2 * 3 from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP));
    }

    @Test
    public void testGroupByConstant5() throws SqlException {
        assertQuery(
                "select-virtual min, 1 + now() * 3 column from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select min(nts), 1 + now() * 3 from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP));
    }

    @Test
    public void testGroupByConstant6() throws SqlException {
        assertQuery(
                "select-virtual now() + now() column, min from (select-group-by [min(nts) min] min(nts) min from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select now() + now(), min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP));
    }

    @Test
    public void testGroupByNotConstant1() throws SqlException {
        assertQuery(
                "select-group-by min(nts) min, column from (select-virtual [nts, 1 + day(nts) * 3 column] nts, 1 + day(nts) * 3 column from (select [nts] from tt timestamp (dts) where nts > '2020-01-01T00:00:00.000000Z'))",
                "select min(nts), 1 + day(nts) * 3 from tt where nts > '2020-01-01T00:00:00.000000Z'",
                modelOf("tt").timestamp("dts").col("nts", ColumnType.TIMESTAMP));
    }

    @Test
    public void testCreateTableNoCacheIndex() throws SqlException {
        assertCreateTable("create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " t TIMESTAMP," +
                        " x SYMBOL capacity 128 nocache index capacity 256," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by YEAR",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL nocache index, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR");
    }

    @Test
    public void testCreateTableOutOfPlaceIndex() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT index capacity 256," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 128 cache index capacity 256," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a) " +
                        ", index (x) " +
                        "timestamp(t) " +
                        "partition by MONTH");
    }

    @Test
    public void testCreateTableOutOfPlaceIndexAndCapacity() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT index capacity 16," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 128 cache index capacity 32," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity 24) " +
                        "timestamp(t) " +
                        "partition by MONTH");
    }

    @Test
    public void testCreateTableOutOfPlaceIndexCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity 10000000) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                173,
                "max index block capacity is");
    }

    @Test
    public void testCreateTableOutOfPlaceIndexCapacityInvalid() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity -) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                174,
                "bad integer");
    }

    @Test
    public void testCreateTableOutOfPlaceIndexCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity 1) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                173,
                "min index block capacity is");
    }

    @Test
    public void testCreateTableOutOfPlaceIndexCapacityLow2() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        ", index (a capacity 16) " +
                        ", index (x capacity -10) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                173,
                "min index block capacity is");
    }

    @Test
    public void testCreateTableRoundedSymbolCapacity() throws SqlException {
        assertCreateTable(
                "create table x (" +
                        "a INT," +
                        " b BYTE," +
                        " c SHORT," +
                        " t TIMESTAMP," +
                        " d LONG," +
                        " e FLOAT," +
                        " f DOUBLE," +
                        " g DATE," +
                        " h BINARY," +
                        " x SYMBOL capacity 512 cache," +
                        " z STRING," +
                        " y BOOLEAN)" +
                        " timestamp(t)" +
                        " partition by MONTH",
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "t TIMESTAMP, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "x SYMBOL capacity 500, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH");
    }

    @Test
    public void testCreateTableSymbolCapacityHigh() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 1100000000, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR",
                80,
                "max symbol capacity is"
        );
    }

    @Test
    public void testCreateTableSymbolCapacityLow() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity -10, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR",
                80,
                "min symbol capacity is"
        );
    }

    @Test
    public void testCreateTableUnexpectedToken() throws Exception {
        assertSyntaxError(
                "create table x blah",
                15,
                "unexpected token"
        );
    }

    @Test
    public void testCreateTableUnexpectedToken2() throws Exception {
        assertSyntaxError(
                "create table x (a int, b double), xyz",
                34,
                "unexpected token"
        );
    }

    @Test
    public void testCreateTableUnexpectedTrailingToken() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by YEAR index",
                133,
                "unexpected token"
        );
    }

    @Test
    public void testCreateTableUnexpectedTrailingToken2() throws Exception {
        assertSyntaxError(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL index, " +
                        "z STRING, " +
                        "bool BOOLEAN) " +
                        "timestamp(t) " +
                        " index",
                116,
                "unexpected token"
        );
    }

    @Test
    public void testCreateTableWitInvalidCommitLag() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH commitLag=asif,",
                90,
                "invalid interval qualifier asif");
    }

    @Test
    public void testCreateTableWitInvalidMaxUncommittedRows() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=asif,",
                95,
                "could not parse maxUncommittedRows value \"asif\"");
    }

    @Test
    public void testCreateTableWithInvalidParameter1() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, o3invalid=250ms",
                112,
                "unrecognized o3invalid after WITH");
    }

    @Test
    public void testCreateTableWithInvalidParameter2() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000 x commitLag=250ms",
                96,
                "unexpected token: x");
    }

    @Test
    public void testCreateTableWithO3() throws Exception {
        assertCreateTable(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY",
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, commitLag=250ms;");
    }

    @Test
    public void testCreateTableWithPartialParameter1() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, commitLag=",
                106,
                "too few arguments for '=' [found=1,expected=2]");
    }

    @Test
    public void testCreateTableWithPartialParameter2() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000, commitLag",
                106,
                "expected parameter after WITH");
    }

    @Test
    public void testCreateTableWithPartialParameter3() throws Exception {
        assertSyntaxError(
                "create table x (a INT, t TIMESTAMP) timestamp(t) partition by DAY WITH maxUncommittedRows=10000,",
                95,
                "unexpected token: ,");
    }

    @Test
    public void testCreateUnsupported() throws Exception {
        assertSyntaxError("create object x", 7, "table");
    }

    @Test
    public void testCrossJoin() throws Exception {
        assertSyntaxError("select x from a a cross join b on b.x = a.x", 31, "cannot");
    }

    @Test
    public void testCrossJoin2() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a cross join b z) a",
                "select a.x from a a cross join b z",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT));
    }

    @Test
    public void testCrossJoin3() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a join select [x] from c on c.x = a.x cross join b z) a",
                "select a.x from a a " +
                        "cross join b z " +
                        "join c on a.x = c.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testCrossJoinNoAlias() throws Exception {
        assertQuery("select-choose a.x x from (select [x] from a a join select [x] from c on c.x = a.x cross join b) a",
                "select a.x from a a " +
                        "cross join b " +
                        "join c on a.x = c.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT));
    }

    @Test
    public void testCrossJoinToInnerJoin() throws SqlException {
        assertQuery(
                "select-choose tab1.x x, tab1.y y, tab2.x x1, tab2.z z from (select [x, y] from tab1 join select [x, z] from tab2 on tab2.x = tab1.x)",
                "select * from tab1 cross join tab2  where tab1.x = tab2.x",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testCrossJoinWithClause() throws SqlException {
        assertQuery(
                "select-choose c.customerId customerId, c.name name, c.age age, c1.customerId customerId1, c1.name name1, c1.age age1 from (select [customerId, name, age] from (select-choose [customerId, name, age] customerId, name, age from (select [customerId, name, age] from customers where name ~ 'X')) c cross join select [customerId, name, age] from (select-choose [customerId, name, age] customerId, name, age from (select [customerId, name, age] from customers where name ~ 'X' and age = 30)) c1) c limit 10",
                "with" +
                        " cust as (customers where name ~ 'X')" +
                        " cust c cross join cust c1 where c1.age = 30 " +
                        " limit 10",
                modelOf("customers")
                        .col("customerId", ColumnType.INT)
                        .col("name", ColumnType.STRING)
                        .col("age", ColumnType.BYTE)
        );
    }

    @Test
    public void testCursorFromFuncAliasConfusing() throws SqlException {
        assertQuery(
                "select-choose x1 from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() x1] pg_catalog.pg_class() x1 from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class() x from long_sequence(2)"
        );
    }

    @Test
    public void testCursorFromFuncAliasUnique() throws SqlException {
        assertQuery(
                "select-choose z from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() z] pg_catalog.pg_class() z from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class() z from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelect() throws SqlException {
        assertQuery(
                "select-virtual x1, x1 . n column from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() x1] pg_catalog.pg_class() x1 from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class() x, (pg_catalog.pg_class()).n from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectConfusingAliases() throws Exception {
        assertFailure(
                "select (pg_catalog.pg_class()).n, (pg_catalog.pg_description()).z, pg_catalog.pg_class() x, pg_catalog.pg_description() x from long_sequence(2)",
                null,
                92,
                "duplicate alias"
        );
    }

    @Test
    public void testCursorInSelectExprFirst() throws SqlException {
        assertQuery(
                "select-virtual pg_class . n column, pg_class x from (select-choose [pg_class] pg_class, pg_class x from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1))",
                "select (pg_catalog.pg_class()).n, pg_catalog.pg_class() x from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectNotAliased() throws SqlException {
        assertQuery(
                "select-virtual pg_class, pg_class . n column from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class(), (pg_catalog.pg_class()).n from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectOneColumn() throws SqlException {
        assertQuery(
                "select-choose x1 from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() x1] pg_catalog.pg_class() x1 from (pg_catalog.pg_class()) _xQdbA1)",
                "select pg_catalog.pg_class() x from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectOneColumnSansAlias() throws SqlException {
        assertQuery
                ("select-choose pg_class from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1)",
                        "select pg_catalog.pg_class() from long_sequence(2)"
                );
    }

    @Test
    public void testCursorInSelectReverseOrder() throws SqlException {
        assertQuery(
                "select-virtual pg_class . n column, pg_class x from (select-choose [pg_class] pg_class, pg_class x from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1))",
                "select (pg_catalog.pg_class()).n, pg_catalog.pg_class() x from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectReverseOrderRepeatAlias() throws SqlException {
        assertQuery(
                "select-virtual pg_class . n column, pg_class from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1)",
                "select (pg_catalog.pg_class()).n, pg_catalog.pg_class() pg_class from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectSameTwice() throws SqlException {
        assertQuery(
                "select-virtual pg_class . n column, pg_class x, pg_class x from (select-choose [pg_class] pg_class, pg_class x from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1))",
                "select (pg_catalog.pg_class()).n, pg_catalog.pg_class() x, pg_catalog.pg_class() x from long_sequence(2)"
        );
    }

    @Test
    public void testCursorInSelectWithAggregation() throws SqlException {
        assertQuery(
                "select-virtual sum - 20 column, column1 from (select-group-by [sum(pg_catalog.pg_class() . n + 1) sum, column1] sum(pg_catalog.pg_class() . n + 1) sum, column1 from (select-virtual [pg_class . y column1] pg_class . y column1 from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1)))",
                "select sum((pg_catalog.pg_class()).n + 1) - 20, pg_catalog.pg_class().y from long_sequence(2)"
        );
    }

    @Test
    public void testCursorMultiple() throws SqlException {
        assertQuery(
                "select-choose pg_class, pg_description from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() pg_class] pg_catalog.pg_class() pg_class from (pg_catalog.pg_class()) _xQdbA1 cross join select-cursor [pg_catalog.pg_description() pg_description] pg_catalog.pg_description() pg_description from (pg_catalog.pg_description()) _xQdbA2)",
                "select pg_catalog.pg_class(), pg_catalog.pg_description() from long_sequence(2)"
        );
    }

    @Test
    public void testCursorMultipleDuplicateAliases() throws SqlException {
        assertQuery(
                "select-choose cc, cc1 from (long_sequence(2) cross join select-cursor [pg_catalog.pg_class() cc] pg_catalog.pg_class() cc from (pg_catalog.pg_class()) _xQdbA1 cross join select-cursor [pg_catalog.pg_description() cc1] pg_catalog.pg_description() cc1 from (pg_catalog.pg_description()) _xQdbA2)",
                "select pg_catalog.pg_class() cc, pg_catalog.pg_description() cc from long_sequence(2)"
        );
    }

    @Test
    public void testDisallowDotInColumnAlias() throws Exception {
        assertSyntaxError("select x x.y, y from tab order by x", 10, "',', 'from' or 'over' expected");
    }

    @Test
    public void testDisallowDotInColumnAlias2() throws Exception {
        assertSyntaxError("select x ., y from tab order by x", 9, "not allowed");
    }

    @Test
    public void testDisallowedColumnAliases() throws SqlException {
        assertQuery(
                "select-virtual x + z column, x - z column1, x * z column2, x / z column3, x % z column4, x ^ z column5 from (select [z, x] from tab1)",
                "select x+z, x-z, x*z, x/z, x%z, x^z from tab1",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testDodgyCaseExpression() throws Exception {
        assertSyntaxError(
                "select case end + 1, b from tab",
                12,
                "'when' expected",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testDuplicateAlias() throws Exception {
        assertSyntaxError("customers a" +
                        " cross join orders a", 30, "duplicate",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("product", ColumnType.STRING)
        );
    }

    @Test
    public void testDuplicateColumnGroupBy() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h",
                "select b, sum(a), k k1, k from x y sample by 3h",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testDuplicateColumnsBasicSelect() throws SqlException {
        assertQuery(
                "select-choose b, a, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1 from (select [b, a, k] from x timestamp (timestamp)))",
                "select b, a, k k1, k from x",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testDuplicateColumnsVirtualAndGroupBySelect() throws SqlException {
        assertQuery(
                "select-group-by sum(b + a) sum, column, k1, k1 k from (select-virtual [a, b, a + b column, k1] a, b, a + b column, k1, k1 k, timestamp from (select-choose [a, b, k k1] a, b, k k1, timestamp from (select [a, b, k] from x timestamp (timestamp)))) sample by 1m",
                "select sum(b+a), a+b, k k1, k from x sample by 1m",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testDuplicateColumnsVirtualSelect() throws SqlException {
        assertQuery(
                "select-virtual b + a column, k1, k1 k from (select-choose [a, b, k k1] a, b, k k1 from (select [a, b, k] from x timestamp (timestamp)))",
                "select b+a, k k1, k from x",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testDuplicateTables() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, customers.customerName customerName, cust.customerId customerId1, cust.customerName customerName1 from (select [customerId, customerName] from customers cross join select [customerId, customerName] from customers cust)",
                "customers cross join customers cust",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("product", ColumnType.STRING)
        );
    }

    @Test
    public void testEmptyOrderBy() throws Exception {
        assertSyntaxError("select x, y from tab order by", 29, "literal expected");
    }

    @Test
    public void testEmptySampleBy() throws Exception {
        assertSyntaxError("select x, y from tab sample by", 30, "literal expected");
    }

    @Test
    public void testEmptyWhere() throws Exception {
        assertFailure(
                "(select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)) where",
                "create table tab (\n" +
                        "    tag string,\n" +
                        "    seq long\n" +
                        ")  partition by NONE",
                71,
                "empty where clause"
        );
    }

    @Test
    public void testEqualsConstantTransitivityLhs() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, o.customerId customerId1 from (select [customerId] from customers c outer join (select [customerId] from orders o where customerId = 100) o on o.customerId = c.customerId where 100 = customerId) c",
                "customers c" +
                        " outer join orders o on c.customerId = o.customerId" +
                        " where 100 = c.customerId",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testEqualsConstantTransitivityRhs() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, o.customerId customerId1 from (select [customerId] from customers c outer join (select [customerId] from orders o where customerId = 100) o on o.customerId = c.customerId where customerId = 100) c",
                "customers c" +
                        " outer join orders o on c.customerId = o.customerId" +
                        " where c.customerId = 100",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testEraseColumnPrefix() throws SqlException {
        assertQuery(
                "select-choose name from (select [name] from cust where name ~ 'x')",
                "cust where cust.name ~ 'x'",
                modelOf("cust").col("name", ColumnType.STRING)
        );
    }

    @Test
    public void testEraseColumnPrefixInJoin() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, o.customerId customerId1, o.x x from (select [customerId] from customers c outer join select [customerId, x] from (select-choose [customerId, x] customerId, x from (select [customerId, x] from orders o where x = 10 and customerId = 100) o) o on customerId = c.customerId where customerId = 100) c",
                "customers c" +
                        " outer join (orders o where o.x = 10) o on c.customerId = o.customerId" +
                        " where c.customerId = 100",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders")
                        .col("customerId", ColumnType.INT)
                        .col("x", ColumnType.INT)
        );
    }

    @Test
    public void testExpressionSyntaxError() throws Exception {
        assertSyntaxError("select x from a where a + b(c,) > 10", 30, "missing argument");

        // when AST cache is not cleared below query will pickup "garbage" and will misrepresent error
        assertSyntaxError("orders join customers on orders.customerId = c.customerId", 45, "alias",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testExtraComma2OrderByInAnalyticFunction() throws Exception {
        assertSyntaxError("select a,b, f(c) over (partition by b order by ts,) from xyz", 50, "Expression expected");
    }

    @Test
    public void testExtraCommaOrderByInAnalyticFunction() throws Exception {
        assertSyntaxError("select a,b, f(c) over (partition by b order by ,ts) from xyz", 47, "Expression expected");
    }

    @Test
    public void testExtraCommaPartitionByInAnalyticFunction() throws Exception {
        assertSyntaxError("select a,b, f(c) over (partition by b, order by ts) from xyz", 45, "')' expected");
    }

    @Test
    public void testFailureForOrderByOnAliasedColumnNotOnSelect() throws Exception {
        assertFailure(
                "select y from tab order by tab.x",
                "create table tab (\n" +
                        "    x double,\n" +
                        "    y int\n" +
                        ")  partition by NONE",
                27,
                "Invalid column: tab.x"
        );
    }

    @Test
    public void testFailureOrderByGroupByColPrefixed() throws Exception {
        assertFailure(
                "select a, sum(b) b from tab order by tab.b, a",
                "create table tab (\n" +
                        "    a int,\n" +
                        "    b int\n" +
                        ")  partition by NONE",
                37,
                "Invalid column: tab.b"
        );
    }

    @Test
    public void testFailureOrderByGroupByColPrefixed2() throws Exception {
        assertFailure(
                "select a, sum(b) b from tab order by a, tab.b",
                "create table tab (\n" +
                        "    a int,\n" +
                        "    b int\n" +
                        ")  partition by NONE",
                40,
                "Invalid column: tab.b"
        );
    }

    @Test
    public void testFailureOrderByGroupByColPrefixed3() throws Exception {
        assertFailure(
                "select a, sum(b) b from tab order by tab.a, tab.b",
                "create table tab (\n" +
                        "    a int,\n" +
                        "    b int\n" +
                        ")  partition by NONE",
                44,
                "Invalid column: tab.b"
        );
    }

    @Test
    public void testFailureOrderByOnOuterResultWhenOrderByColumnIsNotSelected() throws Exception {
        assertFailure(
                "select x, sum(2*y+x) + sum(3/x) z from tab order by z asc, tab.y desc",
                "create table tab (\n" +
                        "    x double,\n" +
                        "    y int\n" +
                        ")  partition by NONE",
                59,
                "Invalid column: tab.y"
        );
    }

    @Test
    public void testFilter1() throws SqlException {
        assertQuery(
                "select-virtual x, cast(x + 10,timestamp) cast from (select-virtual [x, rnd_double() rnd] x, rnd_double() rnd from (select [x] from long_sequence(100000)) where rnd < 0.9999)",
                "select x, cast(x+10 as timestamp) from (select x, rnd_double() rnd from long_sequence(100000)) where rnd<0.9999"
        );
    }

    @Test
    public void testFilter2() throws Exception {
        assertQuery("select-virtual customerId + 1 column, name, count from (select-group-by [customerId, name, count() count] customerId, name, count() count from (select-choose [customerId, customerName name] customerId, customerName name from (select [customerId, customerName] from customers where customerName = 'X')))",
                "select customerId+1, name, count from (select customerId, customerName name, count() count from customers) where name = 'X'",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testFilterOnGroupBy() throws SqlException {
        assertQuery(
                "select-choose hash, count from (select-group-by [hash, count() count] hash, count() count from (select [hash] from transactions.csv) where count > 1)",
                "select * from (select hash, count() from 'transactions.csv') where count > 1;",
                modelOf("transactions.csv").col("hash", ColumnType.LONG256)
        );
    }

    @Test
    public void testFilterOnSubQuery() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, c.customerName customerName, c.count count, o.orderId orderId, o.customerId customerId1 from (select [customerId, customerName, count] from (select-group-by [customerId, customerName, count() count] customerId, customerName, count() count from (select [customerId, customerName] from customers where customerId > 400 and customerId < 1200) where count > 1) c outer join select [orderId, customerId] from orders o on o.customerId = c.customerId post-join-where o.orderId = NaN) c order by customerId",
                "(select customerId, customerName, count() count from customers) c" +
                        " outer join orders o on c.customerId = o.customerId " +
                        " where o.orderId = NaN and c.customerId > 400 and c.customerId < 1200 and count > 1 order by c.customerId",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testFilterPostJoin() throws SqlException {
        assertQuery(
                "select-choose a.tag tag, a.seq hi, b.seq lo from (select [tag, seq] from tab a asof join select [seq, tag] from tab b on b.tag = a.tag post-join-where b.seq < a.seq) a",
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag) where b.seq < a.seq",
                modelOf("tab").col("tag", ColumnType.STRING).col("seq", ColumnType.LONG)
        );
    }

    @Test
    public void testFilterPostJoinSubQuery() throws SqlException {
        assertQuery(
                "select-choose tag, hi, lo from (select-choose [a.tag tag, a.seq hi, b.seq lo] a.tag tag, a.seq hi, b.seq lo from (select [tag, seq] from tab a asof join select [seq, tag] from tab b on b.tag = a.tag post-join-where a.seq > b.seq + 1) a)",
                "(select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)) where hi > lo + 1",
                modelOf("tab").col("tag", ColumnType.STRING).col("seq", ColumnType.LONG)
        );
    }

    @Test
    public void testForOrderByOnNonSelectedColumn() throws Exception {
        assertQuery(
                "select-choose column from (select-virtual [2 * y + x column, x] 2 * y + x column, x from (select-choose [x, y] x, y from (select [x, y] from tab)) order by x)",
                "select 2*y+x from tab order by x",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testJoinOnOtherCondition() throws SqlException {
        assertQuery(
                "select-choose a.id id, b.id id1, b.c c, b.m m from (select [id] from a outer join (select [id, c, m] from b where c > 0) b on b.id = a.id post-join-where m > 20)",
                "select * from a left join b on ( a.id=b.id and c > 0) where m > 20",
                modelOf("a").col("id", ColumnType.INT),
                modelOf("b").col("id", ColumnType.INT).col("c", ColumnType.INT).col("m", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnEqCondition() throws SqlException {
        assertQuery(
                "select-choose a.id id, b.id id1, b.c c, b.m m from (select [id] from a outer join (select [id, c, m] from b where c = 2) b on b.id = a.id post-join-where m > 20)",
                "select * from a left join b on ( a.id=b.id and c = 2) where m > 20",
                modelOf("a").col("id", ColumnType.INT),
                modelOf("b").col("id", ColumnType.INT).col("c", ColumnType.INT).col("m", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnOrCondition() throws SqlException {
        assertQuery(
                "select-choose a.id id, b.id id1, b.c c, b.m m from (select [id] from a outer join (select [id, c, m] from b where c = 2 or c = 10) b on b.id = a.id post-join-where m > 20)",
                "select * from a left join b on (a.id=b.id and (c = 2 or c = 10)) where m > 20",
                modelOf("a").col("id", ColumnType.INT),
                modelOf("b").col("id", ColumnType.INT).col("c", ColumnType.INT).col("m", ColumnType.INT)
        );
    }

    @Test
    public void testForOrderByOnSelectedColumnThatHasNoAlias() throws Exception {
        assertQuery(
                "select-choose column, column1 from (select-virtual [2 * y + x column, 3 / x column1, x] 2 * y + x column, 3 / x column1, x from (select-choose [x, y] x, y from (select [x, y] from tab)) order by x)",
                "select 2*y+x, 3/x from tab order by x",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testFunctionWithoutAlias() throws SqlException {
        assertQuery("select-virtual f(x) f, x from (select [x] from x where x > 1)",
                "select f(x), x from x where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testGenericPreFilterPlacement() throws Exception {
        assertQuery(
                "select-choose customerName, orderId, productId from (select [customerName, customerId] from customers join (select [orderId, productId, customerId, product] from orders where product = 'X') orders on orders.customerId = customers.customerId where customerName ~ 'WTBHZVPVZZ')",
                "select customerName, orderId, productId " +
                        "from customers join orders on customers.customerId = orders.customerId where customerName ~ 'WTBHZVPVZZ' and product = 'X'",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("product", ColumnType.STRING).col("orderId", ColumnType.INT).col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testGroupByTimeInSubQuery() throws SqlException {
        assertQuery(
                "select-group-by min(s) min, max(s) max, avg(s) avg from (select-group-by [sum(value) s, ts] ts, sum(value) s from (select [value, ts] from erdem_x timestamp (ts)))",
                "select min(s), max(s), avg(s) from (select ts, sum(value) s from erdem_x)",
                modelOf("erdem_x").timestamp("ts").col("value", ColumnType.LONG)
        );
    }

    @Test
    public void testGroupByWithLimit() throws SqlException {
        assertQuery(
                "select-group-by fromAddress, toAddress, count() count from (select [fromAddress, toAddress] from transactions.csv) limit 10000",
                "select fromAddress, toAddress, count() from 'transactions.csv' limit 10000",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.STRING)
                        .col("toAddress", ColumnType.STRING)
        );
    }

    @Test
    public void testGroupByWithSubQueryLimit() throws SqlException {
        assertQuery(
                "select-group-by fromAddress, toAddress, count() count from (select-choose [fromAddress, toAddress] fromAddress, toAddress from (select [fromAddress, toAddress] from transactions.csv) limit 10000)",
                "select fromAddress, toAddress, count() from ('transactions.csv' limit 10000)",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.STRING)
                        .col("toAddress", ColumnType.STRING)
        );
    }

    @Test
    public void testInnerJoin() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a join select [x] from b on b.x = a.x) a",
                "select a.x from a a inner join b on b.x = a.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testInnerJoin2() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, customers.customerName customerName, orders.customerId customerId1 from (select [customerId, customerName] from customers join select [customerId] from orders on orders.customerId = customers.customerId where customerName ~ 'WTBHZVPVZZ')",
                "customers join orders on customers.customerId = orders.customerId where customerName ~ 'WTBHZVPVZZ'",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testInnerJoinColumnAliasNull() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c join (select [customerId] from orders o where customerId = null) o on o.customerId = c.customerId) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " join orders o on c.customerId = o.customerId) " +
                        " where kk = null limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testInnerJoinEqualsConstant() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, orders.customerId customerId1, orders.productName productName from (select [customerId] from customers join (select [customerId, productName] from orders where productName = 'WTBHZVPVZZ') orders on orders.customerId = customers.customerId)",
                "customers join orders on customers.customerId = orders.customerId where productName = 'WTBHZVPVZZ'",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING));
    }

    @Test
    public void testInnerJoinEqualsConstantLhs() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, orders.customerId customerId1, orders.productName productName from (select [customerId] from customers join (select [customerId, productName] from orders where 'WTBHZVPVZZ' = productName) orders on orders.customerId = customers.customerId)",
                "customers join orders on customers.customerId = orders.customerId where 'WTBHZVPVZZ' = productName",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING));
    }

    @Test
    public void testInnerJoinPostFilter() throws SqlException {
        assertQuery("select-virtual c, a, b, d, d - b column from (select-choose [z.c c, x.a a, b, d] z.c c, x.a a, b, d from (select [a, c] from x join (select [b, m] from y where b < 20) y on y.m = x.c join select [c, d] from z on z.c = x.c))",
                "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20",
                modelOf("x")
                        .col("c", ColumnType.INT)
                        .col("a", ColumnType.INT),
                modelOf("y")
                        .col("m", ColumnType.INT)
                        .col("b", ColumnType.INT),
                modelOf("z")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
        );
    }

    @Test
    public void testInnerJoinSubQuery() throws Exception {
        assertQuery("select-choose customerName, productName, orderId from (select [customerName, productName, orderId, productId] from (select-choose [customerName, productName, orderId, productId] customerName, orderId, productId, productName from (select [customerName, customerId] from customers join (select [productName, orderId, productId, customerId] from orders where productName ~ 'WTBHZVPVZZ') orders on orders.customerId = customers.customerId)) x join select [productId] from products p on p.productId = x.productId) x",
                "select customerName, productName, orderId from (" +
                        "select \"customerName\", orderId, productId, productName " +
                        "from \"customers\" join orders on \"customers\".\"customerId\" = orders.customerId where productName ~ 'WTBHZVPVZZ'" +
                        ") x" +
                        " join products p on p.productId = x.productId",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT)
        );

        assertQuery("select-choose customerName, productName, orderId from (select [customerName, customerId] from customers join (select [productName, orderId, customerId, productId] from orders o where productName ~ 'WTBHZVPVZZ') o on o.customerId = customers.customerId join select [productId] from products p on p.productId = o.productId)",
                "select customerName, productName, orderId " +
                        " from customers join orders o on customers.customerId = o.customerId " +
                        " join products p on p.productId = o.productId" +
                        " where productName ~ 'WTBHZVPVZZ'",
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testInsertAsSelect() throws SqlException {
        assertModel(
                "insert into x select-choose c, d from (select [c, d] from y)",
                "insert into x select * from y",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectBadBatchSize() throws Exception {
        assertSyntaxError(
                "insert batch 2a lag 100000 into x select * from y",
                13, "bad long integer",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectBatchSize() throws SqlException {
        assertModel(
                "insert batch 15000 into x select-choose c, d from (select [c, d] from y)",
                "insert batch 15000 into x select * from y",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectBatchSizeAndLag() throws SqlException {
        assertModel(
                "insert batch 10000 lag 100000 into x select-choose c, d from (select [c, d] from y)",
                "insert batch 10000 commitLag 100ms into x select * from y",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectColumnCountMismatch() throws Exception {
        assertSyntaxError("insert into x (b) select * from y",
                12, "column count mismatch",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING));
    }

    @Test
    public void testInsertAsSelectColumnList() throws SqlException {
        assertModel(
                "insert into x (a, b) select-choose c, d from (select [c, d] from y)",
                "insert into x (a,b) select * from y",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectDuplicateColumns() throws Exception {
        assertSyntaxError("insert into x (b,b) select * from y",
                17, "duplicate column name",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING));
    }

    @Test
    public void testInsertAsSelectNegativeBatchSize() throws Exception {
        assertSyntaxError(
                "insert batch -25 lag 100000 into x select * from y",
                14, "must be positive",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertAsSelectNegativeLag() throws Exception {
        assertSyntaxError(
                "insert batch 2 commitLag -4s into x select * from y",
                26, "invalid interval qualifier -",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING),
                modelOf("y")
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.STRING)
        );
    }

    @Test
    public void testInsertColumnValueMismatch() throws Exception {
        assertSyntaxError("insert into x (a,b) values (?)",
                15,
                "value count does not match column count",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING));
    }

    @Test
    public void testInsertColumnsAndValues() throws SqlException {
        assertModel("insert into x (a, b) values (3, ?)",
                "insert into x (a,b) values (3, ?)",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING));
    }

    @Test
    public void testInsertColumnsAndValuesQuoted() throws SqlException {
        assertModel("insert into x (a, b) values (3, ?)",
                "insert into \"x\" (\"a\",\"b\") values (3, ?)",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING));
    }

    @Test
    public void testInsertMissingClosingBracket() throws Exception {
        assertSyntaxError("insert into x values (?,?",
                25,
                "',' expected",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING));
    }

    @Test
    public void testInsertMissingValue() throws Exception {
        assertSyntaxError("insert into x values ()",
                22,
                "Expression expected",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING));
    }

    @Test
    public void testInsertMissingValueAfterComma() throws Exception {
        assertSyntaxError("insert into x values (?,",
                24,
                "Expression expected",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING));
    }

    @Test
    public void testInsertMissingValues() throws Exception {
        assertSyntaxError("insert into x values",
                20,
                "'(' expected",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING));
    }

    @Test
    public void testInsertValues() throws SqlException {
        assertModel("insert into x values (3, 'abc', ?)",
                "insert into x values (3, 'abc', ?)",
                ExecutionModel.INSERT,
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.STRING)
                        .col("c", ColumnType.STRING));
    }

    @Test
    public void testInvalidAlias() throws Exception {
        assertSyntaxError("orders join customers on orders.customerId = c.customerId", 45, "alias",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidColumn() throws Exception {
        assertSyntaxError("orders join customers on customerIdx = customerId", 25, "Invalid column",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT).col("productName", ColumnType.STRING).col("productId", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidColumnInExpression() throws Exception {
        assertSyntaxError(
                "select a + b x from tab",
                11,
                "Invalid column",
                modelOf("tab").col("a", ColumnType.INT));
    }

    @Test
    public void testInvalidGroupBy1() throws Exception {
        assertSyntaxError("select x, y from tab sample by x,", 32, "unexpected");
    }

    @Test
    public void testInvalidGroupBy2() throws Exception {
        assertSyntaxError("select x, y from (tab sample by x,)", 33, "')' expected");
    }

    @Test
    public void testInvalidGroupBy3() throws Exception {
        assertSyntaxError("select x, y from tab sample by x, order by y", 32, "unexpected token: ,");
    }

    @Test
    public void testInvalidInnerJoin1() throws Exception {
        assertSyntaxError("select x from a a inner join b z", 31, "'on'");
    }

    @Test
    public void testInvalidInnerJoin2() throws Exception {
        assertSyntaxError("select x from a a inner join b z on", 33, "Expression");
    }

    @Test
    public void testInvalidOrderBy1() throws Exception {
        assertSyntaxError("select x, y from tab order by x,", 32, "literal expected");
    }

    @Test
    public void testInvalidOrderBy2() throws Exception {
        assertSyntaxError("select x, y from (tab order by x,)", 33, "literal expected");
    }

    @Test
    public void testInvalidOuterJoin1() throws Exception {
        assertSyntaxError("select x from a a outer join b z", 31, "'on'");
    }

    @Test
    public void testInvalidOuterJoin2() throws Exception {
        assertSyntaxError("select x from a a outer join b z on", 33, "Expression");
    }

    @Test
    public void testInvalidSelectColumn() throws Exception {
        assertSyntaxError("select c.customerId, orderIdx, o.productId from " +
                        "customers c " +
                        "join (" +
                        "orders latest by customerId where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`)" +
                        ") o on c.customerId = o.customerId", 21, "Invalid column",
                modelOf("customers").col("customerName", ColumnType.STRING).col("customerId", ColumnType.INT),
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );

        assertSyntaxError("select c.customerId, orderId, o.productId2 from " +
                        "customers c " +
                        "join (" +
                        "orders latest by customerId where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`)" +
                        ") o on c.customerId = o.customerId", 30, "Invalid column",
                modelOf("customers").col("customerName", ColumnType.STRING).col("customerId", ColumnType.INT),
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );

        assertSyntaxError("select c.customerId, orderId, o2.productId from " +
                        "customers c " +
                        "join (" +
                        "orders latest by customerId where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`)" +
                        ") o on c.customerId = o.customerId", 30, "Invalid table name",
                modelOf("customers").col("customerName", ColumnType.STRING).col("customerId", ColumnType.INT),
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testInvalidSubQuery() throws Exception {
        assertSyntaxError("select x,y from (tab where x = 100) latest by x", 36, "latest");
    }

    @Test
    public void testInvalidTableName() throws Exception {
        assertSyntaxError("orders join customer on customerId = customerId", 12, "does not exist",
                modelOf("orders").col("customerId", ColumnType.INT));
    }

    @Test
    public void testJoin1() throws Exception {
        assertQuery(
                "select-choose t1.x x, y from (select [x] from (select-choose [x] x from (select [x] from tab t2 latest by x where x > 100) t2) t1 join select [x] from tab2 xx2 on xx2.x = t1.x join select [y, x] from (select-choose [y, x] x, y from (select [y, x, z, b, a] from tab4 latest by z where a > b) where y > 0) x4 on x4.x = t1.x cross join select [b] from tab3 post-join-where xx2.x > tab3.b) t1",
                "select t1.x, y from (select x from tab t2 LATEST BY x where x > 100) t1 " +
                        "join tab2 xx2 on xx2.x = t1.x " +
                        "join tab3 on xx2.x > tab3.b " +
                        "join (select x,y from tab4 latest by z where a > b) x4 on x4.x = t1.x " +
                        "where y > 0",
                modelOf("tab").col("x", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT),
                modelOf("tab3").col("b", ColumnType.INT),
                modelOf("tab4").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.INT).col("a", ColumnType.INT).col("b", ColumnType.INT));
    }

    @Test
    public void testJoin3() throws Exception {
        assertQuery(
                "select-choose x from (select-choose [tab2.x x] tab2.x x from (select [x] from tab join select [x] from tab2 on tab2.x = tab.x cross join select [x] from tab3 post-join-where f(tab3.x,tab2.x) = tab.x))",
                "select x from (select tab2.x from tab join tab2 on tab.x=tab2.x join tab3 on f(tab3.x,tab2.x) = tab.x)",
                modelOf("tab").col("x", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT),
                modelOf("tab3").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinClauseAlignmentBug() throws SqlException {
        assertQuery(
                "select-virtual NULL TABLE_CAT, TABLE_SCHEM, TABLE_NAME, switch(TABLE_SCHEM ~ '^pg_' or TABLE_SCHEM = 'information_schema',true,case(TABLE_SCHEM = 'pg_catalog' or TABLE_SCHEM = 'information_schema',switch(relkind,'r','SYSTEM TABLE','v','SYSTEM VIEW','i','SYSTEM INDEX',NULL),TABLE_SCHEM = 'pg_toast',switch(relkind,'r','SYSTEM TOAST TABLE','i','SYSTEM TOAST INDEX',NULL),switch(relkind,'r','TEMPORARY TABLE','p','TEMPORARY TABLE','i','TEMPORARY INDEX','S','TEMPORARY SEQUENCE','v','TEMPORARY VIEW',NULL)),false,switch(relkind,'r','TABLE','p','PARTITIONED TABLE','i','INDEX','S','SEQUENCE','v','VIEW','c','TYPE','f','FOREIGN TABLE','m','MATERIALIZED VIEW',NULL),NULL) TABLE_TYPE, REMARKS, '' TYPE_CAT, '' TYPE_SCHEM, '' TYPE_NAME, '' SELF_REFERENCING_COL_NAME, '' REF_GENERATION from (select-choose [n.nspname TABLE_SCHEM, c.relname TABLE_NAME, c.relkind relkind, d.description REMARKS] n.nspname TABLE_SCHEM, c.relname TABLE_NAME, c.relkind relkind, d.description REMARKS from (select [nspname, oid] from pg_catalog.pg_namespace() n join (select [relname, relkind, relnamespace, oid] from pg_catalog.pg_class() c where relname like 'quickstart-events2') c on c.relnamespace = n.oid post-join-where false or c.relkind = 'r' and n.nspname !~ '^pg_' and n.nspname != 'information_schema' outer join (select [description, objoid, objsubid, classoid] from pg_catalog.pg_description() d where objsubid = 0) d on d.objoid = c.oid outer join (select [oid, relname, relnamespace] from pg_catalog.pg_class() dc where relname = 'pg_class') dc on dc.oid = d.classoid outer join (select [oid, nspname] from pg_catalog.pg_namespace() dn where nspname = 'pg_catalog') dn on dn.oid = dc.relnamespace) n) n order by TABLE_TYPE, TABLE_SCHEM, TABLE_NAME",
                "SELECT \n" +
                        "     NULL AS TABLE_CAT, \n" +
                        "     n.nspname AS TABLE_SCHEM, \n" +
                        "     \n" +
                        "     c.relname AS TABLE_NAME,  \n" +
                        "     CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  \n" +
                        "        WHEN true THEN \n" +
                        "           CASE  \n" +
                        "                WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN \n" +
                        "                    CASE c.relkind   \n" +
                        "                        WHEN 'r' THEN 'SYSTEM TABLE' \n" +
                        "                        WHEN 'v' THEN 'SYSTEM VIEW'\n" +
                        "                        WHEN 'i' THEN 'SYSTEM INDEX'\n" +
                        "                        ELSE NULL   \n" +
                        "                    END\n" +
                        "                WHEN n.nspname = 'pg_toast' THEN \n" +
                        "                    CASE c.relkind   \n" +
                        "                        WHEN 'r' THEN 'SYSTEM TOAST TABLE'\n" +
                        "                        WHEN 'i' THEN 'SYSTEM TOAST INDEX'\n" +
                        "                        ELSE NULL   \n" +
                        "                    END\n" +
                        "                ELSE \n" +
                        "                    CASE c.relkind\n" +
                        "                        WHEN 'r' THEN 'TEMPORARY TABLE'\n" +
                        "                        WHEN 'p' THEN 'TEMPORARY TABLE'\n" +
                        "                        WHEN 'i' THEN 'TEMPORARY INDEX'\n" +
                        "                        WHEN 'S' THEN 'TEMPORARY SEQUENCE'\n" +
                        "                        WHEN 'v' THEN 'TEMPORARY VIEW'\n" +
                        "                        ELSE NULL   \n" +
                        "                    END  \n" +
                        "            END  \n" +
                        "        WHEN false THEN \n" +
                        "            CASE c.relkind  \n" +
                        "                WHEN 'r' THEN 'TABLE'  \n" +
                        "                WHEN 'p' THEN 'PARTITIONED TABLE'  \n" +
                        "                WHEN 'i' THEN 'INDEX'  \n" +
                        "                WHEN 'S' THEN 'SEQUENCE'  \n" +
                        "                WHEN 'v' THEN 'VIEW'  \n" +
                        "                WHEN 'c' THEN 'TYPE'  \n" +
                        "                WHEN 'f' THEN 'FOREIGN TABLE'  \n" +
                        "                WHEN 'm' THEN 'MATERIALIZED VIEW'  \n" +
                        "                ELSE NULL  \n" +
                        "            END  \n" +
                        "        ELSE NULL  \n" +
                        "    END AS TABLE_TYPE, \n" +
                        "    d.description AS REMARKS,\n" +
                        "    '' as TYPE_CAT,\n" +
                        "    '' as TYPE_SCHEM,\n" +
                        "    '' as TYPE_NAME,\n" +
                        "    '' AS SELF_REFERENCING_COL_NAME,\n" +
                        "    '' AS REF_GENERATION\n" +
                        "FROM \n" +
                        "    pg_catalog.pg_namespace n, \n" +
                        "    pg_catalog.pg_class c  \n" +
                        "    LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0) \n" +
                        "    LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')\n" +
                        "    LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')\n" +
                        "WHERE \n" +
                        "    c.relnamespace = n.oid  \n" +
                        "    AND c.relname LIKE 'quickstart-events2' \n" +
                        "    AND (\n" +
                        "        false  \n" +
                        "        OR  ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ) \n" +
                        "        ) \n" +
                        "ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME"
        );
    }

    @Test
    public void testJoinColumnPropagation() throws SqlException {
        assertQuery(
                "select-group-by city, max(temp) max from (select [temp, sensorId] from readings timestamp (ts) join select [city, sensId] from (select-choose [city, ID sensId] ID sensId, city from (select [city, ID] from sensors)) _xQdbA1 on sensId = readings.sensorId)",
                "SELECT city, max(temp)\n" +
                        "FROM readings\n" +
                        "JOIN(\n" +
                        "    SELECT ID sensId, city\n" +
                        "    FROM sensors)\n" +
                        "ON readings.sensorId = sensId",
                modelOf("sensors")
                        .col("ID", ColumnType.LONG)
                        .col("make", ColumnType.STRING)
                        .col("city", ColumnType.STRING),
                modelOf("readings")
                        .col("ID", ColumnType.LONG)
                        .timestamp("ts")
                        .col("temp", ColumnType.DOUBLE)
                        .col("sensorId", ColumnType.LONG)
        );
    }

    @Test
    public void testJoinColumnResolutionOnSubQuery() throws SqlException {
        assertQuery(
                "select-group-by sum(timestamp) sum from (select [timestamp] from (select-choose [timestamp] ccy, timestamp from (select [timestamp] from y)) _xQdbA1 cross join (select-choose ccy from (select [ccy] from x)) _xQdbA2)",
                "select sum(timestamp) from (y) cross join (x)",
                modelOf("x").col("ccy", ColumnType.SYMBOL),
                modelOf("y").col("ccy", ColumnType.SYMBOL).col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testJoinColumnResolutionOnSubQuery2() throws SqlException {
        assertQuery(
                "select-group-by sum(timestamp) sum from (select [timestamp, ccy, sym] from (select-choose [timestamp, ccy, sym] ccy, timestamp, sym from (select [timestamp, ccy, sym] from y)) _xQdbA1 join select [ccy, sym] from (select-choose [ccy, sym] ccy, sym from (select [ccy, sym] from x)) _xQdbA2 on _xQdbA2.ccy = _xQdbA1.ccy and _xQdbA2.sym = _xQdbA1.sym)",
                "select sum(timestamp) from (y) join (x) on (ccy, sym)",
                modelOf("x").col("ccy", ColumnType.SYMBOL).col("sym", ColumnType.INT),
                modelOf("y").col("ccy", ColumnType.SYMBOL).col("timestamp", ColumnType.TIMESTAMP).col("sym", ColumnType.INT)
        );
    }

    @Test
    public void testJoinColumnResolutionOnSubQuery3() throws SqlException {
        assertQuery(
                "select-group-by sum(timestamp) sum from (select [timestamp] from (select-choose [timestamp] ccy, timestamp from (select [timestamp] from y)) _xQdbA1 cross join x)",
                "select sum(timestamp) from (y) cross join x",
                modelOf("x").col("ccy", ColumnType.SYMBOL),
                modelOf("y").col("ccy", ColumnType.SYMBOL).col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testJoinCycle() throws Exception {
        assertQuery(
                "select-choose" +
                        " orders.customerId customerId," +
                        " orders.orderId orderId," +
                        " customers.customerId customerId1," +
                        " d.orderId orderId1," +
                        " d.productId productId," +
                        " suppliers.supplier supplier," +
                        " products.productId productId1," +
                        " products.supplier supplier1 " +
                        "from (" +
                        "select [customerId, orderId] from orders" +
                        " join select [customerId] from customers on customers.customerId = orders.customerId" +
                        " join (select [orderId, productId] from orderDetails d where orderId = productId) d on d.productId = orders.orderId" +
                        " join select [supplier] from suppliers on suppliers.supplier = orders.orderId" +
                        " join select [productId, supplier] from products on products.productId = orders.orderId and products.supplier = suppliers.supplier" +
                        ")",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and orders.orderId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId and orders.orderId = products.productId" +
                        " where orders.orderId = suppliers.supplier",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.SYMBOL),
                modelOf("suppliers").col("supplier", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testJoinCycle2() throws Exception {
        assertQuery(
                "select-choose orders.customerId customerId, orders.orderId orderId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, suppliers.x x, products.productId productId1, products.supplier supplier1 from (select [customerId, orderId] from orders join select [customerId] from customers on customers.customerId = orders.customerId join select [orderId, productId] from orderDetails d on d.productId = orders.orderId join select [supplier, x] from suppliers on suppliers.x = d.orderId and suppliers.supplier = orders.orderId join select [productId, supplier] from products on products.productId = orders.orderId and products.supplier = suppliers.supplier)",
                "orders" +
                        " join customers on orders.orderId = products.productId" +
                        " join orderDetails d on products.supplier = suppliers.supplier" +
                        " join suppliers on orders.customerId = customers.customerId" +
                        " join products on d.productId = products.productId and orders.orderId = products.productId" +
                        " where orders.orderId = suppliers.supplier and d.orderId = suppliers.x",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.SYMBOL),
                modelOf("suppliers").col("supplier", ColumnType.SYMBOL).col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinDuplicateTables() throws Exception {
        assertSyntaxError(
                "select * from tab cross join tab",
                29,
                "duplicate",
                modelOf("tab").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testJoinFunction() throws SqlException {
        assertQuery(
                "select-choose tab.x x, t.y y, t1.z z from (select [x] from tab join select [y] from t on f(y) = f(x) join select [z] from t1 on z = f(x) const-where 1 = 1)",
                "select * from tab join t on f(x)=f(y) join t1 on 1=1 where z=f(x)",
                modelOf("tab").col("x", ColumnType.INT),
                modelOf("t").col("y", ColumnType.INT),
                modelOf("t1").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testJoinGroupBy() throws Exception {
        assertQuery("select-group-by country, sum(quantity) sum from (select [customerId, orderId] from orders o join (select [country, customerId] from customers c where country ~ '^Z') c on c.customerId = o.customerId join select [quantity, orderId] from orderDetails d on d.orderId = o.orderId) o",
                "select country, sum(quantity) from orders o " +
                        "join customers c on c.customerId = o.customerId " +
                        "join orderDetails d on o.orderId = d.orderId" +
                        " where country ~ '^Z'",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT).col("country", ColumnType.SYMBOL),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("quantity", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testJoinGroupByFilter() throws Exception {
        assertQuery(
                "select-choose country, sum from (select-group-by [country, sum(quantity) sum] country, sum(quantity) sum from (select [quantity, customerId, orderId] from orders o join (select [country, customerId] from customers c where country ~ '^Z') c on c.customerId = o.customerId join select [orderId] from orderDetails d on d.orderId = o.orderId) o where sum > 2)",
                "(select country, sum(quantity) sum from orders o " +
                        "join customers c on c.customerId = o.customerId " +
                        "join orderDetails d on o.orderId = d.orderId" +
                        " where country ~ '^Z') where sum > 2",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT).col("quantity", ColumnType.DOUBLE),
                modelOf("customers").col("customerId", ColumnType.INT).col("country", ColumnType.SYMBOL),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("comment", ColumnType.STRING)
        );
    }

    @Test
    public void testJoinImpliedCrosses() throws Exception {
        assertQuery(
                "select-choose orders.customerId customerId, orders.orderId orderId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [customerId, orderId] from orders cross join select [productId, supplier] from products join select [supplier] from suppliers on suppliers.supplier = products.supplier cross join select [customerId] from customers cross join select [orderId, productId] from orderDetails d const-where 1 = 1 and 2 = 2 and 3 = 3)",
                "orders" +
                        " join customers on 1=1" +
                        " join orderDetails d on 2=2" +
                        " join products on 3=3" +
                        " join suppliers on products.supplier = suppliers.supplier",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.SYMBOL),
                modelOf("suppliers").col("supplier", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testJoinMultipleFields() throws Exception {
        assertQuery(
                "select-choose orders.customerId customerId, orders.orderId orderId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [customerId, orderId] from orders join select [customerId] from customers on customers.customerId = orders.customerId join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = customers.customerId and d.orderId = orders.orderId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier)",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("customerId", ColumnType.INT).col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.SYMBOL),
                modelOf("suppliers").col("supplier", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testJoinOfJoin() throws SqlException {
        assertQuery(
                "select-choose tt.x x, tt.y y, tt.x1 x1, tt.z z, tab2.z z1, tab2.k k from (select [x, y, x1, z] from (select-choose [tab.x x, tab.y y, tab1.x x1, tab1.z z] tab.x x, tab.y y, tab1.x x1, tab1.z z from (select [x, y] from tab join select [x, z] from tab1 on tab1.x = tab.x)) tt join select [z, k] from tab2 on tab2.z = tt.z) tt",
                "select * from (select * from tab join tab1 on (x)) tt join tab2 on(z)",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT),
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("z", ColumnType.INT)
                        .col("k", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnCase() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a cross join b where switch(x,1,10,15)) a",
                "select a.x from a a join b on (CASE WHEN a.x = 1 THEN 10 ELSE 15 END)",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT));
    }

    @Test
    public void testJoinOnCaseDanglingThen() throws Exception {
        assertSyntaxError(
                "select a.x from a a join b on (CASE WHEN a.x THEN 10 10+4 ELSE 15 END)",
                53,
                "dangling expression",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT));
    }

    @Test
    public void testJoinOnColumns() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y from (select [x, z] from tab1 a join select [y, z] from tab2 b on b.z = a.z) a",
                "select a.x, b.y from tab1 a join tab2 b on (z)",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnExpression() throws Exception {
        assertSyntaxError(
                "a join b on (x,x+1)",
                18,
                "Column name expected",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOnExpression2() throws SqlException {
        assertQuery("select-choose a.x x, b.x x1 from (select [x] from a cross join (select [x] from b where x) b where x + 1)",
                "a join b on a.x+1 and b.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOneFieldToTwoAcross2() throws Exception {
        assertQuery(
                "select-choose orders.orderId orderId, orders.customerId customerId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [orderId, customerId] from orders join select [customerId] from customers on customers.customerId = orders.orderId join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.orderId = orders.orderId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier where customerId = orderId)",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = customers.customerId and orders.orderId = d.orderId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOneFieldToTwoReorder() throws Exception {
        assertQuery(
                "select-choose orders.orderId orderId, orders.customerId customerId, d.orderId orderId1, d.productId productId, customers.customerId customerId1, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [orderId, customerId] from orders join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.orderId = orders.customerId join select [customerId] from customers on customers.customerId = orders.customerId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier where orderId = customerId)",
                "orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.orderId = customers.customerId" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinOrder4() throws SqlException {
        assertQuery(
                "select-choose b.id id, e.id id1 from (a cross join select [id] from b asof join d join select [id] from e on e.id = b.id cross join c)",
                "a" +
                        " cross join b cross join c" +
                        " asof join d inner join e on b.id = e.id",
                modelOf("a"),
                modelOf("b").col("id", ColumnType.INT),
                modelOf("c"),
                modelOf("d"),
                modelOf("e").col("id", ColumnType.INT)
        );
    }

    @Test
    public void testJoinReorder() throws Exception {
        assertQuery(
                "select-choose" +
                        " orders.orderId orderId," +
                        " customers.customerId customerId," +
                        " d.orderId orderId1," +
                        " d.productId productId," +
                        " products.productId productId1," +
                        " products.supplier supplier," +
                        " suppliers.supplier supplier1 " +
                        "from (" +
                        "select [orderId] from orders" +
                        " join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.orderId = orders.orderId" +
                        " join select [customerId] from customers on customers.customerId = d.productId" +
                        " join select [productId, supplier] from products on products.productId = d.productId" +
                        " join select [supplier] from suppliers on suppliers.supplier = products.supplier" +
                        " const-where 1 = 1)",
                "orders" +
                        " join customers on 1=1" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinReorder3() throws Exception {
        assertQuery(
                "select-choose orders.orderId orderId, customers.customerId customerId, shippers.shipper shipper, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, products.productId productId1, products.supplier supplier1 from (select [orderId] from orders join select [shipper] from shippers on shippers.shipper = orders.orderId join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = shippers.shipper join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier cross join select [customerId] from customers const-where 1 = 1)",
                "orders" +
                        " outer join customers on 1=1" +
                        " join shippers on shippers.shipper = orders.orderId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = shippers.shipper" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT),
                modelOf("shippers").col("shipper", ColumnType.INT)
        );
    }

    @Test
    public void testJoinReorderRoot() throws Exception {
        assertQuery(
                "select-choose customers.customerId customerId, orders.orderId orderId, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [customerId] from customers join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = customers.customerId join select [orderId] from orders on orders.orderId = d.orderId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier)",
                "customers" +
                        " cross join orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",

                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinReorderRoot2() throws Exception {
        assertQuery(
                "select-choose orders.orderId orderId, customers.customerId customerId, shippers.shipper shipper, d.orderId orderId1, d.productId productId, products.productId productId1, products.supplier supplier, suppliers.supplier supplier1 from (select [orderId] from orders join select [shipper] from shippers on shippers.shipper = orders.orderId join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = shippers.shipper join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier cross join select [customerId] from customers const-where 1 = 1)",
                "orders" +
                        " outer join customers on 1=1" +
                        " join shippers on shippers.shipper = orders.orderId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = shippers.shipper" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT),
                modelOf("shippers").col("shipper", ColumnType.INT)
        );
    }

    @Test
    public void testJoinSubQuery() throws Exception {
        assertQuery(
                "select-choose" +
                        " orders.orderId orderId," +
                        " _xQdbA1.customerId customerId," +
                        " _xQdbA1.customerName customerName " +
                        "from (" +
                        "select [orderId] from orders" +
                        " join select [customerId, customerName] from (select-choose [customerId, customerName] customerId, customerName from (select [customerId, customerName] from customers where customerName ~ 'X')) _xQdbA1 on customerName = orderId)",
                "orders" +
                        " cross join (select customerId, customerName from customers where customerName ~ 'X')" +
                        " where orderId = customerName",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT).col("customerName", ColumnType.STRING)

        );
    }

    @Test
    public void testJoinSubQueryConstantWhere() throws Exception {
        assertQuery(
                "select-choose o.customerId customerId from (select [cid] from (select-choose [customerId cid] customerId cid from (select [customerId] from customers where 100 = customerId)) c outer join (select [customerId] from orders o where customerId = 100) o on o.customerId = c.cid const-where 10 = 9) c",
                "select o.customerId from (select customerId cid from customers) c" +
                        " outer join orders o on c.cid = o.customerId" +
                        " where 100 = c.cid and 10=9",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testJoinSubQueryWherePosition() throws Exception {
        assertQuery(
                "select-choose o.customerId customerId from (select [cid] from (select-choose [customerId cid] customerId cid from (select [customerId] from customers where 100 = customerId)) c outer join (select [customerId] from orders o where customerId = 100) o on o.customerId = c.cid) c",
                "select o.customerId from (select customerId cid from customers) c" +
                        " outer join orders o on c.cid = o.customerId" +
                        " where 100 = c.cid",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testJoinSyntaxError() throws Exception {
        assertSyntaxError(
                "select a.x from a a join b on (a + case when a.x = 1 then 10 else end)",
                66,
                "missing argument",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT));
    }

    @Test
    public void testJoinTableMissing() throws Exception {
        assertSyntaxError(
                "select a from tab join",
                22,
                "table name or sub-query expected"
        );
    }

    @Test
    public void testJoinTimestampPropagation() throws SqlException {
        assertQuery(
                "select-choose ts_stop, id, ts_start, id1 from (select-choose [a.created ts_stop, a.id id, b.created ts_start, b.id id1] a.created ts_stop, a.id id, b.created ts_start, b.id id1 from (select [created, id] from (select-choose [created, id] id, created, event, timestamp from (select-choose [created, id] id, created, event, timestamp from (select [created, id, event] from telemetry_users timestamp (timestamp) where event = 101 and id != '0x05ab1e873d165b00000005743f2c17') order by created) timestamp (created)) a lt join select [created, id] from (select-choose [created, id] id, created, event, timestamp from (select-choose [created, id] id, created, event, timestamp from (select [created, id, event] from telemetry_users timestamp (timestamp) where event = 100) order by created) timestamp (created)) b on b.id = a.id post-join-where a.created - b.created > 10000000000) a)",
                "with \n" +
                        "    starts as ((telemetry_users where event = 100 order by created) timestamp(created)),\n" +
                        "    stops as ((telemetry_users where event = 101 order by created) timestamp(created))\n" +
                        "\n" +
                        "(select a.created ts_stop, a.id, b.created ts_start, b.id from stops a lt join starts b on (id)) where id <> '0x05ab1e873d165b00000005743f2c17' and ts_stop - ts_start > 10000000000\n",
                modelOf("telemetry_users")
                        .col("id", ColumnType.LONG256)
                        .col("created", ColumnType.TIMESTAMP)
                        .col("event", ColumnType.SHORT)
                        .timestamp()
        );
    }

    @Test
    public void testJoinTimestampPropagationWhenTimestampNotSelected() throws SqlException {
        assertQuery(
                "select-distinct id from (select-choose [id] id from (select-choose [a.id id] a.created ts_stop, a.id id, b.created ts_start, b.id id1 from (select [id, created] from (select-choose [id, created] id, created, event, timestamp from (select-choose [created, id] id, created, event, timestamp from (select [created, id, event] from telemetry_users timestamp (timestamp) where event = 101 and id != '0x05ab1e873d165b00000005743f2c17') order by created) timestamp (created)) a lt join select [id, created] from (select-choose [id] id, created, event, timestamp from (select-choose [created, id] id, created, event, timestamp from (select [created, id, event] from telemetry_users timestamp (timestamp) where event = 100) order by created) timestamp (created)) b on b.id = a.id post-join-where a.created - b.created > 10000000000) a))",
                "with \n" +
                        "    starts as ((telemetry_users where event = 100 order by created) timestamp(created)),\n" +
                        "    stops as ((telemetry_users where event = 101 order by created) timestamp(created))\n" +
                        "\n" +
                        "select distinct id from (select a.created ts_stop, a.id, b.created ts_start, b.id from stops a lt join starts b on (id)) where id <> '0x05ab1e873d165b00000005743f2c17' and ts_stop - ts_start > 10000000000\n",
                modelOf("telemetry_users")
                        .col("id", ColumnType.LONG256)
                        .col("created", ColumnType.TIMESTAMP)
                        .col("event", ColumnType.SHORT)
                        .timestamp()
        );
    }

    @Test
    public void testJoinTriangle() throws Exception {
        assertQuery(
                "select-choose o.a a, o.b b, o.c c, c.c c1, c.d d, c.e e, d.b b1, d.d d1, d.quantity quantity from (select [a, b, c] from orders o join select [c, d, e] from customers c on c.c = o.c join select [b, d, quantity] from orderDetails d on d.d = c.d and d.b = o.b) o",
                "orders o" +
                        " join customers c on(c)" +
                        " join orderDetails d on o.b = d.b and c.d = d.d",

                modelOf("orders")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.LONG),
                modelOf("customers")
                        .col("c", ColumnType.LONG)
                        .col("d", ColumnType.INT)
                        .col("e", ColumnType.INT),
                modelOf("orderDetails")
                        .col("b", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("quantity", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testJoinWith() throws SqlException {
        assertQuery(
                "select-choose x.y y, x1.y y1, x2.y y2 from (select [y] from (select-choose [y] y from (select [y] from tab)) x cross join select [y] from (select-choose [y] y from (select [y] from tab)) x1 cross join select [y] from (select-choose [y] y from (select [y] from tab)) x2) x",
                "with x as (select * from tab) x cross join x x1 cross join x x2",
                modelOf("tab").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testJoinWithClausesDefaultAlias() throws SqlException {
        assertQuery(
                "select-choose cust.customerId customerId, cust.name name, ord.customerId customerId1 from (select [customerId, name] from (select-choose [customerId, name] customerId, name from (select [customerId, name] from customers where name ~ 'X')) cust outer join select [customerId] from (select-choose [customerId] customerId from (select [customerId, amount] from orders where amount > 100)) ord on ord.customerId = cust.customerId post-join-where ord.customerId != null) cust limit 10",
                "with" +
                        " cust as (customers where name ~ 'X')," +
                        " ord as (select customerId from orders where amount > 100)" +
                        " cust outer join ord on (customerId) " +
                        " where ord.customerId != null" +
                        " limit 10",
                modelOf("customers").col("customerId", ColumnType.INT).col("name", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("amount", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testJoinWithClausesExplicitAlias() throws SqlException {
        assertQuery(
                "select-choose c.customerId customerId, c.name name, o.customerId customerId1 from (select [customerId, name] from (select-choose [customerId, name] customerId, name from (select [customerId, name] from customers where name ~ 'X')) c outer join select [customerId] from (select-choose [customerId] customerId from (select [customerId, amount] from orders where amount > 100)) o on o.customerId = c.customerId post-join-where o.customerId != null) c limit 10",
                "with" +
                        " cust as (customers where name ~ 'X')," +
                        " ord as (select customerId from orders where amount > 100)" +
                        " cust c outer join ord o on (customerId) " +
                        " where o.customerId != null" +
                        " limit 10",
                modelOf("customers").col("customerId", ColumnType.INT).col("name", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.INT).col("amount", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testJoinWithFilter() throws Exception {
        assertQuery(
                "select-choose" +
                        " customers.customerId customerId," +
                        " orders.orderId orderId," +
                        " d.orderId orderId1," +
                        " d.productId productId," +
                        " d.quantity quantity," +
                        " products.productId productId1," +
                        " products.supplier supplier," +
                        " products.price price," +
                        " suppliers.supplier supplier1" +
                        " from (" +
                        "select [customerId] from customers" +
                        " join (select [orderId, productId, quantity] from orderDetails d where productId = orderId) d on d.productId = customers.customerId" +
                        " join select [orderId] from orders on orders.orderId = d.orderId post-join-where d.quantity < orders.orderId" +
                        " join select [productId, supplier, price] from products on products.productId = d.productId post-join-where products.price > d.quantity or d.orderId = orders.orderId" +
                        " join select [supplier] from suppliers on suppliers.supplier = products.supplier" +
                        ")",
                "customers" +
                        " cross join orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId" +
                        " and (products.price > d.quantity or d.orderId = orders.orderId) and d.quantity < orders.orderId",

                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails")
                        .col("orderId", ColumnType.INT)
                        .col("productId", ColumnType.INT)
                        .col("quantity", ColumnType.DOUBLE),
                modelOf("products").col("productId", ColumnType.INT)
                        .col("supplier", ColumnType.INT)
                        .col("price", ColumnType.DOUBLE),
                modelOf("suppliers").col("supplier", ColumnType.INT)
        );
    }

    @Test
    public void testJoinWithFunction() throws SqlException {
        assertQuery("select-choose x1.a a, x1.s s, x2.a a1, x2.s s1 from (select [a, s] from (select-virtual [rnd_int() a, rnd_symbol(4,4,4,2) s] rnd_int() a, rnd_symbol(4,4,4,2) s from (long_sequence(10))) x1 join select [a, s] from (select-virtual [rnd_int() a, rnd_symbol(4,4,4,2) s] rnd_int() a, rnd_symbol(4,4,4,2) s from (long_sequence(10))) x2 on x2.s = x1.s) x1",
                "with x as (select rnd_int() a, rnd_symbol(4,4,4,2) s from long_sequence(10)) " +
                        "select * from x x1 join x x2 on (s)");
    }

    @Test
    public void testLatestByKeepWhereOutside() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from x latest by b where b = 'PEHN' and a < 22 and test_match())",
                "select * from x latest by b where b = 'PEHN' and a < 22 and test_match()",
                modelOf("x").col("a", ColumnType.INT).col("b", ColumnType.STRING));
    }

    @Test
    public void testLatestByMultipleColumns() throws SqlException {
        assertQuery(
                "select-group-by ts, market_type, avg(bid_price) avg from (select [ts, market_type, bid_price] from market_updates timestamp (ts) latest by ts,market_type) sample by 1s",
                "select ts, market_type, avg(bid_price) FROM market_updates LATEST BY ts, market_type SAMPLE BY 1s",
                modelOf("market_updates").timestamp("ts").col("market_type", ColumnType.SYMBOL).col("bid_price", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testLatestByNonSelectedColumn() throws Exception {
        assertQuery(
                "select-choose x, y from (select-choose [x, y] x, y from (select [y, x, z] from tab t2 latest by z where x > 100) t2 where y > 0) t1",
                "select x, y from (select x, y from tab t2 latest by z where x > 100) t1 where y > 0",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.STRING)
        );
    }

    @Test
    public void testLatestByWithOuterFilter() throws SqlException {
        assertQuery(
                "select-choose time, uuid from (select-choose [time, uuid] time, uuid from (select [uuid, time] from positions timestamp (time) latest by uuid where time < '2021-05-11T14:00') where uuid = '006cb7c6-e0d5-3fea-87f2-83cf4a75bc28')",
                "(positions latest by uuid where time < '2021-05-11T14:00') where uuid = '006cb7c6-e0d5-3fea-87f2-83cf4a75bc28'",
                modelOf("positions").timestamp("time").col("uuid", ColumnType.SYMBOL)

        );
    }

    @Test
    public void testLatestBySyntax() throws Exception {
        assertSyntaxError(
                "select * from tab latest",
                24,
                "by expected"
        );
    }

    @Test
    public void testLatestBySyntax2() throws Exception {
        assertSyntaxError(
                "select * from tab latest by x, ",
                30,
                "literal expected"
        );
    }

    @Test
    public void testLatestBySyntax3() throws Exception {
        assertSyntaxError(
                "select * from tab latest by",
                27,
                "literal expected"
        );
    }

    @Test
    public void testLatestBySyntax4() throws Exception {
        assertSyntaxError(
                "select * from tab latest by x+1",
                29,
                "unexpected token: +"
        );
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a outer join select [x] from b on b.x = a.x) a",
                "select a.x from a a left outer join b on b.x = a.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testLexerReset() {
        for (int i = 0; i < 10; i++) {
            try {
                compiler.compile("select \n" +
                        "-- ltod(Date)\n" +
                        "count() \n" +
                        "-- from acc\n" +
                        "from acc(Date) sample by 1d\n" +
                        "-- where x = 10\n", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                // we now allow column reference from SQL although column access will fail
                TestUtils.assertEquals("unknown function name: acc(LONG)", e.getFlyweightMessage());
            }
        }
    }

    @Test
    public void testLineCommentAtEnd() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) 'b a' where x > 1\n--this is comment",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT));
    }

    @Test
    public void testLineCommentAtMiddle() throws Exception {
        assertQuery("select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) \n" +
                        " -- this is a comment \n" +
                        "'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT));
    }

    @Test
    public void testLineCommentAtStart() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "-- hello, this is a comment\n (x where a > 1) 'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT));
    }

    @Test
    public void testMissingArgument() throws Exception {
        assertSyntaxError(
                "select x from tab where not (x != 1 and)",
                36,
                "too few arguments for 'and' [found=1,expected=2]",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testMissingTable() throws Exception {
        assertSyntaxError(
                "select a from",
                13,
                "table name or sub-query expected"
        );
    }

    @Test
    public void testMissingTableInSubQuery() throws Exception {
        // todo: 24 is the correct position
        assertSyntaxError(
                "with x as (select a from) x",
                25,
                "table name or sub-query expected",
                modelOf("tab").col("b", ColumnType.INT)
        );
    }

    @Test
    public void testMissingWhere() {
        try {
            compiler.compile("select id, x + 10, x from tab id ~ 'HBRO'", sqlExecutionContext);
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            Assert.assertEquals(33, e.getPosition());
        }
    }

    @Test
    public void testMixedFieldsSubQuery() throws Exception {
        assertQuery(
                "select-choose x, y from (select-virtual [x, z + x y] x, z + x y from (select [x, z] from tab t2 latest by x where x > 100) t2 where y > 0) t1",
                "select x, y from (select x,z + x y from tab t2 latest by x where x > 100) t1 where y > 0",
                modelOf("tab").col("x", ColumnType.INT).col("z", ColumnType.INT));
    }

    @Test
    public void testMostRecentWhereClause() throws Exception {
        assertQuery(
                "select-virtual x, sum + 25 ohoh from (select-group-by [x, sum(z) sum] x, sum(z) sum from (select-virtual [a + b * c x, z] a + b * c x, z from (select [a, c, b, z, x, y] from zyzy latest by x where a in (x,y) and b = 10)))",
                "select a+b*c x, sum(z)+25 ohoh from zyzy latest by x where a in (x,y) and b = 10",
                modelOf("zyzy")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testMultipleExpressions() throws Exception {
        assertQuery(
                "select-virtual x, sum + 25 ohoh from (select-group-by [x, sum(z) sum] x, sum(z) sum from (select-virtual [a + b * c x, z] a + b * c x, z from (select [a, c, b, z] from zyzy)))",
                "select a+b*c x, sum(z)+25 ohoh from zyzy",
                modelOf("zyzy")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testNestedJoinReorder() throws Exception {
        assertQuery(
                "select-choose x.orderId orderId, x.productId productId, y.orderId orderId1, y.customerId customerId, y.customerId1 customerId1, y.orderId1 orderId11, y.productId productId1, y.supplier supplier, y.productId1 productId11, y.supplier1 supplier1 from (select [orderId, productId] from (select-choose [orders.orderId orderId, products.productId productId] orders.orderId orderId, products.productId productId from (select [orderId, customerId] from orders join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.orderId = orders.customerId join select [customerId] from customers on customers.customerId = orders.customerId join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier where orderId = customerId)) x cross join select [orderId, customerId, customerId1, orderId1, productId, supplier, productId1, supplier1] from (select-choose [orders.orderId orderId, orders.customerId customerId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, products.productId productId1, products.supplier supplier1] orders.orderId orderId, orders.customerId customerId, customers.customerId customerId1, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, products.productId productId1, products.supplier supplier1 from (select [orderId, customerId] from orders join select [customerId] from customers on customers.customerId = orders.customerId join (select [orderId, productId] from orderDetails d where orderId = productId) d on d.productId = orders.orderId join select [supplier] from suppliers on suppliers.supplier = orders.orderId join select [productId, supplier] from products on products.productId = orders.orderId and products.supplier = suppliers.supplier)) y) x",
                "with x as (select orders.orderId, products.productId from " +
                        "orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.orderId = customers.customerId" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId), " +
                        " y as (" +
                        "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and orders.orderId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId and orders.orderId = products.productId" +
                        " where orders.orderId = suppliers.supplier)" +
                        " x cross join y",
                modelOf("orders").col("orderId", ColumnType.INT).col("customerId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT),
                modelOf("shippers").col("shipper", ColumnType.INT)
        );
    }

    @Test
    public void testNonAggFunctionWithAggFunctionSampleBy() throws SqlException {
        assertQuery(
                "select-group-by day, isin, last(start_price) last from (select-virtual [day(ts) day, isin, start_price] day(ts) day, isin, start_price, ts from (select [ts, isin, start_price] from xetra timestamp (ts) where isin = 'DE000A0KRJS4')) sample by 1d",
                "select day(ts), isin, last(start_price) from xetra where isin='DE000A0KRJS4' sample by 1d",
                modelOf("xetra")
                        .timestamp("ts")
                        .col("isin", ColumnType.SYMBOL)
                        .col("start_price", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testNoopGroupBy() throws SqlException {
        assertQuery(
                "select-group-by sym, avg(bid) avgBid from (select [sym, bid] from x timestamp (ts) where sym in ('AA','BB'))",
                "select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by sym",
                modelOf("x")
                        .col("sym", ColumnType.SYMBOL)
                        .col("bid", ColumnType.INT)
                        .col("ask", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testNoopGroupByAfterFrom() throws Exception {
        assertQuery(
                "select-group-by sym, avg(bid) avgBid from (select [sym, bid] from x timestamp (ts))",
                "select sym, avg(bid) avgBid from x group by sym",
                modelOf("x")
                        .col("sym", ColumnType.SYMBOL)
                        .col("bid", ColumnType.INT)
                        .col("ask", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testNoopGroupByFailureWhenMissingColumn() throws Exception {
        assertFailure(
                "select sym, avg(bid) avgBid from x where sym in ('AA', 'BB' ) group by ",
                "create table x (\n" +
                        "    sym symbol,\n" +
                        "    bid int,\n" +
                        "    ask int\n" +
                        ")  partition by NONE",
                71,
                "literal expected"
        );
    }

    @Test
    public void testNotMoveWhereIntoDistinct() throws SqlException {
        assertQuery(
                "select-choose a from (select-distinct [a] a from (select-choose [a] a from (select [a] from tab)) where a = 10)",
                "(select distinct a from tab) where a = 10",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testNullChecks() throws SqlException {
        assertQuery(
                "select-choose a from (select [a, time] from x timestamp (time) where time in ('2020-08-01T17:00:00.305314Z','2020-09-20T17:00:00.312334Z'))",
                "SELECT \n" +
                        "a\n" +
                        "FROM x WHERE b = 'H' AND time in('2020-08-01T17:00:00.305314Z' , '2020-09-20T17:00:00.312334Z')\n" +
                        "select *", // <-- dangling 'select *'
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.SYMBOL)
                        .timestamp("time")
        );
    }

    @Test
    public void testOfOrderByOnMultipleColumnsWhenColumnIsMissing() throws Exception {
        assertQuery(
                "select-choose z from (select-choose [y z, x] y z, x from (select [y, x] from tab) order by z desc, x)",
                "select y z  from tab order by z desc, x",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testOneAnalyticColumn() throws Exception {
        assertQuery(
                "select-analytic a, b, f(c) f over (partition by b order by ts) from (select [a, b, c, ts] from xyz timestamp (ts))",
                "select a,b, f(c) over (partition by b order by ts) from xyz",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testOneAnalyticColumnAndLimit() throws Exception {
        assertQuery("select-analytic a, b, f(c) f over (partition by b order by ts) from (select [a, b, c, ts] from xyz timestamp (ts)) limit 200",
                "select a,b, f(c) over (partition by b order by ts) from xyz limit 200",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testOneAnalyticColumnPrefixed() throws Exception {
        // extra model in the middle is because we reference "b" as both "b" and "z.b"
        assertQuery(
                "select-analytic a, b, row_number() row_number over (partition by b1 order by ts) from (select-choose [a, b, b b1, ts] a, b, b b1, ts from (select [a, b, ts] from xyz z timestamp (ts)) z) z",
                "select a,b, row_number() over (partition by z.b order by z.ts) from xyz z",
                modelOf("xyz")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testOptimiseNotAnd() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a != b or b != a)",
                "select a, b from tab where not (a = b and b = a)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotEqual() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a != b)",
                "select a, b from tab where not (a = b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotGreater() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a <= b)",
                "select a, b from tab where not (a > b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotGreaterOrEqual() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a < b)",
                "select a, b from tab where not (a >= b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotLess() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a >= b)",
                "select a, b from tab where not (a < b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotLessOrEqual() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a > b)",
                "select a, b from tab where not (a <= b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotLiteral() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where not(a))",
                "select a, b from tab where not (a)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotLiteralOr() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where not(a) and b != a)",
                "select a, b from tab where not (a or b = a)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotNotEqual() throws SqlException {
        assertQuery("select-choose a, b from (select [a, b] from tab where a = b)",
                "select a, b from tab where not (a != b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotNotNotEqual() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a != b)",
                "select a, b from tab where not(not (a != b))",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotOr() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where a != b and b != a)",
                "select a, b from tab where not (a = b or b = a)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptimiseNotOrLiterals() throws SqlException {
        assertQuery(
                "select-choose a, b from (select [a, b] from tab where not(a) and not(b))",
                "select a, b from tab where not (a or b)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT));
    }

    @Test
    public void testOptionalSelect() throws Exception {
        assertQuery(
                "select-choose x from (select [x] from tab t2 latest by x where x > 100) t2",
                "tab t2 latest by x where x > 100",
                modelOf("tab").col("x", ColumnType.INT));
    }

    @Test
    public void testOrderBy1() throws Exception {
        assertQuery(
                "select-choose x, y from (select-choose [x, y, z] x, y, z from (select [x, y, z] from tab) order by x, y, z)",
                "select x,y from tab order by x,y,z",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByAmbiguousColumn() throws Exception {
        assertSyntaxError(
                "select tab1.x from tab1 join tab2 on (x) order by y",
                50,
                "Ambiguous",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByExpression() throws Exception {
        assertSyntaxError("select x, y from tab order by x+y", 31, "literal expected");
    }

    @Test
    public void testOrderByGroupByCol() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab) order by b",
                "select a, sum(b) b from tab order by b",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByGroupByCol2() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab) order by a",
                "select a, sum(b) b from tab order by a",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByGroupByColPrefixed() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab) order by a",
                "select a, sum(b) b from tab order by tab.a",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByGroupByColWithAliasDifferentToColumnName() throws SqlException {
        assertQuery(
                "select-group-by a, max(b) x from (select [a, b] from tab) order by x",
                "select a, max(b) x from tab order by x",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByIssue1() throws SqlException {
        assertQuery(
                "select-virtual to_date(timestamp) t from (select [timestamp] from blocks.csv) order by t",
                "select to_date(timestamp) t from 'blocks.csv' order by t",
                modelOf("blocks.csv").col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testOrderByOnAliasedColumn() throws SqlException {
        assertQuery(
                "select-choose y from (select [y] from tab) order by y",
                "select y from tab order by tab.y",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnExpression() throws SqlException {
        assertQuery(
                "select-virtual y + x z from (select [x, y] from tab) order by z",
                "select y+x z from tab order by z",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnJoinSubQuery() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y, b.s s from (select [x, z] from (select-choose [x, z] x, z from (select [x, z] from tab1 where x = 'Z')) a join select [y, s, z] from (select-choose [y, s, z] x, y, z, s from (select [y, s, z] from tab2 where s ~ 'K')) b on b.z = a.z) a order by s",
                "select a.x, b.y, b.s from (select x,z from tab1 where x = 'Z' order by x) a join (tab2 where s ~ 'K') b on a.z=b.z order by b.s",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnJoinSubQuery2() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y from (select [x, z] from (select-choose [x, z] x, z from (select-choose [x, z, p] x, z, p from (select [x, z, p] from tab1 where x = 'Z') order by p)) a join select [y, z] from (select-choose [y, z] x, y, z, s from (select [y, z, s] from tab2 where s ~ 'K')) b on b.z = a.z) a",
                "select a.x, b.y from (select x,z from tab1 where x = 'Z' order by p) a join (tab2 where s ~ 'K') b on a.z=b.z",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("p", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnJoinSubQuery3() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y from (select [x] from (select-choose [x, z] x, z from (select [x, z] from tab1 where x = 'Z') order by z) a asof join select [y, z] from (select-choose [y, z, s] y, z, s from (select [y, z, s] from tab2 where s ~ 'K') order by s) b on b.z = a.x) a",
                "select a.x, b.y from (select x,z from tab1 where x = 'Z' order by z) a asof join (select y,z,s from tab2 where s ~ 'K' order by s) b where a.x = b.z",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnJoinTableReference() throws SqlException {
        assertQuery(
                "select-choose a.x x, b.y y, b.s s from (select [x, z] from tab1 a join select [y, s, z] from tab2 b on b.z = a.z) a order by s",
                "select a.x, b.y, b.s from tab1 a join tab2 b on a.z = b.z order by b.s",
                modelOf("tab1")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
                        .col("s", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnMultipleColumns() throws SqlException {
        assertQuery(
                "select-choose y z, x from (select [y, x] from tab) order by z desc, x",
                "select y z, x from tab order by z desc, x",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnNonSelectedColumn() throws Exception {
        assertQuery(
                "select-choose y from (select-choose [y, x] y, x from (select [y, x] from tab) order by x)",
                "select y from tab order by x",
                modelOf("tab")
                        .col("y", ColumnType.DOUBLE)
                        .col("x", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testOrderByOnNonSelectedColumn2() throws Exception {
        assertQuery(
                "select-virtual 2 * y + x column, 3 / x xx from (select [x, y] from tab) order by xx",
                "select 2*y+x, 3/x xx from tab order by xx",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnOuterResult() throws SqlException {
        assertQuery(
                "select-virtual x, sum1 + sum z from (select-group-by [x, sum(3 / x) sum, sum(2 * y + x) sum1] x, sum(3 / x) sum, sum(2 * y + x) sum1 from (select [x, y] from tab)) order by z",
                "select x, sum(2*y+x) + sum(3/x) z from tab order by z asc",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByOnSelectedAlias() throws SqlException {
        assertQuery("select-choose y z from (select [y] from tab) order by z",
                "select y z from tab order by z",
                modelOf("tab")
                        .col("x", ColumnType.DOUBLE)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testOrderByPosition() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y] from tab) order by y, x",
                "select x,y from tab order by 2,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPositionCorrupt() throws Exception {
        assertSyntaxError(
                "tab order by 3a, 1",
                13,
                "Invalid column: 3a",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)

        );
    }

    @Test
    public void testOrderByPositionNoSelect() throws Exception {
        assertQuery(
                "select-choose x, y, z from (select [x, y, z] from tab) order by z desc, x",
                "tab order by 3 desc,1",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testOrderByPositionOutOfRange1() throws Exception {
        assertSyntaxError(
                "tab order by 0, 1",
                13,
                "order column position is out of range",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)

        );
    }

    @Test
    public void testOrderByPositionOutOfRange2() throws Exception {
        assertSyntaxError(
                "tab order by 2, 4",
                16,
                "order column position is out of range",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)

        );
    }

    @Test
    public void testOrderByPropagation() throws SqlException {
        assertQuery(
                "select-choose id, customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType from (select-choose [C.contactId id, contactlist.customName customName, contactlist.name name, contactlist.email email, contactlist.country_name country_name, contactlist.country_code country_code, contactlist.city city, contactlist.region region, contactlist.emoji_flag emoji_flag, contactlist.latitude latitude, contactlist.longitude longitude, contactlist.isNotReal isNotReal, contactlist.notRealType notRealType, timestamp] C.contactId id, contactlist.customName customName, contactlist.name name, contactlist.email email, contactlist.country_name country_name, contactlist.country_code country_code, contactlist.city city, contactlist.region region, contactlist.emoji_flag emoji_flag, contactlist.latitude latitude, contactlist.longitude longitude, contactlist.isNotReal isNotReal, contactlist.notRealType notRealType, timestamp from (select [contactId] from (select-distinct [contactId] contactId from (select-choose [contactId] contactId from (select-choose [contactId, groupId] contactId, groupId, timestamp from (select [groupId, contactId] from contact_events timestamp (timestamp) latest by _id) where groupId = 'qIqlX6qESMtTQXikQA46') eventlist) except select-choose [_id contactId] _id contactId from (select-choose [_id, notRealType] _id, customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp from (select [notRealType, _id] from contacts timestamp (timestamp) latest by _id) where notRealType = 'bot') contactlist) C join select [customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp, _id] from (select-choose [customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp, _id] _id, customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp from (select [customName, name, email, country_name, country_code, city, region, emoji_flag, latitude, longitude, isNotReal, notRealType, timestamp, _id] from contacts timestamp (timestamp) latest by _id)) contactlist on contactlist._id = C.contactId) C order by timestamp desc)",
                "WITH \n" +
                        "contactlist AS (SELECT * FROM contacts LATEST BY _id ORDER BY timestamp),\n" +
                        "eventlist AS (SELECT * FROM contact_events LATEST BY _id ORDER BY timestamp),\n" +
                        "C AS (\n" +
                        "    SELECT DISTINCT contactId FROM eventlist WHERE groupId = 'qIqlX6qESMtTQXikQA46'\n" +
                        "    EXCEPT\n" +
                        "    SELECT _id as contactId FROM contactlist WHERE notRealType = 'bot'\n" +
                        ")\n" +
                        "  SELECT \n" +
                        "    C.contactId as id, \n" +
                        "    contactlist.customName,\n" +
                        "    contactlist.name,\n" +
                        "    contactlist.email,\n" +
                        "    contactlist.country_name,\n" +
                        "    contactlist.country_code,\n" +
                        "    contactlist.city,\n" +
                        "    contactlist.region,\n" +
                        "    contactlist.emoji_flag,\n" +
                        "    contactlist.latitude,\n" +
                        "    contactlist.longitude,\n" +
                        "    contactlist.isNotReal,\n" +
                        "    contactlist.notRealType\n" +
                        "  FROM C \n" +
                        "  JOIN contactlist ON contactlist._id = C.contactId\n" +
                        "  ORDER BY timestamp DESC\n",
                modelOf("contacts")
                        .col("_id", ColumnType.SYMBOL)
                        .col("customName", ColumnType.STRING)
                        .col("name", ColumnType.SYMBOL)
                        .col("email", ColumnType.STRING)
                        .col("country_name", ColumnType.SYMBOL)
                        .col("country_code", ColumnType.SYMBOL)
                        .col("city", ColumnType.SYMBOL)
                        .col("region", ColumnType.SYMBOL)
                        .col("emoji_flag", ColumnType.STRING)
                        .col("latitude", ColumnType.DOUBLE)
                        .col("longitude", ColumnType.DOUBLE)
                        .col("isNotReal", ColumnType.SYMBOL)
                        .col("notRealType", ColumnType.SYMBOL)
                        .timestamp(),
                modelOf("contact_events")
                        .col("contactId", ColumnType.SYMBOL)
                        .col("groupId", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testOrderByWithLatestBy() throws Exception {
        assertQuery(
                "select-choose id, vendor, pickup_datetime from (select [id, vendor, pickup_datetime] from trips timestamp (pickup_datetime) latest by vendor_id where pickup_datetime < '2009-01-01T00:02:19.000000Z') order by pickup_datetime",
                "SELECT * FROM trips\n" +
                        "latest by vendor_id\n" +
                        "WHERE pickup_datetime < '2009-01-01T00:02:19.000000Z'\n" +
                        "ORDER BY pickup_datetime",
                modelOf("trips")
                        .col("id", ColumnType.INT)
                        .col("vendor", ColumnType.SYMBOL)
                        .timestamp("pickup_datetime")
        );
    }

    @Test
    public void testOrderByWithSampleBy() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) sum from (select-choose [t, a, b] a, b, t from (select [t, a, b] from tab) order by t) timestamp (t) sample by 2m order by a",
                "select a, sum(b) from (tab order by t) timestamp(t) sample by 2m order by a",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testOrderByWithSampleBy2() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) sum from (select-group-by [a, sum(b) b] a, sum(b) b from (select-choose [t, a, b] a, b, t from (select [t, a, b] from tab) order by t) timestamp (t) sample by 10m) order by a",
                "select a, sum(b) from (select a,sum(b) b from (tab order by t) timestamp(t) sample by 10m order by t) order by a",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testOrderByWithSampleBy3() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) sum from (select-group-by [a, sum(b) b] a, sum(b) b from (select-choose [a, b, t] a, b, t from (select [a, b, t] from tab timestamp (t)) order by t) sample by 10m) order by a",
                "select a, sum(b) from (select a,sum(b) b from (tab order by t) sample by 10m order by t) order by a",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .timestamp("t")
        );
    }

    @Test
    public void testOuterJoin() throws Exception {
        assertQuery(
                "select-choose a.x x from (select [x] from a a outer join select [x] from b on b.x = a.x) a",
                "select a.x from a a outer join b on b.x = a.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoinColumnAlias() throws SqlException {
        assertQuery("select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c outer join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = NaN) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " outer join orders o on c.customerId = o.customerId) " +
                        " where kk = NaN limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoinColumnAliasConst() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c outer join (select [customerId] from orders o where customerId = 10) o on o.customerId = c.customerId) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " outer join orders o on c.customerId = o.customerId) " +
                        " where kk = 10 limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testOuterJoinColumnAliasNull() throws SqlException {
        assertQuery("select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c outer join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = null) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " outer join orders o on c.customerId = o.customerId) " +
                        " where kk = null limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    @Ignore
    public void testPGColumnListQuery() throws SqlException {
        assertQuery(
                "",
                "SELECT c.oid,\n" +
                        "  n.nspname,\n" +
                        "  c.relname\n" +
                        "FROM pg_catalog.pg_class c\n" +
                        "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                        "WHERE c.relname OPERATOR(pg_catalog.~) E'^(movies\\\\.csv)$'\n" +
                        "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
                        "ORDER BY 2, 3;");
    }

    @Test
    public void testPGTableListQuery() throws SqlException {
        assertQuery(
                "select-virtual Schema, Name, switch(relkind,'r','table','v','view','m','materialized view','i','index','S','sequence','s','special','f','foreign table','p','table','I','index') Type, pg_catalog.pg_get_userbyid(relowner) Owner from (select-choose [n.nspname Schema, c.relname Name, c.relkind relkind, c.relowner relowner] n.nspname Schema, c.relname Name, c.relkind relkind, c.relowner relowner from (select [relname, relkind, relowner, relnamespace, oid] from pg_catalog.pg_class() c outer join select [nspname, oid] from pg_catalog.pg_namespace() n on n.oid = c.relnamespace post-join-where n.nspname != 'pg_catalog' and n.nspname != 'information_schema' and n.nspname !~ '^pg_toast' where relkind in ('r','p','v','m','S','f','') and pg_catalog.pg_table_is_visible(oid)) c) c order by Schema, Name",
                "SELECT n.nspname                              as \"Schema\",\n" +
                        "       c.relname                              as \"Name\",\n" +
                        "       CASE c.relkind\n" +
                        "           WHEN 'r' THEN 'table'\n" +
                        "           WHEN 'v' THEN 'view'\n" +
                        "           WHEN 'm' THEN 'materialized view'\n" +
                        "           WHEN 'i' THEN 'index'\n" +
                        "           WHEN 'S' THEN 'sequence'\n" +
                        "           WHEN 's' THEN 'special'\n" +
                        "           WHEN 'f' THEN 'foreign table'\n" +
                        "           WHEN 'p' THEN 'table'\n" +
                        "           WHEN 'I' THEN 'index' END          as \"Type\",\n" +
                        "       pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n" +
                        "FROM pg_catalog.pg_class c\n" +
                        "         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                        "WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', '')\n" +
                        "  AND n.nspname != 'pg_catalog'\n" +
                        "  AND n.nspname != 'information_schema'\n" +
                        "  AND n.nspname !~ '^pg_toast'\n" +
                        "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
                        "ORDER BY 1, 2"
        );
    }

    @Test
    public void testPipeConcatInJoin() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x from (select [x] from tab join select [y] from bap on " +
                        "concat('2',substring(bap.y,0,5)) = concat(tab.x,'1'))",
                "select 1, x from tab join bap on tab.x || '1' = '2' || substring(bap.y, 0, 5)",
                modelOf("tab").col("x", ColumnType.STRING),
                modelOf("bap").col("y", ColumnType.STRING)
        );
    }

    @Test
    public void testPipeConcatInWhere() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x from (select [x, z, y] from tab where concat(x,'-',y) = z)",
                "select 1, x from tab where x || '-' || y = z",
                modelOf("tab").col("x", ColumnType.STRING)
                        .col("y", ColumnType.STRING)
                        .col("z", ColumnType.STRING)
        );
    }

    @Test
    public void testPipeConcatNested() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',x,'3') concat from (select [x] from tab)",
                "select 1, x, '2' || x || '3' from tab",
                modelOf("tab").col("x", ColumnType.STRING)
        );
    }

    @Test
    public void testPipeConcatNested4() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',x,'3',y) concat from (select [x, y] from tab)",
                "select 1, x, '2' || x || '3' || y from tab",
                modelOf("tab").col("x", ColumnType.STRING).col("y", ColumnType.STRING)
        );
    }

    @Test
    public void testPipeConcatWithFunctionConcatOnLeft() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',cast(x + 1,string),'3') concat from (select [x] from tab)",
                "select 1, x, concat('2', cast(x + 1 as string)) || '3' from tab",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testPipeConcatWithFunctionConcatOnRight() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',cast(x + 1,string),'3') concat from (select [x] from tab)",
                "select 1, x, '2' || concat(cast(x + 1 as string), '3') from tab",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testPipeConcatWithNestedCast() throws SqlException {
        assertQuery(
                "select-virtual 1 1, x, concat('2',cast(x + 1,string),'3') concat from (select [x] from tab)",
                "select 1, x, '2' || cast(x + 1 as string) || '3' from tab",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testQueryExceptQuery() throws SqlException {
        assertQuery(
                "select-choose a, b, c, x, y, z from (select [a, b, c, x, y, z] from x) except select-choose a, b, c, x, y, z from (select [a, b, c, x, y, z] from y)",
                "select * from x except select* from y",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("y")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testQueryIntersectQuery() throws SqlException {
        assertQuery(
                "select-choose a, b, c, x, y, z from (select [a, b, c, x, y, z] from x) intersect select-choose a, b, c, x, y, z from (select [a, b, c, x, y, z] from y)",
                "select * from x intersect select* from y",
                modelOf("x")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT),
                modelOf("y")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testRedundantSelect() throws Exception {
        assertSyntaxError(
                "select x from select (select x from a) timestamp(x)",
                22,
                "query is not expected",
                modelOf("a").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testRegexOnFunction() throws SqlException {
        assertQuery(
                "select-choose a from (select-virtual [rnd_str() a] rnd_str() a from (long_sequence(100)) where a ~ '^W')",
                "(select rnd_str() a from long_sequence(100)) where a ~ '^W'"
        );
    }

    @Test
    public void testSampleBy() throws Exception {
        assertQuery(
                "select-group-by x, sum(y) sum from (select [x, y] from tab timestamp (timestamp)) sample by 2m",
                "select x,sum(y) from tab sample by 2m",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSampleByAliasedColumn() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k, k k1 from (select [b, a, k] from x y timestamp (timestamp)) y sample by 3h",
                "select b, sum(a), k, k from x y sample by 3h",
                modelOf("x").col("a", ColumnType.DOUBLE).col("b", ColumnType.SYMBOL).col("k", ColumnType.TIMESTAMP).timestamp()
        );
    }

    @Test
    public void testSampleByAlreadySelected() throws Exception {
        assertQuery(
                "select-group-by x, sum(y) sum from (select [x, y] from tab timestamp (x)) sample by 2m",
                "select x,sum(y) from tab timestamp(x) sample by 2m",
                modelOf("tab")
                        .col("x", ColumnType.TIMESTAMP)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSampleByAltTimestamp() throws Exception {
        assertQuery(
                "select-group-by x, sum(y) sum from (select [x, y] from tab timestamp (t)) sample by 2m",
                "select x,sum(y) from tab timestamp(t) sample by 2m",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillList() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab timestamp (t)) sample by 10m fill(21.1,22,null,98)",
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill(21.1,22,null,98)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMin() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab timestamp (t)) sample by 10m fill(mid)",
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill(mid)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMinAsSubQuery() throws SqlException {
        assertQuery(
                "select-choose a, b from (select-group-by [a, sum(b) b] a, sum(b) b from (select [a, b] from tab timestamp (t)) sample by 10m fill(mid))",
                "select * from (select a,sum(b) b from tab timestamp(t) sample by 10m fill(mid))",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMissingCloseBrace() throws Exception {
        assertSyntaxError(
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill (21231.2344",
                70,
                "')' expected",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMissingOpenBrace() throws Exception {
        assertSyntaxError(
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill 21231.2344",
                59,
                "'(' expected",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillMissingValue() throws Exception {
        assertSyntaxError(
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill ()",
                60,
                "'none', 'prev', 'mid', 'null' or number expected",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByFillValue() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) b from (select [a, b] from tab timestamp (t)) sample by 10m fill(21231.2344)",
                "select a,sum(b) b from tab timestamp(t) sample by 10m fill(21231.2344)",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByIncorrectPlacement() throws Exception {
        assertSyntaxError(
                "select a, sum(b) from ((tab order by t) timestamp(t) sample by 10m order by t) order by a",
                63,
                "at least one aggregation function must be present",
                modelOf("tab")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSampleByInvalidColumn() throws Exception {
        assertSyntaxError("select x,sum(y) from tab timestamp(z) sample by 2m",
                35,
                "Invalid column",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSampleByInvalidType() throws Exception {
        assertSyntaxError("select x,sum(y) from tab timestamp(x) sample by 2m",
                35,
                "not a TIMESTAMP",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSampleByNoAggregate() throws Exception {
        assertSyntaxError("select x,y from tab sample by 2m", 30, "at least one",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSampleByUndefinedTimestamp() throws Exception {
        assertSyntaxError("select x,sum(y) from tab sample by 2m",
                7,
                "base query does not provide dedicated TIMESTAMP column",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSampleByUndefinedTimestampWithDistinct() throws Exception {
        assertSyntaxError("select x,sum(y) from (select distinct x, y from tab) sample by 2m",
                7,
                "base query does not provide dedicated TIMESTAMP column",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSampleByUndefinedTimestampWithJoin() throws Exception {
        assertSyntaxError("select tab.x,sum(y) from tab join tab2 on (x) sample by 2m",
                0,
                "TIMESTAMP column is required but not provided",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT),
                modelOf("tab2")
                        .col("x", ColumnType.INT)
                        .col("z", ColumnType.DOUBLE)
        );
    }

    @Test
    public void testSelectAfterOrderBy() throws SqlException {
        assertQuery("select-distinct Schema from (select-choose [Schema] Schema from (select-virtual [Schema, Name] Schema, Name, switch(relkind,'r','table','v','view','m','materialized view','i','index','S','sequence','s','special','f','foreign table','p','table','I','index') Type, pg_catalog.pg_get_userbyid(relowner) Owner from (select-choose [n.nspname Schema, c.relname Name] n.nspname Schema, c.relname Name, c.relkind relkind, c.relowner relowner from (select [relname, relnamespace, relkind, oid] from pg_catalog.pg_class() c outer join select [nspname, oid] from pg_catalog.pg_namespace() n on n.oid = c.relnamespace post-join-where n.nspname != 'pg_catalog' and n.nspname != 'information_schema' and n.nspname !~ '^pg_toast' where relkind in ('r','p','v','m','S','f','') and pg_catalog.pg_table_is_visible(oid)) c) c order by Schema, Name))",
                "select distinct Schema from \n" +
                        "(SELECT n.nspname                              as \"Schema\",\n" +
                        "       c.relname                              as \"Name\",\n" +
                        "       CASE c.relkind\n" +
                        "           WHEN 'r' THEN 'table'\n" +
                        "           WHEN 'v' THEN 'view'\n" +
                        "           WHEN 'm' THEN 'materialized view'\n" +
                        "           WHEN 'i' THEN 'index'\n" +
                        "           WHEN 'S' THEN 'sequence'\n" +
                        "           WHEN 's' THEN 'special'\n" +
                        "           WHEN 'f' THEN 'foreign table'\n" +
                        "           WHEN 'p' THEN 'table'\n" +
                        "           WHEN 'I' THEN 'index' END          as \"Type\",\n" +
                        "       pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n" +
                        "FROM pg_catalog.pg_class c\n" +
                        "         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                        "WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', '')\n" +
                        "  AND n.nspname != 'pg_catalog'\n" +
                        "  AND n.nspname != 'information_schema'\n" +
                        "  AND n.nspname !~ '^pg_toast'\n" +
                        "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
                        "ORDER BY 1, 2);\n"
        );
    }

    @Test
    public void testSelectAliasAsFunction() throws Exception {
        assertSyntaxError(
                "select sum(x) x() from tab",
                15,
                "',', 'from' or 'over' expected",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSelectAliasIgnoreColumn() throws SqlException {
        assertQuery(
                "select-virtual 3 x from (long_sequence(2))",
                "select 3 x from long_sequence(2)"
        );
    }

    @Test
    public void testSelectAliasNoWhere() throws SqlException {
        assertQuery(
                "select-virtual rnd_int(1,2,0) a from (long_sequence(1))",
                "select rnd_int(1, 2, 0) a"
        );
    }

    @Test
    public void testSelectAnalyticOperator() throws Exception {
        assertSyntaxError(
                "select sum(x), 2*x over() from tab",
                16,
                "Analytic function expected",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSelectAsAliasQuoted() throws SqlException {
        assertQuery(
                "select-choose a 'y y' from (select [a] from tab)",
                "select a as \"y y\" from tab",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testSelectColumnWithAlias() throws SqlException {
        assertQuery(
                "select-virtual a, rnd_int() c from (select-choose [x a] x a from (select [x] from long_sequence(5)))",
                "select x a, rnd_int() c from long_sequence(5)");
    }

    @Test
    public void testSelectColumnsFromJoinSubQueries() throws SqlException {
        assertQuery(
                "select-virtual addr, sum_out - sum_in total from (select-choose [a.addr addr, b.sum_in sum_in, a.sum_out sum_out] a.addr addr, a.count count, a.sum_out sum_out, b.toAddress toAddress, b.count count1, b.sum_in sum_in from (select [addr, sum_out] from (select-group-by [addr, sum(value) sum_out] addr, count() count, sum(value) sum_out from (select-choose [fromAddress addr, value] fromAddress addr, value from (select [fromAddress, value] from transactions.csv))) a join select [sum_in, toAddress] from (select-group-by [sum(value) sum_in, toAddress] toAddress, count() count, sum(value) sum_in from (select [value, toAddress] from transactions.csv)) b on b.toAddress = a.addr) a)",
                "select addr, sum_out - sum_in total from (\n" +
                        "(select fromAddress addr, count(), sum(value) sum_out from 'transactions.csv') a join\n" +
                        "(select toAddress, count(), sum(value) sum_in from 'transactions.csv') b on a.addr = b.toAddress\n" +
                        ")",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.LONG)
                        .col("toAddress", ColumnType.LONG)
                        .col("value", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectColumnsFromJoinSubQueries2() throws SqlException {
        assertQuery("select-choose addr, count, sum_out, toAddress, count1, sum_in from (select-choose [a.addr addr, a.count count, a.sum_out sum_out, b.toAddress toAddress, b.count count1, b.sum_in sum_in] a.addr addr, a.count count, a.sum_out sum_out, b.toAddress toAddress, b.count count1, b.sum_in sum_in from (select [addr, count, sum_out] from (select-group-by [addr, count() count, sum(value) sum_out] addr, count() count, sum(value) sum_out from (select-choose [fromAddress addr, value] fromAddress addr, value from (select [fromAddress, value] from transactions.csv))) a join select [toAddress, count, sum_in] from (select-group-by [toAddress, count() count, sum(value) sum_in] toAddress, count() count, sum(value) sum_in from (select [toAddress, value] from transactions.csv)) b on b.toAddress = a.addr) a)",
                "(\n" +
                        "(select fromAddress addr, count(), sum(value) sum_out from 'transactions.csv') a join\n" +
                        "(select toAddress, count(), sum(value) sum_in from 'transactions.csv') b on a.addr = b.toAddress\n" +
                        ")",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.LONG)
                        .col("toAddress", ColumnType.LONG)
                        .col("value", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectColumnsFromJoinSubQueries3() throws SqlException {
        assertQuery("select-choose a.addr addr, a.count count, a.sum_out sum_out, b.toAddress toAddress, b.count count1, b.sum_in sum_in from (select [addr, count, sum_out] from (select-group-by [addr, count() count, sum(value) sum_out] addr, count() count, sum(value) sum_out from (select-choose [fromAddress addr, value] fromAddress addr, value from (select [fromAddress, value] from transactions.csv))) a join select [toAddress, count, sum_in] from (select-group-by [toAddress, count() count, sum(value) sum_in] toAddress, count() count, sum(value) sum_in from (select [toAddress, value] from transactions.csv)) b on b.toAddress = a.addr) a",
                "(select fromAddress addr, count(), sum(value) sum_out from 'transactions.csv') a join\n" +
                        "(select toAddress, count(), sum(value) sum_in from 'transactions.csv') b on a.addr = b.toAddress\n",
                modelOf("transactions.csv")
                        .col("fromAddress", ColumnType.LONG)
                        .col("toAddress", ColumnType.LONG)
                        .col("value", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinct() throws SqlException {
        assertQuery(
                "select-distinct a, b from (select-choose [a, b] a, b from (select [a, b] from tab))",
                "select distinct a, b from tab",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctArithmetic() throws SqlException {
        assertQuery(
                "select-distinct column from (select-virtual [a + b column] a + b column from (select [b, a] from tab))",
                "select distinct a + b from tab",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctGroupByFunction() throws SqlException {
        assertQuery(
                "select-distinct a, bb from (select-group-by [a, sum(b) bb] a, sum(b) bb from (select [a, b] from tab))",
                "select distinct a, sum(b) bb from tab",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctGroupByFunctionArithmetic() throws SqlException {
        assertQuery(
                "select-distinct a, bb from (select-virtual [a, sum1 + sum bb] a, sum1 + sum bb from (select-group-by [a, sum(c) sum, sum(b) sum1] a, sum(c) sum, sum(b) sum1 from (select [a, c, b] from tab)))",
                "select distinct a, sum(b)+sum(c) bb from tab",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctGroupByFunctionArithmeticLimit() throws SqlException {
        assertQuery("select-distinct a, bb from (select-virtual [a, sum1 + sum bb] a, sum1 + sum bb from (select-group-by [a, sum(c) sum, sum(b) sum1] a, sum(c) sum, sum(b) sum1 from (select [a, c, b] from tab)) limit 10)",
                "select distinct a, sum(b)+sum(c) bb from tab limit 10",
                modelOf("tab")
                        .col("a", ColumnType.STRING)
                        .col("b", ColumnType.LONG)
                        .col("c", ColumnType.LONG)
        );
    }

    @Test
    public void testSelectDistinctUnion() throws SqlException {
        assertQuery("select-choose c from (select-distinct [c] c, b from (select-choose [a c] a c, b from (select [a] from trips)) union select-distinct [c] c, b from (select-choose [c] c, d b from (select [c] from trips)))",
                "select c from (select distinct a c, b from trips union all select distinct c, d b from trips)",
                modelOf("trips")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("tip_amount", ColumnType.DOUBLE)
                        .col("e", ColumnType.INT)
        );
    }

    @Test
    public void testSelectDuplicateAlias() throws SqlException {
        assertQuery(
                "select-choose x, x x1 from (select-choose [x] x from (select [x] from long_sequence(1)))",
                "select x x, x x from long_sequence(1)"
        );
    }

    @Test
    public void testSelectEndsWithSemicolon() throws Exception {
        assertQuery("select-choose x from (select [x] from x)",
                "select * from x;",
                modelOf("x").col("x", ColumnType.INT));
    }

    @Test
    public void testSelectFromNonCursorFunction() throws Exception {
        assertSyntaxError("select * from length('hello')", 14, "function must return CURSOR");
    }

    @Test
    public void testSelectFromSelectWildcardAndExpr() throws SqlException {
        assertQuery(
                "select-virtual column1 + x column from (select-virtual [x, x + y column1] x, y, x1, z, x + y column1 from (select-choose [tab1.x x, tab1.y y] tab1.x x, tab1.y y, tab2.x x1, tab2.z z from (select [x, y] from tab1 join select [x] from tab2 on tab2.x = tab1.x)))",
                "select column1 + x from (select *, tab1.x + y from tab1 join tab2 on (x))",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectFromSubQuery() throws SqlException {
        assertQuery("select-choose x from (select-choose [x] x, y from (select [x, y] from tab where y > 10)) a",
                "select a.x from (tab where y > 10) a",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSelectGroupByAndAnalytic() throws Exception {
        assertSyntaxError(
                "select sum(x), count() over() from tab",
                0,
                "Analytic function is not allowed",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testSelectGroupByArithmetic() throws SqlException {
        assertQuery("select-virtual sum + 10 column, sum1 from (select-group-by [sum(x) sum, sum(y) sum1] sum(x) sum, sum(y) sum1 from (select [x, y] from tab))",
                "select sum(x)+10, sum(y) from tab",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSelectGroupByArithmeticAndLimit() throws SqlException {
        assertQuery("select-virtual sum + 10 column, sum1 from (select-group-by [sum(x) sum, sum(y) sum1] sum(x) sum, sum(y) sum1 from (select [x, y] from tab)) limit 200",
                "select sum(x)+10, sum(y) from tab limit 200",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSelectGroupByUnion() throws SqlException {
        assertQuery("select-choose b from (select-group-by [sum(b) b, a] a, sum(b) b from (select [b, a] from trips) union all select-group-by [avg(d) b, c] c, avg(d) b from (select [d, c] from trips))",
                "select b from (select a, sum(b) b from trips union all select c, avg(d) b from trips)",
                modelOf("trips")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("tip_amount", ColumnType.DOUBLE)
                        .col("e", ColumnType.INT)
        );
    }

    @Test
    public void testSelectLatestByUnion() throws SqlException {
        assertQuery("select-choose b from (select-choose [b] a, b from (select [b, c] from trips latest by c) union all select-choose [d b] c, d b from (select [d, a] from trips latest by a))",
                "select b from (select a, b b from trips latest by c union all select c, d b from trips latest by a)",
                modelOf("trips")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("tip_amount", ColumnType.DOUBLE)
                        .col("e", ColumnType.INT)
        );
    }

    @Test
    public void testSelectMissingExpression() throws Exception {
        assertSyntaxError(
                "select ,a from tab",
                7,
                "missing expression"
        );
    }

    @Test
    public void testSelectMissingExpression2() throws Exception {
        assertSyntaxError(
                "select a, from tab",
                15,
                "column name expected"
        );
    }

    @Test
    public void testSelectNoFromUnion() throws SqlException {
        assertQuery(
                "select-group-by a, sum(b) sum from (select-virtual [1 a, 1 b] 1 a, 1 b from (long_sequence(1)) union all select-virtual 333 333, 1 1 from (long_sequence(1))) x",
                "select a, sum(b) from (select 1 a, 1 b union all select 333, 1) x"
        );
    }

    @Test
    public void testSelectNoWhere() throws SqlException {
        assertQuery(
                "select-virtual rnd_int(1,2,0) rnd_int from (long_sequence(1))",
                "select rnd_int(1, 2, 0)"
        );
    }

    @Test
    public void testSelectOnItsOwn() throws Exception {
        assertSyntaxError("select ", 7, "column expected");
    }

    @Test
    public void testSelectOrderByWhereOnCount() throws SqlException {
        assertQuery("select-choose a, c from (select-group-by [a, count() c] a, count() c from (select [a] from tab) where c > 0 order by a)",
                "(select a, count() c from tab order by 1) where c > 0",
                modelOf("tab").col("a", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testSelectPlainColumns() throws Exception {
        assertQuery(
                "select-choose a, b, c from (select [a, b, c] from t)",
                "select a,b,c from t",
                modelOf("t").col("a", ColumnType.INT).col("b", ColumnType.INT).col("c", ColumnType.INT)
        );
    }

    @Test
    public void testSelectSelectColumn() throws Exception {
        assertSyntaxError(
                "select a, select from tab",
                17,
                "reserved name"
        );
    }

    @Test
    public void testSelectSingleExpression() throws Exception {
        assertQuery(
                "select-virtual a + b * c x from (select [a, c, b] from t)",
                "select a+b*c x from t",
                modelOf("t").col("a", ColumnType.INT).col("b", ColumnType.INT).col("c", ColumnType.INT));
    }

    @Test
    public void testSelectSumFromSubQueryLimit() throws SqlException {
        assertQuery("select-group-by sum(tip_amount) sum from (select-choose [tip_amount] a, b, c, d, tip_amount, e from (select [tip_amount] from trips) limit 100000000)",
                "select sum(tip_amount) from (trips limit 100000000)",
                modelOf("trips")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("d", ColumnType.INT)
                        .col("tip_amount", ColumnType.DOUBLE)
                        .col("e", ColumnType.INT)
        );
    }

    @Test
    public void testSelectSumSquared() throws SqlException {
        assertQuery(
                "select-virtual x, sum * sum x1 from (select-group-by [x, sum(x) sum] x, sum(x) sum from (select [x] from long_sequence(2)))",
                "select x, sum(x)*sum(x) x from long_sequence(2)"
        );
    }

    @Test
    public void testSelectVirtualAliasClash() throws SqlException {
        assertQuery(
                "select-virtual x + x x, x x1 from (select [x] from long_sequence(1) z) z",
                "select x+x x, x from long_sequence(1) z"
        );
    }

    @Test
    public void testSelectWhereOnCount() throws SqlException {
        assertQuery("select-choose a, c from (select-group-by [a, count() c] a, count() c from (select [a] from tab) where c > 0)",
                "(select a, count() c from tab) where c > 0",
                modelOf("tab").col("a", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testSelectWildcard() throws SqlException {
        assertQuery(
                "select-choose tab1.x x, tab1.y y, tab2.x x1, tab2.z z from (select [x, y] from tab1 join select [x, z] from tab2 on tab2.x = tab1.x)",
                "select * from tab1 join tab2 on (x)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardAndExpr() throws SqlException {
        assertQuery(
                "select-virtual x, y, x1, z, x + y column1 from (select-choose [tab1.x x, tab1.y y, tab2.x x1, tab2.z z] tab1.x x, tab1.y y, tab2.x x1, tab2.z z from (select [x, y] from tab1 join select [x, z] from tab2 on tab2.x = tab1.x))",
                "select *, tab1.x + y from tab1 join tab2 on (x)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardAndNotTimestamp() throws Exception {
        assertSyntaxError(
                "select * from (select x from tab1) timestamp(y)",
                45,
                "Invalid column",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSelectWildcardAndTimestamp() throws SqlException {
        assertQuery(
                "select-choose x, y from (select-choose [y, x] x, y from (select [y, x] from tab1)) timestamp (y)",
                "select * from (select x, y from tab1) timestamp(y)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSelectWildcardDetachedStar() throws Exception {
        assertSyntaxError(
                "select tab2.*, bxx.  * from tab1 a join tab2 on (x)",
                33,
                "',', 'from' or 'over' expected",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardInvalidTableAlias() throws Exception {
        assertSyntaxError(
                "select tab2.*, b.* from tab1 a join tab2 on (x)",
                15,
                "invalid table alias",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardMissingStar() throws Exception {
        assertSyntaxError(
                "select tab2.*, a. from tab1 a join tab2 on (x)",
                17,
                "'*' or column name expected",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardPrefixed() throws SqlException {
        assertQuery(
                "select-choose tab2.x x, tab2.z z, tab1.x x1, tab1.y y from (select [x, y] from tab1 join select [x, z] from tab2 on tab2.x = tab1.x)",
                "select tab2.*, tab1.* from tab1 join tab2 on (x)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSelectWildcardPrefixed2() throws SqlException {
        assertQuery("select-choose tab2.x x, tab2.z z, a.x x1, a.y y from (select [x, y] from tab1 a join select [x, z] from tab2 on tab2.x = a.x) a",
                "select tab2.*, a.* from tab1 a join tab2 on (x)",
                modelOf("tab1").col("x", ColumnType.INT).col("y", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSimpleCaseExpression() throws SqlException {
        assertQuery(
                "select-virtual switch(a,1,'A',2,'B','C') + 1 column, b from (select [a, b] from tab)",
                "select case a when 1 then 'A' when 2 then 'B' else 'C' end + 1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testSimpleCaseExpressionAsConstant() throws SqlException {
        assertQuery(
                "select-virtual switch(1,1,'A',2,'B','C') + 1 column, b from (select [b] from tab)",
                "select case 1 when 1 then 'A' when 2 then 'B' else 'C' end + 1, b from tab",
                modelOf("tab").col("a", ColumnType.INT).col("b", ColumnType.INT)
        );
    }

    @Test
    public void testSimpleSubQuery() throws Exception {
        assertQuery(
                "select-choose y from (select-choose [y] y from (select [y] from x where y > 1))",
                "(x) where y > 1",
                modelOf("x").col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSingleTableLimit() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y, z] from tab where x > z) limit 100",
                "select x x, y y from tab where x > z limit 100",
                modelOf("tab")
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSingleTableLimitLoHi() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y, z] from tab where x > z) limit 100,200",
                "select x x, y y from tab where x > z limit 100,200",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSingleTableLimitLoHiExtraToken() throws Exception {
        assertSyntaxError("select x x, y y from tab where x > z limit 100,200 b", 51, "unexpected");
    }

    @Test
    public void testSingleTableNoWhereLimit() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y] from tab) limit 100",
                "select x x, y y from tab limit 100",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT));
    }

    @Test
    public void testSpliceJoin() throws SqlException {
        assertQuery("select-choose t.timestamp timestamp, t.tag tag, q.timestamp timestamp1 from (select [timestamp, tag] from trades t timestamp (timestamp) splice join select [timestamp] from quotes q timestamp (timestamp) where tag = null) t",
                "trades t splice join quotes q where tag = null",
                modelOf("trades").timestamp().col("tag", ColumnType.SYMBOL),
                modelOf("quotes").timestamp()
        );
    }

    @Test
    public void testSpliceJoinColumnAliasNull() throws SqlException {
        assertQuery(
                "select-choose customerId, kk, count from (select-group-by [customerId, kk, count() count] customerId, kk, count() count from (select-choose [c.customerId customerId, o.customerId kk] c.customerId customerId, o.customerId kk from (select [customerId] from customers c splice join select [customerId] from orders o on o.customerId = c.customerId post-join-where o.customerId = null) c) c) limit 10",
                "(select c.customerId, o.customerId kk, count() from customers c" +
                        " splice join orders o on c.customerId = o.customerId) " +
                        " where kk = null limit 10",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testSpliceJoinNullFilter() throws SqlException {
        assertQuery("select-choose t.timestamp timestamp, t.tag tag, q.x x, q.timestamp timestamp1 from (select [timestamp, tag] from trades t timestamp (timestamp) splice join select [x, timestamp] from quotes q timestamp (timestamp) post-join-where x = null) t",
                "trades t splice join quotes q where x = null",
                modelOf("trades").timestamp().col("tag", ColumnType.SYMBOL),
                modelOf("quotes").col("x", ColumnType.SYMBOL).timestamp()
        );
    }

    @Test
    public void testSpliceJoinOrder() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, e.employeeId employeeId, o.customerId customerId1 from (select [customerId] from customers c splice join select [employeeId] from employees e on e.employeeId = c.customerId join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " splice join employees e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId",
                modelOf("customers").col("customerId", ColumnType.SYMBOL),
                modelOf("employees").col("employeeId", ColumnType.STRING),
                modelOf("orders").col("customerId", ColumnType.SYMBOL));
    }

    @Test
    public void testSpliceJoinSubQuery() throws Exception {
        // execution order must be (src: SQL Server)
        //        1. FROM
        //        2. ON
        //        3. JOIN
        //        4. WHERE
        //        5. GROUP BY
        //        6. WITH CUBE or WITH ROLLUP
        //        7. HAVING
        //        8. SELECT
        //        9. DISTINCT
        //        10. ORDER BY
        //        11. TOP
        //
        // which means "where" clause for "e" table has to be explicitly as post-join-where
        assertQuery(
                "select-choose c.customerId customerId, e.blah blah, e.lastName lastName, e.employeeId employeeId, e.timestamp timestamp, o.customerId customerId1 from (select [customerId] from customers c splice join select [blah, lastName, employeeId, timestamp] from (select-virtual ['1' blah, lastName, employeeId, timestamp] '1' blah, lastName, employeeId, timestamp from (select [lastName, employeeId, timestamp] from employees) order by lastName) e on e.employeeId = c.customerId post-join-where e.lastName = 'x' and e.blah = 'y' join select [customerId] from orders o on o.customerId = c.customerId) c",
                "customers c" +
                        " splice join (select '1' blah, lastName, employeeId, timestamp from employees order by lastName) e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId where e.lastName = 'x' and e.blah = 'y'",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP),
                modelOf("orders")
                        .col("customerId", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testSpliceJoinSubQuerySimpleAlias() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, a.blah blah, a.lastName lastName, a.customerId customerId1, a.timestamp timestamp from (select [customerId] from customers c splice join select [blah, lastName, customerId, timestamp] from (select-virtual ['1' blah, lastName, customerId, timestamp] '1' blah, lastName, customerId, timestamp from (select-choose [lastName, employeeId customerId, timestamp] lastName, employeeId customerId, timestamp from (select [lastName, employeeId, timestamp] from employees)) order by lastName) a on a.customerId = c.customerId) c",
                "customers c" +
                        " splice join (select '1' blah, lastName, employeeId customerId, timestamp from employees order by lastName) a on (customerId)",
                modelOf("customers")
                        .col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSpliceJoinSubQuerySimpleNoAlias() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, _xQdbA0.blah blah, _xQdbA0.lastName lastName, _xQdbA0.customerId customerId1, _xQdbA0.timestamp timestamp from (select [customerId] from customers c splice join select [blah, lastName, customerId, timestamp] from (select-virtual ['1' blah, lastName, customerId, timestamp] '1' blah, lastName, customerId, timestamp from (select-choose [lastName, employeeId customerId, timestamp] lastName, employeeId customerId, timestamp from (select [lastName, employeeId, timestamp] from employees)) order by lastName) _xQdbA0 on _xQdbA0.customerId = c.customerId) c",
                "customers c" +
                        " splice join (select '1' blah, lastName, employeeId customerId, timestamp from employees order by lastName) on (customerId)",
                modelOf("customers").col("customerId", ColumnType.SYMBOL),
                modelOf("employees")
                        .col("employeeId", ColumnType.STRING)
                        .col("lastName", ColumnType.STRING)
                        .col("timestamp", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testSubQuery() throws Exception {
        assertQuery(
                "select-choose x, y from (select-choose [x, y] x, y from (select [y, x] from tab t2 latest by x where x > 100) t2 where y > 0) t1",
                "select x, y from (select x, y from tab t2 latest by x where x > 100) t1 where y > 0",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testSubQueryAliasWithSpace() throws Exception {
        assertQuery(
                "select-choose x, a from (select-choose [x, a] x, a from (select [x, a] from x where a > 1 and x > 1)) 'b a'",
                "(x where a > 1) 'b a' where x > 1",
                modelOf("x")
                        .col("x", ColumnType.INT)
                        .col("a", ColumnType.INT));
    }

    @Test
    public void testSubQueryAsArg() throws Exception {
        assertQuery(
                "select-choose customerId from (select [customerId] from customers where (select-choose orderId from (select [orderId] from orders)) > 1)",
                "select * from customers where (select * from orders) > 1",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT)
        );
    }

    @Test
    public void testSubQueryKeepOrderBy() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x from (select [x] from a) order by x)",
                "select x from (select * from a order by x)",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSubQueryLimitLoHi() throws Exception {
        assertQuery(
                "select-choose x, y from (select [x, y] from (select-choose [y, x] x, y from (select [y, x, z] from tab where x > z) limit 100,200) _xQdbA1 where x = y) limit 150",
                "(select x x, y y from tab where x > z limit 100,200) where x = y limit 150",
                modelOf("tab").col("x", ColumnType.INT).col("y", ColumnType.INT).col("z", ColumnType.INT)
        );
    }

    @Test
    public void testSubQuerySyntaxError() throws Exception {
        assertSyntaxError("select x from (select tab. tab where x > 10 t1)", 26, "'*' or column name expected");
    }

    @Test
    public void testTableListToCrossJoin() throws Exception {
        assertQuery("select-choose a.x x from (select [x] from a a join select [x] from c on c.x = a.x) a",
                "select a.x from a a, c where a.x = c.x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("x", ColumnType.INT),
                modelOf("c").col("x", ColumnType.INT));
    }

    @Test
    public void testTableNameAsArithmetic() throws Exception {
        assertSyntaxError(
                "select x from 'tab' + 1",
                20,
                "function, literal or constant is expected",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testTableNameCannotOpen() throws Exception {
        final FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };

        assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                TableModel[] tableModels = new TableModel[]{modelOf("tab").col("x", ColumnType.INT)};
                try {
                    try {
                        for (int i = 0, n = tableModels.length; i < n; i++) {
                            CairoTestUtils.create(tableModels[i]);
                        }
                        compiler.compile("select * from tab", sqlExecutionContext);
                        Assert.fail("Exception expected");
                    } catch (SqlException e) {
                        Assert.assertEquals(14, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                    }
                } finally {
                    for (int i = 0, n = tableModels.length; i < n; i++) {
                        TableModel tableModel = tableModels[i];
                        Path path = tableModel.getPath().of(tableModel.getCairoCfg().getRoot()).concat(tableModel.getName()).slash$();
                        Assert.assertEquals(0, configuration.getFilesFacade().rmdir(path));
                        tableModel.close();
                    }
                }
            }
        });
    }

    @Test
    public void testTableNameJustNoRowidMarker() throws Exception {
        assertSyntaxError(
                "select * from '*!*'",
                14,
                "come on"
        );
    }

    @Test
    public void testTableNameLocked() throws Exception {
        assertMemoryLeak(() -> {
            CharSequence lockedReason = engine.lock(AllowAllCairoSecurityContext.INSTANCE, "tab", "testing");
            Assert.assertNull(lockedReason);
            try {
                TableModel[] tableModels = new TableModel[]{modelOf("tab").col("x", ColumnType.INT)};
                try {
                    try {
                        for (int i = 0, n = tableModels.length; i < n; i++) {
                            CairoTestUtils.create(tableModels[i]);
                        }
                        compiler.compile("select * from tab", sqlExecutionContext);
                        Assert.fail("Exception expected");
                    } catch (SqlException e) {
                        Assert.assertEquals(14, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "table is locked");
                    }
                } finally {
                    for (int i = 0, n = tableModels.length; i < n; i++) {
                        TableModel tableModel = tableModels[i];
                        Path path = tableModel.getPath().of(tableModel.getCairoCfg().getRoot()).concat(tableModel.getName()).slash$();
                        Assert.assertEquals(0, configuration.getFilesFacade().rmdir(path));
                        tableModel.close();
                    }
                }
            } finally {
                engine.unlock(AllowAllCairoSecurityContext.INSTANCE, "tab", null, false);
            }
        });
    }

    @Test
    public void testTableNameReserved() throws Exception {
        try (Path path = new Path()) {
            configuration.getFilesFacade().touch(path.of(root).concat("tab").$());
        }

        assertSyntaxError(
                "select * from tab",
                14,
                "table directory is of unknown format"
        );
    }

    @Test
    public void testTableNameWithNoRowidMarker() throws SqlException {
        assertQuery(
                "select-choose x from (select [x] from *!*tab)",
                "select * from '*!*tab'",
                modelOf("tab").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testTimestampOnSubQuery() throws Exception {
        assertQuery("select-choose x from (select-choose [x] x, y from (select [x, y] from a b where x > y) b) timestamp (x)",
                "select x from (a b) timestamp(x) where x > y",
                modelOf("a").col("x", ColumnType.INT).col("y", ColumnType.INT));
    }

    @Test
    public void testTimestampOnTable() throws Exception {
        assertQuery(
                "select-choose x from (select [x, y] from a b timestamp (x) where x > y) b",
                "select x from a b timestamp(x) where x > y",
                modelOf("a")
                        .col("x", ColumnType.TIMESTAMP)
                        .col("y", ColumnType.TIMESTAMP));
    }

    @Test
    public void testTooManyColumnsEdgeInOrderBy() throws Exception {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
            for (int i = 0; i < SqlParser.MAX_ORDER_BY_COLUMNS - 1; i++) {
                model.col("f" + i, ColumnType.INT);
            }
            CairoTestUtils.create(model);
        }

        StringBuilder b = new StringBuilder();
        b.append("x order by ");
        for (int i = 0; i < SqlParser.MAX_ORDER_BY_COLUMNS - 1; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append('f').append(i);
        }
        QueryModel st = (QueryModel) compiler.testCompileModel(b, sqlExecutionContext);
        Assert.assertEquals(SqlParser.MAX_ORDER_BY_COLUMNS - 1, st.getOrderBy().size());
    }

    @Test
    public void testTooManyColumnsInOrderBy() {
        StringBuilder b = new StringBuilder();
        b.append("x order by ");
        for (int i = 0; i < SqlParser.MAX_ORDER_BY_COLUMNS; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append('f').append(i);
        }
        try {
            compiler.compile(b, sqlExecutionContext);
        } catch (SqlException e) {
            TestUtils.assertEquals("Too many columns", e.getFlyweightMessage());
        }
    }

    @Test
    public void testTwoAnalyticColumns() throws Exception {
        assertQuery(
                "select-analytic a, b, f(c) my over (partition by b order by ts), d(c) d over () from (select [a, b, c, ts] from xyz timestamp (ts))",
                "select a,b, f(c) over (partition by b order by ts) my, d(c) over() from xyz",
                modelOf("xyz")
                        .col("c", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("a", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testUnbalancedBracketInSubQuery() throws Exception {
        assertSyntaxError("select x from (tab where x > 10 t1", 32, "expected");
    }

    @Test
    public void testUndefinedBindVariables() throws SqlException {
        assertQuery(
                "select-virtual $1 + 1 column, $2 $2, $3 $3 from (long_sequence(10))",
                "select $1+1, $2, $3 from long_sequence(10)"
        );
    }

    @Test
    public void testUnderTerminatedOver() throws Exception {
        assertSyntaxError("select a,b, f(c) over (partition by b order by ts from xyz", 50, "expected");
    }

    @Test
    public void testUnderTerminatedOver2() throws Exception {
        assertSyntaxError("select a,b, f(c) over (partition by b order by ts", 49, "'asc' or 'desc' expected");
    }

    @Test
    public void testUnexpectedTokenInAnalyticFunction() throws Exception {
        assertSyntaxError("select a,b, f(c) over (by b order by ts) from xyz", 23, "expected");
    }

    @Test
    public void testUnion() throws SqlException {
        assertQuery("select-choose x from (select [x] from a) union select-choose y from (select [y] from b) union select-choose z from (select [z] from c)",
                "select * from a union select * from b union select * from c",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionAllInSubQuery() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x from (select [x] from a) union select-choose y from (select [y] from b) union all select-choose z from (select [z] from c))",
                "select x from (select * from a union select * from b union all select * from c)",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionColumnMisSelection() throws SqlException {
        assertQuery(
                "select-virtual ts, futures_price, 0.0 spot_price from (select-group-by [ts, avg(bid_price) futures_price] ts, avg(bid_price) futures_price from (select [ts, bid_price, market_type] from market_updates timestamp (ts) where market_type = 'futures') sample by 1m) union all select-virtual ts, 0.0 futures_price, spot_price from (select-group-by [ts, avg(bid_price) spot_price] ts, avg(bid_price) spot_price from (select [ts, bid_price, market_type] from market_updates timestamp (ts) where market_type = 'spot') sample by 1m)",
                "select \n" +
                        "    ts, \n" +
                        "    avg(bid_price) AS futures_price, \n" +
                        "    0.0 AS spot_price \n" +
                        "FROM market_updates \n" +
                        "WHERE market_type = 'futures' \n" +
                        "SAMPLE BY 1m \n" +
                        "UNION ALL\n" +
                        "SELECT \n" +
                        "    ts, \n" +
                        "    0.0 AS futures_price, \n" +
                        "    avg(bid_price) AS spot_price\n" +
                        "FROM market_updates\n" +
                        "WHERE market_type = 'spot' \n" +
                        "SAMPLE BY 1m",
                modelOf("market_updates")
                        .col("bid_price", ColumnType.DOUBLE)
                        .col("market_type", ColumnType.SYMBOL)
                        .timestamp("ts")
        );
    }

    @Test
    public void testUnionDifferentColumnCount() throws Exception {
        assertSyntaxError(
                "select x from (select * from a union select * from b union all select sum(z) from (c order by t) timestamp(t) sample by 6h) order by x",
                63,
                "queries have different number of columns",
                modelOf("a").col("x", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("b").col("y", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("c").col("z", ColumnType.INT).col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testUnionJoinReorder3() throws Exception {
        assertQuery(
                "select-virtual 1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8 from (long_sequence(1)) union select-choose orders.orderId orderId, customers.customerId customerId, shippers.shipper shipper, d.orderId orderId1, d.productId productId, suppliers.supplier supplier, products.productId productId1, products.supplier supplier1 from (select [orderId] from orders join select [shipper] from shippers on shippers.shipper = orders.orderId join (select [orderId, productId] from orderDetails d where productId = orderId) d on d.productId = shippers.shipper join select [productId, supplier] from products on products.productId = d.productId join select [supplier] from suppliers on suppliers.supplier = products.supplier cross join select [customerId] from customers const-where 1 = 1)",
                "select 1, 2, 3, 4, 5, 6, 7, 8 from long_sequence(1)" +
                        " union " +
                        "orders" +
                        " outer join customers on 1=1" +
                        " join shippers on shippers.shipper = orders.orderId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = shippers.shipper" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId" +
                        " where d.productId = d.orderId",
                modelOf("orders").col("orderId", ColumnType.INT),
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orderDetails").col("orderId", ColumnType.INT).col("productId", ColumnType.INT),
                modelOf("products").col("productId", ColumnType.INT).col("supplier", ColumnType.INT),
                modelOf("suppliers").col("supplier", ColumnType.INT),
                modelOf("shippers").col("shipper", ColumnType.INT)
        );
    }

    @Test
    public void testUnionKeepOrderBy() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x from (select [x] from a) union select-choose y from (select [y] from b) union all select-choose z from (select [z] from c order by z))",
                "select x from (select * from a union select * from b union all select * from c order by z)",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionKeepOrderByIndex() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x from (select [x] from a) union select-choose y from (select [y] from b) union all select-choose z from (select [z] from c order by z))",
                "select x from (select * from a union select * from b union all select * from c order by 1)",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionKeepOrderByWhenSampleByPresent() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x, t from (select [x] from a) union select-choose y, t from (select [y, t] from b) union all select-virtual 'a' k, sum from (select-group-by [sum(z) sum] sum(z) sum from (select-choose [t, z] z, t from (select [t, z] from c order by t)) timestamp (t) sample by 6h)) order by x",
                "select x from (select * from a union select * from b union all select 'a' k, sum(z) from (c order by t) timestamp(t) sample by 6h) order by x",
                modelOf("a").col("x", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("b").col("y", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("c").col("z", ColumnType.INT).col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testUnionMoveWhereIntoSubQuery() throws Exception {
        assertQuery(
                "select-virtual 1 1, 2 2, 3 3 from (long_sequence(1)) union select-choose c.customerId customerId, o.customerId customerId1, o.x x from (select [customerId] from customers c outer join select [customerId, x] from (select-choose [customerId, x] customerId, x from (select [customerId, x] from orders o where x = 10 and customerId = 100) o) o on customerId = c.customerId where customerId = 100) c",
                "select 1, 2, 3 from long_sequence(1)" +
                        " union " +
                        "customers c" +
                        " outer join (orders o where o.x = 10) o on c.customerId = o.customerId" +
                        " where c.customerId = 100",
                modelOf("customers").col("customerId", ColumnType.INT),
                modelOf("orders")
                        .col("customerId", ColumnType.INT)
                        .col("x", ColumnType.INT)
        );
    }

    @Test
    public void testUnionRemoveOrderBy() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x from (select [x] from a) union select-choose y from (select [y] from b) union all select-choose z from (select [z] from c)) order by x",
                "select x from (select * from a union select * from b union all select * from c order by z) order by x",
                modelOf("a").col("x", ColumnType.INT),
                modelOf("b").col("y", ColumnType.INT),
                modelOf("c").col("z", ColumnType.INT)
        );
    }

    @Test
    public void testUnionRemoveRedundantOrderBy() throws SqlException {
        assertQuery(
                "select-choose x from (select-choose [x] x, t from (select [x] from a) union select-choose y, t from (select [y, t] from b) union all select-virtual 1 1, sum from (select-group-by [sum(z) sum] sum(z) sum from (select-choose [t, z] z, t from (select [t, z] from c order by t)) timestamp (t) sample by 6h)) order by x",
                "select x from (select * from a union select * from b union all select 1, sum(z) from (c order by t, t) timestamp(t) sample by 6h) order by x",
                modelOf("a").col("x", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("b").col("y", ColumnType.INT).col("t", ColumnType.TIMESTAMP),
                modelOf("c").col("z", ColumnType.INT).col("t", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testUtfStringConstants() throws SqlException {
        assertQuery(
                "select-virtual rnd_str('Raphaël','Léo') rnd_str from (long_sequence(200))",
                "SELECT rnd_str('Raphaël','Léo') FROM long_sequence(200);"
        );
    }

    @Test
    public void testWhereClause() throws Exception {
        assertQuery(
                "select-virtual x, sum + 25 ohoh from (select-group-by [x, sum(z) sum] x, sum(z) sum from (select-virtual [a + b * c x, z] a + b * c x, z from (select [a, c, b, z] from zyzy where a in (0,10) and b = 10)))",
                "select a+b*c x, sum(z)+25 ohoh from zyzy where a in (0,10) AND b = 10",
                modelOf("zyzy")
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .col("c", ColumnType.INT)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .col("z", ColumnType.INT)
        );
    }

    @Test
    public void testWhereClauseInsideInSubQuery() throws SqlException {
        assertQuery(
                "select-choose ts, sym, bid, ask from (select [ts, sym, bid, ask] from trades timestamp (ts) where ts = '2010-01-04' and sym in (select-choose sym from (select [sym, isNewSymbol] from symbols where not(isNewSymbol))))",
                "select * from trades where ts='2010-01-04' and sym in (select sym from symbols where NOT isNewSymbol);",
                modelOf("trades")
                        .timestamp("ts")
                        .col("sym", ColumnType.SYMBOL)
                        .col("bid", ColumnType.DOUBLE)
                        .col("ask", ColumnType.DOUBLE),
                modelOf("symbols")
                        .col("sym", ColumnType.SYMBOL)
                        .col("isNewSymbol", ColumnType.BOOLEAN)
        );
    }

    @Test
    public void testWhereClauseWithInStatement() throws SqlException {
        assertQuery(
                "select-choose sym, bid, ask, ts from (select [sym, bid, ask, ts] from x timestamp (ts) where sym in ('AA','BB')) order by ts desc",
                "select * from x where sym in ('AA', 'BB' ) order by ts desc",
                modelOf("x")
                        .col("sym", ColumnType.SYMBOL)
                        .col("bid", ColumnType.INT)
                        .col("ask", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testWhereNotSelectedColumn() throws Exception {
        assertQuery(
                "select-choose c.customerId customerId, c.weight weight, o.customerId customerId1, o.x x from (select [customerId, weight] from customers c outer join select [customerId, x] from (select-choose [customerId, x] customerId, x from (select [customerId, x] from orders o where x = 10) o) o on o.customerId = c.customerId where weight = 100) c",
                "customers c" +
                        " outer join (orders o where o.x = 10) o on c.customerId = o.customerId" +
                        " where c.weight = 100",
                modelOf("customers").col("customerId", ColumnType.INT).col("weight", ColumnType.DOUBLE),
                modelOf("orders")
                        .col("customerId", ColumnType.INT)
                        .col("x", ColumnType.INT)
        );
    }

    @Test
    public void testWithDuplicateName() throws Exception {
        assertSyntaxError(
                "with x as (tab), x as (tab2) x",
                17,
                "duplicate name",
                modelOf("tab").col("x", ColumnType.INT),
                modelOf("tab2").col("x", ColumnType.INT)
        );
    }

    @Test
    public void testWithRecursive() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select-choose [a] a from (select [a] from tab)) x) y",
                "with x as (select * from tab)," +
                        " y as (select * from x)" +
                        " select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithSelectFrom() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select [a] from tab)) x",
                "with x as (" +
                        " select a from tab" +
                        ") select a from x",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithSelectFrom2() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select [a] from tab)) x",
                "with x as (" +
                        " select a from tab" +
                        ") x",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithSubquery() throws SqlException {
        assertQuery(
                "select-choose 1 from (select-virtual [1 1] 1 1 from ((select-choose a from (select [a] from tab)) x cross join (select-choose a from (select-choose [a] a from (select [a] from tab)) x) y) x)",
                "with x as (select * from tab)," +
                        " y as (select * from x)" +
                        " select * from (select 1 from x cross join y)",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithSyntaxError() throws Exception {
        assertSyntaxError(
                "with x as (" +
                        " select ,a from tab" +
                        ") x",
                19,
                "missing expression",
                modelOf("tab").col("a", ColumnType.INT)

        );
    }

    @Test
    public void testWithTwoAliasesExcept() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select [a] from tab)) x except select-choose a from (select-choose [a] a from (select [a] from tab)) y",
                "with x as (select * from tab)," +
                        " y as (select * from tab)" +
                        " select * from x except select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithTwoAliasesIntersect() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select [a] from tab)) x intersect select-choose a from (select-choose [a] a from (select [a] from tab)) y",
                "with x as (select * from tab)," +
                        " y as (select * from tab)" +
                        " select * from x intersect select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithTwoAliasesUnion() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select [a] from tab)) x union select-choose a from (select-choose [a] a from (select [a] from tab)) y",
                "with x as (select * from tab)," +
                        " y as (select * from tab)" +
                        " select * from x union select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testWithUnionWith() throws SqlException {
        assertQuery(
                "select-choose a from (select-choose [a] a from (select [a] from tab)) x union select-choose a from (select-choose [a] a from (select [a] from tab)) y",
                "with x as (select * from tab) select * from x union with " +
                        " y as (select * from tab) select * from y",
                modelOf("tab").col("a", ColumnType.INT)
        );
    }

    @Test
    public void testTimestampWithTimezoneCast() throws Exception {
        assertQuery("select-virtual cast(t,timestamp) cast from (select [t] from x)",
                "select cast(t as timestamp with time zone) from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testTimestampWithTimezoneCastInSelect() throws Exception {
        assertQuery("select-virtual cast('2005-04-02 12:00:00-07',timestamp) col from (x)",
                "select cast('2005-04-02 12:00:00-07' as timestamp with time zone) col from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testTimestampWithTimezoneConstPrefix() throws Exception {
        assertQuery("select-choose t, tt from (select [t, tt] from x where t > cast('2005-04-02 12:00:00-07',timestamp))",
                "select * from x where t > timestamp with time zone '2005-04-02 12:00:00-07'",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testTimestampWithTimezoneConstPrefixInsideCast() throws Exception {
        assertQuery("select-choose t, tt from (select [t, tt] from x where t > cast(cast('2005-04-02 12:00:00-07',timestamp),DATE))",
                "select * from x where t > CAST(timestamp with time zone '2005-04-02 12:00:00-07' as DATE)",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testTimestampWithTimezoneConstPrefixInInsert() throws Exception {
        assertInsertQuery(
                modelOf("test").col("test_timestamp", ColumnType.TIMESTAMP).col("test_value", ColumnType.STRING));
    }

    @Test
    public void testTimestampWithTimezoneConstPrefixInSelect() throws Exception {
        assertQuery("select-virtual cast('2005-04-02 12:00:00-07',timestamp) alias from (x)",
                "select timestamp with time zone '2005-04-02 12:00:00-07' \"alias\" from x",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testInvalidTypeLiteralCast() throws Exception {
        assertSyntaxError(
                "select * from x where t > timestamp_with_time_zone '2005-04-02 12:00:00-07'",
                26,
                "invalid type",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    @Test
    public void testInvalidTypeCast() throws Exception {
        assertSyntaxError(
                "select cast('2005-04-02 12:00:00-07' as timestamp with time z) col from x",
                11,
                "unbalanced (",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }


    @Test
    public void testInvalidTypeCast2() throws Exception {
        assertSyntaxError(
                "select cast('2005-04-02 12:00:00-07' as timestamp with tz) col from x",
                11,
                "unbalanced (",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP)
        );
    }

    private static void assertSyntaxError(
            SqlCompiler compiler,
            String query,
            int position,
            String contains,
            TableModel... tableModels
    ) throws Exception {
        try {
            assertMemoryLeak(() -> {
                try {
                    for (int i = 0, n = tableModels.length; i < n; i++) {
                        CairoTestUtils.create(tableModels[i]);
                    }
                    compiler.compile(query, sqlExecutionContext);
                    Assert.fail("Exception expected");
                } catch (SqlException e) {
                    Assert.assertEquals(position, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), contains);
                }
            });
        } finally {
            for (int i = 0, n = tableModels.length; i < n; i++) {
                TableModel tableModel = tableModels[i];
                Path path = tableModel.getPath().of(tableModel.getCairoCfg().getRoot()).concat(tableModel.getName()).slash$();
                Assert.assertEquals(0, configuration.getFilesFacade().rmdir(path));
                tableModel.close();
            }
        }
    }

    private static void assertSyntaxError(String query, int position, String contains, TableModel... tableModels) throws Exception {
        assertSyntaxError(compiler, query, position, contains, tableModels);
    }

    private static void checkLiteralIsInSet(
            ExpressionNode node,
            ObjList<LowerCaseCharSequenceHashSet> nameSets,
            LowerCaseCharSequenceIntHashMap modelAliasSet
    ) {
        if (node.type == ExpressionNode.LITERAL) {
            final CharSequence tok = node.token;
            final int dot = Chars.indexOf(tok, '.');
            if (dot == -1) {
                boolean found = false;
                for (int i = 0, n = nameSets.size(); i < n; i++) {
                    boolean f = nameSets.getQuick(i).contains(tok);
                    if (f) {
                        Assert.assertFalse(found);
                        found = true;
                    }
                }
                if (!found) {
                    Assert.fail("column: " + tok);
                }
            } else {
                int index = modelAliasSet.keyIndex(tok, 0, dot);
                Assert.assertTrue(index < 0);
                LowerCaseCharSequenceHashSet set = nameSets.getQuick(modelAliasSet.valueAt(index));
                Assert.assertFalse(set.excludes(tok, dot + 1, tok.length()));
            }
        } else {
            if (node.paramCount < 3) {
                if (node.lhs != null) {
                    checkLiteralIsInSet(node.lhs, nameSets, modelAliasSet);
                }

                if (node.rhs != null) {
                    checkLiteralIsInSet(node.rhs, nameSets, modelAliasSet);
                }
            } else {
                for (int j = 0, k = node.args.size(); j < k; j++) {
                    checkLiteralIsInSet(node.args.getQuick(j), nameSets, modelAliasSet);
                }
            }
        }
    }

    private void assertColumnNames(String query, String... columns) throws SqlException {
        assertColumnNames(compiler, query, columns);
    }

    private void assertColumnNames(SqlCompiler compiler, String query, String... columns) throws SqlException {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        RecordMetadata metadata = cc.getRecordCursorFactory().getMetadata();

        for (int idx = 0; idx < columns.length; idx++) {
            TestUtils.assertEquals(metadata.getColumnName(idx), columns[idx]);
        }
    }

    private void assertCreateTable(String expected, String ddl, TableModel... tableModels) throws SqlException {
        assertModel(expected, ddl, ExecutionModel.CREATE_TABLE, tableModels);
    }

    private void assertModel(String expected, String query, int modelType, TableModel... tableModels) throws SqlException {
        createModelsAndRun(() -> {
            sink.clear();
            ExecutionModel model = compiler.testCompileModel(query, sqlExecutionContext);
            Assert.assertEquals(model.getModelType(), modelType);
            ((Sinkable) model).toSink(sink);
            if (model instanceof QueryModel) {
                validateTopDownColumns((QueryModel) model);
            }
            TestUtils.assertEquals(expected, sink);
        }, tableModels);
    }

    private void assertQuery(String expected, String query, TableModel... tableModels) throws SqlException {
        assertModel(expected, query, ExecutionModel.QUERY, tableModels);
    }

    private void assertInsertQuery(TableModel... tableModels) throws SqlException {
        assertModel(
                "insert into test (test_timestamp, test_value) values (cast('2020-12-31 15:15:51.663+00:00',timestamp), '256')",
                "insert into test (test_timestamp, test_value) values (timestamp with time zone '2020-12-31 15:15:51.663+00:00', '256')",
                ExecutionModel.INSERT,
                tableModels
        );
    }

    private void createModelsAndRun(CairoAware runnable, TableModel... tableModels) throws SqlException {
        try {
            for (int i = 0, n = tableModels.length; i < n; i++) {
                CairoTestUtils.create(tableModels[i]);
            }
            runnable.run();
        } finally {
            Assert.assertTrue(engine.releaseAllReaders());
            for (int i = 0, n = tableModels.length; i < n; i++) {
                TableModel tableModel = tableModels[i];
                Path path = tableModel.getPath().of(tableModel.getCairoCfg().getRoot()).concat(tableModel.getName()).slash$();
                Assert.assertEquals(0, configuration.getFilesFacade().rmdir(path));
                tableModel.close();
            }
        }
    }

    private TableModel modelOf(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.NONE);
    }

    private void validateTopDownColumns(QueryModel model) {
        ObjList<QueryColumn> columns = model.getColumns();
        final ObjList<LowerCaseCharSequenceHashSet> nameSets = new ObjList<>();

        QueryModel nested = model.getNestedModel();
        while (nested != null) {
            nameSets.clear();

            for (int i = 0, n = nested.getJoinModels().size(); i < n; i++) {
                LowerCaseCharSequenceHashSet set = new LowerCaseCharSequenceHashSet();
                final QueryModel m = nested.getJoinModels().getQuick(i);
                final ObjList<QueryColumn> cols = m.getTopDownColumns();
                for (int j = 0, k = cols.size(); j < k; j++) {
                    QueryColumn qc = cols.getQuick(j);
                    Assert.assertTrue(set.add(qc.getName()));
                }
                nameSets.add(set);
            }

            for (int i = 0, n = columns.size(); i < n; i++) {
                checkLiteralIsInSet(columns.getQuick(i).getAst(), nameSets, nested.getAliasIndexes());
            }

            columns = nested.getTopDownColumns();
            nested = nested.getNestedModel();
        }
    }

    @FunctionalInterface
    private interface CairoAware {
        void run() throws SqlException;
    }
}
