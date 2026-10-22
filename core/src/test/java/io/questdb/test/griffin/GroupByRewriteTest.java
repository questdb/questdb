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
                                Async Hash Join Group By workers: 1
                                  logicalJoinType: inner
                                  physicalJoinType: inner
                                  inputSwapped: false
                                  condition: t1.sym=t2.sym
                                  symbolKeyJoin: true
                                  buildStrategy: shared
                                  buildPayload: copied when the probe is large enough
                                  keys: [ts,price,price1]
                                  values: [sum(t1.amount)]
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                    Build
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
    public void testSumNormalisationAfterSumReadOnlyInsideExpression() throws Exception {
        assertMemoryLeak(() -> {
            createSumNormalisationTable();
            assertQuery("SELECT sum(x + 1) * 2 a, sum(x + 2) b FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [sum*2,sum1+COUNT*2]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x+1),sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """)
                    .returns("""
                            a\tb
                            22\t14
                            """);
        });
    }

    @Test
    public void testSumNormalisationCountsRowsOfByteColumn() throws Exception {
        assertMemoryLeak(() -> {
            createSumNormalisationTable();
            assertQuery("SELECT sum(b + 1), sum(b * 2), sum(2 - b), count() FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [sum+count*1,sum*2,count*2-sum,count]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(b),count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """)
                    .returns("""
                            sum\tsum1\tsum2\tcount
                            20\t30\t-5\t5
                            """);
        });
    }

    @Test
    public void testSumNormalisationKeepsSumReadInsideExpression() throws Exception {
        assertMemoryLeak(() -> {
            createSumNormalisationTable();
            assertQuery("SELECT sum(x + 1) s, sum(x + 1) * 2 d FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [sum+COUNT*1,s*2]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x+1),sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """)
                    .returns("""
                            s\td
                            11\t22
                            """);
        });
    }

    @Test
    public void testSumNormalisationInsideCorrelatedLateral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (k SYMBOL, x INT)");
            execute("INSERT INTO tab VALUES ('a', 1), ('a', null), ('b', 3), ('b', 4), ('c', null)");
            assertQuery("SELECT t1.k, l.s FROM tab t1 JOIN LATERAL (SELECT sum(x + 1) AS s FROM tab t2 WHERE t2.k = t1.k) l ON true ORDER BY t1.k")
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort
                              keys: [k]
                                VirtualRecord
                                  functions: [t1.k,l.__qdb_count_carrier__0+coalesce(l.__qdb_count_carrier__1,0)*1]
                                    Hash Left Outer Join Light
                                      condition: l.__qdb_outer_ref__0_k=t1.k
                                      symbolKeyJoin: true
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                                        Hash
                                            VirtualRecord
                                              functions: [__qdb_outer_ref__0_k,sum,COUNT]
                                                GroupBy vectorized: true workers: 1
                                                  keys: [k]
                                                  values: [sum(x),count(x)]
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: tab
                            """)
                    .returns("""
                            k\ts
                            a\t2
                            a\t2
                            b\t9
                            b\t9
                            c\tnull
                            """);
        });
    }

    @Test
    public void testSumNormalisationNamesReuseRenamedAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (x INT, y INT, z INT)");
            execute("INSERT INTO tab VALUES (1, 2, 3), (null, 4, null)");
            assertQuery("SELECT count() COUNT, sum(x + 1) a, sum(y + 1) sum1, sum(z - 1) b, sum(y * 2) Sum2 FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [COUNT,sum+COUNT1*1,sum2+COUNT2*1,sum1-COUNT3*1,sum2*2]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [count(*),sum(x),sum(y),sum(z),count(x),count(y),count(z)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """)
                    .returns("""
                            COUNT\ta\tsum1\tb\tSum2
                            2\t2\t8\t2\t12
                            """);
        });
    }

    @Test
    public void testSumNormalisationReusesLaterColumnSum() throws Exception {
        assertMemoryLeak(() -> {
            createSumNormalisationTable();
            assertQuery("SELECT sum(x + 1) a, sum(x) b FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [b+COUNT*1,b]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """)
                    .returns("""
                            a\tb
                            11\t8
                            """);
        });
    }

    @Test
    public void testSumNormalisationSharesSumAndCount() throws Exception {
        assertMemoryLeak(() -> {
            createSumNormalisationTable();
            assertQuery("SELECT sum(x + 1), sum(2 - x), sum(x * 3), sum(x - 4), sum(3 * x) FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [sum+COUNT*1,COUNT*2-sum,sum*3,sum-COUNT*4,3*sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """)
                    .returns("""
                            sum\tsum1\tsum2\tsum3\tsum4
                            11\t-2\t24\t-4\t24
                            """);
        });
    }

    @Test
    public void testSumNormalisationSkipsSubQueryInput() throws Exception {
        assertMemoryLeak(() -> {
            createSumNormalisationTable();
            assertQuery("SELECT sum(x + 1) FROM (SELECT x FROM tab)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Async Group By workers: 1
                              vectorized: false
                              values: [sum(x+1)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """)
                    .returns("""
                            sum
                            11
                            """);
        });
    }

    @Test
    public void testSumNormalisationWideProjection() throws Exception {
        assertMemoryLeak(() -> {
            final int columnCount = 512;
            final StringBuilder ddl = new StringBuilder("CREATE TABLE tab AS (SELECT ");
            final StringBuilder query = new StringBuilder("SELECT ");
            final StringBuilder expected = new StringBuilder();
            for (int i = 0; i < columnCount; i++) {
                final String separator = i > 0 ? ", " : "";
                ddl.append(separator).append(i).append("::INT c").append(i);
                query.append(separator).append("sum(c").append(i).append(" + 1)");
                expected.append(i > 0 ? "\tsum" + i : "sum");
            }
            expected.append('\n');
            for (int i = 0; i < columnCount; i++) {
                expected.append(i > 0 ? "\t" : "").append(i + 1);
            }
            expected.append('\n');
            execute(ddl.append(" FROM long_sequence(1))").toString());
            assertQuery(query.append(" FROM tab").toString())
                    .noLeakCheck()
                    .noMemoryUsageCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected.toString());
        });
    }

    @Test
    public void testSumNormalisationWithDistinct() throws Exception {
        assertMemoryLeak(() -> {
            createSumNormalisationTable();
            assertQuery("SELECT DISTINCT sum(x * 2) a, sum(x + 1) b, sum(x - 1) c FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("values: [sum(x),count(x)]")
                    .returns("""
                            a\tb\tc
                            16\t11\t5
                            """);
            assertQuery("SELECT DISTINCT sum(x + 1), sum(nocol - 1) FROM tab")
                    .noLeakCheck()
                    .fails(32, "Invalid column: nocol");
        });
    }

    @Test
    public void testSumNormalisationWithGroupByAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE g (k SYMBOL, i INT)");
            execute("INSERT INTO g VALUES ('a', 1), ('b', null), ('a', 3), ('b', 4), ('a', null)");
            assertQuery("SELECT k, sum(i * 2) a, sum(i + 1) b, sum(i - 1) c FROM g GROUP BY k ORDER BY sum(i + 1) DESC")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("values: [sum(i),count(i)]")
                    .returns("""
                            k\ta\tb\tc
                            a\t8\t6\t2
                            b\t8\t5\t3
                            """);
            assertQuery("SELECT k, sum(i * 2) a FROM g GROUP BY k ORDER BY sum(i - 1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ta
                            a\t8
                            b\t8
                            """);
            assertQuery("SELECT k, sum(nocol * 2) FROM g GROUP BY k")
                    .noLeakCheck()
                    .fails(14, "Invalid column: nocol");
        });
    }

    @Test
    public void testSumNormalisationWithSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (ts TIMESTAMP, i INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES ('2024-01-01T00:00:00', 1), ('2024-01-01T00:30:00', 2), ('2024-01-01T02:00:00', 3)");
            assertQuery("SELECT ts, sum(i * 2) a, sum(i + 1) b, sum(i - 1) c FROM s SAMPLE BY 1h")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .withPlanContaining("values: [sum(i),count(i)]")
                    .returns("""
                            ts\ta\tb\tc
                            2024-01-01T00:00:00.000000Z\t6\t5\t1
                            2024-01-01T02:00:00.000000Z\t6\t4\t2
                            """);
            assertQuery("SELECT ts, sum(i * 2) a, sum(i + 1) b, sum(i - 1) c FROM s SAMPLE BY 1h FILL(NULL)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlanContaining("values: [sum(i*2),sum(i+1),sum(i-1)]")
                    .returns("""
                            ts\ta\tb\tc
                            2024-01-01T00:00:00.000000Z\t6\t5\t1
                            2024-01-01T01:00:00.000000Z\tnull\tnull\tnull
                            2024-01-01T02:00:00.000000Z\t6\t4\t2
                            """);
            assertQuery("SELECT ts, sum(nocol + 1) FROM s SAMPLE BY 1h")
                    .noLeakCheck()
                    .fails(15, "Invalid column: nocol");
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

    private static void createSumNormalisationTable() throws Exception {
        execute("CREATE TABLE tab (x INT, b BYTE)");
        execute("INSERT INTO tab VALUES (1, 1), (null, 2), (3, 3), (4, 4), (null, 5)");
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
}
