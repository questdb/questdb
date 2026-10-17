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

public class OrderPlanningTest extends AbstractCairoTest {

    @Test
    public void testAggregateOrderedByKeyReadsPageFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT s, count(), sum(x) FROM t WHERE ts IN '2024-01-01' GROUP BY s ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [s]
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [count(*),sum(x)]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	count	sum
                            a	1	2
                            b	2	4
                            """);
            assertQuery("SELECT s, avg(x), min(x), max(x) FROM t WHERE ts IN '2024-01-01' GROUP BY s ORDER BY s DESC LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light lo: 1
                              keys: [s desc]
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [avg(x),min(x),max(x)]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	avg	min	max
                            b	2.0	1	3
                            """);
            assertQuery("SELECT s, sum(x) FROM t WHERE ts IN '2024-01-01' ORDER BY 1")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [s]
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [sum(x)]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	sum
                            a	2
                            b	4
                            """);
            assertQuery("SELECT DISTINCT s FROM t WHERE ts IN '2024-01-01' ORDER BY s DESC")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [s desc]
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s
                            b
                            a
                            """);
            assertQuery("SELECT s, first(x), last(x) FROM t WHERE ts IN '2024-01-01' GROUP BY s ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [s]
                                Async Group By workers: 1
                                  keys: [s]
                                  values: [first(x),last(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	first	last
                            a	2	2
                            b	1	3
                            """);
            assertQuery("SELECT ts, s, count() FROM t WHERE ts IN '2024-01-01' SAMPLE BY 1d ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [s]
                                Async Group By workers: 1
                                  keys: [ts,s]
                                  keyFunctions: [timestamp_floor_utc('1d',ts)]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts	s	count
                            2024-01-01T00:00:00.000000Z	a	1
                            2024-01-01T00:00:00.000000Z	b	2
                            """);
            assertQuery("WITH q AS (SELECT s, count() c FROM t WHERE ts IN '2024-01-01' GROUP BY s) SELECT * FROM q ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [s]
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	c
                            a	1
                            b	2
                            """);
            assertQuery("SELECT s, count() FROM t WHERE ts IN '2024-01-01' AND s IN ('a', 'b') GROUP BY s ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [s]
                                GroupBy vectorized: false
                                  keys: [s]
                                  values: [count(*)]
                                    FilterOnValues
                                        Table-order scan
                                            Index forward scan on: s deferred: true
                                              filter: s='b'
                                            Index forward scan on: s deferred: true
                                              filter: s='a'
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	count
                            a	1
                            b	2
                            """);
            execute("CREATE TABLE u AS (SELECT * FROM t), INDEX(s) TIMESTAMP(ts)");
            assertQuery("SELECT s, count() FROM u WHERE ts IN '2024-01-01' GROUP BY s ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort light
                              keys: [s]
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: u
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	count
                            a	1
                            b	2
                            """);
        });
    }

    @Test
    public void testAliasedKeyOrderThroughComputedProjectionReachesSymbolIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT s k, x + 1 v FROM t WHERE ts IN '2024-01-01' ORDER BY k, ts LIMIT 2")
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                Limit value: 2 skip-rows-max: 0 take-rows-max: 2
                                    VirtualRecord
                                      functions: [s,x+1,ts]
                                        SortedSymbolIndex
                                            Index forward scan on: s
                                              symbolOrder: asc
                                            Interval forward scan on: t
                                              intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            k	v
                            a	3
                            b	2
                            """);
            assertQuery("SELECT s k, x + 1 v FROM t WHERE ts IN '2024-01-01' ORDER BY k DESC")
                    .noLeakCheck()
                    .withPlan("""
                            VirtualRecord
                              functions: [s,x+1]
                                SortedSymbolIndex
                                    Index forward scan on: s
                                      symbolOrder: desc
                                    Interval forward scan on: t
                                      intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            k	v
                            b	2
                            b	4
                            a	3
                            """);
            assertQuery("SELECT x s, s x, x + 1 v FROM t WHERE ts IN '2024-01-01' ORDER BY x")
                    .noLeakCheck()
                    .withPlan("""
                            VirtualRecord
                              functions: [x,s,x+1]
                                SortedSymbolIndex
                                    Index forward scan on: s
                                      symbolOrder: asc
                                    Interval forward scan on: t
                                      intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	x	v
                            2	a	3
                            1	b	2
                            3	b	4
                            """);
            assertQuery("SELECT x s, s x, x + 1 v FROM t WHERE ts IN '2024-01-01' ORDER BY s")
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort light
                              keys: [s]
                                VirtualRecord
                                  functions: [x,s,x+1]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	x	v
                            1	b	2
                            2	a	3
                            3	b	4
                            """);
            assertQuery("SELECT s::STRING k, x + 1 v FROM t WHERE ts IN '2024-01-01' ORDER BY k")
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort light
                              keys: [k]
                                VirtualRecord
                                  functions: [s::string,x+1]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            k	v
                            a	3
                            b	2
                            b	4
                            """);
        });
    }

    @Test
    public void testAliasedTimestampOrderThroughComputedProjectionReachesFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts k, x + 1 v FROM t WHERE x > 1 ORDER BY k DESC LIMIT 2")
                    .noLeakCheck()
                    .timestampDesc("k")
                    .withPlan("""
                            VirtualRecord
                              functions: [ts,x+1]
                                Async JIT Filter workers: 1
                                  limit: 2
                                  filter: 1<x
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: t
                            """)
                    .returns("""
                            k	v
                            2024-01-02T00:00:00.000000Z	5
                            2024-01-01T02:00:00.000000Z	4
                            """);
        });
    }

    @Test
    public void testDescendingOrderedInputWindowScansBackward() throws Exception {
        assertMemoryLeak(() -> {
            createWindowTable();
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts DESC RANGE 2 SECONDS PRECEDING) a FROM w")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Window
                              functions: [avg(v) over (range between 2000000 preceding and current row)]
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: w
                            """)
                    .returns("""
                            ts	v	a
                            2024-01-01T00:00:09.000000Z	10.0	10.0
                            2024-01-01T00:00:08.000000Z	9.0	9.5
                            2024-01-01T00:00:07.000000Z	8.0	9.0
                            2024-01-01T00:00:06.000000Z	7.0	8.0
                            2024-01-01T00:00:05.000000Z	6.0	7.0
                            2024-01-01T00:00:04.000000Z	5.0	6.0
                            2024-01-01T00:00:03.000000Z	4.0	5.0
                            2024-01-01T00:00:02.000000Z	3.0	4.0
                            2024-01-01T00:00:01.000000Z	2.0	3.0
                            2024-01-01T00:00:00.000000Z	1.0	2.0
                            """);
            final String firstRows = """
                    ts	v	a
                    2024-01-01T00:00:00.000000Z	1.0	2.0
                    2024-01-01T00:00:01.000000Z	2.0	3.0
                    2024-01-01T00:00:02.000000Z	3.0	4.0
                    """;
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts DESC RANGE 2 SECONDS PRECEDING) a FROM w WHERE ts IN '2024-01-01' ORDER BY v LIMIT 3")
                    .noLeakCheck()
                    .withPlan("""
                            Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                                Encode sort
                                  keys: [v]
                                    Window
                                      functions: [avg(v) over (range between 2000000 preceding and current row)]
                                        PageFrame
                                            Row backward scan
                                            Interval backward scan on: w
                                              intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns(firstRows);
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts DESC RANGE 2 SECONDS PRECEDING) a FROM w WHERE ts IN '2024-01-01' ORDER BY ts LIMIT 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(firstRows);
        });
    }

    @Test
    public void testDescendingScanThroughProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts, v FROM (SELECT ts, x + 1 v FROM t) ORDER BY ts DESC")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [ts,x+1]
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: t
                            """)
                    .returns("""
                            ts	v
                            2024-01-02T00:00:00.000000Z	5
                            2024-01-01T02:00:00.000000Z	4
                            2024-01-01T01:00:00.000000Z	3
                            2024-01-01T00:00:00.000000Z	2
                            """);
        });
    }

    @Test
    public void testJoinSlaveDoesNotScanBackward() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT a.x, b.x bx FROM t a JOIN (SELECT * FROM t ORDER BY ts DESC) b ON a.s = b.s ORDER BY a.x, bx")
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort
                              keys: [x, bx]
                                SelectedRecord
                                    Hash Join Light
                                      condition: b.s=a.s
                                      symbolKeyJoin: true
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                        Hash
                                            SelectedRecord
                                                Encode sort light
                                                  keys: [ts desc]
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t
                            """)
                    .returns("""
                            x	bx
                            1	1
                            1	3
                            2	2
                            2	4
                            3	1
                            3	3
                            4	2
                            4	4
                            """);
        });
    }

    @Test
    public void testKeyOrderOverAggregateOfRenamedInput() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT k, sum(v) FROM (SELECT s k, x v FROM t WHERE x < 4) ORDER BY k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k	sum
                            a	2
                            b	4
                            """);
        });
    }

    @Test
    public void testKeyOrderThroughProjectionReachesSymbolIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT s, x FROM (SELECT s, x, ts FROM t WHERE ts IN '2024-01-01') ORDER BY s")
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                SortedSymbolIndex
                                    Index forward scan on: s
                                      symbolOrder: asc
                                    Interval forward scan on: t
                                      intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	x
                            a	2
                            b	1
                            b	3
                            """);
        });
    }

    @Test
    public void testLimitThroughProjectionReachesFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts, x FROM (SELECT ts, x FROM t WHERE x > 1) ORDER BY ts DESC LIMIT 2")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .withPlan("""
                            Async JIT Filter workers: 1
                              limit: 2
                              filter: 1<x
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: t
                            """)
                    .returns("""
                            ts	x
                            2024-01-02T00:00:00.000000Z	4
                            2024-01-01T02:00:00.000000Z	3
                            """);
        });
    }

    @Test
    public void testNonFusedFilterOrderedByKeyReadsPageFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT * FROM (SELECT s, x, x * 2 xx FROM t WHERE ts IN '2024-01-01') WHERE xx > 2 ORDER BY s")
                    .noLeakCheck()
                    .withPlanContaining("Encode sort light", "Filter filter: 2<xx", "PageFrame", "Interval forward scan on: t")
                    .returns("""
                            s	x	xx
                            a	2	4
                            b	3	6
                            """);
            assertQuery("SELECT * FROM (SELECT s, x, x * 2 xx FROM t WHERE ts IN '2024-01-01') WHERE xx > 2 ORDER BY s DESC LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Filter filter: 2<xx", "PageFrame", "Interval forward scan on: t")
                    .returns("""
                            s	x	xx
                            b	3	6
                            """);
        });
    }

    @Test
    public void testOrderedInputWindowReadsIndexInTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            createWindowTable();
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w WHERE s IN ('S1', 'S2')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Window
                              functions: [avg(v) over (range between 2000000 preceding and current row)]
                                FilterOnValues
                                    Table-order scan
                                        Index forward scan on: s deferred: true
                                          filter: s='S2'
                                        Index forward scan on: s deferred: true
                                          filter: s='S1'
                                    Frame forward scan on: w
                            """)
                    .returns("""
                            ts	v	a
                            2024-01-01T00:00:00.000000Z	1.0	1.0
                            2024-01-01T00:00:01.000000Z	2.0	1.5
                            2024-01-01T00:00:03.000000Z	4.0	3.0
                            2024-01-01T00:00:04.000000Z	5.0	4.5
                            2024-01-01T00:00:06.000000Z	7.0	6.0
                            2024-01-01T00:00:07.000000Z	8.0	7.5
                            2024-01-01T00:00:09.000000Z	10.0	9.0
                            """);
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w WHERE s IN ('S1', 'S2') ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .returns("""
                            ts	v	a
                            2024-01-01T00:00:09.000000Z	10.0	9.0
                            2024-01-01T00:00:07.000000Z	8.0	7.5
                            2024-01-01T00:00:06.000000Z	7.0	6.0
                            """);
        });
    }

    @Test
    public void testOrderedInputWindowUnderDescendingOrderScansForward() throws Exception {
        assertMemoryLeak(() -> {
            createWindowTable();
            final String expected = """
                    ts	v	a
                    2024-01-01T00:00:09.000000Z	10.0	9.0
                    2024-01-01T00:00:08.000000Z	9.0	8.0
                    2024-01-01T00:00:07.000000Z	8.0	7.0
                    """;
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .withPlan("""
                            Limit value: 3 skip-rows: 0 take-rows: 3
                                Encode sort
                                  keys: [ts desc]
                                    Window
                                      functions: [avg(v) over (range between 2000000 preceding and current row)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: w
                            """)
                    .returns(expected);
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w ORDER BY 1 DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT ts, v, avg(v) OVER w1 a FROM w WINDOW w1 AS (ORDER BY ts RANGE 2 SECONDS PRECEDING) ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns(expected);
            assertQuery("WITH q AS (SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w) SELECT * FROM q ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT * FROM (SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w) ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT ts t2, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w ORDER BY t2 DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("t2")
                    .expectSize()
                    .returns("""
                            t2	v	a
                            2024-01-01T00:00:09.000000Z	10.0	9.0
                            2024-01-01T00:00:08.000000Z	9.0	8.0
                            2024-01-01T00:00:07.000000Z	8.0	7.0
                            """);
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w WHERE v > 3 ORDER BY ts DESC")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .returns("""
                            ts	v	a
                            2024-01-01T00:00:09.000000Z	10.0	9.0
                            2024-01-01T00:00:08.000000Z	9.0	8.0
                            2024-01-01T00:00:07.000000Z	8.0	7.0
                            2024-01-01T00:00:06.000000Z	7.0	6.0
                            2024-01-01T00:00:05.000000Z	6.0	5.0
                            2024-01-01T00:00:04.000000Z	5.0	4.5
                            2024-01-01T00:00:03.000000Z	4.0	4.0
                            """);
            assertQuery("""
                    SELECT ts, sum(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) sm,
                        count(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) c,
                        first_value(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) f,
                        max(v) OVER (PARTITION BY s ORDER BY ts RANGE 5 SECONDS PRECEDING) mx,
                        stddev_samp(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) sd,
                        last_value(v) OVER (ORDER BY ts) l
                    FROM w ORDER BY ts DESC LIMIT 3""")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns("""
                            ts	sm	c	f	mx	sd	l
                            2024-01-01T00:00:09.000000Z	27.0	3	8.0	10.0	1.0	10.0
                            2024-01-01T00:00:08.000000Z	24.0	3	7.0	9.0	1.0	9.0
                            2024-01-01T00:00:07.000000Z	21.0	3	6.0	8.0	1.0	8.0
                            """);
        });
    }

    @Test
    public void testOrderedInputWindowUnderMultiKeyOrder() throws Exception {
        assertMemoryLeak(() -> {
            createWindowTable();
            final String lastRows = """
                    ts	v	a
                    2024-01-01T00:00:09.000000Z	10.0	9.0
                    2024-01-01T00:00:08.000000Z	9.0	8.0
                    2024-01-01T00:00:07.000000Z	8.0	7.0
                    """;
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w ORDER BY ts DESC, v LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .withPlan("""
                            Limit value: 3 skip-rows: 0 take-rows: 3
                                Encode sort
                                  keys: [ts desc, v]
                                    Window
                                      functions: [avg(v) over (range between 2000000 preceding and current row)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: w
                            """)
                    .returns(lastRows);
            assertQuery("SELECT ts, v, avg(v) OVER w1 a FROM w WINDOW w1 AS (ORDER BY ts RANGE 2 SECONDS PRECEDING) ORDER BY ts DESC, v LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns(lastRows);
            assertQuery("WITH q AS (SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w) SELECT * FROM q ORDER BY ts DESC, v LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns(lastRows);
            assertQuery("SELECT * FROM (SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w) ORDER BY v DESC, ts LIMIT 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns(lastRows);
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w ORDER BY ts, v LIMIT 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts	v	a
                            2024-01-01T00:00:00.000000Z	1.0	1.0
                            2024-01-01T00:00:01.000000Z	2.0	1.5
                            2024-01-01T00:00:02.000000Z	3.0	2.0
                            """);
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts RANGE 2 SECONDS PRECEDING) a FROM w WHERE v > 3 ORDER BY v DESC, ts DESC")
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort
                              keys: [v desc, ts desc]
                                Window
                                  functions: [avg(v) over (range between 2000000 preceding and current row)]
                                    Async JIT Filter workers: 1
                                      filter: 3<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: w
                            """)
                    .returns("""
                            ts	v	a
                            2024-01-01T00:00:09.000000Z	10.0	9.0
                            2024-01-01T00:00:08.000000Z	9.0	8.0
                            2024-01-01T00:00:07.000000Z	8.0	7.0
                            2024-01-01T00:00:06.000000Z	7.0	6.0
                            2024-01-01T00:00:05.000000Z	6.0	5.0
                            2024-01-01T00:00:04.000000Z	5.0	4.5
                            2024-01-01T00:00:03.000000Z	4.0	4.0
                            """);
            assertQuery("""
                    SELECT s, ts, avg(v) OVER (PARTITION BY s ORDER BY ts RANGE 3 SECONDS PRECEDING) a,
                        avg(v) OVER (PARTITION BY s ORDER BY ts ROWS 1 PRECEDING) r
                    FROM w ORDER BY s, ts DESC""")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort
                              keys: [s, ts desc]
                                Window
                                  functions: [avg(v) over (partition by [s] range between 3000000 preceding and current row),avg(v) over (partition by [s] rows between 1 preceding and current row)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: w
                            """)
                    .returns("""
                            s	ts	a	r
                            S0	2024-01-01T00:00:08.000000Z	7.5	7.5
                            S0	2024-01-01T00:00:05.000000Z	4.5	4.5
                            S0	2024-01-01T00:00:02.000000Z	3.0	3.0
                            S1	2024-01-01T00:00:09.000000Z	8.5	8.5
                            S1	2024-01-01T00:00:06.000000Z	5.5	5.5
                            S1	2024-01-01T00:00:03.000000Z	2.5	2.5
                            S1	2024-01-01T00:00:00.000000Z	1.0	1.0
                            S2	2024-01-01T00:00:07.000000Z	6.5	6.5
                            S2	2024-01-01T00:00:04.000000Z	3.5	3.5
                            S2	2024-01-01T00:00:01.000000Z	2.0	2.0
                            """);
            assertQuery("""
                    SELECT ts, v, sum(v) OVER w1 sm, count(v) OVER w1 c, avg(v) OVER (ORDER BY ts ROWS 2 PRECEDING) r,
                        row_number() OVER (ORDER BY ts) rn
                    FROM w WINDOW w1 AS (ORDER BY ts RANGE 2 SECONDS PRECEDING) ORDER BY ts DESC, v LIMIT 3""")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns("""
                            ts	v	sm	c	r	rn
                            2024-01-01T00:00:09.000000Z	10.0	27.0	3	9.0	10
                            2024-01-01T00:00:08.000000Z	9.0	24.0	3	8.0	9
                            2024-01-01T00:00:07.000000Z	8.0	21.0	3	7.0	8
                            """);
            assertQuery("""
                    SELECT ts, v, avg(v) OVER (ORDER BY ts DESC RANGE 2 SECONDS PRECEDING) ad, avg(v) OVER (ORDER BY ts ROWS 2 PRECEDING) r
                    FROM w ORDER BY ts, v LIMIT 4""")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .withPlan("""
                            Encode sort light lo: 4
                              keys: [ts, v]
                                CachedWindowLight
                                  orderedFunctions: [[ts] => [avg(v) over ( rows between 2 preceding and current row)]]
                                  unorderedFunctions: [avg(v) over (range between 2000000 preceding and current row)]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: w
                            """)
                    .returns("""
                            ts	v	ad	r
                            2024-01-01T00:00:00.000000Z	1.0	2.0	1.0
                            2024-01-01T00:00:01.000000Z	2.0	3.0	1.5
                            2024-01-01T00:00:02.000000Z	3.0	4.0	2.0
                            2024-01-01T00:00:03.000000Z	4.0	5.0	3.0
                            """);
            assertQuery("""
                    SELECT ts, v, avg(v) OVER (ORDER BY ts DESC RANGE 2 SECONDS PRECEDING) ad, avg(v) OVER (ORDER BY ts ROWS 2 PRECEDING) r
                    FROM w ORDER BY v DESC, ts""")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	v	ad	r
                            2024-01-01T00:00:09.000000Z	10.0	10.0	9.0
                            2024-01-01T00:00:08.000000Z	9.0	9.5	8.0
                            2024-01-01T00:00:07.000000Z	8.0	9.0	7.0
                            2024-01-01T00:00:06.000000Z	7.0	8.0	6.0
                            2024-01-01T00:00:05.000000Z	6.0	7.0	5.0
                            2024-01-01T00:00:04.000000Z	5.0	6.0	4.0
                            2024-01-01T00:00:03.000000Z	4.0	5.0	3.0
                            2024-01-01T00:00:02.000000Z	3.0	4.0	2.0
                            2024-01-01T00:00:01.000000Z	2.0	3.0	1.5
                            2024-01-01T00:00:00.000000Z	1.0	2.0	1.0
                            """);
        });
    }

    @Test
    public void testPostingIndexDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO p VALUES
                        ('2024-01-01T00:00:00.000000Z', 'A'),
                        ('2024-01-01T01:00:00.000000Z', 'B'),
                        ('2024-01-02T00:00:00.000000Z', 'A')
                    """);
            assertQuery("SELECT DISTINCT sym FROM p")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlan("""
                            PostingIndex op: distinct on: sym
                                Frame forward scan on: p
                            """)
                    .returns("""
                            sym
                            A
                            B
                            """);
        });
    }

    @Test
    public void testWindowWithoutOrderedInputUnderDescendingOrderScansBackward() throws Exception {
        assertMemoryLeak(() -> {
            createWindowTable();
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts ROWS 2 PRECEDING) a FROM w ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .withPlan("""
                            Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                                CachedWindowLight
                                  orderedFunctions: [[ts] => [avg(v) over ( rows between 2 preceding and current row)]]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: w
                            """)
                    .returns("""
                            ts	v	a
                            2024-01-01T00:00:09.000000Z	10.0	9.0
                            2024-01-01T00:00:08.000000Z	9.0	8.0
                            2024-01-01T00:00:07.000000Z	8.0	7.0
                            """);
            assertQuery("SELECT ts, v, avg(v) OVER (ORDER BY ts DESC RANGE 2 SECONDS PRECEDING) a FROM w ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Limit value: 3 skip-rows: 0 take-rows: 3
                                Window
                                  functions: [avg(v) over (range between 2000000 preceding and current row)]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: w
                            """)
                    .returns("""
                            ts	v	a
                            2024-01-01T00:00:09.000000Z	10.0	10.0
                            2024-01-01T00:00:08.000000Z	9.0	9.5
                            2024-01-01T00:00:07.000000Z	8.0	9.0
                            """);
            assertQuery("SELECT ts, v, avg(v, 'second', 2) OVER (ORDER BY ts) e FROM w ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                                CachedWindowLight
                                  orderedFunctions: [[ts] => [avg(v, 'second', 2.0) over (rows between unbounded preceding and current row)]]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: w
                            """);
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'b', 1),
                    ('2024-01-01T01:00:00.000000Z', 'a', 2),
                    ('2024-01-01T02:00:00.000000Z', 'b', 3),
                    ('2024-01-02T00:00:00.000000Z', 'a', 4)
                """);
    }

    private static void createWindowTable() throws Exception {
        execute("CREATE TABLE w AS (SELECT x::DOUBLE v, ('S' || (x % 3))::SYMBOL s, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(10)), INDEX(s) TIMESTAMP(ts) PARTITION BY HOUR");
    }
}
