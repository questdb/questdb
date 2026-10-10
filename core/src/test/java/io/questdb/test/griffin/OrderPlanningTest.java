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
    public void testKeyOrderThroughAggregateOverRenamedInput() throws Exception {
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
}
