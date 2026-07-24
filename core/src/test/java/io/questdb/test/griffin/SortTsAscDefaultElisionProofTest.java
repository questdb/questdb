/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \\ _   _  ___  ___| |_|  _ \\| __ )
 *   | | | | | | |/ _ \\/ __| __| | | |  _ \\
 *   | |_| | |_| |  __/\\__ \\ |_| |_| | |_) |
 *    \\__\\_\\\\__,_|\\___||___/\\__|____/|____/
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
import org.junit.Before;
import org.junit.Test;

/**
 * Proves the ORDER BY timestamp ASC elision fix for the factories reachable under the default
 * (parallel) configuration. Each test pins the suspect query's full plan: the top operator must be a
 * sort keyed on the designated timestamp (i.e. the outer ORDER BY is NOT elided) sitting above the
 * intended reordering factory. Without the matching getScanDirection() / followedOrderByAdvice fix
 * that top sort is dropped and assertsPlan fails.
 */
public class SortTsAscDefaultElisionProofTest extends AbstractCairoTest {

    @Before
    public void setUpTables() throws Exception {
        execute("CREATE TABLE t (timestamp TIMESTAMP, s SYMBOL, x LONG) TIMESTAMP(timestamp) PARTITION BY DAY");
        execute("INSERT INTO t SELECT dateadd('h', x::int, '2026-01-01T00:00:00.000000Z'), 'S' || (x%4), (x*17)%48 FROM long_sequence(48)");
        execute("CREATE TABLE t2 (timestamp TIMESTAMP, s SYMBOL, x LONG) TIMESTAMP(timestamp) PARTITION BY DAY");
        execute("INSERT INTO t2 SELECT dateadd('h', (x+24)::int, '2026-01-01T00:00:00.000000Z'), 'S' || (x%4), x FROM long_sequence(48)");
        execute("CREATE TABLE ai (si SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
        execute("INSERT INTO ai SELECT 'S' || (x%4), (x*7 % 20)::timestamp FROM long_sequence(20)");
    }

    // Async Group By -> SCAN_DIRECTION_OTHER
    @Test
    public void testAsyncGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("(SELECT timestamp, count() c FROM t group by timestamp) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Encode sort light
                          keys: [timestamp]
                            SelectedRecord
                                Async Group By workers: 1
                                  keys: [timestamp]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                        """);
        });
    }

    // AsyncTopKRecordCursorFactory -> ts-aware getScanDirection
    @Test
    public void testAsyncTopK() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("(SELECT timestamp, x FROM t order by x asc limit 30) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Encode sort light
                          keys: [timestamp]
                            SelectedRecord
                                Async Top K lo: 30 workers: 1
                                  filter: null
                                  keys: [x]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                        """);
        });
    }

    // EncodedSortLightRecordCursorFactory -> ts-aware getScanDirection
    @Test
    public void testEncodedSortLight() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("(SELECT timestamp, x FROM t order by x asc) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Encode sort light
                          keys: [timestamp]
                            SelectedRecord
                                Encode sort light
                                  keys: [x]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                        """);
        });
    }

    // UnionAllRecordCursorFactory -> SCAN_DIRECTION_OTHER
    @Test
    public void testUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("(SELECT timestamp FROM t UNION ALL SELECT timestamp FROM t2) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Encode sort
                          keys: [timestamp]
                            SelectedRecord
                                Union All
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t2
                        """);
        });
    }

    // UnionRecordCursorFactory -> SCAN_DIRECTION_OTHER
    @Test
    public void testUnionDistinct() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("(SELECT timestamp FROM t UNION SELECT timestamp FROM t2) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Encode sort
                          keys: [timestamp]
                            SelectedRecord
                                Union
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t2
                        """);
        });
    }

    // SortedSymbolIndexRecordCursorFactory + generateOrderBy followedOrderByAdvice guard
    @Test
    public void testSortedSymbolIndexAndAdviceGuard() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("(SELECT ts, si FROM ai WHERE ts >= 0::timestamp and ts < 100::timestamp order by si) timestamp(ts) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Encode sort light
                          keys: [ts]
                            SelectedRecord
                                SortedSymbolIndex
                                    Index forward scan on: si
                                      symbolOrder: asc
                                    Interval forward scan on: ai
                                      intervals: [("1970-01-01T00:00:00.000000Z","1970-01-01T00:00:00.000099Z")]
                        """);
        });
    }
}
