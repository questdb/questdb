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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Proves the ORDER BY timestamp ASC elision fix for the serial/limited factory variants that only
 * appear when the parallel group-by / filter / top-K paths are disabled. Each test pins the suspect
 * query's full plan: a top sort keyed on the designated timestamp (the outer ORDER BY is NOT elided)
 * above the intended factory. Without the matching getScanDirection() fix that top sort is dropped.
 */
public class SortTsAscSerialElisionProofTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, false);
        super.setUp();
    }

    @Before
    public void setUpTables() throws Exception {
        execute("CREATE TABLE t (timestamp TIMESTAMP, s SYMBOL, x LONG) TIMESTAMP(timestamp) PARTITION BY DAY");
        execute("INSERT INTO t SELECT dateadd('h', x::int, '2026-01-01T00:00:00.000000Z'), 'S' || (x%4), (x*17)%48 FROM long_sequence(48)");
        execute("CREATE TABLE t2 (timestamp TIMESTAMP, s SYMBOL, x LONG) TIMESTAMP(timestamp) PARTITION BY DAY");
        execute("INSERT INTO t2 SELECT dateadd('h', (x+24)::int, '2026-01-01T00:00:00.000000Z'), 'S' || (x%4), x FROM long_sequence(48)");
    }

    // GroupByRecordCursorFactory (vanilla, non-parallel keyed hash aggregation)
    @Test
    public void testVanillaGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("(SELECT timestamp, count() c FROM t group by timestamp) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Encode sort light
                          keys: [timestamp]
                            SelectedRecord
                                GroupBy vectorized: false
                                  keys: [timestamp]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                        """);
        });
    }

    // LongTopKRecordCursorFactory: group-by base supports long top-K; order by the LONG count column
    @Test
    public void testLongTopK() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("(SELECT timestamp, count() c FROM t group by timestamp order by c asc limit 30) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Encode sort light
                          keys: [timestamp]
                            SelectedRecord
                                Long Top K lo: 30
                                  keys: [c asc]
                                    GroupBy vectorized: false
                                      keys: [timestamp]
                                      values: [count(*)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                        """);
        });
    }

    // EncodedSortLimitedLightRecordCursorFactory: ORDER BY non-ts + LIMIT, encode-sort enabled
    @Test
    public void testEncodedSortLimitedLight() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, true);
            assertQuery("(SELECT timestamp, x FROM t order by x asc limit 30) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Encode sort light
                          keys: [timestamp]
                            SelectedRecord
                                Encode sort light lo: 30
                                  keys: [x]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                        """);
        });
    }

    // LimitedSizeSortedLightRecordCursorFactory: ORDER BY non-ts + LIMIT, encode-sort disabled
    @Test
    public void testLimitedSizeSortedLight() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
            assertQuery("(SELECT timestamp, x FROM t order by x asc limit 30) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Sort light
                          keys: [timestamp]
                            SelectedRecord
                                Sort light lo: 30
                                  keys: [x]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                        """);
        });
    }

    // SortedRecordCursorFactory (non-light): ORDER BY non-ts over a base without random access
    @Test
    public void testSortedNonLight() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
            assertQuery("(SELECT timestamp, x FROM (SELECT timestamp, x FROM t UNION ALL SELECT timestamp, x FROM t2) order by x asc) timestamp(timestamp) order by timestamp asc")
                    .noLeakCheck()
                    .assertsPlan("""
                        Sort light
                          keys: [timestamp]
                            SelectedRecord
                                Sort
                                  keys: [x]
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
}
