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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CountDistinctIPv4GroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNonKeyedHappy() throws Exception {
        String expected = """
                count_distinct
                20
                """;
        assertQuery("select count_distinct(a) from x")
                .ddl("create table x as (select rnd_ipv4() a from long_sequence(20))")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testNotNullSentinelCountedOnceGlobal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (v ipv4 not null)");
            execute("insert into tab values ('0.0.0.0'), ('192.168.0.1'), ('0.0.0.0')");
            // count() alongside defeats the optimizer's count_distinct-to-
            // distinct-subquery rewrite, pinning the accumulator path
            assertQuery("select count_distinct(v), count() from tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count_distinct\tcount
                            2\t3
                            """);
        });
    }

    @Test
    public void testNotNullSentinelCountedOnceGrouped() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (g symbol, v ipv4 not null)");
            execute("""
                    insert into tab values
                        ('a', '0.0.0.0'),
                        ('a', '192.168.0.1'),
                        ('a', '0.0.0.0'),
                        ('b', '0.0.0.0'),
                        ('b', '0.0.0.0')
                    """);
            assertQuery("select g, count_distinct(v) from tab order by g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tcount_distinct
                            a\t2
                            b\t1
                            """);
        });
    }

    @Test
    public void testNotNullSentinelCountedOnceOrderPermutations() throws Exception {
        assertMemoryLeak(() -> {
            // the repeated sentinel must dedup no matter where the accumulator
            // transitions from the inlined value to the hash set
            execute("create table tab1 (v ipv4 not null)");
            execute("insert into tab1 values ('192.168.0.1'), ('0.0.0.0'), ('0.0.0.0')");
            execute("create table tab2 (v ipv4 not null)");
            execute("insert into tab2 values ('0.0.0.0'), ('0.0.0.0'), ('192.168.0.1')");
            String expected = """
                    count_distinct\tcount
                    2\t3
                    """;
            assertQuery("select count_distinct(v), count() from tab1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("select count_distinct(v), count() from tab2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testNotNullSentinelCountedOnceParallel() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 2);
        assertMemoryLeak(() -> {
            execute("create table tab (g symbol, v ipv4 not null, ts timestamp not null) timestamp(ts) partition by day bypass wal");
            execute("""
                    insert into tab values
                        ('a', '0.0.0.0', '2024-01-01'),
                        ('a', '192.168.0.1', '2024-01-02'),
                        ('a', '0.0.0.0', '2024-01-03'),
                        ('a', '192.168.0.2', '2024-01-04'),
                        ('a', '0.0.0.0', '2024-01-05'),
                        ('b', '0.0.0.0', '2024-01-06')
                    """);
            assertQuery("select g, count_distinct(v) from tab order by g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tcount_distinct
                            a\t3
                            b\t1
                            """);
            assertQuery("select count_distinct(v), count() from tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count_distinct\tcount
                            3\t6
                            """);
        });
    }

    @Test
    public void testNullableSentinelStaysNull() throws Exception {
        assertMemoryLeak(() -> {
            // control: without NOT NULL the same bit pattern reads as SQL NULL
            // and stays excluded from count_distinct
            execute("create table tab (v ipv4)");
            execute("insert into tab values ('0.0.0.0'), ('192.168.0.1'), ('0.0.0.0')");
            assertQuery("select count_distinct(v), count() from tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count_distinct\tcount
                            1\t3
                            """);
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        execute("create table x (ts timestamp, ip ipv4) timestamp(ts);");
        execute("insert into x values ('2000-01-01', '192.168.1.1'), ('2000-01-01T04:30', '192.168.1.2'), ('2000-01-01T05:30', '192.168.1.3'), ('2000-01-03', '192.168.1.1');");

        String expectedDefault = """
                ts\tcount_distinct
                2000-01-01T00:00:00.000000Z\t3
                2000-01-03T00:00:00.000000Z\t1
                """;
        assertQuery("select ts, count_distinct(ip) from x sample by 1d")
                .timestamp("ts")
                .expectSize()
                .returns(expectedDefault);

        String expectedInterpolated = """
                ts\tcount_distinct
                2000-01-01T00:00:00.000000Z\t3
                2000-01-02T00:00:00.000000Z\t2
                2000-01-03T00:00:00.000000Z\t1
                """;
        assertQuery("select ts, count_distinct(ip) from x sample by 1d fill(linear)")
                .timestamp("ts")
                .expectSize()
                .returns(expectedInterpolated);
    }

}
