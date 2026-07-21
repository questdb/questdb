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

package io.questdb.test.griffin.engine.window;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class UniformWindowFunctionTest extends AbstractCairoTest {

    // Local restore of the plain value-only assert helper that master removed from
    // AbstractCairoTest/TestUtils (consolidated toward fluent assertQuery). Mirrors the
    // same local helper in SubsampleTest.java.
    private void assertSql(CharSequence expected, CharSequence sql) throws SqlException {
        printSql(sql);
        TestUtils.assertEquals(expected, sink);
    }

    @Test
    public void testKeepsAllWhenTargetGteRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(3)");
            // target 5 >= 3 rows -> keep all
            assertSql(
                    "ts\tv\tkeep\n" +
                            "1970-01-01T00:00:00.000001Z\t1.0\ttrue\n" +
                            "1970-01-01T00:00:00.000002Z\t2.0\ttrue\n" +
                            "1970-01-01T00:00:00.000003Z\t3.0\ttrue\n",
                    "select ts, v, uniform(5) over (order by ts) keep from t"
            );
        });
    }

    @Test
    public void testEvenlySpacedSelection() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(5)");
            // n=5, N=3: divisor=2, range=4, half=1. pos(i)=(i*4+1)/2 -> 0, 2, 4 (0-based) => rows 1,3,5
            assertSql(
                    "ts\tv\tkeep\n" +
                            "1970-01-01T00:00:00.000001Z\t1.0\ttrue\n" +
                            "1970-01-01T00:00:00.000002Z\t2.0\tfalse\n" +
                            "1970-01-01T00:00:00.000003Z\t3.0\ttrue\n" +
                            "1970-01-01T00:00:00.000004Z\t4.0\tfalse\n" +
                            "1970-01-01T00:00:00.000005Z\t5.0\ttrue\n",
                    "select ts, v, uniform(3) over (order by ts) keep from t"
            );
        });
    }

    @Test
    public void testFilterYieldsReducedSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(5)");
            assertSql(
                    "ts\tv\n" +
                            "1970-01-01T00:00:00.000001Z\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t3.0\n" +
                            "1970-01-01T00:00:00.000005Z\t5.0\n",
                    "select ts, v from (select ts, v, uniform(3) over (order by ts) keep from t) where keep"
            );
        });
    }

    @Test
    public void testTargetOneKeepsSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(5)");
            // target=1 over 5 rows: divisor (target-1) would be 0, must not divide by zero;
            // keeps a single, roughly-middle row (index (5-1)/2 = 2 -> row 3).
            assertSql(
                    "ts\tv\tkeep\n" +
                            "1970-01-01T00:00:00.000001Z\t1.0\tfalse\n" +
                            "1970-01-01T00:00:00.000002Z\t2.0\tfalse\n" +
                            "1970-01-01T00:00:00.000003Z\t3.0\ttrue\n" +
                            "1970-01-01T00:00:00.000004Z\t4.0\tfalse\n" +
                            "1970-01-01T00:00:00.000005Z\t5.0\tfalse\n",
                    "select ts, v, uniform(1) over (order by ts) keep from t"
            );
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, uniform(3) over (order by ts) from t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [uniform(3) over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }

    @Test
    public void testRejectsNonConstantTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertExceptionNoLeakCheck(
                    "select ts, uniform(v::long) over (order by ts) from t",
                    20,
                    "target must be a constant"
            );
        });
    }
}
