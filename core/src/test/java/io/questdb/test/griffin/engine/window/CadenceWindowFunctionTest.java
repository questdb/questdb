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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CadenceWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testStrideNoSeed() throws Exception {
        // n=10, stride=3, offset=0 (no seed). keep ordinals: 0, then 3,6,9, then pin last (9 already there).
        // -> ordinals 0,3,6,9 -> rows 1,4,7,10
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(10)");
            assertQuery("select ts, v, cadence(3) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\tfalse
                            1970-01-01T00:00:00.000004Z\t4.0\ttrue
                            1970-01-01T00:00:00.000005Z\t5.0\tfalse
                            1970-01-01T00:00:00.000006Z\t6.0\tfalse
                            1970-01-01T00:00:00.000007Z\t7.0\ttrue
                            1970-01-01T00:00:00.000008Z\t8.0\tfalse
                            1970-01-01T00:00:00.000009Z\t9.0\tfalse
                            1970-01-01T00:00:00.000010Z\t10.0\ttrue
                            """);
        });
    }

    @Test
    public void testStrideOne() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(3)");
            assertQuery("select ts, v, cadence(1) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\ttrue
                            1970-01-01T00:00:00.000003Z\t3.0\ttrue
                            """);
        });
    }

    @Test
    public void testDeterministicSeedOffset() throws Exception {
        // With a constant seed the offset shifts the stride start deterministically.
        // n=10, stride=3, seed=42: offset computed via the re-homed splitmix64 mix (see
        // CadenceFunctionFactory.CadenceFunction#computeOffset); verified stable and != 0
        // (the actual, observed output of the algorithm - not independently re-derived here).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(10)");
            assertQuery("select ts, v, cadence(3, 42) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\tfalse
                            1970-01-01T00:00:00.000004Z\t4.0\tfalse
                            1970-01-01T00:00:00.000005Z\t5.0\ttrue
                            1970-01-01T00:00:00.000006Z\t6.0\tfalse
                            1970-01-01T00:00:00.000007Z\t7.0\tfalse
                            1970-01-01T00:00:00.000008Z\t8.0\ttrue
                            1970-01-01T00:00:00.000009Z\t9.0\tfalse
                            1970-01-01T00:00:00.000010Z\t10.0\ttrue
                            """);
        });
    }

    @Test
    public void testRandomSeedOffsetKeepsFirstAndLastOrdinal() throws Exception {
        // SEED_MODE_RANDOM (literal NULL seed) draws a fresh random offset per execution, so the
        // full kept-row set varies run to run and can't be asserted exactly. Two invariants always
        // hold regardless of the random offset though: ordinal 0 (the first row) is always kept,
        // and the last row is always pinned (see CadenceFunctionFactory.CadenceFunction#preparePass2).
        // Assert those two deterministic invariants rather than the varying row set.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(10)");
            assertQuery("select count() from (select ts, cadence(3, null) over (order by ts) keep from t) " +
                    "where keep and (ts = '1970-01-01T00:00:00.000001Z' or ts = '1970-01-01T00:00:00.000010Z')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");
        });
    }

    @Test
    public void testRejectsNonConstantStride() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, cadence(v::long) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(20, "stride must be a constant");
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, cadence(3) over (order by ts) from t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [cadence(3) over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }
}
