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

public class M4WindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testKeepsAllWhenFewRows() throws Exception {
        // n=3, target=8 -> numBuckets=2, but few rows: M4Algorithm.select naturally keeps all of them
        // (first/min/max/last collapse onto the same 3 points across the 2 buckets).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0)");
            assertQuery("select ts, v, m4(ts, v, 8) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t10.0\ttrue
                            1970-01-01T00:00:00.000002Z\t20.0\ttrue
                            1970-01-01T00:00:00.000003Z\t30.0\ttrue
                            """);
        });
    }

    @Test
    public void testMatchesM4AlgorithmOnSpike() throws Exception {
        // Deterministic spike; keep first/min/max/last per time bucket. Expected output filled from
        // the FIRST (stable) run and cross-checked against the old-cursor
        // "SELECT ts, v FROM t SUBSAMPLE m4(v, 8) ORDER BY ts" on the same dataset (byte-for-byte match).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, case when x%5=0 then 100.0 else x end from long_sequence(20)");
            assertQuery("select ts, v from (select ts, v, m4(ts, v, 8) over (order by ts) keep from t) where keep")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize(false)
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t1.0
                            1970-01-01T00:00:00.000005Z\t100.0
                            1970-01-01T00:00:00.000009Z\t9.0
                            1970-01-01T00:00:00.000010Z\t100.0
                            1970-01-01T00:00:00.000011Z\t11.0
                            1970-01-01T00:00:00.000020Z\t100.0
                            """);
        });
    }

    @Test
    public void testRejectsNonNumericValue() throws Exception {
        // SYMBOL is not implicitly castable to DOUBLE, so the overload resolver itself rejects this
        // before newInstance() ever runs (it never gets a chance to raise our own "numeric column
        // expected" message here) - matching the FunctionParser's own type-mismatch diagnostic is the
        // correct outcome for a genuinely non-numeric argument type on a real function call, unlike
        // the bespoke SUBSAMPLE grammar in SqlCodeGenerator.generateSubsample, which has no signature
        // resolution to lean on and does this check by hand.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol) timestamp(ts)");
            assertQuery("select ts, m4(ts, s, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(21, "argument type mismatch");
        });
    }

    @Test
    public void testRejectsNonNumericValueThatOverloadResolutionLetsThrough() throws Exception {
        // CHAR *is* implicitly widenable to DOUBLE per ColumnType's overload rules (it reaches
        // newInstance() as a fuzzy-match candidate), so this is the case that actually exercises our
        // manual numeric-type guard and its SUBSAMPLE-cursor-matching message.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, c char) timestamp(ts)");
            assertQuery("select ts, m4(ts, c, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(18, "numeric column expected, got: CHAR");
        });
    }

    @Test
    public void testRejectsNonConstantTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, m4(ts, v, v::long) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(22, "target must be a constant");
        });
    }

    @Test
    public void testRejectsTargetBelowTwo() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, m4(ts, v, 1) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(21, "target points must be at least 2");
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, m4(ts, v, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [m4(ts,v,8) over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }
}
