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
import io.questdb.test.tools.TestUtils;
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
    public void testKeepsAllRowsWhenCountAtTargetEvenIfBucketingWouldDrop() throws Exception {
        // Distinguishing case for the count <= target keep-all short-circuit: 4 monotonically
        // increasing rows with target=4 -> numBuckets=1 (single bucket over all rows). Bucketing
        // would collapse first=min=row0 and last=max=row3 to just {0,3}, DROPPING the two interior
        // rows (pre-fix window output: true,false,false,true). But bufferCount(4) <= target(4), so
        // the old SUBSAMPLE cursor's selectAll() keeps every row - m4() must match, keeping all four.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0),(4::timestamp,40.0)");
            assertQuery("select ts, v, m4(ts, v, 4) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t10.0\ttrue
                            1970-01-01T00:00:00.000002Z\t20.0\ttrue
                            1970-01-01T00:00:00.000003Z\t30.0\ttrue
                            1970-01-01T00:00:00.000004Z\t40.0\ttrue
                            """);
            // Byte-identical to the old SUBSAMPLE cursor on the same data (its selectAll() path).
            printSql("select ts, v from t SUBSAMPLE m4(v, 4)");
            TestUtils.assertEquals("""
                    ts\tv
                    1970-01-01T00:00:00.000001Z\t10.0
                    1970-01-01T00:00:00.000002Z\t20.0
                    1970-01-01T00:00:00.000003Z\t30.0
                    1970-01-01T00:00:00.000004Z\t40.0
                    """, sink);
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
    public void testFiltersNullAndNaNRows() throws Exception {
        // A NULL/NaN value must not poison a bucket's min/max the way an unfiltered scan would:
        // M4Algorithm.select seeds a bucket's min/max from the first row it sees, and NaN
        // comparisons are always false, so if that seed row is NaN the real min/max in the
        // bucket would never be detected - nor would the null row itself ever be excluded from
        // first/last. The old SUBSAMPLE cursor (SubsampleRecordCursorFactory.bufferInput())
        // drops NULL ts / null-or-NaN value rows before bucketing; m4() must match it exactly.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, null),
                    (2::timestamp, 5.0),
                    (3::timestamp, 100.0),
                    (4::timestamp, 1.0)
                    """);
            // Single bucket (target=4 -> numBuckets=1). With the null row dropped: first=5.0@ts2,
            // min=1.0@ts4, max=100.0@ts3, last=1.0@ts4 - all three non-null rows are kept and the
            // null row is excluded outright (an unfiltered scan would instead seed min/max on the
            // null row at ts1 and never recover, wrongly keeping ts1 and dropping ts2/ts3).
            assertQuery("select ts, v, m4(ts, v, 4) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\tnull\tfalse
                            1970-01-01T00:00:00.000002Z\t5.0\ttrue
                            1970-01-01T00:00:00.000003Z\t100.0\ttrue
                            1970-01-01T00:00:00.000004Z\t1.0\ttrue
                            """);

            // Byte-identical to the old SUBSAMPLE cursor on the same data: same rows kept.
            // Plain printSql/assertEquals (not the fluent assertQuery battery) - the SUBSAMPLE
            // cursor's recordCursorSupportsRandomAccess()/getRecordB() combination doesn't fit
            // assertQuery's noRandomAccess() expectations, same reason SubsampleTest.java uses a
            // bespoke assertSql helper instead of assertQuery for its own SUBSAMPLE assertions.
            printSql("select ts, v from t SUBSAMPLE m4(v, 4)");
            TestUtils.assertEquals("""
                    ts\tv
                    1970-01-01T00:00:00.000002Z\t5.0
                    1970-01-01T00:00:00.000003Z\t100.0
                    1970-01-01T00:00:00.000004Z\t1.0
                    """, sink);
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
