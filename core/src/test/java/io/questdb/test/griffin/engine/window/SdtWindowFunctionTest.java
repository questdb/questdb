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

public class SdtWindowFunctionTest extends AbstractCairoTest {

    private static final String DDL = "create table tab (ts timestamp, val double) timestamp(ts)";

    @Test
    public void testRejectsNegativeCompdev() throws Exception {
        assertQuery("select ts, sdt(ts, val, -1.0) over (order by ts) from tab")
                .ddl(DDL)
                .fails(24, "compdev must be a non-negative finite constant"); // position of the compdev arg (verified against actual)
    }

    @Test
    public void testRejectsNanCompdev() throws Exception {
        assertQuery("select ts, sdt(ts, val, cast('NaN' as double)) over (order by ts) from tab")
                .ddl(DDL)
                .fails(24, "compdev must be a non-negative finite constant");
    }

    @Test
    public void testRejectsNonConstantCompdev() throws Exception {
        // The signature's 3rd slot ('d', lowercase = constant-required) makes the parser itself
        // reject a non-constant argument before our factory's newInstance ever runs; see the
        // deviation note in task-2-report.md.
        assertQuery("select ts, sdt(ts, val, val) over (order by ts) from tab")
                .ddl(DDL)
                .fails(24, "expected: DOUBLE constant, actual: DOUBLE");
    }

    @Test
    public void testRequiresOrderBy() throws Exception {
        assertQuery("select ts, sdt(ts, val, 0.5) over () from tab")
                .ddl(DDL)
                .fails(11, "sdt() requires ORDER BY");
    }

    @Test
    public void testRejectsFraming() throws Exception {
        assertQuery("select ts, sdt(ts, val, 0.5) over (order by ts rows between 1 preceding and current row) from tab")
                .ddl(DDL)
                .fails(11, "sdt() does not support framing; remove ROWS/RANGE clause");
    }

    @Test
    public void testMonotonicRampKeepsEndpoints() throws Exception {
        assertQuery("select ts, val, sdt(ts, val, 0.5) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab select x::timestamp, x from long_sequence(5)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t1.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t2.0\tfalse\n" +
                                "1970-01-01T00:00:00.000003Z\t3.0\tfalse\n" +
                                "1970-01-01T00:00:00.000004Z\t4.0\tfalse\n" +
                                "1970-01-01T00:00:00.000005Z\t5.0\ttrue\n"
                );
    }

    @Test
    public void testWithinBandNoiseCompresses() throws Exception {
        assertQuery("select ts, val, sdt(ts, val, 0.5) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,0.0),(2::timestamp,0.1),(3::timestamp,0.0),(4::timestamp,0.1),(5::timestamp,0.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t0.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t0.1\tfalse\n" +
                                "1970-01-01T00:00:00.000003Z\t0.0\tfalse\n" +
                                "1970-01-01T00:00:00.000004Z\t0.1\tfalse\n" +
                                "1970-01-01T00:00:00.000005Z\t0.0\ttrue\n"
                );
    }

    @Test
    public void testFilteringYieldsCompressedSet() throws Exception {
        assertQuery("select ts, val from (select ts, val, sdt(ts, val, 0.5) over (order by ts) keep from tab) where keep")
                .ddl(DDL, "insert into tab select x::timestamp, x from long_sequence(5)")
                .timestamp("ts")
                .returns(
                        "ts\tval\n" +
                                "1970-01-01T00:00:00.000001Z\t1.0\n" +
                                "1970-01-01T00:00:00.000005Z\t5.0\n"
                );
    }

    @Test
    public void testPartitionsAreIndependent() throws Exception {
        // two interleaved series, each a clean ramp -> each keeps its own endpoints
        assertQuery("select ts, sym, val, sdt(ts, val, 0.5) over (partition by sym order by ts) keep from tab")
                .ddl("create table tab (ts timestamp, sym symbol, val double) timestamp(ts)",
                        "insert into tab values " +
                                "(1::timestamp,'a',1.0),(2::timestamp,'b',10.0)," +
                                "(3::timestamp,'a',2.0),(4::timestamp,'b',20.0)," +
                                "(5::timestamp,'a',3.0),(6::timestamp,'b',30.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tsym\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\tb\t10.0\ttrue\n" +
                                "1970-01-01T00:00:00.000003Z\ta\t2.0\tfalse\n" +
                                "1970-01-01T00:00:00.000004Z\tb\t20.0\tfalse\n" +
                                "1970-01-01T00:00:00.000005Z\ta\t3.0\ttrue\n" +
                                "1970-01-01T00:00:00.000006Z\tb\t30.0\ttrue\n"
                );
    }

    @Test
    public void testRespectNullsFlushesLastPointBeforeGap() throws Exception {
        // A null forces a kept boundary and resets the series; the last real
        // sample before the gap is flushed (kept), only the interior 0 drops.
        assertQuery("select ts, val, sdt(ts, val, 0.5) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,0.0),(2::timestamp,0.0),(3::timestamp,0.0),(4::timestamp,null)," +
                        "(5::timestamp,5.0),(6::timestamp,5.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t0.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t0.0\tfalse\n" +
                                "1970-01-01T00:00:00.000003Z\t0.0\ttrue\n" +
                                "1970-01-01T00:00:00.000004Z\tnull\ttrue\n" +
                                "1970-01-01T00:00:00.000005Z\t5.0\ttrue\n" +
                                "1970-01-01T00:00:00.000006Z\t5.0\ttrue\n"
                );
    }

    @Test
    public void testIgnoreNullsSkipsNull() throws Exception {
        assertQuery("select ts, val, sdt(ts, val, 0.5) ignore nulls over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,0.0),(2::timestamp,0.0),(3::timestamp,null)," +
                        "(4::timestamp,0.0),(5::timestamp,0.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t0.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t0.0\tfalse\n" +
                                "1970-01-01T00:00:00.000003Z\tnull\tfalse\n" +
                                "1970-01-01T00:00:00.000004Z\t0.0\tfalse\n" +
                                "1970-01-01T00:00:00.000005Z\t0.0\ttrue\n"
                );
    }

    @Test
    public void testExplainPlanShowsSdt() throws Exception {
        assertQuery("select ts, sym, sdt(ts, val, 0.5) over (partition by sym order by ts) from tab")
                .ddl("create table tab (ts timestamp, sym symbol, val double) timestamp(ts)")
                .noLeakCheck()
                .assertsPlan("CachedWindowLight\n" +
                        "  unorderedFunctions: [sdt(ts, val, 0.5) over (partition by [sym] order by [ts])]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testPartitionedStatefulTimestampArgInitializedAndClosed() throws Exception {
        // Same regression as testStatefulTimestampArgInitializedAndClosed, for SdtOverPartitionFunction.
        assertQuery("select id from (select id, sdt(json_extract(j, '$.x')::timestamp, val, 0.0) over (partition by sym order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, sym symbol, j varchar, val double, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 'a', '{"x":"2024-01-01T00:00:00.000000Z"}', 0.0, '2024-01-01T00:00:00.000000Z'),
                                (1, 'b', '{"x":"2024-01-01T00:00:01.000000Z"}', 0.0, '2024-01-01T00:00:01.000000Z'),
                                (2, 'a', '{"x":"2024-01-01T00:00:02.000000Z"}', 0.0, '2024-01-01T00:00:02.000000Z'),
                                (3, 'b', '{"x":"2024-01-01T00:00:03.000000Z"}', 0.0, '2024-01-01T00:00:03.000000Z'),
                                (4, 'a', '{"x":"2024-01-01T00:00:04.000000Z"}', 0.0, '2024-01-01T00:00:04.000000Z'),
                                (5, 'b', '{"x":"2024-01-01T00:00:05.000000Z"}', 0.0, '2024-01-01T00:00:05.000000Z')""")
                .returns("""
                        id
                        0
                        1
                        4
                        5
                        """);
    }

    @Test
    public void testPartitionedBackwardTsArgAboveAnchorIsABoundary() throws Exception {
        // Regression: a backward step in the ts argument that stays ABOVE the partition's
        // current anchor is a series boundary (endpoint before it stays flushed, boundary row
        // re-anchors), same as the below-anchor step. Partitions interleave so the swinging-door
        // state - including pendingTs, which the boundary guard reads - round-trips through the
        // per-partition map between the rows of each series. Partition 'a' takes the backward
        // step (all four rows are two-point-segment endpoints); flat monotonic partition 'b'
        // keeps only its endpoints, proving interior compression still works alongside.
        assertQuery("select id from (select id, sdt(ats, val, 0.5) over (partition by sym order by ts) keep from tab) where keep order by id")
                .ddl("create table tab (id int, sym symbol, ats timestamp, val double, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 'a', 0::timestamp, 0.0, 1::timestamp),
                                (1, 'b', 0::timestamp, 0.0, 2::timestamp),
                                (2, 'a', 5000::timestamp, 0.0, 3::timestamp),
                                (3, 'b', 1000::timestamp, 0.0, 4::timestamp),
                                (4, 'a', 3000::timestamp, 0.0, 5::timestamp),
                                (5, 'b', 2000::timestamp, 0.0, 6::timestamp),
                                (6, 'a', 4000::timestamp, 0.0, 7::timestamp),
                                (7, 'b', 3000::timestamp, 0.0, 8::timestamp)""")
                .returns("""
                        id
                        0
                        1
                        2
                        4
                        6
                        7
                        """);
    }

    @Test
    public void testStatefulTimestampArgInitializedAndClosed() throws Exception {
        // Regression: BaseWindowFunction inits/frees only the value arg, so sdt must handle tsArg
        // itself. json_extract builds its native JSON pointer in init() and frees it in close();
        // without init() every read returns null, every row becomes a hard boundary (all rows
        // survive the filter), and without close() the native state leaks (fails the leak check).
        assertQuery("select id from (select id, sdt(json_extract(j, '$.x')::timestamp, val, 0.0) over (order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, j varchar, val double, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, '{"x":"2024-01-01T00:00:00.000000Z"}', 0.0, '2024-01-01T00:00:00.000000Z'),
                                (1, '{"x":"2024-01-01T00:00:01.000000Z"}', 0.0, '2024-01-01T00:00:01.000000Z'),
                                (2, '{"x":"2024-01-01T00:00:02.000000Z"}', 0.0, '2024-01-01T00:00:02.000000Z')""")
                .returns("""
                        id
                        0
                        2
                        """);
    }

    @Test
    public void testNullTimestampArgIsABoundaryUnderRespectNulls() throws Exception {
        // The timestamp argument is any TIMESTAMP expression, not the designated timestamp, so
        // it can be NULL. Such a row has no position on the time axis and cannot join a
        // corridor; RESPECT NULLS keeps it as a boundary and starts a new series after it.
        assertQuery("select id from (select id, sdt(ats, val, 0.0) over (order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, val double, ats timestamp, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 0.0,  '2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                                (1, 0.0,  null,                          '2024-01-01T00:00:01.000000Z'),
                                (2, 0.0,  '2024-01-01T00:00:02.000000Z', '2024-01-01T00:00:02.000000Z'),
                                (3, 10.0, '2024-01-01T00:00:03.000000Z', '2024-01-01T00:00:03.000000Z'),
                                (4, 20.0, '2024-01-01T00:00:04.000000Z', '2024-01-01T00:00:04.000000Z'),
                                (5, 30.0, '2024-01-01T00:00:05.000000Z', '2024-01-01T00:00:05.000000Z')""")
                .returns("""
                        id
                        0
                        1
                        2
                        5
                        """);
    }

    @Test
    public void testNullTimestampArgSkippedUnderIgnoreNulls() throws Exception {
        // IGNORE NULLS drops the row outright and leaves the corridor untouched, so the series
        // spans the gap: 0,0,0 is flat, then the 10/20/30 ramp keeps only its endpoints.
        assertQuery("select id from (select id, sdt(ats, val, 0.0) ignore nulls over (order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, val double, ats timestamp, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 0.0,  '2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                                (1, 0.0,  null,                          '2024-01-01T00:00:01.000000Z'),
                                (2, 0.0,  '2024-01-01T00:00:02.000000Z', '2024-01-01T00:00:02.000000Z'),
                                (3, 10.0, '2024-01-01T00:00:03.000000Z', '2024-01-01T00:00:03.000000Z'),
                                (4, 20.0, '2024-01-01T00:00:04.000000Z', '2024-01-01T00:00:04.000000Z'),
                                (5, 30.0, '2024-01-01T00:00:05.000000Z', '2024-01-01T00:00:05.000000Z')""")
                .returns("""
                        id
                        0
                        2
                        5
                        """);
    }

    @Test
    public void testNanosBackwardJumpWiderThanLongMaxIsABoundary() throws Exception {
        // No NULLs: a long holds only 292 years of nanoseconds, so the 2100 -> 1700 step is a
        // backward span wider than Long.MAX. The subtraction wraps positive and reads as a
        // forward step, and the flat corridor then drops row 1 as interior.
        assertQuery("select id from (select id, sdt(ats, val, 0.0) over (order by ts) keep from tab) where keep")
                .ddl("create table tab (id int, val double, ats timestamp_ns, ts timestamp) timestamp(ts)",
                        """
                                insert into tab values
                                (0, 0.0, '2100-01-01T00:00:00.000000000Z', '2024-01-01T00:00:00.000000Z'),
                                (1, 0.0, '1700-01-01T00:00:00.000000000Z', '2024-01-01T00:00:01.000000Z'),
                                (2, 0.0, '2150-01-01T00:00:00.000000000Z', '2024-01-01T00:00:02.000000Z')""")
                .returns("""
                        id
                        0
                        1
                        2
                        """);
    }

    @Test
    public void testPartitionedSingleRowPerPartitionKept() throws Exception {
        assertQuery("select ts, sym, val, sdt(ts, val, 0.5) over (partition by sym order by ts) keep from tab")
                .ddl("create table tab (ts timestamp, sym symbol, val double) timestamp(ts)",
                        "insert into tab values (1::timestamp,'a',5.0),(2::timestamp,'b',9.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tsym\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\ta\t5.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\tb\t9.0\ttrue\n"
                );
    }

    @Test
    public void testHugeMagnitudeChangedPointIsKept() throws Exception {
        // F3-SDT-OVERFLOW red test: (1e308 + 0.0) - (-1e308) overflows to +Inf inside
        // SwingingDoor's slope terms, so rows 2 and 3 read the same +Inf slope and the corridor
        // wrongly drops row 2 as interior. All inputs are finite and compdev is 0, so any value
        // change must be kept: the hand-derived keep set is all three rows (true slopes from the
        // anchor are 2e308 at dt=1 vs 1e308 at dt=2 - not collinear).
        assertQuery("select ts, val, sdt(ts, val, 0.0) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,-1e308),(2::timestamp,1e308),(3::timestamp,1e308)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t-1.0E308\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t1.0E308\ttrue\n" +
                                "1970-01-01T00:00:00.000003Z\t1.0E308\ttrue\n"
                );
    }

    @Test
    public void testScaledProbeSeriesKeepsAllPoints() throws Exception {
        // F3-SDT-OVERFLOW preservation control (green pre-fix, must stay green): the same shape
        // at magnitude 1 has finite slopes (2 then 1), the doors cross and all rows are kept
        assertQuery("select ts, val, sdt(ts, val, 0.0) over (order by ts) keep from tab")
                .ddl(DDL, "insert into tab values " +
                        "(1::timestamp,-1.0),(2::timestamp,1.0),(3::timestamp,1.0)")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "ts\tval\tkeep\n" +
                                "1970-01-01T00:00:00.000001Z\t-1.0\ttrue\n" +
                                "1970-01-01T00:00:00.000002Z\t1.0\ttrue\n" +
                                "1970-01-01T00:00:00.000003Z\t1.0\ttrue\n"
                );
    }

    @Test
    public void testLongValueGoesThroughImplicitDoubleCast() throws Exception {
        // F2-M4-LONG preservation control (green pre-fix, must stay green): sdt does NOT share
        // BucketSelectWindowFunction's buffer. Its signature is sdt(NDd) - the value slot is
        // DOUBLE - so a LONG column reaches the function through the parser's implicit
        // LONG -> DOUBLE cast, the same SQL-level semantics as writing v::double. Its compdev
        // tolerance is itself a double, so sdt's compression contract is double-domain by
        // construction: 2^53 and 2^53 + 1 are the same double, the series is a flat line within
        // any band, and only the endpoints are kept. This pins that contract; the
        // integral-exactness repair to minmax/m4 must not alter sdt.
        assertQuery("select ts, v, sdt(ts, v, 0.5) over (order by ts) keep from t")
                .ddl("create table t (ts timestamp, v long) timestamp(ts)",
                        """
                                insert into t values
                                (1::timestamp, 9_007_199_254_740_992),
                                (2::timestamp, 9_007_199_254_740_993),
                                (3::timestamp, 9_007_199_254_740_992)
                                """)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tv\tkeep
                        1970-01-01T00:00:00.000001Z\t9007199254740992\ttrue
                        1970-01-01T00:00:00.000002Z\t9007199254740993\tfalse
                        1970-01-01T00:00:00.000003Z\t9007199254740992\ttrue
                        """);
    }
}
