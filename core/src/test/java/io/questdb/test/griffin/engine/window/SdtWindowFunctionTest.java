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
}
