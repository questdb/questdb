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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ApproxCountDistinctLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        assertQuery("select a, approx_count_distinct(42L) from x order by a")
                .ddl("create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))")
                .expectSize()
                .returns("""
                        a\tapprox_count_distinct
                        a\t1
                        b\t1
                        c\t1
                        """);
    }

    @Test
    public void testDifferentPrecisionsDenseHLL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select * from (select rnd_long(1, 1000000, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(1000000)) timestamp(ts))");

            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count_distinct
                            631858
                            """);

            long[] expectedEstimates = new long[]{
                    564553L,
                    557258L,
                    778781L,
                    673573L,
                    648520L,
                    615982L,
                    617525L,
                    612068L,
                    622847L,
                    627491L,
                    637899L,
                    636435L,
                    633988L,
                    632245L,
                    632460L
            };
            for (int precision = 4; precision <= 18; precision++) {
                assertQuery("select approx_count_distinct(s, " + precision + ") as approx_count_distinct" + precision + " from x")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("approx_count_distinct" + precision + "\n" +
                                expectedEstimates[precision - 4] + "\n");
            }
        });
    }

    @Test
    public void testDifferentPrecisionsSparseHLL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select * from (select rnd_long(1, 6, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))");

            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count_distinct
                            6
                            """);

            long[] expectedEstimates = new long[]{
                    5L, 5L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L
            };
            for (int precision = 4; precision <= 18; precision++) {
                assertQuery("select approx_count_distinct(s, " + precision + ") as approx_count_distinct" + precision + " from x")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("approx_count_distinct" + precision + "\n" +
                                expectedEstimates[precision - 4] + "\n");
            }
        });
    }

    @Test
    public void testExpression() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    a\tapprox_count_distinct
                    a\t4
                    b\t4
                    c\t4
                    """;
            assertQuery("select a, approx_count_distinct(s * 42) from x order by a")
                    .noLeakCheck()
                    .ddl("create table x as (select * from (select rnd_symbol('a','b','c') a, rnd_long(1, 8, 0) s from long_sequence(20)))")
                    .expectSize()
                    .returns(expected);
            // multiplication shouldn't affect the number of distinct values,
            // so the result should stay the same
            assertQuery("select a, approx_count_distinct(s) from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testGroupKeyedDenseHLL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_long(0, 100000, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(1000000)" +
                    ") timestamp(ts))");
            assertQuery("select a, count_distinct(s) from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tcount_distinct
                            a\t80933
                            b\t81171
                            c\t81089
                            d\t81362
                            e\t81187
                            f\t81314
                            """);
            assertQuery("select a, approx_count_distinct(s) from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tapprox_count_distinct
                            a\t82605
                            b\t82054
                            c\t81363
                            d\t81954
                            e\t82211
                            f\t81957
                            """);
        });
    }

    @Test
    public void testGroupKeyedSparseHLL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_long(0, 16, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(20)" +
                    ") timestamp(ts))");
            assertQuery("select a, count_distinct(s) from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tcount_distinct
                            a\t2
                            b\t1
                            c\t1
                            d\t4
                            e\t4
                            f\t3
                            """);
            assertQuery("select a, approx_count_distinct(s) from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tapprox_count_distinct
                            a\t2
                            b\t1
                            c\t1
                            d\t4
                            e\t4
                            f\t3
                            """);
        });
    }

    @Test
    public void testGroupNotKeyedDenseHLL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select * from (select rnd_long(1, 1000000, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(1000000)) timestamp(ts))");
            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count_distinct
                            631858
                            """);
            assertQuery("select approx_count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            approx_count_distinct
                            637899
                            """);
        });
    }

    @Test
    public void testGroupNotKeyedSparseHLL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select * from (select rnd_long(1, 6, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))");
            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count_distinct
                            6
                            """);
            assertQuery("select approx_count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            approx_count_distinct
                            6
                            """);
        });
    }

    @Test
    public void testGroupNotKeyedWithNullsDenseHLL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select * from (select rnd_long(1, 1000000, 0) s, timestamp_sequence(10, 100000) ts from long_sequence(1000000)) timestamp(ts)" +
                    ") timestamp(ts) PARTITION BY YEAR");
            String expectedExact = """
                    count_distinct
                    631858
                    """;
            String expectedEstimated = """
                    approx_count_distinct
                    637899
                    """;

            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedExact);
            assertQuery("select approx_count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedEstimated);

            execute("insert into x values(cast(null as LONG), '2021-05-21')");
            execute("insert into x values(cast(null as LONG), '1970-01-01')");
            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedExact);
            assertQuery("select approx_count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedEstimated);
        });
    }

    @Test
    public void testGroupNotKeyedWithNullsSparseHLL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select * from (select rnd_long(1, 6, 0) s, timestamp_sequence(10, 100000) ts from long_sequence(100)) timestamp(ts)" +
                    ") timestamp(ts) PARTITION BY YEAR");
            String expectedExact = """
                    count_distinct
                    6
                    """;
            String expectedEstimated = """
                    approx_count_distinct
                    6
                    """;

            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedExact);
            assertQuery("select approx_count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedEstimated);

            execute("insert into x values(cast(null as LONG), '2021-05-21')");
            execute("insert into x values(cast(null as LONG), '1970-01-01')");
            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedExact);
            assertQuery("select approx_count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedEstimated);
        });
    }

    @Test
    public void testInterpolation() throws Exception {
        assertQuery("select ts, approx_count_distinct(s) from x sample by 1s fill(linear) limit 2")
                .ddl("create table x as (select * from (select rnd_long(0, 16, 0) s, timestamp_sequence(0, 60000000) ts from long_sequence(100)) timestamp(ts))")
                .timestamp("ts")
                .returns("""
                        ts\tapprox_count_distinct
                        1970-01-01T00:00:00.000000Z\t1
                        1970-01-01T00:00:01.000000Z\t1
                        """);
    }

    @Test
    public void testNoValues() throws Exception {
        assertQuery("select approx_count_distinct(a) from x")
                .ddl("create table x (a long)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        approx_count_distinct
                        0
                        """);
    }

    @Test
    public void testNullConstant() throws Exception {
        assertQuery("select a, approx_count_distinct(cast(null as LONG)) from x order by a")
                .ddl("create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))")
                .expectSize()
                .returns("""
                        a\tapprox_count_distinct
                        a\t0
                        b\t0
                        c\t0
                        """);
    }

    @Test
    public void testPrecisionOutOfRange() throws Exception {
        assertQuery("select approx_count_distinct(x, 3) from long_sequence(1)")
                .fails(7, "precision must be between 4 and 18");
        assertQuery("select approx_count_distinct(x, 19) from long_sequence(1)")
                .fails(7, "precision must be between 4 and 18");
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        assertQuery("select ts, approx_count_distinct(s) from x sample by 1s fill(linear)")
                .ddl("create table x as (select * from (select rnd_long(0, 16, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))")
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tapprox_count_distinct
                        1970-01-01T00:00:00.000000Z\t9
                        1970-01-01T00:00:01.000000Z\t7
                        1970-01-01T00:00:02.000000Z\t7
                        1970-01-01T00:00:03.000000Z\t8
                        1970-01-01T00:00:04.000000Z\t8
                        1970-01-01T00:00:05.000000Z\t8
                        1970-01-01T00:00:06.000000Z\t7
                        1970-01-01T00:00:07.000000Z\t8
                        1970-01-01T00:00:08.000000Z\t7
                        1970-01-01T00:00:09.000000Z\t9
                        """);
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertMemoryLeak(() -> assertQuery("with x as (select * from (select rnd_long(1, 8, 0) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(100)) timestamp(ts))\n" +
                        "select ts, approx_count_distinct(s) from x sample by 2s align to first observation")
                .noLeakCheck()
                .timestamp("ts")
                .noRandomAccess()
                .returns("""
                        ts\tapprox_count_distinct
                        1970-01-01T00:00:00.050000Z\t8
                        1970-01-01T00:00:02.050000Z\t8
                        """));
    }

    @Test
    public void testSampleFillValue() throws Exception {
        assertQuery("select ts, approx_count_distinct(s) from x sample by 1s fill(99)")
                .ddl("create table x as (select * from (select rnd_long(0, 8, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))")
                .timestamp("ts")
                .noRandomAccess()
                .returns("""
                        ts\tapprox_count_distinct
                        1970-01-01T00:00:00.000000Z\t5
                        1970-01-01T00:00:01.000000Z\t8
                        1970-01-01T00:00:02.000000Z\t6
                        1970-01-01T00:00:03.000000Z\t7
                        1970-01-01T00:00:04.000000Z\t6
                        1970-01-01T00:00:05.000000Z\t5
                        1970-01-01T00:00:06.000000Z\t6
                        1970-01-01T00:00:07.000000Z\t6
                        1970-01-01T00:00:08.000000Z\t6
                        1970-01-01T00:00:09.000000Z\t7
                        """);
    }

    @Test
    public void testSampleKeyed() throws Exception {
        assertQuery("select a, approx_count_distinct(s), ts from x sample by 5s align to first observation")
                .ddl("create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_long(0, 12, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))")
                .timestamp("ts")
                .noRandomAccess()
                .returns("""
                        a\tapprox_count_distinct\tts
                        a\t4\t1970-01-01T00:00:00.000000Z
                        f\t9\t1970-01-01T00:00:00.000000Z
                        c\t8\t1970-01-01T00:00:00.000000Z
                        e\t4\t1970-01-01T00:00:00.000000Z
                        d\t6\t1970-01-01T00:00:00.000000Z
                        b\t6\t1970-01-01T00:00:00.000000Z
                        b\t5\t1970-01-01T00:00:05.000000Z
                        c\t4\t1970-01-01T00:00:05.000000Z
                        f\t7\t1970-01-01T00:00:05.000000Z
                        e\t6\t1970-01-01T00:00:05.000000Z
                        d\t8\t1970-01-01T00:00:05.000000Z
                        a\t5\t1970-01-01T00:00:05.000000Z
                        """);
    }
}
