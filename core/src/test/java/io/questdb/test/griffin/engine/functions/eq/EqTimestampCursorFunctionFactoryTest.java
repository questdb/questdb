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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqTimestampCursorFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testMultiRowCursorFails() throws Exception {
        // a scalar sub-query yielding more than one row is an error, reported at the sub-query position
        assertMemoryLeak(() -> {
            execute("create table x as (select timestamp_sequence(0, 2500000) ts from long_sequence(2))");
            // timestamp cursor column
            assertQuery("select * from x where ts = (select ts from x)")
                    .fails(28, "scalar sub-query returned more than one row");
            // string cursor column
            assertQuery("select * from x where ts = (select '1970-01-01' from x)")
                    .fails(28, "scalar sub-query returned more than one row");
            // varchar cursor column (separately implemented reader)
            assertQuery("select * from x where ts = (select '1970-01-01'::varchar from x)")
                    .fails(28, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testMultiRowCursorFailsOnDesignatedTimestamp() throws Exception {
        // On a designated-timestamp column, ts = (select ...) is extracted as an interval intrinsic and
        // evaluated by RuntimeIntervalModel instead of the cursor-comparison factory. A scalar sub-query
        // must still reject more than one row here rather than silently taking an arbitrary first row,
        // and the error must point at the offending sub-query, same as the factory path does.
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select timestamp_sequence(0, 2500000) ts from long_sequence(5)" +
                    ") timestamp(ts) partition by day");
            assertQuery("select * from x where ts = (select ts from x limit 2)")
                    .fails(28, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testSubQueryBetweenEndpointsAreRejectedByParser() throws Exception {
        // The expression parser forbids sub-queries inside BETWEEN (ExpressionParser rejects any
        // SELECT lambda while betweenCount > 0), so a scalar sub-query can never reach the
        // dynamic BETWEEN endpoint handoff in WhereClauseParser and no runtime multi-row check
        // is reachable there. Pin the rejection for each endpoint independently so a future
        // parser relaxation is forced to add the runtime position coverage.
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select timestamp_sequence(0, 2500000) ts from long_sequence(5)" +
                    ") timestamp(ts) partition by day");
            assertQuery("select * from x where ts between (select ts from x limit 2) and (select max(ts) from x)")
                    .failsWith("constant expected");
            assertQuery("select * from x where ts between (select min(ts) from x) and (select ts from x limit 2)")
                    .failsWith("constant expected");
        });
    }

    @Test
    public void testMultiRowCursorFailsOnDesignatedTimestampNs() throws Exception {
        // The designated TIMESTAMP_NS variant routes ts = (select ...) through the same runtime
        // interval model as the microsecond one; the nanosecond scalar conversion and the
        // multi-row rejection at the sub-query position must hold for that driver too.
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select timestamp_sequence(0, 2500000)::timestamp_ns ts from long_sequence(5)" +
                    ") timestamp(ts) partition by day");
            assertQuery("select * from x where ts = (select ts from x limit 2)")
                    .fails(28, "scalar sub-query returned more than one row");
            // single-row control: the nanosecond scalar converts and selects exactly the max row
            assertQuery("select count() c from x where ts = (select max(ts) from x)")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n1\n");
        });
    }

    @Test
    public void testMultiRowCursorFailsOnDesignatedTimestampNotEquals() throws Exception {
        // Unlike ts = (select ...), the not-equals intrinsic accepts only runtime constants, so a
        // cursor sub-query bypasses the interval model and lands on the negated cursor-comparison
        // factory even on a designated timestamp. The multi-row rejection and its exact sub-query
        // position must hold on that path too.
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select timestamp_sequence(0, 2500000) ts from long_sequence(5)" +
                    ") timestamp(ts) partition by day");
            assertQuery("select * from x where ts != (select ts from x limit 2)")
                    .fails(29, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testMultiRowCursorFailsOnDesignatedTimestampOrUnion() throws Exception {
        // The OR interval analysis accepts only constant and runtime-constant arms, so cursor
        // sub-queries make the whole disjunction fall back to a boolean filter over two
        // cursor-comparison functions. A multi-row scalar sub-query in the second arm must be
        // rejected at that arm's sub-query position on this fallback path.
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select timestamp_sequence(0, 2500000) ts from long_sequence(5)" +
                    ") timestamp(ts) partition by day");
            assertQuery("select * from x where ts = (select min(ts) from x) or ts = (select ts from x limit 2)")
                    .fails(60, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testCompareNanoTimestampWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select rnd_varchar() a, rnd_long(30000000, 80000000000, 1)::timestamp_ns ts from long_sequence(100)" +
                    ")");

            String expected = """
                    a\tts
                    ŸO(OFг\uDBAE\uDD12ɜ|\t
                    1CW\t
                    O=I~\t
                    Ǭ\uDB37\uDC95Q\t
                    鏻Ê띘Ѷ>͓\uDA8B\uDFC4︵Ƀ^\t
                    Mw1$c\t
                    ;PGY=FU[H\t
                    }L1>ML\t
                    {ϸ\uD9F4\uDFB9\uDA0A\uDC7A\uDA76\uDC87>\uD8F0\uDF66Ҫb\uDBB1\uDEA3\t
                    g<~%j\uD9D3\uDCEE;+\t
                    JY)xuaN\t
                    V_M\t
                    뮣݇8Y\t
                    ^嘢\uD952\uDF63^\t
                    D&9L5BBo,O\t
                    Ɉ\uDAB6\uDF33\uDB00\uDF8AϿ˄礏ɍ\uDB2C\uDD55\t
                    ށڥ[<\uDBCD\uDE09\uDB92\uDC69{UVo\t
                    ʫ\uDACE\uDF0Bǟ\t
                    XBl~ݴ\uD8D6\uDD39!\uDAA9\uDF2C֝\t
                    `^$SJm9h-f\t
                    a Zf\t
                    \uD9A8\uDFFBi⟃2\t
                    <#71^jDS9\t
                    ?+$b[Enk\t
                    Wmτ⻱[N`亲\t
                    vCbc&rm{/&\t
                    """;

            assertQuery("select * from x where ts = (select null)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select null::timestamp)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select null::timestamp_ns)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select null::string)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select null::varchar)")
                    .noLeakCheck()
                    .returns(expected);
            // no rows selected in the cursor
            assertQuery("select * from x where ts = (select 1::timestamp from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select 1000::timestamp_ns from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select '11' from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select '11'::varchar from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select 'hello')")
                    .fails(28, "the cursor selected invalid timestamp value: hello");
            assertQuery("select * from x where ts = (select 'hello'::varchar)")
                    .fails(28, "the cursor selected invalid timestamp value: hello");
            assertQuery("select * from x where ts = (select 'hello'::varchar, 10 x)")
                    .fails(28, "select must provide exactly one column");
            assertQuery("select * from x where ts = (select 10 x)")
                    .fails(28, "cannot compare TIMESTAMP and INT");
        });
    }

    @Test
    public void testCompareNanoTimestampWithString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts = (select '1970-01-03T21:26')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            Eڄ篽\uDB3D\uDF6B,ᵨD\uD939\uDF1E\uD8E5\uDCC3\t1970-01-03T21:26:00.000000000Z
                            """);
        });
    }

    @Test
    public void testCompareNanoTimestampWithStringNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts != (select '1970-01-01T00:00:00.000000Z')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000000Z
                            """);
        });
    }

    @Test
    public void testCompareNanoTimestampWithTimestampNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts != (select max(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000000Z
                            """);

            assertQuery("select * from x where ts != (select max(ts)::timestamp from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testCompareNanoTimestampWithVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts = (select '1970-01-03T20:14'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            \uD8F9\uDFFC\uD8D2\uDE52p\t1970-01-03T20:14:00.000000000Z
                            """);
        });
    }

    @Test
    public void testCompareNanoTimestampWithVarcharFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts = (select ts::varchar from x limit 1)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            Async Filter workers: 1
                              filter: ts=cursor\s
                                Limit value: 1
                                    VirtualRecord
                                      functions: [ts::varchar]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """)
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testCompareNanoTimestampWithVarcharNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts != (select '1970-01-01T00:00:00.000000000Z'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampNanoWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts = (select max(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            Qd%ǧ\t1970-01-03T21:26:37.500000000Z
                            """);

            assertQuery("select * from x where ts = (select max(ts)::timestamp from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            Qd%ǧ\t1970-01-03T21:26:37.500000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select rnd_varchar() a, rnd_long(30000, 80000000, 1)::timestamp ts from long_sequence(100)" +
                    ")");

            String expected = """
                    a\tts
                    ŸO(OFг\uDBAE\uDD12ɜ|\t
                    1CW\t
                    O=I~\t
                    Ǭ\uDB37\uDC95Q\t
                    鏻Ê띘Ѷ>͓\uDA8B\uDFC4︵Ƀ^\t
                    Mw1$c\t
                    ;PGY=FU[H\t
                    }L1>ML\t
                    {ϸ\uD9F4\uDFB9\uDA0A\uDC7A\uDA76\uDC87>\uD8F0\uDF66Ҫb\uDBB1\uDEA3\t
                    g<~%j\uD9D3\uDCEE;+\t
                    JY)xuaN\t
                    V_M\t
                    뮣݇8Y\t
                    ^嘢\uD952\uDF63^\t
                    D&9L5BBo,O\t
                    Ɉ\uDAB6\uDF33\uDB00\uDF8AϿ˄礏ɍ\uDB2C\uDD55\t
                    ށڥ[<\uDBCD\uDE09\uDB92\uDC69{UVo\t
                    ʫ\uDACE\uDF0Bǟ\t
                    XBl~ݴ\uD8D6\uDD39!\uDAA9\uDF2C֝\t
                    `^$SJm9h-f\t
                    a Zf\t
                    \uD9A8\uDFFBi⟃2\t
                    <#71^jDS9\t
                    ?+$b[Enk\t
                    Wmτ⻱[N`亲\t
                    vCbc&rm{/&\t
                    """;

            assertQuery("select * from x where ts = (select null)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select null::timestamp)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select null::timestamp_ns)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select null::string)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select null::varchar)")
                    .noLeakCheck()
                    .returns(expected);
            // no rows selected in the cursor
            assertQuery("select * from x where ts = (select 1::timestamp from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select 1000::timestamp_ns from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select '11' from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select '11'::varchar from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts = (select 'hello')")
                    .fails(28, "the cursor selected invalid timestamp value: hello");
            assertQuery("select * from x where ts = (select 'hello'::varchar)")
                    .fails(28, "the cursor selected invalid timestamp value: hello");
            assertQuery("select * from x where ts = (select 'hello'::varchar, 10 x)")
                    .fails(28, "select must provide exactly one column");
            assertQuery("select * from x where ts = (select 10 x)")
                    .fails(28, "cannot compare TIMESTAMP and INT");
        });
    }

    @Test
    public void testCompareTimestampWithString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts = (select '1970-01-03T21:26')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            Eڄ篽\uDB3D\uDF6B,ᵨD\uD939\uDF1E\uD8E5\uDCC3\t1970-01-03T21:26:00.000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithStringNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts != (select '1970-01-01T00:00:00.000000000Z')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts = (select max(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            Qd%ǧ\t1970-01-03T21:26:37.500000Z
                            """);

            assertQuery("select * from x where ts = (select max(ts)::timestamp_ns from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            Qd%ǧ\t1970-01-03T21:26:37.500000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithTimestampNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts != (select max(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z
                            """);

            assertQuery("select * from x where ts != (select max(ts)::timestamp_ns from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts = (select '1970-01-03T20:14'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            \uD8F9\uDFFC\uD8D2\uDE52p\t1970-01-03T20:14:00.000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithVarcharFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts = (select ts::varchar from x limit 1)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            Async Filter workers: 1
                              filter: ts=cursor\s
                                Limit value: 1
                                    VirtualRecord
                                      functions: [ts::varchar]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """)
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithVarcharNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts != (select '1970-01-01T00:00:00.000000Z'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000Z
                            """);
        });
    }

    @Test
    public void testPreventIntImplicitCastingToTimestampInSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (i int)");
            execute("insert into tab values (1), (2), (3)");

            // int left operand routes to the dedicated int/cursor overload (=(IC)); it is a valid
            // numeric comparison (never an implicit cast to TIMESTAMP). Rows are present so the
            // assertion exercises the IC comparison itself, not merely that it compiles.
            assertQuery("select * from tab where i = (select max(i) from tab)") // = 3
                    .noLeakCheck()
                    .returns("i\n3\n");
        });
    }

    @Test
    public void testPreventVarcharImplicitCastingToTimestampInSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            // a VARCHAR left operand now re-routes to the numeric =(DC) overload, whose guard
            // rejects a non-DOUBLE/FLOAT left operand, so the diagnostic reports the numeric
            // candidate instead of the timestamp one (same tradeoff as the < / > overloads).
            assertQuery("select * from x where a != (select '1970-01-01T00:00:00.000000Z'::varchar)")
                    .fails(22, "left operand must be a DOUBLE or FLOAT, found: VARCHAR");
        });
    }

}
