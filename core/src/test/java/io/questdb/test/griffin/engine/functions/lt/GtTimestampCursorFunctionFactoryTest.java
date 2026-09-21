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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class GtTimestampCursorFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCompareNanoTimestampWithNull() throws Exception {
        testCompareTimestampWithNull(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testCompareTimestampWithNull() throws Exception {
        testCompareTimestampWithNull(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testMultiRowCursorFails() throws Exception {
        // a scalar sub-query yielding more than one row is an error, reported at the sub-query position
        assertMemoryLeak(() -> {
            execute("create table x as (select timestamp_sequence(0, 2500000) ts from long_sequence(2))");
            // timestamp cursor column
            assertQuery("select * from x where ts > (select ts from x)")
                    .fails(28, "scalar sub-query returned more than one row");
            // string cursor column
            assertQuery("select * from x where ts > (select '1970-01-01' from x)")
                    .fails(28, "scalar sub-query returned more than one row");
            // varchar cursor column (separately implemented reader)
            assertQuery("select * from x where ts > (select '1970-01-01'::varchar from x)")
                    .fails(28, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testMultiRowCursorFailsOnDesignatedTimestamp() throws Exception {
        // On a designated-timestamp column, ts > (select ...) is extracted as an interval intrinsic and
        // evaluated by RuntimeIntervalModel instead of the cursor-comparison factory. A scalar sub-query
        // must still reject more than one row here rather than silently taking an arbitrary first row,
        // and the error must point at the offending sub-query, same as the factory path does.
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select timestamp_sequence(0, 2500000) ts from long_sequence(5)" +
                    ") timestamp(ts) partition by day");
            assertQuery("select * from x where ts > (select ts from x limit 2)")
                    .fails(28, "scalar sub-query returned more than one row");
            assertQuery("select * from x where ts >= (select ts from x limit 2)")
                    .fails(29, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testCompareTimestampWithString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts > (select '1970-01-01T00:00')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000Z
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z
                            ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z
                            \uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z
                            \uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z
                            \uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z
                            Z 4xL?4\t1970-01-01T00:00:17.500000Z
                            p-鳓w\t1970-01-01T00:00:20.000000Z
                            h\uDAF5\uDE17qRӽ-\t1970-01-01T00:00:22.500000Z
                            """);

            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from y where ts > (select '1970-01-01T00:00')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            kɷ씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3\t1970-01-01T00:00:02.500000000Z
                            XK&J"\t1970-01-01T00:00:05.000000000Z
                            \uDA43\uDFF0-㔍x钷Mͱ\t1970-01-01T00:00:07.500000000Z
                            \uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\t1970-01-01T00:00:10.000000000Z
                            91g>\t1970-01-01T00:00:12.500000000Z
                            l5J\\d;\t1970-01-01T00:00:15.000000000Z
                            LQ+bO\t1970-01-01T00:00:17.500000000Z
                            h볱9\t1970-01-01T00:00:20.000000000Z
                            a\uDA76\uDDD4*\uDB87\uDF60-ă堝ᢣ΄B\t1970-01-01T00:00:22.500000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithStringNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts <= (select '1970-01-01T00:00:00.000000Z')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z
                            """);

            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from y where ts <= (select '1970-01-01T00:00:00.000000000Z')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            assertQuery("select * from x where ts > (select min(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000Z
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z
                            ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z
                            \uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z
                            \uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z
                            \uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z
                            Z 4xL?4\t1970-01-01T00:00:17.500000Z
                            p-鳓w\t1970-01-01T00:00:20.000000Z
                            h\uDAF5\uDE17qRӽ-\t1970-01-01T00:00:22.500000Z
                            """);
            assertQuery("select * from x where ts > (select min(ts) from y)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000Z
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z
                            ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z
                            \uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z
                            \uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z
                            \uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z
                            Z 4xL?4\t1970-01-01T00:00:17.500000Z
                            p-鳓w\t1970-01-01T00:00:20.000000Z
                            h\uDAF5\uDE17qRӽ-\t1970-01-01T00:00:22.500000Z
                            """);
            assertQuery("select * from y where ts > (select min(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            kɷ씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3\t1970-01-01T00:00:02.500000000Z
                            XK&J"\t1970-01-01T00:00:05.000000000Z
                            \uDA43\uDFF0-㔍x钷Mͱ\t1970-01-01T00:00:07.500000000Z
                            \uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\t1970-01-01T00:00:10.000000000Z
                            91g>\t1970-01-01T00:00:12.500000000Z
                            l5J\\d;\t1970-01-01T00:00:15.000000000Z
                            LQ+bO\t1970-01-01T00:00:17.500000000Z
                            h볱9\t1970-01-01T00:00:20.000000000Z
                            a\uDA76\uDDD4*\uDB87\uDF60-ă堝ᢣ΄B\t1970-01-01T00:00:22.500000000Z
                            """);
            assertQuery("select * from y where ts > (select min(ts) from y)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            kɷ씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3\t1970-01-01T00:00:02.500000000Z
                            XK&J"\t1970-01-01T00:00:05.000000000Z
                            \uDA43\uDFF0-㔍x钷Mͱ\t1970-01-01T00:00:07.500000000Z
                            \uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\t1970-01-01T00:00:10.000000000Z
                            91g>\t1970-01-01T00:00:12.500000000Z
                            l5J\\d;\t1970-01-01T00:00:15.000000000Z
                            LQ+bO\t1970-01-01T00:00:17.500000000Z
                            h볱9\t1970-01-01T00:00:20.000000000Z
                            a\uDA76\uDDD4*\uDB87\uDF60-ă堝ᢣ΄B\t1970-01-01T00:00:22.500000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithTimestampNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");
            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts <= (select min(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z
                            """);
            assertQuery("select * from x where ts <= (select min(ts) from y)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z
                            """);
            assertQuery("select * from y where ts <= (select min(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:00.000000000Z
                            """);
            assertQuery("select * from y where ts <= (select min(ts) from y)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts > (select '1970-01-01T00:00'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000Z
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z
                            ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z
                            \uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z
                            \uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z
                            \uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z
                            Z 4xL?4\t1970-01-01T00:00:17.500000Z
                            p-鳓w\t1970-01-01T00:00:20.000000Z
                            h\uDAF5\uDE17qRӽ-\t1970-01-01T00:00:22.500000Z
                            """);

            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from y where ts > (select '1970-01-01T00:00'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            kɷ씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3\t1970-01-01T00:00:02.500000000Z
                            XK&J"\t1970-01-01T00:00:05.000000000Z
                            \uDA43\uDFF0-㔍x钷Mͱ\t1970-01-01T00:00:07.500000000Z
                            \uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\t1970-01-01T00:00:10.000000000Z
                            91g>\t1970-01-01T00:00:12.500000000Z
                            l5J\\d;\t1970-01-01T00:00:15.000000000Z
                            LQ+bO\t1970-01-01T00:00:17.500000000Z
                            h볱9\t1970-01-01T00:00:20.000000000Z
                            a\uDA76\uDDD4*\uDB87\uDF60-ă堝ᢣ΄B\t1970-01-01T00:00:22.500000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithVarcharFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");
            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where ts > (select ts::varchar from x limit 1) limit 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000Z
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z
                            ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z
                            """);
            assertQuery("select * from x where ts > (select ts::varchar from y limit 1) limit 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            8#3TsZ\t1970-01-01T00:00:02.500000Z
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z
                            ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z
                            """);
            assertQuery("select * from y where ts > (select ts::varchar from x limit 1) limit 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            5䄛~\uDA5A\uDCB4끻\uDBD9\uDC84\uD8F3\uDE52\uDB96\uDC4Dx\t1970-01-01T00:00:02.500000000Z
                            uﮭ3\uD8C8\uDD30\uDBDA\uDEC6\uE937簡믗\t1970-01-01T00:00:05.000000000Z
                            4\uE0CAcY\uD97C\uDDF1櫤ğ\t1970-01-01T00:00:07.500000000Z
                            """);
            assertQuery("select * from y where ts > (select ts::varchar from y limit 1) limit 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            5䄛~\uDA5A\uDCB4끻\uDBD9\uDC84\uD8F3\uDE52\uDB96\uDC4Dx\t1970-01-01T00:00:02.500000000Z
                            uﮭ3\uD8C8\uDD30\uDBDA\uDEC6\uE937簡믗\t1970-01-01T00:00:05.000000000Z
                            4\uE0CAcY\uD97C\uDDF1櫤ğ\t1970-01-01T00:00:07.500000000Z
                            """);
        });
    }

    @Test
    public void testCompareTimestampWithVarcharNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                            ") timestamp(ts) partition by day"
            );

            assertQuery("select * from x where ts <= (select '1970-01-01T00:00:00.000000Z'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z
                            """);
            assertQuery("select * from x where ts <= (select '1970-01-01T00:00:00.000000000Z'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z
                            """);

            execute(
                    "create table y as (" +
                            "select rnd_varchar() a, timestamp_sequence_ns(0, 2500000000) ts from long_sequence(2)" +
                            ") timestamp(ts) partition by day"
            );

            assertQuery("select * from y where ts <= (select '1970-01-01T00:00:00.000000Z'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:00.000000000Z
                            """);
            assertQuery("select * from y where ts <= (select '1970-01-01T00:00:00.000000000Z'::varchar)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts
                            zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testPlans() throws Exception {
        testPlans(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testPlansWithNano() throws Exception {
        testPlans(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testEmptyIntervalShortCircuitWithCursorBound() throws Exception {
        // WhereClauseParser traverses AND predicates right-to-left: `ts = NULL::TIMESTAMP` empties
        // the interval model before the scalar sub-query predicate on the left is parsed. The
        // builder must consume (close) the already-constructed cursor function on that no-op path
        // and the query must return an empty result.
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");
            String[] predicates = {
                    "ts = (select min(ts) from x)",
                    "ts > (select min(ts) from x)",
                    "ts < (select max(ts) from x)",
                    "ts != (select min(ts) from x)"
            };
            for (String predicate : predicates) {
                assertQuery("select count() c from x where " + predicate + " and ts = null::timestamp")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("c\n0\n");
            }
        });
    }

    @Test
    public void testInclusiveBoundMatchesStoredTypeMaximum() throws Exception {
        // The designated timestamp rejects values beyond 9999-12-31, so the domain maximum can
        // only live in a non-designated TIMESTAMP column, where the comparison runs through the
        // cursor-comparison functions instead of the interval intrinsics. A row holding
        // Long.MAX_VALUE distinguishes the inclusive from the strict bound: `>= MAX` must match
        // exactly that row and `> MAX` must match nothing. A fixture with only small timestamps
        // cannot tell `>=` apart from `<`.
        assertMemoryLeak(() -> {
            execute("create table xmax (ts timestamp, t2 timestamp) timestamp(ts)");
            execute("insert into xmax values (0, 0), (1, 9223372036854775807)");

            // assert row identity, not counts: with one small row and one MAX row, a `>=`
            // mistakenly computing `<` would return the same count but the other row
            assertQuery("select ts::long tsv from xmax where t2 >= (select 9_223_372_036_854_775_807::timestamp)")
                    .noLeakCheck()
                    .returns("tsv\n1\n");
            assertQuery("select ts::long tsv from xmax where t2 > (select 9_223_372_036_854_775_807::timestamp)")
                    .noLeakCheck()
                    .returns("tsv\n");
            assertQuery("select ts::long tsv from xmax where t2 < (select 9_223_372_036_854_775_807::timestamp)")
                    .noLeakCheck()
                    .returns("tsv\n0\n");
            assertQuery("select ts::long tsv from xmax where t2 <= (select 9_223_372_036_854_775_807::timestamp)")
                    .noLeakCheck()
                    .returns("tsv\n0\n1\n");
        });
    }

    @Test
    public void testStrictBoundAtTypeExtremeMatchesNothing() throws Exception {
        // `ts > Long.MAX_VALUE` used to wrap the interval low bound to Long.MIN_VALUE and return
        // every row. A strict bound just past the timestamp domain must produce an empty interval
        // for constant and scalar sub-query bounds alike, on both TIMESTAMP and TIMESTAMP_NS.
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");
            execute("create table y as (" +
                    "select timestamp_sequence_ns(0, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            // bare epoch literal bound
            assertQuery("select count() c from x where ts > 9223372036854775807")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n0\n");

            // constant bounds
            assertQuery("select count() c from x where ts > 9_223_372_036_854_775_807::timestamp")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n0\n");
            assertQuery("select count() c from y where ts > 9_223_372_036_854_775_807::timestamp_ns")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n0\n");

            // scalar sub-query bounds
            assertQuery("select count() c from x where ts > (select 9_223_372_036_854_775_807::timestamp)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n0\n");
            assertQuery("select count() c from y where ts > (select 9_223_372_036_854_775_807::timestamp_ns)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n0\n");

            // the inclusive bound stays a normal [MAX, MAX] interval: no rows here, no wrap-around
            assertQuery("select count() c from x where ts >= (select 9_223_372_036_854_775_807::timestamp)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n0\n");
        });
    }

    @Test
    public void testPreventIntImplicitCastingToTimestampInSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (i int)");
            execute("insert into tab values (1), (2), (3)");

            // int left operand routes to the dedicated int/cursor overload (>(IC)); it is a valid
            // numeric comparison (never an implicit cast to TIMESTAMP). Rows are present so the
            // assertion exercises the IC comparison itself, not merely that it compiles.
            assertQuery("select * from tab where i > (select min(i) from tab)") // > 1
                    .noLeakCheck()
                    .returns("i\n2\n3\n");
        });
    }

    @Test
    public void testPreventVarcharImplicitCastingToTimestampInSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertQuery("select * from x where a <= (select '1970-01-01T00:00:00.000000Z'::varchar)")
                    .fails(22, "left operand must be a DOUBLE or FLOAT, found: VARCHAR");
        });
    }

    @Test
    public void testPreventStringImplicitCastingToTimestampInSubQuery() throws Exception {
        // A STRING left operand re-routes to >(DC) exactly like VARCHAR: the DC guard rejects a
        // non-DOUBLE/FLOAT left operand, so the error flips from "must be a TIMESTAMP" to
        // "must be a DOUBLE or FLOAT". Lock the routing so it cannot silently regress.
        assertMemoryLeak(() -> {
            execute("create table x (a string, ts timestamp) timestamp(ts) partition by day");
            assertQuery("select * from x where a <= (select '1970-01-01T00:00:00.000000Z'::string)")
                    .fails(22, "left operand must be a DOUBLE or FLOAT, found: STRING");
        });
    }

    private void testCompareTimestampWithNull(int timestampType) throws Exception {
        assertMemoryLeak(() -> {
            String timestampTypeName = ColumnType.nameOf(timestampType);
            execute("create table x as (" +
                    "  select rnd_varchar() a, rnd_long(30000, 80000000, 1)::" + timestampTypeName + " ts from long_sequence(10)" +
                    ")");

            String expected = "a\tts\n";

            assertQuery("select * from x where ts > (select null)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts > (select null::timestamp)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts > (select null::timestamp_ns)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts > (select null::string)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts > (select null::varchar)")
                    .noLeakCheck()
                    .returns(expected);
            // no rows selected in the cursor
            assertQuery("select * from x where ts > (select 1::timestamp from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts > (select '11' from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts > (select '11'::varchar from x where 1 <> 1)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("select * from x where ts > (select 'hello')")
                    .fails(28, "the cursor selected invalid timestamp value: hello");
            assertQuery("select * from x where ts > (select 'hello'::varchar)")
                    .fails(28, "the cursor selected invalid timestamp value: hello");
            assertQuery("select * from x where ts > (select 'hello'::varchar, 10 x)")
                    .fails(28, "select must provide exactly one column");
            assertQuery("select * from x where ts > (select 10 x)")
                    .fails(28, "cannot compare TIMESTAMP and INT");
        });
    }

    private void testPlans(int timestampType) throws Exception {
        assertMemoryLeak(() -> {
            String timestampTypeName = ColumnType.nameOf(timestampType);
            execute("create table x as (" +
                    "  select rnd_varchar() a, rnd_long(30000, 80000000, 1)::" + timestampTypeName + " ts from long_sequence(10)" +
                    ")");

            // thread-safe
            assertQuery("select * from x where ts > (select null)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts [thread-safe] > cursor\s
                                VirtualRecord
                                  functions: [null]
                                    long_sequence count: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where ts <= (select null)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts [thread-safe] <= cursor\s
                                VirtualRecord
                                  functions: [null]
                                    long_sequence count: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where ts > (select '2026-03-10'::timestamp)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts [thread-safe] > cursor\s
                                VirtualRecord
                                  functions: [2026-03-10T00:00:00.000000Z]
                                    long_sequence count: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where ts <= (select '2014-09-03'::string)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts [thread-safe] <= cursor\s
                                VirtualRecord
                                  functions: ['2014-09-03']
                                    long_sequence count: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where ts > (select '2020-02-21'::varchar)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts [thread-safe] > cursor\s
                                VirtualRecord
                                  functions: ['2020-02-21']
                                    long_sequence count: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where ts <= (select '2020-02-21'::varchar)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts [thread-safe] <= cursor\s
                                VirtualRecord
                                  functions: ['2020-02-21']
                                    long_sequence count: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            // non-thread-safe
            assertQuery("select * from x where ts::varchar::timestamp > (select null)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts::varchar::timestamp > cursor\s
                                VirtualRecord
                                  functions: [null]
                                    long_sequence count: 1 [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where ts::varchar::timestamp > (select '2026-03-10'::timestamp)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts::varchar::timestamp > cursor\s
                                VirtualRecord
                                  functions: [2026-03-10T00:00:00.000000Z]
                                    long_sequence count: 1 [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where ts::varchar::timestamp > (select '2014-09-03'::string)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts::varchar::timestamp > cursor\s
                                VirtualRecord
                                  functions: ['2014-09-03']
                                    long_sequence count: 1 [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where ts::varchar::timestamp > (select '2020-02-21'::varchar)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ts::varchar::timestamp > cursor\s
                                VirtualRecord
                                  functions: ['2020-02-21']
                                    long_sequence count: 1 [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);
        });
    }
}
