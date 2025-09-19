/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
    public void testCompareNanoTimestampWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select rnd_varchar() a, rnd_long(30000000, 80000000000, 1)::timestamp_ns ts from long_sequence(100)" +
                    ")");

            String expected = "a\tts\n" +
                    "ŸO(OFг\uDBAE\uDD12ɜ|\t\n" +
                    "1CW\t\n" +
                    "O=I~\t\n" +
                    "Ǭ\uDB37\uDC95Q\t\n" +
                    "鏻Ê띘Ѷ>͓\uDA8B\uDFC4︵Ƀ^\t\n" +
                    "Mw1$c\t\n" +
                    ";PGY=FU[H\t\n" +
                    "}L1>ML\t\n" +
                    "{ϸ\uD9F4\uDFB9\uDA0A\uDC7A\uDA76\uDC87>\uD8F0\uDF66Ҫb\uDBB1\uDEA3\t\n" +
                    "g<~%j\uD9D3\uDCEE;+\t\n" +
                    "JY)xuaN\t\n" +
                    "V_M\t\n" +
                    "뮣݇8Y\t\n" +
                    "^嘢\uD952\uDF63^\t\n" +
                    "D&9L5BBo,O\t\n" +
                    "Ɉ\uDAB6\uDF33\uDB00\uDF8AϿ˄礏ɍ\uDB2C\uDD55\t\n" +
                    "ށڥ[<\uDBCD\uDE09\uDB92\uDC69{UVo\t\n" +
                    "ʫ\uDACE\uDF0Bǟ\t\n" +
                    "XBl~ݴ\uD8D6\uDD39!\uDAA9\uDF2C֝\t\n" +
                    "`^$SJm9h-f\t\n" +
                    "a Zf\t\n" +
                    "\uD9A8\uDFFBi⟃2\t\n" +
                    "<#71^jDS9\t\n" +
                    "?+$b[Enk\t\n" +
                    "Wmτ⻱[N`亲\t\n" +
                    "vCbc&rm{/&\t\n";

            assertSql(expected, "select * from x where ts = (select null)");
            assertSql(expected, "select * from x where ts = (select null::timestamp)");
            assertSql(expected, "select * from x where ts = (select null::timestamp_ns)");
            assertSql(expected, "select * from x where ts = (select null::string)");
            assertSql(expected, "select * from x where ts = (select null::varchar)");
            // no rows selected in the cursor
            assertSql(expected, "select * from x where ts = (select 1::timestamp from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts = (select 1000::timestamp_ns from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts = (select '11' from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts = (select '11'::varchar from x where 1 <> 1)");
            assertException("select * from x where ts = (select 'hello')", 28, "the cursor selected invalid timestamp value: hello");
            assertException("select * from x where ts = (select 'hello'::varchar)", 28, "the cursor selected invalid timestamp value: hello");
            assertException("select * from x where ts = (select 'hello'::varchar, 10 x)", 28, "select must provide exactly one column");
            assertException("select * from x where ts = (select 10 x)", 28, "cannot compare TIMESTAMP and INT");
        });
    }

    @Test
    public void testCompareNanoTimestampWithString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "Eڄ篽\uDB3D\uDF6B,ᵨD\uD939\uDF1E\uD8E5\uDCC3\t1970-01-03T21:26:00.000000000Z\n",
                    "select * from x where ts = (select '1970-01-03T21:26')"
            );
        });
    }

    @Test
    public void testCompareNanoTimestampWithStringNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000000Z\n",
                    "select * from x where ts != (select '1970-01-01T00:00:00.000000Z')"
            );
        });
    }

    @Test
    public void testCompareNanoTimestampWithTimestampNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000000Z\n",
                    "select * from x where ts != (select max(ts) from x)"
            );

            assertSql(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000000Z\n",
                    "select * from x where ts != (select max(ts)::timestamp from x)"
            );
        });
    }

    @Test
    public void testCompareNanoTimestampWithVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "\uD8F9\uDFFC\uD8D2\uDE52p\t1970-01-03T20:14:00.000000000Z\n",
                    "select * from x where ts = (select '1970-01-03T20:14'::varchar)"
            );
        });
    }

    @Test
    public void testCompareNanoTimestampWithVarcharFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQueryNoLeakCheck(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000000Z\n",
                    "select * from x where ts = (select ts::varchar from x limit 2)",
                    "ts",
                    true
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts=cursor \n" +
                            "    Limit lo: 2\n" +
                            "        VirtualRecord\n" +
                            "          functions: [ts::varchar]\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: x [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts = (select ts::varchar from x limit 2)"
            );
        });
    }

    @Test
    public void testCompareNanoTimestampWithVarcharNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000000Z\n",
                    "select * from x where ts != (select '1970-01-01T00:00:00.000000000Z'::varchar)"
            );
        });
    }

    @Test
    public void testCompareTimestampNanoWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "Qd%ǧ\t1970-01-03T21:26:37.500000000Z\n",
                    "select * from x where ts = (select max(ts) from x)"
            );

            assertSql(
                    "a\tts\n" +
                            "Qd%ǧ\t1970-01-03T21:26:37.500000000Z\n",
                    "select * from x where ts = (select max(ts)::timestamp from x)"
            );
        });
    }

    @Test
    public void testCompareTimestampWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select rnd_varchar() a, rnd_long(30000, 80000000, 1)::timestamp ts from long_sequence(100)" +
                    ")");

            String expected = "a\tts\n" +
                    "ŸO(OFг\uDBAE\uDD12ɜ|\t\n" +
                    "1CW\t\n" +
                    "O=I~\t\n" +
                    "Ǭ\uDB37\uDC95Q\t\n" +
                    "鏻Ê띘Ѷ>͓\uDA8B\uDFC4︵Ƀ^\t\n" +
                    "Mw1$c\t\n" +
                    ";PGY=FU[H\t\n" +
                    "}L1>ML\t\n" +
                    "{ϸ\uD9F4\uDFB9\uDA0A\uDC7A\uDA76\uDC87>\uD8F0\uDF66Ҫb\uDBB1\uDEA3\t\n" +
                    "g<~%j\uD9D3\uDCEE;+\t\n" +
                    "JY)xuaN\t\n" +
                    "V_M\t\n" +
                    "뮣݇8Y\t\n" +
                    "^嘢\uD952\uDF63^\t\n" +
                    "D&9L5BBo,O\t\n" +
                    "Ɉ\uDAB6\uDF33\uDB00\uDF8AϿ˄礏ɍ\uDB2C\uDD55\t\n" +
                    "ށڥ[<\uDBCD\uDE09\uDB92\uDC69{UVo\t\n" +
                    "ʫ\uDACE\uDF0Bǟ\t\n" +
                    "XBl~ݴ\uD8D6\uDD39!\uDAA9\uDF2C֝\t\n" +
                    "`^$SJm9h-f\t\n" +
                    "a Zf\t\n" +
                    "\uD9A8\uDFFBi⟃2\t\n" +
                    "<#71^jDS9\t\n" +
                    "?+$b[Enk\t\n" +
                    "Wmτ⻱[N`亲\t\n" +
                    "vCbc&rm{/&\t\n";

            assertSql(expected, "select * from x where ts = (select null)");
            assertSql(expected, "select * from x where ts = (select null::timestamp)");
            assertSql(expected, "select * from x where ts = (select null::timestamp_ns)");
            assertSql(expected, "select * from x where ts = (select null::string)");
            assertSql(expected, "select * from x where ts = (select null::varchar)");
            // no rows selected in the cursor
            assertSql(expected, "select * from x where ts = (select 1::timestamp from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts = (select 1000::timestamp_ns from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts = (select '11' from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts = (select '11'::varchar from x where 1 <> 1)");
            assertException("select * from x where ts = (select 'hello')", 28, "the cursor selected invalid timestamp value: hello");
            assertException("select * from x where ts = (select 'hello'::varchar)", 28, "the cursor selected invalid timestamp value: hello");
            assertException("select * from x where ts = (select 'hello'::varchar, 10 x)", 28, "select must provide exactly one column");
            assertException("select * from x where ts = (select 10 x)", 28, "cannot compare TIMESTAMP and INT");
        });
    }

    @Test
    public void testCompareTimestampWithString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "Eڄ篽\uDB3D\uDF6B,ᵨD\uD939\uDF1E\uD8E5\uDCC3\t1970-01-03T21:26:00.000000Z\n",
                    "select * from x where ts = (select '1970-01-03T21:26')"
            );
        });
    }

    @Test
    public void testCompareTimestampWithStringNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n",
                    "select * from x where ts != (select '1970-01-01T00:00:00.000000000Z')"
            );
        });
    }

    @Test
    public void testCompareTimestampWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "Qd%ǧ\t1970-01-03T21:26:37.500000Z\n",
                    "select * from x where ts = (select max(ts) from x)"
            );

            assertSql(
                    "a\tts\n" +
                            "Qd%ǧ\t1970-01-03T21:26:37.500000Z\n",
                    "select * from x where ts = (select max(ts)::timestamp_ns from x)"
            );
        });
    }

    @Test
    public void testCompareTimestampWithTimestampNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n",
                    "select * from x where ts != (select max(ts) from x)"
            );

            assertSql(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n",
                    "select * from x where ts != (select max(ts)::timestamp_ns from x)"
            );
        });
    }

    @Test
    public void testCompareTimestampWithVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "\uD8F9\uDFFC\uD8D2\uDE52p\t1970-01-03T20:14:00.000000Z\n",
                    "select * from x where ts = (select '1970-01-03T20:14'::varchar)"
            );
        });
    }

    @Test
    public void testCompareTimestampWithVarcharFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                    ") timestamp(ts) partition by day");

            assertQueryNoLeakCheck(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n",
                    "select * from x where ts = (select ts::varchar from x limit 2)",
                    "ts",
                    true
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts=cursor \n" +
                            "    Limit lo: 2\n" +
                            "        VirtualRecord\n" +
                            "          functions: [ts::varchar]\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: x [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts = (select ts::varchar from x limit 2)"
            );
        });
    }

    @Test
    public void testCompareTimestampWithVarcharNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n",
                    "select * from x where ts != (select '1970-01-01T00:00:00.000000Z'::varchar)"
            );
        });
    }

    @Test
    public void testPreventIntImplicitCastingToTimestampInSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (i int)");

            assertException(
                    "select * from tab where i = (select max(i) from tab)",
                    24,
                    "left operand must be a TIMESTAMP, found: INT"
            );
        });
    }

    @Test
    public void testPreventVarcharImplicitCastingToTimestampInSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertException(
                    "select * from x where a != (select '1970-01-01T00:00:00.000000Z'::varchar)",
                    22,
                    "left operand must be a TIMESTAMP, found: VARCHAR"
            );
        });
    }

}
