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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class GtTimestampCursorFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCompareTimestampWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "  select rnd_varchar() a, rnd_long(30000, 80000000, 1)::timestamp ts from long_sequence(10)" +
                    ")");

            String expected = "a\tts\n";

            assertSql(expected, "select * from x where ts > (select null)");
            assertSql(expected, "select * from x where ts > (select null::timestamp)");
            assertSql(expected, "select * from x where ts < (select null::string)");
            assertSql(expected, "select * from x where ts < (select null::varchar)");
            // no rows selected in the cursor
            assertSql(expected, "select * from x where ts > (select 1::timestamp from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts > (select '11' from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts > (select '11'::varchar from x where 1 <> 1)");
            assertException("select * from x where ts > (select 'hello')", 28, "the cursor selected invalid timestamp value: hello");
            assertException("select * from x where ts > (select 'hello'::varchar)", 28, "the cursor selected invalid timestamp value: hello");
            assertException("select * from x where ts > (select 'hello'::varchar, 10 x)", 28, "select must provide exactly one column");
            assertException("select * from x where ts > (select 10 x)", 28, "cannot compare TIMESTAMP and INT");
        });
    }

    @Test
    public void testCompareTimestampWithString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z\n" +
                            "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z\n" +
                            "\uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z\n" +
                            "\uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z\n" +
                            "Z 4xL?4\t1970-01-01T00:00:17.500000Z\n" +
                            "p-鳓w\t1970-01-01T00:00:20.000000Z\n" +
                            "h\uDAF5\uDE17qRӽ-\t1970-01-01T00:00:22.500000Z\n",
                    "select * from x where ts > (select '1970-01-01T00:00')"
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
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n",
                    "select * from x where ts <= (select '1970-01-01T00:00:00.000000Z')"
            );
        });
    }

    @Test
    public void testCompareTimestampWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z\n" +
                            "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z\n" +
                            "\uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z\n" +
                            "\uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z\n" +
                            "Z 4xL?4\t1970-01-01T00:00:17.500000Z\n" +
                            "p-鳓w\t1970-01-01T00:00:20.000000Z\n" +
                            "h\uDAF5\uDE17qRӽ-\t1970-01-01T00:00:22.500000Z\n",
                    "select * from x where ts > (select min(ts) from x)"
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
                    "select * from x where ts <= (select min(ts) from x)"
            );
        });
    }

    @Test
    public void testCompareTimestampWithVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z\n" +
                            "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z\n" +
                            "\uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z\n" +
                            "\uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z\n" +
                            "Z 4xL?4\t1970-01-01T00:00:17.500000Z\n" +
                            "p-鳓w\t1970-01-01T00:00:20.000000Z\n" +
                            "h\uDAF5\uDE17qRӽ-\t1970-01-01T00:00:22.500000Z\n",
                    "select * from x where ts > (select '1970-01-01T00:00'::varchar)"
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
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n",
                    "select * from x where ts <= (select '1970-01-01T00:00:00.000000Z'::varchar)"
            );
        });
    }
}
