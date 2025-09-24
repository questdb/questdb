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

import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class LtTimestampCursorFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCompareNanoTimestampWithNull() throws Exception {
        testCompareTimestampWithNull(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testCompareTimestampWithNull() throws Exception {
        testCompareTimestampWithNull(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testCompareTimestampWithString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z\n" +
                            "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z\n" +
                            "\uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z\n" +
                            "\uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z\n" +
                            "Z 4xL?4\t1970-01-01T00:00:17.500000Z\n" +
                            "p-鳓w\t1970-01-01T00:00:20.000000Z\n" +
                            "h\uDAF5\uDE17qRӽ-\t1970-01-01T00:00:22.500000Z\n",
                    "select * from x where ts < (select '1970-01-03T21:26')"
            );

            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "Ǆ Ԡ阷l싒8쮠\t1970-01-01T00:00:00.000000000Z\n" +
                            "kɷ씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3\t1970-01-01T00:00:02.500000000Z\n" +
                            "XK&J\"\t1970-01-01T00:00:05.000000000Z\n" +
                            "\uDA43\uDFF0-㔍x钷Mͱ\t1970-01-01T00:00:07.500000000Z\n" +
                            "\uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\t1970-01-01T00:00:10.000000000Z\n" +
                            "91g>\t1970-01-01T00:00:12.500000000Z\n" +
                            "l5J\\d;\t1970-01-01T00:00:15.000000000Z\n" +
                            "LQ+bO\t1970-01-01T00:00:17.500000000Z\n" +
                            "h볱9\t1970-01-01T00:00:20.000000000Z\n" +
                            "a\uDA76\uDDD4*\uDB87\uDF60-ă堝ᢣ΄B\t1970-01-01T00:00:22.500000000Z\n",
                    "select * from y where ts < (select '1970-01-03T21:26')"
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
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n",
                    "select * from x where ts >= (select '1970-01-01T00:00:00.000000Z')"
            );

            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:00.000000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:02.500000000Z\n",
                    "select * from y where ts >= (select '1970-01-01T00:00:00.000000000Z')"
            );
        });
    }

    @Test
    public void testCompareTimestampWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            assertSql(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z\n" +
                            "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z\n" +
                            "\uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z\n" +
                            "\uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z\n" +
                            "Z 4xL?4\t1970-01-01T00:00:17.500000Z\n" +
                            "p-鳓w\t1970-01-01T00:00:20.000000Z\n",
                    "select * from x where ts < (select max(ts) from x)"
            );
            assertSql(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z\n" +
                            "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z\n" +
                            "\uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z\n" +
                            "\uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z\n" +
                            "Z 4xL?4\t1970-01-01T00:00:17.500000Z\n" +
                            "p-鳓w\t1970-01-01T00:00:20.000000Z\n",
                    "select * from x where ts < (select max(ts) from y)"
            );
            assertSql(
                    "a\tts\n" +
                            "Ǆ Ԡ阷l싒8쮠\t1970-01-01T00:00:00.000000000Z\n" +
                            "kɷ씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3\t1970-01-01T00:00:02.500000000Z\n" +
                            "XK&J\"\t1970-01-01T00:00:05.000000000Z\n" +
                            "\uDA43\uDFF0-㔍x钷Mͱ\t1970-01-01T00:00:07.500000000Z\n" +
                            "\uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\t1970-01-01T00:00:10.000000000Z\n" +
                            "91g>\t1970-01-01T00:00:12.500000000Z\n" +
                            "l5J\\d;\t1970-01-01T00:00:15.000000000Z\n" +
                            "LQ+bO\t1970-01-01T00:00:17.500000000Z\n" +
                            "h볱9\t1970-01-01T00:00:20.000000000Z\n",
                    "select * from y where ts < (select max(ts) from x)"
            );
            assertSql(
                    "a\tts\n" +
                            "Ǆ Ԡ阷l싒8쮠\t1970-01-01T00:00:00.000000000Z\n" +
                            "kɷ씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3\t1970-01-01T00:00:02.500000000Z\n" +
                            "XK&J\"\t1970-01-01T00:00:05.000000000Z\n" +
                            "\uDA43\uDFF0-㔍x钷Mͱ\t1970-01-01T00:00:07.500000000Z\n" +
                            "\uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\t1970-01-01T00:00:10.000000000Z\n" +
                            "91g>\t1970-01-01T00:00:12.500000000Z\n" +
                            "l5J\\d;\t1970-01-01T00:00:15.000000000Z\n" +
                            "LQ+bO\t1970-01-01T00:00:17.500000000Z\n" +
                            "h볱9\t1970-01-01T00:00:20.000000000Z\n",
                    "select * from y where ts < (select max(ts) from y)"
            );
        });
    }

    @Test
    public void testCompareTimestampWithTimestampNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");
            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(2)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n",
                    "select * from x where ts >= (select max(ts) from x)"
            );
            assertSql(
                    "a\tts\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n",
                    "select * from x where ts >= (select max(ts) from y)"
            );
            assertSql(
                    "a\tts\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:02.500000000Z\n",
                    "select * from y where ts >= (select max(ts) from x)"
            );
            assertSql(
                    "a\tts\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:02.500000000Z\n",
                    "select * from y where ts >= (select max(ts) from y)"
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
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:07.500000Z\n" +
                            "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t1970-01-01T00:00:10.000000Z\n" +
                            "\uDBAE\uDD12ɜ|\t1970-01-01T00:00:12.500000Z\n" +
                            "\uDB59\uDF3B룒jᷚ\t1970-01-01T00:00:15.000000Z\n" +
                            "Z 4xL?4\t1970-01-01T00:00:17.500000Z\n" +
                            "p-鳓w\t1970-01-01T00:00:20.000000Z\n" +
                            "h\uDAF5\uDE17qRӽ-\t1970-01-01T00:00:22.500000Z\n",
                    "select * from x where ts < (select '1970-01-03T20:14'::varchar)"
            );

            execute("create table y as (" +
                    "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(10)" +
                    ") timestamp(ts) partition by day");

            assertSql(
                    "a\tts\n" +
                            "Ǆ Ԡ阷l싒8쮠\t1970-01-01T00:00:00.000000000Z\n" +
                            "kɷ씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3\t1970-01-01T00:00:02.500000000Z\n" +
                            "XK&J\"\t1970-01-01T00:00:05.000000000Z\n" +
                            "\uDA43\uDFF0-㔍x钷Mͱ\t1970-01-01T00:00:07.500000000Z\n" +
                            "\uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\t1970-01-01T00:00:10.000000000Z\n" +
                            "91g>\t1970-01-01T00:00:12.500000000Z\n" +
                            "l5J\\d;\t1970-01-01T00:00:15.000000000Z\n" +
                            "LQ+bO\t1970-01-01T00:00:17.500000000Z\n" +
                            "h볱9\t1970-01-01T00:00:20.000000000Z\n" +
                            "a\uDA76\uDDD4*\uDB87\uDF60-ă堝ᢣ΄B\t1970-01-01T00:00:22.500000000Z\n",
                    "select * from y where ts < (select '1970-01-03T20:14'::varchar)"
            );
        });
    }

    @Test
    public void testCompareTimestampWithVarcharFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select rnd_varchar() a, timestamp_sequence(0, 2500000) ts from long_sequence(100000)" +
                            ") timestamp(ts) partition by day"
            );
            execute(
                    "create table y as (" +
                            "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(100000)" +
                            ") timestamp(ts) partition by day"
            );

            assertQueryNoLeakCheck(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z\n",
                    "select * from x where ts < (select ts::varchar from x order by ts desc limit 2) limit 3",
                    "ts",
                    true
            );
            assertQueryNoLeakCheck(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:05.000000Z\n",
                    "select * from x where ts < (select ts::varchar from y order by ts desc limit 2) limit 3",
                    "ts",
                    true
            );
            assertQueryNoLeakCheck(
                    "a\tts\n" +
                            "V0&&;\t1970-01-01T00:00:00.000000000Z\n" +
                            "5䄛~\uDA5A\uDCB4끻\uDBD9\uDC84\uD8F3\uDE52\uDB96\uDC4Dx\t1970-01-01T00:00:02.500000000Z\n" +
                            "uﮭ3\uD8C8\uDD30\uDBDA\uDEC6\uE937簡믗\t1970-01-01T00:00:05.000000000Z\n",
                    "select * from y where ts < (select ts::varchar from x order by ts desc limit 2) limit 3",
                    "ts",
                    true
            );
            assertQueryNoLeakCheck(
                    "a\tts\n" +
                            "V0&&;\t1970-01-01T00:00:00.000000000Z\n" +
                            "5䄛~\uDA5A\uDCB4끻\uDBD9\uDC84\uD8F3\uDE52\uDB96\uDC4Dx\t1970-01-01T00:00:02.500000000Z\n" +
                            "uﮭ3\uD8C8\uDD30\uDBDA\uDEC6\uE937簡믗\t1970-01-01T00:00:05.000000000Z\n",
                    "select * from y where ts < (select ts::varchar from y order by ts desc limit 2) limit 3",
                    "ts",
                    true
            );
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

            assertSql(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n",
                    "select * from x where ts >= (select '1970-01-01T00:00:00.000000Z'::varchar)"
            );
            assertSql(
                    "a\tts\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t1970-01-01T00:00:00.000000Z\n" +
                            "8#3TsZ\t1970-01-01T00:00:02.500000Z\n",
                    "select * from x where ts >= (select '1970-01-01T00:00:00.000000000Z'::varchar)"
            );

            execute(
                    "create table y as (" +
                            "select rnd_varchar() a, timestamp_sequence(0::timestamp_ns, 2500000000) ts from long_sequence(2)" +
                            ") timestamp(ts) partition by day"
            );

            assertSql(
                    "a\tts\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:00.000000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:02.500000000Z\n",
                    "select * from y where ts >= (select '1970-01-01T00:00:00.000000Z'::varchar)"
            );
            assertSql(
                    "a\tts\n" +
                            "zV衞͛Ԉ龘и\uDA89\uDFA4~\t1970-01-01T00:00:00.000000000Z\n" +
                            "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t1970-01-01T00:00:02.500000000Z\n",
                    "select * from y where ts >= (select '1970-01-01T00:00:00.000000000Z'::varchar)"
            );
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

    private void testCompareTimestampWithNull(int timestampType) throws Exception {
        assertMemoryLeak(() -> {
            String timestampTypeName = ColumnType.nameOf(timestampType);
            execute("create table x as (" +
                    "  select rnd_varchar() a, rnd_long(30000, 80000000, 1)::" + timestampTypeName + " ts from long_sequence(10)" +
                    ")");

            String expected = "a\tts\n";

            assertSql(expected, "select * from x where ts < (select null)");
            assertSql(expected, "select * from x where ts < (select null::timestamp)");
            assertSql(expected, "select * from x where ts < (select null::timestamp_ns)");
            assertSql(expected, "select * from x where ts < (select null::string)");
            assertSql(expected, "select * from x where ts < (select null::varchar)");
            // no rows selected in the cursor
            assertSql(expected, "select * from x where ts < (select 1::timestamp from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts < (select '11' from x where 1 <> 1)");
            assertSql(expected, "select * from x where ts < (select '11'::varchar from x where 1 <> 1)");
            assertException("select * from x where ts < (select 'hello')", 28, "the cursor selected invalid timestamp value: hello");
            assertException("select * from x where ts < (select 'hello'::varchar)", 28, "the cursor selected invalid timestamp value: hello");
            assertException("select * from x where ts < (select 'hello'::varchar, 10 x)", 28, "select must provide exactly one column");
            assertException("select * from x where ts < (select 10 x)", 28, "cannot compare TIMESTAMP and INT");
        });
    }

    private void testPlans(int timestampType) throws Exception {
        assertMemoryLeak(() -> {
            String timestampTypeName = ColumnType.nameOf(timestampType);
            execute("create table x as (" +
                    "  select rnd_varchar() a, rnd_long(30000, 80000000, 1)::" + timestampTypeName + " ts from long_sequence(10)" +
                    ")");

            // thread-safe
            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts::string::timestamp < cursor \n" +
                            "    VirtualRecord\n" +
                            "      functions: [null]\n" +
                            "        long_sequence count: 1 [state-shared] [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts::string::timestamp < (select null)"
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts::string::timestamp < cursor \n" +
                            "    VirtualRecord\n" +
                            "      functions: [1970-01-01T00:00:00.000001Z]\n" +
                            "        long_sequence count: 1 [state-shared] [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts::string::timestamp < (select 1::timestamp)"
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts::string::timestamp < cursor \n" +
                            "    VirtualRecord\n" +
                            "      functions: ['2015-03-11']\n" +
                            "        long_sequence count: 1 [state-shared] [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts::string::timestamp < (select '2015-03-11'::string)"
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts::string::timestamp < cursor \n" +
                            "    VirtualRecord\n" +
                            "      functions: ['2015-03-12']\n" +
                            "        long_sequence count: 1 [state-shared] [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts::string::timestamp < (select '2015-03-12'::varchar)"
            );

            // thread-safe

            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts [thread-safe] < cursor \n" +
                            "    VirtualRecord\n" +
                            "      functions: [null]\n" +
                            "        long_sequence count: 1 [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts < (select null)"
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts [thread-safe] < cursor \n" +
                            "    VirtualRecord\n" +
                            "      functions: [1970-01-01T00:00:00.000001Z]\n" +
                            "        long_sequence count: 1 [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts < (select 1::timestamp)"
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts [thread-safe] < cursor \n" +
                            "    VirtualRecord\n" +
                            "      functions: ['2015-03-11']\n" +
                            "        long_sequence count: 1 [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts < (select '2015-03-11'::string)"
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: ts [thread-safe] < cursor \n" +
                            "    VirtualRecord\n" +
                            "      functions: ['2015-03-12']\n" +
                            "        long_sequence count: 1 [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "explain select * from x where ts < (select '2015-03-12'::varchar)"
            );
        });
    }

    @Test
    public void testPreventIntImplicitCastingToTimestampInSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (i int)");

            assertException(
                    "select * from tab where i < (select max(i) from tab)",
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
                    "select * from x where a < (select '1970-01-01T00:00:00.000000Z'::varchar)",
                    22,
                    "left operand must be a TIMESTAMP, found: VARCHAR"
            );
        });
    }
}
