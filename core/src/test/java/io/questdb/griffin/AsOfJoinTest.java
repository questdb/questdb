/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

public class AsOfJoinTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAsofJoinForSelectWithoutTimestamp() throws Exception {
        final String expected = "tag\thi\tlo\n" +
                "AA\t315515118\t315515118\n" +
                "BB\t-727724771\t-727724771\n" +
                "CC\t-948263339\t-948263339\n" +
                "CC\t592859671\t592859671\n" +
                "AA\t-847531048\t-847531048\n" +
                "BB\t-2041844972\t-2041844972\n" +
                "BB\t-1575378703\t-1575378703\n" +
                "BB\t1545253512\t1545253512\n" +
                "AA\t1573662097\t1573662097\n" +
                "AA\t339631474\t339631474\n";


        assertQuery(
                "tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false
        );
    }

    @Test
    public void testAsofJoinForSelectWithTimestamps() throws Exception {
        final String expected = "tag\thi\tlo\tts\tts1\n" +
                "AA\t315515118\t315515118\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                "BB\t-727724771\t-727724771\t1970-01-03T00:06:00.000000Z\t1970-01-03T00:06:00.000000Z\n" +
                "CC\t-948263339\t-948263339\t1970-01-03T00:12:00.000000Z\t1970-01-03T00:12:00.000000Z\n" +
                "CC\t592859671\t592859671\t1970-01-03T00:18:00.000000Z\t1970-01-03T00:18:00.000000Z\n" +
                "AA\t-847531048\t-847531048\t1970-01-03T00:24:00.000000Z\t1970-01-03T00:24:00.000000Z\n" +
                "BB\t-2041844972\t-2041844972\t1970-01-03T00:30:00.000000Z\t1970-01-03T00:30:00.000000Z\n" +
                "BB\t-1575378703\t-1575378703\t1970-01-03T00:36:00.000000Z\t1970-01-03T00:36:00.000000Z\n" +
                "BB\t1545253512\t1545253512\t1970-01-03T00:42:00.000000Z\t1970-01-03T00:42:00.000000Z\n" +
                "AA\t1573662097\t1573662097\t1970-01-03T00:48:00.000000Z\t1970-01-03T00:48:00.000000Z\n" +
                "AA\t339631474\t339631474\t1970-01-03T00:54:00.000000Z\t1970-01-03T00:54:00.000000Z\n";


        assertQuery(
                "tag\thi\tlo\tts\tts1\n",
                "select a.tag, a.seq hi, b.seq lo,  a.ts, b.ts from tab a asof join tab b on (tag)",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false
        );
    }

    @Test
    public void testAsofJoinForSelectWithoutTimestampAndWithWhereStatement() throws Exception {
        final String expected = "tag\thi\tlo\n" +
                "AA\t315515118\t315515118\n" +
                "BB\t-727724771\t-727724771\n" +
                "CC\t-948263339\t-948263339\n" +
                "CC\t592859671\t592859671\n" +
                "AA\t-847531048\t-847531048\n" +
                "BB\t-2041844972\t-2041844972\n" +
                "BB\t-1575378703\t-1575378703\n" +
                "BB\t1545253512\t1545253512\n" +
                "AA\t1573662097\t1573662097\n" +
                "AA\t339631474\t339631474\n";
        assertQuery(
                "tag\thi\tlo\n",
                "(select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)) where hi > lo + 1",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false
        );
    }

    @Test
    public void testAsofJoinForSelectWithoutTimestampAndWithWhereStatementV2() throws Exception {
        final String expected = "tag\thi\tlo\n";
        assertQuery(
                "tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag) where b.seq < a.seq",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false
        );
    }
}
