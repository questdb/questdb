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

import io.questdb.cairo.sql.InsertStatement;
import io.questdb.cairo.sql.WriterOutOfDateException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Assert;
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

    @Test
    public void testLtJoin() throws Exception {
        final String expected = "tag\thi\tlo\tts\tts1\n" +
                "AA\t315515118\tNaN\t1970-01-03T00:00:00.000000Z\t\n" +
                "BB\t-727724771\t315515118\t1970-01-03T00:06:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                "CC\t-948263339\t-727724771\t1970-01-03T00:12:00.000000Z\t1970-01-03T00:06:00.000000Z\n" +
                "CC\t592859671\t-948263339\t1970-01-03T00:18:00.000000Z\t1970-01-03T00:12:00.000000Z\n" +
                "AA\t-847531048\t592859671\t1970-01-03T00:24:00.000000Z\t1970-01-03T00:18:00.000000Z\n" +
                "BB\t-2041844972\t-847531048\t1970-01-03T00:30:00.000000Z\t1970-01-03T00:24:00.000000Z\n" +
                "BB\t-1575378703\t-2041844972\t1970-01-03T00:36:00.000000Z\t1970-01-03T00:30:00.000000Z\n" +
                "BB\t1545253512\t-1575378703\t1970-01-03T00:42:00.000000Z\t1970-01-03T00:36:00.000000Z\n" +
                "AA\t1573662097\t1545253512\t1970-01-03T00:48:00.000000Z\t1970-01-03T00:42:00.000000Z\n" +
                "AA\t339631474\t1573662097\t1970-01-03T00:54:00.000000Z\t1970-01-03T00:48:00.000000Z\n";

        assertQuery(
                "tag\thi\tlo\tts\tts1\n",
                "select a.tag, a.seq hi, b.seq lo , a.ts, b.ts from tab a lt join tab b on (tag)",
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
    public void testLtJoinNoTimestamp() throws Exception {
        final String expected = "tag\thi\tlo\n" +
                "AA\t315515118\tNaN\n" +
                "BB\t-727724771\t315515118\n" +
                "CC\t-948263339\t-727724771\n" +
                "CC\t592859671\t-948263339\n" +
                "AA\t-847531048\t592859671\n" +
                "BB\t-2041844972\t-847531048\n" +
                "BB\t-1575378703\t-2041844972\n" +
                "BB\t1545253512\t-1575378703\n" +
                "AA\t1573662097\t1545253512\n" +
                "AA\t339631474\t1573662097\n";

        assertQuery(
                "tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a lt join tab b on (tag)",
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
    public void testLtJoinGaps() throws Exception {
        final String expected = "tag\thi\tlo\n" +
                "AA\t315515118\tNaN\n" +
                "BB\t-727724771\t315515118\n" +
                "CC\t-948263339\t-727724771\n" +
                "CC\t592859671\t-948263339\n" +
                "AA\t-847531048\t592859671\n" +
                "BB\t-2041844972\t-847531048\n" +
                "BB\t-1575378703\t-2041844972\n" +
                "BB\t1545253512\t-1575378703\n" +
                "AA\t1573662097\t1545253512\n" +
                "AA\t339631474\t1573662097\n";

        assertQuery(
                "tag\thi\tlo\n",
                "select a.seq hi, b.seq lo from tab a lt join tab b where a.seq > b.seq + 1",
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

    //select a.seq hi, b.seq lo from tab a lt join b where hi > lo + 1

    @Test
    public void testLtJoinSequenceGapOnKey() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            compiler.compile("create table tab as " +
                    "(" +
                    "select " +
                    "rnd_symbol('AA', 'BB') tag," +
                    " x, " +
                    " timestamp_sequence(0, 10000) ts" +
                    " from" +
                    " long_sequence(20)" +
                    ") timestamp(ts) partition by DAY", sqlExecutionContext);
            //insert
            executeInsert("insert into tab values ('CC', 24, 210000)");
            executeInsert("insert into tab values ('CC', 25, 220000)");
            String ex = "tag\tx\tts\n" +
                    "AA\t1\t1970-01-01T00:00:00.000000Z\n" +
                    "AA\t2\t1970-01-01T00:00:00.010000Z\n" +
                    "BB\t3\t1970-01-01T00:00:00.020000Z\n" +
                    "BB\t4\t1970-01-01T00:00:00.030000Z\n" +
                    "BB\t5\t1970-01-01T00:00:00.040000Z\n" +
                    "BB\t6\t1970-01-01T00:00:00.050000Z\n" +
                    "AA\t7\t1970-01-01T00:00:00.060000Z\n" +
                    "BB\t8\t1970-01-01T00:00:00.070000Z\n" +
                    "AA\t9\t1970-01-01T00:00:00.080000Z\n" +
                    "AA\t10\t1970-01-01T00:00:00.090000Z\n" +
                    "AA\t11\t1970-01-01T00:00:00.100000Z\n" +
                    "AA\t12\t1970-01-01T00:00:00.110000Z\n" +
                    "AA\t13\t1970-01-01T00:00:00.120000Z\n" +
                    "BB\t14\t1970-01-01T00:00:00.130000Z\n" +
                    "BB\t15\t1970-01-01T00:00:00.140000Z\n" +
                    "AA\t16\t1970-01-01T00:00:00.150000Z\n" +
                    "AA\t17\t1970-01-01T00:00:00.160000Z\n" +
                    "BB\t18\t1970-01-01T00:00:00.170000Z\n" +
                    "BB\t19\t1970-01-01T00:00:00.180000Z\n" +
                    "AA\t20\t1970-01-01T00:00:00.190000Z\n" +
                    "CC\t24\t1970-01-01T00:00:00.210000Z\n" +
                    "CC\t25\t1970-01-01T00:00:00.220000Z\n";
            String query = "tab";
            printSqlResult(ex, query, "ts", null, null, true, true);
            // test
            ex = "tag\thi\tlo\n";
            query = "select a.tag, a.x hi, b.x lo from tab a lt join tab b on (tag)  where a.x > b.x + 1";
            printSqlResult(ex, query, null, null, null, false, true);
        });
    }


    @Test
    public void testLtJoinSequenceGap() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            compiler.compile("create table tab as " +
                    "(" +
                    "select " +
                    "rnd_symbol('AA', 'BB') tag," +
                    " x, " +
                    " timestamp_sequence(0, 10000) ts" +
                    " from" +
                    " long_sequence(20)" +
                    ") timestamp(ts) partition by DAY", sqlExecutionContext);
            //insert
            executeInsert("insert into tab values ('CC', 24, 210000)");
            executeInsert("insert into tab values ('CC', 25, 220000)");
            String ex = "tag\tx\tts\n" +
                    "AA\t1\t1970-01-01T00:00:00.000000Z\n" +
                    "AA\t2\t1970-01-01T00:00:00.010000Z\n" +
                    "BB\t3\t1970-01-01T00:00:00.020000Z\n" +
                    "BB\t4\t1970-01-01T00:00:00.030000Z\n" +
                    "BB\t5\t1970-01-01T00:00:00.040000Z\n" +
                    "BB\t6\t1970-01-01T00:00:00.050000Z\n" +
                    "AA\t7\t1970-01-01T00:00:00.060000Z\n" +
                    "BB\t8\t1970-01-01T00:00:00.070000Z\n" +
                    "AA\t9\t1970-01-01T00:00:00.080000Z\n" +
                    "AA\t10\t1970-01-01T00:00:00.090000Z\n" +
                    "AA\t11\t1970-01-01T00:00:00.100000Z\n" +
                    "AA\t12\t1970-01-01T00:00:00.110000Z\n" +
                    "AA\t13\t1970-01-01T00:00:00.120000Z\n" +
                    "BB\t14\t1970-01-01T00:00:00.130000Z\n" +
                    "BB\t15\t1970-01-01T00:00:00.140000Z\n" +
                    "AA\t16\t1970-01-01T00:00:00.150000Z\n" +
                    "AA\t17\t1970-01-01T00:00:00.160000Z\n" +
                    "BB\t18\t1970-01-01T00:00:00.170000Z\n" +
                    "BB\t19\t1970-01-01T00:00:00.180000Z\n" +
                    "AA\t20\t1970-01-01T00:00:00.190000Z\n" +
                    "CC\t24\t1970-01-01T00:00:00.210000Z\n" +
                    "CC\t25\t1970-01-01T00:00:00.220000Z\n";
            String query = "tab";
            printSqlResult(ex, query, "ts", null, null, true, true);
            // test
            ex = "tag\thi\tlo\n";
            query = "select a.tag, a.x hi, b.x lo from tab a lt join tab b where a.x > b.x + 1";
            printSqlResult(ex, query, null, null, null, false, true);
        });
    }
}

