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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DistinctTimeSeriesTest extends AbstractCairoTest {

    @Test
    public void testAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm','googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(500000000000L,x/4) ts," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_char() t," +
                            " CAST(now() as LONG256) l256" + // Semi-random to not change saved txt file result
                            " from long_sequence(500)" +
                            ") timestamp (ts) partition by DAY");

            // create a copy of 'x' as our expected result set
            execute("create table y as (select * from x)");

            // copy 'x' into itself, thus duplicating every row
            execute("insert into x select * from x");

            assertSqlCursors(
                    "y",
                    "select distinct * from x"
            );

            assertQuery("select distinct * from x where 1 != 1")
                    .noLeakCheck()
                    .returns("i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tts\tl\tm\tn\tt\tl256\n");
        });
    }

    @Test
    public void testCursorCorrectness() throws Exception {
        assertMemoryLeak(() -> assertQuery("select distinct * from x order by ts")
                .ddl("create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm','googl') sym," +
                        " timestamp_sequence(500000000000L,330000000L) ts" +
                        " from long_sequence(10)" +
                        ") timestamp (ts) partition by DAY")
                .timestamp("ts")
                .expectSize()
                .returns("""
                        i\tsym\tts
                        1\tmsft\t1970-01-06T18:53:20.000000Z
                        2\tmsft\t1970-01-06T18:58:50.000000Z
                        3\tibm\t1970-01-06T19:04:20.000000Z
                        4\tgoogl\t1970-01-06T19:09:50.000000Z
                        5\tgoogl\t1970-01-06T19:15:20.000000Z
                        6\tgoogl\t1970-01-06T19:20:50.000000Z
                        7\tgoogl\t1970-01-06T19:26:20.000000Z
                        8\tibm\t1970-01-06T19:31:50.000000Z
                        9\tmsft\t1970-01-06T19:37:20.000000Z
                        10\tibm\t1970-01-06T19:42:50.000000Z
                        """));
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm','googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " timestamp_sequence(500000000000L,x/4) ts," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_char() t," +
                            " CAST(now() as LONG256) l256" + // Semi-random to not change saved txt file result
                            " from long_sequence(0)" +
                            ") timestamp (ts) partition by DAY"
            );

            assertQuery("select distinct * from x")
                    .noLeakCheck()
                    .returns("i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tts\tl\tm\tn\tt\tl256\n");
        });
    }

    @Test
    public void testTimestampAscOrder() throws Exception {
        assertMemoryLeak(() -> {
            String expected = """
                    sym\tts
                    msft\t1970-01-06T18:53:20.000000Z
                    msft\t1970-01-06T18:58:50.000000Z
                    ibm\t1970-01-06T19:04:20.000000Z
                    googl\t1970-01-06T19:09:50.000000Z
                    googl\t1970-01-06T19:15:20.000000Z
                    googl\t1970-01-06T19:20:50.000000Z
                    googl\t1970-01-06T19:26:20.000000Z
                    ibm\t1970-01-06T19:31:50.000000Z
                    msft\t1970-01-06T19:37:20.000000Z
                    ibm\t1970-01-06T19:42:50.000000Z
                    """;
            assertQuery("select distinct sym, ts from x order by ts")
                    .ddl("create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm','googl') sym," +
                            " timestamp_sequence(500000000000L,330000000L) ts" +
                            " from long_sequence(10)" +
                            ") timestamp (ts) partition by DAY")
                    .mutateWith(// duplicate timestamp and symbol shouldn't change the result
                            "insert into x values (11, 'ibm', '1970-01-06T19:42:50.000000Z')")
                    .timestampAsc("ts")
                    .sizeMayVary()
                    .returns(expected, expected);
        });
    }

    @Test
    public void testTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            String expected = """
                    sym\tts
                    ibm\t1970-01-06T19:42:50.000000Z
                    msft\t1970-01-06T19:37:20.000000Z
                    ibm\t1970-01-06T19:31:50.000000Z
                    googl\t1970-01-06T19:26:20.000000Z
                    googl\t1970-01-06T19:20:50.000000Z
                    googl\t1970-01-06T19:15:20.000000Z
                    googl\t1970-01-06T19:09:50.000000Z
                    ibm\t1970-01-06T19:04:20.000000Z
                    msft\t1970-01-06T18:58:50.000000Z
                    msft\t1970-01-06T18:53:20.000000Z
                    """;
            assertQuery("select distinct sym, ts from (x order by ts desc) order by ts desc")
                    .ddl("create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm','googl') sym," +
                            " timestamp_sequence(500000000000L,330000000L) ts" +
                            " from long_sequence(10)" +
                            ") timestamp (ts) partition by DAY")
                    .mutateWith(// duplicate timestamp and symbol shouldn't change the result
                            "insert into x values (11, 'ibm', '1970-01-06T19:42:50.000000Z')")
                    .timestampDesc("ts")
                    .expectSize()
                    .returns(expected, expected);
        });
    }
}
