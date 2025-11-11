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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.cairo.SqlJitMode;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InLongFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBindVariables() throws Exception {
        bindVariableService.clear();
        bindVariableService.setLong(0, 4);
        bindVariableService.setLong(1, 2);
        assertQuery(
                """
                        x
                        2
                        4
                        """,
                "select * from x where x in ($1,$2)",
                "create table x as (" +
                        "select x from long_sequence(10)" +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testFewBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE bench AS (SELECT rnd_long(1, 1000, 0) l FROM long_sequence(1_000_000));");

            StringSink sink = new StringSink();
            Rnd rnd = new Rnd(123, 456);
            sink.put("SELECT DISTINCT l FROM bench WHERE l IN (");

            bindVariableService.clear();
            for (int i = 0; i < 5; i++) {
                bindVariableService.setLong(i, rnd.nextLong(1000));
                sink.put("$").put(i + 1).put(',');
            }

            sink.trimTo(sink.length() - 1);
            sink.put(") ORDER BY l LIMIT 5;");

            assertSql("""
                    l
                    69
                    143
                    280
                    291
                    683
                    """, sink);

            if (engine.getConfiguration().getSqlJitMode() == SqlJitMode.JIT_MODE_ENABLED) {
                assertPlanNoLeakCheck(sink, """
                        Long Top K lo: 5
                          keys: [l asc]
                            Async JIT Group By workers: 1
                              keys: [l]
                              filter: l in [69,143,280,291,683]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: bench
                        """);
            } else {
                assertPlanNoLeakCheck(sink, """
                        Long Top K lo: 5
                          keys: [l asc]
                            Async Group By workers: 1
                              keys: [l]
                              filter: l in [69,143,280,291,683]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: bench
                        """);
            }


            sink.clear();
        });
    }

    @Test
    public void testManyBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE bench AS (SELECT rnd_long(1, 1000, 0) l FROM long_sequence(1_000_000));");

            StringSink sink = new StringSink();
            Rnd rnd = new Rnd(123, 456);
            sink.put("SELECT DISTINCT l FROM bench WHERE l IN (");

            bindVariableService.clear();
            for (int i = 0; i < 200; i++) {
                bindVariableService.setLong(i, rnd.nextLong(1000));
                sink.put("$").put(i + 1).put(',');
            }

            sink.trimTo(sink.length() - 1);
            sink.put(") ORDER BY l LIMIT 5;");

            assertSql("""
                    l
                    2
                    5
                    9
                    18
                    20
                    """, sink);

            // should be the same, JIT or no JIT
            assertPlanNoLeakCheck(sink, """
                    Long Top K lo: 5
                      keys: [l asc]
                        Async Group By workers: 1
                          keys: [l]
                          filter: l in [2,5,9,18,20,22,36,42,43,54,58,61,63,65,69,73,76,80,87,92,101,103,108,115,116,122,125,126,128,129,143,144,145,148,151,168,172,173,177,199,208,210,212,223,237,251,254,259,271,274,280,281,282,283,291,292,296,298,299,300,302,303,305,321,322,332,335,359,361,367,372,378,380,384,394,400,402,403,406,417,426,430,440,444,466,468,471,474,476,477,479,489,490,494,499,500,515,520,531,532,540,541,549,551,553,554,558,580,582,584,591,594,600,601,603,605,613,620,628,646,647,650,667,669,674,675,683,690,692,695,710,722,727,729,743,746,778,779,780,787,788,789,793,798,802,811,815,818,821,822,832,835,836,839,842,843,847,852,860,862,866,875,877,892,897,898,900,907,908,909,920,925,934,937,948,963,969,977,979,982,995]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: bench
                    """);


            sink.clear();
        });
    }

    @Test
    public void testManyConst() throws Exception {
        assertQuery(
                """
                        x
                        1
                        3
                        5
                        7
                        """,
                "select * from x where x in (7,5,3,1)",
                "create table x as (" +
                        "select x from long_sequence(10)" +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testSingleConst() throws Exception {
        assertQuery(
                """
                        x
                        1
                        """,
                "select * from x where x in (1)",
                "create table x as (" +
                        "select x from long_sequence(5)" +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testTwoConst() throws Exception {
        assertQuery(
                """
                        x
                        1
                        2
                        """,
                "select * from x where x in (2,1)",
                "create table x as (" +
                        "select x from long_sequence(5)" +
                        ")",
                null,
                true,
                false
        );
    }
}
