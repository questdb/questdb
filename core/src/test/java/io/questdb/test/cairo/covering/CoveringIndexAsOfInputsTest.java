/*******************************************************************************
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

package io.questdb.test.cairo.covering;

import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * The tables an ASOF, LT or SPLICE JOIN reads take the scan, not the covering-index plan: the
 * joins read their slave by time frames and their parallel plans a master's filter over a scan.
 * A key that selects few rows keeps the covering plan, which reads just those.
 * The same query outside a join keeps the covering plan. Each join is compared with the linear
 * ASOF JOIN of the same inputs.
 */
public class CoveringIndexAsOfInputsTest extends AbstractCoveringIndexQueryTest {

    @Test
    public void testSelectiveAsOfInputsKeepTheCoveringPlan() throws Exception {
        // R is 1 row in 2000 of each, well under ASOF_INPUT_COVERING_MAX_SELECTIVITY: the covering index reads
        // its few rows, faster than a plan over a scan of the whole table; F, 1 in 5, takes the scan
        assertMemoryLeak(() -> {
            execute("create table tr (ts timestamp, sym symbol, px double, sz long) timestamp(ts) partition by day bypass wal");
            execute("create table qu (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by day bypass wal");
            execute("insert into tr select (x * 70_000L)::timestamp, case when x % 2000 = 0 then 'R' when x % 5 = 1 then 'F' else 'k' || (x % 40) end, "
                    + "x / 3.0, x from long_sequence(1_200_000)");
            execute("insert into qu select (x * 20_000L)::timestamp, case when x % 2000 = 0 then 'R' when x % 5 = 1 then 'F' else 'k' || (x % 40) end, "
                    + "x / 7.0, x / 5.0 from long_sequence(4_200_000)");
            execute("alter table tr alter column sym add index type posting include (px, sz)");
            execute("alter table qu alter column sym add index type posting include (bid, ask)");
            for (String key : new String[]{"R", "F"}) {
                final String join = "select t.ts, t.px, t.sz, q.bid, q.ask from (select ts, px, sz from tr where sym = '" + key + "') t "
                        + "asof join (select ts, bid, ask from qu where sym = '" + key + "') q";
                final StringSink plan = new StringSink();
                printSql("explain " + join, plan);
                if (key.equals("R")) {
                    TestUtils.assertContains(join, plan, "CoveringIndex");
                } else {
                    TestUtils.assertNotContains(plan, "CoveringIndex");
                }
                assertSameResult(join, join.replace("select t.ts", "select /*+ asof_linear(t q) */ t.ts"));
            }
        });
    }

    @Test
    public void testAsOfInputsTakeTheScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tr (ts timestamp, sym symbol, px double, sz long) timestamp(ts) partition by day bypass wal");
            execute("create table qu (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by day bypass wal");
            // X is 1 row in 20 of each, well under the covering choice's share
            execute("insert into tr select (x * 7_000_000L)::timestamp, case when x % 20 = 0 then 'X' else 'k' || (x % 40) end, x / 3.0, x "
                    + "from long_sequence(200_000)");
            execute("insert into qu select (x * 2_000_000L)::timestamp, case when x % 20 = 0 then 'X' else 'k' || (x % 40) end, x / 7.0, x / 5.0 "
                    + "from long_sequence(700_000)");
            execute("alter table tr alter column sym add index type posting include (px, sz)");
            execute("alter table qu alter column sym add index type posting include (bid, ask)");
            final String inner = "select ts, px, sz from tr where sym = 'X'";
            final StringSink plan = new StringSink();
            printSql("explain " + inner, plan);
            TestUtils.assertContains(plan, "CoveringIndex");
            final String[] joins = {
                    // both sides filtered to one symbol, joined on time alone
                    "select t.ts, t.px, t.sz, q.bid, q.ask from (" + inner + ") t asof join (select ts, bid, ask from qu where sym = 'X') q",
                    "select t.ts, t.px, q.bid from (select ts, sym, px from tr where sym = 'X') t asof join (select ts, sym, bid from qu where sym = 'X') q on (sym)",
                    "select t.ts, t.px, q.bid from (" + inner + ") t lt join (select ts, bid, ask from qu where sym = 'X') q",
            };
            for (String join : joins) {
                plan.clear();
                printSql("explain " + join, plan);
                TestUtils.assertNotContains(plan, "CoveringIndex");
                final String linear = join.replace("select t.ts", "select /*+ asof_linear(t q) */ t.ts");
                assertSameResult(join, linear);
            }
        });
    }
}
