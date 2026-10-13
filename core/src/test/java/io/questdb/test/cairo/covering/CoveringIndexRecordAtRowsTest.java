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

import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * A cursor that reads its base again by row id (a light sort) declares those rows
 * (setRecordAtRows), and a covered frame then decodes them alone rather than every row of every
 * INCLUDE column; a window over a covering scan caches its rows instead. Each query is compared
 * with its scan plan; the decode counter proves the declared-rows decode ran.
 */
public class CoveringIndexRecordAtRowsTest extends AbstractCoveringIndexQueryTest {

    @Test
    public void testDeclaredRowsDecodeAlone() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table q (ts timestamp, sym symbol, ex symbol, v double, l long, vc varchar) timestamp(ts) partition by day bypass wal");
            execute("insert into q select (x * 1_000_000L)::timestamp, case when x % 3 = 0 then 'B' else 'A' end, "
                    + "rnd_symbol('N','P','T',null), (x * 7919 % 1000)::double, x, "
                    + "case when x % 11 = 0 then null else rnd_varchar(1, 12, 0) end from long_sequence(150_000)");
            execute("alter table q alter column sym add index type posting include (ex, v, l, vc)");
            // and rows after the index was built, with a NULL extreme
            execute("insert into q select (200_000_000_000L + x * 1_000_000L)::timestamp, 'A', 'P', "
                    + "case when x = 7 then null else (x % 500)::double end, -x, 'z' || x from long_sequence(5_000)");
            // windows over a covering scan cache the rows the filter's workers decoded, rather than
            // read them back by id
            final String[] windows = {
                    // the shape of the NYSE TAQ query 58: a Min/Max Filter under a window
                    "select * from (select ts, ex, v, l, vc, min(ts) over (partition by ex) mt from "
                            + "(select ts, ex, v, l, vc, min(v) over (partition by ex) mv from q where sym = 'A') where v = mv) where ts = mt",
                    "select ts, ex, v, l, vc, max(l) over (partition by ex) ml from (select ts, ex, v, l, vc from q where sym = 'A' and v > 990)",
            };
            for (String query : windows) {
                final String covered = hinted(query, "use_index");
                assertPlanContains(covered, "CoveringIndex");
                assertPlanContains(covered, "CachedWindow");
                assertPlanLacks(covered, "CachedWindowLight");
                assertSameResult(covered, hinted(query, "no_index"));
            }
            // a light sort reads its rows back by id: a covered frame decodes them alone
            final String sort = "select ts, ex, v, l, vc from q where sym = 'A' and v < 3 order by vc, ts";
            final String covered = hinted(sort, "use_index");
            assertPlanContains(covered, "CoveringIndex");
            final long before = PageFrameMemoryPool.getCoveredRowDecodeCount();
            assertSameResult(covered, hinted(sort, "no_index"));
            Assert.assertTrue(PageFrameMemoryPool.getCoveredRowDecodeCount() > before);
        });
    }

    private void assertPlanContains(String query, String fragment) throws Exception {
        Assert.assertTrue(query, plan(query).contains(fragment));
    }

    private void assertPlanLacks(String query, String fragment) throws Exception {
        Assert.assertFalse(query, plan(query).contains(fragment));
    }

    private String plan(String query) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + query, plan);
        return plan.toString();
    }

    private static String hinted(String query, String hint) {
        return query.replace("select ts, ex, v, l, vc", "select /*+ " + hint + " */ ts, ex, v, l, vc");
    }
}
