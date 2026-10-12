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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The choice, when a query compiles, between a covering-index scan and a scan of the table, from
 * the posting index's per-key counts: a key that selects more than
 * {@code cairo.sql.index.scan.choice.max.selectivity} of the scanned rows takes the scan the
 * {@code no_index} hint gives, a rare key keeps the covering scan, and a covering scan that selects
 * fewer rows than one parallel task keeps its residual filter and its GROUP BY on the query's
 * thread. Hints that pick the access path are respected, a bind-variable key keeps the covering
 * plan, and every arm returns the rows of the {@code no_index} plan.
 * <p>
 * The table holds 1.2M rows (above one full-scan page frame, under which nothing changes): key F
 * about 20% of them, R about 0.4%.
 */
public class CoveringIndexScanChoiceTest extends AbstractCoveringIndexQueryTest {

    @Test
    public void testBindVariableKeyKeepsCovering() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            bindVariableService.clear();
            bindVariableService.setStr(0, "F");
            assertPlanContains("select ts, v from t where sym = $1", "CoveringIndex");
            assertSameResult("select ts, v from t where sym = $1", "select /*+ no_index */ ts, v from t where sym = $1");
        });
    }

    @Test
    public void testChoiceDisabledKeepsCovering() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTable();
            assertPlanContains("select ts, v from t where sym = 'F'", "CoveringIndex");
            assertPlanContains("select count(), sum(v) from t where sym = 'R'", "Async");
        });
    }

    @Test
    public void testFrequentKeyTakesScan() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String[] queries = {
                    "select ts, v from t where sym = 'F'",
                    "select ts, (v + w) / 2 mid from t where sym = 'F'",
                    "select ts, v from t where sym = 'F' and w > 10",
                    "select avg(v) from t where sym = 'F'",
                    "select timestamp_floor('1h', ts) h, s2, avg(v) from t where sym = 'F' order by h, s2",
                    "select ts, v from t where sym in ('F', 'G')",
            };
            for (String q : queries) {
                assertPlanLacks(q, "CoveringIndex");
                assertSameResult(q, q.replace("select ", "select /*+ no_index */ "));
            }
            // the interval narrows the scan below one full-scan frame (400K rows): the covering plan stays
            final String slice = "select ts, v from t where sym = 'F' and ts in '1970-01-02'";
            assertPlanContains(slice, "CoveringIndex");
            assertSameResult(slice, slice.replace("select ", "select /*+ no_index */ "));
        });
    }

    @Test
    public void testHintsPickThePath() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // use_index and force_use_covering keep the covering scan of a frequent key
            assertPlanContains("select /*+ use_index */ ts, v from t where sym = 'F'", "CoveringIndex");
            assertPlanContains("select /*+ force_use_covering */ ts, v from t where sym = 'F'", "CoveringIndex");
            assertSameResult("select /*+ use_index */ ts, v from t where sym = 'F'", "select /*+ no_index */ ts, v from t where sym = 'F'");
            // no_covering and no_index keep their plans for a rare key
            assertPlanLacks("select /*+ no_covering */ ts, v from t where sym = 'R'", "CoveringIndex");
            assertPlanLacks("select /*+ no_index */ ts, v from t where sym = 'R'", "CoveringIndex");
        });
    }

    @Test
    public void testRareKeyKeepsCoveringAndRunsSmallWorkSerially() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String[] queries = {
                    "select ts, v from t where sym = 'R'",
                    "select ts, (v + w) / 2 mid from t where sym = 'R'",
                    "select ts, v from t where sym = 'absent'",
            };
            for (String q : queries) {
                assertPlanContains(q, "CoveringIndex");
                assertSameResult(q, q.replace("select ", "select /*+ no_index */ "));
            }
            // fewer rows than a parallel task: the residual filter and the GROUP BY stay serial
            final String filtered = "select ts, v from t where sym = 'R' and w > 10";
            assertPlanContains(filtered, "CoveringIndex");
            assertPlanLacks(filtered, "Async");
            assertSameResult(filtered, filtered.replace("select ", "select /*+ no_index */ "));
            final String grouped = "select timestamp_floor('1h', ts) h, s2, avg(v) a from t where sym = 'R' order by h, s2";
            assertPlanContains(grouped, "CoveringIndex");
            assertPlanLacks(grouped, "Async");
            assertSameResult(grouped, grouped.replace("select ", "select /*+ no_index */ "));
            final String notKeyed = "select count(), sum(v) from t where sym = 'R'";
            assertPlanLacks(notKeyed, "Async");
            assertSameResult(notKeyed, notKeyed.replace("select ", "select /*+ no_index */ "));
        });
    }

    @Test
    public void testSmallTableKeepsPlans() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table s (ts timestamp, sym symbol index type posting include (v), v long) timestamp(ts) partition by day");
            execute("insert into s select (x * 1_000_000L)::timestamp, case when x % 2 = 0 then 'F' else 'R' end, x from long_sequence(50_000)");
            assertPlanContains("select ts, v from s where sym = 'F'", "CoveringIndex");
            assertPlanContains("select count(), sum(v) from s where sym = 'R'", "Async");
        });
    }

    private void assertPlanContains(String query, String fragment) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + query, plan);
        Assert.assertTrue(query + '\n' + plan, plan.toString().contains(fragment));
    }

    private void assertPlanLacks(String query, String fragment) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + query, plan);
        Assert.assertFalse(query + '\n' + plan, plan.toString().contains(fragment));
    }

    private void createTable() throws Exception {
        execute("create table t (ts timestamp, sym symbol, s2 symbol, v long, w double) timestamp(ts) partition by day");
        // 1.2M rows over 3 days (one 1M-row frame is the floor of the choice)
        execute("insert into t select (x * 216_000L)::timestamp,"
                + " case when x % 5 = 0 then 'F' when x % 250 = 1 then 'R' when x % 5 = 1 then 'G' else 'k' || (x % 97) end,"
                + " 's' || (x % 3), x, x / 7.0 from long_sequence(1_200_000)");
        execute("alter table t alter column sym add index type posting include (s2, v, w)");
        // and rows after the index was built
        execute("insert into t select (259_200_000_000L + x * 216_000L)::timestamp,"
                + " case when x % 5 = 0 then 'F' when x % 250 = 1 then 'R' else 'k' || (x % 97) end,"
                + " 's' || (x % 3), x, x / 7.0 from long_sequence(30_000)");
        final StringSink plan = new StringSink();
        printSql("explain select /*+ use_index */ ts, v from t where sym = 'R'", plan);
        TestUtils.assertContains(plan, "CoveringIndex");
    }
}
