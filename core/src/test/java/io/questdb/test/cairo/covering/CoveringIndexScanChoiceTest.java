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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The choice, when a query compiles, between a covering-index scan and a scan of the table, from
 * the posting index's per-key counts: a key that selects more than
 * {@code cairo.sql.index.scan.choice.max.selectivity} of the scanned rows takes the scan the
 * {@code no_index} hint gives, and a rare key keeps the covering scan. A covering scan's residual
 * filter and GROUP BY are parallel plans, which reduce the frames on the query's thread when, at
 * open, they hold fewer than {@code cairo.sql.parallel.covering.local.max.rows} rows. Hints that
 * pick the access path are respected, a bind-variable key keeps the covering plan, and every arm
 * returns the rows of the {@code no_index} plan.
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
    public void testFrequentKeyScanKeepsSubQueryPredicates() throws Exception {
        // A frequent key generates the table query a second time, as a scan. The sub-queries of
        // the residual filter are models whose where clauses the first generation consumed: the
        // scan must still read their keys and intervals.
        assertMemoryLeak(() -> {
            createTable();
            execute("create table t2 (ts timestamp, sym symbol index type posting, v long, w double) timestamp(ts) partition by day");
            execute("insert into t2 select ts, sym, v, w from t");
            execute("create table r (ts timestamp, k symbol, x double) timestamp(ts) partition by day");
            execute("insert into r select (x * 1_000_000L)::timestamp, case when x % 2 = 0 then 'a' else 'b' end, x * 3.0 from long_sequence(100_000)");
            final String[] queries = {
                    "select ts, v from t where sym = 'F' and v > (select avg(v) from t where sym = 'R')",
                    "select count(), sum(v) from t where sym = 'F' and w * 2 < (select max(x) from r where k = 'a' and ts in '1970-01-01')",
                    "select count() from t where sym = 'F' and w > (select max(w) from t2 where sym = 'R' and ts in '1970-01-01')",
                    "select ts, v from t where sym = 'F' and w > (select max(w) from t where sym = 'R' and ts in '1970-01-02') limit -5",
                    "select count() from t where sym = 'F' and v < (select max(v) from t2 where sym = 'G' and v < (select avg(v) from t where sym = 'R'))",
            };
            for (String q : queries) {
                assertPlanLacks(q.substring(0, q.indexOf(" and ")), "CoveringIndex");
                assertSameResult(q, q.replaceFirst("select ", "select /*+ no_index */ "));
            }
        });
    }

    @Test
    public void testExplainCoveringScanInsideScalarSubQuery() throws Exception {
        // a covering scan in a scalar sub-query of a filter: EXPLAIN threw a NullPointerException
        assertMemoryLeak(() -> {
            execute("create table s (ts timestamp, sym symbol index type posting include (v, w), v long, w double) timestamp(ts) partition by day");
            execute("insert into s select (x * 1_000_000L)::timestamp, case when x % 2 = 0 then 'F' else 'G' end, x, x / 7.0 from long_sequence(1_000)");
            // the outer filter prints against its base's metadata, which the sub-query's covering
            // scan must not inherit
            final String sql = "select count(), sum(v) from s where sym = 'F' and w < (select min(w) + 10 from s where sym = 'G')";
            final StringSink plan = new StringSink();
            printSql("explain " + sql, plan);
            TestUtils.assertContains(plan, "CoveringIndex on: sym with: w\n");
            TestUtils.assertContains(plan, "filter: sym='G'");
            assertSameResult(sql, sql.replace("select count()", "select /*+ no_index */ count()"));
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
    public void testRareKeyKeepsCoveringAndSmallWorkRunsLocally() throws Exception {
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
            // the residual filter and the GROUP BY are parallel plans; R's ~4.9K rows are fewer than
            // the local reduce threshold, so each open reduces the frames on the query's thread
            final String[] parallel = {
                    "select ts, v from t where sym = 'R' and w > 10",
                    "select timestamp_floor('1h', ts) h, s2, avg(v) a from t where sym = 'R' order by h, s2",
                    "select count(), sum(v) from t where sym = 'R'",
            };
            for (String q : parallel) {
                assertPlanContains(q, "CoveringIndex");
                assertPlanContains(q, "Async");
                final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                assertSameResult(q, q.replace("select ", "select /*+ no_index */ "));
                final long[] after = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                Assert.assertTrue(q, after[0] > before[0]);
                Assert.assertEquals(q, before[1], after[1]);
            }
            // a frequent key's covering scan, which use_index keeps, dispatches its frames
            final String frequent = "select /*+ use_index */ count(), sum(v) from t where sym = 'F'";
            assertPlanContains(frequent, "CoveringIndex");
            final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
            assertSameResult(frequent, frequent.replace("use_index", "no_index"));
            Assert.assertTrue(UnorderedPageFrameSequence.getLocalReduceOpensForTesting()[1] > before[1]);
        });
    }

    @Test
    public void testLocalReduceIsDecidedPerOpen() throws Exception {
        // a cached plan compiled while a key has few rows dispatches its frames once the key has
        // many, and the other way round
        assertMemoryLeak(() -> {
            createTable();
            final String[] queries = {
                    "select count(), sum(v), min(ts), max(ts) from t where sym = 'NEW'",
                    "select ts, v from t where sym = 'NEW' and w > 0",
            };
            execute("insert into t select (259_200_000_000L + 30_000 * 216_000L + x * 1_000L)::timestamp, 'NEW', 's1', x, x / 3.0 from long_sequence(1_000)");
            try (RecordCursorFactory grouped = select(queries[0]); RecordCursorFactory filtered = select(queries[1])) {
                final RecordCursorFactory[] factories = {grouped, filtered};
                for (int i = 0; i < factories.length; i++) {
                    final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                    assertFactoryResult(factories[i], queries[i]);
                    final long[] after = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                    Assert.assertTrue(queries[i], after[0] > before[0]);
                    Assert.assertEquals(queries[i], before[1], after[1]);
                }
                execute("insert into t select (259_200_000_000L + 31_000 * 216_000L + x * 1_000L)::timestamp, 'NEW', 's1', x, x / 3.0 from long_sequence(300_000)");
                for (int i = 0; i < factories.length; i++) {
                    final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                    assertFactoryResult(factories[i], queries[i]);
                    final long[] after = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                    Assert.assertTrue(queries[i], after[1] > before[1]);
                    Assert.assertEquals(queries[i], before[0], after[0]);
                }
            }
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

    private void assertFactoryResult(RecordCursorFactory factory, String query) throws Exception {
        final StringSink actual = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            println(factory.getMetadata(), cursor, actual);
        }
        final StringSink expected = new StringSink();
        printSql(query.replaceFirst("select ", "select /*+ no_index */ "), expected);
        TestUtils.assertEquals(query, expected, actual);
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
