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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Does every factory that can read a composite table account for CELLS?
 * <p>
 * A composite table turns one day into several partitions, so anything that walks partitions can be
 * cell-blind. Rather than reason about ~50 factory classes, this drives one query per factory FAMILY
 * against a composite table and its plain twin and requires identical answers — and asserts the PLAN
 * for each, so the day a query stops being routed the way this file assumes, it fails here instead of
 * silently testing a different code path.
 * <p>
 * The survey behind it found three distinct treatments, all of which are asserted below:
 * <ol>
 *     <li><b>Routed through the composite merge</b> — full and interval scans in both directions,
 *     filters, LATEST ON, SAMPLE BY, LIMIT, window functions, joins, unions, ORDER BY. These see cells
 *     merged into timestamp order.</li>
 *     <li><b>Deliberately NOT routed through it</b> — {@code count()}, keyed {@code GROUP BY} and
 *     {@code DISTINCT} run over a raw {@code PageFrame} scan. That is safe precisely because they are
 *     order-independent: a frame scan still visits every cell, and these consumers do not care in which
 *     order. Asserted as a bypass ON PURPOSE, so nobody "fixes" it by routing them through the merge and
 *     paying for a sort nothing needs.</li>
 *     <li><b>Index scans</b> — an indexed EQUALITY on the dimension column is allowed and runs an index
 *     scan per partition, outside the merge. Safe because the predicate selects a single cell per day,
 *     so per-partition iteration still reaches every relevant cell. Other indexed predicates (a
 *     non-dimension column, or an excluded-values predicate) are REFUSED loudly.</li>
 * </ol>
 */
public class CompositeFactoryCoverageTest extends AbstractCairoTest {

    /**
     * Data with the property that makes cell-blindness visible: cells whose rows do NOT all span the
     * queried windows, spread over three days.
     */
    private static final String ROWS =
            "('2023-01-01T01:00:00.000000Z','E0','S0',1.0),"
                    + "('2023-01-01T05:00:00.000000Z','E1','S1',5.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0','S0',11.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0','S1',13.0),"
                    + "('2023-01-02T02:00:00.000000Z','E1','S0',12.0),"
                    + "('2023-01-02T04:00:00.000000Z','E2','S1',14.0),"
                    + "('2023-01-03T02:00:00.000000Z','E1','S0',22.0),"
                    + "('2023-01-03T06:00:00.000000Z','E2','S1',26.0)";

    /**
     * Every family that IS routed through the composite cross-cell merge.
     */
    @Test
    public void testMergeRoutedFamiliesMatchTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final String order = " order by ts, exch, sym, px";
            assertTwinAndPlan("select * from %s" + order, "Composite cross-cell merge scan");
            assertTwinAndPlan("select ts from %s order by ts desc", "Composite cross-cell merge scan");
            assertTwinAndPlan("select * from %s where ts = '2023-01-02T02:00:00.000000Z'" + order,
                    "Composite cross-cell merge scan");
            assertTwinAndPlan("select ts from %s where ts >= '2023-01-02T00:00:00.000000Z' order by ts desc",
                    "Composite cross-cell merge scan");
            assertTwinAndPlan("select * from %s where px > 12" + order, "Composite cross-cell merge scan");
            assertTwinAndPlan("select * from %s latest on ts partition by sym order by sym",
                    "Composite cross-cell merge scan");
            assertTwinAndPlan("select ts, count() from %s sample by 1d order by ts",
                    "Composite cross-cell merge scan");
            assertTwinAndPlan("select * from %s" + order + " limit 3", "Composite cross-cell merge scan");
            assertTwinAndPlan("select ts, exch, row_number() over (partition by exch order by ts) rn from %s"
                    + " order by ts, exch", "Composite cross-cell merge scan");
            assertTwinAndPlan("select ts from %s union all select ts from %s order by ts",
                    "Composite cross-cell merge scan");
            assertTwinAndPlan("select * from %s order by px, ts", "Composite cross-cell merge scan");
        });
    }

    /**
     * The order-independent aggregates that deliberately bypass the merge. Asserted as a bypass so the
     * choice stays visible: routing them through the merge would buy nothing and cost a merge.
     */
    @Test
    public void testOrderIndependentFamiliesBypassMergeAndStillMatch() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            assertTwinAndPlanLacking("select count() from %s", "Composite cross-cell merge scan");
            assertTwinAndPlanLacking("select exch, count() from %s order by exch", "Composite cross-cell merge scan");
            assertTwinAndPlanLacking("select distinct exch from %s order by exch", "Composite cross-cell merge scan");
            // min/max over the DESIGNATED timestamp is NOT in this group -- measured, not assumed: it
            // routes through the merge, presumably because the optimisation that answers it from the
            // ends of the scan needs the merged order. Asserted as merge-routed so the distinction is
            // recorded rather than guessed at.
            assertTwinAndPlan("select min(ts), max(ts) from %s", "Composite cross-cell merge scan");
        });
    }

    /**
     * An indexed equality on the DIMENSION column: allowed, runs an index scan outside the merge, and
     * must still agree with the twin -- including when it feeds an operator that cares about order.
     */
    @Test
    public void testIndexedDimensionEqualityMatchesTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(true);
            final String order = " order by ts, exch, sym, px";
            assertTwinAndPlan("select * from %s where exch = 'E1'" + order, "Index forward scan");
            assertTwin("select * from %s where exch = 'E1' and ts = '2023-01-02T02:00:00.000000Z'" + order);
            assertTwin("select ts from %s where exch = 'E1' order by ts desc");
            assertTwin("select count() from %s where exch = 'E1'");
            // ... and feeding consumers that DO care about order
            assertTwin("select ts, count() from (select * from %s where exch = 'E1') sample by 1d order by ts");
            assertTwin("select * from (select * from %s where exch = 'E1') latest on ts partition by exch");
        });
    }

    /**
     * The indexed predicates composite refuses. A loud refusal is the contract; silently running a
     * cell-blind index scan would not be.
     */
    @Test
    public void testUnsupportedIndexedPredicatesAreRefused() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(true);
            assertRefused("select * from c where sym = 'S0'", "indexed WHERE predicate");
            assertRefused("select * from c where exch != 'E0'", "indexed WHERE predicate");
        });
    }

    /**
     * The same families again under REAL parallelism: four workers and 1-2 row page frames, so a
     * multi-cell day is split into many frames and distributed across threads. Every other test of
     * these factories runs single-worker (plans read "workers: 1"), which cannot expose a frame
     * distribution that loses or double-counts a cell.
     */
    @Test
    public void testFamiliesMatchTwinUnderParallelExecution() throws Exception {
        node1.setProperty(PropertyKey.SHARED_WORKER_COUNT, 4);
        node1.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        node1.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
        assertMemoryLeak(() -> {
            createTwins(false);
            final String order = " order by ts, exch, sym, px";
            assertTwin("select * from %s where px > 12" + order);
            assertTwin("select exch, count(), sum(px) from %s order by exch");
            assertTwin("select exch, sym, count() from %s order by exch, sym");
            assertTwin("select ts, count(), sum(px) from %s sample by 1d order by ts");
            assertTwin("select count() from %s");
            assertTwin("select * from %s where px > 12 and ts >= '2023-01-02T00:00:00.000000Z'"
                    + " and ts < '2023-01-03T00:00:00.000000Z'" + order);
            assertTwin("select distinct exch from %s order by exch");
            assertTwin("select * from %s latest on ts partition by sym order by sym");
            assertTwin("select count() from (select * from %s a asof join %s b on (exch))");
        });
    }

    private void assertRefused(String sql, String expectedMessage) {
        try {
            printSql(sql);
            Assert.fail("expected a composite gate to refuse: " + sql);
        } catch (Throwable expected) {
            TestUtils.assertContains(expected.getMessage(), expectedMessage);
        }
    }

    /**
     * Runs {@code template} (with {@code %s} as the table name) against both twins and requires
     * identical output.
     */
    private void assertTwin(String template) throws SqlException {
        assertSqlCursors(template.replace("%s", "p"), template.replace("%s", "c"));
    }

    private void assertTwinAndPlan(String template, String expectedPlanFragment) throws SqlException {
        assertTwin(template);
        final StringSink plan = new StringSink();
        printSql("explain " + template.replace("%s", "c"), plan);
        TestUtils.assertContains(plan, expectedPlanFragment);
    }

    private void assertTwinAndPlanLacking(String template, String absentPlanFragment) throws SqlException {
        assertTwin(template);
        final StringSink plan = new StringSink();
        printSql("explain " + template.replace("%s", "c"), plan);
        Assert.assertFalse("expected this family to BYPASS the composite merge, but the plan contains \""
                        + absentPlanFragment + "\":\n" + plan,
                io.questdb.std.Chars.contains(plan, absentPlanFragment));
    }

    private void createTwins(boolean indexed) throws SqlException {
        final String idx = indexed ? " index" : "";
        execute("create table c (ts timestamp, exch symbol" + idx + ", sym symbol" + idx + ", px double)"
                + " timestamp(ts) partition by day, exch layout plain wal");
        execute("create table p (ts timestamp, exch symbol" + idx + ", sym symbol" + idx + ", px double)"
                + " timestamp(ts) partition by day wal");
        execute("insert into c values " + ROWS);
        execute("insert into p values " + ROWS);
        drainWalQueue();
    }
}
