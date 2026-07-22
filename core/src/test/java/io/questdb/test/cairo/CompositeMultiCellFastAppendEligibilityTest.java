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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Composite MULTI-cell fast-append eligibility (composite-partitioning fast-append spec 2, Task 1
 * -- detection only; see {@code .superpowers/sdd/task-1-brief.md}). {@link
 * TableWriter#isCompositeMultiCellFastAppendPossible} and the {@code
 * cairo.wal.composite.fastappend.max.open.cells} cap are wired into the WAL-commit path as a COUNTER
 * ONLY, exactly like spec 1's single-cell analog ({@link CompositeFastAppendEligibilityTest}): an
 * eligible commit still takes the existing full O3 composite path, unchanged. A later task makes an
 * eligible commit actually fast-append.
 * <p>
 * These tests prove, via {@link TableWriter#getCompositeMultiCellFastAppendEligibleCount()} (a
 * static, JVM-wide counter -- every assertion below brackets one specific commit with a BEFORE/AFTER
 * delta, so it stays correct regardless of what any other commit in this JVM has already counted):
 * <ul>
 *     <li>a multi-cell commit, globally timestamp-ordered, landing strictly after every touched
 *     cell's own committed max, is eligible;</li>
 *     <li>a multi-cell commit that is globally OUT-OF-ORDER but internally ordered PER CELL is also
 *     eligible -- this predicate independently verifies per-cell ordering rather than gating on the
 *     WAL sequencer's whole-commit {@code ordered} flag (see {@link
 *     TableWriter#isCompositeMultiCellFastAppendPossible}'s own docs for why: that flag only reflects
 *     global order, and composite-partitioning fast-append spec 2 exists precisely because
 *     independent per-cell producer streams routinely interleave into a globally-unordered but
 *     per-cell-ordered shape);</li>
 *     <li>a single-cell commit is never counted here (spec 1's own branch's scope);</li>
 *     <li>one cell landing before ITS OWN committed max vetoes the WHOLE commit (all-or-nothing),
 *     even when every other touched cell is fine;</li>
 *     <li>a brand-new, never-committed cell in the mix vetoes the whole commit;</li>
 *     <li>a commit spanning more distinct cells than the configured {@code
 *     cairo.wal.composite.fastappend.max.open.cells} cap is not eligible;</li>
 *     <li>a commit spanning two days is not eligible (mirrors spec 1's own last-partition gate);</li>
 *     <li>composite query results still exactly match a plain twin throughout -- this task changes
 *     detection/counting only, never behavior (no early return is ever taken for a multi-cell
 *     commit);</li>
 *     <li>with the flag off, an otherwise multi-cell-eligible commit never increments the counter.</li>
 * </ul>
 */
public class CompositeMultiCellFastAppendEligibilityTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testFlagOffMultiCellEligibleCommitDoesNotIncrementCounter() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, "false");

            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Commit 0: the table's very own first-ever commit -- routes the table but never reaches
            // any hook (isRoutedComposite() is false until this commit's own real per-cell dispatch
            // interns the first cell; see CompositeFastAppendEligibilityTest, spec 1's analogous
            // gotcha). Not a meaningful assertion by itself.
            execute("insert into c values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            seedCell("A", "2020-01-01T00:10:00.000000Z", 1.0);
            seedCell("B", "2020-01-01T00:10:00.000000Z", 2.0);

            // A genuinely multi-cell, ordered, append-only-into-both-cells commit -- eligible if the
            // flag were on (proven by testMultiCellEligibilityScenarios' scenario (a)). With the flag
            // off, the whole hook (including isRoutedComposite() and the predicate call) is skipped
            // before any of that is even evaluated -- must not increment.
            long before = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T00:20:00.000000Z','A',1.1)," +
                    "('2020-01-01T00:20:01.000000Z','B',2.1)");
            execute("insert into p values " +
                    "('2020-01-01T00:20:00.000000Z','A',1.1)," +
                    "('2020-01-01T00:20:01.000000Z','B',2.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "flag off: an otherwise multi-cell-eligible commit must not increment the counter",
                    before, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            engine.releaseInactive();
            assertWalTableNotSuspended("p");
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
        });
    }

    @Test
    public void testMultiCellCommitExceedingMaxOpenCellsCapNotEligible() throws Exception {
        assertMemoryLeak(() -> {
            // Set low BEFORE any table/writer for this test exists (mirrors how
            // CompositeFastAppendEligibilityTest.testFlagOffEligibleCommitDoesNotIncrementCounter
            // safely overrides a property mid-test: before the table it affects is created).
            setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_MAX_OPEN_CELLS, "2");

            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            execute("insert into c values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            seedCell("J", "2020-01-01T00:10:00.000000Z", 1.0);
            seedCell("K", "2020-01-01T00:10:00.000000Z", 2.0);
            seedCell("L", "2020-01-01T00:10:00.000000Z", 3.0);

            // Boundary check: exactly 2 distinct cells, at (not over) the cap, otherwise eligible --
            // must still be eligible. Proves the cap comparison is "<=", not an off-by-one "<".
            long beforeAtCap = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T00:20:00.000000Z','J',1.1)," +
                    "('2020-01-01T00:20:01.000000Z','K',2.1)");
            execute("insert into p values " +
                    "('2020-01-01T00:20:00.000000Z','J',1.1)," +
                    "('2020-01-01T00:20:01.000000Z','K',2.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "exactly 2 distinct cells at a configured cap of 2 should still be eligible",
                    beforeAtCap + 1, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            // 3 distinct cells, all pre-existing, ordered, append-only into every one of them -- would
            // be eligible under the default cap (64, proven by scenario (a)), but the cap here is 2.
            long before = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T00:30:00.000000Z','J',1.2)," +
                    "('2020-01-01T00:30:01.000000Z','K',2.2)," +
                    "('2020-01-01T00:30:02.000000Z','L',3.1)");
            execute("insert into p values " +
                    "('2020-01-01T00:30:00.000000Z','J',1.2)," +
                    "('2020-01-01T00:30:01.000000Z','K',2.2)," +
                    "('2020-01-01T00:30:02.000000Z','L',3.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "3 distinct cells exceeding a configured cap of 2 must not be eligible",
                    before, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            engine.releaseInactive();
            assertWalTableNotSuspended("p");
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
        });
    }

    @Test
    public void testMultiCellEligibilityScenarios() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Commit 0: the table's very own first-ever commit -- routes the table but never reaches
            // any hook (see the flag-off test's own comment for why). Each scenario below uses its own
            // never-reused exch values so scenarios cannot interfere with each other's seeded state.
            execute("insert into c values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // ---- Scenario (a): multi-cell, GLOBALLY ordered, append-only into every cell ----
            seedCell("A1", "2020-01-01T00:10:00.000000Z", 1.0);
            seedCell("B1", "2020-01-01T00:10:00.000000Z", 2.0);
            seedCell("C1", "2020-01-01T00:10:00.000000Z", 3.0);

            long beforeA = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T00:20:00.000000Z','A1',1.1)," +
                    "('2020-01-01T00:20:01.000000Z','B1',2.1)," +
                    "('2020-01-01T00:20:02.000000Z','C1',3.1)");
            execute("insert into p values " +
                    "('2020-01-01T00:20:00.000000Z','A1',1.1)," +
                    "('2020-01-01T00:20:01.000000Z','B1',2.1)," +
                    "('2020-01-01T00:20:02.000000Z','C1',3.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "multi-cell, globally ordered, append-only into every cell should be eligible",
                    beforeA + 1, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            // ---- Scenario (b): multi-cell, globally OUT-OF-ORDER, each cell internally ordered ----
            seedCell("A2", "2020-01-01T00:10:00.000000Z", 1.0);
            seedCell("B2", "2020-01-01T00:10:00.000000Z", 2.0);

            long beforeB = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T00:40:00.000000Z','A2',1.1)," + // A2: 00:40
                    "('2020-01-01T00:20:00.000000Z','B2',2.1)," + // B2: 00:20 -- globally OOO vs 00:40 above
                    "('2020-01-01T00:41:00.000000Z','A2',1.2)," + // A2: 00:41 > 00:40 -- ordered within A2
                    "('2020-01-01T00:21:00.000000Z','B2',2.2)");  // B2: 00:21 > 00:20 -- ordered within B2
            execute("insert into p values " +
                    "('2020-01-01T00:40:00.000000Z','A2',1.1)," +
                    "('2020-01-01T00:20:00.000000Z','B2',2.1)," +
                    "('2020-01-01T00:41:00.000000Z','A2',1.2)," +
                    "('2020-01-01T00:21:00.000000Z','B2',2.2)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "globally out-of-order but per-cell-ordered, append-only multi-cell commit should be eligible",
                    beforeB + 1, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            // ---- Scenario (c): single-cell commit -- spec 1's own branch's scope, not counted here ----
            seedCell("A3", "2020-01-01T00:10:00.000000Z", 1.0);

            long beforeC = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            execute("insert into c values ('2020-01-01T00:20:00.000000Z','A3',1.1)");
            execute("insert into p values ('2020-01-01T00:20:00.000000Z','A3',1.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "a single-cell commit must not be counted multi-cell-eligible",
                    beforeC, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            // ---- Scenario (d): one cell out-of-order against ITS OWN committed max: all-or-nothing ----
            seedCell("A4", "2020-01-01T00:10:00.000000Z", 1.0);
            seedCell("B4", "2020-01-01T00:10:00.000000Z", 2.0);

            long beforeD = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T00:30:00.000000Z','A4',1.1)," + // A4: fine, after A4's committed max
                    "('2020-01-01T00:05:00.000000Z','B4',2.1)");  // B4: BEFORE B4's committed max (00:10)
            execute("insert into p values " +
                    "('2020-01-01T00:30:00.000000Z','A4',1.1)," +
                    "('2020-01-01T00:05:00.000000Z','B4',2.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "one cell out-of-order against its own committed max must veto the whole commit",
                    beforeD, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            // ---- Scenario (e): a brand-new, never-seeded cell in the mix ----
            seedCell("A5", "2020-01-01T00:10:00.000000Z", 1.0);

            long beforeE = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T00:30:00.000000Z','A5',1.1)," + // A5: fine, pre-existing, after its max
                    "('2020-01-01T00:15:00.000000Z','Z5',9.0)");  // Z5: brand-new cell, never committed
            execute("insert into p values " +
                    "('2020-01-01T00:30:00.000000Z','A5',1.1)," +
                    "('2020-01-01T00:15:00.000000Z','Z5',9.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "a brand-new never-committed cell in the mix must veto the whole commit",
                    beforeE, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            // ---- Scenario (g): multi-cell but spanning TWO days -- not "last partition" ----
            // Run LAST: even though ruled ineligible, this commit still advances the table's real
            // last day to 2020-01-02 via the unchanged full path (Task 1 never skips real work),
            // which would invalidate every "last day" assumption in the scenarios above were it not
            // last.
            seedCell("A6", "2020-01-01T00:10:00.000000Z", 1.0);
            seedCell("B6", "2020-01-01T00:10:00.000000Z", 2.0);

            long beforeG = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T23:00:00.000000Z','A6',1.1)," +
                    "('2020-01-02T00:30:00.000000Z','B6',2.1)");
            execute("insert into p values " +
                    "('2020-01-01T23:00:00.000000Z','A6',1.1)," +
                    "('2020-01-02T00:30:00.000000Z','B6',2.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "a commit spanning two days must not be multi-cell-eligible",
                    beforeG, TableWriter.getCompositeMultiCellFastAppendEligibleCount());

            // Behavior unchanged throughout (this task only counts, never skips real work): composite
            // results still exactly match the plain twin.
            engine.releaseInactive();
            assertWalTableNotSuspended("p");
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select exch, count() from p group by exch order by exch",
                    "select exch, count() from c group by exch order by exch"
            );
        });
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    /**
     * Inserts one row for a brand-new {@code exch} cell into both {@code c} and its plain twin {@code
     * p}, then drains the WAL so the cell pre-exists (non-empty, real {@code _txn} entry) and its max
     * timestamp is warmed into the writer-instance caches ({@link
     * TableWriter#isCompositeSingleCellFastAppendPossible} and {@link
     * TableWriter#isCompositeMultiCellFastAppendPossible} both fold in every single-cell-shaped
     * commit they see, regardless of that commit's own eligibility -- see their own docs) before any
     * later multi-cell commit into it is exercised.
     */
    private void seedCell(String exch, String ts, double px) throws SqlException {
        execute("insert into c values ('" + ts + "','" + exch + "'," + px + ")");
        execute("insert into p values ('" + ts + "','" + exch + "'," + px + ")");
        drainWalQueue();
        assertWalTableNotSuspended("c");
    }
}
