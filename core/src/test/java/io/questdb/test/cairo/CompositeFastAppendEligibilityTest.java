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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Composite single-cell fast-append eligibility (composite-partitioning fast-append spec 1, Task 1
 * -- detection only; see {@code docs/superpowers/specs/2026-07-21-composite-single-cell-fast-append
 * -design.md} and {@code .superpowers/sdd/task-1-brief.md}). {@link
 * TableWriter#isCompositeSingleCellFastAppendPossible} and the {@code
 * cairo.wal.composite.fastappend.enabled} flag are wired into the WAL-commit path as a COUNTER
 * ONLY: an eligible commit still takes the existing full O3 composite path, unchanged. A later
 * task makes an eligible commit actually fast-append.
 * <p>
 * These tests prove:
 * <ul>
 *     <li>the predicate fires exactly on single-cell, ordered, append-only commits -- never on a
 *     multi-cell commit, never on an out-of-order/O3-into-cell commit -- via {@link
 *     TableWriter#getCompositeFastAppendEligibleCount()}, a static, JVM-wide counter (it has to be:
 *     the writer that actually processes a WAL commit is internal to {@code drainWalQueue()} and is
 *     released afterwards, so a plain per-instance field would not be observable here). Being
 *     static/JVM-wide, every assertion below compares a BEFORE/AFTER delta bracketing one specific
 *     commit, so it stays correct regardless of what any other commit in this JVM has already
 *     counted.</li>
 *     <li>a table's very own FIRST-ever commit is never eligible, even when its shape (single-cell,
 *     ordered) would otherwise qualify: {@code isRoutedComposite()} (one of this task's hook
 *     conditions, gating on the {@code _cell} registry already having an entry) is false until that
 *     same first commit's own real per-cell dispatch interns the first cell -- which happens AFTER
 *     this hook runs. Safe (a conservative miss, never a false positive), but real, and it also means
 *     the per-cell max-timestamp cache is first warmed by the SECOND commit into a cell (the first
 *     one whose predicate call actually runs), not the first.</li>
 *     <li>composite query results still exactly match a plain twin throughout -- this task changes
 *     detection/counting only, never behavior (no early return is ever taken).</li>
 *     <li>with the flag off, an otherwise-eligible commit never increments the counter.</li>
 * </ul>
 */
public class CompositeFastAppendEligibilityTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testFlagOffEligibleCommitDoesNotIncrementCounter() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, "false");

            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Commit 1: routes the table (real per-cell interning happens regardless of this task's
            // flag -- the flag only gates the NEW detection hook, never the pre-existing commit
            // machinery). Not a meaningful assertion by itself.
            execute("insert into c values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T01:00:00.000000Z','A',1.1)");
            execute("insert into p values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T01:00:00.000000Z','A',1.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Commit 2: single-cell, ordered, strictly after cellA's committed max (01:00) --
            // exactly the "append into an already-populated cell" shape the ON test (below) proves
            // eligible. With the flag off, the WHOLE hook (including isRoutedComposite() and the
            // predicate call) is skipped before any of that is even evaluated -- must not increment.
            long before = TableWriter.getCompositeFastAppendEligibleCount();
            execute("insert into c values ('2020-01-01T02:00:00.000000Z','A',1.2)");
            execute("insert into p values ('2020-01-01T02:00:00.000000Z','A',1.2)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "flag off: an otherwise-eligible commit must not increment the counter",
                    before, TableWriter.getCompositeFastAppendEligibleCount());

            engine.releaseInactive();
            assertWalTableNotSuspended("p");
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
        });
    }

    @Test
    public void testSingleCellOrderedAppendEligibleMultiCellAndOutOfOrderAreNot() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Commit 1: the table's very own FIRST-ever commit (single-cell exch='A', ordered).
            // NOT eligible: isRoutedComposite() is false until THIS commit's own real per-cell
            // dispatch interns cellA, which happens after this task's hook runs -- so the hook's
            // isRoutedComposite() condition short-circuits before the predicate is ever called (and
            // the per-cell max-timestamp cache is therefore not warmed by this commit either).
            long before1 = TableWriter.getCompositeFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T01:00:00.000000Z','A',1.1)");
            execute("insert into p values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T01:00:00.000000Z','A',1.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "a table's very first commit is never eligible (isRoutedComposite is false until it runs)",
                    before1, TableWriter.getCompositeFastAppendEligibleCount());

            // Commit 2: single-cell, ordered, strictly after cellA's real committed max (01:00) --
            // now isRoutedComposite() is true (commit 1 interned cellA), so the predicate DOES run,
            // but the per-cell max-timestamp cache is still cold for cellA (commit 1 never warmed
            // it) -- a conservative miss, NOT eligible. This commit's own o3TimestampMax (02:00) DOES
            // warm the cache for cellA, regardless of its own eligibility outcome.
            long before2 = TableWriter.getCompositeFastAppendEligibleCount();
            execute("insert into c values ('2020-01-01T02:00:00.000000Z','A',1.2)");
            execute("insert into p values ('2020-01-01T02:00:00.000000Z','A',1.2)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "a cell's real committed data with a still-cold cache is a conservative miss, not eligible",
                    before2, TableWriter.getCompositeFastAppendEligibleCount());

            // Commit 3: single-cell, ordered, strictly after cellA's cached max (02:00, warmed by
            // commit 2) -- a continuing ordered append into an ALREADY-populated cell, with a warm
            // cache. Eligible.
            long before3 = TableWriter.getCompositeFastAppendEligibleCount();
            execute("insert into c values ('2020-01-01T03:00:00.000000Z','A',1.3)");
            execute("insert into p values ('2020-01-01T03:00:00.000000Z','A',1.3)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "single-cell ordered commit after the cell's cached committed max should be eligible",
                    before3 + 1, TableWriter.getCompositeFastAppendEligibleCount());

            // Commit 4: single-cell, ordered, BRAND NEW cell (exch='B') in an already-routed table --
            // trivially append-only (no committed rows to conflict with), no cache warm-up needed.
            // Eligible.
            long before4 = TableWriter.getCompositeFastAppendEligibleCount();
            execute("insert into c values ('2020-01-01T04:00:00.000000Z','B',2.0)");
            execute("insert into p values ('2020-01-01T04:00:00.000000Z','B',2.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "single-cell ordered commit into a brand-new cell should be eligible",
                    before4 + 1, TableWriter.getCompositeFastAppendEligibleCount());

            // Commit 5: single-cell (exch='A') but landing BEFORE cellA's cached committed max
            // (03:00) -- an out-of-order / O3-into-cell row, not a pure append. Must NOT be
            // eligible (the cache is still warm from commit 3, so this exercises the real
            // timestamp comparison, not a cold-cache miss).
            long before5 = TableWriter.getCompositeFastAppendEligibleCount();
            execute("insert into c values ('2020-01-01T02:45:00.000000Z','A',1.25)");
            execute("insert into p values ('2020-01-01T02:45:00.000000Z','A',1.25)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals(
                    "out-of-order single-cell commit into an already-populated cell should not be eligible",
                    before5, TableWriter.getCompositeFastAppendEligibleCount());

            // Commit 6: multi-cell (exch in ('A','B')), internally timestamp-ordered -- must NOT be
            // eligible (spec 1 is single-cell only), regardless of order/append-only.
            long before6 = TableWriter.getCompositeFastAppendEligibleCount();
            execute("insert into c values " +
                    "('2020-01-01T05:00:00.000000Z','A',1.4), ('2020-01-01T05:30:00.000000Z','B',2.1)");
            execute("insert into p values " +
                    "('2020-01-01T05:00:00.000000Z','A',1.4), ('2020-01-01T05:30:00.000000Z','B',2.1)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            Assert.assertEquals("multi-cell commit should not be eligible",
                    before6, TableWriter.getCompositeFastAppendEligibleCount());

            // Behavior unchanged throughout (this task only counts, never skips real work):
            // composite results still exactly match the plain twin.
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
}
