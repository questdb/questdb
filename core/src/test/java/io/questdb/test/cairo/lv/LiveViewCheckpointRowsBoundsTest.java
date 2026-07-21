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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointRowsBounds;
import io.questdb.cairo.lv.LiveViewCheckpointRowsPlan;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The {@code H -> Q -> L} discovery a bounded ROWS live view plans its localized
 * out-of-order repair against.
 * <p>
 * A RANGE view derives both bounds by arithmetic; a ROWS view has to find them in the
 * data, because {@code Nmax} counts rows of one partition key and says nothing about
 * how much time those rows span. These tests pin the three results of that search -
 * the convergence boundary {@code H}, the output key domain {@code Q}, and the
 * dependency floor {@code L} - and, just as importantly, the rows the search read to
 * produce them. A bound whose discovery costs the view's whole history buys nothing,
 * so every case asserts the scan counters next to the bounds.
 * <p>
 * The main fixture is one row per key every 10 seconds over 40 groups, which makes
 * both bounds countable by hand: with {@code Nmax = 3} a change confined to one group
 * converges three groups above it and depends on three groups below it, out of the 80
 * rows the table holds.
 */
public class LiveViewCheckpointRowsBoundsTest extends AbstractCairoTest {
    private static final int GROUPS = 40;
    // Seconds between adjacent timestamp groups of the main fixture.
    private static final int GROUP_SECONDS = 10;

    @Test
    public void testAffectedKeyShortOfFollowingRowsPinsEofButStillBoundsBelow() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // The change tops out two groups below the last one, so neither key can
            // collect the three following rows its frame needs. That is the case where
            // the change does reach the frame the runtime holds: H stays at end-of-frame
            // and the caller must promote its replay state rather than restore what it
            // found.
            //
            // Q is complete all the same - the scan ran to the end of the table - so the
            // dependency floor is still discovered and the repair is bounded below even
            // while it is unbounded above.
            final Bounds bounds = discover(
                    partitionedView(3),
                    groupTs(GROUPS - 2),
                    groupTs(GROUPS - 2),
                    groupTs(GROUPS - 2)
            );
            Assert.assertEquals(HighBoundTag.EOF, bounds.highBoundTag);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.highTsExclusive);
            Assert.assertEquals(2, bounds.affectedKeyCount);
            Assert.assertEquals(2, bounds.outputKeyCount);
            // Groups 38, 39 and 40 forward - the whole tail, which is all there is.
            Assert.assertEquals(6, bounds.forwardScanRows);
            Assert.assertEquals(groupTs(GROUPS - 5), bounds.dependencyLowTs);
            Assert.assertEquals(6, bounds.backwardScanRows);
        });
    }

    @Test
    public void testBoundsAreRederivedAcrossReuse() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // One instance serves every repair of every view a worker owns, so a stale
            // key map or a stale counter would quietly widen or narrow the next bound.
            // Drive a SYMBOL-keyed discovery, then a LONG-keyed one whose key layout the
            // first map cannot hold, then the first again - and require its result back
            // exactly.
            try (View symbolKeyed = view(partitionedView(3));
                 View longKeyed = view(longKeyedView(2));
                 LiveViewCheckpointRowsBounds bounds = new LiveViewCheckpointRowsBounds(configuration)) {
                final Bounds first = symbolKeyed.discover(bounds, groupTs(20), groupTs(20), groupTs(20));
                final Bounds middle = longKeyed.discover(bounds, groupTs(30), groupTs(30), groupTs(30));
                final Bounds again = symbolKeyed.discover(bounds, groupTs(20), groupTs(20), groupTs(20));

                Assert.assertEquals(HighBoundTag.FINITE, first.highBoundTag);
                Assert.assertEquals(groupTs(24), first.highTsExclusive);
                Assert.assertEquals(groupTs(17), first.dependencyLowTs);
                Assert.assertEquals(2, first.outputKeyCount);

                // Every x is distinct, so no key ever collects a following row and none
                // has a predecessor below the floor either: the second discovery is the
                // both-bounds-refused shape, and its 22 output keys and 58 rows walked
                // below the floor are what the first result must not inherit.
                Assert.assertEquals(2, middle.affectedKeyCount);
                Assert.assertEquals(22, middle.outputKeyCount);
                Assert.assertEquals(HighBoundTag.EOF, middle.highBoundTag);
                Assert.assertEquals(Numbers.LONG_NULL, middle.dependencyLowTs);
                Assert.assertEquals(58, middle.backwardScanRows);

                Assert.assertEquals(first.highBoundTag, again.highBoundTag);
                Assert.assertEquals(first.highTsExclusive, again.highTsExclusive);
                Assert.assertEquals(first.dependencyLowTs, again.dependencyLowTs);
                Assert.assertEquals(first.affectedKeyCount, again.affectedKeyCount);
                Assert.assertEquals(first.outputKeyCount, again.outputKeyCount);
                Assert.assertEquals(first.forwardScanRows, again.forwardScanRows);
                Assert.assertEquals(first.backwardScanRows, again.backwardScanRows);
            }
        });
    }

    @Test
    public void testChangeInvisibleToTheViewLeavesNoBound() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // A row the view's WHERE discards: it is a real change to the base, but the
            // view emits nothing for it, so this snapshot cannot show which keys it
            // touched. Neither bound follows, and the partial key set the forward scan
            // had collected must not be mistaken for Q - a dependency floor for an
            // unfinished key domain would under-read the warm-up.
            execute("INSERT INTO base (ts, sym, x) VALUES ('" + secondLiteral(205) + "', 'b', 999)");
            drainWalQueue();
            final Bounds bounds = discover(filteredView(3), secondTs(205), secondTs(205), secondTs(205));
            Assert.assertEquals(HighBoundTag.EOF, bounds.highBoundTag);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.dependencyLowTs);
            Assert.assertEquals(0, bounds.forwardScanRows);
            Assert.assertEquals(0, bounds.backwardScanRows);
        });
    }

    @Test
    public void testCompleteTimestampTieIsAdmittedAtBothBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Two rows of key 'a' and one of key 'b' share every timestamp, except group
            // 4 which holds key 'a' alone. The change is confined to group 4, so only
            // key 'a' is affected - and with Nmax = 2 it collects both following rows
            // inside group 5, before that group's key 'b' row.
            //
            // Both bounds are timestamps, not row positions, so the rest of the tie
            // comes with them: forward, key 'b' at group 5 still joins Q; backward, the
            // floor lands on the group that satisfied the last key and admits the rows
            // of that group the walk had not reached.
            final StringBuilder rows = new StringBuilder();
            for (int group = 1; group <= 8; group++) {
                if (group > 1) {
                    rows.append(", ");
                }
                final String ts = "'" + secondLiteral(group * GROUP_SECONDS) + "'";
                rows.append("(").append(ts).append(", 'a', 1), ")
                        .append("(").append(ts).append(", 'a', 2)");
                if (group != 4) {
                    rows.append(", (").append(ts).append(", 'b', 3)");
                }
            }
            execute("INSERT INTO base (ts, sym, x) VALUES " + rows);
            drainWalQueue();

            final Bounds bounds = discover(partitionedView(2), groupTs(4), groupTs(4), groupTs(4));
            Assert.assertEquals(HighBoundTag.FINITE, bounds.highBoundTag);
            Assert.assertEquals(groupTs(6), bounds.highTsExclusive);
            Assert.assertEquals(1, bounds.affectedKeyCount);
            // Key 'b' entered Q from the tail of the tie the bound admitted.
            Assert.assertEquals(2, bounds.outputKeyCount);
            // Group 4's two rows plus all three of group 5.
            Assert.assertEquals(5, bounds.forwardScanRows);
            // Walking down, key 'a' is satisfied inside group 3 and key 'b' by the first
            // row of group 2 - which is where the floor lands, tie and all.
            Assert.assertEquals(groupTs(2), bounds.dependencyLowTs);
            Assert.assertEquals(4, bounds.backwardScanRows);
        });
    }

    @Test
    public void testEmptyReplacementIntervalNeedsNoWarmUp() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // The floor sits above every row the view holds, so the replacement re-emits
            // nothing and no key needs its state carried up to it. The dependency floor
            // collapses onto the output floor and neither scan reads a row.
            final Bounds bounds = discover(
                    partitionedView(3),
                    groupTs(GROUPS) + 1,
                    groupTs(GROUPS) + 1,
                    groupTs(GROUPS) + 1
            );
            Assert.assertEquals(HighBoundTag.EOF, bounds.highBoundTag);
            Assert.assertEquals(0, bounds.outputKeyCount);
            Assert.assertEquals(groupTs(GROUPS) + 1, bounds.dependencyLowTs);
            Assert.assertEquals(0, bounds.forwardScanRows);
            Assert.assertEquals(0, bounds.backwardScanRows);
        });
    }

    @Test
    public void testFilterCountsOnlyQualifyingRows() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // The view's WHERE is the predicate the replay applies, so a predecessor is
            // a row the view would have emitted rather than any base row. Key 'b' is
            // filtered away entirely, so both bounds land exactly where the two-key run
            // puts them while reading half the rows.
            final Bounds bounds = discover(filteredView(3), groupTs(20), groupTs(20), groupTs(20));
            Assert.assertEquals(HighBoundTag.FINITE, bounds.highBoundTag);
            Assert.assertEquals(groupTs(24), bounds.highTsExclusive);
            Assert.assertEquals(1, bounds.affectedKeyCount);
            Assert.assertEquals(1, bounds.outputKeyCount);
            Assert.assertEquals(4, bounds.forwardScanRows);
            Assert.assertEquals(groupTs(17), bounds.dependencyLowTs);
            Assert.assertEquals(3, bounds.backwardScanRows);
        });
    }

    @Test
    public void testFloorAtTheViewBoundaryReadsNothingBelowIt() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // The output floor already sits at START FROM, so there is no history the
            // view holds below it and no floor above it to discover - the walk is not
            // opened at all. The bound above is unaffected: it is derived from the change
            // interval, which this says nothing about.
            try (View view = view(partitionedView(3));
                 LiveViewCheckpointRowsBounds bounds = new LiveViewCheckpointRowsBounds(configuration)) {
                final Bounds result = view.discover(bounds, groupTs(20), groupTs(20), groupTs(20), groupTs(20));
                Assert.assertEquals(HighBoundTag.FINITE, result.highBoundTag);
                Assert.assertEquals(groupTs(24), result.highTsExclusive);
                Assert.assertEquals(groupTs(20), result.dependencyLowTs);
                Assert.assertEquals(0, result.backwardScanRows);
            }
        });
    }

    @Test
    public void testKeyFirstSeenInTheReplacementIntervalFallsBackToTheViewBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            execute("INSERT INTO base (ts, sym, x) VALUES ('" + secondLiteral(21 * GROUP_SECONDS) + "', 'c', 7)");
            drainWalQueue();
            // Key 'c' joins the output domain at group 21 with no history at all. The
            // backward walk cannot prove that without reaching the view's boundary, so
            // it reads every row below the floor and the dependency floor lands at S.
            // Localization below is lost even though the bound above still holds - the
            // case an index-restricted per-key seek is what actually fixes.
            final Bounds bounds = discover(partitionedView(3), groupTs(20), groupTs(20), groupTs(20));
            Assert.assertEquals(HighBoundTag.FINITE, bounds.highBoundTag);
            Assert.assertEquals(groupTs(24), bounds.highTsExclusive);
            Assert.assertEquals(2, bounds.affectedKeyCount);
            Assert.assertEquals(3, bounds.outputKeyCount);
            Assert.assertEquals(9, bounds.forwardScanRows);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.dependencyLowTs);
            // Groups 1 through 19, two rows each.
            Assert.assertEquals(38, bounds.backwardScanRows);
        });
    }

    @Test
    public void testPartitionedChangeConvergesAboveAndDependsBelow() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // The canonical case. A change confined to group 20 reaches the three
            // following rows of each key - groups 21, 22 and 23 - and no further, so H
            // is the first distinct timestamp above group 23. Below the floor each key
            // needs three predecessors, which groups 19, 18 and 17 supply.
            //
            // The counters are the point: 8 rows read forward and 6 backward out of the
            // 80 the table holds, and neither number moves as the history grows.
            final Bounds bounds = discover(partitionedView(3), groupTs(20), groupTs(20), groupTs(20));
            Assert.assertEquals(HighBoundTag.FINITE, bounds.highBoundTag);
            Assert.assertEquals(groupTs(24), bounds.highTsExclusive);
            Assert.assertEquals(2, bounds.affectedKeyCount);
            Assert.assertEquals(2, bounds.outputKeyCount);
            Assert.assertEquals(8, bounds.forwardScanRows);
            Assert.assertEquals(groupTs(17), bounds.dependencyLowTs);
            Assert.assertEquals(6, bounds.backwardScanRows);
        });
    }

    @Test
    public void testUnboundedChangeCeilingLeavesNoBound() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // A structural or non-DATA entry in the incorporated range denies the change
            // ceiling. Convergence is measured from that ceiling, so without it there is
            // nothing to measure and the discovery reads no row at all rather than
            // guessing one.
            final Bounds bounds = discover(partitionedView(3), groupTs(20), groupTs(20), Numbers.LONG_NULL);
            Assert.assertEquals(HighBoundTag.EOF, bounds.highBoundTag);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.dependencyLowTs);
            Assert.assertEquals(0, bounds.forwardScanRows);
            Assert.assertEquals(0, bounds.backwardScanRows);
        });
    }

    @Test
    public void testViewBoundaryClampsTheDependencyFloor() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // START FROM sits between the floor an unclamped search would find (group
            // 17) and the output floor itself, so the walk stops at the boundary: the
            // view holds no row below it, and the two groups above it are all the
            // warm-up there is. The scan stops there too rather than reading history the
            // view never emitted.
            try (View view = view(partitionedView(3));
                 LiveViewCheckpointRowsBounds bounds = new LiveViewCheckpointRowsBounds(configuration)) {
                final Bounds result = view.discover(bounds, groupTs(18), groupTs(20), groupTs(20), groupTs(20));
                Assert.assertEquals(HighBoundTag.FINITE, result.highBoundTag);
                Assert.assertEquals(groupTs(24), result.highTsExclusive);
                Assert.assertEquals(groupTs(18), result.dependencyLowTs);
                Assert.assertEquals(4, result.backwardScanRows);
            }
        });
    }

    private static void createSteppedBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertSteppedRows();
    }

    private static String filteredView(int precedingRows) {
        return "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " + rowsFrame(precedingRows)
                + ") AS s FROM base WHERE x < 100";
    }

    // The microsecond timestamp of the given 1-based group of the main fixture.
    private static long groupTs(int group) {
        return secondTs(group * GROUP_SECONDS);
    }

    private static void insertSteppedRows() throws Exception {
        final StringBuilder rows = new StringBuilder();
        for (int group = 1; group <= GROUPS; group++) {
            if (group > 1) {
                rows.append(", ");
            }
            final String ts = "'" + secondLiteral(group * GROUP_SECONDS) + "'";
            rows.append("(").append(ts).append(", 'a', ").append(group).append("), ")
                    .append("(").append(ts).append(", 'b', ").append(group + 100).append(")");
        }
        execute("INSERT INTO base (ts, sym, x) VALUES " + rows);
        drainWalQueue();
    }

    private static String longKeyedView(int precedingRows) {
        return "SELECT ts, sym, sum(x) OVER (PARTITION BY x ORDER BY ts " + rowsFrame(precedingRows)
                + ") AS s FROM base";
    }

    private static String partitionedView(int precedingRows) {
        return "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " + rowsFrame(precedingRows)
                + ") AS s FROM base";
    }

    private static String rowsFrame(int precedingRows) {
        return "ROWS BETWEEN " + precedingRows + " PRECEDING AND CURRENT ROW";
    }

    // A 2026-11-01 microsecond timestamp at the given second-of-day. Every fixture row
    // shares one calendar day, so the base's DAY partitioning never splits a group.
    private static String secondLiteral(int secondOfDay) {
        return String.format("2026-11-01T00:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    private static long secondTs(int secondOfDay) {
        return MicrosTimestampDriver.floor(secondLiteral(secondOfDay));
    }

    private Bounds discover(String viewSql, long outputLowTs, long changeLowTs, long changeMaxTs) throws Exception {
        try (View view = view(viewSql);
             LiveViewCheckpointRowsBounds bounds = new LiveViewCheckpointRowsBounds(configuration)) {
            return view.discover(bounds, outputLowTs, changeLowTs, changeMaxTs);
        }
    }

    private View view(String viewSql) throws Exception {
        sqlExecutionContext.setLiveViewCompile(true);
        try {
            final SqlCompiler compiler = engine.getSqlCompiler();
            try {
                return new View(compiler, select(compiler, viewSql, sqlExecutionContext));
            } catch (Throwable th) {
                compiler.close();
                throw th;
            }
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    // One discovery's results, copied out so a reused driver cannot rewrite an
    // assertion's inputs from under it.
    private static class Bounds {
        private final long affectedKeyCount;
        private final long backwardScanRows;
        private final long dependencyLowTs;
        private final long forwardScanRows;
        private final HighBoundTag highBoundTag;
        private final long highTsExclusive;
        private final long outputKeyCount;

        private Bounds(LiveViewCheckpointRowsBounds bounds) {
            this.affectedKeyCount = bounds.getAffectedKeyCount();
            this.backwardScanRows = bounds.getBackwardScanRows();
            this.dependencyLowTs = bounds.getDependencyLowTs();
            this.forwardScanRows = bounds.getForwardScanRows();
            this.highBoundTag = bounds.getHighBoundTag();
            this.highTsExclusive = bounds.getHighTsExclusive();
            this.outputKeyCount = bounds.getOutputKeyCount();
        }
    }

    // The compiled live-view SELECT taken apart the way a repair takes it apart: the
    // ROWS dependency plan, the base page-frame factory, and the view's own filter.
    private static class View implements AutoCloseable {
        private final SqlCompiler compiler;
        private final RecordCursorFactory factory;
        private final Function filter;
        private final PageFrameRecordCursorFactory pageFrameFactory;
        private final LiveViewCheckpointRowsPlan plan;

        private View(SqlCompiler compiler, RecordCursorFactory factory) {
            this.compiler = compiler;
            this.factory = factory;
            RecordCursorFactory root = factory;
            while (root instanceof QueryProgress) {
                root = root.getBaseFactory();
            }
            Assert.assertTrue(root.getClass().getName(), root instanceof WindowRecordCursorFactory);
            final WindowRecordCursorFactory windowFactory = (WindowRecordCursorFactory) root;
            this.plan = windowFactory.getCheckpointRowsPlan();
            Assert.assertNotNull("the view must carry a finite ROWS plan", plan);
            final RecordCursorFactory filterFactory = windowFactory.getBaseFactory();
            this.filter = filterFactory.getFilter();
            this.pageFrameFactory = (PageFrameRecordCursorFactory)
                    (filter != null ? filterFactory.getBaseFactory() : filterFactory);
        }

        @Override
        public void close() {
            factory.close();
            compiler.close();
        }

        private Bounds discover(
                LiveViewCheckpointRowsBounds bounds,
                long outputLowTs,
                long changeLowTs,
                long changeMaxTs
        ) throws Exception {
            return discover(bounds, Numbers.LONG_NULL, outputLowTs, changeLowTs, changeMaxTs);
        }

        private Bounds discover(
                LiveViewCheckpointRowsBounds bounds,
                long viewLowerBoundTs,
                long outputLowTs,
                long changeLowTs,
                long changeMaxTs
        ) throws Exception {
            bounds.discover(
                    plan,
                    pageFrameFactory,
                    sqlExecutionContext,
                    filter,
                    viewLowerBoundTs,
                    outputLowTs,
                    changeLowTs,
                    changeMaxTs
            );
            return new Bounds(bounds);
        }
    }
}
