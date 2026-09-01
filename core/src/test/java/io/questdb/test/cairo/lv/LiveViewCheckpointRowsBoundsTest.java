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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointOutputKeyDomain;
import io.questdb.cairo.lv.LiveViewCheckpointRowsBounds;
import io.questdb.cairo.lv.LiveViewCheckpointRowsBounds.ScanBudgetStatus;
import io.questdb.cairo.lv.LiveViewCheckpointRowsPlan;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
 * The budget cases assert the other half of that: what a discovery reports when the search
 * turns out to cost more than a repair turn may spend. Each of them pins which bound
 * survives the stop, which falls back, and that the fallback is the value an unlocalized
 * repair would have used anyway - a budget may cost localization, never correctness.
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
    public void testBackwardWalkBudgetKeepsTheHighBoundAndDropsTheFloor() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // A budget that covers the forward pass and runs out inside the walk. The two
            // bounds are independent searches, so the one already proven survives: H is
            // what the forward pass found, and only the floor falls back.
            //
            // S is a safe floor however the walk ended - warming up from it feeds rows
            // that leave a bounded ROWS frame again before R - so the budget costs this
            // repair its localization below, not its correctness. Only the status
            // separates this ending from a walk that reached S because the history did.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_ROWS, 12);
            final Bounds bounds = discover(partitionedView(3), groupTs(20), groupTs(20), groupTs(20));
            Assert.assertEquals(HighBoundTag.FINITE, bounds.highBoundTag);
            Assert.assertEquals(groupTs(24), bounds.highTsExclusive);
            Assert.assertEquals(ScanBudgetStatus.ROWS_EXCEEDED, bounds.scanBudgetStatus);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.dependencyLowTs);
            // Nine rows forward, then four of the six the walk needed.
            Assert.assertEquals(4, bounds.backwardScanRows);
            Assert.assertEquals(13, bounds.scanRows);
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
    public void testExpressionKeyWalksToTheSameFloorTheColumnKeySeeks() throws Exception {
        assertMemoryLeak(() -> {
            // A key the sink cannot read off a page-frame record is projected through the
            // plan's own compiled function instead. `upper(sym)` partitions the fixture
            // exactly as `sym` does, so both views must answer the same three results -
            // what separates them is the descent, and only that.
            //
            // The base is the indexed one, so the column-keyed view seeks; the expression
            // key names no column to seek through and takes the walk over the same 17 rows
            // a composite or unindexed key costs. That is the whole price of the shape.
            //
            // The walk also proves the projector is rebound per cursor: the forward pass
            // and the descent are two cursors, and a key function reading the second one's
            // rows through the first one's symbol tables would resolve 'b' as something
            // else and never satisfy the key the floor belongs to.
            createSparseBase("indexed_base", true);
            try (View byColumn = view(sparseView("indexed_base", 3));
                 View byExpression = view(sparseExpressionView("indexed_base", 3));
                 LiveViewCheckpointRowsBounds bounds = new LiveViewCheckpointRowsBounds(configuration)) {
                final Bounds sought = byColumn.discover(bounds, groupTs(20), groupTs(20), groupTs(20));
                final Bounds walked = byExpression.discover(bounds, groupTs(20), groupTs(20), groupTs(20));

                Assert.assertEquals(HighBoundTag.FINITE, walked.highBoundTag);
                Assert.assertEquals(sought.highTsExclusive, walked.highTsExclusive);
                Assert.assertEquals(sought.affectedKeyCount, walked.affectedKeyCount);
                Assert.assertEquals(sought.outputKeyCount, walked.outputKeyCount);
                Assert.assertEquals(sought.forwardScanRows, walked.forwardScanRows);
                Assert.assertEquals(sought.dependencyLowTs, walked.dependencyLowTs);

                Assert.assertEquals(2, sought.indexedKeyLookups);
                Assert.assertEquals(6, sought.backwardScanRows);
                Assert.assertEquals(0, walked.indexedKeyLookups);
                Assert.assertEquals(17, walked.backwardScanRows);
            }
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
    public void testForwardScanBudgetLeavesNoBoundAndNoKeyDomain() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // A budget that runs out inside the forward pass. What it leaves behind is the
            // key set of a fragment of the replacement interval, so neither bound may be
            // read off it: Q would under-state which keys the replacement re-emits, and a
            // dependency floor discovered for an under-stated Q would under-read the
            // warm-up. Both bounds collapse to what an unlocalized repair uses, and the
            // walk below the floor is never opened.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_ROWS, 4);
            final Bounds bounds = discover(partitionedView(3), groupTs(20), groupTs(20), groupTs(20));
            Assert.assertEquals(ScanBudgetStatus.ROWS_EXCEEDED, bounds.scanBudgetStatus);
            Assert.assertEquals(HighBoundTag.EOF, bounds.highBoundTag);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.highTsExclusive);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.dependencyLowTs);
            // Groups 20 and 21 in full, and the row of group 22 that crossed the budget.
            Assert.assertEquals(4, bounds.forwardScanRows);
            Assert.assertEquals(5, bounds.scanRows);
            Assert.assertEquals(0, bounds.backwardScanRows);
        });
    }

    @Test
    public void testIndexedKeySeekFindsTheSameFloorReadingFewerRows() throws Exception {
        assertMemoryLeak(() -> {
            // Key 'a' has a row in every group; key 'b' only every fifth. The unrestricted
            // walk has to pull every 'a' row to count the three 'b' rows it is waiting for,
            // so its cost follows how sparsely the neediest key is spread rather than how
            // wide the frame is. The seek reads three rows of each key and nothing else.
            //
            // Both must land on the same floor, and it is 'b' that sets it: three 'a' rows
            // reach group 17, three 'b' rows reach group 5, and the warm-up has to satisfy
            // both.
            createSparseBase("plain_base", false);
            createSparseBase("indexed_base", true);
            final Bounds walked = discover(sparseView("plain_base", 3), groupTs(20), groupTs(20), groupTs(20));
            final Bounds sought = discover(sparseView("indexed_base", 3), groupTs(20), groupTs(20), groupTs(20));

            Assert.assertEquals(HighBoundTag.FINITE, walked.highBoundTag);
            Assert.assertEquals(groupTs(36), walked.highTsExclusive);
            Assert.assertEquals(2, walked.affectedKeyCount);
            Assert.assertEquals(2, walked.outputKeyCount);
            Assert.assertEquals(20, walked.forwardScanRows);
            Assert.assertEquals(groupTs(5), walked.dependencyLowTs);

            Assert.assertEquals(walked.highBoundTag, sought.highBoundTag);
            Assert.assertEquals(walked.highTsExclusive, sought.highTsExclusive);
            Assert.assertEquals(walked.affectedKeyCount, sought.affectedKeyCount);
            Assert.assertEquals(walked.outputKeyCount, sought.outputKeyCount);
            Assert.assertEquals(walked.forwardScanRows, sought.forwardScanRows);
            Assert.assertEquals(walked.dependencyLowTs, sought.dependencyLowTs);

            // The forward halves are identical; only the descent differs.
            Assert.assertEquals(0, walked.indexedKeyLookups);
            Assert.assertEquals(17, walked.backwardScanRows);
            Assert.assertEquals(2, sought.indexedKeyLookups);
            Assert.assertEquals(6, sought.backwardScanRows);
        });
    }

    @Test
    public void testIndexedKeySeekStopsAtTheFirstKeyWithoutHistory() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedIndexedBase();
            execute("INSERT INTO base (ts, sym, x) VALUES ('" + secondLiteral(21 * GROUP_SECONDS) + "', 'c', 7)");
            drainWalQueue();
            // The key-with-no-history case the unrestricted walk pays 38 rows for. The seek
            // takes three rows of 'a' and three of 'b', finds 'c' short of even one, and
            // stops there: S is already the lowest floor there is, so no key after 'c'
            // could change the answer and none is sought.
            final Bounds bounds = discover(partitionedView(3), groupTs(20), groupTs(20), groupTs(20));
            Assert.assertEquals(HighBoundTag.FINITE, bounds.highBoundTag);
            Assert.assertEquals(groupTs(24), bounds.highTsExclusive);
            Assert.assertEquals(3, bounds.outputKeyCount);
            Assert.assertEquals(9, bounds.forwardScanRows);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.dependencyLowTs);
            Assert.assertEquals(3, bounds.indexedKeyLookups);
            Assert.assertEquals(6, bounds.backwardScanRows);
        });
    }

    @Test
    public void testIndexedSeekBudgetPublishesNoPartialFloor() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedIndexedBase();
            // The seek answers one key at a time and takes the deepest answer, so a budget
            // that stops it mid-domain leaves a minimum over the keys it did reach - which
            // is not a floor: an unsought key may need more history than every key already
            // answered, and starting the warm-up above that key's Nmax-th predecessor
            // would under-feed it. The floor therefore falls back to S even though key 'a'
            // had already produced one.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_ROWS, 13);
            final Bounds bounds = discover(partitionedView(3), groupTs(20), groupTs(20), groupTs(20));
            Assert.assertEquals(HighBoundTag.FINITE, bounds.highBoundTag);
            Assert.assertEquals(groupTs(24), bounds.highTsExclusive);
            Assert.assertEquals(ScanBudgetStatus.ROWS_EXCEEDED, bounds.scanBudgetStatus);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.dependencyLowTs);
            // Nine rows forward, then all three of key 'a' and two of key 'b'.
            Assert.assertEquals(2, bounds.indexedKeyLookups);
            Assert.assertEquals(5, bounds.backwardScanRows);
            Assert.assertEquals(14, bounds.scanRows);
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
    public void testOutputKeyDomainLeavesInTheCheckpointKeyEncoding() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // Q leaves the discovery twice: counted, for the bounds, and keyed, for a
            // repair that goes on to re-version logical boundaries. The second form has
            // to encode the way a checkpoint partition map keys an entry, because that
            // is what it is compared against at publication time - and for a SYMBOL
            // partition column the two disagree unless the collection asks for the
            // resolved string. The scans themselves keep the reader's table-local
            // integer, which is why the plan carries two projectors rather than one.
            try (View view = view(partitionedView(3));
                 LiveViewCheckpointRowsBounds bounds = new LiveViewCheckpointRowsBounds(configuration)) {
                final Bounds counted = view.discover(bounds, groupTs(20), groupTs(20), groupTs(20));
                Assert.assertTrue(bounds.isOutputKeyDomainComplete());
                Assert.assertEquals(2, counted.outputKeyCount);

                final LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain();
                bounds.collectOutputKeys(domain);
                Assert.assertEquals(counted.outputKeyCount, domain.size());
                Assert.assertTrue("key 'a' must encode as its resolved string", domain.contains(stringKey("a")));
                Assert.assertTrue("key 'b' must encode as its resolved string", domain.contains(stringKey("b")));
                Assert.assertFalse(domain.contains(stringKey("c")));
                // The four-byte symbol id the scans key by, which is what a partition map
                // never holds for a live view: it must not be what the domain carries.
                Assert.assertFalse(domain.contains(new byte[]{0, 0, 0, 0}));
                Assert.assertFalse(domain.contains(new byte[]{1, 0, 0, 0}));
            }

            // A non-SYMBOL key column encodes identically on both sides, and the plan
            // reuses one projector for both. The domain still has to come back keyed.
            try (View view = view(longKeyedView(2));
                 LiveViewCheckpointRowsBounds bounds = new LiveViewCheckpointRowsBounds(configuration)) {
                final Bounds counted = view.discover(bounds, groupTs(20), groupTs(20), groupTs(20));
                Assert.assertTrue(bounds.isOutputKeyDomainComplete());

                final LiveViewCheckpointOutputKeyDomain domain = new LiveViewCheckpointOutputKeyDomain();
                bounds.collectOutputKeys(domain);
                Assert.assertEquals(counted.outputKeyCount, domain.size());
                // The fixture writes x = group for key 'a' and x = group + 100 for 'b',
                // so group 20's own two values are the domain's lowest members.
                Assert.assertTrue(domain.contains(longKey(20)));
                Assert.assertTrue(domain.contains(longKey(120)));
                Assert.assertFalse(domain.contains(longKey(19)));
            }
        });
    }

    @Test
    public void testOutputKeyDomainRefusesTheFragmentABudgetLeaves() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // A budget that stops the forward pass leaves Q a fragment of the interval,
            // and a publication that keeps every key outside Q untouched must not be
            // handed one - it would silently keep the entries of the keys the scan never
            // reached. The refusal is the discovery's, not the caller's.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
            try (View view = view(partitionedView(3));
                 LiveViewCheckpointRowsBounds bounds = new LiveViewCheckpointRowsBounds(configuration)) {
                view.discover(bounds, groupTs(20), groupTs(20), groupTs(20));
                Assert.assertEquals(ScanBudgetStatus.KEYS_EXCEEDED, bounds.getScanBudgetStatus());
                Assert.assertFalse(bounds.isOutputKeyDomainComplete());
                try {
                    bounds.collectOutputKeys(new LiveViewCheckpointOutputKeyDomain());
                    Assert.fail("an incomplete key domain must not be readable");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "output key domain is not available");
                }
            }
        });
    }

    @Test
    public void testOutputKeyBudgetLeavesNoBound() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // The row budget bounds how long a discovery reads; the key budget bounds how
            // wide the replacement it plans would be. A domain past that width is refused
            // whole rather than truncated: a Q missing keys re-emits fewer keys than the
            // timestamp-global replacement deletes.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
            final Bounds bounds = discover(partitionedView(3), groupTs(20), groupTs(20), groupTs(20));
            Assert.assertEquals(ScanBudgetStatus.KEYS_EXCEEDED, bounds.scanBudgetStatus);
            Assert.assertEquals(HighBoundTag.EOF, bounds.highBoundTag);
            Assert.assertEquals(Numbers.LONG_NULL, bounds.dependencyLowTs);
            // Group 20's second row carries the second key, which is the one over the
            // budget - and the row has to be read for the key to be seen at all.
            Assert.assertEquals(2, bounds.outputKeyCount);
            Assert.assertEquals(2, bounds.scanRows);
            Assert.assertEquals(0, bounds.backwardScanRows);
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
    public void testScanRowsCountsEveryRowTheSearchesRead() throws Exception {
        assertMemoryLeak(() -> {
            createSteppedBase();
            // Both budgets disabled, so what the searches read is what the data asked for.
            //
            // The bound counters count qualifying rows, which is what a bound is made of,
            // and the budget cannot be spent in that currency: the filtered view here
            // discards every second row and reads exactly as many as the unfiltered one,
            // so a budget over qualifying rows would let a filter that admits nothing scan
            // the whole table for free. scanRows counts the reads instead - the discarded
            // rows, and the row the forward pass stops on to learn H.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_ROWS, 0);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 0);
            final Bounds unfiltered = discover(partitionedView(3), groupTs(20), groupTs(20), groupTs(20));
            final Bounds filtered = discover(filteredView(3), groupTs(20), groupTs(20), groupTs(20));

            Assert.assertEquals(ScanBudgetStatus.WITHIN, unfiltered.scanBudgetStatus);
            Assert.assertEquals(8, unfiltered.forwardScanRows);
            Assert.assertEquals(6, unfiltered.backwardScanRows);
            Assert.assertEquals(15, unfiltered.scanRows);

            Assert.assertEquals(ScanBudgetStatus.WITHIN, filtered.scanBudgetStatus);
            Assert.assertEquals(4, filtered.forwardScanRows);
            Assert.assertEquals(3, filtered.backwardScanRows);
            Assert.assertEquals(15, filtered.scanRows);
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
    public void testUnseekableKeyShapesFallBackToTheWalk() throws Exception {
        assertMemoryLeak(() -> {
            // The seek needs one key column with an index behind it. A composite key has
            // no single index to seek through, and an unindexed column has none at all -
            // both keep the unrestricted walk, which is why the walk cannot be deleted.
            //
            // Column `tag` mirrors `sym` row for row but carries no index, so all three
            // views describe the same key domain and must agree on both bounds. Running
            // them through one driver also proves the seek is decided per discovery: a
            // sticky key column would carry the first view's index into the next.
            createSparseBase("indexed_base", true);
            try (View seekable = view(sparseView("indexed_base", 3));
                 View composite = view(sparseCompositeView("indexed_base", 3));
                 View unindexed = view(sparseTagView("indexed_base", 3));
                 LiveViewCheckpointRowsBounds bounds = new LiveViewCheckpointRowsBounds(configuration)) {
                final Bounds sought = seekable.discover(bounds, groupTs(20), groupTs(20), groupTs(20));
                final Bounds byComposite = composite.discover(bounds, groupTs(20), groupTs(20), groupTs(20));
                final Bounds byTag = unindexed.discover(bounds, groupTs(20), groupTs(20), groupTs(20));
                final Bounds again = seekable.discover(bounds, groupTs(20), groupTs(20), groupTs(20));

                Assert.assertEquals(2, sought.indexedKeyLookups);
                Assert.assertEquals(6, sought.backwardScanRows);

                for (Bounds walked : new Bounds[]{byComposite, byTag}) {
                    Assert.assertEquals(sought.highBoundTag, walked.highBoundTag);
                    Assert.assertEquals(sought.highTsExclusive, walked.highTsExclusive);
                    Assert.assertEquals(sought.outputKeyCount, walked.outputKeyCount);
                    Assert.assertEquals(sought.dependencyLowTs, walked.dependencyLowTs);
                    Assert.assertEquals(0, walked.indexedKeyLookups);
                    Assert.assertEquals(17, walked.backwardScanRows);
                }

                Assert.assertEquals(sought.dependencyLowTs, again.dependencyLowTs);
                Assert.assertEquals(sought.indexedKeyLookups, again.indexedKeyLookups);
                Assert.assertEquals(sought.backwardScanRows, again.backwardScanRows);
            }
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

    /**
     * One row of key 'a' in every group and one of key 'b' in every fifth, plus a `tag`
     * column that mirrors `sym` without an index. The two keys have the same frame width
     * and wildly different densities, which is exactly what separates a per-key seek from
     * a walk over every key's rows.
     */
    private static void createSparseBase(String tableName, boolean indexed) throws Exception {
        execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym SYMBOL" + (indexed ? " INDEX" : "")
                + ", tag SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final StringBuilder rows = new StringBuilder();
        for (int group = 1; group <= GROUPS; group++) {
            if (group > 1) {
                rows.append(", ");
            }
            final String ts = "'" + secondLiteral(group * GROUP_SECONDS) + "'";
            rows.append("(").append(ts).append(", 'a', 'a', ").append(group).append(")");
            if (group % 5 == 0) {
                rows.append(", (").append(ts).append(", 'b', 'b', ").append(group + 100).append(")");
            }
        }
        execute("INSERT INTO " + tableName + " (ts, sym, tag, x) VALUES " + rows);
        drainWalQueue();
    }

    private static void createSteppedBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertSteppedRows();
    }

    private static void createSteppedIndexedBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL INDEX, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
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

    // One LONG partition-key column, as LiveViewSnapshotKeyCodec writes it.
    private static byte[] longKey(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
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

    private static String sparseCompositeView(String tableName, int precedingRows) {
        return "SELECT ts, sym, sum(x) OVER (PARTITION BY sym, tag ORDER BY ts " + rowsFrame(precedingRows)
                + ") AS s FROM " + tableName;
    }

    // Partitions the sparse fixture exactly as sparseView() does, through an expression the
    // key projector has to compile rather than a column it can read.
    private static String sparseExpressionView(String tableName, int precedingRows) {
        return "SELECT ts, sym, sum(x) OVER (PARTITION BY upper(sym) ORDER BY ts " + rowsFrame(precedingRows)
                + ") AS s FROM " + tableName;
    }

    /**
     * One STRING partition-key column per value, as {@code LiveViewSnapshotKeyCodec}
     * writes it: a four-byte character count then two bytes per character. A live view
     * keys a SYMBOL partition column this way rather than by the symbol id, because the
     * ids are segment-local and would collide across refresh cycles.
     */
    private static byte[] stringKey(String... values) {
        int length = 0;
        for (String value : values) {
            length += Integer.BYTES + value.length() * Character.BYTES;
        }
        final ByteBuffer key = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
        for (String value : values) {
            key.putInt(value.length());
            for (int i = 0; i < value.length(); i++) {
                key.putChar(value.charAt(i));
            }
        }
        return key.array();
    }

    private static String sparseTagView(String tableName, int precedingRows) {
        return "SELECT ts, tag, sum(x) OVER (PARTITION BY tag ORDER BY ts " + rowsFrame(precedingRows)
                + ") AS s FROM " + tableName;
    }

    private static String sparseView(String tableName, int precedingRows) {
        return "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts " + rowsFrame(precedingRows)
                + ") AS s FROM " + tableName;
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
        private final long indexedKeyLookups;
        private final long outputKeyCount;
        private final ScanBudgetStatus scanBudgetStatus;
        private final long scanRows;

        private Bounds(LiveViewCheckpointRowsBounds bounds) {
            this.affectedKeyCount = bounds.getAffectedKeyCount();
            this.backwardScanRows = bounds.getBackwardScanRows();
            this.dependencyLowTs = bounds.getDependencyLowTs();
            this.forwardScanRows = bounds.getForwardScanRows();
            this.highBoundTag = bounds.getHighBoundTag();
            this.highTsExclusive = bounds.getHighTsExclusive();
            this.indexedKeyLookups = bounds.getIndexedKeyLookups();
            this.outputKeyCount = bounds.getOutputKeyCount();
            this.scanBudgetStatus = bounds.getScanBudgetStatus();
            this.scanRows = bounds.getScanRows();
            Assert.assertEquals(scanBudgetStatus != ScanBudgetStatus.WITHIN, bounds.isScanBudgetExceeded());
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
                    null,
                    viewLowerBoundTs,
                    outputLowTs,
                    changeLowTs,
                    changeMaxTs
            );
            return new Bounds(bounds);
        }
    }
}
