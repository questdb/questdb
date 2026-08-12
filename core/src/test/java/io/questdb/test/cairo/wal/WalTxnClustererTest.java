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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.WalTxnClusterer;
import io.questdb.std.LongList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pure unit tests for the pre-split clustering decision. The fixture uses a synthetic range
 * [0, 1_000_000) binned at 1000, uniform density 1 row per timestamp unit, so a cold run of
 * B bins estimates exactly B * 1000 rows and every expectation is exact.
 */
public class WalTxnClustererTest {

    private final WalTxnClusterer clusterer = new WalTxnClusterer();

    @Test
    public void testBinWideningKeepsBinCountUnderCap() {
        // span 1e6, maxBins 100 -> binDuration widens to 1e4; two clusters 3 wide bins apart
        clusterer.clear();
        clusterer.addTxnRange(0, 9_999);
        clusterer.addTxnRange(500_000, 509_999);
        LongList cuts = clusterer.computeCuts(0, 999_999, 1000, 100, 10_000, 1_000_000, 7);
        // cold runs: interior [1,49] (490k rows), trailing [51,99] (490k rows)
        assertCuts(cuts, 10_000, 500_000, 510_000);
    }

    @Test
    public void testBudgetPrefersLargestGaps() {
        clusterer.clear();
        clusterer.addTxnRange(100_000, 199_999);
        clusterer.addTxnRange(700_000, 799_999);
        // budget 3: interior gap (500k rows, 2 cuts) wins, then trailing (200k rows, 1 cut);
        // leading (100k rows) is dropped
        LongList cuts = clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 3);
        assertCuts(cuts, 200_000, 700_000, 800_000);
    }

    @Test
    public void testColdEndsAndInteriorGap() {
        clusterer.clear();
        clusterer.addTxnRange(100_000, 199_999);
        clusterer.addTxnRange(700_000, 799_999);
        // leading [0,99] -> cut at hot edge; interior [200,699] -> two cuts; trailing [800,999] -> one
        LongList cuts = clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 7);
        assertCuts(cuts, 100_000, 200_000, 700_000, 800_000);
    }

    @Test
    public void testDegenerateInputs() {
        // no ranges
        clusterer.clear();
        Assert.assertEquals(0, clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 7).size());
        // t1 == t0
        clusterer.clear();
        clusterer.addTxnRange(0, 0);
        Assert.assertEquals(0, clusterer.computeCuts(0, 0, 1000, 1000, 10_000, 1_000_000, 7).size());
        // zero budget
        clusterer.clear();
        clusterer.addTxnRange(500_000, 500_999);
        Assert.assertEquals(0, clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 0).size());
        // empty partition
        clusterer.clear();
        clusterer.addTxnRange(500_000, 500_999);
        Assert.assertEquals(0, clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 0, 7).size());
    }

    @Test
    public void testFullCoverageIsSingleMerge() {
        clusterer.clear();
        clusterer.addTxnRange(0, 999_999);
        LongList cuts = clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 7);
        Assert.assertEquals(0, cuts.size());
    }

    @Test
    public void testNoCutFallsInsideAnIncomingRange() {
        // A cut is a piece boundary. One falling inside an incoming transaction's range would split
        // that cluster across two pieces, and each piece would then merge its half - the cost the
        // pre-split exists to avoid. The caller relies on this to route a whole cluster into the piece
        // the cut created for it, so the property is pinned here rather than left to the cold-run walk.
        clusterer.clear();
        clusterer.addTxnRange(100_000, 199_999);
        // deliberately off the bin grid on both edges
        clusterer.addTxnRange(450_555, 460_111);
        clusterer.addTxnRange(700_000, 799_999);
        LongList cuts = clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 7);
        Assert.assertTrue("no cuts computed, the fixture proves nothing", cuts.size() > 0);
        assertNoCutInside(cuts, 100_000, 199_999);
        assertNoCutInside(cuts, 450_555, 460_111);
        assertNoCutInside(cuts, 700_000, 799_999);
    }

    @Test
    public void testOverlappingRangesCoalesce() {
        // overlapping and touching ranges make one hot stride; only the qualifying ends cut
        clusterer.clear();
        clusterer.addTxnRange(300_000, 500_000);
        clusterer.addTxnRange(400_000, 600_000);
        clusterer.addTxnRange(600_000, 649_999);
        LongList cuts = clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 7);
        assertCuts(cuts, 300_000, 650_000);
    }

    @Test
    public void testScratchStateIsReusable() {
        // first computation with clusters, then a fresh single-merge computation must not
        // inherit any state from the previous run
        clusterer.clear();
        clusterer.addTxnRange(100_000, 199_999);
        clusterer.addTxnRange(700_000, 799_999);
        Assert.assertEquals(4, clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 7).size());
        clusterer.clear();
        clusterer.addTxnRange(0, 999_999);
        Assert.assertEquals(0, clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 7).size());
    }

    @Test
    public void testSingleTxnRangeCutsBothEdges() {
        // The clusterer takes timestamp ranges, never a transaction count: ONE range isolates its
        // hot stride exactly as two do. This is the unit-level ground for dropping the pre-split's
        // blockTxnCount < 2 gate, so a single-transaction apply clusters too.
        clusterer.clear();
        clusterer.addTxnRange(500_000, 509_999);
        LongList cuts = clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 7);
        // leading cold run [0,499] -> one cut at its hot edge; trailing [510,999] -> one cut at its
        assertCuts(cuts, 500_000, 510_000);
    }

    @Test
    public void testSmallGapFoldsIntoMerge() {
        clusterer.clear();
        clusterer.addTxnRange(0, 199_999);
        clusterer.addTxnRange(205_000, 999_999);
        // interior gap [200,204] estimates 5000 rows < G=10000 -> folded, single merge
        LongList cuts = clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 1_000_000, 7);
        Assert.assertEquals(0, cuts.size());
    }

    @Test
    public void testSubGapGateIsRowBased() {
        // same 5-bin interior gap qualifies when the density is 10x (10 rows per unit)
        clusterer.clear();
        clusterer.addTxnRange(0, 199_999);
        clusterer.addTxnRange(205_000, 999_999);
        LongList cuts = clusterer.computeCuts(0, 999_999, 1000, 1000, 10_000, 10_000_000, 7);
        assertCuts(cuts, 200_000, 205_000);
    }

    private static void assertCuts(LongList cuts, long... expected) {
        final StringBuilder actual = new StringBuilder();
        for (int i = 0; i < cuts.size(); i++) {
            actual.append(cuts.getQuick(i)).append(',');
        }
        final StringBuilder want = new StringBuilder();
        for (long e : expected) {
            want.append(e).append(',');
        }
        Assert.assertEquals(want.toString(), actual.toString());
    }

    private static void assertNoCutInside(LongList cuts, long lo, long hi) {
        for (int i = 0, n = cuts.size(); i < n; i++) {
            final long cut = cuts.getQuick(i);
            // A cut at X puts rows < X left of it and rows >= X right of it, so a cut AT lo is the
            // range's own left edge and harmless. Anything above lo and at or below hi splits it.
            Assert.assertFalse(
                    "cut " + cut + " splits the incoming range [" + lo + ", " + hi + ']',
                    cut > lo && cut <= hi
            );
        }
    }
}
