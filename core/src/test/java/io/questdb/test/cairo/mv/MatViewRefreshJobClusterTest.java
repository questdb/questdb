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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.std.LongList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct unit tests for the static cost-model helpers in MatViewRefreshJob:
 * {@link MatViewRefreshJob#clusterIntervals} and
 * {@link MatViewRefreshJob#capStepByNarrowestInterval}. These functions are
 * pure -- no engine bootstrap required.
 */
public class MatViewRefreshJobClusterTest {

    @Test
    public void testCapStepBucketSizeZero() {
        final LongList intervals = list(10, 20);
        Assert.assertEquals(50L, MatViewRefreshJob.capStepByNarrowestInterval(intervals, 0, 50));
        Assert.assertEquals(50L, MatViewRefreshJob.capStepByNarrowestInterval(intervals, -1, 50));
    }

    @Test
    public void testCapStepEmptyAndSingleInterval() {
        Assert.assertEquals(100L, MatViewRefreshJob.capStepByNarrowestInterval(null, 60, 100));
        Assert.assertEquals(100L, MatViewRefreshJob.capStepByNarrowestInterval(new LongList(), 60, 100));
        // Single interval -- still cap based on its width.
        // 10..20 micros with bucketSize=10 -> widthBuckets = max(1, (20-10)/10 + 1) = 2.
        Assert.assertEquals(2L, MatViewRefreshJob.capStepByNarrowestInterval(list(10, 20), 10, 100));
    }

    @Test
    public void testCapStepNoCapNeededWhenStepIsAlreadySmall() {
        // Width = 5 buckets. step=2 already smaller -- stays unchanged.
        final LongList intervals = list(0, 50);
        Assert.assertEquals(2L, MatViewRefreshJob.capStepByNarrowestInterval(intervals, 10, 2));
    }

    @Test
    public void testCapStepSkipsMalformedHiLessThanLo() {
        // Defensive: an interval with hi < lo would produce a negative width.
        // Such entries are skipped; the cap comes from the well-formed siblings.
        final LongList intervals = list(
                50, 30,    // malformed (hi < lo), width = -20 -> skipped
                100, 100   // well-formed point, widthBuckets = 1
        );
        Assert.assertEquals(1L, MatViewRefreshJob.capStepByNarrowestInterval(intervals, 10, 50));
    }

    @Test
    public void testCapStepPicksNarrowestAmongMany() {
        // Three intervals: widths 5, 1, 3 (in buckets) at bucketSize=10.
        final LongList intervals = list(
                0, 40,        // (40-0)/10 + 1 = 5
                100, 100,     // (0)/10 + 1 = 1
                200, 220      // (20)/10 + 1 = 3
        );
        Assert.assertEquals(1L, MatViewRefreshJob.capStepByNarrowestInterval(intervals, 10, 100));
    }

    @Test
    public void testCapStepPointInterval() {
        // Point interval at 100: width 0 -> max(1, 0/10 + 1) = 1.
        Assert.assertEquals(1L, MatViewRefreshJob.capStepByNarrowestInterval(list(100, 100), 10, 50));
    }

    @Test
    public void testClusterEmptyAndSingleAreNoOps() {
        final LongList empty = new LongList();
        final LongList clustered = new LongList();
        Assert.assertEquals(0, MatViewRefreshJob.clusterIntervals(empty, clustered, 1_000, 8));
        Assert.assertEquals(0, clustered.size());
        Assert.assertEquals(0, empty.size());

        final LongList single = list(100, 200);
        Assert.assertEquals(1, MatViewRefreshJob.clusterIntervals(single, clustered, 1_000, 8));
        Assert.assertEquals(2, clustered.size());
        Assert.assertEquals(100, clustered.getQuick(0));
        Assert.assertEquals(200, clustered.getQuick(1));
        // Source must stay untouched.
        Assert.assertEquals(2, single.size());
        Assert.assertEquals(100, single.getQuick(0));
        Assert.assertEquals(200, single.getQuick(1));
    }

    @Test
    public void testClusterMaxClustersCapForcesMerge() {
        // Five disjoint intervals, gap = 100 > threshold (50), maxClusters = 3.
        // First 3 stay separate; remaining 2 must merge into the 3rd.
        final LongList intervals = list(
                0, 10,
                110, 120,
                220, 230,
                330, 340,
                440, 450
        );
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, 50, 3);
        Assert.assertEquals(3, clusters);
        Assert.assertEquals(6, clustered.size());
        Assert.assertEquals(0, clustered.getQuick(0));
        Assert.assertEquals(10, clustered.getQuick(1));
        Assert.assertEquals(110, clustered.getQuick(2));
        Assert.assertEquals(120, clustered.getQuick(3));
        // Third cluster swallows intervals 3, 4, 5.
        Assert.assertEquals(220, clustered.getQuick(4));
        Assert.assertEquals(450, clustered.getQuick(5));
        // Source must be unchanged.
        Assert.assertEquals(10, intervals.size());
    }

    @Test
    public void testClusterMaxClustersOneFloorsAtOne() {
        // maxClusters < 1 should be treated as 1 (one cluster covers everything).
        final LongList intervals = list(0, 10, 1_000, 1_010, 5_000, 5_010);
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, 1, 0);
        Assert.assertEquals(1, clusters);
        Assert.assertEquals(2, clustered.size());
        Assert.assertEquals(0, clustered.getQuick(0));
        Assert.assertEquals(5_010, clustered.getQuick(1));
    }

    @Test
    public void testClusterMergesAdjacentWhenBelowThreshold() {
        // Two intervals 5us apart; threshold = 10us -> merge.
        final LongList intervals = list(100, 110, 115, 120);
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, 10, 8);
        Assert.assertEquals(1, clusters);
        Assert.assertEquals(2, clustered.size());
        Assert.assertEquals(100, clustered.getQuick(0));
        Assert.assertEquals(120, clustered.getQuick(1));
        // Source untouched.
        Assert.assertEquals(4, intervals.size());
    }

    @Test
    public void testClusterMixedKeepsSomeMergesOthers() {
        // 5 intervals. Gaps: 5, 100, 3, 100.
        // Threshold = 10us -- gaps of 5 and 3 merge; gaps of 100 don't.
        // Expected clusters: [0,15], [115,125], [225, 235]
        final LongList intervals = list(
                0, 10,       // [0, 10]
                15, 15,      // gap 5 -> merge
                115, 125,    // gap 100 -> split
                128, 128,    // gap 3 -> merge
                228, 235     // gap 100 -> split
        );
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, 10, 8);
        Assert.assertEquals(3, clusters);
        Assert.assertEquals(6, clustered.size());
        Assert.assertEquals(0, clustered.getQuick(0));
        Assert.assertEquals(15, clustered.getQuick(1));
        Assert.assertEquals(115, clustered.getQuick(2));
        Assert.assertEquals(128, clustered.getQuick(3));
        Assert.assertEquals(228, clustered.getQuick(4));
        Assert.assertEquals(235, clustered.getQuick(5));
    }

    @Test
    public void testCapStepOverflowingBucketArithmetic() {
        // A pathological huge interval (~290 years in micros) should still
        // produce a sane cap, not overflow.
        final long farFuture = Long.MAX_VALUE / 4;
        final LongList intervals = list(0, farFuture);
        final long step = MatViewRefreshJob.capStepByNarrowestInterval(intervals, 1_000_000, 1_000);
        // Width should be huge -> step stays at the input value (1000).
        Assert.assertEquals(1_000L, step);
    }

    @Test
    public void testCapStepWithNegativeStep() {
        // A negative step (shouldn't happen, but be defensive) should not
        // be widened by the cap.
        final LongList intervals = list(0, 100);
        Assert.assertEquals(-5L, MatViewRefreshJob.capStepByNarrowestInterval(intervals, 10, -5));
    }

    @Test
    public void testClusterIntervalsAtLongMaxValue() {
        // Intervals at the extremes of the long range. The gap arithmetic
        // (lo - prevHi) could overflow if not careful; verify the function
        // remains sane.
        final LongList intervals = list(
                Long.MIN_VALUE, Long.MIN_VALUE + 1,
                Long.MAX_VALUE - 1, Long.MAX_VALUE
        );
        // gap = MAX-1 - (MIN+1) which overflows; the function still produces
        // a deterministic answer (either split or merge -- contract is "no
        // crash", not "specific result"). Just verify it doesn't throw.
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, 1_000_000L, 8);
        Assert.assertTrue("Clusters should be 1 or 2; got: " + clusters, clusters == 1 || clusters == 2);
        Assert.assertEquals("List size must match cluster count * 2", clusters * 2, clustered.size());
    }

    @Test
    public void testClusterIntervalsManySmallIntervalsHitCap() {
        // 100 disjoint point intervals, threshold = 0 so nothing merges by
        // cost. maxClusters = 8 forces merging.
        final LongList intervals = new LongList(200);
        for (long i = 0; i < 100; i++) {
            intervals.add(i * 1000);
            intervals.add(i * 1000);
        }
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, 0, 8);
        Assert.assertEquals(8, clusters);
        // Last cluster must extend to include the last point interval.
        Assert.assertEquals(99 * 1000, clustered.getQuick(clustered.size() - 1));
    }

    @Test
    public void testClusterIntervalsPointIntervalRunsAtMaxBuckets() {
        // 200 point intervals at the cacheCapacity limit. Threshold = 1
        // (don't merge), maxClusters = 32 (force merge of overflow).
        final LongList intervals = new LongList(400);
        for (long i = 0; i < 200; i++) {
            intervals.add(i * 10_000_000L);
            intervals.add(i * 10_000_000L);
        }
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, 1, 32);
        Assert.assertEquals(32, clusters);
        Assert.assertEquals(64, clustered.size());
        // First cluster is the first interval untouched.
        Assert.assertEquals(0L, clustered.getQuick(0));
        Assert.assertEquals(0L, clustered.getQuick(1));
        // Last cluster spans from the 32nd interval start to the last point.
        Assert.assertEquals(31L * 10_000_000L, clustered.getQuick(clustered.size() - 2));
        Assert.assertEquals(199L * 10_000_000L, clustered.getQuick(clustered.size() - 1));
    }

    @Test
    public void testClusterIntervalsWithZeroWidthIntervals() {
        // All point intervals (lo == hi) -- the most common case for
        // single-row WAL transactions. Should merge or split based purely
        // on the gap.
        final LongList intervals = list(
                100, 100,
                105, 105,   // gap 5
                200, 200,   // gap 95
                201, 201    // gap 1
        );
        // Threshold = 10 -> merge gaps of 5 and 1; split gap of 95.
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, 10, 8);
        Assert.assertEquals(2, clusters);
        Assert.assertEquals(100L, clustered.getQuick(0));
        Assert.assertEquals(105L, clustered.getQuick(1));
        Assert.assertEquals(200L, clustered.getQuick(2));
        Assert.assertEquals(201L, clustered.getQuick(3));
    }

    @Test
    public void testClusterNegativeThresholdTreatedAsZero() {
        // Negative threshold should never merge non-overlapping intervals.
        final LongList intervals = list(0, 10, 11, 20);
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, -100, 8);
        Assert.assertEquals(2, clusters);
        Assert.assertEquals(4, clustered.size());
    }

    @Test
    public void testClusterSourceIsNeverMutated() {
        // Lock in the option (i) contract: the source list -- which in production
        // is the live viewState.refreshIntervals reference -- must be preserved
        // bit-for-bit, regardless of whether clustering actually merges anything.
        final LongList src = list(0, 10, 11, 12, 200, 210, 211, 220);
        final long[] before = new long[src.size()];
        for (int i = 0; i < src.size(); i++) {
            before[i] = src.getQuick(i);
        }
        final LongList dst = new LongList();
        // Threshold large enough to merge across every gap -> clustering does
        // collapse entries, so the algorithm exercises its write path.
        MatViewRefreshJob.clusterIntervals(src, dst, 1_000, 8);
        Assert.assertEquals(before.length, src.size());
        for (int i = 0; i < before.length; i++) {
            Assert.assertEquals("src mutated at index " + i, before[i], src.getQuick(i));
        }
    }

    @Test
    public void testClusterSplitsWhenAboveThreshold() {
        // Two intervals 1000us apart; threshold = 10us -> stay separate.
        final LongList intervals = list(100, 110, 1110, 1120);
        final LongList clustered = new LongList();
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, clustered, 10, 8);
        Assert.assertEquals(2, clusters);
        Assert.assertEquals(4, clustered.size());
        Assert.assertEquals(100, clustered.getQuick(0));
        Assert.assertEquals(110, clustered.getQuick(1));
        Assert.assertEquals(1110, clustered.getQuick(2));
        Assert.assertEquals(1120, clustered.getQuick(3));
    }

    @Test
    public void testClusterZeroThresholdNeverMergesGaps() {
        // gap=1 (adjacent but not contiguous): 0 < 1 is false, no merge.
        final LongList intervals = list(0, 10, 11, 20);
        final LongList clustered = new LongList();
        Assert.assertEquals(2, MatViewRefreshJob.clusterIntervals(intervals, clustered, 0, 8));
        Assert.assertEquals(4, clustered.size());
    }

    @Test
    public void testClusterReusesDestinationListAcrossCalls() {
        // The production caller reuses the same scratch list across refreshes.
        // Each call must clear() it first so stale entries from a previous
        // refresh never leak into the iterator.
        final LongList dst = new LongList();
        dst.add(999);
        dst.add(999);
        dst.add(999);
        dst.add(999);

        MatViewRefreshJob.clusterIntervals(list(10, 20), dst, 0, 8);
        Assert.assertEquals(2, dst.size());
        Assert.assertEquals(10L, dst.getQuick(0));
        Assert.assertEquals(20L, dst.getQuick(1));

        // Second call with an empty source -> dst must end up empty too.
        MatViewRefreshJob.clusterIntervals(new LongList(), dst, 0, 8);
        Assert.assertEquals(0, dst.size());
    }

    private static LongList list(long... values) {
        final LongList list = new LongList(values.length);
        for (long v : values) {
            list.add(v);
        }
        return list;
    }
}
