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
        Assert.assertEquals(0, MatViewRefreshJob.clusterIntervals(empty, 1_000, 8));
        Assert.assertEquals(0, empty.size());

        final LongList single = list(100, 200);
        Assert.assertEquals(1, MatViewRefreshJob.clusterIntervals(single, 1_000, 8));
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
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, 50, 3);
        Assert.assertEquals(3, clusters);
        Assert.assertEquals(6, intervals.size());
        Assert.assertEquals(0, intervals.getQuick(0));
        Assert.assertEquals(10, intervals.getQuick(1));
        Assert.assertEquals(110, intervals.getQuick(2));
        Assert.assertEquals(120, intervals.getQuick(3));
        // Third cluster swallows intervals 3, 4, 5.
        Assert.assertEquals(220, intervals.getQuick(4));
        Assert.assertEquals(450, intervals.getQuick(5));
    }

    @Test
    public void testClusterMaxClustersOneFloorsAtOne() {
        // maxClusters < 1 should be treated as 1 (one cluster covers everything).
        final LongList intervals = list(0, 10, 1_000, 1_010, 5_000, 5_010);
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, 1, 0);
        Assert.assertEquals(1, clusters);
        Assert.assertEquals(2, intervals.size());
        Assert.assertEquals(0, intervals.getQuick(0));
        Assert.assertEquals(5_010, intervals.getQuick(1));
    }

    @Test
    public void testClusterMergesAdjacentWhenBelowThreshold() {
        // Two intervals 5us apart; threshold = 10us -> merge.
        final LongList intervals = list(100, 110, 115, 120);
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, 10, 8);
        Assert.assertEquals(1, clusters);
        Assert.assertEquals(2, intervals.size());
        Assert.assertEquals(100, intervals.getQuick(0));
        Assert.assertEquals(120, intervals.getQuick(1));
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
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, 10, 8);
        Assert.assertEquals(3, clusters);
        Assert.assertEquals(6, intervals.size());
        Assert.assertEquals(0, intervals.getQuick(0));
        Assert.assertEquals(15, intervals.getQuick(1));
        Assert.assertEquals(115, intervals.getQuick(2));
        Assert.assertEquals(128, intervals.getQuick(3));
        Assert.assertEquals(228, intervals.getQuick(4));
        Assert.assertEquals(235, intervals.getQuick(5));
    }

    @Test
    public void testClusterNegativeThresholdTreatedAsZero() {
        // Negative threshold should never merge non-overlapping intervals.
        final LongList intervals = list(0, 10, 11, 20);
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, -100, 8);
        Assert.assertEquals(2, clusters);
        Assert.assertEquals(4, intervals.size());
    }

    @Test
    public void testClusterSplitsWhenAboveThreshold() {
        // Two intervals 1000us apart; threshold = 10us -> stay separate.
        final LongList intervals = list(100, 110, 1110, 1120);
        final int clusters = MatViewRefreshJob.clusterIntervals(intervals, 10, 8);
        Assert.assertEquals(2, clusters);
        Assert.assertEquals(4, intervals.size());
        Assert.assertEquals(100, intervals.getQuick(0));
        Assert.assertEquals(110, intervals.getQuick(1));
        Assert.assertEquals(1110, intervals.getQuick(2));
        Assert.assertEquals(1120, intervals.getQuick(3));
    }

    @Test
    public void testClusterZeroThresholdNeverMergesGaps() {
        // gap=1 (adjacent but not contiguous): 0 < 1 is false, no merge.
        final LongList intervals = list(0, 10, 11, 20);
        Assert.assertEquals(2, MatViewRefreshJob.clusterIntervals(intervals, 0, 8));
        Assert.assertEquals(4, intervals.size());
    }

    private static LongList list(long... values) {
        final LongList list = new LongList(values.length);
        for (long v : values) {
            list.add(v);
        }
        return list;
    }
}
