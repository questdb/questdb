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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The sample a posting-count estimate reads, bounded so that its cost does not grow with the
 * partitions or the keys: at most {@value #SAMPLE_PARTITIONS} distinct partitions, and at most
 * {@value #MAX_COUNTS} per-key counts.
 * <ul>
 *   <li>{@link #walkMetadata} walks the frame cursor from partition metadata, without opening a
 *   partition: the rows scanned, the frames, and the G groups, the partitions the walk yields
 *   frames for, with the rows each holds. A forward walk yields a partition's frames one after
 *   another, also through {@link io.questdb.cairo.CompositeAwarePartitionFrameCursor}, whose
 *   pieces keep their partition's index;</li>
 *   <li>{@link #plan} spends the counts on the keys first: a probed frame counts kS = min(K,
 *   max(NULL + 1, {@value #MAX_COUNTS})) of the K sorted index keys, every key of a list up to
 *   {@value #MAX_COUNTS} keys. Per-key row counts are skewed (a few keys hold most of the rows),
 *   so a sample of the keys misses or over-weights the heavy ones and errs by a factor of three
 *   either way, while a key's share of a partition's rows varies far less between partitions.
 *   Of the groups, m = min(G, {@value #SAMPLE_PARTITIONS}, max(1, {@value #MAX_COUNTS} / kS))
 *   are probed. When m = G every group is, each weighted by its rows, and the estimate is the
 *   counts' sum. Otherwise the m targets are placed by rows: the j-th is the group holding the
 *   scan's (j + 1/2) * scanned / m-th row, so a group is probed in proportion to its rows and a
 *   group larger than scanned / m stands for several targets, its weight; at the midpoints, the
 *   first and the last group are probed for their rows only, not for their place. When a list
 *   holds more keys than the budget, NULL (key 0), when listed, is always counted as it is; the
 *   other keys are evenly spread in key order, and
 *   {@link #scaleNonNull} scales their sum by the share of the non-NULL keys they stand for;</li>
 *   <li>{@link #nextProbeFrame}, after the caller rewinds the cursor, returns the frames of the
 *   targets, at most {@value #MAX_COUNTS} / (m * kS) per target, and stops after the last
 *   target: only the targets' frames are requested in full. The caller reports each probed
 *   frame's selected rows to {@link #addProbe}, and {@link #getEstimatedSelected} is the
 *   weighted mean of the probed groups' selected shares times the rows scanned: drawn by rows,
 *   a group's share counts by its targets, not by its rows again.</li>
 * </ul>
 * One instance serves one factory, on the thread that runs its cursor; each estimate starts with
 * {@link #walkMetadata}, which resets it.
 */
final class PostingEstimateSample {
    // the (partition, key) counts one estimate reads at most
    static final int MAX_COUNTS = 4_096;
    // the distinct partitions one estimate probes at most
    static final int SAMPLE_PARTITIONS = 64;
    // the budget the estimates read: the constants above, which only tests change
    private static int maxCounts = MAX_COUNTS;
    private static int samplePartitions = SAMPLE_PARTITIONS;
    // the rows scanned up to the end of each group, in walk order
    private final LongList groupEndRows = new LongList();
    // tests: the partitions the last estimate probed, in walk order
    private final IntList probedPartitions = new IntList();
    // the non-NULL keys each probed frame counts
    private final IntList sampledKeys = new IntList();
    // the groups the targets fall in, distinct and ascending, and the weight of each: its rows
    // when every group is probed, else the targets it stands for
    private final IntList targetGroups = new IntList();
    private final LongList targetWeights = new LongList();
    private long counts;
    private int frameCount;
    private int group;
    private int groupCount;
    private long groupRows;
    private long groupSelected;
    private double groupSumWeight;
    private long groupWeight;
    private boolean hasNull;
    private boolean isTarget;
    private int keyCount;
    private int lastPartition;
    private int maxProbedFrames;
    private int openedAtStart;
    private int openedPartitions;
    private int probedFrames;
    private @Nullable TableReader reader;
    private long scanned;
    private double shareSum;
    private int target;
    private double weightSum;

    /**
     * Sets the budget of every estimate: the partitions it probes and the counts it reads, at
     * most. Tests that change it restore {@value #SAMPLE_PARTITIONS} and {@value #MAX_COUNTS}.
     */
    @TestOnly
    static void setBudgetForTesting(int samplePartitions, int maxCounts) {
        PostingEstimateSample.samplePartitions = samplePartitions;
        PostingEstimateSample.maxCounts = maxCounts;
    }

    // tests: the counts the probed frames read since the last walk
    void addCounts(int n) {
        counts += n;
    }

    /**
     * Adds a probed frame to the selection estimate: the rows it holds, and those of them the
     * keys select.
     */
    void addProbe(long selected, long rows) {
        groupSelected += Math.min(selected, rows);
        groupRows += rows;
    }

    /**
     * Ends an estimate: the partitions it opened are known from here on.
     */
    void finish() {
        // every estimate walks the cursor first, see walkMetadata()
        assert reader != null;
        openedPartitions = reader.getOpenPartitionCount() - openedAtStart;
        reader = null;
    }

    long getCounts() {
        return counts;
    }

    /**
     * The rows the keys are estimated to select over the whole scan, from the frames
     * {@link #addProbe} was given: the probed groups' selected shares, each by its weight, times
     * the rows scanned. 0 when no probed frame held a row.
     */
    long getEstimatedSelected() {
        closeGroup();
        // the mean of the shares lands a few ulps under the exact quotient that equal groups give,
        // floor(selected * scanned / rows); the relative 1e-9 keeps their floor
        return weightSum > 0 ? (long) (shareSum / weightSum * scanned * (1 + 1e-9)) : 0;
    }

    int getFrameCount() {
        return frameCount;
    }

    int getOpenedPartitions() {
        return openedPartitions;
    }

    /**
     * The weight that makes a sum over the probed groups' counts an estimate of the sum over all
     * the groups, for the group of the frame {@link #nextProbeFrame} returned last: 1 when every
     * group is probed, else its targets over its rows, since the targets draw a group in proportion
     * to its rows (Horvitz-Thompson). A mean over pairs, such as the rows per (partition, key),
     * needs it: a few large groups would stand for the whole table otherwise.
     */
    double getProbeSumWeight() {
        return groupSumWeight;
    }

    IntList getProbedPartitions() {
        return probedPartitions;
    }

    int getSampledKey(int i) {
        return sampledKeys.getQuick(i);
    }

    // the non-NULL keys a probed frame counts
    int getSampledKeyCount() {
        return sampledKeys.size();
    }

    long getScanned() {
        return scanned;
    }

    boolean hasNull() {
        return hasNull;
    }

    // whether some keys are not counted, so that the counted ones stand for more
    boolean isScaled() {
        return sampledKeys.size() + (hasNull ? 1 : 0) < keyCount;
    }

    /**
     * The frames of the targets, at most {@code MAX_COUNTS / (m * kS)} per target, or null after
     * the last one. Frames of other partitions are requested with a saturated skip target, which
     * the full cursors answer from partition metadata.
     */
    @Nullable
    PartitionFrame nextProbeFrame(PartitionFrameCursor cursor) {
        final int targetCount = targetGroups.size();
        while (true) {
            // the frames of target partitions are opened; the first one may be the next
            final PartitionFrame frame = cursor.next(target < targetCount && group + 1 == targetGroups.getQuick(target) ? 0 : Long.MAX_VALUE);
            if (frame == null) {
                return null;
            }
            final int partitionIndex = frame.getPartitionIndex();
            if (partitionIndex != lastPartition) {
                lastPartition = partitionIndex;
                group++;
                if (target < targetCount && group == targetGroups.getQuick(target)) {
                    closeGroup();
                    isTarget = true;
                    probedFrames = 0;
                    probedPartitions.add(partitionIndex);
                    groupWeight = targetWeights.getQuick(target);
                    if (targetGroups.size() == groupCount) {
                        groupSumWeight = 1;
                    } else {
                        final int g = targetGroups.getQuick(target);
                        groupSumWeight = (double) groupWeight / (groupEndRows.getQuick(g) - (g == 0 ? 0 : groupEndRows.getQuick(g - 1)));
                    }
                    target++;
                } else if (target == targetCount) {
                    return null;
                } else {
                    isTarget = false;
                }
            }
            if (isTarget && probedFrames < maxProbedFrames) {
                probedFrames++;
                return frame;
            }
        }
    }

    /**
     * Places the targets and picks the keys for the walk {@link #walkMetadata} made.
     *
     * @param sortedIndexKeys the list's index keys, sorted and distinct
     * @return false when there is nothing to probe: no key, or no partition with rows
     */
    boolean plan(IntList sortedIndexKeys) {
        keyCount = sortedIndexKeys.size();
        sampledKeys.clear();
        targetGroups.clear();
        targetWeights.clear();
        if (keyCount == 0 || groupCount == 0 || scanned == 0) {
            hasNull = false;
            return false;
        }
        hasNull = sortedIndexKeys.getQuick(0) == 0;
        final int nullCount = hasNull ? 1 : 0;
        // the keys first: every key while the list fits the budget
        final int keySampleCount = Math.min(keyCount, Math.max(nullCount + 1, maxCounts));
        final int sampleCount = Math.min(Math.min(groupCount, samplePartitions), Math.max(1, maxCounts / keySampleCount));
        maxProbedFrames = Math.max(1, maxCounts / (sampleCount * keySampleCount));
        final int nonNullKeyCount = keyCount - nullCount;
        final int nonNullSampleCount = keySampleCount - nullCount;
        for (int i = 0; i < nonNullSampleCount; i++) {
            sampledKeys.add(sortedIndexKeys.getQuick(nullCount + (int) ((long) i * nonNullKeyCount / nonNullSampleCount)));
        }
        if (sampleCount == groupCount) {
            // every group, by its rows
            for (int g = 0; g < groupCount; g++) {
                targetGroups.add(g);
                targetWeights.add(groupEndRows.getQuick(g) - (g == 0 ? 0 : groupEndRows.getQuick(g - 1)));
            }
        } else {
            // the targets by rows: the j-th at the scan's (2j + 1) * scanned / 2m-th row, which
            // over equal groups is the floor((2j + 1) * G / 2m)-th group
            for (int j = 0, g = 0; j < sampleCount; j++) {
                final long row = (long) ((2.0 * j + 1) * scanned / (2.0 * sampleCount));
                while (groupEndRows.getQuick(g) <= row) {
                    g++;
                }
                final int n = targetGroups.size();
                if (n > 0 && targetGroups.getQuick(n - 1) == g) {
                    targetWeights.increment(n - 1);
                } else {
                    targetGroups.add(g);
                    targetWeights.add(1);
                }
            }
        }
        group = -1;
        target = 0;
        probedFrames = 0;
        isTarget = false;
        lastPartition = -1;
        return true;
    }

    // the sampled non-NULL keys' count scaled to all the non-NULL keys
    long scaleNonNull(long sum) {
        return isScaled() ? (long) ((double) sum * nonNullKeyCount() / sampledKeys.size()) : sum;
    }

    // the share of the non-NULL keys one sampled key stands for, when isScaled()
    double scaleOfNonNull() {
        assert isScaled();
        return (double) nonNullKeyCount() / sampledKeys.size();
    }

    /**
     * Starts an estimate: walks the cursor to its end from partition metadata, counting the rows
     * scanned, the frames and the partitions with rows. The caller rewinds it before
     * {@link #nextProbeFrame}.
     */
    void walkMetadata(PartitionFrameCursor cursor) {
        counts = 0;
        probedPartitions.clear();
        groupEndRows.clear();
        reader = cursor.getTableReader();
        openedAtStart = reader.getOpenPartitionCount();
        openedPartitions = 0;
        scanned = 0;
        frameCount = 0;
        groupCount = 0;
        groupRows = 0;
        groupSelected = 0;
        groupWeight = 0;
        groupSumWeight = 0;
        shareSum = 0;
        weightSum = 0;
        int lastPartition = -1;
        PartitionFrame frame;
        while ((frame = cursor.next(Long.MAX_VALUE)) != null) {
            scanned += frame.getRowHi() - frame.getRowLo();
            frameCount++;
            if (frame.getPartitionIndex() != lastPartition) {
                lastPartition = frame.getPartitionIndex();
                groupCount++;
                groupEndRows.add(scanned);
            } else {
                groupEndRows.setQuick(groupCount - 1, scanned);
            }
        }
    }

    // adds the probed group's share to the estimate, by its weight, see targetWeights
    private void closeGroup() {
        if (groupRows > 0) {
            shareSum += groupWeight * ((double) groupSelected / groupRows);
            weightSum += groupWeight;
        }
        groupRows = 0;
        groupSelected = 0;
    }

    private int nonNullKeyCount() {
        return keyCount - (hasNull ? 1 : 0);
    }
}
