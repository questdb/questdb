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
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The sample a posting-count estimate reads, bounded so that its cost does not grow with the
 * partitions or the keys: at most {@value #SAMPLE_PARTITIONS} distinct partitions, and at most
 * {@value #MAX_COUNTS} per-key counts.
 * <ul>
 *   <li>{@link #walkMetadata} walks the frame cursor from partition metadata, without opening a
 *   partition: the rows scanned, the frames, and the G groups, the partitions the walk yields
 *   frames for. A forward walk yields a partition's frames one after another, also through
 *   {@link io.questdb.cairo.CompositeAwarePartitionFrameCursor}, whose pieces keep their
 *   partition's index;</li>
 *   <li>{@link #plan} places m = min(G, {@value #SAMPLE_PARTITIONS}) targets, the j-th the
 *   floor(j * G / m)-th group, and picks the keys a probed frame counts: kS = min(K,
 *   max(NULL + 1, {@value #MAX_COUNTS} / m)) of the K sorted index keys. NULL (key 0), when
 *   listed, is always counted as it is; the other keys are evenly spread in key order, and
 *   {@link #scaleNonNull} scales their sum by the share of the non-NULL keys they stand for;</li>
 *   <li>{@link #nextProbeFrame}, after the caller rewinds the cursor, returns the frames of the
 *   targets, at most {@value #MAX_COUNTS} / (m * kS) per target, and stops after the last
 *   target: only the targets' frames are requested in full.</li>
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
    // tests: the partitions the last estimate probed, in walk order
    private final IntList probedPartitions = new IntList();
    // the non-NULL keys each probed frame counts
    private final IntList sampledKeys = new IntList();
    private long counts;
    private int frameCount;
    private int group;
    private int groupCount;
    private boolean hasNull;
    private boolean isTarget;
    private int keyCount;
    private int lastPartition;
    private int maxProbedFrames;
    private int openedAtStart;
    private int openedPartitions;
    private int probedFrames;
    private @Nullable TableReader reader;
    private int sampleCount;
    private long scanned;
    private int target;
    private int targetGroup;

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

    int getFrameCount() {
        return frameCount;
    }

    int getOpenedPartitions() {
        return openedPartitions;
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
        while (true) {
            // the frames of target partitions are opened; the first one may be the next
            final PartitionFrame frame = cursor.next(target < sampleCount && group + 1 == targetGroup ? 0 : Long.MAX_VALUE);
            if (frame == null) {
                return null;
            }
            final int partitionIndex = frame.getPartitionIndex();
            if (partitionIndex != lastPartition) {
                lastPartition = partitionIndex;
                group++;
                if (target < sampleCount && group == targetGroup) {
                    isTarget = true;
                    probedFrames = 0;
                    probedPartitions.add(partitionIndex);
                    target++;
                    targetGroup = (int) ((long) target * groupCount / sampleCount);
                } else if (target == sampleCount) {
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
        if (keyCount == 0 || groupCount == 0) {
            hasNull = false;
            sampleCount = 0;
            return false;
        }
        sampleCount = Math.min(groupCount, samplePartitions);
        hasNull = sortedIndexKeys.getQuick(0) == 0;
        final int nullCount = hasNull ? 1 : 0;
        final int keySampleCount = Math.min(keyCount, Math.max(nullCount + 1, maxCounts / sampleCount));
        maxProbedFrames = Math.max(1, maxCounts / (sampleCount * keySampleCount));
        final int nonNullKeyCount = keyCount - nullCount;
        final int nonNullSampleCount = keySampleCount - nullCount;
        for (int i = 0; i < nonNullSampleCount; i++) {
            sampledKeys.add(sortedIndexKeys.getQuick(nullCount + (int) ((long) i * nonNullKeyCount / nonNullSampleCount)));
        }
        group = -1;
        target = 0;
        targetGroup = 0;
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
        reader = cursor.getTableReader();
        openedAtStart = reader.getOpenPartitionCount();
        openedPartitions = 0;
        scanned = 0;
        frameCount = 0;
        groupCount = 0;
        int lastPartition = -1;
        PartitionFrame frame;
        while ((frame = cursor.next(Long.MAX_VALUE)) != null) {
            scanned += frame.getRowHi() - frame.getRowLo();
            frameCount++;
            if (frame.getPartitionIndex() != lastPartition) {
                lastPartition = frame.getPartitionIndex();
                groupCount++;
            }
        }
    }

    private int nonNullKeyCount() {
        return keyCount - (hasNull ? 1 : 0);
    }
}
