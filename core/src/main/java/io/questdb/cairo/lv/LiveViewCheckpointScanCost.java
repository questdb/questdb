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

package io.questdb.cairo.lv;

import io.questdb.cairo.TableReader;
import org.jetbrains.annotations.NotNull;

/**
 * Estimates how many base rows a repair's scan over one inclusive timestamp
 * interval pulls, so {@link LiveViewCheckpointRepairPlan} can choose between
 * restoring a predecessor checkpoint and reconstructing from the dependency floor
 * on evidence rather than on whichever happens to be available.
 * <p>
 * The estimate reads only the pinned reader's transaction file - the partition
 * timestamps and row counts it already holds in memory - so it opens no partition,
 * touches no column and costs one walk over the partitions the interval overlaps.
 * Planning runs it twice per repair, once per disposition, and it must therefore
 * stay cheaper than the difference it is there to detect.
 * <p>
 * Rows inside one partition are assumed evenly spread over that partition's span,
 * because an even spread is the only distribution the transaction file describes.
 * A partition the interval covers whole contributes its exact row count; only the
 * two partitions the interval ends inside are interpolated. Counting those whole
 * instead would price a narrow repair interval as the entire partition holding it,
 * which for a daily partition is the difference the choice turns on.
 * <p>
 * What the estimate deliberately ignores is the view's {@code WHERE} filter and
 * the per-row window work above the scan. Both dispositions read through the same
 * stack, so both are scaled by the same factor and the comparison is unaffected;
 * an absolute row count would need the filter's selectivity, which planning does
 * not know.
 * <p>
 * One instance per refresh job, bound to the repair's pinned reader by {@link #of}
 * and reused across repairs.
 */
public final class LiveViewCheckpointScanCost implements LiveViewCheckpointRepairPlan.ScanCostSource {
    private TableReader reader;

    @Override
    public long estimateScanRows(long lowTs, long highTsInclusive) {
        if (highTsInclusive < lowTs || reader.size() == 0) {
            return 0;
        }
        final int partitionCount = reader.getPartitionCount();
        final long tableMaxTs = reader.getMaxTimestamp();
        // PARTITION BY NONE maintains no table minimum - it reads back as
        // Long.MAX_VALUE, above the maximum - so the single partition's own floor is
        // the only lower bound there is. A partitioned table always has both.
        final long tableMinTs = reader.getMinTimestamp() <= tableMaxTs
                ? reader.getMinTimestamp()
                : reader.getPartitionMinTimestampFromMetadata(0);
        long rows = 0;
        // An interval starting below the table's own minimum searches to -1, which is
        // the first partition. Same clamp the interval partition-frame cursor applies.
        for (int i = Math.max(0, reader.getPartitionIndexByTimestamp(lowTs)); i < partitionCount; i++) {
            // The outer partitions are bounded by the table's own extremes rather than by
            // the partition floor and ceiling: the first partition holds no row below the
            // table minimum and the last none above its maximum, and an interpolation over
            // the nominal span would spread the rows across time that holds none. For
            // PARTITION BY NONE the ceiling is not even representable - it is positive
            // infinity - so the maximum is the only bound there is.
            final long partitionLowTs = i == 0 ? tableMinTs : reader.getPartitionMinTimestampFromMetadata(i);
            if (partitionLowTs > highTsInclusive) {
                break;
            }
            final long partitionHighTs = i + 1 < partitionCount
                    ? reader.getPartitionMaxTimestampFromMetadata(i)
                    : tableMaxTs;
            if (partitionHighTs < lowTs) {
                continue;
            }
            final long partitionRows = reader.getPartitionRowCountFromMetadata(i);
            if (partitionRows <= 0) {
                continue;
            }
            if (partitionLowTs >= lowTs && partitionHighTs <= highTsInclusive) {
                rows += partitionRows;
                continue;
            }
            // The interval ends inside this partition. Arithmetic in double because a
            // PARTITION BY NONE span reaches across the whole timestamp range and would
            // overflow a long subtraction; the ratio needs no more precision than that.
            final double span = (double) partitionHighTs - (double) partitionLowTs + 1;
            final double covered = (double) Math.min(highTsInclusive, partitionHighTs)
                    - (double) Math.max(lowTs, partitionLowTs) + 1;
            rows += (long) (partitionRows * Math.min(1, covered / span));
        }
        return rows;
    }

    /**
     * Binds the estimate to the snapshot one repair plans against. Both dispositions
     * are priced against the same pinned reader for the same reason their bounds are
     * derived against it: a reader at another {@code seqTxn} describes data neither
     * scan is going to read.
     */
    public void of(@NotNull TableReader reader) {
        this.reader = reader;
    }
}
