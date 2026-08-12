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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

public class IntervalFwdPartitionFrameCursor extends AbstractIntervalPartitionFrameCursor {
    private static final Log LOG = LogFactory.getLog(IntervalFwdPartitionFrameCursor.class);

    /**
     * Cursor for partition frames that chronologically intersect collection of intervals.
     * Partition frame low and high row will be within intervals inclusive of edges.
     * Intervals themselves are pairs of microsecond time.
     *
     * @param configuration  engine configuration used to resolve the partition parquet decoder
     * @param intervalModel  pairs of microsecond interval values, as in "low" and "high" inclusive of
     *                       edges.
     * @param timestampIndex index of timestamp column in the readr that is used by this cursor
     */
    public IntervalFwdPartitionFrameCursor(CairoConfiguration configuration, RuntimeIntrinsicIntervalModel intervalModel, int timestampIndex) {
        super(configuration, intervalModel, timestampIndex);
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        int intervalsLo1 = this.intervalsLo;
        int intervalsHi1 = this.intervalsHi;
        int partitionLo1 = this.partitionLo;
        int partitionHi1 = this.partitionHi;
        long partitionLimit1 = this.partitionLimit;
        long size = this.sizeSoFar;

        while (intervalsLo1 < intervalsHi1 && partitionLo1 < partitionHi1) {
            // Task 5b: a cell excluded by a composite dimension predicate is skipped WITHOUT consuming
            // the current interval -- mirrors the "whole partition"/"sibling cell" resets below (both
            // reset partitionLimit1 to -1, this method's own "no residual limit" sentinel, and advance
            // partitionLo1 alone), so a later sibling cell of the SAME day still gets its own chance
            // against this interval. isCellAllowed() short-circuits true (zero cost) when no pruning is
            // in effect, so this is a no-op for a plain table or an un-pruned composite query.
            if (!isCellAllowed(partitionLo1)) {
                partitionLimit1 = -1;
                partitionLo1++;
                continue;
            }
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            final long rowCount = reader.getPartitionRowCountFromMetadata(partitionLo1);
            if (rowCount > 0) {
                final TimestampFinder timestampFinder = initTimestampFinder(partitionLo1, rowCount);

                final long intervalLo = intervals.getQuick(intervalsLo1 * 2);
                final long intervalHi = intervals.getQuick(intervalsLo1 * 2 + 1);

                final long partitionTimestampLoApprox = timestampFinder.minTimestampApproxFromMetadata();
                // interval is wholly below partition, skip interval -- unless a same-day sibling cell
                // follows, in which case fall through to the exact checks (see next()'s twin comment).
                if (partitionTimestampLoApprox > intervalHi && !hasSameDaySiblingAhead(partitionLo1, partitionHi1)) {
                    intervalsLo1++;
                    continue;
                }

                final long partitionTimestampHiApprox = timestampFinder.maxTimestampApproxFromMetadata();
                // interval is wholly above partition, skip partition
                if (partitionTimestampHiApprox < intervalLo) {
                    partitionLimit1 = -1;
                    partitionLo1++;
                    continue;
                }

                reader.openPartition(partitionLo1);
                timestampFinder.prepare();

                final long partitionTimestampLoExact = timestampFinder.minTimestampExact();
                final long partitionTimestampHiExact = timestampFinder.maxTimestampExact();
                // interval is wholly above partition, skip interval
                if (partitionTimestampLoExact > intervalHi) {
                    if (hasSameDaySiblingAhead(partitionLo1, partitionHi1)) {
                        if (intervalsLo1 + 1 < intervalsHi1
                                && intervals.getQuick((intervalsLo1 + 1) * 2) <= partitionTimestampHiExact) {
                            throw multipleSubDayIntervalsOverMultiCellDayUnsupported();
                        }
                        partitionLimit1 = -1;
                        partitionLo1++;
                        continue;
                    }
                    intervalsLo1++;
                    continue;
                }

                // interval is wholly below partition, skip partition
                if (partitionTimestampHiExact < intervalLo) {
                    partitionLimit1 = -1;
                    partitionLo1++;
                    continue;
                }

                // calculate intersection
                long lo;
                if (partitionTimestampLoExact >= intervalLo) {
                    lo = 0;
                } else {
                    // intervalLo is inclusive of value. We will look for bottom index of intervalLo - 1
                    // and then do index + 1 to skip to top of where we need to be.
                    lo = timestampFinder.findTimestamp(intervalLo - 1, partitionLimit1 == -1 ? 0 : partitionLimit1, rowCount - 1) + 1;
                }

                // Interval is inclusive of edges, and we have to bump to high bound because it is non-inclusive.
                long hi = timestampFinder.findTimestamp(intervalHi, lo, rowCount - 1) + 1;
                if (lo < hi) {
                    size += (hi - lo);

                    // we do have whole partition of fragment?
                    if (hi == rowCount) {
                        // whole partition, will need to skip to next one
                        partitionLimit1 = -1;
                        partitionLo1++;
                    } else if (partitionLo1 + 1 < partitionHi1
                            && reader.getPartitionTimestampByIndex(partitionLo1 + 1) == reader.getPartitionTimestampByIndex(partitionLo1)) {
                        // Fragment, but a sibling cell of the same day (composite table) still needs
                        // its own chance to be checked against this SAME interval -- mirrors next()'s
                        // own fix (Task 6c finding), see this class's javadoc.
                        // Task 6c review Part A: symmetric with next() -- gate the unsupported
                        // multiple-sub-day-intervals-over-one-multi-cell-day shape so count() agrees with
                        // the row scan (both throw) rather than silently miscounting the dropped rows.
                        if (intervalsLo1 + 1 < intervalsHi1
                                && intervals.getQuick((intervalsLo1 + 1) * 2) <= partitionTimestampHiExact) {
                            throw multipleSubDayIntervalsOverMultiCellDayUnsupported();
                        }
                        partitionLimit1 = -1;
                        partitionLo1++;
                    } else {
                        // only fragment, need to skip to next interval
                        partitionLimit1 = hi;
                        intervalsLo1++;
                    }
                    continue;
                }
                // Interval yielded an empty frame for THIS cell -- a same-day sibling cell may still
                // hold rows inside it (see next()'s retireIntervalOrVisitSibling).
                if (hasSameDaySiblingAhead(partitionLo1, partitionHi1)) {
                    if (intervalsLo1 + 1 < intervalsHi1
                            && intervals.getQuick((intervalsLo1 + 1) * 2) <= partitionTimestampHiExact) {
                        throw multipleSubDayIntervalsOverMultiCellDayUnsupported();
                    }
                    partitionLimit1 = -1;
                    partitionLo1++;
                    continue;
                }
                partitionLimit1 = hi;
                intervalsLo1++;
            } else {
                // partition was empty, just skip to next
                partitionLo1++;
            }
        }

        counter.add(size - this.sizeSoFar);
    }

    @Override
    public PartitionFrame next(long skipTarget) {
        // order of logical operations is important
        // we are not calculating partition ranges when intervals are empty
        while (intervalsLo < intervalsHi && partitionLo < partitionHi) {
            // Task 5b: see calculateSize()'s identical comment -- this method's own "no residual limit"
            // sentinel is 0 (not -1; see toTop()), matching every other advance-partitionLo-alone branch
            // below.
            if (!isCellAllowed(partitionLo)) {
                partitionLimit = 0;
                partitionLo++;
                continue;
            }
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            long rowCount = reader.getPartitionRowCountFromMetadata(partitionLo);
            if (rowCount > 0) {
                final TimestampFinder timestampFinder = initTimestampFinder(partitionLo, rowCount);

                final long intervalLo = intervals.getQuick(intervalsLo * 2);
                final long intervalHi = intervals.getQuick(intervalsLo * 2 + 1);

                final long partitionTimestampLoApprox = timestampFinder.minTimestampApproxFromMetadata();
                // Interval is wholly above partition, skip interval -- UNLESS a sibling cell of the same
                // day follows (composite table). Retiring the interval here would abandon every later
                // sibling, and a sibling is an independent cell whose rows may well fall inside this
                // interval even though THIS cell's do not. Fall through to the exact checks below, which
                // handle the sibling case uniformly; the cost (opening a partition this early-out would
                // have skipped) is paid only by a composite multi-cell day. Unreachable for a plain
                // table: its partitionLo + 1 is always the NEXT day, never a same-timestamp sibling.
                if (partitionTimestampLoApprox > intervalHi && !hasSameDaySiblingAhead(partitionLo, partitionHi)) {
                    intervalsLo++;
                    continue;
                }

                final long partitionTimestampHiApprox = timestampFinder.maxTimestampApproxFromMetadata();
                // interval is wholly below partition, skip partition
                if (partitionTimestampHiApprox < intervalLo) {
                    partitionLimit = 0;
                    partitionLo++;
                    continue;
                }

                LOG.debug()
                        .$("next [partition=").$(partitionLo)
                        .$(", intervalLo=").$ts(intervalModel.getTimestampDriver(), intervalLo)
                        .$(", intervalHi=").$ts(intervalModel.getTimestampDriver(), intervalHi)
                        .$(", partitionHi=").$ts(intervalModel.getTimestampDriver(), partitionTimestampHiApprox)
                        .$(", partitionLimit=").$(partitionLimit)
                        .$(", rowCount=").$(rowCount)
                        .I$();

                reader.openPartition(partitionLo);
                timestampFinder.prepare();

                final long partitionTimestampLoExact = timestampFinder.minTimestampExact();
                final long partitionTimestampHiExact = timestampFinder.maxTimestampExact();
                // interval is wholly above partition, skip interval
                if (partitionTimestampLoExact > intervalHi) {
                    if (retireIntervalOrVisitSibling(partitionTimestampHiExact)) {
                        continue;
                    }
                    intervalsLo++;
                    continue;
                }

                // interval is wholly below partition, skip partition
                if (partitionTimestampHiExact < intervalLo) {
                    partitionLimit = 0;
                    partitionLo++;
                    continue;
                }

                // calculate intersection

                long lo;
                if (partitionTimestampLoExact < intervalLo) {
                    // intervalLo is inclusive of value. We will look for bottom index of intervalLo - 1
                    // and then do index + 1 to skip to top of where we need to be.
                    // We are not scanning up on the exact value of intervalLo because it may not exist. In which case
                    // the search function will scan up to top of the lower value.
                    lo = timestampFinder.findTimestamp(intervalLo - 1, partitionLimit, rowCount - 1) + 1;
                } else {
                    lo = 0;
                }

                final long hi;
                if (partitionTimestampHiExact > intervalHi) {
                    hi = timestampFinder.findTimestamp(intervalHi, lo, rowCount - 1) + 1;
                } else {
                    hi = rowCount;
                }

                if (lo < hi) {
                    frame.partitionIndex = partitionLo;
                    frame.rowLo = lo;
                    frame.rowHi = hi;
                    sizeSoFar += (hi - lo);

                    final byte format = reader.getPartitionFormat(partitionLo);
                    if (format == PartitionFormat.PARQUET) {
                        frame.format = PartitionFormat.PARQUET;
                        frame.parquetMetaDecoder = reader.getAndInitParquetPartitionDecoder(partitionLo);
                    } else {
                        assert format == PartitionFormat.NATIVE;
                        frame.format = PartitionFormat.NATIVE;
                        frame.parquetMetaDecoder = null;
                    }

                    // we do have whole partition of fragment?
                    if (hi == rowCount) {
                        // whole partition, will need to skip to next one
                        partitionLimit = 0;
                        partitionLo++;
                    } else if (partitionLo + 1 < partitionHi
                            && reader.getPartitionTimestampByIndex(partitionLo + 1) == reader.getPartitionTimestampByIndex(partitionLo)) {
                        // Fragment (interval's HIGH bound reached mid-partition), but a SIBLING cell of
                        // the SAME day (composite table, higher cellKey, not yet visited in this forward
                        // scan) still needs its own chance to be checked against this SAME interval --
                        // do NOT retire the interval yet. Task 6c (read-side differential capstone)
                        // finding: this branch previously always advanced intervalsLo unconditionally,
                        // silently never visiting any sibling cell of a multi-cell day whenever the
                        // FIRST-visited (lowest cellKey) sibling's own data extended past the interval's
                        // hi bound -- e.g. a query entirely within one day, or whose hi bound falls
                        // mid-day, over a composite table with 2+ cells that day, silently dropped every
                        // cell but the lowest cellKey. cullPartitions' own high-boundary fix (commit
                        // 233532984f) correctly widens [partitionLo, partitionHi) to include every
                        // sibling, but this loop never reached the later siblings because it gave up on
                        // the interval (and therefore the whole scan, since composite queries typically
                        // have exactly one interval) as soon as the FIRST cell yielded a fragment.
                        // Provably byte-identical to the prior behaviour for a plain table (a plain
                        // table's partitionLo+1 is always the NEXT DAY, never a same-timestamp sibling,
                        // so this branch is unreachable there) -- kept unconditional rather than gated on
                        // composite detection, mirroring 233532984f's own precedent.
                        //
                        // Task 6c review Part A: the SINGLE-interval sibling visit above is correct, but
                        // 2+ intervals over this SAME multi-cell day are not yet supported -- advancing
                        // partitionLo to the sibling ABANDONS this fragmented cell (its rows past intervalHi
                        // are unconsumed), and monotonic partitionLo can never revisit it for a LATER
                        // interval. If that later interval reaches into this cell's own span (its lo <= this
                        // cell's exact max ts) those leftover rows would be SILENTLY dropped -- gate loudly
                        // instead (proven to fire on exactly the drop cases, never on a correct multi-DAY
                        // date-list). See AbstractIntervalPartitionFrameCursor#multipleSubDayIntervalsOverMultiCellDayUnsupported.
                        if (intervalsLo + 1 < intervalsHi
                                && intervals.getQuick((intervalsLo + 1) * 2) <= partitionTimestampHiExact) {
                            throw multipleSubDayIntervalsOverMultiCellDayUnsupported();
                        }
                        partitionLimit = 0;
                        partitionLo++;
                    } else {
                        // only fragment, no sibling cell left to check -- this interval is now fully
                        // satisfied, exactly as before this fix.
                        partitionLimit = hi;
                        intervalsLo++;
                    }

                    return frame;
                }
                // Interval yielded an empty frame for THIS cell. A sibling cell of the same day is an
                // independent cell and may well have rows inside this interval, so it must get its own
                // chance before the interval is retired -- same reasoning as the fragment branch above.
                if (retireIntervalOrVisitSibling(partitionTimestampHiExact)) {
                    continue;
                }
                partitionLimit = hi;
                intervalsLo++;
            } else {
                // partition was empty, just skip to next
                partitionLo++;
            }
        }
        return null;
    }

    /**
     * Shared tail for {@link #next(long)}'s two interval-retiring exits (cell wholly above the interval,
     * and cell yielding an empty frame). Returns {@code true} when the caller should {@code continue}
     * the scan at the next same-day sibling cell rather than retire the interval.
     * <p>
     * Retiring the interval at either exit is correct for a plain table, where {@code partitionLo + 1} is
     * always the next DAY: nothing of this interval is left to find. It is wrong for a composite
     * multi-cell day, where the following partition can be a SIBLING CELL of the same day -- a separate
     * cell, with its own rows, which may fall squarely inside the interval this cell just failed to
     * match. Retiring the interval there silently drops those rows.
     * <p>
     * Advancing to the sibling instead abandons THIS cell for any LATER interval (monotonic
     * {@code partitionLo} can never come back to it). That is the same trade the fragment branch makes,
     * and it carries the same guard: if a later interval reaches into this cell's own span, the rows it
     * would have matched here are unrecoverable, so fail loudly rather than drop them silently.
     */
    private boolean retireIntervalOrVisitSibling(long partitionTimestampHiExact) {
        if (!hasSameDaySiblingAhead(partitionLo, partitionHi)) {
            return false;
        }
        if (intervalsLo + 1 < intervalsHi
                && intervals.getQuick((intervalsLo + 1) * 2) <= partitionTimestampHiExact) {
            throw multipleSubDayIntervalsOverMultiCellDayUnsupported();
        }
        partitionLimit = 0;
        partitionLo++;
        return true;
    }

    /**
     * Whether the partition after {@code partitionIndex} is a SIBLING CELL of the same day rather than
     * the next day -- i.e. whether the two share a partition timestamp. Only a composite table's
     * multi-cell day can produce this; for a plain table (one cell per day) it is always {@code false},
     * which is what keeps every caller byte-identical for plain tables.
     */
    private boolean hasSameDaySiblingAhead(int partitionIndex, int partitionHiBound) {
        return partitionIndex + 1 < partitionHiBound
                && reader.getPartitionTimestampByIndex(partitionIndex + 1)
                == reader.getPartitionTimestampByIndex(partitionIndex);
    }

    @Override
    public void toTop() {
        super.toTop();
        partitionLimit = 0;
    }
}
