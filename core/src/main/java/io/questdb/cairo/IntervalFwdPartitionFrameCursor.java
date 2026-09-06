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

    /**
     * NOTE on the composite sibling-cell handling below, mirrored from {@link #next(long)}: no COMPOSITE
     * query reaches this method today. The composite cross-cell merge cursor wraps this one and counts by
     * iterating {@code next()} rather than delegating here -- measured, by instrumenting this method and
     * finding that {@code SELECT count()} with a timestamp filter reaches it on a PLAIN table and on no
     * composite query tried. The mirrored logic is kept deliberately rather than left out: counting by
     * iteration is an obvious thing to optimise later, and a delegation added then would silently
     * reintroduce the dropped-rows defect this fix exists for. It is unreachable-for-composite
     * futureproofing, NOT a live fix -- the live fix is in {@code next()}.
     */
    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        int intervalsLo1 = this.intervalsLo;
        int intervalsHi1 = this.intervalsHi;
        int partitionLo1 = this.partitionLo;
        int partitionHi1 = this.partitionHi;
        long partitionLimit1 = this.partitionLimit;
        long size = this.sizeSoFar;

        // 9A: this method must not mutate cursor state, so it carries its own copies of the run fields.
        // Note the sentinel asymmetry with next(): "no residual limit" is -1 here and 0 there. That is
        // pre-existing and deliberately preserved; unifying it is a separate change.
        int runHi1 = -1;
        int runIntervalLo1 = 0;
        int runResume1 = 0;

        while (partitionLo1 < partitionHi1 && (intervalsLo1 < intervalsHi1 || partitionLo1 < runHi1)) {
            if (partitionLo1 >= runHi1) {
                runHi1 = forwardRunEnd(partitionLo1, partitionHi1);
                runIntervalLo1 = intervalsLo1;
                runResume1 = intervalsHi1;
            }
            // this cell has consumed every interval -- hand the run on to its next cell
            if (intervalsLo1 >= intervalsHi1) {
                if (intervalsLo1 < runResume1) {
                    runResume1 = intervalsLo1;
                }
                partitionLimit1 = -1;
                partitionLo1++;
                intervalsLo1 = partitionLo1 >= runHi1 ? runResume1 : runIntervalLo1;
                continue;
            }
            // Task 5b: a cell excluded by a composite dimension predicate is skipped WITHOUT consuming
            // the current interval, so a sibling cell of the SAME day still gets its own chance against
            // it. isCellAllowed() short-circuits true (zero cost) when no pruning is in effect, so this
            // is a no-op for a plain table or an un-pruned composite query.
            if (!isCellAllowed(partitionLo1)) {
                if (intervalsLo1 < runResume1) {
                    runResume1 = intervalsLo1;
                }
                partitionLimit1 = -1;
                partitionLo1++;
                intervalsLo1 = partitionLo1 >= runHi1 ? runResume1 : runIntervalLo1;
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
                // Interval is wholly above partition, retire the interval FOR THIS CELL. 9A dropped the
                // sibling fall-through: siblings walk intervals from runIntervalLo1 themselves.
                if (partitionTimestampLoApprox > intervalHi) {
                    intervalsLo1++;
                    continue;
                }

                final long partitionTimestampHiApprox = timestampFinder.maxTimestampApproxFromMetadata();
                // Interval is wholly below partition: this cell's max ts is under the current interval's
                // lo, therefore under every LATER interval's lo -- the CELL is exhausted.
                if (partitionTimestampHiApprox < intervalLo) {
                    if (intervalsLo1 < runResume1) {
                        runResume1 = intervalsLo1;
                    }
                    intervalsLo1 = intervalsHi1;
                    continue;
                }

                reader.openPartition(partitionLo1);
                timestampFinder.prepare();

                final long partitionTimestampLoExact = timestampFinder.minTimestampExact();
                final long partitionTimestampHiExact = timestampFinder.maxTimestampExact();
                // interval is wholly above partition, skip interval (for THIS cell only)
                if (partitionTimestampLoExact > intervalHi) {
                    intervalsLo1++;
                    continue;
                }

                // interval is wholly below partition -- cell exhausted, see the approx twin above
                if (partitionTimestampHiExact < intervalLo) {
                    if (intervalsLo1 < runResume1) {
                        runResume1 = intervalsLo1;
                    }
                    intervalsLo1 = intervalsHi1;
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
                        // Whole partition consumed, so the CELL is exhausted -- but the INTERVAL is not.
                        // It may reach into the next day, so it survives via runResume1.
                        if (intervalsLo1 < runResume1) {
                            runResume1 = intervalsLo1;
                        }
                        intervalsLo1 = intervalsHi1;
                    } else {
                        // Fragment: the interval's hi bound fell inside this cell, so the interval is
                        // finished FOR THIS CELL. 9A deleted the sibling special-case and its gate.
                        partitionLimit1 = hi;
                        intervalsLo1++;
                    }
                    continue;
                }
                // Interval yielded an empty frame for this cell -- retire it for this cell only.
                partitionLimit1 = hi;
                intervalsLo1++;
            } else {
                // Partition was empty, just skip to next. partitionLimit1 is deliberately NOT reset
                // here, matching the pre-9A walk exactly.
                if (intervalsLo1 < runResume1) {
                    runResume1 = intervalsLo1;
                }
                partitionLo1++;
                intervalsLo1 = partitionLo1 >= runHi1 ? runResume1 : runIntervalLo1;
            }
        }

        counter.add(size - this.sizeSoFar);
    }

    @Override
    public PartitionFrame next(long skipTarget) {
        // order of logical operations is important
        // we are not calculating partition ranges when intervals are empty
        // 9A: "partitionLo < runHi" is the disjunct that makes the cell-major walk work -- it keeps the
        // loop alive for a cell that has consumed every interval so it can reach advanceForwardCell()
        // and hand the run on to its next cell. Without it the loop would exit the moment the FIRST
        // cell of a day finished its intervals, silently dropping every later cell of that day. It also
        // terminates the walk promptly once a run completes with nothing left (partitionLo >= runHi and
        // intervalsLo == intervalsHi) rather than opening empty runs over every remaining partition.
        while (partitionLo < partitionHi && (intervalsLo < intervalsHi || partitionLo < runHi)) {
            // 9A: open a day-run on entry and whenever the previous one completed. Every cell of the run
            // is walked from runIntervalLo, so each cell sees EVERY interval. A PLAIN table's run is a
            // single partition, so this collapses to a no-op there.
            if (partitionLo >= runHi) {
                beginForwardRun();
            }
            // this cell has consumed every interval -- hand the run on to its next cell
            if (intervalsLo >= intervalsHi) {
                partitionLimit = 0;
                advanceForwardCell();
                continue;
            }
            // Task 5b: see calculateSize()'s identical comment -- this method's own "no residual limit"
            // sentinel is 0 (not -1; see toTop()), matching every other advance-partitionLo-alone branch
            // below.
            if (!isCellAllowed(partitionLo)) {
                partitionLimit = 0;
                advanceForwardCell();
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
                // handle the sibling case uniformly. MEASURED, so nobody re-optimises it on a hunch:
                // 400 cells in one day, point query, 447 us/query with this fall-through vs 449 us for a
                // variant that dismisses the cell from approx metadata without opening it -- no
                // difference, because the reader already holds those partitions open. The fall-through is
                // also the safer of the two: it guards using EXACT timestamps rather than conservative
                // approximations, which would throw "unsupported" on queries that actually work.
                // Unreachable for a plain table: its partitionLo + 1 is always the NEXT day.
                //
                // 9A REPLACED ALL OF THE ABOVE with the plain approx check. The sibling fall-through
                // existed only because a retired interval could never be revisited by a later cell of
                // the same day. Cells now walk intervals independently, so retiring this interval FOR
                // THIS CELL abandons nothing -- every sibling gets its own pass from runIntervalLo.
                if (partitionTimestampLoApprox > intervalHi) {
                    intervalsLo++;
                    continue;
                }

                final long partitionTimestampHiApprox = timestampFinder.maxTimestampApproxFromMetadata();
                // Interval is wholly below partition. This cell's max ts is below the current interval's
                // lo, therefore below every LATER interval's lo too -- the CELL is exhausted, not merely
                // this interval.
                if (partitionTimestampHiApprox < intervalLo) {
                    partitionLimit = 0;
                    advanceForwardCell();
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
                // interval is wholly above partition, skip interval (for THIS cell only)
                if (partitionTimestampLoExact > intervalHi) {
                    intervalsLo++;
                    continue;
                }

                // interval is wholly below partition -- cell exhausted, see the approx twin above
                if (partitionTimestampHiExact < intervalLo) {
                    partitionLimit = 0;
                    advanceForwardCell();
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
                        // Whole partition consumed, so the CELL is exhausted -- but the INTERVAL is not.
                        // It may reach into the next day, so it stays live via runResume rather than
                        // being retired here.
                        partitionLimit = 0;
                        advanceForwardCell();
                    } else {
                        // Fragment: the interval's HIGH bound fell inside this cell, so this interval is
                        // finished FOR THIS CELL. 9A deleted the sibling special-case that used to sit
                        // here, along with the gate it carried: a sibling no longer depends on this cell
                        // declining to retire the interval, because it walks intervals from runIntervalLo
                        // itself.
                        partitionLimit = hi;
                        intervalsLo++;
                    }

                    return frame;
                }
                // Interval yielded an empty frame for this cell -- retire the interval FOR THIS CELL
                // only. Siblings walk intervals from runIntervalLo, so nothing is abandoned.
                partitionLimit = hi;
                intervalsLo++;
            } else {
                // Partition was empty, just skip to next. partitionLimit is deliberately NOT reset here,
                // matching the pre-9A walk exactly.
                advanceForwardCell();
            }
        }
        return null;
    }

    /**
     * Ends the current cell and moves to the next one, folding this cell's reached interval index into
     * the run's resume point.
     * <p>
     * {@code runResume} is the MINIMUM index any cell of the run reached, and becomes the global
     * {@code intervalsLo} once the run completes. The minimum, not the last cell's: an interval that
     * reaches past this day must stay live for the next one, and taking the last cell's index would
     * retire it early and silently drop its rows -- the exact defect class 9A exists to end.
     * <p>
     * Deliberately does NOT touch {@code partitionLimit}; callers set it, because the empty-partition
     * branch has to leave it alone to stay byte-identical with the pre-9A walk.
     */
    private void advanceForwardCell() {
        if (intervalsLo < runResume) {
            runResume = intervalsLo;
        }
        partitionLo++;
        // run complete -> publish the resume point; otherwise the next cell of the SAME day restarts at
        // this run's first interval
        intervalsLo = partitionLo >= runHi ? runResume : runIntervalLo;
    }

    @Override
    public void toTop() {
        super.toTop();
        partitionLimit = 0;
    }
}
