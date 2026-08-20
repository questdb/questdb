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

import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Where a multi-segment out-of-order repair had got to when one of its segments parked on
 * the refresh turn's budget.
 * <p>
 * Two loops repair anchor segments one at a time: the inline per-segment repair, which
 * walks the closed segments a change set decomposed into, and the backfill pass, which
 * drains the durable pending-repair set oldest first. Both own <b>one</b> pinned base
 * snapshot across every segment they take, and the snapshot cannot be reopened - QuestDB
 * exposes no as-of reader - so a segment replay that stops half-way has to hand the
 * snapshot to the repair that continues it, and the rest of the loop has to travel with
 * it. That is what this carries: everything the resuming turn needs to finish the parked
 * segment's successors without re-planning them, re-reading the base range they came from,
 * or repairing the segments the loop already published a second time.
 * <p>
 * It holds no resource. The pinned reader, the uncommitted replacement and the staged root
 * versions belong to {@link LiveViewCheckpointRepairSession}, which owns one of these for
 * the same reason it owns those: the loop position is meaningless without the snapshot the
 * remaining segments are planned against.
 *
 * <h2>What each loop puts in it</h2>
 * The change-set loop queues the segments it has not reached, plus the residual bounds it
 * still owes the ordinary plan once they are done - the residual is the correction the
 * runtime is still standing in, and a turn that repaired the closed segments and dropped
 * it would leave the change unconsumed and re-repair every one of them on the next drain.
 * <p>
 * The backfill pass queues nothing: its work list is the durable pending set, which
 * outlives any number of turns. What it records instead is the segment in flight, so the
 * resuming turn clears exactly the entry whose replacement applied and no other, and the
 * moment the pass began, so a pass whose first segment took several turns is still bounded
 * by {@code cairo.live.view.checkpoint.backfill.max.duration} rather than starting its
 * budget over.
 */
public final class LiveViewCheckpointSegmentLoop {
    /**
     * The backfill pass draining the durable pending-repair set.
     */
    public static final int KIND_BACKFILL_PASS = 2;
    /**
     * The inline loop over the closed anchor segments one change set decomposed into.
     */
    public static final int KIND_CHANGE_SET = 1;
    /**
     * No loop: a repair that stands on its own, which is every union-range repair and the
     * residual of a decomposed one.
     */
    public static final int KIND_NONE = 0;
    // segmentStart, minTs, maxTs per queued entry, oldest first - the order a repair has
    // to take them in, because a later segment's cumulative row positions depend on how
    // many rows the earlier ones added.
    private static final int STRIDE = 3;
    private final LongList segments = new LongList();
    private long durableOutputMaxTs = Numbers.LONG_NULL;
    private long finalSeqTxn = Numbers.LONG_NULL;
    private long holdSeqTxn = Numbers.LONG_NULL;
    private long inFlightSegmentStart = Numbers.LONG_NULL;
    private int kind = KIND_NONE;
    private long passStartUs;
    private long residualAdvanceTo = Numbers.LONG_NULL;
    private boolean residualInsertOnly;
    private long residualMaxTs = Numbers.LONG_NULL;
    private long residualMinTs = Numbers.LONG_NULL;
    private long runtimeFrontierTs = Numbers.LONG_NULL;
    private int segmentsRepaired;
    private long viewLowerBoundTimestamp;

    /**
     * Queues one segment the loop has not reached yet.
     */
    public void addSegment(long segmentStart, long minTs, long maxTs) {
        segments.add(segmentStart);
        segments.add(minTs);
        segments.add(maxTs);
    }

    public void clear() {
        segments.clear();
        kind = KIND_NONE;
        inFlightSegmentStart = Numbers.LONG_NULL;
        viewLowerBoundTimestamp = 0;
        holdSeqTxn = Numbers.LONG_NULL;
        finalSeqTxn = Numbers.LONG_NULL;
        durableOutputMaxTs = Numbers.LONG_NULL;
        runtimeFrontierTs = Numbers.LONG_NULL;
        residualMinTs = Numbers.LONG_NULL;
        residualMaxTs = Numbers.LONG_NULL;
        residualAdvanceTo = Numbers.LONG_NULL;
        residualInsertOnly = false;
        passStartUs = 0;
        segmentsRepaired = 0;
    }

    public void copyFrom(@NotNull LiveViewCheckpointSegmentLoop src) {
        segments.clear();
        segments.addAll(src.segments);
        kind = src.kind;
        inFlightSegmentStart = src.inFlightSegmentStart;
        viewLowerBoundTimestamp = src.viewLowerBoundTimestamp;
        holdSeqTxn = src.holdSeqTxn;
        finalSeqTxn = src.finalSeqTxn;
        durableOutputMaxTs = src.durableOutputMaxTs;
        runtimeFrontierTs = src.runtimeFrontierTs;
        residualMinTs = src.residualMinTs;
        residualMaxTs = src.residualMaxTs;
        residualAdvanceTo = src.residualAdvanceTo;
        residualInsertOnly = src.residualInsertOnly;
        passStartUs = src.passStartUs;
        segmentsRepaired = src.segmentsRepaired;
    }

    /**
     * @return the live-view table's highest durable output timestamp as the loop's first
     * turn read it. Every segment plan is derived against this one value, so a
     * continuation quotes it rather than re-reading a table its own replacements have
     * since rewritten
     */
    public long getDurableOutputMaxTs() {
        return durableOutputMaxTs;
    }

    /**
     * @return the {@code seqTxn} the <b>last</b> queued segment commits at, or
     * {@link Numbers#LONG_NULL} when no segment of this loop may advance the watermark.
     * Only the repair that finishes the whole change set advances it; a loop that still
     * owes a residual, and every backfill pass, leaves the watermark where it is
     */
    public long getFinalSeqTxn() {
        return finalSeqTxn;
    }

    /**
     * @return the {@code seqTxn} every segment but the last commits at - the view's own
     * pre-repair watermark, which keeps the change unconsumed until the loop finishes it
     */
    public long getHoldSeqTxn() {
        return holdSeqTxn;
    }

    /**
     * @return the inclusive start of the segment whose replay parked, or
     * {@link Numbers#LONG_NULL} when the loop parked between segments. The backfill pass
     * reads it to clear exactly the pending entry whose replacement applied
     */
    public long getInFlightSegmentStart() {
        return inFlightSegmentStart;
    }

    /**
     * @return which loop parked: {@link #KIND_CHANGE_SET}, {@link #KIND_BACKFILL_PASS}, or
     * {@link #KIND_NONE} for a repair that is not part of one
     */
    public int getKind() {
        return kind;
    }

    /**
     * @return the microsecond clock reading the backfill pass started at, so the pass's
     * duration budget bounds the pass rather than restarting with every turn it takes
     */
    public long getPassStartUs() {
        return passStartUs;
    }

    /**
     * @return the base {@code seqTxn} the residual repair must cover and commit at
     */
    public long getResidualAdvanceTo() {
        return residualAdvanceTo;
    }

    /**
     * @return the highest timestamp the change set touched at or above the active
     * segment's start, or {@link Numbers#LONG_NULL} when the closed segments were the
     * whole of it
     */
    public long getResidualMaxTs() {
        return residualMaxTs;
    }

    /**
     * @return the lowest timestamp the change set touched at or above the active segment's
     * start, or {@link Numbers#LONG_NULL} when the closed segments were the whole of it
     */
    public long getResidualMinTs() {
        return residualMinTs;
    }

    /**
     * @return the view's own {@code latestSeenTs} as the loop's first turn read it, which
     * is the frontier every segment plan proves it converges below
     */
    public long getRuntimeFrontierTs() {
        return runtimeFrontierTs;
    }

    public long getSegmentMaxTs(int index) {
        return segments.getQuick(index * STRIDE + 2);
    }

    public long getSegmentMinTs(int index) {
        return segments.getQuick(index * STRIDE + 1);
    }

    public long getSegmentStart(int index) {
        return segments.getQuick(index * STRIDE);
    }

    /**
     * @return how many segments this loop has already repaired and published across every
     * turn it has taken. Diagnostic, and what a resumed backfill pass charges its duration
     * budget against
     */
    public int getSegmentsRepaired() {
        return segmentsRepaired;
    }

    /**
     * @return the view's {@code START FROM} boundary, the floor every segment plan clamps
     * to
     */
    public long getViewLowerBoundTimestamp() {
        return viewLowerBoundTimestamp;
    }

    /**
     * @return whether every commit the change set covers only ADDED base rows, which is
     * what lets the residual plan a bounded repair
     */
    public boolean isResidualInsertOnly() {
        return residualInsertOnly;
    }

    /**
     * Opens the loop for the backfill pass. The pass queues no segment - the durable
     * pending set is its work list, and it outlives any number of turns.
     */
    public void ofBackfillPass(
            long viewLowerBoundTimestamp,
            long commitSeqTxn,
            long durableOutputMaxTs,
            long runtimeFrontierTs,
            long passStartUs
    ) {
        clear();
        this.kind = KIND_BACKFILL_PASS;
        this.viewLowerBoundTimestamp = viewLowerBoundTimestamp;
        this.holdSeqTxn = commitSeqTxn;
        this.durableOutputMaxTs = durableOutputMaxTs;
        this.runtimeFrontierTs = runtimeFrontierTs;
        this.passStartUs = passStartUs;
    }

    /**
     * Opens the loop for the inline per-segment repair. {@code finalSeqTxn} is the pinned
     * snapshot's own {@code seqTxn} when the closed segments are the whole change set -
     * the last of them then advances the watermark over it - and
     * {@link Numbers#LONG_NULL} when a residual is still owed.
     */
    public void ofChangeSet(
            long viewLowerBoundTimestamp,
            long preRepairSeqTxn,
            long finalSeqTxn,
            long durableOutputMaxTs,
            long runtimeFrontierTs,
            long residualMinTs,
            long residualMaxTs,
            boolean residualInsertOnly,
            long residualAdvanceTo
    ) {
        clear();
        this.kind = KIND_CHANGE_SET;
        this.viewLowerBoundTimestamp = viewLowerBoundTimestamp;
        this.holdSeqTxn = preRepairSeqTxn;
        this.finalSeqTxn = finalSeqTxn;
        this.durableOutputMaxTs = durableOutputMaxTs;
        this.runtimeFrontierTs = runtimeFrontierTs;
        this.residualMinTs = residualMinTs;
        this.residualMaxTs = residualMaxTs;
        this.residualInsertOnly = residualInsertOnly;
        this.residualAdvanceTo = residualAdvanceTo;
    }

    /**
     * Drops the segment the loop is about to repair off the head of the queue and names it
     * as the one in flight, so a replay that parks inside it leaves the queue holding
     * exactly its successors.
     */
    public void removeFirstSegment() {
        if (segments.size() == 0) {
            inFlightSegmentStart = Numbers.LONG_NULL;
            return;
        }
        inFlightSegmentStart = segments.getQuick(0);
        segments.removeIndexBlock(0, STRIDE);
    }

    /**
     * Records that the segment in flight published, which is the moment its pending entry
     * may be cleared and the loop may move on.
     */
    public void segmentRepaired() {
        inFlightSegmentStart = Numbers.LONG_NULL;
        segmentsRepaired++;
    }

    /**
     * Names the segment the backfill pass is repairing. The pass takes its work off the
     * durable set rather than off the queue, so it has nothing to remove and only the
     * in-flight identity to record.
     */
    public void segmentStarted(long segmentStart) {
        inFlightSegmentStart = segmentStart;
    }

    /**
     * @return how many segments the loop has queued and not yet repaired
     */
    public int size() {
        return segments.size() / STRIDE;
    }
}
