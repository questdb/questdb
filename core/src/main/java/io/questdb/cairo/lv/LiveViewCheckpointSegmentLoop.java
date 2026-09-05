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

import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Where a multi-segment out-of-order repair had got to when one of its segments parked on
 * the refresh turn's budget.
 * <p>
 * The per-segment repair walks the closed segments a change set decomposed into, one at a
 * time, and owns <b>one</b> pinned base snapshot across every segment it takes. The
 * snapshot cannot be reopened - QuestDB exposes no as-of reader - so a segment replay that
 * stops half-way has to hand the snapshot to the repair that continues it, and the rest of
 * the loop has to travel with it. That is what this carries: everything the resuming turn
 * needs to finish the parked segment's successors without re-planning them, re-reading the
 * base range they came from, or repairing the segments the loop already published a second
 * time.
 * <p>
 * It holds no resource. The pinned reader, the uncommitted replacement and the staged root
 * versions belong to {@link LiveViewCheckpointRepairSession}, which owns one of these for
 * the same reason it owns those: the loop position is meaningless without the snapshot the
 * remaining segments are planned against.
 *
 * <h2>What the loop puts in it</h2>
 * The loop queues the segments it has not reached, plus the residual bounds it still owes
 * the ordinary plan once they are done - the residual is the correction the
 * runtime is still standing in, and a turn that repaired the closed segments and dropped
 * it would leave the change unconsumed and re-repair every one of them on the next drain.
 * <p>
 * It also queues each segment's <b>affected key domain</b> {@code Q} - the keys its
 * corrections carried - for the segments the cost model priced a keyed read cheaper on.
 * {@code Q} is collected by the change-set decomposition, and that scratch belongs to the
 * turn that classified it: by the time a later turn resumes the loop the same worker has
 * refilled it for whatever it classified since. A loop that carried only the timestamps
 * would therefore repair every segment behind a parked one whole, which is not a corner -
 * the loop parks on the segments a keyed replay is <i>not</i> taken for, precisely because
 * a keyed replay never parks, so the segments behind a park are the ones most likely to be
 * keyed. {@code Q} is a per-segment coordinate like the bounds beside it, and it travels
 * for the same reason they do.
 */
public final class LiveViewCheckpointSegmentLoop {
    // segmentStart, minTs, maxTs, keySetIndex, hasNullKey per queued entry, oldest first -
    // the order a repair has to take them in, because a later segment's cumulative row
    // positions depend on how many rows the earlier ones added. keySetIndex is -1 for a
    // segment carrying no key domain, which is every segment the cost model turned down.
    private static final int STRIDE = 5;
    // Q per queued segment, named by an index the entry carries rather than by the entry's
    // position: the queue drains from its head, and an index that travels with the entry is
    // one no removal has to move. Retained across repairs and cleared rather than dropped,
    // so a worker pays for the growth once.
    private final ObjList<CharSequenceHashSet> keySets = new ObjList<>();
    private final LongList segments = new LongList();
    private long durableOutputMaxTs = Numbers.LONG_NULL;
    private long finalSeqTxn = Numbers.LONG_NULL;
    private boolean hasInFlightNullKey;
    private long holdSeqTxn = Numbers.LONG_NULL;
    private int inFlightKeySetIndex = -1;
    private long inFlightSegmentStart = Numbers.LONG_NULL;
    private boolean isOpen;
    private long residualAdvanceTo = Numbers.LONG_NULL;
    private boolean residualInsertOnly;
    private long residualMaxTs = Numbers.LONG_NULL;
    private long residualMinTs = Numbers.LONG_NULL;
    private long runtimeFrontierTs = Numbers.LONG_NULL;
    private int segmentsRepaired;
    private long viewLowerBoundTimestamp;

    /**
     * Queues one segment the loop has not reached yet, with the key domain its repair may
     * follow.
     *
     * @param keys       the segment's affected keys, or null when its repair must read every
     *                   row of it. Copied rather than referenced: the change set these come
     *                   from is refilled by the next repair this worker classifies, and the
     *                   loop may outlive that by any number of turns
     * @param hasNullKey whether a correction carried the null partition key, which the
     *                   change set holds beside its set rather than in it
     */
    public void addSegment(
            long segmentStart,
            long minTs,
            long maxTs,
            @Nullable CharSequenceHashSet keys,
            boolean hasNullKey
    ) {
        int keySetIndex = -1;
        if (keys != null) {
            keySetIndex = segments.size() / STRIDE;
            final CharSequenceHashSet carried = keySetAt(keySetIndex);
            carried.clear();
            // The values are Strings by the time the change set holds them, so this is a
            // copy of references rather than of characters.
            carried.addAll(keys);
        }
        segments.add(segmentStart);
        segments.add(minTs);
        segments.add(maxTs);
        segments.add(keySetIndex);
        segments.add(keys != null && hasNullKey ? 1 : 0);
    }

    public void clear() {
        segments.clear();
        for (int i = 0, n = keySets.size(); i < n; i++) {
            keySets.getQuick(i).clear();
        }
        isOpen = false;
        inFlightKeySetIndex = -1;
        hasInFlightNullKey = false;
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
        segmentsRepaired = 0;
    }

    public void copyFrom(@NotNull LiveViewCheckpointSegmentLoop src) {
        segments.clear();
        segments.addAll(src.segments);
        for (int i = 0, n = keySets.size(); i < n; i++) {
            keySets.getQuick(i).clear();
        }
        // Index for index, because that is how an entry names its set - and the entry of
        // the segment in flight has been removed from the queue while its set is still
        // named by the index it was added at.
        for (int i = 0, n = src.keySets.size(); i < n; i++) {
            keySetAt(i).addAll(src.keySets.getQuick(i));
        }
        isOpen = src.isOpen;
        inFlightKeySetIndex = src.inFlightKeySetIndex;
        hasInFlightNullKey = src.hasInFlightNullKey;
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
     * owes a residual leaves the watermark where it is
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
     * @return the affected keys of the segment the loop is repairing, or null when that
     * segment has none and its repair must read every row of it. A non-null set is the
     * verdict as well as the domain: the loop carries {@code Q} only for the segments the
     * cost model priced a keyed read cheaper on
     */
    public @Nullable CharSequenceHashSet getInFlightKeys() {
        return inFlightKeySetIndex < 0 ? null : keySets.getQuick(inFlightKeySetIndex);
    }

    /**
     * @return the inclusive start of the segment whose replay parked, or
     * {@link Numbers#LONG_NULL} when the loop parked between segments
     */
    public long getInFlightSegmentStart() {
        return inFlightSegmentStart;
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
     * turn it has taken
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
     * @return whether a correction of the segment the loop is repairing carried the null
     * partition key. Meaningless unless {@link #getInFlightKeys()} is non-null
     */
    public boolean hasInFlightNullKey() {
        return hasInFlightNullKey;
    }

    /**
     * @return whether every commit the change set covers only ADDED base rows, which is
     * what lets the residual plan a bounded repair
     */
    public boolean isResidualInsertOnly() {
        return residualInsertOnly;
    }

    /**
     * @return whether this position belongs to a segment loop at all. False for a repair
     * that stands on its own, which is every union-range repair and the residual of a
     * decomposed one.
     */
    public boolean isOpen() {
        return isOpen;
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
        this.isOpen = true;
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
            inFlightKeySetIndex = -1;
            hasInFlightNullKey = false;
            return;
        }
        inFlightSegmentStart = segments.getQuick(0);
        // The set stays where it is and the in-flight slot names it by the same index the
        // entry did, so dropping the entry costs no copy.
        inFlightKeySetIndex = (int) segments.getQuick(3);
        hasInFlightNullKey = segments.getQuick(4) != 0;
        segments.removeIndexBlock(0, STRIDE);
    }

    /**
     * Records that the segment in flight published, which is the moment the loop may move
     * on to the next one.
     */
    public void segmentRepaired() {
        inFlightSegmentStart = Numbers.LONG_NULL;
        if (inFlightKeySetIndex > -1) {
            keySets.getQuick(inFlightKeySetIndex).clear();
            inFlightKeySetIndex = -1;
        }
        hasInFlightNullKey = false;
        segmentsRepaired++;
    }

    /**
     * @return how many segments the loop has queued and not yet repaired
     */
    public int size() {
        return segments.size() / STRIDE;
    }

    /**
     * The pooled key set at {@code index}, growing the pool one set at a time rather than
     * extending straight to the index asked for.
     * <p>
     * The distinction matters: a loop skips the pool for every segment it carries no key
     * domain for, so the indexes it does ask for have gaps in them, and an extend-and-set
     * would leave nulls in the ones it stepped over. Everything that walks the pool -
     * {@link #clear()}, {@link #copyFrom} - walks all of it.
     */
    private @NotNull CharSequenceHashSet keySetAt(int index) {
        while (keySets.size() <= index) {
            keySets.add(new CharSequenceHashSet());
        }
        return keySets.getQuick(index);
    }
}
