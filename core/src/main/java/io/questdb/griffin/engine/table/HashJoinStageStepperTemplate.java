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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.std.DirectLongList;

/**
 * The bytecode that {@link HashJoinStageStepper#newInstance()} defines a hidden class from, once per
 * stage of a parallel hash join. It must stay a top-level class without lambdas, nested classes or
 * static state, since each hidden copy shares nothing with this class but its bytecode; the
 * constants it reads compile into it.
 */
final class HashJoinStageStepperTemplate extends HashJoinStageStepper {

    @Override
    long countRows(AsyncHashJoinAtom.Slot slot, long rowCount, long from, boolean isDescending) {
        final PageFrameMemoryRecord probeRecord = slot.probeRecord;
        for (long i = from; i < rowCount; i++) {
            final long row = isDescending ? rowCount - 1 - i : i;
            probeRecord.setRowIndex(row);
            slot.probeRow = row;
            if (!step(slot, 0)) {
                return CANCELLED;
            }
        }
        return slot.pairCount;
    }

    @Override
    long countRows(AsyncHashJoinAtom.Slot slot, DirectLongList rows, long from, boolean isDescending) {
        final PageFrameMemoryRecord probeRecord = slot.probeRecord;
        final long n = rows.size();
        for (long p = from; p < n; p++) {
            final long row = rows.get(isDescending ? n - 1 - p : p);
            probeRecord.setRowIndex(row);
            slot.probeRow = row;
            if (!step(slot, 0)) {
                return CANCELLED;
            }
        }
        return slot.pairCount;
    }

    @Override
    long probeRows(AsyncHashJoinAtom.Slot slot, long rowCount, long from, long to, boolean isDescending, long tupleCap) {
        final PageFrameMemoryRecord probeRecord = slot.probeRecord;
        final DirectLongList tuples = slot.tuples;
        for (long i = from; i < to; i++) {
            final long row = isDescending ? rowCount - 1 - i : i;
            probeRecord.setRowIndex(row);
            slot.probeRow = row;
            if (!step(slot, 0)) {
                return CANCELLED;
            }
            if (tuples.size() >= tupleCap) {
                return i + 1;
            }
        }
        return to;
    }

    @Override
    long probeRows(AsyncHashJoinAtom.Slot slot, DirectLongList rows, long from, long to, boolean isDescending, long tupleCap) {
        final PageFrameMemoryRecord probeRecord = slot.probeRecord;
        final DirectLongList tuples = slot.tuples;
        final long n = rows.size();
        for (long p = from; p < to; p++) {
            final long row = rows.get(isDescending ? n - 1 - p : p);
            probeRecord.setRowIndex(row);
            slot.probeRow = row;
            if (!step(slot, 0)) {
                return CANCELLED;
            }
            if (tuples.size() >= tupleCap) {
                return p + 1;
            }
        }
        return to;
    }

    @Override
    boolean step(AsyncHashJoinAtom.Slot slot, int stage) {
        final AsyncHashJoinAtom.StageProbe s = slot.getStageProbe(stage);
        if (isEnumerated) {
            return enumerate(slot, stage, s);
        }
        final long entry;
        if (s.isBuildUnique) {
            // A build of one row per key positions its payload on the lookup itself.
            entry = findSingle(s) ? s.reader.locator() : NO_MATCH;
        } else {
            final FrozenHashJoinBuild.Probe probe = find(s);
            final long head = probe.chainHead();
            if (head == 0) {
                entry = NO_MATCH;
            } else {
                // The first match's locator serves a key of one row; a key of several rows hands
                // its chain to the consumer instead, or, in a count, the chain's length.
                probe.next();
                if (!probe.hasNext()) {
                    entry = s.reader.locator();
                } else if (slot.isCountOnly) {
                    return emitChain(slot, stage, probe);
                } else {
                    entry = -head;
                }
            }
        }
        if (entry != NO_MATCH || isOuter) {
            slot.entries[stage] = entry;
            return emit(slot, stage);
        }
        return true;
    }

    // Hands the probe row, with this stage's current entry, on to the next stage, or, at the last
    // stage, appends its tuple. Returns false when the execution was cancelled.
    private boolean emit(AsyncHashJoinAtom.Slot slot, int stage) {
        final HashJoinStageStepper next = this.next;
        if (next == null) {
            if (slot.isCountOnly) {
                // The tuple stands for as many rows as the product of its deferred chains' lengths.
                slot.pairCount += slot.pairMultiplier;
            } else {
                slot.appendTuple();
            }
            return true;
        }
        return next.step(slot, stage + 1);
    }

    // In a count, hands the probe row on with the rest of the key's chain counted, after the first
    // match that the probe already took: each tuple that comes out of the later stages stands for as
    // many more rows. Walks the chain, which leaves its links as they are. Returns false when the
    // execution was cancelled.
    private boolean emitChain(AsyncHashJoinAtom.Slot slot, int stage, FrozenHashJoinBuild.Probe probe) {
        long chainLength = 1;
        while (probe.hasNext()) {
            if (isInterrupted(slot)) {
                return false;
            }
            probe.next();
            chainLength++;
        }
        final long pairMultiplier = slot.pairMultiplier;
        slot.pairMultiplier = pairMultiplier * chainLength;
        final boolean isActive = emit(slot, stage);
        slot.pairMultiplier = pairMultiplier;
        return isActive;
    }

    // Walks the key's matches in chain order and hands on each pair that passes the stage's filters,
    // and, for a LEFT join's probe row whose pairs all fail the ON residual or that has no match, the
    // row without a match, if it passes the post-join filter.
    private boolean enumerate(AsyncHashJoinAtom.Slot slot, int stage, AsyncHashJoinAtom.StageProbe s) {
        boolean isMatched = false;
        if (s.isBuildUnique) {
            // The lookup positions the payload reader on the key's one row, if it finds one.
            if (findSingle(s)) {
                if (isInterrupted(slot)) {
                    return false;
                }
                final int pair = joinPair(slot, stage, s);
                if (pair == PAIR_CANCELLED) {
                    return false;
                }
                isMatched = pair == PAIR_MATCHED;
            }
        } else {
            final FrozenHashJoinBuild.Probe probe = find(s);
            while (probe.hasNext()) {
                if (isInterrupted(slot)) {
                    return false;
                }
                probe.next();
                final int pair = joinPair(slot, stage, s);
                if (pair == PAIR_CANCELLED) {
                    return false;
                }
                isMatched |= pair == PAIR_MATCHED;
            }
        }
        if (!isMatched && isOuter) {
            slot.entries[stage] = NO_MATCH;
            slot.record.setHasMatch(stage, false);
            final Function postFilter = s.postFilter;
            if ((postFilter == null || postFilter.getBool(slot.record)) && !emit(slot, stage)) {
                return false;
            }
        }
        return true;
    }

    // Looks the probe row's key up and returns the probe, which lists the key's matches.
    private FrozenHashJoinBuild.Probe find(AsyncHashJoinAtom.StageProbe s) {
        if (keyKind == KEY_STAGED) {
            s.recordProbe.findUnchecked(s.keyRecord);
            return s.recordProbe;
        }
        final int key = s.keySource.getInt(keyColumn);
        s.intProbe.findUnchecked(keyKind == KEY_SYMBOL ? s.symbolKeyView.translate(key) : key);
        return s.intProbe;
    }

    // Looks the probe row's key up in a build of one row per key, which positions the payload reader
    // on the key's row, and returns whether the build has one.
    private boolean findSingle(AsyncHashJoinAtom.StageProbe s) {
        if (keyKind == KEY_STAGED) {
            return s.recordProbe.findSingleUnchecked(s.keyRecord);
        }
        final int key = s.keySource.getInt(keyColumn);
        return s.intProbe.findSingleUnchecked(keyKind == KEY_SYMBOL ? s.symbolKeyView.translate(key) : key);
    }

    // Counts a pair and, every so many pairs, checks whether the execution was cancelled.
    private boolean isInterrupted(AsyncHashJoinAtom.Slot slot) {
        return --slot.pairsUntilCheck == 0 && slot.isInterrupted();
    }

    // Runs the stage's filters over the pair of the probe row and the match the payload reader is
    // positioned on, and hands the pair on if it passes both. Returns PAIR_MATCHED when the pair
    // passed the ON residual, which makes the probe row matched whether or not it passes the
    // post-join filter, PAIR_FAILED when it did not, and PAIR_CANCELLED when the execution was
    // cancelled while the later stages joined the pair.
    private int joinPair(AsyncHashJoinAtom.Slot slot, int stage, AsyncHashJoinAtom.StageProbe s) {
        slot.entries[stage] = s.reader.locator();
        slot.record.setHasMatch(stage, true);
        final Function onFilter = s.onFilter;
        if (onFilter != null && !onFilter.getBool(slot.record)) {
            return PAIR_FAILED;
        }
        final Function postFilter = s.postFilter;
        if ((postFilter == null || postFilter.getBool(slot.record)) && !emit(slot, stage)) {
            return PAIR_CANCELLED;
        }
        return PAIR_MATCHED;
    }
}
