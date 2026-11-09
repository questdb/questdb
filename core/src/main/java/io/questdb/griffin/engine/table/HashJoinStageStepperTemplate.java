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
    void probeRows(AsyncHashJoinAtom.Slot slot, long rowCount, boolean isDescending) {
        final PageFrameMemoryRecord probeRecord = slot.probeRecord;
        for (long i = 0; i < rowCount; i++) {
            final long row = isDescending ? rowCount - 1 - i : i;
            probeRecord.setRowIndex(row);
            slot.probeRow = row;
            step(slot, 0);
        }
    }

    @Override
    void probeRows(AsyncHashJoinAtom.Slot slot, DirectLongList rows, boolean isDescending) {
        final PageFrameMemoryRecord probeRecord = slot.probeRecord;
        for (long p = 0, n = rows.size(); p < n; p++) {
            final long row = rows.get(isDescending ? n - 1 - p : p);
            probeRecord.setRowIndex(row);
            slot.probeRow = row;
            step(slot, 0);
        }
    }

    @Override
    void step(AsyncHashJoinAtom.Slot slot, int stage) {
        final AsyncHashJoinAtom.StageProbe s = slot.getStageProbe(stage);
        final long entry;
        if (s.isBuildUnique) {
            // A build of one row per key positions its payload on the lookup itself.
            final boolean isFound;
            if (keyKind == KEY_STAGED) {
                isFound = s.recordProbe.findSingleUnchecked(s.keyRecord);
            } else if (keyKind == KEY_SYMBOL) {
                isFound = s.intProbe.findSingleUnchecked(s.symbolKeyView.translate(s.keySource.getInt(keyColumn)));
            } else {
                isFound = s.intProbe.findSingleUnchecked(s.keySource.getInt(keyColumn));
            }
            entry = isFound ? s.reader.locator() : NO_MATCH;
        } else {
            final FrozenHashJoinBuild.Probe probe;
            if (keyKind == KEY_STAGED) {
                s.recordProbe.findUnchecked(s.keyRecord);
                probe = s.recordProbe;
            } else {
                final int key = s.keySource.getInt(keyColumn);
                s.intProbe.findUnchecked(keyKind == KEY_SYMBOL ? s.symbolKeyView.translate(key) : key);
                probe = s.intProbe;
            }
            final long head = probe.chainHead();
            if (head == 0) {
                entry = NO_MATCH;
            } else {
                // The first match's locator serves a key of one row; a key of several rows hands
                // its chain to the consumer instead.
                probe.next();
                entry = probe.hasNext() ? -head : s.reader.locator();
            }
        }
        if (entry != NO_MATCH || isOuter) {
            slot.entries[stage] = entry;
            slot.appendTuple();
        }
    }
}
