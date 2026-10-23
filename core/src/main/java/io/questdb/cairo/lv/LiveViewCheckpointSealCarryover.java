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

import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Preserves incremental-seal bookkeeping across a converging repair's runtime
 * exchange. Baseline generation and dirty sets must move together; restoring only
 * the baseline after a wipe would silently omit dirty keys from the next root.
 * <p>
 * The caller may restamp the baseline only when the splice preserves the newest
 * root's payload at or above H. Otherwise targets retain the complete-freeze
 * requirement established by restore. An isolated runtime needs the same restamp
 * even though it leaves the primary's state untouched.
 * <p>
 * Capture before head retirement resets minSeenTsSinceCheckpoint and before any
 * runtime wipe. Restore after the overlay restores state and before the closing
 * seal. Carry the minimum too: replay lowers it regardless of which runtime folds
 * its rows. One reusable instance belongs to each repair session.
 */
public final class LiveViewCheckpointSealCarryover implements QuietCloseable {
    // Slot 0 is the anchored window's, slots 1..n each function's, in factory order.
    private final ObjList<LiveViewCheckpointSealState> states = new ObjList<>();
    private boolean hasAnchorWindow;
    private boolean isCaptured;
    // The batch-minimum window the pre-repair runtime stood in. The replay lowers it to
    // the bottom of the range it reads and the restored runtime holds none of those rows,
    // so it has to go back up - which the monotone setLatestSeenTs cannot express.
    private long minSeenTsSinceCheckpoint = Long.MAX_VALUE;
    private int targetCount;

    /**
     * Takes every target's bookkeeping out of the compiled factory. A target already
     * owing a complete freeze contributes nothing and keeps owing it; every other one
     * is left owing one for as long as the carryover holds its bookkeeping, so a repair
     * that unwinds anywhere between here and {@link #restore} leaves a safe runtime
     * rather than a baseline with no dirty set behind it.
     *
     * @param minSeenTsSinceCheckpoint the instance's batch-minimum window, read before
     *                                 the repair's retire resets it
     */
    public void capture(
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            long minSeenTsSinceCheckpoint
    ) {
        clear();
        this.minSeenTsSinceCheckpoint = minSeenTsSinceCheckpoint;
        this.hasAnchorWindow = anchorWindow != null;
        this.targetCount = functions.size() + 1;
        while (states.size() < targetCount) {
            states.add(new LiveViewCheckpointSealState());
        }
        if (anchorWindow != null) {
            anchorWindow.detachCheckpointSealState(states.getQuick(0));
        }
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).detachCheckpointSealState(states.getQuick(i + 1));
        }
        isCaptured = true;
    }

    /**
     * Drops everything the carryover holds, freeing any dirty set no target took back.
     */
    public void clear() {
        for (int i = 0, n = states.size(); i < n; i++) {
            states.getQuick(i).clear();
        }
        isCaptured = false;
        hasAnchorWindow = false;
        minSeenTsSinceCheckpoint = Long.MAX_VALUE;
        targetCount = 0;
    }

    @Override
    public void close() {
        Misc.freeObjList(states);
        states.clear();
        isCaptured = false;
        hasAnchorWindow = false;
        minSeenTsSinceCheckpoint = Long.MAX_VALUE;
        targetCount = 0;
    }

    public boolean isCaptured() {
        return isCaptured;
    }

    /**
     * Hands every held target its bookkeeping back, re-stamped against {@code generation}.
     * <p>
     * {@code generation} is {@link Numbers#LONG_NULL} when the repair published nothing the
     * baseline could name - the splice failed, or its newest root is one the splice
     * re-versioned - and then the bookkeeping is dropped rather than restored: the targets
     * are already on the complete scan the wipe left them on, which is the safe direction.
     * The same applies to a runtime whose shape moved under the carryover, which is a
     * recompiled factory rather than a state to restore.
     *
     * @param instance   the view whose batch-minimum window travels with the state
     * @param generation the generation the repair's splice published, or
     *                   {@link Numbers#LONG_NULL} to drop the bookkeeping
     */
    public void restore(
            @NotNull LiveViewInstance instance,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            long generation
    ) {
        if (!isCaptured) {
            return;
        }
        if (generation == Numbers.LONG_NULL
                || functions.size() + 1 != targetCount
                || (anchorWindow != null) != hasAnchorWindow) {
            clear();
            return;
        }
        if (anchorWindow != null) {
            final LiveViewCheckpointSealState state = states.getQuick(0);
            if (state.isHeld()) {
                anchorWindow.attachCheckpointSealState(state, generation);
            }
        }
        for (int i = 0, n = functions.size(); i < n; i++) {
            final LiveViewCheckpointSealState state = states.getQuick(i + 1);
            if (state.isHeld()) {
                functions.getQuick(i).attachCheckpointSealState(state, generation);
            }
        }
        // Last, and only on the path that restored the sets those keys were named in:
        // the value describes the same batch the dirty sets do, and a seal reading one
        // without the other prices the wrong window.
        instance.forceSetMinSeenTsSinceCheckpoint(minSeenTsSinceCheckpoint);
        clear();
    }
}
