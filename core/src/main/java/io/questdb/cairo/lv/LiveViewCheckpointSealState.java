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

import io.questdb.cairo.map.Map;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * One target's incremental-seal bookkeeping, held aside while a converging repair
 * runs the compiled factory's runtime through a replay that must not inherit it.
 * A target is the anchored {@link LiveViewWindow} or one
 * {@link io.questdb.griffin.engine.window.WindowFunction}.
 * <p>
 * The bookkeeping is three things that only mean anything together: the dirty set
 * naming the keys touched since the last durable root, the logical byte figure that
 * root charges, and the baseline generation both are relative to. A target that put
 * the baseline back without the set would publish a root missing the keys the set
 * named - which no reader detects and a restart fails on - so
 * {@link LiveViewCheckpointSealCarryover} moves all three or none.
 * <p>
 * The dirty set travels as the map itself rather than as a copy of its keys. The
 * repair's wipe would clear it, so the detach takes the reference out of the target
 * first and the attach hands the same map back, which makes the carry O(1) whatever
 * the cadence touched. Nothing reads the map in between - it is the target's own
 * scratch - and the state the {@link LiveViewCheckpointScratchOverlay} restores
 * beside it is the state its keys were named against, entry for entry.
 * <p>
 * Instances are pooled by the carryover and reused across repairs; a held state
 * owns the map it holds until an attach takes it back or {@link #clear()} frees it.
 */
public final class LiveViewCheckpointSealState implements QuietCloseable {
    private Map dirtySet;
    private boolean hasDirtyTracking;
    private boolean hasEvictionsRecorded;
    private boolean held;
    private long logicalStateBytes;

    /**
     * Drops whatever this slot holds, freeing a dirty set no attach took back.
     */
    public void clear() {
        dirtySet = Misc.free(dirtySet);
        hasDirtyTracking = false;
        hasEvictionsRecorded = false;
        logicalStateBytes = 0;
        held = false;
    }

    @Override
    public void close() {
        clear();
    }

    /**
     * @return what the baseline root charges for this target's state
     */
    public long getLogicalStateBytes() {
        return logicalStateBytes;
    }

    /**
     * @return whether the target was tracking dirty keys at all when it was detached.
     * A target that tracks none full-scans every seal, and the attach must not tell it
     * otherwise
     */
    public boolean hasDirtyTracking() {
        return hasDirtyTracking;
    }

    /**
     * @return whether the parked dirty set carries the frontier sweep's eviction
     * markers, which decides whether emptying it also hands its backing memory back
     */
    public boolean hasEvictionsRecorded() {
        return hasEvictionsRecorded;
    }

    /**
     * @return true once a detach has filled this slot and no attach has taken it back
     */
    public boolean isHeld() {
        return held;
    }

    /**
     * Records one target's bookkeeping. Called by the target itself, which owns the
     * meaning of every field; the slot is a carrier and validates none of it.
     */
    public void of(@Nullable Map dirtySet, boolean hasDirtyTracking, boolean hasEvictionsRecorded, long logicalStateBytes) {
        this.dirtySet = dirtySet;
        this.hasDirtyTracking = hasDirtyTracking;
        this.hasEvictionsRecorded = hasEvictionsRecorded;
        this.logicalStateBytes = logicalStateBytes;
        this.held = true;
    }

    /**
     * Hands the parked dirty set to the target taking its bookkeeping back, and
     * releases this slot's claim on it so a later {@link #clear()} cannot free a map
     * the target now owns.
     */
    public @Nullable Map takeDirtySet() {
        final Map taken = dirtySet;
        dirtySet = null;
        held = false;
        return taken;
    }
}
