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

package io.questdb.cairo.lv;

/**
 * A mutable value holder for one {@code LogicalCheckpointEntry} (design section
 * 7): the permanent identity and per-generation leaf values of a logical
 * checkpoint boundary.
 * <p>
 * The type is a reusable flyweight, like {@link LiveViewCheckpointPageRef}:
 * callers pass one instance into an append or reuse one across reader results to
 * stay allocation-free on the checkpoint path. The tree key is
 * {@code (maxTimestamp, checkpointId)}.
 */
public final class LiveViewCheckpointTimelineEntry {

    /**
     * Runtime output-row position observed at checkpoint time, minus the
     * generation's prefix delta correction. The effective position is this value
     * plus the row-position delta index prefix sum (Phase 1 step 3); this class
     * stores only the raw per-root component.
     */
    public long baseLvRowPosition;
    /**
     * Monotonic id disambiguating two cadence events with the same
     * {@link #maxTimestamp}. The low half of the search key.
     */
    public long checkpointId;
    /**
     * Diagnostic live-view-writer {@code seqTxn} at which the boundary was first
     * created (design section 7). Not a recovery watermark.
     */
    public long createdLvSeqTxn;
    /**
     * Logical (decoded) state byte size attributed to this root version.
     */
    public long logicalStateBytes;
    /**
     * Inclusive maximum designated timestamp the boundary represents. The high
     * half of the search key.
     */
    public long maxTimestamp;
    /**
     * Reference to the checkpoint root metadata page for this entry's current
     * root version. May be null (e.g. a placeholder before a root is attached).
     */
    public final LiveViewCheckpointPageRef rootRef = new LiveViewCheckpointPageRef();

    /**
     * Resets every field to its default; the root reference becomes null.
     */
    public LiveViewCheckpointTimelineEntry clear() {
        maxTimestamp = 0;
        checkpointId = 0;
        createdLvSeqTxn = 0;
        baseLvRowPosition = 0;
        logicalStateBytes = 0;
        rootRef.clear();
        return this;
    }

    /**
     * Deep-copies every field from {@code src}, including the root reference.
     */
    public LiveViewCheckpointTimelineEntry copyFrom(LiveViewCheckpointTimelineEntry src) {
        maxTimestamp = src.maxTimestamp;
        checkpointId = src.checkpointId;
        createdLvSeqTxn = src.createdLvSeqTxn;
        baseLvRowPosition = src.baseLvRowPosition;
        logicalStateBytes = src.logicalStateBytes;
        rootRef.of(src.rootRef.getSegmentId(), src.rootRef.getOffset(), src.rootRef.getLength());
        return this;
    }

    /**
     * Sets the search-key fields and the leaf values in one call. The root
     * reference is set separately via {@link #rootRef}.
     */
    public LiveViewCheckpointTimelineEntry of(
            long maxTimestamp,
            long checkpointId,
            long createdLvSeqTxn,
            long baseLvRowPosition,
            long logicalStateBytes
    ) {
        this.maxTimestamp = maxTimestamp;
        this.checkpointId = checkpointId;
        this.createdLvSeqTxn = createdLvSeqTxn;
        this.baseLvRowPosition = baseLvRowPosition;
        this.logicalStateBytes = logicalStateBytes;
        return this;
    }
}
