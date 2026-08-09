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

import io.questdb.cairo.CairoException;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * A reader's handle on one published timeline generation. It is the concrete
 * realization of "a reader pins one generation before resolving any root or
 * page reference": while a pin is held, the generation's superblock roots stay
 * resolvable through this handle and the files it references are protected from
 * garbage collection, even if a concurrent publication has already advanced the
 * current generation.
 * <p>
 * A pin captures, at pin time, the generation number and a snapshot of that
 * generation's three superblock root references ({@code timelineRootRef},
 * {@code rowPositionDeltaRootRef}, {@code segmentDirectoryRootRef}). The snapshot
 * is immutable for the pin's lifetime: a later {@code setCurrentGeneration} on the
 * owning {@link LiveViewCheckpointGenerationTracker} does not disturb a live pin,
 * so a reader that pinned generation {@code G} keeps reading {@code G}'s roots
 * after generation {@code G + 1} is published.
 * <p>
 * Pins are pooled and reused by their {@link LiveViewCheckpointGenerationTracker}
 * to stay allocation-free on the read path; only the tracker constructs, arms, and
 * disarms them. {@link #close()} releases the pin back to its owner and is
 * idempotent - a double close is a no-op. Every accessor other than
 * {@link #isPinned()} requires a held pin.
 */
public final class LiveViewCheckpointGenerationPin implements QuietCloseable {

    private final LiveViewCheckpointPageRef rowPositionDeltaRootRef = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointPageRef segmentDirectoryRootRef = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointPageRef timelineRootRef = new LiveViewCheckpointPageRef();
    private long coveredLvSeqTxn = -1;
    private long generation = LiveViewCheckpointGenerationTracker.NO_GENERATION;
    private long normalizedBaseSeqTxn = -1;
    private LiveViewCheckpointGenerationTracker owner;
    private boolean pinned;

    // Only the tracker constructs pins; the pool hands them back through arm().
    LiveViewCheckpointGenerationPin() {
    }

    /**
     * Releases the pin back to its owning tracker. Idempotent: a second close (or a
     * close of a pin the tracker already reclaimed) does nothing.
     */
    @Override
    public void close() {
        if (!pinned) {
            return;
        }
        final LiveViewCheckpointGenerationTracker o = owner;
        final long g = generation;
        final long baseSeqTxn = normalizedBaseSeqTxn;
        // Clear pinned before releasing so a re-entrant close observes the pin as
        // already released and returns without a double decrement.
        pinned = false;
        o.release(g, baseSeqTxn, this);
    }

    /**
     * @return the generation this pin holds. Requires a held pin.
     */
    public long getGeneration() {
        ensurePinned();
        return generation;
    }

    /**
     * @return the pinned generation's live-view-writer transaction watermark
     */
    public long getCoveredLvSeqTxn() {
        ensurePinned();
        return coveredLvSeqTxn;
    }

    /**
     * @return the pinned generation's authoritative base-table transaction
     * inclusion boundary
     */
    public long getNormalizedBaseSeqTxn() {
        ensurePinned();
        return normalizedBaseSeqTxn;
    }

    /**
     * @return the pinned generation's {@code rowPositionDeltaRootRef} snapshot,
     * immutable for the pin's lifetime. Requires a held pin.
     */
    public LiveViewCheckpointPageRef getRowPositionDeltaRootRef() {
        ensurePinned();
        return rowPositionDeltaRootRef;
    }

    /**
     * @return the pinned generation's {@code segmentDirectoryRootRef} snapshot,
     * immutable for the pin's lifetime. Requires a held pin.
     */
    public LiveViewCheckpointPageRef getSegmentDirectoryRootRef() {
        ensurePinned();
        return segmentDirectoryRootRef;
    }

    /**
     * @return the pinned generation's {@code timelineRootRef} snapshot, immutable
     * for the pin's lifetime. Requires a held pin.
     */
    public LiveViewCheckpointPageRef getTimelineRootRef() {
        ensurePinned();
        return timelineRootRef;
    }

    /**
     * @return true while this pin is held (armed and not yet closed)
     */
    public boolean isPinned() {
        return pinned;
    }

    /**
     * Arms this pooled pin against {@code generation}, snapshotting the generation's
     * three root references. Called by the tracker under its monitor.
     */
    void arm(
            @NotNull LiveViewCheckpointGenerationTracker owner,
            long generation,
            long normalizedBaseSeqTxn,
            long coveredLvSeqTxn,
            @NotNull LiveViewCheckpointPageRef timelineRootRef,
            @NotNull LiveViewCheckpointPageRef rowPositionDeltaRootRef,
            @NotNull LiveViewCheckpointPageRef segmentDirectoryRootRef
    ) {
        this.owner = owner;
        this.generation = generation;
        this.normalizedBaseSeqTxn = normalizedBaseSeqTxn;
        this.coveredLvSeqTxn = coveredLvSeqTxn;
        copyRef(this.timelineRootRef, timelineRootRef);
        copyRef(this.rowPositionDeltaRootRef, rowPositionDeltaRootRef);
        copyRef(this.segmentDirectoryRootRef, segmentDirectoryRootRef);
        this.pinned = true;
    }

    /**
     * Clears this pin for return to the pool. Called by the tracker under its
     * monitor after {@link #release}.
     */
    void disarm() {
        owner = null;
        generation = LiveViewCheckpointGenerationTracker.NO_GENERATION;
        normalizedBaseSeqTxn = -1;
        coveredLvSeqTxn = -1;
        pinned = false;
        timelineRootRef.clear();
        rowPositionDeltaRootRef.clear();
        segmentDirectoryRootRef.clear();
    }

    private static void copyRef(LiveViewCheckpointPageRef dst, LiveViewCheckpointPageRef src) {
        // of() with a null segment id preserves the null encoding, so this copies
        // null and non-null references alike.
        dst.of(src.getSegmentId(), src.getOffset(), src.getLength());
    }

    private void ensurePinned() {
        if (!pinned) {
            throw CairoException.critical(0)
                    .put("live view checkpoint generation pin is not held");
        }
    }
}
