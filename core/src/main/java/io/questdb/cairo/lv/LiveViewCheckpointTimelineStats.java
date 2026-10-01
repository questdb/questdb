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

import org.jetbrains.annotations.NotNull;

/**
 * The shape of one published checkpoint-timeline generation, read straight off
 * the superblock the publication committed. Every field describes the whole
 * generation rather than the root that publication happened to touch, which is
 * what makes the set comparable across a cadence seal, a repair splice and a
 * startup reconciliation.
 * <p>
 * A mutable value holder, filled by whichever component just read a valid
 * superblock and copied by {@link LiveViewInstance} into its own snapshot, so
 * publishing the set costs no allocation on the refresh worker.
 */
public final class LiveViewCheckpointTimelineStats {

    private long entryCount;
    private long generation;
    private long lastWriteNewBytes;
    private long logicalStateBytes;
    private long normalizedBaseSeqTxn;
    private long oldestRetainedGeneration;
    private long physicalBytes;
    private long rowPositionDeltaBytes;

    public void clear() {
        generation = 0;
        entryCount = 0;
        normalizedBaseSeqTxn = 0;
        logicalStateBytes = 0;
        physicalBytes = 0;
        rowPositionDeltaBytes = 0;
        oldestRetainedGeneration = 0;
        lastWriteNewBytes = 0;
    }

    /**
     * @return logical checkpoint boundaries the generation holds. Ids are
     * allocated from zero and monotonically within one history epoch, so the
     * next id to allocate less the boundaries the epoch has retired - the suffix
     * a high-side truncate dropped - is the size of the current entry set
     */
    public long getEntryCount() {
        return entryCount;
    }

    public long getGeneration() {
        return generation;
    }

    /**
     * @return metadata plus data bytes the publication that produced this
     * generation wrote, which is the marginal cost of one boundary rather than
     * a running total
     */
    public long getLastWriteNewBytes() {
        return lastWriteNewBytes;
    }

    /**
     * @return what the generation's roots would cost stored as complete
     * independent state images. Compare against {@link #getPhysicalBytes()} for
     * the sharing the persistent chunk layer and the copy-on-write trees buy
     */
    public long getLogicalStateBytes() {
        return logicalStateBytes;
    }

    public long getNormalizedBaseSeqTxn() {
        return normalizedBaseSeqTxn;
    }

    /**
     * @return the oldest generation whose files purge must still protect: the
     * lower of the two independently valid A/B slots. The distance to
     * {@link #getGeneration()} is the collection lag
     */
    public long getOldestRetainedGeneration() {
        return oldestRetainedGeneration;
    }

    /**
     * @return bytes the timeline has actually written under this history epoch,
     * metadata and data together. Purged segments are not subtracted: this is
     * what was produced, not what currently occupies the directory
     */
    public long getPhysicalBytes() {
        return physicalBytes;
    }

    public long getRowPositionDeltaBytes() {
        return rowPositionDeltaBytes;
    }

    /**
     * Reads the whole set off a committed superblock. {@code lastWriteNewBytes}
     * is the caller's own publication cost, which the superblock only carries as
     * part of its running total.
     */
    public LiveViewCheckpointTimelineStats of(
            @NotNull LiveViewCheckpointSuperblock superblock,
            long lastWriteNewBytes
    ) {
        this.generation = superblock.generation;
        this.entryCount = superblock.nextCheckpointId - superblock.retiredCheckpointCount;
        this.normalizedBaseSeqTxn = superblock.normalizedBaseSeqTxn;
        this.logicalStateBytes = superblock.logicalStateBytes;
        this.physicalBytes = superblock.metadataBytes + superblock.dataBytes;
        this.rowPositionDeltaBytes = superblock.rowPositionDeltaBytes;
        this.oldestRetainedGeneration = superblock.getOldestValidGeneration();
        this.lastWriteNewBytes = lastWriteNewBytes;
        return this;
    }
}
