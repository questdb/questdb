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
 * A mutable value holder for one segment directory entry: a segment's identity
 * and kind, published byte length, per-generation reference count and, once the
 * count reaches zero, the generation at which it retired.
 * <p>
 * The type is a reusable flyweight, like {@link LiveViewCheckpointTimelineEntry}:
 * a reader fills one instance per lookup or scan step, so navigation stays
 * allocation-free. The tree key is {@code segmentId}.
 */
public final class LiveViewCheckpointSegmentDirectoryEntry {

    /**
     * Byte length of the published {@code d.<segmentId>} or {@code m.<segmentId>}
     * file. Every bounded page read validates its offset and length against this.
     */
    public long fileLength;
    /**
     * {@link LiveViewCheckpointSegmentDirectory#SEGMENT_KIND_DATA},
     * {@link LiveViewCheckpointSegmentDirectory#SEGMENT_KIND_META} or
     * {@link LiveViewCheckpointSegmentDirectory#SEGMENT_KIND_BOUNDARY}, which
     * decides both the directory the file lives in and what
     * {@link #referenceCount} counts.
     */
    public long kind;
    /**
     * For a data or boundary-metadata segment, the number of current logical roots
     * that name it - repeated references from one root count once. For a
     * tree-metadata segment, the number of its pages the current generation's
     * superblock-rooted trees still reach.
     */
    public long referenceCount;
    /**
     * Generation at which {@link #referenceCount} last reached zero, or
     * {@link LiveViewCheckpointSegmentDirectory#RETIRE_GENERATION_NONE} while
     * the segment is still referenced.
     */
    public long retireGeneration;
    /**
     * Monotonic segment id, unique across both kinds. The tree key.
     */
    public long segmentId;

    public LiveViewCheckpointSegmentDirectoryEntry clear() {
        segmentId = 0;
        fileLength = 0;
        referenceCount = 0;
        retireGeneration = LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE;
        kind = LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA;
        return this;
    }

    /**
     * @return true when the segment's file lives in {@code meta/} - either kind of
     * metadata segment. Callers that care about the counting unit rather than the
     * directory read {@link #kind} instead.
     */
    public boolean isMetadata() {
        return kind != LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA;
    }

    public LiveViewCheckpointSegmentDirectoryEntry of(
            long segmentId,
            long fileLength,
            long referenceCount,
            long retireGeneration,
            long kind
    ) {
        this.segmentId = segmentId;
        this.fileLength = fileLength;
        this.referenceCount = referenceCount;
        this.retireGeneration = retireGeneration;
        this.kind = kind;
        return this;
    }
}
