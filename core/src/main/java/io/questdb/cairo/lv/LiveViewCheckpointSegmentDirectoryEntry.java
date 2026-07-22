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
 * A mutable value holder for one segment directory entry: a data segment's
 * identity, published byte length, per-generation reference count and, once the
 * count reaches zero, the generation at which it retired.
 * <p>
 * The type is a reusable flyweight, like {@link LiveViewCheckpointTimelineEntry}:
 * a reader fills one instance per lookup or scan step, so navigation stays
 * allocation-free. The tree key is {@code segmentId}.
 */
public final class LiveViewCheckpointSegmentDirectoryEntry {

    /**
     * Byte length of the published {@code d.<segmentId>} file. Every bounded
     * page read validates its offset and length against this.
     */
    public long fileLength;
    /**
     * Number of current logical roots that name this segment. Repeated
     * references from one root count once.
     */
    public long referenceCount;
    /**
     * Generation at which {@link #referenceCount} last reached zero, or
     * {@link LiveViewCheckpointSegmentDirectory#RETIRE_GENERATION_NONE} while
     * the segment is still referenced.
     */
    public long retireGeneration;
    /**
     * Monotonic data segment id. The tree key.
     */
    public long segmentId;

    public LiveViewCheckpointSegmentDirectoryEntry clear() {
        segmentId = 0;
        fileLength = 0;
        referenceCount = 0;
        retireGeneration = LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE;
        return this;
    }

    public LiveViewCheckpointSegmentDirectoryEntry of(
            long segmentId,
            long fileLength,
            long referenceCount,
            long retireGeneration
    ) {
        this.segmentId = segmentId;
        this.fileLength = fileLength;
        this.referenceCount = referenceCount;
        this.retireGeneration = retireGeneration;
        return this;
    }
}
