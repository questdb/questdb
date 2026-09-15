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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Flyweight view of one compaction plan held by the refresh worker's reusable
 * scratch. Physical keys and target-reference columns remain in tracker-bound
 * native storage until publication finishes.
 */
public final class LiveViewCheckpointCompactionPlan {

    private final LiveViewCheckpointCompactionScratch scratch;
    private final LiveViewCheckpointStatePageRef targetFlyweight = new LiveViewCheckpointStatePageRef();
    private long generation;
    private long targetSegmentBytes;
    private long targetSegmentId;

    LiveViewCheckpointCompactionPlan(@NotNull LiveViewCheckpointCompactionScratch scratch) {
        this.scratch = scratch;
    }

    LiveViewCheckpointCompactionPlan of(long targetSegmentId, long targetSegmentBytes, long generation) {
        if (targetSegmentId < 0 || targetSegmentBytes <= 0 || generation < 0) {
            throw CairoException.critical(0)
                    .put("invalid live view checkpoint compaction plan [targetSegmentId=").put(targetSegmentId)
                    .put(", targetSegmentBytes=").put(targetSegmentBytes)
                    .put(", generation=").put(generation).put(']');
        }
        this.targetSegmentId = targetSegmentId;
        this.targetSegmentBytes = targetSegmentBytes;
        this.generation = generation;
        return this;
    }

    public long getGeneration() {
        return generation;
    }

    public long getTargetSegmentBytes() {
        return targetSegmentBytes;
    }

    public long getTargetSegmentId() {
        return targetSegmentId;
    }

    public boolean isDrainedSegment(long segmentId) {
        return scratch.isSelectedSegment(segmentId);
    }

    public @Nullable LiveViewCheckpointStatePageRef redirect(@NotNull LiveViewCheckpointStatePageRef source) {
        return scratch.redirect(source, targetFlyweight);
    }

    public int size() {
        return scratch.getTargetPageCount();
    }
}
