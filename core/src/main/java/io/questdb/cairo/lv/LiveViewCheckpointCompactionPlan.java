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
import io.questdb.std.LongHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;

/**
 * The output of one compaction planning pass: the target segment the driver
 * repacked every drained page into, and a physical-page redirect the publication
 * applies to each root that still names a drained page.
 * <p>
 * A redirect key is a physical page - {@code (segmentId, offset, storedLength)} -
 * because copy-on-write timeline sharing lets one physical state page be named by
 * many roots. The publication rebuilds every root that names a drained segment,
 * swapping each such page reference to the identical relocated reference this
 * plan carries. Because every root sees the same target reference for a shared
 * page, a chunk shared across boundaries stays shared after the move.
 * <p>
 * The plan pins no state of its own: it is built against a generation the driver
 * pinned, records that generation, and {@link LiveViewCheckpointTimelineStoreWriter#publishCompaction}
 * refuses to publish if the timeline has moved under it.
 */
public final class LiveViewCheckpointCompactionPlan {

    private final long generation;
    private final HashMap<PageKey, LiveViewCheckpointStatePageRef> redirects = new HashMap<>();
    private final LongHashSet sourceSegmentIds = new LongHashSet();
    private final long targetSegmentBytes;
    private final long targetSegmentId;

    public LiveViewCheckpointCompactionPlan(long targetSegmentId, long targetSegmentBytes, long generation) {
        if (targetSegmentId < 0 || targetSegmentBytes <= 0 || generation < 0) {
            throw CairoException.critical(0)
                    .put("invalid live view checkpoint compaction plan [targetSegmentId=").put(targetSegmentId)
                    .put(", targetSegmentBytes=").put(targetSegmentBytes)
                    .put(", generation=").put(generation).put(']');
        }
        this.targetSegmentId = targetSegmentId;
        this.targetSegmentBytes = targetSegmentBytes;
        this.generation = generation;
    }

    /**
     * Records that the physical page named by {@code source} was repacked into
     * {@code target}. Both references are copied by value, so the caller may reuse
     * its flyweights.
     */
    public void addRedirect(
            @NotNull LiveViewCheckpointStatePageRef source,
            @NotNull LiveViewCheckpointStatePageRef target
    ) {
        if (source.isNull() || target.isNull()) {
            throw CairoException.critical(0).put("live view checkpoint compaction redirect must not be null");
        }
        if (target.getSegmentId() != targetSegmentId) {
            throw CairoException.critical(0)
                    .put("live view checkpoint compaction redirect target segment mismatch [expected=")
                    .put(targetSegmentId).put(", was=").put(target.getSegmentId()).put(']');
        }
        sourceSegmentIds.add(source.getSegmentId());
        final PageKey key = new PageKey(source.getSegmentId(), source.getOffset(), source.getStoredLength());
        if (redirects.putIfAbsent(key, copyRef(target)) != null) {
            throw CairoException.critical(0)
                    .put("duplicate live view checkpoint compaction redirect [segmentId=")
                    .put(source.getSegmentId()).put(", offset=").put(source.getOffset()).put(']');
        }
    }

    /**
     * @return the generation the plan was built against; the publication refuses
     * to splice if the timeline has advanced past it
     */
    public long getGeneration() {
        return generation;
    }

    public long getTargetSegmentBytes() {
        return targetSegmentBytes;
    }

    public long getTargetSegmentId() {
        return targetSegmentId;
    }

    /**
     * @return true when {@code segmentId} is one of the segments this compaction
     * drains, so a root that names it must be rebuilt
     */
    public boolean isDrainedSegment(long segmentId) {
        return sourceSegmentIds.contains(segmentId);
    }

    /**
     * @return the relocated reference for the physical page {@code source} names,
     * or null when the page was not part of this compaction
     */
    public @Nullable LiveViewCheckpointStatePageRef redirect(@NotNull LiveViewCheckpointStatePageRef source) {
        return redirects.get(new PageKey(source.getSegmentId(), source.getOffset(), source.getStoredLength()));
    }

    public int size() {
        return redirects.size();
    }

    private static LiveViewCheckpointStatePageRef copyRef(LiveViewCheckpointStatePageRef source) {
        return new LiveViewCheckpointStatePageRef().of(
                source.getSegmentId(), source.getOffset(), source.getStoredLength(), source.getDecodedLength(),
                source.getPageKind(), source.getCodec(), source.getRowCount(), source.getFlags()
        );
    }

    private static final class PageKey {
        private final long offset;
        private final long segmentId;
        private final int storedLength;

        private PageKey(long segmentId, long offset, int storedLength) {
            this.segmentId = segmentId;
            this.offset = offset;
            this.storedLength = storedLength;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof PageKey)) {
                return false;
            }
            final PageKey that = (PageKey) obj;
            return segmentId == that.segmentId && offset == that.offset && storedLength == that.storedLength;
        }

        @Override
        public int hashCode() {
            long hash = segmentId * 31 + offset;
            hash = hash * 31 + storedLength;
            return (int) (hash ^ (hash >>> 32));
        }
    }
}
