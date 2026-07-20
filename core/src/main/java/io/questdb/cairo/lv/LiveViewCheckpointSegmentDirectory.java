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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Generation-transactional segment catalogue stored as one checksummed metadata
 * page. Reference counts are per logical root, not per page: each root's segment
 * id list is deduplicated before a change is applied. A zero count records the
 * generation at which the segment became obsolete for later pin-safe purge.
 */
public class LiveViewCheckpointSegmentDirectory implements Closeable {

    public static final int PAGE_KIND = 0x15;
    public static final long RETIRE_GENERATION_NONE = -1;
    private static final int ENTRY_FILE_LENGTH_OFFSET = Long.BYTES;
    private static final int ENTRY_REFERENCE_COUNT_OFFSET = 2 * Long.BYTES;
    private static final int ENTRY_RETIRE_GENERATION_OFFSET = 3 * Long.BYTES;
    private static final int ENTRY_SEGMENT_ID_OFFSET = 0;
    private static final int ENTRY_STRIDE = 4 * Long.BYTES;
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_SIZE = 2 * Integer.BYTES;
    private static final int LONGS_PER_ENTRY = 4;
    private static final int COUNT_OFFSET = Integer.BYTES;
    private static final int VERSION_OFFSET = 0;
    private final LongHashSet addedSegmentIds = new LongHashSet();
    private final LongList entries = new LongList();
    private final LiveViewCheckpointMetaSegmentReader reader;
    private final LongHashSet removedSegmentIds = new LongHashSet();

    public LiveViewCheckpointSegmentDirectory(@NotNull CairoConfiguration configuration) {
        reader = new LiveViewCheckpointMetaSegmentReader(configuration);
    }

    /**
     * Registers a newly published segment with the number of logical roots that
     * reference it in the candidate generation.
     */
    public void addSegment(long segmentId, long fileLength, long referenceCount) {
        if (segmentId < 0 || fileLength <= 0 || referenceCount <= 0) {
            throw CairoException.critical(0)
                    .put("invalid live view checkpoint segment directory entry")
                    .put(" [segmentId=").put(segmentId)
                    .put(", fileLength=").put(fileLength)
                    .put(", referenceCount=").put(referenceCount)
                    .put(']');
        }
        int index = findIndex(segmentId);
        if (index >= 0) {
            throw CairoException.critical(0)
                    .put("duplicate live view checkpoint data segment, segmentId=")
                    .put(segmentId);
        }
        index = -index - 1;
        final int base = index * LONGS_PER_ENTRY;
        entries.add(base, segmentId);
        entries.add(base + 1, fileLength);
        entries.add(base + 2, referenceCount);
        entries.add(base + 3, RETIRE_GENERATION_NONE);
    }

    /**
     * Applies one generation's root replacement. Repeated references to pages in
     * the same segment count once for each root side.
     */
    public void applyRootReferenceChanges(
            @NotNull LongList removedRootSegmentIds,
            @NotNull LongList addedRootSegmentIds,
            long generation
    ) {
        if (generation < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint retire generation must be non-negative, was ")
                    .put(generation);
        }
        removedSegmentIds.clear();
        for (int i = 0, n = removedRootSegmentIds.size(); i < n; i++) {
            final long segmentId = removedRootSegmentIds.getQuick(i);
            validateReferenceSegmentId(segmentId);
            removedSegmentIds.add(segmentId);
        }
        addedSegmentIds.clear();
        for (int i = 0, n = addedRootSegmentIds.size(); i < n; i++) {
            final long segmentId = addedRootSegmentIds.getQuick(i);
            validateReferenceSegmentId(segmentId);
            addedSegmentIds.add(segmentId);
        }

        // Validate the complete transaction before mutating a count. A failed
        // candidate build must leave the reusable directory image untouched.
        for (int i = 0, n = removedSegmentIds.size(); i < n; i++) {
            final long segmentId = removedSegmentIds.get(i);
            final int index = findIndex(segmentId);
            if (index < 0) {
                throw CairoException.critical(0)
                        .put("cannot remove reference to unknown live view checkpoint data segment, segmentId=")
                        .put(segmentId);
            }
            if (entries.getQuick(index * LONGS_PER_ENTRY + 2) <= 0) {
                throw CairoException.critical(0)
                        .put("live view checkpoint data segment reference count underflow, segmentId=")
                        .put(segmentId);
            }
        }
        for (int i = 0, n = addedSegmentIds.size(); i < n; i++) {
            final long segmentId = addedSegmentIds.get(i);
            final int index = findIndex(segmentId);
            if (index < 0) {
                throw CairoException.critical(0)
                        .put("cannot add reference to unknown live view checkpoint data segment, segmentId=")
                        .put(segmentId);
            }
            long count = entries.getQuick(index * LONGS_PER_ENTRY + 2);
            if (removedSegmentIds.contains(segmentId)) {
                count--;
            }
            if (count == Long.MAX_VALUE) {
                throw CairoException.critical(0)
                        .put("live view checkpoint data segment reference count overflow, segmentId=")
                        .put(segmentId);
            }
        }

        for (int i = 0, n = removedSegmentIds.size(); i < n; i++) {
            decrementReferenceCount(removedSegmentIds.get(i), generation);
        }
        for (int i = 0, n = addedSegmentIds.size(); i < n; i++) {
            incrementReferenceCount(addedSegmentIds.get(i));
        }
    }

    public void clear() {
        addedSegmentIds.clear();
        entries.clear();
        removedSegmentIds.clear();
    }

    @Override
    public void close() {
        Misc.free(reader);
        addedSegmentIds.clear();
        entries.clear();
        removedSegmentIds.clear();
    }

    public long getFileLength(long segmentId) {
        return entryValue(segmentId, 1, "file length");
    }

    public long getObsoleteBytes() {
        long bytes = 0;
        for (int i = 0, n = size(); i < n; i++) {
            final int base = i * LONGS_PER_ENTRY;
            if (entries.getQuick(base + 2) == 0) {
                bytes = checkedAdd(bytes, entries.getQuick(base + 1), "obsolete byte count");
            }
        }
        return bytes;
    }

    public long getReferencedBytes() {
        long bytes = 0;
        for (int i = 0, n = size(); i < n; i++) {
            final int base = i * LONGS_PER_ENTRY;
            if (entries.getQuick(base + 2) > 0) {
                bytes = checkedAdd(bytes, entries.getQuick(base + 1), "referenced byte count");
            }
        }
        return bytes;
    }

    public long getReferenceCount(long segmentId) {
        return entryValue(segmentId, 2, "reference count");
    }

    public long getRetireGeneration(long segmentId) {
        return entryValue(segmentId, 3, "retire generation");
    }

    /**
     * Loads and structurally validates a checksummed directory root without
     * opening or scanning any referenced data file.
     */
    public void of(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef rootRef
    ) {
        clear();
        if (rootRef.isNull()) {
            return;
        }
        reader.of(checkpointsDir, rootRef.getSegmentId());
        reader.openPage(rootRef);
        if (reader.getPageKind() != PAGE_KIND) {
            throw invalid("segment directory page kind unknown")
                    .put(", kind=").put(reader.getPageKind());
        }
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < HEADER_SIZE) {
            throw invalid("segment directory payload too small")
                    .put(", payloadLength=").put(payloadLength);
        }
        final int version = reader.getInt(VERSION_OFFSET);
        if (version != FORMAT_VERSION) {
            throw invalid("segment directory format version mismatch")
                    .put(" [expected=").put(FORMAT_VERSION)
                    .put(", actual=").put(version)
                    .put(']');
        }
        final int count = reader.getInt(COUNT_OFFSET);
        if (count < 0) {
            throw invalid("segment directory count negative").put(", count=").put(count);
        }
        final long expectedLength = (long) HEADER_SIZE + (long) count * ENTRY_STRIDE;
        if (expectedLength != payloadLength) {
            throw invalid("segment directory payload length mismatch")
                    .put(" [count=").put(count)
                    .put(", expected=").put(expectedLength)
                    .put(", actual=").put(payloadLength)
                    .put(']');
        }
        long previousSegmentId = -1;
        long offset = HEADER_SIZE;
        for (int i = 0; i < count; i++, offset += ENTRY_STRIDE) {
            final long segmentId = reader.getLong(offset + ENTRY_SEGMENT_ID_OFFSET);
            final long fileLength = reader.getLong(offset + ENTRY_FILE_LENGTH_OFFSET);
            final long referenceCount = reader.getLong(offset + ENTRY_REFERENCE_COUNT_OFFSET);
            final long retireGeneration = reader.getLong(offset + ENTRY_RETIRE_GENERATION_OFFSET);
            if (segmentId < 0 || segmentId <= previousSegmentId) {
                throw invalid("segment directory ids not strictly increasing")
                        .put(" [previous=").put(previousSegmentId)
                        .put(", current=").put(segmentId)
                        .put(']');
            }
            if (fileLength <= 0 || referenceCount < 0) {
                throw invalid("segment directory entry value invalid")
                        .put(" [segmentId=").put(segmentId)
                        .put(", fileLength=").put(fileLength)
                        .put(", referenceCount=").put(referenceCount)
                        .put(']');
            }
            if ((referenceCount == 0 && retireGeneration < 0)
                    || (referenceCount > 0 && retireGeneration != RETIRE_GENERATION_NONE)) {
                throw invalid("segment directory retirement state invalid")
                        .put(" [segmentId=").put(segmentId)
                        .put(", referenceCount=").put(referenceCount)
                        .put(", retireGeneration=").put(retireGeneration)
                        .put(']');
            }
            entries.add(segmentId, fileLength, referenceCount, retireGeneration);
            previousSegmentId = segmentId;
        }
    }

    public int size() {
        return entries.size() / LONGS_PER_ENTRY;
    }

    /**
     * Serializes the complete candidate-generation directory into an immutable,
     * checksummed metadata page.
     */
    public void writeTo(
            @NotNull LiveViewCheckpointMetaSegmentWriter writer,
            @NotNull LiveViewCheckpointPageRef out
    ) {
        final MemoryA mem = writer.beginPage(PAGE_KIND);
        mem.putInt(FORMAT_VERSION);
        mem.putInt(size());
        for (int i = 0, n = entries.size(); i < n; i++) {
            mem.putLong(entries.getQuick(i));
        }
        writer.endPage(out);
    }

    private static void validateReferenceSegmentId(long segmentId) {
        if (segmentId < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint data segment reference id must be non-negative, was ")
                    .put(segmentId);
        }
    }

    private static long checkedAdd(long a, long b, CharSequence what) {
        if (b > Long.MAX_VALUE - a) {
            throw CairoException.critical(0)
                    .put("live view checkpoint segment directory ")
                    .put(what).put(" overflow");
        }
        return a + b;
    }

    private void decrementReferenceCount(long segmentId, long generation) {
        final int index = findIndex(segmentId);
        if (index < 0) {
            throw CairoException.critical(0)
                    .put("cannot remove reference to unknown live view checkpoint data segment, segmentId=")
                    .put(segmentId);
        }
        final int countIndex = index * LONGS_PER_ENTRY + 2;
        final long count = entries.getQuick(countIndex);
        if (count <= 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint data segment reference count underflow, segmentId=")
                    .put(segmentId);
        }
        entries.setQuick(countIndex, count - 1);
        if (count == 1) {
            entries.setQuick(countIndex + 1, generation);
        }
    }

    private long entryValue(long segmentId, int field, CharSequence fieldName) {
        final int index = findIndex(segmentId);
        if (index < 0) {
            throw CairoException.critical(0)
                    .put("unknown live view checkpoint data segment ")
                    .put(fieldName).put(", segmentId=").put(segmentId);
        }
        return entries.getQuick(index * LONGS_PER_ENTRY + field);
    }

    /**
     * Returns the entry index, or {@code -insertionPoint - 1}.
     */
    private int findIndex(long segmentId) {
        int lo = 0;
        int hi = size() - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long value = entries.getQuick(mid * LONGS_PER_ENTRY);
            if (value < segmentId) {
                lo = mid + 1;
            } else if (value > segmentId) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -lo - 1;
    }

    private void incrementReferenceCount(long segmentId) {
        final int index = findIndex(segmentId);
        if (index < 0) {
            throw CairoException.critical(0)
                    .put("cannot add reference to unknown live view checkpoint data segment, segmentId=")
                    .put(segmentId);
        }
        final int countIndex = index * LONGS_PER_ENTRY + 2;
        final long count = entries.getQuick(countIndex);
        if (count == Long.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("live view checkpoint data segment reference count overflow, segmentId=")
                    .put(segmentId);
        }
        entries.setQuick(countIndex, count + 1);
        if (count == 0) {
            entries.setQuick(countIndex + 1, RETIRE_GENERATION_NONE);
        }
    }

    private CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint ").put(reason);
    }
}
