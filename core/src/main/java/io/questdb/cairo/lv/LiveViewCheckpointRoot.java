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
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Durable state root referenced by one logical timeline entry.
 * <p>
 * Beside the anchor and function references, the root carries the sorted set of
 * segments its whole closure names: the data segments its functions' state pages
 * sit in, and the metadata segments holding its own page, its function directory
 * and every anchor-root, function-root and partition-map page below them. Both
 * halves are counted the same way by the catalogue, so publishing this root takes
 * one reference on each and retiring the boundary releases them in one
 * transaction - which is what lets a repair splice or a truncate reclaim a
 * boundary's files without walking anything.
 */
public class LiveViewCheckpointRoot implements Closeable {

    public static final int PAGE_KIND = 0x1a;
    private static final int FIXED_SIZE = 2 * Integer.BYTES + 3 * Long.BYTES + 2 * LiveViewCheckpointPageRef.BYTES;
    private static final int FORMAT_VERSION = 1;
    private final LiveViewCheckpointPageRef anchorRootRef = new LiveViewCheckpointPageRef();
    private long checkpointId;
    private long definitionTxn;
    private final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
    private long maxTimestamp;
    private final LiveViewCheckpointMetaSegmentReader reader;
    private long[] segmentIds = new long[0];

    public LiveViewCheckpointRoot(@NotNull CairoConfiguration configuration) {
        reader = new LiveViewCheckpointMetaSegmentReader(configuration);
    }

    @Override
    public void close() {
        Misc.free(reader);
    }

    /**
     * Unmaps the metadata segment this root was read from while keeping the
     * reader itself, so a reader that outlives one restore holds no mapping into
     * files a later retire, repair or compaction deletes.
     */
    public void detach() {
        reader.close();
    }

    public void getAnchorRootRef(@NotNull LiveViewCheckpointPageRef out) {
        out.of(anchorRootRef.getSegmentId(), anchorRootRef.getOffset(), anchorRootRef.getLength());
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public long getDefinitionTxn() {
        return definitionTxn;
    }

    public void getFunctionDirectoryRef(@NotNull LiveViewCheckpointPageRef out) {
        out.of(functionDirectoryRef.getSegmentId(), functionDirectoryRef.getOffset(), functionDirectoryRef.getLength());
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getSegmentId(int index) {
        return segmentIds[index];
    }

    public int getSegmentIdCount() {
        return segmentIds.length;
    }

    public void of(@Transient @NotNull Path checkpointsDir, @NotNull LiveViewCheckpointPageRef rootRef) {
        LiveViewCheckpointMetadata.validateMetaRef(rootRef, false, "checkpoint root");
        reader.of(checkpointsDir, rootRef.getSegmentId());
        reader.openPage(rootRef);
        if (reader.getPageKind() != PAGE_KIND) {
            throw LiveViewCheckpointMetadata.invalid("root page kind unknown, kind=").put(reader.getPageKind());
        }
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < FIXED_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("root payload too small, length=").put(payloadLength);
        }
        final int version = reader.getInt(0);
        final int segmentCount = reader.getInt(Integer.BYTES);
        if (version != FORMAT_VERSION || segmentCount < 0 || segmentCount > LiveViewCheckpointMetadata.MAX_ENTRY_COUNT) {
            throw LiveViewCheckpointMetadata.invalid("root version or segment count invalid")
                    .put(" [version=").put(version).put(", segmentCount=").put(segmentCount).put(']');
        }
        checkpointId = reader.getLong(2L * Integer.BYTES);
        maxTimestamp = reader.getLong(2L * Integer.BYTES + Long.BYTES);
        definitionTxn = reader.getLong(2L * Integer.BYTES + 2L * Long.BYTES);
        if (checkpointId < 0 || definitionTxn < 0) {
            throw LiveViewCheckpointMetadata.invalid("root checkpoint identity invalid")
                    .put(" [checkpointId=").put(checkpointId).put(", definitionTxn=").put(definitionTxn).put(']');
        }
        long offset = 2L * Integer.BYTES + 3L * Long.BYTES;
        LiveViewCheckpointMetadata.readMetaRef(reader, offset, anchorRootRef);
        LiveViewCheckpointMetadata.validateMetaRef(anchorRootRef, true, "anchor root");
        offset += LiveViewCheckpointPageRef.BYTES;
        LiveViewCheckpointMetadata.readMetaRef(reader, offset, functionDirectoryRef);
        LiveViewCheckpointMetadata.validateMetaRef(functionDirectoryRef, false, "function directory");
        offset += LiveViewCheckpointPageRef.BYTES;
        final long expectedLength = (long) FIXED_SIZE + (long) segmentCount * Long.BYTES;
        if (expectedLength != payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("root payload length mismatch")
                    .put(" [expected=").put(expectedLength).put(", actual=").put(payloadLength).put(']');
        }
        segmentIds = new long[segmentCount];
        long previous = -1;
        for (int i = 0; i < segmentCount; i++) {
            final long segmentId = reader.getLong(offset);
            if (segmentId < 0 || segmentId <= previous) {
                throw LiveViewCheckpointMetadata.invalid("root segment ids not strictly increasing")
                        .put(" [previous=").put(previous).put(", current=").put(segmentId).put(']');
            }
            segmentIds[i] = segmentId;
            previous = segmentId;
            offset += Long.BYTES;
        }
    }

    void ofBuilder(
            long checkpointId,
            long maxTimestamp,
            long definitionTxn,
            LiveViewCheckpointPageRef anchorRootRef,
            LiveViewCheckpointPageRef functionDirectoryRef,
            LongList segmentIds
    ) {
        this.checkpointId = checkpointId;
        this.maxTimestamp = maxTimestamp;
        this.definitionTxn = definitionTxn;
        this.anchorRootRef.of(anchorRootRef.getSegmentId(), anchorRootRef.getOffset(), anchorRootRef.getLength());
        this.functionDirectoryRef.of(functionDirectoryRef.getSegmentId(), functionDirectoryRef.getOffset(), functionDirectoryRef.getLength());
        this.segmentIds = new long[segmentIds.size()];
        for (int i = 0; i < segmentIds.size(); i++) {
            this.segmentIds[i] = segmentIds.getQuick(i);
        }
    }

    void writeTo(@NotNull LiveViewCheckpointMetaSegmentWriter writer, @NotNull LiveViewCheckpointPageRef out) {
        final MemoryA mem = writer.beginPage(PAGE_KIND);
        mem.putInt(FORMAT_VERSION);
        mem.putInt(segmentIds.length);
        mem.putLong(checkpointId);
        mem.putLong(maxTimestamp);
        mem.putLong(definitionTxn);
        LiveViewCheckpointMetadata.putMetaRef(mem, anchorRootRef);
        LiveViewCheckpointMetadata.putMetaRef(mem, functionDirectoryRef);
        for (int i = 0; i < segmentIds.length; i++) {
            mem.putLong(segmentIds[i]);
        }
        writer.endPage(out);
    }
}
