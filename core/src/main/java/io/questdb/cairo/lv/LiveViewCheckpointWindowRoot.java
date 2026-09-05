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
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Checksummed metadata root for one live view's <b>fused</b> window state: the anchor
 * value and every compatible accumulator component of one anchored window, held in a
 * single persistent partition map instead of one map per SELECT-list function.
 * <p>
 * It stands where {@link LiveViewCheckpointAnchorRoot} stands - the enclosing
 * {@link LiveViewCheckpointRoot}'s one state-root reference is a tagged union decoded
 * by page kind - and it replaces both that root and the function roots of the functions
 * it fuses. A function the plan could not group keeps its own root in the function
 * directory beside this one, so "one B-tree per window" means one tree for the grouped
 * components plus independent roots for the shapes the group does not admit.
 *
 * <h2>The manifest is the whole of the layout</h2>
 * A fused leaf entry is a flat run of bytes: no per-partition version, no component
 * tags, no lengths of its own.
 * <pre>
 *   offset 0: anchor value, 8 bytes
 *   then:     components in the manifest's canonical identity order
 *   refs:     empty
 * </pre>
 * Its meaning comes entirely from {@link #getManifest()}, which is written once per
 * root rather than once per partition. That makes a manifest disagreement a silent
 * misread rather than a rejection - a decoder finds the total length it expects and
 * reads the wrong fields out of it - and it is why byte equality against the
 * predecessor's manifest is part of what a writer must prove before it may seal
 * incrementally over leaves that predecessor wrote. See
 * {@link LiveViewWindowStateManifest} for the rest of that argument.
 * <p>
 * Nothing here reaches a data segment. Every component the first fused release admits
 * is fixed width and inlines, so the root's per-segment counts name metadata segments
 * only - its own page and the partition-map pages below it, which older seals may have
 * written and a copy-on-write build still shares.
 */
public class LiveViewCheckpointWindowRoot implements Closeable {

    public static final int PAGE_KIND = 0x1d;
    private static final int FIXED_SIZE = 7 * Integer.BYTES + LiveViewCheckpointPageRef.BYTES;
    private static final int FORMAT_VERSION = 1;
    private final LiveViewCheckpointPageRef partitionMapRootRef = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointMetaSegmentReader reader;
    private int anchorValueType;
    /**
     * Images of the open root, borrowed from {@link #decodedBytes} on the decode
     * path and from the caller's compiled plan on the builder path. A publication
     * opens the same root more than once, so decoding fresh arrays would charge
     * the identity to every open.
     */
    private final LiveViewCheckpointByteArrayPool decodedBytes = new LiveViewCheckpointByteArrayPool();
    private byte[] keySchema = new byte[0];
    private byte[] manifest = new byte[0];
    private final LongList segmentIds = new LongList();
    private final LongList segmentUseCounts = new LongList();
    private int totalInlineStateBytes;
    private byte[] windowIdentity = new byte[0];

    public LiveViewCheckpointWindowRoot(@NotNull CairoConfiguration configuration) {
        reader = new LiveViewCheckpointMetaSegmentReader(configuration);
    }

    /**
     * Decodes the anchor value out of a fused scalar payload. It leads the payload at a
     * fixed offset so a decoder can read it before it has looked at the manifest, and it
     * is encoded exactly as a legacy anchor entry encodes it, which is what lets the two
     * shapes hold the same bytes for the same key across a conversion seal.
     */
    public static long readAnchorValue(byte @NotNull [] scalarState) {
        if (scalarState.length < LiveViewWindowStatePlan.ANCHOR_STATE_OFFSET + LiveViewWindowStatePlan.ANCHOR_STATE_BYTES) {
            throw LiveViewCheckpointMetadata.invalid("window state entry is too short for its anchor value, length=")
                    .put(scalarState.length);
        }
        long value = 0;
        for (int i = LiveViewWindowStatePlan.ANCHOR_STATE_BYTES - 1; i >= 0; i--) {
            value = (value << 8) | (scalarState[LiveViewWindowStatePlan.ANCHOR_STATE_OFFSET + i] & 0xffL);
        }
        return value;
    }

    /**
     * Returns one fused entry's scalar payload, having proved it is the shape the
     * manifest names: exactly {@code totalInlineStateBytes} of scalar state and no state
     * page beside it. Both halves are checked - a payload of the right length that also
     * names a page is not the entry the manifest describes, and reading it as one would
     * take a component's slice out of bytes something else wrote.
     */
    public static byte[] readWindowState(@NotNull LiveViewCheckpointPartitionMapEntry entry, int totalInlineStateBytes) {
        if (entry.getStatePageCount() != 0) {
            throw LiveViewCheckpointMetadata.invalid("window state entry must not reference a state page, pages=")
                    .put(entry.getStatePageCount());
        }
        final byte[] scalarState = entry.getScalarState();
        if (scalarState.length != totalInlineStateBytes) {
            throw LiveViewCheckpointMetadata.invalid("window state entry scalar length invalid [expected=")
                    .put(totalInlineStateBytes).put(", actual=").put(scalarState.length).put(']');
        }
        return scalarState;
    }

    @Override
    public void close() {
        Misc.free(reader);
    }

    /**
     * Unmaps the metadata segment this root was read from while keeping the reader
     * itself, so a reader that outlives one restore holds no mapping into files a later
     * retire, repair or compaction deletes.
     */
    public void detach() {
        reader.close();
    }

    public int getAnchorValueType() {
        return anchorValueType;
    }

    public byte[] getKeySchema() {
        return keySchema;
    }

    /**
     * Returns the encoded component manifest this root's entries are laid out by. The
     * bytes are what a predecessor comparison reads: any difference at all forces the
     * full-scan conversion seal.
     */
    public byte[] getManifest() {
        return manifest;
    }

    public void getPartitionMapRootRef(@NotNull LiveViewCheckpointPageRef out) {
        out.of(partitionMapRootRef.getSegmentId(), partitionMapRootRef.getOffset(), partitionMapRootRef.getLength());
    }

    public long getSegmentId(int index) {
        return segmentIds.getQuick(index);
    }

    public long getSegmentUseCount(int index) {
        return segmentUseCounts.getQuick(index);
    }

    public int getSegmentUseCountSize() {
        return segmentIds.size();
    }

    public int getTotalInlineStateBytes() {
        return totalInlineStateBytes;
    }

    /**
     * Borrows the decoded persisted identity through this root's lifetime. Callers must
     * not mutate the returned bytes or retain them after the root is reopened.
     */
    byte[] borrowWindowIdentity() {
        return windowIdentity;
    }

    public byte[] getWindowIdentity() {
        return windowIdentity;
    }

    public void of(@Transient @NotNull Path checkpointsDir, @NotNull LiveViewCheckpointPageRef rootRef) {
        if (!of0(checkpointsDir, rootRef)) {
            throw LiveViewCheckpointMetadata.invalid("window state root page kind unknown, kind=")
                    .put(reader.getPageKind());
        }
    }

    /**
     * Decodes {@code rootRef} when it names a window-state root, and answers false when
     * it names something else - which is how the state-root tagged union is read: a
     * legacy anchor root under an older checkpoint is an ordinary answer rather than
     * corruption, and only a third kind is.
     */
    public boolean ofIfWindowRoot(@Transient @NotNull Path checkpointsDir, @NotNull LiveViewCheckpointPageRef rootRef) {
        return of0(checkpointsDir, rootRef);
    }

    private boolean of0(Path checkpointsDir, LiveViewCheckpointPageRef rootRef) {
        LiveViewCheckpointMetadata.validateMetaRef(rootRef, false, "window state root");
        reader.of(checkpointsDir, rootRef.getSegmentId());
        reader.openPage(rootRef);
        if (reader.getPageKind() != PAGE_KIND) {
            return false;
        }
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < FIXED_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("window state root payload too small, length=").put(payloadLength);
        }
        final int version = reader.getInt(0);
        if (version != FORMAT_VERSION) {
            throw LiveViewCheckpointMetadata.invalid("window state root format version mismatch")
                    .put(" [expected=").put(FORMAT_VERSION).put(", actual=").put(version).put(']');
        }
        anchorValueType = reader.getInt(Integer.BYTES);
        totalInlineStateBytes = reader.getInt(2L * Integer.BYTES);
        final int windowIdentityLength = reader.getInt(3L * Integer.BYTES);
        final int keySchemaLength = reader.getInt(4L * Integer.BYTES);
        final int manifestLength = reader.getInt(5L * Integer.BYTES);
        final int segmentCount = reader.getInt(6L * Integer.BYTES);
        LiveViewCheckpointMetadata.validateByteArrayLength(windowIdentityLength, "window state identity");
        LiveViewCheckpointMetadata.validateByteArrayLength(keySchemaLength, "window state key schema");
        LiveViewCheckpointMetadata.validateByteArrayLength(manifestLength, "window state manifest");
        if (windowIdentityLength == 0 || keySchemaLength < Integer.BYTES || manifestLength == 0) {
            throw LiveViewCheckpointMetadata.invalid("window state root identity, key schema or manifest invalid")
                    .put(" [windowIdentityLength=").put(windowIdentityLength)
                    .put(", keySchemaLength=").put(keySchemaLength)
                    .put(", manifestLength=").put(manifestLength).put(']');
        }
        // The leaf holds no length of its own, so a payload width the root does not state
        // is one no entry could be sliced by. The budget is deliberately not re-checked
        // here: it is a writer-side storage choice, and a reader applying it would reject
        // entries an earlier build legitimately wrote if the constant ever moved.
        if (totalInlineStateBytes <= LiveViewWindowStatePlan.ANCHOR_STATE_BYTES) {
            throw LiveViewCheckpointMetadata.invalid("window state root inline payload width invalid, bytes=")
                    .put(totalInlineStateBytes);
        }
        if (segmentCount < 0 || segmentCount > LiveViewCheckpointMetadata.MAX_ENTRY_COUNT) {
            throw LiveViewCheckpointMetadata.invalid("window state root segment count invalid, segmentCount=")
                    .put(segmentCount);
        }
        LiveViewCheckpointMetadata.readMetaRef(reader, 7L * Integer.BYTES, partitionMapRootRef);
        LiveViewCheckpointMetadata.validateMetaRef(partitionMapRootRef, true, "window state partition map root");
        final long expectedLength = (long) FIXED_SIZE + windowIdentityLength + keySchemaLength + manifestLength
                + (long) segmentCount * 2 * Long.BYTES;
        if (expectedLength != payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("window state root payload length mismatch")
                    .put(" [expected=").put(expectedLength).put(", actual=").put(payloadLength).put(']');
        }
        long offset = FIXED_SIZE;
        decodedBytes.reset();
        windowIdentity = LiveViewCheckpointMetadata.readBytes(reader, offset, windowIdentityLength, decodedBytes);
        offset += windowIdentityLength;
        keySchema = LiveViewCheckpointMetadata.readBytes(reader, offset, keySchemaLength, decodedBytes);
        offset += keySchemaLength;
        manifest = LiveViewCheckpointMetadata.readBytes(reader, offset, manifestLength, decodedBytes);
        offset += manifestLength;
        segmentIds.clear();
        segmentUseCounts.clear();
        long previous = -1;
        for (int i = 0; i < segmentCount; i++) {
            final long segmentId = reader.getLong(offset);
            final long useCount = reader.getLong(offset + Long.BYTES);
            if (segmentId < 0 || segmentId <= previous || useCount <= 0) {
                throw LiveViewCheckpointMetadata.invalid("window state root segment catalogue invalid")
                        .put(" [segmentId=").put(segmentId).put(", previous=").put(previous)
                        .put(", useCount=").put(useCount).put(']');
            }
            segmentIds.add(segmentId);
            segmentUseCounts.add(useCount);
            previous = segmentId;
            offset += 2L * Long.BYTES;
        }
        return true;
    }

    /**
     * Writes {@code anchorValue} into a fused scalar payload at the fixed offset the
     * anchor leads it with.
     */
    static void encodeAnchorValue(long anchorValue, byte[] scalarState) {
        for (int i = 0; i < LiveViewWindowStatePlan.ANCHOR_STATE_BYTES; i++) {
            scalarState[LiveViewWindowStatePlan.ANCHOR_STATE_OFFSET + i] = (byte) (anchorValue >>> (i * Byte.SIZE));
        }
    }

    void clearBorrowedCompiled() {
        windowIdentity = null;
        keySchema = null;
        manifest = null;
    }

    @TestOnly
    boolean isBorrowingCompiledForTest(byte[] windowIdentity, byte[] keySchema, byte[] manifest) {
        return this.windowIdentity == windowIdentity && this.keySchema == keySchema && this.manifest == manifest;
    }

    void ofBuilder(
            byte[] windowIdentity,
            int anchorValueType,
            byte[] keySchema,
            byte[] manifest,
            int totalInlineStateBytes,
            LiveViewCheckpointPageRef partitionMapRootRef,
            LongList segmentUseCounts
    ) {
        this.windowIdentity = windowIdentity;
        this.anchorValueType = anchorValueType;
        this.keySchema = keySchema;
        this.manifest = manifest;
        this.totalInlineStateBytes = totalInlineStateBytes;
        this.partitionMapRootRef.of(
                partitionMapRootRef.getSegmentId(),
                partitionMapRootRef.getOffset(),
                partitionMapRootRef.getLength()
        );
        final int count = segmentUseCounts.size() / 2;
        segmentIds.clear();
        this.segmentUseCounts.clear();
        for (int i = 0; i < count; i++) {
            segmentIds.add(segmentUseCounts.getQuick(i * 2));
            this.segmentUseCounts.add(segmentUseCounts.getQuick(i * 2 + 1));
        }
    }

    void writeTo(@NotNull LiveViewCheckpointMetaSegmentWriter writer, @NotNull LiveViewCheckpointPageRef out) {
        final MemoryA mem = writer.beginPage(PAGE_KIND);
        mem.putInt(FORMAT_VERSION);
        mem.putInt(anchorValueType);
        mem.putInt(totalInlineStateBytes);
        mem.putInt(windowIdentity.length);
        mem.putInt(keySchema.length);
        mem.putInt(manifest.length);
        mem.putInt(segmentIds.size());
        LiveViewCheckpointMetadata.putMetaRef(mem, partitionMapRootRef);
        LiveViewCheckpointMetadata.putBytes(mem, windowIdentity);
        LiveViewCheckpointMetadata.putBytes(mem, keySchema);
        LiveViewCheckpointMetadata.putBytes(mem, manifest);
        for (int i = 0, n = segmentIds.size(); i < n; i++) {
            mem.putLong(segmentIds.getQuick(i));
            mem.putLong(segmentUseCounts.getQuick(i));
        }
        writer.endPage(out);
    }
}
