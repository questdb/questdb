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
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Checksummed metadata root for the optional anchored-window state.
 * <p>
 * An anchor keeps one last-seen anchor value per partition, so the root pairs
 * the window's identity - name, partition-key schema, and anchor value type -
 * with a persistent partition map that holds those values as scalar metadata.
 * Nothing here reaches a data segment: an anchor entry is a key plus eight
 * bytes, small enough to live in the same checksummed pages the map is built
 * from.
 * <p>
 * Storing the map per key rather than as one serialized image is what lets a
 * cadence seal copy only the leaf paths whose anchor value moved, and lets a
 * localized out-of-order repair re-version only the keys it replayed.
 */
public class LiveViewCheckpointAnchorRoot implements Closeable {

    public static final int PAGE_KIND = 0x1b;
    /** Bytes one partition entry's scalar payload occupies: the last-seen anchor value. */
    static final int ENTRY_STATE_SIZE = Long.BYTES;
    private static final int FIXED_SIZE = 4 * Integer.BYTES + LiveViewCheckpointPageRef.BYTES;
    private static final int FORMAT_VERSION = 1;
    private final LiveViewCheckpointPageRef partitionMapRootRef = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointMetaSegmentReader reader;
    private int anchorValueType;
    private byte[] keySchema = new byte[0];
    private byte[] windowName = new byte[0];

    public LiveViewCheckpointAnchorRoot(@NotNull CairoConfiguration configuration) {
        reader = new LiveViewCheckpointMetaSegmentReader(configuration);
    }

    @Override
    public void close() {
        Misc.free(reader);
    }

    public int getAnchorValueType() {
        return anchorValueType;
    }

    public byte[] getKeySchema() {
        return keySchema;
    }

    public void getPartitionMapRootRef(@NotNull LiveViewCheckpointPageRef out) {
        out.of(partitionMapRootRef.getSegmentId(), partitionMapRootRef.getOffset(), partitionMapRootRef.getLength());
    }

    public byte[] getWindowName() {
        return windowName;
    }

    public void of(@Transient @NotNull Path checkpointsDir, @NotNull LiveViewCheckpointPageRef rootRef) {
        LiveViewCheckpointMetadata.validateMetaRef(rootRef, false, "anchor root");
        reader.of(checkpointsDir, rootRef.getSegmentId());
        reader.openPage(rootRef);
        if (reader.getPageKind() != PAGE_KIND) {
            throw LiveViewCheckpointMetadata.invalid("anchor root page kind unknown, kind=").put(reader.getPageKind());
        }
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < FIXED_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("anchor root payload too small, length=").put(payloadLength);
        }
        final int version = reader.getInt(0);
        if (version != FORMAT_VERSION) {
            throw LiveViewCheckpointMetadata.invalid("anchor root format version mismatch")
                    .put(" [expected=").put(FORMAT_VERSION).put(", actual=").put(version).put(']');
        }
        anchorValueType = reader.getInt(Integer.BYTES);
        final int windowNameLength = reader.getInt(2L * Integer.BYTES);
        final int keySchemaLength = reader.getInt(3L * Integer.BYTES);
        LiveViewCheckpointMetadata.validateByteArrayLength(windowNameLength, "anchor window name");
        LiveViewCheckpointMetadata.validateByteArrayLength(keySchemaLength, "anchor key schema");
        if (windowNameLength == 0 || keySchemaLength < Integer.BYTES) {
            throw LiveViewCheckpointMetadata.invalid("anchor root window name or key schema invalid")
                    .put(" [windowNameLength=").put(windowNameLength)
                    .put(", keySchemaLength=").put(keySchemaLength).put(']');
        }
        LiveViewCheckpointMetadata.readMetaRef(reader, 4L * Integer.BYTES, partitionMapRootRef);
        LiveViewCheckpointMetadata.validateMetaRef(partitionMapRootRef, true, "anchor partition map root");
        final long expectedLength = (long) FIXED_SIZE + windowNameLength + keySchemaLength;
        if (expectedLength != payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("anchor root payload length mismatch")
                    .put(" [expected=").put(expectedLength).put(", actual=").put(payloadLength).put(']');
        }
        windowName = LiveViewCheckpointMetadata.readBytes(reader, FIXED_SIZE, windowNameLength);
        keySchema = LiveViewCheckpointMetadata.readBytes(reader, FIXED_SIZE + (long) windowNameLength, keySchemaLength);
    }

    /**
     * Decodes one anchor-map entry's last-seen anchor value. An anchor entry
     * owns no data page and carries exactly one value, so both are checked
     * before the value is assembled: the shape comes from a decoded page even
     * though the page itself is checksummed.
     */
    public static long readAnchorValue(@NotNull LiveViewCheckpointPartitionMapEntry entry) {
        if (entry.getStatePageCount() != 0) {
            throw LiveViewCheckpointMetadata.invalid("anchor entry must not reference a state page, pages=")
                    .put(entry.getStatePageCount());
        }
        final byte[] scalarState = entry.getScalarState();
        if (scalarState.length != ENTRY_STATE_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("anchor entry scalar state length invalid, length=")
                    .put(scalarState.length);
        }
        long value = 0;
        for (int i = ENTRY_STATE_SIZE - 1; i >= 0; i--) {
            value = (value << 8) | (scalarState[i] & 0xffL);
        }
        return value;
    }

    static byte[] encodeAnchorValue(long anchorValue) {
        final byte[] scalarState = new byte[ENTRY_STATE_SIZE];
        for (int i = 0; i < ENTRY_STATE_SIZE; i++) {
            scalarState[i] = (byte) (anchorValue >>> (i * Byte.SIZE));
        }
        return scalarState;
    }

    void ofBuilder(
            byte[] windowName,
            int anchorValueType,
            byte[] keySchema,
            LiveViewCheckpointPageRef partitionMapRootRef
    ) {
        this.windowName = Arrays.copyOf(windowName, windowName.length);
        this.anchorValueType = anchorValueType;
        this.keySchema = Arrays.copyOf(keySchema, keySchema.length);
        this.partitionMapRootRef.of(
                partitionMapRootRef.getSegmentId(),
                partitionMapRootRef.getOffset(),
                partitionMapRootRef.getLength()
        );
    }

    void writeTo(@NotNull LiveViewCheckpointMetaSegmentWriter writer, @NotNull LiveViewCheckpointPageRef out) {
        final MemoryA mem = writer.beginPage(PAGE_KIND);
        mem.putInt(FORMAT_VERSION);
        mem.putInt(anchorValueType);
        mem.putInt(windowName.length);
        mem.putInt(keySchema.length);
        LiveViewCheckpointMetadata.putMetaRef(mem, partitionMapRootRef);
        LiveViewCheckpointMetadata.putBytes(mem, windowName);
        LiveViewCheckpointMetadata.putBytes(mem, keySchema);
        writer.endPage(out);
    }
}
