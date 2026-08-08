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

import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The persisted description of a fused window state's scalar layout: one ordered
 * component list, written once per window-state root rather than once per partition.
 * <p>
 * A fused leaf entry carries no per-partition version and no component tags. It is a
 * flat run of bytes whose meaning comes entirely from the manifest of the root that
 * names it, which makes the manifest the single thing a decoder has to agree with -
 * and makes disagreement a silent misread rather than a rejection, since the decoder
 * finds the total length it expects and reads the wrong fields out of it.
 *
 * <h2>Why byte equality is the compatibility test</h2>
 * Partition-map leaves are shared copy-on-write across generations, so an incremental
 * seal publishes a new manifest over leaves an older manifest wrote. A recompile can
 * change the manifest without changing {@code definitionTxn} - a component codec
 * version bump across a binary upgrade does exactly that - so the {@code definitionTxn}
 * guard the reader already applies is not sufficient cover. The writer therefore
 * compares the compiled manifest byte-for-byte against the predecessor root's and, on
 * any difference at all, takes the full-scan conversion path a legacy predecessor
 * takes. That is why {@link #isByteEqual} exists and why nothing weaker is offered.
 * <p>
 * The window identity, key schema and anchor value type are <b>not</b> in here: they
 * are the root's own fields, and predecessor compatibility is all four checks
 * together, not this one alone.
 *
 * <h2>Encoded form</h2>
 * <pre>
 *   magic: INT
 *   formatVersion: INT
 *   totalInlineStateBytes: INT   (anchor value included)
 *   anchorStateOffset: INT
 *   anchorStateLength: INT
 *   componentCount: INT
 *   per component, in the plan's canonical order:
 *     storageKind: INT
 *     codecVersion: INT
 *     stateOffset: INT
 *     stateLength: INT
 *     identityLength: INT
 *     identity: identityLength bytes
 * </pre>
 */
public final class LiveViewWindowStateManifest {
    public static final int FORMAT_VERSION = 1;
    /**
     * The component's state lives in the partition-map leaf's scalar slot. The only
     * storage kind the first fused release writes.
     */
    public static final int STORAGE_KIND_INLINE = 1;
    /**
     * Reserved for the combined overflow state page a later format may add for a group
     * that cannot fit the inline budget. Never written today; named so a decoder that
     * meets it reports an unsupported storage kind rather than an unknown integer.
     */
    public static final int STORAGE_KIND_PAGE = 2;
    private static final int MAGIC = 0x4c56574d; // LVWM
    private final int anchorStateLength;
    private final int anchorStateOffset;
    private final int componentCount;
    private final int[] componentStateOffsets;
    private final byte[] encoded;
    private final int totalInlineStateBytes;

    /**
     * @param components            the plan's components, already in canonical
     *                              identity order
     * @param componentOffsets      each component's offset in the fused scalar
     *                              payload, index-aligned with {@code components}
     * @param anchorStateOffset     where the anchor value sits; zero today
     * @param anchorStateLength     how wide the anchor value is
     * @param totalInlineStateBytes the whole scalar payload's width, anchor included
     */
    public LiveViewWindowStateManifest(
            @NotNull ObjList<LiveViewAccumulatorDescriptor> components,
            @NotNull IntList componentOffsets,
            int anchorStateOffset,
            int anchorStateLength,
            int totalInlineStateBytes
    ) {
        if (components.size() != componentOffsets.size()) {
            throw new IllegalArgumentException("live view window state manifest component/offset count mismatch");
        }
        this.componentCount = components.size();
        this.anchorStateOffset = anchorStateOffset;
        this.anchorStateLength = anchorStateLength;
        this.totalInlineStateBytes = totalInlineStateBytes;
        this.componentStateOffsets = new int[componentCount];
        for (int i = 0; i < componentCount; i++) {
            componentStateOffsets[i] = componentOffsets.getQuick(i);
        }
        this.encoded = encode(components, componentOffsets);
        LiveViewCheckpointMetadata.validateByteArrayLength(encoded.length, "window state manifest");
    }

    public int getAnchorStateLength() {
        return anchorStateLength;
    }

    public int getAnchorStateOffset() {
        return anchorStateOffset;
    }

    public int getComponentCount() {
        return componentCount;
    }

    /**
     * Returns component {@code index}'s offset in the fused scalar payload, in the plan's
     * canonical identity order. This is where a seal writes that component's whole-state
     * image and where a restore slices it back out.
     */
    public int getComponentStateOffset(int index) {
        return componentStateOffsets[index];
    }

    /**
     * Returns an owned copy of the encoded manifest, which is what a window root
     * carries and what the predecessor comparison reads.
     */
    public byte[] getEncoded() {
        return Arrays.copyOf(encoded, encoded.length);
    }

    public int getEncodedLength() {
        return encoded.length;
    }

    /**
     * The width of a complete fused leaf scalar payload, anchor value included. A
     * restore requires the entry's scalar to be exactly this long and to name no state
     * page.
     */
    public int getTotalInlineStateBytes() {
        return totalInlineStateBytes;
    }

    /**
     * The predecessor-compatibility test. Any difference at all - a reordered
     * component, a codec version bump, a changed argument - forces the full-scan
     * conversion seal, because an incremental seal across differing manifests would
     * publish the new layout over leaves the old one wrote.
     */
    public boolean isByteEqual(LiveViewWindowStateManifest other) {
        return other != null && Arrays.equals(encoded, other.encoded);
    }

    private byte[] encode(ObjList<LiveViewAccumulatorDescriptor> components, IntList componentOffsets) {
        int size = 6 * Integer.BYTES;
        for (int i = 0; i < componentCount; i++) {
            size += 5 * Integer.BYTES + components.getQuick(i).getEncoded().length;
        }
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(MAGIC);
        buffer.putInt(FORMAT_VERSION);
        buffer.putInt(totalInlineStateBytes);
        buffer.putInt(anchorStateOffset);
        buffer.putInt(anchorStateLength);
        buffer.putInt(componentCount);
        for (int i = 0; i < componentCount; i++) {
            final LiveViewAccumulatorDescriptor component = components.getQuick(i);
            final byte[] identity = component.getEncoded();
            buffer.putInt(STORAGE_KIND_INLINE);
            buffer.putInt(component.getCodecVersion());
            buffer.putInt(componentOffsets.getQuick(i));
            buffer.putInt(component.getStateLength());
            buffer.putInt(identity.length);
            buffer.put(identity);
        }
        return buffer.array();
    }
}
