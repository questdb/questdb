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
 * takes. Both the seal and the restore run that comparison as a plain
 * {@code Arrays.equals} over the encoded bytes: a decoder locates every field of a
 * fused entry by offset alone, so no weaker test can tell a difference it may tolerate
 * from one that silently shifts every field after it.
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
 *     codecVersion: INT
 *     stateOffset: INT
 *     stateLength: INT
 *     identityLength: INT
 *     identity: identityLength bytes
 * </pre>
 * A component carries no storage-kind discriminator. Every component this format
 * admits inlines into the leaf's scalar slot - {@code readWindowState} rejects a fused
 * entry that references a state page at all - so a per-component kind field would have
 * one writable value and no reader. A later format that grows a second storage kind
 * adds the field under {@link #FORMAT_VERSION} 2, which is cheaper than carrying four
 * dead bytes per component through every root until then.
 */
public final class LiveViewWindowStateManifest {
    public static final int FORMAT_VERSION = 1;
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

    /**
     * Borrows the immutable compiled manifest. The containing window-state plan owns
     * this array; package-internal callers must not mutate or outlive that plan.
     */
    byte[] borrowEncoded() {
        return encoded;
    }

    /**
     * The width of a complete fused leaf scalar payload, anchor value included. A
     * restore requires the entry's scalar to be exactly this long and to name no state
     * page.
     */
    public int getTotalInlineStateBytes() {
        return totalInlineStateBytes;
    }

    private byte[] encode(ObjList<LiveViewAccumulatorDescriptor> components, IntList componentOffsets) {
        int size = 6 * Integer.BYTES;
        for (int i = 0; i < componentCount; i++) {
            size += 4 * Integer.BYTES + components.getQuick(i).borrowEncoded().length;
        }
        final byte[] encoded = new byte[size];
        int offset = LiveViewCheckpointMetadata.putInt(encoded, 0, MAGIC);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, FORMAT_VERSION);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, totalInlineStateBytes);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, anchorStateOffset);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, anchorStateLength);
        offset = LiveViewCheckpointMetadata.putInt(encoded, offset, componentCount);
        for (int i = 0; i < componentCount; i++) {
            final LiveViewAccumulatorDescriptor component = components.getQuick(i);
            final byte[] identity = component.borrowEncoded();
            offset = LiveViewCheckpointMetadata.putInt(encoded, offset, component.getCodecVersion());
            offset = LiveViewCheckpointMetadata.putInt(encoded, offset, componentOffsets.getQuick(i));
            offset = LiveViewCheckpointMetadata.putInt(encoded, offset, component.getStateLength());
            offset = LiveViewCheckpointMetadata.putInt(encoded, offset, identity.length);
            offset = LiveViewCheckpointMetadata.putBytes(encoded, offset, identity);
        }
        return encoded;
    }
}
