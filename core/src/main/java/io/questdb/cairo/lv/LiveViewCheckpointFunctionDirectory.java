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

/**
 * Sorted, checksummed mapping from compiler-stable function identity to root.
 */
public class LiveViewCheckpointFunctionDirectory implements Closeable {

    public static final int PAGE_KIND = 0x19;
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_SIZE = 2 * Integer.BYTES;
    private byte[][] identities = new byte[0][];
    private final LiveViewCheckpointMetaSegmentReader reader;
    private LiveViewCheckpointPageRef[] rootRefs = new LiveViewCheckpointPageRef[0];

    public LiveViewCheckpointFunctionDirectory(@NotNull CairoConfiguration configuration) {
        reader = new LiveViewCheckpointMetaSegmentReader(configuration);
    }

    @Override
    public void close() {
        Misc.free(reader);
    }

    public boolean find(@NotNull byte[] identity, @NotNull LiveViewCheckpointPageRef out) {
        int lo = 0;
        int hi = identities.length;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final int cmp = LiveViewCheckpointMetadata.compareBytes(identities[mid], identity);
            if (cmp < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo == identities.length || LiveViewCheckpointMetadata.compareBytes(identities[lo], identity) != 0) {
            return false;
        }
        final LiveViewCheckpointPageRef ref = rootRefs[lo];
        out.of(ref.getSegmentId(), ref.getOffset(), ref.getLength());
        return true;
    }

    public byte[] getIdentity(int index) {
        return identities[index];
    }

    public void getRootRef(int index, @NotNull LiveViewCheckpointPageRef out) {
        final LiveViewCheckpointPageRef ref = rootRefs[index];
        out.of(ref.getSegmentId(), ref.getOffset(), ref.getLength());
    }

    public void of(@Transient @NotNull Path checkpointsDir, @NotNull LiveViewCheckpointPageRef rootRef) {
        LiveViewCheckpointMetadata.validateMetaRef(rootRef, false, "function directory");
        reader.of(checkpointsDir, rootRef.getSegmentId());
        reader.openPage(rootRef);
        if (reader.getPageKind() != PAGE_KIND) {
            throw LiveViewCheckpointMetadata.invalid("function directory page kind unknown, kind=").put(reader.getPageKind());
        }
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < HEADER_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("function directory payload too small, length=").put(payloadLength);
        }
        final int version = reader.getInt(0);
        final int count = reader.getInt(Integer.BYTES);
        // Zero entries is a directory, not a truncation: a view whose every window function
        // is stateless writes one, and the header alone is its whole payload.
        if (version != FORMAT_VERSION || count < 0 || count > LiveViewCheckpointMetadata.MAX_ENTRY_COUNT) {
            throw LiveViewCheckpointMetadata.invalid("function directory version or count invalid")
                    .put(" [version=").put(version).put(", count=").put(count).put(']');
        }
        if ((long) HEADER_SIZE + (long) count * (Integer.BYTES + 1 + LiveViewCheckpointPageRef.BYTES) > payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("function directory count exceeds payload, count=")
                    .put(count);
        }
        identities = new byte[count][];
        rootRefs = new LiveViewCheckpointPageRef[count];
        long offset = HEADER_SIZE;
        byte[] previous = null;
        for (int i = 0; i < count; i++) {
            if (offset > payloadLength - Integer.BYTES) {
                throw LiveViewCheckpointMetadata.invalid("function directory entry header truncated");
            }
            final int identityLength = reader.getInt(offset);
            LiveViewCheckpointMetadata.validateByteArrayLength(identityLength, "function identity");
            if (identityLength == 0 || offset + Integer.BYTES + (long) identityLength + LiveViewCheckpointPageRef.BYTES > payloadLength) {
                throw LiveViewCheckpointMetadata.invalid("function directory entry body truncated");
            }
            offset += Integer.BYTES;
            final byte[] identity = LiveViewCheckpointMetadata.readBytes(reader, offset, identityLength);
            offset += identityLength;
            if (previous != null && LiveViewCheckpointMetadata.compareBytes(previous, identity) >= 0) {
                throw LiveViewCheckpointMetadata.invalid("function directory identities not strictly increasing");
            }
            final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
            LiveViewCheckpointMetadata.readMetaRef(reader, offset, functionRootRef);
            LiveViewCheckpointMetadata.validateMetaRef(functionRootRef, false, "function directory entry");
            offset += LiveViewCheckpointPageRef.BYTES;
            identities[i] = identity;
            rootRefs[i] = functionRootRef;
            previous = identity;
        }
        if (offset != payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("function directory payload has trailing bytes");
        }
    }

    public int size() {
        return identities.length;
    }

    static void writeTo(
            byte[][] identities,
            LiveViewCheckpointPageRef[] rootRefs,
            int count,
            LiveViewCheckpointMetaSegmentWriter writer,
            LiveViewCheckpointPageRef out
    ) {
        final MemoryA mem = writer.beginPage(PAGE_KIND);
        mem.putInt(FORMAT_VERSION);
        mem.putInt(count);
        for (int i = 0; i < count; i++) {
            mem.putInt(identities[i].length);
            LiveViewCheckpointMetadata.putBytes(mem, identities[i]);
            LiveViewCheckpointMetadata.putMetaRef(mem, rootRefs[i]);
        }
        writer.endPage(out);
    }
}
