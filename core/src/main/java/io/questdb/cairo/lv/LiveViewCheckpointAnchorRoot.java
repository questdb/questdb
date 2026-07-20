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

/** Checksummed metadata root for the optional anchored-window state page. */
public class LiveViewCheckpointAnchorRoot implements Closeable {

    public static final int PAGE_KIND = 0x1b;
    private static final int FORMAT_VERSION = 1;
    private static final int PAYLOAD_SIZE = Integer.BYTES + LiveViewCheckpointStatePageRef.BYTES;
    private final LiveViewCheckpointMetaSegmentReader reader;
    private final LiveViewCheckpointStatePageRef statePageRef = new LiveViewCheckpointStatePageRef();

    public LiveViewCheckpointAnchorRoot(@NotNull CairoConfiguration configuration) {
        reader = new LiveViewCheckpointMetaSegmentReader(configuration);
    }

    @Override
    public void close() {
        Misc.free(reader);
    }

    public void getStatePageRef(@NotNull LiveViewCheckpointStatePageRef out) {
        copyStateRef(statePageRef, out);
    }

    public void of(@Transient @NotNull Path checkpointsDir, @NotNull LiveViewCheckpointPageRef rootRef) {
        LiveViewCheckpointMetadata.validateMetaRef(rootRef, false, "anchor root");
        reader.of(checkpointsDir, rootRef.getSegmentId());
        reader.openPage(rootRef);
        if (reader.getPageKind() != PAGE_KIND || reader.getPagePayloadLength() != PAYLOAD_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("anchor root kind or length invalid")
                    .put(" [kind=").put(reader.getPageKind())
                    .put(", length=").put(reader.getPagePayloadLength()).put(']');
        }
        final int version = reader.getInt(0);
        if (version != FORMAT_VERSION) {
            throw LiveViewCheckpointMetadata.invalid("anchor root version invalid, version=").put(version);
        }
        statePageRef.readFrom(reader, Integer.BYTES);
        LiveViewCheckpointMetadata.validateStateRef(statePageRef, false, "anchor state");
    }

    public static void writeTo(
            @NotNull LiveViewCheckpointMetaSegmentWriter writer,
            @NotNull LiveViewCheckpointStatePageRef statePageRef,
            @NotNull LiveViewCheckpointPageRef out
    ) {
        LiveViewCheckpointMetadata.validateStateRef(statePageRef, false, "anchor state");
        final MemoryA mem = writer.beginPage(PAGE_KIND);
        mem.putInt(FORMAT_VERSION);
        statePageRef.writeTo(mem);
        writer.endPage(out);
    }

    private static void copyStateRef(LiveViewCheckpointStatePageRef from, LiveViewCheckpointStatePageRef to) {
        to.of(
                from.getSegmentId(),
                from.getOffset(),
                from.getStoredLength(),
                from.getDecodedLength(),
                from.getPageKind(),
                from.getCodec(),
                from.getRowCount(),
                from.getFlags()
        );
    }
}
