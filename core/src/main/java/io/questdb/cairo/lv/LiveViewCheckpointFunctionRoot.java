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
 * Checksummed metadata root for one compiler-identified window function. The
 * per-segment use counts let a builder update the root's complete data-segment
 * set from changed partitions without walking the whole persistent map.
 */
public class LiveViewCheckpointFunctionRoot implements Closeable {

    public static final int PAGE_KIND = 0x18;
    private static final int FIXED_SIZE = 5 * Integer.BYTES
            + LiveViewCheckpointStatePageRef.BYTES + LiveViewCheckpointPageRef.BYTES;
    private static final int FORMAT_VERSION = 1;
    private final LiveViewCheckpointMetaSegmentReader reader;
    private byte[] functionIdentity = new byte[0];
    private byte[] keySchema = new byte[0];
    private final LiveViewCheckpointPageRef partitionMapRootRef = new LiveViewCheckpointPageRef();
    private long[] segmentIds = new long[0];
    private long[] segmentUseCounts = new long[0];
    private final LiveViewCheckpointStatePageRef scalarStateRef = new LiveViewCheckpointStatePageRef();
    private int stateFormatVersion;

    public LiveViewCheckpointFunctionRoot(@NotNull CairoConfiguration configuration) {
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

    public byte[] getFunctionIdentity() {
        return functionIdentity;
    }

    public byte[] getKeySchema() {
        return keySchema;
    }

    public void getPartitionMapRootRef(@NotNull LiveViewCheckpointPageRef out) {
        out.of(partitionMapRootRef.getSegmentId(), partitionMapRootRef.getOffset(), partitionMapRootRef.getLength());
    }

    public long getSegmentId(int index) {
        return segmentIds[index];
    }

    public long getSegmentUseCount(int index) {
        return segmentUseCounts[index];
    }

    public int getSegmentUseCountSize() {
        return segmentIds.length;
    }

    public void getScalarStateRef(@NotNull LiveViewCheckpointStatePageRef out) {
        final LiveViewCheckpointStatePageRef ref = scalarStateRef;
        out.of(ref.getSegmentId(), ref.getOffset(), ref.getStoredLength(), ref.getDecodedLength(),
                ref.getPageKind(), ref.getCodec(), ref.getRowCount(), ref.getFlags());
    }

    public int getStateFormatVersion() {
        return stateFormatVersion;
    }

    public void of(@Transient @NotNull Path checkpointsDir, @NotNull LiveViewCheckpointPageRef rootRef) {
        LiveViewCheckpointMetadata.validateMetaRef(rootRef, false, "function root");
        reader.of(checkpointsDir, rootRef.getSegmentId());
        reader.openPage(rootRef);
        if (reader.getPageKind() != PAGE_KIND) {
            throw LiveViewCheckpointMetadata.invalid("function root page kind unknown, kind=").put(reader.getPageKind());
        }
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < FIXED_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("function root payload too small, length=").put(payloadLength);
        }
        final int version = reader.getInt(0);
        if (version != FORMAT_VERSION) {
            throw LiveViewCheckpointMetadata.invalid("function root format version mismatch")
                    .put(" [expected=").put(FORMAT_VERSION).put(", actual=").put(version).put(']');
        }
        stateFormatVersion = reader.getInt(Integer.BYTES);
        if (stateFormatVersion <= 0) {
            throw LiveViewCheckpointMetadata.invalid("function state format version invalid, version=").put(stateFormatVersion);
        }
        final int identityLength = reader.getInt(2L * Integer.BYTES);
        final int keySchemaLength = reader.getInt(3L * Integer.BYTES);
        final int segmentCount = reader.getInt(4L * Integer.BYTES);
        LiveViewCheckpointMetadata.validateByteArrayLength(identityLength, "function identity");
        LiveViewCheckpointMetadata.validateByteArrayLength(keySchemaLength, "function key schema");
        if (identityLength == 0 || segmentCount < 0 || segmentCount > LiveViewCheckpointMetadata.MAX_ENTRY_COUNT) {
            throw LiveViewCheckpointMetadata.invalid("function root identity or segment count invalid")
                    .put(" [identityLength=").put(identityLength).put(", segmentCount=").put(segmentCount).put(']');
        }
        scalarStateRef.readFrom(reader, 5L * Integer.BYTES);
        LiveViewCheckpointMetadata.validateStateRef(scalarStateRef, true, "function scalar");
        LiveViewCheckpointMetadata.readMetaRef(
                reader,
                5L * Integer.BYTES + LiveViewCheckpointStatePageRef.BYTES,
                partitionMapRootRef
        );
        LiveViewCheckpointMetadata.validateMetaRef(partitionMapRootRef, true, "function partition map root");
        final long expectedLength = (long) FIXED_SIZE + identityLength + keySchemaLength + (long) segmentCount * 2 * Long.BYTES;
        if (expectedLength != payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("function root payload length mismatch")
                    .put(" [expected=").put(expectedLength).put(", actual=").put(payloadLength).put(']');
        }
        long offset = FIXED_SIZE;
        functionIdentity = LiveViewCheckpointMetadata.readBytes(reader, offset, identityLength);
        offset += identityLength;
        keySchema = LiveViewCheckpointMetadata.readBytes(reader, offset, keySchemaLength);
        offset += keySchemaLength;
        segmentIds = new long[segmentCount];
        segmentUseCounts = new long[segmentCount];
        long previous = -1;
        boolean scalarFound = scalarStateRef.isNull();
        for (int i = 0; i < segmentCount; i++) {
            final long segmentId = reader.getLong(offset);
            final long useCount = reader.getLong(offset + Long.BYTES);
            if (segmentId < 0 || segmentId <= previous || useCount <= 0) {
                throw LiveViewCheckpointMetadata.invalid("function root segment catalogue invalid")
                        .put(" [segmentId=").put(segmentId).put(", previous=").put(previous)
                        .put(", useCount=").put(useCount).put(']');
            }
            segmentIds[i] = segmentId;
            segmentUseCounts[i] = useCount;
            scalarFound |= segmentId == scalarStateRef.getSegmentId();
            previous = segmentId;
            offset += 2L * Long.BYTES;
        }
        if (!scalarFound) {
            throw LiveViewCheckpointMetadata.invalid("function scalar segment missing from root catalogue, segmentId=")
                    .put(scalarStateRef.getSegmentId());
        }
    }

    void clearBorrowedCompiled() {
        functionIdentity = null;
        keySchema = null;
    }

    @TestOnly
    boolean isBorrowingCompiledForTest(byte[] functionIdentity, byte[] keySchema) {
        return this.functionIdentity == functionIdentity && this.keySchema == keySchema;
    }

    void ofBuilder(
            byte[] functionIdentity,
            int stateFormatVersion,
            byte[] keySchema,
            LiveViewCheckpointStatePageRef scalarStateRef,
            LiveViewCheckpointPageRef partitionMapRootRef,
            LongList segmentUseCounts
    ) {
        this.functionIdentity = functionIdentity;
        this.stateFormatVersion = stateFormatVersion;
        this.keySchema = keySchema;
        this.scalarStateRef.of(scalarStateRef.getSegmentId(), scalarStateRef.getOffset(), scalarStateRef.getStoredLength(),
                scalarStateRef.getDecodedLength(), scalarStateRef.getPageKind(), scalarStateRef.getCodec(),
                scalarStateRef.getRowCount(), scalarStateRef.getFlags());
        this.partitionMapRootRef.of(partitionMapRootRef.getSegmentId(), partitionMapRootRef.getOffset(), partitionMapRootRef.getLength());
        final int count = segmentUseCounts.size() / 2;
        segmentIds = new long[count];
        this.segmentUseCounts = new long[count];
        for (int i = 0; i < count; i++) {
            segmentIds[i] = segmentUseCounts.getQuick(i * 2);
            this.segmentUseCounts[i] = segmentUseCounts.getQuick(i * 2 + 1);
        }
    }

    void writeTo(@NotNull LiveViewCheckpointMetaSegmentWriter writer, @NotNull LiveViewCheckpointPageRef out) {
        final MemoryA mem = writer.beginPage(PAGE_KIND);
        mem.putInt(FORMAT_VERSION);
        mem.putInt(stateFormatVersion);
        mem.putInt(functionIdentity.length);
        mem.putInt(keySchema.length);
        mem.putInt(segmentIds.length);
        scalarStateRef.writeTo(mem);
        LiveViewCheckpointMetadata.putMetaRef(mem, partitionMapRootRef);
        LiveViewCheckpointMetadata.putBytes(mem, functionIdentity);
        LiveViewCheckpointMetadata.putBytes(mem, keySchema);
        for (int i = 0; i < segmentIds.length; i++) {
            mem.putLong(segmentIds[i]);
            mem.putLong(segmentUseCounts[i]);
        }
        writer.endPage(out);
    }
}
