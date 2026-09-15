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

import io.questdb.std.IntObjHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Value stored in a persistent checkpoint partition map. The encoded key is
 * ordered byte-for-byte, the scalar payload is function-owned checksummed
 * metadata, and every large state payload is reached through a checksummed
 * {@link LiveViewCheckpointStatePageRef}.
 * <p>
 * This is a mutable flyweight. Public setters copy their inputs so a checkpoint
 * candidate cannot be changed after validation by mutating a caller-owned array.
 */
public final class LiveViewCheckpointPartitionMapEntry {

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final LiveViewCheckpointStatePageRef[] EMPTY_REFS = new LiveViewCheckpointStatePageRef[0];
    private final IntObjHashMap<byte[]> keyBuffers = new IntObjHashMap<>();
    private final IntObjHashMap<LiveViewCheckpointStatePageRef[]> refBuffers = new IntObjHashMap<>();
    private final IntObjHashMap<byte[]> scalarBuffers = new IntObjHashMap<>();
    private byte[] key = EMPTY_BYTES;
    private byte[] scalarState = EMPTY_BYTES;
    private LiveViewCheckpointStatePageRef[] statePageRefs = EMPTY_REFS;
    private int widthLookupCountForTest;

    public LiveViewCheckpointPartitionMapEntry clear() {
        key = EMPTY_BYTES;
        scalarState = EMPTY_BYTES;
        statePageRefs = EMPTY_REFS;
        return this;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getScalarState() {
        return scalarState;
    }

    public int getStatePageCount() {
        return statePageRefs.length;
    }

    @TestOnly
    public int getWidthLookupCountForTest() {
        return widthLookupCountForTest;
    }

    public LiveViewCheckpointStatePageRef getStatePageRef(int index) {
        return statePageRefs[index];
    }

    @TestOnly
    public void resetWidthLookupCountForTest() {
        widthLookupCountForTest = 0;
    }

    public LiveViewCheckpointPartitionMapEntry of(
            @NotNull byte[] key,
            @NotNull byte[] scalarState,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        this.key = copyBytes(key, keyBuffers);
        this.scalarState = copyBytes(scalarState, scalarBuffers);
        this.statePageRefs = copyRefsPooled(statePageRefs);
        return this;
    }

    void copyFrom(@NotNull LiveViewCheckpointPartitionMapEntry other) {
        if (other != this) {
            of(other.key, other.scalarState, other.statePageRefs);
        }
    }

    void ofDecoded(byte[] key, byte[] scalarState, LiveViewCheckpointStatePageRef[] statePageRefs) {
        this.key = key;
        this.scalarState = scalarState;
        this.statePageRefs = statePageRefs;
    }

    static LiveViewCheckpointStatePageRef copyRef(LiveViewCheckpointStatePageRef source) {
        return new LiveViewCheckpointStatePageRef().of(
                source.getSegmentId(), source.getOffset(), source.getStoredLength(), source.getDecodedLength(),
                source.getPageKind(), source.getCodec(), source.getRowCount(), source.getFlags()
        );
    }

    private byte[] copyBytes(byte[] source, IntObjHashMap<byte[]> buffers) {
        if (source.length == 0) {
            return EMPTY_BYTES;
        }
        assert isWidthLookupRecordedForTest();
        byte[] target = buffers.get(source.length);
        if (target == null) {
            target = new byte[source.length];
            buffers.put(source.length, target);
        }
        System.arraycopy(source, 0, target, 0, source.length);
        return target;
    }

    private LiveViewCheckpointStatePageRef[] copyRefsPooled(LiveViewCheckpointStatePageRef[] source) {
        if (source.length == 0) {
            return EMPTY_REFS;
        }
        assert isWidthLookupRecordedForTest();
        LiveViewCheckpointStatePageRef[] target = refBuffers.get(source.length);
        if (target == null) {
            target = new LiveViewCheckpointStatePageRef[source.length];
            for (int i = 0; i < target.length; i++) {
                target[i] = new LiveViewCheckpointStatePageRef();
            }
            refBuffers.put(source.length, target);
        }
        for (int i = 0; i < source.length; i++) {
            final LiveViewCheckpointStatePageRef from = source[i];
            target[i].of(
                    from.getSegmentId(), from.getOffset(), from.getStoredLength(), from.getDecodedLength(),
                    from.getPageKind(), from.getCodec(), from.getRowCount(), from.getFlags()
            );
        }
        return target;
    }

    private boolean isWidthLookupRecordedForTest() {
        widthLookupCountForTest++;
        return true;
    }

    static boolean refsEqual(LiveViewCheckpointStatePageRef[] left, LiveViewCheckpointStatePageRef[] right) {
        if (left.length != right.length) {
            return false;
        }
        for (int i = 0; i < left.length; i++) {
            final LiveViewCheckpointStatePageRef a = left[i];
            final LiveViewCheckpointStatePageRef b = right[i];
            if (a.getSegmentId() != b.getSegmentId() || a.getOffset() != b.getOffset()
                    || a.getStoredLength() != b.getStoredLength() || a.getDecodedLength() != b.getDecodedLength()
                    || a.getPageKind() != b.getPageKind() || a.getCodec() != b.getCodec()
                    || a.getRowCount() != b.getRowCount() || a.getFlags() != b.getFlags()) {
                return false;
            }
        }
        return true;
    }

    LiveViewCheckpointStatePageRef[] statePageRefs() {
        return statePageRefs;
    }
}
