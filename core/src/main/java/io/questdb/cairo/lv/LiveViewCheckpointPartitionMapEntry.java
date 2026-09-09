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

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

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
    private byte[] key = EMPTY_BYTES;
    private byte[] scalarState = EMPTY_BYTES;
    private LiveViewCheckpointStatePageRef[] statePageRefs = EMPTY_REFS;

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

    public LiveViewCheckpointStatePageRef getStatePageRef(int index) {
        return statePageRefs[index];
    }

    public LiveViewCheckpointPartitionMapEntry of(
            @NotNull byte[] key,
            @NotNull byte[] scalarState,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        this.key = Arrays.copyOf(key, key.length);
        this.scalarState = Arrays.copyOf(scalarState, scalarState.length);
        this.statePageRefs = copyRefs(statePageRefs);
        return this;
    }

    void copyFrom(@NotNull LiveViewCheckpointPartitionMapEntry other) {
        key = Arrays.copyOf(other.key, other.key.length);
        scalarState = Arrays.copyOf(other.scalarState, other.scalarState.length);
        statePageRefs = copyRefs(other.statePageRefs);
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

    static LiveViewCheckpointStatePageRef[] copyRefs(LiveViewCheckpointStatePageRef[] source) {
        if (source.length == 0) {
            return EMPTY_REFS;
        }
        final LiveViewCheckpointStatePageRef[] copy = new LiveViewCheckpointStatePageRef[source.length];
        for (int i = 0; i < source.length; i++) {
            copy[i] = copyRef(source[i]);
        }
        return copy;
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
