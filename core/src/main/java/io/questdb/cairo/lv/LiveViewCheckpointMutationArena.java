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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Reusable columnar staging for one partition-map build. Variable-width key,
 * scalar, and state-reference bytes live in one tracker-bound native arena;
 * fixed-width descriptors and sort ordinals use tracker-bound primitive lists.
 */
public final class LiveViewCheckpointMutationArena implements Closeable {

    static final int OP_DOMAIN = 2;
    static final int OP_PUT = 0;
    static final int OP_REMOVE = 1;
    private static final int DESC_KEY_LENGTH = 2;
    private static final int DESC_KEY_OFFSET = 1;
    private static final int DESC_LONGS = 7;
    private static final int DESC_OPERATION = 0;
    private static final int DESC_REF_COUNT = 6;
    private static final int DESC_REF_OFFSET = 5;
    private static final int DESC_SCALAR_LENGTH = 4;
    private static final int DESC_SCALAR_OFFSET = 3;
    private static final int INITIAL_LONG_CAPACITY = 64;
    private static final int MAX_PAGES = 524_288;
    private static final long PAGE_SIZE = 4096;
    private final MemoryCARWImpl bytes = new MemoryCARWImpl(PAGE_SIZE, MAX_PAGES, MemoryTag.NATIVE_DEFAULT);
    private final DirectLongList descriptors = new DirectLongList(INITIAL_LONG_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
    private final DirectLongList ordinals = new DirectLongList(INITIAL_LONG_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
    private final LiveViewCheckpointStatePageRef otherStateRefFlyweight = new LiveViewCheckpointStatePageRef();
    private final LiveViewCheckpointStatePageRef stateRefFlyweight = new LiveViewCheckpointStatePageRef();
    private int lowerBoundCountForTest;
    private int size;
    private int sortedSize;

    public LiveViewCheckpointMutationArena() {
        this(null);
    }

    public LiveViewCheckpointMutationArena(@Nullable MemoryTracker memoryTracker) {
        bytes.setMemoryTracker(memoryTracker);
        descriptors.setMemoryTracker(memoryTracker);
        ordinals.setMemoryTracker(memoryTracker);
    }

    public void clear() {
        if (bytes.getAppendOffset() > 0) {
            bytes.jumpTo(0);
        }
        descriptors.clear();
        ordinals.clear();
        size = 0;
        sortedSize = 0;
    }

    @Override
    public void close() {
        Misc.free(bytes);
        Misc.free(descriptors);
        Misc.free(ordinals);
        size = 0;
        sortedSize = 0;
    }

    public void domain(@NotNull byte[] key) {
        append(OP_DOMAIN, key, null, null);
    }

    public int getMutationCount() {
        return size;
    }

    @TestOnly
    public int getLowerBoundCountForTest() {
        return lowerBoundCountForTest;
    }

    @TestOnly
    public void resetLowerBoundCountForTest() {
        lowerBoundCountForTest = 0;
    }

    public int getSortedMutationIndex(int sortedIndex) {
        return (int) ordinals.get(sortedIndex);
    }

    @TestOnly
    public int compareSortedKeysForTest(int leftSortedIndex, int rightSortedIndex) {
        return compareKey(getSortedMutationIndex(leftSortedIndex), getSortedMutationIndex(rightSortedIndex));
    }

    @TestOnly
    public int sortAndValidateForTest() {
        return sortAndValidate();
    }

    public void put(
            @NotNull byte[] key,
            @NotNull byte[] scalarState,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        append(OP_PUT, key, scalarState, statePageRefs);
    }

    public void put(@NotNull byte[] key, @NotNull byte[] scalarState) {
        append(OP_PUT, key, scalarState, null);
    }

    public void putAnchor(@NotNull byte[] key, long anchorValue) {
        ensureOpen();
        LiveViewCheckpointMetadata.validateByteArrayLength(key.length, "partition key");
        final long keyOffset = appendBytes(key);
        final long scalarOffset = bytes.getAppendOffset();
        for (int i = 0; i < LiveViewCheckpointAnchorRoot.ENTRY_STATE_SIZE; i++) {
            bytes.putByte((byte) (anchorValue >>> (i * Byte.SIZE)));
        }
        appendDescriptor(
                OP_PUT,
                keyOffset,
                key.length,
                scalarOffset,
                LiveViewCheckpointAnchorRoot.ENTRY_STATE_SIZE,
                bytes.getAppendOffset(),
                0
        );
    }

    public void remove(@NotNull byte[] key) {
        append(OP_REMOVE, key, null, null);
    }

    int compareKey(int leftMutationIndex, int rightMutationIndex) {
        final int leftLength = keyLength(leftMutationIndex);
        final int rightLength = keyLength(rightMutationIndex);
        final int n = Math.min(leftLength, rightLength);
        final long leftOffset = keyOffset(leftMutationIndex);
        final long rightOffset = keyOffset(rightMutationIndex);
        for (int i = 0; i < n; i++) {
            final int left = bytes.getByte(leftOffset + i) & 0xff;
            final int right = bytes.getByte(rightOffset + i) & 0xff;
            if (left != right) {
                return left < right ? -1 : 1;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    int compareKey(int mutationIndex, byte[] key) {
        final int leftLength = keyLength(mutationIndex);
        final int n = Math.min(leftLength, key.length);
        final long leftOffset = keyOffset(mutationIndex);
        for (int i = 0; i < n; i++) {
            final int left = bytes.getByte(leftOffset + i) & 0xff;
            final int right = key[i] & 0xff;
            if (left != right) {
                return left < right ? -1 : 1;
            }
        }
        return Integer.compare(leftLength, key.length);
    }

    int compareKey(int mutationIndex, LiveViewCheckpointMutationArena other, int otherMutationIndex) {
        final int leftLength = keyLength(mutationIndex);
        final int rightLength = other.keyLength(otherMutationIndex);
        final int n = Math.min(leftLength, rightLength);
        final long leftOffset = keyOffset(mutationIndex);
        final long rightOffset = other.keyOffset(otherMutationIndex);
        for (int i = 0; i < n; i++) {
            final int left = bytes.getByte(leftOffset + i) & 0xff;
            final int right = other.bytes.getByte(rightOffset + i) & 0xff;
            if (left != right) {
                return left < right ? -1 : 1;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    boolean equalsScalar(int mutationIndex, byte[] scalarState) {
        final int length = scalarLength(mutationIndex);
        if (length != scalarState.length) {
            return false;
        }
        final long offset = scalarOffset(mutationIndex);
        for (int i = 0; i < length; i++) {
            if (bytes.getByte(offset + i) != scalarState[i]) {
                return false;
            }
        }
        return true;
    }

    boolean equalsScalar(int mutationIndex, LiveViewCheckpointMutationArena other, int otherMutationIndex) {
        final int length = scalarLength(mutationIndex);
        if (length != other.scalarLength(otherMutationIndex)) {
            return false;
        }
        final long offset = scalarOffset(mutationIndex);
        final long otherOffset = other.scalarOffset(otherMutationIndex);
        for (int i = 0; i < length; i++) {
            if (bytes.getByte(offset + i) != other.bytes.getByte(otherOffset + i)) {
                return false;
            }
        }
        return true;
    }

    boolean refsEqual(int mutationIndex, LiveViewCheckpointStatePageRef[] refs) {
        final int count = refCount(mutationIndex);
        if (count != refs.length) {
            return false;
        }
        for (int i = 0; i < count; i++) {
            refAt(mutationIndex, i, stateRefFlyweight);
            final LiveViewCheckpointStatePageRef other = refs[i];
            if (stateRefFlyweight.getSegmentId() != other.getSegmentId()
                    || stateRefFlyweight.getOffset() != other.getOffset()
                    || stateRefFlyweight.getStoredLength() != other.getStoredLength()
                    || stateRefFlyweight.getDecodedLength() != other.getDecodedLength()
                    || stateRefFlyweight.getPageKind() != other.getPageKind()
                    || stateRefFlyweight.getCodec() != other.getCodec()
                    || stateRefFlyweight.getRowCount() != other.getRowCount()
                    || stateRefFlyweight.getFlags() != other.getFlags()) {
                return false;
            }
        }
        return true;
    }

    boolean refsEqual(int mutationIndex, LiveViewCheckpointMutationArena other, int otherMutationIndex) {
        final int count = refCount(mutationIndex);
        if (count != other.refCount(otherMutationIndex)) {
            return false;
        }
        final LiveViewCheckpointStatePageRef otherRef = otherStateRefFlyweight;
        for (int i = 0; i < count; i++) {
            refAt(mutationIndex, i, stateRefFlyweight);
            other.refAt(otherMutationIndex, i, otherRef);
            if (stateRefFlyweight.getSegmentId() != otherRef.getSegmentId()
                    || stateRefFlyweight.getOffset() != otherRef.getOffset()
                    || stateRefFlyweight.getStoredLength() != otherRef.getStoredLength()
                    || stateRefFlyweight.getDecodedLength() != otherRef.getDecodedLength()
                    || stateRefFlyweight.getPageKind() != otherRef.getPageKind()
                    || stateRefFlyweight.getCodec() != otherRef.getCodec()
                    || stateRefFlyweight.getRowCount() != otherRef.getRowCount()
                    || stateRefFlyweight.getFlags() != otherRef.getFlags()) {
                return false;
            }
        }
        return true;
    }

    void adjustRefCounts(LongList counts, int mutationIndex, int delta) {
        for (int i = 0, n = refCount(mutationIndex); i < n; i++) {
            refAt(mutationIndex, i, stateRefFlyweight);
            LiveViewCheckpointMetadata.adjustSegmentUseCount(counts, stateRefFlyweight.getSegmentId(), delta);
        }
    }

    int appendDecoded(
            LiveViewCheckpointMetaSegmentReader reader,
            long keyOffset,
            int keyLength,
            long scalarOffset,
            int scalarLength,
            long refsOffset,
            int refCount,
            int operation
    ) {
        ensureOpen();
        final int mutationIndex = size;
        final long arenaKeyOffset = appendBytes(reader, keyOffset, keyLength);
        final long arenaScalarOffset = appendBytes(reader, scalarOffset, scalarLength);
        final long arenaRefOffset = appendBytes(
                reader,
                refsOffset,
                refCount * LiveViewCheckpointStatePageRef.BYTES
        );
        appendDescriptor(
                operation,
                arenaKeyOffset,
                keyLength,
                arenaScalarOffset,
                scalarLength,
                arenaRefOffset,
                refCount
        );
        for (int i = 0; i < refCount; i++) {
            refAt(mutationIndex, i, stateRefFlyweight);
            LiveViewCheckpointMetadata.validateStateRef(stateRefFlyweight, false, "partition");
        }
        return mutationIndex;
    }

    boolean containsSortedKey(byte[] key) {
        int lo = 0;
        int hi = sortedSize;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final int cmp = compareKey(getSortedMutationIndex(mid), key);
            if (cmp < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo < sortedSize && compareKey(getSortedMutationIndex(lo), key) == 0;
    }

    boolean isLowerBoundCountRecordedForTest() {
        lowerBoundCountForTest++;
        return true;
    }

    int keyLength(int mutationIndex) {
        return (int) descriptor(mutationIndex, DESC_KEY_LENGTH);
    }

    int operation(int mutationIndex) {
        return (int) descriptor(mutationIndex, DESC_OPERATION);
    }

    int refCount(int mutationIndex) {
        return (int) descriptor(mutationIndex, DESC_REF_COUNT);
    }

    void refAt(int mutationIndex, int refIndex, LiveViewCheckpointStatePageRef out) {
        final long offset = refOffset(mutationIndex) + (long) refIndex * LiveViewCheckpointStatePageRef.BYTES;
        out.of(
                bytes.getLong(offset),
                bytes.getLong(offset + Long.BYTES),
                bytes.getInt(offset + 2L * Long.BYTES),
                bytes.getInt(offset + 2L * Long.BYTES + Integer.BYTES),
                bytes.getInt(offset + 2L * Long.BYTES + 2L * Integer.BYTES),
                bytes.getInt(offset + 2L * Long.BYTES + 3L * Integer.BYTES),
                bytes.getInt(offset + 2L * Long.BYTES + 4L * Integer.BYTES),
                bytes.getInt(offset + 2L * Long.BYTES + 5L * Integer.BYTES)
        );
    }

    int scalarLength(int mutationIndex) {
        return (int) descriptor(mutationIndex, DESC_SCALAR_LENGTH);
    }

    void writeKeyTo(int mutationIndex, MemoryA mem) {
        putBytes(mem, keyOffset(mutationIndex), keyLength(mutationIndex));
    }

    void writeRefsTo(int mutationIndex, MemoryA mem) {
        for (int i = 0, n = refCount(mutationIndex); i < n; i++) {
            refAt(mutationIndex, i, stateRefFlyweight);
            stateRefFlyweight.writeTo(mem);
        }
    }

    void writeScalarTo(int mutationIndex, MemoryA mem) {
        putBytes(mem, scalarOffset(mutationIndex), scalarLength(mutationIndex));
    }

    int sortAndValidate() {
        ensureOpen();
        if (sortedSize == size) {
            return 0;
        }
        ordinals.clear();
        for (int i = 0; i < size; i++) {
            ordinals.add(i);
        }
        for (int start = size >>> 1; start-- > 0; ) {
            siftDown(start, size);
        }
        for (int end = size; --end > 0; ) {
            swapOrdinals(0, end);
            siftDown(0, end);
        }
        for (int i = 1; i < size; i++) {
            if (compareKey(getSortedMutationIndex(i - 1), getSortedMutationIndex(i)) == 0) {
                throw CairoException.critical(0)
                        .put("duplicate live view checkpoint partition mutation key [left=")
                        .put(getSortedMutationIndex(i - 1))
                        .put(", right=").put(getSortedMutationIndex(i))
                        .put(", count=").put(size).put(']');
            }
        }
        sortedSize = size;
        return size;
    }

    private void append(
            int operation,
            byte[] key,
            @Nullable byte[] scalarState,
            @Nullable LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        ensureOpen();
        LiveViewCheckpointMetadata.validateByteArrayLength(key.length, "partition key");
        final int scalarLength = scalarState == null ? 0 : scalarState.length;
        LiveViewCheckpointMetadata.validateByteArrayLength(scalarLength, "partition scalar state");
        final int refCount = statePageRefs == null ? 0 : statePageRefs.length;
        if (refCount > LiveViewCheckpointMetadata.MAX_STATE_PAGE_REFS) {
            throw CairoException.critical(0).put("too many live view checkpoint partition state page references");
        }
        final long keyOffset = appendBytes(key);
        final long scalarOffset = scalarState == null ? bytes.getAppendOffset() : appendBytes(scalarState);
        final long refOffset = bytes.getAppendOffset();
        for (int i = 0; i < refCount; i++) {
            final LiveViewCheckpointStatePageRef ref = statePageRefs[i];
            LiveViewCheckpointMetadata.validateStateRef(ref, false, "partition");
            bytes.putLong(ref.getSegmentId());
            bytes.putLong(ref.getOffset());
            bytes.putInt(ref.getStoredLength());
            bytes.putInt(ref.getDecodedLength());
            bytes.putInt(ref.getPageKind());
            bytes.putInt(ref.getCodec());
            bytes.putInt(ref.getRowCount());
            bytes.putInt(ref.getFlags());
        }
        appendDescriptor(operation, keyOffset, key.length, scalarOffset, scalarLength, refOffset, refCount);
    }

    private long appendBytes(byte[] value) {
        final long offset = bytes.getAppendOffset();
        for (int i = 0; i < value.length; i++) {
            bytes.putByte(value[i]);
        }
        return offset;
    }

    private long appendBytes(LiveViewCheckpointMetaSegmentReader reader, long sourceOffset, int length) {
        final long offset = bytes.getAppendOffset();
        for (int i = 0; i < length; i++) {
            bytes.putByte(reader.getByte(sourceOffset + i));
        }
        return offset;
    }

    private void appendDescriptor(
            int operation,
            long keyOffset,
            int keyLength,
            long scalarOffset,
            int scalarLength,
            long refOffset,
            int refCount
    ) {
        descriptors.add(operation);
        descriptors.add(keyOffset);
        descriptors.add(keyLength);
        descriptors.add(scalarOffset);
        descriptors.add(scalarLength);
        descriptors.add(refOffset);
        descriptors.add(refCount);
        size++;
    }

    private void putBytes(MemoryA mem, long offset, int length) {
        for (int i = 0; i < length; i++) {
            mem.putByte(bytes.getByte(offset + i));
        }
    }

    private long descriptor(int mutationIndex, int field) {
        return descriptors.get((long) mutationIndex * DESC_LONGS + field);
    }

    private void ensureOpen() {
        if (descriptors.getCapacity() == 0) {
            descriptors.reopen();
        }
        if (ordinals.getCapacity() == 0) {
            ordinals.reopen();
        }
    }

    private long keyOffset(int mutationIndex) {
        return descriptor(mutationIndex, DESC_KEY_OFFSET);
    }

    private long refOffset(int mutationIndex) {
        return descriptor(mutationIndex, DESC_REF_OFFSET);
    }

    private long scalarOffset(int mutationIndex) {
        return descriptor(mutationIndex, DESC_SCALAR_OFFSET);
    }

    private void siftDown(int root, int end) {
        while (true) {
            final int left = (root << 1) + 1;
            if (left >= end) {
                return;
            }
            int largest = left;
            final int right = left + 1;
            if (right < end && compareKey(getSortedMutationIndex(left), getSortedMutationIndex(right)) < 0) {
                largest = right;
            }
            if (compareKey(getSortedMutationIndex(root), getSortedMutationIndex(largest)) >= 0) {
                return;
            }
            swapOrdinals(root, largest);
            root = largest;
        }
    }

    private void swapOrdinals(int left, int right) {
        final long value = ordinals.get(left);
        ordinals.set(left, ordinals.get(right));
        ordinals.set(right, value);
    }
}
