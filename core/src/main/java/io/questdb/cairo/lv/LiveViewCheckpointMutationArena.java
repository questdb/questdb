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
    // The merge's output, for input that arrives as two sorted runs; see sortAndValidate.
    private final DirectLongList mergeScratch = new DirectLongList(INITIAL_LONG_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
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
        mergeScratch.setMemoryTracker(memoryTracker);
        ordinals.setMemoryTracker(memoryTracker);
    }

    /**
     * Frees whatever the previous binding charged and binds {@code memoryTracker}
     * for the next build. A builder shared across views must not carry retained
     * capacity from one view's tracker into another's, so the release runs while
     * the old tracker is still bound.
     */
    public void bind(@Nullable MemoryTracker memoryTracker) {
        release();
        bytes.setMemoryTracker(memoryTracker);
        descriptors.setMemoryTracker(memoryTracker);
        mergeScratch.setMemoryTracker(memoryTracker);
        ordinals.setMemoryTracker(memoryTracker);
    }

    public void clear() {
        if (bytes.getAppendOffset() > 0) {
            bytes.jumpTo(0);
        }
        descriptors.clear();
        mergeScratch.clear();
        ordinals.clear();
        size = 0;
        sortedSize = 0;
    }

    @Override
    public void close() {
        Misc.free(bytes);
        Misc.free(descriptors);
        Misc.free(mergeScratch);
        Misc.free(ordinals);
        size = 0;
        sortedSize = 0;
    }

    /**
     * Frees every native allocation against the tracker that acquired it and
     * detaches that tracker. The arena stays reusable: the next
     * {@link #bind(MemoryTracker)} re-acquires capacity under the new one.
     */
    public void release() {
        bytes.clear();
        bytes.setMemoryTracker(null);
        descriptors.close();
        descriptors.setMemoryTracker(null);
        mergeScratch.close();
        mergeScratch.setMemoryTracker(null);
        ordinals.close();
        ordinals.setMemoryTracker(null);
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
        // Mutations that arrive in key order need no sort, and every freeze walk hands
        // them over that way - it walks the predecessor tree's own order - so one linear
        // scan replaces the heapsort in the common case. A seal that also removes keys
        // hands over two such runs, the removals ahead of the puts, and those merge in one
        // pass. The scan doubles as the duplicate check the sorted order owes within a
        // run; the merge makes it across the two, and anything else falls through to the
        // heapsort, which validates after sorting.
        int descent = -1;
        boolean isTwoRunsAtMost = true;
        for (int i = 1; i < size; i++) {
            final int cmp = compareKey(i - 1, i);
            if (cmp == 0) {
                throw duplicateKey(i - 1, i);
            }
            if (cmp > 0) {
                if (descent >= 0) {
                    isTwoRunsAtMost = false;
                    break;
                }
                descent = i;
            }
        }
        if (descent >= 0 && isTwoRunsAtMost) {
            mergeRuns(descent);
        } else if (descent >= 0) {
            for (int start = size >>> 1; start-- > 0; ) {
                siftDown(start, size);
            }
            for (int end = size; --end > 0; ) {
                swapOrdinals(0, end);
                siftDown(0, end);
            }
            for (int i = 1; i < size; i++) {
                if (compareKey(getSortedMutationIndex(i - 1), getSortedMutationIndex(i)) == 0) {
                    throw duplicateKey(getSortedMutationIndex(i - 1), getSortedMutationIndex(i));
                }
            }
        }
        sortedSize = size;
        return size;
    }

    /**
     * Merges the sorted runs {@code [0, split)} and {@code [split, size)} of mutation
     * indexes into {@link #ordinals}, refusing a key both runs hold.
     */
    private void mergeRuns(int split) {
        mergeScratch.clear();
        mergeScratch.ensureCapacity(size);
        int i = 0;
        int j = split;
        while (i < split && j < size) {
            final int cmp = compareKey(i, j);
            if (cmp == 0) {
                throw duplicateKey(i, j);
            }
            if (cmp < 0) {
                mergeScratch.add(i++);
            } else {
                mergeScratch.add(j++);
            }
        }
        while (i < split) {
            mergeScratch.add(i++);
        }
        while (j < size) {
            mergeScratch.add(j++);
        }
        for (int k = 0; k < size; k++) {
            ordinals.set(k, mergeScratch.get(k));
        }
    }

    private CairoException duplicateKey(int leftMutationIndex, int rightMutationIndex) {
        return CairoException.critical(0)
                .put("duplicate live view checkpoint partition mutation key [left=")
                .put(leftMutationIndex)
                .put(", right=").put(rightMutationIndex)
                .put(", count=").put(size).put(']');
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
