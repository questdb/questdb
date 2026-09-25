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
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Reusable columnar staging for one partition-map build. Variable-width key,
 * scalar, and state-reference bytes live in one tracker-bound native arena;
 * fixed-width descriptors and sort ordinals use tracker-bound primitive lists.
 * <p>
 * A key arrives as an {@code (address, length)} pair valid only for the call, and
 * the arena copies it: the pair must not point into this arena, which moves when it
 * grows.
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
    private static final long PAGE_SIZE = 4096;
    // One root build stages every put, domain entry and decoded leaf entry here, so a page
    // ceiling would cap how many keys a view can checkpoint. The arena sets none: offsets are
    // long end to end, and append() and the partition-map page decoder validate each field's
    // length before staging it. Two limits bound the growth instead: the memory tracker that
    // bind() attaches, which enforces cairo.live.view.refresh.memory.limit.bytes, and the
    // global RSS limit. Each fails the growth with its own out-of-memory error.
    private final MemoryCARWImpl bytes;
    private final DirectLongList descriptors;
    private final DirectLongList ordinals;
    private final LiveViewCheckpointStatePageRef otherStateRefFlyweight = new LiveViewCheckpointStatePageRef();
    private final LiveViewCheckpointStatePageRef stateRefFlyweight = new LiveViewCheckpointStatePageRef();
    private int lowerBoundCountForTest;
    private int size;
    private int sortedSize;

    public LiveViewCheckpointMutationArena() {
        this(null);
    }

    public LiveViewCheckpointMutationArena(@Nullable MemoryTracker memoryTracker) {
        this(memoryTracker, MemoryTag.NATIVE_DEFAULT);
    }

    /**
     * @param memoryTag the tag every allocation of this arena is accounted under. Build
     *                  staging uses {@link MemoryTag#NATIVE_DEFAULT}; a partition-map
     *                  reader's decoded nodes are live-view in-memory state and use
     *                  {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}
     */
    LiveViewCheckpointMutationArena(@Nullable MemoryTracker memoryTracker, int memoryTag) {
        // Lazy, all three: a constructor that throws part-way strands no native memory.
        bytes = new MemoryCARWImpl(PAGE_SIZE, Integer.MAX_VALUE, memoryTag);
        descriptors = new DirectLongList(INITIAL_LONG_CAPACITY, memoryTag, true);
        ordinals = new DirectLongList(INITIAL_LONG_CAPACITY, memoryTag, true);
        bytes.setMemoryTracker(memoryTracker);
        descriptors.setMemoryTracker(memoryTracker);
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
        ordinals.close();
        ordinals.setMemoryTracker(null);
        size = 0;
        sortedSize = 0;
    }

    public void domain(long keyAddress, int keyLength) {
        append(OP_DOMAIN, keyAddress, keyLength, null, null);
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
            long keyAddress,
            int keyLength,
            @NotNull byte[] scalarState,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        append(OP_PUT, keyAddress, keyLength, scalarState, statePageRefs);
    }

    public void put(long keyAddress, int keyLength, @NotNull byte[] scalarState) {
        append(OP_PUT, keyAddress, keyLength, scalarState, null);
    }

    public void remove(long keyAddress, int keyLength) {
        append(OP_REMOVE, keyAddress, keyLength, null, null);
    }

    /**
     * @return the native address of a staged key, valid only until the next append to
     * this arena
     */
    @TestOnly
    public long getKeyAddressForTest(int mutationIndex) {
        return keyAddress(mutationIndex);
    }

    @TestOnly
    public int getKeyLengthForTest(int mutationIndex) {
        return keyLength(mutationIndex);
    }

    /**
     * @return native bytes this arena holds: the variable-width region plus the
     * descriptor and ordinal lists
     */
    long getAllocatedBytes() {
        return bytes.size() + (descriptors.getCapacity() + ordinals.getCapacity()) * Long.BYTES;
    }

    int compareKey(int leftMutationIndex, int rightMutationIndex) {
        return LiveViewCheckpointKeys.compare(
                keyAddress(leftMutationIndex),
                keyLength(leftMutationIndex),
                keyAddress(rightMutationIndex),
                keyLength(rightMutationIndex)
        );
    }

    int compareKey(int mutationIndex, long keyAddress, int keyLength) {
        return LiveViewCheckpointKeys.compare(keyAddress(mutationIndex), keyLength(mutationIndex), keyAddress, keyLength);
    }

    int compareKey(int mutationIndex, LiveViewCheckpointMutationArena other, int otherMutationIndex) {
        return LiveViewCheckpointKeys.compare(
                keyAddress(mutationIndex),
                keyLength(mutationIndex),
                other.keyAddress(otherMutationIndex),
                other.keyLength(otherMutationIndex)
        );
    }

    /**
     * Copies the staged scalar of {@code mutationIndex} into {@code target}, which must be
     * exactly {@link #scalarLength} bytes long.
     */
    void copyScalarTo(int mutationIndex, byte[] target) {
        final int length = scalarLength(mutationIndex);
        assert target.length == length;
        if (length > 0) {
            Unsafe.copyMemory(null, bytes.addressOf(scalarOffset(mutationIndex)), target, Unsafe.BYTE_OFFSET, length);
        }
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

    boolean containsSortedKey(long keyAddress, int keyLength) {
        int lo = 0;
        int hi = sortedSize;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final int cmp = compareKey(getSortedMutationIndex(mid), keyAddress, keyLength);
            if (cmp < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo < sortedSize && compareKey(getSortedMutationIndex(lo), keyAddress, keyLength) == 0;
    }

    boolean isLowerBoundCountRecordedForTest() {
        lowerBoundCountForTest++;
        return true;
    }

    /**
     * @return the native address of a staged key, valid only until the next append to
     * this arena
     */
    long keyAddress(int mutationIndex) {
        return bytes.addressOf(keyOffset(mutationIndex));
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
            long keyAddress,
            int keyLength,
            @Nullable byte[] scalarState,
            @Nullable LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        ensureOpen();
        validate(keyLength, scalarState, statePageRefs);
        // The copy below may move this arena, so a key it already stages cannot be the source.
        assert !isArenaRange(keyAddress, keyLength) : "live view checkpoint mutation key aliases its own arena";
        final long keyOffset = bytes.getAppendOffset();
        if (keyLength > 0) {
            bytes.putBlockOfBytes(keyAddress, keyLength);
        }
        appendPayload(operation, keyOffset, keyLength, scalarState, statePageRefs);
    }

    private long appendBytes(byte[] value) {
        final long offset = bytes.getAppendOffset();
        if (value.length > 0) {
            Unsafe.copyMemory(value, Unsafe.BYTE_OFFSET, null, bytes.appendAddressFor(value.length), value.length);
        }
        return offset;
    }

    private long appendBytes(LiveViewCheckpointMetaSegmentReader reader, long sourceOffset, int length) {
        final long offset = bytes.getAppendOffset();
        if (length > 0) {
            // The page stays mapped until the reader opens another, and the copy
            // completes first.
            bytes.putBlockOfBytes(reader.addressOf(sourceOffset, length), length);
        }
        return offset;
    }

    /**
     * Stages the scalar and state references of a mutation whose key already sits at
     * {@code keyOffset}, then its descriptor.
     */
    private void appendPayload(
            int operation,
            long keyOffset,
            int keyLength,
            @Nullable byte[] scalarState,
            @Nullable LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        final int scalarLength = scalarState == null ? 0 : scalarState.length;
        final int refCount = statePageRefs == null ? 0 : statePageRefs.length;
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
        appendDescriptor(operation, keyOffset, keyLength, scalarOffset, scalarLength, refOffset, refCount);
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

    private boolean isArenaRange(long address, int length) {
        final long lo = bytes.getPageAddress(0);
        return length > 0 && lo != 0 && address < bytes.addressHi() && address + length > lo;
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

    private static void validate(
            int keyLength,
            @Nullable byte[] scalarState,
            @Nullable LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        LiveViewCheckpointMetadata.validateByteArrayLength(keyLength, "partition key");
        LiveViewCheckpointMetadata.validateByteArrayLength(
                scalarState == null ? 0 : scalarState.length,
                "partition scalar state"
        );
        if (statePageRefs != null && statePageRefs.length > LiveViewCheckpointMetadata.MAX_STATE_PAGE_REFS) {
            throw CairoException.critical(0).put("too many live view checkpoint partition state page references");
        }
    }
}
