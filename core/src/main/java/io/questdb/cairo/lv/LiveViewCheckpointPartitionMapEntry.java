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

import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Value stored in a persistent checkpoint partition map. The encoded key is
 * ordered byte-for-byte, the scalar payload is function-owned checksummed
 * metadata, and every large state payload is reached through a checksummed
 * {@link LiveViewCheckpointStatePageRef}.
 * <p>
 * This is a mutable flyweight. Public setters copy their inputs so a checkpoint
 * candidate cannot be changed after validation by mutating caller-owned memory.
 * <p>
 * The key lives in native memory the entry owns, so an owner closes the entry when it
 * is done with it; a closed entry stays usable and allocates again on its next copy.
 * {@link #getKeyAddress()} and {@link #getKeyMemory()} read the key in place and stay
 * valid until the entry is next set, trimmed or closed. The scalar payload and the
 * state page references stay on the heap.
 */
public final class LiveViewCheckpointPartitionMapEntry implements QuietCloseable {

    /**
     * Bytes the key buffer, and separately the scalar cache, of one entry may keep once
     * {@link #trimWidthCaches()} ends an operation. The key buffer holds one key and grows
     * to the widest key the entry has copied; a buffer past the limit is freed. While an
     * operation runs, the scalar cache keeps one array for every width it copies, so a
     * later copy of any of those widths allocates nothing. Without the trim, an entry that
     * outlives its operations would keep one array for every width it has ever copied,
     * including those of views that are gone.
     */
    public static final long MAX_RETAINED_BUFFER_BYTES = 16_777_216;
    /**
     * State page reference slots the reference cache of one entry may keep once
     * {@link #trimWidthCaches()} ends an operation, about 15 MiB with their array slots.
     * A ring function over up to 256 chunks of two state pages takes every even count from
     * 2 to 512, 65,792 slots in all. {@link #MAX_RETAINED_BUFFER_BYTES} describes the policy.
     */
    public static final long MAX_RETAINED_STATE_PAGE_REFS = 262_144;
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final LiveViewCheckpointStatePageRef[] EMPTY_REFS = new LiveViewCheckpointStatePageRef[0];
    // A key is tens of bytes, so the first page holds nearly every key without growing.
    private static final long KEY_BUFFER_PAGE_SIZE = 256;
    // Lazy: nothing is allocated until the first non-empty key is copied in.
    private final MemoryCARWImpl keyMemory =
            new MemoryCARWImpl(KEY_BUFFER_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    private final WidthCache<LiveViewCheckpointStatePageRef[]> refBuffers = new WidthCache<>(MAX_RETAINED_STATE_PAGE_REFS);
    private final WidthCache<byte[]> scalarBuffers = new WidthCache<>(MAX_RETAINED_BUFFER_BYTES);
    private int keyLength;
    private byte[] scalarState = EMPTY_BYTES;
    private LiveViewCheckpointStatePageRef[] statePageRefs = EMPTY_REFS;
    private int widthLookupCountForTest;

    public LiveViewCheckpointPartitionMapEntry clear() {
        keyLength = 0;
        scalarState = EMPTY_BYTES;
        statePageRefs = EMPTY_REFS;
        return this;
    }

    /**
     * Frees the key buffer and clears the entry. Idempotent; the entry stays usable.
     */
    @Override
    public void close() {
        keyMemory.close();
        clear();
    }

    /**
     * @return a heap copy of the key, for assertions only
     */
    @TestOnly
    public byte[] copyKeyForTest() {
        final byte[] copy = new byte[keyLength];
        if (keyLength > 0) {
            Unsafe.copyMemory(null, getKeyAddress(), copy, Unsafe.BYTE_OFFSET, keyLength);
        }
        return copy;
    }

    /**
     * @return the native address of the key's first byte, valid until the entry is next
     * set, trimmed or closed
     */
    public long getKeyAddress() {
        return keyMemory.addressOf(0);
    }

    /**
     * @return native bytes the key buffer holds, whatever the key's length
     */
    @TestOnly
    public long getKeyBufferCapacityForTest() {
        return keyMemory.size();
    }

    public int getKeyLength() {
        return keyLength;
    }

    /**
     * @return the memory holding the key at offset 0, so a decoder can frame the key as a
     * bounded page without copying it; valid until the entry is next set, trimmed or closed
     */
    public MemoryR getKeyMemory() {
        return keyMemory;
    }

    public byte[] getScalarState() {
        return scalarState;
    }

    public int getStatePageCount() {
        return statePageRefs.length;
    }

    /**
     * @return native bytes of the key buffer or image bytes of the scalar width cache of
     * this entry, whichever is larger, headers excluded, the cache counted by walking it
     */
    @TestOnly
    public long getLargestRetainedBufferBytesForTest() {
        return Math.max(keyMemory.size(), scalarBuffers.countRetainedForTest());
    }

    /**
     * @return native bytes of the key buffer plus image bytes of every scalar array this
     * entry keeps for reuse, headers excluded, the cache counted by walking it
     */
    @TestOnly
    public long getRetainedBufferBytesForTest() {
        return keyMemory.size() + scalarBuffers.countRetainedForTest();
    }

    /**
     * @return state page reference slots of every reference array this entry keeps for
     * reuse, counted by walking its width cache
     */
    @TestOnly
    public long getRetainedStatePageRefCountForTest() {
        return refBuffers.countRetainedForTest();
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

    /**
     * Copies the {@code keyLength} key bytes at {@code keyAddress}, the scalar state and
     * the references into this entry. The key must not lie in this entry's own buffer.
     */
    public LiveViewCheckpointPartitionMapEntry of(
            long keyAddress,
            int keyLength,
            @NotNull byte[] scalarState,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        assert keyLength >= 0 : "negative live view checkpoint partition key length";
        copyKey(keyAddress, keyLength);
        this.scalarState = copyBytes(scalarState, scalarBuffers);
        this.statePageRefs = copyRefsPooled(statePageRefs);
        return this;
    }

    /**
     * Ends an operation for the entry's buffers: a key buffer holding more than
     * {@link #MAX_RETAINED_BUFFER_BYTES} native bytes is freed, a scalar cache holding more
     * than that many image bytes or a reference cache holding more than
     * {@link #MAX_RETAINED_STATE_PAGE_REFS} reference slots drops every width, and a buffer
     * or cache within its limit keeps what it holds. An owner that outlives its operations
     * calls this when each one ends; an entry that serves a single operation needs no trim.
     * The scalar state and references the entry holds now stay intact; a freed key buffer
     * takes the key with it, so the entry then holds an empty key.
     */
    public void trimWidthCaches() {
        if (keyMemory.size() > MAX_RETAINED_BUFFER_BYTES) {
            keyMemory.close();
            keyLength = 0;
        }
        scalarBuffers.trim();
        refBuffers.trim();
    }

    void copyFrom(@NotNull LiveViewCheckpointPartitionMapEntry other) {
        if (other != this) {
            of(other.getKeyAddress(), other.keyLength, other.scalarState, other.statePageRefs);
        }
    }

    /**
     * Copies the key, scalar state and references of one decoded entry out of the arena
     * that staged it, so the entry stays valid after the arena is cleared or refilled.
     */
    LiveViewCheckpointPartitionMapEntry ofArena(@NotNull LiveViewCheckpointMutationArena arena, int mutationIndex) {
        copyKey(arena.keyAddress(mutationIndex), arena.keyLength(mutationIndex));
        final int scalarLength = arena.scalarLength(mutationIndex);
        if (scalarLength == 0) {
            scalarState = EMPTY_BYTES;
        } else {
            final byte[] target = scalarBuffer(scalarLength);
            arena.copyScalarTo(mutationIndex, target);
            scalarState = target;
        }
        final int refCount = arena.refCount(mutationIndex);
        if (refCount == 0) {
            statePageRefs = EMPTY_REFS;
        } else {
            final LiveViewCheckpointStatePageRef[] target = refBuffer(refCount);
            for (int i = 0; i < refCount; i++) {
                arena.refAt(mutationIndex, i, target[i]);
            }
            statePageRefs = target;
        }
        return this;
    }

    static LiveViewCheckpointStatePageRef copyRef(LiveViewCheckpointStatePageRef source) {
        return new LiveViewCheckpointStatePageRef().of(
                source.getSegmentId(), source.getOffset(), source.getStoredLength(), source.getDecodedLength(),
                source.getPageKind(), source.getCodec(), source.getRowCount(), source.getFlags()
        );
    }

    private byte[] copyBytes(byte[] source, WidthCache<byte[]> buffers) {
        if (source.length == 0) {
            return EMPTY_BYTES;
        }
        assert isWidthLookupRecordedForTest();
        byte[] target = buffers.get(source.length);
        if (target == null) {
            target = new byte[source.length];
            buffers.retain(source.length, target);
        }
        System.arraycopy(source, 0, target, 0, source.length);
        return target;
    }

    private void copyKey(long address, int length) {
        assert length == 0 || address >= keyMemory.addressHi() || address + length <= keyMemory.getPageAddress(0)
                : "live view checkpoint partition map entry key aliases its own buffer";
        keyLength = 0;
        keyMemory.jumpTo(0);
        if (length > 0) {
            keyMemory.putBlockOfBytes(address, length);
        }
        keyLength = length;
    }

    private LiveViewCheckpointStatePageRef[] copyRefsPooled(LiveViewCheckpointStatePageRef[] source) {
        if (source.length == 0) {
            return EMPTY_REFS;
        }
        final LiveViewCheckpointStatePageRef[] target = refBuffer(source.length);
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

    private LiveViewCheckpointStatePageRef[] refBuffer(int count) {
        assert isWidthLookupRecordedForTest();
        LiveViewCheckpointStatePageRef[] target = refBuffers.get(count);
        if (target == null) {
            target = new LiveViewCheckpointStatePageRef[count];
            for (int i = 0; i < count; i++) {
                target[i] = new LiveViewCheckpointStatePageRef();
            }
            refBuffers.retain(count, target);
        }
        return target;
    }

    private byte[] scalarBuffer(int length) {
        assert isWidthLookupRecordedForTest();
        byte[] target = scalarBuffers.get(length);
        if (target == null) {
            target = new byte[length];
            scalarBuffers.retain(length, target);
        }
        return target;
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

    /**
     * Exact-width arrays one field of the entry reuses across copies, keyed by width.
     * The cache keeps every width a copy hands it. {@link #trim()} ends an operation: a cache
     * holding more than {@code maxRetainedAfterTrim} elements drops every width rather
     * than evicting the least recently used ones, so the copies that follow pay one
     * allocation per width they bring back, and no copy pays for tracking use order.
     */
    private static final class WidthCache<T> {
        private final IntObjHashMap<T> buffersByWidth = new IntObjHashMap<>();
        private final long maxRetainedAfterTrim;
        private long retained;

        private WidthCache(long maxRetainedAfterTrim) {
            this.maxRetainedAfterTrim = maxRetainedAfterTrim;
        }

        /**
         * @return array elements every buffer this cache holds, counted by walking the
         * cache rather than read from the count the cache keeps for itself
         */
        @TestOnly
        private long countRetainedForTest() {
            long count = 0;
            final int[] widths = buffersByWidth.getKeys();
            final Object[] buffers = buffersByWidth.getValues();
            for (int i = 0, n = buffers.length; i < n; i++) {
                if (buffers[i] != null) {
                    count += widths[i];
                }
            }
            return count;
        }

        private T get(int width) {
            return buffersByWidth.get(width);
        }

        /**
         * Keeps {@code buffer}, whose length is {@code width}, for the next copy of that
         * width.
         */
        private void retain(int width, T buffer) {
            buffersByWidth.put(width, buffer);
            retained += width;
        }

        private void trim() {
            if (retained > maxRetainedAfterTrim) {
                buffersByWidth.clear();
                retained = 0;
            }
        }
    }
}
