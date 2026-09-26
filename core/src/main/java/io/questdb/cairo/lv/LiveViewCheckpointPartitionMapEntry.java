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
 * The key and the scalar payload live in native memory the entry owns, one buffer
 * each, so an owner closes the entry when it is done with it; a closed entry stays
 * usable and allocates again on its next copy. {@link #getKeyAddress()},
 * {@link #getKeyMemory()}, {@link #getScalarAddress()} and {@link #getScalarMemory()}
 * read in place and stay valid until the entry is next set, trimmed or closed. The
 * state page references stay on the heap.
 */
public final class LiveViewCheckpointPartitionMapEntry implements QuietCloseable {

    /**
     * Bytes the key buffer of one entry may keep once {@link #trimWidthCaches()} ends an
     * operation. The buffer holds one key and grows to the widest key the entry has
     * copied, so a later copy of any narrower key allocates nothing; a buffer past the
     * limit is freed. Without the trim, an entry that outlives its operations would keep
     * the widest key it has ever copied, including one of a view that is gone.
     */
    public static final long MAX_RETAINED_BUFFER_BYTES = 16_777_216;
    /**
     * Bytes the scalar buffer of one entry may keep once {@link #trimWidthCaches()} ends an
     * operation. The buffer holds one scalar payload and grows to the widest the entry has
     * copied, as the key buffer does. The widest scalar a view writes is a fused leaf of a
     * few hundred bytes; the format admits 1 MiB, which only a crafted root reaches, and a
     * buffer grown past this limit is freed rather than kept for the operations that follow.
     */
    public static final long MAX_RETAINED_SCALAR_BUFFER_BYTES = 65_536;
    /**
     * State page reference slots the reference cache of one entry may keep once
     * {@link #trimWidthCaches()} ends an operation, about 15 MiB with their array slots.
     * A ring function over up to 256 chunks of two state pages takes every even count from
     * 2 to 512, 65,792 slots in all. {@link #MAX_RETAINED_BUFFER_BYTES} describes the policy.
     */
    public static final long MAX_RETAINED_STATE_PAGE_REFS = 262_144;
    private static final LiveViewCheckpointStatePageRef[] EMPTY_REFS = new LiveViewCheckpointStatePageRef[0];
    // A key is tens of bytes, so the first page holds nearly every key without growing.
    private static final long KEY_BUFFER_PAGE_SIZE = 256;
    // A fused leaf, the widest scalar a view writes, fits the first page, as does every
    // inline state image and ring header.
    private static final long SCALAR_BUFFER_PAGE_SIZE = 256;
    // Lazy: nothing is allocated until the first non-empty key is copied in.
    private final MemoryCARWImpl keyMemory =
            new MemoryCARWImpl(KEY_BUFFER_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    private final WidthCache<LiveViewCheckpointStatePageRef[]> refBuffers = new WidthCache<>(MAX_RETAINED_STATE_PAGE_REFS);
    // Lazy, as the key buffer: nothing is allocated until the first non-empty scalar.
    private final MemoryCARWImpl scalarMemory =
            new MemoryCARWImpl(SCALAR_BUFFER_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    private int keyLength;
    private int scalarLength;
    private LiveViewCheckpointStatePageRef[] statePageRefs = EMPTY_REFS;
    private int widthLookupCountForTest;

    /**
     * Empties the entry and keeps both buffers for the next copy.
     */
    public LiveViewCheckpointPartitionMapEntry clear() {
        keyLength = 0;
        scalarLength = 0;
        statePageRefs = EMPTY_REFS;
        return this;
    }

    /**
     * Frees the key and scalar buffers and clears the entry. Idempotent; the entry stays
     * usable.
     */
    @Override
    public void close() {
        keyMemory.close();
        scalarMemory.close();
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
     * @return a heap copy of the scalar payload, for assertions only
     */
    @TestOnly
    public byte[] copyScalarStateForTest() {
        final byte[] copy = new byte[scalarLength];
        if (scalarLength > 0) {
            Unsafe.copyMemory(null, getScalarAddress(), copy, Unsafe.BYTE_OFFSET, scalarLength);
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

    /**
     * @return native bytes of the key buffer or the scalar buffer of this entry, whichever
     * is larger
     */
    @TestOnly
    public long getLargestRetainedBufferBytesForTest() {
        return Math.max(keyMemory.size(), scalarMemory.size());
    }

    /**
     * @return native bytes of the key buffer plus the scalar buffer of this entry
     */
    @TestOnly
    public long getRetainedBufferBytesForTest() {
        return keyMemory.size() + scalarMemory.size();
    }

    /**
     * @return state page reference slots of every reference array this entry keeps for
     * reuse, counted by walking its width cache
     */
    @TestOnly
    public long getRetainedStatePageRefCountForTest() {
        return refBuffers.countRetainedForTest();
    }

    /**
     * @return the native address of the scalar payload's first byte, or 0 when the entry
     * holds no scalar; valid until the entry is next set, trimmed or closed
     */
    public long getScalarAddress() {
        return scalarLength == 0 ? 0 : scalarMemory.addressOf(0);
    }

    /**
     * @return native bytes the scalar buffer holds, whatever the scalar's length
     */
    @TestOnly
    public long getScalarBufferCapacityForTest() {
        return scalarMemory.size();
    }

    public int getScalarLength() {
        return scalarLength;
    }

    /**
     * @return the memory holding the scalar payload at offset 0, so a decoder can frame it
     * as a bounded page without copying it; valid until the entry is next set, trimmed or
     * closed
     */
    public MemoryR getScalarMemory() {
        return scalarMemory;
    }

    public int getStatePageCount() {
        return statePageRefs.length;
    }

    public LiveViewCheckpointStatePageRef getStatePageRef(int index) {
        return statePageRefs[index];
    }

    /**
     * @return reference-cache lookups since the last reset; the key and the scalar take none
     */
    @TestOnly
    public int getWidthLookupCountForTest() {
        return widthLookupCountForTest;
    }

    @TestOnly
    public void resetWidthLookupCountForTest() {
        widthLookupCountForTest = 0;
    }

    /**
     * Copies the {@code keyLength} key bytes at {@code keyAddress}, the
     * {@code scalarLength} scalar bytes at {@code scalarAddress} and the references into
     * this entry. Neither the key nor the scalar may lie in this entry's own buffers.
     */
    public LiveViewCheckpointPartitionMapEntry of(
            long keyAddress,
            int keyLength,
            long scalarAddress,
            int scalarLength,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        return of(keyAddress, keyLength, scalarAddress, scalarLength, statePageRefs, statePageRefs.length);
    }

    /**
     * Copies the {@code keyLength} key bytes at {@code keyAddress}, the
     * {@code scalarLength} scalar bytes at {@code scalarAddress} and the first
     * {@code statePageRefCount} references into this entry, so a caller that keeps its
     * references in a pooled array longer than what it holds hands over only those. Neither
     * the key nor the scalar may lie in this entry's own buffers.
     */
    public LiveViewCheckpointPartitionMapEntry of(
            long keyAddress,
            int keyLength,
            long scalarAddress,
            int scalarLength,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs,
            int statePageRefCount
    ) {
        assert keyLength >= 0 : "negative live view checkpoint partition key length";
        assert scalarLength >= 0 : "negative live view checkpoint partition scalar length";
        assert statePageRefCount >= 0 && statePageRefCount <= statePageRefs.length
                : "live view checkpoint partition reference count outside its array";
        copyKey(keyAddress, keyLength);
        copyScalar(scalarAddress, scalarLength);
        this.statePageRefs = copyRefsPooled(statePageRefs, statePageRefCount);
        return this;
    }

    /**
     * Ends an operation for the entry's buffers: a key buffer holding more than
     * {@link #MAX_RETAINED_BUFFER_BYTES} native bytes or a scalar buffer holding more than
     * {@link #MAX_RETAINED_SCALAR_BUFFER_BYTES} is freed, a reference cache holding more
     * than {@link #MAX_RETAINED_STATE_PAGE_REFS} reference slots drops every width, and a
     * buffer or cache within its limit keeps what it holds. An owner that outlives its
     * operations calls this when each one ends; an entry that serves a single operation
     * needs no trim. The references the entry holds now stay intact; a freed buffer takes
     * its key or scalar with it, so the entry then holds an empty one.
     */
    public void trimWidthCaches() {
        if (keyMemory.size() > MAX_RETAINED_BUFFER_BYTES) {
            keyMemory.close();
            keyLength = 0;
        }
        if (scalarMemory.size() > MAX_RETAINED_SCALAR_BUFFER_BYTES) {
            scalarMemory.close();
            scalarLength = 0;
        }
        refBuffers.trim();
    }

    void copyFrom(@NotNull LiveViewCheckpointPartitionMapEntry other) {
        if (other != this) {
            of(other.getKeyAddress(), other.keyLength, other.getScalarAddress(), other.scalarLength, other.statePageRefs);
        }
    }

    /**
     * @return whether the scalar this entry holds is exactly the {@code length} bytes at
     * {@code address}
     */
    boolean isScalarEqual(long address, int length) {
        return LiveViewCheckpointKeys.equals(getScalarAddress(), scalarLength, address, length);
    }

    /**
     * Copies the key, scalar state and references of one decoded entry out of the arena
     * that staged it, so the entry stays valid after the arena is cleared or refilled.
     */
    LiveViewCheckpointPartitionMapEntry ofArena(@NotNull LiveViewCheckpointMutationArena arena, int mutationIndex) {
        copyKey(arena.keyAddress(mutationIndex), arena.keyLength(mutationIndex));
        copyScalar(arena.scalarAddress(mutationIndex), arena.scalarLength(mutationIndex));
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

    private LiveViewCheckpointStatePageRef[] copyRefsPooled(LiveViewCheckpointStatePageRef[] source, int count) {
        if (count == 0) {
            return EMPTY_REFS;
        }
        final LiveViewCheckpointStatePageRef[] target = refBuffer(count);
        for (int i = 0; i < count; i++) {
            final LiveViewCheckpointStatePageRef from = source[i];
            target[i].of(
                    from.getSegmentId(), from.getOffset(), from.getStoredLength(), from.getDecodedLength(),
                    from.getPageKind(), from.getCodec(), from.getRowCount(), from.getFlags()
            );
        }
        return target;
    }

    private void copyScalar(long address, int length) {
        assert length == 0 || address >= scalarMemory.addressHi() || address + length <= scalarMemory.getPageAddress(0)
                : "live view checkpoint partition map entry scalar aliases its own buffer";
        scalarLength = 0;
        scalarMemory.jumpTo(0);
        if (length > 0) {
            scalarMemory.putBlockOfBytes(address, length);
        }
        scalarLength = length;
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
