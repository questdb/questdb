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

    /**
     * Image bytes the key cache, and separately the scalar cache, of one entry may keep
     * once {@link #trimWidthCaches()} ends an operation; a cache holding more drops every
     * width. While an operation runs, a cache keeps one array for every width it copies,
     * so a later copy of any of those widths allocates nothing. Without the trim, an entry
     * that outlives its operations would keep one array for every width it has ever
     * copied, including those of views that are gone. A STRING key encodes to a 4-byte
     * length plus 2 bytes per character, so the limit covers every key of 1 to 4,093
     * characters at once: 16,773,114 bytes.
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
    private final WidthCache<byte[]> keyBuffers = new WidthCache<>(MAX_RETAINED_BUFFER_BYTES);
    private final WidthCache<LiveViewCheckpointStatePageRef[]> refBuffers = new WidthCache<>(MAX_RETAINED_STATE_PAGE_REFS);
    private final WidthCache<byte[]> scalarBuffers = new WidthCache<>(MAX_RETAINED_BUFFER_BYTES);
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

    /**
     * @return image bytes of the key or the scalar width cache of this entry, whichever
     * keeps more, headers excluded, counted by walking the caches
     */
    @TestOnly
    public long getLargestRetainedBufferBytesForTest() {
        return Math.max(keyBuffers.countRetainedForTest(), scalarBuffers.countRetainedForTest());
    }

    /**
     * @return image bytes of every key and scalar array this entry keeps for reuse,
     * headers excluded, counted by walking its width caches
     */
    @TestOnly
    public long getRetainedBufferBytesForTest() {
        return keyBuffers.countRetainedForTest() + scalarBuffers.countRetainedForTest();
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

    /**
     * Ends an operation for the width caches: a key or scalar cache holding more than
     * {@link #MAX_RETAINED_BUFFER_BYTES} image bytes, or a reference cache holding more
     * than {@link #MAX_RETAINED_STATE_PAGE_REFS} reference slots, drops every width, and a
     * cache within its limit keeps them all. An owner that outlives its operations calls
     * this when each one ends; an entry that serves a single operation needs no trim. The
     * key, scalar state and references the entry holds now stay intact.
     */
    public void trimWidthCaches() {
        keyBuffers.trim();
        scalarBuffers.trim();
        refBuffers.trim();
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
            refBuffers.retain(source.length, target);
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
