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

import io.questdb.cairo.vm.api.MemoryA;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * In-heap image of one immutable persistent partition-map B+ tree page.
 */
final class LiveViewCheckpointPartitionMapNode {

    /**
     * {@link #sourceSegmentId} of a node this build minted rather than decoded -
     * a split sibling, a fresh root, or the empty leaf of a first build. It
     * supersedes no published page.
     */
    static final long NO_SOURCE_SEGMENT_ID = -1;
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_SIZE = 2 * Integer.BYTES;
    LiveViewCheckpointPartitionMapNode[] childNodes = new LiveViewCheckpointPartitionMapNode[0];
    LiveViewCheckpointPageRef[] childRefs = new LiveViewCheckpointPageRef[0];
    LiveViewCheckpointMutationArena[] keyArenas = new LiveViewCheckpointMutationArena[0];
    int[] keyMutationIndexes = new int[0];
    byte[][] keys = new byte[0][];
    byte[][] scalarStates = new byte[0][];
    /**
     * Metadata segment of the published page this node was decoded from, or
     * {@link #NO_SOURCE_SEGMENT_ID}. A copy-on-write build reads it to report the
     * page it is about to supersede, so the segment holding that page loses a
     * reachable page exactly when the build stops naming it.
     */
    long sourceSegmentId = NO_SOURCE_SEGMENT_ID;
    LiveViewCheckpointStatePageRef[][] statePageRefs = new LiveViewCheckpointStatePageRef[0][];
    /**
     * Images the arena-free {@link #decode(LiveViewCheckpointMetaSegmentReader)}
     * borrows for this node's current page. A seal probes the predecessor's map
     * once per live key, so a fresh key, scalar and reference array per decoded
     * entry would charge the whole live domain to every publication. Reset per
     * decode, and owned by this node alone: what leaves it does so through
     * {@link #copyEntryTo}, which copies into the caller's own flyweight.
     */
    private final LiveViewCheckpointByteArrayPool decodedBytes = new LiveViewCheckpointByteArrayPool();
    private final LiveViewCheckpointPageRefPool decodedChildRefs = new LiveViewCheckpointPageRefPool();
    private final LiveViewCheckpointStateRefArrayPool decodedStateRefs = new LiveViewCheckpointStateRefArrayPool();
    private int count;
    private boolean leaf;

    int childIndex(byte[] key) {
        assert !leaf && count > 0;
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (compareKeyAt(mid, key) <= 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return Math.max(0, lo - 1);
    }

    int childIndex(LiveViewCheckpointMutationArena arena, int mutationIndex) {
        assert !leaf && count > 0;
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (compareMutationToKeyAt(arena, mutationIndex, mid) >= 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return Math.max(0, lo - 1);
    }

    int count() {
        return count;
    }

    void copyEntryTo(int index, LiveViewCheckpointPartitionMapEntry out) {
        out.of(keys[index], scalarStates[index], statePageRefs[index]);
    }

    void decode(@NotNull LiveViewCheckpointMetaSegmentReader reader) {
        decode(reader, null, null);
    }

    void decode(
            @NotNull LiveViewCheckpointMetaSegmentReader reader,
            @Nullable LiveViewCheckpointMutationArena arena,
            @Nullable LiveViewCheckpointPageRefPool pageRefPool
    ) {
        final int pageKind = reader.getPageKind();
        if (pageKind != LiveViewCheckpointPartitionMap.PAGE_KIND_LEAF
                && pageKind != LiveViewCheckpointPartitionMap.PAGE_KIND_INTERNAL) {
            throw LiveViewCheckpointMetadata.invalid("partition map page kind unknown, kind=").put(pageKind);
        }
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < HEADER_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("partition map payload too small, length=").put(payloadLength);
        }
        final int version = reader.getInt(0);
        if (version != FORMAT_VERSION) {
            throw LiveViewCheckpointMetadata.invalid("partition map format version mismatch")
                    .put(" [expected=").put(FORMAT_VERSION).put(", actual=").put(version).put(']');
        }
        final int decodedCount = reader.getInt(Integer.BYTES);
        if (decodedCount <= 0 || decodedCount > LiveViewCheckpointMetadata.MAX_ENTRY_COUNT) {
            throw LiveViewCheckpointMetadata.invalid("partition map node count out of bounds, count=").put(decodedCount);
        }
        leaf = pageKind == LiveViewCheckpointPartitionMap.PAGE_KIND_LEAF;
        final int minimumEntrySize = leaf
                ? 3 * Integer.BYTES
                : Integer.BYTES + LiveViewCheckpointPageRef.BYTES;
        if ((long) HEADER_SIZE + (long) decodedCount * minimumEntrySize > payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("partition map node count exceeds payload, count=")
                    .put(decodedCount);
        }
        count = 0;
        sourceSegmentId = NO_SOURCE_SEGMENT_ID;
        if (arena == null) {
            decodedBytes.reset();
            decodedChildRefs.reset();
            decodedStateRefs.reset();
        }
        ensureCapacity(decodedCount);
        long offset = HEADER_SIZE;
        byte[] previousKey = null;
        int previousMutationIndex = -1;
        for (int i = 0; i < decodedCount; i++) {
            requireRemaining(offset, Integer.BYTES, payloadLength, "entry key length");
            final int keyLength = reader.getInt(offset);
            LiveViewCheckpointMetadata.validateByteArrayLength(keyLength, "partition key");
            offset += Integer.BYTES;
            final int scalarLength;
            final int refCount;
            if (leaf) {
                requireRemaining(offset, 2L * Integer.BYTES, payloadLength, "leaf entry header");
                scalarLength = reader.getInt(offset);
                refCount = reader.getInt(offset + Integer.BYTES);
                LiveViewCheckpointMetadata.validateByteArrayLength(scalarLength, "partition scalar state");
                if (refCount < 0 || refCount > LiveViewCheckpointMetadata.MAX_STATE_PAGE_REFS) {
                    throw LiveViewCheckpointMetadata.invalid("partition state page reference count out of bounds, count=").put(refCount);
                }
                offset += 2L * Integer.BYTES;
            } else {
                scalarLength = 0;
                refCount = 0;
            }
            final long tailLength = (long) keyLength + scalarLength
                    + (leaf ? (long) refCount * LiveViewCheckpointStatePageRef.BYTES : LiveViewCheckpointPageRef.BYTES);
            requireRemaining(offset, tailLength, payloadLength, "entry body");
            if (arena != null) {
                final long keyOffset = offset;
                final long scalarOffset = keyOffset + keyLength;
                final long refsOffset = scalarOffset + scalarLength;
                final int mutationIndex = arena.appendDecoded(
                        reader,
                        keyOffset,
                        keyLength,
                        scalarOffset,
                        scalarLength,
                        refsOffset,
                        refCount,
                        leaf ? LiveViewCheckpointMutationArena.OP_PUT : LiveViewCheckpointMutationArena.OP_DOMAIN
                );
                if (previousMutationIndex > -1 && arena.compareKey(previousMutationIndex, mutationIndex) >= 0) {
                    throw LiveViewCheckpointMetadata.invalid("partition map keys not strictly increasing");
                }
                keys[i] = null;
                keyArenas[i] = arena;
                keyMutationIndexes[i] = mutationIndex;
                offset += keyLength;
                if (leaf) {
                    scalarStates[i] = null;
                    statePageRefs[i] = null;
                    offset += scalarLength + (long) refCount * LiveViewCheckpointStatePageRef.BYTES;
                } else {
                    final LiveViewCheckpointPageRef ref = pageRefPool.next();
                    LiveViewCheckpointMetadata.readMetaRef(reader, offset, ref);
                    LiveViewCheckpointMetadata.validateMetaRef(ref, false, "partition child");
                    childRefs[i] = ref;
                    childNodes[i] = null;
                    offset += LiveViewCheckpointPageRef.BYTES;
                }
                previousMutationIndex = mutationIndex;
                count++;
                continue;
            }
            final byte[] key = LiveViewCheckpointMetadata.readBytes(reader, offset, keyLength, decodedBytes);
            offset += keyLength;
            if (previousKey != null && LiveViewCheckpointMetadata.compareBytes(previousKey, key) >= 0) {
                throw LiveViewCheckpointMetadata.invalid("partition map keys not strictly increasing");
            }
            keys[i] = key;
            keyArenas[i] = null;
            keyMutationIndexes[i] = -1;
            if (leaf) {
                scalarStates[i] = LiveViewCheckpointMetadata.readBytes(reader, offset, scalarLength, decodedBytes);
                offset += scalarLength;
                final LiveViewCheckpointStatePageRef[] refs = decodedStateRefs.next(refCount);
                for (int r = 0; r < refCount; r++) {
                    refs[r].readFrom(reader, offset);
                    LiveViewCheckpointMetadata.validateStateRef(refs[r], false, "partition");
                    offset += LiveViewCheckpointStatePageRef.BYTES;
                }
                statePageRefs[i] = refs;
            } else {
                final LiveViewCheckpointPageRef ref = pageRefPool == null
                        ? decodedChildRefs.next()
                        : pageRefPool.next();
                LiveViewCheckpointMetadata.readMetaRef(reader, offset, ref);
                LiveViewCheckpointMetadata.validateMetaRef(ref, false, "partition child");
                childRefs[i] = ref;
                childNodes[i] = null;
                offset += LiveViewCheckpointPageRef.BYTES;
            }
            previousKey = key;
            count++;
        }
        if (offset != payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("partition map payload has trailing bytes")
                    .put(" [consumed=").put(offset).put(", length=").put(payloadLength).put(']');
        }
    }

    int find(byte[] key) {
        final int index = lowerBound(key);
        return index < count && compareKeyAt(index, key) == 0 ? index : -1;
    }

    int find(LiveViewCheckpointMutationArena arena, int mutationIndex) {
        final int index = lowerBound(arena, mutationIndex);
        return index < count && keyEqualsAt(index, arena, mutationIndex) ? index : -1;
    }

    boolean isLeaf() {
        return leaf;
    }

    int lowerBound(byte[] key) {
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (compareKeyAt(mid, key) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    boolean keyEqualsAt(int index, LiveViewCheckpointMutationArena arena, int mutationIndex) {
        return compareMutationToKeyAt(arena, mutationIndex, index) == 0;
    }

    int lowerBound(LiveViewCheckpointMutationArena arena, int mutationIndex) {
        assert arena.isLowerBoundCountRecordedForTest();
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (compareMutationToKeyAt(arena, mutationIndex, mid) > 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    void putEntry(int index, LiveViewCheckpointPartitionMapEntry entry) {
        assert leaf;
        final int existing = index < count && LiveViewCheckpointMetadata.compareBytes(keys[index], entry.getKey()) == 0 ? index : -1;
        if (existing < 0) {
            ensureCapacity(count + 1);
            shiftRight(index);
            count++;
        }
        keys[index] = Arrays.copyOf(entry.getKey(), entry.getKey().length);
        keyArenas[index] = null;
        keyMutationIndexes[index] = -1;
        scalarStates[index] = Arrays.copyOf(entry.getScalarState(), entry.getScalarState().length);
        statePageRefs[index] = LiveViewCheckpointPartitionMapEntry.copyRefs(entry.statePageRefs());
    }

    void putEntry(int index, LiveViewCheckpointMutationArena arena, int mutationIndex) {
        assert leaf;
        final int existing = index < count && compareMutationToKeyAt(arena, mutationIndex, index) == 0 ? index : -1;
        if (existing < 0) {
            ensureCapacity(count + 1);
            shiftRight(index);
            count++;
        }
        keys[index] = null;
        keyArenas[index] = arena;
        keyMutationIndexes[index] = mutationIndex;
        scalarStates[index] = null;
        statePageRefs[index] = null;
    }

    void removeChild(int index) {
        assert !leaf;
        final int moved = count - index - 1;
        if (moved > 0) {
            System.arraycopy(keys, index + 1, keys, index, moved);
            System.arraycopy(keyArenas, index + 1, keyArenas, index, moved);
            System.arraycopy(keyMutationIndexes, index + 1, keyMutationIndexes, index, moved);
            System.arraycopy(childRefs, index + 1, childRefs, index, moved);
            System.arraycopy(childNodes, index + 1, childNodes, index, moved);
        }
        count--;
        keys[count] = null;
        keyArenas[count] = null;
        keyMutationIndexes[count] = -1;
        childRefs[count] = null;
        childNodes[count] = null;
    }

    void removeEntry(int index) {
        assert leaf;
        final int moved = count - index - 1;
        if (moved > 0) {
            System.arraycopy(keys, index + 1, keys, index, moved);
            System.arraycopy(keyArenas, index + 1, keyArenas, index, moved);
            System.arraycopy(keyMutationIndexes, index + 1, keyMutationIndexes, index, moved);
            System.arraycopy(scalarStates, index + 1, scalarStates, index, moved);
            System.arraycopy(statePageRefs, index + 1, statePageRefs, index, moved);
        }
        count--;
        keys[count] = null;
        keyArenas[count] = null;
        keyMutationIndexes[count] = -1;
        scalarStates[count] = null;
        statePageRefs[count] = null;
    }

    void resetInternal() {
        leaf = false;
        count = 0;
        sourceSegmentId = NO_SOURCE_SEGMENT_ID;
    }

    void resetLeaf() {
        leaf = true;
        count = 0;
        sourceSegmentId = NO_SOURCE_SEGMENT_ID;
    }

    void setChild(int index, LiveViewCheckpointPartitionMapNode child) {
        assert !leaf && child.count > 0;
        keys[index] = child.keys[0];
        keyArenas[index] = child.keyArenas[0];
        keyMutationIndexes[index] = child.keyMutationIndexes[0];
        childNodes[index] = child;
        childRefs[index] = null;
    }

    void insertChild(int index, LiveViewCheckpointPartitionMapNode child) {
        assert !leaf && child.count > 0;
        ensureCapacity(count + 1);
        shiftRight(index);
        count++;
        setChild(index, child);
    }

    void splitInto(LiveViewCheckpointPartitionMapNode right) {
        if (leaf) {
            right.resetLeaf();
        } else {
            right.resetInternal();
        }
        final int split = count >>> 1;
        final int rightCount = count - split;
        right.ensureCapacity(rightCount);
        System.arraycopy(keys, split, right.keys, 0, rightCount);
        System.arraycopy(keyArenas, split, right.keyArenas, 0, rightCount);
        System.arraycopy(keyMutationIndexes, split, right.keyMutationIndexes, 0, rightCount);
        if (leaf) {
            System.arraycopy(scalarStates, split, right.scalarStates, 0, rightCount);
            System.arraycopy(statePageRefs, split, right.statePageRefs, 0, rightCount);
        } else {
            System.arraycopy(childRefs, split, right.childRefs, 0, rightCount);
            System.arraycopy(childNodes, split, right.childNodes, 0, rightCount);
        }
        Arrays.fill(keys, split, count, null);
        Arrays.fill(keyArenas, split, count, null);
        Arrays.fill(keyMutationIndexes, split, count, -1);
        if (leaf) {
            Arrays.fill(scalarStates, split, count, null);
            Arrays.fill(statePageRefs, split, count, null);
        } else {
            Arrays.fill(childRefs, split, count, null);
            Arrays.fill(childNodes, split, count, null);
        }
        count = split;
        right.count = rightCount;
    }

    void writeTo(LiveViewCheckpointMetaSegmentWriter writer, LiveViewCheckpointPageRef out) {
        final MemoryA mem = writer.beginPage(
                leaf ? LiveViewCheckpointPartitionMap.PAGE_KIND_LEAF : LiveViewCheckpointPartitionMap.PAGE_KIND_INTERNAL
        );
        mem.putInt(FORMAT_VERSION);
        mem.putInt(count);
        for (int i = 0; i < count; i++) {
            final LiveViewCheckpointMutationArena arena = keyArenas[i];
            final int mutationIndex = keyMutationIndexes[i];
            mem.putInt(arena == null ? keys[i].length : arena.keyLength(mutationIndex));
            if (leaf) {
                mem.putInt(arena == null ? scalarStates[i].length : arena.scalarLength(mutationIndex));
                mem.putInt(arena == null ? statePageRefs[i].length : arena.refCount(mutationIndex));
            }
            if (arena == null) {
                LiveViewCheckpointMetadata.putBytes(mem, keys[i]);
            } else {
                arena.writeKeyTo(mutationIndex, mem);
            }
            if (leaf) {
                if (arena == null) {
                    LiveViewCheckpointMetadata.putBytes(mem, scalarStates[i]);
                    for (int r = 0; r < statePageRefs[i].length; r++) {
                        statePageRefs[i][r].writeTo(mem);
                    }
                } else {
                    arena.writeScalarTo(mutationIndex, mem);
                    arena.writeRefsTo(mutationIndex, mem);
                }
            } else {
                LiveViewCheckpointMetadata.putMetaRef(mem, childRefs[i]);
            }
        }
        writer.endPage(out);
    }

    private void ensureCapacity(int capacity) {
        if (keys.length >= capacity
                && (leaf ? scalarStates.length >= capacity : childRefs.length >= capacity)) {
            return;
        }
        final int newCapacity = Math.max(capacity, Math.max(4, keys.length * 2));
        keys = Arrays.copyOf(keys, newCapacity);
        keyArenas = Arrays.copyOf(keyArenas, newCapacity);
        keyMutationIndexes = Arrays.copyOf(keyMutationIndexes, newCapacity);
        Arrays.fill(keyMutationIndexes, count, newCapacity, -1);
        if (leaf) {
            scalarStates = Arrays.copyOf(scalarStates, newCapacity);
            statePageRefs = Arrays.copyOf(statePageRefs, newCapacity);
        } else {
            childRefs = Arrays.copyOf(childRefs, newCapacity);
            childNodes = Arrays.copyOf(childNodes, newCapacity);
        }
    }

    private void shiftRight(int index) {
        final int moved = count - index;
        if (moved <= 0) {
            return;
        }
        System.arraycopy(keys, index, keys, index + 1, moved);
        System.arraycopy(keyArenas, index, keyArenas, index + 1, moved);
        System.arraycopy(keyMutationIndexes, index, keyMutationIndexes, index + 1, moved);
        if (leaf) {
            System.arraycopy(scalarStates, index, scalarStates, index + 1, moved);
            System.arraycopy(statePageRefs, index, statePageRefs, index + 1, moved);
        } else {
            System.arraycopy(childRefs, index, childRefs, index + 1, moved);
            System.arraycopy(childNodes, index, childNodes, index + 1, moved);
        }
    }

    boolean valueEquals(int index, LiveViewCheckpointMutationArena arena, int mutationIndex) {
        final LiveViewCheckpointMutationArena storedArena = keyArenas[index];
        return storedArena == null
                ? arena.equalsScalar(mutationIndex, scalarStates[index])
                  && arena.refsEqual(mutationIndex, statePageRefs[index])
                : arena.equalsScalar(mutationIndex, storedArena, keyMutationIndexes[index])
                  && arena.refsEqual(mutationIndex, storedArena, keyMutationIndexes[index]);
    }

    private int compareKeyAt(int index, byte[] key) {
        final LiveViewCheckpointMutationArena arena = keyArenas[index];
        return arena == null
                ? LiveViewCheckpointMetadata.compareBytes(keys[index], key)
                : arena.compareKey(keyMutationIndexes[index], key);
    }

    private int compareMutationToKeyAt(
            LiveViewCheckpointMutationArena arena,
            int mutationIndex,
            int index
    ) {
        final LiveViewCheckpointMutationArena storedArena = keyArenas[index];
        return storedArena == null
                ? arena.compareKey(mutationIndex, keys[index])
                : arena.compareKey(mutationIndex, storedArena, keyMutationIndexes[index]);
    }

    private static void requireRemaining(long offset, long length, int payloadLength, CharSequence what) {
        if (length < 0 || offset < 0 || offset > payloadLength - length) {
            throw LiveViewCheckpointMetadata.invalid("partition map ").put(what).put(" truncated")
                    .put(" [offset=").put(offset).put(", length=").put(length)
                    .put(", payloadLength=").put(payloadLength).put(']');
        }
    }

}
