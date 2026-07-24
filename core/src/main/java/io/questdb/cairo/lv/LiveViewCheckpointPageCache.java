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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Hash;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Caches the decoded image of a checkpoint state page so a later restore reads
 * it back instead of mapping the data segment and running the codec again.
 * <p>
 * A live view over a bounded RANGE frame restores its window state from the
 * anchor checkpoint on every base commit, and that restore re-decodes the whole
 * ring of every partition from varint-packed pages. The inputs never change:
 * {@link LiveViewCheckpointDataStore} owns the data segments as immutable files,
 * published by rename and never rewritten, and the seal mints segment ids from a
 * persisted monotonic ceiling. A page identified by
 * {@code (segmentId, offset, pageKind)} therefore decodes to the same bytes
 * forever, which is what makes caching the decoded image safe rather than merely
 * convenient. The cache stores the ref's {@code codec}, {@code rowCount} and
 * {@code decodedLength} alongside it and rejects a hit whose ref disagrees, so
 * metadata that contradicts itself decodes rather than serving bytes of the
 * wrong shape.
 * <p>
 * The one identity a timeline can reuse is a segment id it minted before being
 * retired and rebuilt from scratch - a repair publication, a corrupt-root
 * reconstruct, or a DROP that leaves the directory behind. {@link #bumpEpoch()}
 * covers that: it moves the cache to a new epoch and drops everything the old
 * one held, so no entry can outlive the id space it was minted in.
 * <p>
 * <b>Admission is deterministic, not least-recently-used.</b> A restore is a
 * repeated full sequential scan of every page of every partition, which is the
 * worst case LRU has: once the budget falls below the working set, LRU evicts
 * each page just before its next use and the hit rate collapses to nothing while
 * still paying the bookkeeping. This cache instead admits a page iff a hash of
 * its identity falls under the admission fraction, pinning a stable subset. The
 * subset does not move between restores, so the hit rate degrades linearly with
 * the shortfall - half the budget serves about half the probes - and a probe
 * costs one hash and one array index.
 * <p>
 * The fraction tunes itself. {@link #beginRestore()} and {@link #endRestore()}
 * bracket one restore, over which the cache sums what holding every page it was
 * asked for would cost; {@link #endRestore()} folds that into an EWMA of the
 * working set and sets the fraction to the share of it the budget can carry. A
 * cap comfortably above the working set therefore saturates at one and admits
 * everything, and a frame that fills or an ingest rate that climbs walks the
 * fraction down instead of overrunning the budget.
 * <p>
 * A cache belongs to one live view and runs under the refresh latch that
 * serialises that view's refresh, so nothing here locks. Only the engine-wide
 * {@link LiveViewCheckpointPageCacheBudget} is shared, and only slab growth
 * touches it.
 */
public class LiveViewCheckpointPageCache implements QuietCloseable {

    /**
     * The native memory one allocation takes, carved into equal slots of a single
     * size class. Every slot size divides it exactly and the largest is half of
     * it, so a carve leaves no tail and a slab always holds at least two slots.
     */
    public static final int SLAB_BYTES = 64 * 1024;
    private static final long ADMISSION_FULL = 1L << 32;
    /**
     * Keeps the admission hash independent of the one that places an entry in the
     * table, so the admitted subset does not correlate with bucket occupancy.
     */
    private static final long ADMISSION_SALT = 0x9e3779b97f4a7c15L;
    private static final int ATTRS_STRIDE = 4;
    private static final int ATTR_CODEC = 1;
    private static final int ATTR_DECODED_LENGTH = 3;
    private static final int ATTR_PAGE_KIND = 0;
    private static final int ATTR_ROW_COUNT = 2;
    private static final int INITIAL_CAPACITY = 64;
    private static final int KEYS_STRIDE = 2;
    private static final int KEY_OFFSET = 1;
    private static final int KEY_SEGMENT_ID = 0;
    private static final double LOAD_FACTOR = 0.5;
    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointPageCache.class);
    private static final int MAX_SLOT_BYTES = LiveViewCheckpointStateCodec.CHUNK_ROWS * Long.BYTES;
    private static final int MIN_SLOT_BYTES = Long.BYTES;
    private static final int MIN_SLOT_SHIFT = Numbers.msb(MIN_SLOT_BYTES);
    private static final int SIZE_CLASSES = Numbers.msb(MAX_SLOT_BYTES) - MIN_SLOT_SHIFT + 1;
    private static final long TOMBSTONE = -1;
    /**
     * How much of the working-set estimate one restore rewrites. A restore's own
     * figure moves with what the refresh happened to touch - a correction deep in
     * the history walks more partitions than one at the head - so the estimate the
     * admission fraction is set from follows the trend rather than the last
     * sample, and a single outlier cannot shut admission down.
     */
    private static final double WORKING_SET_EWMA_ALPHA = 0.25;
    private final LiveViewCheckpointPageCacheBudget budget;
    // Free slot addresses per size class. A slot returns here on eviction rather
    // than to the budget: the slab it sits in stays with the cache until close.
    private final ObjList<LongList> freeSlots = new ObjList<>(SIZE_CLASSES);
    private final LongList slabs = new LongList();
    private long[] addresses;
    private long admissionThreshold = ADMISSION_FULL;
    private int[] attrs;
    private long epoch;
    private long hits;
    // Set by hand, and then left alone by the self-tuner - see setAdmissionFraction.
    private boolean isAdmissionFractionPinned;
    private long[] keys;
    private int mask;
    private long misses;
    // Live entries plus tombstones - what the table's load factor is measured on.
    private int occupied;
    private int rehashThreshold;
    // Slot bytes every page probed since beginRestore would cost to hold, whether
    // the probe hit, missed or named a page too small or too large to cache.
    private long restoreProbedBytes;
    private int size;
    private double workingSetBytes;

    public LiveViewCheckpointPageCache(@NotNull LiveViewCheckpointPageCacheBudget budget) {
        this.budget = budget;
        for (int i = 0; i < SIZE_CLASSES; i++) {
            freeSlots.add(new LongList());
        }
    }

    /**
     * Offers the decoded image {@code ref} produced for later reuse. The cache
     * copies the bytes it takes, so the caller keeps ownership of
     * {@code decodedAddress} - typically the codec scratch it decoded into.
     * <p>
     * A page the admission hash rejects, a page the budget cannot cover and a
     * page whose ref is out of the shape a state page can have are all refused
     * the same way: the caller has already decoded, so a refusal costs nothing
     * beyond the probe that preceded it.
     *
     * @return true when the cache now holds the page
     */
    public boolean admit(@NotNull LiveViewCheckpointStatePageRef ref, long decodedAddress) {
        final long segmentId = ref.getSegmentId();
        final long offset = ref.getOffset();
        final int pageKind = ref.getPageKind();
        final int decodedLength = ref.getDecodedLength();
        if (!budget.isEnabled()
                || decodedAddress == 0
                || segmentId == LiveViewCheckpointStatePageRef.NULL_SEGMENT_ID
                || decodedLength < MIN_SLOT_BYTES
                || decodedLength > MAX_SLOT_BYTES
                || !isAdmitted(segmentId, offset, pageKind)) {
            return false;
        }
        // An entry already under this key is either the same immutable page,
        // which needs nothing, or a second answer for one page identity, which
        // must not survive alongside the first.
        final int existing = findIndex(segmentId, offset, pageKind);
        if (existing > -1) {
            if (matches(existing, ref)) {
                return true;
            }
            logPageRefConflict(existing, ref);
            removeAt(existing);
        }
        final int sizeClass = sizeClassOf(decodedLength);
        final long slot = allocateSlot(sizeClass);
        if (slot == 0) {
            return false;
        }
        Vect.memcpy(slot, decodedAddress, decodedLength);
        insert(segmentId, offset, pageKind, ref.getCodec(), ref.getRowCount(), decodedLength, slot);
        return true;
    }

    /**
     * Starts one restore's working-set measurement, discarding whatever a restore
     * that never reached {@link #endRestore()} had counted. A restore the caller
     * abandons - a corrupt root it falls back from, a failure it propagates - is
     * not a sample of anything, because it stopped part way through the pages it
     * would have read.
     */
    public void beginRestore() {
        restoreProbedBytes = 0;
    }

    /**
     * Moves the cache to a new epoch and drops every page the old one held.
     * <p>
     * The caller fires this when a timeline is retired, a repair is published or
     * a corrupt root is reconstructed - the transitions that can restart the
     * segment id space. Keeping any entry across one would let a re-minted id
     * serve the bytes of the file it replaced.
     */
    public void bumpEpoch() {
        epoch++;
        clearEntries();
    }

    @Override
    public void close() {
        clearEntries();
        final int slabCount = slabs.size();
        for (int i = 0; i < slabCount; i++) {
            Unsafe.free(slabs.getQuick(i), SLAB_BYTES, MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE);
        }
        if (slabCount > 0) {
            budget.release((long) slabCount * SLAB_BYTES);
        }
        slabs.clear();
        for (int i = 0; i < SIZE_CLASSES; i++) {
            freeSlots.getQuick(i).clear();
        }
        addresses = null;
        attrs = null;
        keys = null;
        mask = 0;
        occupied = 0;
        rehashThreshold = 0;
        size = 0;
    }

    /**
     * Closes the restore {@link #beginRestore()} opened: folds what it probed into
     * the working-set estimate and sets the admission fraction to the share of
     * that working set the budget can carry.
     * <p>
     * The share is what the budget still has free plus what this cache already
     * holds, so several views sharing one cap converge on splitting it rather than
     * each sizing itself against the whole. A view that stops refreshing hands its
     * slabs back at close and the views that remain widen into them at their next
     * restore.
     * <p>
     * Lowering the fraction evicts nothing. Capacity pressure must not evict here
     * (see the class comment), so the fraction governs what the cache takes in
     * next, and the pages it already holds keep serving until their segment goes.
     */
    public void endRestore() {
        final long probed = restoreProbedBytes;
        restoreProbedBytes = 0;
        // A cold cache takes its first restore whole rather than climbing to it
        // from zero, so one restore is enough to size the fraction.
        workingSetBytes = workingSetBytes > 0
                ? WORKING_SET_EWMA_ALPHA * probed + (1 - WORKING_SET_EWMA_ALPHA) * workingSetBytes
                : probed;
        if (isAdmissionFractionPinned) {
            return;
        }
        if (workingSetBytes <= 0) {
            applyAdmissionFraction(1);
            return;
        }
        final long share = budget.getCapacityBytes() - budget.getUsedBytes() + getUsedBytes();
        applyAdmissionFraction(share / workingSetBytes);
    }

    /**
     * Drops every page held in {@code segmentId}. The caller fires this when
     * compaction repacks a segment or the purge job deletes an unreferenced one:
     * the file is gone, so its pages can never be probed again and the slots they
     * hold are worth more to the pages that remain.
     */
    public void evictSegment(long segmentId) {
        if (size == 0) {
            return;
        }
        for (int i = 0, n = mask + 1; i < n; i++) {
            if (isOccupied(i) && keys[i * KEYS_STRIDE + KEY_SEGMENT_ID] == segmentId) {
                removeAt(i);
            }
        }
    }

    /**
     * Drops every page held in any of {@code segmentIds}, in one sweep of the
     * table rather than one per id. Callers hand it what a compaction pass
     * drained or what a purge unlinked, both of which name a handful of segments,
     * so the membership test stays a scan of the list.
     */
    public void evictSegments(@Nullable LongList segmentIds) {
        if (size == 0 || segmentIds == null || segmentIds.size() == 0) {
            return;
        }
        for (int i = 0, n = mask + 1; i < n; i++) {
            if (isOccupied(i) && segmentIds.indexOf(keys[i * KEYS_STRIDE + KEY_SEGMENT_ID]) > -1) {
                removeAt(i);
            }
        }
    }

    /**
     * @return the fraction of pages, by identity hash, the cache admits. One
     * means every page; zero means the cache is closed to new pages but still
     * serves what it holds
     */
    public double getAdmissionFraction() {
        return (double) admissionThreshold / ADMISSION_FULL;
    }

    public long getEpoch() {
        return epoch;
    }

    public long getHits() {
        return hits;
    }

    public long getMisses() {
        return misses;
    }

    public int getPageCount() {
        return size;
    }

    /**
     * @return pages the cache currently holds from {@code segmentId}, which is
     * what an eviction of that segment would drop
     */
    public int getSegmentPageCount(long segmentId) {
        if (size == 0) {
            return 0;
        }
        int count = 0;
        for (int i = 0, n = mask + 1; i < n; i++) {
            if (isOccupied(i) && keys[i * KEYS_STRIDE + KEY_SEGMENT_ID] == segmentId) {
                count++;
            }
        }
        return count;
    }

    /**
     * @return the native bytes this cache holds. Counts whole slabs, not the
     * pages inside them, because a slab stays with the cache once allocated
     */
    public long getUsedBytes() {
        return (long) slabs.size() * SLAB_BYTES;
    }

    /**
     * @return what holding every page one restore reads would cost this cache,
     * smoothed over the restores it has seen. Zero until the first
     * {@link #endRestore()}, which is when the admission fraction stops being a
     * guess
     */
    public long getWorkingSetBytes() {
        return (long) workingSetBytes;
    }

    /**
     * Looks {@code ref}'s page up.
     *
     * @return the address of the decoded image, valid until the next call that
     * evicts - {@link #admit}, {@link #evictSegment}, {@link #bumpEpoch} or
     * {@link #close} - or zero when the page is not cached
     */
    public long probe(@NotNull LiveViewCheckpointStatePageRef ref) {
        // Every page the restore asks for is part of its working set, whichever way
        // this probe goes: what endRestore sizes the fraction against is the cost of
        // holding the whole scan, not the cost of the part already held.
        restoreProbedBytes += slotBytesOf(ref.getDecodedLength());
        final int index = findIndex(ref.getSegmentId(), ref.getOffset(), ref.getPageKind());
        if (index > -1) {
            if (matches(index, ref)) {
                hits++;
                return addresses[index];
            }
            // A data segment's bytes are immutable, so two refs that name one
            // page and disagree on its shape mean one of them is wrong. Fail
            // closed to a decode and drop the entry rather than pick a winner.
            logPageRefConflict(index, ref);
            removeAt(index);
        }
        misses++;
        return 0;
    }

    /**
     * Sets the fraction of pages the cache admits, clamped to {@code [0, 1]}, and
     * pins it there: {@link #endRestore()} keeps measuring the working set but
     * stops moving the fraction. A caller that wants a cache to hold a known share
     * of what it reads - a differential that must keep two caches serving
     * differently, a switch that forces every probe to miss - needs a fraction the
     * self-tuner cannot walk back.
     * <p>
     * The decision is a hash of the page identity, so the admitted subset is the
     * same on every restore and shrinking the fraction only ever removes pages
     * from it - the pages that stay keep hitting.
     */
    public void setAdmissionFraction(double fraction) {
        isAdmissionFractionPinned = true;
        applyAdmissionFraction(fraction);
    }

    private static int bucketOf(long segmentId, long offset, int pageKind, int mask) {
        return (int) (Hash.hashLong256_64(segmentId, offset, pageKind, 0) & mask);
    }

    private static int sizeClassOf(int decodedLength) {
        return Numbers.msb(Numbers.ceilPow2(decodedLength)) - MIN_SLOT_SHIFT;
    }

    /**
     * @return the native bytes a page of {@code decodedLength} spends here, which
     * is its slot rather than its image: a chunk carries whatever rows one commit
     * appended, so rounding it up to its size class is most of the difference
     * between what a working set decodes to and what holding it costs. Zero for a
     * length no slot class covers, which is a page the cache would refuse
     */
    private static int slotBytesOf(int decodedLength) {
        return decodedLength < MIN_SLOT_BYTES || decodedLength > MAX_SLOT_BYTES
                ? 0
                : MIN_SLOT_BYTES << sizeClassOf(decodedLength);
    }

    /**
     * @return the address of a free slot of {@code sizeClass}, or zero when the
     * budget cannot cover another slab
     */
    private long allocateSlot(int sizeClass) {
        final LongList free = freeSlots.getQuick(sizeClass);
        final int freeCount = free.size();
        if (freeCount > 0) {
            final long address = free.getQuick(freeCount - 1);
            free.setPos(freeCount - 1);
            return address;
        }
        if (!budget.tryAcquire(SLAB_BYTES)) {
            return 0;
        }
        final long slab;
        try {
            slab = Unsafe.malloc(SLAB_BYTES, MemoryTag.NATIVE_LIVE_VIEW_CHECKPOINT_CACHE);
        } catch (Throwable th) {
            budget.release(SLAB_BYTES);
            throw th;
        }
        slabs.add(slab);
        final int slotBytes = MIN_SLOT_BYTES << sizeClass;
        for (int i = 1, n = SLAB_BYTES / slotBytes; i < n; i++) {
            free.add(slab + (long) i * slotBytes);
        }
        return slab;
    }

    private void allocateTable(int capacity) {
        keys = new long[capacity * KEYS_STRIDE];
        attrs = new int[capacity * ATTRS_STRIDE];
        addresses = new long[capacity];
        mask = capacity - 1;
        rehashThreshold = (int) (capacity * LOAD_FACTOR);
        occupied = 0;
    }

    private void applyAdmissionFraction(double fraction) {
        if (Double.isNaN(fraction) || fraction <= 0) {
            admissionThreshold = 0;
        } else if (fraction >= 1) {
            admissionThreshold = ADMISSION_FULL;
        } else {
            admissionThreshold = (long) (fraction * ADMISSION_FULL);
        }
    }

    /**
     * Returns every cached page to its free list and empties the table, keeping
     * the slabs so the next restore refills them without touching the budget.
     */
    private void clearEntries() {
        if (addresses == null) {
            return;
        }
        for (int i = 0, n = mask + 1; i < n; i++) {
            if (isOccupied(i)) {
                releaseSlot(i);
            }
            addresses[i] = 0;
        }
        occupied = 0;
        size = 0;
    }

    /**
     * @return the table index holding {@code (segmentId, offset, pageKind)}, or
     * -1 when the cache does not hold it
     */
    private int findIndex(long segmentId, long offset, int pageKind) {
        if (addresses == null) {
            return -1;
        }
        int index = bucketOf(segmentId, offset, pageKind, mask);
        while (true) {
            final long address = addresses[index];
            if (address == 0) {
                return -1;
            }
            if (address != TOMBSTONE && isKeyAt(index, segmentId, offset, pageKind)) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private void insert(
            long segmentId,
            long offset,
            int pageKind,
            int codec,
            int rowCount,
            int decodedLength,
            long slot
    ) {
        if (addresses == null) {
            allocateTable(INITIAL_CAPACITY);
        } else if (occupied >= rehashThreshold) {
            rehash();
        }
        int index = bucketOf(segmentId, offset, pageKind, mask);
        int tombstone = -1;
        while (addresses[index] != 0) {
            if (addresses[index] == TOMBSTONE && tombstone < 0) {
                tombstone = index;
            }
            index = (index + 1) & mask;
        }
        if (tombstone > -1) {
            // Reusing a tombstone leaves the occupied count alone: the slot was
            // already counted against the load factor when it was first taken.
            index = tombstone;
        } else {
            occupied++;
        }
        keys[index * KEYS_STRIDE + KEY_SEGMENT_ID] = segmentId;
        keys[index * KEYS_STRIDE + KEY_OFFSET] = offset;
        attrs[index * ATTRS_STRIDE + ATTR_PAGE_KIND] = pageKind;
        attrs[index * ATTRS_STRIDE + ATTR_CODEC] = codec;
        attrs[index * ATTRS_STRIDE + ATTR_ROW_COUNT] = rowCount;
        attrs[index * ATTRS_STRIDE + ATTR_DECODED_LENGTH] = decodedLength;
        addresses[index] = slot;
        size++;
    }

    private boolean isAdmitted(long segmentId, long offset, int pageKind) {
        if (admissionThreshold >= ADMISSION_FULL) {
            return true;
        }
        return (Hash.hashLong256_64(segmentId, offset, pageKind, ADMISSION_SALT) >>> 32) < admissionThreshold;
    }

    private boolean isKeyAt(int index, long segmentId, long offset, int pageKind) {
        return keys[index * KEYS_STRIDE + KEY_SEGMENT_ID] == segmentId
                && keys[index * KEYS_STRIDE + KEY_OFFSET] == offset
                && attrs[index * ATTRS_STRIDE + ATTR_PAGE_KIND] == pageKind;
    }

    private boolean isOccupied(int index) {
        return addresses != null && addresses[index] != 0 && addresses[index] != TOMBSTONE;
    }

    private void logPageRefConflict(int index, LiveViewCheckpointStatePageRef ref) {
        LOG.critical().$("live view checkpoint page cache entry contradicts its reference [segmentId=")
                .$(ref.getSegmentId())
                .$(", offset=").$(ref.getOffset())
                .$(", pageKind=").$(ref.getPageKind())
                .$(", cachedCodec=").$(attrs[index * ATTRS_STRIDE + ATTR_CODEC])
                .$(", codec=").$(ref.getCodec())
                .$(", cachedRowCount=").$(attrs[index * ATTRS_STRIDE + ATTR_ROW_COUNT])
                .$(", rowCount=").$(ref.getRowCount())
                .$(", cachedDecodedLength=").$(attrs[index * ATTRS_STRIDE + ATTR_DECODED_LENGTH])
                .$(", decodedLength=").$(ref.getDecodedLength())
                .I$();
    }

    private boolean matches(int index, LiveViewCheckpointStatePageRef ref) {
        return attrs[index * ATTRS_STRIDE + ATTR_CODEC] == ref.getCodec()
                && attrs[index * ATTRS_STRIDE + ATTR_ROW_COUNT] == ref.getRowCount()
                && attrs[index * ATTRS_STRIDE + ATTR_DECODED_LENGTH] == ref.getDecodedLength();
    }

    /**
     * Rebuilds the table, dropping the tombstones deletion left behind. Grows
     * only when the live entries alone crowd the load factor, so a cache that
     * churns at a steady size rehashes in place instead of climbing.
     */
    private void rehash() {
        int capacity = mask + 1;
        while (size + 1 > (int) (capacity * LOAD_FACTOR)) {
            capacity <<= 1;
        }
        final long[] oldKeys = keys;
        final int[] oldAttrs = attrs;
        final long[] oldAddresses = addresses;
        final int oldCapacity = mask + 1;
        allocateTable(capacity);
        for (int i = 0; i < oldCapacity; i++) {
            final long address = oldAddresses[i];
            if (address == 0 || address == TOMBSTONE) {
                continue;
            }
            int index = bucketOf(
                    oldKeys[i * KEYS_STRIDE + KEY_SEGMENT_ID],
                    oldKeys[i * KEYS_STRIDE + KEY_OFFSET],
                    oldAttrs[i * ATTRS_STRIDE + ATTR_PAGE_KIND],
                    mask
            );
            while (addresses[index] != 0) {
                index = (index + 1) & mask;
            }
            keys[index * KEYS_STRIDE + KEY_SEGMENT_ID] = oldKeys[i * KEYS_STRIDE + KEY_SEGMENT_ID];
            keys[index * KEYS_STRIDE + KEY_OFFSET] = oldKeys[i * KEYS_STRIDE + KEY_OFFSET];
            attrs[index * ATTRS_STRIDE + ATTR_PAGE_KIND] = oldAttrs[i * ATTRS_STRIDE + ATTR_PAGE_KIND];
            attrs[index * ATTRS_STRIDE + ATTR_CODEC] = oldAttrs[i * ATTRS_STRIDE + ATTR_CODEC];
            attrs[index * ATTRS_STRIDE + ATTR_ROW_COUNT] = oldAttrs[i * ATTRS_STRIDE + ATTR_ROW_COUNT];
            attrs[index * ATTRS_STRIDE + ATTR_DECODED_LENGTH] = oldAttrs[i * ATTRS_STRIDE + ATTR_DECODED_LENGTH];
            addresses[index] = address;
            occupied++;
        }
    }

    private void releaseSlot(int index) {
        freeSlots.getQuick(sizeClassOf(attrs[index * ATTRS_STRIDE + ATTR_DECODED_LENGTH])).add(addresses[index]);
    }

    /**
     * Frees the slot at {@code index} and tombstones the table entry. A tombstone
     * rather than a hole keeps the probe chains that run through it intact; the
     * next rehash collects them.
     */
    private void removeAt(int index) {
        releaseSlot(index);
        addresses[index] = TOMBSTONE;
        size--;
    }
}
