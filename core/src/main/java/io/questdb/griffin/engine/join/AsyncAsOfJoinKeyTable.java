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

package io.questdb.griffin.engine.join;

import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Per-key state of {@link AsyncAsOfJoinRecordCursorFactory}, owned by one worker slot (or by the
 * query's thread): per key, the last slave row met and the row a backward walk has walked back
 * from. Keys are non-negative ints.
 * <p>
 * Entries are tagged with an epoch: {@link #nextEpoch()} starts a page frame and empties the
 * table without touching its memory, since an entry of an older epoch reads as absent.
 * <p>
 * The table holds the keys it is given, no more: nothing is allocated until the first key, and
 * memory grows with the keys met. While the keys are dense - below the dense key count the owner
 * declares, or not far above the number of keys met - and below {@link #DIRECT_LIMIT}, an entry
 * sits at its key's index (an array, no hashing); otherwise the table is an open addressing hash
 * table sized by the keys of one epoch. Memory is charged to the query's tracker, so growth can
 * throw the tracker's out-of-memory {@code CairoException}.
 * <p>
 * Not thread-safe: one slot's frames are reduced by one thread at a time.
 */
public final class AsyncAsOfJoinKeyTable implements QuietCloseable {
    static final int DIRECT_LIMIT = 1 << 16;
    static final long NO_ROW = -1;
    private static final int DIRECT_INITIAL_CAPACITY = 256;
    private static final int ENTRY_BYTES = 24;
    private static final int HASH_INITIAL_CAPACITY = 1024;
    private static final int MEMORY_TAG = MemoryTag.NATIVE_UNORDERED_MAP;
    private long address;
    private int capacity;
    // keys below this may take the array, however few keys have been met
    private int denseKeyCount;
    private boolean direct = true;
    private int epoch;
    // entries tagged with the current epoch
    private int liveCount;
    private int mask;
    private @Nullable MemoryTracker memoryTracker;
    private int shift;

    /**
     * The entry of a key, created (last row and walked-to row both {@link #NO_ROW}) when the
     * current epoch has none: its address, the last row at +8, the walked-to row at +16.
     */
    public long entry(int key) {
        if (direct) {
            if (key < capacity) {
                final long e = address + (long) key * ENTRY_BYTES;
                final long tag = tagOf(key);
                if (Unsafe.getLong(e) != tag) {
                    initEntry(e, tag);
                    liveCount++;
                }
                return e;
            }
            return entrySlow(key);
        }
        return hashEntry(key, true);
    }

    /**
     * The entry of a key in the current epoch, 0 when it has none.
     */
    public long find(int key) {
        if (direct) {
            if (key < capacity) {
                final long e = address + (long) key * ENTRY_BYTES;
                return Unsafe.getLong(e) == tagOf(key) ? e : 0;
            }
            return 0;
        }
        return hashEntry(key, false);
    }

    @Override
    public void close() {
        free();
    }

    public void free() {
        address = Unsafe.free(address, (long) capacity * ENTRY_BYTES, MEMORY_TAG, memoryTracker);
        capacity = 0;
        mask = 0;
        direct = true;
        epoch = 0;
        liveCount = 0;
    }

    @TestOnly
    public int getCapacity() {
        return capacity;
    }

    @TestOnly
    public boolean isDirect() {
        return direct;
    }

    @TestOnly
    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    /**
     * Starts an epoch: every entry reads as absent from here on.
     */
    public void nextEpoch() {
        if (++epoch == 0) {
            // wrapped: untag every entry, so that no stale tag can match
            if (address != 0) {
                Vect.memset(address, (long) capacity * ENTRY_BYTES, 0);
            }
            epoch = 1;
        }
        liveCount = 0;
    }

    /**
     * Frees the memory and binds the tracker the next allocations charge.
     *
     * @param denseKeyCount the keys are dense below this count (0 for none): such a key takes the
     *                      array as long as it fits {@link #DIRECT_LIMIT}
     */
    public void of(@Nullable MemoryTracker memoryTracker, int denseKeyCount) {
        free();
        this.memoryTracker = memoryTracker;
        this.denseKeyCount = denseKeyCount;
    }

    private static void initEntry(long e, long tag) {
        Unsafe.putLong(e, tag);
        Unsafe.putLong(e + 8, NO_ROW);
        Unsafe.putLong(e + 16, NO_ROW);
    }

    private long allocateZeroed(int entries) {
        final long bytes = (long) entries * ENTRY_BYTES;
        final long a = Unsafe.malloc(bytes, MEMORY_TAG, memoryTracker);
        Vect.memset(a, bytes, 0);
        return a;
    }

    private long entrySlow(int key) {
        assert direct;
        // an array covering the key must not be mostly empty: the key is one of the dense ones, or
        // within a small multiple of the keys met so far
        if (key < DIRECT_LIMIT && (key < denseKeyCount || key < 16L * (liveCount + 16))) {
            // grow the array to cover the key; the new entries read as absent
            final int newCapacity = Math.max(Math.max(DIRECT_INITIAL_CAPACITY, capacity << 1), Numbers.ceilPow2(key + 1));
            final long oldBytes = (long) capacity * ENTRY_BYTES;
            final long newBytes = (long) newCapacity * ENTRY_BYTES;
            address = address == 0
                    ? Unsafe.malloc(newBytes, MEMORY_TAG, memoryTracker)
                    : Unsafe.realloc(address, oldBytes, newBytes, MEMORY_TAG, memoryTracker);
            Vect.memset(address + oldBytes, newBytes - oldBytes, 0);
            capacity = newCapacity;
            mask = newCapacity - 1;
            return entry(key);
        }
        // a key past the array's limit: hash the live entries
        rehash(Math.max(HASH_INITIAL_CAPACITY, Numbers.ceilPow2(4 * (liveCount + 1))));
        return hashEntry(key, true);
    }

    private long hashEntry(int key, boolean insert) {
        final long tag = tagOf(key);
        int h = (int) ((key * 0x9E3779B97F4A7C15L) >>> shift);
        while (true) {
            final long e = address + (long) h * ENTRY_BYTES;
            final long t = Unsafe.getLong(e);
            if (t == tag) {
                return e;
            }
            if ((int) (t >>> 32) != epoch) {
                // an entry of an older epoch is a free slot
                if (!insert) {
                    return 0;
                }
                if (2 * (liveCount + 1) > capacity) {
                    rehash(capacity << 1);
                    return hashEntry(key, true);
                }
                initEntry(e, tag);
                liveCount++;
                return e;
            }
            h = (h + 1) & mask;
        }
    }

    // moves the current epoch's entries into a hash table of the given capacity
    private void rehash(int newCapacity) {
        final long newAddress = allocateZeroed(newCapacity);
        final long oldAddress = address;
        final int oldCapacity = capacity;
        final int newShift = 64 - Numbers.msb(newCapacity);
        final int newMask = newCapacity - 1;
        int live = 0;
        for (int i = 0; i < oldCapacity; i++) {
            final long e = oldAddress + (long) i * ENTRY_BYTES;
            final long t = Unsafe.getLong(e);
            if ((int) (t >>> 32) == epoch) {
                final int key = (int) t;
                int h = (int) ((key * 0x9E3779B97F4A7C15L) >>> newShift);
                while (Unsafe.getLong(newAddress + (long) h * ENTRY_BYTES) != 0) {
                    h = (h + 1) & newMask;
                }
                Vect.memcpy(newAddress + (long) h * ENTRY_BYTES, e, ENTRY_BYTES);
                live++;
            }
        }
        Unsafe.free(oldAddress, (long) oldCapacity * ENTRY_BYTES, MEMORY_TAG, memoryTracker);
        address = newAddress;
        capacity = newCapacity;
        mask = newMask;
        shift = newShift;
        direct = false;
        liveCount = live;
    }

    private long tagOf(int key) {
        return ((long) epoch << 32) | (key & 0xffffffffL);
    }
}
