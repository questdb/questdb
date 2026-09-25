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

import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * Native open-addressed index from {@code (namespace, version, partition key)} to a
 * non-negative int, over the keys of one {@link LiveViewCheckpointKeyArena}. The two
 * primitive qualifiers let one flat table serve several function ordinals and state
 * versions without joining them to the key.
 * <p>
 * The index stores each key as its arena handle, never its bytes, so it is only as valid
 * as the arena it was built over: a handle goes stale when that arena is cleared,
 * released or closed. A probe passes its key as an {@code (address, length)} pair valid
 * for the call; a put passes the handle of a key already in the arena.
 * <p>
 * One slot is {@code [long handle + 1][int hash][int namespace][int version][int value]},
 * 24 bytes, zero marking an empty slot. The geometry is that of
 * {@link LiveViewCheckpointBinaryKeyIndex}: load factor 0.4 over a power-of-two table,
 * doubling the moment the entries reach the capacity. A rehash re-places slots by their
 * stored hash and never re-reads key bytes, and {@link #reserve(long)} sizes the table
 * for a known key count in one allocation.
 * <p>
 * Allocation is lazy: the constructor reserves nothing, so a field initializer never
 * strands native memory when its owner's constructor throws. The memory is tagged
 * {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}, which the process totals count, and no
 * view's refresh tracker counts it: the index replaced a heap table that
 * {@code cairo.live.view.refresh.memory.limit.bytes} never covered, so every allocation
 * and free here is untracked.
 */
final class LiveViewCheckpointKeyIndex implements Mutable, QuietCloseable {
    private static final double LOAD_FACTOR = 0.4;
    // reserve() stops presizing here, 2^24 entries in a 1.5 GiB table; a put past it
    // grows the table as it fills.
    private static final int MAX_RESERVED_CAPACITY = 1 << 24;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final int SLOT_BYTES = 24;
    private static final int SLOT_HASH_OFFSET = Long.BYTES;
    private static final int SLOT_NAMESPACE_OFFSET = SLOT_HASH_OFFSET + Integer.BYTES;
    private static final int SLOT_VERSION_OFFSET = SLOT_NAMESPACE_OFFSET + Integer.BYTES;
    private static final int SLOT_VALUE_OFFSET = SLOT_VERSION_OFFSET + Integer.BYTES;
    private final LiveViewCheckpointKeyArena keys;
    private int capacity;
    private int free;
    private int mask;
    private int slotCount;
    private long slotsAddress;
    private long slotsBytes;

    LiveViewCheckpointKeyIndex(@NotNull LiveViewCheckpointKeyArena keys) {
        this.keys = keys;
        resetGeometry();
    }

    /**
     * Forgets every entry while keeping the table the index has grown to.
     */
    @Override
    public void clear() {
        if (slotsAddress != 0) {
            Vect.memset(slotsAddress, slotsBytes, 0);
        }
        free = capacity;
    }

    /**
     * Frees the table, exactly as {@link #release()} does. Idempotent.
     */
    @Override
    public void close() {
        release();
    }

    /**
     * @return the value put for the {@code keyLength} key bytes at {@code keyAddress}
     * under these qualifiers, or -1 when the index holds none
     */
    int get(int namespace, int version, long keyAddress, int keyLength) {
        if (slotsAddress == 0) {
            return -1;
        }
        final int hash = hash(namespace, version, keyAddress, keyLength);
        int index = hash & mask;
        while (true) {
            final long slot = slotAddress(index);
            final long handlePlusOne = Unsafe.getLong(slot);
            if (handlePlusOne == 0) {
                return -1;
            }
            if (isMatch(slot, handlePlusOne, hash, namespace, version, keyAddress, keyLength)) {
                return Unsafe.getInt(slot + SLOT_VALUE_OFFSET);
            }
            index = (index + 1) & mask;
        }
    }

    /**
     * Maps the key {@code keyHandle} names in this index's arena, under these qualifiers,
     * to {@code value}, replacing the value an equal key already maps to.
     */
    void put(int namespace, int version, long keyHandle, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("negative live view checkpoint key index value");
        }
        ensureSlots();
        // put() appends nothing to the arena, so the address holds for the whole probe.
        final long keyAddress = keys.address(keyHandle);
        final int keyLength = keys.length(keyHandle);
        final int hash = hash(namespace, version, keyAddress, keyLength);
        int index = hash & mask;
        while (true) {
            final long slot = slotAddress(index);
            final long handlePlusOne = Unsafe.getLong(slot);
            if (handlePlusOne == 0) {
                Unsafe.putLong(slot, keyHandle + 1);
                Unsafe.putInt(slot + SLOT_HASH_OFFSET, hash);
                Unsafe.putInt(slot + SLOT_NAMESPACE_OFFSET, namespace);
                Unsafe.putInt(slot + SLOT_VERSION_OFFSET, version);
                Unsafe.putInt(slot + SLOT_VALUE_OFFSET, value);
                if (--free < 1) {
                    rehash(capacity * 2);
                }
                return;
            }
            if (isMatch(slot, handlePlusOne, hash, namespace, version, keyAddress, keyLength)) {
                Unsafe.putInt(slot + SLOT_VALUE_OFFSET, value);
                return;
            }
            index = (index + 1) & mask;
        }
    }

    /**
     * Frees the table. The index stays usable: the next put allocates afresh.
     */
    void release() {
        freeSlots();
        resetGeometry();
    }

    /**
     * Sizes the table so it holds {@code keyCount} entries in all without growing, in one
     * allocation. A table already that large is left as it is.
     */
    void reserve(long keyCount) {
        int target = MIN_INITIAL_CAPACITY;
        while (target <= keyCount && target < MAX_RESERVED_CAPACITY) {
            target *= 2;
        }
        if (target <= capacity) {
            return;
        }
        if (size() == 0) {
            freeSlots();
            capacity = target;
            free = target;
            slotCount = slotCountFor(target);
            mask = slotCount - 1;
        } else {
            rehash(target);
        }
    }

    int size() {
        return capacity - free;
    }

    private static int hash(int namespace, int version, long keyAddress, int keyLength) {
        return Hash.spread(31 * (31 * LiveViewCheckpointKeys.hashCode(keyAddress, keyLength) + namespace) + version);
    }

    private static int slotCountFor(int capacity) {
        return Numbers.ceilPow2((int) (capacity / LOAD_FACTOR));
    }

    private void ensureSlots() {
        if (slotsAddress == 0) {
            final long bytes = (long) slotCount * SLOT_BYTES;
            final long address = Unsafe.malloc(bytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            Vect.memset(address, bytes, 0);
            slotsAddress = address;
            slotsBytes = bytes;
        }
    }

    private void freeSlots() {
        if (slotsAddress != 0) {
            Unsafe.free(slotsAddress, slotsBytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            slotsAddress = 0;
            slotsBytes = 0;
        }
    }

    private boolean isMatch(
            long slot,
            long handlePlusOne,
            int hash,
            int namespace,
            int version,
            long keyAddress,
            int keyLength
    ) {
        if (Unsafe.getInt(slot + SLOT_HASH_OFFSET) != hash
                || Unsafe.getInt(slot + SLOT_NAMESPACE_OFFSET) != namespace
                || Unsafe.getInt(slot + SLOT_VERSION_OFFSET) != version) {
            return false;
        }
        final long handle = handlePlusOne - 1;
        return LiveViewCheckpointKeys.equals(keys.address(handle), keys.length(handle), keyAddress, keyLength);
    }

    /**
     * Moves every entry into a table of {@code newCapacity}, allocated before anything
     * changes, placing each slot by its stored hash in old-slot order.
     */
    private void rehash(int newCapacity) {
        final int newSlotCount = slotCountFor(newCapacity);
        final long newBytes = (long) newSlotCount * SLOT_BYTES;
        final long newAddress = Unsafe.malloc(newBytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        Vect.memset(newAddress, newBytes, 0);
        final long oldAddress = slotsAddress;
        final long oldBytes = slotsBytes;
        final int oldSlotCount = slotCount;
        final int newMask = newSlotCount - 1;
        int entries = 0;
        for (int i = 0; i < oldSlotCount; i++) {
            final long oldSlot = oldAddress + (long) i * SLOT_BYTES;
            if (Unsafe.getLong(oldSlot) != 0) {
                int index = Unsafe.getInt(oldSlot + SLOT_HASH_OFFSET) & newMask;
                while (Unsafe.getLong(newAddress + (long) index * SLOT_BYTES) != 0) {
                    index = (index + 1) & newMask;
                }
                Vect.memcpy(newAddress + (long) index * SLOT_BYTES, oldSlot, SLOT_BYTES);
                entries++;
            }
        }
        slotsAddress = newAddress;
        slotsBytes = newBytes;
        slotCount = newSlotCount;
        mask = newMask;
        capacity = newCapacity;
        free = newCapacity - entries;
        if (oldAddress != 0) {
            Unsafe.free(oldAddress, oldBytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        }
    }

    private void resetGeometry() {
        capacity = MIN_INITIAL_CAPACITY;
        free = capacity;
        slotCount = slotCountFor(capacity);
        mask = slotCount - 1;
    }

    private long slotAddress(int index) {
        return slotsAddress + (long) index * SLOT_BYTES;
    }
}
