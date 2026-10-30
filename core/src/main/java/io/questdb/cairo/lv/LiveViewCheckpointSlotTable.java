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

import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * Native open-addressed slot table that {@link LiveViewCheckpointKeyIndex} and
 * {@link LiveViewCheckpointOutputKeyDomain} hold their keys in. A slot starts with
 * {@code [long handle + 1][int hash]}, zero marking an empty slot, and its owner lays its
 * own fields out from {@link #HEADER_BYTES} on. The table is a power of two of slots,
 * probed linearly from a key's hash, and doubles the moment its entries reach its load
 * factor's share of the slots, rounded down.
 * <p>
 * The owner probes and matches keys itself; the table allocates, grows, copies and frees.
 * It re-places a slot by the hash the slot stores, visiting the old slots in order, so it
 * never re-reads a key, and a table filled by the same puts lays its entries out in the
 * same slot order.
 * <p>
 * Allocation is lazy: the constructor reserves nothing, so a field initializer never
 * strands native memory when its owner's constructor throws. The memory is tagged
 * {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}, which the process totals count, and no view's
 * refresh tracker counts it, because neither owner's heap predecessor was counted.
 */
final class LiveViewCheckpointSlotTable implements Mutable, QuietCloseable {
    static final int HASH_OFFSET = Long.BYTES;
    static final int HEADER_BYTES = HASH_OFFSET + Integer.BYTES;
    // Keeps the doubling below the int range.
    private static final int MAX_SLOT_COUNT = 1 << 30;
    private final double loadFactor;
    private final int minSlotCount;
    private final int slotBytes;
    // The entry count that doubles the table: loadFactor of its slots, rounded down.
    private int capacity;
    private int free;
    private int mask;
    private int slotCount;
    private long slotsAddress;
    private long slotsBytes;

    LiveViewCheckpointSlotTable(int slotBytes, int minSlotCount, double loadFactor) {
        assert slotBytes >= HEADER_BYTES && minSlotCount > 0 && (minSlotCount & (minSlotCount - 1)) == 0;
        this.slotBytes = slotBytes;
        this.minSlotCount = minSlotCount;
        this.loadFactor = loadFactor;
        setGeometry(minSlotCount);
    }

    /**
     * Forgets every entry while keeping the table it has grown to.
     */
    @Override
    public void clear() {
        if (slotsAddress != 0) {
            Vect.memset(slotsAddress, slotsBytes, 0);
        }
        free = capacity;
    }

    /**
     * Frees the table and goes back to the smallest geometry. The table stays usable: the
     * next {@link #ensureAllocated()} allocates afresh. Idempotent.
     */
    @Override
    public void close() {
        if (slotsAddress != 0) {
            Unsafe.free(slotsAddress, slotsBytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            slotsAddress = 0;
            slotsBytes = 0;
        }
        setGeometry(minSlotCount);
    }

    /**
     * Replaces this table's entries with {@code other}'s, at the geometry that growth
     * reaches for them, allocated at once: sized for the entries it holds, not for whatever
     * either table grew to before. It places them in {@code other}'s slot order, so a
     * colliding entry may land in another slot than it holds in {@code other}. A copy that
     * fails to allocate leaves this table empty.
     */
    void copyFrom(@NotNull LiveViewCheckpointSlotTable other) {
        assert other != this && other.slotBytes == slotBytes;
        close();
        final int entryCount = other.size();
        if (entryCount > 0) {
            setGeometry(slotCountFor(entryCount, MAX_SLOT_COUNT));
            ensureAllocated();
            place(other.slotsAddress, other.slotCount);
        }
    }

    /**
     * Allocates the table at its current geometry when it holds none.
     */
    void ensureAllocated() {
        if (slotsAddress == 0) {
            final long bytes = (long) slotCount * slotBytes;
            final long address = Unsafe.malloc(bytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            Vect.memset(address, bytes, 0);
            slotsAddress = address;
            slotsBytes = bytes;
        }
    }

    boolean isAllocated() {
        return slotsAddress != 0;
    }

    int mask() {
        return mask;
    }

    /**
     * Fills {@code slot}, an empty slot of this table, with {@code handle} and {@code hash},
     * then doubles the table if that filled it. A doubling moves every slot, so the owner
     * writes its own fields into {@code slot} first and does not touch it afterwards.
     */
    void occupy(long slot, long handle, int hash) {
        Unsafe.putLong(slot, handle + 1);
        Unsafe.putInt(slot + HASH_OFFSET, hash);
        if (--free < 1) {
            rehash(slotCount * 2);
        }
    }

    /**
     * Sizes the table so it holds {@code entryCount} entries in all without growing, in one
     * allocation, up to {@code maxSlotCount} slots. A table already that large is left as
     * it is, and an empty one stays unallocated until its next use.
     */
    void reserve(long entryCount, int maxSlotCount) {
        final int target = slotCountFor(entryCount, maxSlotCount);
        if (target <= slotCount) {
            return;
        }
        if (size() == 0) {
            close();
            setGeometry(target);
        } else {
            rehash(target);
        }
    }

    int size() {
        return capacity - free;
    }

    long slotAddress(int index) {
        return slotsAddress + (long) index * slotBytes;
    }

    int slotCount() {
        return slotCount;
    }

    private int capacityOf(int slotCount) {
        return (int) (slotCount * loadFactor);
    }

    /**
     * Copies every used slot of the {@code sourceSlotCount} slots at {@code sourceAddress}
     * into this table, visiting them in order. The source holds each entry once, so an
     * entry's place is the first empty slot the probe from its stored hash reaches. The
     * table must have room for all of them.
     */
    private void place(long sourceAddress, int sourceSlotCount) {
        for (int i = 0; i < sourceSlotCount; i++) {
            final long source = sourceAddress + (long) i * slotBytes;
            if (Unsafe.getLong(source) != 0) {
                int index = Unsafe.getInt(source + HASH_OFFSET) & mask;
                while (Unsafe.getLong(slotAddress(index)) != 0) {
                    index = (index + 1) & mask;
                }
                Vect.memcpy(slotAddress(index), source, slotBytes);
                free--;
            }
        }
    }

    /**
     * Moves every entry into a table of {@code newSlotCount} slots, allocated before
     * anything changes.
     */
    private void rehash(int newSlotCount) {
        final long newBytes = (long) newSlotCount * slotBytes;
        final long newAddress = Unsafe.malloc(newBytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        Vect.memset(newAddress, newBytes, 0);
        final long oldAddress = slotsAddress;
        final long oldBytes = slotsBytes;
        final int oldSlotCount = slotCount;
        slotsAddress = newAddress;
        slotsBytes = newBytes;
        setGeometry(newSlotCount);
        if (oldAddress != 0) {
            place(oldAddress, oldSlotCount);
            Unsafe.free(oldAddress, oldBytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        }
    }

    private void setGeometry(int slotCount) {
        this.slotCount = slotCount;
        mask = slotCount - 1;
        capacity = capacityOf(slotCount);
        free = capacity;
    }

    /**
     * @return the smallest slot count, from the minimum up to {@code maxSlotCount}, whose
     * table holds {@code entryCount} entries without growing
     */
    private int slotCountFor(long entryCount, int maxSlotCount) {
        int target = minSlotCount;
        while (capacityOf(target) <= entryCount && target < maxSlotCount) {
            target *= 2;
        }
        return target;
    }
}
