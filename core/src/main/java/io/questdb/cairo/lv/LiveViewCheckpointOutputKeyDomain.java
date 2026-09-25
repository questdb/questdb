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
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * {@code Q}, the output key domain of one localized repair: the partition keys the
 * timestamp-global replacement re-emits, in the encoding a checkpoint partition map
 * keys an entry by.
 * <p>
 * A repair whose replay reconstructs every live key needs none of this - the state it
 * freezes is the whole truth about every boundary it crosses. A ROWS dependency does
 * not reconstruct every key: {@code L} only warms up the keys with a row in
 * {@code [R, H)}, so a key outside that set ends the replay holding whatever rows
 * happened to fall inside {@code [L, H)} rather than its real last {@code Nmax}. This
 * set is what lets the publication tell the two apart - it takes the replay's entry
 * for a key inside {@code Q} and leaves every key outside it exactly as the old root
 * wrote it, which is correct because a key with no qualifying row in {@code [R, H)}
 * is a key the change did not touch.
 * <p>
 * The encoding is the one {@link LiveViewSnapshotKeyCodec} writes off a window
 * function's own map record, because that is what the two sides have to be comparable
 * in. {@link LiveViewCheckpointRowsPlan#getCheckpointKeySink()} is what produces it
 * from a base row: a SYMBOL partition column is a resolved STRING on both sides, never
 * a reader-local integer.
 * <p>
 * Every domain owns its keys: they live in a native {@link LiveViewCheckpointKeyArena}
 * of its own, and {@link #copyFrom} copies the bytes rather than sharing them. The
 * repair plan derives one of these per repair and the capture and the parked session
 * each take their own copy, because the plan and the keyed replay are refilled by the
 * next repair this worker runs while a parked capture still owes its publication. A
 * producer encodes a key straight into the domain between {@link #beginKey()} and
 * {@link #commitKey()}.
 * <p>
 * The set is open-addressed in native memory: one 16-byte slot per position, holding
 * the key's arena handle plus one (zero marks an empty slot) and the key's hash.
 * Membership is asked once per key per function root the boundary writes, so the probe
 * count is the key domain times the roots, and neither a probe nor an add allocates on
 * the heap. Matching is on content. The hash is {@code Hash.spread} of
 * {@code Arrays.hashCode} over the key bytes, and the growth schedule is that of the
 * heap table this replaced, so a domain filled by the same adds, clears and restores
 * iterates its keys in the same slot order. A copy holds the same keys at the geometry
 * that schedule reaches for them, allocated at once.
 * <p>
 * Allocation is lazy: a domain that never received a key holds no native memory. The
 * memory is tagged {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}, which the process totals
 * count, and no view's refresh tracker counts it: the domain replaced a heap set that
 * {@code cairo.live.view.refresh.memory.limit.bytes} never covered, a repair capture's
 * copy included, so every allocation and free here is untracked.
 */
public final class LiveViewCheckpointOutputKeyDomain implements Mutable, QuietCloseable {
    private static final double LOAD_FACTOR = 0.4;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final int SLOT_BYTES = 16;
    private static final int SLOT_HASH_OFFSET = Long.BYTES;
    private final LiveViewCheckpointKeyArena keys = new LiveViewCheckpointKeyArena();
    private int capacity;
    private int free;
    private int mask;
    private int slotCount;
    private long slotsAddress;
    private long slotsBytes;

    public LiveViewCheckpointOutputKeyDomain() {
        resetGeometry();
    }

    /**
     * Drops the key {@link #beginKey()} started without joining it to the domain.
     */
    public void abortKey() {
        keys.abortKey();
    }

    /**
     * Joins the {@code keyLength} key bytes at {@code keyAddress} to the domain, copying
     * them. Adding a key the domain already holds leaves it as it is. The bytes must not
     * lie in this domain's own storage.
     */
    public void add(long keyAddress, int keyLength) {
        ensureSlots();
        final int hash = LiveViewCheckpointKeys.hash(keyAddress, keyLength);
        final int index = findSlot(hash, keyAddress, keyLength);
        if (index < 0) {
            return;
        }
        occupy(index, keys.append(keyAddress, keyLength), hash);
    }

    /**
     * Starts encoding one key straight into the domain's storage. The caller writes the
     * key's bytes through the returned sink and then calls {@link #commitKey()}. The sink
     * is valid only until then.
     */
    public MemoryA beginKey() {
        ensureSlots();
        return keys.beginKey();
    }

    /**
     * Empties the domain, keeping the slot table and key storage it has grown to so a
     * reused domain does not pay for the growth again.
     */
    @Override
    public void clear() {
        keys.clear();
        if (slotsAddress != 0) {
            Vect.memset(slotsAddress, slotsBytes, 0);
        }
        free = capacity;
    }

    /**
     * Frees every native allocation. The domain is left empty and usable. Idempotent.
     */
    @Override
    public void close() {
        freeSlots();
        keys.release();
        resetGeometry();
    }

    /**
     * Ends the key {@link #beginKey()} started and joins it to the domain. A key the
     * domain already holds takes no storage.
     */
    public void commitKey() {
        // beginKey() allocated the table, and nothing in between may clear the domain.
        assert slotsAddress != 0 : "live view checkpoint output key committed without its table";
        final long handle = keys.commitKey();
        final long keyAddress = keys.address(handle);
        final int keyLength = keys.length(handle);
        final int hash = LiveViewCheckpointKeys.hash(keyAddress, keyLength);
        final int index = findSlot(hash, keyAddress, keyLength);
        if (index < 0) {
            keys.discardLastKey(handle);
            return;
        }
        occupy(index, handle, hash);
    }

    public boolean contains(long keyAddress, int keyLength) {
        return slotsAddress != 0 && findSlot(LiveViewCheckpointKeys.hash(keyAddress, keyLength), keyAddress, keyLength) < 0;
    }

    /**
     * Replaces this domain with {@code other}'s keys, copied into storage of its own, so
     * nothing {@code other} does afterwards reaches the copy. The copy takes the geometry
     * {@link #add} would grow to for {@code other}'s key count, allocated once, so it is
     * sized for the keys it holds - not for whatever either domain grew to before - and
     * its peak footprint is the footprint it keeps. It places {@code other}'s keys in
     * {@code other}'s slot order; no consumer reads where a copy's colliding keys land, so
     * a copy's slot order may differ from its source's. A copy that fails to allocate
     * leaves this domain empty.
     */
    public void copyFrom(@NotNull LiveViewCheckpointOutputKeyDomain other) {
        if (other == this) {
            return;
        }
        restoreInitialCapacity();
        final int keyCount = other.size();
        if (keyCount == 0) {
            return;
        }
        try {
            // Handles are arena offsets, and the arena is a byte-for-byte copy of the
            // records other holds, so each handle names the same key here. The key bytes
            // are never re-read: every slot carries its key's hash.
            keys.copyFrom(other.keys);
            // add() doubles the capacity the moment the keys fill it, so the capacity it
            // holds keyCount keys at is the first doubling above keyCount.
            int copyCapacity = MIN_INITIAL_CAPACITY;
            while (copyCapacity <= keyCount) {
                copyCapacity *= 2;
            }
            capacity = copyCapacity;
            free = copyCapacity;
            slotCount = slotCountFor(copyCapacity);
            mask = slotCount - 1;
            ensureSlots();
            for (int i = 0, n = other.slotCount; i < n; i++) {
                final long otherSlot = other.slotAddress(i);
                final long handlePlusOne = Unsafe.getLong(otherSlot);
                if (handlePlusOne != 0) {
                    // other holds each key once, so its place is the first empty slot the
                    // probe from its hash reaches.
                    final int hash = Unsafe.getInt(otherSlot + SLOT_HASH_OFFSET);
                    int index = hash & mask;
                    while (Unsafe.getLong(slotAddress(index)) != 0) {
                        index = (index + 1) & mask;
                    }
                    final long slot = slotAddress(index);
                    Unsafe.putLong(slot, handlePlusOne);
                    Unsafe.putInt(slot + SLOT_HASH_OFFSET, hash);
                    free--;
                }
            }
            // The capacity sits above the key count, so the table never regrows.
            assert free > 0 && size() == keyCount;
        } catch (Throwable th) {
            // The table would otherwise name handles the arena does not hold.
            restoreInitialCapacity();
            throw th;
        }
    }

    /**
     * @return the address of the key bytes in {@code slot}, which must be used. Valid
     * until this domain next changes.
     */
    public long getKeyAddress(int slot) {
        return keys.address(handleAt(slot));
    }

    /**
     * @return the length of the key in {@code slot}, which must be used
     */
    public int getKeyLength(int slot) {
        return keys.length(handleAt(slot));
    }

    /**
     * @return the number of slots to iterate with {@link #isSlotUsed(int)}; zero for a
     * domain that holds no table
     */
    public int getSlotCount() {
        return slotsAddress == 0 ? 0 : slotCount;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean isSlotUsed(int slot) {
        return Unsafe.getLong(slotAddress(slot)) != 0;
    }

    /**
     * Empties the domain and frees its slot table and key storage, back to the initial
     * geometry. For an owner whose domains vary widely in width: {@link #clear()} keeps
     * what one wide domain grew, and every later clear then sweeps all of it however few
     * keys it held.
     */
    public void restoreInitialCapacity() {
        freeSlots();
        keys.release();
        resetGeometry();
    }

    public int size() {
        return capacity - free;
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

    /**
     * The slot the key belongs in when the domain does not hold it, or
     * {@code -slot - 1} when it does. The table must be allocated.
     */
    private int findSlot(int hash, long keyAddress, int keyLength) {
        int index = hash & mask;
        while (true) {
            final long slot = slotAddress(index);
            final long handlePlusOne = Unsafe.getLong(slot);
            if (handlePlusOne == 0) {
                return index;
            }
            if (Unsafe.getInt(slot + SLOT_HASH_OFFSET) == hash) {
                final long handle = handlePlusOne - 1;
                if (LiveViewCheckpointKeys.equals(keys.address(handle), keys.length(handle), keyAddress, keyLength)) {
                    return -index - 1;
                }
            }
            index = (index + 1) & mask;
        }
    }

    private void freeSlots() {
        if (slotsAddress != 0) {
            Unsafe.free(slotsAddress, slotsBytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            slotsAddress = 0;
            slotsBytes = 0;
        }
    }

    private long handleAt(int slot) {
        final long handlePlusOne = Unsafe.getLong(slotAddress(slot));
        assert handlePlusOne != 0 : "live view checkpoint output key slot is empty";
        return handlePlusOne - 1;
    }

    private void occupy(int index, long handle, int hash) {
        final long slot = slotAddress(index);
        Unsafe.putLong(slot, handle + 1);
        Unsafe.putInt(slot + SLOT_HASH_OFFSET, hash);
        if (--free < 1) {
            rehash();
        }
    }

    /**
     * Doubles the capacity and re-places every key by its stored hash, visiting the old
     * slots in order - the schedule and the order the heap table used, so the slot order
     * a domain iterates in is unchanged. Key bytes are not re-read.
     */
    private void rehash() {
        final long oldAddress = slotsAddress;
        final long oldBytes = slotsBytes;
        final int oldSlotCount = slotCount;
        final int newCapacity = capacity * 2;
        final int newSlotCount = slotCountFor(newCapacity);
        final long newBytes = (long) newSlotCount * SLOT_BYTES;
        final long newAddress = Unsafe.malloc(newBytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        Vect.memset(newAddress, newBytes, 0);
        slotsAddress = newAddress;
        slotsBytes = newBytes;
        slotCount = newSlotCount;
        mask = newSlotCount - 1;
        capacity = newCapacity;
        free = newCapacity;
        for (int i = 0; i < oldSlotCount; i++) {
            final long oldSlot = oldAddress + (long) i * SLOT_BYTES;
            final long handlePlusOne = Unsafe.getLong(oldSlot);
            if (handlePlusOne != 0) {
                final int hash = Unsafe.getInt(oldSlot + SLOT_HASH_OFFSET);
                int index = hash & mask;
                while (Unsafe.getLong(slotAddress(index)) != 0) {
                    index = (index + 1) & mask;
                }
                final long slot = slotAddress(index);
                Unsafe.putLong(slot, handlePlusOne);
                Unsafe.putInt(slot + SLOT_HASH_OFFSET, hash);
                free--;
            }
        }
        Unsafe.free(oldAddress, oldBytes, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
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
