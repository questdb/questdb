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
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
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
 * Every domain owns its keys: they live in a native {@link LiveViewCheckpointRecordArena}
 * of its own, and {@link #copyFrom} copies the bytes rather than sharing them. The
 * repair plan derives one of these per repair and the capture takes its own copy,
 * because the plan and the keyed replay are refilled by the next repair this worker runs
 * while a parked capture still owes its publication. The parked session keeps the plan's
 * bounds and not its keys. A producer encodes a key straight into the domain between
 * {@link #beginKey()} and {@link #commitKey()}.
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
    /**
     * The widest domain, in keys, whose storage an owner that reuses it from repair to
     * repair keeps for the next one. The refresh worker's repair plan and keyed replay are
     * such owners. {@link #clear()} keeps the table the widest domain grew, and every later
     * clear sweeps all of it however few keys it held, so one wide repair would charge every
     * later one on that worker for its width. It also bounds the slot table such an owner
     * keeps, to 128 KiB.
     */
    public static final int MAX_RETAINED_KEYS = 1024;
    /**
     * The key storage, in native bytes used or not, that such an owner keeps for the next
     * repair. {@link #MAX_RETAINED_KEYS} bounds the slot table and not the keys, so a few
     * wide keys would otherwise pin an arena of any size on the worker: a thousand
     * 16,000-character STRING keys take 32 MiB. It is the bound the keyed repair
     * transplant's key arena keeps, the worker's other key arena that outlives its
     * operation. Ordinary keys stay far below it at {@link #MAX_RETAINED_KEYS} keys, so
     * their owners still reuse the storage.
     */
    public static final long MAX_RETAINED_KEY_BYTES = LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_KEY_BYTES;
    // The heap table this replaced started at a capacity of 16 keys, doubled it when full,
    // and spread it over ceilPow2(capacity / 0.4) slots, four slots a key of capacity. A
    // table that grows at a quarter of its slots, from 64, keeps that growth schedule.
    private static final double LOAD_FACTOR = 0.25;
    private static final int MIN_SLOT_COUNT = 64;
    private static final int SLOT_BYTES = 16;
    private final LiveViewCheckpointRecordArena keys = LiveViewCheckpointRecordArena.forKeys();
    private final LiveViewCheckpointSlotTable slots = new LiveViewCheckpointSlotTable(SLOT_BYTES, MIN_SLOT_COUNT, LOAD_FACTOR);

    /**
     * Drops the key {@link #beginKey()} started without joining it to the domain.
     */
    public void abortKey() {
        keys.abortRecord();
    }

    /**
     * Joins the {@code keyLength} key bytes at {@code keyAddress} to the domain, copying
     * them. Adding a key the domain already holds leaves it as it is. The bytes must not
     * lie in this domain's own storage.
     */
    public void add(long keyAddress, int keyLength) {
        slots.ensureAllocated();
        final int hash = LiveViewCheckpointKeys.hash(keyAddress, keyLength);
        final int index = findSlot(hash, keyAddress, keyLength);
        if (index < 0) {
            return;
        }
        final long handle = keys.append(keyAddress, keyLength);
        slots.occupy(slots.slotAddress(index), handle, hash);
    }

    /**
     * Starts encoding one key straight into the domain's storage. The caller writes the
     * key's bytes through the returned sink and then calls {@link #commitKey()}. The sink
     * is valid only until then.
     */
    public MemoryA beginKey() {
        slots.ensureAllocated();
        return keys.beginRecord();
    }

    /**
     * Empties the domain, keeping the slot table and key storage it has grown to so a
     * reused domain does not pay for the growth again.
     */
    @Override
    public void clear() {
        keys.clear();
        slots.clear();
    }

    /**
     * Frees every native allocation. The domain is left empty and usable. Idempotent.
     */
    @Override
    public void close() {
        slots.close();
        keys.release();
    }

    /**
     * Ends the key {@link #beginKey()} started and joins it to the domain. A key the
     * domain already holds takes no storage.
     */
    public void commitKey() {
        // beginKey() allocated the table, and nothing in between may clear the domain.
        assert slots.isAllocated() : "live view checkpoint output key committed without its table";
        final long handle = keys.commitRecord();
        final long keyAddress = keys.address(handle);
        final int keyLength = keys.length(handle);
        final int hash = LiveViewCheckpointKeys.hash(keyAddress, keyLength);
        final int index = findSlot(hash, keyAddress, keyLength);
        if (index < 0) {
            keys.discardLastRecord(handle);
            return;
        }
        slots.occupy(slots.slotAddress(index), handle, hash);
    }

    public boolean contains(long keyAddress, int keyLength) {
        return slots.isAllocated() && findSlot(LiveViewCheckpointKeys.hash(keyAddress, keyLength), keyAddress, keyLength) < 0;
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
            slots.copyFrom(other.slots);
            assert size() == keyCount;
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
        return slots.isAllocated() ? slots.slotCount() : 0;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * @return whether an owner that reuses this domain from repair to repair may keep its
     * storage for the next one: at most {@link #MAX_RETAINED_KEYS} keys, and at most
     * {@link #MAX_RETAINED_KEY_BYTES} of key storage, which counts what the storage grew to
     * rather than what the keys use. Such an owner clears a retainable domain and restores
     * the initial capacity of any other.
     */
    public boolean isRetainable() {
        return size() <= MAX_RETAINED_KEYS && keys.capacity() <= MAX_RETAINED_KEY_BYTES;
    }

    public boolean isSlotUsed(int slot) {
        return Unsafe.getLong(slots.slotAddress(slot)) != 0;
    }

    /**
     * Empties the domain and frees its slot table and key storage, back to the initial
     * geometry. For an owner whose domains vary widely in width: {@link #clear()} keeps
     * what one wide domain grew, and every later clear then sweeps all of it however few
     * keys it held.
     */
    public void restoreInitialCapacity() {
        slots.close();
        keys.release();
    }

    public int size() {
        return slots.size();
    }

    /**
     * The slot the key belongs in when the domain does not hold it, or
     * {@code -slot - 1} when it does. The table must be allocated.
     */
    private int findSlot(int hash, long keyAddress, int keyLength) {
        final int mask = slots.mask();
        int index = hash & mask;
        while (true) {
            final long slot = slots.slotAddress(index);
            final long handlePlusOne = Unsafe.getLong(slot);
            if (handlePlusOne == 0) {
                return index;
            }
            if (Unsafe.getInt(slot + LiveViewCheckpointSlotTable.HASH_OFFSET) == hash) {
                final long handle = handlePlusOne - 1;
                if (LiveViewCheckpointKeys.equals(keys.address(handle), keys.length(handle), keyAddress, keyLength)) {
                    return -index - 1;
                }
            }
            index = (index + 1) & mask;
        }
    }

    private long handleAt(int slot) {
        final long handlePlusOne = Unsafe.getLong(slots.slotAddress(slot));
        assert handlePlusOne != 0 : "live view checkpoint output key slot is empty";
        return handlePlusOne - 1;
    }
}
