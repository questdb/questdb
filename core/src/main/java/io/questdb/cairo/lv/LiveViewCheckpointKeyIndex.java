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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Native open-addressed index from {@code (namespace, version, partition key)} to a
 * non-negative int, over the keys of one {@link LiveViewCheckpointRecordArena}. The two
 * primitive qualifiers let one flat table serve several function ordinals and state
 * versions without joining them to the key.
 * <p>
 * The index stores each key as its arena handle, never its bytes, so it is only as valid
 * as the arena it was built over: a handle goes stale when that arena is cleared,
 * released or closed. A probe passes its key as an {@code (address, length)} pair valid
 * for the call; a put passes the handle of a key already in the arena.
 * <p>
 * One slot is {@code [long handle + 1][int hash][int namespace][int version][int value]},
 * 24 bytes, zero marking an empty slot. The table is a power of two of slots, probed
 * linearly, and holds entries in up to {@link #LOAD_FACTOR} of them, the load factor
 * QuestDB's native SQL hash maps default to. It doubles the moment its entries reach that
 * share, so it stays between about a third and seven tenths full: 34 to 69 bytes an entry.
 * A probe rejects a slot on its stored hash and qualifiers before it reads the slot's key,
 * so a longer run of occupied slots costs slot reads rather than key reads. A rehash
 * re-places slots by their stored hash and never re-reads key bytes, and
 * {@link #reserve(long)} sizes the table for a known key count in one allocation.
 * <p>
 * Allocation is lazy: the constructor reserves nothing, so a field initializer never
 * strands native memory when its owner's constructor throws. The memory is tagged
 * {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}, which the process totals count, and no
 * view's refresh tracker counts it: the index replaced a heap table that
 * {@code cairo.live.view.refresh.memory.limit.bytes} never covered, so every allocation
 * and free here is untracked.
 */
final class LiveViewCheckpointKeyIndex implements Mutable, QuietCloseable {
    private static final double LOAD_FACTOR = 0.7;
    // reserve() stops presizing here, 2^25 slots in a 768 MiB table for about 23 million
    // entries; a put past it grows the table as it fills.
    private static final int MAX_RESERVED_SLOT_COUNT = 1 << 25;
    // Holds 21 entries without growing: the 22nd, capacityOf(32), doubles it.
    private static final int MIN_SLOT_COUNT = 32;
    private static final int SLOT_BYTES = 24;
    private static final int SLOT_NAMESPACE_OFFSET = LiveViewCheckpointSlotTable.HEADER_BYTES;
    private static final int SLOT_VERSION_OFFSET = SLOT_NAMESPACE_OFFSET + Integer.BYTES;
    private static final int SLOT_VALUE_OFFSET = SLOT_VERSION_OFFSET + Integer.BYTES;
    private final LiveViewCheckpointRecordArena keys;
    private final LiveViewCheckpointSlotTable slots = new LiveViewCheckpointSlotTable(SLOT_BYTES, MIN_SLOT_COUNT, LOAD_FACTOR);

    LiveViewCheckpointKeyIndex(@NotNull LiveViewCheckpointRecordArena keys) {
        this.keys = keys;
    }

    /**
     * Forgets every entry while keeping the table the index has grown to.
     */
    @Override
    public void clear() {
        slots.clear();
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
        if (!slots.isAllocated()) {
            return -1;
        }
        final int hash = hash(namespace, version, keyAddress, keyLength);
        final int mask = slots.mask();
        int index = hash & mask;
        while (true) {
            final long slot = slots.slotAddress(index);
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
     * @return the slots {@link #get} visits on average to find an entry the index holds:
     * the entry's distance from its home slot, plus one; 0 when it holds none
     */
    @TestOnly
    double getHitProbeAverageForTest() {
        long probes = 0;
        if (slots.isAllocated()) {
            final int mask = slots.mask();
            for (int i = 0, n = slots.slotCount(); i < n; i++) {
                final long slot = slots.slotAddress(i);
                if (Unsafe.getLong(slot) != 0) {
                    probes += ((i - Unsafe.getInt(slot + LiveViewCheckpointSlotTable.HASH_OFFSET)) & mask) + 1;
                }
            }
        }
        return size() == 0 ? 0 : (double) probes / size();
    }

    /**
     * @return the slots {@link #get} visits on average to miss, over every home slot a
     * missing key may hash to: the run of entries from that slot on, plus the empty slot
     * that ends it
     */
    @TestOnly
    double getMissProbeAverageForTest() {
        long probes = 0;
        final int slotCount = slots.slotCount();
        final int mask = slots.mask();
        for (int i = 0; i < slotCount; i++) {
            int run = 0;
            while (slots.isAllocated() && run < slotCount && Unsafe.getLong(slots.slotAddress((i + run) & mask)) != 0) {
                run++;
            }
            probes += run + 1;
        }
        return (double) probes / slotCount;
    }

    /**
     * Maps the key {@code keyHandle} names in this index's arena, under these qualifiers,
     * to {@code value}, replacing the value an equal key already maps to.
     */
    void put(int namespace, int version, long keyHandle, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("negative live view checkpoint key index value");
        }
        slots.ensureAllocated();
        // put() appends nothing to the arena, so the address holds for the whole probe.
        final long keyAddress = keys.address(keyHandle);
        final int keyLength = keys.length(keyHandle);
        final int hash = hash(namespace, version, keyAddress, keyLength);
        final int mask = slots.mask();
        int index = hash & mask;
        while (true) {
            final long slot = slots.slotAddress(index);
            final long handlePlusOne = Unsafe.getLong(slot);
            if (handlePlusOne == 0) {
                Unsafe.putInt(slot + SLOT_NAMESPACE_OFFSET, namespace);
                Unsafe.putInt(slot + SLOT_VERSION_OFFSET, version);
                Unsafe.putInt(slot + SLOT_VALUE_OFFSET, value);
                slots.occupy(slot, keyHandle, hash);
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
        slots.close();
    }

    /**
     * Sizes the table so it holds {@code keyCount} entries in all without growing, in one
     * allocation. A table already that large is left as it is.
     */
    void reserve(long keyCount) {
        slots.reserve(keyCount, MAX_RESERVED_SLOT_COUNT);
    }

    int size() {
        return slots.size();
    }

    /**
     * Avalanches the key's bytes, its length and both qualifiers, so every bit of the slot
     * hash depends on all of them. A table seven tenths full needs that. Over the byte
     * polynomial of {@link LiveViewCheckpointKeys#hashCode}, sequential ids and one key's
     * qualifiers land in long runs of occupied slots, and at this load a hit took up to 29
     * probes where an even spread takes about 2. {@link Hash#hashMem64} leaves the length
     * out, and leading zero bytes add nothing to its polynomial, so without the length the
     * keys that differ only in those bytes, such as the all-zero keys of every length,
     * would share one slot hash. The index has no iteration, so no slot order leaks out of
     * it and its hash is free to differ from the heap tables'.
     */
    private static int hash(int namespace, int version, long keyAddress, int keyLength) {
        return Hash.hashLong128_32(
                Hash.hashMem64(keyAddress, keyLength) ^ keyLength,
                Numbers.encodeLowHighInts(version, namespace)
        );
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
        if (Unsafe.getInt(slot + LiveViewCheckpointSlotTable.HASH_OFFSET) != hash
                || Unsafe.getInt(slot + SLOT_NAMESPACE_OFFSET) != namespace
                || Unsafe.getInt(slot + SLOT_VERSION_OFFSET) != version) {
            return false;
        }
        final long handle = handlePlusOne - 1;
        return LiveViewCheckpointKeys.equals(keys.address(handle), keys.length(handle), keyAddress, keyLength);
    }
}
