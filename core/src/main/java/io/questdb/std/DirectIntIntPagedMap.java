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

package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.Reopenable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Off-heap int-to-int map for keys that form dense blocks, such as the symbol keys that a join
 * reads. A non-negative key selects a page of 256 int slots through a page table, and the slot
 * within the page, so a lookup costs two dependent loads and no hashing. Keys without a page live
 * in a {@link DirectIntIntHashMap}.
 * <p>
 * Each page covers its own range of keys, so dense blocks far apart, such as long-lived symbols
 * with small keys and recently created symbols with large keys, all get pages. A page gets
 * allocated only when the pages hold keys in at least a quarter of their slots on average, or
 * when a quarter of the slots of the new page have keys waiting in the hash map. So, the pages
 * take about 16 bytes or less per key that they cover, as a hash map at load factor 0.5 does when
 * it is at its fullest, and sparse keys stay in the hash map instead of allocating mostly empty
 * pages. The first page is exempt. The page table takes at most 16 bytes per map entry, apart from
 * its initial capacity.
 * <p>
 * A key can reach the hash map before its page exists. The first lookup that finds such a key in
 * the hash map once its page exists copies it into the page, so later lookups skip the hash map.
 * <p>
 * The map is created closed and takes no memory until {@link #reopen()} or
 * {@link #restoreInitialCapacity()} opens it with an empty page table. Pages get allocated on
 * demand, and the hash map opens on the first key that gets no page. {@link #close()} frees all
 * of it, and the map stays reusable. Every allocation charges the {@link MemoryTracker} bound by
 * {@link #setMemoryTracker(MemoryTracker)}.
 * <p>
 * The map cannot store {@code noEntryKey}. {@link #get(int)} returns {@code noEntryValue} for a
 * missing key, so the map cannot store {@code noEntryValue} either.
 */
public class DirectIntIntPagedMap implements Mutable, QuietCloseable, Reopenable {
    // A page table slot holds either the address of a page, which is even, or an odd value that
    // counts the hash map keys in the range of the page: (count << 1) | 1. Unsafe.malloc() returns
    // addresses aligned for all value types, so bit 0 of a page address is always 0.
    private static final long EMPTY_SLOT = 1;
    // A hash map entry takes 16 to 32 bytes at load factor 0.5.
    private static final int MAX_PAGED_BYTES_PER_ENTRY = 16;
    // A page slot takes 4 bytes, so a page takes 16 bytes per key when a quarter of its slots hold keys.
    private static final int MIN_PAGE_FILL_DIVISOR = 4;
    private static final int PAGE_BITS = 8;
    // Non-negative keys span this many pages. For a negative key, key >>> PAGE_BITS is at least
    // as large, so a negative key never indexes the page table.
    private static final int MAX_PAGE_TABLE_CAPACITY = 1 << (31 - PAGE_BITS);
    private static final int PAGE_MASK = (1 << PAGE_BITS) - 1;
    private static final long PAGE_SIZE = (long) Integer.BYTES << PAGE_BITS;
    private static final int PAGE_SLOTS = 1 << PAGE_BITS;
    private final int initialPageTableCapacity;
    private final DirectIntIntHashMap map;
    private final int memoryTag;
    private final int noEntryKey;
    private final int noEntryValue;
    @Nullable
    private MemoryTracker memoryTracker;
    private int pageCount;
    // The number of page slots that hold a value: the entries that put() stored in pages, and the
    // copies of hash map entries that get() made.
    private int pageEntryCount;
    private long pageTableAddress;
    // 0 while the map is closed.
    private int pageTableCapacity;
    // The number of entries that put() stored in pages. The copies of hash map entries do not
    // count, since the hash map counts them already.
    private int pagedSize;

    /**
     * @param initialPageTableCapacity the number of pages that the page table covers when the map
     *                                 opens; each takes 8 bytes
     * @param initialMapCapacity       the initial capacity of the hash map, in entries
     * @param loadFactor               the load factor of the hash map
     * @param noEntryKey               the key that the map cannot store
     * @param noEntryValue             the value that {@link #get(int)} returns for a missing key
     * @param memoryTag                the memory tag of every allocation
     */
    public DirectIntIntPagedMap(
            int initialPageTableCapacity,
            int initialMapCapacity,
            double loadFactor,
            int noEntryKey,
            int noEntryValue,
            int memoryTag
    ) {
        this.initialPageTableCapacity = Math.max(1, Math.min(initialPageTableCapacity, MAX_PAGE_TABLE_CAPACITY));
        this.noEntryKey = noEntryKey;
        this.noEntryValue = noEntryValue;
        this.memoryTag = memoryTag;
        this.map = new DirectIntIntHashMap(initialMapCapacity, loadFactor, noEntryKey, noEntryValue, memoryTag, false);
    }

    /**
     * Removes all entries, and keeps the pages and the hash map allocated.
     */
    @Override
    public void clear() {
        for (long p = pageTableAddress, lim = p + 8L * pageTableCapacity; p < lim; p += 8) {
            final long slot = Unsafe.getLong(p);
            if ((slot & 1) == 0) {
                Unsafe.setMemory(slot, PAGE_SIZE, (byte) 0);
            } else {
                Unsafe.putLong(p, EMPTY_SLOT);
            }
        }
        pageEntryCount = 0;
        pagedSize = 0;
        if (map.isOpen()) {
            map.clear();
        }
    }

    @Override
    public void close() {
        freePages();
        pageTableAddress = Unsafe.free(pageTableAddress, 8L * pageTableCapacity, memoryTag, memoryTracker);
        pageTableCapacity = 0;
        pageEntryCount = 0;
        pagedSize = 0;
        map.close();
        // The blocks are gone, so the tracker that charged them carries no debt for this map any more.
        memoryTracker = null;
    }

    /**
     * Returns the value stored for the key, or {@code noEntryValue} when the map holds no such key.
     */
    public int get(int key) {
        final int pageIndex = key >>> PAGE_BITS;
        long slot = EMPTY_SLOT;
        if (pageIndex < pageTableCapacity) {
            slot = Unsafe.getLong(pageTableAddress + ((long) pageIndex << 3));
            if ((slot & 1) == 0) {
                final int stored = Unsafe.getInt(slot + ((long) (key & PAGE_MASK) << 2));
                if (stored != 0) {
                    return stored ^ noEntryValue;
                }
            }
        }
        return getSparse(key, slot);
    }

    @TestOnly
    public int hashMapSize() {
        return map.size();
    }

    public boolean isOpen() {
        return pageTableAddress != 0;
    }

    @TestOnly
    public int pageCount() {
        return pageCount;
    }

    @TestOnly
    public int pageEntryCount() {
        return pageEntryCount;
    }

    /**
     * Stores the value for the key, and replaces the value that the map already holds for it.
     */
    public void put(int key, int value) {
        assert key != noEntryKey : "noEntryKey cannot be stored";
        assert value != noEntryValue : "noEntryValue reads back as a missing key";
        if (pageTableAddress == 0) {
            throw CairoException.critical(0).put("int-int paged map is closed");
        }
        long slotAddress = 0;
        if (key > -1) {
            final int pageIndex = key >>> PAGE_BITS;
            if (pageIndex >= pageTableCapacity) {
                growPageTable(pageIndex);
            }
            if (pageIndex < pageTableCapacity) {
                slotAddress = pageTableAddress + ((long) pageIndex << 3);
                long slot = Unsafe.getLong(slotAddress);
                if ((slot & 1) != 0 && isPageAllowed(slot >>> 1)) {
                    slot = allocatePage(slotAddress);
                }
                if ((slot & 1) == 0) {
                    putPaged(slot + ((long) (key & PAGE_MASK) << 2), key, value);
                    return;
                }
            }
        }
        if (!map.isOpen()) {
            // the hash map opens on the first key that gets no page
            map.setMemoryTracker(memoryTracker);
            map.reopen();
        }
        final int mapSize = map.size();
        map.put(key, value);
        if (slotAddress != 0 && map.size() > mapSize) {
            // one more key waits for the page
            Unsafe.putLong(slotAddress, Unsafe.getLong(slotAddress) + 2);
        }
    }

    @Override
    public void reopen() {
        if (pageTableAddress == 0) {
            restoreInitialCapacity();
        }
    }

    /**
     * Opens a closed map, or empties an open one: frees the pages and the hash map, and shrinks
     * the page table back to its initial capacity.
     */
    public void restoreInitialCapacity() {
        map.close();
        pageEntryCount = 0;
        pagedSize = 0;
        freePages();
        if (pageTableCapacity != initialPageTableCapacity) {
            pageTableAddress = Unsafe.free(pageTableAddress, 8L * pageTableCapacity, memoryTag, memoryTracker);
            pageTableCapacity = 0;
            // A failed malloc leaves the map closed.
            pageTableAddress = Unsafe.malloc(8L * initialPageTableCapacity, memoryTag, memoryTracker);
            pageTableCapacity = initialPageTableCapacity;
        }
        // the hash map is empty, so no key waits for a page
        fillEmptySlots(pageTableAddress, pageTableCapacity);
    }

    /**
     * Binds the per-workload {@link MemoryTracker} that every subsequent allocation charges. A
     * {@code null} tracker degrades the map to global-only accounting. Rebinding frees the map
     * first, since a block has to be freed under the tracker that charged it. Callers bind the
     * tracker at workload start, before {@link #reopen()}.
     */
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        if (tracker != memoryTracker) {
            close();
            memoryTracker = tracker;
        }
    }

    /**
     * Returns the number of keys that the map holds, in the pages and the hash map together.
     */
    public int size() {
        return pagedSize + map.size();
    }

    private static void fillEmptySlots(long address, long slotCount) {
        for (long p = address, lim = address + 8L * slotCount; p < lim; p += 8) {
            Unsafe.putLong(p, EMPTY_SLOT);
        }
    }

    // Allocates a zeroed page for the page table slot and returns its address. A failed malloc
    // leaves the slot as it was.
    private long allocatePage(long slotAddress) {
        final long page = Unsafe.malloc(PAGE_SIZE, memoryTag, memoryTracker);
        Unsafe.setMemory(page, PAGE_SIZE, (byte) 0);
        Unsafe.putLong(slotAddress, page);
        pageCount++;
        return page;
    }

    private void freePages() {
        for (long p = pageTableAddress, lim = p + 8L * pageTableCapacity; pageCount > 0 && p < lim; p += 8) {
            final long slot = Unsafe.getLong(p);
            if ((slot & 1) == 0) {
                Unsafe.free(slot, PAGE_SIZE, memoryTag, memoryTracker);
                Unsafe.putLong(p, EMPTY_SLOT);
                pageCount--;
            }
        }
    }

    // Looks the key up in the hash map. The slot is the page table slot of the key, or
    // EMPTY_SLOT when the page table does not cover the key; get() found no value in its page.
    private int getSparse(int key, long slot) {
        if (map.size() == 0) {
            return noEntryValue;
        }
        final int value = map.get(key);
        if (value != noEntryValue && (slot & 1) == 0) {
            // The key reached the hash map before its page existed. The copy lets later lookups
            // skip the hash map.
            Unsafe.putInt(slot + ((long) (key & PAGE_MASK) << 2), value ^ noEntryValue);
            pageEntryCount++;
        }
        return value;
    }

    // Grows the page table to cover the page, unless the page table would take more memory than
    // the bound allows. The page table at least doubles, unless the bound stops it short of that,
    // so a key far past the page table does not wait for twice the entries that it needs. A failed
    // realloc leaves the old page table in place.
    private void growPageTable(int pageIndex) {
        final long maxCapacity = Math.min(
                MAX_PAGE_TABLE_CAPACITY,
                Math.max(initialPageTableCapacity, MAX_PAGED_BYTES_PER_ENTRY * (size() + 1L) / 8)
        );
        if (pageIndex >= maxCapacity) {
            return;
        }
        final long newCapacity = Math.min(maxCapacity, Math.max(2L * pageTableCapacity, pageIndex + 1L));
        final long oldSize = 8L * pageTableCapacity;
        pageTableAddress = Unsafe.realloc(pageTableAddress, oldSize, 8L * newCapacity, memoryTag, memoryTracker);
        fillEmptySlots(pageTableAddress + oldSize, newCapacity - pageTableCapacity);
        pageTableCapacity = (int) newCapacity;
    }

    // A page gets allocated when the pages hold at least a quarter of their slots on average, or
    // when at least a quarter of the slots of the new page have keys: the keys that wait in the
    // hash map for it, and the key being put. Either way, the pages take about
    // MAX_PAGED_BYTES_PER_ENTRY bytes or less per key that they cover, apart from the first page.
    private boolean isPageAllowed(long waitingKeyCount) {
        return pageCount == 0
                || MIN_PAGE_FILL_DIVISOR * (waitingKeyCount + 1) >= PAGE_SLOTS
                || MIN_PAGE_FILL_DIVISOR * (long) pageEntryCount >= (long) PAGE_SLOTS * pageCount;
    }

    // Stores the entry at its address within a page.
    private void putPaged(long address, int key, int value) {
        final boolean isEmptySlot = Unsafe.getInt(address) == 0;
        // a non-negative index means that the hash map does not hold the key
        final long index = map.size() > 0 ? map.keyIndex(key) : 0;
        if (index < 0) {
            // A put before the page existed stored the key in the hash map, which counts it
            // already. Both copies must hold the same value.
            map.putAt(index, key, value);
        } else if (isEmptySlot) {
            pagedSize++;
        }
        if (isEmptySlot) {
            pageEntryCount++;
        }
        // Each slot holds value ^ noEntryValue, so that a zeroed slot decodes to noEntryValue and
        // reads as empty. XOR is a bijection, so every other value round-trips.
        Unsafe.putInt(address, value ^ noEntryValue);
    }
}
