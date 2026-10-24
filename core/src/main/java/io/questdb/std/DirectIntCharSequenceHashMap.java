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
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Append-only int-to-UTF-16 map with lock-free reads and non-blocking inserts.
 * All int keys are supported. Values and retired indexes remain allocated until
 * close, so views survive subsequent inserts. Each reader must supply its own
 * view. Close must not run concurrently with any access, including to those views.
 */
public class DirectIntCharSequenceHashMap extends DirectIntCharSequenceHashMapRhsPadding implements QuietCloseable {
    public static final int BUSY = 3;
    public static final int INSERTED = 0;
    public static final int NOT_INSERTED = 1;
    public static final int STOPPED = 2;
    public static final int STOP_BUDGET = 1;
    public static final int STOP_LIMIT = 2;
    public static final int STOP_OOM = 3;
    private static final int CHUNK_HEADER_SIZE = Long.BYTES;
    private static final int HEADER_SIZE = 16;
    private static final int INITIAL_CHUNK_SIZE = 256;
    private static final int MAX_CHUNKS = (1 << 14) - 1;
    private static final int MAX_RECORD_BYTES = 8 * 1024;
    private static final int MAX_SHARED_CHUNK_SIZE = 64 * 1024;
    private static final int MAX_SLOTS = 1 << 30;
    private static final int MIN_DIRECT_SLOTS = 128;
    private static final int OFFSET_BITS = 18;
    private static final int OFFSET_MASK = (1 << OFFSET_BITS) - 1;
    private static final int RETIRED_TABLE_OFFSET = 8;
    private static final int TABLE_MASK_OFFSET = 4;
    private static final int TABLE_SHIFT_OFFSET = 0;
    // Publishing this bit makes the final plain key bounds visible to readers.
    private static final long TABLE_STOPPED = 1;
    private final @Nullable NativeMemoryBudget budget;
    private final int initialSlots;
    private final double loadFactor;
    private final AtomicBoolean lock = new AtomicBoolean();
    private final int memoryTag;
    private volatile long allocatedBytes;
    private int chunkCount;
    private int chunkTableCapacity;
    private volatile boolean isFilling = true;
    private int maxChunks = MAX_CHUNKS;
    private int maxKey = Integer.MIN_VALUE;
    private int maxSize;
    private int maxSlots = MAX_SLOTS;
    private int minKey = Integer.MAX_VALUE;
    private int nextChunkSize = INITIAL_CHUNK_SIZE;
    private long sharedChunk;
    private int sharedChunkIndex;
    private int sharedChunkLimit;
    private int sharedChunkPosition;
    private volatile int size;
    private int stopReason;

    public DirectIntCharSequenceHashMap(int initialCapacity, double loadFactor, int memoryTag, @Nullable NativeMemoryBudget budget) {
        if (!(loadFactor > 0 && loadFactor < 1)) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        double slots = Math.ceil(Math.max(initialCapacity, 4) / loadFactor);
        if (slots > MAX_SLOTS) {
            throw new IllegalArgumentException("int-char sequence hash table capacity overflow");
        }
        this.initialSlots = Numbers.ceilPow2((int) slots);
        this.loadFactor = loadFactor;
        this.memoryTag = memoryTag;
        this.budget = budget;
    }

    public long allocatedBytes() {
        return allocatedBytes;
    }

    @Override
    public void close() {
        isFilling = false;
        if (chunks != 0) {
            for (int i = 1; i <= chunkCount; i++) {
                long address = Unsafe.getLong(chunks + (long) i * Long.BYTES) - CHUNK_HEADER_SIZE;
                free(address, Unsafe.getLong(address));
            }
            long current = chunks;
            int capacity = chunkTableCapacity;
            chunks = 0;
            // Each directory generation doubles capacity.
            while (current != 0) {
                long next = Unsafe.getLong(current);
                free(current, (long) capacity * Long.BYTES);
                capacity /= 2;
                current = next;
            }
        }
        long current = table & ~TABLE_STOPPED;
        table = 0;
        while (current != 0) {
            long next = Unsafe.getLong(current + RETIRED_TABLE_OFFSET);
            free(current, HEADER_SIZE + ((long) Unsafe.getInt(current + TABLE_MASK_OFFSET) + 1) * Long.BYTES);
            current = next;
        }
        size = 0;
        chunkCount = 0;
        chunkTableCapacity = 0;
        sharedChunk = 0;
    }

    public @Nullable DirectString get(int key, @NotNull DirectString view) {
        long t = table;
        if ((t & TABLE_STOPPED) != 0) {
            if (key < minKey || key > maxKey) {
                return null;
            }
            t &= ~TABLE_STOPPED;
        }
        if (t == 0) {
            return null;
        }
        final int shift = Unsafe.getInt(t + TABLE_SHIFT_OFFSET);
        int h = hash(key, shift);
        for (; ; ) {
            final long slot = Unsafe.getLongVolatile(t + HEADER_SIZE + (long) h * Long.BYTES);
            if (slot == 0) {
                return null;
            }
            if ((int) (slot >>> 32) == key) {
                final int ref = (int) slot;
                // Read chunks after the slot so they include the slot's chunk.
                final long c = chunks;
                final long record = Unsafe.getLong(c + (long) (ref >>> OFFSET_BITS) * Long.BYTES)
                        + ((long) (ref & OFFSET_MASK) << 1);
                return view.of(record + Integer.BYTES, Unsafe.getInt(record));
            }
            if (shift < 0) {
                return null;
            }
            h = (h + 1) & Unsafe.getInt(t + TABLE_MASK_OFFSET);
        }
    }

    public int getMaxKeyIfStopped() {
        return (table & TABLE_STOPPED) != 0 ? maxKey : Integer.MAX_VALUE;
    }

    public int getStopReason() {
        return stopReason;
    }

    public boolean isFilling() {
        return isFilling;
    }

    @TestOnly
    public void setMaxChunks(int maxChunks) {
        if (maxChunks < 1 || maxChunks > MAX_CHUNKS) {
            throw new IllegalArgumentException("invalid chunk limit");
        }
        this.maxChunks = maxChunks;
    }

    @TestOnly
    public void setMaxSlots(int maxSlots) {
        if (maxSlots < 1 || maxSlots > MAX_SLOTS) {
            throw new IllegalArgumentException("invalid slot limit");
        }
        this.maxSlots = maxSlots;
    }

    @TestOnly
    public void setNextChunkSize(int nextChunkSize) {
        assert nextChunkSize >= INITIAL_CHUNK_SIZE && nextChunkSize <= MAX_SHARED_CHUNK_SIZE;
        this.nextChunkSize = nextChunkSize;
    }

    public int size() {
        return size;
    }

    /**
     * Returns BUSY while another inserter holds the lock, and STOPPED only to the inserter that
     * stops admission.
     */
    public int tryPutIfAbsent(int key, long charsAddr, int len) {
        assert len >= 0 && (len == 0 || charsAddr != 0);
        if (Integer.BYTES + 2L * len > MAX_RECORD_BYTES || !isFilling) {
            return NOT_INSERTED;
        }
        if (lock.get() || !lock.compareAndSet(false, true)) {
            return BUSY;
        }
        try {
            if (!isFilling) {
                return NOT_INSERTED;
            }
            long t = table;
            if (t != 0 && Unsafe.getInt(t + TABLE_SHIFT_OFFSET) < 0
                    && Math.max(maxKey, key) - (long) Math.min(minKey, key) > Unsafe.getInt(t + TABLE_MASK_OFFSET)) {
                if (!resize(key)) {
                    return STOPPED;
                }
                t = table;
            }
            long slot = t == 0 ? 0 : findSlot(t, key);
            if (slot == -1) {
                return NOT_INSERTED;
            }
            if (size == maxSize) {
                if (!resize(key)) {
                    return STOPPED;
                }
                slot = findSlot(table, key);
            }
            int ref = append(charsAddr, len);
            if (ref == 0) {
                return STOPPED;
            }
            minKey = Math.min(minKey, key);
            maxKey = Math.max(maxKey, key);
            Unsafe.putLongVolatile(slot, ((long) key << 32) | (ref & 0xFFFFFFFFL));
            size++;
            return INSERTED;
        } finally {
            lock.set(false);
        }
    }

    private static void copyValue(long record, long charsAddr, int len) {
        Unsafe.putInt(record, len);
        if (len > 0) {
            Vect.memcpy(record + Integer.BYTES, charsAddr, (long) len * Character.BYTES);
        }
    }

    private static long findSlot(long t, int key) {
        final int mask = Unsafe.getInt(t + TABLE_MASK_OFFSET);
        int h = hash(key, Unsafe.getInt(t + TABLE_SHIFT_OFFSET));
        for (; ; ) {
            final long address = t + HEADER_SIZE + (long) h * Long.BYTES;
            final long slot = Unsafe.getLong(address);
            if (slot == 0) {
                return address;
            }
            if ((int) (slot >>> 32) == key) {
                return -1;
            }
            h = (h + 1) & mask;
        }
    }

    private static int hash(int key, int shift) {
        if (shift < 0) {
            return key & (-1 >>> (shift & 31));
        }
        return (key * 0x9E3779B9) >>> shift;
    }

    private long allocate(long bytes) {
        if (budget != null && !budget.tryReserve(bytes)) {
            stop(STOP_BUDGET);
            return 0;
        }
        return allocateReserved(bytes);
    }

    private long allocateReserved(long bytes) {
        try {
            long address = Unsafe.malloc(bytes, memoryTag);
            //noinspection NonAtomicOperationOnVolatileField
            allocatedBytes += bytes;
            return address;
        } catch (Throwable e) {
            if (budget != null) {
                budget.release(bytes);
            }
            if (e instanceof CairoException ce && ce.isOutOfMemory()) {
                stop(STOP_OOM);
                return 0;
            }
            throw e;
        }
    }

    private int append(long charsAddr, int len) {
        final long recordBytes = Integer.BYTES + (long) len * Character.BYTES;
        if (recordBytes <= sharedChunkLimit - sharedChunkPosition) {
            copyValue(sharedChunk + sharedChunkPosition, charsAddr, len);
            int ref = (sharedChunkIndex << OFFSET_BITS) | (sharedChunkPosition >>> 1);
            sharedChunkPosition += (int) recordBytes;
            return ref;
        }
        if (chunkCount >= maxChunks) {
            stop(STOP_LIMIT);
            return 0;
        }
        final long chunkBytes = Math.max(nextChunkSize, Numbers.ceilPow2((int) recordBytes + CHUNK_HEADER_SIZE));
        final long capacity = chunkBytes - CHUNK_HEADER_SIZE;
        long allocation = allocate(chunkBytes);
        if (allocation == 0) {
            return 0;
        }
        long newChunks = 0;
        long tableBytes = 0;
        try {
            final int index = chunkCount + 1;
            final long oldChunks = chunks;
            final int oldCapacity = chunkTableCapacity;
            int newCapacity = oldCapacity;
            if (index >= oldCapacity) {
                newCapacity = oldCapacity == 0 ? 4 : oldCapacity * 2;
                tableBytes = (long) newCapacity * Long.BYTES;
                newChunks = allocate(tableBytes);
                if (newChunks == 0) {
                    return 0;
                }
                if (oldChunks != 0) {
                    Vect.memcpy(newChunks + Long.BYTES, oldChunks + Long.BYTES, (long) (oldCapacity - 1) * Long.BYTES);
                }
                Unsafe.putLong(newChunks, oldChunks);
            }
            Unsafe.putLong(allocation, chunkBytes);
            long record = allocation + CHUNK_HEADER_SIZE;
            copyValue(record, charsAddr, len);
            long c = newChunks == 0 ? oldChunks : newChunks;
            Unsafe.putLong(c + (long) index * Long.BYTES, record);
            if (newChunks != 0) {
                chunkTableCapacity = newCapacity;
                chunks = newChunks;
                newChunks = 0;
            }
            chunkCount = index;
            sharedChunk = record;
            sharedChunkIndex = index;
            sharedChunkLimit = (int) capacity;
            sharedChunkPosition = (int) recordBytes;
            nextChunkSize = Math.min((int) chunkBytes * 2, MAX_SHARED_CHUNK_SIZE);
            allocation = 0;
            return index << OFFSET_BITS;
        } finally {
            if (allocation != 0) {
                free(allocation, chunkBytes);
            }
            if (newChunks != 0) {
                free(newChunks, tableBytes);
            }
        }
    }

    private void free(long address, long bytes) {
        Unsafe.free(address, bytes, memoryTag);
        //noinspection NonAtomicOperationOnVolatileField
        allocatedBytes -= bytes;
        if (budget != null) {
            budget.release(bytes);
        }
    }

    private boolean resize(int key) {
        final long oldTable = table;
        int slots = oldTable == 0 ? initialSlots : (Unsafe.getInt(oldTable + TABLE_MASK_OFFSET) + 1) * (size == maxSize ? 2 : 1);
        if (slots > maxSlots || slots <= 0) {
            stop(STOP_LIMIT);
            return false;
        }
        final long span = Math.max(maxKey, key) - (long) Math.min(minKey, key);
        final boolean isDenseGrowth = oldTable != 0 && size < maxSize
                && slots <= maxSlots / 2 && span < 2L * slots && 2L * size >= maxSize
                && (budget == null || budget.tryReserve(HEADER_SIZE + 2L * slots * Long.BYTES));
        if (isDenseGrowth) {
            slots *= 2;
        }
        final long bytes = HEADER_SIZE + (long) slots * Long.BYTES;
        final long newTable = isDenseGrowth ? allocateReserved(bytes) : allocate(bytes);
        if (newTable == 0) {
            return false;
        }
        Vect.memset(newTable, bytes, 0);
        int shift = Integer.numberOfLeadingZeros(slots) + 1;
        if (slots >= MIN_DIRECT_SLOTS && span < slots) {
            shift |= Integer.MIN_VALUE;
        }
        Unsafe.putInt(newTable + TABLE_SHIFT_OFFSET, shift);
        Unsafe.putInt(newTable + TABLE_MASK_OFFSET, slots - 1);
        Unsafe.putLong(newTable + RETIRED_TABLE_OFFSET, oldTable);
        if (oldTable != 0) {
            final long end = oldTable + HEADER_SIZE + ((long) Unsafe.getInt(oldTable + TABLE_MASK_OFFSET) + 1) * Long.BYTES;
            for (long address = oldTable + HEADER_SIZE; address < end; address += Long.BYTES) {
                long slot = Unsafe.getLong(address);
                if (slot != 0) {
                    Unsafe.putLong(findSlot(newTable, (int) (slot >>> 32)), slot);
                }
            }
        }
        maxSize = Math.min(slots - 1, (int) (slots * loadFactor));
        table = newTable;
        return true;
    }

    private void stop(int reason) {
        stopReason = reason;
        isFilling = false;
        table |= TABLE_STOPPED;
    }
}

class DirectIntCharSequenceHashMapHotFields extends DirectIntCharSequenceHashMapLhsPadding {
    protected volatile long chunks;
    protected volatile long table;
}

@SuppressWarnings("unused")
class DirectIntCharSequenceHashMapLhsPadding {
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16;
}

@SuppressWarnings("unused")
class DirectIntCharSequenceHashMapRhsPadding extends DirectIntCharSequenceHashMapHotFields {
    protected long p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32;
}
