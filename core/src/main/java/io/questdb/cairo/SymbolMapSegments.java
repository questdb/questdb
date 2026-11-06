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


package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.MemoryTag;
import io.questdb.std.NativeMemoryBudget;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Native copy of a symbol dictionary, loaded {@link #SEGMENT_KEYS} keys at a time. A segment keeps the values
 * in the order and format of the symbol char file, so a scan in key order stays sequential whatever order
 * filled the cache. Reads are lock-free; a load takes a try-lock that never waits. Published values never move
 * and replaced blocks stay allocated until close, so views survive later loads. All memory counts toward the
 * budget. Admission stops on the first allocation that fails. After a budget refusal, a later load admits again
 * once the budget has room for the refused allocation; after a failed malloc, or for a segment too large to
 * cache, admission stays stopped.
 */
public class SymbolMapSegments implements QuietCloseable {
    public static final int LOADED = 0;
    public static final int NOT_LOADED = 1;
    public static final int SEGMENT_BITS = 8;
    public static final int SEGMENT_KEYS = 1 << SEGMENT_BITS;
    public static final int STOPPED = 2;
    public static final int STOP_BUDGET = 1;
    public static final int STOP_OOM = 2;
    private static final long ADDRESS_MASK = (1L << 48) - 1;
    private static final int CAPACITY_OFFSET = 0;
    private static final int COUNT_SHIFT = 48;
    private static final int DIRECTORY_HEADER = 16;
    private static final int DIRECTORY_RETIRED_OFFSET = 8;
    private static final long MAX_CHAR_BYTES = 1L << 30;
    private static final int MIN_PARTIAL_CHAR_BYTES = 1024;
    private static final int RETIRED_OFFSET = 8;
    private static final int SEGMENT_HEADER = 32;
    private static final long SEGMENT_CHARS = SEGMENT_HEADER + (long) SEGMENT_KEYS * Integer.BYTES;
    private static final int SEGMENT_MASK = SEGMENT_KEYS - 1;
    private static final int SIZE_OFFSET = 16;
    private static final int USED_OFFSET = 4;
    private final NativeMemoryBudget budget;
    private final AtomicBoolean lock = new AtomicBoolean();
    private final long maxCharBytes;
    private long allocatedBytes;
    private volatile long directory;
    private volatile boolean isFilling = true;
    private long loadedCharBytes;
    // Bytes the budget refused; zero when admission stays stopped.
    private volatile long resumeBytes;
    private int size;
    private int stopReason;

    public SymbolMapSegments(NativeMemoryBudget budget) {
        this(budget, MAX_CHAR_BYTES);
    }

    @TestOnly
    public SymbolMapSegments(NativeMemoryBudget budget, long maxCharBytes) {
        this.budget = budget;
        this.maxCharBytes = maxCharBytes;
    }

    public long allocatedBytes() {
        return allocatedBytes;
    }

    @Override
    public void close() {
        resumeBytes = 0;
        isFilling = false;
        long d = directory;
        directory = 0;
        if (d != 0) {
            for (int i = 0, n = Unsafe.getInt(d); i < n; i++) {
                long segment = Unsafe.getLong(d + DIRECTORY_HEADER + (long) i * Long.BYTES) & ADDRESS_MASK;
                while (segment != 0) {
                    final long retired = Unsafe.getLong(segment + RETIRED_OFFSET);
                    free(segment, Unsafe.getLong(segment + SIZE_OFFSET));
                    segment = retired;
                }
            }
            while (d != 0) {
                final long retired = Unsafe.getLong(d + DIRECTORY_RETIRED_OFFSET);
                free(d, directorySize(Unsafe.getInt(d)));
                d = retired;
            }
        }
        loadedCharBytes = 0;
        size = 0;
    }

    public @Nullable DirectString get(int key, @NotNull DirectString view) {
        final long slot = slot(key);
        final int offset = key & SEGMENT_MASK;
        if (offset >= (int) (slot >>> COUNT_SHIFT)) {
            return null;
        }
        return record(slot & ADDRESS_MASK, offset, view);
    }

    public int getStopReason() {
        return stopReason;
    }

    /**
     * Copies the keys of the segment holding {@code key} that are below {@code symbolCount}. Returns LOADED when
     * {@code key} is cached, NOT_LOADED when the cache is busy or stopped, and STOPPED to the one caller whose
     * load first stops admission.
     */
    public int load(int key, int symbolCount, MemoryR offsets, MemoryR chars) {
        if ((!isFilling && !canResume()) || lock.get() || !lock.compareAndSet(false, true)) {
            return NOT_LOADED;
        }
        try {
            if (!isFilling) {
                if (!canResume()) {
                    return NOT_LOADED;
                }
                isFilling = true;
            }
            // Only the first stop reports STOPPED, so a cache that resumes and stops again logs once.
            final int stopResult = stopReason == 0 ? STOPPED : NOT_LOADED;
            final int index = key >>> SEGMENT_BITS;
            final long d = directoryFor(index, symbolCount);
            if (d == 0) {
                return stopResult;
            }
            final long slotAddress = d + DIRECTORY_HEADER + (long) index * Long.BYTES;
            final long slot = Unsafe.getLong(slotAddress);
            final long current = slot & ADDRESS_MASK;
            final int count = (int) (slot >>> COUNT_SHIFT);
            final int firstKey = index << SEGMENT_BITS;
            if (key - firstKey < count) {
                return LOADED;
            }
            final int loKey = firstKey + count;
            final int hiKey = (int) Math.min((long) firstKey + SEGMENT_KEYS, symbolCount);
            final long offsetsAddress = offsets.addressOf(SymbolMapWriter.keyToOffset(loKey));
            final long charLo = Unsafe.getLong(offsetsAddress);
            final long charBytes = Unsafe.getLong(offsetsAddress + (long) (hiKey - loKey) * Long.BYTES) - charLo;
            final int used = current == 0 ? 0 : Unsafe.getInt(current + USED_OFFSET);
            long segment = current;
            if (current == 0 || used + charBytes > Unsafe.getInt(current + CAPACITY_OFFSET)) {
                final long neededCharBytes = used + charBytes;
                final long capacity = hiKey - firstKey == SEGMENT_KEYS
                        ? neededCharBytes
                        : partialCapacity(neededCharBytes, charBytes, hiKey - loKey, current == 0 && index == 0);
                segment = newSegment(current, count, used, neededCharBytes, capacity);
                if (segment == 0) {
                    return stopResult;
                }
            }
            final long relativeBase = used - charLo;
            final long segmentOffsets = segment + SEGMENT_HEADER + (long) count * Integer.BYTES;
            for (int k = 0, n = hiKey - loKey; k < n; k++) {
                final long relative = relativeBase + Unsafe.getLong(offsetsAddress + (long) k * Long.BYTES);
                Unsafe.putInt(segmentOffsets + (long) k * Integer.BYTES, (int) relative);
            }
            Vect.memcpy(segment + SEGMENT_CHARS + used, chars.addressOf(charLo), charBytes);
            Unsafe.putInt(segment + USED_OFFSET, (int) (used + charBytes));
            publish(slotAddress, ((long) (hiKey - firstKey) << COUNT_SHIFT) | segment);
            loadedCharBytes += charBytes;
            size += hiKey - loKey;
            return LOADED;
        } finally {
            lock.set(false);
        }
    }

    public int size() {
        return size;
    }

    // Publishes every offset and char of the segment, including those newSegment() copied. Tests
    // override it to read the segment on both sides of the store.
    protected void publish(long slotAddress, long slot) {
        Unsafe.putLongVolatile(slotAddress, slot);
    }

    private static DirectString record(long segment, int offset, DirectString view) {
        final long record = segment + SEGMENT_CHARS + Unsafe.getInt(segment + SEGMENT_HEADER + (long) offset * Integer.BYTES);
        return view.of(record + Integer.BYTES, Unsafe.getInt(record));
    }

    private static long directorySize(int capacity) {
        return DIRECTORY_HEADER + (long) capacity * Long.BYTES;
    }

    private long allocate(long bytes) {
        if (!budget.tryReserve(bytes)) {
            stop(STOP_BUDGET, bytes);
            return 0;
        }
        try {
            final long address = Unsafe.malloc(bytes, MemoryTag.NATIVE_SYMBOL_CACHE);
            allocatedBytes += bytes;
            return address;
        } catch (CairoException e) {
            budget.release(bytes);
            stop(STOP_OOM, 0);
            return 0;
        }
    }

    private boolean canResume() {
        final long bytes = resumeBytes;
        return bytes > 0 && budget.hasRoomFor(bytes);
    }

    private long directoryFor(int index, int symbolCount) {
        final long d = directory;
        final int capacity = d == 0 ? 0 : Unsafe.getInt(d);
        if (index < capacity) {
            return d;
        }
        final int newCapacity = Numbers.ceilPow2(((symbolCount - 1) >>> SEGMENT_BITS) + 1);
        final long bytes = directorySize(newCapacity);
        final long grown = allocate(bytes);
        if (grown != 0) {
            Vect.memset(grown, bytes, 0);
            Unsafe.putInt(grown, newCapacity);
            Unsafe.putLong(grown + DIRECTORY_RETIRED_OFFSET, d);
            if (d != 0) {
                Vect.memcpy(grown + DIRECTORY_HEADER, d + DIRECTORY_HEADER, (long) capacity * Long.BYTES);
            }
            // Publishes the header and the copied slots.
            directory = grown;
        }
        return grown;
    }

    private void free(long address, long bytes) {
        Unsafe.free(address, bytes, MemoryTag.NATIVE_SYMBOL_CACHE);
        budget.release(bytes);
        allocatedBytes -= bytes;
    }

    private long newSegment(long current, int count, int used, long neededCharBytes, long capacity) {
        if (neededCharBytes > maxCharBytes) {
            stop(STOP_BUDGET, 0);
            return 0;
        }
        final long bytes = SEGMENT_CHARS + capacity;
        final long segment = allocate(bytes);
        if (segment != 0) {
            assert (segment & ~ADDRESS_MASK) == 0;
            Unsafe.putInt(segment + CAPACITY_OFFSET, (int) capacity);
            Unsafe.putLong(segment + RETIRED_OFFSET, current);
            Unsafe.putLong(segment + SIZE_OFFSET, bytes);
            if (current != 0) {
                Vect.memcpy(segment + SEGMENT_HEADER, current + SEGMENT_HEADER, (long) count * Integer.BYTES);
                Vect.memcpy(segment + SEGMENT_CHARS, current + SEGMENT_CHARS, used);
            }
        }
        return segment;
    }

    // A replaced block stays allocated, so a dictionary read as it grows would hold every block it outgrew.
    // Only the first block of the first segment starts small, for dictionaries of a few values; a later
    // partial segment gets room for all its keys at the average value size, plus an eighth for values that
    // grow longer, such as sequential ids gaining a digit.
    private long partialCapacity(long neededCharBytes, long newCharBytes, int newKeys, boolean isFirstBlock) {
        long capacity = Math.max(MIN_PARTIAL_CHAR_BYTES, 2 * neededCharBytes);
        if (!isFirstBlock) {
            final long expected = (loadedCharBytes + newCharBytes) * SEGMENT_KEYS / (size + newKeys);
            capacity = Math.max(capacity, expected + (expected >>> 3));
        }
        return Math.min(maxCharBytes, capacity);
    }

    private long slot(int key) {
        final long d = directory;
        if (d == 0) {
            return 0;
        }
        final int index = key >>> SEGMENT_BITS;
        if (index >= Unsafe.getInt(d)) {
            return 0;
        }
        return Unsafe.getLongVolatile(d + DIRECTORY_HEADER + (long) index * Long.BYTES);
    }

    private void stop(int reason, long resumeBytes) {
        stopReason = reason;
        this.resumeBytes = resumeBytes;
        isFilling = false;
    }
}
