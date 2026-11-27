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

package io.questdb.griffin.engine.table;

import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * One entry of a parallel hash join's tuple ring, see {@link HashJoinTupleRing}: the tuples that the
 * task of a probe frame, or a continuation chunk of the frame, wrote, back to back in native memory.
 * A tuple is the probe row's index in its frame followed by one entry per stage, see
 * {@link HashJoinStageStepper}, each in a slot of four bytes in a narrow execution and of eight
 * otherwise; the ring decides once per execution, see
 * {@link HashJoinTupleRing#of(io.questdb.cairo.sql.async.PageFrameSequence, boolean, boolean, long, long)}.
 * A narrow slot holds the value's low 32 bits, which the reads sign-extend, so the entries keep their
 * signs: a locator stays at least 0, and {@link HashJoinStageStepper#NO_MATCH} and a tagged chain head
 * stay below it. The ring picks the narrow layout only for values that fit, and the writer checks
 * each value it narrows all the same, so that a value that does not fails the query rather than
 * joins the wrong row; see {@link #putTuple(long, boolean, int, long, long[])}.
 * <p>
 * The ring's entries sit next to each other on the heap, while different threads write and read
 * them, so the hot loops keep their positions to themselves: the slot that writes an entry keeps its
 * own append address, and hands it back once its chunk is done, see
 * {@link #setAppendAddress(long)}, and the consumer reads the tuples by address, through the static
 * accessors. Neither writes the entry's fields per tuple, which would bounce the cache lines that the
 * neighboring entries share with it between their threads.
 * <p>
 * The list grows on the thread that writes it, charged to the execution's memory tracker, and keeps
 * no memory while closed.
 */
public final class HashJoinTupleList implements Mutable, QuietCloseable {
    private long address;
    private long appendAddress;
    private boolean isNarrow;
    private long limit;
    @Nullable
    private MemoryTracker memoryTracker;
    // 64 less the bits of a narrow slot, 32 but for tests; see putTuple().
    private int narrowShift;
    private int tupleBytes;

    /**
     * A stage's entry in the tuple at the given address: the locator, {@link HashJoinStageStepper#NO_MATCH}
     * or a tagged chain head.
     */
    public static long getEntry(long tupleAddress, int stage, boolean isNarrow) {
        return isNarrow
                ? Unsafe.getInt(tupleAddress + ((long) (stage + 1) << 2))
                : Unsafe.getLong(tupleAddress + ((long) (stage + 1) << 3));
    }

    /**
     * The probe row, by its index in the frame, of the tuple at the given address.
     */
    public static long getProbeRow(long tupleAddress, boolean isNarrow) {
        return isNarrow ? Unsafe.getInt(tupleAddress) : Unsafe.getLong(tupleAddress);
    }

    /**
     * Writes a tuple, the probe row and the entries of the stages, at the given address, and returns
     * 0 when every value fits its slot. A narrow slot holds {@code 64 - narrowShift} bits, the sign
     * included, all 32 of its bytes but for tests; a value that does not fit leaves bits set in the
     * result, which the writer ORs over its tuples and checks once it is done with them.
     */
    static long putTuple(long tupleAddress, boolean isNarrow, int narrowShift, long probeRow, long[] entries) {
        long p = tupleAddress;
        if (isNarrow) {
            Unsafe.putInt(p, (int) probeRow);
            // A value fits when sign-extending its low bits gives it back.
            long overflow = probeRow ^ (probeRow << narrowShift >> narrowShift);
            for (long entry : entries) {
                p += Integer.BYTES;
                Unsafe.putInt(p, (int) entry);
                overflow |= entry ^ (entry << narrowShift >> narrowShift);
            }
            return overflow;
        }
        Unsafe.putLong(p, probeRow);
        for (long entry : entries) {
            p += Long.BYTES;
            Unsafe.putLong(p, entry);
        }
        return 0;
    }

    /**
     * Forgets the tuples and keeps the memory.
     */
    @Override
    public void clear() {
        appendAddress = address;
    }

    @Override
    public void close() {
        if (address != 0) {
            Unsafe.free(address, limit - address, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
            address = 0;
            appendAddress = 0;
            limit = 0;
        }
    }

    /**
     * The address of the first tuple.
     */
    public long getAddress() {
        return address;
    }

    /**
     * Where the next tuple goes, as the writer last handed it back.
     */
    public long getAppendAddress() {
        return appendAddress;
    }

    /**
     * The native memory the list holds, in bytes.
     */
    public long getCapacityBytes() {
        return limit - address;
    }

    /**
     * A stage's entry in the tuple at the given address.
     */
    public long getEntry(long tupleAddress, int stage) {
        return getEntry(tupleAddress, stage, isNarrow);
    }

    /**
     * The end of the list's memory.
     */
    public long getLimit() {
        return limit;
    }

    /**
     * 64 less the bits of a narrow slot; see {@link #putTuple(long, boolean, int, long, long[])}.
     */
    public int getNarrowShift() {
        return narrowShift;
    }

    /**
     * The probe row of the tuple at the given address.
     */
    public long getProbeRow(long tupleAddress) {
        return getProbeRow(tupleAddress, isNarrow);
    }

    /**
     * The address of the tuple at the given index.
     */
    public long getTupleAddress(long index) {
        return address + index * tupleBytes;
    }

    public int getTupleBytes() {
        return tupleBytes;
    }

    /**
     * Doubles the memory, keeping the tuples up to the given append address, and returns where the
     * next tuple goes. A breach of the per-query memory limit throws and leaves the list as it was,
     * with the given append address.
     */
    public long grow(long appendAddress) {
        assert appendAddress >= address && appendAddress <= limit;
        this.appendAddress = appendAddress;
        final long oldCapacity = limit - address;
        final long newCapacity = Math.max(oldCapacity << 1, tupleBytes);
        final long size = appendAddress - address;
        address = Unsafe.realloc(address, oldCapacity, newCapacity, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
        limit = address + newCapacity;
        this.appendAddress = address + size;
        return this.appendAddress;
    }

    public boolean isNarrow() {
        return isNarrow;
    }

    /**
     * Appends a tuple, the probe row and the entries of the stages, as a slot of the join appends
     * them, and returns 0 when every value fits its slot; see
     * {@link #putTuple(long, boolean, int, long, long[])}. A slot keeps its own append address
     * instead; see the class documentation.
     */
    @TestOnly
    public long put(long probeRow, long[] entries) {
        long p = appendAddress;
        if (limit - p < tupleBytes) {
            p = grow(p);
        }
        appendAddress = p + tupleBytes;
        return putTuple(p, isNarrow, narrowShift, probeRow, entries);
    }

    /**
     * Hands back the append address of a writer that kept its own.
     */
    public void setAppendAddress(long appendAddress) {
        assert appendAddress >= address && appendAddress <= limit && (appendAddress - address) % tupleBytes == 0;
        this.appendAddress = appendAddress;
    }

    /**
     * The tuples written so far.
     */
    public long size() {
        return (appendAddress - address) / tupleBytes;
    }

    /**
     * Binds the closed list to an execution's tuple layout and memory tracker.
     *
     * @param narrowShift 64 less the bits of a narrow slot, see
     *                    {@link #putTuple(long, boolean, int, long, long[])}
     */
    public void of(boolean isNarrow, int tupleBytes, int narrowShift, @Nullable MemoryTracker memoryTracker) {
        assert address == 0 && tupleBytes > 0 && (!isNarrow || (narrowShift >= Integer.SIZE && narrowShift < Long.SIZE - 1));
        this.isNarrow = isNarrow;
        this.tupleBytes = tupleBytes;
        this.narrowShift = narrowShift;
        this.memoryTracker = memoryTracker;
    }

    /**
     * Empties the list and gives it room for at least {@code tupleCapacity} tuples, at least one. A
     * list with less room gives its memory back and takes exactly that room, with nothing to copy.
     */
    public void open(long tupleCapacity) {
        appendAddress = address;
        final long capacityBytes = Math.max(1, tupleCapacity) * tupleBytes;
        if (limit - address < capacityBytes) {
            close();
            address = Unsafe.malloc(capacityBytes, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
            appendAddress = address;
            limit = address + capacityBytes;
        }
    }
}
