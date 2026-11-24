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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * Append-only native storage for encoded checkpoint records: partition keys in an arena
 * {@link #forKeys()} makes, and state payloads - an inline scalar image, a fused
 * window-state payload or a member state image - in one {@link #forPayloads()} makes.
 * Each record is a header of one alignment unit, its int length followed by zeroes, then
 * the record bytes, zero-padded to the alignment: {@code [int length][key bytes]} on a
 * four-byte boundary for a key, {@code [int length][int 0][payload bytes]} on an
 * eight-byte boundary for a payload. The record's offset is its handle. A holder keeps
 * the handle, never an address: the arena is one contiguous region that moves when it
 * grows, so an address is valid only until the next call that appends to, grows, clears
 * or frees the arena, while a handle stays valid until the next {@link #clear()},
 * {@link #release()} or {@link #close()}.
 * <p>
 * A key may be empty; a payload never is. A holder that has no payload to name - a
 * page-backed partition, or a key outside the key domain a seal images - stores
 * {@link #NO_RECORD}, which no record's handle can equal.
 * <p>
 * A record is copied in whole with {@link #append(long, int)}, reserved zero-filled with
 * {@link #reserve(int)} and encoded in place at its address, or encoded in place between
 * {@link #beginRecord()} and {@link #commitRecord()}, which is what lets a producer write
 * a key straight into the arena instead of into a scratch buffer it then copies.
 * <p>
 * The padding is part of the byte format, not slack: the heap encoders this arena
 * replaces wrote into zero-filled arrays, and an encoder that leaves a field unwritten
 * (a guarded count on a NULL partition key, say) relies on the zeroes. {@link #reserve}
 * therefore zero-fills the whole record, including over bytes an earlier record left
 * behind after {@link #clear()}.
 * <p>
 * A key and its payload live in two arenas on purpose, so an append of one kind never
 * moves the other while a caller holds its address.
 * <p>
 * Allocation is lazy: the factories reserve nothing, so a field initializer never
 * strands native memory when its owner's constructor throws. The memory is tagged
 * {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}, which the process totals count, and no
 * view's refresh tracker counts it: the arena holds keys and payloads that were heap
 * arrays {@code cairo.live.view.refresh.memory.limit.bytes} never covered, so every
 * allocation and free here is untracked.
 */
final class LiveViewCheckpointRecordArena implements Mutable, QuietCloseable {
    /**
     * The handle of no record. Records start at non-negative offsets, so it never
     * names one.
     */
    static final long NO_RECORD = -1;
    private static final long NO_PENDING_RECORD = -1;
    private static final long PAGE_SIZE = 4096;
    // The record alignment, which is also the header width.
    private final int alignment;
    private final boolean isEmptyRecordAllowed;
    // Names the record kind in assertion and error messages.
    private final String kind;
    private final MemoryCARWImpl memory = new MemoryCARWImpl(PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    private long pendingRecordOffset = NO_PENDING_RECORD;
    private int recordCount;

    private LiveViewCheckpointRecordArena(int alignment, boolean isEmptyRecordAllowed, String kind) {
        assert alignment >= Integer.BYTES && (alignment & (alignment - 1)) == 0;
        this.alignment = alignment;
        this.isEmptyRecordAllowed = isEmptyRecordAllowed;
        this.kind = kind;
    }

    /**
     * @return an arena for partition keys: four-byte aligned records, empty keys allowed
     */
    static LiveViewCheckpointRecordArena forKeys() {
        return new LiveViewCheckpointRecordArena(Integer.BYTES, true, "partition key");
    }

    /**
     * @return an arena for state payloads: eight-byte aligned records, never empty
     */
    static LiveViewCheckpointRecordArena forPayloads() {
        return new LiveViewCheckpointRecordArena(Long.BYTES, false, "payload");
    }

    /**
     * @return whether the {@code length} bytes at {@code address} overlap the region
     * {@code memory} has allocated, which a copy into {@code memory} may move
     */
    static boolean isArenaRange(MemoryCARWImpl memory, long address, int length) {
        final long lo = memory.getPageAddress(0);
        return length > 0 && lo != 0 && address < memory.addressHi() && address + length > lo;
    }

    /**
     * Forgets every record while keeping the capacity the arena has grown to.
     */
    @Override
    public void clear() {
        if (memory.getAppendOffset() > 0) {
            memory.jumpTo(0);
        }
        recordCount = 0;
        pendingRecordOffset = NO_PENDING_RECORD;
    }

    /**
     * Frees the arena's memory, exactly as {@link #release()} does. Idempotent.
     */
    @Override
    public void close() {
        release();
    }

    /**
     * Drops the record {@link #beginRecord()} started, leaving the arena as it was before.
     * A no-op when no record is pending.
     */
    void abortRecord() {
        if (pendingRecordOffset != NO_PENDING_RECORD) {
            memory.jumpTo(pendingRecordOffset);
            pendingRecordOffset = NO_PENDING_RECORD;
        }
    }

    /**
     * @return the address of the record bytes {@code handle} names, valid until the next
     * call that appends to, grows, clears or frees this arena
     */
    long address(long handle) {
        assert isHandleValid(handle) : "live view checkpoint " + kind + " handle outside its arena";
        return memory.addressOf(handle + alignment);
    }

    /**
     * Copies {@code length} record bytes at {@code address} into the arena. The source
     * must not lie inside this arena, which may move while it copies.
     *
     * @return the new record's handle
     */
    long append(long address, int length) {
        assert pendingRecordOffset == NO_PENDING_RECORD
                : "live view checkpoint " + kind + " appended while another is being encoded";
        assert length >= 0;
        assert length > 0 || isEmptyRecordAllowed : "live view checkpoint " + kind + " must not be empty";
        assert !isArenaRange(memory, address, length) : "live view checkpoint " + kind + " aliases its own arena";
        final long recordBytes = recordBytes(length);
        final long handle = memory.getAppendOffset();
        // Throws before it moves the append offset, so a failed append leaves the arena
        // exactly as it was.
        final long record = memory.appendAddressFor(recordBytes);
        putHeader(record, length);
        if (length > 0) {
            Vect.memcpy(record + alignment, address, length);
        }
        // Fewer than alignment bytes, too few to pay for a native memset.
        for (long b = alignment + (long) length; b < recordBytes; b++) {
            Unsafe.putByte(record + b, (byte) 0);
        }
        recordCount++;
        return handle;
    }

    /**
     * Starts encoding one record in place. The caller writes the record's bytes through
     * the returned sink and then calls {@link #commitRecord()}, or {@link #abortRecord()}
     * to drop it. A record begun earlier and never committed - its encoder threw - is
     * dropped here, so it leaves no hole in the arena.
     */
    MemoryA beginRecord() {
        if (pendingRecordOffset != NO_PENDING_RECORD) {
            memory.jumpTo(pendingRecordOffset);
        } else {
            pendingRecordOffset = memory.getAppendOffset();
        }
        for (int i = 0; i < alignment; i += Integer.BYTES) {
            memory.putInt(0);
        }
        return memory;
    }

    /**
     * @return the offset in {@link #memory()} of the record bytes {@code handle} names,
     * which together frame the record as a page with no copy
     */
    long bytesOffset(long handle) {
        assert isHandleValid(handle) : "live view checkpoint " + kind + " handle outside its arena";
        return handle + alignment;
    }

    /**
     * @return the native bytes the arena holds, used or not
     */
    long capacity() {
        return memory.size();
    }

    /**
     * Ends the record {@link #beginRecord()} started: patches its length with the bytes
     * written since and pads the record.
     *
     * @return the new record's handle
     */
    long commitRecord() {
        assert pendingRecordOffset != NO_PENDING_RECORD
                : "live view checkpoint " + kind + " committed without being begun";
        final long handle = pendingRecordOffset;
        final long length = memory.getAppendOffset() - handle - alignment;
        if (length > Integer.MAX_VALUE) {
            abortRecord();
            throw CairoException.critical(0)
                    .put("live view checkpoint ").put(kind).put(" is too long, length=").put(length);
        }
        assert length > 0 || isEmptyRecordAllowed : "live view checkpoint " + kind + " must not be empty";
        Unsafe.putInt(memory.addressOf(handle), (int) length);
        padRecord();
        pendingRecordOffset = NO_PENDING_RECORD;
        recordCount++;
        return handle;
    }

    /**
     * Replaces this arena's records with {@code other}'s, byte for byte, so every handle
     * that named a record in {@code other} names the same record here. The copy is this
     * arena's own: nothing {@code other} does afterwards reaches it.
     */
    void copyFrom(@NotNull LiveViewCheckpointRecordArena other) {
        assert other != this;
        assert other.alignment == alignment : "live view checkpoint arena copied across record layouts";
        assert other.pendingRecordOffset == NO_PENDING_RECORD
                : "live view checkpoint " + kind + " arena copied mid-record";
        clear();
        final long size = other.memory.getAppendOffset();
        if (size > 0) {
            // Throws before it moves the append offset, which leaves this arena empty
            // rather than holding part of the copy.
            final long dst = memory.appendAddressFor(size);
            Vect.memcpy(dst, other.memory.addressOf(0), size);
        }
        recordCount = other.recordCount;
    }

    /**
     * Drops the record {@code handle} names, which must be the last one committed.
     */
    void discardLastRecord(long handle) {
        assert pendingRecordOffset == NO_PENDING_RECORD;
        assert recordCount > 0 && handle >= 0 && handle < memory.getAppendOffset();
        memory.jumpTo(handle);
        recordCount--;
    }

    /**
     * Grows the arena, at most once, so that {@code additionalBytes} more bytes of records
     * fit without another allocation. A no-op when they already fit. A presize only: call
     * it before an encoding loop starts, never inside one while an address is live.
     */
    void ensureCapacity(long additionalBytes) {
        assert additionalBytes >= 0;
        memory.extend(memory.getAppendOffset() + additionalBytes);
    }

    /**
     * @return the record length {@code handle} names
     */
    int length(long handle) {
        assert isHandleValid(handle) : "live view checkpoint " + kind + " handle outside its arena";
        return Unsafe.getInt(memory.addressOf(handle));
    }

    /**
     * @return the arena's storage, for framing a record in place with {@link #bytesOffset};
     * valid until the next call that appends to, grows, clears or frees this arena
     */
    MemoryR memory() {
        return memory;
    }

    /**
     * @return the bytes a record of {@code length} bytes occupies in this arena, its
     * header and padding included
     */
    long recordBytes(int length) {
        final long mask = alignment - 1;
        return alignment + ((length + mask) & ~mask);
    }

    /**
     * @return the records the arena holds since it was last cleared or released
     */
    int recordCount() {
        return recordCount;
    }

    /**
     * Frees the arena's memory. The arena stays usable: the next record allocates afresh.
     */
    void release() {
        memory.close();
        recordCount = 0;
        pendingRecordOffset = NO_PENDING_RECORD;
    }

    /**
     * Appends one zero-filled record of {@code length} bytes, which the caller then
     * encodes in place at {@link #address}.
     *
     * @return the new record's handle
     */
    long reserve(int length) {
        assert pendingRecordOffset == NO_PENDING_RECORD
                : "live view checkpoint " + kind + " reserved while another is being encoded";
        assert length >= 0;
        assert length > 0 || isEmptyRecordAllowed : "live view checkpoint " + kind + " must not be empty";
        final long recordBytes = recordBytes(length);
        final long handle = memory.getAppendOffset();
        // Throws before it moves the append offset, so a failed reserve leaves the arena
        // exactly as it was.
        final long record = memory.appendAddressFor(recordBytes);
        Vect.memset(record, recordBytes, 0);
        Unsafe.putInt(record, length);
        recordCount++;
        return handle;
    }

    /**
     * @return the bytes the arena's records occupy, headers and padding included
     */
    long size() {
        return memory.getAppendOffset();
    }

    private boolean isHandleValid(long handle) {
        final long size = memory.getAppendOffset();
        if (handle < 0 || (handle & (alignment - 1)) != 0 || handle + alignment > size) {
            return false;
        }
        final int length = Unsafe.getInt(memory.addressOf(handle));
        return (length > 0 || (length == 0 && isEmptyRecordAllowed)) && handle + recordBytes(length) <= size;
    }

    private void padRecord() {
        final long misalignment = memory.getAppendOffset() & (alignment - 1);
        if (misalignment != 0) {
            for (long i = misalignment; i < alignment; i++) {
                memory.putByte((byte) 0);
            }
        }
    }

    private void putHeader(long record, int length) {
        Unsafe.putInt(record, length);
        for (int offset = Integer.BYTES; offset < alignment; offset += Integer.BYTES) {
            Unsafe.putInt(record + offset, 0);
        }
    }
}
