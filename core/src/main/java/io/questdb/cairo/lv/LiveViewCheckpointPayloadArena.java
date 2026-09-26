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

import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Append-only native storage for encoded checkpoint state payloads: an inline scalar
 * image, a fused window-state payload or a member state image. Each payload is one
 * record, {@code [int length][int 0][payload bytes]}, zero-padded to an eight-byte
 * boundary, and the record's offset is the payload's handle. A holder keeps the handle,
 * never an address: the arena is one contiguous region that moves when it grows, so an
 * address is valid only until the next {@link #reserve}, {@link #append},
 * {@link #ensureCapacity}, {@link #clear()}, {@link #release()} or {@link #close()}, while a
 * handle stays valid until the next clear, release or close.
 * <p>
 * A payload is never empty. A holder that has no payload to name - a page-backed
 * partition, or a key outside the key domain a seal images - stores {@link #NO_PAYLOAD},
 * which no record's handle can equal.
 * <p>
 * The padding is part of the byte format, not slack: the heap encoders this arena
 * replaces wrote into zero-filled arrays, and an encoder that leaves a field unwritten
 * (a guarded count on a NULL partition key, say) relies on the zeroes. {@link #reserve}
 * therefore zero-fills the whole record, including over bytes an earlier payload left
 * behind after {@link #clear()}.
 * <p>
 * Allocation is lazy: the constructor reserves nothing, so a field initializer never
 * strands native memory when its owner's constructor throws. The memory is tagged
 * {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}, which the process totals count, and no view's
 * refresh tracker counts it: the arena holds payloads that were heap arrays
 * {@code cairo.live.view.refresh.memory.limit.bytes} never covered, so every allocation
 * and free here is untracked.
 * <p>
 * Kept apart from {@link LiveViewCheckpointKeyArena} on purpose: a key and its payload
 * live in two arenas, so an append of one kind never moves the other while a caller
 * holds its address.
 */
final class LiveViewCheckpointPayloadArena implements Mutable, QuietCloseable {
    /**
     * The handle of no payload. Records start at non-negative offsets, so it never
     * names one.
     */
    static final long NO_PAYLOAD = -1;
    private static final int HEADER_BYTES = 2 * Integer.BYTES;
    private static final long PAGE_SIZE = 4096;
    private final MemoryCARWImpl memory = new MemoryCARWImpl(PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    private int payloadCount;

    /**
     * @return the bytes one record of a {@code payloadLength}-byte payload occupies, its
     * header and padding included
     */
    static long recordBytes(int payloadLength) {
        return HEADER_BYTES + ((payloadLength + 7L) & ~7L);
    }

    /**
     * Forgets every payload while keeping the capacity the arena has grown to.
     */
    @Override
    public void clear() {
        if (memory.getAppendOffset() > 0) {
            memory.jumpTo(0);
        }
        payloadCount = 0;
    }

    /**
     * Frees the arena's memory, exactly as {@link #release()} does. Idempotent.
     */
    @Override
    public void close() {
        release();
    }

    /**
     * @return the address of the payload bytes {@code handle} names, valid until the next
     * call that appends to, grows, clears or frees this arena
     */
    long address(long handle) {
        assert isHandleValid(handle) : "live view checkpoint payload handle outside its arena";
        return memory.addressOf(handle + HEADER_BYTES);
    }

    /**
     * Copies {@code length} payload bytes at {@code address} into the arena. The source
     * must not lie inside this arena, which may move while it copies.
     *
     * @return the new payload's handle
     */
    long append(long address, int length) {
        assert length > 0 : "live view checkpoint payload must not be empty";
        assert !isArenaRange(address, length) : "live view checkpoint payload aliases its own arena";
        final long recordBytes = recordBytes(length);
        final long handle = memory.getAppendOffset();
        // Throws before it moves the append offset, so a failed append leaves the arena
        // exactly as it was.
        final long record = memory.appendAddressFor(recordBytes);
        Unsafe.putInt(record, length);
        Unsafe.putInt(record + Integer.BYTES, 0);
        Vect.memcpy(record + HEADER_BYTES, address, length);
        final long padding = recordBytes - HEADER_BYTES - length;
        if (padding > 0) {
            Vect.memset(record + HEADER_BYTES + length, padding, 0);
        }
        payloadCount++;
        return handle;
    }

    /**
     * @return the offset in {@link #memory()} of the payload bytes {@code handle} names,
     * which together frame the payload as a page with no copy
     */
    long bytesOffset(long handle) {
        assert isHandleValid(handle) : "live view checkpoint payload handle outside its arena";
        return handle + HEADER_BYTES;
    }

    /**
     * @return the native bytes the arena holds, used or not
     */
    long capacity() {
        return memory.size();
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
     * @return the payload length {@code handle} names
     */
    int length(long handle) {
        assert isHandleValid(handle) : "live view checkpoint payload handle outside its arena";
        return Unsafe.getInt(memory.addressOf(handle));
    }

    /**
     * @return the arena's storage, for framing a payload in place with {@link #bytesOffset};
     * valid until the next call that appends to, grows, clears or frees this arena
     */
    MemoryR memory() {
        return memory;
    }

    /**
     * @return the payloads the arena holds since it was last cleared or released
     */
    int payloadCount() {
        return payloadCount;
    }

    /**
     * Frees the arena's memory. The arena stays usable: the next record allocates afresh.
     */
    void release() {
        memory.close();
        payloadCount = 0;
    }

    /**
     * Appends one zero-filled record for a {@code length}-byte payload, which the caller
     * then encodes in place at {@link #address}.
     *
     * @return the new payload's handle
     */
    long reserve(int length) {
        assert length > 0 : "live view checkpoint payload must not be empty";
        final long recordBytes = recordBytes(length);
        final long handle = memory.getAppendOffset();
        // Throws before it moves the append offset, so a failed reserve leaves the arena
        // exactly as it was.
        final long record = memory.appendAddressFor(recordBytes);
        Vect.memset(record, recordBytes, 0);
        Unsafe.putInt(record, length);
        payloadCount++;
        return handle;
    }

    /**
     * @return the bytes the arena's records occupy, headers and padding included
     */
    long size() {
        return memory.getAppendOffset();
    }

    private boolean isArenaRange(long address, int length) {
        final long lo = memory.getPageAddress(0);
        return length > 0 && lo != 0 && address < memory.addressHi() && address + length > lo;
    }

    private boolean isHandleValid(long handle) {
        final long size = memory.getAppendOffset();
        if (handle < 0 || (handle & (Long.BYTES - 1)) != 0 || handle + HEADER_BYTES > size) {
            return false;
        }
        final int length = Unsafe.getInt(memory.addressOf(handle));
        return length > 0 && handle + recordBytes(length) <= size;
    }
}
