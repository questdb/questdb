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
 * Append-only native storage for encoded checkpoint partition keys. Each key is one
 * record, {@code [int length][key bytes]}, padded to a four-byte boundary, and the
 * record's offset is the key's handle. A holder keeps the handle, never an address: the
 * arena is one contiguous region that moves when it grows, so an address is valid only
 * until the next append, while a handle stays valid until {@link #clear()},
 * {@link #release()} or {@link #close()}.
 * <p>
 * A key is either copied in whole with {@link #append(long, int)} or encoded in place
 * between {@link #beginKey()} and {@link #commitKey()}, which is what lets a producer
 * write a key straight into the arena instead of into a scratch buffer it then copies.
 * <p>
 * Allocation is lazy: the constructor reserves nothing, so a field initializer never
 * strands native memory when its owner's constructor throws. The memory is tagged
 * {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}, which the process totals count, and no
 * view's refresh tracker counts it: the arena holds keys that were heap arrays
 * {@code cairo.live.view.refresh.memory.limit.bytes} never covered, so every allocation
 * and free here is untracked.
 */
final class LiveViewCheckpointKeyArena implements Mutable, QuietCloseable {
    private static final int LENGTH_PREFIX_BYTES = Integer.BYTES;
    private static final long NO_PENDING_KEY = -1;
    private static final long PAGE_SIZE = 4096;
    private final MemoryCARWImpl memory = new MemoryCARWImpl(PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    private int keyCount;
    private long pendingKeyOffset = NO_PENDING_KEY;

    /**
     * Forgets every key while keeping the capacity the arena has grown to.
     */
    @Override
    public void clear() {
        if (memory.getAppendOffset() > 0) {
            memory.jumpTo(0);
        }
        keyCount = 0;
        pendingKeyOffset = NO_PENDING_KEY;
    }

    /**
     * Frees the arena's memory, exactly as {@link #release()} does. Idempotent.
     */
    @Override
    public void close() {
        release();
    }

    /**
     * Drops the key {@link #beginKey()} started, leaving the arena as it was before.
     * A no-op when no key is pending.
     */
    void abortKey() {
        if (pendingKeyOffset != NO_PENDING_KEY) {
            memory.jumpTo(pendingKeyOffset);
            pendingKeyOffset = NO_PENDING_KEY;
        }
    }

    /**
     * @return the address of the key bytes {@code handle} names, valid until the next
     * append to this arena
     */
    long address(long handle) {
        assert handle >= 0 && handle + LENGTH_PREFIX_BYTES <= memory.getAppendOffset()
                : "live view checkpoint key handle outside its arena";
        return memory.addressOf(handle + LENGTH_PREFIX_BYTES);
    }

    /**
     * Copies {@code keyLength} key bytes at {@code keyAddress} into the arena. The source
     * must not lie inside this arena, which may move while it copies.
     *
     * @return the new key's handle
     */
    long append(long keyAddress, int keyLength) {
        assert pendingKeyOffset == NO_PENDING_KEY : "live view checkpoint key appended while another is being encoded";
        assert keyLength >= 0;
        assert !isArenaRange(keyAddress, keyLength) : "live view checkpoint key aliases its own arena";
        final long handle = memory.getAppendOffset();
        memory.putInt(keyLength);
        if (keyLength > 0) {
            memory.putBlockOfBytes(keyAddress, keyLength);
        }
        padRecord();
        keyCount++;
        return handle;
    }

    /**
     * Starts encoding one key in place. The caller writes the key's bytes through the
     * returned sink and then calls {@link #commitKey()}, or {@link #abortKey()} to drop
     * it. A key begun earlier and never committed - its encoder threw - is dropped here,
     * so it leaves no hole in the arena.
     */
    MemoryA beginKey() {
        if (pendingKeyOffset != NO_PENDING_KEY) {
            memory.jumpTo(pendingKeyOffset);
        } else {
            pendingKeyOffset = memory.getAppendOffset();
        }
        memory.putInt(0);
        return memory;
    }

    /**
     * @return the offset in {@link #memory()} of the key bytes {@code handle} names, which
     * together frame the key as a page with no copy
     */
    long bytesOffset(long handle) {
        assert handle >= 0 && handle + LENGTH_PREFIX_BYTES <= memory.getAppendOffset()
                : "live view checkpoint key handle outside its arena";
        return handle + LENGTH_PREFIX_BYTES;
    }

    /**
     * @return the native bytes the arena holds, used or not
     */
    long capacity() {
        return memory.size();
    }

    /**
     * Ends the key {@link #beginKey()} started: patches its length prefix with the
     * bytes written since and pads the record.
     *
     * @return the new key's handle
     */
    long commitKey() {
        assert pendingKeyOffset != NO_PENDING_KEY : "live view checkpoint key committed without being begun";
        final long handle = pendingKeyOffset;
        final long keyLength = memory.getAppendOffset() - handle - LENGTH_PREFIX_BYTES;
        if (keyLength > Integer.MAX_VALUE) {
            abortKey();
            throw CairoException.critical(0)
                    .put("live view checkpoint partition key is too long, length=").put(keyLength);
        }
        Unsafe.putInt(memory.addressOf(handle), (int) keyLength);
        padRecord();
        pendingKeyOffset = NO_PENDING_KEY;
        keyCount++;
        return handle;
    }

    /**
     * Replaces this arena's keys with {@code other}'s, byte for byte, so every handle
     * that named a key in {@code other} names the same key here. The copy is this
     * arena's own: nothing {@code other} does afterwards reaches it.
     */
    void copyFrom(@NotNull LiveViewCheckpointKeyArena other) {
        assert other != this;
        assert other.pendingKeyOffset == NO_PENDING_KEY : "live view checkpoint key arena copied mid-key";
        clear();
        final long size = other.memory.getAppendOffset();
        if (size > 0) {
            // Throws before it moves the append offset, which leaves this arena empty
            // rather than holding part of the copy.
            final long dst = memory.appendAddressFor(size);
            Vect.memcpy(dst, other.memory.addressOf(0), size);
        }
        keyCount = other.keyCount;
    }

    /**
     * Drops the key {@code handle} names, which must be the last one committed.
     */
    void discardLastKey(long handle) {
        assert pendingKeyOffset == NO_PENDING_KEY;
        assert keyCount > 0 && handle >= 0 && handle < memory.getAppendOffset();
        memory.jumpTo(handle);
        keyCount--;
    }

    int keyCount() {
        return keyCount;
    }

    int length(long handle) {
        assert handle >= 0 && handle + LENGTH_PREFIX_BYTES <= memory.getAppendOffset()
                : "live view checkpoint key handle outside its arena";
        return Unsafe.getInt(memory.addressOf(handle));
    }

    /**
     * @return the arena's storage, for framing a key in place with {@link #bytesOffset};
     * valid until the next append
     */
    MemoryR memory() {
        return memory;
    }

    /**
     * Frees the arena's memory. The arena stays usable: the next append allocates afresh.
     */
    void release() {
        memory.close();
        keyCount = 0;
        pendingKeyOffset = NO_PENDING_KEY;
    }

    /**
     * @return the bytes the arena's records occupy, padding included
     */
    long size() {
        return memory.getAppendOffset();
    }

    private boolean isArenaRange(long address, int length) {
        final long lo = memory.getPageAddress(0);
        return length > 0 && lo != 0 && address < memory.addressHi() && address + length > lo;
    }

    private void padRecord() {
        final long misalignment = memory.getAppendOffset() & (Integer.BYTES - 1);
        if (misalignment != 0) {
            for (long i = misalignment; i < Integer.BYTES; i++) {
                memory.putByte((byte) 0);
            }
        }
    }
}
