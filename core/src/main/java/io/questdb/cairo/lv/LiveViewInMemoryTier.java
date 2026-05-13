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

import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * N=2 double-buffered in-memory tier for a live view (RFC 123 §"In-memory tier").
 * One slot is published for readers; the other is available for the writer to
 * fill during a slow-path copy + append cycle. Readers pin a slot via a CAS
 * refcount; the writer takes a slot with a {@code 0 -> -1} sentinel CAS that
 * fails while any reader pins it.
 * <p>
 * Phase 1b ships the slow-path-only flow: every refresh cycle, the writer
 * acquires the non-published slot, copies still-in-window rows from the
 * published slot, appends the new rows produced this cycle, then flips
 * {@code publishedIdx}. The in-place fast-path append described in the RFC is
 * deferred to Phase 3.
 * <p>
 * Refcounts live in a 16-byte off-heap region (one long per slot) so all CAS
 * traffic uses {@link Os#compareAndSwap(long, long, long)} — no
 * {@code AtomicIntegerArray} on the hot path. Native memory is tagged
 * {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}.
 * <p>
 * The {@code rc == -1} sentinel means "writer in flight on this slot." A reader
 * that observes {@code rc < 0} during its acquire spins until the writer
 * releases (the slow path is bounded by a single column-slab copy, so the spin
 * is short).
 * <p>
 * Close is <strong>deferred</strong>: a {@link #close()} call only marks the
 * tier closed and prevents new {@link #acquireRead()} pins; native memory
 * frees on the last {@link #releaseRead(int)} that returns the live pin
 * count to zero. This is the RFC 123 §"DROP LIVE VIEW" step 4 "modulo cursor
 * pins" clause — a cursor holding a slot pin can outlive the LV's DROP and
 * still call {@code releaseRead} safely.
 */
public class LiveViewInMemoryTier implements QuietCloseable {

    private static final int CLOSED_BIT = 1 << 31;
    private static final long REFCOUNTS_BYTES = 2L * Long.BYTES;
    private static final long RC_WRITER_SENTINEL = -1L;
    private final LiveViewInMemoryBuffer[] slots;
    // High bit = close requested; low 31 bits = active read-pin count. A
    // single atomic lets acquireRead reject post-close (and bound the close-
    // race window) while still freeing native memory eagerly when no cursor
    // is pinning a slot. The 31-bit counter is more than enough for any
    // realistic reader concurrency.
    private final AtomicInteger state = new AtomicInteger(0);
    private volatile int publishedIdx;
    private long refCountsAddr;

    public LiveViewInMemoryTier(IntList columnTypes, int timestampColumnIndex, long pageSize) {
        this.slots = new LiveViewInMemoryBuffer[2];
        try {
            this.slots[0] = new LiveViewInMemoryBuffer(columnTypes, timestampColumnIndex, pageSize);
            this.slots[1] = new LiveViewInMemoryBuffer(columnTypes, timestampColumnIndex, pageSize);
            this.refCountsAddr = Unsafe.malloc(REFCOUNTS_BYTES, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            Unsafe.getUnsafe().putLong(refCountsAddr, 0L);
            Unsafe.getUnsafe().putLong(refCountsAddr + Long.BYTES, 0L);
        } catch (Throwable t) {
            // Defensive: any partial alloc must not leak.
            freeNativeMemory();
            throw t;
        }
        this.publishedIdx = 0;
    }

    /**
     * Acquires a read pin on the currently published slot. Returns the slot
     * index that was pinned, or {@code -1} if the tier has already been
     * closed (in which case the caller must NOT call {@link #releaseRead(int)}).
     * The caller releases a successful pin with the same index via
     * {@link #releaseRead(int)}.
     * <p>
     * Spins while the published slot is held by a writer ({@code rc < 0}).
     * If the writer publishes a swap during the spin the loop re-reads
     * {@code publishedIdx} and retries on the new slot.
     */
    public int acquireRead() {
        // Bump the live-pin counter first so a concurrent close() cannot free
        // native memory underneath the about-to-happen Unsafe writes. If the
        // tier is already closed, bail out before touching refCountsAddr.
        if (!tryIncrementPinCount()) {
            return -1;
        }
        while (true) {
            int idx = publishedIdx;
            long addr = refCountsAddr + ((long) idx) * Long.BYTES;
            long current = Unsafe.getLongVolatile(addr);
            if (current < 0) {
                // Writer in flight on this slot. Yield and re-read; publishedIdx may
                // have moved (slow-path swap completing) or stay on the same slot
                // while a fast-path-style in-place op (Phase 3) finishes.
                Os.pause();
                continue;
            }
            if (Os.compareAndSwap(addr, current, current + 1) == current) {
                // Re-check publishedIdx: a swap may have moved away from this slot
                // between the publishedIdx read and the CAS. If so, release the
                // per-slot rc directly (we still hold the global pin lease, which
                // the next acquireRead iteration consumes) and retry on the new
                // slot.
                if (publishedIdx == idx) {
                    return idx;
                }
                releasePerSlotRc(idx);
            }
        }
    }

    /**
     * Marks the tier closed. New {@link #acquireRead()} calls return {@code -1}
     * from this point. If no cursor currently holds a pin, native memory is
     * freed synchronously; otherwise the last {@link #releaseRead(int)} that
     * drains the pin count to zero performs the free. Idempotent.
     */
    @Override
    public void close() {
        while (true) {
            int s = state.get();
            if ((s & CLOSED_BIT) != 0) {
                // Another caller already marked the tier closed; the free will
                // happen exactly once via either the racing close() or the last
                // releaseRead.
                return;
            }
            if (state.compareAndSet(s, s | CLOSED_BIT)) {
                if ((s & ~CLOSED_BIT) == 0) {
                    // No active read pins: safe to free native memory here.
                    freeNativeMemory();
                }
                return;
            }
        }
    }

    /**
     * Returns the sum of both slots' footprint in bytes — used by
     * {@code live_views().in_mem_bytes}.
     */
    public long footprintBytes() {
        long sum = 0;
        if (slots[0] != null) {
            sum += slots[0].footprintBytes();
        }
        if (slots[1] != null) {
            sum += slots[1].footprintBytes();
        }
        return sum;
    }

    public int getPublishedIdx() {
        return publishedIdx;
    }

    /**
     * Returns the buffer for the given slot index. Lets callers manipulate slot
     * contents during a pinned acquire / write window; not safe to call without
     * holding either a read pin or the writer sentinel on the slot.
     */
    public LiveViewInMemoryBuffer getSlot(int idx) {
        return slots[idx];
    }

    /**
     * Flips {@code publishedIdx} to {@code newPublishedIdx} and releases the
     * writer sentinel on the just-filled slot. The caller must hold the
     * sentinel on {@code newPublishedIdx} (acquired via
     * {@link #tryAcquireWrite(int)}); after this call, the new slot is visible
     * to readers and idle (refcount = 0). The old published slot's refcount is
     * unchanged: readers that pinned it continue to do so until they release.
     * <p>
     * Same-slot publish (Phase 3 fast-path in-place append) reuses the same
     * code path: the slot the writer filled is the slot to release.
     */
    public void publishSwap(int newPublishedIdx) {
        publishedIdx = newPublishedIdx;
        long addr = refCountsAddr + ((long) newPublishedIdx) * Long.BYTES;
        long observed = Os.compareAndSwap(addr, RC_WRITER_SENTINEL, 0L);
        if (observed != RC_WRITER_SENTINEL) {
            throw new IllegalStateException(
                    "publishSwap: writer sentinel not held [slot=" + newPublishedIdx
                            + ", observed=" + observed + "]"
            );
        }
    }

    public void releaseRead(int slotIdx) {
        releasePerSlotRc(slotIdx);
        // Drop the global lease taken at acquireRead; if this was the last
        // pin and close() has been requested in the meantime, free now.
        int after = state.decrementAndGet();
        if (after == CLOSED_BIT) {
            freeNativeMemory();
        }
    }

    /**
     * Drops the writer sentinel on {@code slotIdx} without flipping
     * {@code publishedIdx}. Used by the refresh worker's slow-path error
     * branch: if a copy throws mid-swap, we want to release the sentinel
     * (so the slot can be retried next cycle) without exposing partial /
     * zero-row contents to readers as the new published state.
     */
    public void releaseWriteWithoutPublish(int slotIdx) {
        long addr = refCountsAddr + ((long) slotIdx) * Long.BYTES;
        long observed = Os.compareAndSwap(addr, RC_WRITER_SENTINEL, 0L);
        if (observed != RC_WRITER_SENTINEL) {
            throw new IllegalStateException(
                    "releaseWriteWithoutPublish: writer sentinel not held [slot=" + slotIdx
                            + ", observed=" + observed + "]"
            );
        }
    }

    /**
     * Attempts to take the writer sentinel on the requested slot via a
     * {@code 0 -> -1} CAS. Returns the slot's buffer on success, or
     * {@code null} on failure (some reader has the slot pinned). The caller
     * must follow up with {@link #publishSwap(int)} on success to publish the
     * new slot and release the sentinel, or
     * {@link #releaseWriteWithoutPublish(int)} to release the sentinel
     * without flipping {@code publishedIdx}.
     */
    public LiveViewInMemoryBuffer tryAcquireWrite(int slotIdx) {
        long addr = refCountsAddr + ((long) slotIdx) * Long.BYTES;
        if (Os.compareAndSwap(addr, 0L, RC_WRITER_SENTINEL) == 0L) {
            return slots[slotIdx];
        }
        return null;
    }

    /**
     * Frees the column buffers and the off-heap refcount block. Called either
     * synchronously from {@link #close()} (when no pins are active) or from
     * the last {@link #releaseRead(int)} that drains the pin count.
     * Idempotent within a single tier instance — the AtomicInteger CAS
     * protocol guarantees exactly one caller reaches here.
     */
    private void freeNativeMemory() {
        Misc.free(slots[0]);
        Misc.free(slots[1]);
        slots[0] = null;
        slots[1] = null;
        if (refCountsAddr != 0) {
            refCountsAddr = Unsafe.free(refCountsAddr, REFCOUNTS_BYTES, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        }
    }

    private void releasePerSlotRc(int slotIdx) {
        long addr = refCountsAddr + ((long) slotIdx) * Long.BYTES;
        while (true) {
            long current = Unsafe.getLongVolatile(addr);
            if (current <= 0) {
                throw new IllegalStateException(
                        "releaseRead: refcount underflow [slot=" + slotIdx + ", rc=" + current + "]"
                );
            }
            if (Os.compareAndSwap(addr, current, current - 1) == current) {
                return;
            }
        }
    }

    private boolean tryIncrementPinCount() {
        while (true) {
            int s = state.get();
            if ((s & CLOSED_BIT) != 0) {
                return false;
            }
            if (state.compareAndSet(s, s + 1)) {
                return true;
            }
        }
    }
}
