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
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * N=2 double-buffered in-memory tier for a live view.
 * One slot is published for readers; the other is available for the writer to
 * fill during a slow-path copy + append cycle. Readers pin a slot via a CAS
 * refcount; the writer takes a slot with a {@code 0 -> -1} sentinel CAS that
 * fails while any reader pins it.
 * <p>
 * Two writer paths share the same primitive:
 * <ul>
 *   <li><b>Slow-path swap</b> — writer calls
 *     {@link #tryAcquireWrite(int)} on the <em>non-published</em> slot, copies
 *     retained rows from the published slot, appends new rows, and flips the
 *     published index via {@link #publishSwap(int)}. The post-flip release
 *     also drops the writer sentinel on the new slot.</li>
 *   <li><b>Fast-path in-place append</b> — writer calls
 *     {@link #tryAcquireWrite(int)} on the <em>published</em> slot, appends
 *     new rows in place, and drops the sentinel via
 *     {@link #releaseWriteWithoutPublish(int)} without changing
 *     {@code publishedIdx}. Requires zero active read pins on the published
 *     slot (the {@code 0 -> -1} CAS fails otherwise); the caller falls back
 *     to the slow path on conflict.</li>
 * </ul>
 * Both paths use the same CAS primitive and release through one of the two
 * complementary methods; there is no fast-path-specific API. A symbol-cache
 * rewind ({@link #tryRewindSymbolCache}) takes the sentinel on every slot whose
 * symbol horizon reaches the band it takes back, which proves that no reader
 * pins such a slot; a pinned slot whose horizon stops below the band does not
 * hold the rewind up.
 * <p>
 * Refcounts live in a 16-byte off-heap region (one long per slot) so all CAS
 * traffic uses {@link Os#compareAndSwap(long, long, long)} — no
 * {@code AtomicIntegerArray} on the hot path. Native memory is tagged
 * {@link MemoryTag#NATIVE_LIVE_VIEW_IN_MEM}.
 * <p>
 * The {@code rc == -1} sentinel means "writer in flight on this slot." A reader
 * that observes {@code rc < 0} during its acquire spins until the writer
 * releases (the slow path is bounded by a single column-slab copy and the
 * fast-path by an in-place row append, so the spin is short).
 * <p>
 * Close is <strong>deferred</strong>: a {@link #close()} call only marks the
 * tier closed and prevents new {@link #acquireRead()} pins; native memory
 * frees on the last {@link #releaseRead(int)} that returns the live pin
 * count to zero. This is the DROP LIVE VIEW "modulo cursor pins" clause — a
 * cursor holding a slot pin can outlive the LV's DROP and still call
 * {@code releaseRead} safely.
 */
public class LiveViewInMemoryTier implements QuietCloseable {

    private static final int CLOSED_BIT = 1 << 31;
    private static final long REFCOUNTS_BYTES = 2L * Long.BYTES;
    private static final long RC_WRITER_SENTINEL = -1L;
    // Matches LiveViewInstance.LATCH_SPINS_BEFORE_SLEEP: long enough to absorb a
    // fast-path append without a context switch, short enough that a slow-path
    // swap does not burn a core.
    private static final int SENTINEL_SPINS_BEFORE_SLEEP = 64;
    private final LiveViewInMemoryBuffer[] slots;
    // Eager-interning symbol cache for the un-flushed lead, shared across both
    // slots (symbol ids live in one LV-table id space, slot-independent). Holds
    // the id -> string mapping cursors resolve the lead from, plus the refresh
    // worker's window intern state. Empty (no symbol columns) for a non-SYMBOL
    // output schema. Freed with the tier's native memory on the last pin release.
    private final LiveViewSymbolCache symbolCache;
    // High bit = close requested; low 31 bits = active read-pin count. A
    // single atomic lets acquireRead reject post-close (and bound the close-
    // race window) while still freeing native memory eagerly when no cursor
    // is pinning a slot. The 31-bit counter is more than enough for any
    // realistic reader concurrency.
    private final AtomicInteger state = new AtomicInteger(0);
    // Test-only hook fired each time {@link #acquireRead()} observes the writer
    // sentinel (rc < 0) on the published slot and is about to spin. Production
    // code never sets this; it lets a concurrency test prove the negative-sentinel
    // spin branch was actually reached (and, by the reader staying blocked, that a
    // read cannot pin a slot mid-write) without a timing-based poll.
    @TestOnly
    private volatile Runnable acquireReadSentinelSpinHook;
    // Test-only failure injection for {@link #publishSwap}. Production code
    // never sets this. When non-null, the next publishSwap call throws the
    // stored exception instead of flipping publishedIdx and releasing the
    // writer sentinel; the field is cleared at the same time so subsequent
    // calls succeed. The caller (LiveViewRefreshJob.publishToInMemoryTier)
    // must release the sentinel via releaseWriteWithoutPublish in its catch
    // block — this is exactly the contract the production catch path relies
    // on, so the injection exercises the recovery path end-to-end.
    @TestOnly
    private volatile RuntimeException failNextPublishSwap;
    // Test-only failure injection for {@link #stampSymbolHorizon}, fired after
    // every horizon is stamped and before the reverse-index prune - the only
    // point that method can throw in production. Production code never sets it.
    // It exists because the sentinel-release paths must survive a stamp failure,
    // and nothing else can drive that branch deterministically.
    @TestOnly
    private volatile RuntimeException failNextSymbolHorizonStamp;
    // Whether the drain strandedIdDiskRouteUs records stopped on its turn budget.
    private boolean isStrandedIdDiskRouteBudgetStopped;
    private volatile int publishedIdx;
    private long refCountsAddr;
    // The symbol horizon deficit the drain strandedIdDiskRouteUs records started from.
    private long strandedIdDiskRouteDeficit;
    // When the refresh worker last sent a lead drain's rows straight to the LV table because
    // tryRewindSymbolCache could not take the stranded symbol ids back, or flushed a lead it
    // kept from readers and restaged the slot behind it, or LONG_NULL. The worker lets the next
    // such drain follow at once while these drains converge on the horizon a reader's pin holds
    // the rewind up at - the last one stopped on its turn budget below strandedIdLeadSeqTxn, or
    // the deficit below that horizon fell since it started - and otherwise allows one per FLUSH
    // EVERY interval and keeps the rows of the rest from readers until the cadence flush lands
    // them. This field, strandedIdDiskRouteDeficit, isStrandedIdDiskRouteBudgetStopped and
    // strandedIdLeadSeqTxn are read and written only by the refresh worker under the view's
    // refresh latch.
    private long strandedIdDiskRouteUs = Numbers.LONG_NULL;
    // The highest base seqTxn the view's refresh cursor stood at when the worker rebuilt this
    // tier, or LONG_NULL. Every recovery that discards an un-flushed lead rebuilds the tier
    // before any drain runs below the commits that lead covered, so no lead a reader can still
    // pin was built from a base commit above it - unless an out-of-order hand-off's parked
    // repair is discarded later, on a fault during its turn or a runtime change under it. No
    // rebuild then records the lead the hand-off dropped, so this can stand below that lead's
    // top; see getStrandedIdLeadSeqTxn.
    private long strandedIdLeadSeqTxn = Numbers.LONG_NULL;

    public LiveViewInMemoryTier(IntList columnTypes, int timestampColumnIndex, long pageSize) {
        this(columnTypes, timestampColumnIndex, pageSize, null);
    }

    /**
     * As {@link #LiveViewInMemoryTier(IntList, int, long)}, but charges each slot's
     * column arenas to {@code memoryTracker} so the tier's data-scaled footprint counts
     * against the live view's refresh memory limit. The fixed 16-byte {@code refCountsAddr}
     * stays on global-only accounting - it does not scale with data. A {@code null} tracker
     * is the unit-test path.
     *
     * @param memoryTracker per-view refresh tracker, or {@code null} for no per-view cap
     */
    public LiveViewInMemoryTier(IntList columnTypes, int timestampColumnIndex, long pageSize, @Nullable MemoryTracker memoryTracker) {
        this.slots = new LiveViewInMemoryBuffer[2];
        this.symbolCache = new LiveViewSymbolCache(columnTypes);
        try {
            this.slots[0] = new LiveViewInMemoryBuffer(columnTypes, timestampColumnIndex, pageSize, memoryTracker);
            this.slots[1] = new LiveViewInMemoryBuffer(columnTypes, timestampColumnIndex, pageSize, memoryTracker);
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
        int spins = 0;
        while (true) {
            int idx = publishedIdx;
            long addr = refCountsAddr + ((long) idx) * Long.BYTES;
            long current = Unsafe.getLongVolatile(addr);
            if (current < 0) {
                // Writer in flight on this slot. Yield and re-read; publishedIdx
                // may have moved (slow-path swap completing) or stay on the same
                // slot while a fast-path in-place append finishes.
                final Runnable hook = acquireReadSentinelSpinHook;
                if (hook != null) {
                    hook.run();
                }
                if (spins < SENTINEL_SPINS_BEFORE_SLEEP) {
                    // Stops incrementing at the threshold so a long wait cannot
                    // overflow back into the spin branch.
                    spins++;
                    Os.pause();
                    continue;
                }
                // A refresh turn can hold the sentinel for as long as its copy
                // takes, and this runs at cursor-open, before the query has a
                // circuit breaker to poll - so a bare pause loop would pin a core
                // for that whole time, once per reader.
                Os.sleep(1);
                continue;
            }
            spins = 0;
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
            if ((s & ~CLOSED_BIT) != 0) {
                // Read pins are outstanding: the native free (and its per-view-tracker decrement) is
                // deferred to the last releaseRead, on a reader thread that can outlive the tracker -- a
                // query cursor may stay open across a DROP / invalidate. Hand each slot's outstanding
                // tracker charge to the tracker's covered-bytes ledger (reconcileCovered() clears it
                // from the pooled tracker's used at release, so it recycles clean) and switch the slots
                // to global-only accounting, so the deferred free debits no recycled block. Detach
                // BEFORE publishing CLOSED_BIT via the CAS below: the CAS's release semantics make this
                // detach visible to any reader that later observes CLOSED_BIT through its own atomic
                // decrement, so the deferred free reads a null tracker. detachMemoryTracker is
                // idempotent, so a CAS-retry (a concurrent pin/unpin changed state) re-running it is a
                // no-op.
                for (int i = 0; i < slots.length; i++) {
                    if (slots[i] != null) {
                        slots[i].detachMemoryTracker();
                    }
                }
            }
            if (state.compareAndSet(s, s | CLOSED_BIT)) {
                if ((s & ~CLOSED_BIT) == 0) {
                    // No active read pins: free synchronously (and decrement the per-view tracker now,
                    // unless a pin arrived-then-drained mid-close, in which case the slots are already
                    // detached and reconcileCovered() settles the charge at the tracker's release).
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
        // Snapshot each slot into a local before dereferencing it. This runs
        // lock-free off the live_views() catalogue cursor with no read pin, so a
        // concurrent DROP / invalidate (close() nulls slots[0]/slots[1]) can
        // clear a slot between the null check and the deref - reading the field
        // twice would then NPE the monitoring query.
        final LiveViewInMemoryBuffer slot0 = slots[0];
        if (slot0 != null) {
            sum += slot0.footprintBytes();
        }
        final LiveViewInMemoryBuffer slot1 = slots[1];
        if (slot1 != null) {
            sum += slot1.footprintBytes();
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
     * The symbol horizon deficit the last lead drain the refresh worker sent straight to the
     * LV table started from; meaningful once {@link #getStrandedIdDiskRouteUs} is set.
     * Refresh-worker only, under the view's refresh latch; see {@link #setStrandedIdDiskRoute}.
     */
    public long getStrandedIdDiskRouteDeficit() {
        return strandedIdDiskRouteDeficit;
    }

    /**
     * When the refresh worker last sent a lead drain straight to the LV table because
     * {@link #tryRewindSymbolCache} left stranded symbol ids in the cache, or flushed a lead it
     * kept from readers and restaged the slot behind it, or {@link Numbers#LONG_NULL} when it
     * never has.
     * Refresh-worker only, under the view's refresh latch; see {@link #setStrandedIdDiskRoute}.
     */
    public long getStrandedIdDiskRouteUs() {
        return strandedIdDiskRouteUs;
    }

    /**
     * The highest base seqTxn the view's refresh cursor stood at when the refresh worker
     * rebuilt this tier, or {@link Numbers#LONG_NULL} when it never has. No lead a reader can
     * still pin was built from a base commit above it, unless an out-of-order hand-off's
     * parked repair is discarded later, on a fault during its turn or a runtime change under
     * it. Then no rebuild records the lead the hand-off dropped, and this can stand below that
     * lead's top. That errs the safe way: a drain that stops on its turn budget between this
     * and that top does not count as converging, so the refresh worker keeps the drains of
     * such a deferral from readers unless the last route shrank the symbol horizon deficit.
     * Refresh-worker only, under the view's refresh latch; see
     * {@link #setStrandedIdLeadSeqTxn}.
     */
    public long getStrandedIdLeadSeqTxn() {
        return strandedIdLeadSeqTxn;
    }

    /**
     * Returns the tier's eager-interning symbol cache. Holds the lead's
     * {@code id -> string} mapping (read by cursors) plus the refresh worker's
     * window intern state. Never null; {@link LiveViewSymbolCache#hasSymbolColumns()}
     * is false for a non-SYMBOL output schema.
     */
    public LiveViewSymbolCache getSymbolCache() {
        return symbolCache;
    }

    /**
     * Whether the last lead drain the refresh worker sent straight to the LV table stopped on
     * its turn budget, with base commits it was asked for still to drain. Refresh-worker only,
     * under the view's refresh latch; see {@link #setStrandedIdDiskRouteBudgetStopped}.
     */
    public boolean isStrandedIdDiskRouteBudgetStopped() {
        return isStrandedIdDiskRouteBudgetStopped;
    }

    /**
     * Flips {@code publishedIdx} to {@code newPublishedIdx} and releases the
     * writer sentinel on the just-filled slot. The caller must hold the
     * sentinel on {@code newPublishedIdx} (acquired via
     * {@link #tryAcquireWrite(int)}); after this call, the new slot is visible
     * to readers and idle (refcount = 0). The old published slot's refcount is
     * unchanged: readers that pinned it continue to do so until they release.
     * <p>
     * Same-slot publish is not the fast-path: a fast-path append leaves
     * {@code publishedIdx} untouched and releases the sentinel via
     * {@link #releaseWriteWithoutPublish(int)} instead. Calling
     * {@code publishSwap} on the same slot the reader already sees is harmless
     * but redundant.
     */
    public void publishSwap(int newPublishedIdx) {
        publishSwap(newPublishedIdx, null);
    }

    /**
     * As {@link #publishSwap(int)}, but stamps each SYMBOL column's symbol horizon no higher
     * than {@code symbolHorizonCaps} holds for it. A caller passes the LV table's committed
     * symbol counts for either of two kinds of slot:
     * <ul>
     *   <li>A slot that carries only committed ids - a slot staged from the table - so a
     *     reader of the slot sees no lead band at all. The cache can hold ids at or above the
     *     committed count that the slot's rows do not carry: the ids of a pass a recovery
     *     discarded, which a reader pinning a slot keeps {@link #tryRewindSymbolCache} from
     *     taking back. A horizon over them would expose those ids next to the committed ids
     *     the table gave the same values, and a reader enumerating the slot's symbol table
     *     would find one value under two keys.</li>
     *   <li>An unstamped slot that holds a lead kept from readers, whose rows can carry ids at
     *     or above the cap: the lead's drain interned its values past the stranded ids a
     *     reader's pin keeps in the cache, or past the ids of a block of the view's WAL the
     *     table has not applied. The caller leaves the slot's {@code lvSeqTxn} at
     *     {@link Numbers#LONG_NULL}, so the read fence ({@code LiveViewRouting.isFenced}) never
     *     passes for it, no read routes through it, and none reaches those ids. The cap keeps a
     *     reader that pins the slot meanwhile, reading disk-only, from holding a symbol-id
     *     rewind up. See {@code LiveViewRefreshJob.finishLeadRefresh} and
     *     {@code LiveViewRefreshJob.drainBaseWal}.</li>
     * </ul>
     * A capped slot whose rows carry ids at or above the cap must stay unstamped: stamping it
     * would let a read through the slot reach ids past the slot's horizon.
     *
     * @param symbolHorizonCaps the cap per output column, indexed by column; only SYMBOL
     *                          columns are read. {@code null} stamps the cache's whole band
     */
    public void publishSwap(int newPublishedIdx, @Nullable IntList symbolHorizonCaps) {
        RuntimeException injected = failNextPublishSwap;
        if (injected != null) {
            // Single-shot: clear so a subsequent publishSwap on the same tier
            // succeeds normally. The sentinel stays held on newPublishedIdx;
            // the caller's catch block clears it via
            // releaseWriteWithoutPublish (the production contract).
            failNextPublishSwap = null;
            throw injected;
        }
        // Capture each SYMBOL column's lead horizon onto the slot before it becomes
        // reader-visible. The sentinel-release CAS below is the happens-before edge
        // a reader's acquireRead CAS pairs with, so the stamped horizon (and the
        // backing arrays it bounds) publish to the reader safely.
        // No finally here, unlike releaseWriteWithoutPublish: a stamp that throws
        // must leave publishedIdx alone (the slot never becomes visible) and the
        // sentinel held, because the caller's catch releases it through that
        // method - which now drops the sentinel whatever the stamp does.
        stampSymbolHorizon(newPublishedIdx, symbolHorizonCaps);
        publishedIdx = newPublishedIdx;
        releaseWriterSentinel(newPublishedIdx, "publishSwap");
    }

    /**
     * Returns the number of rows currently held in the published
     * (reader-visible) slot - the live logical content of the in-mem tier,
     * the companion to {@link #footprintBytes()} which reports peak-sticky
     * allocated capacity across both slots. Used by
     * {@code live_views().in_mem_rows}.
     * <p>
     * Runs lock-free off the {@code live_views()} catalogue cursor with no read
     * pin. Snapshots {@code publishedIdx} and the slot reference into locals
     * before dereferencing, so a concurrent DROP / invalidate (close() nulls
     * both slots) cannot NPE the monitoring query; it may momentarily observe a
     * slot mid-swap, acceptable for an approximate monitoring metric. Returns 0
     * when the published slot has been cleared.
     */
    public long publishedRowCount() {
        final int idx = publishedIdx;
        final LiveViewInMemoryBuffer slot = slots[idx];
        return slot == null ? 0L : slot.rowCount();
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
     * {@code publishedIdx}. Two callers:
     * <ul>
     *   <li><b>Slow-path error branch</b> — if a copy throws mid-swap, the
     *     writer releases the sentinel (so the slot can be retried next
     *     cycle) without exposing partial / zero-row contents to readers as
     *     the new published state.</li>
     *   <li><b>Fast-path success</b> — the writer appended in
     *     place on the published slot; readers were already seeing this
     *     slot, so {@code publishedIdx} stays put and only the sentinel
     *     drops.</li>
     * </ul>
     */
    public void releaseWriteWithoutPublish(int slotIdx) {
        releaseWriteWithoutPublish(slotIdx, null);
    }

    /**
     * As {@link #releaseWriteWithoutPublish(int)}, but stamps each SYMBOL column's symbol
     * horizon no higher than {@code symbolHorizonCaps} holds for it, for a slot that carries
     * only committed ids or for an unstamped slot that holds a lead kept from readers. A capped
     * slot whose rows carry ids at or above the cap must stay unstamped. See
     * {@link #publishSwap(int, IntList)}.
     *
     * @param symbolHorizonCaps the cap per output column, indexed by column; only SYMBOL
     *                          columns are read. {@code null} stamps the cache's whole band
     */
    public void releaseWriteWithoutPublish(int slotIdx, @Nullable IntList symbolHorizonCaps) {
        // The fast-path success branch makes the in-place-appended rows reader-
        // visible here (publishedIdx unchanged); the error / both-pinned-skip
        // branches release a slot no reader will see. Stamping the symbol horizon
        // before the release CAS covers the visible case and is a harmless no-op
        // for the others (the slot is not published, or its rows are unchanged).
        //
        // The release is in a finally because this is the recovery path: it is what
        // publishToInMemoryTier's catch calls after publishSwap threw. If a stamp
        // failure escaped here the sentinel would stay set forever, and every
        // reader of this view would then spin on it - so the sentinel drops even
        // when the stamp cannot complete, and the failure propagates on its own.
        try {
            stampSymbolHorizon(slotIdx, symbolHorizonCaps);
        } finally {
            releaseWriterSentinel(slotIdx, "releaseWriteWithoutPublish");
        }
    }

    /**
     * Test-only hook: registers a callback that {@link #acquireRead()} runs each
     * time it observes the writer sentinel (rc &lt; 0) on the published slot and is
     * about to spin. A concurrency test uses it to prove the negative-sentinel spin
     * branch was actually reached, deterministically and without a timing poll.
     * Production code never sets this.
     */
    @TestOnly
    public void setAcquireReadSentinelSpinHook(Runnable hook) {
        this.acquireReadSentinelSpinHook = hook;
    }

    /**
     * Test-only hook: arms a one-shot failure injection on the next
     * {@link #publishSwap(int)} call. Used by smoke tests to drive
     * {@code LiveViewRefreshJob.publishToInMemoryTier}'s catch block via the
     * actual refresh worker without racing concurrent threads. The injection
     * fires once and self-clears; production code never sets this.
     */
    @TestOnly
    public void setFailNextPublishSwap(RuntimeException failure) {
        this.failNextPublishSwap = failure;
    }

    /**
     * Test-only hook: makes the next {@link #stampSymbolHorizon} throw
     * {@code failure} once every horizon is stamped and before the reverse-index
     * prune, standing in for a prune that fails. Production code never sets it.
     */
    @TestOnly
    public void setFailNextSymbolHorizonStamp(RuntimeException failure) {
        this.failNextSymbolHorizonStamp = failure;
    }

    /**
     * Records a lead drain the refresh worker sends straight to the LV table because the
     * symbol-id rewind left stranded ids in the cache (a reader's pin refused
     * {@link #tryRewindSymbolCache}, or the attempt failed): {@code routeUs}, when it was
     * decided, and {@code deficit}, the symbol horizon deficit it started from. The cadence
     * flush of a lead the worker kept from readers, for that reason or over a block of the
     * view's WAL the table had not applied, restages the slot as such a drain does once its
     * apply leaves no block of that WAL unapplied, and then records itself here too, with the
     * deficit of the drain to disk before it. Clears the turn-budget mark of the drain before
     * it. The worker reads the three to tell whether the next such drain may follow at once or
     * keeps its rows from readers until the next cadence flush. Refresh-worker only, under the
     * view's refresh latch; readers never touch them.
     */
    public void setStrandedIdDiskRoute(long routeUs, long deficit) {
        this.strandedIdDiskRouteUs = routeUs;
        this.strandedIdDiskRouteDeficit = deficit;
        this.isStrandedIdDiskRouteBudgetStopped = false;
    }

    /**
     * Marks the drain {@link #setStrandedIdDiskRoute} last recorded as stopped on its turn
     * budget, with base commits it was asked for still to drain. Refresh-worker only, under
     * the view's refresh latch.
     */
    public void setStrandedIdDiskRouteBudgetStopped() {
        this.isStrandedIdDiskRouteBudgetStopped = true;
    }

    /**
     * Records the highest base seqTxn the view's refresh cursor stood at when the refresh
     * worker rebuilt this tier. The worker passes the highest it has seen, so the value never
     * falls. Refresh-worker only, under the view's refresh latch; readers never touch it.
     */
    public void setStrandedIdLeadSeqTxn(long seqTxn) {
        this.strandedIdLeadSeqTxn = seqTxn;
    }

    /**
     * Attempts to take the writer sentinel on the requested slot via a
     * {@code 0 -> -1} CAS. Returns the slot's buffer on success, or
     * {@code null} on failure (some reader has the slot pinned). The caller
     * must follow up with {@link #publishSwap(int)} on success to publish the
     * new slot and release the sentinel, or
     * {@link #releaseWriteWithoutPublish(int)} to release the sentinel
     * without flipping {@code publishedIdx}.
     * <p>
     * Calling with {@code slotIdx = }{@link #getPublishedIdx()} is the Phase
     * 3a fast-path acquire: a successful CAS proves no readers currently pin
     * the published slot, so the writer can append in place and release via
     * {@link #releaseWriteWithoutPublish(int)} without ever flipping the
     * published index.
     */
    public LiveViewInMemoryBuffer tryAcquireWrite(int slotIdx) {
        long addr = refCountsAddr + ((long) slotIdx) * Long.BYTES;
        if (Os.compareAndSwap(addr, 0L, RC_WRITER_SENTINEL) == 0L) {
            return slots[slotIdx];
        }
        return null;
    }

    /**
     * Rewinds every SYMBOL column of the symbol cache to the LV table's committed symbol
     * count ({@link LiveViewSymbolCache#rewind}), provided no reader can resolve an id the
     * rewind takes back. Returns {@code false}, having changed nothing, when one can: a
     * reader pins a slot whose symbol horizon exceeds the committed count of some SYMBOL
     * column. The caller retries on a later cycle.
     * <p>
     * A rewind forgets the ids at and above the committed count and lets the next intern
     * re-bind them, while a reader resolves ids lock-free against its pinned slot. That
     * reader reaches the cache only below the slot's horizon ({@link LiveViewSymbolTable}):
     * every id it resolves - one a row of the slot carries, or one it enumerates - is below
     * the larger of its disk table's symbol count and the horizon, the overlay goes to the
     * cache for an id only when the disk table holds no value for it, and
     * {@code newSymbolKeyOf} stops at the horizon. So each slot goes one of two ways:
     * <ul>
     *   <li>Its writer sentinel's {@code 0 -> -1} CAS succeeds: no reader pins it, and while
     *   the sentinel is held a new reader spins instead of pinning. Releasing it through
     *   {@link #releaseWriteWithoutPublish} re-stamps its horizon from the rewound cache
     *   before the slot can be pinned again, so no later reader of it can reach the
     *   re-bound band - not through {@code keyOf}, which the horizon bounds, nor through the
     *   symbol count.</li>
     *   <li>The CAS fails because a reader pins it. The rewind goes ahead only when the
     *   slot's horizon is at or below the committed count of every SYMBOL column, and leaves
     *   that horizon as it is: every id its readers can resolve through the cache sits
     *   below the band the rewind takes back, and below everything the next intern re-binds.
     *   A slot staged from the LV table stops its horizon at the committed counts, so after
     *   a recovery the slot that holds the rewind up is in practice one that still holds a
     *   lead published before it, and only until the table commits as many values as that
     *   lead lists.</li>
     * </ul>
     * The horizon check stays true for the whole rewind. Only the refresh worker stamps a
     * horizon, always under the slot's writer sentinel, and the worker is the thread running
     * this method, so a horizon this method reads holds still until it returns, whichever
     * reader pins the slot in the meantime. A slot whose horizon exceeds a committed count
     * is either held under this method's sentinel, so no reader can pin it mid-rewind, or
     * makes it return before it changes anything. What a pinned reader still reads is safe
     * under the concurrent rewind and the interns after it, which write only at or above the
     * committed count (see {@link LiveViewSymbolCache#rewind}): the reverse index is a
     * concurrent map whose chain nodes keep their ids, and the {@code id -> string} store
     * publishes its page index through a volatile reference and writes no element below
     * the committed count.
     * <p>
     * The non-published slot cannot hand a reader an id the rewind takes back either. A new
     * reader pins only the published slot, and the writer refills the other one before it
     * publishes it. A reader that pinned the other slot while it was still published can
     * keep that pin through the rewind, but the rewind then goes ahead only when the slot's
     * horizon is at or below every committed count, as the second case above sets out, so
     * that reader resolves no id in the band the rewind takes back.
     * The published slot keeps its rows, so the caller must guarantee they carry no id the
     * rewind takes back - a slot staged from disk, or one re-stamped after an in-step flush.
     *
     * @param committedCounts the LV table's committed symbol count per output column,
     *                        indexed by column; only SYMBOL columns are read
     * @return {@code true} when the cache was rewound, {@code false} when a reader pins a
     * slot whose horizon reaches the band
     */
    public boolean tryRewindSymbolCache(IntList committedCounts) {
        final int publishedIdx = this.publishedIdx;
        final boolean isPublishedHeld = tryAcquireWrite(publishedIdx) != null;
        if (!isPublishedHeld && isSymbolHorizonAbove(publishedIdx, committedCounts)) {
            return false;
        }
        final int otherIdx = 1 - publishedIdx;
        final boolean isOtherHeld = tryAcquireWrite(otherIdx) != null;
        if (!isOtherHeld && isSymbolHorizonAbove(otherIdx, committedCounts)) {
            if (isPublishedHeld) {
                // Nothing changed, so there is no horizon to re-stamp.
                releaseWriterSentinel(publishedIdx, "tryRewindSymbolCache");
            }
            return false;
        }
        try {
            for (int i = 0, n = symbolCache.symbolColumnCount(); i < n; i++) {
                final int col = symbolCache.symbolColumnIndexAt(i);
                symbolCache.rewind(col, committedCounts.getQuick(col));
            }
        } finally {
            // Re-stamps every held horizon even when a rewind threw part-way: a column left
            // half-rewound has re-bound nothing yet, and the horizons it gets are the
            // store sizes it still has. A pinned slot keeps its horizon, which the check
            // above put below every band the rewind can take back.
            try {
                if (isOtherHeld) {
                    releaseWriteWithoutPublish(otherIdx);
                }
            } finally {
                if (isPublishedHeld) {
                    releaseWriteWithoutPublish(publishedIdx);
                }
            }
        }
        return true;
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
        // No native memory of its own (pure Java structures), but clear the
        // intern maps eagerly now that the last pin is gone and no cursor can
        // still read the lead's id -> string mapping.
        symbolCache.close();
        if (refCountsAddr != 0) {
            refCountsAddr = Unsafe.free(refCountsAddr, REFCOUNTS_BYTES, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        }
    }

    /**
     * Reports whether {@code slotIdx}'s stamped symbol horizon exceeds {@code committedCounts}
     * for any SYMBOL column, i.e. whether a reader of the slot may resolve an id at or above a
     * committed count through the cache. Writer-side only: the caller is the refresh worker,
     * the only thread that stamps a horizon, so the horizons it reads here hold still.
     */
    private boolean isSymbolHorizonAbove(int slotIdx, IntList committedCounts) {
        final LiveViewInMemoryBuffer slot = slots[slotIdx];
        for (int i = 0, n = symbolCache.symbolColumnCount(); i < n; i++) {
            final int col = symbolCache.symbolColumnIndexAt(i);
            if (slot.newSymbolMaxId(col) > committedCounts.getQuick(col)) {
                return true;
            }
        }
        return false;
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

    /**
     * Drops the writer sentinel on {@code slotIdx}, restoring it to idle. This CAS
     * is the release edge a reader's {@link #acquireRead()} CAS pairs with, so
     * everything the writer wrote to the slot publishes to readers here.
     */
    private void releaseWriterSentinel(int slotIdx, CharSequence caller) {
        final long addr = refCountsAddr + ((long) slotIdx) * Long.BYTES;
        final long observed = Os.compareAndSwap(addr, RC_WRITER_SENTINEL, 0L);
        if (observed != RC_WRITER_SENTINEL) {
            throw new IllegalStateException(
                    caller + ": writer sentinel not held [slot=" + slotIdx
                            + ", observed=" + observed + "]"
            );
        }
    }

    /**
     * Stamps every SYMBOL output column's current lead horizon
     * ({@link LiveViewSymbolCache#newSymbolMaxIdExclusive}) onto {@code slotIdx},
     * then reclaims the reverse-index band both slots have moved past.
     * Runs on the writer thread under the slot's writer sentinel, just before the
     * sentinel-release / publish CAS, so the snapshot is exact (the sole interner
     * is not growing the lists at this instant) and a reader that pins the slot
     * sees a stable, in-bounds horizon. A non-SYMBOL schema makes this a no-op.
     * <p>
     * Once both slots carry a horizon, the lower of the two is the oldest id band
     * any reader can still ask for - a reader only ever resolves against the slot
     * it pinned - so everything the chains hold below it, bar one node per value,
     * is dead. Both reads are on the writer thread, the only thread that stamps
     * them.
     * <p>
     * The two passes are ordered, not merged: stamping is a plain array store
     * that cannot fail, while {@code pruneReverseIndex} walks and allocates and
     * so can. Doing every stamp first means a prune that throws still leaves the
     * horizon complete, so the release CAS that follows can publish the slot
     * without a reader ever resolving symbols against a half-stamped horizon.
     * Interleaving them would make a mid-loop failure expose exactly that. The
     * per-column prune argument is unchanged by the split: {@code pruneReverseIndex}
     * touches only {@code col}'s own state, so no column's horizon can influence
     * another's reclaimed band.
     * <p>
     * {@code symbolHorizonCaps}, when not null, lowers each column's horizon to at most
     * its cap. Either the slot carries no id at or above the cap, or the slot is unstamped - a
     * lead kept from readers - and no read routes through it to the ids it carries at or
     * above the cap (see {@link #publishSwap(int, IntList)}). A lower horizon only keeps more
     * of the reverse index reachable, so the prune stays correct with it. The caller fills the
     * caps for every SYMBOL column, so reading them keeps the stamp pass from failing.
     */
    private void stampSymbolHorizon(int slotIdx, @Nullable IntList symbolHorizonCaps) {
        final LiveViewInMemoryBuffer slot = slots[slotIdx];
        final LiveViewInMemoryBuffer other = slots[1 - slotIdx];
        // Every close path takes the refresh latch first, which the refresh worker
        // holds across the turn that writes the tier, so a slot cannot be nulled
        // while its writer sentinel is held. Assert rather than tolerate it: with
        // refCountsAddr already zeroed, skipping the stamp would only defer the
        // failure to the release CAS below, at address 0.
        assert slot != null && other != null;
        final int n = symbolCache.symbolColumnCount();
        for (int i = 0; i < n; i++) {
            final int col = symbolCache.symbolColumnIndexAt(i);
            final int horizon = symbolCache.newSymbolMaxIdExclusive(col);
            slot.setNewSymbolMaxId(col, symbolHorizonCaps != null ? Math.min(horizon, symbolHorizonCaps.getQuick(col)) : horizon);
        }
        final RuntimeException injected = failNextSymbolHorizonStamp;
        if (injected != null) {
            // Single-shot, and deliberately fired between the passes: it stands in
            // for a prune failure, the only way this method throws in production.
            failNextSymbolHorizonStamp = null;
            throw injected;
        }
        for (int i = 0; i < n; i++) {
            final int col = symbolCache.symbolColumnIndexAt(i);
            symbolCache.pruneReverseIndex(col, Math.min(slot.newSymbolMaxId(col), other.newSymbolMaxId(col)));
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
