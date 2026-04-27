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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runtime representation of a live view. Wraps a {@link DoubleBufferedTable} plus the
 * compiled factory, merge buffer, and metadata.
 * <p>
 * Concurrency model:
 * <ul>
 *     <li>The InMemoryTable is double-buffered ({@link DoubleBufferedTable}): readers
 *     {@link #acquireForRead} the published buffer lock-free, the refresh worker claims
 *     the non-published buffer via {@link #tryAcquireWriteBuffer}, mutates it, and
 *     {@link #publishWriteBuffer}s to swap the roles atomically. Readers never block
 *     writers; writers never block readers.</li>
 *     <li>{@link #refreshLatch} is a CAS latch held for the entire refresh operation
 *     (SQL compile + cursor + write buffer population), so a concurrent DROP cannot free
 *     the instance while the refresh is compiling — a window that per-buffer ref counts
 *     alone do not cover.</li>
 *     <li>{@link #dropped} is the DROP signal. {@link #tryCloseIfDropped} races refresh,
 *     reader release, and drop to actually free the instance; whichever party finds both
 *     buffers free and the refresh latch free wins, mirroring the {@code MatViewState}
 *     tryLock/tryCloseIfDropped pattern.</li>
 *     <li>{@link #pendingRefresh} is set by the refresh worker when
 *     {@link #tryAcquireWriteBuffer} returns null (readers still hold the write buffer).
 *     {@link LiveViewTimerJob} polls this flag and enqueues another refresh attempt on
 *     the next tick so the retry is decoupled from the refresh worker.</li>
 * </ul>
 */
public class LiveViewInstance implements QuietCloseable {
    private final DoubleBufferedTable bufferedTable = new DoubleBufferedTable();
    private final LiveViewDefinition definition;
    private final AtomicBoolean refreshLatch = new AtomicBoolean(false);
    // Cumulative count of WAL rows the live view refresh job skipped because they were
    // cold relative to the published buffer's oldest visible row and the compiled window
    // functions' lookback horizon. Accessed only while the refresh latch is held.
    private long coldRowSkipCount;
    // Cached factory from the bootstrap compile. Window functions carry state across rows
    // (e.g., row_number() counter), so incremental refresh must reuse the same function
    // instances — reset on every refresh would restart the counters. Accessed only while
    // the refresh latch is held, so no volatile is needed.
    private RecordCursorFactory compiledFactory;
    private volatile boolean dropped;
    private volatile String invalidationReason;
    private volatile boolean isClosed;
    // -1 means the view has not been bootstrapped yet; the next refresh must run a full
    // recompute and capture the base table's seqTxn at the start of the compile.
    private volatile long lastProcessedSeqTxn = -1;
    // Wall-clock microseconds (micros since epoch) of the last refresh completion. Read
    // by LiveViewTimerJob to decide whether the view's merge buffer has been idle for
    // longer than the LAG window and should be force-drained. Updated by the refresh
    // job while the refresh latch is held, so no volatile is needed.
    private long lastRefreshTimeUs;
    // Sorted native-memory staging buffer that holds rows until they age past the LAG
    // window. Allocated lazily on the first refresh, once the base table's metadata is
    // known. Accessed only while the refresh latch is held.
    private MergeBuffer mergeBuffer;
    // Set by the refresh worker when tryAcquireWriteBuffer fails because readers still
    // pin the non-published buffer. LiveViewTimerJob polls this on each tick and
    // enqueues a force-drain task to retry. Cleared on a successful publishWriteBuffer.
    private volatile boolean pendingRefresh;
    // Earliest base-table ts currently included in the window-function accumulator.
    // Set by bootstrap (equal to the effective lower bound used for the base scan) and
    // expanded backward by the cold-path disk-read replay when an any-unbounded view
    // sees a row older than the merge buffer's retention coverage. Long.MAX_VALUE until
    // the first bootstrap completes. Accessed only while the refresh latch is held.
    private long stateHorizonTimestamp = Long.MAX_VALUE;

    public LiveViewInstance(LiveViewDefinition definition) {
        this.definition = definition;
        this.bufferedTable.init(definition.getMetadata());
    }

    /**
     * Releases the write buffer's exclusive claim without publishing. Used when the
     * refresh fails mid-way; contents of the write buffer are discarded (the next
     * {@link #tryAcquireWriteBuffer} will return the same buffer, still dirty, and
     * the caller must re-sync before appending).
     */
    public void abortWriteBuffer(InMemoryTable writeBuffer) {
        bufferedTable.abortWrite(writeBuffer);
    }

    /**
     * Acquires the currently-published {@link InMemoryTable} for reading. Returns null
     * if the view has been dropped or invalidated (caller must not attempt a read).
     * On a non-null return, the caller must call {@link #releaseAfterRead} with the
     * same table reference.
     */
    public InMemoryTable acquireForRead() {
        if (dropped) {
            tryCloseIfDropped();
            return null;
        }
        return bufferedTable.acquire();
    }

    @Override
    public void close() {
        // Shutdown path only — called from LiveViewRegistry.close() after all workers
        // have stopped, so no concurrent refresh or reader is expected. Runtime drops
        // go through markAsDropped() + tryCloseIfDropped() instead.
        dropped = true;
        if (!isClosed) {
            isClosed = true;
            Misc.free(bufferedTable);
            compiledFactory = Misc.free(compiledFactory);
            mergeBuffer = Misc.free(mergeBuffer);
        }
    }

    /**
     * Increments the cumulative count of WAL rows skipped by the cold-path classifier.
     * Called by the refresh job when a row arrives with {@code ts < oldest_visible_ts -
     * max(frameLookback)} and the view's window functions are all bounded in the ts
     * dimension — the row cannot affect any visible output.
     */
    public void addColdRowSkips(long count) {
        coldRowSkipCount += count;
    }

    public long getColdRowSkipCount() {
        return coldRowSkipCount;
    }

    public RecordCursorFactory getCompiledFactory() {
        return compiledFactory;
    }

    public LiveViewDefinition getDefinition() {
        return definition;
    }

    public String getInvalidationReason() {
        return invalidationReason;
    }

    public long getLastProcessedSeqTxn() {
        return lastProcessedSeqTxn;
    }

    public long getLastRefreshTimeUs() {
        return lastRefreshTimeUs;
    }

    public MergeBuffer getMergeBuffer() {
        return mergeBuffer;
    }

    public long getStateHorizonTimestamp() {
        return stateHorizonTimestamp;
    }

    public void invalidate(String reason) {
        invalidationReason = reason;
    }

    public boolean isDropped() {
        return dropped;
    }

    public boolean isInvalid() {
        return invalidationReason != null;
    }

    public boolean isPendingRefresh() {
        return pendingRefresh;
    }

    /**
     * Signals that the view is being dropped. New readers and refreshes must bail out.
     * The actual free is handled by {@link #tryCloseIfDropped()}, which races drop,
     * refresh, and reader release paths — whichever runs last performs the free.
     */
    public void markAsDropped() {
        dropped = true;
    }

    /**
     * Returns the currently-published {@link InMemoryTable} without pinning it. Safe
     * only while the refresh latch is held (i.e. from the refresh worker) — callers
     * that do not serialize against other writers via the latch must use
     * {@link #acquireForRead} instead.
     * <p>
     * Used by the refresh worker to {@link InMemoryTable#copyFrom} the published state
     * into the write buffer before appending incremental delta rows.
     */
    public InMemoryTable peekPublishedBuffer() {
        return bufferedTable.peekPublished();
    }

    /**
     * Atomically swaps the published and write buffers. The buffer returned by the most
     * recent {@link #tryAcquireWriteBuffer} becomes the published buffer readers see.
     * Clears {@link #pendingRefresh}.
     */
    public void publishWriteBuffer() {
        bufferedTable.publishSwap();
        pendingRefresh = false;
    }

    /**
     * Releases a read pin previously obtained from {@link #acquireForRead} or
     * {@link #peekPublishedBuffer}. Must be called for every successful acquire.
     */
    public void releaseAfterRead(InMemoryTable buffer) {
        bufferedTable.release(buffer);
        tryCloseIfDropped();
    }

    /**
     * Stores the factory produced by the bootstrap compile so that subsequent incremental
     * refreshes can reuse its window function instances. Must be called while the refresh
     * latch is held. Frees any previously cached factory.
     */
    public void setCompiledFactory(RecordCursorFactory factory) {
        if (compiledFactory != factory) {
            Misc.free(compiledFactory);
            compiledFactory = factory;
        }
    }

    public void setLastProcessedSeqTxn(long seqTxn) {
        this.lastProcessedSeqTxn = seqTxn;
    }

    public void setLastRefreshTimeUs(long lastRefreshTimeUs) {
        this.lastRefreshTimeUs = lastRefreshTimeUs;
    }

    /**
     * Sets the merge buffer used to hold rows within the LAG window. Must be called
     * while the refresh latch is held. Frees any previously installed buffer.
     */
    public void setMergeBuffer(MergeBuffer mergeBuffer) {
        if (this.mergeBuffer != mergeBuffer) {
            Misc.free(this.mergeBuffer);
            this.mergeBuffer = mergeBuffer;
        }
    }

    /**
     * Records the earliest base-table ts covered by the current window-function state.
     * Set by bootstrap to the effective lower bound used for the base scan; the cold-path
     * disk-read replay reads this to decide whether a warm-path can use the merge buffer
     * (state coverage matches merge buffer coverage) or must re-read from disk (state
     * coverage extends below merge buffer coverage). Must be called while the refresh
     * latch is held.
     */
    public void setStateHorizonTimestamp(long stateHorizonTimestamp) {
        this.stateHorizonTimestamp = stateHorizonTimestamp;
    }

    /**
     * Non-blocking close: runs only when the view is dropped, the refresh latch is
     * free, and no reader currently holds either buffer. Callers (DROP path, refresh
     * finally hook, reader release, failed reader acquire) race to be the one that
     * frees the instance; losers are a no-op.
     * <p>
     * This is safe because every code path that can observe the instance — running
     * refresh, reader with a pinned buffer, or a reader that bailed due to {@code
     * dropped} — invokes this method on its way out, so some thread eventually wins
     * the CAS plus exclusive-buffer claim and performs the free.
     */
    public void tryCloseIfDropped() {
        if (!dropped) {
            return;
        }
        if (!refreshLatch.compareAndSet(false, true)) {
            // A refresh is in flight; its finally hook will retry.
            return;
        }
        try {
            if (!bufferedTable.tryAcquireExclusive()) {
                // Readers still hold one or both buffers; they will retry on release.
                return;
            }
            if (!isClosed) {
                isClosed = true;
                Misc.free(bufferedTable);
                compiledFactory = Misc.free(compiledFactory);
                mergeBuffer = Misc.free(mergeBuffer);
            }
        } finally {
            refreshLatch.set(false);
        }
    }

    /**
     * Attempts to claim exclusive ownership of the non-published buffer. Returns the
     * write buffer on success; {@code null} if readers still pin it — in which case
     * {@link #pendingRefresh} is set and the caller must not consume the cursor it
     * would have fed in.
     * <p>
     * Must be paired with {@link #publishWriteBuffer} on success or {@link
     * #abortWriteBuffer} on mid-cycle failure. Must be called while the refresh latch
     * is held.
     */
    public InMemoryTable tryAcquireWriteBuffer() {
        InMemoryTable writeBuffer = bufferedTable.tryAcquireWrite();
        if (writeBuffer == null) {
            pendingRefresh = true;
        }
        return writeBuffer;
    }

    /**
     * Acquires the refresh latch for the entire refresh operation
     * (compile + cursor + write buffer population). Returns false if another refresh,
     * or a {@link #tryCloseIfDropped} call, currently holds it.
     */
    public boolean tryLockForRefresh() {
        return refreshLatch.compareAndSet(false, true);
    }

    public void unlockAfterRefresh() {
        if (!refreshLatch.compareAndSet(true, false)) {
            throw new IllegalStateException("refresh latch is not held");
        }
    }
}
