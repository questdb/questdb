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

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Double-buffered {@link InMemoryTable}. One buffer is <em>published</em> (readers acquire
 * it lock-free via {@link #acquire()}), the other is the <em>write</em> buffer, mutated
 * in private by the single refresh worker between a {@link #tryAcquireWrite()} claim and
 * the subsequent {@link #publishSwap()}. Readers never block the writer, and the writer
 * never blocks readers — at the cost of keeping two parallel column sets (2x memory).
 * <p>
 * Concurrency primitives:
 * <ul>
 *     <li>{@link #publishedIdx} is a volatile int: readers sample it, writer flips it on
 *         publish. A new reader that samples the old value just before a swap still gets
 *         a valid (frozen) snapshot — refs pin the exact buffer they sampled.</li>
 *     <li>{@link #refCounts} holds one AtomicInteger cell per buffer. Readers CAS
 *         {@code rc -> rc+1} to acquire, decrement to release. The writer CASes the
 *         write buffer's cell {@code 0 -> -1} to take exclusive ownership; a negative
 *         value blocks new readers from that cell, and the writer's CAS only succeeds
 *         when no reader currently pins it.</li>
 * </ul>
 * The writer is single-threaded per view — the caller (typically {@code LiveViewInstance})
 * must serialize refresh invocations via a latch. This class only arbitrates the
 * reader/writer boundary.
 */
public class DoubleBufferedTable implements QuietCloseable {
    private static final int SENTINEL_WRITE_OWNED = -1;
    private final InMemoryTable[] buffers = new InMemoryTable[]{new InMemoryTable(), new InMemoryTable()};
    private final AtomicIntegerArray refCounts = new AtomicIntegerArray(2);
    private boolean isClosed;
    private volatile int publishedIdx;

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            for (int i = 0; i < buffers.length; i++) {
                buffers[i] = Misc.free(buffers[i]);
            }
        }
    }

    /**
     * Acquires the currently-published buffer for reading. The returned table stays
     * valid (its bytes are not mutated) until the matching {@link #release} call.
     * Readers never block — the only failure modes are a negative refCount (writer
     * has claimed the buffer, flip over to the other) or losing the inc CAS (retry).
     */
    public InMemoryTable acquire() {
        for (; ; ) {
            int idx = publishedIdx; // volatile read
            int rc = refCounts.get(idx);
            if (rc < 0) {
                // Writer has claimed this buffer. This can only be the case for
                // the non-published buffer, which means publishedIdx is stale
                // relative to reality — reload and retry.
                Thread.onSpinWait();
                continue;
            }
            if (refCounts.compareAndSet(idx, rc, rc + 1)) {
                return buffers[idx];
            }
        }
    }

    /**
     * Returns the metadata common to both buffers. Safe to call without acquiring.
     * Both buffers are initialised from the same {@link RecordMetadata}, so this is
     * stable for the lifetime of the instance.
     */
    public RecordMetadata getMetadata() {
        return buffers[0].getMetadata();
    }

    public void init(RecordMetadata metadata) {
        buffers[0].init(metadata);
        buffers[1].init(metadata);
    }

    /**
     * Returns the currently-published buffer without pinning it. Safe only when the
     * caller holds a higher-level write lock that excludes concurrent {@link
     * #publishSwap}s (e.g. a live view's refresh latch). Readers must use
     * {@link #acquire()} instead — peeking and then reading would race a publish.
     */
    public InMemoryTable peekPublished() {
        return buffers[publishedIdx];
    }

    /**
     * Atomically swaps the published and write buffer roles. The new published buffer
     * is the one {@link #tryAcquireWrite()} most recently handed out. Must be called
     * exactly once per successful write claim, after the writer has finished mutating
     * the write buffer.
     */
    public void publishSwap() {
        int oldIdx = publishedIdx;
        int newIdx = 1 - oldIdx;
        // The write buffer must currently be claimed (rc = SENTINEL_WRITE_OWNED).
        assert refCounts.get(newIdx) == SENTINEL_WRITE_OWNED : "publishSwap without an active write claim";
        // Flip the published pointer first, then release the sentinel. New readers that
        // sample publishedIdx after this store land on the just-swapped buffer; readers
        // that sampled the old value and are mid-acquire see the still-non-negative
        // refCount on the old-published buffer and proceed there.
        publishedIdx = newIdx;
        refCounts.set(newIdx, 0);
    }

    /**
     * Releases a pinned buffer. {@code pinned} must be the exact instance returned by
     * {@link #acquire()}.
     */
    public void release(InMemoryTable pinned) {
        int idx = (pinned == buffers[0]) ? 0 : 1;
        refCounts.decrementAndGet(idx);
    }

    /**
     * Attempts to take exclusive ownership of the write buffer. Returns the write
     * buffer on success, or {@code null} if in-flight readers still hold it — in
     * which case the caller must not mutate the buffer and must not call
     * {@link #publishSwap()}. Typical retry strategy is to reschedule the refresh
     * for a later tick rather than spin.
     */
    public InMemoryTable tryAcquireWrite() {
        int writeIdx = 1 - publishedIdx;
        if (refCounts.compareAndSet(writeIdx, 0, SENTINEL_WRITE_OWNED)) {
            return buffers[writeIdx];
        }
        return null;
    }

    /**
     * Releases a write claim without publishing. Use when a refresh cycle fails
     * mid-way and the write buffer's contents are discarded; the next
     * {@link #tryAcquireWrite()} call will hand out the same buffer again (still
     * dirty — the caller is expected to re-sync from the published buffer).
     */
    public void abortWrite(InMemoryTable writeBuffer) {
        int idx = (writeBuffer == buffers[0]) ? 0 : 1;
        assert refCounts.get(idx) == SENTINEL_WRITE_OWNED : "abortWrite without an active claim";
        refCounts.set(idx, 0);
    }

    /**
     * Attempts to claim exclusive ownership of both buffers, for close-time freeing.
     * Returns true if no readers currently hold either buffer; false otherwise (in
     * which case the caller should retry later, typically piggybacking on the next
     * reader's {@code release()}).
     */
    public boolean tryAcquireExclusive() {
        if (!refCounts.compareAndSet(0, 0, SENTINEL_WRITE_OWNED)) {
            return false;
        }
        if (!refCounts.compareAndSet(1, 0, SENTINEL_WRITE_OWNED)) {
            refCounts.set(0, 0);
            return false;
        }
        return true;
    }
}
