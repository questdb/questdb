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

package io.questdb.mp.continuation;

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.mp.CarrierIdentity;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * Fixed array of {@link DelayHeap} shards, each drained by a daemon thread that fires
 * {@link DelayedFireable#expire()} on each entry whose deadline has popped. Replaces
 * the older periodic O(N) sweep over every parked SQL waiter with a per-entry blocking
 * wait sized exactly to the next deadline.
 *
 * <p>Sharding spreads the per-heap monitor across cores; the shard count is fixed at
 * construction and never resized. Distribution uses identity hash, so a pooled entry
 * that re-registers always lands in the same shard. {@link DelayHeap} is used instead
 * of {@link java.util.concurrent.DelayQueue} because the latter's internal
 * {@link java.util.concurrent.locks.ReentrantLock} corrupts (IMSE, then permanent lock
 * hold) when called from inside a raw {@link jdk.internal.vm.Continuation} body.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li>Construct cheaply with {@link #TimerShards(int, String, Log)}; no threads start.</li>
 *   <li>{@link #start()} launches one daemon thread per shard.</li>
 *   <li>{@link #register(DelayedFireable)} drops an entry into the appropriate shard.</li>
 *   <li>{@link #shutdown()} drains every shard and calls {@code shutdown()} on each entry,
 *       so parked continuations remount, observe the shutdown flag, and unwind. Idempotent.</li>
 *   <li>{@link #halt()} sets the running flag false, kicks each shard with a poison
 *       sentinel, and joins the threads. Idempotent and safe to call before/after
 *       {@link #shutdown()}.</li>
 * </ul>
 *
 * <p>Late-registration race: if {@link #register} is called after {@link #shutdown}, the
 * entry is sent straight to its {@code shutdown()} hook instead of being inserted into
 * a (possibly already-drained) shard.
 *
 * <p><b>Thread-safety contract:</b> single-use, single-threaded init. {@link #start()}
 * must run to completion before any other method ({@link #register}, {@link #shutdown},
 * {@link #halt}, {@link #size}) is called from any thread. {@code start()} reads
 * {@code running} and writes the {@code threads[]} array without synchronization and is
 * not safe to race against itself or any other entry point. In practice
 * {@code CairoEngine} owns the only instance and drives the lifecycle from a single
 * bootstrap thread; after {@code start()} returns, the instance is published to the
 * rest of the engine and concurrent {@code register}/{@code shutdown}/{@code halt}
 * calls are safe.
 */
public final class TimerShards {
    private final Log log;
    private final DelayHeap<DelayedFireable>[] shards;
    private final String threadNamePrefix;
    private final Thread[] threads;
    private volatile boolean running;

    @SuppressWarnings("unchecked")
    public TimerShards(int shardCount, @NotNull String threadNamePrefix, @NotNull Log log) {
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be >= 1, got " + shardCount);
        }
        this.shards = (DelayHeap<DelayedFireable>[]) new DelayHeap<?>[shardCount];
        for (int i = 0; i < shardCount; i++) {
            this.shards[i] = new DelayHeap<>();
        }
        this.threads = new Thread[shardCount];
        this.threadNamePrefix = threadNamePrefix;
        this.log = log;
    }

    /**
     * Halts the timer threads without draining queued entries. Sets the running flag to
     * {@code false} and pushes a poison sentinel into each shard so the blocked
     * {@code take()} returns and the thread observes the flag. Joins the threads with a
     * small timeout. Idempotent. Used by {@link #shutdown()} and as a defensive cleanup
     * from {@code CairoEngine.close()}.
     */
    public void halt() {
        if (!running) {
            // Make sure threads have actually exited even if shutdown was never called.
            joinThreadsQuietly();
            return;
        }
        running = false;
        for (int i = 0, n = shards.length; i < n; i++) {
            shards[i].offer(PoisonSentinel.INSTANCE);
        }
        joinThreadsQuietly();
    }

    /**
     * Inserts an entry into the appropriate shard. Throws
     * {@link CairoException#queryCancelled()} if {@link #shutdown()} has run or
     * races with this call between the offer and the post-check, so the caller
     * unwinds before parking on an entry that no shard thread will ever fire.
     */
    public void register(@NotNull DelayedFireable entry) {
        if (!running) {
            entry.shutdown();
            return;
        }
        final DelayHeap<DelayedFireable> shard = shards[shardFor(entry)];
        shard.offer(entry);
        if (!running) {
            // Won't fix: a concurrent shutdown() may have already drained the shard, so this
            // entry is now stranded in a dead shard heap. We deliberately don't shard.remove()
            // it: shutdown is a one-shot event, the leak is bounded to entries still in flight
            // at that moment, and they're released when TimerShards itself is collected. Not
            // worth the CPU on the steady-state register() path.
            throw CairoException.queryCancelled();
        }
    }

    /**
     * Drains every shard and invokes {@link DelayedFireable#shutdown()} on each entry.
     * Halts the timer threads. Idempotent. Must run while worker pools are still
     * RUNNING so that parked continuations have a carrier to remount on.
     */
    public synchronized void shutdown() {
        if (!running) {
            joinThreadsQuietly();
            return;
        }
        running = false;
        for (int i = 0, n = shards.length; i < n; i++) {
            shards[i].offer(PoisonSentinel.INSTANCE);
        }
        joinThreadsQuietly();
        // Snapshot via toArray (which sees the full heap, including unexpired
        // entries) then clear. We need every entry regardless of deadline.
        for (int i = 0, n = shards.length; i < n; i++) {
            DelayHeap<DelayedFireable> shard = shards[i];
            Object[] snapshot = shard.toArray();
            shard.clear();
            for (int j = 0, m = snapshot.length; j < m; j++) {
                DelayedFireable entry = (DelayedFireable) snapshot[j];
                if (entry == PoisonSentinel.INSTANCE) {
                    continue;
                }
                try {
                    entry.shutdown();
                } catch (Throwable t) {
                    log.critical().$("error during timer shard shutdown drain [error=").$(t).I$();
                }
            }
        }
    }

    /**
     * Sum of pending entries across all shards. For metrics/tests; not load-bearing.
     */
    public int size() {
        int total = 0;
        for (int i = 0, n = shards.length; i < n; i++) {
            total += shards[i].size();
        }
        return total;
    }

    /**
     * Launches one daemon thread per shard. Each thread loops on {@code shard.take()},
     * calls {@code expire()} on the popped entry, and survives any throwable so a
     * misbehaving entry cannot kill the timer.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        for (int i = 0; i < shards.length; i++) {
            final DelayHeap<DelayedFireable> shard = shards[i];
            Thread t = new Thread(() -> runShard(shard), threadNamePrefix + "-" + i);
            t.setDaemon(true);
            threads[i] = t;
            t.start();
        }
    }

    private void joinThreadsQuietly() {
        for (int i = 0; i < threads.length; i++) {
            Thread t = threads[i];
            if (t == null) {
                continue;
            }
            try {
                t.join(2_000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            threads[i] = null;
        }
    }

    private void runShard(DelayHeap<DelayedFireable> shard) {
        CarrierIdentity.bind();
        try {
            while (running) {
                try {
                    DelayedFireable e = shard.take();
                    if (e == PoisonSentinel.INSTANCE) {
                        return;
                    }
                    if (!running) {
                        // shutdown() flipped running after take() already removed e from the
                        // heap, so its drain snapshot will never see e. Fire e's shutdown hook
                        // here instead of dropping it, otherwise the continuation bound to e is
                        // never resumed and its context (and socket fd) leaks. We are the sole
                        // owner post-take(), so this is exactly one terminal call.
                        e.shutdown();
                        return;
                    }
                    e.expire();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    if (!running) {
                        return;
                    }
                } catch (Throwable t) {
                    log.critical().$("timer shard expire failed [error=").$(t).I$();
                }
            }
        } finally {
            // Release the CarrierLocal row pinned to this shard thread's id so
            // it does not survive across engine restarts in long-running JVMs.
            CarrierIdentity.unbind();
        }
    }

    private int shardFor(Object entry) {
        return (System.identityHashCode(entry) & 0x7fffffff) % shards.length;
    }

    /**
     * Single instance used to wake a {@link DelayHeap#take()} blocked thread when
     * {@link #halt()} or {@link #shutdown()} runs. Its delay is always negative so the
     * blocking take returns immediately; both lifecycle hooks are no-ops.
     */
    private static final class PoisonSentinel implements DelayedFireable {
        static final PoisonSentinel INSTANCE = new PoisonSentinel();

        @Override
        public int compareTo(@NotNull java.util.concurrent.Delayed o) {
            long a = getDelay(TimeUnit.NANOSECONDS);
            long b = o.getDelay(TimeUnit.NANOSECONDS);
            return Long.compare(a, b);
        }

        @Override
        public void expire() {
        }

        @Override
        public long getDelay(@NotNull TimeUnit unit) {
            return unit.convert(Long.MIN_VALUE / 2, TimeUnit.NANOSECONDS);
        }

        @Override
        public void shutdown() {
        }
    }
}
