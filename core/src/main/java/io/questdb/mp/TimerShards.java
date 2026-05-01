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

package io.questdb.mp;

import io.questdb.log.Log;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

/**
 * Fixed array of {@link DelayQueue} shards, each drained by a daemon thread that fires
 * {@link DelayedFireable#expire()} on each entry whose deadline has popped. Replaces
 * the older periodic O(N) sweep over every parked SQL waiter with a per-entry blocking
 * wait sized exactly to the next deadline.
 *
 * <p>Sharding spreads the JDK-internal {@code DelayQueue} lock across cores; the shard
 * count is fixed at construction and never resized. Distribution uses identity hash, so
 * a pooled entry that re-registers always lands in the same shard.
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
 */
public final class TimerShards {
    private final Log log;
    private final DelayQueue<DelayedFireable>[] shards;
    private final Thread[] threads;
    private final String threadNamePrefix;
    private volatile boolean running;

    @SuppressWarnings("unchecked")
    public TimerShards(int shardCount, @NotNull String threadNamePrefix, @NotNull Log log) {
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be >= 1, got " + shardCount);
        }
        this.shards = (DelayQueue<DelayedFireable>[]) new DelayQueue<?>[shardCount];
        for (int i = 0; i < shardCount; i++) {
            this.shards[i] = new DelayQueue<>();
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
     * Inserts an entry into the appropriate shard. If {@link #shutdown()} has already
     * run, the entry's {@link DelayedFireable#shutdown()} is invoked instead so a
     * late-registering caller is unblocked and the entry never sits orphaned in a
     * drained shard.
     */
    public void register(@NotNull DelayedFireable entry) {
        if (!running) {
            entry.shutdown();
            return;
        }
        shards[shardFor(entry)].offer(entry);
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
        // Cannot use DelayQueue.drainTo: it only drains entries whose deadline has
        // already popped. We need every entry, regardless of deadline. Snapshot via
        // toArray (which sees the full heap) then clear.
        for (int i = 0, n = shards.length; i < n; i++) {
            DelayQueue<DelayedFireable> shard = shards[i];
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
            final DelayQueue<DelayedFireable> shard = shards[i];
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

    private void runShard(DelayQueue<DelayedFireable> shard) {
        while (running) {
            try {
                DelayedFireable e = shard.take();
                if (e == PoisonSentinel.INSTANCE || !running) {
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
    }

    private int shardFor(Object entry) {
        return (System.identityHashCode(entry) & 0x7fffffff) % shards.length;
    }

    /**
     * Single instance used to wake a {@link DelayQueue#take()} blocked thread when
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
            return unit.convert(-1, TimeUnit.MILLISECONDS);
        }

        @Override
        public void shutdown() {
        }
    }
}
