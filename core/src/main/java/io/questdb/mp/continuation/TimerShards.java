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

import io.questdb.log.Log;
import io.questdb.mp.CarrierIdentity;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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
 *   <li>{@link #halt()} is an alias for {@link #shutdown()}.</li>
 * </ul>
 *
 * <p>Late registration returns {@link SourceRegistrationResult#NOT_ACCEPTED} without
 * retaining the entry.
 *
 * <p><b>Thread-safety contract:</b> the instance is single-use. {@link #start()} and
 * shutdown serialize lifecycle changes; concurrent registration and removal are safe.
 * A stopped instance cannot restart.
 */
public final class TimerShards {
    private final @Nullable Runnable afterOfferForTesting;
    private final Log log;
    private final ObjList<DelayHeap<DelayedFireable>> shards;
    private final String threadNamePrefix;
    private final ObjList<Thread> threads;
    private boolean hasTimedOutThread;
    private boolean isShutdownComplete;
    private boolean isShutdownRequested;
    private volatile boolean isRunning;

    public TimerShards(int shardCount, @NotNull String threadNamePrefix, @NotNull Log log) {
        this(shardCount, threadNamePrefix, log, null);
    }

    public TimerShards(
            int shardCount,
            @NotNull String threadNamePrefix,
            @NotNull Log log,
            @Nullable Runnable afterOfferForTesting
    ) {
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be >= 1, got " + shardCount);
        }
        this.afterOfferForTesting = afterOfferForTesting;
        this.shards = new ObjList<>(shardCount);
        this.threads = new ObjList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            this.shards.add(new DelayHeap<>());
            this.threads.add(null);
        }
        this.threadNamePrefix = threadNamePrefix;
        this.log = log;
    }

    @TestOnly
    public int getShardCount() {
        return shards.size();
    }

    /**
     * Stops the timer threads and releases every accepted entry.
     */
    public void halt() {
        synchronized (this) {
            if (isShutdownComplete) {
                return;
            }
            requestShutdown();
            // a previous bounded shutdown exhausted its join budget on a still-blocked
            // shard thread; do not spend an unbounded join on the same thread
            if (hasTimedOutThread && hasLiveThread()) {
                return;
            }
            hasTimedOutThread = false;
        }
        if (!isJoinComplete()) {
            throw new IllegalStateException("timer shards did not halt");
        }
        synchronized (this) {
            if (!isShutdownComplete) {
                finishShutdown();
            }
        }
    }

    /**
     * Inserts an entry into the appropriate shard. An accepted entry receives exactly
     * one expire, shutdown, or successful unregister outcome.
     */
    public SourceRegistrationResult register(@NotNull DelayedFireable entry) {
        if (!isRunning) {
            return SourceRegistrationResult.NOT_ACCEPTED;
        }
        final DelayHeap<DelayedFireable> shard = shards.getQuick(shardFor(entry));
        shard.offer(entry);
        if (afterOfferForTesting != null) {
            afterOfferForTesting.run();
        }
        if (!isRunning) {
            if (shard.remove(entry)) {
                return SourceRegistrationResult.NOT_ACCEPTED;
            }
        }
        return SourceRegistrationResult.ACCEPTED;
    }

    /**
     * Drains every shard and invokes {@link DelayedFireable#shutdown()} on each entry.
     * Halts the timer threads. Idempotent. Must run while worker pools are still
     * RUNNING so that parked continuations have a carrier to remount on.
     */
    public void shutdown() {
        synchronized (this) {
            if (isShutdownComplete) {
                return;
            }
            requestShutdown();
        }
        if (!isJoinComplete()) {
            throw new IllegalStateException("timer shards did not halt");
        }
        synchronized (this) {
            if (!isShutdownComplete) {
                finishShutdown();
            }
        }
    }

    /**
     * Bounded variant of {@link #shutdown()}: gives the shard threads until the absolute
     * {@link System#nanoTime()} deadline to exit. Returns false without draining when a
     * thread outlives the deadline; a later call retries from where this one stopped.
     */
    public boolean shutdown(long deadlineNanos) {
        synchronized (this) {
            if (isShutdownComplete) {
                return true;
            }
            requestShutdown();
        }
        if (!isJoinComplete(deadlineNanos)) {
            return false;
        }
        synchronized (this) {
            if (!isShutdownComplete) {
                finishShutdown();
            }
        }
        return true;
    }

    /**
     * Sum of pending entries across all shards. For metrics/tests; not load-bearing.
     */
    public int size() {
        int total = 0;
        for (int i = 0, n = shards.size(); i < n; i++) {
            total += shards.getQuick(i).size();
        }
        return total;
    }

    /**
     * Launches one daemon thread per shard. Each thread loops on {@code shard.take()},
     * calls {@code expire()} on the popped entry, and survives any throwable so a
     * misbehaving entry cannot kill the timer.
     */
    public synchronized void start() {
        if (isRunning) {
            return;
        }
        if (isShutdownRequested) {
            throw new IllegalStateException("timer shards cannot restart after shutdown");
        }
        isRunning = true;
        for (int i = 0, n = shards.size(); i < n; i++) {
            final DelayHeap<DelayedFireable> shard = shards.getQuick(i);
            Thread t = new Thread(() -> runShard(shard), threadNamePrefix + "-" + i);
            t.setDaemon(true);
            threads.setQuick(i, t);
            t.start();
        }
    }

    public boolean unregister(@NotNull DelayedFireable entry) {
        return shards.getQuick(shardFor(entry)).remove(entry);
    }

    private void finishShutdown() {
        isShutdownComplete = true;
        for (int i = 0, n = shards.size(); i < n; i++) {
            final DelayHeap<DelayedFireable> shard = shards.getQuick(i);
            DelayedFireable entry;
            while ((entry = shard.poll()) != null) {
                if (entry instanceof PoisonSentinel) {
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

    private Thread getThread(int index) {
        synchronized (this) {
            return threads.getQuick(index);
        }
    }

    private synchronized boolean hasLiveThread() {
        for (int i = 0, n = threads.size(); i < n; i++) {
            final Thread t = threads.getQuick(i);
            if (t != null) {
                if (t.isAlive()) {
                    return true;
                }
                threads.setQuick(i, null);
            }
        }
        return false;
    }

    private boolean isCurrentThreadAShard() {
        final Thread current = Thread.currentThread();
        for (int i = 0, n = threads.size(); i < n; i++) {
            if (getThread(i) == current) {
                return true;
            }
        }
        return false;
    }

    private synchronized boolean isJoinComplete() {
        if (isCurrentThreadAShard()) {
            return false;
        }
        boolean isInterrupted = false;
        for (int i = 0, n = threads.size(); i < n; i++) {
            final Thread t = getThread(i);
            if (t == null) {
                continue;
            }
            while (t.isAlive()) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    isInterrupted = true;
                }
            }
            synchronized (this) {
                if (threads.getQuick(i) == t) {
                    threads.setQuick(i, null);
                }
            }
        }
        if (isInterrupted) {
            Thread.currentThread().interrupt();
        }
        return true;
    }

    private synchronized boolean isJoinComplete(long deadlineNanos) {
        if (isCurrentThreadAShard()) {
            return false;
        }
        boolean isInterrupted = false;
        boolean isComplete = true;
        for (int i = 0, n = threads.size(); i < n; i++) {
            final Thread t = getThread(i);
            if (t == null) {
                continue;
            }
            while (t.isAlive()) {
                final long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    hasTimedOutThread = true;
                    isComplete = false;
                    break;
                }
                try {
                    t.join(remainingNanos / 1_000_000L, (int) (remainingNanos % 1_000_000L));
                } catch (InterruptedException e) {
                    isInterrupted = true;
                }
            }
            if (!t.isAlive()) {
                synchronized (this) {
                    if (threads.getQuick(i) == t) {
                        threads.setQuick(i, null);
                    }
                }
            }
        }
        if (isInterrupted) {
            Thread.currentThread().interrupt();
        }
        return isComplete;
    }

    private void requestShutdown() {
        if (isShutdownRequested) {
            return;
        }
        isShutdownRequested = true;
        isRunning = false;
        for (int i = 0, n = shards.size(); i < n; i++) {
            if (threads.getQuick(i) != null) {
                shards.getQuick(i).offer(new PoisonSentinel());
            }
        }
    }

    private void runShard(DelayHeap<DelayedFireable> shard) {
        CarrierIdentity.bind();
        boolean isInterrupted = false;
        try {
            while (isRunning) {
                try {
                    DelayedFireable e = shard.take();
                    if (e instanceof PoisonSentinel) {
                        return;
                    }
                    if (!isRunning) {
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
                    isInterrupted = true;
                    if (!isRunning) {
                        return;
                    }
                } catch (Throwable t) {
                    log.critical().$("timer shard expire failed [error=").$(t).I$();
                }
            }
        } finally {
            // Release the CarrierLocal row pinned to this shard thread's id so
            // it does not survive across engine restarts in long-running JVMs.
            try {
                CarrierIdentity.unbind();
            } finally {
                if (isInterrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private int shardFor(Object entry) {
        return (System.identityHashCode(entry) & 0x7fffffff) % shards.size();
    }

    /**
     * Wakes a {@link DelayHeap#take()} blocked thread when {@link #halt()} or
     * {@link #shutdown()} runs. Its delay is always negative so the blocking take
     * returns immediately; both lifecycle hooks are no-ops. The intrusive heap
     * links an entry into at most one heap, so each shard gets its own instance.
     */
    private static final class PoisonSentinel implements DelayedFireable {
        private int heapIndex = -1;

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
        public int getHeapIndex() {
            return heapIndex;
        }

        @Override
        public void setHeapIndex(int heapIndex) {
            this.heapIndex = heapIndex;
        }

        @Override
        public void shutdown() {
        }
    }
}
