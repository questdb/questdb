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

import java.util.PriorityQueue;
import java.util.concurrent.Delayed;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Raw-{@link jdk.internal.vm.Continuation}-safe analogue of
 * {@link java.util.concurrent.DelayQueue}. The JDK class guards its internal
 * {@link PriorityQueue} with a {@link java.util.concurrent.locks.ReentrantLock} that
 * tracks ownership via {@code Thread.currentThread()}. When a cont yields between lock
 * acquisition and release - or when C2 hoists the identity read inconsistently across
 * inlined call sites - the unlock's owner check fails with
 * {@link IllegalMonitorStateException}, the JDK lock is never released, and every
 * subsequent operation on the queue blocks forever.
 *
 * <p>This class uses {@code synchronized} directly. Unlike {@link java.util.concurrent.locks.ReentrantLock},
 * {@code monitorenter}/{@code monitorexit} use the JVM-internal {@code JavaThread*}
 * for ownership tracking rather than the Java {@code Thread.currentThread()} call, so
 * they are not exposed to the C2 LICM hoist hazard documented in
 * {@code CARRIER_LOCAL.md}. The contract is that callers MUST NOT call
 * {@link jdk.internal.vm.Continuation#yield} from within any method of this class -
 * a constraint that holds trivially because no method here invokes anything that could
 * yield (the body is heap ops plus a single {@link Object#notify}).
 *
 * <p>API is the subset {@link TimerShards} actually uses: {@link #offer},
 * {@link #take}, {@link #toArray}, {@link #clear}, {@link #size}. Single-consumer is
 * assumed (one shard daemon per instance) - {@link #notify} not {@code notifyAll} is
 * used to wake waiters.
 */
public class DelayHeap<E extends Delayed> {

    private final PriorityQueue<E> q = new PriorityQueue<>();

    public synchronized void clear() {
        q.clear();
    }

    public synchronized boolean offer(E e) {
        q.offer(e);
        if (q.peek() == e) {
            notify();
        }
        return true;
    }

    public synchronized int size() {
        return q.size();
    }

    public synchronized E take() throws InterruptedException {
        for (; ; ) {
            E first = q.peek();
            if (first == null) {
                wait();
            } else {
                long delay = first.getDelay(NANOSECONDS);
                if (delay <= 0L) {
                    return q.poll();
                }
                long millis = delay / 1_000_000L;
                int nanos = (int) (delay % 1_000_000L);
                wait(millis, nanos);
            }
        }
    }

    public synchronized Object[] toArray() {
        return q.toArray();
    }
}
