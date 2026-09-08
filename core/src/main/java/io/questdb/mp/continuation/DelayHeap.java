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

import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.Delayed;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Raw-{@link jdk.internal.vm.Continuation}-safe analogue of
 * {@link java.util.concurrent.DelayQueue}. The JDK class guards its internal
 * priority queue with a {@link java.util.concurrent.locks.ReentrantLock} that
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
 * <p>The heap is intrusive: each {@link Entry} stores its own heap slot, so an entry
 * can be linked into at most one heap at a time.
 *
 * <p>API is the subset {@link TimerShards} actually uses: {@link #offer},
 * {@link #poll}, {@link #remove}, {@link #take}, {@link #size}. Single-consumer is
 * assumed (one shard daemon per instance) - {@link #notify} not {@code notifyAll}
 * is used to wake waiters.
 */
public class DelayHeap<E extends DelayHeap.Entry> {
    private static final int INITIAL_CAPACITY = 16;
    private final ObjList<E> heap = new ObjList<>(INITIAL_CAPACITY);
    private int size;

    public DelayHeap() {
        heap.setPos(INITIAL_CAPACITY);
    }

    public synchronized void clear() {
        for (int i = 0; i < size; i++) {
            heap.getQuick(i).setHeapIndex(-1);
            heap.setQuick(i, null);
        }
        size = 0;
    }

    public synchronized boolean offer(E e) {
        assert e.getHeapIndex() < 0 : "entry is already in a heap";
        if (size == heap.size()) {
            heap.extendPos(heap.size() << 1);
        }
        final int index = size;
        final int target = findSiftUpTarget(index, e);
        siftUpTo(index, target, e);
        size = index + 1;
        if (heap.getQuick(0) == e) {
            notify();
        }
        return true;
    }

    public synchronized E poll() {
        return size == 0 ? null : removeAt(0);
    }

    public synchronized boolean remove(E e) {
        final int index = e.getHeapIndex();
        if (index < 0 || index >= size || heap.getQuick(index) != e) {
            return false;
        }
        final boolean isHead = index == 0;
        removeAt(index);
        if (isHead) {
            notify();
        }
        return true;
    }

    public synchronized int size() {
        return size;
    }

    public synchronized E take() throws InterruptedException {
        for (; ; ) {
            if (size == 0) {
                wait();
            } else {
                final E first = heap.getQuick(0);
                final long delay = first.getDelay(NANOSECONDS);
                if (delay <= 0L) {
                    return removeAt(0);
                }
                final long millis = delay / 1_000_000L;
                final int nanos = (int) (delay % 1_000_000L);
                wait(millis, nanos);
            }
        }
    }

    @TestOnly
    public synchronized ObjList<E> toList() {
        final ObjList<E> copy = new ObjList<>(size);
        for (int i = 0; i < size; i++) {
            copy.add(heap.getQuick(i));
        }
        return copy;
    }

    private int findSiftDownTarget(int index, E e, int heapSize) {
        final int half = heapSize >>> 1;
        while (index < half) {
            int child = (index << 1) + 1;
            E c = heap.getQuick(child);
            final int right = child + 1;
            if (right < heapSize && c.compareTo(heap.getQuick(right)) > 0) {
                child = right;
                c = heap.getQuick(child);
            }
            if (e.compareTo(c) <= 0) {
                break;
            }
            index = child;
        }
        return index;
    }

    private int findSiftUpTarget(int index, E e) {
        while (index > 0) {
            final int parent = (index - 1) >>> 1;
            if (e.compareTo(heap.getQuick(parent)) >= 0) {
                break;
            }
            index = parent;
        }
        return index;
    }

    private E removeAt(int index) {
        final E removed = heap.getQuick(index);
        final int last = size - 1;
        if (index == last) {
            removed.setHeapIndex(-1);
            heap.setQuick(last, null);
            size = last;
            return removed;
        }

        final E moved = heap.getQuick(last);
        final int target;
        if (index > 0 && moved.compareTo(heap.getQuick((index - 1) >>> 1)) < 0) {
            target = findSiftUpTarget(index, moved);
        } else {
            target = findSiftDownTarget(index, moved, last);
        }

        removed.setHeapIndex(-1);
        heap.setQuick(last, null);
        size = last;
        if (target < index) {
            siftUpTo(index, target, moved);
        } else if (target > index) {
            siftDownTo(index, target, moved);
        } else {
            heap.setQuick(index, moved);
            moved.setHeapIndex(index);
        }
        return removed;
    }

    private void siftDownTo(int index, int target, E e) {
        final long targetPath = (long) target + 1;
        final int targetDepth = 63 - Long.numberOfLeadingZeros(targetPath);
        int currentDepth = 63 - Long.numberOfLeadingZeros((long) index + 1);
        while (index != target) {
            final int child = (int) (targetPath >>> (targetDepth - currentDepth - 1)) - 1;
            final E c = heap.getQuick(child);
            heap.setQuick(index, c);
            c.setHeapIndex(index);
            index = child;
            currentDepth++;
        }
        heap.setQuick(target, e);
        e.setHeapIndex(target);
    }

    private void siftUpTo(int index, int target, E e) {
        while (index != target) {
            final int parent = (index - 1) >>> 1;
            final E p = heap.getQuick(parent);
            heap.setQuick(index, p);
            p.setHeapIndex(index);
            index = parent;
        }
        heap.setQuick(target, e);
        e.setHeapIndex(target);
    }

    public interface Entry extends Delayed {
        int getHeapIndex();

        void setHeapIndex(int heapIndex);
    }
}
