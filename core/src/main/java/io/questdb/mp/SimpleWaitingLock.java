/*******************************************************************************
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Simple blocking lock which allows 2 arbitrary threads to compete for ownership. Behaviour is undefined for more than 2 threads.
 * The lock is not reentrant.
 */
public final class SimpleWaitingLock {
    private final AtomicReference<Thread> ownerOrWaiter = new AtomicReference<>(null);

    public boolean isLocked() {
        return ownerOrWaiter.get() != null;
    }

    /**
     * Acquires the lock. The method will block until the lock is acquired.
     */
    public void lock() {
        tryLock(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /**
     * Tries to acquire the lock, allowing for a specified timeout period.
     * If the lock is not acquired within the given timeout, the method returns false.
     *
     * @param timeout the maximum time to wait for acquiring the lock, in the given TimeUnit
     * @param unit    the time unit of the timeout parameter
     * @return true if the lock was acquired within the timeout period, false otherwise
     */
    public boolean tryLock(long timeout, TimeUnit unit) {
        Thread currentThread = Thread.currentThread();
        Thread expectedOwner = null;
        long deadline = System.nanoTime() + unit.toNanos(timeout); // this might overflow, but that's OK. we subtract current nanoTime and that makes it positive again
        for (long remainingNanos = deadline - System.nanoTime(); remainingNanos > 0; remainingNanos = deadline - System.nanoTime()) {
            if (ownerOrWaiter.compareAndSet(expectedOwner, currentThread)) {
                if (expectedOwner == null) {
                    // there was no owner before -> we acquired the lock and we are the new owner. yay!
                    return true;
                }
                // CAS succeeded, but there was an owner before -> we are a waiter
                LockSupport.parkNanos(remainingNanos);
            }
            expectedOwner = ownerOrWaiter.get();
        }
        return false;
    }

    /**
     * Tries to acquire the lock. If the lock is available, it is acquired and the method returns true.
     * If the lock is already acquired by another thread, the method returns false.
     *
     * @return true if the lock was acquired, false otherwise
     */
    public boolean tryLock() {
        return ownerOrWaiter.compareAndSet(null, Thread.currentThread());
    }

    /**
     * Releases the lock, allowing other threads to acquire it.
     * <p>
     * If the lock is not currently owned by any thread, an IllegalStateException is thrown.
     * If there is another thread waiting to acquire the lock, it is unparked.
     * If there is no waiting thread, the method returns without any additional action.
     * <p>
     * Implementation note: This method does not validate the calling thread is the owner of the lock. The method
     * releases the lock regardless of the calling thread. This makes it somewhat similar to a single permit semaphore.
     * In such case it may set the unpark flag for the formerly owning thread. The unpark flag should have no negative
     * side effect on the formerly owning thread as programs must be designed to handle spurious wakeups anyway.
     *
     * @throws IllegalStateException if the lock is not owned by any thread
     */
    public void unlock() {
        Thread currentThread = Thread.currentThread();
        Thread waitingOrOwningThread = ownerOrWaiter.getAndSet(null);
        if (waitingOrOwningThread == null) {
            throw new IllegalStateException("Lock is not owned");
        }
        if (waitingOrOwningThread == currentThread) {
            // no waiters
            return;
        }
        LockSupport.unpark(waitingOrOwningThread);
    }
}
