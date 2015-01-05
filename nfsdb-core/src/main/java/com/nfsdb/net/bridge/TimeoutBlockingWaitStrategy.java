/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.net.bridge;

import com.lmax.disruptor.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TimeoutBlockingWaitStrategy implements WaitStrategy {
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();
    private final long timeoutInNanos;

    public TimeoutBlockingWaitStrategy(final long timeout, final TimeUnit units) {
        timeoutInNanos = units.toNanos(timeout);
    }

    @Override
    public long waitFor(final long sequence,
                        final Sequence cursorSequence,
                        final Sequence dependentSequence,
                        final SequenceBarrier barrier)
            throws AlertException, InterruptedException, TimeoutException {
        long nanos = timeoutInNanos;
        long t = System.nanoTime();

        long availableSequence;
        if (cursorSequence.get() < sequence) {
            lock.lock();
            try {
                while (cursorSequence.get() < sequence) {
                    barrier.checkAlert();
                    nanos = processorNotifyCondition.awaitNanos(nanos);
                    if (nanos <= 0) {
                        throw TimeoutException.INSTANCE;
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
            Thread.yield();
            if (System.nanoTime() >= t + nanos) {
                throw TimeoutException.INSTANCE;
            }
        }


        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        lock.lock();
        try {
            processorNotifyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
