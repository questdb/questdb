/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.concurrent;

import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.exceptions.TimeoutException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TimeoutBlockingWaitStrategy extends AbstractWaitStrategy {
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long time;
    private final TimeUnit unit;

    public TimeoutBlockingWaitStrategy(long time, TimeUnit unit) {
        this.time = time;
        this.unit = unit;
    }

    @SuppressFBWarnings({"WA_AWAIT_NOT_IN_LOOP", "EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
    @Override
    public void await(int code) {
        lock.lock();
        try {
            if (!condition.await(time, unit)) {
                throw TimeoutException.INSTANCE;
            }
        } catch (InterruptedException e) {
            throw new JournalRuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void signal() {
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }

    }
}
