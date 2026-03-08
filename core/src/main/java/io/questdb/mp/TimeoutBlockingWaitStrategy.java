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

    @Override
    public boolean acceptSignal() {
        return true;
    }

    @Override
    public void await() {
        lock.lock();
        try {
            if (alerted) {
                throw AlertedException.INSTANCE;
            }
            if (!condition.await(time, unit)) {
                throw TimeoutException.INSTANCE;
            }
        } catch (InterruptedException e) {
            throw TimeoutException.INSTANCE;
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
