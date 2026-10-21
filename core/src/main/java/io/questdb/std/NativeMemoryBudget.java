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

package io.questdb.std;

import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A shared native-memory limit whose reservations can be refused without throwing.
 */
public class NativeMemoryBudget {
    private final AtomicLong used = new AtomicLong();
    private volatile long limit;

    public NativeMemoryBudget(long limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("negative memory limit");
        }
        this.limit = limit;
    }

    public long getLimit() {
        return limit;
    }

    public long getUsedBytes() {
        return used.get();
    }

    public void release(long bytes) {
        assert bytes >= 0;
        long remaining = used.addAndGet(-bytes);
        assert remaining >= 0;
    }

    @TestOnly
    public void setLimit(long limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("negative memory limit");
        }
        this.limit = limit;
    }

    public boolean tryReserve(long bytes) {
        assert bytes >= 0;
        long current;
        do {
            current = used.get();
            if (bytes > limit - current) {
                return false;
            }
        } while (!used.compareAndSet(current, current + bytes));
        return true;
    }
}
