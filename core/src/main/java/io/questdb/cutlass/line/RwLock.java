/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.line;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class RwLock {
    private RLock readLock = new RLock();
    private WLock writeLock = new WLock();
    private AtomicInteger readerCount = new AtomicInteger();
    private AtomicInteger writerCount = new AtomicInteger();

    public Closeable readLock() {
        // When reader locked it is odd numbers in count
        int count = readerCount.addAndGet(2);
        if ((count & 1) == 0) {
            return readLock;
        }
        // writer locked
        // rollback lock attempt
        readerCount.addAndGet(-2);

        while (true) {
            while ((readerCount.get() & 1) != 0) {
                LockSupport.parkNanos(10);
            }
            // Try to re-lock reading
            int count2 = readerCount.addAndGet(2);
            if ((count2 & 1) == 0) {
                return readLock;
            }
        }
    }

    public Closeable writeLock() {
        // When reader locked it is even numbers in count
        assert writerCount.incrementAndGet() == 1 : "only supports singe thread attempting to get writer lock";

        int count = readerCount.incrementAndGet();
        if ((count & 1) == 1) {
            return writeLock;
        }

        while (true) {
            while ((readerCount.get() & 1) != 1) {
                LockSupport.parkNanos(10);
            }
            return writeLock;
        }
    }

    public class RLock implements Closeable {
        @Override
        public void close() throws IOException {
            readerCount.addAndGet(-2);
        }
    }

    public class WLock implements Closeable {
        @Override
        public void close() throws IOException {
            assert writerCount.decrementAndGet() == 0 : "only supports singe thread attempting to get writer lock";
            readerCount.decrementAndGet();
        }
    }
}
