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

package io.questdb.test.cairo;

import io.questdb.test.std.TestFilesFacadeImpl;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts the allocations of disk space, and fails, once, the next one for the file descriptor that the test sets in
 * failingFd, as on a full disk. The facade resets failingFd to -1 when it fails the allocation, so a test that
 * asserts -1 after the write proves that the write failed there. Tests of the append memory, which open their files
 * and know the descriptors, use this facade; tests that write through a table writer use AllocateFailingFacade,
 * which finds the file by its name.
 */
public class AllocationCountingFacade extends TestFilesFacadeImpl {
    public final AtomicInteger allocations = new AtomicInteger();
    public final AtomicLong failingFd = new AtomicLong(-1);

    @Override
    public boolean allocate(long fd, long size) {
        allocations.incrementAndGet();
        if (failingFd.compareAndSet(fd, -1)) {
            return false;
        }
        return super.allocate(fd, size);
    }
}
