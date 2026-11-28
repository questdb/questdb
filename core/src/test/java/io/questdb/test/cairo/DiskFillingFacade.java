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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A disk that fills up and stays full: when armed, the allocation of disk space for one file of one table fails, as
 * {@link AllocateFailingFacade} fails it, and every allocation after it fails too, until the test clears isFull to
 * free the disk. failedAllocations counts the allocations that failed, so a test can assert that a write made no
 * allocation after the one that failed.
 */
public class DiskFillingFacade extends AllocateFailingFacade {
    public final AtomicInteger failedAllocations = new AtomicInteger();
    public final AtomicBoolean isFull = new AtomicBoolean();

    public DiskFillingFacade(String tableName, String fileName) {
        super(tableName, fileName);
    }

    @Override
    public boolean allocate(long fd, long size) {
        if (isFull.get()) {
            failedAllocations.incrementAndGet();
            return false;
        }
        return super.allocate(fd, size);
    }

    @Override
    protected void onAllocationFailed() {
        failedAllocations.incrementAndGet();
        isFull.set(true);
    }
}
