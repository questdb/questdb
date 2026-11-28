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

import io.questdb.std.str.LPSZ;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fails, once when armed, the allocation of disk space for one file of one table, as on a full disk: the file that
 * the writer opened last under that name, with or without the column name txn that a column added later has. The
 * test arms the facade before the write that needs the next page of the file, and asserts after it that the facade
 * is no longer armed, which proves that the write failed there.
 */
public class AllocateFailingFacade extends TestFilesFacadeImpl {
    public final AtomicBoolean isArmed = new AtomicBoolean();
    private final TableFile file;

    public AllocateFailingFacade(String tableName, String fileName) {
        this.file = new TableFile(tableName, fileName);
    }

    @Override
    public boolean allocate(long fd, long size) {
        if (file.is(fd) && isArmed.compareAndSet(true, false)) {
            onAllocationFailed();
            return false;
        }
        return super.allocate(fd, size);
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        final long fd = super.openRW(name, opts);
        file.onOpen(name, fd);
        return fd;
    }

    // allocate() calls it when it fails the allocation, for a subclass to chain a further failure to it
    protected void onAllocationFailed() {
    }
}
