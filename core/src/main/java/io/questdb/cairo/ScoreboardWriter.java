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

package io.questdb.cairo;

import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class ScoreboardWriter implements Closeable {
    private final FilesFacade ff;
    private long fd;
    private long pScoreboard;
    private long size;
    private int partitionCount;

    public ScoreboardWriter(FilesFacade ff, @Transient Path path, int initialPartitionCount) {
        this.ff = ff;
        int plen = path.length();
        try {
            this.fd = ff.openRW(path.concat("scoreboard.d").$());
            if (fd == -1) {
                throw CairoException.instance(ff.errno()).put("Could not open scoreboard file [name=").put(path).put(']');
            }
            long memSize = getScoreboardSize(initialPartitionCount);
            if (!ff.allocate(fd, memSize)) {
                throw CairoException.instance(ff.errno()).put("No space left on device [name=").put(path).put(", size=").put(memSize).put(']');
            }
            this.size = memSize;
            pScoreboard = ff.mmap(fd, memSize, 0, Files.MAP_RW);
            this.partitionCount = initialPartitionCount;
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    public void acquireReadLock(long timestamp, long txn) {
        acquireReadLock(pScoreboard, timestamp, txn);
    }

    public boolean acquireWriteLock(long timestamp, long txn) {
        return acquireWriteLock(pScoreboard, timestamp, txn);
    }

    public void addPartition(long timestamp, long txn) {
        acquireHeaderLock(pScoreboard);
        try {
            long newSize = getScoreboardSize(partitionCount + 1);
            pScoreboard = ff.mremap(fd, pScoreboard, size, newSize, 0, Files.MAP_RW);
            size = newSize;
            addPartitionUnsafe(pScoreboard, timestamp, txn);
            partitionCount++;
        } finally {
            releaseHeaderLock(pScoreboard);
        }
    }

    @Override
    public void close() {
        if (pScoreboard != 0) {
            ff.munmap(pScoreboard, size);
            pScoreboard = 0;
        }
        if (fd != -1) {
            ff.close(fd);
            fd = -1;
        }
    }

    public void releaseReadLock(long timestamp, long txn) {
        releaseReadLock(pScoreboard, timestamp, txn);
    }

    public void releaseWriteLock(long timestamp, long txn) {
        releaseWriteLock(pScoreboard, timestamp, txn);
    }

    static native long getScoreboardSize(int partitionCount);

    static native void addPartitionUnsafe(long pScoreboard, long timestamp, long txn);

    static native void acquireHeaderLock(long pScoreboard);

    static native void releaseHeaderLock(long pScoreboard);

    static native boolean acquireWriteLock(long pScoreboard, long timestamp, long txn);

    static native void releaseWriteLock(long pScoreboard, long timestamp, long txn);

    static native void acquireReadLock(long pScoreboard, long timestamp, long txn);

    static native void releaseReadLock(long pScoreboard, long timestamp, long txn);
}
